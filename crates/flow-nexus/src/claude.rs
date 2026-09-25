//! Dynamic readiness projection for daemon-owned Claude sessions.

use std::{fs, path::Path};

use serde_json::Value;
use signal_flow::{EndpointSelection, FlowNode, HarnessKind, RouteReadiness};

const JOBS: &str = "/home/li/.claude/jobs";
const ROSTER: &str = "/home/li/.claude/daemon/roster.json";

pub fn refresh_readiness(mut node: FlowNode) -> FlowNode {
    if node.harness_kind != HarnessKind::Claude {
        return node;
    }
    node.endpoint_selection = refresh_at(&node, Path::new(JOBS), Path::new(ROSTER));
    node
}

fn refresh_at(node: &FlowNode, jobs: &Path, roster_path: &Path) -> EndpointSelection {
    let EndpointSelection::Available(endpoint) = &node.endpoint_selection else {
        return EndpointSelection::Unavailable;
    };
    let short = node
        .session_id
        .split('-')
        .next()
        .unwrap_or(&node.session_id);
    let state = read_json(&jobs.join(short).join("state.json"));
    let roster = read_json(roster_path);
    let Some((state, worker)) = state.zip(roster_worker(roster.as_ref(), short)) else {
        return parked(endpoint.endpoint_path.clone());
    };
    let session_matches = string(&state, "sessionId") == Some(node.session_id.as_str())
        && string(worker, "sessionId") == Some(node.session_id.as_str());
    let daemon_owned = string(&state, "backend") == Some("daemon");
    let lifecycle = string(&state, "state")
        .unwrap_or_default()
        .to_ascii_lowercase();
    let lifecycle_ready = !matches!(lifecycle.as_str(), "done" | "concluded" | "killed");
    let status_ready = match string(&state, "status") {
        Some(status) => matches!(status.to_ascii_lowercase().as_str(), "idle" | "busy"),
        None => true,
    };
    let permission_ready = ["permissionState", "permission_status", "waitingFor"]
        .into_iter()
        .filter_map(|key| string(&state, key))
        .all(|value| !normalized(value).contains("permission"));
    let rendezvous = string(worker, "rendezvousSock").map(Path::new);
    let live_pid = worker
        .get("replPid")
        .or_else(|| worker.get("pid"))
        .and_then(Value::as_u64)
        .is_some_and(|pid| Path::new("/proc").join(pid.to_string()).exists());
    let endpoint_matches = rendezvous
        .and_then(|path| path.parent()?.parent())
        .map(|daemon| daemon.join("control.sock") == Path::new(&endpoint.endpoint_path))
        .unwrap_or(false);
    if session_matches
        && daemon_owned
        && lifecycle_ready
        && status_ready
        && permission_ready
        && live_pid
        && rendezvous.is_some_and(Path::exists)
        && endpoint_matches
        && Path::new(&endpoint.endpoint_path).exists()
    {
        EndpointSelection::Available(signal_flow::Available_Data {
            endpoint_path: endpoint.endpoint_path.clone(),
            route_readiness: RouteReadiness::Ready,
        })
    } else {
        parked(endpoint.endpoint_path.clone())
    }
}

fn parked(endpoint_path: String) -> EndpointSelection {
    EndpointSelection::Available(signal_flow::Available_Data {
        endpoint_path,
        route_readiness: RouteReadiness::Parked,
    })
}

fn read_json(path: &Path) -> Option<Value> {
    serde_json::from_slice(&fs::read(path).ok()?).ok()
}

fn roster_worker<'a>(roster: Option<&'a Value>, short: &str) -> Option<&'a Value> {
    roster?.get("workers")?.get(short)
}

fn string<'a>(value: &'a Value, key: &str) -> Option<&'a str> {
    value.get(key)?.as_str()
}

fn normalized(value: &str) -> String {
    value
        .chars()
        .filter(char::is_ascii_alphabetic)
        .collect::<String>()
        .to_ascii_lowercase()
}

#[cfg(test)]
mod tests {
    use super::{
        finalize_claude_start_after_readiness, refresh_at, ClaudeStartPlan, FinalizesClaudeStart,
    };
    use signal_flow::{
        Available_Data, EndpointSelection, FlowLifecycle, FlowNode, HarnessKind, OriginClue,
        RouteReadiness,
    };
    use std::fs;
    use tempfile::tempdir;

    #[test]
    fn claude_start_uses_replacement_prompt_skip_flag_and_one_start_argument() {
        let plan = ClaudeStartPlan {
            system_prompt_file: "/tmp/system-prompt.md".into(),
            startup_skills: vec!["$spirit".into(), "$psyche".into()],
            startup_bundle_file: "/tmp/start-bundle.md".into(),
        };
        let argv = plan.argv().unwrap();
        assert_eq!(argv[0], "claude");
        assert!(argv
            .windows(2)
            .any(|pair| { pair == ["--system-prompt-file", "/tmp/system-prompt.md"] }));
        assert!(argv
            .iter()
            .any(|argument| argument == "--dangerously-skip-permissions"));
        assert_eq!(
            argv.iter()
                .filter(|argument| argument.as_str() == "--")
                .count(),
            1
        );
        assert_eq!(argv.len(), 8);
        assert_eq!(argv[7], "$spirit\n$psyche\nread /tmp/start-bundle.md");
        assert!(plan
            .command()
            .unwrap()
            .get_envs()
            .any(|(key, value)| { key == "CLAUDE_CODE_CHILD_SESSION" && value.is_none() }));
    }

    #[derive(Default)]
    struct OrderedFinalizer(Vec<&'static str>);

    impl FinalizesClaudeStart for OrderedFinalizer {
        type Error = ();

        fn startup_is_ready(&mut self) -> Result<(), Self::Error> {
            self.0.push("ready");
            Ok(())
        }

        fn claim_flow_id(&mut self) -> Result<(), Self::Error> {
            self.0.push("claim");
            Ok(())
        }

        fn set_flow_title(&mut self) -> Result<(), Self::Error> {
            self.0.push("title");
            Ok(())
        }
    }

    #[test]
    fn claude_claim_and_title_follow_startup_readiness() {
        let mut finalizer = OrderedFinalizer::default();
        finalize_claude_start_after_readiness(&mut finalizer).unwrap();
        assert_eq!(finalizer.0, ["ready", "claim", "title"]);
    }

    #[test]
    fn permission_wait_parks_an_otherwise_live_daemon_session() {
        let directory = tempdir().unwrap();
        let jobs = directory.path().join("jobs");
        let daemon = directory.path().join("daemon");
        let short = "da1e3f9d";
        fs::create_dir_all(jobs.join(short)).unwrap();
        fs::create_dir_all(daemon.join("rv")).unwrap();
        fs::write(daemon.join("control.sock"), "fixture").unwrap();
        fs::write(daemon.join("rv").join(format!("{short}.sock")), "fixture").unwrap();
        fs::write(
            jobs.join(short).join("state.json"),
            r#"{"sessionId":"da1e3f9d-full","backend":"daemon","state":"blocked","waitingFor":"permission prompt"}"#,
        ).unwrap();
        fs::write(
            daemon.join("roster.json"),
            format!(r#"{{"workers":{{"{short}":{{"sessionId":"da1e3f9d-full","rendezvousSock":"{}","replPid":{}}}}}}}"#, daemon.join("rv").join(format!("{short}.sock")).display(), std::process::id()),
        ).unwrap();
        let node = FlowNode {
            flow_id: "da1e3f".into(),
            session_id: "da1e3f9d-full".into(),
            harness_kind: HarnessKind::Claude,
            endpoint_selection: EndpointSelection::Available(Available_Data {
                endpoint_path: daemon.join("control.sock").display().to_string(),
                route_readiness: RouteReadiness::Ready,
            }),
            herdr_route_selection: signal_flow::HerdrRouteSelection::Unavailable,
            origin_clue: OriginClue {
                flow_id: "da1e3f".into(),
                session_id: "da1e3f9d-full".into(),
                turn_id: "unavailable".into(),
            },
            flow_lifecycle: FlowLifecycle::Active,
        };
        assert!(matches!(
            refresh_at(&node, &jobs, &daemon.join("roster.json")),
            EndpointSelection::Available(Available_Data {
                route_readiness: RouteReadiness::Parked,
                ..
            })
        ));
    }
}
