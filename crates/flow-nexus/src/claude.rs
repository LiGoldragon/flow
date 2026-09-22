//! Dynamic readiness projection for daemon-owned Claude sessions.

use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
};

use serde_json::Value;
use signal_flow::{EndpointSelection, FlowNode, HarnessKind, RouteReadiness};

const JOBS: &str = "/home/li/.claude/jobs";
const ROSTER: &str = "/home/li/.claude/daemon/roster.json";

/// The complete, inspectable native invocation for one Claude Start.
///
/// Claude's system-prompt file replaces the vendor instruction body, so this
/// plan deliberately carries a caller-supplied bundle rather than pretending
/// the stock prompt remains in effect. The startup text remains one argument:
/// skill commands followed by exactly one bundle read command.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClaudeStartPlan {
    pub system_prompt_file: PathBuf,
    pub startup_skills: Vec<String>,
    pub startup_bundle_file: PathBuf,
}

#[derive(Debug, thiserror::Error, Eq, PartialEq)]
pub enum ClaudeStartPlanError {
    #[error("a Claude Start requires at least one startup skill command")]
    MissingStartupSkill,
    #[error("a Claude startup skill command must be one line")]
    MultilineStartupSkill,
    #[error("the Claude system-prompt path must not be empty")]
    MissingSystemPrompt,
    #[error("the Claude startup bundle path must not be empty")]
    MissingStartupBundle,
}

impl ClaudeStartPlan {
    pub fn startup_argument(&self) -> Result<String, ClaudeStartPlanError> {
        if self.system_prompt_file.as_os_str().is_empty() {
            return Err(ClaudeStartPlanError::MissingSystemPrompt);
        }
        if self.startup_bundle_file.as_os_str().is_empty() {
            return Err(ClaudeStartPlanError::MissingStartupBundle);
        }
        if self.startup_skills.is_empty() {
            return Err(ClaudeStartPlanError::MissingStartupSkill);
        }
        if self
            .startup_skills
            .iter()
            .any(|skill| skill.contains(['\n', '\r']))
        {
            return Err(ClaudeStartPlanError::MultilineStartupSkill);
        }
        let mut lines = self.startup_skills.clone();
        lines.push(format!("read {}", self.startup_bundle_file.display()));
        Ok(lines.join("\n"))
    }

    pub fn argv(&self) -> Result<Vec<String>, ClaudeStartPlanError> {
        Ok(vec![
            "claude".into(),
            "--bg".into(),
            "--remote-control".into(),
            "--system-prompt-file".into(),
            self.system_prompt_file.display().to_string(),
            "--dangerously-skip-permissions".into(),
            "--".into(),
            self.startup_argument()?,
        ])
    }

    /// Builds the process with inherited child-session state removed. Callers
    /// must complete their startup readiness checks before claiming identity
    /// or setting the thread title.
    pub fn command(&self) -> Result<Command, ClaudeStartPlanError> {
        let argv = self.argv()?;
        let mut command = Command::new(&argv[0]);
        command.args(&argv[1..]);
        command.env_remove("CLAUDE_CODE_CHILD_SESSION");
        Ok(command)
    }
}

/// Identity publication is intentionally downstream of native readiness.
pub trait FinalizesClaudeStart {
    type Error;

    fn startup_is_ready(&mut self) -> Result<(), Self::Error>;
    fn claim_flow_id(&mut self) -> Result<(), Self::Error>;
    fn set_flow_title(&mut self) -> Result<(), Self::Error>;
}

pub fn finalize_claude_start_after_readiness<T: FinalizesClaudeStart>(
    start: &mut T,
) -> Result<(), T::Error> {
    start.startup_is_ready()?;
    start.claim_flow_id()?;
    start.set_flow_title()
}

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
    use super::refresh_at;
    use signal_flow::{
        Available_Data, EndpointSelection, FlowLifecycle, FlowNode, HarnessKind, OriginClue,
        RouteReadiness,
    };
    use std::fs;
    use tempfile::tempdir;

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
