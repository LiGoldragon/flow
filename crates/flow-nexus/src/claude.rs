//! Dynamic readiness projection for daemon-owned Claude sessions.

use std::{
    fs,
    path::{Path, PathBuf},
};

use serde_json::Value;
use signal_flow::{EndpointSelection, FlowNode, HarnessKind, RouteReadiness};

/// The Claude daemon's own records under Claude's home: one `state.json`
/// per job, and the roster of its workers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClaudeDaemon {
    pub jobs: PathBuf,
    pub roster: PathBuf,
}

/// Reads whether a daemon-owned Claude session can take a prompt now.
pub trait ProjectsClaudeReadiness: Sized {
    /// The daemon records under this Claude home.
    fn under(claude_home: &Path) -> Self;

    /// The node with its endpoint's readiness read afresh; a node of
    /// another harness is returned as it is.
    fn refreshed(&self, node: FlowNode) -> FlowNode;

    /// The endpoint selection the daemon's records show for this node.
    fn readiness_of(&self, node: &FlowNode) -> EndpointSelection;
}

impl ProjectsClaudeReadiness for ClaudeDaemon {
    fn under(claude_home: &Path) -> Self {
        Self {
            jobs: claude_home.join("jobs"),
            roster: claude_home.join("daemon/roster.json"),
        }
    }

    fn refreshed(&self, mut node: FlowNode) -> FlowNode {
        if node.harness_kind != HarnessKind::Claude {
            return node;
        }
        node.endpoint_selection = self.readiness_of(&node);
        node
    }

    fn readiness_of(&self, node: &FlowNode) -> EndpointSelection {
        let EndpointSelection::Available(endpoint) = &node.endpoint_selection else {
            return EndpointSelection::Unavailable;
        };
        let short = node
            .session_id
            .split('-')
            .next()
            .unwrap_or(&node.session_id);
        let state = self.jobs.join(short).join("state.json").read_json();
        let roster = self.roster.read_json();
        let Some((state, worker)) =
            state.zip(roster.as_ref().and_then(|roster| roster.worker(short)))
        else {
            return endpoint.endpoint_path.clone().parked();
        };
        let session_matches = state.string_at("sessionId") == Some(node.session_id.as_str())
            && worker.string_at("sessionId") == Some(node.session_id.as_str());
        let daemon_owned = state.string_at("backend") == Some("daemon");
        let lifecycle = state
            .string_at("state")
            .unwrap_or_default()
            .to_ascii_lowercase();
        let lifecycle_ready = !matches!(lifecycle.as_str(), "done" | "concluded" | "killed");
        let status_ready = match state.string_at("status") {
            Some(status) => matches!(status.to_ascii_lowercase().as_str(), "idle" | "busy"),
            None => true,
        };
        let permission_ready = ["permissionState", "permission_status", "waitingFor"]
            .into_iter()
            .filter_map(|key| state.string_at(key))
            .all(|value| !value.letters_lowercased().contains("permission"));
        let rendezvous = worker.string_at("rendezvousSock").map(Path::new);
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
            endpoint.endpoint_path.clone().parked()
        }
    }
}

/// An endpoint path offered but not ready for a prompt.
trait ParksEndpoint {
    fn parked(self) -> EndpointSelection;
}

impl ParksEndpoint for String {
    fn parked(self) -> EndpointSelection {
        EndpointSelection::Available(signal_flow::Available_Data {
            endpoint_path: self,
            route_readiness: RouteReadiness::Parked,
        })
    }
}

/// A file holding one JSON document.
trait ReadsJsonFile {
    fn read_json(&self) -> Option<Value>;
}

impl ReadsJsonFile for Path {
    fn read_json(&self) -> Option<Value> {
        serde_json::from_slice(&fs::read(self).ok()?).ok()
    }
}

/// The daemon's JSON records, read by field.
trait ReadsDaemonRecord {
    fn string_at(&self, key: &str) -> Option<&str>;
    /// The roster's worker for a session's short form.
    fn worker(&self, short: &str) -> Option<&Value>;
}

impl ReadsDaemonRecord for Value {
    fn string_at(&self, key: &str) -> Option<&str> {
        self.get(key)?.as_str()
    }

    fn worker(&self, short: &str) -> Option<&Value> {
        self.get("workers")?.get(short)
    }
}

/// A state value compared by its letters alone.
trait NormalizesState {
    fn letters_lowercased(&self) -> String;
}

impl NormalizesState for str {
    fn letters_lowercased(&self) -> String {
        self.chars()
            .filter(char::is_ascii_alphabetic)
            .collect::<String>()
            .to_ascii_lowercase()
    }
}

#[cfg(test)]
mod tests {
    use super::{ClaudeDaemon, ProjectsClaudeReadiness};
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
            ClaudeDaemon {
                jobs: jobs.clone(),
                roster: daemon.join("roster.json"),
            }
            .readiness_of(&node),
            EndpointSelection::Available(Available_Data {
                route_readiness: RouteReadiness::Parked,
                ..
            })
        ));
    }
}
