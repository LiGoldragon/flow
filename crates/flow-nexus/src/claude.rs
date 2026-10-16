//! Dynamic readiness projection for daemon-owned Claude sessions.

use std::{
    fs,
    io::{Read, Seek, SeekFrom},
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

/// A Claude session's native transcript, read for the size of the context
/// its last turn carried: the last main-thread usage entry's input tokens,
/// cache reads and cache writes together. Read once, when the flow's hook
/// reports `Stopped`; nothing watches the file.
pub trait MeasuresClaudeContext {
    /// Under a Claude transcript root (`projects/`): the one transcript of
    /// this session, `<project>/<session>.jsonl`.
    fn session_transcript(&self, session_id: &str) -> Option<PathBuf>;
    /// In a transcript: the tokens of the context its last turn carried.
    fn context_tokens(&self) -> Option<i64>;
}

/// How much of a transcript's end is read first for its last usage entry.
const CONTEXT_TAIL_BYTES: u64 = 4 * 1024 * 1024;

impl MeasuresClaudeContext for Path {
    fn session_transcript(&self, session_id: &str) -> Option<PathBuf> {
        if session_id.is_empty() || session_id.contains('/') {
            return None;
        }
        let name = format!("{session_id}.jsonl");
        let mut found = fs::read_dir(self)
            .ok()?
            .filter_map(Result::ok)
            .map(|project| project.path().join(&name))
            .filter(|path| path.is_file());
        let transcript = found.next()?;
        found.next().is_none().then_some(transcript)
    }

    fn context_tokens(&self) -> Option<i64> {
        let mut file = fs::File::open(self).ok()?;
        let length = file.metadata().ok()?.len();
        let start = length.saturating_sub(CONTEXT_TAIL_BYTES);
        let mut tail = Vec::new();
        file.seek(SeekFrom::Start(start)).ok()?;
        file.read_to_end(&mut tail).ok()?;
        match String::from_utf8_lossy(&tail).last_context_tokens() {
            Some(tokens) => Some(tokens),
            None if start > 0 => {
                String::from_utf8_lossy(&fs::read(self).ok()?).last_context_tokens()
            }
            None => None,
        }
    }
}

/// Transcript lines, read from the last for a main-thread usage entry.
trait ReadsUsageLines {
    fn last_context_tokens(&self) -> Option<i64>;
}

impl ReadsUsageLines for str {
    fn last_context_tokens(&self) -> Option<i64> {
        self.lines().rev().find_map(|line| {
            let entry: Value = serde_json::from_str(line).ok()?;
            if entry.get("isSidechain").and_then(Value::as_bool) == Some(true) {
                return None;
            }
            let usage = entry.get("message")?.get("usage")?;
            let tokens = |key: &str| usage.get(key).and_then(Value::as_i64).unwrap_or(0);
            usage.get("input_tokens")?;
            Some(
                tokens("input_tokens")
                    + tokens("cache_read_input_tokens")
                    + tokens("cache_creation_input_tokens"),
            )
        })
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
    use super::{ClaudeDaemon, MeasuresClaudeContext, ProjectsClaudeReadiness};
    use signal_flow::{
        Available_Data, EndpointSelection, FlowLifecycle, FlowNode, FlowOrigin, HarnessKind,
        Origin, RouteReadiness,
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
            origin: Origin::Flow(FlowOrigin {
                flow_id: "da1e3f".into(),
                session_id: "da1e3f9d-full".into(),
            }),
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

    /// The context is the last main-thread turn's input, cache reads and
    /// cache writes; a subagent's turn and a line without usage are passed
    /// over, and the session's transcript is found under its project.
    #[test]
    fn the_context_size_is_the_last_main_thread_usage_of_the_session_transcript() {
        let directory = tempdir().unwrap();
        let project = directory.path().join("-home-li-primary");
        fs::create_dir_all(&project).unwrap();
        let transcript = project.join("da1e3f9d-full.jsonl");
        fs::write(
            &transcript,
            concat!(
                r#"{"type":"assistant","message":{"usage":{"input_tokens":5,"cache_read_input_tokens":1000,"cache_creation_input_tokens":20,"output_tokens":9}}}"#,
                "\n",
                r#"{"type":"assistant","message":{"usage":{"input_tokens":2,"cache_read_input_tokens":150385,"cache_creation_input_tokens":575,"output_tokens":116}}}"#,
                "\n",
                r#"{"type":"assistant","isSidechain":true,"message":{"usage":{"input_tokens":1,"cache_read_input_tokens":7,"cache_creation_input_tokens":0}}}"#,
                "\n",
                r#"{"type":"user","message":{"content":"thanks"}}"#,
                "\n",
            ),
        )
        .unwrap();
        assert_eq!(
            directory.path().session_transcript("da1e3f9d-full"),
            Some(transcript.clone())
        );
        assert_eq!(directory.path().session_transcript("absent"), None);
        assert_eq!(transcript.context_tokens(), Some(2 + 150_385 + 575));
        let empty = project.join("empty.jsonl");
        fs::write(&empty, "{\"type\":\"user\"}\n").unwrap();
        assert_eq!(empty.context_tokens(), None);
    }
}
