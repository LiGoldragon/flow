//! Native turn completion drives one queued handover.
//!
//! A Stop hook only announces which native turn is about to end.  Flow
//! commits that request, then a filesystem subscription wakes the observer
//! when the transcript moves.  Only the matching Codex `task_complete`
//! record completes the request.  Completion is retained durably, so a
//! repeated hook or filesystem event cannot drain twice.

use crate::store::AnnouncesLaunchChanges;
use crate::store::ReadsFlowRows;
use crate::store::turn_end::{KeepsTurnEnds, TurnEndQueue, TurnEndRecord};
use crate::waking::SleepsAndWakesSeats;
use crate::{HoldsStopRequest, RunningNexus};
use notify::Watcher;
use signal_flow::{Response, TurnEndRejection, TurnEndRequest};
use std::collections::HashMap;
use std::io::BufRead;
use std::path::{Path, PathBuf};

pub trait HandlesTurnEnds {
    fn queue_turn_end(&self, request: TurnEndRequest) -> Response;
    /// Completes every pending request whose transcript already proves its
    /// exact native turn ended. Returns the flows newly completed.
    fn complete_visible_turn_ends(&self) -> Vec<String>;
    /// Runs for the Nexus lifetime, awakened only by a durable enqueue or a
    /// filesystem notification concerning a watched transcript.
    fn observe_turn_ends(&self);
}

impl HandlesTurnEnds for RunningNexus {
    fn queue_turn_end(&self, mut request: TurnEndRequest) -> Response {
        if request.turn_id.is_empty()
            || request.turn_id.trim() != request.turn_id
            || request.turn_id.chars().any(char::is_control)
        {
            return Response::TurnEndRejected(TurnEndRejection::InvalidTurnId);
        }
        let flow_id = match self.store.flow_for_session(&request.session_id) {
            Ok(Some(flow_id)) => flow_id,
            Ok(None) => return Response::TurnEndRejected(TurnEndRejection::UnknownSession),
            Err(_) => return Response::TurnEndRejected(TurnEndRejection::QueueRefused),
        };
        let Some(path_text) = request.transcript_path_option.clone() else {
            return Response::TurnEndRejected(TurnEndRejection::InvalidTranscriptPath);
        };
        let path = Path::new(&path_text);
        let canonical = match path.canonicalize() {
            Ok(path) if path.is_file() => path,
            _ => return Response::TurnEndRejected(TurnEndRejection::InvalidTranscriptPath),
        };
        if !canonical.is_absolute()
            || !canonical
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.ends_with(".jsonl") && name.contains(&request.session_id))
        {
            return Response::TurnEndRejected(TurnEndRejection::SessionMismatch);
        }
        request.transcript_path_option = Some(canonical.to_string_lossy().into_owned());
        match self.store.queue_turn_end(&flow_id, request.clone()) {
            Ok(TurnEndQueue::Queued) => {
                self.store.launch_changes.announce();
                Response::TurnEndQueued(request)
            }
            Ok(TurnEndQueue::AlreadyQueued) => Response::TurnEndQueued(request),
            Ok(TurnEndQueue::Conflict) => {
                Response::TurnEndRejected(TurnEndRejection::SessionMismatch)
            }
            Err(_) => Response::TurnEndRejected(TurnEndRejection::QueueRefused),
        }
    }

    fn complete_visible_turn_ends(&self) -> Vec<String> {
        let pending = match self.store.pending_turn_ends() {
            Ok(pending) => pending,
            Err(_) => return Vec::new(),
        };
        let mut completed = Vec::new();
        for record in pending {
            if !transcript_completes(&record) {
                continue;
            }
            match self.store.complete_turn_end(&record.request) {
                Ok(true) => {
                    self.settle_stopped(&record.flow_id);
                    completed.push(record.flow_id);
                }
                Ok(false) => {}
                Err(error) => {
                    eprintln!("flow-nexus: native turn completion was not recorded: {error}")
                }
            }
        }
        completed
    }

    fn observe_turn_ends(&self) {
        let changes = self.store.launch_changes.clone();
        let mut watches: HashMap<PathBuf, notify::RecommendedWatcher> = HashMap::new();
        while !self.stopping.requested() {
            let seen = changes.current();
            let pending = self.store.pending_turn_ends().unwrap_or_default();
            for record in &pending {
                let Some(path) = record
                    .request
                    .transcript_path_option
                    .as_deref()
                    .map(PathBuf::from)
                else {
                    continue;
                };
                if watches.contains_key(&path) {
                    continue;
                }
                let announced = changes.clone();
                let mut watcher = match notify::recommended_watcher(
                    move |event: notify::Result<notify::Event>| {
                        if event.is_ok() {
                            announced.announce();
                        }
                    },
                ) {
                    Ok(watcher) => watcher,
                    Err(error) => {
                        eprintln!("flow-nexus: native turn watch refused: {error}");
                        continue;
                    }
                };
                if let Err(error) = watcher.watch(&path, notify::RecursiveMode::NonRecursive) {
                    eprintln!(
                        "flow-nexus: native transcript {} was not watched: {error}",
                        path.display()
                    );
                    continue;
                }
                watches.insert(path, watcher);
            }
            let _ = self.complete_visible_turn_ends();
            if self.stopping.requested() {
                return;
            }
            changes.after(seen);
            let pending_paths = self
                .store
                .pending_turn_ends()
                .unwrap_or_default()
                .into_iter()
                .filter_map(|record| record.request.transcript_path_option.map(PathBuf::from))
                .collect::<Vec<_>>();
            watches.retain(|path, _| pending_paths.contains(path));
        }
    }
}

fn transcript_completes(record: &TurnEndRecord) -> bool {
    let Some(path) = record.request.transcript_path_option.as_deref() else {
        return false;
    };
    let Ok(file) = std::fs::File::open(path) else {
        return false;
    };
    std::io::BufReader::new(file)
        .lines()
        .map_while(Result::ok)
        .filter_map(|line| serde_json::from_str::<serde_json::Value>(&line).ok())
        .any(|row| {
            row.get("type").and_then(serde_json::Value::as_str) == Some("event_msg")
                && row
                    .pointer("/payload/type")
                    .and_then(serde_json::Value::as_str)
                    == Some("task_complete")
                && row
                    .pointer("/payload/turn_id")
                    .and_then(serde_json::Value::as_str)
                    == Some(record.request.turn_id.as_str())
        })
}

#[cfg(test)]
mod parser_tests {
    use super::transcript_completes;
    use crate::store::turn_end::{TurnEndRecord, TurnEndState};
    use signal_flow::TurnEndRequest;
    use std::io::Write;

    #[test]
    fn only_the_named_native_turns_completion_matches() {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        writeln!(
            file,
            "{}",
            serde_json::json!({"type":"event_msg","payload":{
                "type":"task_complete","turn_id":"unrelated"}})
        )
        .unwrap();
        let mut record = TurnEndRecord {
            flow_id: "abcdef".into(),
            request: TurnEndRequest {
                session_id: "session".into(),
                turn_id: "wanted".into(),
                transcript_path_option: Some(file.path().to_string_lossy().into_owned()),
            },
            state: TurnEndState::Pending,
        };
        assert!(!transcript_completes(&record));
        writeln!(
            file,
            "{}",
            serde_json::json!({"type":"event_msg","payload":{
                "type":"task_complete","turn_id":"wanted"}})
        )
        .unwrap();
        assert!(transcript_completes(&record));
        record.request.turn_id = "unrelated-two".into();
        assert!(!transcript_completes(&record));
    }
}
