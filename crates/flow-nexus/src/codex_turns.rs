//! Codex turns: whether an Awake Codex seat is in a turn, read only from
//! its app-server's own events for its thread, and the settlement a turn's
//! end allows.
//!
//! A Codex seat runs no hook, so it never reports `Stopped`; its turn ends
//! are the app-server's. Every connection Flow keeps on a thread (the
//! launch's opening connection, then the Nexus's responder) is subscribed
//! to that thread's events, and each event it brings for that thread is
//! observed here: `turn/started` puts the thread in that turn,
//! `turn/completed` of the turn it is in brings it to rest, and
//! `thread/status/changed` to `active` or `idle` says the same where no
//! turn is named. The `thread/resume` that subscribes a responder answers
//! the thread's status as the app-server holds it then, so a turn that
//! completed while no responder was subscribed (during the launch's
//! registration or Bind, or before a Nexus restart) is not missed. A thread
//! with no such evidence is never taken for resting.
//!
//! A thread's rest settles its flow as a `Stopped` report settles a Claude
//! flow (`SleepsAndWakesSeats::settle_stopped`): the oldest queued request
//! is handed to its thread, which is then `Delivered`, so the next request
//! waits for a turn that ends after it; with nothing queued the seat stays
//! Awake in its pane. Settlement takes
//! the dispatch gate and reads the thread's turn under it, so a completion
//! seen twice, or by two connections, types once.
//!
//! The state is memory only. A Nexus that starts again reads each live
//! thread's status from its responder's `thread/resume`.

use crate::RunningNexus;
use crate::store::address_hold::HoldsLaunchAddress;
use crate::store::metaflow::SleepsMetaflows;
use crate::store::{NamesLiveFlow, ReadsFlowRows};
use crate::waking::{HoldsWakingMetaflows, NotesStopsAtConfirmation, SleepsAndWakesSeats};
use signal_flow::{Address, FlowId, FlowNode, HarnessKind};
use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;

/// Where a thread stands, as its events show it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ThreadTurn {
    /// No event has shown the thread in a turn or at rest.
    Unknown,
    /// In a turn: the one named, or one whose id no event has given.
    Running(Option<String>),
    /// Flow typed work into the seat and no turn has started since.
    Delivered,
    /// Its last turn ended and no other has started.
    Resting,
}

/// One app-server event of a thread, as far as its turns go.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TurnEvent {
    Started(String),
    Completed(String),
    /// A `thread/status/changed` notification.
    Status(ThreadStatus),
    /// The status a `thread/resume` answered: the thread as it is now.
    Resumed(ThreadStatus),
}

/// A thread's status, as the app-server names it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ThreadStatus {
    Idle,
    Active,
    /// Not loaded, or a system error.
    Unavailable,
}

#[derive(Debug, Clone)]
struct ThreadState {
    turn: ThreadTurn,
    completed: VecDeque<String>,
}

impl Default for ThreadState {
    fn default() -> Self {
        Self {
            turn: ThreadTurn::Unknown,
            completed: VecDeque::new(),
        }
    }
}

/// Every Codex thread's turn, keyed by thread id.
#[derive(Debug)]
pub struct CodexTurns {
    threads: Mutex<HashMap<String, ThreadState>>,
}

impl Default for CodexTurns {
    fn default() -> Self {
        Self {
            threads: Mutex::new(HashMap::new()),
        }
    }
}

pub trait ReadsTurnEvent {
    /// The turn event a JSON-RPC notification carries for `thread`; None
    /// for any other message, or one about another thread.
    fn turn_event(&self, thread: &str) -> Option<TurnEvent>;
}

impl ReadsTurnEvent for serde_json::Value {
    fn turn_event(&self, thread: &str) -> Option<TurnEvent> {
        if self.get("id").is_some() {
            return None;
        }
        let params = self.get("params")?;
        if params.get("threadId").and_then(serde_json::Value::as_str) != Some(thread) {
            return None;
        }
        let turn = || {
            params
                .pointer("/turn/id")
                .and_then(serde_json::Value::as_str)
                .filter(|turn| !turn.is_empty())
                .map(str::to_owned)
        };
        match self.get("method").and_then(serde_json::Value::as_str)? {
            "turn/started" => turn().map(TurnEvent::Started),
            "turn/completed" => turn().map(TurnEvent::Completed),
            "thread/status/changed" => params
                .get("status")
                .and_then(|status| status.thread_status())
                .map(TurnEvent::Status),
            _ => None,
        }
    }
}

pub trait ReadsThreadStatus {
    /// The status a ThreadStatus value (`{ "type": "idle" }`) names.
    fn thread_status(&self) -> Option<ThreadStatus>;
}

impl ReadsThreadStatus for serde_json::Value {
    fn thread_status(&self) -> Option<ThreadStatus> {
        match self.get("type").and_then(serde_json::Value::as_str)? {
            "idle" => Some(ThreadStatus::Idle),
            "active" => Some(ThreadStatus::Active),
            "notLoaded" | "systemError" => Some(ThreadStatus::Unavailable),
            _ => None,
        }
    }
}

pub trait TracksCodexTurns {
    /// Applies one event; true when it brought the thread to rest.
    fn observe(&self, thread: &str, event: TurnEvent, remembered_turns: usize) -> bool;
    /// Flow typed work into the seat on `thread`.
    fn delivered(&self, thread: &str);
    fn turn(&self, thread: &str) -> ThreadTurn;
}

impl TracksCodexTurns for CodexTurns {
    fn observe(&self, thread: &str, event: TurnEvent, remembered_turns: usize) -> bool {
        let mut threads = self
            .threads
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let state = threads.entry(thread.to_owned()).or_default();
        let was_resting = state.turn == ThreadTurn::Resting;
        match event {
            TurnEvent::Started(turn) => {
                // A start read after its own completion changes nothing.
                if !state.completed.contains(&turn) {
                    state.turn = ThreadTurn::Running(Some(turn));
                }
            }
            TurnEvent::Completed(turn) => {
                if state.completed.contains(&turn) {
                    return false;
                }
                state.completed.push_back(turn.clone());
                while state.completed.len() > remembered_turns {
                    state.completed.pop_front();
                }
                match &state.turn {
                    // Another turn is the one the thread is in: an earlier
                    // turn's late end does not bring it to rest.
                    ThreadTurn::Running(Some(running)) if *running != turn => {}
                    _ => state.turn = ThreadTurn::Resting,
                }
            }
            TurnEvent::Status(ThreadStatus::Idle) => {
                // A named turn ends with its own completion, and work Flow
                // typed with the turn it starts: an idle sent before either
                // began is not read as its end.
                if !matches!(
                    state.turn,
                    ThreadTurn::Running(Some(_)) | ThreadTurn::Delivered
                ) {
                    state.turn = ThreadTurn::Resting;
                }
            }
            TurnEvent::Resumed(ThreadStatus::Idle) => {
                // Work typed and not yet begun is not read as ended.
                if state.turn != ThreadTurn::Delivered {
                    state.turn = ThreadTurn::Resting;
                }
            }
            TurnEvent::Status(ThreadStatus::Active) | TurnEvent::Resumed(ThreadStatus::Active) => {
                if !matches!(state.turn, ThreadTurn::Running(_)) {
                    state.turn = ThreadTurn::Running(None);
                }
            }
            TurnEvent::Status(ThreadStatus::Unavailable)
            | TurnEvent::Resumed(ThreadStatus::Unavailable) => state.turn = ThreadTurn::Unknown,
        }
        !was_resting && state.turn == ThreadTurn::Resting
    }

    fn delivered(&self, thread: &str) {
        self.threads
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .entry(thread.to_owned())
            .or_default()
            .turn = ThreadTurn::Delivered;
    }

    fn turn(&self, thread: &str) -> ThreadTurn {
        self.threads
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(thread)
            .map(|state| state.turn.clone())
            .unwrap_or(ThreadTurn::Unknown)
    }
}

#[cfg(test)]
mod tests {
    use super::{CodexTurns, TracksCodexTurns, TurnEvent};

    #[test]
    fn the_next_event_uses_the_current_remembered_turn_limit() {
        let turns = CodexTurns::default();
        assert!(turns.observe("thread", TurnEvent::Completed("one".into()), 8));
        assert!(!turns.observe("thread", TurnEvent::Completed("two".into()), 8));
        assert!(!turns.observe("thread", TurnEvent::Completed("three".into()), 1));
        let threads = turns.threads.lock().expect("turn state");
        assert_eq!(
            threads
                .get("thread")
                .expect("thread")
                .completed
                .iter()
                .cloned()
                .collect::<Vec<_>>(),
            vec!["three".to_owned()]
        );
    }
}

/// The settlement a Codex thread's rest allows.
pub trait SettlesRestingCodexSeats {
    /// Off the dispatch gate, from a responder: applies the events under
    /// the gate, then settles the Codex flow on the thread once if they
    /// left it resting with work queued.
    fn codex_events(&self, thread: &str, events: Vec<TurnEvent>);
    /// Under the dispatch gate, as a launch holding it reads its own
    /// connection: applies the events, and notes the flow on the thread to
    /// be settled once the gate is given back when they left it resting.
    fn observe_codex_events(&self, thread: &str, events: Vec<TurnEvent>);
    /// The live Codex flow on `thread`, when there is exactly one.
    fn codex_flow_on(&self, thread: &str) -> Option<FlowNode>;
    /// Whether the flow is a Codex flow whose thread's events show it
    /// resting.
    fn codex_resting(&self, flow_id: &str) -> bool;
    /// Whether the flow is a Codex flow whose thread's events show it in a
    /// turn, or holding work Flow typed that has not begun.
    fn codex_in_turn(&self, flow_id: &str) -> bool;
    /// The Codex flow a metaflow is Awake as, when its launch is confirmed,
    /// no wake holds it, work is queued for it and its thread rests: the
    /// flow whose settlement hands the oldest queued request over.
    fn resting_codex_seat(&self, address: &Address) -> Option<FlowId>;
}

impl SettlesRestingCodexSeats for RunningNexus {
    fn codex_events(&self, thread: &str, events: Vec<TurnEvent>) {
        if events.is_empty() {
            return;
        }
        let remembered_turns = match self.configured_use() {
            Ok(configured) => configured.snapshot.limits.remembered_turns as usize,
            _ => return,
        };
        let seat = {
            let _turn = self
                .dispatch_gate
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            for event in events {
                self.codex_turns.observe(thread, event, remembered_turns);
            }
            self.codex_flow_on(thread).and_then(|node| {
                let metaflow = self.store.metaflow_awake_as(&node.flow_id).ok()??;
                self.resting_codex_seat(&metaflow.address)
            })
        };
        // Settled as a Stop is: off the gate, which it takes, reading the
        // thread's turn again under it.
        if let Some(flow_id) = seat {
            self.settle_stopped(&flow_id);
        }
    }

    fn observe_codex_events(&self, thread: &str, events: Vec<TurnEvent>) {
        let remembered_turns = match self.configured_use() {
            Ok(configured) => configured.snapshot.limits.remembered_turns as usize,
            _ => return,
        };
        for event in events {
            self.codex_turns.observe(thread, event, remembered_turns);
        }
        if self.codex_turns.turn(thread) == ThreadTurn::Resting
            && let Some(node) = self.codex_flow_on(thread)
            && matches!(self.store.address_hold(&node.flow_id), Ok(None))
            && matches!(self.store.metaflow_awake_as(&node.flow_id), Ok(Some(_)))
        {
            // An unconfirmed seat is noted by its confirmation instead.
            self.stopped_at_confirmation.note(&node.flow_id);
        }
    }

    fn codex_flow_on(&self, thread: &str) -> Option<FlowNode> {
        let nodes = self.store.flow_nodes().ok()?;
        let mut live = nodes.into_iter().filter(|node| {
            node.harness_kind == HarnessKind::Codex
                && !thread.is_empty()
                && node.session_id == thread
                && node.flow_lifecycle.is_live()
        });
        let node = live.next()?;
        live.next().is_none().then_some(node)
    }

    fn codex_resting(&self, flow_id: &str) -> bool {
        matches!(
            self.store.flow_node(flow_id),
            Ok(Some(node)) if node.harness_kind == HarnessKind::Codex
                && !node.session_id.is_empty()
                && self.codex_turns.turn(&node.session_id) == ThreadTurn::Resting
        )
    }

    fn codex_in_turn(&self, flow_id: &str) -> bool {
        matches!(
            self.store.flow_node(flow_id),
            Ok(Some(node)) if node.harness_kind == HarnessKind::Codex
                && matches!(
                    self.codex_turns.turn(&node.session_id),
                    ThreadTurn::Running(_) | ThreadTurn::Delivered
                )
        )
    }

    fn resting_codex_seat(&self, address: &Address) -> Option<FlowId> {
        let metaflow = self.store.metaflow(address).ok()??;
        let flow_ethos::State::Awake(flow_id) = metaflow.state else {
            return None;
        };
        (!metaflow.queue.is_empty()
            && !self.waking.is_waking(address)
            && matches!(self.store.address_hold(&flow_id), Ok(None))
            && self.codex_resting(&flow_id))
        .then_some(flow_id)
    }
}
