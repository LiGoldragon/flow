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
//! A thread's coming to rest settles its flow as a `Stopped` report settles
//! a Claude flow (`SleepsAndWakesSeats::settle_stopped`): the oldest queued
//! request is handed over, and the thread is then `Delivered`, so the next
//! request waits for a turn that ends after it; with nothing queued, an
//! ordered replacement or a due refresh is carried out, a Maintained seat
//! stays Awake in its pane, an OnDemand seat sleeps. Settlement takes the
//! dispatch gate and reads the thread's turn under it, so a completion seen
//! twice, or by two connections, types once.
//!
//! The state is memory only. A Nexus that starts again reads each live
//! thread's status from its responder's `thread/resume`. The first
//! authoritative `Resumed(Idle)` a thread shows after a Nexus start settles
//! its seat once; a later one, after a reconnect, settles nothing unless a
//! turn was seen running in between.

use crate::RunningNexus;
use crate::store::address_hold::HoldsLaunchAddress;
use crate::store::metaflow::SleepsMetaflows;
use crate::store::{NamesLiveFlow, ReadsFlowRows};
use crate::turn_ends::RecordsTurnEnd;
use crate::waking::{AtStop, HoldsWakingMetaflows, NotesStopsAtConfirmation, SleepsAndWakesSeats};
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

/// What one event did to a thread: whether it brought the thread to rest,
/// and whether it ended a turn not seen ending before, a turn end
/// `Observe.TurnEnds` sends.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TurnObservation {
    pub rested: bool,
    pub ended: bool,
}

/// How many completed turns of a thread are kept to recognise a repeat.
const REMEMBERED_TURNS: usize = 64;

#[derive(Debug, Clone)]
struct ThreadState {
    turn: ThreadTurn,
    completed: VecDeque<String>,
    /// Whether this Nexus process has read a `Resumed(Idle)` of the thread:
    /// the first one after a start settles, a later one alone does not.
    resumed: bool,
}

impl Default for ThreadState {
    fn default() -> Self {
        Self {
            turn: ThreadTurn::Unknown,
            completed: VecDeque::new(),
            resumed: false,
        }
    }
}

/// Every Codex thread's turn, keyed by thread id.
#[derive(Debug, Default)]
pub struct CodexTurns {
    threads: Mutex<HashMap<String, ThreadState>>,
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
    /// Applies one event: whether it brought the thread to rest, and
    /// whether it ended a turn for the first time.
    fn observe(&self, thread: &str, event: TurnEvent) -> TurnObservation;
    /// Flow typed work into the seat on `thread`.
    fn delivered(&self, thread: &str);
    fn turn(&self, thread: &str) -> ThreadTurn;
}

impl TracksCodexTurns for CodexTurns {
    fn observe(&self, thread: &str, event: TurnEvent) -> TurnObservation {
        let mut threads = self
            .threads
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let state = threads.entry(thread.to_owned()).or_default();
        let was_resting = state.turn == ThreadTurn::Resting;
        let mut ended = false;
        // Whether this event's rest is a rest the seat settles at; only a
        // re-subscription's Idle with no turn seen in between is not.
        let mut settles = true;
        match event {
            TurnEvent::Started(turn) => {
                // A start read after its own completion changes nothing.
                if !state.completed.contains(&turn) {
                    state.turn = ThreadTurn::Running(Some(turn));
                }
            }
            TurnEvent::Completed(turn) => {
                if state.completed.contains(&turn) {
                    return TurnObservation {
                        rested: false,
                        ended: false,
                    };
                }
                ended = true;
                state.completed.push_back(turn.clone());
                if state.completed.len() > REMEMBERED_TURNS {
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
                // The first authoritative status since this Nexus started.
                let first = !state.resumed;
                state.resumed = true;
                // Work typed and not yet begun is not read as ended.
                if state.turn != ThreadTurn::Delivered {
                    let ran = matches!(state.turn, ThreadTurn::Running(_));
                    state.turn = ThreadTurn::Resting;
                    // A re-subscription's Idle after Unknown is no new rest.
                    settles = first || ran;
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
        TurnObservation {
            rested: !was_resting && state.turn == ThreadTurn::Resting && settles,
            ended,
        }
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

/// The settlement a Codex thread's rest allows.
pub trait SettlesRestingCodexSeats {
    /// Off the dispatch gate, from a responder: applies the events under
    /// the gate, then settles the Codex flow on the thread once if they
    /// brought it to rest, or left it resting with work queued; what the
    /// settlement did, or None when nothing was settled.
    fn codex_events(&self, thread: &str, events: Vec<TurnEvent>) -> Option<AtStop>;
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
    /// The Codex flow a metaflow is Awake as, its launch confirmed, no wake
    /// holding it, its thread resting: the flow whose rest is settled.
    fn resting_codex_seat(&self, address: &Address) -> Option<FlowId>;
}

impl SettlesRestingCodexSeats for RunningNexus {
    fn codex_events(&self, thread: &str, events: Vec<TurnEvent>) -> Option<AtStop> {
        if events.is_empty() {
            return None;
        }
        let (seat, ended) = {
            let _turn = self
                .dispatch_gate
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let observed = events
                .into_iter()
                .map(|event| self.codex_turns.observe(thread, event))
                .collect::<Vec<_>>();
            let ended = observed.iter().filter(|observed| observed.ended).count();
            let rested = observed.iter().any(|observed| observed.rested);
            let flow = self.codex_flow_on(thread);
            // A rest settles once whatever is queued; queued work at an
            // event that finds the thread already resting is drained.
            let seat = flow.as_ref().and_then(|node| {
                let metaflow = self.store.metaflow_awake_as(&node.flow_id.0).ok()??;
                (rested || !metaflow.queue.0.is_empty())
                    .then(|| self.resting_codex_seat(&metaflow.address))
                    .flatten()
            });
            (seat, flow.map(|node| (node.flow_id, ended)))
        };
        // Each turn the events ended is a turn end, sent before the
        // settlement it allows.
        if let Some((flow_id, ended)) = ended {
            for _ in 0..ended {
                self.turn_ended(&flow_id.0);
            }
        }
        // Settled as a Stop is: off the gate, which it takes, reading the
        // thread's turn again under it.
        seat.map(|flow_id| self.settle_stopped(&flow_id.0))
    }

    fn observe_codex_events(&self, thread: &str, events: Vec<TurnEvent>) {
        let ended = events
            .into_iter()
            .map(|event| self.codex_turns.observe(thread, event))
            .filter(|observed| observed.ended)
            .count();
        if ended > 0
            && let Some(node) = self.codex_flow_on(thread)
        {
            for _ in 0..ended {
                self.turn_ended(&node.flow_id.0);
            }
        }
        if self.codex_turns.turn(thread) == ThreadTurn::Resting
            && let Some(node) = self.codex_flow_on(thread)
            && matches!(self.store.address_hold(&node.flow_id.0), Ok(None))
            && matches!(self.store.metaflow_awake_as(&node.flow_id.0), Ok(Some(_)))
        {
            // An unconfirmed seat is noted by its confirmation instead.
            self.stopped_at_confirmation.note(&node.flow_id.0);
        }
    }

    fn codex_flow_on(&self, thread: &str) -> Option<FlowNode> {
        let nodes = self.store.flow_nodes().ok()?;
        let mut live = nodes.into_iter().filter(|node| {
            node.harness_kind == HarnessKind::Codex
                && !thread.is_empty()
                && node.session_id.0 == thread
                && node.flow_lifecycle.is_live()
        });
        let node = live.next()?;
        live.next().is_none().then_some(node)
    }

    fn codex_resting(&self, flow_id: &str) -> bool {
        matches!(
            self.store.flow_node(flow_id),
            Ok(Some(node)) if node.harness_kind == HarnessKind::Codex
                && !node.session_id.0.is_empty()
                && self.codex_turns.turn(&node.session_id.0) == ThreadTurn::Resting
        )
    }

    fn codex_in_turn(&self, flow_id: &str) -> bool {
        matches!(
            self.store.flow_node(flow_id),
            Ok(Some(node)) if node.harness_kind == HarnessKind::Codex
                && matches!(
                    self.codex_turns.turn(&node.session_id.0),
                    ThreadTurn::Running(_) | ThreadTurn::Delivered
                )
        )
    }

    fn resting_codex_seat(&self, address: &Address) -> Option<FlowId> {
        let metaflow = self.store.metaflow(address).ok()??;
        let flow_ethos::PermaflowState::Awake(flow_id) = metaflow.permaflow_state else {
            return None;
        };
        (!self.waking.is_waking(address)
            && matches!(self.store.address_hold(&flow_id.0), Ok(None))
            && self.codex_resting(&flow_id.0))
        .then_some(flow_id)
    }
}
