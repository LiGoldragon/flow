//! Wake and drain: a metaflow's seat stays in its pane while it runs, and a
//! seat whose pane is gone wakes fresh.
//!
//! A metaflow is Awake as one flow for as long as that flow's pane runs,
//! busy or idle; no pane is closed because its flow went idle. When that
//! flow's turn ends, Flow reads whether its seat rests. A Claude seat
//! reports `Stopped` naming its own native session and transcript; one
//! whose transcript records its main turn's `end_turn` is ready for work
//! whatever Herdr's screen shows; otherwise Herdr is given a bounded wait to
//! show the seat Idle or Done, since a harness's Stop hook can run while its
//! screen still shows it working. A Codex seat reports no Stop: it rests
//! when its app-server's thread rests. Then, with work queued, the oldest
//! request is handed over (`Operation::Drain`); with nothing queued, the
//! seat stays Awake in its pane. Work queued later for an Awake seat known
//! resting, by the same evidence, is handed over at once.
//!
//! A metaflow is Asleep once its flow has ended: its pane closed by the
//! living, its harness crashed, or the flow replaced. Work queued for an
//! Awake seat whose pane Herdr shows gone records that flow Exited, which
//! puts its metaflow Asleep. A request queued for an Asleep metaflow wakes
//! it (`Operation::Wake`): a fresh flow is launched from the configured
//! layer Model and the module identities the metaflow's last launch named,
//! composed again as any Start is, with the whole queue, oldest first, as
//! its brief. The requests leave the queue only in the write that records
//! the started flow Awake, so a Nexus that dies mid-wake loses none: at its
//! next start it wakes every Asleep metaflow still holding work. Requests
//! that arrive while the wake runs are handed to the woken flow once it is
//! up, oldest first.
//!
//! Each choice of when a seat rests is one function of [`WakePolicy`].
//!
//! Whether a metaflow is due a wake is read from Flow's records alone
//! ([`SleepsAndWakesSeats::wake_is_due`]): it is Asleep and holds work that
//! wakes it. Nothing carried beside a commit decides it, so every path that
//! may leave a wake owed asks the same question: the connection or
//! send-tool responder that queued a request, after its answer was written
//! or failed to be, a replayed send call answered from its record, and the
//! Nexus's start.
//!
//! A wake runs on the thread that asked, off the dispatch gate: it takes the
//! gate for each store step and releases it while the fresh launch settles,
//! as Start does. While it runs, the metaflow is held in
//! [`WakingMetaflows`], so a second request starts no second wake and a
//! turn end does not drain the waking seat.

use crate::RunningNexus;
use crate::claude::{
    ClaudeQueuedHistory, ClaudeTranscriptCursor, LocatesClaudeTranscript, MeasuresClaudeContext,
    ReadsClaudeTurns,
};
use crate::codex::{
    CodexTurnHistory, ReadsCodexTurnHistory, SelectsCodexEndpoint, SubmitsQueuedCodexTurn,
};
use crate::codex_turns::{SettlesRestingCodexSeats, TracksCodexTurns};
use crate::composition::{FreshSeat, RendersQueuedWork};
use crate::delivery::lease::LeasesPanes;
use crate::delivery::{FindsDeliveryTarget, PaneRefusal, ReadsLeasedPane, TargetRefusal};
use crate::generated::operation::{
    Failed_Data, Operation, Outcome, Record_Data, Record_Data_Asleep_Data, Record_Data_Awoken_Data,
    Record_Data_Context_Data,
};
use crate::generated::starter::ProfilesFreshSeat;
use crate::herdr::pane::{PaneAgent, Placement, WritesPane};
use crate::herdr::{LocatesNativeTranscripts, PanePresence, ReadsHerdrPanes, ReadsHerdrRoster};
use crate::launching::LaunchesFlows;
use crate::performing::Performs;
use crate::send_tool::ServesLaunchCalls;
use crate::store::NamesConfiguredModel;
use crate::store::address_hold::HoldsLaunchAddress;
use crate::store::context::JudgesThreshold;
use crate::store::events::RecordsHarnessEvents;
use crate::store::metaflow::SleepsMetaflows;
use crate::store::queue_delivery::{
    AbsenceRecord, AccountsQueueDelivery, NativeDeliveryReceipt, NativeHistoryAnchor,
    NativeTranscriptCursor, PrepareQueueDelivery, QueueDeliveryAttempt, QueueDeliveryPhase,
};
use crate::store::{
    ManagesMetaflowState, NamesLiveFlow, ReadsFlowRoles, ReadsFlowRows, ReadsLaunchAttempt,
    RecordsFlowLifecycle,
};
use flow_native::DigestsPrompt;
use meta_signal_flow::{AbsenceProof, Undecided};
use signal_flow::{
    Address, AgentState, Event, FlowId, FlowNode, HarnessKind, HerdrRouteSelection, LaunchProfile,
    Layer, Metaflow, NativePrompt, NativeStop, Origin, PowerLevel, QueuedRequest,
    Response,
};
use std::collections::HashSet;
use std::sync::Mutex;

/// The choices of rest and wake, each in one place.
pub trait WakePolicy {
    /// A Claude seat that reported Stopped rests when Herdr shows it Idle
    /// or Done. Working, Blocked or unreadable is not resting.
    fn rests_at(agent_state: &AgentState) -> bool {
        matches!(agent_state, AgentState::Idle | AgentState::Done)
    }

    /// The power a fresh seat is registered with, by its layer.
    fn power_of(layer: &Layer) -> PowerLevel {
        match layer {
            Layer::Primary => PowerLevel::High,
            Layer::Secondary => PowerLevel::Medium,
            Layer::Tertiary => PowerLevel::Low,
            Layer::Quaternary => PowerLevel::UltraLow,
        }
    }
}

impl WakePolicy for RunningNexus {}

/// The metaflows a wake is running for. Held for the whole wake and its
/// drain; released under the dispatch gate once the queue is seen empty.
#[derive(Debug, Default)]
pub struct WakingMetaflows {
    waking: Mutex<HashSet<Address>>,
}

/// One running wake's hold on its metaflow.
pub struct WakingLease<'held> {
    held: &'held WakingMetaflows,
    address: Address,
}

pub trait HoldsWakingMetaflows {
    /// Holds the metaflow for a wake; None when a wake already holds it.
    fn hold(&self, address: &Address) -> Option<WakingLease<'_>>;
    fn is_waking(&self, address: &Address) -> bool;
}

impl HoldsWakingMetaflows for WakingMetaflows {
    fn hold(&self, address: &Address) -> Option<WakingLease<'_>> {
        let mut waking = self
            .waking
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        waking.insert(address.clone()).then(|| WakingLease {
            held: self,
            address: address.clone(),
        })
    }

    fn is_waking(&self, address: &Address) -> bool {
        self.waking
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .contains(address)
    }
}

impl Drop for WakingLease<'_> {
    fn drop(&mut self) {
        self.held
            .waking
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(&self.address);
    }
}

/// A metaflow left Awake as a flow that has ended. A flow's end records its
/// metaflow Asleep in the same write; an older Nexus did not, so wherever
/// Awake is read for a decision, such a metaflow is recorded Asleep first
/// and read as it.
pub trait RestsEndedAwake {
    /// Records the metaflow at `address` Asleep (`Record.Asleep`), its flow
    /// joining Past, when it is Awake as a flow whose row is Stopped,
    /// Retired or Exited, or as a process binding whose process no longer
    /// runs, and that holds no Address hold. An Address held only through
    /// ended bindings, with nothing queued, is forgotten instead. True when
    /// either was done.
    fn rest_ended_awake(&self, address: &Address) -> bool;
    /// The same for every metaflow, at the Nexus's start.
    fn rest_every_ended_awake(&self);
}

impl RestsEndedAwake for RunningNexus {
    fn rest_ended_awake(&self, address: &Address) -> bool {
        let Ok(Some(metaflow)) = self.store.metaflow(address) else {
            return false;
        };
        let flow_ethos::State::Awake(flow_id) = metaflow.state else {
            return false;
        };
        let ended = match self.store.flow_node(&flow_id) {
            Ok(Some(node)) => !node.flow_lifecycle.is_live(),
            // Awake as no flow: a process binding. Its process gone, an
            // Address held only through bindings is forgotten; one holding
            // work or a flow's past is rested as an ended flow's would be.
            Ok(None) => {
                use crate::store::metaflow::ForgetsEndedBindings;
                use crate::store::{FlowStore, ReadsFlowMemory};
                if matches!(self.store.forget_ended_binding(address, &flow_id), Ok(true)) {
                    eprintln!(
                        "flow-nexus: {:?} {} {:?} held only through ended binding {flow_id} is forgotten",
                        address.aspect, address.topic, address.layer
                    );
                    return true;
                }
                matches!(
                    self.store.bound_process(address),
                    Ok(Some(process)) if !FlowStore::process_is_live(&process)
                )
            }
            Err(_) => false,
        };
        if !ended || !matches!(self.store.address_hold(&flow_id), Ok(None)) {
            return false;
        }
        match self.perform(Operation::Record(Record_Data::Asleep(
            Record_Data_Asleep_Data {
                address: address.clone(),
                flow_id: flow_id.clone(),
            },
        ))) {
            Outcome::Recorded => {
                eprintln!("flow-nexus: metaflow Awake as ended flow {flow_id} was recorded Asleep");
                true
            }
            _ => false,
        }
    }

    fn rest_every_ended_awake(&self) {
        let Ok(metaflows) = self.store.list_metaflows() else {
            return;
        };
        for metaflow in metaflows {
            self.rest_ended_awake(&metaflow.address);
        }
    }
}

/// The flows whose launch was confirmed after their harness had already
/// reported `Stopped`. Confirmation runs under the dispatch gate, which
/// `settle_stopped` takes, so such a flow is noted here and settled once the
/// gate is released and the confirming answer is written.
#[derive(Debug, Default)]
pub struct StoppedAtConfirmation {
    flows: Mutex<Vec<FlowId>>,
}

pub trait NotesStopsAtConfirmation {
    fn note(&self, flow_id: &str);
    /// Takes every flow noted so far, oldest first.
    fn take(&self) -> Vec<FlowId>;
}

impl NotesStopsAtConfirmation for StoppedAtConfirmation {
    fn note(&self, flow_id: &str) {
        let mut flows = self
            .flows
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if !flows.iter().any(|noted| noted == flow_id) {
            flows.push(flow_id.to_owned());
        }
    }

    fn take(&self) -> Vec<FlowId> {
        std::mem::take(
            &mut *self
                .flows
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()),
        )
    }
}

/// A Stop that landed before its flow's launch was confirmed is settled
/// once confirmation commits.
pub trait SettlesStopsAtConfirmation {
    /// Under the dispatch gate, as confirmation commits: notes the flow when
    /// the last event its harness reported is `Stopped`, or, for a Codex
    /// flow, when its thread is resting.
    fn note_stop_at_confirmation(&self, flow_id: &str);
    /// Off the dispatch gate: settles every noted flow's Stop.
    fn settle_stops_at_confirmation(&self);
}

impl SettlesStopsAtConfirmation for RunningNexus {
    fn note_stop_at_confirmation(&self, flow_id: &str) {
        // A Claude flow's own Stop, or the main end_turn its transcript
        // records, landed before confirmation stands as its turn end; a
        // Codex flow reports no Stop: its thread's rest, as its app-server's
        // events showed it, stands for one.
        let claude_ended = self.turn_end_recorded(flow_id)
            || matches!(
                self.store.flow_node(flow_id),
                Ok(Some(node)) if self.claude_main_turn_ended(&node)
            );
        if claude_ended || self.codex_resting(flow_id) {
            self.stopped_at_confirmation.note(flow_id);
        }
    }

    fn settle_stops_at_confirmation(&self) {
        for flow_id in self.stopped_at_confirmation.take() {
            self.settle_stopped(&flow_id);
        }
    }
}

/// How an Asleep metaflow is woken.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WakePlan {
    /// A new flow, launched from configuration and the context the
    /// metaflow's last launch named.
    Fresh(FreshContext),
    /// No launch of the metaflow names a Herdr session and a context to
    /// wake it with.
    Unplaceable,
}

pub use crate::generated::starter::FreshContext;

/// What a flow's Stop did to its seat.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AtStop {
    /// The seat rested with nothing queued: it stays Awake in its pane.
    Idle,
    /// The seat rested with work queued: the oldest request was typed in.
    Drained(QueuedRequest),
    /// The seat was due a refresh and rested: its pane was closed, it was
    /// recorded Stopped and its metaflow put Asleep, in the same event;
    /// work queued for the metaflow wakes its next, fresh flow. It is due
    /// when its last measured context passed its model's refresh threshold.
    Refreshed(Address),
    /// The seat stays as it is, for the reason named.
    Kept(KeptAtStop),
}

/// Why a Stop left its seat as it was. Every early exit of `settle_stopped`
/// names one, and the Nexus logs it, so a seat left as it was says why.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KeptAtStop {
    /// The store could not be read.
    StoreUnreadable,
    /// No metaflow is Awake as this flow.
    NotAwake,
    /// The flow's launch is not confirmed: it only holds its Address.
    Unconfirmed,
    /// A wake holds the metaflow.
    Waking,
    /// Flow holds no row for the flow.
    UnknownFlow,
    /// After the bounded wait, Herdr does not show the seat resting.
    NotResting(PaneAgent),
    /// The Codex seat's app-server shows its thread in a turn, or holding
    /// work Flow typed whose turn has not begun.
    InTurn,
    /// The Codex seat's app-server has not shown its thread resting.
    ThreadNotResting,
    /// Work is queued and the Drain did not type it.
    DrainRefused(Box<Outcome>),
    /// The seat is due a refresh and has no Herdr route to close.
    RefreshUnrouted,
    /// The seat is due a refresh and its close or its Stopped record was
    /// refused; it stays due and is taken up at its next rest.
    RefreshRefused(Box<Outcome>),
}

/// Why work was not typed into a live flow's pane. Each is logged by name;
/// the operation answers `HerdrRefused` for every one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HandOverRefusal {
    /// The flow is not a live, routable flow Flow holds.
    Target(TargetRefusal),
    /// The pane is not writable: gone, not ready, Blocked, or its composer
    /// holds text or cannot be read.
    Pane(PaneRefusal),
    /// Herdr refused the text before any of it reached the pane.
    Refused,
}

/// Why a wake did not leave its seat Awake. Every refusal is logged by
/// name; the operation answers the `Failed` it maps to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WakeRefusal {
    /// The store could not be read or written.
    StoreUnreadable,
    /// The metaflow is not Asleep.
    NotAsleep,
    /// Nothing is queued for it.
    NothingQueued,
    /// A pane still runs the last flow's native session.
    SessionLive,
    /// Herdr could not be read to see whether the session runs.
    HerdrUnreadable,
    /// Nothing names a Herdr session to wake the seat in.
    Unplaceable,
    /// A fresh launch did not start.
    FreshNotStarted(Failed_Data),
    /// The started flow was not recorded Awake.
    AwakeNotRecorded,
}

/// What a wake refusal answers on the wire.
pub trait AnswersWakeRefusal {
    /// The operation's answer.
    fn failure(&self) -> Failed_Data;
}

impl AnswersWakeRefusal for WakeRefusal {
    fn failure(&self) -> Failed_Data {
        match self {
            Self::StoreUnreadable | Self::AwakeNotRecorded => Failed_Data::StoreRefused,
            Self::NotAsleep => Failed_Data::NotAsleep,
            Self::NothingQueued => Failed_Data::NothingQueued,
            Self::SessionLive => Failed_Data::ConflictingBinding,
            Self::FreshNotStarted(failure) => failure.clone(),
            Self::HerdrUnreadable | Self::Unplaceable => Failed_Data::HerdrRefused,
        }
    }
}

/// The triggers: what a turn's end, a queued request and a Nexus start set
/// going.
pub trait SleepsAndWakesSeats {
    /// After a flow's turn ended (a Claude `Stopped` report, a Codex
    /// thread's rest): hands the oldest queued request to its resting seat;
    /// with nothing queued the seat stays Awake in its pane.
    fn settle_stopped(&self, flow_id: &str) -> AtStop;
    /// After a request was queued for `address`, or a queued one was
    /// answered again: holds the metaflow, and when Flow's records show it
    /// due a wake, wakes it and drains what arrived meanwhile. Nothing when
    /// a wake already runs or none is due, so asking twice wakes once.
    fn wake_due(&self, address: Address);
    /// After work was queued for `address`, or the owner's resolution of a
    /// hand-over committed: when the metaflow's Awake seat has queued work,
    /// records its flow Exited if Herdr shows its pane gone, which puts the
    /// metaflow Asleep for a fresh wake; otherwise, when the seat is known
    /// resting by the evidence the drain at a turn end uses (a Claude
    /// transcript whose main turn ended with no later prompt; a Codex thread
    /// the app-server reports resting), runs that same drain now. A seat not
    /// known resting waits for its turn end. Takes the dispatch gate, so
    /// the caller holds none.
    fn drain_awake(&self, address: &Address);
    /// Whether Flow's records show the metaflow due a wake: Asleep and
    /// holding work that wakes it.
    fn wake_is_due(&self, metaflow: &Metaflow) -> bool;
    /// Types what arrived during a wake into the woken flow, oldest first,
    /// until the queue is seen empty under the gate; the hold is released
    /// there.
    fn drain_woken(&self, address: &Address, lease: WakingLease<'_>);
    /// Which way an Asleep metaflow is woken.
    fn wake_plan(&self, metaflow: &Metaflow) -> WakePlan;
    /// At the Nexus's start: wakes every metaflow due a wake, as a wake the
    /// last Nexus did not start or did not finish left it.
    fn recover_wakes(&self);
}

/// The bodies of the Wake and Drain operations; `perform` delegates to
/// them.
pub trait PerformsWakeAndDrain {
    /// Performed off the dispatch gate; see the module documentation.
    fn wake(&self, address: Address) -> Outcome;
    /// Performed under the dispatch gate.
    fn drain(&self, address: Address) -> Outcome;
    /// Hands over as many queued requests as can be settled now, oldest
    /// first. It stops at the first hand-over whose native receipt is not
    /// yet established, or at any refusal; work behind that request stays
    /// durable. The caller holds the dispatch gate.
    fn drain_all(&self, address: Address) -> Outcome;
}

/// A flow the Nexus starts itself, as a wake starts one: no calling flow is
/// its origin. The start runs under the dispatch gate, which is released
/// while the launch settles.
pub(crate) trait StartsUnbound {
    fn start_unbound(&self, profile: LaunchProfile) -> Result<FlowId, Failed_Data>;
}

impl StartsUnbound for RunningNexus {
    fn start_unbound(&self, profile: LaunchProfile) -> Result<FlowId, Failed_Data> {
        let launch_request_id = profile.launch_request_id.clone();
        let response = {
            let _turn = self
                .dispatch_gate
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            // The Nexus starts this flow itself: no calling flow is its
            // origin.
            let response = self.start(profile, Origin::Unbound);
            self.settle(&launch_request_id, response)
        };
        // A send tool call made by the flow's first turn cannot wake its
        // target while this launch holds the dispatch gate. The gate is
        // released now, so deliver every deferred wake before interpreting
        // the launch result.
        self.wake_deferred();
        // The gate is released before the wait: the launch is settled by the
        // Nexus's own promoter, which needs the gate to do it.
        let response = match response {
            response => Some(response),
        };
        match response {
            Some(Response::Started(launched)) => Ok(launched.flow_id),
            _ => Err(Failed_Data::Unstarted),
        }
    }
}

/// The steps a wake takes.
trait WakesInParts {
    /// The body of `wake`, every early exit a typed refusal.
    fn waken(&self, address: &Address) -> Result<FlowId, WakeRefusal>;
    fn start_fresh(
        &self,
        metaflow: &Metaflow,
        fresh: FreshContext,
        text: &str,
    ) -> Result<FlowId, Failed_Data>;
    /// Types text into a live flow's pane under its lease, as Flow's own
    /// write: no Priority head, since it is not a Message. A refusal is
    /// logged by its reason.
    fn hand_over(&self, flow_id: &str, text: &str) -> Result<(), HandOverRefusal>;
    fn last_launch_profile(&self, flow_id: &str) -> Option<LaunchProfile>;
    /// Writes the context size the flow's native transcript shows now, the
    /// size a Compact of the flow is judged by.
    fn record_context_at_stop(&self, flow_id: &str);
    /// Refuses a wake while a pane already runs this flow's native session:
    /// SessionLive when one does, HerdrUnreadable when Herdr is unreadable.
    fn refuse_live_session(&self, node: &FlowNode, sessions: &[String]) -> Result<(), WakeRefusal>;
    /// The body of `settle_stopped`: waits, boundedly and off the dispatch
    /// gate, for Herdr to show the seat resting, then drains it.
    fn settle_rested(&self, flow_id: &str) -> AtStop;
    /// Ends a seat due a refresh that rests, under the dispatch gate the
    /// caller holds: its pane is closed under the pane's lease and it is
    /// recorded Stopped (its metaflow Asleep).
    fn refresh_at_rest(&self, flow_id: &str, node: &FlowNode, address: Address) -> AtStop;
}

pub(crate) trait ReadsClaudeDelivery {
    fn claude_transcript(&self, node: &FlowNode) -> Option<std::path::PathBuf>;
    fn claude_main_turn_ended(&self, node: &FlowNode) -> bool;
    fn claude_delivery_cursor(&self, node: &FlowNode) -> Option<NativeTranscriptCursor>;
    /// What the flow's transcript after the attempt's cursor shows of its
    /// request; Unreadable when the attempt is no longer the flow's own
    /// session's.
    fn claude_delivery_history(&self, attempt: &QueueDeliveryAttempt) -> ClaudeQueuedHistory;
}

impl ReadsClaudeDelivery for RunningNexus {
    fn claude_transcript(&self, node: &FlowNode) -> Option<std::path::PathBuf> {
        self.herdr
            .native_transcript_root(&HarnessKind::Claude, "")
            .ok()?
            .session_transcript(&node.session_id)
    }

    fn claude_main_turn_ended(&self, node: &FlowNode) -> bool {
        node.harness_kind == HarnessKind::Claude
            && self
                .claude_transcript(node)
                .is_some_and(|transcript| transcript.main_turn_ended(&node.session_id))
    }

    fn claude_delivery_cursor(&self, node: &FlowNode) -> Option<NativeTranscriptCursor> {
        let cursor = self.claude_transcript(node)?.claude_cursor()?;
        Some(NativeTranscriptCursor {
            device: cursor.device,
            inode: cursor.inode,
            offset: cursor.offset,
        })
    }

    fn claude_delivery_history(&self, attempt: &QueueDeliveryAttempt) -> ClaudeQueuedHistory {
        let NativeHistoryAnchor::ClaudeTranscript(cursor) = &attempt.history_anchor else {
            return ClaudeQueuedHistory::Unreadable;
        };
        let node = match self.store.flow_node(&attempt.flow_id) {
            Ok(Some(node))
                if node.harness_kind == HarnessKind::Claude
                    && node.session_id == attempt.thread_id =>
            {
                node
            }
            _ => return ClaudeQueuedHistory::Unreadable,
        };
        let Some(transcript) = self.claude_transcript(&node) else {
            return ClaudeQueuedHistory::Unreadable;
        };
        transcript.queued_history_after(
            &node.session_id,
            &ClaudeTranscriptCursor {
                device: cursor.device,
                inode: cursor.inode,
                offset: cursor.offset,
            },
            &[attempt.queued.clone()].prompt_text(),
        )
    }
}

/// Whether a harness event is the flow's own. A hook runs in every process
/// its harness starts, a nested one included, under the same FLOW_ID; only
/// an event that names the flow's registered native session, and for a Stop
/// that session's own transcript, is taken for the flow's.
pub(crate) trait CorrelatesNativeEvents {
    /// The flow's own Claude turn end.
    fn correlated_stop(&self, flow_id: &str, stop: &NativeStop) -> bool;
    /// A prompt the flow's own Claude session took.
    fn correlated_prompt(&self, flow_id: &str, prompt: &NativePrompt) -> bool;
    /// Of the flow's own Stops and submitted prompts, the last recorded is
    /// a Stop: a turn end landed with no prompt taken after it.
    fn turn_end_recorded(&self, flow_id: &str) -> bool;
}

impl CorrelatesNativeEvents for RunningNexus {
    fn correlated_stop(&self, flow_id: &str, stop: &NativeStop) -> bool {
        let Ok(Some(node)) = self.store.flow_node(flow_id) else {
            return false;
        };
        let (Some(session), Some(reported)) = (
            stop.native_session_id_option.as_deref(),
            stop.transcript_path_option.as_deref(),
        ) else {
            return false;
        };
        let Some(transcript) = (node.harness_kind == HarnessKind::Claude
            && !node.session_id.is_empty()
            && session == node.session_id)
            .then(|| self.claude_transcript(&node))
            .flatten()
        else {
            return false;
        };
        let reported = std::path::Path::new(reported);
        reported == transcript
            || matches!(
                (reported.canonicalize(), transcript.canonicalize()),
                (Ok(reported), Ok(transcript)) if reported == transcript
            )
    }

    fn correlated_prompt(&self, flow_id: &str, prompt: &NativePrompt) -> bool {
        matches!(
            self.store.flow_node(flow_id),
            Ok(Some(node)) if node.harness_kind == HarnessKind::Claude
                && !node.session_id.is_empty()
                && prompt.native_session_id_option.as_deref() == Some(node.session_id.as_str())
        )
    }

    fn turn_end_recorded(&self, flow_id: &str) -> bool {
        let Ok(Some(events)) = self.store.events(flow_id) else {
            return false;
        };
        events
            .iter()
            .rev()
            .find_map(|event| match event {
                Event::Stopped(stop) => self.correlated_stop(flow_id, stop).then_some(true),
                Event::PromptSubmitted(prompt) => {
                    self.correlated_prompt(flow_id, prompt).then_some(false)
                }
                Event::Started | Event::ToolUsed(_) => None,
            })
            .unwrap_or(false)
    }
}

/// Where the seat stands once a reconciled request has settled.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SeatAfter {
    /// The request's own turn has ended and the seat rests: the next
    /// request may be handed over now.
    Resting,
    /// The request's turn may still run: the next waits for its end.
    InItsTurn,
}

/// What reconciling one outstanding hand-over found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Reconciled {
    /// Its exact native receipt is recorded and the request left the queue.
    Settled(QueuedRequest, SeatAfter),
    /// Native history proved it never reached the seat; the request is
    /// still queued and may be handed over again.
    Absent,
    /// Native history could not decide; it stays Uncertain and is never
    /// handed over again.
    Undecided(Undecided),
    /// The store refused, or the recorded receipt no longer settles the
    /// oldest request.
    Refused,
}

/// Reconciliation of a queued request's hand-over from the seat's own
/// native history: Claude's transcript after the stored cursor and its
/// UserPromptSubmit receipts, Codex's thread turns after the stored newest
/// turn. Found settles, proven absence authorises a new attempt, and
/// anything else stays Uncertain with its reason, readable through
/// `ReadQueueDeliveries`.
pub(crate) trait ReconcilesQueueDelivery {
    /// Under the dispatch gate.
    fn reconcile_delivery(&self, attempt: &QueueDeliveryAttempt) -> Reconciled;
    /// Records the receipt and settles the request.
    fn settle_received(
        &self,
        attempt: &QueueDeliveryAttempt,
        turn_id: String,
        native_receipt: String,
        seat_after: SeatAfter,
    ) -> Reconciled;
    /// A prompt the flow's own session took: settles the outstanding
    /// hand-over whose request it exactly is. Anything else, a task
    /// notification among them, settles nothing.
    fn settle_submitted_prompt(&self, flow_id: &str, prompt: &NativePrompt);
    /// At the Nexus's start: reconciles every hand-over the last Nexus left
    /// outstanding, then settles every Claude seat whose own turn end was
    /// recorded and which holds work, as a Stop would.
    fn recover_deliveries(&self);
}

impl ReconcilesQueueDelivery for RunningNexus {
    fn reconcile_delivery(&self, attempt: &QueueDeliveryAttempt) -> Reconciled {
        match &attempt.phase {
            QueueDeliveryPhase::ReceiptRecorded(_) => {
                return if self
                    .store
                    .settle_recorded_delivery(&attempt.attempt_id)
                    .unwrap_or(false)
                {
                    Reconciled::Settled(attempt.queued.clone(), SeatAfter::InItsTurn)
                } else {
                    Reconciled::Refused
                };
            }
            QueueDeliveryPhase::Settled(_) => {
                return Reconciled::Settled(attempt.queued.clone(), SeatAfter::InItsTurn);
            }
            QueueDeliveryPhase::Absent(_) => return Reconciled::Absent,
            QueueDeliveryPhase::Resolved(record) => {
                return match record.queue_resolution_outcome {
                    meta_signal_flow::QueueResolutionOutcome::Delivered => {
                        Reconciled::Settled(attempt.queued.clone(), SeatAfter::InItsTurn)
                    }
                    meta_signal_flow::QueueResolutionOutcome::Absent => Reconciled::Absent,
                };
            }
            QueueDeliveryPhase::Prepared | QueueDeliveryPhase::Uncertain(_) => {}
        }
        let text = [attempt.queued.clone()].prompt_text();
        let found = match attempt.harness_kind {
            HarnessKind::Claude => match self.claude_delivery_history(attempt) {
                // A Claude seat takes input while its turn runs, into its
                // own queue: the next request need not wait.
                ClaudeQueuedHistory::Received(receipt) => {
                    Ok((receipt.turn_id, receipt.row, SeatAfter::Resting))
                }
                // Text typed into a Claude pane may sit in its composer or
                // its input queue unrecorded: absence after typing is
                // never proven from the transcript.
                ClaudeQueuedHistory::Unseen => Err(Undecided::AbsenceUnprovable),
                ClaudeQueuedHistory::Pending => Err(Undecided::PendingInHarness),
                ClaudeQueuedHistory::Ambiguous => Err(Undecided::Ambiguous),
                ClaudeQueuedHistory::Unreadable => Err(Undecided::HistoryUnreadable),
                ClaudeQueuedHistory::Replaced => Err(Undecided::HistoryReplaced),
            },
            HarnessKind::Codex => {
                let NativeHistoryAnchor::CodexTurns(anchor) = &attempt.history_anchor else {
                    return self.undecided(attempt, Undecided::HistoryUnreadable);
                };
                let history = self
                    .store
                    .role(&attempt.flow_id)
                    .ok()
                    .flatten()
                    .and_then(|role| self.codex_endpoints.adapter_for(&role.model_name).ok())
                    .and_then(|adapter| {
                        adapter
                            .queued_turn_after(&attempt.thread_id, anchor.as_deref(), &text)
                            .ok()
                    });
                match history {
                    Some(CodexTurnHistory::Received {
                        turn_id,
                        turn,
                        resting,
                    }) => Ok((
                        turn_id,
                        turn,
                        if resting {
                            SeatAfter::Resting
                        } else {
                            SeatAfter::InItsTurn
                        },
                    )),
                    Some(CodexTurnHistory::Unseen {
                        resting: true,
                        observation,
                    }) => {
                        return if self
                            .store
                            .mark_delivery_absent(
                                &attempt.attempt_id,
                                AbsenceRecord {
                                    proof: AbsenceProof::HistoryAbsent,
                                    observation,
                                },
                            )
                            .unwrap_or(false)
                        {
                            eprintln!(
                                "flow-nexus: queued delivery {} never reached thread {}; a new attempt is authorised",
                                attempt.attempt_id, attempt.thread_id
                            );
                            Reconciled::Absent
                        } else {
                            Reconciled::Refused
                        };
                    }
                    Some(CodexTurnHistory::Unseen { resting: false, .. }) => {
                        Err(Undecided::SeatNotResting)
                    }
                    Some(CodexTurnHistory::AnchorMissing) => Err(Undecided::HistoryReplaced),
                    Some(CodexTurnHistory::Ambiguous) => Err(Undecided::Ambiguous),
                    None => Err(Undecided::HistoryUnreadable),
                }
            }
        };
        match found {
            Ok((turn_id, native_receipt, seat_after)) => {
                self.settle_received(attempt, turn_id, native_receipt, seat_after)
            }
            Err(undecided) => self.undecided(attempt, undecided),
        }
    }

    fn settle_received(
        &self,
        attempt: &QueueDeliveryAttempt,
        turn_id: String,
        native_receipt: String,
        seat_after: SeatAfter,
    ) -> Reconciled {
        let receipt = NativeDeliveryReceipt {
            native_request_id: attempt.native_request_id,
            thread_id: attempt.thread_id.clone(),
            turn_id,
            native_receipt,
        };
        if self
            .store
            .record_delivery_receipt(&attempt.attempt_id, receipt)
            .unwrap_or(false)
            && self
                .store
                .settle_recorded_delivery(&attempt.attempt_id)
                .unwrap_or(false)
        {
            Reconciled::Settled(attempt.queued.clone(), seat_after)
        } else {
            Reconciled::Refused
        }
    }

    fn settle_submitted_prompt(&self, flow_id: &str, prompt: &NativePrompt) {
        use datom_codec::Datomizable;
        use protos::{Compactable, Protosizable};
        let _turn = self
            .dispatch_gate
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let Ok(Some(metaflow)) = self.store.metaflow_awake_as(flow_id) else {
            return;
        };
        let Ok(Some(attempt)) = self.store.outstanding_delivery(&metaflow.address) else {
            return;
        };
        if attempt.flow_id != flow_id
            || attempt.harness_kind != HarnessKind::Claude
            || prompt.native_session_id_option.as_deref() != Some(attempt.thread_id.as_str())
            || !matches!(
                attempt.phase,
                QueueDeliveryPhase::Prepared | QueueDeliveryPhase::Uncertain(_)
            )
            || [attempt.queued.clone()].prompt_text().prompt_sha256() != prompt.prompt_sha256
        {
            return;
        }
        let Some(prompt_id) = prompt
            .prompt_id_option
            .clone()
            .filter(|prompt_id| !prompt_id.is_empty())
        else {
            eprintln!(
                "flow-nexus: flow {flow_id} took queued delivery {} without a prompt id; its transcript settles it",
                attempt.attempt_id
            );
            return;
        };
        let observed = Event::PromptSubmitted(prompt.clone())
            .datomize(vec![])
            .protosize()
            .compact();
        if let Reconciled::Refused =
            self.settle_received(&attempt, prompt_id, observed, SeatAfter::InItsTurn)
        {
            eprintln!(
                "flow-nexus: the receipt of queued delivery {} was not recorded",
                attempt.attempt_id
            );
        }
    }

    fn recover_deliveries(&self) {
        let seats = {
            let _turn = self
                .dispatch_gate
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let Ok(metaflows) = self.store.list_metaflows() else {
                eprintln!("flow-nexus: metaflows unreadable; no queued delivery reconciled");
                return;
            };
            for metaflow in &metaflows {
                if let Ok(Some(attempt)) = self.store.outstanding_delivery(&metaflow.address) {
                    let reconciled = self.reconcile_delivery(&attempt);
                    eprintln!(
                        "flow-nexus: queued delivery {} reconciled at start: {reconciled:?}",
                        attempt.attempt_id
                    );
                }
            }
            metaflows
                .into_iter()
                .filter(|metaflow| !metaflow.queue.is_empty())
                .filter_map(|metaflow| match metaflow.state {
                    flow_ethos::State::Awake(flow_id) => Some(flow_id),
                    _ => None,
                })
                .filter(|flow_id| {
                    matches!(self.store.address_hold(flow_id), Ok(None))
                        && self.turn_end_recorded(flow_id)
                })
                .collect::<Vec<_>>()
        };
        for flow_id in seats {
            self.settle_stopped(&flow_id);
        }
    }
}

trait MarksUndecided {
    fn undecided(&self, attempt: &QueueDeliveryAttempt, undecided: Undecided) -> Reconciled;
}

impl MarksUndecided for RunningNexus {
    fn undecided(&self, attempt: &QueueDeliveryAttempt, undecided: Undecided) -> Reconciled {
        if self
            .store
            .mark_delivery_undecided(&attempt.attempt_id, undecided.clone())
            .unwrap_or(false)
        {
            Reconciled::Undecided(undecided)
        } else {
            Reconciled::Refused
        }
    }
}

impl SleepsAndWakesSeats for RunningNexus {
    fn settle_stopped(&self, flow_id: &str) -> AtStop {
        let at_stop = self.settle_rested(flow_id);
        match &at_stop {
            AtStop::Kept(reason) => {
                eprintln!("flow-nexus: flow {flow_id} kept at its Stop: {reason:?}");
            }
            // The refreshed seat's work wakes the fresh flow, once the gate
            // the refresh ran under is given back; with nothing queued the
            // metaflow sleeps until an Order, a Question or the living's
            // words.
            AtStop::Refreshed(address) => {
                eprintln!("flow-nexus: flow {flow_id} refreshed at its rest");
                self.wake_due(address.clone());
            }
            AtStop::Idle | AtStop::Drained(_) => {}
        }
        at_stop
    }

    fn drain_awake(&self, address: &Address) {
        let resting = {
            let _turn = self
                .dispatch_gate
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let Ok(Some(metaflow)) = self.store.metaflow(address) else {
                return;
            };
            let flow_ethos::State::Awake(flow_id) = metaflow.state else {
                return;
            };
            if metaflow.queue.is_empty()
                || self.waking.is_waking(address)
                || !matches!(self.store.address_hold(&flow_id), Ok(None))
            {
                return;
            }
            let Ok(Some(node)) = self.store.flow_node(&flow_id) else {
                return;
            };
            // A seat whose pane is gone has ended: its flow is recorded
            // Exited, which puts the metaflow Asleep, and the wake that
            // follows starts a fresh flow.
            if matches!(
                self.herdr.pane_presence(&node),
                PanePresence::Absent | PanePresence::Bare
            ) {
                let _ = self.perform(Operation::Record(Record_Data::Exited(flow_id.clone())));
                return;
            }
            let known_resting = match node.harness_kind {
                HarnessKind::Codex => self.codex_resting(&flow_id),
                HarnessKind::Claude => self.claude_main_turn_ended(&node),
            };
            known_resting.then_some(flow_id)
        };
        if let Some(flow_id) = resting {
            self.settle_stopped(&flow_id);
        }
    }

    fn wake_due(&self, address: Address) {
        // Work queued for an Awake seat known resting is handed to it now,
        // as its turn's end would; one in a turn waits for the turn's end.
        // An Awake seat whose pane is gone is put Asleep, to wake below.
        self.drain_awake(&address);
        let Some(lease) = self.waking.hold(&address) else {
            return;
        };
        let due = {
            let _turn = self
                .dispatch_gate
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            self.rest_ended_awake(&address);
            matches!(
                self.store.metaflow(&address),
                Ok(Some(metaflow)) if self.wake_is_due(&metaflow)
            )
        };
        if !due {
            return;
        }
        // A refused wake has logged its reason; its work stays queued.
        if let Outcome::Woken(_) = self.perform(Operation::Wake(address.clone())) {
            self.drain_woken(&address, lease);
        }
        self.settle_stops_at_confirmation();
    }

    fn drain_woken(&self, address: &Address, lease: WakingLease<'_>) {
        loop {
            let _turn = self
                .dispatch_gate
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            match self.perform(Operation::Drain(address.clone())) {
                Outcome::Drained(typed) if typed.is_empty() => {
                    drop(lease);
                    return;
                }
                Outcome::Drained(_) => {}
                outcome => {
                    eprintln!("flow-nexus: work queued during a wake stays queued: {outcome:?}");
                    drop(lease);
                    return;
                }
            }
        }
    }

    fn wake_plan(&self, metaflow: &Metaflow) -> WakePlan {
        use crate::store::metaflow_declaration::ReadsMetaflowDeclarations;
        let last = metaflow
            .past
            .last()
            .and_then(|flow_id| self.last_launch_profile(flow_id));
        // A declared metaflow wakes with the modules it is bound to; one
        // never declared, with those its last launch named.
        match (self.store.metaflow_declaration(&metaflow.address), last) {
            (Ok(Some(declared)), last) => WakePlan::Fresh(FreshContext {
                herdr_session_name: last
                    .map(|profile| profile.herdr_session_name)
                    .unwrap_or_else(|| "default".to_owned()),
                system_modules: declared.system_modules,
                user_modules: declared.user_modules,
                leave_outs_option: None,
            }),
            (_, Some(profile)) => WakePlan::Fresh(FreshContext {
                herdr_session_name: profile.herdr_session_name,
                system_modules: profile.system_modules,
                user_modules: profile.user_modules,
                leave_outs_option: profile.leave_outs_option,
            }),
            (_, None) => WakePlan::Unplaceable,
        }
    }

    fn wake_is_due(&self, metaflow: &Metaflow) -> bool {
        metaflow.state == flow_ethos::State::Asleep && !metaflow.queue.is_empty()
    }

    fn recover_wakes(&self) {
        self.recover_deliveries();
        let due = {
            let _turn = self
                .dispatch_gate
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            self.rest_every_ended_awake();
            match self.store.list_metaflows() {
                Ok(metaflows) => metaflows
                    .into_iter()
                    .filter(|metaflow| self.wake_is_due(metaflow))
                    .map(|metaflow| metaflow.address)
                    .collect::<Vec<_>>(),
                Err(_) => {
                    eprintln!("flow-nexus: metaflows unreadable; no unfinished wake recovered");
                    return;
                }
            }
        };
        for address in due {
            self.wake_due(address);
        }
    }
}

impl PerformsWakeAndDrain for RunningNexus {
    fn wake(&self, address: Address) -> Outcome {
        match self.waken(&address) {
            Ok(flow_id) => Outcome::Woken(flow_id),
            Err(refusal) => {
                eprintln!(
                    "flow-nexus: wake of {:?} {} {:?} refused: {refusal:?}",
                    address.aspect, address.topic, address.layer
                );
                // Nothing was taken: the work is still queued, oldest first.
                Outcome::Failed(refusal.failure())
            }
        }
    }

    fn drain(&self, address: Address) -> Outcome {
        let flow_id = match self.store.metaflow(&address) {
            Ok(Some(Metaflow {
                state: flow_ethos::State::Awake(flow_id),
                ..
            })) => flow_id,
            Ok(_) => return Outcome::Failed(Failed_Data::NotAwake),
            Err(_) => return Outcome::Failed(Failed_Data::StoreRefused),
        };
        let node = match self.store.flow_node(&flow_id) {
            Ok(Some(node)) => node,
            _ => return Outcome::Failed(Failed_Data::StoreRefused),
        };
        // A hand-over still outstanding is reconciled first: its request is
        // the oldest, and nothing behind it is handed over until it is
        // settled, or proved never to have reached the seat.
        let mut drained = Vec::new();
        match self.store.outstanding_delivery(&address) {
            Ok(None) => {}
            Ok(Some(attempt)) => match self.reconcile_delivery(&attempt) {
                Reconciled::Settled(queued, SeatAfter::Resting) => drained.push(queued),
                Reconciled::Settled(queued, SeatAfter::InItsTurn) => {
                    if node.harness_kind == HarnessKind::Codex {
                        // The settled turn may still run: what follows waits
                        // for its end.
                        self.codex_turns.delivered(&node.session_id);
                        return Outcome::Drained(vec![queued]);
                    }
                    drained.push(queued);
                }
                Reconciled::Absent => {}
                Reconciled::Undecided(undecided) => {
                    eprintln!(
                        "flow-nexus: queued delivery {} stays uncertain ({undecided:?}); it is never handed over again",
                        attempt.attempt_id
                    );
                    return Outcome::Failed(Failed_Data::HerdrRefused);
                }
                Reconciled::Refused => return Outcome::Failed(Failed_Data::StoreRefused),
            },
            Err(_) => return Outcome::Failed(Failed_Data::StoreRefused),
        }
        // A flow past its model's refresh threshold is given no new
        // message: what is queued stays for the metaflow's next flow.
        if matches!(self.store.past_threshold(&flow_id), Ok(true)) {
            return Outcome::Drained(drained);
        }
        let oldest = match self.store.metaflow(&address) {
            Ok(Some(metaflow)) => metaflow.queue.into_iter().next(),
            _ => return Outcome::Failed(Failed_Data::StoreRefused),
        };
        let Some(oldest) = oldest else {
            return Outcome::Drained(drained);
        };
        let refused = |drained: Vec<QueuedRequest>, failure| {
            if drained.is_empty() {
                Outcome::Failed(failure)
            } else {
                Outcome::Drained(drained)
            }
        };
        // Native history is anchored before any native input.
        let (anchor, model) = match node.harness_kind {
            HarnessKind::Claude => match self.claude_delivery_cursor(&node) {
                Some(cursor) => (NativeHistoryAnchor::ClaudeTranscript(cursor), None),
                None => return refused(drained, Failed_Data::HerdrRefused),
            },
            HarnessKind::Codex => {
                let model = match self.store.role(&flow_id) {
                    Ok(Some(role)) => role.model_name,
                    _ => return refused(drained, Failed_Data::StoreRefused),
                };
                let adapter = match self.codex_endpoints.adapter_for(&model) {
                    Ok(adapter) => adapter,
                    Err(_) => return refused(drained, Failed_Data::CodexRefused),
                };
                match adapter.newest_turn(&node.session_id) {
                    Ok(newest) => (NativeHistoryAnchor::CodexTurns(newest), Some(adapter)),
                    Err(_) => return refused(drained, Failed_Data::CodexRefused),
                }
            }
        };
        let attempt = match self.store.prepare_oldest_delivery(
            &address,
            &flow_id,
            &node.session_id,
            node.harness_kind.clone(),
            anchor,
        ) {
            Ok(PrepareQueueDelivery::Prepared(attempt)) => attempt,
            Ok(PrepareQueueDelivery::Empty) => return Outcome::Drained(drained),
            Ok(PrepareQueueDelivery::Outstanding(_)) | Err(_) => {
                return refused(drained, Failed_Data::StoreRefused);
            }
        };
        let text = [oldest.clone()].prompt_text();
        if let Some(adapter) = model {
            return match adapter.submit_queued_turn(
                &node.session_id,
                &attempt.attempt_id,
                attempt.native_request_id,
                &text,
            ) {
                Ok(receipt) => match self.settle_received(
                    &attempt,
                    receipt.turn_id,
                    receipt.native_receipt,
                    SeatAfter::InItsTurn,
                ) {
                    Reconciled::Settled(queued, _) => {
                        self.codex_turns.delivered(&node.session_id);
                        drained.push(queued);
                        Outcome::Drained(drained)
                    }
                    _ => refused(drained, Failed_Data::StoreRefused),
                },
                Err(error) => {
                    // The request may have reached the app-server: it is
                    // reconciled from the thread's turns, never resent.
                    eprintln!(
                        "flow-nexus: queued turn {} for thread {} not acknowledged: {error:?}",
                        attempt.attempt_id, node.session_id
                    );
                    let _ = self.store.mark_delivery_uncertain(&attempt.attempt_id);
                    refused(drained, Failed_Data::CodexRefused)
                }
            };
        }
        if let Err(refusal) = self.hand_over(&flow_id, &text) {
            // Herdr refused before any of the text reached the pane.
            return if self
                .store
                .mark_delivery_absent(
                    &attempt.attempt_id,
                    AbsenceRecord {
                        proof: AbsenceProof::HandOverRefused,
                        observation: format!("{refusal:?}"),
                    },
                )
                .unwrap_or(false)
            {
                refused(drained, Failed_Data::HerdrRefused)
            } else {
                refused(drained, Failed_Data::StoreRefused)
            };
        }
        let _ = self.store.mark_delivery_uncertain(&attempt.attempt_id);
        // The receipt may already be in the transcript; otherwise the
        // seat's UserPromptSubmit report, or the next reconciliation,
        // settles it. Typed once, it is never typed again.
        if let ClaudeQueuedHistory::Received(receipt) = self.claude_delivery_history(&attempt) {
            let _ =
                self.settle_received(&attempt, receipt.turn_id, receipt.row, SeatAfter::InItsTurn);
        }
        drained.push(oldest);
        Outcome::Drained(drained)
    }

    fn drain_all(&self, address: Address) -> Outcome {
        let queued = match self.store.metaflow(&address) {
            Ok(Some(metaflow)) => metaflow.queue,
            Ok(None) => return Outcome::Failed(Failed_Data::NotAwake),
            Err(_) => return Outcome::Failed(Failed_Data::StoreRefused),
        };
        let mut removed = 0;
        loop {
            let outcome = self.drain(address.clone());
            let remaining = match self.store.metaflow(&address) {
                Ok(Some(metaflow)) => metaflow.queue.len(),
                _ => return Outcome::Failed(Failed_Data::StoreRefused),
            };
            let now_removed = queued.len().saturating_sub(remaining);
            if now_removed == removed {
                return match outcome {
                    Outcome::Failed(_) if removed == 0 => outcome,
                    // Only requests whose exact native receipt removed
                    // them from the durable queue are reported drained.
                    _ => Outcome::Drained(queued[..removed].to_vec()),
                };
            }
            removed = now_removed;
        }
    }
}

impl WakesInParts for RunningNexus {
    fn waken(&self, address: &Address) -> Result<FlowId, WakeRefusal> {
        let metaflow = {
            let _turn = self
                .dispatch_gate
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            self.rest_ended_awake(address);
            match self.store.metaflow(address) {
                Ok(Some(metaflow)) if metaflow.state == flow_ethos::State::Asleep => metaflow,
                Ok(_) => return Err(WakeRefusal::NotAsleep),
                Err(_) => return Err(WakeRefusal::StoreUnreadable),
            }
        };
        if metaflow.queue.is_empty() {
            return Err(WakeRefusal::NothingQueued);
        }
        let plan = self.wake_plan(&metaflow);
        // The last flow's session must not be running still: a seat that
        // flow's harness keeps, as one started by hand would, is never
        // joined by a second flow of the same metaflow.
        if let Some(node) = metaflow
            .past
            .last()
            .and_then(|flow_id| self.store.flow_node(flow_id).ok().flatten())
            .filter(|node| !node.session_id.is_empty())
        {
            let mut sessions = Vec::new();
            if let WakePlan::Fresh(fresh) = &plan {
                sessions.push(fresh.herdr_session_name.clone());
            }
            if let HerdrRouteSelection::Available(route) = &node.herdr_route_selection {
                sessions.push(route.herdr_session_name.clone());
            }
            self.refuse_live_session(&node, &sessions)?;
        }
        let WakePlan::Fresh(fresh) = plan else {
            return Err(WakeRefusal::Unplaceable);
        };
        let flow_id = self
            .start_fresh(&metaflow, fresh, "")
            .map_err(WakeRefusal::FreshNotStarted)?;
        let _turn = self
            .dispatch_gate
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        match self.perform(Operation::Record(Record_Data::Awoken(
            Record_Data_Awoken_Data {
                address: address.clone(),
                flow_id: flow_id.clone(),
                // Starting the seat delivers no queued request. The durable
                // queue is handed over through the ordinary drain path once
                // the seat is Awake.
                delivered: 0,
            },
        ))) {
            Outcome::Recorded => Ok(flow_id),
            _ => {
                eprintln!("flow-nexus: flow {flow_id} woke but is not recorded Awake");
                Err(WakeRefusal::AwakeNotRecorded)
            }
        }
    }

    fn start_fresh(
        &self,
        metaflow: &Metaflow,
        fresh: FreshContext,
        text: &str,
    ) -> Result<FlowId, Failed_Data> {
        let address = &metaflow.address;
        // The seat's model is the one configured for its layer and aspect.
        let model = match self.store.configured_model(address) {
            Ok(Some(model)) => model,
            Ok(None) => return Err(Failed_Data::CompositionRefused),
            Err(_) => return Err(Failed_Data::StoreRefused),
        };
        let launch_request_id = format!(
            "wake-{}-{}",
            metaflow.past.len(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|elapsed| elapsed.as_nanos())
                .unwrap_or_default()
        );
        let seat = FreshSeat {
            address: address.clone(),
            launch_request_id: launch_request_id.clone(),
            model_name: model,
            effort: <Self as crate::sending::ChoosesEffort>::effort_of(address).into(),
            herdr_session_name: fresh.herdr_session_name,
            system_modules: fresh.system_modules,
            user_modules: fresh.user_modules,
            leave_outs_option: fresh.leave_outs_option,
            prompt: text.to_owned(),
        };
        self.start_unbound(seat.fresh_profile())
    }

    fn hand_over(&self, flow_id: &str, text: &str) -> Result<(), HandOverRefusal> {
        let handed = (|| {
            let target = self
                .delivery_target(flow_id)
                .map_err(HandOverRefusal::Target)?;
            let _lease = self.pane_leases.hold(&target.route);
            let agent_state = self
                .writable_state(&target)
                .map_err(HandOverRefusal::Pane)?;
            let observe = matches!(agent_state, AgentState::Idle | AgentState::Done);
            match self.herdr.place(&target.route, text, observe) {
                Placement::Placed { observed: true }
                    if self.herdr.route_is_available(&target.node) =>
                {
                    let _ = self.store.record_active(flow_id);
                    Ok(())
                }
                Placement::Placed { .. } => Ok(()),
                // It may have reached the pane: it is never typed twice.
                Placement::Uncertain => {
                    eprintln!("flow-nexus: work typed into flow {flow_id}, reaction unknown");
                    Ok(())
                }
                Placement::Refused => Err(HandOverRefusal::Refused),
            }
        })();
        if let Err(refusal) = &handed {
            eprintln!("flow-nexus: work not typed into flow {flow_id}: {refusal:?}");
        }
        handed
    }

    fn last_launch_profile(&self, flow_id: &str) -> Option<LaunchProfile> {
        let launch_request_id = self.store.launch_requests_bound_to(flow_id).ok()?.pop()?;
        self.store
            .composable_launch_profile(&launch_request_id)
            .ok()
            .flatten()
    }

    fn record_context_at_stop(&self, flow_id: &str) {
        let Ok(Some(node)) = self.store.flow_node(flow_id) else {
            return;
        };
        if node.harness_kind != HarnessKind::Claude {
            return;
        }
        let Some(tokens) = self
            .claude_transcript(&node)
            .and_then(|transcript| transcript.context_tokens())
        else {
            eprintln!("flow-nexus: flow {flow_id} stopped with no context size readable");
            return;
        };
        if self.perform(Operation::Record(Record_Data::Context(
            Record_Data_Context_Data {
                flow_id: flow_id.to_owned(),
                tokens,
            },
        ))) != Outcome::Recorded
        {
            eprintln!("flow-nexus: flow {flow_id} context size not recorded");
        }
    }

    fn refuse_live_session(&self, node: &FlowNode, sessions: &[String]) -> Result<(), WakeRefusal> {
        let mut checked = HashSet::new();
        for herdr_session_name in sessions {
            if !checked.insert(herdr_session_name) {
                continue;
            }
            match self.herdr.holds_native_session(herdr_session_name, node) {
                Some(false) => {}
                Some(true) => {
                    eprintln!(
                        "flow-nexus: flow {} is already running; not woken again",
                        node.flow_id
                    );
                    return Err(WakeRefusal::SessionLive);
                }
                None => return Err(WakeRefusal::HerdrUnreadable),
            }
        }
        Ok(())
    }

    fn refresh_at_rest(&self, flow_id: &str, node: &FlowNode, address: Address) -> AtStop {
        let HerdrRouteSelection::Available(route) = &node.herdr_route_selection else {
            return AtStop::Kept(KeptAtStop::RefreshUnrouted);
        };
        let closed = {
            let _lease = self.pane_leases.hold(route);
            self.perform(Operation::Close(node.clone()))
        };
        if closed != Outcome::Closed {
            return AtStop::Kept(KeptAtStop::RefreshRefused(Box::new(closed)));
        }
        let recorded = self.perform(Operation::Record(Record_Data::Stopped(flow_id.to_owned())));
        if recorded != Outcome::Recorded {
            return AtStop::Kept(KeptAtStop::RefreshRefused(Box::new(recorded)));
        }
        AtStop::Refreshed(address)
    }

    fn settle_rested(&self, flow_id: &str) -> AtStop {
        self.record_context_at_stop(flow_id);
        // A Codex seat's rest is its app-server's, and a Claude seat whose
        // transcript records its main turn's end_turn has ended its turn
        // whatever its screen shows (background agents keep Herdr at
        // Working). Otherwise Claude runs its Stop hook while its spinner is
        // still on screen, and Herdr reads its state from the screen: the
        // seat is given a bounded wait to show resting before the gate is
        // taken.
        if let Ok(Some(node)) = self.store.flow_node(flow_id)
            && let HerdrRouteSelection::Available(route) =
                &self.herdr.refresh_route(node).herdr_route_selection
        {
            let node = self.store.flow_node(flow_id).ok().flatten();
            if !node.as_ref().is_some_and(|node| {
                node.harness_kind == HarnessKind::Codex || self.claude_main_turn_ended(node)
            }) {
                self.herdr.rested(route);
            }
        }
        let _turn = self
            .dispatch_gate
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let metaflow = match self.store.metaflow_awake_as(flow_id) {
            Ok(Some(metaflow)) => metaflow,
            Ok(None) => return AtStop::Kept(KeptAtStop::NotAwake),
            Err(_) => return AtStop::Kept(KeptAtStop::StoreUnreadable),
        };
        // A seat whose launch is not yet confirmed only holds its Address:
        // it is not drained until confirmation keeps it.
        match self.store.address_hold(flow_id) {
            Ok(None) => {}
            Ok(Some(_)) => return AtStop::Kept(KeptAtStop::Unconfirmed),
            Err(_) => return AtStop::Kept(KeptAtStop::StoreUnreadable),
        }
        let address = metaflow.address.clone();
        if self.waking.is_waking(&address) {
            return AtStop::Kept(KeptAtStop::Waking);
        }
        let node = match self.store.flow_node(flow_id) {
            Ok(Some(node)) => self.herdr.refresh_route(node),
            Ok(None) => return AtStop::Kept(KeptAtStop::UnknownFlow),
            Err(_) => return AtStop::Kept(KeptAtStop::StoreUnreadable),
        };
        // A seat past its model's refresh threshold is ended at its rest
        // rather than handed work; it is told nothing and writes no handover.
        let refresh_due = matches!(self.store.past_threshold(flow_id), Ok(true));
        match node.harness_kind {
            // A Codex seat is handed work through its app-server, so only
            // the app-server's thread status says it rests, whatever its
            // screen shows.
            HarnessKind::Codex => {
                if self.codex_in_turn(flow_id) {
                    return AtStop::Kept(KeptAtStop::InTurn);
                }
                if !self.codex_resting(flow_id) {
                    return AtStop::Kept(KeptAtStop::ThreadNotResting);
                }
            }
            HarnessKind::Claude => {
                let pane_agent = self.herdr.pane_agent(&node);
                let native_resting = matches!(
                    &pane_agent,
                    PaneAgent::Present { agent_state, .. } if Self::rests_at(agent_state)
                );
                // Closing a seat ends whatever runs in it: one whose main
                // turn ended while its background agents run shows Working,
                // and is refreshed only once Herdr shows it resting.
                if refresh_due && !native_resting {
                    return AtStop::Kept(KeptAtStop::NotResting(pane_agent));
                }
                let claude_main_ready = self.claude_main_turn_ended(&node)
                    && matches!(
                        self.delivery_target(flow_id)
                            .ok()
                            .and_then(|target| self.writable_state(&target).ok()),
                        Some(AgentState::Working | AgentState::Idle | AgentState::Done)
                    );
                if !native_resting && !claude_main_ready {
                    return AtStop::Kept(KeptAtStop::NotResting(pane_agent));
                }
            }
        }
        if refresh_due {
            return self.refresh_at_rest(flow_id, &node, address);
        }
        if !metaflow.queue.is_empty() {
            return match self.perform(Operation::Drain(address)) {
                Outcome::Drained(mut typed) if !typed.is_empty() => {
                    AtStop::Drained(typed.remove(0))
                }
                outcome => AtStop::Kept(KeptAtStop::DrainRefused(Box::new(outcome))),
            };
        }
        // With nothing queued the seat stays Awake in its pane.
        AtStop::Idle
    }
}
