//! Wake and drain: a metaflow's seat stays in its pane while it runs, and a
//! seat whose pane is gone wakes fresh.
//!
//! A metaflow continues as its declaration's `Continuity` names. When its
//! flow's turn ends, Flow reads whether its seat rests. A Claude seat
//! reports `Stopped` naming its own native session and transcript; one
//! whose transcript records its main turn's `end_turn` is ready for work
//! whatever Herdr's screen shows; otherwise Herdr is given a bounded wait to
//! show the seat Idle or Done, since a harness's Stop hook can run while its
//! screen still shows it working. A Codex seat reports no Stop: it rests
//! when its app-server's thread rests. Then, with work queued, the oldest
//! request is handed over (`Operation::Drain`). With nothing queued, an
//! OnDemand seat sleeps: its harness is closed under the pane's lease and
//! its flow recorded Stopped, its metaflow Asleep, keeping its Address and
//! the flow's transcript; a Maintained seat stays Awake in its pane.
//!
//! A HardAbrupt or MiddleAbrupt request queued later for an Awake seat is
//! handed over at once, with the Soft work queued before it; the delivery
//! engine interrupts only for HardAbrupt. Soft work alone waits for the
//! seat's idle event, the turn end above, which hands over the whole queue,
//! oldest first ([`HoldsSoftForIdle`]). A seat known resting by the same
//! evidence has had its idle event, and its Soft work is handed over at
//! once. Held Soft work is the metaflow's durable queue, so a Nexus that
//! starts again holds it as before.
//!
//! A seat is closed, refreshed or put to sleep, only once it rests: its
//! turn ended by the evidence above and no work it started in flight. A
//! Claude seat's work in flight is the background work its own last Stop
//! names; Claude wakes the seat when that work ends, into a turn whose Stop
//! is then its rest (`JudgesClosingRest`). Soft work is handed over at the
//! turn's end whatever background work runs.
//!
//! A seat is replaced at its idle event, its Stop or its Codex thread's turn
//! completed, once it is past its model's refresh threshold, or once it was
//! sent its handover warning before that event: the warning goes at the
//! warning share of the threshold, and the idle event after it refreshes
//! the seat whether or not it rests by the tests above, background work in
//! flight included, and whether or not it wrote its handover. A
//! Maintained metaflow, or one holding work that wakes it, is owed its
//! successor: the intent is recorded, the successor is started and
//! confirmed through Replace, and only then is the predecessor recorded
//! Stopped and its pane closed, in the replacement's reaping. Any other is
//! closed and put Asleep, its queue kept for the next flow. A Maintained
//! metaflow whose flow ends with its pane gone is owed a successor in the
//! write that records the exit, and woken with no queued request needed.
//!
//! A refresh ordered from outside (`Refresh.Address`,
//! [`RefreshesMetaflows`]), whoever asks, the flow itself included, and a
//! Launch or Replace naming a live flow, wait for that flow's idle event
//! too: a flow that does not rest has the order recorded
//! (`WaitsForRestToReplace`), answered RefreshQueued or LaunchPending at
//! once, and carried out at the idle event that finds it resting; a Nexus
//! that starts again carries out what is recorded. Until it rests the flow
//! is handed what it is sent. At its rest the refresh takes the metaflow's
//! hold and ends its flow without a turn. A Maintained metaflow, or one holding work that wakes it,
//! has its successor started and confirmed through Replace before the
//! ending flow is reaped, and the queue is handed to it. Any other has its
//! flow's harness closed and the flow recorded Stopped, the metaflow left
//! Asleep with its queue kept; its successor starts on the next waking
//! request. A Launch addressed to an Awake metaflow is such a refresh, its
//! successor the Launch's, run under the same hold. A Send that arrives
//! while any of these holds the metaflow is queued on it.
//!
//! A metaflow is Asleep once its flow has ended: its seat slept, its pane
//! closed by the living, its harness crashed, or the flow replaced. Work queued for an
//! Awake seat whose pane Herdr shows gone records that flow Exited, which
//! puts its metaflow Asleep. A request queued for an Asleep metaflow wakes
//! it (`Operation::Wake`): a fresh flow is launched from the native model
//! configured for its Model and the module identities the metaflow's last launch named,
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
//! wakes it, or is owed a successor. Nothing carried beside a commit decides it, so every path that
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
use crate::codex::{CodexTurnHistory, ReadsCodexTurnHistory, SelectsCodexEndpoint};
use crate::codex_turns::{SettlesRestingCodexSeats, TracksCodexTurns};
use crate::composition::{FreshSeat, JoinsPurpose, RendersQueuedWork};
use crate::delivery::body::RendersNativeContent;
use crate::delivery::lease::LeasesPanes;
use crate::delivery::{DeliversMessages, FindsDeliveryTarget, ReadsLeasedPane};
use crate::generated::operation::{
    Failed_Data, Operation, Outcome, Record_Data, Record_Data_Asleep_Data, Record_Data_Awoken_Data,
    Record_Data_Context_Data,
};
use crate::generated::starter::ProfilesFreshSeat;
use crate::herdr::pane::{PaneAgent, WritesPane};
use crate::herdr::{LocatesNativeTranscripts, PanePresence, ReadsHerdrPanes};
use crate::launching::LaunchesFlows;
use crate::live::HoldsLiveValue;
use crate::performing::Performs;
use crate::send_tool::ServesLaunchCalls;
use crate::store::NamesConfiguredModel;
use crate::store::address_hold::HoldsLaunchAddress;
use crate::store::context::JudgesThreshold;
use crate::store::events::RecordsHarnessEvents;
use crate::store::handover::HoldsHandovers;
use crate::store::logical_delivery::{
    CorePresentationRoute, RecordsLogicalDeliveries, SimplifiedReplyBinding,
    SimplifiedReplyStanding,
};
use crate::store::metaflow::SleepsMetaflows;
use crate::store::ordered_replacement::{
    NamesOrderedLaunch, OrderedLaunch, OrderedReplacement, OrdersReplacements, ReplacementOrder,
    ShowsWaitingAttempt,
};
use crate::store::queue_delivery::{
    AbsenceRecord, AccountsQueueDelivery, NativeDeliveryReceipt, NativeHistoryAnchor,
    NativeTranscriptCursor, PrepareQueueDelivery, QueueDeliveryAttempt, QueueDeliveryPhase,
};
use crate::store::successor::{OwesSuccessors, ReadsContinuity, SuccessionCause, SuccessorIntent};
use crate::store::{
    ManagesMetaflowState, NamesLiveFlow, ReadsFlowRoles, ReadsFlowRows, ReadsLaunchAttempt,
};
use flow_native::DigestsPrompt;
use meta_signal_flow::{AbsenceProof, Continuity, Undecided};
use signal_flow::{
    Address, AgentState, Event, FlowId, FlowNode, HarnessKind, HerdrRouteSelection, LaunchProfile,
    Metaflow, NativePrompt, NativeStop, Origin, QueueEntry, Response,
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
}

impl WakePolicy for RunningNexus {}

/// Soft work waits for its recipient's idle event. HardAbrupt and
/// MiddleAbrupt are handed to an Awake seat when they arrive; a queue that
/// holds only Soft work is handed over at the seat's next idle event, its
/// Stop or its thread's turn completed, which drains the whole queue, oldest
/// first. A seat whose last turn event already is its rest has had that
/// event, and is handed its Soft work when it arrives.
pub trait HoldsSoftForIdle {
    /// Whether the seat's last turn event is its rest: a Codex thread its
    /// app-server's events show resting, a Claude seat whose transcript
    /// records its main turn's end.
    fn rests_now(&self, flow_id: &str) -> bool;
    /// Whether `queue`, at the seat Awake as `flow_id`, waits for that
    /// seat's idle event: it holds only Soft work and the seat is in its
    /// turn.
    fn waits_for_idle(&self, queue: &[QueueEntry], flow_id: &str) -> bool;
}

impl HoldsSoftForIdle for RunningNexus {
    fn rests_now(&self, flow_id: &str) -> bool {
        match self.store.flow_node(flow_id) {
            Ok(Some(node)) => match node.harness_kind {
                HarnessKind::Codex => self.codex_resting(flow_id),
                HarnessKind::Claude => self.claude_main_turn_ended(&node),
            },
            _ => false,
        }
    }

    fn waits_for_idle(&self, queue: &[QueueEntry], flow_id: &str) -> bool {
        queue
            .iter()
            .all(|entry| matches!(entry.message, signal_flow::Message::Soft(_)))
            && !self.rests_now(flow_id)
    }
}

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

pub(crate) trait IdentifiesWakingLease {
    fn address(&self) -> &Address;
    fn owned_by(&self, holder: &WakingMetaflows) -> bool;
}

impl IdentifiesWakingLease for WakingLease<'_> {
    fn address(&self) -> &Address {
        &self.address
    }

    fn owned_by(&self, holder: &WakingMetaflows) -> bool {
        std::ptr::eq(self.held, holder)
    }
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
        let ended = match self.store.flow_node(&flow_id.0) {
            Ok(Some(node)) => !node.flow_lifecycle.is_live(),
            // Awake as no flow: a process binding. Its process gone, an
            // Address held only through bindings is forgotten; one holding
            // work or a flow's past is rested as an ended flow's would be.
            Ok(None) => {
                use crate::store::metaflow::ForgetsEndedBindings;
                use crate::store::{FlowStore, ReadsFlowMemory};
                if matches!(
                    self.store.forget_ended_binding(address, &flow_id.0),
                    Ok(true)
                ) {
                    eprintln!(
                        "flow-nexus: {:?} {} {:?} held only through ended binding {flow_id} is forgotten",
                        address.aspect,
                        address.topic.0,
                        address.model,
                        flow_id = flow_id.0
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
        if !ended || !matches!(self.store.address_hold(&flow_id.0), Ok(None)) {
            return false;
        }
        match self.perform(Operation::Record(Record_Data::Asleep(
            Record_Data_Asleep_Data {
                address: address.clone(),
                flow_id: flow_id.clone(),
            },
        ))) {
            Outcome::Recorded => {
                eprintln!(
                    "flow-nexus: metaflow Awake as ended flow {flow_id} was recorded Asleep",
                    flow_id = flow_id.0
                );
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
        if !flows.iter().any(|noted| noted.0 == flow_id) {
            flows.push(signal_flow::FlowId(flow_id.to_owned()));
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
            self.settle_stopped(&flow_id.0);
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
    /// A Maintained seat rested with nothing queued: it stays Awake in its
    /// pane.
    Idle,
    /// An OnDemand seat rested with nothing queued: its harness was closed
    /// and its flow recorded Stopped, its metaflow Asleep with its Address
    /// and the flow's transcript kept.
    Slept(Address),
    /// The seat rested with work queued: the oldest request was typed in.
    Drained(QueueEntry),
    /// The seat was due a refresh and stopped: its last measured context
    /// past its model's refresh threshold, or its handover warning sent
    /// before this idle event. A Maintained metaflow, or one holding
    /// work that wakes it, had its successor started and confirmed first,
    /// then the predecessor recorded Stopped and its pane closed in the
    /// replacement's reaping. Any other had its pane closed and was recorded
    /// Stopped, its metaflow Asleep, its queue kept for its next flow.
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
    /// A replacement ordered from outside was carried out at the seat's
    /// rest and refused, with the refusal; the order is spent.
    OrderedRefused(Box<Response>),
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
    /// The seat is owed a successor and no declared or launched context
    /// names one; it stays Awake.
    RefreshUnplaceable,
    /// The successor's launch or its replacement was refused; the
    /// predecessor stays Awake, and its next rest tries again.
    RefreshLaunchRefused(Box<Response>),
    /// An OnDemand seat with nothing queued could not be closed: the close
    /// or its Stopped record was refused; it stays Awake.
    SleepRefused(Box<Outcome>),
    /// An OnDemand seat's successor did not compose, so the seat is not
    /// closed: a closed seat whose successor cannot compose would leave its
    /// Address with no flow. It stays Awake and is handed its queue.
    SuccessorUncomposable(Box<Outcome>),
    /// A restart holds the flow: nothing is settled at a Stop of it.
    Restarting,
}

/// Whether the seat was kept because its successor could not be placed,
/// composed or launched: it stays Awake, is handed its queue though past
/// its threshold, and the refusal is told.
pub trait JudgesSuccessorRefusal {
    fn successor_refused(&self) -> bool;
}

impl JudgesSuccessorRefusal for KeptAtStop {
    fn successor_refused(&self) -> bool {
        matches!(
            self,
            Self::RefreshUnplaceable
                | Self::RefreshLaunchRefused(_)
                | Self::SuccessorUncomposable(_)
        )
    }
}

/// The flows owed a successor whose successor was refused. Each is handed
/// its queue meanwhile, though past its threshold, until a successor
/// stands.
#[derive(Debug, Default)]
pub struct RefreshRefusals {
    flows: Mutex<HashSet<String>>,
}

pub trait NotesRefreshRefusals {
    /// Notes the flow's successor refused.
    fn note(&self, flow_id: &str);
    /// Whether the flow's successor stands refused.
    fn holds(&self, flow_id: &str) -> bool;
    /// Forgets the flow: its successor stands, or it has ended.
    fn clear(&self, flow_id: &str);
}

impl NotesRefreshRefusals for RefreshRefusals {
    fn note(&self, flow_id: &str) {
        self.flows
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(flow_id.to_owned());
    }

    fn holds(&self, flow_id: &str) -> bool {
        self.flows
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .contains(flow_id)
    }

    fn clear(&self, flow_id: &str) {
        self.flows
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(flow_id);
    }
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
    /// The fresh launch was refused for this reason. The queue is kept,
    /// each request waiting in it marked `Retryable.WakeRefused` with it.
    StartRejected(signal_flow::StartRejection),
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
            Self::StartRejected(rejection) => match rejection {
                signal_flow::StartRejection::CompositionRefused(refusal) => {
                    Failed_Data::CompositionRefused(refusal.clone())
                }
                signal_flow::StartRejection::ContextRefused(refusal) => {
                    Failed_Data::ContextRefused(refusal.clone())
                }
                signal_flow::StartRejection::LeaveOutRefused(refusal) => {
                    Failed_Data::LeaveOutRefused(refusal.clone())
                }
                signal_flow::StartRejection::HarnessSpawnRefused(refusal) => {
                    Failed_Data::SpawnRefused(refusal.clone())
                }
                signal_flow::StartRejection::NativeLaunchRefused(refusal) => {
                    Failed_Data::LaunchRefused(refusal.clone())
                }
                signal_flow::StartRejection::AddressAwake => Failed_Data::AddressAwake,
                _ => Failed_Data::Unstarted,
            },
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

/// Why a flow the Nexus starts itself did not start: its start was refused
/// for a reason, or it failed before or after the start was asked.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum FreshRefusal {
    Rejected(signal_flow::StartRejection),
    Failed(Failed_Data),
}

/// A fresh run together with the exact launch that placed its first prompt.
/// A wake keeps both so it never receipts another launch later bound to the
/// same run.
struct FreshStarted {
    flow_id: FlowId,
    launch_request_id: String,
}

/// A flow the Nexus starts itself, as a wake starts one: no calling flow is
/// its origin. The start runs under the dispatch gate, which is released
/// while the launch settles.
pub(crate) trait StartsUnbound {
    fn start_unbound(&self, profile: LaunchProfile) -> Result<FlowId, FreshRefusal>;
}

impl StartsUnbound for RunningNexus {
    fn start_unbound(&self, profile: LaunchProfile) -> Result<FlowId, FreshRefusal> {
        let launch_request_id = profile.launch_request_id.clone();
        let response = {
            let _turn = self
                .dispatch_gate
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            // The Nexus starts this flow itself: no calling flow is its
            // origin.
            let response = self.start(profile, Origin::Unbound);
            self.settle(&launch_request_id.0, response)
        };
        // A send tool call made by the flow's first turn cannot wake its
        // target while this launch holds the dispatch gate. The gate is
        // released now, so deliver every deferred wake before interpreting
        // the launch result.
        self.wake_deferred();
        // The gate is released before the wait: the launch is settled by the
        // Nexus's own promoter, which needs the gate to do it.
        match response {
            Response::Started(launched) => Ok(launched.flow_id),
            Response::StartRejected(rejection) => Err(FreshRefusal::Rejected(rejection)),
            _ => Err(FreshRefusal::Failed(Failed_Data::Unstarted)),
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
    ) -> Result<FreshStarted, FreshRefusal>;
    fn settle_startup(
        &self,
        address: &Address,
        flow_id: &str,
        launch_request_id: &str,
        startup: &[QueueEntry],
        simplified: bool,
    ) -> bool;
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
    /// Closes a resting seat's harness under the pane's lease and records
    /// its flow Stopped (its metaflow Asleep), under the dispatch gate the
    /// caller holds. The refusal names why the seat stays.
    fn close_at_rest(&self, flow_id: &str, node: &FlowNode) -> Result<(), Box<Outcome>>;
    /// Replaces a resting seat owed a successor. The caller has released
    /// the dispatch gate and holds the metaflow's waking lease, so no wake
    /// or drain runs meanwhile: the successor is started and confirmed
    /// through Replace before the predecessor is stopped and closed. The
    /// successor is owed for `cause` until it is confirmed.
    fn refresh_successor_first(
        &self,
        flow_id: &str,
        metaflow: &Metaflow,
        cause: SuccessionCause,
    ) -> AtStop;
    /// Starts a successor an earlier Nexus owed after a Refresh ordered from
    /// outside or a Replace and did not see confirmed, its predecessor still
    /// the flow the metaflow is Awake as: under the metaflow's lock, whatever
    /// the predecessor's context, the Refresh's successor is started as the
    /// refresh started it and the Replace's resumes its own launch. A launch
    /// already refused is not resumed; a new one is started in its place. A
    /// Replace whose launch was never recorded had started nothing, and is
    /// forgotten.
    fn take_up_owed(&self, intent: SuccessorIntent) -> AtStop;
    /// Settles the successor owed at `address` by its Replace's answer: a
    /// launch that has not settled, or a reap refused once the successor
    /// was confirmed, stays owed under the same request; any other answer
    /// forgets it, so the next refresh owes a new launch.
    fn settle_owed(&self, address: &Address, response: &Response);
    /// Whether the launch request has settled refused.
    fn launch_refused(&self, launch_request_id: &str) -> bool;
    /// The launch profile of the successor the metaflow's flow is owed: its
    /// declaration, else what the flow's own launch named. None when
    /// nothing names one or no model is configured for it.
    fn successor_profile(
        &self,
        flow_id: &str,
        metaflow: &Metaflow,
        launch_request_id: String,
    ) -> Option<LaunchProfile>;
    /// Composes the successor the seat would get, launching nothing, under
    /// the dispatch gate the caller holds: an OnDemand seat is closed only
    /// once its successor composes.
    fn successor_composes(&self, flow_id: &str, metaflow: &Metaflow) -> Result<(), KeptAtStop>;
    /// A seat kept because its successor was refused: notes it, so it is
    /// handed its queue though past its threshold, and hands it what is
    /// queued now, as its rest's drain would. The refusal is told to no
    /// flow: it stands as Flow recorded it. Takes the dispatch gate; the
    /// caller holds none.
    fn successor_refused(&self, flow_id: &str);
}

pub(crate) trait ReadsClaudeDelivery {
    fn simplified_first_input_ready(&self, node: &FlowNode) -> bool;
    fn claude_transcript(&self, node: &FlowNode) -> Option<std::path::PathBuf>;
    fn claude_main_turn_ended(&self, node: &FlowNode) -> bool;
    fn claude_delivery_cursor(&self, node: &FlowNode) -> Option<NativeTranscriptCursor>;
    /// What the flow's transcript after the attempt's cursor shows of
    /// `text`, the payload that carried its request; Unreadable when the
    /// attempt is no longer the flow's own session's.
    fn claude_delivery_history(
        &self,
        attempt: &QueueDeliveryAttempt,
        text: &str,
    ) -> ClaudeQueuedHistory;
}

impl ReadsClaudeDelivery for RunningNexus {
    fn simplified_first_input_ready(&self, node: &FlowNode) -> bool {
        if node.harness_kind != HarnessKind::Claude
            || node.flow_lifecycle != signal_flow::FlowLifecycle::Active
            || !self
                .last_launch_profile(&node.flow_id.0)
                .is_some_and(|profile| {
                    profile.flow_kind == signal_flow::FlowKind::Simplified
                        && profile.instruction_prompt.0.trim().is_empty()
                })
        {
            return false;
        }
        let Ok(events) = self.store.events(&node.flow_id.0) else {
            return false;
        };
        if events
            .unwrap_or_default()
            .iter()
            .any(|event| matches!(event, Event::PromptSubmitted(_) | Event::Stopped(_)))
        {
            return false;
        }
        self.claude_transcript(node)
            .is_none_or(|path| path.main_turn_unstarted(&node.session_id.0))
    }

    fn claude_transcript(&self, node: &FlowNode) -> Option<std::path::PathBuf> {
        self.herdr
            .native_transcript_root(&HarnessKind::Claude, "")
            .ok()?
            .session_transcript(&node.session_id.0)
    }

    fn claude_main_turn_ended(&self, node: &FlowNode) -> bool {
        node.harness_kind == HarnessKind::Claude
            && (self.simplified_first_input_ready(node)
                || self
                    .claude_transcript(node)
                    .is_some_and(|transcript| transcript.main_turn_ended(&node.session_id.0)))
    }

    fn claude_delivery_cursor(&self, node: &FlowNode) -> Option<NativeTranscriptCursor> {
        let cursor = match self.claude_transcript(node) {
            Some(transcript) => transcript.claude_cursor()?,
            None if self.simplified_first_input_ready(node) => ClaudeTranscriptCursor {
                device: 0,
                inode: 0,
                offset: 0,
            },
            None => return None,
        };
        Some(NativeTranscriptCursor {
            device: cursor.device,
            inode: cursor.inode,
            offset: cursor.offset,
        })
    }

    fn claude_delivery_history(
        &self,
        attempt: &QueueDeliveryAttempt,
        text: &str,
    ) -> ClaudeQueuedHistory {
        let NativeHistoryAnchor::ClaudeTranscript(cursor) = &attempt.history_anchor else {
            return ClaudeQueuedHistory::Unreadable;
        };
        let node = match self.store.flow_node(&attempt.flow_id) {
            Ok(Some(node))
                if node.harness_kind == HarnessKind::Claude
                    && node.session_id.0 == attempt.thread_id =>
            {
                node
            }
            _ => return ClaudeQueuedHistory::Unreadable,
        };
        let Some(transcript) = self.claude_transcript(&node) else {
            return ClaudeQueuedHistory::Unreadable;
        };
        transcript.queued_history_after(
            &node.session_id.0,
            &ClaudeTranscriptCursor {
                device: cursor.device,
                inode: cursor.inode,
                offset: cursor.offset,
            },
            text,
        )
    }
}

/// What looking for a simplified seat's owed answer came to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SimplifiedAnswer {
    /// The seat owes no answer.
    NoneOwed,
    /// The answer to `input` was queued to `recipient`, its binding complete.
    Queued { input: String, recipient: Address },
    /// The bound input is not yet Delivered.
    InputUnreceived { input: String },
    /// The bound input is Delivered, and the turn it opened shows no ended
    /// answer in the seat's transcript yet.
    TurnUnanswered { input: String },
    /// The store took no answer: its recipient is unknown or the receipt
    /// no longer matches.
    Undelivered { input: String },
    /// The store could not be read or written.
    StoreRefused,
}

/// A simplified seat answers the one input it was bound to: the answer is
/// read from its own transcript, whatever order its prompt hook, its user
/// row and its Stop came in.
pub(crate) trait QueuesSimplifiedAnswers {
    /// Under the dispatch gate the caller holds: when the flow's bound input
    /// is Delivered and the turn that input opened ended with an answer in
    /// the flow's transcript, queues that answer to the binding's recipient
    /// and completes the binding. Each recipient is woken once the gate is
    /// given back (`wake_deferred`).
    fn queue_simplified_answer(&self, flow_id: &str) -> SimplifiedAnswer;
}

impl QueuesSimplifiedAnswers for RunningNexus {
    fn queue_simplified_answer(&self, flow_id: &str) -> SimplifiedAnswer {
        let (binding, receipt) = match self.store.simplified_reply_receipt(flow_id) {
            Ok(SimplifiedReplyStanding::NoneOwed) => return SimplifiedAnswer::NoneOwed,
            Ok(SimplifiedReplyStanding::Unreceived(binding)) => {
                return SimplifiedAnswer::InputUnreceived {
                    input: binding.input_delivery_id,
                };
            }
            Ok(SimplifiedReplyStanding::Received(binding, receipt)) => (binding, receipt),
            Err(_) => return SimplifiedAnswer::StoreRefused,
        };
        let input = binding.input_delivery_id.clone();
        let answer = self
            .store
            .flow_node(flow_id)
            .ok()
            .flatten()
            .filter(|node| {
                node.harness_kind == HarnessKind::Claude
                    && node.session_id.0 == receipt.native_session_id.0
            })
            .and_then(|node| self.claude_transcript(&node))
            .and_then(|transcript| {
                transcript.assistant_answer_after(
                    &receipt.native_session_id.0,
                    &receipt.native_prompt_id.0,
                )
            });
        let Some(answer) = answer else {
            return SimplifiedAnswer::TurnUnanswered { input };
        };
        match self.store.deliver_simplified_reply(
            flow_id,
            &input,
            &receipt.native_session_id.0,
            &receipt.native_prompt_id.0,
            &answer,
        ) {
            Ok(Some(recipient)) => {
                self.defer_wake(recipient.clone());
                // A core presentation's answer goes to the book Tertiary; its
                // sender is told by a Notice.
                if binding.source.is_core_presentation() && binding.recipient != recipient {
                    self.defer_wake(binding.recipient.clone());
                }
                SimplifiedAnswer::Queued { input, recipient }
            }
            Ok(None) => SimplifiedAnswer::Undelivered { input },
            Err(_) => SimplifiedAnswer::StoreRefused,
        }
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
    /// That last Stop, when the last of the flow's own Stops and submitted
    /// prompts is a Stop.
    fn last_turn_end(&self, flow_id: &str) -> Option<NativeStop>;
}

impl CorrelatesNativeEvents for RunningNexus {
    fn correlated_stop(&self, flow_id: &str, stop: &NativeStop) -> bool {
        let Ok(Some(node)) = self.store.flow_node(flow_id) else {
            return false;
        };
        let (Some(session), Some(reported)) = (
            stop.native_session_id_option
                .as_ref()
                .map(|value| value.0.as_str()),
            stop.transcript_path_option
                .as_ref()
                .map(|value| value.0.as_str()),
        ) else {
            return false;
        };
        let Some(transcript) = (node.harness_kind == HarnessKind::Claude
            && !node.session_id.0.is_empty()
            && session == node.session_id.0)
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
                && !node.session_id.0.is_empty()
                && prompt.native_session_id_option.as_ref().map(|value| value.0.as_str()) == Some(node.session_id.0.as_str())
        )
    }

    fn turn_end_recorded(&self, flow_id: &str) -> bool {
        self.last_turn_end(flow_id).is_some()
    }

    fn last_turn_end(&self, flow_id: &str) -> Option<NativeStop> {
        let Ok(Some(events)) = self.store.events(flow_id) else {
            return None;
        };
        events
            .into_iter()
            .rev()
            .find_map(|event| match event {
                Event::Stopped(stop) => self.correlated_stop(flow_id, &stop).then_some(Some(stop)),
                Event::PromptSubmitted(prompt) => {
                    self.correlated_prompt(flow_id, &prompt).then_some(None)
                }
                Event::Started(_) | Event::ToolUsed(_) | Event::Ended(_) | Event::Cleared(_) => {
                    None
                }
            })
            .flatten()
    }
}

/// Whether a seat rests as closing it needs: its turn has ended and no
/// work it started is in flight. A Claude seat rests when the last of its
/// own Stops and submitted prompts is a Stop naming no background work, and
/// Herdr shows it resting or its transcript records its main turn's end; a
/// Codex seat when its app-server shows its thread resting and in no turn.
/// No reading of Herdr stands for the Stop: a seat whose Stop names work in
/// flight is not resting, and the Stop Claude ends its turn with once that
/// work has ended is the event of its rest.
pub(crate) trait JudgesClosingRest {
    /// The flow's own last turn event is a Stop naming no background work.
    fn background_settled(&self, flow_id: &str) -> bool;
    /// The flow rests now, by every evidence closing it needs.
    fn rests_for_closing(&self, flow_id: &str) -> bool;
}

impl JudgesClosingRest for RunningNexus {
    fn background_settled(&self, flow_id: &str) -> bool {
        self.last_turn_end(flow_id)
            .is_some_and(|stop| stop.background_work.0.is_empty())
    }

    fn rests_for_closing(&self, flow_id: &str) -> bool {
        let Ok(Some(node)) = self.store.flow_node(flow_id) else {
            return false;
        };
        match node.harness_kind {
            HarnessKind::Codex => !self.codex_in_turn(flow_id) && self.codex_resting(flow_id),
            HarnessKind::Claude => {
                self.background_settled(flow_id)
                    && (self.claude_main_turn_ended(&node)
                        || matches!(
                            self.herdr.pane_agent(&self.herdr.refresh_route(node)),
                            PaneAgent::Present { agent_state, .. } if Self::rests_at(&agent_state)
                        ))
            }
        }
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
    Settled(QueueEntry, SeatAfter),
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
    /// Records the one receipt on every attempt of `run`, the payload
    /// that carried `attempt`, and settles their requests in order.
    fn settle_received(
        &self,
        attempt: &QueueDeliveryAttempt,
        run: &[QueueDeliveryAttempt],
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

pub(crate) trait RendersAttemptInput {
    fn prompt_text(&self) -> String;
    fn as_settled(&self, seat_after: SeatAfter) -> Reconciled;
}

impl RendersAttemptInput for QueueDeliveryAttempt {
    fn prompt_text(&self) -> String {
        match &self.queue_evidence {
            meta_signal_flow::QueueEvidence::Logical(entry) => {
                [entry.clone()].prompt_text(&self.address)
            }
            meta_signal_flow::QueueEvidence::Legacy(legacy) => legacy.request.native_content(),
        }
    }
    fn as_settled(&self, seat_after: SeatAfter) -> Reconciled {
        match &self.queue_evidence {
            meta_signal_flow::QueueEvidence::Logical(entry) => {
                Reconciled::Settled(entry.clone(), seat_after)
            }
            meta_signal_flow::QueueEvidence::Legacy(_) => Reconciled::Refused,
        }
    }
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
                    attempt.as_settled(SeatAfter::InItsTurn)
                } else {
                    Reconciled::Refused
                };
            }
            QueueDeliveryPhase::Settled(_) => {
                return attempt.as_settled(SeatAfter::InItsTurn);
            }
            QueueDeliveryPhase::Read { .. } => return Reconciled::Refused,
            QueueDeliveryPhase::Absent(_) => return Reconciled::Absent,
            QueueDeliveryPhase::Resolved(record) => {
                return match record.queue_resolution_outcome {
                    meta_signal_flow::QueueResolutionOutcome::Delivered => {
                        attempt.as_settled(SeatAfter::InItsTurn)
                    }
                    meta_signal_flow::QueueResolutionOutcome::Absent => Reconciled::Absent,
                };
            }
            QueueDeliveryPhase::Prepared | QueueDeliveryPhase::Uncertain(_) => {}
        }
        match self.find_receipt(attempt) {
            Ok((run, turn_id, native_receipt, seat_after)) => {
                self.settle_received(attempt, &run, turn_id, native_receipt, seat_after)
            }
            Err(undecided) => self.undecided(attempt, undecided),
        }
    }

    fn settle_received(
        &self,
        attempt: &QueueDeliveryAttempt,
        run: &[QueueDeliveryAttempt],
        turn_id: String,
        native_receipt: String,
        seat_after: SeatAfter,
    ) -> Reconciled {
        for member in run {
            let receipt = NativeDeliveryReceipt {
                native_request_id: member.native_request_id,
                thread_id: member.thread_id.clone(),
                turn_id: turn_id.clone(),
                native_receipt: native_receipt.clone(),
            };
            if !(self
                .store
                .record_delivery_receipt(&member.attempt_id, receipt)
                .unwrap_or(false)
                && self
                    .store
                    .settle_recorded_delivery(&member.attempt_id)
                    .unwrap_or(false))
            {
                return Reconciled::Refused;
            }
        }
        attempt.as_settled(seat_after)
    }

    fn settle_submitted_prompt(&self, flow_id: &str, prompt: &NativePrompt) {
        let _turn = self
            .dispatch_gate
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let Ok(Some(metaflow)) = self.store.metaflow_awake_as(flow_id) else {
            return;
        };
        let Ok(attempts) = self.store.unsettled_deliveries(&metaflow.address) else {
            return;
        };
        let attempt = attempts.into_iter().find(|attempt| {
            attempt.flow_id == flow_id
                && attempt.harness_kind == HarnessKind::Claude
                && prompt
                    .native_session_id_option
                    .as_ref()
                    .map(|value| value.0.as_str())
                    == Some(attempt.thread_id.as_str())
                && matches!(
                    attempt.phase,
                    QueueDeliveryPhase::Prepared | QueueDeliveryPhase::Uncertain(_)
                )
                && self
                    .cohort_runs(attempt)
                    .iter()
                    .any(|(_, text)| text.prompt_sha256() == prompt.prompt_sha256.0)
        });
        let Some(attempt) = attempt else {
            // The correlated hook is durable launch evidence. The logical
            // input becomes Delivered only when confirm_wake atomically
            // applies that exact receipt and removes its startup prefix.
            return;
        };
        // A native hook proves that a submission was observed, not that an
        // exact USER row has been durably recorded. Reconcile the transcript
        // after its pre-submit anchor; an unreadable or not-yet-written row
        // remains explicitly uncertain and cannot remove the logical entry.
        let _ = self.reconcile_delivery(&attempt);
    }

    fn recover_deliveries(&self) {
        let (addresses, outstanding) = {
            let _turn = self
                .dispatch_gate
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let Ok(metaflows) = self.store.list_metaflows() else {
                eprintln!("flow-nexus: metaflows unreadable; no queued delivery reconciled");
                return;
            };
            let outstanding = metaflows
                .iter()
                .filter_map(|metaflow| {
                    self.store
                        .outstanding_delivery(&metaflow.address)
                        .ok()
                        .flatten()
                })
                .collect::<Vec<_>>();
            let addresses = metaflows
                .into_iter()
                .map(|metaflow| metaflow.address)
                .collect::<Vec<_>>();
            (addresses, outstanding)
        };
        // Native history is read off the gate, so a read that waits on its
        // harness holds no other request; each answer is settled under it.
        for attempt in outstanding {
            let found = matches!(
                attempt.phase,
                QueueDeliveryPhase::Prepared | QueueDeliveryPhase::Uncertain(_)
            )
            .then(|| self.find_receipt(&attempt));
            let _turn = self
                .dispatch_gate
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let reconciled = match found {
                Some(Ok((run, turn_id, native_receipt, seat_after))) => {
                    self.settle_received(&attempt, &run, turn_id, native_receipt, seat_after)
                }
                Some(Err(undecided)) => self.undecided(&attempt, undecided),
                None => self.reconcile_delivery(&attempt),
            };
            eprintln!("flow-nexus: a queued delivery was reconciled at start: {reconciled:?}");
        }
        self.recover_typed(&addresses);
        let seats = {
            let _turn = self
                .dispatch_gate
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            self.store
                .list_metaflows()
                .unwrap_or_default()
                .into_iter()
                .filter(|metaflow| !metaflow.queue.0.is_empty())
                .filter_map(|metaflow| match metaflow.state {
                    flow_ethos::State::Awake(flow_id) => Some(flow_id),
                    _ => None,
                })
                .filter(|flow_id| {
                    matches!(self.store.address_hold(&flow_id.0), Ok(None))
                        && self.turn_end_recorded(&flow_id.0)
                })
                .collect::<Vec<_>>()
        };
        for flow_id in seats {
            self.settle_stopped(&flow_id.0);
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
        // A restart holds the Address: no drain, sleep or refresh of it.
        if !matches!(
            crate::store::restart::RecordsRestarts::restart_record(&*self.store, flow_id),
            Ok(None)
        ) {
            eprintln!(
                "flow-nexus: flow {flow_id} kept at its Stop: {:?}",
                KeptAtStop::Restarting
            );
            return AtStop::Kept(KeptAtStop::Restarting);
        }
        let at_stop = self.settle_rested(flow_id);
        match &at_stop {
            AtStop::Kept(reason) if reason.successor_refused() => {
                eprintln!("flow-nexus: flow {flow_id} kept at its Stop: {reason:?}");
                self.successor_refused(flow_id);
            }
            AtStop::Kept(reason) => {
                eprintln!("flow-nexus: flow {flow_id} kept at its Stop: {reason:?}");
            }
            // Work queued for a refreshed metaflow is handed to its
            // successor, or wakes its next flow once the gate the refresh ran
            // under is given back; with nothing queued an OnDemand metaflow
            // sleeps until an Order, a Question or the living's words.
            AtStop::Refreshed(address) => {
                eprintln!("flow-nexus: flow {flow_id} refreshed at its rest");
                self.refresh_refusals.clear(flow_id);
                self.wake_due(address.clone());
            }
            AtStop::Slept(_) => {
                eprintln!("flow-nexus: flow {flow_id} slept at its rest");
                self.refresh_refusals.clear(flow_id);
            }
            AtStop::Idle | AtStop::Drained(_) => {}
        }
        // The recipient of an answer queued at this rest is woken off the
        // gate.
        self.wake_deferred();
        at_stop
    }

    fn drain_awake(&self, address: &Address) {
        {
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
            if metaflow.queue.0.is_empty()
                || self.waking.is_waking(address)
                || !matches!(self.store.address_hold(&flow_id.0), Ok(None))
                || !matches!(
                    crate::store::restart::RecordsRestarts::restart_record(
                        &*self.store,
                        &flow_id.0
                    ),
                    Ok(None)
                )
            {
                return;
            }
            let Ok(Some(node)) = self.store.flow_node(&flow_id.0) else {
                return;
            };
            if matches!(
                self.herdr.pane_presence(&node),
                PanePresence::Absent | PanePresence::Bare
            ) {
                let _ = self.perform(Operation::Record(Record_Data::Exited(flow_id)));
                return;
            }
            // Pressing work is handed over now, the Soft work queued before
            // it included; the delivery engine interrupts only for
            // HardAbrupt. Soft work alone waits for the seat's idle event.
            if self.waits_for_idle(&metaflow.queue.0, &flow_id.0) {
                return;
            }
            if let Some(lease) = self.waking.hold(address) {
                let _ = self.drain_all_held(address.clone(), &lease);
            }
        }
        self.wake_deferred();
    }

    fn wake_due(&self, address: Address) {
        // Work queued for an Awake seat known resting is handed to it now,
        // as its turn's end would; one in a turn waits for the turn's end.
        // An Awake seat whose pane is gone is put Asleep, to wake below.
        self.drain_awake(&address);
        if self.settle_orphaned_order(&address) {
            return;
        }
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
            match self.drain_held(address.clone(), &lease) {
                Outcome::Drained(typed) if typed.is_empty() => {
                    drop(lease);
                    return;
                }
                Outcome::Drained(_) => {}
                _outcome => {
                    eprintln!("flow-nexus: work queued during a wake stays queued");
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
            .0
            .last()
            .and_then(|flow_id| self.last_launch_profile(&flow_id.0));
        // A declared metaflow wakes with the modules it is bound to; one
        // never declared, with those its last launch named.
        match (self.store.metaflow_declaration(&metaflow.address), last) {
            (Ok(Some(declared)), last) => WakePlan::Fresh(FreshContext {
                herdr_session_name: last
                    .map(|profile| profile.herdr_session_name)
                    .unwrap_or_else(|| signal_flow::HerdrSessionName("default".to_owned())),
                flow_kind: declared.flow_kind.clone(),
                tool_set: declared.tool_set.clone(),
                system_modules: declared.system_modules,
                user_modules: declared.user_modules,
                leave_outs_option: None,
                purpose: crate::generated::starter::Purpose(declared.purpose.0),
            }),
            (_, Some(profile)) => WakePlan::Fresh(FreshContext {
                herdr_session_name: profile.herdr_session_name,
                flow_kind: profile.flow_kind.clone(),
                tool_set: profile.tool_set.clone(),
                system_modules: profile.system_modules,
                user_modules: profile.user_modules,
                leave_outs_option: profile.leave_outs_option,
                purpose: crate::generated::starter::Purpose(String::new()),
            }),
            (_, None) => WakePlan::Unplaceable,
        }
    }

    fn wake_is_due(&self, metaflow: &Metaflow) -> bool {
        metaflow.state == flow_ethos::State::Asleep
            && (metaflow
                .queue
                .0
                .iter()
                .any(|entry| !matches!(entry.message, signal_flow::Message::Soft(_)))
                || matches!(self.store.successor_intent(&metaflow.address), Ok(Some(_))))
    }

    fn recover_wakes(&self) {
        self.recover_deliveries();
        // A restart an earlier Nexus left is taken up first, so a seat it
        // binds is handed its queue below.
        crate::restarting::RestartsFlows::recover_restarts(self);
        // What an earlier Nexus left queued for an Awake seat is handed
        // over now, each address's whole queue as one payload.
        let awake = {
            let _turn = self
                .dispatch_gate
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            self.store
                .list_metaflows()
                .unwrap_or_default()
                .into_iter()
                .filter(|metaflow| {
                    !metaflow.queue.0.is_empty()
                        && matches!(metaflow.state, flow_ethos::State::Awake(_))
                })
                .map(|metaflow| metaflow.address)
                .collect::<Vec<_>>()
        };
        for address in awake {
            self.drain_awake(&address);
        }
        // A successor an earlier Nexus owed and did not see confirmed is
        // taken up again while its predecessor is still the flow the
        // metaflow is Awake as. A threshold refresh's predecessor is
        // refreshed at its rest, under the same launch request. A Refresh
        // ordered from outside, or a Replace, has its successor started
        // whatever the predecessor's context. An intent another flow took
        // the Address for is forgotten, unless that flow's own launch is
        // still unconfirmed. An Asleep metaflow owed one is due a wake,
        // below.
        let (refreshes, owed) = {
            let _turn = self
                .dispatch_gate
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let mut refreshes = Vec::new();
            let mut owed = Vec::new();
            for intent in self.store.successor_intents().unwrap_or_default() {
                match self.store.metaflow(&intent.address) {
                    Ok(Some(metaflow)) => match metaflow.state {
                        flow_ethos::State::Awake(current) if current.0 == intent.predecessor => {
                            match intent.cause {
                                SuccessionCause::Refresh | SuccessionCause::Exit => {
                                    refreshes.push(current);
                                }
                                SuccessionCause::Ordered | SuccessionCause::Replace => {
                                    owed.push(intent);
                                }
                            }
                        }
                        flow_ethos::State::Awake(current) => {
                            if !matches!(self.store.address_hold(&current.0), Ok(Some(_))) {
                                let _ = self.store.forget_successor(&intent.address);
                            }
                        }
                        flow_ethos::State::Asleep => {}
                    },
                    Ok(None) => {
                        let _ = self.store.forget_successor(&intent.address);
                    }
                    Err(_) => {}
                }
            }
            (refreshes, owed)
        };
        for flow_id in refreshes {
            self.settle_stopped(&flow_id.0);
        }
        for intent in owed {
            let predecessor = intent.predecessor.clone();
            match self.take_up_owed(intent) {
                AtStop::Refreshed(address) => {
                    eprintln!(
                        "flow-nexus: flow {predecessor} replaced by the successor it was owed"
                    );
                    // What queued meanwhile is handed to the successor.
                    self.wake_due(address);
                }
                kept => {
                    eprintln!(
                        "flow-nexus: successor owed after {predecessor} not started: {kept:?}"
                    );
                }
            }
        }
        // A replacement an earlier Nexus recorded waiting for its flow's
        // rest is carried out now if the flow rests, else at its next idle
        // event.
        self.recover_ordered_replacements();
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

/// A refresh ordered from outside: the metaflow's current flow ends
/// without a turn. A Maintained metaflow, or one holding a request that asks
/// for a wake, has its successor started and confirmed in the same event,
/// before the ending flow is reaped; any other is left Asleep, and its
/// successor runs on the next waking request.
pub trait RefreshesMetaflows {
    /// Runs off the dispatch gate, taking it for each step as a wake does.
    /// Takes the metaflow's lock, the hold a wake takes, so nothing is
    /// delivered to it and what arrives queues. A metaflow owed its
    /// successor has it started and confirmed through Replace, which then
    /// reaps the ending flow; once the lock is released the queue is handed
    /// to the successor, oldest first. Any other has its flow's harness
    /// closed under its pane's lease, typing nothing, and its flow recorded
    /// Stopped, which puts the metaflow Asleep with the flow in Past and its
    /// queue kept. The successor's context is what any wake composes: the
    /// metaflow's declaration, else the last launch's modules, remembering
    /// the ending flow.
    fn refresh(&self, address: &Address) -> Response;
    /// A Replace, as a Launch addressed to an Awake metaflow names one: a
    /// refresh whose successor is the profile's. Under the metaflow's lock
    /// the successor is started and confirmed, then the flow it replaces is
    /// reaped, given no turn; once the lock is released the queue is handed
    /// to the successor, oldest first. A metaflow another holds is refused
    /// Locked. Runs off the dispatch gate, taking it for the replacement.
    fn replace_held(&self, profile: LaunchProfile, origin: Origin) -> Response;
}

impl RefreshesMetaflows for RunningNexus {
    fn refresh(&self, address: &Address) -> Response {
        use crate::store::metaflow_declaration::ReadsMetaflowDeclarations;
        use signal_flow::{RefreshRejection, Refusal, Unknown_Data};
        let turn = self
            .dispatch_gate
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let Some(lock) = self.waking.hold(address) else {
            return Response::RefreshRejected(RefreshRejection::Locked);
        };
        self.rest_ended_awake(address);
        let metaflow = match self.store.metaflow(address) {
            Ok(Some(
                metaflow @ Metaflow {
                    state: flow_ethos::State::Awake(_),
                    ..
                },
            )) => metaflow,
            Ok(Some(_)) => return Response::RefreshRejected(RefreshRejection::NotAwake),
            Ok(None) => {
                return match self.store.metaflow_declaration(address) {
                    Ok(Some(_)) => Response::RefreshRejected(RefreshRejection::NotAwake),
                    Ok(None) => {
                        Response::Refused(Refusal::Unknown(Unknown_Data::Address(address.clone())))
                    }
                    Err(error) => Response::Refused(Refusal::Store(error.to_string())),
                };
            }
            Err(error) => return Response::Refused(Refusal::Store(error.to_string())),
        };
        // A restart holds the flow's Address as a refresh would.
        if let flow_ethos::State::Awake(current) = &metaflow.state
            && !matches!(
                crate::store::restart::RecordsRestarts::restart_record(&*self.store, &current.0),
                Ok(None)
            )
        {
            return Response::RefreshRejected(RefreshRejection::Locked);
        }
        let flow_ethos::State::Awake(flow_id) = metaflow.state.clone() else {
            return Response::RefreshRejected(RefreshRejection::NotAwake);
        };
        // A flow whose launch is not yet confirmed only holds its Address:
        // the launch that holds it is the owed successor's, still pending
        // or ambiguous, or a waking still running.
        match self.store.address_hold(&flow_id.0) {
            Ok(None) => {}
            Ok(Some(_)) => {
                return Response::RefreshRejected(match self.store.successor_intent(address) {
                    Ok(Some(_)) => RefreshRejection::SuccessorUnconfirmed,
                    Ok(None) => RefreshRejection::Locked,
                    Err(error) => return Response::Refused(Refusal::Store(error.to_string())),
                });
            }
            Err(error) => return Response::Refused(Refusal::Store(error.to_string())),
        }
        let node = match self.store.flow_node(&flow_id.0) {
            Ok(Some(node)) => node,
            Ok(None) => {
                return Response::Refused(Refusal::Unknown(Unknown_Data::FlowId(flow_id)));
            }
            Err(error) => return Response::Refused(Refusal::Store(error.to_string())),
        };
        // The recorded route is read first: a Herdr that cannot be read
        // shows no pane, and is never taken for one gone.
        match self.herdr.pane_presence(&node) {
            PanePresence::Present => {}
            // A flow whose pane is gone has ended on its own: it is recorded
            // Exited, which puts the metaflow Asleep.
            PanePresence::Absent | PanePresence::Bare => {
                let _ = self.perform(Operation::Record(Record_Data::Exited(flow_id)));
                return Response::RefreshRejected(RefreshRejection::NotAwake);
            }
            PanePresence::Unknown => {
                return Response::RefreshRejected(RefreshRejection::HerdrUnavailable);
            }
        }
        let node = self.herdr.refresh_route(node);
        // A refresh never closes a working flow, whoever asks: one that does
        // not rest now is refreshed at its idle event.
        if !self.rests_for_closing(&flow_id.0) {
            return self.queue_refresh(address, &flow_id.0);
        }
        self.refresh_held(address, metaflow, flow_id, node, turn, lock)
    }

    fn replace_held(&self, profile: LaunchProfile, origin: Origin) -> Response {
        use signal_flow::ReplaceRejection;
        let address = profile.address.clone();
        let response = {
            let turn = self
                .dispatch_gate
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let Some(lock) = self.waking.hold(&address) else {
                return Response::ReplaceRejected(ReplaceRejection::Locked);
            };
            // A restart holds the Address its predecessor runs at.
            if let Ok(Some(_)) =
                crate::store::restart::RecordsRestarts::restart_at(&*self.store, &address)
            {
                return Response::ReplaceRejected(ReplaceRejection::Locked);
            }
            // A working predecessor is never closed: the replacement waits
            // for its idle event. One already under way or settled resumes.
            if let Some(predecessor) = profile.flow_id_option.clone()
                && matches!(
                    self.store.metaflow(&address),
                    Ok(Some(metaflow)) if metaflow.state == flow_ethos::State::Awake(predecessor.clone())
                )
                && !self.launch_known(&profile.launch_request_id.0)
                && !self.rests_for_closing(&predecessor.0)
            {
                return self.queue_replacement(profile, origin, predecessor);
            }
            drop(turn);
            let response = self.replace_owed(profile, origin);
            drop(lock);
            response
        };
        self.wake_deferred();
        // What queued for the metaflow is handed to the flow it is Awake as
        // now, oldest first.
        self.wake_due(address);
        response
    }
}

/// Replacements ordered from outside wait for their flow's rest. A
/// Refresh.Address, whoever asks, and a Launch or Replace naming a live
/// flow, are carried out at once only when that flow rests; otherwise the
/// order is recorded, answered RefreshQueued or LaunchPending, and carried
/// out at the idle event that finds the flow resting. Until then the flow
/// is handed what it is sent; then, under the metaflow's hold, the queue
/// waits for the successor, as at a threshold refresh.
pub(crate) trait WaitsForRestToReplace {
    /// The refresh of a resting flow under the metaflow's `lock` and the
    /// gate `turn`, both released by it: see [`RefreshesMetaflows::refresh`].
    #[allow(clippy::too_many_arguments)]
    fn refresh_held(
        &self,
        address: &Address,
        metaflow: Metaflow,
        flow_id: FlowId,
        node: FlowNode,
        turn: std::sync::MutexGuard<'_, ()>,
        lock: WakingLease<'_>,
    ) -> Response;
    /// Records a Refresh of the working `flow_id`, under the gate, and
    /// answers RefreshQueued; a Launch already waiting for it refuses it
    /// Locked.
    fn queue_refresh(&self, address: &Address, flow_id: &str) -> Response;
    /// Records the Launch or Replace of the working `predecessor`, under the
    /// gate, once its successor composes, and answers LaunchPending with the
    /// attempt it waits as; another order waiting for it refuses it Locked.
    fn queue_replacement(
        &self,
        profile: LaunchProfile,
        origin: Origin,
        predecessor: FlowId,
    ) -> Response;
    /// The replacement, under the metaflow's hold the caller has: its
    /// successor owed until confirmed, started and confirmed, then its
    /// predecessor reaped. Takes the gate.
    fn replace_owed(&self, profile: LaunchProfile, origin: Origin) -> Response;
    /// Whether Flow already holds an attempt, an outcome or a replacement of
    /// this launch request.
    fn launch_known(&self, launch_request_id: &str) -> bool;
    /// Carries out the order at the rest of its flow, under the gate `turn`
    /// the caller holds; the order is spent whatever it comes to.
    fn replace_as_ordered(
        &self,
        order: OrderedReplacement,
        metaflow: Metaflow,
        node: FlowNode,
        turn: std::sync::MutexGuard<'_, ()>,
    ) -> AtStop;
    /// A Launch ordered for a flow that ended before it rested starts as its
    /// metaflow's next flow; a Refresh ordered for it is spent. True when a
    /// Launch was started, whose own wake hands it the queue.
    fn settle_orphaned_order(&self, address: &Address) -> bool;
    /// Starts the Launch of `order` with no predecessor.
    fn start_ordered(&self, order: OrderedReplacement, launch: OrderedLaunch);
    /// At the Nexus's start: every order whose flow is still current is
    /// carried out if it rests, else waits for its next idle event; one
    /// whose metaflow is Asleep is settled as an orphan; any other is spent.
    fn recover_ordered_replacements(&self);
}

impl WaitsForRestToReplace for RunningNexus {
    fn refresh_held(
        &self,
        address: &Address,
        metaflow: Metaflow,
        flow_id: FlowId,
        node: FlowNode,
        turn: std::sync::MutexGuard<'_, ()>,
        lock: WakingLease<'_>,
    ) -> Response {
        use signal_flow::{RefreshRejection, Refreshed, Refusal};
        let continuity = match self.store.continuity(address) {
            Ok(continuity) => continuity,
            Err(error) => return Response::Refused(Refusal::Store(error.to_string())),
        };
        let wakes = metaflow
            .queue
            .0
            .iter()
            .any(|entry| !matches!(entry.message, signal_flow::Message::Soft(_)));
        let refreshed = Response::Refreshed(Refreshed {
            address: address.clone(),
            flow_id: flow_id.clone(),
        });
        // A Maintained metaflow is never left without a flow, and one holding
        // a waking request is owed the flow that takes it up: the successor
        // is started and confirmed before the ending flow is reaped.
        if continuity == Continuity::Maintained || wakes {
            // Replace takes the gate for its own steps; the lock keeps any
            // wake, drain or delivery of the metaflow out meanwhile.
            drop(turn);
            let replaced =
                self.refresh_successor_first(&flow_id.0, &metaflow, SuccessionCause::Ordered);
            drop(lock);
            self.wake_deferred();
            return match replaced {
                AtStop::Refreshed(_) => {
                    eprintln!(
                        "flow-nexus: flow {flow_id} refreshed; {:?} {} {:?} runs its successor",
                        address.aspect,
                        address.topic.0,
                        address.model,
                        flow_id = flow_id.0
                    );
                    // What queued while the lock stood is handed to the
                    // successor, oldest first.
                    self.wake_due(address.clone());
                    refreshed
                }
                // A refused successor answers the Replace's own rejection;
                // one whose launch is still pending or ambiguous when the
                // confirmation wait ends is unconfirmed, and stays owed.
                AtStop::Kept(KeptAtStop::RefreshLaunchRefused(response)) => {
                    Response::RefreshRejected(match *response {
                        Response::ReplaceRejected(rejection) => {
                            RefreshRejection::SuccessorRefused(rejection)
                        }
                        Response::StartRejected(rejection) => RefreshRejection::SuccessorRefused(
                            signal_flow::ReplaceRejection::LaunchRefused(rejection),
                        ),
                        Response::LaunchPending(_) | Response::StartAmbiguous(_) => {
                            RefreshRejection::SuccessorUnconfirmed
                        }
                        _ => RefreshRejection::PersistenceRefused,
                    })
                }
                AtStop::Kept(KeptAtStop::RefreshUnplaceable) => {
                    Response::RefreshRejected(RefreshRejection::Undeclared)
                }
                _ => Response::RefreshRejected(RefreshRejection::PersistenceRefused),
            };
        }
        if let Err(outcome) = self.close_at_rest(&flow_id.0, &node) {
            return Response::RefreshRejected(match *outcome {
                Outcome::Failed(Failed_Data::HerdrRefused) => RefreshRejection::CloseRefused,
                _ => RefreshRejection::PersistenceRefused,
            });
        }
        // The lock is released under the gate, so a Send admitted after it
        // finds the metaflow Asleep and free, and its waking request wakes
        // it.
        drop(lock);
        drop(turn);
        eprintln!(
            "flow-nexus: flow {flow_id} refreshed; {:?} {} {:?} sleeps until a waking request",
            address.aspect,
            address.topic.0,
            address.model,
            flow_id = flow_id.0
        );
        refreshed
    }

    fn queue_refresh(&self, address: &Address, flow_id: &str) -> Response {
        use signal_flow::{RefreshQueued, RefreshRejection, Refusal};
        match self.store.ordered_replacement(address) {
            Ok(Some(order)) if order.predecessor == flow_id => {
                return match order.order {
                    ReplacementOrder::Refresh => Response::RefreshQueued(RefreshQueued {
                        address: address.clone(),
                        flow_id: signal_flow::FlowId(flow_id.to_owned()),
                    }),
                    ReplacementOrder::Launch(_) => {
                        Response::RefreshRejected(RefreshRejection::Locked)
                    }
                };
            }
            Ok(_) => {}
            Err(error) => return Response::Refused(Refusal::Store(error.to_string())),
        }
        if let Err(error) = self.store.order_replacement(OrderedReplacement {
            address: address.clone(),
            predecessor: flow_id.to_owned(),
            order: ReplacementOrder::Refresh,
        }) {
            return Response::Refused(Refusal::Store(error.to_string()));
        }
        eprintln!("flow-nexus: flow {flow_id} works; its refresh waits for its rest");
        Response::RefreshQueued(RefreshQueued {
            address: address.clone(),
            flow_id: signal_flow::FlowId(flow_id.to_owned()),
        })
    }

    fn queue_replacement(
        &self,
        mut profile: LaunchProfile,
        origin: Origin,
        predecessor: FlowId,
    ) -> Response {
        use crate::launching::{InterpretsLaunchObservations, ResumesReplacement};
        use signal_flow::{ReplaceRejection, StartRejection};
        let persistence = || {
            Response::ReplaceRejected(ReplaceRejection::LaunchRefused(
                StartRejection::LaunchPersistenceRefused,
            ))
        };
        let address = profile.address.clone();
        // The model the Replace launches with, as it will at the rest.
        match self.store.configured_model(&address) {
            Ok(Some(model)) => profile.model_name = signal_flow::ModelName(model),
            Ok(None) | Err(_) => return persistence(),
        }
        match self.store.ordered_replacement(&address) {
            Ok(Some(order)) if order.predecessor == predecessor.0 => {
                return match order.ordered_launch() {
                    Some(launch) if launch.launch_profile == profile && launch.origin == origin => {
                        Response::LaunchPending(launch.waiting_attempt())
                    }
                    _ => Response::ReplaceRejected(ReplaceRejection::Locked),
                };
            }
            Ok(_) => {}
            Err(_) => return persistence(),
        }
        // A successor that cannot compose is refused now, not at the rest.
        let mut composing = profile.clone();
        composing.launch_request_id =
            signal_flow::LaunchRequestId(crate::sending::MintsId::minted_id("compose"));
        let prompt_sha256 = match self.perform(Operation::Compose(composing)) {
            Outcome::Composed(launch) => launch.first_prompt_payload.prompt_sha256,
            refused => return Self::as_replacement(Self::composition_refused(refused)),
        };
        let launch = OrderedLaunch {
            launch_profile: profile,
            origin,
            prompt_sha256: prompt_sha256.0,
        };
        if self
            .store
            .order_replacement(OrderedReplacement {
                address,
                predecessor: predecessor.clone().0,
                order: ReplacementOrder::Launch(Box::new(launch.clone())),
            })
            .is_err()
        {
            return persistence();
        }
        eprintln!(
            "flow-nexus: flow {predecessor} works; its replacement waits for its rest",
            predecessor = predecessor.0
        );
        Response::LaunchPending(launch.waiting_attempt())
    }

    fn replace_owed(&self, profile: LaunchProfile, origin: Origin) -> Response {
        use signal_flow::{ReplaceRejection, StartRejection};
        let address = profile.address.clone();
        let _turn = self
            .dispatch_gate
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        // The successor is owed until it is confirmed, so a Nexus that
        // stops before then starts it at its next start.
        if let Some(predecessor) = profile.flow_id_option.clone()
            && self
                .store
                .owe_successor(SuccessorIntent {
                    address: address.clone(),
                    predecessor: predecessor.0,
                    cause: SuccessionCause::Replace,
                    launch_request_id: profile.launch_request_id.clone().0,
                })
                .is_err()
        {
            return Response::ReplaceRejected(ReplaceRejection::LaunchRefused(
                StartRejection::LaunchPersistenceRefused,
            ));
        }
        let response = self.replace(profile, origin);
        self.settle_owed(&address, &response);
        response
    }

    fn launch_known(&self, launch_request_id: &str) -> bool {
        use crate::store::{RecordsLaunchOutcome, RecordsReplacement};
        !matches!(self.store.launch_attempt(launch_request_id), Ok(None))
            || !matches!(self.store.launch_outcome(launch_request_id), Ok(None))
            || !matches!(self.store.replacement(launch_request_id), Ok(None))
    }

    fn replace_as_ordered(
        &self,
        order: OrderedReplacement,
        metaflow: Metaflow,
        node: FlowNode,
        turn: std::sync::MutexGuard<'_, ()>,
    ) -> AtStop {
        let address = metaflow.address.clone();
        let Some(lock) = self.waking.hold(&address) else {
            return AtStop::Kept(KeptAtStop::Waking);
        };
        eprintln!(
            "flow-nexus: flow {} rests; the replacement ordered for it runs",
            order.predecessor
        );
        let response = match order.order {
            ReplacementOrder::Refresh => self.refresh_held(
                &address,
                metaflow,
                signal_flow::FlowId(order.predecessor),
                node,
                turn,
                lock,
            ),
            ReplacementOrder::Launch(launch) => {
                drop(turn);
                let OrderedLaunch {
                    launch_profile,
                    origin,
                    ..
                } = *launch;
                let response = self.replace_owed(launch_profile, origin);
                drop(lock);
                self.wake_deferred();
                response
            }
        };
        let _ = self.store.forget_ordered_replacement(&address);
        match response {
            Response::Refreshed(_) | Response::Replaced(_) => AtStop::Refreshed(address),
            response => AtStop::Kept(KeptAtStop::OrderedRefused(Box::new(response))),
        }
    }

    fn settle_orphaned_order(&self, address: &Address) -> bool {
        let orphan = {
            let _turn = self
                .dispatch_gate
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            match (
                self.store.ordered_replacement(address),
                self.store.metaflow(address),
            ) {
                (Ok(Some(order)), Ok(Some(metaflow)))
                    if metaflow.state == flow_ethos::State::Asleep =>
                {
                    Some(order)
                }
                _ => None,
            }
        };
        let Some(order) = orphan else {
            return false;
        };
        match order.ordered_launch().cloned() {
            Some(launch) => {
                self.start_ordered(order, launch);
                true
            }
            None => {
                eprintln!(
                    "flow-nexus: flow {} ended before it rested; its refresh is spent",
                    order.predecessor
                );
                let _ = self.store.forget_ordered_replacement(address);
                false
            }
        }
    }

    fn start_ordered(&self, order: OrderedReplacement, launch: OrderedLaunch) {
        let mut profile = launch.launch_profile;
        profile.flow_id_option = None;
        let launch_request_id = profile.launch_request_id.clone();
        let response = self.start(profile, launch.origin);
        let response = self.settle(&launch_request_id.0, response);
        let _ = self.store.forget_ordered_replacement(&order.address);
        eprintln!(
            "flow-nexus: flow {} ended before it rested; the Launch ordered for it starts: {response:?}",
            order.predecessor
        );
        self.wake_due(order.address);
    }

    fn recover_ordered_replacements(&self) {
        let (resting, orphans) = {
            let _turn = self
                .dispatch_gate
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let mut resting = Vec::new();
            let mut orphans = Vec::new();
            for order in self.store.ordered_replacements().unwrap_or_default() {
                match self.store.metaflow(&order.address) {
                    Ok(Some(metaflow)) => match metaflow.state {
                        flow_ethos::State::Awake(current) if current.0 == order.predecessor => {
                            resting.push(current);
                        }
                        flow_ethos::State::Asleep => orphans.push(order.address),
                        flow_ethos::State::Awake(_) => {
                            let _ = self.store.forget_ordered_replacement(&order.address);
                        }
                    },
                    Ok(None) => {
                        let _ = self.store.forget_ordered_replacement(&order.address);
                    }
                    Err(_) => {}
                }
            }
            (resting, orphans)
        };
        // A flow that rests now has had its idle event; one that works is
        // kept for its next.
        for flow_id in resting {
            if self.rests_for_closing(&flow_id.0) {
                self.settle_stopped(&flow_id.0);
            }
        }
        for address in orphans {
            self.settle_orphaned_order(&address);
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
                    address.aspect, address.topic.0, address.model
                );
                // Nothing was taken: the work is still queued, oldest first.
                Outcome::Failed(refusal.failure())
            }
        }
    }

    fn drain(&self, address: Address) -> Outcome {
        let Some(lease) = self.waking.hold(&address) else {
            return Outcome::Failed(Failed_Data::NotAwake);
        };
        self.drain_held(address, &lease)
    }

    fn drain_all(&self, address: Address) -> Outcome {
        let Some(lease) = self.waking.hold(&address) else {
            return Outcome::Failed(Failed_Data::NotAwake);
        };
        self.drain_all_held(address, &lease)
    }
}

pub(crate) trait DrainsHeldSeats {
    fn drain_held(&self, address: Address, lease: &WakingLease<'_>) -> Outcome;
    fn drain_all_held(&self, address: Address, lease: &WakingLease<'_>) -> Outcome;
}

impl DrainsHeldSeats for RunningNexus {
    fn drain_held(&self, address: Address, lease: &WakingLease<'_>) -> Outcome {
        let flow_id = match self.store.metaflow(&address) {
            Ok(Some(Metaflow {
                state: flow_ethos::State::Awake(flow_id),
                ..
            })) => flow_id,
            Ok(_) => return Outcome::Failed(Failed_Data::NotAwake),
            Err(_) => return Outcome::Failed(Failed_Data::StoreRefused),
        };
        let node = match self.store.flow_node(&flow_id.0) {
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
                    if attempt.flow_id == flow_id.0
                        && attempt.thread_id == node.session_id.0
                        && node.harness_kind == HarnessKind::Codex
                    {
                        // Only this bound seat's turn can hold its work.
                        // A predecessor's receipt never marks its successor busy.
                        // The settled turn may still run: what follows waits
                        // for its end.
                        self.codex_turns.delivered(&node.session_id.0);
                        return Outcome::Drained(vec![queued]);
                    }
                    drained.push(queued);
                }
                Reconciled::Absent => {}
                // An attempt native history cannot decide (absence
                // unprovable, a Codex seat not resting, a history replaced or
                // unreadable) is parked: it stays Uncertain, is never typed
                // again, and settles on a later receipt, while the queue
                // behind it moves on.
                Reconciled::Undecided(undecided) => {
                    eprintln!(
                        "flow-nexus: a queued delivery remains uncertain ({undecided:?}); parking it without retry"
                    );
                    if !self
                        .store
                        .park_uncertain_delivery(&attempt.attempt_id)
                        .unwrap_or(false)
                    {
                        return Outcome::Failed(Failed_Data::StoreRefused);
                    }
                    return match self.drain_held(address.clone(), lease) {
                        Outcome::Drained(mut following) => {
                            drained.append(&mut following);
                            Outcome::Drained(drained)
                        }
                        Outcome::AwaitingReceipt(id) if drained.is_empty() => {
                            Outcome::AwaitingReceipt(id)
                        }
                        Outcome::AwaitingReceipt(_) => Outcome::Drained(drained),
                        Outcome::WaitingForTurn(id) if drained.is_empty() => {
                            Outcome::WaitingForTurn(id)
                        }
                        Outcome::WaitingForTurn(_) => Outcome::Drained(drained),
                        Outcome::Failed(reason) if drained.is_empty() => Outcome::Failed(reason),
                        Outcome::Failed(_) => Outcome::Drained(drained),
                        other => other,
                    };
                }
                Reconciled::Refused => return Outcome::Failed(Failed_Data::StoreRefused),
            },
            Err(_) => return Outcome::Failed(Failed_Data::StoreRefused),
        }
        // Payloads typed earlier settle on the receipts native history
        // now shows, and a simplified seat's answer to the input it was
        // bound to is queued once its transcript shows it, so the binding
        // is free for the request now oldest.
        self.reconcile_typed(&address);
        self.queue_simplified_answer(&flow_id.0);
        let queue = match self.store.metaflow(&address) {
            Ok(Some(metaflow)) => metaflow.queue,
            _ => return Outcome::Failed(Failed_Data::StoreRefused),
        };
        let Some(oldest) = queue.0.first().cloned() else {
            return Outcome::Drained(drained);
        };
        let response_request = matches!(
            &oldest.message,
            signal_flow::Message::HardAbrupt(
                signal_flow::Request::Order(_)
                    | signal_flow::Request::Question(_)
                    | signal_flow::Request::Psyche(_)
                    | signal_flow::Request::Psyches(_)
                    | signal_flow::Request::Task(_)
            ) | signal_flow::Message::MiddleAbrupt(
                signal_flow::Request::Order(_)
                    | signal_flow::Request::Question(_)
                    | signal_flow::Request::Psyche(_)
                    | signal_flow::Request::Psyches(_)
                    | signal_flow::Request::Task(_)
            ) | signal_flow::Message::Soft(
                signal_flow::Request::Order(_)
                    | signal_flow::Request::Question(_)
                    | signal_flow::Request::Psyche(_)
                    | signal_flow::Request::Psyches(_)
                    | signal_flow::Request::Task(_)
            )
        );
        if response_request
            && self
                .last_launch_profile(&flow_id.0)
                .is_some_and(|profile| profile.flow_kind == signal_flow::FlowKind::Simplified)
            && let signal_flow::SentBy::Sender(recipient) = &oldest.sent_by
        {
            let binding = SimplifiedReplyBinding {
                flow_id: flow_id.clone().0,
                input_delivery_id: oldest.logical_delivery_id.clone().0,
                source: address.clone(),
                recipient: recipient.clone(),
                completed: false,
            };
            if self.store.bind_simplified_reply(binding).is_err() {
                return Outcome::WaitingForTurn(oldest.logical_delivery_id);
            }
        }
        // One injection carries every queued message, oldest first: the
        // triggering one and all held behind it. A Simplified seat binds its
        // reply to one input, so it takes its oldest alone.
        let simplified = self
            .last_launch_profile(&flow_id.0)
            .is_some_and(|profile| profile.flow_kind == signal_flow::FlowKind::Simplified);
        let cohort: Vec<QueueEntry> = if simplified {
            vec![oldest.clone()]
        } else {
            queue.0
        };
        let refused = |drained: Vec<QueueEntry>, failure| {
            if drained.is_empty() {
                Outcome::Failed(failure)
            } else {
                Outcome::Drained(drained)
            }
        };
        // Native history is anchored before any native input.
        let anchor = match node.harness_kind {
            HarnessKind::Claude => match self.claude_delivery_cursor(&node) {
                Some(cursor) => NativeHistoryAnchor::ClaudeTranscript(cursor),
                None => return refused(drained, Failed_Data::HerdrRefused),
            },
            HarnessKind::Codex => {
                let model = match self.store.role(&flow_id.0) {
                    Ok(Some(role)) => role.model_name,
                    _ => return refused(drained, Failed_Data::StoreRefused),
                };
                let adapter = match self.codex_endpoints.current().adapter_for(&model.0) {
                    Ok(adapter) => adapter,
                    Err(_) => return refused(drained, Failed_Data::CodexRefused),
                };
                match adapter.newest_turn(&node.session_id.0) {
                    Ok(newest) => NativeHistoryAnchor::CodexTurns(newest),
                    Err(_) => return refused(drained, Failed_Data::CodexRefused),
                }
            }
        };
        let attempt = match self.store.prepare_oldest_delivery(
            &address,
            &flow_id.0,
            &node.session_id.0,
            node.harness_kind.clone(),
            anchor,
        ) {
            Ok(PrepareQueueDelivery::Prepared(attempt)) => attempt,
            Ok(PrepareQueueDelivery::Empty) => return Outcome::Drained(drained),
            Ok(PrepareQueueDelivery::Outstanding(_)) | Err(_) => {
                return refused(drained, Failed_Data::StoreRefused);
            }
        };
        let absent = |observation: String| {
            self.store
                .mark_delivery_absent(
                    &attempt.attempt_id,
                    AbsenceRecord {
                        proof: AbsenceProof::HandOverRefused,
                        observation,
                    },
                )
                .unwrap_or(false)
        };
        // One Waking carries the whole cohort, every input under its own id,
        // sender and priority. Every priority uses the same pane delivery;
        // only a cohort containing HardAbrupt interrupts a working seat.
        use crate::message_projection::ProjectsQueueEntry;
        let waking = cohort
            .iter()
            .map(|entry| entry.visible_input(&address))
            .collect::<Vec<_>>();
        let visible_sender = oldest.visible_input(&address).visible_sender;
        let letter = meta_signal_flow::Letter {
            visible_sender,
            content: meta_signal_flow::Content::Waking(waking),
        };
        let message = if cohort
            .iter()
            .any(|entry| matches!(entry.message, signal_flow::Message::HardAbrupt(_)))
        {
            meta_signal_flow::Message::HardAbrupt(letter)
        } else if cohort
            .iter()
            .any(|entry| matches!(entry.message, signal_flow::Message::MiddleAbrupt(_)))
        {
            meta_signal_flow::Message::MiddleAbrupt(letter)
        } else {
            meta_signal_flow::Message::Soft(letter)
        };
        let result = self.deliver_while_waking(
            meta_signal_flow::DeliveryRequest {
                delivery_id: meta_signal_flow::DeliveryId(attempt.attempt_id.clone()),
                flow_id: flow_id.clone(),
                message,
            },
            lease,
        );
        let proved_absent = matches!(&result,
            meta_signal_flow::Response::DeliveryRejected(rejected)
                if matches!(rejected,
                    meta_signal_flow::DeliveryRejection::UnknownFlow
                    | meta_signal_flow::DeliveryRejection::FlowStopped
                    | meta_signal_flow::DeliveryRejection::RouteUnavailable
                    | meta_signal_flow::DeliveryRejection::RecipientWorking
                    | meta_signal_flow::DeliveryRejection::RecipientBlocked
                    | meta_signal_flow::DeliveryRejection::ComposerOccupied
                    | meta_signal_flow::DeliveryRejection::NotDelivered));
        if proved_absent {
            absent(format!("{result:?}"));
            return refused(drained, Failed_Data::HerdrRefused);
        }
        // Typed once, never typed again: every message the payload carried
        // leaves the queue as an Uncertain attempt of the same native
        // anchor, and the one receipt of the payload settles them all.
        let _ = self.store.mark_delivery_uncertain(&attempt.attempt_id);
        let typed = self.park_cohort(&attempt, &cohort);
        if let Reconciled::Settled(..) = self.reconcile_delivery(&attempt)
            && node.harness_kind == HarnessKind::Codex
        {
            self.codex_turns.delivered(&node.session_id.0);
        }
        drained.extend(typed);
        Outcome::Drained(drained)
    }

    fn drain_all_held(&self, address: Address, lease: &WakingLease<'_>) -> Outcome {
        let queued = match self.store.metaflow(&address) {
            Ok(Some(metaflow)) => metaflow.queue,
            Ok(None) => return Outcome::Failed(Failed_Data::NotAwake),
            Err(_) => return Outcome::Failed(Failed_Data::StoreRefused),
        };
        let mut removed = 0;
        let mut delivered = Vec::new();
        loop {
            let outcome = self.drain_held(address.clone(), lease);
            match outcome {
                Outcome::Drained(mut entries) => delivered.append(&mut entries),
                Outcome::AwaitingReceipt(id) => return Outcome::AwaitingReceipt(id),
                Outcome::WaitingForTurn(id) => return Outcome::WaitingForTurn(id),
                Outcome::Failed(reason) => return Outcome::Failed(reason),
                _ => {}
            }
            let remaining = match self.store.metaflow(&address) {
                Ok(Some(metaflow)) => metaflow.queue.0.len(),
                _ => return Outcome::Failed(Failed_Data::StoreRefused),
            };
            let now_removed = queued.0.len().saturating_sub(remaining);
            if now_removed == removed || remaining == 0 {
                return Outcome::Drained(delivered);
            }
            removed = now_removed;
        }
    }
}

/// The messages one payload carried leave the queue together, and are
/// settled together by its one receipt.
pub(crate) trait ParksCohort {
    /// After `head`'s payload was typed: parks `head`, then gives each
    /// further message of `cohort` its own attempt at the same native
    /// anchor and parks it, so none is typed again. Returns those parked,
    /// oldest first.
    fn park_cohort(&self, head: &QueueDeliveryAttempt, cohort: &[QueueEntry]) -> Vec<QueueEntry>;
    /// Every run of the unsettled attempts sharing `attempt`'s native
    /// anchor that holds `attempt`, longest first, each with the payload
    /// text it would have been typed as.
    fn cohort_runs(
        &self,
        attempt: &QueueDeliveryAttempt,
    ) -> Vec<(Vec<QueueDeliveryAttempt>, String)>;
    /// The attempts one payload carried with `attempt`: the unsettled
    /// typed attempts at its native anchor whose attempt numbers run
    /// consecutively through its own, oldest first.
    fn typed_group(&self, attempt: &QueueDeliveryAttempt) -> Vec<QueueDeliveryAttempt>;
    /// Under the gate the caller holds: settles the latest payload typed
    /// into the seat `address` is Awake as, when native history shows its
    /// receipt. One payload, so one or two native reads.
    fn reconcile_typed(&self, address: &Address);
    /// At start, with no gate held: reads native history for every
    /// payload still to settle, then takes the gate only to settle each
    /// one whose receipt it found. A read that cannot decide, or that
    /// waits on its harness, holds no other request.
    fn recover_typed(&self, addresses: &[Address]);
    /// The typed attempts of `flow_id` at `address` still to settle, past
    /// the one still holding the queue's head, by attempt number.
    fn typed(&self, address: &Address, flow_id: &str) -> Vec<QueueDeliveryAttempt>;
    /// The run of typed attempts native history shows one exact receipt
    /// for, with that receipt; else why it cannot be decided.
    fn find_receipt(&self, attempt: &QueueDeliveryAttempt) -> Result<Receipted, Undecided>;
}

/// A run of attempts one payload carried, and the receipt that took it.
pub(crate) type Receipted = (Vec<QueueDeliveryAttempt>, String, String, SeatAfter);

impl ParksCohort for RunningNexus {
    fn park_cohort(&self, head: &QueueDeliveryAttempt, cohort: &[QueueEntry]) -> Vec<QueueEntry> {
        let mut parked = Vec::new();
        let Some((first, rest)) = cohort.split_first() else {
            return parked;
        };
        if !self
            .store
            .park_uncertain_delivery(&head.attempt_id)
            .unwrap_or(false)
        {
            eprintln!("flow-nexus: a typed attempt was not parked; it stays outstanding");
            return parked;
        }
        parked.push(first.clone());
        for entry in rest {
            match self.store.prepare_oldest_delivery(
                &head.address,
                &head.flow_id,
                &head.thread_id,
                head.harness_kind.clone(),
                head.history_anchor.clone(),
            ) {
                Ok(PrepareQueueDelivery::Prepared(next))
                    if next.queue_evidence
                        == meta_signal_flow::QueueEvidence::Logical(entry.clone()) =>
                {
                    let _ = self.store.mark_delivery_uncertain(&next.attempt_id);
                    if !self
                        .store
                        .park_uncertain_delivery(&next.attempt_id)
                        .unwrap_or(false)
                    {
                        eprintln!(
                            "flow-nexus: a typed attempt was not parked; it stays outstanding"
                        );
                        break;
                    }
                    parked.push(entry.clone());
                }
                other => {
                    eprintln!("flow-nexus: a typed request was not given its attempt: {other:?}");
                    break;
                }
            }
        }
        parked
    }

    fn find_receipt(&self, attempt: &QueueDeliveryAttempt) -> Result<Receipted, Undecided> {
        // One payload may have carried this attempt with the others typed
        // with it: each run of them is looked for, the longest first.
        let mut found = Undecided::AbsenceUnprovable;
        for (run, text) in self.cohort_runs(attempt) {
            let answer = match attempt.harness_kind {
                HarnessKind::Claude => match self.claude_delivery_history(attempt, &text) {
                    // A Claude seat takes input while its turn runs, into
                    // its own queue: the next request need not wait.
                    ClaudeQueuedHistory::Received(receipt) => {
                        Ok((receipt.turn_id, receipt.row, SeatAfter::Resting))
                    }
                    // Text typed into a Claude pane may sit in its composer
                    // or its input queue unrecorded: absence after typing is
                    // never proven from the transcript.
                    ClaudeQueuedHistory::Unseen => Err(Undecided::AbsenceUnprovable),
                    ClaudeQueuedHistory::Pending => Err(Undecided::PendingInHarness),
                    ClaudeQueuedHistory::Ambiguous => Err(Undecided::Ambiguous),
                    ClaudeQueuedHistory::Unreadable => Err(Undecided::HistoryUnreadable),
                    ClaudeQueuedHistory::Replaced => Err(Undecided::HistoryReplaced),
                },
                HarnessKind::Codex => {
                    let NativeHistoryAnchor::CodexTurns(anchor) = &attempt.history_anchor else {
                        return Err(Undecided::HistoryUnreadable);
                    };
                    let history = self
                        .store
                        .role(&attempt.flow_id)
                        .ok()
                        .flatten()
                        .and_then(|role| {
                            self.codex_endpoints
                                .current()
                                .adapter_for(&role.model_name.0)
                                .ok()
                        })
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
                        Some(CodexTurnHistory::Unseen { resting: true, .. }) => {
                            // An absent row at this instant is not proof that
                            // an unacknowledged request cannot materialize later.
                            Err(Undecided::AbsenceUnprovable)
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
            match answer {
                Ok((turn_id, native_receipt, seat_after)) => {
                    return Ok((run, turn_id, native_receipt, seat_after));
                }
                // What a run in the harness's own queue shows outweighs a
                // run nothing names.
                Err(undecided) => {
                    if !matches!(found, Undecided::PendingInHarness | Undecided::Ambiguous) {
                        found = undecided;
                    }
                }
            }
        }
        Err(found)
    }

    fn typed_group(&self, attempt: &QueueDeliveryAttempt) -> Vec<QueueDeliveryAttempt> {
        let Ok(unsettled) = self.store.unsettled_deliveries(&attempt.address) else {
            return vec![attempt.clone()];
        };
        let mut siblings = unsettled
            .into_iter()
            .filter(|other| {
                other.flow_id == attempt.flow_id
                    && other.thread_id == attempt.thread_id
                    && other.history_anchor == attempt.history_anchor
                    && matches!(
                        other.queue_evidence,
                        meta_signal_flow::QueueEvidence::Logical(_)
                    )
                    && matches!(
                        other.phase,
                        QueueDeliveryPhase::Prepared
                            | QueueDeliveryPhase::Uncertain(_)
                            | QueueDeliveryPhase::ReceiptRecorded(_)
                    )
            })
            .collect::<Vec<_>>();
        siblings.sort_by_key(|other| other.native_request_id);
        let Some(at) = siblings
            .iter()
            .position(|other| other.attempt_id == attempt.attempt_id)
        else {
            return vec![attempt.clone()];
        };
        let mut start = at;
        while start > 0
            && siblings[start - 1].native_request_id + 1 == siblings[start].native_request_id
        {
            start -= 1;
        }
        let mut end = at + 1;
        while end < siblings.len()
            && siblings[end - 1].native_request_id + 1 == siblings[end].native_request_id
        {
            end += 1;
        }
        siblings[start..end].to_vec()
    }

    fn typed(&self, address: &Address, flow_id: &str) -> Vec<QueueDeliveryAttempt> {
        let Ok(unsettled) = self.store.unsettled_deliveries(address) else {
            return Vec::new();
        };
        let outstanding = self
            .store
            .outstanding_delivery(address)
            .ok()
            .flatten()
            .map(|attempt| attempt.attempt_id);
        let mut typed = unsettled
            .into_iter()
            .filter(|attempt| {
                attempt.flow_id == flow_id
                    && Some(&attempt.attempt_id) != outstanding.as_ref()
                    && matches!(
                        attempt.phase,
                        QueueDeliveryPhase::Prepared | QueueDeliveryPhase::Uncertain(_)
                    )
            })
            .collect::<Vec<_>>();
        typed.sort_by_key(|attempt| attempt.native_request_id);
        typed
    }

    fn reconcile_typed(&self, address: &Address) {
        let Ok(Some(Metaflow {
            state: flow_ethos::State::Awake(flow_id),
            ..
        })) = self.store.metaflow(address)
        else {
            return;
        };
        let latest = self.typed(address, &flow_id.0).into_iter().last();
        if let Some(attempt) = latest
            && let Ok((run, turn_id, native_receipt, seat_after)) = self.find_receipt(&attempt)
        {
            let _ = self.settle_received(&attempt, &run, turn_id, native_receipt, seat_after);
        }
    }

    fn recover_typed(&self, addresses: &[Address]) {
        for address in addresses {
            let flow_id = match self.store.metaflow(address) {
                Ok(Some(Metaflow {
                    state: flow_ethos::State::Awake(flow_id),
                    ..
                })) => flow_id,
                _ => continue,
            };
            let mut covered = HashSet::new();
            for attempt in self.typed(address, &flow_id.0) {
                if !covered.insert(attempt.native_request_id) {
                    continue;
                }
                covered.extend(
                    self.typed_group(&attempt)
                        .iter()
                        .map(|member| member.native_request_id),
                );
                // Read off the gate; settle under it.
                let Ok((run, turn_id, native_receipt, seat_after)) = self.find_receipt(&attempt)
                else {
                    continue;
                };
                let _turn = self
                    .dispatch_gate
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                let _ = self.settle_received(&attempt, &run, turn_id, native_receipt, seat_after);
            }
        }
    }

    fn cohort_runs(
        &self,
        attempt: &QueueDeliveryAttempt,
    ) -> Vec<(Vec<QueueDeliveryAttempt>, String)> {
        let alone = (vec![attempt.clone()], attempt.prompt_text());
        let group = self.typed_group(attempt);
        if group.len() < 2 {
            return vec![alone];
        }
        let entries = group
            .iter()
            .filter_map(|member| match &member.queue_evidence {
                meta_signal_flow::QueueEvidence::Logical(entry) => Some(entry.clone()),
                meta_signal_flow::QueueEvidence::Legacy(_) => None,
            })
            .collect::<Vec<_>>();
        let text = entries.prompt_text(&attempt.address);
        vec![(group, text), alone]
    }
}

impl WakesInParts for RunningNexus {
    fn settle_startup(
        &self,
        address: &Address,
        flow_id: &str,
        launch_request_id: &str,
        startup: &[QueueEntry],
        simplified: bool,
    ) -> bool {
        let harness_kind = self
            .store
            .flow_node(flow_id)
            .ok()
            .flatten()
            .map(|node| node.harness_kind);
        let harness_kind_for_attempt = harness_kind.clone();
        let receipt = self
            .store
            .launch_attempt(launch_request_id)
            .ok()
            .flatten()
            .and_then(|attempt| {
                let signal_flow::PromptDeliveryResult::Observed(observed) =
                    attempt.prompt_delivery_result_option?
                else {
                    return None;
                };
                Some(signal_flow::NativeReceipt {
                    harness_kind: harness_kind_for_attempt.clone()?,
                    native_session_id: observed.native_session_id,
                    native_prompt_id: signal_flow::NativePromptId(observed.native_turn_id.0),
                })
            })
            .or_else(|| {
                use crate::generated::starter::StartupProgress;
                use crate::herdr::launch::ObservesNativeTargetReceipt;
                let intent = self
                    .store
                    .launch_attempt(launch_request_id)
                    .ok()
                    .flatten()?
                    .prompt_delivery_intent_option?;
                let StartupProgress::Received(observed) =
                    self.herdr.observe_native_target_receipt(&intent).ok()?
                else {
                    return None;
                };
                Some(signal_flow::NativeReceipt {
                    harness_kind: harness_kind_for_attempt.clone()?,
                    native_session_id: observed.native_session_id,
                    native_prompt_id: signal_flow::NativePromptId(observed.native_turn_id.0),
                })
            })
            .or_else(|| {
                self.store
                    .events(flow_id)
                    .ok()
                    .flatten()?
                    .into_iter()
                    .rev()
                    .find_map(|event| match event {
                        Event::PromptSubmitted(prompt) => Some(signal_flow::NativeReceipt {
                            harness_kind: harness_kind.clone()?,
                            native_session_id: prompt.native_session_id_option?,
                            native_prompt_id: signal_flow::NativePromptId(
                                prompt.prompt_id_option?.0,
                            ),
                        }),
                        _ => None,
                    })
            });
        let has_logical = startup.iter().any(|entry| {
            self.store
                .logical_delivery_snapshot(&entry.logical_delivery_id)
                .ok()
                .flatten()
                .is_some()
        });
        // A Started response proves the run, while the transcript receipt
        // proves the queued payload. If that second witness is unavailable,
        // preserve the exact queue prefix rather than guessing it delivered.
        if has_logical && receipt.is_none() {
            return false;
        }
        if simplified
            && let Some(oldest) = startup.first()
            && let signal_flow::SentBy::Sender(recipient) = &oldest.sent_by
        {
            let binding = SimplifiedReplyBinding {
                flow_id: flow_id.to_owned(),
                input_delivery_id: oldest.logical_delivery_id.clone().0,
                source: address.clone(),
                recipient: recipient.clone(),
                completed: false,
            };
            if self.store.bind_simplified_reply(binding).is_err() {
                return false;
            }
        }
        self.store
            .settle_wake_startup_receipts(address, flow_id, startup, receipt.as_ref())
            .unwrap_or(false)
    }

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
        // A metaflow owed a successor wakes with nothing queued.
        let owed = matches!(self.store.successor_intent(address), Ok(Some(_)));
        if metaflow.queue.0.is_empty() && !owed {
            return Err(WakeRefusal::NothingQueued);
        }
        let plan = self.wake_plan(&metaflow);
        let simplified = matches!(
            &plan,
            WakePlan::Fresh(fresh) if fresh.flow_kind == signal_flow::FlowKind::Simplified
        );
        // The wake lease fixes this oldest-first batch. Work admitted while
        // the native launch settles is appended behind it and cannot enter a
        // prompt already composed. A Simplified run can owe one answer to
        // one sender, so its whole selected batch is its oldest request;
        // the rest remains ordered for the next turn.
        let startup = if simplified {
            flow_ethos::Queue(metaflow.queue.0.first().cloned().into_iter().collect())
        } else {
            metaflow.queue.clone()
        };
        let startup_text = startup.0.prompt_text(&metaflow.address);
        // The last flow's session must not be running still: a seat that
        // flow's harness keeps, as one started by hand would, is never
        // joined by a second flow of the same metaflow.
        if let Some(node) = metaflow
            .past
            .0
            .last()
            .and_then(|flow_id| self.store.flow_node(&flow_id.0).ok().flatten())
            .filter(|node| !node.session_id.0.is_empty())
        {
            let mut sessions = Vec::new();
            if let WakePlan::Fresh(fresh) = &plan {
                sessions.push(fresh.herdr_session_name.0.clone());
            }
            if let HerdrRouteSelection::Available(route) = &node.herdr_route_selection {
                sessions.push(route.herdr_session_name.0.clone());
            }
            self.refuse_live_session(&node, &sessions)?;
        }
        let WakePlan::Fresh(fresh) = plan else {
            return Err(WakeRefusal::Unplaceable);
        };
        let started = match self.start_fresh(&metaflow, fresh, &startup_text) {
            Ok(started) => started,
            Err(FreshRefusal::Failed(failure)) => {
                return Err(WakeRefusal::FreshNotStarted(failure));
            }
            Err(FreshRefusal::Rejected(rejection)) => {
                // Nothing was taken: every request stays queued, and its
                // sender sees why it waits. The next waking request retries.
                use crate::store::logical_delivery::RecordsLogicalDeliveries;
                let _turn = self
                    .dispatch_gate
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                let queue = match self.store.metaflow(address) {
                    Ok(Some(current)) => current.queue,
                    _ => metaflow.queue.clone(),
                };
                if let Err(error) = self.store.mark_queue_retryable(
                    &queue.0,
                    signal_flow::RetryCause::WakeRefused(rejection.clone()),
                ) {
                    eprintln!(
                        "flow-nexus: the queue of {:?} {} {:?} is not marked WakeRefused: {error}",
                        address.aspect, address.topic.0, address.model
                    );
                }
                return Err(WakeRefusal::StartRejected(rejection));
            }
        };
        let flow_id = started.flow_id;
        if let Some(predecessor) = metaflow.past.0.last() {
            use crate::store::handover::HoldsHandovers;
            if self
                .store
                .close_handover_for_successor(address, &predecessor.0, &flow_id.0)
                .is_err()
            {
                return Err(WakeRefusal::StoreUnreadable);
            }
        }
        let _turn = self
            .dispatch_gate
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        match self.perform(Operation::Record(Record_Data::Awoken(
            Record_Data_Awoken_Data {
                address: address.clone(),
                flow_id: flow_id.clone(),
                // `Started` is the launch boundary that authoritatively
                // accepted this first prompt. Remove exactly its fixed batch;
                // anything admitted while it started remains queued behind.
                // If a response is lost, recovery counts it delivered only
                // when the seat is up and its transcript shows this batch;
                // otherwise the queue and its uncertainty stay preserved.
                delivered_count: crate::generated::operation::DeliveredCount(0),
            },
        ))) {
            Outcome::Recorded
                if self.settle_startup(
                    address,
                    &flow_id.0,
                    &started.launch_request_id,
                    &startup.0,
                    simplified,
                ) =>
            {
                Ok(flow_id)
            }
            _ => {
                eprintln!(
                    "flow-nexus: flow {flow_id} woke but is not recorded Awake",
                    flow_id = flow_id.0
                );
                Err(WakeRefusal::AwakeNotRecorded)
            }
        }
    }

    fn start_fresh(
        &self,
        metaflow: &Metaflow,
        fresh: FreshContext,
        text: &str,
    ) -> Result<FreshStarted, FreshRefusal> {
        let address = &metaflow.address;
        // The seat's model is the one configured for its Model.
        let model = match self.store.configured_model(address) {
            Ok(Some(model)) => model,
            Ok(None) => {
                return Err(FreshRefusal::Rejected(
                    signal_flow::StartRejection::CompositionRefused(
                        signal_flow::CompositionRefusal::Unconfigured,
                    ),
                ));
            }
            Err(_) => return Err(FreshRefusal::Failed(Failed_Data::StoreRefused)),
        };
        let launch_request_id = format!(
            "wake-{}-{}",
            metaflow.past.0.len(),
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
            herdr_session_name: fresh.herdr_session_name.0,
            system_modules: fresh.system_modules,
            user_modules: fresh.user_modules,
            leave_outs_option: fresh.leave_outs_option.map(|value| value.0),
            flow_kind: fresh.flow_kind,
            tool_set: fresh.tool_set,
            prompt: text.with_purpose(&fresh.purpose.0),
        };
        // The metaflow's last flow is the predecessor this wake succeeds, and
        // its handover is given to the new flow. The metaflow is Asleep, so
        // the predecessor takes no part in taking its Address.
        let mut profile = seat.fresh_profile();
        profile.flow_id_option = metaflow.past.0.last().cloned();
        self.start_unbound(profile).map(|flow_id| FreshStarted {
            flow_id,
            launch_request_id,
        })
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
                flow_id: signal_flow::FlowId(flow_id.to_owned()),
                tokens: crate::generated::operation::Tokens(tokens),
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
                        node.flow_id.0
                    );
                    return Err(WakeRefusal::SessionLive);
                }
                None => return Err(WakeRefusal::HerdrUnreadable),
            }
        }
        Ok(())
    }

    fn close_at_rest(&self, flow_id: &str, node: &FlowNode) -> Result<(), Box<Outcome>> {
        let HerdrRouteSelection::Available(route) = &node.herdr_route_selection else {
            return Err(Box::new(Outcome::Failed(Failed_Data::HerdrRefused)));
        };
        let closed = {
            let _lease = self.pane_leases.hold(route);
            self.perform(Operation::Close(node.clone()))
        };
        if closed != Outcome::Closed {
            return Err(Box::new(closed));
        }
        let recorded = self.perform(Operation::Record(Record_Data::Stopped(
            signal_flow::FlowId(flow_id.to_owned()),
        )));
        if recorded != Outcome::Recorded {
            return Err(Box::new(recorded));
        }
        Ok(())
    }

    fn refresh_successor_first(
        &self,
        flow_id: &str,
        metaflow: &Metaflow,
        cause: SuccessionCause,
    ) -> AtStop {
        let address = metaflow.address.clone();
        let Some(mut profile) = self.successor_profile(
            flow_id,
            metaflow,
            crate::sending::MintsId::minted_id("refresh"),
        ) else {
            return AtStop::Kept(KeptAtStop::RefreshUnplaceable);
        };
        // The intent names the replacement's launch request, so a Nexus
        // that takes it up again resumes that launch; a launch already
        // refused is not resumed, and a new one is owed in its place.
        let launch_request_id = match self.store.successor_intent(&address) {
            Ok(Some(intent))
                if intent.predecessor == flow_id
                    && matches!(
                        intent.cause,
                        SuccessionCause::Refresh | SuccessionCause::Ordered
                    )
                    && !intent.launch_request_id.is_empty()
                    && !self.launch_refused(&intent.launch_request_id) =>
            {
                // The successor is owed now for this refresh's cause.
                if intent.cause != cause
                    && self
                        .store
                        .owe_successor(SuccessorIntent {
                            cause,
                            ..intent.clone()
                        })
                        .is_err()
                {
                    return AtStop::Kept(KeptAtStop::StoreUnreadable);
                }
                intent.launch_request_id
            }
            Ok(_) => {
                let intent = SuccessorIntent {
                    address: address.clone(),
                    predecessor: flow_id.to_owned(),
                    cause,
                    launch_request_id: crate::sending::MintsId::minted_id("refresh"),
                };
                if self.store.owe_successor(intent.clone()).is_err() {
                    return AtStop::Kept(KeptAtStop::StoreUnreadable);
                }
                intent.launch_request_id
            }
            Err(_) => return AtStop::Kept(KeptAtStop::StoreUnreadable),
        };
        profile.launch_request_id = signal_flow::LaunchRequestId(launch_request_id);
        let response = {
            let _turn = self
                .dispatch_gate
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            self.replace(profile, Origin::Unbound)
        };
        self.settle_owed(&address, &response);
        match response {
            Response::Replaced(_) => {
                // The launching reap closes the handover before recording
                // Replaced; this response is already past that durable point.
                AtStop::Refreshed(address)
            }
            response => AtStop::Kept(KeptAtStop::RefreshLaunchRefused(Box::new(response))),
        }
    }

    fn successor_profile(
        &self,
        flow_id: &str,
        metaflow: &Metaflow,
        launch_request_id: String,
    ) -> Option<LaunchProfile> {
        let address = metaflow.address.clone();
        // The successor's context is the metaflow's declaration, else what
        // the predecessor's own launch named.
        let mut lineage = metaflow.clone();
        lineage.past.0.retain(|past| past.0 != flow_id);
        lineage.past.0.push(flow_ethos::FlowId(flow_id.to_owned()));
        let WakePlan::Fresh(fresh) = self.wake_plan(&lineage) else {
            return None;
        };
        let model = self.store.configured_model(&address).ok().flatten()?;
        let seat = FreshSeat {
            address: address.clone(),
            launch_request_id,
            model_name: model,
            effort: <Self as crate::sending::ChoosesEffort>::effort_of(&address).into(),
            herdr_session_name: fresh.herdr_session_name.0,
            system_modules: fresh.system_modules,
            user_modules: fresh.user_modules,
            leave_outs_option: fresh.leave_outs_option.map(|value| value.0),
            flow_kind: fresh.flow_kind,
            tool_set: fresh.tool_set,
            prompt: "".with_purpose(&fresh.purpose.0),
        };
        let mut profile = seat.fresh_profile();
        profile.flow_id_option = Some(signal_flow::FlowId(flow_id.to_owned()));
        Some(profile)
    }

    fn successor_composes(&self, flow_id: &str, metaflow: &Metaflow) -> Result<(), KeptAtStop> {
        let Some(profile) = self.successor_profile(
            flow_id,
            metaflow,
            crate::sending::MintsId::minted_id("compose"),
        ) else {
            return Err(KeptAtStop::RefreshUnplaceable);
        };
        match self.perform(Operation::Compose(profile)) {
            Outcome::Composed(_) => Ok(()),
            refused => Err(KeptAtStop::SuccessorUncomposable(Box::new(refused))),
        }
    }

    fn successor_refused(&self, flow_id: &str) {
        let address = match self.store.metaflow_awake_as(flow_id) {
            Ok(Some(metaflow)) => metaflow.address,
            _ => return,
        };
        self.refresh_refusals.note(flow_id);
        // The seat rests now: what is queued is handed to it here, as its
        // turn's end would, under the gate.
        let _turn = self
            .dispatch_gate
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        self.reconcile_typed(&address);
        if matches!(
            self.store.metaflow(&address),
            Ok(Some(metaflow)) if !metaflow.queue.0.is_empty()
        ) {
            let _ = self.perform(Operation::Drain(address));
        }
    }

    fn settle_owed(&self, address: &Address, response: &Response) {
        match response {
            Response::LaunchPending(_)
            | Response::StartAmbiguous(_)
            | Response::ReplaceRejected(signal_flow::ReplaceRejection::ReapRefused(_)) => {}
            _ => {
                let _ = self.store.forget_successor(address);
            }
        }
    }

    fn launch_refused(&self, launch_request_id: &str) -> bool {
        use crate::store::{LaunchOutcome, RecordsLaunchOutcome};
        matches!(
            self.store.launch_outcome(launch_request_id),
            Ok(Some(
                LaunchOutcome::StartRejected(_) | LaunchOutcome::ReplaceRejected(_)
            ))
        )
    }

    fn take_up_owed(&self, intent: SuccessorIntent) -> AtStop {
        let address = intent.address.clone();
        let Some(lock) = self.waking.hold(&address) else {
            return AtStop::Kept(KeptAtStop::Waking);
        };
        // The owed flow is still current: the intent was read under the
        // gate, and the lock keeps every wake and refresh out since.
        let metaflow = match self.store.metaflow(&address) {
            Ok(Some(metaflow))
                if metaflow.state
                    == flow_ethos::State::Awake(flow_ethos::FlowId(intent.predecessor.clone())) =>
            {
                metaflow
            }
            Ok(_) => return AtStop::Kept(KeptAtStop::NotAwake),
            Err(_) => return AtStop::Kept(KeptAtStop::StoreUnreadable),
        };
        let taken = match intent.cause {
            SuccessionCause::Replace => {
                let attempt = match self.store.launch_attempt(&intent.launch_request_id) {
                    Ok(Some(attempt)) => attempt,
                    // A Replace whose launch was never recorded started
                    // nothing: its predecessor was never touched.
                    Ok(None) => {
                        let _ = self.store.forget_successor(&address);
                        return AtStop::Kept(KeptAtStop::Unconfirmed);
                    }
                    Err(_) => return AtStop::Kept(KeptAtStop::StoreUnreadable),
                };
                let mut profile = attempt.launch_profile;
                if self.launch_refused(&intent.launch_request_id) {
                    profile.launch_request_id =
                        signal_flow::LaunchRequestId(crate::sending::MintsId::minted_id("replace"));
                    if self
                        .store
                        .owe_successor(SuccessorIntent {
                            launch_request_id: profile.launch_request_id.clone().0,
                            ..intent
                        })
                        .is_err()
                    {
                        return AtStop::Kept(KeptAtStop::StoreUnreadable);
                    }
                }
                let response = {
                    let _turn = self
                        .dispatch_gate
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner());
                    self.replace(profile, attempt.origin)
                };
                self.settle_owed(&address, &response);
                match response {
                    Response::Replaced(_) => AtStop::Refreshed(address),
                    response => AtStop::Kept(KeptAtStop::RefreshLaunchRefused(Box::new(response))),
                }
            }
            cause => self.refresh_successor_first(&intent.predecessor, &metaflow, cause),
        };
        drop(lock);
        self.wake_deferred();
        taken
    }

    fn settle_rested(&self, flow_id: &str) -> AtStop {
        self.record_context_at_stop(flow_id);
        // Whether the flow was sent its handover warning before this idle
        // event: one warned only by this event's own reminder pass has yet
        // to read it, and is refreshed at its next idle event.
        let warned = {
            use crate::store::handover_reminders::SchedulesHandoverReminders;
            match self.store.metaflow_awake_as(flow_id) {
                Ok(Some(metaflow)) => self
                    .store
                    .warned(&metaflow.address, flow_id)
                    .unwrap_or(false),
                _ => false,
            }
        };
        // The stop measurement is the warning-window opening witness. Run the
        // ordinary reminder admission now; the worker remains responsible for
        // anchored repeats and recovery after a restart.
        use crate::handover_reminding::RemindsHandoverCallers;
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|duration| duration.as_secs() as i64)
            .unwrap_or_default();
        self.send_due_handover_reminders(now);
        // A Codex seat's rest is its app-server's, and a Claude seat whose
        // transcript records its main turn's end_turn has ended its turn
        // whatever its screen shows (background agents keep Herdr at
        // Working). Otherwise Claude runs its Stop hook while its spinner is
        // still on screen, and Herdr reads its state from the screen: the
        // seat is given a bounded wait to show resting before the gate is
        // taken. A seat due a refresh is given the same wait: a refresh
        // closes only a seat that rests.
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
        let turn = self
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
        // A seat is refreshed at this idle event once it is past its
        // threshold, or once it was sent its handover warning, or wrote a
        // handover qualified for the warning window, whether or not it rests
        // and whether or not its handover is written: an absent handover
        // starts its successor from its definition and its queue. A
        // protected Address is never refreshed for its context. The
        // decision is taken under the dispatch gate, as a Soft Send's
        // hand-over at this idle event is: whichever comes first shuts out
        // the other.
        let refresh_due = match self.store.threshold_standing(flow_id) {
            Ok(crate::store::context::ThresholdStanding::Past { .. }) => true,
            Ok(crate::store::context::ThresholdStanding::Below { refresh, .. }) => {
                use crate::protecting::ProtectsFlows;
                let warned = warned
                    || match self.store.handover_green(&address, flow_id, refresh) {
                        Ok(green) => green,
                        Err(_) => return AtStop::Kept(KeptAtStop::StoreUnreadable),
                    };
                warned
                    && match self.store.protected_address(flow_id) {
                        Ok(protected) => protected.is_none(),
                        Err(_) => return AtStop::Kept(KeptAtStop::StoreUnreadable),
                    }
            }
            Ok(_) => false,
            Err(_) => return AtStop::Kept(KeptAtStop::StoreUnreadable),
        };
        let node = match self.store.flow_node(flow_id) {
            Ok(Some(node)) => self.herdr.refresh_route(node),
            Ok(None) => return AtStop::Kept(KeptAtStop::UnknownFlow),
            Err(_) => return AtStop::Kept(KeptAtStop::StoreUnreadable),
        };
        let continuity = match self.store.continuity(&address) {
            Ok(continuity) => continuity,
            Err(_) => return AtStop::Kept(KeptAtStop::StoreUnreadable),
        };
        // Whether the harness may be closed now: a Claude seat only once its
        // Stop names no background work, since closing it would end that
        // work.
        let closable = match node.harness_kind {
            // A seat due a refresh is closed at this idle event whatever it
            // runs.
            _ if refresh_due => true,
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
                true
            }
            HarnessKind::Claude => {
                let pane_agent = self.herdr.pane_agent(&node);
                let native_resting = matches!(
                    &pane_agent,
                    PaneAgent::Present { agent_state, .. } if Self::rests_at(agent_state)
                );
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
                // Its turn has ended. A Stop naming work in flight is no
                // rest: closing the seat would end that work.
                self.background_settled(flow_id)
            }
        };
        // A replacement ordered from outside is carried out once the seat
        // rests; until then the seat is handed what it is sent.
        let ordered = match self.store.ordered_replacement(&address) {
            Ok(order) => order.filter(|order| order.predecessor == flow_id),
            Err(_) => return AtStop::Kept(KeptAtStop::StoreUnreadable),
        };
        if closable && let Some(order) = ordered {
            return self.replace_as_ordered(order, metaflow, node, turn);
        }
        if refresh_due {
            let wakes = metaflow
                .queue
                .0
                .iter()
                .any(|entry| !matches!(entry.message, signal_flow::Message::Soft(_)));
            // A Maintained metaflow, or one holding work that wakes it, is
            // owed its successor, started before the predecessor rests.
            if continuity == Continuity::Maintained || wakes {
                let Some(refreshing) = self.waking.hold(&address) else {
                    return AtStop::Kept(KeptAtStop::Waking);
                };
                // Replace takes the gate for its own steps; the waking lease
                // keeps any wake or drain of the metaflow out meanwhile.
                drop(turn);
                let refreshed =
                    self.refresh_successor_first(flow_id, &metaflow, SuccessionCause::Refresh);
                drop(refreshing);
                self.wake_deferred();
                return refreshed;
            }
            if let HerdrRouteSelection::Unavailable = &node.herdr_route_selection {
                return AtStop::Kept(KeptAtStop::RefreshUnrouted);
            }
            // Its successor is composed at its next wake: the seat is
            // closed only once that successor composes now.
            if let Err(kept) = self.successor_composes(flow_id, &metaflow) {
                return AtStop::Kept(kept);
            }
            return match self.close_at_rest(flow_id, &node) {
                Ok(()) => AtStop::Refreshed(address),
                Err(outcome) => AtStop::Kept(KeptAtStop::RefreshRefused(outcome)),
            };
        }
        self.reconcile_typed(&address);
        // A simplified seat's answer is queued before the seat is handed
        // more or sleeps.
        self.queue_simplified_answer(flow_id);
        if !metaflow.queue.0.is_empty() {
            return match self.perform(Operation::Drain(address)) {
                Outcome::Drained(mut typed) if !typed.is_empty() => {
                    AtStop::Drained(typed.remove(0))
                }
                outcome => AtStop::Kept(KeptAtStop::DrainRefused(Box::new(outcome))),
            };
        }
        // With nothing queued, how the metaflow continues decides: a
        // Maintained seat stays Awake in its pane; an OnDemand seat closes
        // its harness, its Address and transcript kept, once it can be
        // closed without ending work its harness still runs.
        match continuity {
            Continuity::Maintained => AtStop::Idle,
            Continuity::OnDemand if !closable => AtStop::Idle,
            // Its successor is composed at its next wake: the seat is
            // closed only once that successor composes now.
            Continuity::OnDemand => match self.successor_composes(flow_id, &metaflow) {
                Err(kept) => AtStop::Kept(kept),
                Ok(()) => match self.close_at_rest(flow_id, &node) {
                    Ok(()) => AtStop::Slept(address),
                    Err(outcome) => AtStop::Kept(KeptAtStop::SleepRefused(outcome)),
                },
            },
        }
    }
}
