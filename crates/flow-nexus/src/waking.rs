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
//! the flow's transcript; a Maintained seat stays Awake in its pane. Work
//! queued later for an Awake seat known resting, by the same evidence, is
//! handed over at once.
//!
//! A seat past its model's refresh threshold is replaced at its rest. A
//! Maintained metaflow, or one holding work that wakes it, is owed its
//! successor: the intent is recorded, the successor is started and
//! confirmed through Replace, and only then is the predecessor recorded
//! Stopped and its pane closed, in the replacement's reaping. Any other is
//! closed and put Asleep, its queue kept for the next flow. A Maintained
//! metaflow whose flow ends with its pane gone is owed a successor in the
//! write that records the exit, and woken with no queued request needed.
//!
//! A refresh ordered from outside (`Refresh.Address`,
//! [`RefreshesMetaflows`]) takes the metaflow's hold and ends its flow
//! without a turn. A Maintained metaflow, or one holding work that wakes it,
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

use crate::claude::{
    ClaudeQueuedHistory, ClaudeTranscriptCursor, LocatesClaudeTranscript, MeasuresClaudeContext,
    ReadsClaudeTurns,
};
use crate::codex::{
    CodexTurnHistory, ReadsCodexTurnHistory, SelectsCodexEndpoint, SubmitsQueuedCodexTurn,
};
use crate::codex_turns::{SettlesRestingCodexSeats, TracksCodexTurns};
use crate::composition::{FreshSeat, JoinsPurpose};
use crate::delivery::body::{RendersNativeContent, RendersPaneText, ShowsPaneText};
use crate::delivery::lease::LeasesPanes;
use crate::delivery::{
    DeliversMessages, FindsDeliveryTarget, PaneRefusal, ReadsLeasedPane, TargetRefusal,
};
use crate::delivery::body::TypesPaneMessage;
use datom_codec::Datomizable;
use protos::{Compactable, Protosizable};
use crate::generated::operation::{
    Failed_Data, Operation, Outcome, Record_Data, Record_Data_Asleep_Data, Record_Data_Awoken_Data,
    Record_Data_Context_Data,
};
use crate::generated::starter::ProfilesFreshSeat;
use crate::herdr::pane::{PaneAgent, Placement, WritesPane};
use crate::herdr::{LocatesNativeTranscripts, PanePresence, ReadsHerdrPanes, ReadsHerdrRoster};
use crate::launching::LaunchesFlows;
use crate::performing::Performs;
use crate::send_tool::{ServesLaunchCalls, WakesDeferred};
use crate::store::address_hold::HoldsLaunchAddress;
use crate::store::context::JudgesThreshold;
use crate::store::events::RecordsHarnessEvents;
use crate::store::logical_delivery::RecordsLogicalDeliveries;
use crate::store::metaflow::SleepsMetaflows;
use crate::store::queue_delivery::{
    AbsenceRecord, AccountsQueueDelivery, NativeDeliveryReceipt, NativeHistoryAnchor,
    NativeTranscriptCursor, PrepareQueueDelivery, QueueDeliveryAttempt, QueueDeliveryPhase,
};
use crate::store::successor::{OwesSuccessors, ReadsContinuity, SuccessionCause, SuccessorIntent};
use crate::store::{FlowStore, NamesConfiguredModel};
use crate::store::{
    ManagesMetaflowState, NamesLiveFlow, ReadsFlowRoles, ReadsFlowRows, ReadsLaunchAttempt,
    RecordsFlowLifecycle,
};
use crate::{ConfiguredUse, RunningNexus};
use flow_native::DigestsPrompt;
use meta_signal_flow::{AbsenceProof, Continuity, Undecided};
use signal_flow::{
    Address, AgentState, Event, FlowId, FlowNode, HarnessKind, HerdrRouteSelection, LaunchProfile,
    Layer, Metaflow, NativePrompt, NativeStop, Origin, QueueEntry, Response,
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
    fn release(&self, address: &Address);
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

    fn release(&self, address: &Address) {
        self.waking
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(address);
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
    fn rest_ended_awake(&self, configured: &ConfiguredUse, address: &Address) -> bool;
    /// The same for every metaflow, at the Nexus's start.
    fn rest_every_ended_awake(&self, configured: &ConfiguredUse);
}

impl RestsEndedAwake for RunningNexus {
    fn rest_ended_awake(&self, configured: &ConfiguredUse, address: &Address) -> bool {
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
        match self.perform_configured(
            configured,
            Operation::Record(Record_Data::Asleep(Record_Data_Asleep_Data {
                address: address.clone(),
                flow_id: flow_id.clone(),
            })),
        ) {
            Outcome::Recorded => {
                eprintln!("flow-nexus: metaflow Awake as ended flow {flow_id} was recorded Asleep");
                true
            }
            _ => false,
        }
    }

    fn rest_every_ended_awake(&self, configured: &ConfiguredUse) {
        let Ok(metaflows) = self.store.list_metaflows() else {
            return;
        };
        for metaflow in metaflows {
            self.rest_ended_awake(configured, &metaflow.address);
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
    #[cfg(test)]
    fn note_stop_at_confirmation(&self, flow_id: &str);
    fn note_stop_at_confirmation_configured(
        &self,
        configured: &crate::ConfiguredUse,
        flow_id: &str,
    );
    /// Off the dispatch gate: settles every noted flow's Stop.
    #[cfg(test)]
    fn settle_stops_at_confirmation(&self);
    fn settle_stops_at_confirmation_configured(&self, configured: &ConfiguredUse);
}

impl SettlesStopsAtConfirmation for RunningNexus {
    #[cfg(test)]
    fn note_stop_at_confirmation(&self, flow_id: &str) {
        let Ok(configured) = self.test_configured_use() else {
            return;
        };
        self.note_stop_at_confirmation_configured(&configured, flow_id)
    }

    fn note_stop_at_confirmation_configured(
        &self,
        configured: &crate::ConfiguredUse,
        flow_id: &str,
    ) {
        // A Claude flow's own Stop, or the main end_turn its transcript
        // records, landed before confirmation stands as its turn end; a
        // Codex flow reports no Stop: its thread's rest, as its app-server's
        // events showed it, stands for one.
        let claude_ended = self.turn_end_recorded_configured(configured, flow_id)
            || matches!(
                self.store.flow_node(flow_id),
                Ok(Some(node)) if self.claude_main_turn_ended_configured(configured, &node)
            );
        if claude_ended || self.codex_resting(flow_id) {
            self.stopped_at_confirmation.note(flow_id);
        }
    }

    #[cfg(test)]
    fn settle_stops_at_confirmation(&self) {
        let Ok(configured) = self.test_configured_use() else {
            return;
        };
        self.settle_stops_at_confirmation_configured(&configured)
    }

    fn settle_stops_at_confirmation_configured(&self, configured: &ConfiguredUse) {
        for flow_id in self.stopped_at_confirmation.take() {
            self.settle_stopped_configured(configured, &flow_id);
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
    /// The seat was due a refresh and rested, its last measured context past
    /// its model's refresh threshold. A Maintained metaflow, or one holding
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
    #[cfg(test)]
    fn settle_stopped(&self, flow_id: &str) -> AtStop;
    fn settle_stopped_configured(&self, configured: &ConfiguredUse, flow_id: &str) -> AtStop;
    /// After a request was queued for `address`, or a queued one was
    /// answered again: holds the metaflow, and when Flow's records show it
    /// due a wake, wakes it and drains what arrived meanwhile. Nothing when
    /// a wake already runs or none is due, so asking twice wakes once.
    #[cfg(test)]
    fn wake_due(&self, address: Address);
    fn wake_due_configured(&self, configured: &ConfiguredUse, address: Address);
    /// After work was queued for `address`, or the owner's resolution of a
    /// hand-over committed: when the metaflow's Awake seat has queued work,
    /// records its flow Exited if Herdr shows its pane gone, which puts the
    /// metaflow Asleep for a fresh wake; otherwise, when the seat is known
    /// resting by the evidence the drain at a turn end uses (a Claude
    /// transcript whose main turn ended with no later prompt; a Codex thread
    /// the app-server reports resting), runs that same drain now. A seat not
    /// known resting waits for its turn end. Takes the dispatch gate, so
    /// the caller holds none.
    #[cfg(test)]
    fn drain_awake(&self, address: &Address);
    fn drain_awake_configured(&self, configured: &ConfiguredUse, address: &Address);
    /// Whether Flow's records show the metaflow due a wake: Asleep and
    /// holding work that wakes it.
    fn wake_is_due(&self, metaflow: &Metaflow) -> bool;
    /// Types what arrived during a wake into the woken flow, oldest first,
    /// until the queue is seen empty under the gate; the hold is released
    /// there.
    fn drain_woken(&self, configured: &ConfiguredUse, address: &Address, lease: WakingLease<'_>);
    /// Which way an Asleep metaflow is woken.
    fn wake_plan(&self, metaflow: &Metaflow) -> WakePlan;
    /// At the Nexus's start: wakes every metaflow due a wake, as a wake the
    /// last Nexus did not start or did not finish left it.
    #[cfg(test)]
    fn recover_wakes(&self);
    fn recover_wakes_configured(&self, configured: &ConfiguredUse);
}

/// The bodies of the Wake and Drain operations; `perform` delegates to
/// them.
pub trait PerformsWakeAndDrain {
    /// Performed off the dispatch gate; see the module documentation.
    #[cfg(test)]
    fn wake(&self, address: Address) -> Outcome;
    fn wake_configured(&self, configured: &crate::ConfiguredUse, address: Address) -> Outcome;
    /// Performed under the dispatch gate.
    #[cfg(test)]
    fn drain(&self, address: Address) -> Outcome;
    fn drain_configured(&self, configured: &crate::ConfiguredUse, address: Address) -> Outcome;
    /// Hands over as many queued requests as can be settled now, oldest
    /// first. It stops at the first hand-over whose native receipt is not
    /// yet established, or at any refusal; work behind that request stays
    /// durable. The caller holds the dispatch gate.
    #[cfg(test)]
    fn drain_all(&self, address: Address) -> Outcome;
    fn drain_all_configured(&self, configured: &ConfiguredUse, address: Address) -> Outcome;
}

/// A declaration's required metaflows start first. Flow starts each one
/// that is not Awake as a live flow, from its own declaration, before the
/// requested metaflow. A requirement with no declaration, or one that does
/// not start and confirm, rejects the requested Start naming that Address.
pub(crate) trait StartsRequirementsFirst {
    /// Runs off the dispatch gate, taking it per requirement as a wake does.

    fn requirements_first_configured(
        &self,
        configured: &crate::ConfiguredUse,
        profile: &LaunchProfile,
    ) -> Result<(), signal_flow::StartRejection>;
}

impl StartsRequirementsFirst for RunningNexus {
    fn requirements_first_configured(
        &self,
        configured: &crate::ConfiguredUse,
        profile: &LaunchProfile,
    ) -> Result<(), signal_flow::StartRejection> {
        use crate::launching::NamesTopic;
        use crate::store::ReadsLaunchAttempt;
        use crate::store::metaflow::TakesLaunchAddress;
        use crate::store::metaflow_declaration::ReadsMetaflowDeclarations;

        if profile.flow_id_option.is_some() || !profile.address.topic.as_str().is_camel_case_topic()
        {
            return Ok(());
        }
        let requirements = {
            let _turn = self
                .dispatch_gate
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let new_launch = matches!(
                self.store.launch_attempt(&profile.launch_request_id),
                Ok(None)
            );
            if !new_launch {
                return Ok(());
            }
            self.store
                .metaflow_declaration(&profile.address)
                .map_err(|_| signal_flow::StartRejection::LaunchPersistenceRefused)?
                .map(|declaration| declaration.requires)
                .unwrap_or_default()
        };

        for required in requirements {
            if required == profile.address {
                return Err(signal_flow::StartRejection::RequirementUnmet(required));
            }
            let Some(lease) = self.waking.hold(&required) else {
                let awake = {
                    let _turn = self
                        .dispatch_gate
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner());
                    matches!(
                        self.store.metaflow(&required),
                        Ok(Some(Metaflow {
                            state: flow_ethos::State::Awake(flow_id),
                            ..
                        })) if matches!(self.store.flow_is_live(&flow_id), Ok(true))
                    )
                };
                if awake {
                    continue;
                }
                return Err(signal_flow::StartRejection::RequirementUnmet(required));
            };
            let metaflow = {
                let _turn = self
                    .dispatch_gate
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                if !matches!(self.store.metaflow_declaration(&required), Ok(Some(_))) {
                    return Err(signal_flow::StartRejection::RequirementUnmet(required));
                }
                self.rest_ended_awake(configured, &required);
                match self.store.metaflow(&required) {
                    Ok(Some(metaflow)) => match &metaflow.state {
                        flow_ethos::State::Awake(flow_id)
                            if matches!(self.store.flow_is_live(flow_id), Ok(true)) =>
                        {
                            continue;
                        }
                        _ => metaflow,
                    },
                    Ok(None) => Metaflow {
                        address: required.clone(),
                        state: flow_ethos::State::Asleep,
                        past: vec![],
                        queue: vec![],
                    },
                    Err(_) => {
                        return Err(signal_flow::StartRejection::LaunchPersistenceRefused);
                    }
                }
            };
            let WakePlan::Fresh(fresh) = self.wake_plan(&metaflow) else {
                return Err(signal_flow::StartRejection::RequirementUnmet(required));
            };
            let flow_id =
                self.start_fresh(configured, &metaflow, fresh, "")
                    .map_err(|failure| {
                        eprintln!(
                            "flow-nexus: required metaflow {:?} {} {:?} did not start: {failure:?}",
                            required.aspect, required.topic, required.layer
                        );
                        signal_flow::StartRejection::RequirementUnmet(required.clone())
                    })?;
            {
                let _turn = self
                    .dispatch_gate
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                if self.perform_configured(
                    configured,
                    Operation::Record(Record_Data::Awoken(Record_Data_Awoken_Data {
                        address: required.clone(),
                        flow_id,
                        delivered: 0,
                    })),
                ) != Outcome::Recorded
                {
                    return Err(signal_flow::StartRejection::LaunchPersistenceRefused);
                }
            }
            self.drain_woken(configured, &required, lease);
            self.settle_stops_at_confirmation_configured(configured);
        }
        Ok(())
    }
}

/// Why a flow the Nexus starts itself did not start: its start was refused
/// for a reason, or it failed before or after the start was asked.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum FreshRefusal {
    Rejected(signal_flow::StartRejection),
    Failed(Failed_Data),
}

/// A flow the Nexus starts itself, as a wake starts one: no calling flow is
/// its origin. The start runs under the dispatch gate, which is released
/// while the launch settles.
pub(crate) trait StartsUnbound {
    #[cfg(test)]
    fn start_unbound(&self, profile: LaunchProfile) -> Result<FlowId, FreshRefusal>;
    fn start_unbound_configured(
        &self,
        configured: &crate::ConfiguredUse,
        profile: LaunchProfile,
    ) -> Result<FlowId, FreshRefusal>;
}

impl StartsUnbound for RunningNexus {
    #[cfg(test)]
    fn start_unbound(&self, profile: LaunchProfile) -> Result<FlowId, FreshRefusal> {
        let configured = self
            .test_configured_use()
            .map_err(|_| FreshRefusal::Failed(Failed_Data::StoreRefused))?;
        self.start_unbound_configured(&configured, profile)
    }

    fn start_unbound_configured(
        &self,
        configured: &crate::ConfiguredUse,
        profile: LaunchProfile,
    ) -> Result<FlowId, FreshRefusal> {
        let launch_request_id = profile.launch_request_id.clone();
        let response = {
            let _turn = self
                .dispatch_gate
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            // The Nexus starts this flow itself: no calling flow is its
            // origin.
            let response = self.start_configured(configured, profile, Origin::Unbound);
            self.settle_configured(configured, &launch_request_id, response)
        };
        // A send tool call made by the flow's first turn cannot wake its
        // target while this launch holds the dispatch gate. The gate is
        // released now, so deliver every deferred wake before interpreting
        // the launch result.
        self.wake_deferred(configured);
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
    fn waken(
        &self,
        configured: &crate::ConfiguredUse,
        address: &Address,
    ) -> Result<FlowId, WakeRefusal>;
    fn start_fresh(
        &self,
        configured: &crate::ConfiguredUse,
        metaflow: &Metaflow,
        fresh: FreshContext,
        text: &str,
    ) -> Result<FlowId, FreshRefusal>;
    /// Types text into a live flow's pane under its lease, as Flow's own
    /// write: no Priority head, since it is not a Message. A refusal is
    /// logged by its reason.
    fn hand_over(
        &self,
        configured: &crate::ConfiguredUse,
        flow_id: &str,
        text: &str,
    ) -> Result<(), HandOverRefusal>;
    fn last_launch_profile(&self, flow_id: &str) -> Option<LaunchProfile>;
    /// Writes the context size the flow's native transcript shows now, the
    /// size a Compact of the flow is judged by.
    #[cfg(test)]
    fn record_context_at_stop(&self, flow_id: &str);
    fn record_context_at_stop_configured(&self, configured: &crate::ConfiguredUse, flow_id: &str);
    /// Refuses a wake while a pane already runs this flow's native session:
    /// SessionLive when one does, HerdrUnreadable when Herdr is unreadable.
    fn refuse_live_session(
        &self,
        configured: &crate::ConfiguredUse,
        node: &FlowNode,
        sessions: &[String],
    ) -> Result<(), WakeRefusal>;
    /// The body of `settle_stopped`: waits, boundedly and off the dispatch
    /// gate, for Herdr to show the seat resting, then drains it.
    #[cfg(test)]
    fn settle_rested(&self, flow_id: &str) -> AtStop;
    fn settle_rested_configured(&self, configured: &ConfiguredUse, flow_id: &str) -> AtStop;
    /// Closes a resting seat's harness under the pane's lease and records
    /// its flow Stopped (its metaflow Asleep), under the dispatch gate the
    /// caller holds. The refusal names why the seat stays.
    fn close_at_rest(
        &self,
        configured: &crate::ConfiguredUse,
        flow_id: &str,
        node: &FlowNode,
    ) -> Result<(), Box<Outcome>>;
    /// Replaces a resting seat owed a successor. The caller has released
    /// the dispatch gate and holds the metaflow's waking lease, so no wake
    /// or drain runs meanwhile: the successor is started and confirmed
    /// through Replace before the predecessor is stopped and closed. The
    /// successor is owed for `cause` until it is confirmed.

    fn refresh_successor_first_configured(
        &self,
        configured: &crate::ConfiguredUse,
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
    fn take_up_owed(&self, configured: &ConfiguredUse, intent: SuccessorIntent) -> AtStop;
    /// Settles the successor owed at `address` by its Replace's answer: a
    /// launch that has not settled, or a reap refused once the successor
    /// was confirmed, stays owed under the same request; any other answer
    /// forgets it, so the next refresh owes a new launch.
    fn settle_owed(&self, address: &Address, response: &Response);
    /// Whether the launch request has settled refused.
    fn launch_refused(&self, launch_request_id: &str) -> bool;
}

pub(crate) trait ReadsClaudeDelivery {
    #[cfg(test)]
    fn claude_transcript(&self, node: &FlowNode) -> Option<std::path::PathBuf>;
    fn claude_transcript_configured(
        &self,
        configured: &crate::ConfiguredUse,
        node: &FlowNode,
    ) -> Option<std::path::PathBuf>;
    #[cfg(test)]
    fn claude_main_turn_ended(&self, node: &FlowNode) -> bool;
    fn claude_main_turn_ended_configured(
        &self,
        configured: &crate::ConfiguredUse,
        node: &FlowNode,
    ) -> bool;
    #[cfg(test)]
    fn claude_delivery_cursor(&self, node: &FlowNode) -> Option<NativeTranscriptCursor>;
    fn claude_delivery_cursor_configured(
        &self,
        configured: &crate::ConfiguredUse,
        node: &FlowNode,
    ) -> Option<NativeTranscriptCursor>;
    /// What the flow's transcript after the attempt's cursor shows of its
    /// request; Unreadable when the attempt is no longer the flow's own
    /// session's.
    #[cfg(test)]
    fn claude_delivery_history(&self, attempt: &QueueDeliveryAttempt) -> ClaudeQueuedHistory;
    fn claude_delivery_history_configured(
        &self,
        configured: &crate::ConfiguredUse,
        attempt: &QueueDeliveryAttempt,
    ) -> ClaudeQueuedHistory;
}

impl ReadsClaudeDelivery for RunningNexus {
    #[cfg(test)]
    fn claude_transcript(&self, node: &FlowNode) -> Option<std::path::PathBuf> {
        let configured = self.test_configured_use().ok()?;
        self.claude_transcript_configured(&configured, node)
    }

    fn claude_transcript_configured(
        &self,
        configured: &crate::ConfiguredUse,
        node: &FlowNode,
    ) -> Option<std::path::PathBuf> {
        configured
            .herdr
            .native_transcript_root(&HarnessKind::Claude, "")
            .ok()?
            .session_transcript(&node.session_id)
    }

    #[cfg(test)]
    fn claude_main_turn_ended(&self, node: &FlowNode) -> bool {
        let Ok(configured) = self.test_configured_use() else {
            return false;
        };
        self.claude_main_turn_ended_configured(&configured, node)
    }

    fn claude_main_turn_ended_configured(
        &self,
        configured: &crate::ConfiguredUse,
        node: &FlowNode,
    ) -> bool {
        node.harness_kind == HarnessKind::Claude
            && configured
                .herdr
                .native_transcript_root(&HarnessKind::Claude, "")
                .ok()
                .and_then(|root| root.session_transcript(&node.session_id))
                .is_some_and(|transcript| transcript.main_turn_ended(&node.session_id))
    }

    #[cfg(test)]
    fn claude_delivery_cursor(&self, node: &FlowNode) -> Option<NativeTranscriptCursor> {
        let configured = self.test_configured_use().ok()?;
        self.claude_delivery_cursor_configured(&configured, node)
    }

    fn claude_delivery_cursor_configured(
        &self,
        configured: &crate::ConfiguredUse,
        node: &FlowNode,
    ) -> Option<NativeTranscriptCursor> {
        let cursor = match configured
            .herdr
            .native_transcript_root(&HarnessKind::Claude, "")
            .ok()?
            .session_transcript(&node.session_id)
        {
            Some(transcript) => transcript.claude_cursor()?,
            None => return None,
        };
        Some(NativeTranscriptCursor {
            device: cursor.device,
            inode: cursor.inode,
            offset: cursor.offset,
        })
    }

    #[cfg(test)]
    fn claude_delivery_history(&self, attempt: &QueueDeliveryAttempt) -> ClaudeQueuedHistory {
        let Ok(configured) = self.test_configured_use() else {
            return ClaudeQueuedHistory::Unreadable;
        };
        self.claude_delivery_history_configured(&configured, attempt)
    }

    fn claude_delivery_history_configured(
        &self,
        configured: &crate::ConfiguredUse,
        attempt: &QueueDeliveryAttempt,
    ) -> ClaudeQueuedHistory {
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
        let Some(transcript) = configured
            .herdr
            .native_transcript_root(&HarnessKind::Claude, "")
            .ok()
            .and_then(|root| root.session_transcript(&node.session_id))
        else {
            return ClaudeQueuedHistory::Unreadable;
        };
        let Some(text) = attempt.prompt_text(&self.store) else {
            return ClaudeQueuedHistory::Unreadable;
        };
        transcript.queued_history_after(
            &node.session_id,
            &ClaudeTranscriptCursor {
                device: cursor.device,
                inode: cursor.inode,
                offset: cursor.offset,
            },
            &text,
        )
    }
}

/// Whether a harness event is the flow's own. A hook runs in every process
/// its harness starts, a nested one included, under the same FLOW_ID; only
/// an event that names the flow's registered native session, and for a Stop
/// that session's own transcript, is taken for the flow's.
pub(crate) trait CorrelatesNativeEvents {
    /// The flow's own Claude turn end.
    fn correlated_stop_configured(
        &self,
        configured: &ConfiguredUse,
        flow_id: &str,
        stop: &NativeStop,
    ) -> bool;
    /// A prompt the flow's own Claude session took.
    fn correlated_prompt(&self, flow_id: &str, prompt: &NativePrompt) -> bool;
    /// Of the flow's own Stops and submitted prompts, the last recorded is
    /// a Stop: a turn end landed with no prompt taken after it.
    fn turn_end_recorded_configured(&self, configured: &ConfiguredUse, flow_id: &str) -> bool;
}

impl CorrelatesNativeEvents for RunningNexus {
    fn correlated_stop_configured(
        &self,
        configured: &ConfiguredUse,
        flow_id: &str,
        stop: &NativeStop,
    ) -> bool {
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
            .then(|| self.claude_transcript_configured(configured, &node))
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

    fn turn_end_recorded_configured(&self, configured: &ConfiguredUse, flow_id: &str) -> bool {
        let Ok(Some(events)) = self.store.events(flow_id) else {
            return false;
        };
        events
            .iter()
            .rev()
            .find_map(|event| match event {
                Event::Stopped(stop) => self
                    .correlated_stop_configured(configured, flow_id, &stop)
                    .then_some(true),
                Event::PromptSubmitted(prompt) => {
                    self.correlated_prompt(flow_id, &prompt).then_some(false)
                }
                Event::Started(_)
                | Event::ToolUsed(_)
                | Event::Notified(_)
                | Event::Ended(_)
                | Event::Cleared(_) => None,
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
    #[cfg(test)]
    fn reconcile_delivery(&self, attempt: &QueueDeliveryAttempt) -> Reconciled;
    fn reconcile_delivery_configured(
        &self,
        configured: &crate::ConfiguredUse,
        attempt: &QueueDeliveryAttempt,
    ) -> Reconciled;
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
    fn settle_submitted_prompt(
        &self,
        configured: &ConfiguredUse,
        flow_id: &str,
        prompt: &NativePrompt,
    );
    /// At the Nexus's start: reconciles every hand-over the last Nexus left
    /// outstanding, then settles every Claude seat whose own turn end was
    /// recorded and which holds work, as a Stop would.
    #[cfg(test)]
    fn recover_deliveries(&self);
    fn recover_deliveries_configured(&self, configured: &ConfiguredUse);
}

pub(crate) fn render_message(
    entry: &QueueEntry,
    sender: meta_signal_flow::Sender,
) -> meta_signal_flow::Message {
    let request = match &entry.message {
        signal_flow::Message::HardAbrupt(request)
        | signal_flow::Message::MiddleAbrupt(request)
        | signal_flow::Message::Soft(request) => request,
    };
    let (kind, body) = match request {
        signal_flow::Request::Order(words) => (
            meta_signal_flow::Kind::Order,
            meta_signal_flow::Body::Words(words.clone()),
        ),
        signal_flow::Request::Question(words) => (
            meta_signal_flow::Kind::Question,
            meta_signal_flow::Body::Words(words.clone()),
        ),
        signal_flow::Request::Result(words) => (
            meta_signal_flow::Kind::Result,
            meta_signal_flow::Body::Words(words.clone()),
        ),
        signal_flow::Request::Notice(words) => (
            meta_signal_flow::Kind::Notice,
            meta_signal_flow::Body::Words(words.clone()),
        ),
        signal_flow::Request::Psyche(said) => (
            meta_signal_flow::Kind::Psyche,
            meta_signal_flow::Body::Psyche(meta_signal_flow::PsycheWords {
                first_string: said.context.clone(),
                second_string: said.verbatim.clone(),
            }),
        ),
        signal_flow::Request::Psyches(_) => (
            meta_signal_flow::Kind::Psyche,
            meta_signal_flow::Body::Words(request.native_content()),
        ),
    };
    let letter = meta_signal_flow::Letter { kind, sender, body };
    match &entry.message {
        signal_flow::Message::HardAbrupt(_) => meta_signal_flow::Message::HardAbrupt(letter),
        signal_flow::Message::MiddleAbrupt(_) => meta_signal_flow::Message::MiddleAbrupt(letter),
        signal_flow::Message::Soft(_) => meta_signal_flow::Message::Soft(letter),
    }
}

trait RendersAttemptInput {
    fn prompt_text(&self, store: &FlowStore) -> Option<String>;
    fn as_settled(&self, seat_after: SeatAfter) -> Reconciled;
}

impl RendersAttemptInput for QueueDeliveryAttempt {
    fn prompt_text(&self, store: &FlowStore) -> Option<String> {
        match &self.queue_evidence {
            meta_signal_flow::QueueEvidence::Logical(_) => self
                .cohort
                .iter()
                .map(|entry| {
                    store
                        .logical_delivery_sender(&entry.logical_delivery_id)
                        .ok()
                        .flatten()
                        .map(|sender| entry.message.typed_pane_message(sender))
                })
                .collect::<Option<Vec<_>>>()
                .map(|messages| messages.datomize(Vec::new()).protosize().compact()),
            meta_signal_flow::QueueEvidence::Legacy(legacy) => {
                Some(legacy.request.native_content())
            }
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

impl RunningNexus {
    fn typed_queue_message(&self, entry: &QueueEntry) -> Option<meta_signal_flow::Message> {
        let sender = self
            .store
            .logical_delivery_sender(&entry.logical_delivery_id)
            .ok()??;
        Some(entry.message.typed_pane_message(sender))
    }

    pub(crate) fn typed_queue_cohort(
        &self,
        entries: &[QueueEntry],
    ) -> Option<Vec<meta_signal_flow::Message>> {
        entries
            .iter()
            .map(|entry| self.typed_queue_message(entry))
            .collect()
    }

    pub(crate) fn typed_attempt_text(&self, attempt: &QueueDeliveryAttempt) -> Option<String> {
        let meta_signal_flow::QueueEvidence::Logical(entry) = &attempt.queue_evidence else {
            return None;
        };
        let messages = self.typed_queue_cohort(std::slice::from_ref(entry))?;
        Some(messages.datomize(Vec::new()).protosize().compact())
    }
}

impl ReconcilesQueueDelivery for RunningNexus {
    #[cfg(test)]
    fn reconcile_delivery(&self, attempt: &QueueDeliveryAttempt) -> Reconciled {
        let configured = match self.test_configured_use() {
            Ok(configured) => configured,
            Err(_) => return Reconciled::Refused,
        };
        self.reconcile_delivery_configured(&configured, attempt)
    }

    fn reconcile_delivery_configured(
        &self,
        configured: &crate::ConfiguredUse,
        attempt: &QueueDeliveryAttempt,
    ) -> Reconciled {
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
            QueueDeliveryPhase::Absent(_) => return Reconciled::Absent,
            QueueDeliveryPhase::Resolved(record) => {
                return match record.queue_resolution_outcome {
                    meta_signal_flow::QueueResolutionOutcome::Delivered => {
                        attempt.as_settled(SeatAfter::InItsTurn)
                    }
                    meta_signal_flow::QueueResolutionOutcome::Absent => Reconciled::Absent,
                };
            }
            QueueDeliveryPhase::TransportOverlimit(_) => return Reconciled::Refused,
            QueueDeliveryPhase::Prepared | QueueDeliveryPhase::Uncertain(_) => {}
        }
        let Some(text) = attempt.prompt_text(&self.store) else {
            return Reconciled::Refused;
        };
        let found = match attempt.harness_kind {
            HarnessKind::Claude => {
                match self.claude_delivery_history_configured(configured, attempt) {
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
                }
            }
            HarnessKind::Codex => {
                let NativeHistoryAnchor::CodexTurns(anchor) = &attempt.history_anchor else {
                    return self.undecided(attempt, Undecided::HistoryUnreadable);
                };
                let history = self
                    .store
                    .role(&attempt.flow_id)
                    .ok()
                    .flatten()
                    .and_then(|role| {
                        configured
                            .codex_endpoints
                            .adapter_for(&role.model_name)
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
                        // An absent row at this instant is not proof that an
                        // unacknowledged request cannot materialize later.
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
        if attempt.cohort.len() > 1 {
            return if self.store.settle_received_cohort(&attempt.attempt_id, receipt).is_ok() {
                attempt.as_settled(seat_after)
            } else {
                Reconciled::Refused
            };
        }
        if self.store.record_delivery_receipt(&attempt.attempt_id, receipt).unwrap_or(false)
            && self.store.settle_recorded_delivery(&attempt.attempt_id).unwrap_or(false)
        {
            attempt.as_settled(seat_after)
        } else {
            Reconciled::Refused
        }
    }

    fn settle_submitted_prompt(
        &self,
        configured: &ConfiguredUse,
        flow_id: &str,
        prompt: &NativePrompt,
    ) {
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
        let Some(attempt) = attempts.into_iter().find(|attempt| {
            attempt.flow_id == flow_id
                && attempt.harness_kind == HarnessKind::Claude
                && prompt.native_session_id_option.as_deref() == Some(attempt.thread_id.as_str())
                && matches!(
                    attempt.phase,
                    QueueDeliveryPhase::Prepared | QueueDeliveryPhase::Uncertain(_)
                )
                && attempt
                    .prompt_text(&self.store)
                    .is_some_and(|text| text.prompt_sha256() == prompt.prompt_sha256)
        }) else {
            return;
        };
        // A native hook proves that a submission was observed, not that an
        // exact USER row has been durably recorded. Reconcile the transcript
        // after its pre-submit anchor; an unreadable or not-yet-written row
        // remains explicitly uncertain and cannot remove the logical entry.
        let _ = self.reconcile_delivery_configured(configured, &attempt);
    }

    #[cfg(test)]
    fn recover_deliveries(&self) {
        let Ok(configured) = self.test_configured_use() else {
            return;
        };
        self.recover_deliveries_configured(&configured)
    }

    fn recover_deliveries_configured(&self, configured: &ConfiguredUse) {
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
                    let reconciled = self.reconcile_delivery_configured(configured, &attempt);
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
                        && self.turn_end_recorded_configured(configured, flow_id)
                })
                .collect::<Vec<_>>()
        };
        for flow_id in seats {
            self.settle_stopped_configured(configured, &flow_id);
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
    #[cfg(test)]
    fn settle_stopped(&self, flow_id: &str) -> AtStop {
        let Ok(configured) = self.test_configured_use() else {
            return AtStop::Kept(KeptAtStop::StoreUnreadable);
        };
        self.settle_stopped_configured(&configured, flow_id)
    }

    fn settle_stopped_configured(&self, configured: &ConfiguredUse, flow_id: &str) -> AtStop {
        let at_stop = self.settle_rested_configured(configured, flow_id);
        match &at_stop {
            AtStop::Kept(reason) => {
                eprintln!("flow-nexus: flow {flow_id} kept at its Stop: {reason:?}");
            }
            // Work queued for a refreshed metaflow is handed to its
            // successor, or wakes its next flow once the gate the refresh ran
            // under is given back; with nothing queued an OnDemand metaflow
            // sleeps until an Order, a Question or the living's words.
            AtStop::Refreshed(address) => {
                eprintln!("flow-nexus: flow {flow_id} refreshed at its rest");
                self.wake_due_configured(configured, address.clone());
            }
            AtStop::Slept(_) => {
                eprintln!("flow-nexus: flow {flow_id} slept at its rest");
            }
            AtStop::Idle | AtStop::Drained(_) => {}
        }
        at_stop
    }

    #[cfg(test)]
    fn drain_awake(&self, address: &Address) {
        let Ok(configured) = self.test_configured_use() else {
            return;
        };
        self.drain_awake_configured(&configured, address)
    }

    fn drain_awake_configured(&self, configured: &ConfiguredUse, address: &Address) {
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
                configured.herdr.pane_presence(&node),
                PanePresence::Absent | PanePresence::Bare
            ) {
                let _ = self.perform_configured(
                    configured,
                    Operation::Record(Record_Data::Exited(flow_id.clone())),
                );
                return;
            }
            if metaflow
                .queue
                .first()
                .is_some_and(|entry| !matches!(entry.message, signal_flow::Message::Soft(_)))
            {
                let _ = self.drain_all_configured(configured, address.clone());
                return;
            }
            let known_resting = match node.harness_kind {
                HarnessKind::Codex => self.codex_resting(&flow_id),
                HarnessKind::Claude => self.claude_main_turn_ended_configured(configured, &node),
            };
            known_resting.then_some(flow_id)
        };
        if let Some(flow_id) = resting {
            self.settle_stopped_configured(configured, &flow_id);
        }
    }

    #[cfg(test)]
    fn wake_due(&self, address: Address) {
        let Ok(configured) = self.test_configured_use() else {
            return;
        };
        self.wake_due_configured(&configured, address)
    }

    fn wake_due_configured(&self, configured: &ConfiguredUse, address: Address) {
        // Work queued for an Awake seat known resting is handed to it now,
        // as its turn's end would; one in a turn waits for the turn's end.
        // An Awake seat whose pane is gone is put Asleep, to wake below.
        self.drain_awake_configured(configured, &address);
        let Some(lease) = self.waking.hold(&address) else {
            return;
        };
        let due = {
            let _turn = self
                .dispatch_gate
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            self.rest_ended_awake(configured, &address);
            matches!(
                self.store.metaflow(&address),
                Ok(Some(metaflow)) if self.wake_is_due(&metaflow)
            )
        };
        if !due {
            return;
        }
        // A refused wake has logged its reason; its work stays queued.
        if let Outcome::Woken(_) = self
            .configured_effects(configured)
            .perform(Operation::Wake(address.clone()))
        {
            self.drain_woken(configured, &address, lease);
        }
        self.settle_stops_at_confirmation_configured(configured);
    }

    fn drain_woken(&self, configured: &ConfiguredUse, address: &Address, lease: WakingLease<'_>) {
        loop {
            let _turn = self
                .dispatch_gate
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            match self
                .configured_effects(configured)
                .perform(Operation::Drain(address.clone()))
            {
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
                tool_set: declared.tool_set.clone(),
                system_modules: declared.system_modules,
                user_modules: declared.user_modules,
                left_out_option: None,
                purpose: declared.purpose,
            }),
            (_, Some(profile)) => WakePlan::Fresh(FreshContext {
                herdr_session_name: profile.herdr_session_name,
                tool_set: profile.tool_set.clone(),
                system_modules: profile.system_modules,
                user_modules: profile.user_modules,
                left_out_option: profile.left_out_option,
                purpose: String::new(),
            }),
            (_, None) => WakePlan::Unplaceable,
        }
    }

    fn wake_is_due(&self, metaflow: &Metaflow) -> bool {
        metaflow.state == flow_ethos::State::Asleep
            && (metaflow
                .queue
                .iter()
                .any(|entry| entry.delivery == signal_flow::Delivery::Wake)
                || matches!(self.store.successor_intent(&metaflow.address), Ok(Some(_))))
    }

    #[cfg(test)]
    fn recover_wakes(&self) {
        let Ok(configured) = self.test_configured_use() else {
            return;
        };
        self.recover_wakes_configured(&configured)
    }

    fn recover_wakes_configured(&self, configured: &ConfiguredUse) {
        self.recover_deliveries_configured(configured);
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
                        flow_ethos::State::Awake(current) if current == intent.predecessor => {
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
                            if !matches!(self.store.address_hold(&current), Ok(Some(_))) {
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
            self.settle_stopped_configured(configured, &flow_id);
        }
        for intent in owed {
            let predecessor = intent.predecessor.clone();
            match self.take_up_owed(configured, intent) {
                AtStop::Refreshed(address) => {
                    eprintln!(
                        "flow-nexus: flow {predecessor} replaced by the successor it was owed"
                    );
                    // What queued meanwhile is handed to the successor.
                    self.wake_due_configured(configured, address);
                }
                kept => {
                    eprintln!(
                        "flow-nexus: successor owed after {predecessor} not started: {kept:?}"
                    );
                }
            }
        }
        let due = {
            let _turn = self
                .dispatch_gate
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            self.rest_every_ended_awake(configured);
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
            self.wake_due_configured(configured, address);
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
    fn refresh_configured(&self, configured: &ConfiguredUse, address: &Address) -> Response;
    /// A Replace, as a Launch addressed to an Awake metaflow names one: a
    /// refresh whose successor is the profile's. Under the metaflow's lock
    /// the successor is started and confirmed, then the flow it replaces is
    /// reaped, given no turn; once the lock is released the queue is handed
    /// to the successor, oldest first. A metaflow another holds is refused
    /// Locked. Runs off the dispatch gate, taking it for the replacement.
    fn replace_held_configured(
        &self,
        configured: &ConfiguredUse,
        profile: LaunchProfile,
        origin: Origin,
    ) -> Response;
}

impl RefreshesMetaflows for RunningNexus {
    fn refresh_configured(&self, configured: &ConfiguredUse, address: &Address) -> Response {
        use crate::store::metaflow_declaration::ReadsMetaflowDeclarations;
        use signal_flow::{RefreshRejection, Refreshed, Refusal, Unknown_Data};
        let turn = self
            .dispatch_gate
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let Some(lock) = self.waking.hold(address) else {
            return Response::RefreshRejected(RefreshRejection::Locked);
        };
        self.rest_ended_awake(configured, address);
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
        let flow_ethos::State::Awake(flow_id) = metaflow.state.clone() else {
            return Response::RefreshRejected(RefreshRejection::NotAwake);
        };
        // A flow whose launch is not yet confirmed only holds its Address:
        // the launch that holds it is the owed successor's, still pending
        // or ambiguous, or a waking still running.
        match self.store.address_hold(&flow_id) {
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
        let node = match self.store.flow_node(&flow_id) {
            Ok(Some(node)) => node,
            Ok(None) => {
                return Response::Refused(Refusal::Unknown(Unknown_Data::FlowId(flow_id)));
            }
            Err(error) => return Response::Refused(Refusal::Store(error.to_string())),
        };
        // The recorded route is read first: a Herdr that cannot be read
        // shows no pane, and is never taken for one gone.
        match configured.herdr.pane_presence(&node) {
            PanePresence::Present => {}
            // A flow whose pane is gone has ended on its own: it is recorded
            // Exited, which puts the metaflow Asleep.
            PanePresence::Absent | PanePresence::Bare => {
                let _ = self.perform_configured(
                    configured,
                    Operation::Record(Record_Data::Exited(flow_id)),
                );
                return Response::RefreshRejected(RefreshRejection::NotAwake);
            }
            PanePresence::Unknown => {
                return Response::RefreshRejected(RefreshRejection::HerdrUnavailable);
            }
        }
        let node = configured.herdr.refresh_route(node);
        let continuity = match self.store.continuity(address) {
            Ok(continuity) => continuity,
            Err(error) => return Response::Refused(Refusal::Store(error.to_string())),
        };
        let wakes = metaflow
            .queue
            .iter()
            .any(|entry| entry.delivery == signal_flow::Delivery::Wake);
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
            let replaced = self.refresh_successor_first_configured(
                configured,
                &flow_id,
                &metaflow,
                SuccessionCause::Ordered,
            );
            drop(lock);
            self.wake_deferred(configured);
            return match replaced {
                AtStop::Refreshed(_) => {
                    eprintln!(
                        "flow-nexus: flow {flow_id} refreshed; {:?} {} {:?} runs its successor",
                        address.aspect, address.topic, address.layer
                    );
                    // What queued while the lock stood is handed to the
                    // successor, oldest first.
                    self.wake_due_configured(configured, address.clone());
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
        if let Err(outcome) = self.close_at_rest(configured, &flow_id, &node) {
            return Response::RefreshRejected(match *outcome {
                Outcome::Failed(Failed_Data::PaneCloseRefused(failure)) => {
                    RefreshRejection::CloseRefused(failure)
                }
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
            address.aspect, address.topic, address.layer
        );
        refreshed
    }

    fn replace_held_configured(
        &self,
        configured: &ConfiguredUse,
        profile: LaunchProfile,
        origin: Origin,
    ) -> Response {
        use signal_flow::{ReplaceRejection, StartRejection};
        let address = profile.address.clone();
        let response = {
            let _turn = self
                .dispatch_gate
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let Some(lock) = self.waking.hold(&address) else {
                return Response::ReplaceRejected(ReplaceRejection::Locked);
            };
            // The successor is owed until it is confirmed, so a Nexus that
            // stops before then starts it at its next start.
            if let Some(predecessor) = profile.flow_id_option.clone()
                && self
                    .store
                    .owe_successor(SuccessorIntent {
                        address: address.clone(),
                        predecessor,
                        cause: SuccessionCause::Replace,
                        launch_request_id: profile.launch_request_id.clone(),
                    })
                    .is_err()
            {
                return Response::ReplaceRejected(ReplaceRejection::LaunchRefused(
                    StartRejection::LaunchPersistenceRefused,
                ));
            }
            let response = self.replace_configured(configured, profile, origin);
            self.settle_owed(&address, &response);
            drop(lock);
            response
        };
        self.wake_deferred(configured);
        // What queued for the metaflow is handed to the flow it is Awake as
        // now, oldest first.
        self.wake_due_configured(configured, address);
        response
    }
}

impl PerformsWakeAndDrain for RunningNexus {
    #[cfg(test)]
    fn wake(&self, address: Address) -> Outcome {
        let configured = match self.test_configured_use() {
            Ok(configured) => configured,
            Err(_) => return Outcome::Failed(Failed_Data::StoreRefused),
        };
        self.wake_configured(&configured, address)
    }

    fn wake_configured(&self, configured: &crate::ConfiguredUse, address: Address) -> Outcome {
        match self.waken(configured, &address) {
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

    #[cfg(test)]
    fn drain(&self, address: Address) -> Outcome {
        // One queued item is one effect. `drain_all` calls this again for the
        // next item, so a later committed generation takes effect there.
        let configured = match self.test_configured_use() {
            Ok(configured) => configured,
            Err(_) => return Outcome::Failed(Failed_Data::StoreRefused),
        };
        self.drain_configured(&configured, address)
    }

    fn drain_configured(&self, configured: &crate::ConfiguredUse, address: Address) -> Outcome {
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
            Ok(Some(attempt)) => match self.reconcile_delivery_configured(configured, &attempt) {
                Reconciled::Settled(queued, SeatAfter::Resting) => drained.push(queued),
                Reconciled::Settled(queued, SeatAfter::InItsTurn) => {
                    if attempt.flow_id == flow_id
                        && attempt.thread_id == node.session_id
                        && node.harness_kind == HarnessKind::Codex
                    {
                        // Only this bound seat's turn can hold its work.
                        // A predecessor's receipt never marks its successor busy.
                        // The settled turn may still run: what follows waits
                        // for its end.
                        self.codex_turns.delivered(&node.session_id);
                        return Outcome::Drained(vec![queued]);
                    }
                    drained.push(queued);
                }
                Reconciled::Absent => {}
                Reconciled::Undecided(Undecided::AbsenceUnprovable) => {
                    eprintln!(
                        "flow-nexus: queued delivery {} remains uncertain (AbsenceUnprovable); parking it without retry",
                        attempt.attempt_id
                    );
                    if !self
                        .store
                        .park_uncertain_delivery(&attempt.attempt_id)
                        .unwrap_or(false)
                    {
                        return Outcome::Failed(Failed_Data::StoreRefused);
                    }
                    return match self.drain_configured(configured, address) {
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
        let queue = match self.store.metaflow(&address) {
            Ok(Some(metaflow)) => metaflow.queue,
            _ => return Outcome::Failed(Failed_Data::StoreRefused),
        };
        let Some(oldest) = queue.first().cloned() else {
            return Outcome::Drained(drained);
        };
        let cohort: Vec<QueueEntry> = queue;
        let soft = |entry: &QueueEntry| matches!(entry.message, signal_flow::Message::Soft(_));
        if cohort.iter().all(soft) {
            let resting = match node.harness_kind {
                HarnessKind::Codex => self.codex_resting(&flow_id),
                HarnessKind::Claude => {
                    self.turn_end_recorded_configured(configured, &flow_id)
                        || self.claude_main_turn_ended_configured(configured, &node)
                }
            };
            if !resting {
                return Outcome::WaitingForTurn(oldest.logical_delivery_id);
            }
        }
        let refused = |drained: Vec<QueueEntry>, failure| {
            if drained.is_empty() {
                Outcome::Failed(failure)
            } else {
                Outcome::Drained(drained)
            }
        };
        // Native history is anchored before any native input.
        let (anchor, model) = match node.harness_kind {
            HarnessKind::Claude => {
                match self.claude_delivery_cursor_configured(configured, &node) {
                    Some(cursor) => (NativeHistoryAnchor::ClaudeTranscript(cursor), None),
                    None => return refused(drained, Failed_Data::HerdrRefused),
                }
            }
            HarnessKind::Codex => {
                let model = match self.store.role(&flow_id) {
                    Ok(Some(role)) => role.model_name,
                    _ => return refused(drained, Failed_Data::StoreRefused),
                };
                let adapter = match configured.codex_endpoints.adapter_for(&model) {
                    Ok(adapter) => adapter,
                    Err(_) => return refused(drained, Failed_Data::CodexRefused),
                };
                match adapter.newest_turn(&node.session_id) {
                    Ok(newest) => (NativeHistoryAnchor::CodexTurns(newest), Some(adapter)),
                    Err(_) => return refused(drained, Failed_Data::CodexRefused),
                }
            }
        };
        let Some(messages) = self.typed_queue_cohort(&cohort) else {
            return refused(drained, Failed_Data::StoreRefused);
        };
        let text = messages.datomize(Vec::new()).protosize().compact();
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
        let maximum_bytes = configured.snapshot.limits.cohort;
        if text.len() > usize::try_from(maximum_bytes).unwrap_or(0) {
            let size = signal_flow::SendPayloadSize {
                actual_bytes: i64::try_from(text.len()).unwrap_or(i64::MAX),
                maximum_bytes,
            };
            return match self.store.fail_transport_overlimit(&attempt.attempt_id, size) {
                Ok(failed) => Outcome::Drained(failed),
                Err(_) => refused(drained, Failed_Data::StoreRefused),
            };
        }
        if model.is_none() || !cohort.iter().all(soft) {
            let actual = text.len().saturating_add(crate::sending::HERDR_PROMPT_FRAME_BYTES);
            let limit = configured.snapshot.limits.herdr_line;
            if text.len() > crate::sending::MAX_NATIVE_COHORT_BYTES
                || actual > usize::try_from(limit).unwrap_or(0)
            {
                let effective = limit.min(crate::sending::MAX_NATIVE_COHORT_BYTES as i64);
                let size = signal_flow::SendPayloadSize {
                    actual_bytes: i64::try_from(actual).unwrap_or(i64::MAX),
                    maximum_bytes: effective,
                };
                return match self.store.fail_transport_overlimit(&attempt.attempt_id, size) {
                    Ok(failed) => Outcome::Drained(failed),
                    Err(_) => refused(drained, Failed_Data::StoreRefused),
                };
            }
        }
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
        if !cohort.iter().all(soft) {
            let view = crate::herdr::HerdrOperationView {
                configuration: crate::store::CommittedConfigurationSnapshot {
                    generation: configured.generation.clone(),
                    snapshot: configured.snapshot.clone(),
                },
                herdr: configured.herdr.clone(),
            };
            let result = self.deliver_cohort_with_view(
                configured,
                &view,
                attempt.attempt_id.clone(),
                &flow_id,
                messages.clone(),
                false,
            );
            let proved_absent = matches!(&result,
                meta_signal_flow::Response::DeliveryRejected(rejected)
                    if matches!(rejected,
                        meta_signal_flow::DeliveryRejection::UnknownFlow
                        | meta_signal_flow::DeliveryRejection::FlowStopped
                        | meta_signal_flow::DeliveryRejection::RouteUnavailable
                        | meta_signal_flow::DeliveryRejection::RecipientWorking
                        | meta_signal_flow::DeliveryRejection::RecipientBlocked
                        | meta_signal_flow::DeliveryRejection::ComposerOccupied));
            if proved_absent {
                let _ = self.store.mark_delivery_absent(
                    &attempt.attempt_id,
                    AbsenceRecord {
                        proof: AbsenceProof::HandOverRefused,
                        observation: format!("{result:?}"),
                    },
                );
            } else {
                // Placement, interruption and submission are attempt evidence,
                // never a native USER receipt or a terminal Delivered outcome.
                let _ = self.store.mark_delivery_uncertain(&attempt.attempt_id);
                return match self.reconcile_delivery_configured(configured, &attempt) {
                    Reconciled::Settled(entry, _) => {
                        drained.push(entry);
                        Outcome::Drained(drained)
                    }
                    _ => Outcome::AwaitingReceipt(oldest.logical_delivery_id),
                };
            }
            return refused(drained, Failed_Data::HerdrRefused);
        }
        if let Some(adapter) = model {
            let wire_bytes = match crate::codex::queued_turn_wire_bytes(
                &node.session_id,
                &attempt.attempt_id,
                attempt.native_request_id,
                &text,
            ) {
                Ok(bytes) => bytes,
                Err(_) => return refused(drained, Failed_Data::CodexRefused),
            };
            let wire_limit = configured
                .snapshot
                .limits
                .frame
                .min(configured.snapshot.limits.app_server_message);
            if wire_bytes > usize::try_from(wire_limit).unwrap_or(0) {
                let size = signal_flow::SendPayloadSize {
                    actual_bytes: i64::try_from(wire_bytes).unwrap_or(i64::MAX),
                    maximum_bytes: wire_limit,
                };
                return match self.store.fail_transport_overlimit(&attempt.attempt_id, size) {
                    Ok(failed) => Outcome::Drained(failed),
                    Err(_) => refused(drained, Failed_Data::StoreRefused),
                };
            }
            let submitted = adapter.submit_queued_turn(
                &node.session_id,
                &attempt.attempt_id,
                attempt.native_request_id,
                &text,
            );
            if let Ok(acknowledgement) = &submitted {
                eprintln!(
                    "flow-nexus: logical {} attempt {} submitted as Codex turn {}; native USER receipt pending",
                    oldest.logical_delivery_id, attempt.attempt_id, acknowledgement.turn_id
                );
            } else if let Err(error) = &submitted {
                eprintln!(
                    "flow-nexus: logical {} attempt {} Codex submission uncertain: {error:?}",
                    oldest.logical_delivery_id, attempt.attempt_id
                );
            }
            if let Ok(receipt) = submitted {
                match self.store.settle_received_cohort(
                    &attempt.attempt_id,
                    NativeDeliveryReceipt {
                        native_request_id: receipt.native_request_id,
                        thread_id: receipt.thread_id,
                        turn_id: receipt.turn_id,
                        native_receipt: receipt.native_receipt,
                    },
                ) {
                    Ok(settled) => {
                        self.codex_turns.delivered(&node.session_id);
                        drained.extend(settled);
                        return Outcome::Drained(drained);
                    }
                    Err(error) => eprintln!(
                        "flow-nexus: exact Codex receipt for {} could not settle atomically: {error:?}",
                        attempt.attempt_id
                    ),
                }
            }

            // The prepared attempt retains the complete frozen cohort.  A
            // failed or ambiguous local transition is never submitted a
            // second time; native history reconciles that exact vector.
            let _ = self.store.mark_delivery_uncertain(&attempt.attempt_id);
            return Outcome::AwaitingReceipt(oldest.logical_delivery_id);
        } else if let Err(refusal) = self.hand_over(configured, &flow_id, &text) {

            return if absent(format!("{refusal:?}")) {

                refused(drained, Failed_Data::HerdrRefused)
            } else {
                refused(drained, Failed_Data::StoreRefused)
            };
        }
        let _ = self.store.mark_delivery_uncertain(&attempt.attempt_id);
        // The receipt may already be in the transcript; otherwise the
        // seat's UserPromptSubmit report, or the next reconciliation,
        // settles it. Typed once, it is never typed again.
        if let ClaudeQueuedHistory::Received(receipt) =
            self.claude_delivery_history_configured(configured, &attempt)
            && let Reconciled::Settled(entry, _) =
                self.settle_received(&attempt, receipt.turn_id, receipt.row, SeatAfter::InItsTurn)
        {
            drained.push(entry);
            return Outcome::Drained(drained);
        }
        Outcome::AwaitingReceipt(oldest.logical_delivery_id)
    }

    #[cfg(test)]
    fn drain_all(&self, address: Address) -> Outcome {
        let Ok(configured) = self.test_configured_use() else {
            return Outcome::Failed(Failed_Data::StoreRefused);
        };
        self.drain_all_configured(&configured, address)
    }

    fn drain_all_configured(&self, configured: &ConfiguredUse, address: Address) -> Outcome {
        let queued = match self.store.metaflow(&address) {
            Ok(Some(metaflow)) => metaflow.queue,
            Ok(None) => return Outcome::Failed(Failed_Data::NotAwake),
            Err(_) => return Outcome::Failed(Failed_Data::StoreRefused),
        };
        let mut removed = 0;
        let mut delivered = Vec::new();
        loop {
            let outcome = self.drain_configured(configured, address.clone());
            match outcome {
                Outcome::Drained(mut entries) => delivered.append(&mut entries),
                Outcome::AwaitingReceipt(id) => return Outcome::AwaitingReceipt(id),
                Outcome::WaitingForTurn(id) => return Outcome::WaitingForTurn(id),
                Outcome::Failed(reason) => return Outcome::Failed(reason),
                _ => {}
            }
            let remaining = match self.store.metaflow(&address) {
                Ok(Some(metaflow)) => metaflow.queue.len(),
                _ => return Outcome::Failed(Failed_Data::StoreRefused),
            };
            let now_removed = queued.len().saturating_sub(remaining);
            if now_removed == removed {
                return match self.store.unsettled_deliveries(&address) {
                    Ok(unsettled) if unsettled.is_empty() => Outcome::Drained(delivered),
                    Ok(_) => Outcome::Failed(Failed_Data::HerdrRefused),
                    Err(_) => Outcome::Failed(Failed_Data::StoreRefused),
                };
            }
            removed = now_removed;
            if remaining == 0 {
                return match self.store.unsettled_deliveries(&address) {
                    Ok(unsettled) if unsettled.is_empty() => Outcome::Drained(delivered),
                    Ok(_) => Outcome::Failed(Failed_Data::HerdrRefused),
                    Err(_) => Outcome::Failed(Failed_Data::StoreRefused),
                };
            }
        }
    }


}

impl WakesInParts for RunningNexus {
    fn waken(
        &self,
        configured: &crate::ConfiguredUse,
        address: &Address,
    ) -> Result<FlowId, WakeRefusal> {
        let metaflow = {
            let _turn = self
                .dispatch_gate
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            self.rest_ended_awake(configured, address);
            match self.store.metaflow(address) {
                Ok(Some(metaflow)) if metaflow.state == flow_ethos::State::Asleep => metaflow,
                Ok(_) => return Err(WakeRefusal::NotAsleep),
                Err(_) => return Err(WakeRefusal::StoreUnreadable),
            }
        };
        // A metaflow owed a successor wakes with nothing queued.
        let owed = matches!(self.store.successor_intent(address), Ok(Some(_)));
        if metaflow.queue.is_empty() && !owed {
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
            self.refuse_live_session(configured, &node, &sessions)?;
        }
        let WakePlan::Fresh(fresh) = plan else {
            return Err(WakeRefusal::Unplaceable);
        };
        let flow_id = match self.start_fresh(configured, &metaflow, fresh, "") {
            Ok(flow_id) => flow_id,
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
                    &queue,
                    signal_flow::RetryCause::WakeRefused(rejection.clone()),
                ) {
                    eprintln!(
                        "flow-nexus: the queue of {:?} {} {:?} is not marked WakeRefused: {error}",
                        address.aspect, address.topic, address.layer
                    );
                }
                return Err(WakeRefusal::StartRejected(rejection));
            }
        };
        let _turn = self
            .dispatch_gate
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        match self.perform_configured(
            configured,
            Operation::Record(Record_Data::Awoken(Record_Data_Awoken_Data {
                address: address.clone(),
                flow_id: flow_id.clone(),
                // Starting the seat delivers no queued request. The durable
                // queue is handed over through the ordinary drain path once
                // the seat is Awake.
                delivered: 0,
            })),
        ) {
            Outcome::Recorded => Ok(flow_id),
            _ => {
                eprintln!("flow-nexus: flow {flow_id} woke but is not recorded Awake");
                Err(WakeRefusal::AwakeNotRecorded)
            }
        }
    }

    fn start_fresh(
        &self,
        configured: &crate::ConfiguredUse,
        metaflow: &Metaflow,
        fresh: FreshContext,
        text: &str,
    ) -> Result<FlowId, FreshRefusal> {
        let address = &metaflow.address;
        // Model, effort and wording are one committed generation.
        let model = match configured
            .snapshot
            .configured_models
            .iter()
            .find(|model| model.layer == address.layer && model.aspect == address.aspect)
        {
            Some(model) => model,
            None => {
                return Err(FreshRefusal::Rejected(
                    signal_flow::StartRejection::CompositionRefused(
                        signal_flow::CompositionRefusal::Unconfigured,
                    ),
                ));
            }
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
            model_name: model.model_name.clone(),
            effort: model.effort.clone(),
            herdr_session_name: fresh.herdr_session_name,
            system_modules: fresh.system_modules,
            user_modules: fresh.user_modules,
            left_out_option: fresh.left_out_option,
            tool_set: fresh.tool_set,
            prompt: crate::composition::with_purpose_configured(
                &text,
                &fresh.purpose,
                &configured.snapshot.texts,
            ),
        };
        let profile = seat.fresh_profile();
        self.requirements_first_configured(configured, &profile)
            .map_err(FreshRefusal::Rejected)?;
        self.start_unbound_configured(configured, profile)
    }

    fn hand_over(
        &self,
        configured: &crate::ConfiguredUse,
        flow_id: &str,
        text: &str,
    ) -> Result<(), HandOverRefusal> {
        let handed = (|| {
            let target = self
                .delivery_target_with(flow_id, &configured.snapshot)
                .map_err(HandOverRefusal::Target)?;
            let _lease = self.pane_leases.hold(&target.route);
            let agent_state = self
                .writable_state_with(configured, &configured.herdr, &target, false, true)
                .map_err(HandOverRefusal::Pane)?;
            let observe = matches!(agent_state, AgentState::Idle | AgentState::Done);
            match configured.herdr.place(&target.route, text, observe) {
                Placement::Placed { observed: true }
                    if configured.herdr.route_is_available(&target.node) =>
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

    #[cfg(test)]
    fn record_context_at_stop(&self, flow_id: &str) {
        let Ok(configured) = self.test_configured_use() else {
            eprintln!("flow-nexus: flow {flow_id} stopped with configuration unreadable");
            return;
        };
        self.record_context_at_stop_configured(&configured, flow_id);
    }

    fn record_context_at_stop_configured(&self, configured: &crate::ConfiguredUse, flow_id: &str) {
        let Ok(Some(node)) = self.store.flow_node(flow_id) else {
            return;
        };
        if node.harness_kind != HarnessKind::Claude {
            return;
        }
        let Some(tokens) = configured
            .herdr
            .native_transcript_root(&HarnessKind::Claude, "")
            .ok()
            .and_then(|root| root.session_transcript(&node.session_id))
            .and_then(|transcript| {
                transcript.context_tokens(configured.snapshot.limits.context_tail as u64)
            })
        else {
            eprintln!("flow-nexus: flow {flow_id} stopped with no context size readable");
            return;
        };
        if self.perform_configured(
            configured,
            Operation::Record(Record_Data::Context(Record_Data_Context_Data {
                flow_id: flow_id.to_owned(),
                tokens,
            })),
        ) != Outcome::Recorded
        {
            eprintln!("flow-nexus: flow {flow_id} context size not recorded");
        }
    }

    fn refuse_live_session(
        &self,
        configured: &crate::ConfiguredUse,
        node: &FlowNode,
        sessions: &[String],
    ) -> Result<(), WakeRefusal> {
        let mut checked = HashSet::new();
        for herdr_session_name in sessions {
            if !checked.insert(herdr_session_name) {
                continue;
            }
            match configured
                .herdr
                .holds_native_session(herdr_session_name, node)
            {
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

    fn close_at_rest(
        &self,
        configured: &crate::ConfiguredUse,
        flow_id: &str,
        node: &FlowNode,
    ) -> Result<(), Box<Outcome>> {
        let HerdrRouteSelection::Available(route) = &node.herdr_route_selection else {
            return Err(Box::new(Outcome::Failed(Failed_Data::HerdrRefused)));
        };
        let closed = {
            let _lease = self.pane_leases.hold(route);
            self.perform_configured(configured, Operation::Close(node.clone()))
        };
        if closed != Outcome::Closed {
            return Err(Box::new(closed));
        }
        let recorded = self.perform_configured(
            configured,
            Operation::Record(Record_Data::Stopped(flow_id.to_owned())),
        );
        if recorded != Outcome::Recorded {
            return Err(Box::new(recorded));
        }
        Ok(())
    }

    fn refresh_successor_first_configured(
        &self,
        configured: &crate::ConfiguredUse,
        flow_id: &str,
        metaflow: &Metaflow,
        cause: SuccessionCause,
    ) -> AtStop {
        let address = metaflow.address.clone();
        // The successor's context is the metaflow's declaration, else what
        // the predecessor's own launch named.
        let mut lineage = metaflow.clone();
        lineage.past.retain(|past| past != flow_id);
        lineage.past.push(flow_id.to_owned());
        let WakePlan::Fresh(fresh) = self.wake_plan(&lineage) else {
            return AtStop::Kept(KeptAtStop::RefreshUnplaceable);
        };
        let model = match configured
            .snapshot
            .configured_models
            .iter()
            .find(|model| model.layer == address.layer && model.aspect == address.aspect)
        {
            Some(model) => model,
            None => return AtStop::Kept(KeptAtStop::RefreshUnplaceable),
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
        let seat = FreshSeat {
            address: address.clone(),
            launch_request_id: launch_request_id.clone(),
            model_name: model.model_name.clone(),
            effort: model.effort.clone(),
            herdr_session_name: fresh.herdr_session_name,
            system_modules: fresh.system_modules,
            user_modules: fresh.user_modules,
            left_out_option: fresh.left_out_option,
            tool_set: fresh.tool_set,
            prompt: crate::composition::with_purpose_configured(
                "",
                &fresh.purpose,
                &configured.snapshot.texts,
            ),
        };
        let mut profile = seat.fresh_profile();
        profile.flow_id_option = Some(flow_id.to_owned());
        let response = {
            let _turn = self
                .dispatch_gate
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            self.replace_configured(configured, profile, Origin::Unbound)
        };
        self.settle_owed(&address, &response);
        match response {
            Response::Replaced(_) => AtStop::Refreshed(address),
            response => AtStop::Kept(KeptAtStop::RefreshLaunchRefused(Box::new(response))),
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

    fn take_up_owed(&self, configured: &ConfiguredUse, intent: SuccessorIntent) -> AtStop {
        let address = intent.address.clone();
        let Some(lock) = self.waking.hold(&address) else {
            return AtStop::Kept(KeptAtStop::Waking);
        };
        // The owed flow is still current: the intent was read under the
        // gate, and the lock keeps every wake and refresh out since.
        let metaflow = match self.store.metaflow(&address) {
            Ok(Some(metaflow))
                if metaflow.state == flow_ethos::State::Awake(intent.predecessor.clone()) =>
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
                    profile.launch_request_id = crate::sending::MintsId::minted_id("replace");
                    if self
                        .store
                        .owe_successor(SuccessorIntent {
                            launch_request_id: profile.launch_request_id.clone(),
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
                    self.replace_configured(configured, profile, attempt.origin)
                };
                self.settle_owed(&address, &response);
                match response {
                    Response::Replaced(_) => AtStop::Refreshed(address),
                    response => AtStop::Kept(KeptAtStop::RefreshLaunchRefused(Box::new(response))),
                }
            }
            cause => self.refresh_successor_first_configured(
                configured,
                &intent.predecessor,
                &metaflow,
                cause,
            ),
        };
        drop(lock);
        self.wake_deferred(configured);
        taken
    }

    #[cfg(test)]
    fn settle_rested(&self, flow_id: &str) -> AtStop {
        let configured = match self.test_configured_use() {
            Ok(configured) => configured,
            Err(_) => return AtStop::Kept(KeptAtStop::StoreUnreadable),
        };
        self.settle_rested_configured(&configured, flow_id)
    }

    fn settle_rested_configured(&self, configured: &ConfiguredUse, flow_id: &str) -> AtStop {
        self.record_context_at_stop_configured(configured, flow_id);
        // A Codex seat's rest is its app-server's, and a Claude seat whose
        // transcript records its main turn's end_turn has ended its turn
        // whatever its screen shows (background agents keep Herdr at
        // Working). Otherwise Claude runs its Stop hook while its spinner is
        // still on screen, and Herdr reads its state from the screen: the
        // seat is given a bounded wait to show resting before the gate is
        // taken.
        if let Ok(Some(node)) = self.store.flow_node(flow_id)
            && let HerdrRouteSelection::Available(route) =
                &configured.herdr.refresh_route(node).herdr_route_selection
        {
            let node = self.store.flow_node(flow_id).ok().flatten();
            if !node.as_ref().is_some_and(|node| {
                node.harness_kind == HarnessKind::Codex
                    || self.claude_main_turn_ended_configured(configured, node)
            }) {
                configured.herdr.rested(route);
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
        let node = match self.store.flow_node(flow_id) {
            Ok(Some(node)) => {
                // A Flow-owned Claude's Stop lifecycle report may remove
                // Herdr's auto-detected agent before this synchronous hook
                // returns. Keep the persisted, launch-owned pane when its
                // exact selected process is still there; refreshing that
                // route through the managed-agent roster would erase the
                // only pane Flow is responsible for stopping.
                if node.harness_kind == HarnessKind::Claude
                    && configured.herdr.flow_owned_claude_session_is_running(&node)
                {
                    node
                } else {
                    configured.herdr.refresh_route(node)
                }
            }
            Ok(None) => return AtStop::Kept(KeptAtStop::UnknownFlow),
            Err(_) => return AtStop::Kept(KeptAtStop::StoreUnreadable),
        };
        // A seat past its model's refresh threshold is ended at its rest
        // rather than handed work; it is told nothing and writes no handover.
        let refresh_due = matches!(self.store.past_threshold(flow_id), Ok(true));
        let continuity = match self.store.continuity(&address) {
            Ok(continuity) => continuity,
            Err(_) => return AtStop::Kept(KeptAtStop::StoreUnreadable),
        };
        // Whether the harness may be closed now: a Claude seat only once
        // Herdr shows it resting, since a main turn that ended while its
        // background agents run shows Working and closing it would end them.
        let closable = match node.harness_kind {
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
                let pane_agent = configured.herdr.pane_agent(&node);
                let native_resting = matches!(
                    &pane_agent,
                    PaneAgent::Present { agent_state, .. } if Self::rests_at(agent_state)
                );
                // A Flow-owned Claude is intentionally outside Herdr's
                // managed-agent lifecycle. Its synchronous Stop hook is the
                // native turn-end event which reaches this path; the exact
                // selected session in the pane's actual foreground is the
                // process Flow owns and can now stop. Claude may flush the
                // final transcript row only after the hook returns, so that
                // file cannot be the hook's own readiness prerequisite.
                let flow_owned_resting =
                    configured.herdr.flow_owned_claude_session_is_running(&node);
                // Closing a seat ends whatever runs in it: one whose main
                // turn ended while its background agents run shows Working,
                // and is refreshed only once Herdr shows it resting. The
                // Flow-owned Stop boundary above is the equivalent proof.
                if refresh_due && !native_resting && !flow_owned_resting {
                    return AtStop::Kept(KeptAtStop::NotResting(pane_agent));
                }
                let claude_main_ready = self.claude_main_turn_ended_configured(configured, &node)
                    && (flow_owned_resting
                        || matches!(
                            self.delivery_target_with(flow_id, &configured.snapshot)
                                .ok()
                                .and_then(|target| self
                                    .writable_state_with(
                                        &configured,
                                        &configured.herdr,
                                        &target,
                                        false,
                                        true
                                    )
                                    .ok()),
                            Some(AgentState::Working | AgentState::Idle | AgentState::Done)
                        ));
                if !native_resting && !claude_main_ready {
                    return AtStop::Kept(KeptAtStop::NotResting(pane_agent));
                }
                native_resting || flow_owned_resting
            }
        };
        if refresh_due {
            let wakes = metaflow
                .queue
                .iter()
                .any(|entry| entry.delivery == signal_flow::Delivery::Wake);
            // A Maintained metaflow, or one holding work that wakes it, is
            // owed its successor, started before the predecessor rests.
            if continuity == Continuity::Maintained || wakes {
                let Some(refreshing) = self.waking.hold(&address) else {
                    return AtStop::Kept(KeptAtStop::Waking);
                };
                // Replace takes the gate for its own steps; the waking lease
                // keeps any wake or drain of the metaflow out meanwhile.
                drop(turn);
                let refreshed = self.refresh_successor_first_configured(
                    &configured,
                    flow_id,
                    &metaflow,
                    SuccessionCause::Refresh,
                );
                drop(refreshing);
                self.wake_deferred(configured);
                return refreshed;
            }
            if !closable {
                return AtStop::Kept(KeptAtStop::NotResting(configured.herdr.pane_agent(&node)));
            }
            if let HerdrRouteSelection::Unavailable = &node.herdr_route_selection {
                return AtStop::Kept(KeptAtStop::RefreshUnrouted);
            }
            return match self.close_at_rest(configured, flow_id, &node) {
                Ok(()) => AtStop::Refreshed(address),
                Err(outcome) => AtStop::Kept(KeptAtStop::RefreshRefused(outcome)),
            };
        }
        if !metaflow.queue.is_empty() {
            return match self.perform_configured(configured, Operation::Drain(address)) {
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
            Continuity::OnDemand => match self.close_at_rest(configured, flow_id, &node) {
                Ok(()) => AtStop::Slept(address),
                Err(outcome) => AtStop::Kept(KeptAtStop::SleepRefused(outcome)),
            },
        }
    }
}
