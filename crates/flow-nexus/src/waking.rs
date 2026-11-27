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
//! engine interrupts only for HardAbrupt. Soft work waits for the seat's
//! idle event, the turn end above, and only a waking request starts the
//! seat's next turn ([`HoldsSoftForIdle`]): an Order, Question, Task,
//! Living, Result or Done ([`crate::request_waking`]). Its hand-over carries
//! the whole queue, oldest first, the Soft Notices waiting in it included;
//! a queue of Soft Notices alone is not handed over, and they ride with the
//! next hand-over. A seat known resting by the same evidence has had its
//! idle event, and a waking Soft request is handed over at once. Held Soft
//! work is the metaflow's durable queue, so a Nexus that starts again holds
//! it as before.
//!
//! A seat is closed, refreshed or put to sleep, only once it rests: its
//! turn ended by the evidence above and no work it started in flight. A
//! Claude seat's work in flight is the background work its own last Stop
//! names; Claude wakes the seat when that work ends, into a turn whose Stop
//! is then its rest (`JudgesClosingRest`). Soft work due at the turn's end
//! is handed over whatever background work runs.
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
//! hold and starts and confirms the successor through Replace before the
//! ending flow is reaped, even when an OnDemand flow has no waking work.
//! A Launch addressed to an Awake metaflow is such a refresh, its successor
//! the Launch's, run under the same hold. A Send that arrives
//! while any of these holds the metaflow is queued on it.
//!
//! A metaflow is Asleep once its flow has ended: its seat slept, its pane
//! closed by the living, its harness crashed, or the flow replaced. Work queued for an
//! Awake seat whose pane Herdr shows gone records that flow Exited, which
//! puts its metaflow Asleep. A request queued for an Asleep metaflow wakes
//! it by its kind, whatever its priority ([`crate::request_waking`]): an
//! Order, Question, Task, Living, Result or Done does, a Notice waits for
//! the next waking, and a Reminder wakes by the mark it was queued with,
//! Wake when a flow asked for it, Queue when it reminds a Task's sender. The wake (`Operation::Wake`)
//! launches a fresh flow from the native model configured for its Model and
//! the modules its declaration binds it to, composed again
//! as any Start is, with the whole queue, oldest first, as its brief. The requests leave the queue only in the write that records
//! the started flow Awake, so a Nexus that dies mid-wake loses none: at its
//! next start it wakes every Asleep metaflow still holding work. Requests
//! that arrive while the wake runs are handed to the woken flow once it is
//! up, oldest first.
//!
//! The decision record names whether the seat rests.
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
use crate::claude::{LocatesClaudeTranscript, ReadsClaudeTurns};
use crate::composition::{FreshSeat, JoinsPurpose, RendersQueuedWork};
use crate::delivery::DeliversMessages;
use crate::delivery::lease::LeasesPanes;
use crate::generated::memory::RefreshCaller;
use crate::generated::operation::{
    Failed_Data, Operation, Outcome, Record_Data, Record_Data_Asleep_Data,
};
use crate::generated::overriding::HeedsOverrides;
use crate::generated::starter::ProfilesFreshSeat;
use crate::herdr::pane::PaneAgent;
use crate::herdr::{LocatesNativeTranscripts, ReadsHerdrPanes};
use crate::launching::LaunchesFlows;
use crate::performing::Performs;
use crate::request_waking::{HoldsWakingRequest, WakesSleepingPermaflow};
use crate::send_tool::ServesLaunchCalls;
use crate::store::NamesConfiguredModel;
use crate::store::address_hold::HoldsLaunchAddress;
use crate::store::context::JudgesThreshold;
use crate::store::current_seat::RecordsCurrentSeats;
use crate::store::effort_configuration::ReadsEffort;
use crate::store::events::RecordedHarnessEvent;
use crate::store::events::RecordsHarnessEvents;
use crate::store::events::{IdentifiesNativeStop, RecordedStop};
use crate::store::handover::HoldsHandovers;
use crate::store::logical_delivery::{
    HoldsAdmissionAnswers, RecordsLogicalDeliveries, RecordsQueueWrites, SimplifiedReplyBinding,
    SimplifiedReplyStanding,
};
use crate::store::metaflow::{PreparesDeclaredMetaflow, SleepsMetaflows};
use crate::store::metaflow_declaration::ReadsMetaflowDeclarations;
use crate::store::ordered_replacement::{
    NamesOrderedLaunch, OrderedLaunch, OrderedReplacement, OrdersReplacements, ReplacementOrder,
    ShowsWaitingAttempt,
};
use crate::store::pending_reap::RecordsPendingReaps;
use crate::store::queue_delivery::{AccountsQueueDelivery, RequestState};
use crate::store::successor::{OwesSuccessors, ReadsContinuity, SuccessionCause, SuccessorIntent};
use crate::store::{ManagesMetaflowState, ReadsFlowRows, ReadsLaunchAttempt};
use meta_signal_flow::Continuity;
use signal_flow::{
    Address, FlowId, FlowNode, HarnessKind, HerdrRouteSelection, LaunchProfile, Metaflow,
    NativePrompt, Origin, Output, QueueEntry,
};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Condvar, Mutex};

/// A request's priority, read for its hand-over.
pub trait ReadsPriority {
    /// Whether it is HardAbrupt or MiddleAbrupt.
    fn abrupt(&self) -> bool;
}

impl ReadsPriority for signal_flow::Message {
    fn abrupt(&self) -> bool {
        !matches!(self, signal_flow::Message::Soft(_))
    }
}

impl ReadsPriority for signal_flow::SendableMessage {
    fn abrupt(&self) -> bool {
        !matches!(self, signal_flow::SendableMessage::Soft(_))
    }
}

impl ReadsPriority for QueueEntry {
    fn abrupt(&self) -> bool {
        self.message.abrupt()
    }
}

/// When queued work is handed to an Awake seat. HardAbrupt and MiddleAbrupt
/// are handed over when they arrive, a Notice among them. Soft work waits
/// for the seat's idle event, its Stop or its thread's turn completed, and
/// only a waking request starts the seat's next turn: an Order, Question,
/// Task, Living, Result or Done, or a Reminder marked Wake
/// ([`crate::request_waking`]). A queue of Soft Notices alone is not handed
/// over; they ride with the next hand-over, which carries the whole queue,
/// oldest first. A seat whose last turn event already is its rest has had
/// that event, and is handed a waking Soft request when it arrives.
pub trait HoldsSoftForIdle {
    /// Whether the seat's last turn event is its rest: its last transition
    /// leads to Resting, an Idle its harness reported or an owner's
    /// SetState among them.
    fn rests_now(&self, flow_id: &str) -> bool;
    /// Whether `requests` start a turn of a resting seat: one of them is
    /// abrupt or wakes.
    fn due_at_rest<Queued: ReadsPriority + WakesSleepingPermaflow>(
        &self,
        requests: &[Queued],
    ) -> bool {
        requests
            .iter()
            .any(|request| request.abrupt() || request.wakes_sleeping_permaflow())
    }
    /// Whether `requests` are handed now to the seat Awake as `flow_id`:
    /// one of them is abrupt, or they are due at rest and the seat rests.
    fn due_now<Queued: ReadsPriority + WakesSleepingPermaflow>(
        &self,
        requests: &[Queued],
        flow_id: &str,
    ) -> bool;
}

impl HoldsSoftForIdle for RunningNexus {
    fn due_now<Queued: ReadsPriority + WakesSleepingPermaflow>(
        &self,
        requests: &[Queued],
        flow_id: &str,
    ) -> bool {
        use crate::generated::transitions::{AskedFlow, RecordsTransitions};
        match self.store.state_of(AskedFlow(FlowId(flow_id.to_owned()))) {
            Ok(Some(meta_signal_flow::State::Acting)) => requests.iter().any(ReadsPriority::abrupt),
            Ok(Some(meta_signal_flow::State::Resting)) => {
                requests.iter().any(ReadsPriority::abrupt) || self.due_at_rest(requests)
            }
            _ => false,
        }
    }
    // Only the authoritative current row establishes a rest.
    fn rests_now(&self, flow_id: &str) -> bool {
        use crate::generated::transitions::{AskedFlow, RecordsTransitions};
        matches!(
            self.store.state_of(AskedFlow(FlowId(flow_id.to_owned()))),
            Ok(Some(meta_signal_flow::State::Resting))
        )
    }
}

/// The metaflows a wake is running for. Held for the whole wake and its
/// drain; released under the dispatch gate once the queue is seen empty.
#[derive(Clone, Debug, Default)]
pub struct WakingMetaflows {
    waking: Arc<Mutex<HashSet<Address>>>,
    effects: Arc<Mutex<HashMap<Address, usize>>>,
    released: Arc<Condvar>,
    manual_control: Arc<Mutex<()>>,
}

/// One running wake's hold on its metaflow.
pub struct WakingLease {
    held: WakingMetaflows,
    purpose: crate::generated::overriding::AddressLease,
    address: Address,
    /// The priority a Push delivers the queue at, in place of the
    /// priority its own requests carry.
    pushed: Option<signal_flow::Priority>,
}

/// A hold whose drain a Push sets the priority of.
pub(crate) trait PushesAtPriority: Sized {
    /// This hold, delivering at `priority`.
    fn pushed_at(self, priority: signal_flow::Priority) -> Self;
    /// The priority a Push set, if one did.
    fn pushed(&self) -> Option<&signal_flow::Priority>;
}

impl PushesAtPriority for WakingLease {
    fn pushed_at(mut self, priority: signal_flow::Priority) -> Self {
        self.pushed = Some(priority);
        self
    }

    fn pushed(&self) -> Option<&signal_flow::Priority> {
        self.pushed.as_ref()
    }
}

pub(crate) trait IdentifiesWakingLease {
    fn address(&self) -> &Address;
    fn owned_by(&self, holder: &WakingMetaflows) -> bool;
}

impl IdentifiesWakingLease for WakingLease {
    fn address(&self) -> &Address {
        &self.address
    }

    fn owned_by(&self, holder: &WakingMetaflows) -> bool {
        Arc::ptr_eq(&self.held.effects, &holder.effects)
    }
}

pub trait HoldsWakingMetaflows {
    /// Holds the metaflow for a wake; None when a wake already holds it.
    fn hold(&self, address: &Address) -> Option<WakingLease>;
    fn is_waking(&self, address: &Address) -> bool;
}

impl HoldsWakingMetaflows for WakingMetaflows {
    fn hold(&self, address: &Address) -> Option<WakingLease> {
        let mut waking = self
            .waking
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        waking.insert(address.clone()).then(|| {
            use crate::generated::overriding::{AddressLease, SerializesPermaflowEffects};
            let mut lease = self.effect(address.clone());
            lease.purpose = AddressLease::Wake;
            lease
        })
    }

    fn is_waking(&self, address: &Address) -> bool {
        self.waking
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .contains(address)
    }
}

impl Drop for WakingLease {
    fn drop(&mut self) {
        use crate::generated::overriding::AddressLease;
        if self.purpose == AddressLease::Wake {
            self.held
                .waking
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .remove(&self.address);
        }
        let mut effects = self
            .held
            .effects
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(count) = effects.get_mut(&self.address) {
            *count -= 1;
            if *count == 0 {
                effects.remove(&self.address);
            }
        }
        self.held.released.notify_all();
    }
}

impl crate::generated::overriding::SerializesPermaflowEffects for WakingMetaflows {
    fn effect<N: crate::generated::overriding::Addressed>(&self, input: N) -> WakingLease {
        let address = input.address();
        let mut effects = self
            .effects
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *effects.entry(address.clone()).or_default() += 1;
        WakingLease {
            held: self.clone(),
            address,
            purpose: crate::generated::overriding::AddressLease::Effect,
            pushed: None,
        }
    }
}

impl crate::generated::overriding::SerializesPermaflowHolds for RunningNexus {
    fn establish_permaflow_hold<N: crate::generated::overriding::Addressed>(
        &self,
        input: N,
    ) -> Result<bool, crate::store::StoreError> {
        use crate::generated::overriding::RemembersOverrides;
        let address = input.address();
        let _control = self
            .waking
            .manual_control
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let turn = self
            .dispatch_gate
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let mut effects = self
            .waking
            .effects
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        // Materialize a declared, never-started permaflow for Inspect without
        // creating a harness or an owed successor.
        self.store.prepare_declared_metaflow(&address)?;
        let moved = !self.store.permaflow_held(address.clone())?;
        if self.perform(Operation::Record(Record_Data::PermaflowHeld(
            address.clone(),
        ))) != Outcome::Recorded
        {
            return Err(crate::store::StoreError::StateInvariant);
        }
        drop(turn);
        // New leases observe the durable hold before their first effect.
        // Existing leases finish before the acknowledgment leaves.
        while effects.contains_key(&address) {
            effects = self
                .waking
                .released
                .wait(effects)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
        }
        Ok(moved)
    }

    fn remove_permaflow_hold<N: crate::generated::overriding::Addressed>(
        &self,
        input: N,
    ) -> Result<bool, crate::store::StoreError> {
        use crate::generated::overriding::RemembersOverrides;
        let address = input.address();
        let _control = self
            .waking
            .manual_control
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let _turn = self
            .dispatch_gate
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let _effects = self
            .waking
            .effects
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let moved = self.store.permaflow_held(address.clone())?;
        if self.perform(Operation::Record(Record_Data::PermaflowReleased(address)))
            != Outcome::Recorded
        {
            return Err(crate::store::StoreError::StateInvariant);
        }
        Ok(moved)
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
        let ended = matches!(
            self.store.current_seat(&flow_id.0),
            Ok(crate::store::current_seat::CurrentSeatRead::Known(
                crate::store::current_seat::KnownSeat {
                    seat_standing: crate::generated::memory::SeatStanding::Ended,
                    ..
                }
            ))
        );
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
        if self.rests_now(flow_id) {
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
    /// metaflow's declaration binds it to.
    Fresh(FreshContext),
    /// The Address is not declared, or no Herdr session is configured: it
    /// is never woken.
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
    /// A restart ordered while the seat worked was carried out at this
    /// idle event: its harness closed, its own session resumed and bound.
    Restarted(Address),
    /// The seat was due a refresh and its successor started and is
    /// confirmed: the Address receives as the successor now, and the
    /// predecessor's reap is owed, settled on the events that follow.
    ReapPending(Box<signal_flow::ReapPending>),
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
    /// The native rest belongs to a session the flow no longer holds.
    OtherSession,
    /// After the bounded wait, Herdr does not show the seat resting.
    NotResting(PaneAgent),
    /// A replacement ordered from outside was carried out at the seat's
    /// rest and refused, with the refusal; the order is spent.
    OrderedRefused(Box<Output>),
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
    RefreshLaunchRefused(Box<Output>),
    /// An OnDemand seat with nothing queued could not be closed: the close
    /// or its Stopped record was refused; it stays Awake.
    SleepRefused(Box<Outcome>),
    /// An OnDemand seat's successor did not compose, so the seat is not
    /// closed: a closed seat whose successor cannot compose would leave its
    /// Address with no flow. It stays Awake and is handed its queue.
    SuccessorUncomposable(Box<Outcome>),
    /// A restart holds the flow: nothing is settled at a Stop of it.
    Restarting,
    /// An OnDemand seat with nothing queued was not closed: no declaration
    /// or launch names the context its next wake would start. Only its
    /// sleep is declined; it stays Awake.
    SleepUnplaceable,
    /// An OnDemand seat with nothing queued was not closed: the successor
    /// its next wake would start does not compose. Only its sleep is
    /// declined; it stays Awake.
    SleepUncomposable(Box<Outcome>),
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
    /// Notes the refusal and returns whether it is newly standing.
    fn note(&self, flow_id: &str) -> bool;
    /// Whether the flow's successor stands refused.
    fn holds(&self, flow_id: &str) -> bool;
    /// Forgets the flow: its successor stands, or it has ended.
    fn clear(&self, flow_id: &str);
}

impl NotesRefreshRefusals for RefreshRefusals {
    fn note(&self, flow_id: &str) -> bool {
        self.flows
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(flow_id.to_owned())
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
    /// A caller's durable reply is still being written.
    AdmissionReplyPending,
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
}

/// What a wake refusal answers on the wire.
pub trait AnswersWakeRefusal {
    /// The operation's answer.
    fn failure(&self) -> Failed_Data;
}

impl AnswersWakeRefusal for WakeRefusal {
    fn failure(&self) -> Failed_Data {
        match self {
            Self::StoreUnreadable => Failed_Data::StoreRefused,
            Self::NotAsleep => Failed_Data::NotAsleep,
            Self::NothingQueued | Self::AdmissionReplyPending => Failed_Data::NothingQueued,
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
    /// A native rest retries the attached session's pending queue. A kept
    /// seat leaves that queue to its next native rest, with no retry clock.
    fn settle_native_rest(&self, flow_id: &str, session: &str) -> AtStop;
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
    fn has_due_work(&self, metaflow: &Metaflow) -> bool;
    /// Hands over newly due work and releases the Address lease under the
    /// gate when no more work is due.
    fn drain_due_held(&self, address: &Address, lease: WakingLease);
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
    /// Hands over as many queued requests as can be handed now, oldest
    /// first. It stops at any refusal; the work it did not hand over stays
    /// queued. The caller holds the dispatch gate.
    fn drain_all(&self, address: Address) -> Outcome;
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
            Output::Started(launched) => Ok(launched.flow_id),
            // The flow started and receives; only its handover is owed.
            Output::HandoverPending(pending) => Ok(pending.launched.flow_id),
            Output::StartRejected(rejection) => Err(FreshRefusal::Rejected(rejection)),
            _ => Err(FreshRefusal::Failed(Failed_Data::Unstarted)),
        }
    }
}

/// The steps a wake takes.
pub(crate) trait WakesInParts {
    /// The body of `wake`, every early exit a typed refusal.
    fn waken(&self, address: &Address) -> Result<FlowId, WakeRefusal>;
    fn start_fresh(
        &self,
        metaflow: &Metaflow,
        fresh: FreshContext,
        text: &str,
        startup: &[QueueEntry],
    ) -> Result<FlowId, FreshRefusal>;
    fn last_launch_profile(&self, flow_id: &str) -> Option<LaunchProfile>;
    /// Writes the context size the flow's native transcript shows now, the
    /// size a Compact of the flow is judged by.
    /// Refuses a wake while a pane already runs this flow's native session:
    /// SessionLive when one does, HerdrUnreadable when Herdr is unreadable.
    fn refuse_live_session(&self, node: &FlowNode, sessions: &[String]) -> Result<(), WakeRefusal>;
    /// The body of `settle_stopped`: waits, boundedly and off the dispatch
    /// gate, for Herdr to show the seat resting, then drains it.
    fn settle_rested(&self, flow_id: &str, session: &str) -> AtStop;
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
    fn settle_owed(&self, address: &Address, response: &Output);
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
    /// queued now, as its rest's drain would. The first time a refusal
    /// stands, the investigator receives the fault, and a Claude seat past
    /// its threshold is compacted.
    /// Takes the dispatch gate; the caller holds none.
    fn successor_refused(&self, flow_id: &str, reason: &KeptAtStop);
}

pub(crate) trait ReadsClaudeDelivery {
    fn claude_transcript(&self, node: &FlowNode) -> Option<std::path::PathBuf>;
}

impl ReadsClaudeDelivery for RunningNexus {
    fn claude_transcript(&self, node: &FlowNode) -> Option<std::path::PathBuf> {
        self.herdr
            .native_transcript_root(&HarnessKind::Claude, "")
            .ok()?
            .session_transcript(&node.session_id.0)
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
                    input: binding.input_delivery_id.0,
                };
            }
            Ok(SimplifiedReplyStanding::Received(binding, receipt)) => (binding, receipt),
            Err(_) => return SimplifiedAnswer::StoreRefused,
        };
        let input = binding.input_delivery_id.0.clone();
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
    fn correlated_stop(&self, flow_id: &str, stop: &impl IdentifiesNativeStop) -> bool;
    /// A prompt the flow's own Claude session took.
    fn correlated_prompt(&self, flow_id: &str, prompt: &NativePrompt) -> bool;
    /// Of the flow's own Stops and submitted prompts, the last recorded is
    /// a Stop: a turn end landed with no prompt taken after it.
    fn turn_end_recorded(&self, flow_id: &str) -> bool;
    /// That last Stop, when the last of the flow's own Stops and submitted
    /// prompts is a Stop.
    fn last_turn_end(&self, flow_id: &str) -> Option<RecordedStop>;
}

impl CorrelatesNativeEvents for RunningNexus {
    fn correlated_stop(&self, flow_id: &str, stop: &impl IdentifiesNativeStop) -> bool {
        let Ok(Some(node)) = self.store.flow_node(flow_id) else {
            return false;
        };
        let (Some(session), Some(reported)) = (
            stop.native_session().map(|value| value.0.as_str()),
            stop.transcript_path().map(|value| value.0.as_str()),
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

    fn last_turn_end(&self, flow_id: &str) -> Option<RecordedStop> {
        let Ok(Some(events)) = self.store.events(flow_id) else {
            return None;
        };
        events
            .into_iter()
            .rev()
            .find_map(|event| match event {
                RecordedHarnessEvent::Stopped(stop) => {
                    self.correlated_stop(flow_id, &stop).then_some(Some(stop))
                }
                RecordedHarnessEvent::PromptSubmitted(prompt) => {
                    self.correlated_prompt(flow_id, &prompt).then_some(None)
                }
                RecordedHarnessEvent::Started(_)
                | RecordedHarnessEvent::ToolUsed(_)
                | RecordedHarnessEvent::Ended(_)
                | RecordedHarnessEvent::Cleared(_) => None,
            })
            .flatten()
    }
}

/// A known live resting seat may close.
pub(crate) trait JudgesClosingRest {
    fn rests_for_closing(&self, flow_id: &str) -> bool;
}

impl JudgesClosingRest for RunningNexus {
    fn rests_for_closing(&self, flow_id: &str) -> bool {
        use crate::generated::memory::{SeatActivity, SeatStanding};
        use crate::store::current_seat::CurrentSeatRead;
        matches!(self.store.current_seat(flow_id), Ok(CurrentSeatRead::Known(row))
            if row.seat_standing == SeatStanding::Alive(SeatActivity::Resting) && row.background_work.0.is_empty())
    }
}

trait ClassifiesSuccessorCause {
    fn is_automatic_refresh(&self) -> bool;
}

impl ClassifiesSuccessorCause for SuccessionCause {
    fn is_automatic_refresh(&self) -> bool {
        matches!(
            self,
            Self::Refresh | Self::EmergencyRefreshWithoutHandover(_)
        )
    }
}

pub(crate) trait ReadsRestedRefresh {
    fn refresh_standing(
        &self,
        flow_id: &str,
        address: &Address,
        warned: bool,
    ) -> Option<(meta_signal_flow::RefreshMode, bool)>;
    fn refresh_due_on_route(&self, flow_id: &str, address: &Address) -> bool;
}
impl ReadsRestedRefresh for RunningNexus {
    fn refresh_due_on_route(&self, flow_id: &str, address: &Address) -> bool {
        use crate::store::handover_reminders::SchedulesHandoverReminders;
        if !self.rests_now(flow_id) {
            return false;
        }
        if matches!(self.store.ordered_replacement(address),Ok(Some(order)) if order.predecessor.0==flow_id)
        {
            return true;
        }
        let Ok(warned) = self.store.warned(address, flow_id) else {
            return false;
        };
        self.refresh_standing(flow_id, address, warned)
            .is_some_and(|(_, due)| due)
    }
    fn refresh_standing(
        &self,
        flow_id: &str,
        address: &Address,
        warned: bool,
    ) -> Option<(meta_signal_flow::RefreshMode, bool)> {
        use crate::store::context::NamesFlowModel;
        use crate::store::context_warnings::ReadsContextWarningConfiguration;
        let warning_policy_configured = match self.store.flow_model(flow_id) {
            Ok(Some(model)) => match self.store.context_warning_policy(&model) {
                Ok(policy) => policy.is_some(),
                Err(_) => return None,
            },
            Ok(None) => false,
            Err(_) => return None,
        };
        let refresh_mode = match self.store.refresh_mode(address) {
            Ok(mode) => mode,
            Err(_) => return None,
        };
        let refresh_due = if refresh_mode == meta_signal_flow::RefreshMode::Manual {
            false
        } else {
            match self.store.threshold_standing(flow_id) {
                Ok(crate::store::context::ThresholdStanding::Past { .. }) => true,
                Ok(crate::store::context::ThresholdStanding::Below { refresh, .. }) => {
                    use crate::protecting::ProtectsFlows;
                    let warned = !warning_policy_configured
                        && (warned
                            || match self.store.handover_green(address, flow_id, refresh) {
                                Ok(green) => green,
                                Err(_) => return None,
                            });
                    warned
                        && match self.store.protected_address(flow_id) {
                            Ok(protected) => protected.is_none(),
                            Err(_) => return None,
                        }
                }
                Ok(_) => false,
                Err(_) => return None,
            }
        };
        Some((refresh_mode, refresh_due))
    }
}

impl SleepsAndWakesSeats for RunningNexus {
    fn settle_stopped(&self, flow_id: &str) -> AtStop {
        match self.store.flow_node(flow_id) {
            Ok(Some(node)) => self.settle_native_rest(flow_id, &node.session_id.0),
            Ok(None) => AtStop::Kept(KeptAtStop::NotAwake),
            Err(_) => AtStop::Kept(KeptAtStop::StoreUnreadable),
        }
    }

    fn settle_native_rest(&self, flow_id: &str, session: &str) -> AtStop {
        use crate::generated::overriding::SerializesPermaflowEffects;
        let _effect = match self.store.metaflow_awake_as(flow_id) {
            Ok(Some(metaflow)) => {
                let effect = self.waking.effect(metaflow.address.clone());
                if self.permaflow_held(metaflow.address) {
                    return AtStop::Idle;
                }
                Some(effect)
            }
            Ok(None) => None,
            Err(_) => return AtStop::Idle,
        };

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
        // A held Nexus does not act on its own: the seat is neither handed
        // work, put to sleep nor refreshed at its rest. Resume takes up what
        // is owed.
        if self.held() {
            eprintln!("flow-nexus: flow {flow_id} kept at its Stop: the Nexus is held");
            return AtStop::Idle;
        }
        let at_stop = self.settle_rested(flow_id, session);
        match &at_stop {
            AtStop::Kept(reason) if reason.successor_refused() => {
                eprintln!("flow-nexus: flow {flow_id} kept at its Stop: {reason:?}");
                self.successor_refused(flow_id, reason);
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
            AtStop::Restarted(_) => {
                eprintln!("flow-nexus: flow {flow_id} restarted at its idle event");
            }
            // The successor runs and receives; what queued meanwhile is
            // handed to it.
            AtStop::ReapPending(pending) => {
                eprintln!(
                    "flow-nexus: flow {flow_id} refreshed at its rest; its successor {} runs; its reap is {:?}",
                    pending.launched.flow_id.0, pending.reap_standing
                );
                self.refresh_refusals.clear(flow_id);
                self.wake_due(pending.address.clone());
            }
            AtStop::Idle | AtStop::Drained(_) => {}
        }
        // The recipient of an answer queued at this rest is woken off the
        // gate.
        self.wake_deferred();
        at_stop
    }

    fn drain_awake(&self, address: &Address) {
        use crate::generated::overriding::SerializesPermaflowEffects;
        let _effect = self.waking.effect(address.clone());
        if self.permaflow_held(address.clone()) {
            return;
        }

        // A held Nexus delivers nothing on its own.
        if self.held() {
            return;
        }
        let lease =
            {
                let _turn = self
                    .dispatch_gate
                    .lock()
                    .unwrap_or_else(|error| error.into_inner());
                let Ok(Some(metaflow)) = self.store.metaflow(address) else {
                    return;
                };
                let flow_ethos::State::Awake(id) = metaflow.state else {
                    return;
                };
                if !self.due_now(&metaflow.queue.0, &id.0)
                    || metaflow.queue.0.first().is_some_and(|entry| {
                        self.store.pane_input_waits_for_reply(&entry.request_id)
                    })
                    || !matches!(self.store.address_hold(&id.0), Ok(None))
                    || !matches!(
                        crate::store::restart::RecordsRestarts::restart_record(&*self.store, &id.0),
                        Ok(None)
                    )
                {
                    return;
                }
                let Some(lease) = self.waking.hold(address) else {
                    return;
                };
                lease
            };
        self.drain_due_held(address, lease);
        self.wake_deferred();
    }

    fn wake_due(&self, address: Address) {
        use crate::generated::overriding::SerializesPermaflowEffects;
        let _effect = self.waking.effect(address.clone());
        if self.permaflow_held(address.clone()) {
            return;
        }

        // A held Nexus delivers and launches nothing on its own.
        if self.held() {
            return;
        }
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
            if !matches!(self.store.stored_metaflow_lock(&address), Ok(None)) {
                return;
            }
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
            self.drain_due_held(&address, lease);
        }
        self.settle_stops_at_confirmation();
    }

    fn drain_due_held(&self, address: &Address, lease: WakingLease) {
        loop {
            let _turn = self
                .dispatch_gate
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            // What queued while the previous native operation ran waits
            // until it is due for this seat.
            let due = match self.store.metaflow(address) {
                Ok(Some(Metaflow {
                    state: flow_ethos::State::Awake(flow_id),
                    queue,
                    ..
                })) => self.due_now(&queue.0, &flow_id.0),
                _ => false,
            };
            if !due {
                drop(lease);
                return;
            }
            drop(_turn);
            match self.drain_held(address.clone(), &lease) {
                Outcome::Drained(typed) if typed.is_empty() => {
                    drop(lease);
                    return;
                }
                Outcome::Drained(_) => {}
                _outcome => {
                    eprintln!("flow-nexus: work not handed over stays queued");
                    drop(lease);
                    return;
                }
            }
        }
    }

    fn wake_plan(&self, metaflow: &Metaflow) -> WakePlan {
        use crate::store::metaflow_declaration::ReadsMetaflowDeclarations;
        // Every launch opens in the one configured session.
        let session = {
            use crate::launching::NamesLaunchSession;
            match self.launch_session() {
                Ok(session) => session,
                Err(_) => return WakePlan::Unplaceable,
            }
        };
        // A metaflow wakes with the modules its declaration binds it to; an
        // undeclared Address is never woken.
        match self.store.metaflow_declaration(&metaflow.address) {
            Ok(Some(declared)) => WakePlan::Fresh(FreshContext {
                herdr_session_name: session,
                flow_kind: declared.flow_kind,
                tool_set: declared.tool_set,
                system_modules: declared.system_modules,
                prompt_modules: declared.prompt_modules,
                leave_outs_option: None,
                purpose: crate::generated::starter::Purpose(declared.purpose.0),
            }),
            _ => WakePlan::Unplaceable,
        }
    }

    fn wake_is_due(&self, metaflow: &Metaflow) -> bool {
        use crate::store::metaflow::{ProvesWakeOrigin, WakeOrigin};
        if !matches!(
            self.store.wake_origin(metaflow),
            Ok(WakeOrigin::FirstSeat | WakeOrigin::EndedPredecessor(_))
        ) {
            return false;
        }
        self.has_due_work(metaflow)
    }

    fn has_due_work(&self, metaflow: &Metaflow) -> bool {
        if self.permaflow_held(metaflow.address.clone()) {
            return false;
        }
        if metaflow
            .queue
            .0
            .iter()
            .any(|entry| self.store.pane_input_waits_for_reply(&entry.request_id))
        {
            return false;
        }

        metaflow.state == flow_ethos::State::Asleep
            && (metaflow.queue.0.holds_waking_request()
                || self
                    .store
                    .successor_intent(&metaflow.address)
                    .ok()
                    .flatten()
                    .is_some_and(|intent| {
                        !intent.succession_cause.is_automatic_refresh()
                            || matches!(
                                self.store.refresh_mode(&metaflow.address),
                                Ok(meta_signal_flow::RefreshMode::Automatic)
                            )
                    }))
    }

    fn recover_wakes(&self) {
        // A Nexus held when it starts reconciles what its store holds and
        // acts on none of it: no restart, successor, drain or wake is taken
        // up until Resume.
        if self.held() {
            crate::reaping::TakesUpObligations::recover_obligations(self);
            eprintln!("flow-nexus: the Nexus is held; what it owes is taken up at Resume");
            return;
        }
        // A seat whose own turn end was recorded and which holds work is
        // settled as its Stop would settle it.
        let rested = {
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
                        && self.rests_now(&flow_id.0)
                })
                .collect::<Vec<_>>()
        };
        for flow_id in rested {
            self.settle_stopped(&flow_id.0);
        }
        {
            use crate::generated::overriding::{ParksHeldLaunches, ResumesHeldLaunches};
            let addresses = self
                .store
                .held_launches()
                .unwrap_or_default()
                .into_iter()
                .filter_map(|marker| {
                    self.store
                        .launch_attempt(&marker.launch_request_id.0)
                        .ok()
                        .flatten()
                })
                .map(|attempt| attempt.launch_profile.address)
                .collect::<HashSet<_>>();
            for address in addresses {
                self.resume_held_launches(address);
            }
        }
        // A restart an earlier Nexus left is taken up first, so a seat it
        // binds is handed its queue below.
        crate::restarting::RestartsFlows::recover_restarts(self);
        // What a launch owed when the last Nexus stopped is written now, in
        // the store alone: a derived reap's W3, a confirmed fresh launch's
        // settlement. A pane still there is the reap worker's.
        crate::reaping::TakesUpObligations::recover_obligations(self);
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
                if intent.succession_cause.is_automatic_refresh() {
                    match self.store.refresh_mode(&intent.address) {
                        Ok(meta_signal_flow::RefreshMode::Automatic) => {}
                        Ok(meta_signal_flow::RefreshMode::Manual) => {
                            let _ = self.store.forget_successor(&intent.address);
                            continue;
                        }
                        Err(_) => continue,
                    }
                }
                match self.store.metaflow(&intent.address) {
                    Ok(Some(metaflow)) => match metaflow.state {
                        flow_ethos::State::Awake(current) if current.0 == intent.predecessor.0 => {
                            match intent.succession_cause {
                                // An emergency refresh is a threshold
                                // refresh: its predecessor is refreshed at
                                // its rest, as a plain one is.
                                SuccessionCause::Refresh
                                | SuccessionCause::EmergencyRefreshWithoutHandover(_)
                                | SuccessionCause::Exit => {
                                    refreshes.push(current);
                                }
                                SuccessionCause::Ordered(_) | SuccessionCause::Replace => {
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
            let predecessor = intent.predecessor.0.clone();
            match self.take_up_owed(intent) {
                AtStop::Refreshed(address) => {
                    eprintln!(
                        "flow-nexus: flow {predecessor} replaced by the successor it was owed"
                    );
                    // What queued meanwhile is handed to the successor.
                    self.wake_due(address);
                }
                AtStop::ReapPending(pending) => {
                    eprintln!(
                        "flow-nexus: flow {predecessor} replaced by the successor it was owed; its reap is {:?}",
                        pending.reap_standing
                    );
                    self.wake_due(pending.address.clone());
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

/// An ordered refresh owes a successor independently of continuity or queue.
pub trait RefreshesMetaflows {
    /// Takes the same hold as a wake, so incoming work queues. The current
    /// flow is only reaped after Replace confirms its successor. The queue
    /// belongs to that successor once the hold is released.
    ///
    /// A caller that is a flow of the Address refreshes only itself: while
    /// the Address is no longer Awake as that flow, the refresh is refused
    /// CallerNotCurrent, naming it and the flow the Address is Awake as.
    /// Any other caller refreshes the flow the Address is Awake as. Every
    /// refresh is logged with its caller.
    fn refresh(&self, address: &Address, caller: &RefreshCaller) -> Output;
    /// A Replace, as a Launch addressed to an Awake metaflow names one: a
    /// refresh whose successor is the profile's. Under the metaflow's lock
    /// the successor is started and confirmed, then the flow it replaces is
    /// reaped, given no turn; once the lock is released the queue is handed
    /// to the successor, oldest first. A metaflow another holds is refused
    /// Locked. Runs off the dispatch gate, taking it for the replacement.
    fn replace_held(&self, profile: LaunchProfile, origin: Origin) -> Output;
}

impl RefreshesMetaflows for RunningNexus {
    fn refresh(&self, address: &Address, caller: &RefreshCaller) -> Output {
        use crate::store::metaflow_declaration::ReadsMetaflowDeclarations;
        use signal_flow::{CallerNotCurrent_Data, RefreshRejection, Refusal, Unknown_Data};
        let turn = self
            .dispatch_gate
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        eprintln!(
            "flow-nexus: Refresh of {address:?} called by {caller:?}",
            address = address
        );
        // A flow of the Address refreshes only itself: a flow its Address
        // no longer runs as is refused, whatever else holds the Address.
        if let RefreshCaller::AddressFlow(own) = caller {
            let current = match self.store.metaflow(address) {
                Ok(Some(Metaflow {
                    state: flow_ethos::State::Awake(current),
                    ..
                })) => Some(current),
                Ok(_) => None,
                Err(error) => return Output::Refused(Refusal::Store(error.to_string())),
            };
            if current.as_ref() != Some(own) {
                return Output::RefreshRejected(RefreshRejection::CallerNotCurrent(
                    CallerNotCurrent_Data {
                        flow_id: own.clone(),
                        flow_id_option: current,
                    },
                ));
            }
        }
        let Some(lock) = self.waking.hold(address) else {
            return Output::RefreshRejected(RefreshRejection::Locked);
        };
        self.rest_ended_awake(address);
        let metaflow = match self.store.metaflow(address) {
            Ok(Some(
                metaflow @ Metaflow {
                    state: flow_ethos::State::Awake(_),
                    ..
                },
            )) => metaflow,
            Ok(Some(_)) => return Output::RefreshRejected(RefreshRejection::NotAwake),
            Ok(None) => {
                return match self.store.metaflow_declaration(address) {
                    Ok(Some(_)) => Output::RefreshRejected(RefreshRejection::NotAwake),
                    Ok(None) => {
                        Output::Refused(Refusal::Unknown(Unknown_Data::Address(address.clone())))
                    }
                    Err(error) => Output::Refused(Refusal::Store(error.to_string())),
                };
            }
            Err(error) => return Output::Refused(Refusal::Store(error.to_string())),
        };
        // A restart holds the flow's Address as a refresh would.
        if let flow_ethos::State::Awake(current) = &metaflow.state
            && !matches!(
                crate::store::restart::RecordsRestarts::restart_record(&*self.store, &current.0),
                Ok(None)
            )
        {
            return Output::RefreshRejected(RefreshRejection::Locked);
        }
        let flow_ethos::State::Awake(flow_id) = metaflow.state.clone() else {
            return Output::RefreshRejected(RefreshRejection::NotAwake);
        };
        // A flow whose launch is not yet confirmed only holds its Address:
        // the launch that holds it is the owed successor's, still pending
        // or ambiguous, or a waking still running.
        match self.store.address_hold(&flow_id.0) {
            Ok(None) => {}
            Ok(Some(_)) => {
                return Output::RefreshRejected(match self.store.successor_intent(address) {
                    Ok(Some(_)) => RefreshRejection::SuccessorUnconfirmed,
                    Ok(None) => RefreshRejection::Locked,
                    Err(error) => return Output::Refused(Refusal::Store(error.to_string())),
                });
            }
            Err(error) => return Output::Refused(Refusal::Store(error.to_string())),
        }
        match self.store.flow_node(&flow_id.0) {
            Ok(Some(_)) => {}
            Ok(None) => {
                return Output::Refused(Refusal::Unknown(Unknown_Data::FlowId(flow_id)));
            }
            Err(error) => return Output::Refused(Refusal::Store(error.to_string())),
        };
        use crate::generated::memory::SeatStanding;
        use crate::store::current_seat::SeatDecision;
        match self.store.seat_decision(&flow_id.0) {
            Ok(SeatDecision::Observed(SeatStanding::Alive(_))) => {}
            Ok(SeatDecision::Observed(SeatStanding::Ended)) => {
                return Output::RefreshRejected(RefreshRejection::NotAwake);
            }
            Ok(SeatDecision::NoRecord) => return self.queue_refresh(address, &flow_id.0, caller),
            Err(error) => return Output::Refused(Refusal::Store(error.to_string())),
        }
        // A refresh never closes a working flow, whoever asks: one that does
        // not rest now is refreshed at its idle event.
        if self.permaflow_held(address.clone()) || !self.rests_for_closing(&flow_id.0) {
            return self.queue_refresh(address, &flow_id.0, caller);
        }
        self.refresh_held(address, metaflow, flow_id, caller, turn, lock)
    }

    fn replace_held(&self, profile: LaunchProfile, origin: Origin) -> Output {
        use signal_flow::ReplaceRejection;
        let address = profile.address.clone();
        let response = {
            let turn = self
                .dispatch_gate
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let Some(lock) = self.waking.hold(&address) else {
                return Output::ReplaceRejected(ReplaceRejection::Locked);
            };
            // A restart holds the Address its predecessor runs at.
            if let Ok(Some(_)) =
                crate::store::restart::RecordsRestarts::restart_at(&*self.store, &address)
            {
                return Output::ReplaceRejected(ReplaceRejection::Locked);
            }
            // A working predecessor is never closed: the replacement waits
            // for its idle event. One already under way or settled resumes.
            if let Some(predecessor) = profile.flow_id_option.clone()
                && matches!(
                    self.store.metaflow(&address),
                    Ok(Some(metaflow)) if metaflow.state == flow_ethos::State::Awake(predecessor.clone())
                )
                && !self.launch_known(&profile.launch_request_id.0)
                && (self.permaflow_held(address.clone()) || !self.rests_for_closing(&predecessor.0))
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
    fn refresh_held(
        &self,
        address: &Address,
        metaflow: Metaflow,
        flow_id: FlowId,
        caller: &RefreshCaller,
        turn: std::sync::MutexGuard<'_, ()>,
        lock: WakingLease,
    ) -> Output;
    /// Records a Refresh of the working `flow_id` with its `caller`, under
    /// the gate, and answers RefreshQueued; a Refresh already waiting for it
    /// keeps the caller it was recorded with, and a Launch already waiting
    /// for it refuses it Locked.
    fn queue_refresh(&self, address: &Address, flow_id: &str, caller: &RefreshCaller) -> Output;
    /// Records the Launch or Replace of the working `predecessor`, under the
    /// gate, once its successor composes, and answers LaunchPending with the
    /// attempt it waits as; another order waiting for it refuses it Locked.
    fn queue_replacement(
        &self,
        profile: LaunchProfile,
        origin: Origin,
        predecessor: FlowId,
    ) -> Output;
    /// The replacement, under the metaflow's hold the caller has: its
    /// successor owed until confirmed, started and confirmed, then its
    /// predecessor reaped. Takes the gate.
    fn replace_owed(&self, profile: LaunchProfile, origin: Origin) -> Output;
    /// Whether Flow already holds an attempt, an outcome or a replacement of
    /// this launch request.
    fn launch_known(&self, launch_request_id: &str) -> bool;
    /// Carries out the order at the rest of its flow, under the gate `turn`
    /// the caller holds; the order is spent whatever it comes to.
    fn replace_as_ordered(
        &self,
        order: OrderedReplacement,
        metaflow: Metaflow,
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
        caller: &RefreshCaller,
        turn: std::sync::MutexGuard<'_, ()>,
        lock: WakingLease,
    ) -> Output {
        use signal_flow::{RefreshRejection, Refreshed};
        // Replace takes the gate for its own steps; the lock keeps any
        // wake, drain or delivery of the metaflow out meanwhile.
        drop(turn);
        let replaced = self.refresh_successor_first(
            &flow_id.0,
            &metaflow,
            SuccessionCause::Ordered(caller.clone()),
        );
        drop(lock);
        self.wake_deferred();
        let outcome = match replaced {
            // The successor runs; its predecessor's reap is pending.
            AtStop::ReapPending(pending) => {
                eprintln!(
                    "flow-nexus: flow {} refreshed; its successor {} runs; its reap is pending: {:?}",
                    flow_id.0, pending.launched.flow_id.0, pending.reap_standing
                );
                self.wake_due(address.clone());
                Output::ReapPending(*pending)
            }
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
                Output::Refreshed(Refreshed {
                    address: address.clone(),
                    flow_id: flow_id.clone(),
                })
            }
            // A refused successor answers the Replace's own rejection;
            // one whose launch is still pending or ambiguous when the
            // confirmation wait ends is unconfirmed, and stays owed.
            AtStop::Kept(KeptAtStop::RefreshLaunchRefused(response)) => {
                Output::RefreshRejected(match *response {
                    Output::ReplaceRejected(rejection) => {
                        RefreshRejection::SuccessorRefused(rejection)
                    }
                    Output::StartRejected(rejection) => RefreshRejection::SuccessorRefused(
                        signal_flow::ReplaceRejection::LaunchRefused(rejection),
                    ),
                    Output::LaunchPending(_) | Output::StartAmbiguous(_) => {
                        RefreshRejection::SuccessorUnconfirmed
                    }
                    _ => RefreshRejection::PersistenceRefused,
                })
            }
            AtStop::Kept(KeptAtStop::RefreshUnplaceable) => {
                Output::RefreshRejected(RefreshRejection::Undeclared)
            }
            _ => Output::RefreshRejected(RefreshRejection::PersistenceRefused),
        };
        if let Output::RefreshRejected(reason) = &outcome {
            eprintln!(
                "flow-nexus: refresh of {address:?} flow {} rejected: {reason:?}",
                flow_id.0
            );
        }
        outcome
    }

    fn queue_refresh(&self, address: &Address, flow_id: &str, caller: &RefreshCaller) -> Output {
        use signal_flow::{RefreshQueued, RefreshRejection, Refusal};
        match self.store.ordered_replacement(address) {
            Ok(Some(order)) if order.predecessor.0 == flow_id => {
                return match order.replacement_order {
                    ReplacementOrder::Refresh(_) => Output::RefreshQueued(RefreshQueued {
                        address: address.clone(),
                        flow_id: signal_flow::FlowId(flow_id.to_owned()),
                    }),
                    ReplacementOrder::Launch(_) | ReplacementOrder::Restart => {
                        Output::RefreshRejected(RefreshRejection::Locked)
                    }
                };
            }
            Ok(_) => {}
            Err(error) => return Output::Refused(Refusal::Store(error.to_string())),
        }
        if let Err(error) = self.store.order_replacement(OrderedReplacement {
            address: address.clone(),
            predecessor: crate::generated::memory::Predecessor(flow_id.to_owned()),
            replacement_order: ReplacementOrder::Refresh(caller.clone()),
        }) {
            return Output::Refused(Refusal::Store(error.to_string()));
        }
        eprintln!("flow-nexus: flow {flow_id} works; its refresh waits for its rest");
        Output::RefreshQueued(RefreshQueued {
            address: address.clone(),
            flow_id: signal_flow::FlowId(flow_id.to_owned()),
        })
    }

    fn queue_replacement(
        &self,
        mut profile: LaunchProfile,
        origin: Origin,
        predecessor: FlowId,
    ) -> Output {
        use crate::launching::{InterpretsLaunchObservations, ResumesReplacement};
        use signal_flow::{ReplaceRejection, StartRejection};
        let persistence = || {
            Output::ReplaceRejected(ReplaceRejection::LaunchRefused(
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
            Ok(Some(order)) if order.predecessor.0 == predecessor.0 => {
                return match order.ordered_launch() {
                    Some(launch) if launch.launch_profile == profile && launch.origin == origin => {
                        Output::LaunchPending(launch.waiting_attempt())
                    }
                    _ => Output::ReplaceRejected(ReplaceRejection::Locked),
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
            prompt_sha256: crate::generated::memory::PromptSha256(prompt_sha256.0),
        };
        if self
            .store
            .order_replacement(OrderedReplacement {
                address,
                predecessor: crate::generated::memory::Predecessor(predecessor.clone().0),
                replacement_order: ReplacementOrder::Launch(Box::new(launch.clone())),
            })
            .is_err()
        {
            return persistence();
        }
        eprintln!(
            "flow-nexus: flow {predecessor} works; its replacement waits for its rest",
            predecessor = predecessor.0
        );
        Output::LaunchPending(launch.waiting_attempt())
    }

    fn replace_owed(&self, profile: LaunchProfile, origin: Origin) -> Output {
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
                    predecessor: crate::generated::memory::Predecessor(predecessor.0),
                    succession_cause: SuccessionCause::Replace,
                    launch_request_id: profile.launch_request_id.clone(),
                })
                .is_err()
        {
            return Output::ReplaceRejected(ReplaceRejection::LaunchRefused(
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
        turn: std::sync::MutexGuard<'_, ()>,
    ) -> AtStop {
        let address = metaflow.address.clone();
        let Some(lock) = self.waking.hold(&address) else {
            return AtStop::Kept(KeptAtStop::Waking);
        };
        eprintln!(
            "flow-nexus: flow {} rests; processing its ordered replacement",
            order.predecessor.0
        );
        let response = match order.replacement_order {
            // A restart takes the gate for its own steps and holds the
            // Address by its record: the waking lease is given back first.
            ReplacementOrder::Restart => {
                drop(turn);
                drop(lock);
                let _ = self.store.forget_ordered_replacement(&address);
                let response = crate::restarting::RestartsFlows::restart_at_idle_event(
                    self,
                    &order.predecessor.0,
                );
                return match response {
                    Output::Restarted(_) => AtStop::Restarted(address),
                    response => AtStop::Kept(KeptAtStop::OrderedRefused(Box::new(response))),
                };
            }
            ReplacementOrder::Refresh(caller) => self.refresh_held(
                &address,
                metaflow,
                signal_flow::FlowId(order.predecessor.0),
                &caller,
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
            Output::Refreshed(_) | Output::Replaced(_) => AtStop::Refreshed(address),
            // The successor started and receives; the predecessor's reap is
            // owed: the order was carried out.
            Output::ReapPending(pending) => AtStop::ReapPending(Box::new(pending)),
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
                    order.predecessor.0
                );
                let _ = self.store.forget_ordered_replacement(address);
                false
            }
        }
    }

    fn start_ordered(&self, order: OrderedReplacement, launch: OrderedLaunch) {
        use crate::generated::overriding::SerializesPermaflowEffects;
        let _effect = self.waking.effect(order.address.clone());
        if self.held() || self.permaflow_held(order.address.clone()) {
            return;
        }

        let mut profile = launch.launch_profile;
        profile.flow_id_option = None;
        let launch_request_id = profile.launch_request_id.clone();
        // The start, its settlement (which takes over what the Address
        // owes) and the order's forgetting run under the dispatch gate, as
        // every other start's do; the gate is released before the deferred
        // wakes, which need it.
        let response = {
            let _turn = self
                .dispatch_gate
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let response = self.start(profile, launch.origin);
            let response = self.settle(&launch_request_id.0, response);
            let _ = self.store.forget_ordered_replacement(&order.address);
            response
        };
        self.wake_deferred();
        eprintln!(
            "flow-nexus: flow {} ended before it rested; the Launch ordered for it starts: {response:?}",
            order.predecessor.0
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
                if self.permaflow_held(order.address.clone()) {
                    continue;
                }
                match self.store.metaflow(&order.address) {
                    Ok(Some(metaflow)) => match metaflow.state {
                        flow_ethos::State::Awake(current) if current.0 == order.predecessor.0 => {
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
    /// With the metaflow's waking lease and no caller-held dispatch turn:
    /// hands the queue of the seat `address` is Awake as to its pane
    /// in one payload, oldest first, and records the pane writer's own
    /// result in the one store commit that takes those requests from the
    /// queue. Presented or Transported is Delivered and Uncertain is
    /// Uncertain, each final; a refused write leaves them queued.
    fn drain_held(&self, address: Address, lease: &WakingLease) -> Outcome;
    /// Hands over until the queue is empty or a hand-over takes nothing.
    fn drain_all_held(&self, address: Address, lease: &WakingLease) -> Outcome;
}

/// What a hand-over's requests are recorded as, read off the pane writer's
/// own result.
pub(crate) trait RecordsWriterResult {
    /// Delivered for Presented or Transported, naming the seat's session
    /// and `prompt`, the native prompt that took it; Uncertain for
    /// Uncertain.
    fn handed_over_state(&self, node: &FlowNode, prompt: String) -> signal_flow::DeliveryState;
}

impl RecordsWriterResult for meta_signal_flow::VisibleDelivery {
    fn handed_over_state(&self, node: &FlowNode, prompt: String) -> signal_flow::DeliveryState {
        match self.delivery_grade {
            meta_signal_flow::DeliveryGrade::Presented
            | meta_signal_flow::DeliveryGrade::Transported => {
                signal_flow::DeliveryState::Delivered(signal_flow::NativeReceipt {
                    harness_kind: node.harness_kind.clone(),
                    native_session_id: signal_flow::NativeSessionId(node.session_id.0.clone()),
                    native_prompt_id: signal_flow::NativePromptId(prompt),
                })
            }
            meta_signal_flow::DeliveryGrade::Uncertain => signal_flow::DeliveryState::Uncertain,
        }
    }
}

/// The native prompt a Claude seat's own UserPromptSubmit names, the word
/// a pane writer waits for: the id a Simplified seat's answer is read
/// after.
pub(crate) trait ReadsReportedPrompt {
    /// The prompt id of the flow's last submitted prompt its own session
    /// reported, of `digest` when one is named.
    fn reported_prompt(&self, flow_id: &str, digest: Option<&str>) -> Option<String>;
}

impl ReadsReportedPrompt for RunningNexus {
    fn reported_prompt(&self, flow_id: &str, digest: Option<&str>) -> Option<String> {
        self.store
            .events(flow_id)
            .ok()
            .flatten()?
            .into_iter()
            .rev()
            .find_map(|event| match event {
                RecordedHarnessEvent::PromptSubmitted(prompt)
                    if self.correlated_prompt(flow_id, &prompt)
                        && digest.is_none_or(|digest| prompt.prompt_sha256.0 == digest) =>
                {
                    prompt.prompt_id_option.map(|prompt_id| prompt_id.0)
                }
                _ => None,
            })
    }
}

impl DrainsHeldSeats for RunningNexus {
    fn drain_held(&self, address: Address, lease: &WakingLease) -> Outcome {
        let (flow_id, node, cohort, delivery_id, digest, request) = {
            let _turn = self
                .dispatch_gate
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if self.permaflow_held(address.clone()) || self.held() {
                return Outcome::Drained(Vec::new());
            }
            match self.store.stored_metaflow_lock(&address) {
                Ok(None) => {}
                Ok(Some(_)) => return Outcome::Drained(Vec::new()),
                Err(_) => return Outcome::Failed(Failed_Data::StoreRefused),
            }
            let flow_id = match self.store.metaflow(&address) {
                Ok(Some(Metaflow {
                    state: flow_ethos::State::Awake(flow_id),
                    ..
                })) => flow_id,
                Ok(_) => return Outcome::Failed(Failed_Data::NotAwake),
                Err(_) => return Outcome::Failed(Failed_Data::StoreRefused),
            };
            use crate::generated::memory::SeatStanding;
            use crate::store::current_seat::SeatDecision;
            match self.store.seat_decision(&flow_id.0) {
                Ok(SeatDecision::Observed(SeatStanding::Alive(_))) => {}
                Ok(SeatDecision::Observed(SeatStanding::Ended)) => {
                    return Outcome::Failed(Failed_Data::NotAwake);
                }
                Ok(SeatDecision::NoRecord) => return Outcome::Failed(Failed_Data::NoRecord),
                Err(_) => return Outcome::Failed(Failed_Data::StoreRefused),
            }
            let node = match self.store.flow_node(&flow_id.0) {
                Ok(Some(node)) => node,
                _ => return Outcome::Failed(Failed_Data::StoreRefused),
            };
            // A simplified seat's answer to the input it was bound to is queued
            // once its transcript shows it, so the binding is free for the
            // request now oldest.
            self.queue_simplified_answer(&flow_id.0);
            let queue = match self.store.metaflow(&address) {
                Ok(Some(metaflow)) => metaflow.queue,
                _ => return Outcome::Failed(Failed_Data::StoreRefused),
            };
            let Some(oldest) = queue.0.first().cloned() else {
                return Outcome::Drained(Vec::new());
            };
            if self.store.pane_input_waits_for_reply(&oldest.request_id)
                || matches!(
                    self.store
                        .queue_request_state(&oldest.logical_delivery_id.0),
                    Ok(Some(RequestState::InFlight))
                )
            {
                return Outcome::WaitingForTurn(oldest.logical_delivery_id);
            }
            let response_request = matches!(
                &oldest.message,
                signal_flow::Message::HardAbrupt(
                    signal_flow::Request::Order(_)
                        | signal_flow::Request::Question(_)
                        | signal_flow::Request::Living(_)
                        | signal_flow::Request::Task(_)
                ) | signal_flow::Message::MiddleAbrupt(
                    signal_flow::Request::Order(_)
                        | signal_flow::Request::Question(_)
                        | signal_flow::Request::Living(_)
                        | signal_flow::Request::Task(_)
                ) | signal_flow::Message::Soft(
                    signal_flow::Request::Order(_)
                        | signal_flow::Request::Question(_)
                        | signal_flow::Request::Living(_)
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
                    flow_id: flow_id.clone(),
                    input_delivery_id: crate::generated::memory::InputDeliveryId(
                        oldest.logical_delivery_id.clone().0,
                    ),
                    source: crate::generated::memory::Source(address.clone()),
                    recipient: crate::generated::memory::Recipient(recipient.clone()),
                    completed: crate::generated::memory::Completed(false),
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
                queue
                    .0
                    .into_iter()
                    .take_while(|entry| !self.store.pane_input_waits_for_reply(&entry.request_id))
                    .collect()
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
            let message = if let Some(pushed) = lease.pushed() {
                // A Push delivers the whole queue at the priority it names.
                match pushed {
                    signal_flow::Priority::HardAbrupt => {
                        meta_signal_flow::Message::HardAbrupt(letter)
                    }
                    signal_flow::Priority::MiddleAbrupt => {
                        meta_signal_flow::Message::MiddleAbrupt(letter)
                    }
                    signal_flow::Priority::Soft => meta_signal_flow::Message::Soft(letter),
                }
            } else if cohort
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
            // The pane write is named by the oldest request it carries.
            let delivery_id = oldest.logical_delivery_id.0.clone();
            let digest = {
                use crate::delivery::body::{RendersPaneText, ShowsPaneText};
                use flow_native::DigestsPrompt;
                message.pane_text().as_str().prompt_sha256()
            };
            let request = meta_signal_flow::DeliveryRequest {
                delivery_id: meta_signal_flow::DeliveryId(delivery_id.clone()),
                flow_id: flow_id.clone(),
                message,
            };
            if !matches!(
                self.store.begin_queue_write(&address, &flow_id.0, &cohort),
                Ok(true)
            ) {
                return Outcome::Failed(Failed_Data::StoreRefused);
            }
            (flow_id, node, cohort, delivery_id, digest, request)
        };
        let state = match self.deliver_while_waking(request, lease) {
            meta_signal_flow::Output::MessageTyped(delivery) => {
                // A Claude seat that took the text named its prompt in its
                // UserPromptSubmit. Other hand-overs use the pane write id.
                let prompt = match (&delivery.delivery_grade, &node.harness_kind) {
                    (meta_signal_flow::DeliveryGrade::Presented, HarnessKind::Claude) => {
                        self.reported_prompt(&flow_id.0, Some(&digest))
                    }
                    _ => None,
                }
                .unwrap_or(delivery_id);
                Some(delivery.handed_over_state(&node, prompt))
            }
            refused => {
                eprintln!(
                    "flow-nexus: HerdrRefused flow={} delivery={delivery_id} outcome={refused:?}; it stays queued",
                    flow_id.0
                );
                None
            }
        };
        let _turn = self
            .dispatch_gate
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let rejected = state.is_none();
        match self
            .store
            .finish_queue_write(&address, &flow_id.0, &cohort, state)
        {
            Ok(true) if rejected => Outcome::Failed(Failed_Data::HerdrRefused),
            Ok(true) => Outcome::Drained(cohort),
            answer => {
                eprintln!(
                    "flow-nexus: pane write outcome for flow {} was not recorded: {answer:?}; it stays in flight",
                    flow_id.0
                );
                Outcome::Failed(Failed_Data::StoreRefused)
            }
        }
    }

    fn drain_all_held(&self, address: Address, lease: &WakingLease) -> Outcome {
        let mut delivered = Vec::new();
        loop {
            match self.drain_held(address.clone(), lease) {
                Outcome::Drained(entries) if entries.is_empty() => {
                    return Outcome::Drained(delivered);
                }
                Outcome::Drained(mut entries) => delivered.append(&mut entries),
                outcome if delivered.is_empty() => return outcome,
                _ => return Outcome::Drained(delivered),
            }
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
                Ok(Some(metaflow)) if metaflow.state == flow_ethos::State::Asleep => {
                    use crate::handover::NamesPredecessor;
                    if let Some(predecessor) = metaflow.past.0.last()
                        && !predecessor.0.names_predecessor()
                    {
                        return Err(WakeRefusal::FreshNotStarted(
                            Failed_Data::CompositionRefused(
                                signal_flow::CompositionRefusal::HandoverUnreadable(
                                    signal_flow::HandoverUnreadable_Data {
                                        flow_id: predecessor.clone(),
                                        handover_fault:
                                            signal_flow::HandoverFault::InvalidPredecessor,
                                    },
                                ),
                            ),
                        ));
                    }
                    use crate::store::metaflow::{ProvesWakeOrigin, WakeOrigin};
                    match self
                        .store
                        .wake_origin(&metaflow)
                        .map_err(|_| WakeRefusal::StoreUnreadable)?
                    {
                        WakeOrigin::FirstSeat | WakeOrigin::EndedPredecessor(_) => {}
                        WakeOrigin::LivePredecessor => return Err(WakeRefusal::SessionLive),
                        WakeOrigin::Undeclared => return Err(WakeRefusal::Unplaceable),
                        WakeOrigin::Unresolved => {
                            return Err(WakeRefusal::FreshNotStarted(Failed_Data::NoRecord));
                        }
                    }
                    if !self.has_due_work(&metaflow) {
                        return Err(WakeRefusal::NothingQueued);
                    }
                    // A store refusal after a pane write leaves its marker
                    // intact. No fresh prompt may carry that request again.
                    for entry in &metaflow.queue.0 {
                        if !matches!(self.store.queue_entry_is_writable(entry), Ok(true)) {
                            return Err(WakeRefusal::StoreUnreadable);
                        }
                    }
                    metaflow
                }
                Ok(_) => return Err(WakeRefusal::NotAsleep),
                Err(_) => return Err(WakeRefusal::StoreUnreadable),
            }
        };
        // A metaflow owed a successor wakes with nothing queued.
        let owed = self
            .store
            .successor_intent(address)
            .ok()
            .flatten()
            .is_some_and(|intent| {
                !intent.succession_cause.is_automatic_refresh()
                    || matches!(
                        self.store.refresh_mode(address),
                        Ok(meta_signal_flow::RefreshMode::Automatic)
                    )
            });
        if metaflow.queue.0.is_empty() && !owed {
            return Err(WakeRefusal::NothingQueued);
        }
        if metaflow
            .queue
            .0
            .iter()
            .any(|entry| self.store.pane_input_waits_for_reply(&entry.request_id))
        {
            return Err(WakeRefusal::AdmissionReplyPending);
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
        {
            // Planning and native preflight run off the gate. Recheck the
            // selected prefix before starting: an older snapshot grants no
            // permission to submit a removed or non-writable request. The
            // caller's waking lease excludes another writer through Start.
            let _turn = self
                .dispatch_gate
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            let Ok(Some(current)) = self.store.metaflow(address) else {
                return Err(WakeRefusal::StoreUnreadable);
            };
            if current.state != flow_ethos::State::Asleep {
                return Err(WakeRefusal::NotAsleep);
            }
            if !current.queue.0.starts_with(&startup.0)
                || startup
                    .0
                    .iter()
                    .any(|entry| !matches!(self.store.queue_entry_is_writable(entry), Ok(true)))
            {
                return Err(WakeRefusal::StoreUnreadable);
            }
        }
        match self.start_fresh(&metaflow, fresh, &startup_text, &startup.0) {
            Ok(id) => Ok(id),
            Err(FreshRefusal::Failed(failure)) => Err(WakeRefusal::FreshNotStarted(failure)),
            Err(FreshRefusal::Rejected(rejection)) => {
                // Only the prefix, never admissions behind it. A retained
                // ambiguous launch is not a refusal that authorizes a retry.
                use crate::store::fresh_launch::TracksFreshLaunches;
                let _turn = self.dispatch_gate.lock().unwrap_or_else(|e| e.into_inner());
                if matches!(self.store.fresh_cohort(address), Ok(None)) {
                    let _ = self.store.mark_queue_retryable(
                        &startup.0,
                        signal_flow::RetryCause::WakeRefused(rejection.clone()),
                    );
                }
                Err(WakeRefusal::StartRejected(rejection))
            }
        }
    }

    fn start_fresh(
        &self,
        metaflow: &Metaflow,
        fresh: FreshContext,
        text: &str,
        startup: &[QueueEntry],
    ) -> Result<FlowId, FreshRefusal> {
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
        use crate::sending::MintsId;
        let launch_request_id = "wake".minted_id();
        let seat = FreshSeat {
            address: address.clone(),
            launch_request_id: launch_request_id.clone(),
            model_name: model,
            effort: self
                .store
                .configured_effort(address)
                .map_err(|_| FreshRefusal::Failed(Failed_Data::StoreRefused))?,
            herdr_session_name: fresh.herdr_session_name.0,
            system_modules: fresh.system_modules,
            prompt_modules: fresh.prompt_modules,
            leave_outs_option: fresh.leave_outs_option.map(|value| value.0),
            flow_kind: fresh.flow_kind,
            tool_set: fresh.tool_set,
            prompt: text.with_purpose(&fresh.purpose.0),
        };
        // The metaflow's last flow is the predecessor this wake succeeds, and
        // its handover is given to the new flow. The metaflow is Asleep, so
        // the predecessor takes no part in taking its Address.
        let mut profile = seat.fresh_profile();

        use crate::store::fresh_launch::TracksFreshLaunches;
        let claim = {
            let _turn = self.dispatch_gate.lock().unwrap_or_else(|e| e.into_inner());
            use crate::store::metaflow::{ProvesWakeOrigin, WakeOrigin};
            let bound = self
                .store
                .metaflow(address)
                .map_err(|_| FreshRefusal::Failed(Failed_Data::StoreRefused))?
                .ok_or(FreshRefusal::Failed(Failed_Data::NoRecord))?;
            if !matches!(self.store.metaflow_declaration(address), Ok(Some(_))) {
                return Err(FreshRefusal::Failed(Failed_Data::NoRecord));
            }
            profile.flow_id_option = match self
                .store
                .wake_origin(&bound)
                .map_err(|_| FreshRefusal::Failed(Failed_Data::StoreRefused))?
            {
                WakeOrigin::FirstSeat => None,
                WakeOrigin::EndedPredecessor(id) => Some(id),
                _ => return Err(FreshRefusal::Failed(Failed_Data::NoRecord)),
            };
            if !self
                .store
                .claim_fresh(address, &profile.launch_request_id, startup)
                .map_err(|_| FreshRefusal::Failed(Failed_Data::StoreRefused))?
            {
                return Err(FreshRefusal::Failed(Failed_Data::StoreRefused));
            }
            self.store
                .fresh_cohort(address)
                .map_err(|_| FreshRefusal::Failed(Failed_Data::StoreRefused))?
                .ok_or(FreshRefusal::Failed(Failed_Data::StoreRefused))?
        };
        let result = self.start_unbound(profile);
        if result.is_err() {
            let _turn = self.dispatch_gate.lock().unwrap_or_else(|e| e.into_inner());
            self.store
                .reconcile_fresh(&claim)
                .map_err(|_| FreshRefusal::Failed(Failed_Data::StoreRefused))?;
        }
        result
    }

    fn last_launch_profile(&self, flow_id: &str) -> Option<LaunchProfile> {
        let launch_request_id = self.store.launch_requests_bound_to(flow_id).ok()?.pop()?;
        self.store
            .composable_launch_profile(&launch_request_id)
            .ok()
            .flatten()
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
        let automatic = cause.is_automatic_refresh();
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
                if intent.predecessor.0 == flow_id
                    && matches!(
                        intent.succession_cause,
                        SuccessionCause::Refresh | SuccessionCause::Ordered(_)
                    )
                    && !intent.launch_request_id.0.is_empty()
                    && !self.launch_refused(&intent.launch_request_id.0) =>
            {
                // The successor is owed now for this refresh's cause.
                if intent.succession_cause != cause
                    && self
                        .store
                        .owe_successor(SuccessorIntent {
                            succession_cause: cause,
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
                    predecessor: crate::generated::memory::Predecessor(flow_id.to_owned()),
                    succession_cause: cause,
                    launch_request_id: signal_flow::LaunchRequestId(
                        crate::sending::MintsId::minted_id("refresh"),
                    ),
                };
                if self.store.owe_successor(intent.clone()).is_err() {
                    return AtStop::Kept(KeptAtStop::StoreUnreadable);
                }
                intent.launch_request_id
            }
            Err(_) => return AtStop::Kept(KeptAtStop::StoreUnreadable),
        };
        profile.launch_request_id = signal_flow::LaunchRequestId(launch_request_id.0);
        let response = {
            let _turn = self
                .dispatch_gate
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if automatic {
                match self.store.refresh_mode(&address) {
                    Ok(meta_signal_flow::RefreshMode::Automatic) => {}
                    Ok(meta_signal_flow::RefreshMode::Manual) => {
                        return match self.store.forget_successor(&address) {
                            Ok(_) => AtStop::Idle,
                            Err(_) => AtStop::Kept(KeptAtStop::StoreUnreadable),
                        };
                    }
                    Err(_) => return AtStop::Kept(KeptAtStop::StoreUnreadable),
                }
            }
            self.replace(profile, Origin::Unbound)
        };
        self.settle_owed(&address, &response);
        match response {
            Output::Replaced(_) => AtStop::Refreshed(address),
            // The successor is confirmed and receiving; its predecessor's
            // reap is owed.
            Output::ReapPending(pending) => AtStop::ReapPending(Box::new(pending)),
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
        // The successor's context is the metaflow's declaration.
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
            effort: self.store.configured_effort(&address).ok()?,
            herdr_session_name: fresh.herdr_session_name.0,
            system_modules: fresh.system_modules,
            prompt_modules: fresh.prompt_modules,
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

    fn successor_refused(&self, flow_id: &str, reason: &KeptAtStop) {
        use crate::delivery::DeliversMessages;
        use crate::store::logical_delivery::RecordsLogicalDeliveries;
        let address = match self.store.metaflow_awake_as(flow_id) {
            Ok(Some(metaflow)) => metaflow.address,
            _ => return,
        };
        // A standing refusal is reported once. The kept seat still drains.
        let first = self.refresh_refusals.note(flow_id);
        let harness = self
            .store
            .flow_node(flow_id)
            .ok()
            .flatten()
            .map(|node| node.harness_kind);
        let past = matches!(
            self.store.threshold_standing(flow_id),
            Ok(crate::store::context::ThresholdStanding::Past { .. })
        );
        let compact = first && past && harness == Some(HarnessKind::Claude);
        let drain_due = {
            let _turn = self
                .dispatch_gate
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if first {
                match self.store.configured_investigator() {
                    Ok(Some(investigator)) => {
                        let fault = format!(
                            "Investigate the refused successor for {{ {:?} {} {:?} }}, flow {flow_id}: {reason:?}.",
                            address.aspect, address.topic.0, address.model,
                        );
                        match self.store.accept_logical_delivery(
                            &investigator,
                            signal_flow::SentBy::Flow,
                            flow_ethos::Delivery::Wake,
                            signal_flow::Message::Soft(signal_flow::Request::Order(fault)),
                            None,
                        ) {
                            Ok(signal_flow::PrivateSendResponse::SendAccepted(_)) => {
                                // If the investigator is this seat, its drain below
                                // carries the order under the effect already held.
                                if investigator != address {
                                    self.defer_wake(investigator);
                                }
                            }
                            refusal => eprintln!(
                                "flow-nexus: investigator fault for {flow_id} was not queued: {refusal:?}"
                            ),
                        }
                    }
                    Ok(None) => eprintln!(
                        "flow-nexus: investigator is not configured; successor fault for {flow_id} stays in the log"
                    ),
                    Err(error) => eprintln!(
                        "flow-nexus: investigator unreadable; successor fault for {flow_id} stays in the log: {error}"
                    ),
                }
            }
            matches!(
                self.store.metaflow(&address),
                Ok(Some(metaflow)) if self.due_at_rest(&metaflow.queue.0)
            )
        };
        if drain_due {
            let _ = self.perform(Operation::Drain(address.clone()));
        }
        {
            let _turn = self
                .dispatch_gate
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            // The compaction types into the pane and may record the flow
            // Exited when its pane is gone: it runs under the same gate hold,
            // as a meta Command does (the gate is not reentrant, and
            // `command` takes none itself).
            if compact
                && matches!(
                    self.store.refresh_mode(&address),
                    Ok(meta_signal_flow::RefreshMode::Automatic)
                )
            {
                let compacted = self.command(meta_signal_flow::CommandRequest {
                    flow_id: signal_flow::FlowId(flow_id.to_owned()),
                    harness_command_kind: meta_signal_flow::HarnessCommandKind::Compact,
                });
                eprintln!("flow-nexus: the kept flow {flow_id}, past its threshold: {compacted:?}");
            }
        }
    }

    fn settle_owed(&self, address: &Address, response: &Output) {
        match response {
            Output::LaunchPending(_) | Output::StartAmbiguous(_) => {}
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
        use crate::generated::overriding::SerializesPermaflowEffects;
        let _effect = self.waking.effect(address.clone());
        if self.held() || self.permaflow_held(address.clone()) {
            return AtStop::Idle;
        }
        if intent.succession_cause.is_automatic_refresh() {
            let _turn = self.dispatch_gate.lock().unwrap_or_else(|e| e.into_inner());
            match self.store.refresh_mode(&address) {
                Ok(meta_signal_flow::RefreshMode::Automatic) => {}
                Ok(meta_signal_flow::RefreshMode::Manual) => {
                    return match self.store.forget_successor(&address) {
                        Ok(_) => AtStop::Idle,
                        Err(_) => AtStop::Kept(KeptAtStop::StoreUnreadable),
                    };
                }
                Err(_) => return AtStop::Kept(KeptAtStop::StoreUnreadable),
            }
        }
        let Some(lock) = self.waking.hold(&address) else {
            return AtStop::Kept(KeptAtStop::Waking);
        };
        // The owed flow is still current: the intent was read under the
        // gate, and the lock keeps every wake and refresh out since.
        let metaflow = match self.store.metaflow(&address) {
            Ok(Some(metaflow))
                if metaflow.state
                    == flow_ethos::State::Awake(flow_ethos::FlowId(
                        intent.predecessor.clone().0,
                    )) =>
            {
                metaflow
            }
            Ok(_) => return AtStop::Kept(KeptAtStop::NotAwake),
            Err(_) => return AtStop::Kept(KeptAtStop::StoreUnreadable),
        };
        let taken = match intent.succession_cause {
            SuccessionCause::Replace => {
                let attempt = match self.store.launch_attempt(&intent.launch_request_id.0) {
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
                if self.launch_refused(&intent.launch_request_id.0) {
                    profile.launch_request_id =
                        signal_flow::LaunchRequestId(crate::sending::MintsId::minted_id("replace"));
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
                    self.replace(profile, attempt.origin)
                };
                self.settle_owed(&address, &response);
                match response {
                    Output::Replaced(_) => AtStop::Refreshed(address),
                    Output::ReapPending(pending) => AtStop::ReapPending(Box::new(pending)),
                    response => AtStop::Kept(KeptAtStop::RefreshLaunchRefused(Box::new(response))),
                }
            }
            cause => self.refresh_successor_first(&intent.predecessor.0, &metaflow, cause),
        };
        drop(lock);
        self.wake_deferred();
        taken
    }

    fn settle_rested(&self, flow_id: &str, session: &str) -> AtStop {
        if matches!(self.store.flow_node(flow_id), Ok(Some(node)) if node.session_id.0 != session) {
            return AtStop::Kept(KeptAtStop::OtherSession);
        }
        crate::context_warnings::ObservesClaudeContext::observe_claude_context(self, flow_id);
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
        if !self.rests_now(flow_id) {
            return AtStop::Kept(KeptAtStop::InTurn);
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
        // The flow's rest bears on what its launch still owes: each reap it
        // follows and the handover at its Address, taken up once, in the
        // store alone; a reap's pane step is the worker's, armed again here.
        crate::reaping::TakesUpObligations::take_up_at_rest(self, flow_id);
        // A seat is refreshed at this idle event once it is past its
        // threshold, or once it was sent its handover warning, or wrote a
        // handover qualified for the warning window, whether or not it rests
        // and whether or not its handover is written: an absent handover
        // starts its successor from its definition and its queue. A
        // protected Address is never refreshed for its context. The
        // decision is taken under the dispatch gate, as a Soft Send's
        // hand-over at this idle event is: whichever comes first shuts out
        // the other.
        let Some((refresh_mode, refresh_due)) = self.refresh_standing(flow_id, &address, warned)
        else {
            return AtStop::Kept(KeptAtStop::StoreUnreadable);
        };
        let stored_node = match self.store.flow_node(flow_id) {
            Ok(Some(node)) if node.session_id.0 == session => node,
            Ok(Some(_)) => return AtStop::Kept(KeptAtStop::OtherSession),
            Ok(None) => return AtStop::Kept(KeptAtStop::UnknownFlow),
            Err(_) => return AtStop::Kept(KeptAtStop::StoreUnreadable),
        };
        let node = self.herdr.refresh_route(stored_node);
        let continuity = match self.store.continuity(&address) {
            Ok(continuity) => continuity,
            Err(_) => return AtStop::Kept(KeptAtStop::StoreUnreadable),
        };
        // A replacement ordered from outside is carried out once the seat
        // rests; until then the seat is handed what it is sent. A Refresh or
        // a Restart ordered is carried out at this idle event, its
        // background work ended, as a refresh past the threshold is.
        let ordered = match self.store.ordered_replacement(&address) {
            Ok(order) => order.filter(|order| order.predecessor.0 == flow_id),
            Err(_) => return AtStop::Kept(KeptAtStop::StoreUnreadable),
        };
        if !self.rests_now(flow_id) {
            return AtStop::Kept(KeptAtStop::InTurn);
        }
        let closable = self.rests_for_closing(flow_id);
        if closable && let Some(order) = ordered {
            return self.replace_as_ordered(order, metaflow, turn);
        }
        // The warning's own delivery is not proof that the harness has
        // started a turn. Do not sleep or auto-refresh it on this very
        // rest; wait for its next authoritative rest edge.
        use crate::store::handover_reminders::SchedulesHandoverReminders;
        if !warned {
            match self.store.warned(&address, flow_id) {
                Ok(true) => return AtStop::Idle,
                Ok(false) => {}
                Err(_) => return AtStop::Kept(KeptAtStop::StoreUnreadable),
            }
        }
        if refresh_due && !closable {
            return AtStop::Idle;
        }
        if refresh_due {
            let wakes = metaflow.queue.0.holds_waking_request();
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
        // A simplified seat's answer is queued before the seat is handed
        // more or sleeps.
        self.queue_simplified_answer(flow_id);
        // Only work due at its rest starts the seat's next turn; Soft
        // Notices alone stay queued for the next hand-over or waking, and
        // the seat continues as one with nothing due.
        let metaflow = match self.store.metaflow(&address) {
            Ok(Some(metaflow)) => metaflow,
            _ => return AtStop::Kept(KeptAtStop::StoreUnreadable),
        };
        if self.due_at_rest(&metaflow.queue.0) {
            let Some(lease) = self.waking.hold(&address) else {
                return AtStop::Kept(KeptAtStop::Waking);
            };
            drop(turn);
            let handed = self.drain_held(address.clone(), &lease);
            return match handed {
                Outcome::Drained(mut typed) if !typed.is_empty() => {
                    self.drain_due_held(&address, lease);
                    AtStop::Drained(typed.remove(0))
                }
                outcome => AtStop::Kept(KeptAtStop::DrainRefused(Box::new(outcome))),
            };
        }
        if refresh_mode == meta_signal_flow::RefreshMode::Manual {
            return AtStop::Idle;
        }
        // With nothing due, how the metaflow continues decides: a
        // Maintained seat stays Awake in its pane; an OnDemand seat closes
        // its harness, its Address and transcript kept, once it can be
        // closed without ending work its harness still runs.
        match continuity {
            Continuity::Maintained => AtStop::Idle,
            Continuity::OnDemand if !closable => AtStop::Idle,
            // Its successor is composed at its next wake: the seat is
            // closed only once that successor composes now.
            // Only its sleep is declined then: no successor was refused.
            Continuity::OnDemand => match self.successor_composes(flow_id, &metaflow) {
                Err(KeptAtStop::RefreshUnplaceable) => AtStop::Kept(KeptAtStop::SleepUnplaceable),
                Err(KeptAtStop::SuccessorUncomposable(outcome)) => {
                    AtStop::Kept(KeptAtStop::SleepUncomposable(outcome))
                }
                Err(kept) => AtStop::Kept(kept),
                Ok(()) => match self.close_at_rest(flow_id, &node) {
                    Ok(()) => AtStop::Slept(address),
                    Err(outcome) => AtStop::Kept(KeptAtStop::SleepRefused(outcome)),
                },
            },
        }
    }
}

impl crate::generated::overriding::ResumesPermaflowWork for RunningNexus {
    fn resume_permaflow<N: crate::generated::overriding::Addressed>(&self, input: N) -> bool {
        use crate::generated::overriding::{
            RecoversRecordedRestarts, ResumesHeldLaunches, ResumesPermaflowCleanup,
            SerializesPermaflowEffects,
        };
        use crate::reaping::ReapsPredecessors;
        use crate::store::restart::RecordsRestarts;
        let address = input.address();
        let _effect = self.waking.effect(address.clone());
        if self.held() || self.permaflow_held(address.clone()) {
            return false;
        }
        // Only this Address's deferred work is taken up by its release.
        self.resume_permaflow_cleanup(address.clone());
        self.resume_held_launches(address.clone());
        for record in self.store.restart_records().unwrap_or_default() {
            if record.address == address {
                self.recover_restart(record);
            }
        }
        if let Ok(Some(metaflow)) = self.store.metaflow(&address)
            && let flow_ethos::State::Awake(flow_id) = metaflow.state
        {
            self.settle_stopped(&flow_id.0);
        }
        self.settle_orphaned_order(&address);
        if let Ok(Some(intent)) = self.store.successor_intent(&address)
            && matches!(self.store.metaflow(&address), Ok(Some(metaflow))
                if metaflow.state == flow_ethos::State::Awake(FlowId(intent.predecessor.0.clone())))
        {
            self.take_up_owed(intent);
        }
        for row in self.store.pending_reaps().unwrap_or_default() {
            if row.address == address {
                self.rearm(&row.predecessor.0);
            }
        }
        self.wake_due(address);
        true
    }
}
