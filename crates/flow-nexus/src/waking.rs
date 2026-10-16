//! Sleep and wake: a seat that is not running keeps no harness.
//!
//! A metaflow is Awake as one flow. When that flow's harness reports
//! `Stopped`, its turn has ended, and Flow first writes how big the flow's
//! context was (`Record.Context`, read once from its native transcript).
//! It gives Herdr a bounded wait to show the seat Idle or Done, since a
//! harness's Stop hook can run while its screen still shows it working.
//! Then, if Herdr shows the seat resting: with work queued, the oldest
//! request is typed into it (`Operation::Drain`), and the seat stays awake
//! until it stops again with nothing queued; with nothing queued, Flow
//! closes its pane and records the metaflow Asleep (`Operation::Sleep`), but
//! only when the seat can be resumed. The flow's row keeps its native
//! session. A request later queued for the Asleep metaflow wakes it
//! (`Operation::Wake`): the whole queue, oldest first, is handed to a seat in
//! one prompt, by resuming the last flow's native session in a new pane when
//! its context was under its layer's refresh threshold, or else by a fresh
//! launch composed from the configured layer Model and topic modules. The
//! requests leave the queue only in the write that records the started flow
//! Awake, so a Nexus that dies mid-wake loses none: at its next start it
//! wakes every Asleep metaflow still holding work. Requests that arrive while
//! the wake runs are queued, and typed into the woken flow's pane once it is
//! up.
//!
//! Each unruled choice of when to sleep, which requests wake, which layers
//! sleep, and when to resume is one function of [`WakePolicy`], so a ruling
//! changes one place.
//!
//! A wake runs on the connection thread that queued the request, after its
//! answer is written, off the dispatch gate: it takes the gate for each store
//! step and releases it while a fresh launch settles, as Start does. While it
//! runs, the metaflow is held in [`WakingMetaflows`], so a second request
//! starts no second wake and a Stop does not put the waking seat to sleep.

use crate::AwaitsLaunchSettlement;
use crate::RunningNexus;
use crate::claude::MeasuresClaudeContext;
use crate::codex::SelectsCodexEndpoint;
use crate::composition::{
    ComposesFreshSeat, FreshSeat, KeepsLaunchBundles, OpensLaunchComposer, RendersQueuedWork,
    ResumesLaunch,
};
use crate::delivery::lease::LeasesPanes;
use crate::delivery::{FindsDeliveryTarget, ReadsLeasedPane};
use crate::generated::operation::{
    Failed_Data, Operation, Outcome, Record_Data, Record_Data_Asleep_Data, Record_Data_Awoken_Data,
    Record_Data_Context_Data, Sleep_Data, Title_Data,
};
use crate::herdr::launch::{ObservesNativeLaunchBinding, ResumesNativeHerdrHarness};
use crate::herdr::pane::{PaneAgent, Placement, WritesPane};
use crate::herdr::{LocatesNativeTranscripts, ReadsHerdrPanes, ReadsHerdrRoster};
use crate::launching::LaunchesFlows;
use crate::performing::Performs;
use crate::store::address_hold::HoldsLaunchAddress;
use crate::store::context::RecordsContextSize;
use crate::store::events::RecordsHarnessEvents;
use crate::store::metaflow::SleepsMetaflows;
use crate::store::{
    ManagesMetaflowState, NamesLiveFlow, ReadsFlowRows, ReadsLaunchAttempt, RecordsFlowLifecycle,
};
use signal_flow::{
    Address, AgentState, Event, FlowId, FlowLifecycle, FlowNode, HarnessKind, HerdrRoute,
    HerdrRouteSelection, LaunchProfile, Layer, Metaflow, OriginClue, PowerLevel, Request, Response,
    StartRequest,
};
use std::collections::HashSet;
use std::sync::Mutex;

/// The unruled choices of sleep and wake, each in one place.
pub trait WakePolicy {
    /// Effort a fresh seat is launched at: every seat runs at medium.
    const WAKE_EFFORT: &'static str = "medium";

    /// A flow that reported Stopped sleeps at once when Herdr shows it
    /// resting: Idle or Done. Working, Blocked or unreadable keeps it awake.
    fn sleeps_at(agent_state: &AgentState) -> bool {
        matches!(agent_state, AgentState::Idle | AgentState::Done)
    }

    /// Work and the living's words wake a seat; a Result or a Notice waits
    /// queued for its next wake.
    fn wakes_for(request: &Request) -> bool {
        matches!(
            request,
            Request::Order(_) | Request::Question(_) | Request::Psyche(_) | Request::Psyches(_)
        )
    }

    /// A Primary is never woken automatically: what is queued for it waits.
    fn wakes_layer(layer: &Layer) -> bool {
        *layer != Layer::Primary
    }

    /// A Primary is never put to sleep either, since nothing wakes it.
    fn sleeps_layer(layer: &Layer) -> bool {
        Self::wakes_layer(layer)
    }

    /// A seat can be resumed when it is a Claude flow with a native session
    /// (its transcript) that was not retired. Codex seats cannot be resumed
    /// yet. A Retired flow, and an Exited one, whose pane went away without
    /// Flow closing it, stay in Past as provenance, and their seat wakes
    /// fresh: only a Stopped flow is resumed (`record_resumed`).
    fn can_resume(node: &FlowNode) -> bool {
        node.harness_kind == HarnessKind::Claude
            && !node.session_id.is_empty()
            && !matches!(
                node.flow_lifecycle,
                FlowLifecycle::Retired | FlowLifecycle::Exited
            )
    }

    /// A seat sleeps only if it can be resumed, so no saved context is
    /// replaced by a fresh launch for want of a way back to it. A seat too
    /// big to resume still sleeps: it wakes fresh.
    fn sleeps_seat(node: &FlowNode) -> bool {
        Self::can_resume(node)
    }

    /// A resumable seat is resumed unless its context at its last Stop is
    /// known to be at or over its layer's refresh threshold; then it wakes
    /// fresh. An unread size or an unconfigured threshold resumes, since
    /// nothing shows the context too big.
    fn resumes(context_tokens: Option<i64>, refresh_threshold: Option<i64>) -> bool {
        match (context_tokens, refresh_threshold) {
            (Some(tokens), Some(threshold)) => tokens < threshold,
            _ => true,
        }
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
    /// Retired or Exited and that holds no Address hold. True when it was.
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
            self.store.flow_node(&flow_id),
            Ok(Some(node)) if !node.flow_lifecycle.is_live()
        );
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
    /// the last event its harness reported is `Stopped`.
    fn note_stop_at_confirmation(&self, flow_id: &str);
    /// Off the dispatch gate: settles every noted flow's Stop.
    fn settle_stops_at_confirmation(&self);
}

impl SettlesStopsAtConfirmation for RunningNexus {
    fn note_stop_at_confirmation(&self, flow_id: &str) {
        if let Ok(Some(events)) = self.store.events(flow_id)
            && events.last() == Some(&Event::Stopped)
        {
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
    /// The last flow's native session, started again in a new pane.
    Resume {
        node: Box<FlowNode>,
        profile: Box<LaunchProfile>,
    },
    /// A new flow, launched from configuration.
    Fresh { herdr_session_name: String },
    /// Nothing names a Herdr session to wake the seat in.
    Unplaceable,
}

/// What a flow's Stop did to its seat.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AtStop {
    /// The seat rested with nothing queued and could be resumed: it sleeps.
    Slept,
    /// The seat rested with work queued: the oldest request was typed in.
    Drained(Request),
    /// The seat stays as it is, for the reason named.
    Kept(KeptAtStop),
}

/// Why a Stop left its seat as it was. Every early exit of `settle_stopped`
/// names one, and the Nexus logs it, so a seat that does not sleep says why.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KeptAtStop {
    /// The store could not be read.
    StoreUnreadable,
    /// No metaflow is Awake as this flow.
    NotAwake,
    /// The flow's launch is not confirmed: it only holds its Address.
    Unconfirmed,
    /// The metaflow's layer never sleeps.
    LayerNeverSleeps,
    /// A wake holds the metaflow.
    Waking,
    /// Flow holds no row for the flow.
    UnknownFlow,
    /// After the bounded wait, Herdr does not show the seat resting.
    NotResting(PaneAgent),
    /// Work is queued and the Drain did not type it.
    DrainRefused(Box<Outcome>),
    /// The seat cannot be resumed, so it is not put to sleep.
    NotResumable,
    /// A lock is held for the metaflow, or the locks could not be read.
    LockHeld,
    /// The Sleep did not complete.
    SleepRefused(Box<Outcome>),
}

/// The triggers: what a Stopped report, a queued request and a Nexus start
/// set going.
pub trait SleepsAndWakesSeats {
    /// After a flow's `Stopped` report: writes its context size, then types
    /// the oldest queued request into its resting seat, or puts the
    /// metaflow Awake as it to sleep when nothing waits for it.
    fn settle_stopped(&self, flow_id: &str) -> AtStop;
    /// After a request was queued for an Asleep metaflow: wakes it and
    /// drains what arrived meanwhile. Nothing when a wake already runs.
    fn wake_due(&self, address: Address);
    /// Types what arrived during a wake into the woken flow, oldest first,
    /// until the queue is seen empty under the gate; the hold is released
    /// there.
    fn drain_woken(&self, address: &Address, lease: WakingLease<'_>);
    /// Which way an Asleep metaflow is woken.
    fn wake_plan(&self, metaflow: &Metaflow) -> WakePlan;
    /// At the Nexus's start: wakes every Asleep metaflow still holding work
    /// that wakes it, as a wake the last Nexus did not finish left it.
    fn recover_wakes(&self);
}

/// The bodies of the Sleep, Wake and Drain operations; `perform` delegates
/// to them.
pub trait PerformsSleepAndWake {
    fn sleep(&self, sleep: Sleep_Data) -> Outcome;
    /// Performed off the dispatch gate; see the module documentation.
    fn wake(&self, address: Address) -> Outcome;
    /// Performed under the dispatch gate.
    fn drain(&self, address: Address) -> Outcome;
}

/// The steps a wake takes.
trait WakesInParts {
    fn resume(
        &self,
        node: FlowNode,
        profile: LaunchProfile,
        text: &str,
    ) -> Result<FlowId, Failed_Data>;
    fn start_fresh(
        &self,
        metaflow: &Metaflow,
        herdr_session_name: String,
        text: &str,
    ) -> Result<FlowId, Failed_Data>;
    /// Types text into a live flow's pane under its lease, as Flow's own
    /// write: no Priority head, since it is not a Message.
    fn hand_over(&self, flow_id: &str, text: &str) -> Result<(), Failed_Data>;
    fn last_launch_profile(&self, flow_id: &str) -> Option<LaunchProfile>;
    /// Writes the context size the flow's native transcript shows now.
    fn record_context_at_stop(&self, flow_id: &str);
    /// The refresh threshold configured for this layer, in tokens.
    fn refresh_threshold(&self, layer: &Layer) -> Option<i64>;
    /// Refuses a wake while a pane already runs this flow's native session:
    /// ConflictingBinding when one does, HerdrRefused when Herdr is
    /// unreadable.
    fn refuse_live_session(&self, node: &FlowNode, sessions: &[String]) -> Result<(), Failed_Data>;
    fn now() -> i64;
    /// The body of `settle_stopped`: waits, boundedly and off the dispatch
    /// gate, for Herdr to show the seat resting, then drains or sleeps it.
    fn settle_rested(&self, flow_id: &str) -> AtStop;
}

impl SleepsAndWakesSeats for RunningNexus {
    fn settle_stopped(&self, flow_id: &str) -> AtStop {
        let at_stop = self.settle_rested(flow_id);
        if let AtStop::Kept(reason) = &at_stop {
            eprintln!("flow-nexus: flow {flow_id} kept at its Stop: {reason:?}");
        }
        at_stop
    }

    fn wake_due(&self, address: Address) {
        let Some(lease) = self.waking.hold(&address) else {
            return;
        };
        match self.perform(Operation::Wake(address.clone())) {
            Outcome::Woken(_) => self.drain_woken(&address, lease),
            outcome => eprintln!("flow-nexus: a queued request did not wake its seat: {outcome:?}"),
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
        let last = metaflow.past.last();
        let node = last.and_then(|flow_id| self.store.flow_node(flow_id).ok().flatten());
        let profile = last.and_then(|flow_id| self.last_launch_profile(flow_id));
        if let (Some(node), Some(profile)) = (&node, &profile)
            && Self::can_resume(node)
            && profile.harness_kind == HarnessKind::Claude
            && Self::resumes(
                self.store.context_size(&node.flow_id).ok().flatten(),
                self.refresh_threshold(&metaflow.address.layer),
            )
        {
            return WakePlan::Resume {
                node: Box::new(node.clone()),
                profile: Box::new(profile.clone()),
            };
        }
        let route_session = node.and_then(|node| match node.herdr_route_selection {
            HerdrRouteSelection::Available(route) => Some(route.herdr_session_name),
            HerdrRouteSelection::Unavailable => None,
        });
        match profile
            .map(|profile| profile.herdr_session_name)
            .or(route_session)
        {
            Some(herdr_session_name) => WakePlan::Fresh { herdr_session_name },
            None => WakePlan::Unplaceable,
        }
    }

    fn recover_wakes(&self) {
        let due = {
            let _turn = self
                .dispatch_gate
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            self.rest_every_ended_awake();
            match self.store.list_metaflows() {
                Ok(metaflows) => metaflows
                    .into_iter()
                    .filter(|metaflow| {
                        metaflow.state == flow_ethos::State::Asleep
                            && Self::wakes_layer(&metaflow.address.layer)
                            && metaflow.queue.iter().any(Self::wakes_for)
                    })
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

impl PerformsSleepAndWake for RunningNexus {
    fn sleep(&self, sleep: Sleep_Data) -> Outcome {
        let Sleep_Data { address, flow_node } = sleep;
        let flow_id = flow_node.flow_id.clone();
        let HerdrRouteSelection::Available(route) = &flow_node.herdr_route_selection else {
            return Outcome::Failed(Failed_Data::HerdrRefused);
        };
        // No write is left half-typed into a pane as it closes.
        let closed = {
            let _lease = self.pane_leases.hold(route);
            self.perform(Operation::Close(flow_node.clone()))
        };
        if closed != Outcome::Closed {
            return closed;
        }
        // The flow's Stop records its metaflow Asleep in the same write.
        if self.perform(Operation::Record(Record_Data::Stopped(flow_id.clone())))
            != Outcome::Recorded
        {
            return Outcome::Failed(Failed_Data::StoreRefused);
        }
        match self.store.metaflow(&address) {
            Ok(Some(metaflow))
                if metaflow.state == flow_ethos::State::Asleep
                    && metaflow.past.last() == Some(&flow_id) =>
            {
                Outcome::Slept
            }
            _ => Outcome::Failed(Failed_Data::NotAwake),
        }
    }

    fn wake(&self, address: Address) -> Outcome {
        let (metaflow, requests) = {
            let _turn = self
                .dispatch_gate
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            self.rest_ended_awake(&address);
            match self.store.metaflow(&address) {
                Ok(Some(metaflow)) if metaflow.state == flow_ethos::State::Asleep => {
                    if metaflow.queue.is_empty() {
                        return Outcome::Failed(Failed_Data::NothingQueued);
                    }
                    // Read, not taken: the requests leave the queue only
                    // when the started flow is recorded Awake.
                    let requests = metaflow.queue.clone();
                    (metaflow, requests)
                }
                Ok(_) => return Outcome::Failed(Failed_Data::NotAsleep),
                Err(_) => return Outcome::Failed(Failed_Data::StoreRefused),
            }
        };
        let Ok(delivered) = i64::try_from(requests.len()) else {
            return Outcome::Failed(Failed_Data::StoreRefused);
        };
        let text = requests.prompt_text();
        let plan = self.wake_plan(&metaflow);
        // The last flow's session must not be running already: a pane left
        // from a wake the last Nexus did not record, or one started by hand.
        if let Some(node) = metaflow
            .past
            .last()
            .and_then(|flow_id| self.store.flow_node(flow_id).ok().flatten())
            .filter(|node| !node.session_id.is_empty())
        {
            let mut sessions = Vec::new();
            if let WakePlan::Resume { profile, .. } = &plan {
                sessions.push(profile.herdr_session_name.clone());
            }
            if let WakePlan::Fresh { herdr_session_name } = &plan {
                sessions.push(herdr_session_name.clone());
            }
            if let HerdrRouteSelection::Available(route) = &node.herdr_route_selection {
                sessions.push(route.herdr_session_name.clone());
            }
            if let Err(failure) = self.refuse_live_session(&node, &sessions) {
                return Outcome::Failed(failure);
            }
        }
        let woken = match plan {
            WakePlan::Resume { node, profile } => {
                let _turn = self
                    .dispatch_gate
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                self.resume(*node, *profile, &text)
            }
            WakePlan::Fresh { herdr_session_name } => {
                self.start_fresh(&metaflow, herdr_session_name, &text)
            }
            WakePlan::Unplaceable => Err(Failed_Data::HerdrRefused),
        };
        let _turn = self
            .dispatch_gate
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        match woken {
            Ok(flow_id) => match self.perform(Operation::Record(Record_Data::Awoken(
                Record_Data_Awoken_Data {
                    address,
                    flow_id: flow_id.clone(),
                    delivered,
                },
            ))) {
                Outcome::Recorded => Outcome::Woken(flow_id),
                _ => {
                    eprintln!("flow-nexus: flow {flow_id} woke but is not recorded Awake");
                    Outcome::Failed(Failed_Data::StoreRefused)
                }
            },
            // Nothing was taken: the work is still queued, oldest first.
            Err(failure) => Outcome::Failed(failure),
        }
    }

    fn drain(&self, address: Address) -> Outcome {
        let (flow_id, oldest) = match self.store.metaflow(&address) {
            Ok(Some(Metaflow {
                state: flow_ethos::State::Awake(flow_id),
                queue,
                ..
            })) => (flow_id, queue.into_iter().next()),
            Ok(_) => return Outcome::Failed(Failed_Data::NotAwake),
            Err(_) => return Outcome::Failed(Failed_Data::StoreRefused),
        };
        let Some(oldest) = oldest else {
            return Outcome::Drained(Vec::new());
        };
        let typed = vec![oldest];
        if let Err(failure) = self.hand_over(&flow_id, &typed.prompt_text()) {
            return Outcome::Failed(failure);
        }
        // Typed (or possibly typed): it is never typed twice.
        match self.store.remove_oldest_queued(&address, &typed[0]) {
            Ok(true) => Outcome::Drained(typed),
            _ => {
                eprintln!("flow-nexus: work typed into flow {flow_id} is still queued");
                Outcome::Failed(Failed_Data::StoreRefused)
            }
        }
    }
}

impl WakesInParts for RunningNexus {
    fn resume(
        &self,
        node: FlowNode,
        profile: LaunchProfile,
        text: &str,
    ) -> Result<FlowId, Failed_Data> {
        // The bundle copy the flow was launched with is kept while it sleeps.
        if !self.composer.launch_bundles().file_for(&profile).is_file() {
            return Err(Failed_Data::BundleRefused);
        }
        let launch = profile.resumption(text);
        let pane = match self.perform(Operation::Open(launch.clone())) {
            Outcome::Opened(pane) => pane,
            Outcome::Failed(failure) => return Err(failure),
            _ => return Err(Failed_Data::HerdrRefused),
        };
        if let Err(error) =
            self.herdr
                .resume_native_harness(&launch, &pane, &node.flow_id, &node.session_id)
        {
            // Adapter text may carry private paths; only the fact is logged.
            let _ = error;
            eprintln!("flow-nexus: flow {} not resumed by Herdr", node.flow_id);
            return Err(Failed_Data::HerdrRefused);
        }
        // The resumed harness must come up as the same session, and its
        // claim must still name this FlowId: the flow keeps its identity or
        // the resume fails, never silently taking another.
        let binding = self
            .herdr
            .observe_native_binding(&launch, &pane, Some(&node.flow_id))
            .map_err(|_| Failed_Data::ConflictingBinding)?;
        if binding.native_session_id != node.session_id || binding.flow_id != node.flow_id {
            return Err(Failed_Data::ConflictingBinding);
        }
        let route = HerdrRoute {
            herdr_session_name: pane.herdr_session_name.clone(),
            herdr_agent_name: pane.herdr_agent_name.clone(),
            herdr_pane_id: pane.herdr_pane_id.clone(),
            herdr_terminal_id: pane.herdr_terminal_id.clone(),
        };
        if !self
            .store
            .record_resumed(&node.flow_id, route)
            .unwrap_or(false)
        {
            return Err(Failed_Data::StoreRefused);
        }
        if self.perform(Operation::Title(Title_Data {
            composed_launch: launch,
            native_launch_binding: binding,
        })) != Outcome::Titled
        {
            eprintln!(
                "flow-nexus: resumed flow {} keeps its pane untitled",
                node.flow_id
            );
        }
        self.hand_over(&node.flow_id, text)?;
        Ok(node.flow_id)
    }

    fn start_fresh(
        &self,
        metaflow: &Metaflow,
        herdr_session_name: String,
        text: &str,
    ) -> Result<FlowId, Failed_Data> {
        let address = &metaflow.address;
        let snapshot = match self.store.configuration_snapshot() {
            Ok(Some(snapshot)) => snapshot,
            _ => return Err(Failed_Data::StoreRefused),
        };
        let Some(model) = snapshot
            .models
            .iter()
            .find(|model| model.layer == address.layer)
        else {
            return Err(Failed_Data::CompositionRefused);
        };
        let harness_kind = match self.codex_endpoints.adapter_for(&model.native) {
            Ok(_) => HarnessKind::Codex,
            Err(_) => HarnessKind::Claude,
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
            model_name: model.native.clone(),
            harness_kind,
            power_level: Self::power_of(&address.layer),
            effort: Self::WAKE_EFFORT.into(),
            herdr_session_name,
            modules: snapshot
                .modules
                .iter()
                .filter(|module| module.key.topic == address.topic)
                .map(|module| module.source.clone())
                .collect(),
            prompt: text.to_owned(),
        };
        let profile = self
            .composer
            .fresh_profile(&seat)
            .map_err(|_| Failed_Data::CompositionRefused)?;
        let request = StartRequest {
            launch_profile: profile,
            origin_clue: OriginClue {
                flow_id: metaflow
                    .past
                    .last()
                    .cloned()
                    .unwrap_or_else(|| "flow-nexus".into()),
                session_id: "flow-nexus-wake".into(),
                turn_id: launch_request_id.clone(),
            },
        };
        let response = {
            let _turn = self
                .dispatch_gate
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let response = self.start(request);
            self.settle(&launch_request_id, response)
        };
        // The gate is released before the wait: the launch is settled by the
        // Nexus's own promoter, which needs the gate to do it.
        let response = match response {
            Response::StartAmbiguous(_) => self.settled_launch(&launch_request_id),
            response => Some(response),
        };
        match response {
            Some(Response::Started(launched)) => Ok(launched.flow_id),
            _ => Err(Failed_Data::Unstarted),
        }
    }

    fn hand_over(&self, flow_id: &str, text: &str) -> Result<(), Failed_Data> {
        let target = self
            .delivery_target(flow_id)
            .map_err(|_| Failed_Data::HerdrRefused)?;
        let _lease = self.pane_leases.hold(&target.route);
        let agent_state = self
            .writable_state(&target)
            .map_err(|_| Failed_Data::HerdrRefused)?;
        let observe = matches!(agent_state, AgentState::Idle | AgentState::Done);
        match self.herdr.place(&target.route, text, observe) {
            Placement::Placed { observed: true } if self.herdr.route_is_available(&target.node) => {
                let _ = self.store.record_active(flow_id);
                Ok(())
            }
            Placement::Placed { .. } => Ok(()),
            // It may have reached the pane: it is never typed twice.
            Placement::Uncertain => {
                eprintln!("flow-nexus: work typed into flow {flow_id}, reaction unknown");
                Ok(())
            }
            Placement::Refused => Err(Failed_Data::HerdrRefused),
        }
    }

    fn last_launch_profile(&self, flow_id: &str) -> Option<LaunchProfile> {
        let launch_request_id = self.store.launch_requests_bound_to(flow_id).ok()?.pop()?;
        self.store
            .launch_attempt(&launch_request_id)
            .ok()
            .flatten()
            .map(|attempt| attempt.launch_profile)
    }

    fn record_context_at_stop(&self, flow_id: &str) {
        let Ok(Some(node)) = self.store.flow_node(flow_id) else {
            return;
        };
        if node.harness_kind != HarnessKind::Claude {
            return;
        }
        let tokens = self
            .herdr
            .native_transcript_root(&HarnessKind::Claude, "")
            .ok()
            .and_then(|root| root.session_transcript(&node.session_id))
            .and_then(|transcript| transcript.context_tokens());
        let Some(tokens) = tokens else {
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

    fn refresh_threshold(&self, layer: &Layer) -> Option<i64> {
        self.store
            .configured_threshold(layer)
            .ok()
            .flatten()
            .map(|threshold| threshold.refresh)
    }

    fn refuse_live_session(&self, node: &FlowNode, sessions: &[String]) -> Result<(), Failed_Data> {
        let mut checked = HashSet::new();
        for herdr_session_name in sessions {
            if !checked.insert(herdr_session_name) {
                continue;
            }
            match self
                .herdr
                .holds_native_session(herdr_session_name, &node.session_id)
            {
                Some(false) => {}
                Some(true) => {
                    eprintln!(
                        "flow-nexus: flow {} is already running; not woken again",
                        node.flow_id
                    );
                    return Err(Failed_Data::ConflictingBinding);
                }
                None => return Err(Failed_Data::HerdrRefused),
            }
        }
        Ok(())
    }

    fn settle_rested(&self, flow_id: &str) -> AtStop {
        self.record_context_at_stop(flow_id);
        // Claude runs its Stop hook while its spinner is still on screen,
        // and Herdr reads its state from the screen: the seat is given a
        // bounded wait to show resting before the gate is taken.
        if let Ok(Some(node)) = self.store.flow_node(flow_id)
            && let HerdrRouteSelection::Available(route) =
                &self.herdr.refresh_route(node).herdr_route_selection
        {
            self.herdr.rested(route);
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
        // it neither sleeps nor drains until confirmation keeps it.
        match self.store.address_hold(flow_id) {
            Ok(None) => {}
            Ok(Some(_)) => return AtStop::Kept(KeptAtStop::Unconfirmed),
            Err(_) => return AtStop::Kept(KeptAtStop::StoreUnreadable),
        }
        let address = metaflow.address.clone();
        if !Self::sleeps_layer(&address.layer) {
            return AtStop::Kept(KeptAtStop::LayerNeverSleeps);
        }
        if self.waking.is_waking(&address) {
            return AtStop::Kept(KeptAtStop::Waking);
        }
        let node = match self.store.flow_node(flow_id) {
            Ok(Some(node)) => self.herdr.refresh_route(node),
            Ok(None) => return AtStop::Kept(KeptAtStop::UnknownFlow),
            Err(_) => return AtStop::Kept(KeptAtStop::StoreUnreadable),
        };
        match self.herdr.pane_agent(&node) {
            PaneAgent::Present { agent_state, .. } if Self::sleeps_at(&agent_state) => {}
            pane_agent => return AtStop::Kept(KeptAtStop::NotResting(pane_agent)),
        }
        if !metaflow.queue.is_empty() {
            return match self.perform(Operation::Drain(address)) {
                Outcome::Drained(mut typed) if !typed.is_empty() => {
                    AtStop::Drained(typed.remove(0))
                }
                outcome => AtStop::Kept(KeptAtStop::DrainRefused(Box::new(outcome))),
            };
        }
        if !Self::sleeps_seat(&node) {
            return AtStop::Kept(KeptAtStop::NotResumable);
        }
        if self.store.lock_held(&address, Self::now()).unwrap_or(true) {
            return AtStop::Kept(KeptAtStop::LockHeld);
        }
        match self.perform(Operation::Sleep(Sleep_Data {
            address,
            flow_node: node,
        })) {
            Outcome::Slept => AtStop::Slept,
            outcome => AtStop::Kept(KeptAtStop::SleepRefused(Box::new(outcome))),
        }
    }

    fn now() -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .ok()
            .and_then(|elapsed| i64::try_from(elapsed.as_secs()).ok())
            .unwrap_or(0)
    }
}
