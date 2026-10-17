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
//! launch from the configured layer Model and the module identities the
//! metaflow's last launch named, composed again as any Start is. The
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
//! Whether a metaflow is due a wake is read from Flow's records alone
//! ([`SleepsAndWakesSeats::wake_is_due`]): it is Asleep, its layer wakes, and
//! it holds work that wakes it, or its last flow is still live. Nothing
//! carried beside a commit decides it, so every path that may leave a wake
//! owed asks the same question: the connection or send-tool responder that
//! queued a request, after its answer was written or failed to be, a
//! replayed send call answered from its record, and the Nexus's start.
//!
//! A wake runs on the thread that asked, off the dispatch gate: it takes the
//! gate for each store step and releases it while a fresh launch settles, as
//! Start does. While it runs, the metaflow is held in [`WakingMetaflows`], so
//! a second request starts no second wake and a Stop does not put the waking
//! seat to sleep.

use crate::AwaitsLaunchSettlement;
use crate::RunningNexus;
use crate::claude::MeasuresClaudeContext;
use crate::codex::{SelectsCodexEndpoint, SubmitsQueuedCodexTurn};
use crate::codex_turns::{SettlesRestingCodexSeats, TracksCodexTurns};
use crate::composition::{
    ComposesLaunch, CompositionError, FreshSeat, HarnessedProfile, RendersQueuedWork,
};
use crate::delivery::lease::LeasesPanes;
use crate::delivery::{FindsDeliveryTarget, PaneRefusal, ReadsLeasedPane, TargetRefusal};
use crate::generated::operation::{
    Failed_Data, Operation, Outcome, Record_Data, Record_Data_Asleep_Data, Record_Data_Awoken_Data,
    Record_Data_Context_Data, Sleep_Data, Title_Data,
};
use crate::generated::starter::DerivesHarness;
use crate::generated::starter::ProfilesFreshSeat;
use crate::herdr::launch::{
    ObservesNativeLaunchBinding, ResumesNativeHerdrHarness, WaitsForPaneRest,
};
use crate::herdr::pane::{PaneAgent, Placement, WritesPane};
use crate::herdr::{
    LocatesNativeTranscripts, OwnSession, PanePresence, ReadsHerdrPanes, ReadsHerdrRoster,
};
use crate::launching::LaunchesFlows;
use crate::performing::Performs;
use crate::send_tool::ServesLaunchCalls;
use crate::store::address_hold::HoldsLaunchAddress;
use crate::store::context::RecordsContextSize;
use crate::store::events::RecordsHarnessEvents;
use crate::store::metaflow::SleepsMetaflows;
use crate::store::queue_delivery::{
    AccountsQueueDelivery, NativeDeliveryReceipt, PrepareQueueDelivery,
    QueueDeliveryPhase,
};
use crate::store::{
    ManagesMetaflowState, NamesLiveFlow, ReadsFlowRoles, ReadsFlowRows,
    ReadsLaunchAttempt, RecordsFlowLifecycle,
};
use signal_flow::{
    Address, AgentState, ContextRefusal, Event, FlowId, FlowLifecycle, FlowNode, HarnessKind,
    HerdrPaneBinding, HerdrRoute, HerdrRouteSelection, LaunchProfile, Layer, Metaflow, Origin,
    PowerLevel, QueuedRequest, Request, Response,
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
    /// the last event its harness reported is `Stopped`, or, for a Codex
    /// flow, when its thread is resting.
    fn note_stop_at_confirmation(&self, flow_id: &str);
    /// Off the dispatch gate: settles every noted flow's Stop.
    fn settle_stops_at_confirmation(&self);
}

impl SettlesStopsAtConfirmation for RunningNexus {
    fn note_stop_at_confirmation(&self, flow_id: &str) {
        // A Codex flow reports no Stop: its thread's rest, as its
        // app-server's events showed it, stands for one.
        let stopped = matches!(
            self.store.events(flow_id),
            Ok(Some(events)) if events.last() == Some(&Event::Stopped)
        );
        if stopped || self.codex_resting(flow_id) {
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
    /// The seat rested with nothing queued and could be resumed: it sleeps.
    Slept,
    /// The seat rested with work queued: the oldest request was typed in.
    Drained(QueuedRequest),
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
    /// The Codex seat's app-server shows its thread in a turn, or holding
    /// work Flow typed whose turn has not begun.
    InTurn,
    /// Work is queued and the Drain did not type it.
    DrainRefused(Box<Outcome>),
    /// The seat cannot be resumed, so it is not put to sleep.
    NotResumable,
    /// A lock is held for the metaflow, or the locks could not be read.
    LockHeld,
    /// The Sleep did not complete.
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
    /// A pane that is not the flow's own recorded pane runs its native
    /// session.
    SessionLive,
    /// Herdr could not be read to see whether the session runs.
    HerdrUnreadable,
    /// Nothing names a Herdr session to wake the seat in.
    Unplaceable,
    /// The resumed flow's system prompt was not composed again.
    ContextRefused(ContextRefusal),
    /// The resumed flow's launch profile no longer composes.
    NotComposed,
    /// No pane was opened for the resumed session.
    PaneNotOpened(Failed_Data),
    /// Herdr did not start the native session again in the new pane.
    HarnessNotResumed,
    /// The resumed harness's binding could not be read.
    BindingUnread,
    /// The resumed harness came up as another session or FlowId.
    BindingDiffers,
    /// The resumed flow's new route was not recorded.
    ResumeNotRecorded,
    /// The resumed pane did not come to rest before the work was typed.
    NotResting,
    /// The work was not typed into the resumed pane.
    HandOver(HandOverRefusal),
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
            Self::StoreUnreadable | Self::ResumeNotRecorded | Self::AwakeNotRecorded => {
                Failed_Data::StoreRefused
            }
            Self::NotAsleep => Failed_Data::NotAsleep,
            Self::NothingQueued => Failed_Data::NothingQueued,
            Self::SessionLive | Self::BindingUnread | Self::BindingDiffers => {
                Failed_Data::ConflictingBinding
            }
            Self::ContextRefused(refusal) => Failed_Data::ContextRefused(refusal.clone()),
            Self::NotComposed => Failed_Data::CompositionRefused,
            Self::PaneNotOpened(failure) | Self::FreshNotStarted(failure) => failure.clone(),
            Self::HerdrUnreadable
            | Self::Unplaceable
            | Self::HarnessNotResumed
            | Self::NotResting
            | Self::HandOver(_) => Failed_Data::HerdrRefused,
        }
    }
}

/// The triggers: what a Stopped report, a queued request and a Nexus start
/// set going.
pub trait SleepsAndWakesSeats {
    /// After a flow's `Stopped` report: writes its context size, then types
    /// the oldest queued request into its resting seat, or puts the
    /// metaflow Awake as it to sleep when nothing waits for it.
    fn settle_stopped(&self, flow_id: &str) -> AtStop;
    /// After a request was queued for `address`, or a queued one was
    /// answered again: holds the metaflow, and when Flow's records show it
    /// due a wake, wakes it and drains what arrived meanwhile. Nothing when
    /// a wake already runs or none is due, so asking twice wakes once.
    fn wake_due(&self, address: Address);
    /// Whether Flow's records show the metaflow due a wake: Asleep, of a
    /// layer that wakes, holding work that wakes it, or with its last flow
    /// still live, as a wake that failed after resuming its session left it.
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
    /// The body of `wake`, every early exit a typed refusal.
    fn waken(&self, address: &Address) -> Result<FlowId, WakeRefusal>;
    /// The last flow of an Asleep metaflow, when it is still live in the
    /// pane Flow recorded for it, running its own native session under its
    /// claim: a wake that resumed it and failed before recording it Awake.
    fn live_last_flow(&self, metaflow: &Metaflow) -> Option<FlowNode>;
    /// Records such a flow Awake, taking nothing from the queue; what is
    /// queued is then typed in as work that arrived during a wake.
    fn settle_live_resumed(&self, address: &Address, flow_id: &str) -> Result<FlowId, WakeRefusal>;
    fn resume(
        &self,
        node: FlowNode,
        profile: LaunchProfile,
        text: &str,
    ) -> Result<FlowId, WakeRefusal>;
    /// The steps of a resume once its pane is open.
    fn resume_in_pane(
        &self,
        node: &FlowNode,
        launch: &signal_flow::ComposedLaunch,
        pane: &HerdrPaneBinding,
        text: &str,
    ) -> Result<(), WakeRefusal>;
    /// Closes a pane a failed resume opened and records the flow Stopped,
    /// so the next wake starts clean; the queue is untouched.
    fn abandon_resumed(&self, node: &FlowNode, pane: &HerdrPaneBinding);
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
    /// Writes the context size the flow's native transcript shows now.
    fn record_context_at_stop(&self, flow_id: &str);
    /// The refresh threshold configured for this layer, in tokens.
    fn refresh_threshold(&self, layer: &Layer) -> Option<i64>;
    /// Refuses a wake while a pane already runs this flow's native session:
    /// SessionLive when one does, HerdrUnreadable when Herdr is unreadable.
    fn refuse_live_session(&self, node: &FlowNode, sessions: &[String]) -> Result<(), WakeRefusal>;
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
        // Work queued for an Awake Codex seat whose thread rests is handed
        // to it now, as its Stop would; one in a turn waits for the turn's
        // completion.
        let resting = {
            let _turn = self
                .dispatch_gate
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            self.resting_codex_seat(&address)
        };
        if let Some(flow_id) = resting {
            self.settle_stopped(&flow_id);
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
        // A Refresh asked for this metaflow's next wake to be fresh.
        let refreshed = {
            use crate::store::refresh::MarksFreshWakes;
            self.store
                .fresh_wake_marked(&metaflow.address)
                .unwrap_or(false)
        };
        if let (Some(node), Some(profile)) = (&node, &profile)
            && !refreshed
            && Self::can_resume(node)
            && node.harness_kind == HarnessKind::Claude
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
        match profile {
            Some(profile) => WakePlan::Fresh(FreshContext {
                herdr_session_name: profile.herdr_session_name,
                system_modules: profile.system_modules,
                user_modules: profile.user_modules,
                leave_outs_option: profile.leave_outs_option,
            }),
            None => WakePlan::Unplaceable,
        }
    }

    fn wake_is_due(&self, metaflow: &Metaflow) -> bool {
        metaflow.state == flow_ethos::State::Asleep
            && Self::wakes_layer(&metaflow.address.layer)
            && (metaflow
                .queue
                .iter()
                .any(|queued| Self::wakes_for(&queued.request))
                || metaflow.past.last().is_some_and(|flow_id| {
                    matches!(
                        self.store.flow_node(flow_id),
                        Ok(Some(node)) if node.flow_lifecycle.is_live()
                    )
                }))
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

/// A privileged request that an Asleep metaflow's next wake be fresh.
pub trait RefreshesMetaflows {
    /// Marks the metaflow at `address` for a fresh wake: Refreshed when it
    /// is Asleep and its last flow has a recorded launch profile to compose
    /// from (a repeat while marked answers the same), else the typed
    /// rejection. Its queue, past and models are not touched.
    fn refresh(&self, address: Address) -> meta_signal_flow::Response;
}

impl RefreshesMetaflows for RunningNexus {
    fn refresh(&self, address: Address) -> meta_signal_flow::Response {
        use meta_signal_flow::{RefreshRejection, Response};
        let rejected = |rejection| Response::RefreshRejected(rejection);
        self.rest_ended_awake(&address);
        let metaflow = match self.store.metaflow(&address) {
            Ok(Some(metaflow)) => metaflow,
            Ok(None) => return rejected(RefreshRejection::Unknown),
            Err(_) => return rejected(RefreshRejection::StoreRefused),
        };
        match metaflow.state {
            flow_ethos::State::Awake(_) => return rejected(RefreshRejection::Awake),
            flow_ethos::State::Ended => return rejected(RefreshRejection::Ended),
            flow_ethos::State::Asleep => {}
        }
        let profiled = metaflow
            .past
            .last()
            .is_some_and(|flow_id| self.last_launch_profile(flow_id).is_some());
        if !profiled {
            return rejected(RefreshRejection::NoLaunchProfile);
        }
        match self.perform(Operation::Record(Record_Data::Refresh(address.clone()))) {
            Outcome::Recorded => Response::Refreshed(address),
            Outcome::Failed(Failed_Data::NotAsleep) => rejected(RefreshRejection::Awake),
            _ => rejected(RefreshRejection::StoreRefused),
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
        let node = match self.store.flow_node(&flow_id) {
            Ok(Some(node)) => node,
            _ => return Outcome::Failed(Failed_Data::StoreRefused),
        };
        if node.harness_kind == HarnessKind::Codex {
            let attempt = match self.store.prepare_oldest_delivery(
                &address,
                &flow_id,
                &node.session_id,
            ) {
                Ok(PrepareQueueDelivery::Prepared(attempt)) => attempt,
                Ok(PrepareQueueDelivery::Outstanding(attempt)) => {
                    return match attempt.phase {
                        QueueDeliveryPhase::ReceiptRecorded(_) => {
                            if self
                                .store
                                .settle_recorded_delivery(&attempt.attempt_id)
                                .unwrap_or(false)
                            {
                                self.codex_turns.delivered(&node.session_id);
                                Outcome::Drained(vec![attempt.queued])
                            } else {
                                Outcome::Failed(Failed_Data::StoreRefused)
                            }
                        }
                        QueueDeliveryPhase::Prepared
                        | QueueDeliveryPhase::Uncertain
                        | QueueDeliveryPhase::Settled(_) => {
                            Outcome::Failed(Failed_Data::HerdrRefused)
                        }
                    };
                }
                Ok(PrepareQueueDelivery::Empty) => return Outcome::Drained(Vec::new()),
                Err(_) => return Outcome::Failed(Failed_Data::StoreRefused),
            };
            let model = match self.store.role(&flow_id) {
                Ok(Some(role)) => role.model_name,
                _ => return Outcome::Failed(Failed_Data::StoreRefused),
            };
            let adapter = match self.codex_endpoints.adapter_for(&model) {
                Ok(adapter) => adapter,
                Err(_) => return Outcome::Failed(Failed_Data::HerdrRefused),
            };
            let receipt = match adapter.submit_queued_turn(
                &node.session_id,
                &attempt.attempt_id,
                attempt.native_request_id,
                &typed.prompt_text(),
            ) {
                Ok(receipt) => receipt,
                Err(_) => return Outcome::Failed(Failed_Data::HerdrRefused),
            };
            let receipt = NativeDeliveryReceipt {
                native_request_id: receipt.native_request_id,
                thread_id: receipt.thread_id,
                turn_id: receipt.turn_id,
                native_receipt: receipt.native_receipt,
            };
            if !self
                .store
                .record_delivery_receipt(&attempt.attempt_id, receipt)
                .unwrap_or(false)
                || !self
                    .store
                    .settle_recorded_delivery(&attempt.attempt_id)
                    .unwrap_or(false)
            {
                return Outcome::Failed(Failed_Data::StoreRefused);
            }
            self.codex_turns.delivered(&node.session_id);
            return Outcome::Drained(typed);
        }
        if self.hand_over(&flow_id, &typed.prompt_text()).is_err() {
            return Outcome::Failed(Failed_Data::HerdrRefused);
        }
        // A Codex seat's next rest is the end of a turn begun after this.
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
        // The flow's own session, already live in the pane Flow recorded
        // for it, is woken as it is: nothing is opened again.
        if let Some(node) = self.live_last_flow(&metaflow) {
            return self.settle_live_resumed(address, &node.flow_id);
        }
        if metaflow.queue.is_empty() {
            return Err(WakeRefusal::NothingQueued);
        }
        // Read, not taken: the requests leave the queue only when the
        // started flow is recorded Awake.
        let requests = metaflow.queue.clone();
        let Ok(delivered) = i64::try_from(requests.len()) else {
            return Err(WakeRefusal::StoreUnreadable);
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
            if let WakePlan::Fresh(fresh) = &plan {
                sessions.push(fresh.herdr_session_name.clone());
            }
            if let HerdrRouteSelection::Available(route) = &node.herdr_route_selection {
                sessions.push(route.herdr_session_name.clone());
            }
            self.refuse_live_session(&node, &sessions)?;
        }
        let flow_id = match plan {
            WakePlan::Resume { node, profile } => {
                let _turn = self
                    .dispatch_gate
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                self.resume(*node, *profile, &text)?
            }
            WakePlan::Fresh(fresh) => self
                .start_fresh(&metaflow, fresh, &text)
                .map_err(WakeRefusal::FreshNotStarted)?,
            WakePlan::Unplaceable => return Err(WakeRefusal::Unplaceable),
        };
        let _turn = self
            .dispatch_gate
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        match self.perform(Operation::Record(Record_Data::Awoken(
            Record_Data_Awoken_Data {
                address: address.clone(),
                flow_id: flow_id.clone(),
                delivered,
            },
        ))) {
            Outcome::Recorded => Ok(flow_id),
            _ => {
                eprintln!("flow-nexus: flow {flow_id} woke but is not recorded Awake");
                Err(WakeRefusal::AwakeNotRecorded)
            }
        }
    }

    fn live_last_flow(&self, metaflow: &Metaflow) -> Option<FlowNode> {
        let node = self.store.flow_node(metaflow.past.last()?).ok().flatten()?;
        if !node.flow_lifecycle.is_live() {
            return None;
        }
        match self.herdr.own_session(&node) {
            OwnSession::Runs => Some(node),
            presence => {
                eprintln!(
                    "flow-nexus: flow {} is not live in its own pane: {presence:?}",
                    node.flow_id
                );
                None
            }
        }
    }

    fn settle_live_resumed(&self, address: &Address, flow_id: &str) -> Result<FlowId, WakeRefusal> {
        let _turn = self
            .dispatch_gate
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        match self.perform(Operation::Record(Record_Data::Awoken(
            Record_Data_Awoken_Data {
                address: address.clone(),
                flow_id: flow_id.to_owned(),
                delivered: 0,
            },
        ))) {
            Outcome::Recorded => {
                eprintln!(
                    "flow-nexus: flow {flow_id} already live in its own pane is recorded Awake"
                );
                Ok(flow_id.to_owned())
            }
            _ => Err(WakeRefusal::AwakeNotRecorded),
        }
    }

    fn resume(
        &self,
        node: FlowNode,
        profile: LaunchProfile,
        text: &str,
    ) -> Result<FlowId, WakeRefusal> {
        // The system prompt is composed again from the module identities
        // the flow was launched with; nothing of it was kept on disk.
        let harness_kind = self.codex_endpoints.harness_for(&profile.model_name);
        let default = self
            .store
            .default_leave_outs(&profile.address, &harness_kind)
            .unwrap_or_default();
        let harnessed = HarnessedProfile {
            harness_kind,
            launch_profile: profile,
        };
        let launch = match self.composer.resumption(&harnessed, default, text) {
            Ok(launch) => launch,
            Err(CompositionError::Context(refusal)) => {
                return Err(WakeRefusal::ContextRefused(refusal));
            }
            Err(CompositionError::InvalidProfileField(_) | CompositionError::LeaveOut(_)) => {
                return Err(WakeRefusal::NotComposed);
            }
        };
        let pane = match self.perform(Operation::Open(launch.clone())) {
            Outcome::Opened(pane) => pane,
            Outcome::Failed(failure) => return Err(WakeRefusal::PaneNotOpened(failure)),
            _ => return Err(WakeRefusal::PaneNotOpened(Failed_Data::HerdrRefused)),
        };
        match self.resume_in_pane(&node, &launch, &pane, text) {
            Ok(()) => Ok(node.flow_id),
            Err(refusal) => {
                self.abandon_resumed(&node, &pane);
                Err(refusal)
            }
        }
    }

    fn resume_in_pane(
        &self,
        node: &FlowNode,
        launch: &signal_flow::ComposedLaunch,
        pane: &HerdrPaneBinding,
        text: &str,
    ) -> Result<(), WakeRefusal> {
        if self
            .herdr
            .resume_native_harness(launch, pane, &node.flow_id, &node.session_id)
            .is_err()
        {
            // Adapter text may carry private paths; only the fact is logged.
            return Err(WakeRefusal::HarnessNotResumed);
        }
        // The resumed harness must come up as the same session, and its
        // claim must still name this FlowId: the flow keeps its identity or
        // the resume fails, never silently taking another.
        let binding = self
            .herdr
            .observe_native_binding(launch, pane, Some(&node.flow_id))
            .map_err(|_| WakeRefusal::BindingUnread)?;
        if binding.native_session_id != node.session_id || binding.flow_id != node.flow_id {
            return Err(WakeRefusal::BindingDiffers);
        }
        let route = HerdrRoute {
            herdr_session_name: pane.herdr_session_name.clone(),
            herdr_agent_name: pane.herdr_agent_name.clone(),
            herdr_pane_id: pane.herdr_pane_id.clone(),
            herdr_terminal_id: pane.herdr_terminal_id.clone(),
        };
        if !self
            .store
            .record_resumed(&node.flow_id, route.clone())
            .unwrap_or(false)
        {
            return Err(WakeRefusal::ResumeNotRecorded);
        }
        if self.perform(Operation::Title(Title_Data {
            composed_launch: launch.clone(),
            native_launch_binding: binding,
        })) != Outcome::Titled
        {
            eprintln!(
                "flow-nexus: resumed flow {} keeps its pane untitled",
                node.flow_id
            );
        }
        // The pane must be at rest and its composer clear before the work
        // is typed, as for a fresh seat's first prompt.
        self.herdr
            .await_pane_rest(pane)
            .map_err(|_| WakeRefusal::NotResting)?;
        let _ = self.herdr.composer_cleared(&route, &node.harness_kind);
        self.hand_over(&node.flow_id, text)
            .map_err(WakeRefusal::HandOver)
    }

    fn abandon_resumed(&self, node: &FlowNode, pane: &HerdrPaneBinding) {
        let route = HerdrRoute {
            herdr_session_name: pane.herdr_session_name.clone(),
            herdr_agent_name: pane.herdr_agent_name.clone(),
            herdr_pane_id: pane.herdr_pane_id.clone(),
            herdr_terminal_id: pane.herdr_terminal_id.clone(),
        };
        let opened = FlowNode {
            herdr_route_selection: HerdrRouteSelection::Available(route.clone()),
            ..node.clone()
        };
        let closed = match self.herdr.pane_presence(&opened) {
            PanePresence::Absent => true,
            PanePresence::Unknown => false,
            PanePresence::Present | PanePresence::Bare => {
                let _lease = self.pane_leases.hold(&route);
                self.perform(Operation::Close(opened)) == Outcome::Closed
            }
        };
        if !closed {
            eprintln!(
                "flow-nexus: a failed resume of flow {} left its pane open",
                node.flow_id
            );
            return;
        }
        if self.perform(Operation::Record(Record_Data::Stopped(
            node.flow_id.clone(),
        ))) != Outcome::Recorded
        {
            eprintln!(
                "flow-nexus: a failed resume of flow {} is not recorded Stopped",
                node.flow_id
            );
        }
    }

    fn start_fresh(
        &self,
        metaflow: &Metaflow,
        fresh: FreshContext,
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
            effort: Self::WAKE_EFFORT.into(),
            herdr_session_name: fresh.herdr_session_name,
            system_modules: fresh.system_modules,
            user_modules: fresh.user_modules,
            leave_outs_option: fresh.leave_outs_option,
            prompt: text.to_owned(),
        };
        let profile = seat.fresh_profile();
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
        // A first turn may have queued work meanwhile; its targets are woken
        // now that the gate is given back.
        self.wake_deferred();
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
        // A Codex seat whose app-server shows it in a turn is never taken
        // for resting, whatever its screen shows.
        if self.codex_in_turn(flow_id) {
            return AtStop::Kept(KeptAtStop::InTurn);
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
