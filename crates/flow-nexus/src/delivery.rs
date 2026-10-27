//! Flow is the only pane writer. Deliver types a typed Message into a
//! flow's pane; Command sends a harness command; Vet answers what Deliver
//! would refuse. Each write runs under the pane lease, from the first key to
//! the last, and a delivery's progress is kept in its lease row so a crash
//! mid-sequence settles `Uncertain` instead of being retried.
//!
//! Every tier requires the route to match Herdr's snapshot, the agent not to
//! be Blocked (typing into a permission dialog could answer it) and the
//! composer to be blank (a person's draft is never appended to). Soft also
//! requires the agent Idle or Done. The screen read is not atomic with the
//! typing; that is stated, not solved.

use crate::delivery::body::ShowsPaneText;
pub mod body;
pub mod lease;

use crate::RunningNexus;
use crate::compacting::WatchesCompactions;
use crate::generated::operation::{Operation, Record_Data};
use crate::herdr::pane::{Interruption, PaneAgent, Placement, Submission, WritesPane};
use crate::performing::Performs;
use crate::store::context::{JudgesCompaction, JudgesThreshold};
use crate::store::delivery::{LeaseStep, PaneLease, RecordsDeliveries, SelectsHarnessProfile};
use crate::store::{ReadsFlowRows, RecordsReplacement};
use body::{RecognizesLetter, RendersPaneText, VetsBody};
use lease::LeasesPanes;
use meta_signal_flow::{
    CommandGrade, CommandOutcome, CommandRejection, CommandRequest, Delivery, DeliveryGrade,
    DeliveryRejection, DeliveryRequest, HarnessCommand, HarnessProfile, InterruptWitness, Message,
    PaneRead_Data, ReadPaneRejected_Data, Response,
};
use signal_flow::{AgentState, FlowLifecycle, FlowNode, HerdrRoute, HerdrRouteSelection};

/// Why a flow cannot be written to, before its pane is looked at.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TargetRefusal {
    UnknownFlow,
    FlowStopped,
    FlowRetired,
    FlowExited,
    RouteUnavailable,
    PersistenceRefused,
}

impl From<TargetRefusal> for DeliveryRejection {
    fn from(refusal: TargetRefusal) -> Self {
        match refusal {
            TargetRefusal::UnknownFlow => Self::UnknownFlow,
            TargetRefusal::FlowStopped => Self::FlowStopped,
            TargetRefusal::FlowRetired => Self::FlowRetired,
            TargetRefusal::FlowExited => Self::FlowExited,
            TargetRefusal::RouteUnavailable => Self::RouteUnavailable,
            TargetRefusal::PersistenceRefused => Self::PersistenceRefused,
        }
    }
}

impl From<TargetRefusal> for CommandRejection {
    fn from(refusal: TargetRefusal) -> Self {
        match refusal {
            TargetRefusal::UnknownFlow => Self::UnknownFlow,
            TargetRefusal::FlowStopped => Self::FlowStopped,
            TargetRefusal::FlowRetired => Self::FlowRetired,
            TargetRefusal::FlowExited => Self::FlowExited,
            TargetRefusal::RouteUnavailable => Self::RouteUnavailable,
            // Nothing was typed; Command has no store to refuse.
            TargetRefusal::PersistenceRefused => Self::NotDelivered,
        }
    }
}

/// The flow a write is addressed to, as Flow holds it, with the harness
/// profile it is typed with.
pub struct DeliveryTarget {
    pub node: FlowNode,
    pub route: HerdrRoute,
    pub profile: HarnessProfile,
}

/// Finds the live, routable flow a write is for.
pub trait FindsDeliveryTarget {
    fn delivery_target(&self, flow_id: &str) -> Result<DeliveryTarget, TargetRefusal>;
}

impl FindsDeliveryTarget for RunningNexus {
    fn delivery_target(&self, flow_id: &str) -> Result<DeliveryTarget, TargetRefusal> {
        let node = match self.store.flow_node(flow_id) {
            Ok(Some(node)) => node,
            Ok(None) => return Err(TargetRefusal::UnknownFlow),
            Err(_) => return Err(TargetRefusal::PersistenceRefused),
        };
        // A gone flow is refused by who ended it: Flow (Stopped), the owner
        // (Retired), or the seat leaving Herdr (Exited).
        match node.flow_lifecycle {
            FlowLifecycle::Pending | FlowLifecycle::Active => {}
            FlowLifecycle::Stopped => return Err(TargetRefusal::FlowStopped),
            FlowLifecycle::Retired => return Err(TargetRefusal::FlowRetired),
            FlowLifecycle::Exited => return Err(TargetRefusal::FlowExited),
        }
        match self.store.held_successor(flow_id) {
            Ok(false) => {}
            Ok(true) => return Err(TargetRefusal::RouteUnavailable),
            Err(_) => return Err(TargetRefusal::PersistenceRefused),
        }
        let profile = self
            .store
            .delivery_configuration()
            .map_err(|_| TargetRefusal::PersistenceRefused)?
            .profile(&node.harness_kind);
        let HerdrRouteSelection::Available(route) = node.herdr_route_selection.clone() else {
            return Err(TargetRefusal::RouteUnavailable);
        };
        Ok(DeliveryTarget {
            node,
            route,
            profile,
        })
    }
}

/// What the pane shows before anything is typed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PaneRefusal {
    RouteUnavailable,
    RecipientBlocked,
    ComposerOccupied,
    NotDelivered,
}

impl From<PaneRefusal> for DeliveryRejection {
    fn from(refusal: PaneRefusal) -> Self {
        match refusal {
            PaneRefusal::RouteUnavailable => Self::RouteUnavailable,
            PaneRefusal::RecipientBlocked => Self::RecipientBlocked,
            PaneRefusal::ComposerOccupied => Self::ComposerOccupied,
            PaneRefusal::NotDelivered => Self::NotDelivered,
        }
    }
}

impl From<PaneRefusal> for CommandRejection {
    fn from(refusal: PaneRefusal) -> Self {
        match refusal {
            PaneRefusal::RouteUnavailable => Self::RouteUnavailable,
            PaneRefusal::RecipientBlocked => Self::RecipientBlocked,
            // Command has no word for a draft in the composer; nothing typed.
            PaneRefusal::ComposerOccupied | PaneRefusal::NotDelivered => Self::NotDelivered,
        }
    }
}

/// Reads the pane a leased write is about to type into.
pub trait ReadsLeasedPane {
    /// The agent's state when the pane may be written: bound, ready, not
    /// Blocked, and its composer blank. A pane Herdr shows gone records the
    /// flow Exited, where that observation is made.
    fn writable_state(&self, target: &DeliveryTarget) -> Result<AgentState, PaneRefusal>;
    /// The same read for an owner-forced write. A blocked state is then
    /// writable, while route, readiness and a person's composer remain
    /// protected exactly as for a kept write.
    fn forced_writable_state(&self, target: &DeliveryTarget) -> Result<AgentState, PaneRefusal>;
}

impl ReadsLeasedPane for RunningNexus {
    fn writable_state(&self, target: &DeliveryTarget) -> Result<AgentState, PaneRefusal> {
        self.writable_state_with(target, false)
    }

    fn forced_writable_state(&self, target: &DeliveryTarget) -> Result<AgentState, PaneRefusal> {
        self.writable_state_with(target, true)
    }
}

impl RunningNexus {
    fn pane_state(&self, target: &DeliveryTarget) -> Result<AgentState, PaneRefusal> {
        match self.herdr.pane_agent(&target.node) {
            PaneAgent::Present {
                agent_state,
                ready: true,
            } => Ok(agent_state),
            PaneAgent::Present { ready: false, .. } | PaneAgent::Unreadable => {
                Err(PaneRefusal::RouteUnavailable)
            }
            PaneAgent::Absent => {
                let _ = self.perform(Operation::Record(Record_Data::Exited(
                    target.node.flow_id.clone(),
                )));
                Err(PaneRefusal::RouteUnavailable)
            }
        }
    }

    fn writable_state_with(
        &self,
        target: &DeliveryTarget,
        permit_blocked: bool,
    ) -> Result<AgentState, PaneRefusal> {
        let agent_state = self.pane_state(target)?;
        if agent_state == AgentState::Blocked {
            return if permit_blocked {
                Ok(agent_state)
            } else {
                Err(PaneRefusal::RecipientBlocked)
            };
        }
        match self
            .herdr
            .composer_is_blank(&target.route, &target.node.harness_kind)
        {
            Some(true) => Ok(agent_state),
            Some(false) => Err(PaneRefusal::ComposerOccupied),
            None => Err(PaneRefusal::NotDelivered),
        }
    }
}

/// The privileged pane writes.
pub trait DeliversMessages {
    fn deliver(&self, request: DeliveryRequest) -> Response;
    fn vet(&self, request: &DeliveryRequest) -> Response;
    fn command(&self, request: CommandRequest) -> Response;
    fn read_pane(&self, flow_id: &str) -> Response;
}

impl DeliversMessages for RunningNexus {
    fn deliver(&self, request: DeliveryRequest) -> Response {
        match self.store.delivery(&request.delivery_id) {
            Ok(Some(delivery)) => return Response::Delivered(delivery),
            Ok(None) => {}
            Err(_) => {
                return Response::DeliveryRejected(DeliveryRejection::PersistenceRefused);
            }
        }
        let target = match self.delivery_target(&request.flow_id) {
            Ok(target) => target,
            Err(refusal) => return Response::DeliveryRejected(refusal.into()),
        };
        if let Some(refusal) = request.message.refusal(&target.profile) {
            return Response::DeliveryRejected(DeliveryRejection::BodyRefused(refusal));
        }
        // A flow past its model's refresh threshold is given no new
        // message.
        match self.store.past_threshold(&target.node.flow_id) {
            Ok(false) => {}
            Ok(true) if matches!(request.gate, meta_signal_flow::Gate::Forced) => {}
            Ok(true) => return Response::DeliveryRejected(DeliveryRejection::PastThreshold),
            Err(_) => return Response::DeliveryRejected(DeliveryRejection::PersistenceRefused),
        }
        let _lease = self.pane_leases.hold(&target.route);
        match LeasedDelivery::new(self, &request, &target).run() {
            Ok(delivery) => Response::Delivered(delivery),
            Err(rejection) => Response::DeliveryRejected(rejection),
        }
    }

    fn vet(&self, request: &DeliveryRequest) -> Response {
        let target = match self.delivery_target(&request.flow_id) {
            Ok(target) => target,
            Err(refusal) => return Response::DeliveryRejected(refusal.into()),
        };
        match request.message.refusal(&target.profile) {
            Some(refusal) => Response::DeliveryRejected(DeliveryRejection::BodyRefused(refusal)),
            None => Response::Vetted(target.node.flow_id),
        }
    }

    fn command(&self, request: CommandRequest) -> Response {
        let target = match self.delivery_target(&request.flow_id) {
            Ok(target) => target,
            Err(refusal) => return Response::CommandRejected(refusal.into()),
        };
        // Compaction is an emergency measure: below its model's refresh
        // threshold it is refused before anything reaches the pane.
        if request.harness_command == HarnessCommand::Compact
            && matches!(request.gate, meta_signal_flow::Gate::Kept)
        {
            match self.store.compaction_refusal(&target.node.flow_id) {
                Ok(None) => {}
                Ok(Some(refusal)) => {
                    return Response::CommandRejected(CommandRejection::CompactionRefused(refusal));
                }
                Err(_) => return Response::CommandRejected(CommandRejection::NotDelivered),
            }
        }
        let _lease = self.pane_leases.hold(&target.route);
        let state = match &request.harness_command {
            HarnessCommand::Keys(_) => self.pane_state(&target),
            HarnessCommand::Line(_) | HarnessCommand::Clear
                if matches!(request.gate, meta_signal_flow::Gate::Forced) =>
            {
                self.forced_writable_state(&target)
            }
            _ => self.writable_state(&target),
        };
        let agent_state = match state {
            Ok(agent_state) => agent_state,
            Err(refusal) => return Response::CommandRejected(refusal.into()),
        };
        let command_grade = match request.harness_command {
            HarnessCommand::Interrupt => {
                // Nothing to interrupt: no key is sent, since Escape into a
                // resting Claude opens its rewind menu.
                if agent_state != AgentState::Working {
                    return Response::CommandRejected(CommandRejection::NotDelivered);
                }
                match self
                    .herdr
                    .interrupt(&target.route, &target.profile.interrupt_keys)
                {
                    Interruption::Refused => {
                        return Response::CommandRejected(CommandRejection::NotDelivered);
                    }
                    Interruption::Observed => CommandGrade::Observed,
                    Interruption::Unobserved => CommandGrade::Transported,
                }
            }
            HarnessCommand::Compact => {
                // The compaction this asks for is not continuous work.
                self.compaction_requested(&target.node.flow_id);
                let observe = matches!(agent_state, AgentState::Idle | AgentState::Done);
                match self.herdr.place(&target.route, "/compact", observe) {
                    Placement::Refused => {
                        return Response::CommandRejected(CommandRejection::NotDelivered);
                    }
                    Placement::Uncertain => CommandGrade::Uncertain,
                    Placement::Placed { observed: true } => CommandGrade::Observed,
                    Placement::Placed { observed: false } => CommandGrade::Transported,
                }
            }
            HarnessCommand::Line(ref line) => match self.herdr.place(&target.route, &line, true) {
                Placement::Refused => {
                    return Response::CommandRejected(CommandRejection::NotDelivered);
                }
                Placement::Uncertain => CommandGrade::Uncertain,
                Placement::Placed { observed: true } => CommandGrade::Observed,
                Placement::Placed { observed: false } => CommandGrade::Transported,
            },
            HarnessCommand::Keys(ref keys) => {
                if !self.herdr.press(&target.route, keys) {
                    return Response::CommandRejected(CommandRejection::NotDelivered);
                }
                CommandGrade::Transported
            }
            HarnessCommand::Clear => {
                if !self.herdr.retract(&target.route, &target.node.harness_kind) {
                    return Response::CommandRejected(CommandRejection::NotDelivered);
                }
                CommandGrade::Transported
            }
        };
        Response::Commanded(CommandOutcome {
            flow_id: target.node.flow_id,
            harness_command: request.harness_command,
            command_grade,
        })
    }

    fn read_pane(&self, flow_id: &str) -> Response {
        let node = match self.store.flow_node(flow_id) {
            Ok(Some(node)) => node,
            Ok(None) => return Response::ReadPaneRejected(ReadPaneRejected_Data::UnknownFlow),
            Err(_) => return Response::ReadPaneRejected(ReadPaneRejected_Data::HerdrUnavailable),
        };
        let HerdrRouteSelection::Available(route) = &node.herdr_route_selection else {
            return Response::ReadPaneRejected(ReadPaneRejected_Data::RouteUnavailable);
        };
        let agent_state = match self.herdr.pane_agent(&node) {
            PaneAgent::Present { agent_state, .. } => agent_state,
            PaneAgent::Absent => {
                return Response::ReadPaneRejected(ReadPaneRejected_Data::RouteUnavailable);
            }
            PaneAgent::Unreadable => {
                return Response::ReadPaneRejected(ReadPaneRejected_Data::HerdrUnavailable);
            }
        };
        match self.herdr.screen(route) {
            Some(screen) => Response::PaneRead(PaneRead_Data {
                flow_id: node.flow_id,
                agent_state,
                screen,
            }),
            None => Response::ReadPaneRejected(ReadPaneRejected_Data::HerdrUnavailable),
        }
    }
}

/// What became of a placed letter in the composer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Submitted {
    /// It left the composer on Herdr's own submit.
    Seen,
    /// It left only after Flow pressed the submit key once more.
    AfterPress,
    /// The composer could not be read after placing.
    Unreadable,
    /// It would not submit, and was taken back out of the composer.
    TakenBack,
    /// It would neither submit nor be taken back.
    Stuck,
}

/// One delivery's key sequence, run while its pane lease is held.
struct LeasedDelivery<'run> {
    nexus: &'run RunningNexus,
    request: &'run DeliveryRequest,
    target: &'run DeliveryTarget,
}

trait RunsLeasedDelivery<'run> {
    fn new(
        nexus: &'run RunningNexus,
        request: &'run DeliveryRequest,
        target: &'run DeliveryTarget,
    ) -> Self;
    fn step(&self, lease_step: LeaseStep) -> Result<(), DeliveryRejection>;
    /// Nothing reached the pane's composer: the lease row goes, and nothing
    /// is kept, so the same DeliveryId may be delivered again.
    fn refuse(&self, rejection: DeliveryRejection) -> Result<Delivery, DeliveryRejection>;
    fn settle(
        &self,
        interrupt_witness: InterruptWitness,
        delivery_grade: DeliveryGrade,
    ) -> Result<Delivery, DeliveryRejection>;
    /// Sees the placed text leave the composer. When it stays, the submit
    /// key is pressed once; when it stays after that too, the text is taken
    /// back, so the pane is never left holding a letter.
    fn submitted(&self, lines: usize) -> Submitted;
    /// Empties the composer of a letter an interrupt put back into it; a
    /// text that is not a letter is a person's and is never touched.
    fn retracted_restored_letter(&self) -> bool;
    /// Submits again the prompt an interrupt put back into a composer that
    /// was blank when the interrupt was pressed: the prompt of the turn the
    /// interrupt cancelled, which its writer had already submitted. It is
    /// never deleted; the letter follows it.
    fn resubmitted_cancelled_prompt(&self) -> bool;
    fn run(&self) -> Result<Delivery, DeliveryRejection>;
}

impl<'run> RunsLeasedDelivery<'run> for LeasedDelivery<'run> {
    fn new(
        nexus: &'run RunningNexus,
        request: &'run DeliveryRequest,
        target: &'run DeliveryTarget,
    ) -> Self {
        Self {
            nexus,
            request,
            target,
        }
    }

    fn step(&self, lease_step: LeaseStep) -> Result<(), DeliveryRejection> {
        self.nexus
            .store
            .record_lease_step(PaneLease {
                delivery_id: self.request.delivery_id.clone(),
                flow_id: self.target.node.flow_id.clone(),
                herdr_pane_id: self.target.route.herdr_pane_id.clone(),
                lease_step,
            })
            .map_err(|_| DeliveryRejection::PersistenceRefused)
    }

    fn refuse(&self, rejection: DeliveryRejection) -> Result<Delivery, DeliveryRejection> {
        let _ = self.nexus.store.release_lease(&self.request.delivery_id);
        Err(rejection)
    }

    fn settle(
        &self,
        interrupt_witness: InterruptWitness,
        delivery_grade: DeliveryGrade,
    ) -> Result<Delivery, DeliveryRejection> {
        let delivery = Delivery {
            delivery_id: self.request.delivery_id.clone(),
            flow_id: self.target.node.flow_id.clone(),
            interrupt_witness,
            delivery_grade,
        };
        // The text is typed: a store that refuses the outcome does not turn
        // it into a rejection, and the lease row left behind settles it
        // Uncertain on the next open.
        if let Err(error) = self.nexus.store.settle_delivery(delivery.clone()) {
            eprintln!(
                "flow-nexus: delivery {} typed but not recorded: {error}",
                delivery.delivery_id
            );
        }
        Ok(delivery)
    }

    fn submitted(&self, lines: usize) -> Submitted {
        let herdr = &self.nexus.herdr;
        let route = &self.target.route;
        let harness_kind = &self.target.node.harness_kind;
        match herdr.submission(route, harness_kind) {
            Submission::Seen => return Submitted::Seen,
            Submission::Unreadable => return Submitted::Unreadable,
            Submission::Unseen => {}
        }
        if herdr.submit(route, harness_kind) {
            let _ = self.step(LeaseStep::Submitted);
            match herdr.submission(route, harness_kind) {
                Submission::Seen => return Submitted::AfterPress,
                Submission::Unreadable => return Submitted::Unreadable,
                Submission::Unseen => {}
            }
        }
        if herdr.take_back(route, harness_kind, lines)
            && herdr.submission(route, harness_kind) == Submission::Seen
        {
            return Submitted::TakenBack;
        }
        Submitted::Stuck
    }

    fn retracted_restored_letter(&self) -> bool {
        let herdr = &self.nexus.herdr;
        let route = &self.target.route;
        let harness_kind = &self.target.node.harness_kind;
        herdr
            .composer_text(route, harness_kind)
            .is_some_and(|text| text.opens_a_letter())
            && herdr.retract(route, harness_kind)
            && herdr.submission(route, harness_kind) == Submission::Seen
    }

    fn resubmitted_cancelled_prompt(&self) -> bool {
        let herdr = &self.nexus.herdr;
        let route = &self.target.route;
        let harness_kind = &self.target.node.harness_kind;
        herdr.submit(route, harness_kind)
            && herdr.submission(route, harness_kind) == Submission::Seen
    }

    fn run(&self) -> Result<Delivery, DeliveryRejection> {
        let hard = matches!(self.request.message, Message::HardAbrupt(_));
        // A hard delivery reads the composer before it presses any key. A
        // text there that is not Flow's own letter refuses it untouched: an
        // interrupt would cancel the recipient's turn and still leave the
        // letter nowhere to go, so every later hard letter would cancel the
        // next turn too. A blank composer, or one holding Flow's letter, is
        // interrupted; what the interrupt puts back is settled below.
        let mut composer_was_blank = false;
        let mut agent_state = if hard {
            let agent_state = match self.nexus.herdr.pane_agent(&self.target.node) {
                PaneAgent::Present {
                    agent_state,
                    ready: true,
                } => agent_state,
                PaneAgent::Present { ready: false, .. } | PaneAgent::Unreadable => {
                    return Err(DeliveryRejection::RouteUnavailable);
                }
                PaneAgent::Absent => return Err(DeliveryRejection::RouteUnavailable),
            };
            match self
                .nexus
                .herdr
                .composer_text(&self.target.route, &self.target.node.harness_kind)
            {
                Some(text) if text.is_empty() => composer_was_blank = true,
                Some(text) if text.opens_a_letter() => {}
                Some(_) => return Err(DeliveryRejection::ComposerOccupied),
                None => return Err(DeliveryRejection::NotDelivered),
            }
            agent_state
        } else {
            if matches!(self.request.gate, meta_signal_flow::Gate::Forced) {
                self.nexus.forced_writable_state(self.target)?
            } else {
                self.nexus.writable_state(self.target)?
            }
        };
        let soft = matches!(self.request.message, Message::Soft(_));
        if soft
            && !matches!(agent_state, AgentState::Idle | AgentState::Done)
            && !(matches!(self.request.gate, meta_signal_flow::Gate::Forced)
                && agent_state == AgentState::Blocked)
        {
            return Err(DeliveryRejection::RecipientWorking);
        }
        self.step(LeaseStep::Acquired)?;
        let mut interrupt_witness = InterruptWitness::NotRequested;
        if hard && agent_state == AgentState::Working {
            let interruption = self
                .nexus
                .herdr
                .interrupt(&self.target.route, &self.target.profile.interrupt_keys);
            if interruption == Interruption::Refused {
                return self.refuse(DeliveryRejection::NotDelivered);
            }
            if self.step(LeaseStep::Interrupted).is_err() {
                return self.refuse(DeliveryRejection::PersistenceRefused);
            }
            interrupt_witness = if interruption == Interruption::Observed {
                InterruptWitness::Observed
            } else {
                InterruptWitness::Unobserved
            };
            // The interrupt may have raised a dialog or put a queued prompt
            // back into the composer: the pane is read again. A letter it
            // put back is Flow's own and is taken out; left there, it
            // refused this letter and every later one to the pane. Any
            // other text it put back into a blank composer is the prompt of
            // the turn it cancelled: that prompt is submitted again, never
            // left behind to hold the pane and never deleted.
            let read_after_interrupt = || {
                if matches!(self.request.gate, meta_signal_flow::Gate::Forced) {
                    self.nexus.forced_writable_state(self.target)
                } else {
                    self.nexus.writable_state(self.target)
                }
            };
            agent_state = match read_after_interrupt() {
                Ok(agent_state) => agent_state,
                Err(PaneRefusal::ComposerOccupied)
                    if self.retracted_restored_letter()
                        || (composer_was_blank && self.resubmitted_cancelled_prompt()) =>
                {
                    match read_after_interrupt() {
                        Ok(agent_state) => agent_state,
                        Err(refusal) => return self.refuse(refusal.into()),
                    }
                }
                Err(refusal) => return self.refuse(refusal.into()),
            };
        } else if hard {
            agent_state = if matches!(self.request.gate, meta_signal_flow::Gate::Forced) {
                self.nexus.forced_writable_state(self.target)?
            } else {
                self.nexus.writable_state(self.target)?
            };
        }
        let observe = matches!(agent_state, AgentState::Idle | AgentState::Done);
        let text = self.request.message.pane_text();
        // Whether Herdr saw the recipient react; None when what followed
        // the text is not known.
        let reacted = match self
            .nexus
            .herdr
            .place(&self.target.route, text.as_str(), observe)
        {
            Placement::Refused => return self.refuse(DeliveryRejection::NotDelivered),
            Placement::Uncertain => None,
            Placement::Placed { observed } => Some(observed),
        };
        let _ = self.step(LeaseStep::Placed);
        if hard {
            let _ = self
                .nexus
                .herdr
                .press(&self.target.route, &self.target.profile.submit_keys);
            let _ = self.step(LeaseStep::Submitted);
        }
        // Whatever Herdr answered, the letter must not be left typed and
        // unsubmitted: a composer holding it refuses every later letter.
        let submitted = self.submitted(text.as_str().lines().count());
        let observed = match (reacted, submitted) {
            (_, Submitted::TakenBack) => {
                return self.refuse(DeliveryRejection::ComposerOccupied);
            }
            (_, Submitted::Stuck) | (None, Submitted::Seen | Submitted::Unreadable) => {
                return self.settle(interrupt_witness, DeliveryGrade::Uncertain);
            }
            (Some(observed), Submitted::Seen) => observed,
            // Flow's own press submitted it, or the composer could not be
            // read: Herdr took the text, and no reaction to it is witnessed.
            (_, Submitted::AfterPress | Submitted::Unreadable) => false,
        };
        if !observed {
            return self.settle(interrupt_witness, DeliveryGrade::Transported);
        }
        // Presented is two observations: Herdr waited for the recipient's
        // reaction and answered `agent_prompted` for the exact pane and
        // terminal this route names, and the letter was seen to leave the
        // composer. Herdr's answer alone is not a submission: it matches
        // any lifecycle change after the text, and in the e167d8 sandbox
        // (fms-9a3e2b) it answered for a letter Claude Code left typed and
        // unsubmitted. The agent's *name* is never re-read: it is a label
        // an imported pane may not carry at all.
        //
        // A Pending flow seen reacting to a real Deliver is witnessed live.
        if self.target.node.flow_lifecycle == FlowLifecycle::Pending {
            let _ = self.nexus.perform(Operation::Record(Record_Data::Active(
                self.target.node.flow_id.clone(),
            )));
        }
        self.settle(interrupt_witness, DeliveryGrade::Presented)
    }
}
