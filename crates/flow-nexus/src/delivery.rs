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

pub mod body;
pub mod lease;

use crate::RunningNexus;
use crate::herdr::pane::{Interruption, PaneAgent, Placement, WritesPane};
use crate::store::delivery::{LeaseStep, PaneLease, RecordsDeliveries};
use crate::store::{ReadsFlowRows, RecordsFlowLifecycle, RecordsReplacement};
use body::{RendersPaneText, VetsBody};
use lease::LeasesPanes;
use meta_signal_flow::{
    CommandGrade, CommandOutcome, CommandRejection, CommandRequest, Delivery, DeliveryGrade,
    DeliveryRejection, DeliveryRequest, HarnessCommand, HarnessProfile, InterruptWitness, Message,
    Response,
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
}

impl ReadsLeasedPane for RunningNexus {
    fn writable_state(&self, target: &DeliveryTarget) -> Result<AgentState, PaneRefusal> {
        let agent_state = match self.herdr.pane_agent(&target.node) {
            PaneAgent::Present {
                agent_state,
                ready: true,
            } => agent_state,
            PaneAgent::Present { ready: false, .. } | PaneAgent::Unreadable => {
                return Err(PaneRefusal::RouteUnavailable);
            }
            PaneAgent::Absent => {
                let _ = self.store.record_exited(&target.node.flow_id);
                return Err(PaneRefusal::RouteUnavailable);
            }
        };
        if agent_state == AgentState::Blocked {
            return Err(PaneRefusal::RecipientBlocked);
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
        let _lease = self.pane_leases.hold(&target.route);
        let agent_state = match self.writable_state(&target) {
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
        };
        Response::Commanded(CommandOutcome {
            flow_id: target.node.flow_id,
            harness_command: request.harness_command,
            command_grade,
        })
    }
}

/// One delivery's key sequence, run while its pane lease is held.
struct LeasedDelivery<'run> {
    nexus: &'run RunningNexus,
    request: &'run DeliveryRequest,
    target: &'run DeliveryTarget,
}

impl<'run> LeasedDelivery<'run> {
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

    /// Nothing reached the pane's composer: the lease row goes, and nothing
    /// is kept, so the same DeliveryId may be delivered again.
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

    fn run(&self) -> Result<Delivery, DeliveryRejection> {
        let mut agent_state = self.nexus.writable_state(self.target)?;
        let soft = matches!(self.request.message, Message::Soft(_));
        if soft && !matches!(agent_state, AgentState::Idle | AgentState::Done) {
            return Err(DeliveryRejection::RecipientWorking);
        }
        self.step(LeaseStep::Acquired)?;
        let hard = matches!(self.request.message, Message::HardAbrupt(_));
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
            // back into the composer: the pane is read again.
            agent_state = match self.nexus.writable_state(self.target) {
                Ok(agent_state) => agent_state,
                Err(refusal) => return self.refuse(refusal.into()),
            };
        }
        let observe = matches!(agent_state, AgentState::Idle | AgentState::Done);
        let text = self.request.message.pane_text();
        let observed = match self
            .nexus
            .herdr
            .place(&self.target.route, text.as_str(), observe)
        {
            Placement::Refused => return self.refuse(DeliveryRejection::NotDelivered),
            Placement::Uncertain => {
                return self.settle(interrupt_witness, DeliveryGrade::Uncertain);
            }
            Placement::Placed { observed } => observed,
        };
        let _ = self.step(LeaseStep::Placed);
        if hard {
            let _ = self
                .nexus
                .herdr
                .press(&self.target.route, &self.target.profile.submit_keys);
            let _ = self.step(LeaseStep::Submitted);
        }
        if !observed {
            return self.settle(interrupt_witness, DeliveryGrade::Transported);
        }
        // Presented is the observation itself: Herdr waited for the
        // recipient's reaction and answered `agent_prompted` for the exact
        // pane and terminal this route names, so nothing is re-read after.
        // A second snapshot would only say what the pane looks like later,
        // and because it re-read the agent's *name* — a label an imported
        // pane may not carry at all — it turned every nameless recipient's
        // delivery into Uncertain. The label was never the evidence.
        //
        // A Pending flow seen reacting to a real Deliver is witnessed live.
        if self.target.node.flow_lifecycle == FlowLifecycle::Pending {
            let _ = self.nexus.store.record_active(&self.target.node.flow_id);
        }
        self.settle(interrupt_witness, DeliveryGrade::Presented)
    }
}
