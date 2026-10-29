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
use datom_codec::Datomizable;
use protos::{Compactable, Protosizable};
pub mod body;
pub mod lease;

use crate::compacting::WatchesCompactions;
use crate::generated::operation::{Operation, Record_Data};
use crate::herdr::HerdrCli;
use crate::herdr::pane::{Interruption, PaneAgent, Placement, Submission, WritesPane};
use crate::performing::Performs;
use crate::store::context::{JudgesCompaction, JudgesThreshold};
use crate::store::delivery::{LeaseStep, PaneLease, RecordsDeliveries, SelectsHarnessProfile};
use crate::store::queue_delivery::AccountsQueueDelivery;
use crate::store::{ReadsFlowRows, RecordsReplacement};
use crate::waking::TypesQueuedCohorts;
use crate::{ConfiguredUse, RunningNexus};
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

struct DeliveryReadiness {
    agent_state: AgentState,
    composer_was_blank: bool,
}

/// Finds the live, routable flow a write is for.
pub trait FindsDeliveryTarget {
    fn delivery_target(
        &self,
        configured: &ConfiguredUse,
        flow_id: &str,
    ) -> Result<DeliveryTarget, TargetRefusal>;
}

impl FindsDeliveryTarget for RunningNexus {
    fn delivery_target(
        &self,
        configured: &ConfiguredUse,
        flow_id: &str,
    ) -> Result<DeliveryTarget, TargetRefusal> {
        self.delivery_target_with(flow_id, &configured.snapshot)
    }
}

impl RunningNexus {
    #[cfg(test)]
    pub(crate) fn deliver_cohort(
        &self,
        delivery_id: String,
        flow_id: String,
        messages: Vec<Message>,
    ) -> Response {
        let Ok(configured) = self.test_configured_use() else {
            return Response::DeliveryRejected(DeliveryRejection::PersistenceRefused);
        };
        let view = configured.herdr_operation_view();
        self.deliver_cohort_with_view(&configured, &view, delivery_id, &flow_id, messages, true)
    }

    pub(crate) fn delivery_target_with(
        &self,
        flow_id: &str,
        configuration: &crate::store::StoredConfigurationSnapshot,
    ) -> Result<DeliveryTarget, TargetRefusal> {
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
        let profile = configuration
            .configured_harnesses
            .iter()
            .find(|harness| harness.harness_kind == node.harness_kind)
            .map(|harness| HarnessProfile {
                harness_kind: harness.harness_kind.clone(),
                command_sigil_vector: harness.pane_profile.command_sigil_vector.clone(),
                interrupt_keys: harness.pane_profile.interrupt_keys.clone(),
                submit_keys: harness.pane_profile.submit_keys.clone(),
            })
            .ok_or(TargetRefusal::PersistenceRefused)?;
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
    fn writable_state(
        &self,
        configured: &ConfiguredUse,
        target: &DeliveryTarget,
    ) -> Result<AgentState, PaneRefusal>;
    /// The same read for an owner-forced write. A blocked state is then
    /// writable, while route, readiness and a person's composer remain
    /// protected exactly as for a kept write.
    fn forced_writable_state(
        &self,
        configured: &ConfiguredUse,
        target: &DeliveryTarget,
    ) -> Result<AgentState, PaneRefusal>;
}

impl ReadsLeasedPane for RunningNexus {
    fn writable_state(
        &self,
        configured: &ConfiguredUse,
        target: &DeliveryTarget,
    ) -> Result<AgentState, PaneRefusal> {
        self.writable_state_with(configured, &configured.herdr, target, false, true)
    }

    fn forced_writable_state(
        &self,
        configured: &ConfiguredUse,
        target: &DeliveryTarget,
    ) -> Result<AgentState, PaneRefusal> {
        self.writable_state_with(configured, &configured.herdr, target, true, true)
    }
}

impl RunningNexus {
    fn pane_state(
        &self,
        configured: &ConfiguredUse,
        herdr: &HerdrCli,
        target: &DeliveryTarget,
    ) -> Result<AgentState, PaneRefusal> {
        self.pane_state_with(configured, herdr, target, true)
    }

    fn pane_state_with(
        &self,
        configured: &ConfiguredUse,
        herdr: &HerdrCli,
        target: &DeliveryTarget,
        record_exit: bool,
    ) -> Result<AgentState, PaneRefusal> {
        match herdr.pane_agent(&target.node) {
            PaneAgent::Present {
                agent_state,
                ready: true,
            } => Ok(agent_state),
            PaneAgent::Present { ready: false, .. } | PaneAgent::Unreadable => {
                Err(PaneRefusal::RouteUnavailable)
            }
            PaneAgent::Absent => {
                if record_exit {
                    let _ = self.perform_configured(
                        configured,
                        Operation::Record(Record_Data::Exited(target.node.flow_id.clone())),
                    );
                }
                Err(PaneRefusal::RouteUnavailable)
            }
        }
    }

    pub(crate) fn writable_state_with(
        &self,
        configured: &ConfiguredUse,
        herdr: &HerdrCli,
        target: &DeliveryTarget,
        permit_blocked: bool,
        record_exit: bool,
    ) -> Result<AgentState, PaneRefusal> {
        let agent_state = self.pane_state_with(configured, herdr, target, record_exit)?;
        if agent_state == AgentState::Blocked {
            return if permit_blocked {
                Ok(agent_state)
            } else {
                Err(PaneRefusal::RecipientBlocked)
            };
        }
        match herdr.composer_is_blank(&target.route, &target.node.harness_kind) {
            Some(true) => Ok(agent_state),
            Some(false) => Err(PaneRefusal::ComposerOccupied),
            None => Err(PaneRefusal::NotDelivered),
        }
    }

    fn checked_cohort_target(
        &self,
        configuration: &crate::store::StoredConfigurationSnapshot,
        flow_id: &str,
        messages: &[Message],
    ) -> Result<DeliveryTarget, DeliveryRejection> {
        let target = self.delivery_target_with(flow_id, configuration)?;
        if let Some(refusal) = messages
            .iter()
            .find_map(|message| message.refusal(&target.profile))
        {
            return Err(DeliveryRejection::BodyRefused(refusal));
        }
        match self.store.past_threshold(&target.node.flow_id) {
            Ok(false) => {}
            Ok(true) => return Err(DeliveryRejection::PastThreshold),
            Err(_) => return Err(DeliveryRejection::PersistenceRefused),
        }
        Ok(target)
    }

    fn delivery_readiness(
        &self,
        configured: &ConfiguredUse,
        herdr: &HerdrCli,
        target: &DeliveryTarget,
        hard: bool,
        soft: bool,
        record_exit: bool,
    ) -> Result<DeliveryReadiness, DeliveryRejection> {
        let permit_blocked = false;
        let mut composer_was_blank = false;
        let agent_state = if hard {
            let agent_state = self.pane_state_with(configured, herdr, target, record_exit)?;
            if agent_state == AgentState::Blocked && !permit_blocked {
                return Err(DeliveryRejection::RecipientBlocked);
            }
            match herdr.composer_text(&target.route, &target.node.harness_kind) {
                Some(text) if text.is_empty() => composer_was_blank = true,
                Some(text) if text.opens_a_letter() => {}
                Some(_) => return Err(DeliveryRejection::ComposerOccupied),
                None => return Err(DeliveryRejection::NotDelivered),
            }
            agent_state
        } else {
            self.writable_state_with(configured, herdr, target, permit_blocked, record_exit)?
        };
        if soft
            && !matches!(agent_state, AgentState::Idle | AgentState::Done)
            && !(permit_blocked && agent_state == AgentState::Blocked)
        {
            return Err(DeliveryRejection::RecipientWorking);
        }
        Ok(DeliveryReadiness {
            agent_state,
            composer_was_blank,
        })
    }
}

/// The privileged pane writes.
pub trait DeliversMessages {
    fn deliver(&self, configured: &ConfiguredUse, request: DeliveryRequest) -> Response;
    fn vet(&self, configured: &ConfiguredUse, request: &DeliveryRequest) -> Response;
    fn command(&self, configured: &ConfiguredUse, request: CommandRequest) -> Response;
    fn read_pane(&self, configured: &ConfiguredUse, flow_id: &str) -> Response;
}

impl RunningNexus {
    /// Delivers one immutable queued cohort with the configuration generation
    /// selected by its drain boundary.
    pub(crate) fn deliver_cohort_with_view(
        &self,
        configured: &ConfiguredUse,
        view: &crate::herdr::HerdrOperationView,
        delivery_id: String,
        flow_id: &str,
        messages: Vec<Message>,
        persist_outcome: bool,
    ) -> Response {
        if persist_outcome {
            match self.store.delivery(&delivery_id) {
                Ok(Some(delivery)) => return Response::Delivered(delivery),
                Ok(None) => {}
                Err(_) => return Response::DeliveryRejected(DeliveryRejection::PersistenceRefused),
            }
        }
        let target =
            match self.checked_cohort_target(&view.configuration.snapshot, flow_id, &messages) {
                Ok(target) => target,
                Err(refusal) => return Response::DeliveryRejected(refusal),
            };
        let hard = messages
            .iter()
            .any(|message| matches!(message, Message::HardAbrupt(_)));
        let soft = messages
            .iter()
            .all(|message| matches!(message, Message::Soft(_)));
        let text = messages.datomize(Vec::new()).protosize().compact();
        let request = DeliveryRequest { delivery_id };
        let _lease = self.pane_leases.hold(&target.route);
        let delivery = if persist_outcome {
            LeasedDelivery::new_rendered(
                self,
                configured,
                &view.herdr,
                &request,
                &target,
                text,
                hard,
                soft,
            )
        } else {
            LeasedDelivery::new_rendered_unsettled(
                self,
                configured,
                &view.herdr,
                &request,
                &target,
                text,
                hard,
                soft,
            )
        };
        match delivery.run() {
            Ok(delivery) => Response::Delivered(delivery),
            Err(rejection) => Response::DeliveryRejected(rejection),
        }
    }

    #[cfg(test)]
    pub(crate) fn vet_cohort_for_test(&self, flow_id: &str, messages: &[Message]) -> Response {
        let Ok(configured) = self.test_configured_use() else {
            return Response::DeliveryRejected(DeliveryRejection::PersistenceRefused);
        };
        let view = configured.herdr_operation_view();
        let target =
            match self.checked_cohort_target(&view.configuration.snapshot, flow_id, messages) {
                Ok(target) => target,
                Err(refusal) => return Response::DeliveryRejected(refusal),
            };
        let hard = messages
            .iter()
            .any(|message| matches!(message, Message::HardAbrupt(_)));
        let soft = messages
            .iter()
            .all(|message| matches!(message, Message::Soft(_)));
        match self.delivery_readiness(&configured, &view.herdr, &target, hard, soft, false) {
            Ok(_) => Response::Vetted(target.node.flow_id),
            Err(refusal) => Response::DeliveryRejected(refusal),
        }
    }
}

impl DeliversMessages for RunningNexus {
    fn deliver(&self, configured: &ConfiguredUse, request: DeliveryRequest) -> Response {
        match self
            .store
            .settle_interrupted_typed_cohort(&request.delivery_id)
        {
            Ok(Some(delivery)) => return Response::Delivered(delivery),
            Ok(None) => {}
            Err(_) => return Response::DeliveryRejected(DeliveryRejection::PersistenceRefused),
        }
        match self.store.delivery(&request.delivery_id) {
            Ok(Some(delivery)) => return Response::Delivered(delivery),
            Ok(None) => {}
            Err(_) => return Response::DeliveryRejected(DeliveryRejection::PersistenceRefused),
        }
        let (attempt, entries) = match self.store.prepared_delivery_cohort(&request.delivery_id) {
            Ok(Some(referenced)) => referenced,
            Ok(None) => return Response::DeliveryRejected(DeliveryRejection::UnknownDelivery),
            Err(_) => return Response::DeliveryRejected(DeliveryRejection::PersistenceRefused),
        };
        let Some(messages) = self.typed_queue_cohort(&entries) else {
            return Response::DeliveryRejected(DeliveryRejection::PersistenceRefused);
        };
        let view = configured.herdr_operation_view();
        let response = self.deliver_cohort_with_view(
            configured,
            &view,
            request.delivery_id.clone(),
            &attempt.flow_id,
            messages,
            false,
        );
        let Response::Delivered(delivery) = response else {
            return response;
        };
        match self
            .store
            .settle_typed_cohort(&attempt.attempt_id, delivery.clone())
        {
            Ok(true) => Response::Delivered(delivery),
            Ok(false) | Err(_) => Response::DeliveryRejected(DeliveryRejection::PersistenceRefused),
        }
    }

    fn vet(&self, configured: &ConfiguredUse, request: &DeliveryRequest) -> Response {
        let (attempt, entries) = match self.store.prepared_delivery_cohort(&request.delivery_id) {
            Ok(Some(referenced)) => referenced,
            Ok(None) => return Response::DeliveryRejected(DeliveryRejection::UnknownDelivery),
            Err(_) => return Response::DeliveryRejected(DeliveryRejection::PersistenceRefused),
        };
        let Some(messages) = self.typed_queue_cohort(&entries) else {
            return Response::DeliveryRejected(DeliveryRejection::PersistenceRefused);
        };
        let view = configured.herdr_operation_view();
        let target = match self.checked_cohort_target(
            &view.configuration.snapshot,
            &attempt.flow_id,
            &messages,
        ) {
            Ok(target) => target,
            Err(refusal) => return Response::DeliveryRejected(refusal),
        };
        let hard = messages
            .iter()
            .any(|message| matches!(message, Message::HardAbrupt(_)));
        let soft = messages
            .iter()
            .all(|message| matches!(message, Message::Soft(_)));
        match self.delivery_readiness(configured, &view.herdr, &target, hard, soft, false) {
            Ok(_) => Response::Vetted(target.node.flow_id),
            Err(refusal) => Response::DeliveryRejected(refusal),
        }
    }

    fn command(&self, configured: &ConfiguredUse, request: CommandRequest) -> Response {
        let view = configured.herdr_operation_view();
        let herdr = &view.herdr;
        let target = match self.delivery_target_with(&request.flow_id, &view.configuration.snapshot)
        {
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
            HarnessCommand::Keys(_) => self.pane_state(configured, herdr, &target),
            HarnessCommand::Line(_) | HarnessCommand::Clear
                if matches!(request.gate, meta_signal_flow::Gate::Forced) =>
            {
                self.writable_state_with(configured, herdr, &target, true, true)
            }
            _ => self.writable_state_with(configured, herdr, &target, false, true),
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
                match herdr.interrupt(&target.route, &target.profile.interrupt_keys) {
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
                match herdr.place(&target.route, "/compact", observe) {
                    Placement::Refused => {
                        return Response::CommandRejected(CommandRejection::NotDelivered);
                    }
                    Placement::Uncertain => CommandGrade::Uncertain,
                    Placement::Placed { observed: true } => CommandGrade::Observed,
                    Placement::Placed { observed: false } => CommandGrade::Transported,
                }
            }
            HarnessCommand::Line(ref line) => match herdr.place(&target.route, &line, true) {
                Placement::Refused => {
                    return Response::CommandRejected(CommandRejection::NotDelivered);
                }
                Placement::Uncertain => CommandGrade::Uncertain,
                Placement::Placed { observed: true } => CommandGrade::Observed,
                Placement::Placed { observed: false } => CommandGrade::Transported,
            },
            HarnessCommand::Keys(ref keys) => {
                if !herdr.press(&target.route, keys) {
                    return Response::CommandRejected(CommandRejection::NotDelivered);
                }
                CommandGrade::Transported
            }
            HarnessCommand::Clear => {
                if !herdr.retract(&target.route, &target.node.harness_kind) {
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

    fn read_pane(&self, configured: &ConfiguredUse, flow_id: &str) -> Response {
        let view = configured.herdr_operation_view();
        let herdr = &view.herdr;
        let node = match self.store.flow_node(flow_id) {
            Ok(Some(node)) => node,
            Ok(None) => return Response::ReadPaneRejected(ReadPaneRejected_Data::UnknownFlow),
            Err(_) => return Response::ReadPaneRejected(ReadPaneRejected_Data::HerdrUnavailable),
        };
        let HerdrRouteSelection::Available(route) = &node.herdr_route_selection else {
            return Response::ReadPaneRejected(ReadPaneRejected_Data::RouteUnavailable);
        };
        let agent_state = match herdr.pane_agent(&node) {
            PaneAgent::Present { agent_state, .. } => agent_state,
            PaneAgent::Absent => {
                return Response::ReadPaneRejected(ReadPaneRejected_Data::RouteUnavailable);
            }
            PaneAgent::Unreadable => {
                return Response::ReadPaneRejected(ReadPaneRejected_Data::HerdrUnavailable);
            }
        };
        match herdr.screen(route, &node.harness_kind) {
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
    configured: &'run ConfiguredUse,
    herdr: &'run HerdrCli,
    request: &'run DeliveryRequest,
    target: &'run DeliveryTarget,
    text: String,
    hard: bool,
    soft: bool,
    persist_outcome: bool,
}

trait RunsLeasedDelivery<'run> {
    fn new_rendered(
        nexus: &'run RunningNexus,
        configured: &'run ConfiguredUse,
        herdr: &'run HerdrCli,
        request: &'run DeliveryRequest,
        target: &'run DeliveryTarget,
        text: String,
        hard: bool,
        soft: bool,
    ) -> Self;
    fn new_rendered_unsettled(
        nexus: &'run RunningNexus,
        configured: &'run ConfiguredUse,
        herdr: &'run HerdrCli,
        request: &'run DeliveryRequest,
        target: &'run DeliveryTarget,
        text: String,
        hard: bool,
        soft: bool,
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
    fn new_rendered(
        nexus: &'run RunningNexus,
        configured: &'run ConfiguredUse,
        herdr: &'run HerdrCli,
        request: &'run DeliveryRequest,
        target: &'run DeliveryTarget,
        text: String,
        hard: bool,
        soft: bool,
    ) -> Self {
        Self {
            nexus,
            configured,
            herdr,
            request,
            target,
            text,
            hard,
            soft,
            persist_outcome: true,
        }
    }

    fn new_rendered_unsettled(
        nexus: &'run RunningNexus,
        configured: &'run ConfiguredUse,
        herdr: &'run HerdrCli,
        request: &'run DeliveryRequest,
        target: &'run DeliveryTarget,
        text: String,
        hard: bool,
        soft: bool,
    ) -> Self {
        Self {
            nexus,
            configured,
            herdr,
            request,
            target,
            text,
            hard,
            soft,
            persist_outcome: false,
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
        if self.persist_outcome
            && let Err(error) = self.nexus.store.settle_delivery(delivery.clone())
        {
            eprintln!(
                "flow-nexus: delivery {} typed but not recorded: {error}",
                delivery.delivery_id
            );
        }
        Ok(delivery)
    }

    fn submitted(&self, lines: usize) -> Submitted {
        let herdr = self.herdr;
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
        let herdr = self.herdr;
        let route = &self.target.route;
        let harness_kind = &self.target.node.harness_kind;
        herdr
            .composer_text(route, harness_kind)
            .is_some_and(|text| text.opens_a_letter())
            && herdr.retract(route, harness_kind)
            && herdr.submission(route, harness_kind) == Submission::Seen
    }

    fn resubmitted_cancelled_prompt(&self) -> bool {
        let herdr = self.herdr;
        let route = &self.target.route;
        let harness_kind = &self.target.node.harness_kind;
        herdr.submit(route, harness_kind)
            && herdr.submission(route, harness_kind) == Submission::Seen
    }

    fn run(&self) -> Result<Delivery, DeliveryRejection> {
        let hard = self.hard;
        // A hard delivery reads the composer before it presses any key. A
        // text there that is not Flow's own letter refuses it untouched: an
        // interrupt would cancel the recipient's turn and still leave the
        // letter nowhere to go, so every later hard letter would cancel the
        // next turn too. A blank composer, or one holding Flow's letter, is
        // interrupted; what the interrupt puts back is settled below.
        let readiness = self.nexus.delivery_readiness(
            self.configured,
            self.herdr,
            self.target,
            hard,
            self.soft,
            true,
        )?;
        let composer_was_blank = readiness.composer_was_blank;
        let mut agent_state = readiness.agent_state;
        self.step(LeaseStep::Acquired)?;
        let mut interrupt_witness = InterruptWitness::NotRequested;
        if hard && agent_state == AgentState::Working {
            let interruption = self
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
                self.nexus.writable_state_with(
                    self.configured,
                    self.herdr,
                    self.target,
                    false,
                    true,
                )
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
            agent_state = self.nexus.writable_state_with(
                self.configured,
                self.herdr,
                self.target,
                false,
                true,
            )?;
        }
        let observe = matches!(agent_state, AgentState::Idle | AgentState::Done);
        let text = &self.text;
        // Whether Herdr saw the recipient react; None when what followed
        // the text is not known.
        let reacted = match self.herdr.place(&self.target.route, text, observe) {
            Placement::Refused => return self.refuse(DeliveryRejection::NotDelivered),
            Placement::Uncertain => None,
            Placement::Placed { observed } => Some(observed),
        };
        let _ = self.step(LeaseStep::Placed);
        if hard {
            let _ = self
                .herdr
                .press(&self.target.route, &self.target.profile.submit_keys);
            let _ = self.step(LeaseStep::Submitted);
        }
        // Whatever Herdr answered, the letter must not be left typed and
        // unsubmitted: a composer holding it refuses every later letter.
        let submitted = self.submitted(text.lines().count());
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
            let _ = self.nexus.perform_configured(
                self.configured,
                Operation::Record(Record_Data::Active(self.target.node.flow_id.clone())),
            );
        }
        self.settle(interrupt_witness, DeliveryGrade::Presented)
    }
}
