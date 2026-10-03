//! The Operation root at work: every effect the running Nexus performs is
//! one generated `Operation`, performed through `Performs` on the Nexus that
//! holds the store, Herdr, the Codex endpoints and the composer, and every
//! answer is one generated `Outcome`. The launch, stop, retire and delivery
//! paths decide what to do next; this is the only place they act.

use crate::RunningNexus;
use crate::codex::{SelectsCodexEndpoint, SubmitsBoundCodexFirstTurn};
use crate::composition::{ComposesLaunch, KeepsLaunchBundles, OpensLaunchComposer};
use crate::generated::operation::{
    Failed_Data, Operation, Outcome, PaneLaunch, Record_Data, Record_Data_Settled_Data,
    Register_Data, Reserve_Data, Submit_Data, Title_Data,
};
use crate::herdr::OperatesHerdrPane;
use crate::herdr::launch::{
    CreatesHerdrLaunchPane, ObservesNativeLaunchBinding, StartsNativeHerdrHarness,
    SubmitsFirstPromptOnce, TitlesNativeFlow,
};
use crate::store::{
    ConfirmsStartedFlow, FlowRegistration, RecordsFlowLifecycle, RecordsLaunchOutcome,
    RecordsNativeLaunchBinding, RecordsNativeLaunchIntent, RecordsPromptDeliveryIntent,
    RecordsPromptDeliveryResult, RecordsRegistrationAcknowledgement, RecordsReplacement,
    RegistersFlowIdentity, ReservesLaunchAttempt,
};
use signal_flow::{HarnessKind, Response};

/// The Nexus acting: one Operation in, one Outcome out.
pub trait Performs {
    /// What a seat is told the moment its launch receipt is confirmed.
    ///
    /// The first prompt ends by asking for the receipt marker and nothing else,
    /// which is what makes the receipt verifiable: the seat's first turn is
    /// exactly one known line. That same ending ends the turn, so the brief the
    /// first prompt carries would sit there unstarted, waiting for someone to
    /// say go. Nobody says go. Flow does: the receipt is witnessed, the launch
    /// is Started, and Flow types this one line into the bound pane through its
    /// own writer (`Operation::Continue`). No caller and no human follows a
    /// launch.
    const BRIEF_CONTINUATION: &'static str =
        "Launch receipt confirmed. Begin the brief in your first prompt now.";

    fn perform(&self, operation: Operation) -> Outcome;
}

/// A journal or lifecycle write: `Ok(true)` is the only Recorded.
trait AnswersRecord {
    fn recorded(self) -> Outcome;
}

impl<E> AnswersRecord for Result<bool, E> {
    fn recorded(self) -> Outcome {
        match self {
            Ok(true) => Outcome::Recorded,
            Ok(false) | Err(_) => Outcome::Failed(Failed_Data::StoreRefused),
        }
    }
}

impl<E> AnswersRecord for Result<(), E> {
    fn recorded(self) -> Outcome {
        match self {
            Ok(()) => Outcome::Recorded,
            Err(_) => Outcome::Failed(Failed_Data::StoreRefused),
        }
    }
}

impl Performs for RunningNexus {
    fn perform(&self, operation: Operation) -> Outcome {
        match operation {
            Operation::Compose(profile) => match self.composer.compose(&profile) {
                Ok(launch) => Outcome::Composed(launch),
                Err(_) => Outcome::Failed(Failed_Data::CompositionRefused),
            },
            Operation::Reserve(Reserve_Data {
                composed_launch,
                origin_clue,
            }) => match self
                .store
                .reserve_launch_attempt(&composed_launch, origin_clue)
            {
                Ok(reservation) => Outcome::Reserved(reservation),
                Err(_) => Outcome::Failed(Failed_Data::StoreRefused),
            },
            Operation::Record(record) => self.record(record),
            Operation::Register(Register_Data { flow_node, caller }) => {
                match self.store.register_flow_in_role(flow_node, caller) {
                    Ok(FlowRegistration::Registered(node)) => Outcome::Registered(*node),
                    Ok(FlowRegistration::ConflictingBinding) => {
                        Outcome::Failed(Failed_Data::ConflictingBinding)
                    }
                    Err(_) => Outcome::Failed(Failed_Data::StoreRefused),
                }
            }
            Operation::Confirm(flow_id) => match self.store.confirm_started(&flow_id) {
                Ok(Response::Started(started)) => Outcome::Started(started),
                Ok(_) => Outcome::Failed(Failed_Data::Unstarted),
                Err(_) => Outcome::Failed(Failed_Data::StoreRefused),
            },
            Operation::Open(launch) => match self.herdr.create_launch_pane(&launch) {
                Ok(pane) => Outcome::Opened(pane),
                Err(_) => Outcome::Failed(Failed_Data::HerdrRefused),
            },
            Operation::Spawn(PaneLaunch {
                composed_launch,
                herdr_pane_binding,
            }) => match self
                .herdr
                .start_native_harness(&composed_launch, &herdr_pane_binding)
            {
                Ok(()) => Outcome::Spawned,
                Err(_) => Outcome::Failed(Failed_Data::HerdrRefused),
            },
            Operation::Bind(PaneLaunch {
                composed_launch,
                herdr_pane_binding,
            }) => match self
                .herdr
                .observe_native_binding(&composed_launch, &herdr_pane_binding)
            {
                Ok(binding) => Outcome::Bound(binding),
                Err(_) => Outcome::Failed(Failed_Data::HerdrRefused),
            },
            Operation::Title(Title_Data {
                composed_launch,
                native_launch_binding,
            }) => match self
                .herdr
                .title_native_flow(&composed_launch, &native_launch_binding)
            {
                Ok(_) => Outcome::Titled,
                Err(_) => Outcome::Failed(Failed_Data::HerdrRefused),
            },
            Operation::Submit(Submit_Data {
                composed_launch,
                prompt_delivery_intent,
            }) => {
                let submission = match composed_launch.launch_profile.harness_kind {
                    HarnessKind::Codex => self
                        .codex_endpoints
                        .adapter_for(&composed_launch.launch_profile.model_name)
                        .map_err(|_| Failed_Data::CodexRefused)
                        .and_then(|adapter| {
                            adapter
                                .submit_bound_codex_first_turn(
                                    &composed_launch,
                                    &prompt_delivery_intent,
                                )
                                .map_err(|_| Failed_Data::CodexRefused)
                        }),
                    HarnessKind::Claude => self
                        .herdr
                        .submit_first_prompt_once(&composed_launch, &prompt_delivery_intent)
                        .map_err(|_| Failed_Data::HerdrRefused),
                };
                match submission {
                    Ok(result) => Outcome::Submitted(result),
                    Err(failure) => Outcome::Failed(failure),
                }
            }
            Operation::Continue(flow_id) => self.continue_into_brief(&flow_id),
            Operation::Close(node) => match self.herdr.close(&node) {
                true => Outcome::Closed,
                false => Outcome::Failed(Failed_Data::HerdrRefused),
            },
            Operation::Prune(launch_request_id) => match self
                .composer
                .launch_bundles()
                .remove_for_request(&launch_request_id)
            {
                Ok(()) => Outcome::Pruned,
                Err(error) => {
                    eprintln!(
                        "flow-nexus: launch {launch_request_id} bundle copy not removed: {error}"
                    );
                    Outcome::Failed(Failed_Data::BundleRefused)
                }
            },
        }
    }
}

/// The arms of `perform` that are longer than one adapter call.
trait PerformsInParts {
    fn record(&self, record: Record_Data) -> Outcome;
    /// Best effort by design: the flow is Started whatever this does. A
    /// continuation that does not reach the seat is reported to the Nexus
    /// log, never turned into a launch rejection — the seat exists, is
    /// registered and is routable, and one Deliver can reach it.
    fn continue_into_brief(&self, flow_id: &str) -> Outcome;
}

impl PerformsInParts for RunningNexus {
    fn record(&self, record: Record_Data) -> Outcome {
        match record {
            Record_Data::Intent(intent) => {
                self.store.record_native_launch_intent(intent).recorded()
            }
            Record_Data::Binding(binding) => {
                self.store.record_native_launch_binding(binding).recorded()
            }
            Record_Data::Acknowledgement(acknowledgement) => self
                .store
                .record_registration_acknowledgement(acknowledgement)
                .recorded(),
            Record_Data::Delivery(intent) => {
                self.store.record_prompt_delivery_intent(intent).recorded()
            }
            Record_Data::Delivered(result) => {
                self.store.record_prompt_delivery_result(result).recorded()
            }
            Record_Data::Active(flow_id) => self.store.record_active(&flow_id).recorded(),
            Record_Data::Stopped(flow_id) => self.store.record_stopped(&flow_id).recorded(),
            Record_Data::Retired(flow_id) => self.store.record_retired(&flow_id).recorded(),
            Record_Data::Exited(flow_id) => self.store.record_exited(&flow_id).recorded(),
            Record_Data::Replacing(replacement) => {
                self.store.record_replacement(replacement).recorded()
            }
            Record_Data::Withdrawn(launch_request_id) => self
                .store
                .withdraw_replacement(&launch_request_id)
                .recorded(),
            Record_Data::Settled(Record_Data_Settled_Data {
                launch_request_id,
                launch_outcome,
            }) => self
                .store
                .record_launch_outcome(&launch_request_id, launch_outcome)
                .recorded(),
        }
    }

    /// Typed by Flow's own writer, under the pane lease, like any other
    /// write. Exception, noted here: this one line is Flow's own, not a
    /// Message, so it carries no Priority head.
    fn continue_into_brief(&self, flow_id: &str) -> Outcome {
        use crate::delivery::lease::LeasesPanes;
        use crate::delivery::{FindsDeliveryTarget, ReadsLeasedPane};
        use crate::herdr::ReadsHerdrRoster;
        use crate::herdr::pane::{Placement, WritesPane};
        let target = match self.delivery_target(flow_id) {
            Ok(target) => target,
            Err(refusal) => {
                eprintln!(
                    "flow-nexus: flow {flow_id} cannot be continued into its brief: {refusal:?}"
                );
                return Outcome::Failed(Failed_Data::HerdrRefused);
            }
        };
        let _lease = self.pane_leases.hold(&target.route);
        let agent_state = match self.writable_state(&target) {
            Ok(agent_state) => agent_state,
            Err(refusal) => {
                eprintln!("flow-nexus: flow {flow_id} brief continuation refused: {refusal:?}");
                return Outcome::Failed(Failed_Data::HerdrRefused);
            }
        };
        let observe = matches!(
            agent_state,
            signal_flow::AgentState::Idle | signal_flow::AgentState::Done
        );
        match self
            .herdr
            .place(&target.route, Self::BRIEF_CONTINUATION, observe)
        {
            // The seat was seen reacting on its exact pane: it is Active.
            Placement::Placed { observed: true } if self.herdr.route_is_available(&target.node) => {
                let _ = self.perform(Operation::Record(Record_Data::Active(flow_id.into())));
                Outcome::Continued
            }
            Placement::Placed { .. } => Outcome::Continued,
            Placement::Uncertain => {
                eprintln!("flow-nexus: flow {flow_id} brief continuation typed, reaction unknown");
                Outcome::Continued
            }
            Placement::Refused => {
                eprintln!("flow-nexus: flow {flow_id} brief continuation refused by Herdr");
                Outcome::Failed(Failed_Data::HerdrRefused)
            }
        }
    }
}
