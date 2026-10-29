//! The Operation root at work: every effect the running Nexus performs is
//! one generated `Operation`, performed through `Performs` on the Nexus that
//! holds the store, Herdr, the Codex endpoints and the composer, and every
//! answer is one generated `Outcome`. The launch, stop, retire and delivery
//! paths decide what to do next; this is the only place they act.

use crate::codex::{
    AccountsOpenedThread, OpensCodexThread, SelectsCodexEndpoint, SubmitsBoundCodexPart,
};
use crate::composition::{
    ComposesLaunch, CompositionError, ConfiguresHarnessModules, HarnessedProfile,
};
use crate::generated::operation::{
    Failed_Data, Operation, Outcome, PaneLaunch, Record_Data, Record_Data_Asleep_Data,
    Record_Data_Awoken_Data, Record_Data_Context_Data, Record_Data_Harness_Data,
    Record_Data_Settled_Data, Register_Data, Reserve_Data, Reserved_Data, Submit_Data, Title_Data,
};
use crate::generated::starter::DerivesHarness;
use crate::herdr::launch::ReadsHarnessModules;
use crate::herdr::launch::{
    CreatesHerdrLaunchPane, NativeTargetRefusal, ObservesNativeLaunchBinding, SpawnFailure,
    StartsNativeHerdrHarness, SubmitsStartupPart, TitleRefusal, TitlesNativeFlow,
};
use crate::herdr::{OperatesHerdrPane, ReadsHerdrPanes};
use crate::operational::RecordsOperationalState;
use crate::send_tool::ServesLaunchCalls;
use crate::store::{
    ConfirmsStartedFlow, FlowRegistration, RecordsFlowLifecycle, RecordsLaunchOutcome,
    RecordsNativeLaunchBinding, RecordsNativeLaunchIntent, RecordsPromptDeliveryIntent,
    RecordsPromptDeliveryResult, RecordsRegistrationAcknowledgement, RecordsReplacement,
    RegistersFlowIdentity, ReservesLaunchAttempt,
};
use crate::store::{ManagesMetaflowState, ReadsFlowRows};
use crate::title::TitleRefused;
use crate::waking::PerformsWakeAndDrain;
use crate::{BuildsConfiguredEffects, ConfiguredEffects, ConfiguredUse, RunningNexus};
use signal_flow::{
    HarnessKind, LaunchAttemptReservation, NativeLaunchFailure, NativeLaunchRefusal,
    NativeLaunchStage, Response, StartRejection,
};

trait LabelsNativeLaunchStage {
    fn label(&self) -> &'static str;
}

impl LabelsNativeLaunchStage for NativeLaunchStage {
    fn label(&self) -> &'static str {
        match self {
            Self::Open => "open",
            Self::CodexThread => "codex-thread",
            Self::Spawn => "spawn",
            Self::Bind => "bind",
            Self::Naming => "title",
            Self::Receipt => "receipt",
            Self::Confirm => "confirm",
            Self::Unclassified => "unclassified",
        }
    }
}

/// The fixed, actionable categories Flow may retain from an adapter failure.
/// Adapter error text and stderr may contain private paths, arguments, or
/// provider output, so they are deliberately never stored or printed.
pub(crate) trait ClassifiesNativeLaunchFailure {
    fn label(&self) -> &'static str;
    fn from_adapter_error(error: &str) -> Self;
}

impl ClassifiesNativeLaunchFailure for NativeLaunchFailure {
    fn label(&self) -> &'static str {
        match self {
            Self::PaneTargetMismatch => "pane-target-mismatch",
            Self::NativeSessionMissing => "native-session-missing",
            Self::NativeIdentityConflict => "native-identity-conflict",
            Self::FlowClaimUnavailable => "flow-claim-unavailable",
            Self::HarnessMismatch => "harness-mismatch",
            Self::HerdrUnreadable => "herdr-unreadable",
            Self::ForeignBinding => "foreign-binding",
            Self::InvalidFlowId => "invalid-flow-id",
            Self::NotReady => "not-ready",
            Self::TranscriptUnreadable => "transcript-unreadable",
            Self::TitleReadbackDiffers => "title-readback-differs",
            Self::CodexNamingRefused => "codex-naming-refused",
            Self::PaneLabelRefused => "pane-label-refused",
            Self::WorkspaceLabelRefused => "workspace-label-refused",
            Self::AdapterRefused => "adapter-refused",
            Self::BoundaryMismatch => "boundary-mismatch",
            Self::ThreadNotNamed => "thread-not-named",
            Self::PartDiffers => "part-differs",
            Self::LeaveOutShown => "leave-out-shown",
            Self::ModelOrEffortDiffers => "model-or-effort-differs",
            Self::SessionUnrecorded => "session-unrecorded",
            Self::Unclassified => "unclassified",
            Self::SendToolUnsupported => "send-tool-unsupported",
        }
    }

    fn from_adapter_error(error: &str) -> Self {
        if error.contains("not ready") {
            Self::NotReady
        } else if error.contains("does not match the created native pane") {
            Self::PaneTargetMismatch
        } else if error.contains("no native session")
            || error.contains("no official native session")
            || error.contains("empty native identity")
        {
            Self::NativeSessionMissing
        } else if error.contains("conflicts with the Flow-selected")
            || error.contains("not the one Flow reserved")
            || error.contains("observed native identity")
        {
            Self::NativeIdentityConflict
        } else if error.contains("flow claim") || error.contains("flow-id claim") {
            Self::FlowClaimUnavailable
        } else if error.contains("harness") {
            Self::HarnessMismatch
        } else {
            Self::AdapterRefused
        }
    }
}

impl From<&TitleRefusal> for NativeLaunchFailure {
    fn from(refusal: &TitleRefusal) -> Self {
        match refusal {
            TitleRefusal::ForeignBinding => Self::ForeignBinding,
            TitleRefusal::Title(TitleRefused::InvalidFlowId) => Self::InvalidFlowId,
            TitleRefusal::Target(target) => target.into(),
            TitleRefusal::NotReady => Self::NotReady,
            TitleRefusal::TranscriptUnreadable => Self::TranscriptUnreadable,
            TitleRefusal::ReadbackDiffers => Self::TitleReadbackDiffers,
            TitleRefusal::CodexNamingRefused => Self::CodexNamingRefused,
            TitleRefusal::PaneLabelRefused => Self::PaneLabelRefused,
            TitleRefusal::WorkspaceLabelRefused => Self::WorkspaceLabelRefused,
        }
    }
}

impl From<&NativeTargetRefusal> for NativeLaunchFailure {
    fn from(refusal: &NativeTargetRefusal) -> Self {
        match refusal {
            NativeTargetRefusal::HerdrUnreadable(_) => Self::HerdrUnreadable,
            NativeTargetRefusal::PaneMismatch => Self::PaneTargetMismatch,
            NativeTargetRefusal::NoNativeSession => Self::NativeSessionMissing,
            NativeTargetRefusal::IdentityInexact => Self::NativeIdentityConflict,
        }
    }
}

/// A native launch step's refusal: logged as a fixed line, then answered
/// on the operation's outcome with its stage and category.
pub(crate) trait RefusesNativeLaunch {
    fn refused(stage: NativeLaunchStage, adapter_error: String) -> Outcome;
    /// A typed Title refusal, logged by its own category.
    fn refused_title(refusal: &TitleRefusal) -> Outcome;
    fn logged(self) -> Outcome;
    fn log_line(&self) -> String;
}

impl RefusesNativeLaunch for NativeLaunchRefusal {
    fn refused(stage: NativeLaunchStage, adapter_error: String) -> Outcome {
        Self {
            native_launch_stage: stage,
            native_launch_failure: NativeLaunchFailure::from_adapter_error(&adapter_error),
        }
        .logged()
    }

    fn refused_title(refusal: &TitleRefusal) -> Outcome {
        Self {
            native_launch_stage: NativeLaunchStage::Naming,
            native_launch_failure: refusal.into(),
        }
        .logged()
    }

    fn logged(self) -> Outcome {
        // This fixed-format line has no adapter text, stderr, paths, IDs, or
        // request content. Its bounded fields remain useful for diagnosis.
        eprintln!("{}", self.log_line());
        Outcome::Failed(Failed_Data::LaunchRefused(self))
    }

    fn log_line(&self) -> String {
        format!(
            "flow-nexus: herdr stage={} category={}",
            self.native_launch_stage.label(),
            self.native_launch_failure.label()
        )
    }
}

/// The Nexus acting: one Operation in, one Outcome out.
pub trait Performs {
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
impl Performs for ConfiguredEffects<'_> {
    fn perform(&self, operation: Operation) -> Outcome {
        let configured = self.configured;
        match operation {
            // A model that is not exact on its harness is never launched: its
            // receipt could not match the model the transcript records.
            Operation::Compose(mut profile) => {
                let codex_endpoints = &configured.codex_endpoints;
                if !codex_endpoints.launches_exactly(&profile.model_name) {
                    eprintln!(
                        "flow-nexus: launch {} not composed: model {} is not an exact native model",
                        profile.launch_request_id, profile.model_name
                    );
                    return Outcome::Failed(Failed_Data::CompositionRefused(
                        signal_flow::CompositionRefusal::InexactModel,
                    ));
                }
                // A Start naming no leave-outs takes its metaflow's default
                // for the harness it starts on, and none when that harness
                // has no default.
                let harness_kind = codex_endpoints.harness_for(&profile.model_name);
                let declaration_left_out = configured
                    .snapshot
                    .configured_metaflows
                    .iter()
                    .find(|row| row.address == profile.address)
                    .map(|row| row.left_out.clone())
                    .unwrap_or_default();
                let chosen_left_out = profile
                    .left_out_option
                    .take()
                    .unwrap_or(declaration_left_out);
                let harness = codex_endpoints.harnesses.iter().find(|harness| {
                    harness.harness_kind == harness_kind
                        && harness
                            .served_models
                            .iter()
                            .any(|served| served.model_name == profile.model_name)
                });
                let mut default = harness
                    .map(|harness| harness.left_out.clone())
                    .unwrap_or_default();
                let harness_modules = harness
                    .map(|harness| harness.modules.clone())
                    .unwrap_or_default();
                for module in chosen_left_out {
                    if !default.contains(&module) {
                        default.push(module);
                    }
                }
                let request = profile.launch_request_id.clone();
                // The launch's Remembering, resolved to the flows whose raw
                // records the composer reads into its first message.
                let remembered_flows = {
                    use crate::store::metaflow::ResolvesRemembering;
                    match self.operational.remembered_flows(&profile) {
                        Ok(flows) => flows,
                        Err(_) => return Outcome::Failed(Failed_Data::StoreRefused),
                    }
                };
                match configured
                    .composer
                    .with_harness_modules(harness_modules)
                    .compose(
                        HarnessedProfile {
                            remembered_flows,
                            harness_kind,
                            launch_profile: profile,
                        },
                        default,
                    ) {
                    Ok(launch) => Outcome::Composed(launch),
                    Err(CompositionError::Context(refusal)) => {
                        eprintln!("flow-nexus: launch {request} context refused: {refusal:?}");
                        Outcome::Failed(Failed_Data::ContextRefused(refusal))
                    }
                    Err(CompositionError::InvalidProfileField(refusal)) => {
                        eprintln!("flow-nexus: launch {request} not composed: {refusal:?}");
                        Outcome::Failed(Failed_Data::CompositionRefused(refusal))
                    }
                }
            }
            Operation::Reserve(reserve) => self.reserve(configured, reserve),
            Operation::Record(record) => self.record(configured, record),
            Operation::Register(Register_Data { flow_node, caller }) => {
                match self.operational.register_flow_in_role(flow_node, caller) {
                    Ok(FlowRegistration::Registered(node)) => Outcome::Registered(*node),
                    Ok(FlowRegistration::ConflictingBinding) => {
                        Outcome::Failed(Failed_Data::ConflictingBinding)
                    }
                    Err(_) => Outcome::Failed(Failed_Data::StoreRefused),
                }
            }
            Operation::Confirm(flow_id) => match self.operational.confirm_started(&flow_id) {
                Ok(Response::Started(started)) => Outcome::Started(started),
                Ok(Response::StartRejected(StartRejection::AddressAwake)) => {
                    Outcome::Failed(Failed_Data::AddressAwake)
                }
                Ok(_) => Outcome::Failed(Failed_Data::Unstarted),
                Err(_) => Outcome::Failed(Failed_Data::StoreRefused),
            },
            Operation::Open(launch) => match configured.herdr.create_launch_pane(&launch) {
                Ok(pane) => Outcome::Opened(pane),
                Err(error) => NativeLaunchRefusal::refused(NativeLaunchStage::Open, error),
            },
            Operation::Spawn(PaneLaunch {
                composed_launch,
                herdr_pane_binding,
                flow_id_option,
                ..
            }) => {
                // Codex: the thread is opened lean through the app-server
                // and named with its title, then the TUI attaches to it while
                // it is idle; its first turn is started at Submit, once the
                // pane is bound.
                let opened = match composed_launch.harness_kind {
                    HarnessKind::Codex => match configured
                        .codex_endpoints
                        .adapter_for(&composed_launch.launch_profile.model_name)
                        .and_then(|adapter| {
                            adapter.open_thread(&composed_launch, &mut |thread| {
                                // Before anything runs on it: the thread is
                                // recorded as this launch's, durably (a
                                // remote-resumed TUI never reports it to
                                // Herdr, so a launch recovered after this
                                // start binds to it, and a call is sent from
                                // this launch's Address), and its FlowId is
                                // claimed, which names its title.
                                use crate::herdr::launch::ClaimsNativeIdentity;
                                use crate::store::opened_thread::RecordsOpenedThreads;
                                use crate::title::{ShowsNativeTitle, TitlesFlow};
                                let refused = |detail: String| {
                                    crate::codex::CodexAdapterUnavailable::Protocol(detail)
                                };
                                if !self
                                    .operational
                                    .record_opened_thread(
                                        &composed_launch.launch_profile.launch_request_id,
                                        thread,
                                    )
                                    .unwrap_or(false)
                                {
                                    return Err(refused(format!(
                                        "opened thread {thread} not recorded as the launch's"
                                    )));
                                }
                                let flow_id = configured
                                    .herdr
                                    .claim_flow_identity(&HarnessKind::Codex, thread)
                                    .map_err(refused)?;
                                composed_launch
                                    .launch_profile
                                    .native_title(&flow_id)
                                    .map(|title| title.as_str().to_owned())
                                    .map_err(|refusal| refused(refusal.to_string()))
                            })
                        }) {
                        // That connection answers the thread's calls while
                        // the launch holds the gate.
                        Ok((thread, connection)) => {
                            self.hold_launch_responder(connection);
                            Some(thread)
                        }
                        Err(error) => {
                            eprintln!(
                                "flow-nexus: launch {} Codex thread not opened: {error}",
                                composed_launch.launch_profile.launch_request_id
                            );
                            return NativeLaunchRefusal {
                                native_launch_stage: NativeLaunchStage::CodexThread,
                                native_launch_failure: match error {
                                    crate::codex::CodexAdapterUnavailable::SendToolUnsupported(
                                        _,
                                    ) => NativeLaunchFailure::SendToolUnsupported,
                                    _ => NativeLaunchFailure::AdapterRefused,
                                },
                            }
                            .logged();
                        }
                    },
                    HarnessKind::Claude => None,
                };
                // Herdr's start returns once the TUI is at rest; the thread
                // it attaches to has no turn yet.
                let started = match &opened {
                    Some(thread) => configured.herdr.attach_codex_harness(
                        &composed_launch,
                        &herdr_pane_binding,
                        thread,
                    ),
                    None => configured.herdr.start_native_harness(
                        &composed_launch,
                        &herdr_pane_binding,
                        flow_id_option.as_deref(),
                    ),
                };
                match started {
                    Ok(()) => Outcome::Spawned(opened),
                    // A refused start keeps its cause, and the thread a
                    // Codex start opened is accounted for and archived.
                    Err(SpawnFailure::Refused(spawn_refusal_cause)) => {
                        eprintln!(
                            "{}",
                            NativeLaunchRefusal {
                                native_launch_stage: NativeLaunchStage::Spawn,
                                native_launch_failure: NativeLaunchFailure::AdapterRefused,
                            }
                            .log_line()
                        );
                        let opened_thread_option = opened.map(|thread| {
                            match configured
                                .codex_endpoints
                                .adapter_for(&composed_launch.launch_profile.model_name)
                            {
                                Ok(adapter) => adapter.account_opened_thread(&thread),
                                Err(_) => signal_flow::OpenedThread {
                                    native_session_id: thread,
                                    first_turn: signal_flow::FirstTurn::Unread,
                                    thread_archive: signal_flow::ThreadArchive::ArchiveRefused,
                                },
                            }
                        });
                        Outcome::Failed(Failed_Data::SpawnRefused(
                            signal_flow::HarnessSpawnRefusal {
                                spawn_refusal_cause,
                                opened_thread_option,
                            },
                        ))
                    }
                    Err(SpawnFailure::Unattempted(error)) => {
                        NativeLaunchRefusal::refused(NativeLaunchStage::Spawn, error)
                    }
                }
            }
            Operation::Bind(PaneLaunch {
                composed_launch,
                herdr_pane_binding,
                flow_id_option,
                native_session_id_option,
            }) => {
                let observed = match native_session_id_option {
                    Some(thread) => configured.herdr.observe_attached_codex_binding(
                        &composed_launch,
                        &herdr_pane_binding,
                        &thread,
                    ),
                    None => configured.herdr.observe_native_binding(
                        &composed_launch,
                        &herdr_pane_binding,
                        flow_id_option.as_deref(),
                    ),
                };
                match observed {
                    Ok(binding) => Outcome::Bound(binding),
                    Err(error) => NativeLaunchRefusal::refused(NativeLaunchStage::Bind, error),
                }
            }
            Operation::Title(Title_Data {
                composed_launch,
                native_launch_binding,
            }) => {
                match configured
                    .herdr
                    .title_native_flow(&composed_launch, &native_launch_binding)
                {
                    Ok(_) => Outcome::Titled,
                    Err(refusal) => NativeLaunchRefusal::refused_title(&refusal),
                }
            }
            Operation::Submit(Submit_Data {
                composed_launch,
                prompt_delivery_intent,
            }) => {
                let submission = match composed_launch.harness_kind {
                    // The part's turn may call the send tool at once: the
                    // launch, holding the gate, answers it meanwhile.
                    HarnessKind::Codex => self.serving_launch_calls(|| {
                        configured
                            .codex_endpoints
                            .adapter_for(&composed_launch.launch_profile.model_name)
                            .map_err(|_| Failed_Data::CodexRefused)
                            .and_then(|adapter| {
                                adapter
                                    .submit_bound_codex_part(
                                        &composed_launch,
                                        &prompt_delivery_intent,
                                    )
                                    .map_err(|_| Failed_Data::CodexRefused)
                            })
                    }),
                    HarnessKind::Claude => configured
                        .herdr
                        .submit_startup_part(&composed_launch, &prompt_delivery_intent)
                        .map_err(|_| Failed_Data::HerdrRefused),
                };
                match submission {
                    Ok(result) => Outcome::Submitted(result),
                    Err(failure) => Outcome::Failed(failure),
                }
            }
            Operation::Release(flow_id) => self.release(configured, &flow_id),
            Operation::Close(node) => match configured.herdr.close(&node) {
                signal_flow::PaneCloseOutcome::Closed => Outcome::Closed,
                signal_flow::PaneCloseOutcome::Refused(failure) => {
                    Outcome::Failed(Failed_Data::PaneCloseRefused(failure))
                }
            },
            Operation::Wake(address) => self.wake(address),
            Operation::Drain(address) => self.drain(address),
        }
    }
}

pub(crate) trait PerformsConfiguredOperations {
    fn perform_configured(&self, configured: &ConfiguredUse, operation: Operation) -> Outcome;
    #[cfg(test)]
    fn perform_ingress(&self, operation: Operation) -> Outcome;
}

impl PerformsConfiguredOperations for RunningNexus {
    /// Performs one part of an external effect with the configuration selected
    /// by its ingress, then exposes only the configured effects capability.
    fn perform_configured(&self, configured: &ConfiguredUse, operation: Operation) -> Outcome {
        self.configured_effects(configured).perform(operation)
    }

    /// Test-only convenience: production ingress always carries ConfiguredEffects.
    #[cfg(test)]
    fn perform_ingress(&self, operation: Operation) -> Outcome {
        match self.test_configured_use() {
            Ok(configured) => self.configured_effects(&configured).perform(operation),
            Err(_) => Outcome::Failed(Failed_Data::StoreRefused),
        }
    }
}

#[cfg(test)]
impl RunningNexus {
    pub(crate) fn perform_ingress(&self, operation: Operation) -> Outcome {
        <Self as PerformsConfiguredOperations>::perform_ingress(self, operation)
    }
}

/// The arms of `perform` that are longer than one adapter call.
trait PerformsInParts {
    /// Opens the launch attempt; a new Claude attempt also claims its
    /// FlowId for the session Flow chose and holds the flow in Memory, so
    /// the id exists before any harness runs. An attempt that already
    /// existed reserves nothing again: it is not spawned again.
    fn reserve(&self, configured: &ConfiguredUse, reserve: Reserve_Data) -> Outcome;
    fn record(&self, configured: &ConfiguredUse, record: Record_Data) -> Outcome;
    /// Gives back what Reserve took for a launch that was then refused: the
    /// flow held in Memory, then the FlowId claim. Both are needed for
    /// Released; a failure of either is reported to the Nexus log.
    fn release(&self, configured: &ConfiguredUse, flow_id: &str) -> Outcome;
}

impl PerformsInParts for ConfiguredEffects<'_> {
    fn reserve(&self, configured: &ConfiguredUse, reserve: Reserve_Data) -> Outcome {
        use crate::herdr::reservation::ReservesFlowIdentity;
        use crate::store::events::RecordsHarnessEvents;
        let Reserve_Data {
            composed_launch,
            origin,
        } = reserve;
        let reservation = match self
            .operational
            .reserve_launch_attempt(&composed_launch, origin)
        {
            Ok(reservation) => reservation,
            Err(_) => return Outcome::Failed(Failed_Data::StoreRefused),
        };
        let flow_id_option = match &reservation {
            LaunchAttemptReservation::Reserved(_) => {
                match configured.herdr.reserve_flow_identity(&composed_launch) {
                    Ok(flow_id_option) => flow_id_option,
                    Err(error) => {
                        eprintln!(
                            "flow-nexus: launch {} FlowId not claimed: {error}",
                            composed_launch.launch_profile.launch_request_id
                        );
                        return Outcome::Failed(Failed_Data::ClaimRefused);
                    }
                }
            }
            LaunchAttemptReservation::Existing(_) | LaunchAttemptReservation::Conflict => None,
        };
        if let Some(flow_id) = &flow_id_option
            && self.operational.hold_reserved_flow(flow_id).is_err()
        {
            // The claim was made but the flow is not held: give it back.
            let _ = self.release(configured, flow_id);
            return Outcome::Failed(Failed_Data::StoreRefused);
        }
        Outcome::Reserved(Reserved_Data {
            launch_attempt_reservation: reservation,
            flow_id_option,
        })
    }

    fn release(&self, configured: &ConfiguredUse, flow_id: &str) -> Outcome {
        use crate::herdr::reservation::ReleasesFlowIdentity;
        use crate::store::events::RecordsHarnessEvents;
        if let Err(error) = self.operational.release_reserved_flow(flow_id) {
            eprintln!("flow-nexus: reserved flow {flow_id} not released: {error:?}");
            return Outcome::Failed(Failed_Data::StoreRefused);
        }
        match configured.herdr.release_flow_identity(flow_id) {
            Ok(()) => Outcome::Released,
            Err(error) => {
                eprintln!("flow-nexus: FlowId {flow_id} claim not released: {error}");
                Outcome::Failed(Failed_Data::ClaimRefused)
            }
        }
    }

    fn record(&self, configured: &ConfiguredUse, record: Record_Data) -> Outcome {
        match record {
            Record_Data::Intent(intent) => self
                .operational
                .record_native_launch_intent(intent)
                .recorded(),
            Record_Data::Binding(binding) => self
                .operational
                .record_native_launch_binding(binding)
                .recorded(),
            Record_Data::Acknowledgement(acknowledgement) => self
                .operational
                .record_registration_acknowledgement(acknowledgement)
                .recorded(),
            Record_Data::Delivery(intent) => self
                .operational
                .record_prompt_delivery_intent(intent)
                .recorded(),
            Record_Data::Delivered(result) => self
                .operational
                .record_prompt_delivery_result(result)
                .recorded(),
            Record_Data::Active(flow_id) => self.operational.record_active(&flow_id).recorded(),
            Record_Data::Stopped(flow_id) => self.operational.record_stopped(&flow_id).recorded(),
            Record_Data::Retired(flow_id) => self.operational.record_retired(&flow_id).recorded(),
            // No pane outlives its flow: a flow that exited with its pane
            // still there, its harness gone or its own session ended, has
            // that pane closed in the event that records the exit. A pane
            // now running another session is never closed. The close takes
            // no pane lease: an exit is recorded where a write may already
            // hold it, and nothing more is typed into an exited flow.
            Record_Data::Exited(flow_id) => {
                let node = self.operational.flow_node(&flow_id).ok().flatten();
                let recorded = self.operational.record_exited(&flow_id).recorded();
                if recorded == Outcome::Recorded
                    && let Some(node) = node
                    && matches!(
                        configured.herdr.pane_presence(&node),
                        crate::herdr::PanePresence::Present | crate::herdr::PanePresence::Bare
                    )
                    && let signal_flow::PaneCloseOutcome::Refused(failure) =
                        configured.herdr.close(&node)
                {
                    eprintln!(
                        "flow-nexus: flow {flow_id} exited; its pane was not closed: {failure:?}"
                    );
                }
                recorded
            }
            Record_Data::Replacing(replacement) => {
                self.operational.record_replacement(replacement).recorded()
            }
            Record_Data::Withdrawn(launch_request_id) => self
                .operational
                .withdraw_replacement(&launch_request_id)
                .recorded(),
            Record_Data::Settled(Record_Data_Settled_Data {
                launch_request_id,
                launch_outcome,
            }) => self
                .operational
                .record_launch_outcome(&launch_request_id, launch_outcome)
                .recorded(),
            Record_Data::Asleep(Record_Data_Asleep_Data { address, flow_id }) => {
                use crate::store::metaflow::SleepsMetaflows;
                self.operational
                    .sleep_metaflow(&address, &flow_id)
                    .recorded()
            }
            Record_Data::Awoken(Record_Data_Awoken_Data {
                address,
                flow_id,
                delivered,
            }) => {
                use crate::store::metaflow::SleepsMetaflows;
                match usize::try_from(delivered) {
                    Ok(delivered) => self
                        .operational
                        .awaken_metaflow(&address, &flow_id, delivered)
                        .recorded(),
                    Err(_) => Outcome::Failed(Failed_Data::StoreRefused),
                }
            }
            Record_Data::Context(Record_Data_Context_Data { flow_id, tokens }) => {
                use crate::store::context::RecordsContextSize;
                self.operational
                    .record_context_size(&flow_id, tokens)
                    .recorded()
            }
            Record_Data::Harness(Record_Data_Harness_Data { flow_id, event }) => {
                use crate::store::events::{EventRecording, RecordsHarnessEvents};
                match self.operational.record_event(&flow_id, event) {
                    Ok(EventRecording::Recorded) => Outcome::Recorded,
                    Ok(EventRecording::UnknownFlow) => Outcome::Failed(Failed_Data::UnknownFlow),
                    Err(_) => Outcome::Failed(Failed_Data::StoreRefused),
                }
            }
        }
    }
}

#[cfg(test)]
mod failure_tests {
    use super::{ClassifiesNativeLaunchFailure, RefusesNativeLaunch};
    use crate::generated::operation::{Failed_Data, Outcome};
    use signal_flow::{NativeLaunchFailure, NativeLaunchRefusal, NativeLaunchStage};

    #[test]
    fn a_binding_failure_is_answered_with_its_stage_and_allowlisted_category() {
        assert_eq!(
            NativeLaunchFailure::from_adapter_error(
                "official Herdr integration reported no native session",
            ),
            NativeLaunchFailure::NativeSessionMissing
        );
        assert_eq!(
            NativeLaunchRefusal::refused(
                NativeLaunchStage::Bind,
                "official Herdr integration reported no native session".into()
            ),
            Outcome::Failed(Failed_Data::LaunchRefused(NativeLaunchRefusal {
                native_launch_stage: NativeLaunchStage::Bind,
                native_launch_failure: NativeLaunchFailure::NativeSessionMissing,
            }))
        );
    }

    #[test]
    fn a_title_refusal_is_answered_and_logged_by_its_typed_category() {
        use crate::herdr::launch::{NativeTargetRefusal, TitleRefusal};
        let missing = TitleRefusal::Target(NativeTargetRefusal::NoNativeSession);
        let failure = NativeLaunchRefusal {
            native_launch_stage: NativeLaunchStage::Naming,
            native_launch_failure: (&missing).into(),
        };
        assert_eq!(
            failure.log_line(),
            "flow-nexus: herdr stage=title category=native-session-missing"
        );
        assert_eq!(
            NativeLaunchRefusal::refused_title(&missing),
            Outcome::Failed(Failed_Data::LaunchRefused(failure))
        );
        for (refusal, label) in [
            (TitleRefusal::NotReady, "not-ready"),
            (TitleRefusal::ReadbackDiffers, "title-readback-differs"),
            (
                TitleRefusal::Title(crate::title::TitleRefused::InvalidFlowId),
                "invalid-flow-id",
            ),
            (
                TitleRefusal::Target(NativeTargetRefusal::HerdrUnreadable(
                    "secret=/private/socket".into(),
                )),
                "herdr-unreadable",
            ),
        ] {
            let line = NativeLaunchRefusal {
                native_launch_stage: NativeLaunchStage::Naming,
                native_launch_failure: (&refusal).into(),
            }
            .log_line();
            assert!(line.ends_with(&format!("category={label}")), "{line}");
            assert!(!line.contains("secret="));
        }
        // The adapter's own words for a missing session are classified, not
        // filed as a catch-all refusal.
        assert_eq!(
            NativeLaunchFailure::from_adapter_error(
                "receipt target has no official native session"
            ),
            NativeLaunchFailure::NativeSessionMissing
        );
    }

    #[test]
    fn a_workspace_label_refusal_reaches_the_wire_as_its_own_category() {
        use crate::herdr::launch::TitleRefusal;
        for (refusal, category, label) in [
            (
                TitleRefusal::WorkspaceLabelRefused,
                NativeLaunchFailure::WorkspaceLabelRefused,
                "workspace-label-refused",
            ),
            (
                TitleRefusal::PaneLabelRefused,
                NativeLaunchFailure::PaneLabelRefused,
                "pane-label-refused",
            ),
        ] {
            let failure = NativeLaunchRefusal {
                native_launch_stage: NativeLaunchStage::Naming,
                native_launch_failure: category,
            };
            assert_eq!(
                NativeLaunchRefusal::refused_title(&refusal),
                Outcome::Failed(Failed_Data::LaunchRefused(failure.clone()))
            );
            assert_eq!(
                failure.log_line(),
                format!("flow-nexus: herdr stage=title category={label}")
            );
        }
    }

    #[test]
    fn malicious_or_long_adapter_text_is_not_retained_or_rendered() {
        let attacker_text = format!("secret=/private/socket {}", "x".repeat(20_000));
        let failure = NativeLaunchRefusal {
            native_launch_stage: NativeLaunchStage::Spawn,
            native_launch_failure: NativeLaunchFailure::from_adapter_error(&attacker_text),
        };
        let line = failure.log_line();
        assert_eq!(
            failure.native_launch_failure,
            NativeLaunchFailure::AdapterRefused
        );
        assert!(!line.contains("secret="));
        assert!(!line.contains("/private/socket"));
        assert!(line.len() <= 96, "bounded fixed diagnostic");
        assert!(!format!("{failure:?}").contains("secret="));
    }
}
