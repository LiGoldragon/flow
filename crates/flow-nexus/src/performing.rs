//! The Operation root at work: every effect the running Nexus performs is
//! one generated `Operation`, performed through `Performs` on the Nexus that
//! holds the store, Herdr, the Codex endpoints and the composer, and every
//! answer is one generated `Outcome`. The launch, stop, retire and delivery
//! paths decide what to do next; this is the only place they act.

use crate::RunningNexus;
use crate::codex::{
    AccountsOpenedThread, OpensCodexThread, SelectsCodexEndpoint, SubmitsBoundCodexPart,
};
use crate::composition::{ComposesInRoot, CompositionError, HandoverRoot, HarnessedProfile};
use crate::generated::operation::{
    Failed_Data, Open_Data, Operation, Outcome, PaneLaunch, Record_Data, Record_Data_Asleep_Data,
    Record_Data_Awoken_Data, Record_Data_Context_Data, Record_Data_Harness_Data,
    Record_Data_Settled_Data, Register_Data, Reserve_Data, Reserved_Data, Submit_Data, Title_Data,
};
use crate::generated::starter::DerivesHarness;
use crate::herdr::launch::{
    CreatesHerdrLaunchPane, NativeTargetRefusal, ObservesNativeLaunchBinding, SpawnFailure,
    StartsConfiguredNativeHerdrHarness, SubmitsStartupPart, TitleRefusal, TitlesNativeFlow,
};
use crate::herdr::{OperatesHerdrPane, ReadsHerdrPanes};
use crate::launching::ResolvesLaunchRoot;
use crate::live::HoldsLiveValue;
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
impl Performs for RunningNexus {
    fn perform(&self, operation: Operation) -> Outcome {
        match operation {
            // A model that is not exact on its harness is never launched: its
            // receipt could not match the model the transcript records.
            Operation::Compose(profile)
                if !self
                    .codex_endpoints
                    .current()
                    .launches_exactly(&profile.model_name.0) =>
            {
                eprintln!(
                    "flow-nexus: launch {} not composed: model {} is not an exact native model",
                    profile.launch_request_id.0, profile.model_name.0
                );
                Outcome::Failed(Failed_Data::CompositionRefused(
                    signal_flow::CompositionRefusal::InexactModel,
                ))
            }
            Operation::Compose(profile) => {
                // A Start naming no leave-outs takes its permaflow's, from
                // its declaration as it stands now; the composition checks
                // them against the harness it starts on.
                let harness_kind = self
                    .codex_endpoints
                    .current()
                    .harness_for(&profile.model_name.0);
                let default = self
                    .store
                    .declared_leave_outs(&profile.address)
                    .unwrap_or_default();
                let request = profile.launch_request_id.clone();
                // The launch's Remembering, resolved to the flows whose raw
                // records the composer reads into its first message.
                let remembered_flows = {
                    use crate::store::metaflow::ResolvesRemembering;
                    match self.store.remembered_flows(&profile) {
                        Ok(flows) => flows,
                        Err(_) => return Outcome::Failed(Failed_Data::StoreRefused),
                    }
                };
                // A successor's handover is read under the root its flow runs
                // in. A launch that names no predecessor needs none; one that
                // names a predecessor and whose root cannot be resolved is
                // refused if it is given a handover.
                let launch_root = match profile.flow_id_option {
                    None => None,
                    Some(_) => Some(self.launch_root(&profile.address).inspect_err(|rejection| {
                        eprintln!(
                            "flow-nexus: launch {request} root unresolved: {rejection:?}",
                            request = request.0
                        );
                    })),
                };
                // A successor is given every request delivered to its
                // permaflow's flows and still open, under the id it was
                // delivered with.
                let open_requests = match profile.flow_id_option {
                    None => Vec::new(),
                    Some(_) => {
                        use crate::store::logical_delivery::HoldsOpenRequests;
                        match self.store.open_requests(&profile.address) {
                            Ok(open) => open,
                            Err(_) => return Outcome::Failed(Failed_Data::StoreRefused),
                        }
                    }
                };
                match self.composer.current().compose_in_root(
                    HarnessedProfile {
                        remembered_flows: crate::generated::starter::RememberedFlows(
                            remembered_flows,
                        ),
                        harness_kind,
                        launch_profile: profile,
                    },
                    default,
                    match &launch_root {
                        None => HandoverRoot::Unread,
                        Some(Ok(root)) => HandoverRoot::At(std::path::Path::new(&root.0)),
                        Some(Err(_)) => HandoverRoot::Unresolved,
                    },
                    &open_requests,
                ) {
                    Ok(launch) => Outcome::Composed(launch),
                    Err(CompositionError::LeaveOut(refusal)) => {
                        eprintln!(
                            "flow-nexus: launch {request} leave-outs refused: {refusal:?}",
                            request = request.0
                        );
                        Outcome::Failed(Failed_Data::LeaveOutRefused(refusal))
                    }
                    Err(CompositionError::Context(refusal)) => {
                        eprintln!(
                            "flow-nexus: launch {request} context refused: {refusal:?}",
                            request = request.0
                        );
                        Outcome::Failed(Failed_Data::ContextRefused(refusal))
                    }
                    Err(CompositionError::InvalidProfileField(refusal)) => {
                        eprintln!(
                            "flow-nexus: launch {request} not composed: {refusal:?}",
                            request = request.0
                        );
                        Outcome::Failed(Failed_Data::CompositionRefused(refusal))
                    }
                    Err(error @ CompositionError::PresentationPromptUnavailable(_)) => {
                        eprintln!(
                            "flow-nexus: launch {request} not composed: {error}",
                            request = request.0
                        );
                        Outcome::Failed(Failed_Data::CompositionRefused(
                            signal_flow::CompositionRefusal::PresentationUnavailable,
                        ))
                    }
                    Err(CompositionError::HandoverUnreadable { predecessor, fault }) => {
                        eprintln!(
                            "flow-nexus: launch {request} not composed: the handover of {predecessor} could not be read: {fault:?}",
                            request = request.0
                        );
                        Outcome::Failed(Failed_Data::CompositionRefused(
                            signal_flow::CompositionRefusal::HandoverUnreadable(
                                signal_flow::HandoverUnreadable_Data {
                                    flow_id: signal_flow::FlowId(predecessor),
                                    handover_fault: fault,
                                },
                            ),
                        ))
                    }
                }
            }
            Operation::Reserve(reserve) => self.reserve(reserve),
            Operation::Record(record) => self.record(record),
            Operation::Register(Register_Data {
                flow_node,
                caller_identity: caller,
            }) => match self.store.register_flow_in_role(flow_node, caller) {
                Ok(FlowRegistration::Registered(node)) => Outcome::Registered(*node),
                Ok(FlowRegistration::ConflictingBinding) => {
                    Outcome::Failed(Failed_Data::ConflictingBinding)
                }
                Err(_) => Outcome::Failed(Failed_Data::StoreRefused),
            },
            Operation::Confirm(flow_id) => match self.store.confirm_started(&flow_id.0) {
                Ok(Response::Started(started)) => Outcome::Started(started),
                Ok(Response::StartRejected(StartRejection::AddressAwake)) => {
                    Outcome::Failed(Failed_Data::AddressAwake)
                }
                Ok(_) => Outcome::Failed(Failed_Data::Unstarted),
                Err(_) => Outcome::Failed(Failed_Data::StoreRefused),
            },
            Operation::Open(Open_Data {
                composed_launch,
                root,
            }) => match self
                .herdr
                .create_launch_pane_in(&composed_launch, std::path::Path::new(&root.0))
            {
                Ok(pane) => Outcome::Opened(pane),
                Err(error) => NativeLaunchRefusal::refused(NativeLaunchStage::Open, error),
            },
            Operation::Spawn(PaneLaunch {
                composed_launch,
                root,
                herdr_pane_binding,
                flow_id_option,
                harness_arguments,
                native_session_id_option: _,
            }) => {
                // Codex: the thread is opened lean through the app-server
                // and named with its title, then the TUI attaches to it while
                // it is idle; its first turn is started at Submit, once the
                // pane is bound.
                let opened = match composed_launch.harness_kind {
                    HarnessKind::Codex => match self
                        .codex_endpoints
                        .current()
                        .adapter_for(&composed_launch.launch_profile.model_name.0)
                        .and_then(|adapter| {
                            adapter.open_thread_in(
                                &composed_launch,
                                std::path::Path::new(&root.0),
                                &mut |thread| {
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
                                        .store
                                        .record_opened_thread(
                                            &composed_launch.launch_profile.launch_request_id.0,
                                            thread,
                                        )
                                        .unwrap_or(false)
                                    {
                                        return Err(refused(format!(
                                            "opened thread {thread} not recorded as the launch's"
                                        )));
                                    }
                                    let flow_id = self
                                        .herdr
                                        .claim_flow_identity(&HarnessKind::Codex, thread)
                                        .map_err(refused)?;
                                    composed_launch
                                        .launch_profile
                                        .native_title(&flow_id)
                                        .map(|title| title.as_str().to_owned())
                                        .map_err(|refusal| refused(refusal.to_string()))
                                },
                            )
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
                                composed_launch.launch_profile.launch_request_id.0
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
                    Some(thread) => self.herdr.attach_configured_codex_harness(
                        &composed_launch,
                        &herdr_pane_binding,
                        thread,
                        &harness_arguments.0,
                    ),
                    None => self.herdr.start_configured_native_harness(
                        &composed_launch,
                        &herdr_pane_binding,
                        flow_id_option.as_ref().map(|value| value.0.as_str()),
                        &harness_arguments.0,
                    ),
                };
                match started {
                    Ok(()) => Outcome::Spawned(opened.map(signal_flow::NativeSessionId)),
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
                            match self
                                .codex_endpoints
                                .current()
                                .adapter_for(&composed_launch.launch_profile.model_name.0)
                            {
                                Ok(adapter) => adapter.account_opened_thread(&thread),
                                Err(_) => signal_flow::OpenedThread {
                                    native_session_id: signal_flow::NativeSessionId(thread),
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
                ..
            }) => {
                let observed = match native_session_id_option {
                    Some(thread) => self.herdr.observe_attached_codex_binding(
                        &composed_launch,
                        &herdr_pane_binding,
                        &thread.0,
                    ),
                    None => self.herdr.observe_native_binding(
                        &composed_launch,
                        &herdr_pane_binding,
                        flow_id_option.as_ref().map(|value| value.0.as_str()),
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
            }) => match self
                .herdr
                .title_native_flow(&composed_launch, &native_launch_binding)
            {
                Ok(_) => Outcome::Titled,
                Err(refusal) => NativeLaunchRefusal::refused_title(&refusal),
            },
            Operation::Submit(Submit_Data {
                composed_launch,
                prompt_delivery_intent,
            }) => {
                let submission = match composed_launch.harness_kind {
                    // The part's turn may call the send tool at once: the
                    // launch, holding the gate, answers it meanwhile.
                    HarnessKind::Codex => self.serving_launch_calls(|| {
                        self.codex_endpoints
                            .current()
                            .adapter_for(&composed_launch.launch_profile.model_name.0)
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
                    HarnessKind::Claude => self
                        .herdr
                        .submit_startup_part(&composed_launch, &prompt_delivery_intent)
                        .map_err(|_| Failed_Data::HerdrRefused),
                };
                match submission {
                    Ok(result) => Outcome::Submitted(result),
                    Err(failure) => Outcome::Failed(failure),
                }
            }
            Operation::Release(flow_id) => self.release(&flow_id.0),
            Operation::Close(node) => match self.herdr.close(&node) {
                true => Outcome::Closed,
                false => Outcome::Failed(Failed_Data::HerdrRefused),
            },
            // A restart's resume: the session the flow holds, in the new
            // pane, with its FlowId; no prompt follows.
            Operation::Resume(PaneLaunch {
                composed_launch,
                herdr_pane_binding,
                flow_id_option,
                native_session_id_option,
                harness_arguments,
                ..
            }) => {
                use crate::herdr::launch::ResumesHarness;
                let (Some(flow_id), Some(session)) = (flow_id_option, native_session_id_option)
                else {
                    return NativeLaunchRefusal::refused(
                        NativeLaunchStage::Spawn,
                        "a resume needs its FlowId and native session".into(),
                    );
                };
                match self.herdr.resume_configured_harness(
                    &composed_launch,
                    &herdr_pane_binding,
                    &flow_id.0,
                    &session.0,
                    &harness_arguments.0,
                ) {
                    Ok(()) => Outcome::Spawned(None),
                    Err(SpawnFailure::Refused(spawn_refusal_cause)) => Outcome::Failed(
                        Failed_Data::SpawnRefused(signal_flow::HarnessSpawnRefusal {
                            spawn_refusal_cause,
                            opened_thread_option: None,
                        }),
                    ),
                    Err(SpawnFailure::Unattempted(error)) => {
                        NativeLaunchRefusal::refused(NativeLaunchStage::Spawn, error)
                    }
                }
            }
            Operation::Discard(pane) => {
                use crate::restarting::DiscardsCreatedPane;
                match self.herdr.discard_created(&pane) {
                    true => Outcome::Closed,
                    false => Outcome::Failed(Failed_Data::HerdrRefused),
                }
            }
            Operation::Wake(address) => self.wake(address),
            Operation::Drain(address) => self.drain(address),
        }
    }
}

/// The arms of `perform` that are longer than one adapter call.
trait PerformsInParts {
    /// Opens the launch attempt; a new Claude attempt also claims its
    /// FlowId for the session Flow chose and holds the flow in Memory, so
    /// the id exists before any harness runs. An attempt that already
    /// existed reserves nothing again: it is not spawned again.
    fn reserve(&self, reserve: Reserve_Data) -> Outcome;
    fn record(&self, record: Record_Data) -> Outcome;
    /// Gives back what Reserve took for a launch that was then refused: the
    /// flow held in Memory, then the FlowId claim. Both are needed for
    /// Released; a failure of either is reported to the Nexus log.
    fn release(&self, flow_id: &str) -> Outcome;
}

impl PerformsInParts for RunningNexus {
    fn reserve(&self, reserve: Reserve_Data) -> Outcome {
        use crate::herdr::reservation::ReservesFlowIdentity;
        use crate::store::events::RecordsHarnessEvents;
        let Reserve_Data {
            composed_launch,
            origin,
        } = reserve;
        let reservation = match self.store.reserve_launch_attempt(&composed_launch, origin) {
            Ok(reservation) => reservation,
            Err(_) => return Outcome::Failed(Failed_Data::StoreRefused),
        };
        let flow_id_option = match &reservation {
            LaunchAttemptReservation::Reserved(_) => {
                match self.herdr.reserve_flow_identity(&composed_launch) {
                    Ok(flow_id_option) => flow_id_option,
                    Err(error) => {
                        eprintln!(
                            "flow-nexus: launch {} FlowId not claimed: {error}",
                            composed_launch.launch_profile.launch_request_id.0
                        );
                        return Outcome::Failed(Failed_Data::ClaimRefused);
                    }
                }
            }
            LaunchAttemptReservation::Existing(_) | LaunchAttemptReservation::Conflict => None,
        };
        if let Some(flow_id) = &flow_id_option
            && self.store.hold_reserved_flow(flow_id).is_err()
        {
            // The claim was made but the flow is not held: give it back.
            let _ = self.release(flow_id);
            return Outcome::Failed(Failed_Data::StoreRefused);
        }
        Outcome::Reserved(Reserved_Data {
            launch_attempt_reservation: reservation,
            flow_id_option: flow_id_option.map(signal_flow::FlowId),
        })
    }

    fn release(&self, flow_id: &str) -> Outcome {
        use crate::herdr::reservation::ReleasesFlowIdentity;
        use crate::store::events::RecordsHarnessEvents;
        if let Err(error) = self.store.release_reserved_flow(flow_id) {
            eprintln!("flow-nexus: reserved flow {flow_id} not released: {error:?}");
            return Outcome::Failed(Failed_Data::StoreRefused);
        }
        match self.herdr.release_flow_identity(flow_id) {
            Ok(()) => Outcome::Released,
            Err(error) => {
                eprintln!("flow-nexus: FlowId {flow_id} claim not released: {error}");
                Outcome::Failed(Failed_Data::ClaimRefused)
            }
        }
    }

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
            Record_Data::Active(flow_id) => self.store.record_active(&flow_id.0).recorded(),
            Record_Data::Stopped(flow_id) => self.store.record_stopped(&flow_id.0).recorded(),
            Record_Data::Retired(flow_id) => self.store.record_retired(&flow_id.0).recorded(),
            // No pane outlives its flow: a flow that exited with its pane
            // still there, its harness gone or its own session ended, has
            // that pane closed in the event that records the exit. A pane
            // now running another session is never closed. The close takes
            // no pane lease: an exit is recorded where a write may already
            // hold it, and nothing more is typed into an exited flow.
            // A flow a restart holds has not ended: the pane it left is the
            // restart's to close, and its seat's own exit is fenced.
            Record_Data::Exited(flow_id)
                if !matches!(
                    crate::store::restart::RecordsRestarts::restart_record(
                        &*self.store,
                        &flow_id.0
                    ),
                    Ok(None)
                ) =>
            {
                eprintln!(
                    "flow-nexus: flow {flow_id} is restarting; no exit is recorded for it",
                    flow_id = flow_id.0
                );
                Outcome::Failed(Failed_Data::Restarting)
            }
            Record_Data::Exited(flow_id) => {
                let node = self.store.flow_node(&flow_id.0).ok().flatten();
                let recorded = self.store.record_exited(&flow_id.0).recorded();
                if recorded == Outcome::Recorded
                    && let Some(node) = node
                    && matches!(
                        self.herdr.pane_presence(&node),
                        crate::herdr::PanePresence::Present | crate::herdr::PanePresence::Bare
                    )
                    && !self.herdr.close(&node)
                {
                    eprintln!(
                        "flow-nexus: flow {flow_id} exited; its pane was not closed",
                        flow_id = flow_id.0
                    );
                }
                recorded
            }
            Record_Data::Replacing(replacement) => {
                self.store.record_replacement(replacement).recorded()
            }
            Record_Data::Withdrawn(launch_request_id) => self
                .store
                .withdraw_replacement(&launch_request_id.0)
                .recorded(),
            Record_Data::Settled(Record_Data_Settled_Data {
                launch_request_id,
                launch_outcome,
            }) => self
                .store
                .record_launch_outcome(&launch_request_id.0, launch_outcome)
                .recorded(),
            Record_Data::Asleep(Record_Data_Asleep_Data { address, flow_id }) => {
                use crate::store::metaflow::SleepsMetaflows;
                self.store.sleep_metaflow(&address, &flow_id.0).recorded()
            }
            Record_Data::Awoken(Record_Data_Awoken_Data {
                address,
                flow_id,
                delivered_count: delivered,
            }) => {
                use crate::store::metaflow::SleepsMetaflows;
                match usize::try_from(delivered.0) {
                    Ok(delivered) => self
                        .store
                        .awaken_metaflow(&address, &flow_id.0, delivered)
                        .recorded(),
                    Err(_) => Outcome::Failed(Failed_Data::StoreRefused),
                }
            }
            Record_Data::Restart(record) => {
                use crate::store::restart::RecordsRestarts;
                self.store.record_restart(record).recorded()
            }
            Record_Data::Rebound(birth) => {
                use crate::store::restart::RecordsRestarts;
                self.store.rebind_restarted(birth).recorded()
            }
            Record_Data::Resumed(resumed) => {
                use crate::store::metaflow::SleepsMetaflows;
                self.store
                    .resume_metaflow(&resumed.address, resumed.seat_birth)
                    .recorded()
            }
            Record_Data::Context(Record_Data_Context_Data { flow_id, tokens }) => {
                use crate::store::context::RecordsContextSize;
                self.store
                    .record_context_size(&flow_id.0, tokens.0)
                    .recorded()
            }
            Record_Data::Harness(Record_Data_Harness_Data { flow_id, event }) => {
                use crate::store::events::{EventRecording, RecordsHarnessEvents};
                match self.store.record_event(&flow_id.0, event) {
                    Ok(EventRecording::Recorded) => Outcome::Recorded,
                    Ok(EventRecording::UnknownFlow) => Outcome::Failed(Failed_Data::UnknownFlow),
                    Err(_) => Outcome::Failed(Failed_Data::StoreRefused),
                }
            }
            // The obligation records: each one commit (`store::pending_reap`,
            // `store::pending_handover`), the generation it closes judged
            // where the record needs one.
            Record_Data::ReapOwed(owed) => {
                use crate::store::pending_reap::RecordsPendingReaps;
                self.store
                    .reap_owed(owed.pending_reap, owed.replaced_record)
                    .recorded()
            }
            Record_Data::ReapRefused(refused) => {
                use crate::store::pending_reap::RecordsPendingReaps;
                self.store
                    .reap_refused(&refused.flow_id.0, refused.reap_standing)
                    .recorded()
            }
            Record_Data::Reaped(flow_id) => {
                use crate::reaping::ReapsPredecessors;
                match self.settle_reap(&flow_id.0) {
                    Some(Response::Replaced(_)) => Outcome::Recorded,
                    _ => Outcome::Failed(Failed_Data::StoreRefused),
                }
            }
            Record_Data::HandoverOwed(owed) => {
                use crate::store::pending_handover::RecordsPendingHandovers;
                self.store
                    .handover_owed(owed.pending_handover, owed.launched_record)
                    .recorded()
            }
            Record_Data::HandoverRefused(refused) => {
                use crate::store::pending_handover::RecordsPendingHandovers;
                self.store
                    .handover_refused(&refused.address, refused.handover_close_refusal)
                    .recorded()
            }
            Record_Data::HandedOver(address) => {
                use crate::reaping::HandsOverGenerations;
                match self.retry_handover(&address) {
                    Some(Response::Started(_)) => Outcome::Recorded,
                    _ => Outcome::Failed(Failed_Data::StoreRefused),
                }
            }
            Record_Data::StartedHandedOver(launched) => {
                use crate::reaping::HandsOverGenerations;
                use crate::store::ReadsLaunchAttempt;
                let Some((launch_request_id, address)) = self
                    .store
                    .launch_requests_bound_to(&launched.flow_id.0)
                    .ok()
                    .and_then(|mut ids| ids.pop())
                    .and_then(|id| {
                        self.store
                            .launch_attempt(&id)
                            .ok()
                            .flatten()
                            .map(|attempt| (id, attempt.launch_profile.address))
                    })
                else {
                    return Outcome::Failed(Failed_Data::StoreRefused);
                };
                use crate::generated::starter::NamesLaunchedRoute;
                let started = self.store.launched(&launched);
                match self.settle_fresh(&launch_request_id, &address, &started) {
                    Response::Started(_) => Outcome::Recorded,
                    _ => Outcome::Failed(Failed_Data::StoreRefused),
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
