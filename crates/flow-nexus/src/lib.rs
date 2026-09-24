//! Flow Nexus dispatches typed ordinary and privileged Signal requests.
pub mod claude;
pub mod codex;
pub mod composition;
pub mod herdr;
pub mod store;

use codex::{
    CodexEndpoints, ConsumesResetCredit, ResolvesBoundCodexSkills, SubmitsBoundCodexFirstTurn,
};
use composition::{ComposesLaunch, LaunchComposer, OpensLaunchComposer};
use herdr::launch::{
    AcceptsLaunchRegistration, CreatesHerdrLaunchPane, ObservesNativeLaunchBinding,
    ObservesNativeTargetReceipt, ResolvesClaudeNativeSkills, StartsNativeHerdrHarness,
    SubmitsFirstPromptOnce,
};
use signal_flow::{
    EndpointSelection, FlowLifecycle, FlowNode, HarnessKind, HerdrRoute, HerdrRouteSelection,
    LaunchAttemptPhase, LaunchAttemptReservation, NativeLaunchIntent, PromptDeliveryResult, Query,
    RegistrationAcknowledgement, Response, RestartRejection, StartRejection,
};
use std::{
    collections::HashSet,
    fs,
    io::{Read, Write},
    os::unix::{
        fs::{FileTypeExt, MetadataExt, PermissionsExt},
        net::{UnixListener, UnixStream},
    },
    path::{Path, PathBuf},
};
use store::{
    AppliesFlowQuery, AuthorizesFlowRestart, ConfiguresFlowStore, ConfirmsExistingFlow, ReadsExistingConfirmationBinding,
    ConfirmsStartedFlow, FlowStore, OpensFlowStore, ReadsLaunchAttempt, RecordsNativeLaunchBinding,
    RecordsNativeLaunchIntent, RecordsPromptDeliveryIntent, RecordsPromptDeliveryResult,
    RecordsRegistrationAcknowledgement, RegistersExistingFlow, RegistersFlowIdentity,
    ReservesLaunchAttempt,
};

fn process_identity_matches(identity: &meta_signal_flow::ProcessIdentity) -> bool {
    let Ok(process_id) = u32::try_from(identity.process_id) else {
        return false;
    };
    let process_root = PathBuf::from(format!("/proc/{process_id}"));
    let Ok(metadata) = fs::metadata(&process_root) else {
        return false;
    };
    if i64::from(metadata.uid()) != identity.process_user_id {
        return false;
    }
    let Ok(stat) = fs::read_to_string(process_root.join("stat")) else {
        return false;
    };
    let Some((_, fields)) = stat.rsplit_once(") ") else {
        return false;
    };
    fields.split_whitespace().nth(19) == Some(identity.process_start_token.as_str())
}

fn process_cwd_matches(identity: &meta_signal_flow::ProcessIdentity, expected: &str) -> bool {
    let Ok(process_id) = u32::try_from(identity.process_id) else {
        return false;
    };
    let expected = Path::new(expected);
    expected.is_absolute()
        && fs::canonicalize(format!("/proc/{process_id}/cwd")).ok()
            == fs::canonicalize(expected).ok()
}

fn container_is_well_formed(container: &meta_signal_flow::FlowContainer) -> bool {
    !container.herdr_session_name.is_empty()
        && !container.meta_flow_owner_id.is_empty()
        && Path::new(&container.herdr_server_socket_path).is_absolute()
}

fn container_socket_is_live(container: &meta_signal_flow::FlowContainer) -> bool {
    fs::metadata(&container.herdr_server_socket_path)
        .map(|metadata| metadata.file_type().is_socket())
        .unwrap_or(false)
}

fn binding_is_well_formed(binding: &meta_signal_flow::FlowBinding) -> bool {
    !binding.flow_id.is_empty()
        && !binding.model_name.is_empty()
        && !binding.native_session_id.is_empty()
        && !binding.herdr_workspace_id.is_empty()
        && !binding.herdr_pane_id.is_empty()
        && !binding.herdr_tab_id.is_empty()
        && !binding.herdr_terminal_id.is_empty()
        && !binding.herdr_agent_name.is_empty()
        && Path::new(&binding.working_directory).is_absolute()
}

fn refused_binding(
    flow_id: String,
    reason: meta_signal_flow::FlowBindingRefusalReason,
) -> meta_signal_flow::FlowBindingResult {
    meta_signal_flow::FlowBindingResult::Refused(meta_signal_flow::RefusedFlowBinding {
        flow_id,
        flow_binding_refusal_reason: reason,
    })
}

pub struct RunningNexus {
    pub store: FlowStore,
    pub codex_endpoints: CodexEndpoints,
    pub herdr: herdr::HerdrCli,
    pub composer: LaunchComposer,
}

pub trait Dispatches {
    fn dispatch(&self, query: Query) -> Response;
    fn dispatch_meta(&self, query: meta_signal_flow::Query) -> meta_signal_flow::Response;
}

impl Dispatches for RunningNexus {
    fn dispatch(&self, query: Query) -> Response {
        match query {
            Query::Start(request) => {
                let origin = request.origin_clue.clone();
                let launch_request_id = request.launch_profile.launch_request_id.clone();
                let existing = match self.store.launch_attempt(&launch_request_id) {
                    Ok(existing) => existing,
                    Err(_) => {
                        return Response::StartRejected(StartRejection::LaunchPersistenceRefused);
                    }
                };
                if let Some(attempt) = existing {
                    if attempt.launch_profile != request.launch_profile
                        || attempt.origin_clue != origin
                    {
                        return Response::StartRejected(StartRejection::LaunchRequestConflict);
                    }
                    if attempt.launch_attempt_phase == LaunchAttemptPhase::PromptObserved {
                        let Some(binding) = attempt.native_launch_binding_option else {
                            return Response::StartRejected(
                                StartRejection::LaunchPersistenceRefused,
                            );
                        };
                        return self.store.confirm_started(&binding.flow_id).unwrap_or(
                            Response::StartRejected(StartRejection::LaunchPersistenceRefused),
                        );
                    }
                    if attempt.launch_attempt_phase != LaunchAttemptPhase::PromptAmbiguous {
                        return Response::LaunchPending(attempt);
                    }
                    let (Some(intent), Some(binding)) = (
                        attempt.prompt_delivery_intent_option,
                        attempt.native_launch_binding_option,
                    ) else {
                        return Response::StartRejected(StartRejection::LaunchPersistenceRefused);
                    };
                    let observed = self
                        .herdr
                        .observe_native_target_receipt(&intent)
                        .unwrap_or_else(|_| PromptDeliveryResult::Ambiguous(intent.clone()));
                    let receipt = match observed {
                        PromptDeliveryResult::Observed(receipt) => receipt,
                        PromptDeliveryResult::Ambiguous(updated) => {
                            if updated != intent
                                && !self
                                    .store
                                    .record_prompt_delivery_result(PromptDeliveryResult::Ambiguous(
                                        updated.clone(),
                                    ))
                                    .unwrap_or(false)
                            {
                                return Response::StartRejected(
                                    StartRejection::LaunchPersistenceRefused,
                                );
                            }
                            return Response::StartAmbiguous(updated);
                        }
                    };
                    if !self
                        .store
                        .record_prompt_delivery_result(PromptDeliveryResult::Observed(receipt))
                        .unwrap_or(false)
                    {
                        return Response::StartRejected(StartRejection::LaunchPersistenceRefused);
                    }
                    return self.store.confirm_started(&binding.flow_id).unwrap_or(
                        Response::StartRejected(StartRejection::LaunchPersistenceRefused),
                    );
                }
                let launch = match self.composer.compose(&request.launch_profile) {
                    Ok(launch) => launch,
                    Err(_) => return Response::StartRejected(StartRejection::CompositionRefused),
                };
                let codex_adapter = if launch.launch_profile.harness_kind == HarnessKind::Codex {
                    match self
                        .codex_endpoints
                        .adapter_for(&launch.launch_profile.model_name)
                    {
                        Ok(adapter) => Some(adapter),
                        Err(_) => {
                            return Response::StartRejected(StartRejection::NativeLaunchRefused);
                        }
                    }
                } else {
                    None
                };
                match self.store.reserve_launch_attempt(&launch, origin.clone()) {
                    Ok(LaunchAttemptReservation::Reserved(_)) => {}
                    Ok(LaunchAttemptReservation::Existing(attempt)) => {
                        return Response::LaunchPending(attempt);
                    }
                    Ok(LaunchAttemptReservation::Conflict) => {
                        return Response::StartRejected(StartRejection::LaunchRequestConflict);
                    }
                    Err(_) => {
                        return Response::StartRejected(StartRejection::LaunchPersistenceRefused);
                    }
                }
                let native_intent = NativeLaunchIntent {
                    launch_request_id: launch.launch_profile.launch_request_id.clone(),
                    prompt_sha256: launch.first_prompt_payload.prompt_sha256.clone(),
                    harness_kind: launch.launch_profile.harness_kind.clone(),
                    model_name: launch.launch_profile.model_name.clone(),
                    effort: launch.launch_profile.effort.clone(),
                    skill_name_vector: launch.launch_profile.skill_name_vector.clone(),
                };
                if !self
                    .store
                    .record_native_launch_intent(native_intent)
                    .unwrap_or(false)
                {
                    return Response::StartRejected(StartRejection::LaunchPersistenceRefused);
                }
                let pane = match self.herdr.create_launch_pane(&launch) {
                    Ok(pane) => pane,
                    Err(_) => {
                        return Response::StartRejected(StartRejection::NativeLaunchRefused);
                    }
                };
                if self.herdr.start_native_harness(&launch, &pane).is_err() {
                    return Response::StartRejected(StartRejection::NativeLaunchRefused);
                }
                let binding = match self.herdr.observe_native_binding(&launch, &pane) {
                    Ok(binding) => binding,
                    Err(_) => return Response::StartRejected(StartRejection::BindingRefused),
                };
                if !self
                    .store
                    .record_native_launch_binding(binding.clone())
                    .unwrap_or(false)
                {
                    return Response::StartRejected(StartRejection::LaunchPersistenceRefused);
                }
                let node = FlowNode {
                    flow_id: binding.flow_id.clone(),
                    session_id: binding.native_session_id.clone(),
                    harness_kind: binding.harness_kind.clone(),
                    endpoint_selection: EndpointSelection::Unavailable,
                    herdr_route_selection: HerdrRouteSelection::Available(HerdrRoute {
                        herdr_session_name: binding.herdr_pane_binding.herdr_session_name.clone(),
                        herdr_agent_name: binding.herdr_pane_binding.herdr_agent_name.clone(),
                        herdr_pane_id: binding.herdr_pane_binding.herdr_pane_id.clone(),
                        herdr_terminal_id: binding.herdr_pane_binding.herdr_terminal_id.clone(),
                    }),
                    origin_clue: origin,
                    flow_lifecycle: FlowLifecycle::Pending,
                };
                if !self.herdr.validate_registration(&node) {
                    return Response::StartRejected(StartRejection::RegistrationRefused);
                }
                let registered = match self.store.register_flow(node) {
                    Ok(store::FlowRegistration::Registered(node)) => node,
                    Ok(store::FlowRegistration::ConflictingBinding) | Err(_) => {
                        return Response::StartRejected(StartRejection::RegistrationRefused);
                    }
                };
                let acknowledgement = RegistrationAcknowledgement {
                    launch_request_id: binding.launch_request_id.clone(),
                    flow_id: registered.flow_id.clone(),
                    native_session_id: registered.session_id.clone(),
                    herdr_pane_binding: binding.herdr_pane_binding.clone(),
                };
                if !self
                    .store
                    .record_registration_acknowledgement(acknowledgement.clone())
                    .unwrap_or(false)
                {
                    return Response::StartRejected(StartRejection::LaunchPersistenceRefused);
                }
                let native_skill_selection_vector = match launch.launch_profile.harness_kind {
                    HarnessKind::Codex => self
                        .codex_endpoints
                        .adapter_for(&launch.launch_profile.model_name)
                        .and_then(|adapter| adapter.resolve_bound_codex_skills(&launch, &binding))
                        .map_err(|_| ()),
                    HarnessKind::Claude => self
                        .herdr
                        .resolve_claude_native_skills(&launch, &binding)
                        .map_err(|_| ()),
                };
                let Ok(native_skill_selection_vector) = native_skill_selection_vector else {
                    return Response::StartRejected(StartRejection::RegistrationRefused);
                };
                let delivery_intent = match self.herdr.accept_registration(
                    &launch,
                    &binding,
                    &acknowledgement,
                    native_skill_selection_vector,
                ) {
                    Ok(intent) => intent,
                    Err(_) => {
                        return Response::StartRejected(StartRejection::RegistrationRefused);
                    }
                };
                if !self
                    .store
                    .record_prompt_delivery_intent(delivery_intent.clone())
                    .unwrap_or(false)
                {
                    return Response::StartRejected(StartRejection::IntentPersistenceRefused);
                }
                let submission = match launch.launch_profile.harness_kind {
                    HarnessKind::Codex => codex_adapter.as_ref().ok_or(()).and_then(|adapter| {
                        adapter
                            .submit_bound_codex_first_turn(&launch, &delivery_intent)
                            .map_err(|_| ())
                    }),
                    HarnessKind::Claude => self
                        .herdr
                        .submit_first_prompt_once(&launch, &delivery_intent)
                        .map_err(|_| ()),
                };
                let initial = match submission {
                    Ok(result) => result,
                    Err(_) => PromptDeliveryResult::Ambiguous(delivery_intent.clone()),
                };
                if !self
                    .store
                    .record_prompt_delivery_result(initial.clone())
                    .unwrap_or(false)
                {
                    return Response::StartRejected(StartRejection::LaunchPersistenceRefused);
                }
                let result = match initial {
                    PromptDeliveryResult::Observed(receipt) => {
                        PromptDeliveryResult::Observed(receipt)
                    }
                    PromptDeliveryResult::Ambiguous(_) => self
                        .herdr
                        .observe_native_target_receipt(&delivery_intent)
                        .unwrap_or_else(|_| {
                            PromptDeliveryResult::Ambiguous(delivery_intent.clone())
                        }),
                };
                match result {
                    PromptDeliveryResult::Ambiguous(intent) => {
                        if intent != delivery_intent
                            && !self
                                .store
                                .record_prompt_delivery_result(PromptDeliveryResult::Ambiguous(
                                    intent.clone(),
                                ))
                                .unwrap_or(false)
                        {
                            return Response::StartRejected(
                                StartRejection::LaunchPersistenceRefused,
                            );
                        }
                        Response::StartAmbiguous(intent)
                    }
                    PromptDeliveryResult::Observed(receipt) => {
                        if !self
                            .store
                            .record_prompt_delivery_result(PromptDeliveryResult::Observed(receipt))
                            .unwrap_or(false)
                        {
                            return Response::StartRejected(
                                StartRejection::LaunchPersistenceRefused,
                            );
                        }
                        self.store.confirm_started(&binding.flow_id).unwrap_or(
                            Response::StartRejected(StartRejection::LaunchPersistenceRefused),
                        )
                    }
                }
            }
            Query::Restart(request) => {
                let authorization = self
                    .store
                    .authorize_restart(&request.flow_id, &request.origin_clue.flow_id);
                let Ok(Some(token)) = authorization else {
                    return Response::RestartRejected(RestartRejection::ProvenanceMismatch);
                };
                if request.origin_clue.session_id != token.thread_id {
                    return Response::RestartRejected(RestartRejection::ProvenanceMismatch);
                }
                // v2 restart does not carry the original model/endpoint. Moving
                // a thread between stable and next is never inferred from its
                // current process or from a caller claim. Refresh uses a fresh
                // typed Start until the replacement contract is deployed.
                let _ = token;
                Response::RestartRejected(RestartRejection::ResumeRefused)
            }
            Query::ResolveRecipient(flow_id) => {
                match self.store.apply(Query::ResolveRecipient(flow_id)) {
                    Ok(Response::RecipientResolved(node)) => Response::RecipientResolved(
                        self.herdr.refresh_route(claude::refresh_readiness(node)),
                    ),
                    Ok(response) => response,
                    Err(_) => Response::RecipientResolutionRejected(
                        signal_flow::RecipientResolutionRejection::FlowUnavailable,
                    ),
                }
            }
        }
    }

    fn dispatch_meta(&self, query: meta_signal_flow::Query) -> meta_signal_flow::Response {
        match query {
            meta_signal_flow::Query::Configure(configuration) => {
                if self.store.configure(configuration.clone()).is_err() {
                    return meta_signal_flow::Response::ConfigureRejected(
                        meta_signal_flow::ConfigureRejection::StoreRefused,
                    );
                }
                meta_signal_flow::Response::Configured(meta_signal_flow::Configured {
                    configuration,
                    activation: meta_signal_flow::Activation::NexusRestartRequired,
                })
            }
            meta_signal_flow::Query::ConsumeReset(request) => self
                .codex_endpoints
                .next
                .model_names
                .first()
                .ok_or(())
                .and_then(|model| self.codex_endpoints.adapter_for(model).map_err(|_| ()))
                .and_then(|adapter| adapter.consume_reset_credit(&request).map_err(|_| ()))
                .map(meta_signal_flow::Response::ResetConsumed)
                .unwrap_or(meta_signal_flow::Response::ResetRejected(
                    meta_signal_flow::ResetRejection::AdapterUnavailable,
                )),
            meta_signal_flow::Query::MetaBindExisting(request) => {
                let container = request.flow_container;
                if !container_is_well_formed(&container) || !container_socket_is_live(&container) {
                    return meta_signal_flow::Response::BindExistingRejected(
                        meta_signal_flow::BindExistingRejection::ContainerUnavailable,
                    );
                }
                if !process_identity_matches(&container.herdr_server_process_identity) {
                    return meta_signal_flow::Response::BindExistingRejected(
                        meta_signal_flow::BindExistingRejection::ContainerIdentityMismatch,
                    );
                }

                let mut seen_flow_ids = HashSet::new();
                let mut seen_panes = HashSet::new();
                let mut results = Vec::with_capacity(request.flow_binding_vector.len());
                for binding in request.flow_binding_vector {
                    let flow_id = binding.flow_id.clone();
                    if !seen_flow_ids.insert(flow_id.clone()) {
                        results.push(refused_binding(
                            flow_id,
                            meta_signal_flow::FlowBindingRefusalReason::DuplicateFlowId,
                        ));
                        continue;
                    }
                    if !binding_is_well_formed(&binding) {
                        results.push(refused_binding(
                            flow_id,
                            meta_signal_flow::FlowBindingRefusalReason::AnatomyMismatch,
                        ));
                        continue;
                    }
                    let pane_identity = (
                        binding.herdr_workspace_id.clone(),
                        binding.herdr_pane_id.clone(),
                        binding.herdr_tab_id.clone(),
                        binding.herdr_terminal_id.clone(),
                        binding.herdr_agent_name.clone(),
                    );
                    if !seen_panes.insert(pane_identity) {
                        results.push(refused_binding(
                            flow_id,
                            meta_signal_flow::FlowBindingRefusalReason::AmbiguousPane,
                        ));
                        continue;
                    }
                    if !process_identity_matches(&binding.process_identity) {
                        results.push(refused_binding(
                            flow_id,
                            meta_signal_flow::FlowBindingRefusalReason::DeadProcess,
                        ));
                        continue;
                    }
                    if !process_cwd_matches(&binding.process_identity, &binding.working_directory) {
                        results.push(refused_binding(
                            flow_id,
                            meta_signal_flow::FlowBindingRefusalReason::AnatomyMismatch,
                        ));
                        continue;
                    }
                    match self.store.apply(Query::ResolveRecipient(flow_id.clone())) {
                        Ok(Response::RecipientResolutionRejected(
                            signal_flow::RecipientResolutionRejection::UnknownFlow,
                        )) => {}
                        Ok(Response::RecipientResolved(_)) => {
                            results.push(refused_binding(
                                flow_id,
                                meta_signal_flow::FlowBindingRefusalReason::DuplicateFlowId,
                            ));
                            continue;
                        }
                        Ok(_) | Err(_) => {
                            return meta_signal_flow::Response::BindExistingRejected(
                                meta_signal_flow::BindExistingRejection::StoreRefused,
                            );
                        }
                    }

                    let node = FlowNode {
                        flow_id: flow_id.clone(),
                        session_id: binding.native_session_id,
                        harness_kind: binding.harness_kind,
                        endpoint_selection: EndpointSelection::Unavailable,
                        herdr_route_selection: HerdrRouteSelection::Available(HerdrRoute {
                            herdr_session_name: container.herdr_session_name.clone(),
                            herdr_agent_name: binding.herdr_agent_name,
                            herdr_pane_id: binding.herdr_pane_id,
                            herdr_terminal_id: binding.herdr_terminal_id,
                        }),
                        origin_clue: signal_flow::OriginClue {
                            flow_id: container.meta_flow_owner_id.clone(),
                            session_id: container.herdr_session_name.clone(),
                            turn_id: "meta-bind-existing".into(),
                        },
                        flow_lifecycle: FlowLifecycle::Pending,
                    };
                    let flow_type = format!(
                        "{:?}:{:?}:{}",
                        binding.flow_aspect, binding.power_level, binding.model_name
                    );
                    match self.store.register_existing_flow(node, flow_type) {
                        Ok(store::FlowRegistration::Registered(_)) => {
                            results.push(meta_signal_flow::FlowBindingResult::Bound(
                                meta_signal_flow::BoundFlowBinding {
                                    flow_id,
                                    flow_lifecycle:
                                        meta_signal_flow::FlowLifecycle::RegisteredUnconfirmed,
                                },
                            ))
                        }
                        Ok(store::FlowRegistration::ConflictingBinding) => {
                            results.push(refused_binding(
                                flow_id,
                                meta_signal_flow::FlowBindingRefusalReason::DuplicateFlowId,
                            ))
                        }
                        Err(_) => {
                            return meta_signal_flow::Response::BindExistingRejected(
                                meta_signal_flow::BindExistingRejection::StoreRefused,
                            );
                        }
                    }
                }

                meta_signal_flow::Response::BoundExisting(meta_signal_flow::BoundExisting {
                    flow_container: container,
                    flow_binding_result_vector: results,
                })
            }
            meta_signal_flow::Query::MetaConfirmExisting(request) => {
                let Some(route) = self.store.existing_confirmation_binding(&request).ok().flatten() else {
                    return meta_signal_flow::Response::ConfirmExistingRejected(meta_signal_flow::ConfirmExistingRejection::BindingNotRegisteredUnconfirmed);
                };
                let Ok(evidence) = self.herdr.observe_existing_confirmation(&request, &route) else {
                    return meta_signal_flow::Response::ConfirmExistingRejected(meta_signal_flow::ConfirmExistingRejection::FirstStartReceiptUnavailable);
                };
                self.store.confirm_existing(request, evidence).unwrap_or(meta_signal_flow::Response::ConfirmExistingRejected(
                    meta_signal_flow::ConfirmExistingRejection::StoreRefused,
                ))
            }
            meta_signal_flow::Query::RegisterFlow(flow_node) => {
                if !self.herdr.validate_registration(&flow_node) {
                    return meta_signal_flow::Response::FlowRegistrationRejected(
                        meta_signal_flow::FlowRegistrationRejection::UnknownOrUnclaimedIdentity,
                    );
                }
                match self.store.register_flow(flow_node) {
                    Ok(store::FlowRegistration::Registered(node)) => {
                        meta_signal_flow::Response::FlowRegistered(*node)
                    }
                    Ok(store::FlowRegistration::ConflictingBinding) => {
                        meta_signal_flow::Response::FlowRegistrationRejected(
                            meta_signal_flow::FlowRegistrationRejection::ConflictingBinding,
                        )
                    }
                    Err(_) => meta_signal_flow::Response::FlowRegistrationRejected(
                        meta_signal_flow::FlowRegistrationRejection::StoreRefused,
                    ),
                }
            }
        }
    }
}

pub trait OpensRunningNexus {
    fn open(
        store: &Path,
        codex_endpoints: CodexEndpoints,
        source_root: PathBuf,
    ) -> Result<Self, store::StoreError>
    where
        Self: Sized;
}

impl OpensRunningNexus for RunningNexus {
    fn open(
        store: &Path,
        codex_endpoints: CodexEndpoints,
        source_root: PathBuf,
    ) -> Result<Self, store::StoreError> {
        Ok(Self {
            store: FlowStore::open(store)?,
            codex_endpoints: codex_endpoints.clone(),
            herdr: herdr::HerdrCli::default().with_codex_endpoints(codex_endpoints),
            composer: LaunchComposer::at(source_root),
        })
    }
}

pub trait ServesOrdinary {
    fn serve_ordinary(&self, socket: &Path) -> Result<(), String>;
}

impl ServesOrdinary for RunningNexus {
    fn serve_ordinary(&self, socket: &Path) -> Result<(), String> {
        let _ = fs::remove_file(socket);
        let listener = UnixListener::bind(socket).map_err(|error| error.to_string())?;
        fs::set_permissions(socket, fs::Permissions::from_mode(0o600))
            .map_err(|e| e.to_string())?;
        loop {
            let (mut peer, _) = listener.accept().map_err(|error| error.to_string())?;
            let query = Frame::read_query(&mut peer)?;
            let response = self.dispatch(query);
            Frame::write_response(&mut peer, &response)?;
        }
    }
}

pub trait ServesMeta {
    fn serve_meta(&self, socket: &Path) -> Result<(), String>;
}

impl ServesMeta for RunningNexus {
    fn serve_meta(&self, socket: &Path) -> Result<(), String> {
        let _ = fs::remove_file(socket);
        let listener = UnixListener::bind(socket).map_err(|error| error.to_string())?;
        fs::set_permissions(socket, fs::Permissions::from_mode(0o600))
            .map_err(|e| e.to_string())?;
        loop {
            let (mut peer, _) = listener.accept().map_err(|error| error.to_string())?;
            let reply = self.dispatch_meta(Frame::read_meta_query(&mut peer)?);
            Frame::write_meta_response(&mut peer, &reply)?;
        }
    }
}

pub struct Frame;

impl Frame {
    fn write_bytes(peer: &mut UnixStream, bytes: &[u8]) -> Result<(), String> {
        peer.write_all(&(bytes.len() as u32).to_be_bytes())
            .map_err(|e| e.to_string())?;
        peer.write_all(bytes).map_err(|e| e.to_string())
    }
    fn read_bytes(peer: &mut UnixStream) -> Result<Vec<u8>, String> {
        let mut length = [0; 4];
        peer.read_exact(&mut length).map_err(|e| e.to_string())?;
        let length = u32::from_be_bytes(length) as usize;
        if length > 1024 * 1024 {
            return Err("Signal frame exceeds 1 MiB".into());
        }
        let mut bytes = vec![0; length];
        peer.read_exact(&mut bytes).map_err(|e| e.to_string())?;
        Ok(bytes)
    }
    pub fn write_query(peer: &mut UnixStream, value: &Query) -> Result<(), String> {
        Self::write_bytes(
            peer,
            &rkyv::to_bytes::<rkyv::rancor::Error>(value).map_err(|e| e.to_string())?,
        )
    }
    pub fn read_query(peer: &mut UnixStream) -> Result<Query, String> {
        rkyv::from_bytes::<Query, rkyv::rancor::Error>(&Self::read_bytes(peer)?)
            .map_err(|e| e.to_string())
    }
    pub fn write_response(peer: &mut UnixStream, value: &Response) -> Result<(), String> {
        Self::write_bytes(
            peer,
            &rkyv::to_bytes::<rkyv::rancor::Error>(value).map_err(|e| e.to_string())?,
        )
    }
    pub fn read_response(peer: &mut UnixStream) -> Result<Response, String> {
        rkyv::from_bytes::<Response, rkyv::rancor::Error>(&Self::read_bytes(peer)?)
            .map_err(|e| e.to_string())
    }
    pub fn read_meta_query(peer: &mut UnixStream) -> Result<meta_signal_flow::Query, String> {
        rkyv::from_bytes::<meta_signal_flow::Query, rkyv::rancor::Error>(&Self::read_bytes(peer)?)
            .map_err(|e| e.to_string())
    }
    pub fn write_meta_response(
        peer: &mut UnixStream,
        value: &meta_signal_flow::Response,
    ) -> Result<(), String> {
        Self::write_bytes(
            peer,
            &rkyv::to_bytes::<rkyv::rancor::Error>(value).map_err(|e| e.to_string())?,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::{Dispatches, RunningNexus};
    use crate::{
        codex::{CodexEndpoint, CodexEndpoints},
        composition::{ComposesLaunch, LaunchComposer, OpensLaunchComposer},
        herdr::HerdrCli,
        store::{
            AppliesFlowQuery, FlowStore, OpensFlowStore, RecordsNativeLaunchBinding,
            RecordsNativeLaunchIntent, RecordsPromptDeliveryIntent, RecordsPromptDeliveryResult,
            RecordsRegistrationAcknowledgement, RegistersFlowIdentity, ReservesLaunchAttempt,
        },
    };
    use sha2::{Digest, Sha256};
    use signal_flow::{
        EndpointSelection, FlowAspect, FlowLifecycle, FlowNode, HarnessKind, HerdrPaneBinding,
        HerdrRoute, HerdrRouteSelection, LaunchProfile, LaunchSource, NativeLaunchBinding,
        NativeLaunchIntent, NativeTranscriptAbsence, NativeTranscriptBoundary, OriginClue,
        PowerLevel, PromptDeliveryIntent, PromptDeliveryResult, Query, RegistrationAcknowledgement,
        Response, StartRejection, StartRequest,
    };
    use std::{
        collections::BTreeSet,
        fs,
        io::Write,
        os::unix::fs::{MetadataExt, PermissionsExt},
        os::unix::net::UnixListener,
        path::{Path, PathBuf},
        time::Duration,
    };

    struct NexusFixture {
        directory: tempfile::TempDir,
        nexus: RunningNexus,
        snapshot_program: PathBuf,
    }

    trait ControlsHerdrSnapshot {
        fn set_agents(&self, agents: Vec<serde_json::Value>);
    }

    fn current_process_identity() -> meta_signal_flow::ProcessIdentity {
        let process_id = i64::from(std::process::id());
        let metadata = fs::metadata(format!("/proc/{process_id}")).expect("current process");
        let stat = fs::read_to_string(format!("/proc/{process_id}/stat")).expect("process stat");
        let (_, fields) = stat.rsplit_once(") ").expect("process comm boundary");
        meta_signal_flow::ProcessIdentity {
            process_id,
            process_user_id: i64::from(metadata.uid()),
            process_start_token: fields
                .split_whitespace()
                .nth(19)
                .expect("process start token")
                .into(),
        }
    }

    fn existing_binding(flow_id: &str, pane: &str) -> meta_signal_flow::FlowBinding {
        meta_signal_flow::FlowBinding {
            flow_id: flow_id.into(),
            flow_aspect: FlowAspect::Mind,
            power_level: PowerLevel::Medium,
            model_name: "gpt-sol".into(),
            harness_kind: HarnessKind::Codex,
            native_session_id: format!("native-{flow_id}"),
            herdr_workspace_id: "workspace".into(),
            herdr_pane_id: pane.into(),
            herdr_tab_id: "tab".into(),
            herdr_terminal_id: format!("terminal-{pane}"),
            herdr_agent_name: format!("agent-{pane}"),
            process_identity: current_process_identity(),
            working_directory: fs::read_link("/proc/self/cwd")
                .expect("current cwd")
                .to_string_lossy()
                .into_owned(),
        }
    }

    fn flow_container(socket: &Path) -> meta_signal_flow::FlowContainer {
        meta_signal_flow::FlowContainer {
            herdr_session_name: "messaging-build".into(),
            herdr_server_socket_path: socket.to_string_lossy().into_owned(),
            herdr_server_process_identity: current_process_identity(),
            meta_flow_owner_id: "field-owner".into(),
        }
    }

    impl NexusFixture {
        fn new() -> Self {
            let directory = tempfile::tempdir().expect("temporary nexus fixture");
            let flows_root = directory.path().join("flows");
            fs::create_dir(&flows_root).expect("fixture flows root");
            fs::write(
                flows_root.join(".908786.flow-id"),
                "version=1\nharness=codex\nidentity=01a0b22ce24f7452994064490878680f\nalias=908786\n",
            )
            .expect("fixture flow claim");
            let snapshot_program = directory.path().join("herdr-fixture");
            let codex_endpoints = CodexEndpoints {
                stable: CodexEndpoint {
                    client_path: PathBuf::from("/fixture/codex"),
                    home: directory.path().join("codex-home"),
                    socket: "unused".into(),
                    transcript_root: directory.path().join("native-transcripts/codex"),
                    model_names: BTreeSet::from([
                        "unused".into(),
                        "fixture-model".into(),
                        "gpt-sol".into(),
                    ]),
                },
                next: CodexEndpoint {
                    client_path: PathBuf::from("/fixture/codex-next"),
                    home: directory.path().join("codex-next-home"),
                    socket: "unused-next".into(),
                    transcript_root: directory.path().join("native-transcripts/codex-next"),
                    model_names: BTreeSet::from(["gpt-6-sol".into(), "gpt-6-luna".into()]),
                },
                timeout: Duration::from_secs(1),
                workspace_root: directory.path().to_path_buf(),
            };
            let nexus = RunningNexus {
                store: FlowStore::open(&directory.path().join("flow.sema")).expect("fixture store"),
                codex_endpoints: codex_endpoints.clone(),
                herdr: HerdrCli::at(snapshot_program.clone(), flows_root)
                    .with_codex_endpoints(codex_endpoints),
                composer: LaunchComposer::at(directory.path().to_path_buf()),
            };
            Self {
                directory,
                nexus,
                snapshot_program,
            }
        }

        fn node(&self) -> FlowNode {
            FlowNode {
                flow_id: "908786".into(),
                session_id: "01a0b22c-e24f-7452-9940-64490878680f".into(),
                harness_kind: HarnessKind::Codex,
                endpoint_selection: EndpointSelection::Available(signal_flow::Available_Data {
                    endpoint_path: "/tmp/native-fallback.sock".into(),
                    route_readiness: signal_flow::RouteReadiness::Ready,
                }),
                herdr_route_selection: HerdrRouteSelection::Available(HerdrRoute {
                    herdr_session_name: "messaging-build".into(),
                    herdr_agent_name: "psyche-mind-astra".into(),
                    herdr_pane_id: "w1:p3".into(),
                    herdr_terminal_id: "term_65bb7f87270cb3".into(),
                }),
                origin_clue: OriginClue {
                    flow_id: "908786".into(),
                    session_id: "01a0b22c-e24f-7452-9940-64490878680f".into(),
                    turn_id: "unavailable".into(),
                },
                flow_lifecycle: FlowLifecycle::Active,
            }
        }

        fn current_agent(&self) -> serde_json::Value {
            serde_json::json!({
                "agent":"codex",
                "agent_status":"working",
                "cwd":"/home/li/primary",
                "focused":false,
                "foreground_cwd":"/home/li/primary",
                "interactive_ready":true,
                "name":"psyche-mind-astra",
                "pane_id":"w1:p3",
                "revision":4,
                "state_change_seq":85,
                "tab_id":"w1:t1",
                "terminal_id":"term_65bb7f87270cb3",
                "terminal_title":"primary",
                "terminal_title_stripped":"primary",
                "workspace_id":"w1"
            })
        }
    }

    impl ControlsHerdrSnapshot for NexusFixture {
        fn set_agents(&self, agents: Vec<serde_json::Value>) {
            let snapshot = serde_json::json!({
                "id":"cli:api:snapshot",
                "result":{
                    "snapshot":{
                        "agents":agents,
                        "protocol":20,
                        "version":"0.8.2"
                    },
                    "type":"session_snapshot"
                }
            });
            let body = format!(
                "#!/bin/sh\n[ \"$1\" = \"--session\" ] && [ \"$3\" = \"api\" ] && [ \"$4\" = \"snapshot\" ] || exit 64\nprintf '%s\\n' '{}'\n",
                snapshot
            );
            fs::write(&self.snapshot_program, body).expect("snapshot subprocess fixture");
            let mut permissions = fs::metadata(&self.snapshot_program)
                .expect("snapshot fixture metadata")
                .permissions();
            permissions.set_mode(0o700);
            fs::set_permissions(&self.snapshot_program, permissions)
                .expect("snapshot fixture executable");
        }
    }

    #[test]
    fn running_nexus_parses_actual_working_interactive_snapshot() {
        let fixture = NexusFixture::new();
        fixture.set_agents(vec![fixture.current_agent()]);
        let node = fixture.node();
        assert_eq!(
            fixture
                .nexus
                .dispatch_meta(meta_signal_flow::Query::RegisterFlow(node.clone())),
            meta_signal_flow::Response::FlowRegistered(node.clone())
        );
        assert_eq!(
            fixture
                .nexus
                .dispatch(Query::ResolveRecipient("908786".into())),
            Response::RecipientResolved(node)
        );
    }

    #[test]
    fn meta_bind_existing_imports_only_verified_processes_as_pending() {
        let fixture = NexusFixture::new();
        let socket_path = fixture.directory.path().join("herdr.sock");
        let _listener = UnixListener::bind(&socket_path).expect("live Herdr fixture socket");
        let container = flow_container(&socket_path);
        let accepted = existing_binding("mind-live", "pane-1");
        let ambiguous = existing_binding("field-ambiguous", "pane-1");
        let mut dead = existing_binding("psyche-dead", "pane-3");
        dead.process_identity.process_id = i64::MAX;
        let duplicate = existing_binding("mind-live", "pane-4");
        let mut wrong_cwd = existing_binding("field-wrong-cwd", "pane-5");
        wrong_cwd.working_directory = fixture
            .directory
            .path()
            .join("absent")
            .display()
            .to_string();

        let response = fixture
            .nexus
            .dispatch_meta(meta_signal_flow::Query::MetaBindExisting(
                meta_signal_flow::MetaBindExisting {
                    flow_container: container.clone(),
                    flow_binding_vector: vec![accepted, ambiguous, dead, duplicate, wrong_cwd],
                },
            ));
        let meta_signal_flow::Response::BoundExisting(bound) = response else {
            panic!("valid container must return ordered per-flow results")
        };
        assert_eq!(bound.flow_container, container);
        assert_eq!(bound.flow_binding_result_vector.len(), 5);
        assert!(matches!(
            &bound.flow_binding_result_vector[0],
            meta_signal_flow::FlowBindingResult::Bound(binding)
                if binding.flow_id == "mind-live"
                    && binding.flow_lifecycle
                        == meta_signal_flow::FlowLifecycle::RegisteredUnconfirmed
        ));
        assert!(matches!(
            &bound.flow_binding_result_vector[1],
            meta_signal_flow::FlowBindingResult::Refused(binding)
                if binding.flow_binding_refusal_reason
                    == meta_signal_flow::FlowBindingRefusalReason::AmbiguousPane
        ));
        assert!(matches!(
            &bound.flow_binding_result_vector[2],
            meta_signal_flow::FlowBindingResult::Refused(binding)
                if binding.flow_binding_refusal_reason
                    == meta_signal_flow::FlowBindingRefusalReason::DeadProcess
        ));
        assert!(matches!(
            &bound.flow_binding_result_vector[3],
            meta_signal_flow::FlowBindingResult::Refused(binding)
                if binding.flow_binding_refusal_reason
                    == meta_signal_flow::FlowBindingRefusalReason::DuplicateFlowId
        ));
        assert!(matches!(
            &bound.flow_binding_result_vector[4],
            meta_signal_flow::FlowBindingResult::Refused(binding)
                if binding.flow_binding_refusal_reason
                    == meta_signal_flow::FlowBindingRefusalReason::AnatomyMismatch
        ));

        let Response::RecipientResolved(node) = fixture
            .nexus
            .dispatch(Query::ResolveRecipient("mind-live".into()))
        else {
            panic!("accepted existing flow must resolve")
        };
        assert_eq!(node.flow_lifecycle, FlowLifecycle::Pending);
        assert_eq!(node.endpoint_selection, EndpointSelection::Unavailable);
        assert_eq!(node.origin_clue.flow_id, "field-owner");
        assert!(matches!(
            fixture
                .nexus
                .dispatch(Query::ResolveRecipient("field-ambiguous".into())),
            Response::RecipientResolutionRejected(
                signal_flow::RecipientResolutionRejection::UnknownFlow
            )
        ));
    }

    #[test]
    fn meta_confirm_existing_promotes_only_the_exact_pending_binding() {
        let fixture = NexusFixture::new();
        let socket_path = fixture.directory.path().join("herdr.sock");
        let _listener = UnixListener::bind(&socket_path).expect("live Herdr fixture socket");
        let binding = existing_binding("mind-confirm", "pane-confirm");
        assert!(matches!(
            fixture
                .nexus
                .dispatch_meta(meta_signal_flow::Query::MetaBindExisting(
                    meta_signal_flow::MetaBindExisting {
                        flow_container: flow_container(&socket_path),
                        flow_binding_vector: vec![binding.clone()],
                    },
                )),
            meta_signal_flow::Response::BoundExisting(_)
        ));
        let pending = fixture
            .nexus
            .dispatch(Query::ResolveRecipient("mind-confirm".into()));
        assert!(
            matches!(pending, Response::RecipientResolved(ref node) if node.flow_lifecycle == FlowLifecycle::Pending)
        );

        let confirmation = meta_signal_flow::MetaConfirmExisting {
            flow_id: binding.flow_id.clone(),
            native_session_id: binding.native_session_id.clone(),
            herdr_terminal_id: binding.herdr_terminal_id.clone(),
        };
        let mut wrong_terminal = confirmation.clone();
        wrong_terminal.herdr_terminal_id = "other-terminal".into();
        assert_eq!(
            fixture
                .nexus
                .dispatch_meta(meta_signal_flow::Query::MetaConfirmExisting(wrong_terminal)),
            meta_signal_flow::Response::ConfirmExistingRejected(
                meta_signal_flow::ConfirmExistingRejection::FirstStartReceiptUnavailable
            )
        );
        let wrong_receipt = confirmation.clone();
        assert_eq!(
            fixture
                .nexus
                .dispatch_meta(meta_signal_flow::Query::MetaConfirmExisting(wrong_receipt)),
            meta_signal_flow::Response::ConfirmExistingRejected(
                meta_signal_flow::ConfirmExistingRejection::FirstStartReceiptUnavailable
            )
        );
        assert!(matches!(
            fixture.nexus.dispatch(Query::ResolveRecipient("mind-confirm".into())),
            Response::RecipientResolved(ref node) if node.flow_lifecycle == FlowLifecycle::Pending
        ));
        fixture.set_agents(vec![serde_json::json!({"agent":"codex", "name":binding.herdr_agent_name, "pane_id":binding.herdr_pane_id, "terminal_id":binding.herdr_terminal_id, "terminal_title":"native title"})]);
        let transcript_root = fixture.directory.path().join("native-transcripts/codex");
        fs::create_dir_all(&transcript_root).unwrap();
        let transcript_path = transcript_root.join(format!("{}.jsonl", binding.native_session_id));
        fs::write(&transcript_path, format!(
            "{{\"type\":\"turn_context\",\"payload\":{{\"model\":\"gpt-sol\",\"effort\":\"medium\",\"turn_id\":\"turn-confirm\"}}}}\n{{\"type\":\"event_msg\",\"payload\":{{\"thread_id\":\"{}\",\"turn_id\":\"turn-confirm\",\"item\":{{\"type\":\"UserMessage\",\"content\":[{{\"type\":\"skill\",\"name\":\"fabricated\",\"path\":\"/tmp/fabricated\"}},{{\"type\":\"text\",\"text\":\"native prompt\"}}]}}}}}}\n", binding.native_session_id)).unwrap();
        assert!(matches!(fixture.nexus.dispatch_meta(meta_signal_flow::Query::MetaConfirmExisting(confirmation.clone())), meta_signal_flow::Response::ConfirmExistingRejected(_)));
        assert!(matches!(fixture.nexus.dispatch(Query::ResolveRecipient("mind-confirm".into())), Response::RecipientResolved(ref node) if node.flow_lifecycle == FlowLifecycle::Pending));
        fs::write(transcript_path, format!(
            "{{\"type\":\"turn_context\",\"payload\":{{\"model\":\"gpt-sol\",\"effort\":\"medium\",\"turn_id\":\"turn-confirm\"}}}}\n{{\"type\":\"event_msg\",\"payload\":{{\"thread_id\":\"{}\",\"turn_id\":\"turn-confirm\",\"item\":{{\"type\":\"UserMessage\",\"content\":[{{\"type\":\"text\",\"text\":\"native prompt\"}}]}}}}}}\n", binding.native_session_id)).unwrap();
        assert!(matches!(
            fixture.nexus.dispatch_meta(meta_signal_flow::Query::MetaConfirmExisting(confirmation.clone())),
            meta_signal_flow::Response::ConfirmedExisting(ref confirmed)
                if confirmed.flow_id == binding.flow_id && confirmed.native_session_id == binding.native_session_id
        ));
        assert!(matches!(
            fixture.nexus.dispatch(Query::ResolveRecipient("mind-confirm".into())),
            Response::RecipientResolved(ref node) if node.flow_lifecycle == FlowLifecycle::Active
        ));
        let mut mismatch = confirmation;
        mismatch.native_session_id = "wrong-session".into();
        assert_eq!(
            fixture
                .nexus
                .dispatch_meta(meta_signal_flow::Query::MetaConfirmExisting(mismatch)),
            meta_signal_flow::Response::ConfirmExistingRejected(
                meta_signal_flow::ConfirmExistingRejection::BindingNotRegisteredUnconfirmed
            )
        );
        let store_path = fixture.directory.path().join("flow.sema");
        drop(fixture.nexus);
        let reopened = FlowStore::open(&store_path).expect("reopen confirmed store");
        assert!(matches!(
            reopened.apply(Query::ResolveRecipient("mind-confirm".into())),
            Ok(Response::RecipientResolved(ref node)) if node.flow_lifecycle == FlowLifecycle::Active
        ));
    }

    #[test]
    fn meta_bind_existing_rejects_unverified_container_without_store_effects() {
        let fixture = NexusFixture::new();
        let socket_path = fixture.directory.path().join("herdr.sock");
        let _listener = UnixListener::bind(&socket_path).expect("live Herdr fixture socket");
        let mut container = flow_container(&socket_path);
        container.herdr_server_process_identity.process_start_token = "stale".into();

        assert_eq!(
            fixture
                .nexus
                .dispatch_meta(meta_signal_flow::Query::MetaBindExisting(
                    meta_signal_flow::MetaBindExisting {
                        flow_container: container,
                        flow_binding_vector: vec![existing_binding("not-imported", "pane-1")],
                    }
                ),),
            meta_signal_flow::Response::BindExistingRejected(
                meta_signal_flow::BindExistingRejection::ContainerIdentityMismatch
            )
        );
        assert!(matches!(
            fixture
                .nexus
                .dispatch(Query::ResolveRecipient("not-imported".into())),
            Response::RecipientResolutionRejected(
                signal_flow::RecipientResolutionRejection::UnknownFlow
            )
        ));
    }

    #[test]
    fn identical_launch_retry_reads_the_journal_before_a_deleted_source() {
        let fixture = NexusFixture::new();
        let source_path = fixture.directory.path().join("launch-source.md");
        fs::write(&source_path, b"exact source bytes\n").expect("fixture source");
        let profile = LaunchProfile {
            launch_request_id: "retry-request".into(),
            launch_source_vector: vec![LaunchSource {
                source_path: "launch-source.md".into(),
                source_sha256: format!("{:x}", Sha256::digest(b"exact source bytes\n")),
            }],
            skill_name_vector: Vec::new(),
            flow_aspect: FlowAspect::Field,
            power_level: PowerLevel::High,
            harness_kind: HarnessKind::Codex,
            model_name: "fixture-model".into(),
            effort: "medium".into(),
            flow_id_option: None,
            remembered_flow_vector: Vec::new(),
            herdr_session_name: "fixture-session".into(),
            instruction_prompt: "fixture instruction".into(),
        };
        let origin = OriginClue {
            flow_id: "caller".into(),
            session_id: "caller-session".into(),
            turn_id: "caller-turn".into(),
        };
        let composed = fixture
            .nexus
            .composer
            .compose(&profile)
            .expect("new request composes once");
        fixture
            .nexus
            .store
            .reserve_launch_attempt(&composed, origin.clone())
            .expect("reservation persists");
        fs::remove_file(source_path).expect("source removed after reservation");

        assert!(matches!(
            fixture.nexus.dispatch(Query::Start(StartRequest {
                launch_profile: profile.clone(),
                origin_clue: origin.clone(),
            })),
            Response::LaunchPending(_)
        ));
        let mut changed = profile;
        changed.effort = "high".into();
        assert_eq!(
            fixture.nexus.dispatch(Query::Start(StartRequest {
                launch_profile: changed,
                origin_clue: origin,
            })),
            Response::StartRejected(StartRejection::LaunchRequestConflict)
        );
        assert!(!fixture.snapshot_program.exists());
    }

    #[test]
    fn delayed_receipt_after_source_deletion_promotes_without_a_second_external_write() {
        let fixture = NexusFixture::new();
        let source_path = fixture.directory.path().join("delayed-source.md");
        fs::write(&source_path, b"delayed exact bytes\n").expect("fixture source");
        let profile = LaunchProfile {
            launch_request_id: "delayed-request".into(),
            launch_source_vector: vec![LaunchSource {
                source_path: "delayed-source.md".into(),
                source_sha256: format!("{:x}", Sha256::digest(b"delayed exact bytes\n")),
            }],
            skill_name_vector: Vec::new(),
            flow_aspect: FlowAspect::Field,
            power_level: PowerLevel::High,
            harness_kind: HarnessKind::Codex,
            model_name: "fixture-model".into(),
            effort: "medium".into(),
            flow_id_option: None,
            remembered_flow_vector: Vec::new(),
            herdr_session_name: "fixture-session".into(),
            instruction_prompt: "fixture instruction".into(),
        };
        let origin = OriginClue {
            flow_id: "caller".into(),
            session_id: "caller-session".into(),
            turn_id: "caller-turn".into(),
        };
        let composed = fixture.nexus.composer.compose(&profile).unwrap();
        fixture
            .nexus
            .store
            .reserve_launch_attempt(&composed, origin.clone())
            .unwrap();
        fixture
            .nexus
            .store
            .record_native_launch_intent(NativeLaunchIntent {
                launch_request_id: profile.launch_request_id.clone(),
                prompt_sha256: composed.first_prompt_payload.prompt_sha256.clone(),
                harness_kind: HarnessKind::Codex,
                model_name: profile.model_name.clone(),
                effort: profile.effort.clone(),
                skill_name_vector: Vec::new(),
            })
            .unwrap();
        let native_session_id = "01a0b22c-e24f-7452-9940-64490878680f";
        let pane = HerdrPaneBinding {
            launch_request_id: profile.launch_request_id.clone(),
            herdr_session_name: profile.herdr_session_name.clone(),
            herdr_agent_name: "fixture-agent".into(),
            herdr_workspace_id: "fixture-workspace".into(),
            herdr_pane_id: "w1:p1".into(),
            herdr_terminal_id: "fixture-terminal".into(),
        };
        let binding = NativeLaunchBinding {
            launch_request_id: profile.launch_request_id.clone(),
            flow_id: "908786".into(),
            native_session_id: native_session_id.into(),
            harness_kind: HarnessKind::Codex,
            herdr_pane_binding: pane.clone(),
        };
        fixture
            .nexus
            .store
            .record_native_launch_binding(binding.clone())
            .unwrap();
        fixture
            .nexus
            .store
            .register_flow(FlowNode {
                flow_id: binding.flow_id.clone(),
                session_id: binding.native_session_id.clone(),
                harness_kind: HarnessKind::Codex,
                endpoint_selection: EndpointSelection::Unavailable,
                herdr_route_selection: HerdrRouteSelection::Available(HerdrRoute {
                    herdr_session_name: pane.herdr_session_name.clone(),
                    herdr_agent_name: pane.herdr_agent_name.clone(),
                    herdr_pane_id: pane.herdr_pane_id.clone(),
                    herdr_terminal_id: pane.herdr_terminal_id.clone(),
                }),
                origin_clue: origin.clone(),
                flow_lifecycle: FlowLifecycle::Pending,
            })
            .unwrap();
        let acknowledgement = RegistrationAcknowledgement {
            launch_request_id: profile.launch_request_id.clone(),
            flow_id: binding.flow_id.clone(),
            native_session_id: binding.native_session_id.clone(),
            herdr_pane_binding: pane.clone(),
        };
        fixture
            .nexus
            .store
            .record_registration_acknowledgement(acknowledgement)
            .unwrap();
        let transcript_root = fixture.directory.path().join("native-transcripts/codex");
        fs::create_dir_all(&transcript_root).unwrap();
        let root_metadata = fs::metadata(&transcript_root).unwrap();
        let intent = PromptDeliveryIntent {
            launch_request_id: profile.launch_request_id.clone(),
            prompt_sha256: composed.first_prompt_payload.prompt_sha256.clone(),
            flow_id: binding.flow_id.clone(),
            native_session_id: binding.native_session_id.clone(),
            harness_kind: HarnessKind::Codex,
            model_name: profile.model_name.clone(),
            effort: profile.effort.clone(),
            native_skill_selection_vector: Vec::new(),
            herdr_pane_binding: pane,
            native_transcript_boundary: NativeTranscriptBoundary::Absent(NativeTranscriptAbsence {
                native_session_id: binding.native_session_id.clone(),
                harness_kind: HarnessKind::Codex,
                transcript_root_device: root_metadata.dev().to_string(),
                transcript_root_inode: root_metadata.ino().to_string(),
            }),
        };
        fixture
            .nexus
            .store
            .record_prompt_delivery_intent(intent.clone())
            .unwrap();
        fixture
            .nexus
            .store
            .record_prompt_delivery_result(PromptDeliveryResult::Ambiguous(intent.clone()))
            .unwrap();
        fs::remove_file(source_path).unwrap();

        let herdr_calls = fixture.directory.path().join("delayed-herdr-calls");
        let agent = serde_json::json!({"result":{"agent":{
            "name":"fixture-agent",
            "agent":"codex",
            "workspace_id":"fixture-workspace",
            "pane_id":"w1:p1",
            "terminal_id":"fixture-terminal",
            "agent_session":{
                "source":"herdr:codex",
                "agent":"codex",
                "kind":"id",
                "value":native_session_id
            }
        }}});
        let body = format!(
            "#!/bin/sh\nprintf '%s\\n' \"$*\" >> '{}'\n[ \"$*\" = \"--session fixture-session agent get fixture-agent\" ] || exit 64\nprintf '%s\\n' '{}'\n",
            herdr_calls.display(),
            agent
        );
        fs::write(&fixture.snapshot_program, body).unwrap();
        let mut permissions = fs::metadata(&fixture.snapshot_program)
            .unwrap()
            .permissions();
        permissions.set_mode(0o700);
        fs::set_permissions(&fixture.snapshot_program, permissions).unwrap();

        let marker = format!(
            "FLOW_LAUNCH_RECEIPT_V1 launch_request_id={} prompt_body_sha256={}",
            intent.launch_request_id, intent.prompt_sha256
        );
        let transcript = transcript_root.join(format!("rollout-{native_session_id}.jsonl"));
        let mut output = fs::File::create(transcript).unwrap();
        for row in [
            serde_json::json!({"type":"turn_context","payload":{"model":"fixture-model","effort":"medium","turn_id":"turn-delayed"}}),
            serde_json::json!({"type":"event_msg","payload":{"thread_id":native_session_id,"turn_id":"turn-delayed","item":{"type":"UserMessage","content":[{"type":"text","text":composed.first_prompt_payload.first_prompt_text}]}}}),
            serde_json::json!({"type":"event_msg","payload":{"thread_id":native_session_id,"turn_id":"turn-delayed","item":{"type":"AgentMessage","content":[{"type":"Text","text":marker}]}}}),
        ] {
            writeln!(output, "{row}").unwrap();
        }
        output.sync_all().unwrap();

        assert!(matches!(
            fixture.nexus.dispatch(Query::Start(StartRequest {
                launch_profile: profile,
                origin_clue: origin,
            })),
            Response::Started(started)
                if started.flow_id == "908786" && started.session_id == native_session_id
        ));
        assert_eq!(
            fs::read_to_string(herdr_calls).unwrap(),
            "--session fixture-session agent get fixture-agent\n"
        );
    }

    #[test]
    fn running_nexus_marks_stale_or_noninteractive_snapshots_unavailable() {
        let fixture = NexusFixture::new();
        fixture.set_agents(vec![fixture.current_agent()]);
        let node = fixture.node();
        assert!(matches!(
            fixture
                .nexus
                .dispatch_meta(meta_signal_flow::Query::RegisterFlow(node.clone())),
            meta_signal_flow::Response::FlowRegistered(_)
        ));

        for changed_field in ["terminal", "name", "harness", "interactive"] {
            let mut agent = fixture.current_agent();
            match changed_field {
                "terminal" => agent["terminal_id"] = "term_replaced".into(),
                "name" => agent["name"] = "another-agent".into(),
                "harness" => agent["agent"] = "claude".into(),
                "interactive" => agent["interactive_ready"] = false.into(),
                _ => unreachable!("closed fixture variants"),
            }
            fixture.set_agents(vec![agent]);
            let Response::RecipientResolved(resolved) = fixture
                .nexus
                .dispatch(Query::ResolveRecipient("908786".into()))
            else {
                panic!("registered recipient resolves")
            };
            assert_eq!(
                resolved.herdr_route_selection,
                HerdrRouteSelection::Unavailable,
                "stale {changed_field} must not remain routable"
            );
            assert!(matches!(
                resolved.endpoint_selection,
                EndpointSelection::Available(signal_flow::Available_Data {
                    route_readiness: signal_flow::RouteReadiness::Parked,
                    ..
                })
            ));
        }
    }

    #[test]
    fn duplicate_registration_is_idempotent_and_conflict_has_no_partial_mutation() {
        let fixture = NexusFixture::new();
        let mut second_agent = fixture.current_agent();
        second_agent["terminal_id"] = "term_conflicting".into();
        fixture.set_agents(vec![fixture.current_agent(), second_agent]);
        let node = fixture.node();
        let registration = meta_signal_flow::Query::RegisterFlow(node.clone());
        assert!(matches!(
            fixture.nexus.dispatch_meta(registration.clone()),
            meta_signal_flow::Response::FlowRegistered(_)
        ));
        assert!(matches!(
            fixture.nexus.dispatch_meta(registration),
            meta_signal_flow::Response::FlowRegistered(_)
        ));

        let mut conflict = node.clone();
        let HerdrRouteSelection::Available(route) = &mut conflict.herdr_route_selection else {
            panic!("fixture route")
        };
        route.herdr_terminal_id = "term_conflicting".into();
        assert_eq!(
            fixture
                .nexus
                .dispatch_meta(meta_signal_flow::Query::RegisterFlow(conflict)),
            meta_signal_flow::Response::FlowRegistrationRejected(
                meta_signal_flow::FlowRegistrationRejection::ConflictingBinding
            )
        );

        fixture.set_agents(vec![fixture.current_agent()]);
        assert_eq!(
            fixture
                .nexus
                .dispatch(Query::ResolveRecipient("908786".into())),
            Response::RecipientResolved(node)
        );
        assert!(fixture.directory.path().join("flow.sema").exists());
    }
}
