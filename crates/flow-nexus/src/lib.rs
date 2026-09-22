//! Flow Nexus dispatches typed ordinary and privileged Signal requests.
pub mod claude;
pub mod codex;
pub mod herdr;
pub mod store;

/// The Curriculum `tester` alias selects its authored `testing` skill only
/// for that role. The catalog is supplied by the server's installed source
/// resolver; request payloads cannot supply skill paths or bodies.
mod tester_selection {
    #[derive(Clone, Debug, Eq, PartialEq)]
    pub(crate) struct AuthoredSkill<'a> {
        pub id: &'a str,
        pub path: &'a str,
        pub body: &'a str,
        pub source_revision: &'a str,
    }

    #[derive(Clone, Debug, Eq, PartialEq)]
    pub(crate) enum SelectionError {
        UnsupportedRole,
        MissingTestingSkill,
        AmbiguousTestingSkill,
        IncompleteTestingSkill,
    }

    pub(crate) fn select_tester<'a>(
        role_alias: &str,
        installed: &'a [AuthoredSkill<'a>],
    ) -> Result<&'a AuthoredSkill<'a>, SelectionError> {
        if role_alias != "tester" {
            return Err(SelectionError::UnsupportedRole);
        }
        let mut matching = installed.iter().filter(|skill| skill.id == "testing");
        let skill = matching.next().ok_or(SelectionError::MissingTestingSkill)?;
        if matching.next().is_some() {
            return Err(SelectionError::AmbiguousTestingSkill);
        }
        if skill.path.trim().is_empty()
            || skill.body.trim().is_empty()
            || skill.source_revision.trim().is_empty()
        {
            return Err(SelectionError::IncompleteTestingSkill);
        }
        Ok(skill)
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        const TESTING: AuthoredSkill<'static> = AuthoredSkill {
            id: "testing",
            path: "/generated/testing/SKILL.md",
            body: "Choose a procedure, independent oracle, and negative cases.",
            source_revision: "c9c39549",
        };

        #[test]
        fn tester_selects_one_authored_testing_skill_only() {
            let other = AuthoredSkill {
                id: "messaging",
                ..TESTING
            };
            let installed = [other, TESTING];
            assert_eq!(select_tester("tester", &installed), Ok(&installed[1]));
            assert_eq!(
                select_tester("default", &installed),
                Err(SelectionError::UnsupportedRole)
            );
        }

        #[test]
        fn missing_or_ambiguous_testing_id_refuses() {
            assert_eq!(
                select_tester("tester", &[]),
                Err(SelectionError::MissingTestingSkill)
            );
            assert_eq!(
                select_tester("tester", &[TESTING, TESTING]),
                Err(SelectionError::AmbiguousTestingSkill)
            );
        }

        #[test]
        fn incomplete_installed_skill_refuses() {
            let missing_body = AuthoredSkill {
                body: "",
                ..TESTING
            };
            assert_eq!(
                select_tester("tester", &[missing_body]),
                Err(SelectionError::IncompleteTestingSkill)
            );
        }
    }
}

use codex::{CodexAdapter, ConsumesResetCredit, ResumesCodex};
// The adapter owns construction from registered evidence.  The Nexus exposes
// only its crate-visible observation vocabulary to the ordinary handler.
pub(crate) use codex::{
    Availability, DeliveryGateActivity, DutyActivity, EvidenceMetric, EvidenceQuality,
    EvidenceSource, EvidenceStatus, ObserveSessionsError, ObserveSessionsNormalizer,
    ObservedSession, ReadsIndependentNativeEvents, RegisteredOpaqueIdentity,
    ResolvedNativeIdentity, ResolvesRegisteredIdentity, SessionContextEvidence, TaskActivity,
};
use signal_flow::{Query, Response, RestartRejection, StartRejection};
use std::{
    fs,
    io::{Read, Write},
    os::unix::{
        fs::PermissionsExt,
        net::{UnixListener, UnixStream},
    },
    path::Path,
};
use store::{
    AppliesFlowQuery, AuthorizesFlowRestart, ConfiguresFlowStore, ConfirmsStartedFlow, FlowStore,
    ManagesDeliveryPermits, OpensFlowStore, RecordsPendingThread, RecordsRestartedFlow,
    RegistersFlowIdentity, ReservesPendingStart,
};
use triad_runtime::ConnectionContext;

fn store_binding(binding: signal_flow::DeliveryBinding) -> store::DeliveryBinding {
    store::DeliveryBinding {
        native_thread: binding.native_thread,
        harness_session: binding.harness_session,
        route_identity: binding.route_identity,
        endpoint_identity: binding.endpoint_identity,
        process_pid: binding.process_id,
        process_start_time: binding.process_start_time,
    }
}

fn wire_binding(binding: store::DeliveryBinding) -> signal_flow::DeliveryBinding {
    signal_flow::DeliveryBinding {
        native_thread: binding.native_thread,
        harness_session: binding.harness_session,
        route_identity: binding.route_identity,
        endpoint_identity: binding.endpoint_identity,
        process_id: binding.process_pid,
        process_start_time: binding.process_start_time,
    }
}

fn store_generation(
    generation: signal_flow::BindingGeneration,
) -> Result<u64, signal_flow::DeliveryRejection> {
    generation
        .try_into()
        .map_err(|_| signal_flow::DeliveryRejection::GenerationOverflow)
}

fn wire_generation(
    generation: u64,
) -> Result<signal_flow::BindingGeneration, signal_flow::DeliveryRejection> {
    generation
        .try_into()
        .map_err(|_| signal_flow::DeliveryRejection::GenerationOverflow)
}

fn wire_permit(
    permit: store::DeliveryPermit,
) -> Result<signal_flow::DeliveryPermit, signal_flow::DeliveryRejection> {
    Ok(signal_flow::DeliveryPermit {
        attempt_id: permit.attempt_id,
        source_event_identifier: permit.source_event_identifier,
        delivery_token: permit.token,
        binding_generation: wire_generation(permit.binding_generation)?,
        delivery_binding: wire_binding(permit.binding),
    })
}

fn wire_rejection(rejection: store::DeliveryRejection) -> signal_flow::DeliveryRejection {
    use signal_flow::DeliveryRejection as Wire;
    use store::DeliveryRejection as Store;
    match rejection {
        Store::UnknownFlow => Wire::UnknownFlow,
        Store::MissingState => Wire::MissingState,
        Store::BindingUnavailable => Wire::BindingUnavailable,
        Store::StaleBinding => Wire::StaleBinding,
        Store::StaleGeneration => Wire::StaleGeneration,
        Store::RefreshHeld => Wire::RefreshHeld(signal_flow::RefreshHeld {
            delivery_permit_option: None,
        }),
        Store::Busy => Wire::Busy,
        Store::AttemptConflict => Wire::AttemptConflict,
        Store::StalePermit => Wire::StalePermit,
        Store::TransitionConflict => Wire::TransitionConflict,
        Store::ActivePermit => Wire::ActivePermit,
        Store::CorruptState => Wire::CorruptState,
        Store::GenerationOverflow => Wire::GenerationOverflow,
        Store::CapacityExhausted => Wire::CapacityExhausted,
    }
}

fn wire_state(
    state: store::BindingState,
) -> Result<signal_flow::DeliveryState, signal_flow::DeliveryRejection> {
    Ok(signal_flow::DeliveryState {
        delivery_binding: wire_binding(state.binding),
        binding_generation: wire_generation(state.binding_generation)?,
        lifecycle_generation: wire_generation(state.lifecycle_generation)?,
        admission_gate: match state.admission {
            store::AdmissionGate::Open => signal_flow::AdmissionGate::Open,
            store::AdmissionGate::RefreshHeld { transition_id } => {
                signal_flow::AdmissionGate::RefreshHeld(signal_flow::RefreshHeld_Data {
                    transition_id,
                })
            }
        },
        delivery_permit_option: state.permit.map(wire_permit).transpose()?,
    })
}

/// The exact, server-resolved facts a Message component must authorize before
/// Flow changes a delivery gate. This is assembled from the producer-owned
/// request after Flow has accepted the Unix connection; it has no caller
/// identity or native-binding constructor.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PreAcquireScope {
    pub flow_id: signal_flow::FlowId,
    pub expected_binding: signal_flow::DeliveryBinding,
    pub expected_binding_generation: signal_flow::BindingGeneration,
    pub source_event_identifier: signal_flow::SourceEventIdentifier,
    pub attempt_id: signal_flow::AttemptId,
}

impl From<&signal_flow::AcquireDelivery> for PreAcquireScope {
    fn from(request: &signal_flow::AcquireDelivery) -> Self {
        Self {
            flow_id: request.flow_id.clone(),
            expected_binding: request.delivery_binding.clone(),
            expected_binding_generation: request.binding_generation,
            source_event_identifier: request.source_event_identifier.clone(),
            attempt_id: request.attempt_id.clone(),
        }
    }
}

/// An opaque grant produced only by a trusted, registered local component
/// verifier. Its fields remain private so a Signal caller cannot manufacture
/// authority from a UID, PID, or native-binding-shaped payload.
#[derive(Debug)]
pub(crate) struct VerifiedPreAcquire {
    _private: (),
}

impl VerifiedPreAcquire {
    /// Only a validator which has already matched the accepted kernel peer to
    /// the registered Message component may mint this internal marker. It is
    /// intentionally unavailable to the Signal codec and carries no claimed
    /// sender identity.
    pub(crate) fn from_verified_component() -> Self {
        Self { _private: () }
    }
}

/// A refusal deliberately has no public Signal representation yet. The
/// existing Flow delivery vocabulary has no authorization rejection, so the
/// ordinary socket maps it to its existing fail-closed StoreRefused reply.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DelegationRefusal {
    VerifierUnavailable,
    MessageComponentUnregistered,
    CapabilityGenerationMismatch,
    ScopeRejected,
}

/// Verifies a pre-acquire authority record against the kernel-vouched peer of
/// the accepted Flow Unix connection. Implementations must resolve the
/// registered Message component and its current capability generation; they
/// must not trust query fields as sender identity.
pub(crate) trait FlowLockedDelegationVerifier {
    fn verify_pre_acquire(
        &self,
        connection: &ConnectionContext,
        scope: PreAcquireScope,
    ) -> Result<VerifiedPreAcquire, DelegationRefusal>;
}

struct UnavailableDelegationVerifier;

impl FlowLockedDelegationVerifier for UnavailableDelegationVerifier {
    fn verify_pre_acquire(
        &self,
        _: &ConnectionContext,
        _: PreAcquireScope,
    ) -> Result<VerifiedPreAcquire, DelegationRefusal> {
        Err(DelegationRefusal::VerifierUnavailable)
    }
}

pub struct RunningNexus {
    pub store: FlowStore,
    pub codex: CodexAdapter,
    pub herdr: herdr::HerdrCli,
}

impl RunningNexus {
    /// Dispatch one ordinary request from an accepted Flow socket. Delivery
    /// acquisition is the identity-sensitive edge: no verifier means no
    /// permit. Direct in-process dispatch remains available for Flow's own
    /// lifecycle work and fixtures, but never substitutes for socket peer
    /// verification.
    pub(crate) fn dispatch_ordinary_peer<V: FlowLockedDelegationVerifier>(
        &self,
        connection: &ConnectionContext,
        verifier: &V,
        query: Query,
    ) -> Response {
        if let Query::AcquireDelivery(request) = &query {
            if verifier
                .verify_pre_acquire(connection, PreAcquireScope::from(request))
                .is_err()
            {
                return Response::DeliveryAcquireRejected(
                    signal_flow::DeliveryRejection::StoreRefused,
                );
            }
        }
        self.dispatch(query)
    }

    pub(crate) fn serve_ordinary_with_delegation_verifier<V: FlowLockedDelegationVerifier>(
        &self,
        socket: &Path,
        verifier: &V,
    ) -> Result<(), String> {
        let _ = fs::remove_file(socket);
        let listener = UnixListener::bind(socket).map_err(|error| error.to_string())?;
        fs::set_permissions(socket, fs::Permissions::from_mode(0o600))
            .map_err(|error| error.to_string())?;
        loop {
            let (mut peer, _) = listener.accept().map_err(|error| error.to_string())?;
            let connection =
                ConnectionContext::from_stream(&peer).map_err(|error| error.to_string())?;
            let query = Frame::read_query(&mut peer)?;
            let response = self.dispatch_ordinary_peer(&connection, verifier, query);
            Frame::write_response(&mut peer, &response)?;
        }
    }
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
                let goal = match request.flow_type.as_str() {
                    "codex-medium" => {
                        "Follow the predefined Codex medium flow procedure. Read the origin clue first, recover the caller's goal from its transcript, then carry the work to completion."
                    }
                    _ => return Response::StartRejected(StartRejection::UnknownFlowType),
                };
                let Ok(Some(pending)) = self.store.reserve_pending_start(Query::Start(request))
                else {
                    return Response::StartRejected(StartRejection::LaunchRefused);
                };
                let launched =
                    self.codex
                        .start_codex_observed(&pending.flow_id, goal, &origin, |thread| {
                            if self
                                .store
                                .record_pending_thread(&pending, thread.into())
                                .unwrap_or(false)
                            {
                                Ok(())
                            } else {
                                Err(codex::CodexAdapterUnavailable::Protocol(
                                    "pending thread persistence failed".into(),
                                ))
                            }
                        });
                match launched {
                    Ok(_) => self
                        .store
                        .confirm_started(&pending.flow_id)
                        .unwrap_or(Response::StartRejected(StartRejection::LaunchRefused)),
                    Err(_) => Response::StartRejected(StartRejection::LaunchRefused),
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
                let origin = signal_flow::OriginClue {
                    flow_id: token.authority_flow_id.clone(),
                    session_id: token.thread_id.clone(),
                    turn_id: "restart".into(),
                };
                if self
                    .codex
                    .resume_codex(&token.thread_id, "Resume this Flow.", &origin)
                    .is_err()
                {
                    return Response::RestartRejected(RestartRejection::ResumeRefused);
                }
                self.store
                    .record_restarted(token)
                    .unwrap_or(Response::RestartRejected(RestartRejection::ResumeRefused))
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
            Query::AcquireDelivery(request) => {
                let Ok(expected_binding_generation) = store_generation(request.binding_generation)
                else {
                    return Response::DeliveryAcquireRejected(
                        signal_flow::DeliveryRejection::GenerationOverflow,
                    );
                };
                match self.store.acquire_delivery(store::AcquireDelivery {
                    flow_id: request.flow_id,
                    expected_binding: store_binding(request.delivery_binding),
                    expected_binding_generation,
                    attempt_id: request.attempt_id,
                    source_event_identifier: request.source_event_identifier,
                }) {
                    Ok(store::AcquireDeliveryOutcome::Granted(permit)) => match wire_permit(permit)
                    {
                        Ok(permit) => Response::DeliveryGranted(permit),
                        Err(rejection) => Response::DeliveryAcquireRejected(rejection),
                    },
                    Ok(store::AcquireDeliveryOutcome::AlreadyGranted(permit)) => {
                        match wire_permit(permit) {
                            Ok(permit) => Response::DeliveryAlreadyGranted(permit),
                            Err(rejection) => Response::DeliveryAcquireRejected(rejection),
                        }
                    }
                    Ok(store::AcquireDeliveryOutcome::Rejected(rejection)) => {
                        Response::DeliveryAcquireRejected(wire_rejection(rejection))
                    }
                    Err(_) => Response::DeliveryAcquireRejected(
                        signal_flow::DeliveryRejection::StoreRefused,
                    ),
                }
            }
            Query::BeginRefresh(request) => {
                let Ok(expected_binding_generation) = store_generation(request.binding_generation)
                else {
                    return Response::RefreshRejected(
                        signal_flow::DeliveryRejection::GenerationOverflow,
                    );
                };
                match self.store.begin_refresh(store::BeginRefresh {
                    flow_id: request.flow_id,
                    expected_binding_generation,
                    transition_id: request.transition_id,
                }) {
                    Ok(store::BeginRefreshOutcome::Held { active_permit }) => {
                        match active_permit.map(wire_permit).transpose() {
                            Ok(delivery_permit_option) => {
                                Response::RefreshHeld(signal_flow::RefreshHeld {
                                    delivery_permit_option,
                                })
                            }
                            Err(rejection) => Response::RefreshRejected(rejection),
                        }
                    }
                    Ok(store::BeginRefreshOutcome::AlreadyHeld { active_permit }) => {
                        match active_permit.map(wire_permit).transpose() {
                            Ok(delivery_permit_option) => {
                                Response::RefreshAlreadyHeld(signal_flow::RefreshHeld {
                                    delivery_permit_option,
                                })
                            }
                            Err(rejection) => Response::RefreshRejected(rejection),
                        }
                    }
                    Ok(store::BeginRefreshOutcome::Rejected(rejection)) => {
                        Response::RefreshRejected(wire_rejection(rejection))
                    }
                    Err(_) => {
                        Response::RefreshRejected(signal_flow::DeliveryRejection::StoreRefused)
                    }
                }
            }
            Query::ReleaseConfirmed(request) => {
                let Ok(expected_binding_generation) = store_generation(request.binding_generation)
                else {
                    return Response::DeliveryReleaseRejected(
                        signal_flow::DeliveryRejection::GenerationOverflow,
                    );
                };
                let receipt = signal_flow::ReleaseReceipt {
                    attempt_id: request.attempt_id.clone(),
                    transport_receipt_id: request.transport_receipt_id.clone(),
                };
                match self.store.release_confirmed(store::ReleaseConfirmed {
                    flow_id: request.flow_id,
                    attempt_id: request.attempt_id,
                    token: request.delivery_token,
                    binding: store_binding(request.delivery_binding),
                    expected_binding_generation,
                    transport_receipt_id: request.transport_receipt_id,
                }) {
                    Ok(store::ReleaseConfirmedOutcome::Released) => {
                        Response::DeliveryReleased(receipt)
                    }
                    Ok(store::ReleaseConfirmedOutcome::AlreadyReleased) => {
                        Response::DeliveryAlreadyReleased(receipt)
                    }
                    Ok(store::ReleaseConfirmedOutcome::Rejected(rejection)) => {
                        Response::DeliveryReleaseRejected(wire_rejection(rejection))
                    }
                    Err(_) => Response::DeliveryReleaseRejected(
                        signal_flow::DeliveryRejection::StoreRefused,
                    ),
                }
            }
            Query::ReadyReattach(request) => {
                let Ok(expected_old_binding_generation) =
                    store_generation(request.expected_old_binding_generation.binding_generation)
                else {
                    return Response::DeliveryReattachRejected(
                        signal_flow::DeliveryRejection::GenerationOverflow,
                    );
                };
                match self.store.ready_reattach(store::ReadyReattach {
                    flow_id: request.flow_id,
                    transition_id: request.transition_id,
                    expected_old_binding: store::DeliveryBinding {
                        native_thread: request.expected_old_binding.native_thread,
                        harness_session: request.expected_old_binding.harness_session,
                        route_identity: request.expected_old_binding.route_identity,
                        endpoint_identity: request.expected_old_binding.endpoint_identity,
                        process_pid: request.expected_old_binding.process_id,
                        process_start_time: request.expected_old_binding.process_start_time,
                    },
                    expected_old_binding_generation,
                    registration_id: request.registration_id,
                }) {
                    Ok(store::ReadyReattachOutcome::Opened { binding_generation }) => {
                        match wire_generation(binding_generation) {
                            Ok(binding_generation) => {
                                Response::DeliveryReattached(signal_flow::ReattachReceipt {
                                    binding_generation,
                                })
                            }
                            Err(rejection) => Response::DeliveryReattachRejected(rejection),
                        }
                    }
                    Ok(store::ReadyReattachOutcome::AlreadyOpened { binding_generation }) => {
                        match wire_generation(binding_generation) {
                            Ok(binding_generation) => {
                                Response::DeliveryAlreadyReattached(signal_flow::ReattachReceipt {
                                    binding_generation,
                                })
                            }
                            Err(rejection) => Response::DeliveryReattachRejected(rejection),
                        }
                    }
                    Ok(store::ReadyReattachOutcome::Rejected(rejection)) => {
                        Response::DeliveryReattachRejected(wire_rejection(rejection))
                    }
                    Err(_) => Response::DeliveryReattachRejected(
                        signal_flow::DeliveryRejection::StoreRefused,
                    ),
                }
            }
            Query::ReadDeliveryState(request) => {
                match self.store.read_delivery_state(&request.flow_id) {
                    Ok(store::ReadDeliveryStateOutcome::State(state)) => match wire_state(state) {
                        Ok(state) => Response::DeliveryStateRead(state),
                        Err(rejection) => Response::DeliveryStateRejected(rejection),
                    },
                    Ok(store::ReadDeliveryStateOutcome::Rejected(rejection)) => {
                        Response::DeliveryStateRejected(wire_rejection(rejection))
                    }
                    Err(_) => Response::DeliveryStateRejected(
                        signal_flow::DeliveryRejection::StoreRefused,
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
                .codex
                .consume_reset_credit(&request)
                .map(meta_signal_flow::Response::ResetConsumed)
                .unwrap_or(meta_signal_flow::Response::ResetRejected(
                    meta_signal_flow::ResetRejection::AdapterUnavailable,
                )),
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
            meta_signal_flow::Query::SubmitBindingRegistration(_) => {
                meta_signal_flow::Response::BindingRegistrationRejected(
                    meta_signal_flow::BindingRegistrationRejection::VerifierUnavailable,
                )
            }
        }
    }
}

pub trait OpensRunningNexus {
    fn open(
        store: &Path,
        socket: String,
        model: String,
        timeout: std::time::Duration,
    ) -> Result<Self, store::StoreError>
    where
        Self: Sized;
}

impl OpensRunningNexus for RunningNexus {
    fn open(
        store: &Path,
        socket: String,
        model: String,
        timeout: std::time::Duration,
    ) -> Result<Self, store::StoreError> {
        Ok(Self {
            store: FlowStore::open(store)?,
            codex: CodexAdapter {
                socket,
                model,
                timeout,
            },
            herdr: herdr::HerdrCli::default(),
        })
    }
}

pub trait ServesOrdinary {
    fn serve_ordinary(&self, socket: &Path) -> Result<(), String>;
}

impl ServesOrdinary for RunningNexus {
    fn serve_ordinary(&self, socket: &Path) -> Result<(), String> {
        self.serve_ordinary_with_delegation_verifier(socket, &UnavailableDelegationVerifier)
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
    use super::{store_generation, wire_generation, wire_rejection, Dispatches, RunningNexus};
    use crate::{
        codex::CodexAdapter,
        herdr::HerdrCli,
        store::{FlowStore, OpensFlowStore},
    };
    use signal_flow::{
        EndpointSelection, FlowLifecycle, FlowNode, HarnessKind, HerdrRoute, HerdrRouteSelection,
        OriginClue, Query, Response,
    };
    use std::{fs, os::unix::fs::PermissionsExt, path::PathBuf, time::Duration};

    struct NexusFixture {
        directory: tempfile::TempDir,
        nexus: RunningNexus,
        snapshot_program: PathBuf,
    }

    trait ControlsHerdrSnapshot {
        fn set_agents(&self, agents: Vec<serde_json::Value>);
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
            let nexus = RunningNexus {
                store: FlowStore::open(&directory.path().join("flow.sema")).expect("fixture store"),
                codex: CodexAdapter {
                    socket: "unused".into(),
                    model: "unused".into(),
                    timeout: Duration::from_secs(1),
                },
                herdr: HerdrCli::at(snapshot_program.clone(), flows_root),
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

    #[test]
    fn direct_binding_registration_without_a_validator_is_refused() {
        let fixture = NexusFixture::new();
        let submission = meta_signal_flow::BindingRegistrationSubmission {
            flow_node: fixture.node(),
            registration_id: "registration-raw".into(),
            binding_registration_phase: meta_signal_flow::BindingRegistrationPhase::Bootstrap,
            readiness_receipt_id: "receipt-raw".into(),
            proof_digest: "digest-raw".into(),
        };
        assert_eq!(
            fixture
                .nexus
                .dispatch_meta(meta_signal_flow::Query::SubmitBindingRegistration(
                    submission
                )),
            meta_signal_flow::Response::BindingRegistrationRejected(
                meta_signal_flow::BindingRegistrationRejection::VerifierUnavailable
            )
        );
    }

    #[test]
    fn delivery_generation_conversion_refuses_negative_and_unrepresentable_values() {
        assert_eq!(
            store_generation(-1),
            Err(signal_flow::DeliveryRejection::GenerationOverflow)
        );
        assert_eq!(
            wire_generation(u64::MAX),
            Err(signal_flow::DeliveryRejection::GenerationOverflow)
        );
    }

    #[test]
    fn delivery_rejection_keeps_capacity_and_missing_state_typed() {
        assert_eq!(
            wire_rejection(crate::store::DeliveryRejection::CapacityExhausted),
            signal_flow::DeliveryRejection::CapacityExhausted
        );
        assert_eq!(
            wire_rejection(crate::store::DeliveryRejection::MissingState),
            signal_flow::DeliveryRejection::MissingState
        );
    }

    #[test]
    fn ordinary_peer_without_registered_component_verifier_cannot_acquire_delivery() {
        let fixture = NexusFixture::new();
        let request = signal_flow::AcquireDelivery {
            flow_id: "flow-locked".into(),
            delivery_binding: signal_flow::DeliveryBinding {
                native_thread: "native".into(),
                harness_session: "session".into(),
                route_identity: "route".into(),
                endpoint_identity: "endpoint".into(),
                process_id: 42,
                process_start_time: 7,
            },
            binding_generation: 1,
            attempt_id: "attempt".into(),
            source_event_identifier: "event".into(),
        };
        let connection =
            ConnectionContext::from(triad_runtime::UnixCredentials::new(1000, 1000, 44));

        assert_eq!(
            fixture.nexus.dispatch_ordinary_peer(
                &connection,
                &UnavailableDelegationVerifier,
                Query::AcquireDelivery(request),
            ),
            Response::DeliveryAcquireRejected(signal_flow::DeliveryRejection::StoreRefused),
        );
    }

    #[test]
    fn preacquire_scope_binds_every_permit_key_before_verification() {
        let request = signal_flow::AcquireDelivery {
            flow_id: "recipient".into(),
            delivery_binding: signal_flow::DeliveryBinding {
                native_thread: "native".into(),
                harness_session: "session".into(),
                route_identity: "route".into(),
                endpoint_identity: "endpoint".into(),
                process_id: 99,
                process_start_time: 12,
            },
            binding_generation: 3,
            attempt_id: "attempt-3".into(),
            source_event_identifier: "event-3".into(),
        };
        assert_eq!(
            PreAcquireScope::from(&request),
            PreAcquireScope {
                flow_id: "recipient".into(),
                expected_binding: request.delivery_binding,
                expected_binding_generation: 3,
                source_event_identifier: "event-3".into(),
                attempt_id: "attempt-3".into(),
            }
        );
        let _ = VerifiedPreAcquire::from_verified_component();
    }

    struct PermittingVerifier;

    impl FlowLockedDelegationVerifier for PermittingVerifier {
        fn verify_pre_acquire(
            &self,
            _: &ConnectionContext,
            _: PreAcquireScope,
        ) -> Result<VerifiedPreAcquire, DelegationRefusal> {
            Ok(VerifiedPreAcquire::from_verified_component())
        }
    }

    #[test]
    fn a_verified_component_reaches_the_store_instead_of_the_fail_closed_gate() {
        let fixture = NexusFixture::new();
        let request = signal_flow::AcquireDelivery {
            flow_id: "unregistered".into(),
            delivery_binding: signal_flow::DeliveryBinding {
                native_thread: "native".into(),
                harness_session: "session".into(),
                route_identity: "route".into(),
                endpoint_identity: "endpoint".into(),
                process_id: 42,
                process_start_time: 7,
            },
            binding_generation: 1,
            attempt_id: "attempt".into(),
            source_event_identifier: "event".into(),
        };
        let connection =
            ConnectionContext::from(triad_runtime::UnixCredentials::new(1000, 1000, 44));

        assert_eq!(
            fixture.nexus.dispatch_ordinary_peer(
                &connection,
                &PermittingVerifier,
                Query::AcquireDelivery(request),
            ),
            Response::DeliveryAcquireRejected(signal_flow::DeliveryRejection::UnknownFlow),
        );
    }
}
