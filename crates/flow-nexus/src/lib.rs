//! Flow Nexus dispatches typed ordinary and privileged Signal requests.
pub mod claude;
pub mod codex;
pub mod composition;
#[cfg(test)]
mod fixture_executable;
pub mod herdr;
pub mod launching;
pub mod store;
pub mod title;

use codex::{CodexEndpoints, ConsumesResetCredit};
use composition::{LaunchBundles, LaunchComposer, OpensLaunchComposer};
use herdr::OperatesHerdrPane;
use launching::{LaunchesFlows, ObservesLaunch, PrunesLaunchBundles};
use signal_flow::{
    EndpointSelection, FlowLifecycle, FlowNode, HerdrRoute, HerdrRouteSelection, ObserveSelection,
    Query, Response, RestartRejection,
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
    sync::Mutex,
    time::Duration,
};
use store::{
    AppliesFlowQuery, AuthorizesFlowRestart, ConfiguresFlowStore, FlowStore, OpensFlowStore,
    ReadsFlowRows, RecordsFlowLifecycle, RecordsReplacement, RegistersExistingFlow,
    RegistersFlowIdentity,
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
    /// Serializes dispatch; see `dispatch_serially`.
    pub dispatch_gate: Mutex<()>,
}

pub trait Dispatches {
    fn dispatch(&self, query: Query) -> Response;
    fn dispatch_meta(&self, query: meta_signal_flow::Query) -> meta_signal_flow::Response;
}

impl Dispatches for RunningNexus {
    fn dispatch(&self, query: Query) -> Response {
        match query {
            Query::Start(request) => {
                let launch_request_id = request.launch_profile.launch_request_id.clone();
                if let Some(settled) = self.settled(&request) {
                    return settled;
                }
                let response = self.start(request);
                self.settle(&launch_request_id, response)
            }
            Query::Replace(request) => self.replace(request),
            Query::LaunchStatus(launch_request_id) => self.launch_status(&launch_request_id),
            // Over a connection Observe streams; a direct dispatch answers the
            // subscription's opening frame.
            Query::Observe(ObserveSelection::Launch(launch_request_id)) => {
                self.launch_status(&launch_request_id)
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
            Query::Send(request) => {
                let node = match self.store.flow_node(&request.flow_id) {
                    Ok(Some(node)) => node,
                    Ok(None) => {
                        return Response::SendRejected(signal_flow::SendRejection::UnknownFlow);
                    }
                    Err(_) => {
                        return Response::SendRejected(
                            signal_flow::SendRejection::PersistenceRefused,
                        );
                    }
                };
                if node.flow_lifecycle == FlowLifecycle::Stopped {
                    return Response::SendRejected(signal_flow::SendRejection::FlowStopped);
                }
                match self.store.held_successor(&request.flow_id) {
                    Ok(false) => {}
                    Ok(true) => {
                        return Response::SendRejected(
                            signal_flow::SendRejection::RouteUnavailable,
                        );
                    }
                    Err(_) => {
                        return Response::SendRejected(
                            signal_flow::SendRejection::PersistenceRefused,
                        );
                    }
                }
                let node = self.herdr.refresh_route(node);
                if !matches!(
                    node.herdr_route_selection,
                    HerdrRouteSelection::Available(_)
                ) {
                    return Response::SendRejected(signal_flow::SendRejection::RouteUnavailable);
                }
                let send_outcome = match self.herdr.prompt(&node, &request.bare_input) {
                    Ok(send_outcome) => send_outcome,
                    Err(rejection) => return Response::SendRejected(rejection),
                };
                // A Pending flow becomes Active only when it was seen reacting
                // to a real Send. The input is already typed, so a refused
                // record is not turned into a rejection: the flow stays
                // Pending until its next Presented Send.
                if node.flow_lifecycle == FlowLifecycle::Pending
                    && matches!(send_outcome, signal_flow::SendOutcome::Presented(_))
                {
                    let _ = self.store.record_active(&request.flow_id);
                }
                Response::Sent(send_outcome)
            }
            Query::Stop(flow_id) => {
                let node = match self.store.flow_node(&flow_id) {
                    Ok(Some(node)) => node,
                    Ok(None) => {
                        return Response::StopRejected(signal_flow::StopRejection::UnknownFlow);
                    }
                    Err(_) => {
                        return Response::StopRejected(
                            signal_flow::StopRejection::PersistenceRefused,
                        );
                    }
                };
                if node.flow_lifecycle == FlowLifecycle::Stopped {
                    return Response::StopRejected(signal_flow::StopRejection::AlreadyStopped);
                }
                let node = self.herdr.refresh_route(node);
                if !matches!(
                    node.herdr_route_selection,
                    HerdrRouteSelection::Available(_)
                ) {
                    return Response::StopRejected(signal_flow::StopRejection::RouteUnavailable);
                }
                if !self.herdr.close(&node) {
                    return Response::StopRejected(signal_flow::StopRejection::CloseRefused);
                }
                if !self.store.record_stopped(&flow_id).unwrap_or(false) {
                    return Response::StopRejected(signal_flow::StopRejection::PersistenceRefused);
                }
                self.prune_launch_bundles_of(&flow_id);
                Response::Stopped(flow_id)
            }
            Query::List(_) => {
                self.store
                    .flow_nodes()
                    .map(Response::Listed)
                    .unwrap_or(Response::ListRejected(
                        signal_flow::ListRejection::PersistenceRefused,
                    ))
            }
        }
    }

    fn dispatch_meta(&self, query: meta_signal_flow::Query) -> meta_signal_flow::Response {
        match query {
            meta_signal_flow::Query::Configure(configuration) => {
                // The Codex adapters and the composer read the runtime
                // configuration when the Nexus opens, so every change waits
                // for the next start.
                let stored = self
                    .store
                    .configure(configuration)
                    .and_then(|_| self.store.configuration());
                let Ok(configuration) = stored else {
                    return meta_signal_flow::Response::ConfigureRejected(
                        meta_signal_flow::ConfigureRejection::StoreRefused,
                    );
                };
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
                        Ok(Response::RecipientResolved(_))
                        | Ok(Response::RecipientResolutionRejected(
                            signal_flow::RecipientResolutionRejection::FlowUnavailable,
                        )) => {
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
    /// Opens the Nexus from its default configuration: the store at the
    /// default location seeds or resumes configuration, and deployment
    /// overrides are laid over the stored runtime configuration.
    fn open(
        defaults: &store::DefaultConfiguration,
        overrides: &store::DeploymentOverrides,
    ) -> Result<Self, store::StoreError>
    where
        Self: Sized;
}

impl OpensRunningNexus for RunningNexus {
    fn open(
        defaults: &store::DefaultConfiguration,
        overrides: &store::DeploymentOverrides,
    ) -> Result<Self, store::StoreError> {
        let store = FlowStore::open_seeded(&defaults.store_path(), defaults)?;
        let runtime = store.adopt_overrides(overrides)?;
        let codex_endpoints = CodexEndpoints::from(&runtime);
        let overlap = codex_endpoints.overlapping_models();
        if !overlap.is_empty() {
            eprintln!(
                "flow-nexus: models selected by both Codex endpoints are unavailable: {overlap:?}"
            );
        }
        let source_root = PathBuf::from(&runtime.source_root);
        let launch_bundles = LaunchBundles::at(defaults.launch_bundle_directory());
        Ok(Self {
            store,
            codex_endpoints: codex_endpoints.clone(),
            herdr: herdr::HerdrCli::default()
                .with_source_root(&source_root)
                .with_codex_endpoints(codex_endpoints)
                .with_launch_bundles(launch_bundles.clone()),
            composer: LaunchComposer::at(source_root, launch_bundles),
            dispatch_gate: Mutex::new(()),
        })
    }
}

/// One accepted connection: read one frame, answer it, close — except
/// Observe, which keeps the connection as its subscription until the
/// outcome frame. A malformed frame, an idle peer past the read timeout, or
/// a failed write drops only this connection.
pub struct Connection {
    peer: UnixStream,
}

impl Connection {
    const READ_TIMEOUT: Duration = Duration::from_secs(5);
    const WRITE_TIMEOUT: Duration = Duration::from_secs(5);

    pub fn accepted(peer: UnixStream) -> Result<Self, String> {
        peer.set_read_timeout(Some(Self::READ_TIMEOUT))
            .and_then(|_| peer.set_write_timeout(Some(Self::WRITE_TIMEOUT)))
            .map_err(|error| error.to_string())?;
        Ok(Self { peer })
    }

    fn serve_ordinary(mut self, nexus: &RunningNexus) -> Result<(), String> {
        let query = Frame::read_query(&mut self.peer)?;
        let response = match query {
            // Reads of launch state never wait behind a running launch.
            Query::LaunchStatus(launch_request_id) => nexus.launch_status(&launch_request_id),
            Query::Observe(ObserveSelection::Launch(launch_request_id)) => {
                let peer = &mut self.peer;
                return nexus.observe_launch(&launch_request_id, &mut |response| {
                    Frame::write_response(peer, response)
                });
            }
            query => nexus.dispatch_serially(query),
        };
        Frame::write_response(&mut self.peer, &response)
    }

    fn serve_meta(mut self, nexus: &RunningNexus) -> Result<(), String> {
        let query = Frame::read_meta_query(&mut self.peer)?;
        let response = nexus.dispatch_meta_serially(query);
        Frame::write_meta_response(&mut self.peer, &response)
    }
}

/// Binds a socket and serves each connection on its own thread.
pub trait ListensOnSocket {
    fn listen(
        &self,
        socket: &Path,
        surface: &str,
        serve: fn(Connection, &Self) -> Result<(), String>,
    ) -> Result<(), String>
    where
        Self: Sync + Sized,
    {
        let _ = fs::remove_file(socket);
        let listener = UnixListener::bind(socket).map_err(|error| error.to_string())?;
        fs::set_permissions(socket, fs::Permissions::from_mode(0o600))
            .map_err(|e| e.to_string())?;
        std::thread::scope(|scope| {
            for peer in listener.incoming() {
                let connection = match peer
                    .map_err(|error| error.to_string())
                    .and_then(Connection::accepted)
                {
                    Ok(connection) => connection,
                    Err(error) => {
                        eprintln!("flow-nexus: {surface} accept failed: {error}");
                        continue;
                    }
                };
                scope.spawn(move || {
                    if let Err(error) = serve(connection, self) {
                        eprintln!("flow-nexus: {surface} connection dropped: {error}");
                    }
                });
            }
        });
        Err(format!("{surface} listener closed"))
    }
}

impl ListensOnSocket for RunningNexus {}

impl RunningNexus {
    /// Store transitions are read-modify-write; one dispatch runs at a time
    /// across both sockets while frames are read concurrently.
    fn dispatch_serially(&self, query: Query) -> Response {
        let _turn = self
            .dispatch_gate
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        self.dispatch(query)
    }

    fn dispatch_meta_serially(&self, query: meta_signal_flow::Query) -> meta_signal_flow::Response {
        let _turn = self
            .dispatch_gate
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        self.dispatch_meta(query)
    }
}

pub trait ServesOrdinary {
    fn serve_ordinary(&self, socket: &Path) -> Result<(), String>;
}

impl ServesOrdinary for RunningNexus {
    fn serve_ordinary(&self, socket: &Path) -> Result<(), String> {
        self.listen(socket, "ordinary", Connection::serve_ordinary)
    }
}

pub trait ServesMeta {
    fn serve_meta(&self, socket: &Path) -> Result<(), String>;
}

impl ServesMeta for RunningNexus {
    fn serve_meta(&self, socket: &Path) -> Result<(), String> {
        self.listen(socket, "meta", Connection::serve_meta)
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
    use crate::fixture_executable::{FixtureExecutable, InstallsScript};
    use crate::{
        codex::{CodexEndpoint, CodexEndpoints},
        composition::{ComposesLaunch, LaunchBundles, LaunchComposer, OpensLaunchComposer},
        herdr::HerdrCli,
        store::{
            FlowStore, OpensFlowStore, RecordsFlowLifecycle, RecordsNativeLaunchBinding,
            RecordsNativeLaunchIntent, RecordsPromptDeliveryIntent, RecordsPromptDeliveryResult,
            RecordsRegistrationAcknowledgement, RegistersFlowIdentity, ReservesLaunchAttempt,
        },
    };
    use sha2::{Digest, Sha256};
    use signal_flow::{
        EndpointSelection, FlowAspect, FlowLifecycle, FlowNode, HarnessKind, HerdrPaneBinding,
        HerdrRoute, HerdrRouteSelection, LaunchAttemptPhase, LaunchProfile, LaunchSource,
        NativeLaunchBinding, NativeLaunchIntent, NativeTranscriptAbsence, NativeTranscriptBoundary,
        OriginClue, PowerLevel, PromptDeliveryIntent, PromptDeliveryResult, Query,
        RegistrationAcknowledgement, Response, StartRejection, StartRequest,
    };
    use std::{
        collections::BTreeSet,
        fs,
        io::Write,
        os::unix::fs::MetadataExt,
        os::unix::net::UnixListener,
        path::{Path, PathBuf},
        time::Duration,
    };

    struct NexusFixture {
        directory: tempfile::TempDir,
        nexus: RunningNexus,
        snapshot_program: PathBuf,
    }

    /// How the fixture Herdr answers `agent prompt`, shaped as Herdr 0.8.2
    /// answers: a success reply on stdout, an error reply on stderr.
    #[derive(Clone, Copy)]
    enum PromptFixture<'a> {
        /// The text is typed and the reply is `agent_prompted` for this pane.
        Prompted(&'a str),
        /// This error code is answered before any text is typed.
        RefusedBeforeInput(&'a str),
        /// The text is typed, then this error code is answered.
        FailedAfterInput(&'a str),
    }

    impl PromptFixture<'_> {
        fn shell(&self, typed: &std::path::Path) -> String {
            let typing = format!("printf '%s' \"$6\" >> '{}'", typed.display());
            let error = |code: &str| {
                format!(
                    "printf '%s\\n' '{{\"id\":\"cli:agent:prompt\",\"error\":{{\"code\":\"{code}\",\"message\":\"fixture\"}}}}' >&2; exit 1"
                )
            };
            match self {
                Self::Prompted(pane) => format!(
                    "{typing}; printf '%s\\n' '{{\"id\":\"cli:agent:prompt\",\"result\":{{\"type\":\"agent_prompted\",\"agent\":{{\"pane_id\":\"{pane}\"}}}}}}'"
                ),
                Self::RefusedBeforeInput(code) => error(code),
                Self::FailedAfterInput(code) => format!("{typing}; {}", error(code)),
            }
        }
    }

    trait ControlsHerdrSnapshot {
        fn set_agents(&self, agents: Vec<serde_json::Value>);
        fn accept_pane_operations(
            &self,
            agents: Vec<serde_json::Value>,
            prompt: PromptFixture,
        ) -> PathBuf;
        /// The bytes the fixture Herdr typed into the pane, in order.
        fn typed(&self) -> Option<String>;
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
                    .with_codex_endpoints(codex_endpoints)
                    .with_launch_bundles(LaunchBundles::at(
                        directory.path().join("launch-bundles"),
                    )),
                composer: LaunchComposer::at(
                    directory.path().to_path_buf(),
                    LaunchBundles::at(directory.path().join("launch-bundles")),
                ),
                dispatch_gate: std::sync::Mutex::new(()),
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
            FixtureExecutable {
                path: self.snapshot_program.clone(),
            }
            .install(&body);
        }

        fn accept_pane_operations(
            &self,
            agents: Vec<serde_json::Value>,
            prompt: PromptFixture,
        ) -> PathBuf {
            let snapshot = serde_json::json!({
                "id":"cli:api:snapshot",
                "result":{"snapshot":{"agents":agents,"protocol":20,"version":"0.8.2"},
                "type":"session_snapshot"}
            });
            let log = self.directory.path().join("herdr-operations.log");
            let typed = self.directory.path().join("pane-typed.txt");
            let body = format!(
                "#!/bin/sh\ncase \"$3 $4\" in\n  \"api snapshot\") printf '%s\\n' '{}' ;;\n  \"agent prompt\") printf '%s\\n' \"$*\" >> '{}'; {} ;;\n  \"pane close\") printf '%s\\n' \"$*\" >> '{}' ;;\n  *) exit 64 ;;\nesac\n",
                snapshot,
                log.display(),
                prompt.shell(&typed),
                log.display(),
            );
            FixtureExecutable {
                path: self.snapshot_program.clone(),
            }
            .install(&body);
            log
        }

        fn typed(&self) -> Option<String> {
            fs::read_to_string(self.directory.path().join("pane-typed.txt")).ok()
        }
    }

    #[test]
    fn a_malformed_frame_drops_only_its_connection() {
        use super::{Frame, ServesOrdinary};
        use std::{io::Write, os::unix::net::UnixStream};
        let fixture: &'static NexusFixture = Box::leak(Box::new(NexusFixture::new()));
        let socket = fixture.directory.path().join("ordinary.sock");
        let served = socket.clone();
        std::thread::spawn(move || fixture.nexus.serve_ordinary(&served));
        let connect = || {
            for _ in 0..200 {
                if let Ok(peer) = UnixStream::connect(&socket) {
                    peer.set_read_timeout(Some(Duration::from_secs(2)))
                        .expect("test peer timeout");
                    return peer;
                }
                std::thread::sleep(Duration::from_millis(10));
            }
            panic!("ordinary socket never listened")
        };

        let mut idle = connect();
        let mut garbage = connect();
        garbage
            .write_all(&16u32.to_be_bytes())
            .and_then(|_| garbage.write_all(&[0xff; 16]))
            .expect("garbage frame written");
        assert!(Frame::read_response(&mut garbage).is_err());

        let mut peer = connect();
        Frame::write_query(&mut peer, &Query::List(signal_flow::ListRequest {}))
            .expect("list written");
        assert!(matches!(
            Frame::read_response(&mut peer).expect("list answered"),
            Response::Listed(_)
        ));
        idle.write_all(&[0]).expect("idle peer still open");
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

    impl NexusFixture {
        fn settled_agent(&self) -> serde_json::Value {
            let mut agent = self.current_agent();
            agent["agent_status"] = serde_json::Value::String("idle".into());
            agent
        }

        fn register_with(&self, lifecycle: FlowLifecycle) {
            let mut node = self.node();
            node.flow_lifecycle = lifecycle;
            assert!(matches!(
                self.nexus
                    .dispatch_meta(meta_signal_flow::Query::RegisterFlow(node)),
                meta_signal_flow::Response::FlowRegistered(_)
            ));
        }

        fn send(&self, bare_input: &str) -> Response {
            self.nexus.dispatch(Query::Send(signal_flow::SendRequest {
                flow_id: "908786".into(),
                bare_input: bare_input.into(),
            }))
        }

        fn only_lifecycle(&self) -> FlowLifecycle {
            let Response::Listed(rows) = self
                .nexus
                .dispatch(Query::List(signal_flow::ListRequest {}))
            else {
                panic!("list must return durable Flow rows")
            };
            assert_eq!(rows.len(), 1);
            rows[0].flow_lifecycle.clone()
        }
    }

    fn prompts(operation_log: &std::path::Path) -> Vec<String> {
        fs::read_to_string(operation_log)
            .unwrap_or_default()
            .lines()
            .filter(|line| line.contains(" agent prompt "))
            .map(str::to_owned)
            .collect()
    }

    const OBSERVED_PROMPT: &str = "--session messaging-build agent prompt w1:p3 bare prompt --wait --until working --until idle --until done --until blocked --timeout 10000";

    #[test]
    fn observed_send_promotes_a_pending_flow_and_types_only_the_bare_input() {
        let fixture = NexusFixture::new();
        let operation_log = fixture.accept_pane_operations(
            vec![fixture.settled_agent()],
            PromptFixture::Prompted("w1:p3"),
        );
        fixture.register_with(FlowLifecycle::Pending);

        let response = fixture.send("bare prompt");
        let Response::Sent(signal_flow::SendOutcome::Presented(receipt)) = response else {
            panic!("a settled recipient seen reacting is Presented: {response:?}")
        };
        assert_eq!(receipt.flow_id, "908786");
        assert_eq!(receipt.herdr_pane_id, "w1:p3");
        assert!(receipt.presentation_observed_unix_milliseconds > 0);
        assert_eq!(fixture.only_lifecycle(), FlowLifecycle::Active);
        assert_eq!(fixture.typed().as_deref(), Some("bare prompt"));
        assert_eq!(prompts(&operation_log), vec![OBSERVED_PROMPT.to_owned()]);

        assert_eq!(
            fixture.nexus.dispatch(Query::Stop("908786".into())),
            Response::Stopped("908786".into())
        );
        assert_eq!(fixture.only_lifecycle(), FlowLifecycle::Stopped);
        let operations = fs::read_to_string(operation_log).expect("Herdr operation log");
        assert!(operations.contains("pane close w1:p3"));
        assert!(!operations.contains("pane wait-output"));
        assert!(!operations.contains("pane read"));
    }

    #[test]
    fn stop_removes_the_bundle_copy_of_the_launch_that_bound_the_flow() {
        let fixture = NexusFixture::new();
        fixture.accept_pane_operations(
            vec![fixture.current_agent()],
            PromptFixture::Prompted("w1:p3"),
        );
        let node = fixture.node();
        assert_eq!(
            fixture
                .nexus
                .dispatch_meta(meta_signal_flow::Query::RegisterFlow(node.clone())),
            meta_signal_flow::Response::FlowRegistered(node)
        );
        let launch = fixture.staged_launch("stopped-request", None);
        launch.reserve(&fixture.nexus);
        launch.record_native_intent(&fixture.nexus);
        launch.bind(&fixture.nexus);
        let bundles = fixture.nexus.composer.launch_bundles().clone();
        let bound_copy = bundles.file_for_request("stopped-request");
        let other_copy = bundles.file_for_request("another-request");
        fs::create_dir_all(bound_copy.parent().unwrap()).unwrap();
        fs::write(&bound_copy, b"bound copy\n").unwrap();
        fs::write(&other_copy, b"other copy\n").unwrap();

        assert_eq!(
            fixture.nexus.dispatch(Query::Stop("908786".into())),
            Response::Stopped("908786".into())
        );
        assert!(!bound_copy.exists(), "the stopped Flow's copy is removed");
        assert!(other_copy.exists(), "another launch's copy is kept");
    }

    #[test]
    fn working_pending_flow_takes_the_send_as_accepted_and_stays_pending() {
        let fixture = NexusFixture::new();
        let operation_log = fixture.accept_pane_operations(
            vec![fixture.current_agent()],
            PromptFixture::Prompted("w1:p3"),
        );
        fixture.register_with(FlowLifecycle::Pending);

        assert_eq!(
            fixture.send("queued prompt"),
            Response::Sent(signal_flow::SendOutcome::Accepted("908786".into()))
        );
        assert_eq!(fixture.only_lifecycle(), FlowLifecycle::Pending);
        assert_eq!(fixture.typed().as_deref(), Some("queued prompt"));
        assert_eq!(
            prompts(&operation_log),
            vec!["--session messaging-build agent prompt w1:p3 queued prompt".to_owned()]
        );
    }

    #[test]
    fn refusal_before_input_types_nothing_and_is_not_delivered() {
        for code in [
            "agent_blocked",
            "agent_not_found",
            "agent_not_ready",
            "agent_target_ambiguous",
            "empty_agent_prompt",
            "agent_prompt_failed",
        ] {
            let fixture = NexusFixture::new();
            let operation_log = fixture.accept_pane_operations(
                vec![fixture.settled_agent()],
                PromptFixture::RefusedBeforeInput(code),
            );
            fixture.register_with(FlowLifecycle::Pending);

            assert_eq!(
                fixture.send("bare prompt"),
                Response::SendRejected(signal_flow::SendRejection::NotDelivered),
                "{code}"
            );
            assert_eq!(fixture.typed(), None, "{code}");
            assert_eq!(prompts(&operation_log).len(), 1, "{code}");
            assert_eq!(fixture.only_lifecycle(), FlowLifecycle::Pending, "{code}");
        }
    }

    #[test]
    fn typed_prompt_whose_reaction_is_unobserved_is_uncertain_once() {
        for failure in [
            "agent_prompt_stalled",
            "timeout",
            "agent_not_running",
            "unlisted",
        ] {
            let fixture = NexusFixture::new();
            let operation_log = fixture.accept_pane_operations(
                vec![fixture.settled_agent()],
                PromptFixture::FailedAfterInput(failure),
            );
            fixture.register_with(FlowLifecycle::Pending);

            assert_eq!(
                fixture.send("bare prompt"),
                Response::Sent(signal_flow::SendOutcome::Uncertain("908786".into())),
                "{failure}"
            );
            assert_eq!(fixture.typed().as_deref(), Some("bare prompt"), "{failure}");
            assert_eq!(
                prompts(&operation_log),
                vec![OBSERVED_PROMPT.to_owned()],
                "{failure}: exactly one prompt, no retry"
            );
            assert_eq!(
                fixture.only_lifecycle(),
                FlowLifecycle::Pending,
                "{failure}"
            );
        }
    }

    #[test]
    fn reaction_reported_for_another_pane_is_uncertain_and_keeps_pending() {
        let fixture = NexusFixture::new();
        let operation_log = fixture.accept_pane_operations(
            vec![fixture.settled_agent()],
            PromptFixture::Prompted("w1:p9"),
        );
        fixture.register_with(FlowLifecycle::Pending);

        assert_eq!(
            fixture.send("bare prompt"),
            Response::Sent(signal_flow::SendOutcome::Uncertain("908786".into()))
        );
        assert_eq!(prompts(&operation_log).len(), 1);
        assert_eq!(fixture.only_lifecycle(), FlowLifecycle::Pending);
    }

    #[test]
    fn active_send_to_a_working_flow_is_accepted_without_claiming_presentation() {
        let fixture = NexusFixture::new();
        fixture.accept_pane_operations(
            vec![fixture.current_agent()],
            PromptFixture::Prompted("w1:p3"),
        );
        fixture.register_with(FlowLifecycle::Active);

        assert_eq!(
            fixture.send("ordinary active prompt"),
            Response::Sent(signal_flow::SendOutcome::Accepted("908786".into()))
        );
        assert_eq!(fixture.only_lifecycle(), FlowLifecycle::Active);
    }

    #[test]
    fn active_send_to_a_settled_flow_is_observed_presented() {
        let fixture = NexusFixture::new();
        fixture.accept_pane_operations(
            vec![fixture.settled_agent()],
            PromptFixture::Prompted("w1:p3"),
        );
        fixture.register_with(FlowLifecycle::Active);

        assert!(matches!(
            fixture.send("bare prompt"),
            Response::Sent(signal_flow::SendOutcome::Presented(_))
        ));
        assert_eq!(fixture.only_lifecycle(), FlowLifecycle::Active);
    }

    #[test]
    fn stale_pending_send_is_refused_before_prompt_and_keeps_pending() {
        let fixture = NexusFixture::new();
        fixture.set_agents(vec![fixture.current_agent()]);
        let mut node = fixture.node();
        node.flow_lifecycle = FlowLifecycle::Pending;
        assert!(matches!(
            fixture
                .nexus
                .dispatch_meta(meta_signal_flow::Query::RegisterFlow(node)),
            meta_signal_flow::Response::FlowRegistered(_)
        ));
        let mut stale = fixture.current_agent();
        stale["terminal_id"] = serde_json::Value::String("term-replaced".into());
        fixture.set_agents(vec![stale]);

        assert_eq!(
            fixture
                .nexus
                .dispatch(Query::Send(signal_flow::SendRequest {
                    flow_id: "908786".into(),
                    bare_input: "stale route prompt".into(),
                })),
            Response::SendRejected(signal_flow::SendRejection::RouteUnavailable)
        );
        let Response::Listed(rows) = fixture
            .nexus
            .dispatch(Query::List(signal_flow::ListRequest {}))
        else {
            panic!("list must return durable Flow rows")
        };
        assert_eq!(rows[0].flow_lifecycle, FlowLifecycle::Pending);
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
        let bundle_path = fixture.directory.path().join("flow-system-prompt.md");
        fs::write(&source_path, b"exact source bytes\n").expect("fixture source");
        fs::write(&bundle_path, b"fixture bundle\n").expect("fixture bundle");
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
            system_prompt_bundle_file: bundle_path.to_string_lossy().into_owned(),
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
        let bundle_path = fixture.directory.path().join("flow-system-prompt.md");
        fs::write(&source_path, b"delayed exact bytes\n").expect("fixture source");
        fs::write(&bundle_path, b"fixture bundle\n").expect("fixture bundle");
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
            system_prompt_bundle_file: bundle_path.to_string_lossy().into_owned(),
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
            "#!/bin/sh\nprintf '%s\\n' \"$*\" >> '{}'\n[ \"$*\" = \"--session fixture-session agent get w1:p1\" ] || exit 64\nprintf '%s\\n' '{}'\n",
            herdr_calls.display(),
            agent
        );
        FixtureExecutable {
            path: fixture.snapshot_program.clone(),
        }
        .install(&body);

        let marker = crate::composition::LaunchReceipt::MARKER;
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
            "--session fixture-session agent get w1:p1\n"
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

        for changed_field in ["terminal", "pane", "harness", "interactive"] {
            let mut agent = fixture.current_agent();
            match changed_field {
                "terminal" => agent["terminal_id"] = "term_replaced".into(),
                "pane" => agent["pane_id"] = "w1:p9".into(),
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

    /// 88475f renamed its Herdr agent after launch (claude-86b6e54c… to
    /// psyche-opus-88475f). The route is the session, pane and terminal
    /// ids; the new name is read back, and the pane is still the one to
    /// close.
    #[test]
    fn a_renamed_agent_keeps_its_route_and_reports_its_current_name() {
        let fixture = NexusFixture::new();
        fixture.set_agents(vec![fixture.current_agent()]);
        let node = fixture.node();
        assert!(matches!(
            fixture
                .nexus
                .dispatch_meta(meta_signal_flow::Query::RegisterFlow(node.clone())),
            meta_signal_flow::Response::FlowRegistered(_)
        ));
        let mut renamed = fixture.current_agent();
        renamed["name"] = "psyche-opus-88475f".into();
        fixture.set_agents(vec![renamed]);
        let Response::RecipientResolved(resolved) = fixture
            .nexus
            .dispatch(Query::ResolveRecipient("908786".into()))
        else {
            panic!("registered recipient resolves")
        };
        let HerdrRouteSelection::Available(route) = &resolved.herdr_route_selection else {
            panic!("a renamed agent keeps its route: {resolved:?}")
        };
        let HerdrRouteSelection::Available(stored) = &node.herdr_route_selection else {
            panic!("fixture node carries a route")
        };
        assert_eq!(route.herdr_agent_name, "psyche-opus-88475f");
        assert_eq!(route.herdr_session_name, stored.herdr_session_name);
        assert_eq!(route.herdr_pane_id, stored.herdr_pane_id);
        assert_eq!(route.herdr_terminal_id, stored.herdr_terminal_id);
        assert!(crate::herdr::ReadsHerdrRoster::route_is_available(
            &fixture.nexus.herdr,
            &node
        ));
        assert_eq!(
            fixture.nexus.herdr.pane_presence(&node),
            crate::herdr::PanePresence::Present
        );
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

    /// A launch driven phase by phase through the store, as the Start path
    /// drives it, so a fixture can stop at any phase.
    struct StagedLaunch {
        profile: LaunchProfile,
        origin: OriginClue,
        composed: signal_flow::ComposedLaunch,
        pane: HerdrPaneBinding,
        binding: NativeLaunchBinding,
        intent: Option<PromptDeliveryIntent>,
        transcript_root: PathBuf,
    }

    const STAGED_SESSION: &str = "01a0b22c-e24f-7452-9940-64490878680f";

    impl NexusFixture {
        fn staged_launch(
            &self,
            launch_request_id: &str,
            predecessor: Option<&str>,
        ) -> StagedLaunch {
            let source_path = self.directory.path().join("staged-source.md");
            let bundle_path = self.directory.path().join("flow-system-prompt.md");
            fs::write(&source_path, b"staged exact bytes\n").expect("fixture source");
            fs::write(&bundle_path, b"fixture bundle\n").expect("fixture bundle");
            let profile = LaunchProfile {
                launch_request_id: launch_request_id.into(),
                launch_source_vector: vec![LaunchSource {
                    source_path: "staged-source.md".into(),
                    source_sha256: format!("{:x}", Sha256::digest(b"staged exact bytes\n")),
                }],
                skill_name_vector: Vec::new(),
                flow_aspect: FlowAspect::Field,
                power_level: PowerLevel::High,
                harness_kind: HarnessKind::Codex,
                model_name: "fixture-model".into(),
                effort: "medium".into(),
                flow_id_option: predecessor.map(str::to_owned),
                remembered_flow_vector: Vec::new(),
                herdr_session_name: "fixture-session".into(),
                system_prompt_bundle_file: bundle_path.to_string_lossy().into_owned(),
                instruction_prompt: "fixture instruction".into(),
            };
            let composed = self
                .nexus
                .composer
                .compose(&profile)
                .expect("launch composes");
            let pane = HerdrPaneBinding {
                launch_request_id: launch_request_id.into(),
                herdr_session_name: "fixture-session".into(),
                herdr_agent_name: "fixture-agent".into(),
                herdr_workspace_id: "fixture-workspace".into(),
                herdr_pane_id: "w1:p1".into(),
                herdr_terminal_id: "fixture-terminal".into(),
            };
            let transcript_root = self.directory.path().join("native-transcripts/codex");
            fs::create_dir_all(&transcript_root).expect("transcript root");
            StagedLaunch {
                binding: NativeLaunchBinding {
                    launch_request_id: launch_request_id.into(),
                    flow_id: "908786".into(),
                    native_session_id: STAGED_SESSION.into(),
                    harness_kind: HarnessKind::Codex,
                    herdr_pane_binding: pane.clone(),
                },
                profile,
                origin: OriginClue {
                    flow_id: "caller".into(),
                    session_id: "caller-session".into(),
                    turn_id: "caller-turn".into(),
                },
                composed,
                pane,
                intent: None,
                transcript_root,
            }
        }

        /// A Herdr stand-in that knows the successor's agent, shows the
        /// predecessor's pane in every snapshot, and logs every call.
        fn herdr_for_replacement(&self, close_status: i32) -> PathBuf {
            self.herdr_for_replacement_showing(close_status, vec![self.current_agent()])
        }

        /// The same stand-in after the predecessor's pane has exited: no
        /// snapshot shows it.
        fn herdr_after_predecessor_exit(&self, close_status: i32) -> PathBuf {
            self.herdr_for_replacement_showing(close_status, Vec::new())
        }

        /// The replacement stand-in with the predecessor's Herdr session
        /// unreachable: its snapshot fails, while the successor's session
        /// still answers. Any close is logged.
        fn herdr_unreachable_for_predecessor(&self) -> PathBuf {
            let log = self.herdr_for_replacement(0);
            let body = fs::read_to_string(&self.snapshot_program).expect("Herdr fixture");
            let body = body.replacen(
                "case \"$*\" in\n",
                "case \"$*\" in\n  \"--session messaging-build api snapshot\") echo 'herdr: server unreachable' >&2; exit 1 ;;\n",
                1,
            );
            FixtureExecutable {
                path: self.snapshot_program.clone(),
            }
            .install(&body);
            log
        }

        fn herdr_for_replacement_showing(
            &self,
            close_status: i32,
            agents: Vec<serde_json::Value>,
        ) -> PathBuf {
            let log = self.directory.path().join("herdr-calls.log");
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
                    "value":STAGED_SESSION
                }
            }}});
            let snapshot = serde_json::json!({
                "id":"cli:api:snapshot",
                "result":{"snapshot":{"agents":agents,"protocol":20,"version":"0.8.2"},
                "type":"session_snapshot"}
            });
            let body = format!(
                "#!/bin/sh\nprintf '%s\\n' \"$*\" >> '{}'\ncase \"$*\" in\n  \"--session fixture-session agent get w1:p1\") printf '%s\\n' '{}' ;;\n  *\" api snapshot\") printf '%s\\n' '{}' ;;\n  *\" pane close \"*) exit {} ;;\n  *) exit 64 ;;\nesac\n",
                log.display(),
                agent,
                snapshot,
                close_status
            );
            FixtureExecutable {
                path: self.snapshot_program.clone(),
            }
            .install(&body);
            log
        }

        /// The predecessor: the fixture's Herdr pane w1:p3, Active.
        fn register_predecessor(&self, flow_id: &str) {
            fs::write(
                self.directory
                    .path()
                    .join(format!("flows/.{flow_id}.flow-id")),
                format!(
                    "version=1\nharness=codex\nidentity=02b0c33df35f8563aa5175501989791a\nalias={flow_id}\n"
                ),
            )
            .expect("predecessor flow claim");
            let mut node = self.node();
            node.flow_id = flow_id.into();
            assert!(matches!(
                self.nexus.store.register_flow(node),
                Ok(crate::store::FlowRegistration::Registered(_))
            ));
            assert!(self.nexus.store.record_active(flow_id).unwrap());
        }

        fn routable(&self, flow_id: &str) -> bool {
            matches!(
                self.nexus.dispatch(Query::ResolveRecipient(flow_id.into())),
                Response::RecipientResolved(_)
            )
        }

        fn lifecycle(&self, flow_id: &str) -> FlowLifecycle {
            let Response::Listed(rows) = self
                .nexus
                .dispatch(Query::List(signal_flow::ListRequest {}))
            else {
                panic!("List answers")
            };
            rows.into_iter()
                .find(|row| row.flow_id == flow_id)
                .expect("listed flow")
                .flow_lifecycle
        }
    }

    impl StagedLaunch {
        fn request(&self) -> StartRequest {
            StartRequest {
                launch_profile: self.profile.clone(),
                origin_clue: self.origin.clone(),
            }
        }

        fn reserve(&self, nexus: &RunningNexus) {
            nexus
                .store
                .reserve_launch_attempt(&self.composed, self.origin.clone())
                .expect("reservation persists");
        }

        fn record_native_intent(&self, nexus: &RunningNexus) {
            assert!(
                nexus
                    .store
                    .record_native_launch_intent(NativeLaunchIntent {
                        launch_request_id: self.profile.launch_request_id.clone(),
                        prompt_sha256: self.composed.first_prompt_payload.prompt_sha256.clone(),
                        harness_kind: HarnessKind::Codex,
                        model_name: self.profile.model_name.clone(),
                        effort: self.profile.effort.clone(),
                        skill_name_vector: Vec::new(),
                    })
                    .unwrap()
            );
        }

        fn bind(&self, nexus: &RunningNexus) {
            assert!(
                nexus
                    .store
                    .record_native_launch_binding(self.binding.clone())
                    .unwrap()
            );
        }

        fn register_and_acknowledge(&self, nexus: &RunningNexus) {
            nexus
                .store
                .register_flow(FlowNode {
                    flow_id: self.binding.flow_id.clone(),
                    session_id: self.binding.native_session_id.clone(),
                    harness_kind: HarnessKind::Codex,
                    endpoint_selection: EndpointSelection::Unavailable,
                    herdr_route_selection: HerdrRouteSelection::Available(HerdrRoute {
                        herdr_session_name: self.pane.herdr_session_name.clone(),
                        herdr_agent_name: self.pane.herdr_agent_name.clone(),
                        herdr_pane_id: self.pane.herdr_pane_id.clone(),
                        herdr_terminal_id: self.pane.herdr_terminal_id.clone(),
                    }),
                    origin_clue: self.origin.clone(),
                    flow_lifecycle: FlowLifecycle::Pending,
                })
                .unwrap();
            assert!(
                nexus
                    .store
                    .record_registration_acknowledgement(RegistrationAcknowledgement {
                        launch_request_id: self.profile.launch_request_id.clone(),
                        flow_id: self.binding.flow_id.clone(),
                        native_session_id: self.binding.native_session_id.clone(),
                        herdr_pane_binding: self.pane.clone(),
                    })
                    .unwrap()
            );
        }

        fn record_prompt_intent(&mut self, nexus: &RunningNexus) {
            let root_metadata = fs::metadata(&self.transcript_root).unwrap();
            let intent = PromptDeliveryIntent {
                launch_request_id: self.profile.launch_request_id.clone(),
                prompt_sha256: self.composed.first_prompt_payload.prompt_sha256.clone(),
                flow_id: self.binding.flow_id.clone(),
                native_session_id: self.binding.native_session_id.clone(),
                harness_kind: HarnessKind::Codex,
                model_name: self.profile.model_name.clone(),
                effort: self.profile.effort.clone(),
                native_skill_selection_vector: Vec::new(),
                herdr_pane_binding: self.pane.clone(),
                native_transcript_boundary: NativeTranscriptBoundary::Absent(
                    NativeTranscriptAbsence {
                        native_session_id: self.binding.native_session_id.clone(),
                        harness_kind: HarnessKind::Codex,
                        transcript_root_device: root_metadata.dev().to_string(),
                        transcript_root_inode: root_metadata.ino().to_string(),
                    },
                ),
            };
            assert!(
                nexus
                    .store
                    .record_prompt_delivery_intent(intent.clone())
                    .unwrap()
            );
            self.intent = Some(intent);
        }

        fn record_ambiguity(&self, nexus: &RunningNexus) {
            assert!(
                nexus
                    .store
                    .record_prompt_delivery_result(PromptDeliveryResult::Ambiguous(
                        self.intent.clone().expect("prompt intent recorded"),
                    ))
                    .unwrap()
            );
        }

        fn stage_to_ambiguity(&mut self, nexus: &RunningNexus) {
            self.reserve(nexus);
            self.record_native_intent(nexus);
            self.bind(nexus);
            self.register_and_acknowledge(nexus);
            self.record_prompt_intent(nexus);
            self.record_ambiguity(nexus);
        }

        /// The harness writes the first turn and the launch receipt.
        fn write_receipt(&self) {
            let marker = crate::composition::LaunchReceipt::MARKER;
            let mut rows = String::new();
            for row in [
                serde_json::json!({"type":"turn_context","payload":{"model":"fixture-model","effort":"medium","turn_id":"turn-staged"}}),
                serde_json::json!({"type":"event_msg","payload":{"thread_id":STAGED_SESSION,"turn_id":"turn-staged","item":{"type":"UserMessage","content":[{"type":"text","text":self.composed.first_prompt_payload.first_prompt_text}]}}}),
                serde_json::json!({"type":"event_msg","payload":{"thread_id":STAGED_SESSION,"turn_id":"turn-staged","item":{"type":"AgentMessage","content":[{"type":"Text","text":marker}]}}}),
            ] {
                rows.push_str(&format!("{row}\n"));
            }
            fs::write(
                self.transcript_root
                    .join(format!("rollout-{STAGED_SESSION}.jsonl")),
                rows,
            )
            .expect("transcript written");
        }
    }

    #[test]
    fn replace_stops_the_predecessor_before_the_successor_is_routable() {
        let fixture = NexusFixture::new();
        let calls = fixture.herdr_for_replacement(0);
        fixture.register_predecessor("fac697");
        let mut launch = fixture.staged_launch("replace-request", Some("fac697"));
        launch.stage_to_ambiguity(&fixture.nexus);
        assert!(fixture.routable("908786"), "a plain pending launch routes");

        // The successor's first turn is not seen yet: the replacement waits,
        // and the successor is held out of routing while the predecessor
        // still receives.
        assert!(matches!(
            fixture.nexus.dispatch(Query::Replace(launch.request())),
            Response::StartAmbiguous(_)
        ));
        assert!(fixture.routable("fac697"));
        assert!(!fixture.routable("908786"));
        assert_eq!(
            fixture
                .nexus
                .dispatch(Query::Send(signal_flow::SendRequest {
                    flow_id: "908786".into(),
                    bare_input: "too early".into(),
                })),
            Response::SendRejected(signal_flow::SendRejection::RouteUnavailable)
        );
        assert!(matches!(
            fixture.nexus.dispatch(Query::LaunchStatus("replace-request".into())),
            Response::LaunchPending(attempt)
                if attempt.launch_attempt_phase == LaunchAttemptPhase::PromptAmbiguous
        ));

        launch.write_receipt();
        // A stand-in for the successor's per-launch copy: a Started Flow
        // keeps it, since its harness was told to read it.
        let successor_copy = fixture
            .nexus
            .composer
            .launch_bundles()
            .file_for_request("replace-request");
        fs::create_dir_all(successor_copy.parent().unwrap()).unwrap();
        fs::write(&successor_copy, b"successor copy\n").unwrap();
        let Response::Replaced(replaced) = fixture.nexus.dispatch(Query::Replace(launch.request()))
        else {
            panic!("the observed successor replaces its predecessor")
        };
        assert!(successor_copy.exists(), "a Started Flow keeps its copy");
        assert_eq!(replaced.flow_id, "fac697");
        assert_eq!(replaced.started.flow_id, "908786");
        assert_eq!(replaced.started.session_id, STAGED_SESSION);

        assert_eq!(fixture.lifecycle("fac697"), FlowLifecycle::Stopped);
        assert_eq!(fixture.lifecycle("908786"), FlowLifecycle::Active);
        assert_eq!(
            fixture
                .nexus
                .dispatch(Query::ResolveRecipient("fac697".into())),
            Response::RecipientResolutionRejected(
                signal_flow::RecipientResolutionRejection::FlowUnavailable
            )
        );
        assert!(fixture.routable("908786"));
        assert_eq!(
            fixture
                .nexus
                .dispatch(Query::Send(signal_flow::SendRequest {
                    flow_id: "fac697".into(),
                    bare_input: "to the old seat".into(),
                })),
            Response::SendRejected(signal_flow::SendRejection::FlowStopped)
        );
        assert_eq!(
            fixture
                .nexus
                .dispatch(Query::LaunchStatus("replace-request".into())),
            Response::Replaced(replaced.clone())
        );
        // A repeated Replace answers the settled outcome and closes nothing.
        assert_eq!(
            fixture.nexus.dispatch(Query::Replace(launch.request())),
            Response::Replaced(replaced)
        );
        let calls = fs::read_to_string(calls).expect("Herdr calls");
        assert_eq!(
            calls
                .matches("--session messaging-build pane close w1:p3")
                .count(),
            1,
            "{calls}"
        );
        assert!(!calls.contains("agent prompt"), "{calls}");
    }

    #[test]
    fn a_refused_reap_leaves_neither_flow_routable_until_it_is_retried() {
        let fixture = NexusFixture::new();
        fixture.herdr_for_replacement(1);
        fixture.register_predecessor("fac697");
        let mut launch = fixture.staged_launch("reap-request", Some("fac697"));
        launch.stage_to_ambiguity(&fixture.nexus);
        launch.write_receipt();

        let refused = Response::ReplaceRejected(signal_flow::ReplaceRejection::ReapRefused(
            signal_flow::StopRejection::CloseRefused,
        ));
        assert_eq!(
            fixture.nexus.dispatch(Query::Replace(launch.request())),
            refused
        );
        assert_eq!(fixture.lifecycle("fac697"), FlowLifecycle::Stopped);
        assert!(!fixture.routable("fac697"));
        assert!(!fixture.routable("908786"));
        assert_eq!(
            fixture
                .nexus
                .dispatch(Query::LaunchStatus("reap-request".into())),
            refused
        );

        fixture.herdr_for_replacement(0);
        assert!(matches!(
            fixture.nexus.dispatch(Query::Replace(launch.request())),
            Response::Replaced(replaced) if replaced.flow_id == "fac697"
        ));
        assert!(!fixture.routable("fac697"));
        assert!(fixture.routable("908786"));
    }

    #[test]
    fn an_unreachable_herdr_refuses_the_reap_instead_of_counting_it_done() {
        let fixture = NexusFixture::new();
        fixture.herdr_for_replacement(0);
        fixture.register_predecessor("fac697");
        let mut launch = fixture.staged_launch("unreachable-request", Some("fac697"));
        launch.stage_to_ambiguity(&fixture.nexus);
        launch.write_receipt();

        let calls = fixture.herdr_unreachable_for_predecessor();
        let refused = Response::ReplaceRejected(signal_flow::ReplaceRejection::ReapRefused(
            signal_flow::StopRejection::RouteUnavailable,
        ));
        assert_eq!(
            fixture.nexus.dispatch(Query::Replace(launch.request())),
            refused,
            "a failed snapshot is not an absent pane"
        );
        let logged = fs::read_to_string(&calls).expect("Herdr call log");
        assert!(logged.contains("api snapshot"), "{logged}");
        assert!(!logged.contains("pane close"), "{logged}");
        assert_eq!(fixture.lifecycle("fac697"), FlowLifecycle::Stopped);
        assert!(!fixture.routable("908786"));
        assert_eq!(
            fixture
                .nexus
                .dispatch(Query::LaunchStatus("unreachable-request".into())),
            refused
        );

        // Herdr answers again and shows the pane: the retry closes it.
        let calls = fixture.herdr_for_replacement(0);
        assert!(matches!(
            fixture.nexus.dispatch(Query::Replace(launch.request())),
            Response::Replaced(replaced) if replaced.flow_id == "fac697"
        ));
        assert!(
            fs::read_to_string(calls)
                .expect("Herdr call log")
                .contains("pane close")
        );
        assert!(fixture.routable("908786"));
    }

    #[test]
    fn a_predecessor_whose_pane_is_gone_is_already_reaped() {
        let fixture = NexusFixture::new();
        // Any close would fail: the reap must not need one.
        let calls = fixture.herdr_after_predecessor_exit(1);
        fixture.register_predecessor("fac697");
        let mut launch = fixture.staged_launch("exited-request", Some("fac697"));
        launch.stage_to_ambiguity(&fixture.nexus);
        launch.write_receipt();

        let replaced = fixture.nexus.dispatch(Query::Replace(launch.request()));
        assert!(
            matches!(&replaced, Response::Replaced(replaced) if replaced.flow_id == "fac697"),
            "an absent pane is a completed reap, not ReapRefused: {replaced:?}"
        );
        assert_eq!(fixture.lifecycle("fac697"), FlowLifecycle::Stopped);
        assert!(!fixture.routable("fac697"));
        assert!(fixture.routable("908786"));
        assert_eq!(
            fixture
                .nexus
                .dispatch(Query::LaunchStatus("exited-request".into())),
            replaced
        );
        let calls = fs::read_to_string(calls).expect("Herdr call log");
        assert!(
            !calls.contains("pane close"),
            "no close is attempted on an absent pane: {calls}"
        );
    }

    #[test]
    fn replace_refuses_an_absent_unknown_or_stopped_predecessor() {
        let fixture = NexusFixture::new();
        let absent = fixture.staged_launch("absent-request", None);
        assert_eq!(
            fixture.nexus.dispatch(Query::Replace(absent.request())),
            Response::ReplaceRejected(signal_flow::ReplaceRejection::PredecessorAbsent)
        );
        let unknown = fixture.staged_launch("unknown-request", Some("nobody"));
        assert_eq!(
            fixture.nexus.dispatch(Query::Replace(unknown.request())),
            Response::ReplaceRejected(signal_flow::ReplaceRejection::UnknownPredecessor)
        );
        fixture.register_predecessor("fac697");
        assert!(fixture.nexus.store.record_stopped("fac697").unwrap());
        let stopped = fixture.staged_launch("stopped-request", Some("fac697"));
        assert_eq!(
            fixture.nexus.dispatch(Query::Replace(stopped.request())),
            Response::ReplaceRejected(signal_flow::ReplaceRejection::PredecessorStopped)
        );
        for request in ["absent-request", "unknown-request", "stopped-request"] {
            assert_eq!(
                fixture.nexus.dispatch(Query::LaunchStatus(request.into())),
                Response::LaunchStatusRejected(
                    signal_flow::LaunchStatusRejection::UnknownLaunchRequest
                )
            );
        }
        assert!(
            !fixture.snapshot_program.exists(),
            "no launch was attempted"
        );
    }

    #[test]
    fn launch_status_answers_pending_and_every_outcome_once() {
        let fixture = NexusFixture::new();
        assert_eq!(
            fixture
                .nexus
                .dispatch(Query::LaunchStatus("never-sent".into())),
            Response::LaunchStatusRejected(
                signal_flow::LaunchStatusRejection::UnknownLaunchRequest
            )
        );

        // Started.
        fixture.herdr_for_replacement(0);
        let mut started = fixture.staged_launch("started-request", None);
        started.stage_to_ambiguity(&fixture.nexus);
        assert!(matches!(
            fixture.nexus.dispatch(Query::LaunchStatus("started-request".into())),
            Response::LaunchPending(attempt)
                if attempt.launch_attempt_phase == LaunchAttemptPhase::PromptAmbiguous
        ));
        started.write_receipt();
        let Response::Started(outcome) = fixture.nexus.dispatch(Query::Start(started.request()))
        else {
            panic!("the observed launch starts")
        };
        assert_eq!(
            fixture
                .nexus
                .dispatch(Query::LaunchStatus("started-request".into())),
            Response::Started(outcome)
        );

        // StartRejected after reservation: Herdr cannot open the pane.
        fs::remove_file(&fixture.snapshot_program).expect("Herdr withdrawn");
        let mut rejected = fixture.staged_launch("rejected-request", None);
        rejected.profile.harness_kind = HarnessKind::Claude;
        let rejection = fixture.nexus.dispatch(Query::Start(rejected.request()));
        assert_eq!(
            rejection,
            Response::StartRejected(StartRejection::NativeLaunchRefused)
        );
        assert_eq!(
            fixture
                .nexus
                .dispatch(Query::LaunchStatus("rejected-request".into())),
            rejection
        );
        assert_eq!(
            fixture.nexus.dispatch(Query::Start(rejected.request())),
            rejection,
            "a settled rejection is not launched again"
        );
        // The refused Claude launch wrote its copy at compose time; with the
        // rejection stored, the copy is gone.
        let bundles = fixture.nexus.composer.launch_bundles().clone();
        assert!(fixture.directory.path().join("launch-bundles").is_dir());
        assert!(!bundles.file_for_request("rejected-request").exists());

        // ReplaceRejected: the successor's launch is refused; the
        // predecessor keeps receiving.
        fixture.register_predecessor("fac697");
        let mut refused = fixture.staged_launch("refused-request", Some("fac697"));
        refused.profile.harness_kind = HarnessKind::Claude;
        let rejection = fixture.nexus.dispatch(Query::Replace(refused.request()));
        assert_eq!(
            rejection,
            Response::ReplaceRejected(signal_flow::ReplaceRejection::LaunchRefused(
                StartRejection::NativeLaunchRefused
            ))
        );
        assert_eq!(
            fixture
                .nexus
                .dispatch(Query::LaunchStatus("refused-request".into())),
            rejection
        );
        assert_eq!(fixture.lifecycle("fac697"), FlowLifecycle::Active);
        assert!(!bundles.file_for_request("refused-request").exists());
        // Replaced is answered by the replacement fixture above.
    }

    /// 88475f: Start answered StartAmbiguous, the receipt landed half a
    /// minute later, and nobody subscribed or sent Start again. The
    /// Nexus's own watch promotes it.
    #[test]
    fn a_receipt_after_ambiguity_promotes_with_no_subscriber_and_no_second_start() {
        use crate::launching::{LaunchesFlows, PromotesAmbiguousLaunches};
        let fixture: &'static NexusFixture = Box::leak(Box::new(NexusFixture::new()));
        let calls = fixture.herdr_for_replacement(0);
        let mut launch = fixture.staged_launch("unwatched-request", None);
        launch.reserve(&fixture.nexus);
        launch.record_native_intent(&fixture.nexus);
        launch.bind(&fixture.nexus);
        launch.register_and_acknowledge(&fixture.nexus);
        launch.record_prompt_intent(&fixture.nexus);
        launch.record_ambiguity(&fixture.nexus);
        std::thread::spawn(move || fixture.nexus.promote_ambiguous_launches());
        let status = || fixture.nexus.launch_status("unwatched-request");
        // The promoter has looked once and found no receipt.
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while fs::read_to_string(&calls)
            .unwrap_or_default()
            .lines()
            .count()
            == 0
        {
            assert!(
                std::time::Instant::now() < deadline,
                "promoter never looked"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
        assert!(matches!(
            status(),
            Response::LaunchPending(attempt)
                if attempt.launch_attempt_phase == LaunchAttemptPhase::PromptAmbiguous
        ));

        // Only the transcript moves.
        launch.write_receipt();
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        let started = loop {
            if let Response::Started(started) = status() {
                break started;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "receipt never promoted: {:?}",
                status()
            );
            std::thread::sleep(Duration::from_millis(20));
        };
        assert_eq!(started.flow_id, "908786");
        let calls = fs::read_to_string(calls).unwrap_or_default();
        assert!(!calls.contains("agent prompt"), "{calls}");
        assert!(!calls.contains("pane create"), "{calls}");
    }

    #[test]
    fn a_launch_observer_receives_each_phase_once_and_the_outcome_last() {
        use super::{Frame, ServesOrdinary};
        use std::os::unix::net::UnixStream;
        let fixture: &'static NexusFixture = Box::leak(Box::new(NexusFixture::new()));
        let calls = fixture.herdr_for_replacement(0);
        let socket = fixture.directory.path().join("observe.sock");
        let served = socket.clone();
        std::thread::spawn(move || fixture.nexus.serve_ordinary(&served));
        let subscribe = || {
            for _ in 0..200 {
                if let Ok(mut peer) = UnixStream::connect(&socket) {
                    peer.set_read_timeout(Some(Duration::from_secs(10)))
                        .expect("observer timeout");
                    Frame::write_query(
                        &mut peer,
                        &Query::Observe(signal_flow::ObserveSelection::Launch(
                            "observed-request".into(),
                        )),
                    )
                    .expect("Observe written");
                    return peer;
                }
                std::thread::sleep(Duration::from_millis(10));
            }
            panic!("ordinary socket never listened")
        };
        let phase = |peer: &mut UnixStream| match Frame::read_response(peer) {
            Ok(Response::LaunchPending(attempt)) => attempt.launch_attempt_phase,
            other => panic!("expected a LaunchPending frame, got {other:?}"),
        };

        let mut launch = fixture.staged_launch("observed-request", None);
        launch.reserve(&fixture.nexus);
        let mut observer = subscribe();
        assert_eq!(phase(&mut observer), LaunchAttemptPhase::Reserved);
        launch.record_native_intent(&fixture.nexus);
        assert_eq!(
            phase(&mut observer),
            LaunchAttemptPhase::NativeLaunchIntentRecorded
        );
        launch.bind(&fixture.nexus);
        assert_eq!(phase(&mut observer), LaunchAttemptPhase::NativeBound);
        launch.register_and_acknowledge(&fixture.nexus);
        assert_eq!(
            phase(&mut observer),
            LaunchAttemptPhase::RegistrationAcknowledged
        );
        launch.record_prompt_intent(&fixture.nexus);
        assert_eq!(
            phase(&mut observer),
            LaunchAttemptPhase::PromptIntentRecorded
        );
        launch.record_ambiguity(&fixture.nexus);
        assert_eq!(phase(&mut observer), LaunchAttemptPhase::PromptAmbiguous);

        // Only the transcript moves; no one sends Start again.
        launch.write_receipt();
        let Ok(Response::Started(started)) = Frame::read_response(&mut observer) else {
            panic!("the observer's last frame is the outcome")
        };
        assert_eq!(started.flow_id, "908786");
        assert!(
            Frame::read_response(&mut observer).is_err(),
            "the exchange ends after the outcome"
        );

        let mut late = subscribe();
        assert_eq!(
            Frame::read_response(&mut late).expect("late observer answered"),
            Response::Started(started)
        );
        assert!(Frame::read_response(&mut late).is_err());
        let calls = fs::read_to_string(calls).unwrap_or_default();
        assert!(!calls.contains("agent prompt"), "{calls}");
        assert!(!calls.contains("pane create"), "{calls}");
    }
}
