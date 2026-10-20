//! Flow Nexus dispatches typed ordinary and privileged Signal requests.

use crate::store::ManagesMetaflowState;
use crate::store::ReadsFlowMemory;
// The generated Operation root names the store's own types by this crate's
// name, as `flow_nexus::LaunchOutcome`; the Memory root is not compiled yet.
extern crate self as flow_nexus;
use crate::claude::ProjectsClaudeReadiness;
use crate::codex::SelectsCodexEndpoint;
use crate::herdr::ConfiguresHerdrCli;
use crate::herdr::ReadsHerdrPanes;
use crate::store::AnnouncesLaunchChanges;
use crate::store::AnswersLaunchOutcome;
pub use caller::CallerProcess;
pub use composition::CompositionError;
pub use context::VerifiedContext;
pub use generated::curriculum::{
    ContextProvenance, Outcome as CurriculumOutcome, Query as CurriculumQuery, StartupComposition,
    StartupSelection,
};
pub use generated::operation::Outcome as OperationOutcome;
pub use store::{LaunchOutcome, Replacement};
pub mod binding;
pub mod caller;
pub mod claude;
pub mod codex;
pub mod codex_turns;
pub mod composition;
pub mod context;
pub mod delivery;
pub mod lean;
mod metaflow;
pub mod recovery;
use metaflow::DispatchesMetaflowPeer;
#[cfg(test)]
mod fixture_executable;
/// Generated from the operation and startup Ethos roots; `build.rs` keeps
/// both outputs fresh. The `datom` feature enables their textual CLI surface.
#[allow(unexpected_cfgs, clippy::large_enum_variant)]
pub mod generated;
pub mod herdr;
pub mod launching;
pub mod observe_agent;
pub mod peer;
pub mod performing;
pub mod reporting;
pub mod send_tool;
pub mod store;
pub mod title;
pub mod values;
pub mod waking;

use binding::{
    ChecksFlowBinding, ChecksFlowContainer, ChecksProcessIdentity, RefusesBinding,
    VerifiesExistingBinding,
};
use caller::{IdentifiesPeer, LocatesCallerPane, ResolvesCaller};
use codex::{CodexEndpoints, ConsumesResetCredit};
use composition::{LaunchComposer, OpensLaunchComposer};
use context::{CurriculumComposer, OpensCurriculumComposer};
use delivery::{DeliversMessages, lease::LeasesPanes};
use generated::operation::{Operation, Outcome, Record_Data};
use generated::starter::DerivesOrigin;
use launching::{LaunchesFlows, ObservesLaunch};
use observe_agent::ObservesAgent;
use peer::{AdmitsMetaPeer, NamesMetaPeer, ResolvesPeer};
use performing::Performs;
use reporting::RecordsReports;
use send_tool::ServesLaunchCalls;
use signal_flow::{
    EndpointSelection, FlowLifecycle, FlowNode, HerdrRoute, HerdrRouteSelection, ObserveSelection,
    Origin, Process, Query, Response, RestartRejection,
};
use std::{
    collections::HashSet,
    fs,
    io::{Read, Write},
    os::unix::{
        fs::PermissionsExt,
        net::{UnixListener, UnixStream},
    },
    path::{Path, PathBuf},
    sync::{
        Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use store::{
    AppliesFlowQuery, AuthorizesFlowRestart, FlowStore, NamesLiveFlow, OpensFlowStore,
    ReadsFlowRows, RecordsLaunchOutcome, RegistersExistingFlow, RegistersFlowIdentity,
};
use waking::SettlesStopsAtConfirmation;
use waking::SleepsAndWakesSeats;

pub struct RunningNexus {
    pub store: FlowStore,
    pub codex_endpoints: CodexEndpoints,
    pub herdr: herdr::HerdrCli,
    pub composer: LaunchComposer,
    /// Serializes dispatch; see `dispatch_serially`.
    pub dispatch_gate: Mutex<()>,
    /// The exclusive hold on each pane Flow is writing; see
    /// `delivery::lease`.
    pub pane_leases: delivery::lease::PaneLeases,
    /// The metaflows a wake is running for; see `waking`.
    pub waking: waking::WakingMetaflows,
    /// Flows confirmed after their Stop, awaiting settlement; see `waking`.
    pub stopped_at_confirmation: waking::StoppedAtConfirmation,
    /// Set once the Nexus is told to stop; see [`StopsNexus`].
    pub stopping: Stopping,
    /// The send-tool responder of the Codex launch running now; see
    /// `send_tool::ServesLaunchCalls`.
    pub launch_calls: send_tool::LaunchCalls,
    /// Whether each Codex thread is in a turn, as its app-server's events
    /// show it; see `codex_turns`.
    pub codex_turns: codex_turns::CodexTurns,
}

pub trait Dispatches {
    /// A query no calling process stands behind: its origin is Unbound.
    fn dispatch(&self, query: Query) -> Response {
        self.dispatch_from(query, Origin::Unbound)
    }
    /// A query from `origin`, the calling process as the Nexus derived it.
    fn dispatch_from(&self, query: Query, origin: Origin) -> Response;
    /// A meta query with no peer behind it: one that records who asked is
    /// refused as PeerUnknown.
    fn dispatch_meta(&self, query: meta_signal_flow::Query) -> meta_signal_flow::Response {
        self.dispatch_meta_from(query, None)
    }
    /// A meta query from `resolver`, the meta peer as the Nexus read it.
    fn dispatch_meta_from(
        &self,
        query: meta_signal_flow::Query,
        resolver: Option<meta_signal_flow::ResolverIdentity>,
    ) -> meta_signal_flow::Response;
}

impl Dispatches for RunningNexus {
    fn dispatch_from(&self, query: Query, origin: Origin) -> Response {
        match query {
            Query::Start(profile) => {
                let launch_request_id = profile.launch_request_id.clone();
                if let Some(settled) = self.settled(&profile, &origin) {
                    return settled;
                }
                let response = self.start(profile, origin);
                self.settle(&launch_request_id, response)
            }
            Query::Replace(profile) => self.replace(profile, origin),
            Query::LaunchStatus(launch_request_id) => self.launch_status(&launch_request_id),
            // Over a connection Observe streams; a direct dispatch answers the
            // subscription's opening frame.
            Query::Observe(ObserveSelection::Launch(launch_request_id)) => {
                self.launch_status(&launch_request_id)
            }
            Query::Observe(ObserveSelection::Agent(flow_id)) => self.agent_observation(&flow_id),
            Query::Restart(flow_id) => {
                // The restart is authorized by the flow that asks: its
                // origin is the calling process's bound flow and session.
                let Origin::Flow(caller) = origin else {
                    return Response::RestartRejected(RestartRejection::ProvenanceMismatch);
                };
                let authorization = self.store.authorize_restart(&flow_id, &caller.flow_id);
                let Ok(Some(token)) = authorization else {
                    return Response::RestartRejected(RestartRejection::ProvenanceMismatch);
                };
                if caller.session_id != token.thread_id {
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
                        self.herdr
                            .refresh_route(self.herdr.claude_daemon.refreshed(node)),
                    ),
                    Ok(response) => response,
                    Err(error) => Response::Refused(signal_flow::Refusal::Store(error.to_string())),
                }
            }
            Query::Stop(flow_id) => {
                let node = match self.store.flow_node(&flow_id) {
                    Ok(Some(node)) => node,
                    Ok(None) => {
                        return Response::StopRejected(signal_flow::StopRejection::UnknownFlow);
                    }
                    Err(error) => {
                        return Response::Refused(signal_flow::Refusal::Store(error.to_string()));
                    }
                };
                // A Retired seat is gone already, so there is nothing left
                // for Stop to close: AlreadyStopped is what it answers.
                if !node.flow_lifecycle.is_live() {
                    return Response::StopRejected(signal_flow::StopRejection::AlreadyStopped);
                }
                // The route is the flow's recorded Herdr pane: a Pending flow
                // whose launch was refused, or a seat not ready for a prompt,
                // is stopped when Herdr still shows its binding there.
                let HerdrRouteSelection::Available(route) = &node.herdr_route_selection else {
                    return Response::StopRejected(signal_flow::StopRejection::RouteUnavailable);
                };
                if self.herdr.pane_presence(&node) != herdr::PanePresence::Present {
                    return Response::StopRejected(signal_flow::StopRejection::RouteUnavailable);
                }
                // No write is left half-typed into a pane as it closes.
                let closed = {
                    let _lease = self.pane_leases.hold(route);
                    self.perform(Operation::Close(node.clone()))
                };
                if closed != Outcome::Closed {
                    return Response::StopRejected(signal_flow::StopRejection::CloseRefused);
                }
                if self.perform(Operation::Record(Record_Data::Stopped(flow_id.clone())))
                    != Outcome::Recorded
                {
                    return Response::StopRejected(signal_flow::StopRejection::PersistenceRefused);
                }
                Response::Stopped(flow_id)
            }
            // A dispatch without a connection has no peer to read.
            Query::ResolveCaller(claim) => self.resolve_caller(None, claim),
            Query::Report(report) => self.report(report),
            Query::Current(address) => {
                use crate::waking::RestsEndedAwake;
                self.rest_ended_awake(&address);
                self.store
                    .current_metaflow(&address)
                    .unwrap_or_else(|error| {
                        Response::Refused(signal_flow::Refusal::Store(error.to_string()))
                    })
            }
            Query::Bind(_)
            | Query::Identify(_)
            | Query::IdentifyThread(_)
            | Query::Lock(_)
            | Query::Deliver(_)
            | Query::Release(_) => Response::Refused(signal_flow::Refusal::NotMessage),
            Query::Metaflows => self.store.list_metaflows().map_or_else(
                |error| Response::Refused(signal_flow::Refusal::Store(error.to_string())),
                Response::Listed,
            ),
        }
    }

    fn dispatch_meta_from(
        &self,
        query: meta_signal_flow::Query,
        resolver: Option<meta_signal_flow::ResolverIdentity>,
    ) -> meta_signal_flow::Response {
        match query {
            meta_signal_flow::Query::Configuration => match self.store.configuration_snapshot() {
                Ok(Some(snapshot)) => meta_signal_flow::Response::Configuration(snapshot),
                Ok(None) => meta_signal_flow::Response::Unconfigured,
                Err(error) => meta_signal_flow::Response::Refused(
                    meta_signal_flow::Refusal::Store(error.to_string()),
                ),
            },
            // A declared family replaces the committed family whole; this
            // Nexus keeps the Codex endpoints it opened with until its next
            // start.
            meta_signal_flow::Query::Configure(change) => {
                match self.store.configure_change(change) {
                    Ok(outcome) => meta_signal_flow::Response::Configured(outcome),
                    Err(store::StoreError::DuplicateConfigurationEntry) => {
                        meta_signal_flow::Response::ConfigureRejected(
                            meta_signal_flow::ConfigureRejection::DuplicateEntry,
                        )
                    }
                    Err(store::StoreError::CodexModelsOverlap) => {
                        meta_signal_flow::Response::ConfigureRejected(
                            meta_signal_flow::ConfigureRejection::CodexModelsOverlap,
                        )
                    }
                    Err(store::StoreError::LeaveOutNotForHarness) => {
                        meta_signal_flow::Response::ConfigureRejected(
                            meta_signal_flow::ConfigureRejection::LeaveOutNotForHarness,
                        )
                    }
                    Err(error) => meta_signal_flow::Response::Refused(
                        meta_signal_flow::Refusal::Store(error.to_string()),
                    ),
                }
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
            // A seat Flow lost. Retire keeps the row, its origin and its
            // history, and takes the flow out of receiving: it is not Stop,
            // which closes a pane Flow still holds, and it is not a delete.
            // Privileged, because it changes what Flow holds of a flow
            // without that flow acting.
            meta_signal_flow::Query::Retire(flow_id) => {
                let node = match self.store.flow_node(&flow_id) {
                    Ok(Some(node)) => node,
                    Ok(None) => {
                        return meta_signal_flow::Response::RetireRejected(
                            meta_signal_flow::RetireRejection::UnknownFlow,
                        );
                    }
                    Err(_) => {
                        return meta_signal_flow::Response::RetireRejected(
                            meta_signal_flow::RetireRejection::StoreRefused,
                        );
                    }
                };
                if !node.flow_lifecycle.is_live() {
                    return meta_signal_flow::Response::RetireRejected(
                        meta_signal_flow::RetireRejection::AlreadyGone,
                    );
                }
                if self.perform(Operation::Record(Record_Data::Retired(flow_id.clone())))
                    != Outcome::Recorded
                {
                    return meta_signal_flow::Response::RetireRejected(
                        meta_signal_flow::RetireRejection::StoreRefused,
                    );
                }
                match self.store.flow_node(&flow_id) {
                    Ok(Some(retired)) => meta_signal_flow::Response::FlowRetired(retired),
                    Ok(None) | Err(_) => meta_signal_flow::Response::RetireRejected(
                        meta_signal_flow::RetireRejection::StoreRefused,
                    ),
                }
            }
            meta_signal_flow::Query::MetaBindExisting(request) => {
                let container = request.flow_container;
                if !container.is_well_formed() || !container.socket_is_live() {
                    return meta_signal_flow::Response::BindExistingRejected(
                        meta_signal_flow::BindExistingRejection::ContainerUnavailable,
                    );
                }
                if !container.herdr_server_process_identity.is_live_process() {
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
                        results.push(
                            meta_signal_flow::FlowBindingRefusalReason::DuplicateFlowId
                                .refusal_of(flow_id),
                        );
                        continue;
                    }
                    if !binding.is_well_formed() {
                        results.push(
                            meta_signal_flow::FlowBindingRefusalReason::AnatomyMismatch
                                .refusal_of(flow_id),
                        );
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
                        results.push(
                            meta_signal_flow::FlowBindingRefusalReason::AmbiguousPane
                                .refusal_of(flow_id),
                        );
                        continue;
                    }
                    if !binding.process_identity.is_live_process() {
                        results.push(
                            meta_signal_flow::FlowBindingRefusalReason::DeadProcess
                                .refusal_of(flow_id),
                        );
                        continue;
                    }
                    if !binding
                        .process_identity
                        .works_in(&binding.working_directory)
                    {
                        results.push(
                            meta_signal_flow::FlowBindingRefusalReason::AnatomyMismatch
                                .refusal_of(flow_id),
                        );
                        continue;
                    }
                    if let Err(reason) = self
                        .herdr
                        .verify_existing_binding(&container.herdr_session_name, &binding)
                    {
                        results.push(reason.refusal_of(flow_id));
                        continue;
                    }
                    // A flow already in the store is not imported again: the
                    // store asserts the binding's role only when the stored
                    // flow is this binding, and refuses any other.
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
                        // A flow bound after the fact has no launch that asked
                        // for it.
                        origin: Origin::Unbound,
                        flow_lifecycle: FlowLifecycle::Pending,
                    };
                    let role = signal_flow::Caller {
                        flow_id: flow_id.clone(),
                        aspect: binding.aspect,
                        power_level: binding.power_level,
                        model_name: binding.model_name,
                    };
                    match self.store.register_existing_flow(node, role) {
                        Ok(store::FlowRegistration::Registered(_)) => {
                            results.push(meta_signal_flow::FlowBindingResult::Bound(
                                meta_signal_flow::BoundFlowBinding {
                                    flow_id,
                                    flow_lifecycle:
                                        meta_signal_flow::FlowLifecycle::RegisteredUnconfirmed,
                                },
                            ))
                        }
                        Ok(store::FlowRegistration::ConflictingBinding) => results.push(
                            meta_signal_flow::FlowBindingRefusalReason::DuplicateFlowId
                                .refusal_of(flow_id),
                        ),
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
            meta_signal_flow::Query::Deliver(request) => self.deliver(request),
            meta_signal_flow::Query::Vet(request) => self.vet(&request),
            meta_signal_flow::Query::Command(request) => self.command(request),
            meta_signal_flow::Query::ResolvePeer(identity) => self.resolve_peer(&identity),
            meta_signal_flow::Query::ReadEvents(flow_id) => self.read_events(flow_id),
            meta_signal_flow::Query::ReadQueueDeliveries(address) => {
                self.read_queue_deliveries(address)
            }
            meta_signal_flow::Query::ResolveQueueDelivery(resolution) => match resolver {
                Some(resolver) => self.resolve_queue_delivery(resolution, resolver),
                None => meta_signal_flow::Response::MetaRefused(
                    meta_signal_flow::MetaRefusal::PeerUnknown,
                ),
            },
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
    /// Opens the configured Nexus from one typed Start payload. Durable
    /// runner values come from the committed Meta configuration snapshot;
    /// socket paths come only from this Start value.
    fn open(
        defaults: &store::DefaultConfiguration,
        start: &flow_ethos::Start,
    ) -> Result<Self, store::StoreError>
    where
        Self: Sized;
}

impl OpensRunningNexus for RunningNexus {
    fn open(
        _defaults: &store::DefaultConfiguration,
        start: &flow_ethos::Start,
    ) -> Result<Self, store::StoreError> {
        let store_path = Path::new(&start.store_path);
        // The configured opener checks the typed store path read-only first,
        // reads the committed snapshot, and only then registers runtime tables.
        // It never seeds defaults or settles work; it drops the retired
        // tables and upgrades the flows and outcomes from their 0.34 shape.
        let store = FlowStore::open_configured(store_path)?;
        let snapshot = store
            .configuration_snapshot()?
            .ok_or(store::StoreError::NotConfigured)?;
        let runtime = store::RuntimeConfiguration::from(&snapshot.nexus);
        let codex_endpoints = CodexEndpoints::from(&runtime);
        let overlap = codex_endpoints.overlapping_models();
        if !overlap.is_empty() {
            eprintln!(
                "flow-nexus: models selected by both Codex endpoints are unavailable: {overlap:?}"
            );
        }
        let source_root = PathBuf::from(&runtime.source_root);
        // The socket this Nexus serves: a changed one takes effect only on
        // restart, so the store's value at open is the one being served.
        let ordinary_socket = PathBuf::from(&start.ordinary_socket_path);
        Ok(Self {
            store,
            codex_endpoints: codex_endpoints.clone(),
            herdr: herdr::HerdrCli::default()
                .with_source_root(&source_root)
                .with_codex_endpoints(codex_endpoints)
                .with_ordinary_socket(&ordinary_socket),
            // A launch's context is composed by the Curriculum Nexus.
            composer: LaunchComposer::with_context(context::ContextComposer::Curriculum(
                CurriculumComposer::from_environment(),
            )),
            dispatch_gate: Mutex::new(()),
            pane_leases: delivery::lease::PaneLeases::default(),
            waking: waking::WakingMetaflows::default(),
            stopped_at_confirmation: waking::StoppedAtConfirmation::default(),
            stopping: Stopping::default(),
            launch_calls: send_tool::LaunchCalls::default(),
            codex_turns: codex_turns::CodexTurns::default(),
        })
    }
}

/// One accepted connection: read one frame, answer it, close — except
/// Observe, which keeps the connection as its subscription until the
/// outcome frame. A malformed frame, an idle peer past the read timeout, or
/// a failed write drops only this connection.
pub struct Connection {
    peer: UnixStream,
    /// PID and kernel start token captured when this socket was accepted.
    /// Privileged ordinary operations recheck both before using the peer.
    process: Option<Process>,
}

pub trait ServesConnection: Sized {
    const READ_TIMEOUT: Duration = Duration::from_secs(5);
    const WRITE_TIMEOUT: Duration = Duration::from_secs(5);
    fn accepted(peer: UnixStream) -> Result<Self, String>;
    fn serve_ordinary(self, nexus: &RunningNexus) -> Result<(), String>;
    fn serve_meta(self, nexus: &RunningNexus) -> Result<(), String>;
}

impl ServesConnection for Connection {
    fn accepted(peer: UnixStream) -> Result<Self, String> {
        peer.set_read_timeout(Some(Self::READ_TIMEOUT))
            .and_then(|_| peer.set_write_timeout(Some(Self::WRITE_TIMEOUT)))
            .map_err(|error| error.to_string())?;
        let process = peer
            .peer_process()
            .and_then(|peer| FlowStore::process_identity_for_pid(peer.process_id));
        Ok(Self { peer, process })
    }

    fn serve_ordinary(mut self, nexus: &RunningNexus) -> Result<(), String> {
        let query = self.peer.read_query()?;
        let response = match query {
            // Reads of launch state never wait behind a running launch.
            Query::LaunchStatus(launch_request_id) => nexus.launch_status(&launch_request_id),
            Query::Observe(ObserveSelection::Launch(launch_request_id)) => {
                let peer = &mut self.peer;
                return nexus.observe_launch(&launch_request_id, &mut |response| {
                    peer.write_response(response)
                });
            }
            Query::Observe(ObserveSelection::Agent(flow_id)) => {
                let peer = &mut self.peer;
                return nexus
                    .observe_agent(&flow_id, &mut |response| peer.write_response(response));
            }
            // A hook's Report never waits behind a running launch: the
            // launch may itself be waiting on the harness whose hook this is.
            // What the report sets going runs after the hook has its answer
            // and the connection is closed: the harness waits on its hook,
            // and the settling waits on the harness.
            Query::Report(report) => {
                let (flow_id, event) = (report.flow_id.clone(), report.event.clone());
                let response = nexus.report(report);
                self.peer.write_response(&response)?;
                drop(self);
                if response == Response::Reported {
                    nexus.after_report(&flow_id, &event);
                }
                return Ok(());
            }
            // The caller is the peer of this connection, read from the kernel.
            Query::ResolveCaller(claim) => nexus.resolve_caller(
                self.peer
                    .peer_process()
                    .and_then(|process| process.caller_pane(&nexus.herdr)),
                claim,
            ),
            query @ (Query::Bind(_)
            | Query::Identify(_)
            | Query::IdentifyThread(_)
            | Query::Lock(_)
            | Query::Deliver(_)
            | Query::Release(_)) => {
                // A queued request is durable: the recipient's wake is read
                // from Flow's records after the answer, whether or not the
                // answer reached the sender, off the dispatch gate.
                let answer = nexus.dispatch_metaflow_peer(query, self.process.as_ref());
                let written = self.peer.write_response(&answer.response);
                drop(self);
                if let Some(address) = answer.wake {
                    nexus.wake_due(address);
                }
                return written;
            }
            // A launch's origin is the calling process, as Identify reads
            // it: its pane's bound flow and that flow's native session.
            query @ (Query::Start(_) | Query::Replace(_) | Query::Restart(_)) => {
                let origin = nexus.origin_of(self.peer.peer_process());
                nexus.dispatch_serially(query, origin)
            }
            query => nexus.dispatch_serially(query, Origin::Unbound),
        };
        self.peer.write_response(&response)?;
        drop(self);
        nexus.settle_stops_at_confirmation();
        Ok(())
    }

    fn serve_meta(mut self, nexus: &RunningNexus) -> Result<(), String> {
        let query = self.peer.read_meta_query()?;
        // The peer is read from the kernel; nothing in the query names it.
        let peer = self.peer.peer_process();
        let response = match nexus.meta_refusal(peer) {
            Some(refusal) => meta_signal_flow::Response::MetaRefused(refusal),
            None => {
                let resolver = nexus.meta_resolver(peer);
                nexus.dispatch_meta_serially(query, resolver)
            }
        };
        self.peer.write_meta_response(&response)?;
        drop(self);
        nexus.settle_stops_at_confirmation();
        Ok(())
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
        let listener = ClaimedSocket::bind(socket)?;
        std::thread::scope(|scope| {
            // A stop requested before the bind found no listener to wake.
            let incoming = listener
                .incoming()
                .take(if self.stopping() { 0 } else { usize::MAX });
            for peer in incoming {
                // A stop wakes this accept with its own connection; whatever
                // was accepted with it is closed unanswered.
                if self.stopping() {
                    break;
                }
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
        // Every accepted connection has finished: the scope joined them.
        let _ = fs::remove_file(socket);
        if self.stopping() {
            Ok(())
        } else {
            Err(format!("{surface} listener closed"))
        }
    }

    /// Whether the Nexus has been told to stop.
    fn stopping(&self) -> bool;
}

impl ListensOnSocket for RunningNexus {
    fn stopping(&self) -> bool {
        self.stopping.requested()
    }
}

/// The Nexus's stop request: once set it stays set.
#[derive(Default)]
pub struct Stopping {
    requested: AtomicBool,
}

/// What a stop request can say: whether it was made, and making it.
pub trait HoldsStopRequest {
    fn requested(&self) -> bool;
    fn request(&self);
}

impl HoldsStopRequest for Stopping {
    fn requested(&self) -> bool {
        self.requested.load(Ordering::SeqCst)
    }

    fn request(&self) {
        self.requested.store(true, Ordering::SeqCst);
    }
}

/// Stops a running Nexus so that every thread borrowing it returns and the
/// store closes cleanly: the accept loops stop taking connections and join
/// the ones in flight, the launch promoter and launch observers return, and
/// agent observers end at their next read timeout.
pub trait StopsNexus {
    fn stop(&self, sockets: &[&Path]);
}

impl StopsNexus for RunningNexus {
    fn stop(&self, sockets: &[&Path]) {
        self.stopping.request();
        self.store.launch_changes.announce();
        // Each accept loop is blocked in accept; a connection wakes it. A
        // listener not yet bound sees the request once it binds.
        for socket in sockets {
            let _ = UnixStream::connect(socket);
        }
    }
}

/// A socket path this process binds, after making sure no live listener is
/// there: a socket file nobody accepts on is stale and removed, a socket
/// that accepts belongs to a live process and is refused.
pub struct ClaimedSocket;

/// Binds a socket path this process claims.
pub trait ClaimsSocket {
    fn bind(socket: &Path) -> Result<UnixListener, String>;
}

impl ClaimsSocket for ClaimedSocket {
    fn bind(socket: &Path) -> Result<UnixListener, String> {
        use std::os::unix::fs::FileTypeExt;
        if let Some(directory) = socket
            .parent()
            .filter(|directory| !directory.as_os_str().is_empty())
        {
            fs::create_dir_all(directory)
                .map_err(|error| format!("cannot create {}: {error}", directory.display()))?;
        }
        match fs::symlink_metadata(socket) {
            Ok(metadata) if metadata.file_type().is_socket() => match UnixStream::connect(socket) {
                Ok(_peer) => {
                    return Err(format!(
                        "socket {} is already accepting connections",
                        socket.display()
                    ));
                }
                Err(error) if error.kind() == std::io::ErrorKind::ConnectionRefused => {
                    fs::remove_file(socket).map_err(|error| {
                        format!("cannot remove stale socket {}: {error}", socket.display())
                    })?;
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => {
                    return Err(format!(
                        "cannot establish whether socket {} is stale: {error}",
                        socket.display()
                    ));
                }
            },
            Ok(_) => {
                return Err(format!(
                    "socket path {} exists and is not a socket",
                    socket.display()
                ));
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(format!(
                    "cannot inspect socket {}: {error}",
                    socket.display()
                ));
            }
        }
        let listener = UnixListener::bind(socket)
            .map_err(|error| format!("cannot bind socket {}: {error}", socket.display()))?;
        if let Err(error) = fs::set_permissions(socket, fs::Permissions::from_mode(0o600)) {
            drop(listener);
            let _ = fs::remove_file(socket);
            return Err(format!(
                "cannot protect socket {}: {error}",
                socket.display()
            ));
        }
        Ok(listener)
    }
}

/// SIGTERM and SIGINT, taken by one watching thread instead of killing the
/// process, so a stop closes the store. The signals are blocked before any
/// thread starts, so every thread inherits the block; a spawned child
/// process starts with an empty mask.
pub struct TerminationSignals {
    set: libc::sigset_t,
}

/// Blocks the termination signals, then watches for them.
pub trait WatchesTerminationSignals: Sized {
    /// Blocks SIGTERM and SIGINT in the calling thread; call before any
    /// other thread starts.
    fn block() -> Self;

    /// Waits for SIGTERM or SIGINT until `finished` holds; true when a
    /// signal came.
    fn watch(&self, finished: &dyn Fn() -> bool) -> bool;
}

impl WatchesTerminationSignals for TerminationSignals {
    fn block() -> Self {
        // SAFETY: the set is initialized by sigemptyset before use, and
        // pthread_sigmask only reads it.
        unsafe {
            let mut set = std::mem::zeroed::<libc::sigset_t>();
            libc::sigemptyset(&mut set);
            libc::sigaddset(&mut set, libc::SIGTERM);
            libc::sigaddset(&mut set, libc::SIGINT);
            libc::pthread_sigmask(libc::SIG_BLOCK, &set, std::ptr::null_mut());
            Self { set }
        }
    }

    fn watch(&self, finished: &dyn Fn() -> bool) -> bool {
        const POLL: libc::timespec = libc::timespec {
            tv_sec: 0,
            tv_nsec: 200_000_000,
        };
        while !finished() {
            // SAFETY: the set and timeout are valid for the call; no
            // siginfo is requested.
            let signal = unsafe { libc::sigtimedwait(&self.set, std::ptr::null_mut(), &POLL) };
            if signal == libc::SIGTERM || signal == libc::SIGINT {
                return true;
            }
        }
        false
    }
}

/// Reports a listed row as what Herdr shows of it, without writing anything.
///
/// A flow's stored row says what Flow last recorded, which is not the same as
/// what is there now, and List used to report the row alone. Two witnessed
/// ways it went wrong: a seat whose pane was gone stayed Pending on the route
/// it was bound to, and a seat that was plainly live stayed Pending because
/// only a Presented Deliver ever promoted one — so a flow bound through the
/// privileged contract, or one started before Flow Nexus, was Pending forever.
/// A listed flow is therefore reconciled against Herdr before it is answered:
///
/// - its bound pane present: the flow is witnessed live, so its route is
///   refreshed and it is reported Active;
/// - its bound pane gone: the flow is reported `Exited`, with no route and no
///   endpoint. `Exited` is the seat's own going, never `Retired` — an exit
///   retains the flow's record and retirement is an act of authority.
///
/// **This writes nothing.** List is a query, and a query does not change what
/// it is asked about: the row keeps whatever Flow recorded, and only a command
/// that witnesses a pane's fate — `Stop`, a replacement's reap, `Retire`, or a
/// `Deliver` that finds the pane gone — persists an ended lifecycle. Reading the
/// truth and recording it are deliberately separate.
///
/// This helper is for internal native-flow reconciliation. Metaflows on the
/// ordinary wire are read from their own durable records and never projected
/// from these native seat rows.
pub trait ReconcilesListedFlows {
    fn reconciled(&self, nodes: Vec<FlowNode>) -> Vec<FlowNode>;
    fn reconciled_node(&self, node: FlowNode) -> FlowNode;
}

impl ReconcilesListedFlows for RunningNexus {
    fn reconciled(&self, nodes: Vec<FlowNode>) -> Vec<FlowNode> {
        nodes
            .into_iter()
            .map(|node| self.reconciled_node(node))
            .collect()
    }

    fn reconciled_node(&self, node: FlowNode) -> FlowNode {
        if !node.flow_lifecycle.is_live() {
            return node;
        }
        match self.herdr.pane_presence(&node) {
            herdr::PanePresence::Present => {
                let mut node = self.herdr.refresh_route(node);
                node.flow_lifecycle = FlowLifecycle::Active;
                node
            }
            herdr::PanePresence::Unknown => node,
            herdr::PanePresence::Absent | herdr::PanePresence::Bare => FlowNode {
                endpoint_selection: EndpointSelection::Unavailable,
                herdr_route_selection: HerdrRouteSelection::Unavailable,
                flow_lifecycle: FlowLifecycle::Exited,
                ..node
            },
        }
    }
}

/// Waits out the transient ambiguity of a launch that is in fact succeeding.
///
/// Start submits the first prompt and cannot know in that instant whether
/// the seat answered: a receipt that has not landed yet is
/// `PromptDeliveryResult::Ambiguous`, and answering the caller with it makes
/// every caller of a working launch handle a non-failure. The ambiguity is
/// not intrinsic — the Nexus's own promoter is already watching the seat's
/// transcript and settles the launch the moment the receipt lands — so
/// Start simply waits for that outcome and answers it: Started, Replaced, or
/// a typed rejection.
///
/// The wait ends on the outcome, never on the clock; the deadline only
/// bounds a launch whose seat never answers at all, and a caller who reaches
/// it still gets `StartAmbiguous` and can ask again or subscribe with
/// `Observe.Launch`. The dispatch gate is not held: the promoter needs it.
pub trait AwaitsLaunchSettlement {
    /// How long Start waits for a seat to answer its receipt. A native
    /// harness opens, loads its skills and replies well inside this; it is
    /// the bound on a seat that never will.
    const LAUNCH_SETTLEMENT_WAIT: Duration = Duration::from_secs(180);

    /// The settled outcome, or None when nothing settled within the wait.
    fn settled_launch(&self, launch_request_id: &str) -> Option<Response>;
}

impl AwaitsLaunchSettlement for RunningNexus {
    fn settled_launch(&self, launch_request_id: &str) -> Option<Response> {
        let changes = self.store.launch_changes.clone();
        let deadline = std::time::Instant::now() + Self::LAUNCH_SETTLEMENT_WAIT;
        loop {
            let seen = changes.current();
            match self.store.launch_outcome(launch_request_id) {
                Ok(Some(outcome)) => return Some(outcome.response(&self.store)),
                Ok(None) => {}
                Err(_) => return None,
            }
            if !changes.until(seen, deadline) {
                return None;
            }
        }
    }
}

trait DispatchesSerially {
    /// Store transitions are read-modify-write; one dispatch runs at a time
    /// across both sockets while frames are read concurrently.
    fn dispatch_serially(&self, query: Query, origin: Origin) -> Response;
    fn dispatch_meta_serially(
        &self,
        query: meta_signal_flow::Query,
        resolver: Option<meta_signal_flow::ResolverIdentity>,
    ) -> meta_signal_flow::Response;
}

impl DispatchesSerially for RunningNexus {
    fn dispatch_serially(&self, query: Query, origin: Origin) -> Response {
        let launch_request_id = match &query {
            Query::Start(profile) | Query::Replace(profile) => {
                Some(profile.launch_request_id.clone())
            }
            _ => None,
        };
        let response = {
            let _turn = self
                .dispatch_gate
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            self.dispatch_from(query, origin)
        };
        // A launch's first turn may have queued work meanwhile; its targets
        // are woken now that the gate is given back.
        self.wake_deferred();
        // The gate is released before the wait: the launch is settled by the
        // Nexus's own promoter, which needs the gate to do it.
        match (launch_request_id, &response) {
            (Some(launch_request_id), Response::StartAmbiguous(_)) => {
                self.settled_launch(&launch_request_id).unwrap_or(response)
            }
            _ => response,
        }
    }

    fn dispatch_meta_serially(
        &self,
        query: meta_signal_flow::Query,
        resolver: Option<meta_signal_flow::ResolverIdentity>,
    ) -> meta_signal_flow::Response {
        let response = {
            let _turn = self
                .dispatch_gate
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            self.dispatch_meta_from(query, resolver)
        };
        // A resolution that committed leaves the oldest request queued (Absent)
        // or the next one oldest (Delivered): a seat already resting gets no
        // turn end, so it is handed that request now, the gate given back.
        if let meta_signal_flow::Response::QueueDeliveryResolved(resolved) = &response {
            self.drain_awake(&resolved.address);
        }
        response
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

/// A Unix stream carries Signal as length-prefixed rkyv frames of at most
/// 1 MiB.
pub trait CarriesSignalFrames {
    const FRAME_LIMIT: usize = 1024 * 1024;

    fn write_frame(&mut self, bytes: &[u8]) -> Result<(), String>;
    fn read_frame(&mut self) -> Result<Vec<u8>, String>;
    fn write_query(&mut self, value: &Query) -> Result<(), String>;
    fn read_query(&mut self) -> Result<Query, String>;
    fn write_response(&mut self, value: &Response) -> Result<(), String>;
    fn read_response(&mut self) -> Result<Response, String>;
    fn read_meta_query(&mut self) -> Result<meta_signal_flow::Query, String>;
    fn write_meta_response(&mut self, value: &meta_signal_flow::Response) -> Result<(), String>;
}

impl CarriesSignalFrames for UnixStream {
    fn write_frame(&mut self, bytes: &[u8]) -> Result<(), String> {
        self.write_all(&(bytes.len() as u32).to_be_bytes())
            .map_err(|e| e.to_string())?;
        self.write_all(bytes).map_err(|e| e.to_string())
    }

    fn read_frame(&mut self) -> Result<Vec<u8>, String> {
        let mut length = [0; 4];
        self.read_exact(&mut length).map_err(|e| e.to_string())?;
        let length = u32::from_be_bytes(length) as usize;
        if length > Self::FRAME_LIMIT {
            return Err("Signal frame exceeds 1 MiB".into());
        }
        let mut bytes = vec![0; length];
        self.read_exact(&mut bytes).map_err(|e| e.to_string())?;
        Ok(bytes)
    }

    fn write_query(&mut self, value: &Query) -> Result<(), String> {
        self.write_frame(&rkyv::to_bytes::<rkyv::rancor::Error>(value).map_err(|e| e.to_string())?)
    }

    fn read_query(&mut self) -> Result<Query, String> {
        rkyv::from_bytes::<Query, rkyv::rancor::Error>(&self.read_frame()?)
            .map_err(|e| e.to_string())
    }

    fn write_response(&mut self, value: &Response) -> Result<(), String> {
        self.write_frame(&rkyv::to_bytes::<rkyv::rancor::Error>(value).map_err(|e| e.to_string())?)
    }

    fn read_response(&mut self) -> Result<Response, String> {
        rkyv::from_bytes::<Response, rkyv::rancor::Error>(&self.read_frame()?)
            .map_err(|e| e.to_string())
    }

    fn read_meta_query(&mut self) -> Result<meta_signal_flow::Query, String> {
        rkyv::from_bytes::<meta_signal_flow::Query, rkyv::rancor::Error>(&self.read_frame()?)
            .map_err(|e| e.to_string())
    }

    fn write_meta_response(&mut self, value: &meta_signal_flow::Response) -> Result<(), String> {
        self.write_frame(&rkyv::to_bytes::<rkyv::rancor::Error>(value).map_err(|e| e.to_string())?)
    }
}

#[cfg(test)]
mod tests {
    use crate::DispatchesSerially;
    use crate::herdr::ConfiguresHerdrCli;
    use crate::herdr::ReadsHerdrPanes;
    mod configuring;
    mod delivery;
    mod launching_nexus;
    mod model;
    mod queue_drain;
    mod reservation;
    mod secondary;
    mod send_tool;
    mod submission;
    mod waking;

    use super::{Dispatches, ReconcilesListedFlows, RunningNexus};
    use crate::caller::{
        CallerPane, CallerProcess, IdentifiesPeer, LocatesCallerPane, ReadsProcess, ResolvesCaller,
    };
    use crate::fixture_executable::{FixtureExecutable, InstallsScript};
    use crate::{
        codex::{CodexEndpoint, CodexEndpoints},
        composition::{ComposesLaunch, LaunchComposer, OpensLaunchComposer},
        context::FixtureComposer,
        herdr::HerdrCli,
        store::{
            FlowStore, LaunchOutcome, OpensFlowStore, RecordsFlowLifecycle, RecordsLaunchOutcome,
            RecordsNativeLaunchBinding, RecordsNativeLaunchIntent, RecordsPromptDeliveryIntent,
            RecordsPromptDeliveryResult, RecordsRegistrationAcknowledgement, RegistersFlowIdentity,
            ReservesLaunchAttempt,
        },
    };
    use signal_flow::{
        Aspect, Caller, EndpointSelection, FlowLifecycle, FlowNode, FlowOrigin, HarnessKind,
        HerdrPaneBinding, HerdrRoute, HerdrRouteSelection, LaunchAttemptPhase, LaunchProfile,
        LaunchStatusOutcome, NativeLaunchBinding, NativeLaunchIntent, NativeTranscriptAbsence,
        NativeTranscriptBoundary, Origin, PowerLevel, PromptDeliveryIntent, PromptDeliveryResult,
        Query, RegistrationAcknowledgement, Response, StartRejection,
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
        /// The fixture Herdr server's socket for the session
        /// `messaging-build`: the test process listens on it, standing as
        /// that server, and every marked fixture process descends from it.
        _herdr_server: UnixListener,
    }

    fn inspected(response: Response, launch_request_id: &str) -> signal_flow::LaunchInspection {
        let Response::LaunchInspected(inspection) = response else {
            panic!("expected a launch inspection, got {response:?}")
        };
        assert_eq!(
            inspection.launch_attempt.launch_profile.launch_request_id,
            launch_request_id
        );
        inspection
    }

    fn assert_inspection_outcome(
        response: Response,
        launch_request_id: &str,
        expected_outcome: LaunchStatusOutcome,
        expected_flow_id: Option<&str>,
    ) {
        let inspection = inspected(response, launch_request_id);
        assert_eq!(
            inspection.launch_status_outcome_option,
            Some(expected_outcome)
        );
        assert_eq!(
            inspection
                .flow_node_option
                .as_ref()
                .map(|node| node.flow_id.as_str()),
            expected_flow_id
        );
    }

    /// How the fixture Herdr answers `agent prompt`, shaped as Herdr 0.8.2
    /// answers: a success reply on stdout, an error reply on stderr.
    #[derive(Clone, Copy)]
    enum PromptFixture<'a> {
        /// The text is typed and the reply is `agent_prompted` for this pane.
        Prompted(&'a str),
        /// As Prompted, but a Claude seat's transcript records nothing yet:
        /// its receipt comes later, from its hook or its transcript.
        PromptedUnrecorded(&'a str),
        /// This error code is answered before any text is typed.
        RefusedBeforeInput(&'a str),
        /// The text is typed, then this error code is answered.
        FailedAfterInput(&'a str),
    }

    impl PromptFixture<'_> {
        fn shell(
            &self,
            typed: &std::path::Path,
            agents: &[serde_json::Value],
            claude_receipt: Option<(&std::path::Path, &str)>,
        ) -> String {
            let mut typing = format!("printf '%s' \"$6\" >> '{}'", typed.display());
            if let Some((transcript, session)) = claude_receipt
                && !matches!(self, Self::PromptedUnrecorded(_))
            {
                typing.push_str(&format!(
                    "; escaped=$(printf '%s' \"$6\" | sed ':a;N;$!ba;s/\\\\/\\\\\\\\/g;s/\"/\\\\\"/g;s/\\n/\\\\n/g'); printf '{{\"type\":\"user\",\"sessionId\":\"{session}\",\"uuid\":\"fixture-receipt\",\"message\":{{\"content\":\"%s\"}}}}\\n' \"$escaped\" >> '{}'",
                    transcript.display()
                ));
            }
            let error = |code: &str| {
                format!(
                    "printf '%s\\n' '{{\"id\":\"cli:agent:prompt\",\"error\":{{\"code\":\"{code}\",\"message\":\"fixture\"}}}}' >&2; exit 1"
                )
            };
            match self {
                Self::Prompted(pane) | Self::PromptedUnrecorded(pane) => {
                    // Herdr answers with the prompted pane's own AgentInfo,
                    // which the roster already holds; a pane the roster does
                    // not know is answered with its id alone, as a reply
                    // naming another pane is.
                    let agent = agents
                        .iter()
                        .find(|agent| {
                            agent.get("pane_id").and_then(serde_json::Value::as_str) == Some(pane)
                        })
                        .cloned()
                        .unwrap_or_else(|| serde_json::json!({ "pane_id": pane }));
                    let reply = serde_json::json!({
                        "id":"cli:agent:prompt",
                        "result":{ "type":"agent_prompted", "agent":agent }
                    });
                    format!("{typing}; printf '%s\\n' '{reply}'")
                }
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

    /// The answer to a launch whose pane Herdr did not open.
    fn pane_unopened() -> StartRejection {
        StartRejection::NativeLaunchRefused(signal_flow::NativeLaunchRefusal {
            native_launch_stage: signal_flow::NativeLaunchStage::Open,
            native_launch_failure: signal_flow::NativeLaunchFailure::AdapterRefused,
        })
    }

    /// The answer to a launch whose claimed flow was not ready for its title.
    fn title_not_ready() -> StartRejection {
        StartRejection::NativeLaunchRefused(signal_flow::NativeLaunchRefusal {
            native_launch_stage: signal_flow::NativeLaunchStage::Naming,
            native_launch_failure: signal_flow::NativeLaunchFailure::NotReady,
        })
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
            aspect: Aspect::Mind,
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

    /// Herdr and the Codex rollouts as they show `bindings` live, for the
    /// one MetaBindExisting `run` performs: each pane in the snapshot with
    /// its harness, this test process in its foreground, each Codex session
    /// named by a rollout's session_meta. The fixture's own Herdr returns
    /// after it.
    fn with_bound_world<T>(
        fixture: &NexusFixture,
        bindings: &[&meta_signal_flow::FlowBinding],
        run: impl FnOnce() -> T,
    ) -> T {
        use crate::herdr::launch::NamesHerdrHarness;
        let agents = bindings
            .iter()
            .map(|binding| {
                let harness = binding.harness_kind.expected_harness();
                let mut agent = serde_json::json!({
                    "agent": harness,
                    "agent_status": "idle",
                    "name": binding.herdr_agent_name,
                    "pane_id": binding.herdr_pane_id,
                    "terminal_id": binding.herdr_terminal_id,
                    "workspace_id": binding.herdr_workspace_id,
                });
                // A Claude pane reports its session; a Codex one reports
                // none, and its rollout below proves it.
                if binding.harness_kind == HarnessKind::Claude {
                    agent["agent_session"] = serde_json::json!({
                        "source": format!("herdr:{harness}"),
                        "agent": harness,
                        "kind": "id",
                        "value": binding.native_session_id,
                    });
                }
                agent
            })
            .collect::<Vec<_>>();
        let rollouts = fixture.directory.path().join("native-transcripts/codex");
        fs::create_dir_all(&rollouts).expect("rollout root");
        for binding in bindings
            .iter()
            .filter(|binding| binding.harness_kind == HarnessKind::Codex)
        {
            fs::write(
                rollouts.join(format!("rollout-{}.jsonl", binding.native_session_id)),
                format!(
                    "{}\n",
                    serde_json::json!({"type":"session_meta","payload":{"id":binding.native_session_id}})
                ),
            )
            .expect("rollout written");
        }
        let previous = fs::read(&fixture.snapshot_program).ok();
        herdr_world(fixture, agents, std::process::id());
        let result = run();
        match previous {
            Some(bytes) => fs::write(&fixture.snapshot_program, bytes).expect("Herdr restored"),
            None => {
                let _ = fs::remove_file(&fixture.snapshot_program);
            }
        }
        result
    }

    /// A Herdr whose session snapshot shows `agents` and whose every pane
    /// runs `pid` in its foreground.
    fn herdr_world(fixture: &NexusFixture, agents: Vec<serde_json::Value>, pid: u32) {
        let snapshot = serde_json::json!({
            "id":"cli:api:snapshot",
            "result":{"snapshot":{"agents":agents,"protocol":20,"version":"0.8.2"},
            "type":"session_snapshot"}
        });
        FixtureExecutable {
            path: fixture.snapshot_program.clone(),
        }
        .install(&format!(
            "#!/bin/sh\n{}\ncase \"$3 $4\" in\n  \"api snapshot\") printf '%s\\n' '{}' ;;\n  \"pane process-info\") printf '{{\"result\":{{\"process_info\":{{\"pane_id\":\"%s\",\"foreground_processes\":[{{\"pid\":{}}}]}}}}}}\\n' \"$6\" ;;\n  *) exit 64 ;;\nesac\n",
            fixture.session_listing(),
            snapshot,
            pid,
        ));
    }

    /// One binding's MetaBindExisting result against the fixture's Herdr.
    fn bound_once(
        fixture: &NexusFixture,
        binding: meta_signal_flow::FlowBinding,
    ) -> meta_signal_flow::FlowBindingResult {
        let socket_path = fixture.directory.path().join("herdr-verify.sock");
        let _ = fs::remove_file(&socket_path);
        let _listener = UnixListener::bind(&socket_path).expect("live Herdr fixture socket");
        let meta_signal_flow::Response::BoundExisting(bound) =
            fixture
                .nexus
                .dispatch_meta(meta_signal_flow::Query::MetaBindExisting(
                    meta_signal_flow::MetaBindExisting {
                        flow_container: flow_container(&socket_path),
                        flow_binding_vector: vec![binding],
                    },
                ))
        else {
            panic!("a live container answers per binding")
        };
        let [result] = bound.flow_binding_result_vector.as_slice() else {
            panic!("one binding, one result")
        };
        result.clone()
    }

    fn refused_for(
        result: &meta_signal_flow::FlowBindingResult,
    ) -> Option<meta_signal_flow::FlowBindingRefusalReason> {
        match result {
            meta_signal_flow::FlowBindingResult::Refused(refused) => {
                Some(refused.flow_binding_refusal_reason.clone())
            }
            meta_signal_flow::FlowBindingResult::Bound(_) => None,
        }
    }

    /// MetaBindExisting takes no pane or session as given: the pane must be
    /// in the container's Herdr session with the binding's harness, run the
    /// binding's process, and run its session, as Herdr reports it for
    /// Claude and as a rollout's session_meta proves it for a Codex pane
    /// that reports none. Each failure is refused with its reason and
    /// nothing is stored.
    #[test]
    fn meta_bind_existing_verifies_the_pane_process_and_session_with_herdr() {
        use meta_signal_flow::FlowBindingRefusalReason as Reason;
        let fixture = NexusFixture::new();
        let shown = |binding: &meta_signal_flow::FlowBinding| {
            serde_json::json!({
                "agent": if binding.harness_kind == HarnessKind::Claude { "claude" } else { "codex" },
                "agent_status": "idle",
                "pane_id": binding.herdr_pane_id,
                "terminal_id": binding.herdr_terminal_id,
                "workspace_id": binding.herdr_workspace_id,
            })
        };
        let me = std::process::id();

        // Herdr unreadable.
        FixtureExecutable {
            path: fixture.snapshot_program.clone(),
        }
        .install("#!/bin/sh\nexit 64\n");
        let codex = existing_binding("codex-unread", "pane-1");
        assert_eq!(
            refused_for(&bound_once(&fixture, codex)),
            Some(Reason::HerdrUnreadable)
        );

        // The pane is not in the session, or another harness runs in it.
        let absent = existing_binding("codex-absent", "pane-2");
        let mut elsewhere = shown(&absent);
        elsewhere["pane_id"] = serde_json::Value::String("pane-9".into());
        herdr_world(&fixture, vec![elsewhere], me);
        assert_eq!(
            refused_for(&bound_once(&fixture, absent.clone())),
            Some(Reason::PaneAbsent)
        );
        let mut claude_there = shown(&absent);
        claude_there["agent"] = serde_json::Value::String("claude".into());
        herdr_world(&fixture, vec![claude_there], me);
        assert_eq!(
            refused_for(&bound_once(&fixture, absent)),
            Some(Reason::PaneAbsent)
        );

        // The pane runs another process.
        let other_process = existing_binding("codex-other-process", "pane-3");
        herdr_world(&fixture, vec![shown(&other_process)], 1);
        assert_eq!(
            refused_for(&bound_once(&fixture, other_process)),
            Some(Reason::ProcessMismatch)
        );

        // Herdr reports another session.
        let mismatched = existing_binding("codex-mismatched", "pane-4");
        let mut reporting = shown(&mismatched);
        reporting["agent_session"] = serde_json::json!({
            "source":"herdr:codex","agent":"codex","kind":"id","value":"native-someone-else"
        });
        herdr_world(&fixture, vec![reporting], me);
        assert_eq!(
            refused_for(&bound_once(&fixture, mismatched)),
            Some(Reason::SessionMismatch)
        );

        // A Codex pane reporting no session, and no rollout names it.
        let unproven = existing_binding("codex-unproven", "pane-5");
        herdr_world(&fixture, vec![shown(&unproven)], me);
        assert_eq!(
            refused_for(&bound_once(&fixture, unproven.clone())),
            Some(Reason::SessionUnproven)
        );
        // A rollout under that name whose session_meta names another thread
        // proves nothing either.
        let rollouts = fixture.directory.path().join("native-transcripts/codex");
        fs::create_dir_all(&rollouts).unwrap();
        fs::write(
            rollouts.join("rollout-native-codex-unproven.jsonl"),
            "{\"type\":\"session_meta\",\"payload\":{\"id\":\"another-thread\"}}\n",
        )
        .unwrap();
        assert_eq!(
            refused_for(&bound_once(&fixture, unproven.clone())),
            Some(Reason::SessionUnproven)
        );
        // The rollout naming it proves it.
        fs::write(
            rollouts.join("rollout-native-codex-unproven.jsonl"),
            "{\"type\":\"session_meta\",\"payload\":{\"id\":\"native-codex-unproven\"}}\n",
        )
        .unwrap();
        assert_eq!(refused_for(&bound_once(&fixture, unproven)), None);

        // A Claude pane must report its session, and that session.
        let mut claude = existing_binding("claude-quiet", "pane-6");
        claude.harness_kind = HarnessKind::Claude;
        herdr_world(&fixture, vec![shown(&claude)], me);
        assert_eq!(
            refused_for(&bound_once(&fixture, claude.clone())),
            Some(Reason::SessionUnreported)
        );
        let mut reporting = shown(&claude);
        reporting["agent_session"] = serde_json::json!({
            "source":"herdr:claude","agent":"claude","kind":"id","value":"native-claude-quiet"
        });
        herdr_world(&fixture, vec![reporting], me);
        assert_eq!(refused_for(&bound_once(&fixture, claude)), None);

        // Nothing refused was stored.
        for refused in [
            "codex-unread",
            "codex-absent",
            "codex-other-process",
            "codex-mismatched",
        ] {
            assert!(matches!(
                fixture
                    .nexus
                    .dispatch(Query::ResolveRecipient(refused.into())),
                Response::RecipientResolutionRejected(
                    signal_flow::RecipientResolutionRejection::UnknownFlow
                )
            ));
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
            Self::with_store(|_| {})
        }

        /// A fixture whose store at `flow.sema` is first prepared by `seed`.
        fn with_store(seed: impl FnOnce(&Path)) -> Self {
            let directory = tempfile::tempdir().expect("temporary nexus fixture");
            seed(&directory.path().join("flow.sema"));
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
                composer: LaunchComposer::with_context(crate::context::ContextComposer::Fixture(
                    FixtureComposer::from(
                        &[
                            ("spirit-core", "Spirit body."),
                            ("intent-flow", "Intent body."),
                            ("operation-main-flow", "Main flow body."),
                            ("knowledge-flow", "Knowledge body."),
                        ][..],
                    ),
                )),
                dispatch_gate: std::sync::Mutex::new(()),
                pane_leases: crate::delivery::lease::PaneLeases::default(),
                waking: crate::waking::WakingMetaflows::default(),
                stopped_at_confirmation: crate::waking::StoppedAtConfirmation::default(),
                stopping: crate::Stopping::default(),
                launch_calls: crate::send_tool::LaunchCalls::default(),
                codex_turns: crate::codex_turns::CodexTurns::default(),
            };
            let herdr_config = directory.path().join("herdr-config");
            let session_socket = herdr_config.join("sessions/messaging-build/herdr.sock");
            fs::create_dir_all(session_socket.parent().expect("session directory"))
                .expect("fixture Herdr session directory");
            let herdr_server =
                UnixListener::bind(&session_socket).expect("fixture Herdr server socket");
            let fixture = Self {
                directory,
                nexus,
                snapshot_program,
                _herdr_server: herdr_server,
            };
            // The default session is listed, stopped, as Herdr lists it.
            fixture.list_herdr_sessions(&[
                ("default", herdr_config.join("herdr.sock")),
                ("messaging-build", session_socket),
            ]);
            fixture
        }

        /// What the fixture Herdr's `session list --json` answers.
        fn list_herdr_sessions(&self, sessions: &[(&str, PathBuf)]) {
            let sessions: Vec<serde_json::Value> = sessions
                .iter()
                .map(|(name, socket)| serde_json::json!({"name": name, "socket_path": socket}))
                .collect();
            fs::write(
                self.directory.path().join("herdr-sessions.json"),
                serde_json::json!({"sessions": sessions}).to_string(),
            )
            .expect("fixture Herdr session list");
        }

        /// The fixture Herdr's first line after its shebang: it answers
        /// `session list --json` from the listing the fixture keeps.
        fn session_listing(&self) -> String {
            format!(
                "[ \"$1 $2 $3\" = \"session list --json\" ] && {{ cat '{}'; exit 0; }}",
                self.directory.path().join("herdr-sessions.json").display()
            )
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
                origin: Origin::Flow(FlowOrigin {
                    flow_id: "908786".into(),
                    session_id: "01a0b22c-e24f-7452-9940-64490878680f".into(),
                }),
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
                "workspace_id":"w1",
                "agent_session":{
                    "source":"herdr:codex","agent":"codex","kind":"id",
                    "value":"01a0b22c-e24f-7452-9940-64490878680f"
                }
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
                "#!/bin/sh\n{}\n[ \"$1\" = \"--session\" ] && [ \"$3\" = \"api\" ] && [ \"$4\" = \"snapshot\" ] || exit 64\nprintf '%s\\n' '{}'\n",
                self.session_listing(),
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
                "result":{"snapshot":{"agents":agents.clone(),"protocol":20,"version":"0.8.2"},
                "type":"session_snapshot"}
            });
            let log = self.directory.path().join("herdr-operations.log");
            let typed = self.directory.path().join("pane-typed.txt");
            let claude_session = agents.iter().find_map(|agent| {
                let session = agent.pointer("/agent_session/value")?.as_str()?;
                (agent.pointer("/agent_session/agent")?.as_str()? == "claude").then_some(session)
            });
            let claude_transcript = claude_session.map(|session| {
                self.directory
                    .path()
                    .join("native-transcripts/claude/-home-li-primary")
                    .join(format!("{session}.jsonl"))
            });
            let body = format!(
                "#!/bin/sh\n{}\ncase \"$3 $4\" in\n  \"api snapshot\") printf '%s\\n' '{}' ;;\n  \"agent prompt\") printf '%s\\n' \"$*\" >> '{}'; {} ;;\n  \"pane close\"|\"pane send-keys\"|\"agent wait\") printf '%s\\n' \"$*\" >> '{}' ;;\n  \"agent read\") printf '%s\\n' '❯ ' '› ' ;;\n  *) exit 64 ;;\nesac\n",
                self.session_listing(),
                snapshot,
                log.display(),
                prompt.shell(
                    &typed,
                    &agents,
                    claude_transcript.as_deref().zip(claude_session),
                ),
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
        use super::{CarriesSignalFrames, ServesOrdinary};
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
        assert!(garbage.read_response().is_err());

        let mut peer = connect();
        peer.write_query(&Query::Metaflows)
            .expect("metaflows query written");
        assert!(matches!(
            peer.read_response().expect("list answered"),
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

        /// A MiddleAbrupt Deliver of `text` from the owner: the tier that
        /// neither interrupts nor waits for the recipient to rest.
        fn send(&self, text: &str) -> Sent {
            self.send_to("908786", text)
        }

        fn send_to(&self, flow_id: &str, text: &str) -> Sent {
            match self.nexus.dispatch_meta(meta_signal_flow::Query::Deliver(
                meta_signal_flow::DeliveryRequest {
                    delivery_id: format!("delivery-{flow_id}-{text}"),
                    flow_id: flow_id.into(),
                    message: middle_abrupt(text),
                },
            )) {
                meta_signal_flow::Response::Delivered(delivery) => {
                    assert_eq!(delivery.flow_id, flow_id);
                    assert_eq!(
                        delivery.interrupt_witness,
                        meta_signal_flow::InterruptWitness::NotRequested
                    );
                    Sent::Graded(delivery.delivery_grade)
                }
                meta_signal_flow::Response::DeliveryRejected(rejection) => Sent::Refused(rejection),
                other => panic!("Deliver answers Delivered or DeliveryRejected: {other:?}"),
            }
        }

        /// The lifecycle as the row holds it, with no reconciliation. This
        /// is what the Deliver path did or did not write; reconciliation is
        /// a separate claim from the stored row.
        fn stored_lifecycle(&self) -> FlowLifecycle {
            use crate::store::ReadsFlowRows as _;
            self.nexus
                .store
                .flow_node("908786")
                .expect("stored row")
                .expect("registered flow")
                .flow_lifecycle
        }

        fn only_lifecycle(&self) -> FlowLifecycle {
            use crate::store::ReadsFlowRows as _;
            let rows = self.nexus.store.flow_nodes().expect("Flow rows read");
            assert_eq!(rows.len(), 1);
            self.nexus.reconciled_node(rows[0].clone()).flow_lifecycle
        }
    }

    /// What a Deliver answered, without its identifiers.
    #[derive(Debug, PartialEq, Eq)]
    enum Sent {
        Graded(meta_signal_flow::DeliveryGrade),
        Refused(meta_signal_flow::DeliveryRejection),
    }

    /// The MessageId every fixture letter names: what a recipient reading
    /// the pane would Acknowledge.
    const FIXTURE_MESSAGE_ID: &str = "m-7f3a2c";

    fn middle_abrupt(text: &str) -> meta_signal_flow::Message {
        meta_signal_flow::Message::MiddleAbrupt(meta_signal_flow::Letter {
            message_id: FIXTURE_MESSAGE_ID.into(),
            sender: meta_signal_flow::Sender::Owner,
            content: meta_signal_flow::Content::Text(text.into()),
        })
    }

    /// The pane text of a MiddleAbrupt from the owner, written out by hand:
    /// the head comes first, then the id, the sender and the text.
    fn rendered(text: &str) -> String {
        format!("MiddleAbrupt.{{ {FIXTURE_MESSAGE_ID} Owner Text.«{text}» }}")
    }

    fn prompts(operation_log: &std::path::Path) -> Vec<String> {
        fs::read_to_string(operation_log)
            .unwrap_or_default()
            .lines()
            .filter(|line| line.contains(" agent prompt "))
            .map(str::to_owned)
            .collect()
    }

    const OBSERVED_PROMPT: &str = "--session messaging-build agent prompt w1:p3 MiddleAbrupt.{ m-7f3a2c Owner Text.«bare prompt» } --wait --until working --until idle --until done --until blocked --timeout 10000";

    #[test]
    fn observed_send_promotes_a_pending_flow_and_types_only_the_bare_input() {
        let fixture = NexusFixture::new();
        let operation_log = fixture.accept_pane_operations(
            vec![fixture.settled_agent()],
            PromptFixture::Prompted("w1:p3"),
        );
        fixture.register_with(FlowLifecycle::Pending);

        assert_eq!(
            fixture.send("bare prompt"),
            Sent::Graded(meta_signal_flow::DeliveryGrade::Presented),
            "a settled recipient seen reacting is Presented"
        );
        assert_eq!(fixture.only_lifecycle(), FlowLifecycle::Active);
        assert_eq!(
            fixture.typed().as_deref(),
            Some(rendered("bare prompt").as_str())
        );
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

    /// The pane a staged launch registered, as Herdr shows a seat not yet
    /// able to take a prompt.
    fn unready_launch_agent() -> serde_json::Value {
        serde_json::json!({
            "agent":"codex","agent_status":"unknown","interactive_ready":false,
            "name":"fixture-agent","pane_id":"w1:p1","terminal_id":"fixture-terminal",
            "workspace_id":"fixture-workspace",
            "agent_session":{
                "source":"herdr:codex","agent":"codex","kind":"id",
                "value":STAGED_SESSION
            }
        })
    }

    /// A launch refused after its flow was registered, whose FlowId claim
    /// was given back, leaves a Pending flow in a live pane that is not
    /// ready for a prompt. Stop resolves the route from the recorded pane,
    /// closes it, and records the flow Stopped.
    #[test]
    fn stop_closes_the_live_pane_of_a_pending_flow_that_was_never_ready() {
        let fixture = NexusFixture::new();
        let operation_log = fixture.accept_pane_operations(
            vec![unready_launch_agent()],
            PromptFixture::Prompted("w1:p1"),
        );
        let launch = fixture.staged_launch("orphaned-request", None);
        launch.reserve(&fixture.nexus);
        launch.record_native_intent(&fixture.nexus);
        launch.bind(&fixture.nexus);
        launch.register_and_acknowledge(&fixture.nexus);
        fs::remove_file(fixture.directory.path().join("flows/.908786.flow-id")).unwrap();
        assert_eq!(fixture.lifecycle("908786"), FlowLifecycle::Pending);

        assert_eq!(
            fixture.nexus.dispatch(Query::Stop("908786".into())),
            Response::Stopped("908786".into())
        );
        assert_eq!(fixture.lifecycle("908786"), FlowLifecycle::Stopped);
        let operations = fs::read_to_string(&operation_log).expect("Herdr operation log");
        assert!(operations.contains("pane close w1:p1"), "{operations}");

        // A pane Herdr no longer shows is not closed: the route is gone.
        let fixture = NexusFixture::new();
        fixture.accept_pane_operations(vec![], PromptFixture::Prompted("w1:p1"));
        let launch = fixture.staged_launch("vanished-request", None);
        launch.reserve(&fixture.nexus);
        launch.record_native_intent(&fixture.nexus);
        launch.bind(&fixture.nexus);
        launch.register_and_acknowledge(&fixture.nexus);
        assert_eq!(
            fixture.nexus.dispatch(Query::Stop("908786".into())),
            Response::StopRejected(signal_flow::StopRejection::RouteUnavailable)
        );
    }

    /// A launch refused after its flow was registered in its pane closes
    /// that pane and records the flow Stopped, which gives back the Address
    /// its acknowledgement held.
    #[test]
    fn a_refused_launch_closes_its_pane_and_stops_its_flow() {
        use crate::launching::{AbandonsRefusedLaunch, LaunchesFlows};
        use crate::store::address_hold::HoldsLaunchAddress;
        use crate::store::metaflow::SleepsMetaflows;

        let fixture = NexusFixture::new();
        let operation_log = fixture.accept_pane_operations(
            vec![unready_launch_agent()],
            PromptFixture::Prompted("w1:p1"),
        );
        let launch = fixture.staged_launch("refused-title-request", None);
        launch.reserve(&fixture.nexus);
        launch.record_native_intent(&fixture.nexus);
        launch.bind(&fixture.nexus);
        launch.register_and_acknowledge(&fixture.nexus);
        assert!(
            fixture
                .nexus
                .store
                .metaflow_awake_as("908786")
                .unwrap()
                .is_some()
        );

        fixture
            .nexus
            .abandon_refused_launch("refused-title-request", &HarnessKind::Codex, None);
        let operations = fs::read_to_string(&operation_log).expect("Herdr operation log");
        assert!(operations.contains("pane close w1:p1"), "{operations}");
        assert_eq!(fixture.lifecycle("908786"), FlowLifecycle::Stopped);
        assert_eq!(
            fixture.nexus.store.metaflow_awake_as("908786").unwrap(),
            None
        );
        assert_eq!(fixture.nexus.store.address_hold("908786").unwrap(), None);
        assert_eq!(
            fixture.nexus.settle(
                "refused-title-request",
                Response::StartRejected(title_not_ready())
            ),
            Response::StartRejected(title_not_ready())
        );
    }

    /// A pane whose harness never appeared shows in Herdr's snapshot as a
    /// pane with no agent; the refused launch closes it once it holds the
    /// session the pane was created for. Without one, nothing is closed and
    /// the pane stays recorded for a later reconciliation.
    #[test]
    fn a_refused_launch_closes_a_pane_whose_harness_never_appeared() {
        use crate::herdr::PaneOwnership;
        use crate::launching::{AbandonsRefusedLaunch, LaunchTeardown};
        use crate::store::opened_thread::RecordsOpenedThreads;
        use crate::store::{ReadsCreatedLaunchPane, RecordsCreatedLaunchPane};
        let fixture = NexusFixture::new();
        let launch = fixture.staged_launch("bare-pane-request", None);
        launch.reserve(&fixture.nexus);
        launch.record_native_intent(&fixture.nexus);
        assert!(
            fixture
                .nexus
                .store
                .record_created_launch_pane(launch.pane.clone())
                .unwrap()
        );
        fixture
            .nexus
            .store
            .request_harness_start("bare-pane-request")
            .unwrap();
        let snapshot = serde_json::json!({
            "id":"cli:api:snapshot",
            "result":{"snapshot":{
                "agents":[],
                "panes":[{"pane_id":launch.pane.herdr_pane_id,"terminal_id":launch.pane.herdr_terminal_id}],
                "protocol":20,"version":"0.8.2"},
            "type":"session_snapshot"}
        });
        let log = fixture.directory.path().join("bare-pane.log");
        FixtureExecutable {
            path: fixture.snapshot_program.clone(),
        }
        .install(&format!(
            "#!/bin/sh\ncase \"$3 $4\" in\n  \"api snapshot\") printf '%s\\n' '{}' ;;\n  \"pane close\") printf '%s\\n' \"$*\" >> '{}' ;;\n  *) exit 64 ;;\nesac\n",
            snapshot,
            log.display(),
        ));
        // No session recorded for the pane: missing identity closes nothing.
        assert_eq!(
            fixture
                .nexus
                .abandon_refused_launch("bare-pane-request", &HarnessKind::Codex, None),
            LaunchTeardown::Kept(PaneOwnership::Unidentified)
        );
        assert!(fs::read_to_string(&log).unwrap_or_default().is_empty());
        assert!(
            fixture
                .nexus
                .store
                .recoverable_launch_pane("bare-pane-request")
                .unwrap()
                .is_some()
        );
        // The thread the launch opened is the pane's session.
        assert!(
            fixture
                .nexus
                .store
                .record_opened_thread("bare-pane-request", "bare-pane-thread")
                .unwrap()
        );
        assert_eq!(
            fixture
                .nexus
                .abandon_refused_launch("bare-pane-request", &HarnessKind::Codex, None),
            LaunchTeardown::TakenDown
        );
        let operations = fs::read_to_string(&log).expect("Herdr operation log");
        assert!(
            operations.contains(&format!("pane close {}", launch.pane.herdr_pane_id)),
            "{operations}"
        );
        assert_eq!(
            fixture
                .nexus
                .store
                .recoverable_launch_pane("bare-pane-request")
                .unwrap(),
            None
        );
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
            Sent::Graded(meta_signal_flow::DeliveryGrade::Transported)
        );
        // The Deliver did not promote it: only a Presented Deliver does that.
        // List separately reports the seat live, because its pane is.
        assert_eq!(fixture.stored_lifecycle(), FlowLifecycle::Pending);
        assert_eq!(fixture.only_lifecycle(), FlowLifecycle::Active);
        assert_eq!(
            fixture.typed().as_deref(),
            Some(rendered("queued prompt").as_str())
        );
        assert_eq!(
            prompts(&operation_log),
            vec![format!(
                "--session messaging-build agent prompt w1:p3 {}",
                rendered("queued prompt")
            )]
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
                Sent::Refused(meta_signal_flow::DeliveryRejection::NotDelivered),
                "{code}"
            );
            assert_eq!(fixture.typed(), None, "{code}");
            assert_eq!(prompts(&operation_log).len(), 1, "{code}");
            assert_eq!(fixture.stored_lifecycle(), FlowLifecycle::Pending, "{code}");
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
                Sent::Graded(meta_signal_flow::DeliveryGrade::Uncertain),
                "{failure}"
            );
            assert_eq!(
                fixture.typed().as_deref(),
                Some(rendered("bare prompt").as_str()),
                "{failure}"
            );
            assert_eq!(
                prompts(&operation_log),
                vec![OBSERVED_PROMPT.to_owned()],
                "{failure}: exactly one prompt, no retry"
            );
            assert_eq!(
                fixture.stored_lifecycle(),
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
            Sent::Graded(meta_signal_flow::DeliveryGrade::Uncertain)
        );
        assert_eq!(prompts(&operation_log).len(), 1);
        assert_eq!(fixture.stored_lifecycle(), FlowLifecycle::Pending);
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
            Sent::Graded(meta_signal_flow::DeliveryGrade::Transported)
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

        assert_eq!(
            fixture.send("bare prompt"),
            Sent::Graded(meta_signal_flow::DeliveryGrade::Presented)
        );
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
            fixture.send_to("908786", "stale route prompt"),
            Sent::Refused(meta_signal_flow::DeliveryRejection::RouteUnavailable)
        );
        use crate::store::ReadsFlowRows as _;
        let rows = fixture.nexus.store.flow_nodes().expect("Flow rows read");
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

        let response = with_bound_world(&fixture, &[&accepted.clone()], || {
            fixture
                .nexus
                .dispatch_meta(meta_signal_flow::Query::MetaBindExisting(
                    meta_signal_flow::MetaBindExisting {
                        flow_container: container.clone(),
                        flow_binding_vector: vec![accepted, ambiguous, dead, duplicate, wrong_cwd],
                    },
                ))
        });
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
        assert_eq!(node.origin, Origin::Unbound);
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

    /// A Codex pane as Herdr 0.8.2 reports a rested imported Codex flow: done,
    /// with no `interactive_ready` key (the live 26c50c, 2026-09-25).
    fn rested_imported_codex_agent() -> serde_json::Value {
        serde_json::json!({
            "agent":"codex", "agent_status":"done", "name":"agent-w1:p3",
            "pane_id":"w1:p3", "terminal_id":"terminal-w1:p3"
        })
    }

    /// Herdr omits `name` for a pane it was never given one for. The label
    /// is not part of the binding and is no part of the presentation either:
    /// a nameless recipient seen reacting is Presented, as any other is.
    /// Grading through `current_route`, which re-reads the name, settled
    /// every such delivery Uncertain.
    fn nameless_rested_codex_agent() -> serde_json::Value {
        let mut agent = rested_imported_codex_agent();
        agent.as_object_mut().expect("agent object").remove("name");
        agent
    }

    #[test]
    fn a_nameless_recipient_seen_reacting_is_presented() {
        let fixture = NexusFixture::new();
        let operation_log = fixture.accept_pane_operations(
            vec![nameless_rested_codex_agent()],
            PromptFixture::Prompted("w1:p3"),
        );
        bind_existing_caller(&fixture, "908786", "w1:p3");

        assert_eq!(
            fixture.send("bare prompt"),
            Sent::Graded(meta_signal_flow::DeliveryGrade::Presented)
        );
        assert_eq!(prompts(&operation_log), vec![OBSERVED_PROMPT.to_owned()]);
    }

    fn resolved(fixture: &NexusFixture) -> FlowNode {
        let Response::RecipientResolved(node) = fixture
            .nexus
            .dispatch(Query::ResolveRecipient("908786".into()))
        else {
            panic!("an imported flow must resolve")
        };
        node
    }

    fn listed(fixture: &NexusFixture) -> FlowNode {
        use crate::store::ReadsFlowRows as _;
        let rows = fixture.nexus.store.flow_nodes().expect("Flow rows read");
        assert_eq!(rows.len(), 1);
        fixture.nexus.reconciled_node(rows[0].clone())
    }

    // The fixture's Codex endpoints name sockets that do not exist: an
    // imported Codex flow is routed through its Herdr pane alone, and its
    // endpoint stays a separate fact that neither gates nor changes.
    #[test]
    fn an_imported_codex_flow_is_sent_through_its_pane_without_an_endpoint() {
        let fixture = NexusFixture::new();
        let operation_log = fixture.accept_pane_operations(
            vec![rested_imported_codex_agent()],
            PromptFixture::Prompted("w1:p3"),
        );
        bind_existing_caller(&fixture, "908786", "w1:p3");

        let node = resolved(&fixture);
        assert_eq!(node.endpoint_selection, EndpointSelection::Unavailable);
        assert_eq!(
            node.herdr_route_selection,
            HerdrRouteSelection::Available(HerdrRoute {
                herdr_session_name: "messaging-build".into(),
                herdr_agent_name: "agent-w1:p3".into(),
                herdr_pane_id: "w1:p3".into(),
                herdr_terminal_id: "terminal-w1:p3".into(),
            })
        );
        assert_eq!(node.flow_lifecycle, FlowLifecycle::Pending);

        assert_eq!(
            fixture.send("bare prompt"),
            Sent::Graded(meta_signal_flow::DeliveryGrade::Presented)
        );
        assert_eq!(
            fixture.typed().as_deref(),
            Some(rendered("bare prompt").as_str())
        );
        assert_eq!(prompts(&operation_log), vec![OBSERVED_PROMPT.to_owned()]);
        let after = listed(&fixture);
        assert_eq!(after.flow_lifecycle, FlowLifecycle::Active);
        assert_eq!(after.endpoint_selection, EndpointSelection::Unavailable);
    }

    #[test]
    fn an_imported_codex_flow_whose_pane_is_gone_is_refused_before_typing() {
        let fixture = NexusFixture::new();
        let operation_log =
            fixture.accept_pane_operations(vec![], PromptFixture::Prompted("w1:p3"));
        bind_existing_caller(&fixture, "908786", "w1:p3");

        let node = resolved(&fixture);
        assert_eq!(node.herdr_route_selection, HerdrRouteSelection::Unavailable);
        assert_eq!(
            fixture.send("bare prompt"),
            Sent::Refused(meta_signal_flow::DeliveryRejection::RouteUnavailable)
        );
        assert_eq!(fixture.typed(), None);
        assert!(prompts(&operation_log).is_empty());
        // The pane is gone and Flow never closed it, so the flow Exited. The
        // Deliver above is the command that witnessed it, so the row itself now
        // says so; List would report the same either way, and Retired is not
        // what an observation ever produces.
        assert_eq!(fixture.stored_lifecycle(), FlowLifecycle::Exited);
        assert_eq!(fixture.only_lifecycle(), FlowLifecycle::Exited);
    }

    fn rebind(
        fixture: &NexusFixture,
        binding: meta_signal_flow::FlowBinding,
    ) -> meta_signal_flow::FlowBindingResult {
        let socket_path = fixture.directory.path().join("herdr-rebind.sock");
        let _ = fs::remove_file(&socket_path);
        let _listener = UnixListener::bind(&socket_path).expect("live Herdr fixture socket");
        let meta_signal_flow::Response::BoundExisting(bound) =
            with_bound_world(fixture, &[&binding.clone()], || {
                fixture
                    .nexus
                    .dispatch_meta(meta_signal_flow::Query::MetaBindExisting(
                        meta_signal_flow::MetaBindExisting {
                            flow_container: flow_container(&socket_path),
                            flow_binding_vector: vec![binding],
                        },
                    ))
            })
        else {
            panic!("a live container answers per binding")
        };
        let [result] = bound.flow_binding_result_vector.as_slice() else {
            panic!("one binding, one result")
        };
        result.clone()
    }

    /// The binding of the registered 908786 row (a RegisterFlow row, Active
    /// and roleless, as 5f38bc is live) under a newer agent name.
    fn binding_of_registered_row(fixture: &NexusFixture) -> meta_signal_flow::FlowBinding {
        let node = fixture.node();
        let mut binding = existing_binding("908786", "w1:p3");
        binding.native_session_id = node.session_id;
        binding.herdr_terminal_id = "term_65bb7f87270cb3".into();
        binding.herdr_agent_name = "field-astra-908786".into();
        binding
    }

    fn duplicate(flow_binding_result: &meta_signal_flow::FlowBindingResult) -> bool {
        matches!(
            flow_binding_result,
            meta_signal_flow::FlowBindingResult::Refused(refused)
                if refused.flow_binding_refusal_reason
                    == meta_signal_flow::FlowBindingRefusalReason::DuplicateFlowId
        )
    }

    #[test]
    fn a_matching_binding_of_a_flow_already_held_asserts_only_its_role() {
        use crate::store::ReadsFlowRoles;
        let fixture = NexusFixture::new();
        fixture.set_agents(vec![fixture.current_agent()]);
        fixture.register_with(FlowLifecycle::Active);
        let before = listed(&fixture);
        assert_eq!(fixture.nexus.store.role("908786").expect("role read"), None);

        let role = Caller {
            flow_id: "908786".into(),
            aspect: Aspect::Mind,
            power_level: PowerLevel::Medium,
            model_name: "gpt-sol".into(),
        };
        for _ in 0..2 {
            let result = rebind(&fixture, binding_of_registered_row(&fixture));
            assert!(
                matches!(
                    &result,
                    meta_signal_flow::FlowBindingResult::Bound(bound) if bound.flow_id == "908786"
                ),
                "{result:?}"
            );
            assert_eq!(
                fixture.nexus.store.role("908786").expect("role read"),
                Some(role.clone())
            );
            assert_eq!(listed(&fixture), before);
        }
    }

    #[test]
    fn a_differing_binding_of_a_flow_already_held_is_refused_and_writes_nothing() {
        use crate::store::ReadsFlowRoles;
        let fixture = NexusFixture::new();
        fixture.set_agents(vec![fixture.current_agent()]);
        fixture.register_with(FlowLifecycle::Active);
        let before = listed(&fixture);

        let mut other_terminal = binding_of_registered_row(&fixture);
        other_terminal.herdr_terminal_id = "term_reused".into();
        let mut other_pane = binding_of_registered_row(&fixture);
        other_pane.herdr_pane_id = "w1:p4".into();
        let mut other_thread = binding_of_registered_row(&fixture);
        other_thread.native_session_id = "another-thread".into();
        let mut other_harness = binding_of_registered_row(&fixture);
        other_harness.harness_kind = HarnessKind::Claude;
        for binding in [other_terminal, other_pane, other_thread, other_harness] {
            let result = rebind(&fixture, binding);
            assert!(duplicate(&result), "{result:?}");
            assert_eq!(fixture.nexus.store.role("908786").expect("role read"), None);
            assert_eq!(listed(&fixture), before);
        }

        // A role once recorded is not replaced by a different one.
        assert!(!duplicate(&rebind(
            &fixture,
            binding_of_registered_row(&fixture)
        )));
        let mut other_role = binding_of_registered_row(&fixture);
        other_role.power_level = PowerLevel::High;
        assert!(duplicate(&rebind(&fixture, other_role)));
        assert_eq!(
            fixture
                .nexus
                .store
                .role("908786")
                .expect("role read")
                .map(|role| role.power_level),
            Some(PowerLevel::Medium)
        );
        assert_eq!(listed(&fixture), before);
    }

    #[test]
    fn identical_launch_retry_reads_the_journal_before_a_deleted_source() {
        let fixture = NexusFixture::new();
        let profile = LaunchProfile {
            launch_request_id: "retry-request".into(),
            system_modules: vec!["spirit-core".into(), "intent-flow".into()],
            user_modules: vec!["operation-main-flow".into()],
            address: signal_flow::Address {
                aspect: Aspect::Field,
                topic: "flow".into(),
                layer: signal_flow::Layer::Secondary,
            },
            model_name: "fixture-model".into(),
            effort: "medium".into(),
            flow_id_option: None,
            remembered_flow_vector: Vec::new(),
            herdr_session_name: "fixture-session".into(),
            leave_outs_option: None,
            instruction_prompt: "fixture instruction".into(),
        };
        let origin = Origin::Flow(FlowOrigin {
            flow_id: "caller".into(),
            session_id: "caller-session".into(),
        });
        let composed = fixture
            .nexus
            .composer
            .compose(
                crate::composition::HarnessedProfile {
                    harness_kind: HarnessKind::Codex,
                    launch_profile: profile.clone(),
                },
                Vec::<signal_flow::LeaveOut>::new(),
            )
            .expect("new request composes once");
        fixture
            .nexus
            .store
            .reserve_launch_attempt(&composed, origin.clone())
            .expect("reservation persists");

        assert!(matches!(
            fixture
                .nexus
                .dispatch_from(Query::Start(profile.clone()), origin.clone()),
            Response::LaunchPending(_)
        ));
        let mut changed = profile;
        changed.effort = "high".into();
        assert_eq!(
            fixture
                .nexus
                .dispatch_from(Query::Start(changed), origin.clone()),
            Response::StartRejected(StartRejection::LaunchRequestConflict)
        );
        assert!(!fixture.snapshot_program.exists());
    }

    #[test]
    fn delayed_receipt_after_source_deletion_promotes_without_a_second_external_write() {
        let fixture = NexusFixture::new();
        let profile = LaunchProfile {
            launch_request_id: "delayed-request".into(),
            system_modules: vec!["spirit-core".into(), "intent-flow".into()],
            user_modules: vec!["operation-main-flow".into()],
            address: signal_flow::Address {
                aspect: Aspect::Field,
                topic: "flow".into(),
                layer: signal_flow::Layer::Secondary,
            },
            model_name: "fixture-model".into(),
            effort: "medium".into(),
            flow_id_option: None,
            remembered_flow_vector: Vec::new(),
            herdr_session_name: "fixture-session".into(),
            leave_outs_option: None,
            instruction_prompt: "fixture instruction".into(),
        };
        let origin = Origin::Flow(FlowOrigin {
            flow_id: "caller".into(),
            session_id: "caller-session".into(),
        });
        let composed = fixture
            .nexus
            .composer
            .compose(
                crate::composition::HarnessedProfile {
                    harness_kind: HarnessKind::Codex,
                    launch_profile: profile.clone(),
                },
                Vec::<signal_flow::LeaveOut>::new(),
            )
            .unwrap();
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
                system_prompt_sha256: composed.system_prompt.system_prompt_sha256.clone(),
                harness_kind: HarnessKind::Codex,
                model_name: profile.model_name.clone(),
                effort: profile.effort.clone(),
                prompt_sha256_vector: composed
                    .first_prompt_payload
                    .prompt_part_vector
                    .iter()
                    .map(|part| part.prompt_sha256.clone())
                    .collect(),
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
                origin: origin.clone(),
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
            prompt_sha256_vector: composed
                .first_prompt_payload
                .prompt_part_vector
                .iter()
                .map(|part| part.prompt_sha256.clone())
                .collect(),
            parts_submitted: 1,
            stops_reported: 0,
            leave_outs: Vec::new(),
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
        // The seat is really there: its pane answers `agent get` and the
        // session snapshot shows it idle.
        let snapshot = serde_json::json!({
            "id":"cli:api:snapshot",
            "result":{"snapshot":{"agents":[serde_json::json!({
                "agent":"codex",
                "agent_status":"idle",
                "interactive_ready":true,
                "name":"fixture-agent",
                "pane_id":"w1:p1",
                "revision":1,
                "terminal_id":"fixture-terminal",
                "workspace_id":"fixture-workspace"
            })],"protocol":20,"version":"0.8.2"},
            "type":"session_snapshot"}
        });
        let body = format!(
            "#!/bin/sh\nprintf '%s\\n' \"$*\" >> '{}'\ncase \"$*\" in\n  \"--session fixture-session agent get w1:p1\") printf '%s\\n' '{}' ;;\n  *\" api snapshot\") printf '%s\\n' '{}' ;;\n  *\" agent prompt \"*) exit 0 ;;\n  *\" agent read \"*) printf '%s\\n' '› ' ;;\n  *) exit 64 ;;\nesac\n",
            herdr_calls.display(),
            agent,
            snapshot
        );
        FixtureExecutable {
            path: fixture.snapshot_program.clone(),
        }
        .install(&body);

        let transcript = transcript_root.join(format!("rollout-{native_session_id}.jsonl"));
        let mut output = fs::File::create(transcript).unwrap();
        for row in [
            serde_json::json!({"type":"session_meta","payload":{"id":native_session_id}}),
            serde_json::json!({"type":"turn_context","payload":{"model":"fixture-model","effort":"medium","turn_id":"turn-delayed"}}),
            serde_json::json!({"type":"event_msg","payload":{"thread_id":native_session_id,"turn_id":"turn-delayed","item":{"type":"UserMessage","content":[{"type":"text","text":composed.first_prompt_payload.first_prompt_body}]}}}),
            serde_json::json!({"type":"response_item","payload":{"type":"message","role":"assistant","content":[{"type":"output_text","text":"Beginning."}]}}),
        ] {
            writeln!(output, "{row}").unwrap();
        }
        output.sync_all().unwrap();

        assert!(matches!(
            fixture.nexus.dispatch_from(Query::Start(profile), origin.clone()),
            Response::Started(started)
                if started.flow_id == "908786" && started.session_id == native_session_id
        ));
        let calls = fs::read_to_string(herdr_calls).unwrap();
        // The receipt was promoted from what was already written: no second
        // external write, and the first prompt is never re-typed.
        nothing_was_typed_after_the_first_prompt(&calls);
    }

    /// A seat Flow never stopped can still be gone: its pane leaves Herdr and
    /// nothing can reach it. List used to answer such a flow Pending on the
    /// route it was bound to, which is what made a gone seat look launchable.
    /// It is reported `Exited` — the seat's own going — with no route, its row
    /// and history kept. It is never `Retired`: an exit retains the record and
    /// retirement is an act of authority, not an inference from a pane.
    #[test]
    fn a_flow_whose_pane_left_herdr_is_listed_exited_and_keeps_its_row() {
        let fixture = NexusFixture::new();
        fixture.set_agents(vec![fixture.current_agent()]);
        let node = fixture.node();
        assert!(matches!(
            fixture
                .nexus
                .dispatch_meta(meta_signal_flow::Query::RegisterFlow(node.clone())),
            meta_signal_flow::Response::FlowRegistered(_)
        ));
        assert_eq!(fixture.only_lifecycle(), FlowLifecycle::Active);

        // The pane exits. Nothing else changes.
        fixture.set_agents(vec![]);

        use crate::store::ReadsFlowRows as _;
        let rows = fixture.nexus.store.flow_nodes().expect("Flow rows read");
        let rows = fixture.nexus.reconciled(rows);
        assert_eq!(rows.len(), 1, "the row and its history stay");
        assert_eq!(rows[0].flow_id, "908786");
        assert_eq!(rows[0].flow_lifecycle, FlowLifecycle::Exited);
        assert_ne!(
            rows[0].flow_lifecycle,
            FlowLifecycle::Retired,
            "a pane going away never retires a flow"
        );
        assert_eq!(
            rows[0].herdr_route_selection,
            HerdrRouteSelection::Unavailable
        );
        assert_eq!(rows[0].endpoint_selection, EndpointSelection::Unavailable);
        assert_eq!(rows[0].origin, node.origin, "its provenance is kept");

        // List wrote nothing: the stored row still says what Flow recorded.
        assert_eq!(
            fixture.stored_lifecycle(),
            FlowLifecycle::Active,
            "List must not write"
        );

        // A Deliver is a command, and it does record the exit it witnesses.
        assert_eq!(
            fixture.send("to a seat that is gone"),
            Sent::Refused(meta_signal_flow::DeliveryRejection::RouteUnavailable)
        );
        assert_eq!(fixture.stored_lifecycle(), FlowLifecycle::Exited);
        assert_eq!(
            fixture.send("again"),
            Sent::Refused(meta_signal_flow::DeliveryRejection::FlowExited)
        );
        assert_eq!(
            fixture
                .nexus
                .dispatch(Query::ResolveRecipient("908786".into())),
            Response::RecipientResolutionRejected(
                signal_flow::RecipientResolutionRejection::FlowUnavailable
            )
        );
    }

    /// A Herdr that cannot be read never retires a flow: the pane's fate is
    /// unknown, so the row is answered as it stands.
    #[test]
    fn an_unreadable_herdr_leaves_a_listed_flow_as_it_stands() {
        let fixture = NexusFixture::new();
        fixture.set_agents(vec![fixture.current_agent()]);
        assert!(matches!(
            fixture
                .nexus
                .dispatch_meta(meta_signal_flow::Query::RegisterFlow(fixture.node())),
            meta_signal_flow::Response::FlowRegistered(_)
        ));
        FixtureExecutable {
            path: fixture.snapshot_program.clone(),
        }
        .install("#!/bin/sh\necho 'herdr: server unreachable' >&2\nexit 1\n");

        assert_eq!(fixture.only_lifecycle(), FlowLifecycle::Active);
    }

    /// A seat bound through the privileged contract, or started before Flow
    /// Nexus, has no Presented Deliver to promote it, and so stayed Pending for
    /// as long as it lived. Herdr showing its bound pane present is a witness
    /// that it is live, and List reports so — without writing: List is a
    /// query, and a query does not change what it is asked about.
    #[test]
    fn a_pending_seat_whose_pane_herdr_shows_live_is_listed_active() {
        let fixture = NexusFixture::new();
        fixture.set_agents(vec![fixture.current_agent()]);
        fixture.register_with(FlowLifecycle::Pending);
        assert_eq!(fixture.stored_lifecycle(), FlowLifecycle::Pending);

        assert_eq!(fixture.only_lifecycle(), FlowLifecycle::Active);
        // Listing it again, and again, changes nothing in the store.
        assert_eq!(fixture.only_lifecycle(), FlowLifecycle::Active);
        assert_eq!(
            fixture.stored_lifecycle(),
            FlowLifecycle::Pending,
            "List must not write"
        );
    }

    /// Flow had no way to retire a flow at all, so a seat retired elsewhere
    /// stayed listed as if it were launchable. Retire is privileged, keeps the
    /// row and its history, and takes the flow out of receiving. It is the
    /// only thing that produces Retired: no observation ever does.
    #[test]
    fn an_operation_is_answered_by_its_own_outcome() {
        use crate::generated::operation::{Failed_Data, Operation, Outcome, Record_Data};
        use crate::performing::Performs;
        let fixture = NexusFixture::new();
        fixture.set_agents(vec![fixture.current_agent()]);
        fixture.register_with(FlowLifecycle::Active);

        // A record of a flow the store does not hold is refused, not made.
        assert_eq!(
            fixture
                .nexus
                .perform(Operation::Record(Record_Data::Stopped("unknown".into()))),
            Outcome::Failed(Failed_Data::StoreRefused)
        );
        // Confirming a launch the store never bound does not start it.
        assert_eq!(
            fixture.nexus.perform(Operation::Confirm("unknown".into())),
            Outcome::Failed(Failed_Data::Unstarted)
        );
        // A witnessed exit of a live flow is recorded, and the row says so.
        assert_eq!(
            fixture
                .nexus
                .perform(Operation::Record(Record_Data::Exited("908786".into()))),
            Outcome::Recorded
        );
        assert_eq!(fixture.stored_lifecycle(), FlowLifecycle::Exited);
        // An exit is recorded once: the flow is no longer live.
        assert_eq!(
            fixture
                .nexus
                .perform(Operation::Record(Record_Data::Exited("908786".into()))),
            Outcome::Failed(Failed_Data::StoreRefused)
        );
    }

    /// Ruling 12 of flow f1c841: a harness event reaches Flow as
    /// `Report.{ FlowId Event }`, recorded in the flow's Memory and answered
    /// Reported; a FlowId Flow does not hold is refused and never adopted.
    /// The owner reads the events back with ReadEvents.
    #[test]
    fn a_report_is_recorded_for_a_held_flow_and_refused_for_an_unknown_one() {
        use signal_flow::{Event, Report_Data};
        let fixture = NexusFixture::new();
        fixture.set_agents(vec![fixture.current_agent()]);
        fixture.register_with(FlowLifecycle::Active);
        let report = |flow_id: &str, event: Event| {
            fixture.nexus.dispatch(Query::Report(Report_Data {
                flow_id: flow_id.into(),
                event,
            }))
        };

        assert_eq!(
            report("0a0a0a", Event::Started),
            Response::Refused(signal_flow::Refusal::Unknown(
                signal_flow::Unknown_Data::FlowId("0a0a0a".into())
            ))
        );
        assert_eq!(
            fixture
                .nexus
                .dispatch_meta(meta_signal_flow::Query::ReadEvents("0a0a0a".into())),
            meta_signal_flow::Response::ReadEventsRejected(
                meta_signal_flow::ReadEventsRejected_Data::UnknownFlow
            ),
            "the refused flow was not adopted"
        );
        assert_eq!(report("908786", Event::Started), Response::Reported);
        assert_eq!(
            report("908786", Event::ToolUsed("Bash".into())),
            Response::Reported
        );
        let stop = Event::Stopped(signal_flow::NativeStop {
            native_session_id_option: Some("other-session".into()),
            transcript_path_option: None,
        });
        assert_eq!(report("908786", stop.clone()), Response::Reported);
        assert_eq!(
            fixture
                .nexus
                .dispatch_meta(meta_signal_flow::Query::ReadEvents("908786".into())),
            meta_signal_flow::Response::EventsRead(meta_signal_flow::EventsRead_Data {
                flow_id: "908786".into(),
                event_vector: vec![Event::Started, Event::ToolUsed("Bash".into()), stop],
            })
        );
    }

    #[test]
    fn retire_keeps_the_row_and_takes_the_flow_out_of_receiving() {
        let fixture = NexusFixture::new();
        fixture.set_agents(vec![fixture.current_agent()]);
        let node = fixture.node();
        fixture.register_with(FlowLifecycle::Active);

        assert_eq!(
            fixture
                .nexus
                .dispatch_meta(meta_signal_flow::Query::Retire("unknown".into())),
            meta_signal_flow::Response::RetireRejected(
                meta_signal_flow::RetireRejection::UnknownFlow
            )
        );

        let meta_signal_flow::Response::FlowRetired(retired) = fixture
            .nexus
            .dispatch_meta(meta_signal_flow::Query::Retire("908786".into()))
        else {
            panic!("a live flow is retired")
        };
        assert_eq!(retired.flow_lifecycle, FlowLifecycle::Retired);
        assert_eq!(retired.flow_id, "908786");
        assert_eq!(retired.session_id, node.session_id, "its history is kept");
        assert_eq!(retired.origin, node.origin);

        // Retiring it again changes nothing and says so.
        assert_eq!(
            fixture
                .nexus
                .dispatch_meta(meta_signal_flow::Query::Retire("908786".into())),
            meta_signal_flow::Response::RetireRejected(
                meta_signal_flow::RetireRejection::AlreadyGone
            )
        );

        // Its pane is still live in Herdr, and it stays Retired anyway:
        // retirement is a decision about the flow, not a reading of Herdr,
        // and List does not talk it back into being live.
        assert_eq!(fixture.only_lifecycle(), FlowLifecycle::Retired);
        assert_eq!(fixture.stored_lifecycle(), FlowLifecycle::Retired);
        assert_eq!(
            fixture.send("to a retired flow"),
            Sent::Refused(meta_signal_flow::DeliveryRejection::FlowRetired)
        );
        assert_eq!(
            fixture
                .nexus
                .dispatch(Query::ResolveRecipient("908786".into())),
            Response::RecipientResolutionRejected(
                signal_flow::RecipientResolutionRejection::FlowUnavailable
            )
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
        origin: Origin,
        composed: signal_flow::ComposedLaunch,
        pane: HerdrPaneBinding,
        binding: NativeLaunchBinding,
        intent: Option<PromptDeliveryIntent>,
        transcript_root: PathBuf,
    }

    const STAGED_SESSION: &str = "01a0b22c-e24f-7452-9940-64490878680f";

    impl NexusFixture {
        /// Declares the model a profile names as the one model configured,
        /// for its Address's layer and aspect, as a running Nexus's
        /// configuration would before such a Start.
        fn configure_model_of(&self, profile: &LaunchProfile) {
            use crate::store::ManagesMetaflowState as _;
            self.nexus
                .store
                .configure_change(meta_signal_flow::ConfigurationChange::Models(vec![
                    meta_signal_flow::ConfigureModel {
                        layer: profile.address.layer.clone(),
                        aspect: profile.address.aspect.clone(),
                        native: profile.model_name.clone(),
                    },
                ]))
                .expect("model configured");
        }

        fn staged_launch(
            &self,
            launch_request_id: &str,
            predecessor: Option<&str>,
        ) -> StagedLaunch {
            let profile = LaunchProfile {
                launch_request_id: launch_request_id.into(),
                system_modules: vec!["spirit-core".into(), "intent-flow".into()],
                user_modules: vec!["operation-main-flow".into()],
                // Each staged launch is the seat of its own Address.
                address: signal_flow::Address {
                    aspect: Aspect::Field,
                    topic: launch_request_id.into(),
                    layer: signal_flow::Layer::Secondary,
                },
                model_name: "fixture-model".into(),
                effort: "medium".into(),
                flow_id_option: predecessor.map(str::to_owned),
                remembered_flow_vector: Vec::new(),
                herdr_session_name: "fixture-session".into(),
                leave_outs_option: None,
                instruction_prompt: "fixture instruction".into(),
            };
            let composed = self
                .nexus
                .composer
                .compose(
                    crate::composition::HarnessedProfile {
                        harness_kind: HarnessKind::Codex,
                        launch_profile: profile.clone(),
                    },
                    Vec::<signal_flow::LeaveOut>::new(),
                )
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
            self.configure_model_of(&profile);
            StagedLaunch {
                binding: NativeLaunchBinding {
                    launch_request_id: launch_request_id.into(),
                    flow_id: "908786".into(),
                    native_session_id: STAGED_SESSION.into(),
                    harness_kind: HarnessKind::Codex,
                    herdr_pane_binding: pane.clone(),
                },
                profile,
                origin: Origin::Unbound,
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

        /// The successor's own pane as a snapshot shows it: the staged
        /// launch binds w1:p1 on the fixture terminal. Every replacement
        /// stand-in shows it, because the successor's seat is really there;
        /// only the predecessor's presence varies between these tests.
        fn successor_agent(&self) -> serde_json::Value {
            serde_json::json!({
                "agent":"codex",
                "agent_status":"working",
                "interactive_ready":true,
                "name":"fixture-agent",
                "pane_id":"w1:p1",
                "revision":1,
                "terminal_id":"fixture-terminal",
                "workspace_id":"fixture-workspace",
                "agent_session":{
                    "source":"herdr:codex","agent":"codex","kind":"id",
                    "value":STAGED_SESSION
                }
            })
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
            let mut agents = agents;
            agents.push(self.successor_agent());
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
                "result":{"snapshot":{"agents":agents.clone(),"protocol":20,"version":"0.8.2"},
                "type":"session_snapshot"}
            });
            let body = format!(
                "#!/bin/sh\nprintf '%s\\n' \"$*\" >> '{}'\ncase \"$*\" in\n  \"--session fixture-session agent get w1:p1\") printf '%s\\n' '{}' ;;\n  *\" api snapshot\") printf '%s\\n' '{}' ;;\n  *\" pane close \"*) exit {} ;;\n  *\" agent read \"*) printf '%s\\n' '› ' ;;\n  *) exit 64 ;;\nesac\n",
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
            use crate::store::ReadsFlowRows as _;
            self.nexus
                .store
                .flow_node(flow_id)
                .expect("Flow row read")
                .expect("flow row")
                .flow_lifecycle
        }
    }

    impl StagedLaunch {
        /// The profile a Start or Replace carries; an ordinary dispatch
        /// with no calling flow derives the Unbound origin.
        fn request(&self) -> LaunchProfile {
            self.profile.clone()
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
                        system_prompt_sha256: self
                            .composed
                            .system_prompt
                            .system_prompt_sha256
                            .clone(),
                        harness_kind: HarnessKind::Codex,
                        model_name: self.profile.model_name.clone(),
                        effort: self.profile.effort.clone(),
                        prompt_sha256_vector: self
                            .composed
                            .first_prompt_payload
                            .prompt_part_vector
                            .iter()
                            .map(|part| part.prompt_sha256.clone())
                            .collect(),
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

        /// Registers the bound flow in the role its profile names, as
        /// Start does, and journals the acknowledgement, which takes the
        /// launch's Address.
        fn register_and_acknowledge(&self, nexus: &RunningNexus) {
            nexus
                .store
                .register_flow_in_role(
                    FlowNode {
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
                        origin: self.origin.clone(),
                        flow_lifecycle: FlowLifecycle::Pending,
                    },
                    signal_flow::Caller {
                        flow_id: self.binding.flow_id.clone(),
                        aspect: self.profile.address.aspect.clone(),
                        power_level: <RunningNexus as crate::waking::WakePolicy>::power_of(
                            &self.profile.address.layer,
                        ),
                        model_name: self.profile.model_name.clone(),
                    },
                )
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
                prompt_sha256_vector: self
                    .composed
                    .first_prompt_payload
                    .prompt_part_vector
                    .iter()
                    .map(|part| part.prompt_sha256.clone())
                    .collect(),
                parts_submitted: 1,
                stops_reported: 0,
                leave_outs: Vec::new(),
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

        /// The harness writes the authenticated first turn: the receipt.
        fn write_receipt(&self) {
            let mut rows = String::new();
            for row in [
                serde_json::json!({"type":"session_meta","payload":{"id":STAGED_SESSION}}),
                serde_json::json!({"type":"turn_context","payload":{"model":"fixture-model","effort":"medium","turn_id":"turn-staged"}}),
                serde_json::json!({"type":"event_msg","payload":{"thread_id":STAGED_SESSION,"turn_id":"turn-staged","item":{"type":"UserMessage","content":[{"type":"text","text":self.composed.first_prompt_payload.first_prompt_body}]}}}),
                // The flow's first answer: the receipt.
                serde_json::json!({"type":"response_item","payload":{"type":"message","role":"assistant","content":[{"type":"output_text","text":"Beginning."}]}}),
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
            fixture.send_to("908786", "too early"),
            Sent::Refused(meta_signal_flow::DeliveryRejection::RouteUnavailable)
        );
        assert!(matches!(
            fixture.nexus.dispatch(Query::LaunchStatus("replace-request".into())),
            Response::LaunchInspected(inspection)
                if inspection.launch_attempt.launch_attempt_phase
                    == LaunchAttemptPhase::PromptAmbiguous
                    && inspection.launch_status_outcome_option.is_none()
                    && inspection.flow_node_option.as_ref()
                        .is_some_and(|node| node.flow_id == "908786")
        ));

        launch.write_receipt();
        let Response::Replaced(replaced) = fixture.nexus.dispatch(Query::Replace(launch.request()))
        else {
            panic!("the observed successor replaces its predecessor")
        };
        assert_eq!(replaced.flow_id, "fac697");
        assert_eq!(replaced.launched.flow_id, "908786");
        assert_eq!(replaced.launched.session_id, STAGED_SESSION);

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
            fixture.send_to("fac697", "to the old seat"),
            Sent::Refused(meta_signal_flow::DeliveryRejection::FlowStopped)
        );
        assert_inspection_outcome(
            fixture
                .nexus
                .dispatch(Query::LaunchStatus("replace-request".into())),
            "replace-request",
            LaunchStatusOutcome::Replaced(replaced.clone()),
            Some("908786"),
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
        nothing_was_typed_after_the_first_prompt(&calls);
    }

    #[test]
    fn a_refused_reap_leaves_neither_flow_routable_until_it_is_retried() {
        let fixture = NexusFixture::new();
        fixture.herdr_for_replacement(1);
        fixture.register_predecessor("fac697");
        let mut launch = fixture.staged_launch("reap-request", Some("fac697"));
        launch.stage_to_ambiguity(&fixture.nexus);
        launch.write_receipt();

        let rejection =
            signal_flow::ReplaceRejection::ReapRefused(signal_flow::StopRejection::CloseRefused);
        let refused = Response::ReplaceRejected(rejection.clone());
        assert_eq!(
            fixture.nexus.dispatch(Query::Replace(launch.request())),
            refused
        );
        assert_eq!(fixture.lifecycle("fac697"), FlowLifecycle::Stopped);
        assert!(!fixture.routable("fac697"));
        assert!(!fixture.routable("908786"));
        assert_inspection_outcome(
            fixture
                .nexus
                .dispatch(Query::LaunchStatus("reap-request".into())),
            "reap-request",
            LaunchStatusOutcome::ReplaceRejected(rejection),
            Some("908786"),
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
        let rejection = signal_flow::ReplaceRejection::ReapRefused(
            signal_flow::StopRejection::RouteUnavailable,
        );
        let refused = Response::ReplaceRejected(rejection.clone());
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
        assert_inspection_outcome(
            fixture
                .nexus
                .dispatch(Query::LaunchStatus("unreachable-request".into())),
            "unreachable-request",
            LaunchStatusOutcome::ReplaceRejected(rejection),
            Some("908786"),
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
        assert_inspection_outcome(
            fixture
                .nexus
                .dispatch(Query::LaunchStatus("exited-request".into())),
            "exited-request",
            LaunchStatusOutcome::Replaced(match replaced {
                Response::Replaced(replaced) => replaced,
                other => panic!("expected replaced outcome, got {other:?}"),
            }),
            Some("908786"),
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
        fixture
            .nexus
            .store
            .record_launch_outcome(
                "outcome-without-attempt",
                LaunchOutcome::StartRejected(StartRejection::CompositionRefused),
            )
            .unwrap();
        assert_eq!(
            fixture
                .nexus
                .dispatch(Query::LaunchStatus("outcome-without-attempt".into())),
            Response::LaunchStatusRejected(signal_flow::LaunchStatusRejection::InconsistentRecords)
        );
        assert!(
            !fixture.snapshot_program.exists(),
            "no launch was attempted"
        );
    }

    /// 42265e: a launch registered at its Address but never confirmed was
    /// Stopped; the Address came back, yet the unsettled attempt kept
    /// refusing every fresh Start there. A launch whose flow has ended is
    /// settled by that end, and the Address takes a fresh launch.
    #[test]
    fn an_unconfirmed_launch_stopped_leaves_its_address_to_a_fresh_start() {
        use crate::store::ReadsLaunchAttempt as _;
        use crate::store::address_hold::HoldsLaunchAddress as _;
        use crate::store::metaflow::SleepsMetaflows as _;
        use crate::store::metaflow::TakesLaunchAddress as _;
        let fixture = NexusFixture::new();
        fixture.herdr_for_replacement(0);
        let mut seat = fixture.staged_launch("seat-request", None);
        let address = seat.profile.address.clone();
        seat.stage_to_ambiguity(&fixture.nexus);
        let mut fresh = fixture.staged_launch("fresh-request", None);
        fresh.profile.address = address.clone();
        assert_eq!(
            fixture
                .nexus
                .store
                .launch_address_refusal(&fresh.profile)
                .unwrap(),
            Some(StartRejection::AddressAwake),
            "the unconfirmed launch holds the Address while its flow lives"
        );

        assert!(matches!(
            fixture.nexus.dispatch(Query::Stop("908786".into())),
            Response::Stopped(_)
        ));
        assert_eq!(fixture.lifecycle("908786"), FlowLifecycle::Stopped);
        assert_eq!(fixture.nexus.store.metaflow(&address).unwrap(), None);
        assert_eq!(fixture.nexus.store.address_hold("908786").unwrap(), None);
        assert_eq!(
            fixture.nexus.store.launch_outcome("seat-request").unwrap(),
            None,
            "the stopped launch has no outcome, as the live store holds it"
        );
        assert_eq!(
            fixture
                .nexus
                .store
                .launch_address_refusal(&fresh.profile)
                .unwrap(),
            None,
            "a fresh Start at the Address is not refused"
        );
        assert!(
            fixture
                .nexus
                .store
                .ambiguous_launch_attempts()
                .unwrap()
                .is_empty(),
            "a stopped launch is no longer watched for its receipt"
        );
    }

    /// A store an older Nexus left with a hold and an Awake metaflow behind
    /// an ended flow: the next Start at that Address gives the hold back
    /// before it reads the Address, with no hand edit.
    #[test]
    fn a_stale_hold_behind_an_ended_flow_is_given_back_at_start() {
        use crate::store::address_hold::HoldsLaunchAddress as _;
        use crate::store::address_hold::LeavesStaleHold as _;
        use crate::store::metaflow::SleepsMetaflows as _;
        let fixture = NexusFixture::new();
        fixture.herdr_for_replacement(0);
        let mut seat = fixture.staged_launch("seat-request", None);
        let address = seat.profile.address.clone();
        seat.stage_to_ambiguity(&fixture.nexus);
        assert!(fixture.nexus.store.record_stopped("908786").unwrap());
        fixture.nexus.store.leave_stale_hold("908786", &address);
        assert!(
            fixture
                .nexus
                .store
                .address_hold("908786")
                .unwrap()
                .is_some()
        );

        let mut fresh = fixture.staged_launch("fresh-request", None);
        fresh.profile.address = address.clone();
        // The Start gives the stale hold back before anything launches; the
        // fixture's Herdr refuses what follows.
        let response = fixture.nexus.dispatch(Query::Start(fresh.request()));
        assert!(
            !matches!(
                response,
                Response::StartRejected(StartRejection::AddressAwake)
            ),
            "the Address is free: {response:?}"
        );
        assert_eq!(fixture.nexus.store.address_hold("908786").unwrap(), None);
        assert!(
            fixture
                .nexus
                .store
                .metaflow(&address)
                .unwrap()
                .is_none_or(|metaflow| metaflow.state != flow_ethos::State::Awake("908786".into())),
            "the ended flow no longer names the Address"
        );
    }

    /// A launch at an Address that is in flight before its native binding,
    /// while no metaflow exists yet, still refuses another Start there.
    #[test]
    fn a_launch_in_flight_before_binding_refuses_another_start_at_its_address() {
        use crate::store::metaflow::SleepsMetaflows as _;
        let fixture = NexusFixture::new();
        let calls = fixture.herdr_for_replacement(0);
        let seat = fixture.staged_launch("seat-request", None);
        let address = seat.profile.address.clone();
        seat.reserve(&fixture.nexus);
        seat.record_native_intent(&fixture.nexus);
        assert_eq!(fixture.nexus.store.metaflow(&address).unwrap(), None);
        let before = fs::read_to_string(&calls).unwrap_or_default();
        let mut second = fixture.staged_launch("second-request", None);
        second.profile.address = address;
        assert_eq!(
            fixture.nexus.dispatch(Query::Start(second.request())),
            Response::StartRejected(StartRejection::AddressAwake)
        );
        assert_eq!(
            fs::read_to_string(&calls).unwrap_or_default(),
            before,
            "the refused Start touches no pane"
        );
    }

    /// A receipt that contradicts the launch, here another model, refuses
    /// it: the launch is StartRejected, its pane closed, its flow Stopped
    /// and its Address given back, rather than left ambiguous.
    #[test]
    fn a_receipt_that_contradicts_the_launch_rejects_it() {
        use crate::store::address_hold::HoldsLaunchAddress as _;
        use crate::store::metaflow::SleepsMetaflows as _;
        let fixture = NexusFixture::new();
        let calls = fixture.herdr_for_replacement(0);
        let mut seat = fixture.staged_launch("seat-request", None);
        let address = seat.profile.address.clone();
        seat.stage_to_ambiguity(&fixture.nexus);
        let rows = [
            serde_json::json!({"type":"session_meta","payload":{"id":STAGED_SESSION}}),
            serde_json::json!({"type":"turn_context","payload":{"model":"other-model","effort":"medium","turn_id":"turn-staged"}}),
            serde_json::json!({"type":"event_msg","payload":{"thread_id":STAGED_SESSION,"turn_id":"turn-staged","item":{"type":"UserMessage","content":[{"type":"text","text":seat.composed.first_prompt_payload.first_prompt_body}]}}}),
        ]
        .iter()
        .map(|row| format!("{row}\n"))
        .collect::<String>();
        fs::write(
            seat.transcript_root
                .join(format!("rollout-{STAGED_SESSION}.jsonl")),
            rows,
        )
        .expect("transcript written");

        let model_differs = StartRejection::NativeLaunchRefused(signal_flow::NativeLaunchRefusal {
            native_launch_stage: signal_flow::NativeLaunchStage::Receipt,
            native_launch_failure: signal_flow::NativeLaunchFailure::ModelOrEffortDiffers,
        });
        assert_eq!(
            fixture.nexus.dispatch(Query::Start(seat.request())),
            Response::StartRejected(model_differs.clone())
        );
        assert_eq!(
            fixture.nexus.store.launch_outcome("seat-request").unwrap(),
            Some(LaunchOutcome::StartRejected(model_differs))
        );
        assert_eq!(fixture.lifecycle("908786"), FlowLifecycle::Stopped);
        assert!(
            fs::read_to_string(&calls)
                .unwrap_or_default()
                .contains("pane close"),
            "the refused launch's pane is closed"
        );
        assert_eq!(fixture.nexus.store.address_hold("908786").unwrap(), None);
        assert_eq!(fixture.nexus.store.metaflow(&address).unwrap(), None);
    }

    /// A launch names the Address of its seat: the write acknowledging the
    /// seat's witnessed binding records that metaflow Awake as the launched
    /// flow and holds it, confirmation keeps it in the write that makes the
    /// flow Active, and a Start at an Address a live seat holds is refused
    /// before anything launches.
    #[test]
    fn a_confirmed_launch_takes_its_address_and_a_held_address_refuses_the_next_start() {
        use crate::store::address_hold::HoldsLaunchAddress as _;
        use crate::store::metaflow::SleepsMetaflows as _;
        let fixture = NexusFixture::new();
        let calls = fixture.herdr_for_replacement(0);
        let mut seat = fixture.staged_launch("seat-request", None);
        let address = seat.profile.address.clone();
        seat.stage_to_ambiguity(&fixture.nexus);
        let held = fixture
            .nexus
            .store
            .metaflow(&address)
            .unwrap()
            .expect("the acknowledged binding took the Address");
        assert_eq!(held.state, flow_ethos::State::Awake("908786".into()));
        assert!(
            fixture
                .nexus
                .store
                .address_hold("908786")
                .unwrap()
                .is_some(),
            "until confirmation the launch only holds it"
        );
        let mut second = fixture.staged_launch("second-request", None);
        second.profile.address = address.clone();
        assert_eq!(
            fixture.nexus.dispatch(Query::Start(second.request())),
            Response::StartRejected(StartRejection::AddressAwake),
            "another unsettled launch names the Address"
        );

        seat.write_receipt();
        let Response::Started(started) = fixture.nexus.dispatch(Query::Start(seat.request()))
        else {
            panic!("the observed launch starts")
        };
        let metaflow = fixture
            .nexus
            .store
            .metaflow(&address)
            .unwrap()
            .expect("the launch recorded its metaflow");
        assert_eq!(
            metaflow.state,
            flow_ethos::State::Awake(started.flow_id.clone())
        );
        assert!(metaflow.past.is_empty());
        assert_eq!(
            fixture.nexus.store.address_hold(&started.flow_id).unwrap(),
            None,
            "confirmation keeps the Address and drops the hold"
        );
        assert_eq!(
            fixture
                .nexus
                .store
                .metaflow_awake_as(&started.flow_id)
                .unwrap(),
            Some(metaflow)
        );

        let launched = fs::read_to_string(&calls).unwrap_or_default();
        assert_eq!(
            fixture.nexus.dispatch(Query::Start(second.request())),
            Response::StartRejected(StartRejection::AddressAwake),
            "the Address is Awake as a live flow"
        );
        assert_eq!(
            fs::read_to_string(&calls).unwrap_or_default(),
            launched,
            "a refused Start touches no pane"
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
        let pending = inspected(
            fixture
                .nexus
                .dispatch(Query::LaunchStatus("started-request".into())),
            "started-request",
        );
        assert_eq!(
            pending.launch_attempt.launch_attempt_phase,
            LaunchAttemptPhase::PromptAmbiguous
        );
        assert!(pending.launch_status_outcome_option.is_none());
        started.write_receipt();
        let Response::Started(outcome) = fixture.nexus.dispatch(Query::Start(started.request()))
        else {
            panic!("the observed launch starts")
        };
        assert_inspection_outcome(
            fixture
                .nexus
                .dispatch(Query::LaunchStatus("started-request".into())),
            "started-request",
            LaunchStatusOutcome::Started(outcome),
            Some("908786"),
        );

        // StartRejected after reservation: Herdr cannot open the pane. The
        // Claude launch's FlowId is claimed at Reserve, before the pane.
        fixture.claims_flow_ids();
        fs::remove_file(&fixture.snapshot_program).expect("Herdr withdrawn");
        let mut rejected = fixture.staged_launch("rejected-request", None);
        rejected.profile.model_name = "claude-fixture".into();
        fixture.configure_model_of(&rejected.profile);
        let rejection = fixture.nexus.dispatch(Query::Start(rejected.request()));
        assert_eq!(rejection, Response::StartRejected(pane_unopened()));
        assert_inspection_outcome(
            fixture
                .nexus
                .dispatch(Query::LaunchStatus("rejected-request".into())),
            "rejected-request",
            LaunchStatusOutcome::StartRejected(pane_unopened()),
            None,
        );
        assert_eq!(
            fixture.nexus.dispatch(Query::Start(rejected.request())),
            rejection,
            "a settled rejection is not launched again"
        );
        // Nothing composed was written to disk for it.
        assert!(!fixture.directory.path().join("launch-bundles").exists());

        // ReplaceRejected: the successor's launch is refused; the
        // predecessor keeps receiving.
        fixture.register_predecessor("fac697");
        let mut refused = fixture.staged_launch("refused-request", Some("fac697"));
        refused.profile.model_name = "claude-fixture".into();
        fixture.configure_model_of(&refused.profile);
        let rejection = fixture.nexus.dispatch(Query::Replace(refused.request()));
        assert_eq!(
            rejection,
            Response::ReplaceRejected(
                signal_flow::ReplaceRejection::LaunchRefused(pane_unopened())
            )
        );
        assert_inspection_outcome(
            fixture
                .nexus
                .dispatch(Query::LaunchStatus("refused-request".into())),
            "refused-request",
            LaunchStatusOutcome::ReplaceRejected(signal_flow::ReplaceRejection::LaunchRefused(
                pane_unopened(),
            )),
            None,
        );
        assert_eq!(fixture.lifecycle("fac697"), FlowLifecycle::Active);
        // Replaced is answered by the replacement fixture above.
    }

    /// e167d8: a plain Start first answered StartAmbiguous and only the
    /// watcher promoted it later, so every caller of a launch that was in
    /// fact succeeding had to handle a non-failure. The ambiguity is not
    /// intrinsic — the Nexus is already watching the seat's transcript — so
    /// Start waits for that settlement and answers it. The caller sees
    /// Started; StartAmbiguous is left for a seat that never answers.
    #[test]
    fn a_plain_start_answers_started_rather_than_the_transient_ambiguity() {
        use crate::launching::PromotesAmbiguousLaunches;
        let fixture: &'static NexusFixture = Box::leak(Box::new(NexusFixture::new()));
        let calls = fixture.herdr_for_replacement(0);
        let mut launch = fixture.staged_launch("settling-request", None);
        launch.stage_to_ambiguity(&fixture.nexus);
        let request = launch.request();
        let looks = || {
            fs::read_to_string(&calls)
                .unwrap_or_default()
                .lines()
                .count()
        };
        let before_start = looks();

        // No promoter runs yet, so Start is the only thing that can look at
        // the transcript: the log growing is Start having looked and found
        // no receipt, which is exactly where it used to answer ambiguity.
        let answering = std::thread::spawn(move || {
            fixture
                .nexus
                .dispatch_serially(Query::Start(request), Origin::Unbound)
        });
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while looks() == before_start {
            assert!(
                std::time::Instant::now() < deadline,
                "Start never looked for the receipt"
            );
            std::thread::sleep(Duration::from_millis(10));
        }

        // Only now does the seat answer its receipt, and only now is there
        // anything watching for it.
        launch.write_receipt();
        std::thread::spawn(move || fixture.nexus.promote_ambiguous_launches());

        let answer = answering.join().expect("Start answered");
        let Response::Started(started) = answer else {
            panic!("a launch that succeeded answers Started, not {answer:?}")
        };
        assert_eq!(started.flow_id, "908786");
        assert_inspection_outcome(
            fixture
                .nexus
                .dispatch(Query::LaunchStatus("settling-request".into())),
            "settling-request",
            LaunchStatusOutcome::Started(started),
            Some("908786"),
        );
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
        let pending = inspected(status(), "unwatched-request");
        assert_eq!(
            pending.launch_attempt.launch_attempt_phase,
            LaunchAttemptPhase::PromptAmbiguous
        );
        assert!(pending.launch_status_outcome_option.is_none());

        // Only the transcript moves.
        launch.write_receipt();
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        let started = loop {
            if let Response::LaunchInspected(inspection) = status()
                && let Some(LaunchStatusOutcome::Started(started)) =
                    inspection.launch_status_outcome_option
            {
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
        let calls = fs::read_to_string(&calls).unwrap_or_default();
        nothing_was_typed_after_the_first_prompt(&calls);
        assert!(!calls.contains("pane create"), "{calls}");
    }

    #[test]
    fn a_launch_observer_receives_each_phase_once_and_the_outcome_last() {
        use super::{CarriesSignalFrames, ServesOrdinary};
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
                    peer.write_query(&Query::Observe(signal_flow::ObserveSelection::Launch(
                        "observed-request".into(),
                    )))
                    .expect("Observe written");
                    return peer;
                }
                std::thread::sleep(Duration::from_millis(10));
            }
            panic!("ordinary socket never listened")
        };
        let phase = |peer: &mut UnixStream| match peer.read_response() {
            Ok(Response::LaunchInspected(inspection)) => {
                inspection.launch_attempt.launch_attempt_phase
            }
            other => panic!("expected a LaunchInspected frame, got {other:?}"),
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
        let Ok(Response::LaunchInspected(inspection)) = observer.read_response() else {
            panic!("the observer's last frame is the outcome")
        };
        let Some(LaunchStatusOutcome::Started(started)) = inspection.launch_status_outcome_option
        else {
            panic!("the observer's last inspection carries Started")
        };
        assert_eq!(started.flow_id, "908786");
        assert_eq!(
            inspection
                .flow_node_option
                .as_ref()
                .map(|node| node.flow_id.as_str()),
            Some("908786")
        );
        assert!(
            observer.read_response().is_err(),
            "the exchange ends after the outcome"
        );

        let mut late = subscribe();
        assert_inspection_outcome(
            late.read_response().expect("late observer answered"),
            "observed-request",
            LaunchStatusOutcome::Started(started),
            Some("908786"),
        );
        assert!(late.read_response().is_err());
        let calls = fs::read_to_string(calls).unwrap_or_default();
        nothing_was_typed_after_the_first_prompt(&calls);
        assert!(!calls.contains("pane create"), "{calls}");
    }

    /// Asserts the Herdr call log shows no typed prompt. The Codex first
    /// prompt travels over its app-server and carries the brief; once the
    /// launch is witnessed nothing more is typed into the seat's pane.
    fn nothing_was_typed_after_the_first_prompt(calls: &str) {
        assert_eq!(calls.matches("agent prompt").count(), 0, "{calls}");
    }

    fn bind_existing_caller(fixture: &NexusFixture, flow_id: &str, pane: &str) {
        let socket_path = fixture
            .directory
            .path()
            .join(format!("herdr-{flow_id}.sock"));
        let _listener = UnixListener::bind(&socket_path).expect("live Herdr fixture socket");
        let binding = existing_binding(flow_id, pane);
        let response = with_bound_world(fixture, &[&binding.clone()], || {
            fixture
                .nexus
                .dispatch_meta(meta_signal_flow::Query::MetaBindExisting(
                    meta_signal_flow::MetaBindExisting {
                        flow_container: flow_container(&socket_path),
                        flow_binding_vector: vec![binding],
                    },
                ))
        });
        assert!(matches!(
            response,
            meta_signal_flow::Response::BoundExisting(bound)
                if matches!(
                    bound.flow_binding_result_vector.as_slice(),
                    [meta_signal_flow::FlowBindingResult::Bound(_)]
                )
        ));
    }

    fn codex_agent_in(pane: &str, terminal: &str) -> serde_json::Value {
        serde_json::json!({
            "agent":"codex", "agent_status":"working", "name":format!("agent-{pane}"),
            "pane_id":pane, "terminal_id":terminal
        })
    }

    fn pane(pane: &str) -> Option<CallerPane> {
        Some(CallerPane {
            herdr_session_name: "messaging-build".into(),
            herdr_pane_id: pane.into(),
        })
    }

    fn mind_live() -> Caller {
        Caller {
            flow_id: "mind-live".into(),
            aspect: Aspect::Mind,
            power_level: PowerLevel::Medium,
            model_name: "gpt-sol".into(),
        }
    }

    /// A freshly spawned process is not yet marked. glibc's `posix_spawn`
    /// wakes the vfork parent from inside the child's `execve`, at
    /// `mm_release`, which the kernel reaches before it has laid the new
    /// environment into the new address space: for a few dozen microseconds
    /// `/proc/<pid>/environ` reads back empty (the `comm` is already the new
    /// program's). A reader cannot tell that from a scrubbed environment, so
    /// it would take the peer for the owner. Every fixture peer therefore
    /// waits for its own marks before it is used as one. The Nexus needs no
    /// such wait: its peers are processes that have already connected to it.
    trait SettlesItsMarks {
        /// A pane's marks appear within microseconds; this is only a bound.
        const SETTLING_LIMIT: Duration = Duration::from_secs(5);

        fn settled_pane_id(&self) -> String;
    }

    impl SettlesItsMarks for CallerProcess {
        fn settled_pane_id(&self) -> String {
            let deadline = std::time::Instant::now() + Self::SETTLING_LIMIT;
            loop {
                if let Some(pane_id) = self.marked_pane_id() {
                    return pane_id;
                }
                assert!(
                    std::time::Instant::now() < deadline,
                    "the fixture peer's Herdr marks never became readable"
                );
                std::thread::yield_now();
            }
        }
    }

    /// A process that sleeps in a pane of Herdr's default-session shape,
    /// killed on drop: it carries `HERDR_PANE_ID` alone, and its Herdr
    /// server ancestor is the test process, listening on the fixture
    /// session's socket.
    struct MarkedProcess(std::process::Child);

    impl MarkedProcess {
        /// The program is spawned directly, never through a shell that
        /// `exec`s it: `SettlesItsMarks` settles the marks of one process,
        /// and a second `execve` in that same process reopens the window it
        /// waited out — `/proc/<pid>/environ` reads back empty again, which
        /// a reader cannot tell from a scrubbed environment. A shell is
        /// spawned only where the shell itself is the marked process and
        /// stays (its command line ends in another word, so the shell does
        /// not turn its last command into an `exec` of its own accord).
        fn spawn(program: &str, arguments: &[&str], pane: &str) -> Self {
            let child = std::process::Command::new(program)
                .args(arguments)
                .env_remove("HERDR_SESSION")
                .env("HERDR_PANE_ID", pane)
                .spawn()
                .expect("marked fixture process");
            CallerProcess {
                process_id: child.id(),
            }
            .settled_pane_id();
            Self(child)
        }
    }

    impl Drop for MarkedProcess {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }

    #[test]
    fn a_caller_bound_in_the_registry_resolves_with_its_role() {
        let fixture = NexusFixture::new();
        bind_existing_caller(&fixture, "mind-live", "pane-1");
        fixture.set_agents(vec![codex_agent_in("pane-1", "terminal-pane-1")]);

        assert_eq!(
            fixture.nexus.resolve_caller(pane("pane-1"), None),
            Response::CallerResolved(mind_live())
        );
        assert_eq!(
            fixture
                .nexus
                .resolve_caller(pane("pane-1"), Some("mind-live".into())),
            Response::CallerResolved(mind_live())
        );
    }

    #[test]
    fn a_process_in_a_pane_is_found_by_its_own_marks_or_its_ancestors() {
        let fixture = NexusFixture::new();
        bind_existing_caller(&fixture, "mind-live", "pane-1");
        fixture.set_agents(vec![codex_agent_in("pane-1", "terminal-pane-1")]);
        // The first sleeper carries the pane mark itself; the second shell
        // stays the parent of a sleeper whose environment drops it. Neither
        // carries a session: the session is the one whose socket their
        // Herdr server ancestor listens on.
        let marked = MarkedProcess::spawn("sleep", &["30"], "pane-1");
        let scrubbed = MarkedProcess::spawn(
            "sh",
            &["-c", "env -u HERDR_SESSION -u HERDR_PANE_ID sleep 30; true"],
            "pane-2",
        );
        let marked_process = CallerProcess {
            process_id: marked.0.id(),
        };
        assert_eq!(marked_process.marked_pane(), None);
        assert_eq!(
            marked_process.caller_pane(&fixture.nexus.herdr),
            pane("pane-1")
        );

        let shell = scrubbed.0.id();
        let children = format!("/proc/{shell}/task/{shell}/children");
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        let sleeper = loop {
            // The child is `env` until it execs `sleep` without the marks.
            let listed = fs::read_to_string(&children).unwrap_or_default();
            if let Some(child) = listed.split_whitespace().next()
                && fs::read_to_string(format!("/proc/{child}/comm")).unwrap_or_default()
                    == "sleep\n"
            {
                break child.parse::<u32>().expect("child process id");
            }
            assert!(std::time::Instant::now() < deadline, "sleeper started");
            std::thread::yield_now();
        };
        let sleeper = CallerProcess {
            process_id: sleeper,
        };
        assert_eq!(sleeper.marked_pane_id(), None);
        assert_eq!(sleeper.caller_pane(&fixture.nexus.herdr), pane("pane-2"));

        assert_eq!(
            fixture
                .nexus
                .resolve_caller(marked_process.caller_pane(&fixture.nexus.herdr), None),
            Response::CallerResolved(mind_live())
        );
    }

    /// A Start's origin is derived from its calling process as Identify
    /// resolves a caller: the one flow bound in the peer's pane, with its
    /// native session. A process in no bound pane, or a peer not read, is
    /// Unbound: recorded as such, never refused.
    #[test]
    fn a_start_origin_is_the_bound_flow_in_the_callers_pane_or_unbound() {
        use crate::generated::starter::DerivesOrigin;
        use crate::store::ReadsFlowRows;
        let fixture = NexusFixture::new();
        bind_existing_caller(&fixture, "mind-live", "pane-1");
        fixture.set_agents(vec![codex_agent_in("pane-1", "terminal-pane-1")]);
        let bound = MarkedProcess::spawn("sleep", &["30"], "pane-1");
        let unbound = MarkedProcess::spawn("sleep", &["30"], "pane-9");
        let session_id = fixture
            .nexus
            .store
            .flow_node("mind-live")
            .unwrap()
            .expect("the bound flow's row")
            .session_id;
        assert_eq!(
            fixture.nexus.origin_of(Some(CallerProcess {
                process_id: bound.0.id(),
            })),
            Origin::Flow(FlowOrigin {
                flow_id: "mind-live".into(),
                session_id,
            })
        );
        assert_eq!(
            fixture.nexus.origin_of(Some(CallerProcess {
                process_id: unbound.0.id(),
            })),
            Origin::Unbound
        );
        assert_eq!(
            fixture.nexus.origin_of(None::<CallerProcess>),
            Origin::Unbound
        );
    }

    /// A Herdr server that listens on the sockets of two listed sessions
    /// does not say which one its pane is in: the pane is not found, and no
    /// flow is named.
    #[test]
    fn a_pane_id_whose_herdr_server_serves_two_sessions_names_no_pane() {
        let fixture = NexusFixture::new();
        bind_existing_caller(&fixture, "mind-live", "pane-1");
        fixture.set_agents(vec![codex_agent_in("pane-1", "terminal-pane-1")]);
        let marked = MarkedProcess::spawn("sleep", &["30"], "pane-1");
        let marked_process = CallerProcess {
            process_id: marked.0.id(),
        };
        assert_eq!(
            marked_process.caller_pane(&fixture.nexus.herdr),
            pane("pane-1")
        );

        let herdr_config = fixture.directory.path().join("herdr-config");
        let other_socket = herdr_config.join("sessions/other/herdr.sock");
        fs::create_dir_all(other_socket.parent().unwrap()).unwrap();
        let _other_server = UnixListener::bind(&other_socket).unwrap();
        fixture.list_herdr_sessions(&[
            ("default", herdr_config.join("herdr.sock")),
            (
                "messaging-build",
                herdr_config.join("sessions/messaging-build/herdr.sock"),
            ),
            ("other", other_socket),
        ]);
        assert_eq!(marked_process.caller_pane(&fixture.nexus.herdr), None);
        assert_eq!(
            fixture
                .nexus
                .resolve_caller(marked_process.caller_pane(&fixture.nexus.herdr), None),
            Response::CallerResolutionRejected(
                signal_flow::CallerResolutionRejection::CallerUnknown
            )
        );
    }

    /// The Herdr server is known by the session socket it serves, never by
    /// its binary: the `herdr` this Nexus runs (here the fixture script, as
    /// on a host where a wrapper on `PATH` stands before Herdr's own binary)
    /// is not the binary the server runs, and a process between the peer and
    /// the server that serves no session is walked past.
    #[test]
    fn a_pane_id_names_the_session_its_server_serves_whatever_herdr_path_names() {
        let fixture = NexusFixture::new();
        bind_existing_caller(&fixture, "mind-live", "pane-1");
        fixture.set_agents(vec![codex_agent_in("pane-1", "terminal-pane-1")]);
        let server = fs::canonicalize(std::env::current_exe().unwrap()).unwrap();
        let herdr_cli = fs::canonicalize(&fixture.snapshot_program).unwrap();
        assert_ne!(herdr_cli, server);

        let shell = MarkedProcess::spawn("sh", &["-c", "sleep 30; true"], "pane-1");
        let shell_id = shell.0.id();
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        let peer = loop {
            let children = fs::read_to_string(format!("/proc/{shell_id}/task/{shell_id}/children"))
                .unwrap_or_default();
            if let Some(child) = children.split_whitespace().next() {
                let process = CallerProcess {
                    process_id: child.parse().unwrap(),
                };
                if process.marked_pane_id().as_deref() == Some("pane-1") {
                    break process;
                }
            }
            assert!(std::time::Instant::now() < deadline, "fixture child");
            std::thread::sleep(Duration::from_millis(20));
        };
        assert_eq!(peer.caller_pane(&fixture.nexus.herdr), pane("pane-1"));
        assert_eq!(
            fixture
                .nexus
                .resolve_caller(peer.caller_pane(&fixture.nexus.herdr), None),
            Response::CallerResolved(mind_live())
        );
    }

    #[test]
    fn an_unbound_peer_is_caller_unknown() {
        let fixture = NexusFixture::new();
        bind_existing_caller(&fixture, "mind-live", "pane-1");
        fixture.set_agents(vec![
            codex_agent_in("pane-1", "terminal-pane-1"),
            codex_agent_in("pane-9", "terminal-pane-9"),
        ]);
        let unknown = Response::CallerResolutionRejected(
            signal_flow::CallerResolutionRejection::CallerUnknown,
        );

        // A pane the registry binds no flow to.
        assert_eq!(fixture.nexus.resolve_caller(pane("pane-9"), None), unknown);
        // A peer outside any Herdr pane.
        assert_eq!(fixture.nexus.resolve_caller(None, None), unknown);
        // A dispatch has no connection, so no peer.
        assert_eq!(fixture.nexus.dispatch(Query::ResolveCaller(None)), unknown);
        // Herdr reused the pane ID for a new terminal: the binding is gone.
        fixture.set_agents(vec![codex_agent_in("pane-1", "terminal-new")]);
        assert_eq!(fixture.nexus.resolve_caller(pane("pane-1"), None), unknown);
    }

    #[test]
    fn a_claimed_flow_id_that_differs_is_caller_mismatch() {
        let fixture = NexusFixture::new();
        bind_existing_caller(&fixture, "mind-live", "pane-1");
        fixture.set_agents(vec![codex_agent_in("pane-1", "terminal-pane-1")]);
        assert_eq!(
            fixture
                .nexus
                .resolve_caller(pane("pane-1"), Some("field-other".into())),
            Response::CallerResolutionRejected(
                signal_flow::CallerResolutionRejection::CallerMismatch(mind_live())
            )
        );
    }

    #[test]
    fn the_kernel_names_the_peer_of_a_connection() {
        let (ours, theirs) = std::os::unix::net::UnixStream::pair().expect("socket pair");
        let own = Some(CallerProcess {
            process_id: std::process::id(),
        });
        assert_eq!(ours.peer_process(), own);
        assert_eq!(theirs.peer_process(), own);
    }

    #[test]
    fn a_nexus_over_earlier_launch_attempt_rows_opens_and_answers() {
        let rows = crate::store::EarlierLaunchAttemptRows {
            bound_flow_id: "launched-earlier".into(),
        };
        let fixture = NexusFixture::with_store(|path| rows.seed(path));
        assert_eq!(fixture.nexus.store.opening_settlements.len(), 3);
        use crate::store::ReadsFlowRows as _;
        assert_eq!(
            fixture
                .nexus
                .store
                .flow_node("launched-earlier")
                .expect("Flow row read")
                .expect("earlier Flow row")
                .flow_id,
            "launched-earlier"
        );
        // Every earlier row is kept aside: none answers as an attempt.
        for request in [
            crate::store::EarlierLaunchAttemptRows::EARLIER_REQUEST,
            crate::store::EarlierLaunchAttemptRows::BEFORE_ADDRESS_REQUEST,
            crate::store::EarlierLaunchAttemptRows::UNREADABLE_REQUEST,
        ] {
            assert_eq!(
                fixture.nexus.dispatch(Query::LaunchStatus(request.into())),
                Response::LaunchStatusRejected(
                    signal_flow::LaunchStatusRejection::UnknownLaunchRequest
                )
            );
        }
        // The 0.34 row is carried and answers.
        assert!(matches!(
            fixture.nexus.dispatch(Query::LaunchStatus(
                crate::store::EarlierLaunchAttemptRows::ZERO_THIRTY_FOUR_REQUEST.into()
            )),
            Response::LaunchInspected(inspection)
                if inspection.launch_attempt.launch_request_id
                    == crate::store::EarlierLaunchAttemptRows::ZERO_THIRTY_FOUR_REQUEST
        ));
        // The flow the earlier launch bound keeps the role it named.
        assert_eq!(
            crate::store::ReadsFlowRoles::role(&fixture.nexus.store, "launched-earlier")
                .unwrap()
                .map(|role| role.aspect),
            Some(Aspect::Psyche)
        );
    }
}
