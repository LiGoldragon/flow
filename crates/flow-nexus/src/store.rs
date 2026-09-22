//! Durable Flow Nexus identity and dispatch state.
//!
//! The ordinary Signal contract remains the public boundary.  This module
//! owns its single `.sema` store and lowers a closed `signal_flow::Query`
//! into its typed, durable records.

use std::{io::Read, path::Path, sync::Mutex};

use meta_signal_flow::Configuration;
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use sema_engine::{
    Assertion, Engine, EngineOpen, EngineRecord, FamilyName, KeyedMutation, QueryPlan, RecordKey,
    SchemaHash, SchemaVersion, TableDescriptor, TableName, TableReference,
};
use signal_flow::{
    EndpointSelection, FlowLifecycle as SignalFlowLifecycle, FlowNode, HarnessKind, HerdrRoute,
    HerdrRouteSelection, OriginClue, Query, RecipientResolutionRejection, Response,
    RestartRejection, Restarted, RouteReadiness, StartRejection, Started,
};

const FLOW_TABLE_NAME: TableName = TableName::new("flow_nexus_flows");
const FLOW_STATE_TABLE_NAME: TableName = TableName::new("flow_nexus_state");
const FLOW_CONFIGURATION_TABLE_NAME: TableName = TableName::new("flow_nexus_configuration");
const FLOW_HERDR_ROUTE_TABLE_NAME: TableName = TableName::new("flow_nexus_herdr_routes");
const FLOW_DELIVERY_BINDING_TABLE_NAME: TableName = TableName::new("flow_nexus_delivery_bindings");
const FLOW_VERIFIED_BINDING_TABLE_NAME: TableName =
    TableName::new("flow_nexus_verified_delivery_bindings");
const MAX_COMPLETED_DELIVERY_ATTEMPTS: usize = 1024;
const STATE_KEY: &str = "identity";
const CONFIGURATION_KEY: &str = "configured";
const DEFAULT_ORDINARY_SOCKET: &str = "/run/user/1001/flow/flow.sock";
const DEFAULT_META_SOCKET: &str = "/run/user/1001/flow/flow-meta.sock";

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
struct FlowRecord {
    flow_id: String,
    flow_type: String,
    origin: OriginClue,
    thread_id: Option<String>,
    harness_kind: HarnessKind,
    endpoint_selection: EndpointSelection,
    lifecycle: FlowLifecycle,
    generation: u64,
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
#[rkyv(derive(Debug))]
enum FlowLifecycle {
    Pending,
    Active,
}

impl EngineRecord for FlowRecord {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.flow_id.clone())
    }
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
struct FlowHerdrRouteRecord {
    flow_id: String,
    route: HerdrRoute,
}

/// The immutable identity of an endpoint admitted to receive a delivery.
/// Every field is supplied by the registration authority; missing evidence is
/// rejected rather than synthesized from local process state.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
#[rkyv(derive(Debug))]
pub struct DeliveryBinding {
    pub native_thread: String,
    pub harness_session: String,
    pub route_identity: String,
    pub endpoint_identity: String,
    pub process_pid: i64,
    pub process_start_time: i64,
}

/// Admission is independent from an already-issued delivery permit.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
#[rkyv(derive(Debug))]
pub enum AdmissionGate {
    Open,
    RefreshHeld { transition_id: String },
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
#[rkyv(derive(Debug))]
pub struct DeliveryPermit {
    pub attempt_id: String,
    pub source_event_identifier: String,
    pub token: String,
    pub binding_generation: u64,
    pub binding: DeliveryBinding,
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
#[rkyv(derive(Debug))]
pub struct CompletionRecord {
    pub attempt_id: String,
    pub token: String,
    pub binding: DeliveryBinding,
    pub binding_generation: u64,
    pub transport_receipt_id: String,
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
#[rkyv(derive(Debug))]
struct ReattachCompletion {
    transition_id: String,
    registration_id: String,
    old_binding: DeliveryBinding,
    old_binding_generation: u64,
    new_binding: DeliveryBinding,
    new_binding_generation: u64,
    new_lifecycle_generation: u64,
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
#[rkyv(derive(Debug))]
struct VerifiedBindingRecord {
    flow_id: String,
    registration_id: String,
    transition_id: String,
    binding: DeliveryBinding,
    lifecycle_generation: u64,
    expected_binding_generation: Option<u64>,
    readiness_receipt_id: String,
    proof_digest: String,
    consumed: bool,
    retired_registration_ids: Vec<String>,
}

impl EngineRecord for VerifiedBindingRecord {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.flow_id.clone())
    }
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
#[rkyv(derive(Debug))]
struct FlowDeliveryBindingRecord {
    flow_id: String,
    binding: DeliveryBinding,
    binding_generation: u64,
    lifecycle_generation: u64,
    admission: AdmissionGate,
    permit: Option<DeliveryPermit>,
    last_completion: Option<CompletionRecord>,
    completed_attempts: Vec<String>,
    last_reattach: Option<ReattachCompletion>,
}

impl EngineRecord for FlowDeliveryBindingRecord {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.flow_id.clone())
    }
}

impl EngineRecord for FlowHerdrRouteRecord {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.flow_id.clone())
    }
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
#[rkyv(derive(Debug))]
struct FlowStoreState {
    next_flow_number: u64,
}

impl EngineRecord for FlowStoreState {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(STATE_KEY)
    }
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
struct FlowStoreConfiguration {
    configuration: Configuration,
}

impl EngineRecord for FlowStoreConfiguration {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(CONFIGURATION_KEY)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("sema-engine operation failed: {0}")]
    Engine(#[from] sema_engine::Error),
    #[error("flow nexus state record is absent or duplicated")]
    StateInvariant,
    #[error("secure delivery token source failed: {0}")]
    TokenSource(String),
}

pub struct FlowStore {
    engine: Engine,
    flows: TableReference<FlowRecord>,
    state: TableReference<FlowStoreState>,
    configuration: TableReference<FlowStoreConfiguration>,
    herdr_routes: TableReference<FlowHerdrRouteRecord>,
    delivery_bindings: TableReference<FlowDeliveryBindingRecord>,
    verified_bindings: TableReference<VerifiedBindingRecord>,
    delivery_guard: Mutex<()>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BindingState {
    pub binding: DeliveryBinding,
    pub binding_generation: u64,
    pub lifecycle_generation: u64,
    pub admission: AdmissionGate,
    pub permit: Option<DeliveryPermit>,
    pub last_completion: Option<CompletionRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InitializeDeliveryBinding {
    pub flow_id: String,
    pub binding: DeliveryBinding,
    pub lifecycle_generation: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AcquireDelivery {
    pub flow_id: String,
    pub expected_binding: DeliveryBinding,
    pub expected_binding_generation: u64,
    pub attempt_id: String,
    pub source_event_identifier: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BeginRefresh {
    pub flow_id: String,
    pub expected_binding_generation: u64,
    pub transition_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReleaseConfirmed {
    pub flow_id: String,
    pub attempt_id: String,
    pub token: String,
    pub binding: DeliveryBinding,
    pub expected_binding_generation: u64,
    pub transport_receipt_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadyReattach {
    pub flow_id: String,
    pub transition_id: String,
    pub expected_old_binding: DeliveryBinding,
    pub expected_old_binding_generation: u64,
    pub registration_id: String,
}

/// The Flow handler writes this only after validating the replacement against
/// native registration and readiness evidence. Store-only code cannot invent
/// the six binding fields from a legacy `FlowNode`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedBindingRegistration {
    pub flow_id: String,
    pub registration_id: String,
    pub refresh_transition_id: Option<String>,
    pub binding: DeliveryBinding,
    pub lifecycle_generation: u64,
    pub expected_binding_generation: Option<u64>,
    pub readiness_receipt_id: String,
    pub proof_digest: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BootstrapBinding {
    pub flow_id: String,
    pub registration_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeliveryRejection {
    UnknownFlow,
    MissingState,
    BindingUnavailable,
    StaleBinding,
    StaleGeneration,
    RefreshHeld,
    Busy,
    AttemptConflict,
    StalePermit,
    TransitionConflict,
    ActivePermit,
    CorruptState,
    GenerationOverflow,
    CapacityExhausted,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InitializeDeliveryOutcome {
    Initialized(BindingState),
    Rejected(DeliveryRejection),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AcquireDeliveryOutcome {
    Granted(DeliveryPermit),
    AlreadyGranted(DeliveryPermit),
    Rejected(DeliveryRejection),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BeginRefreshOutcome {
    Held {
        active_permit: Option<DeliveryPermit>,
    },
    AlreadyHeld {
        active_permit: Option<DeliveryPermit>,
    },
    Rejected(DeliveryRejection),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReleaseConfirmedOutcome {
    Released,
    AlreadyReleased,
    Rejected(DeliveryRejection),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReadyReattachOutcome {
    Opened { binding_generation: u64 },
    AlreadyOpened { binding_generation: u64 },
    Rejected(DeliveryRejection),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VerifiedBindingOutcome {
    Recorded,
    AlreadyRecorded,
    Rejected(DeliveryRejection),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BootstrapBindingOutcome {
    Initialized(BindingState),
    AlreadyInitialized(BindingState),
    Rejected(DeliveryRejection),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReadDeliveryStateOutcome {
    State(BindingState),
    Rejected(DeliveryRejection),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestartAuthorization {
    pub flow_id: String,
    pub authority_flow_id: String,
    pub thread_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingLaunch {
    pub flow_id: String,
    pub origin: OriginClue,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FlowRegistration {
    Registered(Box<FlowNode>),
    ConflictingBinding,
}

/// The constructor is a trait operation so the Nexus's storage boundary is
/// part of its ontology rather than an unclassified helper.
pub trait OpensFlowStore {
    fn open(path: &Path) -> Result<Self, StoreError>
    where
        Self: Sized;
}

/// The only ordinary-socket operation that reaches durable Flow state.
pub trait AppliesFlowQuery {
    fn apply(&self, query: Query) -> Result<Response, StoreError>;
}

/// Reserves a flow identity before a launcher asks the daemon for a thread.
pub trait ReservesPendingStart {
    fn reserve_pending_start(&self, query: Query) -> Result<Option<PendingLaunch>, StoreError>;
}

/// Records the daemon thread immediately after `thread/start` has accepted it.
pub trait RecordsPendingThread {
    fn record_pending_thread(
        &self,
        pending: &PendingLaunch,
        thread_id: String,
    ) -> Result<bool, StoreError>;
}

/// Marks a known pending launch active only after its first turn was accepted.
pub trait ConfirmsStartedFlow {
    fn confirm_started(&self, flow_id: &str) -> Result<Response, StoreError>;
}

/// Reads the persisted daemon thread only after checking restart authority.
pub trait AuthorizesFlowRestart {
    fn authorize_restart(
        &self,
        flow_id: &str,
        authority_flow_id: &str,
    ) -> Result<Option<RestartAuthorization>, StoreError>;
}

/// Advances the generation after the adapter has accepted the resume turn.
pub trait RecordsRestartedFlow {
    fn record_restarted(&self, authorization: RestartAuthorization)
        -> Result<Response, StoreError>;
}

/// Configuration is durable policy and shares the Nexus's sole `.sema` store.
pub trait ConfiguresFlowStore {
    fn configuration(&self) -> Result<Configuration, StoreError>;
    fn configure(&self, configuration: Configuration) -> Result<(), StoreError>;
}

pub trait RegistersFlowIdentity {
    fn register_flow(&self, flow_node: FlowNode) -> Result<FlowRegistration, StoreError>;
}

/// Flow owns this state machine. Its Signal-facing adapter is deliberately
/// outside this module, so Message cannot impersonate an admission decision.
pub trait ManagesDeliveryPermits {
    fn record_verified_binding(
        &self,
        request: VerifiedBindingRegistration,
    ) -> Result<VerifiedBindingOutcome, StoreError>;
    fn bootstrap_delivery_binding(
        &self,
        request: BootstrapBinding,
    ) -> Result<BootstrapBindingOutcome, StoreError>;
    fn acquire_delivery(
        &self,
        request: AcquireDelivery,
    ) -> Result<AcquireDeliveryOutcome, StoreError>;
    fn begin_refresh(&self, request: BeginRefresh) -> Result<BeginRefreshOutcome, StoreError>;
    fn release_confirmed(
        &self,
        request: ReleaseConfirmed,
    ) -> Result<ReleaseConfirmedOutcome, StoreError>;
    fn ready_reattach(&self, request: ReadyReattach) -> Result<ReadyReattachOutcome, StoreError>;
    fn read_delivery_state(&self, flow_id: &str) -> Result<ReadDeliveryStateOutcome, StoreError>;
}

trait ReadsFlowStore {
    fn state(&self) -> Result<FlowStoreState, StoreError>;
    fn flow(&self, flow_id: &str) -> Result<Option<FlowRecord>, StoreError>;
    fn herdr_route(&self, flow_id: &str) -> Result<Option<FlowHerdrRouteRecord>, StoreError>;
    fn stored_configuration(&self) -> Result<FlowStoreConfiguration, StoreError>;
    fn resolve_recipient(&self, flow_id: &str) -> Result<Response, StoreError>;
}

trait WritesFlowStore {
    fn reserve_start(
        &self,
        flow_type: String,
        origin: OriginClue,
    ) -> Result<PendingLaunch, StoreError>;
    fn record_thread(&self, pending: &PendingLaunch, thread_id: String)
        -> Result<bool, StoreError>;
    fn confirm_start(&self, flow_id: &str) -> Result<Response, StoreError>;
    fn restart(&self, authorization: RestartAuthorization) -> Result<Response, StoreError>;
}

impl OpensFlowStore for FlowStore {
    fn open(path: &Path) -> Result<Self, StoreError> {
        let mut engine = Engine::open(EngineOpen::new(path, SchemaVersion::new(1)))?;
        let flows = engine.register_table(TableDescriptor::new(
            FLOW_TABLE_NAME,
            FamilyName::new("flow-nexus-flow"),
            SchemaHash::for_label("flow-nexus-flow-v5"),
        ))?;
        let state = engine.register_table(TableDescriptor::new(
            FLOW_STATE_TABLE_NAME,
            FamilyName::new("flow-nexus-state"),
            SchemaHash::for_label("flow-nexus-state-v1"),
        ))?;
        let configuration = engine.register_table(TableDescriptor::new(
            FLOW_CONFIGURATION_TABLE_NAME,
            FamilyName::new("flow-nexus-configuration"),
            SchemaHash::for_label("flow-nexus-configuration-v1"),
        ))?;
        let herdr_routes = engine.register_table(TableDescriptor::new(
            FLOW_HERDR_ROUTE_TABLE_NAME,
            FamilyName::new("flow-nexus-herdr-route"),
            SchemaHash::for_label("flow-nexus-herdr-route-v1"),
        ))?;
        let delivery_bindings = engine.register_table(TableDescriptor::new(
            FLOW_DELIVERY_BINDING_TABLE_NAME,
            FamilyName::new("flow-nexus-delivery-binding"),
            SchemaHash::for_label("flow-nexus-delivery-binding-v1"),
        ))?;
        let verified_bindings = engine.register_table(TableDescriptor::new(
            FLOW_VERIFIED_BINDING_TABLE_NAME,
            FamilyName::new("flow-nexus-verified-delivery-binding"),
            SchemaHash::for_label("flow-nexus-verified-delivery-binding-v1"),
        ))?;
        let store = Self {
            engine,
            flows,
            state,
            configuration,
            herdr_routes,
            delivery_bindings,
            verified_bindings,
            delivery_guard: Mutex::new(()),
        };
        if store
            .engine
            .match_records(QueryPlan::key(store.state, RecordKey::new(STATE_KEY)))?
            .records()
            .is_empty()
        {
            store.engine.assert(Assertion::new(
                store.state,
                FlowStoreState {
                    next_flow_number: 1,
                },
            ))?;
        }
        if store
            .engine
            .match_records(QueryPlan::key(
                store.configuration,
                RecordKey::new(CONFIGURATION_KEY),
            ))?
            .records()
            .is_empty()
        {
            store.engine.assert(Assertion::new(
                store.configuration,
                FlowStoreConfiguration {
                    configuration: Configuration {
                        ordinary_socket_path: DEFAULT_ORDINARY_SOCKET.into(),
                        meta_socket_path: DEFAULT_META_SOCKET.into(),
                    },
                },
            ))?;
        }
        Ok(store)
    }
}

impl AppliesFlowQuery for FlowStore {
    fn apply(&self, query: Query) -> Result<Response, StoreError> {
        match query {
            Query::Start(_) => Ok(Response::StartRejected(StartRejection::LaunchRefused)),
            Query::Restart(_) => Ok(Response::RestartRejected(RestartRejection::ResumeRefused)),
            Query::ResolveRecipient(flow_id) => self.resolve_recipient(&flow_id),
        }
    }
}

impl ReservesPendingStart for FlowStore {
    fn reserve_pending_start(&self, query: Query) -> Result<Option<PendingLaunch>, StoreError> {
        match query {
            Query::Start(request) if request.flow_type == "codex-medium" => self
                .reserve_start(request.flow_type, request.origin_clue)
                .map(Some),
            Query::Start(_) | Query::Restart(_) | Query::ResolveRecipient(_) => Ok(None),
        }
    }
}

impl RecordsPendingThread for FlowStore {
    fn record_pending_thread(
        &self,
        pending: &PendingLaunch,
        thread_id: String,
    ) -> Result<bool, StoreError> {
        self.record_thread(pending, thread_id)
    }
}

impl ConfirmsStartedFlow for FlowStore {
    fn confirm_started(&self, flow_id: &str) -> Result<Response, StoreError> {
        self.confirm_start(flow_id)
    }
}

impl AuthorizesFlowRestart for FlowStore {
    fn authorize_restart(
        &self,
        flow_id: &str,
        authority_flow_id: &str,
    ) -> Result<Option<RestartAuthorization>, StoreError> {
        let Some(flow) = self.flow(flow_id)? else {
            return Ok(None);
        };
        if flow.flow_id != authority_flow_id {
            return Ok(None);
        }
        let Some(thread_id) = flow.thread_id else {
            return Ok(None);
        };
        Ok(Some(RestartAuthorization {
            flow_id: flow.flow_id,
            authority_flow_id: authority_flow_id.into(),
            thread_id,
        }))
    }
}

impl RecordsRestartedFlow for FlowStore {
    fn record_restarted(
        &self,
        authorization: RestartAuthorization,
    ) -> Result<Response, StoreError> {
        self.restart(authorization)
    }
}

impl ConfiguresFlowStore for FlowStore {
    fn configuration(&self) -> Result<Configuration, StoreError> {
        Ok(self.stored_configuration()?.configuration)
    }

    fn configure(&self, configuration: Configuration) -> Result<(), StoreError> {
        self.engine.mutate_keyed(KeyedMutation::new(
            self.configuration,
            RecordKey::new(CONFIGURATION_KEY),
            FlowStoreConfiguration { configuration },
        ))?;
        Ok(())
    }
}

impl RegistersFlowIdentity for FlowStore {
    fn register_flow(&self, flow_node: FlowNode) -> Result<FlowRegistration, StoreError> {
        let HerdrRouteSelection::Available(route) = flow_node.herdr_route_selection.clone() else {
            return Ok(FlowRegistration::ConflictingBinding);
        };
        if let Some(flow) = self.flow(&flow_node.flow_id)? {
            if flow.thread_id.as_deref() != Some(flow_node.session_id.as_str())
                || flow.harness_kind != flow_node.harness_kind
            {
                return Ok(FlowRegistration::ConflictingBinding);
            }
            match self.herdr_route(&flow_node.flow_id)? {
                Some(existing) if existing.route != route => {
                    return Ok(FlowRegistration::ConflictingBinding);
                }
                Some(_) => return Ok(FlowRegistration::Registered(Box::new(flow_node))),
                None => {
                    self.engine.assert(Assertion::new(
                        self.herdr_routes,
                        FlowHerdrRouteRecord {
                            flow_id: flow_node.flow_id.clone(),
                            route: route.clone(),
                        },
                    ))?;
                    return Ok(FlowRegistration::Registered(Box::new(flow_node)));
                }
            }
        }
        let lifecycle = match flow_node.flow_lifecycle {
            SignalFlowLifecycle::Pending => FlowLifecycle::Pending,
            SignalFlowLifecycle::Active => FlowLifecycle::Active,
        };
        let record = FlowRecord {
            flow_id: flow_node.flow_id.clone(),
            flow_type: match flow_node.harness_kind {
                HarnessKind::Codex => "codex-registered".into(),
                HarnessKind::Claude => "claude-registered".into(),
            },
            origin: flow_node.origin_clue.clone(),
            thread_id: Some(flow_node.session_id.clone()),
            harness_kind: flow_node.harness_kind.clone(),
            endpoint_selection: flow_node.endpoint_selection.clone(),
            lifecycle,
            generation: 1,
        };
        self.engine.commit_atomic(
            self.engine
                .begin_atomic_commit()
                .assert(self.flows, record)
                .assert(
                    self.herdr_routes,
                    FlowHerdrRouteRecord {
                        flow_id: flow_node.flow_id.clone(),
                        route,
                    },
                ),
        )?;
        Ok(FlowRegistration::Registered(Box::new(flow_node)))
    }
}

impl ManagesDeliveryPermits for FlowStore {
    fn record_verified_binding(
        &self,
        request: VerifiedBindingRegistration,
    ) -> Result<VerifiedBindingOutcome, StoreError> {
        let _guard = self
            .delivery_guard
            .lock()
            .map_err(|_| StoreError::StateInvariant)?;
        if self.flow(&request.flow_id)?.is_none() {
            return Ok(VerifiedBindingOutcome::Rejected(
                DeliveryRejection::UnknownFlow,
            ));
        }
        if !binding_is_complete(&request.binding)
            || request.lifecycle_generation == 0
            || request.registration_id.is_empty()
            || request.readiness_receipt_id.is_empty()
            || request.proof_digest.is_empty()
        {
            return Ok(VerifiedBindingOutcome::Rejected(
                DeliveryRejection::BindingUnavailable,
            ));
        }
        let mut candidate = VerifiedBindingRecord {
            flow_id: request.flow_id.clone(),
            registration_id: request.registration_id,
            binding: request.binding,
            lifecycle_generation: request.lifecycle_generation,
            expected_binding_generation: request.expected_binding_generation,
            transition_id: request.refresh_transition_id.unwrap_or_default(),
            readiness_receipt_id: request.readiness_receipt_id,
            proof_digest: request.proof_digest,
            consumed: false,
            retired_registration_ids: Vec::new(),
        };
        if let Some(existing) = self.verified_binding(&request.flow_id)? {
            if existing == candidate {
                return Ok(VerifiedBindingOutcome::AlreadyRecorded);
            }
            if !existing.consumed {
                return Ok(VerifiedBindingOutcome::Rejected(
                    DeliveryRejection::TransitionConflict,
                ));
            }
            if candidate.registration_id == existing.registration_id
                || existing
                    .retired_registration_ids
                    .iter()
                    .any(|id| id == &candidate.registration_id)
            {
                return Ok(VerifiedBindingOutcome::Rejected(
                    DeliveryRejection::TransitionConflict,
                ));
            }
            // A consumed record is replay history. A successor may replace it
            // only while the current state proves the requested generation and
            // refresh transition; never overwrite a live candidate.
            let Some(state) = self.delivery_binding(&request.flow_id)? else {
                return Ok(VerifiedBindingOutcome::Rejected(
                    DeliveryRejection::MissingState,
                ));
            };
            if candidate.expected_binding_generation != Some(state.binding_generation)
                || candidate.transition_id.is_empty()
                || !matches!(&state.admission, AdmissionGate::RefreshHeld { transition_id } if transition_id == &candidate.transition_id)
            {
                return Ok(VerifiedBindingOutcome::Rejected(
                    DeliveryRejection::TransitionConflict,
                ));
            }
            candidate
                .retired_registration_ids
                .extend(existing.retired_registration_ids);
            candidate
                .retired_registration_ids
                .push(existing.registration_id);
            self.engine.mutate_keyed(KeyedMutation::new(
                self.verified_bindings,
                RecordKey::new(candidate.flow_id.clone()),
                candidate,
            ))?;
            return Ok(VerifiedBindingOutcome::Recorded);
        }
        self.engine
            .assert(Assertion::new(self.verified_bindings, candidate))?;
        Ok(VerifiedBindingOutcome::Recorded)
    }

    fn bootstrap_delivery_binding(
        &self,
        request: BootstrapBinding,
    ) -> Result<BootstrapBindingOutcome, StoreError> {
        let _guard = self
            .delivery_guard
            .lock()
            .map_err(|_| StoreError::StateInvariant)?;
        let Some(candidate) = self.verified_binding(&request.flow_id)? else {
            return Ok(BootstrapBindingOutcome::Rejected(
                self.missing_delivery_rejection(&request.flow_id)?,
            ));
        };
        if let Some(existing) = self.delivery_binding(&request.flow_id)? {
            return Ok(
                if candidate.registration_id == request.registration_id && candidate.consumed {
                    BootstrapBindingOutcome::AlreadyInitialized(binding_state(existing))
                } else {
                    BootstrapBindingOutcome::Rejected(DeliveryRejection::BindingUnavailable)
                },
            );
        }
        if candidate.registration_id != request.registration_id || candidate.consumed {
            return Ok(BootstrapBindingOutcome::Rejected(
                DeliveryRejection::BindingUnavailable,
            ));
        }
        if candidate.expected_binding_generation.is_some() || !candidate.transition_id.is_empty() {
            return Ok(BootstrapBindingOutcome::Rejected(
                DeliveryRejection::TransitionConflict,
            ));
        }
        let record = FlowDeliveryBindingRecord {
            flow_id: request.flow_id,
            binding: candidate.binding.clone(),
            binding_generation: 1,
            lifecycle_generation: candidate.lifecycle_generation,
            admission: AdmissionGate::Open,
            permit: None,
            last_completion: None,
            completed_attempts: Vec::new(),
            last_reattach: None,
        };
        let mut consumed = candidate;
        consumed.consumed = true;
        self.engine.commit_atomic(
            self.engine
                .begin_atomic_commit()
                .assert(self.delivery_bindings, record.clone())
                .mutate(self.verified_bindings, consumed),
        )?;
        Ok(BootstrapBindingOutcome::Initialized(binding_state(record)))
    }

    fn acquire_delivery(
        &self,
        request: AcquireDelivery,
    ) -> Result<AcquireDeliveryOutcome, StoreError> {
        let _guard = self
            .delivery_guard
            .lock()
            .map_err(|_| StoreError::StateInvariant)?;
        let Some(mut state) = self.checked_delivery_state(
            &request.flow_id,
            &request.expected_binding,
            request.expected_binding_generation,
        )?
        else {
            return Ok(AcquireDeliveryOutcome::Rejected(
                self.delivery_state_rejection(
                    &request.flow_id,
                    &request.expected_binding,
                    request.expected_binding_generation,
                )?,
            ));
        };
        if state
            .completed_attempts
            .iter()
            .any(|id| id == &request.attempt_id)
        {
            return Ok(AcquireDeliveryOutcome::Rejected(
                DeliveryRejection::AttemptConflict,
            ));
        }
        if let Some(permit) = state.permit.clone() {
            return Ok(
                if permit.attempt_id == request.attempt_id
                    && permit.source_event_identifier == request.source_event_identifier
                    && permit.binding == request.expected_binding
                {
                    AcquireDeliveryOutcome::AlreadyGranted(permit)
                } else {
                    AcquireDeliveryOutcome::Rejected(DeliveryRejection::Busy)
                },
            );
        }
        if matches!(state.admission, AdmissionGate::RefreshHeld { .. }) {
            return Ok(AcquireDeliveryOutcome::Rejected(
                DeliveryRejection::RefreshHeld,
            ));
        }
        if state.completed_attempts.len() >= MAX_COMPLETED_DELIVERY_ATTEMPTS {
            return Ok(AcquireDeliveryOutcome::Rejected(
                DeliveryRejection::CapacityExhausted,
            ));
        }
        if request.attempt_id.is_empty() || request.source_event_identifier.is_empty() {
            return Ok(AcquireDeliveryOutcome::Rejected(
                DeliveryRejection::CorruptState,
            ));
        }
        let permit = DeliveryPermit {
            attempt_id: request.attempt_id,
            source_event_identifier: request.source_event_identifier,
            token: secure_token()?,
            binding_generation: state.binding_generation,
            binding: state.binding.clone(),
        };
        state.permit = Some(permit.clone());
        self.engine.mutate_keyed(KeyedMutation::new(
            self.delivery_bindings,
            RecordKey::new(state.flow_id.clone()),
            state,
        ))?;
        Ok(AcquireDeliveryOutcome::Granted(permit))
    }

    fn begin_refresh(&self, request: BeginRefresh) -> Result<BeginRefreshOutcome, StoreError> {
        let _guard = self
            .delivery_guard
            .lock()
            .map_err(|_| StoreError::StateInvariant)?;
        let Some(mut state) = self.delivery_binding(&request.flow_id)? else {
            return Ok(BeginRefreshOutcome::Rejected(
                self.missing_delivery_rejection(&request.flow_id)?,
            ));
        };
        if state.binding_generation != request.expected_binding_generation {
            return Ok(BeginRefreshOutcome::Rejected(
                DeliveryRejection::StaleGeneration,
            ));
        }
        if request.transition_id.is_empty() {
            return Ok(BeginRefreshOutcome::Rejected(
                DeliveryRejection::TransitionConflict,
            ));
        }
        match &state.admission {
            AdmissionGate::RefreshHeld { transition_id }
                if transition_id == &request.transition_id =>
            {
                Ok(BeginRefreshOutcome::AlreadyHeld {
                    active_permit: state.permit,
                })
            }
            AdmissionGate::RefreshHeld { .. } => Ok(BeginRefreshOutcome::Rejected(
                DeliveryRejection::TransitionConflict,
            )),
            AdmissionGate::Open => {
                state.admission = AdmissionGate::RefreshHeld {
                    transition_id: request.transition_id,
                };
                let active_permit = state.permit.clone();
                self.engine.mutate_keyed(KeyedMutation::new(
                    self.delivery_bindings,
                    RecordKey::new(state.flow_id.clone()),
                    state,
                ))?;
                Ok(BeginRefreshOutcome::Held { active_permit })
            }
        }
    }

    fn release_confirmed(
        &self,
        request: ReleaseConfirmed,
    ) -> Result<ReleaseConfirmedOutcome, StoreError> {
        let _guard = self
            .delivery_guard
            .lock()
            .map_err(|_| StoreError::StateInvariant)?;
        let Some(mut state) = self.delivery_binding(&request.flow_id)? else {
            return Ok(ReleaseConfirmedOutcome::Rejected(
                self.missing_delivery_rejection(&request.flow_id)?,
            ));
        };
        if state.binding_generation != request.expected_binding_generation {
            return Ok(ReleaseConfirmedOutcome::Rejected(
                DeliveryRejection::StaleGeneration,
            ));
        }
        if state.binding != request.binding {
            return Ok(ReleaseConfirmedOutcome::Rejected(
                DeliveryRejection::StaleBinding,
            ));
        }
        if let Some(last) = &state.last_completion {
            if last.attempt_id == request.attempt_id
                && last.token == request.token
                && last.transport_receipt_id == request.transport_receipt_id
            {
                return Ok(ReleaseConfirmedOutcome::AlreadyReleased);
            }
        }
        let Some(permit) = state.permit.clone() else {
            return Ok(ReleaseConfirmedOutcome::Rejected(
                DeliveryRejection::StalePermit,
            ));
        };
        if permit.attempt_id != request.attempt_id
            || permit.token != request.token
            || permit.binding != request.binding
            || permit.binding_generation != request.expected_binding_generation
            || request.transport_receipt_id.is_empty()
        {
            return Ok(ReleaseConfirmedOutcome::Rejected(
                DeliveryRejection::StalePermit,
            ));
        }
        state.last_completion = Some(CompletionRecord {
            attempt_id: request.attempt_id.clone(),
            token: request.token,
            binding: request.binding,
            binding_generation: request.expected_binding_generation,
            transport_receipt_id: request.transport_receipt_id,
        });
        state.completed_attempts.push(request.attempt_id);
        state.permit = None;
        self.engine.mutate_keyed(KeyedMutation::new(
            self.delivery_bindings,
            RecordKey::new(state.flow_id.clone()),
            state,
        ))?;
        Ok(ReleaseConfirmedOutcome::Released)
    }

    fn ready_reattach(&self, request: ReadyReattach) -> Result<ReadyReattachOutcome, StoreError> {
        let _guard = self
            .delivery_guard
            .lock()
            .map_err(|_| StoreError::StateInvariant)?;
        let Some(mut state) = self.delivery_binding(&request.flow_id)? else {
            return Ok(ReadyReattachOutcome::Rejected(
                self.missing_delivery_rejection(&request.flow_id)?,
            ));
        };
        if let Some(last) = &state.last_reattach {
            if last.transition_id == request.transition_id
                && last.registration_id == request.registration_id
                && last.old_binding == request.expected_old_binding
                && last.old_binding_generation == request.expected_old_binding_generation
            {
                return Ok(ReadyReattachOutcome::AlreadyOpened {
                    binding_generation: last.new_binding_generation,
                });
            }
        }
        if state.binding != request.expected_old_binding {
            return Ok(ReadyReattachOutcome::Rejected(
                DeliveryRejection::StaleBinding,
            ));
        }
        if state.binding_generation != request.expected_old_binding_generation {
            return Ok(ReadyReattachOutcome::Rejected(
                DeliveryRejection::StaleGeneration,
            ));
        }
        let Some(candidate) = self.verified_binding(&request.flow_id)? else {
            return Ok(ReadyReattachOutcome::Rejected(
                DeliveryRejection::BindingUnavailable,
            ));
        };
        if candidate.registration_id != request.registration_id
            || candidate.consumed
            || candidate.expected_binding_generation
                != Some(request.expected_old_binding_generation)
            || candidate.transition_id != request.transition_id
        {
            return Ok(ReadyReattachOutcome::Rejected(
                DeliveryRejection::BindingUnavailable,
            ));
        }
        let AdmissionGate::RefreshHeld { transition_id } = &state.admission else {
            return Ok(ReadyReattachOutcome::Rejected(
                DeliveryRejection::TransitionConflict,
            ));
        };
        if transition_id != &request.transition_id {
            return Ok(ReadyReattachOutcome::Rejected(
                DeliveryRejection::TransitionConflict,
            ));
        }
        if state.permit.is_some() {
            return Ok(ReadyReattachOutcome::Rejected(
                DeliveryRejection::ActivePermit,
            ));
        }
        let Some(next_generation) = state.binding_generation.checked_add(1) else {
            return Ok(ReadyReattachOutcome::Rejected(
                DeliveryRejection::GenerationOverflow,
            ));
        };
        state.binding = candidate.binding.clone();
        state.binding_generation = next_generation;
        state.lifecycle_generation = candidate.lifecycle_generation;
        state.admission = AdmissionGate::Open;
        state.last_reattach = Some(ReattachCompletion {
            transition_id: request.transition_id,
            registration_id: request.registration_id,
            old_binding: request.expected_old_binding,
            old_binding_generation: request.expected_old_binding_generation,
            new_binding: state.binding.clone(),
            new_binding_generation: next_generation,
            new_lifecycle_generation: state.lifecycle_generation,
        });
        let mut consumed = candidate;
        consumed.consumed = true;
        self.engine.commit_atomic(
            self.engine
                .begin_atomic_commit()
                .mutate(self.delivery_bindings, state)
                .mutate(self.verified_bindings, consumed),
        )?;
        Ok(ReadyReattachOutcome::Opened {
            binding_generation: next_generation,
        })
    }

    fn read_delivery_state(&self, flow_id: &str) -> Result<ReadDeliveryStateOutcome, StoreError> {
        let _guard = self
            .delivery_guard
            .lock()
            .map_err(|_| StoreError::StateInvariant)?;
        let Some(state) = self.delivery_binding(flow_id)? else {
            return Ok(ReadDeliveryStateOutcome::Rejected(
                self.missing_delivery_rejection(flow_id)?,
            ));
        };
        Ok(ReadDeliveryStateOutcome::State(binding_state(state)))
    }
}

impl FlowStore {
    fn verified_binding(&self, flow_id: &str) -> Result<Option<VerifiedBindingRecord>, StoreError> {
        let records = self
            .engine
            .match_records(QueryPlan::key(
                self.verified_bindings,
                RecordKey::new(flow_id),
            ))?
            .records()
            .to_vec();
        match records.as_slice() {
            [] => Ok(None),
            [record] if binding_is_complete(&record.binding) => Ok(Some(record.clone())),
            _ => Err(StoreError::StateInvariant),
        }
    }

    fn delivery_binding(
        &self,
        flow_id: &str,
    ) -> Result<Option<FlowDeliveryBindingRecord>, StoreError> {
        let records = self
            .engine
            .match_records(QueryPlan::key(
                self.delivery_bindings,
                RecordKey::new(flow_id),
            ))?
            .records()
            .to_vec();
        match records.as_slice() {
            [] => Ok(None),
            [record] if binding_is_complete(&record.binding) && record.binding_generation > 0 => {
                Ok(Some(record.clone()))
            }
            [_] => Err(StoreError::StateInvariant),
            _ => Err(StoreError::StateInvariant),
        }
    }

    fn missing_delivery_rejection(&self, flow_id: &str) -> Result<DeliveryRejection, StoreError> {
        Ok(if self.flow(flow_id)?.is_some() {
            DeliveryRejection::MissingState
        } else {
            DeliveryRejection::UnknownFlow
        })
    }

    fn delivery_state_rejection(
        &self,
        flow_id: &str,
        expected_binding: &DeliveryBinding,
        expected_generation: u64,
    ) -> Result<DeliveryRejection, StoreError> {
        let Some(state) = self.delivery_binding(flow_id)? else {
            return self.missing_delivery_rejection(flow_id);
        };
        Ok(if state.binding != *expected_binding {
            DeliveryRejection::StaleBinding
        } else if state.binding_generation != expected_generation {
            DeliveryRejection::StaleGeneration
        } else {
            DeliveryRejection::CorruptState
        })
    }

    fn checked_delivery_state(
        &self,
        flow_id: &str,
        expected_binding: &DeliveryBinding,
        expected_generation: u64,
    ) -> Result<Option<FlowDeliveryBindingRecord>, StoreError> {
        let Some(state) = self.delivery_binding(flow_id)? else {
            return Ok(None);
        };
        Ok(
            (state.binding == *expected_binding && state.binding_generation == expected_generation)
                .then_some(state),
        )
    }
}

fn binding_is_complete(binding: &DeliveryBinding) -> bool {
    !binding.native_thread.is_empty()
        && !binding.harness_session.is_empty()
        && !binding.route_identity.is_empty()
        && !binding.endpoint_identity.is_empty()
        && binding.process_pid > 0
        && binding.process_start_time > 0
}

fn binding_state(record: FlowDeliveryBindingRecord) -> BindingState {
    BindingState {
        binding: record.binding,
        binding_generation: record.binding_generation,
        lifecycle_generation: record.lifecycle_generation,
        admission: record.admission,
        permit: record.permit,
        last_completion: record.last_completion,
    }
}

fn secure_token() -> Result<String, StoreError> {
    let mut bytes = [0_u8; 32];
    std::fs::File::open("/dev/urandom")
        .and_then(|mut source| source.read_exact(&mut bytes))
        .map_err(|error| StoreError::TokenSource(error.to_string()))?;
    Ok(base64::Engine::encode(
        &base64::engine::general_purpose::URL_SAFE_NO_PAD,
        bytes,
    ))
}

impl ReadsFlowStore for FlowStore {
    fn state(&self) -> Result<FlowStoreState, StoreError> {
        let records = self
            .engine
            .match_records(QueryPlan::key(self.state, RecordKey::new(STATE_KEY)))?
            .records()
            .to_vec();
        match records.as_slice() {
            [state] => Ok(state.clone()),
            _ => Err(StoreError::StateInvariant),
        }
    }

    fn flow(&self, flow_id: &str) -> Result<Option<FlowRecord>, StoreError> {
        let records = self
            .engine
            .match_records(QueryPlan::key(self.flows, RecordKey::new(flow_id)))?
            .records()
            .to_vec();
        match records.as_slice() {
            [] => Ok(None),
            [flow] => Ok(Some(flow.clone())),
            _ => Err(StoreError::StateInvariant),
        }
    }

    fn herdr_route(&self, flow_id: &str) -> Result<Option<FlowHerdrRouteRecord>, StoreError> {
        let records = self
            .engine
            .match_records(QueryPlan::key(self.herdr_routes, RecordKey::new(flow_id)))?
            .records()
            .to_vec();
        match records.as_slice() {
            [] => Ok(None),
            [route] => Ok(Some(route.clone())),
            _ => Err(StoreError::StateInvariant),
        }
    }

    fn stored_configuration(&self) -> Result<FlowStoreConfiguration, StoreError> {
        let records = self
            .engine
            .match_records(QueryPlan::key(
                self.configuration,
                RecordKey::new(CONFIGURATION_KEY),
            ))?
            .records()
            .to_vec();
        match records.as_slice() {
            [configuration] => Ok(configuration.clone()),
            _ => Err(StoreError::StateInvariant),
        }
    }

    fn resolve_recipient(&self, flow_id: &str) -> Result<Response, StoreError> {
        let Some(flow) = self.flow(flow_id)? else {
            return Ok(Response::RecipientResolutionRejected(
                RecipientResolutionRejection::UnknownFlow,
            ));
        };
        let Some(session_id) = flow.thread_id else {
            return Ok(Response::RecipientResolutionRejected(
                RecipientResolutionRejection::FlowUnavailable,
            ));
        };
        Ok(Response::RecipientResolved(FlowNode {
            flow_id: flow.flow_id,
            session_id,
            harness_kind: flow.harness_kind.clone(),
            endpoint_selection: flow.endpoint_selection.clone(),
            herdr_route_selection: self
                .herdr_route(flow_id)?
                .map(|record| HerdrRouteSelection::Available(record.route))
                .unwrap_or(HerdrRouteSelection::Unavailable),
            origin_clue: flow.origin,
            flow_lifecycle: match flow.lifecycle {
                FlowLifecycle::Active => SignalFlowLifecycle::Active,
                FlowLifecycle::Pending => SignalFlowLifecycle::Pending,
            },
        }))
    }
}

impl WritesFlowStore for FlowStore {
    fn reserve_start(
        &self,
        flow_type: String,
        origin: OriginClue,
    ) -> Result<PendingLaunch, StoreError> {
        let state = self.state()?;
        let flow_id = format!("flow-{:016x}", state.next_flow_number);
        self.engine.commit_atomic(
            self.engine
                .begin_atomic_commit()
                .assert(
                    self.flows,
                    FlowRecord {
                        flow_id: flow_id.clone(),
                        flow_type,
                        origin: origin.clone(),
                        thread_id: None,
                        harness_kind: HarnessKind::Codex,
                        endpoint_selection: EndpointSelection::Available(
                            signal_flow::Available_Data {
                                endpoint_path:
                                    "/home/li/.codex/app-server-control/app-server-control.sock"
                                        .into(),
                                route_readiness: RouteReadiness::Parked,
                            },
                        ),
                        lifecycle: FlowLifecycle::Pending,
                        generation: 0,
                    },
                )
                .mutate(
                    self.state,
                    FlowStoreState {
                        next_flow_number: state.next_flow_number + 1,
                    },
                ),
        )?;
        Ok(PendingLaunch { flow_id, origin })
    }

    fn record_thread(
        &self,
        pending: &PendingLaunch,
        thread_id: String,
    ) -> Result<bool, StoreError> {
        let Some(mut flow) = self.flow(&pending.flow_id)? else {
            return Ok(false);
        };
        if flow.origin != pending.origin
            || flow.lifecycle != FlowLifecycle::Pending
            || flow.thread_id.is_some()
        {
            return Ok(false);
        }
        flow.thread_id = Some(thread_id);
        self.engine.mutate_keyed(KeyedMutation::new(
            self.flows,
            RecordKey::new(pending.flow_id.clone()),
            flow,
        ))?;
        Ok(true)
    }

    fn confirm_start(&self, flow_id: &str) -> Result<Response, StoreError> {
        let Some(mut flow) = self.flow(flow_id)? else {
            return Ok(Response::StartRejected(StartRejection::LaunchRefused));
        };
        if flow.lifecycle != FlowLifecycle::Pending || flow.thread_id.is_none() {
            return Ok(Response::StartRejected(StartRejection::LaunchRefused));
        }
        flow.lifecycle = FlowLifecycle::Active;
        flow.generation = 1;
        flow.endpoint_selection = EndpointSelection::Available(signal_flow::Available_Data {
            endpoint_path: "/home/li/.codex/app-server-control/app-server-control.sock".into(),
            route_readiness: RouteReadiness::Ready,
        });
        let origin_clue = flow.origin.clone();
        let session_id = flow.thread_id.clone().expect("checked thread identity");
        self.engine.mutate_keyed(KeyedMutation::new(
            self.flows,
            RecordKey::new(flow_id),
            flow,
        ))?;
        Ok(Response::Started(Started {
            flow_id: flow_id.into(),
            session_id,
            origin_clue,
        }))
    }

    fn restart(&self, authorization: RestartAuthorization) -> Result<Response, StoreError> {
        let Some(mut flow) = self.flow(&authorization.flow_id)? else {
            return Ok(Response::RestartRejected(RestartRejection::UnknownFlow));
        };
        if flow.flow_id != authorization.authority_flow_id
            || flow.thread_id.as_deref() != Some(&authorization.thread_id)
        {
            return Ok(Response::RestartRejected(
                RestartRejection::ProvenanceMismatch,
            ));
        }
        flow.lifecycle = FlowLifecycle::Active;
        let Some(next_generation) = flow.generation.checked_add(1) else {
            return Ok(Response::RestartRejected(RestartRejection::ResumeRefused));
        };
        flow.generation = next_generation;
        let generation = flow.generation;
        let commit = self.engine.begin_atomic_commit().mutate(self.flows, flow);
        if let Some(mut candidate) = self.verified_binding(&authorization.flow_id)? {
            if !candidate.consumed {
                candidate.consumed = true;
                self.engine
                    .commit_atomic(commit.mutate(self.verified_bindings, candidate))?;
            } else {
                self.engine.commit_atomic(commit)?;
            }
        } else {
            self.engine.commit_atomic(commit)?;
        }
        Ok(Response::Restarted(Restarted {
            flow_id: authorization.flow_id,
            session_id: authorization.thread_id,
            generation: generation as i64,
        }))
    }
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        process::Command,
        time::{Duration, Instant},
    };

    use super::{
        AcquireDelivery, AcquireDeliveryOutcome, AdmissionGate, AppliesFlowQuery,
        AuthorizesFlowRestart, BeginRefresh, BeginRefreshOutcome, BootstrapBinding,
        BootstrapBindingOutcome, ConfiguresFlowStore, ConfirmsStartedFlow, DeliveryBinding,
        DeliveryRejection, FlowStore, ManagesDeliveryPermits, OpensFlowStore,
        ReadDeliveryStateOutcome, ReadsFlowStore, ReadyReattach, ReadyReattachOutcome,
        RecordsPendingThread, RecordsRestartedFlow, RegistersFlowIdentity, ReleaseConfirmed,
        ReleaseConfirmedOutcome, ReservesPendingStart, VerifiedBindingOutcome,
        VerifiedBindingRegistration,
    };
    use meta_signal_flow::Configuration;
    use signal_flow::{OriginClue, Query, Response, Restarted, StartRequest};

    struct StoreFixture {
        directory: tempfile::TempDir,
    }

    trait OpensFixtureStore {
        fn store(&self) -> FlowStore;
    }

    impl StoreFixture {
        fn new() -> Self {
            Self {
                directory: tempfile::tempdir().expect("temporary store directory"),
            }
        }
    }

    impl OpensFixtureStore for StoreFixture {
        fn store(&self) -> FlowStore {
            <FlowStore as OpensFlowStore>::open(&self.directory.path().join("flow.sema"))
                .expect("store opens")
        }
    }

    trait StartsFixtureFlow {
        fn start(&self, store: &FlowStore) -> String;
    }

    impl StartsFixtureFlow for StoreFixture {
        fn start(&self, store: &FlowStore) -> String {
            let pending = store
                .reserve_pending_start(Query::Start(StartRequest {
                    flow_type: "codex-medium".into(),
                    origin_clue: OriginClue {
                        flow_id: "9fc62b".into(),
                        session_id: "session-1".into(),
                        turn_id: "turn-7".into(),
                    },
                }))
                .expect("start reserves")
                .expect("flow type is accepted");
            assert!(store
                .record_pending_thread(&pending, "thread-1".into())
                .expect("thread persists"));
            let response = store
                .confirm_started(&pending.flow_id)
                .expect("start confirms");
            let Response::Started(started) = response else {
                panic!("start must be accepted")
            };
            started.flow_id
        }
    }

    trait InitializesFixtureDelivery {
        fn registered_delivery(&self, store: &FlowStore, flow_id: &str) -> DeliveryBinding;
    }

    impl InitializesFixtureDelivery for StoreFixture {
        fn registered_delivery(&self, store: &FlowStore, flow_id: &str) -> DeliveryBinding {
            let node = signal_flow::FlowNode {
                flow_id: flow_id.into(),
                session_id: format!("session-{flow_id}"),
                harness_kind: signal_flow::HarnessKind::Codex,
                endpoint_selection: signal_flow::EndpointSelection::Available(
                    signal_flow::Available_Data {
                        endpoint_path: format!("/tmp/{flow_id}.sock"),
                        route_readiness: signal_flow::RouteReadiness::Ready,
                    },
                ),
                herdr_route_selection: signal_flow::HerdrRouteSelection::Available(
                    signal_flow::HerdrRoute {
                        herdr_session_name: format!("session-{flow_id}"),
                        herdr_agent_name: "recipient".into(),
                        herdr_pane_id: "w1:p2".into(),
                        herdr_terminal_id: format!("terminal-{flow_id}"),
                    },
                ),
                origin_clue: signal_flow::OriginClue {
                    flow_id: flow_id.into(),
                    session_id: format!("session-{flow_id}"),
                    turn_id: "turn-registration".into(),
                },
                flow_lifecycle: signal_flow::FlowLifecycle::Active,
            };
            store
                .register_flow(node)
                .expect("flow registration persists");
            let binding = DeliveryBinding {
                native_thread: format!("thread-{flow_id}"),
                harness_session: format!("session-{flow_id}"),
                route_identity: format!("route-{flow_id}"),
                endpoint_identity: format!("endpoint-{flow_id}"),
                process_pid: 42,
                process_start_time: 7,
            };
            assert_eq!(
                store
                    .record_verified_binding(VerifiedBindingRegistration {
                        flow_id: flow_id.into(),
                        registration_id: format!("registration-{flow_id}"),
                        binding: binding.clone(),
                        lifecycle_generation: 1,
                        expected_binding_generation: None,
                        refresh_transition_id: None,
                        readiness_receipt_id: format!("ready-{flow_id}"),
                        proof_digest: format!("digest-{flow_id}"),
                    })
                    .expect("binding registration persists"),
                VerifiedBindingOutcome::Recorded
            );
            assert_eq!(
                store
                    .bootstrap_delivery_binding(BootstrapBinding {
                        flow_id: flow_id.into(),
                        registration_id: format!("registration-{flow_id}"),
                    })
                    .expect("binding initialization persists"),
                BootstrapBindingOutcome::Initialized(super::BindingState {
                    binding: binding.clone(),
                    binding_generation: 1,
                    lifecycle_generation: 1,
                    admission: AdmissionGate::Open,
                    permit: None,
                    last_completion: None,
                })
            );
            binding
        }
    }

    fn acquire(flow_id: &str, binding: DeliveryBinding, attempt_id: &str) -> AcquireDelivery {
        AcquireDelivery {
            flow_id: flow_id.into(),
            expected_binding: binding,
            expected_binding_generation: 1,
            attempt_id: attempt_id.into(),
            source_event_identifier: format!("source-{attempt_id}"),
        }
    }

    #[test]
    fn missing_delivery_state_fails_closed_until_explicit_initialization() {
        let fixture = StoreFixture::new();
        let store = fixture.store();
        let binding = fixture.registered_delivery(&store, "delivery-missing");
        // The helper initializes it; a different registered flow has no delivery state.
        store
            .register_flow(signal_flow::FlowNode {
                flow_id: "uninitialized".into(),
                session_id: "session-uninitialized".into(),
                harness_kind: signal_flow::HarnessKind::Codex,
                endpoint_selection: signal_flow::EndpointSelection::Unavailable,
                herdr_route_selection: signal_flow::HerdrRouteSelection::Available(
                    signal_flow::HerdrRoute {
                        herdr_session_name: "uninitialized".into(),
                        herdr_agent_name: "recipient".into(),
                        herdr_pane_id: "w1:p9".into(),
                        herdr_terminal_id: "terminal-uninitialized".into(),
                    },
                ),
                origin_clue: signal_flow::OriginClue {
                    flow_id: "uninitialized".into(),
                    session_id: "session-uninitialized".into(),
                    turn_id: "turn-registration".into(),
                },
                flow_lifecycle: signal_flow::FlowLifecycle::Active,
            })
            .unwrap();
        assert_eq!(
            store
                .acquire_delivery(acquire("uninitialized", binding, "attempt-1"))
                .unwrap(),
            AcquireDeliveryOutcome::Rejected(DeliveryRejection::MissingState)
        );
    }

    #[test]
    fn acquire_before_refresh_may_finish_but_release_does_not_open_admission() {
        let fixture = StoreFixture::new();
        let store = fixture.store();
        let binding = fixture.registered_delivery(&store, "delivery-order");
        let permit = match store
            .acquire_delivery(acquire("delivery-order", binding.clone(), "attempt-1"))
            .unwrap()
        {
            AcquireDeliveryOutcome::Granted(permit) => permit,
            outcome => panic!("expected permit, got {outcome:?}"),
        };
        assert_eq!(
            store
                .begin_refresh(BeginRefresh {
                    flow_id: "delivery-order".into(),
                    expected_binding_generation: 1,
                    transition_id: "refresh-1".into(),
                })
                .unwrap(),
            BeginRefreshOutcome::Held {
                active_permit: Some(permit.clone())
            }
        );
        assert_eq!(
            store
                .release_confirmed(ReleaseConfirmed {
                    flow_id: "delivery-order".into(),
                    attempt_id: permit.attempt_id.clone(),
                    token: permit.token.clone(),
                    binding: binding.clone(),
                    expected_binding_generation: 1,
                    transport_receipt_id: "submitted-1".into(),
                })
                .unwrap(),
            ReleaseConfirmedOutcome::Released
        );
        assert_eq!(
            store
                .acquire_delivery(acquire("delivery-order", binding, "attempt-2"))
                .unwrap(),
            AcquireDeliveryOutcome::Rejected(DeliveryRejection::RefreshHeld)
        );
    }

    #[test]
    fn refresh_before_acquire_blocks_new_admission() {
        let fixture = StoreFixture::new();
        let store = fixture.store();
        let binding = fixture.registered_delivery(&store, "delivery-refresh-first");
        assert!(matches!(
            store
                .begin_refresh(BeginRefresh {
                    flow_id: "delivery-refresh-first".into(),
                    expected_binding_generation: 1,
                    transition_id: "refresh-1".into(),
                })
                .unwrap(),
            BeginRefreshOutcome::Held {
                active_permit: None
            }
        ));
        assert_eq!(
            store
                .acquire_delivery(acquire("delivery-refresh-first", binding, "attempt-1"))
                .unwrap(),
            AcquireDeliveryOutcome::Rejected(DeliveryRejection::RefreshHeld)
        );
    }

    #[test]
    fn same_attempt_is_idempotent_without_a_second_send_authority() {
        let fixture = StoreFixture::new();
        let store = fixture.store();
        let binding = fixture.registered_delivery(&store, "delivery-idempotent");
        let first = store
            .acquire_delivery(acquire("delivery-idempotent", binding.clone(), "attempt-1"))
            .unwrap();
        let second = store
            .acquire_delivery(acquire("delivery-idempotent", binding, "attempt-1"))
            .unwrap();
        let AcquireDeliveryOutcome::Granted(permit) = first else {
            panic!("first permit")
        };
        assert_eq!(second, AcquireDeliveryOutcome::AlreadyGranted(permit));
    }

    #[test]
    fn definitively_released_attempt_is_tombstoned_and_cannot_be_reacquired() {
        let fixture = StoreFixture::new();
        let store = fixture.store();
        let binding = fixture.registered_delivery(&store, "delivery-tombstone");
        let permit = match store
            .acquire_delivery(acquire("delivery-tombstone", binding.clone(), "attempt-a"))
            .unwrap()
        {
            AcquireDeliveryOutcome::Granted(permit) => permit,
            outcome => panic!("expected permit, got {outcome:?}"),
        };
        assert_eq!(
            store
                .release_confirmed(ReleaseConfirmed {
                    flow_id: "delivery-tombstone".into(),
                    attempt_id: permit.attempt_id.clone(),
                    token: permit.token.clone(),
                    binding: binding.clone(),
                    expected_binding_generation: 1,
                    transport_receipt_id: "submitted-a".into(),
                })
                .unwrap(),
            ReleaseConfirmedOutcome::Released
        );
        assert_eq!(
            store
                .acquire_delivery(acquire("delivery-tombstone", binding, "attempt-a"))
                .unwrap(),
            AcquireDeliveryOutcome::Rejected(DeliveryRejection::AttemptConflict)
        );
    }

    #[test]
    fn restart_retains_refresh_gate_and_ambiguous_permit() {
        let fixture = StoreFixture::new();
        let store = fixture.store();
        let binding = fixture.registered_delivery(&store, "delivery-restart");
        let permit = match store
            .acquire_delivery(acquire("delivery-restart", binding, "attempt-1"))
            .unwrap()
        {
            AcquireDeliveryOutcome::Granted(permit) => permit,
            outcome => panic!("expected permit, got {outcome:?}"),
        };
        store
            .begin_refresh(BeginRefresh {
                flow_id: "delivery-restart".into(),
                expected_binding_generation: 1,
                transition_id: "refresh-1".into(),
            })
            .unwrap();
        drop(store);
        let reopened = fixture.store();
        let ReadDeliveryStateOutcome::State(state) =
            reopened.read_delivery_state("delivery-restart").unwrap()
        else {
            panic!("state persists")
        };
        assert_eq!(
            state.admission,
            AdmissionGate::RefreshHeld {
                transition_id: "refresh-1".into()
            }
        );
        assert_eq!(state.permit, Some(permit));
    }

    #[test]
    fn ready_reattach_refuses_an_active_permit() {
        let fixture = StoreFixture::new();
        let store = fixture.store();
        let binding = fixture.registered_delivery(&store, "delivery-active");
        store
            .acquire_delivery(acquire("delivery-active", binding.clone(), "attempt-1"))
            .unwrap();
        store
            .begin_refresh(BeginRefresh {
                flow_id: "delivery-active".into(),
                expected_binding_generation: 1,
                transition_id: "refresh-1".into(),
            })
            .unwrap();
        assert_eq!(
            store
                .ready_reattach(ReadyReattach {
                    flow_id: "delivery-active".into(),
                    transition_id: "refresh-1".into(),
                    expected_old_binding: binding.clone(),
                    expected_old_binding_generation: 1,
                    registration_id: "registration-new".into(),
                })
                .unwrap(),
            ReadyReattachOutcome::Rejected(DeliveryRejection::ActivePermit)
        );
    }

    #[test]
    fn exact_ready_reattach_advances_once_and_replays_idempotently() {
        let fixture = StoreFixture::new();
        let store = fixture.store();
        let binding = fixture.registered_delivery(&store, "delivery-ready");
        store
            .begin_refresh(BeginRefresh {
                flow_id: "delivery-ready".into(),
                expected_binding_generation: 1,
                transition_id: "refresh-1".into(),
            })
            .unwrap();
        let request = ReadyReattach {
            flow_id: "delivery-ready".into(),
            transition_id: "refresh-1".into(),
            expected_old_binding: binding.clone(),
            expected_old_binding_generation: 1,
            registration_id: "registration-new".into(),
        };
        assert_eq!(
            store
                .record_verified_binding(VerifiedBindingRegistration {
                    flow_id: "delivery-ready".into(),
                    registration_id: "registration-new".into(),
                    binding: DeliveryBinding {
                        native_thread: "thread-new".into(),
                        ..binding
                    },
                    lifecycle_generation: 2,
                    expected_binding_generation: Some(1),
                    refresh_transition_id: Some("refresh-1".into()),
                    readiness_receipt_id: "ready-new".into(),
                    proof_digest: "digest-new".into(),
                })
                .unwrap(),
            VerifiedBindingOutcome::Recorded
        );
        assert_eq!(
            store.ready_reattach(request.clone()).unwrap(),
            ReadyReattachOutcome::Opened {
                binding_generation: 2
            }
        );
        assert_eq!(
            store.ready_reattach(request).unwrap(),
            ReadyReattachOutcome::AlreadyOpened {
                binding_generation: 2
            }
        );
    }

    #[test]
    fn ready_reattach_rejects_an_unregistered_foreign_endpoint() {
        let fixture = StoreFixture::new();
        let store = fixture.store();
        let binding = fixture.registered_delivery(&store, "delivery-foreign");
        store
            .begin_refresh(BeginRefresh {
                flow_id: "delivery-foreign".into(),
                expected_binding_generation: 1,
                transition_id: "refresh-1".into(),
            })
            .unwrap();
        assert_eq!(
            store
                .ready_reattach(ReadyReattach {
                    flow_id: "delivery-foreign".into(),
                    transition_id: "refresh-1".into(),
                    expected_old_binding: binding.clone(),
                    expected_old_binding_generation: 1,
                    registration_id: "foreign-registration".into(),
                })
                .unwrap(),
            ReadyReattachOutcome::Rejected(DeliveryRejection::BindingUnavailable)
        );
    }

    #[test]
    fn reopen_recovers_origin_and_matching_authority_restarts() {
        let fixture = StoreFixture::new();
        let flow_id = fixture.start(&fixture.store());
        let reopened = fixture.store();
        assert_eq!(
            reopened
                .record_restarted(
                    reopened
                        .authorize_restart(&flow_id, &flow_id)
                        .expect("authorization reads")
                        .expect("owner is authorized"),
                )
                .expect("restart persists"),
            Response::Restarted(Restarted {
                flow_id,
                session_id: "thread-1".into(),
                generation: 2,
            })
        );
    }

    #[test]
    fn second_live_store_open_is_refused_and_drop_allows_reopen() {
        let fixture = StoreFixture::new();
        let path = fixture.directory.path().join("flow.sema");
        let first = <FlowStore as OpensFlowStore>::open(&path).expect("first store opens");
        assert!(matches!(
            <FlowStore as OpensFlowStore>::open(&path),
            Err(StoreError::Engine(_))
        ));
        drop(first);
        <FlowStore as OpensFlowStore>::open(&path).expect("dropped store reopens");
    }

    #[cfg(unix)]
    #[test]
    fn symlink_alias_cannot_bypass_live_store_ownership() {
        let fixture = StoreFixture::new();
        let path = fixture.directory.path().join("flow.sema");
        let alias = fixture.directory.path().join("flow-alias.sema");
        let first = <FlowStore as OpensFlowStore>::open(&path).expect("first store opens");
        std::os::unix::fs::symlink(&path, &alias).expect("symlink creates");

        assert!(matches!(
            <FlowStore as OpensFlowStore>::open(&alias),
            Err(StoreError::Engine(_))
        ));
        drop(first);
        <FlowStore as OpensFlowStore>::open(&alias).expect("alias opens after owner drops");
    }

    #[test]
    fn raw_sema_engine_open_is_refused_while_flow_store_is_live() {
        let fixture = StoreFixture::new();
        let path = fixture.directory.path().join("flow.sema");
        let first = <FlowStore as OpensFlowStore>::open(&path).expect("first store opens");

        assert!(sema_engine::Engine::open(sema_engine::EngineOpen::new(
            &path,
            sema_engine::SchemaVersion::new(1),
        ))
        .is_err());
        drop(first);
        sema_engine::Engine::open(sema_engine::EngineOpen::new(
            &path,
            sema_engine::SchemaVersion::new(1),
        ))
        .expect("raw engine opens after store drops");
    }

    #[test]
    fn child_holds_store_for_crash_fixture() {
        let Ok(path) = std::env::var("FLOW_STORE_CRASH_FIXTURE_PATH") else {
            return;
        };
        let _store = <FlowStore as OpensFlowStore>::open(std::path::Path::new(&path))
            .expect("child store opens");
        fs::write(format!("{path}.ready"), b"ready").expect("child signals readiness");
        loop {
            std::thread::sleep(Duration::from_secs(1));
        }
    }

    #[test]
    fn process_death_releases_store_ownership_only_after_child_is_reaped() {
        let fixture = StoreFixture::new();
        let path = fixture.directory.path().join("flow.sema");
        let ready = format!("{}.ready", path.display());
        let mut child = Command::new(std::env::current_exe().expect("test executable"))
            .args([
                "--exact",
                "store::tests::child_holds_store_for_crash_fixture",
                "--nocapture",
            ])
            .env("FLOW_STORE_CRASH_FIXTURE_PATH", &path)
            .spawn()
            .expect("child test starts");
        let deadline = Instant::now() + Duration::from_secs(5);
        while !std::path::Path::new(&ready).exists() {
            assert!(Instant::now() < deadline, "child did not acquire the store");
            std::thread::sleep(Duration::from_millis(10));
        }
        assert!(matches!(
            <FlowStore as OpensFlowStore>::open(&path),
            Err(StoreError::Engine(_))
        ));
        child.kill().expect("terminate child holder");
        child.wait().expect("reap child holder");
        <FlowStore as OpensFlowStore>::open(&path).expect("reopen after child death");
    }

    #[test]
    fn mismatching_or_unknown_authority_is_rejected() {
        let fixture = StoreFixture::new();
        let store = fixture.store();
        let flow_id = fixture.start(&store);
        assert_eq!(
            store
                .authorize_restart(&flow_id, "9fc62b")
                .expect("parent authority evaluates"),
            None
        );
        assert_eq!(
            store
                .authorize_restart(&flow_id, "another-flow")
                .expect("authorization evaluates"),
            None
        );
        assert_eq!(
            store
                .authorize_restart("flow-unknown", "flow-unknown")
                .expect("unknown flow evaluates"),
            None
        );
    }

    #[test]
    fn pending_thread_recovers_and_restart_activates_it_without_second_launch() {
        let fixture = StoreFixture::new();
        let store = fixture.store();
        let pending = store
            .reserve_pending_start(Query::Start(StartRequest {
                flow_type: "codex-medium".into(),
                origin_clue: OriginClue {
                    flow_id: "9fc62b".into(),
                    session_id: "session-2".into(),
                    turn_id: "turn-3".into(),
                },
            }))
            .expect("reserve")
            .expect("accepted flow type");
        assert!(store
            .record_pending_thread(&pending, "thread-pending".into())
            .expect("thread persists"));
        drop(store);
        let recovered = fixture.store();
        let authorization = recovered
            .authorize_restart(&pending.flow_id, &pending.flow_id)
            .expect("pending thread reads")
            .expect("known pending thread is resumable");
        assert_eq!(authorization.thread_id, "thread-pending");
        assert_eq!(
            recovered
                .authorize_restart(&pending.flow_id, "9fc62b")
                .expect("parent authority evaluates"),
            None
        );
        assert_eq!(
            recovered
                .record_restarted(authorization)
                .expect("accepted resume activates pending flow"),
            Response::Restarted(Restarted {
                flow_id: pending.flow_id,
                session_id: "thread-pending".into(),
                generation: 1,
            })
        );
    }

    #[test]
    fn failed_thread_start_leaves_a_non_resumable_pending_record() {
        let fixture = StoreFixture::new();
        let store = fixture.store();
        let pending = store
            .reserve_pending_start(Query::Start(StartRequest {
                flow_type: "codex-medium".into(),
                origin_clue: OriginClue {
                    flow_id: "9fc62b".into(),
                    session_id: "session-3".into(),
                    turn_id: "turn-4".into(),
                },
            }))
            .expect("reserve")
            .expect("accepted flow type");
        assert_eq!(
            store
                .authorize_restart(&pending.flow_id, &pending.flow_id)
                .expect("authorization evaluates"),
            None
        );
    }

    #[test]
    fn configured_policy_is_recovered_from_the_same_store() {
        let fixture = StoreFixture::new();
        let store = fixture.store();
        let configuration = Configuration {
            ordinary_socket_path: "/tmp/ordinary-test.sock".into(),
            meta_socket_path: "/tmp/meta-test.sock".into(),
        };
        store
            .configure(configuration.clone())
            .expect("policy persists");
        drop(store);
        assert_eq!(
            fixture.store().configuration().expect("policy recovers"),
            configuration
        );
    }

    #[test]
    fn active_flow_resolves_to_its_daemon_owned_codex_session() {
        let fixture = StoreFixture::new();
        let store = fixture.store();
        let flow_id = fixture.start(&store);
        let Response::RecipientResolved(node) = store
            .apply(Query::ResolveRecipient(flow_id.clone()))
            .expect("identity resolves")
        else {
            panic!("active flow must resolve")
        };
        assert_eq!(node.flow_id, flow_id);
        assert_eq!(node.session_id, "thread-1");
        assert_eq!(node.harness_kind, signal_flow::HarnessKind::Codex);
        assert!(matches!(
            node.endpoint_selection,
            signal_flow::EndpointSelection::Available(signal_flow::Available_Data {
                route_readiness: signal_flow::RouteReadiness::Ready,
                ..
            })
        ));
        assert_eq!(
            node.herdr_route_selection,
            signal_flow::HerdrRouteSelection::Unavailable
        );
    }

    #[test]
    fn registered_existing_flow_resolves_without_a_new_launch() {
        let fixture = StoreFixture::new();
        let store = fixture.store();
        let node = signal_flow::FlowNode {
            flow_id: "da1e3f".into(),
            session_id: "claude-session".into(),
            harness_kind: signal_flow::HarnessKind::Claude,
            endpoint_selection: signal_flow::EndpointSelection::Unavailable,
            herdr_route_selection: signal_flow::HerdrRouteSelection::Available(
                signal_flow::HerdrRoute {
                    herdr_session_name: "messaging-build".into(),
                    herdr_agent_name: "recipient".into(),
                    herdr_pane_id: "w1:p2".into(),
                    herdr_terminal_id: "term-current".into(),
                },
            ),
            origin_clue: signal_flow::OriginClue {
                flow_id: "da1e3f".into(),
                session_id: "claude-session".into(),
                turn_id: "unavailable".into(),
            },
            flow_lifecycle: signal_flow::FlowLifecycle::Active,
        };
        store
            .register_flow(node.clone())
            .expect("registration persists");
        drop(store);
        assert_eq!(
            fixture
                .store()
                .apply(Query::ResolveRecipient("da1e3f".into()))
                .unwrap(),
            Response::RecipientResolved(node)
        );
    }

    #[test]
    fn conflicting_session_or_route_cannot_replace_a_binding() {
        let fixture = StoreFixture::new();
        let store = fixture.store();
        let node = signal_flow::FlowNode {
            flow_id: "da1e3f".into(),
            session_id: "da1e3f9d-full".into(),
            harness_kind: signal_flow::HarnessKind::Claude,
            endpoint_selection: signal_flow::EndpointSelection::Unavailable,
            herdr_route_selection: signal_flow::HerdrRouteSelection::Available(
                signal_flow::HerdrRoute {
                    herdr_session_name: "messaging-build".into(),
                    herdr_agent_name: "recipient".into(),
                    herdr_pane_id: "w1:p2".into(),
                    herdr_terminal_id: "term-current".into(),
                },
            ),
            origin_clue: signal_flow::OriginClue {
                flow_id: "da1e3f".into(),
                session_id: "da1e3f9d-full".into(),
                turn_id: "unavailable".into(),
            },
            flow_lifecycle: signal_flow::FlowLifecycle::Active,
        };
        assert!(matches!(
            store.register_flow(node.clone()).unwrap(),
            super::FlowRegistration::Registered(_)
        ));
        let mut conflicting_session = node.clone();
        conflicting_session.session_id = "da1e3f9d-replaced".into();
        assert_eq!(
            store.register_flow(conflicting_session).unwrap(),
            super::FlowRegistration::ConflictingBinding
        );
        let mut conflicting_route = node;
        let signal_flow::HerdrRouteSelection::Available(route) =
            &mut conflicting_route.herdr_route_selection
        else {
            panic!("fixture route")
        };
        route.herdr_terminal_id = "term-replaced".into();
        assert_eq!(
            store.register_flow(conflicting_route).unwrap(),
            super::FlowRegistration::ConflictingBinding
        );
    }

    #[test]
    fn an_existing_v5_row_reopens_unchanged_and_defaults_to_unavailable_route() {
        let fixture = StoreFixture::new();
        let store = fixture.store();
        let old_row = super::FlowRecord {
            flow_id: "old-v5".into(),
            flow_type: "claude-registered".into(),
            origin: signal_flow::OriginClue {
                flow_id: "old-v5".into(),
                session_id: "old-v5-session".into(),
                turn_id: "unavailable".into(),
            },
            thread_id: Some("old-v5-session".into()),
            harness_kind: signal_flow::HarnessKind::Claude,
            endpoint_selection: signal_flow::EndpointSelection::Unavailable,
            lifecycle: super::FlowLifecycle::Active,
            generation: 7,
        };
        store
            .engine
            .assert(sema_engine::Assertion::new(store.flows, old_row.clone()))
            .expect("v5 fixture row persists");
        drop(store);
        let reopened = fixture.store();
        assert_eq!(reopened.flow("old-v5").unwrap(), Some(old_row));
        let Response::RecipientResolved(node) = reopened
            .apply(Query::ResolveRecipient("old-v5".into()))
            .unwrap()
        else {
            panic!("old active flow resolves")
        };
        assert_eq!(
            node.herdr_route_selection,
            signal_flow::HerdrRouteSelection::Unavailable
        );
    }
}
