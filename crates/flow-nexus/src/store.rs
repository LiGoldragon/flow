//! Durable Flow Nexus identity and dispatch state.
//!
//! The ordinary Signal contract remains the public boundary.  This module
//! owns its single `.sema` store and lowers a closed `signal_flow::Query`
//! into its typed, durable records.

use std::path::Path;

use meta_signal_flow::Configuration;
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use sema_engine::{
    Assertion, Engine, EngineOpen, EngineRecord, FamilyName, KeyedMutation, QueryPlan, RecordKey,
    SchemaHash, SchemaVersion, TableDescriptor, TableName, TableReference,
};
use signal_flow::{
    CallerProof, ComposedLaunch, DeliveryHoldReason, EndpointSelection,
    FlowLifecycle as SignalFlowLifecycle, FlowNode, HarnessKind, HerdrRoute, HerdrRouteSelection,
    LaunchAttempt, LaunchAttemptPhase, LaunchAttemptReservation, NativeLaunchBinding,
    NativeLaunchIntent, NativeTargetReceipt, OriginClue, ProcessIdentity, PromptDeliveryIntent,
    PromptDeliveryResult, Query, RecipientDisposition, RecipientHold, RecipientReroute,
    RecipientResolutionRejection, RefreshAttempt, RefreshAttemptPhase, RefreshPolicy,
    RefreshRejection, RefreshRequest, RegistrationAcknowledgement, ReplacementIdempotencyKey,
    Response, RouteReadiness, StartRejection, Started,
};

const FLOW_TABLE_NAME: TableName = TableName::new("flow_nexus_flows");
const FLOW_STATE_TABLE_NAME: TableName = TableName::new("flow_nexus_state");
const FLOW_CONFIGURATION_TABLE_NAME: TableName = TableName::new("flow_nexus_configuration");
const FLOW_HERDR_ROUTE_TABLE_NAME: TableName = TableName::new("flow_nexus_herdr_routes");
const FLOW_LAUNCH_ATTEMPT_TABLE_NAME: TableName = TableName::new("flow_nexus_launch_attempts");
const FLOW_RUNTIME_TABLE_NAME: TableName = TableName::new("flow_nexus_runtime");
const FLOW_REFRESH_ATTEMPT_TABLE_NAME: TableName = TableName::new("flow_nexus_refresh_attempts");
const FLOW_ROUTE_TRANSFER_TABLE_NAME: TableName = TableName::new("flow_nexus_route_transfers");
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

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
struct StoredLaunchAttempt {
    attempt: LaunchAttempt,
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
struct StoredFlowRuntime {
    flow_id: String,
    process_identity_option: Option<ProcessIdentity>,
    native_target_receipt_option: Option<NativeTargetReceipt>,
}

impl EngineRecord for StoredFlowRuntime {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.flow_id.clone())
    }
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
struct StoredRefreshAttempt {
    attempt: RefreshAttempt,
}

impl EngineRecord for StoredRefreshAttempt {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(FlowStore::refresh_key(
            &self.attempt.replacement_idempotency_key,
        ))
    }
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
struct StoredRouteTransfer {
    predecessor_flow_id: String,
    replacement_idempotency_key: ReplacementIdempotencyKey,
}

impl EngineRecord for StoredRouteTransfer {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.predecessor_flow_id.clone())
    }
}

impl EngineRecord for StoredLaunchAttempt {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.attempt.launch_request_id.clone())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("sema-engine operation failed: {0}")]
    Engine(#[from] sema_engine::Error),
    #[error("flow nexus state record is absent or duplicated")]
    StateInvariant,
}

pub struct FlowStore {
    engine: Engine,
    flows: TableReference<FlowRecord>,
    state: TableReference<FlowStoreState>,
    configuration: TableReference<FlowStoreConfiguration>,
    herdr_routes: TableReference<FlowHerdrRouteRecord>,
    launch_attempts: TableReference<StoredLaunchAttempt>,
    flow_runtime: TableReference<StoredFlowRuntime>,
    refresh_attempts: TableReference<StoredRefreshAttempt>,
    route_transfers: TableReference<StoredRouteTransfer>,
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

/// Configuration is durable policy and shares the Nexus's sole `.sema` store.
pub trait ConfiguresFlowStore {
    fn configuration(&self) -> Result<Configuration, StoreError>;
    fn configure(&self, configuration: Configuration) -> Result<(), StoreError>;
}

pub trait RegistersFlowIdentity {
    fn register_flow(&self, flow_node: FlowNode) -> Result<FlowRegistration, StoreError>;
}

/// Reserves one correlation ID and its exact composed-prompt fingerprint.
pub trait ReservesLaunchAttempt {
    fn reserve_launch_attempt(
        &self,
        launch: &ComposedLaunch,
        origin: OriginClue,
    ) -> Result<LaunchAttemptReservation, StoreError>;
}

/// Journals the intent to create an external native seat before any pane write.
pub trait RecordsNativeLaunchIntent {
    fn record_native_launch_intent(&self, intent: NativeLaunchIntent) -> Result<bool, StoreError>;
}

/// Journals the exact native/Herdr tuple observed after the external launch.
pub trait RecordsNativeLaunchBinding {
    fn record_native_launch_binding(
        &self,
        binding: NativeLaunchBinding,
    ) -> Result<bool, StoreError>;
}

/// Journals the registration acknowledgement only when it exactly matches the binding.
pub trait RecordsRegistrationAcknowledgement {
    fn record_registration_acknowledgement(
        &self,
        acknowledgement: RegistrationAcknowledgement,
    ) -> Result<bool, StoreError>;
}

/// Persists the one-shot prompt intent before the adapter may write the prompt.
pub trait RecordsPromptDeliveryIntent {
    fn record_prompt_delivery_intent(
        &self,
        intent: PromptDeliveryIntent,
    ) -> Result<bool, StoreError>;
}

/// Persists either the authentic observed receipt or durable ambiguity.
pub trait RecordsPromptDeliveryResult {
    fn record_prompt_delivery_result(
        &self,
        result: PromptDeliveryResult,
    ) -> Result<bool, StoreError>;
}

pub trait ReadsLaunchAttempt {
    fn launch_attempt(&self, launch_request_id: &str) -> Result<Option<LaunchAttempt>, StoreError>;
}

pub trait RecordsFlowRuntimeEvidence {
    fn record_flow_runtime_evidence(
        &self,
        flow_id: &str,
        process_identity: ProcessIdentity,
        native_target_receipt: NativeTargetReceipt,
    ) -> Result<bool, StoreError>;
}

pub trait ReadsFlowRuntimeEvidence {
    fn process_identity(&self, flow_id: &str) -> Result<Option<ProcessIdentity>, StoreError>;
    fn native_target_receipt(
        &self,
        flow_id: &str,
    ) -> Result<Option<NativeTargetReceipt>, StoreError>;
}

/// Atomically journals an exact refresh request and removes the predecessor
/// from delivery admission before any replacement-side external write.
pub trait ReservesRefreshAttempt {
    fn reserve_refresh_attempt(
        &self,
        request: RefreshRequest,
        policy: RefreshPolicy,
        caller_proof: CallerProof,
    ) -> Result<Response, StoreError>;
}

trait ReadsFlowStore {
    fn state(&self) -> Result<FlowStoreState, StoreError>;
    fn flow(&self, flow_id: &str) -> Result<Option<FlowRecord>, StoreError>;
    fn herdr_route(&self, flow_id: &str) -> Result<Option<FlowHerdrRouteRecord>, StoreError>;
    fn stored_configuration(&self) -> Result<FlowStoreConfiguration, StoreError>;
    fn stored_launch_attempt(
        &self,
        launch_request_id: &str,
    ) -> Result<Option<StoredLaunchAttempt>, StoreError>;
    fn stored_flow_runtime(&self, flow_id: &str) -> Result<Option<StoredFlowRuntime>, StoreError>;
    fn stored_refresh_attempt(
        &self,
        key: &ReplacementIdempotencyKey,
    ) -> Result<Option<StoredRefreshAttempt>, StoreError>;
    fn stored_route_transfer(
        &self,
        predecessor_flow_id: &str,
    ) -> Result<Option<StoredRouteTransfer>, StoreError>;
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
    fn mutate_launch_attempt(&self, attempt: LaunchAttempt) -> Result<(), StoreError>;
}

impl FlowStore {
    fn refresh_key(key: &ReplacementIdempotencyKey) -> String {
        format!("{}:{}", key.flow_id, key.transcript_record_sha256)
    }
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
        let launch_attempts = engine.register_table(TableDescriptor::new(
            FLOW_LAUNCH_ATTEMPT_TABLE_NAME,
            FamilyName::new("flow-nexus-launch-attempt"),
            SchemaHash::for_label("flow-nexus-launch-attempt-v1"),
        ))?;
        let flow_runtime = engine.register_table(TableDescriptor::new(
            FLOW_RUNTIME_TABLE_NAME,
            FamilyName::new("flow-nexus-runtime"),
            SchemaHash::for_label("flow-nexus-runtime-v1"),
        ))?;
        let refresh_attempts = engine.register_table(TableDescriptor::new(
            FLOW_REFRESH_ATTEMPT_TABLE_NAME,
            FamilyName::new("flow-nexus-refresh-attempt"),
            SchemaHash::for_label("flow-nexus-refresh-attempt-v1"),
        ))?;
        let route_transfers = engine.register_table(TableDescriptor::new(
            FLOW_ROUTE_TRANSFER_TABLE_NAME,
            FamilyName::new("flow-nexus-route-transfer"),
            SchemaHash::for_label("flow-nexus-route-transfer-v1"),
        ))?;
        let store = Self {
            engine,
            flows,
            state,
            configuration,
            herdr_routes,
            launch_attempts,
            flow_runtime,
            refresh_attempts,
            route_transfers,
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
            Query::Start(_) => Ok(Response::StartRejected(StartRejection::NativeLaunchRefused)),
            Query::Refresh(_) => Ok(Response::RefreshRejected(
                RefreshRejection::CallerProofUnavailable,
            )),
            Query::ResolveRecipient(flow_id) => self.resolve_recipient(&flow_id),
        }
    }
}

impl ReservesPendingStart for FlowStore {
    fn reserve_pending_start(&self, query: Query) -> Result<Option<PendingLaunch>, StoreError> {
        match query {
            Query::Start(request) => self
                .reserve_start("legacy-test-start".into(), request.origin_clue)
                .map(Some),
            Query::Refresh(_) | Query::ResolveRecipient(_) => Ok(None),
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
        if flow_node.flow_lifecycle != SignalFlowLifecycle::RegisteredUnconfirmed {
            return Ok(FlowRegistration::ConflictingBinding);
        }
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
                            route,
                        },
                    ))?;
                    return Ok(FlowRegistration::Registered(Box::new(flow_node)));
                }
            }
        }
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
            lifecycle: FlowLifecycle::Pending,
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

impl ReservesLaunchAttempt for FlowStore {
    fn reserve_launch_attempt(
        &self,
        launch: &ComposedLaunch,
        origin: OriginClue,
    ) -> Result<LaunchAttemptReservation, StoreError> {
        let launch_request_id = launch.launch_profile.launch_request_id.clone();
        let prompt_sha256 = launch.first_prompt_payload.prompt_sha256.clone();
        if let Some(existing) = self.stored_launch_attempt(&launch_request_id)? {
            return Ok(
                if existing.attempt.prompt_sha256 == prompt_sha256
                    && existing.attempt.launch_profile == launch.launch_profile
                    && existing.attempt.origin_clue == origin
                {
                    LaunchAttemptReservation::Existing(existing.attempt)
                } else {
                    LaunchAttemptReservation::Conflict
                },
            );
        }
        let attempt = LaunchAttempt {
            launch_request_id,
            launch_profile: launch.launch_profile.clone(),
            prompt_sha256,
            origin_clue: origin,
            launch_attempt_phase: LaunchAttemptPhase::Reserved,
            native_launch_intent_option: None,
            native_launch_binding_option: None,
            registration_acknowledgement_option: None,
            prompt_delivery_intent_option: None,
            prompt_delivery_result_option: None,
        };
        self.engine.assert(Assertion::new(
            self.launch_attempts,
            StoredLaunchAttempt {
                attempt: attempt.clone(),
            },
        ))?;
        Ok(LaunchAttemptReservation::Reserved(attempt))
    }
}

impl RecordsNativeLaunchIntent for FlowStore {
    fn record_native_launch_intent(&self, intent: NativeLaunchIntent) -> Result<bool, StoreError> {
        let Some(mut stored) = self.stored_launch_attempt(&intent.launch_request_id)? else {
            return Ok(false);
        };
        if stored.attempt.launch_attempt_phase != LaunchAttemptPhase::Reserved
            || stored.attempt.prompt_sha256 != intent.prompt_sha256
            || stored.attempt.launch_profile.harness_kind != intent.harness_kind
            || stored.attempt.launch_profile.model_name != intent.model_name
            || stored.attempt.launch_profile.effort != intent.effort
            || stored.attempt.launch_profile.skill_name_vector != intent.skill_name_vector
            || stored.attempt.native_launch_intent_option.is_some()
        {
            return Ok(false);
        }
        stored.attempt.launch_attempt_phase = LaunchAttemptPhase::NativeLaunchIntentRecorded;
        stored.attempt.native_launch_intent_option = Some(intent);
        self.mutate_launch_attempt(stored.attempt)?;
        Ok(true)
    }
}

impl RecordsNativeLaunchBinding for FlowStore {
    fn record_native_launch_binding(
        &self,
        binding: NativeLaunchBinding,
    ) -> Result<bool, StoreError> {
        let Some(mut stored) = self.stored_launch_attempt(&binding.launch_request_id)? else {
            return Ok(false);
        };
        let Some(intent) = stored.attempt.native_launch_intent_option.as_ref() else {
            return Ok(false);
        };
        if stored.attempt.launch_attempt_phase != LaunchAttemptPhase::NativeLaunchIntentRecorded
            || intent.launch_request_id != binding.launch_request_id
            || intent.harness_kind != binding.harness_kind
            || binding.herdr_pane_binding.launch_request_id != binding.launch_request_id
            || stored.attempt.native_launch_binding_option.is_some()
        {
            return Ok(false);
        }
        stored.attempt.launch_attempt_phase = LaunchAttemptPhase::NativeBound;
        stored.attempt.native_launch_binding_option = Some(binding);
        self.mutate_launch_attempt(stored.attempt)?;
        Ok(true)
    }
}

impl RecordsRegistrationAcknowledgement for FlowStore {
    fn record_registration_acknowledgement(
        &self,
        acknowledgement: RegistrationAcknowledgement,
    ) -> Result<bool, StoreError> {
        let Some(mut stored) = self.stored_launch_attempt(&acknowledgement.launch_request_id)?
        else {
            return Ok(false);
        };
        let Some(binding) = stored.attempt.native_launch_binding_option.as_ref() else {
            return Ok(false);
        };
        if stored.attempt.launch_attempt_phase != LaunchAttemptPhase::NativeBound
            || acknowledgement.launch_request_id != binding.launch_request_id
            || acknowledgement.flow_id != binding.flow_id
            || acknowledgement.native_session_id != binding.native_session_id
            || acknowledgement.herdr_pane_binding != binding.herdr_pane_binding
            || stored.attempt.registration_acknowledgement_option.is_some()
        {
            return Ok(false);
        }
        stored.attempt.launch_attempt_phase = LaunchAttemptPhase::RegistrationAcknowledged;
        stored.attempt.registration_acknowledgement_option = Some(acknowledgement);
        self.mutate_launch_attempt(stored.attempt)?;
        Ok(true)
    }
}

impl RecordsPromptDeliveryIntent for FlowStore {
    fn record_prompt_delivery_intent(
        &self,
        intent: PromptDeliveryIntent,
    ) -> Result<bool, StoreError> {
        let Some(mut stored) = self.stored_launch_attempt(&intent.launch_request_id)? else {
            return Ok(false);
        };
        let Some(acknowledgement) = stored.attempt.registration_acknowledgement_option.as_ref()
        else {
            return Ok(false);
        };
        let Some(binding) = stored.attempt.native_launch_binding_option.as_ref() else {
            return Ok(false);
        };
        let Some(native_intent) = stored.attempt.native_launch_intent_option.as_ref() else {
            return Ok(false);
        };
        let valid_skills = intent
            .native_skill_selection_vector
            .iter()
            .map(|selection| selection.skill_name.as_str())
            .eq(native_intent.skill_name_vector.iter().map(String::as_str))
            && intent
                .native_skill_selection_vector
                .iter()
                .all(|selection| {
                    Path::new(&selection.native_skill_path).is_absolute()
                        && selection.native_skill_sha256.len() == 64
                        && selection
                            .native_skill_sha256
                            .bytes()
                            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
                });
        let valid_boundary = match &intent.native_transcript_boundary {
            signal_flow::NativeTranscriptBoundary::Existing(cursor) => {
                cursor.native_session_id == intent.native_session_id
                    && cursor.harness_kind == intent.harness_kind
                    && cursor.transcript_byte_offset >= 0
                    && !cursor.transcript_device.is_empty()
                    && cursor
                        .transcript_device
                        .bytes()
                        .all(|byte| byte.is_ascii_digit())
                    && !cursor.transcript_inode.is_empty()
                    && cursor
                        .transcript_inode
                        .bytes()
                        .all(|byte| byte.is_ascii_digit())
                    && cursor.transcript_prefix_sha256.len() == 64
                    && cursor
                        .transcript_prefix_sha256
                        .bytes()
                        .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
            }
            signal_flow::NativeTranscriptBoundary::Absent(absence) => {
                absence.native_session_id == intent.native_session_id
                    && absence.harness_kind == intent.harness_kind
                    && !absence.transcript_root_device.is_empty()
                    && absence
                        .transcript_root_device
                        .bytes()
                        .all(|byte| byte.is_ascii_digit())
                    && !absence.transcript_root_inode.is_empty()
                    && absence
                        .transcript_root_inode
                        .bytes()
                        .all(|byte| byte.is_ascii_digit())
            }
        };
        if stored.attempt.launch_attempt_phase != LaunchAttemptPhase::RegistrationAcknowledged
            || intent.prompt_sha256 != stored.attempt.prompt_sha256
            || intent.launch_request_id != acknowledgement.launch_request_id
            || intent.flow_id != acknowledgement.flow_id
            || intent.native_session_id != acknowledgement.native_session_id
            || intent.harness_kind != binding.harness_kind
            || intent.model_name != native_intent.model_name
            || intent.effort != native_intent.effort
            || !valid_skills
            || intent.herdr_pane_binding != acknowledgement.herdr_pane_binding
            || !valid_boundary
            || stored.attempt.prompt_delivery_intent_option.is_some()
        {
            return Ok(false);
        }
        stored.attempt.launch_attempt_phase = LaunchAttemptPhase::PromptIntentRecorded;
        stored.attempt.prompt_delivery_intent_option = Some(intent);
        self.mutate_launch_attempt(stored.attempt)?;
        Ok(true)
    }
}

impl RecordsPromptDeliveryResult for FlowStore {
    fn record_prompt_delivery_result(
        &self,
        result: PromptDeliveryResult,
    ) -> Result<bool, StoreError> {
        let launch_request_id = match &result {
            PromptDeliveryResult::Observed(receipt) => &receipt.launch_request_id,
            PromptDeliveryResult::Ambiguous(intent) => &intent.launch_request_id,
        };
        let Some(mut stored) = self.stored_launch_attempt(launch_request_id)? else {
            return Ok(false);
        };
        let Some(intent) = stored.attempt.prompt_delivery_intent_option.as_ref() else {
            return Ok(false);
        };
        let first_result = stored.attempt.launch_attempt_phase
            == LaunchAttemptPhase::PromptIntentRecorded
            && stored.attempt.prompt_delivery_result_option.is_none();
        let ambiguity_promotion = stored.attempt.launch_attempt_phase
            == LaunchAttemptPhase::PromptAmbiguous
            && matches!(
                stored.attempt.prompt_delivery_result_option,
                Some(PromptDeliveryResult::Ambiguous(_))
            )
            && matches!(&result, PromptDeliveryResult::Observed(_));
        let boundary_advancement = if stored.attempt.launch_attempt_phase
            == LaunchAttemptPhase::PromptAmbiguous
        {
            match (
                stored.attempt.prompt_delivery_result_option.as_ref(),
                &result,
            ) {
                (
                    Some(PromptDeliveryResult::Ambiguous(previous)),
                    PromptDeliveryResult::Ambiguous(updated),
                ) => {
                    previous.launch_request_id == updated.launch_request_id
                        && previous.prompt_sha256 == updated.prompt_sha256
                        && previous.flow_id == updated.flow_id
                        && previous.native_session_id == updated.native_session_id
                        && previous.harness_kind == updated.harness_kind
                        && previous.model_name == updated.model_name
                        && previous.effort == updated.effort
                        && previous.native_skill_selection_vector
                            == updated.native_skill_selection_vector
                        && previous.herdr_pane_binding == updated.herdr_pane_binding
                        && matches!(
                        (&previous.native_transcript_boundary, &updated.native_transcript_boundary),
                        (
                            signal_flow::NativeTranscriptBoundary::Absent(absence),
                            signal_flow::NativeTranscriptBoundary::Existing(cursor),
                        ) if absence.native_session_id == cursor.native_session_id
                            && absence.harness_kind == cursor.harness_kind
                            && cursor.transcript_byte_offset >= 0
                            && !cursor.transcript_device.is_empty()
                            && cursor.transcript_device.bytes().all(|byte| byte.is_ascii_digit())
                            && !cursor.transcript_inode.is_empty()
                            && cursor.transcript_inode.bytes().all(|byte| byte.is_ascii_digit())
                            && cursor.transcript_prefix_sha256.len() == 64
                            && cursor.transcript_prefix_sha256.bytes().all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
                        )
                }
                _ => false,
            }
        } else {
            false
        };
        if !first_result && !ambiguity_promotion && !boundary_advancement {
            return Ok(false);
        }
        let phase = match &result {
            PromptDeliveryResult::Ambiguous(observed_intent) if observed_intent == intent => {
                LaunchAttemptPhase::PromptAmbiguous
            }
            PromptDeliveryResult::Ambiguous(observed_intent) if boundary_advancement => {
                stored.attempt.prompt_delivery_intent_option = Some(observed_intent.clone());
                LaunchAttemptPhase::PromptAmbiguous
            }
            PromptDeliveryResult::Observed(receipt)
                if receipt.launch_request_id == intent.launch_request_id
                    && receipt.prompt_sha256 == intent.prompt_sha256
                    && receipt.flow_id == intent.flow_id
                    && receipt.native_session_id == intent.native_session_id
                    && !receipt.native_turn_id.is_empty()
                    && receipt.receipt_sha256.len() == 64
                    && receipt
                        .receipt_sha256
                        .bytes()
                        .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
                    && receipt.model_name == intent.model_name
                    && receipt.effort == intent.effort
                    && receipt.native_skill_selection_vector
                        == intent.native_skill_selection_vector =>
            {
                LaunchAttemptPhase::PromptObserved
            }
            PromptDeliveryResult::Observed(_) | PromptDeliveryResult::Ambiguous(_) => {
                return Ok(false);
            }
        };
        stored.attempt.launch_attempt_phase = phase;
        stored.attempt.prompt_delivery_result_option = Some(result);
        self.mutate_launch_attempt(stored.attempt)?;
        Ok(true)
    }
}

impl ReadsLaunchAttempt for FlowStore {
    fn launch_attempt(&self, launch_request_id: &str) -> Result<Option<LaunchAttempt>, StoreError> {
        Ok(self
            .stored_launch_attempt(launch_request_id)?
            .map(|stored| stored.attempt))
    }
}

impl RecordsFlowRuntimeEvidence for FlowStore {
    fn record_flow_runtime_evidence(
        &self,
        flow_id: &str,
        process_identity: ProcessIdentity,
        native_target_receipt: NativeTargetReceipt,
    ) -> Result<bool, StoreError> {
        if native_target_receipt.flow_id != flow_id || self.flow(flow_id)?.is_none() {
            return Ok(false);
        }
        let record = StoredFlowRuntime {
            flow_id: flow_id.to_owned(),
            process_identity_option: Some(process_identity),
            native_target_receipt_option: Some(native_target_receipt),
        };
        match self.stored_flow_runtime(flow_id)? {
            Some(existing) => Ok(existing == record),
            None => {
                self.engine
                    .assert(Assertion::new(self.flow_runtime, record))?;
                Ok(true)
            }
        }
    }
}

impl ReadsFlowRuntimeEvidence for FlowStore {
    fn process_identity(&self, flow_id: &str) -> Result<Option<ProcessIdentity>, StoreError> {
        Ok(self
            .stored_flow_runtime(flow_id)?
            .and_then(|record| record.process_identity_option))
    }

    fn native_target_receipt(
        &self,
        flow_id: &str,
    ) -> Result<Option<NativeTargetReceipt>, StoreError> {
        Ok(self
            .stored_flow_runtime(flow_id)?
            .and_then(|record| record.native_target_receipt_option))
    }
}

impl ReservesRefreshAttempt for FlowStore {
    fn reserve_refresh_attempt(
        &self,
        request: RefreshRequest,
        policy: RefreshPolicy,
        caller_proof: CallerProof,
    ) -> Result<Response, StoreError> {
        if self.flow(&request.flow_id)?.is_none() {
            return Ok(Response::RefreshRejected(RefreshRejection::UnknownFlow));
        }
        let key = ReplacementIdempotencyKey {
            flow_id: request.flow_id.clone(),
            transcript_record_sha256: request
                .transcript_handover_reference
                .transcript_record_sha256
                .clone(),
        };
        if let Some(transfer) = self.stored_route_transfer(&request.flow_id)? {
            let Some(existing) =
                self.stored_refresh_attempt(&transfer.replacement_idempotency_key)?
            else {
                return Ok(Response::RefreshRejected(
                    RefreshRejection::RefreshPersistenceRefused,
                ));
            };
            if transfer.replacement_idempotency_key != key {
                return Ok(Response::RefreshRejected(
                    RefreshRejection::RefreshAlreadyInProgress,
                ));
            }
            if existing.attempt.refresh_request != request
                || existing.attempt.refresh_policy != policy
                || existing.attempt.caller_proof_option.as_ref() != Some(&caller_proof)
            {
                return Ok(Response::RefreshRejected(
                    RefreshRejection::IdempotencyConflict,
                ));
            }
            return Ok(Response::RefreshProgress(existing.attempt));
        }
        if let Some(existing) = self.stored_refresh_attempt(&key)? {
            if existing.attempt.refresh_request == request
                && existing.attempt.refresh_policy == policy
                && existing.attempt.caller_proof_option.as_ref() == Some(&caller_proof)
            {
                return Ok(Response::RefreshProgress(existing.attempt));
            }
            return Ok(Response::RefreshRejected(
                RefreshRejection::IdempotencyConflict,
            ));
        }
        let attempt = RefreshAttempt {
            replacement_idempotency_key: key.clone(),
            refresh_request: request.clone(),
            refresh_policy: policy,
            refresh_attempt_phase: RefreshAttemptPhase::RouteLocked,
            flow_id_option: None,
            caller_proof_option: Some(caller_proof),
            replacement_ready_proof_option: None,
            cutover_receipt_option: None,
        };
        self.engine.commit_atomic(
            self.engine
                .begin_atomic_commit()
                .assert(
                    self.refresh_attempts,
                    StoredRefreshAttempt {
                        attempt: attempt.clone(),
                    },
                )
                .assert(
                    self.route_transfers,
                    StoredRouteTransfer {
                        predecessor_flow_id: request.flow_id,
                        replacement_idempotency_key: key,
                    },
                ),
        )?;
        Ok(Response::RefreshProgress(attempt))
    }
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

    fn stored_launch_attempt(
        &self,
        launch_request_id: &str,
    ) -> Result<Option<StoredLaunchAttempt>, StoreError> {
        let records = self
            .engine
            .match_records(QueryPlan::key(
                self.launch_attempts,
                RecordKey::new(launch_request_id),
            ))?
            .records()
            .to_vec();
        match records.as_slice() {
            [] => Ok(None),
            [attempt] => Ok(Some(attempt.clone())),
            _ => Err(StoreError::StateInvariant),
        }
    }

    fn stored_flow_runtime(&self, flow_id: &str) -> Result<Option<StoredFlowRuntime>, StoreError> {
        let records = self
            .engine
            .match_records(QueryPlan::key(self.flow_runtime, RecordKey::new(flow_id)))?
            .records()
            .to_vec();
        match records.as_slice() {
            [] => Ok(None),
            [record] => Ok(Some(record.clone())),
            _ => Err(StoreError::StateInvariant),
        }
    }

    fn stored_refresh_attempt(
        &self,
        key: &ReplacementIdempotencyKey,
    ) -> Result<Option<StoredRefreshAttempt>, StoreError> {
        let records = self
            .engine
            .match_records(QueryPlan::key(
                self.refresh_attempts,
                RecordKey::new(Self::refresh_key(key)),
            ))?
            .records()
            .to_vec();
        match records.as_slice() {
            [] => Ok(None),
            [record] => Ok(Some(record.clone())),
            _ => Err(StoreError::StateInvariant),
        }
    }

    fn stored_route_transfer(
        &self,
        predecessor_flow_id: &str,
    ) -> Result<Option<StoredRouteTransfer>, StoreError> {
        let records = self
            .engine
            .match_records(QueryPlan::key(
                self.route_transfers,
                RecordKey::new(predecessor_flow_id),
            ))?
            .records()
            .to_vec();
        match records.as_slice() {
            [] => Ok(None),
            [record] => Ok(Some(record.clone())),
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
        if let Some(transfer) = self.stored_route_transfer(flow_id)? {
            let Some(refresh) =
                self.stored_refresh_attempt(&transfer.replacement_idempotency_key)?
            else {
                return Ok(Response::RecipientResolutionRejected(
                    RecipientResolutionRejection::FlowUnavailable,
                ));
            };
            if let Some(replacement_flow_id) = refresh.attempt.flow_id_option {
                if refresh.attempt.replacement_ready_proof_option.is_some() {
                    let flow_lifecycle = if matches!(
                        refresh.attempt.refresh_attempt_phase,
                        RefreshAttemptPhase::Archived | RefreshAttemptPhase::Complete
                    ) {
                        SignalFlowLifecycle::Archived
                    } else {
                        SignalFlowLifecycle::Retiring
                    };
                    return Ok(Response::RecipientDispositioned(
                        RecipientDisposition::Reroute(RecipientReroute {
                            flow_id: flow_id.to_owned(),
                            replacement_flow_id,
                            flow_lifecycle,
                        }),
                    ));
                }
            }
            return Ok(Response::RecipientDispositioned(
                RecipientDisposition::Held(RecipientHold {
                    flow_id: flow_id.to_owned(),
                    flow_lifecycle: SignalFlowLifecycle::Retiring,
                    delivery_hold_reason: DeliveryHoldReason::ReplacementNotReady,
                }),
            ));
        }
        let lifecycle = match flow.lifecycle {
            FlowLifecycle::Active => SignalFlowLifecycle::Ready,
            FlowLifecycle::Pending => SignalFlowLifecycle::RegisteredUnconfirmed,
        };
        if lifecycle != SignalFlowLifecycle::Ready {
            return Ok(Response::RecipientDispositioned(
                RecipientDisposition::Held(RecipientHold {
                    flow_id: flow_id.to_owned(),
                    flow_lifecycle: lifecycle,
                    delivery_hold_reason: DeliveryHoldReason::NativeReceiptUnconfirmed,
                }),
            ));
        }
        Ok(Response::RecipientDispositioned(
            RecipientDisposition::Deliverable(FlowNode {
                flow_id: flow.flow_id,
                session_id,
                harness_kind: flow.harness_kind.clone(),
                endpoint_selection: flow.endpoint_selection.clone(),
                herdr_route_selection: self
                    .herdr_route(flow_id)?
                    .map(|record| HerdrRouteSelection::Available(record.route))
                    .unwrap_or(HerdrRouteSelection::Unavailable),
                origin_clue: flow.origin,
                flow_lifecycle: SignalFlowLifecycle::Ready,
            }),
        ))
    }
}

impl WritesFlowStore for FlowStore {
    fn mutate_launch_attempt(&self, attempt: LaunchAttempt) -> Result<(), StoreError> {
        self.engine.mutate_keyed(KeyedMutation::new(
            self.launch_attempts,
            RecordKey::new(attempt.launch_request_id.clone()),
            StoredLaunchAttempt { attempt },
        ))?;
        Ok(())
    }

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
            return Ok(Response::StartRejected(StartRejection::NativeLaunchRefused));
        };
        if flow.thread_id.is_none() {
            return Ok(Response::StartRejected(StartRejection::NativeLaunchRefused));
        }
        let origin_clue = flow.origin.clone();
        let session_id = flow.thread_id.clone().expect("checked thread identity");
        if flow.lifecycle == FlowLifecycle::Pending {
            flow.lifecycle = FlowLifecycle::Active;
            flow.generation = 1;
            self.engine.mutate_keyed(KeyedMutation::new(
                self.flows,
                RecordKey::new(flow_id),
                flow,
            ))?;
        }
        Ok(Response::Started(Started {
            flow_id: flow_id.into(),
            session_id,
            origin_clue,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::{
        AppliesFlowQuery, ConfiguresFlowStore, ConfirmsStartedFlow, FlowStore, OpensFlowStore,
        ReadsFlowStore, ReadsLaunchAttempt, RecordsNativeLaunchBinding, RecordsNativeLaunchIntent,
        RecordsPendingThread, RecordsPromptDeliveryIntent, RecordsPromptDeliveryResult,
        RecordsRegistrationAcknowledgement, RegistersFlowIdentity, ReservesLaunchAttempt,
        ReservesPendingStart, ReservesRefreshAttempt,
    };
    use meta_signal_flow::Configuration;
    use signal_flow::{
        CallerProof, CallerRelationship, ComposedLaunch, DeliveryHoldReason, FirstPromptPayload,
        FlowAspect, FlowLifecycle as SignalFlowLifecycle, HandoverSelection, HarnessKind,
        HerdrPaneBinding, LaunchAttemptPhase, LaunchAttemptReservation, LaunchProfile,
        NativeLaunchBinding, NativeLaunchIntent, NativeSkillSelection, NativeTargetReceipt,
        NativeTranscriptAbsence, NativeTranscriptBoundary, NativeTranscriptCursor, OriginClue,
        PowerLevel, ProcessIdentity, PromptDeliveryIntent, PromptDeliveryResult, Query,
        RecipientDisposition, RecipientHold, RefreshAttemptPhase, RefreshPolicy, RefreshRejection,
        RefreshRequest, RegistrationAcknowledgement, Response, StartRequest, TargetReceiptRequest,
        TranscriptHandoverReference, TranscriptRole,
    };

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

        fn launch_profile(&self, launch_request_id: &str) -> LaunchProfile {
            LaunchProfile {
                launch_request_id: launch_request_id.into(),
                launch_source_vector: Vec::new(),
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
            }
        }

        fn composed_launch(&self, launch_request_id: &str, prompt_sha256: &str) -> ComposedLaunch {
            ComposedLaunch {
                launch_profile: self.launch_profile(launch_request_id),
                first_prompt_payload: FirstPromptPayload {
                    first_prompt_body: "fixture prompt".into(),
                    prompt_sha256: prompt_sha256.into(),
                    first_prompt_text: "fixture prompt\nreceipt request".into(),
                },
                target_receipt_request: TargetReceiptRequest {
                    launch_request_id: launch_request_id.into(),
                    prompt_sha256: prompt_sha256.into(),
                },
            }
        }

        fn origin(&self) -> OriginClue {
            OriginClue {
                flow_id: "9fc62b".into(),
                session_id: "caller-session".into(),
                turn_id: "caller-turn".into(),
            }
        }

        fn pane(&self, launch_request_id: &str) -> HerdrPaneBinding {
            HerdrPaneBinding {
                launch_request_id: launch_request_id.into(),
                herdr_session_name: "fixture-session".into(),
                herdr_agent_name: "fixture-agent".into(),
                herdr_workspace_id: "fixture-workspace".into(),
                herdr_pane_id: "w1:p1".into(),
                herdr_terminal_id: "fixture-terminal".into(),
            }
        }
    }

    #[test]
    fn duplicate_launch_request_returns_the_durable_attempt_and_changed_fingerprint_conflicts() {
        let fixture = StoreFixture::new();
        let store = fixture.store();
        let launch = fixture.composed_launch(
            "request-once",
            "1111111111111111111111111111111111111111111111111111111111111111",
        );
        let origin = fixture.origin();
        assert!(matches!(
            store
                .reserve_launch_attempt(&launch, origin.clone())
                .expect("first reservation persists"),
            LaunchAttemptReservation::Reserved(_)
        ));
        let intent = NativeLaunchIntent {
            launch_request_id: "request-once".into(),
            prompt_sha256: launch.first_prompt_payload.prompt_sha256.clone(),
            harness_kind: HarnessKind::Codex,
            model_name: launch.launch_profile.model_name.clone(),
            effort: launch.launch_profile.effort.clone(),
            skill_name_vector: launch.launch_profile.skill_name_vector.clone(),
        };
        assert!(
            store
                .record_native_launch_intent(intent.clone())
                .expect("first external intent persists")
        );
        assert!(
            !store
                .record_native_launch_intent(intent)
                .expect("a repeated external intent is refused")
        );
        assert!(matches!(
            store
                .reserve_launch_attempt(&launch, origin.clone())
                .expect("identical duplicate reads"),
            LaunchAttemptReservation::Existing(attempt)
                if attempt.launch_attempt_phase == LaunchAttemptPhase::NativeLaunchIntentRecorded
        ));
        let mut changed_profile = launch.clone();
        changed_profile.launch_profile.effort = "high".into();
        assert_eq!(
            store
                .reserve_launch_attempt(&changed_profile, origin.clone())
                .expect("changed profile evaluates before external work"),
            LaunchAttemptReservation::Conflict
        );
        let changed = fixture.composed_launch(
            "request-once",
            "2222222222222222222222222222222222222222222222222222222222222222",
        );
        assert_eq!(
            store
                .reserve_launch_attempt(&changed, origin)
                .expect("changed duplicate evaluates"),
            LaunchAttemptReservation::Conflict
        );
    }

    #[test]
    fn prompt_intent_requires_exact_registration_and_authenticated_pre_send_boundary() {
        let fixture = StoreFixture::new();
        let store = fixture.store();
        let mut launch = fixture.composed_launch(
            "request-boundary",
            "3333333333333333333333333333333333333333333333333333333333333333",
        );
        launch.launch_profile.harness_kind = HarnessKind::Claude;
        launch.launch_profile.skill_name_vector = vec!["spirit".into(), "main-flow".into()];
        store
            .reserve_launch_attempt(&launch, fixture.origin())
            .expect("reservation persists");
        assert!(
            store
                .record_native_launch_intent(NativeLaunchIntent {
                    launch_request_id: "request-boundary".into(),
                    prompt_sha256: launch.first_prompt_payload.prompt_sha256.clone(),
                    harness_kind: HarnessKind::Claude,
                    model_name: launch.launch_profile.model_name.clone(),
                    effort: launch.launch_profile.effort.clone(),
                    skill_name_vector: launch.launch_profile.skill_name_vector.clone(),
                })
                .unwrap()
        );
        let pane = fixture.pane("request-boundary");
        let binding = NativeLaunchBinding {
            launch_request_id: "request-boundary".into(),
            flow_id: "native-flow".into(),
            native_session_id: "native-session".into(),
            harness_kind: HarnessKind::Claude,
            herdr_pane_binding: pane.clone(),
        };
        assert!(store.record_native_launch_binding(binding).unwrap());
        assert!(
            store
                .record_registration_acknowledgement(RegistrationAcknowledgement {
                    launch_request_id: "request-boundary".into(),
                    flow_id: "native-flow".into(),
                    native_session_id: "native-session".into(),
                    herdr_pane_binding: pane.clone(),
                })
                .unwrap()
        );
        let intent = PromptDeliveryIntent {
            launch_request_id: "request-boundary".into(),
            prompt_sha256: launch.first_prompt_payload.prompt_sha256,
            flow_id: "native-flow".into(),
            native_session_id: "native-session".into(),
            harness_kind: HarnessKind::Claude,
            model_name: launch.launch_profile.model_name.clone(),
            effort: launch.launch_profile.effort.clone(),
            native_skill_selection_vector: vec![
                NativeSkillSelection {
                    skill_name: "main-flow".into(),
                    native_skill_path: "/configured/main-flow/SKILL.md".into(),
                    native_skill_sha256:
                        "6666666666666666666666666666666666666666666666666666666666666666".into(),
                },
                NativeSkillSelection {
                    skill_name: "spirit".into(),
                    native_skill_path: "/configured/spirit/SKILL.md".into(),
                    native_skill_sha256:
                        "7777777777777777777777777777777777777777777777777777777777777777".into(),
                },
            ],
            herdr_pane_binding: pane,
            native_transcript_boundary: NativeTranscriptBoundary::Absent(NativeTranscriptAbsence {
                native_session_id: "different-session".into(),
                harness_kind: HarnessKind::Claude,
                transcript_root_device: "2049".into(),
                transcript_root_inode: "99143".into(),
            }),
        };
        assert!(
            !store
                .record_prompt_delivery_intent(intent.clone())
                .expect("out-of-order native skill selection is refused")
        );
        let mut exact = intent;
        exact.native_skill_selection_vector.swap(0, 1);
        assert!(
            !store
                .record_prompt_delivery_intent(exact.clone())
                .expect("mismatching boundary is refused")
        );
        let NativeTranscriptBoundary::Absent(absence) = &mut exact.native_transcript_boundary
        else {
            panic!("absence fixture")
        };
        absence.native_session_id = "native-session".into();
        assert!(
            store
                .record_prompt_delivery_intent(exact.clone())
                .expect("exact pre-send evidence persists")
        );
        assert!(
            !store
                .record_prompt_delivery_intent(exact.clone())
                .expect("one-shot prompt intent cannot be replayed")
        );
        drop(store);
        let reopened = fixture.store();
        assert_eq!(
            reopened
                .launch_attempt("request-boundary")
                .unwrap()
                .expect("journal recovers")
                .prompt_delivery_intent_option,
            Some(exact.clone())
        );
        assert!(
            reopened
                .record_prompt_delivery_result(PromptDeliveryResult::Ambiguous(exact.clone()))
                .unwrap()
        );
        let mut cursor_intent = exact.clone();
        cursor_intent.native_transcript_boundary =
            NativeTranscriptBoundary::Existing(NativeTranscriptCursor {
                native_session_id: "native-session".into(),
                harness_kind: HarnessKind::Claude,
                transcript_device: "2049".into(),
                transcript_inode: "99144".into(),
                transcript_byte_offset: 127,
                transcript_prefix_sha256:
                    "5555555555555555555555555555555555555555555555555555555555555555".into(),
            });
        assert!(
            reopened
                .record_prompt_delivery_result(PromptDeliveryResult::Ambiguous(
                    cursor_intent.clone(),
                ))
                .expect("fresh transcript advances the durable boundary")
        );
        assert_eq!(
            reopened
                .launch_attempt("request-boundary")
                .unwrap()
                .expect("advanced boundary recovers")
                .prompt_delivery_intent_option,
            Some(cursor_intent.clone())
        );
        assert!(
            !reopened
                .record_prompt_delivery_result(PromptDeliveryResult::Observed(
                    NativeTargetReceipt {
                        launch_request_id: cursor_intent.launch_request_id.clone(),
                        prompt_sha256: cursor_intent.prompt_sha256.clone(),
                        flow_id: cursor_intent.flow_id.clone(),
                        native_session_id: cursor_intent.native_session_id.clone(),
                        native_turn_id: "native-turn".into(),
                        receipt_sha256:
                            "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
                                .into(),
                        model_name: cursor_intent.model_name.clone(),
                        effort: cursor_intent.effort.clone(),
                        native_skill_selection_vector: cursor_intent
                            .native_skill_selection_vector
                            .clone(),
                    }
                ))
                .expect("caller-shaped receipt is validated before promotion")
        );
        assert!(
            reopened
                .record_prompt_delivery_result(PromptDeliveryResult::Observed(
                    NativeTargetReceipt {
                        launch_request_id: cursor_intent.launch_request_id,
                        prompt_sha256: cursor_intent.prompt_sha256,
                        flow_id: cursor_intent.flow_id,
                        native_session_id: cursor_intent.native_session_id,
                        native_turn_id: "native-turn".into(),
                        receipt_sha256:
                            "4444444444444444444444444444444444444444444444444444444444444444"
                                .into(),
                        model_name: cursor_intent.model_name,
                        effort: cursor_intent.effort,
                        native_skill_selection_vector: cursor_intent.native_skill_selection_vector,
                    }
                ))
                .expect("authentic receipt promotes ambiguity")
        );
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
                    launch_profile: self.launch_profile("legacy-start-1"),
                    origin_clue: OriginClue {
                        flow_id: "9fc62b".into(),
                        session_id: "session-1".into(),
                        turn_id: "turn-7".into(),
                    },
                }))
                .expect("start reserves")
                .expect("flow type is accepted");
            assert!(
                store
                    .record_pending_thread(&pending, "thread-1".into())
                    .expect("thread persists")
            );
            let response = store
                .confirm_started(&pending.flow_id)
                .expect("start confirms");
            let Response::Started(started) = response else {
                panic!("start must be accepted")
            };
            started.flow_id
        }
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
        let Response::RecipientDispositioned(RecipientDisposition::Deliverable(node)) = store
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
                route_readiness: signal_flow::RouteReadiness::Parked,
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
            flow_lifecycle: signal_flow::FlowLifecycle::RegisteredUnconfirmed,
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
            Response::RecipientDispositioned(RecipientDisposition::Held(
                signal_flow::RecipientHold {
                    flow_id: node.flow_id,
                    flow_lifecycle: signal_flow::FlowLifecycle::RegisteredUnconfirmed,
                    delivery_hold_reason: signal_flow::DeliveryHoldReason::NativeReceiptUnconfirmed,
                }
            ))
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
            flow_lifecycle: signal_flow::FlowLifecycle::RegisteredUnconfirmed,
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
        let Response::RecipientDispositioned(RecipientDisposition::Deliverable(node)) = reopened
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

    #[test]
    fn refresh_admission_is_idempotent_and_holds_predecessor_before_launch() {
        let fixture = StoreFixture::new();
        let store = fixture.store();
        let flow_id = fixture.start(&store);
        let process_identity = ProcessIdentity {
            process_id: 42,
            process_user_id: 1001,
            process_start_token: "9001".into(),
        };
        let caller_proof = CallerProof {
            flow_id: flow_id.clone(),
            process_identity,
            caller_relationship: CallerRelationship::Harness,
        };
        let policy = RefreshPolicy {
            maximum_handover_age_seconds: 86_400,
        };
        let mut profile = fixture.launch_profile("refresh-launch");
        profile.flow_id_option = Some(flow_id.clone());
        let request = RefreshRequest {
            flow_id: flow_id.clone(),
            caller_flow_hint: flow_id.clone(),
            transcript_handover_reference: TranscriptHandoverReference {
                harness_kind: HarnessKind::Codex,
                native_session_id: "thread-1".into(),
                native_turn_id: "turn-handoff".into(),
                transcript_item_id: "item-handoff".into(),
                transcript_role: TranscriptRole::Assistant,
                transcript_title: "Handoff — exact fixture".into(),
                transcript_timestamp_seconds: 1_700_000_000,
                transcript_record_sha256:
                    "a1e4e331d40278d0c2c1fdf2cdabd1690682bd13c1fd49dadd40c9df3dc6d6ad".into(),
                handover_selection: HandoverSelection::WholeMessage,
            },
            launch_profile: profile,
            origin_clue: OriginClue {
                flow_id: flow_id.clone(),
                session_id: "thread-1".into(),
                turn_id: "turn-caller".into(),
            },
        };

        let first = store
            .reserve_refresh_attempt(request.clone(), policy.clone(), caller_proof.clone())
            .expect("refresh admission persists");
        let Response::RefreshProgress(attempt) = &first else {
            panic!("accepted refresh returns its durable attempt")
        };
        assert_eq!(
            attempt.refresh_attempt_phase,
            RefreshAttemptPhase::RouteLocked
        );
        assert_eq!(
            store
                .apply(Query::ResolveRecipient(flow_id.clone()))
                .expect("predecessor disposition reads"),
            Response::RecipientDispositioned(RecipientDisposition::Held(RecipientHold {
                flow_id: flow_id.clone(),
                flow_lifecycle: SignalFlowLifecycle::Retiring,
                delivery_hold_reason: DeliveryHoldReason::ReplacementNotReady,
            }))
        );
        assert_eq!(
            store
                .reserve_refresh_attempt(request.clone(), policy.clone(), caller_proof.clone())
                .expect("identical retry reads journal"),
            first
        );

        let mut conflicting = request;
        conflicting.launch_profile.effort = "high".into();
        assert_eq!(
            store
                .reserve_refresh_attempt(conflicting, policy, caller_proof)
                .expect("conflict evaluates"),
            Response::RefreshRejected(RefreshRejection::IdempotencyConflict)
        );
    }
}
