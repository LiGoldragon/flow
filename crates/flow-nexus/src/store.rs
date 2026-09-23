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
    ComposedLaunch, EndpointSelection, FlowLifecycle as SignalFlowLifecycle, FlowNode, HarnessKind,
    HerdrRoute, HerdrRouteSelection, LaunchAttempt, LaunchAttemptPhase, LaunchAttemptReservation,
    NativeLaunchBinding, NativeLaunchIntent, OriginClue, PromptDeliveryIntent,
    PromptDeliveryResult, Query, RecipientResolutionRejection, RegistrationAcknowledgement,
    Response, RestartRejection, Restarted, RouteReadiness, StartRejection, Started,
};

const FLOW_TABLE_NAME: TableName = TableName::new("flow_nexus_flows");
const FLOW_STATE_TABLE_NAME: TableName = TableName::new("flow_nexus_state");
const FLOW_CONFIGURATION_TABLE_NAME: TableName = TableName::new("flow_nexus_configuration");
const FLOW_HERDR_ROUTE_TABLE_NAME: TableName = TableName::new("flow_nexus_herdr_routes");
const FLOW_LAUNCH_ATTEMPT_TABLE_NAME: TableName = TableName::new("flow_nexus_launch_attempts");
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

trait ReadsFlowStore {
    fn state(&self) -> Result<FlowStoreState, StoreError>;
    fn flow(&self, flow_id: &str) -> Result<Option<FlowRecord>, StoreError>;
    fn herdr_route(&self, flow_id: &str) -> Result<Option<FlowHerdrRouteRecord>, StoreError>;
    fn stored_configuration(&self) -> Result<FlowStoreConfiguration, StoreError>;
    fn stored_launch_attempt(
        &self,
        launch_request_id: &str,
    ) -> Result<Option<StoredLaunchAttempt>, StoreError>;
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
    fn mutate_launch_attempt(&self, attempt: LaunchAttempt) -> Result<(), StoreError>;
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
        let store = Self {
            engine,
            flows,
            state,
            configuration,
            herdr_routes,
            launch_attempts,
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
            Query::Restart(_) => Ok(Response::RestartRejected(RestartRejection::ResumeRefused)),
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
            Query::Restart(_) | Query::ResolveRecipient(_) => Ok(None),
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
                            route,
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
                    !selection.native_skill_path.is_empty()
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
        if flow.lifecycle != FlowLifecycle::Pending || flow.thread_id.is_none() {
            return Ok(Response::StartRejected(StartRejection::NativeLaunchRefused));
        }
        flow.lifecycle = FlowLifecycle::Active;
        flow.generation = 1;
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
        flow.generation += 1;
        let generation = flow.generation;
        self.engine.mutate_keyed(KeyedMutation::new(
            self.flows,
            RecordKey::new(authorization.flow_id.clone()),
            flow,
        ))?;
        Ok(Response::Restarted(Restarted {
            flow_id: authorization.flow_id,
            session_id: authorization.thread_id,
            generation: generation as i64,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::{
        AppliesFlowQuery, AuthorizesFlowRestart, ConfiguresFlowStore, ConfirmsStartedFlow,
        FlowStore, OpensFlowStore, ReadsFlowStore, ReadsLaunchAttempt, RecordsNativeLaunchBinding,
        RecordsNativeLaunchIntent, RecordsPendingThread, RecordsPromptDeliveryIntent,
        RecordsPromptDeliveryResult, RecordsRegistrationAcknowledgement, RecordsRestartedFlow,
        RegistersFlowIdentity, ReservesLaunchAttempt, ReservesPendingStart,
    };
    use meta_signal_flow::Configuration;
    use signal_flow::{
        ComposedLaunch, FirstPromptPayload, FlowAspect, HarnessKind, HerdrPaneBinding,
        LaunchAttemptPhase, LaunchAttemptReservation, LaunchProfile, NativeLaunchBinding,
        NativeLaunchIntent, NativeTargetReceipt, NativeTranscriptAbsence, NativeTranscriptBoundary,
        NativeTranscriptCursor, OriginClue, PowerLevel, PromptDeliveryIntent, PromptDeliveryResult,
        Query, RegistrationAcknowledgement, Response, Restarted, StartRequest,
        TargetReceiptRequest,
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
            native_skill_selection_vector: Vec::new(),
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
                .expect("mismatching boundary is refused")
        );
        let mut exact = intent;
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
                        native_skill_selection_vector: cursor_intent
                            .native_skill_selection_vector,
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
                launch_profile: fixture.launch_profile("legacy-start-2"),
                origin_clue: OriginClue {
                    flow_id: "9fc62b".into(),
                    session_id: "session-2".into(),
                    turn_id: "turn-3".into(),
                },
            }))
            .expect("reserve")
            .expect("accepted flow type");
        assert!(
            store
                .record_pending_thread(&pending, "thread-pending".into())
                .expect("thread persists")
        );
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
                launch_profile: fixture.launch_profile("legacy-start-3"),
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
