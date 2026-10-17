//! Durable Flow Nexus identity and dispatch state.
//!
//! The ordinary Signal contract remains the public boundary.  This module
//! owns its single `.sema` store and lowers a closed `signal_flow::Query`
//! into its typed, durable records.

use crate::store::delivery::RegistersDeliveryTables;
pub(crate) mod address_hold;
pub mod context;
mod created_pane;
pub mod delivery;
pub mod events;
pub(crate) mod launch_cleanup;
pub(crate) mod metaflow;
pub(crate) mod opened_thread;
pub(crate) mod refresh;
pub(crate) mod send_call;
use address_hold::HoldsLaunchAddress;
use created_pane::StoredLaunchPane;
pub use created_pane::{ReadsCreatedLaunchPane, RecordsCreatedLaunchPane};
use flow_ethos::State as MetaflowState;
use metaflow::{TakesLaunchAddress, WritesTakenAddress};

use delivery::{DeliveryConfiguration, DeliveryTables, RecordsDeliveries};
use std::{
    fs,
    path::Path,
    sync::{Arc, Condvar, Mutex},
};

use meta_signal_flow::{
    ConfigurationChange, ConfigurationSnapshot, ConfigureLeaveOut, ConfigureModel, ConfigureNexus,
    ConfigureOutcome, ConfigureThreshold,
};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use sema_engine::{
    Assertion, Decision, Engine, EngineOpen, EngineRecord, FamilyName, KeyedMutation,
    MemoryDefinition, MemoryUpgrades, MemoryVersion, QueryPlan, RecordKey, Retraction, SchemaHash,
    SchemaVersion, TableDescriptor, TableName, TableReference, TableRegistration, UpgradeFrom,
    UpgradeOutcome,
};
use signal_flow::{
    Address, Aspect, Caller, CallerResolutionRejection, ComposedLaunch, EndpointSelection,
    FlowLifecycle as SignalFlowLifecycle, FlowNode, HarnessKind, HerdrPaneBinding, HerdrRoute,
    HerdrRouteSelection, LaunchAttempt, LaunchAttemptPhase, LaunchAttemptReservation,
    LaunchProfile, Launched, Layer, LeaveOut, Lock, Metaflow, NativeLaunchBinding,
    NativeLaunchIntent, NativeTranscriptBoundary, Origin, PowerLevel, Process,
    PromptDeliveryIntent, PromptDeliveryResult, Query, RecipientResolutionRejection,
    RegistrationAcknowledgement, RememberedFlow, ReplaceRejection, Replaced, Request, Response,
    RestartRejection, Restarted, RouteReadiness, StartRejection,
};

const FLOW_TABLE_NAME: TableName = TableName::new("flow_nexus_flows");
const FLOW_STATE_TABLE_NAME: TableName = TableName::new("flow_nexus_state");
const FLOW_CONFIGURATION_TABLE_NAME: TableName = TableName::new("flow_nexus_configuration");
const FLOW_HERDR_ROUTE_TABLE_NAME: TableName = TableName::new("flow_nexus_herdr_routes");
const FLOW_LAUNCH_PANE_TABLE_NAME: TableName = TableName::new("flow_nexus_launch_panes");
const FLOW_LAUNCH_ATTEMPT_TABLE_NAME: TableName = TableName::new("flow_nexus_launch_attempts");
const FLOW_RUNTIME_CONFIGURATION_TABLE_NAME: TableName =
    TableName::new("flow_nexus_runtime_configuration");
const FLOW_LAUNCH_OUTCOME_TABLE_NAME: TableName = TableName::new("flow_nexus_launch_outcomes");
const FLOW_REPLACEMENT_TABLE_NAME: TableName = TableName::new("flow_nexus_replacements");
const FLOW_ROLE_TABLE_NAME: TableName = TableName::new("flow_nexus_roles");
const FLOW_QUARANTINED_LAUNCH_ATTEMPT_TABLE_NAME: TableName =
    TableName::new("flow_nexus_quarantined_launch_attempts");
const METAFLOW_TABLE_NAME: TableName = TableName::new("flow_nexus_metaflows");
const METAFLOW_PROCESS_TABLE_NAME: TableName = TableName::new("flow_nexus_metaflow_processes");
const METAFLOW_LOCK_TABLE_NAME: TableName = TableName::new("flow_nexus_metaflow_locks");
const META_NEXUS_CONFIGURATION_TABLE_NAME: TableName =
    TableName::new("flow_nexus_meta_nexus_configuration");
const META_MODEL_CONFIGURATION_TABLE_NAME: TableName =
    TableName::new("flow_nexus_meta_model_configuration");
const META_THRESHOLD_CONFIGURATION_TABLE_NAME: TableName =
    TableName::new("flow_nexus_meta_threshold_configuration");
const META_LEAVE_OUT_CONFIGURATION_TABLE_NAME: TableName =
    TableName::new("flow_nexus_meta_leave_out_configuration");
/// The tables a store before 0.35.0 kept and 0.35.0 drops when it opens:
/// the meta configuration's topic modules, which meta-signal-flow 20.0.0
/// no longer configures, and the system-prompt bundle 0.34.6 kept for each
/// Address, which a fresh wake no longer reads. Each is named with the
/// family identity it was registered under.
const RETIRED_TABLES: &[(&str, &str, &str)] = &[
    (
        "flow_nexus_meta_module_configuration",
        "flow-nexus-meta-module-configuration",
        "flow-nexus-meta-module-configuration-v1",
    ),
    (
        "flow_nexus_start_bundles",
        "flow-nexus-start-bundle",
        "flow-nexus-start-bundle-v1",
    ),
];
// These are this Flow source's authored Memory version candidates. They are
// separate from the generic Sema layout version: the registered state-family
// identity and its identity record must both match before a path is resumed.
const FLOW_SCHEMA_VERSION: u32 = 2;
const FLOW_MEMORY_SCHEMA_VERSION: u64 = 2;
const STATE_KEY: &str = "identity";
const CONFIGURATION_KEY: &str = "configured";
const RUNTIME_CONFIGURATION_KEY: &str = "runtime";

const SEMA_META_TABLE: redb::TableDefinition<&str, u64> = redb::TableDefinition::new("__sema_meta");
const SEMA_CATALOG_TABLE: redb::TableDefinition<&str, &[u8]> =
    redb::TableDefinition::new("__sema_engine_catalog");
const SEMA_ENGINE_COUNTERS_TABLE: redb::TableDefinition<&str, &[u8]> =
    redb::TableDefinition::new("__sema_engine_counters");
const FLOW_STATE_RAW_TABLE: redb::TableDefinition<String, &[u8]> =
    redb::TableDefinition::new("flow_nexus_state");
const SEMA_SCHEMA_VERSION_KEY: &str = "schema_version";
const SEMA_ENGINE_STORAGE_LAYOUT_KEY: &str = "engine_storage_layout";
// This authored Memory candidate is pinned to sema-engine 9884905, whose
// Engine storage layout is 7. A later Engine layout needs an explicit Memory
// compatibility decision; Engine::open must not silently refold an occupant.
const FLOW_ENGINE_STORAGE_LAYOUT: u64 = 7;

/// Internal runtime view used by existing Flow launch adapters. The public
/// meta contract stores and returns `ConfigurationSnapshot` instead.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Configuration {
    pub ordinary_socket_path: String,
    pub meta_socket_path: String,
    pub source_root: String,
    pub stable_codex: meta_signal_flow::CodexEndpoint,
    pub next_codex: meta_signal_flow::CodexEndpoint,
    pub harness_profile_vector: Vec<meta_signal_flow::HarnessProfile>,
    pub meta_aspects: Vec<Aspect>,
    pub message_nexus_path: String,
}

pub use flow_defaults::{DefaultConfiguration, LaysOutDefaults, ReadsAnchors};

/// The configuration a new store is seeded with, read off the default
/// layout: every path is anchored on the user's home or runtime directory,
/// and nothing names a particular user.
pub trait SeedsConfiguration {
    fn configuration(&self) -> Configuration;
    fn socket_configuration(&self) -> FlowStoreConfiguration;
    fn runtime_configuration(&self) -> RuntimeConfiguration;
    fn delivery_configuration(&self) -> DeliveryConfiguration;
}

impl SeedsConfiguration for DefaultConfiguration {
    fn configuration(&self) -> Configuration {
        self.socket_configuration().with_runtime(
            &SeedsConfiguration::runtime_configuration(self),
            &self.delivery_configuration(),
        )
    }

    fn socket_configuration(&self) -> FlowStoreConfiguration {
        FlowStoreConfiguration {
            ordinary_socket_path: self.ordinary_socket_path().to_string_lossy().into_owned(),
            meta_socket_path: self.meta_socket_path().to_string_lossy().into_owned(),
        }
    }

    fn runtime_configuration(&self) -> RuntimeConfiguration {
        RuntimeConfiguration {
            source_root: self.source_root().to_string_lossy().into_owned(),
            stable_codex: CodexEndpointConfiguration::from(self.stable_codex()),
            next_codex: CodexEndpointConfiguration::from(self.next_codex()),
        }
    }

    /// Psyche seats deploy and manage flows, so they alone among flows
    /// reach the meta socket by default. No Message Nexus executable is
    /// admitted by path until one is configured.
    fn delivery_configuration(&self) -> DeliveryConfiguration {
        use delivery::DefaultsHarnessProfile;
        DeliveryConfiguration {
            harness_profile_vector: vec![
                HarnessKind::Claude.default_profile(),
                HarnessKind::Codex.default_profile(),
            ],
            meta_aspects: vec![Aspect::Psyche],
            message_nexus_path: String::new(),
        }
    }
}

impl From<flow_defaults::DefaultCodexEndpoint> for CodexEndpointConfiguration {
    fn from(endpoint: flow_defaults::DefaultCodexEndpoint) -> Self {
        Self {
            client_path: endpoint.client_path,
            home: endpoint.home,
            socket: endpoint.socket,
            model_names: endpoint.model_names,
        }
    }
}

/// Launch configuration held in the Nexus's Sema store beside the socket
/// paths. Together they are the meta `Configuration`: seeded from
/// `DefaultConfiguration` and replaced only by meta `Configure`.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub struct RuntimeConfiguration {
    pub source_root: String,
    pub stable_codex: CodexEndpointConfiguration,
    pub next_codex: CodexEndpointConfiguration,
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub struct CodexEndpointConfiguration {
    pub client_path: String,
    pub home: String,
    pub socket: String,
    pub model_names: Vec<String>,
}

impl From<&meta_signal_flow::CodexEndpoint> for CodexEndpointConfiguration {
    fn from(endpoint: &meta_signal_flow::CodexEndpoint) -> Self {
        Self {
            client_path: endpoint.client_path.clone(),
            home: endpoint.home.clone(),
            socket: endpoint.control_socket_path.clone(),
            model_names: endpoint.model_name_vector.clone(),
        }
    }
}

impl From<&CodexEndpointConfiguration> for meta_signal_flow::CodexEndpoint {
    fn from(endpoint: &CodexEndpointConfiguration) -> Self {
        Self {
            client_path: endpoint.client_path.clone(),
            home: endpoint.home.clone(),
            control_socket_path: endpoint.socket.clone(),
            model_name_vector: endpoint.model_names.clone(),
        }
    }
}

impl From<&Configuration> for RuntimeConfiguration {
    fn from(configuration: &Configuration) -> Self {
        Self {
            source_root: configuration.source_root.clone(),
            stable_codex: CodexEndpointConfiguration::from(&configuration.stable_codex),
            next_codex: CodexEndpointConfiguration::from(&configuration.next_codex),
        }
    }
}

impl From<&ConfigureNexus> for RuntimeConfiguration {
    fn from(configuration: &ConfigureNexus) -> Self {
        Self {
            source_root: configuration.source_root.clone(),
            stable_codex: CodexEndpointConfiguration::from(&configuration.stable_codex),
            next_codex: CodexEndpointConfiguration::from(&configuration.next_codex),
        }
    }
}

impl EngineRecord for RuntimeConfiguration {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(RUNTIME_CONFIGURATION_KEY)
    }
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct FlowRecord {
    flow_id: String,
    flow_type: String,
    origin: Origin,
    thread_id: Option<String>,
    harness_kind: HarnessKind,
    endpoint_selection: EndpointSelection,
    lifecycle: FlowLifecycle,
    generation: u64,
}

/// The durable address-indexed Metaflow defined by `flow_ethos::Metaflow`.
/// Its state and queue are separate from native Flow launch records: an
/// address can remain asleep or ended after the process it named is gone.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct StoredMetaflow {
    metaflow: Metaflow,
}

impl EngineRecord for StoredMetaflow {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(FlowStore::address_record_key(&self.metaflow.address))
    }
}

impl MemoryDefinition for StoredMetaflow {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(2)
    }

    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label("flow-nexus-metaflow-v2")
    }
}

/// A metaflow as Flow kept it before 0.35.7, its queue of bare requests
/// with no sender. It is read only by the v1-to-v2 family upgrades of the
/// metaflows and of the Address holds that keep a metaflow as found.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct MetaflowBeforeSenders {
    address: Address,
    state: MetaflowState,
    past: Vec<String>,
    queue: Vec<Request>,
}

/// Each request queued before Flow kept senders is carried with its sender
/// Unknown; nothing else changes.
impl From<MetaflowBeforeSenders> for Metaflow {
    fn from(previous: MetaflowBeforeSenders) -> Self {
        Self {
            address: previous.address,
            state: previous.state,
            past: previous.past,
            queue: previous
                .queue
                .into_iter()
                .map(|request| flow_ethos::QueuedRequest {
                    sent_by: flow_ethos::SentBy::Unknown,
                    request,
                })
                .collect(),
        }
    }
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct StoredMetaflowV1 {
    metaflow: MetaflowBeforeSenders,
}

impl EngineRecord for StoredMetaflowV1 {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(FlowStore::address_record_key(&self.metaflow.address))
    }
}

impl MemoryDefinition for StoredMetaflowV1 {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(1)
    }

    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label("flow-nexus-metaflow-v1")
    }
}

impl UpgradeFrom<StoredMetaflowV1> for StoredMetaflow {
    type Refusal = std::convert::Infallible;

    fn upgrade_from(previous: StoredMetaflowV1) -> Decision<Self, Self::Refusal> {
        Decision::Ready(Self {
            metaflow: previous.metaflow.into(),
        })
    }
}

/// The process bound to an address, indexed by PID so a reused PID replaces
/// only a binding whose recorded start token no longer names the process.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct StoredMetaflowProcess {
    process: Process,
    address: Address,
}

impl EngineRecord for StoredMetaflowProcess {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(FlowStore::address_record_key(&self.address))
    }
}

/// At most one lock is held for a recipient address.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct StoredMetaflowLock {
    lock: Lock,
}

impl EngineRecord for StoredMetaflowLock {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(FlowStore::address_record_key(&self.lock.address))
    }
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
struct StoredMetaNexusConfiguration {
    nexus: ConfigureNexus,
}

impl EngineRecord for StoredMetaNexusConfiguration {
    fn record_key(&self) -> RecordKey {
        RecordKey::new("nexus")
    }
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
struct StoredMetaModelConfiguration {
    model: ConfigureModel,
}

impl EngineRecord for StoredMetaModelConfiguration {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(FlowStore::layer_key(&self.model.layer))
    }
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
struct StoredMetaThresholdConfiguration {
    threshold: ConfigureThreshold,
}

impl EngineRecord for StoredMetaThresholdConfiguration {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(FlowStore::layer_key(&self.threshold.layer))
    }
}

/// One metaflow's default leave-outs, keyed by its Address.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
struct StoredMetaLeaveOutConfiguration {
    leave_out: ConfigureLeaveOut,
}

impl EngineRecord for StoredMetaLeaveOutConfiguration {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(FlowStore::address_record_key(&self.leave_out.address))
    }
}

/// Reads the requested process and its ancestors, each with the kernel start
/// token. The request's token must match before any ancestor can identify it.

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
#[rkyv(derive(Debug))]
enum FlowLifecycle {
    Pending,
    Active,
    Stopped,
    Retired,
    Exited,
}

/// Whether a lifecycle on the wire still names a reachable seat.
///
/// Stopped, Exited and Retired are all gone, and each names who ended the
/// flow: Flow closed a pane it held, the seat's own pane went away, or an
/// owner retired it. None can be sent to, resolved as a recipient, stopped
/// or replaced, and none is a deletion.
pub trait NamesLiveFlow {
    fn is_live(&self) -> bool;
}

impl NamesLiveFlow for SignalFlowLifecycle {
    fn is_live(&self) -> bool {
        matches!(self, Self::Pending | Self::Active)
    }
}

impl NamesLiveFlow for FlowLifecycle {
    fn is_live(&self) -> bool {
        matches!(self, Self::Pending | Self::Active)
    }
}

impl EngineRecord for FlowRecord {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.flow_id.clone())
    }
}

impl MemoryDefinition for FlowRecord {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(6)
    }

    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label("flow-nexus-flow-v6")
    }
}

/// Who asked for a launch, as signal-flow 16.0.0 archived it: the calling
/// flow, its session and its turn, as the caller named them. A flow row,
/// an outcome and a launch attempt written before 17.0.0 carry it.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
struct OriginClueBeforeOrigin {
    flow_id: String,
    session_id: String,
    turn_id: String,
}

/// The Nexus's own wake named itself with this session; every other clue
/// named a calling flow.
const WAKE_ORIGIN_SESSION: &str = "flow-nexus-wake";

impl From<OriginClueBeforeOrigin> for Origin {
    fn from(clue: OriginClueBeforeOrigin) -> Self {
        if clue.session_id == WAKE_ORIGIN_SESSION || clue.flow_id.is_empty() {
            Self::Unbound
        } else {
            Self::Flow(signal_flow::FlowOrigin {
                flow_id: clue.flow_id,
                session_id: clue.session_id,
            })
        }
    }
}

/// A flow row as Flow 0.34 archived it, its origin the clue the caller
/// named. It is read only by the v5-to-v6 family upgrade.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct FlowRecordV5 {
    flow_id: String,
    flow_type: String,
    origin: OriginClueBeforeOrigin,
    thread_id: Option<String>,
    harness_kind: HarnessKind,
    endpoint_selection: EndpointSelection,
    lifecycle: FlowLifecycle,
    generation: u64,
}

impl EngineRecord for FlowRecordV5 {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.flow_id.clone())
    }
}

impl MemoryDefinition for FlowRecordV5 {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(5)
    }

    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label("flow-nexus-flow-v5")
    }
}

impl UpgradeFrom<FlowRecordV5> for FlowRecord {
    type Refusal = std::convert::Infallible;

    fn upgrade_from(previous: FlowRecordV5) -> Decision<Self, Self::Refusal> {
        Decision::Ready(Self {
            flow_id: previous.flow_id,
            flow_type: previous.flow_type,
            origin: previous.origin.into(),
            thread_id: previous.thread_id,
            harness_kind: previous.harness_kind,
            endpoint_selection: previous.endpoint_selection,
            lifecycle: previous.lifecycle,
            generation: previous.generation,
        })
    }
}

/// A flow's role: its aspect, power and model, keyed by its FlowId. It is
/// exactly what ResolveCaller answers, so the Caller is stored as it is.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
struct StoredRole {
    caller: Caller,
}

impl EngineRecord for StoredRole {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.caller.flow_id.clone())
    }
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct FlowHerdrRouteRecord {
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

impl MemoryDefinition for FlowStoreState {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(FLOW_MEMORY_SCHEMA_VERSION)
    }

    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label("flow-nexus-state-v2")
    }
}

impl EngineRecord for FlowStoreState {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(STATE_KEY)
    }
}

/// The socket half of the meta `Configuration`. Its archive is the one the
/// former two-field `Configuration` record had, so a store written before
/// the contract grew its runtime fields still reads.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub struct FlowStoreConfiguration {
    ordinary_socket_path: String,
    meta_socket_path: String,
}

trait CompletesConfiguration {
    fn with_runtime(
        self,
        runtime: &RuntimeConfiguration,
        delivery: &DeliveryConfiguration,
    ) -> Configuration;
}

impl CompletesConfiguration for FlowStoreConfiguration {
    fn with_runtime(
        self,
        runtime: &RuntimeConfiguration,
        delivery: &DeliveryConfiguration,
    ) -> Configuration {
        Configuration {
            ordinary_socket_path: self.ordinary_socket_path,
            meta_socket_path: self.meta_socket_path,
            source_root: runtime.source_root.clone(),
            stable_codex: meta_signal_flow::CodexEndpoint::from(&runtime.stable_codex),
            next_codex: meta_signal_flow::CodexEndpoint::from(&runtime.next_codex),
            harness_profile_vector: delivery.harness_profile_vector.clone(),
            meta_aspects: delivery.meta_aspects.clone(),
            message_nexus_path: delivery.message_nexus_path.clone(),
        }
    }
}

impl From<&Configuration> for FlowStoreConfiguration {
    fn from(configuration: &Configuration) -> Self {
        Self {
            ordinary_socket_path: configuration.ordinary_socket_path.clone(),
            meta_socket_path: configuration.meta_socket_path.clone(),
        }
    }
}

impl EngineRecord for FlowStoreConfiguration {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(CONFIGURATION_KEY)
    }
}

/// A launch attempt, and what it knows of the context its seat started
/// with: named by module identity, or kept from a launch before 0.35.0.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct StoredLaunchAttempt {
    attempt: LaunchAttempt,
    context: LaunchContext,
}

impl EngineRecord for StoredLaunchAttempt {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.attempt.launch_request_id.clone())
    }
}

impl MemoryDefinition for StoredLaunchAttempt {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(2)
    }

    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label("flow-nexus-launch-attempt-v2")
    }
}

/// The launch types as signal-flow 16.0.0 archived them, before 17.0.0
/// named a launch's context by module identity. A Flow 0.34 launch-attempt
/// row is read in them by the v1-to-v2 family upgrade; every earlier shape
/// carried them too, and is only read to recognize a row set aside.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
struct LaunchSourceBeforeModules {
    source_path: String,
    source_sha256: String,
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
struct NativeSkillSelectionBeforeModules {
    skill_name: String,
    native_skill_path: String,
    native_skill_sha256: String,
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
struct NativeLaunchIntentBeforeModules {
    launch_request_id: String,
    prompt_sha256: String,
    harness_kind: HarnessKind,
    model_name: String,
    effort: String,
    skill_name_vector: Vec<String>,
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
struct PromptDeliveryIntentBeforeModules {
    launch_request_id: String,
    prompt_sha256: String,
    flow_id: String,
    native_session_id: String,
    harness_kind: HarnessKind,
    model_name: String,
    effort: String,
    native_skill_selection_vector: Vec<NativeSkillSelectionBeforeModules>,
    herdr_pane_binding: HerdrPaneBinding,
    native_transcript_boundary: NativeTranscriptBoundary,
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
struct NativeTargetReceiptBeforeModules {
    launch_request_id: String,
    prompt_sha256: String,
    flow_id: String,
    native_session_id: String,
    native_turn_id: String,
    receipt_sha256: String,
    model_name: String,
    effort: String,
    native_skill_selection_vector: Vec<NativeSkillSelectionBeforeModules>,
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
#[allow(clippy::large_enum_variant)] // mirrors the archived 16.0.0 layout
enum PromptDeliveryResultBeforeModules {
    Observed(NativeTargetReceiptBeforeModules),
    Ambiguous(PromptDeliveryIntentBeforeModules),
}

/// A launch attempt as Flow 0.34 archived it with signal-flow 16.0.0,
/// before 17.0.0 named a launch's context by module identity. It is read
/// only by the v1-to-v2 family upgrade.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
struct StoredLaunchAttemptV1 {
    attempt: LaunchAttemptBeforeModules,
}

impl EngineRecord for StoredLaunchAttemptV1 {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.attempt.launch_request_id.clone())
    }
}

impl MemoryDefinition for StoredLaunchAttemptV1 {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(1)
    }

    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label("flow-nexus-launch-attempt-v1")
    }
}

impl From<NativeSkillSelectionBeforeModules> for NativeSkill {
    fn from(selection: NativeSkillSelectionBeforeModules) -> Self {
        Self {
            skill_name: selection.skill_name,
            skill_path: selection.native_skill_path,
            skill_sha256: selection.native_skill_sha256,
        }
    }
}

/// A 0.34 launch's one first message was its one part: the part's hash is
/// the prompt's. It recorded no system-prompt hash and no Stop count, and
/// left nothing out.
impl From<PromptDeliveryIntentBeforeModules> for PromptDeliveryIntent {
    fn from(intent: PromptDeliveryIntentBeforeModules) -> Self {
        Self {
            prompt_sha256_vector: vec![intent.prompt_sha256.clone()],
            launch_request_id: intent.launch_request_id,
            prompt_sha256: intent.prompt_sha256,
            flow_id: intent.flow_id,
            native_session_id: intent.native_session_id,
            harness_kind: intent.harness_kind,
            model_name: intent.model_name,
            effort: intent.effort,
            parts_submitted: 1,
            stops_reported: 0,
            leave_outs: Vec::new(),
            herdr_pane_binding: intent.herdr_pane_binding,
            native_transcript_boundary: intent.native_transcript_boundary,
        }
    }
}

impl From<PromptDeliveryResultBeforeModules> for PromptDeliveryResult {
    fn from(result: PromptDeliveryResultBeforeModules) -> Self {
        match result {
            PromptDeliveryResultBeforeModules::Observed(receipt) => {
                Self::Observed(signal_flow::NativeTargetReceipt {
                    prompt_sha256_vector: vec![receipt.prompt_sha256.clone()],
                    launch_request_id: receipt.launch_request_id,
                    prompt_sha256: receipt.prompt_sha256,
                    flow_id: receipt.flow_id,
                    native_session_id: receipt.native_session_id,
                    native_turn_id: receipt.native_turn_id,
                    receipt_sha256: receipt.receipt_sha256,
                    model_name: receipt.model_name,
                    effort: receipt.effort,
                })
            }
            PromptDeliveryResultBeforeModules::Ambiguous(intent) => Self::Ambiguous(intent.into()),
        }
    }
}

/// Every value a 0.34 launch-attempt row held is carried: what the current
/// shape names, into it; what it no longer names (the skill sources, the
/// skills, the power, the harness, the bundle file, the native skill
/// selections and the origin clue whole), into `LaunchContext::BeforeModules`.
impl UpgradeFrom<StoredLaunchAttemptV1> for StoredLaunchAttempt {
    type Refusal = std::convert::Infallible;

    fn upgrade_from(previous: StoredLaunchAttemptV1) -> Decision<Self, Self::Refusal> {
        let previous = previous.attempt;
        let profile = previous.launch_profile;
        let skills_of = |selections: &Vec<NativeSkillSelectionBeforeModules>| {
            selections
                .iter()
                .cloned()
                .map(NativeSkill::from)
                .collect::<Vec<_>>()
        };
        let intent_skills = previous
            .prompt_delivery_intent_option
            .as_ref()
            .map(|intent| skills_of(&intent.native_skill_selection_vector));
        let result_skills = previous
            .prompt_delivery_result_option
            .as_ref()
            .map(|result| match result {
                PromptDeliveryResultBeforeModules::Observed(receipt) => {
                    skills_of(&receipt.native_skill_selection_vector)
                }
                PromptDeliveryResultBeforeModules::Ambiguous(intent) => {
                    skills_of(&intent.native_skill_selection_vector)
                }
            });
        let launch_intent_skills = previous
            .native_launch_intent_option
            .as_ref()
            .map(|intent| intent.skill_name_vector.clone());
        let context = LaunchContext::BeforeModules(ContextBeforeModules {
            launch_sources: profile
                .launch_source_vector
                .into_iter()
                .map(|source| LaunchSource {
                    source_path: source.source_path,
                    source_sha256: source.source_sha256,
                })
                .collect(),
            skills: profile.skill_name_vector,
            power_level: profile.power_level,
            harness_kind: profile.harness_kind,
            bundle_file: profile.system_prompt_bundle_file,
            origin_clue: OriginClue {
                flow_id: previous.origin_clue.flow_id.clone(),
                session_id: previous.origin_clue.session_id.clone(),
                turn_id: previous.origin_clue.turn_id.clone(),
            },
            launch_intent_skills,
            intent_skills,
            result_skills,
        });
        Decision::Ready(Self {
            attempt: LaunchAttempt {
                launch_request_id: previous.launch_request_id,
                launch_profile: LaunchProfile {
                    launch_request_id: profile.launch_request_id,
                    system_modules: Vec::new(),
                    user_modules: Vec::new(),
                    address: profile.address,
                    model_name: profile.model_name,
                    effort: profile.effort,
                    flow_id_option: profile.flow_id_option,
                    remembered_flow_vector: profile.remembered_flow_vector,
                    herdr_session_name: profile.herdr_session_name,
                    leave_outs_option: Some(Vec::new()),
                    instruction_prompt: profile.instruction_prompt,
                },
                origin: previous.origin_clue.into(),
                launch_attempt_phase: previous.launch_attempt_phase,
                native_launch_intent_option: previous.native_launch_intent_option.map(|intent| {
                    NativeLaunchIntent {
                        prompt_sha256_vector: vec![intent.prompt_sha256.clone()],
                        launch_request_id: intent.launch_request_id,
                        prompt_sha256: intent.prompt_sha256,
                        system_prompt_sha256: String::new(),
                        harness_kind: intent.harness_kind,
                        model_name: intent.model_name,
                        effort: intent.effort,
                    }
                }),
                native_launch_binding_option: previous.native_launch_binding_option,
                registration_acknowledgement_option: previous.registration_acknowledgement_option,
                prompt_delivery_intent_option: previous
                    .prompt_delivery_intent_option
                    .map(PromptDeliveryIntent::from),
                prompt_delivery_result_option: previous
                    .prompt_delivery_result_option
                    .map(PromptDeliveryResult::from),
                prompt_sha256: previous.prompt_sha256,
            },
            context,
        })
    }
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
struct LaunchAttemptBeforeModules {
    launch_request_id: String,
    launch_profile: LaunchProfileBeforeModules,
    prompt_sha256: String,
    origin_clue: OriginClueBeforeOrigin,
    launch_attempt_phase: LaunchAttemptPhase,
    native_launch_intent_option: Option<NativeLaunchIntentBeforeModules>,
    native_launch_binding_option: Option<NativeLaunchBinding>,
    registration_acknowledgement_option: Option<RegistrationAcknowledgement>,
    prompt_delivery_intent_option: Option<PromptDeliveryIntentBeforeModules>,
    prompt_delivery_result_option: Option<PromptDeliveryResultBeforeModules>,
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
struct LaunchProfileBeforeModules {
    launch_request_id: String,
    launch_source_vector: Vec<LaunchSourceBeforeModules>,
    skill_name_vector: Vec<String>,
    address: Address,
    power_level: PowerLevel,
    harness_kind: HarnessKind,
    model_name: String,
    effort: String,
    flow_id_option: Option<String>,
    remembered_flow_vector: Vec<RememberedFlow>,
    herdr_session_name: String,
    system_prompt_bundle_file: String,
    instruction_prompt: String,
}

/// A launch attempt as Flow archived it before signal-flow 3.0.0 (ab70332)
/// added `SystemPromptBundleFile` to the launch profile: the same fields in
/// the same order without it. Rows of this shape are only recognized and
/// kept aside; nothing is written in it.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
struct StoredLaunchAttemptBeforeBundle {
    attempt: LaunchAttemptBeforeBundle,
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
struct LaunchAttemptBeforeBundle {
    launch_request_id: String,
    launch_profile: LaunchProfileBeforeBundle,
    prompt_sha256: String,
    origin_clue: OriginClueBeforeOrigin,
    launch_attempt_phase: LaunchAttemptPhase,
    native_launch_intent_option: Option<NativeLaunchIntentBeforeModules>,
    native_launch_binding_option: Option<NativeLaunchBinding>,
    registration_acknowledgement_option: Option<RegistrationAcknowledgement>,
    prompt_delivery_intent_option: Option<PromptDeliveryIntentBeforeModules>,
    prompt_delivery_result_option: Option<PromptDeliveryResultBeforeModules>,
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
struct LaunchProfileBeforeBundle {
    launch_request_id: String,
    launch_source_vector: Vec<LaunchSourceBeforeModules>,
    skill_name_vector: Vec<String>,
    aspect: Aspect,
    power_level: PowerLevel,
    harness_kind: HarnessKind,
    model_name: String,
    effort: String,
    flow_id_option: Option<String>,
    remembered_flow_vector: Vec<RememberedFlow>,
    herdr_session_name: String,
    instruction_prompt: String,
}

/// A launch attempt as Flow archived it before signal-flow 16.0.0 put the
/// launched seat's `Address` where the launch profile named its `Aspect`.
/// Its profile names no topic or layer, so it cannot become a current row;
/// it is only recognized and kept aside.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
struct StoredLaunchAttemptBeforeAddress {
    attempt: LaunchAttemptBeforeAddress,
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
struct LaunchAttemptBeforeAddress {
    launch_request_id: String,
    launch_profile: LaunchProfileBeforeAddress,
    prompt_sha256: String,
    origin_clue: OriginClueBeforeOrigin,
    launch_attempt_phase: LaunchAttemptPhase,
    native_launch_intent_option: Option<NativeLaunchIntentBeforeModules>,
    native_launch_binding_option: Option<NativeLaunchBinding>,
    registration_acknowledgement_option: Option<RegistrationAcknowledgement>,
    prompt_delivery_intent_option: Option<PromptDeliveryIntentBeforeModules>,
    prompt_delivery_result_option: Option<PromptDeliveryResultBeforeModules>,
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
struct LaunchProfileBeforeAddress {
    launch_request_id: String,
    launch_source_vector: Vec<LaunchSourceBeforeModules>,
    skill_name_vector: Vec<String>,
    aspect: Aspect,
    power_level: PowerLevel,
    harness_kind: HarnessKind,
    model_name: String,
    effort: String,
    flow_id_option: Option<String>,
    remembered_flow_vector: Vec<RememberedFlow>,
    herdr_session_name: String,
    system_prompt_bundle_file: String,
    instruction_prompt: String,
}

/// A launch-attempt row named to the engine only so it can be retracted when
/// it no longer reads in the current shape. Its archive is empty, so it
/// checks against any bytes; it is never asserted and never read for
/// content. An exception to typed rows, taken only for moving such a row
/// aside.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
struct UnreadLaunchAttempt;

/// The shape a launch-attempt row that no longer reads in the current shape
/// was found to have. New shapes are appended, so a kept row's shape keeps
/// reading.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub enum ArchivedLaunchAttemptShape {
    /// Written before signal-flow 3.0.0 added `SystemPromptBundleFile`.
    BeforeSystemPromptBundle,
    /// No known shape reads it; the text is the current shape's decode error.
    Unknown(String),
    /// Written before signal-flow 16.0.0 named the launched seat's Address.
    BeforeAddress,
}

/// A launch-attempt row moved out of the attempts table when the store
/// opened, kept whole: its stored key, its original archive bytes, and the
/// shape it was found to have. No earlier shape names the launched seat's
/// Address, so none is carried into the attempts table.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub struct QuarantinedLaunchAttempt {
    pub launch_request_id: String,
    /// 1 for the first row of this launch request set aside, then 2, ...
    pub quarantine_number: u64,
    pub archive: Vec<u8>,
    pub shape: ArchivedLaunchAttemptShape,
}

impl EngineRecord for QuarantinedLaunchAttempt {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(format!(
            "{}/{}",
            self.launch_request_id, self.quarantine_number
        ))
    }
}

/// The role a kept launch-attempt row of a known shape names for the flow
/// its native launch bound: what a store written before roles adopts.
trait NamesKeptLaunchRole {
    fn kept_role(&self, flow_id: &str) -> Option<Caller>;
}

impl NamesKeptLaunchRole for QuarantinedLaunchAttempt {
    fn kept_role(&self, flow_id: &str) -> Option<Caller> {
        let (binding, aspect, power_level, model_name) = match self.shape {
            ArchivedLaunchAttemptShape::BeforeAddress => {
                let kept =
                    rkyv::from_bytes::<StoredLaunchAttemptBeforeAddress, rkyv::rancor::Error>(
                        &self.archive,
                    )
                    .ok()?
                    .attempt;
                let profile = kept.launch_profile;
                (
                    kept.native_launch_binding_option,
                    profile.aspect,
                    profile.power_level,
                    profile.model_name,
                )
            }
            ArchivedLaunchAttemptShape::BeforeSystemPromptBundle => {
                let kept =
                    rkyv::from_bytes::<StoredLaunchAttemptBeforeBundle, rkyv::rancor::Error>(
                        &self.archive,
                    )
                    .ok()?
                    .attempt;
                let profile = kept.launch_profile;
                (
                    kept.native_launch_binding_option,
                    profile.aspect,
                    profile.power_level,
                    profile.model_name,
                )
            }
            ArchivedLaunchAttemptShape::Unknown(_) => return None,
        };
        binding
            .filter(|binding| binding.flow_id == flow_id)
            .map(|_| Caller {
                flow_id: flow_id.into(),
                aspect,
                power_level,
                model_name,
            })
    }
}

/// What opening the store did with a launch-attempt row that no longer read
/// in the current shape. Its Display is the line the Nexus logs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LaunchAttemptSettlement {
    /// Of a known earlier shape: moved aside whole into the quarantine
    /// table, where its shape is named.
    SetAside {
        launch_request_id: String,
        from: ArchivedLaunchAttemptShape,
    },
    /// Moved aside into the quarantine table; no known shape reads it.
    Quarantined {
        launch_request_id: String,
        decode_error: String,
    },
    /// The attempts could not be settled; the store opened without it.
    Unsettled { store_error: String },
}

impl std::fmt::Display for LaunchAttemptSettlement {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::SetAside {
                launch_request_id,
                from,
            } => write!(
                formatter,
                "LaunchAttemptSetAside.{{ {launch_request_id} {} }}",
                match from {
                    ArchivedLaunchAttemptShape::BeforeSystemPromptBundle => {
                        "BeforeSystemPromptBundle"
                    }
                    ArchivedLaunchAttemptShape::BeforeAddress => "BeforeAddress",
                    ArchivedLaunchAttemptShape::Unknown(_) => "Unknown",
                }
            ),
            Self::Quarantined {
                launch_request_id,
                decode_error,
            } => write!(
                formatter,
                "LaunchAttemptQuarantined.{{ {launch_request_id} «{decode_error}» }}"
            ),
            Self::Unsettled { store_error } => {
                write!(formatter, "LaunchAttemptsUnsettled.«{store_error}»")
            }
        }
    }
}

/// How a launch request settled. It is kept beside the attempt so a
/// LaunchStatus or an Observe.Launch answers it without re-running the launch.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(
    feature = "datom",
    derive(datom_codec::Datomizable, datom_codec::Composing)
)]
pub enum LaunchOutcome {
    Started(LaunchedRecord),
    Replaced(ReplacedRecord),
    StartRejected(StartRejection),
    ReplaceRejected(ReplaceRejection),
}

use crate::generated::starter::{
    ContextBeforeModules, LaunchContext, LaunchSource, NativeSkill, OriginClue,
};
pub use crate::generated::starter::{LaunchedRecord, ReplacedRecord};
use crate::generated::starter::{LaunchedRecording, NamesLaunchedRoute};

impl From<&Launched> for LaunchedRecord {
    fn from(launched: &Launched) -> Self {
        Self {
            flow_id: launched.flow_id.clone(),
            session_id: launched.session_id.clone(),
            origin: launched.origin.clone(),
        }
    }
}

impl From<&Replaced> for ReplacedRecord {
    fn from(replaced: &Replaced) -> Self {
        Self {
            flow_id: replaced.flow_id.clone(),
            launched_record: LaunchedRecord::from(&replaced.launched),
        }
    }
}

/// One marker-consistent read of the persisted launch request and its
/// currently recorded Flow, if the attempt identifies one.
pub(crate) struct LaunchInspectionRecords {
    pub(crate) launch_attempt: LaunchAttempt,
    pub(crate) launch_outcome_option: Option<LaunchOutcome>,
    pub(crate) flow_node_option: Option<FlowNode>,
}

pub trait AnswersLaunchOutcome {
    /// The reply the outcome is on the wire, naming each launched flow's
    /// Herdr route as its row holds it now.
    fn response(&self, store: &FlowStore) -> Response;
    /// The outcome as LaunchStatus reports it.
    fn status_outcome(&self, store: &FlowStore) -> signal_flow::LaunchStatusOutcome;
    /// A refused reap leaves the predecessor Stopped and the successor held;
    /// it is the one outcome a repeated Replace takes up again.
    fn awaits_reaping(&self) -> bool;
}

/// A kept launch made a wire `Launched` again, with its flow's route as
/// the flow's row holds it now.
impl NamesLaunchedRoute for FlowStore {
    fn launched<N: LaunchedRecording>(&self, input: N) -> Launched {
        let record = input.launched_record();
        Launched {
            herdr_route_selection: self
                .flow_node(&record.flow_id)
                .ok()
                .flatten()
                .map(|node| node.herdr_route_selection)
                .unwrap_or(HerdrRouteSelection::Unavailable),
            flow_id: record.flow_id,
            session_id: record.session_id,
            origin: record.origin,
        }
    }
}

impl AnswersLaunchOutcome for LaunchOutcome {
    fn response(&self, store: &FlowStore) -> Response {
        match self {
            Self::Started(started) => Response::Started(store.launched(started)),
            Self::Replaced(replaced) => Response::Replaced(Replaced {
                flow_id: replaced.flow_id.clone(),
                launched: store.launched(&replaced.launched_record),
            }),
            Self::StartRejected(rejection) => Response::StartRejected(rejection.clone()),
            Self::ReplaceRejected(rejection) => Response::ReplaceRejected(rejection.clone()),
        }
    }

    fn status_outcome(&self, store: &FlowStore) -> signal_flow::LaunchStatusOutcome {
        match self {
            Self::Started(started) => {
                signal_flow::LaunchStatusOutcome::Started(store.launched(started))
            }
            Self::Replaced(replaced) => signal_flow::LaunchStatusOutcome::Replaced(Replaced {
                flow_id: replaced.flow_id.clone(),
                launched: store.launched(&replaced.launched_record),
            }),
            Self::StartRejected(rejection) => {
                signal_flow::LaunchStatusOutcome::StartRejected(rejection.clone())
            }
            Self::ReplaceRejected(rejection) => {
                signal_flow::LaunchStatusOutcome::ReplaceRejected(rejection.clone())
            }
        }
    }

    fn awaits_reaping(&self) -> bool {
        matches!(
            self,
            Self::ReplaceRejected(ReplaceRejection::ReapRefused(_))
        )
    }
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
struct StoredLaunchOutcome {
    launch_request_id: String,
    launch_outcome: LaunchOutcome,
}

impl EngineRecord for StoredLaunchOutcome {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.launch_request_id.clone())
    }
}

impl MemoryDefinition for StoredLaunchOutcome {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(5)
    }

    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label("flow-nexus-launch-outcome-v5")
    }
}

/// An outcome as Flow 0.35.3 to 0.35.5 archived it, its rejections
/// signal-flow 18's, before a refused registration or native step carried
/// its stage. This shape is read only by the v4-to-v5 family upgrade.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
struct StoredLaunchOutcomeV4 {
    launch_request_id: String,
    launch_outcome: LaunchOutcomeV4,
}

impl EngineRecord for StoredLaunchOutcomeV4 {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.launch_request_id.clone())
    }
}

impl MemoryDefinition for StoredLaunchOutcomeV4 {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(4)
    }

    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label("flow-nexus-launch-outcome-v4")
    }
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
enum LaunchOutcomeV4 {
    Started(LaunchedRecord),
    Replaced(ReplacedRecord),
    StartRejected(StartRejectionV18),
    ReplaceRejected(ReplaceRejectionV18),
}

/// A start rejection as signal-flow 18.0.0 archived it: a refused native
/// step and a refused registration carry no stage, in the same order.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
enum StartRejectionV18 {
    CompositionRefused,
    LaunchRequestConflict,
    LaunchPersistenceRefused,
    NativeLaunchRefused,
    BindingRefused(signal_flow::BindingRefusal),
    RegistrationRefused,
    IntentPersistenceRefused,
    OriginUnavailable,
    AddressAwake,
    AddressEnded,
    ContextRefused(signal_flow::ContextRefusal),
    LeaveOutRefused(signal_flow::LeaveOutRefusal),
    HarnessSpawnRefused(signal_flow::HarnessSpawnRefusal),
}

/// A replace rejection as signal-flow 18.0.0 archived it.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
enum ReplaceRejectionV18 {
    PredecessorAbsent,
    UnknownPredecessor,
    PredecessorStopped,
    LaunchRefused(StartRejectionV18),
    ReapRefused(signal_flow::StopRejection),
}

impl From<StartRejectionV18> for StartRejection {
    fn from(rejection: StartRejectionV18) -> Self {
        match rejection {
            StartRejectionV18::CompositionRefused => Self::CompositionRefused,
            StartRejectionV18::LaunchRequestConflict => Self::LaunchRequestConflict,
            StartRejectionV18::LaunchPersistenceRefused => Self::LaunchPersistenceRefused,
            // The stage was not kept.
            StartRejectionV18::NativeLaunchRefused => {
                Self::NativeLaunchRefused(signal_flow::NativeLaunchRefusal {
                    native_launch_stage: signal_flow::NativeLaunchStage::Unclassified,
                    native_launch_failure: signal_flow::NativeLaunchFailure::Unclassified,
                })
            }
            StartRejectionV18::BindingRefused(refusal) => Self::BindingRefused(refusal),
            StartRejectionV18::RegistrationRefused => {
                Self::RegistrationRefused(signal_flow::RegistrationRefusal::Unclassified)
            }
            StartRejectionV18::IntentPersistenceRefused => Self::IntentPersistenceRefused,
            StartRejectionV18::OriginUnavailable => Self::OriginUnavailable,
            StartRejectionV18::AddressAwake => Self::AddressAwake,
            StartRejectionV18::AddressEnded => Self::AddressEnded,
            StartRejectionV18::ContextRefused(refusal) => Self::ContextRefused(refusal),
            StartRejectionV18::LeaveOutRefused(refusal) => Self::LeaveOutRefused(refusal),
            StartRejectionV18::HarnessSpawnRefused(refusal) => Self::HarnessSpawnRefused(refusal),
        }
    }
}

impl From<ReplaceRejectionV18> for ReplaceRejection {
    fn from(rejection: ReplaceRejectionV18) -> Self {
        match rejection {
            ReplaceRejectionV18::PredecessorAbsent => Self::PredecessorAbsent,
            ReplaceRejectionV18::UnknownPredecessor => Self::UnknownPredecessor,
            ReplaceRejectionV18::PredecessorStopped => Self::PredecessorStopped,
            ReplaceRejectionV18::LaunchRefused(rejection) => Self::LaunchRefused(rejection.into()),
            ReplaceRejectionV18::ReapRefused(rejection) => Self::ReapRefused(rejection),
        }
    }
}

impl From<LaunchOutcomeV4> for LaunchOutcome {
    fn from(outcome: LaunchOutcomeV4) -> Self {
        match outcome {
            LaunchOutcomeV4::Started(launched) => Self::Started(launched),
            LaunchOutcomeV4::Replaced(replaced) => Self::Replaced(replaced),
            LaunchOutcomeV4::StartRejected(rejection) => Self::StartRejected(rejection.into()),
            LaunchOutcomeV4::ReplaceRejected(rejection) => Self::ReplaceRejected(rejection.into()),
        }
    }
}

impl UpgradeFrom<StoredLaunchOutcomeV4> for StoredLaunchOutcome {
    type Refusal = std::convert::Infallible;

    fn upgrade_from(previous: StoredLaunchOutcomeV4) -> Decision<Self, Self::Refusal> {
        Decision::Ready(Self {
            launch_request_id: previous.launch_request_id,
            launch_outcome: previous.launch_outcome.into(),
        })
    }
}

/// An outcome as Flow 0.35.2 archived it, its rejections signal-flow 17's,
/// before a refused harness start carried its cause. This shape is read
/// only by the v3-to-v5 family upgrade.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
struct StoredLaunchOutcomeV3 {
    launch_request_id: String,
    launch_outcome: LaunchOutcomeV3,
}

impl EngineRecord for StoredLaunchOutcomeV3 {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.launch_request_id.clone())
    }
}

impl MemoryDefinition for StoredLaunchOutcomeV3 {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(3)
    }

    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label("flow-nexus-launch-outcome-v3")
    }
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
enum LaunchOutcomeV3 {
    Started(LaunchedRecord),
    Replaced(ReplacedRecord),
    StartRejected(StartRejectionV17),
    ReplaceRejected(ReplaceRejectionV17),
}

/// A start rejection as signal-flow 17.0.0 archived it: every variant of
/// the current one but the refused harness start, in the same order.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
enum StartRejectionV17 {
    CompositionRefused,
    LaunchRequestConflict,
    LaunchPersistenceRefused,
    NativeLaunchRefused,
    BindingRefused(signal_flow::BindingRefusal),
    RegistrationRefused,
    IntentPersistenceRefused,
    OriginUnavailable,
    AddressAwake,
    AddressEnded,
    ContextRefused(signal_flow::ContextRefusal),
    LeaveOutRefused(signal_flow::LeaveOutRefusal),
}

/// A replace rejection as signal-flow 17.0.0 archived it.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
enum ReplaceRejectionV17 {
    PredecessorAbsent,
    UnknownPredecessor,
    PredecessorStopped,
    LaunchRefused(StartRejectionV17),
    ReapRefused(signal_flow::StopRejection),
}

impl From<StartRejectionV17> for StartRejection {
    fn from(rejection: StartRejectionV17) -> Self {
        StartRejectionV18::from(rejection).into()
    }
}

impl From<StartRejectionV17> for StartRejectionV18 {
    fn from(rejection: StartRejectionV17) -> Self {
        match rejection {
            StartRejectionV17::CompositionRefused => Self::CompositionRefused,
            StartRejectionV17::LaunchRequestConflict => Self::LaunchRequestConflict,
            StartRejectionV17::LaunchPersistenceRefused => Self::LaunchPersistenceRefused,
            StartRejectionV17::NativeLaunchRefused => Self::NativeLaunchRefused,
            StartRejectionV17::BindingRefused(refusal) => Self::BindingRefused(refusal),
            StartRejectionV17::RegistrationRefused => Self::RegistrationRefused,
            StartRejectionV17::IntentPersistenceRefused => Self::IntentPersistenceRefused,
            StartRejectionV17::OriginUnavailable => Self::OriginUnavailable,
            StartRejectionV17::AddressAwake => Self::AddressAwake,
            StartRejectionV17::AddressEnded => Self::AddressEnded,
            StartRejectionV17::ContextRefused(refusal) => Self::ContextRefused(refusal),
            StartRejectionV17::LeaveOutRefused(refusal) => Self::LeaveOutRefused(refusal),
        }
    }
}

impl From<ReplaceRejectionV17> for ReplaceRejection {
    fn from(rejection: ReplaceRejectionV17) -> Self {
        match rejection {
            ReplaceRejectionV17::PredecessorAbsent => Self::PredecessorAbsent,
            ReplaceRejectionV17::UnknownPredecessor => Self::UnknownPredecessor,
            ReplaceRejectionV17::PredecessorStopped => Self::PredecessorStopped,
            ReplaceRejectionV17::LaunchRefused(rejection) => Self::LaunchRefused(rejection.into()),
            ReplaceRejectionV17::ReapRefused(rejection) => Self::ReapRefused(rejection),
        }
    }
}

impl From<LaunchOutcomeV3> for LaunchOutcome {
    fn from(outcome: LaunchOutcomeV3) -> Self {
        match outcome {
            LaunchOutcomeV3::Started(launched) => Self::Started(launched),
            LaunchOutcomeV3::Replaced(replaced) => Self::Replaced(replaced),
            LaunchOutcomeV3::StartRejected(rejection) => Self::StartRejected(rejection.into()),
            LaunchOutcomeV3::ReplaceRejected(rejection) => Self::ReplaceRejected(rejection.into()),
        }
    }
}

impl UpgradeFrom<StoredLaunchOutcomeV3> for StoredLaunchOutcome {
    type Refusal = std::convert::Infallible;

    fn upgrade_from(previous: StoredLaunchOutcomeV3) -> Decision<Self, Self::Refusal> {
        Decision::Ready(Self {
            launch_request_id: previous.launch_request_id,
            launch_outcome: previous.launch_outcome.into(),
        })
    }
}

/// An outcome as Flow 0.34 archived it, each launched flow's origin the
/// clue its caller named. This shape is read only by the v2-to-v3 family
/// upgrade.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
struct StoredLaunchOutcomeV2 {
    launch_request_id: String,
    launch_outcome: LaunchOutcomeV2,
}

impl EngineRecord for StoredLaunchOutcomeV2 {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.launch_request_id.clone())
    }
}

impl MemoryDefinition for StoredLaunchOutcomeV2 {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(2)
    }

    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label("flow-nexus-launch-outcome-v2")
    }
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
enum LaunchOutcomeV2 {
    Started(LaunchedRecordV2),
    Replaced(ReplacedRecordV2),
    StartRejected(StartRejectionV17),
    ReplaceRejected(ReplaceRejectionV17),
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
struct LaunchedRecordV2 {
    flow_id: String,
    session_id: String,
    origin_clue: OriginClueBeforeOrigin,
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
struct ReplacedRecordV2 {
    flow_id: String,
    launched_record: LaunchedRecordV2,
}

impl From<LaunchedRecordV2> for LaunchedRecord {
    fn from(launched: LaunchedRecordV2) -> Self {
        Self {
            flow_id: launched.flow_id,
            session_id: launched.session_id,
            origin: launched.origin_clue.into(),
        }
    }
}

impl From<LaunchOutcomeV2> for LaunchOutcome {
    fn from(outcome: LaunchOutcomeV2) -> Self {
        match outcome {
            LaunchOutcomeV2::Started(launched) => Self::Started(launched.into()),
            LaunchOutcomeV2::Replaced(replaced) => Self::Replaced(ReplacedRecord {
                flow_id: replaced.flow_id,
                launched_record: replaced.launched_record.into(),
            }),
            LaunchOutcomeV2::StartRejected(rejection) => Self::StartRejected(rejection.into()),
            LaunchOutcomeV2::ReplaceRejected(rejection) => Self::ReplaceRejected(rejection.into()),
        }
    }
}

impl UpgradeFrom<StoredLaunchOutcomeV2> for StoredLaunchOutcome {
    type Refusal = std::convert::Infallible;

    fn upgrade_from(previous: StoredLaunchOutcomeV2) -> Decision<Self, Self::Refusal> {
        Decision::Ready(Self {
            launch_request_id: previous.launch_request_id,
            launch_outcome: previous.launch_outcome.into(),
        })
    }
}

/// A launch request that replaces a predecessor. While it stands without a
/// Replaced outcome, the flow its launch binds is held out of routing.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq, Hash)]
#[cfg_attr(
    feature = "datom",
    derive(datom_codec::Datomizable, datom_codec::Composing)
)]
pub struct Replacement {
    pub launch_request_id: String,
    pub predecessor: String,
}

impl EngineRecord for Replacement {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.launch_request_id.clone())
    }
}

/// The store's announcement of launch movement: every change to a launch
/// attempt or outcome, and every flow that ends or resumes, advances the
/// count and wakes whoever waits on it.
/// Nothing re-reads on a timer; a waiter sleeps until a change is announced.
#[derive(Clone, Default)]
pub struct LaunchChanges {
    count: Arc<(Mutex<u64>, Condvar)>,
}

pub trait AnnouncesLaunchChanges {
    fn announce(&self);
    fn current(&self) -> u64;
    /// Blocks until the count moves past `seen` or `deadline` passes.
    /// `true` when a change came, `false` when the deadline did. A caller
    /// that must eventually answer waits this way; one that runs for the
    /// life of the Nexus uses [`Self::after`].
    fn until(&self, seen: u64, deadline: std::time::Instant) -> bool;
    /// Blocks until the count moves past `seen`, then returns the new count.
    fn after(&self, seen: u64) -> u64;
}

impl AnnouncesLaunchChanges for LaunchChanges {
    fn announce(&self) {
        let (count, changed) = &*self.count;
        *count
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) += 1;
        changed.notify_all();
    }

    fn current(&self) -> u64 {
        *self
            .count
            .0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn until(&self, seen: u64, deadline: std::time::Instant) -> bool {
        let (count, changed) = &*self.count;
        let mut guard = count
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        while *guard == seen {
            let Some(remaining) = deadline.checked_duration_since(std::time::Instant::now()) else {
                return false;
            };
            let (next, timed_out) = changed
                .wait_timeout(guard, remaining)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            guard = next;
            if timed_out.timed_out() && *guard == seen {
                return false;
            }
        }
        true
    }

    fn after(&self, seen: u64) -> u64 {
        let (count, changed) = &*self.count;
        let guard = count
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *changed
            .wait_while(guard, |current| *current == seen)
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("sema-engine operation failed: {0}")]
    Engine(#[from] sema_engine::Error),
    #[error("launch outcome family upgrade failed: {0}")]
    LaunchOutcomeUpgrade(String),
    #[error("flow family upgrade failed: {0}")]
    FlowUpgrade(String),
    #[error("launch attempt family upgrade failed: {0}")]
    LaunchAttemptUpgrade(String),
    #[error("metaflow family upgrade failed: {0}")]
    MetaflowUpgrade(String),
    #[error("retired tables could not be dropped: {0}")]
    RetiredTableDrop(String),
    #[error("flow nexus state record is absent or duplicated")]
    StateInvariant,
    #[error("launch inspection rows changed while being read")]
    LaunchStatusReadViewChanged,
    #[error("launch inspection rows are inconsistent")]
    LaunchStatusInconsistent,
    #[error("metaflow rows changed while being read")]
    MetaflowReadViewChanged,
    #[error("configuration rows changed while being read")]
    ConfigurationReadViewChanged,
    #[error("a configuration declaration names one key twice")]
    DuplicateConfigurationEntry,
    #[error("store path must be an absolute file path")]
    InvalidStorePath,
    #[error("meta configuration is not configured")]
    NotConfigured,
    #[error("a Codex model names both the stable and the next endpoint")]
    CodexModelsOverlap,
    #[error("store {path} is not a recognized Flow Memory v2 store: {reason}")]
    UnrecognizedMemory { path: String, reason: String },
}

/// The families whose rows change shape in 0.35, upgraded from their 0.34
/// predecessor when the store opens: a flow's origin and a launched
/// outcome's origin are derived, no longer the clue the caller named; a
/// launch attempt names its context by module identity, or keeps what a
/// 0.34 launch named instead.
trait UpgradesFlowFamilies {
    /// A store before 0.35.7 keeps queues of bare requests; each is carried
    /// with its sender Unknown.
    fn upgraded_metaflows(
        &mut self,
        descriptor: TableDescriptor<StoredMetaflow>,
    ) -> Result<TableReference<StoredMetaflow>, StoreError>;
    /// The metaflow an Address hold found is carried the same way.
    fn upgraded_address_holds(
        &mut self,
        descriptor: TableDescriptor<address_hold::AddressHold>,
    ) -> Result<TableReference<address_hold::AddressHold>, StoreError>;
    fn upgraded_flows(
        &mut self,
        descriptor: TableDescriptor<FlowRecord>,
    ) -> Result<TableReference<FlowRecord>, StoreError>;
    fn upgraded_launch_outcomes(
        &mut self,
        descriptor: TableDescriptor<StoredLaunchOutcome>,
    ) -> Result<TableReference<StoredLaunchOutcome>, StoreError>;
    /// A 0.34 attempts table first moves aside whole each row older than
    /// 0.34's shape, then every row left is carried into the current shape.
    fn upgraded_launch_attempts(
        &mut self,
        descriptor: TableDescriptor<StoredLaunchAttempt>,
        quarantine: TableReference<QuarantinedLaunchAttempt>,
    ) -> Result<
        (
            TableReference<StoredLaunchAttempt>,
            Vec<LaunchAttemptSettlement>,
        ),
        StoreError,
    >;
}

impl UpgradesFlowFamilies for Engine {
    fn upgraded_metaflows(
        &mut self,
        descriptor: TableDescriptor<StoredMetaflow>,
    ) -> Result<TableReference<StoredMetaflow>, StoreError> {
        match self.upgrade_table::<StoredMetaflowV1, StoredMetaflow>(descriptor) {
            UpgradeOutcome::Upgraded(receipt) => Ok(receipt.table()),
            UpgradeOutcome::AlreadyCurrent(table) => Ok(table),
            UpgradeOutcome::Refused(refusal) => match *refusal.reason() {},
            UpgradeOutcome::Failed(failure) => {
                Err(StoreError::MetaflowUpgrade(format!("{failure:?}")))
            }
        }
    }

    fn upgraded_address_holds(
        &mut self,
        descriptor: TableDescriptor<address_hold::AddressHold>,
    ) -> Result<TableReference<address_hold::AddressHold>, StoreError> {
        match self
            .upgrade_table::<address_hold::AddressHoldV1, address_hold::AddressHold>(descriptor)
        {
            UpgradeOutcome::Upgraded(receipt) => Ok(receipt.table()),
            UpgradeOutcome::AlreadyCurrent(table) => Ok(table),
            UpgradeOutcome::Refused(refusal) => match *refusal.reason() {},
            UpgradeOutcome::Failed(failure) => {
                Err(StoreError::MetaflowUpgrade(format!("{failure:?}")))
            }
        }
    }

    fn upgraded_flows(
        &mut self,
        descriptor: TableDescriptor<FlowRecord>,
    ) -> Result<TableReference<FlowRecord>, StoreError> {
        match self.upgrade_table::<FlowRecordV5, FlowRecord>(descriptor) {
            UpgradeOutcome::Upgraded(receipt) => Ok(receipt.table()),
            UpgradeOutcome::AlreadyCurrent(table) => Ok(table),
            UpgradeOutcome::Refused(refusal) => match *refusal.reason() {},
            UpgradeOutcome::Failed(failure) => Err(StoreError::FlowUpgrade(format!("{failure:?}"))),
        }
    }

    fn upgraded_launch_outcomes(
        &mut self,
        descriptor: TableDescriptor<StoredLaunchOutcome>,
    ) -> Result<TableReference<StoredLaunchOutcome>, StoreError> {
        // A 0.35.3 to 0.35.5 store holds v4 rows, a 0.35.2 store v3 rows
        // and a 0.34 store v2 rows; the newest edge is tried first, and a
        // store it does not admit is tried on the next. An admission refusal
        // writes nothing.
        let from_v3 = descriptor.clone();
        let from_v2 = descriptor.clone();
        match self.upgrade_table::<StoredLaunchOutcomeV4, StoredLaunchOutcome>(descriptor) {
            UpgradeOutcome::Upgraded(receipt) => Ok(receipt.table()),
            UpgradeOutcome::AlreadyCurrent(table) => Ok(table),
            UpgradeOutcome::Refused(refusal) => match *refusal.reason() {},
            UpgradeOutcome::Failed(failure)
                if failure.phase() == sema_engine::StoragePhase::Admission =>
            {
                match self.upgrade_table::<StoredLaunchOutcomeV3, StoredLaunchOutcome>(from_v3) {
                    UpgradeOutcome::Upgraded(receipt) => Ok(receipt.table()),
                    UpgradeOutcome::AlreadyCurrent(table) => Ok(table),
                    UpgradeOutcome::Refused(refusal) => match *refusal.reason() {},
                    UpgradeOutcome::Failed(failure)
                        if failure.phase() == sema_engine::StoragePhase::Admission =>
                    {
                        match self
                            .upgrade_table::<StoredLaunchOutcomeV2, StoredLaunchOutcome>(from_v2)
                        {
                            UpgradeOutcome::Upgraded(receipt) => Ok(receipt.table()),
                            UpgradeOutcome::AlreadyCurrent(table) => Ok(table),
                            UpgradeOutcome::Refused(refusal) => match *refusal.reason() {},
                            UpgradeOutcome::Failed(failure) => {
                                Err(StoreError::LaunchOutcomeUpgrade(format!("{failure:?}")))
                            }
                        }
                    }
                    UpgradeOutcome::Failed(failure) => {
                        Err(StoreError::LaunchOutcomeUpgrade(format!("{failure:?}")))
                    }
                }
            }
            UpgradeOutcome::Failed(failure) => {
                Err(StoreError::LaunchOutcomeUpgrade(format!("{failure:?}")))
            }
        }
    }

    fn upgraded_launch_attempts(
        &mut self,
        descriptor: TableDescriptor<StoredLaunchAttempt>,
        quarantine: TableReference<QuarantinedLaunchAttempt>,
    ) -> Result<
        (
            TableReference<StoredLaunchAttempt>,
            Vec<LaunchAttemptSettlement>,
        ),
        StoreError,
    > {
        let predecessor = TableDescriptor::<StoredLaunchAttemptV1>::new(
            FLOW_LAUNCH_ATTEMPT_TABLE_NAME,
            FamilyName::new("flow-nexus-launch-attempt"),
            StoredLaunchAttemptV1::schema_hash(),
        )
        .family_identity();
        let settlements = if self
            .catalog()
            .family_identity(&FLOW_LAUNCH_ATTEMPT_TABLE_NAME)
            == Some(&predecessor)
        {
            // Opening never fails on a row older than 0.34's shape: each is
            // moved aside whole, and every such move is logged.
            self.settle_launch_attempts(quarantine)
                .unwrap_or_else(|error| {
                    vec![LaunchAttemptSettlement::Unsettled {
                        store_error: error.to_string(),
                    }]
                })
        } else {
            Vec::new()
        };
        let table =
            match self.upgrade_table::<StoredLaunchAttemptV1, StoredLaunchAttempt>(descriptor) {
                UpgradeOutcome::Upgraded(receipt) => receipt.table(),
                UpgradeOutcome::AlreadyCurrent(table) => table,
                UpgradeOutcome::Refused(refusal) => match *refusal.reason() {},
                UpgradeOutcome::Failed(failure) => {
                    return Err(StoreError::LaunchAttemptUpgrade(format!("{failure:?}")));
                }
            };
        Ok((table, settlements))
    }
}

pub(crate) struct MetaConfigurationTables {
    nexus: TableReference<StoredMetaNexusConfiguration>,
    models: TableReference<StoredMetaModelConfiguration>,
    thresholds: TableReference<StoredMetaThresholdConfiguration>,
    leave_outs: TableReference<StoredMetaLeaveOutConfiguration>,
}

trait RegistersMetaConfigurationTables: Sized {
    fn register(engine: &mut Engine) -> Result<Self, StoreError>;
}

impl RegistersMetaConfigurationTables for MetaConfigurationTables {
    fn register(engine: &mut Engine) -> Result<Self, StoreError> {
        Ok(Self {
            nexus: engine.register_table(TableDescriptor::new(
                META_NEXUS_CONFIGURATION_TABLE_NAME,
                FamilyName::new("flow-nexus-meta-nexus-configuration"),
                SchemaHash::for_label("flow-nexus-meta-nexus-configuration-v1"),
            ))?,
            models: engine.register_table(TableDescriptor::new(
                META_MODEL_CONFIGURATION_TABLE_NAME,
                FamilyName::new("flow-nexus-meta-model-configuration"),
                SchemaHash::for_label("flow-nexus-meta-model-configuration-v1"),
            ))?,
            thresholds: engine.register_table(TableDescriptor::new(
                META_THRESHOLD_CONFIGURATION_TABLE_NAME,
                FamilyName::new("flow-nexus-meta-threshold-configuration"),
                SchemaHash::for_label("flow-nexus-meta-threshold-configuration-v1"),
            ))?,
            leave_outs: engine.register_table(TableDescriptor::new(
                META_LEAVE_OUT_CONFIGURATION_TABLE_NAME,
                FamilyName::new("flow-nexus-meta-leave-out-configuration"),
                SchemaHash::for_label("flow-nexus-meta-leave-out-configuration-v1"),
            ))?,
        })
    }
}

pub struct FlowStore {
    engine: Engine,
    flows: TableReference<FlowRecord>,
    state: TableReference<FlowStoreState>,
    metaflows: TableReference<StoredMetaflow>,
    metaflow_processes: TableReference<StoredMetaflowProcess>,
    metaflow_locks: TableReference<StoredMetaflowLock>,
    configuration: TableReference<FlowStoreConfiguration>,
    runtime_configuration: TableReference<RuntimeConfiguration>,
    herdr_routes: TableReference<FlowHerdrRouteRecord>,
    launch_attempts: TableReference<StoredLaunchAttempt>,
    launch_panes: TableReference<StoredLaunchPane>,
    launch_outcomes: TableReference<StoredLaunchOutcome>,
    replacements: TableReference<Replacement>,
    roles: TableReference<StoredRole>,
    meta_configuration_tables: MetaConfigurationTables,
    quarantined_launch_attempts: TableReference<QuarantinedLaunchAttempt>,
    delivery_tables: DeliveryTables,
    event_tables: events::EventTables,
    context_sizes: TableReference<context::FlowContextSize>,
    address_holds: TableReference<address_hold::AddressHold>,
    refreshes: TableReference<refresh::MetaflowRefresh>,
    opened_threads: TableReference<opened_thread::OpenedLaunchThread>,
    send_calls: TableReference<crate::generated::send_tool::SendCall>,
    launch_cleanups: TableReference<launch_cleanup::LaunchCleanup>,
    pub launch_changes: LaunchChanges,
    /// What opening did with launch-attempt rows older than 0.34's shape;
    /// each is also logged.
    pub opening_settlements: Vec<LaunchAttemptSettlement>,
}

/// The one-request configuration listener's view of the same Sema store.
/// It registers only the three rows Configure owns and never seeds the rest
/// of Flow state, settles launches/deliveries, or upgrades stored families.
/// Keeping this Engine alive holds redb's exclusive writer lock while that
/// listener owns the meta socket.
pub struct ConfigurationOnlyStore {
    engine: Engine,
    configuration: TableReference<FlowStoreConfiguration>,
    runtime_configuration: TableReference<RuntimeConfiguration>,
    delivery_configuration: TableReference<DeliveryConfiguration>,
    meta_configuration_tables: MetaConfigurationTables,
}

pub trait ConfiguresOnlyStore: Sized {
    fn open(path: &Path) -> Result<Self, StoreError>;
    fn configuration_snapshot(&self) -> Result<Option<ConfigurationSnapshot>, StoreError>;
    fn configure_change(&self, change: ConfigurationChange)
    -> Result<ConfigureOutcome, StoreError>;
    fn configuration(&self, defaults: &DefaultConfiguration) -> Result<Configuration, StoreError>;
    fn configure(&self, configuration: Configuration) -> Result<Configuration, StoreError>;
}

impl ConfiguresOnlyStore for ConfigurationOnlyStore {
    /// Opens the same versioned store as normal startup without invoking its
    /// seeding, settlement, adoption, or family-upgrade path; a store that
    /// still holds a retired table drops it first.
    fn open(path: &Path) -> Result<Self, StoreError> {
        let recognized = FlowStore::recognize_flow_memory(path)?;
        let state_descriptor = TableDescriptor::<FlowStoreState>::new(
            FLOW_STATE_TABLE_NAME,
            FamilyName::new("flow-nexus-state"),
            SchemaHash::for_label("flow-nexus-state-v2"),
        );
        let configuration_descriptor = TableDescriptor::new(
            FLOW_CONFIGURATION_TABLE_NAME,
            FamilyName::new("flow-nexus-configuration"),
            SchemaHash::for_label("flow-nexus-configuration-v1"),
        );
        let runtime_configuration_descriptor = TableDescriptor::new(
            FLOW_RUNTIME_CONFIGURATION_TABLE_NAME,
            FamilyName::new("flow-nexus-runtime-configuration"),
            SchemaHash::for_label("flow-nexus-runtime-configuration-v1"),
        );
        let delivery_configuration_descriptor = TableDescriptor::new(
            delivery::DELIVERY_CONFIGURATION_TABLE_NAME,
            FamilyName::new("flow-nexus-delivery-configuration"),
            SchemaHash::for_label("flow-nexus-delivery-configuration-v1"),
        );
        let meta_nexus_descriptor = TableDescriptor::new(
            META_NEXUS_CONFIGURATION_TABLE_NAME,
            FamilyName::new("flow-nexus-meta-nexus-configuration"),
            SchemaHash::for_label("flow-nexus-meta-nexus-configuration-v1"),
        );
        let meta_models_descriptor = TableDescriptor::new(
            META_MODEL_CONFIGURATION_TABLE_NAME,
            FamilyName::new("flow-nexus-meta-model-configuration"),
            SchemaHash::for_label("flow-nexus-meta-model-configuration-v1"),
        );
        let meta_thresholds_descriptor = TableDescriptor::new(
            META_THRESHOLD_CONFIGURATION_TABLE_NAME,
            FamilyName::new("flow-nexus-meta-threshold-configuration"),
            SchemaHash::for_label("flow-nexus-meta-threshold-configuration-v1"),
        );
        let meta_leave_outs_descriptor = TableDescriptor::new(
            META_LEAVE_OUT_CONFIGURATION_TABLE_NAME,
            FamilyName::new("flow-nexus-meta-leave-out-configuration"),
            SchemaHash::for_label("flow-nexus-meta-leave-out-configuration-v1"),
        );
        if recognized {
            FlowStore::drop_retired_tables(path)?;
        }
        // Family identities are part of the open request, so Engine validates
        // them before its first durable write. Existing paths have already
        // passed the read-only Memory identity check above.
        let mut engine = Engine::open(
            EngineOpen::new(path, SchemaVersion::new(FLOW_SCHEMA_VERSION))
                .with_family(&state_descriptor)
                .with_family(&configuration_descriptor)
                .with_family(&runtime_configuration_descriptor)
                .with_family(&delivery_configuration_descriptor)
                .with_family(&meta_nexus_descriptor)
                .with_family(&meta_models_descriptor)
                .with_family(&meta_thresholds_descriptor)
                .with_family(&meta_leave_outs_descriptor),
        )?;
        let state = engine.register_table(state_descriptor)?;
        let configuration = engine.register_table(configuration_descriptor)?;
        let runtime_configuration = engine.register_table(runtime_configuration_descriptor)?;
        let delivery_configuration = engine.register_table(delivery_configuration_descriptor)?;
        let meta_configuration_tables = MetaConfigurationTables {
            nexus: engine.register_table(meta_nexus_descriptor)?,
            models: engine.register_table(meta_models_descriptor)?,
            thresholds: engine.register_table(meta_thresholds_descriptor)?,
            leave_outs: engine.register_table(meta_leave_outs_descriptor)?,
        };
        if !recognized {
            engine.assert(Assertion::new(
                state,
                FlowStoreState {
                    next_flow_number: 1,
                },
            ))?;
        }
        Ok(Self {
            engine,
            configuration,
            runtime_configuration,
            delivery_configuration,
            meta_configuration_tables,
        })
    }

    fn configuration_snapshot(&self) -> Result<Option<ConfigurationSnapshot>, StoreError> {
        FlowStore::meta_configuration_snapshot(&self.engine, &self.meta_configuration_tables)
    }

    fn configure_change(
        &self,
        change: ConfigurationChange,
    ) -> Result<ConfigureOutcome, StoreError> {
        FlowStore::configure_meta_change(&self.engine, &self.meta_configuration_tables, change)
    }

    /// Reads either a wholly unconfigured store (using its declared defaults
    /// without writing them) or all three committed configuration records.
    /// A partial or duplicated configuration refuses service.
    fn configuration(&self, defaults: &DefaultConfiguration) -> Result<Configuration, StoreError> {
        let socket = self
            .engine
            .match_records(QueryPlan::all(self.configuration))?
            .records()
            .to_vec();
        let runtime = self
            .engine
            .match_records(QueryPlan::all(self.runtime_configuration))?
            .records()
            .to_vec();
        let delivery = self
            .engine
            .match_records(QueryPlan::all(self.delivery_configuration))?
            .records()
            .to_vec();
        match (socket.as_slice(), runtime.as_slice(), delivery.as_slice()) {
            ([], [], []) => Ok(defaults.configuration()),
            ([socket], [runtime], [delivery])
                if socket.record_key() == defaults.socket_configuration().record_key()
                    && runtime.record_key() == defaults.runtime_configuration().record_key()
                    && delivery.record_key() == defaults.delivery_configuration().record_key() =>
            {
                Ok(socket.clone().with_runtime(runtime, delivery))
            }
            _ => Err(StoreError::StateInvariant),
        }
    }

    /// Commits socket, runtime, and delivery configuration together. Fresh
    /// stores assert the three records; configured stores replace them. A
    /// partial/duplicated prior state is refused before any write.
    fn configure(&self, configuration: Configuration) -> Result<Configuration, StoreError> {
        let socket_rows = self
            .engine
            .match_records(QueryPlan::all(self.configuration))?
            .records()
            .to_vec();
        let runtime_rows = self
            .engine
            .match_records(QueryPlan::all(self.runtime_configuration))?
            .records()
            .to_vec();
        let delivery_rows = self
            .engine
            .match_records(QueryPlan::all(self.delivery_configuration))?
            .records()
            .to_vec();
        let socket = FlowStoreConfiguration::from(&configuration);
        let runtime = RuntimeConfiguration::from(&configuration);
        let delivery = DeliveryConfiguration::from(&configuration);
        let commit =
            if socket_rows.is_empty() && runtime_rows.is_empty() && delivery_rows.is_empty() {
                self.engine
                    .begin_atomic_commit()
                    .assert(self.configuration, socket)
                    .assert(self.runtime_configuration, runtime)
                    .assert(self.delivery_configuration, delivery)
            } else if socket_rows.len() == 1
                && runtime_rows.len() == 1
                && delivery_rows.len() == 1
                && socket_rows[0].record_key() == socket.record_key()
                && runtime_rows[0].record_key() == runtime.record_key()
                && delivery_rows[0].record_key() == delivery.record_key()
            {
                self.engine
                    .begin_atomic_commit()
                    .mutate(self.configuration, socket)
                    .mutate(self.runtime_configuration, runtime)
                    .mutate(self.delivery_configuration, delivery)
            } else {
                return Err(StoreError::StateInvariant);
            };
        self.engine.commit_atomic(commit)?;
        Ok(configuration)
    }
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
    pub origin: Origin,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FlowRegistration {
    Registered(Box<FlowNode>),
    ConflictingBinding,
}

/// The constructor is a trait operation so the Nexus's storage boundary is
/// part of its ontology rather than an unclassified helper.
pub trait OpensFlowStore {
    /// Opens the store; a new store persists the given defaults and a
    /// populated store resumes what it holds.
    fn open_seeded(path: &Path, defaults: &DefaultConfiguration) -> Result<Self, StoreError>
    where
        Self: Sized;

    /// Opens an already-configured, recognized Flow Memory store without
    /// seeding defaults, importing an older store, or settling prior work.
    /// It drops the retired tables and upgrades the flows and launch
    /// outcomes from their 0.34 predecessor.
    fn open_configured(path: &Path) -> Result<Self, StoreError>
    where
        Self: Sized;

    fn open(path: &Path) -> Result<Self, StoreError>
    where
        Self: Sized,
    {
        Self::open_seeded(path, &DefaultConfiguration::from_environment())
    }
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
    fn runtime_configuration(&self) -> Result<RuntimeConfiguration, StoreError>;
    fn configure_runtime(&self, configuration: RuntimeConfiguration) -> Result<(), StoreError>;
}

pub trait RegistersFlowIdentity {
    /// Registers a flow whose role is not known (the meta RegisterFlow).
    fn register_flow(&self, flow_node: FlowNode) -> Result<FlowRegistration, StoreError>;
    /// Registers a launched flow with the role its launch profile named.
    fn register_flow_in_role(
        &self,
        flow_node: FlowNode,
        role: Caller,
    ) -> Result<FlowRegistration, StoreError>;
}

/// Imports one already-running native flow without claiming a launch receipt.
pub trait RegistersExistingFlow {
    fn register_existing_flow(
        &self,
        flow_node: FlowNode,
        role: Caller,
    ) -> Result<FlowRegistration, StoreError>;
}

/// The role each flow was bound or launched in, and the flows a Herdr pane
/// holds: what ResolveCaller reads.
pub trait ReadsFlowRoles {
    fn role(&self, flow_id: &str) -> Result<Option<Caller>, StoreError>;
    /// The routable flows whose recorded route names this session and pane.
    fn flows_in_pane(
        &self,
        herdr_session_name: &str,
        herdr_pane_id: &str,
    ) -> Result<Vec<FlowNode>, StoreError>;
}

/// Reserves one correlation ID and its exact composed-prompt fingerprint.
pub trait ReservesLaunchAttempt {
    fn reserve_launch_attempt(
        &self,
        launch: &ComposedLaunch,
        origin: Origin,
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

/// Settles a launch request once; reads its settlement back.
pub trait RecordsLaunchOutcome {
    fn record_launch_outcome(
        &self,
        launch_request_id: &str,
        outcome: LaunchOutcome,
    ) -> Result<(), StoreError>;
    fn launch_outcome(&self, launch_request_id: &str) -> Result<Option<LaunchOutcome>, StoreError>;
}

/// Marks a launch request as the replacement of a predecessor, and answers
/// whether a flow is a successor still held out of routing.
pub trait RecordsReplacement {
    fn record_replacement(&self, replacement: Replacement) -> Result<(), StoreError>;
    fn withdraw_replacement(&self, launch_request_id: &str) -> Result<(), StoreError>;
    fn replacement(&self, launch_request_id: &str) -> Result<Option<Replacement>, StoreError>;
    fn held_successor(&self, flow_id: &str) -> Result<bool, StoreError>;
}

pub trait ReadsLaunchAttempt {
    fn launch_attempt(&self, launch_request_id: &str) -> Result<Option<LaunchAttempt>, StoreError>;
    /// The profile of a launch that named its seat's context by module
    /// identity, which a later seat can be composed from again; None for a
    /// launch kept from before 0.35.0, whose context no module names.
    fn composable_launch_profile(
        &self,
        launch_request_id: &str,
    ) -> Result<Option<LaunchProfile>, StoreError>;
    /// The launch requests whose native launch bound this Flow ID.
    fn launch_requests_bound_to(&self, flow_id: &str) -> Result<Vec<String>, StoreError>;
    /// Every attempt whose first prompt is ambiguous and that is unsettled.
    fn ambiguous_launch_attempts(&self) -> Result<Vec<LaunchAttempt>, StoreError>;
    /// Whether an attempt is still in flight: it has no outcome, and the
    /// flow its native launch bound, if any, has not ended. A launch whose
    /// flow was Stopped, Retired or Exited is settled by that end, whatever
    /// its first prompt showed; one not yet bound, or bound to a flow not
    /// yet registered, is in flight.
    fn launch_attempt_unsettled(&self, attempt: &LaunchAttempt) -> Result<bool, StoreError>;
}

/// Reads durable native Flow seat rows for internal lifecycle handling.
pub trait ReadsFlowRows {
    fn flow_node(&self, flow_id: &str) -> Result<Option<FlowNode>, StoreError>;
    fn flow_nodes(&self) -> Result<Vec<FlowNode>, StoreError>;
}

/// Changes lifecycle only after the corresponding Herdr operation succeeds.
pub trait RecordsFlowLifecycle {
    fn record_active(&self, flow_id: &str) -> Result<bool, StoreError>;
    fn record_stopped(&self, flow_id: &str) -> Result<bool, StoreError>;
    /// An owner retires the flow: it leaves Flow's receiving deliberately,
    /// whatever its pane is doing. Its row and its history stay. A flow that
    /// is already ended keeps the state it has — how it went is never
    /// rewritten, and no observation ever produces this one.
    fn record_retired(&self, flow_id: &str) -> Result<bool, StoreError>;
    /// The flow's own pane went away and Flow never closed it. Its row and
    /// its history stay, and it is live no longer. This is what a witnessed
    /// exit records — never Retired, which only authority writes.
    fn record_exited(&self, flow_id: &str) -> Result<bool, StoreError>;
    /// A Stopped flow whose native session was resumed in a new pane is
    /// Pending again on that pane's route. Only a Stopped flow resumes: an
    /// exited or retired one stays as it went.
    fn record_resumed(&self, flow_id: &str, route: HerdrRoute) -> Result<bool, StoreError>;
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
    fn reserve_start(&self, flow_type: String, origin: Origin)
    -> Result<PendingLaunch, StoreError>;
    fn record_thread(&self, pending: &PendingLaunch, thread_id: String)
    -> Result<bool, StoreError>;
    fn confirm_start(&self, flow_id: &str) -> Result<Response, StoreError>;
    fn restart(&self, authorization: RestartAuthorization) -> Result<Response, StoreError>;
    /// Writes the attempt's next state, keeping what its row knows of the
    /// context its seat started with.
    fn mutate_launch_attempt(&self, stored: StoredLaunchAttempt) -> Result<(), StoreError>;
}

impl OpensFlowStore for FlowStore {
    fn open_configured(path: &Path) -> Result<Self, StoreError> {
        if !FlowStore::recognize_flow_memory(path)? {
            return Err(StoreError::NotConfigured);
        }

        let configuration_only = ConfigurationOnlyStore::open(path)?;
        if configuration_only.configuration_snapshot()?.is_none() {
            return Err(StoreError::NotConfigured);
        }
        drop(configuration_only);

        let flows_descriptor = TableDescriptor::<FlowRecord>::new(
            FLOW_TABLE_NAME,
            FamilyName::new("flow-nexus-flow"),
            FlowRecord::schema_hash(),
        )
        .with_predecessor::<FlowRecordV5>();
        let state_descriptor = TableDescriptor::<FlowStoreState>::new(
            FLOW_STATE_TABLE_NAME,
            FamilyName::new("flow-nexus-state"),
            SchemaHash::for_label("flow-nexus-state-v2"),
        );
        let metaflows_descriptor = TableDescriptor::<StoredMetaflow>::new(
            METAFLOW_TABLE_NAME,
            FamilyName::new("flow-nexus-metaflow"),
            StoredMetaflow::schema_hash(),
        )
        .with_predecessor::<StoredMetaflowV1>();
        let metaflow_processes_descriptor = TableDescriptor::<StoredMetaflowProcess>::new(
            METAFLOW_PROCESS_TABLE_NAME,
            FamilyName::new("flow-nexus-metaflow-process"),
            SchemaHash::for_label("flow-nexus-metaflow-process-v1"),
        );
        let metaflow_locks_descriptor = TableDescriptor::<StoredMetaflowLock>::new(
            METAFLOW_LOCK_TABLE_NAME,
            FamilyName::new("flow-nexus-metaflow-lock"),
            SchemaHash::for_label("flow-nexus-metaflow-lock-v1"),
        );
        let configuration_descriptor = TableDescriptor::<FlowStoreConfiguration>::new(
            FLOW_CONFIGURATION_TABLE_NAME,
            FamilyName::new("flow-nexus-configuration"),
            SchemaHash::for_label("flow-nexus-configuration-v1"),
        );
        let runtime_configuration_descriptor = TableDescriptor::<RuntimeConfiguration>::new(
            FLOW_RUNTIME_CONFIGURATION_TABLE_NAME,
            FamilyName::new("flow-nexus-runtime-configuration"),
            SchemaHash::for_label("flow-nexus-runtime-configuration-v1"),
        );
        let herdr_routes_descriptor = TableDescriptor::<FlowHerdrRouteRecord>::new(
            FLOW_HERDR_ROUTE_TABLE_NAME,
            FamilyName::new("flow-nexus-herdr-route"),
            SchemaHash::for_label("flow-nexus-herdr-route-v1"),
        );
        let launch_attempts_descriptor = TableDescriptor::<StoredLaunchAttempt>::new(
            FLOW_LAUNCH_ATTEMPT_TABLE_NAME,
            FamilyName::new("flow-nexus-launch-attempt"),
            StoredLaunchAttempt::schema_hash(),
        )
        .with_predecessor::<StoredLaunchAttemptV1>();
        let launch_panes_descriptor = TableDescriptor::<StoredLaunchPane>::new(
            FLOW_LAUNCH_PANE_TABLE_NAME,
            FamilyName::new("flow-nexus-launch-pane"),
            SchemaHash::for_label("flow-nexus-launch-pane-v1"),
        );
        let launch_outcomes_descriptor = TableDescriptor::<StoredLaunchOutcome>::new(
            FLOW_LAUNCH_OUTCOME_TABLE_NAME,
            FamilyName::new("flow-nexus-launch-outcome"),
            StoredLaunchOutcome::schema_hash(),
        )
        .with_predecessor::<StoredLaunchOutcomeV2>()
        .with_predecessor::<StoredLaunchOutcomeV3>()
        .with_predecessor::<StoredLaunchOutcomeV4>();
        let replacements_descriptor = TableDescriptor::<Replacement>::new(
            FLOW_REPLACEMENT_TABLE_NAME,
            FamilyName::new("flow-nexus-replacement"),
            SchemaHash::for_label("flow-nexus-replacement-v1"),
        );
        let roles_descriptor = TableDescriptor::<StoredRole>::new(
            FLOW_ROLE_TABLE_NAME,
            FamilyName::new("flow-nexus-role"),
            SchemaHash::for_label("flow-nexus-role-v1"),
        );
        let quarantined_launch_attempts_descriptor =
            TableDescriptor::<QuarantinedLaunchAttempt>::new(
                FLOW_QUARANTINED_LAUNCH_ATTEMPT_TABLE_NAME,
                FamilyName::new("flow-nexus-quarantined-launch-attempt"),
                SchemaHash::for_label("flow-nexus-quarantined-launch-attempt-v1"),
            );
        let delivery_descriptor = TableDescriptor::<delivery::StoredDelivery>::new(
            delivery::DELIVERY_TABLE_NAME,
            FamilyName::new("flow-nexus-delivery"),
            SchemaHash::for_label("flow-nexus-delivery-v1"),
        );
        let pane_lease_descriptor = TableDescriptor::<delivery::PaneLease>::new(
            delivery::PANE_LEASE_TABLE_NAME,
            FamilyName::new("flow-nexus-pane-lease"),
            SchemaHash::for_label("flow-nexus-pane-lease-v1"),
        );
        let delivery_configuration_descriptor = TableDescriptor::<DeliveryConfiguration>::new(
            delivery::DELIVERY_CONFIGURATION_TABLE_NAME,
            FamilyName::new("flow-nexus-delivery-configuration"),
            SchemaHash::for_label("flow-nexus-delivery-configuration-v1"),
        );
        let meta_nexus_descriptor = TableDescriptor::<StoredMetaNexusConfiguration>::new(
            META_NEXUS_CONFIGURATION_TABLE_NAME,
            FamilyName::new("flow-nexus-meta-nexus-configuration"),
            SchemaHash::for_label("flow-nexus-meta-nexus-configuration-v1"),
        );
        let meta_models_descriptor = TableDescriptor::<StoredMetaModelConfiguration>::new(
            META_MODEL_CONFIGURATION_TABLE_NAME,
            FamilyName::new("flow-nexus-meta-model-configuration"),
            SchemaHash::for_label("flow-nexus-meta-model-configuration-v1"),
        );
        let meta_thresholds_descriptor = TableDescriptor::<StoredMetaThresholdConfiguration>::new(
            META_THRESHOLD_CONFIGURATION_TABLE_NAME,
            FamilyName::new("flow-nexus-meta-threshold-configuration"),
            SchemaHash::for_label("flow-nexus-meta-threshold-configuration-v1"),
        );
        let meta_leave_outs_descriptor = TableDescriptor::<StoredMetaLeaveOutConfiguration>::new(
            META_LEAVE_OUT_CONFIGURATION_TABLE_NAME,
            FamilyName::new("flow-nexus-meta-leave-out-configuration"),
            SchemaHash::for_label("flow-nexus-meta-leave-out-configuration-v1"),
        );
        let events_descriptor = TableDescriptor::<events::FlowEvents>::new(
            events::FLOW_EVENT_TABLE_NAME,
            FamilyName::new("flow-nexus-flow-events"),
            SchemaHash::for_label("flow-nexus-flow-events-v1"),
        );
        let context_sizes_descriptor =
            <context::FlowContextSize as context::DescribesContextSizeTable>::descriptor();
        let address_holds_descriptor =
            <address_hold::AddressHold as address_hold::DescribesAddressHoldTable>::descriptor();
        let refreshes_descriptor =
            <refresh::MetaflowRefresh as refresh::DescribesRefreshTable>::descriptor();
        let opened_threads_descriptor =
            <opened_thread::OpenedLaunchThread as opened_thread::DescribesOpenedThreadTable>::descriptor(
            );
        let send_calls_descriptor = <crate::generated::send_tool::SendCall as send_call::DescribesSendCallTable>::descriptor();
        let launch_cleanups_descriptor =
            <launch_cleanup::LaunchCleanup as launch_cleanup::DescribesLaunchCleanupTable>::descriptor();
        let mut engine = Engine::open(
            EngineOpen::new(path, SchemaVersion::new(FLOW_SCHEMA_VERSION))
                .with_family(&flows_descriptor)
                .with_family(&state_descriptor)
                .with_family(&metaflows_descriptor)
                .with_family(&metaflow_processes_descriptor)
                .with_family(&metaflow_locks_descriptor)
                .with_family(&configuration_descriptor)
                .with_family(&runtime_configuration_descriptor)
                .with_family(&herdr_routes_descriptor)
                .with_family(&launch_attempts_descriptor)
                .with_family(&launch_panes_descriptor)
                .with_family(&launch_outcomes_descriptor)
                .with_family(&replacements_descriptor)
                .with_family(&roles_descriptor)
                .with_family(&quarantined_launch_attempts_descriptor)
                .with_family(&delivery_descriptor)
                .with_family(&pane_lease_descriptor)
                .with_family(&delivery_configuration_descriptor)
                .with_family(&meta_nexus_descriptor)
                .with_family(&meta_models_descriptor)
                .with_family(&meta_thresholds_descriptor)
                .with_family(&meta_leave_outs_descriptor)
                .with_family(&events_descriptor)
                .with_family(&context_sizes_descriptor)
                .with_family(&address_holds_descriptor)
                .with_family(&refreshes_descriptor)
                .with_family(&opened_threads_descriptor)
                .with_family(&send_calls_descriptor)
                .with_family(&launch_cleanups_descriptor),
        )?;
        let flows = engine.upgraded_flows(flows_descriptor)?;
        let state = engine.register_table(state_descriptor)?;
        let metaflows = engine.upgraded_metaflows(metaflows_descriptor)?;
        let metaflow_processes = engine.register_table(metaflow_processes_descriptor)?;
        let metaflow_locks = engine.register_table(metaflow_locks_descriptor)?;
        let configuration = engine.register_table(configuration_descriptor)?;
        let runtime_configuration = engine.register_table(runtime_configuration_descriptor)?;
        let herdr_routes = engine.register_table(herdr_routes_descriptor)?;
        let quarantined_launch_attempts =
            engine.register_table(quarantined_launch_attempts_descriptor)?;
        let (launch_attempts, opening_settlements) = engine
            .upgraded_launch_attempts(launch_attempts_descriptor, quarantined_launch_attempts)?;
        let launch_panes = engine.register_table(launch_panes_descriptor)?;
        let launch_outcomes = engine.upgraded_launch_outcomes(launch_outcomes_descriptor)?;
        let replacements = engine.register_table(replacements_descriptor)?;
        let roles = engine.register_table(roles_descriptor)?;
        let delivery_tables = DeliveryTables::register(&mut engine)?;
        let meta_configuration_tables = MetaConfigurationTables::register(&mut engine)?;
        let event_tables =
            <events::EventTables as events::RegistersEventTables>::register(&mut engine)?;
        let context_sizes = engine.register_table(context_sizes_descriptor)?;
        let address_holds = engine.upgraded_address_holds(address_holds_descriptor)?;
        let refreshes = engine.register_table(refreshes_descriptor)?;
        let opened_threads = engine.register_table(opened_threads_descriptor)?;
        let send_calls = engine.register_table(send_calls_descriptor)?;
        let launch_cleanups = engine.register_table(launch_cleanups_descriptor)?;
        let store = Self {
            engine,
            flows,
            state,
            metaflows,
            metaflow_processes,
            metaflow_locks,
            configuration,
            runtime_configuration,
            herdr_routes,
            launch_attempts,
            launch_panes,
            launch_outcomes,
            replacements,
            roles,
            meta_configuration_tables,
            quarantined_launch_attempts,
            delivery_tables,
            event_tables,
            context_sizes,
            address_holds,
            refreshes,
            opened_threads,
            send_calls,
            launch_cleanups,
            launch_changes: LaunchChanges::default(),
            opening_settlements,
        };
        for settlement in &store.opening_settlements {
            eprintln!("flow-nexus: {settlement}");
        }

        // Recognition already checked the authored Memory identity record;
        // these reads require the complete committed configuration before any
        // runtime adapter is constructed by RunningNexus.
        store.state()?;
        store
            .configuration_snapshot()?
            .ok_or(StoreError::NotConfigured)?;
        Ok(store)
    }

    fn open_seeded(path: &Path, defaults: &DefaultConfiguration) -> Result<Self, StoreError> {
        if path.exists() {
            FlowStore::drop_retired_tables(path)?;
        }
        let mut engine = Engine::open(EngineOpen::new(path, SchemaVersion::new(1)))?;
        let flows = engine.upgraded_flows(
            TableDescriptor::new(
                FLOW_TABLE_NAME,
                FamilyName::new("flow-nexus-flow"),
                FlowRecord::schema_hash(),
            )
            .with_predecessor::<FlowRecordV5>(),
        )?;
        let state = engine.register_table(TableDescriptor::new(
            FLOW_STATE_TABLE_NAME,
            FamilyName::new("flow-nexus-state"),
            SchemaHash::for_label("flow-nexus-state-v1"),
        ))?;
        let metaflows = engine.upgraded_metaflows(
            TableDescriptor::new(
                METAFLOW_TABLE_NAME,
                FamilyName::new("flow-nexus-metaflow"),
                StoredMetaflow::schema_hash(),
            )
            .with_predecessor::<StoredMetaflowV1>(),
        )?;
        let metaflow_processes = engine.register_table(TableDescriptor::new(
            METAFLOW_PROCESS_TABLE_NAME,
            FamilyName::new("flow-nexus-metaflow-process"),
            SchemaHash::for_label("flow-nexus-metaflow-process-v1"),
        ))?;
        let metaflow_locks = engine.register_table(TableDescriptor::new(
            METAFLOW_LOCK_TABLE_NAME,
            FamilyName::new("flow-nexus-metaflow-lock"),
            SchemaHash::for_label("flow-nexus-metaflow-lock-v1"),
        ))?;
        let configuration = engine.register_table(TableDescriptor::new(
            FLOW_CONFIGURATION_TABLE_NAME,
            FamilyName::new("flow-nexus-configuration"),
            SchemaHash::for_label("flow-nexus-configuration-v1"),
        ))?;
        let runtime_configuration = engine.register_table(TableDescriptor::new(
            FLOW_RUNTIME_CONFIGURATION_TABLE_NAME,
            FamilyName::new("flow-nexus-runtime-configuration"),
            SchemaHash::for_label("flow-nexus-runtime-configuration-v1"),
        ))?;
        let herdr_routes = engine.register_table(TableDescriptor::new(
            FLOW_HERDR_ROUTE_TABLE_NAME,
            FamilyName::new("flow-nexus-herdr-route"),
            SchemaHash::for_label("flow-nexus-herdr-route-v1"),
        ))?;
        let launch_panes = engine.register_table(TableDescriptor::<StoredLaunchPane>::new(
            FLOW_LAUNCH_PANE_TABLE_NAME,
            FamilyName::new("flow-nexus-launch-pane"),
            SchemaHash::for_label("flow-nexus-launch-pane-v1"),
        ))?;
        let launch_outcomes = engine.upgraded_launch_outcomes(
            TableDescriptor::new(
                FLOW_LAUNCH_OUTCOME_TABLE_NAME,
                FamilyName::new("flow-nexus-launch-outcome"),
                StoredLaunchOutcome::schema_hash(),
            )
            .with_predecessor::<StoredLaunchOutcomeV2>()
            .with_predecessor::<StoredLaunchOutcomeV3>()
            .with_predecessor::<StoredLaunchOutcomeV4>(),
        )?;
        let replacements = engine.register_table(TableDescriptor::new(
            FLOW_REPLACEMENT_TABLE_NAME,
            FamilyName::new("flow-nexus-replacement"),
            SchemaHash::for_label("flow-nexus-replacement-v1"),
        ))?;
        let roles = engine.register_table(TableDescriptor::new(
            FLOW_ROLE_TABLE_NAME,
            FamilyName::new("flow-nexus-role"),
            SchemaHash::for_label("flow-nexus-role-v1"),
        ))?;
        let quarantined_launch_attempts = engine.register_table(TableDescriptor::new(
            FLOW_QUARANTINED_LAUNCH_ATTEMPT_TABLE_NAME,
            FamilyName::new("flow-nexus-quarantined-launch-attempt"),
            SchemaHash::for_label("flow-nexus-quarantined-launch-attempt-v1"),
        ))?;
        let (launch_attempts, opening_settlements) = engine.upgraded_launch_attempts(
            TableDescriptor::new(
                FLOW_LAUNCH_ATTEMPT_TABLE_NAME,
                FamilyName::new("flow-nexus-launch-attempt"),
                StoredLaunchAttempt::schema_hash(),
            )
            .with_predecessor::<StoredLaunchAttemptV1>(),
            quarantined_launch_attempts,
        )?;
        let delivery_tables = DeliveryTables::register(&mut engine)?;
        let meta_configuration_tables = MetaConfigurationTables::register(&mut engine)?;
        let event_tables =
            <events::EventTables as events::RegistersEventTables>::register(&mut engine)?;
        let context_sizes = engine.register_table(
            <context::FlowContextSize as context::DescribesContextSizeTable>::descriptor(),
        )?;
        let address_holds = engine.upgraded_address_holds(
            <address_hold::AddressHold as address_hold::DescribesAddressHoldTable>::descriptor(),
        )?;
        let refreshes = engine.register_table(
            <refresh::MetaflowRefresh as refresh::DescribesRefreshTable>::descriptor(),
        )?;
        let opened_threads = engine.register_table(
            <opened_thread::OpenedLaunchThread as opened_thread::DescribesOpenedThreadTable>::descriptor(
            ),
        )?;
        let send_calls = engine.register_table(
            <crate::generated::send_tool::SendCall as send_call::DescribesSendCallTable>::descriptor(),
        )?;
        let launch_cleanups = engine.register_table(
            <launch_cleanup::LaunchCleanup as launch_cleanup::DescribesLaunchCleanupTable>::descriptor(),
        )?;
        let store = Self {
            engine,
            flows,
            state,
            metaflows,
            metaflow_processes,
            metaflow_locks,
            configuration,
            runtime_configuration,
            herdr_routes,
            launch_attempts,
            launch_panes,
            launch_outcomes,
            replacements,
            roles,
            meta_configuration_tables,
            quarantined_launch_attempts,
            delivery_tables,
            event_tables,
            context_sizes,
            address_holds,
            refreshes,
            opened_threads,
            send_calls,
            launch_cleanups,
            launch_changes: LaunchChanges::default(),
            opening_settlements,
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
                defaults.socket_configuration(),
            ))?;
        }
        if store
            .engine
            .match_records(QueryPlan::key(
                store.runtime_configuration,
                RecordKey::new(RUNTIME_CONFIGURATION_KEY),
            ))?
            .records()
            .is_empty()
        {
            store.engine.assert(Assertion::new(
                store.runtime_configuration,
                defaults.runtime_configuration(),
            ))?;
        }
        for settlement in &store.opening_settlements {
            eprintln!("flow-nexus: {settlement}");
        }
        store.adopt_legacy_roles()?;
        store.seed_delivery_configuration(defaults)?;
        // A delivery a crash left under the lease is settled Uncertain here
        // and never retried: what reached its pane is not known.
        for delivery in store.settle_interrupted_deliveries()? {
            eprintln!(
                "flow-nexus: delivery {} to {} was interrupted under its lease; settled Uncertain",
                delivery.delivery_id, delivery.flow_id
            );
        }
        Ok(store)
    }
}

pub(crate) trait ManagesMetaflowState: Sized {
    fn configuration_snapshot(&self) -> Result<Option<ConfigurationSnapshot>, StoreError>;
    /// Replace one configuration family whole with its declaration.
    fn configure_change(&self, change: ConfigurationChange)
    -> Result<ConfigureOutcome, StoreError>;
    /// The threshold configured for one layer, if any.
    fn configured_threshold(&self, layer: &Layer)
    -> Result<Option<ConfigureThreshold>, StoreError>;
    /// The leave-outs a metaflow's Start takes when it names none; empty
    /// when none are configured.
    fn default_leave_outs(&self, address: &Address) -> Result<Vec<LeaveOut>, StoreError>;
    fn stored_metaflow(&self, address: &Address) -> Result<Option<StoredMetaflow>, StoreError>;
    fn stored_metaflow_process(
        &self,
        address: &Address,
    ) -> Result<Option<StoredMetaflowProcess>, StoreError>;
    fn stored_metaflow_lock(
        &self,
        address: &Address,
    ) -> Result<Option<StoredMetaflowLock>, StoreError>;
    fn bound_process(&self, address: &Address) -> Result<Option<Process>, StoreError>;
    fn current_metaflow(&self, address: &Address) -> Result<Response, StoreError>;
    fn list_metaflows(&self) -> Result<Vec<Metaflow>, StoreError>;
    fn identify_process(&self, process: Process) -> Result<Response, StoreError>;
    fn end_metaflow(
        &self,
        address: Address,
        now_epoch_seconds: i64,
    ) -> Result<Response, StoreError>;
    fn release_metaflow_lock(
        &self,
        lock: Lock,
        now_epoch_seconds: i64,
    ) -> Result<Response, StoreError>;
}

impl ManagesMetaflowState for FlowStore {
    fn configuration_snapshot(&self) -> Result<Option<ConfigurationSnapshot>, StoreError> {
        FlowStore::meta_configuration_snapshot(&self.engine, &self.meta_configuration_tables)
    }

    fn configure_change(
        &self,
        change: ConfigurationChange,
    ) -> Result<ConfigureOutcome, StoreError> {
        FlowStore::configure_meta_change(&self.engine, &self.meta_configuration_tables, change)
    }

    fn configured_threshold(
        &self,
        layer: &Layer,
    ) -> Result<Option<ConfigureThreshold>, StoreError> {
        let rows = self.engine.match_records(QueryPlan::key(
            self.meta_configuration_tables.thresholds,
            RecordKey::new(FlowStore::layer_key(layer)),
        ))?;
        match rows.records() {
            [] => Ok(None),
            [stored] => Ok(Some(stored.threshold.clone())),
            _ => Err(StoreError::StateInvariant),
        }
    }

    fn default_leave_outs(&self, address: &Address) -> Result<Vec<LeaveOut>, StoreError> {
        let rows = self.engine.match_records(QueryPlan::key(
            self.meta_configuration_tables.leave_outs,
            RecordKey::new(FlowStore::address_record_key(address)),
        ))?;
        match rows.records() {
            [] => Ok(Vec::new()),
            [stored] => Ok(stored.leave_out.leave_outs.clone()),
            _ => Err(StoreError::StateInvariant),
        }
    }

    fn stored_metaflow(&self, address: &Address) -> Result<Option<StoredMetaflow>, StoreError> {
        let rows = self
            .engine
            .match_records(QueryPlan::key(
                self.metaflows,
                RecordKey::new(FlowStore::address_record_key(address)),
            ))?
            .records()
            .to_vec();
        match rows.as_slice() {
            [] => Ok(None),
            [metaflow] if metaflow.metaflow.address == *address => Ok(Some(metaflow.clone())),
            _ => Err(StoreError::StateInvariant),
        }
    }

    fn stored_metaflow_process(
        &self,
        address: &Address,
    ) -> Result<Option<StoredMetaflowProcess>, StoreError> {
        let rows = self
            .engine
            .match_records(QueryPlan::key(
                self.metaflow_processes,
                RecordKey::new(FlowStore::address_record_key(address)),
            ))?
            .records()
            .to_vec();
        match rows.as_slice() {
            [] => Ok(None),
            [binding] if binding.address == *address => Ok(Some(binding.clone())),
            _ => Err(StoreError::StateInvariant),
        }
    }

    fn stored_metaflow_lock(
        &self,
        address: &Address,
    ) -> Result<Option<StoredMetaflowLock>, StoreError> {
        let rows = self
            .engine
            .match_records(QueryPlan::key(
                self.metaflow_locks,
                RecordKey::new(FlowStore::address_record_key(address)),
            ))?
            .records()
            .to_vec();
        match rows.as_slice() {
            [] => Ok(None),
            [lock] if lock.lock.address == *address => Ok(Some(lock.clone())),
            _ => Err(StoreError::StateInvariant),
        }
    }

    fn bound_process(&self, address: &Address) -> Result<Option<Process>, StoreError> {
        Ok(self
            .stored_metaflow_process(address)?
            .map(|binding| binding.process))
    }

    fn current_metaflow(&self, address: &Address) -> Result<Response, StoreError> {
        let current = match self.stored_metaflow(address)? {
            None => signal_flow::Current_Data::Unknown,
            Some(stored) => match stored.metaflow.state {
                MetaflowState::Awake(flow_id) => signal_flow::Current_Data::Awake(flow_id),
                MetaflowState::Asleep => signal_flow::Current_Data::Asleep,
                MetaflowState::Ended => signal_flow::Current_Data::Ended,
            },
        };
        Ok(Response::Current(current))
    }

    fn list_metaflows(&self) -> Result<Vec<Metaflow>, StoreError> {
        let rows = self.engine.match_records(QueryPlan::all(self.metaflows))?;
        let marker = rows.database_marker();
        let mut metaflows = rows
            .records()
            .iter()
            .map(|stored| stored.metaflow.clone())
            .collect::<Vec<_>>();
        metaflows.sort_by_key(|metaflow| FlowStore::address_record_key(&metaflow.address));
        if self.engine.current_database_marker()? != marker {
            return Err(StoreError::MetaflowReadViewChanged);
        }
        Ok(metaflows)
    }

    fn identify_process(&self, process: Process) -> Result<Response, StoreError> {
        let lineage = FlowStore::process_lineage(&process);
        if lineage.is_empty() {
            return Ok(Response::Refused(signal_flow::Refusal::Unidentified(
                process,
            )));
        }
        for ancestor in lineage {
            let rows = self
                .engine
                .match_records(QueryPlan::all(self.metaflow_processes))?;
            let Some(binding) = rows.records().iter().find(|binding| {
                binding.process == ancestor && FlowStore::process_is_live(&binding.process)
            }) else {
                continue;
            };
            return Ok(Response::Identified(binding.address.clone()));
        }
        Ok(Response::Refused(signal_flow::Refusal::Unidentified(
            process,
        )))
    }

    fn end_metaflow(
        &self,
        address: Address,
        now_epoch_seconds: i64,
    ) -> Result<Response, StoreError> {
        let Some(mut stored) = self.stored_metaflow(&address)? else {
            return Ok(Response::Refused(signal_flow::Refusal::Unknown(
                signal_flow::Unknown_Data::Address(address),
            )));
        };
        if stored.metaflow.state == MetaflowState::Ended {
            return Ok(Response::Refused(signal_flow::Refusal::Ended(address)));
        }
        let lock = self.stored_metaflow_lock(&address)?;
        if let Some(lock) = lock.as_ref()
            && lock.lock.until > now_epoch_seconds
        {
            return Ok(Response::Refused(signal_flow::Refusal::Held(
                lock.lock.clone(),
            )));
        }
        stored.metaflow.state = MetaflowState::Ended;
        let mut commit = self
            .engine
            .begin_atomic_commit()
            .mutate(self.metaflows, stored);
        if lock.is_some() {
            commit = commit.retract(
                self.metaflow_locks,
                RecordKey::new(FlowStore::address_record_key(&address)),
            );
        }
        self.engine.commit_atomic(commit)?;
        Ok(Response::Ended)
    }

    fn release_metaflow_lock(
        &self,
        lock: Lock,
        now_epoch_seconds: i64,
    ) -> Result<Response, StoreError> {
        let Some(stored) = self.stored_metaflow_lock(&lock.address)? else {
            return Ok(Response::Refused(signal_flow::Refusal::Unknown(
                signal_flow::Unknown_Data::Lock(lock),
            )));
        };
        if stored.lock != lock {
            return Ok(Response::Refused(signal_flow::Refusal::Unknown(
                signal_flow::Unknown_Data::Lock(lock),
            )));
        }
        let expired = lock.until <= now_epoch_seconds;
        self.engine.retract(Retraction::new(
            self.metaflow_locks,
            RecordKey::new(FlowStore::address_record_key(&lock.address)),
        ))?;
        if expired {
            Ok(Response::Refused(signal_flow::Refusal::Lapsed))
        } else {
            Ok(Response::Released)
        }
    }
}

impl AppliesFlowQuery for FlowStore {
    fn apply(&self, query: Query) -> Result<Response, StoreError> {
        match query {
            // The store alone launches nothing.
            Query::Start(_) => Ok(Response::StartRejected(
                StartRejection::LaunchPersistenceRefused,
            )),
            Query::Restart(_) => Ok(Response::RestartRejected(RestartRejection::ResumeRefused)),
            Query::ResolveRecipient(flow_id) => self.resolve_recipient(&flow_id),
            Query::Stop(_) => Ok(Response::StopRejected(
                signal_flow::StopRejection::PersistenceRefused,
            )),
            Query::Bind(_)
            | Query::Identify(_)
            | Query::Lock(_)
            | Query::Deliver(_)
            | Query::Release(_)
            | Query::End(_) => Ok(Response::Refused(signal_flow::Refusal::NotMessage)),
            Query::Current(address) => self.current_metaflow(&address),
            Query::Metaflows => self.list_metaflows().map(Response::Listed),
            Query::Replace(_) => Ok(Response::ReplaceRejected(ReplaceRejection::LaunchRefused(
                StartRejection::LaunchPersistenceRefused,
            ))),
            // The caller is read from the socket, which the store never sees.
            Query::ResolveCaller(_) => Ok(Response::CallerResolutionRejected(
                CallerResolutionRejection::CallerUnknown,
            )),
            Query::LaunchStatus(_) | Query::Observe(_) => Ok(Response::LaunchStatusRejected(
                signal_flow::LaunchStatusRejection::PersistenceRefused,
            )),
            Query::QueueTurnEnd(_) => Ok(Response::TurnEndRejected(
                signal_flow::TurnEndRejection::QueueRefused,
            )),
            Query::Report(report) => {
                use events::{EventRecording, RecordsHarnessEvents};
                match self.record_event(&report.flow_id, report.event)? {
                    EventRecording::Recorded => Ok(Response::Reported),
                    EventRecording::UnknownFlow => {
                        Ok(Response::Refused(signal_flow::Refusal::Unknown(
                            signal_flow::Unknown_Data::FlowId(report.flow_id),
                        )))
                    }
                }
            }
        }
    }
}

impl ReservesPendingStart for FlowStore {
    fn reserve_pending_start(&self, query: Query) -> Result<Option<PendingLaunch>, StoreError> {
        match query {
            Query::Start(_) => self
                .reserve_start("legacy-test-start".into(), Origin::Unbound)
                .map(Some),
            Query::Restart(_)
            | Query::ResolveRecipient(_)
            | Query::Stop(_)
            | Query::Current(_)
            | Query::Metaflows
            | Query::Replace(_)
            | Query::LaunchStatus(_)
            | Query::Observe(_)
            | Query::ResolveCaller(_)
            | Query::QueueTurnEnd(_)
            | Query::Report(_)
            | Query::Bind(_)
            | Query::Identify(_)
            | Query::Lock(_)
            | Query::Deliver(_)
            | Query::Release(_)
            | Query::End(_) => Ok(None),
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
        Ok(self.stored_configuration()?.with_runtime(
            &self.runtime_configuration()?,
            &self.delivery_configuration()?,
        ))
    }

    fn configure(&self, configuration: Configuration) -> Result<(), StoreError> {
        self.engine.commit_atomic(
            self.engine
                .begin_atomic_commit()
                .mutate(
                    self.configuration,
                    FlowStoreConfiguration::from(&configuration),
                )
                .mutate(
                    self.runtime_configuration,
                    RuntimeConfiguration::from(&configuration),
                )
                .mutate(
                    self.delivery_tables.configuration,
                    DeliveryConfiguration::from(&configuration),
                ),
        )?;
        Ok(())
    }

    fn runtime_configuration(&self) -> Result<RuntimeConfiguration, StoreError> {
        if let Some(snapshot) = self.configuration_snapshot()? {
            return Ok(RuntimeConfiguration::from(&snapshot.nexus));
        }
        let records = self
            .engine
            .match_records(QueryPlan::key(
                self.runtime_configuration,
                RecordKey::new(RUNTIME_CONFIGURATION_KEY),
            ))?
            .records()
            .to_vec();
        match records.as_slice() {
            [configuration] => Ok(configuration.clone()),
            _ => Err(StoreError::StateInvariant),
        }
    }

    fn configure_runtime(&self, configuration: RuntimeConfiguration) -> Result<(), StoreError> {
        self.engine.mutate_keyed(KeyedMutation::new(
            self.runtime_configuration,
            RecordKey::new(RUNTIME_CONFIGURATION_KEY),
            configuration,
        ))?;
        Ok(())
    }
}

trait RegistersBindings {
    /// A binding that names a flow already in the store asserts only its
    /// role, and only when the stored flow is that binding: not Stopped, the
    /// same native thread and harness, and a route on the same Herdr session,
    /// pane and terminal (the agent name may have changed since). A role
    /// already recorded must be the same one. Nothing else is written: the
    /// flow keeps its lifecycle, endpoint, route and origin.
    fn assert_role_of_matching_binding(
        &self,
        flow: FlowRecord,
        binding: &FlowNode,
        role: Caller,
    ) -> Result<FlowRegistration, StoreError>;
    fn register_flow_as(
        &self,
        flow_node: FlowNode,
        flow_type: String,
        role: Option<Caller>,
    ) -> Result<FlowRegistration, StoreError>;
}

impl RegistersBindings for FlowStore {
    fn assert_role_of_matching_binding(
        &self,
        flow: FlowRecord,
        binding: &FlowNode,
        role: Caller,
    ) -> Result<FlowRegistration, StoreError> {
        let HerdrRouteSelection::Available(bound_route) = &binding.herdr_route_selection else {
            return Ok(FlowRegistration::ConflictingBinding);
        };
        let same_route = self.herdr_route(&flow.flow_id)?.is_some_and(|stored| {
            stored.route.herdr_session_name == bound_route.herdr_session_name
                && stored.route.herdr_pane_id == bound_route.herdr_pane_id
                && stored.route.herdr_terminal_id == bound_route.herdr_terminal_id
        });
        if !flow.lifecycle.is_live()
            || flow.thread_id.as_deref() != Some(binding.session_id.as_str())
            || flow.harness_kind != binding.harness_kind
            || !same_route
        {
            return Ok(FlowRegistration::ConflictingBinding);
        }
        match self.role(&flow.flow_id)? {
            Some(existing) if existing != role => {
                return Ok(FlowRegistration::ConflictingBinding);
            }
            Some(_) => {}
            None => {
                self.engine
                    .assert(Assertion::new(self.roles, StoredRole { caller: role }))?;
            }
        }
        match self.flow_node(&flow.flow_id)? {
            Some(stored) => Ok(FlowRegistration::Registered(Box::new(stored))),
            None => Err(StoreError::StateInvariant),
        }
    }

    fn register_flow_as(
        &self,
        flow_node: FlowNode,
        flow_type: String,
        role: Option<Caller>,
    ) -> Result<FlowRegistration, StoreError> {
        if role
            .as_ref()
            .is_some_and(|role| role.flow_id != flow_node.flow_id)
        {
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
                Some(_) => {}
                None => {
                    self.engine.assert(Assertion::new(
                        self.herdr_routes,
                        FlowHerdrRouteRecord {
                            flow_id: flow_node.flow_id.clone(),
                            route,
                        },
                    ))?;
                }
            }
            if let Some(role) = role {
                match self.role(&flow_node.flow_id)? {
                    Some(existing) if existing != role => {
                        return Ok(FlowRegistration::ConflictingBinding);
                    }
                    Some(_) => {}
                    None => {
                        self.engine
                            .assert(Assertion::new(self.roles, StoredRole { caller: role }))?;
                    }
                }
            }
            return Ok(FlowRegistration::Registered(Box::new(flow_node)));
        }
        let lifecycle = match flow_node.flow_lifecycle {
            SignalFlowLifecycle::Pending => FlowLifecycle::Pending,
            SignalFlowLifecycle::Active => FlowLifecycle::Active,
            SignalFlowLifecycle::Stopped => FlowLifecycle::Stopped,
            SignalFlowLifecycle::Retired => FlowLifecycle::Retired,
            SignalFlowLifecycle::Exited => FlowLifecycle::Exited,
        };
        let record = FlowRecord {
            flow_id: flow_node.flow_id.clone(),
            flow_type,
            origin: flow_node.origin.clone(),
            thread_id: Some(flow_node.session_id.clone()),
            harness_kind: flow_node.harness_kind.clone(),
            endpoint_selection: flow_node.endpoint_selection.clone(),
            lifecycle,
            generation: 1,
        };
        let commit = self
            .engine
            .begin_atomic_commit()
            .assert(self.flows, record)
            .assert(
                self.herdr_routes,
                FlowHerdrRouteRecord {
                    flow_id: flow_node.flow_id.clone(),
                    route,
                },
            );
        let commit = match role {
            Some(role) => commit.assert(self.roles, StoredRole { caller: role }),
            None => commit,
        };
        self.engine.commit_atomic(commit)?;
        Ok(FlowRegistration::Registered(Box::new(flow_node)))
    }
}

trait SettlesLaunchAttempts {
    /// Reads every row of a 0.34 attempts table by itself. A row that reads
    /// in 0.34's shape stays, for the family upgrade to carry. Any other row
    /// is moved aside whole into the quarantine table, in one commit with
    /// its retraction, naming the earlier shape it reads in, or Unknown.
    fn settle_launch_attempts(
        &mut self,
        quarantine: TableReference<QuarantinedLaunchAttempt>,
    ) -> Result<Vec<LaunchAttemptSettlement>, StoreError>;
    /// Every launch-attempt row as its stored key and archive bytes, read
    /// through the engine's read-only storage reader. Reading the archive
    /// undecoded is an exception taken only here: it is how a row that no
    /// shape reads is found and kept whole.
    fn launch_attempt_archives(&self) -> Result<Vec<(String, Vec<u8>)>, StoreError>;
    fn quarantined_launch_attempts_of(
        &self,
        quarantine: TableReference<QuarantinedLaunchAttempt>,
        launch_request_id: &str,
    ) -> Result<Vec<QuarantinedLaunchAttempt>, StoreError>;
}

impl SettlesLaunchAttempts for Engine {
    fn settle_launch_attempts(
        &mut self,
        quarantine: TableReference<QuarantinedLaunchAttempt>,
    ) -> Result<Vec<LaunchAttemptSettlement>, StoreError> {
        let mut settlements = Vec::new();
        let mut unread = None;
        for (launch_request_id, archive) in self.launch_attempt_archives()? {
            let Err(decode_error) =
                rkyv::from_bytes::<StoredLaunchAttemptV1, rkyv::rancor::Error>(&archive)
            else {
                continue;
            };
            let shape =
                if rkyv::from_bytes::<StoredLaunchAttemptBeforeAddress, rkyv::rancor::Error>(
                    &archive,
                )
                .is_ok()
                {
                    ArchivedLaunchAttemptShape::BeforeAddress
                } else if rkyv::from_bytes::<StoredLaunchAttemptBeforeBundle, rkyv::rancor::Error>(
                    &archive,
                )
                .is_ok()
                {
                    ArchivedLaunchAttemptShape::BeforeSystemPromptBundle
                } else {
                    ArchivedLaunchAttemptShape::Unknown(decode_error.to_string())
                };
            // The same table, named for moving aside a row 0.34's shape
            // does not read.
            let unread_table = match unread {
                Some(table) => table,
                None => {
                    let table =
                        self.register_table(TableDescriptor::<UnreadLaunchAttempt>::new(
                            FLOW_LAUNCH_ATTEMPT_TABLE_NAME,
                            FamilyName::new("flow-nexus-launch-attempt"),
                            StoredLaunchAttemptV1::schema_hash(),
                        ))?;
                    unread = Some(table);
                    table
                }
            };
            let quarantine_number = self
                .quarantined_launch_attempts_of(quarantine, &launch_request_id)?
                .len() as u64
                + 1;
            self.commit_atomic(
                self.begin_atomic_commit()
                    .retract(unread_table, RecordKey::new(launch_request_id.clone()))
                    .assert(
                        quarantine,
                        QuarantinedLaunchAttempt {
                            launch_request_id: launch_request_id.clone(),
                            quarantine_number,
                            archive,
                            shape: shape.clone(),
                        },
                    ),
            )?;
            settlements.push(match shape {
                ArchivedLaunchAttemptShape::Unknown(decode_error) => {
                    LaunchAttemptSettlement::Quarantined {
                        launch_request_id,
                        decode_error,
                    }
                }
                from => LaunchAttemptSettlement::SetAside {
                    launch_request_id,
                    from,
                },
            });
        }
        Ok(settlements)
    }

    fn launch_attempt_archives(&self) -> Result<Vec<(String, Vec<u8>)>, StoreError> {
        let definition =
            redb::TableDefinition::<String, &[u8]>::new(FLOW_LAUNCH_ATTEMPT_TABLE_NAME.as_str());
        Ok(self
            .storage_reader()
            .read(|transaction| {
                let table = match transaction.open_table(definition) {
                    Ok(table) => table,
                    Err(redb::TableError::TableDoesNotExist(_)) => return Ok(Vec::new()),
                    Err(error) => return Err(error.into()),
                };
                let mut rows = Vec::new();
                for entry in redb::ReadableTable::iter(&table)? {
                    let (key, archive) = entry?;
                    rows.push((key.value(), archive.value().to_vec()));
                }
                Ok(rows)
            })
            .map_err(sema_engine::Error::from)?)
    }

    fn quarantined_launch_attempts_of(
        &self,
        quarantine: TableReference<QuarantinedLaunchAttempt>,
        launch_request_id: &str,
    ) -> Result<Vec<QuarantinedLaunchAttempt>, StoreError> {
        Ok(self
            .match_records(QueryPlan::all(quarantine))?
            .records()
            .iter()
            .filter(|quarantined| quarantined.launch_request_id == launch_request_id)
            .cloned()
            .collect())
    }
}

trait AdoptsLegacyRoles {
    /// A store written before roles were kept has flows and no role rows.
    /// Each such flow's role is taken from what the store already holds: the
    /// launch profile of the launch that bound it, else the flow type
    /// MetaBindExisting wrote as `<aspect>:<power>:<model>`. Parsing that
    /// text is an exception taken only here, to read rows written before the
    /// typed record existed; nothing new is written in that form for reading.
    fn adopt_legacy_roles(&self) -> Result<(), StoreError>;
}

impl AdoptsLegacyRoles for FlowStore {
    fn adopt_legacy_roles(&self) -> Result<(), StoreError> {
        let flows = self
            .engine
            .match_records(QueryPlan::all(self.flows))?
            .records()
            .to_vec();
        // Settlement has already moved aside every row that did
        // not read; should the attempts still not read, adoption says so and
        // reads the flow types alone rather than refusing to open the store.
        let attempts = match self
            .engine
            .match_records(QueryPlan::all(self.launch_attempts))
        {
            Ok(matched) => matched.records().to_vec(),
            Err(error) => {
                eprintln!("flow-nexus: RoleAdoptionWithoutLaunchAttempts.«{error}»");
                Vec::new()
            }
        };
        // A launch archived in an earlier shape is kept aside whole; the
        // role it names is read from there.
        let kept_attempts = self
            .engine
            .match_records(QueryPlan::all(self.quarantined_launch_attempts))?
            .records()
            .to_vec();
        for flow in flows {
            if self.role(&flow.flow_id)?.is_some() {
                continue;
            }
            let launched = attempts.iter().find_map(|stored| {
                let attempt = &stored.attempt;
                attempt
                    .native_launch_binding_option
                    .as_ref()
                    .filter(|binding| binding.flow_id == flow.flow_id)
                    .map(|_| Caller {
                        flow_id: flow.flow_id.clone(),
                        aspect: attempt.launch_profile.address.aspect.clone(),
                        power_level: <crate::RunningNexus as crate::waking::WakePolicy>::power_of(
                            &attempt.launch_profile.address.layer,
                        ),
                        model_name: attempt.launch_profile.model_name.clone(),
                    })
            });
            let kept = || {
                kept_attempts
                    .iter()
                    .find_map(|kept| kept.kept_role(&flow.flow_id))
            };
            let Some(role) = launched.or_else(kept).or_else(|| flow.legacy_role()) else {
                continue;
            };
            self.engine
                .assert(Assertion::new(self.roles, StoredRole { caller: role }))?;
        }
        Ok(())
    }
}

trait ReadsLegacyRole {
    /// The role MetaBindExisting wrote into the flow type before roles had
    /// their own record; see `adopt_legacy_roles`.
    fn legacy_role(&self) -> Option<Caller>;
}

impl ReadsLegacyRole for FlowRecord {
    fn legacy_role(&self) -> Option<Caller> {
        let mut parts = self.flow_type.splitn(3, ':');
        let aspect = match parts.next()? {
            "Psyche" => Aspect::Psyche,
            "Mind" => Aspect::Mind,
            "Field" => Aspect::Field,
            _ => return None,
        };
        let power_level = match parts.next()? {
            "High" => PowerLevel::High,
            "Medium" => PowerLevel::Medium,
            "Low" => PowerLevel::Low,
            "UltraLow" => PowerLevel::UltraLow,
            _ => return None,
        };
        let model_name = parts.next().filter(|model| !model.is_empty())?;
        Some(Caller {
            flow_id: self.flow_id.clone(),
            aspect,
            power_level,
            model_name: model_name.into(),
        })
    }
}

impl RegistersFlowIdentity for FlowStore {
    fn register_flow(&self, flow_node: FlowNode) -> Result<FlowRegistration, StoreError> {
        let flow_type = match flow_node.harness_kind {
            HarnessKind::Codex => "codex-registered".into(),
            HarnessKind::Claude => "claude-registered".into(),
        };
        self.register_flow_as(flow_node, flow_type, None)
    }

    fn register_flow_in_role(
        &self,
        flow_node: FlowNode,
        role: Caller,
    ) -> Result<FlowRegistration, StoreError> {
        let flow_type = match flow_node.harness_kind {
            HarnessKind::Codex => "codex-registered".into(),
            HarnessKind::Claude => "claude-registered".into(),
        };
        self.register_flow_as(flow_node, flow_type, Some(role))
    }
}

impl RegistersExistingFlow for FlowStore {
    fn register_existing_flow(
        &self,
        flow_node: FlowNode,
        role: Caller,
    ) -> Result<FlowRegistration, StoreError> {
        if flow_node.flow_lifecycle != SignalFlowLifecycle::Pending
            || role.flow_id != flow_node.flow_id
        {
            return Ok(FlowRegistration::ConflictingBinding);
        }
        if let Some(flow) = self.flow(&flow_node.flow_id)? {
            return self.assert_role_of_matching_binding(flow, &flow_node, role);
        }
        // The flow type keeps the form it had before roles were a record.
        let flow_type = format!(
            "{:?}:{:?}:{}",
            role.aspect, role.power_level, role.model_name
        );
        self.register_flow_as(flow_node, flow_type, Some(role))
    }
}

impl ReadsFlowRoles for FlowStore {
    fn role(&self, flow_id: &str) -> Result<Option<Caller>, StoreError> {
        let records = self
            .engine
            .match_records(QueryPlan::key(self.roles, RecordKey::new(flow_id)))?
            .records()
            .to_vec();
        match records.as_slice() {
            [] => Ok(None),
            [role] => Ok(Some(role.caller.clone())),
            _ => Err(StoreError::StateInvariant),
        }
    }

    fn flows_in_pane(
        &self,
        herdr_session_name: &str,
        herdr_pane_id: &str,
    ) -> Result<Vec<FlowNode>, StoreError> {
        let mut nodes = Vec::new();
        for node in self.flow_nodes()? {
            let HerdrRouteSelection::Available(route) = &node.herdr_route_selection else {
                continue;
            };
            if route.herdr_session_name != herdr_session_name
                || route.herdr_pane_id != herdr_pane_id
                || !node.flow_lifecycle.is_live()
                || self.held_successor(&node.flow_id)?
            {
                continue;
            }
            nodes.push(node);
        }
        Ok(nodes)
    }
}

impl ReservesLaunchAttempt for FlowStore {
    fn reserve_launch_attempt(
        &self,
        launch: &ComposedLaunch,
        origin: Origin,
    ) -> Result<LaunchAttemptReservation, StoreError> {
        let launch_request_id = launch.launch_profile.launch_request_id.clone();
        let prompt_sha256 = launch.first_prompt_payload.prompt_sha256.clone();
        if let Some(existing) = self.stored_launch_attempt(&launch_request_id)? {
            return Ok(
                if existing.attempt.prompt_sha256 == prompt_sha256
                    && existing.attempt.launch_profile == launch.launch_profile
                    && existing.attempt.origin == origin
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
            origin,
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
                context: LaunchContext::Modules,
            },
        ))?;
        self.launch_changes.announce();
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
            || stored.attempt.launch_profile.model_name != intent.model_name
            || stored.attempt.launch_profile.effort != intent.effort
            || intent.prompt_sha256_vector.is_empty()
            || !intent
                .prompt_sha256_vector
                .iter()
                .chain([&intent.system_prompt_sha256])
                .all(|hash| {
                    hash.len() == 64
                        && hash
                            .bytes()
                            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
                })
            || stored.attempt.native_launch_intent_option.is_some()
        {
            return Ok(false);
        }
        stored.attempt.launch_attempt_phase = LaunchAttemptPhase::NativeLaunchIntentRecorded;
        stored.attempt.native_launch_intent_option = Some(intent);
        self.mutate_launch_attempt(stored)?;
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
        self.mutate_launch_attempt(stored)?;
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
        // The seat's binding is witnessed and its flow registered: the
        // launch takes its Address now, before the first prompt, so the
        // seat's first tool call is identified. A refused taking refuses
        // the acknowledgement.
        let flow_id = acknowledgement.flow_id.clone();
        let commit = match self.held(
            &stored.attempt.launch_profile,
            &flow_id,
            self.engine.begin_atomic_commit(),
        )? {
            Ok(commit) => commit,
            Err(_) => return Ok(false),
        };
        stored.attempt.launch_attempt_phase = LaunchAttemptPhase::RegistrationAcknowledged;
        stored.attempt.registration_acknowledgement_option = Some(acknowledgement);
        self.engine
            .commit_atomic(commit.mutate(self.launch_attempts, stored))?;
        self.launch_changes.announce();
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
        // The first part is the one about to be submitted; no Stop count
        // can be negative.
        let valid_parts = intent.prompt_sha256_vector == native_intent.prompt_sha256_vector
            && intent.parts_submitted == 1
            && intent.stops_reported >= 0;
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
            || !valid_parts
            || intent.herdr_pane_binding != acknowledgement.herdr_pane_binding
            || !valid_boundary
            || stored.attempt.prompt_delivery_intent_option.is_some()
        {
            return Ok(false);
        }
        stored.attempt.launch_attempt_phase = LaunchAttemptPhase::PromptIntentRecorded;
        stored.attempt.prompt_delivery_intent_option = Some(intent);
        self.mutate_launch_attempt(stored)?;
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
                        && previous.prompt_sha256_vector == updated.prompt_sha256_vector
                        && previous.parts_submitted == updated.parts_submitted
                        && previous.stops_reported == updated.stops_reported
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
        // The next part of a split first message is recorded before it is
        // typed: one more part submitted, the Stop count it waited on, and
        // nothing else changed.
        let part_advancement = stored.attempt.launch_attempt_phase
            == LaunchAttemptPhase::PromptAmbiguous
            && match (
                stored.attempt.prompt_delivery_result_option.as_ref(),
                &result,
            ) {
                (
                    Some(PromptDeliveryResult::Ambiguous(previous)),
                    PromptDeliveryResult::Ambiguous(updated),
                ) => {
                    let mut expected = previous.clone();
                    expected.parts_submitted += 1;
                    expected.stops_reported = updated.stops_reported;
                    &expected == updated
                        && updated.stops_reported >= previous.stops_reported
                        && usize::try_from(updated.parts_submitted)
                            .is_ok_and(|parts| parts <= updated.prompt_sha256_vector.len())
                }
                _ => false,
            };
        if !first_result && !ambiguity_promotion && !boundary_advancement && !part_advancement {
            return Ok(false);
        }
        let phase = match &result {
            PromptDeliveryResult::Ambiguous(observed_intent) if observed_intent == intent => {
                LaunchAttemptPhase::PromptAmbiguous
            }
            PromptDeliveryResult::Ambiguous(observed_intent)
                if boundary_advancement || part_advancement =>
            {
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
                    && receipt.prompt_sha256_vector == intent.prompt_sha256_vector
                    && usize::try_from(intent.parts_submitted)
                        .is_ok_and(|parts| parts == intent.prompt_sha256_vector.len()) =>
            {
                LaunchAttemptPhase::PromptObserved
            }
            PromptDeliveryResult::Observed(_) | PromptDeliveryResult::Ambiguous(_) => {
                return Ok(false);
            }
        };
        stored.attempt.launch_attempt_phase = phase;
        stored.attempt.prompt_delivery_result_option = Some(result);
        self.mutate_launch_attempt(stored)?;
        Ok(true)
    }
}

impl RecordsLaunchOutcome for FlowStore {
    fn record_launch_outcome(
        &self,
        launch_request_id: &str,
        outcome: LaunchOutcome,
    ) -> Result<(), StoreError> {
        // A launch refused before it was confirmed gives back the Address
        // its flow held, in the write that settles it.
        let refused = matches!(
            outcome,
            LaunchOutcome::StartRejected(_)
                | LaunchOutcome::ReplaceRejected(ReplaceRejection::LaunchRefused(_))
        );
        let stored = StoredLaunchOutcome {
            launch_request_id: launch_request_id.into(),
            launch_outcome: outcome,
        };
        let commit = self.engine.begin_atomic_commit();
        let mut commit = if self.launch_outcome(launch_request_id)?.is_some() {
            commit.mutate(self.launch_outcomes, stored)
        } else {
            commit.assert(self.launch_outcomes, stored)
        };
        if refused
            && let Some(binding) = self
                .launch_attempt(launch_request_id)?
                .and_then(|attempt| attempt.native_launch_binding_option)
        {
            commit = self.given_back(&binding.flow_id, commit)?;
        }
        self.engine.commit_atomic(commit)?;
        self.launch_changes.announce();
        Ok(())
    }

    fn launch_outcome(&self, launch_request_id: &str) -> Result<Option<LaunchOutcome>, StoreError> {
        let records = self
            .engine
            .match_records(QueryPlan::key(
                self.launch_outcomes,
                RecordKey::new(launch_request_id),
            ))?
            .records()
            .to_vec();
        match records.as_slice() {
            [] => Ok(None),
            [stored] => Ok(Some(stored.launch_outcome.clone())),
            _ => Err(StoreError::StateInvariant),
        }
    }
}

impl RecordsReplacement for FlowStore {
    fn record_replacement(&self, replacement: Replacement) -> Result<(), StoreError> {
        if self.replacement(&replacement.launch_request_id)?.is_none() {
            self.engine
                .assert(Assertion::new(self.replacements, replacement))?;
        }
        Ok(())
    }

    fn withdraw_replacement(&self, launch_request_id: &str) -> Result<(), StoreError> {
        if self.replacement(launch_request_id)?.is_some() {
            self.engine.retract(Retraction::new(
                self.replacements,
                RecordKey::new(launch_request_id),
            ))?;
        }
        Ok(())
    }

    fn replacement(&self, launch_request_id: &str) -> Result<Option<Replacement>, StoreError> {
        let records = self
            .engine
            .match_records(QueryPlan::key(
                self.replacements,
                RecordKey::new(launch_request_id),
            ))?
            .records()
            .to_vec();
        match records.as_slice() {
            [] => Ok(None),
            [replacement] => Ok(Some(replacement.clone())),
            _ => Err(StoreError::StateInvariant),
        }
    }

    fn held_successor(&self, flow_id: &str) -> Result<bool, StoreError> {
        for replacement in self
            .engine
            .match_records(QueryPlan::all(self.replacements))?
            .records()
        {
            if matches!(
                self.launch_outcome(&replacement.launch_request_id)?,
                Some(LaunchOutcome::Replaced(_))
            ) {
                continue;
            }
            let bound = self
                .launch_attempt(&replacement.launch_request_id)?
                .and_then(|attempt| attempt.native_launch_binding_option)
                .is_some_and(|binding| binding.flow_id == flow_id);
            if bound {
                return Ok(true);
            }
        }
        Ok(false)
    }
}

impl ReadsLaunchAttempt for FlowStore {
    fn launch_attempt(&self, launch_request_id: &str) -> Result<Option<LaunchAttempt>, StoreError> {
        Ok(self
            .stored_launch_attempt(launch_request_id)?
            .map(|stored| stored.attempt))
    }

    fn composable_launch_profile(
        &self,
        launch_request_id: &str,
    ) -> Result<Option<LaunchProfile>, StoreError> {
        Ok(self
            .stored_launch_attempt(launch_request_id)?
            .and_then(|stored| match stored.context {
                LaunchContext::Modules => Some(stored.attempt.launch_profile),
                LaunchContext::BeforeModules(_) => None,
            }))
    }

    fn launch_requests_bound_to(&self, flow_id: &str) -> Result<Vec<String>, StoreError> {
        Ok(self
            .engine
            .match_records(QueryPlan::all(self.launch_attempts))?
            .records()
            .iter()
            .filter(|stored| {
                stored
                    .attempt
                    .native_launch_binding_option
                    .as_ref()
                    .is_some_and(|binding| binding.flow_id == flow_id)
            })
            .map(|stored| stored.attempt.launch_request_id.clone())
            .collect())
    }

    fn ambiguous_launch_attempts(&self) -> Result<Vec<LaunchAttempt>, StoreError> {
        let mut ambiguous = Vec::new();
        for stored in self
            .engine
            .match_records(QueryPlan::all(self.launch_attempts))?
            .records()
        {
            if stored.attempt.launch_attempt_phase == LaunchAttemptPhase::PromptAmbiguous
                && self.launch_attempt_unsettled(&stored.attempt)?
            {
                ambiguous.push(stored.attempt.clone());
            }
        }
        Ok(ambiguous)
    }

    fn launch_attempt_unsettled(&self, attempt: &LaunchAttempt) -> Result<bool, StoreError> {
        if self.launch_outcome(&attempt.launch_request_id)?.is_some() {
            return Ok(false);
        }
        let Some(binding) = attempt.native_launch_binding_option.as_ref() else {
            return Ok(true);
        };
        Ok(self
            .flow(&binding.flow_id)?
            .is_none_or(|flow| flow.lifecycle.is_live()))
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
        let Some(node) = self.flow_node(flow_id)? else {
            return Ok(Response::RecipientResolutionRejected(
                RecipientResolutionRejection::UnknownFlow,
            ));
        };
        if node.session_id.is_empty()
            || !node.flow_lifecycle.is_live()
            || self.held_successor(flow_id)?
        {
            return Ok(Response::RecipientResolutionRejected(
                RecipientResolutionRejection::FlowUnavailable,
            ));
        }
        Ok(Response::RecipientResolved(node))
    }
}

pub(crate) trait ReadsLaunchInspectionRecords: Sized {
    fn launch_inspection_records(
        &self,
        launch_request_id: &str,
    ) -> Result<Option<LaunchInspectionRecords>, StoreError>;
}

impl ReadsLaunchInspectionRecords for FlowStore {
    /// Reads a launch attempt, its outcome and its recorded Flow from one
    /// unchanged database marker. This is a checked multi-read view, not a
    /// storage transaction.
    fn launch_inspection_records(
        &self,
        launch_request_id: &str,
    ) -> Result<Option<LaunchInspectionRecords>, StoreError> {
        let attempts = self.engine.match_records(QueryPlan::key(
            self.launch_attempts,
            RecordKey::new(launch_request_id),
        ))?;
        let outcomes = self.engine.match_records(QueryPlan::key(
            self.launch_outcomes,
            RecordKey::new(launch_request_id),
        ))?;
        let marker = attempts.database_marker();
        if outcomes.database_marker() != marker {
            return Err(StoreError::LaunchStatusReadViewChanged);
        }

        let attempt_rows = attempts.records();
        let outcome_rows = outcomes.records();
        if attempt_rows.len() > 1 || outcome_rows.len() > 1 {
            if self.engine.current_database_marker()? != marker {
                return Err(StoreError::LaunchStatusReadViewChanged);
            }
            return Err(StoreError::LaunchStatusInconsistent);
        }
        let Some(stored_attempt) = attempt_rows.first() else {
            if self.engine.current_database_marker()? != marker {
                return Err(StoreError::LaunchStatusReadViewChanged);
            }
            return if outcome_rows.is_empty() {
                Ok(None)
            } else {
                Err(StoreError::LaunchStatusInconsistent)
            };
        };

        let launch_attempt = stored_attempt.attempt.clone();
        if launch_attempt.launch_request_id != launch_request_id
            || launch_attempt.launch_profile.launch_request_id != launch_request_id
        {
            if self.engine.current_database_marker()? != marker {
                return Err(StoreError::LaunchStatusReadViewChanged);
            }
            return Err(StoreError::LaunchStatusInconsistent);
        }
        let launch_outcome_option = outcome_rows
            .first()
            .map(|stored| stored.launch_outcome.clone());
        let mut inconsistent = outcome_rows
            .first()
            .is_some_and(|stored| stored.launch_request_id != launch_request_id);
        let flow_id_option = match launch_outcome_option.as_ref() {
            Some(LaunchOutcome::Started(launched)) => {
                let matches_attempt = launch_attempt
                    .native_launch_binding_option
                    .as_ref()
                    .is_some_and(|binding| {
                        binding.launch_request_id == launch_request_id
                            && binding.flow_id == launched.flow_id
                            && binding.native_session_id == launched.session_id
                    });
                if matches_attempt {
                    Some(launched.flow_id.clone())
                } else {
                    inconsistent = true;
                    None
                }
            }
            Some(LaunchOutcome::Replaced(replaced)) => {
                let successor = &replaced.launched_record;
                let matches_attempt = launch_attempt
                    .native_launch_binding_option
                    .as_ref()
                    .is_some_and(|binding| {
                        binding.launch_request_id == launch_request_id
                            && binding.flow_id == successor.flow_id
                            && binding.native_session_id == successor.session_id
                    });
                if matches_attempt {
                    Some(successor.flow_id.clone())
                } else {
                    inconsistent = true;
                    None
                }
            }
            _ => launch_attempt
                .native_launch_binding_option
                .as_ref()
                .map(|binding| binding.flow_id.clone()),
        };

        let mut flow_node_option = None;
        if let Some(flow_id) = flow_id_option {
            let flows = self
                .engine
                .match_records(QueryPlan::key(self.flows, RecordKey::new(flow_id.as_str())))?;
            if flows.database_marker() != marker {
                return Err(StoreError::LaunchStatusReadViewChanged);
            }
            let flow_rows = flows.records();
            if flow_rows.len() > 1 {
                inconsistent = true;
            } else if let Some(flow) = flow_rows.first() {
                let routes = self.engine.match_records(QueryPlan::key(
                    self.herdr_routes,
                    RecordKey::new(flow_id.as_str()),
                ))?;
                if routes.database_marker() != marker {
                    return Err(StoreError::LaunchStatusReadViewChanged);
                }
                let route_rows = routes.records();
                if route_rows.len() > 1 {
                    inconsistent = true;
                } else {
                    flow_node_option = Some(FlowStore::recorded_flow_node(
                        flow.clone(),
                        route_rows.first().cloned(),
                    ));
                }
            }
        }

        if self.engine.current_database_marker()? != marker {
            return Err(StoreError::LaunchStatusReadViewChanged);
        }
        if inconsistent {
            return Err(StoreError::LaunchStatusInconsistent);
        }

        Ok(Some(LaunchInspectionRecords {
            launch_attempt,
            launch_outcome_option,
            flow_node_option,
        }))
    }
}

impl ReadsFlowRows for FlowStore {
    fn flow_node(&self, flow_id: &str) -> Result<Option<FlowNode>, StoreError> {
        let Some(flow) = self.flow(flow_id)? else {
            return Ok(None);
        };
        Ok(Some(FlowStore::recorded_flow_node(
            flow,
            self.herdr_route(flow_id)?,
        )))
    }

    fn flow_nodes(&self) -> Result<Vec<FlowNode>, StoreError> {
        let mut nodes = self
            .engine
            .match_records(QueryPlan::all(self.flows))?
            .records()
            .iter()
            .map(|flow| self.flow_node(&flow.flow_id))
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        nodes.sort_by(|left, right| left.flow_id.cmp(&right.flow_id));
        Ok(nodes)
    }
}

impl RecordsFlowLifecycle for FlowStore {
    fn record_active(&self, flow_id: &str) -> Result<bool, StoreError> {
        let Some(mut flow) = self.flow(flow_id)? else {
            return Ok(false);
        };
        if !flow.lifecycle.is_live() {
            return Ok(false);
        }
        flow.lifecycle = FlowLifecycle::Active;
        self.engine.mutate_keyed(KeyedMutation::new(
            self.flows,
            RecordKey::new(flow_id),
            flow,
        ))?;
        Ok(true)
    }

    fn record_stopped(&self, flow_id: &str) -> Result<bool, StoreError> {
        let Some(mut flow) = self.flow(flow_id)? else {
            return Ok(false);
        };
        flow.lifecycle = FlowLifecycle::Stopped;
        // A flow that ends before its launch was confirmed gives back the
        // Address it held; a confirmed one puts its metaflow to sleep.
        let commit = self.engine.begin_atomic_commit().mutate(self.flows, flow);
        self.engine
            .commit_atomic(self.ended_at_address(flow_id, commit)?)?;
        self.launch_changes.announce();
        Ok(true)
    }

    fn record_retired(&self, flow_id: &str) -> Result<bool, StoreError> {
        let Some(mut flow) = self.flow(flow_id)? else {
            return Ok(false);
        };
        if !flow.lifecycle.is_live() {
            return Ok(false);
        }
        flow.lifecycle = FlowLifecycle::Retired;
        // A flow that ends before its launch was confirmed gives back the
        // Address it held; a confirmed one puts its metaflow to sleep.
        let commit = self.engine.begin_atomic_commit().mutate(self.flows, flow);
        self.engine
            .commit_atomic(self.ended_at_address(flow_id, commit)?)?;
        self.launch_changes.announce();
        Ok(true)
    }

    fn record_exited(&self, flow_id: &str) -> Result<bool, StoreError> {
        let Some(mut flow) = self.flow(flow_id)? else {
            return Ok(false);
        };
        if !flow.lifecycle.is_live() {
            return Ok(false);
        }
        flow.lifecycle = FlowLifecycle::Exited;
        // A flow that ends before its launch was confirmed gives back the
        // Address it held; a confirmed one puts its metaflow to sleep.
        let commit = self.engine.begin_atomic_commit().mutate(self.flows, flow);
        self.engine
            .commit_atomic(self.ended_at_address(flow_id, commit)?)?;
        self.launch_changes.announce();
        Ok(true)
    }

    fn record_resumed(&self, flow_id: &str, route: HerdrRoute) -> Result<bool, StoreError> {
        let Some(mut flow) = self.flow(flow_id)? else {
            return Ok(false);
        };
        if !matches!(flow.lifecycle, FlowLifecycle::Stopped) {
            return Ok(false);
        }
        flow.lifecycle = FlowLifecycle::Pending;
        let route = FlowHerdrRouteRecord {
            flow_id: flow_id.to_owned(),
            route,
        };
        let commit = self.engine.begin_atomic_commit().mutate(self.flows, flow);
        let commit = if self.herdr_route(flow_id)?.is_some() {
            commit.mutate(self.herdr_routes, route)
        } else {
            commit.assert(self.herdr_routes, route)
        };
        self.engine.commit_atomic(commit)?;
        self.launch_changes.announce();
        Ok(true)
    }
}

impl WritesFlowStore for FlowStore {
    fn mutate_launch_attempt(&self, stored: StoredLaunchAttempt) -> Result<(), StoreError> {
        self.engine.mutate_keyed(KeyedMutation::new(
            self.launch_attempts,
            RecordKey::new(stored.attempt.launch_request_id.clone()),
            stored,
        ))?;
        self.launch_changes.announce();
        Ok(())
    }

    fn reserve_start(
        &self,
        flow_type: String,
        origin: Origin,
    ) -> Result<PendingLaunch, StoreError> {
        let state = self.state()?;
        let stable_socket = self.runtime_configuration()?.stable_codex.socket;
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
                                endpoint_path: stable_socket,
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
        let unrecorded = || {
            Response::StartRejected(StartRejection::NativeLaunchRefused(
                signal_flow::NativeLaunchRefusal {
                    native_launch_stage: signal_flow::NativeLaunchStage::Confirm,
                    native_launch_failure: signal_flow::NativeLaunchFailure::SessionUnrecorded,
                },
            ))
        };
        let Some(mut flow) = self.flow(flow_id)? else {
            return Ok(unrecorded());
        };
        if flow.thread_id.is_none() {
            return Ok(unrecorded());
        }
        let origin = flow.origin.clone();
        let session_id = flow.thread_id.clone().expect("checked thread identity");
        // The launch that bound this flow names the Address it takes, in
        // the same write that makes it Active; a launch that held it since
        // its binding keeps it and drops the hold.
        let mut taken = None;
        if let Some(launch_request_id) = self.launch_requests_bound_to(flow_id)?.pop()
            && let Some(attempt) = self.launch_attempt(&launch_request_id)?
        {
            let profile = attempt.launch_profile;
            match self.address_taking(
                &profile.address,
                flow_id,
                profile.flow_id_option.as_deref(),
                0,
            )? {
                metaflow::AddressTaking::Takes(taking) => taken = Some(taking),
                metaflow::AddressTaking::Refused(rejection) => {
                    return Ok(Response::StartRejected(rejection));
                }
            }
        }
        let mut commit = self.engine.begin_atomic_commit();
        let mut written = false;
        if flow.lifecycle == FlowLifecycle::Pending {
            flow.lifecycle = FlowLifecycle::Active;
            flow.generation = 1;
            commit = commit.mutate(self.flows, flow);
            written = true;
        }
        if let Some(taking) = taken {
            commit = taking.written(self, commit);
            written = true;
        }
        if self.address_hold(flow_id)?.is_some() {
            commit = self.kept(flow_id, commit)?;
            written = true;
        }
        if written {
            self.engine.commit_atomic(commit)?;
        }
        let herdr_route_selection = self
            .flow_node(flow_id)?
            .map(|node| node.herdr_route_selection)
            .unwrap_or(HerdrRouteSelection::Unavailable);
        Ok(Response::Started(Launched {
            flow_id: flow_id.into(),
            session_id,
            herdr_route_selection,
            origin,
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

/// Rows as earlier Flow versions left them, for fixtures: one launch attempt
/// archived before `SystemPromptBundleFile`, whose native launch bound
/// `bound_flow_id` (registered here without a role, as Start registered it
/// then), one archived before the profile named an Address, and one row no
/// known shape reads.
#[cfg(test)]
pub(crate) struct EarlierLaunchAttemptRows {
    pub bound_flow_id: String,
}

#[cfg(test)]
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
struct NotALaunchAttempt {
    launch_request_id: String,
}

#[cfg(test)]
impl EngineRecord for NotALaunchAttempt {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.launch_request_id.clone())
    }
}

#[cfg(test)]
impl EngineRecord for StoredLaunchAttemptBeforeBundle {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.attempt.launch_request_id.clone())
    }
}

#[cfg(test)]
impl EngineRecord for StoredLaunchAttemptBeforeAddress {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.attempt.launch_request_id.clone())
    }
}

#[cfg(test)]
impl EarlierLaunchAttemptRows {
    pub const EARLIER_REQUEST: &str = "flow06-earlier";
    pub const BEFORE_ADDRESS_REQUEST: &str = "flow15-before-address";
    pub const UNREADABLE_REQUEST: &str = "unreadable-request";
    pub const ZERO_THIRTY_FOUR_REQUEST: &str = "flow34-before-modules";

    /// A launch as signal-flow 15.0.0 archived it: its profile named the
    /// Aspect alone. It bound no flow.
    fn before_address_attempt(&self) -> LaunchAttemptBeforeAddress {
        let earlier = self.earlier_attempt();
        let profile = earlier.launch_profile;
        LaunchAttemptBeforeAddress {
            launch_request_id: Self::BEFORE_ADDRESS_REQUEST.into(),
            launch_profile: LaunchProfileBeforeAddress {
                launch_request_id: Self::BEFORE_ADDRESS_REQUEST.into(),
                launch_source_vector: profile.launch_source_vector,
                skill_name_vector: profile.skill_name_vector,
                aspect: Aspect::Field,
                power_level: profile.power_level,
                harness_kind: profile.harness_kind,
                model_name: profile.model_name,
                effort: profile.effort,
                flow_id_option: None,
                remembered_flow_vector: profile.remembered_flow_vector,
                herdr_session_name: profile.herdr_session_name,
                system_prompt_bundle_file: "/kept/bundle.md".into(),
                instruction_prompt: profile.instruction_prompt,
            },
            prompt_sha256: earlier.prompt_sha256,
            origin_clue: earlier.origin_clue,
            launch_attempt_phase: LaunchAttemptPhase::Reserved,
            native_launch_intent_option: None,
            native_launch_binding_option: None,
            registration_acknowledgement_option: None,
            prompt_delivery_intent_option: None,
            prompt_delivery_result_option: None,
        }
    }

    fn earlier_attempt(&self) -> LaunchAttemptBeforeBundle {
        let pane = signal_flow::HerdrPaneBinding {
            launch_request_id: Self::EARLIER_REQUEST.into(),
            herdr_session_name: "messaging-build".into(),
            herdr_agent_name: "psyche-opus-earlier".into(),
            herdr_workspace_id: "w17".into(),
            herdr_pane_id: "w17:p1".into(),
            herdr_terminal_id: "term-earlier".into(),
        };
        LaunchAttemptBeforeBundle {
            launch_request_id: Self::EARLIER_REQUEST.into(),
            launch_profile: LaunchProfileBeforeBundle {
                launch_request_id: Self::EARLIER_REQUEST.into(),
                launch_source_vector: vec![LaunchSourceBeforeModules {
                    source_path: "skills/operation-main-flow.md".into(),
                    source_sha256: "ab".repeat(32),
                }],
                skill_name_vector: vec!["operation-main-flow".into()],
                aspect: Aspect::Psyche,
                power_level: PowerLevel::Medium,
                harness_kind: HarnessKind::Claude,
                model_name: "claude-opus-5-5".into(),
                effort: "medium".into(),
                flow_id_option: None,
                remembered_flow_vector: vec![RememberedFlow {
                    flow_id: "e51411".into(),
                    remembering_depth: 1,
                }],
                herdr_session_name: "messaging-build".into(),
                instruction_prompt: "earlier instruction".into(),
            },
            prompt_sha256: "cd".repeat(32),
            origin_clue: OriginClueBeforeOrigin {
                flow_id: "e51411".into(),
                session_id: "e51411-session".into(),
                turn_id: "handover-successor".into(),
            },
            launch_attempt_phase: LaunchAttemptPhase::NativeBound,
            native_launch_intent_option: Some(NativeLaunchIntentBeforeModules {
                launch_request_id: Self::EARLIER_REQUEST.into(),
                prompt_sha256: "cd".repeat(32),
                harness_kind: HarnessKind::Claude,
                model_name: "claude-opus-5-5".into(),
                effort: "medium".into(),
                skill_name_vector: vec!["operation-main-flow".into()],
            }),
            native_launch_binding_option: Some(NativeLaunchBinding {
                launch_request_id: Self::EARLIER_REQUEST.into(),
                flow_id: self.bound_flow_id.clone(),
                native_session_id: format!("{}-session", self.bound_flow_id),
                harness_kind: HarnessKind::Claude,
                herdr_pane_binding: pane,
            }),
            registration_acknowledgement_option: None,
            prompt_delivery_intent_option: None,
            prompt_delivery_result_option: None,
        }
    }

    /// A launch as Flow 0.34 archived it: its context named by skill
    /// sources and a bundle file, its first message observed.
    fn zero_thirty_four_attempt(&self) -> LaunchAttemptBeforeModules {
        let request = Self::ZERO_THIRTY_FOUR_REQUEST;
        let selection = NativeSkillSelectionBeforeModules {
            skill_name: "operation-main-flow".into(),
            native_skill_path: "/kept/skills/operation-main-flow/SKILL.md".into(),
            native_skill_sha256: "ef".repeat(32),
        };
        let pane = signal_flow::HerdrPaneBinding {
            launch_request_id: request.into(),
            herdr_session_name: "messaging-build".into(),
            herdr_agent_name: "mind-sol-34".into(),
            herdr_workspace_id: "w34".into(),
            herdr_pane_id: "w34:p1".into(),
            herdr_terminal_id: "term-34".into(),
        };
        let boundary = NativeTranscriptBoundary::Absent(signal_flow::NativeTranscriptAbsence {
            native_session_id: "session-34".into(),
            harness_kind: HarnessKind::Codex,
            transcript_root_device: "64768".into(),
            transcript_root_inode: "1234".into(),
        });
        let intent = PromptDeliveryIntentBeforeModules {
            launch_request_id: request.into(),
            prompt_sha256: "ab".repeat(32),
            flow_id: "flow34".into(),
            native_session_id: "session-34".into(),
            harness_kind: HarnessKind::Codex,
            model_name: "gpt-sol".into(),
            effort: "medium".into(),
            native_skill_selection_vector: vec![selection.clone()],
            herdr_pane_binding: pane.clone(),
            native_transcript_boundary: boundary,
        };
        LaunchAttemptBeforeModules {
            launch_request_id: request.into(),
            launch_profile: LaunchProfileBeforeModules {
                launch_request_id: request.into(),
                launch_source_vector: vec![LaunchSourceBeforeModules {
                    source_path: "/kept/skills/operation-main-flow.md".into(),
                    source_sha256: "cd".repeat(32),
                }],
                skill_name_vector: vec!["operation-main-flow".into()],
                address: Address {
                    aspect: Aspect::Mind,
                    topic: "flow".into(),
                    layer: Layer::Secondary,
                },
                power_level: PowerLevel::Medium,
                harness_kind: HarnessKind::Codex,
                model_name: "gpt-sol".into(),
                effort: "medium".into(),
                flow_id_option: Some("predecessor-34".into()),
                remembered_flow_vector: vec![RememberedFlow {
                    flow_id: "e51411".into(),
                    remembering_depth: 2,
                }],
                herdr_session_name: "messaging-build".into(),
                system_prompt_bundle_file: "/kept/bundles/flow34.md".into(),
                instruction_prompt: "0.34 instruction".into(),
            },
            prompt_sha256: "ab".repeat(32),
            origin_clue: OriginClueBeforeOrigin {
                flow_id: "e51411".into(),
                session_id: "e51411-session".into(),
                turn_id: "turn-34".into(),
            },
            launch_attempt_phase: LaunchAttemptPhase::PromptObserved,
            native_launch_intent_option: Some(NativeLaunchIntentBeforeModules {
                launch_request_id: request.into(),
                prompt_sha256: "ab".repeat(32),
                harness_kind: HarnessKind::Codex,
                model_name: "gpt-sol".into(),
                effort: "medium".into(),
                skill_name_vector: vec!["operation-main-flow".into()],
            }),
            native_launch_binding_option: Some(NativeLaunchBinding {
                launch_request_id: request.into(),
                flow_id: "flow34".into(),
                native_session_id: "session-34".into(),
                harness_kind: HarnessKind::Codex,
                herdr_pane_binding: pane.clone(),
            }),
            registration_acknowledgement_option: Some(RegistrationAcknowledgement {
                launch_request_id: request.into(),
                flow_id: "flow34".into(),
                native_session_id: "session-34".into(),
                herdr_pane_binding: pane,
            }),
            prompt_delivery_intent_option: Some(intent),
            prompt_delivery_result_option: Some(PromptDeliveryResultBeforeModules::Observed(
                NativeTargetReceiptBeforeModules {
                    launch_request_id: request.into(),
                    prompt_sha256: "ab".repeat(32),
                    flow_id: "flow34".into(),
                    native_session_id: "session-34".into(),
                    native_turn_id: "turn-1".into(),
                    receipt_sha256: "12".repeat(32),
                    model_name: "gpt-sol".into(),
                    effort: "medium".into(),
                    native_skill_selection_vector: vec![selection],
                },
            )),
        }
    }

    /// Writes the rows into the store at `path` and closes it: the store
    /// is made current, then its attempts table is set back to 0.34's
    /// family identity, as an 0.34 store holds it, and the earlier rows
    /// are written under it.
    pub fn seed(&self, path: &Path) {
        let store = FlowStore::open(path).expect("store to seed opens");
        let node = signal_flow::FlowNode {
            flow_id: self.bound_flow_id.clone(),
            session_id: format!("{}-session", self.bound_flow_id),
            harness_kind: HarnessKind::Claude,
            endpoint_selection: EndpointSelection::Unavailable,
            herdr_route_selection: HerdrRouteSelection::Available(HerdrRoute {
                herdr_session_name: "messaging-build".into(),
                herdr_agent_name: "psyche-opus-earlier".into(),
                herdr_pane_id: "w17:p1".into(),
                herdr_terminal_id: "term-earlier".into(),
            }),
            origin: self.earlier_attempt().origin_clue.into(),
            flow_lifecycle: SignalFlowLifecycle::Pending,
        };
        assert!(matches!(
            store.register_flow(node).expect("earlier flow registered"),
            FlowRegistration::Registered(_)
        ));
        drop(store);
        StoredLaunchAttemptV1::restore_family(path);

        let mut engine = Engine::open(EngineOpen::new(path, SchemaVersion::new(1)))
            .expect("store opens under the engine");
        // Every shape names the table under 0.34's identity.
        let descriptor = || {
            (
                FLOW_LAUNCH_ATTEMPT_TABLE_NAME,
                FamilyName::new("flow-nexus-launch-attempt"),
                StoredLaunchAttemptV1::schema_hash(),
            )
        };
        let (name, family, schema) = descriptor();
        let zero_thirty_four = engine
            .register_table::<StoredLaunchAttemptV1>(TableDescriptor::new(name, family, schema))
            .expect("0.34 shape names the table");
        engine
            .assert(Assertion::new(
                zero_thirty_four,
                StoredLaunchAttemptV1 {
                    attempt: self.zero_thirty_four_attempt(),
                },
            ))
            .expect("0.34 row written");
        let (name, family, schema) = descriptor();
        let earlier = engine
            .register_table::<StoredLaunchAttemptBeforeBundle>(TableDescriptor::new(
                name, family, schema,
            ))
            .expect("earlier shape names the table");
        engine
            .assert(Assertion::new(
                earlier,
                StoredLaunchAttemptBeforeBundle {
                    attempt: self.earlier_attempt(),
                },
            ))
            .expect("earlier row written");
        let (name, family, schema) = descriptor();
        let before_address = engine
            .register_table::<StoredLaunchAttemptBeforeAddress>(TableDescriptor::new(
                name, family, schema,
            ))
            .expect("15.0.0 shape names the table");
        engine
            .assert(Assertion::new(
                before_address,
                StoredLaunchAttemptBeforeAddress {
                    attempt: self.before_address_attempt(),
                },
            ))
            .expect("15.0.0 row written");
        let (name, family, schema) = descriptor();
        let unreadable = engine
            .register_table::<NotALaunchAttempt>(TableDescriptor::new(name, family, schema))
            .expect("unreadable shape names the table");
        engine
            .assert(Assertion::new(
                unreadable,
                NotALaunchAttempt {
                    launch_request_id: Self::UNREADABLE_REQUEST.into(),
                },
            ))
            .expect("unreadable row written");
    }
}

/// Sets a store's attempts table back to the family identity Flow 0.34
/// registered it under. The table must hold no row.
#[cfg(test)]
trait RestoresZeroThirtyFourFamily {
    fn restore_family(path: &Path);
}

#[cfg(test)]
impl RestoresZeroThirtyFourFamily for StoredLaunchAttemptV1 {
    fn restore_family(path: &Path) {
        let registration = TableRegistration::new(
            TableDescriptor::<StoredLaunchAttemptV1>::new(
                FLOW_LAUNCH_ATTEMPT_TABLE_NAME,
                FamilyName::new("flow-nexus-launch-attempt"),
                StoredLaunchAttemptV1::schema_hash(),
            )
            .family_identity(),
        );
        let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&registration)
            .expect("registration archives")
            .to_vec();
        let database = redb::Database::open(path).expect("store opens directly");
        let write = database.begin_write().expect("catalog transaction starts");
        {
            let mut catalog = write.open_table(SEMA_CATALOG_TABLE).expect("catalog opens");
            catalog
                .insert(FLOW_LAUNCH_ATTEMPT_TABLE_NAME.as_str(), bytes.as_slice())
                .expect("0.34 identity written");
        }
        write.commit().expect("catalog commits");
    }
}

/// Store identity, catalogue validation and committed-record operations.
pub(crate) trait ReadsFlowMemory {
    fn layer_key(layer: &Layer) -> &'static str {
        match layer {
            Layer::Primary => "Primary",
            Layer::Secondary => "Secondary",
            Layer::Tertiary => "Tertiary",
            Layer::Quaternary => "Quaternary",
        }
    }

    fn address_record_key(address: &Address) -> String {
        let aspect = match &address.aspect {
            Aspect::Psyche => "Psyche",
            Aspect::Mind => "Mind",
            Aspect::Field => "Field",
        };
        let layer = match &address.layer {
            Layer::Primary => "Primary",
            Layer::Secondary => "Secondary",
            Layer::Tertiary => "Tertiary",
            Layer::Quaternary => "Quaternary",
        };
        format!("{aspect}:{}:{}:{layer}", address.topic.len(), address.topic)
    }

    fn process_start(pid: i64) -> Option<(i64, i64)> {
        let pid = u32::try_from(pid).ok()?;
        let stat = fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
        let (_, fields) = stat.rsplit_once(") ")?;
        let mut fields = fields.split_whitespace();
        let _state = fields.next()?;
        let parent = fields.next()?.parse::<i64>().ok()?;
        let mut fields = fields.skip(17);
        let started = fields.next()?.parse::<i64>().ok()?;
        Some((parent, started))
    }

    fn process_is_live(process: &Process) -> bool {
        FlowStore::process_start(process.pid).is_some_and(|(_, started)| started == process.started)
    }

    fn process_identity_for_pid(pid: u32) -> Option<Process> {
        let pid = i64::from(pid);
        let (_, started) = FlowStore::process_start(pid)?;
        Some(Process { pid, started })
    }

    fn process_executable(process: &Process) -> Option<std::path::PathBuf> {
        if !FlowStore::process_is_live(process) {
            return None;
        }
        let executable = fs::canonicalize(format!("/proc/{}/exe", process.pid)).ok()?;
        FlowStore::process_is_live(process).then_some(executable)
    }

    fn process_lineage(process: &Process) -> Vec<Process> {
        const ANCESTRY_LIMIT: usize = 64;
        let mut lineage = Vec::new();
        let mut pid = process.pid;
        for depth in 0..ANCESTRY_LIMIT {
            let Some((parent, started)) = FlowStore::process_start(pid) else {
                break;
            };
            if depth == 0 && started != process.started {
                return Vec::new();
            }
            lineage.push(Process { pid, started });
            if pid <= 1 || parent <= 1 || parent == pid {
                break;
            }
            pid = parent;
        }
        lineage
    }

    fn current_flow_family_identity(table_name: &str) -> Option<(&'static str, SchemaHash)> {
        let (family, version) = match table_name {
            "flow_nexus_flows" => return Some(("flow-nexus-flow", FlowRecord::schema_hash())),
            "flow_nexus_state" => ("flow-nexus-state", "flow-nexus-state-v2"),
            "flow_nexus_configuration" => {
                ("flow-nexus-configuration", "flow-nexus-configuration-v1")
            }
            "flow_nexus_herdr_routes" => ("flow-nexus-herdr-route", "flow-nexus-herdr-route-v1"),
            "flow_nexus_launch_panes" => ("flow-nexus-launch-pane", "flow-nexus-launch-pane-v1"),
            "flow_nexus_launch_attempts" => {
                return Some((
                    "flow-nexus-launch-attempt",
                    StoredLaunchAttempt::schema_hash(),
                ));
            }
            "flow_nexus_runtime_configuration" => (
                "flow-nexus-runtime-configuration",
                "flow-nexus-runtime-configuration-v1",
            ),
            "flow_nexus_launch_outcomes" => {
                return Some((
                    "flow-nexus-launch-outcome",
                    StoredLaunchOutcome::schema_hash(),
                ));
            }
            "flow_nexus_replacements" => ("flow-nexus-replacement", "flow-nexus-replacement-v1"),
            "flow_nexus_roles" => ("flow-nexus-role", "flow-nexus-role-v1"),
            "flow_nexus_quarantined_launch_attempts" => (
                "flow-nexus-quarantined-launch-attempt",
                "flow-nexus-quarantined-launch-attempt-v1",
            ),
            "flow_nexus_metaflows" => ("flow-nexus-metaflow", "flow-nexus-metaflow-v2"),
            "flow_nexus_metaflow_processes" => (
                "flow-nexus-metaflow-process",
                "flow-nexus-metaflow-process-v1",
            ),
            "flow_nexus_metaflow_locks" => {
                ("flow-nexus-metaflow-lock", "flow-nexus-metaflow-lock-v1")
            }
            "flow_nexus_deliveries" => ("flow-nexus-delivery", "flow-nexus-delivery-v1"),
            "flow_nexus_pane_leases" => ("flow-nexus-pane-lease", "flow-nexus-pane-lease-v1"),
            "flow_nexus_delivery_configuration" => (
                "flow-nexus-delivery-configuration",
                "flow-nexus-delivery-configuration-v1",
            ),
            "flow_nexus_meta_nexus_configuration" => (
                "flow-nexus-meta-nexus-configuration",
                "flow-nexus-meta-nexus-configuration-v1",
            ),
            "flow_nexus_meta_model_configuration" => (
                "flow-nexus-meta-model-configuration",
                "flow-nexus-meta-model-configuration-v1",
            ),
            "flow_nexus_meta_threshold_configuration" => (
                "flow-nexus-meta-threshold-configuration",
                "flow-nexus-meta-threshold-configuration-v1",
            ),
            "flow_nexus_meta_leave_out_configuration" => (
                "flow-nexus-meta-leave-out-configuration",
                "flow-nexus-meta-leave-out-configuration-v1",
            ),
            "flow_nexus_flow_events" => ("flow-nexus-flow-events", "flow-nexus-flow-events-v1"),
            "flow_nexus_flow_context_sizes" => (
                context::FLOW_CONTEXT_SIZE_FAMILY,
                context::FLOW_CONTEXT_SIZE_SCHEMA,
            ),
            "flow_nexus_address_holds" => (
                address_hold::ADDRESS_HOLD_FAMILY,
                address_hold::ADDRESS_HOLD_SCHEMA,
            ),
            "flow_nexus_metaflow_refreshes" => (
                refresh::METAFLOW_REFRESH_FAMILY,
                refresh::METAFLOW_REFRESH_SCHEMA,
            ),
            "flow_nexus_launch_opened_threads" => (
                opened_thread::OPENED_THREAD_FAMILY,
                opened_thread::OPENED_THREAD_SCHEMA,
            ),
            "flow_nexus_send_calls" => (send_call::SEND_CALL_FAMILY, send_call::SEND_CALL_SCHEMA),
            "flow_nexus_launch_cleanups" => (
                launch_cleanup::LAUNCH_CLEANUP_FAMILY,
                launch_cleanup::LAUNCH_CLEANUP_SCHEMA,
            ),
            _ => return None,
        };
        Some((family, SchemaHash::for_label(version)))
    }

    /// A family registration this Flow opens: its current identity, a
    /// predecessor of a family the open upgrades, or a retired table the
    /// open drops.
    fn is_current_flow_registration(table_key: &str, registration: &TableRegistration) -> bool {
        let identity = registration.identity();
        let matches = |family: &str, schema_hash: SchemaHash| {
            identity.table_name() == table_key
                && identity.family().as_str() == family
                && identity.schema_hash() == schema_hash
        };
        let predecessors: &[(&str, SchemaHash)] = match table_key {
            "flow_nexus_flows" => &[("flow-nexus-flow", FlowRecordV5::schema_hash())],
            "flow_nexus_launch_outcomes" => &[
                (
                    "flow-nexus-launch-outcome",
                    StoredLaunchOutcomeV2::schema_hash(),
                ),
                (
                    "flow-nexus-launch-outcome",
                    StoredLaunchOutcomeV3::schema_hash(),
                ),
                (
                    "flow-nexus-launch-outcome",
                    StoredLaunchOutcomeV4::schema_hash(),
                ),
            ],
            "flow_nexus_launch_attempts" => &[(
                "flow-nexus-launch-attempt",
                StoredLaunchAttemptV1::schema_hash(),
            )],
            "flow_nexus_metaflows" => &[("flow-nexus-metaflow", StoredMetaflowV1::schema_hash())],
            "flow_nexus_address_holds" => &[(
                address_hold::ADDRESS_HOLD_FAMILY,
                <address_hold::AddressHoldV1 as MemoryDefinition>::schema_hash(),
            )],
            _ => &[],
        };
        FlowStore::current_flow_family_identity(table_key)
            .is_some_and(|(family, schema_hash)| matches(family, schema_hash))
            || predecessors
                .iter()
                .any(|(family, schema_hash)| matches(family, *schema_hash))
            || RETIRED_TABLES.iter().any(|(table, family, schema)| {
                *table == table_key && matches(family, SchemaHash::for_label(schema))
            })
    }

    /// Drops each retired table a store holds, its rows and its catalog
    /// registration, in one write before the engine opens the store.
    fn drop_retired_tables(path: &Path) -> Result<(), StoreError> {
        use redb::ReadableTable as _;
        let failed = |error: String| StoreError::RetiredTableDrop(error);
        let database = redb::Database::open(path).map_err(|error| failed(error.to_string()))?;
        let write = database
            .begin_write()
            .map_err(|error| failed(error.to_string()))?;
        let mut dropped = false;
        {
            let mut catalog = write
                .open_table(SEMA_CATALOG_TABLE)
                .map_err(|error| failed(error.to_string()))?;
            for (table, _, _) in RETIRED_TABLES {
                if catalog
                    .get(*table)
                    .map_err(|error| failed(error.to_string()))?
                    .is_some()
                {
                    catalog
                        .remove(*table)
                        .map_err(|error| failed(error.to_string()))?;
                    dropped = true;
                }
            }
        }
        for (table, _, _) in RETIRED_TABLES {
            dropped |= write
                .delete_table(redb::TableDefinition::<&str, &[u8]>::new(table))
                .map_err(|error| failed(error.to_string()))?;
        }
        if dropped {
            write.commit().map_err(|error| failed(error.to_string()))?;
        } else {
            write.abort().map_err(|error| failed(error.to_string()))?;
        }
        Ok(())
    }

    fn has_expected_flow_catalog(actual: &std::collections::BTreeSet<String>) -> bool {
        const CONFIGURATION_ONLY: &[&str] = &[
            "flow_nexus_state",
            "flow_nexus_configuration",
            "flow_nexus_runtime_configuration",
            "flow_nexus_delivery_configuration",
            "flow_nexus_meta_nexus_configuration",
            "flow_nexus_meta_model_configuration",
            "flow_nexus_meta_threshold_configuration",
        ];
        const RUNNING: &[&str] = &[
            "flow_nexus_flows",
            "flow_nexus_state",
            "flow_nexus_metaflows",
            "flow_nexus_metaflow_processes",
            "flow_nexus_metaflow_locks",
            "flow_nexus_configuration",
            "flow_nexus_runtime_configuration",
            "flow_nexus_herdr_routes",
            "flow_nexus_launch_attempts",
            "flow_nexus_launch_panes",
            "flow_nexus_launch_outcomes",
            "flow_nexus_replacements",
            "flow_nexus_roles",
            "flow_nexus_quarantined_launch_attempts",
            "flow_nexus_deliveries",
            "flow_nexus_pane_leases",
            "flow_nexus_delivery_configuration",
            "flow_nexus_meta_nexus_configuration",
            "flow_nexus_meta_model_configuration",
            "flow_nexus_meta_threshold_configuration",
            "flow_nexus_flow_events",
        ];
        // A running store keeps each flow's context size from 0.28.0 on, and
        // each launch's Address hold from 0.32.0 on; a running store written
        // before either opens, and its first full open adds the table, as
        // the first full open of a configuration-only store adds the running
        // tables.
        // A store keeps its metaflows' default leave-outs from 0.35.0 on,
        // its fresh-wake marks from 0.35.2 on, the Codex thread each
        // launch opened from 0.35.6 on, the send tool's calls from 0.35.7
        // on, and the cleanup of each interrupted launch from 0.35.11 on.
        const ADDED_LATER: &[&str] = &[
            "flow_nexus_flow_context_sizes",
            "flow_nexus_address_holds",
            "flow_nexus_meta_leave_out_configuration",
            "flow_nexus_metaflow_refreshes",
            "flow_nexus_launch_opened_threads",
            "flow_nexus_send_calls",
            "flow_nexus_launch_cleanups",
        ];
        // A store before 0.35.0 may still hold a table 0.35.0 drops.
        let retired = |name: &String| RETIRED_TABLES.iter().any(|(table, _, _)| table == name);
        let running = RUNNING.iter().all(|name| actual.contains(*name))
            && actual.iter().all(|name| {
                RUNNING.contains(&name.as_str())
                    || ADDED_LATER.contains(&name.as_str())
                    || retired(name)
            });
        let configuration_only = CONFIGURATION_ONLY.iter().all(|name| actual.contains(*name))
            && actual.iter().all(|name| {
                CONFIGURATION_ONLY.contains(&name.as_str())
                    || name == "flow_nexus_meta_leave_out_configuration"
                    || retired(name)
            });
        configuration_only || running
    }

    fn recognize_flow_memory(path: &Path) -> Result<bool, StoreError> {
        if !path.is_absolute() || path.file_name().is_none() {
            return Err(StoreError::InvalidStorePath);
        }
        if !path.exists() {
            return Ok(false);
        }

        let reject = |reason: String| StoreError::UnrecognizedMemory {
            path: path.display().to_string(),
            reason,
        };
        // ReadOnlyDatabase refuses an active writer and never runs redb recovery.
        // In particular, do not use Database::open here: Sema's normal writer open
        // can stamp schema/layout metadata before a caller discovers this is an
        // old or foreign occupant.
        match redb::ReadOnlyDatabase::open(path) {
            Ok(database) => FlowStore::recognize_open_memory(&database, &reject),
            // A writer that did not close cleanly leaves redb's recovery flag,
            // and a read-only open refuses to repair. The flagged store is
            // recognized through a repaired copy, so the original is written
            // only by the writer open that follows recognition.
            Err(redb::DatabaseError::RepairAborted) => {
                FlowStore::recognize_repaired_copy(path, &reject)
            }
            Err(error) => Err(reject(error.to_string())),
        }
    }

    fn recognize_repaired_copy(
        path: &Path,
        reject: &dyn Fn(String) -> StoreError,
    ) -> Result<bool, StoreError> {
        /// The repaired copy is removed however recognition ends.
        struct RecognitionCopy(std::path::PathBuf);
        impl Drop for RecognitionCopy {
            fn drop(&mut self) {
                let _ = std::fs::remove_file(&self.0);
            }
        }
        let name = path
            .file_name()
            .ok_or(StoreError::InvalidStorePath)?
            .to_string_lossy();
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|elapsed| elapsed.as_nanos())
            .unwrap_or_default();
        let copy = RecognitionCopy(path.with_file_name(format!(
            ".{name}.recognition-{}-{nanos}",
            std::process::id()
        )));
        std::fs::copy(path, &copy.0).map_err(|error| {
            reject(format!(
                "cannot copy flagged store for recognition: {error}"
            ))
        })?;
        redb::Database::create(&copy.0)
            .map(drop)
            .map_err(|error| reject(format!("flagged store copy cannot be repaired: {error}")))?;
        let database = redb::ReadOnlyDatabase::open(&copy.0)
            .map_err(|error| reject(format!("repaired store copy: {error}")))?;
        let recognized = FlowStore::recognize_open_memory(&database, reject);
        drop(database);
        recognized
    }

    fn recognize_open_memory(
        database: &redb::ReadOnlyDatabase,
        reject: &dyn Fn(String) -> StoreError,
    ) -> Result<bool, StoreError> {
        use redb::{ReadableDatabase as _, ReadableTable as _};
        let read = database
            .begin_read()
            .map_err(|error| reject(error.to_string()))?;
        let version = {
            let table = read
                .open_table(SEMA_META_TABLE)
                .map_err(|error| reject(error.to_string()))?;
            table
                .get(SEMA_SCHEMA_VERSION_KEY)
                .map_err(|error| reject(error.to_string()))?
                .map(|value| value.value())
        };
        if version != Some(u64::from(FLOW_SCHEMA_VERSION)) {
            return Err(reject(format!(
                "Sema schema version is {version:?}, expected {FLOW_SCHEMA_VERSION}"
            )));
        }

        let storage_layout = {
            let counters = read
                .open_table(SEMA_ENGINE_COUNTERS_TABLE)
                .map_err(|error| reject(error.to_string()))?;
            counters
                .get(SEMA_ENGINE_STORAGE_LAYOUT_KEY)
                .map_err(|error| reject(error.to_string()))?
                .map(|value| rkyv::from_bytes::<u64, rkyv::rancor::Error>(value.value()))
                .transpose()
                .map_err(|error| reject(format!("engine storage layout: {error}")))?
        };
        if storage_layout != Some(FLOW_ENGINE_STORAGE_LAYOUT) {
            return Err(reject(format!(
                "Sema Engine storage layout is {storage_layout:?}, expected {FLOW_ENGINE_STORAGE_LAYOUT}"
            )));
        }

        let catalog_is_current = {
            let catalog = read
                .open_table(SEMA_CATALOG_TABLE)
                .map_err(|error| reject(error.to_string()))?;
            let mut registered_tables = std::collections::BTreeSet::new();
            for entry in catalog.iter().map_err(|error| reject(error.to_string()))? {
                let (table_key, bytes) = entry.map_err(|error| reject(error.to_string()))?;
                let table_key = table_key.value();
                let registration =
                    rkyv::from_bytes::<TableRegistration, rkyv::rancor::Error>(bytes.value())
                        .map_err(|error| {
                            reject(format!(
                                "Flow family registration {table_key:?} cannot be read: {error}"
                            ))
                        })?;
                if !FlowStore::is_current_flow_registration(table_key, &registration) {
                    return Err(reject(format!(
                        "family registration {table_key:?} is not an authored Flow Memory v2 identity"
                    )));
                }
                registered_tables.insert(table_key.to_owned());
            }
            FlowStore::has_expected_flow_catalog(&registered_tables)
        };
        if !catalog_is_current {
            return Err(reject(
                "family catalog is not a complete authored Flow Memory v2 configuration-only or running schema".into(),
            ));
        }

        let state_is_recognized = {
            let table = read
                .open_table(FLOW_STATE_RAW_TABLE)
                .map_err(|error| reject(error.to_string()))?;
            let bytes = table
                .get(STATE_KEY.to_owned())
                .map_err(|error| reject(error.to_string()))?
                .ok_or_else(|| reject("Flow state identity record is absent".into()))?;
            let state = rkyv::from_bytes::<FlowStoreState, rkyv::rancor::Error>(bytes.value())
                .map_err(|error| reject(format!("Flow state identity cannot be read: {error}")))?;
            state.next_flow_number > 0
        };
        if !state_is_recognized {
            return Err(reject("Flow state identity is invalid".into()));
        }
        Ok(true)
    }

    fn meta_configuration_snapshot(
        engine: &Engine,
        tables: &MetaConfigurationTables,
    ) -> Result<Option<ConfigurationSnapshot>, StoreError> {
        let nexus_rows = engine.match_records(QueryPlan::all(tables.nexus))?;
        let marker = nexus_rows.database_marker();
        let nexus = match nexus_rows.records() {
            [] => None,
            [stored] if stored.record_key() == RecordKey::new("nexus") => {
                Some(stored.nexus.clone())
            }
            _ => return Err(StoreError::StateInvariant),
        };
        let models_rows = engine.match_records(QueryPlan::all(tables.models))?;
        let thresholds_rows = engine.match_records(QueryPlan::all(tables.thresholds))?;
        let leave_out_rows = engine.match_records(QueryPlan::all(tables.leave_outs))?;
        if models_rows.database_marker() != marker
            || thresholds_rows.database_marker() != marker
            || leave_out_rows.database_marker() != marker
            || engine.current_database_marker()? != marker
        {
            return Err(StoreError::ConfigurationReadViewChanged);
        }
        let Some(nexus) = nexus else {
            return Ok(None);
        };
        let mut models = models_rows
            .records()
            .iter()
            .map(|stored| stored.model.clone())
            .collect::<Vec<_>>();
        models.sort_by_key(|model| FlowStore::layer_key(&model.layer));
        let mut thresholds = thresholds_rows
            .records()
            .iter()
            .map(|stored| stored.threshold.clone())
            .collect::<Vec<_>>();
        thresholds.sort_by_key(|threshold| FlowStore::layer_key(&threshold.layer));
        let mut default_leave_outs = leave_out_rows
            .records()
            .iter()
            .map(|stored| stored.leave_out.clone())
            .collect::<Vec<_>>();
        default_leave_outs
            .sort_by_key(|leave_out| FlowStore::address_record_key(&leave_out.address));
        Ok(Some(ConfigurationSnapshot {
            nexus,
            models,
            thresholds,
            default_leave_outs,
        }))
    }

    /// Replace one configuration family's committed rows with the declared
    /// rows in one atomic commit: a declared row absent from the store is
    /// asserted, a changed one mutated, a stored row the declaration leaves
    /// out retracted. A declaration naming one key twice is refused before
    /// any write; one equal to the committed rows writes nothing.
    fn replace_configuration_family<T>(
        engine: &Engine,
        table: TableReference<T>,
        declared: Vec<T>,
    ) -> Result<ConfigureOutcome, StoreError>
    where
        T: sema_engine::EngineStoredRecord + PartialEq + Clone + Send + Sync + 'static,
        <T as rkyv::Archive>::Archived: rkyv::Deserialize<T, rkyv::api::high::HighDeserializer<rkyv::rancor::Error>>
            + for<'validation> rkyv::bytecheck::CheckBytes<
                rkyv::rancor::Strategy<
                    rkyv::validation::Validator<
                        rkyv::validation::archive::ArchiveValidator<'validation>,
                        rkyv::validation::shared::SharedValidator,
                    >,
                    rkyv::rancor::Error,
                >,
            >,
    {
        let declared_keys = declared
            .iter()
            .map(EngineRecord::record_key)
            .collect::<Vec<_>>();
        if declared_keys
            .iter()
            .enumerate()
            .any(|(index, key)| declared_keys[..index].contains(key))
        {
            return Err(StoreError::DuplicateConfigurationEntry);
        }
        let committed = engine
            .match_records(QueryPlan::all(table))?
            .records()
            .to_vec();
        let mut commit = engine.begin_atomic_commit();
        let mut changed = false;
        for stored in &committed {
            if !declared_keys.contains(&stored.record_key()) {
                commit = commit.retract(table, stored.record_key());
                changed = true;
            }
        }
        for row in declared {
            match committed
                .iter()
                .find(|stored| stored.record_key() == row.record_key())
            {
                Some(stored) if stored == &row => {}
                Some(_) => {
                    commit = commit.mutate(table, row);
                    changed = true;
                }
                None => {
                    commit = commit.assert(table, row);
                    changed = true;
                }
            }
        }
        if !changed {
            return Ok(ConfigureOutcome::Unchanged);
        }
        engine.commit_atomic(commit)?;
        Ok(ConfigureOutcome::Replaced)
    }

    /// One declared configuration family replaces the committed family
    /// whole. A Nexus whose Codex endpoints share a model is refused before
    /// any write.
    fn configure_meta_change(
        engine: &Engine,
        tables: &MetaConfigurationTables,
        change: ConfigurationChange,
    ) -> Result<ConfigureOutcome, StoreError> {
        match change {
            ConfigurationChange::Nexus(nexus) => {
                if nexus
                    .stable_codex
                    .model_name_vector
                    .iter()
                    .any(|model| nexus.next_codex.model_name_vector.contains(model))
                {
                    return Err(StoreError::CodexModelsOverlap);
                }
                FlowStore::replace_configuration_family(
                    engine,
                    tables.nexus,
                    vec![StoredMetaNexusConfiguration { nexus }],
                )
            }
            ConfigurationChange::Models(models) => FlowStore::replace_configuration_family(
                engine,
                tables.models,
                models
                    .into_iter()
                    .map(|model| StoredMetaModelConfiguration { model })
                    .collect(),
            ),
            ConfigurationChange::Thresholds(thresholds) => FlowStore::replace_configuration_family(
                engine,
                tables.thresholds,
                thresholds
                    .into_iter()
                    .map(|threshold| StoredMetaThresholdConfiguration { threshold })
                    .collect(),
            ),
            ConfigurationChange::LeaveOuts(leave_outs) => FlowStore::replace_configuration_family(
                engine,
                tables.leave_outs,
                leave_outs
                    .into_iter()
                    .map(|leave_out| StoredMetaLeaveOutConfiguration { leave_out })
                    .collect(),
            ),
        }
    }

    fn recorded_flow_node(flow: FlowRecord, route: Option<FlowHerdrRouteRecord>) -> FlowNode {
        FlowNode {
            flow_id: flow.flow_id,
            session_id: flow.thread_id.unwrap_or_default(),
            harness_kind: flow.harness_kind,
            endpoint_selection: flow.endpoint_selection,
            herdr_route_selection: route
                .map(|record| HerdrRouteSelection::Available(record.route))
                .unwrap_or(HerdrRouteSelection::Unavailable),
            origin: flow.origin,
            flow_lifecycle: match flow.lifecycle {
                FlowLifecycle::Pending => SignalFlowLifecycle::Pending,
                FlowLifecycle::Active => SignalFlowLifecycle::Active,
                FlowLifecycle::Stopped => SignalFlowLifecycle::Stopped,
                FlowLifecycle::Retired => SignalFlowLifecycle::Retired,
                FlowLifecycle::Exited => SignalFlowLifecycle::Exited,
            },
        }
    }
}

impl ReadsFlowMemory for FlowStore {}

#[cfg(test)]
mod tests {
    use super::{ReadsCreatedLaunchPane, RecordsCreatedLaunchPane};
    #[test]
    fn created_pane_survives_reopen_and_a_second_failed_binding_is_orphaned() {
        let fixture = StoreFixture::new();
        let launch = fixture.composed_launch(
            "created-pane",
            "1111111111111111111111111111111111111111111111111111111111111111",
        );
        let store = fixture.store();
        store
            .reserve_launch_attempt(&launch, fixture.origin())
            .expect("launch reservation");
        assert!(
            store
                .record_native_launch_intent(NativeLaunchIntent {
                    launch_request_id: "created-pane".into(),
                    prompt_sha256: launch.first_prompt_payload.prompt_sha256.clone(),
                    harness_kind: HarnessKind::Codex,
                    model_name: launch.launch_profile.model_name.clone(),
                    effort: launch.launch_profile.effort.clone(),
                    system_prompt_sha256: launch.system_prompt.system_prompt_sha256.clone(),
                    prompt_sha256_vector: launch
                        .first_prompt_payload
                        .prompt_part_vector
                        .iter()
                        .map(|part| part.prompt_sha256.clone())
                        .collect(),
                })
                .expect("intent persists")
        );
        let pane = fixture.pane("created-pane");
        assert!(
            store
                .record_created_launch_pane(pane.clone())
                .expect("pane persists")
        );
        assert!(
            !store
                .record_created_launch_pane(pane.clone())
                .expect("pane cannot be replaced")
        );
        assert_eq!(
            store
                .request_harness_start("created-pane")
                .expect("start reservation"),
            Some(pane.clone())
        );
        assert_eq!(
            store
                .request_harness_start("created-pane")
                .expect("second start reservation"),
            None
        );
        drop(store);
        let reopened = fixture.store();
        assert_eq!(
            reopened
                .recoverable_launch_pane("created-pane")
                .expect("recoverable pane"),
            Some(pane)
        );
        assert!(
            reopened
                .orphan_launch_pane("created-pane")
                .expect("orphan persists")
        );
        assert_eq!(
            reopened
                .recoverable_launch_pane("created-pane")
                .expect("orphan is not retried"),
            None
        );
    }

    use super::{
        AppliesFlowQuery, AuthorizesFlowRestart, ConfigurationOnlyStore, ConfiguresFlowStore,
        ConfirmsStartedFlow, FlowStore, LaunchOutcome, LaunchOutcomeV2, OpensFlowStore,
        ReadsFlowRoles, ReadsFlowStore, ReadsLaunchAttempt, RecordsLaunchOutcome,
        RecordsNativeLaunchBinding, RecordsNativeLaunchIntent, RecordsPendingThread,
        RecordsPromptDeliveryIntent, RecordsPromptDeliveryResult,
        RecordsRegistrationAcknowledgement, RecordsRestartedFlow, RegistersExistingFlow,
        RegistersFlowIdentity, ReservesLaunchAttempt, ReservesPendingStart, StoredLaunchOutcomeV2,
    };
    use super::{Configuration, ConfiguresOnlyStore, ReadsFlowMemory};
    use super::{FlowRecordV5, LaunchedRecordV2, OriginClueBeforeOrigin, ReplacedRecordV2};
    use super::{LaunchOutcomeV3, ReplaceRejectionV17, StartRejectionV17, StoredLaunchOutcomeV3};
    use super::{LaunchOutcomeV4, ReplaceRejectionV18, StartRejectionV18, StoredLaunchOutcomeV4};
    use super::{LaunchedRecord, ReplacedRecord};
    use super::{LaysOutDefaults, SeedsConfiguration};
    use crate::store::RegistersBindings;
    use crate::store::delivery::RecordsDeliveries;
    use crate::store::delivery::{LeaseStep, PaneLease};
    use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
    use sema_engine::{
        Assertion, Engine, EngineOpen, EngineRecord, FamilyName, QueryPlan, SchemaHash,
        SchemaVersion, TableDescriptor, TableName,
    };
    use signal_flow::{
        Aspect, BindingRefusal, ComposedLaunch, FirstPromptPayload, FlowOrigin, HarnessKind,
        HerdrPaneBinding, LaunchAttemptPhase, LaunchAttemptReservation, LaunchProfile,
        NativeLaunchBinding, NativeLaunchIntent, NativeTargetReceipt, NativeTranscriptAbsence,
        NativeTranscriptBoundary, NativeTranscriptCursor, Origin, PowerLevel, PromptDeliveryIntent,
        PromptDeliveryResult, Query, RegistrationAcknowledgement, ReplaceRejection, Response,
        Restarted, StartRejection, StopRejection, TargetReceiptRequest,
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

        fn path(&self) -> std::path::PathBuf {
            self.directory.path().join("flow.sema")
        }

        /// Outcome rows of the 0.34 shape, under the family label given.
        fn seed_outcomes(
            &self,
            label: &str,
            rows: impl IntoIterator<Item = StoredLaunchOutcomeV2>,
        ) {
            let mut engine = Engine::open(EngineOpen::new(self.path(), SchemaVersion::new(1)))
                .expect("outcome store opens");
            let outcomes = engine
                .register_table(TableDescriptor::new(
                    TableName::new("flow_nexus_launch_outcomes"),
                    FamilyName::new("flow-nexus-launch-outcome"),
                    SchemaHash::for_label(label),
                ))
                .expect("outcome family registers");
            for row in rows {
                engine
                    .assert(Assertion::new(outcomes, row))
                    .expect("outcome persists");
            }
        }

        fn launch_profile(&self, launch_request_id: &str) -> LaunchProfile {
            LaunchProfile {
                launch_request_id: launch_request_id.into(),
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
            }
        }

        fn composed_launch(&self, launch_request_id: &str, prompt_sha256: &str) -> ComposedLaunch {
            ComposedLaunch {
                harness_kind: HarnessKind::Codex,
                launch_profile: self.launch_profile(launch_request_id),
                system_prompt: signal_flow::SystemPrompt {
                    system_prompt_text: "fixture system prompt".into(),
                    system_prompt_sha256: "9".repeat(64),
                },
                first_prompt_payload: FirstPromptPayload {
                    first_prompt_body: "fixture prompt".into(),
                    prompt_sha256: prompt_sha256.into(),
                    prompt_part_vector: vec![signal_flow::PromptPart {
                        prompt_part_text: "fixture prompt".into(),
                        prompt_sha256: prompt_sha256.into(),
                    }],
                },
                target_receipt_request: TargetReceiptRequest {
                    launch_request_id: launch_request_id.into(),
                    prompt_sha256: prompt_sha256.into(),
                },
                leave_outs: Vec::new(),
            }
        }

        fn origin(&self) -> Origin {
            Origin::Flow(FlowOrigin {
                flow_id: "9fc62b".into(),
                session_id: "caller-session".into(),
            })
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
            system_prompt_sha256: launch.system_prompt.system_prompt_sha256.clone(),
            prompt_sha256_vector: launch
                .first_prompt_payload
                .prompt_part_vector
                .iter()
                .map(|part| part.prompt_sha256.clone())
                .collect(),
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
        launch.harness_kind = HarnessKind::Claude;
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
                    system_prompt_sha256: launch.system_prompt.system_prompt_sha256.clone(),
                    prompt_sha256_vector: launch
                        .first_prompt_payload
                        .prompt_part_vector
                        .iter()
                        .map(|part| part.prompt_sha256.clone())
                        .collect(),
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
            prompt_sha256: launch.first_prompt_payload.prompt_sha256.clone(),
            flow_id: "native-flow".into(),
            native_session_id: "native-session".into(),
            harness_kind: HarnessKind::Claude,
            model_name: launch.launch_profile.model_name.clone(),
            effort: launch.launch_profile.effort.clone(),
            prompt_sha256_vector: vec![
                "7777777777777777777777777777777777777777777777777777777777777777".into(),
            ],
            parts_submitted: 1,
            stops_reported: 0,
            leave_outs: Vec::new(),
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
                .expect("part hashes other than the native intent's are refused")
        );
        let mut exact = intent;
        exact.prompt_sha256_vector = vec![launch.first_prompt_payload.prompt_sha256.clone()];
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
                        prompt_sha256_vector: cursor_intent.prompt_sha256_vector.clone(),
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
                        prompt_sha256_vector: cursor_intent.prompt_sha256_vector,
                    }
                ))
                .expect("authentic receipt promotes ambiguity")
        );
    }

    /// A first message sent in parts: each next part is recorded submitted
    /// before it is typed, one at a time, never past the last, and the
    /// receipt is taken only once every part was submitted.
    #[test]
    fn parts_advance_one_at_a_time_and_the_receipt_waits_for_the_last() {
        let fixture = StoreFixture::new();
        let store = fixture.store();
        let mut launch = fixture.composed_launch(
            "request-parts",
            "8888888888888888888888888888888888888888888888888888888888888888",
        );
        launch.first_prompt_payload.prompt_part_vector = vec![
            signal_flow::PromptPart {
                prompt_part_text: "part one".into(),
                prompt_sha256: "a".repeat(64),
            },
            signal_flow::PromptPart {
                prompt_part_text: "part two".into(),
                prompt_sha256: "b".repeat(64),
            },
        ];
        store
            .reserve_launch_attempt(&launch, fixture.origin())
            .expect("reservation persists");
        assert!(
            store
                .record_native_launch_intent(NativeLaunchIntent {
                    launch_request_id: "request-parts".into(),
                    prompt_sha256: launch.first_prompt_payload.prompt_sha256.clone(),
                    system_prompt_sha256: launch.system_prompt.system_prompt_sha256.clone(),
                    harness_kind: HarnessKind::Codex,
                    model_name: launch.launch_profile.model_name.clone(),
                    effort: launch.launch_profile.effort.clone(),
                    prompt_sha256_vector: vec!["a".repeat(64), "b".repeat(64)],
                })
                .unwrap()
        );
        let pane = fixture.pane("request-parts");
        assert!(
            store
                .record_native_launch_binding(NativeLaunchBinding {
                    launch_request_id: "request-parts".into(),
                    flow_id: "parts-flow".into(),
                    native_session_id: "parts-session".into(),
                    harness_kind: HarnessKind::Codex,
                    herdr_pane_binding: pane.clone(),
                })
                .unwrap()
        );
        assert!(
            store
                .record_registration_acknowledgement(RegistrationAcknowledgement {
                    launch_request_id: "request-parts".into(),
                    flow_id: "parts-flow".into(),
                    native_session_id: "parts-session".into(),
                    herdr_pane_binding: pane.clone(),
                })
                .unwrap()
        );
        let first = PromptDeliveryIntent {
            launch_request_id: "request-parts".into(),
            prompt_sha256: launch.first_prompt_payload.prompt_sha256.clone(),
            flow_id: "parts-flow".into(),
            native_session_id: "parts-session".into(),
            harness_kind: HarnessKind::Codex,
            model_name: launch.launch_profile.model_name.clone(),
            effort: launch.launch_profile.effort.clone(),
            prompt_sha256_vector: vec!["a".repeat(64), "b".repeat(64)],
            parts_submitted: 1,
            stops_reported: 2,
            leave_outs: Vec::new(),
            herdr_pane_binding: pane,
            native_transcript_boundary: NativeTranscriptBoundary::Absent(NativeTranscriptAbsence {
                native_session_id: "parts-session".into(),
                harness_kind: HarnessKind::Codex,
                transcript_root_device: "2049".into(),
                transcript_root_inode: "99150".into(),
            }),
        };
        assert!(store.record_prompt_delivery_intent(first.clone()).unwrap());
        assert!(
            store
                .record_prompt_delivery_result(PromptDeliveryResult::Ambiguous(first.clone()))
                .unwrap()
        );
        let receipt = |intent: &PromptDeliveryIntent| NativeTargetReceipt {
            launch_request_id: intent.launch_request_id.clone(),
            prompt_sha256: intent.prompt_sha256.clone(),
            flow_id: intent.flow_id.clone(),
            native_session_id: intent.native_session_id.clone(),
            native_turn_id: "parts-turn".into(),
            receipt_sha256: "c".repeat(64),
            model_name: intent.model_name.clone(),
            effort: intent.effort.clone(),
            prompt_sha256_vector: intent.prompt_sha256_vector.clone(),
        };
        assert!(
            !store
                .record_prompt_delivery_result(PromptDeliveryResult::Observed(receipt(&first)))
                .unwrap(),
            "no receipt before the last part is submitted"
        );
        let mut skipped = first.clone();
        skipped.parts_submitted = 3;
        assert!(
            !store
                .record_prompt_delivery_result(PromptDeliveryResult::Ambiguous(skipped))
                .unwrap(),
            "no part past the last"
        );
        let mut fewer_stops = first.clone();
        fewer_stops.parts_submitted = 2;
        fewer_stops.stops_reported = 1;
        assert!(
            !store
                .record_prompt_delivery_result(PromptDeliveryResult::Ambiguous(fewer_stops))
                .unwrap(),
            "the Stop count never goes back"
        );
        let mut second = first.clone();
        second.parts_submitted = 2;
        second.stops_reported = 3;
        assert!(
            store
                .record_prompt_delivery_result(PromptDeliveryResult::Ambiguous(second.clone()))
                .unwrap()
        );
        assert_eq!(
            store
                .launch_attempt("request-parts")
                .unwrap()
                .unwrap()
                .prompt_delivery_intent_option,
            Some(second.clone())
        );
        assert!(
            store
                .record_prompt_delivery_result(PromptDeliveryResult::Observed(receipt(&second)))
                .unwrap()
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
                .reserve_pending_start(Query::Start(self.launch_profile("legacy-start-1")))
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
            .reserve_pending_start(Query::Start(fixture.launch_profile("legacy-start-2")))
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
            .reserve_pending_start(Query::Start(fixture.launch_profile("legacy-start-3")))
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
    fn new_store_seeds_defaults_from_home_and_runtime_directory() {
        let fixture = StoreFixture::new();
        let defaults = super::DefaultConfiguration {
            home: std::path::PathBuf::from("/home/someone"),
            runtime_directory: std::path::PathBuf::from("/run/user/4242"),
        };
        let path = fixture.directory.path().join("seeded.sema");
        let store = FlowStore::open_seeded(&path, &defaults).expect("store opens");
        let configuration = store.configuration().expect("configuration");
        assert_eq!(configuration, defaults.configuration());
        assert_eq!(
            configuration.ordinary_socket_path,
            "/run/user/4242/flow/flow.sock"
        );
        assert_eq!(
            configuration.meta_socket_path,
            "/run/user/4242/flow/flow-meta.sock"
        );
        assert_eq!(configuration.source_root, "/home/someone/primary");
        assert_eq!(
            configuration.stable_codex.control_socket_path,
            "/home/someone/.codex/app-server-control/app-server-control.sock"
        );
        let runtime = store
            .runtime_configuration()
            .expect("runtime configuration");
        assert_eq!(runtime.source_root, "/home/someone/primary");
        assert_eq!(
            runtime.stable_codex.socket,
            "/home/someone/.codex/app-server-control/app-server-control.sock"
        );
        assert_eq!(runtime.next_codex.home, "/home/someone/.codex-next");
        assert_eq!(
            defaults.store_path(),
            std::path::Path::new("/home/someone/.local/state/flow/flow.sema")
        );
        drop(store);

        let other = super::DefaultConfiguration {
            home: std::path::PathBuf::from("/elsewhere"),
            runtime_directory: std::path::PathBuf::from("/run/elsewhere"),
        };
        let resumed = FlowStore::open_seeded(&path, &other).expect("store reopens");
        assert_eq!(
            resumed
                .runtime_configuration()
                .expect("resumed")
                .source_root,
            "/home/someone/primary",
            "a populated store resumes what it holds"
        );
    }

    #[test]
    fn configuration_only_store_defaults_without_seeding_then_commits_all_configuration() {
        let fixture = StoreFixture::new();
        let defaults = super::DefaultConfiguration {
            home: std::path::PathBuf::from("/home/configuration-only"),
            runtime_directory: std::path::PathBuf::from("/run/user/4243"),
        };
        let path = fixture.directory.path().join("configuration-only.sema");
        let store = ConfigurationOnlyStore::open(&path).expect("configuration store opens");
        assert_eq!(
            store
                .engine
                .match_records(QueryPlan::all(store.configuration))
                .expect("socket records read")
                .records()
                .len(),
            0,
            "reading fresh defaults writes no socket configuration"
        );
        assert_eq!(
            store.configuration(&defaults).expect("fresh defaults"),
            defaults.configuration()
        );
        assert_eq!(
            store
                .engine
                .match_records(QueryPlan::all(store.runtime_configuration))
                .expect("runtime records read")
                .records()
                .len(),
            0,
            "reading fresh defaults writes no runtime configuration"
        );
        assert_eq!(
            store
                .engine
                .match_records(QueryPlan::all(store.delivery_configuration))
                .expect("delivery records read")
                .records()
                .len(),
            0,
            "reading fresh defaults writes no delivery configuration"
        );

        let mut configured = defaults.configuration();
        configured.meta_socket_path = "/run/user/4243/flow/custom-meta.sock".into();
        let persisted = store
            .configure(configured.clone())
            .expect("all configuration commits");
        assert_eq!(persisted, configured);
        drop(store);
        assert!(FlowStore::recognize_flow_memory(&path).expect("new Memory recognizes"));

        let reopened = ConfigurationOnlyStore::open(&path).expect("configured store reopens");
        assert_eq!(
            reopened
                .configuration(&defaults)
                .expect("persisted configuration"),
            configured,
            "the next listener reads the committed endpoint instead of defaults"
        );
    }

    #[test]
    fn configuration_only_store_refuses_partial_state_without_filling_it() {
        let fixture = StoreFixture::new();
        let defaults = super::DefaultConfiguration {
            home: std::path::PathBuf::from("/home/configuration-only"),
            runtime_directory: std::path::PathBuf::from("/run/user/4244"),
        };
        let path = fixture.directory.path().join("partial-configuration.sema");
        let store = ConfigurationOnlyStore::open(&path).expect("configuration store opens");
        let initial = defaults.configuration();
        store
            .engine
            .assert(Assertion::new(
                store.configuration,
                super::FlowStoreConfiguration::from(&initial),
            ))
            .expect("one partial row is present");

        assert!(matches!(
            store.configure(defaults.configuration()),
            Err(super::StoreError::StateInvariant)
        ));
        assert_eq!(
            store
                .engine
                .match_records(QueryPlan::all(store.configuration))
                .expect("socket records read")
                .records()
                .len(),
            1
        );
        assert_eq!(
            store
                .engine
                .match_records(QueryPlan::all(store.runtime_configuration))
                .expect("runtime records read")
                .records()
                .len(),
            0,
            "a failed configuration does not partially seed the missing rows"
        );
        assert_eq!(
            store
                .engine
                .match_records(QueryPlan::all(store.delivery_configuration))
                .expect("delivery records read")
                .records()
                .len(),
            0
        );
    }

    #[test]
    fn configured_opener_and_configuration_listener_leave_a_delivery_lease_untouched() {
        let fixture = StoreFixture::new();
        let path = fixture.path();
        let defaults = super::DefaultConfiguration {
            home: fixture.directory.path().join("home"),
            runtime_directory: fixture.directory.path().join("runtime"),
        };
        let config = ConfigurationOnlyStore::open(&path).expect("fresh Memory opens");
        let settings = defaults.configuration();
        config
            .configure_change(meta_signal_flow::ConfigurationChange::Nexus(
                meta_signal_flow::ConfigureNexus {
                    source_root: settings.source_root,
                    stable_codex: settings.stable_codex,
                    next_codex: settings.next_codex,
                    harness_profiles: settings.harness_profile_vector,
                    meta_aspects: settings.meta_aspects,
                    message_nexus_path: settings.message_nexus_path,
                    message_nexus_binary: "/fixture/message-nexus".into(),
                    lease: 60,
                },
            ))
            .expect("configuration commits");
        drop(config);

        let store = FlowStore::open_configured(&path).expect("configured Memory opens");
        assert!(store.opening_settlements.is_empty());
        store
            .record_lease_step(PaneLease {
                delivery_id: "interrupted-delivery".into(),
                flow_id: "flow-1".into(),
                herdr_pane_id: "pane-1".into(),
                lease_step: LeaseStep::Placed,
            })
            .expect("interrupted delivery lease is present");
        drop(store);

        let config = ConfigurationOnlyStore::open(&path)
            .expect("configuration-only listener reopens recognized Memory");
        assert!(
            config
                .configuration_snapshot()
                .expect("snapshot reads")
                .is_some()
        );
        drop(config);

        let reopened = FlowStore::open_configured(&path).expect("configured Memory resumes");
        assert!(reopened.opening_settlements.is_empty());
        let leases = reopened
            .engine
            .match_records(QueryPlan::all(reopened.delivery_tables.pane_leases))
            .expect("pane lease reads")
            .records()
            .to_vec();
        assert_eq!(leases.len(), 1, "either open did not settle the lease");
        assert_eq!(leases[0].delivery_id, "interrupted-delivery");
    }

    /// A running store written before context sizes were kept (0.27.0)
    /// is recognized, and its next full open adds the table.
    #[test]
    fn a_running_store_from_before_context_sizes_opens_and_gains_the_table() {
        let fixture = StoreFixture::new();
        let path = fixture.path();
        let defaults = super::DefaultConfiguration {
            home: fixture.directory.path().join("home"),
            runtime_directory: fixture.directory.path().join("runtime"),
        };
        let config = ConfigurationOnlyStore::open(&path).expect("fresh Memory opens");
        let settings = defaults.configuration();
        config
            .configure_change(meta_signal_flow::ConfigurationChange::Nexus(
                meta_signal_flow::ConfigureNexus {
                    source_root: settings.source_root,
                    stable_codex: settings.stable_codex,
                    next_codex: settings.next_codex,
                    harness_profiles: settings.harness_profile_vector,
                    meta_aspects: settings.meta_aspects,
                    message_nexus_path: settings.message_nexus_path,
                    message_nexus_binary: "/fixture/message-nexus".into(),
                    lease: 60,
                },
            ))
            .expect("configuration commits");
        drop(config);
        drop(FlowStore::open_configured(&path).expect("configured Memory opens"));

        // The catalog as 0.27.0 wrote it: no context size family.
        let database = redb::Database::open(&path).expect("store opens directly");
        let write = database.begin_write().expect("catalog transaction starts");
        {
            let mut catalog = write
                .open_table(super::SEMA_CATALOG_TABLE)
                .expect("catalog opens");
            assert!(
                catalog
                    .remove("flow_nexus_flow_context_sizes")
                    .expect("catalog entry removes")
                    .is_some()
            );
        }
        write.commit().expect("catalog commits");
        drop(database);

        let store = FlowStore::open_configured(&path).expect("0.27.0 running Memory opens");
        use super::context::RecordsContextSize;
        store
            .record_context_size("5a4d0b", 90_000)
            .expect("the added table takes a row");
        drop(store);
        let reopened = FlowStore::open_configured(&path).expect("upgraded Memory reopens");
        assert_eq!(reopened.context_size("5a4d0b").unwrap(), Some(90_000));
    }

    /// A row of a table 0.35.0 retired: the meta topic modules and the
    /// 0.34.6 start bundles. Its shape is never read; it only fills a table.
    #[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
    struct RetiredRow {
        key: String,
        bytes: Vec<u8>,
    }

    impl EngineRecord for RetiredRow {
        fn record_key(&self) -> sema_engine::RecordKey {
            sema_engine::RecordKey::new(self.key.clone())
        }
    }

    /// A store from 0.34.6 holds the meta topic modules and the start
    /// bundles with rows in them; its next open drops both tables, rows and
    /// registrations, and the store keeps opening without them.
    #[test]
    fn a_store_from_0_34_6_drops_its_retired_tables_when_it_opens() {
        let fixture = StoreFixture::new();
        let path = fixture.path();
        let defaults = super::DefaultConfiguration {
            home: fixture.directory.path().join("home"),
            runtime_directory: fixture.directory.path().join("runtime"),
        };
        let config = ConfigurationOnlyStore::open(&path).expect("fresh Memory opens");
        let settings = defaults.configuration();
        config
            .configure_change(meta_signal_flow::ConfigurationChange::Nexus(
                meta_signal_flow::ConfigureNexus {
                    source_root: settings.source_root,
                    stable_codex: settings.stable_codex,
                    next_codex: settings.next_codex,
                    harness_profiles: settings.harness_profile_vector,
                    meta_aspects: settings.meta_aspects,
                    message_nexus_path: settings.message_nexus_path,
                    message_nexus_binary: "/fixture/message-nexus".into(),
                    lease: 60,
                },
            ))
            .expect("configuration commits");
        drop(config);
        drop(FlowStore::open_configured(&path).expect("configured Memory opens"));

        // The tables as 0.34.6 kept them, each holding a row.
        {
            let mut engine = Engine::open(EngineOpen::new(&path, SchemaVersion::new(2)))
                .expect("store opens under the engine");
            for (table, family, schema) in super::RETIRED_TABLES {
                let retired = engine
                    .register_table(TableDescriptor::<RetiredRow>::new(
                        TableName::new(table),
                        FamilyName::new(family.to_string()),
                        SchemaHash::for_label(schema),
                    ))
                    .expect("retired family registers");
                engine
                    .assert(Assertion::new(
                        retired,
                        RetiredRow {
                            key: "Field:flow:Secondary".into(),
                            bytes: b"kept bytes".to_vec(),
                        },
                    ))
                    .expect("retired row persists");
            }
        }
        let tables = || {
            use redb::{ReadableDatabase as _, ReadableTable as _, TableHandle as _};
            let database = redb::ReadOnlyDatabase::open(&path).expect("store reads directly");
            let read = database.begin_read().expect("read starts");
            let registered = read
                .open_table(super::SEMA_CATALOG_TABLE)
                .expect("catalog opens")
                .iter()
                .expect("catalog iterates")
                .map(|entry| entry.expect("catalog entry").0.value().to_owned())
                .collect::<Vec<_>>();
            let stored = read
                .list_tables()
                .expect("tables list")
                .map(|table| table.name().to_owned())
                .collect::<Vec<_>>();
            (registered, stored)
        };
        let (registered, stored) = tables();
        for (table, _, _) in super::RETIRED_TABLES {
            assert!(
                registered.iter().any(|name| name == *table),
                "{table} seeded"
            );
            assert!(
                stored.iter().any(|name| name == *table),
                "{table} rows seeded"
            );
        }

        drop(FlowStore::open_configured(&path).expect("the 0.34.6 store opens"));
        let (registered, stored) = tables();
        for (table, _, _) in super::RETIRED_TABLES {
            assert!(
                !registered.iter().any(|name| name == *table),
                "{table} unregistered"
            );
            assert!(
                !stored.iter().any(|name| name == *table),
                "{table} rows dropped"
            );
        }
        drop(FlowStore::open_configured(&path).expect("the store reopens without them"));
        drop(ConfigurationOnlyStore::open(&path).expect("configuration reopens without them"));
    }

    /// A running store written before Address holds were kept (0.31.0)
    /// is recognized, and its next full open adds the table.
    #[test]
    fn a_running_store_from_before_address_holds_opens_and_gains_the_table() {
        let fixture = StoreFixture::new();
        let path = fixture.path();
        let defaults = super::DefaultConfiguration {
            home: fixture.directory.path().join("home"),
            runtime_directory: fixture.directory.path().join("runtime"),
        };
        let config = ConfigurationOnlyStore::open(&path).expect("fresh Memory opens");
        let settings = defaults.configuration();
        config
            .configure_change(meta_signal_flow::ConfigurationChange::Nexus(
                meta_signal_flow::ConfigureNexus {
                    source_root: settings.source_root,
                    stable_codex: settings.stable_codex,
                    next_codex: settings.next_codex,
                    harness_profiles: settings.harness_profile_vector,
                    meta_aspects: settings.meta_aspects,
                    message_nexus_path: settings.message_nexus_path,
                    message_nexus_binary: "/fixture/message-nexus".into(),
                    lease: 60,
                },
            ))
            .expect("configuration commits");
        drop(config);
        drop(FlowStore::open_configured(&path).expect("configured Memory opens"));

        // The catalog as 0.31.0 wrote it: no Address hold family.
        let database = redb::Database::open(&path).expect("store opens directly");
        let write = database.begin_write().expect("catalog transaction starts");
        {
            let mut catalog = write
                .open_table(super::SEMA_CATALOG_TABLE)
                .expect("catalog opens");
            assert!(
                catalog
                    .remove("flow_nexus_address_holds")
                    .expect("catalog entry removes")
                    .is_some()
            );
        }
        write.commit().expect("catalog commits");
        drop(database);

        let store = FlowStore::open_configured(&path).expect("0.31.0 running Memory opens");
        use super::address_hold::HoldsLaunchAddress;
        assert_eq!(store.address_hold("908786").unwrap(), None);
        drop(store);
        let reopened = FlowStore::open_configured(&path).expect("upgraded Memory reopens");
        assert_eq!(reopened.address_hold("908786").unwrap(), None);
    }

    /// Sets redb's recovery-required flag, as a writer killed before it
    /// closed leaves it.
    fn flag_recovery_required(path: &std::path::Path) {
        let mut bytes = std::fs::read(path).expect("store bytes read");
        bytes[9] |= 2;
        std::fs::write(path, bytes).expect("flagged store written");
        assert!(matches!(
            redb::ReadOnlyDatabase::open(path),
            Err(redb::DatabaseError::RepairAborted)
        ));
    }

    fn directory_entries(path: &std::path::Path) -> Vec<std::ffi::OsString> {
        let mut names = std::fs::read_dir(path.parent().expect("store directory"))
            .expect("store directory lists")
            .map(|entry| entry.expect("directory entry").file_name())
            .collect::<Vec<_>>();
        names.sort();
        names
    }

    #[test]
    fn a_flagged_flow_memory_is_recognized_through_a_repaired_copy_and_then_opens() {
        let fixture = StoreFixture::new();
        let path = fixture.path();
        drop(ConfigurationOnlyStore::open(&path).expect("fresh Flow Memory opens"));
        flag_recovery_required(&path);
        let before = std::fs::read(&path).expect("flagged bytes read");
        let entries = directory_entries(&path);

        assert!(FlowStore::recognize_flow_memory(&path).expect("flagged Memory recognizes"));
        assert_eq!(
            std::fs::read(&path).expect("flagged bytes reread"),
            before,
            "recognition never writes the original"
        );
        assert_eq!(directory_entries(&path), entries, "the copy is discarded");

        drop(ConfigurationOnlyStore::open(&path).expect("flagged Memory opens and repairs"));
        assert_eq!(std::fs::read(&path).expect("repaired bytes")[9] & 2, 0);
        assert!(FlowStore::recognize_flow_memory(&path).expect("repaired Memory recognizes"));
    }

    #[test]
    fn a_flagged_foreign_store_is_refused_without_changing_it() {
        let fixture = StoreFixture::new();
        let path = fixture.path();
        let mut engine = Engine::open(EngineOpen::new(
            &path,
            SchemaVersion::new(super::FLOW_SCHEMA_VERSION),
        ))
        .expect("foreign schema-v2 store opens");
        engine
            .register_table(TableDescriptor::<super::FlowRecord>::new(
                super::FLOW_TABLE_NAME,
                FamilyName::new("flow-nexus-flow"),
                SchemaHash::for_label("flow-nexus-flow-v5"),
            ))
            .expect("foreign occupant has one unrelated Flow family");
        drop(engine);
        flag_recovery_required(&path);
        let before = std::fs::read(&path).expect("flagged foreign bytes read");
        let entries = directory_entries(&path);

        assert!(matches!(
            ConfigurationOnlyStore::open(&path),
            Err(super::StoreError::UnrecognizedMemory { .. })
        ));
        assert!(matches!(
            FlowStore::open_configured(&path),
            Err(super::StoreError::UnrecognizedMemory { .. })
        ));
        assert_eq!(std::fs::read(&path).expect("foreign bytes reread"), before);
        assert_eq!(directory_entries(&path), entries, "the copy is discarded");
    }

    #[test]
    fn configuration_only_refuses_old_memory_without_changing_any_bytes() {
        let fixture = StoreFixture::new();
        let path = fixture.path();
        // A family label no Flow wrote since 0.20 is not recognized.
        fixture.seed_outcomes(
            "flow-nexus-launch-outcome-v1",
            [StoredLaunchOutcomeV2 {
                launch_request_id: "old-row".into(),
                launch_outcome: LaunchOutcomeV2::StartRejected(
                    StartRejectionV17::CompositionRefused,
                ),
            }],
        );
        let before = std::fs::read(&path).expect("old store bytes read");

        assert!(matches!(
            ConfigurationOnlyStore::open(&path),
            Err(super::StoreError::UnrecognizedMemory { .. })
        ));
        assert_eq!(std::fs::read(&path).expect("old bytes reread"), before);
    }

    #[test]
    fn configuration_only_refuses_corrupt_bytes_without_changing_them() {
        let fixture = StoreFixture::new();
        let path = fixture.path();
        let malformed = b"not a redb Flow Memory";
        std::fs::write(&path, malformed).expect("malformed occupant written");

        assert!(matches!(
            ConfigurationOnlyStore::open(&path),
            Err(super::StoreError::UnrecognizedMemory { .. })
        ));
        assert_eq!(
            std::fs::read(&path).expect("malformed bytes reread"),
            malformed
        );
    }

    #[test]
    fn configuration_only_refuses_a_foreign_schema_v2_store_without_changing_it() {
        let fixture = StoreFixture::new();
        let path = fixture.path();
        let mut engine = Engine::open(EngineOpen::new(
            &path,
            SchemaVersion::new(super::FLOW_SCHEMA_VERSION),
        ))
        .expect("foreign schema-v2 store opens");
        engine
            .register_table(TableDescriptor::<super::FlowRecord>::new(
                super::FLOW_TABLE_NAME,
                FamilyName::new("flow-nexus-flow"),
                SchemaHash::for_label("flow-nexus-flow-v5"),
            ))
            .expect("foreign occupant has one unrelated Flow family");
        drop(engine);
        let before = std::fs::read(&path).expect("foreign store bytes read");

        assert!(matches!(
            ConfigurationOnlyStore::open(&path),
            Err(super::StoreError::UnrecognizedMemory { .. })
        ));
        assert_eq!(std::fs::read(&path).expect("foreign bytes reread"), before);
    }

    #[test]
    fn configuration_only_refuses_an_unrecognized_family_before_engine_open() {
        let fixture = StoreFixture::new();
        let path = fixture.path();
        drop(ConfigurationOnlyStore::open(&path).expect("fresh Flow Memory opens"));

        let mut engine = Engine::open(EngineOpen::new(
            &path,
            SchemaVersion::new(super::FLOW_SCHEMA_VERSION),
        ))
        .expect("recognized Flow Memory reopens for fixture setup");
        engine
            .register_table(TableDescriptor::<super::FlowRecord>::new(
                TableName::new("unrecognized_flows"),
                FamilyName::new("unrecognized-flow"),
                SchemaHash::for_label("unrecognized-flow-v1"),
            ))
            .expect("foreign family is planted");
        drop(engine);
        let before = std::fs::read(&path).expect("foreign family bytes read");

        assert!(matches!(
            ConfigurationOnlyStore::open(&path),
            Err(super::StoreError::UnrecognizedMemory { .. })
        ));
        assert_eq!(
            std::fs::read(&path).expect("foreign family bytes reread"),
            before,
            "unrecognized catalog entries are rejected before Engine stamps the file"
        );
    }

    #[test]
    fn configuration_only_refuses_a_different_engine_layout_without_changing_it() {
        let fixture = StoreFixture::new();
        let path = fixture.path();
        drop(ConfigurationOnlyStore::open(&path).expect("fresh Flow Memory opens"));

        let database = redb::Database::open(&path).expect("recognized fixture opens directly");
        let write = database
            .begin_write()
            .expect("layout fixture transaction starts");
        {
            let mut counters = write
                .open_table(super::SEMA_ENGINE_COUNTERS_TABLE)
                .expect("engine counters open");
            let foreign_layout =
                rkyv::to_bytes::<rkyv::rancor::Error>(&(super::FLOW_ENGINE_STORAGE_LAYOUT + 1))
                    .expect("foreign layout encodes with the engine counter representation");
            counters
                .insert(
                    super::SEMA_ENGINE_STORAGE_LAYOUT_KEY,
                    foreign_layout.as_slice(),
                )
                .expect("foreign layout is planted");
        }
        write.commit().expect("foreign layout commits");
        drop(database);
        let before = std::fs::read(&path).expect("foreign layout bytes read");

        assert!(matches!(
            ConfigurationOnlyStore::open(&path),
            Err(super::StoreError::UnrecognizedMemory { .. })
        ));
        assert_eq!(
            std::fs::read(&path).expect("foreign layout bytes reread"),
            before,
            "a different Sema Engine layout is rejected without repair or restamping"
        );
    }

    #[test]
    fn configured_policy_is_recovered_from_the_same_store() {
        let fixture = StoreFixture::new();
        let store = fixture.store();
        let endpoint = |name: &str| meta_signal_flow::CodexEndpoint {
            client_path: format!("/opt/{name}-client"),
            home: format!("/srv/{name}"),
            control_socket_path: format!("/srv/{name}/control.sock"),
            model_name_vector: vec![format!("{name}-model")],
        };
        let configuration = Configuration {
            ordinary_socket_path: "/tmp/ordinary-test.sock".into(),
            meta_socket_path: "/tmp/meta-test.sock".into(),
            source_root: "/srv/source".into(),
            stable_codex: endpoint("stable"),
            next_codex: endpoint("next"),
            harness_profile_vector: vec![meta_signal_flow::HarnessProfile {
                harness_kind: signal_flow::HarnessKind::Codex,
                command_sigil_vector: vec!["/".into()],
                interrupt_keys: vec!["esc".into()],
                submit_keys: Vec::new(),
            }],
            meta_aspects: vec![signal_flow::Aspect::Psyche, signal_flow::Aspect::Mind],
            message_nexus_path: "/opt/message-nexus".into(),
        };
        store
            .configure(configuration.clone())
            .expect("policy persists");
        drop(store);
        let store = fixture.store();
        assert_eq!(
            store.configuration().expect("policy recovers"),
            configuration
        );
        let runtime = store.runtime_configuration().expect("runtime recovers");
        assert_eq!(runtime.source_root, "/srv/source");
        assert_eq!(runtime.next_codex.socket, "/srv/next/control.sock");
        assert_eq!(runtime.stable_codex.model_names, vec!["stable-model"]);
    }

    #[test]
    fn socket_record_keeps_the_archive_of_the_former_two_field_configuration() {
        #[derive(rkyv::Archive, rkyv::Serialize)]
        struct FormerConfiguration {
            ordinary_socket_path: String,
            meta_socket_path: String,
        }
        #[derive(rkyv::Archive, rkyv::Serialize)]
        struct FormerRecord {
            configuration: FormerConfiguration,
        }
        let former = rkyv::to_bytes::<rkyv::rancor::Error>(&FormerRecord {
            configuration: FormerConfiguration {
                ordinary_socket_path: "/run/user/1001/flow/flow.sock".into(),
                meta_socket_path: "/run/user/1001/flow/flow-meta.sock".into(),
            },
        })
        .expect("former archive");
        let current = rkyv::to_bytes::<rkyv::rancor::Error>(&super::FlowStoreConfiguration {
            ordinary_socket_path: "/run/user/1001/flow/flow.sock".into(),
            meta_socket_path: "/run/user/1001/flow/flow-meta.sock".into(),
        })
        .expect("current archive");
        assert_eq!(former.as_slice(), current.as_slice());
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
            origin: signal_flow::Origin::Flow(signal_flow::FlowOrigin {
                flow_id: "da1e3f".into(),
                session_id: "claude-session".into(),
            }),
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

    fn pane_node(flow_id: &str, pane: &str) -> signal_flow::FlowNode {
        signal_flow::FlowNode {
            flow_id: flow_id.into(),
            session_id: format!("{flow_id}-session"),
            harness_kind: signal_flow::HarnessKind::Claude,
            endpoint_selection: signal_flow::EndpointSelection::Unavailable,
            herdr_route_selection: signal_flow::HerdrRouteSelection::Available(
                signal_flow::HerdrRoute {
                    herdr_session_name: "messaging-build".into(),
                    herdr_agent_name: format!("agent-{pane}"),
                    herdr_pane_id: pane.into(),
                    herdr_terminal_id: format!("terminal-{pane}"),
                },
            ),
            origin: signal_flow::Origin::Flow(signal_flow::FlowOrigin {
                flow_id: "field-owner".into(),
                session_id: "messaging-build".into(),
            }),
            flow_lifecycle: signal_flow::FlowLifecycle::Pending,
        }
    }

    #[test]
    fn a_store_written_before_roles_adopts_them_when_it_reopens() {
        let fixture = StoreFixture::new();
        let store = fixture.store();
        // As MetaBindExisting wrote it before the role had its own record.
        store
            .register_flow_as(
                pane_node("bound-old", "w1:p1"),
                "Mind:Medium:gpt-sol".into(),
                None,
            )
            .expect("legacy bound flow");
        store
            .register_flow(pane_node("registered-old", "w1:p2"))
            .expect("flow registered without a role");
        assert_eq!(store.role("bound-old").unwrap(), None);
        drop(store);

        let store = fixture.store();
        assert_eq!(
            store.role("bound-old").unwrap(),
            Some(signal_flow::Caller {
                flow_id: "bound-old".into(),
                aspect: Aspect::Mind,
                power_level: PowerLevel::Medium,
                model_name: "gpt-sol".into(),
            })
        );
        assert_eq!(store.role("registered-old").unwrap(), None);
    }

    #[test]
    fn earlier_launch_attempt_rows_are_set_aside_whole_once_and_logged() {
        use super::{
            ArchivedLaunchAttemptShape, EarlierLaunchAttemptRows, LaunchAttemptSettlement,
            QueryPlan, ReadsFlowRows,
        };
        let fixture = StoreFixture::new();
        let path = fixture.directory.path().join("flow.sema");
        let rows = EarlierLaunchAttemptRows {
            bound_flow_id: "launched-earlier".into(),
        };
        rows.seed(&path);

        let store = fixture.store();
        let quarantined = match store.opening_settlements.as_slice() {
            [
                LaunchAttemptSettlement::SetAside {
                    launch_request_id: earlier,
                    from: ArchivedLaunchAttemptShape::BeforeSystemPromptBundle,
                },
                LaunchAttemptSettlement::SetAside {
                    launch_request_id: before_address,
                    from: ArchivedLaunchAttemptShape::BeforeAddress,
                },
                LaunchAttemptSettlement::Quarantined {
                    launch_request_id: unreadable,
                    decode_error,
                },
            ] => {
                assert_eq!(earlier, EarlierLaunchAttemptRows::EARLIER_REQUEST);
                assert_eq!(
                    before_address,
                    EarlierLaunchAttemptRows::BEFORE_ADDRESS_REQUEST
                );
                assert_eq!(unreadable, EarlierLaunchAttemptRows::UNREADABLE_REQUEST);
                decode_error.clone()
            }
            other => panic!("unexpected settlements: {other:?}"),
        };
        assert_eq!(
            store.opening_settlements[0].to_string(),
            "LaunchAttemptSetAside.{ flow06-earlier BeforeSystemPromptBundle }"
        );
        assert_eq!(
            store.opening_settlements[1].to_string(),
            "LaunchAttemptSetAside.{ flow15-before-address BeforeAddress }"
        );
        assert_eq!(
            store.opening_settlements[2].to_string(),
            format!("LaunchAttemptQuarantined.{{ unreadable-request «{quarantined}» }}")
        );
        // No earlier shape names the launched seat's Address: none is
        // carried into the attempts table.
        for request in [
            EarlierLaunchAttemptRows::EARLIER_REQUEST,
            EarlierLaunchAttemptRows::BEFORE_ADDRESS_REQUEST,
            EarlierLaunchAttemptRows::UNREADABLE_REQUEST,
        ] {
            assert_eq!(store.launch_attempt(request).unwrap(), None);
        }
        // The 0.34 row is carried, not set aside.
        assert!(
            store
                .launch_attempt(EarlierLaunchAttemptRows::ZERO_THIRTY_FOUR_REQUEST)
                .unwrap()
                .is_some()
        );
        // Every whole-table read answers again.
        assert!(store.ambiguous_launch_attempts().is_ok());
        assert_eq!(store.flow_nodes().unwrap().len(), 1);
        // Every row is kept whole, each with the shape it was found to have.
        let kept = store
            .engine
            .match_records(QueryPlan::all(store.quarantined_launch_attempts))
            .unwrap()
            .records()
            .to_vec();
        assert_eq!(kept.len(), 3);
        assert!(kept.iter().any(|row| {
            row.launch_request_id == EarlierLaunchAttemptRows::EARLIER_REQUEST
                && row.shape == ArchivedLaunchAttemptShape::BeforeSystemPromptBundle
                && rkyv::from_bytes::<super::StoredLaunchAttemptBeforeBundle, rkyv::rancor::Error>(
                    &row.archive,
                )
                .is_ok_and(|earlier| earlier.attempt == rows.earlier_attempt())
        }));
        assert!(kept.iter().any(|row| {
            row.launch_request_id == EarlierLaunchAttemptRows::BEFORE_ADDRESS_REQUEST
                && row.shape == ArchivedLaunchAttemptShape::BeforeAddress
                && rkyv::from_bytes::<super::StoredLaunchAttemptBeforeAddress, rkyv::rancor::Error>(
                    &row.archive,
                )
                .is_ok_and(|kept| kept.attempt == rows.before_address_attempt())
        }));
        assert!(kept.iter().any(|row| row.launch_request_id
            == EarlierLaunchAttemptRows::UNREADABLE_REQUEST
            && row.shape == ArchivedLaunchAttemptShape::Unknown(quarantined.clone())));
        // The flow the earlier launch bound takes its role from the kept row.
        assert_eq!(
            store.role("launched-earlier").unwrap(),
            Some(signal_flow::Caller {
                flow_id: "launched-earlier".into(),
                aspect: Aspect::Psyche,
                power_level: PowerLevel::Medium,
                model_name: "claude-opus-5-5".into(),
            })
        );
        drop(store);

        let store = fixture.store();
        assert_eq!(store.opening_settlements, Vec::new());
        assert_eq!(
            store
                .engine
                .match_records(QueryPlan::all(store.quarantined_launch_attempts))
                .unwrap()
                .records()
                .len(),
            3
        );
    }

    /// How many rows each Flow table holds: read raw off a store file, or
    /// read whole, every row decoded, through an open store.
    trait CountsRowsByTable {
        fn rows_by_table(&self) -> std::collections::BTreeMap<String, u64>;
    }

    /// Every table the catalog registers, each row counted undecoded.
    impl CountsRowsByTable for std::path::Path {
        fn rows_by_table(&self) -> std::collections::BTreeMap<String, u64> {
            use redb::{ReadableDatabase as _, ReadableTable as _, ReadableTableMetadata as _};
            let database = redb::ReadOnlyDatabase::open(self).expect("store reads directly");
            let read = database.begin_read().expect("read starts");
            let catalog = read
                .open_table(super::SEMA_CATALOG_TABLE)
                .expect("catalog opens");
            let names = catalog
                .iter()
                .expect("catalog iterates")
                .map(|entry| entry.expect("catalog entry").0.value().to_owned())
                .collect::<Vec<_>>();
            names
                .into_iter()
                .map(|name| {
                    let rows =
                        match read.open_table(redb::TableDefinition::<String, &[u8]>::new(&name)) {
                            Ok(table) => table.len().expect("table length"),
                            Err(redb::TableError::TableDoesNotExist(_)) => 0,
                            Err(error) => panic!("table {name} opens: {error}"),
                        };
                    (name, rows)
                })
                .collect()
        }
    }

    /// Every table the store holds, each row read and decoded in its
    /// current shape.
    impl CountsRowsByTable for FlowStore {
        fn rows_by_table(&self) -> std::collections::BTreeMap<String, u64> {
            let engine = &self.engine;
            let meta = &self.meta_configuration_tables;
            let delivery = &self.delivery_tables;
            let read = |name: &str, rows: usize| (name.to_owned(), rows as u64);
            [
                read(
                    "flow_nexus_flows",
                    engine
                        .match_records(QueryPlan::all(self.flows))
                        .expect("flows read")
                        .records()
                        .len(),
                ),
                read(
                    "flow_nexus_state",
                    engine
                        .match_records(QueryPlan::all(self.state))
                        .expect("state reads")
                        .records()
                        .len(),
                ),
                read(
                    "flow_nexus_metaflows",
                    engine
                        .match_records(QueryPlan::all(self.metaflows))
                        .expect("metaflows read")
                        .records()
                        .len(),
                ),
                read(
                    "flow_nexus_metaflow_processes",
                    engine
                        .match_records(QueryPlan::all(self.metaflow_processes))
                        .expect("metaflow processes read")
                        .records()
                        .len(),
                ),
                read(
                    "flow_nexus_metaflow_locks",
                    engine
                        .match_records(QueryPlan::all(self.metaflow_locks))
                        .expect("metaflow locks read")
                        .records()
                        .len(),
                ),
                read(
                    "flow_nexus_configuration",
                    engine
                        .match_records(QueryPlan::all(self.configuration))
                        .expect("configuration reads")
                        .records()
                        .len(),
                ),
                read(
                    "flow_nexus_runtime_configuration",
                    engine
                        .match_records(QueryPlan::all(self.runtime_configuration))
                        .expect("runtime configuration reads")
                        .records()
                        .len(),
                ),
                read(
                    "flow_nexus_herdr_routes",
                    engine
                        .match_records(QueryPlan::all(self.herdr_routes))
                        .expect("herdr routes read")
                        .records()
                        .len(),
                ),
                read(
                    "flow_nexus_launch_attempts",
                    engine
                        .match_records(QueryPlan::all(self.launch_attempts))
                        .expect("launch attempts read")
                        .records()
                        .len(),
                ),
                read(
                    "flow_nexus_launch_panes",
                    engine
                        .match_records(QueryPlan::all(self.launch_panes))
                        .expect("launch panes read")
                        .records()
                        .len(),
                ),
                read(
                    "flow_nexus_launch_outcomes",
                    engine
                        .match_records(QueryPlan::all(self.launch_outcomes))
                        .expect("launch outcomes read")
                        .records()
                        .len(),
                ),
                read(
                    "flow_nexus_replacements",
                    engine
                        .match_records(QueryPlan::all(self.replacements))
                        .expect("replacements read")
                        .records()
                        .len(),
                ),
                read(
                    "flow_nexus_roles",
                    engine
                        .match_records(QueryPlan::all(self.roles))
                        .expect("roles read")
                        .records()
                        .len(),
                ),
                read(
                    "flow_nexus_quarantined_launch_attempts",
                    engine
                        .match_records(QueryPlan::all(self.quarantined_launch_attempts))
                        .expect("quarantined launch attempts read")
                        .records()
                        .len(),
                ),
                read(
                    "flow_nexus_deliveries",
                    engine
                        .match_records(QueryPlan::all(delivery.deliveries))
                        .expect("deliveries read")
                        .records()
                        .len(),
                ),
                read(
                    "flow_nexus_pane_leases",
                    engine
                        .match_records(QueryPlan::all(delivery.pane_leases))
                        .expect("pane leases read")
                        .records()
                        .len(),
                ),
                read(
                    "flow_nexus_delivery_configuration",
                    engine
                        .match_records(QueryPlan::all(delivery.configuration))
                        .expect("delivery configuration reads")
                        .records()
                        .len(),
                ),
                read(
                    "flow_nexus_meta_nexus_configuration",
                    engine
                        .match_records(QueryPlan::all(meta.nexus))
                        .expect("meta nexus configuration reads")
                        .records()
                        .len(),
                ),
                read(
                    "flow_nexus_meta_model_configuration",
                    engine
                        .match_records(QueryPlan::all(meta.models))
                        .expect("meta model configuration reads")
                        .records()
                        .len(),
                ),
                read(
                    "flow_nexus_meta_threshold_configuration",
                    engine
                        .match_records(QueryPlan::all(meta.thresholds))
                        .expect("meta threshold configuration reads")
                        .records()
                        .len(),
                ),
                read(
                    "flow_nexus_meta_leave_out_configuration",
                    engine
                        .match_records(QueryPlan::all(meta.leave_outs))
                        .expect("meta leave-out configuration reads")
                        .records()
                        .len(),
                ),
                read(
                    "flow_nexus_flow_events",
                    engine
                        .match_records(QueryPlan::all(self.event_tables.events))
                        .expect("flow events read")
                        .records()
                        .len(),
                ),
                read(
                    "flow_nexus_flow_context_sizes",
                    engine
                        .match_records(QueryPlan::all(self.context_sizes))
                        .expect("context sizes read")
                        .records()
                        .len(),
                ),
                read(
                    "flow_nexus_address_holds",
                    engine
                        .match_records(QueryPlan::all(self.address_holds))
                        .expect("address holds read")
                        .records()
                        .len(),
                ),
                read(
                    "flow_nexus_metaflow_refreshes",
                    engine
                        .match_records(QueryPlan::all(self.refreshes))
                        .expect("fresh-wake marks read")
                        .records()
                        .len(),
                ),
                read(
                    "flow_nexus_launch_opened_threads",
                    engine
                        .match_records(QueryPlan::all(self.opened_threads))
                        .expect("opened threads read")
                        .records()
                        .len(),
                ),
                read(
                    "flow_nexus_send_calls",
                    engine
                        .match_records(QueryPlan::all(self.send_calls))
                        .expect("send calls read")
                        .records()
                        .len(),
                ),
                read(
                    "flow_nexus_launch_cleanups",
                    engine
                        .match_records(QueryPlan::all(self.launch_cleanups))
                        .expect("launch cleanups read")
                        .records()
                        .len(),
                ),
            ]
            .into_iter()
            .collect()
        }
    }

    /// A store Flow 0.34.4 wrote, opened as production opens it: every
    /// row of every table reads in the current shape, and each table holds
    /// as many rows as it did. `FLOW_0_34_STORE` names that store; it is
    /// copied and only the copy is opened. No such store is kept in the
    /// repository, so the test runs only when asked for by name with
    /// `--ignored` and the variable set.
    #[test]
    #[ignore = "needs FLOW_0_34_STORE naming a store Flow 0.34.4 wrote"]
    fn a_0_34_4_store_opens_and_every_row_of_every_table_reads() {
        use super::{LaunchAttemptSettlement, ManagesMetaflowState, ReadsFlowRows};
        let source = std::env::var_os("FLOW_0_34_STORE")
            .map(std::path::PathBuf::from)
            .expect("FLOW_0_34_STORE names the 0.34.4 store to copy");
        let directory = tempfile::tempdir().expect("copy directory");
        let path = directory.path().join("flow.sema");
        std::fs::copy(&source, &path).expect("0.34.4 store copied");
        let mut permissions = std::fs::metadata(&path)
            .expect("copy metadata")
            .permissions();
        #[allow(clippy::permissions_set_readonly_false)]
        permissions.set_readonly(false);
        std::fs::set_permissions(&path, permissions).expect("copy writable");

        let before = path.rows_by_table();
        eprintln!("rows before: {before:?}");
        let store = FlowStore::open_configured(&path).expect("the 0.34.4 store opens");
        let set_aside = store
            .opening_settlements
            .iter()
            .filter(|settlement| !matches!(settlement, LaunchAttemptSettlement::Unsettled { .. }))
            .count() as u64;
        assert!(
            !store
                .opening_settlements
                .iter()
                .any(|settlement| matches!(settlement, LaunchAttemptSettlement::Unsettled { .. })),
            "{:?}",
            store.opening_settlements
        );
        let after = store.rows_by_table();
        eprintln!("rows after: {after:?}");
        eprintln!("launch-attempt rows set aside: {set_aside}");
        for (table, rows) in &before {
            if super::RETIRED_TABLES
                .iter()
                .any(|(retired, _, _)| retired == table)
            {
                eprintln!("retired table {table} held {rows} rows");
                assert!(!after.contains_key(table), "{table} is retired");
                continue;
            }
            let expected = match table.as_str() {
                "flow_nexus_launch_attempts" => rows - set_aside,
                "flow_nexus_quarantined_launch_attempts" => rows + set_aside,
                _ => *rows,
            };
            assert_eq!(after.get(table), Some(&expected), "rows of {table}");
        }
        for (table, rows) in &after {
            if !before.contains_key(table) {
                assert_eq!(*rows, 0, "{table} is added empty");
            }
        }

        // Every whole-table read answers, and every attempt and outcome.
        assert!(store.ambiguous_launch_attempts().is_ok());
        assert_eq!(
            store.flow_nodes().expect("flow nodes read").len() as u64,
            after["flow_nexus_flows"]
        );
        assert_eq!(
            store.list_metaflows().expect("metaflows list").len() as u64,
            after["flow_nexus_metaflows"]
        );
        assert!(
            store
                .configuration_snapshot()
                .expect("snapshot reads")
                .is_some()
        );
        let attempts = store
            .engine
            .match_records(QueryPlan::all(store.launch_attempts))
            .expect("attempts read")
            .records()
            .to_vec();
        for stored in &attempts {
            let request = &stored.attempt.launch_request_id;
            assert!(
                store
                    .launch_attempt(request)
                    .expect("attempt reads")
                    .is_some()
            );
            store.launch_outcome(request).expect("outcome reads");
        }
        drop(store);

        // The upgrade is done once: a reopen carries nothing again.
        let reopened = FlowStore::open_configured(&path).expect("the upgraded store reopens");
        assert_eq!(reopened.opening_settlements, Vec::new());
        assert_eq!(reopened.rows_by_table(), after);
    }

    /// A 0.34 launch-attempt row is carried into the current shape when the
    /// store opens: what the current shape names, into it, and what it no
    /// longer names, kept beside it; no later seat is composed from it.
    #[test]
    fn a_0_34_launch_attempt_row_is_carried_with_what_it_named_kept() {
        use super::{
            Address, ContextBeforeModules, EarlierLaunchAttemptRows, LaunchAttempt, LaunchContext,
            LaunchSource, Layer, NativeSkill, OriginClue,
        };
        let fixture = StoreFixture::new();
        let path = fixture.directory.path().join("flow.sema");
        let rows = EarlierLaunchAttemptRows {
            bound_flow_id: "launched-earlier".into(),
        };
        rows.seed(&path);
        let request = EarlierLaunchAttemptRows::ZERO_THIRTY_FOUR_REQUEST;

        let store = fixture.store();
        let pane = signal_flow::HerdrPaneBinding {
            launch_request_id: request.into(),
            herdr_session_name: "messaging-build".into(),
            herdr_agent_name: "mind-sol-34".into(),
            herdr_workspace_id: "w34".into(),
            herdr_pane_id: "w34:p1".into(),
            herdr_terminal_id: "term-34".into(),
        };
        let intent = signal_flow::PromptDeliveryIntent {
            launch_request_id: request.into(),
            prompt_sha256: "ab".repeat(32),
            flow_id: "flow34".into(),
            native_session_id: "session-34".into(),
            harness_kind: HarnessKind::Codex,
            model_name: "gpt-sol".into(),
            effort: "medium".into(),
            prompt_sha256_vector: vec!["ab".repeat(32)],
            parts_submitted: 1,
            stops_reported: 0,
            leave_outs: Vec::new(),
            herdr_pane_binding: pane.clone(),
            native_transcript_boundary: signal_flow::NativeTranscriptBoundary::Absent(
                signal_flow::NativeTranscriptAbsence {
                    native_session_id: "session-34".into(),
                    harness_kind: HarnessKind::Codex,
                    transcript_root_device: "64768".into(),
                    transcript_root_inode: "1234".into(),
                },
            ),
        };
        let expected = LaunchAttempt {
            launch_request_id: request.into(),
            launch_profile: signal_flow::LaunchProfile {
                launch_request_id: request.into(),
                system_modules: Vec::new(),
                user_modules: Vec::new(),
                address: Address {
                    aspect: Aspect::Mind,
                    topic: "flow".into(),
                    layer: Layer::Secondary,
                },
                model_name: "gpt-sol".into(),
                effort: "medium".into(),
                flow_id_option: Some("predecessor-34".into()),
                remembered_flow_vector: vec![signal_flow::RememberedFlow {
                    flow_id: "e51411".into(),
                    remembering_depth: 2,
                }],
                herdr_session_name: "messaging-build".into(),
                leave_outs_option: Some(Vec::new()),
                instruction_prompt: "0.34 instruction".into(),
            },
            prompt_sha256: "ab".repeat(32),
            origin: Origin::Flow(signal_flow::FlowOrigin {
                flow_id: "e51411".into(),
                session_id: "e51411-session".into(),
            }),
            launch_attempt_phase: LaunchAttemptPhase::PromptObserved,
            native_launch_intent_option: Some(signal_flow::NativeLaunchIntent {
                launch_request_id: request.into(),
                prompt_sha256: "ab".repeat(32),
                system_prompt_sha256: String::new(),
                harness_kind: HarnessKind::Codex,
                model_name: "gpt-sol".into(),
                effort: "medium".into(),
                prompt_sha256_vector: vec!["ab".repeat(32)],
            }),
            native_launch_binding_option: Some(signal_flow::NativeLaunchBinding {
                launch_request_id: request.into(),
                flow_id: "flow34".into(),
                native_session_id: "session-34".into(),
                harness_kind: HarnessKind::Codex,
                herdr_pane_binding: pane.clone(),
            }),
            registration_acknowledgement_option: Some(signal_flow::RegistrationAcknowledgement {
                launch_request_id: request.into(),
                flow_id: "flow34".into(),
                native_session_id: "session-34".into(),
                herdr_pane_binding: pane,
            }),
            prompt_delivery_intent_option: Some(intent),
            prompt_delivery_result_option: Some(signal_flow::PromptDeliveryResult::Observed(
                signal_flow::NativeTargetReceipt {
                    launch_request_id: request.into(),
                    prompt_sha256: "ab".repeat(32),
                    flow_id: "flow34".into(),
                    native_session_id: "session-34".into(),
                    native_turn_id: "turn-1".into(),
                    receipt_sha256: "12".repeat(32),
                    model_name: "gpt-sol".into(),
                    effort: "medium".into(),
                    prompt_sha256_vector: vec!["ab".repeat(32)],
                },
            )),
        };
        let skill = NativeSkill {
            skill_name: "operation-main-flow".into(),
            skill_path: "/kept/skills/operation-main-flow/SKILL.md".into(),
            skill_sha256: "ef".repeat(32),
        };
        let stored = store
            .stored_launch_attempt(request)
            .unwrap()
            .expect("0.34 row carried");
        assert_eq!(stored.attempt, expected);
        assert_eq!(
            stored.context,
            LaunchContext::BeforeModules(ContextBeforeModules {
                launch_sources: vec![LaunchSource {
                    source_path: "/kept/skills/operation-main-flow.md".into(),
                    source_sha256: "cd".repeat(32),
                }],
                skills: vec!["operation-main-flow".into()],
                power_level: PowerLevel::Medium,
                harness_kind: HarnessKind::Codex,
                bundle_file: "/kept/bundles/flow34.md".into(),
                origin_clue: OriginClue {
                    flow_id: "e51411".into(),
                    session_id: "e51411-session".into(),
                    turn_id: "turn-34".into(),
                },
                launch_intent_skills: Some(vec!["operation-main-flow".into()]),
                intent_skills: Some(vec![skill.clone()]),
                result_skills: Some(vec![skill]),
            })
        );
        // No module names its context: no later seat is composed from it.
        assert_eq!(store.composable_launch_profile(request).unwrap(), None);
        assert_eq!(
            store.launch_attempt(request).unwrap(),
            Some(expected.clone())
        );
        drop(store);

        // The family is current now: a reopen carries nothing again.
        let store = fixture.store();
        assert_eq!(store.opening_settlements, Vec::new());
        assert_eq!(store.launch_attempt(request).unwrap(), Some(expected));
    }

    #[test]
    fn a_role_is_kept_with_its_binding_and_a_different_one_conflicts() {
        let fixture = StoreFixture::new();
        let store = fixture.store();
        let role = signal_flow::Caller {
            flow_id: "psyche-high".into(),
            aspect: Aspect::Psyche,
            power_level: PowerLevel::High,
            model_name: "claude-opus-5-5".into(),
        };
        let node = pane_node("psyche-high", "w1:p3");
        assert!(matches!(
            store
                .register_existing_flow(node.clone(), role.clone())
                .unwrap(),
            super::FlowRegistration::Registered(_)
        ));
        assert_eq!(store.role("psyche-high").unwrap(), Some(role.clone()));
        assert_eq!(
            store
                .flows_in_pane("messaging-build", "w1:p3")
                .unwrap()
                .into_iter()
                .map(|node| node.flow_id)
                .collect::<Vec<_>>(),
            vec!["psyche-high".to_string()]
        );
        let mut other = role;
        other.model_name = "claude-sonnet".into();
        assert_eq!(
            store.register_flow_in_role(node, other).unwrap(),
            super::FlowRegistration::ConflictingBinding
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
            origin: signal_flow::Origin::Flow(signal_flow::FlowOrigin {
                flow_id: "da1e3f".into(),
                session_id: "da1e3f9d-full".into(),
            }),
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

    /// A flow row of 0.34 named the clue its caller gave as its origin; the
    /// open upgrades it to the origin it was: the calling flow, or Unbound
    /// for the Nexus's own wake. Nothing else of the row changes.
    /// A store before 0.35.7 keeps queues of bare requests: opening carries
    /// every metaflow whole, each queued request with its sender Unknown.
    #[test]
    fn a_v1_metaflow_queue_upgrades_with_its_senders_unknown() {
        use super::ManagesMetaflowState;
        use signal_flow::{Address, Layer, Request};
        let fixture = StoreFixture::new();
        drop(fixture.store());
        let address = Address {
            aspect: Aspect::Mind,
            topic: "flow".into(),
            layer: Layer::Tertiary,
        };
        let database = redb::Database::open(fixture.path()).expect("store opens directly");
        let write = database.begin_write().expect("catalog transaction starts");
        {
            let mut catalog = write
                .open_table(super::SEMA_CATALOG_TABLE)
                .expect("catalog opens");
            catalog
                .remove("flow_nexus_metaflows")
                .expect("metaflow family removes");
        }
        write
            .delete_table(redb::TableDefinition::<&str, &[u8]>::new(
                "flow_nexus_metaflows",
            ))
            .expect("metaflow rows drop");
        write.commit().expect("catalog commits");
        drop(database);
        {
            let mut engine = Engine::open(EngineOpen::new(fixture.path(), SchemaVersion::new(1)))
                .expect("v1 store opens");
            let metaflows = engine
                .register_table(TableDescriptor::new(
                    TableName::new("flow_nexus_metaflows"),
                    FamilyName::new("flow-nexus-metaflow"),
                    SchemaHash::for_label("flow-nexus-metaflow-v1"),
                ))
                .expect("v1 metaflow family registers");
            engine
                .assert(Assertion::new(
                    metaflows,
                    super::StoredMetaflowV1 {
                        metaflow: super::MetaflowBeforeSenders {
                            address: address.clone(),
                            state: flow_ethos::State::Asleep,
                            past: vec!["a1a1a1".into()],
                            queue: vec![
                                Request::Order("first".into()),
                                Request::Notice("second".into()),
                            ],
                        },
                    },
                ))
                .expect("v1 row persists");
        }

        let store = fixture.store();
        let metaflow = store
            .list_metaflows()
            .unwrap()
            .into_iter()
            .find(|metaflow| metaflow.address == address)
            .expect("the metaflow is carried");
        assert_eq!(metaflow.state, flow_ethos::State::Asleep);
        assert_eq!(metaflow.past, vec!["a1a1a1".to_owned()]);
        assert_eq!(
            metaflow.queue,
            vec![
                flow_ethos::QueuedRequest {
                    sent_by: flow_ethos::SentBy::Unknown,
                    request: Request::Order("first".into()),
                },
                flow_ethos::QueuedRequest {
                    sent_by: flow_ethos::SentBy::Unknown,
                    request: Request::Notice("second".into()),
                },
            ]
        );
    }

    #[test]
    fn a_v5_flow_row_upgrades_to_its_derived_origin() {
        let fixture = StoreFixture::new();
        drop(fixture.store());
        let clue = |flow_id: &str, session_id: &str| OriginClueBeforeOrigin {
            flow_id: flow_id.into(),
            session_id: session_id.into(),
            turn_id: "turn-1".into(),
        };
        let row = |flow_id: &str, origin: OriginClueBeforeOrigin| FlowRecordV5 {
            flow_id: flow_id.into(),
            flow_type: "claude-registered".into(),
            origin,
            thread_id: Some(format!("{flow_id}-session")),
            harness_kind: HarnessKind::Claude,
            endpoint_selection: signal_flow::EndpointSelection::Unavailable,
            lifecycle: super::FlowLifecycle::Active,
            generation: 7,
        };
        // The store as 0.34 left it: the flows family at v5.
        let database = redb::Database::open(fixture.path()).expect("store opens directly");
        let write = database.begin_write().expect("catalog transaction starts");
        {
            let mut catalog = write
                .open_table(super::SEMA_CATALOG_TABLE)
                .expect("catalog opens");
            catalog
                .remove("flow_nexus_flows")
                .expect("flow family removes");
        }
        write
            .delete_table(redb::TableDefinition::<&str, &[u8]>::new(
                "flow_nexus_flows",
            ))
            .expect("flow rows drop");
        write.commit().expect("catalog commits");
        drop(database);
        {
            let mut engine = Engine::open(EngineOpen::new(fixture.path(), SchemaVersion::new(1)))
                .expect("v5 store opens");
            let flows = engine
                .register_table(TableDescriptor::new(
                    TableName::new("flow_nexus_flows"),
                    FamilyName::new("flow-nexus-flow"),
                    SchemaHash::for_label("flow-nexus-flow-v5"),
                ))
                .expect("v5 flow family registers");
            for kept in [
                row("old-called", clue("caller-flow", "caller-session")),
                row("old-woken", clue("e51411", "flow-nexus-wake")),
            ] {
                engine
                    .assert(Assertion::new(flows, kept))
                    .expect("v5 row persists");
            }
        }

        let store = fixture.store();
        let called = store.flow("old-called").unwrap().expect("upgraded row");
        assert_eq!(
            called.origin,
            Origin::Flow(FlowOrigin {
                flow_id: "caller-flow".into(),
                session_id: "caller-session".into(),
            })
        );
        assert_eq!(called.generation, 7);
        assert_eq!(called.thread_id.as_deref(), Some("old-called-session"));
        assert_eq!(
            store
                .flow("old-woken")
                .unwrap()
                .expect("upgraded row")
                .origin,
            Origin::Unbound
        );
        let Response::RecipientResolved(node) = store
            .apply(Query::ResolveRecipient("old-called".into()))
            .unwrap()
        else {
            panic!("old active flow resolves")
        };
        assert_eq!(
            node.herdr_route_selection,
            signal_flow::HerdrRouteSelection::Unavailable
        );
        drop(store);
        // Reopening sees v6 and leaves the row as upgraded.
        assert_eq!(
            fixture.store().flow("old-called").unwrap().unwrap().origin,
            called.origin
        );
    }

    /// A 0.35.5 store's outcomes, each rejection in signal-flow 18's shape,
    /// open as the current outcomes: a refused native step or registration,
    /// whose stage was not kept, reads Unclassified, every other outcome as
    /// it was; the store then keeps a refusal with its stage.
    #[test]
    fn v4_launch_outcomes_upgrade_to_the_staged_refusal_shape() {
        let fixture = StoreFixture::new();
        let started = LaunchedRecord {
            flow_id: "started-flow".into(),
            session_id: "started-session".into(),
            origin: Origin::Unbound,
        };
        let spawn = signal_flow::HarnessSpawnRefusal {
            spawn_refusal_cause: signal_flow::SpawnRefusalCause::CommandTooLong,
            opened_thread_option: None,
        };
        let rows = [
            StoredLaunchOutcomeV4 {
                launch_request_id: "v4-started".into(),
                launch_outcome: LaunchOutcomeV4::Started(started.clone()),
            },
            StoredLaunchOutcomeV4 {
                launch_request_id: "v4-native".into(),
                launch_outcome: LaunchOutcomeV4::StartRejected(
                    StartRejectionV18::NativeLaunchRefused,
                ),
            },
            StoredLaunchOutcomeV4 {
                launch_request_id: "v4-registration".into(),
                launch_outcome: LaunchOutcomeV4::ReplaceRejected(
                    ReplaceRejectionV18::LaunchRefused(StartRejectionV18::RegistrationRefused),
                ),
            },
            StoredLaunchOutcomeV4 {
                launch_request_id: "v4-spawn".into(),
                launch_outcome: LaunchOutcomeV4::StartRejected(
                    StartRejectionV18::HarnessSpawnRefused(spawn.clone()),
                ),
            },
        ];
        {
            let mut engine = Engine::open(EngineOpen::new(fixture.path(), SchemaVersion::new(1)))
                .expect("outcome store opens");
            let outcomes = engine
                .register_table(TableDescriptor::new(
                    TableName::new("flow_nexus_launch_outcomes"),
                    FamilyName::new("flow-nexus-launch-outcome"),
                    SchemaHash::for_label("flow-nexus-launch-outcome-v4"),
                ))
                .expect("v4 outcome family registers");
            for row in rows {
                engine
                    .assert(Assertion::new(outcomes, row))
                    .expect("v4 outcome persists");
            }
        }
        let expected = [
            ("v4-started", LaunchOutcome::Started(started)),
            (
                "v4-native",
                LaunchOutcome::StartRejected(StartRejection::NativeLaunchRefused(
                    signal_flow::NativeLaunchRefusal {
                        native_launch_stage: signal_flow::NativeLaunchStage::Unclassified,
                        native_launch_failure: signal_flow::NativeLaunchFailure::Unclassified,
                    },
                )),
            ),
            (
                "v4-registration",
                LaunchOutcome::ReplaceRejected(ReplaceRejection::LaunchRefused(
                    StartRejection::RegistrationRefused(
                        signal_flow::RegistrationRefusal::Unclassified,
                    ),
                )),
            ),
            (
                "v4-spawn",
                LaunchOutcome::StartRejected(StartRejection::HarnessSpawnRefused(spawn)),
            ),
        ];
        let store = fixture.store();
        for (request, outcome) in &expected {
            assert_eq!(
                store
                    .launch_outcome(request)
                    .expect("upgraded outcome reads"),
                Some(outcome.clone()),
                "the v4 outcome for {request} survives the family upgrade"
            );
        }
        let staged = LaunchOutcome::StartRejected(StartRejection::RegistrationRefused(
            signal_flow::RegistrationRefusal::NativeTarget(
                signal_flow::NativeTargetRefusal::IdentityInexact,
            ),
        ));
        store
            .record_launch_outcome("v5-staged", staged.clone())
            .expect("a staged refusal persists");
        drop(store);
        let reopened = fixture.store();
        for (request, outcome) in expected.iter().chain([("v5-staged", staged)].iter()) {
            assert_eq!(
                reopened
                    .launch_outcome(request)
                    .expect("outcome reads after reopening"),
                Some(outcome.clone()),
                "the outcome for {request} reads unchanged after reopening"
            );
        }
    }

    /// A 0.35.2 store's outcomes, each rejection in signal-flow 17's shape,
    /// open as the current outcomes, and the store then keeps a refused
    /// harness start with its cause and its opened thread.
    #[test]
    fn v3_launch_outcomes_upgrade_to_the_refused_spawn_shape() {
        let fixture = StoreFixture::new();
        let started = LaunchedRecord {
            flow_id: "started-flow".into(),
            session_id: "started-session".into(),
            origin: Origin::Unbound,
        };
        let rows = [
            StoredLaunchOutcomeV3 {
                launch_request_id: "v3-started".into(),
                launch_outcome: LaunchOutcomeV3::Started(started.clone()),
            },
            StoredLaunchOutcomeV3 {
                launch_request_id: "v3-leave-out".into(),
                launch_outcome: LaunchOutcomeV3::StartRejected(StartRejectionV17::LeaveOutRefused(
                    signal_flow::LeaveOutRefusal::NotForHarness(
                        signal_flow::LeaveOut::CodexPermissions,
                    ),
                )),
            },
            StoredLaunchOutcomeV3 {
                launch_request_id: "v3-native".into(),
                launch_outcome: LaunchOutcomeV3::StartRejected(
                    StartRejectionV17::NativeLaunchRefused,
                ),
            },
            StoredLaunchOutcomeV3 {
                launch_request_id: "v3-replace".into(),
                launch_outcome: LaunchOutcomeV3::ReplaceRejected(
                    ReplaceRejectionV17::LaunchRefused(StartRejectionV17::ContextRefused(
                        signal_flow::ContextRefusal::SystemTooLarge,
                    )),
                ),
            },
        ];
        {
            let mut engine = Engine::open(EngineOpen::new(fixture.path(), SchemaVersion::new(1)))
                .expect("outcome store opens");
            let outcomes = engine
                .register_table(TableDescriptor::new(
                    TableName::new("flow_nexus_launch_outcomes"),
                    FamilyName::new("flow-nexus-launch-outcome"),
                    SchemaHash::for_label("flow-nexus-launch-outcome-v3"),
                ))
                .expect("v3 outcome family registers");
            for row in rows {
                engine
                    .assert(Assertion::new(outcomes, row))
                    .expect("v3 outcome persists");
            }
        }
        let expected = [
            ("v3-started", LaunchOutcome::Started(started)),
            (
                "v3-leave-out",
                LaunchOutcome::StartRejected(StartRejection::LeaveOutRefused(
                    signal_flow::LeaveOutRefusal::NotForHarness(
                        signal_flow::LeaveOut::CodexPermissions,
                    ),
                )),
            ),
            (
                "v3-native",
                LaunchOutcome::StartRejected(StartRejection::NativeLaunchRefused(
                    signal_flow::NativeLaunchRefusal {
                        native_launch_stage: signal_flow::NativeLaunchStage::Unclassified,
                        native_launch_failure: signal_flow::NativeLaunchFailure::Unclassified,
                    },
                )),
            ),
            (
                "v3-replace",
                LaunchOutcome::ReplaceRejected(ReplaceRejection::LaunchRefused(
                    StartRejection::ContextRefused(signal_flow::ContextRefusal::SystemTooLarge),
                )),
            ),
        ];
        let store = fixture.store();
        for (request, outcome) in &expected {
            assert_eq!(
                store
                    .launch_outcome(request)
                    .expect("upgraded outcome reads"),
                Some(outcome.clone()),
                "the v3 outcome for {request} survives the family upgrade"
            );
        }
        let refused = LaunchOutcome::StartRejected(StartRejection::HarnessSpawnRefused(
            signal_flow::HarnessSpawnRefusal {
                spawn_refusal_cause: signal_flow::SpawnRefusalCause::Herdr(
                    signal_flow::HerdrRefusal {
                        herdr_error_code_option: Some("invalid_agent_argument".into()),
                        herdr_error_message:
                            "agent arguments cannot be encoded safely for the target shell".into(),
                    },
                ),
                opened_thread_option: Some(signal_flow::OpenedThread {
                    native_session_id: "01a14773-f58c-7ca1-978b-1ac675d19399".into(),
                    first_turn: signal_flow::FirstTurn::Answered,
                    thread_archive: signal_flow::ThreadArchive::Archived,
                }),
            },
        ));
        store
            .record_launch_outcome("v4-refused", refused.clone())
            .expect("a refused spawn outcome persists");
        drop(store);
        let reopened = fixture.store();
        for (request, outcome) in expected.iter().chain([("v4-refused", refused)].iter()) {
            assert_eq!(
                reopened
                    .launch_outcome(request)
                    .expect("outcome reads after reopening"),
                Some(outcome.clone()),
                "the outcome for {request} reads unchanged after reopening"
            );
        }
    }

    #[test]
    fn v2_launch_outcomes_upgrade_to_their_derived_origin() {
        let fixture = StoreFixture::new();
        let clue = OriginClueBeforeOrigin {
            flow_id: "origin-flow".into(),
            session_id: "origin-session".into(),
            turn_id: "origin-turn".into(),
        };
        let origin = Origin::Flow(FlowOrigin {
            flow_id: "origin-flow".into(),
            session_id: "origin-session".into(),
        });
        fixture.seed_outcomes(
            "flow-nexus-launch-outcome-v2",
            [
                StoredLaunchOutcomeV2 {
                    launch_request_id: "old-started".into(),
                    launch_outcome: LaunchOutcomeV2::Started(LaunchedRecordV2 {
                        flow_id: "started-flow".into(),
                        session_id: "started-session".into(),
                        origin_clue: clue.clone(),
                    }),
                },
                StoredLaunchOutcomeV2 {
                    launch_request_id: "old-replaced".into(),
                    launch_outcome: LaunchOutcomeV2::Replaced(ReplacedRecordV2 {
                        flow_id: "predecessor-flow".into(),
                        launched_record: LaunchedRecordV2 {
                            flow_id: "successor-flow".into(),
                            session_id: "successor-session".into(),
                            origin_clue: clue,
                        },
                    }),
                },
                StoredLaunchOutcomeV2 {
                    launch_request_id: "old-binding".into(),
                    launch_outcome: LaunchOutcomeV2::StartRejected(
                        StartRejectionV17::BindingRefused(BindingRefusal::Unclassified),
                    ),
                },
                StoredLaunchOutcomeV2 {
                    launch_request_id: "old-reap-rejection".into(),
                    launch_outcome: LaunchOutcomeV2::ReplaceRejected(
                        ReplaceRejectionV17::ReapRefused(StopRejection::CloseRefused),
                    ),
                },
            ],
        );

        let expected = [
            (
                "old-started",
                LaunchOutcome::Started(LaunchedRecord {
                    flow_id: "started-flow".into(),
                    session_id: "started-session".into(),
                    origin: origin.clone(),
                }),
            ),
            (
                "old-replaced",
                LaunchOutcome::Replaced(ReplacedRecord {
                    flow_id: "predecessor-flow".into(),
                    launched_record: LaunchedRecord {
                        flow_id: "successor-flow".into(),
                        session_id: "successor-session".into(),
                        origin,
                    },
                }),
            ),
            (
                "old-binding",
                LaunchOutcome::StartRejected(StartRejection::BindingRefused(
                    BindingRefusal::Unclassified,
                )),
            ),
            (
                "old-reap-rejection",
                LaunchOutcome::ReplaceRejected(ReplaceRejection::ReapRefused(
                    StopRejection::CloseRefused,
                )),
            ),
        ];

        let store = fixture.store();
        for (request, outcome) in &expected {
            assert_eq!(
                store
                    .launch_outcome(request)
                    .expect("upgraded outcome reads"),
                Some(outcome.clone()),
                "the v2 outcome for {request} survives the family upgrade"
            );
        }
        drop(store);

        // Reopening sees v3 and leaves each migrated outcome unchanged.
        let reopened = fixture.store();
        for (request, outcome) in &expected {
            assert_eq!(
                reopened
                    .launch_outcome(request)
                    .expect("reopened outcome reads"),
                Some(outcome.clone()),
                "the v2 family migration is idempotent for {request}"
            );
        }
    }

    #[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
    struct CorruptOutcomeRow {
        launch_request_id: String,
        marker: [u8; 64],
    }

    impl EngineRecord for CorruptOutcomeRow {
        fn record_key(&self) -> sema_engine::RecordKey {
            sema_engine::RecordKey::new(self.launch_request_id.clone())
        }
    }

    #[test]
    fn corrupt_v2_launch_outcome_refuses_upgrade_and_keeps_its_row() {
        let fixture = StoreFixture::new();
        let row = CorruptOutcomeRow {
            launch_request_id: "corrupt-outcome".into(),
            marker: [u8::MAX; 64],
        };
        let mut engine = Engine::open(EngineOpen::new(fixture.path(), SchemaVersion::new(1)))
            .expect("corrupt fixture store opens");
        let outcomes = engine
            .register_table(TableDescriptor::new(
                TableName::new("flow_nexus_launch_outcomes"),
                FamilyName::new("flow-nexus-launch-outcome"),
                SchemaHash::for_label("flow-nexus-launch-outcome-v2"),
            ))
            .expect("v1 outcome family registers");
        engine
            .assert(Assertion::new(outcomes, row.clone()))
            .expect("corrupt fixture row is stored under the v1 family");
        drop(engine);

        assert!(matches!(
            FlowStore::open(&fixture.path()),
            Err(super::StoreError::LaunchOutcomeUpgrade(_))
        ));

        // The failed upgrade leaves its predecessor family and bytes
        // available to the type that wrote them.
        let mut engine = Engine::open(EngineOpen::new(fixture.path(), SchemaVersion::new(1)))
            .expect("predecessor store remains openable");
        let outcomes = engine
            .register_table(TableDescriptor::<CorruptOutcomeRow>::new(
                TableName::new("flow_nexus_launch_outcomes"),
                FamilyName::new("flow-nexus-launch-outcome"),
                SchemaHash::for_label("flow-nexus-launch-outcome-v2"),
            ))
            .expect("corrupt predecessor family remains registered");
        assert_eq!(
            engine
                .match_records(QueryPlan::all(outcomes))
                .expect("the original row remains readable in its own shape")
                .records()
                .to_vec(),
            vec![row]
        );
    }
}
