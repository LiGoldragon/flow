//! Durable Flow Nexus identity and dispatch state.
//!
//! The ordinary Signal contract remains the public boundary.  This module
//! owns its single `.sema` store and lowers a closed `signal_flow::Query`
//! into its typed, durable records.

use crate::store::delivery::RegistersDeliveryTables;
pub(crate) mod address_hold;
pub mod compaction;
pub mod context;
pub(crate) mod counters;
mod created_pane;
pub mod delivery;
pub mod events;
pub(crate) mod handover;
pub(crate) mod handover_configuration;
pub(crate) mod launch_cleanup;
pub(crate) mod logical_delivery;
pub(crate) mod metaflow;
pub(crate) mod metaflow_declaration;
pub(crate) mod nexus_configuration;
pub(crate) mod opened_thread;
pub(crate) mod queue_delivery;
pub mod reminder;
pub(crate) mod repair;
pub(crate) mod restart;
pub(crate) mod send_call;
pub(crate) mod send_outcome;
pub(crate) mod send_server;
pub(crate) mod sent;
mod staged_open;
pub(crate) mod successor;
use address_hold::HoldsLaunchAddress;
use created_pane::StoredLaunchPane;
pub use created_pane::{ReadsCreatedLaunchPane, RecordsCreatedLaunchPane};
use flow_ethos::State as MetaflowState;
pub(crate) use logical_delivery::RecordsLogicalDeliveries;
use logical_delivery::{DeclaresLogicalDeliveryTables, LogicalDeliveryTables};
use metaflow::{TakesLaunchAddress, WritesTakenAddress};
use nexus_configuration::{DescribesNexusConfigurationTable, StoredMetaNexusConfiguration};
use queue_delivery::{AccountsQueueDelivery, QueueDeliveryPhase};
use queue_delivery::{DeclaresQueueDeliveryTables, QueueDeliveryTables};
use send_outcome::{DeclaresSendOutcomeTables, SendOutcomeTables};
use staged_open::{StagedOpen, StagesOpen};

use crate::model::RunsModel;
use delivery::{DeliveryConfiguration, DeliveryTables, RecordsDeliveries};
use std::{
    fs,
    os::unix::fs::MetadataExt,
    path::Path,
    sync::{Arc, Condvar, Mutex},
};

use meta_signal_flow::{
    ConfigurationChange, ConfigurationSnapshot, ConfigureCounters, ConfigureHarnessFlags,
    ConfigureLeaveOut, ConfigureMetaflow, ConfigureModel, ConfigureNexus, ConfigureOutcome,
    ConfigureSendServer, ConfigureThreshold, CountersRefusal, HeartbeatConfig,
};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use sema_engine::{
    Assertion, Engine, EngineOpen, EngineRecord, FamilyName, KeyedMutation, MemoryDefinition,
    MemoryVersion, QueryPlan, RecordKey, Retraction, SchemaHash, SchemaVersion, TableDescriptor,
    TableName, TableReference, TableRegistration,
};
#[cfg(test)]
use signal_flow::Request;
use signal_flow::{
    Address, Aspect, Caller, CallerResolutionRejection, ComposedLaunch, EndpointSelection,
    FlowLifecycle as SignalFlowLifecycle, FlowNode, HarnessKind, HerdrRoute, HerdrRouteSelection,
    LaunchAttempt, LaunchAttemptPhase, LaunchAttemptReservation, LaunchProfile, Launched, LeaveOut,
    Lock, Metaflow, Model, NativeLaunchBinding, NativeLaunchIntent, Origin, Process,
    PromptDeliveryIntent, PromptDeliveryResult, Query, RecipientResolutionRejection,
    RegistrationAcknowledgement, ReplaceRejection, Replaced, Response, RouteReadiness,
    StartRejection,
};

const FLOW_TABLE_NAME: TableName = TableName::new("flow_nexus_flows");
const FLOW_NATIVE_OWNER_PROOF_TABLE_NAME: TableName =
    TableName::new("flow_nexus_native_owner_proofs");
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
const METAFLOW_TABLE_NAME: TableName = TableName::new("flow_nexus_metaflows");
const METAFLOW_PROCESS_TABLE_NAME: TableName = TableName::new("flow_nexus_metaflow_processes");
const METAFLOW_LOCK_TABLE_NAME: TableName = TableName::new("flow_nexus_metaflow_locks");
const META_NEXUS_CONFIGURATION_TABLE_NAME: TableName =
    TableName::new("flow_nexus_meta_nexus_configuration");
const META_MODEL_CONFIGURATION_TABLE_NAME: TableName =
    TableName::new("flow_nexus_meta_model_configuration");
const META_THRESHOLD_CONFIGURATION_TABLE_NAME: TableName =
    TableName::new("flow_nexus_meta_model_threshold_configuration");
const META_THRESHOLD_CONFIGURATION_FAMILY: &str = "flow-nexus-meta-model-threshold-configuration";
const META_THRESHOLD_CONFIGURATION_SCHEMA: &str =
    "flow-nexus-meta-model-threshold-configuration-v1";
const META_INVESTIGATION_CONFIGURATION_TABLE_NAME: TableName =
    TableName::new("flow_nexus_meta_investigation_configuration");
const META_INVESTIGATION_CONFIGURATION_FAMILY: &str = "flow-nexus-meta-investigation-configuration";
const META_INVESTIGATION_CONFIGURATION_SCHEMA: &str =
    "flow-nexus-meta-investigation-configuration-v2";
const META_DEFAULT_MINUTES_CONFIGURATION_TABLE_NAME: TableName =
    TableName::new("flow_nexus_meta_default_minutes_configuration");
const META_DEFAULT_MINUTES_CONFIGURATION_FAMILY: &str =
    "flow-nexus-meta-default-minutes-configuration";
const META_DEFAULT_MINUTES_CONFIGURATION_SCHEMA: &str =
    "flow-nexus-meta-default-minutes-configuration-v1";
const META_HARNESS_FLAGS_CONFIGURATION_TABLE_NAME: TableName =
    TableName::new("flow_nexus_meta_harness_flags_configuration");
const META_HARNESS_FLAGS_CONFIGURATION_FAMILY: &str = "flow-nexus-meta-harness-flags-configuration";
const META_HARNESS_FLAGS_CONFIGURATION_SCHEMA: &str =
    "flow-nexus-meta-harness-flags-configuration-v2";
const META_HEARTBEAT_CONFIGURATION_TABLE_NAME: TableName =
    TableName::new("flow_nexus_meta_heartbeat_configuration");
const META_HEARTBEAT_CONFIGURATION_FAMILY: &str = "flow-nexus-meta-heartbeat-configuration";
const META_HEARTBEAT_CONFIGURATION_SCHEMA: &str = "flow-nexus-meta-heartbeat-configuration-v2";
const META_PROTECTED_ADDRESS_CONFIGURATION_TABLE_NAME: TableName =
    TableName::new("flow_nexus_meta_protected_address_configuration");
const META_PROTECTED_ADDRESS_CONFIGURATION_FAMILY: &str =
    "flow-nexus-meta-protected-address-configuration";
const META_PROTECTED_ADDRESS_CONFIGURATION_SCHEMA: &str =
    "flow-nexus-meta-protected-address-configuration-v1";
/// The families 0.53.1 added. A store 0.53.0 wrote holds every other
/// family and none of these; it is opened, and each is registered empty.
const ADDED_IN_0531: &[&str] = &["flow_nexus_meta_protected_address_configuration"];
const OPTIONAL_HANDOVER_TABLE: &str = "flow_nexus_meta_handover_configuration";
const OPTIONAL_HANDOVER_RUNTIME_TABLE: &str = "flow_nexus_handovers";
const HEARTBEAT_STANDING_TABLE_NAME: TableName = TableName::new("flow_nexus_heartbeat_standings");
const HEARTBEAT_STANDING_FAMILY: &str = "flow-nexus-heartbeat-standing";
const HEARTBEAT_STANDING_SCHEMA: &str = "flow-nexus-heartbeat-standing-v2";
/// The default Minutes a new Memory holds until Configure declares one:
/// the Minutes Flow gives a request that carries none of its own.
pub(crate) const SEEDED_DEFAULT_MINUTES: i64 = 30;
const META_LEAVE_OUT_CONFIGURATION_TABLE_NAME: TableName =
    TableName::new("flow_nexus_meta_leave_out_configuration");
/// The tables a configuration-only store holds, all of them.
const CONFIGURATION_ONLY_TABLES: &[&str] = &[
    "flow_nexus_state",
    "flow_nexus_configuration",
    "flow_nexus_runtime_configuration",
    "flow_nexus_delivery_configuration",
    "flow_nexus_meta_nexus_configuration",
    "flow_nexus_meta_model_configuration",
    "flow_nexus_meta_leave_out_configuration",
    "flow_nexus_meta_send_server_configuration",
    "flow_nexus_meta_model_threshold_configuration",
    "flow_nexus_meta_investigation_configuration",
    "flow_nexus_meta_default_minutes_configuration",
    "flow_nexus_meta_harness_flags_configuration",
    "flow_nexus_meta_heartbeat_configuration",
    "flow_nexus_meta_metaflow_configuration",
    "flow_nexus_meta_protected_address_configuration",
    "flow_nexus_meta_handover_configuration",
];
/// The tables a running store holds, all of them.
const RUNNING_TABLES: &[&str] = &[
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
    "flow_nexus_deliveries",
    "flow_nexus_pane_leases",
    "flow_nexus_delivery_configuration",
    "flow_nexus_meta_nexus_configuration",
    "flow_nexus_meta_model_configuration",
    "flow_nexus_flow_events",
    "flow_nexus_native_owner_proofs",
    "flow_nexus_flow_context_sizes",
    "flow_nexus_address_holds",
    "flow_nexus_meta_leave_out_configuration",
    "flow_nexus_launch_opened_threads",
    "flow_nexus_send_calls",
    "flow_nexus_launch_cleanups",
    "flow_nexus_meta_send_server_configuration",
    "flow_nexus_queue_delivery_counter",
    "flow_nexus_queue_delivery_requests",
    "flow_nexus_queue_delivery_attempts",
    "flow_nexus_meta_metaflow_configuration",
    "flow_nexus_meta_model_threshold_configuration",
    "flow_nexus_meta_investigation_configuration",
    "flow_nexus_meta_default_minutes_configuration",
    "flow_nexus_meta_harness_flags_configuration",
    "flow_nexus_meta_heartbeat_configuration",
    "flow_nexus_compactions",
    "flow_nexus_reminders",
    "flow_nexus_successor_intents",
    "flow_nexus_send_outcome_counter",
    "flow_nexus_send_outcomes",
    "flow_nexus_logical_delivery_counter",
    "flow_nexus_logical_delivery_current",
    "flow_nexus_logical_delivery_events",
    "flow_nexus_simplified_reply_bindings",
    "flow_nexus_logical_delivery_purposes",
    "flow_nexus_heartbeat_standings",
    "flow_nexus_restarts",
    "flow_nexus_seat_births",
    "flow_nexus_meta_protected_address_configuration",
    "flow_nexus_meta_handover_configuration",
    "flow_nexus_handovers",
];
// These are this Flow source's authored Memory version candidates. They are
// separate from the generic Sema layout version: the registered state-family
// identity and its identity record must both match before a path is resumed.
// Flow Memory v3 begins with 0.52.0, which opens no earlier store.
// Restart (0.53.0) adds one family, RestartRecord; 0.53.0 also opens no earlier store.
pub(crate) const FLOW_SCHEMA_VERSION: u32 = 3;
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
            message_nexus_path_option: None,
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

/// Exact foreground process witnessed at adoption, scoped to this host boot.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
struct NativeOwnerProof {
    process_id: i64,
    process_user_id: i64,
    process_start_token: String,
    host_id: String,
    boot_id: String,
    harness_kind: HarnessKind,
    native_session_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct NativeOwnerEnvironment {
    pub(crate) host_id: String,
    pub(crate) boot_id: String,
}

trait ReadsNativeOwnerEnvironment: Send + Sync {
    fn native_owner_environment(&self) -> Option<NativeOwnerEnvironment>;
}

/// The host and boot the running system reports.
struct SystemNativeOwnerEnvironment;

/// A fixed host and boot, for a store a test opens away from the system's.
#[cfg(test)]
struct FixedNativeOwnerEnvironment(NativeOwnerEnvironment);

#[cfg(test)]
impl ReadsNativeOwnerEnvironment for FixedNativeOwnerEnvironment {
    fn native_owner_environment(&self) -> Option<NativeOwnerEnvironment> {
        Some(self.0.clone())
    }
}

trait ReadsSystemNativeOwnerEnvironment {
    fn system() -> Option<NativeOwnerEnvironment>;
}

impl ReadsNativeOwnerEnvironment for SystemNativeOwnerEnvironment {
    fn native_owner_environment(&self) -> Option<NativeOwnerEnvironment> {
        NativeOwnerEnvironment::system()
    }
}

impl ReadsSystemNativeOwnerEnvironment for NativeOwnerEnvironment {
    fn system() -> Option<NativeOwnerEnvironment> {
        Some(Self {
            host_id: fs::read_to_string("/etc/machine-id")
                .ok()?
                .trim()
                .to_owned(),
            boot_id: fs::read_to_string("/proc/sys/kernel/random/boot_id")
                .ok()?
                .trim()
                .to_owned(),
        })
        .filter(|environment| !environment.host_id.is_empty() && !environment.boot_id.is_empty())
    }
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
struct NativeOwnerProofRecord {
    flow_id: String,
    proof: NativeOwnerProof,
}

impl EngineRecord for NativeOwnerProofRecord {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.flow_id.clone())
    }
}

impl MemoryDefinition for NativeOwnerProofRecord {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(1)
    }

    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label("flow-nexus-native-owner-proof-v1")
    }
}

trait DescribesNativeOwnerProofTable {
    fn descriptor() -> TableDescriptor<NativeOwnerProofRecord>;
}

impl DescribesNativeOwnerProofTable for NativeOwnerProofRecord {
    fn descriptor() -> TableDescriptor<NativeOwnerProofRecord> {
        TableDescriptor::new(
            FLOW_NATIVE_OWNER_PROOF_TABLE_NAME,
            FamilyName::new("flow-nexus-native-owner-proof"),
            NativeOwnerProofRecord::schema_hash(),
        )
    }
}

trait DerivesNativeOwnerProof {
    fn from_binding(
        binding: &meta_signal_flow::FlowBinding,
        environment: &NativeOwnerEnvironment,
    ) -> Option<Self>
    where
        Self: Sized;
    fn current_process_state(
        &self,
        environment: Option<&NativeOwnerEnvironment>,
    ) -> NativeOwnerState;
}

impl DerivesNativeOwnerProof for NativeOwnerProof {
    fn from_binding(
        binding: &meta_signal_flow::FlowBinding,
        environment: &NativeOwnerEnvironment,
    ) -> Option<Self> {
        (!environment.host_id.is_empty() && !environment.boot_id.is_empty()).then(|| Self {
            process_id: binding.process_identity.process_id,
            process_user_id: binding.process_identity.process_user_id,
            process_start_token: binding.process_identity.process_start_token.clone(),
            host_id: environment.host_id.clone(),
            boot_id: environment.boot_id.clone(),
            harness_kind: binding.harness_kind.clone(),
            native_session_id: binding.native_session_id.clone(),
        })
    }

    fn current_process_state(
        &self,
        environment: Option<&NativeOwnerEnvironment>,
    ) -> NativeOwnerState {
        let Some(environment) = environment else {
            return NativeOwnerState::Unknown;
        };
        if environment.host_id != self.host_id || environment.boot_id != self.boot_id {
            return NativeOwnerState::Unknown;
        }
        let Ok(process_id) = u32::try_from(self.process_id) else {
            return NativeOwnerState::Unknown;
        };
        let root = std::path::PathBuf::from(format!("/proc/{process_id}"));
        let metadata = match fs::metadata(&root) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return NativeOwnerState::Dead;
            }
            Err(_) => return NativeOwnerState::Unknown,
        };
        if i64::from(metadata.uid()) != self.process_user_id {
            return NativeOwnerState::Unknown;
        }
        let Ok(stat) = fs::read_to_string(root.join("stat")) else {
            return NativeOwnerState::Unknown;
        };
        let Some((_, fields)) = stat.rsplit_once(") ") else {
            return NativeOwnerState::Unknown;
        };
        let Some(start_token) = fields.split_whitespace().nth(19) else {
            return NativeOwnerState::Unknown;
        };
        if start_token == self.process_start_token {
            NativeOwnerState::Live
        } else {
            NativeOwnerState::Dead
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NativeOwnerState {
    Live,
    Dead,
    Unknown,
}

trait ReadsNativeOwnerProof {
    fn native_owner_proof_record(
        &self,
        flow_id: &str,
    ) -> Result<Option<NativeOwnerProofRecord>, StoreError>;
    fn native_owner_proof(&self, flow_id: &str) -> Result<Option<NativeOwnerProof>, StoreError>;
}

impl ReadsNativeOwnerProof for FlowStore {
    fn native_owner_proof_record(
        &self,
        flow_id: &str,
    ) -> Result<Option<NativeOwnerProofRecord>, StoreError> {
        Ok(self
            .engine
            .match_records(QueryPlan::key(
                self.native_owner_proofs,
                RecordKey::new(flow_id.to_owned()),
            ))?
            .records()
            .first()
            .cloned())
    }

    fn native_owner_proof(&self, flow_id: &str) -> Result<Option<NativeOwnerProof>, StoreError> {
        Ok(self
            .native_owner_proof_record(flow_id)?
            .map(|record| record.proof))
    }
}

impl ReadsNativeOwnerEnvironment for FlowStore {
    fn native_owner_environment(&self) -> Option<NativeOwnerEnvironment> {
        self.native_owner_environment_reader
            .native_owner_environment()
    }
}

/// The durable address-indexed Metaflow defined by `flow_ethos::Metaflow`.
/// Its state and queue are separate from native Flow launch records: an
/// address remains asleep after the process it named is gone.
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
        MemoryVersion::new(7)
    }

    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label("flow-nexus-metaflow-v7")
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

impl MemoryDefinition for StoredMetaflowProcess {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(2)
    }

    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label("flow-nexus-metaflow-process-v2")
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

impl MemoryDefinition for StoredMetaflowLock {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(2)
    }

    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label("flow-nexus-metaflow-lock-v2")
    }
}

/// The native model one Model runs, keyed by the Model.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct StoredMetaModelConfiguration {
    model: ConfigureModel,
}

impl EngineRecord for StoredMetaModelConfiguration {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(FlowStore::model_record_key(&self.model.model))
    }
}

impl MemoryDefinition for StoredMetaModelConfiguration {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(3)
    }

    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label("flow-nexus-meta-model-configuration-v3")
    }
}

/// The model family's descriptor, from v1 on.
pub(crate) trait DescribesModelTable {
    fn descriptor() -> TableDescriptor<StoredMetaModelConfiguration>;
}

impl DescribesModelTable for StoredMetaModelConfiguration {
    fn descriptor() -> TableDescriptor<StoredMetaModelConfiguration> {
        TableDescriptor::new(
            META_MODEL_CONFIGURATION_TABLE_NAME,
            FamilyName::new("flow-nexus-meta-model-configuration"),
            StoredMetaModelConfiguration::schema_hash(),
        )
    }
}

/// One model's refresh threshold, keyed by the model's name.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
struct StoredMetaThresholdConfiguration {
    threshold: ConfigureThreshold,
}

impl EngineRecord for StoredMetaThresholdConfiguration {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.threshold.model_name.clone())
    }
}

trait DescribesThresholdTable {
    fn descriptor() -> TableDescriptor<StoredMetaThresholdConfiguration>;
}

impl DescribesThresholdTable for StoredMetaThresholdConfiguration {
    fn descriptor() -> TableDescriptor<StoredMetaThresholdConfiguration> {
        TableDescriptor::new(
            META_THRESHOLD_CONFIGURATION_TABLE_NAME,
            FamilyName::new(META_THRESHOLD_CONFIGURATION_FAMILY),
            SchemaHash::for_label(META_THRESHOLD_CONFIGURATION_SCHEMA),
        )
    }
}

/// The Address of the Investigation seat, one row.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
struct StoredMetaInvestigationConfiguration {
    investigation: Address,
}

impl EngineRecord for StoredMetaInvestigationConfiguration {
    fn record_key(&self) -> RecordKey {
        RecordKey::new("investigation")
    }
}

impl MemoryDefinition for StoredMetaInvestigationConfiguration {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(2)
    }

    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label(META_INVESTIGATION_CONFIGURATION_SCHEMA)
    }
}

trait DescribesInvestigationTable {
    fn descriptor() -> TableDescriptor<StoredMetaInvestigationConfiguration>;
}

impl DescribesInvestigationTable for StoredMetaInvestigationConfiguration {
    fn descriptor() -> TableDescriptor<StoredMetaInvestigationConfiguration> {
        TableDescriptor::new(
            META_INVESTIGATION_CONFIGURATION_TABLE_NAME,
            FamilyName::new(META_INVESTIGATION_CONFIGURATION_FAMILY),
            SchemaHash::for_label(META_INVESTIGATION_CONFIGURATION_SCHEMA),
        )
    }
}

/// The default Minutes, one row; absent until Configure declares one.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
struct StoredMetaDefaultMinutesConfiguration {
    default_minutes: i64,
}

impl EngineRecord for StoredMetaDefaultMinutesConfiguration {
    fn record_key(&self) -> RecordKey {
        RecordKey::new("default-minutes")
    }
}

trait DescribesDefaultMinutesTable {
    fn descriptor() -> TableDescriptor<StoredMetaDefaultMinutesConfiguration>;
}

impl DescribesDefaultMinutesTable for StoredMetaDefaultMinutesConfiguration {
    fn descriptor() -> TableDescriptor<StoredMetaDefaultMinutesConfiguration> {
        TableDescriptor::new(
            META_DEFAULT_MINUTES_CONFIGURATION_TABLE_NAME,
            FamilyName::new(META_DEFAULT_MINUTES_CONFIGURATION_FAMILY),
            SchemaHash::for_label(META_DEFAULT_MINUTES_CONFIGURATION_SCHEMA),
        )
    }
}

/// Extra native harness arguments for one metaflow, keyed by Address. An
/// absent row means the empty argument string.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
struct StoredMetaHarnessFlagsConfiguration {
    configured: ConfigureHarnessFlags,
}

impl EngineRecord for StoredMetaHarnessFlagsConfiguration {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(FlowStore::address_record_key(&self.configured.address))
    }
}

impl MemoryDefinition for StoredMetaHarnessFlagsConfiguration {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(2)
    }

    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label(META_HARNESS_FLAGS_CONFIGURATION_SCHEMA)
    }
}

trait DescribesHarnessFlagsTable {
    fn descriptor() -> TableDescriptor<StoredMetaHarnessFlagsConfiguration>;
}

impl DescribesHarnessFlagsTable for StoredMetaHarnessFlagsConfiguration {
    fn descriptor() -> TableDescriptor<StoredMetaHarnessFlagsConfiguration> {
        TableDescriptor::new(
            META_HARNESS_FLAGS_CONFIGURATION_TABLE_NAME,
            FamilyName::new(META_HARNESS_FLAGS_CONFIGURATION_FAMILY),
            Self::schema_hash(),
        )
    }
}

/// The configured sampling heartbeat; no row means disabled.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
struct StoredMetaHeartbeatConfiguration {
    heartbeat: HeartbeatConfig,
}

impl EngineRecord for StoredMetaHeartbeatConfiguration {
    fn record_key(&self) -> RecordKey {
        RecordKey::new("heartbeat")
    }
}

impl MemoryDefinition for StoredMetaHeartbeatConfiguration {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(2)
    }

    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label(META_HEARTBEAT_CONFIGURATION_SCHEMA)
    }
}

trait DescribesHeartbeatTable {
    fn descriptor() -> TableDescriptor<StoredMetaHeartbeatConfiguration>;
}

impl DescribesHeartbeatTable for StoredMetaHeartbeatConfiguration {
    fn descriptor() -> TableDescriptor<StoredMetaHeartbeatConfiguration> {
        TableDescriptor::new(
            META_HEARTBEAT_CONFIGURATION_TABLE_NAME,
            FamilyName::new(META_HEARTBEAT_CONFIGURATION_FAMILY),
            SchemaHash::for_label(META_HEARTBEAT_CONFIGURATION_SCHEMA),
        )
    }
}

/// One protected Address, one row per Address; none by default.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
struct StoredMetaProtectedAddressConfiguration {
    address: Address,
}

impl EngineRecord for StoredMetaProtectedAddressConfiguration {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(FlowStore::address_record_key(&self.address))
    }
}

impl MemoryDefinition for StoredMetaProtectedAddressConfiguration {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(1)
    }

    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label(META_PROTECTED_ADDRESS_CONFIGURATION_SCHEMA)
    }
}

trait DescribesProtectedAddressTable {
    fn descriptor() -> TableDescriptor<StoredMetaProtectedAddressConfiguration>;
}

impl DescribesProtectedAddressTable for StoredMetaProtectedAddressConfiguration {
    fn descriptor() -> TableDescriptor<StoredMetaProtectedAddressConfiguration> {
        TableDescriptor::new(
            META_PROTECTED_ADDRESS_CONFIGURATION_TABLE_NAME,
            FamilyName::new(META_PROTECTED_ADDRESS_CONFIGURATION_FAMILY),
            SchemaHash::for_label(META_PROTECTED_ADDRESS_CONFIGURATION_SCHEMA),
        )
    }
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct StoredHeartbeatStanding {
    pub address: Address,
    pub baseline_awake: i64,
    pub baseline_working: Option<i64>,
    pub baseline_working_complete: bool,
    pub baseline_sent: i64,
    pub baseline_queue: i64,
    pub latest_awake: i64,
    pub latest_working: Option<i64>,
    pub latest_working_complete: bool,
    pub latest_sent: i64,
    pub latest_queue: i64,
    pub outstanding: Option<String>,
}
pub(crate) trait JudgesHeartbeatStanding {
    fn changed(&self) -> bool;
}
impl JudgesHeartbeatStanding for StoredHeartbeatStanding {
    fn changed(&self) -> bool {
        self.baseline_awake != self.latest_awake
            || self.baseline_sent != self.latest_sent
            || self.baseline_queue != self.latest_queue
    }
}
impl EngineRecord for StoredHeartbeatStanding {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(FlowStore::address_record_key(&self.address))
    }
}

impl MemoryDefinition for StoredHeartbeatStanding {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(2)
    }
    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label(HEARTBEAT_STANDING_SCHEMA)
    }
}
trait DescribesHeartbeatStandingTable {
    fn descriptor() -> TableDescriptor<StoredHeartbeatStanding>;
}
impl DescribesHeartbeatStandingTable for StoredHeartbeatStanding {
    fn descriptor() -> TableDescriptor<StoredHeartbeatStanding> {
        TableDescriptor::new(
            HEARTBEAT_STANDING_TABLE_NAME,
            FamilyName::new(HEARTBEAT_STANDING_FAMILY),
            Self::schema_hash(),
        )
    }
}

/// One metaflow's default leave-outs on one harness, keyed by its Address
/// and that harness.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct StoredMetaLeaveOutConfiguration {
    leave_out: ConfigureLeaveOut,
}

impl EngineRecord for StoredMetaLeaveOutConfiguration {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(FlowStore::leave_out_record_key(
            &self.leave_out.address,
            &self.leave_out.harness_kind,
        ))
    }
}

impl MemoryDefinition for StoredMetaLeaveOutConfiguration {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(3)
    }

    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label("flow-nexus-meta-leave-out-configuration-v3")
    }
}

/// The native model a seat runs is the one configured for its Address's
/// Model, named exactly; nothing else is launched.
pub(crate) trait NamesConfiguredModel {
    /// The native model configured for the Address's Model, if any.
    fn configured_model(&self, address: &Address) -> Result<Option<String>, StoreError>;
}

impl NamesConfiguredModel for FlowStore {
    fn configured_model(&self, address: &Address) -> Result<Option<String>, StoreError> {
        let key = RecordKey::new(FlowStore::model_record_key(&address.model));
        Ok(self
            .engine
            .match_records(QueryPlan::key(self.meta_configuration_tables.models, key))?
            .records()
            .first()
            .map(|stored| stored.model.native.clone()))
    }
}

/// The leave-out family's descriptor, from v1 on.
pub(crate) trait DescribesLeaveOutTable {
    fn descriptor() -> TableDescriptor<StoredMetaLeaveOutConfiguration>;
}

impl DescribesLeaveOutTable for StoredMetaLeaveOutConfiguration {
    fn descriptor() -> TableDescriptor<StoredMetaLeaveOutConfiguration> {
        TableDescriptor::new(
            META_LEAVE_OUT_CONFIGURATION_TABLE_NAME,
            FamilyName::new("flow-nexus-meta-leave-out-configuration"),
            StoredMetaLeaveOutConfiguration::schema_hash(),
        )
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

/// A flow's role: its aspect and model, keyed by its FlowId. It is
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

/// Seeds a flow's role directly, for tests of what reads its model.
#[cfg(test)]
pub(crate) trait SeedsRole {
    fn seed_role(&self, role: Caller);
}

#[cfg(test)]
impl SeedsRole for FlowStore {
    fn seed_role(&self, role: Caller) {
        self.engine
            .assert(Assertion::new(self.roles, StoredRole { caller: role }))
            .expect("role seeded");
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

/// The socket half of the meta `Configuration`.
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
            message_nexus_path: delivery
                .message_nexus_path_option
                .clone()
                .unwrap_or_default(),
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

/// A launch attempt; its profile names its seat's context by module
/// identity.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct StoredLaunchAttempt {
    attempt: LaunchAttempt,
}

impl EngineRecord for StoredLaunchAttempt {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.attempt.launch_request_id.clone())
    }
}

impl MemoryDefinition for StoredLaunchAttempt {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(5)
    }

    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label("flow-nexus-launch-attempt-v5")
    }
}

// How a launch request settled, and a replacement under way: the record
// types the Memory root declares (`ethos/memory.ethos`).
pub use crate::generated::memory::{LaunchOutcome, Replacement};

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
        MemoryVersion::new(9)
    }

    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label("flow-nexus-launch-outcome-v9")
    }
}

/// A launch request that replaces a predecessor. While it stands without a
/// Replaced outcome, the flow its launch binds is held out of routing.
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
    #[error("handover configuration refused: {0:?}")]
    HandoverConfigurationRefused(meta_signal_flow::HandoverConfigurationRefusal),
    #[error("heartbeat period must be positive")]
    InvalidHeartbeatPeriod,
    #[error("store path must be an absolute file path")]
    InvalidStorePath,
    #[error("meta configuration is not configured")]
    NotConfigured,
    #[error("the executable default configuration is missing or invalid")]
    MissingDefaultConfiguration,
    #[error("a Codex model names both the stable and the next endpoint")]
    CodexModelsOverlap,
    #[error("a leave-out default names a leave-out of another harness than its own")]
    LeaveOutNotForHarness,
    #[error("the topic {0} is not an uncapitalized camelCase identifier")]
    TopicRefused(String),
    #[error("the default Minutes is refused: {0:?}")]
    MinutesRefused(signal_flow::MinutesRefusal),
    #[error("the counters are refused: {0:?}")]
    CountersRefused(CountersRefusal),
    #[error("store {path} is held by another open; its lock cannot be acquired")]
    StoreHeld { path: String },
    #[error("the store's open could not be staged: {0}")]
    StagedOpen(String),
    #[error("store {path} is not a recognized Flow Memory v3 store: {reason}")]
    UnrecognizedMemory { path: String, reason: String },
    /// The store is an earlier Flow's, of Flow Memory `version`: this Flow
    /// opens no earlier store, and leaves it as it is.
    #[error("store {path} is an earlier Flow's Memory v{version}; this Flow opens Memory v3 alone")]
    EarlierMemory { path: String, version: u64 },
}

pub(crate) struct MetaConfigurationTables {
    handover: TableReference<handover_configuration::StoredHandoverConfiguration>,
    nexus: TableReference<StoredMetaNexusConfiguration>,
    models: TableReference<StoredMetaModelConfiguration>,
    thresholds: TableReference<StoredMetaThresholdConfiguration>,
    leave_outs: TableReference<StoredMetaLeaveOutConfiguration>,
    pub(crate) send_servers: TableReference<send_server::StoredMetaSendServerConfiguration>,
    pub(crate) metaflows: TableReference<metaflow_declaration::StoredMetaMetaflowConfiguration>,
    investigation: TableReference<StoredMetaInvestigationConfiguration>,
    default_minutes: TableReference<StoredMetaDefaultMinutesConfiguration>,
    harness_flags: TableReference<StoredMetaHarnessFlagsConfiguration>,
    heartbeat: TableReference<StoredMetaHeartbeatConfiguration>,
    protected_addresses: TableReference<StoredMetaProtectedAddressConfiguration>,
}

trait RegistersMetaConfigurationTables: Sized {
    fn register(engine: &mut Engine) -> Result<Self, StoreError>;
}

impl RegistersMetaConfigurationTables for MetaConfigurationTables {
    fn register(engine: &mut Engine) -> Result<Self, StoreError> {
        Ok(Self {
            handover: engine.register_table(<handover_configuration::StoredHandoverConfiguration as handover_configuration::DescribesHandoverConfiguration>::descriptor())?,
            nexus: engine.register_table(StoredMetaNexusConfiguration::descriptor())?,
            models: engine.register_table(StoredMetaModelConfiguration::descriptor())?,
            thresholds: engine.register_table(StoredMetaThresholdConfiguration::descriptor())?,
            leave_outs: engine.register_table(StoredMetaLeaveOutConfiguration::descriptor())?,
            send_servers: engine.register_table(
                <send_server::StoredMetaSendServerConfiguration as send_server::DescribesSendServerTable>::descriptor(),
            )?,
            investigation: engine
                .register_table(StoredMetaInvestigationConfiguration::descriptor())?,
            default_minutes: engine
                .register_table(StoredMetaDefaultMinutesConfiguration::descriptor())?,
            harness_flags: engine
                .register_table(StoredMetaHarnessFlagsConfiguration::descriptor())?,
            heartbeat: engine.register_table(StoredMetaHeartbeatConfiguration::descriptor())?,
            protected_addresses: engine
                .register_table(StoredMetaProtectedAddressConfiguration::descriptor())?,
            metaflows: engine.register_table(
                <metaflow_declaration::StoredMetaMetaflowConfiguration as metaflow_declaration::DescribesMetaflowDeclarationTable>::descriptor(),
            )?,
        })
    }
}

pub struct FlowStore {
    engine: Engine,
    native_owner_environment_reader: Arc<dyn ReadsNativeOwnerEnvironment>,
    flows: TableReference<FlowRecord>,
    native_owner_proofs: TableReference<NativeOwnerProofRecord>,
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
    delivery_tables: DeliveryTables,
    event_tables: events::EventTables,
    context_sizes: TableReference<context::FlowContextSize>,
    address_holds: TableReference<address_hold::AddressHold>,
    opened_threads: TableReference<opened_thread::OpenedLaunchThread>,
    send_calls: TableReference<crate::generated::send_tool::SendCall>,
    launch_cleanups: TableReference<launch_cleanup::LaunchCleanup>,
    compactions: TableReference<compaction::Compaction>,
    reminders: TableReference<reminder::Reminder>,
    successor_intents: TableReference<successor::SuccessorIntent>,
    queue_delivery: QueueDeliveryTables,
    send_outcomes: SendOutcomeTables,
    logical_deliveries: LogicalDeliveryTables,
    heartbeat_standings: TableReference<StoredHeartbeatStanding>,
    restarts: TableReference<restart::StoredRestart>,
    seat_births: TableReference<crate::generated::memory::SeatBirth>,
    handovers: TableReference<handover::HandoverMemory>,
    pub launch_changes: LaunchChanges,
}

pub(crate) trait TracksHeartbeat {
    fn heartbeat_standings(&self) -> Result<Vec<StoredHeartbeatStanding>, StoreError>;
    fn replace_heartbeat_standings(
        &self,
        standings: Vec<StoredHeartbeatStanding>,
    ) -> Result<(), StoreError>;
}
impl TracksHeartbeat for FlowStore {
    fn heartbeat_standings(&self) -> Result<Vec<StoredHeartbeatStanding>, StoreError> {
        Ok(self
            .engine
            .match_records(QueryPlan::all(self.heartbeat_standings))?
            .records()
            .to_vec())
    }
    fn replace_heartbeat_standings(
        &self,
        standings: Vec<StoredHeartbeatStanding>,
    ) -> Result<(), StoreError> {
        let held = self.heartbeat_standings()?;
        let mut commit = self.engine.begin_atomic_commit();
        for row in &held {
            if !standings
                .iter()
                .any(|next| next.record_key() == row.record_key())
            {
                commit = commit.retract(self.heartbeat_standings, row.record_key());
            }
        }
        for row in standings {
            commit = match held.iter().find(|old| old.record_key() == row.record_key()) {
                Some(old) if old == &row => commit,
                Some(_) => commit.mutate(self.heartbeat_standings, row),
                None => commit.assert(self.heartbeat_standings, row),
            };
        }
        self.engine.commit_atomic(commit)?;
        Ok(())
    }
}

#[cfg(test)]
pub(crate) trait SetsNativeOwnerEnvironmentForTests {
    fn set_native_owner_environment_for_tests(&mut self, environment: NativeOwnerEnvironment);
}

#[cfg(test)]
impl SetsNativeOwnerEnvironmentForTests for FlowStore {
    fn set_native_owner_environment_for_tests(&mut self, environment: NativeOwnerEnvironment) {
        self.native_owner_environment_reader = Arc::new(FixedNativeOwnerEnvironment(environment));
    }
}

/// Every configuration family a store opened for configuration only
/// holds: the Configuration snapshot but for the counters, which only the
/// running store holds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfiguredFamilies {
    pub nexus: ConfigureNexus,
    pub models: Vec<ConfigureModel>,
    pub thresholds: Vec<ConfigureThreshold>,
    pub default_leave_outs: Vec<ConfigureLeaveOut>,
    pub send_servers: Vec<ConfigureSendServer>,
    pub metaflows: Vec<ConfigureMetaflow>,
    pub investigation: Option<Address>,
    pub default_minutes: i64,
    pub configured_harness_flags: Vec<ConfigureHarnessFlags>,
    pub heartbeat: Option<HeartbeatConfig>,
    pub protected_addresses: Vec<Address>,
    pub handover_policies: meta_signal_flow::HandoverPoliciesConfiguration,
    pub handover_archive: meta_signal_flow::HandoverArchiveConfiguration,
    pub handover_count: meta_signal_flow::HandoverCountConfiguration,
}

pub trait CompletesSnapshot {
    /// The Configuration snapshot these families make with the counters.
    fn with_counters(self, counters: ConfigureCounters) -> ConfigurationSnapshot;
}

impl CompletesSnapshot for ConfiguredFamilies {
    fn with_counters(self, counters: ConfigureCounters) -> ConfigurationSnapshot {
        ConfigurationSnapshot {
            nexus: self.nexus,
            models: self.models,
            thresholds: self.thresholds,
            default_leave_outs: self.default_leave_outs,
            send_servers: self.send_servers,
            metaflows: self.metaflows,
            investigation: self.investigation,
            default_minutes: self.default_minutes,
            configured_harness_flags: self.configured_harness_flags,
            heartbeat: self.heartbeat,
            protected_addresses: self.protected_addresses,
            handover_policies: self.handover_policies,
            handover_archive: self.handover_archive,
            handover_count: self.handover_count,
            counters,
        }
    }
}

/// The one-request configuration listener's view of the same Sema store.
/// It registers only the rows Configure owns and never seeds the rest of
/// Flow state or settles launches/deliveries.
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
    /// The open itself, on the store at `path` and in its place.
    fn open_in_place(path: &Path) -> Result<Self, StoreError>;
    fn configuration_snapshot(&self) -> Result<Option<ConfiguredFamilies>, StoreError>;
    fn configure_change(&self, change: ConfigurationChange)
    -> Result<ConfigureOutcome, StoreError>;
    fn configuration(&self, defaults: &DefaultConfiguration) -> Result<Configuration, StoreError>;
    fn configure(&self, configuration: Configuration) -> Result<Configuration, StoreError>;
}

impl ConfiguresOnlyStore for ConfigurationOnlyStore {
    /// Opens the same versioned store as normal startup without invoking its
    /// seeding or settlement path. A store this Flow did not write, an
    /// earlier Flow's among them, is refused before anything is written.
    fn open(path: &Path) -> Result<Self, StoreError> {
        Self::open_in_place(path)
    }

    fn open_in_place(path: &Path) -> Result<Self, StoreError> {
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
        let delivery_configuration_descriptor = TableDescriptor::<DeliveryConfiguration>::new(
            delivery::DELIVERY_CONFIGURATION_TABLE_NAME,
            FamilyName::new("flow-nexus-delivery-configuration"),
            DeliveryConfiguration::schema_hash(),
        );
        let meta_nexus_descriptor = StoredMetaNexusConfiguration::descriptor();
        let meta_models_descriptor = StoredMetaModelConfiguration::descriptor();
        let meta_thresholds_descriptor = StoredMetaThresholdConfiguration::descriptor();
        let meta_investigation_descriptor = StoredMetaInvestigationConfiguration::descriptor();
        let meta_handover_descriptor = <handover_configuration::StoredHandoverConfiguration as handover_configuration::DescribesHandoverConfiguration>::descriptor();
        let meta_default_minutes_descriptor = StoredMetaDefaultMinutesConfiguration::descriptor();
        let meta_harness_flags_descriptor = StoredMetaHarnessFlagsConfiguration::descriptor();
        let meta_heartbeat_descriptor = StoredMetaHeartbeatConfiguration::descriptor();
        let meta_protected_addresses_descriptor =
            StoredMetaProtectedAddressConfiguration::descriptor();
        let meta_leave_outs_descriptor = StoredMetaLeaveOutConfiguration::descriptor();
        let meta_send_servers_descriptor = <send_server::StoredMetaSendServerConfiguration as send_server::DescribesSendServerTable>::descriptor();
        let meta_metaflows_descriptor = <metaflow_declaration::StoredMetaMetaflowConfiguration as metaflow_declaration::DescribesMetaflowDeclarationTable>::descriptor();
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
                .with_family(&meta_leave_outs_descriptor)
                .with_family(&meta_send_servers_descriptor)
                .with_family(&meta_investigation_descriptor)
                .with_family(&meta_default_minutes_descriptor)
                .with_family(&meta_harness_flags_descriptor)
                .with_family(&meta_heartbeat_descriptor)
                .with_family(&meta_protected_addresses_descriptor)
                .with_family(&meta_metaflows_descriptor)
                .with_family(&meta_handover_descriptor),
        )?;
        let state = engine.register_table(state_descriptor)?;
        let configuration = engine.register_table(configuration_descriptor)?;
        let runtime_configuration = engine.register_table(runtime_configuration_descriptor)?;
        let delivery_configuration = engine.register_table(delivery_configuration_descriptor)?;
        let meta_configuration_tables = MetaConfigurationTables {
            handover: engine.register_table(<handover_configuration::StoredHandoverConfiguration as handover_configuration::DescribesHandoverConfiguration>::descriptor())?,
            nexus: engine.register_table(meta_nexus_descriptor)?,
            models: engine.register_table(meta_models_descriptor)?,
            thresholds: engine.register_table(meta_thresholds_descriptor)?,
            leave_outs: engine.register_table(meta_leave_outs_descriptor)?,
            send_servers: engine.register_table(meta_send_servers_descriptor)?,
            investigation: engine.register_table(meta_investigation_descriptor)?,
            default_minutes: engine.register_table(meta_default_minutes_descriptor)?,
            harness_flags: engine.register_table(meta_harness_flags_descriptor)?,
            heartbeat: engine.register_table(meta_heartbeat_descriptor)?,
            protected_addresses: engine.register_table(meta_protected_addresses_descriptor)?,
            metaflows: engine.register_table(meta_metaflows_descriptor)?,
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

    fn configuration_snapshot(&self) -> Result<Option<ConfiguredFamilies>, StoreError> {
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
pub struct PendingLaunch {
    pub flow_id: String,
    pub origin: Origin,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FlowRegistration {
    Registered(Box<FlowNode>),
    ConflictingBinding,
}

/// A witnessed existing seat and its Address became one durable binding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ExistingSeatAdoption {
    Bound(Box<FlowNode>),
    Refused(meta_signal_flow::FlowBindingRefusalReason),
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
    /// seeding defaults or settling prior work. A store an earlier Flow
    /// wrote is refused, never upgraded.
    fn open_configured(path: &Path) -> Result<Self, StoreError>
    where
        Self: Sized;

    /// The configured open itself, on the store at `path` and in its place.
    fn open_configured_in_place(path: &Path) -> Result<Self, StoreError>
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
pub(crate) trait RegistersExistingFlow {
    fn adopt_existing_flow(
        &self,
        flow_node: FlowNode,
        role: Caller,
        address: Address,
        binding: &meta_signal_flow::FlowBinding,
    ) -> Result<ExistingSeatAdoption, StoreError>;
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
    /// The profile of a launch, which a later seat can be composed from
    /// again.
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
}

/// A flow whose harness goes on in a new native session: Claude's /clear
/// ends one session and starts the next in the same harness. The FlowId
/// stays.
pub trait RebindsNativeSession {
    /// The live flow `flow_id`, bound to the session `previous`, is bound
    /// to `session` now: its row and its owner proof name the new session,
    /// in one write. False, and nothing written, when the flow is not live
    /// or is not bound to `previous`.
    fn rebind_native_session(
        &self,
        flow_id: &str,
        previous: &str,
        session: &str,
    ) -> Result<bool, StoreError>;
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
    /// Writes the attempt's next state, keeping what its row knows of the
    /// context its seat started with.
    fn mutate_launch_attempt(&self, stored: StoredLaunchAttempt) -> Result<(), StoreError>;
}

impl OpensFlowStore for FlowStore {
    /// The first running open of a configuration-only store adds the
    /// running families on a staged copy that replaces the store only once
    /// the whole open has succeeded.
    fn open_configured(path: &Path) -> Result<Self, StoreError> {
        if !FlowStore::recognize_flow_memory(path)? {
            return Err(StoreError::NotConfigured);
        }
        StagedOpen::run(path, |copy| Self::open_configured_in_place(copy).map(drop))?;
        Self::open_configured_in_place(path)
    }

    fn open_configured_in_place(path: &Path) -> Result<Self, StoreError> {
        if !FlowStore::recognize_flow_memory(path)? {
            return Err(StoreError::NotConfigured);
        }

        let configuration_only = ConfigurationOnlyStore::open_in_place(path)?;
        if configuration_only.configuration_snapshot()?.is_none() {
            return Err(StoreError::NotConfigured);
        }
        drop(configuration_only);

        let flows_descriptor = TableDescriptor::<FlowRecord>::new(
            FLOW_TABLE_NAME,
            FamilyName::new("flow-nexus-flow"),
            FlowRecord::schema_hash(),
        );
        let native_owner_proofs_descriptor = NativeOwnerProofRecord::descriptor();
        let state_descriptor = TableDescriptor::<FlowStoreState>::new(
            FLOW_STATE_TABLE_NAME,
            FamilyName::new("flow-nexus-state"),
            SchemaHash::for_label("flow-nexus-state-v2"),
        );
        let metaflows_descriptor = TableDescriptor::<StoredMetaflow>::new(
            METAFLOW_TABLE_NAME,
            FamilyName::new("flow-nexus-metaflow"),
            StoredMetaflow::schema_hash(),
        );
        let metaflow_processes_descriptor = TableDescriptor::<StoredMetaflowProcess>::new(
            METAFLOW_PROCESS_TABLE_NAME,
            FamilyName::new("flow-nexus-metaflow-process"),
            StoredMetaflowProcess::schema_hash(),
        );
        let metaflow_locks_descriptor = TableDescriptor::<StoredMetaflowLock>::new(
            METAFLOW_LOCK_TABLE_NAME,
            FamilyName::new("flow-nexus-metaflow-lock"),
            StoredMetaflowLock::schema_hash(),
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
        );
        let launch_panes_descriptor = TableDescriptor::<StoredLaunchPane>::new(
            FLOW_LAUNCH_PANE_TABLE_NAME,
            FamilyName::new("flow-nexus-launch-pane"),
            SchemaHash::for_label("flow-nexus-launch-pane-v1"),
        );
        let launch_outcomes_descriptor = TableDescriptor::<StoredLaunchOutcome>::new(
            FLOW_LAUNCH_OUTCOME_TABLE_NAME,
            FamilyName::new("flow-nexus-launch-outcome"),
            StoredLaunchOutcome::schema_hash(),
        );
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
            DeliveryConfiguration::schema_hash(),
        );
        let meta_nexus_descriptor = StoredMetaNexusConfiguration::descriptor();
        let meta_models_descriptor = StoredMetaModelConfiguration::descriptor();
        let meta_thresholds_descriptor = StoredMetaThresholdConfiguration::descriptor();
        let meta_investigation_descriptor = StoredMetaInvestigationConfiguration::descriptor();
        let meta_handover_descriptor = <handover_configuration::StoredHandoverConfiguration as handover_configuration::DescribesHandoverConfiguration>::descriptor();
        let compactions_descriptor =
            <compaction::Compaction as compaction::DescribesCompactionTable>::descriptor();
        let reminders_descriptor =
            <reminder::Reminder as reminder::DescribesReminderTable>::descriptor();
        let successor_intents_descriptor =
            <successor::SuccessorIntent as successor::DescribesSuccessorIntentTable>::descriptor();
        let meta_leave_outs_descriptor = StoredMetaLeaveOutConfiguration::descriptor();
        let events_descriptor = <events::FlowEvents as events::DescribesEventTable>::descriptor();
        let context_sizes_descriptor =
            <context::FlowContextSize as context::DescribesContextSizeTable>::descriptor();
        let address_holds_descriptor =
            <address_hold::AddressHold as address_hold::DescribesAddressHoldTable>::descriptor();
        let opened_threads_descriptor =
            <opened_thread::OpenedLaunchThread as opened_thread::DescribesOpenedThreadTable>::descriptor(
            );
        let send_calls_descriptor = <crate::generated::send_tool::SendCall as send_call::DescribesSendCallTable>::descriptor();
        let launch_cleanups_descriptor =
            <launch_cleanup::LaunchCleanup as launch_cleanup::DescribesLaunchCleanupTable>::descriptor();
        let queue_delivery_descriptors = QueueDeliveryTables::descriptors();
        let send_outcome_descriptors = SendOutcomeTables::descriptors();
        let logical_delivery_descriptors = LogicalDeliveryTables::descriptors();
        let heartbeat_standing_descriptor = StoredHeartbeatStanding::descriptor();
        let restart_descriptor =
            <FlowStore as restart::DescribesRestartTables>::restart_descriptor();
        let seat_birth_descriptor =
            <FlowStore as restart::DescribesRestartTables>::seat_birth_descriptor();
        let mut engine = Engine::open(
            EngineOpen::new(path, SchemaVersion::new(FLOW_SCHEMA_VERSION))
                .with_family(&flows_descriptor)
                .with_family(&native_owner_proofs_descriptor)
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
                .with_family(&delivery_descriptor)
                .with_family(&pane_lease_descriptor)
                .with_family(&delivery_configuration_descriptor)
                .with_family(&meta_nexus_descriptor)
                .with_family(&meta_models_descriptor)
                .with_family(&meta_thresholds_descriptor)
                .with_family(&meta_leave_outs_descriptor)
                .with_family(&<send_server::StoredMetaSendServerConfiguration as send_server::DescribesSendServerTable>::descriptor())
                .with_family(&meta_investigation_descriptor)
                .with_family(&StoredMetaDefaultMinutesConfiguration::descriptor())
                .with_family(&StoredMetaHarnessFlagsConfiguration::descriptor())
                .with_family(&StoredMetaHeartbeatConfiguration::descriptor())
                .with_family(&StoredMetaProtectedAddressConfiguration::descriptor())
                .with_family(&meta_handover_descriptor)
                .with_family(&compactions_descriptor)
                .with_family(&reminders_descriptor)
                .with_family(&successor_intents_descriptor)
                .with_family(&<metaflow_declaration::StoredMetaMetaflowConfiguration as metaflow_declaration::DescribesMetaflowDeclarationTable>::descriptor())
                .with_family(&events_descriptor)
                .with_family(&context_sizes_descriptor)
                .with_family(&address_holds_descriptor)
                .with_family(&opened_threads_descriptor)
                .with_family(&send_calls_descriptor)
                .with_family(&launch_cleanups_descriptor)
                .with_family(&queue_delivery_descriptors.0)
                .with_family(&queue_delivery_descriptors.1)
                .with_family(&queue_delivery_descriptors.2)
                .with_family(&send_outcome_descriptors.counter)
                .with_family(&send_outcome_descriptors.outcomes)
                .with_family(&logical_delivery_descriptors.counter)
                .with_family(&logical_delivery_descriptors.current)
                .with_family(&logical_delivery_descriptors.events)
                .with_family(&logical_delivery_descriptors.simplified_replies)
                .with_family(&logical_delivery_descriptors.purposes)
                .with_family(&heartbeat_standing_descriptor)
                .with_family(&restart_descriptor)
                .with_family(&seat_birth_descriptor)
                .with_family(&<handover::HandoverMemory as handover::DescribesHandoverTable>::descriptor()),
        )?;
        let flows = engine.register_table(flows_descriptor)?;
        let native_owner_proofs = engine.register_table(native_owner_proofs_descriptor)?;
        let state = engine.register_table(state_descriptor)?;
        let metaflows = engine.register_table(metaflows_descriptor)?;
        let metaflow_processes = engine.register_table(metaflow_processes_descriptor)?;
        let metaflow_locks = engine.register_table(metaflow_locks_descriptor)?;
        let configuration = engine.register_table(configuration_descriptor)?;
        let runtime_configuration = engine.register_table(runtime_configuration_descriptor)?;
        let herdr_routes = engine.register_table(herdr_routes_descriptor)?;
        let launch_attempts = engine.register_table(launch_attempts_descriptor)?;
        let launch_panes = engine.register_table(launch_panes_descriptor)?;
        let launch_outcomes = engine.register_table(launch_outcomes_descriptor)?;
        let replacements = engine.register_table(replacements_descriptor)?;
        let roles = engine.register_table(roles_descriptor)?;
        let delivery_tables = DeliveryTables::register(&mut engine)?;
        let meta_configuration_tables = MetaConfigurationTables::register(&mut engine)?;
        let event_tables =
            <events::EventTables as events::RegistersEventTables>::register(&mut engine)?;
        let context_sizes = engine.register_table(context_sizes_descriptor)?;
        let address_holds = engine.register_table(address_holds_descriptor)?;
        let opened_threads = engine.register_table(opened_threads_descriptor)?;
        let send_calls = engine.register_table(send_calls_descriptor)?;
        let launch_cleanups = engine.register_table(launch_cleanups_descriptor)?;
        let compactions = engine.register_table(compactions_descriptor)?;
        let reminders = engine.register_table(reminders_descriptor)?;
        let successor_intents = engine.register_table(successor_intents_descriptor)?;
        let queue_delivery =
            QueueDeliveryTables::register(&mut engine, queue_delivery_descriptors)?;
        let send_outcomes = SendOutcomeTables::register(&mut engine, send_outcome_descriptors)?;
        let logical_deliveries =
            LogicalDeliveryTables::register(&mut engine, logical_delivery_descriptors)?;
        let heartbeat_standings = engine.register_table(heartbeat_standing_descriptor)?;
        let restarts = engine.register_table(restart_descriptor)?;
        let seat_births = engine.register_table(seat_birth_descriptor)?;
        let handovers =
            <FlowStore as handover::RegistersHandoverTable>::register_handover_table(&mut engine)?;
        let store = Self {
            engine,
            native_owner_environment_reader: Arc::new(SystemNativeOwnerEnvironment),
            flows,
            native_owner_proofs,
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
            delivery_tables,
            event_tables,
            context_sizes,
            address_holds,
            opened_threads,
            send_calls,
            launch_cleanups,
            compactions,
            reminders,
            successor_intents,
            queue_delivery,
            send_outcomes,
            logical_deliveries,
            heartbeat_standings,
            restarts,
            seat_births,
            handovers,
            launch_changes: LaunchChanges::default(),
        };

        // Recognition already checked the authored Memory identity record;
        // these reads require the complete committed configuration before any
        // runtime adapter is constructed by RunningNexus.
        store.state()?;
        store.archive_legacy_queue()?;
        store.align_followed_requests()?;
        store.seed_missing_queued_deliveries()?;
        store
            .configuration_snapshot()?
            .ok_or(StoreError::NotConfigured)?;
        Ok(store)
    }

    fn open_seeded(path: &Path, defaults: &DefaultConfiguration) -> Result<Self, StoreError> {
        let mut engine = Engine::open(EngineOpen::new(path, SchemaVersion::new(1)))?;
        let flows = engine.register_table(TableDescriptor::new(
            FLOW_TABLE_NAME,
            FamilyName::new("flow-nexus-flow"),
            FlowRecord::schema_hash(),
        ))?;
        let native_owner_proofs = engine.register_table(NativeOwnerProofRecord::descriptor())?;
        let state = engine.register_table(TableDescriptor::new(
            FLOW_STATE_TABLE_NAME,
            FamilyName::new("flow-nexus-state"),
            SchemaHash::for_label("flow-nexus-state-v1"),
        ))?;
        let metaflows = engine.register_table(TableDescriptor::new(
            METAFLOW_TABLE_NAME,
            FamilyName::new("flow-nexus-metaflow"),
            StoredMetaflow::schema_hash(),
        ))?;
        let metaflow_processes =
            engine.register_table(TableDescriptor::<StoredMetaflowProcess>::new(
                METAFLOW_PROCESS_TABLE_NAME,
                FamilyName::new("flow-nexus-metaflow-process"),
                StoredMetaflowProcess::schema_hash(),
            ))?;
        let metaflow_locks = engine.register_table(TableDescriptor::<StoredMetaflowLock>::new(
            METAFLOW_LOCK_TABLE_NAME,
            FamilyName::new("flow-nexus-metaflow-lock"),
            StoredMetaflowLock::schema_hash(),
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
        let launch_outcomes = engine.register_table(TableDescriptor::new(
            FLOW_LAUNCH_OUTCOME_TABLE_NAME,
            FamilyName::new("flow-nexus-launch-outcome"),
            StoredLaunchOutcome::schema_hash(),
        ))?;
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
        let launch_attempts = engine.register_table(TableDescriptor::new(
            FLOW_LAUNCH_ATTEMPT_TABLE_NAME,
            FamilyName::new("flow-nexus-launch-attempt"),
            StoredLaunchAttempt::schema_hash(),
        ))?;
        let delivery_tables = DeliveryTables::register(&mut engine)?;
        let meta_configuration_tables = MetaConfigurationTables::register(&mut engine)?;
        let event_tables =
            <events::EventTables as events::RegistersEventTables>::register(&mut engine)?;
        let context_sizes = engine.register_table(
            <context::FlowContextSize as context::DescribesContextSizeTable>::descriptor(),
        )?;
        let address_holds = engine.register_table(
            <address_hold::AddressHold as address_hold::DescribesAddressHoldTable>::descriptor(),
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
        let compactions = engine.register_table(
            <compaction::Compaction as compaction::DescribesCompactionTable>::descriptor(),
        )?;
        let reminders = engine.register_table(
            <reminder::Reminder as reminder::DescribesReminderTable>::descriptor(),
        )?;
        let successor_intents = engine.register_table(
            <successor::SuccessorIntent as successor::DescribesSuccessorIntentTable>::descriptor(),
        )?;
        let queue_delivery =
            QueueDeliveryTables::register(&mut engine, QueueDeliveryTables::descriptors())?;
        let send_outcomes =
            SendOutcomeTables::register(&mut engine, SendOutcomeTables::descriptors())?;
        let logical_deliveries =
            LogicalDeliveryTables::register(&mut engine, LogicalDeliveryTables::descriptors())?;
        let heartbeat_standings = engine.register_table(StoredHeartbeatStanding::descriptor())?;
        let restarts = engine
            .register_table(<FlowStore as restart::DescribesRestartTables>::restart_descriptor())?;
        let seat_births = engine.register_table(
            <FlowStore as restart::DescribesRestartTables>::seat_birth_descriptor(),
        )?;
        let handovers =
            <FlowStore as handover::RegistersHandoverTable>::register_handover_table(&mut engine)?;
        let store = Self {
            engine,
            native_owner_environment_reader: Arc::new(SystemNativeOwnerEnvironment),
            flows,
            native_owner_proofs,
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
            delivery_tables,
            event_tables,
            context_sizes,
            address_holds,
            opened_threads,
            send_calls,
            launch_cleanups,
            compactions,
            reminders,
            successor_intents,
            queue_delivery,
            send_outcomes,
            logical_deliveries,
            heartbeat_standings,
            restarts,
            seat_births,
            handovers,
            launch_changes: LaunchChanges::default(),
        };
        store.archive_legacy_queue()?;
        store.align_followed_requests()?;
        store.seed_missing_queued_deliveries()?;
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
        store.seed_delivery_configuration(defaults)?;
        // A delivery a crash left under the lease is settled Uncertain here
        // and never retried: what reached its pane is not known.
        for _delivery in store.settle_interrupted_deliveries()? {
            eprintln!("flow-nexus: a delivery was interrupted under its lease; settled Uncertain");
        }
        // Native queue delivery is not atomic with this store. A durable
        // native receipt is enough to finish the local settlement after a
        // restart; an attempt without one becomes Uncertain, is never
        // submitted again automatically, and is reconciled from native
        // history once the Nexus runs (`ReconcilesQueueDelivery`).
        for attempt in store.recover_outstanding_deliveries()? {
            match attempt.phase {
                QueueDeliveryPhase::ReceiptRecorded(_) => {
                    if !store.settle_recorded_delivery(&attempt.attempt_id)? {
                        return Err(StoreError::StateInvariant);
                    }
                }
                QueueDeliveryPhase::Uncertain(_) => eprintln!(
                    "flow-nexus: a queued delivery is uncertain; it is reconciled from native history"
                ),
                QueueDeliveryPhase::Prepared
                | QueueDeliveryPhase::Settled(_)
                | QueueDeliveryPhase::Absent(_)
                | QueueDeliveryPhase::Resolved(_) => {
                    return Err(StoreError::StateInvariant);
                }
            }
        }
        Ok(store)
    }
}

pub(crate) trait ManagesMetaflowState: Sized {
    fn configuration_snapshot(&self) -> Result<Option<ConfigurationSnapshot>, StoreError>;
    /// Replace one configuration family whole with its declaration.
    fn configure_change(&self, change: ConfigurationChange)
    -> Result<ConfigureOutcome, StoreError>;
    /// Whether ProtectedAddresses names `address`.
    fn protects(&self, address: &Address) -> Result<bool, StoreError>;
    /// The refresh threshold configured for one model, if any.
    fn configured_threshold(
        &self,
        model_name: &str,
    ) -> Result<Option<ConfigureThreshold>, StoreError>;
    /// The leave-outs a metaflow's Start on one harness takes when it names
    /// none; empty when none are configured for that harness.
    fn default_leave_outs(
        &self,
        address: &Address,
        harness_kind: &HarnessKind,
    ) -> Result<Vec<LeaveOut>, StoreError>;
    /// Extra native argv configured for this metaflow; empty when absent.
    fn harness_flags(&self, address: &Address) -> Result<String, StoreError>;
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
    fn release_metaflow_lock(
        &self,
        lock: Lock,
        now_epoch_seconds: i64,
    ) -> Result<Response, StoreError>;
}

impl ManagesMetaflowState for FlowStore {
    /// The committed families with the counters as they stand.
    fn configuration_snapshot(&self) -> Result<Option<ConfigurationSnapshot>, StoreError> {
        use counters::ConfiguresCounters;
        let Some(families) =
            FlowStore::meta_configuration_snapshot(&self.engine, &self.meta_configuration_tables)?
        else {
            return Ok(None);
        };
        Ok(Some(families.with_counters(self.counters()?)))
    }

    /// The counters are the running store's; every other family is
    /// declared as a configuration-only store declares it.
    fn configure_change(
        &self,
        change: ConfigurationChange,
    ) -> Result<ConfigureOutcome, StoreError> {
        use counters::ConfiguresCounters;
        match change {
            ConfigurationChange::Counters(declared) => self.configure_counters(declared),
            change => FlowStore::configure_meta_change(
                &self.engine,
                &self.meta_configuration_tables,
                change,
            ),
        }
    }

    fn protects(&self, address: &Address) -> Result<bool, StoreError> {
        let rows = self.engine.match_records(QueryPlan::key(
            self.meta_configuration_tables.protected_addresses,
            RecordKey::new(FlowStore::address_record_key(address)),
        ))?;
        match rows.records() {
            [] => Ok(false),
            [stored] if &stored.address == address => Ok(true),
            _ => Err(StoreError::StateInvariant),
        }
    }

    fn configured_threshold(
        &self,
        model_name: &str,
    ) -> Result<Option<ConfigureThreshold>, StoreError> {
        let rows = self.engine.match_records(QueryPlan::key(
            self.meta_configuration_tables.thresholds,
            RecordKey::new(model_name),
        ))?;
        match rows.records() {
            [] => Ok(None),
            [stored] => Ok(Some(stored.threshold.clone())),
            _ => Err(StoreError::StateInvariant),
        }
    }

    fn default_leave_outs(
        &self,
        address: &Address,
        harness_kind: &HarnessKind,
    ) -> Result<Vec<LeaveOut>, StoreError> {
        let rows = self.engine.match_records(QueryPlan::key(
            self.meta_configuration_tables.leave_outs,
            RecordKey::new(FlowStore::leave_out_record_key(address, harness_kind)),
        ))?;
        match rows.records() {
            [] => Ok(Vec::new()),
            [stored] => Ok(stored.leave_out.leave_outs.clone()),
            _ => Err(StoreError::StateInvariant),
        }
    }

    fn harness_flags(&self, address: &Address) -> Result<String, StoreError> {
        let rows = self.engine.match_records(QueryPlan::key(
            self.meta_configuration_tables.harness_flags,
            RecordKey::new(FlowStore::address_record_key(address)),
        ))?;
        match rows.records() {
            [] => Ok(String::new()),
            [stored] if stored.configured.address == *address => {
                Ok(stored.configured.harness_flags.clone())
            }
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
            return Ok(Response::Refused(
                <signal_flow::Refusal as crate::sending::RefusesUnidentified>::unidentified(
                    process,
                ),
            ));
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
        Ok(Response::Refused(
            <signal_flow::Refusal as crate::sending::RefusesUnidentified>::unidentified(process),
        ))
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
            Query::Rehearse(_) => Ok(Response::RehearseRejected(
                StartRejection::LaunchPersistenceRefused,
            )),
            // A refresh closes a pane, which the store alone cannot.
            Query::Refresh(_) => Ok(Response::Refused(signal_flow::Refusal::Store(
                "the store alone closes no pane".into(),
            ))),
            // A restart closes and opens panes, which the store alone cannot.
            Query::Restart(_) => Ok(Response::Refused(signal_flow::Refusal::Store(
                "the store alone closes no pane".into(),
            ))),
            Query::ResolveRecipient(flow_id) => self.resolve_recipient(&flow_id),
            Query::Stop(_) => Ok(Response::StopRejected(
                signal_flow::StopRejection::PersistenceRefused,
            )),
            Query::Bind(_)
            | Query::Identify(_)
            | Query::IdentifyThread(_)
            | Query::Lock(_)
            | Query::Deliver(_)
            | Query::Release(_) => Ok(Response::Refused(signal_flow::Refusal::NotMessage)),
            // The sender is read from the socket, which the store never sees.
            Query::Send(_)
            | Query::DeliverAll(_)
            | Query::BindSeat(_)
            | Query::Adopt(_)
            | Query::ObserveSends(_)
            | Query::ObserveSender(_) => Ok(Response::Refused(
                <signal_flow::Refusal as crate::sending::RefusesUnidentified>::unidentified(
                    signal_flow::Process { pid: 0, started: 0 },
                ),
            )),
            Query::Launch(_) => Ok(Response::StartRejected(
                StartRejection::LaunchPersistenceRefused,
            )),
            Query::Keep(_) | Query::Drop(_) | Query::Glance(_) | Query::Read(_) => {
                Ok(Response::Refused(signal_flow::Refusal::Store(
                    "handover requires running Nexus caller".into(),
                )))
            }
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
            Query::Refresh(_)
            | Query::Rehearse(_)
            | Query::ResolveRecipient(_)
            | Query::Stop(_)
            | Query::Current(_)
            | Query::Metaflows
            | Query::Replace(_)
            | Query::LaunchStatus(_)
            | Query::Observe(_)
            | Query::ObserveSends(_)
            | Query::ObserveSender(_)
            | Query::ResolveCaller(_)
            | Query::Report(_)
            | Query::Bind(_)
            | Query::Identify(_)
            | Query::IdentifyThread(_)
            | Query::Lock(_)
            | Query::Deliver(_)
            | Query::Release(_)
            | Query::Send(_)
            | Query::DeliverAll(_)
            | Query::Launch(_)
            | Query::BindSeat(_)
            | Query::Adopt(_)
            | Query::Restart(_)
            | Query::Keep(_)
            | Query::Drop(_)
            | Query::Glance(_)
            | Query::Read(_) => Ok(None),
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
    fn register_flow_as(
        &self,
        flow_node: FlowNode,
        flow_type: String,
        role: Option<Caller>,
    ) -> Result<FlowRegistration, StoreError>;
}

impl RegistersBindings for FlowStore {
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
    fn adopt_existing_flow(
        &self,
        flow_node: FlowNode,
        role: Caller,
        address: Address,
        binding: &meta_signal_flow::FlowBinding,
    ) -> Result<ExistingSeatAdoption, StoreError> {
        use meta_signal_flow::FlowBindingRefusalReason as Refusal;
        if role.flow_id != flow_node.flow_id || role.aspect != address.aspect {
            return Ok(ExistingSeatAdoption::Refused(Refusal::RoleMismatch));
        }
        if self.configured_model(&address)?.as_deref() != Some(role.model_name.as_str()) {
            return Ok(ExistingSeatAdoption::Refused(Refusal::RoleMismatch));
        }
        let HerdrRouteSelection::Available(route) = flow_node.herdr_route_selection.clone() else {
            return Ok(ExistingSeatAdoption::Refused(Refusal::AnatomyMismatch));
        };
        let existing_metaflow = self.stored_metaflow(&address)?;
        let already_bound_elsewhere = self
            .engine
            .match_records(QueryPlan::all(self.metaflows))?
            .records()
            .iter()
            .any(|stored| {
                stored.metaflow.address != address
                    && matches!(
                        &stored.metaflow.state,
                        MetaflowState::Awake(current) if current == &flow_node.flow_id
                    )
            });
        if already_bound_elsewhere {
            return Ok(ExistingSeatAdoption::Refused(Refusal::DuplicateFlowId));
        }
        let mut metaflow = existing_metaflow
            .as_ref()
            .map(|stored| stored.metaflow.clone())
            .unwrap_or(Metaflow {
                address: address.clone(),
                state: MetaflowState::Asleep,
                past: Vec::new(),
                queue: Vec::new(),
            });
        let previous_flow = match &metaflow.state {
            MetaflowState::Awake(current) if current == &flow_node.flow_id => None,
            MetaflowState::Awake(_) => {
                let MetaflowState::Awake(current) = &metaflow.state else {
                    unreachable!();
                };
                let Some(previous) = self.flow(current)? else {
                    return Ok(ExistingSeatAdoption::Refused(Refusal::AddressOwnerUnknown));
                };
                let Some(proof) = self.native_owner_proof(current)? else {
                    return Ok(ExistingSeatAdoption::Refused(Refusal::AddressOwnerUnknown));
                };
                if proof.harness_kind != previous.harness_kind
                    || previous.thread_id.as_deref() != Some(proof.native_session_id.as_str())
                {
                    return Ok(ExistingSeatAdoption::Refused(Refusal::AddressOwnerUnknown));
                }
                match proof.current_process_state(self.native_owner_environment().as_ref()) {
                    NativeOwnerState::Live => {
                        return Ok(ExistingSeatAdoption::Refused(Refusal::AddressOwnerLive));
                    }
                    NativeOwnerState::Unknown => {
                        return Ok(ExistingSeatAdoption::Refused(Refusal::AddressOwnerUnknown));
                    }
                    NativeOwnerState::Dead => Some(current.clone()),
                }
            }
            MetaflowState::Asleep => None,
        };
        if let Some(previous) = &previous_flow {
            metaflow.past.push(previous.clone());
        }
        metaflow.past.retain(|past| past != &flow_node.flow_id);
        metaflow.state = MetaflowState::Awake(flow_node.flow_id.clone());

        let existing_flow = self.flow(&flow_node.flow_id)?;
        if let Some(existing) = &existing_flow {
            if existing.thread_id.as_deref() != Some(flow_node.session_id.as_str())
                || existing.harness_kind != flow_node.harness_kind
            {
                return Ok(ExistingSeatAdoption::Refused(
                    Refusal::NativeIdentityMismatch,
                ));
            }
            if self
                .role(&flow_node.flow_id)?
                .is_some_and(|stored| stored != role)
            {
                return Ok(ExistingSeatAdoption::Refused(Refusal::RoleMismatch));
            }
        }
        let lifecycle = match flow_node.flow_lifecycle {
            SignalFlowLifecycle::Pending | SignalFlowLifecycle::Active => FlowLifecycle::Active,
            SignalFlowLifecycle::Stopped
            | SignalFlowLifecycle::Retired
            | SignalFlowLifecycle::Exited => {
                return Ok(ExistingSeatAdoption::Refused(Refusal::DeadProcess));
            }
        };
        let flow_record = FlowRecord {
            flow_id: flow_node.flow_id.clone(),
            flow_type: format!("{:?}:{}", role.aspect, role.model_name),
            origin: flow_node.origin.clone(),
            thread_id: Some(flow_node.session_id.clone()),
            harness_kind: flow_node.harness_kind.clone(),
            endpoint_selection: flow_node.endpoint_selection.clone(),
            lifecycle,
            generation: existing_flow.as_ref().map_or(1, |flow| flow.generation),
        };
        let Some(environment) = self.native_owner_environment() else {
            return Ok(ExistingSeatAdoption::Refused(Refusal::AddressOwnerUnknown));
        };
        let Some(owner_proof) = NativeOwnerProof::from_binding(binding, &environment) else {
            return Ok(ExistingSeatAdoption::Refused(Refusal::AddressOwnerUnknown));
        };
        let owner_proof = NativeOwnerProofRecord {
            flow_id: flow_node.flow_id.clone(),
            proof: owner_proof,
        };
        let existing_owner_proof = self.native_owner_proof_record(&flow_node.flow_id)?;
        let existing_route = self.herdr_route(&flow_node.flow_id)?;
        let existing_role = self.role(&flow_node.flow_id)?;
        let mut commit = self.engine.begin_atomic_commit();
        commit = match existing_flow {
            Some(_) => commit.mutate(self.flows, flow_record),
            None => commit.assert(self.flows, flow_record),
        };
        commit = match existing_route {
            Some(_) => commit.mutate(
                self.herdr_routes,
                FlowHerdrRouteRecord {
                    flow_id: flow_node.flow_id.clone(),
                    route,
                },
            ),
            None => commit.assert(
                self.herdr_routes,
                FlowHerdrRouteRecord {
                    flow_id: flow_node.flow_id.clone(),
                    route,
                },
            ),
        };
        if existing_role.is_none() {
            commit = commit.assert(self.roles, StoredRole { caller: role });
        }
        commit = match existing_owner_proof {
            Some(_) => commit.mutate(self.native_owner_proofs, owner_proof),
            None => commit.assert(self.native_owner_proofs, owner_proof),
        };
        if let Some(previous) = previous_flow.filter(|previous| previous != &flow_node.flow_id)
            && let Some(mut old) = self.flow(&previous)?
            && old.lifecycle.is_live()
        {
            old.lifecycle = FlowLifecycle::Exited;
            commit = commit.mutate(self.flows, old);
        }
        commit = match existing_metaflow {
            Some(_) => commit.mutate(self.metaflows, StoredMetaflow { metaflow }),
            None => commit.assert(self.metaflows, StoredMetaflow { metaflow }),
        };
        self.engine.commit_atomic(commit)?;
        // The Nexus's Codex responder set follows this signal. An adopted
        // seat was committed without announcing it, so no connection ever
        // subscribed to that thread until an unrelated store change. With
        // no thread events, its first rest could not drain durable work.
        self.launch_changes.announce();
        Ok(ExistingSeatAdoption::Bound(Box::new(flow_node)))
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
            },
        ))?;
        self.launch_changes.announce();
        Ok(LaunchAttemptReservation::Reserved(attempt))
    }
}

impl RecordsNativeLaunchIntent for FlowStore {
    fn record_native_launch_intent(&self, intent: NativeLaunchIntent) -> Result<bool, StoreError> {
        use sha2::{Digest, Sha256};
        let Some(mut stored) = self.stored_launch_attempt(&intent.launch_request_id)? else {
            return Ok(false);
        };
        let empty_simplified_start = intent.prompt_sha256_vector.is_empty()
            && stored.attempt.launch_profile.flow_kind == signal_flow::FlowKind::Simplified
            && matches!(&stored.attempt.launch_profile.tool_set,
                signal_flow::ToolSet::Explicit(tools) if tools.is_empty())
            && stored.attempt.launch_profile.instruction_prompt.is_empty()
            && intent.prompt_sha256 == format!("{:x}", Sha256::digest([]));
        if stored.attempt.launch_attempt_phase != LaunchAttemptPhase::Reserved
            || stored.attempt.prompt_sha256 != intent.prompt_sha256
            || stored.attempt.launch_profile.model_name != intent.model_name
            || stored.attempt.launch_profile.effort != intent.effort
            || (intent.prompt_sha256_vector.is_empty() && !empty_simplified_start)
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
        use sha2::{Digest, Sha256};
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
        let empty_simplified_start = native_intent.prompt_sha256_vector.is_empty()
            && native_intent.prompt_sha256 == format!("{:x}", Sha256::digest([]))
            && intent.prompt_sha256 == native_intent.prompt_sha256
            && stored.attempt.launch_profile.flow_kind == signal_flow::FlowKind::Simplified
            && matches!(&stored.attempt.launch_profile.tool_set,
                signal_flow::ToolSet::Explicit(tools) if tools.is_empty())
            && stored.attempt.launch_profile.instruction_prompt.is_empty();
        let valid_parts = intent.prompt_sha256_vector == native_intent.prompt_sha256_vector
            && if native_intent.prompt_sha256_vector.is_empty() {
                empty_simplified_start && intent.parts_submitted == 0
            } else {
                intent.parts_submitted == 1
            }
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
            .map(|stored| stored.attempt.launch_profile))
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
        // Address it held; a confirmed one puts its metaflow to sleep. A
        // restart record it holds goes in the same write.
        let commit = restart::RecordsRestarts::restart_retracted(
            self,
            flow_id,
            self.engine.begin_atomic_commit().mutate(self.flows, flow),
        )?;
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
        // A Maintained metaflow whose flow went without Flow or an owner
        // ending it is owed a successor, in the same write.
        let owed = match self.address_hold(flow_id)? {
            Some(_) => None,
            None => metaflow::SleepsMetaflows::metaflow_awake_as(self, flow_id)?,
        };
        // A flow that ends before its launch was confirmed gives back the
        // Address it held; a confirmed one puts its metaflow to sleep.
        let commit = self.engine.begin_atomic_commit().mutate(self.flows, flow);
        let commit = match owed {
            Some(metaflow)
                if successor::ReadsContinuity::continuity(self, &metaflow.address)?
                    == meta_signal_flow::Continuity::Maintained =>
            {
                successor::OwesSuccessors::owing_successor(
                    self,
                    successor::SuccessorIntent {
                        address: metaflow.address,
                        predecessor: flow_id.to_owned(),
                        cause: successor::SuccessionCause::Exit,
                        launch_request_id: String::new(),
                    },
                    commit,
                )?
            }
            _ => commit,
        };
        self.engine
            .commit_atomic(self.ended_at_address(flow_id, commit)?)?;
        self.launch_changes.announce();
        Ok(true)
    }
}

impl RebindsNativeSession for FlowStore {
    fn rebind_native_session(
        &self,
        flow_id: &str,
        previous: &str,
        session: &str,
    ) -> Result<bool, StoreError> {
        let Some(mut flow) = self.flow(flow_id)? else {
            return Ok(false);
        };
        if !flow.lifecycle.is_live() || flow.thread_id.as_deref() != Some(previous) {
            return Ok(false);
        }
        flow.thread_id = Some(session.to_owned());
        let mut commit = self.engine.begin_atomic_commit().mutate(self.flows, flow);
        if let Some(mut owner_proof) = self.native_owner_proof_record(flow_id)?
            && owner_proof.proof.native_session_id == previous
        {
            owner_proof.proof.native_session_id = session.to_owned();
            commit = commit.mutate(self.native_owner_proofs, owner_proof);
        }
        self.engine.commit_atomic(commit)?;
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
        let mut taken_address = None;
        if let Some(launch_request_id) = self.launch_requests_bound_to(flow_id)?.pop()
            && let Some(attempt) = self.launch_attempt(&launch_request_id)?
        {
            let profile = attempt.launch_profile;
            taken_address = Some(profile.address.clone());
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
        } else if let Some(address) = &taken_address
            && written
        {
            // A confirmed flow is the successor its Address was owed, if any.
            commit = successor::OwesSuccessors::successor_given(self, address, commit)?;
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
}

/// Store identity, catalogue validation and committed-record operations.
pub(crate) trait ReadsFlowMemory {
    fn aspect_key(aspect: &Aspect) -> &'static str {
        match aspect {
            Aspect::Psyche => "Psyche",
            Aspect::Mind => "Mind",
            Aspect::Field => "Field",
        }
    }

    /// A configured model's key: its Model.
    fn model_record_key(model: &Model) -> String {
        model.name().to_owned()
    }

    /// A leave-out default's key: its Address's key, then its harness.
    fn leave_out_record_key(address: &Address, harness_kind: &HarnessKind) -> String {
        format!(
            "{}:{}",
            FlowStore::address_record_key(address),
            FlowStore::harness_key(harness_kind)
        )
    }

    fn harness_key(harness_kind: &HarnessKind) -> &'static str {
        match harness_kind {
            HarnessKind::Claude => "Claude",
            HarnessKind::Codex => "Codex",
        }
    }

    fn address_record_key(address: &Address) -> String {
        format!(
            "{}:{}:{}:{}",
            FlowStore::aspect_key(&address.aspect),
            address.topic.len(),
            address.topic,
            address.model.name()
        )
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
            "flow_nexus_metaflows" => ("flow-nexus-metaflow", "flow-nexus-metaflow-v7"),
            "flow_nexus_metaflow_processes" => (
                "flow-nexus-metaflow-process",
                "flow-nexus-metaflow-process-v2",
            ),
            "flow_nexus_metaflow_locks" => {
                ("flow-nexus-metaflow-lock", "flow-nexus-metaflow-lock-v2")
            }
            "flow_nexus_deliveries" => ("flow-nexus-delivery", "flow-nexus-delivery-v1"),
            "flow_nexus_pane_leases" => ("flow-nexus-pane-lease", "flow-nexus-pane-lease-v1"),
            "flow_nexus_delivery_configuration" => (
                "flow-nexus-delivery-configuration",
                "flow-nexus-delivery-configuration-v2",
            ),
            "flow_nexus_meta_nexus_configuration" => {
                return Some((
                    nexus_configuration::META_NEXUS_CONFIGURATION_FAMILY,
                    StoredMetaNexusConfiguration::schema_hash(),
                ));
            }
            "flow_nexus_meta_model_configuration" => {
                return Some((
                    "flow-nexus-meta-model-configuration",
                    StoredMetaModelConfiguration::schema_hash(),
                ));
            }
            "flow_nexus_meta_model_threshold_configuration" => (
                META_THRESHOLD_CONFIGURATION_FAMILY,
                META_THRESHOLD_CONFIGURATION_SCHEMA,
            ),
            "flow_nexus_meta_investigation_configuration" => (
                META_INVESTIGATION_CONFIGURATION_FAMILY,
                META_INVESTIGATION_CONFIGURATION_SCHEMA,
            ),
            "flow_nexus_meta_default_minutes_configuration" => (
                META_DEFAULT_MINUTES_CONFIGURATION_FAMILY,
                META_DEFAULT_MINUTES_CONFIGURATION_SCHEMA,
            ),
            "flow_nexus_meta_harness_flags_configuration" => (
                META_HARNESS_FLAGS_CONFIGURATION_FAMILY,
                META_HARNESS_FLAGS_CONFIGURATION_SCHEMA,
            ),
            "flow_nexus_meta_heartbeat_configuration" => (
                META_HEARTBEAT_CONFIGURATION_FAMILY,
                META_HEARTBEAT_CONFIGURATION_SCHEMA,
            ),
            "flow_nexus_meta_protected_address_configuration" => (
                META_PROTECTED_ADDRESS_CONFIGURATION_FAMILY,
                META_PROTECTED_ADDRESS_CONFIGURATION_SCHEMA,
            ),
            "flow_nexus_send_outcome_counter" => {
                (send_outcome::COUNTER_FAMILY, send_outcome::COUNTER_SCHEMA)
            }
            "flow_nexus_send_outcomes" => {
                (send_outcome::OUTCOME_FAMILY, send_outcome::OUTCOME_SCHEMA)
            }
            "flow_nexus_logical_delivery_counter" => (
                "flow-nexus-logical-delivery-counter",
                "flow-nexus-logical-delivery-counter-v1",
            ),
            "flow_nexus_logical_delivery_current" => (
                "flow-nexus-logical-delivery-current",
                logical_delivery::CURRENT_SCHEMA,
            ),
            "flow_nexus_logical_delivery_events" => (
                "flow-nexus-logical-delivery-event",
                logical_delivery::EVENT_SCHEMA,
            ),
            "flow_nexus_simplified_reply_bindings" => (
                "flow-nexus-simplified-reply-binding",
                "flow-nexus-simplified-reply-binding-v2",
            ),
            "flow_nexus_logical_delivery_purposes" => (
                "flow-nexus-logical-delivery-purpose",
                "flow-nexus-logical-delivery-purpose-v2",
            ),
            "flow_nexus_restarts" => (restart::RESTART_FAMILY, restart::RESTART_SCHEMA),
            "flow_nexus_seat_births" => (restart::SEAT_BIRTH_FAMILY, restart::SEAT_BIRTH_SCHEMA),
            "flow_nexus_heartbeat_standings" => {
                (HEARTBEAT_STANDING_FAMILY, HEARTBEAT_STANDING_SCHEMA)
            }
            "flow_nexus_compactions" => {
                (compaction::COMPACTION_FAMILY, compaction::COMPACTION_SCHEMA)
            }
            "flow_nexus_reminders" => (reminder::REMINDER_FAMILY, reminder::REMINDER_SCHEMA),
            "flow_nexus_successor_intents" => (
                successor::SUCCESSOR_INTENT_FAMILY,
                successor::SUCCESSOR_INTENT_SCHEMA,
            ),
            "flow_nexus_meta_leave_out_configuration" => {
                return Some((
                    "flow-nexus-meta-leave-out-configuration",
                    StoredMetaLeaveOutConfiguration::schema_hash(),
                ));
            }
            "flow_nexus_meta_send_server_configuration" => (
                send_server::META_SEND_SERVER_CONFIGURATION_FAMILY,
                send_server::META_SEND_SERVER_CONFIGURATION_SCHEMA,
            ),
            "flow_nexus_meta_metaflow_configuration" => (
                metaflow_declaration::META_METAFLOW_CONFIGURATION_FAMILY,
                metaflow_declaration::META_METAFLOW_CONFIGURATION_SCHEMA,
            ),
            "flow_nexus_meta_handover_configuration" => (
                "flow-nexus-handover-configuration",
                "flow-nexus-handover-configuration-v1",
            ),
            "flow_nexus_handovers" => (handover::HANDOVER_FAMILY, handover::HANDOVER_SCHEMA),
            "flow_nexus_flow_events" => (events::FLOW_EVENT_FAMILY, events::FLOW_EVENT_SCHEMA),
            "flow_nexus_flow_context_sizes" => (
                context::FLOW_CONTEXT_SIZE_FAMILY,
                context::FLOW_CONTEXT_SIZE_SCHEMA,
            ),
            "flow_nexus_native_owner_proofs" => (
                "flow-nexus-native-owner-proof",
                "flow-nexus-native-owner-proof-v1",
            ),
            "flow_nexus_address_holds" => (
                address_hold::ADDRESS_HOLD_FAMILY,
                address_hold::ADDRESS_HOLD_SCHEMA,
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
            "flow_nexus_queue_delivery_counter" => (
                "flow-nexus-queue-delivery-counter",
                "flow-nexus-queue-delivery-counter-v1",
            ),
            "flow_nexus_queue_delivery_requests" => (
                "flow-nexus-queue-delivery-request",
                "flow-nexus-queue-delivery-request-v7",
            ),
            "flow_nexus_queue_delivery_attempts" => (
                "flow-nexus-queue-delivery-attempt",
                "flow-nexus-queue-delivery-attempt-v8",
            ),
            _ => return None,
        };
        Some((family, SchemaHash::for_label(version)))
    }

    /// A family registration this Flow opens: its current identity alone.
    fn is_current_flow_registration(table_key: &str, registration: &TableRegistration) -> bool {
        let identity = registration.identity();
        FlowStore::current_flow_family_identity(table_key).is_some_and(|(family, schema_hash)| {
            identity.table_name() == table_key
                && identity.family().as_str() == family
                && identity.schema_hash() == schema_hash
        })
    }

    /// The catalog of a configuration-only store or of a running store,
    /// whole: every table of the one and none other, or, as 0.53.0 wrote
    /// it, every table but the ones 0.53.1 added.
    fn has_expected_flow_catalog(actual: &std::collections::BTreeSet<String>) -> bool {
        let is = |tables: &[&str]| {
            actual.iter().all(|name| {
                tables.contains(&name.as_str())
                    || *name == OPTIONAL_HANDOVER_TABLE
                    || *name == OPTIONAL_HANDOVER_RUNTIME_TABLE
            }) && tables.iter().all(|name| {
                actual.contains(*name)
                    || ADDED_IN_0531.contains(name)
                    || *name == OPTIONAL_HANDOVER_TABLE
                    || *name == OPTIONAL_HANDOVER_RUNTIME_TABLE
            }) && (ADDED_IN_0531.iter().all(|name| actual.contains(*name))
                || ADDED_IN_0531.iter().all(|name| !actual.contains(*name)))
        };
        is(CONFIGURATION_ONLY_TABLES) || is(RUNNING_TABLES)
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
        let earlier = |version: u64| StoreError::EarlierMemory {
            path: path.display().to_string(),
            version,
        };
        // ReadOnlyDatabase refuses an active writer and never runs redb recovery.
        // In particular, do not use Database::open here: Sema's normal writer open
        // can stamp schema/layout metadata before a caller discovers this is an
        // old or foreign occupant.
        match redb::ReadOnlyDatabase::open(path) {
            Ok(database) => FlowStore::recognize_open_memory(&database, &reject, &earlier),
            // A writer that did not close cleanly leaves redb's recovery flag,
            // and a read-only open refuses to repair. The flagged store is
            // recognized through a repaired copy, so the original is written
            // only by the writer open that follows recognition.
            Err(redb::DatabaseError::RepairAborted) => {
                FlowStore::recognize_repaired_copy(path, &reject, &earlier)
            }
            Err(redb::DatabaseError::DatabaseAlreadyOpen) => Err(StoreError::StoreHeld {
                path: path.display().to_string(),
            }),
            Err(error) => Err(reject(error.to_string())),
        }
    }

    fn recognize_repaired_copy(
        path: &Path,
        reject: &dyn Fn(String) -> StoreError,
        earlier: &dyn Fn(u64) -> StoreError,
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
        let recognized = FlowStore::recognize_open_memory(&database, reject, earlier);
        drop(database);
        recognized
    }

    fn recognize_open_memory(
        database: &redb::ReadOnlyDatabase,
        reject: &dyn Fn(String) -> StoreError,
        earlier: &dyn Fn(u64) -> StoreError,
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
        // A store an earlier Flow wrote is refused, never upgraded: Flow
        // Memory v3 begins with 0.52.0.
        match version {
            Some(version) if version == u64::from(FLOW_SCHEMA_VERSION) => {}
            Some(version) if version < u64::from(FLOW_SCHEMA_VERSION) => {
                return Err(earlier(version));
            }
            version => {
                return Err(reject(format!(
                    "Sema schema version is {version:?}, expected {FLOW_SCHEMA_VERSION}"
                )));
            }
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
                        "family registration {table_key:?} is not an authored Flow Memory v3 identity"
                    )));
                }
                registered_tables.insert(table_key.to_owned());
            }
            FlowStore::has_expected_flow_catalog(&registered_tables)
        };
        if !catalog_is_current {
            return Err(reject(
                "family catalog is not a complete authored Flow Memory v3 configuration-only or running schema".into(),
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
    ) -> Result<Option<ConfiguredFamilies>, StoreError> {
        let nexus_rows = engine.match_records(QueryPlan::all(tables.nexus))?;
        let handover_rows = engine.match_records(QueryPlan::all(tables.handover))?;
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
        let send_server_rows = engine.match_records(QueryPlan::all(tables.send_servers))?;
        let investigation_rows = engine.match_records(QueryPlan::all(tables.investigation))?;
        let default_minutes_rows = engine.match_records(QueryPlan::all(tables.default_minutes))?;
        let harness_flags_rows = engine.match_records(QueryPlan::all(tables.harness_flags))?;
        let heartbeat_rows = engine.match_records(QueryPlan::all(tables.heartbeat))?;
        let protected_address_rows =
            engine.match_records(QueryPlan::all(tables.protected_addresses))?;
        let handover = handover_rows
            .records()
            .first()
            .cloned()
            .unwrap_or_else(handover_configuration::unconfigured);
        let metaflow_rows = engine.match_records(QueryPlan::all(tables.metaflows))?;
        if models_rows.database_marker() != marker
            || metaflow_rows.database_marker() != marker
            || thresholds_rows.database_marker() != marker
            || leave_out_rows.database_marker() != marker
            || send_server_rows.database_marker() != marker
            || investigation_rows.database_marker() != marker
            || default_minutes_rows.database_marker() != marker
            || harness_flags_rows.database_marker() != marker
            || heartbeat_rows.database_marker() != marker
            || protected_address_rows.database_marker() != marker
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
        models.sort_by_key(|model| FlowStore::model_record_key(&model.model));
        let mut thresholds = thresholds_rows
            .records()
            .iter()
            .map(|stored| stored.threshold.clone())
            .collect::<Vec<_>>();
        thresholds.sort_by(|left, right| left.model_name.cmp(&right.model_name));
        let investigation = match investigation_rows.records() {
            [] => None,
            [stored] => Some(stored.investigation.clone()),
            _ => return Err(StoreError::StateInvariant),
        };
        // A Memory no DefaultMinutes was declared in holds the seed.
        let default_minutes = match default_minutes_rows.records() {
            [] => SEEDED_DEFAULT_MINUTES,
            [stored] => stored.default_minutes,
            _ => return Err(StoreError::StateInvariant),
        };
        let heartbeat = match heartbeat_rows.records() {
            [] => None,
            [stored] => Some(stored.heartbeat.clone()),
            _ => return Err(StoreError::StateInvariant),
        };
        let mut default_leave_outs = leave_out_rows
            .records()
            .iter()
            .map(|stored| stored.leave_out.clone())
            .collect::<Vec<_>>();
        default_leave_outs.sort_by_key(|leave_out| {
            FlowStore::leave_out_record_key(&leave_out.address, &leave_out.harness_kind)
        });
        let mut send_servers = send_server_rows
            .records()
            .iter()
            .map(|stored| stored.send_server.clone())
            .collect::<Vec<_>>();
        send_servers.sort_by(|left, right| {
            left.codex_app_server_binary
                .cmp(&right.codex_app_server_binary)
        });
        let mut metaflows = metaflow_rows
            .records()
            .iter()
            .map(|stored| stored.metaflow.clone())
            .collect::<Vec<_>>();
        metaflows.sort_by_key(|metaflow| FlowStore::address_record_key(&metaflow.address));
        let mut configured_harness_flags = harness_flags_rows
            .records()
            .iter()
            .map(|stored| stored.configured.clone())
            .collect::<Vec<_>>();
        configured_harness_flags
            .sort_by_key(|configured| FlowStore::address_record_key(&configured.address));
        let mut protected_addresses = protected_address_rows
            .records()
            .iter()
            .map(|stored| stored.address.clone())
            .collect::<Vec<_>>();
        protected_addresses.sort_by_key(FlowStore::address_record_key);
        Ok(Some(ConfiguredFamilies {
            nexus,
            models,
            thresholds,
            default_leave_outs,
            send_servers,
            investigation,
            metaflows,
            default_minutes,
            configured_harness_flags,
            heartbeat,
            protected_addresses,
            handover_policies: handover.policies,
            handover_archive: handover.archive,
            handover_count: handover.count,
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
        use crate::launching::NamesTopic;
        // Every Address a declaration names has a camelCase topic, checked
        // before anything is written.
        let addresses: Vec<&Address> = match &change {
            ConfigurationChange::LeaveOuts(entries) => {
                entries.iter().map(|entry| &entry.address).collect()
            }
            ConfigurationChange::Metaflows(entries) => {
                entries.iter().map(|entry| &entry.address).collect()
            }
            ConfigurationChange::HarnessFlags(entries) => {
                entries.iter().map(|entry| &entry.address).collect()
            }
            ConfigurationChange::Investigation(address) => vec![address],
            ConfigurationChange::Heartbeat(Some(config)) => {
                config.heartbeat_recipients.iter().collect()
            }
            ConfigurationChange::ProtectedAddresses(addresses) => addresses.iter().collect(),
            _ => Vec::new(),
        };
        if let Some(refused) = addresses
            .into_iter()
            .find(|address| !address.topic.as_str().is_camel_case_topic())
        {
            return Err(StoreError::TopicRefused(refused.topic.clone()));
        }
        match change {
            ConfigurationChange::HandoverPolicies(policies) => {
                if policies
                    .iter()
                    .any(|p| !(0..=100).contains(&p.warning_share_left))
                {
                    return Err(StoreError::HandoverConfigurationRefused(
                        meta_signal_flow::HandoverConfigurationRefusal::InvalidWarningShareLeft,
                    ));
                }
                if policies.iter().any(|p| p.every_minutes <= 0) {
                    return Err(StoreError::HandoverConfigurationRefused(
                        meta_signal_flow::HandoverConfigurationRefusal::InvalidEveryMinutes,
                    ));
                }
                if policies
                    .iter()
                    .enumerate()
                    .any(|(i, p)| policies[..i].iter().any(|q| q.model_name == p.model_name))
                {
                    return Err(StoreError::HandoverConfigurationRefused(
                        meta_signal_flow::HandoverConfigurationRefusal::DuplicatePolicy,
                    ));
                }
                let old = engine
                    .match_records(QueryPlan::all(tables.handover))?
                    .records()
                    .first()
                    .cloned()
                    .unwrap_or_else(handover_configuration::unconfigured);
                FlowStore::replace_configuration_family(
                    engine,
                    tables.handover,
                    vec![handover_configuration::StoredHandoverConfiguration {
                        policies: meta_signal_flow::HandoverPoliciesConfiguration::Configured(
                            policies,
                        ),
                        archive: old.archive,
                        count: old.count,
                    }],
                )
            }
            ConfigurationChange::HandoverArchive(path) => {
                let old = engine
                    .match_records(QueryPlan::all(tables.handover))?
                    .records()
                    .first()
                    .cloned()
                    .unwrap_or_else(handover_configuration::unconfigured);
                FlowStore::replace_configuration_family(
                    engine,
                    tables.handover,
                    vec![handover_configuration::StoredHandoverConfiguration {
                        policies: old.policies,
                        archive: meta_signal_flow::HandoverArchiveConfiguration::Configured(path),
                        count: old.count,
                    }],
                )
            }
            ConfigurationChange::HandoverCount(count) => {
                if count < 0 {
                    return Err(StoreError::HandoverConfigurationRefused(
                        meta_signal_flow::HandoverConfigurationRefusal::NegativeHandoverCount,
                    ));
                }
                let old = engine
                    .match_records(QueryPlan::all(tables.handover))?
                    .records()
                    .first()
                    .cloned()
                    .unwrap_or_else(handover_configuration::unconfigured);
                FlowStore::replace_configuration_family(
                    engine,
                    tables.handover,
                    vec![handover_configuration::StoredHandoverConfiguration {
                        policies: old.policies,
                        archive: old.archive,
                        count: meta_signal_flow::HandoverCountConfiguration::Configured(count),
                    }],
                )
            }
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
            ConfigurationChange::LeaveOuts(leave_outs) => {
                use crate::lean::BelongsToHarness;
                if leave_outs.iter().any(|entry| {
                    entry
                        .leave_outs
                        .iter()
                        .any(|leave_out| leave_out.harness() != entry.harness_kind)
                }) {
                    return Err(StoreError::LeaveOutNotForHarness);
                }
                FlowStore::replace_configuration_family(
                    engine,
                    tables.leave_outs,
                    leave_outs
                        .into_iter()
                        .map(|leave_out| StoredMetaLeaveOutConfiguration { leave_out })
                        .collect(),
                )
            }
            ConfigurationChange::Metaflows(metaflows) => FlowStore::replace_configuration_family(
                engine,
                tables.metaflows,
                metaflows
                    .into_iter()
                    .map(
                        |metaflow| metaflow_declaration::StoredMetaMetaflowConfiguration {
                            metaflow,
                        },
                    )
                    .collect(),
            ),
            ConfigurationChange::HarnessFlags(configured) => {
                FlowStore::replace_configuration_family(
                    engine,
                    tables.harness_flags,
                    configured
                        .into_iter()
                        .map(|configured| StoredMetaHarnessFlagsConfiguration { configured })
                        .collect(),
                )
            }
            ConfigurationChange::SendServers(send_servers) => {
                FlowStore::replace_configuration_family(
                    engine,
                    tables.send_servers,
                    send_servers
                        .into_iter()
                        .map(
                            |send_server| send_server::StoredMetaSendServerConfiguration {
                                send_server,
                            },
                        )
                        .collect(),
                )
            }
            ConfigurationChange::Investigation(investigation) => {
                FlowStore::replace_configuration_family(
                    engine,
                    tables.investigation,
                    vec![StoredMetaInvestigationConfiguration { investigation }],
                )
            }
            // The default Minutes meets the rule a Task's Minutes meets at
            // admission, from now, before anything is written.
            ConfigurationChange::DefaultMinutes(default_minutes) => {
                use crate::reminding::CountsUnixSeconds;
                use crate::store::reminder::RulesMinutes;
                default_minutes
                    .ruled_interval(std::time::SystemTime::now().seconds_since_epoch())
                    .map_err(StoreError::MinutesRefused)?;
                FlowStore::replace_configuration_family(
                    engine,
                    tables.default_minutes,
                    vec![StoredMetaDefaultMinutesConfiguration { default_minutes }],
                )
            }
            ConfigurationChange::Heartbeat(heartbeat) => {
                if let Some(configured) = heartbeat.as_ref() {
                    if configured.heartbeat_period_seconds <= 0 {
                        return Err(StoreError::InvalidHeartbeatPeriod);
                    }
                    if configured
                        .heartbeat_recipients
                        .iter()
                        .enumerate()
                        .any(|(index, address)| {
                            configured.heartbeat_recipients[..index].contains(address)
                        })
                    {
                        return Err(StoreError::DuplicateConfigurationEntry);
                    }
                }
                FlowStore::replace_configuration_family(
                    engine,
                    tables.heartbeat,
                    heartbeat
                        .into_iter()
                        .map(|heartbeat| StoredMetaHeartbeatConfiguration { heartbeat })
                        .collect(),
                )
            }
            ConfigurationChange::ProtectedAddresses(addresses) => {
                FlowStore::replace_configuration_family(
                    engine,
                    tables.protected_addresses,
                    addresses
                        .into_iter()
                        .map(|address| StoredMetaProtectedAddressConfiguration { address })
                        .collect(),
                )
            }
            // A store opened for configuration only holds no counters; the
            // running store declares them (`counters::ConfiguresCounters`).
            ConfigurationChange::Counters(_) => {
                Err(StoreError::CountersRefused(CountersRefusal::NotRunning))
            }
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
    use crate::store::logical_delivery::{RecordsLogicalDeliveries, SimplifiedReplyBinding};
    use crate::store::metaflow::{SeedsMetaflow, SleepsMetaflows};

    use std::fs;
    use std::os::unix::fs::MetadataExt;

    #[test]
    fn heartbeat_admission_atomically_advances_and_survives_reopen() {
        use super::{StoredHeartbeatStanding, TracksHeartbeat};
        use crate::heartbeat::DeliveryPurpose;
        use signal_flow::{Address, Aspect, Message, Metaflow, Model, Request, SentBy};
        let fixture = StoreFixture::new();
        let store = fixture.store();
        let recipient = Address {
            aspect: Aspect::Field,
            topic: "heartbeatTest".into(),
            model: Model::Sol,
        };
        store.seed_metaflow(Metaflow {
            address: recipient.clone(),
            state: flow_ethos::State::Asleep,
            past: Vec::new(),
            queue: Vec::new(),
        });
        store
            .replace_heartbeat_standings(vec![StoredHeartbeatStanding {
                address: recipient.clone(),
                baseline_awake: 1,
                baseline_working: None,
                baseline_working_complete: false,
                baseline_sent: 2,
                baseline_queue: 3,
                latest_awake: 4,
                latest_working: Some(5),
                latest_working_complete: true,
                latest_sent: 6,
                latest_queue: 7,
                outstanding: None,
            }])
            .expect("standing persists");
        let signal_flow::PrivateSendResponse::SendAccepted(ordinary) = store
            .accept_logical_delivery(
                &recipient,
                SentBy::Flow,
                flow_ethos::Delivery::Wake,
                Message::Soft(Request::Notice("ordinary".into())),
                None,
            )
            .expect("ordinary admission commits")
        else {
            panic!("ordinary accepted")
        };
        let signal_flow::PrivateSendResponse::SendAccepted(accepted) = store
            .accept_logical_delivery_for(
                &recipient,
                SentBy::Flow,
                flow_ethos::Delivery::Wake,
                Message::Soft(Request::Notice("heartbeat".into())),
                None,
                DeliveryPurpose::Heartbeat,
            )
            .expect("heartbeat admission commits")
        else {
            panic!("heartbeat accepted")
        };
        let standing = store.heartbeat_standings().unwrap().pop().unwrap();
        assert_eq!(standing.baseline_awake, 4);
        assert_eq!(standing.baseline_working, Some(5));
        assert_eq!(standing.baseline_sent, 6);
        assert_eq!(standing.baseline_queue, 7);
        assert_eq!(
            standing.outstanding,
            Some(accepted.logical_delivery_id.clone())
        );
        assert!(store.delivery_purposes().unwrap().iter().any(|row| {
            row.logical_delivery_id == accepted.logical_delivery_id
                && row.purpose == DeliveryPurpose::Heartbeat
        }));
        assert!(store.delivery_purposes().unwrap().iter().any(|row| {
            row.logical_delivery_id == ordinary.logical_delivery_id
                && row.purpose == DeliveryPurpose::Ordinary
        }));
        drop(store);
        let reopened = fixture.store();
        let standing = reopened.heartbeat_standings().unwrap().pop().unwrap();
        assert_eq!(standing.outstanding, Some(accepted.logical_delivery_id));
    }

    #[test]
    fn simplified_reply_requires_consumed_input_and_queues_once_to_its_sender() {
        use signal_flow::{
            Address, Aspect, DeliveryState, Message, Metaflow, Model, NativeReceipt, Request,
            SentBy,
        };
        let fixture = StoreFixture::new();
        let store = fixture.store();
        let source = Address {
            aspect: Aspect::Mind,
            topic: "judgment".into(),
            model: Model::Sol,
        };
        let recipient = Address {
            aspect: Aspect::Psyche,
            topic: "core".into(),
            model: Model::Opus,
        };
        for address in [&source, &recipient] {
            store.seed_metaflow(Metaflow {
                address: address.clone(),
                state: flow_ethos::State::Asleep,
                past: Vec::new(),
                queue: Vec::new(),
            });
        }
        let signal_flow::PrivateSendResponse::SendAccepted(input) = store
            .accept_logical_delivery(
                &source,
                SentBy::Sender(recipient.clone()),
                flow_ethos::Delivery::Wake,
                Message::Soft(Request::Order("judge this".into())),
                None,
            )
            .unwrap()
        else {
            panic!("input accepted")
        };
        assert_eq!(
            store
                .deliver_simplified_reply(
                    "simplified",
                    &input.logical_delivery_id,
                    "claude-session",
                    "prompt-1",
                    "unsolicited startup Stop"
                )
                .unwrap(),
            None,
            "an unsolicited Stop has no waking request to answer"
        );
        assert_eq!(
            store
                .deliver_simplified_reply(
                    "regular-flow",
                    &input.logical_delivery_id,
                    "claude-session",
                    "prompt-1",
                    "ordinary final message"
                )
                .unwrap(),
            None,
            "regular flows do not acquire a simplified reply binding"
        );
        store
            .bind_simplified_reply(SimplifiedReplyBinding {
                flow_id: "simplified".into(),
                input_delivery_id: input.logical_delivery_id.clone(),
                source: source.clone(),
                recipient: recipient.clone(),
                completed: false,
            })
            .unwrap();
        assert_eq!(
            store
                .deliver_simplified_reply(
                    "simplified",
                    &input.logical_delivery_id,
                    "claude-session",
                    "prompt-1",
                    "answer"
                )
                .unwrap(),
            None,
            "a Stop before the native input receipt cannot send an unrelated answer"
        );
        store
            .transition_logical_delivery(
                &input.logical_delivery_id,
                DeliveryState::Delivered(NativeReceipt {
                    harness_kind: flow_ethos::HarnessKind::Claude,
                    native_session_id: "claude-session".into(),
                    native_prompt_id: "prompt-1".into(),
                }),
            )
            .unwrap();
        assert_eq!(
            store
                .deliver_simplified_reply(
                    "simplified",
                    &input.logical_delivery_id,
                    "claude-session",
                    "prompt-1",
                    "answer"
                )
                .unwrap(),
            Some(recipient.clone())
        );
        assert_eq!(
            store
                .deliver_simplified_reply(
                    "simplified",
                    &input.logical_delivery_id,
                    "claude-session",
                    "prompt-1",
                    "answer"
                )
                .unwrap(),
            None,
            "a repeated Stop must not duplicate its Result"
        );
        let queued = store.metaflow(&recipient).unwrap().unwrap().queue;
        assert_eq!(queued.len(), 1);
        assert_eq!(queued[0].sent_by, SentBy::Sender(source));
        assert_eq!(
            queued[0].message,
            Message::Soft(Request::Result(flow_ethos::ResultPayload::Historical(
                "answer".into()
            )))
        );
        assert_eq!(queued[0].delivery, flow_ethos::Delivery::Wake);

        // A completed judgment may subsequently be woken by another sender;
        // its next result must follow that new logical input, not the prior
        // sender retained in the old binding.
        let second_sender = Address {
            aspect: Aspect::Field,
            topic: "review".into(),
            model: Model::Sol,
        };
        store.seed_metaflow(Metaflow {
            address: second_sender.clone(),
            state: flow_ethos::State::Asleep,
            past: Vec::new(),
            queue: Vec::new(),
        });
        let signal_flow::PrivateSendResponse::SendAccepted(second_input) = store
            .accept_logical_delivery(
                &second_sender,
                SentBy::Sender(recipient.clone()),
                flow_ethos::Delivery::Wake,
                Message::Soft(Request::Question("second judgment".into())),
                None,
            )
            .unwrap()
        else {
            panic!("second input accepted")
        };
        store
            .bind_simplified_reply(SimplifiedReplyBinding {
                flow_id: "simplified".into(),
                input_delivery_id: second_input.logical_delivery_id.clone(),
                source: second_sender.clone(),
                recipient: recipient.clone(),
                completed: false,
            })
            .unwrap();
        store
            .transition_logical_delivery(
                &second_input.logical_delivery_id,
                DeliveryState::Delivered(NativeReceipt {
                    harness_kind: flow_ethos::HarnessKind::Claude,
                    native_session_id: "claude-session".into(),
                    native_prompt_id: "prompt-2".into(),
                }),
            )
            .unwrap();
        assert_eq!(
            store
                .deliver_simplified_reply(
                    "simplified",
                    &second_input.logical_delivery_id,
                    "claude-session",
                    "prompt-2",
                    "second answer"
                )
                .unwrap(),
            Some(recipient.clone())
        );
        assert_eq!(
            store
                .deliver_simplified_reply(
                    "simplified",
                    &second_input.logical_delivery_id,
                    "claude-session",
                    "prompt-2",
                    "second answer"
                )
                .unwrap(),
            None,
            "duplicate Stop for the next input also queues exactly once"
        );
        let queued = store.metaflow(&recipient).unwrap().unwrap().queue;
        assert_eq!(queued.len(), 2);
        assert_eq!(queued[1].sent_by, SentBy::Sender(second_sender));
        assert_eq!(
            queued[1].message,
            Message::Soft(Request::Result(flow_ethos::ResultPayload::Historical(
                "second answer".into()
            )))
        );
    }

    #[test]
    fn core_presentation_is_queued_as_is_once_and_secondary_gets_only_a_notice() {
        use signal_flow::{
            Address, Aspect, DeliveryState, Message, Metaflow, Model, NativeReceipt, Request,
            SentBy,
        };
        let fixture = StoreFixture::new();
        let store = fixture.store();
        let source = Address {
            aspect: Aspect::Psyche,
            topic: "core".into(),
            model: Model::Fable,
        };
        let sender = Address {
            aspect: Aspect::Psyche,
            topic: "core".into(),
            model: Model::Opus,
        };
        let book = Address {
            aspect: Aspect::Psyche,
            topic: "book".into(),
            model: Model::Sonnet,
        };
        for address in [&source, &sender, &book] {
            store.seed_metaflow(Metaflow {
                address: address.clone(),
                state: flow_ethos::State::Asleep,
                past: Vec::new(),
                queue: Vec::new(),
            });
        }
        let signal_flow::PrivateSendResponse::SendAccepted(input) = store
            .accept_logical_delivery(
                &source,
                SentBy::Sender(sender.clone()),
                flow_ethos::Delivery::Wake,
                Message::Soft(Request::Question("review this book source".into())),
                None,
            )
            .unwrap()
        else {
            panic!("core Secondary may wake core Primary")
        };
        store
            .bind_simplified_reply(SimplifiedReplyBinding {
                flow_id: "simplified-core-primary".into(),
                input_delivery_id: input.logical_delivery_id.clone(),
                source: source.clone(),
                recipient: sender.clone(),
                completed: false,
            })
            .unwrap();
        store
            .transition_logical_delivery(
                &input.logical_delivery_id,
                DeliveryState::Delivered(NativeReceipt {
                    harness_kind: flow_ethos::HarnessKind::Claude,
                    native_session_id: "claude-core-primary".into(),
                    native_prompt_id: "prompt-core".into(),
                }),
            )
            .unwrap();
        assert_eq!(
            store
                .deliver_simplified_reply(
                    "simplified-core-primary",
                    &input.logical_delivery_id,
                    "claude-core-primary",
                    "prompt-core",
                    "verbatim presentation body",
                )
                .unwrap(),
            Some(book.clone())
        );
        assert_eq!(
            store
                .deliver_simplified_reply(
                    "simplified-core-primary",
                    &input.logical_delivery_id,
                    "claude-core-primary",
                    "prompt-core",
                    "verbatim presentation body",
                )
                .unwrap(),
            None,
            "replayed Stop cannot create another book order or notice"
        );
        let book_queue = store.metaflow(&book).unwrap().unwrap().queue;
        assert_eq!(book_queue.len(), 1);
        assert_eq!(book_queue[0].sent_by, SentBy::Sender(source.clone()));
        assert_eq!(
            book_queue[0].message,
            Message::Soft(Request::Order("verbatim presentation body".into()))
        );
        let secondary_queue = store.metaflow(&sender).unwrap().unwrap().queue;
        assert_eq!(secondary_queue.len(), 1);
        assert_eq!(secondary_queue[0].sent_by, SentBy::Sender(source));
        let Message::Soft(Request::Notice(notice)) = &secondary_queue[0].message else {
            panic!("the core Secondary gets a Notice, not a duplicate body")
        };
        assert!(
            notice.contains("queued"),
            "queued is not delivered: {notice}"
        );
        assert!(!notice.contains("handed"));
        assert!(!notice.contains("verbatim presentation body"));
    }
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
        AppliesFlowQuery, ConfigurationOnlyStore, ConfiguresFlowStore, ConfirmsStartedFlow,
        DerivesNativeOwnerProof, FlowStore, ManagesMetaflowState, NativeOwnerEnvironment,
        NativeOwnerProof, NativeOwnerState, OpensFlowStore, ReadsFlowRoles, ReadsLaunchAttempt,
        RecordsNativeLaunchBinding, RecordsNativeLaunchIntent, RecordsPendingThread,
        RecordsPromptDeliveryIntent, RecordsPromptDeliveryResult,
        RecordsRegistrationAcknowledgement, RegistersExistingFlow, RegistersFlowIdentity,
        ReservesLaunchAttempt, ReservesPendingStart, SetsNativeOwnerEnvironmentForTests,
    };

    #[test]
    fn native_owner_death_requires_same_host_boot_and_pid_start_evidence() {
        let process_id = std::process::id();
        let metadata = fs::metadata(format!("/proc/{process_id}")).expect("current process");
        let stat = fs::read_to_string(format!("/proc/{process_id}/stat")).expect("process stat");
        let (_, fields) = stat.rsplit_once(") ").expect("stat comm boundary");
        let start_token = fields
            .split_whitespace()
            .nth(19)
            .expect("process start token")
            .to_owned();
        let environment = NativeOwnerEnvironment {
            host_id: "test-host".into(),
            boot_id: "test-boot".into(),
        };
        let proof = NativeOwnerProof {
            process_id: i64::from(process_id),
            process_user_id: i64::from(metadata.uid()),
            process_start_token: start_token.clone(),
            host_id: environment.host_id.clone(),
            boot_id: environment.boot_id.clone(),
            harness_kind: HarnessKind::Codex,
            native_session_id: "native-session".into(),
        };
        assert_eq!(
            proof.current_process_state(Some(&environment)),
            NativeOwnerState::Live
        );
        assert_eq!(proof.current_process_state(None), NativeOwnerState::Unknown);
        let mut reused = proof.clone();
        reused.process_start_token = "not-the-recorded-start".into();
        assert_eq!(
            reused.current_process_state(Some(&environment)),
            NativeOwnerState::Dead
        );
        let mut another_boot = proof.clone();
        another_boot.boot_id = "another-boot".into();
        assert_eq!(
            another_boot.current_process_state(Some(&environment)),
            NativeOwnerState::Unknown
        );
        let mut another_host = proof;
        another_host.host_id = "another-host".into();
        assert_eq!(
            another_host.current_process_state(Some(&environment)),
            NativeOwnerState::Unknown
        );
        // An unavailable process observation is not evidence of death.
        let mut unavailable = another_host;
        unavailable.host_id = environment.host_id.clone();
        unavailable.boot_id = environment.boot_id.clone();
        unavailable.process_id = -1;
        assert_eq!(
            unavailable.current_process_state(Some(&environment)),
            NativeOwnerState::Unknown
        );
    }

    #[test]
    fn adoption_replaces_only_an_exactly_observed_exited_process_owner() {
        use std::process::{Command, Stdio};

        let fixture = StoreFixture::new();
        fixture.configured();
        let mut store = <FlowStore as OpensFlowStore>::open_configured(&fixture.path())
            .expect("configured store opens");
        store.set_native_owner_environment_for_tests(NativeOwnerEnvironment {
            host_id: "isolated-test-host".into(),
            boot_id: "isolated-test-boot".into(),
        });
        store
            .configure_change(meta_signal_flow::ConfigurationChange::Models(vec![
                meta_signal_flow::ConfigureModel {
                    model: Model::Sol,
                    native: "gpt-sol".into(),
                },
            ]))
            .expect("owner model configured");
        let mut incumbent_process = Command::new("sleep")
            .arg("30")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("incumbent native process starts");
        let process_id = incumbent_process.id();
        let metadata = fs::metadata(format!("/proc/{process_id}")).expect("process metadata");
        let stat = fs::read_to_string(format!("/proc/{process_id}/stat")).expect("process stat");
        let (_, fields) = stat.rsplit_once(") ").expect("stat comm boundary");
        let start_token = fields
            .split_whitespace()
            .nth(19)
            .expect("process start token")
            .to_owned();
        let address = Address {
            aspect: Aspect::Mind,
            topic: "core".into(),
            model: Model::Sol,
        };
        let route = |flow_id: &str| signal_flow::FlowNode {
            flow_id: flow_id.into(),
            session_id: format!("session-{flow_id}"),
            harness_kind: HarnessKind::Claude,
            endpoint_selection: signal_flow::EndpointSelection::Unavailable,
            herdr_route_selection: signal_flow::HerdrRouteSelection::Available(
                signal_flow::HerdrRoute {
                    herdr_session_name: "messaging-build".into(),
                    herdr_agent_name: format!("agent-{flow_id}"),
                    herdr_pane_id: format!("pane-{flow_id}"),
                    herdr_terminal_id: format!("terminal-{flow_id}"),
                },
            ),
            origin: Origin::Unbound,
            flow_lifecycle: signal_flow::FlowLifecycle::Active,
        };
        let binding =
            |flow_id: &str, pid: i64, uid: i64, start: String| meta_signal_flow::FlowBinding {
                flow_id: flow_id.into(),
                aspect: Aspect::Mind,
                model_name: "gpt-sol".into(),
                harness_kind: HarnessKind::Claude,
                native_session_id: format!("session-{flow_id}"),
                herdr_workspace_id: "workspace".into(),
                herdr_pane_id: format!("pane-{flow_id}"),
                herdr_tab_id: format!("tab-{flow_id}"),
                herdr_terminal_id: format!("terminal-{flow_id}"),
                herdr_agent_name: format!("agent-{flow_id}"),
                process_identity: meta_signal_flow::ProcessIdentity {
                    process_id: pid,
                    process_user_id: uid,
                    process_start_token: start,
                },
                working_directory: "/tmp".into(),
            };
        let incumbent = binding(
            "incumbent",
            i64::from(process_id),
            i64::from(metadata.uid()),
            start_token,
        );
        let role = |flow_id: &str| Caller {
            flow_id: flow_id.into(),
            aspect: Aspect::Mind,
            model_name: "gpt-sol".into(),
        };
        assert!(matches!(
            store
                .adopt_existing_flow(
                    route("incumbent"),
                    role("incumbent"),
                    address.clone(),
                    &incumbent
                )
                .expect("initial live owner imports"),
            super::ExistingSeatAdoption::Bound(_)
        ));
        incumbent_process.kill().expect("incumbent process exits");
        incumbent_process.wait().expect("incumbent process reaped");

        let current = std::process::id();
        let current_metadata = fs::metadata(format!("/proc/{current}")).expect("current process");
        let current_stat =
            fs::read_to_string(format!("/proc/{current}/stat")).expect("current process stat");
        let (_, current_fields) = current_stat.rsplit_once(") ").expect("stat comm boundary");
        let challenger = binding(
            "challenger",
            i64::from(current),
            i64::from(current_metadata.uid()),
            current_fields
                .split_whitespace()
                .nth(19)
                .expect("process start token")
                .to_owned(),
        );
        assert!(matches!(
            store
                .adopt_existing_flow(
                    route("challenger"),
                    role("challenger"),
                    address.clone(),
                    &challenger
                )
                .expect("exact dead process owner can be replaced"),
            super::ExistingSeatAdoption::Bound(_)
        ));
        let adopted = store
            .stored_metaflow(&address)
            .expect("metaflow reads")
            .expect("metaflow remains");
        assert_eq!(adopted.metaflow.past, vec!["incumbent"]);
        assert_eq!(
            adopted.metaflow.state,
            flow_ethos::State::Awake("challenger".into())
        );
    }

    use super::{Configuration, ConfiguresOnlyStore, ReadsFlowMemory};

    use super::{LaysOutDefaults, SeedsConfiguration};

    use crate::store::delivery::RecordsDeliveries;
    use crate::store::delivery::{LeaseStep, PaneLease};
    use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
    use sema_engine::{
        Assertion, Engine, EngineOpen, EngineRecord, FamilyName, QueryPlan, SchemaHash,
        SchemaVersion, TableDescriptor, TableName,
    };
    use signal_flow::{
        Address, Aspect, Caller, ComposedLaunch, FirstPromptPayload, FlowOrigin, HarnessKind,
        HerdrPaneBinding, LaunchAttemptPhase, LaunchAttemptReservation, LaunchProfile, Model,
        NativeLaunchBinding, NativeLaunchIntent, NativeTargetReceipt, NativeTranscriptAbsence,
        NativeTranscriptBoundary, NativeTranscriptCursor, Origin, PromptDeliveryIntent,
        PromptDeliveryResult, Query, RegistrationAcknowledgement, Response, TargetReceiptRequest,
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

        /// A configured current-version store, as a deployment holds one.
        fn configured(&self) {
            let defaults = super::DefaultConfiguration {
                home: self.directory.path().join("home"),
                runtime_directory: self.directory.path().join("runtime"),
            };
            let config = ConfigurationOnlyStore::open(&self.path()).expect("fresh Memory opens");
            let settings = defaults.configuration();
            config
                .configure_change(meta_signal_flow::ConfigurationChange::Nexus(
                    meta_signal_flow::ConfigureNexus {
                        source_root: settings.source_root,
                        stable_codex: settings.stable_codex,
                        next_codex: settings.next_codex,
                        harness_profiles: settings.harness_profile_vector,
                        meta_aspects: settings.meta_aspects,
                        message_nexus_path_option: Some(settings.message_nexus_path.clone()),
                        message_nexus_binary_option: Some("/fixture/message-nexus".into()),

                        curriculum_nexus_path: "/fixture/curriculum.sock".into(),
                        lease: 60,
                        presentation_prompt_option: None,
                    },
                ))
                .expect("configuration commits");
        }

        /// The staged copies beside the store.
        fn staged_copies(&self) -> Vec<String> {
            std::fs::read_dir(self.directory.path())
                .expect("store directory lists")
                .map(|entry| {
                    entry
                        .expect("entry")
                        .file_name()
                        .to_string_lossy()
                        .into_owned()
                })
                .filter(|name| name.starts_with(".flow.sema.open-"))
                .collect()
        }

        fn configured_store(&self) -> FlowStore {
            FlowStore::open_configured(&self.path()).expect("configured store opens")
        }

        fn launch_profile(&self, launch_request_id: &str) -> LaunchProfile {
            LaunchProfile {
                launch_request_id: launch_request_id.into(),
                system_modules: vec!["spirit-core".into(), "intent-flow".into()],
                user_modules: vec!["operation-main-flow".into()],
                address: signal_flow::Address {
                    aspect: Aspect::Field,
                    topic: "flow".into(),
                    model: signal_flow::Model::Sol,
                },
                model_name: "fixture-model".into(),
                effort: "medium".into(),
                flow_id_option: None,
                remembering: signal_flow::Remembering::Predecessors(0),
                herdr_session_name: "fixture-session".into(),
                leave_outs_option: None,
                instruction_prompt: "fixture instruction".into(),
                flow_kind: signal_flow::FlowKind::Regular,
                tool_set: signal_flow::ToolSet::Default,
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
    fn zero_part_intent_requires_a_proven_empty_tool_free_simplified_launch() {
        use sha2::{Digest, Sha256};
        let empty_hash = format!("{:x}", Sha256::digest([]));
        for case in 0..6 {
            let fixture = StoreFixture::new();
            let store = fixture.store();
            let mut launch = fixture.composed_launch("idle-judgment", &empty_hash);
            launch.harness_kind = HarnessKind::Claude;
            launch.launch_profile.flow_kind = signal_flow::FlowKind::Simplified;
            launch.launch_profile.tool_set = signal_flow::ToolSet::Explicit(Vec::new());
            launch.launch_profile.instruction_prompt.clear();
            launch.first_prompt_payload.first_prompt_body.clear();
            launch.first_prompt_payload.prompt_part_vector.clear();
            match case {
                1 => launch.launch_profile.flow_kind = signal_flow::FlowKind::Regular,
                2 => launch.launch_profile.instruction_prompt = "Actual request".into(),
                3 => launch.launch_profile.tool_set = signal_flow::ToolSet::Default,
                4 => launch.harness_kind = HarnessKind::Codex,
                5 => launch.first_prompt_payload.prompt_sha256 = "1".repeat(64),
                _ => {}
            }
            assert!(matches!(
                store
                    .reserve_launch_attempt(&launch, fixture.origin())
                    .unwrap(),
                LaunchAttemptReservation::Reserved(_)
            ));
            let intent = NativeLaunchIntent {
                launch_request_id: launch.launch_profile.launch_request_id.clone(),
                prompt_sha256: launch.first_prompt_payload.prompt_sha256.clone(),
                harness_kind: launch.harness_kind.clone(),
                model_name: launch.launch_profile.model_name.clone(),
                effort: launch.launch_profile.effort.clone(),
                system_prompt_sha256: launch.system_prompt.system_prompt_sha256.clone(),
                prompt_sha256_vector: Vec::new(),
            };
            assert_eq!(
                store.record_native_launch_intent(intent).unwrap(),
                case == 0 || case == 4,
                "only the proven empty simplified startup is admitted, case {case}"
            );
            let attempt = store.launch_attempt("idle-judgment").unwrap().unwrap();
            assert_eq!(
                attempt.launch_attempt_phase,
                if case == 0 || case == 4 {
                    LaunchAttemptPhase::NativeLaunchIntentRecorded
                } else {
                    LaunchAttemptPhase::Reserved
                }
            );
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
            let mut store =
                <FlowStore as OpensFlowStore>::open(&self.directory.path().join("flow.sema"))
                    .expect("store opens");
            store.set_native_owner_environment_for_tests(NativeOwnerEnvironment {
                host_id: "isolated-test-host".into(),
                boot_id: "isolated-test-boot".into(),
            });
            store
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
                    message_nexus_path_option: Some(settings.message_nexus_path.clone()),
                    message_nexus_binary_option: Some("/fixture/message-nexus".into()),

                    curriculum_nexus_path: "/fixture/curriculum.sock".into(),
                    lease: 60,
                    presentation_prompt_option: None,
                },
            ))
            .expect("configuration commits");
        drop(config);

        let store = FlowStore::open_configured(&path).expect("configured Memory opens");
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
        let leases = reopened
            .engine
            .match_records(QueryPlan::all(reopened.delivery_tables.pane_leases))
            .expect("pane lease reads")
            .records()
            .to_vec();
        assert_eq!(leases.len(), 1, "either open did not settle the lease");
        assert_eq!(leases[0].delivery_id, "interrupted-delivery");
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

    /// Rewrites a store's Sema schema version, as an earlier Flow left it.
    fn stamp_schema_version(path: &std::path::Path, version: u64) {
        let database = redb::Database::open(path).expect("store opens directly");
        let write = database.begin_write().expect("meta transaction starts");
        {
            let mut meta = write
                .open_table(super::SEMA_META_TABLE)
                .expect("meta table opens");
            meta.insert(super::SEMA_SCHEMA_VERSION_KEY, version)
                .expect("schema version writes");
        }
        write.commit().expect("meta commits");
    }

    /// A store an earlier Flow wrote is Flow Memory v2: no open upgrades it.
    /// Each open refuses it typed, as an earlier Flow's store, and leaves it
    /// byte for byte as it was.
    #[test]
    fn an_earlier_flows_store_is_refused_and_left_byte_for_byte() {
        let fixture = StoreFixture::new();
        fixture.configured();
        drop(fixture.configured_store());
        stamp_schema_version(&fixture.path(), 2);
        let path = fixture.path();
        let before = std::fs::read(&path).expect("earlier store reads");
        let entries = directory_entries(&path);
        let earlier = |error: super::StoreError| match error {
            super::StoreError::EarlierMemory {
                path: refused,
                version,
            } => {
                assert_eq!(refused, path.display().to_string());
                assert_eq!(version, 2);
            }
            other => panic!("an earlier store is refused as one: {other:?}"),
        };
        earlier(ConfigurationOnlyStore::open(&path).err().expect("refused"));
        earlier(FlowStore::open_configured(&path).err().expect("refused"));
        assert_eq!(std::fs::read(&path).expect("earlier store rereads"), before);
        assert_eq!(directory_entries(&path), entries, "no copy is left");
    }

    /// A Memory v3 store holding a family under the identity an earlier
    /// Flow registered it with, the 0.51 metaflow family, is refused before
    /// anything is written: no family is upgraded on open.
    #[test]
    fn a_family_under_an_earlier_identity_is_refused_not_upgraded() {
        let fixture = StoreFixture::new();
        fixture.configured();
        drop(fixture.configured_store());
        let path = fixture.path();
        {
            let database = redb::Database::open(&path).expect("store opens directly");
            let write = database.begin_write().expect("catalog transaction starts");
            {
                let mut catalog = write
                    .open_table(super::SEMA_CATALOG_TABLE)
                    .expect("catalog opens");
                catalog
                    .remove("flow_nexus_metaflows")
                    .expect("metaflow family removes");
            }
            let _ = write.delete_table(redb::TableDefinition::<&str, &[u8]>::new(
                "flow_nexus_metaflows",
            ));
            write.commit().expect("catalog commits");
        }
        {
            #[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone)]
            struct EarlierMetaflow {
                key: String,
            }
            impl EngineRecord for EarlierMetaflow {
                fn record_key(&self) -> sema_engine::RecordKey {
                    sema_engine::RecordKey::new(self.key.clone())
                }
            }
            let mut engine = Engine::open(EngineOpen::new(
                &path,
                SchemaVersion::new(super::FLOW_SCHEMA_VERSION),
            ))
            .expect("store opens");
            engine
                .register_table(TableDescriptor::<EarlierMetaflow>::new(
                    TableName::new("flow_nexus_metaflows"),
                    FamilyName::new("flow-nexus-metaflow"),
                    SchemaHash::for_label("flow-nexus-metaflow-v6"),
                ))
                .expect("the earlier metaflow family registers");
        }
        let before = std::fs::read(&path).expect("store reads");
        for refused in [
            ConfigurationOnlyStore::open(&path).err(),
            FlowStore::open_configured(&path).err(),
        ] {
            assert!(
                matches!(
                    &refused,
                    Some(super::StoreError::UnrecognizedMemory { reason, .. })
                        if reason.contains("flow_nexus_metaflows")
                ),
                "{refused:?}"
            );
        }
        assert_eq!(std::fs::read(&path).expect("store rereads"), before);
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
    fn a_role_is_kept_with_its_binding_and_a_different_one_conflicts() {
        let fixture = StoreFixture::new();
        let store = fixture.store();
        let role = signal_flow::Caller {
            flow_id: "psyche-high".into(),
            aspect: Aspect::Psyche,
            model_name: "claude-opus-5-5".into(),
        };
        let node = pane_node("psyche-high", "w1:p3");
        assert!(matches!(
            store
                .register_flow_in_role(node.clone(), role.clone())
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

    /// A crash in the first running open of a configuration-only store,
    /// between two of the families it adds, leaves the configuration-only
    /// store it was; the next open adds every running family and clears
    /// the copy the crash left.
    #[test]
    fn a_crash_between_added_families_leaves_the_configuration_only_store() {
        use super::staged_open::{InterruptsStagedOpen, StagedOpen, StagesOpen};
        let fixture = StoreFixture::new();
        fixture.configured();
        let bytes = std::fs::read(fixture.path()).expect("configuration-only store reads");
        let staged = StagedOpen::begin(&fixture.path())
            .expect("staging starts")
            .expect("a configuration-only store has running families to add");
        {
            let mut engine = Engine::open(EngineOpen::new(
                staged.copy(),
                SchemaVersion::new(super::FLOW_SCHEMA_VERSION),
            ))
            .expect("the copy opens");
            engine
                .register_table(TableDescriptor::<super::FlowRecord>::new(
                    super::FLOW_TABLE_NAME,
                    FamilyName::new("flow-nexus-flow"),
                    <super::FlowRecord as sema_engine::MemoryDefinition>::schema_hash(),
                ))
                .expect("one running family registers on the copy");
        }
        staged.crash();
        assert!(
            std::fs::read(fixture.path()).expect("store reads") == bytes,
            "the store is the configuration-only store it was"
        );
        assert_eq!(fixture.staged_copies().len(), 1, "the crash left its copy");
        drop(fixture.configured_store());
        assert_eq!(fixture.staged_copies(), Vec::<String>::new());
        let tables = {
            use redb::{ReadableDatabase as _, ReadableTable as _};
            let database = redb::ReadOnlyDatabase::open(fixture.path()).expect("store reads");
            let read = database.begin_read().expect("read starts");
            let catalog = read
                .open_table(super::SEMA_CATALOG_TABLE)
                .expect("catalog opens");
            catalog
                .iter()
                .expect("catalog iterates")
                .map(|entry| entry.expect("catalog entry").0.value().to_owned())
                .collect::<std::collections::BTreeSet<_>>()
        };
        assert_eq!(
            tables,
            super::RUNNING_TABLES
                .iter()
                .map(|table| (*table).to_owned())
                .collect::<std::collections::BTreeSet<_>>()
        );
    }

    #[test]
    fn a_legacy_configuration_catalog_without_handover_families_upgrades_idempotently() {
        let fixture = StoreFixture::new();
        fixture.configured();
        let database = redb::Database::open(fixture.path()).expect("legacy catalog opens");
        {
            let write = database.begin_write().expect("legacy catalog writes");
            let mut catalog = write
                .open_table(super::SEMA_CATALOG_TABLE)
                .expect("catalog opens");
            catalog
                .delete(super::OPTIONAL_HANDOVER_TABLE)
                .expect("configuration family removes");
            catalog
                .delete(super::OPTIONAL_HANDOVER_RUNTIME_TABLE)
                .expect("runtime family removes");
            write.commit().expect("legacy catalog commits");
        }
        let read = database.begin_read().expect("legacy catalog reads");
        let catalog = read
            .open_table(super::SEMA_CATALOG_TABLE)
            .expect("catalog opens");
        assert!(
            catalog
                .get(super::OPTIONAL_HANDOVER_TABLE)
                .unwrap()
                .is_none()
        );
        assert!(
            catalog
                .get(super::OPTIONAL_HANDOVER_RUNTIME_TABLE)
                .unwrap()
                .is_none()
        );
        drop(read);
        drop(database);
        let configuration = ConfigurationOnlyStore::open(&fixture.path()).expect("legacy opens");
        configuration
            .configure_change(meta_signal_flow::ConfigurationChange::HandoverCount(0))
            .expect("handover config commits");
        drop(configuration);
        for _ in 0..2 {
            let reopened = ConfigurationOnlyStore::open(&fixture.path()).expect("legacy reopens");
            assert_eq!(
                reopened
                    .configuration_snapshot()
                    .unwrap()
                    .unwrap()
                    .handover_count,
                meta_signal_flow::HandoverCountConfiguration::Configured(0)
            );
            drop(reopened);
        }
    }

    /// A store 0.53.0 wrote holds every running family but the one 0.53.1
    /// added, ProtectedAddresses: it is opened, that family registered
    /// empty, and nothing is protected. A store missing any other family is
    /// still refused, byte for byte as it was.
    #[test]
    fn a_store_0530_wrote_opens_with_no_address_protected() {
        use super::ManagesMetaflowState;
        let strip = |path: &std::path::Path, table: &str| {
            let database = redb::Database::open(path).expect("store opens raw");
            let write = database.begin_write().expect("write starts");
            {
                let mut catalog = write
                    .open_table(super::SEMA_CATALOG_TABLE)
                    .expect("catalog opens");
                assert!(
                    catalog
                        .remove(table)
                        .expect("catalog entry removed")
                        .is_some()
                );
            }
            // A family no row was written to has no table of its own yet;
            // one that has is dropped with its rows.
            write
                .delete_table(redb::TableDefinition::<&str, &[u8]>::new(table))
                .expect("table dropped");
            write.commit().expect("strip commits");
        };
        let fixture = StoreFixture::new();
        fixture.configured();
        drop(fixture.configured_store());
        strip(
            &fixture.path(),
            "flow_nexus_meta_protected_address_configuration",
        );
        let store = fixture.configured_store();
        assert_eq!(
            store
                .configuration_snapshot()
                .expect("snapshot reads")
                .expect("configured")
                .protected_addresses,
            Vec::<signal_flow::Address>::new()
        );
        drop(store);

        let other = StoreFixture::new();
        other.configured();
        drop(other.configured_store());
        strip(&other.path(), "flow_nexus_meta_heartbeat_configuration");
        let before = std::fs::read(other.path()).expect("store reads");
        assert!(matches!(
            FlowStore::open_configured(&other.path()),
            Err(super::StoreError::UnrecognizedMemory { .. })
        ));
        assert_eq!(std::fs::read(other.path()).expect("store rereads"), before);
    }

    /// A crash-left copy goes at the next open, one that adds nothing
    /// included.
    #[test]
    fn a_crash_left_copy_is_removed_by_an_open_that_adds_nothing() {
        use super::ConfiguresOnlyStore;
        let fixture = StoreFixture::new();
        fixture.configured();
        drop(fixture.configured_store());
        let leave = |fixture: &StoreFixture| {
            std::fs::write(fixture.directory.path().join(".flow.sema.open-1-2"), b"x")
                .expect("copy planted");
            assert_eq!(fixture.staged_copies().len(), 1);
        };
        leave(&fixture);
        drop(super::ConfigurationOnlyStore::open(&fixture.path()).expect("configuration open"));
        leave(&fixture);
        drop(fixture.configured_store());
        assert_eq!(fixture.staged_copies(), Vec::<String>::new());
    }

    /// A copy a live staging open holds is not removed, and the open that
    /// meets it is refused as the store held.
    #[test]
    fn a_copy_held_by_a_live_staging_open_stays_and_contention_is_a_lock_refusal() {
        use super::staged_open::{StagedOpen, StagesOpen};
        let fixture = StoreFixture::new();
        fixture.configured();
        let staged = StagedOpen::begin(&fixture.path())
            .expect("staging starts")
            .expect("a configuration-only store has running families to add");
        assert_eq!(fixture.staged_copies().len(), 1);
        let refused = StagedOpen::begin(&fixture.path());
        assert!(
            matches!(refused, Err(super::StoreError::StoreHeld { .. })),
            "contention is a lock refusal"
        );
        assert_eq!(fixture.staged_copies().len(), 1, "the held copy stays");
        drop(staged);
        assert_eq!(fixture.staged_copies(), Vec::<String>::new());
    }
}
