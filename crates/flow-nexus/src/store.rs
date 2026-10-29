//! Durable Flow Nexus identity and dispatch state.
//!
//! The ordinary Signal contract remains the public boundary.  This module
//! owns its single `.sema` store and lowers a closed `signal_flow::Query`
//! into its typed, durable records.

use crate::store::delivery::RegistersDeliveryTables;
pub(crate) mod address_hold;
pub mod compaction;
pub mod context;
mod created_pane;
pub mod delivery;
pub mod events;
mod full_configuration;
pub(crate) mod harness;
pub(crate) mod launch_cleanup;
pub(crate) mod logical_delivery;
pub(crate) mod metaflow;
pub(crate) mod metaflow_declaration;
pub(crate) mod nexus_configuration;
pub(crate) mod opened_thread;
pub(crate) mod queue_delivery;
pub(crate) mod repair;
pub(crate) mod send_call;
pub(crate) mod send_outcome;
pub(crate) mod send_server;
pub(crate) mod send_v31;
pub(crate) mod send_v32;
pub(crate) mod send_v33;
pub(crate) mod sent;
mod staged_upgrade;
pub(crate) mod successor;
pub(crate) mod texts;
pub(crate) mod timings_limits;
use address_hold::HoldsLaunchAddress;
use created_pane::StoredLaunchPane;
pub use created_pane::{ReadsCreatedLaunchPane, RecordsCreatedLaunchPane};
use flow_ethos::State as MetaflowState;
use harness::UpgradesHarnesses;
use logical_delivery::{
    DeclaresLogicalDeliveryTables, LogicalDeliveryTables, RecordsLogicalDeliveries,
};
use metaflow::{TakesLaunchAddress, WritesTakenAddress};
use metaflow_declaration::UpgradesMetaflowDeclarations;
use nexus_configuration::{
    DescribesNexusConfigurationTable, StoredMetaNexusConfiguration, UpgradesNexusConfiguration,
};
use queue_delivery::{AccountsQueueDelivery, QueueDeliveryPhase};
use queue_delivery::{DeclaresQueueDeliveryTables, QueueDeliveryTables};
use send_outcome::{DeclaresSendOutcomeTables, SendOutcomeTables};
use staged_upgrade::{StagedUpgrade, StagesUpgrade, UpgradeScope};
use texts::RegistersTextsTable;
use timings_limits::RegistersTimingAndLimitTables;

use delivery::{DeliveryConfiguration, DeliveryTables, RecordsDeliveries};
use std::{
    fs,
    os::unix::fs::MetadataExt,
    path::Path,
    sync::{Arc, Condvar, Mutex},
};

use meta_signal_flow::{
    ConfigurationChange, ConfigurationEntry, ConfigureModel, ConfigureNexus, ConfigureOutcome,
    ConfigureThreshold, EntryKey,
};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use sema_engine::{
    Assertion, AtomicCommit, DatabaseMarker, Decision, Engine, EngineOpen, EngineRecord, FamilyName,
    KeyedMutation, MemoryDefinition, MemoryUpgrades, MemoryVersion, QueryPlan, RecordKey,
    Retraction, SchemaHash, SchemaVersion, TableDescriptor, TableName, TableReference,
    TableRegistration, UpgradeFrom, UpgradeOutcome,
};
use signal_flow::{
    Address, Aspect, Caller, CallerResolutionRejection, ComposedLaunch, EndpointSelection, FlowId,
    FlowLifecycle as SignalFlowLifecycle, FlowNode, HarnessKind, HerdrPaneBinding, HerdrRoute,
    HerdrRouteSelection, LaunchAttempt, LaunchAttemptPhase, LaunchAttemptReservation,
    LaunchProfile, Launched, Layer, Lock, Metaflow, NativeLaunchBinding, NativeLaunchIntent,
    NativeTranscriptBoundary, Origin, Process, PromptDeliveryIntent, PromptDeliveryResult, Query,
    RecipientResolutionRejection, RegistrationAcknowledgement, Remembering, ReplaceRejection,
    Replaced, Request, Response, RouteReadiness, StartRejection, StopRejection,
};

/// Applies the meta wire's index cursor to an already ordered snapshot.
/// `after` is the number of rows consumed; `next_page` is the cursor for
/// the next request and is absent once the snapshot is exhausted.
pub(crate) fn page_records<T: Clone>(
    records: &[T],
    page: &meta_signal_flow::Page,
) -> (Vec<T>, meta_signal_flow::NextPage) {
    let start = usize::try_from(page.after.max(0))
        .unwrap_or(usize::MAX)
        .min(records.len());
    let count = usize::try_from(page.count.max(0)).unwrap_or(usize::MAX);
    let end = start.saturating_add(count).min(records.len());
    let next = (end < records.len()).then(|| i64::try_from(end).unwrap_or(i64::MAX));
    (records[start..end].to_vec(), next)
}

type LeaveOut = String;

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
enum LeaveOutV37 {
    ClaudeAiConnectors,
    ConfiguredMcpServers,
    AgentTool,
    ExplorePlanAgents,
    FileTools,
    WorkflowTool,
    ScheduleWakeupTool,
    ReportFindingsTool,
    ListAgentsTool,
    CronTools,
    WebTools,
    DeferredTools,
    SkillTool,
    BundledSkills,
    ClaudeMd,
    GitInstructions,
    AttributionReminder,
    UserSettings,
    ToolSearch,
    BashTool,
    CodexSkillListing,
    CodexAgentsMd,
    CodexCollaborationMode,
    CodexEnvironmentContext,
    CodexPermissions,
    CodexDeveloperInstructions,
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
enum LeaveOutRefusalV37 {
    Indispensable(LeaveOutV37),
    NeedsWorkflowLeftOut(LeaveOutV37),
    NotForHarness(LeaveOutV37),
}

impl LeaveOutV37 {
    fn harness(&self) -> HarnessKind {
        match self {
            Self::CodexSkillListing
            | Self::CodexAgentsMd
            | Self::CodexCollaborationMode
            | Self::CodexEnvironmentContext
            | Self::CodexPermissions
            | Self::CodexDeveloperInstructions => HarnessKind::Codex,
            _ => HarnessKind::Claude,
        }
    }

    fn module_names(&self) -> &'static [&'static str] {
        match self {
            Self::ClaudeAiConnectors => &["ClaudeAiConnectors"],
            Self::ConfiguredMcpServers => &["McpServers"],
            Self::AgentTool => &["Agent"],
            Self::ExplorePlanAgents => &["ExplorePlanAgents"],
            Self::FileTools => &["Read", "Edit", "Write"],
            Self::WorkflowTool => &["Workflow"],
            Self::ScheduleWakeupTool => &["ScheduleWakeup"],
            Self::ReportFindingsTool => &["ReportFindings"],
            Self::ListAgentsTool => &["ListAgents"],
            Self::CronTools => &["Cron"],
            Self::WebTools => &["WebFetch", "WebSearch"],
            Self::DeferredTools => &[
                "Cron",
                "DesignSync",
                "Worktree",
                "LSP",
                "Monitor",
                "NotebookEdit",
                "PushNotification",
                "RemoteTrigger",
                "SendMessage",
                "TaskStop",
                "WebFetch",
                "WebSearch",
            ],
            Self::SkillTool => &["Skill", "SlashCommands"],
            Self::BundledSkills => &["BundledSkills"],
            Self::ClaudeMd => &["ClaudeMd"],
            Self::GitInstructions => &["GitInstructions"],
            Self::AttributionReminder => &["Attribution"],
            Self::UserSettings => &["UserSettings"],
            Self::ToolSearch => &["ToolSearch"],
            Self::BashTool => &["Bash"],
            Self::CodexSkillListing => &["Skills"],
            Self::CodexAgentsMd => &["AgentsMd"],
            Self::CodexCollaborationMode => &["CollaborationMode"],
            Self::CodexEnvironmentContext => &["EnvironmentContext"],
            Self::CodexPermissions => &["Permissions"],
            Self::CodexDeveloperInstructions => &["DeveloperInstructions"],
        }
    }
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
struct ConfigureLeaveOutV47 {
    address: Address,
    harness_kind: HarnessKind,
    left_out: Vec<LeaveOutV37>,
}

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
const FLOW_QUARANTINED_LAUNCH_ATTEMPT_TABLE_NAME: TableName =
    TableName::new("flow_nexus_quarantined_launch_attempts");
const FLOW_QUARANTINED_LAUNCH_OUTCOME_TABLE_NAME: TableName =
    TableName::new("flow_nexus_quarantined_launch_outcomes");
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
    "flow-nexus-meta-model-threshold-configuration-v2";
const META_INVESTIGATION_CONFIGURATION_TABLE_NAME: TableName =
    TableName::new("flow_nexus_meta_investigation_configuration");
const META_INVESTIGATION_CONFIGURATION_FAMILY: &str = "flow-nexus-meta-investigation-configuration";
const META_INVESTIGATION_CONFIGURATION_SCHEMA: &str =
    "flow-nexus-meta-investigation-configuration-v1";
const META_LEAVE_OUT_CONFIGURATION_TABLE_NAME: TableName =
    TableName::new("flow_nexus_meta_leave_out_configuration");
/// The tables an older store kept and an open drops: the meta
/// configuration's topic modules, which meta-signal-flow 20.0.0 no longer
/// configures, and the system-prompt bundle 0.34.6 kept for each Address,
/// both dropped from 0.35.0 on; and the fresh-wake marks, which chose
/// between resuming a sleeping flow and waking it fresh, dropped from
/// 0.35.22 on, since every wake is fresh; and the thresholds kept one per
/// layer with a handover, dropped from 0.35.25 on, since a threshold
/// belongs to a model and no flow writes a handover. Each is named with the
/// family identity it was registered under.
const RETIRED_TABLES: &[(&str, &str, &str)] = &[
    (
        "flow_nexus_runtime_configuration",
        "flow-nexus-runtime-configuration",
        "flow-nexus-runtime-configuration-v1",
    ),
    (
        "flow_nexus_delivery_configuration",
        "flow-nexus-delivery-configuration",
        "flow-nexus-delivery-configuration-v1",
    ),
    (
        "flow_nexus_meta_leave_out_configuration",
        "flow-nexus-meta-leave-out-configuration",
        "flow-nexus-meta-leave-out-configuration-v2",
    ),
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
    (
        "flow_nexus_metaflow_refreshes",
        "flow-nexus-metaflow-refresh",
        "flow-nexus-metaflow-refresh-v1",
    ),
    (
        "flow_nexus_meta_threshold_configuration",
        "flow-nexus-meta-threshold-configuration",
        "flow-nexus-meta-threshold-configuration-v1",
    ),
    (
        "flow_nexus_landings",
        "flow-nexus-landing",
        "flow-nexus-landing-v1",
    ),
    (
        "flow_nexus_refresh_due",
        "flow-nexus-refresh-due",
        "flow-nexus-refresh-due-v1",
    ),
];
/// The tables a configuration-only store holds, all of them.
const CONFIGURATION_ONLY_TABLES: &[&str] = &[
    "flow_nexus_state",
    "flow_nexus_configuration",
    "flow_nexus_meta_nexus_configuration",
    "flow_nexus_meta_model_configuration",
];
/// The tables every running store holds.
const RUNNING_TABLES: &[&str] = &[
    "flow_nexus_flows",
    "flow_nexus_state",
    "flow_nexus_metaflows",
    "flow_nexus_metaflow_processes",
    "flow_nexus_metaflow_locks",
    "flow_nexus_configuration",
    "flow_nexus_herdr_routes",
    "flow_nexus_launch_attempts",
    "flow_nexus_launch_panes",
    "flow_nexus_launch_outcomes",
    "flow_nexus_replacements",
    "flow_nexus_roles",
    "flow_nexus_quarantined_launch_attempts",
    "flow_nexus_quarantined_launch_outcomes",
    "flow_nexus_deliveries",
    "flow_nexus_pane_leases",
    "flow_nexus_meta_nexus_configuration",
    "flow_nexus_meta_model_configuration",
    "flow_nexus_flow_events",
];
const RUNNING_ADDED_TABLES: &[&str] = &["flow_nexus_native_owner_proofs"];
// A running store keeps each flow's context size from 0.28.0 on, and
// each launch's Address hold from 0.32.0 on; a running store written
// before either opens, and its first full open adds the table, as
// the first full open of a configuration-only store adds the running
// tables.
// A store keeps its metaflows' default leave-outs from 0.35.0 on, the
// Codex thread each launch opened from 0.35.6 on, the send tool's calls from 0.35.7
// on, the cleanup of each interrupted launch from 0.35.11 on, and
// its trusted send servers and queued-delivery accounting from
// 0.35.19 on. It records each ordinary Send outcome from 0.38.1 on.
/// The tables a running store holds once this Flow has opened it.
const ADDED_LATER_TABLES: &[&str] = &[
    "flow_nexus_meta_harness_configuration",
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
    "flow_nexus_meta_timings_configuration",
    "flow_nexus_meta_limits_configuration",
    "flow_nexus_meta_texts_configuration",
    "flow_nexus_meta_model_threshold_configuration",
    "flow_nexus_meta_investigation_configuration",
    "flow_nexus_compactions",
    "flow_nexus_successor_intents",
    "flow_nexus_send_outcome_counter",
    "flow_nexus_send_outcomes",
    "flow_nexus_logical_delivery_counter",
    "flow_nexus_logical_delivery_current",
    "flow_nexus_logical_delivery_events",
    "flow_nexus_simplified_reply_bindings",
    "flow_nexus_full_configuration_state",
    "flow_nexus_full_configuration_receipts",
];
/// The tables a configuration-only store may hold beside its own, each
/// added by a later Flow.
const CONFIGURATION_ONLY_ADDED_TABLES: &[&str] = &[
    "flow_nexus_meta_harness_configuration",
    "flow_nexus_meta_leave_out_configuration",
    "flow_nexus_meta_send_server_configuration",
    "flow_nexus_meta_model_threshold_configuration",
    "flow_nexus_meta_investigation_configuration",
    "flow_nexus_meta_metaflow_configuration",
    "flow_nexus_meta_timings_configuration",
    "flow_nexus_meta_limits_configuration",
    "flow_nexus_meta_texts_configuration",
    "flow_nexus_full_configuration_state",
    "flow_nexus_full_configuration_receipts",
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

/// The store-owned configuration families. The runtime `Start` belongs to the
/// running Nexus and is composed into the public snapshot at dispatch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredConfigurationSnapshot {
    pub configured_nexus: meta_signal_flow::ConfigureNexus,
    pub configured_harnesses: Vec<meta_signal_flow::Harness>,
    pub configured_models: Vec<meta_signal_flow::ConfigureModel>,
    pub configured_thresholds: Vec<meta_signal_flow::ConfigureThreshold>,
    pub configured_send_servers: Vec<meta_signal_flow::ConfigureSendServer>,
    pub configured_metaflows: Vec<meta_signal_flow::ConfigureMetaflow>,
    pub configured_investigation: Option<flow_ethos::Address>,
    pub timings: meta_signal_flow::Timings,
    pub limits: meta_signal_flow::Limits,
    pub texts: meta_signal_flow::Texts,
}

/// One coherent committed view of every mutable configuration family.
/// `generation` is the database marker shared by every row read into the
/// snapshot, and names the configuration generation an operation uses.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommittedConfigurationSnapshot {
    pub generation: DatabaseMarker,
    pub snapshot: StoredConfigurationSnapshot,
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
        let absent_endpoint = || CodexEndpointConfiguration {
            client_path: String::new(),
            home: String::new(),
            socket: String::new(),
            model_names: Vec::new(),
        };
        RuntimeConfiguration {
            source_root: self.source_root().to_string_lossy().into_owned(),
            stable_codex: absent_endpoint(),
            next_codex: absent_endpoint(),
        }
    }

    /// Every aspect reaches the meta socket by default. No Message Nexus
    /// executable is admitted by path until one is configured.
    fn delivery_configuration(&self) -> DeliveryConfiguration {
        use delivery::DefaultsHarnessProfile;
        DeliveryConfiguration {
            harness_profile_vector: vec![
                HarnessKind::Claude.default_profile(),
                HarnessKind::Codex.default_profile(),
            ],
            meta_aspects: vec![Aspect::Psyche, Aspect::Mind, Aspect::Field],
            message_nexus_path: String::new(),
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

trait ReadsNativeOwnerEnvironment {
    fn native_owner_environment(&self) -> Option<NativeOwnerEnvironment>;
}

trait ReadsSystemNativeOwnerEnvironment {
    fn system() -> Option<NativeOwnerEnvironment>;
}

impl ReadsNativeOwnerEnvironment for NativeOwnerEnvironment {
    fn native_owner_environment(&self) -> Option<NativeOwnerEnvironment> {
        Self::system()
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
        self.native_owner_environment_override
            .clone()
            .or_else(NativeOwnerEnvironment::system)
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
        MemoryVersion::new(5)
    }

    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label("flow-nexus-metaflow-v5")
    }
}

/// Queue shape from before logical-delivery identities. Keep this mirror
/// distinct: the new QueueEntry has a different archived representation.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
struct QueuedRequestBeforeLogicalId {
    sent_by: flow_ethos::SentBy,
    request: Request,
}
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
enum SentByBeforeFlow {
    Sender(flow_ethos::Sender),
    Unknown,
}
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
struct QueuedRequestBeforeFlow {
    sent_by: SentByBeforeFlow,
    request: Request,
}
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
struct MetaflowBeforeFlow {
    address: Address,
    state: MetaflowState,
    past: Vec<String>,
    queue: Vec<QueuedRequestBeforeFlow>,
}
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
struct MetaflowBeforeLogicalIds {
    address: Address,
    state: MetaflowState,
    past: Vec<String>,
    queue: Vec<QueuedRequestBeforeLogicalId>,
}
trait LogicalQueueConvertible {
    fn into_logical_queue(self, address: &Address) -> Vec<flow_ethos::QueueEntry>;
}
impl LogicalQueueConvertible for Vec<QueuedRequestBeforeLogicalId> {
    fn into_logical_queue(self, address: &Address) -> Vec<flow_ethos::QueueEntry> {
        self.into_iter()
            .enumerate()
            .map(|(index, old)| flow_ethos::QueueEntry {
                logical_delivery_id: format!(
                    "legacy:{}:{index:016x}",
                    FlowStore::address_record_key(address)
                ),
                sent_by: old.sent_by,
                delivery: flow_ethos::Delivery::Queue,
                message: flow_ethos::Message::Soft(old.request),
            })
            .collect()
    }
}
trait LogicalMetaflowConvertible {
    fn into_logical_metaflow(self) -> Metaflow;
}
impl LogicalMetaflowConvertible for MetaflowBeforeLogicalIds {
    fn into_logical_metaflow(self) -> Metaflow {
        let queue = self.queue.into_logical_queue(&self.address);
        Metaflow {
            address: self.address,
            state: self.state,
            past: self.past,
            queue,
        }
    }
}
impl LogicalMetaflowConvertible for MetaflowBeforeFlow {
    fn into_logical_metaflow(self) -> Metaflow {
        let queue = self
            .queue
            .into_iter()
            .map(|entry| QueuedRequestBeforeLogicalId {
                sent_by: match entry.sent_by {
                    SentByBeforeFlow::Sender(sender) => flow_ethos::SentBy::Sender(sender),
                    SentByBeforeFlow::Unknown => flow_ethos::SentBy::Unknown,
                },
                request: entry.request,
            })
            .collect();
        MetaflowBeforeLogicalIds {
            address: self.address,
            state: self.state,
            past: self.past,
            queue,
        }
        .into_logical_metaflow()
    }
}

/// The last metaflow shape before stable logical identities, stored at
/// memory v4. Historical queue entries retain their payload and sender,
/// but receive deterministic migration IDs and are not linked to attempts.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct StoredMetaflowV3 {
    metaflow: MetaflowBeforeFlow,
}

impl EngineRecord for StoredMetaflowV3 {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(FlowStore::address_record_key(&self.metaflow.address))
    }
}

impl MemoryDefinition for StoredMetaflowV3 {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(3)
    }

    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label("flow-nexus-metaflow-v3")
    }
}

impl UpgradeFrom<StoredMetaflowV3> for StoredMetaflow {
    type Refusal = std::convert::Infallible;

    fn upgrade_from(previous: StoredMetaflowV3) -> Decision<Self, Self::Refusal> {
        Decision::Ready(Self {
            metaflow: (previous.metaflow).into_logical_metaflow(),
        })
    }
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct StoredMetaflowV4 {
    metaflow: MetaflowBeforeLogicalIds,
}
impl EngineRecord for StoredMetaflowV4 {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(FlowStore::address_record_key(&self.metaflow.address))
    }
}
impl MemoryDefinition for StoredMetaflowV4 {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(4)
    }
    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label("flow-nexus-metaflow-v4")
    }
}
impl UpgradeFrom<StoredMetaflowV4> for StoredMetaflow {
    type Refusal = std::convert::Infallible;
    fn upgrade_from(previous: StoredMetaflowV4) -> Decision<Self, Self::Refusal> {
        Decision::Ready(Self {
            metaflow: (previous.metaflow).into_logical_metaflow(),
        })
    }
}

/// A metaflow's state as Flow kept it before 0.35.22, when a metaflow
/// could be ended. It is read only by the upgrades of the metaflows and of
/// the Address holds that keep a metaflow as found.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) enum StateBeforeIdle {
    Awake(String),
    Asleep,
    Ended,
}

/// A topic never dies; it goes idle. An ended metaflow is carried Asleep,
/// its past and its queue kept.
impl From<StateBeforeIdle> for MetaflowState {
    fn from(previous: StateBeforeIdle) -> Self {
        match previous {
            StateBeforeIdle::Awake(flow_id) => Self::Awake(flow_id),
            StateBeforeIdle::Asleep | StateBeforeIdle::Ended => Self::Asleep,
        }
    }
}

/// A metaflow as Flow 0.35.7 to 0.35.21 kept it, its state possibly Ended.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct MetaflowBeforeIdle {
    address: Address,
    state: StateBeforeIdle,
    past: Vec<String>,
    queue: Vec<QueuedRequestBeforeFlow>,
}

impl From<MetaflowBeforeIdle> for Metaflow {
    fn from(previous: MetaflowBeforeIdle) -> Self {
        (MetaflowBeforeFlow {
            address: previous.address,
            state: previous.state.into(),
            past: previous.past,
            queue: previous.queue,
        })
        .into_logical_metaflow()
    }
}

/// A metaflow as Flow kept it before 0.35.7, its queue of bare requests
/// with no sender. It is read only by the v1 family upgrades of the
/// metaflows and of the Address holds that keep a metaflow as found.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct MetaflowBeforeSenders {
    address: Address,
    state: StateBeforeIdle,
    past: Vec<String>,
    queue: Vec<Request>,
}

/// Each request queued before Flow kept senders is carried with its sender
/// Unknown, and an ended metaflow Asleep; nothing else changes.
impl From<MetaflowBeforeSenders> for Metaflow {
    fn from(previous: MetaflowBeforeSenders) -> Self {
        (MetaflowBeforeFlow {
            address: previous.address,
            state: previous.state.into(),
            past: previous.past,
            queue: previous
                .queue
                .into_iter()
                .map(|request| QueuedRequestBeforeFlow {
                    sent_by: SentByBeforeFlow::Unknown,
                    request,
                })
                .collect(),
        })
        .into_logical_metaflow()
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

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct StoredMetaflowV2 {
    metaflow: MetaflowBeforeIdle,
}

impl EngineRecord for StoredMetaflowV2 {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(FlowStore::address_record_key(&self.metaflow.address))
    }
}

impl MemoryDefinition for StoredMetaflowV2 {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(2)
    }

    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label("flow-nexus-metaflow-v2")
    }
}

impl UpgradeFrom<StoredMetaflowV2> for StoredMetaflow {
    type Refusal = std::convert::Infallible;

    fn upgrade_from(previous: StoredMetaflowV2) -> Decision<Self, Self::Refusal> {
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
    /// The exact flow authenticated when this process was bound. Historical
    /// bindings have no authentic flow identity and cannot send.
    flow_id_option: Option<FlowId>,
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

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct StoredMetaflowProcessV1 {
    process: Process,
    address: Address,
}

pub(crate) enum ProcessSenderResolution {
    NoBinding,
    MissingFlowIdentity,
    StaleFlowIdentity,
    Sender(meta_signal_flow::Sender),
}

impl EngineRecord for StoredMetaflowProcessV1 {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(FlowStore::address_record_key(&self.address))
    }
}

impl MemoryDefinition for StoredMetaflowProcessV1 {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(1)
    }

    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label("flow-nexus-metaflow-process-v1")
    }
}

impl UpgradeFrom<StoredMetaflowProcessV1> for StoredMetaflowProcess {
    type Refusal = std::convert::Infallible;

    fn upgrade_from(previous: StoredMetaflowProcessV1) -> Decision<Self, Self::Refusal> {
        Decision::Ready(Self {
            process: previous.process,
            address: previous.address,
            flow_id_option: None,
        })
    }
}

/// At most one lock is held for a recipient address.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct StoredMetaflowLock {
    lock: Lock,
    /// The flow authenticated when the lease was created. A historical
    /// lease has no authentic flow identity and can never deliver.
    sender_flow_id_option: Option<FlowId>,
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

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
struct StoredMetaflowLockV1 {
    lock: Lock,
}

impl EngineRecord for StoredMetaflowLockV1 {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(FlowStore::address_record_key(&self.lock.address))
    }
}

impl MemoryDefinition for StoredMetaflowLockV1 {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(1)
    }

    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label("flow-nexus-metaflow-lock-v1")
    }
}

impl UpgradeFrom<StoredMetaflowLockV1> for StoredMetaflowLock {
    type Refusal = std::convert::Infallible;

    fn upgrade_from(previous: StoredMetaflowLockV1) -> Decision<Self, Self::Refusal> {
        Decision::Ready(Self {
            lock: previous.lock,
            sender_flow_id_option: None,
        })
    }
}

/// The model one layer and aspect runs, keyed by both.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct StoredMetaModelConfiguration {
    model: ConfigureModel,
}

impl EngineRecord for StoredMetaModelConfiguration {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(FlowStore::model_record_key(
            &self.model.layer,
            &self.model.aspect,
        ))
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

/// A layer's model as Flow kept it before 0.35.19, keyed by its Layer
/// alone and naming no aspect. It is read only by the v1-to-v2 upgrade of
/// the model family.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct ModelBeforeAspect {
    layer: Layer,
    native: String,
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct StoredMetaModelConfigurationV1 {
    model: ModelBeforeAspect,
}

impl EngineRecord for StoredMetaModelConfigurationV1 {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(FlowStore::layer_key(&self.model.layer))
    }
}

impl MemoryDefinition for StoredMetaModelConfigurationV1 {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(1)
    }

    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label("flow-nexus-meta-model-configuration-v1")
    }
}

/// A layer's one model was the Psyche seat's: before 0.35.19 every
/// configured model was a Claude model and only Psyche runs on Claude. Each
/// is carried whole as that layer's Psyche model; Mind and Field have no
/// model until one is declared for them.
impl UpgradeFrom<StoredMetaModelConfigurationV1> for StoredMetaModelConfiguration {
    type Refusal = std::convert::Infallible;

    fn upgrade_from(previous: StoredMetaModelConfigurationV1) -> Decision<Self, Self::Refusal> {
        let ModelBeforeAspect { layer, native } = previous.model;
        Decision::Ready(Self {
            model: ConfigureModel {
                layer,
                aspect: Aspect::Psyche,
                model_name: native,
                effort: "medium".into(),
            },
        })
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
        .with_predecessor::<StoredMetaModelConfigurationV1>()
    }
}

/// One model's refresh threshold, keyed by the model's name.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
struct StoredMetaThresholdConfiguration {
    threshold: ConfigureThreshold,
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
struct ConfigureThresholdV1 {
    model_name: String,
    refresh: i64,
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
struct StoredMetaThresholdConfigurationV1 {
    threshold: ConfigureThresholdV1,
}

impl EngineRecord for StoredMetaThresholdConfigurationV1 {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.threshold.model_name.clone())
    }
}

impl MemoryDefinition for StoredMetaThresholdConfigurationV1 {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(1)
    }
    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label("flow-nexus-meta-model-threshold-configuration-v1")
    }
}

impl MemoryDefinition for StoredMetaThresholdConfiguration {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(2)
    }
    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label(META_THRESHOLD_CONFIGURATION_SCHEMA)
    }
}

impl UpgradeFrom<StoredMetaThresholdConfigurationV1> for StoredMetaThresholdConfiguration {
    type Refusal = std::convert::Infallible;
    fn upgrade_from(previous: StoredMetaThresholdConfigurationV1) -> Decision<Self, Self::Refusal> {
        Decision::Ready(Self {
            threshold: ConfigureThreshold {
                model_name: previous.threshold.model_name,
                refresh_tokens: previous.threshold.refresh,
            },
        })
    }
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
        .with_predecessor::<StoredMetaThresholdConfigurationV1>()
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

/// One metaflow's default leave-outs on one harness, keyed by its Address
/// and that harness.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct StoredMetaLeaveOutConfiguration {
    leave_out: ConfigureLeaveOutV47,
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
        MemoryVersion::new(2)
    }

    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label("flow-nexus-meta-leave-out-configuration-v2")
    }
}

/// A metaflow's default leave-outs as Flow kept them before 0.35.19, keyed
/// by its Address alone and naming no harness. It is read only by the
/// v1-to-v2 upgrade of the leave-out family.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct LeaveOutDefaultBeforeHarness {
    address: Address,
    left_out: Vec<LeaveOutV37>,
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct StoredMetaLeaveOutConfigurationV1 {
    leave_out: LeaveOutDefaultBeforeHarness,
}

impl EngineRecord for StoredMetaLeaveOutConfigurationV1 {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(FlowStore::address_record_key(&self.leave_out.address))
    }
}

/// The model a seat runs is the one configured for its Address's layer and
/// aspect, named exactly; nothing else is launched.
pub(crate) trait NamesConfiguredModel {
    /// The model configured for the Address's layer and aspect, if any.
    fn configured_model(&self, address: &Address) -> Result<Option<String>, StoreError>;
    fn configured_effort(&self, address: &Address) -> Result<Option<String>, StoreError>;
}

impl NamesConfiguredModel for FlowStore {
    fn configured_model(&self, address: &Address) -> Result<Option<String>, StoreError> {
        let key = RecordKey::new(FlowStore::model_record_key(&address.layer, &address.aspect));
        Ok(self
            .engine
            .match_records(QueryPlan::key(self.meta_configuration_tables.models, key))?
            .records()
            .first()
            .map(|stored| stored.model.model_name.clone()))
    }

    fn configured_effort(&self, address: &Address) -> Result<Option<String>, StoreError> {
        let key = RecordKey::new(FlowStore::model_record_key(&address.layer, &address.aspect));
        Ok(self
            .engine
            .match_records(QueryPlan::key(self.meta_configuration_tables.models, key))?
            .records()
            .first()
            .map(|stored| stored.model.effort.clone()))
    }
}

impl MemoryDefinition for StoredMetaLeaveOutConfigurationV1 {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(1)
    }

    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label("flow-nexus-meta-leave-out-configuration-v1")
    }
}

/// Why a v1 leave-out default names no one harness to carry it under.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LeaveOutUpgradeRefusal {
    /// Its leave-outs belong to more than one harness.
    MixedHarnesses,
    /// It names no leave-out, so no harness is derived.
    NoHarness,
}

/// A v1 default is carried whole under the one harness every leave-out it
/// names belongs to; a default naming leave-outs of both harnesses, or
/// none, is refused, and the upgrade with it.
impl UpgradeFrom<StoredMetaLeaveOutConfigurationV1> for StoredMetaLeaveOutConfiguration {
    type Refusal = LeaveOutUpgradeRefusal;

    fn upgrade_from(previous: StoredMetaLeaveOutConfigurationV1) -> Decision<Self, Self::Refusal> {
        let LeaveOutDefaultBeforeHarness { address, left_out } = previous.leave_out;
        let Some(first) = left_out.first() else {
            return Decision::Refused(LeaveOutUpgradeRefusal::NoHarness);
        };
        let harness_kind = first.harness();
        if left_out
            .iter()
            .any(|leave_out| leave_out.harness() != harness_kind)
        {
            return Decision::Refused(LeaveOutUpgradeRefusal::MixedHarnesses);
        }
        Decision::Ready(Self {
            leave_out: ConfigureLeaveOutV47 {
                address,
                harness_kind,
                left_out,
            },
        })
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
        .with_predecessor::<StoredMetaLeaveOutConfigurationV1>()
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
        MemoryVersion::new(7)
    }

    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label("flow-nexus-flow-v7")
    }
}

/// A flow row before role names changed from power to layer.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct FlowRecordV6 {
    flow_id: String,
    flow_type: String,
    origin: Origin,
    thread_id: Option<String>,
    harness_kind: HarnessKind,
    endpoint_selection: EndpointSelection,
    lifecycle: FlowLifecycle,
    generation: u64,
}

impl EngineRecord for FlowRecordV6 {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.flow_id.clone())
    }
}

impl MemoryDefinition for FlowRecordV6 {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(6)
    }
    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label("flow-nexus-flow-v6")
    }
}

impl UpgradeFrom<FlowRecordV6> for FlowRecord {
    type Refusal = std::convert::Infallible;
    fn upgrade_from(previous: FlowRecordV6) -> Decision<Self, Self::Refusal> {
        let flow_type = previous
            .flow_type
            .replace(":High:", ":Primary:")
            .replace(":Medium:", ":Secondary:")
            .replace(":Low:", ":Tertiary:")
            .replace(":UltraLow:", ":Quaternary:");
        Decision::Ready(Self {
            flow_id: previous.flow_id,
            flow_type,
            origin: previous.origin,
            thread_id: previous.thread_id,
            harness_kind: previous.harness_kind,
            endpoint_selection: previous.endpoint_selection,
            lifecycle: previous.lifecycle,
            generation: previous.generation,
        })
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

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
struct CallerV1 {
    flow_id: String,
    aspect: Aspect,
    power_level: PowerLevel,
    model_name: String,
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
struct StoredRoleV1 {
    caller: CallerV1,
}

impl EngineRecord for StoredRoleV1 {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.caller.flow_id.clone())
    }
}
impl MemoryDefinition for StoredRoleV1 {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(1)
    }
    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label("flow-nexus-role-v1")
    }
}
impl UpgradeFrom<StoredRoleV1> for StoredRole {
    type Refusal = std::convert::Infallible;
    fn upgrade_from(previous: StoredRoleV1) -> Decision<Self, Self::Refusal> {
        Decision::Ready(Self {
            caller: Caller {
                flow_id: previous.caller.flow_id,
                aspect: previous.caller.aspect,
                layer: previous.caller.power_level.into(),
                model_name: previous.caller.model_name,
            },
        })
    }
}

impl EngineRecord for StoredRole {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.caller.flow_id.clone())
    }
}

impl MemoryDefinition for StoredRole {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(2)
    }
    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label("flow-nexus-role-v2")
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
        MemoryVersion::new(5)
    }

    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label("flow-nexus-launch-attempt-v5")
    }
}

/// The launch-attempt shape before the removed FlowKind position.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
struct StoredLaunchAttemptV4 {
    attempt: LaunchAttemptV4,
    context: LaunchContext,
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
struct LaunchAttemptV4 {
    launch_request_id: String,
    launch_profile: LaunchProfileV4,
    prompt_sha256: String,
    origin: Origin,
    launch_attempt_phase: LaunchAttemptPhase,
    native_launch_intent_option: Option<NativeLaunchIntent>,
    native_launch_binding_option: Option<NativeLaunchBinding>,
    registration_acknowledgement_option: Option<RegistrationAcknowledgement>,
    prompt_delivery_intent_option: Option<PromptDeliveryIntent>,
    prompt_delivery_result_option: Option<PromptDeliveryResult>,
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
struct LaunchProfileV4 {
    launch_request_id: String,
    system_modules: Vec<String>,
    user_modules: Vec<String>,
    remembering: Remembering,
    address: Address,
    model_name: String,
    effort: String,
    flow_id_option: Option<String>,
    herdr_session_name: String,
    left_out_option: Option<Vec<LeaveOut>>,
    flow_kind: metaflow_declaration::FlowKindV4,
    tool_set: signal_flow::ToolSet,
    instruction_prompt: String,
}

impl EngineRecord for StoredLaunchAttemptV4 {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.attempt.launch_request_id.clone())
    }
}
impl MemoryDefinition for StoredLaunchAttemptV4 {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(4)
    }
    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label("flow-nexus-launch-attempt-v4")
    }
}
impl UpgradeFrom<StoredLaunchAttemptV4> for StoredLaunchAttempt {
    type Refusal = std::convert::Infallible;
    fn upgrade_from(previous: StoredLaunchAttemptV4) -> Decision<Self, Self::Refusal> {
        let attempt = previous.attempt;
        let profile = attempt.launch_profile;
        Decision::Ready(Self {
            attempt: LaunchAttempt {
                launch_request_id: attempt.launch_request_id,
                launch_profile: LaunchProfile {
                    launch_request_id: profile.launch_request_id,
                    system_modules: profile.system_modules,
                    user_modules: profile.user_modules,
                    remembering: profile.remembering,
                    address: profile.address,
                    model_name: profile.model_name,
                    effort: profile.effort,
                    flow_id_option: profile.flow_id_option,
                    herdr_session_name: profile.herdr_session_name,
                    left_out_option: profile.left_out_option,
                    tool_set: profile.tool_set,
                    instruction_prompt: profile.instruction_prompt,
                },
                prompt_sha256: attempt.prompt_sha256,
                origin: attempt.origin,
                launch_attempt_phase: attempt.launch_attempt_phase,
                native_launch_intent_option: attempt.native_launch_intent_option,
                native_launch_binding_option: attempt.native_launch_binding_option,
                registration_acknowledgement_option: attempt.registration_acknowledgement_option,
                prompt_delivery_intent_option: attempt.prompt_delivery_intent_option,
                prompt_delivery_result_option: attempt.prompt_delivery_result_option,
            },
            context: previous.context,
        })
    }
}

/// A remembered flow as signal-flow 30.0.0 and earlier archived it in a
/// launch profile, before a launch named its remembering as `Remembering`.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct RememberedFlow {
    pub(crate) flow_id: String,
    pub(crate) remembering_depth: i64,
}

/// The remembering an archived launch named: the flows it named, one by
/// one, or none.
pub(crate) trait RemembersArchivedFlows {
    fn remembering(self) -> Remembering;
}

impl RemembersArchivedFlows for Vec<RememberedFlow> {
    fn remembering(self) -> Remembering {
        if self.is_empty() {
            return Remembering::Predecessors(0);
        }
        Remembering::Custom(self.into_iter().map(|flow| flow.flow_id).collect())
    }
}

/// The v3 attempt layout, before the launch profile named its remembering
/// as `Remembering`.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
struct StoredLaunchAttemptV3 {
    attempt: LaunchAttemptV3,
    context: LaunchContext,
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
struct LaunchAttemptV3 {
    launch_request_id: String,
    launch_profile: LaunchProfileV3,
    prompt_sha256: String,
    origin: Origin,
    launch_attempt_phase: LaunchAttemptPhase,
    native_launch_intent_option: Option<NativeLaunchIntent>,
    native_launch_binding_option: Option<NativeLaunchBinding>,
    registration_acknowledgement_option: Option<RegistrationAcknowledgement>,
    prompt_delivery_intent_option: Option<PromptDeliveryIntent>,
    prompt_delivery_result_option: Option<PromptDeliveryResult>,
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
struct LaunchProfileV3 {
    launch_request_id: String,
    system_modules: Vec<String>,
    user_modules: Vec<String>,
    address: Address,
    model_name: String,
    effort: String,
    flow_id_option: Option<String>,
    remembered_flow_vector: Vec<RememberedFlow>,
    herdr_session_name: String,
    left_out_option: Option<Vec<LeaveOut>>,
    flow_kind: metaflow_declaration::FlowKindV4,
    tool_set: signal_flow::ToolSet,
    instruction_prompt: String,
}

impl EngineRecord for StoredLaunchAttemptV3 {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.attempt.launch_request_id.clone())
    }
}

impl MemoryDefinition for StoredLaunchAttemptV3 {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(3)
    }

    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label("flow-nexus-launch-attempt-v3")
    }
}

impl UpgradeFrom<StoredLaunchAttemptV3> for StoredLaunchAttempt {
    type Refusal = std::convert::Infallible;

    fn upgrade_from(previous: StoredLaunchAttemptV3) -> Decision<Self, Self::Refusal> {
        let attempt = previous.attempt;
        let profile = attempt.launch_profile;
        Decision::Ready(Self {
            attempt: LaunchAttempt {
                launch_request_id: attempt.launch_request_id,
                launch_profile: LaunchProfile {
                    launch_request_id: profile.launch_request_id,
                    system_modules: profile.system_modules,
                    user_modules: profile.user_modules,
                    remembering: profile.remembered_flow_vector.remembering(),
                    address: profile.address,
                    model_name: profile.model_name,
                    effort: profile.effort,
                    flow_id_option: profile.flow_id_option,
                    herdr_session_name: profile.herdr_session_name,
                    left_out_option: profile.left_out_option,
                    tool_set: profile.tool_set,
                    instruction_prompt: profile.instruction_prompt,
                },
                prompt_sha256: attempt.prompt_sha256,
                origin: attempt.origin,
                launch_attempt_phase: attempt.launch_attempt_phase,
                native_launch_intent_option: attempt.native_launch_intent_option,
                native_launch_binding_option: attempt.native_launch_binding_option,
                registration_acknowledgement_option: attempt.registration_acknowledgement_option,
                prompt_delivery_intent_option: attempt.prompt_delivery_intent_option,
                prompt_delivery_result_option: attempt.prompt_delivery_result_option,
            },
            context: previous.context,
        })
    }
}

/// The v2 attempt layout, before the launch profile named its simplified
/// kind and declared tool set.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
struct StoredLaunchAttemptV2 {
    attempt: LaunchAttemptV2,
    context: LaunchContext,
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
struct LaunchAttemptV2 {
    launch_request_id: String,
    launch_profile: LaunchProfileV2,
    prompt_sha256: String,
    origin: Origin,
    launch_attempt_phase: LaunchAttemptPhase,
    native_launch_intent_option: Option<NativeLaunchIntent>,
    native_launch_binding_option: Option<NativeLaunchBinding>,
    registration_acknowledgement_option: Option<RegistrationAcknowledgement>,
    prompt_delivery_intent_option: Option<PromptDeliveryIntent>,
    prompt_delivery_result_option: Option<PromptDeliveryResult>,
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
struct LaunchProfileV2 {
    launch_request_id: String,
    system_modules: Vec<String>,
    user_modules: Vec<String>,
    address: Address,
    model_name: String,
    effort: String,
    flow_id_option: Option<String>,
    remembered_flow_vector: Vec<RememberedFlow>,
    herdr_session_name: String,
    left_out_option: Option<Vec<LeaveOut>>,
    instruction_prompt: String,
}

impl EngineRecord for StoredLaunchAttemptV2 {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.attempt.launch_request_id.clone())
    }
}

impl MemoryDefinition for StoredLaunchAttemptV2 {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(2)
    }

    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label("flow-nexus-launch-attempt-v2")
    }
}

impl UpgradeFrom<StoredLaunchAttemptV2> for StoredLaunchAttempt {
    type Refusal = std::convert::Infallible;

    fn upgrade_from(previous: StoredLaunchAttemptV2) -> Decision<Self, Self::Refusal> {
        let attempt = previous.attempt;
        let profile = attempt.launch_profile;
        Decision::Ready(Self {
            attempt: LaunchAttempt {
                launch_request_id: attempt.launch_request_id,
                launch_profile: LaunchProfile {
                    launch_request_id: profile.launch_request_id,
                    system_modules: profile.system_modules,
                    user_modules: profile.user_modules,
                    remembering: profile.remembered_flow_vector.remembering(),
                    address: profile.address,
                    model_name: profile.model_name,
                    effort: profile.effort,
                    flow_id_option: profile.flow_id_option,
                    herdr_session_name: profile.herdr_session_name,
                    left_out_option: profile.left_out_option,
                    tool_set: signal_flow::ToolSet::Default,
                    instruction_prompt: profile.instruction_prompt,
                },
                prompt_sha256: attempt.prompt_sha256,
                origin: attempt.origin,
                launch_attempt_phase: attempt.launch_attempt_phase,
                native_launch_intent_option: attempt.native_launch_intent_option,
                native_launch_binding_option: attempt.native_launch_binding_option,
                registration_acknowledgement_option: attempt.registration_acknowledgement_option,
                prompt_delivery_intent_option: attempt.prompt_delivery_intent_option,
                prompt_delivery_result_option: attempt.prompt_delivery_result_option,
            },
            context: previous.context,
        })
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
            left_out: Vec::new(),
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
            layer: profile.power_level.into(),
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
                    remembering: profile.remembered_flow_vector.remembering(),
                    address: profile.address,
                    model_name: profile.model_name,
                    effort: profile.effort,
                    flow_id_option: profile.flow_id_option,
                    herdr_session_name: profile.herdr_session_name,
                    left_out_option: Some(Vec::new()),
                    tool_set: signal_flow::ToolSet::Default,
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
enum PowerLevel {
    High,
    Medium,
    Low,
    UltraLow,
}

impl From<PowerLevel> for Layer {
    fn from(value: PowerLevel) -> Self {
        match value {
            PowerLevel::High => Layer::Primary,
            PowerLevel::Medium => Layer::Secondary,
            PowerLevel::Low => Layer::Tertiary,
            PowerLevel::UltraLow => Layer::Quaternary,
        }
    }
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
                layer: power_level.into(),
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

// How a launch request settled, and a replacement under way: the record
// types the Memory root declares (`ethos/memory.ethos`).
pub use crate::generated::memory::{LaunchOutcome, Replacement};

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
            Self::Archived(reference) => Response::LaunchArchived(reference.clone()),
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
            Self::Archived(reference) => {
                signal_flow::LaunchStatusOutcome::Archived(reference.clone())
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
        MemoryVersion::new(12)
    }

    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label("flow-nexus-launch-outcome-v12")
    }
}

/// The launch outcome archived before harness modules replaced the legacy
/// leave-out enum and its launch-time refusal.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
struct StoredLaunchOutcomeV11 {
    launch_request_id: String,
    launch_outcome: LaunchOutcomeV11,
}

impl EngineRecord for StoredLaunchOutcomeV11 {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.launch_request_id.clone())
    }
}

impl MemoryDefinition for StoredLaunchOutcomeV11 {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(11)
    }
    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label("flow-nexus-launch-outcome-v11")
    }
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
enum StartRejectionV39 {
    CompositionRefused(signal_flow::CompositionRefusal),
    LaunchRequestConflict,
    LaunchPersistenceRefused,
    NativeLaunchRefused(signal_flow::NativeLaunchRefusal),
    BindingRefused(signal_flow::BindingRefusal),
    RegistrationRefused(signal_flow::RegistrationRefusal),
    IntentPersistenceRefused,
    OriginUnavailable,
    AddressAwake,
    ContextRefused(signal_flow::ContextRefusal),
    LeaveOutRefused(LeaveOutRefusalV37),
    HarnessSpawnRefused(signal_flow::HarnessSpawnRefusal),
    ModelRefused(signal_flow::ModelRefusal),
    RequirementUnmet(signal_flow::RequirementUnmet),
    Undeclared,
    TopicRefused(String),
}

impl From<StartRejectionV39> for StartRejection {
    fn from(rejection: StartRejectionV39) -> Self {
        match rejection {
            StartRejectionV39::CompositionRefused(refusal) => Self::CompositionRefused(refusal),
            StartRejectionV39::LaunchRequestConflict => Self::LaunchRequestConflict,
            StartRejectionV39::LaunchPersistenceRefused => Self::LaunchPersistenceRefused,
            StartRejectionV39::NativeLaunchRefused(refusal) => Self::NativeLaunchRefused(refusal),
            StartRejectionV39::BindingRefused(refusal) => Self::BindingRefused(refusal),
            StartRejectionV39::RegistrationRefused(refusal) => Self::RegistrationRefused(refusal),
            StartRejectionV39::IntentPersistenceRefused => Self::IntentPersistenceRefused,
            StartRejectionV39::OriginUnavailable => Self::OriginUnavailable,
            StartRejectionV39::AddressAwake => Self::AddressAwake,
            StartRejectionV39::ContextRefused(refusal) => Self::ContextRefused(refusal),
            StartRejectionV39::LeaveOutRefused(_) => {
                unreachable!("legacy leave-out refusals are archived before conversion")
            }
            StartRejectionV39::HarnessSpawnRefused(refusal) => Self::HarnessSpawnRefused(refusal),
            StartRejectionV39::ModelRefused(refusal) => Self::ModelRefused(refusal),
            StartRejectionV39::RequirementUnmet(address) => Self::RequirementUnmet(address),
            StartRejectionV39::Undeclared => Self::Undeclared,
            StartRejectionV39::TopicRefused(topic) => Self::TopicRefused(topic),
        }
    }
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
enum ReplaceRejectionV39 {
    PredecessorAbsent,
    UnknownPredecessor,
    PredecessorStopped,
    LaunchRefused(StartRejectionV39),
    ReapRefused(StopRejection),
    Locked,
}

impl From<ReplaceRejectionV39> for ReplaceRejection {
    fn from(rejection: ReplaceRejectionV39) -> Self {
        match rejection {
            ReplaceRejectionV39::PredecessorAbsent => Self::PredecessorAbsent,
            ReplaceRejectionV39::UnknownPredecessor => Self::UnknownPredecessor,
            ReplaceRejectionV39::PredecessorStopped => Self::PredecessorStopped,
            ReplaceRejectionV39::LaunchRefused(rejection) => Self::LaunchRefused(rejection.into()),
            ReplaceRejectionV39::ReapRefused(rejection) => Self::ReapRefused(rejection),
            ReplaceRejectionV39::Locked => Self::Locked,
        }
    }
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
enum LaunchOutcomeV11 {
    Started(LaunchedRecord),
    Replaced(ReplacedRecord),
    StartRejected(StartRejectionV39),
    ReplaceRejected(ReplaceRejectionV39),
}

impl From<LaunchOutcomeV11> for LaunchOutcome {
    fn from(outcome: LaunchOutcomeV11) -> Self {
        match outcome {
            LaunchOutcomeV11::Started(launched) => Self::Started(launched),
            LaunchOutcomeV11::Replaced(replaced) => Self::Replaced(replaced),
            LaunchOutcomeV11::StartRejected(rejection) => Self::StartRejected(rejection.into()),
            LaunchOutcomeV11::ReplaceRejected(rejection) => Self::ReplaceRejected(rejection.into()),
        }
    }
}

impl UpgradeFrom<StoredLaunchOutcomeV11> for StoredLaunchOutcome {
    type Refusal = EndedAddressOutcome;
    fn upgrade_from(previous: StoredLaunchOutcomeV11) -> Decision<Self, Self::Refusal> {
        Decision::Ready(Self {
            launch_request_id: previous.launch_request_id,
            launch_outcome: previous.launch_outcome.into(),
        })
    }
}

/// An outcome archived before signal-flow removed the two composition
/// refusals that belonged to Simplified and presentation launches.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
struct StoredLaunchOutcomeV9 {
    launch_request_id: String,
    launch_outcome: LaunchOutcomeV9,
}
impl EngineRecord for StoredLaunchOutcomeV9 {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.launch_request_id.clone())
    }
}
impl MemoryDefinition for StoredLaunchOutcomeV9 {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(9)
    }
    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label("flow-nexus-launch-outcome-v9")
    }
}
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
enum LaunchOutcomeV9 {
    Started(LaunchedRecord),
    Replaced(ReplacedRecord),
    StartRejected(StartRejectionV37),
    ReplaceRejected(ReplaceRejectionV37),
}
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
enum CompositionRefusalV37 {
    InvalidLaunchRequestId,
    InvalidModelName,
    InvalidEffort,
    InvalidToolSet,
    InvalidHerdrSessionName,
    InvalidSystemModules,
    InvalidUserModules,
    InvalidRemembering,
    InexactModel,
    Unconfigured,
    PresentationUnavailable,
    Unrecorded,
}
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
enum StartRejectionV37 {
    CompositionRefused(CompositionRefusalV37),
    LaunchRequestConflict,
    LaunchPersistenceRefused,
    NativeLaunchRefused(signal_flow::NativeLaunchRefusal),
    BindingRefused(signal_flow::BindingRefusal),
    RegistrationRefused(signal_flow::RegistrationRefusal),
    IntentPersistenceRefused,
    OriginUnavailable,
    AddressAwake,
    ContextRefused(signal_flow::ContextRefusal),
    LeaveOutRefused(LeaveOutRefusalV37),
    HarnessSpawnRefused(signal_flow::HarnessSpawnRefusal),
    ModelRefused(signal_flow::ModelRefusal),
    RequirementUnmet(signal_flow::Address),
    Undeclared,
    TopicRefused(String),
}
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
enum ReplaceRejectionV37 {
    PredecessorAbsent,
    UnknownPredecessor,
    PredecessorStopped,
    LaunchRefused(StartRejectionV37),
    ReapRefused(signal_flow::StopRejection),
    Locked,
}
impl From<CompositionRefusalV37> for signal_flow::CompositionRefusal {
    fn from(value: CompositionRefusalV37) -> Self {
        match value {
            CompositionRefusalV37::InvalidLaunchRequestId => Self::InvalidLaunchRequestId,
            CompositionRefusalV37::InvalidModelName => Self::InvalidModelName,
            CompositionRefusalV37::InvalidEffort => Self::InvalidEffort,
            CompositionRefusalV37::InvalidToolSet
            | CompositionRefusalV37::PresentationUnavailable => Self::Unrecorded,
            CompositionRefusalV37::InvalidHerdrSessionName => Self::InvalidHerdrSessionName,
            CompositionRefusalV37::InvalidSystemModules => Self::InvalidSystemModules,
            CompositionRefusalV37::InvalidUserModules => Self::InvalidUserModules,
            CompositionRefusalV37::InvalidRemembering => Self::InvalidRemembering,
            CompositionRefusalV37::InexactModel => Self::InexactModel,
            CompositionRefusalV37::Unconfigured => Self::Unconfigured,
            CompositionRefusalV37::Unrecorded => Self::Unrecorded,
        }
    }
}
impl From<StartRejectionV37> for StartRejection {
    fn from(value: StartRejectionV37) -> Self {
        match value {
            StartRejectionV37::CompositionRefused(x) => Self::CompositionRefused(x.into()),
            StartRejectionV37::LaunchRequestConflict => Self::LaunchRequestConflict,
            StartRejectionV37::LaunchPersistenceRefused => Self::LaunchPersistenceRefused,
            StartRejectionV37::NativeLaunchRefused(x) => Self::NativeLaunchRefused(x),
            StartRejectionV37::BindingRefused(x) => Self::BindingRefused(x),
            StartRejectionV37::RegistrationRefused(x) => Self::RegistrationRefused(x),
            StartRejectionV37::IntentPersistenceRefused => Self::IntentPersistenceRefused,
            StartRejectionV37::OriginUnavailable => Self::OriginUnavailable,
            StartRejectionV37::AddressAwake => Self::AddressAwake,
            StartRejectionV37::ContextRefused(x) => Self::ContextRefused(x),
            StartRejectionV37::LeaveOutRefused(_) => Self::LaunchPersistenceRefused,
            StartRejectionV37::HarnessSpawnRefused(x) => Self::HarnessSpawnRefused(x),
            StartRejectionV37::ModelRefused(x) => Self::ModelRefused(x),
            StartRejectionV37::RequirementUnmet(x) => Self::RequirementUnmet(x),
            StartRejectionV37::Undeclared => Self::Undeclared,
            StartRejectionV37::TopicRefused(x) => Self::TopicRefused(x),
        }
    }
}
impl From<ReplaceRejectionV37> for ReplaceRejection {
    fn from(value: ReplaceRejectionV37) -> Self {
        match value {
            ReplaceRejectionV37::PredecessorAbsent => Self::PredecessorAbsent,
            ReplaceRejectionV37::UnknownPredecessor => Self::UnknownPredecessor,
            ReplaceRejectionV37::PredecessorStopped => Self::PredecessorStopped,
            ReplaceRejectionV37::LaunchRefused(x) => Self::LaunchRefused(x.into()),
            ReplaceRejectionV37::ReapRefused(x) => Self::ReapRefused(x),
            ReplaceRejectionV37::Locked => Self::Locked,
        }
    }
}
impl From<LaunchOutcomeV9> for LaunchOutcome {
    fn from(value: LaunchOutcomeV9) -> Self {
        match value {
            LaunchOutcomeV9::Started(x) => Self::Started(x),
            LaunchOutcomeV9::Replaced(x) => Self::Replaced(x),
            LaunchOutcomeV9::StartRejected(x) => Self::StartRejected(x.into()),
            LaunchOutcomeV9::ReplaceRejected(x) => Self::ReplaceRejected(x.into()),
        }
    }
}
impl UpgradeFrom<StoredLaunchOutcomeV9> for StoredLaunchOutcome {
    type Refusal = EndedAddressOutcome;
    fn upgrade_from(previous: StoredLaunchOutcomeV9) -> Decision<Self, Self::Refusal> {
        Decision::Ready(Self {
            launch_request_id: previous.launch_request_id,
            launch_outcome: previous.launch_outcome.into(),
        })
    }
}

/// The launch outcome archived before pane-close refusals carried the exact
/// identity, bounded-stop, or Herdr-close failure.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
struct StoredLaunchOutcomeV10 {
    launch_request_id: String,
    launch_outcome: LaunchOutcomeV10,
}
impl EngineRecord for StoredLaunchOutcomeV10 {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.launch_request_id.clone())
    }
}
impl MemoryDefinition for StoredLaunchOutcomeV10 {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(10)
    }
    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label("flow-nexus-launch-outcome-v10")
    }
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
enum StopRejectionV38 {
    UnknownFlow,
    AlreadyStopped,
    RouteUnavailable,
    CloseRefused,
    PersistenceRefused,
}

impl From<StopRejectionV38> for StopRejection {
    fn from(rejection: StopRejectionV38) -> Self {
        match rejection {
            StopRejectionV38::UnknownFlow => Self::UnknownFlow,
            StopRejectionV38::AlreadyStopped => Self::AlreadyStopped,
            StopRejectionV38::RouteUnavailable => Self::RouteUnavailable,
            StopRejectionV38::CloseRefused => {
                Self::CloseRefused(signal_flow::PaneCloseFailure::IdentityUnavailable)
            }
            StopRejectionV38::PersistenceRefused => Self::PersistenceRefused,
        }
    }
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
enum ReplaceRejectionV38 {
    PredecessorAbsent,
    UnknownPredecessor,
    PredecessorStopped,
    LaunchRefused(StartRejectionV39),
    ReapRefused(StopRejectionV38),
    Locked,
}

impl From<ReplaceRejectionV38> for ReplaceRejection {
    fn from(rejection: ReplaceRejectionV38) -> Self {
        match rejection {
            ReplaceRejectionV38::PredecessorAbsent => Self::PredecessorAbsent,
            ReplaceRejectionV38::UnknownPredecessor => Self::UnknownPredecessor,
            ReplaceRejectionV38::PredecessorStopped => Self::PredecessorStopped,
            ReplaceRejectionV38::LaunchRefused(rejection) => Self::LaunchRefused(rejection.into()),
            ReplaceRejectionV38::ReapRefused(rejection) => Self::ReapRefused(rejection.into()),
            ReplaceRejectionV38::Locked => Self::Locked,
        }
    }
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
enum LaunchOutcomeV10 {
    Started(LaunchedRecord),
    Replaced(ReplacedRecord),
    StartRejected(StartRejectionV39),
    ReplaceRejected(ReplaceRejectionV38),
}

impl From<LaunchOutcomeV10> for LaunchOutcome {
    fn from(outcome: LaunchOutcomeV10) -> Self {
        match outcome {
            LaunchOutcomeV10::Started(launched) => Self::Started(launched),
            LaunchOutcomeV10::Replaced(replaced) => Self::Replaced(replaced),
            LaunchOutcomeV10::StartRejected(rejection) => Self::StartRejected(rejection.into()),
            LaunchOutcomeV10::ReplaceRejected(rejection) => Self::ReplaceRejected(rejection.into()),
        }
    }
}

impl UpgradeFrom<StoredLaunchOutcomeV10> for StoredLaunchOutcome {
    type Refusal = EndedAddressOutcome;

    fn upgrade_from(previous: StoredLaunchOutcomeV10) -> Decision<Self, Self::Refusal> {
        Decision::Ready(Self {
            launch_request_id: previous.launch_request_id,
            launch_outcome: previous.launch_outcome.into(),
        })
    }
}

/// The launch outcome archived before signal-flow removed the two
/// composition refusals that belonged to Simplified and presentation
/// launches. Those refusals have no current meaning and become Unrecorded.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
struct StoredLaunchOutcomeV8 {
    launch_request_id: String,
    launch_outcome: LaunchOutcomeV8,
}

impl EngineRecord for StoredLaunchOutcomeV8 {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.launch_request_id.clone())
    }
}

impl MemoryDefinition for StoredLaunchOutcomeV8 {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(8)
    }

    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label("flow-nexus-launch-outcome-v8")
    }
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
enum LaunchOutcomeV8 {
    Started(LaunchedRecord),
    Replaced(ReplacedRecord),
    StartRejected(StartRejectionV34),
    ReplaceRejected(ReplaceRejectionV34),
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
enum CompositionRefusalV34 {
    InvalidLaunchRequestId,
    InvalidModelName,
    InvalidEffort,
    InvalidToolSet,
    InvalidHerdrSessionName,
    InvalidSystemModules,
    InvalidUserModules,
    InvalidRemembering,
    InexactModel,
    Unconfigured,
    PresentationUnavailable,
    Unrecorded,
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
enum StartRejectionV34 {
    CompositionRefused(CompositionRefusalV34),
    LaunchRequestConflict,
    LaunchPersistenceRefused,
    NativeLaunchRefused(signal_flow::NativeLaunchRefusal),
    BindingRefused(signal_flow::BindingRefusal),
    RegistrationRefused(signal_flow::RegistrationRefusal),
    IntentPersistenceRefused,
    OriginUnavailable,
    AddressAwake,
    ContextRefused(signal_flow::ContextRefusal),
    LeaveOutRefused(LeaveOutRefusalV37),
    HarnessSpawnRefused(signal_flow::HarnessSpawnRefusal),
    ModelRefused(signal_flow::ModelRefusal),
    NoSecondary,
    Undeclared,
    TopicRefused(String),
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
enum ReplaceRejectionV34 {
    PredecessorAbsent,
    UnknownPredecessor,
    PredecessorStopped,
    LaunchRefused(StartRejectionV34),
    ReapRefused(StopRejectionV38),
    Locked,
}

impl From<CompositionRefusalV34> for signal_flow::CompositionRefusal {
    fn from(refusal: CompositionRefusalV34) -> Self {
        match refusal {
            CompositionRefusalV34::InvalidLaunchRequestId => Self::InvalidLaunchRequestId,
            CompositionRefusalV34::InvalidModelName => Self::InvalidModelName,
            CompositionRefusalV34::InvalidEffort => Self::InvalidEffort,
            CompositionRefusalV34::InvalidToolSet
            | CompositionRefusalV34::PresentationUnavailable => Self::Unrecorded,
            CompositionRefusalV34::InvalidHerdrSessionName => Self::InvalidHerdrSessionName,
            CompositionRefusalV34::InvalidSystemModules => Self::InvalidSystemModules,
            CompositionRefusalV34::InvalidUserModules => Self::InvalidUserModules,
            CompositionRefusalV34::InvalidRemembering => Self::InvalidRemembering,
            CompositionRefusalV34::InexactModel => Self::InexactModel,
            CompositionRefusalV34::Unconfigured => Self::Unconfigured,
            CompositionRefusalV34::Unrecorded => Self::Unrecorded,
        }
    }
}

impl From<StartRejectionV34> for StartRejection {
    fn from(rejection: StartRejectionV34) -> Self {
        match rejection {
            StartRejectionV34::CompositionRefused(refusal) => {
                Self::CompositionRefused(refusal.into())
            }
            StartRejectionV34::LaunchRequestConflict => Self::LaunchRequestConflict,
            StartRejectionV34::LaunchPersistenceRefused => Self::LaunchPersistenceRefused,
            StartRejectionV34::NativeLaunchRefused(refusal) => Self::NativeLaunchRefused(refusal),
            StartRejectionV34::BindingRefused(refusal) => Self::BindingRefused(refusal),
            StartRejectionV34::RegistrationRefused(refusal) => Self::RegistrationRefused(refusal),
            StartRejectionV34::IntentPersistenceRefused => Self::IntentPersistenceRefused,
            StartRejectionV34::OriginUnavailable => Self::OriginUnavailable,
            StartRejectionV34::AddressAwake => Self::AddressAwake,
            StartRejectionV34::ContextRefused(refusal) => Self::ContextRefused(refusal),
            StartRejectionV34::LeaveOutRefused(_) => {
                unreachable!("legacy leave-out refusals are archived before conversion")
            }
            StartRejectionV34::HarnessSpawnRefused(refusal) => Self::HarnessSpawnRefused(refusal),
            StartRejectionV34::ModelRefused(refusal) => Self::ModelRefused(refusal),
            StartRejectionV34::NoSecondary => {
                unreachable!("v8 NoSecondary is resolved by the contextual store upgrade")
            }
            StartRejectionV34::Undeclared => Self::Undeclared,
            StartRejectionV34::TopicRefused(topic) => Self::TopicRefused(topic),
        }
    }
}

impl From<ReplaceRejectionV34> for ReplaceRejection {
    fn from(rejection: ReplaceRejectionV34) -> Self {
        match rejection {
            ReplaceRejectionV34::PredecessorAbsent => Self::PredecessorAbsent,
            ReplaceRejectionV34::UnknownPredecessor => Self::UnknownPredecessor,
            ReplaceRejectionV34::PredecessorStopped => Self::PredecessorStopped,
            ReplaceRejectionV34::LaunchRefused(rejection) => Self::LaunchRefused(rejection.into()),
            ReplaceRejectionV34::ReapRefused(rejection) => Self::ReapRefused(rejection.into()),
            ReplaceRejectionV34::Locked => Self::Locked,
        }
    }
}

impl From<LaunchOutcomeV8> for LaunchOutcome {
    fn from(outcome: LaunchOutcomeV8) -> Self {
        match outcome {
            LaunchOutcomeV8::Started(launched) => Self::Started(launched),
            LaunchOutcomeV8::Replaced(replaced) => Self::Replaced(replaced),
            LaunchOutcomeV8::StartRejected(rejection) => Self::StartRejected(rejection.into()),
            LaunchOutcomeV8::ReplaceRejected(rejection) => Self::ReplaceRejected(rejection.into()),
        }
    }
}

impl LaunchOutcomeV8 {
    fn needs_requirement_address(&self) -> bool {
        matches!(self, Self::StartRejected(StartRejectionV34::NoSecondary))
            || matches!(
                self,
                Self::ReplaceRejected(ReplaceRejectionV34::LaunchRefused(
                    StartRejectionV34::NoSecondary
                ))
            )
    }

    fn with_requirement_address(self, address: Address) -> LaunchOutcome {
        match self {
            Self::StartRejected(StartRejectionV34::NoSecondary) => {
                LaunchOutcome::StartRejected(StartRejection::RequirementUnmet(address))
            }
            Self::ReplaceRejected(ReplaceRejectionV34::LaunchRefused(
                StartRejectionV34::NoSecondary,
            )) => LaunchOutcome::ReplaceRejected(ReplaceRejection::LaunchRefused(
                StartRejection::RequirementUnmet(address),
            )),
            other => other.into(),
        }
    }
}

trait HasLegacyLeaveOutRefusal {
    fn has_legacy_leave_out_refusal(&self) -> bool;
}

macro_rules! legacy_leave_out_refusal {
    ($start:ty, $replace:ty, $outcome:ty) => {
        impl HasLegacyLeaveOutRefusal for $start {
            fn has_legacy_leave_out_refusal(&self) -> bool {
                matches!(self, Self::LeaveOutRefused(_))
            }
        }
        impl HasLegacyLeaveOutRefusal for $replace {
            fn has_legacy_leave_out_refusal(&self) -> bool {
                match self {
                    Self::LaunchRefused(rejection) => rejection.has_legacy_leave_out_refusal(),
                    _ => false,
                }
            }
        }
        impl HasLegacyLeaveOutRefusal for $outcome {
            fn has_legacy_leave_out_refusal(&self) -> bool {
                match self {
                    Self::StartRejected(rejection) => rejection.has_legacy_leave_out_refusal(),
                    Self::ReplaceRejected(rejection) => rejection.has_legacy_leave_out_refusal(),
                    _ => false,
                }
            }
        }
    };
}

macro_rules! legacy_leave_out_outcome {
    ($replace:ty, $outcome:ty) => {
        impl HasLegacyLeaveOutRefusal for $replace {
            fn has_legacy_leave_out_refusal(&self) -> bool {
                match self {
                    Self::LaunchRefused(rejection) => rejection.has_legacy_leave_out_refusal(),
                    _ => false,
                }
            }
        }
        impl HasLegacyLeaveOutRefusal for $outcome {
            fn has_legacy_leave_out_refusal(&self) -> bool {
                match self {
                    Self::StartRejected(rejection) => rejection.has_legacy_leave_out_refusal(),
                    Self::ReplaceRejected(rejection) => rejection.has_legacy_leave_out_refusal(),
                    _ => false,
                }
            }
        }
    };
}

legacy_leave_out_refusal!(StartRejectionV39, ReplaceRejectionV39, LaunchOutcomeV11);
legacy_leave_out_outcome!(ReplaceRejectionV38, LaunchOutcomeV10);
legacy_leave_out_refusal!(StartRejectionV37, ReplaceRejectionV37, LaunchOutcomeV9);
legacy_leave_out_refusal!(StartRejectionV34, ReplaceRejectionV34, LaunchOutcomeV8);
legacy_leave_out_refusal!(StartRejectionV32, ReplaceRejectionV32, LaunchOutcomeV7);
legacy_leave_out_refusal!(StartRejectionV30, ReplaceRejectionV30, LaunchOutcomeV6);
legacy_leave_out_refusal!(StartRejectionV22, ReplaceRejectionV22, LaunchOutcomeV5);
legacy_leave_out_refusal!(StartRejectionV18, ReplaceRejectionV18, LaunchOutcomeV4);
legacy_leave_out_refusal!(StartRejectionV17, ReplaceRejectionV17, LaunchOutcomeV3);
impl HasLegacyLeaveOutRefusal for LaunchOutcomeV2 {
    fn has_legacy_leave_out_refusal(&self) -> bool {
        match self {
            Self::StartRejected(rejection) => rejection.has_legacy_leave_out_refusal(),
            Self::ReplaceRejected(rejection) => rejection.has_legacy_leave_out_refusal(),
            _ => false,
        }
    }
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
enum HistoricalAttemptReason {
    Missing,
    Ambiguous,
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
struct QuarantinedLaunchOutcomeV1 {
    launch_request_id: String,
    archive: Vec<u8>,
    reason: HistoricalAttemptReason,
}

impl EngineRecord for QuarantinedLaunchOutcomeV1 {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.launch_request_id.clone())
    }
}

impl MemoryDefinition for QuarantinedLaunchOutcomeV1 {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(1)
    }
    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label("flow-nexus-quarantined-launch-outcome-v1")
    }
}

/// One historical launch outcome kept byte-for-byte when its meaning cannot
/// be projected into the live launch vocabulary.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
struct QuarantinedLaunchOutcome {
    launch_request_id: String,
    origin_family: String,
    origin_version: i64,
    archive: Vec<u8>,
    reason: signal_flow::LaunchArchiveReason,
}

impl EngineRecord for QuarantinedLaunchOutcome {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.launch_request_id.clone())
    }
}

impl MemoryDefinition for QuarantinedLaunchOutcome {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(2)
    }
    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label("flow-nexus-quarantined-launch-outcome-v2")
    }
}

impl QuarantinedLaunchOutcome {
    fn reference(&self) -> signal_flow::LaunchOutcomeArchive {
        signal_flow::LaunchOutcomeArchive {
            launch_request_id: self.launch_request_id.clone(),
            launch_outcome_family: self.origin_family.clone(),
            launch_outcome_version: self.origin_version,
            launch_archive_reason: self.reason.clone(),
        }
    }
}

impl UpgradeFrom<QuarantinedLaunchOutcomeV1> for QuarantinedLaunchOutcome {
    type Refusal = std::convert::Infallible;

    fn upgrade_from(previous: QuarantinedLaunchOutcomeV1) -> Decision<Self, Self::Refusal> {
        Decision::Ready(Self {
            launch_request_id: previous.launch_request_id,
            origin_family: "flow-nexus-launch-outcome".into(),
            origin_version: 8,
            archive: previous.archive,
            reason: match previous.reason {
                HistoricalAttemptReason::Missing => {
                    signal_flow::LaunchArchiveReason::HistoricalAttemptMissing
                }
                HistoricalAttemptReason::Ambiguous => {
                    signal_flow::LaunchArchiveReason::HistoricalAttemptAmbiguous
                }
            },
        })
    }
}

impl UpgradeFrom<StoredLaunchOutcomeV8> for StoredLaunchOutcome {
    type Refusal = EndedAddressOutcome;

    fn upgrade_from(previous: StoredLaunchOutcomeV8) -> Decision<Self, Self::Refusal> {
        Decision::Ready(Self {
            launch_request_id: previous.launch_request_id,
            launch_outcome: previous.launch_outcome.into(),
        })
    }
}

/// An outcome as Flow 0.40 archived it before a composition refusal kept
/// its reason, its rejections signal-flow 32's before CompositionRefusal.
/// This shape is read by the v7-to-v8 family upgrade.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
struct StoredLaunchOutcomeV7 {
    launch_request_id: String,
    launch_outcome: LaunchOutcomeV7,
}

impl EngineRecord for StoredLaunchOutcomeV7 {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.launch_request_id.clone())
    }
}

impl MemoryDefinition for StoredLaunchOutcomeV7 {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(7)
    }

    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label("flow-nexus-launch-outcome-v7")
    }
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
enum LaunchOutcomeV7 {
    Started(LaunchedRecord),
    Replaced(ReplacedRecord),
    StartRejected(StartRejectionV32),
    ReplaceRejected(ReplaceRejectionV32),
}

/// A start rejection as signal-flow 32.0.0 archived it before
/// CompositionRefused carried its reason.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
enum StartRejectionV32 {
    CompositionRefused,
    LaunchRequestConflict,
    LaunchPersistenceRefused,
    NativeLaunchRefused(signal_flow::NativeLaunchRefusal),
    BindingRefused(signal_flow::BindingRefusal),
    RegistrationRefused(signal_flow::RegistrationRefusal),
    IntentPersistenceRefused,
    OriginUnavailable,
    AddressAwake,
    ContextRefused(signal_flow::ContextRefusal),
    LeaveOutRefused(LeaveOutRefusalV37),
    HarnessSpawnRefused(signal_flow::HarnessSpawnRefusal),
    ModelRefused(signal_flow::ModelRefusal),
    NoSecondary,
    Undeclared,
    TopicRefused(String),
}

/// A replace rejection as signal-flow 32.0.0 archived it, its launch's
/// rejection before CompositionRefused carried its reason.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
enum ReplaceRejectionV32 {
    PredecessorAbsent,
    UnknownPredecessor,
    PredecessorStopped,
    LaunchRefused(StartRejectionV32),
    ReapRefused(StopRejectionV38),
    Locked,
}

fn unrecorded_secondary() -> signal_flow::Address {
    signal_flow::Address {
        aspect: signal_flow::Aspect::Psyche,
        topic: "unrecorded".into(),
        layer: signal_flow::Layer::Secondary,
    }
}

impl From<StartRejectionV32> for StartRejection {
    fn from(rejection: StartRejectionV32) -> Self {
        match rejection {
            StartRejectionV32::CompositionRefused => {
                Self::CompositionRefused(signal_flow::CompositionRefusal::Unrecorded)
            }
            StartRejectionV32::LaunchRequestConflict => Self::LaunchRequestConflict,
            StartRejectionV32::LaunchPersistenceRefused => Self::LaunchPersistenceRefused,
            StartRejectionV32::NativeLaunchRefused(refusal) => Self::NativeLaunchRefused(refusal),
            StartRejectionV32::BindingRefused(refusal) => Self::BindingRefused(refusal),
            StartRejectionV32::RegistrationRefused(refusal) => Self::RegistrationRefused(refusal),
            StartRejectionV32::IntentPersistenceRefused => Self::IntentPersistenceRefused,
            StartRejectionV32::OriginUnavailable => Self::OriginUnavailable,
            StartRejectionV32::AddressAwake => Self::AddressAwake,
            StartRejectionV32::ContextRefused(refusal) => Self::ContextRefused(refusal),
            StartRejectionV32::LeaveOutRefused(_) => {
                unreachable!("legacy leave-out refusals are archived before conversion")
            }
            StartRejectionV32::HarnessSpawnRefused(refusal) => Self::HarnessSpawnRefused(refusal),
            StartRejectionV32::ModelRefused(refusal) => Self::ModelRefused(refusal),
            StartRejectionV32::NoSecondary => Self::RequirementUnmet(unrecorded_secondary()),
            StartRejectionV32::Undeclared => Self::Undeclared,
            StartRejectionV32::TopicRefused(topic) => Self::TopicRefused(topic),
        }
    }
}

impl From<ReplaceRejectionV32> for ReplaceRejection {
    fn from(rejection: ReplaceRejectionV32) -> Self {
        match rejection {
            ReplaceRejectionV32::PredecessorAbsent => Self::PredecessorAbsent,
            ReplaceRejectionV32::UnknownPredecessor => Self::UnknownPredecessor,
            ReplaceRejectionV32::PredecessorStopped => Self::PredecessorStopped,
            ReplaceRejectionV32::LaunchRefused(rejection) => Self::LaunchRefused(rejection.into()),
            ReplaceRejectionV32::ReapRefused(rejection) => Self::ReapRefused(rejection.into()),
            ReplaceRejectionV32::Locked => Self::Locked,
        }
    }
}

impl From<LaunchOutcomeV7> for LaunchOutcome {
    fn from(outcome: LaunchOutcomeV7) -> Self {
        match outcome {
            LaunchOutcomeV7::Started(launched) => Self::Started(launched),
            LaunchOutcomeV7::Replaced(replaced) => Self::Replaced(replaced),
            LaunchOutcomeV7::StartRejected(rejection) => Self::StartRejected(rejection.into()),
            LaunchOutcomeV7::ReplaceRejected(rejection) => Self::ReplaceRejected(rejection.into()),
        }
    }
}

impl UpgradeFrom<StoredLaunchOutcomeV7> for StoredLaunchOutcome {
    type Refusal = EndedAddressOutcome;

    fn upgrade_from(previous: StoredLaunchOutcomeV7) -> Decision<Self, Self::Refusal> {
        Decision::Ready(Self {
            launch_request_id: previous.launch_request_id,
            launch_outcome: previous.launch_outcome.into(),
        })
    }
}

/// An outcome as Flow 0.35.22 to 0.39 archived it, its rejections
/// signal-flow 30's: before a Start could be refused for its topic or a
/// missing record, and a Replace for a held metaflow. This shape is read by
/// the v6-to-v7 family upgrade, and every older shape is carried through it.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
struct StoredLaunchOutcomeV6 {
    launch_request_id: String,
    launch_outcome: LaunchOutcomeV6,
}

impl EngineRecord for StoredLaunchOutcomeV6 {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.launch_request_id.clone())
    }
}

impl MemoryDefinition for StoredLaunchOutcomeV6 {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(6)
    }

    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label("flow-nexus-launch-outcome-v6")
    }
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
enum LaunchOutcomeV6 {
    Started(LaunchedRecord),
    Replaced(ReplacedRecord),
    StartRejected(StartRejectionV30),
    ReplaceRejected(ReplaceRejectionV30),
}

/// A context refusal as signal-flow 30.0.0 and every earlier one archived
/// it, before RecordMissing was placed after ModuleMissing: SystemTooLarge
/// and ModuleTooLarge archived one place earlier.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
enum ContextRefusalV30 {
    ComposerUnavailable,
    ComposerRejected,
    MetaflowMismatch,
    PayloadMismatch,
    ModuleMissing,
    SystemTooLarge,
    ModuleTooLarge,
}

/// A start rejection as signal-flow 30.0.0 archived it, before
/// TopicRefused, its context refusal before RecordMissing.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
enum StartRejectionV30 {
    CompositionRefused,
    LaunchRequestConflict,
    LaunchPersistenceRefused,
    NativeLaunchRefused(signal_flow::NativeLaunchRefusal),
    BindingRefused(signal_flow::BindingRefusal),
    RegistrationRefused(signal_flow::RegistrationRefusal),
    IntentPersistenceRefused,
    OriginUnavailable,
    AddressAwake,
    ContextRefused(ContextRefusalV30),
    LeaveOutRefused(LeaveOutRefusalV37),
    HarnessSpawnRefused(signal_flow::HarnessSpawnRefusal),
    ModelRefused(signal_flow::ModelRefusal),
    NoSecondary,
    Undeclared,
}

/// A replace rejection as signal-flow 30.0.0 archived it, before Locked.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
enum ReplaceRejectionV30 {
    PredecessorAbsent,
    UnknownPredecessor,
    PredecessorStopped,
    LaunchRefused(StartRejectionV30),
    ReapRefused(StopRejectionV38),
}

impl From<ContextRefusalV30> for signal_flow::ContextRefusal {
    fn from(refusal: ContextRefusalV30) -> Self {
        match refusal {
            ContextRefusalV30::ComposerUnavailable => Self::ComposerUnavailable,
            ContextRefusalV30::ComposerRejected => Self::ComposerRejected,
            ContextRefusalV30::MetaflowMismatch => Self::MetaflowMismatch,
            ContextRefusalV30::PayloadMismatch => Self::PayloadMismatch,
            ContextRefusalV30::ModuleMissing => Self::ModuleMissing,
            ContextRefusalV30::SystemTooLarge => Self::SystemTooLarge,
            ContextRefusalV30::ModuleTooLarge => Self::ModuleTooLarge,
        }
    }
}

impl From<StartRejectionV30> for StartRejection {
    fn from(rejection: StartRejectionV30) -> Self {
        match rejection {
            StartRejectionV30::CompositionRefused => {
                Self::CompositionRefused(signal_flow::CompositionRefusal::Unrecorded)
            }
            StartRejectionV30::LaunchRequestConflict => Self::LaunchRequestConflict,
            StartRejectionV30::LaunchPersistenceRefused => Self::LaunchPersistenceRefused,
            StartRejectionV30::NativeLaunchRefused(refusal) => Self::NativeLaunchRefused(refusal),
            StartRejectionV30::BindingRefused(refusal) => Self::BindingRefused(refusal),
            StartRejectionV30::RegistrationRefused(refusal) => Self::RegistrationRefused(refusal),
            StartRejectionV30::IntentPersistenceRefused => Self::IntentPersistenceRefused,
            StartRejectionV30::OriginUnavailable => Self::OriginUnavailable,
            StartRejectionV30::AddressAwake => Self::AddressAwake,
            StartRejectionV30::ContextRefused(refusal) => Self::ContextRefused(refusal.into()),
            StartRejectionV30::LeaveOutRefused(_) => {
                unreachable!("legacy leave-out refusals are archived before conversion")
            }
            StartRejectionV30::HarnessSpawnRefused(refusal) => Self::HarnessSpawnRefused(refusal),
            StartRejectionV30::ModelRefused(refusal) => Self::ModelRefused(refusal),
            StartRejectionV30::NoSecondary => Self::RequirementUnmet(unrecorded_secondary()),
            StartRejectionV30::Undeclared => Self::Undeclared,
        }
    }
}

impl From<ReplaceRejectionV30> for ReplaceRejection {
    fn from(rejection: ReplaceRejectionV30) -> Self {
        match rejection {
            ReplaceRejectionV30::PredecessorAbsent => Self::PredecessorAbsent,
            ReplaceRejectionV30::UnknownPredecessor => Self::UnknownPredecessor,
            ReplaceRejectionV30::PredecessorStopped => Self::PredecessorStopped,
            ReplaceRejectionV30::LaunchRefused(rejection) => Self::LaunchRefused(rejection.into()),
            ReplaceRejectionV30::ReapRefused(rejection) => Self::ReapRefused(rejection.into()),
        }
    }
}

impl From<LaunchOutcomeV6> for LaunchOutcome {
    fn from(outcome: LaunchOutcomeV6) -> Self {
        match outcome {
            LaunchOutcomeV6::Started(launched) => Self::Started(launched),
            LaunchOutcomeV6::Replaced(replaced) => Self::Replaced(replaced),
            LaunchOutcomeV6::StartRejected(rejection) => Self::StartRejected(rejection.into()),
            LaunchOutcomeV6::ReplaceRejected(rejection) => Self::ReplaceRejected(rejection.into()),
        }
    }
}

impl UpgradeFrom<StoredLaunchOutcomeV6> for StoredLaunchOutcome {
    type Refusal = EndedAddressOutcome;

    fn upgrade_from(previous: StoredLaunchOutcomeV6) -> Decision<Self, Self::Refusal> {
        Decision::Ready(Self {
            launch_request_id: previous.launch_request_id,
            launch_outcome: previous.launch_outcome.into(),
        })
    }
}

/// An outcome as Flow 0.35.6 to 0.35.21 archived it, its rejections
/// signal-flow 22's, when a Start could be refused for an ended Address.
/// This shape is read by the v5-to-v7 family upgrade, and every older
/// shape is carried through it.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
struct StoredLaunchOutcomeV5 {
    launch_request_id: String,
    launch_outcome: LaunchOutcomeV5,
}

impl EngineRecord for StoredLaunchOutcomeV5 {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.launch_request_id.clone())
    }
}

impl MemoryDefinition for StoredLaunchOutcomeV5 {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(5)
    }

    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label("flow-nexus-launch-outcome-v5")
    }
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
enum LaunchOutcomeV5 {
    Started(LaunchedRecord),
    Replaced(ReplacedRecord),
    StartRejected(StartRejectionV22),
    ReplaceRejected(ReplaceRejectionV22),
}

/// A start rejection as signal-flow 22.0.0 archived it, AddressEnded
/// between AddressAwake and ContextRefused.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
enum StartRejectionV22 {
    CompositionRefused,
    LaunchRequestConflict,
    LaunchPersistenceRefused,
    NativeLaunchRefused(signal_flow::NativeLaunchRefusal),
    BindingRefused(signal_flow::BindingRefusal),
    RegistrationRefused(signal_flow::RegistrationRefusal),
    IntentPersistenceRefused,
    OriginUnavailable,
    AddressAwake,
    AddressEnded,
    ContextRefused(ContextRefusalV30),
    LeaveOutRefused(LeaveOutRefusalV37),
    HarnessSpawnRefused(signal_flow::HarnessSpawnRefusal),
    ModelRefused(signal_flow::ModelRefusal),
}

/// A replace rejection as signal-flow 22.0.0 archived it.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
enum ReplaceRejectionV22 {
    PredecessorAbsent,
    UnknownPredecessor,
    PredecessorStopped,
    LaunchRefused(StartRejectionV22),
    ReapRefused(StopRejectionV38),
}

/// A kept outcome that refused a Start for an ended Address. No metaflow
/// is ended any more and no current rejection says what it said, so the
/// upgrade refuses it rather than name another reason, and the store is
/// left as it was.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EndedAddressOutcome;

impl TryFrom<StartRejectionV22> for StartRejectionV30 {
    type Error = EndedAddressOutcome;

    fn try_from(rejection: StartRejectionV22) -> Result<Self, Self::Error> {
        Ok(match rejection {
            StartRejectionV22::CompositionRefused => Self::CompositionRefused,
            StartRejectionV22::LaunchRequestConflict => Self::LaunchRequestConflict,
            StartRejectionV22::LaunchPersistenceRefused => Self::LaunchPersistenceRefused,
            StartRejectionV22::NativeLaunchRefused(refusal) => Self::NativeLaunchRefused(refusal),
            StartRejectionV22::BindingRefused(refusal) => Self::BindingRefused(refusal),
            StartRejectionV22::RegistrationRefused(refusal) => Self::RegistrationRefused(refusal),
            StartRejectionV22::IntentPersistenceRefused => Self::IntentPersistenceRefused,
            StartRejectionV22::OriginUnavailable => Self::OriginUnavailable,
            StartRejectionV22::AddressAwake => Self::AddressAwake,
            StartRejectionV22::AddressEnded => return Err(EndedAddressOutcome),
            StartRejectionV22::ContextRefused(refusal) => Self::ContextRefused(refusal),
            StartRejectionV22::LeaveOutRefused(_) => {
                unreachable!("legacy leave-out refusals are archived before conversion")
            }
            StartRejectionV22::HarnessSpawnRefused(refusal) => Self::HarnessSpawnRefused(refusal),
            StartRejectionV22::ModelRefused(refusal) => Self::ModelRefused(refusal),
        })
    }
}

impl TryFrom<ReplaceRejectionV22> for ReplaceRejectionV30 {
    type Error = EndedAddressOutcome;

    fn try_from(rejection: ReplaceRejectionV22) -> Result<Self, Self::Error> {
        Ok(match rejection {
            ReplaceRejectionV22::PredecessorAbsent => Self::PredecessorAbsent,
            ReplaceRejectionV22::UnknownPredecessor => Self::UnknownPredecessor,
            ReplaceRejectionV22::PredecessorStopped => Self::PredecessorStopped,
            ReplaceRejectionV22::LaunchRefused(rejection) => {
                Self::LaunchRefused(rejection.try_into()?)
            }
            ReplaceRejectionV22::ReapRefused(rejection) => Self::ReapRefused(rejection.into()),
        })
    }
}

impl TryFrom<LaunchOutcomeV5> for LaunchOutcomeV6 {
    type Error = EndedAddressOutcome;

    fn try_from(outcome: LaunchOutcomeV5) -> Result<Self, Self::Error> {
        Ok(match outcome {
            LaunchOutcomeV5::Started(launched) => Self::Started(launched),
            LaunchOutcomeV5::Replaced(replaced) => Self::Replaced(replaced),
            LaunchOutcomeV5::StartRejected(rejection) => Self::StartRejected(rejection.try_into()?),
            LaunchOutcomeV5::ReplaceRejected(rejection) => {
                Self::ReplaceRejected(rejection.try_into()?)
            }
        })
    }
}

/// Every older outcome is carried as a v5 one, then as a v6 one, then into
/// the current shape.
trait UpgradesThroughV5: Into<LaunchOutcomeV5> {
    fn stored(
        launch_request_id: String,
        outcome: Self,
    ) -> Decision<StoredLaunchOutcome, EndedAddressOutcome> {
        match LaunchOutcomeV6::try_from(outcome.into()) {
            Ok(launch_outcome) => Decision::Ready(StoredLaunchOutcome {
                launch_request_id,
                launch_outcome: launch_outcome.into(),
            }),
            Err(refusal) => Decision::Refused(refusal),
        }
    }
}

impl UpgradesThroughV5 for LaunchOutcomeV5 {}
impl UpgradesThroughV5 for LaunchOutcomeV4 {}
impl UpgradesThroughV5 for LaunchOutcomeV3 {}
impl UpgradesThroughV5 for LaunchOutcomeV2 {}

impl UpgradeFrom<StoredLaunchOutcomeV5> for StoredLaunchOutcome {
    type Refusal = EndedAddressOutcome;

    fn upgrade_from(previous: StoredLaunchOutcomeV5) -> Decision<Self, Self::Refusal> {
        LaunchOutcomeV5::stored(previous.launch_request_id, previous.launch_outcome)
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
    ContextRefused(ContextRefusalV30),
    LeaveOutRefused(LeaveOutRefusalV37),
    HarnessSpawnRefused(signal_flow::HarnessSpawnRefusal),
}

/// A replace rejection as signal-flow 18.0.0 archived it.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
enum ReplaceRejectionV18 {
    PredecessorAbsent,
    UnknownPredecessor,
    PredecessorStopped,
    LaunchRefused(StartRejectionV18),
    ReapRefused(StopRejectionV38),
}

impl From<StartRejectionV18> for StartRejectionV22 {
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
            StartRejectionV18::LeaveOutRefused(_) => {
                unreachable!("legacy leave-out refusals are archived before conversion")
            }
            StartRejectionV18::HarnessSpawnRefused(refusal) => Self::HarnessSpawnRefused(refusal),
        }
    }
}

impl From<ReplaceRejectionV18> for ReplaceRejectionV22 {
    fn from(rejection: ReplaceRejectionV18) -> Self {
        match rejection {
            ReplaceRejectionV18::PredecessorAbsent => Self::PredecessorAbsent,
            ReplaceRejectionV18::UnknownPredecessor => Self::UnknownPredecessor,
            ReplaceRejectionV18::PredecessorStopped => Self::PredecessorStopped,
            ReplaceRejectionV18::LaunchRefused(rejection) => Self::LaunchRefused(rejection.into()),
            ReplaceRejectionV18::ReapRefused(rejection) => Self::ReapRefused(rejection.into()),
        }
    }
}

impl From<LaunchOutcomeV4> for LaunchOutcomeV5 {
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
    type Refusal = EndedAddressOutcome;

    fn upgrade_from(previous: StoredLaunchOutcomeV4) -> Decision<Self, Self::Refusal> {
        LaunchOutcomeV4::stored(previous.launch_request_id, previous.launch_outcome)
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
    ContextRefused(ContextRefusalV30),
    LeaveOutRefused(LeaveOutRefusalV37),
}

/// A replace rejection as signal-flow 17.0.0 archived it.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
enum ReplaceRejectionV17 {
    PredecessorAbsent,
    UnknownPredecessor,
    PredecessorStopped,
    LaunchRefused(StartRejectionV17),
    ReapRefused(StopRejectionV38),
}

impl From<StartRejectionV17> for StartRejectionV22 {
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
            StartRejectionV17::LeaveOutRefused(_) => {
                unreachable!("legacy leave-out refusals are archived before conversion")
            }
        }
    }
}

impl From<ReplaceRejectionV17> for ReplaceRejectionV22 {
    fn from(rejection: ReplaceRejectionV17) -> Self {
        match rejection {
            ReplaceRejectionV17::PredecessorAbsent => Self::PredecessorAbsent,
            ReplaceRejectionV17::UnknownPredecessor => Self::UnknownPredecessor,
            ReplaceRejectionV17::PredecessorStopped => Self::PredecessorStopped,
            ReplaceRejectionV17::LaunchRefused(rejection) => Self::LaunchRefused(rejection.into()),
            ReplaceRejectionV17::ReapRefused(rejection) => Self::ReapRefused(rejection.into()),
        }
    }
}

impl From<LaunchOutcomeV3> for LaunchOutcomeV5 {
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
    type Refusal = EndedAddressOutcome;

    fn upgrade_from(previous: StoredLaunchOutcomeV3) -> Decision<Self, Self::Refusal> {
        LaunchOutcomeV3::stored(previous.launch_request_id, previous.launch_outcome)
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

impl From<LaunchOutcomeV2> for LaunchOutcomeV5 {
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
    type Refusal = EndedAddressOutcome;

    fn upgrade_from(previous: StoredLaunchOutcomeV2) -> Decision<Self, Self::Refusal> {
        LaunchOutcomeV2::stored(previous.launch_request_id, previous.launch_outcome)
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
    #[error("configuration seed is invalid: {0}")]
    ConfigurationSeed(String),
    #[error("launch outcome family upgrade failed: {0}")]
    LaunchOutcomeUpgrade(String),
    #[error("flow family upgrade failed: {0}")]
    FlowUpgrade(String),
    #[error("launch attempt family upgrade failed: {0}")]
    LaunchAttemptUpgrade(String),
    #[error("metaflow family upgrade failed: {0}")]
    MetaflowUpgrade(String),
    #[error("send call family upgrade failed: {0}")]
    SendCallUpgrade(String),
    #[error("queue delivery family upgrade failed: {0}")]
    QueueDeliveryUpgrade(String),
    #[error("logical delivery family upgrade failed: {0}")]
    LogicalDeliveryUpgrade(String),
    #[error("the model family could not be upgraded: {0}")]
    ModelUpgrade(String),
    #[error("the Nexus configuration family could not be upgraded: {0}")]
    NexusUpgrade(String),
    #[error("flow events upgrade failed: {0}")]
    EventUpgrade(String),
    #[error("successor intent family upgrade failed: {0}")]
    SuccessorUpgrade(String),
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
    #[error("the configured entry does not exist")]
    UnknownConfigurationEntry,
    #[error("store path must be an absolute file path")]
    InvalidStorePath,
    #[error("meta configuration is not configured")]
    NotConfigured,
    #[error("a Codex model names both the stable and the next endpoint")]
    ModelServedTwice(String),
    #[error("no configured harness declares module {0}")]
    UnknownHarnessModule(String),
    #[error("configured harness module {0} has no removable text, tool or switch")]
    IrremovableHarnessModule(String),
    #[error("the topic {0} is not an uncapitalized camelCase identifier")]
    TopicRefused(String),
    #[error("a configured native lifecycle timing is outside the supported duration range")]
    TimingOutOfRange,
    #[error("the leave-out default of {entry} cannot be carried under one harness: {reason:?}")]
    LeaveOutUpgrade {
        entry: String,
        reason: LeaveOutUpgradeRefusal,
    },
    #[error("the leave-out family could not be upgraded: {0}")]
    LeaveOutUpgradeFailed(String),
    #[error("the harness configuration family could not be upgraded: {0}")]
    HarnessUpgrade(String),
    #[error("store {path} is held by another open; its lock cannot be acquired")]
    StoreHeld { path: String },
    #[error("the store upgrade could not be staged: {0}")]
    StagedUpgrade(String),
    #[error("store {path} is not a recognized Flow Memory v2 store: {reason}")]
    UnrecognizedMemory { path: String, reason: String },
}

/// The families whose rows change shape in 0.35, upgraded from their 0.34
/// predecessor when the store opens: a flow's origin and a launched
/// outcome's origin are derived, no longer the clue the caller named; a
/// launch attempt names its context by module identity, or keeps what a
/// 0.34 launch named instead.
trait UpgradesFlowFamilies {
    fn upgraded_metaflow_processes(
        &mut self,
        descriptor: TableDescriptor<StoredMetaflowProcess>,
    ) -> Result<TableReference<StoredMetaflowProcess>, StoreError>;
    fn upgraded_metaflow_locks(
        &mut self,
        descriptor: TableDescriptor<StoredMetaflowLock>,
    ) -> Result<TableReference<StoredMetaflowLock>, StoreError>;
    /// A store before 0.35.7 keeps queues of bare requests; each is carried
    /// with its sender Unknown. A store before 0.35.22 may keep an ended
    /// metaflow; it is carried Asleep.
    fn upgraded_metaflows(
        &mut self,
        descriptor: TableDescriptor<StoredMetaflow>,
    ) -> Result<TableReference<StoredMetaflow>, StoreError>;
    /// A store before 0.35.19 keeps one model per Layer; each is carried as
    /// that layer's Psyche model and re-keyed by its Layer and Aspect.
    fn upgraded_models(
        &mut self,
        descriptor: TableDescriptor<StoredMetaModelConfiguration>,
    ) -> Result<TableReference<StoredMetaModelConfiguration>, StoreError>;
    fn upgraded_thresholds(
        &mut self,
        descriptor: TableDescriptor<StoredMetaThresholdConfiguration>,
    ) -> Result<TableReference<StoredMetaThresholdConfiguration>, StoreError>;
    /// The metaflow an Address hold found is carried the same way.
    fn upgraded_address_holds(
        &mut self,
        descriptor: TableDescriptor<address_hold::AddressHold>,
    ) -> Result<TableReference<address_hold::AddressHold>, StoreError>;
    /// A store before 0.35.22 keeps send calls whose refusal could name an
    /// ended recipient; such a call refuses the upgrade.
    fn upgraded_send_calls(
        &mut self,
        descriptor: TableDescriptor<crate::generated::send_tool::SendCall>,
    ) -> Result<TableReference<crate::generated::send_tool::SendCall>, StoreError>;
    fn upgraded_flows(
        &mut self,
        descriptor: TableDescriptor<FlowRecord>,
    ) -> Result<TableReference<FlowRecord>, StoreError>;
    fn upgraded_roles(
        &mut self,
        descriptor: TableDescriptor<StoredRole>,
    ) -> Result<TableReference<StoredRole>, StoreError>;
    fn upgraded_launch_outcomes(
        &mut self,
        descriptor: TableDescriptor<StoredLaunchOutcome>,
        _attempts: TableReference<StoredLaunchAttempt>,
        quarantine: TableReference<QuarantinedLaunchOutcome>,
    ) -> Result<TableReference<StoredLaunchOutcome>, StoreError>;
    fn upgraded_quarantined_launch_outcomes(
        &mut self,
        descriptor: TableDescriptor<QuarantinedLaunchOutcome>,
    ) -> Result<TableReference<QuarantinedLaunchOutcome>, StoreError>;
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
    /// A store before 0.35.19 keeps each metaflow's default leave-outs
    /// under its Address alone; each is carried under the harness its
    /// leave-outs belong to, and re-keyed by its Address and that harness.
    /// A default whose leave-outs mix harnesses, or name none, refuses the
    /// upgrade by its entry; the open is staged, so the store stays as it
    /// was, every family with it.
    fn upgraded_leave_out_defaults(
        &mut self,
        descriptor: TableDescriptor<StoredMetaLeaveOutConfiguration>,
    ) -> Result<TableReference<StoredMetaLeaveOutConfiguration>, StoreError>;
}

impl UpgradesFlowFamilies for Engine {
    fn upgraded_metaflow_processes(
        &mut self,
        descriptor: TableDescriptor<StoredMetaflowProcess>,
    ) -> Result<TableReference<StoredMetaflowProcess>, StoreError> {
        match self.upgrade_table::<StoredMetaflowProcessV1, StoredMetaflowProcess>(descriptor) {
            UpgradeOutcome::Upgraded(receipt) => Ok(receipt.table()),
            UpgradeOutcome::AlreadyCurrent(table) => Ok(table),
            UpgradeOutcome::Refused(refusal) => match *refusal.reason() {},
            UpgradeOutcome::Failed(failure) => Err(StoreError::MetaflowUpgrade(format!("{failure:?}"))),
        }
    }

    fn upgraded_metaflow_locks(
        &mut self,
        descriptor: TableDescriptor<StoredMetaflowLock>,
    ) -> Result<TableReference<StoredMetaflowLock>, StoreError> {
        match self.upgrade_table::<StoredMetaflowLockV1, StoredMetaflowLock>(descriptor) {
            UpgradeOutcome::Upgraded(receipt) => Ok(receipt.table()),
            UpgradeOutcome::AlreadyCurrent(table) => Ok(table),
            UpgradeOutcome::Refused(refusal) => match *refusal.reason() {},
            UpgradeOutcome::Failed(failure) => Err(StoreError::MetaflowUpgrade(format!("{failure:?}"))),
        }
    }

    fn upgraded_quarantined_launch_outcomes(
        &mut self,
        descriptor: TableDescriptor<QuarantinedLaunchOutcome>,
    ) -> Result<TableReference<QuarantinedLaunchOutcome>, StoreError> {
        match self.upgrade_table::<QuarantinedLaunchOutcomeV1, QuarantinedLaunchOutcome>(descriptor)
        {
            UpgradeOutcome::Upgraded(receipt) => Ok(receipt.table()),
            UpgradeOutcome::AlreadyCurrent(table) => Ok(table),
            UpgradeOutcome::Refused(refusal) => match *refusal.reason() {},
            UpgradeOutcome::Failed(failure) => Err(StoreError::LaunchOutcomeUpgrade(format!(
                "archived launch outcome family could not be upgraded: {failure:?}"
            ))),
        }
    }

    fn upgraded_thresholds(
        &mut self,
        descriptor: TableDescriptor<StoredMetaThresholdConfiguration>,
    ) -> Result<TableReference<StoredMetaThresholdConfiguration>, StoreError> {
        match self
            .upgrade_table::<StoredMetaThresholdConfigurationV1, StoredMetaThresholdConfiguration>(
                descriptor,
            ) {
            UpgradeOutcome::Upgraded(receipt) => Ok(receipt.table()),
            UpgradeOutcome::AlreadyCurrent(table) => Ok(table),
            UpgradeOutcome::Refused(refusal) => match *refusal.reason() {},
            UpgradeOutcome::Failed(failure) => {
                Err(StoreError::ModelUpgrade(format!("{failure:?}")))
            }
        }
    }

    fn upgraded_models(
        &mut self,
        descriptor: TableDescriptor<StoredMetaModelConfiguration>,
    ) -> Result<TableReference<StoredMetaModelConfiguration>, StoreError> {
        let table = match self
            .upgrade_table::<StoredMetaModelConfigurationV1, StoredMetaModelConfiguration>(
                descriptor,
            ) {
            UpgradeOutcome::Upgraded(receipt) => receipt.table(),
            UpgradeOutcome::AlreadyCurrent(table) => table,
            UpgradeOutcome::Refused(refusal) => match *refusal.reason() {},
            UpgradeOutcome::Failed(failure) => {
                return Err(StoreError::ModelUpgrade(format!("{failure:?}")));
            }
        };
        // The upgrade carries each row under the key it was stored by. A
        // row still under its Layer alone moves to its Layer and Aspect in
        // one commit. The open runs on a staged copy, so the store never
        // holds the upgrade without the move.
        let mut commit = self.begin_atomic_commit();
        let mut moved = false;
        for row in self.match_records(QueryPlan::all(table))?.records() {
            let layer_key = RecordKey::new(FlowStore::layer_key(&row.model.layer));
            if self
                .match_records(QueryPlan::key(table, layer_key.clone()))?
                .records()
                .is_empty()
            {
                continue;
            }
            commit = commit.retract(table, layer_key).assert(table, row.clone());
            moved = true;
        }
        if moved {
            self.commit_atomic(commit)?;
        }
        Ok(table)
    }

    fn upgraded_leave_out_defaults(
        &mut self,
        descriptor: TableDescriptor<StoredMetaLeaveOutConfiguration>,
    ) -> Result<TableReference<StoredMetaLeaveOutConfiguration>, StoreError> {
        let table = match self
            .upgrade_table::<StoredMetaLeaveOutConfigurationV1, StoredMetaLeaveOutConfiguration>(
                descriptor,
            ) {
            UpgradeOutcome::Upgraded(receipt) => receipt.table(),
            UpgradeOutcome::AlreadyCurrent(table) => table,
            UpgradeOutcome::Refused(refusal) => {
                return Err(StoreError::LeaveOutUpgrade {
                    entry: refusal.key().to_owned_string(),
                    reason: refusal.reason().clone(),
                });
            }
            UpgradeOutcome::Failed(failure) => {
                return Err(StoreError::LeaveOutUpgradeFailed(format!("{failure:?}")));
            }
        };
        // The upgrade carries each row under the key it was stored by. A
        // row still under its Address alone moves to its Address and
        // harness in one commit. The open runs on a staged copy, so the
        // store never holds the upgrade without the move.
        let mut commit = self.begin_atomic_commit();
        let mut moved = false;
        for row in self.match_records(QueryPlan::all(table))?.records() {
            let address_key = RecordKey::new(FlowStore::address_record_key(&row.leave_out.address));
            if self
                .match_records(QueryPlan::key(table, address_key.clone()))?
                .records()
                .is_empty()
            {
                continue;
            }
            commit = commit
                .retract(table, address_key)
                .assert(table, row.clone());
            moved = true;
        }
        if moved {
            self.commit_atomic(commit)?;
        }
        Ok(table)
    }

    fn upgraded_metaflows(
        &mut self,
        descriptor: TableDescriptor<StoredMetaflow>,
    ) -> Result<TableReference<StoredMetaflow>, StoreError> {
        // v4 predates stable queue identities; older families are tried only
        // after each newer archived shape declines admission.
        let from_v3 = descriptor.clone();
        let from_v2 = descriptor.clone();
        let from_v1 = descriptor.clone();
        match self.upgrade_table::<StoredMetaflowV4, StoredMetaflow>(descriptor) {
            UpgradeOutcome::Upgraded(receipt) => return Ok(receipt.table()),
            UpgradeOutcome::AlreadyCurrent(table) => return Ok(table),
            UpgradeOutcome::Refused(refusal) => match *refusal.reason() {},
            UpgradeOutcome::Failed(failure)
                if failure.phase() == sema_engine::StoragePhase::Admission => {}
            UpgradeOutcome::Failed(failure) => {
                return Err(StoreError::MetaflowUpgrade(format!("{failure:?}")));
            }
        }
        // The newest edge is tried first and an admission refusal writes
        // nothing; legacy rows are converted only by their matching mirror.
        match self.upgrade_table::<StoredMetaflowV3, StoredMetaflow>(from_v3) {
            UpgradeOutcome::Upgraded(receipt) => return Ok(receipt.table()),
            UpgradeOutcome::AlreadyCurrent(table) => return Ok(table),
            UpgradeOutcome::Refused(refusal) => match *refusal.reason() {},
            UpgradeOutcome::Failed(failure)
                if failure.phase() == sema_engine::StoragePhase::Admission => {}
            UpgradeOutcome::Failed(failure) => {
                return Err(StoreError::MetaflowUpgrade(format!("{failure:?}")));
            }
        }
        match self.upgrade_table::<StoredMetaflowV2, StoredMetaflow>(from_v2) {
            UpgradeOutcome::Upgraded(receipt) => Ok(receipt.table()),
            UpgradeOutcome::AlreadyCurrent(table) => Ok(table),
            UpgradeOutcome::Refused(refusal) => match *refusal.reason() {},
            UpgradeOutcome::Failed(failure)
                if failure.phase() == sema_engine::StoragePhase::Admission =>
            {
                match self.upgrade_table::<StoredMetaflowV1, StoredMetaflow>(from_v1) {
                    UpgradeOutcome::Upgraded(receipt) => Ok(receipt.table()),
                    UpgradeOutcome::AlreadyCurrent(table) => Ok(table),
                    UpgradeOutcome::Refused(refusal) => match *refusal.reason() {},
                    UpgradeOutcome::Failed(failure) => {
                        Err(StoreError::MetaflowUpgrade(format!("{failure:?}")))
                    }
                }
            }
            UpgradeOutcome::Failed(failure) => {
                Err(StoreError::MetaflowUpgrade(format!("{failure:?}")))
            }
        }
    }

    fn upgraded_address_holds(
        &mut self,
        descriptor: TableDescriptor<address_hold::AddressHold>,
    ) -> Result<TableReference<address_hold::AddressHold>, StoreError> {
        let from_v2 = descriptor.clone();
        let from_v1 = descriptor.clone();
        match self
            .upgrade_table::<address_hold::AddressHoldV3, address_hold::AddressHold>(descriptor)
        {
            UpgradeOutcome::Upgraded(receipt) => return Ok(receipt.table()),
            UpgradeOutcome::AlreadyCurrent(table) => return Ok(table),
            UpgradeOutcome::Refused(refusal) => match *refusal.reason() {},
            UpgradeOutcome::Failed(failure)
                if failure.phase() == sema_engine::StoragePhase::Admission => {}
            UpgradeOutcome::Failed(failure) => {
                return Err(StoreError::MetaflowUpgrade(format!("{failure:?}")));
            }
        }
        match self.upgrade_table::<address_hold::AddressHoldV2, address_hold::AddressHold>(from_v2)
        {
            UpgradeOutcome::Upgraded(receipt) => Ok(receipt.table()),
            UpgradeOutcome::AlreadyCurrent(table) => Ok(table),
            UpgradeOutcome::Refused(refusal) => match *refusal.reason() {},
            UpgradeOutcome::Failed(failure)
                if failure.phase() == sema_engine::StoragePhase::Admission =>
            {
                match self.upgrade_table::<address_hold::AddressHoldV1, address_hold::AddressHold>(
                    from_v1,
                ) {
                    UpgradeOutcome::Upgraded(receipt) => Ok(receipt.table()),
                    UpgradeOutcome::AlreadyCurrent(table) => Ok(table),
                    UpgradeOutcome::Refused(refusal) => match *refusal.reason() {},
                    UpgradeOutcome::Failed(failure) => {
                        Err(StoreError::MetaflowUpgrade(format!("{failure:?}")))
                    }
                }
            }
            UpgradeOutcome::Failed(failure) => {
                Err(StoreError::MetaflowUpgrade(format!("{failure:?}")))
            }
        }
    }

    fn upgraded_send_calls(
        &mut self,
        descriptor: TableDescriptor<crate::generated::send_tool::SendCall>,
    ) -> Result<TableReference<crate::generated::send_tool::SendCall>, StoreError> {
        // Try the newest predecessor first; one refused at admission names an
        // older family, tried next.
        match self.upgrade_table::<send_call::SendCallV6, crate::generated::send_tool::SendCall>(
            descriptor.clone(),
        ) {
            UpgradeOutcome::Upgraded(receipt) => return Ok(receipt.table()),
            UpgradeOutcome::AlreadyCurrent(table) => return Ok(table),
            UpgradeOutcome::Refused(refusal) => match *refusal.reason() {},
            UpgradeOutcome::Failed(failure)
                if failure.phase() == sema_engine::StoragePhase::Admission => {}
            UpgradeOutcome::Failed(failure) => {
                return Err(StoreError::SendCallUpgrade(format!("{failure:?}")));
            }
        }
        match self.upgrade_table::<send_call::SendCallV5, crate::generated::send_tool::SendCall>(
            descriptor.clone(),
        ) {
            UpgradeOutcome::Upgraded(receipt) => return Ok(receipt.table()),
            UpgradeOutcome::AlreadyCurrent(table) => return Ok(table),
            UpgradeOutcome::Refused(refusal) => match *refusal.reason() {},
            UpgradeOutcome::Failed(failure)
                if failure.phase() == sema_engine::StoragePhase::Admission => {}
            UpgradeOutcome::Failed(failure) => {
                return Err(StoreError::SendCallUpgrade(format!("{failure:?}")));
            }
        }
        match self.upgrade_table::<send_call::SendCallV4, crate::generated::send_tool::SendCall>(
            descriptor.clone(),
        ) {
            UpgradeOutcome::Upgraded(receipt) => return Ok(receipt.table()),
            UpgradeOutcome::AlreadyCurrent(table) => return Ok(table),
            UpgradeOutcome::Refused(refusal) => match *refusal.reason() {},
            UpgradeOutcome::Failed(failure)
                if failure.phase() == sema_engine::StoragePhase::Admission => {}
            UpgradeOutcome::Failed(failure) => {
                return Err(StoreError::SendCallUpgrade(format!("{failure:?}")));
            }
        }
        match self.upgrade_table::<send_call::SendCallV3, crate::generated::send_tool::SendCall>(
            descriptor.clone(),
        ) {
            UpgradeOutcome::Upgraded(receipt) => Ok(receipt.table()),
            UpgradeOutcome::AlreadyCurrent(table) => Ok(table),
            UpgradeOutcome::Refused(refusal) => match *refusal.reason() {},
            UpgradeOutcome::Failed(failure)
                if failure.phase() == sema_engine::StoragePhase::Admission =>
            {
                match self.upgrade_table::<send_call::SendCallV2, crate::generated::send_tool::SendCall>(descriptor.clone()) {
            UpgradeOutcome::Upgraded(receipt) => Ok(receipt.table()),
            UpgradeOutcome::AlreadyCurrent(table) => Ok(table),
            UpgradeOutcome::Refused(refusal) => Err(StoreError::SendCallUpgrade(format!(
                "call {:?} was refused for an ended recipient, which no longer exists",
                refusal.key()
            ))),
            UpgradeOutcome::Failed(failure)
                if failure.phase() == sema_engine::StoragePhase::Admission =>
            {
                match self
                    .upgrade_table::<send_call::SendCallV1, crate::generated::send_tool::SendCall>(
                        descriptor,
                    ) {
                    UpgradeOutcome::Upgraded(receipt) => Ok(receipt.table()),
                    UpgradeOutcome::AlreadyCurrent(table) => Ok(table),
                    UpgradeOutcome::Refused(refusal) => Err(StoreError::SendCallUpgrade(format!(
                        "call {:?} was refused for an ended recipient, which no longer exists",
                        refusal.key()
                    ))),
                    UpgradeOutcome::Failed(failure) => {
                        Err(StoreError::SendCallUpgrade(format!("{failure:?}")))
                    }
                }
            }
                    UpgradeOutcome::Failed(failure) => {
                        Err(StoreError::SendCallUpgrade(format!("{failure:?}")))
                    }
                }
            }
            UpgradeOutcome::Failed(failure) => Err(StoreError::SendCallUpgrade(format!("{failure:?}"))),
        }
    }

    fn upgraded_flows(
        &mut self,
        descriptor: TableDescriptor<FlowRecord>,
    ) -> Result<TableReference<FlowRecord>, StoreError> {
        match self.upgrade_table::<FlowRecordV6, FlowRecord>(descriptor.clone()) {
            UpgradeOutcome::Upgraded(receipt) => Ok(receipt.table()),
            UpgradeOutcome::AlreadyCurrent(table) => Ok(table),
            UpgradeOutcome::Refused(refusal) => match *refusal.reason() {},
            UpgradeOutcome::Failed(failure)
                if failure.phase() == sema_engine::StoragePhase::Admission =>
            {
                match self.upgrade_table::<FlowRecordV5, FlowRecord>(descriptor) {
                    UpgradeOutcome::Upgraded(receipt) => Ok(receipt.table()),
                    UpgradeOutcome::AlreadyCurrent(table) => Ok(table),
                    UpgradeOutcome::Refused(refusal) => match *refusal.reason() {},
                    UpgradeOutcome::Failed(failure) => {
                        Err(StoreError::FlowUpgrade(format!("{failure:?}")))
                    }
                }
            }
            UpgradeOutcome::Failed(failure) => Err(StoreError::FlowUpgrade(format!("{failure:?}"))),
        }
    }

    fn upgraded_roles(
        &mut self,
        descriptor: TableDescriptor<StoredRole>,
    ) -> Result<TableReference<StoredRole>, StoreError> {
        match self.upgrade_table::<StoredRoleV1, StoredRole>(descriptor) {
            UpgradeOutcome::Upgraded(receipt) => Ok(receipt.table()),
            UpgradeOutcome::AlreadyCurrent(table) => Ok(table),
            UpgradeOutcome::Refused(refusal) => match *refusal.reason() {},
            UpgradeOutcome::Failed(failure) => Err(StoreError::FlowUpgrade(format!("{failure:?}"))),
        }
    }

    fn upgraded_launch_outcomes(
        &mut self,
        descriptor: TableDescriptor<StoredLaunchOutcome>,
        attempts: TableReference<StoredLaunchAttempt>,
        quarantine: TableReference<QuarantinedLaunchOutcome>,
    ) -> Result<TableReference<StoredLaunchOutcome>, StoreError> {
        // A store before harness modules replaced leave-outs holds v11 rows.
        // A store before pane-close failures were typed holds v10 rows. A
        // store before Simplified and its presentation path were removed holds
        // v9 rows. A store before required metaflows holds v8 rows; a 0.40
        // store before CompositionRefusal holds v7 rows, a 0.35.22
        // to 0.39 store v6 rows, a 0.35.6 to 0.35.21 store v5
        // rows, a 0.35.3 to 0.35.5 store v4 rows, a 0.35.2 store v3 rows and
        // a 0.34 store v2 rows; the
        // newest edge is tried first, and a store it does not admit is tried
        // on the next. An admission refusal writes nothing. An outcome that
        // refused a Start for an ended Address refuses the upgrade.
        let failed = |failure| StoreError::LaunchOutcomeUpgrade(format!("{failure:?}"));
        let refused = |key: &RecordKey| {
            StoreError::LaunchOutcomeUpgrade(format!(
                "launch {key:?} was refused for an ended Address, which no longer exists"
            ))
        };
        let admission = |failure: &sema_engine::StorageFailure| {
            failure.phase() == sema_engine::StoragePhase::Admission
        };
        let from_v10 = descriptor.clone();
        let from_v8 = descriptor.clone();
        let from_v7 = descriptor.clone();
        let from_v6 = descriptor.clone();
        let from_v5 = descriptor.clone();
        let from_v4 = descriptor.clone();
        let from_v3 = descriptor.clone();
        let from_v2 = descriptor.clone();
        self.archive_legacy_leave_outs(quarantine)?;
        match self.upgrade_table::<StoredLaunchOutcomeV11, StoredLaunchOutcome>(descriptor.clone())
        {
            UpgradeOutcome::Upgraded(receipt) => return Ok(receipt.table()),
            UpgradeOutcome::AlreadyCurrent(table) => return Ok(table),
            UpgradeOutcome::Refused(refusal) => return Err(refused(refusal.key())),
            UpgradeOutcome::Failed(failure) if admission(&failure) => {}
            UpgradeOutcome::Failed(failure) => return Err(failed(failure)),
        }
        match self.upgrade_table::<StoredLaunchOutcomeV10, StoredLaunchOutcome>(from_v10) {
            UpgradeOutcome::Upgraded(receipt) => return Ok(receipt.table()),
            UpgradeOutcome::AlreadyCurrent(table) => return Ok(table),
            UpgradeOutcome::Refused(refusal) => return Err(refused(refusal.key())),
            UpgradeOutcome::Failed(failure) if admission(&failure) => {}
            UpgradeOutcome::Failed(failure) => return Err(failed(failure)),
        }
        match self.upgrade_table::<StoredLaunchOutcomeV9, StoredLaunchOutcome>(descriptor.clone()) {
            UpgradeOutcome::Upgraded(receipt) => return Ok(receipt.table()),
            UpgradeOutcome::AlreadyCurrent(table) => return Ok(table),
            UpgradeOutcome::Refused(refusal) => return Err(refused(refusal.key())),
            UpgradeOutcome::Failed(failure) if admission(&failure) => {}
            UpgradeOutcome::Failed(failure) => return Err(failed(failure)),
        }
        let v8 = TableDescriptor::<StoredLaunchOutcomeV8>::new(
            FLOW_LAUNCH_OUTCOME_TABLE_NAME,
            FamilyName::new("flow-nexus-launch-outcome"),
            StoredLaunchOutcomeV8::schema_hash(),
        )
        .family_identity();
        if self
            .catalog()
            .family_identity(&FLOW_LAUNCH_OUTCOME_TABLE_NAME)
            == Some(&v8)
        {
            self.settle_launch_outcomes(attempts, quarantine)?;
        }
        match self.upgrade_table_with_context::<
            StoredLaunchOutcomeV8,
            StoredLaunchOutcome,
            StoredLaunchAttempt,
            HistoricalAttemptReason,
            _,
        >(from_v8, attempts, |previous, attempts| {
            let needs_address = previous.launch_outcome.needs_requirement_address();
            let matches = attempts
                .iter()
                .filter(|(key, attempt)| {
                    key == &previous.launch_request_id
                        && attempt.attempt.launch_request_id == previous.launch_request_id
                })
                .collect::<Vec<_>>();
            let address = match matches.as_slice() {
                [(_, attempt)] => Some(attempt.attempt.launch_profile.address.clone()),
                [] if needs_address => return Decision::Refused(HistoricalAttemptReason::Missing),
                _ if needs_address => {
                    return Decision::Refused(HistoricalAttemptReason::Ambiguous);
                }
                _ => None,
            };
            Decision::Ready(StoredLaunchOutcome {
                launch_request_id: previous.launch_request_id,
                launch_outcome: match address {
                    Some(address) => previous.launch_outcome.with_requirement_address(address),
                    None => previous.launch_outcome.into(),
                },
            })
        }) {
            UpgradeOutcome::Upgraded(receipt) => return Ok(receipt.table()),
            UpgradeOutcome::AlreadyCurrent(table) => return Ok(table),
            UpgradeOutcome::Refused(refusal) => {
                return Err(StoreError::LaunchOutcomeUpgrade(format!(
                    "launch {:?} has {:?} historical attempt",
                    refusal.key(),
                    refusal.reason()
                )));
            }
            UpgradeOutcome::Failed(failure) if admission(&failure) => {}
            UpgradeOutcome::Failed(failure) => return Err(failed(failure)),
        }
        match self.upgrade_table::<StoredLaunchOutcomeV7, StoredLaunchOutcome>(from_v7) {
            UpgradeOutcome::Upgraded(receipt) => return Ok(receipt.table()),
            UpgradeOutcome::AlreadyCurrent(table) => return Ok(table),
            UpgradeOutcome::Refused(refusal) => return Err(refused(refusal.key())),
            UpgradeOutcome::Failed(failure) if admission(&failure) => {}
            UpgradeOutcome::Failed(failure) => return Err(failed(failure)),
        }
        match self.upgrade_table::<StoredLaunchOutcomeV6, StoredLaunchOutcome>(from_v6) {
            UpgradeOutcome::Upgraded(receipt) => return Ok(receipt.table()),
            UpgradeOutcome::AlreadyCurrent(table) => return Ok(table),
            UpgradeOutcome::Refused(refusal) => return Err(refused(refusal.key())),
            UpgradeOutcome::Failed(failure) if admission(&failure) => {}
            UpgradeOutcome::Failed(failure) => return Err(failed(failure)),
        }
        match self.upgrade_table::<StoredLaunchOutcomeV5, StoredLaunchOutcome>(from_v5) {
            UpgradeOutcome::Upgraded(receipt) => return Ok(receipt.table()),
            UpgradeOutcome::AlreadyCurrent(table) => return Ok(table),
            UpgradeOutcome::Refused(refusal) => return Err(refused(refusal.key())),
            UpgradeOutcome::Failed(failure) if admission(&failure) => {}
            UpgradeOutcome::Failed(failure) => return Err(failed(failure)),
        }
        match self.upgrade_table::<StoredLaunchOutcomeV4, StoredLaunchOutcome>(from_v4) {
            UpgradeOutcome::Upgraded(receipt) => return Ok(receipt.table()),
            UpgradeOutcome::AlreadyCurrent(table) => return Ok(table),
            UpgradeOutcome::Refused(refusal) => return Err(refused(refusal.key())),
            UpgradeOutcome::Failed(failure) if admission(&failure) => {}
            UpgradeOutcome::Failed(failure) => return Err(failed(failure)),
        }
        match self.upgrade_table::<StoredLaunchOutcomeV3, StoredLaunchOutcome>(from_v3) {
            UpgradeOutcome::Upgraded(receipt) => return Ok(receipt.table()),
            UpgradeOutcome::AlreadyCurrent(table) => return Ok(table),
            UpgradeOutcome::Refused(refusal) => return Err(refused(refusal.key())),
            UpgradeOutcome::Failed(failure) if admission(&failure) => {}
            UpgradeOutcome::Failed(failure) => return Err(failed(failure)),
        }
        match self.upgrade_table::<StoredLaunchOutcomeV2, StoredLaunchOutcome>(from_v2) {
            UpgradeOutcome::Upgraded(receipt) => Ok(receipt.table()),
            UpgradeOutcome::AlreadyCurrent(table) => Ok(table),
            UpgradeOutcome::Refused(refusal) => Err(refused(refusal.key())),
            UpgradeOutcome::Failed(failure) => Err(failed(failure)),
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
        let second_predecessor = TableDescriptor::<StoredLaunchAttemptV2>::new(
            FLOW_LAUNCH_ATTEMPT_TABLE_NAME,
            FamilyName::new("flow-nexus-launch-attempt"),
            StoredLaunchAttemptV2::schema_hash(),
        )
        .family_identity();
        let third_predecessor = TableDescriptor::<StoredLaunchAttemptV3>::new(
            FLOW_LAUNCH_ATTEMPT_TABLE_NAME,
            FamilyName::new("flow-nexus-launch-attempt"),
            StoredLaunchAttemptV3::schema_hash(),
        )
        .family_identity();
        let current_predecessor = TableDescriptor::<StoredLaunchAttemptV4>::new(
            FLOW_LAUNCH_ATTEMPT_TABLE_NAME,
            FamilyName::new("flow-nexus-launch-attempt"),
            StoredLaunchAttemptV4::schema_hash(),
        )
        .family_identity();
        let family = self
            .catalog()
            .family_identity(&FLOW_LAUNCH_ATTEMPT_TABLE_NAME)
            .cloned();
        let settlements = if family.as_ref() == Some(&predecessor) {
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
        let upgrade = if family.as_ref() == Some(&predecessor) {
            self.upgrade_table::<StoredLaunchAttemptV1, StoredLaunchAttempt>(descriptor)
        } else if family.as_ref() == Some(&second_predecessor) {
            self.upgrade_table::<StoredLaunchAttemptV2, StoredLaunchAttempt>(descriptor)
        } else if family.as_ref() == Some(&third_predecessor) {
            self.upgrade_table::<StoredLaunchAttemptV3, StoredLaunchAttempt>(descriptor)
        } else {
            debug_assert!(
                family.is_none()
                    || family.as_ref() == Some(&current_predecessor)
                    || family.as_ref() == Some(&descriptor.family_identity()),
                "launch-attempt family has an undeclared identity"
            );
            self.upgrade_table::<StoredLaunchAttemptV4, StoredLaunchAttempt>(descriptor)
        };
        let table = match upgrade {
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
    harnesses: TableReference<harness::StoredHarness>,
    models: TableReference<StoredMetaModelConfiguration>,
    thresholds: TableReference<StoredMetaThresholdConfiguration>,
    pub(crate) send_servers: TableReference<send_server::StoredMetaSendServerConfiguration>,
    pub(crate) metaflows: TableReference<metaflow_declaration::StoredMetaMetaflowConfiguration>,
    investigation: TableReference<StoredMetaInvestigationConfiguration>,
    timings: TableReference<timings_limits::StoredTimings>,
    limits: TableReference<timings_limits::StoredLimits>,
    texts: TableReference<texts::StoredTexts>,
    full_state: TableReference<full_configuration::StoredFullConfigurationState>,
    full_receipts: TableReference<full_configuration::StoredFullConfigurationReceipt>,
}

trait RegistersMetaConfigurationTables: Sized {
    fn register(engine: &mut Engine) -> Result<Self, StoreError>;
}

impl RegistersMetaConfigurationTables for MetaConfigurationTables {
    fn register(engine: &mut Engine) -> Result<Self, StoreError> {
        let thresholds =
            engine.upgraded_thresholds(StoredMetaThresholdConfiguration::descriptor())?;
        let tables = Self {
            nexus: engine
                .upgraded_nexus_configuration(StoredMetaNexusConfiguration::descriptor())?,
            harnesses: engine.upgraded_harnesses(harness::StoredHarness::descriptor())?,
            models: engine.upgraded_models(StoredMetaModelConfiguration::descriptor())?,
            thresholds,
            send_servers: engine.register_table(
                <send_server::StoredMetaSendServerConfiguration as send_server::DescribesSendServerTable>::descriptor(),
            )?,
            investigation: engine
                .register_table(StoredMetaInvestigationConfiguration::descriptor())?,
            timings: engine.upgraded_timings(timings_limits::timings_descriptor())?,
            limits: engine.register_limits()?,
            texts: engine.register_texts()?,
            full_state: engine.register_table(full_configuration::state_descriptor())?,
            full_receipts: engine.register_table(full_configuration::receipt_descriptor())?,
            metaflows: engine.upgraded_metaflow_declarations(
                <metaflow_declaration::StoredMetaMetaflowConfiguration as metaflow_declaration::DescribesMetaflowDeclarationTable>::descriptor(),
            )?,
        };
        if engine.match_records(QueryPlan::all(tables.full_state))?.records().is_empty() {
            engine.assert(Assertion::new(tables.full_state, full_configuration::StoredFullConfigurationState { revision: 0 }))?;
        }
        Ok(tables)
    }
}

pub struct FlowStore {
    engine: Engine,
    configuration_guard: Mutex<()>,
    native_owner_environment_override: Option<NativeOwnerEnvironment>,
    flows: TableReference<FlowRecord>,
    native_owner_proofs: TableReference<NativeOwnerProofRecord>,
    state: TableReference<FlowStoreState>,
    metaflows: TableReference<StoredMetaflow>,
    metaflow_processes: TableReference<StoredMetaflowProcess>,
    metaflow_locks: TableReference<StoredMetaflowLock>,
    configuration: TableReference<FlowStoreConfiguration>,
    herdr_routes: TableReference<FlowHerdrRouteRecord>,
    launch_attempts: TableReference<StoredLaunchAttempt>,
    launch_panes: TableReference<StoredLaunchPane>,
    launch_outcomes: TableReference<StoredLaunchOutcome>,
    replacements: TableReference<Replacement>,
    roles: TableReference<StoredRole>,
    meta_configuration_tables: MetaConfigurationTables,
    quarantined_launch_attempts: TableReference<QuarantinedLaunchAttempt>,
    quarantined_launch_outcomes: TableReference<QuarantinedLaunchOutcome>,
    delivery_tables: DeliveryTables,
    event_tables: events::EventTables,
    context_sizes: TableReference<context::FlowContextSize>,
    address_holds: TableReference<address_hold::AddressHold>,
    opened_threads: TableReference<opened_thread::OpenedLaunchThread>,
    send_calls: TableReference<crate::generated::send_tool::SendCall>,
    launch_cleanups: TableReference<launch_cleanup::LaunchCleanup>,
    compactions: TableReference<compaction::Compaction>,
    successor_intents: TableReference<successor::SuccessorIntent>,
    queue_delivery: QueueDeliveryTables,
    send_outcomes: SendOutcomeTables,
    logical_deliveries: LogicalDeliveryTables,
    pub launch_changes: LaunchChanges,
    /// What opening did with launch-attempt rows older than 0.34's shape;
    /// each is also logged.
    pub opening_settlements: Vec<LaunchAttemptSettlement>,
}

#[cfg(test)]
pub(crate) trait SetsNativeOwnerEnvironmentForTests {
    fn set_native_owner_environment_for_tests(&mut self, environment: NativeOwnerEnvironment);
}

#[cfg(test)]
impl SetsNativeOwnerEnvironmentForTests for FlowStore {
    fn set_native_owner_environment_for_tests(&mut self, environment: NativeOwnerEnvironment) {
        self.native_owner_environment_override = Some(environment);
    }
}

/// The one-request configuration listener's view of the same Sema store.
/// It registers only the rows Configure owns and never seeds the rest of
/// Flow state or settles launches/deliveries; of the stored families it
/// upgrades only the leave-out defaults it serves.
/// Keeping this Engine alive holds redb's exclusive writer lock while that
/// listener owns the meta socket.
pub struct ConfigurationOnlyStore {
    engine: Engine,
    configuration_guard: Mutex<()>,
    configuration: TableReference<FlowStoreConfiguration>,
    meta_configuration_tables: MetaConfigurationTables,
}

pub trait ConfiguresOnlyStore: Sized {
    fn open(path: &Path) -> Result<Self, StoreError>;
    /// The open itself, on the store at `path` and in its place.
    fn open_in_place(path: &Path) -> Result<Self, StoreError>;
    fn configuration_snapshot(&self) -> Result<Option<StoredConfigurationSnapshot>, StoreError>;
    fn configured_texts(&self) -> Result<meta_signal_flow::Texts, StoreError>;
    fn runtime_bounds(
        &self,
    ) -> Result<(meta_signal_flow::Timings, meta_signal_flow::Limits), StoreError>;
    fn configure_change(&self, change: ConfigurationChange)
    -> Result<ConfigureOutcome, StoreError>;
    fn configure_full(&self, request: meta_signal_flow::FullConfigurationRequest)
    -> Result<meta_signal_flow::FullConfigurationReceipt, StoreError>;
    fn configure_entry(&self, entry: ConfigurationEntry) -> Result<ConfigureOutcome, StoreError>;
    fn remove_entry(&self, key: EntryKey) -> Result<ConfigureOutcome, StoreError>;
}

impl ConfiguresOnlyStore for ConfigurationOnlyStore {
    /// Opens the same versioned store as normal startup without invoking its
    /// seeding, settlement or adoption path, upgrading only the families it
    /// serves; a store that still holds a retired table drops it first. An
    /// open that changes the store runs on a staged copy that replaces the
    /// store only once the whole open has succeeded.
    fn open(path: &Path) -> Result<Self, StoreError> {
        if FlowStore::recognize_flow_memory(path)? {
            StagedUpgrade::run(path, UpgradeScope::ConfigurationOnly, |copy| {
                Self::open_in_place(copy).map(drop)
            })?;
        }
        Self::open_in_place(path)
    }

    fn open_in_place(path: &Path) -> Result<Self, StoreError> {
        let recognized = FlowStore::recognize_flow_memory(path)?;
        let legacy_nexus = if recognized {
            nexus_configuration::legacy_nexus(path)?
        } else {
            None
        };
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
        let meta_nexus_descriptor = StoredMetaNexusConfiguration::descriptor();
        let meta_harnesses_descriptor = harness::StoredHarness::descriptor();
        let meta_models_descriptor = StoredMetaModelConfiguration::descriptor();
        let meta_thresholds_descriptor = StoredMetaThresholdConfiguration::descriptor();
        let meta_investigation_descriptor = StoredMetaInvestigationConfiguration::descriptor();
        let meta_send_servers_descriptor = <send_server::StoredMetaSendServerConfiguration as send_server::DescribesSendServerTable>::descriptor();
        let meta_metaflows_descriptor = <metaflow_declaration::StoredMetaMetaflowConfiguration as metaflow_declaration::DescribesMetaflowDeclarationTable>::descriptor();
        let timings_descriptor = timings_limits::timings_descriptor();
        let limits_descriptor = timings_limits::limits_descriptor();
        let texts_descriptor = texts::texts_descriptor();
        let full_state_descriptor = full_configuration::state_descriptor();
        let full_receipt_descriptor = full_configuration::receipt_descriptor();
        if recognized {
            FlowStore::migrate_retired_leave_outs(path)?;
            FlowStore::drop_retired_tables(path)?;
        }
        // Family identities are part of the open request, so Engine validates
        // them before its first durable write. Existing paths have already
        // passed the read-only Memory identity check above.
        let mut engine = Engine::open(
            EngineOpen::new(path, SchemaVersion::new(FLOW_SCHEMA_VERSION))
                .with_family(&state_descriptor)
                .with_family(&configuration_descriptor)
                .with_family(&meta_nexus_descriptor)
                .with_family(&meta_harnesses_descriptor)
                .with_family(&meta_models_descriptor)
                .with_family(&meta_thresholds_descriptor)
                .with_family(&meta_send_servers_descriptor)
                .with_family(&meta_investigation_descriptor)
                .with_family(&meta_metaflows_descriptor)
                .with_family(&timings_descriptor)
                .with_family(&limits_descriptor)
                .with_family(&texts_descriptor)
                .with_family(&full_state_descriptor)
                .with_family(&full_receipt_descriptor),
        )?;
        let state = engine.register_table(state_descriptor)?;
        let configuration = engine.register_table(configuration_descriptor)?;
        let meta_configuration_tables = MetaConfigurationTables {
            nexus: engine.upgraded_nexus_configuration(meta_nexus_descriptor)?,
            harnesses: engine.upgraded_harnesses(meta_harnesses_descriptor)?,
            models: engine.upgraded_models(meta_models_descriptor)?,
            thresholds: engine.upgraded_thresholds(meta_thresholds_descriptor)?,
            send_servers: engine.register_table(meta_send_servers_descriptor)?,
            investigation: engine.register_table(meta_investigation_descriptor)?,
            metaflows: engine.upgraded_metaflow_declarations(meta_metaflows_descriptor)?,
            timings: engine.upgraded_timings(timings_descriptor)?,
            limits: engine.register_limits()?,
            texts: engine.register_texts()?,
            full_state: engine.register_table(full_state_descriptor)?,
            full_receipts: engine.register_table(full_receipt_descriptor)?,
        };
        if engine.match_records(QueryPlan::all(meta_configuration_tables.full_state))?.records().is_empty() {
            engine.assert(Assertion::new(meta_configuration_tables.full_state, full_configuration::StoredFullConfigurationState { revision: 0 }))?;
        }
        if !recognized {
            let defaults = DefaultConfiguration::from_environment();
            let source_root = defaults.source_root();
            engine.assert(Assertion::new(
                meta_configuration_tables.nexus,
                StoredMetaNexusConfiguration {
                    nexus: meta_signal_flow::ConfigureNexus {
                        source_root: source_root.to_string_lossy().into_owned(),
                        flows_root: source_root.join("flows").to_string_lossy().into_owned(),
                        meta_aspects: vec![Aspect::Psyche, Aspect::Mind, Aspect::Field],
                        message_nexus_path: String::new(),
                        message_nexus_binary: String::new(),
                        curriculum_nexus_path: defaults
                            .curriculum_socket_path()
                            .to_string_lossy()
                            .into_owned(),
                        lease: 60,
                        executables: meta_signal_flow::Executables {
                            herdr: String::new(),
                            flow_id_claimer: String::new(),
                            hook: String::new(),
                        },
                        default_herdr_session: "default".into(),
                    },
                },
            ))?;
        }
        if let Some(legacy) = legacy_nexus {
            let harnesses_empty = engine
                .match_records(QueryPlan::all(meta_configuration_tables.harnesses))?
                .records()
                .is_empty();
            if harnesses_empty {
                let models = engine
                    .match_records(QueryPlan::all(meta_configuration_tables.models))?
                    .records()
                    .iter()
                    .map(|row| row.model.clone())
                    .collect::<Vec<_>>();
                let migrated = harness::from_legacy_nexus(&legacy, &models);
                FlowStore::replace_configuration_family(
                    &engine,
                    meta_configuration_tables.harnesses,
                    migrated,
                )?;
            }
        }
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
            configuration_guard: Mutex::new(()),
            configuration,
            meta_configuration_tables,
        })
    }

    fn configuration_snapshot(&self) -> Result<Option<StoredConfigurationSnapshot>, StoreError> {
        FlowStore::meta_configuration_snapshot(&self.engine, &self.meta_configuration_tables)
    }

    fn configured_texts(&self) -> Result<meta_signal_flow::Texts, StoreError> {
        FlowStore::meta_texts(&self.engine, &self.meta_configuration_tables)
    }

    fn runtime_bounds(
        &self,
    ) -> Result<(meta_signal_flow::Timings, meta_signal_flow::Limits), StoreError> {
        let timings = self
            .engine
            .match_records(QueryPlan::all(self.meta_configuration_tables.timings))?;
        let limits = self
            .engine
            .match_records(QueryPlan::all(self.meta_configuration_tables.limits))?;
        match (timings.records(), limits.records()) {
            ([timings], [limits]) => Ok((timings.timings.clone(), limits.limits.clone())),
            _ => Err(StoreError::StateInvariant),
        }
    }

    fn configure_change(
        &self,
        change: ConfigurationChange,
    ) -> Result<ConfigureOutcome, StoreError> {
        FlowStore::configure_meta_change(&self.engine, &self.meta_configuration_tables, change)
    }

    fn configure_full(&self, request: meta_signal_flow::FullConfigurationRequest) -> Result<meta_signal_flow::FullConfigurationReceipt, StoreError> {
        let _guard = self.configuration_guard.lock().unwrap_or_else(|e| e.into_inner());
        <FlowStore as ReadsFlowMemory>::configure_full(&self.engine, &self.meta_configuration_tables, request)
    }

    fn configure_entry(&self, entry: ConfigurationEntry) -> Result<ConfigureOutcome, StoreError> {
        FlowStore::configure_meta_entry(&self.engine, &self.meta_configuration_tables, entry)
    }

    fn remove_entry(&self, key: EntryKey) -> Result<ConfigureOutcome, StoreError> {
        FlowStore::remove_meta_entry(&self.engine, &self.meta_configuration_tables, key)
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
    /// seeding defaults, importing an older store, or settling prior work.
    /// It drops the retired tables and upgrades the flows and launch
    /// outcomes from their 0.34 predecessor.
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
    /// An open that changes the store runs on a staged copy that replaces
    /// the store only once the whole open has succeeded; what that open
    /// set aside is reported by the store returned.
    fn open_configured(path: &Path) -> Result<Self, StoreError> {
        if !FlowStore::recognize_flow_memory(path)? {
            return Err(StoreError::NotConfigured);
        }
        let staged = StagedUpgrade::run(path, UpgradeScope::Running, |copy| {
            Self::open_configured_in_place(copy)
                .map(|mut store| std::mem::take(&mut store.opening_settlements))
        })?;
        let mut store = Self::open_configured_in_place(path)?;
        if let Some(settlements) = staged {
            store.opening_settlements = settlements;
        }
        Ok(store)
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
        )
        .with_predecessor::<FlowRecordV5>()
        .with_predecessor::<FlowRecordV6>();
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
        )
        .with_predecessor::<StoredMetaflowV1>()
        .with_predecessor::<StoredMetaflowV2>()
        .with_predecessor::<StoredMetaflowV3>()
        .with_predecessor::<StoredMetaflowV4>();
        let metaflow_processes_descriptor = TableDescriptor::<StoredMetaflowProcess>::new(
            METAFLOW_PROCESS_TABLE_NAME,
            FamilyName::new("flow-nexus-metaflow-process"),
            SchemaHash::for_label("flow-nexus-metaflow-process-v2"),
        )
        .with_predecessor::<StoredMetaflowProcessV1>();
        let metaflow_locks_descriptor = TableDescriptor::<StoredMetaflowLock>::new(
            METAFLOW_LOCK_TABLE_NAME,
            FamilyName::new("flow-nexus-metaflow-lock"),
            SchemaHash::for_label("flow-nexus-metaflow-lock-v2"),
        )
        .with_predecessor::<StoredMetaflowLockV1>();
        let configuration_descriptor = TableDescriptor::<FlowStoreConfiguration>::new(
            FLOW_CONFIGURATION_TABLE_NAME,
            FamilyName::new("flow-nexus-configuration"),
            SchemaHash::for_label("flow-nexus-configuration-v1"),
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
        .with_predecessor::<StoredLaunchAttemptV1>()
        .with_predecessor::<StoredLaunchAttemptV2>()
        .with_predecessor::<StoredLaunchAttemptV3>()
        .with_predecessor::<StoredLaunchAttemptV4>();
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
        .with_predecessor::<StoredLaunchOutcomeV4>()
        .with_predecessor::<StoredLaunchOutcomeV5>()
        .with_predecessor::<StoredLaunchOutcomeV6>()
        .with_predecessor::<StoredLaunchOutcomeV7>()
        .with_predecessor::<StoredLaunchOutcomeV8>()
        .with_predecessor::<StoredLaunchOutcomeV9>()
        .with_predecessor::<StoredLaunchOutcomeV10>()
        .with_predecessor::<StoredLaunchOutcomeV11>();
        let replacements_descriptor = TableDescriptor::<Replacement>::new(
            FLOW_REPLACEMENT_TABLE_NAME,
            FamilyName::new("flow-nexus-replacement"),
            SchemaHash::for_label("flow-nexus-replacement-v1"),
        );
        let roles_descriptor = TableDescriptor::<StoredRole>::new(
            FLOW_ROLE_TABLE_NAME,
            FamilyName::new("flow-nexus-role"),
            SchemaHash::for_label("flow-nexus-role-v2"),
        )
        .with_predecessor::<StoredRoleV1>();
        let quarantined_launch_attempts_descriptor =
            TableDescriptor::<QuarantinedLaunchAttempt>::new(
                FLOW_QUARANTINED_LAUNCH_ATTEMPT_TABLE_NAME,
                FamilyName::new("flow-nexus-quarantined-launch-attempt"),
                SchemaHash::for_label("flow-nexus-quarantined-launch-attempt-v1"),
            );
        let quarantined_launch_outcomes_descriptor =
            TableDescriptor::<QuarantinedLaunchOutcome>::new(
                FLOW_QUARANTINED_LAUNCH_OUTCOME_TABLE_NAME,
                FamilyName::new("flow-nexus-quarantined-launch-outcome"),
                QuarantinedLaunchOutcome::schema_hash(),
            )
            .with_predecessor::<QuarantinedLaunchOutcomeV1>();
        let delivery_descriptor = TableDescriptor::<delivery::StoredDelivery>::new(
            delivery::DELIVERY_TABLE_NAME,
            FamilyName::new("flow-nexus-delivery"),
            delivery::StoredDelivery::schema_hash(),
        )
        .with_predecessor::<delivery::StoredDeliveryV1>();
        let pane_lease_descriptor = TableDescriptor::<delivery::PaneLease>::new(
            delivery::PANE_LEASE_TABLE_NAME,
            FamilyName::new("flow-nexus-pane-lease"),
            SchemaHash::for_label("flow-nexus-pane-lease-v1"),
        );
        let meta_nexus_descriptor = StoredMetaNexusConfiguration::descriptor();
        let meta_models_descriptor = StoredMetaModelConfiguration::descriptor();
        let meta_thresholds_descriptor = StoredMetaThresholdConfiguration::descriptor();
        let meta_investigation_descriptor = StoredMetaInvestigationConfiguration::descriptor();
        let compactions_descriptor =
            <compaction::Compaction as compaction::DescribesCompactionTable>::descriptor();
        let successor_intents_descriptor =
            <successor::SuccessorIntent as successor::DescribesSuccessorIntentTable>::descriptor();
        let timings_descriptor = timings_limits::timings_descriptor();
        let limits_descriptor = timings_limits::limits_descriptor();
        let texts_descriptor = texts::texts_descriptor();
        let full_state_descriptor = full_configuration::state_descriptor();
        let full_receipt_descriptor = full_configuration::receipt_descriptor();
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
        let mut engine = Engine::open(
            EngineOpen::new(path, SchemaVersion::new(FLOW_SCHEMA_VERSION))
                .with_family(&flows_descriptor)
                .with_family(&native_owner_proofs_descriptor)
                .with_family(&state_descriptor)
                .with_family(&metaflows_descriptor)
                .with_family(&metaflow_processes_descriptor)
                .with_family(&metaflow_locks_descriptor)
                .with_family(&configuration_descriptor)
                .with_family(&herdr_routes_descriptor)
                .with_family(&launch_attempts_descriptor)
                .with_family(&launch_panes_descriptor)
                .with_family(&launch_outcomes_descriptor)
                .with_family(&replacements_descriptor)
                .with_family(&roles_descriptor)
                .with_family(&quarantined_launch_attempts_descriptor)
                .with_family(&quarantined_launch_outcomes_descriptor)
                .with_family(&delivery_descriptor)
                .with_family(&pane_lease_descriptor)
                .with_family(&meta_nexus_descriptor)
                .with_family(&meta_models_descriptor)
                .with_family(&meta_thresholds_descriptor)
                .with_family(&<send_server::StoredMetaSendServerConfiguration as send_server::DescribesSendServerTable>::descriptor())
                .with_family(&meta_investigation_descriptor)
                .with_family(&timings_descriptor)
                .with_family(&limits_descriptor)
                .with_family(&texts_descriptor)
                .with_family(&full_state_descriptor)
                .with_family(&full_receipt_descriptor)
                .with_family(&compactions_descriptor)
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
                .with_family(&logical_delivery_descriptors.events),
        )?;
        let flows = engine.upgraded_flows(flows_descriptor)?;
        let native_owner_proofs = engine.register_table(native_owner_proofs_descriptor)?;
        let state = engine.register_table(state_descriptor)?;
        let metaflows = engine.upgraded_metaflows(metaflows_descriptor)?;
        let metaflow_processes =
            engine.upgraded_metaflow_processes(metaflow_processes_descriptor)?;
        let metaflow_locks = engine.upgraded_metaflow_locks(metaflow_locks_descriptor)?;
        let configuration = engine.register_table(configuration_descriptor)?;
        let herdr_routes = engine.register_table(herdr_routes_descriptor)?;
        let quarantined_launch_attempts =
            engine.register_table(quarantined_launch_attempts_descriptor)?;
        let quarantined_launch_outcomes =
            engine.upgraded_quarantined_launch_outcomes(quarantined_launch_outcomes_descriptor)?;
        let (launch_attempts, opening_settlements) = engine
            .upgraded_launch_attempts(launch_attempts_descriptor, quarantined_launch_attempts)?;
        let launch_panes = engine.register_table(launch_panes_descriptor)?;
        let launch_outcomes = engine.upgraded_launch_outcomes(
            launch_outcomes_descriptor,
            launch_attempts,
            quarantined_launch_outcomes,
        )?;
        let replacements = engine.register_table(replacements_descriptor)?;
        let roles = engine.upgraded_roles(roles_descriptor)?;
        let delivery_tables = DeliveryTables::register(&mut engine)?;
        let meta_configuration_tables = MetaConfigurationTables::register(&mut engine)?;
        let event_tables =
            <events::EventTables as events::RegistersEventTables>::register(&mut engine)?;
        let context_sizes = engine.register_table(context_sizes_descriptor)?;
        let address_holds = engine.upgraded_address_holds(address_holds_descriptor)?;
        let opened_threads = engine.register_table(opened_threads_descriptor)?;
        let send_calls = engine.upgraded_send_calls(send_calls_descriptor)?;
        let launch_cleanups = engine.register_table(launch_cleanups_descriptor)?;
        let compactions = engine.register_table(compactions_descriptor)?;
        let successor_intents = successor::UpgradesSuccessorIntents::upgraded_successor_intents(
            &mut engine,
            successor_intents_descriptor,
        )?;
        let queue_delivery =
            QueueDeliveryTables::register(&mut engine, queue_delivery_descriptors)?;
        let send_outcomes = SendOutcomeTables::register(&mut engine, send_outcome_descriptors)?;
        let logical_deliveries =
            LogicalDeliveryTables::register(&mut engine, logical_delivery_descriptors)?;
        let store = Self {
            engine,
            configuration_guard: Mutex::new(()),
            native_owner_environment_override: None,
            flows,
            native_owner_proofs,
            state,
            metaflows,
            metaflow_processes,
            metaflow_locks,
            configuration,
            herdr_routes,
            launch_attempts,
            launch_panes,
            launch_outcomes,
            replacements,
            roles,
            meta_configuration_tables,
            quarantined_launch_attempts,
            quarantined_launch_outcomes,
            delivery_tables,
            event_tables,
            context_sizes,
            address_holds,
            opened_threads,
            send_calls,
            launch_cleanups,
            compactions,
            successor_intents,
            queue_delivery,
            send_outcomes,
            logical_deliveries,
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
        store.archive_legacy_queue()?;
        store.seed_missing_queued_deliveries()?;
        store.quarantine_missing_historical_senders()?;
        store
            .configuration_snapshot()?
            .ok_or(StoreError::NotConfigured)?;
        Ok(store)
    }

    fn open_seeded(path: &Path, defaults: &DefaultConfiguration) -> Result<Self, StoreError> {
        if path.exists() {
            FlowStore::migrate_retired_leave_outs(path)?;
            FlowStore::drop_retired_tables(path)?;
        }
        let mut engine = Engine::open(EngineOpen::new(path, SchemaVersion::new(1)))?;
        let flows = engine.upgraded_flows(
            TableDescriptor::new(
                FLOW_TABLE_NAME,
                FamilyName::new("flow-nexus-flow"),
                FlowRecord::schema_hash(),
            )
            .with_predecessor::<FlowRecordV5>()
            .with_predecessor::<FlowRecordV6>(),
        )?;
        let native_owner_proofs = engine.register_table(NativeOwnerProofRecord::descriptor())?;
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
            .with_predecessor::<StoredMetaflowV1>()
            .with_predecessor::<StoredMetaflowV2>()
            .with_predecessor::<StoredMetaflowV3>()
            .with_predecessor::<StoredMetaflowV4>(),
        )?;
        let metaflow_processes = engine.upgraded_metaflow_processes(
            TableDescriptor::new(
                METAFLOW_PROCESS_TABLE_NAME,
                FamilyName::new("flow-nexus-metaflow-process"),
                SchemaHash::for_label("flow-nexus-metaflow-process-v2"),
            )
            .with_predecessor::<StoredMetaflowProcessV1>(),
        )?;
        let metaflow_locks = engine.register_table(TableDescriptor::new(
            METAFLOW_LOCK_TABLE_NAME,
            FamilyName::new("flow-nexus-metaflow-lock"),
            SchemaHash::for_label("flow-nexus-metaflow-lock-v2"),
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
        let launch_panes = engine.register_table(TableDescriptor::<StoredLaunchPane>::new(
            FLOW_LAUNCH_PANE_TABLE_NAME,
            FamilyName::new("flow-nexus-launch-pane"),
            SchemaHash::for_label("flow-nexus-launch-pane-v1"),
        ))?;
        let launch_outcomes_descriptor = TableDescriptor::new(
            FLOW_LAUNCH_OUTCOME_TABLE_NAME,
            FamilyName::new("flow-nexus-launch-outcome"),
            StoredLaunchOutcome::schema_hash(),
        )
        .with_predecessor::<StoredLaunchOutcomeV2>()
        .with_predecessor::<StoredLaunchOutcomeV3>()
        .with_predecessor::<StoredLaunchOutcomeV4>()
        .with_predecessor::<StoredLaunchOutcomeV5>()
        .with_predecessor::<StoredLaunchOutcomeV6>()
        .with_predecessor::<StoredLaunchOutcomeV7>()
        .with_predecessor::<StoredLaunchOutcomeV8>()
        .with_predecessor::<StoredLaunchOutcomeV9>()
        .with_predecessor::<StoredLaunchOutcomeV10>()
        .with_predecessor::<StoredLaunchOutcomeV11>();
        let replacements = engine.register_table(TableDescriptor::new(
            FLOW_REPLACEMENT_TABLE_NAME,
            FamilyName::new("flow-nexus-replacement"),
            SchemaHash::for_label("flow-nexus-replacement-v1"),
        ))?;
        let roles = engine.upgraded_roles(
            TableDescriptor::new(
                FLOW_ROLE_TABLE_NAME,
                FamilyName::new("flow-nexus-role"),
                SchemaHash::for_label("flow-nexus-role-v2"),
            )
            .with_predecessor::<StoredRoleV1>(),
        )?;
        let quarantined_launch_attempts = engine.register_table(TableDescriptor::new(
            FLOW_QUARANTINED_LAUNCH_ATTEMPT_TABLE_NAME,
            FamilyName::new("flow-nexus-quarantined-launch-attempt"),
            SchemaHash::for_label("flow-nexus-quarantined-launch-attempt-v1"),
        ))?;
        let quarantined_launch_outcomes = engine.upgraded_quarantined_launch_outcomes(
            TableDescriptor::new(
                FLOW_QUARANTINED_LAUNCH_OUTCOME_TABLE_NAME,
                FamilyName::new("flow-nexus-quarantined-launch-outcome"),
                QuarantinedLaunchOutcome::schema_hash(),
            )
            .with_predecessor::<QuarantinedLaunchOutcomeV1>(),
        )?;
        let (launch_attempts, opening_settlements) = engine.upgraded_launch_attempts(
            TableDescriptor::new(
                FLOW_LAUNCH_ATTEMPT_TABLE_NAME,
                FamilyName::new("flow-nexus-launch-attempt"),
                StoredLaunchAttempt::schema_hash(),
            )
            .with_predecessor::<StoredLaunchAttemptV1>()
            .with_predecessor::<StoredLaunchAttemptV2>()
            .with_predecessor::<StoredLaunchAttemptV3>()
            .with_predecessor::<StoredLaunchAttemptV4>(),
            quarantined_launch_attempts,
        )?;
        let launch_outcomes = engine.upgraded_launch_outcomes(
            launch_outcomes_descriptor,
            launch_attempts,
            quarantined_launch_outcomes,
        )?;
        let delivery_tables = DeliveryTables::register(&mut engine)?;
        let meta_configuration_tables = MetaConfigurationTables::register(&mut engine)?;
        if engine
            .match_records(QueryPlan::all(meta_configuration_tables.nexus))?
            .records()
            .is_empty()
        {
            let source_root = defaults.source_root();
            engine.assert(Assertion::new(
                meta_configuration_tables.nexus,
                StoredMetaNexusConfiguration {
                    nexus: meta_signal_flow::ConfigureNexus {
                        source_root: source_root.to_string_lossy().into_owned(),
                        flows_root: source_root.join("flows").to_string_lossy().into_owned(),
                        meta_aspects: vec![Aspect::Psyche, Aspect::Mind, Aspect::Field],
                        message_nexus_path: String::new(),
                        message_nexus_binary: String::new(),
                        curriculum_nexus_path: defaults
                            .curriculum_socket_path()
                            .to_string_lossy()
                            .into_owned(),
                        lease: 60,
                        executables: meta_signal_flow::Executables {
                            herdr: String::new(),
                            flow_id_claimer: String::new(),
                            hook: String::new(),
                        },
                        default_herdr_session: "default".into(),
                    },
                },
            ))?;
        }
        let event_tables =
            <events::EventTables as events::RegistersEventTables>::register(&mut engine)?;
        let context_sizes = engine.register_table(
            <context::FlowContextSize as context::DescribesContextSizeTable>::descriptor(),
        )?;
        let address_holds = engine.upgraded_address_holds(
            <address_hold::AddressHold as address_hold::DescribesAddressHoldTable>::descriptor(),
        )?;
        let opened_threads = engine.register_table(
            <opened_thread::OpenedLaunchThread as opened_thread::DescribesOpenedThreadTable>::descriptor(
            ),
        )?;
        let send_calls = engine.upgraded_send_calls(
            <crate::generated::send_tool::SendCall as send_call::DescribesSendCallTable>::descriptor(),
        )?;
        let launch_cleanups = engine.register_table(
            <launch_cleanup::LaunchCleanup as launch_cleanup::DescribesLaunchCleanupTable>::descriptor(),
        )?;
        let compactions = engine.register_table(
            <compaction::Compaction as compaction::DescribesCompactionTable>::descriptor(),
        )?;
        let successor_intents = successor::UpgradesSuccessorIntents::upgraded_successor_intents(
            &mut engine,
            <successor::SuccessorIntent as successor::DescribesSuccessorIntentTable>::descriptor(),
        )?;
        let queue_delivery =
            QueueDeliveryTables::register(&mut engine, QueueDeliveryTables::descriptors())?;
        let send_outcomes =
            SendOutcomeTables::register(&mut engine, SendOutcomeTables::descriptors())?;
        let logical_deliveries =
            LogicalDeliveryTables::register(&mut engine, LogicalDeliveryTables::descriptors())?;
        let store = Self {
            engine,
            configuration_guard: Mutex::new(()),
            native_owner_environment_override: None,
            flows,
            native_owner_proofs,
            state,
            metaflows,
            metaflow_processes,
            metaflow_locks,
            configuration,
            herdr_routes,
            launch_attempts,
            launch_panes,
            launch_outcomes,
            replacements,
            roles,
            meta_configuration_tables,
            quarantined_launch_attempts,
            quarantined_launch_outcomes,
            delivery_tables,
            event_tables,
            context_sizes,
            address_holds,
            opened_threads,
            send_calls,
            launch_cleanups,
            compactions,
            successor_intents,
            queue_delivery,
            send_outcomes,
            logical_deliveries,
            launch_changes: LaunchChanges::default(),
            opening_settlements,
        };
        store.archive_legacy_queue()?;
        store.seed_missing_queued_deliveries()?;
        store.quarantine_missing_historical_senders()?;
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
        for settlement in &store.opening_settlements {
            eprintln!("flow-nexus: {settlement}");
        }
        store.adopt_legacy_roles()?;
        // A delivery a crash left under the lease is settled Uncertain here
        // and never retried: what reached its pane is not known.
        for delivery in store.settle_interrupted_deliveries()? {
            eprintln!(
                "flow-nexus: delivery {} to {} was interrupted under its lease; settled Uncertain",
                delivery.delivery_id, delivery.flow_id
            );
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
                    "flow-nexus: queued delivery {} is uncertain; it is reconciled from native history",
                    attempt.attempt_id
                ),
                QueueDeliveryPhase::Prepared
                | QueueDeliveryPhase::Settled(_)
                | QueueDeliveryPhase::Absent(_)
                | QueueDeliveryPhase::Resolved(_)
                | QueueDeliveryPhase::TransportOverlimit(_) => {
                    return Err(StoreError::StateInvariant);
                }
            }
        }
        Ok(store)
    }
}

pub(crate) trait ManagesMetaflowState: Sized {
    fn configuration_snapshot(&self) -> Result<Option<StoredConfigurationSnapshot>, StoreError>;
    fn configured_texts(&self) -> Result<meta_signal_flow::Texts, StoreError>;
    /// Replace one configuration family whole with its declaration.
    fn configure_change(&self, change: ConfigurationChange)
    -> Result<ConfigureOutcome, StoreError>;
    fn configure_full(&self, request: meta_signal_flow::FullConfigurationRequest)
    -> Result<meta_signal_flow::FullConfigurationReceipt, StoreError>;
    fn configure_entry(&self, entry: ConfigurationEntry) -> Result<ConfigureOutcome, StoreError>;
    fn remove_entry(&self, key: EntryKey) -> Result<ConfigureOutcome, StoreError>;
    /// The refresh threshold configured for one model, if any.
    fn configured_threshold(
        &self,
        model_name: &str,
    ) -> Result<Option<ConfigureThreshold>, StoreError>;
    /// The leave-outs a metaflow's Start on one harness takes when it names
    /// none; empty when none are configured for that harness.
    fn default_left_out(
        &self,
        address: &Address,
        harness_kind: &HarnessKind,
    ) -> Result<Vec<LeaveOut>, StoreError>;
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
    fn identify_process_sender(
        &self,
        process: Process,
    ) -> Result<ProcessSenderResolution, StoreError>;
    fn release_metaflow_lock(
        &self,
        lock: Lock,
        now_epoch_seconds: i64,
    ) -> Result<Response, StoreError>;
}

impl ManagesMetaflowState for FlowStore {
    fn configuration_snapshot(&self) -> Result<Option<StoredConfigurationSnapshot>, StoreError> {
        FlowStore::meta_configuration_snapshot(&self.engine, &self.meta_configuration_tables)
    }

    fn configured_texts(&self) -> Result<meta_signal_flow::Texts, StoreError> {
        FlowStore::meta_texts(&self.engine, &self.meta_configuration_tables)
    }

    fn configure_change(
        &self,
        change: ConfigurationChange,
    ) -> Result<ConfigureOutcome, StoreError> {
        FlowStore::configure_meta_change(&self.engine, &self.meta_configuration_tables, change)
    }

    fn configure_full(&self, request: meta_signal_flow::FullConfigurationRequest) -> Result<meta_signal_flow::FullConfigurationReceipt, StoreError> {
        let _guard = self.configuration_guard.lock().unwrap_or_else(|e| e.into_inner());
        <FlowStore as ReadsFlowMemory>::configure_full(&self.engine, &self.meta_configuration_tables, request)
    }

    fn configure_entry(&self, entry: ConfigurationEntry) -> Result<ConfigureOutcome, StoreError> {
        FlowStore::configure_meta_entry(&self.engine, &self.meta_configuration_tables, entry)
    }

    fn remove_entry(&self, key: EntryKey) -> Result<ConfigureOutcome, StoreError> {
        FlowStore::remove_meta_entry(&self.engine, &self.meta_configuration_tables, key)
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

    fn default_left_out(
        &self,
        address: &Address,
        harness_kind: &HarnessKind,
    ) -> Result<Vec<LeaveOut>, StoreError> {
        let _ = harness_kind;
        Ok(self
            .configuration_snapshot()?
            .and_then(|snapshot| {
                snapshot
                    .configured_metaflows
                    .into_iter()
                    .find(|row| &row.address == address)
                    .map(|row| row.left_out)
            })
            .unwrap_or_default())
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

    fn identify_process_sender(
        &self,
        process: Process,
    ) -> Result<ProcessSenderResolution, StoreError> {
        let lineage = FlowStore::process_lineage(&process);
        if lineage.is_empty() {
            return Ok(ProcessSenderResolution::NoBinding);
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
            let Some(flow_id) = binding.flow_id_option.clone() else {
                return Ok(ProcessSenderResolution::MissingFlowIdentity);
            };
            let Some(stored) = self.stored_metaflow(&binding.address)? else {
                return Ok(ProcessSenderResolution::StaleFlowIdentity);
            };
            if stored.metaflow.state != MetaflowState::Awake(flow_id.clone()) {
                return Ok(ProcessSenderResolution::StaleFlowIdentity);
            }
            return Ok(ProcessSenderResolution::Sender(meta_signal_flow::Sender {
                address: binding.address.clone(),
                flow_id,
            }));
        }
        Ok(ProcessSenderResolution::NoBinding)
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

impl FlowStore {
    /// Reads every mutable configuration family from one database marker.
    /// Callers keep this value for the duration of one logical effect.
    pub(crate) fn committed_configuration_snapshot(
        &self,
    ) -> Result<Option<CommittedConfigurationSnapshot>, StoreError> {
        Self::meta_committed_configuration_snapshot(&self.engine, &self.meta_configuration_tables)
    }
}

impl AppliesFlowQuery for FlowStore {
    fn apply(&self, query: Query) -> Result<Response, StoreError> {
        match query {
            // The store alone launches nothing.
            Query::Start(_) => Ok(Response::StartRejected(
                StartRejection::LaunchPersistenceRefused,
            )),
            // A refresh closes a pane, which the store alone cannot.
            Query::Refresh(_) => Ok(Response::Refused(signal_flow::Refusal::Store(
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
            | Query::ObserveSends(_)
            | Query::ObserveSender(_) => Ok(Response::Refused(signal_flow::Refusal::Unidentified(
                signal_flow::Process { pid: 0, started: 0 },
            ))),
            Query::Launch(_) => Ok(Response::StartRejected(
                StartRejection::LaunchPersistenceRefused,
            )),
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
                let unresolved = match &report.reporter {
                    signal_flow::Reporter::Flow(flow_id) => flow_id.clone(),
                    signal_flow::Reporter::Session(session_id) => session_id.clone(),
                };
                let flow_id = match report.reporter {
                    signal_flow::Reporter::Flow(flow_id) => Some(flow_id),
                    signal_flow::Reporter::Session(session_id) => {
                        let mut matches = self
                            .flow_nodes()?
                            .into_iter()
                            .filter(|node| node.session_id == session_id);
                        let found = matches.next();
                        (matches.next().is_none())
                            .then_some(found)
                            .flatten()
                            .map(|node| node.flow_id)
                    }
                };
                let Some(flow_id) = flow_id else {
                    return Ok(Response::Refused(signal_flow::Refusal::Unknown(
                        signal_flow::Unknown_Data::FlowId(unresolved),
                    )));
                };
                match self.record_event(&flow_id, report.event)? {
                    EventRecording::Recorded => Ok(Response::Reported),
                    EventRecording::UnknownFlow => Ok(Response::Refused(
                        signal_flow::Refusal::Unknown(signal_flow::Unknown_Data::FlowId(flow_id)),
                    )),
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
            | Query::BindSeat(_) => Ok(None),
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

trait SettlesLaunchOutcomes {
    /// Archives every legacy leave-out refusal before any predecessor
    /// conversion can erase its historical meaning.
    fn archive_legacy_leave_outs(
        &mut self,
        quarantine: TableReference<QuarantinedLaunchOutcome>,
    ) -> Result<(), StoreError>;
    /// Moves a v8 NoSecondary outcome whose historical attempt cannot name
    /// exactly one Address into a durable archive before the remaining v8
    /// family is upgraded. The surrounding staged store open activates the
    /// archive, retraction, and family upgrade together.
    fn settle_launch_outcomes(
        &mut self,
        attempts: TableReference<StoredLaunchAttempt>,
        quarantine: TableReference<QuarantinedLaunchOutcome>,
    ) -> Result<(), StoreError>;
    fn launch_outcome_archives(&self) -> Result<Vec<(String, Vec<u8>)>, StoreError>;
}

impl SettlesLaunchOutcomes for Engine {
    fn archive_legacy_leave_outs(
        &mut self,
        quarantine: TableReference<QuarantinedLaunchOutcome>,
    ) -> Result<(), StoreError> {
        macro_rules! archive_version {
            ($stored:ty, $version:literal) => {{
                let descriptor = TableDescriptor::<$stored>::new(
                    FLOW_LAUNCH_OUTCOME_TABLE_NAME,
                    FamilyName::new("flow-nexus-launch-outcome"),
                    <$stored>::schema_hash(),
                );
                if self.catalog().family_identity(&FLOW_LAUNCH_OUTCOME_TABLE_NAME)
                    == Some(&descriptor.family_identity())
                {
                    let predecessor = self.register_table(descriptor)?;
                    for (stored_key, archive) in self.launch_outcome_archives()? {
                        let previous = rkyv::from_bytes::<$stored, rkyv::rancor::Error>(&archive)
                            .map_err(|error| {
                                StoreError::LaunchOutcomeUpgrade(format!(
                                    "v{} launch outcome {stored_key} could not be read before archival: {error:?}",
                                    $version
                                ))
                            })?;
                        if previous.launch_outcome.has_legacy_leave_out_refusal() {
                            self.commit_atomic(
                                self.begin_atomic_commit()
                                    .retract(predecessor, RecordKey::new(stored_key.clone()))
                                    .assert(
                                        quarantine,
                                        QuarantinedLaunchOutcome {
                                            launch_request_id: stored_key,
                                            origin_family: "flow-nexus-launch-outcome".into(),
                                            origin_version: $version,
                                            archive,
                                            reason: signal_flow::LaunchArchiveReason::LegacyLeaveOutRefusal,
                                        },
                                    ),
                            )?;
                        }
                    }
                    return Ok(());
                }
            }};
        }
        archive_version!(StoredLaunchOutcomeV11, 11);
        archive_version!(StoredLaunchOutcomeV10, 10);
        archive_version!(StoredLaunchOutcomeV9, 9);
        archive_version!(StoredLaunchOutcomeV8, 8);
        archive_version!(StoredLaunchOutcomeV7, 7);
        archive_version!(StoredLaunchOutcomeV6, 6);
        archive_version!(StoredLaunchOutcomeV5, 5);
        archive_version!(StoredLaunchOutcomeV4, 4);
        archive_version!(StoredLaunchOutcomeV3, 3);
        archive_version!(StoredLaunchOutcomeV2, 2);
        Ok(())
    }

    fn settle_launch_outcomes(
        &mut self,
        _attempts: TableReference<StoredLaunchAttempt>,
        quarantine: TableReference<QuarantinedLaunchOutcome>,
    ) -> Result<(), StoreError> {
        let attempts = self
            .launch_attempt_archives()?
            .into_iter()
            .filter_map(|(key, archive)| {
                rkyv::from_bytes::<StoredLaunchAttempt, rkyv::rancor::Error>(&archive)
                    .ok()
                    .map(|attempt| (key, attempt))
            })
            .collect::<Vec<_>>();
        let mut predecessor = None;
        for (stored_key, archive) in self.launch_outcome_archives()? {
            let Ok(previous) =
                rkyv::from_bytes::<StoredLaunchOutcomeV8, rkyv::rancor::Error>(&archive)
            else {
                continue;
            };
            if !previous.launch_outcome.needs_requirement_address() {
                continue;
            }
            let exact = attempts
                .iter()
                .filter(|(key, attempt)| {
                    key == &previous.launch_request_id
                        && attempt.attempt.launch_request_id == previous.launch_request_id
                })
                .count();
            if exact == 1 {
                continue;
            }
            let related = attempts.iter().any(|(key, attempt)| {
                key == &previous.launch_request_id
                    || attempt.attempt.launch_request_id == previous.launch_request_id
            });
            let reason = if exact > 1 || related || stored_key != previous.launch_request_id {
                HistoricalAttemptReason::Ambiguous
            } else {
                HistoricalAttemptReason::Missing
            };
            let old = match predecessor {
                Some(table) => table,
                None => {
                    let table =
                        self.register_table(TableDescriptor::<StoredLaunchOutcomeV8>::new(
                            FLOW_LAUNCH_OUTCOME_TABLE_NAME,
                            FamilyName::new("flow-nexus-launch-outcome"),
                            StoredLaunchOutcomeV8::schema_hash(),
                        ))?;
                    predecessor = Some(table);
                    table
                }
            };
            self.commit_atomic(
                self.begin_atomic_commit()
                    .retract(old, RecordKey::new(stored_key.clone()))
                    .assert(
                        quarantine,
                        QuarantinedLaunchOutcome {
                            launch_request_id: stored_key,
                            origin_family: "flow-nexus-launch-outcome".into(),
                            origin_version: 8,
                            archive,
                            reason: match reason {
                                HistoricalAttemptReason::Missing => {
                                    signal_flow::LaunchArchiveReason::HistoricalAttemptMissing
                                }
                                HistoricalAttemptReason::Ambiguous => {
                                    signal_flow::LaunchArchiveReason::HistoricalAttemptAmbiguous
                                }
                            },
                        },
                    ),
            )?;
        }
        Ok(())
    }

    fn launch_outcome_archives(&self) -> Result<Vec<(String, Vec<u8>)>, StoreError> {
        let definition =
            redb::TableDefinition::<String, &[u8]>::new(FLOW_LAUNCH_OUTCOME_TABLE_NAME.as_str());
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
                        layer: attempt.launch_profile.address.layer.clone(),
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
        let layer = match parts.next()? {
            "High" | "Primary" => Layer::Primary,
            "Medium" | "Secondary" => Layer::Secondary,
            "Low" | "Tertiary" => Layer::Tertiary,
            "UltraLow" | "Quaternary" => Layer::Quaternary,
            _ => return None,
        };
        let model_name = parts.next().filter(|model| !model.is_empty())?;
        Some(Caller {
            flow_id: self.flow_id.clone(),
            aspect,
            layer,
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
    fn adopt_existing_flow(
        &self,
        flow_node: FlowNode,
        role: Caller,
        address: Address,
        binding: &meta_signal_flow::FlowBinding,
    ) -> Result<ExistingSeatAdoption, StoreError> {
        use meta_signal_flow::FlowBindingRefusalReason as Refusal;
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
            flow_type: format!("{:?}:{:?}:{}", role.aspect, role.layer, role.model_name),
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
            && if native_intent.prompt_sha256_vector.is_empty() {
                false
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
            [] => {
                let archived = self
                    .engine
                    .match_records(QueryPlan::key(
                        self.quarantined_launch_outcomes,
                        RecordKey::new(launch_request_id),
                    ))?
                    .records()
                    .to_vec();
                match archived.as_slice() {
                    [] => Ok(None),
                    [archived] => Ok(Some(LaunchOutcome::Archived(archived.reference()))),
                    _ => Err(StoreError::StateInvariant),
                }
            }
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
        let snapshot = self
            .configuration_snapshot()?
            .ok_or(StoreError::NotConfigured)?;
        let endpoint_selection = snapshot
            .configured_harnesses
            .into_iter()
            .find(|harness| harness.harness_kind == HarnessKind::Codex)
            .and_then(|harness| harness.control_socket_path_option)
            .map(|endpoint_path| {
                EndpointSelection::Available(signal_flow::Available_Data {
                    endpoint_path,
                    route_readiness: RouteReadiness::Parked,
                })
            })
            .unwrap_or(EndpointSelection::Unavailable);
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
                        harness_kind: signal_flow::HarnessKind::Codex,
                        endpoint_selection,
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
            endpoint_selection: signal_flow::EndpointSelection::Unavailable,
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

    /// A configured model's key: its layer and aspect.
    fn model_record_key(layer: &Layer, aspect: &Aspect) -> String {
        let aspect = match aspect {
            Aspect::Psyche => "Psyche",
            Aspect::Mind => "Mind",
            Aspect::Field => "Field",
        };
        format!("{}:{aspect}", FlowStore::layer_key(layer))
    }

    /// A leave-out default's key: its Address's key, then its harness.
    fn leave_out_record_key(address: &Address, harness_kind: &HarnessKind) -> String {
        let harness = match harness_kind {
            HarnessKind::Claude => "Claude",
            HarnessKind::Codex => "Codex",
        };
        format!("{}:{harness}", FlowStore::address_record_key(address))
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
            "flow_nexus_roles" => ("flow-nexus-role", "flow-nexus-role-v2"),
            "flow_nexus_quarantined_launch_attempts" => (
                "flow-nexus-quarantined-launch-attempt",
                "flow-nexus-quarantined-launch-attempt-v1",
            ),
            "flow_nexus_quarantined_launch_outcomes" => (
                "flow-nexus-quarantined-launch-outcome",
                "flow-nexus-quarantined-launch-outcome-v2",
            ),
            "flow_nexus_metaflows" => ("flow-nexus-metaflow", "flow-nexus-metaflow-v5"),
            "flow_nexus_metaflow_processes" => (
                "flow-nexus-metaflow-process",
                "flow-nexus-metaflow-process-v2",
            ),
            "flow_nexus_metaflow_locks" => {
                ("flow-nexus-metaflow-lock", "flow-nexus-metaflow-lock-v2")
            }
            "flow_nexus_deliveries" => {
                return Some((
                    "flow-nexus-delivery",
                    delivery::StoredDelivery::schema_hash(),
                ));
            }
            "flow_nexus_pane_leases" => ("flow-nexus-pane-lease", "flow-nexus-pane-lease-v1"),
            "flow_nexus_delivery_configuration" => (
                "flow-nexus-delivery-configuration",
                "flow-nexus-delivery-configuration-v1",
            ),
            "flow_nexus_meta_nexus_configuration" => {
                return Some((
                    nexus_configuration::META_NEXUS_CONFIGURATION_FAMILY,
                    StoredMetaNexusConfiguration::schema_hash(),
                ));
            }
            "flow_nexus_meta_harness_configuration" => {
                return Some((
                    "flow-nexus-meta-harness-configuration",
                    harness::StoredHarness::schema_hash(),
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
            "flow_nexus_compactions" => {
                (compaction::COMPACTION_FAMILY, compaction::COMPACTION_SCHEMA)
            }
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
            "flow_nexus_meta_timings_configuration" => {
                return Some((
                    "flow-nexus-meta-timings-configuration",
                    timings_limits::StoredTimings::schema_hash(),
                ));
            }
            "flow_nexus_meta_limits_configuration" => {
                return Some((
                    "flow-nexus-meta-limits-configuration",
                    timings_limits::StoredLimits::schema_hash(),
                ));
            }
            "flow_nexus_meta_texts_configuration" => {
                return Some((
                    "flow-nexus-meta-texts-configuration",
                    texts::StoredTexts::schema_hash(),
                ));
            }
            "flow_nexus_full_configuration_state" => {
                return Some(("flow-nexus-full-configuration-state", full_configuration::StoredFullConfigurationState::schema_hash()));
            }
            "flow_nexus_full_configuration_receipts" => {
                return Some(("flow-nexus-full-configuration-receipt", full_configuration::StoredFullConfigurationReceipt::schema_hash()));
            }
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
                "flow-nexus-queue-delivery-request-v6",
            ),
            "flow_nexus_queue_delivery_attempts" => (
                "flow-nexus-queue-delivery-attempt",
                "flow-nexus-queue-delivery-attempt-v7",
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
            "flow_nexus_deliveries" => &[(
                "flow-nexus-delivery",
                delivery::StoredDeliveryV1::schema_hash(),
            )],
            "flow_nexus_send_outcomes" => &[
                (
                    send_outcome::OUTCOME_FAMILY,
                    send_outcome::StoredSendOutcomeV1::schema_hash(),
                ),
                (
                    send_outcome::OUTCOME_FAMILY,
                    send_outcome::StoredSendOutcomeV2::schema_hash(),
                ),
                (
                    send_outcome::OUTCOME_FAMILY,
                    send_outcome::StoredSendOutcomeV3::schema_hash(),
                ),
                (
                    send_outcome::OUTCOME_FAMILY,
                    send_outcome::StoredSendOutcomeV4::schema_hash(),
                ),
            ],
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
                (
                    "flow-nexus-launch-outcome",
                    StoredLaunchOutcomeV5::schema_hash(),
                ),
                (
                    "flow-nexus-launch-outcome",
                    StoredLaunchOutcomeV6::schema_hash(),
                ),
                (
                    "flow-nexus-launch-outcome",
                    StoredLaunchOutcomeV7::schema_hash(),
                ),
                (
                    "flow-nexus-launch-outcome",
                    SchemaHash::for_label("flow-nexus-launch-outcome-v8"),
                ),
            ],
            "flow_nexus_launch_attempts" => &[
                (
                    "flow-nexus-launch-attempt",
                    StoredLaunchAttemptV1::schema_hash(),
                ),
                (
                    "flow-nexus-launch-attempt",
                    StoredLaunchAttemptV2::schema_hash(),
                ),
                (
                    "flow-nexus-launch-attempt",
                    StoredLaunchAttemptV3::schema_hash(),
                ),
                (
                    "flow-nexus-launch-attempt",
                    StoredLaunchAttemptV4::schema_hash(),
                ),
            ],
            "flow_nexus_meta_metaflow_configuration" => &[
                (
                    metaflow_declaration::META_METAFLOW_CONFIGURATION_FAMILY,
                    SchemaHash::for_label("flow-nexus-meta-metaflow-configuration-v1"),
                ),
                (
                    metaflow_declaration::META_METAFLOW_CONFIGURATION_FAMILY,
                    SchemaHash::for_label("flow-nexus-meta-metaflow-configuration-v2"),
                ),
                (
                    metaflow_declaration::META_METAFLOW_CONFIGURATION_FAMILY,
                    SchemaHash::for_label("flow-nexus-meta-metaflow-configuration-v3"),
                ),
                (
                    metaflow_declaration::META_METAFLOW_CONFIGURATION_FAMILY,
                    SchemaHash::for_label("flow-nexus-meta-metaflow-configuration-v4"),
                ),
            ],
            "flow_nexus_metaflows" => &[
                ("flow-nexus-metaflow", StoredMetaflowV1::schema_hash()),
                ("flow-nexus-metaflow", StoredMetaflowV2::schema_hash()),
                ("flow-nexus-metaflow", StoredMetaflowV3::schema_hash()),
                ("flow-nexus-metaflow", StoredMetaflowV4::schema_hash()),
            ],
            "flow_nexus_metaflow_locks" => &[(
                "flow-nexus-metaflow-lock",
                StoredMetaflowLockV1::schema_hash(),
            )],
            "flow_nexus_meta_model_configuration" => &[(
                "flow-nexus-meta-model-configuration",
                StoredMetaModelConfigurationV1::schema_hash(),
            )],
            "flow_nexus_meta_harness_configuration" => &[(
                "flow-nexus-meta-harness-configuration",
                harness::StoredHarnessV1::schema_hash(),
            )],
            "flow_nexus_meta_nexus_configuration" => &[(
                nexus_configuration::META_NEXUS_CONFIGURATION_FAMILY,
                <nexus_configuration::StoredMetaNexusConfigurationV1 as MemoryDefinition>::schema_hash(),
            )],
            "flow_nexus_logical_delivery_current" => &[
                (
                    "flow-nexus-logical-delivery-current",
                    <logical_delivery::CurrentLogicalDeliveryV1 as MemoryDefinition>::schema_hash(),
                ),
                (
                    "flow-nexus-logical-delivery-current",
                    <logical_delivery::CurrentLogicalDeliveryV2 as MemoryDefinition>::schema_hash(),
                ),
                (
                    "flow-nexus-logical-delivery-current",
                    <logical_delivery::CurrentLogicalDeliveryV3 as MemoryDefinition>::schema_hash(),
                ),
                (
                    "flow-nexus-logical-delivery-current",
                    <logical_delivery::CurrentLogicalDeliveryV4 as MemoryDefinition>::schema_hash(),
                ),
                (
                    "flow-nexus-logical-delivery-current",
                    <logical_delivery::CurrentLogicalDeliveryV5 as MemoryDefinition>::schema_hash(),
                ),
            ],
            "flow_nexus_logical_delivery_events" => &[
                (
                    "flow-nexus-logical-delivery-event",
                    <logical_delivery::LogicalDeliveryEventV1 as MemoryDefinition>::schema_hash(),
                ),
                (
                    "flow-nexus-logical-delivery-event",
                    <logical_delivery::LogicalDeliveryEventV2 as MemoryDefinition>::schema_hash(),
                ),
                (
                    "flow-nexus-logical-delivery-event",
                    <logical_delivery::LogicalDeliveryEventV3 as MemoryDefinition>::schema_hash(),
                ),
            ],
            "flow_nexus_successor_intents" => &[(
                successor::SUCCESSOR_INTENT_FAMILY,
                <successor::SuccessorIntentV1 as MemoryDefinition>::schema_hash(),
            )],
            "flow_nexus_meta_leave_out_configuration" => &[(
                "flow-nexus-meta-leave-out-configuration",
                StoredMetaLeaveOutConfigurationV1::schema_hash(),
            )],
            "flow_nexus_address_holds" => &[
                (
                    address_hold::ADDRESS_HOLD_FAMILY,
                    <address_hold::AddressHoldV1 as MemoryDefinition>::schema_hash(),
                ),
                (
                    address_hold::ADDRESS_HOLD_FAMILY,
                    <address_hold::AddressHoldV2 as MemoryDefinition>::schema_hash(),
                ),
                (
                    address_hold::ADDRESS_HOLD_FAMILY,
                    <address_hold::AddressHoldV3 as MemoryDefinition>::schema_hash(),
                ),
            ],
            "flow_nexus_send_calls" => &[
                (
                    send_call::SEND_CALL_FAMILY,
                    <send_call::SendCallV1 as MemoryDefinition>::schema_hash(),
                ),
                (
                    send_call::SEND_CALL_FAMILY,
                    <send_call::SendCallV2 as MemoryDefinition>::schema_hash(),
                ),
                (
                    send_call::SEND_CALL_FAMILY,
                    <send_call::SendCallV3 as MemoryDefinition>::schema_hash(),
                ),
                (
                    send_call::SEND_CALL_FAMILY,
                    <send_call::SendCallV4 as MemoryDefinition>::schema_hash(),
                ),
                (
                    send_call::SEND_CALL_FAMILY,
                    <send_call::SendCallV5 as MemoryDefinition>::schema_hash(),
                ),
                (
                    send_call::SEND_CALL_FAMILY,
                    <send_call::SendCallV6 as MemoryDefinition>::schema_hash(),
                ),
            ],
            "flow_nexus_queue_delivery_requests" => &[
                (
                    "flow-nexus-queue-delivery-request",
                    <queue_delivery::StoredDeliveryRequestV2 as MemoryDefinition>::schema_hash(),
                ),
                (
                    "flow-nexus-queue-delivery-request",
                    <queue_delivery::StoredDeliveryRequestV3 as MemoryDefinition>::schema_hash(),
                ),
                (
                    "flow-nexus-queue-delivery-request",
                    <queue_delivery::StoredDeliveryRequestV4 as MemoryDefinition>::schema_hash(),
                ),
                (
                    "flow-nexus-queue-delivery-request",
                    <queue_delivery::StoredDeliveryRequestV5 as MemoryDefinition>::schema_hash(),
                ),
            ],
            "flow_nexus_queue_delivery_attempts" => &[
                (
                    "flow-nexus-queue-delivery-attempt",
                    <queue_delivery::QueueDeliveryAttemptV4 as MemoryDefinition>::schema_hash(),
                ),
                (
                    "flow-nexus-queue-delivery-attempt",
                    <queue_delivery::QueueDeliveryAttemptV5 as MemoryDefinition>::schema_hash(),
                ),
                (
                    "flow-nexus-queue-delivery-attempt",
                    <queue_delivery::QueueDeliveryAttemptV6 as MemoryDefinition>::schema_hash(),
                ),
            ],
            "flow_nexus_flow_events" => &[
                (
                    events::FLOW_EVENT_FAMILY,
                    <events::FlowEventsV1 as MemoryDefinition>::schema_hash(),
                ),
                (
                    events::FLOW_EVENT_FAMILY,
                    <events::FlowEventsV2 as MemoryDefinition>::schema_hash(),
                ),
                (
                    events::FLOW_EVENT_FAMILY,
                    <events::FlowEventsV3 as MemoryDefinition>::schema_hash(),
                ),
            ],
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
        use redb::{ReadableDatabase as _, ReadableTable as _};
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

    /// Before the legacy leave-out family is retired, carry its behavior
    /// names into the declaration of the same Address. This runs on the
    /// staged store generation, so the merge and the later table retirement
    /// become visible together only when the staged generation activates.
    fn migrate_retired_leave_outs(path: &Path) -> Result<(), StoreError> {
        use redb::{ReadableDatabase as _, ReadableTable as _};
        let database = redb::Database::open(path)
            .map_err(|error| StoreError::LeaveOutUpgradeFailed(error.to_string()))?;
        let read = database
            .begin_read()
            .map_err(|error| StoreError::LeaveOutUpgradeFailed(error.to_string()))?;
        let catalog = read
            .open_table(SEMA_CATALOG_TABLE)
            .map_err(|error| StoreError::LeaveOutUpgradeFailed(error.to_string()))?;
        let present = catalog
            .get(META_LEAVE_OUT_CONFIGURATION_TABLE_NAME.as_str())
            .map_err(|error| StoreError::LeaveOutUpgradeFailed(error.to_string()))?
            .is_some();
        drop(catalog);
        drop(read);
        drop(database);
        if !present {
            return Ok(());
        }

        let leave_out_descriptor = StoredMetaLeaveOutConfiguration::descriptor();
        let metaflow_descriptor = <metaflow_declaration::StoredMetaMetaflowConfiguration as metaflow_declaration::DescribesMetaflowDeclarationTable>::descriptor();
        let mut engine = Engine::open(
            EngineOpen::new(path, SchemaVersion::new(FLOW_SCHEMA_VERSION))
                .with_family(&leave_out_descriptor)
                .with_family(&metaflow_descriptor),
        )?;
        let leave_outs = engine.upgraded_leave_out_defaults(leave_out_descriptor)?;
        let metaflows = engine.upgraded_metaflow_declarations(metaflow_descriptor)?;
        let defaults = engine
            .match_records(QueryPlan::all(leave_outs))?
            .records()
            .to_vec();
        let declarations = engine
            .match_records(QueryPlan::all(metaflows))?
            .records()
            .to_vec();
        let mut commit = engine.begin_atomic_commit();
        let mut changed = false;
        for mut declaration in declarations {
            let mut declaration_changed = false;
            for default in defaults
                .iter()
                .filter(|default| default.leave_out.address == declaration.metaflow.address)
            {
                for name in default
                    .leave_out
                    .left_out
                    .iter()
                    .flat_map(LeaveOutV37::module_names)
                {
                    if !declaration
                        .metaflow
                        .left_out
                        .iter()
                        .any(|known| known == name)
                    {
                        declaration.metaflow.left_out.push((*name).to_owned());
                        changed = true;
                        declaration_changed = true;
                    }
                }
            }
            if declaration_changed {
                commit = commit.mutate(metaflows, declaration);
            }
        }
        if changed {
            engine.commit_atomic(commit)?;
        }
        Ok(())
    }

    fn has_expected_flow_catalog(actual: &std::collections::BTreeSet<String>) -> bool {
        // A store before 0.35.0 may still hold a table 0.35.0 drops.
        let retired = |name: &String| RETIRED_TABLES.iter().any(|(table, _, _)| table == name);
        let running = RUNNING_TABLES.iter().all(|name| actual.contains(*name))
            && actual.iter().all(|name| {
                RUNNING_TABLES.contains(&name.as_str())
                    || RUNNING_ADDED_TABLES.contains(&name.as_str())
                    || ADDED_LATER_TABLES.contains(&name.as_str())
                    || retired(name)
            });
        let configuration_only = CONFIGURATION_ONLY_TABLES
            .iter()
            .all(|name| actual.contains(*name))
            && actual.iter().all(|name| {
                CONFIGURATION_ONLY_TABLES.contains(&name.as_str())
                    || CONFIGURATION_ONLY_ADDED_TABLES.contains(&name.as_str())
                    || name == "flow_nexus_queue_delivery_counter"
                    || name == "flow_nexus_queue_delivery_requests"
                    || name == "flow_nexus_queue_delivery_attempts"
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
            Err(redb::DatabaseError::DatabaseAlreadyOpen) => Err(StoreError::StoreHeld {
                path: path.display().to_string(),
            }),
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
    ) -> Result<Option<StoredConfigurationSnapshot>, StoreError> {
        Ok(Self::meta_committed_configuration_snapshot(engine, tables)?
            .map(|committed| committed.snapshot))
    }

    fn meta_committed_configuration_snapshot(
        engine: &Engine,
        tables: &MetaConfigurationTables,
    ) -> Result<Option<CommittedConfigurationSnapshot>, StoreError> {
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
        let harness_rows = engine.match_records(QueryPlan::all(tables.harnesses))?;
        let thresholds_rows = engine.match_records(QueryPlan::all(tables.thresholds))?;
        let send_server_rows = engine.match_records(QueryPlan::all(tables.send_servers))?;
        let investigation_rows = engine.match_records(QueryPlan::all(tables.investigation))?;
        let metaflow_rows = engine.match_records(QueryPlan::all(tables.metaflows))?;
        let timings_rows = engine.match_records(QueryPlan::all(tables.timings))?;
        let limits_rows = engine.match_records(QueryPlan::all(tables.limits))?;
        let texts_rows = engine.match_records(QueryPlan::all(tables.texts))?;
        if models_rows.database_marker() != marker
            || harness_rows.database_marker() != marker
            || metaflow_rows.database_marker() != marker
            || thresholds_rows.database_marker() != marker
            || send_server_rows.database_marker() != marker
            || investigation_rows.database_marker() != marker
            || timings_rows.database_marker() != marker
            || limits_rows.database_marker() != marker
            || texts_rows.database_marker() != marker
            || engine.current_database_marker()? != marker
        {
            return Err(StoreError::ConfigurationReadViewChanged);
        }
        let Some(nexus) = nexus else {
            return Ok(None);
        };
        let timings = match timings_rows.records() {
            [stored] if stored.record_key() == RecordKey::new("timings") => stored.timings.clone(),
            _ => return Err(StoreError::StateInvariant),
        };
        let limits = match limits_rows.records() {
            [stored] if stored.record_key() == RecordKey::new("limits") => stored.limits.clone(),
            _ => return Err(StoreError::StateInvariant),
        };
        let mut models = models_rows
            .records()
            .iter()
            .map(|stored| stored.model.clone())
            .collect::<Vec<_>>();
        models.sort_by_key(|model| FlowStore::model_record_key(&model.layer, &model.aspect));
        let mut harnesses = harness_rows
            .records()
            .iter()
            .map(|stored| stored.harness.clone())
            .collect::<Vec<_>>();
        harnesses.sort_by(|left, right| left.harness_name.cmp(&right.harness_name));
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
        let texts = match texts_rows.records() {
            [stored] if stored.record_key() == RecordKey::new("texts") => stored.texts.clone(),
            _ => return Err(StoreError::StateInvariant),
        };
        Ok(Some(CommittedConfigurationSnapshot {
            generation: marker,
            snapshot: StoredConfigurationSnapshot {
                configured_nexus: nexus,
                configured_harnesses: harnesses,
                configured_models: models,
                configured_thresholds: thresholds,
                configured_send_servers: send_servers,
                configured_metaflows: metaflows,
                configured_investigation: investigation,
                timings,
                limits,
                texts,
            },
        }))
    }

    fn configure_meta_entry(
        engine: &Engine,
        tables: &MetaConfigurationTables,
        entry: ConfigurationEntry,
    ) -> Result<ConfigureOutcome, StoreError> {
        let snapshot = FlowStore::meta_configuration_snapshot(engine, tables)?
            .ok_or(StoreError::NotConfigured)?;
        match entry {
            ConfigurationEntry::ModelEntry(entry) => {
                let mut rows = snapshot.configured_models;
                match rows
                    .iter()
                    .position(|row| row.layer == entry.layer && row.aspect == entry.aspect)
                {
                    Some(index) => rows[index] = entry,
                    None => rows.push(entry),
                }
                FlowStore::configure_meta_change(engine, tables, ConfigurationChange::Models(rows))
            }
            ConfigurationEntry::ThresholdEntry(entry) => {
                let mut rows = snapshot.configured_thresholds;
                match rows
                    .iter()
                    .position(|row| row.model_name == entry.model_name)
                {
                    Some(index) => rows[index] = entry,
                    None => rows.push(entry),
                }
                FlowStore::configure_meta_change(
                    engine,
                    tables,
                    ConfigurationChange::Thresholds(rows),
                )
            }
            ConfigurationEntry::HarnessEntry(entry) => {
                let mut rows = snapshot.configured_harnesses;
                match rows
                    .iter()
                    .position(|row| row.harness_name == entry.harness_name)
                {
                    Some(index) => rows[index] = entry,
                    None => rows.push(entry),
                }
                FlowStore::configure_meta_change(
                    engine,
                    tables,
                    ConfigurationChange::Harnesses(rows),
                )
            }
            ConfigurationEntry::SendServerEntry(entry) => {
                let mut rows = snapshot.configured_send_servers;
                match rows
                    .iter()
                    .position(|row| row.codex_app_server_binary == entry.codex_app_server_binary)
                {
                    Some(index) => rows[index] = entry,
                    None => rows.push(entry),
                }
                FlowStore::configure_meta_change(
                    engine,
                    tables,
                    ConfigurationChange::SendServers(rows),
                )
            }
            ConfigurationEntry::MetaflowEntry(entry) => {
                let mut rows = snapshot.configured_metaflows;
                match rows.iter().position(|row| row.address == entry.address) {
                    Some(index) => rows[index] = entry,
                    None => rows.push(entry),
                }
                FlowStore::configure_meta_change(
                    engine,
                    tables,
                    ConfigurationChange::Metaflows(rows),
                )
            }
        }
    }

    fn remove_meta_entry(
        engine: &Engine,
        tables: &MetaConfigurationTables,
        key: EntryKey,
    ) -> Result<ConfigureOutcome, StoreError> {
        let snapshot = FlowStore::meta_configuration_snapshot(engine, tables)?
            .ok_or(StoreError::NotConfigured)?;
        let outcome = match key {
            EntryKey::ModelKey(key) => {
                let mut rows = snapshot.configured_models;
                let before = rows.len();
                rows.retain(|row| row.layer != key.layer || row.aspect != key.aspect);
                if rows.len() == before {
                    return Err(StoreError::UnknownConfigurationEntry);
                }
                FlowStore::configure_meta_change(engine, tables, ConfigurationChange::Models(rows))?
            }
            EntryKey::ThresholdKey(key) => {
                let mut rows = snapshot.configured_thresholds;
                let before = rows.len();
                rows.retain(|row| row.model_name != key);
                if rows.len() == before {
                    return Err(StoreError::UnknownConfigurationEntry);
                }
                FlowStore::configure_meta_change(
                    engine,
                    tables,
                    ConfigurationChange::Thresholds(rows),
                )?
            }
            EntryKey::HarnessKey(key) => {
                let mut rows = snapshot.configured_harnesses;
                let before = rows.len();
                rows.retain(|row| row.harness_name != key);
                if rows.len() == before {
                    return Err(StoreError::UnknownConfigurationEntry);
                }
                FlowStore::configure_meta_change(
                    engine,
                    tables,
                    ConfigurationChange::Harnesses(rows),
                )?
            }
            EntryKey::SendServerKey(key) => {
                let mut rows = snapshot.configured_send_servers;
                let before = rows.len();
                rows.retain(|row| row.codex_app_server_binary != key);
                if rows.len() == before {
                    return Err(StoreError::UnknownConfigurationEntry);
                }
                FlowStore::configure_meta_change(
                    engine,
                    tables,
                    ConfigurationChange::SendServers(rows),
                )?
            }
            EntryKey::MetaflowKey(key) => {
                let mut rows = snapshot.configured_metaflows;
                let before = rows.len();
                rows.retain(|row| row.address != key);
                if rows.len() == before {
                    return Err(StoreError::UnknownConfigurationEntry);
                }
                FlowStore::configure_meta_change(
                    engine,
                    tables,
                    ConfigurationChange::Metaflows(rows),
                )?
            }
        };
        match outcome {
            ConfigureOutcome::Replaced | ConfigureOutcome::Removed => Ok(ConfigureOutcome::Removed),
            ConfigureOutcome::Unchanged => Err(StoreError::StateInvariant),
        }
    }

    fn meta_texts(
        engine: &Engine,
        tables: &MetaConfigurationTables,
    ) -> Result<meta_signal_flow::Texts, StoreError> {
        let rows = engine.match_records(QueryPlan::all(tables.texts))?;
        match rows.records() {
            [stored] if stored.record_key() == RecordKey::new("texts") => Ok(stored.texts.clone()),
            _ => Err(StoreError::StateInvariant),
        }
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

    fn append_configuration_family<T>(
        engine: &Engine,
        table: TableReference<T>,
        declared: Vec<T>,
        mut commit: AtomicCommit,
    ) -> Result<(AtomicCommit, bool), StoreError>
    where
        T: sema_engine::EngineStoredRecord + PartialEq + Clone + Send + Sync + 'static,
        <T as rkyv::Archive>::Archived: rkyv::Deserialize<T, rkyv::api::high::HighDeserializer<rkyv::rancor::Error>>
            + for<'validation> rkyv::bytecheck::CheckBytes<rkyv::rancor::Strategy<rkyv::validation::Validator<rkyv::validation::archive::ArchiveValidator<'validation>, rkyv::validation::shared::SharedValidator>, rkyv::rancor::Error>>,
    {
        let keys = declared.iter().map(EngineRecord::record_key).collect::<Vec<_>>();
        if keys.iter().enumerate().any(|(i, key)| keys[..i].contains(key)) { return Err(StoreError::DuplicateConfigurationEntry); }
        let stored = engine.match_records(QueryPlan::all(table))?.records().to_vec();
        let mut changed = false;
        for row in &stored { if !keys.contains(&row.record_key()) { commit = commit.retract(table, row.record_key()); changed = true; } }
        for row in declared {
            match stored.iter().find(|old| old.record_key() == row.record_key()) {
                Some(old) if old == &row => {}
                Some(_) => { commit = commit.mutate(table, row); changed = true; }
                None => { commit = commit.assert(table, row); changed = true; }
            }
        }
        Ok((commit, changed))
    }

    fn configure_full(
        engine: &Engine,
        tables: &MetaConfigurationTables,
        request: meta_signal_flow::FullConfigurationRequest,
    ) -> Result<meta_signal_flow::FullConfigurationReceipt, StoreError> {
        use meta_signal_flow::{FullConfigurationDisposition as Disposition, Historical};
        let existing = engine.match_records(QueryPlan::key(
            tables.full_receipts,
            RecordKey::new(&request.configuration_request_id),
        ))?;
        if let [stored] = existing.records() {
            if stored.request == request {
                let mut receipt = stored.receipt.clone();
                receipt.historical = Historical::Replay;
                return Ok(receipt);
            }
            return Ok(meta_signal_flow::FullConfigurationReceipt {
                full_configuration_request: request,
                observed_revision: stored.receipt.resulting_revision,
                resulting_revision: stored.receipt.resulting_revision,
                full_configuration_disposition: Disposition::IdentifierConflict,
                historical: Historical::Replay,
            });
        } else if !existing.records().is_empty() { return Err(StoreError::StateInvariant); }
        let state = engine.match_records(QueryPlan::all(tables.full_state))?;
        let [state] = state.records() else { return Err(StoreError::StateInvariant) };
        let observed = state.revision;
        let candidate = &request.full_configuration;
        let current = FlowStore::meta_configuration_snapshot(engine, tables)?.ok_or(StoreError::NotConfigured)?;
        let equal = current.configured_harnesses == candidate.harnesses
            && current.configured_models == candidate.models
            && current.configured_thresholds == candidate.thresholds
            && current.configured_nexus == candidate.nexus
            && current.configured_send_servers == candidate.send_servers
            && current.configured_metaflows == candidate.metaflows
            && current.configured_investigation == candidate.investigation
            && current.timings == candidate.timings && current.limits == candidate.limits && current.texts == candidate.texts;
        let (disposition, resulting) = if request.configuration_revision != observed {
            (Disposition::Stale(observed), observed)
        } else if equal { (Disposition::Unchanged, observed) } else { (Disposition::Replaced, observed + 1) };
        let receipt = meta_signal_flow::FullConfigurationReceipt {
            full_configuration_request: request.clone(), observed_revision: observed,
            resulting_revision: resulting, full_configuration_disposition: disposition.clone(), historical: Historical::New,
        };
        let mut commit = engine.begin_atomic_commit();
        if matches!(disposition, Disposition::Replaced) {
            macro_rules! family { ($table:expr, $rows:expr) => {{ let next = FlowStore::append_configuration_family(engine, $table, $rows, commit)?; commit = next.0; }} }
            family!(tables.harnesses, candidate.harnesses.iter().cloned().map(|harness| harness::StoredHarness { harness }).collect());
            family!(tables.models, candidate.models.iter().cloned().map(|model| StoredMetaModelConfiguration { model }).collect());
            family!(tables.thresholds, candidate.thresholds.iter().cloned().map(|threshold| StoredMetaThresholdConfiguration { threshold }).collect());
            family!(tables.nexus, vec![StoredMetaNexusConfiguration { nexus: candidate.nexus.clone() }]);
            family!(tables.send_servers, candidate.send_servers.iter().cloned().map(|send_server| send_server::StoredMetaSendServerConfiguration { send_server }).collect());
            family!(tables.metaflows, candidate.metaflows.iter().cloned().map(|metaflow| metaflow_declaration::StoredMetaMetaflowConfiguration { metaflow }).collect());
            family!(tables.investigation, candidate.investigation.iter().cloned().map(|investigation| StoredMetaInvestigationConfiguration { investigation }).collect());
            family!(tables.timings, vec![timings_limits::StoredTimings { timings: candidate.timings.clone() }]);
            family!(tables.limits, vec![timings_limits::StoredLimits { limits: candidate.limits.clone() }]);
            family!(tables.texts, vec![texts::StoredTexts { texts: candidate.texts.clone() }]);
            commit = commit.mutate(tables.full_state, full_configuration::StoredFullConfigurationState { revision: resulting });
        }
        commit = commit.assert(tables.full_receipts, full_configuration::StoredFullConfigurationReceipt { request, receipt: receipt.clone() });
        engine.commit_atomic(commit)?;
        Ok(receipt)
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
            ConfigurationChange::Metaflows(entries) => {
                entries.iter().map(|entry| &entry.address).collect()
            }
            ConfigurationChange::Investigation(address) => address.iter().collect(),
            _ => Vec::new(),
        };
        if let Some(refused) = addresses
            .into_iter()
            .find(|address| !address.topic.as_str().is_camel_case_topic())
        {
            return Err(StoreError::TopicRefused(refused.topic.clone()));
        }
        match change {
            ConfigurationChange::Harnesses(harnesses) => {
                let mut models = std::collections::BTreeSet::new();
                for model in harnesses
                    .iter()
                    .flat_map(|harness| harness.served_models.iter())
                {
                    if !models.insert(model.model_name.clone()) {
                        return Err(StoreError::ModelServedTwice(model.model_name.clone()));
                    }
                }
                FlowStore::replace_configuration_family(
                    engine,
                    tables.harnesses,
                    harnesses
                        .into_iter()
                        .map(|harness| harness::StoredHarness { harness })
                        .collect(),
                )
            }
            ConfigurationChange::Nexus(nexus) => FlowStore::replace_configuration_family(
                engine,
                tables.nexus,
                vec![StoredMetaNexusConfiguration { nexus }],
            ),
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
            ConfigurationChange::Metaflows(metaflows) => {
                let configured = engine
                    .match_records(QueryPlan::all(tables.harnesses))?
                    .records()
                    .iter()
                    .map(|stored| stored.harness.clone())
                    .collect::<Vec<_>>();
                for name in metaflows
                    .iter()
                    .flat_map(|metaflow| metaflow.left_out.iter())
                {
                    let modules = configured
                        .iter()
                        .flat_map(|harness| harness.modules.iter())
                        .filter(|module| &module.harness_module_name == name)
                        .collect::<Vec<_>>();
                    if modules.is_empty() {
                        return Err(StoreError::UnknownHarnessModule(name.clone()));
                    }
                    if modules.iter().all(|module| {
                        matches!(
                            module.module_text,
                            meta_signal_flow::ModuleText::Retained(_)
                                | meta_signal_flow::ModuleText::Absent
                        ) && module.tools.is_empty()
                            && module.switches.is_empty()
                    }) {
                        return Err(StoreError::IrremovableHarnessModule(name.clone()));
                    }
                }
                FlowStore::replace_configuration_family(
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
                    investigation
                        .into_iter()
                        .map(|investigation| StoredMetaInvestigationConfiguration { investigation })
                        .collect(),
                )
            }
            ConfigurationChange::Timings(timings) => {
                if !timings_limits::SupportedNativeTimings::supported(&timings) {
                    return Err(StoreError::TimingOutOfRange);
                }
                FlowStore::replace_configuration_family(
                    engine,
                    tables.timings,
                    vec![timings_limits::StoredTimings { timings }],
                )
            }
            ConfigurationChange::Limits(limits) => {
                // A cohort is one canonical vector carried as one native
                // submission. Its configured ceiling must fit every native
                // transport it can reach; one message retains the distinct
                // 64 KiB admission ceiling.
                let native_ceiling = limits
                    .herdr_line
                    .min(crate::sending::MAX_NATIVE_COHORT_BYTES as i64);
                let codex_bound = limits.cohort.saturating_mul(2).saturating_add(4096);
                if limits.send_body != 64 * 1024
                    || limits.cohort <= 0
                    || limits.cohort > native_ceiling
                    || limits.herdr_envelope < crate::sending::HERDR_PROMPT_FRAME_BYTES as i64
                    || limits.cohort.saturating_add(crate::sending::HERDR_PROMPT_FRAME_BYTES as i64) > limits.herdr_line
                    || codex_bound > limits.frame
                    || codex_bound > limits.app_server_message
                {
                    return Err(StoreError::ConfigurationSeed(
                        "Limits must keep SendBody at 64 KiB and Cohort within every native transport ceiling".into(),
                    ));
                }
                FlowStore::replace_configuration_family(
                    engine,
                    tables.limits,
                    vec![timings_limits::StoredLimits { limits }],
                )
            }
            ConfigurationChange::Texts(texts) => FlowStore::replace_configuration_family(
                engine,
                tables.texts,
                vec![texts::StoredTexts { texts }],
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
    use super::{LeaveOutV37, ReadsCreatedLaunchPane, RecordsCreatedLaunchPane};
    use crate::store::queue_delivery::AccountsQueueDelivery;
    use std::fs;
    use std::os::unix::fs::MetadataExt;

    fn production_leave_out_defaults() -> Vec<(signal_flow::Address, Vec<LeaveOutV37>)> {
        use signal_flow::{Address, Aspect, Layer};
        let claude = vec![
            LeaveOutV37::ClaudeAiConnectors,
            LeaveOutV37::ExplorePlanAgents,
            LeaveOutV37::WorkflowTool,
            LeaveOutV37::SkillTool,
            LeaveOutV37::BundledSkills,
            LeaveOutV37::ScheduleWakeupTool,
            LeaveOutV37::ReportFindingsTool,
            LeaveOutV37::ListAgentsTool,
            LeaveOutV37::CronTools,
            LeaveOutV37::DeferredTools,
            LeaveOutV37::ClaudeMd,
            LeaveOutV37::GitInstructions,
        ];
        let codex = vec![
            LeaveOutV37::CodexAgentsMd,
            LeaveOutV37::CodexSkillListing,
            LeaveOutV37::CodexCollaborationMode,
            LeaveOutV37::CodexEnvironmentContext,
            LeaveOutV37::CodexPermissions,
            LeaveOutV37::CodexDeveloperInstructions,
        ];
        [
            (Aspect::Field, "illustrator", Layer::Tertiary, &claude),
            (Aspect::Field, "core", Layer::Primary, &codex),
            (Aspect::Field, "publisher", Layer::Quaternary, &claude),
            (Aspect::Mind, "prototyping", Layer::Primary, &codex),
            (Aspect::Mind, "prototyping", Layer::Secondary, &codex),
            (Aspect::Mind, "core", Layer::Primary, &codex),
            (Aspect::Mind, "core", Layer::Secondary, &codex),
            (Aspect::Psyche, "communication", Layer::Primary, &claude),
            (Aspect::Psyche, "communication", Layer::Secondary, &claude),
            (Aspect::Psyche, "core", Layer::Primary, &claude),
            (Aspect::Psyche, "core", Layer::Secondary, &claude),
            (Aspect::Psyche, "core", Layer::Tertiary, &claude),
            (Aspect::Psyche, "flow", Layer::Primary, &claude),
            (Aspect::Psyche, "flow", Layer::Secondary, &claude),
            (Aspect::Psyche, "ethos", Layer::Primary, &claude),
            (Aspect::Psyche, "ethos", Layer::Secondary, &claude),
            (Aspect::Psyche, "nexus", Layer::Secondary, &claude),
            (Aspect::Psyche, "spirit", Layer::Primary, &claude),
            (Aspect::Psyche, "spirit", Layer::Secondary, &claude),
            (Aspect::Psyche, "chronos", Layer::Primary, &claude),
            (Aspect::Psyche, "chronos", Layer::Secondary, &claude),
        ]
        .into_iter()
        .map(|(aspect, topic, layer, values)| {
            (
                Address {
                    aspect,
                    topic: topic.into(),
                    layer,
                },
                values.clone(),
            )
        })
        .collect()
    }

    #[test]
    fn immediately_previous_catalog_upgrades_and_reopens_with_records() {
        use super::events::{EventV2, FlowEventsV2, NativeStopV2};
        use super::metaflow_declaration::{
            ConfigureMetaflowV1, ReadsMetaflowDeclarations, StoredMetaMetaflowConfigurationV1,
        };
        use super::{MemoryDefinition, StoredLaunchAttemptV2};
        let fixture = StoreFixture::new();
        fixture.configured();
        drop(fixture.configured_store());
        let tables = [
            "flow_nexus_launch_attempts",
            "flow_nexus_flow_events",
            "flow_nexus_meta_metaflow_configuration",
        ];
        let database = redb::Database::open(fixture.path()).unwrap();
        let write = database.begin_write().unwrap();
        {
            let mut catalog = write.open_table(super::SEMA_CATALOG_TABLE).unwrap();
            for name in tables {
                catalog.remove(name).unwrap();
            }
        }
        for name in tables {
            write
                .delete_table(redb::TableDefinition::<&str, &[u8]>::new(name))
                .unwrap();
        }
        write.commit().unwrap();
        drop(database);
        let mut engine = Engine::open(EngineOpen::new(
            fixture.path(),
            SchemaVersion::new(super::FLOW_SCHEMA_VERSION),
        ))
        .unwrap();
        engine
            .register_table::<StoredLaunchAttemptV2>(TableDescriptor::new(
                TableName::new(tables[0]),
                FamilyName::new("flow-nexus-launch-attempt"),
                StoredLaunchAttemptV2::schema_hash(),
            ))
            .unwrap();
        let events = engine
            .register_table::<FlowEventsV2>(TableDescriptor::new(
                TableName::new(tables[1]),
                FamilyName::new("flow-nexus-flow-events"),
                FlowEventsV2::schema_hash(),
            ))
            .unwrap();
        engine
            .assert(Assertion::new(
                events,
                FlowEventsV2 {
                    flow_id: "old-flow".into(),
                    event_vector: vec![
                        EventV2::Started,
                        EventV2::Stopped(NativeStopV2 {
                            native_session_id_option: Some("old-session".into()),
                            transcript_path_option: Some("/old/transcript".into()),
                        }),
                    ],
                },
            ))
            .unwrap();
        let declarations = engine
            .register_table::<StoredMetaMetaflowConfigurationV1>(TableDescriptor::new(
                TableName::new(tables[2]),
                FamilyName::new("flow-nexus-meta-metaflow-configuration"),
                StoredMetaMetaflowConfigurationV1::schema_hash(),
            ))
            .unwrap();
        let address = Address {
            aspect: Aspect::Psyche,
            topic: "judgment".into(),
            layer: Layer::Primary,
        };
        engine
            .assert(Assertion::new(
                declarations,
                StoredMetaMetaflowConfigurationV1 {
                    metaflow: ConfigureMetaflowV1 {
                        address: address.clone(),
                        system_modules: vec!["system".into()],
                        user_modules: vec!["user".into()],
                    },
                },
            ))
            .unwrap();
        drop(engine);
        for _ in 0..2 {
            let store = fixture.configured_store();
            let declaration = store.metaflow_declaration(&address).unwrap().unwrap();
            assert_eq!(declaration.system_modules, ["system"]);
            assert_eq!(declaration.user_modules, ["user"]);
            assert_eq!(declaration.tool_set, signal_flow::ToolSet::Default);
            let events = super::events::RecordsHarnessEvents::events(&store, "old-flow")
                .unwrap()
                .unwrap();
            assert_eq!(events.len(), 2);
            assert!(
                matches!(&events[1], signal_flow::Event::Stopped(stop) if stop.native_session_id_option.as_deref() == Some("old-session") && stop.last_assistant_message_option.is_none())
            );
        }
    }

    /// A store written before Continuity, before a session's end was an
    /// event, before a successor could be owed for an ordered Refresh or a
    /// Replace, and before a queued request could be withdrawn, opens with
    /// every one of those families upgraded, and opens again unchanged: its
    /// declarations Maintained at the core Addresses alone, its events,
    /// successors, deliveries and send calls as they were kept.
    #[test]
    fn a_store_before_continuity_and_withdrawal_upgrades_and_reopens() {
        use super::MemoryDefinition;
        use super::events::{EventV3, FlowEventsV3};
        use super::logical_delivery::{CurrentLogicalDeliveryV1, LogicalDeliveryEventV1};
        use super::metaflow_declaration::{
            AvailabilityV3, ConfigureMetaflowV3, ReadsMetaflowDeclarations,
            StoredMetaMetaflowConfigurationV3,
        };
        use super::send_call::{AcceptedV4, SendCallV4, SendOutcomeV4};
        use super::send_v31::{
            DeliverySnapshotV31, DeliveryStateV31, RefusalV31, SendAcceptanceV31,
        };
        use super::successor::{
            OwesSuccessors, SuccessionCause, SuccessionCauseV1, SuccessorIntent, SuccessorIntentV1,
        };
        use crate::store::logical_delivery::RecordsLogicalDeliveries;
        let fixture = StoreFixture::new();
        fixture.configured();
        drop(fixture.configured_store());
        let tables = [
            "flow_nexus_flow_events",
            "flow_nexus_successor_intents",
            "flow_nexus_logical_delivery_current",
            "flow_nexus_logical_delivery_events",
            "flow_nexus_send_calls",
            "flow_nexus_meta_metaflow_configuration",
        ];
        let database = redb::Database::open(fixture.path()).unwrap();
        let write = database.begin_write().unwrap();
        {
            let mut catalog = write.open_table(super::SEMA_CATALOG_TABLE).unwrap();
            for name in tables {
                catalog.remove(name).unwrap();
            }
        }
        for name in tables {
            write
                .delete_table(redb::TableDefinition::<&str, &[u8]>::new(name))
                .unwrap();
        }
        write.commit().unwrap();
        drop(database);
        let core = Address {
            aspect: Aspect::Mind,
            topic: "core".into(),
            layer: Layer::Secondary,
        };
        let elsewhere = Address {
            aspect: Aspect::Mind,
            topic: "flow".into(),
            layer: Layer::Primary,
        };
        let snapshot = DeliverySnapshotV31 {
            delivery_cursor: 7,
            logical_delivery_id: "logical-old".into(),
            sent_by: signal_flow::SentBy::Sender(elsewhere.clone()),
            address: core.clone(),
            delivery_state: DeliveryStateV31::Failed(RefusalV31::Direct("gone".into())),
        };
        let key = crate::generated::send_tool::ToolCallKey {
            thread_id: "thread-old".into(),
            turn_id: "turn-old".into(),
            call_id: "call-old".into(),
        };
        let mut engine = Engine::open(EngineOpen::new(
            fixture.path(),
            SchemaVersion::new(super::FLOW_SCHEMA_VERSION),
        ))
        .unwrap();
        let events = engine
            .register_table::<FlowEventsV3>(TableDescriptor::new(
                TableName::new(tables[0]),
                FamilyName::new("flow-nexus-flow-events"),
                FlowEventsV3::schema_hash(),
            ))
            .unwrap();
        engine
            .assert(Assertion::new(
                events,
                FlowEventsV3 {
                    flow_id: "old-flow".into(),
                    event_vector: vec![EventV3::Started, EventV3::ToolUsed("Bash".into())],
                },
            ))
            .unwrap();
        let successors = engine
            .register_table::<SuccessorIntentV1>(TableDescriptor::new(
                TableName::new(tables[1]),
                FamilyName::new("flow-nexus-successor-intent"),
                SuccessorIntentV1::schema_hash(),
            ))
            .unwrap();
        engine
            .assert(Assertion::new(
                successors,
                SuccessorIntentV1 {
                    address: core.clone(),
                    predecessor: "old-flow".into(),
                    cause: SuccessionCauseV1::Exit,
                    launch_request_id: String::new(),
                },
            ))
            .unwrap();
        let current = engine
            .register_table::<CurrentLogicalDeliveryV1>(TableDescriptor::new(
                TableName::new(tables[2]),
                FamilyName::new("flow-nexus-logical-delivery-current"),
                CurrentLogicalDeliveryV1::schema_hash(),
            ))
            .unwrap();
        engine
            .assert(Assertion::new(
                current,
                CurrentLogicalDeliveryV1 {
                    snapshot: snapshot.clone(),
                },
            ))
            .unwrap();
        let delivery_events = engine
            .register_table::<LogicalDeliveryEventV1>(TableDescriptor::new(
                TableName::new(tables[3]),
                FamilyName::new("flow-nexus-logical-delivery-event"),
                LogicalDeliveryEventV1::schema_hash(),
            ))
            .unwrap();
        engine
            .assert(Assertion::new(
                delivery_events,
                LogicalDeliveryEventV1 {
                    snapshot: snapshot.clone(),
                },
            ))
            .unwrap();
        let calls = engine
            .register_table::<SendCallV4>(TableDescriptor::new(
                TableName::new(tables[4]),
                FamilyName::new("flow-nexus-send-call"),
                SendCallV4::schema_hash(),
            ))
            .unwrap();
        engine
            .assert(Assertion::new(
                calls,
                SendCallV4 {
                    tool_call_key: key.clone(),
                    content_sha256: "sha-old".into(),
                    send_outcome: SendOutcomeV4::Accepted(AcceptedV4 {
                        sender: elsewhere.clone(),
                        target: core.clone(),
                        send_acceptance: SendAcceptanceV31 {
                            logical_delivery_id: "logical-old".into(),
                            delivery_cursor: 7,
                            delivery_state: DeliveryStateV31::Queued,
                        },
                    }),
                },
            ))
            .unwrap();
        let declarations = engine
            .register_table::<StoredMetaMetaflowConfigurationV3>(TableDescriptor::new(
                TableName::new(tables[5]),
                FamilyName::new("flow-nexus-meta-metaflow-configuration"),
                StoredMetaMetaflowConfigurationV3::schema_hash(),
            ))
            .unwrap();
        for (address, availability) in [
            (core.clone(), AvailabilityV3::OnDemand),
            (elsewhere.clone(), AvailabilityV3::Required),
        ] {
            engine
                .assert(Assertion::new(
                    declarations,
                    StoredMetaMetaflowConfigurationV3 {
                        metaflow: ConfigureMetaflowV3 {
                            address,
                            system_modules: vec!["spirit:core".into()],
                            user_modules: vec![],
                            flow_kind: super::metaflow_declaration::FlowKindV4::Regular,
                            tool_set: signal_flow::ToolSet::Default,
                            availability,
                            purpose: "Kept.".into(),
                            records: vec![],
                        },
                    },
                ))
                .unwrap();
        }
        drop(engine);
        for _ in 0..2 {
            let store = fixture.configured_store();
            assert_eq!(
                super::events::RecordsHarnessEvents::events(&store, "old-flow")
                    .unwrap()
                    .unwrap(),
                vec![
                    signal_flow::Event::Started(None),
                    signal_flow::Event::ToolUsed("Bash".into())
                ]
            );
            assert_eq!(
                store.successor_intent(&core).unwrap(),
                Some(SuccessorIntent {
                    address: core.clone(),
                    predecessor: "old-flow".into(),
                    cause: SuccessionCause::Exit,
                    launch_request_id: String::new(),
                })
            );
            let upgraded: signal_flow::DeliverySnapshot = snapshot.clone().into();
            assert_eq!(
                upgraded.delivery_state,
                signal_flow::DeliveryState::Failed(signal_flow::Refusal::Direct("gone".into()))
            );
            assert_eq!(
                store
                    .logical_delivery_snapshot(&"logical-old".to_owned())
                    .unwrap(),
                Some(upgraded.clone())
            );
            assert_eq!(
                store
                    .engine
                    .match_records(QueryPlan::all(store.logical_deliveries.events))
                    .unwrap()
                    .records()
                    .iter()
                    .map(|event| event.snapshot.clone())
                    .collect::<Vec<_>>(),
                vec![upgraded]
            );
            let kept = store
                .engine
                .match_records(QueryPlan::all(store.send_calls))
                .unwrap()
                .records()
                .to_vec();
            assert!(
                matches!(
                    kept.as_slice(),
                    [call] if call.tool_call_key == key
                        && matches!(
                            &call.send_outcome,
                            crate::generated::send_tool::SendOutcome::Accepted(accepted)
                                if accepted.send_acceptance.delivery_state
                                    == signal_flow::DeliveryState::Queued
                        )
                ),
                "{kept:?}"
            );
            assert_eq!(
                store
                    .metaflow_declaration(&core)
                    .unwrap()
                    .expect("core declared")
                    .continuity,
                meta_signal_flow::Continuity::Maintained
            );
            let other = store
                .metaflow_declaration(&elsewhere)
                .unwrap()
                .expect("other declared");
            assert_eq!(other.continuity, meta_signal_flow::Continuity::OnDemand);
            assert_eq!(other.purpose, "Kept.");
        }
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
        AppliesFlowQuery, ConfigurationOnlyStore, ConfirmsStartedFlow, DerivesNativeOwnerProof,
        FlowStore, LaunchOutcome, LaunchOutcomeV2, ManagesMetaflowState, NativeOwnerEnvironment,
        NativeOwnerProof, NativeOwnerState, OpensFlowStore, ReadsFlowRoles, ReadsFlowStore,
        ReadsLaunchAttempt, RecordsLaunchOutcome, RecordsNativeLaunchBinding,
        RecordsNativeLaunchIntent, RecordsPendingThread, RecordsPromptDeliveryIntent,
        RecordsPromptDeliveryResult, RecordsRegistrationAcknowledgement, RegistersExistingFlow,
        RegistersFlowIdentity, ReservesLaunchAttempt, ReservesPendingStart,
        SetsNativeOwnerEnvironmentForTests, StoredLaunchOutcomeV2,
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
                    layer: Layer::Secondary,
                    aspect: Aspect::Mind,
                    model_name: "gpt-sol".into(),
                    effort: "medium".into(),
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
            layer: Layer::Secondary,
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
            layer: Layer::Secondary,
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

    use super::{
        CompositionRefusalV34, LaunchOutcomeV8, ReplaceRejectionV34, StartRejectionV34,
        StoredLaunchOutcomeV8,
    };
    use super::{
        CompositionRefusalV37, LaunchOutcomeV9, ReplaceRejectionV37, StartRejectionV37,
        StoredLaunchOutcomeV9,
    };
    use super::{Configuration, ConfiguresOnlyStore, ReadsFlowMemory};
    use super::{
        ContextRefusalV30, LaunchOutcomeV6, ReplaceRejectionV30, StartRejectionV30,
        StoredLaunchOutcomeV6,
    };
    use super::{FlowRecordV5, LaunchedRecordV2, OriginClueBeforeOrigin, ReplacedRecordV2};
    use super::{LaunchOutcomeV3, ReplaceRejectionV17, StartRejectionV17, StoredLaunchOutcomeV3};
    use super::{LaunchOutcomeV4, ReplaceRejectionV18, StartRejectionV18, StoredLaunchOutcomeV4};
    use super::{LaunchOutcomeV5, ReplaceRejectionV22, StartRejectionV22, StoredLaunchOutcomeV5};
    use super::{LaunchOutcomeV7, ReplaceRejectionV32, StartRejectionV32, StoredLaunchOutcomeV7};
    use super::{LaunchOutcomeV10, ReplaceRejectionV38, StopRejectionV38, StoredLaunchOutcomeV10};
    use super::{LaunchOutcomeV11, ReplaceRejectionV39, StartRejectionV39, StoredLaunchOutcomeV11};
    use super::{LaunchedRecord, ReplacedRecord};
    use super::{LaysOutDefaults, SeedsConfiguration};
    use crate::store::RegistersBindings;
    use crate::store::delivery::RecordsDeliveries;
    use crate::store::delivery::{LeaseStep, PaneLease};
    use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
    use sema_engine::{
        Assertion, Engine, EngineOpen, EngineRecord, FamilyName, MemoryDefinition, QueryPlan,
        RecordKey, SchemaHash, SchemaVersion, TableDescriptor, TableName,
    };
    use signal_flow::{
        Address, Aspect, BindingRefusal, Caller, ComposedLaunch, FirstPromptPayload, FlowOrigin,
        HarnessKind, HerdrPaneBinding, LaunchAttemptPhase, LaunchAttemptReservation, LaunchProfile,
        Layer, NativeLaunchBinding, NativeLaunchIntent, NativeTargetReceipt,
        NativeTranscriptAbsence, NativeTranscriptBoundary, NativeTranscriptCursor, Origin,
        PromptDeliveryIntent, PromptDeliveryResult, Query, RegistrationAcknowledgement,
        ReplaceRejection, Response, StartRejection, StopRejection, TargetReceiptRequest,
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

        /// Replaces the store's model family with its v1 shape, one model
        /// per Layer under that Layer alone, as 0.35.11 and earlier kept it.
        fn seed_v1_models(&self, schema: u32, models: &[(signal_flow::Layer, &str)]) {
            let database = redb::Database::open(self.path()).expect("store opens directly");
            let write = database.begin_write().expect("catalog transaction starts");
            {
                let mut catalog = write
                    .open_table(super::SEMA_CATALOG_TABLE)
                    .expect("catalog opens");
                catalog
                    .remove("flow_nexus_meta_model_configuration")
                    .expect("model family removes");
            }
            let _ = write.delete_table(redb::TableDefinition::<&str, &[u8]>::new(
                "flow_nexus_meta_model_configuration",
            ));
            write.commit().expect("catalog commits");
            drop(database);
            let mut engine = Engine::open(EngineOpen::new(self.path(), SchemaVersion::new(schema)))
                .expect("v1 store opens");
            let table = engine
                .register_table(TableDescriptor::new(
                    TableName::new("flow_nexus_meta_model_configuration"),
                    FamilyName::new("flow-nexus-meta-model-configuration"),
                    SchemaHash::for_label("flow-nexus-meta-model-configuration-v1"),
                ))
                .expect("v1 model family registers");
            for (layer, native) in models {
                engine
                    .assert(Assertion::new(
                        table,
                        super::StoredMetaModelConfigurationV1 {
                            model: super::ModelBeforeAspect {
                                layer: layer.clone(),
                                native: (*native).into(),
                            },
                        },
                    ))
                    .expect("v1 model row persists");
            }
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
                        flows_root: "/tmp/flows".into(),
                        meta_aspects: settings.meta_aspects,
                        message_nexus_path: settings.message_nexus_path,
                        message_nexus_binary: "/fixture/message-nexus".into(),
                        curriculum_nexus_path: "/fixture/curriculum.sock".into(),
                        lease: 60,
                        executables: meta_signal_flow::Executables {
                            herdr: "herdr".into(),
                            flow_id_claimer: "flow-id".into(),
                            hook: "flow-hook".into(),
                        },
                        default_herdr_session: "default".into(),
                    },
                ))
                .expect("configuration commits");
        }

        /// The tables 0.35.19 adds to a running store.
        const ADDED_IN_0_35_19: &'static [&'static str] = &[
            "flow_nexus_meta_send_server_configuration",
            "flow_nexus_queue_delivery_counter",
            "flow_nexus_queue_delivery_requests",
            "flow_nexus_queue_delivery_attempts",
        ];

        /// Removes the tables 0.35.19 adds, empty in the store, so the
        /// catalog is the one 0.35.11 wrote.
        fn as_0_35_11(&self) {
            let database = redb::Database::open(self.path()).expect("store opens directly");
            let write = database.begin_write().expect("catalog transaction starts");
            {
                let mut catalog = write
                    .open_table(super::SEMA_CATALOG_TABLE)
                    .expect("catalog opens");
                for table in Self::ADDED_IN_0_35_19 {
                    catalog.remove(*table).expect("added family removes");
                }
            }
            for table in Self::ADDED_IN_0_35_19 {
                let _ = write.delete_table(redb::TableDefinition::<&str, &[u8]>::new(table));
            }
            write.commit().expect("catalog commits");
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
                .filter(|name| name.starts_with(".flow.sema.upgrade-"))
                .collect()
        }

        fn configured_store(&self) -> FlowStore {
            FlowStore::open_configured(&self.path()).expect("configured store opens")
        }

        /// Replaces the store's leave-out family with its v1 shape, holding
        /// each default given under its Address alone, as 0.35.0 to 0.35.11
        /// kept it.
        fn seed_v1_left_out(&self, defaults: &[(signal_flow::Address, Vec<LeaveOutV37>)]) {
            let database = redb::Database::open(self.path()).expect("store opens directly");
            let write = database.begin_write().expect("catalog transaction starts");
            {
                let mut catalog = write
                    .open_table(super::SEMA_CATALOG_TABLE)
                    .expect("catalog opens");
                catalog
                    .remove("flow_nexus_meta_leave_out_configuration")
                    .expect("leave-out family removes");
            }
            let _ = write.delete_table(redb::TableDefinition::<&str, &[u8]>::new(
                "flow_nexus_meta_leave_out_configuration",
            ));
            write.commit().expect("catalog commits");
            drop(database);
            let mut engine = Engine::open(EngineOpen::new(
                self.path(),
                SchemaVersion::new(super::FLOW_SCHEMA_VERSION),
            ))
            .expect("v1 store opens");
            let table = engine
                .register_table(TableDescriptor::new(
                    TableName::new("flow_nexus_meta_leave_out_configuration"),
                    FamilyName::new("flow-nexus-meta-leave-out-configuration"),
                    SchemaHash::for_label("flow-nexus-meta-leave-out-configuration-v1"),
                ))
                .expect("v1 leave-out family registers");
            for (address, left_out) in defaults {
                engine
                    .assert(Assertion::new(
                        table,
                        super::StoredMetaLeaveOutConfigurationV1 {
                            leave_out: super::LeaveOutDefaultBeforeHarness {
                                address: address.clone(),
                                left_out: left_out.clone(),
                            },
                        },
                    ))
                    .expect("v1 leave-out row persists");
            }
        }

        /// Replaces the store's events family with its v1 shape, each flow's
        /// events as 0.35.11 and earlier kept them.
        fn seed_v1_events(&self, flows: &[(&str, Vec<super::events::EventBeforeIdentity>)]) {
            let database = redb::Database::open(self.path()).expect("store opens directly");
            let write = database.begin_write().expect("catalog transaction starts");
            {
                let mut catalog = write
                    .open_table(super::SEMA_CATALOG_TABLE)
                    .expect("catalog opens");
                catalog
                    .remove("flow_nexus_flow_events")
                    .expect("events family removes");
            }
            let _ = write.delete_table(redb::TableDefinition::<&str, &[u8]>::new(
                "flow_nexus_flow_events",
            ));
            write.commit().expect("catalog commits");
            drop(database);
            let mut engine = Engine::open(EngineOpen::new(
                self.path(),
                SchemaVersion::new(super::FLOW_SCHEMA_VERSION),
            ))
            .expect("v1 store opens");
            let table = engine
                .register_table(TableDescriptor::new(
                    TableName::new("flow_nexus_flow_events"),
                    FamilyName::new("flow-nexus-flow-events"),
                    SchemaHash::for_label("flow-nexus-flow-events-v1"),
                ))
                .expect("v1 events family registers");
            for (flow_id, event_vector) in flows {
                engine
                    .assert(Assertion::new(
                        table,
                        super::events::FlowEventsV1 {
                            flow_id: (*flow_id).into(),
                            event_vector: event_vector.clone(),
                        },
                    ))
                    .expect("v1 event row persists");
            }
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

        fn seed_v8_outcomes_replacing(
            &self,
            rows: impl IntoIterator<Item = StoredLaunchOutcomeV8>,
        ) {
            if self.path().exists() {
                let database = redb::Database::open(self.path()).expect("store opens directly");
                let write = database.begin_write().expect("catalog transaction starts");
                {
                    let mut catalog = write
                        .open_table(super::SEMA_CATALOG_TABLE)
                        .expect("catalog opens");
                    catalog
                        .remove(super::FLOW_LAUNCH_OUTCOME_TABLE_NAME.as_str())
                        .expect("outcome family removes");
                }
                let _ = write.delete_table(redb::TableDefinition::<&str, &[u8]>::new(
                    super::FLOW_LAUNCH_OUTCOME_TABLE_NAME.as_str(),
                ));
                write.commit().expect("old outcome family deletion commits");
            }
            let mut engine = Engine::open(EngineOpen::new(self.path(), SchemaVersion::new(1)))
                .expect("outcome store opens");
            let outcomes = engine
                .register_table(TableDescriptor::new(
                    super::FLOW_LAUNCH_OUTCOME_TABLE_NAME,
                    FamilyName::new("flow-nexus-launch-outcome"),
                    StoredLaunchOutcomeV8::schema_hash(),
                ))
                .expect("v8 outcome family registers");
            for row in rows {
                engine
                    .assert(Assertion::new(outcomes, row))
                    .expect("v8 outcome persists");
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
                remembering: signal_flow::Remembering::Predecessors(0),
                herdr_session_name: "fixture-session".into(),
                left_out_option: None,
                instruction_prompt: "fixture instruction".into(),
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
                left_out: Vec::new(),
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
            left_out: Vec::new(),
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
            left_out: Vec::new(),
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
                    source_root: settings.source_root.clone(),
                    flows_root: "/tmp/flows".into(),
                    meta_aspects: settings.meta_aspects,
                    message_nexus_path: settings.message_nexus_path,
                    message_nexus_binary: "/fixture/message-nexus".into(),
                    curriculum_nexus_path: "/fixture/curriculum.sock".into(),
                    lease: 60,
                    executables: meta_signal_flow::Executables {
                        herdr: "herdr".into(),
                        flow_id_claimer: "flow-id".into(),
                        hook: "flow-hook".into(),
                    },
                    default_herdr_session: "default".into(),
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
                    source_root: settings.source_root.clone(),
                    flows_root: "/tmp/flows".into(),
                    meta_aspects: settings.meta_aspects,
                    message_nexus_path: settings.message_nexus_path,
                    message_nexus_binary: "/fixture/message-nexus".into(),
                    curriculum_nexus_path: "/fixture/curriculum.sock".into(),
                    lease: 60,
                    executables: meta_signal_flow::Executables {
                        herdr: "herdr".into(),
                        flow_id_claimer: "flow-id".into(),
                        hook: "flow-hook".into(),
                    },
                    default_herdr_session: "default".into(),
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

    /// A row of a retired table. Its shape is never read; it only fills a
    /// table.
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

    /// An older store holds every retired table with rows in it: the meta
    /// topic modules and start bundles of 0.34.6, the fresh-wake marks of
    /// 0.35.21. Its next open drops each table, rows and registration, and
    /// the store keeps opening without them.
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
                    source_root: settings.source_root.clone(),
                    flows_root: "/tmp/flows".into(),
                    meta_aspects: settings.meta_aspects,
                    message_nexus_path: settings.message_nexus_path,
                    message_nexus_binary: "/fixture/message-nexus".into(),
                    curriculum_nexus_path: "/fixture/curriculum.sock".into(),
                    lease: 60,
                    executables: meta_signal_flow::Executables {
                        herdr: "herdr".into(),
                        flow_id_claimer: "flow-id".into(),
                        hook: "flow-hook".into(),
                    },
                    default_herdr_session: "default".into(),
                },
            ))
            .expect("configuration commits");
        drop(config);
        drop(FlowStore::open_configured(&path).expect("configured Memory opens"));

        // The retired tables as an older store kept them, each holding a row.
        {
            let mut engine = Engine::open(EngineOpen::new(&path, SchemaVersion::new(2)))
                .expect("store opens under the engine");
            for (table, family, schema) in
                super::RETIRED_TABLES
                    .iter()
                    .copied()
                    .filter(|(table, _, _)| {
                        *table != super::META_LEAVE_OUT_CONFIGURATION_TABLE_NAME.as_str()
                    })
            {
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
        for (table, _, _) in super::RETIRED_TABLES
            .iter()
            .copied()
            .filter(|(table, _, _)| {
                *table != super::META_LEAVE_OUT_CONFIGURATION_TABLE_NAME.as_str()
            })
        {
            assert!(
                registered.iter().any(|name| name.as_str() == table),
                "{table} seeded"
            );
            assert!(
                stored.iter().any(|name| name.as_str() == table),
                "{table} rows seeded"
            );
        }

        drop(FlowStore::open_configured(&path).expect("the older store opens"));
        let (registered, stored) = tables();
        for (table, _, _) in super::RETIRED_TABLES
            .iter()
            .copied()
            .filter(|(table, _, _)| {
                *table != super::META_LEAVE_OUT_CONFIGURATION_TABLE_NAME.as_str()
            })
        {
            assert!(
                !registered.iter().any(|name| name.as_str() == table),
                "{table} unregistered"
            );
            assert!(
                !stored.iter().any(|name| name.as_str() == table),
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
                    source_root: settings.source_root.clone(),
                    flows_root: "/tmp/flows".into(),
                    meta_aspects: settings.meta_aspects,
                    message_nexus_path: settings.message_nexus_path,
                    message_nexus_binary: "/fixture/message-nexus".into(),
                    curriculum_nexus_path: "/fixture/curriculum.sock".into(),
                    lease: 60,
                    executables: meta_signal_flow::Executables {
                        herdr: "herdr".into(),
                        flow_id_claimer: "flow-id".into(),
                        hook: "flow-hook".into(),
                    },
                    default_herdr_session: "default".into(),
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
        assert_eq!(
            node.endpoint_selection,
            signal_flow::EndpointSelection::Unavailable,
            "a fresh store fabricates no Codex endpoint before a harness is configured"
        );
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
                layer: Layer::Secondary,
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
                layer: Layer::Secondary,
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
                    "flow_nexus_meta_model_threshold_configuration",
                    engine
                        .match_records(QueryPlan::all(meta.thresholds))
                        .expect("meta threshold configuration reads")
                        .records()
                        .len(),
                ),
                read(
                    "flow_nexus_meta_investigation_configuration",
                    engine
                        .match_records(QueryPlan::all(meta.investigation))
                        .expect("meta investigation configuration reads")
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
                read(
                    "flow_nexus_compactions",
                    engine
                        .match_records(QueryPlan::all(self.compactions))
                        .expect("compactions read")
                        .records()
                        .len(),
                ),
                read(
                    "flow_nexus_successor_intents",
                    engine
                        .match_records(QueryPlan::all(self.successor_intents))
                        .expect("successor intents read")
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
            left_out: Vec::new(),
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
                remembering: signal_flow::Remembering::Custom(vec!["e51411".into()]),
                herdr_session_name: "messaging-build".into(),
                left_out_option: Some(Vec::new()),
                tool_set: signal_flow::ToolSet::Default,
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
                layer: Layer::Secondary,
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
            layer: Layer::Primary,
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

    /// A store before 0.35.19 keeps Stops without their native identity:
    /// opening carries every flow's events in order, each such Stop as
    /// `Stopped.{ None None }`, which is never taken for the flow's own.
    #[test]
    fn a_v1_stop_upgrades_to_a_stop_of_unknown_identity() {
        use super::events::{EventBeforeIdentity, FlowEventsV1, RecordsHarnessEvents};
        use signal_flow::{Event, NativeStop};
        let fixture = StoreFixture::new();
        drop(fixture.store());
        let database = redb::Database::open(fixture.path()).expect("store opens directly");
        let write = database.begin_write().expect("catalog transaction starts");
        {
            let mut catalog = write
                .open_table(super::SEMA_CATALOG_TABLE)
                .expect("catalog opens");
            catalog
                .remove("flow_nexus_flow_events")
                .expect("events family removes");
        }
        write
            .delete_table(redb::TableDefinition::<&str, &[u8]>::new(
                "flow_nexus_flow_events",
            ))
            .expect("event rows drop");
        write.commit().expect("catalog commits");
        drop(database);
        {
            let mut engine = Engine::open(EngineOpen::new(fixture.path(), SchemaVersion::new(1)))
                .expect("v1 store opens");
            let events = engine
                .register_table(TableDescriptor::new(
                    TableName::new("flow_nexus_flow_events"),
                    FamilyName::new("flow-nexus-flow-events"),
                    SchemaHash::for_label("flow-nexus-flow-events-v1"),
                ))
                .expect("v1 events family registers");
            engine
                .assert(Assertion::new(
                    events,
                    FlowEventsV1 {
                        flow_id: "a1a1a1".into(),
                        event_vector: vec![
                            EventBeforeIdentity::Started,
                            EventBeforeIdentity::ToolUsed("Bash".into()),
                            EventBeforeIdentity::Stopped,
                        ],
                    },
                ))
                .expect("v1 row persists");
        }

        let store = fixture.store();
        assert_eq!(
            store.events("a1a1a1").unwrap(),
            Some(vec![
                Event::Started(None),
                Event::ToolUsed("Bash".into()),
                Event::Stopped(NativeStop {
                    native_session_id_option: None,
                    transcript_path_option: None,
                    last_assistant_message_option: None,
                }),
            ])
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
                            state: super::StateBeforeIdle::Asleep,
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
        assert!(metaflow.queue.is_empty());
        let expected = vec![
            flow_ethos::QueueEntry {
                logical_delivery_id: format!(
                    "legacy:{}:{:016x}",
                    FlowStore::address_record_key(&address),
                    0
                ),
                sent_by: flow_ethos::SentBy::Unknown,
                delivery: flow_ethos::Delivery::Queue,
                message: flow_ethos::Message::Soft(Request::Order("first".into())),
            },
            flow_ethos::QueueEntry {
                logical_delivery_id: format!(
                    "legacy:{}:{:016x}",
                    FlowStore::address_record_key(&address),
                    1
                ),
                sent_by: flow_ethos::SentBy::Unknown,
                delivery: flow_ethos::Delivery::Queue,
                message: flow_ethos::Message::Soft(Request::Notice("second".into())),
            },
        ];
        let archive = store.observe_legacy_archive().unwrap();
        for entry in expected {
            assert!(archive.iter().any(|record| record.address == address
                && record.queue_evidence
                    == meta_signal_flow::QueueEvidence::Logical(entry.clone())));
        }
    }

    #[test]
    fn a_v1_process_binding_reopens_without_inventing_a_sender_flow() {
        use sema_engine::{MemoryDefinition, MemoryUpgrades, UpgradeOutcome};
        let fixture = StoreFixture::new();
        drop(fixture.store());
        let table_name = TableName::new("fixture_metaflow_processes");
        let address = Address {
            aspect: Aspect::Psyche,
            topic: "core".into(),
            layer: Layer::Secondary,
        };
        let old = super::StoredMetaflowProcessV1 {
            process: signal_flow::Process {
                pid: 7,
                started: 11,
            },
            address,
        };
        {
            let mut engine = Engine::open(EngineOpen::new(fixture.path(), SchemaVersion::new(1)))
                .expect("v1 store opens");
            let table = engine
                .register_table(TableDescriptor::new(
                    table_name,
                    FamilyName::new("fixture-metaflow-process"),
                    super::StoredMetaflowProcessV1::schema_hash(),
                ))
                .expect("v1 process family registers");
            engine
                .assert(Assertion::new(table, old))
                .expect("v1 process persists");
        }
        for _ in 0..2 {
            let mut engine = Engine::open(EngineOpen::new(fixture.path(), SchemaVersion::new(1)))
                .expect("store reopens");
            let descriptor = TableDescriptor::<super::StoredMetaflowProcess>::new(
                table_name,
                FamilyName::new("fixture-metaflow-process"),
                super::StoredMetaflowProcess::schema_hash(),
            )
            .with_predecessor::<super::StoredMetaflowProcessV1>();
            let table = match engine
                .upgrade_table::<super::StoredMetaflowProcessV1, super::StoredMetaflowProcess>(
                    descriptor,
                ) {
                UpgradeOutcome::Upgraded(receipt) => receipt.table(),
                UpgradeOutcome::AlreadyCurrent(table) => table,
                other => panic!("process upgrade failed: {other:?}"),
            };
            let rows = engine
                .match_records(QueryPlan::all(table))
                .expect("upgraded rows read");
            let [carried] = rows.records() else {
                panic!("one process binding is carried")
            };
            assert_eq!(carried.flow_id_option, None);
        }
    }

    #[test]
    fn a_v1_delivery_lock_reopens_without_inventing_a_sender_flow() {
        use sema_engine::{MemoryDefinition, MemoryUpgrades, UpgradeOutcome};
        let fixture = StoreFixture::new();
        drop(fixture.store());
        let table_name = TableName::new("fixture_metaflow_locks");
        let address = Address {
            aspect: Aspect::Psyche,
            topic: "core".into(),
            layer: Layer::Secondary,
        };
        let old = super::StoredMetaflowLockV1 {
            lock: signal_flow::Lock {
                sender: address.clone(),
                address: address.clone(),
                until: 42,
            },
        };
        {
            let mut engine = Engine::open(EngineOpen::new(fixture.path(), SchemaVersion::new(1)))
                .expect("v1 store opens");
            let table = engine
                .register_table(TableDescriptor::new(
                    table_name,
                    FamilyName::new("fixture-metaflow-lock"),
                    super::StoredMetaflowLockV1::schema_hash(),
                ))
                .expect("v1 lock family registers");
            engine
                .assert(Assertion::new(table, old))
                .expect("v1 lock persists");
        }

        for _ in 0..2 {
            let mut engine = Engine::open(EngineOpen::new(fixture.path(), SchemaVersion::new(1)))
                .expect("store reopens");
            let descriptor = TableDescriptor::<super::StoredMetaflowLock>::new(
                table_name,
                FamilyName::new("fixture-metaflow-lock"),
                super::StoredMetaflowLock::schema_hash(),
            )
            .with_predecessor::<super::StoredMetaflowLockV1>();
            let table = match engine
                .upgrade_table::<super::StoredMetaflowLockV1, super::StoredMetaflowLock>(descriptor)
            {
                UpgradeOutcome::Upgraded(receipt) => receipt.table(),
                UpgradeOutcome::AlreadyCurrent(table) => table,
                other => panic!("lock upgrade failed: {other:?}"),
            };
            let rows = engine
                .match_records(QueryPlan::all(table))
                .expect("upgraded rows read");
            let [carried] = rows.records() else {
                panic!("one lock is carried")
            };
            assert_eq!(carried.sender_flow_id_option, None);
        }
    }

    /// A topic never dies: a 0.35.21 store's ended metaflow opens Asleep,
    /// its past and its queue kept, and an Awake one as it was.
    #[test]
    fn a_v2_ended_metaflow_upgrades_asleep() {
        use super::ManagesMetaflowState;
        use signal_flow::{Address, Layer, Request};
        let fixture = StoreFixture::new();
        drop(fixture.store());
        let at = |layer| Address {
            aspect: Aspect::Mind,
            topic: "flow".into(),
            layer,
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
        let queued = vec![super::QueuedRequestBeforeFlow {
            sent_by: super::SentByBeforeFlow::Unknown,
            request: Request::Order("kept".into()),
        }];
        {
            let mut engine = Engine::open(EngineOpen::new(fixture.path(), SchemaVersion::new(1)))
                .expect("v2 store opens");
            let metaflows = engine
                .register_table(TableDescriptor::new(
                    TableName::new("flow_nexus_metaflows"),
                    FamilyName::new("flow-nexus-metaflow"),
                    SchemaHash::for_label("flow-nexus-metaflow-v2"),
                ))
                .expect("v2 metaflow family registers");
            for (layer, state) in [
                (Layer::Tertiary, super::StateBeforeIdle::Ended),
                (
                    Layer::Secondary,
                    super::StateBeforeIdle::Awake("b2b2b2".into()),
                ),
            ] {
                engine
                    .assert(Assertion::new(
                        metaflows,
                        super::StoredMetaflowV2 {
                            metaflow: super::MetaflowBeforeIdle {
                                address: at(layer),
                                state,
                                past: vec!["a1a1a1".into()],
                                queue: queued.clone(),
                            },
                        },
                    ))
                    .expect("v2 row persists");
            }
        }

        let store = fixture.store();
        let metaflows = store.list_metaflows().unwrap();
        let carried = |layer: Layer| {
            let address = at(layer);
            metaflows
                .iter()
                .find(|metaflow| metaflow.address == address)
                .cloned()
                .expect("the metaflow is carried")
        };
        let idle = carried(Layer::Tertiary);
        assert_eq!(idle.state, flow_ethos::State::Asleep);
        assert_eq!(idle.past, vec!["a1a1a1".to_owned()]);
        assert!(idle.queue.is_empty());
        let archive = store.observe_legacy_archive().unwrap();
        let archived = archive
            .iter()
            .find(|record| record.address == at(Layer::Tertiary))
            .expect("legacy queue is archived");
        let meta_signal_flow::QueueEvidence::Logical(entry) = &archived.queue_evidence else {
            panic!("the full migrated queue slot is kept")
        };
        assert_eq!(entry.sent_by, flow_ethos::SentBy::Unknown);
        assert_eq!(
            entry.message,
            flow_ethos::Message::Soft(Request::Order("kept".into()))
        );
        assert_eq!(
            carried(Layer::Secondary).state,
            flow_ethos::State::Awake("b2b2b2".into())
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

    /// Registers a launch outcome family at its v5 shape, as 0.35.6 to
    /// 0.35.21 kept it, holding these rows.
    fn seed_v5_launch_outcomes(fixture: &StoreFixture, rows: Vec<StoredLaunchOutcomeV5>) {
        let mut engine = Engine::open(EngineOpen::new(fixture.path(), SchemaVersion::new(1)))
            .expect("outcome store opens");
        let outcomes = engine
            .register_table(TableDescriptor::new(
                TableName::new("flow_nexus_launch_outcomes"),
                FamilyName::new("flow-nexus-launch-outcome"),
                SchemaHash::for_label("flow-nexus-launch-outcome-v5"),
            ))
            .expect("v5 outcome family registers");
        for row in rows {
            engine
                .assert(Assertion::new(outcomes, row))
                .expect("v5 outcome persists");
        }
    }

    /// A 0.35.21 store's outcomes, each rejection in signal-flow 22's shape
    /// with AddressEnded among them, open as the current outcomes: every
    /// rejection after AddressEnded keeps its meaning though its archived
    /// discriminant moves.
    #[test]
    fn v5_launch_outcomes_upgrade_without_the_ended_address() {
        let fixture = StoreFixture::new();
        let started = LaunchedRecord {
            flow_id: "started-flow".into(),
            session_id: "started-session".into(),
            origin: Origin::Unbound,
        };
        seed_v5_launch_outcomes(
            &fixture,
            vec![
                StoredLaunchOutcomeV5 {
                    launch_request_id: "v5-started".into(),
                    launch_outcome: LaunchOutcomeV5::Started(started.clone()),
                },
                StoredLaunchOutcomeV5 {
                    launch_request_id: "v5-awake".into(),
                    launch_outcome: LaunchOutcomeV5::StartRejected(StartRejectionV22::AddressAwake),
                },
                StoredLaunchOutcomeV5 {
                    launch_request_id: "v5-context".into(),
                    launch_outcome: LaunchOutcomeV5::StartRejected(
                        StartRejectionV22::ContextRefused(ContextRefusalV30::ModuleMissing),
                    ),
                },
                StoredLaunchOutcomeV5 {
                    launch_request_id: "v5-model".into(),
                    launch_outcome: LaunchOutcomeV5::ReplaceRejected(
                        ReplaceRejectionV22::LaunchRefused(StartRejectionV22::ModelRefused(
                            signal_flow::ModelRefusal::Differs("claude-opus-5-5".into()),
                        )),
                    ),
                },
            ],
        );
        let store = fixture.store();
        for (request, outcome) in [
            ("v5-started", LaunchOutcome::Started(started)),
            (
                "v5-awake",
                LaunchOutcome::StartRejected(StartRejection::AddressAwake),
            ),
            (
                "v5-context",
                LaunchOutcome::StartRejected(StartRejection::ContextRefused(
                    signal_flow::ContextRefusal::ModuleMissing,
                )),
            ),
            (
                "v5-model",
                LaunchOutcome::ReplaceRejected(ReplaceRejection::LaunchRefused(
                    StartRejection::ModelRefused(signal_flow::ModelRefusal::Differs(
                        "claude-opus-5-5".into(),
                    )),
                )),
            ),
        ] {
            assert_eq!(
                store
                    .launch_outcome(request)
                    .expect("upgraded outcome reads"),
                Some(outcome),
                "the v5 outcome for {request} survives the family upgrade"
            );
        }
    }

    #[test]
    fn every_legacy_leave_out_refusal_is_archived_losslessly() {
        macro_rules! assert_version {
            ($stored:ident, $outcome:ident, $start:ident, $replace:ident, $version:literal) => {{
                let fixture = StoreFixture::new();
                let rows = [
                    (
                        "start-indispensable",
                        super::LeaveOutRefusalV37::Indispensable(super::LeaveOutV37::UserSettings),
                        false,
                    ),
                    (
                        "start-needs-workflow",
                        super::LeaveOutRefusalV37::NeedsWorkflowLeftOut(
                            super::LeaveOutV37::SkillTool,
                        ),
                        false,
                    ),
                    (
                        "start-not-for-harness",
                        super::LeaveOutRefusalV37::NotForHarness(
                            super::LeaveOutV37::CodexSkillListing,
                        ),
                        false,
                    ),
                    (
                        "replace-indispensable",
                        super::LeaveOutRefusalV37::Indispensable(super::LeaveOutV37::UserSettings),
                        true,
                    ),
                    (
                        "replace-needs-workflow",
                        super::LeaveOutRefusalV37::NeedsWorkflowLeftOut(
                            super::LeaveOutV37::BundledSkills,
                        ),
                        true,
                    ),
                    (
                        "replace-not-for-harness",
                        super::LeaveOutRefusalV37::NotForHarness(super::LeaveOutV37::CodexAgentsMd),
                        true,
                    ),
                ];
                let mut archives = Vec::new();
                {
                    let mut engine =
                        Engine::open(EngineOpen::new(fixture.path(), SchemaVersion::new(1)))
                            .expect("outcome store opens");
                    let outcomes = engine
                        .register_table(TableDescriptor::new(
                            TableName::new("flow_nexus_launch_outcomes"),
                            FamilyName::new("flow-nexus-launch-outcome"),
                            $stored::schema_hash(),
                        ))
                        .expect("legacy outcome family registers");
                    for (suffix, refusal, replacement) in rows {
                        let key = format!("v{}-{suffix}", $version);
                        let rejection = $start::LeaveOutRefused(refusal);
                        let previous = $stored {
                            launch_request_id: key.clone(),
                            launch_outcome: if replacement {
                                $outcome::ReplaceRejected($replace::LaunchRefused(rejection))
                            } else {
                                $outcome::StartRejected(rejection)
                            },
                        };
                        let archive = rkyv::to_bytes::<rkyv::rancor::Error>(&previous)
                            .expect("legacy outcome archives")
                            .to_vec();
                        archives.push((key, archive.clone()));
                        engine
                            .assert(Assertion::new(outcomes, previous))
                            .expect("legacy outcome persists");
                    }
                }
                let store = fixture.store();
                for (key, archive) in &archives {
                    assert_eq!(
                        store.launch_outcome(key).expect("archived outcome reads"),
                        Some(LaunchOutcome::Archived(signal_flow::LaunchOutcomeArchive {
                            launch_request_id: key.clone(),
                            launch_outcome_family: "flow-nexus-launch-outcome".into(),
                            launch_outcome_version: $version,
                            launch_archive_reason:
                                signal_flow::LaunchArchiveReason::LegacyLeaveOutRefusal,
                        }))
                    );
                    let record = store
                        .engine
                        .match_records(QueryPlan::key(
                            store.quarantined_launch_outcomes,
                            RecordKey::new(key),
                        ))
                        .expect("archive record reads");
                    assert_eq!(record.records().len(), 1);
                    assert_eq!(record.records()[0].archive, *archive);
                }
                drop(store);
                let reopened = fixture.store();
                assert_eq!(
                    reopened
                        .engine
                        .match_records(QueryPlan::all(reopened.quarantined_launch_outcomes))
                        .expect("reopened archives read")
                        .records()
                        .len(),
                    archives.len()
                );
            }};
        }
        assert_version!(
            StoredLaunchOutcomeV11,
            LaunchOutcomeV11,
            StartRejectionV39,
            ReplaceRejectionV39,
            11
        );
        assert_version!(
            StoredLaunchOutcomeV10,
            LaunchOutcomeV10,
            StartRejectionV39,
            ReplaceRejectionV38,
            10
        );
        assert_version!(
            StoredLaunchOutcomeV9,
            LaunchOutcomeV9,
            StartRejectionV37,
            ReplaceRejectionV37,
            9
        );
        assert_version!(
            StoredLaunchOutcomeV8,
            LaunchOutcomeV8,
            StartRejectionV34,
            ReplaceRejectionV34,
            8
        );
        assert_version!(
            StoredLaunchOutcomeV7,
            LaunchOutcomeV7,
            StartRejectionV32,
            ReplaceRejectionV32,
            7
        );
        assert_version!(
            StoredLaunchOutcomeV6,
            LaunchOutcomeV6,
            StartRejectionV30,
            ReplaceRejectionV30,
            6
        );
        assert_version!(
            StoredLaunchOutcomeV5,
            LaunchOutcomeV5,
            StartRejectionV22,
            ReplaceRejectionV22,
            5
        );
        assert_version!(
            StoredLaunchOutcomeV4,
            LaunchOutcomeV4,
            StartRejectionV18,
            ReplaceRejectionV18,
            4
        );
        assert_version!(
            StoredLaunchOutcomeV3,
            LaunchOutcomeV3,
            StartRejectionV17,
            ReplaceRejectionV17,
            3
        );
        assert_version!(
            StoredLaunchOutcomeV2,
            LaunchOutcomeV2,
            StartRejectionV17,
            ReplaceRejectionV17,
            2
        );
    }

    #[test]
    fn quarantined_outcome_v1_gains_provenance_once() {
        let fixture = StoreFixture::new();
        {
            let mut engine = Engine::open(EngineOpen::new(fixture.path(), SchemaVersion::new(1)))
                .expect("store opens");
            let quarantine = engine
                .register_table(TableDescriptor::new(
                    super::FLOW_QUARANTINED_LAUNCH_OUTCOME_TABLE_NAME,
                    FamilyName::new("flow-nexus-quarantined-launch-outcome"),
                    super::QuarantinedLaunchOutcomeV1::schema_hash(),
                ))
                .expect("v1 archive family registers");
            engine
                .assert(Assertion::new(
                    quarantine,
                    super::QuarantinedLaunchOutcomeV1 {
                        launch_request_id: "old-archive".into(),
                        archive: vec![1, 2, 3],
                        reason: super::HistoricalAttemptReason::Missing,
                    },
                ))
                .expect("v1 archive persists");
        }
        let store = fixture.store();
        assert_eq!(
            store
                .launch_outcome("old-archive")
                .expect("upgraded archive reads"),
            Some(LaunchOutcome::Archived(signal_flow::LaunchOutcomeArchive {
                launch_request_id: "old-archive".into(),
                launch_outcome_family: "flow-nexus-launch-outcome".into(),
                launch_outcome_version: 8,
                launch_archive_reason: signal_flow::LaunchArchiveReason::HistoricalAttemptMissing,
            }))
        );
        drop(store);
        assert!(
            fixture
                .store()
                .launch_outcome("old-archive")
                .expect("reopened archive reads")
                .is_some()
        );
    }

    /// The immediately preceding B12 shape keeps required addresses exactly
    /// and turns only the removed composition refusals into Unrecorded.
    #[test]
    fn v9_launch_outcomes_preserve_requirements_and_remove_old_refusals() {
        let fixture = StoreFixture::new();
        let required = signal_flow::Address {
            aspect: signal_flow::Aspect::Mind,
            topic: "review".into(),
            layer: signal_flow::Layer::Tertiary,
        };
        let rows = vec![
            StoredLaunchOutcomeV9 {
                launch_request_id: "v9-requirement".into(),
                launch_outcome: LaunchOutcomeV9::StartRejected(
                    StartRejectionV37::RequirementUnmet(required.clone()),
                ),
            },
            StoredLaunchOutcomeV9 {
                launch_request_id: "v9-simplified".into(),
                launch_outcome: LaunchOutcomeV9::StartRejected(
                    StartRejectionV37::CompositionRefused(CompositionRefusalV37::InvalidToolSet),
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
                    StoredLaunchOutcomeV9::schema_hash(),
                ))
                .expect("v9 outcome family registers");
            for row in rows {
                engine
                    .assert(Assertion::new(outcomes, row))
                    .expect("v9 outcome persists");
            }
        }
        let store = fixture.store();
        assert_eq!(
            store
                .launch_outcome("v9-requirement")
                .expect("outcome reads"),
            Some(LaunchOutcome::StartRejected(
                StartRejection::RequirementUnmet(required)
            ))
        );
        assert_eq!(
            store
                .launch_outcome("v9-simplified")
                .expect("outcome reads"),
            Some(LaunchOutcome::StartRejected(
                StartRejection::CompositionRefused(signal_flow::CompositionRefusal::Unrecorded)
            ))
        );
    }

    #[test]
    fn v10_generic_close_refusal_upgrades_once_to_identity_unavailable() {
        let fixture = StoreFixture::new();
        {
            let mut engine = Engine::open(EngineOpen::new(fixture.path(), SchemaVersion::new(1)))
                .expect("outcome store opens");
            let outcomes = engine
                .register_table(TableDescriptor::new(
                    TableName::new("flow_nexus_launch_outcomes"),
                    FamilyName::new("flow-nexus-launch-outcome"),
                    StoredLaunchOutcomeV10::schema_hash(),
                ))
                .expect("v10 outcome family registers");
            engine
                .assert(Assertion::new(
                    outcomes,
                    StoredLaunchOutcomeV10 {
                        launch_request_id: "v10-close".into(),
                        launch_outcome: LaunchOutcomeV10::ReplaceRejected(
                            ReplaceRejectionV38::ReapRefused(StopRejectionV38::CloseRefused),
                        ),
                    },
                ))
                .expect("v10 outcome persists");
        }
        let expected = LaunchOutcome::ReplaceRejected(ReplaceRejection::ReapRefused(
            StopRejection::CloseRefused(signal_flow::PaneCloseFailure::IdentityUnavailable),
        ));
        let store = fixture.store();
        assert_eq!(
            store
                .launch_outcome("v10-close")
                .expect("upgraded outcome reads"),
            Some(expected.clone())
        );
        drop(store);
        assert_eq!(
            fixture
                .store()
                .launch_outcome("v10-close")
                .expect("reopened outcome reads"),
            Some(expected)
        );
    }

    /// A v8 outcome holding the two removed composition refusals opens with
    /// either refusal as Unrecorded; the surrounding rejection shape stays.
    #[test]
    fn v8_launch_outcomes_remove_simplified_and_presentation_refusals() {
        let fixture = StoreFixture::new();
        let rows = vec![
            StoredLaunchOutcomeV8 {
                launch_request_id: "v8-tool-set".into(),
                launch_outcome: LaunchOutcomeV8::StartRejected(
                    StartRejectionV34::CompositionRefused(CompositionRefusalV34::InvalidToolSet),
                ),
            },
            StoredLaunchOutcomeV8 {
                launch_request_id: "v8-presentation".into(),
                launch_outcome: LaunchOutcomeV8::ReplaceRejected(
                    ReplaceRejectionV34::LaunchRefused(StartRejectionV34::CompositionRefused(
                        CompositionRefusalV34::PresentationUnavailable,
                    )),
                ),
            },
            StoredLaunchOutcomeV8 {
                launch_request_id: "v8-model".into(),
                launch_outcome: LaunchOutcomeV8::StartRejected(
                    StartRejectionV34::CompositionRefused(CompositionRefusalV34::InvalidModelName),
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
                    SchemaHash::for_label("flow-nexus-launch-outcome-v8"),
                ))
                .expect("v8 outcome family registers");
            for row in rows {
                engine
                    .assert(Assertion::new(outcomes, row))
                    .expect("v8 outcome persists");
            }
        }
        let store = fixture.store();
        let unrecorded =
            StartRejection::CompositionRefused(signal_flow::CompositionRefusal::Unrecorded);
        assert_eq!(
            store.launch_outcome("v8-tool-set").expect("outcome reads"),
            Some(LaunchOutcome::StartRejected(unrecorded.clone()))
        );
        assert_eq!(
            store
                .launch_outcome("v8-presentation")
                .expect("outcome reads"),
            Some(LaunchOutcome::ReplaceRejected(
                ReplaceRejection::LaunchRefused(unrecorded)
            ))
        );
        assert_eq!(
            store.launch_outcome("v8-model").expect("outcome reads"),
            Some(LaunchOutcome::StartRejected(
                StartRejection::CompositionRefused(
                    signal_flow::CompositionRefusal::InvalidModelName
                )
            ))
        );
    }

    #[test]
    fn v8_no_secondary_uses_the_unique_attempt_address_and_reopens_idempotently() {
        let fixture = StoreFixture::new();
        let store = fixture.store();
        let launch = fixture.composed_launch("v8-requirement", &"7".repeat(64));
        assert!(matches!(
            store
                .reserve_launch_attempt(&launch, Origin::Unbound)
                .expect("attempt persists"),
            LaunchAttemptReservation::Reserved(_)
        ));
        drop(store);
        fixture.seed_v8_outcomes_replacing([StoredLaunchOutcomeV8 {
            launch_request_id: "v8-requirement".into(),
            launch_outcome: LaunchOutcomeV8::StartRejected(StartRejectionV34::NoSecondary),
        }]);

        let store = fixture.store();
        assert_eq!(
            store
                .launch_outcome("v8-requirement")
                .expect("outcome reads"),
            Some(LaunchOutcome::StartRejected(
                StartRejection::RequirementUnmet(launch.launch_profile.address.clone())
            ))
        );
        assert!(
            store
                .engine
                .match_records(QueryPlan::all(store.quarantined_launch_outcomes))
                .expect("archive reads")
                .records()
                .is_empty()
        );
        drop(store);

        let reopened = fixture.store();
        assert_eq!(
            reopened
                .launch_outcome("v8-requirement")
                .expect("reopened outcome reads"),
            Some(LaunchOutcome::StartRejected(
                StartRejection::RequirementUnmet(launch.launch_profile.address)
            ))
        );
    }

    #[test]
    fn v8_no_secondary_without_its_attempt_is_archived_whole_and_not_fabricated() {
        let fixture = StoreFixture::new();
        let initial = fixture.store();
        initial
            .reserve_launch_attempt(
                &fixture.composed_launch("unrelated-attempt", &"8".repeat(64)),
                Origin::Unbound,
            )
            .expect("unrelated attempt persists");
        drop(initial);
        let previous = StoredLaunchOutcomeV8 {
            launch_request_id: "v8-missing-attempt".into(),
            launch_outcome: LaunchOutcomeV8::ReplaceRejected(ReplaceRejectionV34::LaunchRefused(
                StartRejectionV34::NoSecondary,
            )),
        };
        let archive = rkyv::to_bytes::<rkyv::rancor::Error>(&previous)
            .expect("v8 outcome archives")
            .to_vec();
        fixture.seed_v8_outcomes_replacing([previous]);

        let store = fixture.store();
        assert_eq!(
            store
                .launch_outcome("v8-missing-attempt")
                .expect("active outcomes read"),
            Some(LaunchOutcome::Archived(signal_flow::LaunchOutcomeArchive {
                launch_request_id: "v8-missing-attempt".into(),
                launch_outcome_family: "flow-nexus-launch-outcome".into(),
                launch_outcome_version: 8,
                launch_archive_reason: signal_flow::LaunchArchiveReason::HistoricalAttemptMissing,
            }))
        );
        assert_eq!(
            store
                .engine
                .match_records(QueryPlan::all(store.quarantined_launch_outcomes))
                .expect("archive reads")
                .records(),
            &[super::QuarantinedLaunchOutcome {
                launch_request_id: "v8-missing-attempt".into(),
                origin_family: "flow-nexus-launch-outcome".into(),
                origin_version: 8,
                archive,
                reason: signal_flow::LaunchArchiveReason::HistoricalAttemptMissing,
            }]
        );
        drop(store);

        let reopened = fixture.store();
        assert_eq!(
            reopened
                .engine
                .match_records(QueryPlan::all(reopened.quarantined_launch_outcomes))
                .expect("reopened archive reads")
                .records()
                .len(),
            1,
            "reopening neither activates nor duplicates the archived row"
        );
    }

    #[test]
    fn v8_no_secondary_with_a_conflicting_attempt_is_archived_as_ambiguous() {
        let fixture = StoreFixture::new();
        let store = fixture.store();
        store
            .reserve_launch_attempt(
                &fixture.composed_launch("different-request", &"6".repeat(64)),
                Origin::Unbound,
            )
            .expect("attempt persists");
        let conflicting = store
            .stored_launch_attempt("different-request")
            .expect("attempt reads")
            .expect("attempt exists");
        drop(store);
        {
            let mut engine = Engine::open(EngineOpen::new(fixture.path(), SchemaVersion::new(1)))
                .expect("store opens");
            let attempts = engine
                .register_table(TableDescriptor::new(
                    super::FLOW_LAUNCH_ATTEMPT_TABLE_NAME,
                    FamilyName::new("flow-nexus-launch-attempt"),
                    super::StoredLaunchAttempt::schema_hash(),
                ))
                .expect("attempt family registers");
            engine
                .assert_keyed(sema_engine::KeyedAssertion::new(
                    attempts,
                    RecordKey::new("v8-conflicting-attempt"),
                    conflicting,
                ))
                .expect("conflicting keyed attempt persists");
        }
        fixture.seed_v8_outcomes_replacing([StoredLaunchOutcomeV8 {
            launch_request_id: "v8-conflicting-attempt".into(),
            launch_outcome: LaunchOutcomeV8::StartRejected(StartRejectionV34::NoSecondary),
        }]);

        let store = fixture.store();
        let archived = store
            .engine
            .match_records(QueryPlan::all(store.quarantined_launch_outcomes))
            .expect("archive reads");
        assert_eq!(archived.records().len(), 1);
        assert_eq!(
            archived.records()[0].reason,
            signal_flow::LaunchArchiveReason::HistoricalAttemptAmbiguous
        );
        assert_eq!(
            store
                .launch_outcome("v8-conflicting-attempt")
                .expect("active outcomes read"),
            Some(LaunchOutcome::Archived(signal_flow::LaunchOutcomeArchive {
                launch_request_id: "v8-conflicting-attempt".into(),
                launch_outcome_family: "flow-nexus-launch-outcome".into(),
                launch_outcome_version: 8,
                launch_archive_reason: signal_flow::LaunchArchiveReason::HistoricalAttemptAmbiguous,
            }))
        );
    }

    /// A 0.40 store's outcomes archived before a composition refusal kept
    /// its reason open as the current outcomes: such a refusal reads
    /// Unrecorded, every other outcome as it was.
    #[test]
    fn v7_launch_outcomes_upgrade_to_the_composition_refusal_shape() {
        let fixture = StoreFixture::new();
        let started = LaunchedRecord {
            flow_id: "started-flow".into(),
            session_id: "started-session".into(),
            origin: Origin::Unbound,
        };
        let rows = vec![
            StoredLaunchOutcomeV7 {
                launch_request_id: "v7-started".into(),
                launch_outcome: LaunchOutcomeV7::Started(started.clone()),
            },
            StoredLaunchOutcomeV7 {
                launch_request_id: "v7-composition".into(),
                launch_outcome: LaunchOutcomeV7::StartRejected(
                    StartRejectionV32::CompositionRefused,
                ),
            },
            StoredLaunchOutcomeV7 {
                launch_request_id: "v7-topic".into(),
                launch_outcome: LaunchOutcomeV7::StartRejected(StartRejectionV32::TopicRefused(
                    "PrimaryCommitting".into(),
                )),
            },
            StoredLaunchOutcomeV7 {
                launch_request_id: "v7-replace".into(),
                launch_outcome: LaunchOutcomeV7::ReplaceRejected(
                    ReplaceRejectionV32::LaunchRefused(StartRejectionV32::CompositionRefused),
                ),
            },
            StoredLaunchOutcomeV7 {
                launch_request_id: "v7-locked".into(),
                launch_outcome: LaunchOutcomeV7::ReplaceRejected(ReplaceRejectionV32::Locked),
            },
        ];
        {
            let mut engine = Engine::open(EngineOpen::new(fixture.path(), SchemaVersion::new(1)))
                .expect("outcome store opens");
            let outcomes = engine
                .register_table(TableDescriptor::new(
                    TableName::new("flow_nexus_launch_outcomes"),
                    FamilyName::new("flow-nexus-launch-outcome"),
                    SchemaHash::for_label("flow-nexus-launch-outcome-v7"),
                ))
                .expect("v7 outcome family registers");
            for row in rows {
                engine
                    .assert(Assertion::new(outcomes, row))
                    .expect("v7 outcome persists");
            }
        }
        let store = fixture.store();
        let unrecorded =
            StartRejection::CompositionRefused(signal_flow::CompositionRefusal::Unrecorded);
        for (request, outcome) in [
            ("v7-started", LaunchOutcome::Started(started)),
            (
                "v7-composition",
                LaunchOutcome::StartRejected(unrecorded.clone()),
            ),
            (
                "v7-topic",
                LaunchOutcome::StartRejected(StartRejection::TopicRefused(
                    "PrimaryCommitting".into(),
                )),
            ),
            (
                "v7-replace",
                LaunchOutcome::ReplaceRejected(ReplaceRejection::LaunchRefused(unrecorded)),
            ),
            (
                "v7-locked",
                LaunchOutcome::ReplaceRejected(ReplaceRejection::Locked),
            ),
        ] {
            assert_eq!(
                store
                    .launch_outcome(request)
                    .expect("upgraded outcome reads"),
                Some(outcome),
                "the v7 outcome for {request} survives the family upgrade"
            );
        }
    }

    /// A 0.35.22 to 0.39 store's outcomes, each rejection in signal-flow
    /// 30's shape, open as the current outcomes: a context refusal archived
    /// before RecordMissing keeps its meaning though its discriminant moves.
    #[test]
    fn v6_launch_outcomes_upgrade_to_the_topic_and_record_shape() {
        let fixture = StoreFixture::new();
        let started = LaunchedRecord {
            flow_id: "started-flow".into(),
            session_id: "started-session".into(),
            origin: Origin::Unbound,
        };
        let rows = vec![
            StoredLaunchOutcomeV6 {
                launch_request_id: "v6-started".into(),
                launch_outcome: LaunchOutcomeV6::Started(started.clone()),
            },
            StoredLaunchOutcomeV6 {
                launch_request_id: "v6-system".into(),
                launch_outcome: LaunchOutcomeV6::StartRejected(StartRejectionV30::ContextRefused(
                    ContextRefusalV30::SystemTooLarge,
                )),
            },
            StoredLaunchOutcomeV6 {
                launch_request_id: "v6-module".into(),
                launch_outcome: LaunchOutcomeV6::StartRejected(StartRejectionV30::ContextRefused(
                    ContextRefusalV30::ModuleTooLarge,
                )),
            },
            StoredLaunchOutcomeV6 {
                launch_request_id: "v6-undeclared".into(),
                launch_outcome: LaunchOutcomeV6::ReplaceRejected(
                    ReplaceRejectionV30::LaunchRefused(StartRejectionV30::Undeclared),
                ),
            },
            StoredLaunchOutcomeV6 {
                launch_request_id: "v6-reap".into(),
                launch_outcome: LaunchOutcomeV6::ReplaceRejected(ReplaceRejectionV30::ReapRefused(
                    StopRejectionV38::CloseRefused,
                )),
            },
        ];
        {
            let mut engine = Engine::open(EngineOpen::new(fixture.path(), SchemaVersion::new(1)))
                .expect("outcome store opens");
            let outcomes = engine
                .register_table(TableDescriptor::new(
                    TableName::new("flow_nexus_launch_outcomes"),
                    FamilyName::new("flow-nexus-launch-outcome"),
                    SchemaHash::for_label("flow-nexus-launch-outcome-v6"),
                ))
                .expect("v6 outcome family registers");
            for row in rows {
                engine
                    .assert(Assertion::new(outcomes, row))
                    .expect("v6 outcome persists");
            }
        }
        let store = fixture.store();
        for (request, outcome) in [
            ("v6-started", LaunchOutcome::Started(started)),
            (
                "v6-system",
                LaunchOutcome::StartRejected(StartRejection::ContextRefused(
                    signal_flow::ContextRefusal::SystemTooLarge,
                )),
            ),
            (
                "v6-module",
                LaunchOutcome::StartRejected(StartRejection::ContextRefused(
                    signal_flow::ContextRefusal::ModuleTooLarge,
                )),
            ),
            (
                "v6-undeclared",
                LaunchOutcome::ReplaceRejected(ReplaceRejection::LaunchRefused(
                    StartRejection::Undeclared,
                )),
            ),
            (
                "v6-reap",
                LaunchOutcome::ReplaceRejected(ReplaceRejection::ReapRefused(
                    StopRejection::CloseRefused(signal_flow::PaneCloseFailure::IdentityUnavailable),
                )),
            ),
        ] {
            assert_eq!(
                store
                    .launch_outcome(request)
                    .expect("upgraded outcome reads"),
                Some(outcome),
                "the v6 outcome for {request} survives the family upgrade"
            );
        }
    }

    /// A kept outcome that refused a Start for an ended Address names a
    /// state no metaflow has any more: the open refuses it rather than give
    /// it another reason, and a later open finds it kept and refuses again.
    #[test]
    fn a_v5_outcome_refused_for_an_ended_address_refuses_the_open() {
        let fixture = StoreFixture::new();
        seed_v5_launch_outcomes(
            &fixture,
            vec![StoredLaunchOutcomeV5 {
                launch_request_id: "v5-ended".into(),
                launch_outcome: LaunchOutcomeV5::StartRejected(StartRejectionV22::AddressEnded),
            }],
        );
        for open in ["first", "second"] {
            let Err(error) = <FlowStore as OpensFlowStore>::open(&fixture.path()) else {
                panic!("an ended Address outcome opens at the {open} open")
            };
            assert!(
                error.to_string().contains("v5-ended"),
                "the {open} refusal names the kept outcome: {error}"
            );
        }
    }

    /// A 0.35.2 store's outcomes, each rejection in signal-flow 17's shape,
    /// open as the current outcomes, and the store then keeps a refused
    /// harness start with its cause and its opened thread.
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
                        ReplaceRejectionV17::ReapRefused(StopRejectionV38::CloseRefused),
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
                    StopRejection::CloseRefused(signal_flow::PaneCloseFailure::IdentityUnavailable),
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

    /// The models the production store held at 0.35.11, one per Layer.
    fn production_models() -> Vec<(signal_flow::Layer, &'static str)> {
        use signal_flow::Layer;
        vec![
            (Layer::Primary, "claude-fable-5-1"),
            (Layer::Quaternary, "claude-haiku-5-5"),
            (Layer::Secondary, "claude-opus-5-5"),
            (Layer::Tertiary, "claude-sonnet-5-5"),
        ]
    }

    /// A store before 0.35.19 keeps one model per Layer, every one a
    /// Claude model. Opening carries each whole as that layer's Psyche
    /// model, keyed by its Layer and Aspect: declaring the same rows again
    /// changes nothing, Mind and Field have no model, and a second open
    /// reads the same rows.
    #[test]
    fn production_models_upgrade_each_as_its_layers_psyche_model() {
        use super::{ManagesMetaflowState, NamesConfiguredModel};
        use signal_flow::Address;
        let fixture = StoreFixture::new();
        drop(fixture.store());
        let models = production_models();
        fixture.seed_v1_models(1, &models);
        let expected = models
            .iter()
            .map(|(layer, native)| meta_signal_flow::ConfigureModel {
                layer: layer.clone(),
                aspect: Aspect::Psyche,
                model_name: (*native).into(),
                effort: "medium".into(),
            })
            .collect::<Vec<_>>();
        for reopening in 0..2 {
            let store = fixture.store();
            for (layer, native) in &models {
                for aspect in [Aspect::Psyche, Aspect::Mind, Aspect::Field] {
                    let address = Address {
                        aspect: aspect.clone(),
                        topic: "flow".into(),
                        layer: layer.clone(),
                    };
                    assert_eq!(
                        store.configured_model(&address).expect("model reads"),
                        (aspect == Aspect::Psyche).then(|| (*native).to_owned()),
                        "{aspect:?} {layer:?} (open {reopening})"
                    );
                }
            }
            assert_eq!(
                store
                    .configure_change(meta_signal_flow::ConfigurationChange::Models(
                        expected.clone()
                    ))
                    .expect("declaration commits"),
                meta_signal_flow::ConfigureOutcome::Unchanged,
                "every carried row, and no other, is under its Layer and Aspect (open {reopening})"
            );
        }
    }

    /// A Nexus declared before it named the Curriculum socket opens naming
    /// the socket Curriculum serves under the opening process's runtime
    /// directory, every other value as it was; the Nexus then opens on it.
    #[test]
    fn a_v1_nexus_declaration_opens_naming_the_default_curriculum_socket() {
        use super::nexus_configuration::{ConfigureNexusV1, StoredMetaNexusConfigurationV1};
        let fixture = StoreFixture::new();
        fixture.configured();
        drop(fixture.configured_store());
        let database = redb::Database::open(fixture.path()).expect("store opens directly");
        let write = database.begin_write().expect("catalog transaction starts");
        {
            let mut catalog = write
                .open_table(super::SEMA_CATALOG_TABLE)
                .expect("catalog opens");
            catalog
                .remove("flow_nexus_meta_nexus_configuration")
                .expect("nexus family removes");
        }
        let _ = write.delete_table(redb::TableDefinition::<&str, &[u8]>::new(
            "flow_nexus_meta_nexus_configuration",
        ));
        write.commit().expect("catalog commits");
        drop(database);
        {
            let mut engine = Engine::open(EngineOpen::new(
                fixture.path(),
                SchemaVersion::new(super::FLOW_SCHEMA_VERSION),
            ))
            .expect("store opens");
            let table = engine
                .register_table(TableDescriptor::new(
                    super::META_NEXUS_CONFIGURATION_TABLE_NAME,
                    FamilyName::new("flow-nexus-meta-nexus-configuration"),
                    SchemaHash::for_label("flow-nexus-meta-nexus-configuration-v1"),
                ))
                .expect("v1 nexus family registers");
            let endpoint = |name: &str| meta_signal_flow::CodexEndpoint {
                client_path: format!("/fixture/{name}-client"),
                home: format!("/fixture/{name}"),
                control_socket_path: format!("/fixture/{name}.sock"),
                model_name_vector: vec![format!("gpt-{name}")],
            };
            engine
                .assert(Assertion::new(
                    table,
                    StoredMetaNexusConfigurationV1 {
                        nexus: ConfigureNexusV1 {
                            source_root: "/fixture/source".into(),
                            stable_codex: endpoint("stable"),
                            next_codex: endpoint("next"),
                            harness_profiles: vec![],
                            meta_aspects: vec![Aspect::Psyche, Aspect::Mind, Aspect::Field],
                            message_nexus_path: "/fixture/message.sock".into(),
                            message_nexus_binary: "/fixture/message-nexus".into(),
                            lease: 45,
                        },
                    },
                ))
                .expect("v1 nexus row persists");
        }
        let configuration = ConfigurationOnlyStore::open(&fixture.path()).expect("opens");
        let nexus = configuration
            .configuration_snapshot()
            .expect("snapshot reads")
            .expect("store configured")
            .configured_nexus;
        drop(configuration);
        assert_eq!(nexus.source_root, "/fixture/source");
        assert_eq!(nexus.message_nexus_path, "/fixture/message.sock");
        assert_eq!(nexus.message_nexus_binary, "/fixture/message-nexus");
        assert_eq!(nexus.lease, 45);
        assert_eq!(nexus.flows_root, "/fixture/source/flows");
        assert!(
            nexus
                .curriculum_nexus_path
                .ends_with("/curriculum/curriculum.sock"),
            "{}",
            nexus.curriculum_nexus_path
        );
        let store = fixture.configured_store();
        assert_eq!(
            store
                .configuration_snapshot()
                .expect("snapshot reads")
                .expect("store configured")
                .configured_nexus,
            nexus
        );
    }

    /// The configuration-only open, which the deployment runs before the
    /// Nexus, carries a v1 model family the same way.
    #[test]
    fn the_configuration_only_open_upgrades_v1_models() {
        let fixture = StoreFixture::new();
        let defaults = super::DefaultConfiguration {
            home: fixture.directory.path().join("home"),
            runtime_directory: fixture.directory.path().join("runtime"),
        };
        let settings = defaults.configuration();
        let config = ConfigurationOnlyStore::open(&fixture.path()).expect("fresh Memory opens");
        config
            .configure_change(meta_signal_flow::ConfigurationChange::Nexus(
                meta_signal_flow::ConfigureNexus {
                    source_root: settings.source_root.clone(),
                    flows_root: "/tmp/flows".into(),
                    meta_aspects: settings.meta_aspects,
                    message_nexus_path: settings.message_nexus_path,
                    message_nexus_binary: "/fixture/message-nexus".into(),
                    curriculum_nexus_path: "/fixture/curriculum.sock".into(),
                    lease: 60,
                    executables: meta_signal_flow::Executables {
                        herdr: "herdr".into(),
                        flow_id_claimer: "flow-id".into(),
                        hook: "flow-hook".into(),
                    },
                    default_herdr_session: "default".into(),
                },
            ))
            .expect("Nexus configured");
        drop(config);
        let models = production_models();
        fixture.seed_v1_models(super::FLOW_SCHEMA_VERSION, &models);
        let configuration = ConfigurationOnlyStore::open(&fixture.path()).expect("opens");
        let carried = configuration
            .configuration_snapshot()
            .expect("snapshot reads")
            .expect("store configured")
            .configured_models;
        assert_eq!(
            carried,
            models
                .iter()
                .map(|(layer, native)| meta_signal_flow::ConfigureModel {
                    layer: layer.clone(),
                    aspect: Aspect::Psyche,
                    model_name: (*native).into(),
                    effort: "medium".into(),
                })
                .collect::<Vec<_>>()
        );
        assert_eq!(
            configuration
                .configure_change(meta_signal_flow::ConfigurationChange::Models(carried))
                .expect("declaration commits"),
            meta_signal_flow::ConfigureOutcome::Unchanged
        );
    }

    /// The leave-out defaults the production store held at 0.35.11, each
    /// under its Address alone: every Claude metaflow's twelve, every Codex
    /// metaflow's six.
    #[test]
    fn a_crash_between_family_upgrades_leaves_the_0_35_11_store() {
        use super::staged_upgrade::{InterruptsStagedUpgrade, StagedUpgrade, StagesUpgrade};
        use super::{DescribesModelTable, NamesConfiguredModel, UpgradesFlowFamilies};
        let fixture = StoreFixture::new();
        fixture.configured();
        drop(fixture.configured_store());
        let models = production_models();
        fixture.seed_v1_models(super::FLOW_SCHEMA_VERSION, &models);
        fixture.seed_v1_left_out(&production_leave_out_defaults());
        fixture.seed_v1_events(&[("a1a1a1", Vec::new())]);
        fixture.as_0_35_11();
        let bytes = std::fs::read(fixture.path()).expect("0.35.11 store reads");
        let staged = StagedUpgrade::begin(&fixture.path(), super::UpgradeScope::Running)
            .expect("staging starts")
            .expect("a 0.35.11 store has families to upgrade");
        {
            let mut engine = Engine::open(EngineOpen::new(
                staged.copy(),
                SchemaVersion::new(super::FLOW_SCHEMA_VERSION),
            ))
            .expect("the copy opens");
            engine
                .upgraded_models(super::StoredMetaModelConfiguration::descriptor())
                .expect("the models upgrade on the copy");
        }
        staged.crash();
        assert!(
            std::fs::read(fixture.path()).expect("store reads") == bytes,
            "the store is the 0.35.11 store it was"
        );
        assert_eq!(fixture.staged_copies().len(), 1, "the crash left its copy");
        let store = fixture.configured_store();
        let (layer, native) = &models[0];
        assert_eq!(
            store
                .configured_model(&signal_flow::Address {
                    aspect: Aspect::Psyche,
                    topic: "flow".into(),
                    layer: layer.clone(),
                })
                .expect("model reads"),
            Some((*native).to_owned())
        );
        drop(store);
        assert_eq!(fixture.staged_copies(), Vec::<String>::new());
    }

    /// The retired LeaveOut row and the declaration are one logical change:
    /// its mapped module names join the declaration on the staged copy, the
    /// old table disappears at activation, and reopening repeats nothing.
    #[test]
    fn retired_leave_outs_become_declaration_modules_once() {
        use super::metaflow_declaration::{
            DescribesMetaflowDeclarationTable, UpgradesMetaflowDeclarations,
        };
        let fixture = StoreFixture::new();
        fixture.configured();
        let address = signal_flow::Address {
            aspect: Aspect::Psyche,
            topic: "flow".into(),
            layer: Layer::Secondary,
        };
        {
            let descriptor = <super::metaflow_declaration::StoredMetaMetaflowConfiguration as DescribesMetaflowDeclarationTable>::descriptor();
            let mut engine = Engine::open(
                EngineOpen::new(
                    fixture.path(),
                    SchemaVersion::new(super::FLOW_SCHEMA_VERSION),
                )
                .with_family(&descriptor),
            )
            .expect("current declaration store opens");
            let table = engine
                .upgraded_metaflow_declarations(descriptor)
                .expect("declaration family is current");
            engine
                .assert(Assertion::new(
                    table,
                    super::metaflow_declaration::StoredMetaMetaflowConfiguration {
                        metaflow: meta_signal_flow::ConfigureMetaflow {
                            address: address.clone(),
                            system_modules: vec!["spirit-core".into()],
                            user_modules: vec![],
                            tool_set: signal_flow::ToolSet::Default,
                            continuity: meta_signal_flow::Continuity::OnDemand,
                            purpose: String::new(),
                            records: vec![],
                            left_out: vec!["AlreadyNamed".into()],
                            requires: vec![],
                        },
                    },
                ))
                .expect("current declaration persists");
        }
        fixture.seed_v1_left_out(&[(
            address.clone(),
            vec![LeaveOutV37::FileTools, LeaveOutV37::SkillTool],
        )]);

        let read = || {
            let store = fixture.configured_store();
            let snapshot = store
                .configuration_snapshot()
                .expect("snapshot reads")
                .expect("configuration exists");
            let row = snapshot
                .configured_metaflows
                .into_iter()
                .find(|row| row.address == address)
                .expect("declaration remains");
            drop(store);
            row.left_out
        };
        assert_eq!(
            read(),
            vec![
                "AlreadyNamed",
                "Read",
                "Edit",
                "Write",
                "Skill",
                "SlashCommands"
            ]
        );
        assert_eq!(
            read(),
            vec![
                "AlreadyNamed",
                "Read",
                "Edit",
                "Write",
                "Skill",
                "SlashCommands"
            ],
            "reopening is idempotent"
        );
        use redb::{ReadableDatabase as _, TableHandle as _};
        let database = redb::ReadOnlyDatabase::open(fixture.path()).expect("store reads");
        assert!(
            database
                .begin_read()
                .expect("read starts")
                .list_tables()
                .expect("tables list")
                .all(
                    |table| table.name() != super::META_LEAVE_OUT_CONFIGURATION_TABLE_NAME.as_str()
                ),
            "the retired LeaveOut table is gone"
        );
    }

    /// A declaration may omit only behavior that a configured harness
    /// actually exposes and that Flow can remove by text, tool, or switch.
    #[test]
    fn metaflow_left_out_names_are_known_and_removable() {
        let fixture = StoreFixture::new();
        fixture.configured();
        let config = ConfigurationOnlyStore::open(&fixture.path()).expect("configuration opens");
        let module =
            |name: &str, text: meta_signal_flow::ModuleText| meta_signal_flow::HarnessModule {
                harness_module_name: name.into(),
                module_text: text,
                tools: vec![],
                switches: vec![],
            };
        config
            .configure_change(meta_signal_flow::ConfigurationChange::Harnesses(vec![
                meta_signal_flow::Harness {
                    harness_name: "claude".into(),
                    harness_kind: HarnessKind::Claude,
                    harness_version: "fixture".into(),
                    executable: "/fixture/claude".into(),
                    home: "/fixture/home".into(),
                    control_socket_path_option: None,
                    served_models: vec![],
                    modules: vec![
                        module(
                            "Injected",
                            meta_signal_flow::ModuleText::Injected("text".into()),
                        ),
                        module(
                            "Fixed",
                            meta_signal_flow::ModuleText::Retained("text".into()),
                        ),
                    ],
                    left_out: vec![],
                    arguments: vec![],
                    settings: vec![],
                    parameters: vec![],
                    unset: vec![],
                    pane_profile: meta_signal_flow::PaneProfile {
                        command_sigil_vector: vec![],
                        interrupt_keys: vec![],
                        submit_keys: vec![],
                        compact_line: "/compact".into(),
                        composer: meta_signal_flow::Composer {
                            glyph: "❯".into(),
                            retraction: meta_signal_flow::Retraction::Keys(vec![]),
                            composer_lines: 1,
                            retracted_lines: 1,
                        },
                    },
                },
            ]))
            .expect("harness commits");
        let declaration = |name: &str| meta_signal_flow::ConfigureMetaflow {
            address: Address {
                aspect: Aspect::Psyche,
                topic: "flow".into(),
                layer: Layer::Secondary,
            },
            system_modules: vec![],
            user_modules: vec![],
            tool_set: signal_flow::ToolSet::Default,
            continuity: meta_signal_flow::Continuity::OnDemand,
            purpose: String::new(),
            records: vec![],
            left_out: vec![name.into()],
            requires: vec![],
        };
        assert!(matches!(
            config.configure_change(meta_signal_flow::ConfigurationChange::Metaflows(vec![declaration("Unknown")])),
            Err(super::StoreError::UnknownHarnessModule(name)) if name == "Unknown"
        ));
        assert!(matches!(
            config.configure_change(meta_signal_flow::ConfigurationChange::Metaflows(vec![declaration("Fixed")])),
            Err(super::StoreError::IrremovableHarnessModule(name)) if name == "Fixed"
        ));
        assert_eq!(
            config
                .configure_change(meta_signal_flow::ConfigurationChange::Metaflows(vec![
                    declaration("Injected")
                ]))
                .expect("removable module commits"),
            meta_signal_flow::ConfigureOutcome::Replaced
        );
    }

    /// A crash-left copy goes at the next open, one that upgrades nothing
    /// included.
    #[test]
    fn a_crash_left_copy_is_removed_by_an_open_that_upgrades_nothing() {
        use super::ConfiguresOnlyStore;
        let fixture = StoreFixture::new();
        fixture.configured();
        drop(fixture.configured_store());
        let leave = |fixture: &StoreFixture| {
            std::fs::write(
                fixture.directory.path().join(".flow.sema.upgrade-1-2"),
                b"x",
            )
            .expect("copy planted");
            assert_eq!(fixture.staged_copies().len(), 1);
        };
        leave(&fixture);
        drop(super::ConfigurationOnlyStore::open(&fixture.path()).expect("configuration open"));
        assert_eq!(fixture.staged_copies(), Vec::<String>::new());
        leave(&fixture);
        drop(fixture.configured_store());
        assert_eq!(fixture.staged_copies(), Vec::<String>::new());
    }

    /// A copy a live staging open holds is not removed, and the open that
    /// meets it is refused as the store held.
    #[test]
    fn a_copy_held_by_a_live_staging_open_stays_and_contention_is_a_lock_refusal() {
        use super::staged_upgrade::{StagedUpgrade, StagesUpgrade};
        let fixture = StoreFixture::new();
        fixture.configured();
        drop(fixture.configured_store());
        fixture.as_0_35_11();
        let staged = StagedUpgrade::begin(&fixture.path(), super::UpgradeScope::Running)
            .expect("staging starts")
            .expect("a 0.35.11 store has families to upgrade");
        assert_eq!(fixture.staged_copies().len(), 1);
        let refused = StagedUpgrade::begin(&fixture.path(), super::UpgradeScope::Running);
        assert!(
            matches!(refused, Err(super::StoreError::StoreHeld { .. })),
            "contention is a lock refusal"
        );
        assert_eq!(fixture.staged_copies().len(), 1, "the held copy stays");
        drop(staged);
        assert_eq!(fixture.staged_copies(), Vec::<String>::new());
    }
}

/// A v3 launch attempt row, written in its old shape, reads as v4 with its
/// remembered flows as Custom.
#[cfg(test)]
mod v3_attempts {
    use super::{
        LaunchAttemptV3, LaunchContext, LaunchProfileV3, MemoryDefinition, OpensFlowStore,
        ReadsLaunchAttempt, RememberedFlow, StoredLaunchAttemptV3,
    };
    use sema_engine::{
        Assertion, Engine, EngineOpen, FamilyName, SchemaVersion, TableDescriptor, TableName,
    };
    use signal_flow::{Address, Aspect, LaunchAttemptPhase, Layer, Origin, Remembering};

    fn v3_attempt(request: &str, remembered: Vec<RememberedFlow>) -> StoredLaunchAttemptV3 {
        StoredLaunchAttemptV3 {
            attempt: LaunchAttemptV3 {
                launch_request_id: request.into(),
                launch_profile: LaunchProfileV3 {
                    launch_request_id: request.into(),
                    system_modules: vec!["spirit-core".into()],
                    user_modules: vec!["operation-main-flow".into()],
                    address: Address {
                        aspect: Aspect::Mind,
                        topic: "flow".into(),
                        layer: Layer::Secondary,
                    },
                    model_name: "gpt-6-sol".into(),
                    effort: "medium".into(),
                    flow_id_option: None,
                    remembered_flow_vector: remembered,
                    herdr_session_name: "default".into(),
                    left_out_option: None,
                    flow_kind: super::metaflow_declaration::FlowKindV4::Regular,
                    tool_set: signal_flow::ToolSet::Default,
                    instruction_prompt: "Carry on.".into(),
                },
                prompt_sha256: "0".repeat(64),
                origin: Origin::Unbound,
                launch_attempt_phase: LaunchAttemptPhase::Reserved,
                native_launch_intent_option: None,
                native_launch_binding_option: None,
                registration_acknowledgement_option: None,
                prompt_delivery_intent_option: None,
                prompt_delivery_result_option: None,
            },
            context: LaunchContext::Modules,
        }
    }

    #[test]
    fn a_v3_launch_attempt_upgrades_to_v4_with_its_remembering() {
        let directory = tempfile::tempdir().expect("store directory");
        let path = directory.path().join("flow.sema");
        {
            let mut engine =
                Engine::open(EngineOpen::new(&path, SchemaVersion::new(1))).expect("store opens");
            let attempts = engine
                .register_table(TableDescriptor::new(
                    TableName::new("flow_nexus_launch_attempts"),
                    FamilyName::new("flow-nexus-launch-attempt"),
                    StoredLaunchAttemptV3::schema_hash(),
                ))
                .expect("v3 attempt family registers");
            engine
                .assert(Assertion::new(attempts, v3_attempt("v3-none", vec![])))
                .expect("v3 attempt persists");
            engine
                .assert(Assertion::new(
                    attempts,
                    v3_attempt(
                        "v3-named",
                        vec![
                            RememberedFlow {
                                flow_id: "36b71b".into(),
                                remembering_depth: 1,
                            },
                            RememberedFlow {
                                flow_id: "e3c8c1".into(),
                                remembering_depth: 2,
                            },
                        ],
                    ),
                ))
                .expect("v3 attempt persists");
        }
        let store = <super::FlowStore as OpensFlowStore>::open(&path).expect("store reopens");
        let none = store
            .launch_attempt("v3-none")
            .expect("upgraded attempt reads")
            .expect("the v3 attempt is kept");
        assert_eq!(
            none.launch_profile.remembering,
            Remembering::Predecessors(0)
        );
        let named = store
            .launch_attempt("v3-named")
            .expect("upgraded attempt reads")
            .expect("the v3 attempt is kept");
        assert_eq!(
            named.launch_profile.remembering,
            Remembering::Custom(vec!["36b71b".into(), "e3c8c1".into()])
        );
        assert_eq!(named.launch_profile.address.topic, "flow");
    }

    #[test]
    fn a_threshold_upgrade_preserves_its_token_count() {
        use sema_engine::{Decision, UpgradeFrom};
        let previous = super::StoredMetaThresholdConfigurationV1 {
            threshold: super::ConfigureThresholdV1 {
                model_name: "gpt-sol".into(),
                refresh: 150_000,
            },
        };
        let Decision::Ready(current) =
            super::StoredMetaThresholdConfiguration::upgrade_from(previous)
        else {
            panic!("threshold upgrade is total")
        };
        assert_eq!(current.threshold.refresh_tokens, 150_000);
    }

    #[test]
    fn a_power_role_upgrades_to_the_exact_layer() {
        use sema_engine::{Decision, UpgradeFrom};
        let previous = super::StoredRoleV1 {
            caller: super::CallerV1 {
                flow_id: "908786".into(),
                aspect: Aspect::Mind,
                power_level: super::PowerLevel::Medium,
                model_name: "gpt-sol".into(),
            },
        };
        let Decision::Ready(current) = super::StoredRole::upgrade_from(previous);
        assert_eq!(current.caller.layer, Layer::Secondary);
    }

    #[test]
    fn a_power_named_flow_type_upgrades_to_the_layer_name() {
        use sema_engine::{Decision, UpgradeFrom};
        let previous = super::FlowRecordV6 {
            flow_id: "908786".into(),
            flow_type: "Mind:Medium:gpt-sol".into(),
            origin: Origin::Unbound,
            thread_id: None,
            harness_kind: signal_flow::HarnessKind::Codex,
            endpoint_selection: signal_flow::EndpointSelection::Unavailable,
            lifecycle: super::FlowLifecycle::Active,
            generation: 1,
        };
        let Decision::Ready(current) = super::FlowRecord::upgrade_from(previous);
        assert_eq!(current.flow_type, "Mind:Secondary:gpt-sol");
    }
}
