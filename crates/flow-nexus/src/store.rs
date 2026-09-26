//! Durable Flow Nexus identity and dispatch state.
//!
//! The ordinary Signal contract remains the public boundary.  This module
//! owns its single `.sema` store and lowers a closed `signal_flow::Query`
//! into its typed, durable records.

use std::{
    os::unix::fs::MetadataExt,
    path::{Path, PathBuf},
    sync::{Arc, Condvar, Mutex},
};

use meta_signal_flow::Configuration;
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use sema_engine::{
    Assertion, Engine, EngineOpen, EngineRecord, FamilyName, KeyedMutation, QueryPlan, RecordKey,
    Retraction, SchemaHash, SchemaVersion, TableDescriptor, TableName, TableReference,
};
use signal_flow::{
    Caller, CallerResolutionRejection, ComposedLaunch, EndpointSelection, FlowAspect,
    FlowLifecycle as SignalFlowLifecycle, FlowNode, HarnessKind, HerdrRoute, HerdrRouteSelection,
    LaunchAttempt, LaunchAttemptPhase, LaunchAttemptReservation, LaunchSource, NativeLaunchBinding,
    NativeLaunchIntent, OriginClue, PowerLevel, PromptDeliveryIntent, PromptDeliveryResult, Query,
    RecipientResolutionRejection, RegistrationAcknowledgement, RememberedFlow, ReplaceRejection,
    Replaced, Response, RestartRejection, Restarted, RouteReadiness, StartRejection, Started,
};

const FLOW_TABLE_NAME: TableName = TableName::new("flow_nexus_flows");
const FLOW_STATE_TABLE_NAME: TableName = TableName::new("flow_nexus_state");
const FLOW_CONFIGURATION_TABLE_NAME: TableName = TableName::new("flow_nexus_configuration");
const FLOW_HERDR_ROUTE_TABLE_NAME: TableName = TableName::new("flow_nexus_herdr_routes");
const FLOW_LAUNCH_ATTEMPT_TABLE_NAME: TableName = TableName::new("flow_nexus_launch_attempts");
const FLOW_RUNTIME_CONFIGURATION_TABLE_NAME: TableName =
    TableName::new("flow_nexus_runtime_configuration");
const FLOW_LAUNCH_OUTCOME_TABLE_NAME: TableName = TableName::new("flow_nexus_launch_outcomes");
const FLOW_REPLACEMENT_TABLE_NAME: TableName = TableName::new("flow_nexus_replacements");
const FLOW_ROLE_TABLE_NAME: TableName = TableName::new("flow_nexus_roles");
const FLOW_QUARANTINED_LAUNCH_ATTEMPT_TABLE_NAME: TableName =
    TableName::new("flow_nexus_quarantined_launch_attempts");
const STATE_KEY: &str = "identity";
const CONFIGURATION_KEY: &str = "configured";
const RUNTIME_CONFIGURATION_KEY: &str = "runtime";

/// The two anchors the default configuration is derived from: the user's
/// home (`HOME`, else the password database) and runtime directory
/// (`XDG_RUNTIME_DIR`, else `/run/user/<uid>`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DefaultConfiguration {
    pub home: PathBuf,
    pub runtime_directory: PathBuf,
}

/// The executable's default configuration: every path is anchored on the
/// user's home or runtime directory, and nothing names a particular user.
impl DefaultConfiguration {
    const STATE_DIRECTORY: &str = ".local/state/flow";
    const STORE_FILE: &str = "flow.sema";
    const LAUNCH_BUNDLE_DIRECTORY: &str = "launch-bundles";
    const SOCKET_DIRECTORY: &str = "flow";
    const ORDINARY_SOCKET: &str = "flow.sock";
    const META_SOCKET: &str = "flow-meta.sock";
    const SOURCE_ROOT: &str = "primary";
    const STABLE_CODEX_CLIENT: &str = "codex-stable-flow-client";
    const STABLE_CODEX_HOME: &str = ".codex";
    const STABLE_CODEX_MODELS: [&str; 3] = ["gpt-5.6-terra", "gpt-5.6-sol", "gpt-5.6-luna"];
    const NEXT_CODEX_CLIENT: &str = "codex-next-flow-client";
    const NEXT_CODEX_HOME: &str = ".codex-next";
    const NEXT_CODEX_MODELS: [&str; 3] = ["gpt-6-sol", "gpt-6-luna", "gpt-6-astra"];
    const CODEX_CONTROL_SOCKET: &str = "app-server-control/app-server-control.sock";

    pub fn from_environment() -> Self {
        let user_id = std::fs::metadata("/proc/self")
            .map(|metadata| metadata.uid())
            .unwrap_or(0);
        let home = std::env::var_os("HOME")
            .map(PathBuf::from)
            .filter(|home| home.is_absolute())
            .or_else(|| Self::password_database_home(user_id))
            .unwrap_or_else(|| PathBuf::from("/"));
        let runtime_directory = std::env::var_os("XDG_RUNTIME_DIR")
            .map(PathBuf::from)
            .filter(|directory| directory.is_absolute())
            .unwrap_or_else(|| PathBuf::from(format!("/run/user/{user_id}")));
        Self {
            home,
            runtime_directory,
        }
    }

    fn password_database_home(user_id: u32) -> Option<PathBuf> {
        let database = std::fs::read_to_string("/etc/passwd").ok()?;
        database.lines().find_map(|line| {
            let fields = line.split(':').collect::<Vec<_>>();
            (fields.len() >= 6 && fields[2] == user_id.to_string())
                .then(|| PathBuf::from(fields[5]))
        })
    }

    pub fn state_directory(&self) -> PathBuf {
        self.home.join(Self::STATE_DIRECTORY)
    }

    /// Where the Nexus writes each launch's own copy of the system-prompt
    /// bundle.
    pub fn launch_bundle_directory(&self) -> PathBuf {
        self.state_directory().join(Self::LAUNCH_BUNDLE_DIRECTORY)
    }

    pub fn store_path(&self) -> PathBuf {
        self.state_directory().join(Self::STORE_FILE)
    }

    pub fn socket_directory(&self) -> PathBuf {
        self.runtime_directory.join(Self::SOCKET_DIRECTORY)
    }

    pub fn configuration(&self) -> Configuration {
        self.socket_configuration()
            .with_runtime(&self.runtime_configuration())
    }

    fn socket_configuration(&self) -> FlowStoreConfiguration {
        let directory = self.socket_directory();
        FlowStoreConfiguration {
            ordinary_socket_path: directory
                .join(Self::ORDINARY_SOCKET)
                .to_string_lossy()
                .into_owned(),
            meta_socket_path: directory
                .join(Self::META_SOCKET)
                .to_string_lossy()
                .into_owned(),
        }
    }

    pub fn runtime_configuration(&self) -> RuntimeConfiguration {
        RuntimeConfiguration {
            source_root: self
                .home
                .join(Self::SOURCE_ROOT)
                .to_string_lossy()
                .into_owned(),
            stable_codex: self.codex_endpoint(
                Self::STABLE_CODEX_CLIENT,
                Self::STABLE_CODEX_HOME,
                &Self::STABLE_CODEX_MODELS,
            ),
            next_codex: self.codex_endpoint(
                Self::NEXT_CODEX_CLIENT,
                Self::NEXT_CODEX_HOME,
                &Self::NEXT_CODEX_MODELS,
            ),
        }
    }

    fn codex_endpoint(
        &self,
        client: &str,
        home: &str,
        models: &[&str],
    ) -> CodexEndpointConfiguration {
        let home = self.home.join(home);
        CodexEndpointConfiguration {
            client_path: client.into(),
            socket: home
                .join(Self::CODEX_CONTROL_SOCKET)
                .to_string_lossy()
                .into_owned(),
            home: home.to_string_lossy().into_owned(),
            model_names: models.iter().map(|model| (*model).to_owned()).collect(),
        }
    }
}

/// Launch configuration held in the Nexus's Sema store beside the socket
/// paths. Together they are the meta `Configuration`: seeded from
/// `DefaultConfiguration`, replaced by meta `Configure`, and still
/// overridable by the deployment through `DeploymentOverrides`.
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

/// Deployment-supplied values that replace stored runtime configuration.
/// Exception to meta-socket-only configuration, taken at this site: the meta
/// `Configure` contract lacks the source root and Codex endpoint fields, so
/// the deployment's `FLOW_SOURCE_ROOT` and `FLOW_CODEX_{STABLE,NEXT}_{CLIENT,
/// SOCKET,HOME,MODELS}` are accepted here until it carries them. Every one is
/// optional; an absent or malformed value leaves the stored value in force.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DeploymentOverrides {
    values: Vec<(String, String)>,
}

impl DeploymentOverrides {
    pub fn from_environment() -> Self {
        Self {
            values: std::env::vars()
                .filter(|(name, _)| name.starts_with("FLOW_"))
                .collect(),
        }
    }

    pub fn from_values(values: Vec<(String, String)>) -> Self {
        Self { values }
    }

    fn value(&self, name: &str) -> Option<&str> {
        self.values
            .iter()
            .find(|(candidate, _)| candidate == name)
            .map(|(_, value)| value.as_str())
    }

    fn absolute(&self, name: &str) -> Option<String> {
        let value = self.value(name)?;
        if Path::new(value).is_absolute() {
            Some(value.to_owned())
        } else {
            eprintln!("flow-nexus: ignoring {name}: not an absolute path");
            None
        }
    }

    fn models(&self, name: &str) -> Option<Vec<String>> {
        let models = self
            .value(name)?
            .split(',')
            .map(str::trim)
            .filter(|model| !model.is_empty())
            .map(str::to_owned)
            .collect::<Vec<_>>();
        if models.is_empty() {
            eprintln!("flow-nexus: ignoring {name}: selects no model");
            return None;
        }
        Some(models)
    }

    fn apply_endpoint(&self, prefix: &str, endpoint: &mut CodexEndpointConfiguration) {
        if let Some(client) = self.absolute(&format!("FLOW_CODEX_{prefix}_CLIENT")) {
            endpoint.client_path = client;
        }
        if let Some(socket) = self.absolute(&format!("FLOW_CODEX_{prefix}_SOCKET")) {
            endpoint.socket = socket;
        }
        if let Some(home) = self.absolute(&format!("FLOW_CODEX_{prefix}_HOME")) {
            endpoint.home = home;
        }
        if let Some(models) = self.models(&format!("FLOW_CODEX_{prefix}_MODELS")) {
            endpoint.model_names = models;
        }
    }

    pub fn apply(&self, mut configuration: RuntimeConfiguration) -> RuntimeConfiguration {
        if let Some(source_root) = self.absolute("FLOW_SOURCE_ROOT") {
            configuration.source_root = source_root;
        }
        self.apply_endpoint("STABLE", &mut configuration.stable_codex);
        self.apply_endpoint("NEXT", &mut configuration.next_codex);
        configuration
    }
}

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

impl FlowLifecycle {
    /// Whether the flow can still be reached: only a live seat can be sent
    /// to, resolved as a recipient, stopped or replaced. Stopped, Retired and
    /// Exited are all gone; they differ in who ended the flow.
    fn is_live(&self) -> bool {
        matches!(self, Self::Pending | Self::Active)
    }
}

impl EngineRecord for FlowRecord {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.flow_id.clone())
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

/// The socket half of the meta `Configuration`. Its archive is the one the
/// former two-field `Configuration` record had, so a store written before
/// the contract grew its runtime fields still reads.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
struct FlowStoreConfiguration {
    ordinary_socket_path: String,
    meta_socket_path: String,
}

impl FlowStoreConfiguration {
    fn with_runtime(self, runtime: &RuntimeConfiguration) -> Configuration {
        Configuration {
            ordinary_socket_path: self.ordinary_socket_path,
            meta_socket_path: self.meta_socket_path,
            source_root: runtime.source_root.clone(),
            stable_codex: meta_signal_flow::CodexEndpoint::from(&runtime.stable_codex),
            next_codex: meta_signal_flow::CodexEndpoint::from(&runtime.next_codex),
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

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
struct StoredLaunchAttempt {
    attempt: LaunchAttempt,
}

impl EngineRecord for StoredLaunchAttempt {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.attempt.launch_request_id.clone())
    }
}

/// A launch attempt as Flow archived it before signal-flow 3.0.0 (ab70332)
/// added `SystemPromptBundleFile` to the launch profile: the same fields in
/// the same order without it. Rows of this shape are read only to carry them
/// forward; nothing is written in it.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
struct StoredLaunchAttemptBeforeBundle {
    attempt: LaunchAttemptBeforeBundle,
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
struct LaunchAttemptBeforeBundle {
    launch_request_id: String,
    launch_profile: LaunchProfileBeforeBundle,
    prompt_sha256: String,
    origin_clue: OriginClue,
    launch_attempt_phase: LaunchAttemptPhase,
    native_launch_intent_option: Option<NativeLaunchIntent>,
    native_launch_binding_option: Option<NativeLaunchBinding>,
    registration_acknowledgement_option: Option<RegistrationAcknowledgement>,
    prompt_delivery_intent_option: Option<PromptDeliveryIntent>,
    prompt_delivery_result_option: Option<PromptDeliveryResult>,
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
struct LaunchProfileBeforeBundle {
    launch_request_id: String,
    launch_source_vector: Vec<LaunchSource>,
    skill_name_vector: Vec<String>,
    flow_aspect: FlowAspect,
    power_level: PowerLevel,
    harness_kind: HarnessKind,
    model_name: String,
    effort: String,
    flow_id_option: Option<String>,
    remembered_flow_vector: Vec<RememberedFlow>,
    herdr_session_name: String,
    instruction_prompt: String,
}

/// A launch-attempt row named to the engine only so it can be retracted when
/// no known shape reads it. Its archive is empty, so it checks against any
/// bytes; it is never asserted and never read for content. An exception to
/// typed rows, taken only for moving such a row aside.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
struct UnreadLaunchAttempt;

/// The shape a launch-attempt row that no longer reads in the current shape
/// was found to have.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub enum ArchivedLaunchAttemptShape {
    /// Written before signal-flow 3.0.0 added `SystemPromptBundleFile`. It is
    /// carried forward with an empty `SystemPromptBundleFile`, the one field
    /// its archive never held.
    BeforeSystemPromptBundle,
    /// No known shape reads it; the text is the current shape's decode error.
    Unknown(String),
}

/// A launch-attempt row moved out of the attempts table when the store
/// opened, kept whole: its stored key, its original archive bytes, and the
/// shape it was found to have. A row of a known shape is also carried forward
/// into the attempts table; an Unknown one stays only here.
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

/// What opening the store did with a launch-attempt row that no longer read
/// in the current shape. Its Display is the line the Nexus logs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LaunchAttemptSettlement {
    /// Carried forward from a known earlier shape into the current one.
    Migrated {
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
            Self::Migrated {
                launch_request_id,
                from,
            } => write!(
                formatter,
                "LaunchAttemptMigrated.{{ {launch_request_id} {} }}",
                match from {
                    ArchivedLaunchAttemptShape::BeforeSystemPromptBundle => {
                        "BeforeSystemPromptBundle"
                    }
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

impl From<LaunchAttemptBeforeBundle> for LaunchAttempt {
    fn from(attempt: LaunchAttemptBeforeBundle) -> Self {
        let profile = attempt.launch_profile;
        Self {
            launch_request_id: attempt.launch_request_id,
            launch_profile: signal_flow::LaunchProfile {
                launch_request_id: profile.launch_request_id,
                launch_source_vector: profile.launch_source_vector,
                skill_name_vector: profile.skill_name_vector,
                flow_aspect: profile.flow_aspect,
                power_level: profile.power_level,
                harness_kind: profile.harness_kind,
                model_name: profile.model_name,
                effort: profile.effort,
                flow_id_option: profile.flow_id_option,
                remembered_flow_vector: profile.remembered_flow_vector,
                herdr_session_name: profile.herdr_session_name,
                system_prompt_bundle_file: String::new(),
                instruction_prompt: profile.instruction_prompt,
            },
            prompt_sha256: attempt.prompt_sha256,
            origin_clue: attempt.origin_clue,
            launch_attempt_phase: attempt.launch_attempt_phase,
            native_launch_intent_option: attempt.native_launch_intent_option,
            native_launch_binding_option: attempt.native_launch_binding_option,
            registration_acknowledgement_option: attempt.registration_acknowledgement_option,
            prompt_delivery_intent_option: attempt.prompt_delivery_intent_option,
            prompt_delivery_result_option: attempt.prompt_delivery_result_option,
        }
    }
}

/// How a launch request settled. It is kept beside the attempt so a
/// LaunchStatus or an Observe.Launch answers it without re-running the launch.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub enum LaunchOutcome {
    Started(Started),
    Replaced(Replaced),
    StartRejected(StartRejection),
    ReplaceRejected(ReplaceRejection),
}

impl LaunchOutcome {
    /// The reply the outcome is on the wire.
    pub fn response(&self) -> Response {
        match self {
            Self::Started(started) => Response::Started(started.clone()),
            Self::Replaced(replaced) => Response::Replaced(replaced.clone()),
            Self::StartRejected(rejection) => Response::StartRejected(rejection.clone()),
            Self::ReplaceRejected(rejection) => Response::ReplaceRejected(rejection.clone()),
        }
    }

    /// A refused reap leaves the predecessor Stopped and the successor held;
    /// it is the one outcome a repeated Replace takes up again.
    pub fn awaits_reaping(&self) -> bool {
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

/// A launch request that replaces a predecessor. While it stands without a
/// Replaced outcome, the flow its launch binds is held out of routing.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
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
/// attempt or outcome advances the count and wakes whoever waits on it.
/// Nothing re-reads on a timer; a waiter sleeps until a change is announced.
#[derive(Clone, Default)]
pub struct LaunchChanges {
    count: Arc<(Mutex<u64>, Condvar)>,
}

impl LaunchChanges {
    pub fn announce(&self) {
        let (count, changed) = &*self.count;
        *count
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) += 1;
        changed.notify_all();
    }

    pub fn current(&self) -> u64 {
        *self
            .count
            .0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Blocks until the count moves past `seen` or `deadline` passes.
    /// `true` when a change came, `false` when the deadline did. A caller
    /// that must eventually answer waits this way; one that runs for the
    /// life of the Nexus uses [`Self::after`].
    pub fn until(&self, seen: u64, deadline: std::time::Instant) -> bool {
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

    /// Blocks until the count moves past `seen`, then returns the new count.
    pub fn after(&self, seen: u64) -> u64 {
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
}

pub struct FlowStore {
    engine: Engine,
    flows: TableReference<FlowRecord>,
    state: TableReference<FlowStoreState>,
    configuration: TableReference<FlowStoreConfiguration>,
    runtime_configuration: TableReference<RuntimeConfiguration>,
    herdr_routes: TableReference<FlowHerdrRouteRecord>,
    launch_attempts: TableReference<StoredLaunchAttempt>,
    launch_outcomes: TableReference<StoredLaunchOutcome>,
    replacements: TableReference<Replacement>,
    roles: TableReference<StoredRole>,
    unread_launch_attempts: TableReference<UnreadLaunchAttempt>,
    quarantined_launch_attempts: TableReference<QuarantinedLaunchAttempt>,
    pub launch_changes: LaunchChanges,
    /// What opening did with launch-attempt rows that no longer read in the
    /// current shape; each is also logged.
    pub opening_settlements: Vec<LaunchAttemptSettlement>,
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
    /// Opens the store; a new store persists the given defaults and a
    /// populated store resumes what it holds.
    fn open_seeded(path: &Path, defaults: &DefaultConfiguration) -> Result<Self, StoreError>
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

    /// Lays deployment overrides over the stored runtime configuration,
    /// persisting the result only when it differs.
    fn adopt_overrides(
        &self,
        overrides: &DeploymentOverrides,
    ) -> Result<RuntimeConfiguration, StoreError> {
        let stored = self.runtime_configuration()?;
        let adopted = overrides.apply(stored.clone());
        if adopted != stored {
            self.configure_runtime(adopted.clone())?;
        }
        Ok(adopted)
    }
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
    /// The launch requests whose native launch bound this Flow ID.
    fn launch_requests_bound_to(&self, flow_id: &str) -> Result<Vec<String>, StoreError>;
    /// Every attempt whose first prompt is ambiguous and that has no outcome.
    fn ambiguous_launch_attempts(&self) -> Result<Vec<LaunchAttempt>, StoreError>;
}

/// Reads the durable rows used by the ordinary Send, Stop, and List requests.
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
    fn open_seeded(path: &Path, defaults: &DefaultConfiguration) -> Result<Self, StoreError> {
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
        let launch_attempts = engine.register_table(TableDescriptor::new(
            FLOW_LAUNCH_ATTEMPT_TABLE_NAME,
            FamilyName::new("flow-nexus-launch-attempt"),
            SchemaHash::for_label("flow-nexus-launch-attempt-v1"),
        ))?;
        let launch_outcomes = engine.register_table(TableDescriptor::new(
            FLOW_LAUNCH_OUTCOME_TABLE_NAME,
            FamilyName::new("flow-nexus-launch-outcome"),
            SchemaHash::for_label("flow-nexus-launch-outcome-v1"),
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
        // The same table, named for moving aside a row no shape reads.
        let unread_launch_attempts = engine.register_table(TableDescriptor::new(
            FLOW_LAUNCH_ATTEMPT_TABLE_NAME,
            FamilyName::new("flow-nexus-launch-attempt"),
            SchemaHash::for_label("flow-nexus-launch-attempt-v1"),
        ))?;
        let quarantined_launch_attempts = engine.register_table(TableDescriptor::new(
            FLOW_QUARANTINED_LAUNCH_ATTEMPT_TABLE_NAME,
            FamilyName::new("flow-nexus-quarantined-launch-attempt"),
            SchemaHash::for_label("flow-nexus-quarantined-launch-attempt-v1"),
        ))?;
        let mut store = Self {
            engine,
            flows,
            state,
            configuration,
            runtime_configuration,
            herdr_routes,
            launch_attempts,
            launch_outcomes,
            replacements,
            roles,
            unread_launch_attempts,
            quarantined_launch_attempts,
            launch_changes: LaunchChanges::default(),
            opening_settlements: Vec::new(),
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
        // Opening never fails on a launch-attempt row: each is read, carried
        // forward, or moved aside, and every such move is logged.
        store.opening_settlements = store.settle_launch_attempts().unwrap_or_else(|error| {
            vec![LaunchAttemptSettlement::Unsettled {
                store_error: error.to_string(),
            }]
        });
        for settlement in &store.opening_settlements {
            eprintln!("flow-nexus: {settlement}");
        }
        store.adopt_legacy_roles()?;
        Ok(store)
    }
}

impl AppliesFlowQuery for FlowStore {
    fn apply(&self, query: Query) -> Result<Response, StoreError> {
        match query {
            Query::Start(_) => Ok(Response::StartRejected(StartRejection::NativeLaunchRefused)),
            Query::Restart(_) => Ok(Response::RestartRejected(RestartRejection::ResumeRefused)),
            Query::ResolveRecipient(flow_id) => self.resolve_recipient(&flow_id),
            Query::Send(_) => Ok(Response::SendRejected(
                signal_flow::SendRejection::PersistenceRefused,
            )),
            Query::Stop(_) => Ok(Response::StopRejected(
                signal_flow::StopRejection::PersistenceRefused,
            )),
            Query::List(_) => Ok(Response::ListRejected(
                signal_flow::ListRejection::PersistenceRefused,
            )),
            Query::Replace(_) => Ok(Response::ReplaceRejected(ReplaceRejection::LaunchRefused(
                StartRejection::NativeLaunchRefused,
            ))),
            // The caller is read from the socket, which the store never sees.
            Query::ResolveCaller(_) => Ok(Response::CallerResolutionRejected(
                CallerResolutionRejection::CallerUnknown,
            )),
            Query::LaunchStatus(_) | Query::Observe(_) => Ok(Response::LaunchStatusRejected(
                signal_flow::LaunchStatusRejection::PersistenceRefused,
            )),
        }
    }
}

impl ReservesPendingStart for FlowStore {
    fn reserve_pending_start(&self, query: Query) -> Result<Option<PendingLaunch>, StoreError> {
        match query {
            Query::Start(request) => self
                .reserve_start("legacy-test-start".into(), request.origin_clue)
                .map(Some),
            Query::Restart(_)
            | Query::ResolveRecipient(_)
            | Query::Send(_)
            | Query::Stop(_)
            | Query::List(_)
            | Query::Replace(_)
            | Query::LaunchStatus(_)
            | Query::Observe(_)
            | Query::ResolveCaller(_) => Ok(None),
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
        Ok(self
            .stored_configuration()?
            .with_runtime(&self.runtime_configuration()?))
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
                ),
        )?;
        Ok(())
    }

    fn runtime_configuration(&self) -> Result<RuntimeConfiguration, StoreError> {
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

impl FlowStore {
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
            origin: flow_node.origin_clue.clone(),
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

    /// Reads every launch-attempt row by itself. A row that reads in the
    /// current shape stays. Any other row is moved aside whole into the
    /// quarantine table, in one commit with its retraction; a row of a known
    /// earlier shape is then carried forward into the current shape. A carry
    /// interrupted before it landed is completed at the next open, since the
    /// quarantined row still names its shape and no attempt holds its key.
    fn settle_launch_attempts(&self) -> Result<Vec<LaunchAttemptSettlement>, StoreError> {
        let mut settlements = Vec::new();
        for (launch_request_id, archive) in self.launch_attempt_archives()? {
            let Err(decode_error) =
                rkyv::from_bytes::<StoredLaunchAttempt, rkyv::rancor::Error>(&archive)
            else {
                continue;
            };
            let shape = match rkyv::from_bytes::<StoredLaunchAttemptBeforeBundle, rkyv::rancor::Error>(
                &archive,
            ) {
                Ok(_) => ArchivedLaunchAttemptShape::BeforeSystemPromptBundle,
                Err(_) => ArchivedLaunchAttemptShape::Unknown(decode_error.to_string()),
            };
            let quarantine_number = self
                .quarantined_launch_attempts_of(&launch_request_id)?
                .len() as u64
                + 1;
            self.engine.commit_atomic(
                self.engine
                    .begin_atomic_commit()
                    .retract(
                        self.unread_launch_attempts,
                        RecordKey::new(launch_request_id.clone()),
                    )
                    .assert(
                        self.quarantined_launch_attempts,
                        QuarantinedLaunchAttempt {
                            launch_request_id: launch_request_id.clone(),
                            quarantine_number,
                            archive,
                            shape: shape.clone(),
                        },
                    ),
            )?;
            if let ArchivedLaunchAttemptShape::Unknown(decode_error) = shape {
                settlements.push(LaunchAttemptSettlement::Quarantined {
                    launch_request_id,
                    decode_error,
                });
            }
        }
        for quarantined in self
            .engine
            .match_records(QueryPlan::all(self.quarantined_launch_attempts))?
            .records()
        {
            if quarantined.shape != ArchivedLaunchAttemptShape::BeforeSystemPromptBundle
                || self
                    .stored_launch_attempt(&quarantined.launch_request_id)?
                    .is_some()
            {
                continue;
            }
            let Ok(earlier) = rkyv::from_bytes::<
                StoredLaunchAttemptBeforeBundle,
                rkyv::rancor::Error,
            >(&quarantined.archive) else {
                continue;
            };
            self.engine.assert(Assertion::new(
                self.launch_attempts,
                StoredLaunchAttempt {
                    attempt: earlier.attempt.into(),
                },
            ))?;
            settlements.push(LaunchAttemptSettlement::Migrated {
                launch_request_id: quarantined.launch_request_id.clone(),
                from: quarantined.shape.clone(),
            });
        }
        if !settlements.is_empty() {
            self.launch_changes.announce();
        }
        Ok(settlements)
    }

    /// Every launch-attempt row as its stored key and archive bytes, read
    /// through the engine's read-only storage reader. Reading the archive
    /// undecoded is an exception taken only here: it is how a row that no
    /// shape reads is found and kept whole.
    fn launch_attempt_archives(&self) -> Result<Vec<(String, Vec<u8>)>, StoreError> {
        let definition =
            redb::TableDefinition::<String, &[u8]>::new(FLOW_LAUNCH_ATTEMPT_TABLE_NAME.as_str());
        Ok(self
            .engine
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
        launch_request_id: &str,
    ) -> Result<Vec<QuarantinedLaunchAttempt>, StoreError> {
        Ok(self
            .engine
            .match_records(QueryPlan::all(self.quarantined_launch_attempts))?
            .records()
            .iter()
            .filter(|quarantined| quarantined.launch_request_id == launch_request_id)
            .cloned()
            .collect())
    }

    /// A store written before roles were kept has flows and no role rows.
    /// Each such flow's role is taken from what the store already holds: the
    /// launch profile of the launch that bound it, else the flow type
    /// MetaBindExisting wrote as `<aspect>:<power>:<model>`. Parsing that
    /// text is an exception taken only here, to read rows written before the
    /// typed record existed; nothing new is written in that form for reading.
    fn adopt_legacy_roles(&self) -> Result<(), StoreError> {
        let flows = self
            .engine
            .match_records(QueryPlan::all(self.flows))?
            .records()
            .to_vec();
        // Settlement has already carried or moved aside every row that did
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
                        flow_aspect: attempt.launch_profile.flow_aspect.clone(),
                        power_level: attempt.launch_profile.power_level.clone(),
                        model_name: attempt.launch_profile.model_name.clone(),
                    })
            });
            let Some(role) = launched.or_else(|| flow.legacy_role()) else {
                continue;
            };
            self.engine
                .assert(Assertion::new(self.roles, StoredRole { caller: role }))?;
        }
        Ok(())
    }
}

impl FlowRecord {
    /// The role MetaBindExisting wrote into the flow type before roles had
    /// their own record; see `adopt_legacy_roles`.
    fn legacy_role(&self) -> Option<Caller> {
        let mut parts = self.flow_type.splitn(3, ':');
        let flow_aspect = match parts.next()? {
            "Psyche" => FlowAspect::Psyche,
            "Mind" => FlowAspect::Mind,
            "Field" => FlowAspect::Field,
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
            flow_aspect,
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
            role.flow_aspect, role.power_level, role.model_name
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

impl RecordsLaunchOutcome for FlowStore {
    fn record_launch_outcome(
        &self,
        launch_request_id: &str,
        outcome: LaunchOutcome,
    ) -> Result<(), StoreError> {
        let stored = StoredLaunchOutcome {
            launch_request_id: launch_request_id.into(),
            launch_outcome: outcome,
        };
        if self.launch_outcome(launch_request_id)?.is_some() {
            self.engine.mutate_keyed(KeyedMutation::new(
                self.launch_outcomes,
                RecordKey::new(launch_request_id),
                stored,
            ))?;
        } else {
            self.engine
                .assert(Assertion::new(self.launch_outcomes, stored))?;
        }
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
                && self
                    .launch_outcome(&stored.attempt.launch_request_id)?
                    .is_none()
            {
                ambiguous.push(stored.attempt.clone());
            }
        }
        Ok(ambiguous)
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

impl ReadsFlowRows for FlowStore {
    fn flow_node(&self, flow_id: &str) -> Result<Option<FlowNode>, StoreError> {
        let Some(flow) = self.flow(flow_id)? else {
            return Ok(None);
        };
        Ok(Some(FlowNode {
            flow_id: flow.flow_id,
            session_id: flow.thread_id.unwrap_or_default(),
            harness_kind: flow.harness_kind,
            endpoint_selection: flow.endpoint_selection,
            herdr_route_selection: self
                .herdr_route(flow_id)?
                .map(|record| HerdrRouteSelection::Available(record.route))
                .unwrap_or(HerdrRouteSelection::Unavailable),
            origin_clue: flow.origin,
            flow_lifecycle: match flow.lifecycle {
                FlowLifecycle::Pending => SignalFlowLifecycle::Pending,
                FlowLifecycle::Active => SignalFlowLifecycle::Active,
                FlowLifecycle::Stopped => SignalFlowLifecycle::Stopped,
                FlowLifecycle::Retired => SignalFlowLifecycle::Retired,
                FlowLifecycle::Exited => SignalFlowLifecycle::Exited,
            },
        }))
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
        self.engine.mutate_keyed(KeyedMutation::new(
            self.flows,
            RecordKey::new(flow_id),
            flow,
        ))?;
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
        self.engine.mutate_keyed(KeyedMutation::new(
            self.flows,
            RecordKey::new(flow_id),
            flow,
        ))?;
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
        self.engine.mutate_keyed(KeyedMutation::new(
            self.flows,
            RecordKey::new(flow_id),
            flow,
        ))?;
        Ok(true)
    }
}

impl WritesFlowStore for FlowStore {
    fn mutate_launch_attempt(&self, attempt: LaunchAttempt) -> Result<(), StoreError> {
        self.engine.mutate_keyed(KeyedMutation::new(
            self.launch_attempts,
            RecordKey::new(attempt.launch_request_id.clone()),
            StoredLaunchAttempt { attempt },
        ))?;
        self.launch_changes.announce();
        Ok(())
    }

    fn reserve_start(
        &self,
        flow_type: String,
        origin: OriginClue,
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
/// then), and one row no known shape reads.
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
impl EarlierLaunchAttemptRows {
    pub const EARLIER_REQUEST: &str = "flow06-earlier";
    pub const UNREADABLE_REQUEST: &str = "unreadable-request";

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
                launch_source_vector: vec![LaunchSource {
                    source_path: "skills/main-flow.md".into(),
                    source_sha256: "ab".repeat(32),
                }],
                skill_name_vector: vec!["main-flow".into()],
                flow_aspect: FlowAspect::Psyche,
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
            origin_clue: OriginClue {
                flow_id: "e51411".into(),
                session_id: "e51411-session".into(),
                turn_id: "handover-successor".into(),
            },
            launch_attempt_phase: LaunchAttemptPhase::NativeBound,
            native_launch_intent_option: Some(NativeLaunchIntent {
                launch_request_id: Self::EARLIER_REQUEST.into(),
                prompt_sha256: "cd".repeat(32),
                harness_kind: HarnessKind::Claude,
                model_name: "claude-opus-5-5".into(),
                effort: "medium".into(),
                skill_name_vector: vec!["main-flow".into()],
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

    /// Writes the rows into the store at `path` and closes it.
    pub fn seed(&self, path: &Path) {
        let mut store = FlowStore::open(path).expect("store to seed opens");
        let earlier = store
            .engine
            .register_table::<StoredLaunchAttemptBeforeBundle>(TableDescriptor::new(
                FLOW_LAUNCH_ATTEMPT_TABLE_NAME,
                FamilyName::new("flow-nexus-launch-attempt"),
                SchemaHash::for_label("flow-nexus-launch-attempt-v1"),
            ))
            .expect("earlier shape names the table");
        let unreadable = store
            .engine
            .register_table::<NotALaunchAttempt>(TableDescriptor::new(
                FLOW_LAUNCH_ATTEMPT_TABLE_NAME,
                FamilyName::new("flow-nexus-launch-attempt"),
                SchemaHash::for_label("flow-nexus-launch-attempt-v1"),
            ))
            .expect("unreadable shape names the table");
        store
            .engine
            .assert(Assertion::new(
                earlier,
                StoredLaunchAttemptBeforeBundle {
                    attempt: self.earlier_attempt(),
                },
            ))
            .expect("earlier row written");
        store
            .engine
            .assert(Assertion::new(
                unreadable,
                NotALaunchAttempt {
                    launch_request_id: Self::UNREADABLE_REQUEST.into(),
                },
            ))
            .expect("unreadable row written");
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
            origin_clue: self.earlier_attempt().origin_clue,
            flow_lifecycle: SignalFlowLifecycle::Pending,
        };
        assert!(matches!(
            store.register_flow(node).expect("earlier flow registered"),
            FlowRegistration::Registered(_)
        ));
    }
}

#[cfg(test)]
mod tests {
    use super::{
        AppliesFlowQuery, AuthorizesFlowRestart, ConfiguresFlowStore, ConfirmsStartedFlow,
        FlowStore, OpensFlowStore, ReadsFlowRoles, ReadsFlowStore, ReadsLaunchAttempt,
        RecordsNativeLaunchBinding, RecordsNativeLaunchIntent, RecordsPendingThread,
        RecordsPromptDeliveryIntent, RecordsPromptDeliveryResult,
        RecordsRegistrationAcknowledgement, RecordsRestartedFlow, RegistersExistingFlow,
        RegistersFlowIdentity, ReservesLaunchAttempt, ReservesPendingStart,
    };
    use meta_signal_flow::Configuration;
    use signal_flow::{
        ComposedLaunch, FirstPromptPayload, FlowAspect, HarnessKind, HerdrPaneBinding,
        LaunchAttemptPhase, LaunchAttemptReservation, LaunchProfile, NativeLaunchBinding,
        NativeLaunchIntent, NativeSkillSelection, NativeTargetReceipt, NativeTranscriptAbsence,
        NativeTranscriptBoundary, NativeTranscriptCursor, OriginClue, PowerLevel,
        PromptDeliveryIntent, PromptDeliveryResult, Query, RegistrationAcknowledgement, Response,
        Restarted, StartRequest, TargetReceiptRequest,
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
                system_prompt_bundle_file: "/tmp/flow-system-prompt.md".into(),
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
    fn deployment_overrides_replace_stored_runtime_values_only_when_valid() {
        let fixture = StoreFixture::new();
        let store = fixture.store();
        let overrides = super::DeploymentOverrides::from_values(vec![
            ("FLOW_SOURCE_ROOT".into(), "/srv/source".into()),
            (
                "FLOW_CODEX_STABLE_CLIENT".into(),
                "/opt/stable-client".into(),
            ),
            ("FLOW_CODEX_NEXT_MODELS".into(), "model-a, model-b".into()),
            ("FLOW_CODEX_NEXT_SOCKET".into(), "relative.sock".into()),
        ]);
        let before = store.runtime_configuration().expect("seeded");
        let adopted = store
            .adopt_overrides(&overrides)
            .expect("overrides adopted");
        assert_eq!(adopted.source_root, "/srv/source");
        assert_eq!(adopted.stable_codex.client_path, "/opt/stable-client");
        assert_eq!(adopted.next_codex.model_names, vec!["model-a", "model-b"]);
        assert_eq!(adopted.next_codex.socket, before.next_codex.socket);
        drop(store);
        assert_eq!(
            fixture.store().runtime_configuration().expect("persisted"),
            adopted
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
            origin_clue: signal_flow::OriginClue {
                flow_id: "field-owner".into(),
                session_id: "messaging-build".into(),
                turn_id: "meta-bind-existing".into(),
            },
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
                flow_aspect: FlowAspect::Mind,
                power_level: PowerLevel::Medium,
                model_name: "gpt-sol".into(),
            })
        );
        assert_eq!(store.role("registered-old").unwrap(), None);
    }

    #[test]
    fn earlier_launch_attempt_rows_are_migrated_or_quarantined_once_and_logged() {
        use super::{
            ArchivedLaunchAttemptShape, EarlierLaunchAttemptRows, LaunchAttemptSettlement,
            QueryPlan, ReadsFlowRows,
        };
        let fixture = StoreFixture::new();
        let path = fixture.directory.path().join("flow.sema");
        let store = fixture.store();
        let current = fixture.composed_launch("current-request", &"ef".repeat(32));
        store
            .reserve_launch_attempt(&current, fixture.origin())
            .expect("current row reserved");
        drop(store);
        let rows = EarlierLaunchAttemptRows {
            bound_flow_id: "launched-earlier".into(),
        };
        rows.seed(&path);

        let store = fixture.store();
        let quarantined = match store.opening_settlements.as_slice() {
            [
                LaunchAttemptSettlement::Quarantined {
                    launch_request_id: unreadable,
                    decode_error,
                },
                LaunchAttemptSettlement::Migrated {
                    launch_request_id: migrated,
                    from: ArchivedLaunchAttemptShape::BeforeSystemPromptBundle,
                },
            ] => {
                assert_eq!(unreadable, EarlierLaunchAttemptRows::UNREADABLE_REQUEST);
                assert_eq!(migrated, EarlierLaunchAttemptRows::EARLIER_REQUEST);
                decode_error.clone()
            }
            other => panic!("unexpected settlements: {other:?}"),
        };
        assert_eq!(
            store.opening_settlements[1].to_string(),
            "LaunchAttemptMigrated.{ flow06-earlier BeforeSystemPromptBundle }"
        );
        assert_eq!(
            store.opening_settlements[0].to_string(),
            format!("LaunchAttemptQuarantined.{{ unreadable-request «{quarantined}» }}")
        );
        // The migrated row reads in the current shape, whole but for the
        // one field its archive never held.
        let expected: signal_flow::LaunchAttempt = rows.earlier_attempt().into();
        assert_eq!(expected.launch_profile.system_prompt_bundle_file, "");
        assert_eq!(
            store
                .launch_attempt(EarlierLaunchAttemptRows::EARLIER_REQUEST)
                .unwrap(),
            Some(expected)
        );
        assert_eq!(
            store
                .launch_attempt("current-request")
                .unwrap()
                .map(|attempt| attempt.launch_profile),
            Some(current.launch_profile)
        );
        assert_eq!(
            store
                .launch_attempt(EarlierLaunchAttemptRows::UNREADABLE_REQUEST)
                .unwrap(),
            None
        );
        // Every whole-table read answers again.
        assert!(store.ambiguous_launch_attempts().is_ok());
        assert_eq!(store.flow_nodes().unwrap().len(), 1);
        assert_eq!(
            store.launch_requests_bound_to("launched-earlier").unwrap(),
            vec![EarlierLaunchAttemptRows::EARLIER_REQUEST.to_string()]
        );
        // Both rows are kept whole, each with the shape it was found to have.
        let kept = store
            .engine
            .match_records(QueryPlan::all(store.quarantined_launch_attempts))
            .unwrap()
            .records()
            .to_vec();
        assert_eq!(kept.len(), 2);
        assert!(kept.iter().any(|row| {
            row.launch_request_id == EarlierLaunchAttemptRows::EARLIER_REQUEST
                && row.shape == ArchivedLaunchAttemptShape::BeforeSystemPromptBundle
                && rkyv::from_bytes::<super::StoredLaunchAttemptBeforeBundle, rkyv::rancor::Error>(
                    &row.archive,
                )
                .is_ok_and(|earlier| earlier.attempt == rows.earlier_attempt())
        }));
        assert!(kept.iter().any(|row| row.launch_request_id
            == EarlierLaunchAttemptRows::UNREADABLE_REQUEST
            && row.shape == ArchivedLaunchAttemptShape::Unknown(quarantined.clone())));
        // The flow the earlier launch bound takes its role from that launch.
        assert_eq!(
            store.role("launched-earlier").unwrap(),
            Some(signal_flow::Caller {
                flow_id: "launched-earlier".into(),
                flow_aspect: FlowAspect::Psyche,
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
            2
        );
    }

    #[test]
    fn a_carry_interrupted_after_quarantine_completes_at_the_next_open() {
        use super::{
            ArchivedLaunchAttemptShape, EarlierLaunchAttemptRows, LaunchAttemptSettlement,
            QuarantinedLaunchAttempt, StoredLaunchAttemptBeforeBundle,
        };
        let fixture = StoreFixture::new();
        let rows = EarlierLaunchAttemptRows {
            bound_flow_id: "launched-earlier".into(),
        };
        let store = fixture.store();
        // As a crash would leave it: moved aside, not yet carried forward.
        store
            .engine
            .assert(sema_engine::Assertion::new(
                store.quarantined_launch_attempts,
                QuarantinedLaunchAttempt {
                    launch_request_id: EarlierLaunchAttemptRows::EARLIER_REQUEST.into(),
                    quarantine_number: 1,
                    archive: rkyv::to_bytes::<rkyv::rancor::Error>(
                        &StoredLaunchAttemptBeforeBundle {
                            attempt: rows.earlier_attempt(),
                        },
                    )
                    .unwrap()
                    .to_vec(),
                    shape: ArchivedLaunchAttemptShape::BeforeSystemPromptBundle,
                },
            ))
            .unwrap();
        drop(store);

        let store = fixture.store();
        assert_eq!(
            store.opening_settlements,
            vec![LaunchAttemptSettlement::Migrated {
                launch_request_id: EarlierLaunchAttemptRows::EARLIER_REQUEST.into(),
                from: ArchivedLaunchAttemptShape::BeforeSystemPromptBundle,
            }]
        );
        assert_eq!(
            store
                .launch_attempt(EarlierLaunchAttemptRows::EARLIER_REQUEST)
                .unwrap(),
            Some(rows.earlier_attempt().into())
        );
    }

    #[test]
    fn a_role_is_kept_with_its_binding_and_a_different_one_conflicts() {
        let fixture = StoreFixture::new();
        let store = fixture.store();
        let role = signal_flow::Caller {
            flow_id: "psyche-high".into(),
            flow_aspect: FlowAspect::Psyche,
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
