#![allow(dead_code, non_camel_case_types, non_snake_case)]
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct NexusRuntimeSettings {
    pub ordinary_socket_path: flow_ethos::OrdinarySocketPath,
    pub meta_socket_path: flow_ethos::MetaSocketPath,
    pub source_root: flow_ethos::SourceRoot,
    pub stable_codex: flow_ethos::StableCodex,
    pub next_codex: flow_ethos::NextCodex,
    pub harness_profile_vector: std::vec::Vec<flow_ethos::HarnessProfile>,
    pub meta_aspects: flow_ethos::MetaAspects,
    pub message_nexus_path_option: std::option::Option<flow_ethos::MessageNexusPath>,
    pub message_nexus_binary_option: std::option::Option<flow_ethos::MessageNexusBinary>,
    pub lease: flow_ethos::Lease,
}
#[rustfmt::skip]
pub type Native = String;
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct ConfigureModel {
    pub model: flow_ethos::Model,
    pub native: Native,
}
#[rustfmt::skip]
pub type Refresh = i64;
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct ConfigureThreshold {
    pub model_name: signal_flow::ModelName,
    pub refresh: Refresh,
}
#[rustfmt::skip]
pub type WarningShareLeft = i64;
#[rustfmt::skip]
pub type ReminderText = String;
#[rustfmt::skip]
pub type EveryMinutes = i64;
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct HandoverPolicy {
    pub model_name: signal_flow::ModelName,
    pub warning_share_left: WarningShareLeft,
    pub reminder_text: ReminderText,
    pub every_minutes: EveryMinutes,
}
#[rustfmt::skip]
pub type Repository = String;
#[rustfmt::skip]
pub type Path = String;
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum HandoverPoliciesConfiguration {
    Configured(std::vec::Vec<HandoverPolicy>),
    Unconfigured,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum HandoverArchiveConfiguration {
    Configured(Path),
    Unconfigured,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum HandoverCountConfiguration {
    Configured(i64),
    Unconfigured,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct ConfigureLeaveOut {
    pub address: flow_ethos::Address,
    pub harness_kind: signal_flow::HarnessKind,
    pub leave_outs: signal_flow::LeaveOuts,
}
#[rustfmt::skip]
pub type CurriculumNexusPath = String;
#[rustfmt::skip]
pub type PresentationPrompt = String;
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct ConfigureNexus {
    pub source_root: flow_ethos::SourceRoot,
    pub stable_codex: flow_ethos::StableCodex,
    pub next_codex: flow_ethos::NextCodex,
    pub harness_profiles: flow_ethos::HarnessProfiles,
    pub meta_aspects: flow_ethos::MetaAspects,
    pub message_nexus_path_option: std::option::Option<flow_ethos::MessageNexusPath>,
    pub message_nexus_binary_option: std::option::Option<flow_ethos::MessageNexusBinary>,
    pub curriculum_nexus_path: CurriculumNexusPath,
    pub lease: flow_ethos::Lease,
    pub presentation_prompt_option: std::option::Option<PresentationPrompt>,
}
#[rustfmt::skip]
pub type CodexAppServerBinary = String;
#[rustfmt::skip]
pub type McpSendServerBinary = String;
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct ConfigureSendServer {
    pub codex_app_server_binary: CodexAppServerBinary,
    pub mcp_send_server_binary: McpSendServerBinary,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum Continuity {
    OnDemand,
    Maintained,
}
#[rustfmt::skip]
pub type Purpose = String;
#[rustfmt::skip]
pub type Root = String;
#[rustfmt::skip]
pub type RecordPath = String;
#[rustfmt::skip]
pub type Records = std::vec::Vec<RecordPath>;
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct ConfigureMetaflow {
    pub address: flow_ethos::Address,
    pub root_option: std::option::Option<Root>,
    pub system_modules: signal_flow::SystemModules,
    pub user_modules: signal_flow::UserModules,
    pub flow_kind: signal_flow::FlowKind,
    pub tool_set: signal_flow::ToolSet,
    pub continuity: Continuity,
    pub purpose: Purpose,
    pub records: Records,
}
#[rustfmt::skip]
pub type HarnessFlags = String;
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct ConfigureHarnessFlags {
    pub address: flow_ethos::Address,
    pub harness_flags: HarnessFlags,
}
#[rustfmt::skip]
pub type LogicalDelivery = i64;
#[rustfmt::skip]
pub type QueueAttempt = i64;
#[rustfmt::skip]
pub type SendOutcome = i64;
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct ConfigureCounters {
    pub logical_delivery: LogicalDelivery,
    pub queue_attempt: QueueAttempt,
    pub send_outcome: SendOutcome,
}
#[rustfmt::skip]
pub type HeartbeatPeriodSeconds = i64;
#[rustfmt::skip]
pub type HeartbeatRecipients = std::vec::Vec<flow_ethos::Address>;
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct HeartbeatConfig {
    pub heartbeat_period_seconds: HeartbeatPeriodSeconds,
    pub heartbeat_recipients: HeartbeatRecipients,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum CountersRefusal {
    NotEmpty,
    Lowered(ConfigureCounters),
    NotRunning,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum ConfigurationChange {
    Models(std::vec::Vec<ConfigureModel>),
    Thresholds(std::vec::Vec<ConfigureThreshold>),
    HandoverPolicies(std::vec::Vec<HandoverPolicy>),
    HandoverArchive(Path),
    HandoverCount(i64),
    LeaveOuts(std::vec::Vec<ConfigureLeaveOut>),
    Nexus(ConfigureNexus),
    SendServers(std::vec::Vec<ConfigureSendServer>),
    Metaflows(std::vec::Vec<ConfigureMetaflow>),
    HarnessFlags(std::vec::Vec<ConfigureHarnessFlags>),
    Investigation(flow_ethos::Address),
    DefaultMinutes(flow_ethos::Minutes),
    Heartbeat(std::option::Option<HeartbeatConfig>),
    ProtectedAddresses(std::vec::Vec<flow_ethos::Address>),
    Counters(ConfigureCounters),
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum ConfigureOutcome {
    Replaced,
    Unchanged,
}
#[rustfmt::skip]
pub type Nexus = ConfigureNexus;
#[rustfmt::skip]
pub type Models = std::vec::Vec<ConfigureModel>;
#[rustfmt::skip]
pub type Thresholds = std::vec::Vec<ConfigureThreshold>;
#[rustfmt::skip]
pub type HandoverPolicies = HandoverPoliciesConfiguration;
#[rustfmt::skip]
pub type HandoverArchive = HandoverArchiveConfiguration;
#[rustfmt::skip]
pub type HandoverCount = HandoverCountConfiguration;
#[rustfmt::skip]
pub type DefaultLeaveOuts = std::vec::Vec<ConfigureLeaveOut>;
#[rustfmt::skip]
pub type SendServers = std::vec::Vec<ConfigureSendServer>;
#[rustfmt::skip]
pub type Metaflows = std::vec::Vec<ConfigureMetaflow>;
#[rustfmt::skip]
pub type ConfiguredHarnessFlags = std::vec::Vec<ConfigureHarnessFlags>;
#[rustfmt::skip]
pub type Investigation = std::option::Option<flow_ethos::Address>;
#[rustfmt::skip]
pub type DefaultMinutes = flow_ethos::Minutes;
#[rustfmt::skip]
pub type Heartbeat = std::option::Option<HeartbeatConfig>;
#[rustfmt::skip]
pub type ProtectedAddresses = std::vec::Vec<flow_ethos::Address>;
#[rustfmt::skip]
pub type Counters = ConfigureCounters;
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct ConfigurationSnapshot {
    pub nexus: Nexus,
    pub models: Models,
    pub thresholds: Thresholds,
    pub handover_policies: HandoverPolicies,
    pub handover_archive: HandoverArchive,
    pub handover_count: HandoverCount,
    pub default_leave_outs: DefaultLeaveOuts,
    pub send_servers: SendServers,
    pub metaflows: Metaflows,
    pub configured_harness_flags: ConfiguredHarnessFlags,
    pub investigation: Investigation,
    pub default_minutes: DefaultMinutes,
    pub heartbeat: Heartbeat,
    pub protected_addresses: ProtectedAddresses,
    pub counters: Counters,
}
#[rustfmt::skip]
pub type IdempotencyKey = String;
#[rustfmt::skip]
pub type CreditId = String;
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum CreditSelection {
    Next,
    Specific(CreditId),
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct ResetRequest {
    pub idempotency_key: IdempotencyKey,
    pub credit_selection: CreditSelection,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum ResetOutcome {
    Reset,
    NothingToReset,
    NoCredit,
    AlreadyRedeemed,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum HandoverConfigurationRefusal {
    InvalidWarningShareLeft,
    InvalidEveryMinutes,
    InvalidHandoverCount,
    NegativeHandoverCount,
    DuplicatePolicy,
    DuplicateArchive,
    StoreRefused,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum ConfigureRejection {
    HandoverConfigurationRefused(HandoverConfigurationRefusal),
    StoreRefused,
    DuplicateEntry,
    CodexModelsOverlap,
    LeaveOutNotForHarness,
    TopicRefused(flow_ethos::Topic),
    MinutesRefused(signal_flow::MinutesRefusal),
    CountersRefused(CountersRefusal),
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum ResetRejection {
    AdapterUnavailable,
    ProtocolRefused,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum FlowRegistrationRejection {
    StoreRefused,
    UnknownOrUnclaimedIdentity,
    ConflictingBinding,
}
#[rustfmt::skip]
pub type ProcessId = i64;
#[rustfmt::skip]
pub type ProcessUserId = i64;
#[rustfmt::skip]
pub type ProcessStartToken = String;
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct ProcessIdentity {
    pub process_id: ProcessId,
    pub process_user_id: ProcessUserId,
    pub process_start_token: ProcessStartToken,
}
#[rustfmt::skip]
pub type HerdrServerSocketPath = String;
#[rustfmt::skip]
pub type HerdrServerProcessIdentity = ProcessIdentity;
#[rustfmt::skip]
pub type MetaFlowOwnerId = signal_flow::FlowId;
#[rustfmt::skip]
pub type HerdrTabId = String;
#[rustfmt::skip]
pub type WorkingDirectory = String;
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum FlowLifecycle {
    RegisteredUnconfirmed,
    AdoptedActive,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct FlowContainer {
    pub herdr_session_name: signal_flow::HerdrSessionName,
    pub herdr_server_socket_path: HerdrServerSocketPath,
    pub herdr_server_process_identity: HerdrServerProcessIdentity,
    pub meta_flow_owner_id: MetaFlowOwnerId,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct FlowBinding {
    pub flow_id: signal_flow::FlowId,
    pub aspect: signal_flow::Aspect,
    pub model_name: signal_flow::ModelName,
    pub harness_kind: signal_flow::HarnessKind,
    pub native_session_id: signal_flow::NativeSessionId,
    pub herdr_workspace_id: signal_flow::HerdrWorkspaceId,
    pub herdr_pane_id: signal_flow::HerdrPaneId,
    pub herdr_tab_id: HerdrTabId,
    pub herdr_terminal_id: signal_flow::HerdrTerminalId,
    pub herdr_agent_name: signal_flow::HerdrAgentName,
    pub process_identity: ProcessIdentity,
    pub working_directory: WorkingDirectory,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct MetaBindExisting {
    pub flow_container: FlowContainer,
    pub address: flow_ethos::Address,
    pub flow_binding: FlowBinding,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct BoundFlowBinding {
    pub flow_id: signal_flow::FlowId,
    pub flow_lifecycle: FlowLifecycle,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum FlowBindingRefusalReason {
    AmbiguousPane,
    DeadProcess,
    DuplicateFlowId,
    AnatomyMismatch,
    PaneAbsent,
    ProcessMismatch,
    SessionMismatch,
    SessionUnreported,
    SessionUnproven,
    HerdrUnreadable,
    RoleMismatch,
    AddressOwnerLive,
    AddressOwnerUnknown,
    NativeIdentityMismatch,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct RefusedFlowBinding {
    pub flow_id: signal_flow::FlowId,
    pub flow_binding_refusal_reason: FlowBindingRefusalReason,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum FlowBindingResult {
    Bound(BoundFlowBinding),
    Refused(RefusedFlowBinding),
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct BoundExisting {
    pub flow_container: FlowContainer,
    pub flow_binding_result: FlowBindingResult,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum BindExistingRejection {
    ContainerUnavailable,
    ContainerIdentityMismatch,
    OwnerMismatch,
    StoreRefused,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum RetireRejection {
    UnknownFlow,
    AlreadyGone,
    StoreRefused,
    CloseRefused,
    Maintained,
    RestartUnresolved,
    Protected,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct OldSeat {
    pub herdr_route: signal_flow::HerdrRoute,
    pub process_option: std::option::Option<signal_flow::Process>,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum RestartFailure {
    CloseRefused,
    CloseUnproven,
    ResumeRefused(String),
    BindMismatch,
    Stopped,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum RestartPhase {
    Closing,
    Closed,
    Opening,
    Spawning(signal_flow::HerdrPaneBinding),
    Failed(RestartFailure),
    Interrupted(std::option::Option<signal_flow::HerdrPaneBinding>),
}
#[rustfmt::skip]
pub type Attempts = i64;
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct RestartRecord {
    pub flow_id: signal_flow::FlowId,
    pub native_session_id: signal_flow::NativeSessionId,
    pub address: flow_ethos::Address,
    pub old_seat: OldSeat,
    pub restart_phase: RestartPhase,
    pub attempts: Attempts,
}
#[rustfmt::skip]
pub type DeliveryId = String;
#[rustfmt::skip]
pub type CommandLine = String;
#[rustfmt::skip]
pub type ByteOffset = i64;
#[rustfmt::skip]
pub type PsycheContext = String;
#[rustfmt::skip]
pub type PsycheVerbatim = String;
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct Psyche_Data {
    pub psyche_context: PsycheContext,
    pub psyche_verbatim: PsycheVerbatim,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum Content {
    Waking(std::vec::Vec<signal_flow::VisibleNativeInput>),
    Text(String),
    Psyche(Psyche_Data),
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct Letter {
    pub visible_sender: signal_flow::VisibleSender,
    pub content: Content,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum Message {
    HardAbrupt(Letter),
    MiddleAbrupt(Letter),
    Soft(Letter),
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct DeliveryRequest {
    pub delivery_id: DeliveryId,
    pub flow_id: signal_flow::FlowId,
    pub message: Message,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum InterruptWitness {
    NotRequested,
    Observed,
    Unobserved,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum DeliveryGrade {
    Transported,
    Presented,
    Uncertain,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct Delivery {
    pub delivery_id: DeliveryId,
    pub flow_id: signal_flow::FlowId,
    pub interrupt_witness: InterruptWitness,
    pub delivery_grade: DeliveryGrade,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum BodyRefusal {
    EmptyBody,
    HarnessCommand(CommandLine),
    ControlCharacter(ByteOffset),
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum DeliveryRejection {
    UnknownFlow,
    FlowStopped,
    RouteUnavailable,
    RecipientWorking,
    RecipientBlocked,
    ComposerOccupied,
    BodyRefused(BodyRefusal),
    NotDelivered,
    PersistenceRefused,
    FlowRetired,
    FlowExited,
    PastThreshold,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum HarnessCommand {
    Compact,
    Interrupt,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct CommandRequest {
    pub flow_id: signal_flow::FlowId,
    pub harness_command: HarnessCommand,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum CommandGrade {
    Transported,
    Observed,
    Uncertain,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct CommandOutcome {
    pub flow_id: signal_flow::FlowId,
    pub harness_command: HarnessCommand,
    pub command_grade: CommandGrade,
}
#[rustfmt::skip]
pub type ContextTokens = i64;
#[rustfmt::skip]
pub type RefreshTokens = i64;
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct ContextBelowThreshold {
    pub context_tokens: ContextTokens,
    pub refresh_tokens: RefreshTokens,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum CompactionRefusal {
    BelowThreshold(ContextBelowThreshold),
    Unmeasured,
    NoThreshold,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum CommandRejection {
    UnknownFlow,
    FlowStopped,
    RouteUnavailable,
    RecipientBlocked,
    UnsupportedForHarness,
    NotDelivered,
    FlowRetired,
    FlowExited,
    CompactionRefused(CompactionRefusal),
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum MetaRefusal {
    PeerUnknown,
    PeerNotAuthorized(signal_flow::Caller),
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum StoreRefusal {
    Store(String),
}
#[rustfmt::skip]
pub type QueueAttemptId = String;
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum Undecided {
    HistoryUnreadable,
    HistoryReplaced,
    PendingInHarness,
    Ambiguous,
    SeatNotResting,
    AbsenceUnprovable,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum AbsenceProof {
    HandOverRefused,
    HistoryAbsent,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum QueueDeliveryStanding {
    Prepared,
    Uncertain(std::option::Option<Undecided>),
    ReceiptRecorded,
    Absent(AbsenceProof),
    Resolved(QueueResolutionRecord),
}
#[rustfmt::skip]
pub type QueueResolutionReason = String;
#[rustfmt::skip]
pub type UnixSeconds = i64;
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum QueueResolutionOutcome {
    Delivered,
    Absent,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct QueueDeliveryResolution {
    pub queue_attempt_id: QueueAttemptId,
    pub queue_resolution_outcome: QueueResolutionOutcome,
    pub queue_resolution_reason: QueueResolutionReason,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct ResolverIdentity {
    pub process_identity: ProcessIdentity,
    pub flow_id_option: std::option::Option<signal_flow::FlowId>,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct QueueResolutionRecord {
    pub queue_resolution_outcome: QueueResolutionOutcome,
    pub resolver_identity: ResolverIdentity,
    pub queue_resolution_reason: QueueResolutionReason,
    pub unix_seconds: UnixSeconds,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum ResolveQueueDeliveryRejection {
    UnknownAttempt,
    NotUncertain,
    NotOldest,
    EmptyReason,
    StoreRefused,
    NativeReceiptRequired,
    LegacyUncorrelated,
    AlreadyResolved(QueueResolutionRecord),
    AlreadyDelivered(signal_flow::NativeReceipt),
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct QueueDelivery {
    pub queue_attempt_id: QueueAttemptId,
    pub flow_id: signal_flow::FlowId,
    pub native_session_id: signal_flow::NativeSessionId,
    pub harness_kind: signal_flow::HarnessKind,
    pub queue_evidence: QueueEvidence,
    pub queue_delivery_standing: QueueDeliveryStanding,
}
#[rustfmt::skip]
pub type QueueRequestId = String;
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct LegacyArchiveEntry {
    pub queue_request_id: QueueRequestId,
    pub address: flow_ethos::Address,
    pub queue_evidence: QueueEvidence,
    pub queue_attempt_id_vector: std::vec::Vec<QueueAttemptId>,
}
#[rustfmt::skip]
pub type SendOutcomeId = String;
#[rustfmt::skip]
pub type RequestDigest = String;
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct QueueEvidence_Legacy_Data {
    pub sent_by: signal_flow::SentBy,
    pub request: signal_flow::Request,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum QueueEvidence {
    Logical(flow_ethos::QueueEntry),
    Legacy(QueueEvidence_Legacy_Data),
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum PriorSendOutcome {
    Queued,
    Delivered,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum OrdinarySendOutcome {
    Accepted(signal_flow::SendAcceptance),
    Refused(signal_flow::Refusal),
    PriorAdmission(PriorSendOutcome),
    Sent(signal_flow::SendReceipt),
    Failed(signal_flow::SendFailure),
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct SendOutcomeRecord {
    pub send_outcome_id: SendOutcomeId,
    pub process_option: std::option::Option<signal_flow::Process>,
    pub first_address_option: std::option::Option<flow_ethos::Address>,
    pub second_address_option: std::option::Option<flow_ethos::Address>,
    pub request_digest: RequestDigest,
    pub ordinary_send_outcome: OrdinarySendOutcome,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct Binding {
    pub flow_node_option: std::option::Option<signal_flow::FlowNode>,
    pub process_option: std::option::Option<signal_flow::Process>,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum Attempt {
    Queued(QueueDelivery),
    Launch(signal_flow::LaunchAttempt),
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum VisibleAttempt {
    Queued(VisibleQueueDelivery),
    Launch(signal_flow::LaunchAttempt),
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct VisibleMetaflow {
    pub address: flow_ethos::Address,
    pub state: flow_ethos::State,
    pub flow_id_vector: std::vec::Vec<signal_flow::FlowId>,
    pub visible_native_input_vector: std::vec::Vec<signal_flow::VisibleNativeInput>,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct VisibleDelivery {
    pub interrupt_witness: InterruptWitness,
    pub delivery_grade: DeliveryGrade,
}
#[rustfmt::skip]
pub type VisibleResolverIdentity = ProcessIdentity;
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct VisibleQueueResolutionRecord {
    pub queue_resolution_outcome: QueueResolutionOutcome,
    pub visible_resolver_identity: VisibleResolverIdentity,
    pub queue_resolution_reason: QueueResolutionReason,
    pub unix_seconds: UnixSeconds,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum VisibleQueueDeliveryStanding {
    Prepared,
    Uncertain(std::option::Option<Undecided>),
    ReceiptRecorded,
    Absent(AbsenceProof),
    Resolved(VisibleQueueResolutionRecord),
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct VisibleQueueEvidence_Legacy_Data {
    pub visible_sender: signal_flow::VisibleSender,
    pub visible_request: signal_flow::VisibleRequest,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum VisibleQueueEvidence {
    Logical(signal_flow::VisibleNativeInput),
    Legacy(VisibleQueueEvidence_Legacy_Data),
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct VisibleQueueDelivery {
    pub native_session_id: signal_flow::NativeSessionId,
    pub harness_kind: signal_flow::HarnessKind,
    pub visible_queue_evidence: VisibleQueueEvidence,
    pub visible_queue_delivery_standing: VisibleQueueDeliveryStanding,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct VisibleQueueDeliveryResolution {
    pub queue_resolution_outcome: QueueResolutionOutcome,
    pub queue_resolution_reason: QueueResolutionReason,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum VisibleResolveQueueDeliveryRejected {
    UnknownAttempt,
    NotUncertain,
    NotOldest,
    EmptyReason,
    StoreRefused,
    NativeReceiptRequired,
    LegacyUncorrelated,
    AlreadyResolved(VisibleQueueResolutionRecord),
    AlreadyDelivered(signal_flow::NativeReceipt),
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum VisibleOrdinarySendOutcome {
    Accepted(signal_flow::VisibleSendAcceptance),
    Refused(signal_flow::VisibleRefusal),
    PriorAdmission(PriorSendOutcome),
    Sent(signal_flow::VisibleSendReceipt),
    Failed(signal_flow::VisibleSendFailure),
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct VisibleSendOutcomeRecord {
    pub process_option: std::option::Option<signal_flow::Process>,
    pub first_address_option: std::option::Option<flow_ethos::Address>,
    pub second_address_option: std::option::Option<flow_ethos::Address>,
    pub request_digest: RequestDigest,
    pub visible_ordinary_send_outcome: VisibleOrdinarySendOutcome,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct VisibleLegacyArchiveEntry {
    pub address: flow_ethos::Address,
    pub visible_queue_evidence: VisibleQueueEvidence,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct DebugSnapshot {
    pub address: flow_ethos::Address,
    pub flow_id_vector: std::vec::Vec<signal_flow::FlowId>,
    pub logical_delivery_id_vector: std::vec::Vec<flow_ethos::LogicalDeliveryId>,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum Rebinding {
    Pane(signal_flow::HerdrPaneId),
    Flow(signal_flow::FlowId),
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct Rebind_Data {
    pub address: flow_ethos::Address,
    pub rebinding: Rebinding,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct Withdraw_Data {
    pub address: flow_ethos::Address,
    pub logical_delivery_id: flow_ethos::LogicalDeliveryId,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum Query {
    Configuration,
    Configure(ConfigurationChange),
    ConsumeReset(ResetRequest),
    RegisterFlow(signal_flow::FlowNode),
    MetaBindExisting(MetaBindExisting),
    Retire(signal_flow::FlowId),
    Restart(signal_flow::FlowId),
    Refresh(flow_ethos::Address),
    Deliver(DeliveryRequest),
    Vet(DeliveryRequest),
    Command(CommandRequest),
    ResolvePeer(ProcessIdentity),
    ReadEvents(signal_flow::FlowId),
    ReadQueueDeliveries(flow_ethos::Address),
    ReadSendOutcomes,
    ObserveLegacyArchive,
    Debug(flow_ethos::Address),
    ResolveQueueDelivery(QueueDeliveryResolution),
    Inspect(flow_ethos::Address),
    Rebind(Rebind_Data),
    Withdraw(Withdraw_Data),
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct EventsRead_Data {
    pub flow_id: signal_flow::FlowId,
    pub event_vector: std::vec::Vec<signal_flow::Event>,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum ReadEventsRejected_Data {
    UnknownFlow,
    StoreRefused,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct QueueDeliveriesRead_Data {
    pub address: flow_ethos::Address,
    pub visible_queue_delivery_vector: std::vec::Vec<VisibleQueueDelivery>,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum ReadQueueDeliveriesRejected_Data {
    UnknownAddress,
    StoreRefused,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum ReadSendOutcomesRejected_Data {
    StoreRefused,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct QueueDeliveryResolved_Data {
    pub address: flow_ethos::Address,
    pub visible_queue_delivery_resolution: VisibleQueueDeliveryResolution,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct Inspected_Data {
    pub visible_metaflow: VisibleMetaflow,
    pub binding: Binding,
    pub lock_vector: std::vec::Vec<flow_ethos::Lock>,
    pub visible_attempt_vector: std::vec::Vec<VisibleAttempt>,
    pub restart_record_option: std::option::Option<RestartRecord>,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum InspectRejected_Data {
    UnknownAddress,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum RebindRejected_Data {
    UnknownAddress,
    PaneAbsent,
    FlowUnknown,
    NotAwake,
    HerdrUnavailable,
    FlowTaken(flow_ethos::Address),
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum WithdrawRejected_Data {
    UnknownAddress,
    NotQueued,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum Response {
    Configured(ConfigureOutcome),
    Configuration(ConfigurationSnapshot),
    Unconfigured,
    ResetConsumed(ResetOutcome),
    FlowRegistered(signal_flow::FlowNode),
    ConfigureRejected(ConfigureRejection),
    ResetRejected(ResetRejection),
    FlowRegistrationRejected(FlowRegistrationRejection),
    BoundExisting(BoundExisting),
    BindExistingRejected(BindExistingRejection),
    FlowRetired(signal_flow::FlowNode),
    RetireRejected(RetireRejection),
    Restarted(signal_flow::FlowId),
    RestartRefused(signal_flow::RestartRefusal),
    Refreshed(signal_flow::Refreshed),
    RefreshRejected(signal_flow::RefreshRejection),
    OrdinaryRefused(signal_flow::Refusal),
    Delivered(VisibleDelivery),
    DeliveryRejected(DeliveryRejection),
    Vetted,
    Commanded(CommandOutcome),
    CommandRejected(CommandRejection),
    PeerResolved(signal_flow::Caller),
    PeerResolutionRejected(signal_flow::CallerResolutionRejection),
    MetaRefused(MetaRefusal),
    Refused(StoreRefusal),
    EventsRead(EventsRead_Data),
    ReadEventsRejected(ReadEventsRejected_Data),
    QueueDeliveriesRead(QueueDeliveriesRead_Data),
    ReadQueueDeliveriesRejected(ReadQueueDeliveriesRejected_Data),
    SendOutcomesRead(std::vec::Vec<VisibleSendOutcomeRecord>),
    ReadSendOutcomesRejected(ReadSendOutcomesRejected_Data),
    LegacyArchiveObserved(std::vec::Vec<VisibleLegacyArchiveEntry>),
    ObserveLegacyArchiveRejected(StoreRefusal),
    QueueDeliveryResolved(QueueDeliveryResolved_Data),
    ResolveQueueDeliveryRejected(VisibleResolveQueueDeliveryRejected),
    Inspected(Inspected_Data),
    InspectRejected(InspectRejected_Data),
    Rebound(flow_ethos::Address),
    RebindRejected(RebindRejected_Data),
    Withdrawn,
    Debugged(DebugSnapshot),
    WithdrawRejected(WithdrawRejected_Data),
}
