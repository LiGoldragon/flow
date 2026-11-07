#![allow(dead_code, non_camel_case_types, non_snake_case)]
#[rustfmt::skip]
pub type SessionId = String;
#[rustfmt::skip]
pub type Tokens = i64;
#[rustfmt::skip]
pub type Timestamp = i64;
#[rustfmt::skip]
pub type FlowType = String;
#[rustfmt::skip]
pub type EndpointPath = String;
#[rustfmt::skip]
pub type HerdrSessionName = String;
#[rustfmt::skip]
pub type HerdrAgentName = String;
#[rustfmt::skip]
pub type HerdrWorkspaceId = String;
#[rustfmt::skip]
pub type HerdrPaneId = String;
#[rustfmt::skip]
pub type HerdrTerminalId = String;
#[rustfmt::skip]
pub type LaunchRequestId = String;
#[rustfmt::skip]
pub type ModuleIdentity = String;
#[rustfmt::skip]
pub type SystemModules = std::vec::Vec<ModuleIdentity>;
#[rustfmt::skip]
pub type UserModules = std::vec::Vec<ModuleIdentity>;
#[rustfmt::skip]
pub type Effort = String;
#[rustfmt::skip]
pub type RememberingDepth = i64;
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum Remembering {
    Predecessors(RememberingDepth),
    Custom(std::vec::Vec<flow_ethos::FlowId>),
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct LaunchRequest {
    pub address: flow_ethos::Address,
    pub remembering: Remembering,
    pub instruction_prompt: InstructionPrompt,
}
#[rustfmt::skip]
pub type SystemPromptText = String;
#[rustfmt::skip]
pub type SystemPromptSha256 = String;
#[rustfmt::skip]
pub type InstructionPrompt = String;
#[rustfmt::skip]
pub type Command = String;
#[rustfmt::skip]
pub type FirstPromptBody = String;
#[rustfmt::skip]
pub type PromptPartText = String;
#[rustfmt::skip]
pub type PromptSha256 = String;
#[rustfmt::skip]
pub type NativeSessionId = String;
#[rustfmt::skip]
pub type NativeTurnId = String;
#[rustfmt::skip]
pub type ReceiptSha256 = String;
#[rustfmt::skip]
pub type TranscriptDevice = String;
#[rustfmt::skip]
pub type TranscriptInode = String;
#[rustfmt::skip]
pub type TranscriptByteOffset = i64;
#[rustfmt::skip]
pub type TranscriptPrefixSha256 = String;
#[rustfmt::skip]
pub type TranscriptRootDevice = String;
#[rustfmt::skip]
pub type TranscriptRootInode = String;
#[rustfmt::skip]
pub type PartsSubmitted = i64;
#[rustfmt::skip]
pub type StopsReported = i64;
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum LeaveOut {
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
#[rustfmt::skip]
pub type LeaveOuts = std::vec::Vec<LeaveOut>;
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum FlowKind {
    Regular,
    Simplified,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum ToolSet {
    Default,
    Explicit(std::vec::Vec<String>),
}
#[rustfmt::skip]
pub type ArtifactToolsAllowed = bool;
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct LaunchProfile {
    pub launch_request_id: LaunchRequestId,
    pub system_modules: SystemModules,
    pub user_modules: UserModules,
    pub remembering: Remembering,
    pub address: flow_ethos::Address,
    pub model_name: flow_ethos::ModelName,
    pub effort: Effort,
    pub flow_id_option: std::option::Option<flow_ethos::FlowId>,
    pub herdr_session_name: HerdrSessionName,
    pub leave_outs_option: std::option::Option<LeaveOuts>,
    pub flow_kind: FlowKind,
    pub tool_set: ToolSet,
    pub instruction_prompt: InstructionPrompt,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct TargetReceiptRequest {
    pub launch_request_id: LaunchRequestId,
    pub prompt_sha256: PromptSha256,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct PromptPart {
    pub prompt_part_text: PromptPartText,
    pub prompt_sha256: PromptSha256,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct FirstPromptPayload {
    pub first_prompt_body: FirstPromptBody,
    pub prompt_sha256: PromptSha256,
    pub prompt_part_vector: std::vec::Vec<PromptPart>,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct SystemPrompt {
    pub system_prompt_text: SystemPromptText,
    pub system_prompt_sha256: SystemPromptSha256,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct ComposedLaunch {
    pub launch_profile: LaunchProfile,
    pub harness_kind: flow_ethos::HarnessKind,
    pub system_prompt: SystemPrompt,
    pub first_prompt_payload: FirstPromptPayload,
    pub target_receipt_request: TargetReceiptRequest,
    pub leave_outs: LeaveOuts,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct HerdrPaneBinding {
    pub launch_request_id: LaunchRequestId,
    pub herdr_session_name: HerdrSessionName,
    pub herdr_agent_name: HerdrAgentName,
    pub herdr_workspace_id: HerdrWorkspaceId,
    pub herdr_pane_id: HerdrPaneId,
    pub herdr_terminal_id: HerdrTerminalId,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct NativeLaunchIntent {
    pub launch_request_id: LaunchRequestId,
    pub prompt_sha256: PromptSha256,
    pub system_prompt_sha256: SystemPromptSha256,
    pub harness_kind: flow_ethos::HarnessKind,
    pub model_name: flow_ethos::ModelName,
    pub effort: Effort,
    pub prompt_sha256_vector: std::vec::Vec<PromptSha256>,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct NativeLaunchBinding {
    pub launch_request_id: LaunchRequestId,
    pub flow_id: flow_ethos::FlowId,
    pub native_session_id: NativeSessionId,
    pub harness_kind: flow_ethos::HarnessKind,
    pub herdr_pane_binding: HerdrPaneBinding,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct RegistrationAcknowledgement {
    pub launch_request_id: LaunchRequestId,
    pub flow_id: flow_ethos::FlowId,
    pub native_session_id: NativeSessionId,
    pub herdr_pane_binding: HerdrPaneBinding,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct NativeTranscriptCursor {
    pub native_session_id: NativeSessionId,
    pub harness_kind: flow_ethos::HarnessKind,
    pub transcript_device: TranscriptDevice,
    pub transcript_inode: TranscriptInode,
    pub transcript_byte_offset: TranscriptByteOffset,
    pub transcript_prefix_sha256: TranscriptPrefixSha256,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct NativeTranscriptAbsence {
    pub native_session_id: NativeSessionId,
    pub harness_kind: flow_ethos::HarnessKind,
    pub transcript_root_device: TranscriptRootDevice,
    pub transcript_root_inode: TranscriptRootInode,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum NativeTranscriptBoundary {
    Existing(NativeTranscriptCursor),
    Absent(NativeTranscriptAbsence),
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct PromptDeliveryIntent {
    pub launch_request_id: LaunchRequestId,
    pub prompt_sha256: PromptSha256,
    pub flow_id: flow_ethos::FlowId,
    pub native_session_id: NativeSessionId,
    pub harness_kind: flow_ethos::HarnessKind,
    pub model_name: flow_ethos::ModelName,
    pub effort: Effort,
    pub prompt_sha256_vector: std::vec::Vec<PromptSha256>,
    pub parts_submitted: PartsSubmitted,
    pub stops_reported: StopsReported,
    pub leave_outs: LeaveOuts,
    pub herdr_pane_binding: HerdrPaneBinding,
    pub native_transcript_boundary: NativeTranscriptBoundary,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct NativeTargetReceipt {
    pub launch_request_id: LaunchRequestId,
    pub prompt_sha256: PromptSha256,
    pub flow_id: flow_ethos::FlowId,
    pub native_session_id: NativeSessionId,
    pub native_turn_id: NativeTurnId,
    pub receipt_sha256: ReceiptSha256,
    pub model_name: flow_ethos::ModelName,
    pub effort: Effort,
    pub prompt_sha256_vector: std::vec::Vec<PromptSha256>,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum PromptDeliveryResult {
    Observed(NativeTargetReceipt),
    Ambiguous(PromptDeliveryIntent),
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum LaunchAttemptPhase {
    Reserved,
    NativeLaunchIntentRecorded,
    NativeBound,
    RegistrationAcknowledged,
    PromptIntentRecorded,
    PromptObserved,
    PromptAmbiguous,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct LaunchAttempt {
    pub launch_request_id: LaunchRequestId,
    pub launch_profile: LaunchProfile,
    pub prompt_sha256: PromptSha256,
    pub origin: Origin,
    pub launch_attempt_phase: LaunchAttemptPhase,
    pub native_launch_intent_option: std::option::Option<NativeLaunchIntent>,
    pub native_launch_binding_option: std::option::Option<NativeLaunchBinding>,
    pub registration_acknowledgement_option: std::option::Option<
        RegistrationAcknowledgement,
    >,
    pub prompt_delivery_intent_option: std::option::Option<PromptDeliveryIntent>,
    pub prompt_delivery_result_option: std::option::Option<PromptDeliveryResult>,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum LaunchStatusOutcome {
    Started(Launched),
    Replaced(Replaced),
    StartRejected(StartRejection),
    ReplaceRejected(ReplaceRejection),
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct LaunchInspection {
    pub launch_attempt: LaunchAttempt,
    pub launch_status_outcome_option: std::option::Option<LaunchStatusOutcome>,
    pub flow_node_option: std::option::Option<FlowNode>,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum LaunchAttemptReservation {
    Reserved(LaunchAttempt),
    Existing(LaunchAttempt),
    Conflict,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct FlowOrigin {
    pub flow_id: flow_ethos::FlowId,
    pub session_id: SessionId,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum Origin {
    Flow(FlowOrigin),
    Unbound,
}
#[rustfmt::skip]
pub type RecipientResolutionRequest = flow_ethos::FlowId;
#[rustfmt::skip]
pub type StopRequest = flow_ethos::FlowId;
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum RouteReadiness {
    Ready,
    Parked,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct Available_Data {
    pub endpoint_path: EndpointPath,
    pub route_readiness: RouteReadiness,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum EndpointSelection {
    Available(Available_Data),
    Unavailable,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct HerdrRoute {
    pub herdr_session_name: HerdrSessionName,
    pub herdr_agent_name: HerdrAgentName,
    pub herdr_pane_id: HerdrPaneId,
    pub herdr_terminal_id: HerdrTerminalId,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum HerdrRouteSelection {
    Available(HerdrRoute),
    Unavailable,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum FlowLifecycle {
    Pending,
    Active,
    Stopped,
    Retired,
    Exited,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct FlowNode {
    pub flow_id: flow_ethos::FlowId,
    pub session_id: SessionId,
    pub harness_kind: flow_ethos::HarnessKind,
    pub endpoint_selection: EndpointSelection,
    pub herdr_route_selection: HerdrRouteSelection,
    pub origin: Origin,
    pub flow_lifecycle: FlowLifecycle,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct Launched {
    pub flow_id: flow_ethos::FlowId,
    pub session_id: SessionId,
    pub herdr_route_selection: HerdrRouteSelection,
    pub origin: Origin,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct Refreshed {
    pub address: flow_ethos::Address,
    pub flow_id: flow_ethos::FlowId,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum BindingRefusal {
    PaneMismatch,
    TerminalMismatch,
    HarnessMismatch,
    NotReady,
    ReservedIdentityMismatch,
    ClaimUnavailable,
    ObservationRefused,
    TransportRefused,
    Unclassified,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum LeaveOutRefusal {
    Indispensable(LeaveOut),
    NeedsWorkflowLeftOut(LeaveOut),
    NotForHarness(LeaveOut),
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum ModelRefusal {
    Unconfigured,
    Differs(flow_ethos::ModelName),
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum ContextRefusal {
    ComposerUnavailable,
    ComposerRejected(String),
    MetaflowMismatch,
    PayloadMismatch,
    ModuleMissing(std::option::Option<ModuleIdentity>),
    RecordMissing,
    SystemTooLarge,
    ModuleTooLarge,
}
#[rustfmt::skip]
pub type HerdrErrorCode = String;
#[rustfmt::skip]
pub type HerdrErrorMessage = String;
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct HerdrRefusal {
    pub herdr_error_code_option: std::option::Option<HerdrErrorCode>,
    pub herdr_error_message: HerdrErrorMessage,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum SpawnRefusalCause {
    Herdr(HerdrRefusal),
    ControlCharacter,
    CommandTooLong,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum FirstTurn {
    Answered,
    InProgress,
    Unanswered,
    Unread,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum ThreadArchive {
    Archived,
    ArchiveRefused,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct OpenedThread {
    pub native_session_id: NativeSessionId,
    pub first_turn: FirstTurn,
    pub thread_archive: ThreadArchive,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct HarnessSpawnRefusal {
    pub spawn_refusal_cause: SpawnRefusalCause,
    pub opened_thread_option: std::option::Option<OpenedThread>,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum NativeTargetRefusal {
    HerdrUnreadable,
    PaneMismatch,
    NoNativeSession,
    IdentityInexact,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum RegistrationRefusal {
    RouteUnverified,
    RoleConflict,
    RoleUnrecorded,
    PaneBindingForeign,
    AcknowledgementMismatch,
    PromptNotCanonical,
    NativeTarget(NativeTargetRefusal),
    TranscriptBoundary,
    ThreadNotNamed,
    Unclassified,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum NativeLaunchStage {
    Open,
    CodexThread,
    Spawn,
    Bind,
    Naming,
    Receipt,
    Confirm,
    Unclassified,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum NativeLaunchFailure {
    PaneTargetMismatch,
    NativeSessionMissing,
    NativeIdentityConflict,
    FlowClaimUnavailable,
    HarnessMismatch,
    HerdrUnreadable,
    ForeignBinding,
    InvalidFlowId,
    NotReady,
    TranscriptUnreadable,
    TitleReadbackDiffers,
    CodexNamingRefused,
    PaneLabelRefused,
    AdapterRefused,
    BoundaryMismatch,
    ThreadNotNamed,
    PartDiffers,
    LeaveOutShown,
    ModelOrEffortDiffers,
    SessionUnrecorded,
    Unclassified,
    SendToolUnsupported,
    WorkspaceLabelRefused,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct NativeLaunchRefusal {
    pub native_launch_stage: NativeLaunchStage,
    pub native_launch_failure: NativeLaunchFailure,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct HandoverUnreadable_Data {
    pub flow_id: flow_ethos::FlowId,
    pub handover_fault: HandoverFault,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum CompositionRefusal {
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
    HandoverUnreadable(HandoverUnreadable_Data),
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum HandoverFault {
    InvalidPredecessor,
    RootUnresolved,
    NotUtf8,
    Unreadable(String),
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum HarnessFlagsRefusal {
    UnclosedSingleQuote,
    UnclosedDoubleQuote,
    TrailingEscape,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum StartRejection {
    CompositionRefused(CompositionRefusal),
    MalformedHarnessFlags(HarnessFlagsRefusal),
    LaunchRequestConflict,
    LaunchPersistenceRefused,
    NativeLaunchRefused(NativeLaunchRefusal),
    BindingRefused(BindingRefusal),
    RegistrationRefused(RegistrationRefusal),
    IntentPersistenceRefused,
    OriginUnavailable,
    AddressAwake,
    ContextRefused(ContextRefusal),
    LeaveOutRefused(LeaveOutRefusal),
    HarnessSpawnRefused(HarnessSpawnRefusal),
    ModelRefused(ModelRefusal),
    Undeclared,
    TopicRefused(flow_ethos::Topic),
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum RefreshRejection {
    NotAwake,
    Locked,
    CloseRefused,
    PersistenceRefused,
    HerdrUnavailable,
    SuccessorRefused(ReplaceRejection),
    Undeclared,
    SuccessorUnconfirmed,
    Protected,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum RecipientResolutionRejection {
    UnknownFlow,
    FlowUnavailable,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum StopRejection {
    UnknownFlow,
    AlreadyStopped,
    RouteUnavailable,
    CloseRefused,
    PersistenceRefused,
    Restarting,
    SpawnUnproven,
    Protected,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct ResumeRefused_Data {
    pub flow_id: flow_ethos::FlowId,
    pub string: String,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum RestartRefusal {
    UnknownFlow(flow_ethos::FlowId),
    NoHeldSession(flow_ethos::FlowId),
    TranscriptAbsent(flow_ethos::FlowId),
    Busy(flow_ethos::FlowId),
    CloseRefused(flow_ethos::FlowId),
    ResumeRefused(ResumeRefused_Data),
    Interrupted(flow_ethos::FlowId),
    Unsupported(flow_ethos::FlowId),
    Protected(flow_ethos::FlowId),
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct Replaced {
    pub flow_id: flow_ethos::FlowId,
    pub launched: Launched,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum ReplaceRejection {
    PredecessorAbsent,
    UnknownPredecessor,
    PredecessorStopped,
    LaunchRefused(StartRejection),
    ReapRefused(StopRejection),
    Locked,
    Protected,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum ObserveSelection {
    Launch(LaunchRequestId),
    Agent(flow_ethos::FlowId),
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum AgentState {
    Idle,
    Working,
    Blocked,
    Done,
    Unknown,
    Gone,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct AgentObservation {
    pub flow_id: flow_ethos::FlowId,
    pub agent_state: AgentState,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum LaunchStatusRejection {
    UnknownLaunchRequest,
    PersistenceRefused,
    ReadViewChanged,
    InconsistentRecords,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct Caller {
    pub flow_id: flow_ethos::FlowId,
    pub aspect: flow_ethos::Aspect,
    pub model_name: flow_ethos::ModelName,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum CallerResolutionRejection {
    CallerUnknown,
    CallerMismatch(Caller),
}
#[rustfmt::skip]
pub type Title = String;
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct Presentation {
    pub title: Title,
}
#[rustfmt::skip]
pub type TranscriptPath = String;
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct SeatPane {
    pub herdr_session_name: HerdrSessionName,
    pub herdr_pane_id: HerdrPaneId,
}
#[rustfmt::skip]
pub type PaneLabel = String;
#[rustfmt::skip]
pub type ObservedAt = i64;
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct SeatEvidence {
    pub seat_pane_option: std::option::Option<SeatPane>,
    pub pane_label_option: std::option::Option<PaneLabel>,
    pub native_session_id_option: std::option::Option<NativeSessionId>,
    pub observed_at: ObservedAt,
    pub process: flow_ethos::Process,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum Unknown_Data {
    Address(flow_ethos::Address),
    Lock(flow_ethos::Lock),
    FlowId(flow_ethos::FlowId),
    Key(flow_ethos::Key),
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum Refusal {
    Locked,
    Held(flow_ethos::Lock),
    Awake(flow_ethos::FlowId),
    Asleep,
    Lapsed,
    Unknown(Unknown_Data),
    NotConfigured,
    NotMessage,
    Taken(flow_ethos::Address),
    Unidentified(SeatEvidence),
    Store(String),
    NotSendServer(flow_ethos::Process),
    Oversized(SendPayloadSize),
    Direct(String),
    TopicRefused(flow_ethos::Topic),
    NoOpenTask(flow_ethos::LogicalDeliveryId),
    Unresolved,
    Ambiguous,
    MinutesRefused(MinutesRefusal),
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum MinutesRefusal {
    Zero,
    Negative,
    Unreachable,
}
#[rustfmt::skip]
pub type ActualBytes = i64;
#[rustfmt::skip]
pub type MaximumBytes = i64;
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct SendPayloadSize {
    pub actual_bytes: ActualBytes,
    pub maximum_bytes: MaximumBytes,
}
#[rustfmt::skip]
pub type PromptId = String;
#[rustfmt::skip]
pub type LastAssistantMessageOption = std::option::Option<String>;
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct NativeStop {
    pub native_session_id_option: std::option::Option<NativeSessionId>,
    pub transcript_path_option: std::option::Option<TranscriptPath>,
    pub last_assistant_message_option: LastAssistantMessageOption,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct NativePrompt {
    pub native_session_id_option: std::option::Option<NativeSessionId>,
    pub prompt_id_option: std::option::Option<PromptId>,
    pub prompt_sha256: PromptSha256,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct VisibleResultPayload_Answered_Data {
    pub answerable: flow_ethos::Answerable,
    pub string: String,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum VisibleResultPayload {
    Answered(VisibleResultPayload_Answered_Data),
    Historical(String),
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct VisibleDonePayload_Answered_Data {
    pub answerable: flow_ethos::Answerable,
    pub string: String,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum VisibleDonePayload {
    Answered(VisibleDonePayload_Answered_Data),
    Historical(String),
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct Result_Data {
    pub answerable: flow_ethos::Answerable,
    pub string: String,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct Done_Data {
    pub answerable: flow_ethos::Answerable,
    pub string: String,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum SendableRequest {
    Order(String),
    Question(String),
    Psyche(flow_ethos::Said),
    Psyches(std::vec::Vec<flow_ethos::Said>),
    Result(Result_Data),
    Notice(String),
    Task(flow_ethos::AnswerableTask),
    Done(Done_Data),
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum SendableMessage {
    HardAbrupt(SendableRequest),
    MiddleAbrupt(SendableRequest),
    Soft(SendableRequest),
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum VisibleRequest {
    Order(String),
    Question(String),
    Psyche(flow_ethos::Said),
    Psyches(std::vec::Vec<flow_ethos::Said>),
    Result(VisibleResultPayload),
    Notice(String),
    Task(flow_ethos::AnswerableTask),
    Done(VisibleDonePayload),
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum VisibleMessage {
    HardAbrupt(VisibleRequest),
    MiddleAbrupt(VisibleRequest),
    Soft(VisibleRequest),
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum VisibleSender {
    Sender(flow_ethos::Address),
    Unknown,
    Notification,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct VisibleNativeInput {
    pub visible_sender: VisibleSender,
    pub visible_message: VisibleMessage,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct SendRequest {
    pub recipient: flow_ethos::Recipient,
    pub sendable_message: SendableMessage,
}
#[rustfmt::skip]
pub type DeliveryCursor = i64;
#[rustfmt::skip]
pub type NativePromptId = String;
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct NativeReceipt {
    pub harness_kind: flow_ethos::HarnessKind,
    pub native_session_id: NativeSessionId,
    pub native_prompt_id: NativePromptId,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum RetryCause {
    HistoryAbsent,
    HandOverRefused,
    OwnerResolvedAbsent,
    WakeRefused(StartRejection),
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum DeliveryState {
    Queued,
    Attempting,
    Retryable(RetryCause),
    Uncertain,
    Delivered(NativeReceipt),
    Failed(Refusal),
    Withdrawn,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct SendReceipt {
    pub logical_delivery_id: flow_ethos::LogicalDeliveryId,
    pub native_receipt: NativeReceipt,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct SendFailure {
    pub logical_delivery_id_option: std::option::Option<flow_ethos::LogicalDeliveryId>,
    pub send_failure_reason: SendFailureReason,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum SendFailureReason {
    RecipientUnavailable(flow_ethos::Address),
    QueueNotPermitted,
    Refused(Refusal),
    ReceiptUncertain,
    NotSent(String),
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct SendAcceptance {
    pub logical_delivery_id: flow_ethos::LogicalDeliveryId,
    pub delivery_cursor: DeliveryCursor,
    pub delivery_state: DeliveryState,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct DeliverySnapshot {
    pub delivery_cursor: DeliveryCursor,
    pub logical_delivery_id: flow_ethos::LogicalDeliveryId,
    pub sent_by: flow_ethos::SentBy,
    pub address: flow_ethos::Address,
    pub delivery_state: DeliveryState,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct SendObservation {
    pub delivery_cursor: DeliveryCursor,
    pub delivery_snapshot_vector: std::vec::Vec<DeliverySnapshot>,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum DrainState {
    Delivered,
    Held(std::vec::Vec<flow_ethos::LogicalDeliveryId>),
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct DrainOutcome {
    pub address: flow_ethos::Address,
    pub drain_state: DrainState,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum PrivateSendResponse {
    SendAccepted(SendAcceptance),
    Refused(Refusal),
    Sent(SendReceipt),
    Failed(SendFailure),
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum VisibleRefusal {
    Locked,
    Held(flow_ethos::Lock),
    Awake,
    Asleep,
    Lapsed,
    Unknown(VisibleUnknown),
    NotConfigured,
    NotMessage,
    Taken(flow_ethos::Address),
    Unidentified(SeatEvidence),
    Store(String),
    NotSendServer(flow_ethos::Process),
    Oversized(SendPayloadSize),
    Direct(String),
    TopicRefused(flow_ethos::Topic),
    Unresolved,
    Ambiguous,
    MinutesRefused(MinutesRefusal),
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum VisibleUnknown {
    Address(flow_ethos::Address),
    Lock(flow_ethos::Lock),
    Flow,
    Key(flow_ethos::Key),
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum VisibleDeliveryState {
    Queued,
    Attempting,
    Retryable(RetryCause),
    Uncertain,
    Delivered(NativeReceipt),
    Failed(VisibleRefusal),
    Withdrawn,
}
#[rustfmt::skip]
pub type VisibleSendReceipt = NativeReceipt;
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct VisibleSendFailure {
    pub visible_send_failure_reason: VisibleSendFailureReason,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum VisibleSendFailureReason {
    RecipientUnavailable(flow_ethos::Address),
    QueueNotPermitted,
    Refused(VisibleRefusal),
    ReceiptUncertain,
    NotSent(String),
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct VisibleSendAcceptance {
    pub delivery_cursor: DeliveryCursor,
    pub visible_delivery_state: VisibleDeliveryState,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct VisibleDeliverySnapshot {
    pub delivery_cursor: DeliveryCursor,
    pub visible_sender: VisibleSender,
    pub address: flow_ethos::Address,
    pub visible_delivery_state: VisibleDeliveryState,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct VisibleSendObservation {
    pub delivery_cursor: DeliveryCursor,
    pub visible_delivery_snapshot_vector: std::vec::Vec<VisibleDeliverySnapshot>,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum VisibleDrainState {
    Delivered,
    Held,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct VisibleDrainOutcome {
    pub address: flow_ethos::Address,
    pub visible_drain_state: VisibleDrainState,
}
#[rustfmt::skip]
pub type Description = String;
#[rustfmt::skip]
pub type Body = String;
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct Section {
    pub title: Title,
    pub description: Description,
    pub body: Body,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct Heading {
    pub title: Title,
    pub description: Description,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct Known_Data {
    pub flow_id: flow_ethos::FlowId,
    pub tokens: Tokens,
    pub timestamp: Timestamp,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum Touch {
    Unknown,
    Known(Known_Data),
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum HandoverRefusal {
    MissingTitle,
    UnknownFlow,
    UnknownCaller,
    NotOwner,
    Store(String),
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum Event {
    Started(std::option::Option<NativeSessionId>),
    ToolUsed(String),
    Stopped(NativeStop),
    PromptSubmitted(NativePrompt),
    Ended(std::option::Option<NativeSessionId>),
    Cleared(std::option::Option<NativeSessionId>),
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct Report_Data {
    pub flow_id: flow_ethos::FlowId,
    pub event: Event,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct Lock_Data {
    pub sender: flow_ethos::Sender,
    pub recipient: flow_ethos::Recipient,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct Bind_Data {
    pub address: flow_ethos::Address,
    pub process: flow_ethos::Process,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct Deliver_Data {
    pub lock: flow_ethos::Lock,
    pub message: flow_ethos::Message,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct IdentifyThread_Data {
    pub process: flow_ethos::Process,
    pub native_session_id: NativeSessionId,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct ObserveSender_Data {
    pub process: flow_ethos::Process,
    pub native_session_id_option: std::option::Option<NativeSessionId>,
    pub delivery_cursor_option: std::option::Option<DeliveryCursor>,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct Adopt_Data {
    pub address: flow_ethos::Address,
    pub native_session_id: NativeSessionId,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct Keep_Data {
    pub address: flow_ethos::Address,
    pub section: Section,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct Drop_Data {
    pub address: flow_ethos::Address,
    pub title: Title,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum Query {
    Start(LaunchProfile),
    Refresh(flow_ethos::Address),
    ResolveRecipient(RecipientResolutionRequest),
    Stop(flow_ethos::FlowId),
    Replace(LaunchProfile),
    LaunchStatus(LaunchRequestId),
    Observe(ObserveSelection),
    ResolveCaller(std::option::Option<flow_ethos::FlowId>),
    Report(Report_Data),
    Lock(Lock_Data),
    Bind(Bind_Data),
    Identify(flow_ethos::Process),
    Deliver(Deliver_Data),
    Release(flow_ethos::Lock),
    Current(flow_ethos::Address),
    Metaflows,
    IdentifyThread(IdentifyThread_Data),
    Send(SendRequest),
    ObserveSends(std::option::Option<DeliveryCursor>),
    ObserveSender(ObserveSender_Data),
    DeliverAll(flow_ethos::Address),
    Launch(LaunchRequest),
    Rehearse(LaunchRequest),
    BindSeat(flow_ethos::Address),
    Adopt(Adopt_Data),
    Restart(flow_ethos::FlowId),
    Keep(Keep_Data),
    Drop(Drop_Data),
    Glance(flow_ethos::Address),
    Read(flow_ethos::Address),
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum Current_Data {
    Awake(flow_ethos::FlowId),
    Asleep,
    Unknown,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct Kept_Data {
    pub address: flow_ethos::Address,
    pub title: Title,
    pub touch: Touch,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct Dropped_Data {
    pub address: flow_ethos::Address,
    pub title: Title,
    pub touch: Touch,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum Response {
    Started(Launched),
    LaunchPending(LaunchAttempt),
    StartAmbiguous(PromptDeliveryIntent),
    Refreshed(Refreshed),
    RecipientResolved(FlowNode),
    Stopped(flow_ethos::FlowId),
    Listed(std::vec::Vec<flow_ethos::Metaflow>),
    StartRejected(StartRejection),
    Rehearsed(Command),
    RehearseRejected(StartRejection),
    RefreshRejected(RefreshRejection),
    RecipientResolutionRejected(RecipientResolutionRejection),
    StopRejected(StopRejection),
    Replaced(Replaced),
    ReplaceRejected(ReplaceRejection),
    LaunchInspected(LaunchInspection),
    LaunchStatusRejected(LaunchStatusRejection),
    CallerResolved(Caller),
    CallerResolutionRejected(CallerResolutionRejection),
    AgentObserved(AgentObservation),
    Reported,
    Locked(flow_ethos::Lock),
    Bound(flow_ethos::FlowId),
    Identified(flow_ethos::Address),
    SendAccepted(VisibleSendAcceptance),
    SendsObserved(VisibleSendObservation),
    SendChanged(VisibleDeliverySnapshot),
    Drained(VisibleDrainOutcome),
    Woken(flow_ethos::FlowId),
    Adopted(flow_ethos::FlowId),
    Released,
    Current(Current_Data),
    Refused(Refusal),
    SendRefused(VisibleRefusal),
    Sent(VisibleSendReceipt),
    Failed(VisibleSendFailure),
    Restarted(flow_ethos::FlowId),
    RestartRefused(RestartRefusal),
    KeepRefused(HandoverRefusal),
    DropRefused(HandoverRefusal),
    GlanceRefused(HandoverRefusal),
    ReadRefused(HandoverRefusal),
    Kept(Kept_Data),
    Dropped(Dropped_Data),
    Glanced(std::vec::Vec<Heading>),
    Handover(std::vec::Vec<Section>),
}
