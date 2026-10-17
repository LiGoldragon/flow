#![allow(dead_code, non_camel_case_types, non_snake_case)]
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct ContextRequest {
    pub address: signal_flow::Address,
    pub harness_kind: signal_flow::HarnessKind,
    pub system_modules: signal_flow::SystemModules,
    pub user_modules: signal_flow::UserModules,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct HarnessedProfile {
    pub launch_profile: signal_flow::LaunchProfile,
    pub harness_kind: signal_flow::HarnessKind,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct FreshContext {
    pub herdr_session_name: signal_flow::HerdrSessionName,
    pub system_modules: signal_flow::SystemModules,
    pub user_modules: signal_flow::UserModules,
    pub leave_outs_option: std::option::Option<signal_flow::LeaveOuts>,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct LaunchedRecord {
    pub flow_id: signal_flow::FlowId,
    pub session_id: signal_flow::SessionId,
    pub origin: signal_flow::Origin,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct ReplacedRecord {
    pub flow_id: signal_flow::FlowId,
    pub launched_record: LaunchedRecord,
}
#[rustfmt::skip]
pub type SourcePath = String;
#[rustfmt::skip]
pub type SourceSha256 = String;
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct LaunchSource {
    pub source_path: SourcePath,
    pub source_sha256: SourceSha256,
}
#[rustfmt::skip]
pub type LaunchSources = std::vec::Vec<LaunchSource>;
#[rustfmt::skip]
pub type SkillName = String;
#[rustfmt::skip]
pub type SkillPath = String;
#[rustfmt::skip]
pub type SkillSha256 = String;
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct NativeSkill {
    pub skill_name: SkillName,
    pub skill_path: SkillPath,
    pub skill_sha256: SkillSha256,
}
#[rustfmt::skip]
pub type NativeSkills = std::vec::Vec<NativeSkill>;
#[rustfmt::skip]
pub type TurnId = String;
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct OriginClue {
    pub flow_id: signal_flow::FlowId,
    pub session_id: signal_flow::SessionId,
    pub turn_id: TurnId,
}
#[rustfmt::skip]
pub type Skills = std::vec::Vec<String>;
#[rustfmt::skip]
pub type BundleFile = String;
#[rustfmt::skip]
pub type LaunchIntentSkills = std::option::Option<std::vec::Vec<String>>;
#[rustfmt::skip]
pub type IntentSkills = std::option::Option<NativeSkills>;
#[rustfmt::skip]
pub type ResultSkills = std::option::Option<NativeSkills>;
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct ContextBeforeModules {
    pub launch_sources: LaunchSources,
    pub skills: Skills,
    pub power_level: signal_flow::PowerLevel,
    pub harness_kind: signal_flow::HarnessKind,
    pub bundle_file: BundleFile,
    pub origin_clue: OriginClue,
    pub launch_intent_skills: LaunchIntentSkills,
    pub intent_skills: IntentSkills,
    pub result_skills: ResultSkills,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum LaunchContext {
    Modules,
    BeforeModules(ContextBeforeModules),
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum StartupProgress {
    Received(signal_flow::NativeTargetReceipt),
    Waiting(signal_flow::PromptDeliveryIntent),
    PartAnswered(signal_flow::PromptDeliveryIntent),
}
#[rustfmt::skip]
pub type SeenParts = i64;
#[rustfmt::skip]
pub type LastPartAnswered = bool;
#[rustfmt::skip]
pub type NativeTurn = String;
#[rustfmt::skip]
pub type PartTurn = String;
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct StartupReading {
    pub seen_parts: SeenParts,
    pub last_part_answered: LastPartAnswered,
    pub native_turn_option: std::option::Option<NativeTurn>,
    pub part_turn_option: std::option::Option<PartTurn>,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct LeanStart {
    pub harness_kind: signal_flow::HarnessKind,
    pub leave_outs: signal_flow::LeaveOuts,
}
#[rustfmt::skip]
pub type SystemText = String;
#[rustfmt::skip]
pub type UserSegments = std::vec::Vec<String>;
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct VerifiedContext {
    pub(super) address: signal_flow::Address,
    pub(super) system_text: SystemText,
    pub(super) user_segments: UserSegments,
}
#[rustfmt::skip]
pub trait Textual {
    fn text(&self) -> String;
}
#[rustfmt::skip]
pub trait Profiled {
    fn profile(&self) -> signal_flow::LaunchProfile;
    fn harness_kind(&self) -> signal_flow::HarnessKind;
}
#[rustfmt::skip]
pub trait LeaveOutListing {
    fn leave_outs(&self) -> signal_flow::LeaveOuts;
}
#[rustfmt::skip]
pub trait ContextRequesting {
    fn context_request(&self) -> ContextRequest;
}
#[rustfmt::skip]
pub trait StartupComposing {
    fn composition(&self) -> flow_nexus::StartupComposition;
}
#[rustfmt::skip]
pub trait CurriculumQuerying {
    fn query(&self) -> flow_nexus::CurriculumQuery;
}
#[rustfmt::skip]
pub trait ProvenanceListing {
    fn provenance(&self) -> std::vec::Vec<flow_nexus::ContextProvenance>;
}
#[rustfmt::skip]
pub trait SegmentListing {
    fn segments(&self) -> std::vec::Vec<String>;
}
#[rustfmt::skip]
pub trait PartNumbering {
    fn part_number(&self) -> i64;
    fn part_count(&self) -> i64;
}
#[rustfmt::skip]
pub trait SystemPrompted {
    fn system_prompt(&self) -> signal_flow::SystemPrompt;
}
#[rustfmt::skip]
pub trait PaneBound {
    fn pane(&self) -> signal_flow::HerdrPaneBinding;
}
#[rustfmt::skip]
pub trait JsonRow {
    fn row(&self) -> serde_json::Value;
}
#[rustfmt::skip]
pub trait DeliveryIntended {
    fn intent(&self) -> signal_flow::PromptDeliveryIntent;
}
#[rustfmt::skip]
pub trait Progressing {
    fn progress(&self) -> StartupProgress;
}
#[rustfmt::skip]
pub trait OutcomeCarrying {
    fn outcome(&self) -> flow_nexus::OperationOutcome;
}
#[rustfmt::skip]
pub trait LaunchedRecording {
    fn launched_record(&self) -> LaunchedRecord;
}
#[rustfmt::skip]
pub trait CallerProcessing {
    fn caller_process(&self) -> std::option::Option<flow_nexus::CallerProcess>;
}
#[rustfmt::skip]
pub trait ComposesFlowContext {
    fn compose_flow_context<N: ContextRequesting>(
        &self,
        input: N,
    ) -> std::result::Result<
        flow_nexus::StartupComposition,
        signal_flow::ContextRefusal,
    >;
}
#[rustfmt::skip]
pub trait OpensCurriculumComposer {
    fn at<N: Textual>(input: N) -> Self
    where
        Self: Sized;
    fn from_environment() -> Self
    where
        Self: Sized;
}
#[rustfmt::skip]
pub trait SelectsStartup {
    fn startup_selection(&self) -> flow_nexus::StartupSelection;
}
#[rustfmt::skip]
pub trait ExchangesCurriculumFrames {
    fn exchange<N: CurriculumQuerying>(
        &self,
        input: N,
    ) -> std::result::Result<flow_nexus::CurriculumOutcome, String>;
}
#[rustfmt::skip]
pub trait VerifiesContext {
    fn verified<N: ContextRequesting, O: StartupComposing>(
        input_0: N,
        input_1: O,
    ) -> std::result::Result<Self, signal_flow::ContextRefusal>
    where
        Self: Sized;
}
#[rustfmt::skip]
pub trait ReadsVerifiedContext {
    fn address(&self) -> signal_flow::Address;
    fn system_text(&self) -> String;
    fn user_segments(&self) -> std::vec::Vec<String>;
}
#[rustfmt::skip]
pub trait CutsRenderedText {
    fn rendered_segments<N: ProvenanceListing>(
        &self,
        input: N,
    ) -> std::option::Option<std::vec::Vec<String>>;
}
#[rustfmt::skip]
pub trait FitsOneMessage {
    fn fits_one_message<N: Textual>(&self, input: N) -> bool;
    fn system_argument<N: Textual>(&self, input: N) -> String;
    fn fits_one_argument<N: Textual>(&self, input: N) -> bool;
}
#[rustfmt::skip]
pub trait SplitsStartupMessage {
    fn part_header<N: PartNumbering>(input: N) -> String;
    fn startup_parts<N: SegmentListing, O: Textual>(
        &self,
        input_0: N,
        input_1: O,
    ) -> std::result::Result<std::vec::Vec<String>, signal_flow::ContextRefusal>;
}
#[rustfmt::skip]
pub trait JoinsContextAndBrief {
    fn joined_with<N: Textual>(&self, input: N) -> std::vec::Vec<String>;
}
#[rustfmt::skip]
pub trait RendersBrief {
    fn brief(&self) -> String;
}
#[rustfmt::skip]
pub trait ResolvesLeaveOuts {
    fn effective_leave_outs<N: LeaveOutListing>(
        &self,
        input: N,
    ) -> std::result::Result<signal_flow::LeaveOuts, flow_nexus::CompositionError>;
}
#[rustfmt::skip]
pub trait ComposesVerifiedContext {
    fn verified_context<N: Profiled>(
        &self,
        input: N,
    ) -> std::result::Result<VerifiedContext, flow_nexus::CompositionError>;
    fn launch_system_prompt<N: Profiled, O: ReadsVerifiedContext>(
        &self,
        input_0: N,
        input_1: O,
    ) -> std::result::Result<signal_flow::SystemPrompt, signal_flow::ContextRefusal>;
}
#[rustfmt::skip]
pub trait AssemblesComposedLaunch {
    fn assembled<
        N: Profiled,
        O: SystemPrompted,
        P: Textual,
        Q: SegmentListing,
        R: LeaveOutListing,
    >(
        input_0: N,
        input_1: O,
        input_2: P,
        input_3: Q,
        input_4: R,
    ) -> signal_flow::ComposedLaunch;
}
#[rustfmt::skip]
pub trait ComposesLaunch {
    fn compose<N: Profiled, O: LeaveOutListing>(
        &self,
        input_0: N,
        input_1: O,
    ) -> std::result::Result<signal_flow::ComposedLaunch, flow_nexus::CompositionError>;
    fn resumption<N: Profiled, O: LeaveOutListing, P: Textual>(
        &self,
        input_0: N,
        input_1: O,
        input_2: P,
    ) -> std::result::Result<signal_flow::ComposedLaunch, flow_nexus::CompositionError>;
}
#[rustfmt::skip]
pub trait ProfilesFreshSeat {
    fn fresh_profile(&self) -> signal_flow::LaunchProfile;
}
#[rustfmt::skip]
pub trait PromptsThroughHerdrSocket {
    fn herdr_session_socket<N: Textual>(
        &self,
        input: N,
    ) -> std::result::Result<String, String>;
    fn prompt_through_socket<N: PaneBound, O: Textual>(
        &self,
        input_0: N,
        input_1: O,
    ) -> std::result::Result<serde_json::Value, String>;
}
#[rustfmt::skip]
pub trait ReadsStartupRecord {
    fn read_record<N: JsonRow, O: DeliveryIntended>(
        &mut self,
        input_0: N,
        input_1: O,
    ) -> std::result::Result<
        std::option::Option<String>,
        signal_flow::NativeLaunchFailure,
    >;
    fn read_part<N: Textual, O: DeliveryIntended>(
        &mut self,
        input_0: N,
        input_1: O,
    ) -> std::result::Result<i64, signal_flow::NativeLaunchFailure>;
}
#[rustfmt::skip]
pub trait ReadsTypedUserText {
    fn claude_typed_text(&self) -> std::option::Option<String>;
}
#[rustfmt::skip]
pub trait RendersLeanStart {
    fn refusal(&self) -> std::option::Option<signal_flow::LeaveOutRefusal>;
    fn arguments(&self) -> std::vec::Vec<String>;
    fn environment(&self) -> std::vec::Vec<String>;
    fn disallowed_tools(&self) -> std::vec::Vec<String>;
    fn attribution_off(&self) -> bool;
    fn skill_listing_off(&self) -> bool;
}
#[rustfmt::skip]
pub trait SettlesStartupProgress {
    fn settle_progress<N: Profiled, O: DeliveryIntended, P: Progressing>(
        &self,
        input_0: N,
        input_1: O,
        input_2: P,
    ) -> signal_flow::Response;
    fn advance_startup<N: Profiled, O: DeliveryIntended>(
        &self,
        input_0: N,
        input_1: O,
    ) -> signal_flow::Response;
    fn stops_reported<N: Textual>(&self, input: N) -> i64;
    fn composition_refused<N: OutcomeCarrying>(input: N) -> signal_flow::Response;
}
#[rustfmt::skip]
pub trait NamesLaunchedRoute {
    fn launched<N: LaunchedRecording>(&self, input: N) -> signal_flow::Launched;
}
#[rustfmt::skip]
pub trait DerivesOrigin {
    fn origin_of<N: CallerProcessing>(&self, input: N) -> signal_flow::Origin;
}
#[rustfmt::skip]
pub trait DerivesHarness {
    fn harness_for<N: Textual>(&self, input: N) -> signal_flow::HarnessKind;
}
#[rustfmt::skip]
const _: () = {
    fn assert_verifiedcontext_verifiescontext<T: VerifiesContext>() {}
    let _ = assert_verifiedcontext_verifiescontext::<VerifiedContext>;
    fn assert_verifiedcontext_readsverifiedcontext<T: ReadsVerifiedContext>() {}
    let _ = assert_verifiedcontext_readsverifiedcontext::<VerifiedContext>;
};
