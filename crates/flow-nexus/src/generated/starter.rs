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
    pub origin_clue: signal_flow::OriginClue,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct ReplacedRecord {
    pub flow_id: signal_flow::FlowId,
    pub launched_record: LaunchedRecord,
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
pub trait SelectsStartup {
    fn startup_selection(&self) -> flow_nexus::StartupSelection;
}
#[rustfmt::skip]
pub trait ReadsVerifiedContext {
    fn address(&self) -> signal_flow::Address;
    fn system_text(&self) -> String;
    fn user_segments(&self) -> std::vec::Vec<String>;
}
#[rustfmt::skip]
pub trait RendersBrief {
    fn brief(&self) -> String;
}
#[rustfmt::skip]
pub trait ProfilesFreshSeat {
    fn fresh_profile(&self) -> signal_flow::LaunchProfile;
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
