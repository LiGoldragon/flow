#![allow(
    dead_code,
    non_camel_case_types,
    non_snake_case,
    clippy::type_complexity
)]
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum LaunchOutcome {
    Started(flow_nexus::LaunchedRecord),
    Replaced(flow_nexus::ReplacedRecord),
    StartRejected(signal_flow::StartRejection),
    ReplaceRejected(signal_flow::ReplaceRejection),
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct Predecessor(pub String);
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct Replacement {
    pub launch_request_id: signal_flow::LaunchRequestId,
    pub predecessor: Predecessor,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct SeatBirth {
    pub flow_id: signal_flow::FlowId,
    pub herdr_route: signal_flow::HerdrRoute,
    pub process: signal_flow::Process,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct RecordedStop {
    pub native_session_id_option: std::option::Option<signal_flow::NativeSessionId>,
    pub transcript_path_option: std::option::Option<signal_flow::TranscriptPath>,
    pub background_work: signal_flow::BackgroundWork,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum RecordedHarnessEvent {
    Started(std::option::Option<signal_flow::NativeSessionId>),
    ToolUsed(String),
    Stopped(RecordedStop),
    PromptSubmitted(signal_flow::NativePrompt),
    Ended(std::option::Option<signal_flow::NativeSessionId>),
    Cleared(std::option::Option<signal_flow::NativeSessionId>),
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct FlowEvents {
    pub flow_id: signal_flow::FlowId,
    pub recorded_harness_event_vector: std::vec::Vec<RecordedHarnessEvent>,
}
