#![allow(dead_code, non_camel_case_types, non_snake_case)]
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
pub type Predecessor = String;
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct Replacement {
    pub launch_request_id: signal_flow::LaunchRequestId,
    pub predecessor: Predecessor,
}
