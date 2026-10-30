#![allow(dead_code, non_camel_case_types, non_snake_case)]
#[rustfmt::skip]
pub type Minutes = i64;
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct Flow_Data {
    pub address: crate::Address,
    pub flow_id: crate::FlowId,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum Sender {
    Flow(Flow_Data),
    Living,
    Nexus,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct Order_Data {
    pub minutes: Minutes,
    pub string: String,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct Result_Data {
    pub logical_delivery_id: crate::LogicalDeliveryId,
    pub string: String,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum Request {
    Order(Order_Data),
    Question(String),
    Psyche(std::vec::Vec<crate::Said>),
    Result(Result_Data),
    Notice(String),
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum Message {
    HardAbrupt(Request),
    MiddleAbrupt(Request),
    Soft(Request),
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct NativeInput {
    pub logical_delivery_id: crate::LogicalDeliveryId,
    pub sender: Sender,
    pub message: Message,
}
#[rustfmt::skip]
pub type Waking = std::vec::Vec<NativeInput>;
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct QueueEntry {
    pub logical_delivery_id: crate::LogicalDeliveryId,
    pub sender: Sender,
    pub delivery: crate::Delivery,
    pub message: Message,
}
#[rustfmt::skip]
pub type HeldId = String;
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum HeldFamily {
    MetaflowV5,
    QueuedRequestBeforeLogicalId,
    QueuedRequestBeforeFlow,
    MetaflowBeforeSenders,
    StoredDeliveryRequestV2,
    QueueDeliveryAttemptV4,
    QueueEvidence,
    CurrentLogicalDelivery,
    LogicalDeliveryEvent,
    AddressHoldV3,
    SendOutcome,
}
