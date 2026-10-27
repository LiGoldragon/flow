#![allow(dead_code, non_camel_case_types, non_snake_case)]
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct PaneLaunch {
    pub composed_launch: signal_flow::ComposedLaunch,
    pub herdr_pane_binding: signal_flow::HerdrPaneBinding,
    pub flow_id_option: std::option::Option<signal_flow::FlowId>,
    pub native_session_id_option: std::option::Option<signal_flow::NativeSessionId>,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct Reserve_Data {
    pub composed_launch: signal_flow::ComposedLaunch,
    pub origin: signal_flow::Origin,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct Record_Data_Settled_Data {
    pub launch_request_id: signal_flow::LaunchRequestId,
    pub launch_outcome: flow_nexus::LaunchOutcome,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct Record_Data_Harness_Data {
    pub flow_id: signal_flow::FlowId,
    pub event: signal_flow::Event,
}
#[rustfmt::skip]
pub type Tokens = i64;
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct Record_Data_Context_Data {
    pub flow_id: signal_flow::FlowId,
    pub tokens: Tokens,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct Record_Data_Asleep_Data {
    pub address: signal_flow::Address,
    pub flow_id: signal_flow::FlowId,
}
#[rustfmt::skip]
pub type Delivered = i64;
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct Record_Data_Awoken_Data {
    pub address: signal_flow::Address,
    pub flow_id: signal_flow::FlowId,
    pub delivered: Delivered,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum Record_Data {
    Intent(signal_flow::NativeLaunchIntent),
    Binding(signal_flow::NativeLaunchBinding),
    Acknowledgement(signal_flow::RegistrationAcknowledgement),
    Delivery(signal_flow::PromptDeliveryIntent),
    Delivered(signal_flow::PromptDeliveryResult),
    Active(signal_flow::FlowId),
    Stopped(signal_flow::FlowId),
    Retired(signal_flow::FlowId),
    Exited(signal_flow::FlowId),
    Replacing(flow_nexus::Replacement),
    Withdrawn(signal_flow::LaunchRequestId),
    Settled(Record_Data_Settled_Data),
    Harness(Record_Data_Harness_Data),
    Context(Record_Data_Context_Data),
    Asleep(Record_Data_Asleep_Data),
    Awoken(Record_Data_Awoken_Data),
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct Register_Data {
    pub flow_node: signal_flow::FlowNode,
    pub caller: signal_flow::Caller,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct Title_Data {
    pub composed_launch: signal_flow::ComposedLaunch,
    pub native_launch_binding: signal_flow::NativeLaunchBinding,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct Submit_Data {
    pub composed_launch: signal_flow::ComposedLaunch,
    pub prompt_delivery_intent: signal_flow::PromptDeliveryIntent,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum Operation {
    Compose(signal_flow::LaunchProfile),
    Reserve(Reserve_Data),
    Record(Record_Data),
    Register(Register_Data),
    Confirm(signal_flow::FlowId),
    Open(signal_flow::ComposedLaunch),
    Spawn(PaneLaunch),
    Bind(PaneLaunch),
    Title(Title_Data),
    Submit(Submit_Data),
    Close(signal_flow::FlowNode),
    Release(signal_flow::FlowId),
    Wake(signal_flow::Address),
    Drain(signal_flow::Address),
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct Reserved_Data {
    pub launch_attempt_reservation: signal_flow::LaunchAttemptReservation,
    pub flow_id_option: std::option::Option<signal_flow::FlowId>,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum Failed_Data {
    CompositionRefused(signal_flow::CompositionRefusal),
    StoreRefused,
    ConflictingBinding,
    Unstarted,
    HerdrRefused,
    PaneCloseRefused(signal_flow::PaneCloseFailure),
    CodexRefused,
    UnknownFlow,
    ClaimRefused,
    NotAsleep,
    NotAwake,
    NothingQueued,
    AddressAwake,
    ContextRefused(signal_flow::ContextRefusal),
    SpawnRefused(signal_flow::HarnessSpawnRefusal),
    LaunchRefused(signal_flow::NativeLaunchRefusal),
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum Outcome {
    Composed(signal_flow::ComposedLaunch),
    Reserved(Reserved_Data),
    Recorded,
    Registered(signal_flow::FlowNode),
    Started(signal_flow::Launched),
    Opened(signal_flow::HerdrPaneBinding),
    Spawned(std::option::Option<signal_flow::NativeSessionId>),
    Bound(signal_flow::NativeLaunchBinding),
    Titled,
    Submitted(signal_flow::PromptDeliveryResult),
    Closed,
    Released,
    Woken(signal_flow::FlowId),
    Drained(std::vec::Vec<signal_flow::QueueEntry>),
    AwaitingReceipt(signal_flow::LogicalDeliveryId),
    WaitingForTurn(signal_flow::LogicalDeliveryId),
    Failed(Failed_Data),
}
