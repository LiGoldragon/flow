#![allow(dead_code, non_camel_case_types, non_snake_case)]
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum SendKind {
    Order,
    Question,
    Result,
    Notice,
}
#[rustfmt::skip]
pub type Target = signal_flow::Address;
#[rustfmt::skip]
pub type Body = String;
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum SendPriority {
    HardAbrupt,
    MiddleAbrupt,
    Soft,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct SendArguments {
    pub target: Target,
    pub delivery: signal_flow::Delivery,
    pub send_priority: SendPriority,
    pub send_kind: SendKind,
    pub body: Body,
}
#[rustfmt::skip]
pub type ThreadId = String;
#[rustfmt::skip]
pub type TurnId = String;
#[rustfmt::skip]
pub type CallId = String;
#[rustfmt::skip]
pub type ToolName = String;
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct ToolCallKey {
    pub thread_id: ThreadId,
    pub turn_id: TurnId,
    pub call_id: CallId,
}
#[rustfmt::skip]
pub type Arguments = String;
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct ToolCall {
    pub tool_call_key: ToolCallKey,
    pub tool_name: ToolName,
    pub arguments: Arguments,
}
#[rustfmt::skip]
pub type ContentSha256 = String;
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum SendRefusal {
    UnknownTool,
    Malformed,
    ThreadUnowned,
    FlowEnded,
    NotAwake,
    KeyReused,
    UnknownRecipient(Target),
    Held,
    Oversized(signal_flow::SendPayloadSize),
    Store,
    TopicRefused(signal_flow::Topic),
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct Accepted_Data {
    pub sender: signal_flow::Sender,
    pub target: Target,
    pub send_acceptance: signal_flow::SendAcceptance,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct LegacyUnknown_Data {
    pub sender: signal_flow::Sender,
    pub target: Target,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum SendOutcome {
    Accepted(Accepted_Data),
    LegacyUnknown(LegacyUnknown_Data),
    Refused(SendRefusal),
}
#[rustfmt::skip]
pub type SendInvocationId = String;
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct SendInvocation {
    pub send_invocation_id: SendInvocationId,
    pub tool_call_key: ToolCallKey,
    pub tool_name: ToolName,
    pub content_sha256: ContentSha256,
    pub send_outcome: SendOutcome,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct SendCall {
    pub tool_call_key: ToolCallKey,
    pub content_sha256: ContentSha256,
    pub send_outcome: SendOutcome,
}
#[rustfmt::skip]
pub trait ParamsCarrying {
    fn params(&self) -> serde_json::Value;
}
#[rustfmt::skip]
pub trait ToolCallCarrying {
    fn tool_call(&self) -> ToolCall;
}
#[rustfmt::skip]
pub trait DeclaresSendTool {
    fn tool_name() -> ToolName;
    fn tool_spec() -> serde_json::Value;
}
#[rustfmt::skip]
pub trait ReadsToolCall {
    fn read_tool_call<N: ParamsCarrying>(input: N) -> std::option::Option<ToolCall>;
}
#[rustfmt::skip]
pub trait ReadsSendArguments {
    fn send_arguments(&self) -> std::option::Option<SendArguments>;
    fn content_sha256(&self) -> ContentSha256;
}
#[rustfmt::skip]
pub trait AnswersToolCall {
    fn tool_response(&self) -> serde_json::Value;
}
#[rustfmt::skip]
pub trait HandlesSendCall {
    fn handle_send_call<N: ToolCallCarrying>(&self, input: N) -> SendOutcome;
    fn handle_held_send_call<N: ToolCallCarrying>(&self, input: N) -> SendOutcome;
}
