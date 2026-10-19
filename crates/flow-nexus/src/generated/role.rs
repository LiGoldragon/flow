#![allow(dead_code, non_camel_case_types, non_snake_case)]
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum RoleRefusal {
    ComposerUnavailable,
    ComposerRejected,
    RoleMismatch,
    PayloadMismatch,
    ManifestMismatch,
    ProvenanceMismatch,
    UnsupportedPolicy,
}
#[rustfmt::skip]
pub type SubagentRoleNames = std::vec::Vec<String>;
#[rustfmt::skip]
pub type SubagentType = String;
#[rustfmt::skip]
pub type Nested = bool;
#[rustfmt::skip]
pub type RequestsModel = bool;
#[rustfmt::skip]
pub type RequestsEffort = bool;
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct AgentCall {
    pub subagent_type: SubagentType,
    pub nested: Nested,
    pub requests_model: RequestsModel,
    pub requests_effort: RequestsEffort,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum AgentCaller {
    Resolved(signal_flow::Address),
    Unresolved,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum AgentDenial {
    NestedCaller,
    ModelOverridden,
    EffortOverridden,
    CallerUnresolved,
    PolicyUnavailable,
    CallerNotPermitted,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum AgentDecision {
    Ungoverned,
    Permitted,
    Denied(AgentDenial),
}
#[rustfmt::skip]
pub trait RoleSelecting {
    fn role_selection(&self) -> flow_nexus::RoleSelection;
}
#[rustfmt::skip]
pub trait RoleComposing {
    fn role_composition(&self) -> flow_nexus::RoleComposition;
}
#[rustfmt::skip]
pub trait Addressed {
    fn addressed(&self) -> signal_flow::Address;
}
#[rustfmt::skip]
pub trait AgentCalling {
    fn agent_call(&self) -> AgentCall;
}
#[rustfmt::skip]
pub trait ResolvesAgentCaller {
    fn agent_caller(&self) -> AgentCaller;
}
#[rustfmt::skip]
pub trait OpensSubagentRoles {
    fn from_environment() -> Self
    where
        Self: Sized;
}
#[rustfmt::skip]
pub trait ComposesRole {
    fn compose_role<N: RoleSelecting>(
        &self,
        input: N,
    ) -> std::result::Result<flow_nexus::RoleComposition, RoleRefusal>;
}
#[rustfmt::skip]
pub trait VerifiesRole {
    fn verified_role<N: RoleSelecting, O: RoleComposing>(
        input_0: N,
        input_1: O,
    ) -> std::result::Result<Self, RoleRefusal>
    where
        Self: Sized;
}
#[rustfmt::skip]
pub trait ReadsVerifiedRole {
    fn role_name(&self) -> String;
    fn permits<N: Addressed>(&self, input: N) -> bool;
    fn native_definition(&self) -> std::result::Result<serde_json::Value, RoleRefusal>;
}
#[rustfmt::skip]
pub trait ComposesSubagentRoles {
    fn governs<N: AgentCalling>(&self, input: N) -> bool;
    fn permitted_roles<N: Addressed>(
        &self,
        input: N,
    ) -> std::result::Result<std::vec::Vec<flow_nexus::VerifiedRole>, RoleRefusal>;
    fn agents_argument<N: Addressed>(
        &self,
        input: N,
    ) -> std::result::Result<std::option::Option<String>, RoleRefusal>;
}
#[rustfmt::skip]
pub trait AuthorizesAgentCall {
    fn decide<N: AgentCalling, O: ResolvesAgentCaller>(
        &self,
        input_0: N,
        input_1: O,
    ) -> AgentDecision;
}
