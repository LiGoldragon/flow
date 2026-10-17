#![allow(dead_code, non_camel_case_types, non_snake_case)]
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum HerdrUnread {
    ServerUnreachable,
    PaneUnreadable,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum FreshRefusal {
    ConfigurationUnread,
    NoLayerModel,
    Rejected(signal_flow::StartRejection),
    Unsettled,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum WakeRecovery {
    Terminal,
    AwaitingPane(signal_flow::HerdrRoute),
    AwaitingServer,
    Reconcile,
    Unresolved,
}
#[rustfmt::skip]
pub type PaneId = String;
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct AgentPaneEvent {
    pub herdr_session_name: signal_flow::HerdrSessionName,
    pub pane_id: PaneId,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct AwaitedPane {
    pub address: signal_flow::Address,
    pub herdr_route: signal_flow::HerdrRoute,
}
#[rustfmt::skip]
pub struct AwaitedPanes_State {
    pub(super) awaited_pane_vector: std::vec::Vec<AwaitedPane>,
    pub(super) pane_subscription_vector: std::vec::Vec<flow_nexus::PaneSubscription>,
    pub(super) launch_changes: flow_nexus::LaunchChanges,
}
#[rustfmt::skip]
#[derive(Clone)]
pub struct AwaitedPanes {
    pub(super) state: std::sync::Arc<std::sync::Mutex<AwaitedPanes_State>>,
}
#[rustfmt::skip]
pub trait ClassifiesWakeRefusal {
    fn recovery(&self) -> WakeRecovery;
}
#[rustfmt::skip]
pub trait PaneEventing {
    fn pane_event(&self) -> AgentPaneEvent;
}
#[rustfmt::skip]
pub trait WakesOnPaneEvent {
    fn wake_on_pane_event<N: PaneEventing>(
        &self,
        input: N,
    ) -> std::vec::Vec<signal_flow::Address>;
}
#[rustfmt::skip]
pub trait Addressed {
    fn address(&self) -> signal_flow::Address;
}
#[rustfmt::skip]
pub trait Routed {
    fn route(&self) -> signal_flow::HerdrRoute;
}
#[rustfmt::skip]
pub trait Subscribing {
    fn subscription(&self) -> flow_nexus::PaneSubscription;
}
#[rustfmt::skip]
pub trait HoldsAwaitedPanes {
    fn create() -> Self
    where
        Self: Sized;
    fn await_pane<N: Addressed, O: Routed>(&self, input_0: N, input_1: O) -> bool;
    fn forget<N: Addressed>(&self, input: N) -> bool;
    fn take<N: PaneEventing>(&self, input: N) -> std::vec::Vec<signal_flow::Address>;
    fn release<N: PaneEventing>(&self, input: N) -> bool;
    fn is_awaited<N: PaneEventing>(&self, input: N) -> bool;
    fn awaited(&self) -> std::vec::Vec<AwaitedPane>;
    fn unsubscribed_panes(&self) -> std::vec::Vec<AgentPaneEvent>;
    fn subscribed<N: Subscribing>(&self, input: N) -> bool;
    fn unsubscribed<N: PaneEventing>(&self, input: N) -> bool;
    fn close_all(&self) -> bool;
    fn changes(&self) -> flow_nexus::LaunchChanges;
}
#[rustfmt::skip]
const _: () = {
    fn assert_awaitedpanes_holdsawaitedpanes<T: HoldsAwaitedPanes>() {}
    let _ = assert_awaitedpanes_holdsawaitedpanes::<AwaitedPanes>;
};
