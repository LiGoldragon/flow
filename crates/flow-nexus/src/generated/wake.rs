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
