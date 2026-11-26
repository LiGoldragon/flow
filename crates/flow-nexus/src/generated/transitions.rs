#![allow(
    dead_code,
    non_camel_case_types,
    non_snake_case,
    clippy::type_complexity
)]
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct Caused {
    pub seat: flow_nexus::generated::overriding::Seat,
    pub cause: flow_nexus::generated::memory::Cause,
    pub moment: flow_nexus::generated::memory::Moment,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct Before(pub flow_nexus::generated::turn_ledger::ThreadStanding);
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct After(pub flow_nexus::generated::turn_ledger::ThreadStanding);
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct Stepped_Data {
    pub before: Before,
    pub after: After,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct Measured_Data {
    pub thread_standing: flow_nexus::generated::turn_ledger::ThreadStanding,
    pub tokens: signal_flow::Tokens,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum CodexReport {
    Stepped(Stepped_Data),
    Measured(Measured_Data),
    Closed,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct ClaudeReport {
    pub flow_id: signal_flow::FlowId,
    pub hook_event: signal_flow::Event,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct ReportedEvent {
    pub flow_id: signal_flow::FlowId,
    pub event: flow_nexus::generated::memory::Event,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct CodexFlowReport {
    pub flow_id: signal_flow::FlowId,
    pub codex_report: CodexReport,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct AskedFlow(pub signal_flow::FlowId);
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum TransitionWrite {
    Appended(flow_nexus::generated::memory::Flow),
    Opened(flow_nexus::generated::memory::Flow),
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum ReceiptTurn {
    Running,
    Ended,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct Arrival {
    pub caused: Caused,
    pub receipt_turn: ReceiptTurn,
}
#[rustfmt::skip]
pub trait LeadsToState {
    fn leads_to(
        &self,
        input: std::option::Option<meta_signal_flow::generated::signal::State>,
    ) -> std::option::Option<meta_signal_flow::generated::signal::State>;
}
#[rustfmt::skip]
pub trait TypesCodexReport {
    fn codex_event(
        &self,
        input: std::option::Option<signal_flow::Tokens>,
    ) -> std::option::Option<flow_nexus::generated::memory::Event>;
}
#[rustfmt::skip]
pub trait RecordsTransitions {
    fn record_caused(
        &self,
        input: Caused,
    ) -> std::result::Result<
        std::option::Option<flow_nexus::generated::memory::Transition>,
        flow_nexus::store::StoreError,
    >;
    fn arrivals(
        &self,
        input: std::vec::Vec<Arrival>,
    ) -> std::result::Result<
        std::vec::Vec<TransitionWrite>,
        flow_nexus::store::StoreError,
    >;
    fn state_of(
        &self,
        input: AskedFlow,
    ) -> std::result::Result<
        std::option::Option<meta_signal_flow::generated::signal::State>,
        flow_nexus::store::StoreError,
    >;
}
#[rustfmt::skip]
pub trait ReportsEvents {
    fn claude_event(
        &self,
        input: ClaudeReport,
    ) -> std::option::Option<flow_nexus::generated::memory::Event>;
    fn report_event(&self, input: ReportedEvent) -> bool;
    fn report_codex(&self, input: CodexFlowReport) -> bool;
}
#[rustfmt::skip]
const _: () = {
    fn assert_cause_leadstostate<T: LeadsToState>() {}
    let _ = assert_cause_leadstostate::<flow_nexus::generated::memory::Cause>;
};
#[rustfmt::skip]
const _: () = {
    fn assert_caused_leadstostate<T: LeadsToState>() {}
    let _ = assert_caused_leadstostate::<Caused>;
};
#[rustfmt::skip]
const _: () = {
    fn assert_arrival_leadstostate<T: LeadsToState>() {}
    let _ = assert_arrival_leadstostate::<Arrival>;
};
#[rustfmt::skip]
const _: () = {
    fn assert_codexreport_typescodexreport<T: TypesCodexReport>() {}
    let _ = assert_codexreport_typescodexreport::<CodexReport>;
};
#[rustfmt::skip]
const _: () = {
    fn assert_flowstore_recordstransitions<T: RecordsTransitions>() {}
    let _ = assert_flowstore_recordstransitions::<flow_nexus::store::FlowStore>;
};
#[rustfmt::skip]
const _: () = {
    fn assert_runningnexus_reportsevents<T: ReportsEvents>() {}
    let _ = assert_runningnexus_reportsevents::<flow_nexus::RunningNexus>;
};
