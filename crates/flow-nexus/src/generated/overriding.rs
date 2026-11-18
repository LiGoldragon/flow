#![allow(
    dead_code,
    non_camel_case_types,
    non_snake_case,
    clippy::type_complexity
)]
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum Subject {
    Named(signal_flow::FlowId),
    Permaflow(signal_flow::Address),
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct Seat {
    pub flow_id: signal_flow::FlowId,
    pub address: signal_flow::Address,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct Transitioned {
    pub seat: Seat,
    pub transition: flow_nexus::generated::memory::Transition,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum Holding {
    Held(flow_nexus::generated::memory::Moment),
    Acting,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct CodexThread(pub String);
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum TurnInterruption {
    Rested,
    Interrupted,
    PaneUnreadable,
    KeysRefused,
    AppServerRefused(String),
    StoreRefused(String),
}
#[rustfmt::skip]
pub trait RemembersOverrides {
    fn record_transition(
        &self,
        input: Transitioned,
    ) -> std::result::Result<bool, flow_nexus::store::StoreError>;
    fn transitions(
        &self,
    ) -> std::result::Result<
        std::vec::Vec<flow_nexus::generated::memory::Flow>,
        flow_nexus::store::StoreError,
    >;
    fn holding(&self) -> std::result::Result<Holding, flow_nexus::store::StoreError>;
    fn switch(
        &self,
        input: Holding,
    ) -> std::result::Result<bool, flow_nexus::store::StoreError>;
    fn forget_flow(
        &self,
        input: Transitioned,
    ) -> std::result::Result<bool, flow_nexus::store::StoreError>;
    fn seat(
        &self,
        input: Subject,
    ) -> std::result::Result<std::option::Option<Seat>, flow_nexus::store::StoreError>;
    fn seats(
        &self,
    ) -> std::result::Result<std::vec::Vec<Seat>, flow_nexus::store::StoreError>;
}
#[rustfmt::skip]
pub trait HeedsOverrides {
    fn held(&self) -> bool;
    fn state_now(&self, input: Seat) -> meta_signal_flow::generated::signal::State;
    fn seat_named(
        &self,
        input: Subject,
    ) -> std::result::Result<Seat, meta_signal_flow::generated::signal::Output>;
    fn record_override(
        &self,
        input: Transitioned,
    ) -> std::result::Result<bool, meta_signal_flow::generated::signal::Output>;
    fn record_named(
        &self,
        input: Subject,
    ) -> std::result::Result<bool, meta_signal_flow::generated::signal::Output>;
    fn record_every_seat(
        &self,
    ) -> std::result::Result<bool, meta_signal_flow::generated::signal::Output>;
}
#[rustfmt::skip]
pub trait LaunchAttemptCarrying {
    fn carried_launch_attempt(&self) -> signal_flow::LaunchAttempt;
}
#[rustfmt::skip]
pub trait RecognizesInterruptedLaunches {
    fn launch_is_interrupted<N: LaunchAttemptCarrying>(
        &self,
        input: N,
    ) -> std::result::Result<bool, flow_nexus::store::StoreError>;
}
#[rustfmt::skip]
pub trait DescribesOverrideTable {
    fn descriptor() -> sema_engine::TableDescriptor<Self>
    where
        Self: Sized;
}
#[rustfmt::skip]
pub trait EntersByOverride {
    fn overridden(&self) -> flow_nexus::generated::memory::Transition;
}
#[rustfmt::skip]
pub trait InterruptsRunningTurns {
    fn interrupt_running_turn(&self, input: Seat) -> TurnInterruption;
}
#[rustfmt::skip]
pub trait InterruptsCodexThreads {
    fn interrupt_thread(
        &self,
        input: CodexThread,
    ) -> std::result::Result<
        TurnInterruption,
        flow_nexus::codex::CodexAdapterUnavailable,
    >;
}
#[rustfmt::skip]
const _: () = {
    fn assert_launchattempt_launchattemptcarrying<T: LaunchAttemptCarrying>() {}
    let _ = assert_launchattempt_launchattemptcarrying::<signal_flow::LaunchAttempt>;
};
#[rustfmt::skip]
const _: () = {
    fn assert_flowstore_remembersoverrides<T: RemembersOverrides>() {}
    let _ = assert_flowstore_remembersoverrides::<flow_nexus::store::FlowStore>;
    fn assert_flowstore_recognizesinterruptedlaunches<T: RecognizesInterruptedLaunches>() {}
    let _ = assert_flowstore_recognizesinterruptedlaunches::<
        flow_nexus::store::FlowStore,
    >;
};
#[rustfmt::skip]
const _: () = {
    fn assert_runningnexus_heedsoverrides<T: HeedsOverrides>() {}
    let _ = assert_runningnexus_heedsoverrides::<flow_nexus::RunningNexus>;
    fn assert_runningnexus_interruptsrunningturns<T: InterruptsRunningTurns>() {}
    let _ = assert_runningnexus_interruptsrunningturns::<flow_nexus::RunningNexus>;
};
#[rustfmt::skip]
const _: () = {
    fn assert_codexadapter_interruptscodexthreads<T: InterruptsCodexThreads>() {}
    let _ = assert_codexadapter_interruptscodexthreads::<
        flow_nexus::codex::CodexAdapter,
    >;
};
#[rustfmt::skip]
const _: () = {
    fn assert_flow_describesoverridetable<T: DescribesOverrideTable>() {}
    let _ = assert_flow_describesoverridetable::<flow_nexus::generated::memory::Flow>;
};
#[rustfmt::skip]
const _: () = {
    fn assert_storedhold_describesoverridetable<T: DescribesOverrideTable>() {}
    let _ = assert_storedhold_describesoverridetable::<
        flow_nexus::generated::memory::StoredHold,
    >;
};
#[rustfmt::skip]
const _: () = {
    fn assert_state_entersbyoverride<T: EntersByOverride>() {}
    let _ = assert_state_entersbyoverride::<meta_signal_flow::generated::signal::State>;
};
