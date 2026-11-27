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
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum AddressLease {
    Wake,
    Effect,
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
pub trait Addressed {
    fn address(&self) -> signal_flow::Address;
}
#[rustfmt::skip]
pub trait ParkedLaunch {
    fn launch(&self) -> signal_flow::ComposedLaunch;
    fn origin(&self) -> signal_flow::Origin;
    fn attempt(&self) -> signal_flow::LaunchAttempt;
}
#[rustfmt::skip]
pub trait HeldLaunch {
    fn request_id(&self) -> signal_flow::LaunchRequestId;
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
    fn permaflow_held<N: Addressed>(
        &self,
        input: N,
    ) -> std::result::Result<bool, flow_nexus::store::StoreError>;
    fn hold_permaflow<N: Addressed>(
        &self,
        input: N,
    ) -> std::result::Result<bool, flow_nexus::store::StoreError>;
    fn release_permaflow<N: Addressed>(
        &self,
        input: N,
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
    fn permaflow_held<N: Addressed>(&self, input: N) -> bool;
    fn state_now(
        &self,
        input: Seat,
    ) -> std::option::Option<meta_signal_flow::generated::signal::State>;
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
pub trait SerializesPermaflowEffects {
    fn effect<N: Addressed>(&self, input: N) -> flow_nexus::waking::WakingLease;
}
#[rustfmt::skip]
pub trait SerializesPermaflowHolds {
    fn establish_permaflow_hold<N: Addressed>(
        &self,
        input: N,
    ) -> std::result::Result<bool, flow_nexus::store::StoreError>;
}
#[rustfmt::skip]
pub trait AdmitsPermaflowControl {
    fn machine_control(
        &self,
        input: Subject,
    ) -> std::result::Result<
        std::option::Option<flow_nexus::waking::WakingLease>,
        meta_signal_flow::generated::signal::Output,
    >;
    fn ordinary_control(
        &self,
        input: Subject,
    ) -> std::result::Result<
        std::option::Option<flow_nexus::waking::WakingLease>,
        signal_flow::Output,
    >;
}
#[rustfmt::skip]
pub trait ParksHeldLaunches {
    fn park_held_launch<N: ParkedLaunch>(
        &self,
        input: N,
    ) -> std::result::Result<
        signal_flow::LaunchAttemptReservation,
        flow_nexus::store::StoreError,
    >;
    fn held_launches(
        &self,
    ) -> std::result::Result<
        std::vec::Vec<flow_nexus::generated::memory::StoredHeldLaunch>,
        flow_nexus::store::StoreError,
    >;
    fn marked_held_launch<N: HeldLaunch>(
        &self,
        input: N,
    ) -> std::result::Result<bool, flow_nexus::store::StoreError>;
}
#[rustfmt::skip]
pub trait RecoverableRestart {
    fn record(&self) -> meta_signal_flow::generated::signal::RestartRecord;
}
#[rustfmt::skip]
pub trait RecoversRecordedRestarts {
    fn recover_restart<N: RecoverableRestart>(&self, input: N) -> bool;
}
#[rustfmt::skip]
pub trait ResumesPermaflowWork {
    fn resume_permaflow<N: Addressed>(&self, input: N) -> bool;
}
#[rustfmt::skip]
pub trait ResumesPermaflowCleanup {
    fn resume_permaflow_cleanup<N: Addressed>(&self, input: N) -> bool;
}
#[rustfmt::skip]
pub trait ResumesHeldLaunches {
    fn resume_held_launches<N: Addressed>(&self, input: N) -> bool;
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
    fn assert_flowstore_parksheldlaunches<T: ParksHeldLaunches>() {}
    let _ = assert_flowstore_parksheldlaunches::<flow_nexus::store::FlowStore>;
    fn assert_flowstore_recognizesinterruptedlaunches<T: RecognizesInterruptedLaunches>() {}
    let _ = assert_flowstore_recognizesinterruptedlaunches::<
        flow_nexus::store::FlowStore,
    >;
};
#[rustfmt::skip]
const _: () = {
    fn assert_runningnexus_heedsoverrides<T: HeedsOverrides>() {}
    let _ = assert_runningnexus_heedsoverrides::<flow_nexus::RunningNexus>;
    fn assert_runningnexus_serializespermaflowholds<T: SerializesPermaflowHolds>() {}
    let _ = assert_runningnexus_serializespermaflowholds::<flow_nexus::RunningNexus>;
    fn assert_runningnexus_admitspermaflowcontrol<T: AdmitsPermaflowControl>() {}
    let _ = assert_runningnexus_admitspermaflowcontrol::<flow_nexus::RunningNexus>;
    fn assert_runningnexus_resumesheldlaunches<T: ResumesHeldLaunches>() {}
    let _ = assert_runningnexus_resumesheldlaunches::<flow_nexus::RunningNexus>;
    fn assert_runningnexus_resumespermaflowwork<T: ResumesPermaflowWork>() {}
    let _ = assert_runningnexus_resumespermaflowwork::<flow_nexus::RunningNexus>;
    fn assert_runningnexus_resumespermaflowcleanup<T: ResumesPermaflowCleanup>() {}
    let _ = assert_runningnexus_resumespermaflowcleanup::<flow_nexus::RunningNexus>;
    fn assert_runningnexus_recoversrecordedrestarts<T: RecoversRecordedRestarts>() {}
    let _ = assert_runningnexus_recoversrecordedrestarts::<flow_nexus::RunningNexus>;
    fn assert_runningnexus_interruptsrunningturns<T: InterruptsRunningTurns>() {}
    let _ = assert_runningnexus_interruptsrunningturns::<flow_nexus::RunningNexus>;
};
#[rustfmt::skip]
const _: () = {
    fn assert_wakingmetaflows_serializespermafloweffects<T: SerializesPermaflowEffects>() {}
    let _ = assert_wakingmetaflows_serializespermafloweffects::<
        flow_nexus::waking::WakingMetaflows,
    >;
};
#[rustfmt::skip]
const _: () = {
    fn assert_address_addressed<T: Addressed>() {}
    let _ = assert_address_addressed::<signal_flow::Address>;
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
    fn assert_storedpermaflowhold_describesoverridetable<T: DescribesOverrideTable>() {}
    let _ = assert_storedpermaflowhold_describesoverridetable::<
        flow_nexus::generated::memory::StoredPermaflowHold,
    >;
};
#[rustfmt::skip]
const _: () = {
    fn assert_storedheldlaunch_describesoverridetable<T: DescribesOverrideTable>() {}
    let _ = assert_storedheldlaunch_describesoverridetable::<
        flow_nexus::generated::memory::StoredHeldLaunch,
    >;
    fn assert_storedheldlaunch_heldlaunch<T: HeldLaunch>() {}
    let _ = assert_storedheldlaunch_heldlaunch::<
        flow_nexus::generated::memory::StoredHeldLaunch,
    >;
};
#[rustfmt::skip]
const _: () = {
    fn assert_launchreservation_parkedlaunch<T: ParkedLaunch>() {}
    let _ = assert_launchreservation_parkedlaunch::<
        flow_nexus::generated::operation::LaunchReservation,
    >;
};
#[rustfmt::skip]
const _: () = {
    fn assert_restartrecord_recoverablerestart<T: RecoverableRestart>() {}
    let _ = assert_restartrecord_recoverablerestart::<
        meta_signal_flow::generated::signal::RestartRecord,
    >;
};
#[rustfmt::skip]
const _: () = {
    fn assert_launchrequestid_heldlaunch<T: HeldLaunch>() {}
    let _ = assert_launchrequestid_heldlaunch::<signal_flow::LaunchRequestId>;
};
#[rustfmt::skip]
const _: () = {
    fn assert_state_entersbyoverride<T: EntersByOverride>() {}
    let _ = assert_state_entersbyoverride::<meta_signal_flow::generated::signal::State>;
};
