#![allow(dead_code, non_camel_case_types, non_snake_case)]
#[rustfmt::skip]
pub type Commit = String;
#[rustfmt::skip]
pub type Subject = String;
#[rustfmt::skip]
pub type ChangedPath = String;
#[rustfmt::skip]
pub type ChangedLine = String;
#[rustfmt::skip]
pub type ChangedLines = std::vec::Vec<ChangedLine>;
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct ChangedModule {
    pub changed_path: ChangedPath,
    pub changed_lines: ChangedLines,
}
#[rustfmt::skip]
pub type ChangedModules = std::vec::Vec<ChangedModule>;
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct LandedChange {
    pub commit: Commit,
    pub subject: Subject,
    pub changed_modules: ChangedModules,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum Step {
    Pending,
    Witnessed(String),
    Failed(String),
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum Retrained {
    Marked,
    Ended,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct Retraining {
    pub flow_id: signal_flow::FlowId,
    pub address: signal_flow::Address,
    pub retrained: Retrained,
}
#[rustfmt::skip]
pub type ModuleIdentity = String;
#[rustfmt::skip]
pub type ModuleIdentities = std::vec::Vec<ModuleIdentity>;
#[rustfmt::skip]
pub type RepositoryName = String;
#[rustfmt::skip]
pub type SourceRevision = String;
#[rustfmt::skip]
pub type SourceRelativePath = String;
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct ModuleSource {
    pub module_identity: ModuleIdentity,
    pub repository_name: RepositoryName,
    pub source_revision: SourceRevision,
    pub source_relative_path: SourceRelativePath,
}
#[rustfmt::skip]
pub type ModuleSources = std::vec::Vec<ModuleSource>;
#[rustfmt::skip]
pub type Base = String;
#[rustfmt::skip]
pub type ChangedPaths = std::vec::Vec<ChangedPath>;
#[rustfmt::skip]
pub type Deploy = Step;
#[rustfmt::skip]
pub type Retrain = Step;
#[rustfmt::skip]
pub type Retrainings = std::vec::Vec<Retraining>;
#[rustfmt::skip]
pub type Implement = Step;
#[rustfmt::skip]
pub type Owners = std::vec::Vec<signal_flow::Address>;
#[rustfmt::skip]
pub type Reported = bool;
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct Landing {
    pub commit: Commit,
    pub base: Base,
    pub subject: Subject,
    pub changed_paths: ChangedPaths,
    pub deploy: Deploy,
    pub retrain: Retrain,
    pub retrainings: Retrainings,
    pub implement: Implement,
    pub owners: Owners,
    pub reported: Reported,
}
#[rustfmt::skip]
pub type Landings = std::vec::Vec<Landing>;
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum RefreshCause {
    Landed(Commit),
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct RefreshDue {
    pub flow_id: signal_flow::FlowId,
    pub refresh_cause: RefreshCause,
}
#[rustfmt::skip]
pub type RefreshesDue = std::vec::Vec<RefreshDue>;
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct LandingOrder {
    pub address: signal_flow::Address,
    pub request: signal_flow::Request,
}
#[rustfmt::skip]
pub type LandingOrders = std::vec::Vec<LandingOrder>;
#[rustfmt::skip]
pub trait CommitNaming {
    fn commit(&self) -> Commit;
}
#[rustfmt::skip]
pub trait FlowNaming {
    fn flow_id(&self) -> signal_flow::FlowId;
}
#[rustfmt::skip]
pub trait ModuleListing {
    fn module_identities(&self) -> ModuleIdentities;
}
#[rustfmt::skip]
pub trait ModuleSourcing {
    fn module_source(&self) -> ModuleSource;
}
#[rustfmt::skip]
pub trait LandingCarrying {
    fn carried_landing(&self) -> Landing;
}
#[rustfmt::skip]
pub trait RefreshDueCarrying {
    fn carried_refresh_due(&self) -> RefreshDue;
}
#[rustfmt::skip]
pub trait LandingOrdering {
    fn landing_orders(&self) -> LandingOrders;
}
#[rustfmt::skip]
pub trait LandingWatching {
    fn landing_watch(&self) -> flow_ethos::LandingWatch;
}
#[rustfmt::skip]
pub trait ChangeCarrying {
    fn carried_change(&self) -> LandedChange;
}
#[rustfmt::skip]
pub trait ArgumentListing {
    fn arguments(&self) -> std::vec::Vec<String>;
}
#[rustfmt::skip]
pub trait NamesModule {
    fn module_identity(&self) -> std::option::Option<ModuleIdentity>;
    fn module_topic(&self) -> std::option::Option<String>;
}
#[rustfmt::skip]
pub trait ReadsContextRepository {
    fn git<N: ArgumentListing>(&self, input: N) -> std::result::Result<String, String>;
    fn main_commit(&self) -> std::result::Result<Commit, String>;
    fn git_directory(&self) -> std::result::Result<String, String>;
    fn module_paths(&self) -> std::result::Result<ChangedPaths, String>;
    fn commits_after<N: CommitNaming>(
        &self,
        input: N,
    ) -> std::result::Result<std::vec::Vec<Commit>, String>;
    fn landed_change<N: CommitNaming>(
        &self,
        input: N,
    ) -> std::result::Result<LandedChange, String>;
    fn includes<N: CommitNaming, O: CommitNaming>(&self, input_0: N, input_1: O) -> bool;
    fn depth<N: CommitNaming>(&self, input: N) -> i64;
}
#[rustfmt::skip]
pub trait DeploysContext {
    fn deploy<N: CommitNaming>(&self, input: N) -> std::result::Result<String, String>;
}
#[rustfmt::skip]
pub trait ResolvesModuleSources {
    fn module_sources<N: ModuleListing>(
        &self,
        input: N,
    ) -> std::result::Result<ModuleSources, String>;
}
#[rustfmt::skip]
pub trait ReportsLanding {
    fn finished_report(&self) -> std::option::Option<signal_flow::Request>;
    fn failure_report(&self) -> std::option::Option<signal_flow::Request>;
}
#[rustfmt::skip]
pub trait KeepsLandings {
    fn landing_row<N: CommitNaming>(
        &self,
        input: N,
    ) -> std::result::Result<std::option::Option<Landing>, flow_nexus::StoreError>;
    fn landing_rows(&self) -> std::result::Result<Landings, flow_nexus::StoreError>;
    fn write_landing<N: LandingCarrying>(
        &self,
        input: N,
    ) -> std::result::Result<bool, flow_nexus::StoreError>;
    fn queue_landing_orders<N: LandingCarrying, O: LandingOrdering>(
        &self,
        input_0: N,
        input_1: O,
    ) -> std::result::Result<
        std::vec::Vec<signal_flow::Address>,
        flow_nexus::StoreError,
    >;
    fn refresh_due<N: FlowNaming>(
        &self,
        input: N,
    ) -> std::result::Result<std::option::Option<RefreshDue>, flow_nexus::StoreError>;
    fn refreshes_due(&self) -> std::result::Result<RefreshesDue, flow_nexus::StoreError>;
    fn mark_refresh_due<N: LandingCarrying, O: RefreshDueCarrying>(
        &self,
        input_0: N,
        input_1: O,
    ) -> std::result::Result<bool, flow_nexus::StoreError>;
    fn clear_refresh_due<N: FlowNaming>(
        &self,
        input: N,
    ) -> std::result::Result<bool, flow_nexus::StoreError>;
}
#[rustfmt::skip]
pub trait RunsLandings {
    fn watch_landings<N: LandingWatching>(&self, input: N) -> bool;
    fn served_module<N: LandingWatching>(
        &self,
        input: N,
    ) -> std::result::Result<ModuleSource, String>;
    fn record_landings<N: LandingWatching>(
        &self,
        input: N,
    ) -> std::result::Result<Landings, String>;
    fn advance_landings<N: LandingWatching>(
        &self,
        input: N,
    ) -> std::result::Result<Landings, String>;
    fn retrain_flows<N: ModuleSourcing, O: LandingCarrying>(
        &self,
        input_0: N,
        input_1: O,
    ) -> std::result::Result<Landing, String>;
    fn implementation_orders<N: ChangeCarrying>(&self, input: N) -> LandingOrders;
    fn settle_refreshes(&self) -> bool;
    fn end_refresh<N: FlowNaming>(&self, input: N) -> bool;
}
