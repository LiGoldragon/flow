#![allow(dead_code, non_camel_case_types, non_snake_case)]
#[rustfmt::skip]
pub type FlowId = String;
#[rustfmt::skip]
pub type Topic = String;
#[rustfmt::skip]
pub type Blake3 = String;
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum Layer {
    Primary,
    Secondary,
    Tertiary,
    Quaternary,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum Subaspect {
    Vision,
    Intent,
    Knowledge,
    Operation,
    Trial,
    Compensation,
}
#[rustfmt::skip]
pub type Repository = String;
#[rustfmt::skip]
pub type Hash = Blake3;
#[rustfmt::skip]
pub type Path = String;
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct Source {
    pub repository: Repository,
    pub hash: Hash,
    pub path: Path,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct Key {
    pub subaspect: Subaspect,
    pub topic: Topic,
}
#[rustfmt::skip]
pub type Context = String;
#[rustfmt::skip]
pub type Verbatim = String;
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct Said {
    pub context: Context,
    pub verbatim: Verbatim,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum Request {
    Order(String),
    Question(String),
    Psyche(Said),
    Psyches(std::vec::Vec<Said>),
    Result(String),
    Notice(String),
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum Aspect {
    Psyche,
    Mind,
    Field,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum FlowAspect {
    Psyche,
    Mind,
    Field,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct Address {
    pub aspect: Aspect,
    pub topic: Topic,
    pub layer: Layer,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum Recipient {
    Address(Address),
    Up,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum State {
    Awake(FlowId),
    Asleep,
    Ended,
}
#[rustfmt::skip]
pub type Past = std::vec::Vec<FlowId>;
#[rustfmt::skip]
pub type Queue = std::vec::Vec<Request>;
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct Metaflow {
    pub address: Address,
    pub state: State,
    pub past: Past,
    pub queue: Queue,
}
#[rustfmt::skip]
pub type Until = i64;
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct Lock {
    pub sender: Sender,
    pub address: Address,
    pub until: Until,
}
#[rustfmt::skip]
pub type Pid = i64;
#[rustfmt::skip]
pub type Started = i64;
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct Process {
    pub pid: Pid,
    pub started: Started,
}
#[rustfmt::skip]
pub type Sender = Address;
#[rustfmt::skip]
pub type OrdinarySocketPath = String;
#[rustfmt::skip]
pub type MetaSocketPath = String;
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct Start {
    pub ordinary_socket_path: OrdinarySocketPath,
    pub meta_socket_path: MetaSocketPath,
}
#[rustfmt::skip]
pub type SourceRoot = String;
#[rustfmt::skip]
pub type ClientPath = String;
#[rustfmt::skip]
pub type Home = String;
#[rustfmt::skip]
pub type ControlSocketPath = String;
#[rustfmt::skip]
pub type ModelName = String;
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct CodexEndpoint {
    pub client_path: ClientPath,
    pub home: Home,
    pub control_socket_path: ControlSocketPath,
    pub model_name_vector: std::vec::Vec<ModelName>,
}
#[rustfmt::skip]
pub type StableCodex = CodexEndpoint;
#[rustfmt::skip]
pub type NextCodex = CodexEndpoint;
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum HarnessKind {
    Codex,
    Claude,
}
#[rustfmt::skip]
pub type KeyName = String;
#[rustfmt::skip]
pub type CommandSigil = String;
#[rustfmt::skip]
pub type InterruptKeys = std::vec::Vec<KeyName>;
#[rustfmt::skip]
pub type SubmitKeys = std::vec::Vec<KeyName>;
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct HarnessProfile {
    pub harness_kind: HarnessKind,
    pub command_sigil_vector: std::vec::Vec<CommandSigil>,
    pub interrupt_keys: InterruptKeys,
    pub submit_keys: SubmitKeys,
}
#[rustfmt::skip]
pub type HarnessProfiles = std::vec::Vec<HarnessProfile>;
#[rustfmt::skip]
pub type MetaAspects = std::vec::Vec<FlowAspect>;
#[rustfmt::skip]
pub type MessageNexusPath = String;
#[rustfmt::skip]
pub type MessageNexusBinary = String;
#[rustfmt::skip]
pub type Lease = i64;
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct Nexus {
    pub source_root: SourceRoot,
    pub stable_codex: StableCodex,
    pub next_codex: NextCodex,
    pub harness_profiles: HarnessProfiles,
    pub meta_aspects: MetaAspects,
    pub message_nexus_path: MessageNexusPath,
    pub message_nexus_binary: MessageNexusBinary,
    pub lease: Lease,
}
