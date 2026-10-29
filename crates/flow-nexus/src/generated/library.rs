#![allow(dead_code, non_camel_case_types, non_snake_case)]
#[rustfmt::skip]
pub type OrdinarySocketPath = String;
#[rustfmt::skip]
pub type MetaSocketPath = String;
#[rustfmt::skip]
pub type StorePath = String;
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct Start {
    pub ordinary_socket_path: OrdinarySocketPath,
    pub meta_socket_path: MetaSocketPath,
    pub store_path: StorePath,
}
#[rustfmt::skip]
pub type Executable = String;
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct HarnessOverride {
    pub harness_name: meta_signal_flow::HarnessName,
    pub executable: Executable,
    pub home: flow_ethos::Home,
    pub control_socket_path_option: std::option::Option<flow_ethos::ControlSocketPath>,
    pub harness_model_vector: std::vec::Vec<meta_signal_flow::HarnessModel>,
}
#[rustfmt::skip]
pub type HarnessOverrides = std::vec::Vec<HarnessOverride>;
