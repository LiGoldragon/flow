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
pub type Path = String;
#[rustfmt::skip]
pub type Reason = String;
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct StoreRefusal {
    pub path: Path,
    pub reason: Reason,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum StartRefusal {
    Store(StoreRefusal),
}
