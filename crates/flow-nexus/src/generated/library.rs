#![allow(dead_code, non_camel_case_types, non_snake_case)]
#[rustfmt::skip]
pub type PlaceName = String;
#[rustfmt::skip]
pub type Runtime = String;
#[rustfmt::skip]
pub type State = String;
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct Place {
    pub place_name: PlaceName,
    pub runtime: Runtime,
    pub state: State,
}
