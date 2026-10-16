//! The system-prompt bundle Flow 0.34.6 kept for each Address it started.
//! Since 0.35.0 a fresh wake composes its context again from the module
//! identities its metaflow's last launch named, so the rows are neither read
//! nor written; the table stays registered so a store that holds them opens.

use super::{FlowStore, ReadsFlowMemory};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use sema_engine::{EngineRecord, FamilyName, RecordKey, SchemaHash, TableDescriptor, TableName};
use signal_flow::Address;

pub(super) const START_BUNDLE_TABLE_NAME: TableName = TableName::new("flow_nexus_start_bundles");
pub(super) const START_BUNDLE_FAMILY: &str = "flow-nexus-start-bundle";
pub(super) const START_BUNDLE_SCHEMA: &str = "flow-nexus-start-bundle-v1";

/// The caller's bundle bytes of the last Start at an Address.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub struct StartBundle {
    pub address: Address,
    pub bytes: Vec<u8>,
}

impl EngineRecord for StartBundle {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(FlowStore::address_record_key(&self.address))
    }
}

pub(super) trait DescribesStartBundleTable {
    fn descriptor() -> TableDescriptor<StartBundle>;
}

impl DescribesStartBundleTable for StartBundle {
    fn descriptor() -> TableDescriptor<StartBundle> {
        TableDescriptor::new(
            START_BUNDLE_TABLE_NAME,
            FamilyName::new(START_BUNDLE_FAMILY),
            SchemaHash::for_label(START_BUNDLE_SCHEMA),
        )
    }
}
