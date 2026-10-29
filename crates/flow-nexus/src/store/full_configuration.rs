use meta_signal_flow::{FullConfigurationReceipt, FullConfigurationRequest};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use sema_engine::{
    EngineRecord, FamilyName, MemoryDefinition, MemoryVersion, RecordKey, SchemaHash,
    TableDescriptor, TableName,
};

pub(super) const STATE_TABLE: TableName = TableName::new("flow_nexus_full_configuration_state");
pub(super) const RECEIPT_TABLE: TableName =
    TableName::new("flow_nexus_full_configuration_receipts");

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct StoredFullConfigurationState {
    pub(crate) revision: i64,
}
impl EngineRecord for StoredFullConfigurationState {
    fn record_key(&self) -> RecordKey {
        RecordKey::new("configuration")
    }
}
impl MemoryDefinition for StoredFullConfigurationState {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(1)
    }
    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label("flow-nexus-full-configuration-state-v1")
    }
}
impl StoredFullConfigurationState {
    pub(crate) fn descriptor() -> TableDescriptor<Self> {
        TableDescriptor::new(
            STATE_TABLE,
            FamilyName::new("flow-nexus-full-configuration-state"),
            Self::schema_hash(),
        )
    }
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct StoredFullConfigurationReceipt {
    pub(crate) request: FullConfigurationRequest,
    pub(crate) receipt: FullConfigurationReceipt,
}
impl EngineRecord for StoredFullConfigurationReceipt {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(&self.request.configuration_request_id)
    }
}
impl MemoryDefinition for StoredFullConfigurationReceipt {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(1)
    }
    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label("flow-nexus-full-configuration-receipt-v1")
    }
}
impl StoredFullConfigurationReceipt {
    pub(crate) fn descriptor() -> TableDescriptor<Self> {
        TableDescriptor::new(
            RECEIPT_TABLE,
            FamilyName::new("flow-nexus-full-configuration-receipt"),
            Self::schema_hash(),
        )
    }
}
