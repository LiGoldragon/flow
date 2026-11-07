//! The meta Nexus family: the one declared `ConfigureNexus`, the sockets and
//! endpoints a Nexus serves and reaches.

use meta_signal_flow::ConfigureNexus;
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use sema_engine::{
    EngineRecord, FamilyName, MemoryDefinition, MemoryVersion, RecordKey, SchemaHash,
    TableDescriptor,
};

pub(super) const META_NEXUS_CONFIGURATION_FAMILY: &str = "flow-nexus-meta-nexus-configuration";
pub(super) const META_NEXUS_CONFIGURATION_SCHEMA: &str = "flow-nexus-meta-nexus-configuration-v4";

/// The declared Nexus, one row.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct StoredMetaNexusConfiguration {
    pub(crate) nexus: ConfigureNexus,
}

impl EngineRecord for StoredMetaNexusConfiguration {
    fn record_key(&self) -> RecordKey {
        RecordKey::new("nexus")
    }
}

impl MemoryDefinition for StoredMetaNexusConfiguration {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(4)
    }

    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label(META_NEXUS_CONFIGURATION_SCHEMA)
    }
}

/// The Nexus family's descriptor.
pub(crate) trait DescribesNexusConfigurationTable {
    fn descriptor() -> TableDescriptor<StoredMetaNexusConfiguration>;
}

impl DescribesNexusConfigurationTable for StoredMetaNexusConfiguration {
    fn descriptor() -> TableDescriptor<StoredMetaNexusConfiguration> {
        TableDescriptor::new(
            super::META_NEXUS_CONFIGURATION_TABLE_NAME,
            FamilyName::new(META_NEXUS_CONFIGURATION_FAMILY),
            StoredMetaNexusConfiguration::schema_hash(),
        )
    }
}
