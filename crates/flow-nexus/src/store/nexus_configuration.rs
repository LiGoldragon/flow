//! The meta Nexus family: the one declared `ConfigureNexus`, the sockets and
//! endpoints a Nexus serves and reaches.

pub(crate) use crate::generated::memory::StoredMetaNexusConfiguration;
use sema_engine::{
    EngineRecord, FamilyName, MemoryDefinition, MemoryVersion, RecordKey, SchemaHash,
    TableDescriptor,
};

pub(super) const META_NEXUS_CONFIGURATION_FAMILY: &str = "flow-nexus-meta-nexus-configuration";
pub(super) const META_NEXUS_CONFIGURATION_SCHEMA: &str = "flow-nexus-meta-nexus-configuration-v5";

impl EngineRecord for StoredMetaNexusConfiguration {
    fn record_key(&self) -> RecordKey {
        RecordKey::new("nexus")
    }
}

impl MemoryDefinition for StoredMetaNexusConfiguration {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(5)
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
        .with_predecessor::<super::living_upgrade::StoredNexusBeforeSession>()
    }
}
