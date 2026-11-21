//! The permaflows declared in Flow's configuration: the meta Metaflows
//! family, one row per Address, each the whole definition of that Address:
//! its workspace, its leave-outs, the belief modules an ordinary Launch of
//! that Address composes into the system prompt and into the first user
//! message, its kind, tool set, continuity, purpose and records. v7 (0.55.0)
//! carries the leave-outs; v8 adds effort. Earlier declarations upgrade on
//! open through `leave_out_upgrade` and `effort_upgrade`.

use super::{FlowStore, ReadsFlowMemory, StoreError};
pub(crate) use crate::generated::memory::StoredMetaMetaflowConfiguration;
use meta_signal_flow::{ConfigureMetaflow, RefreshMode};
use sema_engine::{
    EngineRecord, FamilyName, MemoryDefinition, MemoryVersion, QueryPlan, RecordKey, SchemaHash,
    TableDescriptor, TableName,
};
use signal_flow::Address;

pub(super) const META_METAFLOW_CONFIGURATION_TABLE_NAME: TableName =
    TableName::new("flow_nexus_meta_metaflow_configuration");
pub(super) const META_METAFLOW_CONFIGURATION_FAMILY: &str =
    "flow-nexus-meta-metaflow-configuration";
pub(super) const META_METAFLOW_CONFIGURATION_SCHEMA: &str =
    "flow-nexus-meta-metaflow-configuration-v9";

impl EngineRecord for StoredMetaMetaflowConfiguration {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(FlowStore::address_record_key(
            &self.configure_metaflow.address,
        ))
    }
}

impl MemoryDefinition for StoredMetaMetaflowConfiguration {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(9)
    }

    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label(META_METAFLOW_CONFIGURATION_SCHEMA)
    }
}

pub(super) trait DescribesMetaflowDeclarationTable {
    fn descriptor() -> TableDescriptor<StoredMetaMetaflowConfiguration>;
}

impl DescribesMetaflowDeclarationTable for StoredMetaMetaflowConfiguration {
    fn descriptor() -> TableDescriptor<StoredMetaMetaflowConfiguration> {
        TableDescriptor::new(
            META_METAFLOW_CONFIGURATION_TABLE_NAME,
            FamilyName::new(META_METAFLOW_CONFIGURATION_FAMILY),
            SchemaHash::for_label(META_METAFLOW_CONFIGURATION_SCHEMA),
        )
        .with_predecessor::<super::leave_out_upgrade::StoredMetaMetaflowConfigurationV6>()
        .with_predecessor::<super::effort_upgrade::StoredMetaMetaflowConfigurationV7>()
        .with_predecessor::<super::effort_upgrade::StoredMetaMetaflowConfigurationV8>()
    }
}

pub(crate) trait ReadsMetaflowDeclarations {
    /// The mode of this permaflow; undeclared seats keep Automatic behavior.
    fn refresh_mode(&self, address: &Address) -> Result<RefreshMode, StoreError> {
        Ok(self
            .metaflow_declaration(address)?
            .map(|declaration| declaration.refresh_mode)
            .unwrap_or(RefreshMode::Automatic))
    }
    /// The declaration of the metaflow at `address`, if Flow's configuration
    /// holds one.
    fn metaflow_declaration(
        &self,
        address: &Address,
    ) -> Result<Option<ConfigureMetaflow>, StoreError>;
}

impl ReadsMetaflowDeclarations for FlowStore {
    fn metaflow_declaration(
        &self,
        address: &Address,
    ) -> Result<Option<ConfigureMetaflow>, StoreError> {
        let records = self
            .engine
            .match_records(QueryPlan::key(
                self.meta_configuration_tables.metaflows,
                RecordKey::new(FlowStore::address_record_key(address)),
            ))?
            .records()
            .to_vec();
        match records.as_slice() {
            [] => Ok(None),
            [stored] => Ok(Some(stored.configure_metaflow.clone())),
            _ => Err(StoreError::StateInvariant),
        }
    }
}
