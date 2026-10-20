//! The metaflows declared in Flow's configuration: the meta Metaflows
//! family, one row per Address, each naming the context modules an ordinary
//! Launch of that Address composes into the system prompt and into the first
//! user message. A store written before metaflows were declared opens with
//! the table added and holds none.

use super::{FlowStore, ReadsFlowMemory, StoreError};
use meta_signal_flow::ConfigureMetaflow;
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use sema_engine::{
    EngineRecord, FamilyName, QueryPlan, RecordKey, SchemaHash, TableDescriptor, TableName,
};
use signal_flow::Address;

pub(super) const META_METAFLOW_CONFIGURATION_TABLE_NAME: TableName =
    TableName::new("flow_nexus_meta_metaflow_configuration");
pub(super) const META_METAFLOW_CONFIGURATION_FAMILY: &str =
    "flow-nexus-meta-metaflow-configuration";
pub(super) const META_METAFLOW_CONFIGURATION_SCHEMA: &str =
    "flow-nexus-meta-metaflow-configuration-v1";

/// One declared metaflow, keyed by its Address.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct StoredMetaMetaflowConfiguration {
    pub metaflow: ConfigureMetaflow,
}

impl EngineRecord for StoredMetaMetaflowConfiguration {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(FlowStore::address_record_key(&self.metaflow.address))
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
    }
}

pub(crate) trait ReadsMetaflowDeclarations {
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
            [stored] => Ok(Some(stored.metaflow.clone())),
            _ => Err(StoreError::StateInvariant),
        }
    }
}
