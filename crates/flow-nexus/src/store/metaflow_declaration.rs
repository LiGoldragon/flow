//! The metaflows declared in Flow's configuration: the meta Metaflows
//! family, one row per Address, each naming the context modules an ordinary
//! Launch of that Address composes into the system prompt and into the first
//! user message. A store written before metaflows were declared opens with
//! the table added and holds none.

use super::{FlowStore, ReadsFlowMemory, StoreError};
use meta_signal_flow::ConfigureMetaflow;
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use sema_engine::{
    Decision, Engine, EngineRecord, FamilyName, MemoryDefinition, MemoryUpgrades, MemoryVersion,
    QueryPlan, RecordKey, SchemaHash, TableDescriptor, TableName, TableReference, UpgradeFrom,
    UpgradeOutcome,
};
use signal_flow::Address;

pub(super) const META_METAFLOW_CONFIGURATION_TABLE_NAME: TableName =
    TableName::new("flow_nexus_meta_metaflow_configuration");
pub(super) const META_METAFLOW_CONFIGURATION_FAMILY: &str =
    "flow-nexus-meta-metaflow-configuration";
pub(super) const META_METAFLOW_CONFIGURATION_SCHEMA: &str =
    "flow-nexus-meta-metaflow-configuration-v2";

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

impl MemoryDefinition for StoredMetaMetaflowConfiguration {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(2)
    }

    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label(META_METAFLOW_CONFIGURATION_SCHEMA)
    }
}

/// The archived declaration before kind and tool-set were introduced.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(super) struct ConfigureMetaflowV1 {
    pub(super) address: Address,
    pub(super) system_modules: Vec<String>,
    pub(super) user_modules: Vec<String>,
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(super) struct StoredMetaMetaflowConfigurationV1 {
    pub(super) metaflow: ConfigureMetaflowV1,
}

impl EngineRecord for StoredMetaMetaflowConfigurationV1 {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(FlowStore::address_record_key(&self.metaflow.address))
    }
}

impl MemoryDefinition for StoredMetaMetaflowConfigurationV1 {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(1)
    }

    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label("flow-nexus-meta-metaflow-configuration-v1")
    }
}

impl UpgradeFrom<StoredMetaMetaflowConfigurationV1> for StoredMetaMetaflowConfiguration {
    type Refusal = std::convert::Infallible;

    fn upgrade_from(previous: StoredMetaMetaflowConfigurationV1) -> Decision<Self, Self::Refusal> {
        let previous = previous.metaflow;
        Decision::Ready(Self {
            metaflow: ConfigureMetaflow {
                address: previous.address,
                system_modules: previous.system_modules,
                user_modules: previous.user_modules,
                flow_kind: signal_flow::FlowKind::Regular,
                tool_set: signal_flow::ToolSet::Default,
            },
        })
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
        .with_predecessor::<StoredMetaMetaflowConfigurationV1>()
    }
}

pub(crate) trait UpgradesMetaflowDeclarations {
    fn upgraded_metaflow_declarations(
        &mut self,
        descriptor: TableDescriptor<StoredMetaMetaflowConfiguration>,
    ) -> Result<TableReference<StoredMetaMetaflowConfiguration>, StoreError>;
}

impl UpgradesMetaflowDeclarations for Engine {
    fn upgraded_metaflow_declarations(
        &mut self,
        descriptor: TableDescriptor<StoredMetaMetaflowConfiguration>,
    ) -> Result<TableReference<StoredMetaMetaflowConfiguration>, StoreError> {
        match self
            .upgrade_table::<StoredMetaMetaflowConfigurationV1, StoredMetaMetaflowConfiguration>(
                descriptor,
            ) {
            UpgradeOutcome::Upgraded(receipt) => Ok(receipt.table()),
            UpgradeOutcome::AlreadyCurrent(table) => Ok(table),
            UpgradeOutcome::Refused(refusal) => {
                Err(StoreError::MetaflowUpgrade(refusal.key().to_owned_string()))
            }
            UpgradeOutcome::Failed(failure) => {
                Err(StoreError::MetaflowUpgrade(format!("{failure:?}")))
            }
        }
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

#[cfg(test)]
mod tests {
    use super::{
        ConfigureMetaflowV1, StoredMetaMetaflowConfiguration, StoredMetaMetaflowConfigurationV1,
    };
    use sema_engine::{Decision, UpgradeFrom};
    use signal_flow::{Address, Aspect, FlowKind, Layer, ToolSet};

    #[test]
    fn v1_declaration_upgrade_preserves_context_and_defaults_regular_tools() {
        let previous = StoredMetaMetaflowConfigurationV1 {
            metaflow: ConfigureMetaflowV1 {
                address: Address {
                    aspect: Aspect::Psyche,
                    topic: "book".into(),
                    layer: Layer::Tertiary,
                },
                system_modules: vec!["spirit-core".into()],
                user_modules: vec!["vision-book".into()],
            },
        };
        let Decision::Ready(upgraded) = StoredMetaMetaflowConfiguration::upgrade_from(previous);
        assert_eq!(upgraded.metaflow.system_modules, ["spirit-core"]);
        assert_eq!(upgraded.metaflow.user_modules, ["vision-book"]);
        assert_eq!(upgraded.metaflow.flow_kind, FlowKind::Regular);
        assert_eq!(upgraded.metaflow.tool_set, ToolSet::Default);
    }
}
