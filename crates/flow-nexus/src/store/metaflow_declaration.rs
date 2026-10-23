//! The metaflows declared in Flow's configuration: the meta Metaflows
//! family, one row per Address, each naming the context modules an ordinary
//! Launch of that Address composes into the system prompt and into the first
//! user message. A store written before metaflows were declared opens with
//! the table added and holds none. A declaration archived before it named
//! its continuity, purpose and records opens Maintained when it declares
//! { Psyche core Primary } and OnDemand otherwise, with no purpose and no
//! records.

use super::{FlowStore, ReadsFlowMemory, StoreError};
use meta_signal_flow::{ConfigureMetaflow, Continuity};
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
    "flow-nexus-meta-metaflow-configuration-v3";

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
        MemoryVersion::new(3)
    }

    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label(META_METAFLOW_CONFIGURATION_SCHEMA)
    }
}

/// How a declaration archived before Continuity continues: the Address the
/// living is never left without is Maintained, every other OnDemand.
pub(crate) trait DefaultsContinuity {
    fn default_continuity(&self) -> Continuity;
}

impl DefaultsContinuity for Address {
    fn default_continuity(&self) -> Continuity {
        let maintained = self.aspect == signal_flow::Aspect::Psyche
            && self.topic == "core"
            && self.layer == signal_flow::Layer::Primary;
        if maintained {
            Continuity::Maintained
        } else {
            Continuity::OnDemand
        }
    }
}

/// The archived declaration before continuity, purpose and records.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(super) struct ConfigureMetaflowV2 {
    pub(super) address: Address,
    pub(super) system_modules: Vec<String>,
    pub(super) user_modules: Vec<String>,
    pub(super) flow_kind: signal_flow::FlowKind,
    pub(super) tool_set: signal_flow::ToolSet,
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(super) struct StoredMetaMetaflowConfigurationV2 {
    pub(super) metaflow: ConfigureMetaflowV2,
}

impl EngineRecord for StoredMetaMetaflowConfigurationV2 {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(FlowStore::address_record_key(&self.metaflow.address))
    }
}

impl MemoryDefinition for StoredMetaMetaflowConfigurationV2 {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(2)
    }

    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label("flow-nexus-meta-metaflow-configuration-v2")
    }
}

impl UpgradeFrom<StoredMetaMetaflowConfigurationV2> for StoredMetaMetaflowConfiguration {
    type Refusal = std::convert::Infallible;

    fn upgrade_from(previous: StoredMetaMetaflowConfigurationV2) -> Decision<Self, Self::Refusal> {
        let previous = previous.metaflow;
        Decision::Ready(Self {
            metaflow: ConfigureMetaflow {
                continuity: previous.address.default_continuity(),
                address: previous.address,
                system_modules: previous.system_modules,
                user_modules: previous.user_modules,
                flow_kind: previous.flow_kind,
                tool_set: previous.tool_set,
                purpose: String::new(),
                records: Vec::new(),
            },
        })
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
                continuity: previous.address.default_continuity(),
                address: previous.address,
                system_modules: previous.system_modules,
                user_modules: previous.user_modules,
                flow_kind: signal_flow::FlowKind::Regular,
                tool_set: signal_flow::ToolSet::Default,
                purpose: String::new(),
                records: Vec::new(),
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
        .with_predecessor::<StoredMetaMetaflowConfigurationV2>()
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
        // A v2 family upgrades from v2; a v2 upgrade refused at admission
        // names a v1 family, which upgrades from v1.
        let from_v1 = descriptor.clone();
        match self
            .upgrade_table::<StoredMetaMetaflowConfigurationV2, StoredMetaMetaflowConfiguration>(
                descriptor,
            ) {
            UpgradeOutcome::Upgraded(receipt) => return Ok(receipt.table()),
            UpgradeOutcome::AlreadyCurrent(table) => return Ok(table),
            UpgradeOutcome::Refused(refusal) => match *refusal.reason() {},
            UpgradeOutcome::Failed(failure)
                if failure.phase() == sema_engine::StoragePhase::Admission => {}
            UpgradeOutcome::Failed(failure) => {
                return Err(StoreError::MetaflowUpgrade(format!("{failure:?}")));
            }
        }
        match self
            .upgrade_table::<StoredMetaMetaflowConfigurationV1, StoredMetaMetaflowConfiguration>(
                from_v1,
            ) {
            UpgradeOutcome::Upgraded(receipt) => Ok(receipt.table()),
            UpgradeOutcome::AlreadyCurrent(table) => Ok(table),
            UpgradeOutcome::Refused(refusal) => match *refusal.reason() {},
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
        ConfigureMetaflowV1, ConfigureMetaflowV2, StoredMetaMetaflowConfiguration,
        StoredMetaMetaflowConfigurationV1, StoredMetaMetaflowConfigurationV2,
    };
    use meta_signal_flow::Continuity;
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
        assert_eq!(upgraded.metaflow.continuity, Continuity::OnDemand);
    }

    /// A declaration archived before continuity opens Maintained for
    /// { Psyche core Primary } and OnDemand for every other Address.
    #[test]
    fn v2_declaration_upgrade_maintains_only_the_psyche_core_primary() {
        for (layer, continuity) in [
            (Layer::Primary, Continuity::Maintained),
            (Layer::Secondary, Continuity::OnDemand),
        ] {
            let previous = StoredMetaMetaflowConfigurationV2 {
                metaflow: ConfigureMetaflowV2 {
                    address: Address {
                        aspect: Aspect::Psyche,
                        topic: "core".into(),
                        layer,
                    },
                    system_modules: vec!["spirit:core".into()],
                    user_modules: vec![],
                    flow_kind: FlowKind::Simplified,
                    tool_set: ToolSet::Default,
                },
            };
            let Decision::Ready(upgraded) = StoredMetaMetaflowConfiguration::upgrade_from(previous);
            assert_eq!(upgraded.metaflow.continuity, continuity);
            assert_eq!(upgraded.metaflow.flow_kind, FlowKind::Simplified);
            assert!(upgraded.metaflow.purpose.is_empty());
            assert!(upgraded.metaflow.records.is_empty());
        }
    }
}
