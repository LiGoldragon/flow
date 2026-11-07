use meta_signal_flow::{
    HandoverArchiveConfiguration, HandoverCountConfiguration, HandoverPoliciesConfiguration,
    PublicationDeadlineConfiguration,
};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use sema_engine::TableName;
use sema_engine::{
    Decision, Engine, EngineRecord, FamilyName, MemoryDefinition, MemoryUpgrades, MemoryVersion,
    RecordKey, SchemaHash, TableDescriptor, TableReference, UpgradeFrom, UpgradeOutcome,
};
const HANDOVER_CONFIGURATION_TABLE_NAME: TableName =
    TableName::new("flow_nexus_meta_handover_configuration");
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct StoredHandoverConfigurationV1 {
    pub policies: HandoverPoliciesConfiguration,
    pub archive: HandoverArchiveConfiguration,
    pub count: HandoverCountConfiguration,
}
impl EngineRecord for StoredHandoverConfigurationV1 {
    fn record_key(&self) -> RecordKey {
        RecordKey::new("handover")
    }
}
impl MemoryDefinition for StoredHandoverConfigurationV1 {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(1)
    }
    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label("flow-nexus-handover-configuration-v1")
    }
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct StoredHandoverConfiguration {
    pub policies: HandoverPoliciesConfiguration,
    pub archive: HandoverArchiveConfiguration,
    pub count: HandoverCountConfiguration,
    pub publication_deadline: PublicationDeadlineConfiguration,
}
impl UpgradeFrom<StoredHandoverConfigurationV1> for StoredHandoverConfiguration {
    type Refusal = std::convert::Infallible;
    fn upgrade_from(predecessor: StoredHandoverConfigurationV1) -> Decision<Self, Self::Refusal> {
        Decision::Ready(Self {
            policies: predecessor.policies,
            archive: predecessor.archive,
            count: predecessor.count,
            publication_deadline: PublicationDeadlineConfiguration::Configured(60_000),
        })
    }
}

impl EngineRecord for StoredHandoverConfiguration {
    fn record_key(&self) -> RecordKey {
        RecordKey::new("handover")
    }
}
impl MemoryDefinition for StoredHandoverConfiguration {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(2)
    }
    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label("flow-nexus-handover-configuration-v2")
    }
}
pub(crate) trait DescribesHandoverConfiguration {
    fn descriptor() -> TableDescriptor<StoredHandoverConfiguration>;
}
impl DescribesHandoverConfiguration for StoredHandoverConfiguration {
    fn descriptor() -> TableDescriptor<StoredHandoverConfiguration> {
        TableDescriptor::new(
            HANDOVER_CONFIGURATION_TABLE_NAME,
            FamilyName::new("flow-nexus-handover-configuration"),
            Self::schema_hash(),
        )
        .with_predecessor::<StoredHandoverConfigurationV1>()
    }
}
pub(crate) fn unconfigured() -> StoredHandoverConfiguration {
    StoredHandoverConfiguration {
        policies: HandoverPoliciesConfiguration::Unconfigured,
        archive: HandoverArchiveConfiguration::Unconfigured,
        count: HandoverCountConfiguration::Unconfigured,
        publication_deadline: PublicationDeadlineConfiguration::Configured(60_000),
    }
}

pub(crate) trait ReadsHandoverConfiguration {
    fn handover_policies(&self) -> Result<HandoverPoliciesConfiguration, super::StoreError>;
    fn handover_archive(&self) -> Result<HandoverArchiveConfiguration, super::StoreError>;
    fn handover_count(&self) -> Result<HandoverCountConfiguration, super::StoreError>;
    fn publication_deadline(&self) -> Result<PublicationDeadlineConfiguration, super::StoreError>;
}

impl ReadsHandoverConfiguration for super::FlowStore {
    fn handover_policies(&self) -> Result<HandoverPoliciesConfiguration, super::StoreError> {
        use sema_engine::{QueryPlan, RecordKey};
        let rows = self
            .engine
            .match_records(QueryPlan::key(
                self.meta_configuration_tables.handover,
                RecordKey::new("handover"),
            ))?
            .records()
            .to_vec();
        Ok(rows
            .first()
            .map(|row| row.policies.clone())
            .unwrap_or(HandoverPoliciesConfiguration::Unconfigured))
    }

    fn handover_archive(&self) -> Result<HandoverArchiveConfiguration, super::StoreError> {
        use sema_engine::{QueryPlan, RecordKey};
        let rows = self
            .engine
            .match_records(QueryPlan::key(
                self.meta_configuration_tables.handover,
                RecordKey::new("handover"),
            ))?
            .records()
            .to_vec();
        Ok(rows
            .first()
            .map(|row| row.archive.clone())
            .unwrap_or(HandoverArchiveConfiguration::Unconfigured))
    }

    fn handover_count(&self) -> Result<HandoverCountConfiguration, super::StoreError> {
        use sema_engine::{QueryPlan, RecordKey};
        let rows = self
            .engine
            .match_records(QueryPlan::key(
                self.meta_configuration_tables.handover,
                RecordKey::new("handover"),
            ))?
            .records()
            .to_vec();
        Ok(rows
            .first()
            .map(|row| row.count.clone())
            .unwrap_or(HandoverCountConfiguration::Unconfigured))
    }
    fn publication_deadline(&self) -> Result<PublicationDeadlineConfiguration, super::StoreError> {
        use sema_engine::{QueryPlan, RecordKey};
        let rows = self
            .engine
            .match_records(QueryPlan::key(
                self.meta_configuration_tables.handover,
                RecordKey::new("handover"),
            ))?
            .records()
            .to_vec();
        Ok(rows
            .first()
            .map(|row| row.publication_deadline.clone())
            .unwrap_or(PublicationDeadlineConfiguration::Configured(60_000)))
    }
}

pub(crate) fn upgrade_or_register(
    engine: &mut Engine,
) -> Result<TableReference<StoredHandoverConfiguration>, String> {
    let descriptor = <StoredHandoverConfiguration as DescribesHandoverConfiguration>::descriptor();
    match engine
        .upgrade_table::<StoredHandoverConfigurationV1, StoredHandoverConfiguration>(descriptor)
    {
        UpgradeOutcome::Upgraded(receipt) => Ok(receipt.table()),
        UpgradeOutcome::AlreadyCurrent(table) => Ok(table),
        UpgradeOutcome::Refused(refusal) => Err(format!(
            "handover configuration upgrade refused: {refusal:?}"
        )),
        UpgradeOutcome::Failed(failure) => Err(format!(
            "handover configuration upgrade failed: {failure:?}"
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn v1_upgrade_preserves_archive_policy_and_count_and_seeds_default_deadline() {
        let old = StoredHandoverConfigurationV1 {
            policies: HandoverPoliciesConfiguration::Configured(vec![]),
            archive: HandoverArchiveConfiguration::Configured("/archive".into()),
            count: HandoverCountConfiguration::Configured(3),
        };
        let Decision::Ready(current) = StoredHandoverConfiguration::upgrade_from(old) else {
            panic!("legacy handover configuration must upgrade");
        };
        assert_eq!(
            current.archive,
            HandoverArchiveConfiguration::Configured("/archive".into())
        );
        assert_eq!(current.count, HandoverCountConfiguration::Configured(3));
        assert_eq!(
            current.publication_deadline,
            PublicationDeadlineConfiguration::Configured(60_000)
        );
    }
}
