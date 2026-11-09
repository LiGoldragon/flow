use meta_signal_flow::{
    Duration, HandoverArchiveConfiguration, HandoverCountConfiguration,
    HandoverPoliciesConfiguration,
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
    pub publication_deadline: Duration,
}
impl UpgradeFrom<StoredHandoverConfigurationV1> for StoredHandoverConfiguration {
    type Refusal = std::convert::Infallible;
    fn upgrade_from(predecessor: StoredHandoverConfigurationV1) -> Decision<Self, Self::Refusal> {
        Decision::Ready(Self {
            policies: predecessor.policies,
            archive: predecessor.archive,
            count: predecessor.count,
            publication_deadline: 60_000,
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
pub(crate) trait DefaultsToUnconfigured {
    fn unconfigured() -> Self;
}
impl DefaultsToUnconfigured for StoredHandoverConfiguration {
    fn unconfigured() -> Self {
        StoredHandoverConfiguration {
            policies: HandoverPoliciesConfiguration::Unconfigured,
            archive: HandoverArchiveConfiguration::Unconfigured,
            count: HandoverCountConfiguration::Unconfigured,
            publication_deadline: 60_000,
        }
    }
}

pub(crate) trait ReadsHandoverConfiguration {
    fn handover_policies(&self) -> Result<HandoverPoliciesConfiguration, super::StoreError>;
    fn handover_archive(&self) -> Result<HandoverArchiveConfiguration, super::StoreError>;
    fn handover_count(&self) -> Result<HandoverCountConfiguration, super::StoreError>;
    fn publication_deadline(&self) -> Result<Duration, super::StoreError>;
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
    fn publication_deadline(&self) -> Result<Duration, super::StoreError> {
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
            .map(|row| row.publication_deadline)
            .unwrap_or(60_000))
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
    use crate::store::handover::{ClosedHandover, DescribesHandoverTable, HandoverMemory};
    use sema_engine::{Assertion, EngineOpen, QueryPlan, SchemaVersion};
    use signal_flow::{Address, Aspect, Model, Section, Touch};

    #[test]
    fn v1_upgrade_preserves_archive_policy_and_count_and_seeds_default_deadline() {
        let old = StoredHandoverConfigurationV1 {
            policies: HandoverPoliciesConfiguration::Configured(vec![]),
            archive: HandoverArchiveConfiguration::Configured("/archive".into()),
            count: HandoverCountConfiguration::Configured(3),
        };
        let Decision::Ready(current) = StoredHandoverConfiguration::upgrade_from(old);
        assert_eq!(
            current.archive,
            HandoverArchiveConfiguration::Configured("/archive".into())
        );
        assert_eq!(current.count, HandoverCountConfiguration::Configured(3));
        assert_eq!(current.publication_deadline, 60_000);
    }

    #[test]
    fn persisted_v1_upgrades_once_and_preserves_configuration_and_handover_rows() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("handover-v1.sema");
        let predecessor_descriptor = TableDescriptor::<StoredHandoverConfigurationV1>::new(
            HANDOVER_CONFIGURATION_TABLE_NAME,
            FamilyName::new("flow-nexus-handover-configuration"),
            StoredHandoverConfigurationV1::schema_hash(),
        );
        let handover_descriptor = HandoverMemory::descriptor();
        let predecessor = StoredHandoverConfigurationV1 {
            policies: HandoverPoliciesConfiguration::Configured(vec![
                meta_signal_flow::HandoverPolicy {
                    model_name: "claude-opus-5-5".into(),
                    warning_share_left: 15,
                    reminder_text: "Continue every five minutes.".into(),
                    every_minutes: 5,
                },
            ]),
            archive: HandoverArchiveConfiguration::Configured("/archive/v1".into()),
            count: HandoverCountConfiguration::Configured(3),
        };
        let address = Address {
            aspect: Aspect::Mind,
            topic: "upgradeSibling".into(),
            model: Model::Sol,
        };
        let sibling = HandoverMemory {
            address: address.clone(),
            current: vec![Section {
                title: "Live".into(),
                description: "current generation".into(),
                body: "live bytes".into(),
            }],
            touch: Some(Touch::Unknown),
            generation: 8,
            current_flow_id: Some("successor".into()),
            closed: vec![ClosedHandover {
                generation: 7,
                predecessor: "predecessor".into(),
                successor: "successor".into(),
                sections: vec![Section {
                    title: "Closed".into(),
                    description: "archived generation".into(),
                    body: "closed bytes".into(),
                }],
                touch: Some(Touch::Unknown),
            }],
        };
        {
            let mut engine = Engine::open(EngineOpen::new(&path, SchemaVersion::new(1))).unwrap();
            let configuration = engine.register_table(predecessor_descriptor).unwrap();
            let handovers = engine.register_table(handover_descriptor.clone()).unwrap();
            engine
                .assert(Assertion::new(configuration, predecessor.clone()))
                .unwrap();
            engine
                .assert(Assertion::new(handovers, sibling.clone()))
                .unwrap();
        }

        let assert_current =
            |engine: &Engine,
             configuration: TableReference<StoredHandoverConfiguration>,
             handovers: TableReference<HandoverMemory>| {
                let current = engine
                    .match_records(QueryPlan::key(configuration, RecordKey::new("handover")))
                    .unwrap()
                    .records()[0]
                    .clone();
                assert_eq!(current.policies, predecessor.policies);
                assert_eq!(current.archive, predecessor.archive);
                assert_eq!(current.count, predecessor.count);
                assert_eq!(current.publication_deadline, 60_000);
                assert_eq!(
                    engine
                        .match_records(QueryPlan::key(handovers, sibling.record_key()))
                        .unwrap()
                        .records(),
                    std::slice::from_ref(&sibling)
                );
            };

        let current_descriptor = StoredHandoverConfiguration::descriptor();
        {
            let mut engine = Engine::open(
                EngineOpen::new(&path, SchemaVersion::new(1))
                    .with_family(&current_descriptor)
                    .with_family(&handover_descriptor),
            )
            .unwrap();
            let configuration = upgrade_or_register(&mut engine).unwrap();
            let handovers = engine.register_table(handover_descriptor.clone()).unwrap();
            assert_current(&engine, configuration, handovers);
        }
        {
            let mut engine = Engine::open(
                EngineOpen::new(&path, SchemaVersion::new(1))
                    .with_family(&current_descriptor)
                    .with_family(&handover_descriptor),
            )
            .unwrap();
            let configuration = upgrade_or_register(&mut engine).unwrap();
            let handovers = engine.register_table(handover_descriptor.clone()).unwrap();
            assert_current(&engine, configuration, handovers);
        }
    }
}
