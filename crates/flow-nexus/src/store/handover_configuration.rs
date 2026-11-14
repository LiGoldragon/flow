pub(crate) use crate::generated::memory::StoredHandoverConfiguration;
use meta_signal_flow::{
    Duration, HandoverArchiveConfiguration, HandoverCountConfiguration,
    HandoverPoliciesConfiguration, SuccessorRecoveryText,
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
pub(crate) struct StoredHandoverConfigurationV2 {
    pub handover_policies_configuration: HandoverPoliciesConfiguration,
    pub handover_archive_configuration: HandoverArchiveConfiguration,
    pub handover_count_configuration: HandoverCountConfiguration,
    pub duration: Duration,
}
impl EngineRecord for StoredHandoverConfigurationV2 {
    fn record_key(&self) -> RecordKey {
        RecordKey::new("handover")
    }
}
impl MemoryDefinition for StoredHandoverConfigurationV2 {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(2)
    }
    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label("flow-nexus-handover-configuration-v2")
    }
}

pub(crate) trait SeedsSuccessorRecoveryText {
    fn initial() -> Self;
}
impl SeedsSuccessorRecoveryText for SuccessorRecoveryText {
    fn initial() -> Self {
        Self("Your predecessor reached its emergency context threshold and was refreshed at rest without writing a handover. Recover what your open requests need from its transcript before you continue.".into())
    }
}
impl UpgradeFrom<StoredHandoverConfigurationV2> for StoredHandoverConfiguration {
    type Refusal = std::convert::Infallible;
    fn upgrade_from(before: StoredHandoverConfigurationV2) -> Decision<Self, Self::Refusal> {
        Decision::Ready(Self {
            handover_policies_configuration: before.handover_policies_configuration,
            handover_archive_configuration: before.handover_archive_configuration,
            handover_count_configuration: before.handover_count_configuration,
            duration: before.duration,
            successor_recovery_text: SuccessorRecoveryText::initial(),
        })
    }
}

/// v1 takes the publication deadline v2 added, at its seeded 60 s; v2 then
/// takes the recovery text, so a v1 store is upgraded through v2, each
/// format carrying only the step from the one before.
impl UpgradeFrom<StoredHandoverConfigurationV1> for StoredHandoverConfigurationV2 {
    type Refusal = std::convert::Infallible;
    fn upgrade_from(predecessor: StoredHandoverConfigurationV1) -> Decision<Self, Self::Refusal> {
        Decision::Ready(Self {
            handover_policies_configuration: predecessor.policies,
            handover_archive_configuration: predecessor.archive,
            handover_count_configuration: predecessor.count,
            duration: meta_signal_flow::Duration(60_000),
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
        MemoryVersion::new(3)
    }
    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label("flow-nexus-handover-configuration-v3")
    }
}
/// The v2 family, the step a v1 store takes on its way to v3.
pub(crate) trait DescribesHandoverConfigurationV2 {
    fn descriptor() -> TableDescriptor<StoredHandoverConfigurationV2>;
}
impl DescribesHandoverConfigurationV2 for StoredHandoverConfigurationV2 {
    fn descriptor() -> TableDescriptor<StoredHandoverConfigurationV2> {
        TableDescriptor::new(
            HANDOVER_CONFIGURATION_TABLE_NAME,
            FamilyName::new("flow-nexus-handover-configuration"),
            Self::schema_hash(),
        )
        .with_predecessor::<StoredHandoverConfigurationV1>()
    }
}
/// The current family. It names both earlier formats, so an open admits a
/// store at either; the upgrade itself runs v1 to v2 to v3.
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
        .with_predecessor::<StoredHandoverConfigurationV2>()
    }
}
pub(crate) trait DefaultsToUnconfigured {
    fn unconfigured() -> Self;
}
impl DefaultsToUnconfigured for StoredHandoverConfiguration {
    fn unconfigured() -> Self {
        StoredHandoverConfiguration {
            handover_policies_configuration: HandoverPoliciesConfiguration::Unconfigured,
            handover_archive_configuration: HandoverArchiveConfiguration::Unconfigured,
            handover_count_configuration: HandoverCountConfiguration::Unconfigured,
            duration: meta_signal_flow::Duration(60_000),
            successor_recovery_text: SuccessorRecoveryText::initial(),
        }
    }
}

pub(crate) trait ReadsHandoverConfiguration {
    fn successor_recovery_text(&self) -> Result<SuccessorRecoveryText, super::StoreError>;
    fn handover_policies(&self) -> Result<HandoverPoliciesConfiguration, super::StoreError>;
    fn handover_archive(&self) -> Result<HandoverArchiveConfiguration, super::StoreError>;
    fn handover_count(&self) -> Result<HandoverCountConfiguration, super::StoreError>;
    fn publication_deadline(&self) -> Result<Duration, super::StoreError>;
}

impl ReadsHandoverConfiguration for super::FlowStore {
    fn successor_recovery_text(&self) -> Result<SuccessorRecoveryText, super::StoreError> {
        let rows = self.engine.match_records(sema_engine::QueryPlan::key(
            self.meta_configuration_tables.handover,
            RecordKey::new("handover"),
        ))?;
        match rows.records() {
            [row] => Ok(row.successor_recovery_text.clone()),
            _ => Err(super::StoreError::StateInvariant),
        }
    }

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
            .map(|row| row.handover_policies_configuration.clone())
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
            .map(|row| row.handover_archive_configuration.clone())
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
            .map(|row| row.handover_count_configuration.clone())
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
            .map(|row| row.duration)
            .unwrap_or(meta_signal_flow::Duration(60_000)))
    }
}

/// The current handover-configuration table: a v1 store is first upgraded
/// to v2, then a v2 store to v3; a current store is opened and an absent
/// family registered fresh.
pub(crate) trait UpgradesHandoverConfiguration: Sized {
    fn upgrade_or_register(engine: &mut Engine) -> Result<TableReference<Self>, String>;
}

/// One step of the chain, its refusal or failure named.
trait SettlesHandoverConfigurationStep<Table> {
    fn settled(self) -> Result<TableReference<Table>, String>;
}

impl<Table> SettlesHandoverConfigurationStep<Table>
    for UpgradeOutcome<Table, std::convert::Infallible>
{
    fn settled(self) -> Result<TableReference<Table>, String> {
        match self {
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
}

impl UpgradesHandoverConfiguration for StoredHandoverConfiguration {
    fn upgrade_or_register(
        engine: &mut Engine,
    ) -> Result<TableReference<StoredHandoverConfiguration>, String> {
        let stored_v1 = engine.list_tables().into_iter().any(|table| {
            table.identity().table_name() == HANDOVER_CONFIGURATION_TABLE_NAME.as_str()
                && table.identity().schema_hash() == StoredHandoverConfigurationV1::schema_hash()
        });
        if stored_v1 {
            engine
                .upgrade_table::<StoredHandoverConfigurationV1, StoredHandoverConfigurationV2>(
                    <StoredHandoverConfigurationV2 as DescribesHandoverConfigurationV2>::descriptor(),
                )
                .settled()?;
        }
        engine
            .upgrade_table::<StoredHandoverConfigurationV2, StoredHandoverConfiguration>(
                <StoredHandoverConfiguration as DescribesHandoverConfiguration>::descriptor(),
            )
            .settled()
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
            archive: HandoverArchiveConfiguration::Configured(
                meta_signal_flow::generated::signal::Path("/archive".into()),
            ),
            count: HandoverCountConfiguration::Configured(3),
        };
        let Decision::Ready(v2) = StoredHandoverConfigurationV2::upgrade_from(old);
        let Decision::Ready(current) = StoredHandoverConfiguration::upgrade_from(v2);
        assert_eq!(
            current.handover_archive_configuration,
            HandoverArchiveConfiguration::Configured(meta_signal_flow::generated::signal::Path(
                "/archive".into()
            ))
        );
        assert_eq!(
            current.handover_count_configuration,
            HandoverCountConfiguration::Configured(3)
        );
        assert_eq!(current.duration, meta_signal_flow::Duration(60_000));
        assert_eq!(
            current.successor_recovery_text,
            SuccessorRecoveryText::initial()
        );
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
                    model_name: flow_ethos::ModelName("claude-opus-5-5".into()),
                    warning_share_left: meta_signal_flow::WarningShareLeft(15),
                    reminder_text: meta_signal_flow::ReminderText(
                        "Continue every five minutes.".into(),
                    ),
                    every_minutes: meta_signal_flow::EveryMinutes(5),
                },
            ]),
            archive: HandoverArchiveConfiguration::Configured(
                meta_signal_flow::generated::signal::Path("/archive/v1".into()),
            ),
            count: HandoverCountConfiguration::Configured(3),
        };
        let address = Address {
            aspect: Aspect::Mind,
            topic: flow_ethos::Topic("upgradeSibling".into()),
            model: Model::Sol,
        };
        let sibling = HandoverMemory {
            address: address.clone(),
            section_vector: vec![Section {
                title: signal_flow::Title("Live".into()),
                description: signal_flow::Description("current generation".into()),
                body: signal_flow::Body("live bytes".into()),
            }],
            touch_option: Some(Touch::Unknown),
            handover_generation: signal_flow::HandoverGeneration(8),
            flow_id_option: Some(flow_ethos::FlowId("successor".into())),
            closed_handover_vector: vec![ClosedHandover {
                handover_generation: signal_flow::HandoverGeneration(7),
                predecessor: crate::generated::memory::Predecessor("predecessor".into()),
                successor: signal_flow::Successor(flow_ethos::FlowId("successor".into())),
                section_vector: vec![Section {
                    title: signal_flow::Title("Closed".into()),
                    description: signal_flow::Description("archived generation".into()),
                    body: signal_flow::Body("closed bytes".into()),
                }],
                touch_option: Some(Touch::Unknown),
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
                assert_eq!(
                    current.handover_policies_configuration,
                    predecessor.policies
                );
                assert_eq!(current.handover_archive_configuration, predecessor.archive);
                assert_eq!(current.handover_count_configuration, predecessor.count);
                assert_eq!(current.duration, meta_signal_flow::Duration(60_000));
                assert_eq!(
                    current.successor_recovery_text,
                    SuccessorRecoveryText::initial()
                );
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
            let configuration =
                StoredHandoverConfiguration::upgrade_or_register(&mut engine).unwrap();
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
            let configuration =
                StoredHandoverConfiguration::upgrade_or_register(&mut engine).unwrap();
            let handovers = engine.register_table(handover_descriptor.clone()).unwrap();
            assert_current(&engine, configuration, handovers);
        }
    }

    #[test]
    fn persisted_v2_upgrade_preserves_configuration_and_never_reseeds_changed_text() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("handover-v2.sema");
        let old = StoredHandoverConfigurationV2 {
            handover_policies_configuration: HandoverPoliciesConfiguration::Unconfigured,
            handover_archive_configuration: HandoverArchiveConfiguration::Configured(
                meta_signal_flow::generated::signal::Path("/v2/archive".into()),
            ),
            handover_count_configuration: HandoverCountConfiguration::Configured(7),
            duration: Duration(12345),
        };
        {
            let mut engine = Engine::open(EngineOpen::new(&path, SchemaVersion::new(1))).unwrap();
            let descriptor = TableDescriptor::<StoredHandoverConfigurationV2>::new(
                HANDOVER_CONFIGURATION_TABLE_NAME,
                FamilyName::new("flow-nexus-handover-configuration"),
                StoredHandoverConfigurationV2::schema_hash(),
            );
            let table = engine.register_table(descriptor).unwrap();
            engine.assert(Assertion::new(table, old.clone())).unwrap();
        }
        {
            let mut engine = Engine::open(
                EngineOpen::new(&path, SchemaVersion::new(1))
                    .with_family(&StoredHandoverConfiguration::descriptor()),
            )
            .unwrap();
            let table = StoredHandoverConfiguration::upgrade_or_register(&mut engine).unwrap();
            let row = engine
                .match_records(QueryPlan::key(table, RecordKey::new("handover")))
                .unwrap()
                .records()[0]
                .clone();
            assert_eq!(
                row.handover_archive_configuration,
                old.handover_archive_configuration
            );
            assert_eq!(
                row.handover_count_configuration,
                old.handover_count_configuration
            );
            assert_eq!(row.duration, old.duration);
            assert_eq!(
                row.successor_recovery_text,
                SuccessorRecoveryText::initial()
            );
            let mut changed = row;
            changed.successor_recovery_text = SuccessorRecoveryText("After migration.".into());
            engine
                .mutate_keyed(sema_engine::KeyedMutation::new(
                    table,
                    RecordKey::new("handover"),
                    changed,
                ))
                .unwrap();
        }
        {
            let mut engine = Engine::open(
                EngineOpen::new(&path, SchemaVersion::new(1))
                    .with_family(&StoredHandoverConfiguration::descriptor()),
            )
            .unwrap();
            let table = StoredHandoverConfiguration::upgrade_or_register(&mut engine).unwrap();
            let row = engine
                .match_records(QueryPlan::key(table, RecordKey::new("handover")))
                .unwrap()
                .records()[0]
                .clone();
            assert_eq!(row.successor_recovery_text.0, "After migration.");
        }
    }

}
