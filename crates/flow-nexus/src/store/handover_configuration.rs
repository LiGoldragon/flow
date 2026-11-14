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
            current.successor_recovery_text.0,
            crate::tests::handover_configuration::APPROVED_RECOVERY_TEXT
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
                    current.successor_recovery_text.0,
                    crate::tests::handover_configuration::APPROVED_RECOVERY_TEXT
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
        // Gap 3: a nonempty policy vector, each policy distinct in every
        // field, is carried whole.
        let old = StoredHandoverConfigurationV2 {
            handover_policies_configuration: HandoverPoliciesConfiguration::Configured(vec![
                meta_signal_flow::HandoverPolicy {
                    model_name: flow_ethos::ModelName("v2ModelOne".into()),
                    warning_share_left: meta_signal_flow::WarningShareLeft(19),
                    reminder_text: meta_signal_flow::ReminderText("V2 reminder one.".into()),
                    every_minutes: meta_signal_flow::EveryMinutes(4),
                },
                meta_signal_flow::HandoverPolicy {
                    model_name: flow_ethos::ModelName("v2ModelTwo".into()),
                    warning_share_left: meta_signal_flow::WarningShareLeft(31),
                    reminder_text: meta_signal_flow::ReminderText("V2 reminder two.".into()),
                    every_minutes: meta_signal_flow::EveryMinutes(9),
                },
            ]),
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
                row.handover_policies_configuration,
                old.handover_policies_configuration
            );
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
                row.successor_recovery_text.0,
                crate::tests::handover_configuration::APPROVED_RECOVERY_TEXT
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
            assert_eq!(
                row,
                StoredHandoverConfiguration {
                    handover_policies_configuration: old.handover_policies_configuration.clone(),
                    handover_archive_configuration: old.handover_archive_configuration.clone(),
                    handover_count_configuration: old.handover_count_configuration.clone(),
                    duration: old.duration,
                    successor_recovery_text: SuccessorRecoveryText("After migration.".into()),
                }
            );
        }
    }

    /// The six v1 stores the published producers wrote, byte for byte:
    /// 4b813404 on meta-signal-flow 66 and b0331a84 on meta-signal-flow 68.
    /// Their provenance (producer source, dependency closure, archive
    /// features, digests) is `tests/unit/historical-v1-handover/`.
    const HISTORICAL_V1_STORES: [(&str, &str, &[u8]); 6] = [
        (
            "4b813404",
            "unconfigured",
            include_bytes!("../../tests/unit/historical-v1-handover/4b813404/stores/unconfigured.sema"),
        ),
        (
            "4b813404",
            "empty-policy",
            include_bytes!("../../tests/unit/historical-v1-handover/4b813404/stores/empty-policy.sema"),
        ),
        (
            "4b813404",
            "nonempty-policy",
            include_bytes!(
                "../../tests/unit/historical-v1-handover/4b813404/stores/nonempty-policy.sema"
            ),
        ),
        (
            "b0331a84",
            "unconfigured",
            include_bytes!("../../tests/unit/historical-v1-handover/b0331a84/stores/unconfigured.sema"),
        ),
        (
            "b0331a84",
            "empty-policy",
            include_bytes!("../../tests/unit/historical-v1-handover/b0331a84/stores/empty-policy.sema"),
        ),
        (
            "b0331a84",
            "nonempty-policy",
            include_bytes!(
                "../../tests/unit/historical-v1-handover/b0331a84/stores/nonempty-policy.sema"
            ),
        ),
    ];

    /// What each historical store holds, as its producer's manifest writes
    /// it, in the current types; the deadline v2 seeds and the approved
    /// recovery text v3 seeds follow.
    fn historical_upgraded(store: &str) -> StoredHandoverConfiguration {
        let (policies, archive, count) = match store {
            "unconfigured" => (
                HandoverPoliciesConfiguration::Unconfigured,
                HandoverArchiveConfiguration::Unconfigured,
                HandoverCountConfiguration::Unconfigured,
            ),
            "empty-policy" => (
                HandoverPoliciesConfiguration::Configured(Vec::new()),
                HandoverArchiveConfiguration::Configured(
                    meta_signal_flow::generated::signal::Path(
                        "/historical/empty policy/archive".into(),
                    ),
                ),
                HandoverCountConfiguration::Configured(7),
            ),
            "nonempty-policy" => (
                HandoverPoliciesConfiguration::Configured(vec![
                    meta_signal_flow::HandoverPolicy {
                        model_name: flow_ethos::ModelName("historicalModelOne".into()),
                        warning_share_left: meta_signal_flow::WarningShareLeft(17),
                        reminder_text: meta_signal_flow::ReminderText(
                            "Historical reminder one: preserve nested text.".into(),
                        ),
                        every_minutes: meta_signal_flow::EveryMinutes(3),
                    },
                    meta_signal_flow::HandoverPolicy {
                        model_name: flow_ethos::ModelName("historicalModelTwo".into()),
                        warning_share_left: meta_signal_flow::WarningShareLeft(29),
                        reminder_text: meta_signal_flow::ReminderText(
                            "Historical reminder two: distinct bytes.".into(),
                        ),
                        every_minutes: meta_signal_flow::EveryMinutes(11),
                    },
                ]),
                HandoverArchiveConfiguration::Configured(
                    meta_signal_flow::generated::signal::Path(
                        "/historical/nonempty policy/archive".into(),
                    ),
                ),
                HandoverCountConfiguration::Configured(13),
            ),
            other => panic!("no historical store {other}"),
        };
        StoredHandoverConfiguration {
            handover_policies_configuration: policies,
            handover_archive_configuration: archive,
            handover_count_configuration: count,
            duration: Duration(60_000),
            successor_recovery_text: SuccessorRecoveryText(
                crate::tests::handover_configuration::APPROVED_RECOVERY_TEXT.into(),
            ),
        }
    }

    /// Gap 4, the whole file. Each producer-written store opens in this
    /// engine, and the family's own upgrader takes it through v2 to v3:
    /// every v1 field kept, the deadline and the approved text seeded once,
    /// a text changed after the upgrade kept across reopens.
    #[test]
    fn producer_written_v1_stores_upgrade_whole_through_v2_to_v3_and_never_reseed() {
        for (producer, store, bytes) in HISTORICAL_V1_STORES {
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join(format!("{store}.sema"));
            std::fs::write(&path, bytes).unwrap();
            let expected = historical_upgraded(store);
            let changed = StoredHandoverConfiguration {
                successor_recovery_text: SuccessorRecoveryText("Changed after upgrade.".into()),
                ..expected.clone()
            };
            let open = || {
                Engine::open(
                    EngineOpen::new(&path, SchemaVersion::new(1))
                        .with_family(&StoredHandoverConfiguration::descriptor()),
                )
                .unwrap_or_else(|error| panic!("{producer} {store} opens: {error:?}"))
            };
            {
                let mut engine = open();
                let table = StoredHandoverConfiguration::upgrade_or_register(&mut engine)
                    .unwrap_or_else(|error| panic!("{producer} {store} upgrades: {error}"));
                assert_eq!(
                    engine
                        .match_records(QueryPlan::all(table))
                        .unwrap()
                        .records(),
                    std::slice::from_ref(&expected),
                    "{producer} {store}"
                );
                engine
                    .mutate_keyed(sema_engine::KeyedMutation::new(
                        table,
                        RecordKey::new("handover"),
                        changed.clone(),
                    ))
                    .unwrap();
            }
            for _ in 0..2 {
                let mut engine = open();
                let table = StoredHandoverConfiguration::upgrade_or_register(&mut engine).unwrap();
                assert_eq!(
                    engine
                        .match_records(QueryPlan::all(table))
                        .unwrap()
                        .records(),
                    std::slice::from_ref(&changed),
                    "{producer} {store} reopened"
                );
            }
        }
    }

    /// Gap 4, through Flow's own openers. The producer's catalog entry and
    /// row, exact bytes, stand in a configured Flow store for its handover
    /// family; the configuration opener upgrades them, Configure changes
    /// the recovery text, and the Flow opener reads the change back after
    /// each reopen, every v1 field kept.
    #[test]
    fn producer_written_v1_rows_upgrade_through_flow_openers_and_keep_a_configured_text() {
        use crate::store::{
            ConfigurationOnlyStore, ConfiguresOnlyStore, FlowStore, ManagesMetaflowState,
            OpensFlowStore,
        };
        let rows = redb::TableDefinition::<String, &[u8]>::new(super::super::OPTIONAL_HANDOVER_TABLE);
        for (producer, store, bytes) in HISTORICAL_V1_STORES {
            let directory = tempfile::tempdir().unwrap();
            let produced = directory.path().join("produced.sema");
            std::fs::write(&produced, bytes).unwrap();
            let (registration, row) = {
                use redb::ReadableDatabase as _;
                let database = redb::Database::open(&produced).unwrap();
                let read = database.begin_read().unwrap();
                let catalog = read.open_table(super::super::SEMA_CATALOG_TABLE).unwrap();
                let registration = catalog
                    .get(super::super::OPTIONAL_HANDOVER_TABLE)
                    .unwrap()
                    .expect("the producer registered the family")
                    .value()
                    .to_vec();
                let row = read
                    .open_table(rows)
                    .unwrap()
                    .get("handover".to_owned())
                    .unwrap()
                    .expect("the producer wrote its row")
                    .value()
                    .to_vec();
                (registration, row)
            };
            let fixture = crate::tests::configuring::ConfiguredStore::new();
            {
                let database = redb::Database::open(fixture.path()).unwrap();
                let write = database.begin_write().unwrap();
                write
                    .open_table(super::super::SEMA_CATALOG_TABLE)
                    .unwrap()
                    .insert(super::super::OPTIONAL_HANDOVER_TABLE, registration.as_slice())
                    .unwrap();
                write
                    .open_table(rows)
                    .unwrap()
                    .insert("handover".to_owned(), row.as_slice())
                    .unwrap();
                write.commit().unwrap();
            }
            let expected = historical_upgraded(store);
            let configuration = ConfigurationOnlyStore::open(fixture.path())
                .unwrap_or_else(|error| panic!("{producer} {store} upgrades: {error}"));
            let families = configuration.configuration_snapshot().unwrap().unwrap();
            assert_eq!(
                (
                    families.handover_policies,
                    families.handover_archive,
                    families.handover_count,
                    families.publication_deadline,
                    families.successor_recovery_text,
                ),
                (
                    expected.handover_policies_configuration.clone(),
                    expected.handover_archive_configuration.clone(),
                    expected.handover_count_configuration.clone(),
                    expected.duration,
                    expected.successor_recovery_text.clone(),
                ),
                "{producer} {store}"
            );
            configuration
                .configure_change(meta_signal_flow::ConfigurationChange::SuccessorRecoveryText(
                    SuccessorRecoveryText("Configured after the historical upgrade.".into()),
                ))
                .unwrap();
            drop(configuration);
            for _ in 0..2 {
                let running = FlowStore::open_configured(fixture.path())
                    .unwrap_or_else(|error| panic!("{producer} {store} reopens: {error}"));
                let snapshot = running.configuration_snapshot().unwrap().unwrap();
                assert_eq!(
                    snapshot.successor_recovery_text.0,
                    "Configured after the historical upgrade.",
                    "{producer} {store}"
                );
                assert_eq!(
                    snapshot.configured_handover_policies.0,
                    expected.handover_policies_configuration
                );
                assert_eq!(
                    snapshot.configured_handover_archive.0,
                    expected.handover_archive_configuration
                );
                assert_eq!(
                    snapshot.configured_handover_count.0,
                    expected.handover_count_configuration
                );
                assert_eq!(
                    snapshot.configured_publication_deadline.0,
                    expected.duration
                );
            }
        }
    }
}
