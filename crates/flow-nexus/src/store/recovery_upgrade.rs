//! The step this release takes on open, after the 0.55.0 leave-out step:
//! the launch-outcome family from v10 to v11 and the successor-intent
//! family from v3 to v4. Each family's step reads the format of the one
//! before it and only that one, so a store of any earlier release reaches
//! the current formats through the ordered chain of steps:
//!
//! - launch outcomes: v9 (0.54.6) to v10 in the reap step, then v10 to v11
//!   here;
//! - successor intents: v3 to v4 here.
//!
//! v11 is the launch outcome on signal-flow 64, whose `CompositionRefusal`
//! appends `RecoveryUnavailable` and `RecoveryPredecessorMismatch`; the
//! rejection it holds archives wider (a `StartRejection` in 46 bytes where
//! v10 held one in 30). v10 holds the rejection v9 did, `StartRejectionV9`,
//! and each row is read through it and written in the current layout;
//! every other outcome is its own. v4 is the successor intent whose cause
//! may be `EmergencyRefreshWithoutHandover`; every v3 cause keeps its
//! ordinary meaning (`successor_upgrade`).
//!
//! Each step is its own write and a no-op once its family is current, so
//! an open interrupted between them resumes where it stopped. An older
//! daemon declares v10 or v3 alone and is refused at open by the new
//! identity; rollback is the restore of the store copy taken before the
//! deploy.

use super::reap_upgrade::{StartRejectionV9, StoredLaunchOutcomeV9};
use super::successor::SuccessorIntent;
use super::successor_upgrade::{SuccessorIntentV3, UpgradesSuccessorIntent};
use super::{
    FLOW_LAUNCH_OUTCOME_TABLE_NAME, FlowStore, LaunchOutcome, StoreError, StoredLaunchOutcome,
};
use crate::generated::memory::SupersededHandover;
use crate::generated::starter::{LaunchedRecord, ReplacedRecord};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use sema_engine::{
    Decision, Engine, EngineRecord, FamilyName, MemoryDefinition, MemoryUpgrades, MemoryVersion,
    RecordKey, SchemaHash, TableDescriptor, UpgradeFrom, UpgradeOutcome,
};
use signal_flow::{LaunchRequestId, ReplaceRejection};

/// The launch-outcome family's schema label from 0.54.7 to 0.56.x.
pub(crate) const LAUNCH_OUTCOME_SCHEMA_V10: &str = "flow-nexus-launch-outcome-v10";

/// `ReplaceRejection` as v10 stored it: five variants, the refused launch
/// holding the rejection of signal-flow 51 to 63.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) enum ReplaceRejectionV10 {
    PredecessorAbsent,
    UnknownPredecessor,
    PredecessorStopped,
    LaunchRefused(StartRejectionV9),
    Locked,
}

/// `LaunchOutcome` as v10 stored it: seven variants, each rejection in the
/// layout of signal-flow 51 to 63.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) enum LaunchOutcomeV10 {
    Started(LaunchedRecord),
    Replaced(ReplacedRecord),
    StartRejected(StartRejectionV9),
    ReplaceRejected(ReplaceRejectionV10),
    ReapPending(ReplacedRecord),
    HandoverPending(LaunchedRecord),
    HandoverSuperseded(SupersededHandover),
}

/// One stored launch outcome of v10.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct StoredLaunchOutcomeV10 {
    pub(crate) launch_request_id: LaunchRequestId,
    pub(crate) launch_outcome: LaunchOutcomeV10,
}

impl EngineRecord for StoredLaunchOutcomeV10 {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.launch_request_id.0.clone())
    }
}

impl MemoryDefinition for StoredLaunchOutcomeV10 {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(10)
    }

    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label(LAUNCH_OUTCOME_SCHEMA_V10)
    }
}

impl From<ReplaceRejectionV10> for ReplaceRejection {
    fn from(rejection: ReplaceRejectionV10) -> Self {
        match rejection {
            ReplaceRejectionV10::PredecessorAbsent => ReplaceRejection::PredecessorAbsent,
            ReplaceRejectionV10::UnknownPredecessor => ReplaceRejection::UnknownPredecessor,
            ReplaceRejectionV10::PredecessorStopped => ReplaceRejection::PredecessorStopped,
            ReplaceRejectionV10::LaunchRefused(start) => {
                ReplaceRejection::LaunchRefused(start.into())
            }
            ReplaceRejectionV10::Locked => ReplaceRejection::Locked,
        }
    }
}

impl From<LaunchOutcomeV10> for LaunchOutcome {
    fn from(outcome: LaunchOutcomeV10) -> Self {
        match outcome {
            LaunchOutcomeV10::Started(record) => LaunchOutcome::Started(record),
            LaunchOutcomeV10::Replaced(record) => LaunchOutcome::Replaced(record),
            LaunchOutcomeV10::StartRejected(rejection) => {
                LaunchOutcome::StartRejected(rejection.into())
            }
            LaunchOutcomeV10::ReplaceRejected(rejection) => {
                LaunchOutcome::ReplaceRejected(rejection.into())
            }
            LaunchOutcomeV10::ReapPending(replaced) => LaunchOutcome::ReapPending(replaced),
            LaunchOutcomeV10::HandoverPending(launched) => LaunchOutcome::HandoverPending(launched),
            LaunchOutcomeV10::HandoverSuperseded(superseded) => {
                LaunchOutcome::HandoverSuperseded(superseded)
            }
        }
    }
}

/// A v10 row is its own outcome in the current layout; nothing refuses.
impl UpgradeFrom<StoredLaunchOutcomeV10> for StoredLaunchOutcome {
    type Refusal = std::convert::Infallible;

    fn upgrade_from(before: StoredLaunchOutcomeV10) -> Decision<Self, Self::Refusal> {
        Decision::Ready(Self {
            launch_request_id: before.launch_request_id,
            launch_outcome: before.launch_outcome.into(),
        })
    }
}

/// The launch-outcome family's descriptors: v10, the format the reap step
/// writes, and the current one.
pub(crate) trait DescribesLaunchOutcomeTables {
    /// v10, evolving from v9.
    fn launch_outcome_v10_descriptor() -> TableDescriptor<StoredLaunchOutcomeV10>;
    /// The current family, evolving from v10. It names v9 as well, so an
    /// open admits a store at either; the conversions run v9 to v10 to
    /// v11.
    fn launch_outcome_descriptor() -> TableDescriptor<StoredLaunchOutcome>;
}

impl DescribesLaunchOutcomeTables for FlowStore {
    fn launch_outcome_v10_descriptor() -> TableDescriptor<StoredLaunchOutcomeV10> {
        TableDescriptor::<StoredLaunchOutcomeV10>::new(
            FLOW_LAUNCH_OUTCOME_TABLE_NAME,
            FamilyName::new("flow-nexus-launch-outcome"),
            StoredLaunchOutcomeV10::schema_hash(),
        )
        .with_predecessor::<StoredLaunchOutcomeV9>()
    }

    fn launch_outcome_descriptor() -> TableDescriptor<StoredLaunchOutcome> {
        TableDescriptor::<StoredLaunchOutcome>::new(
            FLOW_LAUNCH_OUTCOME_TABLE_NAME,
            FamilyName::new("flow-nexus-launch-outcome"),
            StoredLaunchOutcome::schema_hash(),
        )
        .with_predecessor::<StoredLaunchOutcomeV9>()
        .with_predecessor::<StoredLaunchOutcomeV10>()
    }
}

/// The startup step, run after the leave-out step and before any family
/// is registered.
pub(crate) trait UpgradesRecoveryMemory {
    /// Upgrades a v10 launch-outcome family and a v3 successor-intent
    /// family; a family already current, or absent, is left as it is.
    fn upgrade_recovery_memory(engine: &mut Engine) -> Result<(), StoreError>;
}

impl UpgradesRecoveryMemory for FlowStore {
    fn upgrade_recovery_memory(engine: &mut Engine) -> Result<(), StoreError> {
        let stored = |table: &str, schema_hash: SchemaHash| {
            engine.list_tables().into_iter().any(|registration| {
                registration.identity().table_name() == table
                    && registration.identity().schema_hash() == schema_hash
            })
        };
        let launch_outcomes_at_v10 = stored(
            FLOW_LAUNCH_OUTCOME_TABLE_NAME.as_str(),
            StoredLaunchOutcomeV10::schema_hash(),
        );
        let successor_intents_at_v3 = stored(
            super::successor::SUCCESSOR_INTENT_TABLE_NAME.as_str(),
            SuccessorIntentV3::schema_hash(),
        );
        if launch_outcomes_at_v10 {
            match engine.upgrade_table::<StoredLaunchOutcomeV10, StoredLaunchOutcome>(
                Self::launch_outcome_descriptor(),
            ) {
                UpgradeOutcome::Upgraded(_) | UpgradeOutcome::AlreadyCurrent(_) => {}
                UpgradeOutcome::Refused(refusal) => {
                    return Err(StoreError::RecoveryUpgrade(format!("{refusal:?}")));
                }
                UpgradeOutcome::Failed(failure) => {
                    return Err(StoreError::RecoveryUpgrade(format!("{failure:?}")));
                }
            }
        }
        if successor_intents_at_v3 {
            <SuccessorIntent as UpgradesSuccessorIntent>::upgrade_or_register(engine)
                .map_err(StoreError::SuccessorIntentUpgrade)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::reap_upgrade::CompositionRefusalV9;
    use sema_engine::{Assertion, EngineOpen, QueryPlan, SchemaVersion};

    fn launched(flow: &str) -> LaunchedRecord {
        LaunchedRecord {
            flow_id: signal_flow::FlowId(flow.into()),
            session_id: signal_flow::SessionId(format!("session-{flow}")),
            origin: signal_flow::Origin::Unbound,
        }
    }

    /// Every outcome a v10 family can hold, each rejection in the layout of
    /// signal-flow 51 to 63.
    fn v10_rows() -> Vec<StoredLaunchOutcomeV10> {
        let replaced = ReplacedRecord {
            flow_id: signal_flow::FlowId("0000aa".into()),
            launched_record: launched("0000bb"),
        };
        [
            LaunchOutcomeV10::Started(launched("000001")),
            LaunchOutcomeV10::Replaced(replaced.clone()),
            LaunchOutcomeV10::StartRejected(StartRejectionV9::CompositionRefused(
                CompositionRefusalV9::HandoverUnreadable(signal_flow::HandoverUnreadable_Data {
                    flow_id: signal_flow::FlowId("0000cc".into()),
                    handover_fault: signal_flow::HandoverFault::RootUnresolved,
                }),
            )),
            LaunchOutcomeV10::StartRejected(StartRejectionV9::TopicRefused(signal_flow::Topic(
                "badTopic".into(),
            ))),
            LaunchOutcomeV10::ReplaceRejected(ReplaceRejectionV10::LaunchRefused(
                StartRejectionV9::CompositionRefused(CompositionRefusalV9::Unrecorded),
            )),
            LaunchOutcomeV10::ReplaceRejected(ReplaceRejectionV10::Locked),
            LaunchOutcomeV10::ReapPending(replaced),
            LaunchOutcomeV10::HandoverPending(launched("000002")),
            LaunchOutcomeV10::HandoverSuperseded(SupersededHandover {
                address: signal_flow::Address {
                    aspect: signal_flow::Aspect::Mind,
                    topic: signal_flow::Topic("superseded".into()),
                    model: signal_flow::Model::Sol,
                },
                handover_owner: signal_flow::HandoverOwner(signal_flow::FlowId("0000dd".into())),
                launched_record: launched("000003"),
                successor: signal_flow::Successor(signal_flow::FlowId("0000ee".into())),
            }),
        ]
        .into_iter()
        .enumerate()
        .map(|(index, launch_outcome)| StoredLaunchOutcomeV10 {
            launch_request_id: LaunchRequestId(format!("launch-{index}")),
            launch_outcome,
        })
        .collect()
    }

    /// The chain's v10 to v11 step: every v10 row is read through the
    /// frozen v10 layout and written in the current one, each outcome its
    /// own and each rejection the same rejection; a second open changes
    /// nothing. The v10 rejection layout reads what signal-flow 60
    /// archived, whose `StartRejection` is 63's variant for variant.
    #[test]
    fn a_v10_launch_outcome_family_upgrades_to_v11_with_every_outcome_its_own() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("launch-outcomes-v10.sema");
        let rows = v10_rows();
        {
            let mut engine = Engine::open(EngineOpen::new(&path, SchemaVersion::new(1))).unwrap();
            let table = engine
                .register_table(TableDescriptor::<StoredLaunchOutcomeV10>::new(
                    FLOW_LAUNCH_OUTCOME_TABLE_NAME,
                    FamilyName::new("flow-nexus-launch-outcome"),
                    StoredLaunchOutcomeV10::schema_hash(),
                ))
                .unwrap();
            for row in rows.clone() {
                engine.assert(Assertion::new(table, row)).unwrap();
            }
        }
        let mut expected: Vec<StoredLaunchOutcome> = rows
            .into_iter()
            .map(|row| StoredLaunchOutcome {
                launch_request_id: row.launch_request_id,
                launch_outcome: row.launch_outcome.into(),
            })
            .collect();
        expected.sort_by(|left, right| left.launch_request_id.0.cmp(&right.launch_request_id.0));
        assert!(expected.iter().any(|row| matches!(
            &row.launch_outcome,
            LaunchOutcome::StartRejected(signal_flow::StartRejection::CompositionRefused(
                signal_flow::CompositionRefusal::HandoverUnreadable(_)
            ))
        )));
        for _ in 0..2 {
            let mut engine = Engine::open(
                EngineOpen::new(&path, SchemaVersion::new(1))
                    .with_family(&FlowStore::launch_outcome_descriptor()),
            )
            .unwrap();
            FlowStore::upgrade_recovery_memory(&mut engine).expect("the step lands");
            let table = engine
                .register_table(FlowStore::launch_outcome_descriptor())
                .expect("the family is current");
            let mut read = engine
                .match_records(QueryPlan::all(table))
                .unwrap()
                .records()
                .to_vec();
            read.sort_by(|left, right| left.launch_request_id.0.cmp(&right.launch_request_id.0));
            assert_eq!(read, expected);
        }
        let published = signal_flow_60::StartRejection::CompositionRefused(
            signal_flow_60::CompositionRefusal::HandoverUnreadable(
                signal_flow_60::HandoverUnreadable_Data {
                    flow_id: signal_flow_60::FlowId("0000cc".into()),
                    handover_fault: signal_flow_60::HandoverFault::RootUnresolved,
                },
            ),
        );
        let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&published).unwrap();
        assert_eq!(
            rkyv::from_bytes::<StartRejectionV9, rkyv::rancor::Error>(&bytes).unwrap(),
            StartRejectionV9::CompositionRefused(CompositionRefusalV9::HandoverUnreadable(
                signal_flow::HandoverUnreadable_Data {
                    flow_id: signal_flow::FlowId("0000cc".into()),
                    handover_fault: signal_flow::HandoverFault::RootUnresolved,
                }
            ))
        );
    }
}
