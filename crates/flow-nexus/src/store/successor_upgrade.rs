//! The exact v3 successor archive upgrades to v4; old Refresh stays plain.
//! v4 is frozen here as the format the recovery step writes: the cause may
//! be `EmergencyRefreshWithoutHandover`, and an ordered Refresh's cause is
//! the unit `Ordered`, with no caller. The next step (`refresh_caller_upgrade`)
//! takes v4 to the current v5, whose `Ordered` holds its caller.
use super::ReadsFlowMemory as _;
use crate::generated::memory::{EmergencyRefreshWithoutHandover_Data, Predecessor};
use rkyv::{Archive, Deserialize, Serialize};
use sema_engine::{
    Decision, Engine, EngineRecord, FamilyName, MemoryDefinition, MemoryUpgrades, MemoryVersion,
    RecordKey, SchemaHash, TableDescriptor, TableReference, UpgradeFrom, UpgradeOutcome,
};
use signal_flow::{Address, LaunchRequestId};

#[derive(Archive, Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) enum SuccessionCauseV3 {
    Refresh,
    Exit,
    Ordered,
    Replace,
}
#[derive(Archive, Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct SuccessorIntentV3 {
    pub address: Address,
    pub predecessor: Predecessor,
    pub succession_cause: SuccessionCauseV3,
    pub launch_request_id: LaunchRequestId,
}
impl EngineRecord for SuccessorIntentV3 {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(super::FlowStore::address_record_key(&self.address))
    }
}
impl MemoryDefinition for SuccessorIntentV3 {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(3)
    }
    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label("flow-nexus-successor-intent-v3")
    }
}
/// `SuccessionCause` as v4 stored it: `Ordered` names no caller.
#[derive(Archive, Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) enum SuccessionCauseV4 {
    Refresh,
    Exit,
    Ordered,
    Replace,
    EmergencyRefreshWithoutHandover(EmergencyRefreshWithoutHandover_Data),
}
#[derive(Archive, Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct SuccessorIntentV4 {
    pub address: Address,
    pub predecessor: Predecessor,
    pub succession_cause: SuccessionCauseV4,
    pub launch_request_id: LaunchRequestId,
}
impl EngineRecord for SuccessorIntentV4 {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(super::FlowStore::address_record_key(&self.address))
    }
}
impl MemoryDefinition for SuccessorIntentV4 {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(4)
    }
    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label(SUCCESSOR_INTENT_SCHEMA_V4)
    }
}
/// The successor-intent family's schema label from this step's release to
/// the refresh-caller step.
pub(crate) const SUCCESSOR_INTENT_SCHEMA_V4: &str = "flow-nexus-successor-intent-v4";
pub(crate) trait DescribesSuccessorIntentV4Table {
    /// v4, evolving from v3.
    fn descriptor_v4() -> TableDescriptor<SuccessorIntentV4>;
}
impl DescribesSuccessorIntentV4Table for SuccessorIntentV4 {
    fn descriptor_v4() -> TableDescriptor<SuccessorIntentV4> {
        TableDescriptor::new(
            super::successor::SUCCESSOR_INTENT_TABLE_NAME,
            FamilyName::new(super::successor::SUCCESSOR_INTENT_FAMILY),
            SchemaHash::for_label(SUCCESSOR_INTENT_SCHEMA_V4),
        )
        .with_predecessor::<SuccessorIntentV3>()
    }
}
impl UpgradeFrom<SuccessorIntentV3> for SuccessorIntentV4 {
    type Refusal = std::convert::Infallible;
    fn upgrade_from(before: SuccessorIntentV3) -> Decision<Self, Self::Refusal> {
        Decision::Ready(Self {
            address: before.address,
            predecessor: before.predecessor,
            launch_request_id: before.launch_request_id,
            succession_cause: match before.succession_cause {
                SuccessionCauseV3::Refresh => SuccessionCauseV4::Refresh,
                SuccessionCauseV3::Exit => SuccessionCauseV4::Exit,
                SuccessionCauseV3::Ordered => SuccessionCauseV4::Ordered,
                SuccessionCauseV3::Replace => SuccessionCauseV4::Replace,
            },
        })
    }
}
pub(crate) trait UpgradesSuccessorIntent: Sized {
    fn upgrade_or_register(engine: &mut Engine) -> Result<TableReference<Self>, String>;
}
impl UpgradesSuccessorIntent for SuccessorIntentV4 {
    fn upgrade_or_register(engine: &mut Engine) -> Result<TableReference<Self>, String> {
        match engine.upgrade_table::<SuccessorIntentV3, SuccessorIntentV4>(Self::descriptor_v4()) {
            UpgradeOutcome::Upgraded(receipt) => Ok(receipt.table()),
            UpgradeOutcome::AlreadyCurrent(table) => Ok(table),
            UpgradeOutcome::Refused(refusal) => Err(format!("{refusal:?}")),
            UpgradeOutcome::Failed(failure) => Err(format!("{failure:?}")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn every_v3_cause_upgrades_to_the_corresponding_ordinary_cause() {
        for (old, expected) in [
            (SuccessionCauseV3::Refresh, SuccessionCauseV4::Refresh),
            (SuccessionCauseV3::Exit, SuccessionCauseV4::Exit),
            (SuccessionCauseV3::Ordered, SuccessionCauseV4::Ordered),
            (SuccessionCauseV3::Replace, SuccessionCauseV4::Replace),
        ] {
            let before = SuccessorIntentV3 {
                address: Address {
                    aspect: signal_flow::Aspect::Mind,
                    topic: flow_ethos::Topic("recoveryUpgrade".into()),
                    model: signal_flow::Model::Sol,
                },
                predecessor: Predecessor("before".into()),
                succession_cause: old,
                launch_request_id: LaunchRequestId("launch-before".into()),
            };
            let Decision::Ready(after) = SuccessorIntentV4::upgrade_from(before.clone());
            assert_eq!(after.succession_cause, expected);
            assert_eq!(after.address, before.address);
            assert_eq!(after.predecessor, before.predecessor);
            assert_eq!(after.launch_request_id, before.launch_request_id);
        }
    }

    /// Gap 6. A persisted v3 intent upgrades through the family's own
    /// upgrader, and the emergency provenance written over it, with no path
    /// and with one, reads back whole after a reopen: Address, predecessor,
    /// launch request and the cause with its exact predecessor and path.
    #[test]
    fn persisted_v3_intent_upgrades_and_v4_emergency_provenance_survives_reopen() {
        use sema_engine::{
            Assertion, EngineOpen, FamilyName, QueryPlan, SchemaVersion, TableDescriptor,
        };
        for path in [
            None,
            Some(signal_flow::TranscriptPath(
                "/native/sessions/before transcript.jsonl".into(),
            )),
        ] {
            let directory = tempfile::tempdir().unwrap();
            let store = directory.path().join("successor-v3.sema");
            let address = Address {
                aspect: signal_flow::Aspect::Mind,
                topic: flow_ethos::Topic("persistedRecovery".into()),
                model: signal_flow::Model::Sol,
            };
            let old = SuccessorIntentV3 {
                address: address.clone(),
                predecessor: Predecessor("before".into()),
                succession_cause: SuccessionCauseV3::Refresh,
                launch_request_id: LaunchRequestId("launch-before".into()),
            };
            {
                let mut engine =
                    Engine::open(EngineOpen::new(&store, SchemaVersion::new(1))).unwrap();
                let descriptor = TableDescriptor::<SuccessorIntentV3>::new(
                    super::super::successor::SUCCESSOR_INTENT_TABLE_NAME,
                    FamilyName::new(super::super::successor::SUCCESSOR_INTENT_FAMILY),
                    SuccessorIntentV3::schema_hash(),
                );
                let table = engine.register_table(descriptor).unwrap();
                engine.assert(Assertion::new(table, old.clone())).unwrap();
            }
            let key = old.record_key();
            let emergency = SuccessorIntentV4 {
                address: address.clone(),
                predecessor: Predecessor("before".into()),
                succession_cause: SuccessionCauseV4::EmergencyRefreshWithoutHandover(
                    crate::generated::memory::EmergencyRefreshWithoutHandover_Data {
                        flow_id: signal_flow::FlowId("before".into()),
                        transcript_path_option: path.clone(),
                    },
                ),
                launch_request_id: LaunchRequestId("launch-before".into()),
            };
            {
                let mut engine = Engine::open(
                    EngineOpen::new(&store, SchemaVersion::new(1))
                        .with_family(&SuccessorIntentV4::descriptor_v4()),
                )
                .unwrap();
                let table = SuccessorIntentV4::upgrade_or_register(&mut engine).unwrap();
                let row = engine
                    .match_records(QueryPlan::key(table, key.clone()))
                    .unwrap()
                    .records()[0]
                    .clone();
                assert_eq!(
                    row,
                    SuccessorIntentV4 {
                        succession_cause: SuccessionCauseV4::Refresh,
                        ..emergency.clone()
                    }
                );
                engine
                    .mutate_keyed(sema_engine::KeyedMutation::new(
                        table,
                        key.clone(),
                        emergency.clone(),
                    ))
                    .unwrap();
            }
            for _ in 0..2 {
                let mut engine = Engine::open(
                    EngineOpen::new(&store, SchemaVersion::new(1))
                        .with_family(&SuccessorIntentV4::descriptor_v4()),
                )
                .unwrap();
                let table = SuccessorIntentV4::upgrade_or_register(&mut engine).unwrap();
                assert_eq!(
                    engine
                        .match_records(QueryPlan::key(table, key.clone()))
                        .unwrap()
                        .records(),
                    std::slice::from_ref(&emergency),
                    "{path:?}"
                );
            }
        }
    }
}
