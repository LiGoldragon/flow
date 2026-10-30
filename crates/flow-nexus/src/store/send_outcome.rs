//! Durable, countable answers to ordinary Send queries.

use super::send_v31::{RefusalV31, SendAcceptanceV31, SendFailureV31};
use super::send_v32::{RefusalV32, SendAcceptanceV32, SendFailureV32};
use super::send_v34::{RefusalV34, SendAcceptanceV34, SendFailureV34};
use super::*;
use meta_signal_flow::{OrdinarySendOutcome, SendOutcomeRecord};
use signal_flow::{Address, Process, Response};

pub(super) const COUNTER_FAMILY: &str = "flow-nexus-send-outcome-counter";
pub(super) const COUNTER_SCHEMA: &str = "flow-nexus-send-outcome-counter-v1";
pub(super) const OUTCOME_FAMILY: &str = "flow-nexus-send-outcome";
pub(super) const OUTCOME_SCHEMA: &str = "flow-nexus-send-outcome-v5";

const COUNTER_KEY: &str = "counter";
const SEND_OUTCOME_COUNTER_TABLE_NAME: TableName =
    TableName::new("flow_nexus_send_outcome_counter");
const SEND_OUTCOME_TABLE_NAME: TableName = TableName::new("flow_nexus_send_outcomes");

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct SendOutcomeCounter {
    next_id: u64,
}

impl EngineRecord for SendOutcomeCounter {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(COUNTER_KEY)
    }
}

impl MemoryDefinition for SendOutcomeCounter {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(1)
    }

    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label(COUNTER_SCHEMA)
    }
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct StoredSendOutcome {
    record: SendOutcomeRecord,
}

impl EngineRecord for StoredSendOutcome {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.record.send_outcome_id.clone())
    }
}

impl MemoryDefinition for StoredSendOutcome {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(5)
    }

    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label(OUTCOME_SCHEMA)
    }
}

/// An ordinary Send's typed answer as signal-flow 34 archived it, read
/// only by the v4-to-v5 upgrade.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) enum OrdinarySendOutcomeV4 {
    Accepted(SendAcceptanceV34),
    Refused(RefusalV34),
    PriorAdmission(meta_signal_flow::PriorSendOutcome),
    Sent(signal_flow::SendReceipt),
    Failed(SendFailureV34),
}
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct SendOutcomeRecordV4 {
    send_outcome_id: String,
    process_option: Option<Process>,
    first_address_option: Option<Address>,
    second_address_option: Option<Address>,
    request_digest: String,
    ordinary_send_outcome: OrdinarySendOutcomeV4,
}
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct StoredSendOutcomeV4 {
    record: SendOutcomeRecordV4,
}
impl EngineRecord for StoredSendOutcomeV4 {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.record.send_outcome_id.clone())
    }
}
impl MemoryDefinition for StoredSendOutcomeV4 {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(4)
    }
    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label("flow-nexus-send-outcome-v4")
    }
}
impl UpgradeFrom<StoredSendOutcomeV4> for StoredSendOutcome {
    type Refusal = std::convert::Infallible;
    fn upgrade_from(previous: StoredSendOutcomeV4) -> Decision<Self, Self::Refusal> {
        let old = previous.record;
        let outcome = match old.ordinary_send_outcome {
            OrdinarySendOutcomeV4::Accepted(acceptance) => {
                OrdinarySendOutcome::Accepted(acceptance.into())
            }
            OrdinarySendOutcomeV4::Refused(refusal) => OrdinarySendOutcome::Refused(refusal.into()),
            OrdinarySendOutcomeV4::PriorAdmission(prior) => {
                OrdinarySendOutcome::PriorAdmission(prior)
            }
            OrdinarySendOutcomeV4::Sent(receipt) => OrdinarySendOutcome::Sent(receipt),
            OrdinarySendOutcomeV4::Failed(failure) => OrdinarySendOutcome::Failed(failure.into()),
        };
        Decision::Ready(Self {
            record: SendOutcomeRecord {
                send_outcome_id: old.send_outcome_id,
                process_option: old.process_option,
                first_address_option: old.first_address_option,
                second_address_option: old.second_address_option,
                request_digest: old.request_digest,
                ordinary_send_outcome: outcome,
            },
        })
    }
}

/// An ordinary Send's typed answer as signal-flow 32 archived it, read
/// only by the v3-to-v5 upgrade.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) enum OrdinarySendOutcomeV3 {
    Accepted(SendAcceptanceV32),
    Refused(RefusalV32),
    PriorAdmission(meta_signal_flow::PriorSendOutcome),
    Sent(signal_flow::SendReceipt),
    Failed(SendFailureV32),
}
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct SendOutcomeRecordV3 {
    send_outcome_id: String,
    process_option: Option<Process>,
    first_address_option: Option<Address>,
    second_address_option: Option<Address>,
    request_digest: String,
    ordinary_send_outcome: OrdinarySendOutcomeV3,
}
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct StoredSendOutcomeV3 {
    record: SendOutcomeRecordV3,
}
impl EngineRecord for StoredSendOutcomeV3 {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.record.send_outcome_id.clone())
    }
}
impl MemoryDefinition for StoredSendOutcomeV3 {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(3)
    }
    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label("flow-nexus-send-outcome-v3")
    }
}
impl UpgradeFrom<StoredSendOutcomeV3> for StoredSendOutcome {
    type Refusal = std::convert::Infallible;
    fn upgrade_from(previous: StoredSendOutcomeV3) -> Decision<Self, Self::Refusal> {
        let old = previous.record;
        let outcome = match old.ordinary_send_outcome {
            OrdinarySendOutcomeV3::Accepted(acceptance) => {
                OrdinarySendOutcome::Accepted(acceptance.into())
            }
            OrdinarySendOutcomeV3::Refused(refusal) => OrdinarySendOutcome::Refused(refusal.into()),
            OrdinarySendOutcomeV3::PriorAdmission(prior) => {
                OrdinarySendOutcome::PriorAdmission(prior)
            }
            OrdinarySendOutcomeV3::Sent(receipt) => OrdinarySendOutcome::Sent(receipt),
            OrdinarySendOutcomeV3::Failed(failure) => OrdinarySendOutcome::Failed(failure.into()),
        };
        Decision::Ready(Self {
            record: SendOutcomeRecord {
                send_outcome_id: old.send_outcome_id,
                process_option: old.process_option,
                first_address_option: old.first_address_option,
                second_address_option: old.second_address_option,
                request_digest: old.request_digest,
                ordinary_send_outcome: outcome,
            },
        })
    }
}

/// An ordinary Send's typed answer as signal-flow 31 archived it, read
/// only by the v2-to-v5 upgrade.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) enum OrdinarySendOutcomeV2 {
    Accepted(SendAcceptanceV31),
    Refused(RefusalV31),
    PriorAdmission(meta_signal_flow::PriorSendOutcome),
    Sent(signal_flow::SendReceipt),
    Failed(SendFailureV31),
}
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct SendOutcomeRecordV2 {
    send_outcome_id: String,
    process_option: Option<Process>,
    first_address_option: Option<Address>,
    second_address_option: Option<Address>,
    request_digest: String,
    ordinary_send_outcome: OrdinarySendOutcomeV2,
}
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct StoredSendOutcomeV2 {
    record: SendOutcomeRecordV2,
}
impl EngineRecord for StoredSendOutcomeV2 {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.record.send_outcome_id.clone())
    }
}
impl MemoryDefinition for StoredSendOutcomeV2 {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(2)
    }
    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label("flow-nexus-send-outcome-v2")
    }
}
impl UpgradeFrom<StoredSendOutcomeV2> for StoredSendOutcome {
    type Refusal = std::convert::Infallible;
    fn upgrade_from(previous: StoredSendOutcomeV2) -> Decision<Self, Self::Refusal> {
        let old = previous.record;
        let outcome = match old.ordinary_send_outcome {
            OrdinarySendOutcomeV2::Accepted(acceptance) => {
                OrdinarySendOutcome::Accepted(acceptance.into())
            }
            OrdinarySendOutcomeV2::Refused(refusal) => OrdinarySendOutcome::Refused(refusal.into()),
            OrdinarySendOutcomeV2::PriorAdmission(prior) => {
                OrdinarySendOutcome::PriorAdmission(prior)
            }
            OrdinarySendOutcomeV2::Sent(receipt) => OrdinarySendOutcome::Sent(receipt),
            OrdinarySendOutcomeV2::Failed(failure) => OrdinarySendOutcome::Failed(failure.into()),
        };
        Decision::Ready(Self {
            record: SendOutcomeRecord {
                send_outcome_id: old.send_outcome_id,
                process_option: old.process_option,
                first_address_option: old.first_address_option,
                second_address_option: old.second_address_option,
                request_digest: old.request_digest,
                ordinary_send_outcome: outcome,
            },
        })
    }
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) enum PriorOrdinarySendOutcome {
    Queued,
    Delivered,
    Refused(RefusalV31),
}
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct SendOutcomeRecordV1 {
    send_outcome_id: String,
    process_option: Option<Process>,
    first_address_option: Option<Address>,
    second_address_option: Option<Address>,
    request_digest: String,
    ordinary_send_outcome: PriorOrdinarySendOutcome,
}
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct StoredSendOutcomeV1 {
    record: SendOutcomeRecordV1,
}
impl EngineRecord for StoredSendOutcomeV1 {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.record.send_outcome_id.clone())
    }
}
impl MemoryDefinition for StoredSendOutcomeV1 {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(1)
    }
    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label("flow-nexus-send-outcome-v1")
    }
}
impl UpgradeFrom<StoredSendOutcomeV1> for StoredSendOutcome {
    type Refusal = std::convert::Infallible;
    fn upgrade_from(previous: StoredSendOutcomeV1) -> Decision<Self, Self::Refusal> {
        let old = previous.record;
        let outcome = match old.ordinary_send_outcome {
            PriorOrdinarySendOutcome::Queued => {
                OrdinarySendOutcome::PriorAdmission(meta_signal_flow::PriorSendOutcome::Queued)
            }
            PriorOrdinarySendOutcome::Delivered => {
                OrdinarySendOutcome::PriorAdmission(meta_signal_flow::PriorSendOutcome::Delivered)
            }
            PriorOrdinarySendOutcome::Refused(refusal) => {
                OrdinarySendOutcome::Refused(refusal.into())
            }
        };
        Decision::Ready(Self {
            record: SendOutcomeRecord {
                send_outcome_id: old.send_outcome_id,
                process_option: old.process_option,
                first_address_option: old.first_address_option,
                second_address_option: old.second_address_option,
                request_digest: old.request_digest,
                ordinary_send_outcome: outcome,
            },
        })
    }
}

pub(crate) struct SendOutcomeTables {
    counter: TableReference<SendOutcomeCounter>,
    outcomes: TableReference<StoredSendOutcome>,
    write_gate: Mutex<()>,
}

pub(crate) struct SendOutcomeDescriptors {
    pub(crate) counter: TableDescriptor<SendOutcomeCounter>,
    pub(crate) outcomes: TableDescriptor<StoredSendOutcome>,
}

pub(crate) trait DeclaresSendOutcomeTables {
    fn descriptors() -> SendOutcomeDescriptors;
    fn register(
        engine: &mut Engine,
        descriptors: SendOutcomeDescriptors,
    ) -> Result<SendOutcomeTables, StoreError>;
}

impl DeclaresSendOutcomeTables for SendOutcomeTables {
    fn descriptors() -> SendOutcomeDescriptors {
        SendOutcomeDescriptors {
            counter: TableDescriptor::new(
                SEND_OUTCOME_COUNTER_TABLE_NAME,
                FamilyName::new(COUNTER_FAMILY),
                SendOutcomeCounter::schema_hash(),
            ),
            outcomes: TableDescriptor::new(
                SEND_OUTCOME_TABLE_NAME,
                FamilyName::new(OUTCOME_FAMILY),
                StoredSendOutcome::schema_hash(),
            )
            .with_predecessor::<StoredSendOutcomeV1>()
            .with_predecessor::<StoredSendOutcomeV2>()
            .with_predecessor::<StoredSendOutcomeV3>()
            .with_predecessor::<StoredSendOutcomeV4>(),
        }
    }

    fn register(
        engine: &mut Engine,
        descriptors: SendOutcomeDescriptors,
    ) -> Result<SendOutcomeTables, StoreError> {
        Ok(SendOutcomeTables {
            counter: engine.register_table(descriptors.counter)?,
            // A v4 family upgrades from v4, a v3 family from v3; one refused at admission is a v2
            // family, then a v1 family, each upgraded from its own shape.
            outcomes: engine.upgraded_outcomes(descriptors.outcomes)?,
            write_gate: Mutex::new(()),
        })
    }
}

trait UpgradesSendOutcomes {
    fn upgraded_outcomes(
        &mut self,
        descriptor: TableDescriptor<StoredSendOutcome>,
    ) -> Result<TableReference<StoredSendOutcome>, StoreError>;
}

impl UpgradesSendOutcomes for Engine {
    fn upgraded_outcomes(
        &mut self,
        descriptor: TableDescriptor<StoredSendOutcome>,
    ) -> Result<TableReference<StoredSendOutcome>, StoreError> {
        let refused = |failure: &dyn std::fmt::Debug| {
            StoreError::QueueDeliveryUpgrade(format!("sender outcome upgrade: {failure:?}"))
        };
        match self.upgrade_table::<StoredSendOutcomeV4, StoredSendOutcome>(descriptor.clone()) {
            UpgradeOutcome::Upgraded(receipt) => return Ok(receipt.table()),
            UpgradeOutcome::AlreadyCurrent(table) => return Ok(table),
            UpgradeOutcome::Refused(refusal) => match *refusal.reason() {},
            UpgradeOutcome::Failed(failure)
                if failure.phase() == sema_engine::StoragePhase::Admission => {}
            UpgradeOutcome::Failed(failure) => return Err(refused(&failure)),
        }
        match self.upgrade_table::<StoredSendOutcomeV3, StoredSendOutcome>(descriptor.clone()) {
            UpgradeOutcome::Upgraded(receipt) => return Ok(receipt.table()),
            UpgradeOutcome::AlreadyCurrent(table) => return Ok(table),
            UpgradeOutcome::Refused(refusal) => match *refusal.reason() {},
            UpgradeOutcome::Failed(failure)
                if failure.phase() == sema_engine::StoragePhase::Admission => {}
            UpgradeOutcome::Failed(failure) => return Err(refused(&failure)),
        }
        match self.upgrade_table::<StoredSendOutcomeV2, StoredSendOutcome>(descriptor.clone()) {
            UpgradeOutcome::Upgraded(receipt) => return Ok(receipt.table()),
            UpgradeOutcome::AlreadyCurrent(table) => return Ok(table),
            UpgradeOutcome::Refused(refusal) => match *refusal.reason() {},
            UpgradeOutcome::Failed(failure)
                if failure.phase() == sema_engine::StoragePhase::Admission => {}
            UpgradeOutcome::Failed(failure) => return Err(refused(&failure)),
        }
        match self.upgrade_table::<StoredSendOutcomeV1, StoredSendOutcome>(descriptor) {
            UpgradeOutcome::Upgraded(receipt) => Ok(receipt.table()),
            UpgradeOutcome::AlreadyCurrent(table) => Ok(table),
            UpgradeOutcome::Refused(refusal) => match *refusal.reason() {},
            UpgradeOutcome::Failed(failure) => Err(refused(&failure)),
        }
    }
}

pub(crate) trait RecordsSendOutcomes {
    fn record_send_outcome(
        &self,
        caller: Option<Process>,
        sender: Option<Address>,
        recipient: Option<Address>,
        request_digest: String,
        response: Response,
    ) -> Result<SendOutcomeRecord, StoreError>;
    fn read_send_outcomes(&self) -> Result<Vec<SendOutcomeRecord>, StoreError>;
}

impl RecordsSendOutcomes for FlowStore {
    fn record_send_outcome(
        &self,
        caller: Option<Process>,
        sender: Option<Address>,
        recipient: Option<Address>,
        request_digest: String,
        response: Response,
    ) -> Result<SendOutcomeRecord, StoreError> {
        let _write = self
            .send_outcomes
            .write_gate
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let counters = self
            .engine
            .match_records(QueryPlan::key(
                self.send_outcomes.counter,
                RecordKey::new(COUNTER_KEY),
            ))?
            .records()
            .to_vec();
        let ordinary_send_outcome = match response {
            Response::SendAccepted(acceptance) => OrdinarySendOutcome::Accepted(acceptance),
            Response::Refused(refusal) => OrdinarySendOutcome::Refused(refusal),
            Response::Sent(receipt) => OrdinarySendOutcome::Sent(receipt),
            Response::Failed(failure) => OrdinarySendOutcome::Failed(failure),
            _ => return Err(StoreError::StateInvariant),
        };
        let existing_counter = match counters.as_slice() {
            [] => None,
            [counter] => Some(counter.clone()),
            _ => return Err(StoreError::StateInvariant),
        };
        let record_number = existing_counter
            .as_ref()
            .map(|counter| counter.next_id)
            .unwrap_or(1);
        let next_counter = SendOutcomeCounter {
            next_id: record_number
                .checked_add(1)
                .ok_or(StoreError::StateInvariant)?,
        };
        let record = SendOutcomeRecord {
            send_outcome_id: format!("send-outcome-{record_number:016x}"),
            process_option: caller,
            first_address_option: sender,
            second_address_option: recipient,
            request_digest,
            ordinary_send_outcome,
        };
        let counter_mutation = self.engine.begin_atomic_commit();
        let counter_mutation = if existing_counter.is_some() {
            counter_mutation.mutate(self.send_outcomes.counter, next_counter)
        } else {
            counter_mutation.assert(self.send_outcomes.counter, next_counter)
        };
        self.engine.commit_atomic(counter_mutation.assert(
            self.send_outcomes.outcomes,
            StoredSendOutcome {
                record: record.clone(),
            },
        ))?;
        Ok(record)
    }

    fn read_send_outcomes(&self) -> Result<Vec<SendOutcomeRecord>, StoreError> {
        Ok(self
            .engine
            .match_records(QueryPlan::all(self.send_outcomes.outcomes))?
            .records()
            .iter()
            .map(|stored| stored.record.clone())
            .collect())
    }
}

/// The send outcome ids: a store that has allocated none begins at 1.
pub(crate) struct SendOutcomeIds<'store>(pub(crate) &'store FlowStore);

impl super::counters::CountsIds for SendOutcomeIds<'_> {
    fn hold_writes(&self) -> Option<std::sync::MutexGuard<'_, ()>> {
        Some(
            self.0
                .send_outcomes
                .write_gate
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()),
        )
    }

    fn id_counter(&self) -> Result<super::counters::IdCounter, StoreError> {
        let counters = self.0.engine.match_records(QueryPlan::key(
            self.0.send_outcomes.counter,
            RecordKey::new(COUNTER_KEY),
        ))?;
        match counters.records() {
            [] => Ok(super::counters::IdCounter {
                next: 1,
                written: false,
            }),
            [counter] => Ok(super::counters::IdCounter {
                next: i64::try_from(counter.next_id).map_err(|_| StoreError::StateInvariant)?,
                written: true,
            }),
            _ => Err(StoreError::StateInvariant),
        }
    }

    fn ids_recorded(&self) -> Result<bool, StoreError> {
        Ok(!self
            .0
            .engine
            .match_records(QueryPlan::all(self.0.send_outcomes.outcomes))?
            .records()
            .is_empty())
    }

    fn declare_next_id(
        &self,
        commit: sema_engine::AtomicCommit,
        counter: &super::counters::IdCounter,
        next: i64,
    ) -> Result<sema_engine::AtomicCommit, StoreError> {
        let row = SendOutcomeCounter {
            next_id: u64::try_from(next).map_err(|_| StoreError::StateInvariant)?,
        };
        Ok(if counter.written {
            commit.mutate(self.0.send_outcomes.counter, row)
        } else {
            commit.assert(self.0.send_outcomes.counter, row)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn open_tables(engine: &mut Engine) -> Result<SendOutcomeTables, StoreError> {
        SendOutcomeTables::register(engine, SendOutcomeTables::descriptors())
    }

    #[test]
    fn send_outcome_counter_and_records_survive_reopen() {
        let fixture = tempfile::tempdir().expect("temporary store path");
        let store_path = fixture.path().join("outcomes.sema");
        let first = SendOutcomeRecord {
            send_outcome_id: "send-outcome-0000000000000001".into(),
            process_option: None,
            first_address_option: None,
            second_address_option: None,
            request_digest: "abc".into(),
            ordinary_send_outcome: OrdinarySendOutcome::Accepted(signal_flow::SendAcceptance {
                logical_delivery_id: "logical-delivery-0000000000000001".into(),
                delivery_cursor: 1,
                delivery_state: signal_flow::DeliveryState::Queued,
            }),
        };
        {
            let descriptors = SendOutcomeTables::descriptors();
            let mut engine = Engine::open(
                EngineOpen::new(&store_path, SchemaVersion::new(1))
                    .with_family(&descriptors.counter)
                    .with_family(&descriptors.outcomes),
            )
            .expect("ledger store opens");
            let tables = open_tables(&mut engine).expect("ledger tables register");
            let stored = StoredSendOutcome {
                record: first.clone(),
            };
            engine
                .commit_atomic(
                    engine
                        .begin_atomic_commit()
                        .assert(tables.counter, SendOutcomeCounter { next_id: 2 })
                        .assert(tables.outcomes, stored),
                )
                .expect("seed record commits");
        }
        {
            let descriptors = SendOutcomeTables::descriptors();
            let mut engine = Engine::open(
                EngineOpen::new(&store_path, SchemaVersion::new(1))
                    .with_family(&descriptors.counter)
                    .with_family(&descriptors.outcomes),
            )
            .expect("ledger store reopens");
            let tables = open_tables(&mut engine).expect("ledger tables reopen");
            let rows = engine
                .match_records(QueryPlan::all(tables.outcomes))
                .expect("ledger reads")
                .records()
                .to_vec();
            assert_eq!(rows, vec![StoredSendOutcome { record: first }]);
        }
    }

    /// Outcomes kept under the v2 label, in signal-flow 31's shape, open as
    /// the current outcomes, every refusal and state as it was.
    #[test]
    fn v2_send_outcomes_upgrade_to_the_withdrawn_and_topic_shape() {
        use super::super::send_v31::{DeliveryStateV31, SendFailureReasonV31};
        let fixture = tempfile::tempdir().expect("temporary store path");
        let store_path = fixture.path().join("outcomes.sema");
        let previous = |id: &str, ordinary_send_outcome| StoredSendOutcomeV2 {
            record: SendOutcomeRecordV2 {
                send_outcome_id: id.into(),
                process_option: None,
                first_address_option: None,
                second_address_option: None,
                request_digest: "abc".into(),
                ordinary_send_outcome,
            },
        };
        {
            let mut engine = Engine::open(EngineOpen::new(&store_path, SchemaVersion::new(1)))
                .expect("ledger store opens");
            let outcomes = engine
                .register_table(TableDescriptor::<StoredSendOutcomeV2>::new(
                    SEND_OUTCOME_TABLE_NAME,
                    FamilyName::new(OUTCOME_FAMILY),
                    StoredSendOutcomeV2::schema_hash(),
                ))
                .expect("v2 family registers");
            for row in [
                previous(
                    "accepted",
                    OrdinarySendOutcomeV2::Accepted(SendAcceptanceV31 {
                        logical_delivery_id: "logical-1".into(),
                        delivery_cursor: 1,
                        delivery_state: DeliveryStateV31::Failed(RefusalV31::Direct("gone".into())),
                    }),
                ),
                previous(
                    "refused",
                    OrdinarySendOutcomeV2::Refused(RefusalV31::OffRoute),
                ),
                previous(
                    "busy",
                    OrdinarySendOutcomeV2::Failed(SendFailureV31 {
                        logical_delivery_id_option: None,
                        send_failure_reason: SendFailureReasonV31::RecipientBusy(Address {
                            aspect: signal_flow::Aspect::Psyche,
                            topic: "core".into(),
                            layer: signal_flow::Layer::Secondary,
                        }),
                    }),
                ),
                previous(
                    "failed",
                    OrdinarySendOutcomeV2::Failed(SendFailureV31 {
                        logical_delivery_id_option: None,
                        send_failure_reason: SendFailureReasonV31::Refused(RefusalV31::NoneAbove),
                    }),
                ),
            ] {
                engine
                    .assert(sema_engine::Assertion::new(outcomes, row))
                    .expect("outcome persists");
            }
        }
        let descriptors = SendOutcomeTables::descriptors();
        let mut engine = Engine::open(
            EngineOpen::new(&store_path, SchemaVersion::new(1))
                .with_family(&descriptors.counter)
                .with_family(&descriptors.outcomes),
        )
        .expect("ledger store reopens");
        let tables = open_tables(&mut engine).expect("ledger tables upgrade");
        let rows = engine
            .match_records(QueryPlan::all(tables.outcomes))
            .expect("ledger reads")
            .records()
            .iter()
            .map(|stored| {
                (
                    stored.record.send_outcome_id.clone(),
                    stored.record.ordinary_send_outcome.clone(),
                )
            })
            .collect::<std::collections::BTreeMap<_, _>>();
        assert_eq!(
            rows["accepted"],
            OrdinarySendOutcome::Accepted(signal_flow::SendAcceptance {
                logical_delivery_id: "logical-1".into(),
                delivery_cursor: 1,
                delivery_state: signal_flow::DeliveryState::Failed(signal_flow::Refusal::Direct(
                    "gone".into()
                )),
            })
        );
        assert_eq!(
            rows["refused"],
            OrdinarySendOutcome::Refused(signal_flow::Refusal::Direct("off route".into()))
        );
        assert_eq!(
            rows["busy"],
            OrdinarySendOutcome::Failed(signal_flow::SendFailure {
                logical_delivery_id_option: None,
                send_failure_reason: signal_flow::SendFailureReason::NotSent(
                    "recipient busy { Psyche core Secondary }".into()
                ),
            })
        );
        assert_eq!(
            rows["failed"],
            OrdinarySendOutcome::Failed(signal_flow::SendFailure {
                logical_delivery_id_option: None,
                send_failure_reason: signal_flow::SendFailureReason::Refused(
                    signal_flow::Refusal::NoneAbove
                ),
            })
        );
    }

    /// Outcomes kept under the v3 label, in signal-flow 32's shape, open as
    /// the current outcomes, every refusal and state as it was.
    #[test]
    fn v3_send_outcomes_upgrade_off_route_and_recipient_busy() {
        use super::super::send_v32::{DeliveryStateV32, SendFailureReasonV32};
        let fixture = tempfile::tempdir().expect("temporary store path");
        let store_path = fixture.path().join("outcomes.sema");
        let previous = |id: &str, ordinary_send_outcome| StoredSendOutcomeV3 {
            record: SendOutcomeRecordV3 {
                send_outcome_id: id.into(),
                process_option: None,
                first_address_option: None,
                second_address_option: None,
                request_digest: "abc".into(),
                ordinary_send_outcome,
            },
        };
        {
            let mut engine = Engine::open(EngineOpen::new(&store_path, SchemaVersion::new(1)))
                .expect("ledger store opens");
            let outcomes = engine
                .register_table(TableDescriptor::<StoredSendOutcomeV3>::new(
                    SEND_OUTCOME_TABLE_NAME,
                    FamilyName::new(OUTCOME_FAMILY),
                    StoredSendOutcomeV3::schema_hash(),
                ))
                .expect("v3 family registers");
            for row in [
                previous(
                    "accepted",
                    OrdinarySendOutcomeV3::Accepted(SendAcceptanceV32 {
                        logical_delivery_id: "logical-1".into(),
                        delivery_cursor: 1,
                        delivery_state: DeliveryStateV32::Failed(RefusalV32::Direct("gone".into())),
                    }),
                ),
                previous(
                    "refused",
                    OrdinarySendOutcomeV3::Refused(RefusalV32::OffRoute),
                ),
                previous(
                    "busy",
                    OrdinarySendOutcomeV3::Failed(SendFailureV32 {
                        logical_delivery_id_option: None,
                        send_failure_reason: SendFailureReasonV32::RecipientBusy(Address {
                            aspect: signal_flow::Aspect::Psyche,
                            topic: "core".into(),
                            layer: signal_flow::Layer::Secondary,
                        }),
                    }),
                ),
                previous(
                    "failed",
                    OrdinarySendOutcomeV3::Failed(SendFailureV32 {
                        logical_delivery_id_option: None,
                        send_failure_reason: SendFailureReasonV32::Refused(RefusalV32::NoneAbove),
                    }),
                ),
            ] {
                engine
                    .assert(sema_engine::Assertion::new(outcomes, row))
                    .expect("outcome persists");
            }
        }
        let descriptors = SendOutcomeTables::descriptors();
        let mut engine = Engine::open(
            EngineOpen::new(&store_path, SchemaVersion::new(1))
                .with_family(&descriptors.counter)
                .with_family(&descriptors.outcomes),
        )
        .expect("ledger store reopens");
        let tables = open_tables(&mut engine).expect("ledger tables upgrade");
        let rows = engine
            .match_records(QueryPlan::all(tables.outcomes))
            .expect("ledger reads")
            .records()
            .iter()
            .map(|stored| {
                (
                    stored.record.send_outcome_id.clone(),
                    stored.record.ordinary_send_outcome.clone(),
                )
            })
            .collect::<std::collections::BTreeMap<_, _>>();
        assert_eq!(
            rows["accepted"],
            OrdinarySendOutcome::Accepted(signal_flow::SendAcceptance {
                logical_delivery_id: "logical-1".into(),
                delivery_cursor: 1,
                delivery_state: signal_flow::DeliveryState::Failed(signal_flow::Refusal::Direct(
                    "gone".into()
                )),
            })
        );
        assert_eq!(
            rows["refused"],
            OrdinarySendOutcome::Refused(signal_flow::Refusal::Direct("off route".into()))
        );
        assert_eq!(
            rows["busy"],
            OrdinarySendOutcome::Failed(signal_flow::SendFailure {
                logical_delivery_id_option: None,
                send_failure_reason: signal_flow::SendFailureReason::NotSent(
                    "recipient busy { Psyche core Secondary }".into()
                ),
            })
        );
        assert_eq!(
            rows["failed"],
            OrdinarySendOutcome::Failed(signal_flow::SendFailure {
                logical_delivery_id_option: None,
                send_failure_reason: signal_flow::SendFailureReason::Refused(
                    signal_flow::Refusal::NoneAbove
                ),
            })
        );
    }

    /// Outcomes kept under the v4 label, in signal-flow 34's shape, open as
    /// the current outcomes; a stored `Unidentified` keeps its process and
    /// gains no pane, no label and no session, observed at 0.
    #[test]
    fn v4_send_outcomes_upgrade_unidentified_to_its_evidence() {
        use super::super::send_v34::{DeliveryStateV34, SendFailureReasonV34};
        let fixture = tempfile::tempdir().expect("temporary store path");
        let store_path = fixture.path().join("outcomes.sema");
        let process = Process {
            pid: 4242,
            started: 987654,
        };
        let previous = |id: &str, ordinary_send_outcome| StoredSendOutcomeV4 {
            record: SendOutcomeRecordV4 {
                send_outcome_id: id.into(),
                process_option: None,
                first_address_option: None,
                second_address_option: None,
                request_digest: "abc".into(),
                ordinary_send_outcome,
            },
        };
        {
            let mut engine = Engine::open(EngineOpen::new(&store_path, SchemaVersion::new(1)))
                .expect("ledger store opens");
            let outcomes = engine
                .register_table(TableDescriptor::<StoredSendOutcomeV4>::new(
                    SEND_OUTCOME_TABLE_NAME,
                    FamilyName::new(OUTCOME_FAMILY),
                    StoredSendOutcomeV4::schema_hash(),
                ))
                .expect("v4 family registers");
            for row in [
                previous(
                    "accepted",
                    OrdinarySendOutcomeV4::Accepted(SendAcceptanceV34 {
                        logical_delivery_id: "logical-1".into(),
                        delivery_cursor: 1,
                        delivery_state: DeliveryStateV34::Failed(RefusalV34::Unidentified(
                            process.clone(),
                        )),
                    }),
                ),
                previous(
                    "refused",
                    OrdinarySendOutcomeV4::Refused(RefusalV34::Unidentified(process.clone())),
                ),
                previous(
                    "failed",
                    OrdinarySendOutcomeV4::Failed(SendFailureV34 {
                        logical_delivery_id_option: None,
                        send_failure_reason: SendFailureReasonV34::Refused(RefusalV34::NoneAbove),
                    }),
                ),
            ] {
                engine
                    .assert(sema_engine::Assertion::new(outcomes, row))
                    .expect("outcome persists");
            }
        }
        let descriptors = SendOutcomeTables::descriptors();
        let mut engine = Engine::open(
            EngineOpen::new(&store_path, SchemaVersion::new(1))
                .with_family(&descriptors.counter)
                .with_family(&descriptors.outcomes),
        )
        .expect("ledger store reopens");
        let tables = open_tables(&mut engine).expect("ledger tables upgrade");
        let rows = engine
            .match_records(QueryPlan::all(tables.outcomes))
            .expect("ledger reads")
            .records()
            .iter()
            .map(|stored| {
                (
                    stored.record.send_outcome_id.clone(),
                    stored.record.ordinary_send_outcome.clone(),
                )
            })
            .collect::<std::collections::BTreeMap<_, _>>();
        let evidence = signal_flow::SeatEvidence {
            seat_pane_option: None,
            pane_label_option: None,
            native_session_id_option: None,
            observed_at: 0,
            process,
        };
        assert_eq!(
            rows["accepted"],
            OrdinarySendOutcome::Accepted(signal_flow::SendAcceptance {
                logical_delivery_id: "logical-1".into(),
                delivery_cursor: 1,
                delivery_state: signal_flow::DeliveryState::Failed(
                    signal_flow::Refusal::Unidentified(evidence.clone())
                ),
            })
        );
        assert_eq!(
            rows["refused"],
            OrdinarySendOutcome::Refused(signal_flow::Refusal::Unidentified(evidence))
        );
        assert_eq!(
            rows["failed"],
            OrdinarySendOutcome::Failed(signal_flow::SendFailure {
                logical_delivery_id_option: None,
                send_failure_reason: signal_flow::SendFailureReason::Refused(
                    signal_flow::Refusal::NoneAbove
                ),
            })
        );
    }
}
