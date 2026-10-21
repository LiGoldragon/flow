//! Durable, countable answers to ordinary Send queries.

use super::*;
use meta_signal_flow::{OrdinarySendOutcome, SendOutcomeRecord};
use signal_flow::{Address, Process, Response};

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
        SchemaHash::for_label("flow-nexus-send-outcome-counter-v1")
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
        MemoryVersion::new(1)
    }

    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label("flow-nexus-send-outcome-v1")
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
                FamilyName::new("flow-nexus-send-outcome-counter"),
                SendOutcomeCounter::schema_hash(),
            ),
            outcomes: TableDescriptor::new(
                SEND_OUTCOME_TABLE_NAME,
                FamilyName::new("flow-nexus-send-outcome"),
                StoredSendOutcome::schema_hash(),
            ),
        }
    }

    fn register(
        engine: &mut Engine,
        descriptors: SendOutcomeDescriptors,
    ) -> Result<SendOutcomeTables, StoreError> {
        Ok(SendOutcomeTables {
            counter: engine.register_table(descriptors.counter)?,
            outcomes: engine.register_table(descriptors.outcomes)?,
            write_gate: Mutex::new(()),
        })
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
            Response::Queued => OrdinarySendOutcome::Queued,
            Response::Delivered => OrdinarySendOutcome::Delivered,
            Response::Refused(refusal) => OrdinarySendOutcome::Refused(refusal),
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

#[cfg(test)]
mod tests {
    use super::*;

    fn open_tables(engine: &mut Engine) -> Result<SendOutcomeTables, StoreError> {
        SendOutcomeTables::register(engine, SendOutcomeTables::descriptors())
    }

    #[test]
    fn send_outcome_counter_and_records_survive_reopen() {
        let fixture = tempfile::tempdir().expect("temporary store path");
        let first = SendOutcomeRecord {
            send_outcome_id: "send-outcome-0000000000000001".into(),
            process_option: None,
            first_address_option: None,
            second_address_option: None,
            request_digest: "abc".into(),
            ordinary_send_outcome: OrdinarySendOutcome::Delivered,
        };
        {
            let descriptors = SendOutcomeTables::descriptors();
            let mut engine = Engine::open(
                EngineOpen::new(fixture.path(), SchemaVersion::new(1))
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
                EngineOpen::new(fixture.path(), SchemaVersion::new(1))
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
}
