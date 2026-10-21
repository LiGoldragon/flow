//! Durable logical delivery lifecycle, distinct from individual native
//! hand-over attempts. The event table is append-only; the current table is
//! a materialized view used for sender-filtered reconnect snapshots.

use super::*;
use flow_ethos::{Delivery, Message, QueueEntry};
use sema_engine::AtomicCommit;
use signal_flow::{
    Address, DeliverySnapshot, DeliveryState, Lock, LogicalDeliveryId, Refusal, Response,
    SendAcceptance, SentBy, Unknown_Data,
};

const COUNTER_KEY: &str = "counter";
const COUNTER_TABLE_NAME: TableName = TableName::new("flow_nexus_logical_delivery_counter");
const CURRENT_TABLE_NAME: TableName = TableName::new("flow_nexus_logical_delivery_current");
const EVENT_TABLE_NAME: TableName = TableName::new("flow_nexus_logical_delivery_events");

/// A lock that the caller has authenticated and checked for expiry while
/// holding the serial dispatch gate. The store consumes it with admission.
pub(crate) struct VerifiedDeliveryLock {
    pub(crate) lock: Lock,
    pub(crate) now: i64,
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct LogicalDeliveryCounter {
    next_cursor: i64,
}
impl EngineRecord for LogicalDeliveryCounter {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(COUNTER_KEY)
    }
}
impl MemoryDefinition for LogicalDeliveryCounter {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(1)
    }
    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label("flow-nexus-logical-delivery-counter-v1")
    }
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct CurrentLogicalDelivery {
    pub snapshot: DeliverySnapshot,
}
impl EngineRecord for CurrentLogicalDelivery {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.snapshot.logical_delivery_id.clone())
    }
}
impl MemoryDefinition for CurrentLogicalDelivery {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(1)
    }
    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label("flow-nexus-logical-delivery-current-v1")
    }
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct LogicalDeliveryEvent {
    pub snapshot: DeliverySnapshot,
}
impl EngineRecord for LogicalDeliveryEvent {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(format!("{:020}", self.snapshot.delivery_cursor))
    }
}
impl MemoryDefinition for LogicalDeliveryEvent {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(1)
    }
    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label("flow-nexus-logical-delivery-event-v1")
    }
}

pub(crate) struct LogicalDeliveryTables {
    pub(crate) counter: TableReference<LogicalDeliveryCounter>,
    pub(crate) current: TableReference<CurrentLogicalDelivery>,
    pub(crate) events: TableReference<LogicalDeliveryEvent>,
    pub(crate) write_gate: Mutex<()>,
}
pub(crate) struct LogicalDeliveryDescriptors {
    pub(crate) counter: TableDescriptor<LogicalDeliveryCounter>,
    pub(crate) current: TableDescriptor<CurrentLogicalDelivery>,
    pub(crate) events: TableDescriptor<LogicalDeliveryEvent>,
}
pub(crate) trait DeclaresLogicalDeliveryTables {
    fn descriptors() -> LogicalDeliveryDescriptors;
    fn register(
        engine: &mut Engine,
        descriptors: LogicalDeliveryDescriptors,
    ) -> Result<LogicalDeliveryTables, StoreError>;
}
impl DeclaresLogicalDeliveryTables for LogicalDeliveryTables {
    fn descriptors() -> LogicalDeliveryDescriptors {
        LogicalDeliveryDescriptors {
            counter: TableDescriptor::new(
                COUNTER_TABLE_NAME,
                FamilyName::new("flow-nexus-logical-delivery-counter"),
                LogicalDeliveryCounter::schema_hash(),
            ),
            current: TableDescriptor::new(
                CURRENT_TABLE_NAME,
                FamilyName::new("flow-nexus-logical-delivery-current"),
                CurrentLogicalDelivery::schema_hash(),
            ),
            events: TableDescriptor::new(
                EVENT_TABLE_NAME,
                FamilyName::new("flow-nexus-logical-delivery-event"),
                LogicalDeliveryEvent::schema_hash(),
            ),
        }
    }
    fn register(
        engine: &mut Engine,
        descriptors: LogicalDeliveryDescriptors,
    ) -> Result<LogicalDeliveryTables, StoreError> {
        Ok(LogicalDeliveryTables {
            counter: engine.register_table(descriptors.counter)?,
            current: engine.register_table(descriptors.current)?,
            events: engine.register_table(descriptors.events)?,
            write_gate: Mutex::new(()),
        })
    }
}

/// Mutations are assembled into the caller's AtomicCommit so metaflow queue
/// changes and lifecycle transitions have one durability boundary.
pub(crate) trait RecordsLogicalDeliveries {
    fn seed_missing_queued_deliveries(&self) -> Result<(), StoreError>;
    fn accept_logical_delivery(
        &self,
        address: &Address,
        sent_by: SentBy,
        delivery: Delivery,
        message: Message,
        held_lock: Option<VerifiedDeliveryLock>,
    ) -> Result<Response, StoreError>;
    fn transition_logical_delivery(
        &self,
        id: &LogicalDeliveryId,
        state: DeliveryState,
    ) -> Result<DeliverySnapshot, StoreError>;
    fn commit_logical_transition(
        &self,
        id: &LogicalDeliveryId,
        state: DeliveryState,
        commit: AtomicCommit,
    ) -> Result<DeliverySnapshot, StoreError>;
    fn read_logical_delivery_snapshots(
        &self,
        sender: &Address,
        after_cursor: Option<i64>,
    ) -> Result<(i64, Vec<DeliverySnapshot>), StoreError>;
}
impl RecordsLogicalDeliveries for FlowStore {
    fn seed_missing_queued_deliveries(&self) -> Result<(), StoreError> {
        let _guard = self
            .logical_deliveries
            .write_gate
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let metaflows = self.engine.match_records(QueryPlan::all(self.metaflows))?;
        let current = self
            .engine
            .match_records(QueryPlan::all(self.logical_deliveries.current))?;
        let mut present = current
            .records()
            .iter()
            .map(|row| row.snapshot.logical_delivery_id.clone())
            .collect::<std::collections::HashSet<_>>();
        let (mut cursor, create_counter) = self.next_logical_cursor()?;
        let mut snapshots = Vec::new();
        for metaflow in metaflows.records() {
            for entry in &metaflow.metaflow.queue {
                if !present.insert(entry.logical_delivery_id.clone()) {
                    continue;
                }
                snapshots.push(DeliverySnapshot {
                    delivery_cursor: cursor,
                    logical_delivery_id: entry.logical_delivery_id.clone(),
                    sent_by: entry.sent_by.clone(),
                    address: metaflow.metaflow.address.clone(),
                    delivery_state: DeliveryState::Queued,
                });
                cursor = cursor.checked_add(1).ok_or(StoreError::StateInvariant)?;
            }
        }
        if snapshots.is_empty() {
            return Ok(());
        }
        let next = LogicalDeliveryCounter {
            next_cursor: cursor,
        };
        let mut commit = self.engine.begin_atomic_commit();
        commit = if create_counter {
            commit.assert(self.logical_deliveries.counter, next)
        } else {
            commit.mutate(self.logical_deliveries.counter, next)
        };
        for snapshot in &snapshots {
            commit = commit
                .assert(
                    self.logical_deliveries.current,
                    CurrentLogicalDelivery {
                        snapshot: snapshot.clone(),
                    },
                )
                .assert(
                    self.logical_deliveries.events,
                    LogicalDeliveryEvent {
                        snapshot: snapshot.clone(),
                    },
                );
        }
        self.engine.commit_atomic(commit)?;
        self.launch_changes.announce();
        Ok(())
    }
    fn accept_logical_delivery(
        &self,
        address: &Address,
        sent_by: SentBy,
        delivery: Delivery,
        message: Message,
        held_lock: Option<VerifiedDeliveryLock>,
    ) -> Result<Response, StoreError> {
        let _guard = self
            .logical_deliveries
            .write_gate
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let Some(stored) = self.stored_metaflow(address)? else {
            return Ok(Response::Refused(Refusal::Unknown(Unknown_Data::Address(
                address.clone(),
            ))));
        };
        let lock = if let Some(held) = held_lock {
            let Some(existing) = self.stored_metaflow_lock(address)? else {
                return Ok(Response::Refused(Refusal::Unknown(Unknown_Data::Lock(
                    held.lock,
                ))));
            };
            if existing.lock != held.lock {
                return Ok(Response::Refused(Refusal::Unknown(Unknown_Data::Lock(
                    held.lock,
                ))));
            }
            if held.lock.until <= held.now {
                return Ok(Response::Refused(Refusal::Lapsed));
            }
            Some(held.lock)
        } else {
            None
        };
        let (next, create) = self.next_logical_cursor()?;
        let id = format!("logical-delivery-{next:020}");
        stored.metaflow.queue.push(QueueEntry {
            logical_delivery_id: id.clone(),
            sent_by: sent_by.clone(),
            delivery,
            message,
        });
        let snapshot = DeliverySnapshot {
            delivery_cursor: next,
            logical_delivery_id: id.clone(),
            sent_by,
            address: stored.metaflow.address.clone(),
            delivery_state: DeliveryState::Queued,
        };
        let next_counter = LogicalDeliveryCounter {
            next_cursor: next.checked_add(1).ok_or(StoreError::StateInvariant)?,
        };
        let commit = self
            .engine
            .begin_atomic_commit()
            .mutate(self.metaflows, stored);
        let commit = if create {
            commit.assert(self.logical_deliveries.counter, next_counter)
        } else {
            commit.mutate(self.logical_deliveries.counter, next_counter)
        };
        let commit = commit
            .assert(
                self.logical_deliveries.current,
                CurrentLogicalDelivery {
                    snapshot: snapshot.clone(),
                },
            )
            .assert(
                self.logical_deliveries.events,
                LogicalDeliveryEvent {
                    snapshot: snapshot.clone(),
                },
            );
        let commit = if let Some(lock) = lock {
            commit.retract(
                self.metaflow_locks,
                RecordKey::new(FlowStore::address_record_key(&lock.address)),
            )
        } else {
            commit
        };
        self.engine.commit_atomic(commit)?;
        self.launch_changes.announce();
        Ok(Response::SendAccepted(SendAcceptance {
            logical_delivery_id: id,
            delivery_cursor: next,
            delivery_state: DeliveryState::Queued,
        }))
    }
    fn transition_logical_delivery(
        &self,
        id: &LogicalDeliveryId,
        state: DeliveryState,
    ) -> Result<DeliverySnapshot, StoreError> {
        self.commit_logical_transition(id, state, self.engine.begin_atomic_commit())
    }
    fn commit_logical_transition(
        &self,
        id: &LogicalDeliveryId,
        state: DeliveryState,
        commit: AtomicCommit,
    ) -> Result<DeliverySnapshot, StoreError> {
        let _guard = self
            .logical_deliveries
            .write_gate
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let key = RecordKey::new(id.clone());
        let records = self
            .engine
            .match_records(QueryPlan::key(self.logical_deliveries.current, key))?;
        let [current] = records.records() else {
            return Err(StoreError::StateInvariant);
        };
        if current.snapshot.delivery_state == state {
            if commit.operation_count() == 0 {
                return Ok(current.snapshot.clone());
            }
            self.engine.commit_atomic(commit)?;
            self.launch_changes.announce();
            return Ok(current.snapshot.clone());
        }
        let (next, create) = self.next_logical_cursor()?;
        let mut snapshot = current.snapshot.clone();
        snapshot.delivery_cursor = next;
        snapshot.delivery_state = state;
        let counter = LogicalDeliveryCounter {
            next_cursor: next.checked_add(1).ok_or(StoreError::StateInvariant)?,
        };
        let commit = if create {
            commit.assert(self.logical_deliveries.counter, counter)
        } else {
            commit.mutate(self.logical_deliveries.counter, counter)
        };
        let commit = commit
            .mutate(
                self.logical_deliveries.current,
                CurrentLogicalDelivery {
                    snapshot: snapshot.clone(),
                },
            )
            .assert(
                self.logical_deliveries.events,
                LogicalDeliveryEvent {
                    snapshot: snapshot.clone(),
                },
            );
        self.engine.commit_atomic(commit)?;
        self.launch_changes.announce();
        Ok(snapshot)
    }
    fn read_logical_delivery_snapshots(
        &self,
        sender: &Address,
        after_cursor: Option<i64>,
    ) -> Result<(i64, Vec<DeliverySnapshot>), StoreError> {
        let current = self
            .engine
            .match_records(QueryPlan::all(self.logical_deliveries.current))?;
        let events = self
            .engine
            .match_records(QueryPlan::all(self.logical_deliveries.events))?;
        let highwater = events
            .records()
            .iter()
            .map(|r| r.snapshot.delivery_cursor)
            .max()
            .unwrap_or(0);
        let mut snapshots: Vec<DeliverySnapshot> = if let Some(cursor) = after_cursor {
            events
                .records()
                .iter()
                .filter(|r| {
                    r.snapshot.delivery_cursor > cursor
                        && &r.snapshot.sent_by == &signal_flow::SentBy::Sender(sender.clone())
                })
                .map(|r| r.snapshot.clone())
                .collect()
        } else {
            current
                .records()
                .iter()
                .filter(|r| &r.snapshot.sent_by == &signal_flow::SentBy::Sender(sender.clone()))
                .map(|r| r.snapshot.clone())
                .collect()
        };
        snapshots.sort_by_key(|snapshot| snapshot.delivery_cursor);
        Ok((highwater, snapshots))
    }
}

impl FlowStore {
    fn next_logical_cursor(&self) -> Result<(i64, bool), StoreError> {
        let records = self.engine.match_records(QueryPlan::key(
            self.logical_deliveries.counter,
            RecordKey::new(COUNTER_KEY),
        ))?;
        match records.records() {
            [] => Ok((1, true)),
            [counter] => Ok((counter.next_cursor, false)),
            _ => Err(StoreError::StateInvariant),
        }
    }
}
