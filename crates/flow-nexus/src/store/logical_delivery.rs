//! Durable logical delivery lifecycle, distinct from individual native
//! hand-over attempts. The event table is append-only; the current table is
//! a materialized view used for sender-filtered reconnect snapshots.

use super::send_v31::DeliverySnapshotV31;
use super::send_v32::DeliverySnapshotV32;
use super::*;
use crate::generated::send_tool::{
    Accepted_Data, SendCall, SendOutcome as ToolSendOutcome, SendRefusal as ToolSendRefusal,
    Target, ToolCallKey,
};
use flow_ethos::{Delivery, Message, QueueEntry};
use sema_engine::AtomicCommit;
use signal_flow::{
    Address, DeliverySnapshot, DeliveryState, Lock, LogicalDeliveryId, Refusal, Response,
    SendAcceptance, SendObservation, SentBy, Unknown_Data,
};

const COUNTER_KEY: &str = "counter";
const COUNTER_TABLE_NAME: TableName = TableName::new("flow_nexus_logical_delivery_counter");
const CURRENT_TABLE_NAME: TableName = TableName::new("flow_nexus_logical_delivery_current");
const EVENT_TABLE_NAME: TableName = TableName::new("flow_nexus_logical_delivery_events");
pub(super) const CURRENT_SCHEMA: &str = "flow-nexus-logical-delivery-current-v6";
pub(super) const EVENT_SCHEMA: &str = "flow-nexus-logical-delivery-event-v4";
/// A lock that the caller has authenticated and checked for expiry while
/// holding the serial dispatch gate. The store consumes it with admission.
pub(crate) struct VerifiedDeliveryLock {
    pub(crate) lock: Lock,
    pub(crate) now: i64,
}

struct LogicalCursorAllocation {
    cursor: i64,
    existing: bool,
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
pub(crate) enum LogicalSenderIdentity {
    Sender(meta_signal_flow::Sender),
    /// The row predates admission-time sender identity. Its payload remains
    /// durable, but it is never eligible for native delivery.
    MissingHistoricalSender,
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct CurrentLogicalDelivery {
    pub snapshot: DeliverySnapshot,
    pub sender_identity: LogicalSenderIdentity,
}
impl EngineRecord for CurrentLogicalDelivery {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.snapshot.logical_delivery_id.clone())
    }
}
impl MemoryDefinition for CurrentLogicalDelivery {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(6)
    }
    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label(CURRENT_SCHEMA)
    }
}

/// The batch-2 current row before absence of historical sender identity was
/// a typed, non-runnable disposition.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct CurrentLogicalDeliveryV5 {
    pub snapshot: DeliverySnapshot,
    pub sender_option: Option<meta_signal_flow::Sender>,
}
impl EngineRecord for CurrentLogicalDeliveryV5 {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.snapshot.logical_delivery_id.clone())
    }
}
impl MemoryDefinition for CurrentLogicalDeliveryV5 {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(5)
    }
    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label("flow-nexus-logical-delivery-current-v5")
    }
}
impl UpgradeFrom<CurrentLogicalDeliveryV5> for CurrentLogicalDelivery {
    type Refusal = std::convert::Infallible;
    fn upgrade_from(previous: CurrentLogicalDeliveryV5) -> Decision<Self, Self::Refusal> {
        Decision::Ready(Self {
            snapshot: previous.snapshot,
            sender_identity: previous.sender_option.map_or(
                LogicalSenderIdentity::MissingHistoricalSender,
                LogicalSenderIdentity::Sender,
            ),
        })
    }
}

/// The batch-1 current row, before a queued message durably retained the
/// exact Flow identity resolved at admission.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct CurrentLogicalDeliveryV4 {
    pub snapshot: DeliverySnapshot,
}
impl EngineRecord for CurrentLogicalDeliveryV4 {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.snapshot.logical_delivery_id.clone())
    }
}
impl MemoryDefinition for CurrentLogicalDeliveryV4 {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(4)
    }
    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label("flow-nexus-logical-delivery-current-v4")
    }
}
impl UpgradeFrom<CurrentLogicalDeliveryV4> for CurrentLogicalDelivery {
    type Refusal = std::convert::Infallible;
    fn upgrade_from(previous: CurrentLogicalDeliveryV4) -> Decision<Self, Self::Refusal> {
        Decision::Ready(Self {
            snapshot: previous.snapshot,
            sender_identity: LogicalSenderIdentity::MissingHistoricalSender,
        })
    }
}

/// A delivery's current snapshot as signal-flow 33 archived it, read only
/// by the v3-to-v4 upgrade. Its shape is unchanged by removal of the
/// unrelated `QueueNotPermitted` failure variant.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct CurrentLogicalDeliveryV3 {
    pub snapshot: DeliverySnapshot,
}
impl EngineRecord for CurrentLogicalDeliveryV3 {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.snapshot.logical_delivery_id.clone())
    }
}
impl MemoryDefinition for CurrentLogicalDeliveryV3 {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(3)
    }
    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label("flow-nexus-logical-delivery-current-v3")
    }
}
impl UpgradeFrom<CurrentLogicalDeliveryV3> for CurrentLogicalDelivery {
    type Refusal = std::convert::Infallible;
    fn upgrade_from(previous: CurrentLogicalDeliveryV3) -> Decision<Self, Self::Refusal> {
        Decision::Ready(Self {
            snapshot: previous.snapshot,
            sender_identity: LogicalSenderIdentity::MissingHistoricalSender,
        })
    }
}

/// A delivery's current snapshot as signal-flow 32 archived it, read only
/// by the v2-to-v3 upgrade.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct CurrentLogicalDeliveryV2 {
    pub snapshot: DeliverySnapshotV32,
}
impl EngineRecord for CurrentLogicalDeliveryV2 {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.snapshot.logical_delivery_id.clone())
    }
}
impl MemoryDefinition for CurrentLogicalDeliveryV2 {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(2)
    }
    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label("flow-nexus-logical-delivery-current-v2")
    }
}
impl UpgradeFrom<CurrentLogicalDeliveryV2> for CurrentLogicalDelivery {
    type Refusal = std::convert::Infallible;
    fn upgrade_from(previous: CurrentLogicalDeliveryV2) -> Decision<Self, Self::Refusal> {
        Decision::Ready(Self {
            snapshot: previous.snapshot.into(),
            sender_identity: LogicalSenderIdentity::MissingHistoricalSender,
        })
    }
}

/// A delivery's current snapshot as signal-flow 31 archived it, read only
/// by the v1-to-v3 upgrade.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct CurrentLogicalDeliveryV1 {
    pub snapshot: DeliverySnapshotV31,
}
impl EngineRecord for CurrentLogicalDeliveryV1 {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.snapshot.logical_delivery_id.clone())
    }
}
impl MemoryDefinition for CurrentLogicalDeliveryV1 {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(1)
    }
    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label("flow-nexus-logical-delivery-current-v1")
    }
}
impl UpgradeFrom<CurrentLogicalDeliveryV1> for CurrentLogicalDelivery {
    type Refusal = std::convert::Infallible;
    fn upgrade_from(previous: CurrentLogicalDeliveryV1) -> Decision<Self, Self::Refusal> {
        Decision::Ready(Self {
            snapshot: previous.snapshot.into(),
            sender_identity: LogicalSenderIdentity::MissingHistoricalSender,
        })
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
        MemoryVersion::new(4)
    }
    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label(EVENT_SCHEMA)
    }
}

/// A delivery event as signal-flow 33 archived it, read only by the
/// v3-to-v4 upgrade. The snapshot itself is unchanged.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct LogicalDeliveryEventV3 {
    pub snapshot: DeliverySnapshot,
}
impl EngineRecord for LogicalDeliveryEventV3 {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(format!("{:020}", self.snapshot.delivery_cursor))
    }
}
impl MemoryDefinition for LogicalDeliveryEventV3 {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(3)
    }
    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label("flow-nexus-logical-delivery-event-v3")
    }
}
impl UpgradeFrom<LogicalDeliveryEventV3> for LogicalDeliveryEvent {
    type Refusal = std::convert::Infallible;
    fn upgrade_from(previous: LogicalDeliveryEventV3) -> Decision<Self, Self::Refusal> {
        Decision::Ready(Self {
            snapshot: previous.snapshot,
        })
    }
}

/// A delivery's lifecycle event as signal-flow 32 archived it, read only by
/// the v2-to-v3 upgrade.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct LogicalDeliveryEventV2 {
    pub snapshot: DeliverySnapshotV32,
}
impl EngineRecord for LogicalDeliveryEventV2 {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(format!("{:020}", self.snapshot.delivery_cursor))
    }
}
impl MemoryDefinition for LogicalDeliveryEventV2 {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(2)
    }
    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label("flow-nexus-logical-delivery-event-v2")
    }
}
impl UpgradeFrom<LogicalDeliveryEventV2> for LogicalDeliveryEvent {
    type Refusal = std::convert::Infallible;
    fn upgrade_from(previous: LogicalDeliveryEventV2) -> Decision<Self, Self::Refusal> {
        Decision::Ready(Self {
            snapshot: previous.snapshot.into(),
        })
    }
}

/// A delivery's lifecycle event as signal-flow 31 archived it, read only by
/// the v1-to-v3 upgrade.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct LogicalDeliveryEventV1 {
    pub snapshot: DeliverySnapshotV31,
}
impl EngineRecord for LogicalDeliveryEventV1 {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(format!("{:020}", self.snapshot.delivery_cursor))
    }
}
impl MemoryDefinition for LogicalDeliveryEventV1 {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(1)
    }
    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label("flow-nexus-logical-delivery-event-v1")
    }
}
impl UpgradeFrom<LogicalDeliveryEventV1> for LogicalDeliveryEvent {
    type Refusal = std::convert::Infallible;
    fn upgrade_from(previous: LogicalDeliveryEventV1) -> Decision<Self, Self::Refusal> {
        Decision::Ready(Self {
            snapshot: previous.snapshot.into(),
        })
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
            )
            .with_predecessor::<CurrentLogicalDeliveryV1>()
            .with_predecessor::<CurrentLogicalDeliveryV2>()
            .with_predecessor::<CurrentLogicalDeliveryV3>()
            .with_predecessor::<CurrentLogicalDeliveryV4>()
            .with_predecessor::<CurrentLogicalDeliveryV5>(),
            events: TableDescriptor::new(
                EVENT_TABLE_NAME,
                FamilyName::new("flow-nexus-logical-delivery-event"),
                LogicalDeliveryEvent::schema_hash(),
            )
            .with_predecessor::<LogicalDeliveryEventV1>()
            .with_predecessor::<LogicalDeliveryEventV2>()
            .with_predecessor::<LogicalDeliveryEventV3>(),
        }
    }
    fn register(
        engine: &mut Engine,
        descriptors: LogicalDeliveryDescriptors,
    ) -> Result<LogicalDeliveryTables, StoreError> {
        Ok(LogicalDeliveryTables {
            counter: engine.register_table(descriptors.counter)?,
            // Try the newest predecessor first, then each older family.
            current:
                match engine.upgrade_table::<CurrentLogicalDeliveryV5, CurrentLogicalDelivery>(
                    descriptors.current.clone(),
                ) {
                    UpgradeOutcome::Upgraded(receipt) => receipt.table(),
                    UpgradeOutcome::AlreadyCurrent(table) => table,
                    UpgradeOutcome::Refused(refusal) => match *refusal.reason() {},
                    UpgradeOutcome::Failed(failure)
                        if failure.phase() == sema_engine::StoragePhase::Admission =>
                    {
                        match engine.upgrade_table::<CurrentLogicalDeliveryV4, CurrentLogicalDelivery>(descriptors.current.clone()) {
            UpgradeOutcome::Upgraded(receipt) => receipt.table(),
            UpgradeOutcome::AlreadyCurrent(table) => table,
            UpgradeOutcome::Refused(refusal) => match *refusal.reason() {},
            UpgradeOutcome::Failed(failure)
                if failure.phase() == sema_engine::StoragePhase::Admission =>
            {
                match engine.upgrade_table::<CurrentLogicalDeliveryV3, CurrentLogicalDelivery>(descriptors.current.clone()) {
                    UpgradeOutcome::Upgraded(receipt) => receipt.table(),
                    UpgradeOutcome::AlreadyCurrent(table) => table,
                    UpgradeOutcome::Refused(refusal) => match *refusal.reason() {},
                    UpgradeOutcome::Failed(failure)
                        if failure.phase() == sema_engine::StoragePhase::Admission =>
                    {
                        match engine.upgrade_table::<CurrentLogicalDeliveryV2, CurrentLogicalDelivery>(descriptors.current.clone()) {
                            UpgradeOutcome::Upgraded(receipt) => receipt.table(),
                            UpgradeOutcome::AlreadyCurrent(table) => table,
                            UpgradeOutcome::Refused(refusal) => match *refusal.reason() {},
                            UpgradeOutcome::Failed(failure)
                                if failure.phase() == sema_engine::StoragePhase::Admission =>
                            {
                                match engine.upgrade_table::<CurrentLogicalDeliveryV1, CurrentLogicalDelivery>(descriptors.current) {
                                    UpgradeOutcome::Upgraded(receipt) => receipt.table(),
                                    UpgradeOutcome::AlreadyCurrent(table) => table,
                                    UpgradeOutcome::Refused(refusal) => match *refusal.reason() {},
                                    UpgradeOutcome::Failed(failure) => {
                                        return Err(StoreError::LogicalDeliveryUpgrade(format!(
                                            "current deliveries: {failure:?}"
                                        )));
                                    }
                                }
                            }
                            UpgradeOutcome::Failed(failure) => {
                                return Err(StoreError::LogicalDeliveryUpgrade(format!(
                                    "current deliveries: {failure:?}"
                                )));
                            }
                        }
                    }
                    UpgradeOutcome::Failed(failure) => {
                        return Err(StoreError::LogicalDeliveryUpgrade(format!(
                            "current deliveries: {failure:?}"
                        )));
                    }
                }
            }
            UpgradeOutcome::Failed(failure) => {
                return Err(StoreError::LogicalDeliveryUpgrade(format!(
                    "current deliveries: {failure:?}"
                )));
            }
        }
                    }
                    UpgradeOutcome::Failed(failure) => {
                        return Err(StoreError::LogicalDeliveryUpgrade(format!(
                            "current deliveries: {failure:?}"
                        )));
                    }
                },
            events: match engine.upgrade_table::<LogicalDeliveryEventV3, LogicalDeliveryEvent>(
                descriptors.events.clone(),
            ) {
                UpgradeOutcome::Upgraded(receipt) => receipt.table(),
                UpgradeOutcome::AlreadyCurrent(table) => table,
                UpgradeOutcome::Refused(refusal) => match *refusal.reason() {},
                UpgradeOutcome::Failed(failure)
                    if failure.phase() == sema_engine::StoragePhase::Admission =>
                {
                    match engine.upgrade_table::<LogicalDeliveryEventV2, LogicalDeliveryEvent>(
                        descriptors.events.clone(),
                    ) {
                        UpgradeOutcome::Upgraded(receipt) => receipt.table(),
                        UpgradeOutcome::AlreadyCurrent(table) => table,
                        UpgradeOutcome::Refused(refusal) => match *refusal.reason() {},
                        UpgradeOutcome::Failed(failure)
                            if failure.phase() == sema_engine::StoragePhase::Admission =>
                        {
                            match engine
                                .upgrade_table::<LogicalDeliveryEventV1, LogicalDeliveryEvent>(
                                    descriptors.events,
                                ) {
                                UpgradeOutcome::Upgraded(receipt) => receipt.table(),
                                UpgradeOutcome::AlreadyCurrent(table) => table,
                                UpgradeOutcome::Refused(refusal) => match *refusal.reason() {},
                                UpgradeOutcome::Failed(failure) => {
                                    return Err(StoreError::LogicalDeliveryUpgrade(format!(
                                        "delivery events: {failure:?}"
                                    )));
                                }
                            }
                        }
                        UpgradeOutcome::Failed(failure) => {
                            return Err(StoreError::LogicalDeliveryUpgrade(format!(
                                "delivery events: {failure:?}"
                            )));
                        }
                    }
                }
                UpgradeOutcome::Failed(failure) => {
                    return Err(StoreError::LogicalDeliveryUpgrade(format!(
                        "delivery events: {failure:?}"
                    )));
                }
            },
            write_gate: Mutex::new(()),
        })
    }
}

/// Mutations are assembled into the caller's AtomicCommit so metaflow queue
/// changes and lifecycle transitions have one durability boundary.
pub(crate) trait RecordsLogicalDeliveries {
    fn seed_missing_queued_deliveries(&self) -> Result<(), StoreError>;
    fn force_queue_wake(&self, address: &Address) -> Result<(), StoreError>;
    #[expect(
        clippy::too_many_arguments,
        reason = "Preserve the existing atomic tool-admission boundary during the delivery repair; its inputs remain individually typed."
    )]
    fn accept_tool_send(
        &self,
        key: &ToolCallKey,
        content_sha256: &str,
        sender: meta_signal_flow::Sender,
        target: &Target,
        delivery: Delivery,
        message: Message,
        now: i64,
    ) -> Result<ToolSendOutcome, StoreError>;
    fn accept_logical_delivery(
        &self,
        address: &Address,
        sender: meta_signal_flow::Sender,
        delivery: Delivery,
        message: Message,
        held_lock: Option<VerifiedDeliveryLock>,
    ) -> Result<Response, StoreError>;
    #[cfg(test)]
    fn strip_logical_sender_for_test(&self, id: &str) -> Result<(), StoreError>;
    #[cfg(test)]
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
    fn logical_delivery_snapshot(
        &self,
        id: &LogicalDeliveryId,
    ) -> Result<Option<DeliverySnapshot>, StoreError>;
    fn logical_delivery_sender(
        &self,
        id: &LogicalDeliveryId,
    ) -> Result<Option<meta_signal_flow::Sender>, StoreError>;
    #[cfg(test)]
    fn seed_logical_delivery_sender(
        &self,
        id: &LogicalDeliveryId,
        sender: meta_signal_flow::Sender,
    ) -> Result<(), StoreError>;
    /// Marks each request of `queue` that waits, Queued or Retryable,
    /// `Retryable(cause)`, so its sender sees why it waits; the queue itself
    /// is untouched. Answers how many changed.
    fn mark_queue_retryable(
        &self,
        queue: &[QueueEntry],
        cause: signal_flow::RetryCause,
    ) -> Result<usize, StoreError>;
    fn read_logical_delivery_snapshots(
        &self,
        sender: &Address,
        after_cursor: Option<i64>,
    ) -> Result<SendObservation, StoreError>;
}
impl RecordsLogicalDeliveries for FlowStore {
    fn accept_tool_send(
        &self,
        key: &ToolCallKey,
        content_sha256: &str,
        sender: meta_signal_flow::Sender,
        target: &Target,
        delivery: Delivery,
        message: Message,
        now: i64,
    ) -> Result<ToolSendOutcome, StoreError> {
        use crate::store::send_call::KeysToolCall;
        let _guard = self
            .logical_deliveries
            .write_gate
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let call_key = RecordKey::new(key.record_key_text());
        let prior = self
            .engine
            .match_records(QueryPlan::key(self.send_calls, call_key))?;
        match prior.records() {
            [record] if record.content_sha256 == content_sha256 => {
                return Ok(record.send_outcome.clone());
            }
            [_] => return Ok(ToolSendOutcome::Refused(ToolSendRefusal::KeyReused)),
            [] => {}
            _ => return Err(StoreError::StateInvariant),
        }
        let record = |send_outcome| SendCall {
            tool_call_key: key.clone(),
            content_sha256: content_sha256.to_owned(),
            send_outcome,
        };
        let Some(mut stored) = self.stored_metaflow(target)? else {
            let outcome =
                ToolSendOutcome::Refused(ToolSendRefusal::UnknownRecipient(target.clone()));
            self.engine
                .assert(Assertion::new(self.send_calls, record(outcome.clone())))?;
            return Ok(outcome);
        };
        if self
            .stored_metaflow_lock(target)?
            .is_some_and(|held| held.lock.until > now)
        {
            let outcome = ToolSendOutcome::Refused(ToolSendRefusal::Held);
            self.engine
                .assert(Assertion::new(self.send_calls, record(outcome.clone())))?;
            return Ok(outcome);
        }
        let allocation = self.allocate_logical_cursor()?;
        let cursor = allocation.cursor;
        let create_counter = !allocation.existing;
        let id = format!("logical-delivery-{cursor:020}");
        stored.metaflow.queue.push(QueueEntry {
            logical_delivery_id: id.clone(),
            sent_by: SentBy::Sender(sender.address.clone()),
            delivery,
            message,
        });
        let snapshot = DeliverySnapshot {
            delivery_cursor: cursor,
            logical_delivery_id: id.clone(),
            sent_by: SentBy::Sender(sender.address.clone()),
            address: stored.metaflow.address.clone(),
            delivery_state: DeliveryState::Queued,
        };
        let acceptance = SendAcceptance {
            logical_delivery_id: id,
            delivery_cursor: cursor,
            delivery_state: DeliveryState::Queued,
        };
        let outcome = ToolSendOutcome::Accepted(Accepted_Data {
            sender: sender.address.clone(),
            target: target.clone(),
            send_acceptance: acceptance,
        });
        let next_counter = LogicalDeliveryCounter {
            next_cursor: cursor.checked_add(1).ok_or(StoreError::StateInvariant)?,
        };
        let commit = self
            .engine
            .begin_atomic_commit()
            .mutate(self.metaflows, stored);
        let commit = if create_counter {
            commit.assert(self.logical_deliveries.counter, next_counter)
        } else {
            commit.mutate(self.logical_deliveries.counter, next_counter)
        };
        self.engine.commit_atomic(
            commit
                .assert(
                    self.logical_deliveries.current,
                    CurrentLogicalDelivery {
                        snapshot: snapshot.clone(),
                        sender_identity: LogicalSenderIdentity::Sender(sender),
                    },
                )
                .assert(
                    self.logical_deliveries.events,
                    LogicalDeliveryEvent { snapshot },
                )
                .assert(self.send_calls, record(outcome.clone())),
        )?;
        self.launch_changes.announce();
        Ok(outcome)
    }
    fn force_queue_wake(&self, address: &Address) -> Result<(), StoreError> {
        let _guard = self
            .logical_deliveries
            .write_gate
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let Some(mut stored) = self.stored_metaflow(address)? else {
            return Err(StoreError::StateInvariant);
        };
        let mut changed = false;
        for entry in &mut stored.metaflow.queue {
            if entry.delivery != Delivery::Wake {
                entry.delivery = Delivery::Wake;
                changed = true;
            }
        }
        if changed {
            self.engine.commit_atomic(
                self.engine
                    .begin_atomic_commit()
                    .mutate(self.metaflows, stored),
            )?;
            self.launch_changes.announce();
        }
        Ok(())
    }
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
        let allocation = self.allocate_logical_cursor()?;
        let mut cursor = allocation.cursor;
        let create_counter = !allocation.existing;
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
                        sender_identity: LogicalSenderIdentity::MissingHistoricalSender,
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
        sender: meta_signal_flow::Sender,
        delivery: Delivery,
        message: Message,
        held_lock: Option<VerifiedDeliveryLock>,
    ) -> Result<Response, StoreError> {
        let _guard = self
            .logical_deliveries
            .write_gate
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let Some(mut stored) = self.stored_metaflow(address)? else {
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
        let allocation = self.allocate_logical_cursor()?;
        let next = allocation.cursor;
        let create = !allocation.existing;
        let id = format!("logical-delivery-{next:020}");
        let sent_by = SentBy::Sender(sender.address.clone());
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
                    sender_identity: LogicalSenderIdentity::Sender(sender),
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
    #[cfg(test)]
    fn strip_logical_sender_for_test(&self, id: &str) -> Result<(), StoreError> {
        let rows = self.engine.match_records(QueryPlan::key(
            self.logical_deliveries.current,
            RecordKey::new(id),
        ))?;
        let [current] = rows.records() else {
            return Err(StoreError::StateInvariant);
        };
        let mut historical = current.clone();
        historical.sender_option = None;
        self.engine.mutate(sema_engine::Mutation::new(
            self.logical_deliveries.current,
            historical,
        ))?;
        Ok(())
    }
    #[cfg(test)]
    fn transition_logical_delivery(
        &self,
        id: &LogicalDeliveryId,
        state: DeliveryState,
    ) -> Result<DeliverySnapshot, StoreError> {
        self.commit_logical_transition(id, state, self.engine.begin_atomic_commit())
    }
    fn mark_queue_retryable(
        &self,
        queue: &[QueueEntry],
        cause: signal_flow::RetryCause,
    ) -> Result<usize, StoreError> {
        let mut changed = 0;
        for entry in queue {
            let Some(snapshot) = self.logical_delivery_snapshot(&entry.logical_delivery_id)? else {
                continue;
            };
            let retryable = DeliveryState::Retryable(cause.clone());
            if snapshot.delivery_state != retryable
                && matches!(
                    snapshot.delivery_state,
                    DeliveryState::Queued | DeliveryState::Retryable(_)
                )
            {
                self.commit_logical_transition(
                    &entry.logical_delivery_id,
                    retryable,
                    self.engine.begin_atomic_commit(),
                )?;
                changed += 1;
            }
        }
        Ok(changed)
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
        let allocation = self.allocate_logical_cursor()?;
        let next = allocation.cursor;
        let create = !allocation.existing;
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
                    sender_identity: current.sender_identity.clone(),
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
    fn logical_delivery_snapshot(
        &self,
        id: &LogicalDeliveryId,
    ) -> Result<Option<DeliverySnapshot>, StoreError> {
        let rows = self.engine.match_records(QueryPlan::key(
            self.logical_deliveries.current,
            RecordKey::new(id.clone()),
        ))?;
        match rows.records() {
            [] => Ok(None),
            [current] => Ok(Some(current.snapshot.clone())),
            _ => Err(StoreError::StateInvariant),
        }
    }
    fn logical_delivery_sender(
        &self,
        id: &LogicalDeliveryId,
    ) -> Result<Option<meta_signal_flow::Sender>, StoreError> {
        let rows = self.engine.match_records(QueryPlan::key(
            self.logical_deliveries.current,
            RecordKey::new(id.clone()),
        ))?;
        match rows.records() {
            [] => Ok(None),
            [current] => Ok(match &current.sender_identity {
                LogicalSenderIdentity::Sender(sender) => Some(sender.clone()),
                LogicalSenderIdentity::MissingHistoricalSender => None,
            }),
            _ => Err(StoreError::StateInvariant),
        }
    }
    #[cfg(test)]
    fn seed_logical_delivery_sender(
        &self,
        id: &LogicalDeliveryId,
        sender: meta_signal_flow::Sender,
    ) -> Result<(), StoreError> {
        let rows = self.engine.match_records(QueryPlan::key(
            self.logical_deliveries.current,
            RecordKey::new(id.clone()),
        ))?;
        let [current] = rows.records() else {
            return Err(StoreError::StateInvariant);
        };
        self.engine
            .commit_atomic(self.engine.begin_atomic_commit().mutate(
                self.logical_deliveries.current,
                CurrentLogicalDelivery {
                    snapshot: current.snapshot.clone(),
                    sender_identity: LogicalSenderIdentity::Sender(sender),
                },
            ))?;
        Ok(())
    }
    fn read_logical_delivery_snapshots(
        &self,
        sender: &Address,
        after_cursor: Option<i64>,
    ) -> Result<SendObservation, StoreError> {
        let _guard = self
            .logical_deliveries
            .write_gate
            .lock()
            .unwrap_or_else(|e| e.into_inner());
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
                        && r.snapshot.sent_by == signal_flow::SentBy::Sender(sender.clone())
                })
                .map(|r| r.snapshot.clone())
                .collect()
        } else {
            current
                .records()
                .iter()
                .filter(|r| r.snapshot.sent_by == signal_flow::SentBy::Sender(sender.clone()))
                .map(|r| r.snapshot.clone())
                .collect()
        };
        // Archive is a separate, typed observation, not an active queue.
        // Filter both reconnect snapshots and cursor replay; keep historical
        // events and the global high-water cursor intact, including uncertainty.
        let archived = self.quarantined_request_ids()?;
        snapshots.retain(|snapshot| !archived.contains(&snapshot.logical_delivery_id));
        snapshots.sort_by_key(|snapshot| snapshot.delivery_cursor);
        Ok(SendObservation {
            delivery_cursor: highwater,
            delivery_snapshot_vector: snapshots,
        })
    }
}

trait AllocatesLogicalCursor {
    fn allocate_logical_cursor(&self) -> Result<LogicalCursorAllocation, StoreError>;
}

impl AllocatesLogicalCursor for FlowStore {
    fn allocate_logical_cursor(&self) -> Result<LogicalCursorAllocation, StoreError> {
        let records = self.engine.match_records(QueryPlan::key(
            self.logical_deliveries.counter,
            RecordKey::new(COUNTER_KEY),
        ))?;
        match records.records() {
            [] => Ok(LogicalCursorAllocation {
                cursor: 1,
                existing: false,
            }),
            [counter] => Ok(LogicalCursorAllocation {
                cursor: counter.next_cursor,
                existing: true,
            }),
            _ => Err(StoreError::StateInvariant),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use flow_ethos::{Aspect, Layer};

    fn snapshot() -> DeliverySnapshot {
        let address = Address {
            aspect: Aspect::Mind,
            topic: "historicalSender".into(),
            layer: Layer::Secondary,
        };
        DeliverySnapshot {
            delivery_cursor: 7,
            logical_delivery_id: "logical-historical".into(),
            sent_by: SentBy::Sender(address.clone()),
            address,
            delivery_state: DeliveryState::Queued,
        }
    }

    #[test]
    fn v5_sender_presence_upgrades_to_an_explicit_identity_disposition() {
        let sender = meta_signal_flow::Sender {
            address: Address {
                aspect: Aspect::Psyche,
                topic: "core".into(),
                layer: Layer::Secondary,
            },
            flow_id: "sender-flow".into(),
        };
        let Decision::Ready(known) =
            CurrentLogicalDelivery::upgrade_from(CurrentLogicalDeliveryV5 {
                snapshot: snapshot(),
                sender_option: Some(sender.clone()),
            });
        assert_eq!(known.sender_identity, LogicalSenderIdentity::Sender(sender));
        let Decision::Ready(missing) =
            CurrentLogicalDelivery::upgrade_from(CurrentLogicalDeliveryV5 {
                snapshot: snapshot(),
                sender_option: None,
            });
        assert_eq!(
            missing.sender_identity,
            LogicalSenderIdentity::MissingHistoricalSender
        );
    }
}
