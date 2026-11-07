//! Durable logical delivery lifecycle, distinct from individual native
//! hand-over attempts. The event table is append-only; the current table is
//! a materialized view used for sender-filtered reconnect snapshots.

use super::ManagesMetaflowState;
use super::handover::HoldsHandovers;
use super::handover_reminders::SchedulesHandoverReminders;
use super::metaflow::SleepsMetaflows;
use super::queue_delivery::AccountsQueueDelivery;
use super::reminder::RecordsReminders;
use super::*;
use crate::generated::send_tool::{
    Accepted_Data, SendCall, SendOutcome as ToolSendOutcome, SendRefusal as ToolSendRefusal,
    Target, ToolCallKey,
};
use datom_codec::Datomizable;
use flow_ethos::{Delivery, Message, QueueEntry};
use protos::{Compactable, Protosizable};
use sema_engine::AtomicCommit;
use signal_flow::{
    Address, DeliverySnapshot, DeliveryState, Lock, LogicalDeliveryId, NativeReceipt, Refusal,
    Request, SendAcceptance, SendObservation, SentBy, Unknown_Data,
};

const COUNTER_KEY: &str = "counter";
const COUNTER_TABLE_NAME: TableName = TableName::new("flow_nexus_logical_delivery_counter");
const CURRENT_TABLE_NAME: TableName = TableName::new("flow_nexus_logical_delivery_current");
const EVENT_TABLE_NAME: TableName = TableName::new("flow_nexus_logical_delivery_events");
pub(super) const CURRENT_SCHEMA: &str = "flow-nexus-logical-delivery-current-v8";
pub(super) const EVENT_SCHEMA: &str = "flow-nexus-logical-delivery-event-v7";
const SIMPLIFIED_REPLY_TABLE_NAME: TableName =
    TableName::new("flow_nexus_simplified_reply_bindings");
const PURPOSE_TABLE_NAME: TableName = TableName::new("flow_nexus_logical_delivery_purposes");
const REQUEST_ID_TABLE_NAME: TableName = TableName::new("flow_nexus_request_ids");
const DELIVER_CALL_TABLE_NAME: TableName = TableName::new("flow_nexus_deliver_calls");

/// The entropy boundary for request ids.  Production draws from the operating
/// system; tests can replace it with a deterministic sequence without changing
/// admission or collision handling.
pub(crate) trait DrawsRequestId: Send + Sync {
    fn draw_request_id(&self) -> Result<flow_ethos::RequestId, StoreError>;
}

pub(crate) struct SystemRequestIdDrawer;

impl DrawsRequestId for SystemRequestIdDrawer {
    fn draw_request_id(&self) -> Result<flow_ethos::RequestId, StoreError> {
        const ALPHABET: &[u8; 36] = b"0123456789abcdefghijklmnopqrstuvwxyz";
        // 252 is the greatest multiple of 36 representable by one byte.  The
        // rejection keeps every base36 character equiprobable.
        let mut id = String::with_capacity(4);
        while id.len() < 4 {
            let mut byte = [0_u8; 1];
            getrandom::fill(&mut byte).map_err(|_| StoreError::StateInvariant)?;
            if byte[0] < 252 {
                id.push(ALPHABET[(byte[0] % 36) as usize] as char);
            }
        }
        Ok(id)
    }
}
const STORE_IDENTITY_TABLE_NAME: TableName = TableName::new("flow_nexus_store_identity");
const STORE_IDENTITY_KEY: &str = "store";

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct StoredStoreIdentity {
    pub store_identity: signal_flow::StoreIdentity,
}
impl EngineRecord for StoredStoreIdentity {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(STORE_IDENTITY_KEY)
    }
}
impl MemoryDefinition for StoredStoreIdentity {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(1)
    }
    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label("flow-nexus-store-identity-v1")
    }
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) enum RequestClosure {
    Open,
    Answered(LogicalDeliveryId),
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct RequestIdBinding {
    pub request_id: flow_ethos::RequestId,
    pub logical_delivery_id: LogicalDeliveryId,
    pub closure: RequestClosure,
}
impl EngineRecord for RequestIdBinding {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.request_id.clone())
    }
}
impl MemoryDefinition for RequestIdBinding {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(1)
    }
    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label("flow-nexus-request-id-v1")
    }
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct DeliverCall {
    pub key: signal_flow::DeliverKey,
    pub sender: Address,
    pub digest: String,
    pub answer: SendAcceptance,
}
trait KeysDeliverCalls {
    fn deliver_call_record_key(&self) -> RecordKey;
}
impl KeysDeliverCalls for signal_flow::DeliverKey {
    fn deliver_call_record_key(&self) -> RecordKey {
        // The engine key is the canonical Datom projection of the typed key;
        // one codec owns its representation and both fields remain distinct.
        RecordKey::new(self.datomize(Vec::new()).protosize().compact())
    }
}
impl EngineRecord for DeliverCall {
    fn record_key(&self) -> RecordKey {
        self.key.deliver_call_record_key()
    }
}
impl MemoryDefinition for DeliverCall {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(1)
    }
    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label("flow-nexus-deliver-call-v1")
    }
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct LogicalDeliveryPurpose {
    pub logical_delivery_id: String,
    pub address: Address,
    pub purpose: crate::heartbeat::DeliveryPurpose,
}
impl EngineRecord for LogicalDeliveryPurpose {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.logical_delivery_id.clone())
    }
}

impl MemoryDefinition for LogicalDeliveryPurpose {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(2)
    }
    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label("flow-nexus-logical-delivery-purpose-v2")
    }
}

pub(crate) trait CorePresentationRoute {
    fn is_core_presentation(&self) -> bool;
}

impl CorePresentationRoute for Address {
    fn is_core_presentation(&self) -> bool {
        self.aspect == signal_flow::Aspect::Psyche
            && self.topic == "core"
            && self.model == signal_flow::Model::Fable
    }
}

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
pub(crate) enum DeliveryAdmission {
    Message,
    Ordinary,
    HistoricalUnknown,
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct CurrentLogicalDelivery {
    pub snapshot: DeliverySnapshot,
    pub archived: Option<signal_flow::DeliveryCursor>,
    pub admission: DeliveryAdmission,
}
impl EngineRecord for CurrentLogicalDelivery {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.snapshot.logical_delivery_id.clone())
    }
}

impl MemoryDefinition for CurrentLogicalDelivery {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(8)
    }
    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label(CURRENT_SCHEMA)
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
        MemoryVersion::new(7)
    }
    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label(EVENT_SCHEMA)
    }
}

/// The one request whose wake is being judged by a simplified flow, and the
/// exact sender to receive that judgment. It is keyed by the woken FlowId,
/// never by a mutable metaflow-wide last sender.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct SimplifiedReplyBinding {
    pub flow_id: String,
    pub input_delivery_id: String,
    pub source: Address,
    pub recipient: Address,
    pub completed: bool,
}
/// What a simplified flow's reply binding owes now.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SimplifiedReplyStanding {
    /// No binding, or its answer already queued to its recipient.
    NoneOwed,
    /// The bound input is not yet Delivered: no native receipt anchors an
    /// answer.
    Unreceived(SimplifiedReplyBinding),
    /// The bound input is Delivered under this receipt; its answer is owed.
    Received(SimplifiedReplyBinding, NativeReceipt),
}
impl EngineRecord for SimplifiedReplyBinding {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.flow_id.clone())
    }
}

impl MemoryDefinition for SimplifiedReplyBinding {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(2)
    }
    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label("flow-nexus-simplified-reply-binding-v2")
    }
}

pub(crate) struct LogicalDeliveryTables {
    pub(crate) counter: TableReference<LogicalDeliveryCounter>,
    pub(crate) current: TableReference<CurrentLogicalDelivery>,
    pub(crate) events: TableReference<LogicalDeliveryEvent>,
    pub(crate) simplified_replies: TableReference<SimplifiedReplyBinding>,
    pub(crate) purposes: TableReference<LogicalDeliveryPurpose>,
    pub(crate) request_ids: TableReference<RequestIdBinding>,
    pub(crate) deliver_calls: TableReference<DeliverCall>,
    pub(crate) store_identity: TableReference<StoredStoreIdentity>,
    pub(crate) write_gate: Mutex<()>,
    pending_answers: std::sync::Arc<PendingAdmissionAnswers>,
}

#[derive(Default)]
struct PendingAdmissionAnswers {
    request_ids: Mutex<std::collections::HashMap<flow_ethos::RequestId, usize>>,
}

/// One admitted call whose answer has not finished writing to its caller.
/// The hold is process-local by design: after a restart no call is still
/// being answered by the old process.
pub(crate) struct PendingAnswerHold {
    pending: std::sync::Arc<PendingAdmissionAnswers>,
    request_id: flow_ethos::RequestId,
}

impl Drop for PendingAnswerHold {
    fn drop(&mut self) {
        let mut held = self
            .pending
            .request_ids
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let Some(count) = held.get_mut(&self.request_id) else {
            return;
        };
        if *count == 1 {
            held.remove(&self.request_id);
        } else {
            *count -= 1;
        }
    }
}
pub(crate) struct LogicalDeliveryDescriptors {
    pub(crate) counter: TableDescriptor<LogicalDeliveryCounter>,
    pub(crate) current: TableDescriptor<CurrentLogicalDelivery>,
    pub(crate) events: TableDescriptor<LogicalDeliveryEvent>,
    pub(crate) simplified_replies: TableDescriptor<SimplifiedReplyBinding>,
    pub(crate) purposes: TableDescriptor<LogicalDeliveryPurpose>,
    pub(crate) request_ids: TableDescriptor<RequestIdBinding>,
    pub(crate) deliver_calls: TableDescriptor<DeliverCall>,
    pub(crate) store_identity: TableDescriptor<StoredStoreIdentity>,
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
            .with_predecessor::<super::request_id_upgrade::PriorCurrentLogicalDelivery>(),
            events: TableDescriptor::new(
                EVENT_TABLE_NAME,
                FamilyName::new("flow-nexus-logical-delivery-event"),
                LogicalDeliveryEvent::schema_hash(),
            )
            .with_predecessor::<super::request_id_upgrade::PriorLogicalDeliveryEvent>(),
            simplified_replies: TableDescriptor::new(
                SIMPLIFIED_REPLY_TABLE_NAME,
                FamilyName::new("flow-nexus-simplified-reply-binding"),
                SimplifiedReplyBinding::schema_hash(),
            ),
            purposes: TableDescriptor::new(
                PURPOSE_TABLE_NAME,
                FamilyName::new("flow-nexus-logical-delivery-purpose"),
                LogicalDeliveryPurpose::schema_hash(),
            ),
            request_ids: TableDescriptor::new(
                REQUEST_ID_TABLE_NAME,
                FamilyName::new("flow-nexus-request-id"),
                RequestIdBinding::schema_hash(),
            ),
            deliver_calls: TableDescriptor::new(
                DELIVER_CALL_TABLE_NAME,
                FamilyName::new("flow-nexus-deliver-call"),
                DeliverCall::schema_hash(),
            ),
            store_identity: TableDescriptor::new(
                STORE_IDENTITY_TABLE_NAME,
                FamilyName::new("flow-nexus-store-identity"),
                StoredStoreIdentity::schema_hash(),
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
            simplified_replies: engine.register_table(descriptors.simplified_replies)?,
            purposes: engine.register_table(descriptors.purposes)?,
            request_ids: engine.register_table(descriptors.request_ids)?,
            deliver_calls: engine.register_table(descriptors.deliver_calls)?,
            store_identity: engine.register_table(descriptors.store_identity)?,
            write_gate: Mutex::new(()),
            pending_answers: Default::default(),
        })
    }
}

pub(crate) trait HoldsAdmissionAnswers {
    fn hold_admission_answer(&self, request_id: &flow_ethos::RequestId) -> PendingAnswerHold;
    fn admission_answer_is_pending(&self, request_id: &flow_ethos::RequestId) -> bool;
}

impl HoldsAdmissionAnswers for FlowStore {
    fn hold_admission_answer(&self, request_id: &flow_ethos::RequestId) -> PendingAnswerHold {
        let pending = self.logical_deliveries.pending_answers.clone();
        *pending
            .request_ids
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .entry(request_id.clone())
            .or_default() += 1;
        PendingAnswerHold {
            pending,
            request_id: request_id.clone(),
        }
    }

    fn admission_answer_is_pending(&self, request_id: &flow_ethos::RequestId) -> bool {
        self.logical_deliveries
            .pending_answers
            .request_ids
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .contains_key(request_id)
    }
}

/// Mutations are assembled into the caller's AtomicCommit so metaflow queue
/// changes and lifecycle transitions have one durability boundary.
pub(crate) struct LogicalDeliveryTransition {
    pub(crate) purpose: crate::heartbeat::DeliveryPurpose,
    pub(crate) handover_reminder: Option<crate::store::handover_reminders::HandoverReminder>,
    pub(crate) deliver_call: Option<DeliverCallSpec>,
    pub(crate) upgrade_notice: Option<crate::store::request_id_upgrade::UpgradeNoticeAdmission>,
}

#[derive(Clone)]
pub(crate) struct DeliverCallSpec {
    pub key: signal_flow::DeliverKey,
    pub sender: Address,
    pub digest: String,
}

pub(crate) trait MintsRequestIds {
    fn mint_request_id(&self) -> Result<Result<flow_ethos::RequestId, Refusal>, StoreError>;
    fn mint_request_id_with_held_count(
        &self,
        held_count: usize,
    ) -> Result<Result<flow_ethos::RequestId, Refusal>, StoreError>;
    fn request_binding(
        &self,
        id: &flow_ethos::RequestId,
    ) -> Result<Option<RequestIdBinding>, StoreError>;
    fn answerable_by(
        &self,
        binding: &RequestIdBinding,
        answerer: &Address,
        requester: &Address,
        task: bool,
    ) -> Result<bool, StoreError>;
    fn store_identity(&self) -> Result<signal_flow::StoreIdentity, StoreError>;
}

impl MintsRequestIds for FlowStore {
    fn store_identity(&self) -> Result<signal_flow::StoreIdentity, StoreError> {
        let rows = self.engine.match_records(QueryPlan::key(
            self.logical_deliveries.store_identity,
            RecordKey::new(STORE_IDENTITY_KEY),
        ))?;
        match rows.records() {
            [row] => Ok(row.store_identity.clone()),
            [] => {
                self.seed_store_identity()?;
                let seeded = self.engine.match_records(QueryPlan::key(
                    self.logical_deliveries.store_identity,
                    RecordKey::new(STORE_IDENTITY_KEY),
                ))?;
                match seeded.records() {
                    [row] => Ok(row.store_identity.clone()),
                    _ => Err(StoreError::StateInvariant),
                }
            }
            _ => Err(StoreError::StateInvariant),
        }
    }
    fn request_binding(
        &self,
        id: &flow_ethos::RequestId,
    ) -> Result<Option<RequestIdBinding>, StoreError> {
        let rows = self.engine.match_records(QueryPlan::key(
            self.logical_deliveries.request_ids,
            RecordKey::new(id.clone()),
        ))?;
        Ok(rows.records().first().cloned())
    }
    fn answerable_by(
        &self,
        binding: &RequestIdBinding,
        answerer: &Address,
        requester: &Address,
        task: bool,
    ) -> Result<bool, StoreError> {
        let Some(retained) = self
            .retained_logical_requests()?
            .into_iter()
            .find(|value| value.entry.logical_delivery_id == binding.logical_delivery_id)
        else {
            return Ok(false);
        };
        let original_sender = match &retained.entry.sent_by {
            SentBy::Sender(value) => value,
            _ => return Ok(false),
        };
        let request = match &retained.entry.message {
            Message::HardAbrupt(value) | Message::MiddleAbrupt(value) | Message::Soft(value) => {
                value
            }
        };
        let correct_kind = if task {
            matches!(request, Request::Task(_))
        } else {
            matches!(request, Request::Order(_) | Request::Question(_))
        };
        let delivered = self
            .logical_delivery_snapshot(&binding.logical_delivery_id)?
            .is_some_and(|value| {
                matches!(
                    value.delivery_state,
                    DeliveryState::Delivered(_) | DeliveryState::Uncertain
                )
            });
        Ok(correct_kind
            && retained.address == *answerer
            && original_sender == requester
            && delivered)
    }
    fn mint_request_id(&self) -> Result<Result<flow_ethos::RequestId, Refusal>, StoreError> {
        let held = self
            .engine
            .match_records(QueryPlan::all(self.logical_deliveries.request_ids))?;
        self.mint_request_id_with_held_count(held.records().len())
    }
    fn mint_request_id_with_held_count(
        &self,
        held_count: usize,
    ) -> Result<Result<flow_ethos::RequestId, Refusal>, StoreError> {
        const SPACE: usize = 36usize.pow(4);
        if held_count == SPACE {
            return Ok(Err(Refusal::RequestIdSpaceFull));
        }
        if held_count > SPACE {
            return Err(StoreError::StateInvariant);
        }
        let draws = self.request_id_draws()?;
        for _ in 0..draws {
            let id = self.request_id_drawer.draw_request_id()?;
            if id.len() != 4
                || !id
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || byte.is_ascii_lowercase())
            {
                return Err(StoreError::StateInvariant);
            }
            let exists = self.engine.match_records(QueryPlan::key(
                self.logical_deliveries.request_ids,
                RecordKey::new(id.clone()),
            ))?;
            if exists.records().is_empty() {
                return Ok(Ok(id));
            }
        }
        Ok(Err(Refusal::RequestIdDrawsCollided))
    }
}

pub(crate) trait RecordsLogicalDeliveries {
    fn deliver_call(
        &self,
        key: &signal_flow::DeliverKey,
    ) -> Result<Option<DeliverCall>, StoreError>;
    fn acknowledge_deliver_calls(&self, keys: &[signal_flow::DeliverKey])
    -> Result<(), StoreError>;
    fn debug_delivery(
        &self,
        request_id: &flow_ethos::RequestId,
    ) -> Result<Option<meta_signal_flow::DeliveryDebugged_Data>, StoreError>;
    fn seed_store_identity(&self) -> Result<(), StoreError>;
    fn archive_request(
        &self,
        request_id: &str,
        sender: &Address,
    ) -> Result<Result<(), signal_flow::ArchiveRejection>, StoreError>;
    fn seed_missing_queued_deliveries(&self) -> Result<(), StoreError>;
    fn force_queue_wake(&self, address: &Address) -> Result<(), StoreError>;
    /// Resolves a public message against durable private evidence. The
    /// authenticated sender and resolved recipient are supplied by the
    /// Nexus, never by the message. Answer correlation is exact typed data.
    fn resolve_sendable_message(
        &self,
        sender: &Address,
        recipient: &Address,
        message: signal_flow::SendableMessage,
    ) -> Result<Result<Message, Refusal>, StoreError>;
    fn accept_tool_send(
        &self,
        key: &ToolCallKey,
        content_sha256: &str,
        sender: Address,
        target: &Target,
        message: Message,
        now: i64,
    ) -> Result<ToolSendOutcome, StoreError>;
    fn accept_logical_delivery(
        &self,
        address: &Address,
        sent_by: SentBy,
        delivery: Delivery,
        message: Message,
        held_lock: Option<VerifiedDeliveryLock>,
    ) -> Result<signal_flow::PrivateSendResponse, StoreError> {
        self.accept_logical_delivery_for(
            address,
            sent_by,
            delivery,
            message,
            held_lock,
            crate::heartbeat::DeliveryPurpose::Ordinary,
        )
    }
    fn accept_deliver_call(
        &self,
        address: &Address,
        sender: Address,
        delivery: Delivery,
        message: Message,
        held_lock: VerifiedDeliveryLock,
        spec: DeliverCallSpec,
    ) -> Result<signal_flow::PrivateSendResponse, StoreError>;
    fn accept_logical_delivery_for(
        &self,
        address: &Address,
        sent_by: SentBy,
        delivery: Delivery,
        message: Message,
        held_lock: Option<VerifiedDeliveryLock>,
        purpose: crate::heartbeat::DeliveryPurpose,
    ) -> Result<signal_flow::PrivateSendResponse, StoreError>;
    fn accept_handover_reminder(
        &self,
        address: &Address,
        message: Message,
        reminder: crate::store::handover_reminders::HandoverReminder,
    ) -> Result<signal_flow::PrivateSendResponse, StoreError>;
    fn accept_logical_delivery_for_with_transition(
        &self,
        address: &Address,
        sent_by: SentBy,
        delivery: Delivery,
        message: Message,
        held_lock: Option<VerifiedDeliveryLock>,
        transition: LogicalDeliveryTransition,
    ) -> Result<signal_flow::PrivateSendResponse, StoreError>;
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
    /// Marks each request of `queue` that waits, Queued or Retryable,
    /// `Retryable(cause)`, so its sender sees why it waits; the queue itself
    /// is untouched. Answers how many changed.
    fn mark_queue_retryable(
        &self,
        queue: &[QueueEntry],
        cause: signal_flow::RetryCause,
    ) -> Result<usize, StoreError>;
    /// Under one durable commit, records the exact native receipt for every
    /// logical request in a fresh wake's fixed startup prefix and removes
    /// only that prefix from the metaflow queue.
    fn settle_wake_startup_receipts(
        &self,
        address: &Address,
        flow_id: &str,
        startup: &[QueueEntry],
        receipt: Option<&signal_flow::NativeReceipt>,
    ) -> Result<bool, StoreError>;
    fn read_logical_delivery_snapshots(
        &self,
        sender: &Address,
        after_cursor: Option<i64>,
    ) -> Result<SendObservation, StoreError>;
    fn read_message_delivery_snapshots(
        &self,
        after_cursor: Option<i64>,
    ) -> Result<SendObservation, StoreError>;
    fn debug_snapshot(
        &self,
        address: &Address,
    ) -> Result<meta_signal_flow::DebugSnapshot, StoreError>;
    fn delivery_purposes(&self) -> Result<Vec<LogicalDeliveryPurpose>, StoreError>;
    fn bind_simplified_reply(&self, binding: SimplifiedReplyBinding) -> Result<(), StoreError>;
    /// What the flow's reply binding owes: nothing, or an answer to an
    /// input not yet received, or one to an input received under its
    /// native receipt.
    fn simplified_reply_receipt(
        &self,
        flow_id: &str,
    ) -> Result<SimplifiedReplyStanding, StoreError>;
    fn deliver_simplified_reply(
        &self,
        flow_id: &str,
        input_delivery_id: &str,
        native_session_id: &str,
        native_prompt_id: &str,
        body: &str,
    ) -> Result<Option<Address>, StoreError>;
}
impl RecordsLogicalDeliveries for FlowStore {
    fn debug_delivery(
        &self,
        request_id: &flow_ethos::RequestId,
    ) -> Result<Option<meta_signal_flow::DeliveryDebugged_Data>, StoreError> {
        use super::queue_delivery::{NativeDeliveryReceipt, QueueDeliveryPhase};
        let Some(binding) = self.request_binding(request_id)? else {
            return Ok(None);
        };
        let Some(snapshot) = self.logical_delivery_snapshot(&binding.logical_delivery_id)? else {
            return Err(StoreError::StateInvariant);
        };
        let attempts = self.delivery_attempts_for(&binding.logical_delivery_id)?;
        let receipt = attempts.iter().find_map(|attempt| {
            let recorded: &NativeDeliveryReceipt = match &attempt.phase {
                QueueDeliveryPhase::ReceiptRecorded(value) | QueueDeliveryPhase::Settled(value) => {
                    value
                }
                QueueDeliveryPhase::Read {
                    late_receipt: Some(value),
                    ..
                } => value,
                _ => return None,
            };
            Some(NativeReceipt {
                harness_kind: attempt.harness_kind.clone(),
                native_session_id: attempt.thread_id.clone(),
                native_prompt_id: recorded.turn_id.clone(),
            })
        });
        let read_by = match binding.closure {
            RequestClosure::Open => None,
            RequestClosure::Answered(answer) => Some(answer),
        };
        Ok(Some(meta_signal_flow::DeliveryDebugged_Data {
            delivery_snapshot: snapshot,
            receipt,
            read_by,
        }))
    }

    fn deliver_call(
        &self,
        key: &signal_flow::DeliverKey,
    ) -> Result<Option<DeliverCall>, StoreError> {
        let rows = self.engine.match_records(QueryPlan::key(
            self.logical_deliveries.deliver_calls,
            key.deliver_call_record_key(),
        ))?;
        match rows.records() {
            [] => Ok(None),
            [row] => Ok(Some(row.clone())),
            _ => Err(StoreError::StateInvariant),
        }
    }

    fn acknowledge_deliver_calls(
        &self,
        keys: &[signal_flow::DeliverKey],
    ) -> Result<(), StoreError> {
        let _guard = self
            .logical_deliveries
            .write_gate
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let mut commit = self.engine.begin_atomic_commit();
        for key in keys {
            if self.deliver_call(key)?.is_some() {
                commit = commit.retract(
                    self.logical_deliveries.deliver_calls,
                    key.deliver_call_record_key(),
                );
            }
        }
        self.engine.commit_atomic(commit)?;
        Ok(())
    }

    fn accept_deliver_call(
        &self,
        address: &Address,
        sender: Address,
        delivery: Delivery,
        message: Message,
        held_lock: VerifiedDeliveryLock,
        spec: DeliverCallSpec,
    ) -> Result<signal_flow::PrivateSendResponse, StoreError> {
        self.accept_logical_delivery_for_with_transition(
            address,
            SentBy::Sender(sender),
            delivery,
            message,
            Some(held_lock),
            LogicalDeliveryTransition {
                purpose: crate::heartbeat::DeliveryPurpose::Ordinary,
                handover_reminder: None,
                deliver_call: Some(spec),
                upgrade_notice: None,
            },
        )
    }
    fn archive_request(
        &self,
        request_id: &str,
        sender: &Address,
    ) -> Result<Result<(), signal_flow::ArchiveRejection>, StoreError> {
        use signal_flow::ArchiveRejection;
        let _guard = self
            .logical_deliveries
            .write_gate
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let Some(binding) = self.request_binding(&request_id.to_owned())? else {
            return Ok(Err(ArchiveRejection::UnknownRequest));
        };
        let Some(retained) = self
            .retained_logical_requests()?
            .into_iter()
            .find(|value| value.entry.logical_delivery_id == binding.logical_delivery_id)
        else {
            return Err(StoreError::StateInvariant);
        };
        if !matches!(&retained.entry.sent_by, SentBy::Sender(owner) if owner == sender) {
            return Ok(Err(ArchiveRejection::UnknownRequest));
        }
        if self.admission_answer_is_pending(&binding.request_id) {
            return Ok(Err(ArchiveRejection::NotYet));
        }
        use super::queue_delivery::EndsQueueDeliveryPhase;
        if self
            .delivery_attempts_for(&binding.logical_delivery_id)?
            .iter()
            .any(|attempt| !attempt.phase.is_finished())
        {
            return Ok(Err(ArchiveRejection::NotYet));
        }
        let replies = self
            .engine
            .match_records(QueryPlan::all(self.logical_deliveries.simplified_replies))?;
        if replies
            .records()
            .iter()
            .any(|reply| !reply.completed && reply.input_delivery_id == binding.logical_delivery_id)
        {
            return Ok(Err(ArchiveRejection::NotYet));
        }
        let mut request_named = false;
        for held in self.retained_logical_requests()? {
            if held.entry.logical_delivery_id == binding.logical_delivery_id {
                continue;
            }
            let names = match &held.entry.message {
                Message::HardAbrupt(request)
                | Message::MiddleAbrupt(request)
                | Message::Soft(request) => match request {
                    Request::Result(flow_ethos::ResultPayload::Answered(answer)) => {
                        answer.request_id == request_id
                    }
                    Request::Done(flow_ethos::DonePayload::Answered(answer)) => {
                        answer.request_id == request_id
                    }
                    Request::Reminder(reminder) => reminder.request_id == request_id,
                    _ => false,
                },
            };
            if names
                && self
                    .logical_delivery_snapshot(&held.entry.logical_delivery_id)?
                    .is_some_and(|snapshot| {
                        !matches!(
                            snapshot.delivery_state,
                            DeliveryState::Delivered(_)
                                | DeliveryState::Read
                                | DeliveryState::Failed(_)
                                | DeliveryState::Withdrawn
                                | DeliveryState::Archived
                        )
                    })
            {
                request_named = true;
                break;
            }
        }
        if request_named {
            return Ok(Err(ArchiveRejection::NotYet));
        }
        let task_announcements = self
            .engine
            .match_records(QueryPlan::all(self.request_id_upgrade.task_announcements))?;
        if task_announcements.records().iter().any(|row| {
            matches!(
                row.status,
                super::request_id_upgrade::TaskAnnouncementStatus::Admitted(_)
            ) && row.tasks.contains(&binding.logical_delivery_id)
        }) {
            return Ok(Err(ArchiveRejection::NotYet));
        }
        let assignments = self
            .engine
            .match_records(QueryPlan::all(self.request_id_upgrade.upgrade_assignments))?;
        if assignments.records().iter().any(|row| {
            matches!(
                row.status,
                super::request_id_upgrade::TaskAnnouncementStatus::Admitted(_)
            ) && row.decisions.iter().any(|decision| {
                decision
                    .assigned
                    .iter()
                    .any(|assigned| assigned.order == binding.logical_delivery_id)
                    || decision
                        .open
                        .iter()
                        .any(|order| order.order == binding.logical_delivery_id)
            })
        }) {
            return Ok(Err(ArchiveRejection::NotYet));
        }
        let deliver_calls = self
            .engine
            .match_records(QueryPlan::all(self.logical_deliveries.deliver_calls))?;
        if deliver_calls
            .records()
            .iter()
            .any(|call| call.answer.logical_delivery_id == binding.logical_delivery_id)
        {
            return Ok(Err(ArchiveRejection::NotYet));
        }
        let rows = self.engine.match_records(QueryPlan::key(
            self.logical_deliveries.current,
            RecordKey::new(binding.logical_delivery_id.clone()),
        ))?;
        let [current] = rows.records() else {
            return Err(StoreError::StateInvariant);
        };
        if current.archived.is_some()
            || !matches!(
                current.snapshot.delivery_state,
                DeliveryState::Delivered(_)
                    | DeliveryState::Read
                    | DeliveryState::Failed(_)
                    | DeliveryState::Withdrawn
            )
        {
            return Ok(Err(ArchiveRejection::NotYet));
        }
        let allocation = self.allocate_logical_cursor()?;
        let cursor = allocation.cursor;
        let is_task = matches!(
            &retained.entry.message,
            Message::HardAbrupt(Request::Task(_))
                | Message::MiddleAbrupt(Request::Task(_))
                | Message::Soft(Request::Task(_))
        );
        let task_reminder = if is_task {
            let reminders = self.engine.match_records(QueryPlan::key(
                self.reminders,
                RecordKey::new(binding.logical_delivery_id.clone()),
            ))?;
            match reminders.records() {
                [] => false,
                [_] => true,
                _ => return Err(StoreError::StateInvariant),
            }
        } else {
            false
        };
        let mut archived = current.clone();
        archived.archived = Some(cursor);
        let mut event = current.snapshot.clone();
        event.delivery_cursor = cursor;
        event.delivery_state = DeliveryState::Archived;
        let next_counter = LogicalDeliveryCounter {
            next_cursor: cursor.checked_add(1).ok_or(StoreError::StateInvariant)?,
        };
        let commit = self
            .engine
            .begin_atomic_commit()
            .mutate(self.logical_deliveries.current, archived)
            .assert(
                self.logical_deliveries.events,
                LogicalDeliveryEvent { snapshot: event },
            )
            .retract(
                self.logical_deliveries.request_ids,
                RecordKey::new(request_id.to_owned()),
            );
        let commit = if allocation.existing {
            commit.mutate(self.logical_deliveries.counter, next_counter)
        } else {
            commit.assert(self.logical_deliveries.counter, next_counter)
        };
        let commit = if task_reminder {
            commit.retract(
                self.reminders,
                RecordKey::new(binding.logical_delivery_id.clone()),
            )
        } else {
            commit
        };
        self.engine.commit_atomic(commit)?;
        self.launch_changes.announce();
        Ok(Ok(()))
    }

    fn seed_store_identity(&self) -> Result<(), StoreError> {
        let rows = self.engine.match_records(QueryPlan::key(
            self.logical_deliveries.store_identity,
            RecordKey::new(STORE_IDENTITY_KEY),
        ))?;
        if !rows.records().is_empty() {
            return Ok(());
        }
        let mut store_identity = String::with_capacity(16);
        for _ in 0..4 {
            store_identity.push_str(&self.request_id_drawer.draw_request_id()?);
        }
        self.engine.assert(Assertion::new(
            self.logical_deliveries.store_identity,
            StoredStoreIdentity { store_identity },
        ))?;
        Ok(())
    }
    fn delivery_purposes(&self) -> Result<Vec<LogicalDeliveryPurpose>, StoreError> {
        Ok(self
            .engine
            .match_records(QueryPlan::all(self.logical_deliveries.purposes))?
            .records()
            .to_vec())
    }
    fn debug_snapshot(
        &self,
        address: &Address,
    ) -> Result<meta_signal_flow::DebugSnapshot, StoreError> {
        let mut flow_id_vector = Vec::new();
        if let Some(metaflow) = self.metaflow(address)? {
            flow_id_vector.extend(metaflow.past);
            if let flow_ethos::State::Awake(flow_id) = metaflow.state {
                flow_id_vector.push(flow_id);
            }
        }
        let current = self
            .engine
            .match_records(QueryPlan::all(self.logical_deliveries.current))?;
        let logical_delivery_id_vector = current
            .records()
            .iter()
            .filter(|row| row.snapshot.address == *address)
            .map(|row| row.snapshot.logical_delivery_id.clone())
            .collect();
        Ok(meta_signal_flow::DebugSnapshot {
            address: address.clone(),
            flow_id_vector,
            logical_delivery_id_vector,
        })
    }
    fn settle_wake_startup_receipts(
        &self,
        address: &Address,
        flow_id: &str,
        startup: &[QueueEntry],
        receipt: Option<&signal_flow::NativeReceipt>,
    ) -> Result<bool, StoreError> {
        let _guard = self
            .logical_deliveries
            .write_gate
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let Some(mut stored) = self.stored_metaflow(address)? else {
            return Ok(false);
        };
        if stored.metaflow.state != flow_ethos::State::Awake(flow_id.to_owned())
            || !stored.metaflow.queue.starts_with(startup)
        {
            return Ok(false);
        }

        let allocation = self.allocate_logical_cursor()?;
        let mut next_cursor = allocation.cursor;
        let mut changed = false;
        let mut commit = self.engine.begin_atomic_commit();
        let mut seen = std::collections::HashSet::new();
        for entry in startup {
            let records = self.engine.match_records(QueryPlan::key(
                self.logical_deliveries.current,
                RecordKey::new(entry.logical_delivery_id.clone()),
            ))?;
            let Some(current) = records.records().first() else {
                // A legacy queue entry has no logical lifecycle row. Its
                // exact prefix position is still consumed by this receipt.
                continue;
            };
            if !seen.insert(entry.logical_delivery_id.clone()) {
                return Err(StoreError::StateInvariant);
            }
            if records.records().len() != 1 {
                return Err(StoreError::StateInvariant);
            }
            if current.snapshot.address != *address {
                return Err(StoreError::StateInvariant);
            }
            let delivered =
                DeliveryState::Delivered(receipt.cloned().ok_or(StoreError::StateInvariant)?);
            if current.snapshot.delivery_state == delivered {
                // Reconcile the historical partial shape: the receipt was
                // durable but the exact prefix had not yet been removed.
                continue;
            }
            if matches!(current.snapshot.delivery_state, DeliveryState::Delivered(_)) {
                return Err(StoreError::StateInvariant);
            }
            if !matches!(
                current.snapshot.delivery_state,
                DeliveryState::Queued | DeliveryState::Retryable(_)
            ) {
                return Err(StoreError::StateInvariant);
            }
            let mut snapshot = current.snapshot.clone();
            snapshot.delivery_cursor = next_cursor;
            snapshot.delivery_state = delivered;
            next_cursor = next_cursor
                .checked_add(1)
                .ok_or(StoreError::StateInvariant)?;
            commit = commit
                .mutate(
                    self.logical_deliveries.current,
                    CurrentLogicalDelivery {
                        snapshot: snapshot.clone(),
                        archived: None,
                        admission: current.admission.clone(),
                    },
                )
                .assert(
                    self.logical_deliveries.events,
                    LogicalDeliveryEvent { snapshot },
                );
            changed = true;
        }
        if changed {
            let counter = LogicalDeliveryCounter { next_cursor };
            commit = if allocation.existing {
                commit.mutate(self.logical_deliveries.counter, counter)
            } else {
                commit.assert(self.logical_deliveries.counter, counter)
            };
        }
        stored.metaflow.queue.drain(..startup.len());
        commit = commit.mutate(self.metaflows, stored);
        self.engine.commit_atomic(commit)?;
        self.launch_changes.announce();
        Ok(true)
    }
    fn bind_simplified_reply(&self, binding: SimplifiedReplyBinding) -> Result<(), StoreError> {
        let _guard = self
            .logical_deliveries
            .write_gate
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let key = RecordKey::new(binding.flow_id.clone());
        let existing = self.engine.match_records(QueryPlan::key(
            self.logical_deliveries.simplified_replies,
            key.clone(),
        ))?;
        if let [record] = existing.records() {
            if record == &binding {
                return Ok(());
            }
            if record.completed {
                let commit = self
                    .engine
                    .begin_atomic_commit()
                    .mutate(self.logical_deliveries.simplified_replies, binding);
                self.engine.commit_atomic(commit)?;
                return Ok(());
            }
            return Err(StoreError::StateInvariant);
        }
        self.engine.assert(Assertion::new(
            self.logical_deliveries.simplified_replies,
            binding,
        ))?;
        Ok(())
    }

    fn simplified_reply_receipt(
        &self,
        flow_id: &str,
    ) -> Result<SimplifiedReplyStanding, StoreError> {
        let binding = self.engine.match_records(QueryPlan::key(
            self.logical_deliveries.simplified_replies,
            RecordKey::new(flow_id.to_owned()),
        ))?;
        let Some(binding) = binding.records().first().cloned() else {
            return Ok(SimplifiedReplyStanding::NoneOwed);
        };
        if binding.completed {
            return Ok(SimplifiedReplyStanding::NoneOwed);
        }
        let input = self.engine.match_records(QueryPlan::key(
            self.logical_deliveries.current,
            RecordKey::new(binding.input_delivery_id.clone()),
        ))?;
        let receipt =
            input
                .records()
                .first()
                .and_then(|record| match &record.snapshot.delivery_state {
                    DeliveryState::Delivered(receipt) => Some(receipt.clone()),
                    _ => None,
                });
        Ok(match receipt {
            Some(receipt) => SimplifiedReplyStanding::Received(binding, receipt),
            None => SimplifiedReplyStanding::Unreceived(binding),
        })
    }

    fn deliver_simplified_reply(
        &self,
        flow_id: &str,
        input_delivery_id: &str,
        native_session_id: &str,
        native_prompt_id: &str,
        body: &str,
    ) -> Result<Option<Address>, StoreError> {
        let _guard = self
            .logical_deliveries
            .write_gate
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let key = RecordKey::new(flow_id.to_owned());
        let binding = self.engine.match_records(QueryPlan::key(
            self.logical_deliveries.simplified_replies,
            key.clone(),
        ))?;
        let Some(mut binding) = binding.records().first().cloned() else {
            return Ok(None);
        };
        if binding.completed || binding.input_delivery_id != input_delivery_id || body.is_empty() {
            return Ok(None);
        }
        let input = self.engine.match_records(QueryPlan::key(
            self.logical_deliveries.current,
            RecordKey::new(binding.input_delivery_id.clone()),
        ))?;
        let consumed = input.records().first().is_some_and(|record| {
            matches!(
                &record.snapshot.delivery_state,
                DeliveryState::Delivered(receipt)
                    if receipt.native_session_id == native_session_id
                        && receipt.native_prompt_id == native_prompt_id
            )
        });
        if !consumed {
            return Ok(None);
        }
        let core_presentation = binding.source.is_core_presentation();
        let destination = if core_presentation {
            Address {
                aspect: signal_flow::Aspect::Psyche,
                topic: "book".into(),
                model: signal_flow::Model::Sonnet,
            }
        } else {
            binding.recipient.clone()
        };
        let Some(mut target) = self.stored_metaflow(&destination)? else {
            return Ok(None);
        };
        let allocation = self.allocate_logical_cursor()?;
        let cursor = allocation.cursor;
        let request_id = self
            .mint_request_id()?
            .map_err(|_| StoreError::StateInvariant)?;
        let id = format!("simplified-result-{flow_id}-{}", binding.input_delivery_id);
        let source = binding.source.clone();
        target.metaflow.queue.push(QueueEntry {
            logical_delivery_id: id.clone(),
            request_id: request_id.clone(),
            sent_by: SentBy::Sender(source.clone()),
            delivery: flow_ethos::Delivery::Wake,
            message: Message::Soft(if core_presentation {
                Request::Order(body.to_owned())
            } else {
                Request::Result(flow_ethos::ResultPayload::Historical(body.to_owned()))
            }),
        });
        let snapshot = DeliverySnapshot {
            delivery_cursor: cursor,
            logical_delivery_id: id,
            request_id: request_id.clone(),
            sent_by: SentBy::Sender(source),
            address: destination.clone(),
            delivery_state: DeliveryState::Queued,
        };
        binding.completed = true;
        let mut commit = self
            .engine
            .begin_atomic_commit()
            .mutate(self.metaflows, target);
        let mut next_cursor = cursor.checked_add(1).ok_or(StoreError::StateInvariant)?;
        if core_presentation {
            let Some(mut notification) = self.stored_metaflow(&binding.recipient)? else {
                return Err(StoreError::StateInvariant);
            };
            let notice_id = format!("simplified-publication-notice-{flow_id}-{input_delivery_id}");
            let notice_request_id = self
                .mint_request_id()?
                .map_err(|_| StoreError::StateInvariant)?;
            notification.metaflow.queue.push(QueueEntry {
                logical_delivery_id: notice_id.clone(),
                request_id: notice_request_id.clone(),
                sent_by: SentBy::Sender(binding.source.clone()),
                delivery: flow_ethos::Delivery::Wake,
                message: Message::Soft(Request::Notice(
                    "The native presentation source was queued for Psyche book Tertiary as the sole artifact source. Publication is not yet witnessed.".into(),
                )),
            });
            let notice_snapshot = DeliverySnapshot {
                delivery_cursor: next_cursor,
                logical_delivery_id: notice_id,
                request_id: notice_request_id.clone(),
                sent_by: SentBy::Sender(binding.source.clone()),
                address: binding.recipient.clone(),
                delivery_state: DeliveryState::Queued,
            };
            commit = commit
                .mutate(self.metaflows, notification)
                .assert(
                    self.logical_deliveries.current,
                    CurrentLogicalDelivery {
                        snapshot: notice_snapshot.clone(),
                        archived: None,
                        admission: DeliveryAdmission::Ordinary,
                    },
                )
                .assert(
                    self.logical_deliveries.events,
                    LogicalDeliveryEvent {
                        snapshot: notice_snapshot.clone(),
                    },
                )
                .assert(
                    self.logical_deliveries.request_ids,
                    RequestIdBinding {
                        request_id: notice_request_id,
                        logical_delivery_id: notice_snapshot.logical_delivery_id.clone(),
                        closure: RequestClosure::Open,
                    },
                );
            next_cursor = next_cursor
                .checked_add(1)
                .ok_or(StoreError::StateInvariant)?;
        }
        let counter = LogicalDeliveryCounter { next_cursor };
        commit = if allocation.existing {
            commit.mutate(self.logical_deliveries.counter, counter)
        } else {
            commit.assert(self.logical_deliveries.counter, counter)
        };
        self.engine.commit_atomic(
            commit
                .mutate(self.logical_deliveries.simplified_replies, binding)
                .assert(
                    self.logical_deliveries.current,
                    CurrentLogicalDelivery {
                        snapshot: snapshot.clone(),
                        archived: None,
                        admission: DeliveryAdmission::Ordinary,
                    },
                )
                .assert(
                    self.logical_deliveries.events,
                    LogicalDeliveryEvent {
                        snapshot: snapshot.clone(),
                    },
                )
                .assert(
                    self.logical_deliveries.request_ids,
                    RequestIdBinding {
                        request_id,
                        logical_delivery_id: snapshot.logical_delivery_id.clone(),
                        closure: RequestClosure::Open,
                    },
                ),
        )?;
        self.launch_changes.announce();
        Ok(Some(destination))
    }

    fn accept_tool_send(
        &self,
        key: &ToolCallKey,
        content_sha256: &str,
        sender: Address,
        target: &Target,
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
                if let ToolSendOutcome::Accepted(accepted) = &record.send_outcome {
                    let logical_id = &accepted.send_acceptance.logical_delivery_id;
                    let binding = self.request_binding(&accepted.send_acceptance.request_id)?;
                    if binding
                        .as_ref()
                        .is_some_and(|binding| binding.logical_delivery_id == *logical_id)
                    {
                        return Ok(record.send_outcome.clone());
                    }
                    let current = self.engine.match_records(QueryPlan::key(
                        self.logical_deliveries.current,
                        RecordKey::new(logical_id.clone()),
                    ))?;
                    let [current] = current.records() else {
                        return Err(StoreError::StateInvariant);
                    };
                    if current.archived.is_none() {
                        return Err(StoreError::StateInvariant);
                    }
                    return Ok(ToolSendOutcome::Archived(
                        crate::generated::send_tool::Archived_Data {
                            sender: accepted.sender.clone(),
                            target: accepted.target.clone(),
                        },
                    ));
                }
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
        let request = match &message {
            Message::HardAbrupt(request)
            | Message::MiddleAbrupt(request)
            | Message::Soft(request) => request.clone(),
        };
        let request_id = match self.mint_request_id()? {
            Ok(value) => value,
            Err(refusal) => {
                let outcome = ToolSendOutcome::Refused(match refusal {
                    Refusal::RequestIdSpaceFull => ToolSendRefusal::RequestIdSpaceFull,
                    Refusal::RequestIdDrawsCollided => ToolSendRefusal::RequestIdDrawsCollided,
                    _ => ToolSendRefusal::Store,
                });
                self.engine
                    .assert(Assertion::new(self.send_calls, record(outcome.clone())))?;
                return Ok(outcome);
            }
        };
        let answered_binding = match &request {
            Request::Result(flow_ethos::ResultPayload::Answered(answered)) => Some(
                self.request_binding(&answered.request_id)?
                    .ok_or(StoreError::StateInvariant)?,
            ),
            Request::Done(flow_ethos::DonePayload::Answered(answered)) => Some(
                self.request_binding(&answered.request_id)?
                    .ok_or(StoreError::StateInvariant)?,
            ),
            _ => None,
        };
        let answered_uncertain = match &answered_binding {
            Some(binding) => self
                .logical_delivery_snapshot(&binding.logical_delivery_id)?
                .is_some_and(|snapshot| snapshot.delivery_state == DeliveryState::Uncertain),
            None => false,
        };
        let allocation = self.allocate_logical_cursor()?;
        let cursor = allocation.cursor;
        let create_counter = !allocation.existing;
        let id = format!("logical-delivery-{cursor:020}");
        let delivery = if matches!(message, Message::Soft(_)) {
            flow_ethos::Delivery::Queue
        } else {
            flow_ethos::Delivery::Wake
        };
        let entry = QueueEntry {
            logical_delivery_id: id.clone(),
            request_id: request_id.clone(),
            sent_by: SentBy::Sender(sender.clone()),
            delivery,
            message,
        };
        stored.metaflow.queue.push(entry.clone());
        let snapshot = DeliverySnapshot {
            delivery_cursor: cursor,
            logical_delivery_id: id.clone(),
            request_id: request_id.clone(),
            sent_by: SentBy::Sender(sender.clone()),
            address: stored.metaflow.address.clone(),
            delivery_state: DeliveryState::Queued,
        };
        let acceptance = SendAcceptance {
            logical_delivery_id: id,
            request_id: request_id.clone(),
            delivery_cursor: cursor,
            delivery_state: DeliveryState::Queued,
        };
        let accepted_logical_id = acceptance.logical_delivery_id.clone();
        let outcome = ToolSendOutcome::Accepted(Accepted_Data {
            sender: sender.clone(),
            target: target.clone(),
            send_acceptance: acceptance,
        });
        let next_counter = LogicalDeliveryCounter {
            next_cursor: cursor
                .checked_add(if answered_uncertain { 2 } else { 1 })
                .ok_or(StoreError::StateInvariant)?,
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
        let commit = self.assert_logical_request(target, entry, commit);
        let commit = if let Some(mut binding) = answered_binding {
            let rows = self.engine.match_records(QueryPlan::key(
                self.logical_deliveries.current,
                RecordKey::new(binding.logical_delivery_id.clone()),
            ))?;
            let [current] = rows.records() else {
                return Err(StoreError::StateInvariant);
            };
            if current.archived.is_some() {
                return Err(StoreError::StateInvariant);
            }
            let answered_logical_id = binding.logical_delivery_id.clone();
            binding.closure = RequestClosure::Answered(accepted_logical_id.clone());
            let commit = commit.mutate(self.logical_deliveries.request_ids, binding);
            if answered_uncertain {
                let mut read = current.clone();
                read.snapshot.delivery_cursor = cursor + 1;
                read.snapshot.delivery_state = DeliveryState::Read;
                self.read_uncertain_delivery(&answered_logical_id, &accepted_logical_id, commit)?
                    .mutate(self.logical_deliveries.current, read.clone())
                    .assert(
                        self.logical_deliveries.events,
                        LogicalDeliveryEvent {
                            snapshot: read.snapshot,
                        },
                    )
            } else {
                commit
            }
        } else {
            commit
        };
        self.engine.commit_atomic(
            commit
                .assert(
                    self.logical_deliveries.current,
                    CurrentLogicalDelivery {
                        snapshot: snapshot.clone(),
                        archived: None,
                        admission: DeliveryAdmission::Ordinary,
                    },
                )
                .assert(
                    self.logical_deliveries.events,
                    LogicalDeliveryEvent { snapshot },
                )
                .assert(
                    self.logical_deliveries.request_ids,
                    RequestIdBinding {
                        request_id,
                        logical_delivery_id: accepted_logical_id,
                        closure: RequestClosure::Open,
                    },
                )
                .assert(
                    self.logical_deliveries.purposes,
                    LogicalDeliveryPurpose {
                        logical_delivery_id: format!("logical-delivery-{cursor:020}"),
                        address: target.clone(),
                        purpose: crate::heartbeat::DeliveryPurpose::Ordinary,
                    },
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
        // An entry already handed to a native attempt keeps the evidence
        // that attempt carries; only entries not yet handed over change.
        let handed_over = self
            .outstanding_delivery(address)?
            .map(|attempt| attempt.request_id);
        let mut commit = self.engine.begin_atomic_commit();
        let mut changed = false;
        for entry in &mut stored.metaflow.queue {
            if entry.delivery == flow_ethos::Delivery::Wake
                || handed_over.as_deref() == Some(entry.logical_delivery_id.as_str())
            {
                continue;
            }
            let before = entry.clone();
            entry.delivery = flow_ethos::Delivery::Wake;
            commit = self.follow_queue_entry(address, &before, entry, commit)?;
            changed = true;
        }
        if changed {
            self.engine
                .commit_atomic(commit.mutate(self.metaflows, stored))?;
            self.launch_changes.announce();
        }
        Ok(())
    }
    fn resolve_sendable_message(
        &self,
        sender: &Address,
        recipient: &Address,
        message: signal_flow::SendableMessage,
    ) -> Result<Result<Message, Refusal>, StoreError> {
        use flow_ethos::{ResultPayload, ResultPayload_Answered_Data};
        use signal_flow::{SendableMessage, SendableRequest};

        let (priority, request) = match message {
            SendableMessage::HardAbrupt(request) => (0_u8, request),
            SendableMessage::MiddleAbrupt(request) => (1_u8, request),
            SendableMessage::Soft(request) => (2_u8, request),
        };
        let private = match request {
            SendableRequest::Order(value) => Request::Order(value),
            SendableRequest::Question(value) => Request::Question(value),
            SendableRequest::Psyche(value) => Request::Psyche(value),
            SendableRequest::Psyches(value) => Request::Psyches(value),
            SendableRequest::Notice(value) => Request::Notice(value),
            SendableRequest::Task(value) => Request::Task(value),
            SendableRequest::Result(result) => {
                let Some(binding) = self.request_binding(&result.request_id)? else {
                    return Ok(Err(Refusal::Unresolved));
                };
                if binding.closure != RequestClosure::Open
                    || !self.answerable_by(&binding, sender, recipient, false)?
                {
                    return Ok(Err(Refusal::Unresolved));
                }
                Request::Result(ResultPayload::Answered(ResultPayload_Answered_Data {
                    logical_delivery_id: binding.logical_delivery_id,
                    request_id: result.request_id,
                    string: result.string,
                }))
            }
            SendableRequest::Done(done) => {
                let Some(binding) = self.request_binding(&done.request_id)? else {
                    return Ok(Err(Refusal::Unresolved));
                };
                if binding.closure != RequestClosure::Open
                    || !self.answerable_by(&binding, sender, recipient, true)?
                {
                    return Ok(Err(Refusal::Unresolved));
                }
                Request::Done(flow_ethos::DonePayload::Answered(
                    flow_ethos::DonePayload_Answered_Data {
                        logical_delivery_id: binding.logical_delivery_id,
                        request_id: done.request_id,
                        string: done.string,
                    },
                ))
            }
        };
        Ok(Ok(match priority {
            0 => Message::HardAbrupt(private),
            1 => Message::MiddleAbrupt(private),
            _ => Message::Soft(private),
        }))
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
                    request_id: entry.request_id.clone(),
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
                        archived: None,
                        admission: DeliveryAdmission::Ordinary,
                    },
                )
                .assert(
                    self.logical_deliveries.events,
                    LogicalDeliveryEvent {
                        snapshot: snapshot.clone(),
                    },
                )
                .assert(
                    self.logical_deliveries.request_ids,
                    RequestIdBinding {
                        request_id: snapshot.request_id.clone(),
                        logical_delivery_id: snapshot.logical_delivery_id.clone(),
                        closure: RequestClosure::Open,
                    },
                );
        }
        self.engine.commit_atomic(commit)?;
        self.launch_changes.announce();
        Ok(())
    }
    fn accept_logical_delivery_for(
        &self,
        address: &Address,
        sent_by: SentBy,
        delivery: Delivery,
        message: Message,
        held_lock: Option<VerifiedDeliveryLock>,
        purpose: crate::heartbeat::DeliveryPurpose,
    ) -> Result<signal_flow::PrivateSendResponse, StoreError> {
        self.accept_logical_delivery_for_with_transition(
            address,
            sent_by,
            delivery,
            message,
            held_lock,
            LogicalDeliveryTransition {
                purpose,
                handover_reminder: None,
                deliver_call: None,
                upgrade_notice: None,
            },
        )
    }
    fn accept_handover_reminder(
        &self,
        address: &Address,
        message: Message,
        reminder: crate::store::handover_reminders::HandoverReminder,
    ) -> Result<signal_flow::PrivateSendResponse, StoreError> {
        self.accept_logical_delivery_for_with_transition(
            address,
            SentBy::Flow,
            Delivery::Queue,
            message,
            None,
            LogicalDeliveryTransition {
                purpose: crate::heartbeat::DeliveryPurpose::Ordinary,
                handover_reminder: Some(reminder),
                deliver_call: None,
                upgrade_notice: None,
            },
        )
    }
    fn accept_logical_delivery_for_with_transition(
        &self,
        address: &Address,
        sent_by: SentBy,
        delivery: Delivery,
        message: Message,
        held_lock: Option<VerifiedDeliveryLock>,
        transition: LogicalDeliveryTransition,
    ) -> Result<signal_flow::PrivateSendResponse, StoreError> {
        let _guard = self
            .logical_deliveries
            .write_gate
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        // Reminder admission follows the global order logical -> handover.
        // Keep/Drop takes only the handover gate, so its mutation cannot race
        // the green check and the atomic Notice/state commit below.
        let _handover_guard = self
            .handover_write_gate
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let LogicalDeliveryTransition {
            purpose,
            handover_reminder,
            deliver_call,
            upgrade_notice,
        } = transition;
        let admission = if deliver_call.is_some() {
            DeliveryAdmission::Message
        } else {
            DeliveryAdmission::Ordinary
        };
        // A retry may present the same pre-commit snapshot after the prior
        // atomic commit succeeded. The durable sent count is the expected
        // state witness; refuse the stale attempt before creating a second
        // logical Notice.
        if let Some(expected) = handover_reminder.as_ref() {
            let stale = || {
                signal_flow::PrivateSendResponse::Refused(Refusal::Unknown(Unknown_Data::Address(
                    address.clone(),
                )))
            };
            let Some(live) = self.handover_memory(&expected.address)? else {
                return Ok(stale());
            };
            if live.generation != expected.generation || live.current_flow_id.is_none() {
                return Ok(stale());
            }
            // Revalidate green while holding the same logical write gate as
            // admission. A Keep/Drop that won the gate before this call
            // therefore suppresses the stale warning rather than queueing it.
            if let Some(flow_id) = live.current_flow_id.as_deref()
                && let Some(threshold) = self.configured_threshold(&expected.model_name)?
                && self.handover_green(&expected.address, flow_id, threshold.refresh)?
            {
                return Ok(stale());
            }
            if let Some(held) = self.handover_reminder(&expected.address, expected.generation)?
                && held.sent >= expected.sent
            {
                return Ok(stale());
            }
        }
        let Some(mut stored) = self.stored_metaflow(address)? else {
            return Ok(signal_flow::PrivateSendResponse::Refused(Refusal::Unknown(
                Unknown_Data::Address(address.clone()),
            )));
        };
        let request = match &message {
            Message::HardAbrupt(request)
            | Message::MiddleAbrupt(request)
            | Message::Soft(request) => request.clone(),
        };
        if let Request::Done(flow_ethos::DonePayload::Answered(done)) = &request
            && self.reminder(&done.logical_delivery_id)?.is_none()
        {
            return Ok(signal_flow::PrivateSendResponse::Refused(
                Refusal::Unresolved,
            ));
        }
        // Flow owns the Minutes rule at admission: above zero, and every
        // deadline its reminders reach, computed with checked arithmetic,
        // inside the clock. No policy maximum exists.
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|elapsed| elapsed.as_secs() as i64)
            .unwrap_or(0);
        let task_due = match &request {
            Request::Task(task) => {
                use super::reminder::RulesMinutes;
                match task.minutes.ruled_interval(now) {
                    Ok(interval) => Some(now + interval),
                    Err(refusal) => {
                        return Ok(signal_flow::PrivateSendResponse::Refused(
                            Refusal::MinutesRefused(refusal),
                        ));
                    }
                }
            }
            _ => None,
        };
        let task_sender = sent_by.clone();
        let lock = if let Some(held) = held_lock {
            let Some(existing) = self.stored_metaflow_lock(address)? else {
                return Ok(signal_flow::PrivateSendResponse::Refused(Refusal::Unknown(
                    Unknown_Data::Lock(held.lock),
                )));
            };
            if existing.lock != held.lock {
                return Ok(signal_flow::PrivateSendResponse::Refused(Refusal::Unknown(
                    Unknown_Data::Lock(held.lock),
                )));
            }
            if held.lock.until <= held.now {
                return Ok(signal_flow::PrivateSendResponse::Refused(Refusal::Lapsed));
            }
            Some(held.lock)
        } else {
            None
        };
        let request_id = match self.mint_request_id()? {
            Ok(value) => value,
            Err(refusal) => return Ok(signal_flow::PrivateSendResponse::Refused(refusal)),
        };
        let answered_binding = match &request {
            Request::Result(flow_ethos::ResultPayload::Answered(answered)) => Some(
                self.request_binding(&answered.request_id)?
                    .ok_or(StoreError::StateInvariant)?,
            ),
            Request::Done(flow_ethos::DonePayload::Answered(answered)) => Some(
                self.request_binding(&answered.request_id)?
                    .ok_or(StoreError::StateInvariant)?,
            ),
            _ => None,
        };
        let answered_uncertain = match &answered_binding {
            Some(binding) => self
                .logical_delivery_snapshot(&binding.logical_delivery_id)?
                .is_some_and(|snapshot| snapshot.delivery_state == DeliveryState::Uncertain),
            None => false,
        };
        let allocation = self.allocate_logical_cursor()?;
        let next = allocation.cursor;
        let create = !allocation.existing;
        let id = format!("logical-delivery-{next:020}");
        let entry = QueueEntry {
            logical_delivery_id: id.clone(),
            request_id: request_id.clone(),
            sent_by: sent_by.clone(),
            delivery,
            message,
        };
        stored.metaflow.queue.push(entry.clone());
        let snapshot = DeliverySnapshot {
            delivery_cursor: next,
            logical_delivery_id: id.clone(),
            request_id: request_id.clone(),
            sent_by,
            address: stored.metaflow.address.clone(),
            delivery_state: DeliveryState::Queued,
        };
        let next_counter = LogicalDeliveryCounter {
            next_cursor: next
                .checked_add(if answered_uncertain { 2 } else { 1 })
                .ok_or(StoreError::StateInvariant)?,
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
                    archived: None,
                    admission,
                },
            )
            .assert(
                self.logical_deliveries.events,
                LogicalDeliveryEvent {
                    snapshot: snapshot.clone(),
                },
            )
            .assert(
                self.logical_deliveries.purposes,
                LogicalDeliveryPurpose {
                    logical_delivery_id: id.clone(),
                    address: address.clone(),
                    purpose,
                },
            )
            .assert(
                self.logical_deliveries.request_ids,
                RequestIdBinding {
                    request_id: request_id.clone(),
                    logical_delivery_id: id.clone(),
                    closure: RequestClosure::Open,
                },
            );
        let commit = if let Some(call) = deliver_call {
            commit.assert(
                self.logical_deliveries.deliver_calls,
                DeliverCall {
                    key: call.key,
                    sender: call.sender,
                    digest: call.digest,
                    answer: SendAcceptance {
                        logical_delivery_id: id.clone(),
                        request_id: request_id.clone(),
                        delivery_cursor: next,
                        delivery_state: DeliveryState::Queued,
                    },
                },
            )
        } else {
            commit
        };
        let commit = self.assert_logical_request(address, entry, commit);
        let commit = if let Some(mut binding) = answered_binding {
            let rows = self.engine.match_records(QueryPlan::key(
                self.logical_deliveries.current,
                RecordKey::new(binding.logical_delivery_id.clone()),
            ))?;
            let [current] = rows.records() else {
                return Err(StoreError::StateInvariant);
            };
            if current.archived.is_some() {
                return Err(StoreError::StateInvariant);
            }
            let answered_logical_id = binding.logical_delivery_id.clone();
            binding.closure = RequestClosure::Answered(id.clone());
            let commit = commit.mutate(self.logical_deliveries.request_ids, binding);
            if answered_uncertain {
                let mut read = current.clone();
                read.snapshot.delivery_cursor = next + 1;
                read.snapshot.delivery_state = DeliveryState::Read;
                self.read_uncertain_delivery(&answered_logical_id, &id, commit)?
                    .mutate(self.logical_deliveries.current, read.clone())
                    .assert(
                        self.logical_deliveries.events,
                        LogicalDeliveryEvent {
                            snapshot: read.snapshot,
                        },
                    )
            } else {
                commit
            }
        } else {
            commit
        };
        let commit = match &request {
            Request::Task(task) => {
                let (reminded, due) = match &task_sender {
                    SentBy::Sender(sender) => (sender.clone(), task_due),
                    _ => (address.clone(), None),
                };
                commit.assert(
                    self.reminders,
                    super::reminder::Reminder {
                        key: id.clone(),
                        address: reminded,
                        recipient: address.clone(),
                        minutes: task.minutes,
                        due,
                        sent: 0,
                    },
                )
            }
            Request::Done(flow_ethos::DonePayload::Answered(done)) => commit.retract(
                self.reminders,
                RecordKey::new(done.logical_delivery_id.clone()),
            ),
            Request::Done(flow_ethos::DonePayload::Historical(_)) => commit,
            _ => commit,
        };
        let commit = if let Some(lock) = lock {
            commit.retract(
                self.metaflow_locks,
                RecordKey::new(FlowStore::address_record_key(&lock.address)),
            )
        } else {
            commit
        };
        let commit = if purpose == crate::heartbeat::DeliveryPurpose::Heartbeat {
            let standing = self.engine.match_records(QueryPlan::key(
                self.heartbeat_standings,
                RecordKey::new(FlowStore::address_record_key(address)),
            ))?;
            if let [held] = standing.records() {
                let mut next = held.clone();
                next.baseline_awake = next.latest_awake;
                next.baseline_working = next.latest_working;
                next.baseline_working_complete = next.latest_working_complete;
                next.baseline_sent = next.latest_sent;
                next.baseline_queue = next.latest_queue;
                next.outstanding = Some(id.clone());
                commit.mutate(self.heartbeat_standings, next)
            } else {
                commit
            }
        } else {
            commit
        };
        let commit = if let Some(reminder) = handover_reminder {
            let key = reminder.record_key();
            let existing = self
                .engine
                .match_records(QueryPlan::key(self.handover_reminders, key.clone()))?;
            if existing.records().is_empty() {
                commit.assert(self.handover_reminders, reminder)
            } else {
                commit.mutate(self.handover_reminders, reminder)
            }
        } else {
            commit
        };
        let commit = match upgrade_notice {
            Some(crate::store::request_id_upgrade::UpgradeNoticeAdmission::Task(mut row)) => {
                row.tries = row.tries.checked_add(1).ok_or(StoreError::StateInvariant)?;
                row.status =
                    crate::store::request_id_upgrade::TaskAnnouncementStatus::Admitted(id.clone());
                commit.mutate(self.request_id_upgrade.task_announcements, row)
            }
            Some(crate::store::request_id_upgrade::UpgradeNoticeAdmission::Assignment(mut row)) => {
                row.tries = row.tries.checked_add(1).ok_or(StoreError::StateInvariant)?;
                row.status =
                    crate::store::request_id_upgrade::TaskAnnouncementStatus::Admitted(id.clone());
                commit.mutate(self.request_id_upgrade.upgrade_assignments, row)
            }
            None => commit,
        };
        self.engine.commit_atomic(commit)?;
        self.launch_changes.announce();
        Ok(signal_flow::PrivateSendResponse::SendAccepted(
            SendAcceptance {
                logical_delivery_id: id,
                request_id,
                delivery_cursor: next,
                delivery_state: DeliveryState::Queued,
            },
        ))
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
                    archived: None,
                    admission: current.admission.clone(),
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
        let counters = self.engine.match_records(QueryPlan::key(
            self.logical_deliveries.counter,
            RecordKey::new(COUNTER_KEY),
        ))?;
        let highwater = counters
            .records()
            .first()
            .map(|counter| counter.next_cursor.saturating_sub(1))
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
                .filter(|r| {
                    r.archived.is_none()
                        && r.snapshot.sent_by == signal_flow::SentBy::Sender(sender.clone())
                })
                .map(|r| r.snapshot.clone())
                .collect()
        };
        snapshots.sort_by_key(|snapshot| snapshot.delivery_cursor);
        Ok(SendObservation {
            store_identity: self.store_identity()?,
            delivery_cursor: highwater,
            delivery_snapshot_vector: snapshots,
        })
    }
    fn read_message_delivery_snapshots(
        &self,
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
        let counters = self.engine.match_records(QueryPlan::key(
            self.logical_deliveries.counter,
            RecordKey::new(COUNTER_KEY),
        ))?;
        let highwater = counters
            .records()
            .first()
            .map(|counter| counter.next_cursor.saturating_sub(1))
            .unwrap_or(0);
        let admitted: std::collections::HashSet<_> = current
            .records()
            .iter()
            .filter(|row| row.admission == DeliveryAdmission::Message)
            .map(|row| row.snapshot.logical_delivery_id.clone())
            .collect();
        let mut snapshots: Vec<DeliverySnapshot> = if let Some(cursor) = after_cursor {
            events
                .records()
                .iter()
                .filter(|row| {
                    row.snapshot.delivery_cursor > cursor
                        && admitted.contains(&row.snapshot.logical_delivery_id)
                })
                .map(|row| row.snapshot.clone())
                .collect()
        } else {
            current
                .records()
                .iter()
                .filter(|row| row.archived.is_none() && row.admission == DeliveryAdmission::Message)
                .map(|row| row.snapshot.clone())
                .collect()
        };
        snapshots.sort_by_key(|snapshot| snapshot.delivery_cursor);
        Ok(SendObservation {
            store_identity: self.store_identity()?,
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

/// The logical delivery ids: a store that has allocated none begins at 1.
pub(crate) struct LogicalDeliveryIds<'store>(pub(crate) &'store FlowStore);

#[cfg(test)]
pub(crate) trait HoldsLogicalDeliveryGateForTest {
    fn with_logical_delivery_gate<T>(&self, body: impl FnOnce() -> T) -> T;
    fn logical_delivery_gate_would_block(&self) -> bool;
}

#[cfg(test)]
impl HoldsLogicalDeliveryGateForTest for FlowStore {
    fn with_logical_delivery_gate<T>(&self, body: impl FnOnce() -> T) -> T {
        let _guard = self
            .logical_deliveries
            .write_gate
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        body()
    }

    fn logical_delivery_gate_would_block(&self) -> bool {
        matches!(
            self.logical_deliveries.write_gate.try_lock(),
            Err(std::sync::TryLockError::WouldBlock)
        )
    }
}

impl super::counters::CountsIds for LogicalDeliveryIds<'_> {
    fn hold_writes(&self) -> Option<std::sync::MutexGuard<'_, ()>> {
        Some(
            self.0
                .logical_deliveries
                .write_gate
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()),
        )
    }

    fn id_counter(&self) -> Result<super::counters::IdCounter, StoreError> {
        let allocation = self.0.allocate_logical_cursor()?;
        Ok(super::counters::IdCounter {
            next: allocation.cursor,
            written: allocation.existing,
        })
    }

    fn ids_recorded(&self) -> Result<bool, StoreError> {
        let deliveries = &self.0.logical_deliveries;
        Ok(!self
            .0
            .engine
            .match_records(QueryPlan::all(deliveries.events))?
            .records()
            .is_empty()
            || !self
                .0
                .engine
                .match_records(QueryPlan::all(deliveries.current))?
                .records()
                .is_empty())
    }

    fn declare_next_id(
        &self,
        commit: AtomicCommit,
        counter: &super::counters::IdCounter,
        next: i64,
    ) -> Result<AtomicCommit, StoreError> {
        let row = LogicalDeliveryCounter { next_cursor: next };
        Ok(if counter.written {
            commit.mutate(self.0.logical_deliveries.counter, row)
        } else {
            commit.assert(self.0.logical_deliveries.counter, row)
        })
    }
}
