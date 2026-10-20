//! Durable accounting around handing one queued request to a native seat.
//!
//! Preparing is committed before native I/O, with the native history anchor
//! (Claude's transcript cursor, Codex's newest turn) the hand-over is later
//! reconciled against.  A prepared attempt is never implicitly reusable after
//! recovery: recovery marks it uncertain.  Only an exactly correlated native
//! receipt makes an attempt settleable, and settlement removes the
//! still-oldest queue entry in the same commit.  Only native history that
//! proves the request never reached the seat ends an attempt Absent, with
//! that proof recorded, and only then may the request be handed over again.

use super::*;
use meta_signal_flow::{
    AbsenceProof, QueueResolutionOutcome, QueueResolutionRecord, ResolveQueueDeliveryRejected_Data,
    Undecided,
};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use signal_flow::{Address, HarnessKind, QueuedRequest};

const DELIVERY_COUNTER_TABLE_NAME: TableName = TableName::new("flow_nexus_queue_delivery_counter");
const DELIVERY_REQUEST_TABLE_NAME: TableName = TableName::new("flow_nexus_queue_delivery_requests");
const DELIVERY_ATTEMPT_TABLE_NAME: TableName = TableName::new("flow_nexus_queue_delivery_attempts");
const COUNTER_KEY: &str = "counter";

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct DeliveryCounter {
    next_request: u64,
    next_attempt: u64,
}

impl EngineRecord for DeliveryCounter {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(COUNTER_KEY)
    }
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
enum RequestState {
    Queued,
    Settled,
    /// Its attempt was proved never to have reached the seat; the request
    /// is still queued and a later attempt carries it.
    Unsent,
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct StoredDeliveryRequest {
    request_id: String,
    address: Address,
    queued: QueuedRequest,
    state: RequestState,
}

impl EngineRecord for StoredDeliveryRequest {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.request_id.clone())
    }
}

impl MemoryDefinition for StoredDeliveryRequest {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(3)
    }

    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label("flow-nexus-queue-delivery-request-v3")
    }
}

/// A delivery request as Flow 0.35.17 to 0.37.2 kept it, before `SentBy`
/// named Flow. The variant is appended, so its bytes read as the current
/// request; the v2-to-v3 upgrade moves it unchanged.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct StoredDeliveryRequestV2 {
    request_id: String,
    address: Address,
    queued: QueuedRequest,
    state: RequestState,
}

impl EngineRecord for StoredDeliveryRequestV2 {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.request_id.clone())
    }
}

impl MemoryDefinition for StoredDeliveryRequestV2 {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(2)
    }

    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label("flow-nexus-queue-delivery-request-v2")
    }
}

impl UpgradeFrom<StoredDeliveryRequestV2> for StoredDeliveryRequest {
    type Refusal = std::convert::Infallible;

    fn upgrade_from(previous: StoredDeliveryRequestV2) -> Decision<Self, Self::Refusal> {
        Decision::Ready(Self {
            request_id: previous.request_id,
            address: previous.address,
            queued: previous.queued,
            state: previous.state,
        })
    }
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub struct NativeDeliveryReceipt {
    pub native_request_id: u64,
    pub thread_id: String,
    pub turn_id: String,
    /// The native response exactly as observed, not a locally inferred
    /// acknowledgement.
    pub native_receipt: String,
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub struct NativeTranscriptCursor {
    pub device: u64,
    pub inode: u64,
    pub offset: u64,
}

/// Where native history stood before the hand-over, so what came after it
/// can be read for this attempt and nothing earlier.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub enum NativeHistoryAnchor {
    /// The Claude transcript file and its length before the text was typed.
    ClaudeTranscript(NativeTranscriptCursor),
    /// The Codex thread's newest turn before `turn/start`; None when the
    /// thread had no turn.
    CodexTurns(Option<String>),
}

/// Why an attempt was ended without reaching the seat: the kind of proof,
/// and the native observation that gave it.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub struct AbsenceRecord {
    pub proof: AbsenceProof,
    pub observation: String,
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub enum QueueDeliveryPhase {
    Prepared,
    /// Native input may have reached the seat and no exact receipt is
    /// held. It is never sent again; the reason names why the last
    /// reconciliation could not decide, None before one ran.
    Uncertain(Option<Undecided>),
    ReceiptRecorded(NativeDeliveryReceipt),
    Settled(NativeDeliveryReceipt),
    /// Native history proved the request never reached the seat.
    Absent(AbsenceRecord),
    /// The owner resolved an Uncertain attempt: who, why and when are
    /// recorded with the outcome.
    Resolved(QueueResolutionRecord),
}

pub trait EndsQueueDeliveryPhase {
    /// Settled, Absent and Resolved attempts hold no request back.
    fn is_finished(&self) -> bool;
}

impl EndsQueueDeliveryPhase for QueueDeliveryPhase {
    fn is_finished(&self) -> bool {
        matches!(self, Self::Settled(_) | Self::Absent(_) | Self::Resolved(_))
    }
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub struct QueueDeliveryAttempt {
    pub request_id: String,
    pub attempt_id: String,
    pub native_request_id: u64,
    pub flow_id: String,
    pub thread_id: String,
    pub harness_kind: HarnessKind,
    pub history_anchor: NativeHistoryAnchor,
    pub address: Address,
    pub queued: QueuedRequest,
    pub phase: QueueDeliveryPhase,
}

impl EngineRecord for QueueDeliveryAttempt {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.attempt_id.clone())
    }
}

impl MemoryDefinition for QueueDeliveryAttempt {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(5)
    }

    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label("flow-nexus-queue-delivery-attempt-v5")
    }
}

/// A delivery attempt as Flow 0.35.17 to 0.37.2 kept it, before `SentBy`
/// named Flow. The variant is appended, so its bytes read as the current
/// attempt; the v4-to-v5 upgrade moves it unchanged.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub struct QueueDeliveryAttemptV4 {
    request_id: String,
    attempt_id: String,
    native_request_id: u64,
    flow_id: String,
    thread_id: String,
    harness_kind: HarnessKind,
    history_anchor: NativeHistoryAnchor,
    address: Address,
    queued: QueuedRequest,
    phase: QueueDeliveryPhase,
}

impl EngineRecord for QueueDeliveryAttemptV4 {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.attempt_id.clone())
    }
}

impl MemoryDefinition for QueueDeliveryAttemptV4 {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(4)
    }

    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label("flow-nexus-queue-delivery-attempt-v4")
    }
}

impl UpgradeFrom<QueueDeliveryAttemptV4> for QueueDeliveryAttempt {
    type Refusal = std::convert::Infallible;

    fn upgrade_from(previous: QueueDeliveryAttemptV4) -> Decision<Self, Self::Refusal> {
        Decision::Ready(Self {
            request_id: previous.request_id,
            attempt_id: previous.attempt_id,
            native_request_id: previous.native_request_id,
            flow_id: previous.flow_id,
            thread_id: previous.thread_id,
            harness_kind: previous.harness_kind,
            history_anchor: previous.history_anchor,
            address: previous.address,
            queued: previous.queued,
            phase: previous.phase,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PrepareQueueDelivery {
    Empty,
    Prepared(QueueDeliveryAttempt),
    /// An attempt for this address is already outstanding.  The caller must
    /// inspect its phase; this is never permission to repeat native I/O.
    Outstanding(QueueDeliveryAttempt),
}

pub(crate) struct QueueDeliveryTables {
    counter: TableReference<DeliveryCounter>,
    requests: TableReference<StoredDeliveryRequest>,
    attempts: TableReference<QueueDeliveryAttempt>,
}

pub(crate) type QueueDeliveryDescriptors = (
    TableDescriptor<DeliveryCounter>,
    TableDescriptor<StoredDeliveryRequest>,
    TableDescriptor<QueueDeliveryAttempt>,
);

pub(crate) trait DeclaresQueueDeliveryTables: Sized {
    fn descriptors() -> QueueDeliveryDescriptors;
    fn register(
        engine: &mut Engine,
        descriptors: QueueDeliveryDescriptors,
    ) -> Result<Self, StoreError>;
    /// A store before 0.38.0 keeps v2 requests: each is carried unchanged.
    fn upgraded_requests(
        engine: &mut Engine,
        descriptor: TableDescriptor<StoredDeliveryRequest>,
    ) -> Result<TableReference<StoredDeliveryRequest>, StoreError> {
        match engine.upgrade_table::<StoredDeliveryRequestV2, StoredDeliveryRequest>(descriptor) {
            UpgradeOutcome::Upgraded(receipt) => Ok(receipt.table()),
            UpgradeOutcome::AlreadyCurrent(table) => Ok(table),
            UpgradeOutcome::Refused(refusal) => match *refusal.reason() {},
            UpgradeOutcome::Failed(failure) => {
                Err(StoreError::QueueDeliveryUpgrade(format!("{failure:?}")))
            }
        }
    }
    /// A store before 0.38.0 keeps v4 attempts: each is carried unchanged.
    fn upgraded_attempts(
        engine: &mut Engine,
        descriptor: TableDescriptor<QueueDeliveryAttempt>,
    ) -> Result<TableReference<QueueDeliveryAttempt>, StoreError> {
        match engine.upgrade_table::<QueueDeliveryAttemptV4, QueueDeliveryAttempt>(descriptor) {
            UpgradeOutcome::Upgraded(receipt) => Ok(receipt.table()),
            UpgradeOutcome::AlreadyCurrent(table) => Ok(table),
            UpgradeOutcome::Refused(refusal) => match *refusal.reason() {},
            UpgradeOutcome::Failed(failure) => {
                Err(StoreError::QueueDeliveryUpgrade(format!("{failure:?}")))
            }
        }
    }
}

impl DeclaresQueueDeliveryTables for QueueDeliveryTables {
    fn descriptors() -> QueueDeliveryDescriptors {
        (
            TableDescriptor::new(
                DELIVERY_COUNTER_TABLE_NAME,
                FamilyName::new("flow-nexus-queue-delivery-counter"),
                SchemaHash::for_label("flow-nexus-queue-delivery-counter-v1"),
            ),
            TableDescriptor::new(
                DELIVERY_REQUEST_TABLE_NAME,
                FamilyName::new("flow-nexus-queue-delivery-request"),
                StoredDeliveryRequest::schema_hash(),
            )
            .with_predecessor::<StoredDeliveryRequestV2>(),
            TableDescriptor::new(
                DELIVERY_ATTEMPT_TABLE_NAME,
                FamilyName::new("flow-nexus-queue-delivery-attempt"),
                QueueDeliveryAttempt::schema_hash(),
            )
            .with_predecessor::<QueueDeliveryAttemptV4>(),
        )
    }

    fn register(
        engine: &mut Engine,
        descriptors: QueueDeliveryDescriptors,
    ) -> Result<Self, StoreError> {
        Ok(Self {
            counter: engine.register_table(descriptors.0)?,
            requests: Self::upgraded_requests(engine, descriptors.1)?,
            attempts: Self::upgraded_attempts(engine, descriptors.2)?,
        })
    }
}

/// Durable queue/native-delivery accounting.  Callers serialize preparation
/// with queue mutation (the Nexus dispatch gate does so in production).
pub trait AccountsQueueDelivery {
    /// Gives the oldest queued request distinct durable request, attempt and
    /// native request identities before any native send occurs.
    fn prepare_oldest_delivery(
        &self,
        address: &Address,
        flow_id: &str,
        thread_id: &str,
        harness_kind: HarnessKind,
        history_anchor: NativeHistoryAnchor,
    ) -> Result<PrepareQueueDelivery, StoreError>;

    /// Records the exact native response only when all correlation fields
    /// match the prepared attempt.
    fn record_delivery_receipt(
        &self,
        attempt_id: &str,
        receipt: NativeDeliveryReceipt,
    ) -> Result<bool, StoreError>;

    /// Atomically removes the request if it remains oldest and retains the
    /// settled attempt, so a duplicate completion is idempotent.
    fn settle_recorded_delivery(&self, attempt_id: &str) -> Result<bool, StoreError>;

    /// Native submission returned without an exactly correlated receipt.
    /// The request stays queued and this attempt can only be reconciled,
    /// never submitted again by assumption.
    fn mark_delivery_uncertain(&self, attempt_id: &str) -> Result<bool, StoreError>;

    /// A reconciliation read native history and could not decide: the
    /// attempt stays Uncertain, naming why.
    fn mark_delivery_undecided(
        &self,
        attempt_id: &str,
        undecided: Undecided,
    ) -> Result<bool, StoreError>;

    /// Native history proved the request never reached the seat: the
    /// attempt ends Absent with its proof, its request row is Unsent, and
    /// the request, still queued, may be prepared again.
    fn mark_delivery_absent(
        &self,
        attempt_id: &str,
        absence: AbsenceRecord,
    ) -> Result<bool, StoreError>;

    /// The owner's resolution of one Uncertain attempt. Delivered removes
    /// the still-oldest request from the queue, Absent leaves it queued for
    /// a new attempt; either way the attempt ends Resolved with `record`,
    /// in the same commit. Refused, with nothing written, when the attempt
    /// is unknown, is not Uncertain, is no longer the address's oldest
    /// request, or the reason is empty.
    fn resolve_uncertain_delivery(
        &self,
        attempt_id: &str,
        record: QueueResolutionRecord,
    ) -> Result<Result<QueueDeliveryAttempt, ResolveQueueDeliveryRejected_Data>, StoreError>;

    /// The address's one attempt that still holds its oldest request back,
    /// if any.
    fn outstanding_delivery(
        &self,
        address: &Address,
    ) -> Result<Option<QueueDeliveryAttempt>, StoreError>;

    /// Every attempt of the address that did not settle, Absent ones
    /// included, oldest first.
    fn unsettled_deliveries(
        &self,
        address: &Address,
    ) -> Result<Vec<QueueDeliveryAttempt>, StoreError>;

    /// On process recovery, makes every receipt-less prepared attempt
    /// explicitly uncertain.  Receipt-recorded attempts remain settleable.
    fn recover_outstanding_deliveries(&self) -> Result<Vec<QueueDeliveryAttempt>, StoreError>;

    fn queue_delivery_attempt(
        &self,
        attempt_id: &str,
    ) -> Result<Option<QueueDeliveryAttempt>, StoreError>;
}

trait ReadsQueueDelivery {
    fn delivery_attempts(&self) -> Result<Vec<QueueDeliveryAttempt>, StoreError>;
    fn outstanding_for(
        &self,
        address: &Address,
    ) -> Result<Option<QueueDeliveryAttempt>, StoreError>;
}

impl ReadsQueueDelivery for FlowStore {
    fn delivery_attempts(&self) -> Result<Vec<QueueDeliveryAttempt>, StoreError> {
        Ok(self
            .engine
            .match_records(QueryPlan::all(self.queue_delivery.attempts))?
            .records()
            .to_vec())
    }

    fn outstanding_for(
        &self,
        address: &Address,
    ) -> Result<Option<QueueDeliveryAttempt>, StoreError> {
        let mut matching = self
            .delivery_attempts()?
            .into_iter()
            .filter(|attempt| &attempt.address == address && !attempt.phase.is_finished());
        let found = matching.next();
        if matching.next().is_some() {
            return Err(StoreError::StateInvariant);
        }
        Ok(found)
    }
}

impl AccountsQueueDelivery for FlowStore {
    fn prepare_oldest_delivery(
        &self,
        address: &Address,
        flow_id: &str,
        thread_id: &str,
        harness_kind: HarnessKind,
        history_anchor: NativeHistoryAnchor,
    ) -> Result<PrepareQueueDelivery, StoreError> {
        if let Some(attempt) = self.outstanding_for(address)? {
            return Ok(PrepareQueueDelivery::Outstanding(attempt));
        }
        let Some(stored) = self.stored_metaflow(address)? else {
            return Ok(PrepareQueueDelivery::Empty);
        };
        let Some(queued) = stored.metaflow.queue.first().cloned() else {
            return Ok(PrepareQueueDelivery::Empty);
        };
        if flow_id.is_empty()
            || thread_id.is_empty()
            || stored.metaflow.state != flow_ethos::State::Awake(flow_id.to_owned())
        {
            return Err(StoreError::StateInvariant);
        }

        let counters = self
            .engine
            .match_records(QueryPlan::key(
                self.queue_delivery.counter,
                RecordKey::new(COUNTER_KEY),
            ))?
            .records()
            .to_vec();
        let (current, new_counter) = match counters.as_slice() {
            [] => (
                None,
                DeliveryCounter {
                    next_request: 2,
                    // Codex reserves the first two app-server request ids
                    // while opening and resuming a connection.
                    next_attempt: 4,
                },
            ),
            [counter] => (
                Some(counter.clone()),
                DeliveryCounter {
                    next_request: counter
                        .next_request
                        .checked_add(1)
                        .ok_or(StoreError::StateInvariant)?,
                    next_attempt: counter
                        .next_attempt
                        .checked_add(1)
                        .ok_or(StoreError::StateInvariant)?,
                },
            ),
            _ => return Err(StoreError::StateInvariant),
        };
        let request_number = current.as_ref().map_or(1, |value| value.next_request);
        let attempt_number = current.as_ref().map_or(3, |value| value.next_attempt);
        let request_id = format!("queue-request-{request_number:016x}");
        let attempt_id = format!("queue-attempt-{attempt_number:016x}");
        let native_request_id = attempt_number;
        let request = StoredDeliveryRequest {
            request_id: request_id.clone(),
            address: address.clone(),
            queued: queued.clone(),
            state: RequestState::Queued,
        };
        let attempt = QueueDeliveryAttempt {
            request_id,
            attempt_id,
            native_request_id,
            flow_id: flow_id.to_owned(),
            thread_id: thread_id.to_owned(),
            harness_kind,
            history_anchor,
            address: address.clone(),
            queued,
            phase: QueueDeliveryPhase::Prepared,
        };
        let commit = self.engine.begin_atomic_commit();
        let commit = if current.is_some() {
            commit.mutate(self.queue_delivery.counter, new_counter)
        } else {
            commit.assert(self.queue_delivery.counter, new_counter)
        };
        self.engine.commit_atomic(
            commit
                .assert(self.queue_delivery.requests, request)
                .assert(self.queue_delivery.attempts, attempt.clone()),
        )?;
        Ok(PrepareQueueDelivery::Prepared(attempt))
    }

    fn record_delivery_receipt(
        &self,
        attempt_id: &str,
        receipt: NativeDeliveryReceipt,
    ) -> Result<bool, StoreError> {
        let Some(mut attempt) = self.queue_delivery_attempt(attempt_id)? else {
            return Ok(false);
        };
        if attempt.native_request_id != receipt.native_request_id
            || attempt.thread_id != receipt.thread_id
            || receipt.turn_id.is_empty()
            || receipt.native_receipt.is_empty()
        {
            return Ok(false);
        }
        match &attempt.phase {
            QueueDeliveryPhase::Prepared | QueueDeliveryPhase::Uncertain(_) => {
                attempt.phase = QueueDeliveryPhase::ReceiptRecorded(receipt);
                self.engine.mutate_keyed(KeyedMutation::new(
                    self.queue_delivery.attempts,
                    RecordKey::new(attempt_id),
                    attempt,
                ))?;
                Ok(true)
            }
            QueueDeliveryPhase::ReceiptRecorded(recorded)
            | QueueDeliveryPhase::Settled(recorded) => Ok(recorded == &receipt),
            QueueDeliveryPhase::Absent(_) | QueueDeliveryPhase::Resolved(_) => {
                Err(StoreError::StateInvariant)
            }
        }
    }

    fn settle_recorded_delivery(&self, attempt_id: &str) -> Result<bool, StoreError> {
        let Some(mut attempt) = self.queue_delivery_attempt(attempt_id)? else {
            return Ok(false);
        };
        if matches!(&attempt.phase, QueueDeliveryPhase::Settled(_)) {
            return Ok(true);
        }
        let QueueDeliveryPhase::ReceiptRecorded(receipt) = &attempt.phase else {
            return Ok(false);
        };
        let receipt = receipt.clone();
        let Some(mut metaflow) = self.stored_metaflow(&attempt.address)? else {
            return Ok(false);
        };
        if metaflow.metaflow.queue.first() != Some(&attempt.queued) {
            return Ok(false);
        }
        let requests = self
            .engine
            .match_records(QueryPlan::key(
                self.queue_delivery.requests,
                RecordKey::new(attempt.request_id.clone()),
            ))?
            .records()
            .to_vec();
        let [request] = requests.as_slice() else {
            return Err(StoreError::StateInvariant);
        };
        if request.address != attempt.address
            || request.queued != attempt.queued
            || request.state != RequestState::Queued
        {
            return Err(StoreError::StateInvariant);
        }
        let mut request = request.clone();
        request.state = RequestState::Settled;
        metaflow.metaflow.queue.remove(0);
        attempt.phase = QueueDeliveryPhase::Settled(receipt);
        self.engine.commit_atomic(
            self.engine
                .begin_atomic_commit()
                .mutate(self.metaflows, metaflow)
                .mutate(self.queue_delivery.requests, request)
                .mutate(self.queue_delivery.attempts, attempt),
        )?;
        Ok(true)
    }

    fn mark_delivery_uncertain(&self, attempt_id: &str) -> Result<bool, StoreError> {
        let Some(mut attempt) = self.queue_delivery_attempt(attempt_id)? else {
            return Ok(false);
        };
        match attempt.phase {
            QueueDeliveryPhase::Prepared => {
                attempt.phase = QueueDeliveryPhase::Uncertain(None);
                self.engine.mutate_keyed(KeyedMutation::new(
                    self.queue_delivery.attempts,
                    RecordKey::new(attempt_id),
                    attempt,
                ))?;
                Ok(true)
            }
            QueueDeliveryPhase::Uncertain(_) => Ok(true),
            QueueDeliveryPhase::ReceiptRecorded(_)
            | QueueDeliveryPhase::Settled(_)
            | QueueDeliveryPhase::Absent(_)
            | QueueDeliveryPhase::Resolved(_) => Ok(false),
        }
    }

    fn mark_delivery_undecided(
        &self,
        attempt_id: &str,
        undecided: Undecided,
    ) -> Result<bool, StoreError> {
        let Some(mut attempt) = self.queue_delivery_attempt(attempt_id)? else {
            return Ok(false);
        };
        let marked = QueueDeliveryPhase::Uncertain(Some(undecided));
        if attempt.phase == marked {
            return Ok(true);
        }
        if !matches!(
            attempt.phase,
            QueueDeliveryPhase::Prepared | QueueDeliveryPhase::Uncertain(_)
        ) {
            return Ok(false);
        }
        attempt.phase = marked;
        self.engine.mutate_keyed(KeyedMutation::new(
            self.queue_delivery.attempts,
            RecordKey::new(attempt_id),
            attempt,
        ))?;
        Ok(true)
    }

    fn mark_delivery_absent(
        &self,
        attempt_id: &str,
        absence: AbsenceRecord,
    ) -> Result<bool, StoreError> {
        let Some(mut attempt) = self.queue_delivery_attempt(attempt_id)? else {
            return Ok(false);
        };
        match &attempt.phase {
            QueueDeliveryPhase::Prepared | QueueDeliveryPhase::Uncertain(_) => {}
            QueueDeliveryPhase::Absent(recorded) => return Ok(recorded == &absence),
            QueueDeliveryPhase::ReceiptRecorded(_)
            | QueueDeliveryPhase::Settled(_)
            | QueueDeliveryPhase::Resolved(_) => {
                return Ok(false);
            }
        }
        let requests = self
            .engine
            .match_records(QueryPlan::key(
                self.queue_delivery.requests,
                RecordKey::new(attempt.request_id.clone()),
            ))?
            .records()
            .to_vec();
        let [request] = requests.as_slice() else {
            return Err(StoreError::StateInvariant);
        };
        if request.state != RequestState::Queued {
            return Err(StoreError::StateInvariant);
        }
        let mut request = request.clone();
        request.state = RequestState::Unsent;
        attempt.phase = QueueDeliveryPhase::Absent(absence);
        self.engine.commit_atomic(
            self.engine
                .begin_atomic_commit()
                .mutate(self.queue_delivery.requests, request)
                .mutate(self.queue_delivery.attempts, attempt),
        )?;
        Ok(true)
    }

    fn resolve_uncertain_delivery(
        &self,
        attempt_id: &str,
        record: QueueResolutionRecord,
    ) -> Result<Result<QueueDeliveryAttempt, ResolveQueueDeliveryRejected_Data>, StoreError> {
        use ResolveQueueDeliveryRejected_Data as Rejected;
        let Some(mut attempt) = self.queue_delivery_attempt(attempt_id)? else {
            return Ok(Err(Rejected::UnknownAttempt));
        };
        if !matches!(attempt.phase, QueueDeliveryPhase::Uncertain(_)) {
            return Ok(Err(Rejected::NotUncertain));
        }
        if record.queue_resolution_reason.trim().is_empty() {
            return Ok(Err(Rejected::EmptyReason));
        }
        let Some(mut metaflow) = self.stored_metaflow(&attempt.address)? else {
            return Ok(Err(Rejected::NotOldest));
        };
        if metaflow.metaflow.queue.first() != Some(&attempt.queued) {
            return Ok(Err(Rejected::NotOldest));
        }
        let requests = self
            .engine
            .match_records(QueryPlan::key(
                self.queue_delivery.requests,
                RecordKey::new(attempt.request_id.clone()),
            ))?
            .records()
            .to_vec();
        let [request] = requests.as_slice() else {
            return Err(StoreError::StateInvariant);
        };
        if request.address != attempt.address
            || request.queued != attempt.queued
            || request.state != RequestState::Queued
        {
            return Err(StoreError::StateInvariant);
        }
        let mut request = request.clone();
        let commit = self.engine.begin_atomic_commit();
        let commit = match record.queue_resolution_outcome {
            QueueResolutionOutcome::Delivered => {
                request.state = RequestState::Settled;
                metaflow.metaflow.queue.remove(0);
                commit.mutate(self.metaflows, metaflow)
            }
            QueueResolutionOutcome::Absent => {
                request.state = RequestState::Unsent;
                commit
            }
        };
        attempt.phase = QueueDeliveryPhase::Resolved(record);
        self.engine.commit_atomic(
            commit
                .mutate(self.queue_delivery.requests, request)
                .mutate(self.queue_delivery.attempts, attempt.clone()),
        )?;
        Ok(Ok(attempt))
    }

    fn outstanding_delivery(
        &self,
        address: &Address,
    ) -> Result<Option<QueueDeliveryAttempt>, StoreError> {
        self.outstanding_for(address)
    }

    fn unsettled_deliveries(
        &self,
        address: &Address,
    ) -> Result<Vec<QueueDeliveryAttempt>, StoreError> {
        let mut unsettled = self
            .delivery_attempts()?
            .into_iter()
            .filter(|attempt| {
                &attempt.address == address
                    && !matches!(attempt.phase, QueueDeliveryPhase::Settled(_))
            })
            .collect::<Vec<_>>();
        // Attempt ids are fixed-width hex of one counter: their order is
        // the order they were prepared in.
        unsettled.sort_by(|left, right| left.attempt_id.cmp(&right.attempt_id));
        Ok(unsettled)
    }

    fn recover_outstanding_deliveries(&self) -> Result<Vec<QueueDeliveryAttempt>, StoreError> {
        let mut outstanding = Vec::new();
        for mut attempt in self.delivery_attempts()? {
            match attempt.phase.clone() {
                QueueDeliveryPhase::Prepared => {
                    attempt.phase = QueueDeliveryPhase::Uncertain(None);
                    self.engine.mutate_keyed(KeyedMutation::new(
                        self.queue_delivery.attempts,
                        RecordKey::new(attempt.attempt_id.clone()),
                        attempt.clone(),
                    ))?;
                    outstanding.push(attempt);
                }
                QueueDeliveryPhase::Uncertain(_) | QueueDeliveryPhase::ReceiptRecorded(_) => {
                    outstanding.push(attempt);
                }
                QueueDeliveryPhase::Settled(_)
                | QueueDeliveryPhase::Absent(_)
                | QueueDeliveryPhase::Resolved(_) => {}
            }
        }
        Ok(outstanding)
    }

    fn queue_delivery_attempt(
        &self,
        attempt_id: &str,
    ) -> Result<Option<QueueDeliveryAttempt>, StoreError> {
        let rows = self
            .engine
            .match_records(QueryPlan::key(
                self.queue_delivery.attempts,
                RecordKey::new(attempt_id),
            ))?
            .records()
            .to_vec();
        match rows.as_slice() {
            [] => Ok(None),
            [attempt] => Ok(Some(attempt.clone())),
            _ => Err(StoreError::StateInvariant),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::metaflow::QueuesFlowRequest;
    use crate::store::metaflow::SeedsMetaflow;
    use crate::store::metaflow::SleepsMetaflows;
    use flow_defaults::DefaultConfiguration;
    use flow_ethos::{Aspect, Layer, Metaflow, Request, SentBy, State};
    use std::path::{Path, PathBuf};

    fn address() -> Address {
        Address {
            aspect: Aspect::Mind,
            topic: "delivery-accounting".into(),
            layer: Layer::Secondary,
        }
    }

    fn queued(body: &str) -> QueuedRequest {
        QueuedRequest {
            sent_by: SentBy::Sender(Address {
                aspect: Aspect::Psyche,
                topic: "core".into(),
                layer: Layer::Secondary,
            }),
            request: Request::Order(body.into()),
        }
    }

    fn defaults(directory: &Path) -> DefaultConfiguration {
        DefaultConfiguration {
            home: directory.join("home"),
            runtime_directory: directory.join("runtime"),
        }
    }

    fn seeded(path: &Path, defaults: &DefaultConfiguration) -> FlowStore {
        let store = FlowStore::open_seeded(path, defaults).expect("store opens");
        store.seed_metaflow(Metaflow {
            address: address(),
            state: State::Awake("flow-delivery".into()),
            past: vec![],
            queue: vec![queued("first"), queued("second")],
        });
        store
    }

    struct Fixture {
        _directory: tempfile::TempDir,
        path: PathBuf,
        defaults: DefaultConfiguration,
    }

    impl Fixture {
        fn new() -> (Self, FlowStore) {
            let directory = tempfile::tempdir().expect("private fixture");
            let path = directory.path().join("flow.sema");
            let defaults = defaults(directory.path());
            let store = seeded(&path, &defaults);
            (
                Self {
                    _directory: directory,
                    path,
                    defaults,
                },
                store,
            )
        }

        fn reopen(&self) -> FlowStore {
            FlowStore::open_seeded(&self.path, &self.defaults).expect("store reopens")
        }
    }

    fn receipt(attempt: &QueueDeliveryAttempt) -> NativeDeliveryReceipt {
        NativeDeliveryReceipt {
            native_request_id: attempt.native_request_id,
            thread_id: "thread-7".into(),
            turn_id: "turn-9".into(),
            native_receipt: r#"{"id":3,"result":{}}"#.into(),
        }
    }

    #[test]
    fn prepare_is_durable_and_recovery_never_reauthorizes_a_send() {
        let (fixture, store) = Fixture::new();
        let PrepareQueueDelivery::Prepared(prepared) = store
            .prepare_oldest_delivery(
                &address(),
                "flow-delivery",
                "thread-7",
                HarnessKind::Claude,
                NativeHistoryAnchor::ClaudeTranscript(NativeTranscriptCursor {
                    device: 1,
                    inode: 2,
                    offset: 3,
                }),
            )
            .unwrap()
        else {
            panic!("oldest request prepared")
        };
        assert_ne!(prepared.request_id, prepared.attempt_id);
        assert!(prepared.native_request_id >= 3);
        assert_eq!(prepared.queued, queued("first"));
        drop(store);

        let reopened = fixture.reopen();
        let recovered = reopened.recover_outstanding_deliveries().unwrap();
        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0].attempt_id, prepared.attempt_id);
        assert_eq!(recovered[0].phase, QueueDeliveryPhase::Uncertain(None));
        assert_eq!(
            reopened
                .prepare_oldest_delivery(
                    &address(),
                    "flow-delivery",
                    "thread-7",
                    HarnessKind::Claude,
                    NativeHistoryAnchor::ClaudeTranscript(NativeTranscriptCursor {
                        device: 1,
                        inode: 2,
                        offset: 3,
                    })
                )
                .unwrap(),
            PrepareQueueDelivery::Outstanding(recovered[0].clone())
        );
        assert!(
            !reopened
                .settle_recorded_delivery(&prepared.attempt_id)
                .unwrap()
        );
        assert_eq!(
            reopened.metaflow(&address()).unwrap().unwrap().queue,
            vec![queued("first"), queued("second")]
        );
    }

    #[test]
    fn native_acceptance_after_a_crash_reconciles_without_another_attempt() {
        let (fixture, store) = Fixture::new();
        let PrepareQueueDelivery::Prepared(prepared) = store
            .prepare_oldest_delivery(
                &address(),
                "flow-delivery",
                "thread-7",
                HarnessKind::Claude,
                NativeHistoryAnchor::ClaudeTranscript(NativeTranscriptCursor {
                    device: 4,
                    inode: 5,
                    offset: 6,
                }),
            )
            .unwrap()
        else {
            panic!("oldest request prepared")
        };
        drop(store);

        let reopened = fixture.reopen();
        let recovered = reopened.recover_outstanding_deliveries().unwrap();
        let [uncertain] = recovered.as_slice() else {
            panic!("one uncertain attempt survives")
        };
        assert_eq!(uncertain.attempt_id, prepared.attempt_id);
        assert_eq!(uncertain.phase, QueueDeliveryPhase::Uncertain(None));
        assert!(
            reopened
                .record_delivery_receipt(&prepared.attempt_id, receipt(&prepared))
                .unwrap()
        );
        assert!(
            reopened
                .settle_recorded_delivery(&prepared.attempt_id)
                .unwrap()
        );
        assert_eq!(
            reopened.metaflow(&address()).unwrap().unwrap().queue,
            vec![queued("second")]
        );
        let PrepareQueueDelivery::Prepared(next) = reopened
            .prepare_oldest_delivery(
                &address(),
                "flow-delivery",
                "thread-7",
                HarnessKind::Claude,
                NativeHistoryAnchor::ClaudeTranscript(NativeTranscriptCursor {
                    device: 4,
                    inode: 5,
                    offset: 7,
                }),
            )
            .unwrap()
        else {
            panic!("next request gets its own attempt")
        };
        assert_ne!(next.attempt_id, prepared.attempt_id);
        assert_eq!(next.queued, queued("second"));
    }

    #[test]
    fn exact_receipt_survives_restart_and_settles_oldest_once() {
        let (fixture, store) = Fixture::new();
        let PrepareQueueDelivery::Prepared(prepared) = store
            .prepare_oldest_delivery(
                &address(),
                "flow-delivery",
                "thread-7",
                HarnessKind::Codex,
                NativeHistoryAnchor::CodexTurns(None),
            )
            .unwrap()
        else {
            panic!("oldest request prepared")
        };
        let exact = receipt(&prepared);
        let mut wrong = exact.clone();
        wrong.native_request_id = 99;
        assert!(
            !store
                .record_delivery_receipt(&prepared.attempt_id, wrong)
                .unwrap()
        );
        assert!(
            store
                .record_delivery_receipt(&prepared.attempt_id, exact.clone())
                .unwrap()
        );
        drop(store);

        let reopened = fixture.reopen();
        // Opening reconciles the durable native receipt and settles the
        // local queue without another native submission.
        assert!(
            reopened
                .recover_outstanding_deliveries()
                .unwrap()
                .is_empty()
        );
        assert!(
            reopened
                .settle_recorded_delivery(&prepared.attempt_id)
                .unwrap()
        );
        // A duplicate completion observes the retained settled attempt and
        // neither removes nor sends the next request.
        assert!(
            reopened
                .record_delivery_receipt(&prepared.attempt_id, exact.clone())
                .unwrap()
        );
        assert!(
            reopened
                .settle_recorded_delivery(&prepared.attempt_id)
                .unwrap()
        );
        assert_eq!(
            reopened.metaflow(&address()).unwrap().unwrap().queue,
            vec![queued("second")]
        );
        assert_eq!(
            reopened
                .queue_delivery_attempt(&prepared.attempt_id)
                .unwrap()
                .unwrap()
                .phase,
            QueueDeliveryPhase::Settled(exact)
        );
    }

    #[test]
    fn proven_absence_authorises_a_new_attempt_and_undecided_stays_uncertain() {
        let (fixture, store) = Fixture::new();
        let PrepareQueueDelivery::Prepared(first) = store
            .prepare_oldest_delivery(
                &address(),
                "flow-delivery",
                "thread-7",
                HarnessKind::Codex,
                NativeHistoryAnchor::CodexTurns(Some("turn-1".into())),
            )
            .unwrap()
        else {
            panic!("oldest request prepared")
        };
        assert!(store.mark_delivery_uncertain(&first.attempt_id).unwrap());
        assert!(
            store
                .mark_delivery_undecided(&first.attempt_id, Undecided::SeatNotResting)
                .unwrap()
        );
        drop(store);
        let store = fixture.reopen();
        let [recovered] = store
            .recover_outstanding_deliveries()
            .unwrap()
            .try_into()
            .unwrap();
        assert_eq!(
            recovered.phase,
            QueueDeliveryPhase::Uncertain(Some(Undecided::SeatNotResting))
        );
        // Undecided is never permission to send again.
        assert!(matches!(
            store
                .prepare_oldest_delivery(
                    &address(),
                    "flow-delivery",
                    "thread-7",
                    HarnessKind::Codex,
                    NativeHistoryAnchor::CodexTurns(Some("turn-1".into())),
                )
                .unwrap(),
            PrepareQueueDelivery::Outstanding(_)
        ));
        let absence = AbsenceRecord {
            proof: AbsenceProof::HistoryAbsent,
            observation: "idle; no turn after turn-1 carries the request".into(),
        };
        assert!(
            store
                .mark_delivery_absent(&first.attempt_id, absence.clone())
                .unwrap()
        );
        assert!(
            store
                .mark_delivery_absent(&first.attempt_id, absence.clone())
                .unwrap()
        );
        assert!(
            !store
                .record_delivery_receipt(&first.attempt_id, receipt(&first))
                .is_ok_and(|recorded| recorded)
        );
        assert_eq!(
            store.metaflow(&address()).unwrap().unwrap().queue,
            vec![queued("first"), queued("second")]
        );
        let PrepareQueueDelivery::Prepared(second) = store
            .prepare_oldest_delivery(
                &address(),
                "flow-delivery",
                "thread-7",
                HarnessKind::Codex,
                NativeHistoryAnchor::CodexTurns(Some("turn-1".into())),
            )
            .unwrap()
        else {
            panic!("a proven absence authorises a new attempt")
        };
        assert_ne!(second.attempt_id, first.attempt_id);
        assert_ne!(second.native_request_id, first.native_request_id);
        assert_eq!(second.queued, queued("first"));
        let unsettled = store.unsettled_deliveries(&address()).unwrap();
        assert_eq!(
            unsettled
                .iter()
                .map(|attempt| (attempt.attempt_id.clone(), attempt.phase.clone()))
                .collect::<Vec<_>>(),
            vec![
                (
                    first.attempt_id.clone(),
                    QueueDeliveryPhase::Absent(absence)
                ),
                (second.attempt_id.clone(), QueueDeliveryPhase::Prepared),
            ]
        );
    }

    #[test]
    fn settlement_refuses_changed_oldest_and_preserves_sender_and_order() {
        let (_fixture, store) = Fixture::new();
        let PrepareQueueDelivery::Prepared(prepared) = store
            .prepare_oldest_delivery(
                &address(),
                "flow-delivery",
                "thread-7",
                HarnessKind::Codex,
                NativeHistoryAnchor::CodexTurns(None),
            )
            .unwrap()
        else {
            panic!("oldest request prepared")
        };
        store.seed_metaflow(Metaflow {
            address: address(),
            state: State::Awake("flow-delivery".into()),
            past: vec![],
            queue: vec![queued("second"), queued("first")],
        });
        assert!(
            store
                .record_delivery_receipt(&prepared.attempt_id, receipt(&prepared))
                .unwrap()
        );
        assert!(
            !store
                .settle_recorded_delivery(&prepared.attempt_id)
                .unwrap()
        );
        assert_eq!(
            store.metaflow(&address()).unwrap().unwrap().queue,
            vec![queued("second"), queued("first")]
        );
    }

    fn resolution(outcome: QueueResolutionOutcome, reason: &str) -> QueueResolutionRecord {
        QueueResolutionRecord {
            queue_resolution_outcome: outcome,
            resolver_identity: meta_signal_flow::ResolverIdentity {
                process_identity: meta_signal_flow::ProcessIdentity {
                    process_id: 4242,
                    process_user_id: 1001,
                    process_start_token: "777".into(),
                },
                flow_id_option: Some("aa034f".into()),
            },
            queue_resolution_reason: reason.into(),
            unix_seconds: 1_790_000_123,
        }
    }

    fn prepare_claude(store: &FlowStore) -> QueueDeliveryAttempt {
        let PrepareQueueDelivery::Prepared(attempt) = store
            .prepare_oldest_delivery(
                &address(),
                "flow-delivery",
                "thread-7",
                HarnessKind::Claude,
                NativeHistoryAnchor::ClaudeTranscript(NativeTranscriptCursor {
                    device: 1,
                    inode: 2,
                    offset: 3,
                }),
            )
            .unwrap()
        else {
            panic!("oldest request prepared")
        };
        assert!(store.mark_delivery_uncertain(&attempt.attempt_id).unwrap());
        assert!(
            store
                .mark_delivery_undecided(&attempt.attempt_id, Undecided::AbsenceUnprovable)
                .unwrap()
        );
        attempt
    }

    #[test]
    fn resolved_delivered_leaves_the_queue_with_its_record_and_survives_restart() {
        let (fixture, store) = Fixture::new();
        let attempt = prepare_claude(&store);
        let record = resolution(QueueResolutionOutcome::Delivered, "seen in the pane");
        let resolved = store
            .resolve_uncertain_delivery(&attempt.attempt_id, record.clone())
            .unwrap()
            .unwrap();
        assert_eq!(resolved.phase, QueueDeliveryPhase::Resolved(record.clone()));
        drop(store);

        let store = fixture.reopen();
        assert_eq!(
            store.metaflow(&address()).unwrap().unwrap().queue,
            vec![queued("second")]
        );
        assert_eq!(
            store
                .queue_delivery_attempt(&attempt.attempt_id)
                .unwrap()
                .unwrap()
                .phase,
            QueueDeliveryPhase::Resolved(record.clone())
        );
        assert!(store.outstanding_delivery(&address()).unwrap().is_none());
        assert_eq!(
            store
                .unsettled_deliveries(&address())
                .unwrap()
                .into_iter()
                .map(|attempt| attempt.phase)
                .collect::<Vec<_>>(),
            vec![QueueDeliveryPhase::Resolved(record)]
        );
        // The next request gets its own attempt.
        let PrepareQueueDelivery::Prepared(next) = store
            .prepare_oldest_delivery(
                &address(),
                "flow-delivery",
                "thread-7",
                HarnessKind::Claude,
                NativeHistoryAnchor::ClaudeTranscript(NativeTranscriptCursor {
                    device: 1,
                    inode: 2,
                    offset: 4,
                }),
            )
            .unwrap()
        else {
            panic!("the next request is prepared")
        };
        assert_eq!(next.queued, queued("second"));
    }

    #[test]
    fn resolved_absent_keeps_the_request_and_authorises_exactly_one_new_attempt() {
        let (fixture, store) = Fixture::new();
        let attempt = prepare_claude(&store);
        let record = resolution(QueueResolutionOutcome::Absent, "composer was empty");
        store
            .resolve_uncertain_delivery(&attempt.attempt_id, record.clone())
            .unwrap()
            .unwrap();
        drop(store);

        let store = fixture.reopen();
        assert_eq!(
            store.metaflow(&address()).unwrap().unwrap().queue,
            vec![queued("first"), queued("second")]
        );
        assert!(store.outstanding_delivery(&address()).unwrap().is_none());
        let anchor = NativeHistoryAnchor::ClaudeTranscript(NativeTranscriptCursor {
            device: 1,
            inode: 2,
            offset: 9,
        });
        let PrepareQueueDelivery::Prepared(next) = store
            .prepare_oldest_delivery(
                &address(),
                "flow-delivery",
                "thread-7",
                HarnessKind::Claude,
                anchor.clone(),
            )
            .unwrap()
        else {
            panic!("a resolution to Absent authorises a new attempt")
        };
        assert_ne!(next.attempt_id, attempt.attempt_id);
        assert_eq!(next.queued, queued("first"));
        assert!(matches!(
            store
                .prepare_oldest_delivery(
                    &address(),
                    "flow-delivery",
                    "thread-7",
                    HarnessKind::Claude,
                    anchor,
                )
                .unwrap(),
            PrepareQueueDelivery::Outstanding(_)
        ));
        assert_eq!(
            store
                .queue_delivery_attempt(&attempt.attempt_id)
                .unwrap()
                .unwrap()
                .phase,
            QueueDeliveryPhase::Resolved(record)
        );
    }

    #[test]
    fn a_resolution_is_refused_for_what_is_not_the_oldest_uncertain_attempt() {
        use ResolveQueueDeliveryRejected_Data as Rejected;
        let (_fixture, store) = Fixture::new();
        let delivered = resolution(QueueResolutionOutcome::Delivered, "seen");
        assert_eq!(
            store
                .resolve_uncertain_delivery("queue-attempt-unknown", delivered.clone())
                .unwrap(),
            Err(Rejected::UnknownAttempt)
        );

        // Prepared is not Uncertain.
        let PrepareQueueDelivery::Prepared(prepared) = store
            .prepare_oldest_delivery(
                &address(),
                "flow-delivery",
                "thread-7",
                HarnessKind::Claude,
                NativeHistoryAnchor::ClaudeTranscript(NativeTranscriptCursor {
                    device: 1,
                    inode: 2,
                    offset: 3,
                }),
            )
            .unwrap()
        else {
            panic!("oldest request prepared")
        };
        assert_eq!(
            store
                .resolve_uncertain_delivery(&prepared.attempt_id, delivered.clone())
                .unwrap(),
            Err(Rejected::NotUncertain)
        );

        // ReceiptRecorded is not Uncertain.
        assert!(
            store
                .record_delivery_receipt(&prepared.attempt_id, receipt(&prepared))
                .unwrap()
        );
        assert_eq!(
            store
                .resolve_uncertain_delivery(&prepared.attempt_id, delivered.clone())
                .unwrap(),
            Err(Rejected::NotUncertain)
        );
        assert!(
            store
                .settle_recorded_delivery(&prepared.attempt_id)
                .unwrap()
        );
        // Settled is not Uncertain.
        assert_eq!(
            store
                .resolve_uncertain_delivery(&prepared.attempt_id, delivered.clone())
                .unwrap(),
            Err(Rejected::NotUncertain)
        );

        // An empty reason is refused and nothing is written.
        let second = prepare_claude(&store);
        assert_eq!(
            store
                .resolve_uncertain_delivery(
                    &second.attempt_id,
                    resolution(QueueResolutionOutcome::Delivered, "  ")
                )
                .unwrap(),
            Err(Rejected::EmptyReason)
        );
        assert_eq!(
            store
                .queue_delivery_attempt(&second.attempt_id)
                .unwrap()
                .unwrap()
                .phase,
            QueueDeliveryPhase::Uncertain(Some(Undecided::AbsenceUnprovable))
        );

        // An attempt that is no longer the oldest is refused, the queue
        // untouched.
        store.seed_metaflow(Metaflow {
            address: address(),
            state: State::Awake("flow-delivery".into()),
            past: vec![],
            queue: vec![queued("other"), queued("second")],
        });
        assert_eq!(
            store
                .resolve_uncertain_delivery(&second.attempt_id, delivered.clone())
                .unwrap(),
            Err(Rejected::NotOldest)
        );
        assert_eq!(
            store.metaflow(&address()).unwrap().unwrap().queue,
            vec![queued("other"), queued("second")]
        );

        // Resolved is not Uncertain: a resolution is made once.
        store.seed_metaflow(Metaflow {
            address: address(),
            state: State::Awake("flow-delivery".into()),
            past: vec![],
            queue: vec![queued("second")],
        });
        store
            .resolve_uncertain_delivery(&second.attempt_id, delivered.clone())
            .unwrap()
            .unwrap();
        assert_eq!(
            store
                .resolve_uncertain_delivery(&second.attempt_id, delivered)
                .unwrap(),
            Err(Rejected::NotUncertain)
        );
    }

    /// A store Flow 0.37.2 left, its metaflow at v3 and its queue
    /// deliveries at v2 and v4, opens with every row carried unchanged
    /// under the identities that admit a Flow sender.
    #[test]
    fn a_store_before_flow_senders_opens_with_its_queue_carried() {
        let (fixture, store) = Fixture::new();
        let PrepareQueueDelivery::Prepared(prepared) = store
            .prepare_oldest_delivery(
                &address(),
                "flow-delivery",
                "thread-7",
                HarnessKind::Claude,
                NativeHistoryAnchor::ClaudeTranscript(NativeTranscriptCursor {
                    device: 1,
                    inode: 2,
                    offset: 3,
                }),
            )
            .unwrap()
        else {
            panic!("oldest request prepared")
        };
        let metaflow = store.metaflow(&address()).unwrap().expect("seeded");
        let requests = store
            .engine
            .match_records(QueryPlan::all(store.queue_delivery.requests))
            .unwrap()
            .records()
            .to_vec();
        assert_eq!(requests.len(), 1);
        drop(store);

        let tables = [
            "flow_nexus_metaflows",
            "flow_nexus_queue_delivery_requests",
            "flow_nexus_queue_delivery_attempts",
        ];
        let database = redb::Database::open(&fixture.path).expect("store opens directly");
        let write = database.begin_write().expect("catalog transaction starts");
        {
            let mut catalog = write
                .open_table(super::super::SEMA_CATALOG_TABLE)
                .expect("catalog opens");
            for table in tables {
                catalog.remove(table).expect("family removes");
            }
        }
        for table in tables {
            write
                .delete_table(redb::TableDefinition::<&str, &[u8]>::new(table))
                .expect("rows drop");
        }
        write.commit().expect("catalog commits");
        drop(database);
        {
            let mut engine = Engine::open(EngineOpen::new(&fixture.path, SchemaVersion::new(1)))
                .expect("0.37.2 store opens");
            let metaflows = engine
                .register_table(TableDescriptor::new(
                    TableName::new("flow_nexus_metaflows"),
                    FamilyName::new("flow-nexus-metaflow"),
                    SchemaHash::for_label("flow-nexus-metaflow-v3"),
                ))
                .expect("v3 metaflow family registers");
            engine
                .assert(Assertion::new(
                    metaflows,
                    super::super::StoredMetaflowV3 {
                        metaflow: metaflow.clone(),
                    },
                ))
                .expect("v3 metaflow persists");
            let old_requests = engine
                .register_table(TableDescriptor::new(
                    DELIVERY_REQUEST_TABLE_NAME,
                    FamilyName::new("flow-nexus-queue-delivery-request"),
                    SchemaHash::for_label("flow-nexus-queue-delivery-request-v2"),
                ))
                .expect("v2 request family registers");
            for request in &requests {
                engine
                    .assert(Assertion::new(
                        old_requests,
                        StoredDeliveryRequestV2 {
                            request_id: request.request_id.clone(),
                            address: request.address.clone(),
                            queued: request.queued.clone(),
                            state: request.state.clone(),
                        },
                    ))
                    .expect("v2 request persists");
            }
            let old_attempts = engine
                .register_table(TableDescriptor::new(
                    DELIVERY_ATTEMPT_TABLE_NAME,
                    FamilyName::new("flow-nexus-queue-delivery-attempt"),
                    SchemaHash::for_label("flow-nexus-queue-delivery-attempt-v4"),
                ))
                .expect("v4 attempt family registers");
            engine
                .assert(Assertion::new(
                    old_attempts,
                    QueueDeliveryAttemptV4 {
                        request_id: prepared.request_id.clone(),
                        attempt_id: prepared.attempt_id.clone(),
                        native_request_id: prepared.native_request_id,
                        flow_id: prepared.flow_id.clone(),
                        thread_id: prepared.thread_id.clone(),
                        harness_kind: prepared.harness_kind.clone(),
                        history_anchor: prepared.history_anchor.clone(),
                        address: prepared.address.clone(),
                        queued: prepared.queued.clone(),
                        phase: prepared.phase.clone(),
                    },
                ))
                .expect("v4 attempt persists");
        }

        let reopened = fixture.reopen();
        assert_eq!(reopened.metaflow(&address()).unwrap(), Some(metaflow));
        // Carried as it was; the open makes a receipt-less attempt Uncertain.
        assert_eq!(
            reopened
                .queue_delivery_attempt(&prepared.attempt_id)
                .unwrap(),
            Some(QueueDeliveryAttempt {
                phase: QueueDeliveryPhase::Uncertain(None),
                ..prepared.clone()
            })
        );
        assert_eq!(
            reopened
                .engine
                .match_records(QueryPlan::all(reopened.queue_delivery.requests))
                .unwrap()
                .records()
                .to_vec(),
            requests
        );
        // A request Flow raised is kept, and kept across an open.
        let raised = QueuedRequest {
            sent_by: SentBy::Flow,
            request: Request::Order("investigate".into()),
        };
        assert!(
            reopened
                .queue_flow_request(&address(), raised.clone())
                .unwrap()
        );
        drop(reopened);
        let queue = fixture
            .reopen()
            .metaflow(&address())
            .unwrap()
            .unwrap()
            .queue;
        assert_eq!(queue.last(), Some(&raised));
    }
}
