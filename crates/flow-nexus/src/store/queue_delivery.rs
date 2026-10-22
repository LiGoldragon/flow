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

use super::logical_delivery::RecordsLogicalDeliveries;
use super::*;
use flow_ethos::Request;
use meta_signal_flow::{
    AbsenceProof, Legacy_Data, QueueEvidence, QueueResolutionOutcome, QueueResolutionRecord,
    ResolveQueueDeliveryRejected_Data, Undecided,
};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use sema_engine::AtomicCommit;
use signal_flow::{Address, DeliveryState, HarnessKind, NativeReceipt, RetryCause, SentBy};

trait CarriesLegacySender {
    fn with_legacy_sender(self, sent_by: SentBy) -> QueueEvidence;
}

impl CarriesLegacySender for Request {
    fn with_legacy_sender(self, sent_by: SentBy) -> QueueEvidence {
        QueueEvidence::Legacy(Legacy_Data {
            sent_by,
            request: self,
        })
    }
}
trait CommitsAttemptState {
    fn commit_attempt_state(
        &self,
        evidence: &QueueEvidence,
        state: DeliveryState,
        commit: AtomicCommit,
    ) -> Result<(), StoreError>;
}

impl CommitsAttemptState for FlowStore {
    fn commit_attempt_state(
        &self,
        evidence: &QueueEvidence,
        state: DeliveryState,
        commit: AtomicCommit,
    ) -> Result<(), StoreError> {
        if let QueueEvidence::Logical(entry) = evidence {
            self.commit_logical_transition(&entry.logical_delivery_id, state, commit)?;
        } else {
            // Legacy attempts retain history without a fabricated lifecycle link.
            self.engine.commit_atomic(commit)?;
        }
        Ok(())
    }
}

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
    /// Native history could not resolve this request. It remains in the
    /// delivery journal as Uncertain, but is no longer an active queue item
    /// and must never be handed over again automatically.
    Parked,
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
enum RequestStateV4 {
    Queued,
    Settled,
    Unsent,
}

trait UpgradesRequestState {
    fn upgrade(self) -> RequestState;
}

impl UpgradesRequestState for RequestStateV4 {
    fn upgrade(self) -> RequestState {
        match self {
            Self::Queued => RequestState::Queued,
            Self::Settled => RequestState::Settled,
            Self::Unsent => RequestState::Unsent,
        }
    }
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct StoredDeliveryRequest {
    request_id: String,
    address: Address,
    queue_evidence: QueueEvidence,
    state: RequestState,
}

impl EngineRecord for StoredDeliveryRequest {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.request_id.clone())
    }
}

impl MemoryDefinition for StoredDeliveryRequest {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(5)
    }

    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label("flow-nexus-queue-delivery-request-v5")
    }
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct StoredDeliveryRequestV4 {
    request_id: String,
    address: Address,
    queue_evidence: QueueEvidence,
    state: RequestStateV4,
}

impl EngineRecord for StoredDeliveryRequestV4 {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.request_id.clone())
    }
}

impl MemoryDefinition for StoredDeliveryRequestV4 {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(4)
    }

    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label("flow-nexus-queue-delivery-request-v4")
    }
}

impl UpgradeFrom<StoredDeliveryRequestV4> for StoredDeliveryRequest {
    type Refusal = std::convert::Infallible;

    fn upgrade_from(previous: StoredDeliveryRequestV4) -> Decision<Self, Self::Refusal> {
        Decision::Ready(Self {
            request_id: previous.request_id,
            address: previous.address,
            queue_evidence: previous.queue_evidence,
            state: previous.state.upgrade(),
        })
    }
}

/// A delivery request as Flow 0.35.17 to 0.37.2 kept it, before `SentBy`
/// named Flow. The variant is appended, so its bytes read as the current
/// request; the v2-to-v3 upgrade moves it unchanged.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct StoredDeliveryRequestV2 {
    request_id: String,
    address: Address,
    queued: super::QueuedRequestBeforeFlow,
    state: RequestStateV4,
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
            queue_evidence: previous.queued.request.with_legacy_sender(
                match previous.queued.sent_by {
                    super::SentByBeforeFlow::Sender(sender) => SentBy::Sender(sender),
                    super::SentByBeforeFlow::Unknown => SentBy::Unknown,
                },
            ),
            state: previous.state.upgrade(),
        })
    }
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct StoredDeliveryRequestV3 {
    request_id: String,
    address: Address,
    queued: super::QueuedRequestBeforeLogicalId,
    state: RequestStateV4,
}
impl EngineRecord for StoredDeliveryRequestV3 {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.request_id.clone())
    }
}
impl MemoryDefinition for StoredDeliveryRequestV3 {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(3)
    }
    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label("flow-nexus-queue-delivery-request-v3")
    }
}
impl UpgradeFrom<StoredDeliveryRequestV3> for StoredDeliveryRequest {
    type Refusal = std::convert::Infallible;
    fn upgrade_from(previous: StoredDeliveryRequestV3) -> Decision<Self, Self::Refusal> {
        Decision::Ready(Self {
            request_id: previous.request_id,
            address: previous.address,
            queue_evidence: previous
                .queued
                .request
                .with_legacy_sender(previous.queued.sent_by),
            state: previous.state.upgrade(),
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
    pub queue_evidence: QueueEvidence,
    pub phase: QueueDeliveryPhase,
}

impl EngineRecord for QueueDeliveryAttempt {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.attempt_id.clone())
    }
}

impl MemoryDefinition for QueueDeliveryAttempt {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(6)
    }

    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label("flow-nexus-queue-delivery-attempt-v6")
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
    queued: super::QueuedRequestBeforeFlow,
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
            queue_evidence: previous.queued.request.with_legacy_sender(
                match previous.queued.sent_by {
                    super::SentByBeforeFlow::Sender(sender) => SentBy::Sender(sender),
                    super::SentByBeforeFlow::Unknown => SentBy::Unknown,
                },
            ),
            phase: previous.phase,
        })
    }
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub struct QueueDeliveryAttemptV5 {
    request_id: String,
    attempt_id: String,
    native_request_id: u64,
    flow_id: String,
    thread_id: String,
    harness_kind: HarnessKind,
    history_anchor: NativeHistoryAnchor,
    address: Address,
    queued: super::QueuedRequestBeforeLogicalId,
    phase: QueueDeliveryPhase,
}
impl EngineRecord for QueueDeliveryAttemptV5 {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.attempt_id.clone())
    }
}
impl MemoryDefinition for QueueDeliveryAttemptV5 {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(5)
    }
    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label("flow-nexus-queue-delivery-attempt-v5")
    }
}
impl UpgradeFrom<QueueDeliveryAttemptV5> for QueueDeliveryAttempt {
    type Refusal = std::convert::Infallible;
    fn upgrade_from(previous: QueueDeliveryAttemptV5) -> Decision<Self, Self::Refusal> {
        Decision::Ready(Self {
            request_id: previous.request_id,
            attempt_id: previous.attempt_id,
            native_request_id: previous.native_request_id,
            flow_id: previous.flow_id,
            thread_id: previous.thread_id,
            harness_kind: previous.harness_kind,
            history_anchor: previous.history_anchor,
            address: previous.address,
            queue_evidence: previous
                .queued
                .request
                .with_legacy_sender(previous.queued.sent_by),
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
        let from_v4 = descriptor.clone();
        match engine
            .upgrade_table::<StoredDeliveryRequestV4, StoredDeliveryRequest>(descriptor.clone())
        {
            UpgradeOutcome::Upgraded(receipt) => return Ok(receipt.table()),
            UpgradeOutcome::AlreadyCurrent(table) => return Ok(table),
            UpgradeOutcome::Refused(refusal) => match *refusal.reason() {},
            UpgradeOutcome::Failed(failure)
                if failure.phase() == sema_engine::StoragePhase::Admission => {}
            UpgradeOutcome::Failed(failure) => {
                return Err(StoreError::QueueDeliveryUpgrade(format!("{failure:?}")));
            }
        }
        let from_v2 = from_v4;
        match engine.upgrade_table::<StoredDeliveryRequestV3, StoredDeliveryRequest>(descriptor) {
            UpgradeOutcome::Upgraded(receipt) => Ok(receipt.table()),
            UpgradeOutcome::AlreadyCurrent(table) => Ok(table),
            UpgradeOutcome::Refused(refusal) => match *refusal.reason() {},
            UpgradeOutcome::Failed(failure)
                if failure.phase() == sema_engine::StoragePhase::Admission =>
            {
                match engine
                    .upgrade_table::<StoredDeliveryRequestV2, StoredDeliveryRequest>(from_v2)
                {
                    UpgradeOutcome::Upgraded(receipt) => Ok(receipt.table()),
                    UpgradeOutcome::AlreadyCurrent(table) => Ok(table),
                    UpgradeOutcome::Refused(refusal) => match *refusal.reason() {},
                    UpgradeOutcome::Failed(failure) => {
                        Err(StoreError::QueueDeliveryUpgrade(format!("{failure:?}")))
                    }
                }
            }
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
        let from_v4 = descriptor.clone();
        match engine.upgrade_table::<QueueDeliveryAttemptV5, QueueDeliveryAttempt>(descriptor) {
            UpgradeOutcome::Upgraded(receipt) => Ok(receipt.table()),
            UpgradeOutcome::AlreadyCurrent(table) => Ok(table),
            UpgradeOutcome::Refused(refusal) => match *refusal.reason() {},
            UpgradeOutcome::Failed(failure)
                if failure.phase() == sema_engine::StoragePhase::Admission =>
            {
                match engine.upgrade_table::<QueueDeliveryAttemptV4, QueueDeliveryAttempt>(from_v4)
                {
                    UpgradeOutcome::Upgraded(receipt) => Ok(receipt.table()),
                    UpgradeOutcome::AlreadyCurrent(table) => Ok(table),
                    UpgradeOutcome::Refused(refusal) => match *refusal.reason() {},
                    UpgradeOutcome::Failed(failure) => {
                        Err(StoreError::QueueDeliveryUpgrade(format!("{failure:?}")))
                    }
                }
            }
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
            .with_predecessor::<StoredDeliveryRequestV2>()
            .with_predecessor::<StoredDeliveryRequestV3>()
            .with_predecessor::<StoredDeliveryRequestV4>(),
            TableDescriptor::new(
                DELIVERY_ATTEMPT_TABLE_NAME,
                FamilyName::new("flow-nexus-queue-delivery-attempt"),
                QueueDeliveryAttempt::schema_hash(),
            )
            .with_predecessor::<QueueDeliveryAttemptV4>()
            .with_predecessor::<QueueDeliveryAttemptV5>(),
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

    /// Removes an unresolved request from active queue processing without
    /// changing its Uncertain delivery record or permitting automatic retry.
    fn park_uncertain_delivery(&self, attempt_id: &str) -> Result<bool, StoreError>;

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

    /// The owner's resolution of one Uncertain attempt. Only Absent is
    /// actionable: it retains the request and authorises a retry. Delivered
    /// requires an exact native receipt and cannot be asserted by an owner.
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
    fn request_state(&self, request_id: &str) -> Result<Option<RequestState>, StoreError>;
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
        let attempts = self.delivery_attempts()?;
        let mut matching = attempts.into_iter().filter_map(|attempt| {
            if &attempt.address != address || attempt.phase.is_finished() {
                return None;
            }
            match self.request_state(&attempt.request_id) {
                Ok(Some(RequestState::Parked)) => None,
                Ok(_) => Some(Ok(attempt)),
                Err(error) => Some(Err(error)),
            }
        });
        let first = matching.next().transpose()?;
        if matching.next().transpose()?.is_some() {
            return Err(StoreError::StateInvariant);
        }
        return Ok(first);
    }

    fn request_state(&self, request_id: &str) -> Result<Option<RequestState>, StoreError> {
        let rows = self.engine.match_records(QueryPlan::key(
            self.queue_delivery.requests,
            RecordKey::new(request_id),
        ))?;
        Ok(rows.records().first().map(|request| request.state.clone()))
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
        let attempt_number = current.as_ref().map_or(3, |value| value.next_attempt);
        let request_id = queued.logical_delivery_id.clone();
        let attempt_id = format!("queue-attempt-{attempt_number:016x}");
        let native_request_id = attempt_number;
        let request = StoredDeliveryRequest {
            request_id: request_id.clone(),
            address: address.clone(),
            queue_evidence: QueueEvidence::Logical(queued.clone()),
            state: RequestState::Queued,
        };
        let logical_delivery_id = queued.logical_delivery_id.clone();
        let old_request = self.engine.match_records(QueryPlan::key(
            self.queue_delivery.requests,
            RecordKey::new(request_id.clone()),
        ))?;
        let old_request = match old_request.records() {
            [] => None,
            [old] if old.address == *address && old.queue_evidence == request.queue_evidence => {
                Some(old.clone())
            }
            _ => return Err(StoreError::StateInvariant),
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
            queue_evidence: QueueEvidence::Logical(queued),
            phase: QueueDeliveryPhase::Prepared,
        };
        let commit = self.engine.begin_atomic_commit();
        let commit = if current.is_some() {
            commit.mutate(self.queue_delivery.counter, new_counter)
        } else {
            commit.assert(self.queue_delivery.counter, new_counter)
        };
        let commit = if old_request.is_some() {
            commit.mutate(self.queue_delivery.requests, request)
        } else {
            commit.assert(self.queue_delivery.requests, request)
        };
        self.commit_logical_transition(
            &logical_delivery_id,
            DeliveryState::Attempting,
            commit.assert(self.queue_delivery.attempts, attempt.clone()),
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
        let QueueEvidence::Logical(logical_entry) = &attempt.queue_evidence else {
            return Ok(false);
        };
        let logical_id = logical_entry.logical_delivery_id.clone();
        let native_receipt = NativeReceipt {
            harness_kind: attempt.harness_kind.clone(),
            native_session_id: attempt.thread_id.clone(),
            native_prompt_id: receipt.turn_id.clone(),
        };
        let is_parked = self.request_state(&attempt.request_id)? == Some(RequestState::Parked);
        if !is_parked && metaflow.metaflow.queue.first() != Some(logical_entry) {
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
            || request.queue_evidence != attempt.queue_evidence
            || (!is_parked && request.state != RequestState::Queued)
            || (is_parked && request.state != RequestState::Parked)
        {
            return Err(StoreError::StateInvariant);
        }
        let mut request = request.clone();
        request.state = RequestState::Settled;
        if !is_parked {
            metaflow.metaflow.queue.remove(0);
        }
        attempt.phase = QueueDeliveryPhase::Settled(receipt);
        let commit = self
            .engine
            .begin_atomic_commit()
            .mutate(self.metaflows, metaflow)
            .mutate(self.queue_delivery.requests, request)
            .mutate(self.queue_delivery.attempts, attempt);
        self.commit_logical_transition(
            &logical_id,
            DeliveryState::Delivered(native_receipt),
            commit,
        )?;
        Ok(true)
    }

    fn park_uncertain_delivery(&self, attempt_id: &str) -> Result<bool, StoreError> {
        let Some(attempt) = self.queue_delivery_attempt(attempt_id)? else {
            return Ok(false);
        };
        if !matches!(attempt.phase, QueueDeliveryPhase::Uncertain(_)) {
            return Ok(false);
        }
        let QueueEvidence::Logical(entry) = &attempt.queue_evidence else {
            return Ok(false);
        };
        let Some(mut metaflow) = self.stored_metaflow(&attempt.address)? else {
            return Ok(false);
        };
        if metaflow.metaflow.queue.first() != Some(entry) {
            return Ok(false);
        }
        let rows = self.engine.match_records(QueryPlan::key(
            self.queue_delivery.requests,
            RecordKey::new(attempt.request_id.clone()),
        ))?;
        let [request] = rows.records() else {
            return Err(StoreError::StateInvariant);
        };
        if request.address != attempt.address
            || request.queue_evidence != attempt.queue_evidence
            || request.state != RequestState::Queued
        {
            return Err(StoreError::StateInvariant);
        }
        let mut parked = request.clone();
        parked.state = RequestState::Parked;
        metaflow.metaflow.queue.remove(0);
        let commit = self
            .engine
            .begin_atomic_commit()
            .mutate(self.metaflows, metaflow)
            .mutate(self.queue_delivery.requests, parked);
        self.engine.commit_atomic(commit)?;
        Ok(true)
    }

    fn mark_delivery_uncertain(&self, attempt_id: &str) -> Result<bool, StoreError> {
        let Some(mut attempt) = self.queue_delivery_attempt(attempt_id)? else {
            return Ok(false);
        };
        let evidence = attempt.queue_evidence.clone();
        match &attempt.phase {
            QueueDeliveryPhase::Prepared => {
                attempt.phase = QueueDeliveryPhase::Uncertain(None);
                let commit = self
                    .engine
                    .begin_atomic_commit()
                    .mutate(self.queue_delivery.attempts, attempt);
                self.commit_attempt_state(&evidence, DeliveryState::Uncertain, commit)?;
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
            &attempt.phase,
            QueueDeliveryPhase::Prepared | QueueDeliveryPhase::Uncertain(_)
        ) {
            return Ok(false);
        }
        let evidence = attempt.queue_evidence.clone();
        attempt.phase = marked;
        let commit = self
            .engine
            .begin_atomic_commit()
            .mutate(self.queue_delivery.attempts, attempt);
        self.commit_attempt_state(&evidence, DeliveryState::Uncertain, commit)?;
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
        let evidence = attempt.queue_evidence.clone();
        request.state = RequestState::Unsent;
        attempt.phase = QueueDeliveryPhase::Absent(absence);
        let commit = self
            .engine
            .begin_atomic_commit()
            .mutate(self.queue_delivery.requests, request)
            .mutate(self.queue_delivery.attempts, attempt);
        self.commit_attempt_state(
            &evidence,
            DeliveryState::Retryable(RetryCause::HistoryAbsent),
            commit,
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
        if let QueueDeliveryPhase::Settled(receipt) = &attempt.phase {
            return Ok(Err(Rejected::AlreadyDelivered(NativeReceipt {
                harness_kind: attempt.harness_kind.clone(),
                native_session_id: attempt.thread_id.clone(),
                native_prompt_id: receipt.turn_id.clone(),
            })));
        }
        if let QueueDeliveryPhase::Resolved(prior) = &attempt.phase {
            if prior.queue_resolution_outcome == QueueResolutionOutcome::Absent
                && record.queue_resolution_outcome == QueueResolutionOutcome::Absent
                && prior.resolver_identity == record.resolver_identity
                && prior.queue_resolution_reason == record.queue_resolution_reason
            {
                return Ok(Ok(attempt));
            }
            return Ok(Err(Rejected::AlreadyResolved(prior.clone())));
        }
        if !matches!(attempt.phase, QueueDeliveryPhase::Uncertain(_)) {
            return Ok(Err(Rejected::NotUncertain));
        }
        if record.queue_resolution_reason.trim().is_empty() {
            return Ok(Err(Rejected::EmptyReason));
        }
        let Some(metaflow) = self.stored_metaflow(&attempt.address)? else {
            return Ok(Err(Rejected::NotOldest));
        };
        let QueueEvidence::Logical(logical_entry) = &attempt.queue_evidence else {
            return Ok(Err(Rejected::LegacyUncorrelated));
        };
        if metaflow.metaflow.queue.first() != Some(logical_entry) {
            return Ok(Err(Rejected::NotOldest));
        }
        if record.queue_resolution_outcome == QueueResolutionOutcome::Delivered {
            return Ok(Err(Rejected::NativeReceiptRequired));
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
            || request.queue_evidence != attempt.queue_evidence
            || request.state != RequestState::Queued
        {
            return Err(StoreError::StateInvariant);
        }
        let mut request = request.clone();
        request.state = RequestState::Unsent;
        attempt.phase = QueueDeliveryPhase::Resolved(record);
        let evidence = attempt.queue_evidence.clone();
        let commit = self
            .engine
            .begin_atomic_commit()
            .mutate(self.queue_delivery.requests, request)
            .mutate(self.queue_delivery.attempts, attempt.clone());
        self.commit_attempt_state(
            &evidence,
            DeliveryState::Retryable(RetryCause::OwnerResolvedAbsent),
            commit,
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
                    let evidence = attempt.queue_evidence.clone();
                    let commit = self
                        .engine
                        .begin_atomic_commit()
                        .mutate(self.queue_delivery.attempts, attempt.clone());
                    self.commit_attempt_state(&evidence, DeliveryState::Uncertain, commit)?;
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
    use flow_ethos::{Aspect, Layer, Metaflow, QueueEntry, Request, SentBy, State};
    use std::path::{Path, PathBuf};

    fn address() -> Address {
        Address {
            aspect: Aspect::Mind,
            topic: "delivery-accounting".into(),
            layer: Layer::Secondary,
        }
    }

    fn queued(body: &str) -> QueueEntry {
        QueueEntry {
            logical_delivery_id: format!("logical-{body}"),
            sent_by: SentBy::Sender(Address {
                aspect: Aspect::Psyche,
                topic: "core".into(),
                layer: Layer::Secondary,
            }),
            delivery: flow_ethos::Delivery::Queue,
            message: flow_ethos::Message::Soft(Request::Order(body.into())),
        }
    }

    fn before_flow(entry: &QueueEntry) -> super::super::QueuedRequestBeforeFlow {
        let sent_by = match &entry.sent_by {
            SentBy::Sender(sender) => super::super::SentByBeforeFlow::Sender(sender.clone()),
            SentBy::Unknown | SentBy::Flow => super::super::SentByBeforeFlow::Unknown,
        };
        let request = match &entry.message {
            flow_ethos::Message::Soft(request)
            | flow_ethos::Message::MiddleAbrupt(request)
            | flow_ethos::Message::HardAbrupt(request) => request.clone(),
        };
        super::super::QueuedRequestBeforeFlow { sent_by, request }
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
        assert_eq!(
            prepared.queue_evidence,
            QueueEvidence::Logical(queued("first"))
        );
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
        use super::super::logical_delivery::RecordsLogicalDeliveries;
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
        let sender = match queued("first").sent_by {
            SentBy::Sender(sender) => sender,
            _ => panic!("test request has a sender"),
        };
        let observation = reopened
            .read_logical_delivery_snapshots(&sender, None)
            .unwrap();
        assert!(observation.delivery_snapshot_vector.iter().any(|snapshot| {
            snapshot.logical_delivery_id == queued("first").logical_delivery_id
                && snapshot.delivery_state == DeliveryState::Uncertain
        }));
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
        assert_eq!(
            next.queue_evidence,
            QueueEvidence::Logical(queued("second"))
        );
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
        assert_eq!(
            second.queue_evidence,
            QueueEvidence::Logical(queued("first"))
        );
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
    fn owner_delivered_resolution_requires_native_receipt_and_changes_nothing() {
        use ResolveQueueDeliveryRejected_Data as Rejected;
        let (_fixture, store) = Fixture::new();
        let attempt = prepare_claude(&store);
        let record = resolution(QueueResolutionOutcome::Delivered, "seen in the pane");
        assert_eq!(
            store
                .resolve_uncertain_delivery(&attempt.attempt_id, record)
                .unwrap(),
            Err(Rejected::NativeReceiptRequired)
        );
        assert_eq!(
            store.metaflow(&address()).unwrap().unwrap().queue,
            vec![queued("first"), queued("second")]
        );
        assert_eq!(
            store
                .queue_delivery_attempt(&attempt.attempt_id)
                .unwrap()
                .unwrap()
                .phase,
            QueueDeliveryPhase::Uncertain(Some(Undecided::AbsenceUnprovable))
        );
        assert_eq!(
            store
                .outstanding_delivery(&address())
                .unwrap()
                .map(|outstanding| outstanding.attempt_id),
            Some(attempt.attempt_id)
        );
    }

    #[test]
    fn parked_uncertain_request_unblocks_queue_and_late_receipt_still_settles() {
        let (fixture, store) = Fixture::new();
        let first = prepare_claude(&store);
        assert!(store.park_uncertain_delivery(&first.attempt_id).unwrap());
        assert_eq!(
            store.metaflow(&address()).unwrap().unwrap().queue,
            vec![queued("second")]
        );
        assert!(store.outstanding_delivery(&address()).unwrap().is_none());
        assert_eq!(
            store
                .queue_delivery_attempt(&first.attempt_id)
                .unwrap()
                .unwrap()
                .phase,
            QueueDeliveryPhase::Uncertain(Some(Undecided::AbsenceUnprovable))
        );
        drop(store);

        let reopened = fixture.reopen();
        assert_eq!(
            reopened.metaflow(&address()).unwrap().unwrap().queue,
            vec![queued("second")]
        );
        assert!(reopened.outstanding_delivery(&address()).unwrap().is_none());
        let recovered = reopened.recover_outstanding_deliveries().unwrap();
        assert_eq!(
            recovered.len(),
            1,
            "parked uncertainty remains observable after restart"
        );
        assert_eq!(recovered[0].attempt_id, first.attempt_id);
        assert_eq!(
            recovered[0].phase,
            QueueDeliveryPhase::Uncertain(Some(Undecided::AbsenceUnprovable))
        );
        let PrepareQueueDelivery::Prepared(second) = reopened
            .prepare_oldest_delivery(
                &address(),
                "flow-delivery",
                "thread-7",
                HarnessKind::Claude,
                NativeHistoryAnchor::ClaudeTranscript(NativeTranscriptCursor {
                    device: 1,
                    inode: 2,
                    offset: 9,
                }),
            )
            .unwrap()
        else {
            panic!("the later queued request is eligible")
        };
        let exact = receipt(&first);
        assert!(
            reopened
                .record_delivery_receipt(&first.attempt_id, exact)
                .unwrap()
        );
        assert!(
            reopened
                .settle_recorded_delivery(&first.attempt_id)
                .unwrap()
        );
        assert_eq!(
            reopened
                .queue_delivery_attempt(&first.attempt_id)
                .unwrap()
                .unwrap()
                .phase,
            QueueDeliveryPhase::Settled(receipt(&first))
        );
        assert_eq!(
            reopened.metaflow(&address()).unwrap().unwrap().queue,
            vec![queued("second")]
        );
        assert_eq!(
            reopened.outstanding_delivery(&address()).unwrap(),
            Some(second)
        );
    }

    #[test]
    fn owner_resolution_refuses_legacy_attempt_without_logical_lineage() {
        use ResolveQueueDeliveryRejected_Data as Rejected;
        let (_fixture, store) = Fixture::new();
        let attempt = prepare_claude(&store);
        let mut attempt = store
            .queue_delivery_attempt(&attempt.attempt_id)
            .unwrap()
            .unwrap();
        let QueueEvidence::Logical(entry) = &attempt.queue_evidence else {
            panic!("fixture initially has a logical request")
        };
        let legacy = Request::Order("first".into()).with_legacy_sender(entry.sent_by.clone());
        let requests = store
            .engine
            .match_records(QueryPlan::key(
                store.queue_delivery.requests,
                RecordKey::new(attempt.request_id.clone()),
            ))
            .unwrap()
            .records()
            .to_vec();
        let [request] = requests.as_slice() else {
            panic!("one request is prepared")
        };
        let mut request = request.clone();
        request.queue_evidence = legacy.clone();
        attempt.queue_evidence = legacy;
        attempt.phase = QueueDeliveryPhase::Uncertain(Some(Undecided::AbsenceUnprovable));
        store
            .engine
            .commit_atomic(
                store
                    .engine
                    .begin_atomic_commit()
                    .mutate(store.queue_delivery.requests, request)
                    .mutate(store.queue_delivery.attempts, attempt.clone()),
            )
            .unwrap();

        assert_eq!(
            store
                .resolve_uncertain_delivery(
                    &attempt.attempt_id,
                    resolution(QueueResolutionOutcome::Absent, "no matching receipt"),
                )
                .unwrap(),
            Err(Rejected::LegacyUncorrelated)
        );
        assert_eq!(
            store.queue_delivery_attempt(&attempt.attempt_id).unwrap(),
            Some(attempt)
        );
        assert_eq!(
            store.metaflow(&address()).unwrap().unwrap().queue,
            vec![queued("first"), queued("second")]
        );
    }

    #[test]
    fn resolved_absent_keeps_the_request_and_authorises_exactly_one_new_attempt() {
        use super::super::logical_delivery::RecordsLogicalDeliveries;
        let (fixture, store) = Fixture::new();
        let attempt = prepare_claude(&store);
        let record = resolution(QueueResolutionOutcome::Absent, "composer was empty");
        let resolved = store
            .resolve_uncertain_delivery(&attempt.attempt_id, record.clone())
            .unwrap()
            .unwrap();
        assert_eq!(resolved.phase, QueueDeliveryPhase::Resolved(record.clone()));
        let sender = match queued("first").sent_by {
            SentBy::Sender(sender) => sender,
            _ => panic!("test request has a sender"),
        };
        let before_duplicate = store
            .read_logical_delivery_snapshots(&sender, None)
            .unwrap();
        assert!(
            before_duplicate
                .delivery_snapshot_vector
                .iter()
                .any(|snapshot| {
                    snapshot.logical_delivery_id == queued("first").logical_delivery_id
                        && snapshot.delivery_state
                            == DeliveryState::Retryable(RetryCause::OwnerResolvedAbsent)
                })
        );
        assert_eq!(
            store
                .resolve_uncertain_delivery(&attempt.attempt_id, record.clone())
                .unwrap()
                .unwrap(),
            resolved,
            "same owner resolution is idempotent"
        );
        assert_eq!(
            store
                .read_logical_delivery_snapshots(&sender, None)
                .unwrap(),
            before_duplicate,
            "duplicate resolution does not allocate another cursor"
        );
        assert_eq!(
            store
                .resolve_uncertain_delivery(
                    &attempt.attempt_id,
                    resolution(QueueResolutionOutcome::Absent, "different reason"),
                )
                .unwrap(),
            Err(ResolveQueueDeliveryRejected_Data::AlreadyResolved(
                record.clone()
            ))
        );
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
        assert_eq!(next.queue_evidence, QueueEvidence::Logical(queued("first")));
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
            Err(Rejected::AlreadyDelivered(NativeReceipt {
                harness_kind: HarnessKind::Claude,
                native_session_id: "thread-7".into(),
                native_prompt_id: "turn-9".into(),
            }))
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

        // Owner Absent is resolved once and cannot be contradicted later.
        store.seed_metaflow(Metaflow {
            address: address(),
            state: State::Awake("flow-delivery".into()),
            past: vec![],
            queue: vec![queued("second")],
        });
        let absent = resolution(QueueResolutionOutcome::Absent, "it was not taken");
        let resolved = store
            .resolve_uncertain_delivery(&second.attempt_id, absent.clone())
            .unwrap()
            .unwrap();
        assert_eq!(resolved.phase, QueueDeliveryPhase::Resolved(absent.clone()));
        assert_eq!(
            store
                .resolve_uncertain_delivery(
                    &second.attempt_id,
                    resolution(QueueResolutionOutcome::Absent, "different reason"),
                )
                .unwrap(),
            Err(Rejected::AlreadyResolved(absent))
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
        let mut migrated_metaflow = metaflow.clone();
        for (index, entry) in migrated_metaflow.queue.iter_mut().enumerate() {
            entry.logical_delivery_id = format!(
                "legacy:{}:{index:016x}",
                FlowStore::address_record_key(&migrated_metaflow.address)
            );
        }
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
                        metaflow: super::super::MetaflowBeforeFlow {
                            address: metaflow.address.clone(),
                            state: metaflow.state.clone(),
                            past: metaflow.past.clone(),
                            queue: metaflow.queue.iter().map(before_flow).collect(),
                        },
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
                            queued: match &request.queue_evidence {
                                QueueEvidence::Logical(entry) => before_flow(entry),
                                QueueEvidence::Legacy(_) => unreachable!(),
                            },
                            state: match request.state {
                                RequestState::Queued => RequestStateV4::Queued,
                                RequestState::Settled => RequestStateV4::Settled,
                                RequestState::Unsent => RequestStateV4::Unsent,
                                RequestState::Parked => {
                                    unreachable!("old fixture has no parked row")
                                }
                            },
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
                        queued: match &prepared.queue_evidence {
                            QueueEvidence::Logical(entry) => before_flow(entry),
                            QueueEvidence::Legacy(_) => unreachable!(),
                        },
                        phase: prepared.phase.clone(),
                    },
                ))
                .expect("v4 attempt persists");
        }

        let reopened = fixture.reopen();
        assert_eq!(
            reopened.metaflow(&address()).unwrap(),
            Some(migrated_metaflow)
        );
        // Carried as it was; the open makes a receipt-less attempt Uncertain.
        let QueueEvidence::Logical(old_entry) = &prepared.queue_evidence else {
            unreachable!()
        };
        let old_entry = before_flow(old_entry);
        let mut migrated_attempt = prepared.clone();
        migrated_attempt.phase = QueueDeliveryPhase::Uncertain(None);
        migrated_attempt.queue_evidence =
            old_entry
                .request
                .with_legacy_sender(match old_entry.sent_by {
                    super::super::SentByBeforeFlow::Sender(sender) => SentBy::Sender(sender),
                    super::super::SentByBeforeFlow::Unknown => SentBy::Unknown,
                });
        let mut migrated_requests = requests.clone();
        for request in &mut migrated_requests {
            let QueueEvidence::Logical(entry) = &request.queue_evidence else {
                unreachable!()
            };
            let legacy = before_flow(entry);
            request.queue_evidence = legacy.request.with_legacy_sender(match legacy.sent_by {
                super::super::SentByBeforeFlow::Sender(sender) => SentBy::Sender(sender),
                super::super::SentByBeforeFlow::Unknown => SentBy::Unknown,
            });
        }
        assert_eq!(
            reopened
                .queue_delivery_attempt(&prepared.attempt_id)
                .unwrap(),
            Some(migrated_attempt)
        );
        assert_eq!(
            reopened
                .engine
                .match_records(QueryPlan::all(reopened.queue_delivery.requests))
                .unwrap()
                .records()
                .to_vec(),
            migrated_requests
        );
        // A request Flow raised is kept, and kept across an open.
        let raised = QueueEntry {
            logical_delivery_id: "flow-raised".into(),
            sent_by: SentBy::Flow,
            delivery: flow_ethos::Delivery::Queue,
            message: flow_ethos::Message::Soft(Request::Order("investigate".into())),
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
        assert_eq!(
            queue.last().map(|entry| &entry.sent_by),
            Some(&SentBy::Flow)
        );
        assert_eq!(
            queue.last().map(|entry| &entry.message),
            Some(&raised.message)
        );
    }
}
