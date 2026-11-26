//! The queue delivery attempts an earlier Nexus wrote while it reconciled
//! each hand-over against native history. A hand-over is now recorded from
//! the pane writer's own result on the request's logical delivery
//! (`logical_delivery::record_hand_over`), and no attempt is written. The
//! tables stay in the store format, read for what earlier attempts hold:
//! ReadQueueDeliveries, an owner's resolution or abandonment, a withdrawal,
//! a retired seat's settlement and an archive.

use super::attempt_membership::{KeepsAttemptMemberships, MembershipOwner};
use super::logical_delivery::{DeliveryRecipient, RecordsLogicalDeliveries};
use super::*;
pub(crate) use crate::generated::memory::{
    AbsenceRecord, DeliveryCounter, NativeDeliveryReceipt, NativeHistoryAnchor,
    NativeTranscriptCursor, QueueDeliveryAttempt, QueueDeliveryPhase, Read_Data, RequestState,
    StoredDeliveryRequest,
};
pub(crate) use crate::generated::memory::{Answer, LateReceipt, NextAttempt, NextRequest};
use meta_signal_flow::{
    AbandonQueueDeliveryRejected_Data, AbandonmentRecord, QueueDelivery, QueueDeliveryStanding,
    QueueEvidence, QueueResolutionOutcome, QueueResolutionRecord, ResolveQueueDeliveryRejection,
    WithdrawRejected_Data,
};
use sema_engine::AtomicCommit;
use signal_flow::{Address, DeliveryState, NativeReceipt, RetryCause};

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
            self.commit_logical_transition(
                &entry.logical_delivery_id,
                state,
                crate::generated::transitions::ReceiptTurn::Running,
                commit,
            )?;
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
/// The first attempt of a store that has made none: an attempt's number is
/// also its Codex app-server request id, and a connection reserves the
/// first two while opening and resuming.
const FIRST_ATTEMPT: i64 = 3;
/// The next request number a store that has made no attempt writes.
const FIRST_NEXT_REQUEST: i64 = 2;

impl EngineRecord for DeliveryCounter {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(COUNTER_KEY)
    }
}

impl EngineRecord for StoredDeliveryRequest {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.queue_request_id.0.clone())
    }
}

impl MemoryDefinition for StoredDeliveryRequest {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(9)
    }

    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label("flow-nexus-queue-delivery-request-v9")
    }
}

/// An attempt as the meta socket shows it: its standing, with the flow and
/// native session it was handed to and the request it carries.
pub trait ShowsQueueDelivery {
    /// None for a Settled attempt, which is not listed.
    fn queue_delivery(self) -> Option<QueueDelivery>;
}

impl ShowsQueueDelivery for QueueDeliveryAttempt {
    fn queue_delivery(self) -> Option<QueueDelivery> {
        let queue_delivery_standing = match self.queue_delivery_phase {
            QueueDeliveryPhase::Prepared => QueueDeliveryStanding::Prepared,
            QueueDeliveryPhase::Uncertain(undecided) => QueueDeliveryStanding::Uncertain(undecided),
            QueueDeliveryPhase::ReceiptRecorded(_) => QueueDeliveryStanding::ReceiptRecorded,
            QueueDeliveryPhase::Absent(absence) => {
                QueueDeliveryStanding::Absent(absence.absence_proof)
            }
            QueueDeliveryPhase::Resolved(record) => QueueDeliveryStanding::Resolved(record),
            QueueDeliveryPhase::Abandoned(record) => QueueDeliveryStanding::Abandoned(record),
            QueueDeliveryPhase::Settled(_) | QueueDeliveryPhase::Read { .. } => return None,
        };
        Some(QueueDelivery {
            queue_attempt_id: self.queue_attempt_id,
            flow_id: self.flow_id,
            native_session_id: signal_flow::NativeSessionId(self.thread_id.0),
            harness_kind: self.harness_kind,
            queue_evidence: self.queue_evidence,
            queue_delivery_standing,
        })
    }
}

pub trait EndsQueueDeliveryPhase {
    /// Settled, Read, Absent, Resolved and Abandoned attempts hold no request back.
    fn is_finished(&self) -> bool;
}

impl EndsQueueDeliveryPhase for QueueDeliveryPhase {
    fn is_finished(&self) -> bool {
        matches!(
            self,
            Self::Settled(_)
                | Self::Read { .. }
                | Self::Absent(_)
                | Self::Resolved(_)
                | Self::Abandoned(_)
        )
    }
}

impl EngineRecord for QueueDeliveryAttempt {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.queue_attempt_id.0.clone())
    }
}

impl MemoryDefinition for QueueDeliveryAttempt {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(11)
    }

    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label(super::delivery_attempt_upgrade::SCHEMA_V11)
    }
}

pub(crate) struct QueueDeliveryTables {
    pub(super) counter: TableReference<DeliveryCounter>,
    pub(super) requests: TableReference<StoredDeliveryRequest>,
    pub(super) attempts: TableReference<QueueDeliveryAttempt>,
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
            .with_predecessor::<super::request_id_upgrade::PriorStoredDeliveryRequest>()
            .with_predecessor::<super::living_upgrade::DeliveryRequestBeforeLiving>(),
            TableDescriptor::new(
                DELIVERY_ATTEMPT_TABLE_NAME,
                FamilyName::new("flow-nexus-queue-delivery-attempt"),
                QueueDeliveryAttempt::schema_hash(),
            )
            .with_predecessor::<super::request_id_upgrade::PriorQueueDeliveryAttempt>()
            .with_predecessor::<super::living_upgrade::DeliveryAttemptBeforeLiving>()
            .with_predecessor::<super::delivery_attempt_upgrade::QueueDeliveryAttemptV10>(),
        )
    }

    fn register(
        engine: &mut Engine,
        descriptors: QueueDeliveryDescriptors,
    ) -> Result<Self, StoreError> {
        Ok(Self {
            counter: engine.register_table(descriptors.0)?,
            requests: engine.register_table(descriptors.1)?,
            attempts: engine.register_table(descriptors.2)?,
        })
    }
}

/// Durable queue/native-delivery accounting.  Callers serialize preparation
/// with queue mutation (the Nexus dispatch gate does so in production).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RetainedLogicalRequest {
    pub(crate) address: Address,
    pub(crate) entry: flow_ethos::QueueEntry,
}

pub trait AccountsQueueDelivery {
    /// The private delivery state carried by the retained queue record.
    fn queue_request_state(&self, id: &str) -> Result<Option<RequestState>, StoreError>;

    /// Stages a state change on the existing request record. Queue producers
    /// without retained evidence acquire the same record before a write.
    fn stage_queue_request_state(
        &self,
        address: &Address,
        entry: &flow_ethos::QueueEntry,
        state: RequestState,
        commit: AtomicCommit,
    ) -> Result<AtomicCommit, StoreError>;

    /// Outstanding stock attempts, before any restart settlement changes them.
    fn unfinished_queue_attempts(&self) -> Result<Vec<QueueDeliveryAttempt>, StoreError>;

    /// Retains a stock attempt as uncertain, with no receipt to replay.
    fn stage_interrupted_queue_attempt(
        &self,
        attempt: QueueDeliveryAttempt,
        commit: AtomicCommit,
    ) -> Result<AtomicCommit, StoreError>;

    fn delivery_attempts_for(
        &self,
        logical_delivery_id: &str,
    ) -> Result<Vec<QueueDeliveryAttempt>, StoreError>;
    /// Every admission whose complete typed logical request is retained.
    /// Legacy evidence is intentionally absent: it has no authoritative
    /// logical identity or chronology for answer correlation.
    fn retained_logical_requests(&self) -> Result<Vec<RetainedLogicalRequest>, StoreError>;

    /// Adds the admission's complete request evidence to the same atomic
    /// commit as its metaflow queue and logical lifecycle records. An
    /// attempt later reuses this row; withdrawal parks it without erasing it.
    fn assert_logical_request(
        &self,
        address: &Address,
        entry: flow_ethos::QueueEntry,
        commit: AtomicCommit,
    ) -> AtomicCommit;

    /// A queue entry whose delivery policy changes in place carries its
    /// retained request evidence with it, in the caller's commit, so every
    /// later comparison of request, attempt and queue head stays exact.
    fn follow_queue_entry(
        &self,
        address: &Address,
        before: &flow_ethos::QueueEntry,
        after: &flow_ethos::QueueEntry,
        commit: AtomicCommit,
    ) -> Result<AtomicCommit, StoreError>;

    /// On open, aligns a retained request with its queue entry where the
    /// two differ only in delivery policy. Identity, order, request state
    /// and every attempt are kept as they are.
    fn align_followed_requests(&self) -> Result<(), StoreError>;

    /// Quarantine pre-logical-identity queue data once, when the store
    /// opens, before anything is served; no delivery runs it. Original
    /// request and attempt records are kept, never reclassified as
    /// delivered or absent and never correlated by matching bodies.
    fn archive_legacy_queue(&self) -> Result<(), StoreError>;
    fn observe_legacy_archive(
        &self,
    ) -> Result<Vec<meta_signal_flow::LegacyArchiveEntry>, StoreError>;

    /// Atomically removes the request if it remains oldest and retains the
    /// settled attempt, so a duplicate completion is idempotent. The
    /// request arrives folded with `receipt_turn`, the turn its receipt
    /// lies in.
    fn settle_recorded_delivery(
        &self,
        attempt_id: &str,
        receipt_turn: crate::generated::transitions::ReceiptTurn,
    ) -> Result<bool, StoreError>;

    /// Adds an authenticated answer's settlement of one Uncertain attempt to
    /// the caller's admission commit.  There must be exactly one unfinished
    /// attempt for the logical delivery. `held` is the metaflow row the
    /// caller's commit writes: when the attempt's Address is held's, the
    /// request leaves held's queue there, so one commit writes that row
    /// once; another Address's row is written here.
    fn read_uncertain_delivery(
        &self,
        logical_delivery_id: &str,
        answer_logical_delivery_id: &str,
        held: &mut StoredMetaflow,
        commit: AtomicCommit,
    ) -> Result<AtomicCommit, StoreError>;

    /// Removes an unresolved request from active queue processing without
    /// changing its Uncertain delivery record or permitting automatic retry.
    fn park_uncertain_delivery(&self, attempt_id: &str) -> Result<bool, StoreError>;

    /// Native submission returned without an exactly correlated receipt.
    /// The request stays queued and this attempt can only be reconciled,
    /// never submitted again by assumption.
    fn mark_delivery_uncertain(&self, attempt_id: &str) -> Result<bool, StoreError>;

    /// The owner's resolution of one Uncertain attempt. Only Absent is
    /// actionable: it retains the request and authorises a retry. Delivered
    /// requires an exact native receipt and cannot be asserted by an owner.
    fn resolve_uncertain_delivery(
        &self,
        attempt_id: &str,
        record: QueueResolutionRecord,
    ) -> Result<Result<QueueDeliveryAttempt, ResolveQueueDeliveryRejection>, StoreError>;

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

    /// Stages the queue and attempt changes that accompany archiving an open request.
    fn stage_request_end(
        &self,
        retained: &RetainedLogicalRequest,
        attempts: Vec<QueueDeliveryAttempt>,
        record: AbandonmentRecord,
        commit: AtomicCommit,
    ) -> Result<(AtomicCommit, Option<DeliveryState>), StoreError>;

    /// Finish one attempt, park its request if queued, and end its logical delivery.
    fn abandon_queue_delivery(
        &self,
        attempt_id: &str,
        record: AbandonmentRecord,
    ) -> Result<Result<Address, AbandonQueueDeliveryRejected_Data>, StoreError>;

    /// The owner withdraws the request `id` from the queue of the metaflow
    /// at `address`, and closes the attempt that holds it back, as its
    /// hand-over stands: one whose receipt is recorded settles as delivered;
    /// one that may have reached the seat stays Uncertain, parked so it is
    /// never handed over again; a request that never reached the seat ends
    /// Withdrawn.
    fn withdraw_queued(
        &self,
        address: &Address,
        id: &str,
    ) -> Result<Result<(), WithdrawRejected_Data>, StoreError>;
}

trait ReadsQueueDelivery {
    fn outstanding_for(
        &self,
        address: &Address,
    ) -> Result<Option<QueueDeliveryAttempt>, StoreError>;
    fn request_state(&self, request_id: &str) -> Result<Option<RequestState>, StoreError>;
}

impl ReadsQueueDelivery for FlowStore {
    fn outstanding_for(
        &self,
        address: &Address,
    ) -> Result<Option<QueueDeliveryAttempt>, StoreError> {
        // The range holds this Address's unfinished attempts exactly.
        let attempts = self.member_attempts(MembershipOwner::UnfinishedAt(address))?;
        let mut matching = attempts.into_iter().filter_map(|attempt| {
            match self.request_state(&attempt.queue_request_id.0) {
                Ok(Some(RequestState::Parked | RequestState::Settled)) => None,
                Ok(_) => Some(Ok(attempt)),
                Err(error) => Some(Err(error)),
            }
        });
        let first = matching.next().transpose()?;
        if matching.next().transpose()?.is_some() {
            return Err(StoreError::StateInvariant);
        }
        Ok(first)
    }

    fn request_state(&self, request_id: &str) -> Result<Option<RequestState>, StoreError> {
        let rows = self.engine.match_records(QueryPlan::key(
            self.queue_delivery.requests,
            RecordKey::new(request_id),
        ))?;
        Ok(rows
            .records()
            .first()
            .map(|request| request.request_state.clone()))
    }
}

impl AccountsQueueDelivery for FlowStore {
    fn queue_request_state(&self, id: &str) -> Result<Option<RequestState>, StoreError> {
        self.request_state(id)
    }

    fn stage_queue_request_state(
        &self,
        address: &Address,
        entry: &flow_ethos::QueueEntry,
        state: RequestState,
        commit: AtomicCommit,
    ) -> Result<AtomicCommit, StoreError> {
        let id = &entry.logical_delivery_id.0;
        let rows = self.engine.match_records(QueryPlan::key(
            self.queue_delivery.requests,
            RecordKey::new(id.clone()),
        ))?;
        match rows.records() {
            [] => Ok(commit.assert(
                self.queue_delivery.requests,
                StoredDeliveryRequest {
                    queue_request_id: meta_signal_flow::QueueRequestId(id.clone()),
                    address: address.clone(),
                    queue_evidence: QueueEvidence::Logical(entry.clone()),
                    request_state: state,
                },
            )),
            [request]
                if request.address == *address
                    && matches!(&request.queue_evidence, QueueEvidence::Logical(held)
                    if held.logical_delivery_id == entry.logical_delivery_id) =>
            {
                let mut request = request.clone();
                request.request_state = state;
                Ok(commit.mutate(self.queue_delivery.requests, request))
            }
            _ => Err(StoreError::StateInvariant),
        }
    }

    fn unfinished_queue_attempts(&self) -> Result<Vec<QueueDeliveryAttempt>, StoreError> {
        self.member_attempts(MembershipOwner::Unfinished)
    }

    fn stage_interrupted_queue_attempt(
        &self,
        mut attempt: QueueDeliveryAttempt,
        commit: AtomicCommit,
    ) -> Result<AtomicCommit, StoreError> {
        match attempt.queue_delivery_phase {
            QueueDeliveryPhase::Uncertain(_) => Ok(commit),
            QueueDeliveryPhase::Prepared | QueueDeliveryPhase::ReceiptRecorded(_) => {
                attempt.queue_delivery_phase = QueueDeliveryPhase::Uncertain(None);
                self.commit_attempt(commit, attempt)
            }
            _ => Err(StoreError::StateInvariant),
        }
    }

    fn delivery_attempts_for(
        &self,
        logical_delivery_id: &str,
    ) -> Result<Vec<QueueDeliveryAttempt>, StoreError> {
        self.member_attempts(MembershipOwner::LogicalDelivery(logical_delivery_id))
    }
    fn retained_logical_requests(&self) -> Result<Vec<RetainedLogicalRequest>, StoreError> {
        Ok(self
            .engine
            .match_records(QueryPlan::all(self.queue_delivery.requests))?
            .records()
            .iter()
            .filter_map(|request| match &request.queue_evidence {
                QueueEvidence::Logical(entry) => Some(RetainedLogicalRequest {
                    address: request.address.clone(),
                    entry: entry.clone(),
                }),
                QueueEvidence::Legacy(_) => None,
            })
            .collect())
    }

    fn assert_logical_request(
        &self,
        address: &Address,
        entry: flow_ethos::QueueEntry,
        commit: AtomicCommit,
    ) -> AtomicCommit {
        commit.assert(
            self.queue_delivery.requests,
            StoredDeliveryRequest {
                queue_request_id: meta_signal_flow::QueueRequestId(
                    entry.logical_delivery_id.clone().0,
                ),
                address: address.clone(),
                queue_evidence: QueueEvidence::Logical(entry),
                request_state: RequestState::Queued,
            },
        )
    }

    fn follow_queue_entry(
        &self,
        address: &Address,
        before: &flow_ethos::QueueEntry,
        after: &flow_ethos::QueueEntry,
        commit: AtomicCommit,
    ) -> Result<AtomicCommit, StoreError> {
        let rows = self.engine.match_records(QueryPlan::key(
            self.queue_delivery.requests,
            RecordKey::new(before.logical_delivery_id.clone().0),
        ))?;
        let before_evidence = QueueEvidence::Logical(before.clone());
        let after_evidence = QueueEvidence::Logical(after.clone());
        match rows.records() {
            [] => Ok(commit),
            [request]
                if request.address == *address && request.queue_evidence == after_evidence =>
            {
                Ok(commit)
            }
            [request]
                if request.address == *address && request.queue_evidence == before_evidence =>
            {
                let mut followed = request.clone();
                followed.queue_evidence = after_evidence;
                Ok(commit.mutate(self.queue_delivery.requests, followed))
            }
            _ => Err(StoreError::StateInvariant),
        }
    }

    fn align_followed_requests(&self) -> Result<(), StoreError> {
        let mut commit = self.engine.begin_atomic_commit();
        let mut changed = false;
        for metaflow in self.list_metaflows()? {
            for entry in &metaflow.queue.0 {
                let rows = self.engine.match_records(QueryPlan::key(
                    self.queue_delivery.requests,
                    RecordKey::new(entry.logical_delivery_id.clone().0),
                ))?;
                let [request] = rows.records() else {
                    continue;
                };
                let QueueEvidence::Logical(retained) = &request.queue_evidence else {
                    continue;
                };
                let mut as_queued = retained.clone();
                as_queued.delivery = entry.delivery.clone();
                if request.address != metaflow.address || retained == entry || &as_queued != entry {
                    continue;
                }
                // Only a request still waiting for its hand-over is aligned.
                // A parked request, or an entry any unfinished attempt was
                // handed, may already be native input and keeps its record.
                if !matches!(
                    request.request_state,
                    RequestState::Queued | RequestState::Unsent
                ) || self
                    .delivery_attempts_for(&entry.logical_delivery_id.0)?
                    .iter()
                    .any(|attempt| !attempt.queue_delivery_phase.is_finished())
                {
                    continue;
                }
                let mut aligned = request.clone();
                aligned.queue_evidence = QueueEvidence::Logical(entry.clone());
                commit = commit.mutate(self.queue_delivery.requests, aligned);
                changed = true;
            }
        }
        if changed {
            self.engine.commit_atomic(commit)?;
        }
        Ok(())
    }

    fn archive_legacy_queue(&self) -> Result<(), StoreError> {
        let requests = self
            .engine
            .match_records(QueryPlan::all(self.queue_delivery.requests))?
            .records()
            .to_vec();
        let mut commit = self.engine.begin_atomic_commit();
        let mut changed = false;
        // These request records retain their own original identities and
        // evidence. They cannot safely be paired to a migrated queue slot.
        for request in &requests {
            if matches!(request.queue_evidence, QueueEvidence::Legacy(_))
                && matches!(
                    request.request_state,
                    RequestState::Queued | RequestState::Unsent
                )
            {
                let mut archived = request.clone();
                archived.request_state = RequestState::Parked;
                commit = commit.mutate(self.queue_delivery.requests, archived);
                changed = true;
            }
        }
        for metaflow in self.list_metaflows()? {
            let mut stored = self
                .stored_metaflow(&metaflow.address)?
                .ok_or(StoreError::StateInvariant)?;
            let mut kept = Vec::new();
            let mut removed = false;
            for entry in metaflow.queue.0 {
                // This namespace is assigned only by the pre-logical-ID
                // metaflow migration, never by ordinary send admission.
                if !entry.logical_delivery_id.0.starts_with("legacy:") {
                    kept.push(entry);
                    continue;
                }
                let id = entry.logical_delivery_id.clone();
                let request_id = entry.request_id.clone();
                let evidence = QueueEvidence::Logical(entry);
                match requests
                    .iter()
                    .find(|request| request.queue_request_id.0 == id.0)
                {
                    Some(request)
                        if request.address == metaflow.address
                            && request.queue_evidence == evidence =>
                    {
                        let mut archived = request.clone();
                        archived.request_state = RequestState::Parked;
                        commit = commit.mutate(self.queue_delivery.requests, archived);
                    }
                    Some(_) => return Err(StoreError::StateInvariant),
                    None => {
                        commit = commit.assert(
                            self.queue_delivery.requests,
                            StoredDeliveryRequest {
                                queue_request_id: meta_signal_flow::QueueRequestId(id.clone().0),
                                address: metaflow.address.clone(),
                                queue_evidence: evidence,
                                request_state: RequestState::Parked,
                            },
                        );
                    }
                }
                if self.logical_delivery_snapshot(&id)?.is_some() {
                    commit = commit
                        .retract(
                            self.logical_deliveries.current,
                            RecordKey::new(id.clone().0),
                        )
                        .retract(
                            self.logical_deliveries.request_ids,
                            RecordKey::new(request_id.0),
                        );
                    for event in self
                        .engine
                        .match_records(QueryPlan::all(self.logical_deliveries.events))?
                        .records()
                        .iter()
                        .filter(|event| event.delivery_snapshot.logical_delivery_id == id)
                    {
                        commit = commit.retract(self.logical_deliveries.events, event.record_key());
                    }
                }
                removed = true;
            }
            if removed {
                stored.metaflow.queue = flow_ethos::Queue(kept);
                commit = commit.mutate(self.metaflows, stored);
                changed = true;
            }
        }
        if changed {
            self.engine.commit_atomic(commit)?;
        }
        Ok(())
    }

    fn observe_legacy_archive(
        &self,
    ) -> Result<Vec<meta_signal_flow::LegacyArchiveEntry>, StoreError> {
        let requests = self
            .engine
            .match_records(QueryPlan::all(self.queue_delivery.requests))?;
        let mut archive = Vec::new();
        for request in requests.records().iter().filter(|request| {
            request.request_state == RequestState::Parked
                && (matches!(request.queue_evidence, QueueEvidence::Legacy(_))
                    || request.queue_request_id.0.starts_with("legacy:"))
        }) {
            // A request's attempts are all at its Address.
            let queue_attempt_id_vector = self
                .member_attempts(MembershipOwner::Address(&request.address))?
                .into_iter()
                .filter(|attempt| attempt.queue_request_id == request.queue_request_id)
                .map(|attempt| attempt.queue_attempt_id)
                .collect();
            archive.push(meta_signal_flow::LegacyArchiveEntry {
                queue_request_id: meta_signal_flow::QueueRequestId(
                    request.queue_request_id.0.clone(),
                ),
                address: request.address.clone(),
                queue_evidence: request.queue_evidence.clone(),
                queue_attempt_id_vector,
            });
        }
        archive.sort_by(|left, right| left.queue_request_id.cmp(&right.queue_request_id));
        Ok(archive)
    }

    fn read_uncertain_delivery(
        &self,
        logical_delivery_id: &str,
        answer_logical_delivery_id: &str,
        held: &mut StoredMetaflow,
        commit: AtomicCommit,
    ) -> Result<AtomicCommit, StoreError> {
        if self.request_state(logical_delivery_id)? == Some(RequestState::Settled) {
            return Ok(commit);
        }
        let mut unfinished = self
            .delivery_attempts_for(logical_delivery_id)?
            .into_iter()
            .filter(|attempt| !attempt.queue_delivery_phase.is_finished());
        // A hand-over recorded Uncertain from its pane writer's result holds
        // no attempt: its answer settles nothing more here.
        let Some(mut attempt) = unfinished.next() else {
            return Ok(commit);
        };
        if unfinished.next().is_some()
            || !matches!(
                attempt.queue_delivery_phase,
                QueueDeliveryPhase::Uncertain(_)
            )
        {
            return Err(StoreError::StateInvariant);
        }
        let requests = self
            .engine
            .match_records(QueryPlan::key(
                self.queue_delivery.requests,
                RecordKey::new(attempt.queue_request_id.0.clone()),
            ))?
            .records()
            .to_vec();
        let [request] = requests.as_slice() else {
            return Err(StoreError::StateInvariant);
        };
        if request.address != attempt.address || request.queue_evidence != attempt.queue_evidence {
            return Err(StoreError::StateInvariant);
        }
        let mut request = request.clone();
        // The answered request's own metaflow: the held row when the answer
        // is queued to the same Address, else that Address's row.
        let mut elsewhere = if held.metaflow.address == attempt.address {
            None
        } else {
            Some(
                self.stored_metaflow(&attempt.address)?
                    .ok_or(StoreError::StateInvariant)?,
            )
        };
        let metaflow = match elsewhere.as_mut() {
            Some(row) => row,
            None => held,
        };
        match request.request_state {
            RequestState::Queued => {
                let QueueEvidence::Logical(entry) = &attempt.queue_evidence else {
                    return Err(StoreError::StateInvariant);
                };
                if metaflow.metaflow.queue.0.first() != Some(entry) {
                    return Err(StoreError::StateInvariant);
                }
                metaflow.metaflow.queue.0.remove(0);
            }
            RequestState::Parked => {}
            RequestState::Settled | RequestState::Unsent | RequestState::InFlight => {
                return Err(StoreError::StateInvariant);
            }
        }
        request.request_state = RequestState::Settled;
        attempt.queue_delivery_phase = QueueDeliveryPhase::Read(Read_Data {
            answer: Answer(answer_logical_delivery_id.to_owned()),
            late_receipt: LateReceipt(None),
        });
        let commit = match elsewhere {
            Some(row) => commit.mutate(self.metaflows, row),
            None => commit,
        };
        self.commit_attempt(
            commit.mutate(self.queue_delivery.requests, request),
            attempt,
        )
    }

    fn settle_recorded_delivery(
        &self,
        attempt_id: &str,
        receipt_turn: crate::generated::transitions::ReceiptTurn,
    ) -> Result<bool, StoreError> {
        let Some(mut attempt) = self.queue_delivery_attempt(attempt_id)? else {
            return Ok(false);
        };
        if matches!(
            &attempt.queue_delivery_phase,
            QueueDeliveryPhase::Settled(_) | QueueDeliveryPhase::Read { .. }
        ) {
            return Ok(true);
        }
        let QueueDeliveryPhase::ReceiptRecorded(receipt) = &attempt.queue_delivery_phase else {
            return Ok(false);
        };
        let receipt = receipt.clone();
        let Some(mut metaflow) = self.stored_metaflow(&attempt.address)? else {
            return Ok(false);
        };
        let QueueEvidence::Logical(logical_entry) = &attempt.queue_evidence else {
            return Ok(false);
        };
        if logical_entry.logical_delivery_id.0.starts_with("legacy:") {
            // A late receipt is retained on its original attempt, but the
            // kept legacy archive never becomes a delivered lifecycle.
            return Ok(false);
        }
        let logical_id = logical_entry.logical_delivery_id.clone();
        let native_receipt = NativeReceipt {
            harness_kind: attempt.harness_kind.clone(),
            native_session_id: signal_flow::NativeSessionId(attempt.thread_id.0.clone()),
            native_prompt_id: signal_flow::NativePromptId(receipt.turn_id.0.clone()),
        };
        let is_parked =
            self.request_state(&attempt.queue_request_id.0)? == Some(RequestState::Parked);
        if !is_parked && metaflow.metaflow.queue.0.first() != Some(logical_entry) {
            return Ok(false);
        }
        let requests = self
            .engine
            .match_records(QueryPlan::key(
                self.queue_delivery.requests,
                RecordKey::new(attempt.queue_request_id.0.clone()),
            ))?
            .records()
            .to_vec();
        let [request] = requests.as_slice() else {
            return Err(StoreError::StateInvariant);
        };
        if request.address != attempt.address
            || request.queue_evidence != attempt.queue_evidence
            || (!is_parked && request.request_state != RequestState::Queued)
            || (is_parked && request.request_state != RequestState::Parked)
        {
            return Err(StoreError::StateInvariant);
        }
        let mut request = request.clone();
        request.request_state = RequestState::Settled;
        if !is_parked {
            metaflow.metaflow.queue.0.remove(0);
        }
        attempt.queue_delivery_phase = QueueDeliveryPhase::Settled(receipt);
        let landed_at = attempt.flow_id.clone();
        let commit = self
            .engine
            .begin_atomic_commit()
            .mutate(self.metaflows, metaflow)
            .mutate(self.queue_delivery.requests, request);
        let commit = self.commit_attempt(commit, attempt)?;
        self.commit_logical_transition_at(
            &logical_id,
            DeliveryState::Delivered(native_receipt),
            receipt_turn,
            commit,
            DeliveryRecipient::Exact(&landed_at),
        )?;
        Ok(true)
    }

    fn park_uncertain_delivery(&self, attempt_id: &str) -> Result<bool, StoreError> {
        let Some(attempt) = self.queue_delivery_attempt(attempt_id)? else {
            return Ok(false);
        };
        if !matches!(
            attempt.queue_delivery_phase,
            QueueDeliveryPhase::Uncertain(_)
        ) {
            return Ok(false);
        }
        let QueueEvidence::Logical(entry) = &attempt.queue_evidence else {
            return Ok(false);
        };
        let Some(mut metaflow) = self.stored_metaflow(&attempt.address)? else {
            return Ok(false);
        };
        if metaflow.metaflow.queue.0.first() != Some(entry) {
            return Ok(false);
        }
        let rows = self.engine.match_records(QueryPlan::key(
            self.queue_delivery.requests,
            RecordKey::new(attempt.queue_request_id.0.clone()),
        ))?;
        let [request] = rows.records() else {
            return Err(StoreError::StateInvariant);
        };
        if request.address != attempt.address
            || request.queue_evidence != attempt.queue_evidence
            || request.request_state != RequestState::Queued
        {
            return Err(StoreError::StateInvariant);
        }
        let mut parked = request.clone();
        parked.request_state = RequestState::Parked;
        metaflow.metaflow.queue.0.remove(0);
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
        match &attempt.queue_delivery_phase {
            QueueDeliveryPhase::Prepared => {
                attempt.queue_delivery_phase = QueueDeliveryPhase::Uncertain(None);
                let commit = self.commit_attempt(self.engine.begin_atomic_commit(), attempt)?;
                self.commit_attempt_state(&evidence, DeliveryState::Uncertain, commit)?;
                Ok(true)
            }
            QueueDeliveryPhase::Uncertain(_) => Ok(true),
            QueueDeliveryPhase::ReceiptRecorded(_)
            | QueueDeliveryPhase::Settled(_)
            | QueueDeliveryPhase::Read { .. }
            | QueueDeliveryPhase::Absent(_)
            | QueueDeliveryPhase::Resolved(_)
            | QueueDeliveryPhase::Abandoned(_) => Ok(false),
        }
    }

    fn resolve_uncertain_delivery(
        &self,
        attempt_id: &str,
        record: QueueResolutionRecord,
    ) -> Result<Result<QueueDeliveryAttempt, ResolveQueueDeliveryRejection>, StoreError> {
        use ResolveQueueDeliveryRejection as Rejected;
        let Some(mut attempt) = self.queue_delivery_attempt(attempt_id)? else {
            return Ok(Err(Rejected::UnknownAttempt));
        };
        if let QueueDeliveryPhase::Settled(receipt) = &attempt.queue_delivery_phase {
            let _ = receipt;
            return Ok(Err(Rejected::AlreadyDelivered));
        }
        if matches!(
            attempt.queue_delivery_phase,
            QueueDeliveryPhase::Read { .. }
        ) {
            return Ok(Err(Rejected::AlreadyRead));
        }
        if let QueueDeliveryPhase::Resolved(prior) = &attempt.queue_delivery_phase {
            if prior.queue_resolution_outcome == QueueResolutionOutcome::Absent
                && record.queue_resolution_outcome == QueueResolutionOutcome::Absent
                && prior.resolver_identity == record.resolver_identity
                && prior.queue_resolution_reason == record.queue_resolution_reason
            {
                return Ok(Ok(attempt));
            }
            return Ok(Err(Rejected::AlreadyResolved(prior.clone())));
        }
        if !matches!(
            attempt.queue_delivery_phase,
            QueueDeliveryPhase::Uncertain(_)
        ) {
            return Ok(Err(Rejected::NotUncertain));
        }
        if record.queue_resolution_reason.0.trim().is_empty() {
            return Ok(Err(Rejected::EmptyReason));
        }
        let Some(metaflow) = self.stored_metaflow(&attempt.address)? else {
            return Ok(Err(Rejected::NotOldest));
        };
        let QueueEvidence::Logical(logical_entry) = &attempt.queue_evidence else {
            return Ok(Err(Rejected::LegacyUncorrelated));
        };
        if metaflow.metaflow.queue.0.first() != Some(logical_entry) {
            return Ok(Err(Rejected::NotOldest));
        }
        if record.queue_resolution_outcome == QueueResolutionOutcome::Delivered {
            return Ok(Err(Rejected::NativeReceiptRequired));
        }
        let requests = self
            .engine
            .match_records(QueryPlan::key(
                self.queue_delivery.requests,
                RecordKey::new(attempt.queue_request_id.0.clone()),
            ))?
            .records()
            .to_vec();
        let [request] = requests.as_slice() else {
            return Err(StoreError::StateInvariant);
        };
        if request.address != attempt.address
            || request.queue_evidence != attempt.queue_evidence
            || request.request_state != RequestState::Queued
        {
            return Err(StoreError::StateInvariant);
        }
        let mut request = request.clone();
        request.request_state = RequestState::Unsent;
        attempt.queue_delivery_phase = QueueDeliveryPhase::Resolved(record);
        let evidence = attempt.queue_evidence.clone();
        let commit = self
            .engine
            .begin_atomic_commit()
            .mutate(self.queue_delivery.requests, request);
        let commit = self.commit_attempt(commit, attempt.clone())?;
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
        // Attempt ids are fixed-width hex of one counter: their order is
        // the order they were prepared in.
        self.member_attempts(MembershipOwner::UnsettledAt(address))
    }

    fn recover_outstanding_deliveries(&self) -> Result<Vec<QueueDeliveryAttempt>, StoreError> {
        let mut outstanding = Vec::new();
        for mut attempt in self.member_attempts(MembershipOwner::Unfinished)? {
            let state = self.request_state(&attempt.queue_request_id.0)?;
            if state == Some(RequestState::Settled)
                || (state == Some(RequestState::Parked)
                    && (matches!(attempt.queue_evidence, QueueEvidence::Legacy(_))
                        || attempt.queue_request_id.0.starts_with("legacy:")))
            {
                continue;
            }
            match attempt.queue_delivery_phase.clone() {
                QueueDeliveryPhase::Prepared => {
                    attempt.queue_delivery_phase = QueueDeliveryPhase::Uncertain(None);
                    let evidence = attempt.queue_evidence.clone();
                    let commit =
                        self.commit_attempt(self.engine.begin_atomic_commit(), attempt.clone())?;
                    self.commit_attempt_state(&evidence, DeliveryState::Uncertain, commit)?;
                    outstanding.push(attempt);
                }
                QueueDeliveryPhase::Uncertain(_) | QueueDeliveryPhase::ReceiptRecorded(_) => {
                    outstanding.push(attempt);
                }
                QueueDeliveryPhase::Settled(_)
                | QueueDeliveryPhase::Read { .. }
                | QueueDeliveryPhase::Absent(_)
                | QueueDeliveryPhase::Resolved(_)
                | QueueDeliveryPhase::Abandoned(_) => {}
            }
        }
        Ok(outstanding)
    }

    fn stage_request_end(
        &self,
        retained: &RetainedLogicalRequest,
        attempts: Vec<QueueDeliveryAttempt>,
        record: AbandonmentRecord,
        mut commit: AtomicCommit,
    ) -> Result<(AtomicCommit, Option<DeliveryState>), StoreError> {
        let logical_id = &retained.entry.logical_delivery_id;
        let mut terminal = None;
        if let Some(mut stored) = self.stored_metaflow(&retained.address)?
            && let Some(index) = stored
                .metaflow
                .queue
                .0
                .iter()
                .position(|entry| entry.logical_delivery_id == *logical_id)
        {
            if stored.metaflow.queue.0.remove(index) != retained.entry {
                return Err(StoreError::StateInvariant);
            }
            let rows = self.engine.match_records(QueryPlan::key(
                self.queue_delivery.requests,
                RecordKey::new(logical_id.0.clone()),
            ))?;
            let [request] = rows.records() else {
                return Err(StoreError::StateInvariant);
            };
            if request.address != retained.address
                || request.queue_evidence != QueueEvidence::Logical(retained.entry.clone())
            {
                return Err(StoreError::StateInvariant);
            }
            let mut parked = request.clone();
            parked.request_state = RequestState::Parked;
            commit = commit
                .mutate(self.metaflows, stored)
                .mutate(self.queue_delivery.requests, parked);
            terminal = Some(DeliveryState::Withdrawn);
        }
        for mut attempt in attempts {
            attempt.queue_delivery_phase = QueueDeliveryPhase::Abandoned(record.clone());
            commit = self.commit_attempt(commit, attempt)?;
            terminal = Some(DeliveryState::Abandoned);
        }
        Ok((commit, terminal))
    }

    fn abandon_queue_delivery(
        &self,
        attempt_id: &str,
        record: AbandonmentRecord,
    ) -> Result<Result<Address, AbandonQueueDeliveryRejected_Data>, StoreError> {
        use AbandonQueueDeliveryRejected_Data as Rejected;
        let Some(mut attempt) = self.queue_delivery_attempt(attempt_id)? else {
            return Ok(Err(Rejected::UnknownAttempt));
        };
        if attempt.queue_delivery_phase.is_finished() {
            return Ok(Err(Rejected::AlreadyFinished));
        }
        if record.queue_resolution_reason.0.trim().is_empty() {
            return Ok(Err(Rejected::EmptyReason));
        }
        let address = attempt.address.clone();
        let evidence = attempt.queue_evidence.clone();
        let rows = self.engine.match_records(QueryPlan::key(
            self.queue_delivery.requests,
            RecordKey::new(attempt.queue_request_id.0.clone()),
        ))?;
        let [request] = rows.records() else {
            return Err(StoreError::StateInvariant);
        };
        if request.address != address || request.queue_evidence != evidence {
            return Err(StoreError::StateInvariant);
        }
        let mut commit = self.engine.begin_atomic_commit();
        match request.request_state {
            RequestState::Parked => {}
            RequestState::Queued => {
                let Some(mut stored) = self.stored_metaflow(&address)? else {
                    return Err(StoreError::StateInvariant);
                };
                let QueueEvidence::Logical(entry) = &evidence else {
                    return Err(StoreError::StateInvariant);
                };
                if stored.metaflow.queue.0.first() != Some(entry) {
                    return Err(StoreError::StateInvariant);
                }
                stored.metaflow.queue.0.remove(0);
                let mut parked = request.clone();
                parked.request_state = RequestState::Parked;
                commit = commit
                    .mutate(self.metaflows, stored)
                    .mutate(self.queue_delivery.requests, parked);
            }
            RequestState::Unsent | RequestState::Settled | RequestState::InFlight => {
                return Err(StoreError::StateInvariant);
            }
        }
        attempt.queue_delivery_phase = QueueDeliveryPhase::Abandoned(record);
        let commit = self.commit_attempt(commit, attempt)?;
        if let QueueEvidence::Logical(entry) = &evidence
            && !entry.logical_delivery_id.0.starts_with("legacy:")
        {
            self.commit_attempt_state(&evidence, DeliveryState::Abandoned, commit)?;
        } else {
            self.engine.commit_atomic(commit)?;
        }
        Ok(Ok(address))
    }

    fn withdraw_queued(
        &self,
        address: &Address,
        id: &str,
    ) -> Result<Result<(), WithdrawRejected_Data>, StoreError> {
        let Some(mut stored) = self.stored_metaflow(address)? else {
            return Ok(Err(WithdrawRejected_Data::UnknownAddress));
        };
        let Some(index) = stored
            .metaflow
            .queue
            .0
            .iter()
            .position(|entry| entry.logical_delivery_id.0 == id)
        else {
            return Ok(Err(WithdrawRejected_Data::NotQueued));
        };
        if self.request_state(id)? == Some(RequestState::InFlight) {
            return Ok(Err(WithdrawRejected_Data::NotQueued));
        }
        if let Some(attempt) = self.outstanding_for(address)?
            && matches!(&attempt.queue_evidence,
                QueueEvidence::Logical(entry) if entry.logical_delivery_id.0 == id)
        {
            let closed = match attempt.queue_delivery_phase {
                QueueDeliveryPhase::ReceiptRecorded(_) => self.settle_recorded_delivery(
                    &attempt.queue_attempt_id.0,
                    crate::generated::transitions::ReceiptTurn::Running,
                )?,
                // A prepared hand-over is potentially native input: it is
                // never taken for absent.
                _ => {
                    self.mark_delivery_uncertain(&attempt.queue_attempt_id.0)?
                        && self.park_uncertain_delivery(&attempt.queue_attempt_id.0)?
                }
            };
            return if closed {
                Ok(Ok(()))
            } else {
                Err(StoreError::StateInvariant)
            };
        }
        // The request never reached the seat: no attempt holds it, or its
        // last attempt was proved or resolved absent.
        let entry = stored.metaflow.queue.0.remove(index);
        let evidence = QueueEvidence::Logical(entry);
        let rows = self.engine.match_records(QueryPlan::key(
            self.queue_delivery.requests,
            RecordKey::new(id.to_owned()),
        ))?;
        let commit = self
            .engine
            .begin_atomic_commit()
            .mutate(self.metaflows, stored);
        let commit = match rows.records() {
            [] => commit,
            [request] if request.address == *address && request.queue_evidence == evidence => {
                let mut parked = request.clone();
                parked.request_state = RequestState::Parked;
                commit.mutate(self.queue_delivery.requests, parked)
            }
            _ => return Err(StoreError::StateInvariant),
        };
        let logical_id = flow_ethos::LogicalDeliveryId(id.to_owned());
        if self.logical_delivery_snapshot(&logical_id)?.is_some() {
            self.commit_logical_transition(
                &logical_id,
                DeliveryState::Withdrawn,
                crate::generated::transitions::ReceiptTurn::Running,
                commit,
            )?;
        } else {
            self.engine.commit_atomic(commit)?;
        }
        Ok(Ok(()))
    }

    fn queue_delivery_attempt(
        &self,
        attempt_id: &str,
    ) -> Result<Option<QueueDeliveryAttempt>, StoreError> {
        self.stored_attempt(attempt_id)
    }
}

/// The queue attempt ids: a store that has made no attempt begins at
/// `FIRST_ATTEMPT`.
pub(crate) struct QueueAttemptIds<'store>(pub(crate) &'store FlowStore);

impl super::counters::CountsIds for QueueAttemptIds<'_> {
    /// Attempts are prepared under the Nexus's dispatch, with no gate of
    /// their own.
    fn hold_writes(&self) -> Option<std::sync::MutexGuard<'_, ()>> {
        None
    }

    fn id_counter(&self) -> Result<super::counters::IdCounter, StoreError> {
        let counters = self.0.engine.match_records(QueryPlan::key(
            self.0.queue_delivery.counter,
            RecordKey::new(COUNTER_KEY),
        ))?;
        let next = match counters.records() {
            [] => FIRST_ATTEMPT,
            [counter] => counter.next_attempt.0,
            _ => return Err(StoreError::StateInvariant),
        };
        Ok(super::counters::IdCounter {
            next,
            written: !counters.records().is_empty(),
        })
    }

    fn ids_recorded(&self) -> Result<bool, StoreError> {
        Ok(!self
            .0
            .engine
            .match_first_in_key_range(QueryPlan::first_in_key_range(
                self.0.queue_delivery.attempts,
                sema_engine::KeyRange::new(None, None),
                sema_engine::KeyRangeLimit::new(1),
            ))?
            .records()
            .is_empty())
    }

    fn declare_next_id(
        &self,
        commit: AtomicCommit,
        counter: &super::counters::IdCounter,
        next: i64,
    ) -> Result<AtomicCommit, StoreError> {
        if next < 0 {
            return Err(StoreError::StateInvariant);
        }
        let next_attempt = NextAttempt(next);
        if !counter.written {
            return Ok(commit.assert(
                self.0.queue_delivery.counter,
                DeliveryCounter {
                    next_request: NextRequest(FIRST_NEXT_REQUEST),
                    next_attempt,
                },
            ));
        }
        let counters = self.0.engine.match_records(QueryPlan::key(
            self.0.queue_delivery.counter,
            RecordKey::new(COUNTER_KEY),
        ))?;
        let [standing] = counters.records() else {
            return Err(StoreError::StateInvariant);
        };
        Ok(commit.mutate(
            self.0.queue_delivery.counter,
            DeliveryCounter {
                next_request: standing.next_request,
                next_attempt,
            },
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::store::metaflow::SeedsMetaflow;
    use crate::store::metaflow::SleepsMetaflows;
    use flow_defaults::DefaultConfiguration;
    use flow_ethos::{Aspect, Metaflow, Model, QueueEntry, Request, SentBy, State};
    use std::path::{Path, PathBuf};

    fn address() -> Address {
        Address {
            aspect: Aspect::Mind,
            topic: flow_ethos::Topic("delivery-accounting".into()),
            model: Model::Sol,
        }
    }

    fn queued(body: &str) -> QueueEntry {
        let value = body.bytes().fold(2166136261_u32, |hash, byte| {
            hash.wrapping_mul(16777619) ^ u32::from(byte)
        });
        QueueEntry {
            logical_delivery_id: flow_ethos::LogicalDeliveryId(format!("logical-{body}")),
            request_id: flow_ethos::RequestId(format!("{:04x}", value & 0xffff)),
            sent_by: SentBy::Sender(Address {
                aspect: Aspect::Psyche,
                topic: flow_ethos::Topic("core".into()),
                model: Model::Opus,
            }),
            delivery: flow_ethos::Delivery::Queue,
            message: flow_ethos::Message::Soft(Request::Order(body.into())),
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
            state: State::Awake(flow_ethos::FlowId("flow-delivery".into())),
            past: flow_ethos::Past(vec![]),
            queue: flow_ethos::Queue(vec![queued("first"), queued("second")]),
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

    #[test]
    fn kept_legacy_archive_is_not_a_pending_sender_snapshot_or_cursor_replay() {
        use super::super::logical_delivery::RecordsLogicalDeliveries;
        let (fixture, store) = Fixture::new();
        let mut legacy = queued("legacy-body-kept-whole");
        legacy.logical_delivery_id = flow_ethos::LogicalDeliveryId("legacy:address:0".into());
        let mut metaflow = store.metaflow(&address()).unwrap().unwrap();
        metaflow.queue.0.insert(0, legacy.clone());
        store.seed_metaflow(metaflow);
        let SentBy::Sender(sender) = legacy.sent_by.clone() else {
            unreachable!()
        };
        let before = store
            .read_logical_delivery_snapshots(&sender, None)
            .unwrap();
        assert!(
            before
                .delivery_snapshot_vector
                .iter()
                .any(|snapshot| snapshot.logical_delivery_id == legacy.logical_delivery_id)
        );
        store.archive_legacy_queue().unwrap();
        let after = store
            .read_logical_delivery_snapshots(&sender, None)
            .unwrap();
        assert_eq!(after.delivery_cursor, before.delivery_cursor);
        assert_eq!(after.delivery_snapshot_vector.len(), 2);
        assert!(
            after
                .delivery_snapshot_vector
                .iter()
                .all(|snapshot| !snapshot.logical_delivery_id.0.starts_with("legacy:"))
        );
        assert_eq!(
            store
                .read_logical_delivery_snapshots(&sender, Some(0))
                .unwrap(),
            after
        );
        let archive = store.observe_legacy_archive().unwrap();
        assert_eq!(archive.len(), 1);
        assert_eq!(archive[0].queue_evidence, QueueEvidence::Logical(legacy));
        drop(store);
        let reopened = fixture.reopen();
        assert_eq!(reopened.observe_legacy_archive().unwrap(), archive);
        assert_eq!(
            reopened
                .read_logical_delivery_snapshots(&sender.clone(), None)
                .unwrap(),
            after
        );
    }

    #[test]
    fn legacy_archive_keeps_every_queue_slot_across_addresses_and_restart() {
        let (fixture, store) = Fixture::new();
        let mut expected = Vec::new();
        for topic in ["delivery-accounting", "other"] {
            let mut metaflow = store.metaflow(&address()).unwrap().unwrap();
            metaflow.address.topic = flow_ethos::Topic(topic.into());
            metaflow.queue = flow_ethos::Queue(
                (0..3)
                    .map(|index| {
                        let mut entry = queued("same body");
                        entry.logical_delivery_id =
                            flow_ethos::LogicalDeliveryId(format!("legacy:{topic}:{index}"));
                        entry
                    })
                    .collect(),
            );
            expected.extend(
                metaflow
                    .queue
                    .0
                    .iter()
                    .cloned()
                    .map(|entry| (metaflow.address.clone(), entry)),
            );
            let stored = super::super::StoredMetaflow { metaflow };
            if topic == "delivery-accounting" {
                store
                    .engine
                    .mutate_keyed(KeyedMutation::new(
                        store.metaflows,
                        stored.record_key(),
                        stored,
                    ))
                    .unwrap();
            } else {
                store
                    .engine
                    .assert(Assertion::new(store.metaflows, stored))
                    .unwrap();
            }
        }
        store.archive_legacy_queue().unwrap();
        let archive = store.observe_legacy_archive().unwrap();
        assert_eq!(archive.len(), expected.len());
        // The read-only wire shape carries the full archive, not a body
        // digest or a projection from which the original cannot be read.
        {
            use datom_codec::{Actualizing, Budget, Datomizable, Path, Potential};
            use protos::{Compactable, Protosizable, ReaderBudget};
            let response = meta_signal_flow::Output::LegacyArchiveObserved(
                archive
                    .iter()
                    .map(crate::message_projection::ProjectsLegacyArchiveEntry::visible_legacy_archive_entry)
                    .collect(),
            );
            let text = response.datomize(Path::new()).protosize().compact();
            let mut budget = Budget {
                remaining: 65536,
                reader: ReaderBudget::try_from(65536).expect("a constant reader budget"),
                depth: 0,
                maximum_depth: 1024,
            };
            assert_eq!(
                Potential::<meta_signal_flow::Output>::from(text)
                    .actualize(&mut budget)
                    .unwrap(),
                response
            );
            assert_eq!(
                Potential::<meta_signal_flow::Input>::from("ObserveLegacyArchive")
                    .actualize(&mut budget)
                    .unwrap(),
                meta_signal_flow::Input::ObserveLegacyArchive
            );
        }
        for (address, entry) in &expected {
            assert!(store.metaflow(address).unwrap().unwrap().queue.0.is_empty());
            assert!(archive.iter().any(|record| record.address == *address
                && record.queue_evidence == QueueEvidence::Logical(entry.clone())));
        }
        drop(store);
        let reopened = fixture.reopen();
        assert_eq!(reopened.observe_legacy_archive().unwrap(), archive);
        reopened.archive_legacy_queue().unwrap();
        assert_eq!(reopened.observe_legacy_archive().unwrap(), archive);
        assert!(
            reopened
                .recover_outstanding_deliveries()
                .unwrap()
                .is_empty()
        );
    }
}

#[cfg(test)]
pub(crate) trait SeedsStockQueueAttempts {
    fn seed_stock_queue_attempt(&self, attempt: QueueDeliveryAttempt);
}

#[cfg(test)]
impl SeedsStockQueueAttempts for FlowStore {
    fn seed_stock_queue_attempt(&self, attempt: QueueDeliveryAttempt) {
        let commit = self
            .commit_attempt(self.engine.begin_atomic_commit(), attempt)
            .unwrap();
        self.engine.commit_atomic(commit).unwrap();
    }
}
