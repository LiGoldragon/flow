//! Durable accounting around handing one queued request to a native seat.
//!
//! Preparing is committed before native I/O.  A prepared attempt is never
//! implicitly reusable after recovery: recovery marks it uncertain.  Only an
//! exactly correlated native receipt makes an attempt settleable, and
//! settlement removes the still-oldest queue entry in the same commit.

use super::*;
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use signal_flow::{Address, QueuedRequest};

const DELIVERY_COUNTER_TABLE_NAME: TableName = TableName::new("flow_nexus_queue_delivery_counter");
const DELIVERY_REQUEST_TABLE_NAME: TableName = TableName::new("flow_nexus_queue_delivery_requests");
const DELIVERY_ATTEMPT_TABLE_NAME: TableName = TableName::new("flow_nexus_queue_delivery_attempts");
const COUNTER_KEY: &str = "counter";

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(super) struct DeliveryCounter {
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
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(super) struct StoredDeliveryRequest {
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
pub enum QueueDeliveryPhase {
    Prepared,
    /// Recovery found a prepared attempt with no durable native receipt.
    /// It may have reached the native seat and must not be sent again.
    Uncertain,
    ReceiptRecorded(NativeDeliveryReceipt),
    Settled(NativeDeliveryReceipt),
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub struct QueueDeliveryAttempt {
    pub request_id: String,
    pub attempt_id: String,
    pub native_request_id: u64,
    pub flow_id: String,
    pub thread_id: String,
    pub address: Address,
    pub queued: QueuedRequest,
    pub phase: QueueDeliveryPhase,
}

impl EngineRecord for QueueDeliveryAttempt {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.attempt_id.clone())
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

pub(super) type QueueDeliveryDescriptors = (
    TableDescriptor<DeliveryCounter>,
    TableDescriptor<StoredDeliveryRequest>,
    TableDescriptor<QueueDeliveryAttempt>,
);

impl QueueDeliveryTables {
    pub(crate) fn descriptors() -> QueueDeliveryDescriptors {
        (
            TableDescriptor::new(
                DELIVERY_COUNTER_TABLE_NAME,
                FamilyName::new("flow-nexus-queue-delivery-counter"),
                SchemaHash::for_label("flow-nexus-queue-delivery-counter-v1"),
            ),
            TableDescriptor::new(
                DELIVERY_REQUEST_TABLE_NAME,
                FamilyName::new("flow-nexus-queue-delivery-request"),
                SchemaHash::for_label("flow-nexus-queue-delivery-request-v1"),
            ),
            TableDescriptor::new(
                DELIVERY_ATTEMPT_TABLE_NAME,
                FamilyName::new("flow-nexus-queue-delivery-attempt"),
                SchemaHash::for_label("flow-nexus-queue-delivery-attempt-v1"),
            ),
        )
    }

    pub(crate) fn register(
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
pub trait AccountsQueueDelivery {
    /// Gives the oldest queued request distinct durable request, attempt and
    /// native request identities before any native send occurs.
    fn prepare_oldest_delivery(
        &self,
        address: &Address,
        flow_id: &str,
        thread_id: &str,
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

    /// On process recovery, makes every receipt-less prepared attempt
    /// explicitly uncertain.  Receipt-recorded attempts remain settleable.
    fn recover_outstanding_deliveries(&self) -> Result<Vec<QueueDeliveryAttempt>, StoreError>;

    fn queue_delivery_attempt(
        &self,
        attempt_id: &str,
    ) -> Result<Option<QueueDeliveryAttempt>, StoreError>;
}

impl FlowStore {
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
        let mut matching = self.delivery_attempts()?.into_iter().filter(|attempt| {
            &attempt.address == address && !matches!(&attempt.phase, QueueDeliveryPhase::Settled(_))
        });
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
            || stored.metaflow.state
                != flow_ethos::State::Awake(flow_id.to_owned())
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
            QueueDeliveryPhase::Prepared | QueueDeliveryPhase::Uncertain => {
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

    fn recover_outstanding_deliveries(&self) -> Result<Vec<QueueDeliveryAttempt>, StoreError> {
        let mut outstanding = Vec::new();
        for mut attempt in self.delivery_attempts()? {
            match attempt.phase.clone() {
                QueueDeliveryPhase::Prepared => {
                    attempt.phase = QueueDeliveryPhase::Uncertain;
                    self.engine.mutate_keyed(KeyedMutation::new(
                        self.queue_delivery.attempts,
                        RecordKey::new(attempt.attempt_id.clone()),
                        attempt.clone(),
                    ))?;
                    outstanding.push(attempt);
                }
                QueueDeliveryPhase::Uncertain | QueueDeliveryPhase::ReceiptRecorded(_) => {
                    outstanding.push(attempt);
                }
                QueueDeliveryPhase::Settled(_) => {}
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
        let PrepareQueueDelivery::Prepared(prepared) =
            store.prepare_oldest_delivery(&address(), "flow-delivery", "thread-7").unwrap()
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
        assert_eq!(recovered[0].phase, QueueDeliveryPhase::Uncertain);
        assert_eq!(
            reopened.prepare_oldest_delivery(&address(), "flow-delivery", "thread-7").unwrap(),
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
    fn exact_receipt_survives_restart_and_settles_oldest_once() {
        let (fixture, store) = Fixture::new();
        let PrepareQueueDelivery::Prepared(prepared) =
            store.prepare_oldest_delivery(&address(), "flow-delivery", "thread-7").unwrap()
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
        assert_eq!(
            reopened.recover_outstanding_deliveries().unwrap()[0].phase,
            QueueDeliveryPhase::ReceiptRecorded(exact.clone())
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
    fn settlement_refuses_changed_oldest_and_preserves_sender_and_order() {
        let (_fixture, store) = Fixture::new();
        let PrepareQueueDelivery::Prepared(prepared) =
            store.prepare_oldest_delivery(&address(), "flow-delivery", "thread-7").unwrap()
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
}
