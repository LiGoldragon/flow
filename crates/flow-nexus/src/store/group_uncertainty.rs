//! Atomic accounting for a cohort submitted as one native input.

use super::logical_delivery::{
    AllocatesLogicalCursor, CurrentLogicalDelivery, LogicalDeliveryCounter, LogicalDeliveryEvent,
    NextCursor,
};
use super::queue_delivery::{
    AccountsQueueDelivery, COUNTER_KEY, EndsQueueDeliveryPhase, NativeRequestId, NextAttempt,
    NextRequest, QueueDeliveryAttempt, QueueDeliveryPhase, RequestState, StoredDeliveryRequest,
};
use super::*;
use crate::generated::memory::{SubmittedCohortOrigin, SubmittedDeliveryCohort};
use flow_ethos::QueueEntry;
use meta_signal_flow::{QueueAttemptId, QueueEvidence, QueueRequestId};
use signal_flow::DeliveryState;

impl EngineRecord for SubmittedDeliveryCohort {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.queue_attempt_id_vector[0].0.clone())
    }
}

impl MemoryDefinition for SubmittedDeliveryCohort {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(1)
    }

    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label("flow-nexus-submitted-delivery-cohort-v1")
    }
}

pub(super) trait DescribesSubmittedCohorts {
    const TABLE_NAME: &'static str;
    const FAMILY_NAME: &'static str;
    fn descriptor() -> TableDescriptor<SubmittedDeliveryCohort>;
}

impl DescribesSubmittedCohorts for SubmittedDeliveryCohort {
    const TABLE_NAME: &'static str = "flow_nexus_submitted_delivery_cohorts";
    const FAMILY_NAME: &'static str = "flow-nexus-submitted-delivery-cohorts";

    fn descriptor() -> TableDescriptor<SubmittedDeliveryCohort> {
        TableDescriptor::new(
            TableName::new(Self::TABLE_NAME),
            FamilyName::new(Self::FAMILY_NAME),
            Self::schema_hash(),
        )
    }
}

pub(crate) trait ReadsSubmittedCohorts {
    fn submitted_cohort(
        &self,
        attempt: &QueueDeliveryAttempt,
    ) -> Result<Option<SubmittedDeliveryCohort>, StoreError>;

    fn submitted_attempts(
        &self,
        attempt: &QueueDeliveryAttempt,
        cohort: SubmittedDeliveryCohort,
    ) -> Result<Vec<QueueDeliveryAttempt>, StoreError>;
}

impl ReadsSubmittedCohorts for FlowStore {
    fn submitted_cohort(
        &self,
        attempt: &QueueDeliveryAttempt,
    ) -> Result<Option<SubmittedDeliveryCohort>, StoreError> {
        let rows = self
            .engine
            .match_records(QueryPlan::all(self.logical_deliveries.cohorts))?;
        let mut found = rows.records().iter().filter(|cohort| {
            cohort
                .queue_attempt_id_vector
                .contains(&attempt.queue_attempt_id)
        });
        let result = found.next().cloned();
        if found.next().is_some() {
            return Err(StoreError::StateInvariant);
        }
        Ok(result)
    }

    fn submitted_attempts(
        &self,
        attempt: &QueueDeliveryAttempt,
        cohort: SubmittedDeliveryCohort,
    ) -> Result<Vec<QueueDeliveryAttempt>, StoreError> {
        let mut previous_id = None;
        cohort
            .queue_attempt_id_vector
            .into_iter()
            .map(|id| {
                let member = self
                    .queue_delivery_attempt(&id.0)?
                    .ok_or(StoreError::StateInvariant)?;
                if member.address != attempt.address
                    || member.flow_id != attempt.flow_id
                    || member.thread_id != attempt.thread_id
                    || member.harness_kind != attempt.harness_kind
                    || member.native_history_anchor != attempt.native_history_anchor
                    || !matches!(member.queue_evidence, QueueEvidence::Logical(_))
                    || previous_id.is_some_and(|previous| previous >= member.native_request_id.0)
                {
                    return Err(StoreError::StateInvariant);
                }
                previous_id = Some(member.native_request_id.0);
                Ok(member)
            })
            .collect()
    }
}

pub(crate) trait StagesUncertainCohorts {
    /// The caller holds the dispatch gate from preparation through native I/O.
    /// Every member is parked before that I/O; recovery therefore retains the
    /// uncertainty of the whole submitted input without submitting its suffix.
    fn mark_startup_uncertain(
        &self,
        address: &Address,
        cohort: &[QueueEntry],
    ) -> Result<(), StoreError>;

    fn stage_uncertain_cohort(
        &self,
        head: &QueueDeliveryAttempt,
        cohort: &[QueueEntry],
        origin: SubmittedCohortOrigin,
    ) -> Result<Vec<QueueDeliveryAttempt>, StoreError>;
}

impl StagesUncertainCohorts for FlowStore {
    fn mark_startup_uncertain(
        &self,
        address: &Address,
        cohort: &[QueueEntry],
    ) -> Result<(), StoreError> {
        let _guard = self
            .logical_deliveries
            .write_gate
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let Some(stored) = self.stored_metaflow(address)? else {
            return Err(StoreError::StateInvariant);
        };
        if stored.metaflow.state != flow_ethos::State::Asleep
            || !stored.metaflow.queue.0.starts_with(cohort)
        {
            return Err(StoreError::StateInvariant);
        }
        let allocation = self.allocate_logical_cursor()?;
        let mut next_cursor = allocation.cursor;
        let mut commit = self.engine.begin_atomic_commit();
        for entry in cohort {
            let rows = self.engine.match_records(QueryPlan::key(
                self.logical_deliveries.current,
                RecordKey::new(entry.logical_delivery_id.0.clone()),
            ))?;
            let [current] = rows.records() else {
                return Err(StoreError::StateInvariant);
            };
            if current.archived_cursor.0.is_some()
                || current.delivery_snapshot.address != *address
                || !matches!(
                    current.delivery_snapshot.delivery_state,
                    DeliveryState::Queued | DeliveryState::Retryable(_)
                )
            {
                return Err(StoreError::StateInvariant);
            }
            let mut snapshot = current.delivery_snapshot.clone();
            snapshot.delivery_cursor = signal_flow::DeliveryCursor(next_cursor);
            snapshot.delivery_state = DeliveryState::Uncertain;
            next_cursor = next_cursor
                .checked_add(1)
                .ok_or(StoreError::StateInvariant)?;
            commit = commit
                .mutate(
                    self.logical_deliveries.current,
                    CurrentLogicalDelivery {
                        delivery_snapshot: snapshot.clone(),
                        archived_cursor: current.archived_cursor.clone(),
                        delivery_admission: current.delivery_admission.clone(),
                    },
                )
                .assert(
                    self.logical_deliveries.events,
                    LogicalDeliveryEvent {
                        delivery_snapshot: snapshot,
                    },
                );
        }
        let counter = LogicalDeliveryCounter {
            next_cursor: NextCursor(next_cursor),
        };
        commit = if allocation.existing {
            commit.mutate(self.logical_deliveries.counter, counter)
        } else {
            commit.assert(self.logical_deliveries.counter, counter)
        };
        self.engine.commit_atomic(commit)?;
        self.launch_changes.announce();
        Ok(())
    }

    fn stage_uncertain_cohort(
        &self,
        head: &QueueDeliveryAttempt,
        cohort: &[QueueEntry],
        origin: SubmittedCohortOrigin,
    ) -> Result<Vec<QueueDeliveryAttempt>, StoreError> {
        if cohort.is_empty()
            || head.queue_delivery_phase != QueueDeliveryPhase::Prepared
            || head.queue_evidence != QueueEvidence::Logical(cohort[0].clone())
            || head.queue_request_id.0 != cohort[0].logical_delivery_id.0
            || self
                .queue_delivery_attempt(&head.queue_attempt_id.0)?
                .as_ref()
                != Some(head)
        {
            return Err(StoreError::StateInvariant);
        }
        let Some(mut metaflow) = self.stored_metaflow(&head.address)? else {
            return Err(StoreError::StateInvariant);
        };
        if !metaflow.metaflow.queue.0.starts_with(cohort)
            || metaflow.metaflow.state != flow_ethos::State::Awake(head.flow_id.clone())
        {
            return Err(StoreError::StateInvariant);
        }
        let mut identities = std::collections::HashSet::new();
        for entry in cohort {
            if !identities.insert(entry.logical_delivery_id.clone()) {
                return Err(StoreError::StateInvariant);
            }
            for existing in self.delivery_attempts_for(&entry.logical_delivery_id.0)? {
                if existing != *head && !existing.queue_delivery_phase.is_finished() {
                    return Err(StoreError::StateInvariant);
                }
            }
        }

        let counters = self.engine.match_records(QueryPlan::key(
            self.queue_delivery.counter,
            RecordKey::new(COUNTER_KEY),
        ))?;
        let [counter] = counters.records() else {
            return Err(StoreError::StateInvariant);
        };
        let additional = i64::try_from(cohort.len() - 1).map_err(|_| StoreError::StateInvariant)?;
        let mut next_counter = counter.clone();
        next_counter.next_attempt = NextAttempt(
            counter
                .next_attempt
                .0
                .checked_add(additional)
                .ok_or(StoreError::StateInvariant)?,
        );
        next_counter.next_request = NextRequest(
            counter
                .next_request
                .0
                .checked_add(additional)
                .ok_or(StoreError::StateInvariant)?,
        );

        let _guard = self
            .logical_deliveries
            .write_gate
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let allocation = self.allocate_logical_cursor()?;
        let mut next_cursor = allocation.cursor;
        let mut commit = self.engine.begin_atomic_commit();
        let mut attempts = Vec::with_capacity(cohort.len());
        for (index, entry) in cohort.iter().enumerate() {
            let request_id = QueueRequestId(entry.logical_delivery_id.0.clone());
            let evidence = QueueEvidence::Logical(entry.clone());
            let rows = self.engine.match_records(QueryPlan::key(
                self.queue_delivery.requests,
                RecordKey::new(request_id.0.clone()),
            ))?;
            let request = StoredDeliveryRequest {
                queue_request_id: request_id.clone(),
                address: head.address.clone(),
                queue_evidence: evidence.clone(),
                request_state: RequestState::Parked,
            };
            commit = match rows.records() {
                [] if index != 0 => commit.assert(self.queue_delivery.requests, request),
                [old]
                    if old.address == head.address
                        && old.queue_evidence == evidence
                        && old.request_state == RequestState::Queued =>
                {
                    commit.mutate(self.queue_delivery.requests, request)
                }
                _ => return Err(StoreError::StateInvariant),
            };
            let mut attempt = head.clone();
            if index != 0 {
                let offset = i64::try_from(index - 1).map_err(|_| StoreError::StateInvariant)?;
                let number = counter
                    .next_attempt
                    .0
                    .checked_add(offset)
                    .ok_or(StoreError::StateInvariant)?;
                attempt.queue_request_id = request_id;
                attempt.queue_attempt_id = QueueAttemptId(format!("queue-attempt-{number:016x}"));
                attempt.native_request_id = NativeRequestId(number);
                attempt.queue_evidence = evidence;
            }
            attempt.queue_delivery_phase = QueueDeliveryPhase::Uncertain(None);
            commit = if index == 0 {
                commit.mutate(self.queue_delivery.attempts, attempt.clone())
            } else {
                commit.assert(self.queue_delivery.attempts, attempt.clone())
            };
            attempts.push(attempt);

            let current_rows = self.engine.match_records(QueryPlan::key(
                self.logical_deliveries.current,
                RecordKey::new(entry.logical_delivery_id.0.clone()),
            ))?;
            let [current] = current_rows.records() else {
                return Err(StoreError::StateInvariant);
            };
            if current.archived_cursor.0.is_some()
                || current.delivery_snapshot.address != head.address
                || current.delivery_snapshot.logical_delivery_id != entry.logical_delivery_id
                || current.delivery_snapshot.request_id != entry.request_id
                || current.delivery_snapshot.sent_by != entry.sent_by
            {
                return Err(StoreError::StateInvariant);
            }
            match current.delivery_snapshot.delivery_state {
                DeliveryState::Uncertain => continue,
                DeliveryState::Delivered(_)
                | DeliveryState::Read
                | DeliveryState::Failed(_)
                | DeliveryState::Withdrawn
                | DeliveryState::Archived => {
                    return Err(StoreError::StateInvariant);
                }
                _ => {}
            }
            let mut snapshot = current.delivery_snapshot.clone();
            snapshot.delivery_cursor = signal_flow::DeliveryCursor(next_cursor);
            snapshot.delivery_state = DeliveryState::Uncertain;
            next_cursor = next_cursor
                .checked_add(1)
                .ok_or(StoreError::StateInvariant)?;
            commit = commit
                .mutate(
                    self.logical_deliveries.current,
                    CurrentLogicalDelivery {
                        delivery_snapshot: snapshot.clone(),
                        archived_cursor: current.archived_cursor.clone(),
                        delivery_admission: current.delivery_admission.clone(),
                    },
                )
                .assert(
                    self.logical_deliveries.events,
                    LogicalDeliveryEvent {
                        delivery_snapshot: snapshot,
                    },
                );
        }
        if next_cursor != allocation.cursor {
            let counter = LogicalDeliveryCounter {
                next_cursor: NextCursor(next_cursor),
            };
            commit = if allocation.existing {
                commit.mutate(self.logical_deliveries.counter, counter)
            } else {
                commit.assert(self.logical_deliveries.counter, counter)
            };
        }
        if additional != 0 {
            commit = commit.mutate(self.queue_delivery.counter, next_counter);
        }
        metaflow.metaflow.queue.0.drain(..cohort.len());
        commit = commit.mutate(self.metaflows, metaflow).assert(
            self.logical_deliveries.cohorts,
            SubmittedDeliveryCohort {
                submitted_cohort_origin: origin,
                queue_attempt_id_vector: attempts
                    .iter()
                    .map(|attempt| attempt.queue_attempt_id.clone())
                    .collect(),
            },
        );
        self.engine.commit_atomic(commit)?;
        self.launch_changes.announce();
        Ok(attempts)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::logical_delivery::RecordsLogicalDeliveries;
    use crate::store::metaflow::{SeedsMetaflow, SleepsMetaflows};
    use crate::store::queue_delivery::{NativeHistoryAnchor, PrepareQueueDelivery};
    use flow_defaults::DefaultConfiguration;
    use flow_ethos::{Aspect, Metaflow, Model, Queue, Request, SentBy, State, Topic};
    use std::path::PathBuf;

    fn address() -> Address {
        Address {
            aspect: Aspect::Mind,
            topic: Topic("uncertain-cohort".into()),
            model: Model::Sol,
        }
    }

    struct Fixture {
        _directory: tempfile::TempDir,
        path: PathBuf,
        defaults: DefaultConfiguration,
    }

    impl Fixture {
        fn new() -> (Self, FlowStore, Vec<QueueEntry>, QueueDeliveryAttempt) {
            let directory = tempfile::tempdir().expect("private fixture");
            let path = directory.path().join("flow.sema");
            let defaults = DefaultConfiguration {
                home: directory.path().join("home"),
                runtime_directory: directory.path().join("runtime"),
            };
            let store = FlowStore::open_seeded(&path, &defaults).expect("store opens");
            let cohort = ["first", "second", "third"]
                .into_iter()
                .enumerate()
                .map(|(index, body)| QueueEntry {
                    logical_delivery_id: flow_ethos::LogicalDeliveryId(format!("logical-{body}")),
                    request_id: flow_ethos::RequestId(format!("a{index:03}")),
                    sent_by: SentBy::Sender(address()),
                    delivery: flow_ethos::Delivery::Queue,
                    message: flow_ethos::Message::Soft(Request::Order(body.into())),
                })
                .collect::<Vec<_>>();
            store.seed_metaflow(Metaflow {
                address: address(),
                state: State::Awake(flow_ethos::FlowId("cohort-flow".into())),
                past: flow_ethos::Past(vec![]),
                queue: Queue(cohort.clone()),
            });
            let PrepareQueueDelivery::Prepared(head) = store
                .prepare_oldest_delivery(
                    &address(),
                    "cohort-flow",
                    "cohort-thread",
                    HarnessKind::Codex,
                    NativeHistoryAnchor::CodexTurns(None),
                )
                .expect("head prepares")
            else {
                panic!("prepared head")
            };
            (
                Self {
                    _directory: directory,
                    path,
                    defaults,
                },
                store,
                cohort,
                head,
            )
        }

        fn reopen(&self) -> FlowStore {
            FlowStore::open_seeded(&self.path, &self.defaults).expect("store reopens")
        }
    }

    #[test]
    fn whole_cohort_is_parked_uncertain_and_stays_out_of_the_queue_after_reopen() {
        let (fixture, store, cohort, head) = Fixture::new();
        let attempts = store
            .stage_uncertain_cohort(&head, &cohort, SubmittedCohortOrigin::Drain)
            .expect("cohort stages");
        assert_eq!(attempts.len(), cohort.len());
        let mut identities = std::collections::HashSet::new();
        let mut native_ids = std::collections::HashSet::new();
        for (entry, attempt) in cohort.iter().zip(&attempts) {
            assert!(identities.insert(attempt.queue_attempt_id.clone()));
            assert!(native_ids.insert(attempt.native_request_id.0));
            assert_eq!(attempt.native_history_anchor, head.native_history_anchor);
            assert_eq!(
                attempt.queue_evidence,
                QueueEvidence::Logical(entry.clone())
            );
            assert_eq!(
                attempt.queue_delivery_phase,
                QueueDeliveryPhase::Uncertain(None)
            );
            assert_eq!(
                store
                    .logical_delivery_snapshot(&entry.logical_delivery_id)
                    .unwrap()
                    .unwrap()
                    .delivery_state,
                DeliveryState::Uncertain
            );
            let rows = store
                .engine
                .match_records(QueryPlan::key(
                    store.queue_delivery.requests,
                    RecordKey::new(entry.logical_delivery_id.0.clone()),
                ))
                .unwrap();
            assert_eq!(rows.records()[0].request_state, RequestState::Parked);
        }
        assert!(
            store
                .metaflow(&address())
                .unwrap()
                .unwrap()
                .queue
                .0
                .is_empty()
        );
        assert!(store.outstanding_delivery(&address()).unwrap().is_none());
        drop(store);
        let reopened = fixture.reopen();
        assert!(
            reopened
                .metaflow(&address())
                .unwrap()
                .unwrap()
                .queue
                .0
                .is_empty()
        );
        let recovered = reopened.recover_outstanding_deliveries().unwrap();
        assert_eq!(recovered.len(), cohort.len());
        for attempt in &attempts {
            assert!(recovered.iter().any(|recovered| recovered == attempt));
        }
        assert_eq!(
            reopened
                .prepare_oldest_delivery(
                    &address(),
                    "cohort-flow",
                    "cohort-thread",
                    HarnessKind::Codex,
                    NativeHistoryAnchor::CodexTurns(None),
                )
                .unwrap(),
            PrepareQueueDelivery::Empty
        );
    }

    #[test]
    fn mismatched_prefix_changes_no_durable_records() {
        let (_fixture, store, cohort, head) = Fixture::new();
        let records = || {
            (
                store
                    .engine
                    .match_records(QueryPlan::all(store.metaflows))
                    .unwrap()
                    .records()
                    .to_vec(),
                store
                    .engine
                    .match_records(QueryPlan::all(store.queue_delivery.counter))
                    .unwrap()
                    .records()
                    .to_vec(),
                store
                    .engine
                    .match_records(QueryPlan::all(store.queue_delivery.requests))
                    .unwrap()
                    .records()
                    .to_vec(),
                store
                    .engine
                    .match_records(QueryPlan::all(store.queue_delivery.attempts))
                    .unwrap()
                    .records()
                    .to_vec(),
                store
                    .engine
                    .match_records(QueryPlan::all(store.logical_deliveries.counter))
                    .unwrap()
                    .records()
                    .to_vec(),
                store
                    .engine
                    .match_records(QueryPlan::all(store.logical_deliveries.current))
                    .unwrap()
                    .records()
                    .to_vec(),
                store
                    .engine
                    .match_records(QueryPlan::all(store.logical_deliveries.events))
                    .unwrap()
                    .records()
                    .to_vec(),
            )
        };
        let before = records();
        let mut mismatched = cohort.clone();
        mismatched.swap(1, 2);
        assert!(
            store
                .stage_uncertain_cohort(&head, &mismatched, SubmittedCohortOrigin::Drain)
                .is_err()
        );
        assert_eq!(records(), before);
    }
    #[test]
    fn a_late_lifecycle_error_leaves_the_prepared_head_and_counters_unchanged() {
        let (_fixture, store, cohort, head) = Fixture::new();
        store
            .transition_logical_delivery(&cohort[2].logical_delivery_id, DeliveryState::Withdrawn)
            .unwrap();
        let records = || {
            (
                store
                    .engine
                    .match_records(QueryPlan::all(store.metaflows))
                    .unwrap()
                    .records()
                    .to_vec(),
                store
                    .engine
                    .match_records(QueryPlan::all(store.queue_delivery.counter))
                    .unwrap()
                    .records()
                    .to_vec(),
                store
                    .engine
                    .match_records(QueryPlan::all(store.queue_delivery.requests))
                    .unwrap()
                    .records()
                    .to_vec(),
                store
                    .engine
                    .match_records(QueryPlan::all(store.queue_delivery.attempts))
                    .unwrap()
                    .records()
                    .to_vec(),
                store
                    .engine
                    .match_records(QueryPlan::all(store.logical_deliveries.counter))
                    .unwrap()
                    .records()
                    .to_vec(),
                store
                    .engine
                    .match_records(QueryPlan::all(store.logical_deliveries.current))
                    .unwrap()
                    .records()
                    .to_vec(),
                store
                    .engine
                    .match_records(QueryPlan::all(store.logical_deliveries.events))
                    .unwrap()
                    .records()
                    .to_vec(),
            )
        };
        let before = records();
        assert!(
            store
                .stage_uncertain_cohort(&head, &cohort, SubmittedCohortOrigin::Drain)
                .is_err()
        );
        assert_eq!(records(), before);
        assert_eq!(
            store
                .queue_delivery_attempt(&head.queue_attempt_id.0)
                .unwrap(),
            Some(head)
        );
    }
}
