//! One atomic admission's ordered members, retained independently of queue state.

use super::logical_delivery::{
    LogicalDeliveryTransition, RecordsLogicalDeliveries, RequestedDeliveryAdmission,
};
use super::queue_delivery::AccountsQueueDelivery;
use super::*;
use crate::generated::memory::LogicalDeliveryGroup;
use crate::generated::operation::AdmissionMessage;
use signal_flow::{DeliveryState, LogicalDeliveryId, QueueEntry, Refusal, SendAcceptance, SentBy};

impl EngineRecord for LogicalDeliveryGroup {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.logical_delivery_id_vector[0].0.clone())
    }
}

impl MemoryDefinition for LogicalDeliveryGroup {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(1)
    }
    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label("flow-nexus-logical-delivery-group-v1")
    }
}

pub(super) trait DescribesDeliveryGroups {
    fn descriptor() -> TableDescriptor<LogicalDeliveryGroup>;
}

impl DescribesDeliveryGroups for LogicalDeliveryGroup {
    fn descriptor() -> TableDescriptor<LogicalDeliveryGroup> {
        TableDescriptor::new(
            TableName::new("flow_nexus_logical_delivery_groups"),
            FamilyName::new("flow-nexus-logical-delivery-groups"),
            Self::schema_hash(),
        )
    }
}

/// Pure batch preflight. Callers hold the ordinary logical admission gate.
pub(crate) trait ValidatesDeliveryGroups {
    fn validate_delivery_group_capacity(
        &self,
        count: usize,
    ) -> Result<Result<(), Refusal>, StoreError>;
    fn validate_delivery_group(
        &self,
        address: &Address,
        sender: &SentBy,
        messages: &[AdmissionMessage],
    ) -> Result<Result<(), Refusal>, StoreError>;
}

impl ValidatesDeliveryGroups for FlowStore {
    fn validate_delivery_group_capacity(
        &self,
        count: usize,
    ) -> Result<Result<(), Refusal>, StoreError> {
        use super::logical_delivery::REQUEST_ID_SPACE;
        let held = self
            .engine
            .match_records(QueryPlan::all(self.logical_deliveries.request_ids))?;
        let capacity = REQUEST_ID_SPACE
            .checked_sub(held.records().len())
            .ok_or(StoreError::StateInvariant)?;
        Ok(if count > capacity {
            Err(Refusal::RequestIdSpaceFull)
        } else {
            Ok(())
        })
    }

    fn validate_delivery_group(
        &self,
        address: &Address,
        sender: &SentBy,
        messages: &[AdmissionMessage],
    ) -> Result<Result<(), Refusal>, StoreError> {
        use super::logical_delivery::MintsRequestIds;
        use super::reminder::{RecordsReminders, RulesMinutes};
        use crate::composition::RendersQueuedWork;
        use flow_ethos::{Message, Request};
        // Projection hides private logical IDs. Fixed-width public request
        // IDs give the final native size without drawing an ID.
        let projected = messages
            .iter()
            .map(|message| QueueEntry {
                logical_delivery_id: LogicalDeliveryId(String::new()),
                request_id: flow_ethos::RequestId("0000".into()),
                sent_by: sender.clone(),
                delivery: message.delivery.clone(),
                message: message.message.clone(),
            })
            .collect::<Vec<_>>();
        let actual_bytes = projected.prompt_text(address).len();
        if actual_bytes > crate::sending::MAX_SEND_PAYLOAD_BYTES {
            return Ok(Err(Refusal::Oversized(signal_flow::SendPayloadSize {
                actual_bytes: signal_flow::ActualBytes(
                    i64::try_from(actual_bytes).unwrap_or(i64::MAX),
                ),
                maximum_bytes: signal_flow::MaximumBytes(
                    crate::sending::MAX_SEND_PAYLOAD_BYTES as i64,
                ),
            })));
        }
        if let Err(refusal) = self.validate_delivery_group_capacity(messages.len())? {
            return Ok(Err(refusal));
        }
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .ok()
            .and_then(|elapsed| i64::try_from(elapsed.as_secs()).ok())
            .ok_or(StoreError::StateInvariant)?;
        let mut answered = std::collections::HashSet::new();
        for message in messages {
            let (Message::HardAbrupt(request)
            | Message::MiddleAbrupt(request)
            | Message::Soft(request)) = &message.message;
            match request {
                Request::Task(task) => {
                    if let Err(refusal) = task.minutes.0.ruled_interval(now) {
                        return Ok(Err(Refusal::MinutesRefused(refusal)));
                    }
                }
                Request::Done(flow_ethos::DonePayload::Answered(done)) => {
                    if self.reminder(&done.logical_delivery_id.0)?.is_none() {
                        return Ok(Err(Refusal::Unresolved));
                    }
                }
                _ => {}
            }
            let answer_id = match request {
                Request::Result(flow_ethos::ResultPayload::Answered(answer)) => {
                    Some(&answer.request_id)
                }
                Request::Done(flow_ethos::DonePayload::Answered(answer)) => {
                    Some(&answer.request_id)
                }
                _ => None,
            };
            if let Some(id) = answer_id {
                if self.request_binding(id)?.is_none() {
                    return Err(StoreError::StateInvariant);
                }
                if !answered.insert(id.clone()) {
                    return Ok(Err(Refusal::Unresolved));
                }
            }
        }
        Ok(Ok(()))
    }
}

pub(crate) trait AdmitsDeliveryGroups {
    fn admit_delivery_group(
        &self,
        address: &Address,
        sender: SentBy,
        messages: Vec<AdmissionMessage>,
    ) -> Result<Result<Vec<SendAcceptance>, Refusal>, StoreError>;
}

impl AdmitsDeliveryGroups for FlowStore {
    fn admit_delivery_group(
        &self,
        address: &Address,
        sender: SentBy,
        messages: Vec<AdmissionMessage>,
    ) -> Result<Result<Vec<SendAcceptance>, Refusal>, StoreError> {
        if messages.is_empty() {
            return Ok(Ok(Vec::new()));
        }
        self.accept_requested_delivery_admission(
            address,
            sender,
            RequestedDeliveryAdmission::SendSeveral(messages),
            None,
            LogicalDeliveryTransition {
                purpose: crate::heartbeat::DeliveryPurpose::Ordinary,
                context_warning: None,
                handover_reminder: None,
                deliver_call: None,
                upgrade_notice: None,
            },
        )
    }
}

pub(crate) trait SelectsDeliveryGroups {
    fn delivery_group(
        &self,
        id: &LogicalDeliveryId,
    ) -> Result<Option<LogicalDeliveryGroup>, StoreError>;
    fn oldest_delivery_group(
        &self,
        address: &Address,
        queue: &[QueueEntry],
    ) -> Result<Vec<QueueEntry>, StoreError>;
    fn complete_delivery_groups(
        &self,
        address: &Address,
        queue: &[QueueEntry],
    ) -> Result<Vec<QueueEntry>, StoreError>;
    fn contains_delivery_group(&self, queue: &[QueueEntry]) -> Result<bool, StoreError>;
}

impl SelectsDeliveryGroups for FlowStore {
    fn delivery_group(
        &self,
        id: &LogicalDeliveryId,
    ) -> Result<Option<LogicalDeliveryGroup>, StoreError> {
        let rows = self
            .engine
            .match_records(QueryPlan::all(self.logical_deliveries.groups))?;
        let mut matched = rows
            .records()
            .iter()
            .filter(|group| group.logical_delivery_id_vector.contains(id));
        let result = matched.next().cloned();
        if matched.next().is_some() {
            return Err(StoreError::StateInvariant);
        }
        Ok(result)
    }

    fn oldest_delivery_group(
        &self,
        address: &Address,
        queue: &[QueueEntry],
    ) -> Result<Vec<QueueEntry>, StoreError> {
        let Some(first) = queue.first() else {
            return Ok(Vec::new());
        };
        let Some(group) = self.delivery_group(&first.logical_delivery_id)? else {
            return Ok(vec![first.clone()]);
        };
        if group.address != *address || group.logical_delivery_id_vector.is_empty() {
            return Err(StoreError::StateInvariant);
        }
        let retained = self.retained_logical_requests()?;
        let mut identities = std::collections::HashSet::new();
        let mut survivors = Vec::new();
        for id in &group.logical_delivery_id_vector {
            if !identities.insert(id) {
                return Err(StoreError::StateInvariant);
            }
            let members = retained
                .iter()
                .filter(|member| member.entry.logical_delivery_id == *id)
                .collect::<Vec<_>>();
            let [member] = members.as_slice() else {
                return Err(StoreError::StateInvariant);
            };
            let snapshot = self
                .logical_delivery_snapshot(id)?
                .ok_or(StoreError::StateInvariant)?;
            if member.address != *address
                || snapshot.address != *address
                || snapshot.logical_delivery_id != *id
                || snapshot.request_id != member.entry.request_id
                || snapshot.sent_by != member.entry.sent_by
            {
                return Err(StoreError::StateInvariant);
            }
            match snapshot.delivery_state {
                DeliveryState::Withdrawn => {
                    // Explicit persisted withdrawal is the omission witness;
                    // archival retains this snapshot and the original request.
                    if queue.iter().any(|entry| entry.logical_delivery_id == *id) {
                        return Err(StoreError::StateInvariant);
                    }
                }
                DeliveryState::Queued | DeliveryState::Retryable(_) => {
                    survivors.push(member.entry.clone())
                }
                _ => return Err(StoreError::StateInvariant),
            }
        }
        if survivors.is_empty() || !queue.starts_with(&survivors) {
            return Err(StoreError::StateInvariant);
        }
        Ok(survivors)
    }

    fn complete_delivery_groups(
        &self,
        address: &Address,
        queue: &[QueueEntry],
    ) -> Result<Vec<QueueEntry>, StoreError> {
        let mut remaining = queue;
        while !remaining.is_empty() {
            let unit = self.oldest_delivery_group(address, remaining)?;
            remaining = &remaining[unit.len()..];
        }
        Ok(queue.to_vec())
    }

    fn contains_delivery_group(&self, queue: &[QueueEntry]) -> Result<bool, StoreError> {
        for entry in queue {
            if self.delivery_group(&entry.logical_delivery_id)?.is_some() {
                return Ok(true);
            }
        }
        Ok(false)
    }
}
/// Refuses one complete, positively selected, never-submitted group.
/// The caller holds the existing dispatch gate; no external work occurs here.
pub(crate) trait RefusesDeliveryGroups {
    fn refuse_delivery_group(
        &self,
        address: &Address,
        selected: &[QueueEntry],
        reason: Refusal,
    ) -> Result<(), StoreError>;
}

impl RefusesDeliveryGroups for FlowStore {
    fn refuse_delivery_group(
        &self,
        address: &Address,
        selected: &[QueueEntry],
        reason: Refusal,
    ) -> Result<(), StoreError> {
        use super::logical_delivery::{
            AllocatesLogicalCursor, LogicalDeliveryCounter, LogicalDeliveryEvent, NextCursor,
        };
        use super::queue_delivery::RequestState;
        let _guard = self
            .logical_deliveries
            .write_gate
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let Some(first) = selected.first() else {
            return Err(StoreError::StateInvariant);
        };
        if self.delivery_group(&first.logical_delivery_id)?.is_none() {
            return Err(StoreError::StateInvariant);
        }
        let mut stored = self
            .stored_metaflow(address)?
            .ok_or(StoreError::StateInvariant)?;
        if self.oldest_delivery_group(address, &stored.metaflow.queue.0)? != selected
            || self.outstanding_for(address)?.is_some()
        {
            return Err(StoreError::StateInvariant);
        }
        let allocation = self.allocate_logical_cursor()?;
        let mut cursor = allocation.cursor;
        let mut commit = self.engine.begin_atomic_commit();
        for entry in selected {
            let records = self.engine.match_records(QueryPlan::key(
                self.logical_deliveries.current,
                RecordKey::new(entry.logical_delivery_id.0.clone()),
            ))?;
            let [current] = records.records() else {
                return Err(StoreError::StateInvariant);
            };
            if current.archived_cursor.0.is_some()
                || !matches!(
                    current.delivery_snapshot.delivery_state,
                    DeliveryState::Queued | DeliveryState::Retryable(_)
                )
            {
                return Err(StoreError::StateInvariant);
            }
            let mut failed = current.clone();
            failed.delivery_snapshot.delivery_cursor = signal_flow::DeliveryCursor(cursor);
            failed.delivery_snapshot.delivery_state = DeliveryState::Failed(reason.clone());
            cursor = cursor.checked_add(1).ok_or(StoreError::StateInvariant)?;
            commit = commit
                .mutate(self.logical_deliveries.current, failed.clone())
                .assert(
                    self.logical_deliveries.events,
                    LogicalDeliveryEvent {
                        delivery_snapshot: failed.delivery_snapshot,
                    },
                );
            let requests = self.engine.match_records(QueryPlan::key(
                self.queue_delivery.requests,
                RecordKey::new(entry.logical_delivery_id.0.clone()),
            ))?;
            commit = match requests.records() {
                [] => commit,
                [request]
                    if request.address == *address
                        && request.queue_evidence
                            == meta_signal_flow::QueueEvidence::Logical(entry.clone())
                        && matches!(
                            request.request_state,
                            RequestState::Queued | RequestState::Unsent
                        ) =>
                {
                    let mut parked = request.clone();
                    parked.request_state = RequestState::Parked;
                    commit.mutate(self.queue_delivery.requests, parked)
                }
                _ => return Err(StoreError::StateInvariant),
            };
        }
        stored.metaflow.queue.0.drain(..selected.len());
        let counter = LogicalDeliveryCounter {
            next_cursor: NextCursor(cursor),
        };
        commit = if allocation.existing {
            commit.mutate(self.logical_deliveries.counter, counter)
        } else {
            commit.assert(self.logical_deliveries.counter, counter)
        };
        self.engine
            .commit_atomic(commit.mutate(self.metaflows, stored))?;
        self.launch_changes.announce();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::logical_delivery::DrawsRequestId;
    use crate::store::memory_digest::DigestsMemory;
    use crate::store::metaflow::{SeedsMetaflow, SleepsMetaflows};
    use flow_defaults::DefaultConfiguration;
    use flow_ethos::{AnswerableTask, Delivery, Message, Minutes, Request};
    use std::collections::{BTreeMap, HashSet};
    use std::path::{Path, PathBuf};
    use std::sync::{Arc, Mutex};

    const GROUP_TABLE: &str = "flow_nexus_logical_delivery_groups";

    fn address() -> Address {
        Address {
            aspect: signal_flow::Aspect::Mind,
            topic: flow_ethos::Topic("delivery-group-fixture".into()),
            model: signal_flow::Model::Sol,
        }
    }

    fn order(body: &str) -> AdmissionMessage {
        AdmissionMessage {
            delivery: Delivery::Queue,
            message: Message::Soft(Request::Order(body.into())),
        }
    }

    fn task(minutes: i64) -> AdmissionMessage {
        AdmissionMessage {
            delivery: Delivery::Queue,
            message: Message::Soft(Request::Task(AnswerableTask {
                minutes: Minutes(minutes),
                string: "group task".into(),
            })),
        }
    }

    #[derive(Default)]
    struct SequentialIds(Mutex<u64>);

    impl DrawsRequestId for SequentialIds {
        fn draw_request_id(&self) -> Result<flow_ethos::RequestId, StoreError> {
            let mut next = self.0.lock().map_err(|_| StoreError::StateInvariant)?;
            let id = flow_ethos::RequestId(format!("{:04}", *next));
            *next += 1;
            Ok(id)
        }
    }

    struct RepeatedId;

    impl DrawsRequestId for RepeatedId {
        fn draw_request_id(&self) -> Result<flow_ethos::RequestId, StoreError> {
            Ok(flow_ethos::RequestId("same".into()))
        }
    }

    struct Fixture {
        _directory: tempfile::TempDir,
        path: PathBuf,
        defaults: DefaultConfiguration,
    }

    impl Fixture {
        fn new() -> (Self, FlowStore) {
            let directory = tempfile::tempdir().expect("private group fixture");
            let path = directory.path().join("flow.sema");
            let defaults = DefaultConfiguration {
                home: directory.path().join("home"),
                runtime_directory: directory.path().join("runtime"),
            };
            let mut store = FlowStore::open_seeded(&path, &defaults).expect("store opens");
            store.seed_metaflow(flow_ethos::Metaflow {
                address: address(),
                state: flow_ethos::State::Asleep,
                past: flow_ethos::Past(vec![]),
                queue: flow_ethos::Queue(vec![]),
            });
            store.set_request_id_drawer_for_tests(Arc::new(SequentialIds::default()));
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
            FlowStore::open_seeded(&self.path, &self.defaults).expect("group store reopens")
        }
    }

    fn admit(store: &FlowStore, messages: Vec<AdmissionMessage>) -> Vec<SendAcceptance> {
        store
            .admit_delivery_group(&address(), SentBy::Sender(address()), messages)
            .expect("admission reads store")
            .expect("group admitted")
    }

    fn admit_plain(store: &FlowStore, message: AdmissionMessage) -> Vec<SendAcceptance> {
        let signal_flow::PrivateSendResponse::SendAccepted(accepted) = store
            .accept_logical_delivery(
                &address(),
                SentBy::Sender(address()),
                message.delivery,
                message.message,
                None,
            )
            .unwrap()
        else {
            panic!("plain Send admitted");
        };
        vec![accepted]
    }

    #[test]
    fn admission_keeps_order_unique_ids_group_selectors_and_reopen() {
        let (fixture, store) = Fixture::new();
        let before = admit_plain(&store, order("ordinary before"));
        let accepted = admit(
            &store,
            vec![order("first"), order("second"), order("third")],
        );
        let after = admit_plain(&store, order("ordinary after"));
        let queue = store.metaflow(&address()).unwrap().unwrap().queue.0;
        let members = queue[1..4].to_vec();
        assert_eq!(queue.len(), 5);
        let mut request_ids = HashSet::new();
        let mut logical_ids = HashSet::new();
        for (index, (entry, receipt)) in members.iter().zip(&accepted).enumerate() {
            assert!(request_ids.insert(receipt.request_id.clone()));
            assert!(logical_ids.insert(receipt.logical_delivery_id.clone()));
            assert_eq!(entry.logical_delivery_id, receipt.logical_delivery_id);
            assert_eq!(entry.request_id, receipt.request_id);
            assert_eq!(receipt.request_id.0, format!("{:04}", index + 1));
            assert_eq!(receipt.delivery_state, DeliveryState::Queued);
            assert_eq!(
                entry.message,
                order(["first", "second", "third"][index]).message
            );
            if index > 0 {
                assert_eq!(
                    receipt.delivery_cursor.0,
                    accepted[index - 1].delivery_cursor.0 + 1
                );
            }
        }
        let group = store
            .delivery_group(&accepted[0].logical_delivery_id)
            .unwrap()
            .unwrap();
        assert_eq!(group.address, address());
        assert_eq!(
            group.logical_delivery_id_vector,
            accepted
                .iter()
                .map(|receipt| receipt.logical_delivery_id.clone())
                .collect::<Vec<_>>()
        );
        for receipt in &accepted {
            assert_eq!(
                store.delivery_group(&receipt.logical_delivery_id).unwrap(),
                Some(group.clone())
            );
        }
        assert!(
            store
                .delivery_group(&before[0].logical_delivery_id)
                .unwrap()
                .is_none()
        );
        assert!(
            store
                .delivery_group(&after[0].logical_delivery_id)
                .unwrap()
                .is_none()
        );
        assert_eq!(
            store.oldest_delivery_group(&address(), &queue).unwrap(),
            queue[..1]
        );
        assert_eq!(
            store
                .oldest_delivery_group(&address(), &queue[1..])
                .unwrap(),
            members
        );
        assert_eq!(
            store.complete_delivery_groups(&address(), &queue).unwrap(),
            queue
        );
        assert!(store.contains_delivery_group(&members).unwrap());
        assert!(!store.contains_delivery_group(&queue[..1]).unwrap());
        assert!(
            store
                .oldest_delivery_group(&address(), &members[..2])
                .is_err()
        );
        assert!(
            store
                .oldest_delivery_group(&address(), &members[1..])
                .is_err()
        );
        let archive_before = store.memory_digest().unwrap();
        drop(store);
        let reopened = fixture.reopen();
        assert_eq!(reopened.memory_digest().unwrap(), archive_before);
        assert_eq!(
            reopened.metaflow(&address()).unwrap().unwrap().queue.0,
            queue
        );
        assert_eq!(
            reopened
                .delivery_group(&accepted[1].logical_delivery_id)
                .unwrap(),
            Some(group)
        );
        assert_eq!(
            reopened
                .complete_delivery_groups(&address(), &queue)
                .unwrap(),
            queue
        );
    }

    #[test]
    fn invalid_late_minutes_refuses_the_entire_admission_without_writes() {
        let (_fixture, store) = Fixture::new();
        let before = store.memory_digest().unwrap();
        let result = store
            .admit_delivery_group(
                &address(),
                SentBy::Sender(address()),
                vec![task(1), order("middle"), task(0)],
            )
            .expect("admission reads store");
        assert!(matches!(result, Err(Refusal::MinutesRefused(_))));
        assert_eq!(store.memory_digest().unwrap(), before);
    }

    #[test]
    fn aggregate_oversize_refuses_the_entire_admission_without_writes() {
        let (_fixture, store) = Fixture::new();
        let before = store.memory_digest().unwrap();
        let body = "x".repeat(crate::sending::MAX_SEND_PAYLOAD_BYTES / 2 + 1);
        let result = store
            .admit_delivery_group(
                &address(),
                SentBy::Sender(address()),
                vec![order(&body), order(&body)],
            )
            .expect("admission reads store");
        assert!(matches!(result, Err(Refusal::Oversized(_))));
        assert_eq!(store.memory_digest().unwrap(), before);
    }

    #[test]
    fn a_batch_local_id_collision_refuses_every_member_without_writes() {
        let (_fixture, mut store) = Fixture::new();
        store.set_request_id_drawer_for_tests(Arc::new(RepeatedId));
        let before = store.memory_digest().unwrap();
        let result = store
            .admit_delivery_group(
                &address(),
                SentBy::Sender(address()),
                vec![order("first"), order("second")],
            )
            .expect("admission reads store");
        assert_eq!(result, Err(Refusal::RequestIdDrawsCollided));
        assert_eq!(store.memory_digest().unwrap(), before);
        assert!(
            store
                .metaflow(&address())
                .unwrap()
                .unwrap()
                .queue
                .0
                .is_empty()
        );
    }

    /// Reads the actual persisted archives of every catalogued family. A
    /// registered family whose row table has not materialized contributes no rows.
    fn archived_family_rows(path: &Path) -> BTreeMap<String, Vec<(String, Vec<u8>)>> {
        use redb::{ReadableDatabase as _, ReadableTable as _};
        let database = redb::ReadOnlyDatabase::open(path).expect("archive reader opens");
        let read = database.begin_read().expect("archive read starts");
        let catalog = read.open_table(SEMA_CATALOG_TABLE).expect("catalog reads");
        let mut archives = BTreeMap::new();
        for entry in catalog.iter().expect("catalog iterates") {
            let (name, _) = entry.expect("catalog entry reads");
            let name = name.value().to_owned();
            if name == GROUP_TABLE {
                continue;
            }
            let definition = redb::TableDefinition::<String, &[u8]>::new(&name);
            let rows = match read.open_table(definition) {
                Ok(table) => table
                    .iter()
                    .expect("family iterates")
                    .map(|row| {
                        let (key, bytes) = row.expect("family archive reads");
                        (key.value(), bytes.value().to_vec())
                    })
                    .collect(),
                Err(redb::TableError::TableDoesNotExist(_)) => Vec::new(),
                Err(error) => panic!("cannot read family {name}: {error}"),
            };
            archives.insert(name, rows);
        }
        archives
    }

    #[test]
    fn current_store_without_group_family_reopens_with_existing_archives_unchanged() {
        let (fixture, store) = Fixture::new();
        let accepted = admit(&store, vec![order("preexisting ordinary request")]);
        assert!(
            store
                .engine
                .match_records(QueryPlan::all(store.logical_deliveries.groups))
                .unwrap()
                .records()
                .is_empty()
        );
        let queue_before = store.metaflow(&address()).unwrap().unwrap().queue;
        let digest_before = store.memory_digest().unwrap();
        drop(store);
        {
            let database = redb::Database::open(&fixture.path).expect("old Current fixture opens");
            let write = database.begin_write().expect("family removal starts");
            let removed = write
                .open_table(SEMA_CATALOG_TABLE)
                .expect("catalog opens")
                .remove(GROUP_TABLE)
                .expect("only group registration removes")
                .expect("group family was registered")
                .value()
                .to_vec();
            assert!(!removed.is_empty());
            write
                .delete_table(redb::TableDefinition::<String, &[u8]>::new(GROUP_TABLE))
                .expect("empty group row table removes when materialized");
            write.commit().expect("old Current fixture commits");
        }
        let raw_before = archived_family_rows(&fixture.path);
        let reopened = fixture.reopen();
        assert!(
            reopened
                .engine
                .match_records(QueryPlan::all(reopened.logical_deliveries.groups))
                .unwrap()
                .records()
                .is_empty()
        );
        assert!(
            reopened
                .delivery_group(&accepted[0].logical_delivery_id)
                .unwrap()
                .is_none()
        );
        assert_eq!(
            reopened.metaflow(&address()).unwrap().unwrap().queue,
            queue_before
        );
        assert_eq!(reopened.memory_digest().unwrap(), digest_before);
        drop(reopened);
        assert_eq!(archived_family_rows(&fixture.path), raw_before);
    }
    #[test]
    fn startup_uncertainty_keeps_exact_prefix_and_blocks_selection_after_reopen() {
        use crate::store::group_uncertainty::StagesUncertainCohorts;
        let (fixture, store) = Fixture::new();
        admit(&store, vec![order("first"), order("second")]);
        let prefix = store.metaflow(&address()).unwrap().unwrap().queue.0;
        admit(&store, vec![order("later")]);
        let before = store.metaflow(&address()).unwrap().unwrap();
        store.mark_startup_uncertain(&address(), &prefix).unwrap();
        assert_eq!(store.metaflow(&address()).unwrap().unwrap(), before);
        for member in &prefix {
            assert_eq!(
                store
                    .logical_delivery_snapshot(&member.logical_delivery_id)
                    .unwrap()
                    .unwrap()
                    .delivery_state,
                DeliveryState::Uncertain
            );
        }
        drop(store);
        let store = fixture.reopen();
        let queue = store.metaflow(&address()).unwrap().unwrap().queue.0;
        assert!(store.complete_delivery_groups(&address(), &queue).is_err());
        let digest = store.memory_digest().unwrap();
        assert!(store.mark_startup_uncertain(&address(), &prefix).is_err());
        assert_eq!(store.memory_digest().unwrap(), digest);
    }

    #[test]
    fn immediate_startup_receipt_atomically_settles_the_premarked_group_only() {
        use crate::store::group_uncertainty::StagesUncertainCohorts;
        let (_fixture, store) = Fixture::new();
        admit(&store, vec![order("first"), order("second")]);
        let prefix = store.metaflow(&address()).unwrap().unwrap().queue.0;
        store.mark_startup_uncertain(&address(), &prefix).unwrap();
        admit(&store, vec![order("later")]);
        let mut awake = store.metaflow(&address()).unwrap().unwrap();
        awake.state = flow_ethos::State::Awake(flow_ethos::FlowId("started".into()));
        store.seed_metaflow(awake);
        let receipt = signal_flow::NativeReceipt {
            harness_kind: signal_flow::HarnessKind::Codex,
            native_session_id: signal_flow::NativeSessionId("startup-session".into()),
            native_prompt_id: signal_flow::NativePromptId("startup-prompt".into()),
        };
        assert!(
            store
                .settle_wake_startup_receipts(&address(), "started", &prefix, Some(&receipt))
                .unwrap()
        );
        assert_eq!(
            store.metaflow(&address()).unwrap().unwrap().queue.0.len(),
            1
        );
        for member in prefix {
            assert_eq!(
                store
                    .logical_delivery_snapshot(&member.logical_delivery_id)
                    .unwrap()
                    .unwrap()
                    .delivery_state,
                DeliveryState::Delivered(receipt.clone())
            );
        }
    }

    #[test]
    fn delayed_startup_parks_the_whole_premarked_group_atomically() {
        use crate::store::group_uncertainty::StagesUncertainCohorts;
        use crate::store::queue_delivery::{AccountsQueueDelivery, PrepareQueueDelivery};
        let (fixture, store) = Fixture::new();
        admit(&store, vec![order("first"), order("second")]);
        let prefix = store.metaflow(&address()).unwrap().unwrap().queue.0;
        store.mark_startup_uncertain(&address(), &prefix).unwrap();
        drop(store);
        let store = fixture.reopen();
        let mut awake = store.metaflow(&address()).unwrap().unwrap();
        awake.state = flow_ethos::State::Awake(flow_ethos::FlowId("started".into()));
        store.seed_metaflow(awake);
        let PrepareQueueDelivery::Prepared(head) = store
            .prepare_oldest_delivery(
                &address(),
                "started",
                "startup-session",
                signal_flow::HarnessKind::Codex,
                crate::store::queue_delivery::NativeHistoryAnchor::CodexTurns(None),
            )
            .unwrap()
        else {
            panic!("head prepares after Started")
        };
        let attempts = store.stage_uncertain_cohort(&head, &prefix).unwrap();
        assert_eq!(attempts.len(), prefix.len());
        assert!(
            store
                .metaflow(&address())
                .unwrap()
                .unwrap()
                .queue
                .0
                .is_empty()
        );
        drop(store);
        let store = fixture.reopen();
        assert_eq!(
            store.unsettled_deliveries(&address()).unwrap().len(),
            prefix.len()
        );
        assert!(
            store
                .metaflow(&address())
                .unwrap()
                .unwrap()
                .queue
                .0
                .is_empty()
        );
    }
    #[test]
    fn rejected_start_keeps_premarked_group_uncertain_without_retrying_it() {
        use crate::store::group_uncertainty::StagesUncertainCohorts;
        use crate::store::queue_delivery::AccountsQueueDelivery;
        let (fixture, store) = Fixture::new();
        admit(&store, vec![order("first"), order("second")]);
        let prefix = store.metaflow(&address()).unwrap().unwrap().queue.0;
        let digest = store.memory_digest().unwrap();
        assert!(
            store
                .mark_startup_uncertain(&address(), &prefix[1..])
                .is_err()
        );
        assert_eq!(store.memory_digest().unwrap(), digest);
        store.mark_startup_uncertain(&address(), &prefix).unwrap();
        let cause =
            signal_flow::RetryCause::WakeRefused(signal_flow::StartRejection::ContextRefused(
                signal_flow::ContextRefusal::ModuleMissing(None),
            ));
        assert_eq!(store.mark_queue_retryable(&prefix, cause).unwrap(), 0);
        drop(store);
        let store = fixture.reopen();
        assert_eq!(store.metaflow(&address()).unwrap().unwrap().queue.0, prefix);
        assert!(store.oldest_delivery_group(&address(), &prefix).is_err());
        assert!(store.unsettled_deliveries(&address()).unwrap().is_empty());
    }
    fn withdraw_and_archive(store: &FlowStore, member: &QueueEntry) {
        assert!(
            store
                .withdraw_queued(&address(), &member.logical_delivery_id.0)
                .unwrap()
                .is_ok()
        );
        assert!(store.archive_request(&member.request_id.0).unwrap().is_ok());
    }

    fn cancelled_positions_preserve_survivors(cancelled: &[usize]) {
        let (fixture, store) = Fixture::new();
        admit(&store, vec![order("first"), order("middle"), order("last")]);
        let original = store.metaflow(&address()).unwrap().unwrap().queue.0;
        let group = store
            .delivery_group(&original[0].logical_delivery_id)
            .unwrap()
            .unwrap();
        let later = admit(&store, vec![order("later")]);
        for index in cancelled {
            withdraw_and_archive(&store, &original[*index]);
        }
        let survivors = original
            .iter()
            .enumerate()
            .filter(|(index, _)| !cancelled.contains(index))
            .map(|(_, member)| member.clone())
            .collect::<Vec<_>>();
        let queue = store.metaflow(&address()).unwrap().unwrap().queue.0;
        let selected = store.oldest_delivery_group(&address(), &queue).unwrap();
        if survivors.is_empty() {
            assert_eq!(selected[0].request_id, later[0].request_id);
        } else {
            assert_eq!(selected, survivors);
        }
        assert_eq!(
            store.complete_delivery_groups(&address(), &queue).unwrap(),
            queue
        );
        assert_eq!(
            store
                .delivery_group(&original[0].logical_delivery_id)
                .unwrap()
                .unwrap(),
            group
        );
        for index in cancelled {
            assert_eq!(
                store
                    .logical_delivery_snapshot(&original[*index].logical_delivery_id)
                    .unwrap()
                    .unwrap()
                    .delivery_state,
                DeliveryState::Withdrawn
            );
        }
        drop(store);
        let store = fixture.reopen();
        assert_eq!(
            store.complete_delivery_groups(&address(), &queue).unwrap(),
            queue
        );
        assert_eq!(
            store.oldest_delivery_group(&address(), &queue).unwrap(),
            selected
        );
        assert_eq!(
            store
                .delivery_group(&original[0].logical_delivery_id)
                .unwrap()
                .unwrap(),
            group
        );
    }

    #[test]
    fn cancel_first_preserves_group_survivors_and_later_work_after_reopen() {
        cancelled_positions_preserve_survivors(&[0]);
    }
    #[test]
    fn cancel_middle_preserves_group_survivors_and_later_work_after_reopen() {
        cancelled_positions_preserve_survivors(&[1]);
    }
    #[test]
    fn cancel_last_preserves_group_survivors_and_later_work_after_reopen() {
        cancelled_positions_preserve_survivors(&[2]);
    }
    #[test]
    fn cancel_all_leaves_later_work_selectable_without_group_delivery() {
        cancelled_positions_preserve_survivors(&[0, 1, 2]);
    }
    #[test]
    fn cancel_two_keeps_single_survivor_as_original_group_unit() {
        cancelled_positions_preserve_survivors(&[0, 2]);
    }

    #[test]
    fn omitted_group_member_requires_positive_exact_withdrawal_evidence() {
        for corruption in 0..4 {
            let (_fixture, store) = Fixture::new();
            admit(&store, vec![order("first"), order("second")]);
            let original = store.metaflow(&address()).unwrap().unwrap().queue.0;
            withdraw_and_archive(&store, &original[0]);
            let key = RecordKey::new(original[0].logical_delivery_id.0.clone());
            let commit = match corruption {
                0 => store
                    .engine
                    .begin_atomic_commit()
                    .retract(store.logical_deliveries.current, key),
                1 => store
                    .engine
                    .begin_atomic_commit()
                    .retract(store.queue_delivery.requests, key),
                _ => {
                    let rows = store
                        .engine
                        .match_records(QueryPlan::key(store.logical_deliveries.current, key))
                        .unwrap();
                    let [current] = rows.records() else {
                        panic!("withdrawn row exists")
                    };
                    let mut changed = current.clone();
                    if corruption == 2 {
                        changed.delivery_snapshot.delivery_state = DeliveryState::Archived;
                    } else {
                        changed.delivery_snapshot.request_id =
                            flow_ethos::RequestId("wrong".into());
                    }
                    store
                        .engine
                        .begin_atomic_commit()
                        .mutate(store.logical_deliveries.current, changed)
                }
            };
            store.engine.commit_atomic(commit).unwrap();
            let digest = store.memory_digest().unwrap();
            let queue = store.metaflow(&address()).unwrap().unwrap().queue.0;
            assert!(store.oldest_delivery_group(&address(), &queue).is_err());
            assert_eq!(store.memory_digest().unwrap(), digest);
        }
    }

    #[test]
    fn cancellation_before_premark_freezes_only_survivors_and_later_cancel_cannot_rewrite_them() {
        use crate::store::group_uncertainty::StagesUncertainCohorts;
        use crate::store::logical_delivery::HoldsOpenRequests;
        let (fixture, store) = Fixture::new();
        admit(&store, vec![order("first"), order("middle"), order("last")]);
        let original = store.metaflow(&address()).unwrap().unwrap().queue.0;
        let group = store
            .delivery_group(&original[0].logical_delivery_id)
            .unwrap()
            .unwrap();
        withdraw_and_archive(&store, &original[0]);
        let queue = store.metaflow(&address()).unwrap().unwrap().queue.0;
        let survivors = store.oldest_delivery_group(&address(), &queue).unwrap();
        store
            .mark_startup_uncertain(&address(), &survivors)
            .unwrap();
        // This is the production Cancel branch predicate after the cutoff:
        // reached inputs are not withdrawn; ordinary archive may refuse NotYet.
        assert!(
            store
                .delivered_to_be_answered(&survivors[0].logical_delivery_id)
                .unwrap()
        );
        assert!(matches!(
            store.archive_request(&survivors[0].request_id.0).unwrap(),
            Err(signal_flow::ArchiveRejection::NotYet)
        ));
        assert_eq!(
            store.metaflow(&address()).unwrap().unwrap().queue.0,
            survivors
        );
        assert_eq!(
            store
                .delivery_group(&original[0].logical_delivery_id)
                .unwrap()
                .unwrap(),
            group
        );
        drop(store);
        let store = fixture.reopen();
        assert_eq!(
            store.metaflow(&address()).unwrap().unwrap().queue.0,
            survivors
        );
        assert!(store.oldest_delivery_group(&address(), &survivors).is_err());
    }
    struct ForbiddenDraw;
    impl DrawsRequestId for ForbiddenDraw {
        fn draw_request_id(&self) -> Result<flow_ethos::RequestId, StoreError> {
            panic!("a refused preflight must not draw an ID")
        }
    }

    #[test]
    fn singleton_batch_has_provenance_plain_send_and_empty_batch_do_not() {
        let (fixture, store) = Fixture::new();
        let plain = admit_plain(&store, order("plain"));
        let singleton = admit(&store, vec![order("singleton batch")]);
        assert!(
            store
                .delivery_group(&plain[0].logical_delivery_id)
                .unwrap()
                .is_none()
        );
        let group = store
            .delivery_group(&singleton[0].logical_delivery_id)
            .unwrap()
            .unwrap();
        assert_eq!(
            group.logical_delivery_id_vector,
            vec![singleton[0].logical_delivery_id.clone()]
        );
        let digest = store.memory_digest().unwrap();
        assert!(admit(&store, vec![]).is_empty());
        assert_eq!(store.memory_digest().unwrap(), digest);
        drop(store);
        let store = fixture.reopen();
        assert_eq!(
            store
                .delivery_group(&singleton[0].logical_delivery_id)
                .unwrap(),
            Some(group)
        );
        assert!(
            store
                .delivery_group(&plain[0].logical_delivery_id)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn preflight_oversize_precedes_invalid_minutes_without_drawing() {
        let (_fixture, mut store) = Fixture::new();
        store.set_request_id_drawer_for_tests(Arc::new(ForbiddenDraw));
        let digest = store.memory_digest().unwrap();
        let result = store
            .admit_delivery_group(
                &address(),
                SentBy::Unbound,
                vec![
                    task(0),
                    order(&"x".repeat(crate::sending::MAX_SEND_PAYLOAD_BYTES)),
                ],
            )
            .unwrap();
        assert!(matches!(result, Err(Refusal::Oversized(_))));
        assert_eq!(store.memory_digest().unwrap(), digest);
    }

    #[test]
    fn preflight_late_invalid_minutes_never_draws_earlier_id() {
        let (_fixture, mut store) = Fixture::new();
        store.set_request_id_drawer_for_tests(Arc::new(ForbiddenDraw));
        let digest = store.memory_digest().unwrap();
        assert!(matches!(
            store
                .admit_delivery_group(
                    &address(),
                    SentBy::Unbound,
                    vec![order("valid first"), task(0)]
                )
                .unwrap(),
            Err(Refusal::MinutesRefused(_))
        ));
        assert_eq!(store.memory_digest().unwrap(), digest);
    }

    #[test]
    fn whole_group_refusal_preserves_cancelled_history_and_later_liveness_on_reopen() {
        let (fixture, store) = Fixture::new();
        let accepted = admit(
            &store,
            vec![order("first"), order("cancelled"), order("last")],
        );
        let original = store.metaflow(&address()).unwrap().unwrap().queue.0;
        let group = store
            .delivery_group(&accepted[0].logical_delivery_id)
            .unwrap();
        withdraw_and_archive(&store, &original[1]);
        let later = admit(&store, vec![order("later")]);
        let queue = store.metaflow(&address()).unwrap().unwrap().queue.0;
        let selected = store.oldest_delivery_group(&address(), &queue).unwrap();
        let reason = Refusal::Unresolved;
        store
            .refuse_delivery_group(&address(), &selected, reason.clone())
            .unwrap();
        for member in &selected {
            assert_eq!(
                store
                    .logical_delivery_snapshot(&member.logical_delivery_id)
                    .unwrap()
                    .unwrap()
                    .delivery_state,
                DeliveryState::Failed(reason.clone())
            );
        }
        assert_eq!(
            store
                .logical_delivery_snapshot(&original[1].logical_delivery_id)
                .unwrap()
                .unwrap()
                .delivery_state,
            DeliveryState::Withdrawn
        );
        assert_eq!(
            store
                .delivery_group(&accepted[0].logical_delivery_id)
                .unwrap(),
            group
        );
        let queue = store.metaflow(&address()).unwrap().unwrap().queue.0;
        assert_eq!(queue.len(), 1);
        assert_eq!(queue[0].request_id, later[0].request_id);
        assert_eq!(
            store.oldest_delivery_group(&address(), &queue).unwrap(),
            queue
        );
        drop(store);
        let store = fixture.reopen();
        assert_eq!(store.metaflow(&address()).unwrap().unwrap().queue.0, queue);
        assert_eq!(
            store
                .delivery_group(&accepted[0].logical_delivery_id)
                .unwrap(),
            group
        );
    }

    #[test]
    fn partial_group_refusal_rejects_without_mutation() {
        let (_fixture, store) = Fixture::new();
        admit(&store, vec![order("first"), order("second")]);
        let queue = store.metaflow(&address()).unwrap().unwrap().queue.0;
        let digest = store.memory_digest().unwrap();
        assert!(
            store
                .refuse_delivery_group(&address(), &queue[..1], Refusal::Unresolved)
                .is_err()
        );
        assert_eq!(store.memory_digest().unwrap(), digest);
    }
    #[test]
    fn batch_capacity_check_refuses_without_draw_or_mutation() {
        let (_fixture, mut store) = Fixture::new();
        store.set_request_id_drawer_for_tests(Arc::new(ForbiddenDraw));
        let digest = store.memory_digest().unwrap();
        assert_eq!(
            store
                .validate_delivery_group_capacity(
                    super::super::logical_delivery::REQUEST_ID_SPACE + 1
                )
                .unwrap(),
            Err(Refusal::RequestIdSpaceFull)
        );
        assert_eq!(store.memory_digest().unwrap(), digest);
    }
}
