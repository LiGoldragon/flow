//! Work's terminal choice is committed with its lifecycle, before delivery.

use super::queue_delivery::AccountsQueueDelivery as _;
use super::*;
use crate::generated::memory::{
    CommittedWorkOutcome, PendingWorkNotification, RequestIdBinding, StoredWorkNotificationDefault,
};
use flow_ethos::{NotificationType, WorkNotification, WorkNotificationPolicy, WorkRequest};
use meta_signal_flow::{DefaultNotificationDestination, DefaultNotificationDestinations};
use sema_engine::AtomicCommit;
use signal_flow::{Message, Refusal, Request, SentBy};

/// A newly appended outbox is the event that starts dispatch. The
/// transaction's caller announces only after the commit succeeds.
#[derive(Clone, Copy, Default)]
pub(super) enum WorkOutboxChange {
    #[default]
    Unchanged,
    Appended,
}

pub(super) trait AnnouncesWorkOutboxChange {
    fn announce(self, store: &FlowStore);
}

impl AnnouncesWorkOutboxChange for WorkOutboxChange {
    fn announce(self, store: &FlowStore) {
        if matches!(self, Self::Appended) {
            store.work_notice_changes.announce();
        }
    }
}

pub(crate) struct StagedWorkOutcome {
    pub(super) commit: AtomicCommit,
    pub(super) outbox: WorkOutboxChange,
}

pub(super) const DEFAULT_TABLE: &str = "flow_nexus_work_notification_defaults";
pub(super) const DEFAULT_FAMILY: &str = "flow-nexus-work-notification-default";
pub(super) const DEFAULT_SCHEMA: &str = "flow-nexus-work-notification-default-v1";
pub(super) const OUTCOME_TABLE: &str = "flow_nexus_committed_work_outcomes";
pub(super) const OUTCOME_FAMILY: &str = "flow-nexus-committed-work-outcome";
pub(super) const OUTCOME_SCHEMA: &str = "flow-nexus-committed-work-outcome-v1";
pub(super) const OUTBOX_TABLE: &str = "flow_nexus_pending_work_notifications";
pub(super) const OUTBOX_FAMILY: &str = "flow-nexus-pending-work-notification";
pub(super) const OUTBOX_SCHEMA: &str = "flow-nexus-pending-work-notification-v1";

trait IdentifiesNotificationDefault {
    fn default_key(&self) -> RecordKey;
}

impl IdentifiesNotificationDefault for NotificationType {
    fn default_key(&self) -> RecordKey {
        RecordKey::new(match self {
            Self::WorkDone => "WorkDone",
            Self::WorkCancelled => "WorkCancelled",
        })
    }
}

impl EngineRecord for StoredWorkNotificationDefault {
    fn record_key(&self) -> RecordKey {
        self.notification_type.default_key()
    }
}

impl EngineRecord for CommittedWorkOutcome {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.logical_delivery_id.0.clone())
    }
}

impl EngineRecord for PendingWorkNotification {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(format!(
            "{}:{}",
            self.logical_delivery_id.0,
            FlowStore::address_record_key(&self.address)
        ))
    }
}

macro_rules! notification_family {
    ($record:ty, $table:expr, $family:expr, $schema:expr) => {
        impl MemoryDefinition for $record {
            fn memory_version() -> MemoryVersion {
                MemoryVersion::new(1)
            }

            fn schema_hash() -> SchemaHash {
                SchemaHash::for_label($schema)
            }
        }

        impl DescribesNotificationFamily for $record {
            fn descriptor() -> TableDescriptor<Self> {
                TableDescriptor::new(
                    TableName::new($table),
                    FamilyName::new($family),
                    Self::schema_hash(),
                )
            }
        }
    };
}

pub(super) trait DescribesNotificationFamily: Sized {
    fn descriptor() -> TableDescriptor<Self>;
}

notification_family!(
    StoredWorkNotificationDefault,
    DEFAULT_TABLE,
    DEFAULT_FAMILY,
    DEFAULT_SCHEMA
);
notification_family!(
    CommittedWorkOutcome,
    OUTCOME_TABLE,
    OUTCOME_FAMILY,
    OUTCOME_SCHEMA
);
notification_family!(
    PendingWorkNotification,
    OUTBOX_TABLE,
    OUTBOX_FAMILY,
    OUTBOX_SCHEMA
);

pub(crate) trait ClassifiesRoutineNotice {
    fn routine_notice(&self, delivery: &flow_ethos::Delivery) -> bool;
}

impl ClassifiesRoutineNotice for Request {
    fn routine_notice(&self, delivery: &flow_ethos::Delivery) -> bool {
        match self {
            Request::Notice(_) | Request::WorkNotification(_) => true,
            Request::Reminder(_) => *delivery == flow_ethos::Delivery::Queue,
            _ => false,
        }
    }
}

pub(crate) trait KeepsWorkNotices {
    fn notification_defaults(&self) -> Result<DefaultNotificationDestinations, StoreError>;
    fn reserve_routine_notices_for_inspection(&self, address: &Address) -> Result<(), StoreError>;
    fn work_admission_refusal(
        &self,
        request: &WorkRequest,
        sender: &SentBy,
    ) -> Result<Option<Refusal>, StoreError>;
    fn stage_work_outcome(
        &self,
        binding: &RequestIdBinding,
        kind: NotificationType,
        actor: Option<&Address>,
        commit: AtomicCommit,
    ) -> Result<StagedWorkOutcome, StoreError>;
    fn pending_work_notices(&self) -> Result<Vec<PendingWorkNotification>, StoreError>;
    fn work_outcome(
        &self,
        request: &flow_ethos::RequestId,
    ) -> Result<Option<meta_signal_flow::WorkOutcomeRecord>, StoreError>;
    fn work_notification(
        &self,
        pending: &PendingWorkNotification,
    ) -> Result<Option<WorkNotification>, StoreError>;
    fn close_work_notice(
        &self,
        pending: PendingWorkNotification,
        commit: AtomicCommit,
    ) -> AtomicCommit;
}

trait ResolvesNotificationDefault {
    fn notification_default(&self, kind: &NotificationType) -> Result<Option<Address>, StoreError>;
}

impl ResolvesNotificationDefault for FlowStore {
    fn notification_default(&self, kind: &NotificationType) -> Result<Option<Address>, StoreError> {
        let rows = self.engine.match_records(QueryPlan::key(
            self.meta_configuration_tables.work_notification_defaults,
            kind.default_key(),
        ))?;
        match rows.records() {
            [] => Ok(None),
            [row] => Ok(Some(row.address.clone())),
            _ => Err(StoreError::StateInvariant),
        }
    }
}

impl KeepsWorkNotices for FlowStore {
    fn notification_defaults(&self) -> Result<DefaultNotificationDestinations, StoreError> {
        let mut destinations = Vec::new();
        for kind in [NotificationType::WorkDone, NotificationType::WorkCancelled] {
            if let Some(address) = self.notification_default(&kind)? {
                destinations.push(DefaultNotificationDestination {
                    notification_type: kind,
                    address,
                });
            }
        }
        Ok(DefaultNotificationDestinations(destinations))
    }

    fn reserve_routine_notices_for_inspection(&self, address: &Address) -> Result<(), StoreError> {
        use super::metaflow::SleepsMetaflows as _;
        use super::queue_delivery::QueueDeparture;
        if !self.designer_notice_policy()?.0.contains(address) {
            return Ok(());
        }
        let Some(metaflow) = self.metaflow(address)? else {
            return Ok(());
        };
        for entry in metaflow.queue.0 {
            let (Message::HardAbrupt(request)
            | Message::MiddleAbrupt(request)
            | Message::Soft(request)) = &entry.message;
            if request.routine_notice(&entry.delivery)
                && self
                    .move_queued(
                        address,
                        &entry.logical_delivery_id.0,
                        QueueDeparture::InspectionOnly,
                    )?
                    .is_err()
            {
                return Err(StoreError::StateInvariant);
            }
        }
        Ok(())
    }

    fn work_admission_refusal(
        &self,
        request: &WorkRequest,
        sender: &SentBy,
    ) -> Result<Option<Refusal>, StoreError> {
        use crate::launching::NamesTopic as _;
        let policy = &request.work_notification_policy;
        if matches!(policy, WorkNotificationPolicy::Quiet) {
            return Ok(None);
        }
        if !matches!(sender, SentBy::Sender(_)) {
            return Ok(Some(Refusal::UnaddressedNotificationSender));
        }
        match policy {
            WorkNotificationPolicy::Quiet => {}
            WorkNotificationPolicy::Default => {
                for kind in [NotificationType::WorkDone, NotificationType::WorkCancelled] {
                    if self.notification_default(&kind)?.is_none() {
                        return Ok(Some(Refusal::MissingDefaultNotificationDestination(kind)));
                    }
                }
            }
            WorkNotificationPolicy::Destinations(routes) => {
                if let Some(empty) = routes.iter().find(|route| route.address_vector.is_empty()) {
                    return Ok(Some(Refusal::EmptyNotificationDestinations(
                        empty.notification_type.clone(),
                    )));
                }
                for address in routes.iter().flat_map(|rule| &rule.address_vector) {
                    if !address.topic.0.as_str().is_camel_case_topic() {
                        return Ok(Some(Refusal::TopicRefused(address.topic.clone())));
                    }
                }
            }
        }
        Ok(None)
    }

    fn stage_work_outcome(
        &self,
        binding: &RequestIdBinding,
        kind: NotificationType,
        actor: Option<&Address>,
        commit: AtomicCommit,
    ) -> Result<StagedWorkOutcome, StoreError> {
        let unchanged = |commit| {
            Ok(StagedWorkOutcome {
                commit,
                outbox: WorkOutboxChange::Unchanged,
            })
        };
        let Some(retained) = self.retained_logical_request(&binding.logical_delivery_id)? else {
            return unchanged(commit);
        };
        let (Message::HardAbrupt(request)
        | Message::MiddleAbrupt(request)
        | Message::Soft(request)) = retained.entry.message;
        let Request::Work(work) = request else {
            return unchanged(commit);
        };
        let policy = work.work_notification_policy;
        if matches!(policy, WorkNotificationPolicy::Quiet) {
            return unchanged(commit);
        }
        let SentBy::Sender(address) = retained.entry.sent_by else {
            return Err(StoreError::StateInvariant);
        };
        let mut destinations = match policy {
            WorkNotificationPolicy::Quiet => Vec::new(),
            WorkNotificationPolicy::Default => vec![
                self.notification_default(&kind)?
                    .ok_or(StoreError::StateInvariant)?,
            ],
            WorkNotificationPolicy::Destinations(routes) => routes
                .into_iter()
                .filter(|rule| rule.notification_type == kind)
                .flat_map(|rule| rule.address_vector)
                .collect(),
        };
        destinations.retain(|destination| Some(destination) != actor);
        let mut seen = std::collections::HashSet::new();
        destinations.retain(|destination| seen.insert(destination.clone()));
        let commit = commit.assert(
            self.logical_deliveries.work_outcomes,
            CommittedWorkOutcome {
                logical_delivery_id: binding.logical_delivery_id.clone(),
                work_notification: WorkNotification {
                    request_id: binding.request_id.clone(),
                    notification_type: kind,
                    address,
                },
                address_vector: destinations.clone(),
            },
        );
        let outbox = if destinations.is_empty() {
            WorkOutboxChange::Unchanged
        } else {
            WorkOutboxChange::Appended
        };
        let commit = destinations.into_iter().fold(commit, |commit, address| {
            commit.assert(
                self.logical_deliveries.work_notice_outbox,
                PendingWorkNotification {
                    logical_delivery_id: binding.logical_delivery_id.clone(),
                    address,
                },
            )
        });
        Ok(StagedWorkOutcome { commit, outbox })
    }

    fn work_outcome(
        &self,
        request: &flow_ethos::RequestId,
    ) -> Result<Option<meta_signal_flow::WorkOutcomeRecord>, StoreError> {
        use super::logical_delivery::MintsRequestIds as _;
        let Some(binding) = self.request_binding(request)? else {
            return Ok(None);
        };
        let rows = self.engine.match_records(QueryPlan::key(
            self.logical_deliveries.work_outcomes,
            RecordKey::new(binding.logical_delivery_id.0),
        ))?;
        match rows.records() {
            [] => Ok(None),
            [row] => Ok(Some(meta_signal_flow::WorkOutcomeRecord {
                work_notification: row.work_notification.clone(),
                address_vector: row.address_vector.clone(),
            })),
            _ => Err(StoreError::StateInvariant),
        }
    }

    fn pending_work_notices(&self) -> Result<Vec<PendingWorkNotification>, StoreError> {
        Ok(self
            .engine
            .match_records(QueryPlan::all(self.logical_deliveries.work_notice_outbox))?
            .records()
            .to_vec())
    }

    fn work_notification(
        &self,
        pending: &PendingWorkNotification,
    ) -> Result<Option<WorkNotification>, StoreError> {
        let pending_rows = self.engine.match_records(QueryPlan::key(
            self.logical_deliveries.work_notice_outbox,
            pending.record_key(),
        ))?;
        if pending_rows.records() != std::slice::from_ref(pending) {
            return Ok(None);
        }
        let rows = self.engine.match_records(QueryPlan::key(
            self.logical_deliveries.work_outcomes,
            RecordKey::new(pending.logical_delivery_id.0.clone()),
        ))?;
        match rows.records() {
            [row] if row.address_vector.contains(&pending.address) => {
                Ok(Some(row.work_notification.clone()))
            }
            _ => Err(StoreError::StateInvariant),
        }
    }

    fn close_work_notice(
        &self,
        pending: PendingWorkNotification,
        commit: AtomicCommit,
    ) -> AtomicCommit {
        commit.retract(
            self.logical_deliveries.work_notice_outbox,
            pending.record_key(),
        )
    }
}
