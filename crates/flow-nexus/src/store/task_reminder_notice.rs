//! The Notices Flow queues about a Task that has no Done, each reminder and
//! the failure that comes with the last, tied to their Task by a record of
//! their own, so that whatever closes the Task withdraws the ones still
//! waiting in a queue: a Done naming it, a Cancel, a CloseRequest, an
//! ArchiveRequest. A Notice already handed to its seat or delivered stays
//! as it is. The family is new in 0.60.1: a store written before holds none
//! and is opened with it registered empty.

use super::logical_delivery::{LogicalDeliveryTransition, RecordsLogicalDeliveries};
use super::metaflow::SleepsMetaflows;
use super::queue_delivery::{AccountsQueueDelivery, EndsQueueDeliveryPhase};
use super::{FlowStore, StoreError};
pub(crate) use crate::generated::memory::{RemindedTask, TaskReminderNotice};
use sema_engine::{
    EngineRecord, FamilyName, MemoryDefinition, MemoryVersion, QueryPlan, RecordKey, SchemaHash,
    TableDescriptor, TableName,
};
use signal_flow::{Address, LogicalDeliveryId, Message, Request, RequestId, SentBy};

pub(super) const TASK_REMINDER_NOTICE_TABLE: &str = "flow_nexus_task_reminder_notices";
pub(super) const TASK_REMINDER_NOTICE_FAMILY: &str = "flow-nexus-task-reminder-notice";
pub(super) const TASK_REMINDER_NOTICE_SCHEMA: &str = "flow-nexus-task-reminder-notice-v1";

impl EngineRecord for TaskReminderNotice {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.logical_delivery_id.0.clone())
    }
}

impl MemoryDefinition for TaskReminderNotice {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(1)
    }

    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label(TASK_REMINDER_NOTICE_SCHEMA)
    }
}

pub(super) trait DescribesTaskReminderNoticeTable {
    fn descriptor() -> TableDescriptor<TaskReminderNotice>;
}

impl DescribesTaskReminderNoticeTable for TaskReminderNotice {
    fn descriptor() -> TableDescriptor<TaskReminderNotice> {
        TableDescriptor::new(
            TableName::new(TASK_REMINDER_NOTICE_TABLE),
            FamilyName::new(TASK_REMINDER_NOTICE_FAMILY),
            SchemaHash::for_label(TASK_REMINDER_NOTICE_SCHEMA),
        )
    }
}

/// The Task a reminder Notice is about, as the Notice is admitted: the
/// logical delivery the Task was bound to and the id it had. The Notice's
/// own logical delivery is drawn by its admission, which records the tie in
/// the same commit.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ReminderOfTask {
    pub(crate) task: RemindedTask,
    pub(crate) request_id: RequestId,
}

impl ReminderOfTask {
    /// The record tying the Notice admitted as `notice` to its Task.
    pub(super) fn notice(&self, notice: LogicalDeliveryId) -> TaskReminderNotice {
        TaskReminderNotice {
            logical_delivery_id: notice,
            reminded_task: self.task.clone(),
            request_id: self.request_id.clone(),
        }
    }
}

pub(crate) trait TiesNoticesToTasks {
    /// Queues the Notice `text` from Flow for `address`, the Task's sender,
    /// tied to the Task `of` names.
    fn accept_task_reminder_notice(
        &self,
        address: &Address,
        text: String,
        of: ReminderOfTask,
    ) -> Result<signal_flow::PrivateSendResponse, StoreError>;
    /// The Task bound to `task` is closed: every Notice tied to it that is
    /// still queued and that no hand-over holds leaves its queue unsent and
    /// ends Withdrawn, and every tie to it is retracted; a Notice already
    /// handed over or delivered stays as it stands. Called once the commit
    /// that closed the Task is made, with none of the store's write gates
    /// held. The Task stays closed whatever this meets: a store that
    /// refuses leaves the Notices queued and is logged.
    fn close_task_reminder_notices(&self, task: &LogicalDeliveryId);
}

impl TiesNoticesToTasks for FlowStore {
    fn accept_task_reminder_notice(
        &self,
        address: &Address,
        text: String,
        of: ReminderOfTask,
    ) -> Result<signal_flow::PrivateSendResponse, StoreError> {
        self.accept_logical_delivery_for_with_transition(
            address,
            SentBy::Flow,
            flow_ethos::Delivery::Queue,
            Message::Soft(Request::Notice(text)),
            None,
            LogicalDeliveryTransition {
                purpose: crate::heartbeat::DeliveryPurpose::Ordinary,
                handover_reminder: None,
                deliver_call: None,
                upgrade_notice: None,
                context_warning: None,
                task_reminder: Some(of),
            },
        )
    }

    fn close_task_reminder_notices(&self, task: &LogicalDeliveryId) {
        if let Err(error) = self.withdraw_task_reminder_notices(task) {
            eprintln!(
                "flow-nexus: the reminder Notices of closed Task {} were not withdrawn: {error}",
                task.0
            );
        }
    }
}

trait WithdrawsTaskReminderNotices {
    /// The ties to the Task bound to `task`, oldest Notice first.
    fn ties_to(&self, task: &LogicalDeliveryId) -> Result<Vec<TaskReminderNotice>, StoreError>;
    /// What `close_task_reminder_notices` does, answering what refused.
    fn withdraw_task_reminder_notices(&self, task: &LogicalDeliveryId) -> Result<(), StoreError>;
}

impl WithdrawsTaskReminderNotices for FlowStore {
    fn withdraw_task_reminder_notices(&self, task: &LogicalDeliveryId) -> Result<(), StoreError> {
        let ties = self.ties_to(task)?;
        if ties.is_empty() {
            return Ok(());
        }
        for tie in &ties {
            let Some(snapshot) = self.logical_delivery_snapshot(&tie.logical_delivery_id)? else {
                continue;
            };
            let Some(metaflow) = self.metaflow(&snapshot.address)? else {
                continue;
            };
            let queued = metaflow
                .queue
                .0
                .iter()
                .any(|entry| entry.logical_delivery_id == tie.logical_delivery_id);
            if !queued {
                continue;
            }
            // A Notice handed to its seat may already be there: it is left
            // to its hand-over.
            let handed_over = self
                .delivery_attempts_for(&tie.logical_delivery_id.0)?
                .iter()
                .any(|attempt| !attempt.queue_delivery_phase.is_finished());
            if handed_over {
                continue;
            }
            if let Err(rejected) =
                self.withdraw_queued(&snapshot.address, &tie.logical_delivery_id.0)?
            {
                eprintln!(
                    "flow-nexus: a reminder Notice {} of a closed Task stayed queued: {rejected:?}",
                    tie.logical_delivery_id.0
                );
            }
        }
        let commit = ties
            .iter()
            .fold(self.engine.begin_atomic_commit(), |commit, tie| {
                commit.retract(
                    self.task_reminder_notices,
                    RecordKey::new(tie.logical_delivery_id.0.clone()),
                )
            });
        self.engine.commit_atomic(commit)?;
        Ok(())
    }

    fn ties_to(&self, task: &LogicalDeliveryId) -> Result<Vec<TaskReminderNotice>, StoreError> {
        let mut ties: Vec<TaskReminderNotice> = self
            .engine
            .match_records(QueryPlan::all(self.task_reminder_notices))?
            .records()
            .iter()
            .filter(|tie| tie.reminded_task.0 == *task)
            .cloned()
            .collect();
        ties.sort_by(|left, right| left.logical_delivery_id.cmp(&right.logical_delivery_id));
        Ok(ties)
    }
}
