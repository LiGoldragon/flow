//! Reminders asked for by `Remind.{ Address Minutes String }`. Each is a
//! request like any other: it is named by a request id minted when it is
//! asked, bound open until it is sent or cancelled, and kept in Flow's
//! Memory until its minutes lapse, so a restart of the Nexus keeps it. When
//! they lapse the Address it names is sent `Reminder.{ RequestId String }`.

use super::logical_delivery::{MintsRequestIds, RequestClosure, RequestIdBinding};
use super::{AnnouncesLaunchChanges, FlowStore, StoreError};
pub(crate) use crate::generated::memory::ScheduledReminder;
use sema_engine::{
    EngineRecord, FamilyName, MemoryDefinition, MemoryVersion, QueryPlan, RecordKey, SchemaHash,
    TableDescriptor, TableName,
};
use signal_flow::{Address, Refusal, RequestId, SentBy};

pub(super) const SCHEDULED_REMINDER_TABLE_NAME: TableName =
    TableName::new("flow_nexus_scheduled_reminders");
pub(super) const SCHEDULED_REMINDER_FAMILY: &str = "flow-nexus-scheduled-reminder";
pub(super) const SCHEDULED_REMINDER_SCHEMA: &str = "flow-nexus-scheduled-reminder-v1";

impl EngineRecord for ScheduledReminder {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.request_id.0.clone())
    }
}

impl MemoryDefinition for ScheduledReminder {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(1)
    }

    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label(SCHEDULED_REMINDER_SCHEMA)
    }
}

pub(super) trait DescribesScheduledReminderTable {
    fn descriptor() -> TableDescriptor<ScheduledReminder>;
}

impl DescribesScheduledReminderTable for ScheduledReminder {
    fn descriptor() -> TableDescriptor<ScheduledReminder> {
        TableDescriptor::new(
            SCHEDULED_REMINDER_TABLE_NAME,
            FamilyName::new(SCHEDULED_REMINDER_FAMILY),
            SchemaHash::for_label(SCHEDULED_REMINDER_SCHEMA),
        )
    }
}

/// The logical delivery a reminder's request is bound to until it is sent:
/// none exists yet, so its binding names the reminder itself.
pub(crate) trait MarksScheduledReminder {
    fn scheduled_marker(&self) -> signal_flow::LogicalDeliveryId;
}

impl MarksScheduledReminder for RequestId {
    fn scheduled_marker(&self) -> signal_flow::LogicalDeliveryId {
        signal_flow::LogicalDeliveryId(format!("reminder:{}", self.0))
    }
}

pub(crate) trait SchedulesReminders {
    /// Mints the reminder's request id and keeps it, due at `due`, its
    /// request bound open, in one commit; the refusal when no id can be
    /// drawn.
    fn schedule_reminder(
        &self,
        address: Address,
        sent_by: SentBy,
        due: i64,
        text: String,
    ) -> Result<Result<RequestId, Refusal>, StoreError>;
    /// The reminder `request_id` names, while it is not yet sent.
    fn scheduled_reminder(
        &self,
        request_id: &RequestId,
    ) -> Result<Option<ScheduledReminder>, StoreError>;
    /// Every reminder due at `now`.
    fn due_scheduled_reminders(&self, now: i64) -> Result<Vec<ScheduledReminder>, StoreError>;
    /// The earliest time after `now` a reminder falls due.
    fn next_scheduled_reminder_due(&self, now: i64) -> Result<Option<i64>, StoreError>;
    /// Takes the reminder away and closes its request, as answered by
    /// `closed_by`: the logical delivery that carried it, or the marker of a
    /// cancellation.
    fn retire_scheduled_reminder(
        &self,
        reminder: &ScheduledReminder,
        closed_by: signal_flow::LogicalDeliveryId,
    ) -> Result<(), StoreError>;
    /// Moves a kept reminder's due time, as its minutes lapsing would.
    #[cfg(test)]
    fn rewind_scheduled_reminder(&self, request_id: &RequestId, due: i64)
    -> Result<(), StoreError>;
}

impl SchedulesReminders for FlowStore {
    fn schedule_reminder(
        &self,
        address: Address,
        sent_by: SentBy,
        due: i64,
        text: String,
    ) -> Result<Result<RequestId, Refusal>, StoreError> {
        let _guard = self
            .logical_deliveries
            .write_gate
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let request_id = match self.mint_request_id()? {
            Ok(request_id) => request_id,
            Err(refusal) => return Ok(Err(refusal)),
        };
        let commit = self
            .engine
            .begin_atomic_commit()
            .assert(
                self.logical_deliveries.request_ids,
                RequestIdBinding {
                    request_id: request_id.clone(),
                    logical_delivery_id: request_id.scheduled_marker(),
                    request_closure: RequestClosure::Open,
                },
            )
            .assert(
                self.scheduled_reminders,
                ScheduledReminder {
                    request_id: request_id.clone(),
                    address,
                    sent_by,
                    due: crate::generated::memory::Due(due),
                    text: crate::generated::memory::Text(text),
                },
            );
        self.engine.commit_atomic(commit)?;
        self.reminder_changes.announce();
        Ok(Ok(request_id))
    }

    fn scheduled_reminder(
        &self,
        request_id: &RequestId,
    ) -> Result<Option<ScheduledReminder>, StoreError> {
        Ok(self
            .engine
            .match_records(QueryPlan::key(
                self.scheduled_reminders,
                RecordKey::new(request_id.0.clone()),
            ))?
            .records()
            .first()
            .cloned())
    }

    fn due_scheduled_reminders(&self, now: i64) -> Result<Vec<ScheduledReminder>, StoreError> {
        Ok(self
            .engine
            .match_records(QueryPlan::all(self.scheduled_reminders))?
            .records()
            .iter()
            .filter(|reminder| reminder.due.0 <= now)
            .cloned()
            .collect())
    }

    fn next_scheduled_reminder_due(&self, now: i64) -> Result<Option<i64>, StoreError> {
        Ok(self
            .engine
            .match_records(QueryPlan::all(self.scheduled_reminders))?
            .records()
            .iter()
            .map(|reminder| reminder.due.0)
            .filter(|due| *due > now)
            .min())
    }

    fn retire_scheduled_reminder(
        &self,
        reminder: &ScheduledReminder,
        closed_by: signal_flow::LogicalDeliveryId,
    ) -> Result<(), StoreError> {
        let _guard = self
            .logical_deliveries
            .write_gate
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let commit = self.engine.begin_atomic_commit().retract(
            self.scheduled_reminders,
            RecordKey::new(reminder.request_id.0.clone()),
        );
        let commit = match self.request_binding(&reminder.request_id)? {
            Some(mut binding) => {
                binding.request_closure = RequestClosure::Answered(closed_by);
                commit.mutate(self.logical_deliveries.request_ids, binding)
            }
            None => commit,
        };
        self.engine.commit_atomic(commit)?;
        self.reminder_changes.announce();
        Ok(())
    }

    #[cfg(test)]
    fn rewind_scheduled_reminder(
        &self,
        request_id: &RequestId,
        due: i64,
    ) -> Result<(), StoreError> {
        let Some(mut reminder) = self.scheduled_reminder(request_id)? else {
            return Err(StoreError::StateInvariant);
        };
        reminder.due = crate::generated::memory::Due(due);
        self.engine.commit_atomic(
            self.engine
                .begin_atomic_commit()
                .mutate(self.scheduled_reminders, reminder),
        )?;
        Ok(())
    }
}
