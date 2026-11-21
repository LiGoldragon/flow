//! Reminders Flow keeps in its Memory, so a refresh of the reminded
//! metaflow or a restart of the Nexus keeps them.
//!
//! A Task is an order with its minutes, kept under the id Flow answered its
//! send with. When its minutes lapse with no Done naming it, its sender is
//! reminded, `TASK_REMINDERS` reminders in all, each a
//! `Reminder.{ RequestId String }` naming the task by its id with one short
//! line. A Done naming the task closes it.

use super::AnnouncesLaunchChanges;
use super::{FlowStore, StoreError};
pub(crate) use crate::generated::memory::{Due, Reminder, Sent};
use sema_engine::{
    Assertion, EngineRecord, FamilyName, KeyedMutation, MemoryDefinition, MemoryVersion, QueryPlan,
    RecordKey, Retraction, SchemaHash, TableDescriptor, TableName,
};
use signal_flow::Address;

pub(super) const REMINDER_TABLE_NAME: TableName = TableName::new("flow_nexus_reminders");
pub(super) const REMINDER_FAMILY: &str = "flow-nexus-reminder";
pub(super) const REMINDER_SCHEMA: &str = "flow-nexus-reminder-v3";

/// How many reminders an unanswered task gets: one, at its deadline, as
/// vision:flow-nexus:0 says the sender is reminded when the minutes pass.
pub const TASK_REMINDERS: i64 = 1;

/// The Minutes rule a Task meets at admission, and a default Minutes at
/// its Configure: above zero, and the deadline of its last reminder,
/// computed from `now` with checked arithmetic, inside the clock. No
/// policy maximum exists.
pub trait RulesMinutes {
    /// The interval between reminders in seconds, or why the Minutes is
    /// refused.
    fn ruled_interval(&self, now: i64) -> Result<i64, signal_flow::MinutesRefusal>;
}

impl RulesMinutes for i64 {
    fn ruled_interval(&self, now: i64) -> Result<i64, signal_flow::MinutesRefusal> {
        use signal_flow::MinutesRefusal;
        if *self == 0 {
            return Err(MinutesRefusal::Zero);
        }
        if *self < 0 {
            return Err(MinutesRefusal::Negative);
        }
        let interval = self.checked_mul(60).ok_or(MinutesRefusal::Unreachable)?;
        interval
            .checked_mul(TASK_REMINDERS)
            .and_then(|seconds| now.checked_add(seconds))
            .ok_or(MinutesRefusal::Unreachable)?;
        Ok(interval)
    }
}

impl EngineRecord for Reminder {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.logical_delivery_id.0.clone())
    }
}

impl MemoryDefinition for Reminder {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(3)
    }

    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label(REMINDER_SCHEMA)
    }
}

pub(super) trait DescribesReminderTable {
    fn descriptor() -> TableDescriptor<Reminder>;
}

impl DescribesReminderTable for Reminder {
    fn descriptor() -> TableDescriptor<Reminder> {
        TableDescriptor::new(
            REMINDER_TABLE_NAME,
            FamilyName::new(REMINDER_FAMILY),
            SchemaHash::for_label(REMINDER_SCHEMA),
        )
    }
}

/// One reminder due now: what to queue, and for whom.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DueReminder {
    /// The task's sender, as Flow recorded it when it admitted the task.
    pub address: Address,
    /// The task's own RequestId and one short line, never the task's text.
    pub reminder: flow_ethos::Reminder_Data,
}

/// The reminder due at `now`, and its next state. A reminder that has
/// already sent `TASK_REMINDERS`, as one held from a release that sent more
/// may have, is spent: it sends nothing and falls due no more.
pub trait AdvancesReminder {
    /// `task` is the RequestId the task was answered with.
    fn advanced(
        &self,
        task: &flow_ethos::RequestId,
        now: i64,
    ) -> (Option<DueReminder>, Option<Reminder>);
}

impl AdvancesReminder for Reminder {
    fn advanced(
        &self,
        task: &flow_ethos::RequestId,
        now: i64,
    ) -> (Option<DueReminder>, Option<Reminder>) {
        if self.sent.0 >= TASK_REMINDERS {
            let spent = Reminder {
                due_option: None,
                ..self.clone()
            };
            return (None, Some(spent));
        }
        let sent = self.sent.0 + 1;
        let last = sent >= TASK_REMINDERS;
        let line = format!(
            "The task to {} has no Done after {} minutes.",
            self.recipient.0.named(),
            self.minutes.0 * sent,
        );
        let next = Reminder {
            due_option: (!last).then(|| Due(now + self.minutes.0 * 60)),
            sent: Sent(sent),
            ..self.clone()
        };
        (
            Some(DueReminder {
                address: self.address.clone(),
                reminder: flow_ethos::Reminder_Data {
                    request_id: task.clone(),
                    string: line,
                },
            }),
            Some(next),
        )
    }
}

/// An Address as a reader writes it: `Field core Sol`.
pub trait NamesAddress {
    fn named(&self) -> String;
}

impl NamesAddress for Address {
    fn named(&self) -> String {
        format!("{:?} {} {:?}", self.aspect, self.topic.0, self.model)
    }
}

pub trait RecordsReminders {
    /// Holds a reminder; one already held under its key is replaced.
    fn hold_reminder(&self, reminder: Reminder) -> Result<(), StoreError>;
    /// The reminder held under `key`.
    fn reminder(&self, key: &str) -> Result<Option<Reminder>, StoreError>;
    /// Closes the reminder held under `key`; false when none is held.
    fn close_reminder(&self, key: &str) -> Result<bool, StoreError>;
    /// Every reminder due at or before `now`.
    fn due_reminders(&self, now: i64) -> Result<Vec<Reminder>, StoreError>;
    /// The earliest time after `now` a held reminder falls due, in Unix
    /// seconds.
    fn next_reminder_due(&self, now: i64) -> Result<Option<i64>, StoreError>;
}

impl RecordsReminders for FlowStore {
    fn hold_reminder(&self, reminder: Reminder) -> Result<(), StoreError> {
        if self.reminder(&reminder.logical_delivery_id.0)?.is_some() {
            let key = RecordKey::new(reminder.logical_delivery_id.0.clone());
            self.engine
                .mutate_keyed(KeyedMutation::new(self.reminders, key, reminder))?;
        } else {
            self.engine
                .assert(Assertion::new(self.reminders, reminder))?;
        }
        self.reminder_changes.announce();
        Ok(())
    }

    fn reminder(&self, key: &str) -> Result<Option<Reminder>, StoreError> {
        Ok(self
            .engine
            .match_records(QueryPlan::key(
                self.reminders,
                RecordKey::new(key.to_owned()),
            ))?
            .records()
            .first()
            .cloned())
    }

    fn close_reminder(&self, key: &str) -> Result<bool, StoreError> {
        if self.reminder(key)?.is_none() {
            return Ok(false);
        }
        self.engine.retract(Retraction::new(
            self.reminders,
            RecordKey::new(key.to_owned()),
        ))?;
        self.reminder_changes.announce();
        Ok(true)
    }

    fn due_reminders(&self, now: i64) -> Result<Vec<Reminder>, StoreError> {
        Ok(self
            .engine
            .match_records(QueryPlan::all(self.reminders))?
            .records()
            .iter()
            .filter(|reminder| reminder.due_option.is_some_and(|due| due.0 <= now))
            .cloned()
            .collect())
    }

    fn next_reminder_due(&self, now: i64) -> Result<Option<i64>, StoreError> {
        Ok(self
            .engine
            .match_records(QueryPlan::all(self.reminders))?
            .records()
            .iter()
            .filter_map(|reminder| reminder.due_option.map(|due| due.0))
            .filter(|due| *due > now)
            .min())
    }
}

#[cfg(test)]
mod tests {
    use super::{AdvancesReminder, RecordsReminders, Reminder, TASK_REMINDERS};
    use crate::store::{FlowStore, OpensFlowStore};
    use signal_flow::{Address, Aspect, Model};

    fn address(model: Model) -> Address {
        Address {
            aspect: Aspect::Psyche,
            topic: flow_ethos::Topic("flow".into()),
            model,
        }
    }

    #[test]
    fn a_task_is_reminded_once_and_told_no_failure() {
        let reminder = Reminder {
            logical_delivery_id: flow_ethos::LogicalDeliveryId("logical-delivery-1".into()),
            address: address(Model::Sonnet),
            recipient: crate::generated::memory::Recipient(Address {
                aspect: Aspect::Field,
                topic: flow_ethos::Topic("core".into()),
                model: Model::Sol,
            }),
            minutes: flow_ethos::Minutes(10),
            due_option: Some(crate::generated::memory::Due(600)),
            sent: crate::generated::memory::Sent(0),
        };
        let task = flow_ethos::RequestId("k7ph".into());
        let (due, next) = reminder.advanced(&task, 600);
        let due = due.expect("the first reminder is sent");
        assert_eq!(due.address, address(Model::Sonnet));
        let spent = next.expect("a task stays open for its Done");
        assert_eq!(spent.sent, crate::generated::memory::Sent(TASK_REMINDERS));
        assert_eq!(spent.due_option, None, "no second reminder falls due");
        assert_eq!(
            due.reminder,
            flow_ethos::Reminder_Data {
                request_id: task,
                string: "The task to Field core Sol has no Done after 10 minutes.".into(),
            },
            "the reminder names the task by its id, not by its text"
        );
        assert!(!due.reminder.string.contains("logical-delivery-1"));
    }

    /// A reminder held from a release that sent more, already past its
    /// count and still due, sends nothing and falls due no more.
    #[test]
    fn a_reminder_past_its_count_is_spent_without_a_notice() {
        let reminder = Reminder {
            logical_delivery_id: flow_ethos::LogicalDeliveryId("logical-delivery-1".into()),
            address: address(Model::Sonnet),
            recipient: crate::generated::memory::Recipient(address(Model::Sol)),
            minutes: flow_ethos::Minutes(10),
            due_option: Some(crate::generated::memory::Due(1200)),
            sent: crate::generated::memory::Sent(2),
        };
        let (due, next) = reminder.advanced(&flow_ethos::RequestId("k7ph".into()), 1200);
        assert_eq!(due, None);
        assert_eq!(
            next,
            Some(Reminder {
                due_option: None,
                ..reminder
            })
        );
    }

    #[test]
    fn a_reminder_is_kept_across_a_reopen_until_closed() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("flow.sema");
        let reminder = Reminder {
            logical_delivery_id: flow_ethos::LogicalDeliveryId("logical-delivery-1".into()),
            address: address(Model::Opus),
            recipient: crate::generated::memory::Recipient(address(Model::Sonnet)),
            minutes: flow_ethos::Minutes(10),
            due_option: Some(crate::generated::memory::Due(5)),
            sent: crate::generated::memory::Sent(0),
        };
        {
            let store = FlowStore::open(&path).unwrap();
            store.hold_reminder(reminder.clone()).unwrap();
            assert!(store.due_reminders(4).unwrap().is_empty());
        }
        let store = FlowStore::open(&path).unwrap();
        assert_eq!(store.due_reminders(5).unwrap(), vec![reminder]);
        assert!(store.close_reminder("logical-delivery-1").unwrap());
        assert!(!store.close_reminder("logical-delivery-1").unwrap());
        assert!(store.due_reminders(9).unwrap().is_empty());
    }
}
