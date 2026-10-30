//! Reminders Flow keeps in its Memory, so a refresh of the reminded
//! metaflow or a restart of the Nexus keeps them.
//!
//! A Task is an order with its minutes, kept under the id Flow answered its
//! send with. When its minutes lapse with no Done naming it, its sender is
//! reminded, then again every interval, `TASK_REMINDERS` reminders in all;
//! the last is copied to the sender's Secondary as a failure. A Done naming
//! the task closes it. A Remind is one reminder at its instant.
//!
//! The rows live in their own table, so a store written before reminders
//! opens with the table added and holding none.

use super::{FlowStore, StoreError};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use sema_engine::{
    Assertion, EngineRecord, FamilyName, KeyedMutation, QueryPlan, RecordKey, Retraction,
    SchemaHash, TableDescriptor, TableName,
};
use signal_flow::Address;

pub(super) const REMINDER_TABLE_NAME: TableName = TableName::new("flow_nexus_reminders");
pub(super) const REMINDER_FAMILY: &str = "flow-nexus-reminder";
pub(super) const REMINDER_SCHEMA: &str = "flow-nexus-reminder-v1";

/// How many reminders an unanswered task gets: the first at its deadline,
/// then one every interval. Ruled 2026-10-29: the first firing and three
/// repeats.
pub const TASK_REMINDERS: i64 = 4;

/// What a reminder is about.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub enum Reminding {
    /// A task: the order its sender sent `recipient`, `minutes` long.
    Task { recipient: Address, minutes: i64 },
    /// A Remind: the caller's own words.
    Remind(String),
}

/// One reminder Flow holds.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub struct Reminder {
    /// The task's LogicalDeliveryId, or the Remind's own id.
    pub key: String,
    /// The metaflow reminded: the task's sender, the Remind's caller.
    pub address: Address,
    pub reminding: Reminding,
    /// When the next reminder is due, in Unix seconds; None once a task has
    /// had all its reminders, or set none (its sender unknown), and stays
    /// open for its Done.
    pub due: Option<i64>,
    /// The reminders sent so far.
    pub sent: i64,
}

impl EngineRecord for Reminder {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.key.clone())
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

/// One reminder due now: what to queue, for whom, and the failure copy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DueReminder {
    pub address: Address,
    pub notice: String,
    /// The sender's Secondary and the failure it is told, on a task's last
    /// reminder, when the sender is not that Secondary itself.
    pub failure: Option<(Address, String)>,
}

/// The reminder's next state once one is sent at `now`.
pub trait AdvancesReminder {
    fn advanced(&self, now: i64) -> (DueReminder, Option<Reminder>);
}

impl AdvancesReminder for Reminder {
    fn advanced(&self, now: i64) -> (DueReminder, Option<Reminder>) {
        let sent = self.sent + 1;
        match &self.reminding {
            Reminding::Remind(body) => (
                DueReminder {
                    address: self.address.clone(),
                    notice: body.clone(),
                    failure: None,
                },
                None,
            ),
            Reminding::Task { recipient, minutes } => {
                let last = sent >= TASK_REMINDERS;
                let notice = format!(
                    "Reminder {sent} of {TASK_REMINDERS}: task {} to {} has no Done after {} minutes.",
                    self.key,
                    recipient.named(),
                    minutes * sent,
                );
                let secondary = Address {
                    aspect: self.address.aspect.clone(),
                    topic: self.address.topic.clone(),
                    layer: signal_flow::Layer::Secondary,
                };
                let failure = (last && secondary != self.address).then(|| {
                    (
                        secondary,
                        format!(
                            "Failure: task {} from {} to {} has no Done after {TASK_REMINDERS} reminders.",
                            self.key,
                            self.address.named(),
                            recipient.named(),
                        ),
                    )
                });
                let next = Reminder {
                    due: (!last).then(|| now + minutes * 60),
                    sent,
                    ..self.clone()
                };
                (
                    DueReminder {
                        address: self.address.clone(),
                        notice,
                        failure,
                    },
                    Some(next),
                )
            }
        }
    }
}

/// An Address as a reader writes it: `Field core Secondary`.
pub trait NamesAddress {
    fn named(&self) -> String;
}

impl NamesAddress for Address {
    fn named(&self) -> String {
        format!("{:?} {} {:?}", self.aspect, self.topic, self.layer)
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
}

impl RecordsReminders for FlowStore {
    fn hold_reminder(&self, reminder: Reminder) -> Result<(), StoreError> {
        if self.reminder(&reminder.key)?.is_some() {
            let key = RecordKey::new(reminder.key.clone());
            self.engine
                .mutate_keyed(KeyedMutation::new(self.reminders, key, reminder))?;
        } else {
            self.engine
                .assert(Assertion::new(self.reminders, reminder))?;
        }
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
        Ok(true)
    }

    fn due_reminders(&self, now: i64) -> Result<Vec<Reminder>, StoreError> {
        Ok(self
            .engine
            .match_records(QueryPlan::all(self.reminders))?
            .records()
            .iter()
            .filter(|reminder| reminder.due.is_some_and(|due| due <= now))
            .cloned()
            .collect())
    }
}

/// Reads an RFC 3339 date-time, `2026-10-29T23:30:00-06:00` or with `Z`,
/// fractional seconds allowed, as Unix seconds.
pub trait ReadsInstant {
    fn unix_seconds(&self) -> Option<i64>;
}

impl ReadsInstant for str {
    fn unix_seconds(&self) -> Option<i64> {
        let bytes = self.as_bytes();
        let number = |range: std::ops::Range<usize>| -> Option<i64> {
            let text = self.get(range)?;
            text.bytes()
                .all(|byte| byte.is_ascii_digit())
                .then(|| text.parse().ok())?
        };
        if bytes.len() < 20
            || bytes[4] != b'-'
            || bytes[7] != b'-'
            || !matches!(bytes[10], b'T' | b't' | b' ')
            || bytes[13] != b':'
            || bytes[16] != b':'
        {
            return None;
        }
        let (year, month, day) = (number(0..4)?, number(5..7)?, number(8..10)?);
        let (hour, minute, second) = (number(11..13)?, number(14..16)?, number(17..19)?);
        let mut rest = &self[19..];
        if let Some(fraction) = rest.strip_prefix('.') {
            let digits = fraction.bytes().take_while(u8::is_ascii_digit).count();
            if digits == 0 {
                return None;
            }
            rest = &fraction[digits..];
        }
        let offset = match rest {
            "Z" | "z" => 0,
            _ => {
                let sign = match rest.as_bytes().first()? {
                    b'+' => 1,
                    b'-' => -1,
                    _ => return None,
                };
                if rest.len() != 6 || rest.as_bytes()[3] != b':' {
                    return None;
                }
                let hours: i64 = rest[1..3].parse().ok()?;
                let minutes: i64 = rest[4..6].parse().ok()?;
                if hours > 23 || minutes > 59 {
                    return None;
                }
                sign * (hours * 3600 + minutes * 60)
            }
        };
        let leap = (year % 4 == 0 && year % 100 != 0) || year % 400 == 0;
        let days_in_month = [
            31,
            if leap { 29 } else { 28 },
            31,
            30,
            31,
            30,
            31,
            31,
            30,
            31,
            30,
            31,
        ];
        if !(1..=12).contains(&month)
            || day < 1
            || day > days_in_month[(month - 1) as usize]
            || hour > 23
            || minute > 59
            || second > 60
        {
            return None;
        }
        // Days from the civil date, Howard Hinnant's algorithm.
        let (y, m) = if month <= 2 {
            (year - 1, month + 9)
        } else {
            (year, month - 3)
        };
        let era = y.div_euclid(400);
        let year_of_era = y - era * 400;
        let day_of_year = (153 * m + 2) / 5 + day - 1;
        let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
        let days = era * 146_097 + day_of_era - 719_468;
        Some(days * 86_400 + hour * 3600 + minute * 60 + second - offset)
    }
}

#[cfg(test)]
mod tests {
    use super::{
        AdvancesReminder, ReadsInstant, RecordsReminders, Reminder, Reminding, TASK_REMINDERS,
    };
    use crate::store::{FlowStore, OpensFlowStore};
    use signal_flow::{Address, Aspect, Layer};

    fn address(layer: Layer) -> Address {
        Address {
            aspect: Aspect::Psyche,
            topic: "flow".into(),
            layer,
        }
    }

    /// Request as Flow 0.40.4 archived it, before Task and Done.
    #[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
    enum RequestBeforeTasks {
        Order(String),
        Question(String),
        Psyche(signal_flow::Said),
        Psyches(Vec<signal_flow::Said>),
        Result(String),
        Notice(String),
    }

    #[test]
    fn a_request_archived_before_tasks_reads_unchanged() {
        for (old, new) in [
            (
                RequestBeforeTasks::Order("deploy".into()),
                signal_flow::Request::Order("deploy".into()),
            ),
            (
                RequestBeforeTasks::Psyche(signal_flow::Said {
                    context: "c".into(),
                    verbatim: "v".into(),
                }),
                signal_flow::Request::Psyche(signal_flow::Said {
                    context: "c".into(),
                    verbatim: "v".into(),
                }),
            ),
            (
                RequestBeforeTasks::Notice("n".into()),
                signal_flow::Request::Notice("n".into()),
            ),
        ] {
            let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&old).unwrap();
            assert_eq!(
                rkyv::from_bytes::<signal_flow::Request, rkyv::rancor::Error>(&bytes).unwrap(),
                new
            );
        }
        assert_eq!(
            std::mem::size_of::<rkyv::Archived<RequestBeforeTasks>>(),
            std::mem::size_of::<rkyv::Archived<signal_flow::Request>>()
        );
    }

    #[test]
    fn an_rfc_3339_instant_reads_as_unix_seconds() {
        // 1_000_000_000 is 2001-09-09T01:46:40Z, a value from outside the parser.
        assert_eq!("2001-09-09T01:46:40Z".unix_seconds(), Some(1_000_000_000));
        assert_eq!(
            "2001-09-08T19:46:40-06:00".unix_seconds(),
            Some(1_000_000_000)
        );
        assert_eq!(
            "2001-09-09T01:46:40.250+00:00".unix_seconds(),
            Some(1_000_000_000)
        );
        assert_eq!("1970-01-01T00:00:00Z".unix_seconds(), Some(0));
        assert_eq!("tomorrow".unix_seconds(), None);
        assert_eq!("2026-02-30T00:00:00Z".unix_seconds(), None);
        assert_eq!("2026-10-29T23:30:00".unix_seconds(), None);
    }

    #[test]
    fn a_task_is_reminded_four_times_the_last_copied_to_the_secondary() {
        let mut reminder = Reminder {
            key: "logical-delivery-1".into(),
            address: address(Layer::Tertiary),
            reminding: Reminding::Task {
                recipient: Address {
                    aspect: Aspect::Field,
                    topic: "core".into(),
                    layer: Layer::Secondary,
                },
                minutes: 10,
            },
            due: Some(600),
            sent: 0,
        };
        let mut failures = Vec::new();
        let mut now = 600;
        for _ in 0..TASK_REMINDERS {
            let (due, next) = reminder.advanced(now);
            assert_eq!(due.address, address(Layer::Tertiary));
            failures.push(due.failure.clone());
            reminder = next.expect("a task stays open for its Done");
            now += 600;
        }
        assert_eq!(reminder.sent, 4);
        assert_eq!(reminder.due, None);
        assert!(failures[..3].iter().all(Option::is_none));
        assert_eq!(
            failures[3].as_ref().map(|(to, _)| to.clone()),
            Some(address(Layer::Secondary))
        );
    }

    #[test]
    fn a_reminder_is_kept_across_a_reopen_until_closed() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("flow.sema");
        let reminder = Reminder {
            key: "remind-1".into(),
            address: address(Layer::Secondary),
            reminding: Reminding::Remind("look".into()),
            due: Some(5),
            sent: 0,
        };
        {
            let store = FlowStore::open(&path).unwrap();
            store.hold_reminder(reminder.clone()).unwrap();
            assert!(store.due_reminders(4).unwrap().is_empty());
        }
        let store = FlowStore::open(&path).unwrap();
        assert_eq!(store.due_reminders(5).unwrap(), vec![reminder]);
        assert!(store.close_reminder("remind-1").unwrap());
        assert!(!store.close_reminder("remind-1").unwrap());
        assert!(store.due_reminders(9).unwrap().is_empty());
    }
}
