//! Durable state for reminders while a handover warning window is open.
//!
//! This family deliberately contains scheduling state only. Admission and
//! delivery remain the ordinary logical-delivery path; the Nexus decides when
//! to call `advance_handover_reminder` and what address/message to deliver.

use super::AnnouncesLaunchChanges;
use super::{FlowStore, ReadsFlowMemory, StoreError};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use sema_engine::{
    Assertion, EngineRecord, FamilyName, MemoryDefinition, MemoryVersion, RecordKey, SchemaHash,
    TableDescriptor, TableName,
};
use signal_flow::Address;

pub(super) const HANDOVER_REMINDER_TABLE_NAME: TableName =
    TableName::new("flow_nexus_handover_reminders");
pub(super) const HANDOVER_REMINDER_FAMILY: &str = "flow-nexus-handover-reminder";
pub(super) const HANDOVER_REMINDER_SCHEMA: &str = "flow-nexus-handover-reminder-v1";

/// The durable reminder clock for one handover generation.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub struct HandoverReminder {
    pub address: Address,
    /// The generation of the current handover. Successors use a new row.
    pub generation: i64,
    pub model_name: String,
    /// Unix seconds at which the warning window opened.
    pub opened_at: i64,
    /// The first reminder is due at `opened_at` (inclusive).
    pub next_due: i64,
    /// Number of successfully admitted reminders.
    pub sent: i64,
    pub every_minutes: i64,
    pub reminder_text: String,
}

impl EngineRecord for HandoverReminder {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(format!(
            "{}:{}",
            FlowStore::address_record_key(&self.address),
            self.generation
        ))
    }
}
impl MemoryDefinition for HandoverReminder {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(1)
    }
    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label(HANDOVER_REMINDER_SCHEMA)
    }
}

pub(super) trait DescribesHandoverReminderTable {
    fn descriptor() -> TableDescriptor<HandoverReminder>;
}
impl DescribesHandoverReminderTable for HandoverReminder {
    fn descriptor() -> TableDescriptor<HandoverReminder> {
        TableDescriptor::new(
            HANDOVER_REMINDER_TABLE_NAME,
            FamilyName::new(HANDOVER_REMINDER_FAMILY),
            Self::schema_hash(),
        )
    }
}

pub trait SchedulesHandoverReminders {
    fn handover_reminder(
        &self,
        address: &Address,
        generation: i64,
    ) -> Result<Option<HandoverReminder>, StoreError>;
    fn due_handover_reminders(&self, now: i64) -> Result<Vec<HandoverReminder>, StoreError>;
    /// The earliest time after `now` a handover reminder falls due, in
    /// Unix seconds.
    fn next_handover_reminder_due(&self, now: i64) -> Result<Option<i64>, StoreError>;
    fn hold_handover_reminder(&self, reminder: HandoverReminder) -> Result<(), StoreError>;
    fn close_handover_reminder(
        &self,
        address: &Address,
        generation: i64,
    ) -> Result<bool, StoreError>;
}

impl SchedulesHandoverReminders for FlowStore {
    fn handover_reminder(
        &self,
        address: &Address,
        generation: i64,
    ) -> Result<Option<HandoverReminder>, StoreError> {
        let key = RecordKey::new(format!(
            "{}:{}",
            Self::address_record_key(address),
            generation
        ));
        Ok(self
            .engine
            .match_records(sema_engine::QueryPlan::key(self.handover_reminders, key))?
            .records()
            .first()
            .cloned())
    }
    fn due_handover_reminders(&self, now: i64) -> Result<Vec<HandoverReminder>, StoreError> {
        Ok(self
            .engine
            .match_records(sema_engine::QueryPlan::all(self.handover_reminders))?
            .records()
            .iter()
            .filter(|row| row.next_due <= now)
            .cloned()
            .collect())
    }
    fn next_handover_reminder_due(&self, now: i64) -> Result<Option<i64>, StoreError> {
        Ok(self
            .engine
            .match_records(sema_engine::QueryPlan::all(self.handover_reminders))?
            .records()
            .iter()
            .map(|row| row.next_due)
            .filter(|due| *due > now)
            .min())
    }
    fn hold_handover_reminder(&self, reminder: HandoverReminder) -> Result<(), StoreError> {
        let key = reminder.record_key();
        if self
            .engine
            .match_records(sema_engine::QueryPlan::key(
                self.handover_reminders,
                key.clone(),
            ))?
            .records()
            .is_empty()
        {
            self.engine
                .assert(Assertion::new(self.handover_reminders, reminder))?;
        } else {
            self.engine.mutate_keyed(sema_engine::KeyedMutation::new(
                self.handover_reminders,
                key,
                reminder,
            ))?;
        }
        self.reminder_changes.announce();
        Ok(())
    }
    fn close_handover_reminder(
        &self,
        address: &Address,
        generation: i64,
    ) -> Result<bool, StoreError> {
        let Some(row) = self.handover_reminder(address, generation)? else {
            return Ok(false);
        };
        self.engine.retract(sema_engine::Retraction::new(
            self.handover_reminders,
            row.record_key(),
        ))?;
        self.reminder_changes.announce();
        Ok(true)
    }
}

/// Pure scheduling rules, kept on the data-bearing record so policy/window
/// arithmetic is shared by store and tests without a competing free function.
pub trait HandoverReminderRules {
    /// Warning eligibility is inclusive at the boundary. `left` is a share of
    /// the refresh threshold, and all arithmetic is widened before multiplying.
    fn warning_open(refresh: i64, warning_share_left: i64, context_tokens: i64) -> bool;
    fn next_due(&self) -> Result<i64, StoreError>;
}
impl HandoverReminderRules for HandoverReminder {
    fn warning_open(refresh: i64, warning_share_left: i64, context_tokens: i64) -> bool {
        if refresh < 0 || !(0..=100).contains(&warning_share_left) || context_tokens < 0 {
            return false;
        }
        let used = i128::from(100 - warning_share_left);
        i128::from(context_tokens) * 100 >= i128::from(refresh) * used
    }
    fn next_due(&self) -> Result<i64, StoreError> {
        let seconds = self
            .every_minutes
            .checked_mul(60)
            .ok_or(StoreError::StateInvariant)?;
        self.next_due
            .checked_add(seconds)
            .ok_or(StoreError::StateInvariant)
    }
}

/// Constructs the first due reminder at the exact warning crossing. The
/// initial emission is therefore immediate, while repeats are anchored to it.
pub trait OpensHandoverReminder {
    fn open_handover_reminder(
        address: Address,
        generation: i64,
        model_name: String,
        opened_at: i64,
        every_minutes: i64,
        reminder_text: String,
    ) -> Result<HandoverReminder, StoreError>;
}
impl OpensHandoverReminder for HandoverReminder {
    fn open_handover_reminder(
        address: Address,
        generation: i64,
        model_name: String,
        opened_at: i64,
        every_minutes: i64,
        reminder_text: String,
    ) -> Result<HandoverReminder, StoreError> {
        if every_minutes <= 0 {
            return Err(StoreError::StateInvariant);
        }
        Ok(Self {
            address,
            generation,
            model_name,
            opened_at,
            next_due: opened_at,
            sent: 0,
            every_minutes,
            reminder_text,
        })
    }
}
