//! Scheduling decisions for handover warning reminders.
//!
//! This module has no privileged delivery path. Callers hand its returned
//! message to ordinary logical admission, exactly as any other Notice.

use crate::store::handover_reminders::{HandoverReminder, HandoverReminderRules};

/// One due reminder is admitted through the ordinary delivery machinery.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HandoverReminderNotice {
    pub address: signal_flow::Address,
    pub text: String,
    pub generation: i64,
}

pub trait DecidesHandoverReminder {
    /// Returns the notice only at or after its due instant. The caller must
    /// durably advance the row after successful ordinary admission.
    fn notice_if_due(&self, now: i64) -> Option<HandoverReminderNotice>;
    /// Anchors every repeat to the first warning moment, not to delivery time.
    fn advanced_due(&self) -> Option<i64>;
    fn advanced(&self) -> Option<HandoverReminder>;
}
impl DecidesHandoverReminder for HandoverReminder {
    fn notice_if_due(&self, now: i64) -> Option<HandoverReminderNotice> {
        (now >= self.next_due).then(|| HandoverReminderNotice {
            address: self.address.clone(),
            text: self.reminder_text.clone(),
            generation: self.generation,
        })
    }
    fn advanced_due(&self) -> Option<i64> { self.next_due().ok() }
    fn advanced(&self) -> Option<HandoverReminder> {
        let mut next = self.clone();
        next.sent = next.sent.checked_add(1)?;
        next.next_due = next.next_due().ok()?;
        Some(next)
    }
}
