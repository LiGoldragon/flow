//! Scheduling decisions for handover warning reminders.
//!
//! This module has no privileged delivery path. Callers hand its returned
//! message to ordinary logical admission, exactly as any other Notice.

use crate::store::handover_reminders::{HandoverReminder, HandoverReminderRules};
use signal_flow::{Keep_Data, Query, Section};

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
/// Typed command rendering: the reminder contains copyable Keep input rather
/// than a hand-written path or URI. The datom codec is the sole printer.
pub trait RendersHandoverCommands {
    fn keep_commands(&self, sections: &[Section]) -> Option<String>;
}
impl RendersHandoverCommands for HandoverReminderNotice {
    fn keep_commands(&self, sections: &[Section]) -> Option<String> {
        use datom_codec::{Datomizable, Composing};
        use protos::{Compactable, Protosizable};
        let section = sections.first()?.clone();
        let query = Query::Keep(Keep_Data { address: self.address.clone(), section });
        Some(query.datomize(vec![]).protosize().compact())
    }
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

/// Runtime capability used by the reminder tick. The caller supplies the
/// clock so retry/reopen tests can exercise exact boundaries without sleeping.
pub trait RemindsHandoverCallers {
    fn send_due_handover_reminders(&self, now: i64);
}

impl RemindsHandoverCallers for crate::RunningNexus {
    fn send_due_handover_reminders(&self, now: i64) {
        use crate::store::handover_reminders::{SchedulesHandoverReminders, HandoverReminderRules};
        use crate::store::logical_delivery::RecordsLogicalDeliveries;
        use flow_ethos::Delivery;
        use signal_flow::{Message, Request, SentBy};
        let Ok(due) = self.store.due_handover_reminders(now) else { return };
        for reminder in due {
            // Admission is deliberately ordinary. In particular this does
            // not use DeliveryPurpose::Heartbeat or any refresh hold bypass.
            let accepted = self.store.accept_logical_delivery(
                &reminder.address,
                SentBy::Flow,
                Delivery::Queue,
                Message::HardAbrupt(Request::Notice({
                    let base = HandoverReminderNotice { address: reminder.address.clone(), text: reminder.reminder_text.clone(), generation: reminder.generation };
                    base.keep_commands(&self.store.handover(&reminder.address).unwrap_or_default()).unwrap_or(reminder.reminder_text.clone())
                })),
                None,
            );
            if !matches!(accepted, Ok(signal_flow::PrivateSendResponse::SendAccepted(_))) { continue; }
            let Some(mut next) = reminder.advanced() else { continue };
            // A current green check in the tick owner closes this row before
            // calling this method. Once admitted, advance durably; retries
            // therefore retain the same due row until ordinary admission wins.
            if let Ok(next_due) = next.next_due() {
                next.next_due = next_due;
                let _ = self.store.hold_handover_reminder(next);
            }
        }
    }
}
