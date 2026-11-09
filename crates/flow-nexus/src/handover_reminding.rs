//! Scheduling decisions for handover warning reminders.
//!
//! This module has no privileged delivery path. Callers hand its returned
//! message to ordinary logical admission, exactly as any other Notice.

use crate::store::handover_reminders::{HandoverReminder, HandoverReminderRules};
use signal_flow::{Drop_Data, Keep_Data, Query, Section};

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
    fn keep_commands(&self, sections: &[Section]) -> String;
    fn drop_command(&self, title: signal_flow::Title) -> String;
    fn notice_text(&self, sections: &[Section]) -> String;
}
impl RendersHandoverCommands for HandoverReminderNotice {
    fn keep_commands(&self, sections: &[Section]) -> String {
        use datom_codec::Datomizable;
        use protos::{Compactable, Protosizable};
        let section = sections.first().cloned().unwrap_or(Section {
            title: String::new(),
            description: String::new(),
            body: String::new(),
        });
        let query = Query::Keep(Keep_Data {
            address: self.address.clone(),
            section,
        });
        format!(
            "flow-nexus '{}'",
            query
                .datomize(vec![])
                .protosize()
                .compact()
                .replace('\'', "'\"'\"'")
        )
    }
    fn notice_text(&self, sections: &[Section]) -> String {
        let mut commands = vec![self.keep_commands(sections)];
        if sections.is_empty() {
            commands.push(self.drop_command(signal_flow::Title::from("")));
        } else {
            for section in sections {
                commands.push(self.drop_command(section.title.clone()));
            }
        }
        commands.push(self.text.clone());
        commands.join("\n")
    }
    fn drop_command(&self, title: signal_flow::Title) -> String {
        use datom_codec::Datomizable;
        use protos::{Compactable, Protosizable};
        let query = Query::Drop(Drop_Data {
            address: self.address.clone(),
            title,
        });
        format!(
            "flow-nexus '{}'",
            query
                .datomize(vec![])
                .protosize()
                .compact()
                .replace('\'', "'\"'\"'")
        )
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
    fn advanced_due(&self) -> Option<i64> {
        self.next_due().ok()
    }
    fn advanced(&self) -> Option<HandoverReminder> {
        let mut next = self.clone();
        next.sent = next.sent.checked_add(1)?;
        next.next_due = next.next_due().ok()?;
        Some(next)
    }
}

/// The complete observation needed to open or close one warning window.
/// The live flow supplies this from its latest Stop and current handover row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HandoverWindowObservation {
    pub address: signal_flow::Address,
    pub generation: i64,
    pub model_name: String,
    pub refresh_threshold: i64,
    pub warning_share_left: i64,
    pub context_tokens: i64,
    pub current_flow_id: String,
    pub touched_flow_id: Option<String>,
    /// Result of the authoritative `HoldsHandovers::handover_green` check.
    pub green: bool,
    pub reminder_text: String,
}

pub trait DiscoversHandoverWindow {
    fn warning_window_opens(&self) -> bool;
    fn warning_window_is_current(&self) -> bool;
}
impl DiscoversHandoverWindow for HandoverWindowObservation {
    fn warning_window_opens(&self) -> bool {
        HandoverReminder::warning_open(
            self.refresh_threshold,
            self.warning_share_left,
            self.context_tokens,
        ) && self.context_tokens >= 0
    }
    fn warning_window_is_current(&self) -> bool {
        self.warning_window_opens() && !self.green
    }
}

/// Runtime capability used by the reminder tick. The caller supplies the
/// clock so retry/reopen tests can exercise exact boundaries without sleeping.
pub trait RemindsHandoverCallers {
    fn send_due_handover_reminders(&self, now: i64);
}

impl RemindsHandoverCallers for crate::RunningNexus {
    fn send_due_handover_reminders(&self, now: i64) {
        use crate::store::ManagesMetaflowState;
        use crate::store::context::{NamesFlowModel, RecordsContextSize};
        use crate::store::handover::HoldsHandovers;
        use crate::store::handover_configuration::ReadsHandoverConfiguration;
        use crate::store::handover_reminders::{
            HandoverReminderRules, OpensHandoverReminder, SchedulesHandoverReminders,
        };
        use crate::store::logical_delivery::RecordsLogicalDeliveries;
        use crate::waking::SleepsAndWakesSeats;
        use meta_signal_flow::HandoverPoliciesConfiguration;
        use signal_flow::{Message, Request};
        // Discover warning windows from every awake metaflow before reading
        // due rows. All inputs are durable Memory observations.
        let Ok(metaflows) = self.store.list_metaflows() else {
            return;
        };
        let Ok(configuration) = self.store.handover_policies() else {
            return;
        };
        let HandoverPoliciesConfiguration::Configured(policies) = configuration else {
            return;
        };
        for metaflow in metaflows {
            let flow_ethos::State::Awake(flow_id) = metaflow.state else {
                continue;
            };
            let Ok(Some(model_name)) = self.store.flow_model(&flow_id) else {
                continue;
            };
            let Some(policy) = policies.iter().find(|p| p.model_name == model_name) else {
                continue;
            };
            let Ok(Some(refresh)) = self.store.configured_threshold(&model_name) else {
                continue;
            };
            let Ok(Some(tokens)) = self.store.context_size(&flow_id) else {
                continue;
            };
            let Ok(live) = self.store.ensure_live_handover(&metaflow.address, &flow_id) else {
                continue;
            };
            let generation = live.generation;
            let Ok(green) = self
                .store
                .handover_green(&metaflow.address, &flow_id, refresh.refresh)
            else {
                continue;
            };
            let existing = self
                .store
                .handover_reminder(&metaflow.address, generation)
                .ok()
                .flatten();
            if green {
                if existing.is_some() {
                    let _ = self
                        .store
                        .close_handover_reminder(&metaflow.address, generation);
                }
                continue;
            }
            if !HandoverReminder::warning_open(refresh.refresh, policy.warning_share_left, tokens) {
                if existing.is_some() {
                    let _ = self
                        .store
                        .close_handover_reminder(&metaflow.address, generation);
                }
                continue;
            }
            if existing.is_none() {
                let Ok(reminder) = HandoverReminder::open_handover_reminder(
                    metaflow.address.clone(),
                    generation,
                    model_name,
                    now,
                    policy.every_minutes,
                    policy.reminder_text.clone(),
                ) else {
                    continue;
                };
                let _ = self.store.hold_handover_reminder(reminder);
            }
        }
        let Ok(due) = self.store.due_handover_reminders(now) else {
            return;
        };
        for reminder in due {
            let Ok(live) = self.store.handover_memory(&reminder.address) else {
                continue;
            };
            let Some(live) = live else { continue };
            if live.generation != reminder.generation || live.current_flow_id.is_none() {
                let _ = self
                    .store
                    .close_handover_reminder(&reminder.address, reminder.generation);
                continue;
            }
            let Some(flow_id) = live.current_flow_id.as_deref() else {
                continue;
            };
            let Ok(Some(model_name)) = self.store.flow_model(flow_id) else {
                continue;
            };
            let Ok(configuration) = self.store.handover_policies() else {
                continue;
            };
            let meta_signal_flow::HandoverPoliciesConfiguration::Configured(policies) =
                configuration
            else {
                continue;
            };
            let Some(policy) = policies
                .iter()
                .find(|policy| policy.model_name == model_name)
            else {
                continue;
            };
            let Ok(Some(refresh)) = self.store.configured_threshold(&model_name) else {
                continue;
            };
            let Ok(Some(tokens)) = self.store.context_size(flow_id) else {
                continue;
            };
            if !HandoverReminder::warning_open(refresh.refresh, policy.warning_share_left, tokens) {
                continue;
            }
            // Admission is deliberately ordinary. In particular this does
            // not use DeliveryPurpose::Heartbeat or any refresh hold bypass.
            let sections = match self.store.handover(&reminder.address) {
                Ok(sections) => sections,
                Err(_) => continue,
            };
            let base = HandoverReminderNotice {
                address: reminder.address.clone(),
                text: reminder.reminder_text.clone(),
                generation: reminder.generation,
            };
            let mut admitted = reminder.clone();
            admitted.sent = match admitted.sent.checked_add(1) {
                Some(value) => value,
                None => continue,
            };
            admitted.next_due = match admitted.next_due() {
                Ok(value) => value,
                Err(_) => continue,
            };
            let accepted = self.store.accept_handover_reminder(
                &reminder.address,
                Message::HardAbrupt(Request::Notice(base.notice_text(&sections))),
                admitted,
            );
            if !matches!(
                accepted,
                Ok(signal_flow::PrivateSendResponse::SendAccepted(_))
            ) {
                continue;
            }
            self.wake_due(reminder.address.clone());
        }
    }
}
