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
/// The warning a flow is sent: the configured words, then the path its
/// handover is written at, `flows/<its FlowId>/handover.md`, which its
/// successor's first prompt carries; nothing else.
pub trait RendersHandoverWarning {
    fn notice_text(&self, flow_id: &str) -> String;
}

impl RendersHandoverWarning for HandoverReminderNotice {
    fn notice_text(&self, flow_id: &str) -> String {
        format!("{}\nflows/{flow_id}/handover.md", self.text)
    }
}

impl DecidesHandoverReminder for HandoverReminder {
    fn notice_if_due(&self, now: i64) -> Option<HandoverReminderNotice> {
        (now >= self.next_due.0).then(|| HandoverReminderNotice {
            address: self.address.clone(),
            text: self.reminder_text.clone().0,
            generation: self.handover_generation.0,
        })
    }
    fn advanced_due(&self) -> Option<i64> {
        self.next_due().ok()
    }
    fn advanced(&self) -> Option<HandoverReminder> {
        let mut next = self.clone();
        next.sent = crate::generated::memory::Sent(next.sent.0.checked_add(1)?);
        next.next_due = crate::generated::memory::NextDue(next.next_due().ok()?);
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
        use crate::codex_turns::SettlesRestingCodexSeats;
        use crate::store::ManagesMetaflowState;
        use crate::store::ReadsFlowRows;
        use crate::store::context::JudgesThreshold;
        use crate::store::context::{NamesFlowModel, RecordsContextSize};
        use crate::store::context_warnings::ReadsContextWarningConfiguration;
        use crate::store::handover::HoldsHandovers;
        use crate::store::handover_configuration::ReadsHandoverConfiguration;
        use crate::store::handover_reminders::{
            HandoverReminderRules, OpensHandoverReminder, SchedulesHandoverReminders,
        };
        use crate::store::logical_delivery::RecordsLogicalDeliveries;
        use crate::waking::{CorrelatesNativeEvents, SleepsAndWakesSeats};
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
            let Ok(Some(model_name)) = self.store.flow_model(&flow_id.0) else {
                continue;
            };
            if !matches!(self.store.context_warning_policy(&model_name), Ok(None)) {
                continue;
            }
            let Some(policy) = policies.iter().find(|p| p.model_name.0 == model_name) else {
                continue;
            };
            let Ok(Some(refresh)) = self.store.configured_threshold(&model_name) else {
                continue;
            };
            let Ok(Some(tokens)) = self.store.context_size(&flow_id.0) else {
                continue;
            };
            let Ok(live) = self
                .store
                .ensure_live_handover(&metaflow.address, &flow_id.0)
            else {
                continue;
            };
            let generation = live.handover_generation;
            let Ok(green) = self.store.handover_green(
                &metaflow.address,
                &flow_id.0,
                refresh.refresh_threshold.0,
            ) else {
                continue;
            };
            let existing = self
                .store
                .handover_reminder(&metaflow.address, generation.0)
                .ok()
                .flatten();
            if green {
                if existing.is_some() {
                    let _ = self
                        .store
                        .close_handover_reminder(&metaflow.address, generation.0);
                }
                continue;
            }
            if !HandoverReminder::warning_open(
                refresh.refresh_threshold.0,
                policy.warning_share_left.0,
                tokens,
            ) {
                if existing.is_some() {
                    let _ = self
                        .store
                        .close_handover_reminder(&metaflow.address, generation.0);
                }
                continue;
            }
            if existing.is_none() {
                let Ok(reminder) = HandoverReminder::open_handover_reminder(
                    metaflow.address.clone(),
                    generation.0,
                    model_name,
                    now,
                    policy.every_minutes.0,
                    policy.reminder_text.0.clone(),
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
            if live.handover_generation != reminder.handover_generation
                || live.flow_id_option.is_none()
            {
                let _ = self
                    .store
                    .close_handover_reminder(&reminder.address, reminder.handover_generation.0);
                continue;
            }
            let Some(flow_id) = live.flow_id_option.as_ref().map(|value| value.0.as_str()) else {
                continue;
            };
            let Ok(Some(model_name)) = self.store.flow_model(flow_id) else {
                continue;
            };
            if !matches!(self.store.context_warning_policy(&model_name), Ok(None)) {
                continue;
            }
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
                .find(|policy| policy.model_name.0 == model_name)
            else {
                continue;
            };
            let Ok(Some(refresh)) = self.store.configured_threshold(&model_name) else {
                continue;
            };
            let Ok(Some(tokens)) = self.store.context_size(flow_id) else {
                continue;
            };
            if !HandoverReminder::warning_open(
                refresh.refresh_threshold.0,
                policy.warning_share_left.0,
                tokens,
            ) {
                continue;
            }
            // A flow past its threshold at its idle event is refreshed at
            // that event: a warning typed now would only start a turn it
            // never finishes.
            if matches!(
                self.store.threshold_standing(flow_id),
                Ok(crate::store::context::ThresholdStanding::Past { .. })
            ) && match self.store.flow_node(flow_id) {
                Ok(Some(node)) => match node.harness_kind {
                    signal_flow::HarnessKind::Codex => self.codex_resting(flow_id),
                    signal_flow::HarnessKind::Claude => self.turn_end_recorded(flow_id),
                },
                _ => false,
            } {
                continue;
            }
            // Admission is deliberately ordinary. In particular this does
            // not use DeliveryPurpose::Heartbeat or any refresh hold bypass.
            // MiddleAbrupt reaches the flow at its next tool boundary.
            let base = HandoverReminderNotice {
                address: reminder.address.clone(),
                text: reminder.reminder_text.clone().0,
                generation: reminder.handover_generation.0,
            };
            let mut admitted = reminder.clone();
            admitted.sent = match admitted.sent.0.checked_add(1) {
                Some(value) => crate::generated::memory::Sent(value),
                None => continue,
            };
            admitted.next_due = match admitted.next_due() {
                Ok(value) => crate::generated::memory::NextDue(value),
                Err(_) => continue,
            };
            let accepted = self.store.accept_handover_reminder(
                &reminder.address,
                Message::MiddleAbrupt(Request::Notice(base.notice_text(flow_id))),
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
