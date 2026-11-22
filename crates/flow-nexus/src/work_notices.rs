//! A committed outcome makes one durable, non-waking admission at each
//! selected address. Selection is already fixed in Memory.

use crate::RunningNexus;
use crate::generated::overriding::HeedsOverrides as _;
use crate::store::logical_delivery::{LogicalDeliveryTransition, RecordsLogicalDeliveries};
use crate::store::work_notices::KeepsWorkNotices as _;
use crate::waking::SleepsAndWakesSeats as _;
use flow_ethos::{Delivery, Message, Request, SentBy};
use signal_flow::PrivateSendResponse;

pub trait DeliversWorkNotices {
    fn deliver_work_notices(&self);
}

impl DeliversWorkNotices for RunningNexus {
    fn deliver_work_notices(&self) {
        let addresses = {
            let _gate = self.dispatch_gate.lock().unwrap_or_else(|e| e.into_inner());
            if self.held() {
                return;
            }
            let Ok(outbox) = self.store.pending_work_notices() else {
                return;
            };
            let mut addresses = std::collections::HashSet::new();
            for pending in outbox {
                let Ok(Some(notice)) = self.store.work_notification(&pending) else {
                    continue;
                };
                let address = pending.address.clone();
                if matches!(
                    self.store.accept_logical_delivery_for_with_transition(
                        &address,
                        SentBy::Flow,
                        Delivery::Queue,
                        Message::Soft(Request::WorkNotification(notice)),
                        None,
                        LogicalDeliveryTransition {
                            purpose: crate::heartbeat::DeliveryPurpose::Ordinary,
                            context_warning: None,
                            handover_reminder: None,
                            deliver_call: None,
                            upgrade_notice: None,
                            work_notice: Some(pending),
                        },
                    ),
                    Ok(PrivateSendResponse::SendAccepted(accepted))
                        if accepted.delivery_state == signal_flow::DeliveryState::Queued
                ) {
                    addresses.insert(address);
                }
            }
            addresses
        };
        for address in addresses {
            self.drain_awake(&address);
        }
    }
}
