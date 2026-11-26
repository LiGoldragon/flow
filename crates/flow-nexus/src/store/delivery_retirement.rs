//! Settle the native deliveries that belong to a seat leaving its metaflow.

use crate::reminding::CountsUnixSeconds;
use crate::store::attempt_membership::{KeepsAttemptMemberships, MembershipOwner};
use crate::store::metaflow::SleepsMetaflows;
use crate::store::queue_delivery::{
    AccountsQueueDelivery, EndsQueueDeliveryPhase, QueueDeliveryAttempt, QueueDeliveryPhase,
};
use crate::store::{AnnouncesLaunchChanges, FlowStore, LaunchChanges, StoreError, StoredMetaflow};
use crate::{HoldsStopRequest, RunningNexus};
use flow_ethos::{Address, FlowId, State};
use meta_signal_flow::{AbandonmentCause, AbandonmentRecord, QueueResolutionReason, UnixSeconds};
use sema_engine::{
    DeltaKind, QueryPlan, SinkError, SubscriptionDeliveryMode, SubscriptionEvent, SubscriptionSink,
};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

struct Retirement {
    address: Address,
    flow_id: FlowId,
}

#[derive(Default)]
struct ReceivingSeats {
    awake: HashMap<Address, FlowId>,
    retirements: Vec<Retirement>,
}

#[derive(Default)]
pub(crate) struct DeliveryRetirements {
    pub(crate) changes: LaunchChanges,
    seats: Mutex<ReceivingSeats>,
}

impl SubscriptionSink<StoredMetaflow> for DeliveryRetirements {
    fn delivery_mode(&self) -> SubscriptionDeliveryMode {
        SubscriptionDeliveryMode::Inline
    }

    fn deliver(&self, event: SubscriptionEvent<StoredMetaflow>) -> Result<(), SinkError> {
        let mut seats = self.seats.lock().unwrap_or_else(|e| e.into_inner());
        match event {
            SubscriptionEvent::InitialSnapshot(initial) => {
                seats.awake = initial
                    .snapshot()
                    .records()
                    .iter()
                    .filter_map(|row| {
                        let State::Awake(id) = &row.metaflow.state else {
                            return None;
                        };
                        Some((row.metaflow.address.clone(), id.clone()))
                    })
                    .collect();
            }
            SubscriptionEvent::Delta(delta) => {
                let row = &delta.record().metaflow;
                let current = match (&row.state, delta.kind()) {
                    (State::Awake(id), DeltaKind::Assert | DeltaKind::Mutate) => Some(id),
                    _ => None,
                };
                if seats.awake.get(&row.address) == current {
                    return Ok(());
                }
                if let Some(flow_id) = seats.awake.remove(&row.address) {
                    seats.retirements.push(Retirement {
                        address: row.address.clone(),
                        flow_id,
                    });
                    self.changes.announce();
                }
                if let Some(id) = current {
                    seats.awake.insert(row.address.clone(), id.clone());
                }
            }
        }
        Ok(())
    }
}

trait ReadsRetirements {
    fn watch_retirements(&self) -> Result<(), StoreError>;
    fn take_retirements(&self) -> Vec<Retirement>;
    fn stranded_attempts(&self) -> Result<Vec<QueueDeliveryAttempt>, StoreError>;
}

impl ReadsRetirements for FlowStore {
    fn watch_retirements(&self) -> Result<(), StoreError> {
        let sink: Arc<dyn SubscriptionSink<StoredMetaflow>> = self.delivery_retirements.clone();
        self.engine
            .subscribe(QueryPlan::all(self.metaflows), sink)?;
        Ok(())
    }

    fn take_retirements(&self) -> Vec<Retirement> {
        let mut seats = self
            .delivery_retirements
            .seats
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        std::mem::take(&mut seats.retirements)
    }

    fn stranded_attempts(&self) -> Result<Vec<QueueDeliveryAttempt>, StoreError> {
        let mut current = HashMap::new();
        let mut stranded = Vec::new();
        for attempt in self.member_attempts(MembershipOwner::Unfinished)? {
            let state = match current.entry(attempt.address.clone()) {
                std::collections::hash_map::Entry::Occupied(entry) => entry.into_mut(),
                std::collections::hash_map::Entry::Vacant(entry) => {
                    entry.insert(self.metaflow(&attempt.address)?.map(|row| row.state))
                }
            };
            if !matches!(state.as_ref(), Some(State::Awake(id)) if id == &attempt.flow_id) {
                stranded.push(attempt);
            }
        }
        Ok(stranded)
    }
}

pub trait SettlesRetiredDeliveries {
    /// Subscribe before opening the sockets; every later retirement is queued.
    fn watch_delivery_retirements(&self) -> Result<(), StoreError>;
    /// At open, settle stranded attempts once before current-seat recovery.
    fn settle_stranded_deliveries(&self) -> Result<(), StoreError>;
    /// Follow each committed change of receiving seat until the Nexus stops.
    fn settle_delivery_retirements(&self);
}

trait SettlesRetiredAttempt {
    fn settle_retired_attempt(&self, before: QueueDeliveryAttempt) -> Result<(), StoreError>;
}

impl SettlesRetiredDeliveries for RunningNexus {
    fn watch_delivery_retirements(&self) -> Result<(), StoreError> {
        self.store.watch_retirements()
    }

    fn settle_stranded_deliveries(&self) -> Result<(), StoreError> {
        let attempts = {
            let _turn = self.dispatch_gate.lock().unwrap_or_else(|e| e.into_inner());
            self.store.stranded_attempts()?
        };
        for attempt in attempts {
            self.settle_retired_attempt(attempt)?;
        }
        Ok(())
    }

    fn settle_delivery_retirements(&self) {
        let changes = &self.store.delivery_retirements.changes;
        while !self.stopping.requested() {
            let seen = changes.current();
            for retired in self.store.take_retirements() {
                let attempts = {
                    let _turn = self.dispatch_gate.lock().unwrap_or_else(|e| e.into_inner());
                    self.store
                        .member_attempts(MembershipOwner::UnfinishedAt(&retired.address))
                };
                match attempts {
                    Ok(attempts) => {
                        for attempt in attempts
                            .into_iter()
                            .filter(|attempt| attempt.flow_id == retired.flow_id)
                        {
                            if let Err(error) = self.settle_retired_attempt(attempt) {
                                eprintln!(
                                    "flow-nexus: retired delivery settlement failed: {error}"
                                );
                            }
                        }
                    }
                    Err(error) => eprintln!("flow-nexus: retired attempts unreadable: {error}"),
                }
            }
            if !self.stopping.requested() {
                changes.after(seen);
            }
        }
    }
}

impl SettlesRetiredAttempt for RunningNexus {
    fn settle_retired_attempt(&self, before: QueueDeliveryAttempt) -> Result<(), StoreError> {
        let _turn = self.dispatch_gate.lock().unwrap_or_else(|e| e.into_inner());
        if self
            .store
            .metaflow(&before.address)?
            .is_some_and(|row| row.state == State::Awake(before.flow_id.clone()))
        {
            return Ok(());
        }
        let Some(attempt) = self
            .store
            .queue_delivery_attempt(&before.queue_attempt_id.0)?
        else {
            return Ok(());
        };
        if attempt.queue_delivery_phase.is_finished() {
            return Ok(());
        }
        // An attempt a retired seat leaves is settled on the receipt it
        // already recorded, or abandoned: no history is read for it.
        if matches!(
            attempt.queue_delivery_phase,
            QueueDeliveryPhase::ReceiptRecorded(_)
        ) {
            self.store.settle_recorded_delivery(
                &attempt.queue_attempt_id.0,
                crate::generated::transitions::ReceiptTurn::Running,
            )?;
        } else {
            let record = AbandonmentRecord {
                abandonment_cause: AbandonmentCause::FlowRetired(attempt.flow_id),
                queue_resolution_reason: QueueResolutionReason(
                    "The receiving flow retired.".into(),
                ),
                unix_seconds: UnixSeconds(std::time::SystemTime::now().seconds_since_epoch()),
            };
            let _ = self
                .store
                .abandon_queue_delivery(&attempt.queue_attempt_id.0, record)?;
        }
        Ok(())
    }
}
