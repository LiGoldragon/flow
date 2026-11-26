//! An already committed lock retraction names its exact recipient. An
//! authoritative Alive change names its exact seat. Neither the initial
//! snapshot nor a general store notification is a reason to retry an Address.
use super::current_seat::{RecordsCurrentSeats, SeatDecision};
use super::metaflow::SleepsMetaflows;
use super::{
    AnnouncesLaunchChanges, FlowStore, LaunchChanges, ManagesMetaflowState, ReadsFlowMemory,
    StoreError, StoredMetaflowLock,
};
use crate::event_wait::{AnnouncesOnce, AwaitsDescriptors};
use crate::generated::memory::{CurrentSeat, SeatStanding};
use crate::generated::overriding::{HeedsOverrides, SerializesPermaflowEffects};
use crate::waking::SleepsAndWakesSeats;
use crate::{HoldsStopRequest, RunningNexus};
use flow_ethos::{Address, State};
use sema_engine::{
    DeltaKind, QueryPlan, SinkError, SubscriptionDeliveryMode, SubscriptionEvent, SubscriptionSink,
};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

#[derive(Default)]
struct Targets {
    addresses: BTreeMap<String, Address>,
    seats: BTreeSet<String>,
    active: BTreeSet<String>,
}
#[derive(Default)]
pub(crate) struct RecipientRetries {
    targets: Mutex<Targets>,
    changes: LaunchChanges,
    #[cfg(test)]
    completed: Mutex<Vec<Address>>,
    #[cfg(test)]
    completion: LaunchChanges,
}
struct ReleasedRecipients {
    retries: Arc<RecipientRetries>,
    observers: LaunchChanges,
}
impl SubscriptionSink<StoredMetaflowLock> for ReleasedRecipients {
    fn delivery_mode(&self) -> SubscriptionDeliveryMode {
        SubscriptionDeliveryMode::Inline
    }
    fn deliver(&self, event: SubscriptionEvent<StoredMetaflowLock>) -> Result<(), SinkError> {
        if let SubscriptionEvent::Delta(delta) = event
            && delta.kind() == DeltaKind::Retract
        {
            let address = delta.record().lock.second_address.clone();
            let mut targets = self
                .retries
                .targets
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            targets
                .addresses
                .insert(FlowStore::address_record_key(&address), address);
            drop(targets);
            self.retries.changes.announce();
            self.observers.announce();
        }
        Ok(())
    }
}
impl SubscriptionSink<CurrentSeat> for RecipientRetries {
    fn delivery_mode(&self) -> SubscriptionDeliveryMode {
        SubscriptionDeliveryMode::Inline
    }
    fn deliver(&self, event: SubscriptionEvent<CurrentSeat>) -> Result<(), SinkError> {
        if let SubscriptionEvent::Delta(delta) = event
            && matches!(delta.kind(), DeltaKind::Assert | DeltaKind::Mutate)
            && matches!(delta.record().seat_standing, SeatStanding::Alive(_))
        {
            self.targets
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .seats
                .insert(delta.record().flow_id.0.clone());
            self.changes.announce();
        }
        Ok(())
    }
}
pub trait RetriesRecipients {
    fn watch_recipient_retries(&self) -> Result<(), StoreError>;
    fn serve_recipient_retries(&self);
}
trait DispatchesRecipientRetries {
    fn ready_recipients(&self) -> Vec<(String, Address)>;
    fn retry_recipient(&self, address: &Address);
}
impl DispatchesRecipientRetries for RunningNexus {
    fn ready_recipients(&self) -> Vec<(String, Address)> {
        let retries = &self.store.recipient_retries;
        let seats = std::mem::take(
            &mut retries
                .targets
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .seats,
        );
        let _turn = self.dispatch_gate.lock().unwrap_or_else(|e| e.into_inner());
        let resolved = seats
            .into_iter()
            .filter_map(|seat| {
                self.store
                    .metaflow_awake_as(&seat)
                    .ok()
                    .flatten()
                    .map(|r| r.address)
            })
            .collect::<Vec<_>>();
        let mut targets = retries.targets.lock().unwrap_or_else(|e| e.into_inner());
        for address in resolved {
            targets
                .addresses
                .insert(FlowStore::address_record_key(&address), address);
        }
        let mut ready = Vec::new();
        for (key, address) in std::mem::take(&mut targets.addresses) {
            if targets.active.insert(key.clone()) {
                ready.push((key, address));
            } else {
                targets.addresses.insert(key, address);
            }
        }
        ready
    }
    fn retry_recipient(&self, address: &Address) {
        let _effect = self.waking.effect(address.clone());
        {
            let _turn = self.dispatch_gate.lock().unwrap_or_else(|e| e.into_inner());
            if self.held()
                || self.permaflow_held(address.clone())
                || !matches!(self.store.stored_metaflow_lock(address), Ok(None))
            {
                return;
            }
            let Ok(Some(row)) = self.store.metaflow(address) else {
                return;
            };
            if row.queue.0.is_empty() {
                return;
            }
            if let State::Awake(id) = row.state
                && !matches!(
                    self.store.seat_decision(&id.0),
                    Ok(SeatDecision::Observed(SeatStanding::Alive(_)))
                )
            {
                return;
            }
        }
        self.wake_due(address.clone());
    }
}

impl RetriesRecipients for RunningNexus {
    fn watch_recipient_retries(&self) -> Result<(), StoreError> {
        let sink = &self.store.recipient_retries;
        let locks = Arc::new(ReleasedRecipients {
            retries: sink.clone(),
            observers: self.store.launch_changes.clone(),
        });
        self.store
            .engine
            .subscribe(QueryPlan::all(self.store.metaflow_locks), locks)?;
        self.store
            .engine
            .subscribe(QueryPlan::all(self.store.current_seats), sink.clone())?;
        Ok(())
    }
    fn serve_recipient_retries(&self) {
        let retries = &self.store.recipient_retries;
        std::thread::scope(|scope| {
            while !self.stopping.requested() {
                let Ok(cue) = retries.changes.next_descriptor() else {
                    return;
                };
                for (key, address) in self.ready_recipients() {
                    scope.spawn(move || {
                        self.retry_recipient(&address);
                        #[cfg(test)]
                        {
                            retries.completed.lock().unwrap().push(address);
                            retries.completion.announce();
                        }
                        let mut targets = retries.targets.lock().unwrap_or_else(|e| e.into_inner());
                        targets.active.remove(&key);
                        if targets.addresses.contains_key(&key) {
                            retries.changes.announce();
                        }
                    });
                }
                if [self.stopping.descriptor(), cue.descriptor()]
                    .ready(None)
                    .is_err()
                {
                    return;
                }
            }
        });
    }
}

#[cfg(test)]
pub(crate) trait ObservesRetryCompletion {
    fn completed_recipients(&self) -> Vec<Address>;
    fn active_with_followup(&self, address: &Address) -> bool;
    fn completed_by(&self, addresses: &[Address], deadline: std::time::Instant) -> bool;
}
#[cfg(test)]
impl ObservesRetryCompletion for FlowStore {
    fn active_with_followup(&self, address: &Address) -> bool {
        let key = Self::address_record_key(address);
        let targets = self.recipient_retries.targets.lock().unwrap();
        targets.active.contains(&key) && targets.addresses.contains_key(&key)
    }
    fn completed_recipients(&self) -> Vec<Address> {
        self.recipient_retries.completed.lock().unwrap().clone()
    }
    fn completed_by(&self, addresses: &[Address], deadline: std::time::Instant) -> bool {
        self.recipient_retries
            .completion
            .until_holds(deadline, &mut || {
                let completed = self.recipient_retries.completed.lock().unwrap();
                addresses.iter().all(|address| completed.contains(address))
            })
    }
}
