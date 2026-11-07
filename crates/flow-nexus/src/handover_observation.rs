//! Ephemeral FIFO observation of handover-generation retirement.

use signal_flow::{
    Address, HandoverGenerationKey, Response, RetirementChange, RetirementObservationFailure,
    RetirementSnapshot,
};
use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, Condvar, Mutex},
};

const RETIREMENT_OBSERVATION_CAPACITY: usize = 64;

#[derive(Default)]
pub(crate) struct HandoverRetirementObservations {
    registry: Mutex<ObservationRegistry>,
}

#[derive(Default)]
struct ObservationRegistry {
    next_id: u64,
    subscriptions: HashMap<u64, RegisteredObservation>,
}

struct RegisteredObservation {
    address: Address,
    queue: Arc<ObservationQueue>,
}

struct ObservationQueue {
    capacity: usize,
    state: Mutex<ObservationQueueState>,
    changed: Condvar,
}

#[derive(Default)]
struct ObservationQueueState {
    changes: VecDeque<RetirementChange>,
    terminal: Option<RetirementObservationFailure>,
    disconnected: bool,
}

#[derive(Clone)]
pub struct RetirementSubscription {
    id: u64,
    queue: Arc<ObservationQueue>,
    pub(crate) snapshot: RetirementSnapshot,
}

pub(crate) trait ObservesHandoverRetirement {
    fn attach(&self, address: Address, keys: Vec<HandoverGenerationKey>) -> RetirementSubscription;
    fn change(&self, change: RetirementChange);
    fn fail(&self, address: &Address, failure: RetirementObservationFailure);
    fn fail_all(&self, failure: RetirementObservationFailure);
    fn detach(&self, subscription: &RetirementSubscription);
    #[cfg(test)]
    fn subscription_count(&self) -> usize;
}

pub(crate) trait ReadsRetirementSubscription {
    fn next_response(&self) -> Result<Response, String>;
    fn disconnect(&self);
    #[cfg(test)]
    fn queued_change_count(&self) -> usize;
}

trait QueuesRetirementChanges {
    fn push(&self, change: RetirementChange);
    fn fail(&self, failure: RetirementObservationFailure);
}

trait NamesRetirementAddress {
    fn address(&self) -> &Address;
}

impl ObservesHandoverRetirement for HandoverRetirementObservations {
    fn attach(&self, address: Address, keys: Vec<HandoverGenerationKey>) -> RetirementSubscription {
        let mut registry = self
            .registry
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let id = registry.next_id;
        registry.next_id = registry.next_id.wrapping_add(1);
        let queue = Arc::new(ObservationQueue {
            capacity: RETIREMENT_OBSERVATION_CAPACITY,
            state: Mutex::new(ObservationQueueState::default()),
            changed: Condvar::new(),
        });
        registry.subscriptions.insert(
            id,
            RegisteredObservation {
                address: address.clone(),
                queue: Arc::clone(&queue),
            },
        );
        RetirementSubscription {
            id,
            queue,
            snapshot: RetirementSnapshot {
                address,
                handover_generation_key_vector: keys,
            },
        }
    }

    fn change(&self, change: RetirementChange) {
        let address = change.address();
        let registry = self
            .registry
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        for observation in registry
            .subscriptions
            .values()
            .filter(|observation| &observation.address == address)
        {
            observation.queue.push(change.clone());
        }
    }

    fn fail(&self, address: &Address, failure: RetirementObservationFailure) {
        let registry = self
            .registry
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        for observation in registry
            .subscriptions
            .values()
            .filter(|observation| &observation.address == address)
        {
            observation.queue.fail(failure.clone());
        }
    }

    fn fail_all(&self, failure: RetirementObservationFailure) {
        let registry = self
            .registry
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        for observation in registry.subscriptions.values() {
            observation.queue.fail(failure.clone());
        }
    }

    fn detach(&self, subscription: &RetirementSubscription) {
        self.registry
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .subscriptions
            .remove(&subscription.id);
    }

    #[cfg(test)]
    fn subscription_count(&self) -> usize {
        self.registry
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .subscriptions
            .len()
    }
}

impl ReadsRetirementSubscription for RetirementSubscription {
    fn next_response(&self) -> Result<Response, String> {
        let mut state = self
            .queue
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        loop {
            if let Some(change) = state.changes.pop_front() {
                return Ok(Response::RetirementChanged(change));
            }
            if let Some(failure) = state.terminal.take() {
                return Ok(Response::RetirementObservationFailed(failure));
            }
            if state.disconnected {
                return Err("the retirement-observing peer disconnected".into());
            }
            state = self
                .queue
                .changed
                .wait(state)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
        }
    }

    fn disconnect(&self) {
        let mut state = self
            .queue
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state.disconnected = true;
        self.queue.changed.notify_all();
    }

    #[cfg(test)]
    fn queued_change_count(&self) -> usize {
        self.queue
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .changes
            .len()
    }
}

impl QueuesRetirementChanges for ObservationQueue {
    fn push(&self, change: RetirementChange) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if state.terminal.is_some() {
            return;
        }
        if state.changes.len() == self.capacity {
            state.terminal = Some(RetirementObservationFailure::Overflow);
        } else {
            state.changes.push_back(change);
        }
        self.changed.notify_all();
    }

    fn fail(&self, failure: RetirementObservationFailure) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if state.terminal.is_none() {
            state.terminal = Some(failure);
        }
        self.changed.notify_all();
    }
}

impl NamesRetirementAddress for RetirementChange {
    fn address(&self) -> &Address {
        match self {
            RetirementChange::GenerationClosed(key) | RetirementChange::GenerationRemoved(key) => {
                &key.address
            }
            RetirementChange::ArchiveCommitted(committed) => {
                &committed.handover_generation_key.address
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use signal_flow::{Aspect, Model};

    trait BuildsObservationFixture {
        fn address() -> Address;
        fn key(generation: i64) -> HandoverGenerationKey;
    }

    struct Fixture;

    impl BuildsObservationFixture for Fixture {
        fn address() -> Address {
            Address {
                aspect: Aspect::Mind,
                topic: "retirementQueue".into(),
                model: Model::Sol,
            }
        }

        fn key(generation: i64) -> HandoverGenerationKey {
            HandoverGenerationKey {
                address: Self::address(),
                handover_generation: generation,
                predecessor: format!("predecessor-{generation}"),
                successor: format!("successor-{generation}"),
            }
        }
    }

    #[test]
    fn overflow_is_an_out_of_band_terminal_after_the_complete_fifo() {
        let observations = HandoverRetirementObservations::default();
        let subscription = observations.attach(Fixture::address(), Vec::new());
        for generation in 0..=RETIREMENT_OBSERVATION_CAPACITY as i64 {
            observations.change(RetirementChange::GenerationClosed(Fixture::key(generation)));
        }
        for generation in 0..RETIREMENT_OBSERVATION_CAPACITY as i64 {
            assert_eq!(
                subscription.next_response().unwrap(),
                Response::RetirementChanged(RetirementChange::GenerationClosed(Fixture::key(
                    generation
                )))
            );
        }
        assert_eq!(
            subscription.next_response().unwrap(),
            Response::RetirementObservationFailed(RetirementObservationFailure::Overflow)
        );
    }

    #[test]
    fn disconnect_wakes_an_idle_subscription_as_incomplete() {
        let observations = HandoverRetirementObservations::default();
        let subscription = observations.attach(Fixture::address(), Vec::new());
        subscription.disconnect();
        assert_eq!(
            subscription.next_response(),
            Err("the retirement-observing peer disconnected".into())
        );
    }
}
