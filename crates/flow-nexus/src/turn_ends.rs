//! Turn ends, for a peer that acts when a seat's turn ends: the Energy
//! Nexus reads a provider then, instead of on a timer.
//!
//! A turn end is a Claude flow's `Stopped` in its own session, whatever
//! background work it names, or a Codex flow's thread reporting a turn
//! completed, once per turn. It is recorded for a flow a metaflow is Awake
//! as, carrying that metaflow's Address, the flow's harness, the flow and
//! when Flow observed it; a flow Awake as no metaflow records none.
//!
//! `Observe.TurnEnds` opens with `TurnEndsObserved`, the last turn end of
//! each harness, kept in Memory, then sends one `TurnEnded` for each turn
//! end that follows, in order, until the observer goes. Each subscriber has
//! its own queue, filled where the turn end is recorded, so a burst loses
//! none; the subscriber sleeps on the store's announcements and nothing is
//! re-read on a timer. Opening and recording take one lock, so the state on
//! open and the events after it neither overlap nor leave a gap.

use crate::store::AnnouncesLaunchChanges;
use crate::store::metaflow::SleepsMetaflows;
use crate::store::turn_end::RecordsTurnEnds;
use crate::store::{FlowStore, ReadsFlowRows, StoreError};
use crate::{HoldsStopRequest, RunningNexus};
use signal_flow::{Response, TurnEnd};
use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};

/// Every open turn-end subscription's queue of turn ends not yet sent.
#[derive(Debug, Default)]
pub struct TurnEndSubscribers {
    queues: Mutex<TurnEndQueues>,
}

#[derive(Debug, Default)]
struct TurnEndQueues {
    next: u64,
    pending: HashMap<u64, Vec<TurnEnd>>,
}

/// One open subscription; it leaves the subscribers when dropped.
pub struct TurnEndSubscription<'a> {
    subscribers: &'a TurnEndSubscribers,
    id: u64,
}

pub trait HoldsTurnEndSubscribers {
    /// Records `turn_end` as its harness's last and queues it for every
    /// subscriber.
    fn publish(&self, store: &FlowStore, turn_end: TurnEnd) -> Result<(), StoreError>;
    /// Opens a subscription with the state it opens on: the last turn end
    /// of each harness.
    fn subscribe(
        &self,
        store: &FlowStore,
    ) -> Result<(TurnEndSubscription<'_>, Vec<TurnEnd>), StoreError>;
}

impl HoldsTurnEndSubscribers for TurnEndSubscribers {
    fn publish(&self, store: &FlowStore, turn_end: TurnEnd) -> Result<(), StoreError> {
        let mut queues = self
            .queues
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let recorded = store.record_turn_end(turn_end.clone());
        for pending in queues.pending.values_mut() {
            pending.push(turn_end.clone());
        }
        recorded
    }

    fn subscribe(
        &self,
        store: &FlowStore,
    ) -> Result<(TurnEndSubscription<'_>, Vec<TurnEnd>), StoreError> {
        let mut queues = self
            .queues
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let opening = store.last_turn_ends()?;
        let id = queues.next;
        queues.next += 1;
        queues.pending.insert(id, Vec::new());
        Ok((
            TurnEndSubscription {
                subscribers: self,
                id,
            },
            opening,
        ))
    }
}

pub trait TakesTurnEnds {
    /// The turn ends queued for this subscription since it last took them,
    /// oldest first.
    fn take(&self) -> Vec<TurnEnd>;
}

impl TakesTurnEnds for TurnEndSubscription<'_> {
    fn take(&self) -> Vec<TurnEnd> {
        self.subscribers
            .queues
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .pending
            .get_mut(&self.id)
            .map(std::mem::take)
            .unwrap_or_default()
    }
}

impl Drop for TurnEndSubscription<'_> {
    fn drop(&mut self) {
        self.subscribers
            .queues
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .pending
            .remove(&self.id);
    }
}

pub trait RecordsTurnEnd {
    /// The turn of `flow_id` ended: recorded with the Address of the
    /// metaflow it is Awake as and its harness, sent to every subscriber.
    fn turn_ended(&self, flow_id: &str);
}

impl RecordsTurnEnd for RunningNexus {
    fn turn_ended(&self, flow_id: &str) {
        let node = match self.store.flow_node(flow_id) {
            Ok(Some(node)) => node,
            Ok(None) => return,
            Err(error) => {
                eprintln!("flow-nexus: the turn end of {flow_id} was not read: {error}");
                return;
            }
        };
        let metaflow = match self.store.metaflow_awake_as(flow_id) {
            Ok(Some(metaflow)) => metaflow,
            Ok(None) => {
                eprintln!(
                    "flow-nexus: flow {flow_id} ended a turn awake as no metaflow; no turn end is recorded"
                );
                return;
            }
            Err(error) => {
                eprintln!("flow-nexus: the turn end of {flow_id} was not read: {error}");
                return;
            }
        };
        let observed_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .ok()
            .and_then(|elapsed| i64::try_from(elapsed.as_secs()).ok())
            .unwrap_or_default();
        let turn_end = TurnEnd {
            address: metaflow.address,
            harness_kind: node.harness_kind,
            flow_id: node.flow_id,
            observed_at: signal_flow::ObservedAt(observed_at),
        };
        if let Err(error) = self.turn_ends.publish(&self.store, turn_end) {
            eprintln!(
                "flow-nexus: the turn end of {flow_id} was sent but not kept as its harness's last: {error}"
            );
        }
        self.store.launch_changes.announce();
    }
}

pub trait ObservesTurnEnds {
    /// Sends `TurnEndsObserved` with the last turn end of each harness,
    /// then one `TurnEnded` for each later turn end, until the observer
    /// goes or the Nexus stops. It waits on the store's announcements;
    /// nothing is re-read on a timer.
    fn observe_turn_ends(
        &self,
        disconnected: &AtomicBool,
        send: &mut dyn FnMut(&Response) -> Result<(), String>,
    ) -> Result<(), String>;
}

impl ObservesTurnEnds for RunningNexus {
    fn observe_turn_ends(
        &self,
        disconnected: &AtomicBool,
        send: &mut dyn FnMut(&Response) -> Result<(), String>,
    ) -> Result<(), String> {
        let changes = self.store.launch_changes.clone();
        let (subscription, opening) = self
            .turn_ends
            .subscribe(&self.store)
            .map_err(|error| error.to_string())?;
        send(&Response::TurnEndsObserved(opening))?;
        loop {
            let seen = changes.current();
            if disconnected.load(Ordering::Acquire) {
                return Err("the observing peer disconnected".into());
            }
            if self.stopping.requested() {
                return Err("the Nexus is stopping".into());
            }
            for turn_end in subscription.take() {
                send(&Response::TurnEnded(turn_end))?;
            }
            changes.after(seen);
        }
    }
}
