//! A confirmed Claude Stop can precede Herdr's writable pane. Keep the
//! existing queue indexed by its bound rest until the pane announces ready.
use crate::generated::overriding::{HeedsOverrides, SerializesPermaflowEffects};
use crate::herdr::pane::{PaneAgent, ReadsAgentStatus, WritesPane};
use crate::observation_lifetime::{
    CancelsObservation, ObservationCancellation, ObservationCancellationCause,
};
use crate::observe_agent::{
    LocatesHerdrSession, PaneEvent, PaneEvents, PaneObservation, ReadsPaneEvents,
};
use crate::restarting::{FencesSeatEvents, SeatWitness};
use crate::store::metaflow::SleepsMetaflows;
use crate::store::metaflow_watch::WatchesMetaflows;
use crate::store::transitions::ReadsRestEpoch;
use crate::store::{AnnouncesLaunchChanges, LaunchChanges, ReadsFlowRows};
use crate::waking::{DrainsHeldSeats, HoldsWakingMetaflows};
use crate::{HoldsStopRequest, RunningNexus};
use signal_flow::{Address, AgentState, HarnessKind};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::thread::{Scope, ScopedJoinHandle};

#[derive(Clone, Debug, PartialEq, Eq)]
struct BoundRest {
    flow_id: String,
    session: String,
    seat: SeatWitness,
    address: Address,
    epoch: usize,
}

#[derive(Default)]
pub(crate) struct PendingRests {
    requested: Mutex<BTreeMap<String, BoundRest>>,
    pub(crate) changes: LaunchChanges,
}

trait TracksBoundRests {
    fn remember(&self, rest: BoundRest);
    fn remove(&self, rest: &BoundRest);
    fn snapshot(&self) -> BTreeMap<String, BoundRest>;
}

impl TracksBoundRests for PendingRests {
    fn remember(&self, rest: BoundRest) {
        let mut requested = self.requested.lock().unwrap_or_else(|e| e.into_inner());
        if requested.get(&rest.flow_id) != Some(&rest) {
            requested.insert(rest.flow_id.clone(), rest);
            self.changes.announce();
        }
    }
    fn remove(&self, rest: &BoundRest) {
        let mut requested = self.requested.lock().unwrap_or_else(|e| e.into_inner());
        if requested.get(&rest.flow_id) == Some(rest) {
            requested.remove(&rest.flow_id);
            self.changes.announce();
        }
    }
    fn snapshot(&self) -> BTreeMap<String, BoundRest> {
        self.requested
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }
}

pub(crate) trait RemembersPendingRest {
    fn remember_pending_rest(&self, flow_id: &str, session: &str);
}

trait ReadsBoundRest {
    fn bound_rest(&self, flow_id: &str, session: &str) -> Option<BoundRest>;
    fn rest_still_bound(&self, rest: &BoundRest) -> bool;
}

impl ReadsBoundRest for RunningNexus {
    fn bound_rest(&self, flow_id: &str, session: &str) -> Option<BoundRest> {
        let node = self.store.flow_node(flow_id).ok()??;
        if node.harness_kind != HarnessKind::Claude || node.session_id.0 != session {
            return None;
        }
        let metaflow = self.store.metaflow_awake_as(flow_id).ok()??;
        if metaflow.queue.0.is_empty() {
            return None;
        }
        let seat = self.seat_witness(flow_id)?;
        if !self.witness_holds(flow_id, Some(&seat)) {
            return None;
        }
        Some(BoundRest {
            flow_id: flow_id.to_owned(),
            session: session.to_owned(),
            seat,
            address: metaflow.address,
            epoch: self.store.rest_epoch(flow_id).ok()??,
        })
    }

    fn rest_still_bound(&self, rest: &BoundRest) -> bool {
        self.bound_rest(&rest.flow_id, &rest.session).as_ref() == Some(rest)
    }
}

impl RemembersPendingRest for RunningNexus {
    fn remember_pending_rest(&self, flow_id: &str, session: &str) {
        let _turn = self.dispatch_gate.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(rest) = self.bound_rest(flow_id, session) {
            self.pending_rests.remember(rest);
        }
    }
}

struct RestObserver<'scope> {
    rest: BoundRest,
    cancel: Arc<ObservationCancellation>,
    thread: ScopedJoinHandle<'scope, ()>,
}

pub trait ServesRestObservations {
    fn serve_rest_observations<'scope, 'env>(&'env self, scope: &'scope Scope<'scope, 'env>);
}

impl ServesRestObservations for RunningNexus {
    fn serve_rest_observations<'scope, 'env>(&'env self, scope: &'scope Scope<'scope, 'env>) {
        let changes = self.pending_rests.changes.clone();
        if let Err(error) = self.store.watch_native_rests(changes.clone()) {
            eprintln!("flow-nexus: the pending rest observations could not be attached: {error}");
            return;
        }
        let mut observers = BTreeMap::<String, RestObserver<'scope>>::new();
        while !self.stopping.requested() {
            let seen = changes.current();
            let mut requested = self.pending_rests.snapshot();
            requested.retain(|_, rest| {
                if self.rest_still_bound(rest) {
                    true
                } else {
                    self.pending_rests.remove(rest);
                    false
                }
            });
            let ended = observers
                .iter()
                .filter(|(flow, watcher)| requested.get(*flow) != Some(&watcher.rest))
                .map(|(flow, _)| flow.clone())
                .collect::<Vec<_>>();
            for flow in ended {
                if let Some(watcher) = observers.remove(&flow) {
                    watcher
                        .cancel
                        .cancel(ObservationCancellationCause::ObligationRetracted);
                    let _ = watcher.thread.join();
                }
            }
            for (flow, rest) in requested {
                if observers.contains_key(&flow) {
                    continue;
                }
                let cancel = match ObservationCancellation::open(changes.clone()) {
                    Ok(cancel) => Arc::new(cancel),
                    Err(error) => {
                        eprintln!("flow-nexus: cannot observe the rest of {flow}: {error}");
                        self.pending_rests.remove(&rest);
                        continue;
                    }
                };
                let worker_rest = rest.clone();
                let worker_cancel = cancel.clone();
                let thread = scope.spawn(move || {
                    if let Err(error) = self.observe_bound_rest(&worker_rest, &worker_cancel)
                        && worker_cancel.cause().is_none()
                    {
                        eprintln!(
                            "flow-nexus: pending rest of {} had no Herdr observation: {error}",
                            worker_rest.flow_id
                        );
                    }
                    self.pending_rests.remove(&worker_rest);
                });
                observers.insert(
                    flow,
                    RestObserver {
                        rest,
                        cancel,
                        thread,
                    },
                );
            }
            changes.after_while(seen, &|| !self.stopping.requested());
        }
        for observer in observers.into_values() {
            observer
                .cancel
                .cancel(ObservationCancellationCause::NexusStopped);
            let _ = observer.thread.join();
        }
    }
}

trait ObservesBoundRest {
    fn observe_bound_rest(
        &self,
        rest: &BoundRest,
        cancellation: &ObservationCancellation,
    ) -> Result<(), String>;
    fn drain_bound_rest(&self, rest: &BoundRest) -> bool;
}

impl ObservesBoundRest for RunningNexus {
    fn observe_bound_rest(
        &self,
        rest: &BoundRest,
        cancellation: &ObservationCancellation,
    ) -> Result<(), String> {
        let route = &rest.seat.route;
        let socket = self
            .herdr
            .session_socket(&route.herdr_session_name.0)
            .ok_or("Herdr session socket not found")?;
        let mut events = PaneEvents::subscribe(
            &socket,
            &route.herdr_pane_id.0,
            PaneObservation::InputReadiness,
            cancellation,
        )?;
        // The opening read covers readiness reached before subscription.
        if self.drain_bound_rest(rest) {
            return Ok(());
        }
        while let Some(event) = events.next_event(cancellation)? {
            match event {
                PaneEvent::Status(status)
                    if matches!(
                        Some(status.as_str()).agent_state(),
                        AgentState::Idle | AgentState::Done
                    ) =>
                {
                    if self.drain_bound_rest(rest) {
                        return Ok(());
                    }
                }
                PaneEvent::Updated => {
                    if self.drain_bound_rest(rest) {
                        return Ok(());
                    }
                }
                PaneEvent::Gone => return Ok(()),
                PaneEvent::Status(_) => {}
            }
        }
        Ok(())
    }

    fn drain_bound_rest(&self, rest: &BoundRest) -> bool {
        let _effect = self.waking.effect(rest.address.clone());
        let completed = {
            let _turn = self.dispatch_gate.lock().unwrap_or_else(|e| e.into_inner());
            if !self.rest_still_bound(rest) || self.stopping.requested() {
                return true;
            }
            if self.held() || self.permaflow_held(rest.address.clone()) {
                return false;
            }
            let Ok(Some(node)) = self.store.flow_node(&rest.flow_id) else {
                return false;
            };
            if !matches!(
                self.herdr.pane_agent(&node),
                PaneAgent::Present {
                    agent_state: AgentState::Idle | AgentState::Done,
                    ready: true,
                }
            ) {
                return false;
            }
            let Some(lease) = self.waking.hold(&rest.address) else {
                return false;
            };
            let _outcome = self.drain_all_held(rest.address.clone(), &lease);
            !self.rest_still_bound(rest)
        };
        use crate::send_tool::ServesLaunchCalls;
        self.wake_deferred();
        completed
    }
}
