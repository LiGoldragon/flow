//! A confirmed Claude Stop can precede Herdr's writable pane. Keep the
//! existing queue indexed by its bound rest until the pane announces ready.
use crate::generated::overriding::{HeedsOverrides, SerializesPermaflowEffects};
use crate::herdr::pane::{PaneAgent, WritesPane};
use crate::herdr::session_stream::{ReadsSessionChanges, SharesSessionStreams};
use crate::herdr::{PaneOwnership, ReadsHerdrPanes};
use crate::observe_agent::LocatesHerdrSession;
use crate::restarting::{FencesSeatEvents, SeatWitness};
use crate::store::metaflow::SleepsMetaflows;
use crate::store::metaflow_watch::WatchesMetaflows;
use crate::store::transitions::ReadsRestEpoch;
use crate::store::{AnnouncesLaunchChanges, LaunchChanges, ReadsFlowRows};
use crate::waking::{DrainsHeldSeats, HoldsSoftForIdle, HoldsWakingMetaflows};
use crate::{HoldsStopRequest, RunningNexus};
use signal_flow::{Address, HarnessKind};
use std::collections::BTreeMap;
use std::sync::Mutex;
use std::thread::Scope;

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

pub trait ServesRestObservations {
    fn serve_rest_observations<'scope, 'env>(&'env self, scope: &'scope Scope<'scope, 'env>);
}

impl ServesRestObservations for RunningNexus {
    fn serve_rest_observations<'scope, 'env>(&'env self, _scope: &'scope Scope<'scope, 'env>) {
        let changes = self.pending_rests.changes.clone();
        if let Err(error) = self.store.watch_native_rests(changes.clone()) {
            eprintln!("flow-nexus: the pending rest observations could not be attached: {error}");
            return;
        }
        self.herdr.session_streams.announce_to(changes.clone());
        while !self.stopping.requested() {
            let seen = changes.current();
            for rest in self.pending_rests.snapshot().into_values() {
                if !self.rest_still_bound(&rest) {
                    self.pending_rests.remove(&rest);
                    continue;
                }
                let route = &rest.seat.route;
                let Some(socket) = self.herdr.session_socket(&route.herdr_session_name.0) else {
                    continue;
                };
                let Ok(cursor) = self
                    .herdr
                    .session_streams
                    .follow(&socket, Some(&route.herdr_pane_id.0))
                else {
                    continue;
                };
                if cursor.available() && self.drain_bound_rest(&rest) {
                    self.pending_rests.remove(&rest);
                }
            }
            changes.after_while(seen, &|| !self.stopping.requested());
        }
    }
}
trait DrainsBoundRest {
    fn drain_bound_rest(&self, rest: &BoundRest) -> bool;
}
impl DrainsBoundRest for RunningNexus {
    fn drain_bound_rest(&self, rest: &BoundRest) -> bool {
        let _effect = self.waking.effect(rest.address.clone());
        if !self.rest_still_bound(rest) || self.stopping.requested() {
            return true;
        }
        let Ok(Some(node)) = self.store.flow_node(&rest.flow_id) else {
            return false;
        };
        if self.herdr.pane_ownership(&node) != PaneOwnership::Owned {
            return false;
        }
        if !matches!(
            self.herdr.pane_agent(&node),
            PaneAgent::Present { ready: true, .. }
        ) {
            return false;
        }
        let lease = {
            let _turn = self.dispatch_gate.lock().unwrap_or_else(|e| e.into_inner());
            if !self.rest_still_bound(rest) || self.stopping.requested() {
                return true;
            }
            if self.held() || self.permaflow_held(rest.address.clone()) {
                return false;
            }
            // Only work due at the rest starts the seat's next turn.
            if !matches!(self.store.metaflow(&rest.address),
                Ok(Some(metaflow)) if self.due_at_rest(&metaflow.queue.0))
            {
                return false;
            }
            let Some(lease) = self.waking.hold(&rest.address) else {
                return false;
            };
            lease
        };
        use crate::generated::operation::Outcome;
        use crate::waking::SleepsAndWakesSeats;
        if matches!(
            self.drain_held(rest.address.clone(), &lease),
            Outcome::Drained(entries) if !entries.is_empty()
        ) {
            self.drain_due_held(&rest.address, lease);
        } else {
            drop(lease);
        }
        let completed = !self.rest_still_bound(rest);
        use crate::send_tool::ServesLaunchCalls;
        self.wake_deferred();
        completed
    }
}
