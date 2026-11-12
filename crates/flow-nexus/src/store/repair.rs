//! What the meta socket's repairs read and write: a metaflow inspected
//! whole, its flow rebound to a pane or to another flow. Each writes in one
//! commit and does nothing to a harness.

use super::metaflow::SleepsMetaflows;
use super::queue_delivery::{AccountsQueueDelivery, ShowsQueueDelivery};
use super::*;
use meta_signal_flow::{Attempt, Binding, RebindRejected_Data};
use signal_flow::{Address, FlowId, HerdrRoute};

pub(crate) struct Inspection {
    pub(crate) metaflow: Metaflow,
    pub(crate) binding: Binding,
    pub(crate) lock_vector: Vec<Lock>,
    pub(crate) attempt_vector: Vec<Attempt>,
    pub(crate) restart_record_option: Option<meta_signal_flow::RestartRecord>,
}

pub(crate) trait RepairsMetaflows {
    /// The metaflow at `address` as Flow holds it: its state, Past and
    /// queue; the row of the flow it is Awake as and the process bound to
    /// its Address; the sender lock held on it; and every attempt
    /// outstanding for it, the hand-over that holds its oldest request back
    /// and each launch at its Address not settled. None when Flow holds no
    /// metaflow there.
    fn inspection(&self, address: &Address) -> Result<Option<Inspection>, StoreError>;
    /// The flow the metaflow at `address` is Awake as, or why there is
    /// none to rebind.
    fn current_flow(
        &self,
        address: &Address,
    ) -> Result<Result<FlowId, RebindRejected_Data>, StoreError>;
    /// Records `route` as the Herdr route of `flow_id`.
    fn rebind_route(&self, flow_id: &str, route: HerdrRoute) -> Result<(), StoreError>;
    /// The metaflow at `address` is Awake as `flow_id` now; the flow it was
    /// Awake as joins Past. A flow another metaflow is Awake as is refused
    /// FlowTaken, naming that metaflow: no flow is two metaflows' current
    /// flow.
    fn rebind_current_flow(
        &self,
        address: &Address,
        flow_id: &str,
    ) -> Result<Result<(), RebindRejected_Data>, StoreError>;
}

/// The obligations Inspect lists at an Address, read and never written:
/// each PendingReap at the Address at its standing, each derived owed reap
/// at `Unrecorded`, the PendingHandover at its refusal, and the derived owed
/// handover at the refusal its owner's evidence gives now. Missing and
/// Unknown are each shown as themselves.
pub(crate) trait InspectsObligations {
    fn obligations_at(&self, address: &Address) -> Result<Vec<Attempt>, StoreError>;
}

impl InspectsObligations for FlowStore {
    fn obligations_at(&self, address: &Address) -> Result<Vec<Attempt>, StoreError> {
        use super::handover::{HandoverOwnerEvidence, HoldsHandovers, JudgesHandoverOwner};
        use super::pending_handover::{DerivesOwedHandover, RecordsPendingHandovers};
        use super::pending_reap::{NamesReapedRecord, RecordsPendingReaps, WritesPendingReap};
        let mut obligations = Vec::new();
        for row in self.pending_reaps_at(address)? {
            let Some(replaced) = self.reaped_record(&row)? else {
                continue;
            };
            let mut reply = self.reap_pending_reply(&replaced);
            reply.address = row.address.clone();
            reply.reap_standing = row.reap_standing.clone();
            obligations.push(Attempt::Reap(reply));
        }
        for derived in self.derived_reaps()? {
            if derived.attempt.launch_profile.address != *address {
                continue;
            }
            let replaced = derived.replaced_record();
            obligations.push(Attempt::Reap(signal_flow::ReapPending {
                address: address.clone(),
                predecessor: signal_flow::Predecessor(replaced.flow_id.clone()),
                launched: self.launched(&replaced.launched_record),
                reap_standing: signal_flow::ReapStanding::Unrecorded,
            }));
        }
        if let Some(row) = self.pending_handover(address)? {
            let session = self
                .flow_node(&row.successor.0.0)?
                .map(|node| node.session_id)
                .unwrap_or(signal_flow::SessionId(String::new()));
            let mut reply = self.handover_pending_reply(&LaunchedRecord {
                flow_id: row.successor.0.clone(),
                session_id: session,
                origin: Origin::Unbound,
            });
            reply.address = row.address.clone();
            reply.handover_owner = row.handover_owner.clone();
            reply.handover_close_refusal = row.handover_close_refusal.clone();
            obligations.push(Attempt::Handover(reply));
        }
        if let Some((receiving, _, owner)) = self.derived_handover(address)? {
            let refusal = match self.handover_memory(address)? {
                Some(memory) => match self.owner_evidence(&memory, "", &receiving.0) {
                    HandoverOwnerEvidence::Refused(refusal) => {
                        signal_flow::HandoverCloseRefusal::Owner(refusal)
                    }
                    _ => signal_flow::HandoverCloseRefusal::PersistenceRefused,
                },
                None => signal_flow::HandoverCloseRefusal::PersistenceRefused,
            };
            let node = self.flow_node(&receiving.0)?;
            obligations.push(Attempt::Handover(signal_flow::HandoverPending {
                address: address.clone(),
                handover_owner: signal_flow::HandoverOwner(FlowId(owner)),
                launched: Launched {
                    flow_id: receiving.clone(),
                    session_id: node
                        .as_ref()
                        .map(|node| node.session_id.clone())
                        .unwrap_or(signal_flow::SessionId(String::new())),
                    herdr_route_selection: node
                        .map(|node| node.herdr_route_selection)
                        .unwrap_or(HerdrRouteSelection::Unavailable),
                    origin: Origin::Unbound,
                },
                handover_close_refusal: refusal,
            }));
        }
        Ok(obligations)
    }
}

impl RepairsMetaflows for FlowStore {
    fn inspection(&self, address: &Address) -> Result<Option<Inspection>, StoreError> {
        let Some(stored) = self.stored_metaflow(address)? else {
            return Ok(None);
        };
        let metaflow = stored.metaflow;
        let flow_node_option = match &metaflow.state {
            MetaflowState::Awake(flow_id) => self.flow_node(&flow_id.0)?,
            MetaflowState::Asleep => None,
        };
        let lock_vector = self
            .stored_metaflow_lock(address)?
            .map(|held| held.lock)
            .into_iter()
            .collect();
        let mut attempt_vector = self
            .outstanding_delivery(address)?
            .and_then(ShowsQueueDelivery::queue_delivery)
            .map(Attempt::Queued)
            .into_iter()
            .collect::<Vec<_>>();
        for stored in self
            .engine
            .match_records(QueryPlan::all(self.launch_attempts))?
            .records()
        {
            if stored.attempt.launch_profile.address == *address
                && self.launch_attempt_unsettled(&stored.attempt)?
            {
                attempt_vector.push(Attempt::Launch(stored.attempt.clone()));
            }
        }
        attempt_vector.extend(self.obligations_at(address)?);
        Ok(Some(Inspection {
            binding: Binding {
                flow_node_option,
                process_option: self.bound_process(address)?,
            },
            restart_record_option: super::restart::RecordsRestarts::restart_at(self, address)?,
            metaflow,
            lock_vector,
            attempt_vector,
        }))
    }

    fn current_flow(
        &self,
        address: &Address,
    ) -> Result<Result<FlowId, RebindRejected_Data>, StoreError> {
        Ok(match self.stored_metaflow(address)? {
            None => Err(RebindRejected_Data::UnknownAddress),
            Some(stored) => match stored.metaflow.state {
                MetaflowState::Awake(flow_id) => Ok(flow_id),
                MetaflowState::Asleep => Err(RebindRejected_Data::NotAwake),
            },
        })
    }

    fn rebind_route(&self, flow_id: &str, route: HerdrRoute) -> Result<(), StoreError> {
        let record = FlowHerdrRouteRecord {
            flow_id: flow_id.to_owned(),
            route,
        };
        let commit = self.engine.begin_atomic_commit();
        let commit = match self.herdr_route(flow_id)? {
            Some(_) => commit.mutate(self.herdr_routes, record),
            None => commit.assert(self.herdr_routes, record),
        };
        self.engine.commit_atomic(commit)?;
        Ok(())
    }

    fn rebind_current_flow(
        &self,
        address: &Address,
        flow_id: &str,
    ) -> Result<Result<(), RebindRejected_Data>, StoreError> {
        let current = match self.current_flow(address)? {
            Ok(current) => current,
            Err(rejection) => return Ok(Err(rejection)),
        };
        if self.flow(flow_id)?.is_none() {
            return Ok(Err(RebindRejected_Data::FlowUnknown));
        }
        if current.0 == flow_id {
            return Ok(Ok(()));
        }
        if let Some(other) = self.metaflow_awake_as(flow_id)? {
            return Ok(Err(RebindRejected_Data::FlowTaken(other.address)));
        }
        let Some(mut stored) = self.stored_metaflow(address)? else {
            return Ok(Err(RebindRejected_Data::UnknownAddress));
        };
        stored
            .metaflow
            .past
            .0
            .retain(|past| past != &current && past.0 != flow_id);
        stored.metaflow.past.0.push(current);
        stored.metaflow.state = MetaflowState::Awake(flow_ethos::FlowId(flow_id.to_owned()));
        // The rebound flow receives from this commit: what the flow it
        // displaced owed at the Address is taken over in it.
        let commit = self
            .engine
            .begin_atomic_commit()
            .mutate(self.metaflows, stored);
        let reassigned = super::pending_reap::ReassignsPendingReaps::reaps_reassigned(
            self,
            address,
            Some(&flow_ethos::FlowId(flow_id.to_owned())),
            commit,
        )?;
        #[cfg(test)]
        {
            use crate::test_faults::{InjectsCrashes, RefusedWrite};
            self.faults.attempted("CurrentFlowRebind");
            if self.faults.refused_now(RefusedWrite::CurrentFlowRebind) {
                return Err(StoreError::StateInvariant);
            }
        }
        self.engine.commit_atomic(reassigned.commit)?;
        if reassigned.moved > 0 {
            super::AnnouncesLaunchChanges::announce(&self.launch_changes);
        }
        Ok(Ok(()))
    }
}
