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

impl RepairsMetaflows for FlowStore {
    fn inspection(&self, address: &Address) -> Result<Option<Inspection>, StoreError> {
        let Some(stored) = self.stored_metaflow(address)? else {
            return Ok(None);
        };
        let metaflow = stored.metaflow;
        let flow_node_option = match &metaflow.state {
            MetaflowState::Awake(flow_id) => self.flow_node(flow_id)?,
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
        Ok(Some(Inspection {
            binding: Binding {
                flow_node_option,
                process_option: self.bound_process(address)?,
            },
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
        if current == flow_id {
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
            .retain(|past| past != &current && past != flow_id);
        stored.metaflow.past.push(current);
        stored.metaflow.state = MetaflowState::Awake(flow_id.to_owned());
        self.engine.commit_atomic(
            self.engine
                .begin_atomic_commit()
                .mutate(self.metaflows, stored),
        )?;
        Ok(Ok(()))
    }
}
