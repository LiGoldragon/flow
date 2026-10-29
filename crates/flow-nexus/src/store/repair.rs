//! What the meta socket's repairs read and write: a metaflow inspected
//! whole, its flow rebound to a pane or to another flow. Each writes in one
//! commit and does nothing to a harness.

use super::metaflow::SleepsMetaflows;
use super::queue_delivery::{AccountsQueueDelivery, ShowsQueueDelivery};
use super::*;
use meta_signal_flow::{
    Attempt, Binding, Inspected_Data, RebindRejected_Data, SuccessionCause as WireSuccessionCause,
    Successor,
};
use signal_flow::{Address, FlowId, HerdrRoute};

pub(crate) trait RepairsMetaflows {
    /// The metaflow at `address` as Flow holds it: its state, Past and
    /// queue; the row of the flow it is Awake as and the process bound to
    /// its Address; the sender lock held on it; and every attempt
    /// outstanding for it, the hand-over that holds its oldest request back
    /// and each launch at its Address not settled. None when Flow holds no
    /// metaflow there.
    fn inspection(&self, address: &Address) -> Result<Option<Inspected_Data>, StoreError>;
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
    fn inspection(&self, address: &Address) -> Result<Option<Inspected_Data>, StoreError> {
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
        use super::successor::OwesSuccessors;
        let successor_option = self.successor_intent(address)?.map(|intent| Successor {
            address: intent.address,
            predecessor: intent.predecessor,
            succession_cause: match intent.cause {
                super::successor::SuccessionCause::Refresh => WireSuccessionCause::Refresh,
                super::successor::SuccessionCause::Exit => WireSuccessionCause::Exit,
                super::successor::SuccessionCause::Ordered => WireSuccessionCause::Ordered,
                super::successor::SuccessionCause::Replace => WireSuccessionCause::Replace,
            },
            launch_request_id: intent.launch_request_id,
        });
        Ok(Some(Inspected_Data {
            binding: Binding {
                flow_node_option,
                process_option: self.bound_process(address)?,
            },
            metaflow,
            lock_vector,
            attempt_vector,
            successor_option,
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
                MetaflowState::Asleep => Err(RebindRejected_Data::FlowNotLive),
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
        let Some(record) = self.flow(flow_id)? else {
            return Ok(Err(RebindRejected_Data::FlowUnknown));
        };
        if !record.lifecycle.is_live() {
            return Ok(Err(RebindRejected_Data::FlowNotLive));
        }
        let current = match self.stored_metaflow(address)? {
            None => return Ok(Err(RebindRejected_Data::UnknownAddress)),
            Some(stored) => match stored.metaflow.state {
                MetaflowState::Awake(current) => Some(current),
                MetaflowState::Asleep => None,
            },
        };
        if current.as_deref() == Some(flow_id) {
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
            .retain(|past| Some(past.as_str()) != current.as_deref() && past != flow_id);
        if let Some(current) = current {
            stored.metaflow.past.push(current);
        }
        stored.metaflow.state = MetaflowState::Awake(flow_id.to_owned());
        self.engine.commit_atomic(
            self.engine
                .begin_atomic_commit()
                .mutate(self.metaflows, stored),
        )?;
        Ok(Ok(()))
    }
}

type ContextRepairState = Option<(Option<i64>, Option<i64>)>;

/// Store-only owner repairs. These methods never touch a harness.
pub(crate) trait RepairsStoredState {
    fn enqueue_repair(
        &self,
        address: &Address,
        entry: flow_ethos::QueueEntry,
    ) -> Result<Result<(), meta_signal_flow::QueueRepairRejected_Data>, StoreError>;
    fn requeue_repair(
        &self,
        address: &Address,
        id: &str,
        position: i64,
    ) -> Result<Result<(), meta_signal_flow::QueueRepairRejected_Data>, StoreError>;
    fn sleep_repair(
        &self,
        address: &Address,
    ) -> Result<Result<(), meta_signal_flow::QueueRepairRejected_Data>, StoreError>;
    fn forget_repair(
        &self,
        address: &Address,
    ) -> Result<Result<(), meta_signal_flow::QueueRepairRejected_Data>, StoreError>;
    fn set_lifecycle_repair(
        &self,
        flow_id: &str,
        lifecycle: signal_flow::FlowLifecycle,
    ) -> Result<bool, StoreError>;
    fn quarantined_repairs(&self) -> Result<Vec<meta_signal_flow::QuarantinedAttempt>, StoreError>;
    fn context_repair(
        &self,
        flow_id: &str,
    ) -> Result<ContextRepairState, StoreError>;
    fn settle_launch_repair(
        &self,
        request: &str,
        settlement: meta_signal_flow::Settlement,
    ) -> Result<Result<(), meta_signal_flow::SettleLaunchRejected_Data>, StoreError>;
}

impl RepairsStoredState for FlowStore {
    fn enqueue_repair(
        &self,
        address: &Address,
        entry: flow_ethos::QueueEntry,
    ) -> Result<Result<(), meta_signal_flow::QueueRepairRejected_Data>, StoreError> {
        let Some(mut stored) = self.stored_metaflow(address)? else {
            return Ok(Err(
                meta_signal_flow::QueueRepairRejected_Data::UnknownAddress,
            ));
        };
        if stored
            .metaflow
            .queue
            .iter()
            .any(|queued| queued.logical_delivery_id == entry.logical_delivery_id)
        {
            return Ok(Err(
                meta_signal_flow::QueueRepairRejected_Data::DuplicateRequest,
            ));
        }
        stored.metaflow.queue.push(entry);
        self.engine.commit_atomic(
            self.engine
                .begin_atomic_commit()
                .mutate(self.metaflows, stored),
        )?;
        Ok(Ok(()))
    }

    fn requeue_repair(
        &self,
        address: &Address,
        id: &str,
        position: i64,
    ) -> Result<Result<(), meta_signal_flow::QueueRepairRejected_Data>, StoreError> {
        let Some(mut stored) = self.stored_metaflow(address)? else {
            return Ok(Err(
                meta_signal_flow::QueueRepairRejected_Data::UnknownAddress,
            ));
        };
        let Some(index) = stored
            .metaflow
            .queue
            .iter()
            .position(|entry| entry.logical_delivery_id == id)
        else {
            return Ok(Err(meta_signal_flow::QueueRepairRejected_Data::NotQueued));
        };
        let entry = stored.metaflow.queue.remove(index);
        let position = usize::try_from(position)
            .unwrap_or(usize::MAX)
            .min(stored.metaflow.queue.len());
        stored.metaflow.queue.insert(position, entry);
        self.engine.commit_atomic(
            self.engine
                .begin_atomic_commit()
                .mutate(self.metaflows, stored),
        )?;
        Ok(Ok(()))
    }

    fn sleep_repair(
        &self,
        address: &Address,
    ) -> Result<Result<(), meta_signal_flow::QueueRepairRejected_Data>, StoreError> {
        let Some(mut stored) = self.stored_metaflow(address)? else {
            return Ok(Err(
                meta_signal_flow::QueueRepairRejected_Data::UnknownAddress,
            ));
        };
        if let MetaflowState::Awake(flow) = &stored.metaflow.state {
            stored.metaflow.past.push(flow.clone());
        }
        stored.metaflow.state = MetaflowState::Asleep;
        self.engine.commit_atomic(
            self.engine
                .begin_atomic_commit()
                .mutate(self.metaflows, stored),
        )?;
        Ok(Ok(()))
    }

    fn forget_repair(
        &self,
        address: &Address,
    ) -> Result<Result<(), meta_signal_flow::QueueRepairRejected_Data>, StoreError> {
        if self.stored_metaflow(address)?.is_none() {
            return Ok(Err(
                meta_signal_flow::QueueRepairRejected_Data::UnknownAddress,
            ));
        }
        let key = RecordKey::new(FlowStore::address_record_key(address));
        let mut commit = self
            .engine
            .begin_atomic_commit()
            .retract(self.metaflows, key.clone());
        if self.bound_process(address)?.is_some() {
            commit = commit.retract(self.metaflow_processes, key.clone());
        }
        if self.stored_metaflow_lock(address)?.is_some() {
            commit = commit.retract(self.metaflow_locks, key.clone());
        }
        use super::successor::OwesSuccessors;
        commit = self.successor_given(address, commit)?;
        self.engine.commit_atomic(commit)?;
        Ok(Ok(()))
    }

    fn set_lifecycle_repair(
        &self,
        flow_id: &str,
        lifecycle: signal_flow::FlowLifecycle,
    ) -> Result<bool, StoreError> {
        let Some(mut flow) = self.flow(flow_id)? else {
            return Ok(false);
        };
        flow.lifecycle = match lifecycle {
            signal_flow::FlowLifecycle::Pending => FlowLifecycle::Pending,
            signal_flow::FlowLifecycle::Active => FlowLifecycle::Active,
            signal_flow::FlowLifecycle::Stopped => FlowLifecycle::Stopped,
            signal_flow::FlowLifecycle::Retired => FlowLifecycle::Retired,
            signal_flow::FlowLifecycle::Exited => FlowLifecycle::Exited,
        };
        self.engine.mutate_keyed(KeyedMutation::new(
            self.flows,
            RecordKey::new(flow_id),
            flow,
        ))?;
        self.launch_changes.announce();
        Ok(true)
    }

    fn quarantined_repairs(&self) -> Result<Vec<meta_signal_flow::QuarantinedAttempt>, StoreError> {
        Ok(self
            .engine
            .match_records(QueryPlan::all(self.quarantined_launch_attempts))?
            .records()
            .iter()
            .map(|row| meta_signal_flow::QuarantinedAttempt {
                launch_request_id: row.launch_request_id.clone(),
                quarantine_number: i64::try_from(row.quarantine_number).unwrap_or(i64::MAX),
            })
            .collect())
    }

    fn context_repair(
        &self,
        flow_id: &str,
    ) -> Result<ContextRepairState, StoreError> {
        if self.flow(flow_id)?.is_none() {
            return Ok(None);
        }
        use super::context::{NamesFlowModel, RecordsContextSize};
        let context = self.context_size(flow_id)?;
        let threshold = match self.flow_model(flow_id)? {
            Some(model) => self
                .configured_threshold(&model)?
                .map(|configured| configured.refresh_tokens),
            None => None,
        };
        Ok(Some((context, threshold)))
    }

    fn settle_launch_repair(
        &self,
        request: &str,
        settlement: meta_signal_flow::Settlement,
    ) -> Result<Result<(), meta_signal_flow::SettleLaunchRejected_Data>, StoreError> {
        use super::address_hold::HoldsLaunchAddress;
        use crate::generated::memory::LaunchOutcome;
        use crate::generated::starter::LaunchedRecord;
        if self.launch_attempt(request)?.is_none() {
            return Ok(Err(
                meta_signal_flow::SettleLaunchRejected_Data::UnknownLaunch,
            ));
        }
        if self.launch_outcome(request)?.is_some() {
            return Ok(Err(
                meta_signal_flow::SettleLaunchRejected_Data::AlreadySettled,
            ));
        }
        let abandoned = matches!(settlement, meta_signal_flow::Settlement::Abandoned);
        let outcome = match settlement {
            meta_signal_flow::Settlement::Started(flow_id) => {
                let Some(node) = self.flow_node(&flow_id)? else {
                    return Ok(Err(
                        meta_signal_flow::SettleLaunchRejected_Data::FlowUnknown,
                    ));
                };
                LaunchOutcome::Started(LaunchedRecord {
                    flow_id,
                    session_id: node.session_id,
                    origin: node.origin,
                })
            }
            meta_signal_flow::Settlement::Abandoned => {
                LaunchOutcome::StartRejected(signal_flow::StartRejection::OriginUnavailable)
            }
        };
        self.record_launch_outcome(request, outcome)?;
        if let Some(attempt) = self.launch_attempt(request)?
            && let Some(binding) = attempt.native_launch_binding_option
            && abandoned
        {
            let commit = self.given_back(&binding.flow_id, self.engine.begin_atomic_commit())?;
            self.engine.commit_atomic(commit)?;
        }
        Ok(Ok(()))
    }
}
