//! A fresh wake has one durable launch identity and an exact queue prefix.
//! The launch-attempt phase is the only evidence of possible native input.
use super::logical_delivery::{RecordsQueueWrites, StagesQueueStates};
use super::queue_delivery::RequestState;
use super::*;
use crate::generated::memory::{
    Completed, FreshLaunchCohort, InputDeliveryId, Recipient, SimplifiedReplyBinding, Source,
};
use flow_ethos::QueueEntry;
use sema_engine::AtomicCommit;
use signal_flow::{DeliveryState, NativeReceipt, PromptDeliveryResult};
pub(super) const TABLE: &str = "flow_nexus_fresh_launch_cohorts";
pub(super) const FAMILY: &str = "flow-nexus-fresh-launch-cohort";
pub(super) const SCHEMA: &str = "flow-nexus-fresh-launch-cohort-v1";
pub(super) trait DescribesFreshCohorts {
    fn descriptor() -> TableDescriptor<FreshLaunchCohort>;
}
impl DescribesFreshCohorts for FreshLaunchCohort {
    fn descriptor() -> TableDescriptor<Self> {
        TableDescriptor::new(
            TableName::new(TABLE),
            FamilyName::new(FAMILY),
            SchemaHash::for_label(SCHEMA),
        )
    }
}
impl EngineRecord for FreshLaunchCohort {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(FlowStore::address_record_key(&self.address))
    }
}
pub(crate) trait TracksFreshLaunches {
    fn fresh_cohort(&self, address: &Address) -> Result<Option<FreshLaunchCohort>, StoreError>;
    fn claim_fresh(
        &self,
        address: &Address,
        request: &signal_flow::LaunchRequestId,
        prefix: &[QueueEntry],
    ) -> Result<bool, StoreError>;
    fn reconcile_fresh(&self, claim: &FreshLaunchCohort) -> Result<(), StoreError>;
    fn recover_fresh_launches(&self) -> Result<(), StoreError>;
    fn stage_fresh_confirmation(
        &self,
        commit: AtomicCommit,
        attempt: &signal_flow::LaunchAttempt,
        taking: &mut metaflow::TakenAddress,
    ) -> Result<AtomicCommit, StoreError>;
}
trait SelectsClaimedPrefix {
    fn take_prefix(&self, stored: &mut StoredMetaflow) -> Result<Vec<QueueEntry>, StoreError>;
}
impl SelectsClaimedPrefix for FreshLaunchCohort {
    fn take_prefix(&self, stored: &mut StoredMetaflow) -> Result<Vec<QueueEntry>, StoreError> {
        let queue = &mut stored.metaflow.queue.0;
        if stored.metaflow.address != self.address
            || !queue
                .iter()
                .take(self.logical_delivery_id_vector.len())
                .map(|q| &q.logical_delivery_id)
                .eq(self.logical_delivery_id_vector.iter())
        {
            return Err(StoreError::StateInvariant);
        }
        Ok(queue
            .drain(..self.logical_delivery_id_vector.len())
            .collect())
    }
}
impl TracksFreshLaunches for FlowStore {
    fn fresh_cohort(&self, address: &Address) -> Result<Option<FreshLaunchCohort>, StoreError> {
        let found = self.engine.match_records(QueryPlan::key(
            self.fresh_launch_cohorts,
            RecordKey::new(Self::address_record_key(address)),
        ))?;
        Ok(match found.records() {
            [] => None,
            [claim] => Some(claim.clone()),
            _ => return Err(StoreError::StateInvariant),
        })
    }
    fn claim_fresh(
        &self,
        address: &Address,
        request: &signal_flow::LaunchRequestId,
        prefix: &[QueueEntry],
    ) -> Result<bool, StoreError> {
        let _guard = self
            .logical_deliveries
            .write_gate
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let Some(stored) = self.stored_metaflow(address)? else {
            return Ok(false);
        };
        if stored.metaflow.state != flow_ethos::State::Asleep
            || !stored.metaflow.queue.0.starts_with(prefix)
            || self.fresh_cohort(address)?.is_some()
        {
            return Ok(false);
        }
        for entry in prefix {
            if !self.queue_entry_is_writable(entry)? {
                return Ok(false);
            }
        }
        let claim = FreshLaunchCohort {
            address: address.clone(),
            launch_request_id: request.clone(),
            logical_delivery_id_vector: prefix
                .iter()
                .map(|q| q.logical_delivery_id.clone())
                .collect(),
        };
        self.engine.commit_atomic(
            self.engine
                .begin_atomic_commit()
                .assert(self.fresh_launch_cohorts, claim),
        )?;
        #[cfg(test)]
        crate::test_faults::InjectsCrashes::crash_if_armed(
            &self.faults,
            crate::test_faults::CrashPoint::AfterFreshClaim,
        );
        Ok(true)
    }
    fn recover_fresh_launches(&self) -> Result<(), StoreError> {
        for claim in self
            .engine
            .match_records(QueryPlan::all(self.fresh_launch_cohorts))?
            .records()
        {
            use super::overriding::PositionsHeldLaunches;
            use crate::generated::overriding::ParksHeldLaunches;
            let id = &claim.launch_request_id;
            if let Some(attempt) = self.launch_attempt(&id.0)?
                && attempt.launch_profile.address == claim.address
                && attempt.launch_attempt_phase == signal_flow::LaunchAttemptPhase::Reserved
                && attempt.native_launch_intent_option.is_none()
                && attempt.prompt_delivery_intent_option.is_none()
                && self.launch_outcome(&id.0)?.is_none()
                && !self.marked_held_launch(id.clone())?
            {
                // Reserve precedes the durable native intent, itself written
                // before Open. Resume the exact reservation through the same
                // ordered startup path as a previously parked one.
                self.engine.commit_atomic(
                    self.engine
                        .begin_atomic_commit()
                        .assert(self.held_launches, self.next_held_launch(id)?),
                )?;
            }
            self.reconcile_fresh(claim)?;
        }
        Ok(())
    }
    fn reconcile_fresh(&self, claim: &FreshLaunchCohort) -> Result<(), StoreError> {
        let Some(current) = self.fresh_cohort(&claim.address)? else {
            return Ok(());
        };
        if current != *claim {
            return Ok(());
        }
        let attempt = self.launch_attempt(&claim.launch_request_id.0)?;
        if let Some(attempt) = &attempt
            && attempt.launch_profile.address != claim.address
        {
            return Err(StoreError::StateInvariant);
        }
        if let Some(attempt) = &attempt
            && let Some(PromptDeliveryResult::Observed(receipt)) =
                &attempt.prompt_delivery_result_option
        {
            if !matches!(self.confirm_start(&receipt.flow_id.0)?, Output::Started(_)) {
                return Err(StoreError::StateInvariant);
            }
            return Ok(());
        }
        let _guard = self
            .logical_deliveries
            .write_gate
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let refused = matches!(
            self.launch_outcome(&claim.launch_request_id.0)?,
            Some(LaunchOutcome::StartRejected(_))
        );
        let possible = attempt
            .as_ref()
            .is_some_and(|a| a.prompt_delivery_intent_option.is_some());
        if !possible {
            // A crash before native creation, or a terminal refusal before
            // prompt intent, proves no input. A live/parked native attempt is
            // still the owner: do not mint a new launch around it.
            if attempt.is_some() && !refused {
                return Ok(());
            }
            self.engine.commit_atomic(
                self.engine
                    .begin_atomic_commit()
                    .retract(self.fresh_launch_cohorts, claim.record_key()),
            )?;
            return Ok(());
        }
        // The old native session may still own the Address, even after all
        // possibly submitted requests have become final Uncertain. Keep its
        // launch identity until that same attempt confirms; later arrivals
        // must not launch a duplicate session.
        if claim.logical_delivery_id_vector.is_empty() {
            return Ok(());
        }
        let mut stored = self
            .stored_metaflow(&claim.address)?
            .ok_or(StoreError::StateInvariant)?;
        let prefix = claim.take_prefix(&mut stored)?;
        let (commit, _) = self.stage_queue_states(
            self.engine.begin_atomic_commit(),
            &claim.address,
            &prefix,
            &DeliveryState::Uncertain,
            Some(&RequestState::Settled),
        )?;
        let mut guarded = claim.clone();
        guarded.logical_delivery_id_vector.clear();
        self.engine.commit_atomic(
            commit
                .mutate(self.metaflows, stored)
                .mutate(self.fresh_launch_cohorts, guarded),
        )?;
        self.launch_changes.announce();
        Ok(())
    }
    fn stage_fresh_confirmation(
        &self,
        mut commit: AtomicCommit,
        attempt: &signal_flow::LaunchAttempt,
        taking: &mut metaflow::TakenAddress,
    ) -> Result<AtomicCommit, StoreError> {
        let Some(claim) = self.fresh_cohort(&attempt.launch_profile.address)? else {
            return Ok(commit);
        };
        if claim.launch_request_id != attempt.launch_request_id {
            return Err(StoreError::StateInvariant);
        }
        let entries = claim.take_prefix(&mut taking.metaflow)?;
        let key = claim.record_key();
        if entries.is_empty() {
            return Ok(commit.retract(self.fresh_launch_cohorts, key));
        }
        let binding = attempt
            .native_launch_binding_option
            .as_ref()
            .ok_or(StoreError::StateInvariant)?;
        let state = match &attempt.prompt_delivery_result_option {
            Some(PromptDeliveryResult::Observed(receipt))
                if receipt.launch_request_id == claim.launch_request_id
                    && binding.flow_id == receipt.flow_id
                    && binding.native_session_id == receipt.native_session_id =>
            {
                DeliveryState::Delivered(NativeReceipt {
                    harness_kind: binding.harness_kind.clone(),
                    native_session_id: receipt.native_session_id.clone(),
                    native_prompt_id: signal_flow::NativePromptId(receipt.native_turn_id.0.clone()),
                })
            }
            Some(PromptDeliveryResult::Ambiguous(_)) => DeliveryState::Uncertain,
            _ => return Err(StoreError::StateInvariant),
        };
        let (staged, arrivals) = self.stage_queue_states(
            commit,
            &claim.address,
            &entries,
            &state,
            Some(&RequestState::Settled),
        )?;
        commit = self.stage_queue_arrivals(
            staged,
            &claim.address,
            Some(&binding.flow_id.0),
            true,
            arrivals,
        )?;
        if matches!(state, DeliveryState::Delivered(_))
            && attempt.launch_profile.flow_kind == signal_flow::FlowKind::Simplified
            && let Some(entry) = entries.first()
            && let signal_flow::SentBy::Sender(to) = &entry.sent_by
        {
            let reply = SimplifiedReplyBinding {
                flow_id: binding.flow_id.clone(),
                input_delivery_id: InputDeliveryId(entry.logical_delivery_id.0.clone()),
                source: Source(claim.address),
                recipient: Recipient(to.clone()),
                completed: Completed(false),
            };
            commit = commit.assert(self.logical_deliveries.simplified_replies, reply);
        }
        Ok(commit.retract(self.fresh_launch_cohorts, key))
    }
}
