//! The handover a fresh launch owes at its Address, kept as an obligation
//! once its flow is confirmed and receiving.
//!
//! A PendingHandover is keyed by the Address, one at most per Address, and
//! names the owner the handover row names, the flow it is owed to, that
//! flow's launch and why it was not handed over. It is written by C2
//! (`handover_owed`), its refusal rewritten by `handover_refused`, and it
//! is retracted by `handed_over` or by a later launch's supersession.
//! Every write announces `launch_changes` after its commit.

use super::handover::{HandoverClosing, SupersedesObligations, WritesHandoverClosing};
use super::{
    AnnouncesLaunchChanges, FlowStore, LaunchOutcome, ReadsFlowMemory, RecordsLaunchOutcome,
    StoreError, WritesLaunchOutcome,
};
use crate::generated::memory::PendingHandover;
use crate::generated::starter::LaunchedRecord;
use sema_engine::{
    AtomicCommit, EngineRecord, FamilyName, MemoryDefinition, MemoryVersion, QueryPlan, RecordKey,
    SchemaHash, TableDescriptor, TableName,
};
use signal_flow::{Address, HandoverCloseRefusal};

pub(super) const PENDING_HANDOVER_TABLE_NAME: TableName =
    TableName::new("flow_nexus_pending_handovers");
pub(super) const PENDING_HANDOVER_FAMILY: &str = "flow-nexus-pending-handover";
pub(super) const PENDING_HANDOVER_SCHEMA: &str = "flow-nexus-pending-handover-v1";

impl EngineRecord for PendingHandover {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(FlowStore::address_record_key(&self.address))
    }
}

impl MemoryDefinition for PendingHandover {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(1)
    }

    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label(PENDING_HANDOVER_SCHEMA)
    }
}

pub(crate) trait DescribesPendingHandoverTable {
    fn descriptor() -> TableDescriptor<PendingHandover>;
}

impl DescribesPendingHandoverTable for PendingHandover {
    fn descriptor() -> TableDescriptor<PendingHandover> {
        TableDescriptor::new(
            PENDING_HANDOVER_TABLE_NAME,
            FamilyName::new(PENDING_HANDOVER_FAMILY),
            PendingHandover::schema_hash(),
        )
    }
}

/// The Memory operations of pending handovers.
pub(crate) trait RecordsPendingHandovers {
    /// The PendingHandover at `address`.
    fn pending_handover(&self, address: &Address) -> Result<Option<PendingHandover>, StoreError>;
    /// Every PendingHandover.
    fn pending_handovers(&self) -> Result<Vec<PendingHandover>, StoreError>;
    /// C1, one commit: the generation closed when `closing` moves one, the
    /// outcome `Started`, and the supersession at the Address.
    fn started_handed_over(
        &self,
        launch_request_id: &str,
        address: &Address,
        launched: LaunchedRecord,
        closing: HandoverClosing<'_>,
    ) -> Result<(), StoreError>;
    /// C2, one commit: the outcome `HandoverPending`, the PendingHandover
    /// replacing any at its Address, and the supersession there.
    fn handover_owed(
        &self,
        pending: PendingHandover,
        launched: LaunchedRecord,
    ) -> Result<(), StoreError>;
    /// The PendingHandover's refusal rewritten; false when no row stands.
    fn handover_refused(
        &self,
        address: &Address,
        refusal: HandoverCloseRefusal,
    ) -> Result<bool, StoreError>;
    /// One commit: the generation closed, the outcome `Started`, the
    /// PendingHandover retracted.
    fn handed_over(
        &self,
        address: &Address,
        closing: HandoverClosing<'_>,
    ) -> Result<(), StoreError>;
}

impl RecordsPendingHandovers for FlowStore {
    fn pending_handover(&self, address: &Address) -> Result<Option<PendingHandover>, StoreError> {
        let rows = self
            .engine
            .match_records(QueryPlan::key(
                self.pending_handovers,
                RecordKey::new(FlowStore::address_record_key(address)),
            ))?
            .records()
            .to_vec();
        match rows.as_slice() {
            [] => Ok(None),
            [row] => Ok(Some(row.clone())),
            _ => Err(StoreError::StateInvariant),
        }
    }

    fn pending_handovers(&self) -> Result<Vec<PendingHandover>, StoreError> {
        let mut rows = self
            .engine
            .match_records(QueryPlan::all(self.pending_handovers))?
            .records()
            .to_vec();
        rows.sort_by_key(|row| FlowStore::address_record_key(&row.address));
        Ok(rows)
    }

    fn started_handed_over(
        &self,
        launch_request_id: &str,
        address: &Address,
        launched: LaunchedRecord,
        closing: HandoverClosing<'_>,
    ) -> Result<(), StoreError> {
        #[cfg(test)]
        {
            use crate::test_faults::{InjectsCrashes, RefusedWrite};
            self.faults.attempted("StartedHandedOver");
            if self.faults.refused_now(RefusedWrite::StartedSettle) {
                return Err(StoreError::StateInvariant);
            }
        }
        let by = launched.flow_id.clone();
        let commit = closing.written(self, self.engine.begin_atomic_commit());
        let commit =
            self.outcome_written(launch_request_id, LaunchOutcome::Started(launched), commit)?;
        let commit = self.superseding(address, &by, commit, false)?;
        self.engine.commit_atomic(commit)?;
        #[cfg(test)]
        crate::test_faults::InjectsCrashes::crash_if_armed(
            &self.faults,
            crate::test_faults::CrashPoint::AfterHandedOver,
        );
        closing.announce(self);
        self.launch_changes.announce();
        Ok(())
    }

    fn handover_owed(
        &self,
        pending: PendingHandover,
        launched: LaunchedRecord,
    ) -> Result<(), StoreError> {
        #[cfg(test)]
        {
            use crate::test_faults::{InjectsCrashes, RefusedWrite};
            self.faults.attempted("HandoverOwed");
            if self.faults.refused_now(RefusedWrite::HandoverOwing) {
                return Err(StoreError::StateInvariant);
            }
        }
        let address = pending.address.clone();
        let commit = self.outcome_written(
            &pending.launch_request_id.0,
            LaunchOutcome::HandoverPending(launched.clone()),
            self.engine.begin_atomic_commit(),
        )?;
        // The supersession rewrites an earlier launch's outcome; the row it
        // left is replaced here, in the same commit.
        let commit = self.superseding(&address, &launched.flow_id, commit, true)?;
        let commit = self.pending_handover_written(pending, commit)?;
        self.engine.commit_atomic(commit)?;
        #[cfg(test)]
        crate::test_faults::InjectsCrashes::crash_if_armed(
            &self.faults,
            crate::test_faults::CrashPoint::AfterHandoverOwed,
        );
        self.launch_changes.announce();
        Ok(())
    }

    fn handover_refused(
        &self,
        address: &Address,
        refusal: HandoverCloseRefusal,
    ) -> Result<bool, StoreError> {
        #[cfg(test)]
        {
            use crate::test_faults::{InjectsCrashes, RefusedWrite};
            self.faults.attempted("HandoverRefused");
            if self.faults.refused_now(RefusedWrite::HandoverRewrite) {
                return Err(StoreError::StateInvariant);
            }
        }
        let Some(mut row) = self.pending_handover(address)? else {
            return Ok(false);
        };
        if row.handover_close_refusal == refusal {
            return Ok(true);
        }
        row.handover_close_refusal = refusal;
        let commit = self
            .engine
            .begin_atomic_commit()
            .mutate(self.pending_handovers, row);
        self.engine.commit_atomic(commit)?;
        self.launch_changes.announce();
        Ok(true)
    }

    fn handed_over(
        &self,
        address: &Address,
        closing: HandoverClosing<'_>,
    ) -> Result<(), StoreError> {
        #[cfg(test)]
        {
            use crate::test_faults::{InjectsCrashes, RefusedWrite};
            self.faults.attempted("HandedOver");
            if self.faults.refused_now(RefusedWrite::HandedOver) {
                return Err(StoreError::StateInvariant);
            }
        }
        let row = self
            .pending_handover(address)?
            .ok_or(StoreError::StateInvariant)?;
        let launched = match self.launch_outcome(&row.launch_request_id.0)? {
            Some(LaunchOutcome::HandoverPending(launched) | LaunchOutcome::Started(launched)) => {
                launched
            }
            _ => return Err(StoreError::StateInvariant),
        };
        let commit = closing.written(self, self.engine.begin_atomic_commit());
        let commit = self.outcome_written(
            &row.launch_request_id.0,
            LaunchOutcome::Started(launched),
            commit,
        )?;
        let commit = commit.retract(
            self.pending_handovers,
            RecordKey::new(FlowStore::address_record_key(address)),
        );
        self.engine.commit_atomic(commit)?;
        #[cfg(test)]
        crate::test_faults::InjectsCrashes::crash_if_armed(
            &self.faults,
            crate::test_faults::CrashPoint::AfterHandedOver,
        );
        closing.announce(self);
        self.launch_changes.announce();
        Ok(())
    }
}

/// The handover a receiving flow is owed and no row records.
pub(crate) trait DerivesOwedHandover {
    /// The row at `address` names an owner other than the Address's
    /// receiving flow, no PendingHandover names that flow, and no reap
    /// carries the handover (its launch is no replacement, or one already
    /// Replaced): the receiving flow, its launch and the owner the row
    /// names.
    fn derived_handover(
        &self,
        address: &Address,
    ) -> Result<Option<(signal_flow::FlowId, String, String)>, StoreError>;
}

impl DerivesOwedHandover for FlowStore {
    fn derived_handover(
        &self,
        address: &Address,
    ) -> Result<Option<(signal_flow::FlowId, String, String)>, StoreError> {
        use super::handover::HoldsHandovers;
        use super::metaflow::ReadsSuccession;
        use super::{ReadsLaunchAttempt, RecordsReplacement};
        let Some(receiving) = self.receiving_flow(address)? else {
            return Ok(None);
        };
        let Some(owner) = self
            .handover_memory(address)?
            .and_then(|row| row.flow_id_option)
        else {
            return Ok(None);
        };
        if owner == receiving {
            return Ok(None);
        }
        if self
            .pending_handover(address)?
            .is_some_and(|row| row.successor.0 == receiving)
        {
            return Ok(None);
        }
        let Some(launch_request_id) = self.launch_requests_bound_to(&receiving.0)?.pop() else {
            return Ok(None);
        };
        // A replacement's reap carries the handover until its step 3.
        if self.replacement(&launch_request_id)?.is_some()
            && !matches!(
                self.launch_outcome(&launch_request_id)?,
                Some(LaunchOutcome::Replaced(_))
            )
        {
            return Ok(None);
        }
        Ok(Some((receiving, launch_request_id, owner.0)))
    }
}

/// Adds a PendingHandover to a commit, as a new row or over the one its
/// Address keys.
pub(crate) trait WritesPendingHandover {
    fn pending_handover_written(
        &self,
        row: PendingHandover,
        commit: AtomicCommit,
    ) -> Result<AtomicCommit, StoreError>;
}

impl WritesPendingHandover for FlowStore {
    fn pending_handover_written(
        &self,
        row: PendingHandover,
        commit: AtomicCommit,
    ) -> Result<AtomicCommit, StoreError> {
        Ok(if self.pending_handover(&row.address)?.is_some() {
            commit.mutate(self.pending_handovers, row)
        } else {
            commit.assert(self.pending_handovers, row)
        })
    }
}
