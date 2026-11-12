//! The reap a replacement owes its predecessor, kept as an obligation once
//! its successor is confirmed and receiving.
//!
//! A PendingReap is keyed by the predecessor and names the launch whose
//! successor it follows, the Address, where the reap stands, and who owes
//! it. Its successor is read through that launch's outcome, never copied
//! here. The row is written by W3 (`reap_owed`), its standing rewritten by
//! `reap_refused`, and it is retracted by `reaped` or by a reassignment.
//! Every write announces `launch_changes` after its commit; the reap
//! worker's supervisor wakes on it.
//!
//! Who owes a reap follows the Address's receiving flow. W3 writes
//! `LaunchSuccessor`: the successor its launch's outcome names. Every
//! commit that changes which flow the Address receives as carries
//! `reaps_reassigned` for it: the end of a flow (`ended_at_address`), the
//! settlement of a launch that made a flow the receiving one
//! (`superseding`, in W3, C1 and C2), and the adoption, binding, rebinding,
//! sleep and forgetting of a metaflow. A row whose pane step is owed or
//! whose predecessor is to be read again becomes `TakenOver` by the new
//! receiving flow, or `AddressHeld` when nothing receives there; a row
//! whose pane was already witnessed gone is finished in that commit from
//! the evidence its standing records, with no Herdr call; a row that stands
//! as typed state is untouched. The one exception runs from a launch's
//! confirmation (W2) to its settlement, which owes the reassignment.
//!
//! The derived owed reap is the obligation no row records but the rows
//! imply: a Replacement whose launch has no outcome and whose attempt is
//! bound to a confirmed flow. It is what a refused W3, or a crash between
//! the successor's confirmation and W3, leaves.

use super::address_hold::HoldsLaunchAddress;
use super::handover::{HandoverClosing, SupersedesObligations, WritesHandoverClosing};
use super::{
    AnnouncesLaunchChanges, FlowLifecycle, FlowStore, LaunchOutcome, NamesLiveFlow, ReadsFlowStore,
    ReadsLaunchAttempt, RecordsLaunchOutcome, StoreError, WritesLaunchOutcome,
};
use crate::generated::memory::{PendingReap, ReapResponsibility, Replacement, Responsible};
use crate::generated::starter::{LaunchedRecord, ReplacedRecord};
use sema_engine::{
    AtomicCommit, EngineRecord, FamilyName, MemoryDefinition, MemoryVersion, QueryPlan, RecordKey,
    SchemaHash, TableDescriptor, TableName,
};
use signal_flow::{Address, LaunchAttempt, ReapStanding};

pub(super) const PENDING_REAP_TABLE_NAME: TableName = TableName::new("flow_nexus_pending_reaps");
pub(super) const PENDING_REAP_FAMILY: &str = "flow-nexus-pending-reap";
/// v2 carries `ReapResponsibility`. The family is new in 0.54.7, so no v1
/// store was ever released: a v1 store from an earlier candidate build is
/// refused at open by its family identity.
pub(super) const PENDING_REAP_SCHEMA: &str = "flow-nexus-pending-reap-v2";

impl EngineRecord for PendingReap {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.predecessor.0.clone())
    }
}

impl MemoryDefinition for PendingReap {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(2)
    }

    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label(PENDING_REAP_SCHEMA)
    }
}

pub(crate) trait DescribesPendingReapTable {
    fn descriptor() -> TableDescriptor<PendingReap>;
}

impl DescribesPendingReapTable for PendingReap {
    fn descriptor() -> TableDescriptor<PendingReap> {
        TableDescriptor::new(
            PENDING_REAP_TABLE_NAME,
            FamilyName::new(PENDING_REAP_FAMILY),
            PendingReap::schema_hash(),
        )
    }
}

/// The step a pending reap still owes, as its standing says.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReapStep {
    /// The predecessor's pane is still to be closed or seen gone: the
    /// worker's (`Owed`, `HerdrUnavailable`, `CloseRefused`).
    PaneClose,
    /// The pane was positively witnessed gone; only the store commit is
    /// owed (`HandoverRefused`, `SettleUnrecorded`). Its closure evidence is
    /// the standing itself: the worker reaches its settlement only after
    /// its own `Absent` snapshot, Herdr's own Gone, or a `Retired` or
    /// `Exited` predecessor. Whatever takes it up makes no Herdr call.
    StoreSettlement,
    /// The predecessor's row is to be read again (`PredecessorUnknown`).
    PredecessorReading,
    /// Stands as typed state; no event takes it [PSYCHE-2]
    /// (`PredecessorMissing`; `Unrecorded` is an answer, never a row).
    Stands,
}

/// The step a reap's standing owes.
pub(crate) trait JudgesReapStep {
    fn reap_step(&self) -> ReapStep;
}

impl JudgesReapStep for ReapStanding {
    fn reap_step(&self) -> ReapStep {
        match self {
            ReapStanding::Owed | ReapStanding::HerdrUnavailable | ReapStanding::CloseRefused => {
                ReapStep::PaneClose
            }
            ReapStanding::HandoverRefused(_) | ReapStanding::SettleUnrecorded => {
                ReapStep::StoreSettlement
            }
            ReapStanding::PredecessorUnknown => ReapStep::PredecessorReading,
            ReapStanding::PredecessorMissing | ReapStanding::Unrecorded => ReapStep::Stands,
        }
    }
}

/// Who owes a pending reap, read from its row and, for `LaunchSuccessor`,
/// its launch's outcome.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ReapOwner {
    /// A flow: its bearing events take the reap up.
    Flow(signal_flow::FlowId),
    /// The row's own Address: no flow receives there.
    Address,
}

/// How a reassigning commit left the reaps at its Address.
pub(crate) struct ReassignedReaps {
    /// The commit with the reassignments added.
    pub(crate) commit: AtomicCommit,
    /// How many rows it rewrites or finishes; an announcement is owed
    /// after the commit when any.
    pub(crate) moved: usize,
}

/// The one reassignment rule both paths add to a commit: the end of a flow
/// and the commit that makes another flow the Address's receiving one.
pub(crate) trait ReassignsPendingReaps {
    /// Every PendingReap at `address` whose owner is not `receiving_after`
    /// (the Address's receiving flow as this commit leaves it) is
    /// reassigned in `commit`, as its step decides: `PaneClose` and
    /// `PredecessorReading` rows become `TakenOver` by that flow, or
    /// `AddressHeld` when it is None; `StoreSettlement` rows are finished,
    /// their outcome `Replaced` and the row retracted, with no generation
    /// closed; `Stands` rows are untouched. A row whose owner is Missing is
    /// left untouched, logged. A store read refused refuses the commit.
    fn reaps_reassigned(
        &self,
        address: &Address,
        receiving_after: Option<&signal_flow::FlowId>,
        commit: AtomicCommit,
    ) -> Result<ReassignedReaps, StoreError>;
}

/// A Replacement whose launch has no outcome and whose attempt is bound to
/// a confirmed flow: the reap it owes is recorded nowhere yet.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DerivedReap {
    pub(crate) replacement: Replacement,
    pub(crate) attempt: LaunchAttempt,
    /// The successor the attempt bound, confirmed.
    pub(crate) successor: signal_flow::FlowId,
}

/// The successor and Address of a reap, as its row and its launch read.
pub(crate) trait NamesReapedRecord {
    /// The ReplacedRecord the derived reap's W3 writes: the predecessor,
    /// and the successor the attempt bound with its native session and
    /// the attempt's origin.
    fn replaced_record(&self) -> ReplacedRecord;
}

impl NamesReapedRecord for DerivedReap {
    fn replaced_record(&self) -> ReplacedRecord {
        let session = self
            .attempt
            .native_launch_binding_option
            .as_ref()
            .map(|binding| binding.native_session_id.0.clone())
            .unwrap_or_default();
        ReplacedRecord {
            flow_id: signal_flow::FlowId(self.replacement.predecessor.0.clone()),
            launched_record: LaunchedRecord {
                flow_id: self.successor.clone(),
                session_id: signal_flow::SessionId(session),
                origin: self.attempt.origin.clone(),
            },
        }
    }
}

/// The Memory operations of pending reaps.
pub(crate) trait RecordsPendingReaps {
    /// The PendingReap keyed by `predecessor`.
    fn pending_reap(&self, predecessor: &str) -> Result<Option<PendingReap>, StoreError>;
    /// Every PendingReap, sorted by predecessor.
    fn pending_reaps(&self) -> Result<Vec<PendingReap>, StoreError>;
    /// Every derived owed reap, sorted by launch request.
    fn derived_reaps(&self) -> Result<Vec<DerivedReap>, StoreError>;
    /// The derived owed reap of one launch, if it is one.
    fn derived_reap(&self, launch_request_id: &str) -> Result<Option<DerivedReap>, StoreError>;
    /// The successor a PendingReap follows, read through its launch's
    /// outcome.
    fn reaped_record(&self, row: &PendingReap) -> Result<Option<ReplacedRecord>, StoreError>;
    /// Who owes the reap. Ok(Some): the owner. Ok(None): a
    /// `LaunchSuccessor` row whose launch outcome is positively absent or
    /// not `ReapPending`; its owner is Missing and is never taken for the
    /// Address. Err: the store could not be read.
    fn reap_owner(&self, row: &PendingReap) -> Result<Option<ReapOwner>, StoreError>;
    /// W3, one commit: the predecessor Stopped when its row is live and the
    /// standing is `Owed`, the outcome `ReapPending`, the PendingReap, and
    /// the supersession at its Address.
    fn reap_owed(&self, pending: PendingReap, replaced: ReplacedRecord) -> Result<(), StoreError>;
    /// The PendingReap's standing rewritten; false when no row stands.
    fn reap_refused(&self, predecessor: &str, standing: ReapStanding) -> Result<bool, StoreError>;
    /// One commit: the generation closed when `closing` moves one, the
    /// outcome `Replaced`, the PendingReap retracted.
    fn reaped(&self, predecessor: &str, closing: HandoverClosing<'_>) -> Result<(), StoreError>;
}

impl RecordsPendingReaps for FlowStore {
    fn pending_reap(&self, predecessor: &str) -> Result<Option<PendingReap>, StoreError> {
        let rows = self
            .engine
            .match_records(QueryPlan::key(
                self.pending_reaps,
                RecordKey::new(predecessor),
            ))?
            .records()
            .to_vec();
        match rows.as_slice() {
            [] => Ok(None),
            [row] => Ok(Some(row.clone())),
            _ => Err(StoreError::StateInvariant),
        }
    }

    fn pending_reaps(&self) -> Result<Vec<PendingReap>, StoreError> {
        let mut rows = self
            .engine
            .match_records(QueryPlan::all(self.pending_reaps))?
            .records()
            .to_vec();
        rows.sort_by(|left, right| left.predecessor.cmp(&right.predecessor));
        Ok(rows)
    }

    fn derived_reaps(&self) -> Result<Vec<DerivedReap>, StoreError> {
        let mut launch_requests = self
            .engine
            .match_records(QueryPlan::all(self.replacements))?
            .records()
            .iter()
            .map(|replacement| replacement.launch_request_id.0.clone())
            .collect::<Vec<_>>();
        launch_requests.sort();
        let mut derived = Vec::new();
        for launch_request_id in launch_requests {
            if let Some(reap) = self.derived_reap(&launch_request_id)? {
                derived.push(reap);
            }
        }
        Ok(derived)
    }

    fn derived_reap(&self, launch_request_id: &str) -> Result<Option<DerivedReap>, StoreError> {
        use super::RecordsReplacement;
        let Some(replacement) = self.replacement(launch_request_id)? else {
            return Ok(None);
        };
        if self.launch_outcome(launch_request_id)?.is_some() {
            return Ok(None);
        }
        let Some(attempt) = self.launch_attempt(launch_request_id)? else {
            return Ok(None);
        };
        let Some(binding) = attempt.native_launch_binding_option.clone() else {
            return Ok(None);
        };
        // Confirmed: its row is past Pending and it holds no Address
        // unconfirmed.
        let confirmed = self
            .flow(&binding.flow_id.0)?
            .is_some_and(|flow| flow.lifecycle != FlowLifecycle::Pending)
            && self.address_hold(&binding.flow_id.0)?.is_none();
        Ok(confirmed.then_some(DerivedReap {
            replacement,
            attempt,
            successor: binding.flow_id,
        }))
    }

    fn reaped_record(&self, row: &PendingReap) -> Result<Option<ReplacedRecord>, StoreError> {
        Ok(match self.launch_outcome(&row.launch_request_id.0)? {
            Some(LaunchOutcome::ReapPending(replaced) | LaunchOutcome::Replaced(replaced)) => {
                Some(replaced)
            }
            _ => None,
        })
    }

    fn reap_owner(&self, row: &PendingReap) -> Result<Option<ReapOwner>, StoreError> {
        Ok(match &row.reap_responsibility {
            ReapResponsibility::LaunchSuccessor => {
                match self.launch_outcome(&row.launch_request_id.0)? {
                    Some(LaunchOutcome::ReapPending(replaced)) => {
                        Some(ReapOwner::Flow(replaced.launched_record.flow_id))
                    }
                    _ => None,
                }
            }
            ReapResponsibility::TakenOver(Responsible(flow_id)) => {
                Some(ReapOwner::Flow(flow_id.clone()))
            }
            ReapResponsibility::AddressHeld => Some(ReapOwner::Address),
        })
    }

    fn reap_owed(&self, pending: PendingReap, replaced: ReplacedRecord) -> Result<(), StoreError> {
        #[cfg(test)]
        {
            use crate::test_faults::{InjectsCrashes, RefusedWrite};
            self.faults.attempted("ReapOwed");
            if self.faults.refused_now(RefusedWrite::ReapOwing) {
                return Err(StoreError::StateInvariant);
            }
        }
        // W3 creates a reap and never reassigns one: its row is owed by the
        // successor its outcome names, whatever the operand carries.
        let pending = PendingReap {
            reap_responsibility: ReapResponsibility::LaunchSuccessor,
            ..pending
        };
        let predecessor = pending.predecessor.0.clone();
        let mut commit = self.engine.begin_atomic_commit();
        // The predecessor is recorded Stopped in the write that owes its
        // reap, so no point stands between the two. A row that could not be
        // read, or that no flow names, records no Stopped.
        if pending.reap_standing == ReapStanding::Owed
            && let Some(mut flow) = self.flow(&predecessor)?
            && flow.lifecycle.is_live()
        {
            flow.lifecycle = FlowLifecycle::Stopped;
            commit = commit.mutate(self.flows, flow);
            commit = self.ended_at_address(&predecessor, commit)?;
        }
        commit = self.outcome_written(
            &pending.launch_request_id.0,
            LaunchOutcome::ReapPending(replaced.clone()),
            commit,
        )?;
        let address = pending.address.clone();
        commit = self.pending_reap_written(pending, commit)?;
        commit = self.superseding(&address, &replaced.launched_record.flow_id, commit, false)?;
        self.engine.commit_atomic(commit)?;
        #[cfg(test)]
        crate::test_faults::InjectsCrashes::crash_if_armed(
            &self.faults,
            crate::test_faults::CrashPoint::AfterReapOwed,
        );
        self.launch_changes.announce();
        Ok(())
    }

    fn reap_refused(&self, predecessor: &str, standing: ReapStanding) -> Result<bool, StoreError> {
        #[cfg(test)]
        {
            use crate::test_faults::{InjectsCrashes, RefusedWrite};
            self.faults.attempted("ReapRefused");
            if self.faults.refused_now(RefusedWrite::ReapRewrite) {
                return Err(StoreError::StateInvariant);
            }
        }
        let Some(mut row) = self.pending_reap(predecessor)? else {
            return Ok(false);
        };
        if row.reap_standing == standing {
            return Ok(true);
        }
        row.reap_standing = standing;
        let commit = self
            .engine
            .begin_atomic_commit()
            .mutate(self.pending_reaps, row);
        self.engine.commit_atomic(commit)?;
        #[cfg(test)]
        crate::test_faults::InjectsCrashes::crash_if_armed(
            &self.faults,
            crate::test_faults::CrashPoint::AfterReapRefused,
        );
        self.launch_changes.announce();
        Ok(true)
    }

    fn reaped(&self, predecessor: &str, closing: HandoverClosing<'_>) -> Result<(), StoreError> {
        #[cfg(test)]
        {
            use crate::test_faults::{InjectsCrashes, RefusedWrite};
            self.faults.attempted("Reaped");
            if self.faults.refused_now(RefusedWrite::ReapSettle) {
                return Err(StoreError::StateInvariant);
            }
        }
        let row = self
            .pending_reap(predecessor)?
            .ok_or(StoreError::StateInvariant)?;
        let replaced = self
            .reaped_record(&row)?
            .ok_or(StoreError::StateInvariant)?;
        let commit = closing.written(self, self.engine.begin_atomic_commit());
        let commit = self.outcome_written(
            &row.launch_request_id.0,
            LaunchOutcome::Replaced(replaced),
            commit,
        )?;
        let commit = commit.retract(self.pending_reaps, RecordKey::new(predecessor));
        self.engine.commit_atomic(commit)?;
        #[cfg(test)]
        crate::test_faults::InjectsCrashes::crash_if_armed(
            &self.faults,
            crate::test_faults::CrashPoint::AfterReaped,
        );
        closing.announce(self);
        self.launch_changes.announce();
        Ok(())
    }
}

/// Adds a PendingReap to a commit, as a new row or over the one its key
/// names.
pub(crate) trait WritesPendingReap {
    fn pending_reap_written(
        &self,
        row: PendingReap,
        commit: AtomicCommit,
    ) -> Result<AtomicCommit, StoreError>;
    /// The PendingReaps at `address`.
    fn pending_reaps_at(&self, address: &Address) -> Result<Vec<PendingReap>, StoreError>;
}

impl WritesPendingReap for FlowStore {
    fn pending_reap_written(
        &self,
        row: PendingReap,
        commit: AtomicCommit,
    ) -> Result<AtomicCommit, StoreError> {
        Ok(if self.pending_reap(&row.predecessor.0)?.is_some() {
            commit.mutate(self.pending_reaps, row)
        } else {
            commit.assert(self.pending_reaps, row)
        })
    }

    fn pending_reaps_at(&self, address: &Address) -> Result<Vec<PendingReap>, StoreError> {
        Ok(self
            .pending_reaps()?
            .into_iter()
            .filter(|row| &row.address == address)
            .collect())
    }
}

impl ReassignsPendingReaps for FlowStore {
    fn reaps_reassigned(
        &self,
        address: &Address,
        receiving_after: Option<&signal_flow::FlowId>,
        commit: AtomicCommit,
    ) -> Result<ReassignedReaps, StoreError> {
        let wanted = match receiving_after {
            Some(flow_id) => ReapOwner::Flow(flow_id.clone()),
            None => ReapOwner::Address,
        };
        let mut commit = commit;
        let mut moved = 0;
        for row in self.pending_reaps_at(address)? {
            let Some(owner) = self.reap_owner(&row)? else {
                // The row and its ReapPending outcome are written in one
                // commit (W3), so a Missing owner is a broken store
                // invariant: nothing settles or moves it.
                eprintln!(
                    "flow-nexus: the reap of {} at {address:?} has no owner its launch {} names; it is left as it stands",
                    row.predecessor.0, row.launch_request_id.0
                );
                continue;
            };
            if owner == wanted {
                continue;
            }
            match row.reap_standing.reap_step() {
                ReapStep::Stands => {}
                ReapStep::StoreSettlement => {
                    let Some(replaced) = self.reaped_record(&row)? else {
                        eprintln!(
                            "flow-nexus: the reap of {} at {address:?} names no outcome to finish; it is left as it stands",
                            row.predecessor.0
                        );
                        continue;
                    };
                    // Finished from the closure its standing records. No
                    // generation is written: the flow it was owed to no
                    // longer receives, and the handover row stays for the
                    // next launch's own settlement to close from its owner.
                    commit = self.outcome_written(
                        &row.launch_request_id.0,
                        LaunchOutcome::Replaced(replaced),
                        commit,
                    )?;
                    commit = commit.retract(
                        self.pending_reaps,
                        RecordKey::new(row.predecessor.0.clone()),
                    );
                    moved += 1;
                }
                ReapStep::PaneClose | ReapStep::PredecessorReading => {
                    let reap_responsibility = match receiving_after {
                        Some(flow_id) => {
                            ReapResponsibility::TakenOver(Responsible(flow_id.clone()))
                        }
                        None => ReapResponsibility::AddressHeld,
                    };
                    commit = commit.mutate(
                        self.pending_reaps,
                        PendingReap {
                            reap_responsibility,
                            ..row
                        },
                    );
                    moved += 1;
                }
            }
        }
        #[cfg(test)]
        if moved > 0 {
            crate::test_faults::InjectsCrashes::attempted(&self.faults, "ReapsReassigned");
        }
        Ok(ReassignedReaps { commit, moved })
    }
}
