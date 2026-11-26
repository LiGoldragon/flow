//! Flow's Memory of the override panel: each flow's transitions, recorded
//! with the cause Overridden, and the panel's switch. Both families are
//! new in 0.57.0: a store written before holds neither and is opened with
//! each registered empty, its switch Acting and no flow's transition held.

use super::{FlowStore, ReadsFlowStore, StoreError};
use crate::generated::memory::{Flow, StoredHold, StoredPermaflowHold};
use crate::generated::overriding::{
    DescribesOverrideTable, Holding, RemembersOverrides, Seat, Subject, Transitioned,
};
use crate::store::metaflow::SleepsMetaflows;
use crate::store::{AnnouncesLaunchChanges, ManagesMetaflowState, ReadsFlowMemory, ReadsFlowRows};
use sema_engine::{
    EngineRecord, FamilyName, MemoryDefinition, MemoryVersion, QueryPlan, RecordKey, SchemaHash,
    TableDescriptor, TableName,
};

pub(super) const TRANSITIONS_TABLE: &str = "flow_nexus_flow_transitions";
pub(super) const TRANSITIONS_FAMILY: &str = "flow-nexus-flow-transitions";
pub(super) const TRANSITIONS_SCHEMA: &str = "flow-nexus-flow-transitions-v1";
pub(super) const HOLD_TABLE: &str = "flow_nexus_hold";
pub(super) const HOLD_FAMILY: &str = "flow-nexus-hold";
pub(super) const HOLD_SCHEMA: &str = "flow-nexus-hold-v1";
pub(super) const PERMAFLOW_HOLD_TABLE: &str = "flow_nexus_permaflow_holds";
pub(super) const PERMAFLOW_HOLD_FAMILY: &str = "flow-nexus-permaflow-holds";
pub(super) const PERMAFLOW_HOLD_SCHEMA: &str = "flow-nexus-permaflow-holds-v1";
/// The switch is one row.
const HOLD_KEY: &str = "hold";

impl EngineRecord for Flow {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.flow_id.0.clone())
    }
}

impl MemoryDefinition for Flow {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(1)
    }

    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label(TRANSITIONS_SCHEMA)
    }
}

impl DescribesOverrideTable for Flow {
    fn descriptor() -> TableDescriptor<Flow> {
        TableDescriptor::new(
            TableName::new(TRANSITIONS_TABLE),
            FamilyName::new(TRANSITIONS_FAMILY),
            SchemaHash::for_label(TRANSITIONS_SCHEMA),
        )
    }
}

impl EngineRecord for StoredHold {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(HOLD_KEY)
    }
}

impl MemoryDefinition for StoredHold {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(1)
    }

    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label(HOLD_SCHEMA)
    }
}

impl DescribesOverrideTable for StoredHold {
    fn descriptor() -> TableDescriptor<StoredHold> {
        TableDescriptor::new(
            TableName::new(HOLD_TABLE),
            FamilyName::new(HOLD_FAMILY),
            SchemaHash::for_label(HOLD_SCHEMA),
        )
    }
}

impl EngineRecord for StoredPermaflowHold {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(FlowStore::address_record_key(&self.address))
    }
}

impl MemoryDefinition for StoredPermaflowHold {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(1)
    }

    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label(PERMAFLOW_HOLD_SCHEMA)
    }
}

impl DescribesOverrideTable for StoredPermaflowHold {
    fn descriptor() -> TableDescriptor<StoredPermaflowHold> {
        TableDescriptor::new(
            TableName::new(PERMAFLOW_HOLD_TABLE),
            FamilyName::new(PERMAFLOW_HOLD_FAMILY),
            SchemaHash::for_label(PERMAFLOW_HOLD_SCHEMA),
        )
    }
}

impl RemembersOverrides for FlowStore {
    fn permaflow_held<N: crate::generated::overriding::Addressed>(
        &self,
        input: N,
    ) -> Result<bool, StoreError> {
        let address = input.address();
        Ok(!self
            .engine
            .match_records(QueryPlan::key(
                self.permaflow_holds,
                StoredPermaflowHold { address }.record_key(),
            ))?
            .records()
            .is_empty())
    }

    fn hold_permaflow<N: crate::generated::overriding::Addressed>(
        &self,
        input: N,
    ) -> Result<bool, StoreError> {
        let address = input.address();
        if self.permaflow_held(address.clone())? {
            return Ok(false);
        }
        let commit = self
            .engine
            .begin_atomic_commit()
            .assert(self.permaflow_holds, StoredPermaflowHold { address });
        self.engine.commit_atomic(commit)?;
        self.launch_changes.announce();
        Ok(true)
    }

    fn release_permaflow<N: crate::generated::overriding::Addressed>(
        &self,
        input: N,
    ) -> Result<bool, StoreError> {
        let address = input.address();
        if !self.permaflow_held(address.clone())? {
            return Ok(false);
        }
        let key = StoredPermaflowHold { address }.record_key();
        let commit = self
            .engine
            .begin_atomic_commit()
            .retract(self.permaflow_holds, key);
        self.engine.commit_atomic(commit)?;
        self.launch_changes.announce();
        self.reminder_changes.announce();
        Ok(true)
    }

    /// Appends the transition to its flow's record, the record naming the
    /// seat's Address, in one write.
    fn record_transition(&self, transitioned: Transitioned) -> Result<bool, StoreError> {
        let _gate = self
            .transitions_gate
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let key = RecordKey::new(transitioned.seat.flow_id.0.clone());
        let held = self
            .engine
            .match_records(QueryPlan::key(self.flow_transitions, key))?
            .records()
            .first()
            .cloned();
        let commit = self.engine.begin_atomic_commit();
        let commit = match held {
            Some(mut flow) => {
                flow.address = transitioned.seat.address;
                flow.transition_vector.push(transitioned.transition);
                commit.mutate(self.flow_transitions, flow)
            }
            None => commit.assert(
                self.flow_transitions,
                Flow {
                    flow_id: transitioned.seat.flow_id,
                    address: transitioned.seat.address,
                    transition_vector: vec![transitioned.transition],
                },
            ),
        };
        #[cfg(test)]
        {
            use crate::test_faults::{InjectsCrashes, RefusedWrite};
            if self.faults.refused_now(RefusedWrite::OverrideTransition) {
                return Err(StoreError::StateInvariant);
            }
        }
        self.engine.commit_atomic(commit)?;
        self.launch_changes.announce();
        Ok(true)
    }

    fn transitions(&self) -> Result<Vec<Flow>, StoreError> {
        Ok(self
            .engine
            .match_records(QueryPlan::all(self.flow_transitions))?
            .records()
            .to_vec())
    }

    /// No row is a Nexus that acts on its own, as every store written
    /// before the panel is.
    fn holding(&self) -> Result<Holding, StoreError> {
        Ok(
            match self
                .engine
                .match_records(QueryPlan::key(self.hold, RecordKey::new(HOLD_KEY)))?
                .records()
                .first()
            {
                Some(held) => Holding::Held(held.moment),
                None => Holding::Acting,
            },
        )
    }

    /// Held writes the row with its moment, a Hold already standing
    /// keeping the moment it was taken; Acting takes the row away. True
    /// when the switch moved. Every thread that waits on a reminder or a
    /// launch is told, so a clock held back runs again at Resume.
    fn switch(&self, holding: Holding) -> Result<bool, StoreError> {
        let standing = self.holding()?;
        let commit = self.engine.begin_atomic_commit();
        let commit = match (standing, holding) {
            (Holding::Held(_), Holding::Held(_)) | (Holding::Acting, Holding::Acting) => {
                return Ok(false);
            }
            (Holding::Acting, Holding::Held(moment)) => {
                commit.assert(self.hold, StoredHold { moment })
            }
            (Holding::Held(_), Holding::Acting) => {
                commit.retract(self.hold, RecordKey::new(HOLD_KEY))
            }
        };
        self.engine.commit_atomic(commit)?;
        self.reminder_changes.announce();
        self.launch_changes.announce();
        Ok(true)
    }

    /// One commit: the flow's row, route, role, owner proof, events and
    /// context size retracted where held, the flow taken out of its
    /// metaflow's Past, and its transition appended. False, and nothing
    /// written, when Flow holds no row for the flow.
    fn forget_flow(&self, transitioned: Transitioned) -> Result<bool, StoreError> {
        let _gate = self
            .transitions_gate
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let flow_id = transitioned.seat.flow_id.0.clone();
        if self.flow(&flow_id)?.is_none() {
            return Ok(false);
        }
        let key = || RecordKey::new(flow_id.clone());
        let mut commit = self.engine.begin_atomic_commit().retract(self.flows, key());
        if !self
            .engine
            .match_records(QueryPlan::key(self.herdr_routes, key()))?
            .records()
            .is_empty()
        {
            commit = commit.retract(self.herdr_routes, key());
        }
        if !self
            .engine
            .match_records(QueryPlan::key(self.roles, key()))?
            .records()
            .is_empty()
        {
            commit = commit.retract(self.roles, key());
        }
        if !self
            .engine
            .match_records(QueryPlan::key(self.native_owner_proofs, key()))?
            .records()
            .is_empty()
        {
            commit = commit.retract(self.native_owner_proofs, key());
        }
        if !self
            .engine
            .match_records(QueryPlan::key(self.event_tables.events, key()))?
            .records()
            .is_empty()
        {
            commit = commit.retract(self.event_tables.events, key());
        }
        if !self
            .engine
            .match_records(QueryPlan::key(self.context_sizes, key()))?
            .records()
            .is_empty()
        {
            commit = commit.retract(self.context_sizes, key());
        }
        if let Some(mut metaflow) = self.metaflow(&transitioned.seat.address)?
            && metaflow.past.0.contains(&transitioned.seat.flow_id)
        {
            metaflow
                .past
                .0
                .retain(|past| past != &transitioned.seat.flow_id);
            commit = commit.mutate(
                self.metaflows,
                crate::generated::memory::StoredMetaflow { metaflow },
            );
        }
        let held = self
            .engine
            .match_records(QueryPlan::key(self.flow_transitions, key()))?
            .records()
            .first()
            .cloned();
        commit = match held {
            Some(mut flow) => {
                flow.address = transitioned.seat.address;
                flow.transition_vector.push(transitioned.transition);
                commit.mutate(self.flow_transitions, flow)
            }
            None => commit.assert(
                self.flow_transitions,
                Flow {
                    flow_id: transitioned.seat.flow_id,
                    address: transitioned.seat.address,
                    transition_vector: vec![transitioned.transition],
                },
            ),
        };
        self.engine.commit_atomic(commit)?;
        self.launch_changes.announce();
        self.metaflow_changes.announce();
        Ok(true)
    }

    /// A flow named is seated at the Address its record names, else the
    /// metaflow Awake as it, else the metaflow whose Past holds it, else the
    /// Address its launch named; a flow Flow holds no row for, or at no
    /// Address, has no seat. An Address is
    /// seated by the flow it is Awake as, else the last flow of its Past.
    fn seat(&self, subject: Subject) -> Result<Option<Seat>, StoreError> {
        match subject {
            Subject::Named(flow_id) => {
                if self.flow(&flow_id.0)?.is_none() {
                    return Ok(None);
                }
                if let Some(metaflow) = self.metaflow_awake_as(&flow_id.0)? {
                    return Ok(Some(Seat {
                        flow_id,
                        address: metaflow.address,
                    }));
                }
                if let Some(metaflow) = self
                    .list_metaflows()?
                    .into_iter()
                    .find(|metaflow| metaflow.past.0.contains(&flow_id))
                {
                    return Ok(Some(Seat {
                        flow_id,
                        address: metaflow.address,
                    }));
                }
                // A flow no metaflow names is seated at the Address its
                // launch named.
                use crate::store::ReadsLaunchAttempt;
                for launch_request_id in self.launch_requests_bound_to(&flow_id.0)? {
                    if let Some(attempt) = self.launch_attempt(&launch_request_id)? {
                        return Ok(Some(Seat {
                            flow_id,
                            address: attempt.launch_profile.address,
                        }));
                    }
                }
                Ok(None)
            }
            Subject::Permaflow(address) => {
                Ok(self
                    .metaflow(&address)?
                    .and_then(|metaflow| match metaflow.state {
                        flow_ethos::State::Awake(flow_id) => Some(Seat { flow_id, address }),
                        flow_ethos::State::Asleep => metaflow
                            .past
                            .0
                            .last()
                            .cloned()
                            .map(|flow_id| Seat { flow_id, address }),
                    }))
            }
        }
    }

    /// Every metaflow Awake as a flow Flow holds live.
    fn seats(&self) -> Result<Vec<Seat>, StoreError> {
        use crate::store::NamesLiveFlow;
        let mut seats = Vec::new();
        for metaflow in self.list_metaflows()? {
            let flow_ethos::State::Awake(flow_id) = metaflow.state else {
                continue;
            };
            if self
                .flow_node(&flow_id.0)?
                .is_some_and(|node| node.flow_lifecycle.is_live())
            {
                seats.push(Seat {
                    flow_id,
                    address: metaflow.address,
                });
            }
        }
        Ok(seats)
    }
}

pub(super) const HELD_LAUNCH_TABLE: &str = "flow_nexus_held_launches";
pub(super) const HELD_LAUNCH_FAMILY: &str = "flow-nexus-held-launches";
pub(super) const HELD_LAUNCH_SCHEMA: &str = "flow-nexus-held-launches-v1";

impl EngineRecord for crate::generated::memory::StoredHeldLaunch {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.launch_request_id.0.clone())
    }
}
impl MemoryDefinition for crate::generated::memory::StoredHeldLaunch {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(1)
    }
    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label(HELD_LAUNCH_SCHEMA)
    }
}
impl DescribesOverrideTable for crate::generated::memory::StoredHeldLaunch {
    fn descriptor() -> TableDescriptor<Self> {
        TableDescriptor::new(
            TableName::new(HELD_LAUNCH_TABLE),
            FamilyName::new(HELD_LAUNCH_FAMILY),
            SchemaHash::for_label(HELD_LAUNCH_SCHEMA),
        )
    }
}
impl crate::generated::overriding::ParksHeldLaunches for FlowStore {
    fn held_launches(&self) -> Result<Vec<crate::generated::memory::StoredHeldLaunch>, StoreError> {
        let mut rows = self
            .engine
            .match_records(QueryPlan::all(self.held_launches))?
            .records()
            .to_vec();
        rows.sort_by_key(|row| row.held_launch_position.0);
        Ok(rows)
    }
    fn marked_held_launch<N: crate::generated::overriding::HeldLaunch>(
        &self,
        input: N,
    ) -> Result<bool, StoreError> {
        Ok(!self
            .engine
            .match_records(QueryPlan::key(
                self.held_launches,
                RecordKey::new(input.request_id().0),
            ))?
            .records()
            .is_empty())
    }
    fn park_held_launch<N: crate::generated::overriding::ParkedLaunch>(
        &self,
        input: N,
    ) -> Result<signal_flow::LaunchAttemptReservation, StoreError> {
        use crate::store::ReadsFlowStore;
        let attempt = input.attempt();
        if let Some(existing) = self.stored_launch_attempt(&attempt.launch_request_id.0)? {
            return Ok(
                if existing.launch_attempt.launch_profile == attempt.launch_profile
                    && existing.launch_attempt.prompt_sha256 == attempt.prompt_sha256
                    && existing.launch_attempt.origin == attempt.origin
                {
                    signal_flow::LaunchAttemptReservation::Existing(existing.launch_attempt)
                } else {
                    signal_flow::LaunchAttemptReservation::Conflict
                },
            );
        }
        let position = self
            .held_launches()?
            .last()
            .map(|row| row.held_launch_position.0)
            .map_or(Some(0), |position| position.checked_add(1))
            .ok_or(StoreError::StateInvariant)?;
        let marker = crate::generated::memory::StoredHeldLaunch {
            launch_request_id: attempt.launch_request_id.clone(),
            held_launch_position: crate::generated::memory::HeldLaunchPosition(position),
        };
        let commit = self
            .engine
            .begin_atomic_commit()
            .assert(
                self.launch_attempts,
                crate::generated::memory::StoredLaunchAttempt {
                    launch_attempt: attempt.clone(),
                },
            )
            .assert(self.held_launches, marker);
        self.engine.commit_atomic(commit)?;
        self.launch_changes.announce();
        Ok(signal_flow::LaunchAttemptReservation::Reserved(attempt))
    }
}
