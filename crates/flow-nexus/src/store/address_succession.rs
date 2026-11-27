//! The durable address relation; original request and receipt authors are
//! not rewritten when ownership follows this relation.

use super::{FlowStore, ReadsFlowMemory, StoreError};
use crate::generated::memory::StoredAddressSuccession;
use sema_engine::{
    EngineRecord, FamilyName, MemoryDefinition, MemoryVersion, QueryPlan, RecordKey, SchemaHash,
    TableDescriptor, TableName,
};
use signal_flow::{Address, AddressSuccession};
use std::collections::HashSet;

pub(super) const TABLE: &str = "flow_nexus_address_successions";
pub(super) const FAMILY: &str = "flow-nexus-address-successions";
pub(super) const SCHEMA: &str = "flow-nexus-address-successions-v1";

impl EngineRecord for StoredAddressSuccession {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(FlowStore::address_record_key(&self.first_address))
    }
}

impl MemoryDefinition for StoredAddressSuccession {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(1)
    }

    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label(SCHEMA)
    }
}

pub(super) trait AddressSuccessionTable {
    fn descriptor() -> TableDescriptor<StoredAddressSuccession>;
}

impl AddressSuccessionTable for StoredAddressSuccession {
    fn descriptor() -> TableDescriptor<StoredAddressSuccession> {
        TableDescriptor::new(
            TableName::new(TABLE),
            FamilyName::new(FAMILY),
            SchemaHash::for_label(SCHEMA),
        )
    }
}

pub(crate) trait ReadsAddressSuccessions {
    fn address_succession(
        &self,
        predecessor: &Address,
    ) -> Result<Option<StoredAddressSuccession>, StoreError>;

    /// The old address and the effective successor it names to new callers.
    fn retired_address(
        &self,
        predecessor: &Address,
    ) -> Result<Option<AddressSuccession>, StoreError> {
        let current = self.current_address(predecessor)?;
        Ok((current != *predecessor).then(|| AddressSuccession {
            first_address: predecessor.clone(),
            second_address: current,
        }))
    }

    /// Which address owns the predecessor's outstanding obligations now.
    /// Repeated addresses indicate invalid Memory, not any route to follow.
    fn current_address(&self, predecessor: &Address) -> Result<Address, StoreError> {
        let mut current = predecessor.clone();
        let mut seen = HashSet::new();
        while let Some(edge) = self.address_succession(&current)? {
            if !seen.insert(current) {
                return Err(StoreError::StateInvariant);
            }
            current = edge.second_address;
        }
        Ok(current)
    }
}

impl ReadsAddressSuccessions for FlowStore {
    fn address_succession(
        &self,
        predecessor: &Address,
    ) -> Result<Option<StoredAddressSuccession>, StoreError> {
        let rows = self.engine.match_records(QueryPlan::key(
            self.address_successions,
            RecordKey::new(Self::address_record_key(predecessor)),
        ))?;
        Ok(rows.records().first().cloned())
    }
}

pub(crate) trait RecordsAddressSuccessions {
    /// Installs a relation and moves untouched queue entries as one commit.
    /// An old seat must already have ended; attempts stay with their native
    /// evidence and an unresolved attempt does not become a fresh delivery.
    fn record_address_succession(
        &self,
        succession: &AddressSuccession,
    ) -> Result<Result<AddressSuccession, meta_signal_flow::SucceedRefused_Data>, StoreError>;
}

impl RecordsAddressSuccessions for FlowStore {
    fn record_address_succession(
        &self,
        succession: &AddressSuccession,
    ) -> Result<Result<AddressSuccession, meta_signal_flow::SucceedRefused_Data>, StoreError> {
        use super::ManagesMetaflowState;
        use super::queue_delivery::{AccountsQueueDelivery, EndsQueueDeliveryPhase};
        use meta_signal_flow::{QueueEvidence, StoreFailure, SucceedRefused_Data};
        use signal_flow::DeliveryState;

        let _hold = self
            .logical_deliveries
            .write_gate
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let old = &succession.first_address;
        let new = &succession.second_address;
        if let Some(previous) = self.address_succession(old)? {
            let held = AddressSuccession {
                first_address: previous.first_address,
                second_address: previous.second_address,
            };
            return Ok(if succession == &held {
                Ok(held)
            } else {
                Err(SucceedRefused_Data::ConflictingSuccessor(held))
            });
        }
        let owner = self.current_address(new)?;
        if owner == *old {
            return Ok(Err(SucceedRefused_Data::Cycle(old.clone())));
        }
        let Some(mut predecessor) = self.stored_metaflow(old)? else {
            return Ok(Err(SucceedRefused_Data::UnknownAddress(old.clone())));
        };
        let Some(mut successor) = self.stored_metaflow(&owner)? else {
            return Ok(Err(SucceedRefused_Data::UnknownAddress(owner)));
        };
        if predecessor.metaflow.state != flow_ethos::State::Asleep {
            return Ok(Err(SucceedRefused_Data::StoreRefused(StoreFailure(
                "the predecessor's native seat must be closed first".into(),
            ))));
        }
        let mut commit = self.engine.begin_atomic_commit();
        for entry in &predecessor.metaflow.queue.0 {
            let id = &entry.logical_delivery_id;
            let key = RecordKey::new(id.0.clone());
            let attempts = self.delivery_attempts_for(&id.0)?;
            let current = self
                .engine
                .match_records(QueryPlan::key(self.logical_deliveries.current, key.clone()))?;
            let requests = self
                .engine
                .match_records(QueryPlan::key(self.queue_delivery.requests, key))?;
            let ([current], [request]) = (current.records(), requests.records()) else {
                return Err(StoreError::StateInvariant);
            };
            if current.delivery_snapshot.delivery_state != DeliveryState::Queued
                || attempts
                    .iter()
                    .any(|attempt| !attempt.queue_delivery_phase.is_finished())
            {
                return Ok(Err(SucceedRefused_Data::StoreRefused(StoreFailure(
                    format!("logical delivery {} has unresolved native placement", id.0),
                ))));
            }
            if current.delivery_snapshot.address != *old
                || request.address != *old
                || request.queue_evidence != QueueEvidence::Logical(entry.clone())
            {
                return Err(StoreError::StateInvariant);
            }
            let mut current = current.clone();
            current.delivery_snapshot.address = owner.clone();
            let mut request = request.clone();
            request.address = owner.clone();
            // The historical admission event and every native attempt remain
            // unchanged; only the standing destination has moved.
            commit = commit
                .mutate(self.logical_deliveries.current, current)
                .mutate(self.queue_delivery.requests, request);
        }
        successor
            .metaflow
            .queue
            .0
            .append(&mut predecessor.metaflow.queue.0);
        successor
            .metaflow
            .queue
            .0
            .sort_by(|left, right| left.logical_delivery_id.cmp(&right.logical_delivery_id));
        commit = commit
            .mutate(self.metaflows, predecessor)
            .mutate(self.metaflows, successor)
            .assert(
                self.address_successions,
                StoredAddressSuccession {
                    first_address: old.clone(),
                    second_address: new.clone(),
                },
            );
        self.engine.commit_atomic(commit)?;
        Ok(Ok(succession.clone()))
    }
}
