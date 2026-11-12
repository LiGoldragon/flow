//! Durable current and closed handover generations.

use super::AnnouncesLaunchChanges;
use super::context::NamesFlowModel;
use super::handover_configuration::ReadsHandoverConfiguration;
use super::{FlowStore, ReadsFlowMemory, StoreError};
use crate::handover_observation::{ObservesHandoverRetirement, RetirementSubscription};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use sema_engine::{
    Assertion, AtomicCommit, EngineRecord, FamilyName, KeyedMutation, MemoryDefinition,
    MemoryVersion, QueryPlan, RecordKey, SchemaHash, TableDescriptor, TableName, TableReference,
};
use signal_flow::{
    Address, ArchiveCommitted, HandoverArchiveIdentity, HandoverGenerationKey, HandoverRefusal,
    RetirementChange, RetirementObservationFailure, Section, Touch,
};

pub(super) const HANDOVER_TABLE_NAME: TableName = TableName::new("flow_nexus_handovers");
pub(super) const HANDOVER_FAMILY: &str = "flow-nexus-handover";
pub(super) const HANDOVER_SCHEMA: &str = "flow-nexus-handover-v2";

/// One address's live handover and its closed generations awaiting archive.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub struct HandoverMemory {
    pub address: Address,
    pub current: Vec<Section>,
    pub touch: Option<Touch>,
    pub generation: i64,
    pub current_flow_id: Option<String>,
    pub closed: Vec<ClosedHandover>,
}

/// A predecessor's immutable handover after its successor starts.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub struct ClosedHandover {
    pub generation: i64,
    pub predecessor: String,
    pub successor: String,
    pub sections: Vec<Section>,
    pub touch: Option<Touch>,
}

/// Authenticated identity of the flow writing the live generation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HandoverWriter {
    pub address: Address,
    pub flow_id: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HandoverRemovalOutcome {
    Removed,
    AlreadyAbsent,
    IdentityMismatch,
}

trait IdentifiesClosedHandover {
    fn generation_key(&self, address: &Address) -> HandoverGenerationKey;
}

impl IdentifiesClosedHandover for ClosedHandover {
    fn generation_key(&self, address: &Address) -> HandoverGenerationKey {
        HandoverGenerationKey {
            address: address.clone(),
            handover_generation: signal_flow::HandoverGeneration(self.generation),
            predecessor: signal_flow::Predecessor(flow_ethos::FlowId(self.predecessor.clone())),
            successor: signal_flow::Successor(flow_ethos::FlowId(self.successor.clone())),
        }
    }
}

impl EngineRecord for HandoverMemory {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(FlowStore::address_record_key(&self.address))
    }
}

impl MemoryDefinition for HandoverMemory {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(2)
    }

    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label(HANDOVER_SCHEMA)
    }
}

pub(super) trait DescribesHandoverTable {
    fn descriptor() -> TableDescriptor<HandoverMemory>;
}

impl DescribesHandoverTable for HandoverMemory {
    fn descriptor() -> TableDescriptor<HandoverMemory> {
        TableDescriptor::new(
            HANDOVER_TABLE_NAME,
            FamilyName::new(HANDOVER_FAMILY),
            Self::schema_hash(),
        )
    }
}

/// The Memory operations required by the runtime handover path.
pub trait HoldsHandovers {
    fn handover_addresses_with_closed(&self) -> Result<Vec<Address>, StoreError>;
    /// Reads the complete Memory row. `None` means no handover has ever been
    /// written; `Some(Unknown)` means a successful write had no measurement.
    fn handover_memory(&self, address: &Address) -> Result<Option<HandoverMemory>, StoreError>;
    fn ensure_live_handover(
        &self,
        address: &Address,
        flow_id: &str,
    ) -> Result<HandoverMemory, StoreError>;
    fn handover(&self, address: &Address) -> Result<Vec<Section>, StoreError>;
    fn handover_for_flow(
        &self,
        address: &Address,
        flow_id: &str,
    ) -> Result<Option<Vec<Section>>, StoreError>;
    fn closed_handovers(
        &self,
        address: &Address,
        count: usize,
    ) -> Result<Vec<ClosedHandover>, StoreError>;
    fn observe_handover_retirement(
        &self,
        address: &Address,
    ) -> Result<RetirementSubscription, StoreError>;
    fn complete_closed_handover_retirement(
        &self,
        address: &Address,
        expected: &ClosedHandover,
        archive_identity: HandoverArchiveIdentity,
    ) -> Result<HandoverRemovalOutcome, StoreError>;
    fn fail_handover_retirement(&self, address: &Address, detail: String);
    #[cfg(test)]
    fn fail_next_handover_removal(&self);
    #[cfg(test)]
    fn fail_next_closed_handover_read(&self);
    #[cfg(test)]
    fn fail_next_retirement_completion_read(&self);
    #[cfg(test)]
    fn keep_handover(
        &self,
        address: &Address,
        caller: &Address,
        section: Section,
        touch: Touch,
    ) -> Result<Touch, HandoverRefusal>;
    fn keep_handover_for_flow(
        &self,
        writer: &HandoverWriter,
        section: Section,
        touch: Touch,
    ) -> Result<Touch, HandoverRefusal>;
    #[cfg(test)]
    fn drop_handover(
        &self,
        address: &Address,
        caller: &Address,
        title: signal_flow::Title,
        touch: Touch,
    ) -> Result<Touch, HandoverRefusal>;
    fn drop_handover_for_flow(
        &self,
        writer: &HandoverWriter,
        title: signal_flow::Title,
        touch: Touch,
    ) -> Result<Touch, HandoverRefusal>;
    /// The generation `predecessor` held closed for `successor` in its own
    /// write: `handover_closing` written and announced alone.
    #[cfg(test)]
    fn close_handover_for_successor(
        &self,
        address: &Address,
        predecessor: &str,
        successor: &str,
    ) -> Result<(), StoreError>;
    fn handover_headings(&self, address: &Address)
    -> Result<Vec<signal_flow::Heading>, StoreError>;
    /// Whether the current flow has a policy-qualified measured handover touch
    /// and may be automatically refreshed at its threshold.
    fn handover_green(
        &self,
        address: &Address,
        flow_id: &str,
        refresh: i64,
    ) -> Result<bool, StoreError>;
}

pub(crate) trait RegistersHandoverTable {
    fn register_handover_table(
        engine: &mut sema_engine::Engine,
    ) -> Result<TableReference<HandoverMemory>, StoreError>;
}

impl RegistersHandoverTable for FlowStore {
    fn register_handover_table(
        engine: &mut sema_engine::Engine,
    ) -> Result<TableReference<HandoverMemory>, StoreError> {
        Ok(engine.register_table(HandoverMemory::descriptor())?)
    }
}

impl HoldsHandovers for FlowStore {
    fn handover_addresses_with_closed(&self) -> Result<Vec<Address>, StoreError> {
        Ok(self
            .engine
            .match_records(sema_engine::QueryPlan::all(self.handovers))?
            .records()
            .iter()
            .filter(|row| !row.closed.is_empty())
            .map(|row| row.address.clone())
            .collect())
    }

    fn handover_memory(&self, address: &Address) -> Result<Option<HandoverMemory>, StoreError> {
        self.handover_row(address)
    }

    fn ensure_live_handover(
        &self,
        address: &Address,
        flow_id: &str,
    ) -> Result<HandoverMemory, StoreError> {
        let _gate = self
            .handover_write_gate
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(row) = self.handover_row(address)? {
            if row.current_flow_id.as_deref() == Some(flow_id) {
                return Ok(row);
            }
            return Err(StoreError::StateInvariant);
        }
        let row = HandoverMemory {
            address: address.clone(),
            current: Vec::new(),
            touch: None,
            generation: 0,
            current_flow_id: Some(flow_id.into()),
            closed: Vec::new(),
        };
        self.write_handover(row.clone())?;
        Ok(row)
    }

    fn handover(&self, address: &Address) -> Result<Vec<Section>, StoreError> {
        Ok(self
            .handover_row(address)?
            .map_or_else(Vec::new, |row| row.current))
    }

    fn handover_for_flow(
        &self,
        address: &Address,
        flow_id: &str,
    ) -> Result<Option<Vec<Section>>, StoreError> {
        let Some(row) = self.handover_row(address)? else {
            return Ok(None);
        };
        if row.current_flow_id.as_deref() == Some(flow_id) {
            return Ok(Some(row.current));
        }
        Ok(row
            .closed
            .iter()
            .rev()
            .find(|closed| closed.predecessor == flow_id)
            .map(|closed| closed.sections.clone()))
    }

    fn closed_handovers(
        &self,
        address: &Address,
        count: usize,
    ) -> Result<Vec<ClosedHandover>, StoreError> {
        #[cfg(test)]
        if std::mem::take(
            &mut *self
                .fail_next_closed_handover_read
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()),
        ) {
            return Err(StoreError::StateInvariant);
        }
        let Some(row) = self.handover_row(address)? else {
            return Ok(Vec::new());
        };
        if count == 0 {
            return Ok(row.closed);
        }
        let excess = row.closed.len().saturating_sub(count);
        Ok(row.closed.into_iter().take(excess).collect())
    }

    fn observe_handover_retirement(
        &self,
        address: &Address,
    ) -> Result<RetirementSubscription, StoreError> {
        let _gate = self
            .handover_write_gate
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let keys = self.handover_row(address)?.map_or_else(Vec::new, |row| {
            row.closed
                .iter()
                .map(|closed| closed.generation_key(address))
                .collect()
        });
        Ok(self.handover_observations.attach(address.clone(), keys))
    }

    fn complete_closed_handover_retirement(
        &self,
        address: &Address,
        expected: &ClosedHandover,
        archive_identity: HandoverArchiveIdentity,
    ) -> Result<HandoverRemovalOutcome, StoreError> {
        let _gate = self
            .handover_write_gate
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        #[cfg(test)]
        let row_read = if std::mem::take(
            &mut *self
                .fail_next_retirement_completion_read
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()),
        ) {
            Err(StoreError::StateInvariant)
        } else {
            self.handover_row(address)
        };
        #[cfg(not(test))]
        let row_read = self.handover_row(address);
        let row = match row_read {
            Ok(row) => row,
            Err(error) => {
                self.handover_observations.fail(
                    address,
                    RetirementObservationFailure::MissingEvidence(format!(
                        "the archived generation could not be read from Memory: {error}"
                    )),
                );
                return Err(error);
            }
        };
        let Some(mut row) = row else {
            self.handover_observations.fail(
                address,
                RetirementObservationFailure::MissingEvidence(
                    "the archived generation is already absent".into(),
                ),
            );
            return Ok(HandoverRemovalOutcome::AlreadyAbsent);
        };
        let Some(position) = row.closed.iter().position(|closed| closed == expected) else {
            let outcome = if row
                .closed
                .iter()
                .any(|closed| closed.generation == expected.generation)
            {
                HandoverRemovalOutcome::IdentityMismatch
            } else {
                HandoverRemovalOutcome::AlreadyAbsent
            };
            self.handover_observations.fail(
                address,
                RetirementObservationFailure::MissingEvidence(match outcome {
                    HandoverRemovalOutcome::IdentityMismatch => {
                        "the archived generation identity does not match Memory".into()
                    }
                    HandoverRemovalOutcome::AlreadyAbsent => {
                        "the archived generation is already absent".into()
                    }
                    HandoverRemovalOutcome::Removed => unreachable!(),
                }),
            );
            return Ok(outcome);
        };
        let key = expected.generation_key(address);
        self.handover_observations
            .change(RetirementChange::ArchiveCommitted(ArchiveCommitted {
                handover_generation_key: key.clone(),
                handover_archive_identity: archive_identity,
            }));
        row.closed.remove(position);
        #[cfg(test)]
        let write = if std::mem::take(
            &mut *self
                .fail_next_handover_removal
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()),
        ) {
            Err(StoreError::StateInvariant)
        } else {
            self.write_handover(row)
        };
        #[cfg(not(test))]
        let write = self.write_handover(row);
        if let Err(error) = write {
            self.handover_observations.fail(
                address,
                RetirementObservationFailure::MissingEvidence(format!(
                    "the archived generation could not be removed from Memory: {error}"
                )),
            );
            return Err(error);
        }
        self.handover_observations
            .change(RetirementChange::GenerationRemoved(key));
        Ok(HandoverRemovalOutcome::Removed)
    }

    fn fail_handover_retirement(&self, address: &Address, detail: String) {
        let _gate = self
            .handover_write_gate
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        self.handover_observations.fail(
            address,
            RetirementObservationFailure::MissingEvidence(detail),
        );
    }

    #[cfg(test)]
    fn fail_next_handover_removal(&self) {
        *self
            .fail_next_handover_removal
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = true;
    }

    #[cfg(test)]
    fn fail_next_closed_handover_read(&self) {
        *self
            .fail_next_closed_handover_read
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = true;
    }

    #[cfg(test)]
    fn fail_next_retirement_completion_read(&self) {
        *self
            .fail_next_retirement_completion_read
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = true;
    }

    fn handover_green(
        &self,
        address: &Address,
        flow_id: &str,
        refresh: i64,
    ) -> Result<bool, StoreError> {
        let policies = self.handover_policies()?;
        let Some(model_name) = self.flow_model(flow_id)? else {
            return Ok(false);
        };
        let meta_signal_flow::HandoverPoliciesConfiguration::Configured(policies) = policies else {
            return Ok(false);
        };
        let Some(policy) = policies
            .into_iter()
            .find(|policy| policy.model_name.0 == model_name)
        else {
            return Ok(false);
        };
        let Some(row) = self.handover_row(address)? else {
            return Ok(false);
        };
        let Some(signal_flow::Touch::Known(touch)) = row.touch else {
            return Ok(false);
        };
        if touch.flow_id.0 != flow_id {
            return Ok(false);
        }
        let left = i128::from(100 - policy.warning_share_left.0);
        Ok(i128::from(touch.tokens.0) * 100 >= i128::from(refresh) * left)
    }

    fn handover_headings(
        &self,
        address: &Address,
    ) -> Result<Vec<signal_flow::Heading>, StoreError> {
        Ok(self.handover_row(address)?.map_or_else(Vec::new, |row| {
            row.current
                .into_iter()
                .map(|section| signal_flow::Heading {
                    title: section.title,
                    description: section.description,
                })
                .collect()
        }))
    }

    #[cfg(test)]
    fn keep_handover(
        &self,
        address: &Address,
        caller: &Address,
        section: Section,
        touch: Touch,
    ) -> Result<Touch, HandoverRefusal> {
        let _gate = self
            .handover_write_gate
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        ensure_owner(address, caller)?;
        let mut row = self
            .handover_row(address)
            .map_err(StoreError::handover_refusal)?
            .unwrap_or_else(|| HandoverMemory {
                address: address.clone(),
                current: Vec::new(),
                touch: None,
                generation: 0,
                current_flow_id: None,
                closed: Vec::new(),
            });
        if let Some(existing) = row
            .current
            .iter_mut()
            .find(|item| item.title == section.title)
        {
            *existing = section;
        } else {
            row.current.push(section);
        }
        row.touch = Some(touch.clone());
        self.write_handover(row)
            .map_err(StoreError::handover_refusal)?;
        Ok(touch)
    }

    fn keep_handover_for_flow(
        &self,
        writer: &HandoverWriter,
        section: Section,
        touch: Touch,
    ) -> Result<Touch, HandoverRefusal> {
        let _gate = self
            .handover_write_gate
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        self.ensure_live_writer(&writer.address, &writer.flow_id)?;
        let mut row = self
            .handover_row(&writer.address)
            .map_err(StoreError::handover_refusal)?
            .unwrap_or_else(|| HandoverMemory {
                address: writer.address.clone(),
                current: Vec::new(),
                touch: None,
                generation: 0,
                current_flow_id: Some(writer.flow_id.clone()),
                closed: Vec::new(),
            });
        if let Some(existing) = row
            .current
            .iter_mut()
            .find(|item| item.title == section.title)
        {
            *existing = section;
        } else {
            row.current.push(section);
        }
        row.current_flow_id = Some(writer.flow_id.clone());
        row.touch = Some(touch.clone());
        self.write_handover(row)
            .map_err(StoreError::handover_refusal)?;
        Ok(touch)
    }

    #[cfg(test)]
    fn drop_handover(
        &self,
        address: &Address,
        caller: &Address,
        title: signal_flow::Title,
        touch: Touch,
    ) -> Result<Touch, HandoverRefusal> {
        let _gate = self
            .handover_write_gate
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        ensure_owner(address, caller)?;
        let mut row = self
            .handover_row(address)
            .map_err(StoreError::handover_refusal)?
            .unwrap_or_else(|| HandoverMemory {
                address: address.clone(),
                current: Vec::new(),
                touch: None,
                generation: 0,
                current_flow_id: None,
                closed: Vec::new(),
            });
        if !row.current.iter().any(|section| section.title == title) {
            return Err(HandoverRefusal::MissingTitle);
        }
        row.current.retain(|section| section.title != title);
        row.touch = Some(touch.clone());
        self.write_handover(row)
            .map_err(StoreError::handover_refusal)?;
        Ok(touch)
    }

    fn drop_handover_for_flow(
        &self,
        writer: &HandoverWriter,
        title: signal_flow::Title,
        touch: Touch,
    ) -> Result<Touch, HandoverRefusal> {
        let _gate = self
            .handover_write_gate
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        self.ensure_live_writer(&writer.address, &writer.flow_id)?;
        let mut row = self
            .handover_row(&writer.address)
            .map_err(StoreError::handover_refusal)?
            .ok_or(HandoverRefusal::MissingTitle)?;
        if !row.current.iter().any(|section| section.title == title) {
            return Err(HandoverRefusal::MissingTitle);
        }
        row.current.retain(|section| section.title != title);
        row.current_flow_id = Some(writer.flow_id.clone());
        row.touch = Some(touch.clone());
        self.write_handover(row)
            .map_err(StoreError::handover_refusal)?;
        Ok(touch)
    }

    #[cfg(test)]
    fn close_handover_for_successor(
        &self,
        address: &Address,
        predecessor: &str,
        successor: &str,
    ) -> Result<(), StoreError> {
        let closing = self
            .handover_closing(address, predecessor, successor)
            .map_err(|_| StoreError::StateInvariant)?;
        if !closing.moves() {
            return Ok(());
        }
        let commit = closing.written(self, self.engine.begin_atomic_commit());
        self.engine.commit_atomic(commit)?;
        closing.announce(self);
        Ok(())
    }
}

trait ReadsAndWritesHandoverRows {
    fn handover_row(&self, address: &Address) -> Result<Option<HandoverMemory>, StoreError>;
    fn write_handover(&self, row: HandoverMemory) -> Result<(), StoreError>;
}

impl ReadsAndWritesHandoverRows for FlowStore {
    fn handover_row(&self, address: &Address) -> Result<Option<HandoverMemory>, StoreError> {
        let rows = self
            .engine
            .match_records(QueryPlan::key(
                self.handovers,
                RecordKey::new(Self::address_record_key(address)),
            ))?
            .records()
            .to_vec();
        match rows.as_slice() {
            [] => Ok(None),
            [row] => Ok(Some(row.clone())),
            _ => Err(StoreError::StateInvariant),
        }
    }

    fn write_handover(&self, row: HandoverMemory) -> Result<(), StoreError> {
        let key = row.record_key();
        if self.handover_row(&row.address)?.is_some() {
            self.engine
                .mutate_keyed(KeyedMutation::new(self.handovers, key, row))?;
        } else {
            self.engine.assert(Assertion::new(self.handovers, row))?;
        }
        Ok(())
    }
}

#[cfg(test)]
fn ensure_owner(address: &Address, caller: &Address) -> Result<(), HandoverRefusal> {
    if address == caller {
        Ok(())
    } else {
        Err(HandoverRefusal::NotOwner)
    }
}

trait ChecksLiveHandoverWriter {
    fn ensure_live_writer(&self, address: &Address, flow_id: &str) -> Result<(), HandoverRefusal>;
}

impl ChecksLiveHandoverWriter for FlowStore {
    fn ensure_live_writer(&self, address: &Address, flow_id: &str) -> Result<(), HandoverRefusal> {
        if let Some(row) = self
            .handover_row(address)
            .map_err(StoreError::handover_refusal)?
            && let Some(owner) = row.current_flow_id.as_deref()
            && owner != flow_id
        {
            return Err(HandoverRefusal::NotOwner);
        }
        Ok(())
    }
}

trait BecomesHandoverRefusal {
    fn handover_refusal(self) -> HandoverRefusal;
}

impl BecomesHandoverRefusal for StoreError {
    fn handover_refusal(self) -> HandoverRefusal {
        HandoverRefusal::Store(self.to_string())
    }
}

/// What the handover row's owner is, judged for a succession from
/// `predecessor` to `successor`: none, either of the two, a flow whose row
/// says it ended, or a refusal that keeps live, missing and unknown apart.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum HandoverOwnerEvidence {
    Unowned,
    Predecessor,
    Successor,
    /// The owner's row is read and says Stopped, Retired or Exited.
    Ended(String),
    /// Live (Pending or Active), Missing (no row) or Unknown (unread).
    Refused(signal_flow::HandoverOwnerRefusal),
}

/// Judges the handover row's owner from its flow row alone, so a missing
/// row and an unread one stay apart.
pub(crate) trait JudgesHandoverOwner {
    fn owner_evidence(
        &self,
        row: &HandoverMemory,
        predecessor: &str,
        successor: &str,
    ) -> HandoverOwnerEvidence;
    /// The flow row a judgement reads: an owner or a predecessor.
    fn judged_flow(&self, flow_id: &str) -> Result<Option<super::FlowRecord>, StoreError>;
    /// The lifecycle that row records.
    fn judged_lifecycle(
        &self,
        flow_id: &str,
    ) -> Result<Option<signal_flow::FlowLifecycle>, StoreError>;
}

impl JudgesHandoverOwner for FlowStore {
    fn owner_evidence(
        &self,
        row: &HandoverMemory,
        predecessor: &str,
        successor: &str,
    ) -> HandoverOwnerEvidence {
        use super::NamesLiveFlow;
        use signal_flow::HandoverOwnerRefusal;
        let Some(owner) = row.current_flow_id.as_deref() else {
            return HandoverOwnerEvidence::Unowned;
        };
        if owner == successor {
            return HandoverOwnerEvidence::Successor;
        }
        if owner == predecessor {
            return HandoverOwnerEvidence::Predecessor;
        }
        let flow_id = || flow_ethos::FlowId(owner.to_owned());
        match self.judged_flow(owner) {
            Ok(Some(flow)) if flow.lifecycle.is_live() => {
                HandoverOwnerEvidence::Refused(HandoverOwnerRefusal::Live(flow_id()))
            }
            Ok(Some(_)) => HandoverOwnerEvidence::Ended(owner.to_owned()),
            Ok(None) => HandoverOwnerEvidence::Refused(HandoverOwnerRefusal::Missing(flow_id())),
            Err(_) => HandoverOwnerEvidence::Refused(HandoverOwnerRefusal::Unknown(flow_id())),
        }
    }

    fn judged_flow(&self, flow_id: &str) -> Result<Option<super::FlowRecord>, StoreError> {
        #[cfg(test)]
        if crate::test_faults::InjectsCrashes::refused_now(
            &self.faults,
            crate::test_faults::RefusedWrite::FlowRead,
        ) {
            return Err(StoreError::StateInvariant);
        }
        super::ReadsFlowStore::flow(self, flow_id)
    }

    fn judged_lifecycle(
        &self,
        flow_id: &str,
    ) -> Result<Option<signal_flow::FlowLifecycle>, StoreError> {
        use super::FlowLifecycle;
        Ok(self.judged_flow(flow_id)?.map(|flow| match flow.lifecycle {
            FlowLifecycle::Pending => signal_flow::FlowLifecycle::Pending,
            FlowLifecycle::Active => signal_flow::FlowLifecycle::Active,
            FlowLifecycle::Stopped => signal_flow::FlowLifecycle::Stopped,
            FlowLifecycle::Retired => signal_flow::FlowLifecycle::Retired,
            FlowLifecycle::Exited => signal_flow::FlowLifecycle::Exited,
        }))
    }
}

/// The generation a succession closes, judged under the handover write
/// gate and held there until its commit is written: the row it leaves and
/// the generation it closed, or nothing when nothing moves.
pub(crate) struct HandoverClosing<'gate> {
    _gate: Option<std::sync::MutexGuard<'gate, ()>>,
    moved: Option<MovedHandover>,
}

struct MovedHandover {
    row: HandoverMemory,
    existing: bool,
    closed: ClosedHandover,
}

/// Judges and writes a generation's closing.
pub(crate) trait ClosesHandovers {
    /// The closing of `address`'s generation for `successor`, `predecessor`
    /// being the flow the Address received as before it. The closed
    /// generation names its real owner, the row's owner when it names one.
    /// Ok and empty when nothing moves; refused when the owner is live,
    /// missing or unread, or the row cannot be read.
    fn handover_closing(
        &self,
        address: &Address,
        predecessor: &str,
        successor: &str,
    ) -> Result<HandoverClosing<'_>, signal_flow::HandoverCloseRefusal>;
    /// A closing that moves nothing.
    fn nothing_closing(&self) -> HandoverClosing<'_>;
}

impl ClosesHandovers for FlowStore {
    fn handover_closing(
        &self,
        address: &Address,
        predecessor: &str,
        successor: &str,
    ) -> Result<HandoverClosing<'_>, signal_flow::HandoverCloseRefusal> {
        use signal_flow::HandoverCloseRefusal;
        let gate = self
            .handover_write_gate
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let nothing = |gate| HandoverClosing {
            _gate: Some(gate),
            moved: None,
        };
        let row = self
            .handover_row(address)
            .map_err(|_| HandoverCloseRefusal::PersistenceRefused)?;
        let Some(mut row) = row else {
            if predecessor == successor || predecessor.is_empty() {
                return Ok(nothing(gate));
            }
            let closed = ClosedHandover {
                generation: 0,
                predecessor: predecessor.into(),
                successor: successor.into(),
                sections: Vec::new(),
                touch: None,
            };
            return Ok(HandoverClosing {
                _gate: Some(gate),
                moved: Some(MovedHandover {
                    row: HandoverMemory {
                        address: address.clone(),
                        current: Vec::new(),
                        touch: None,
                        generation: 1,
                        current_flow_id: Some(successor.into()),
                        closed: vec![closed.clone()],
                    },
                    existing: false,
                    closed,
                }),
            });
        };
        let owner = match self.owner_evidence(&row, predecessor, successor) {
            HandoverOwnerEvidence::Successor => return Ok(nothing(gate)),
            HandoverOwnerEvidence::Refused(refusal) => {
                return Err(HandoverCloseRefusal::Owner(refusal));
            }
            HandoverOwnerEvidence::Unowned => predecessor.to_owned(),
            HandoverOwnerEvidence::Predecessor => predecessor.to_owned(),
            HandoverOwnerEvidence::Ended(owner) => owner,
        };
        if owner == successor || owner.is_empty() {
            return Ok(nothing(gate));
        }
        let sections = row.current.clone();
        let touch = row.touch.take();
        let closed = ClosedHandover {
            generation: row.generation,
            predecessor: owner,
            successor: successor.into(),
            sections: sections.clone(),
            touch,
        };
        row.closed.push(closed.clone());
        row.current = sections;
        row.touch = None;
        row.current_flow_id = Some(successor.into());
        row.generation += 1;
        Ok(HandoverClosing {
            _gate: Some(gate),
            moved: Some(MovedHandover {
                row,
                existing: true,
                closed,
            }),
        })
    }

    fn nothing_closing(&self) -> HandoverClosing<'_> {
        HandoverClosing {
            _gate: None,
            moved: None,
        }
    }
}

/// What a closing adds to its commit, and announces after it.
pub(crate) trait WritesHandoverClosing {
    /// Whether it closes a generation.
    #[cfg(test)]
    fn moves(&self) -> bool;
    fn written(&self, store: &FlowStore, commit: AtomicCommit) -> AtomicCommit;
    /// The generation closed and the reminder change, after the commit.
    fn announce(&self, store: &FlowStore);
}

impl WritesHandoverClosing for HandoverClosing<'_> {
    #[cfg(test)]
    fn moves(&self) -> bool {
        self.moved.is_some()
    }

    fn written(&self, store: &FlowStore, commit: AtomicCommit) -> AtomicCommit {
        match &self.moved {
            Some(moved) if moved.existing => commit.mutate(store.handovers, moved.row.clone()),
            Some(moved) => commit.assert(store.handovers, moved.row.clone()),
            None => commit,
        }
    }

    fn announce(&self, store: &FlowStore) {
        if let Some(moved) = &self.moved {
            store
                .handover_observations
                .change(RetirementChange::GenerationClosed(
                    moved.closed.generation_key(&moved.row.address),
                ));
            store.reminder_changes.announce();
        }
    }
}

/// The supersession a launch's settlement adds to its commit: every
/// earlier obligation at the Address whose handover this launch takes over.
pub(crate) trait SupersedesObligations {
    /// A PendingHandover at `address` naming another successor than `by`
    /// is retracted, its launch's outcome rewritten `HandoverSuperseded`;
    /// with `keeps_row` the row is left for the caller to replace in the
    /// same commit. A PendingReap at `address` whose standing is
    /// `HandoverRefused` and whose successor is not `by` is retracted, its
    /// launch's outcome rewritten `Replaced`. Any other PendingReap stays.
    fn superseding(
        &self,
        address: &Address,
        by: &signal_flow::FlowId,
        commit: AtomicCommit,
        keeps_row: bool,
    ) -> Result<AtomicCommit, StoreError>;
}

impl SupersedesObligations for FlowStore {
    fn superseding(
        &self,
        address: &Address,
        by: &signal_flow::FlowId,
        commit: AtomicCommit,
        keeps_row: bool,
    ) -> Result<AtomicCommit, StoreError> {
        use super::pending_handover::RecordsPendingHandovers;
        use super::pending_reap::{RecordsPendingReaps, WritesPendingReap};
        use super::{LaunchOutcome, RecordsLaunchOutcome, WritesLaunchOutcome};
        let mut commit = commit;
        let mut superseded = false;
        if let Some(row) = self.pending_handover(address)?
            && row.successor.0 != *by
        {
            superseded = true;
            if let Some(
                LaunchOutcome::HandoverPending(launched) | LaunchOutcome::Started(launched),
            ) = self.launch_outcome(&row.launch_request_id.0)?
            {
                commit = self.outcome_written(
                    &row.launch_request_id.0,
                    LaunchOutcome::HandoverSuperseded(
                        crate::generated::memory::SupersededHandover {
                            address: address.clone(),
                            handover_owner: row.handover_owner.clone(),
                            launched_record: launched,
                            successor: signal_flow::Successor(by.clone()),
                        },
                    ),
                    commit,
                )?;
            }
            if !keeps_row {
                commit = commit.retract(
                    self.pending_handovers,
                    RecordKey::new(Self::address_record_key(address)),
                );
            }
        }
        for row in self.pending_reaps_at(address)? {
            if !matches!(
                row.reap_standing,
                signal_flow::ReapStanding::HandoverRefused(_)
            ) {
                continue;
            }
            let Some(replaced) = self.reaped_record(&row)? else {
                continue;
            };
            if replaced.launched_record.flow_id == *by {
                continue;
            }
            superseded = true;
            commit = self.outcome_written(
                &row.launch_request_id.0,
                LaunchOutcome::Replaced(replaced),
                commit,
            )?;
            commit = commit.retract(
                self.pending_reaps,
                RecordKey::new(row.predecessor.0.clone()),
            );
        }
        #[cfg(test)]
        if superseded {
            crate::test_faults::InjectsCrashes::attempted(&self.faults, "Superseding");
        }
        let _ = superseded;
        Ok(commit)
    }
}
