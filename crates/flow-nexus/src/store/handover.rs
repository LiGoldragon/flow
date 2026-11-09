//! Durable current and closed handover generations.

use super::AnnouncesLaunchChanges;
use super::context::NamesFlowModel;
use super::handover_configuration::ReadsHandoverConfiguration;
use super::{FlowStore, ReadsFlowMemory, StoreError};
use crate::handover_observation::{ObservesHandoverRetirement, RetirementSubscription};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use sema_engine::{
    Assertion, EngineRecord, FamilyName, KeyedMutation, MemoryDefinition, MemoryVersion, QueryPlan,
    RecordKey, SchemaHash, TableDescriptor, TableName, TableReference,
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
            handover_generation: self.generation,
            predecessor: self.predecessor.clone(),
            successor: self.successor.clone(),
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
            .find(|policy| policy.model_name == model_name)
        else {
            return Ok(false);
        };
        let Some(row) = self.handover_row(address)? else {
            return Ok(false);
        };
        let Some(signal_flow::Touch::Known(touch)) = row.touch else {
            return Ok(false);
        };
        if touch.flow_id != flow_id {
            return Ok(false);
        }
        let left = i128::from(100 - policy.warning_share_left);
        Ok(i128::from(touch.tokens) * 100 >= i128::from(refresh) * left)
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

    fn close_handover_for_successor(
        &self,
        address: &Address,
        predecessor: &str,
        successor: &str,
    ) -> Result<(), StoreError> {
        let _gate = self
            .handover_write_gate
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let Some(mut row) = self.handover_row(address)? else {
            if predecessor == successor {
                return Ok(());
            }
            let closed = ClosedHandover {
                generation: 0,
                predecessor: predecessor.into(),
                successor: successor.into(),
                sections: Vec::new(),
                touch: None,
            };
            self.write_handover(HandoverMemory {
                address: address.clone(),
                current: Vec::new(),
                touch: None,
                generation: 1,
                current_flow_id: Some(successor.into()),
                closed: vec![closed.clone()],
            })?;
            self.handover_observations
                .change(RetirementChange::GenerationClosed(
                    closed.generation_key(address),
                ));
            self.reminder_changes.announce();
            return Ok(());
        };
        if predecessor == successor {
            return Ok(());
        }
        if row.current_flow_id.as_deref() == Some(successor) {
            return Ok(());
        }
        if let Some(owner) = row.current_flow_id.as_deref()
            && owner != predecessor
        {
            return Err(StoreError::StateInvariant);
        }
        let sections = row.current.clone();
        let touch = row.touch.take();
        let closed = ClosedHandover {
            generation: row.generation,
            predecessor: predecessor.into(),
            successor: successor.into(),
            sections: sections.clone(),
            touch,
        };
        row.closed.push(closed.clone());
        row.current = sections;
        row.touch = None;
        row.current_flow_id = Some(successor.into());
        row.generation += 1;
        self.write_handover(row)?;
        self.handover_observations
            .change(RetirementChange::GenerationClosed(
                closed.generation_key(address),
            ));
        self.reminder_changes.announce();
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
