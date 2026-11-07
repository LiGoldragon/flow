//! Durable current and closed handover generations.

use super::context::{NamesFlowModel, RecordsContextSize};
use super::handover_configuration::ReadsHandoverConfiguration;
use super::metaflow::SleepsMetaflows;
use super::{FlowStore, ManagesMetaflowState, ReadsFlowMemory, StoreError};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use sema_engine::{
    Assertion, EngineRecord, FamilyName, KeyedMutation, MemoryDefinition, MemoryVersion, QueryPlan,
    RecordKey, SchemaHash, TableDescriptor, TableName, TableReference,
};
use signal_flow::{Address, HandoverRefusal, Section, Touch};

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
    /// Reads the complete Memory row. `None` means no handover has ever been
    /// written; `Some(Unknown)` means a successful write had no measurement.
    fn handover_memory(&self, address: &Address) -> Result<Option<HandoverMemory>, StoreError>;
    fn handover(&self, address: &Address) -> Result<Vec<Section>, StoreError>;
    fn handover_for_flow(
        &self,
        address: &Address,
        flow_id: &str,
    ) -> Result<Option<Vec<Section>>, StoreError>;
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
    fn handover_memory(&self, address: &Address) -> Result<Option<HandoverMemory>, StoreError> {
        self.handover_row(address)
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

    fn keep_handover(
        &self,
        address: &Address,
        caller: &Address,
        section: Section,
        touch: Touch,
    ) -> Result<Touch, HandoverRefusal> {
        ensure_owner(address, caller)?;
        let mut row = self
            .handover_row(address)
            .map_err(store_refusal)?
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
        self.write_handover(row).map_err(store_refusal)?;
        Ok(touch)
    }

    fn keep_handover_for_flow(
        &self,
        writer: &HandoverWriter,
        section: Section,
        touch: Touch,
    ) -> Result<Touch, HandoverRefusal> {
        ensure_live_writer(self, &writer.address, &writer.flow_id)?;
        let result = self.keep_handover(&writer.address, &writer.address, section, touch)?;
        let mut row = self
            .handover_row(&writer.address)
            .map_err(store_refusal)?
            .ok_or_else(|| HandoverRefusal::Store("handover row disappeared".into()))?;
        row.current_flow_id = Some(writer.flow_id.clone());
        self.write_handover(row).map_err(store_refusal)?;
        Ok(result)
    }

    fn drop_handover(
        &self,
        address: &Address,
        caller: &Address,
        title: signal_flow::Title,
        touch: Touch,
    ) -> Result<Touch, HandoverRefusal> {
        ensure_owner(address, caller)?;
        let mut row = self
            .handover_row(address)
            .map_err(store_refusal)?
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
        self.write_handover(row).map_err(store_refusal)?;
        Ok(touch)
    }

    fn drop_handover_for_flow(
        &self,
        writer: &HandoverWriter,
        title: signal_flow::Title,
        touch: Touch,
    ) -> Result<Touch, HandoverRefusal> {
        ensure_live_writer(self, &writer.address, &writer.flow_id)?;
        let result = self.drop_handover(&writer.address, &writer.address, title, touch)?;
        let mut row = self
            .handover_row(&writer.address)
            .map_err(store_refusal)?
            .ok_or_else(|| HandoverRefusal::Store("handover row disappeared".into()))?;
        row.current_flow_id = Some(writer.flow_id.clone());
        self.write_handover(row).map_err(store_refusal)?;
        Ok(result)
    }

    fn close_handover_for_successor(
        &self,
        address: &Address,
        predecessor: &str,
        successor: &str,
    ) -> Result<(), StoreError> {
        let Some(mut row) = self.handover_row(address)? else {
            if predecessor == successor {
                return Ok(());
            }
            return self.write_handover(HandoverMemory {
                address: address.clone(),
                current: Vec::new(),
                touch: None,
                generation: 1,
                current_flow_id: Some(successor.into()),
                closed: vec![ClosedHandover {
                    generation: 0,
                    predecessor: predecessor.into(),
                    successor: successor.into(),
                    sections: Vec::new(),
                    touch: None,
                }],
            });
        };
        if predecessor == successor {
            return Ok(());
        }
        if row.current_flow_id.as_deref() == Some(successor) {
            return Ok(());
        }
        let sections = row.current.clone();
        let touch = row.touch.take();
        row.closed.push(ClosedHandover {
            generation: row.generation,
            predecessor: predecessor.into(),
            successor: successor.into(),
            sections: sections.clone(),
            touch,
        });
        row.current = sections;
        row.touch = None;
        row.current_flow_id = Some(successor.into());
        row.generation += 1;
        self.write_handover(row)
    }
}

impl FlowStore {
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

fn ensure_owner(address: &Address, caller: &Address) -> Result<(), HandoverRefusal> {
    if address == caller {
        Ok(())
    } else {
        Err(HandoverRefusal::NotOwner)
    }
}

fn ensure_live_writer(
    store: &FlowStore,
    address: &Address,
    flow_id: &str,
) -> Result<(), HandoverRefusal> {
    if let Some(row) = store.handover_row(address).map_err(store_refusal)? {
        if let Some(owner) = row.current_flow_id.as_deref() {
            if owner != flow_id {
                return Err(HandoverRefusal::NotOwner);
            }
        }
    }
    Ok(())
}

fn store_refusal(error: StoreError) -> HandoverRefusal {
    HandoverRefusal::Store(error.to_string())
}
