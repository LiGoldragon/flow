//! Durable current and closed handover generations.

use super::{FlowStore, StoreError};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use sema_engine::{
    Assertion, EngineRecord, FamilyName, KeyedMutation, MemoryDefinition, MemoryVersion, QueryPlan,
    RecordKey, SchemaHash, TableDescriptor, TableName, TableReference,
};
use signal_flow::{Address, HandoverRefusal, Section, Touch};

pub(super) const HANDOVER_TABLE_NAME: TableName = TableName::new("flow_nexus_handovers");
pub(super) const HANDOVER_FAMILY: &str = "flow-nexus-handover";
pub(super) const HANDOVER_SCHEMA: &str = "flow-nexus-handover-v1";

/// One address's live handover and its closed generations awaiting archive.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub struct HandoverMemory {
    pub address: Address,
    pub current: Vec<Section>,
    pub touch: Touch,
    pub generation: i64,
    pub closed: Vec<ClosedHandover>,
}

/// A predecessor's immutable handover after its successor starts.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub struct ClosedHandover {
    pub generation: i64,
    pub predecessor: String,
    pub sections: Vec<Section>,
    pub touch: Touch,
}

impl EngineRecord for HandoverMemory {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(FlowStore::address_record_key(&self.address))
    }
}

impl MemoryDefinition for HandoverMemory {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(1)
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
    fn keep_handover(
        &self,
        address: &Address,
        caller: &Address,
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
    fn close_handover_for_successor(
        &self,
        address: &Address,
        predecessor: &str,
    ) -> Result<(), StoreError>;
    fn handover_headings(&self, address: &Address)
        -> Result<Vec<signal_flow::Heading>, StoreError>;
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
                touch: Touch::Unknown,
                generation: 0,
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
        row.touch = touch.clone();
        self.write_handover(row).map_err(store_refusal)?;
        Ok(touch)
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
                touch: Touch::Unknown,
                generation: 0,
                closed: Vec::new(),
            });
        if !row.current.iter().any(|section| section.title == title) {
            return Err(HandoverRefusal::MissingTitle);
        }
        row.current.retain(|section| section.title != title);
        row.touch = touch.clone();
        self.write_handover(row).map_err(store_refusal)?;
        Ok(touch)
    }

    fn close_handover_for_successor(
        &self,
        address: &Address,
        predecessor: &str,
    ) -> Result<(), StoreError> {
        let Some(mut row) = self.handover_row(address)? else {
            return Ok(());
        };
        // The successor inherits the predecessor's content, while the
        // predecessor snapshot remains immutable for archival.  Clone here
        // rather than sharing any mutable representation across generations.
        let sections = row.current.clone();
        let touch = std::mem::replace(&mut row.touch, Touch::Unknown);
        if !sections.is_empty() {
            row.closed.push(ClosedHandover {
                generation: row.generation,
                predecessor: predecessor.into(),
                sections: sections.clone(),
                touch,
            });
        }
        row.current = sections;
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

fn store_refusal(error: StoreError) -> HandoverRefusal {
    HandoverRefusal::Store(error.to_string())
}
