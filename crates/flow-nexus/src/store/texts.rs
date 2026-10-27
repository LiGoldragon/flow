//! The singleton authored text configuration family.

use super::StoreError;
use datom_codec::{Actualizing, Budget, Potential};
use meta_signal_flow::Texts;
use protos::ReaderBudget;
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use sema_engine::{
    Assertion, Engine, EngineRecord, FamilyName, MemoryDefinition, MemoryVersion, QueryPlan,
    RecordKey, SchemaHash, TableDescriptor, TableName, TableReference,
};

pub(super) const TEXTS_TABLE_NAME: TableName =
    TableName::new("flow_nexus_meta_texts_configuration");

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct StoredTexts {
    pub(crate) texts: Texts,
}

impl EngineRecord for StoredTexts {
    fn record_key(&self) -> RecordKey {
        RecordKey::new("texts")
    }
}

impl MemoryDefinition for StoredTexts {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(1)
    }
    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label("flow-nexus-meta-texts-configuration-v1")
    }
}

fn budget() -> Budget {
    Budget {
        remaining: 65_536,
        reader: ReaderBudget { remaining: 65_536 },
        depth: 0,
        maximum_depth: 1_024,
    }
}

pub(crate) fn seed_texts() -> Result<Texts, StoreError> {
    Potential::<Texts>::from(include_str!("../../seed/texts.datom"))
        .actualize(&mut budget())
        .map_err(|error| StoreError::ConfigurationSeed(format!("invalid texts seed: {error:?}")))
}

pub(crate) trait RegistersTextsTable {
    fn register_texts(&mut self) -> Result<TableReference<StoredTexts>, StoreError>;
}

impl RegistersTextsTable for Engine {
    fn register_texts(&mut self) -> Result<TableReference<StoredTexts>, StoreError> {
        let table = self.register_table(texts_descriptor())?;
        if self
            .match_records(QueryPlan::all(table))?
            .records()
            .is_empty()
        {
            self.assert(Assertion::new(
                table,
                StoredTexts {
                    texts: seed_texts()?,
                },
            ))?;
        }
        Ok(table)
    }
}

pub(crate) fn texts_descriptor() -> TableDescriptor<StoredTexts> {
    TableDescriptor::new(
        TEXTS_TABLE_NAME,
        FamilyName::new("flow-nexus-meta-texts-configuration"),
        StoredTexts::schema_hash(),
    )
}
