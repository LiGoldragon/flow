//! The singleton Timings and Limits configuration families. Their first
//! committed values are authored as Datom under `seed/`; after that every
//! value comes from the store's committed configuration snapshot.

use super::StoreError;
use datom_codec::{Actualizing, Budget, Potential};
use meta_signal_flow::{Limits, Timings};
use protos::ReaderBudget;
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use sema_engine::{
    Assertion, Engine, EngineRecord, FamilyName, MemoryDefinition, MemoryVersion, QueryPlan,
    RecordKey, SchemaHash, TableDescriptor, TableName, TableReference,
};

pub(super) const TIMINGS_TABLE_NAME: TableName =
    TableName::new("flow_nexus_meta_timings_configuration");
pub(super) const LIMITS_TABLE_NAME: TableName =
    TableName::new("flow_nexus_meta_limits_configuration");

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct StoredTimings {
    pub(crate) timings: Timings,
}

impl EngineRecord for StoredTimings {
    fn record_key(&self) -> RecordKey {
        RecordKey::new("timings")
    }
}
impl MemoryDefinition for StoredTimings {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(1)
    }
    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label("flow-nexus-meta-timings-configuration-v1")
    }
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct StoredLimits {
    pub(crate) limits: Limits,
}
impl EngineRecord for StoredLimits {
    fn record_key(&self) -> RecordKey {
        RecordKey::new("limits")
    }
}
impl MemoryDefinition for StoredLimits {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(1)
    }
    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label("flow-nexus-meta-limits-configuration-v1")
    }
}

pub(crate) trait RegistersTimingAndLimitTables {
    fn register_timings(&mut self) -> Result<TableReference<StoredTimings>, StoreError>;
    fn register_limits(&mut self) -> Result<TableReference<StoredLimits>, StoreError>;
}

fn budget() -> Budget {
    Budget {
        remaining: 65_536,
        reader: ReaderBudget { remaining: 65_536 },
        depth: 0,
        maximum_depth: 1_024,
    }
}

pub(crate) fn seed_timings() -> Result<Timings, StoreError> {
    Potential::<Timings>::from(include_str!("../../seed/timings.datom"))
        .actualize(&mut budget())
        .map_err(|error| StoreError::ConfigurationSeed(format!("invalid timings seed: {error:?}")))
}

pub(crate) fn seed_limits() -> Result<Limits, StoreError> {
    Potential::<Limits>::from(include_str!("../../seed/limits.datom"))
        .actualize(&mut budget())
        .map_err(|error| StoreError::ConfigurationSeed(format!("invalid limits seed: {error:?}")))
}

impl RegistersTimingAndLimitTables for Engine {
    fn register_timings(&mut self) -> Result<TableReference<StoredTimings>, StoreError> {
        let table = self.register_table(TableDescriptor::new(
            TIMINGS_TABLE_NAME,
            FamilyName::new("flow-nexus-meta-timings-configuration"),
            StoredTimings::schema_hash(),
        ))?;
        if self
            .match_records(QueryPlan::all(table))?
            .records()
            .is_empty()
        {
            let timings = seed_timings()?;
            self.assert(Assertion::new(table, StoredTimings { timings }))?;
        }
        Ok(table)
    }

    fn register_limits(&mut self) -> Result<TableReference<StoredLimits>, StoreError> {
        let table = self.register_table(TableDescriptor::new(
            LIMITS_TABLE_NAME,
            FamilyName::new("flow-nexus-meta-limits-configuration"),
            StoredLimits::schema_hash(),
        ))?;
        if self
            .match_records(QueryPlan::all(table))?
            .records()
            .is_empty()
        {
            let limits = seed_limits()?;
            self.assert(Assertion::new(table, StoredLimits { limits }))?;
        }
        Ok(table)
    }
}

pub(crate) fn timings_descriptor() -> TableDescriptor<StoredTimings> {
    TableDescriptor::new(
        TIMINGS_TABLE_NAME,
        FamilyName::new("flow-nexus-meta-timings-configuration"),
        StoredTimings::schema_hash(),
    )
}

pub(crate) fn limits_descriptor() -> TableDescriptor<StoredLimits> {
    TableDescriptor::new(
        LIMITS_TABLE_NAME,
        FamilyName::new("flow-nexus-meta-limits-configuration"),
        StoredLimits::schema_hash(),
    )
}
