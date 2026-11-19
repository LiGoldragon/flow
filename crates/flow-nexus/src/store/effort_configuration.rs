//! Effort is stored per model and per permaflow. The first model records
//! preserve the behavior of stores written before effort was configurable.
use super::metaflow_declaration::ReadsMetaflowDeclarations;
use super::{FlowStore, ReadsFlowMemory, StoreError};
pub(crate) use crate::generated::memory::StoredModelEffort;
use meta_signal_flow::{Effort, ModelEffort};
use sema_engine::{
    Assertion, Engine, EngineRecord, FamilyName, MemoryDefinition, MemoryVersion, QueryPlan,
    RecordKey, SchemaHash, TableDescriptor, TableName, TableReference,
};
use signal_flow::{Address, Model};

pub(crate) const TABLE: &str = "flow_nexus_model_effort";
pub(crate) const FAMILY: &str = "flow-nexus-model-effort";
pub(crate) const SCHEMA: &str = "flow-nexus-model-effort-v1";

impl EngineRecord for StoredModelEffort {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(FlowStore::model_record_key(&self.model_effort.model))
    }
}
impl MemoryDefinition for StoredModelEffort {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(1)
    }
    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label(SCHEMA)
    }
}
pub(crate) trait RegistersModelEfforts {
    fn register(engine: &mut Engine) -> Result<TableReference<StoredModelEffort>, StoreError>;
}
impl RegistersModelEfforts for StoredModelEffort {
    fn register(engine: &mut Engine) -> Result<TableReference<Self>, StoreError> {
        let table = engine.register_table(TableDescriptor::new(
            TableName::new(TABLE),
            FamilyName::new(FAMILY),
            Self::schema_hash(),
        ))?;
        // Complete missing rows individually, so an interrupted initial upgrade
        // can resume without replacing any value already configured.
        for model in [
            Model::Fable,
            Model::Opus,
            Model::Sonnet,
            Model::Haiku,
            Model::Astra,
            Model::Sol,
            Model::Luna,
            Model::Terra,
        ] {
            let key = RecordKey::new(FlowStore::model_record_key(&model));
            if engine
                .match_records(QueryPlan::key(table, key))?
                .records()
                .is_empty()
            {
                let effort = match model {
                    Model::Terra => Effort::Low,
                    _ => Effort::Medium,
                };
                engine.assert(Assertion::new(
                    table,
                    Self {
                        model_effort: ModelEffort { model, effort },
                    },
                ))?;
            }
        }
        Ok(table)
    }
}

/// The existing launch wire carries native effort text; empty text means
/// the harness chooses its own default and its setting is omitted.
pub(crate) trait NativeEffort {
    fn native_effort(&self) -> &'static str;
}
impl NativeEffort for Effort {
    fn native_effort(&self) -> &'static str {
        match self {
            Self::Default => "",
            Self::High => "high",
            Self::Medium => "medium",
            Self::Low => "low",
        }
    }
}
pub(crate) trait ReadsEffort {
    fn configured_effort(&self, address: &Address) -> Result<String, StoreError>;
}
impl ReadsEffort for FlowStore {
    fn configured_effort(&self, address: &Address) -> Result<String, StoreError> {
        let declared = self
            .metaflow_declaration(address)?
            .map(|value| value.effort)
            .unwrap_or(Effort::Default);
        let effort = match declared {
            Effort::Default => {
                let rows = self.engine.match_records(QueryPlan::key(
                    self.meta_configuration_tables.model_efforts,
                    RecordKey::new(Self::model_record_key(&address.model)),
                ))?;
                match rows.records() {
                    [row] => row.model_effort.effort.clone(),
                    _ => return Err(StoreError::StateInvariant),
                }
            }
            effort => effort,
        };
        Ok(effort.native_effort().to_owned())
    }
}
