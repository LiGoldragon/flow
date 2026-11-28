//! Effort is stored per model and per permaflow, each Default or the
//! harness's own spelling. Flow holds no list of levels: a spelling is
//! carried to the launch as written, and the harness that refuses it says
//! so there.
use super::effort_upgrade::{EffortV1, SpellsNativeEffort};
use super::metaflow_declaration::ReadsMetaflowDeclarations;
use super::{FlowStore, ReadsFlowMemory, StoreError};
pub(crate) use crate::generated::memory::StoredModelEffort;
use meta_signal_flow::{Effort, ModelEffort};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use sema_engine::{
    Decision, Engine, EngineRecord, FamilyName, MemoryDefinition, MemoryUpgrades, MemoryVersion,
    QueryPlan, RecordKey, SchemaHash, TableDescriptor, TableName, TableReference, UpgradeFrom,
    UpgradeOutcome,
};
use signal_flow::{Address, Model};

pub(crate) const TABLE: &str = "flow_nexus_model_effort";
pub(crate) const FAMILY: &str = "flow-nexus-model-effort";
pub(crate) const SCHEMA: &str = "flow-nexus-model-effort-v2";
/// The model efforts 0.60.2 through 0.60.7 wrote, each a closed level.
pub(crate) const SCHEMA_V1: &str = "flow-nexus-model-effort-v1";

impl EngineRecord for StoredModelEffort {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(FlowStore::model_record_key(&self.model_effort.model))
    }
}
impl MemoryDefinition for StoredModelEffort {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(2)
    }
    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label(SCHEMA)
    }
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct ModelEffortV1 {
    pub model: Model,
    pub effort: EffortV1,
}
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct StoredModelEffortV1 {
    pub model_effort: ModelEffortV1,
}
impl EngineRecord for StoredModelEffortV1 {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(FlowStore::model_record_key(&self.model_effort.model))
    }
}
impl MemoryDefinition for StoredModelEffortV1 {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(1)
    }
    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label(SCHEMA_V1)
    }
}
impl UpgradeFrom<StoredModelEffortV1> for StoredModelEffort {
    type Refusal = std::convert::Infallible;
    fn upgrade_from(before: StoredModelEffortV1) -> Decision<Self, Self::Refusal> {
        Decision::Ready(Self {
            model_effort: ModelEffort {
                model: before.model_effort.model,
                effort: before.model_effort.effort.native_effort(),
            },
        })
    }
}

pub(crate) trait RegistersModelEfforts {
    fn register(engine: &mut Engine) -> Result<TableReference<StoredModelEffort>, StoreError>;
}
impl RegistersModelEfforts for StoredModelEffort {
    /// The table, its closed levels of 0.60.2 upgraded to their spellings.
    /// A model with no row is Default: its launches omit the argument.
    fn register(engine: &mut Engine) -> Result<TableReference<Self>, StoreError> {
        let descriptor = TableDescriptor::new(
            TableName::new(TABLE),
            FamilyName::new(FAMILY),
            Self::schema_hash(),
        )
        .with_predecessor::<StoredModelEffortV1>();
        let stored_v1 = engine.list_tables().iter().any(|table| {
            table.identity().table_name() == TABLE
                && table.identity().schema_hash() == StoredModelEffortV1::schema_hash()
        });
        if !stored_v1 {
            return Ok(engine.register_table(descriptor)?);
        }
        match engine.upgrade_table::<StoredModelEffortV1, Self>(descriptor) {
            UpgradeOutcome::Upgraded(receipt) => Ok(receipt.table()),
            UpgradeOutcome::AlreadyCurrent(table) => Ok(table),
            UpgradeOutcome::Refused(reason) => match *reason.reason() {},
            UpgradeOutcome::Failed(reason) => Err(StoreError::EffortUpgrade(format!("{reason:?}"))),
        }
    }
}

pub(crate) trait ReadsEffort {
    /// The effort a launch at `address` passes: its permaflow's, and where
    /// that is Default its model's. Default omits the argument.
    fn configured_effort(&self, address: &Address) -> Result<Effort, StoreError>;
    /// The spelling the launch wire carries: the configured spelling, as
    /// written, or for Default the empty spelling that omits the argument.
    fn launch_effort(&self, address: &Address) -> Result<String, StoreError> {
        Ok(match self.configured_effort(address)? {
            Effort::Native(spelling) => spelling,
            Effort::Default => String::new(),
        })
    }
}
impl ReadsEffort for FlowStore {
    fn configured_effort(&self, address: &Address) -> Result<Effort, StoreError> {
        if let Some(Effort::Native(spelling)) = self
            .metaflow_declaration(address)?
            .map(|declaration| declaration.effort)
        {
            return Ok(Effort::Native(spelling));
        }
        let rows = self.engine.match_records(QueryPlan::key(
            self.meta_configuration_tables.model_efforts,
            RecordKey::new(Self::model_record_key(&address.model)),
        ))?;
        match rows.records() {
            [] => Ok(Effort::Default),
            [row] => Ok(row.model_effort.effort.clone()),
            _ => Err(StoreError::StateInvariant),
        }
    }
}
