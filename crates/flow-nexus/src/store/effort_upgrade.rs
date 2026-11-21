//! The exact permaflow record written through 0.60.1, before effort.
use super::metaflow_declaration::{
    DescribesMetaflowDeclarationTable, StoredMetaMetaflowConfiguration,
};
use super::{FlowStore, ReadsFlowMemory, StoreError};
use meta_signal_flow::{
    ConfigureMetaflow, Continuity, Effort, Purpose, Records, RefreshMode, Root,
};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use sema_engine::{
    Decision, Engine, EngineRecord, MemoryDefinition, MemoryUpgrades, MemoryVersion, RecordKey,
    SchemaHash, UpgradeFrom, UpgradeOutcome,
};
use signal_flow::{Address, FlowKind, LeaveOuts, PromptModules, SystemModules, ToolSet};

pub(crate) const SCHEMA_V7: &str = "flow-nexus-meta-metaflow-configuration-v7";
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct ConfigureMetaflowV7 {
    pub address: Address,
    pub root_option: Option<Root>,
    pub leave_outs: LeaveOuts,
    pub system_modules: SystemModules,
    pub prompt_modules: PromptModules,
    pub flow_kind: FlowKind,
    pub tool_set: ToolSet,
    pub continuity: Continuity,
    pub purpose: Purpose,
    pub records: Records,
}
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct StoredMetaMetaflowConfigurationV7 {
    pub configure_metaflow: ConfigureMetaflowV7,
}
impl EngineRecord for StoredMetaMetaflowConfigurationV7 {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(FlowStore::address_record_key(
            &self.configure_metaflow.address,
        ))
    }
}
impl MemoryDefinition for StoredMetaMetaflowConfigurationV7 {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(7)
    }
    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label(SCHEMA_V7)
    }
}
/// The pre-RefreshMode declaration archives exactly as meta 0.91 wrote it.
pub(crate) const SCHEMA_V8: &str = "flow-nexus-meta-metaflow-configuration-v8";
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct ConfigureMetaflowV8 {
    pub address: Address,
    pub root_option: Option<Root>,
    pub leave_outs: LeaveOuts,
    pub system_modules: SystemModules,
    pub prompt_modules: PromptModules,
    pub flow_kind: FlowKind,
    pub tool_set: ToolSet,
    pub continuity: Continuity,
    pub purpose: Purpose,
    pub records: Records,
    pub effort: Effort,
}
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct StoredMetaMetaflowConfigurationV8 {
    pub configure_metaflow: ConfigureMetaflowV8,
}
impl EngineRecord for StoredMetaMetaflowConfigurationV8 {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(FlowStore::address_record_key(
            &self.configure_metaflow.address,
        ))
    }
}
impl MemoryDefinition for StoredMetaMetaflowConfigurationV8 {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(8)
    }
    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label(SCHEMA_V8)
    }
}
impl UpgradeFrom<StoredMetaMetaflowConfigurationV8> for StoredMetaMetaflowConfiguration {
    type Refusal = std::convert::Infallible;
    fn upgrade_from(before: StoredMetaMetaflowConfigurationV8) -> Decision<Self, Self::Refusal> {
        let before = before.configure_metaflow;
        Decision::Ready(Self {
            configure_metaflow: ConfigureMetaflow {
                address: before.address,
                root_option: before.root_option,
                leave_outs: before.leave_outs,
                system_modules: before.system_modules,
                prompt_modules: before.prompt_modules,
                flow_kind: before.flow_kind,
                tool_set: before.tool_set,
                continuity: before.continuity,
                refresh_mode: RefreshMode::Automatic,
                purpose: before.purpose,
                records: before.records,
                effort: before.effort,
            },
        })
    }
}
impl UpgradeFrom<StoredMetaMetaflowConfigurationV7> for StoredMetaMetaflowConfiguration {
    type Refusal = std::convert::Infallible;
    fn upgrade_from(before: StoredMetaMetaflowConfigurationV7) -> Decision<Self, Self::Refusal> {
        let before = before.configure_metaflow;
        Decision::Ready(Self {
            configure_metaflow: ConfigureMetaflow {
                address: before.address,
                root_option: before.root_option,
                leave_outs: before.leave_outs,
                system_modules: before.system_modules,
                prompt_modules: before.prompt_modules,
                flow_kind: before.flow_kind,
                tool_set: before.tool_set,
                continuity: before.continuity,
                refresh_mode: RefreshMode::Automatic,
                purpose: before.purpose,
                records: before.records,
                effort: Effort::Default,
            },
        })
    }
}
pub(crate) trait UpgradesEffort {
    fn upgrade_effort(engine: &mut Engine) -> Result<(), StoreError>;
}
impl UpgradesEffort for FlowStore {
    fn upgrade_effort(engine: &mut Engine) -> Result<(), StoreError> {
        let previous = engine.list_tables().iter().any(|table| {
            table.identity().table_name()
                == super::metaflow_declaration::META_METAFLOW_CONFIGURATION_TABLE_NAME.as_str()
                && table.identity().schema_hash()
                    == StoredMetaMetaflowConfigurationV7::schema_hash()
        });
        if previous {
            match engine.upgrade_table::<StoredMetaMetaflowConfigurationV7, StoredMetaMetaflowConfiguration>(StoredMetaMetaflowConfiguration::descriptor()) {
                UpgradeOutcome::Upgraded(_) | UpgradeOutcome::AlreadyCurrent(_) => {}
                UpgradeOutcome::Refused(reason) => return Err(StoreError::EffortUpgrade(format!("{reason:?}"))),
                UpgradeOutcome::Failed(reason) => return Err(StoreError::EffortUpgrade(format!("{reason:?}"))),
            }
        }
        let before_refresh_mode = engine.list_tables().iter().any(|table| {
            table.identity().table_name()
                == super::metaflow_declaration::META_METAFLOW_CONFIGURATION_TABLE_NAME.as_str()
                && table.identity().schema_hash()
                    == StoredMetaMetaflowConfigurationV8::schema_hash()
        });
        if before_refresh_mode {
            match engine.upgrade_table::<StoredMetaMetaflowConfigurationV8, StoredMetaMetaflowConfiguration>(StoredMetaMetaflowConfiguration::descriptor()) {
                UpgradeOutcome::Upgraded(_) | UpgradeOutcome::AlreadyCurrent(_) => {}
                UpgradeOutcome::Refused(reason) => match *reason.reason() {},
                UpgradeOutcome::Failed(reason) => return Err(StoreError::RefreshModeUpgrade(format!("{reason:?}"))),
            }
        }
        Ok(())
    }
}
