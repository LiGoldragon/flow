//! Context-warning configuration and per-flow stage state. Logical admission
//! appends progress to the same atomic commit as its actual queued notice.

use super::*;
use crate::context_warnings::JudgesContextWarnings;
pub(crate) use crate::generated::memory::{
    ContextWarningProgress, StoredContextWarningPolicy, StoredHarnessHookEvents,
};
use meta_signal_flow::{ContextWarningPolicy, HarnessHookEvent};

pub(super) const POLICY_TABLE: &str = "flow_nexus_context_warning_policies";
pub(super) const POLICY_FAMILY: &str = "flow-nexus-context-warning-policies";
pub(super) const POLICY_SCHEMA: &str = "flow-nexus-context-warning-policies-v1";
pub(super) const HOOK_TABLE: &str = "flow_nexus_harness_hook_events";
pub(super) const HOOK_FAMILY: &str = "flow-nexus-harness-hook-events";
pub(super) const HOOK_SCHEMA: &str = "flow-nexus-harness-hook-events-v1";
pub(super) const PROGRESS_TABLE: &str = "flow_nexus_context_warning_progress";
pub(super) const PROGRESS_FAMILY: &str = "flow-nexus-context-warning-progress";
pub(super) const PROGRESS_SCHEMA: &str = "flow-nexus-context-warning-progress-v1";

pub(crate) struct ContextWarningConfigurationTables {
    pub(super) policies: TableReference<StoredContextWarningPolicy>,
    pub(super) hooks: TableReference<StoredHarnessHookEvents>,
}

pub(crate) trait RegistersContextWarningConfiguration: Sized {
    fn register(engine: &mut Engine) -> Result<Self, StoreError>;
}

pub(crate) trait ReadsContextWarningConfiguration {
    fn context_warning_policy(
        &self,
        model: &str,
    ) -> Result<Option<ContextWarningPolicy>, StoreError>;
    fn configured_hook_events(&self) -> Result<Vec<HarnessHookEvent>, StoreError>;
}

pub(crate) trait ReadsContextWarningProgress {
    fn warning_progress(&self, flow_id: &str)
    -> Result<Option<ContextWarningProgress>, StoreError>;
}

pub(crate) trait ProjectsWarningPolicy {
    fn policy(&self) -> ContextWarningPolicy;
}

pub(crate) trait DescribesWarningRecord: EngineRecord + MemoryDefinition {
    fn descriptor() -> TableDescriptor<Self>
    where
        Self: Sized;
}

pub(crate) trait ConfiguresContextWarnings {
    fn configure_policy(
        engine: &Engine,
        tables: &ContextWarningConfigurationTables,
        policy: ContextWarningPolicy,
    ) -> Result<ConfigurationCommit, StoreError>;
    fn configure_hooks(
        engine: &Engine,
        tables: &ContextWarningConfigurationTables,
        events: Vec<HarnessHookEvent>,
    ) -> Result<ConfigurationCommit, StoreError>;
}

impl EngineRecord for StoredContextWarningPolicy {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.model_name.0.clone())
    }
}

impl EngineRecord for StoredHarnessHookEvents {
    fn record_key(&self) -> RecordKey {
        RecordKey::new("hooks")
    }
}

impl EngineRecord for ContextWarningProgress {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.flow_id.0.clone())
    }
}

impl MemoryDefinition for StoredContextWarningPolicy {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(1)
    }
    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label(POLICY_SCHEMA)
    }
}

impl MemoryDefinition for StoredHarnessHookEvents {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(1)
    }
    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label(HOOK_SCHEMA)
    }
}

impl MemoryDefinition for ContextWarningProgress {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(1)
    }
    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label(PROGRESS_SCHEMA)
    }
}

impl DescribesWarningRecord for StoredContextWarningPolicy {
    fn descriptor() -> TableDescriptor<Self> {
        TableDescriptor::new(
            TableName::new(POLICY_TABLE),
            FamilyName::new(POLICY_FAMILY),
            Self::schema_hash(),
        )
    }
}

impl DescribesWarningRecord for StoredHarnessHookEvents {
    fn descriptor() -> TableDescriptor<Self> {
        TableDescriptor::new(
            TableName::new(HOOK_TABLE),
            FamilyName::new(HOOK_FAMILY),
            Self::schema_hash(),
        )
    }
}

impl DescribesWarningRecord for ContextWarningProgress {
    fn descriptor() -> TableDescriptor<Self> {
        TableDescriptor::new(
            TableName::new(PROGRESS_TABLE),
            FamilyName::new(PROGRESS_FAMILY),
            Self::schema_hash(),
        )
    }
}

impl RegistersContextWarningConfiguration for ContextWarningConfigurationTables {
    fn register(engine: &mut Engine) -> Result<Self, StoreError> {
        Ok(Self {
            policies: engine.register_table(StoredContextWarningPolicy::descriptor())?,
            hooks: engine.register_table(StoredHarnessHookEvents::descriptor())?,
        })
    }
}

impl ProjectsWarningPolicy for StoredContextWarningPolicy {
    fn policy(&self) -> ContextWarningPolicy {
        ContextWarningPolicy {
            model_name: self.model_name.clone(),
            context_warning_limit: self.context_warning_limit.clone(),
            context_usage_projection: self.context_usage_projection.clone(),
            warning_boundary_vector: self.warning_boundary_vector.clone(),
        }
    }
}

impl ConfiguresContextWarnings for FlowStore {
    fn configure_policy(
        engine: &Engine,
        tables: &ContextWarningConfigurationTables,
        policy: ContextWarningPolicy,
    ) -> Result<ConfigurationCommit, StoreError> {
        policy
            .validate()
            .map_err(StoreError::ContextWarningConfigurationRefused)?;
        Self::declare_configuration_entry(
            engine,
            tables.policies,
            StoredContextWarningPolicy {
                model_name: policy.model_name,
                context_warning_limit: policy.context_warning_limit,
                context_usage_projection: policy.context_usage_projection,
                warning_boundary_vector: policy.warning_boundary_vector,
            },
        )
    }

    fn configure_hooks(
        engine: &Engine,
        tables: &ContextWarningConfigurationTables,
        events: Vec<HarnessHookEvent>,
    ) -> Result<ConfigurationCommit, StoreError> {
        if events
            .iter()
            .enumerate()
            .any(|(index, event)| events[..index].contains(event))
        {
            return Err(StoreError::ContextWarningConfigurationRefused(
                meta_signal_flow::ContextWarningConfigurationRefusal::DuplicateHookEvent,
            ));
        }
        Self::declare_configuration_entry(engine, tables.hooks, StoredHarnessHookEvents(events))
    }
}

impl ReadsContextWarningConfiguration for FlowStore {
    fn context_warning_policy(
        &self,
        model: &str,
    ) -> Result<Option<ContextWarningPolicy>, StoreError> {
        let rows = self.engine.match_records(QueryPlan::key(
            self.meta_configuration_tables.context_warnings.policies,
            RecordKey::new(model),
        ))?;
        match rows.records() {
            [] => Ok(None),
            [row] => Ok(Some(row.policy())),
            _ => Err(StoreError::StateInvariant),
        }
    }

    fn configured_hook_events(&self) -> Result<Vec<HarnessHookEvent>, StoreError> {
        let rows = self.engine.match_records(QueryPlan::key(
            self.meta_configuration_tables.context_warnings.hooks,
            RecordKey::new("hooks"),
        ))?;
        match rows.records() {
            [] => Ok(Vec::new()),
            [row] => Ok(row.0.clone()),
            _ => Err(StoreError::StateInvariant),
        }
    }
}

impl ReadsContextWarningProgress for FlowStore {
    fn warning_progress(
        &self,
        flow_id: &str,
    ) -> Result<Option<ContextWarningProgress>, StoreError> {
        let rows = self.engine.match_records(QueryPlan::key(
            self.warning_progress,
            RecordKey::new(flow_id),
        ))?;
        match rows.records() {
            [] => Ok(None),
            [row] => Ok(Some(row.clone())),
            _ => Err(StoreError::StateInvariant),
        }
    }
}
