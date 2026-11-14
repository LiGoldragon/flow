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
