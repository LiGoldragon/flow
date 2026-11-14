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

pub(crate) enum HookConfigurationOrigin {
    ExistingStore,
    NewStore,
}

pub(crate) struct ContextWarningConfigurationTables {
    pub(super) policies: TableReference<StoredContextWarningPolicy>,
    pub(super) hooks: TableReference<StoredHarnessHookEvents>,
}

pub(crate) trait RegistersContextWarningConfiguration: Sized {
    fn register(engine: &mut Engine, origin: HookConfigurationOrigin) -> Result<Self, StoreError>;
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
    fn register(engine: &mut Engine, origin: HookConfigurationOrigin) -> Result<Self, StoreError> {
        let policies = engine.register_table(StoredContextWarningPolicy::descriptor())?;
        let hooks = engine.register_table(StoredHarnessHookEvents::descriptor())?;
        if engine
            .match_records(QueryPlan::all(hooks))?
            .records()
            .is_empty()
        {
            // A pre-vector configuration installed exactly these five hooks.
            // Persist that historical value once; a new store persists none.
            let events = match origin {
                HookConfigurationOrigin::NewStore => Vec::new(),
                HookConfigurationOrigin::ExistingStore => vec![
                    HarnessHookEvent::SessionStart,
                    HarnessHookEvent::PostToolUse,
                    HarnessHookEvent::UserPromptSubmit,
                    HarnessHookEvent::Stop,
                    HarnessHookEvent::SessionEnd,
                ],
            };
            engine.commit_atomic(
                engine
                    .begin_atomic_commit()
                    .assert(hooks, StoredHarnessHookEvents(events)),
            )?;
        }
        Ok(Self { policies, hooks })
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

/// A recorded native observation, supplied while the runtime holds dispatch.
/// The latest owner, model, policy and durable stages are read at admission.
pub(crate) struct ContextWarningAdmission {
    pub(crate) flow_id: signal_flow::FlowId,
    pub(crate) tokens: signal_flow::Tokens,
}

pub(crate) struct PreparedContextWarning {
    progress: ContextWarningProgress,
    crossing: crate::context_warnings::WarningCrossing,
    existing: bool,
    pub(crate) message: flow_ethos::Message,
}

pub(crate) trait PreparesContextWarningAdmission {
    fn prepare_context_warning(
        &self,
        metaflow: &flow_ethos::Metaflow,
        observation: ContextWarningAdmission,
    ) -> Result<Option<PreparedContextWarning>, StoreError>;
    fn commit_context_warning(
        &self,
        prepared: PreparedContextWarning,
        delivery_id: signal_flow::LogicalDeliveryId,
        commit: sema_engine::AtomicCommit,
    ) -> sema_engine::AtomicCommit;
}

/// Admission has no native transport. The runtime owns dispatch exclusion
/// until this returns and releases it before announcing delivery work.
pub(crate) trait AdmitsContextWarnings {
    fn admit_context_warning(
        &self,
        observation: ContextWarningAdmission,
    ) -> Result<Option<signal_flow::PrivateSendResponse>, StoreError>;
}

impl PreparesContextWarningAdmission for FlowStore {
    fn prepare_context_warning(
        &self,
        metaflow: &flow_ethos::Metaflow,
        observation: ContextWarningAdmission,
    ) -> Result<Option<PreparedContextWarning>, StoreError> {
        use super::context::{NamesFlowModel, RecordsContextSize};
        use crate::context_warnings::TracksContextWarnings;
        if observation.tokens.0 < 0
            || metaflow.state != flow_ethos::State::Awake(observation.flow_id.clone())
            || self.context_size(&observation.flow_id.0)? != Some(observation.tokens.0)
        {
            return Ok(None);
        }
        let Some(model) = self.flow_model(&observation.flow_id.0)? else {
            return Ok(None);
        };
        let Some(policy) = self.context_warning_policy(&model)? else {
            return Ok(None);
        };
        let Some(node) = self.flow_node(&observation.flow_id.0)? else {
            return Ok(None);
        };
        let native_projection = match node.harness_kind {
            signal_flow::HarnessKind::Codex => {
                meta_signal_flow::ContextUsageProjection::CodexLastTotal
            }
            signal_flow::HarnessKind::Claude => {
                meta_signal_flow::ContextUsageProjection::ClaudeCompletedInputWithCache
            }
        };
        if !node.flow_lifecycle.is_live()
            || policy.context_usage_projection != native_projection
            || policy
                .context_warning_limit
                .context_boundary_evidence
                .harness_kind
                != node.harness_kind
        {
            return Ok(None);
        }
        let held = self.warning_progress(&observation.flow_id.0)?;
        let existing = held.is_some();
        let progress = held.unwrap_or_else(|| ContextWarningProgress::initial(observation.flow_id));
        let Some(crossing) = policy.crossing(&progress, observation.tokens.0) else {
            return Ok(None);
        };
        let Some(boundary) = policy.boundary(&crossing.selected) else {
            return Ok(None);
        };
        let Some(text) = policy.warning_text(&crossing.selected, observation.tokens.0) else {
            return Ok(None);
        };
        let request = flow_ethos::Request::Notice(text);
        let message = match boundary.warning_priority {
            meta_signal_flow::WarningPriority::Soft => flow_ethos::Message::Soft(request),
            meta_signal_flow::WarningPriority::MiddleAbrupt => {
                flow_ethos::Message::MiddleAbrupt(request)
            }
            meta_signal_flow::WarningPriority::HardAbrupt => {
                flow_ethos::Message::HardAbrupt(request)
            }
        };
        Ok(Some(PreparedContextWarning {
            progress,
            crossing,
            existing,
            message,
        }))
    }

    fn commit_context_warning(
        &self,
        prepared: PreparedContextWarning,
        delivery_id: signal_flow::LogicalDeliveryId,
        commit: sema_engine::AtomicCommit,
    ) -> sema_engine::AtomicCommit {
        use crate::context_warnings::TracksContextWarnings;
        let next = prepared.progress.admitted(prepared.crossing, delivery_id);
        if prepared.existing {
            commit.mutate(self.warning_progress, next)
        } else {
            commit.assert(self.warning_progress, next)
        }
    }
}

impl AdmitsContextWarnings for FlowStore {
    fn admit_context_warning(
        &self,
        observation: ContextWarningAdmission,
    ) -> Result<Option<signal_flow::PrivateSendResponse>, StoreError> {
        use super::logical_delivery::{LogicalDeliveryTransition, RecordsLogicalDeliveries};
        use super::metaflow::SleepsMetaflows;
        let Some(metaflow) = self.metaflow_awake_as(&observation.flow_id.0)? else {
            return Ok(None);
        };
        // This preview avoids allocating a request for an unchanged stage.
        // Logical admission repeats the judgment under its write gates.
        let Some(preview) = self.prepare_context_warning(
            &metaflow,
            ContextWarningAdmission {
                flow_id: observation.flow_id.clone(),
                tokens: observation.tokens.clone(),
            },
        )?
        else {
            return Ok(None);
        };
        self.accept_logical_delivery_for_with_transition(
            &metaflow.address,
            flow_ethos::SentBy::Flow,
            flow_ethos::Delivery::Queue,
            preview.message,
            None,
            LogicalDeliveryTransition {
                purpose: crate::heartbeat::DeliveryPurpose::Ordinary,
                context_warning: Some(observation),
                handover_reminder: None,
                deliver_call: None,
                upgrade_notice: None,
            },
        )
        .map(Some)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_existing_store_without_a_vector_gets_its_historical_hooks_once() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("old-configuration.sema");
        // A pre-vector engine has no hook-event record. This is a migration
        // fixture, not a historical binary execution or a native hook proof.
        drop(Engine::open(EngineOpen::new(&path, SchemaVersion::new(1))).unwrap());
        let selected = vec![HarnessHookEvent::PostToolUse, HarnessHookEvent::Stop];
        {
            let mut engine = Engine::open(EngineOpen::new(&path, SchemaVersion::new(1))).unwrap();
            let tables = ContextWarningConfigurationTables::register(
                &mut engine,
                HookConfigurationOrigin::ExistingStore,
            )
            .unwrap();
            assert_eq!(
                engine
                    .match_records(QueryPlan::all(tables.hooks))
                    .unwrap()
                    .records(),
                &[StoredHarnessHookEvents(vec![
                    HarnessHookEvent::SessionStart,
                    HarnessHookEvent::PostToolUse,
                    HarnessHookEvent::UserPromptSubmit,
                    HarnessHookEvent::Stop,
                    HarnessHookEvent::SessionEnd,
                ])],
            );
            FlowStore::configure_hooks(&engine, &tables, selected.clone()).unwrap();
        }
        {
            let mut engine = Engine::open(EngineOpen::new(&path, SchemaVersion::new(1))).unwrap();
            let tables = ContextWarningConfigurationTables::register(
                &mut engine,
                HookConfigurationOrigin::ExistingStore,
            )
            .unwrap();
            assert_eq!(
                engine
                    .match_records(QueryPlan::all(tables.hooks))
                    .unwrap()
                    .records(),
                &[StoredHarnessHookEvents(selected)]
            );
            FlowStore::configure_hooks(&engine, &tables, Vec::new()).unwrap();
        }
        let mut engine = Engine::open(EngineOpen::new(&path, SchemaVersion::new(1))).unwrap();
        let tables = ContextWarningConfigurationTables::register(
            &mut engine,
            HookConfigurationOrigin::ExistingStore,
        )
        .unwrap();
        assert_eq!(
            engine
                .match_records(QueryPlan::all(tables.hooks))
                .unwrap()
                .records(),
            &[StoredHarnessHookEvents(Vec::new())]
        );
    }

    #[test]
    fn a_new_store_persists_an_authoritative_empty_vector() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("new-configuration.sema");
        let mut engine = Engine::open(EngineOpen::new(&path, SchemaVersion::new(1))).unwrap();
        let tables = ContextWarningConfigurationTables::register(
            &mut engine,
            HookConfigurationOrigin::NewStore,
        )
        .unwrap();
        assert_eq!(
            engine
                .match_records(QueryPlan::all(tables.hooks))
                .unwrap()
                .records(),
            &[StoredHarnessHookEvents(Vec::new())]
        );
    }
}
