use crate::{
    AgentToolAccess, Commit, ConfigurationObservation, MetaInput, Permaflow, PermaflowEdit, Refusal,
};
use kameo::prelude::*;
use sema_engine::{
    Engine, EngineOpen, EngineRecord, FamilyName, QueryPlan, RecordKey, SchemaHash, SchemaVersion,
    TableDescriptor, TableName, TableReference,
};
use signal_flow::Address;
use std::path::Path;

/// This actor alone has the engine handle, including for reads and launch planning.
#[derive(Actor)]
pub struct PermaflowMemory {
    engine: Engine,
    permaflows: TableReference<Permaflow>,
}

trait PermaflowRecord {
    const TABLE: TableName;
    fn descriptor() -> TableDescriptor<Permaflow>;
    fn key(address: &Address) -> RecordKey;
}

impl PermaflowRecord for Permaflow {
    const TABLE: TableName = TableName::new("flow_rewrite_permaflow");

    fn descriptor() -> TableDescriptor<Self> {
        TableDescriptor::new(
            Self::TABLE,
            FamilyName::new("flow-rewrite-permaflow"),
            SchemaHash::for_label("flow-rewrite-permaflow-v1"),
        )
    }

    fn key(address: &Address) -> RecordKey {
        RecordKey::new(format!(
            "{:?}:{:?}:{}",
            address.aspect, address.model, address.topic.0
        ))
    }
}

impl EngineRecord for Permaflow {
    fn record_key(&self) -> RecordKey {
        Self::key(&self.address)
    }
}

pub trait OpensPermaflowMemory: Sized {
    fn open(path: &Path) -> sema_engine::Result<Self>;
}

impl OpensPermaflowMemory for PermaflowMemory {
    fn open(path: &Path) -> sema_engine::Result<Self> {
        let descriptor = Permaflow::descriptor();
        let mut engine =
            Engine::open(EngineOpen::new(path, SchemaVersion::new(1)).with_family(&descriptor))?;
        let permaflows = engine.register_table(descriptor)?;
        Ok(Self { engine, permaflows })
    }
}

pub struct ReadPermaflow(pub Address);

pub enum ConfigurationEffect {
    Committed,
    Unchanged,
    Observed,
}

impl Message<MetaInput> for PermaflowMemory {
    type Reply = Result<(ConfigurationEffect, ConfigurationObservation), Refusal>;

    async fn handle(
        &mut self,
        input: MetaInput,
        _: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        match input {
            MetaInput::Configuration(address) => self
                .observe(&address)
                .map(|observation| (ConfigurationEffect::Observed, observation)),
            MetaInput::Configure(edit) => self.edit(edit),
        }
    }
}

impl Message<ReadPermaflow> for PermaflowMemory {
    type Reply = Result<ConfigurationObservation, Refusal>;

    async fn handle(
        &mut self,
        input: ReadPermaflow,
        _: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        self.observe(&input.0)
    }
}

trait ReadsPermaflow {
    fn read(&self, address: &Address) -> Result<Option<Permaflow>, Refusal>;
    fn observe(&self, address: &Address) -> Result<ConfigurationObservation, Refusal>;
}

impl ReadsPermaflow for PermaflowMemory {
    fn read(&self, address: &Address) -> Result<Option<Permaflow>, Refusal> {
        let rows = self
            .engine
            .match_records(QueryPlan::key(self.permaflows, Permaflow::key(address)))
            .map_err(|_| Refusal::MemoryUnavailable)?;
        match rows.records() {
            [] => Ok(None),
            [row] if row.address == *address => Ok(Some(row.clone())),
            _ => Err(Refusal::CorruptMemory),
        }
    }

    fn observe(&self, address: &Address) -> Result<ConfigurationObservation, Refusal> {
        let permaflow = self.read(address)?.ok_or(Refusal::UnknownPermaflow)?;
        let key = permaflow.record_key();
        let ledger = self
            .engine
            .commit_log()
            .map_err(|_| Refusal::MemoryUnavailable)?;
        let entry = ledger
            .iter()
            .rev()
            .find(|entry| {
                let operations = entry.operations();
                let concerns_row = |operation: &sema_engine::CommitLogOperation| {
                    operation.table_name() == Permaflow::TABLE.as_str()
                        && operation.key() == Some(&key)
                };
                concerns_row(operations.head()) || operations.tail().iter().any(concerns_row)
            })
            .ok_or(Refusal::CorruptMemory)?;
        Ok(ConfigurationObservation {
            permaflow,
            commit: Commit {
                sequence: entry.commit_sequence().value(),
                snapshot: entry.snapshot().value(),
                operation_count: entry.operation_count() as u64,
            },
        })
    }
}

trait EditsPermaflow {
    fn edit(
        &mut self,
        edit: PermaflowEdit,
    ) -> Result<(ConfigurationEffect, ConfigurationObservation), Refusal>;
}

impl EditsPermaflow for PermaflowMemory {
    fn edit(
        &mut self,
        edit: PermaflowEdit,
    ) -> Result<(ConfigurationEffect, ConfigurationObservation), Refusal> {
        let commit = self.engine.begin_atomic_commit();
        let (commit, permaflow) = match edit {
            PermaflowEdit::Define(permaflow) => {
                if self.read(&permaflow.address)?.is_some() {
                    return Err(Refusal::AlreadyDefined);
                }
                (commit.assert(self.permaflows, permaflow.clone()), permaflow)
            }
            PermaflowEdit::AddAgentToolLeaveOut(address) => {
                let mut record = self.read(&address)?.ok_or(Refusal::UnknownPermaflow)?;
                if record.agent_tool_access == AgentToolAccess::Excluded {
                    return self
                        .observe(&address)
                        .map(|observation| (ConfigurationEffect::Unchanged, observation));
                }
                record.agent_tool_access = AgentToolAccess::Excluded;
                (commit.mutate(self.permaflows, record.clone()), record)
            }
        };
        let receipt = self
            .engine
            .commit_atomic(commit)
            .map_err(|_| Refusal::MemoryUnavailable)?;
        Ok((
            ConfigurationEffect::Committed,
            ConfigurationObservation {
                permaflow,
                commit: Commit {
                    sequence: receipt.commit_sequence().value(),
                    snapshot: receipt.snapshot().value(),
                    operation_count: receipt.operation_count() as u64,
                },
            },
        ))
    }
}
