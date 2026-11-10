//! The send tool's calls. Each call a Codex thread makes of Flow's send
//! tool is kept once handled, keyed by its thread, turn and call id, with
//! the digest of what it asked and the outcome it was answered with. A
//! queued call's row is written in the commit that queues its request, so a
//! call the app-server replays after a crash or a reconnect finds its row
//! and is answered from it, its request queued once.

use super::{
    FlowStore, NamesLiveFlow, ReadsFlowRows, ReadsLaunchAttempt, StoreError,
    metaflow::{SleepsMetaflows, TakesLaunchAddress},
    opened_thread::RecordsOpenedThreads,
};
use crate::generated::send_tool::{SendCall, SendRefusal, ToolCallKey};
use sema_engine::{
    Assertion, EngineRecord, FamilyName, MemoryDefinition, MemoryVersion, QueryPlan, RecordKey,
    SchemaHash, TableDescriptor, TableName,
};
use signal_flow::{Address, HarnessKind};

pub(super) const SEND_CALL_TABLE_NAME: TableName = TableName::new("flow_nexus_send_calls");
pub(super) const SEND_CALL_FAMILY: &str = "flow-nexus-send-call";
pub(super) const SEND_CALL_SCHEMA: &str = "flow-nexus-send-call-v8";

/// The record key of a call: each part prefixed by its length, so no two
/// keys of different parts read alike.
pub(crate) trait KeysToolCall {
    fn record_key_text(&self) -> String;
}

impl KeysToolCall for ToolCallKey {
    fn record_key_text(&self) -> String {
        format!(
            "{}:{}{}:{}{}:{}",
            self.thread_id.0.len(),
            self.thread_id.0,
            self.turn_id.0.len(),
            self.turn_id.0,
            self.call_id.0.len(),
            self.call_id.0
        )
    }
}

impl EngineRecord for SendCall {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.tool_call_key.record_key_text())
    }
}

impl MemoryDefinition for SendCall {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(8)
    }

    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label(SEND_CALL_SCHEMA)
    }
}

pub(super) trait DescribesSendCallTable {
    fn descriptor() -> TableDescriptor<SendCall>;
}

impl DescribesSendCallTable for SendCall {
    fn descriptor() -> TableDescriptor<SendCall> {
        TableDescriptor::new(
            SEND_CALL_TABLE_NAME,
            FamilyName::new(SEND_CALL_FAMILY),
            SchemaHash::for_label(SEND_CALL_SCHEMA),
        )
        .with_predecessor::<super::request_id_upgrade::PriorSendCall>()
    }
}

pub(crate) trait KeepsSendCalls {
    /// The Address a thread's calls are sent from: the metaflow Awake as
    /// the one live flow Flow holds on that Codex thread; else, while the
    /// launch that opened the thread is unsettled and its Address is still
    /// that launch's to take, that Address, the launching metaflow (a first
    /// turn calls before its flow is bound). ThreadUnowned when no flow
    /// Flow holds runs it and no unsettled launch opened it, FlowEnded when
    /// its flow is Stopped, Retired or Exited, NotAwake when its metaflow is
    /// not Awake as that flow.
    fn thread_sender(&self, thread_id: &str) -> Result<Result<Address, SendRefusal>, StoreError>;
    /// The Address of the unsettled Codex launch that opened `thread_id`,
    /// while no other flow or launch holds that Address.
    fn launching_address(&self, thread_id: &str) -> Result<Option<Address>, StoreError>;
    /// The row a call left, when it was handled.
    fn send_call(&self, key: &ToolCallKey) -> Result<Option<SendCall>, StoreError>;
    /// Records a call refused before anything was queued.
    fn record_send_call(&self, call: SendCall) -> Result<(), StoreError>;
    /// Replaces the recorded answer of the same call after its one logical
    /// delivery advances. This never admits or submits work.
    fn update_send_call(&self, call: SendCall) -> Result<(), StoreError>;
}

impl KeepsSendCalls for FlowStore {
    fn thread_sender(&self, thread_id: &str) -> Result<Result<Address, SendRefusal>, StoreError> {
        let flows = self
            .flow_nodes()?
            .into_iter()
            .filter(|node| {
                node.harness_kind == HarnessKind::Codex
                    && !thread_id.is_empty()
                    && node.session_id.0 == thread_id
            })
            .collect::<Vec<_>>();
        let live = flows
            .iter()
            .filter(|node| node.flow_lifecycle.is_live())
            .collect::<Vec<_>>();
        let flow = match live.as_slice() {
            [] => None,
            [flow] => Some(flow),
            _ => return Err(StoreError::StateInvariant),
        };
        if let Some(flow) = flow
            && let Some(metaflow) = self.metaflow_awake_as(&flow.flow_id.0)?
        {
            return Ok(Ok(metaflow.address));
        }
        if let Some(address) = self.launching_address(thread_id)? {
            return Ok(Ok(address));
        }
        Ok(Err(if flows.is_empty() {
            SendRefusal::ThreadUnowned
        } else if flow.is_none() {
            SendRefusal::FlowEnded
        } else {
            SendRefusal::NotAwake
        }))
    }

    fn launching_address(&self, thread_id: &str) -> Result<Option<Address>, StoreError> {
        let Some(launch_request_id) = self.thread_launch(thread_id)? else {
            return Ok(None);
        };
        let Some(attempt) = self.launch_attempt(&launch_request_id)? else {
            return Ok(None);
        };
        let codex = attempt
            .native_launch_intent_option
            .as_ref()
            .is_some_and(|intent| intent.harness_kind == HarnessKind::Codex);
        if !codex
            || !self.launch_attempt_unsettled(&attempt)?
            || self
                .launch_address_refusal(&attempt.launch_profile)?
                .is_some()
        {
            return Ok(None);
        }
        Ok(Some(attempt.launch_profile.address))
    }

    fn send_call(&self, key: &ToolCallKey) -> Result<Option<SendCall>, StoreError> {
        let records = self
            .engine
            .match_records(QueryPlan::key(
                self.send_calls,
                RecordKey::new(key.record_key_text()),
            ))?
            .records()
            .to_vec();
        match records.as_slice() {
            [] => Ok(None),
            [call] => Ok(Some(call.clone())),
            _ => Err(StoreError::StateInvariant),
        }
    }

    fn record_send_call(&self, call: SendCall) -> Result<(), StoreError> {
        self.engine.assert(Assertion::new(self.send_calls, call))?;
        Ok(())
    }

    fn update_send_call(&self, call: SendCall) -> Result<(), StoreError> {
        self.engine.commit_atomic(
            self.engine
                .begin_atomic_commit()
                .mutate(self.send_calls, call),
        )?;
        Ok(())
    }
}
