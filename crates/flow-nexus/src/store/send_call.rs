//! The send tool's calls. Each call a Codex thread makes of Flow's send
//! tool is kept once handled, keyed by its thread, turn and call id, with
//! the digest of what it asked and the outcome it was answered with. A
//! queued call's row is written in the commit that queues its request, so a
//! call the app-server replays after a crash or a reconnect finds its row
//! and is answered from it, its request queued once. The rows are their own
//! table, so a store written before them opens with the table added and
//! holds none.

use super::{
    FlowStore, ManagesMetaflowState, MetaflowState, NamesLiveFlow, ReadsFlowRows,
    ReadsLaunchAttempt, StoreError,
    metaflow::{SleepsMetaflows, TakesLaunchAddress},
    opened_thread::RecordsOpenedThreads,
};
use crate::generated::send_tool::{
    Queued_Data, SendCall, SendOutcome, SendRefusal, Target, ToolCallKey,
};
use sema_engine::{
    Assertion, EngineRecord, FamilyName, QueryPlan, RecordKey, SchemaHash, TableDescriptor,
    TableName,
};
use signal_flow::{Address, HarnessKind, QueuedRequest, Request, SentBy};

pub(super) const SEND_CALL_TABLE_NAME: TableName = TableName::new("flow_nexus_send_calls");
pub(super) const SEND_CALL_FAMILY: &str = "flow-nexus-send-call";
pub(super) const SEND_CALL_SCHEMA: &str = "flow-nexus-send-call-v1";

/// The record key of a call: each part prefixed by its length, so no two
/// keys of different parts read alike.
pub(crate) trait KeysToolCall {
    fn record_key_text(&self) -> String;
}

impl KeysToolCall for ToolCallKey {
    fn record_key_text(&self) -> String {
        format!(
            "{}:{}{}:{}{}:{}",
            self.thread_id.len(),
            self.thread_id,
            self.turn_id.len(),
            self.turn_id,
            self.call_id.len(),
            self.call_id
        )
    }
}

impl EngineRecord for SendCall {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.tool_call_key.record_key_text())
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
    /// Queues a call's request for its target under the rules a Lock and a
    /// Deliver follow, and records the call with its outcome in the same
    /// commit: an absent target is UnknownRecipient, an Ended one
    /// RecipientEnded, one Message holds an unexpired lock on is Held; a
    /// refused call is recorded alone.
    fn queue_send(
        &self,
        key: &ToolCallKey,
        content_sha256: &str,
        sender: Address,
        target: &Target,
        request: Request,
        now: i64,
    ) -> Result<SendOutcome, StoreError>;
}

impl KeepsSendCalls for FlowStore {
    fn thread_sender(&self, thread_id: &str) -> Result<Result<Address, SendRefusal>, StoreError> {
        let flows = self
            .flow_nodes()?
            .into_iter()
            .filter(|node| {
                node.harness_kind == HarnessKind::Codex
                    && !thread_id.is_empty()
                    && node.session_id == thread_id
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
            && let Some(metaflow) = self.metaflow_awake_as(&flow.flow_id)?
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

    fn queue_send(
        &self,
        key: &ToolCallKey,
        content_sha256: &str,
        sender: Address,
        target: &Target,
        request: Request,
        now: i64,
    ) -> Result<SendOutcome, StoreError> {
        let call = |send_outcome| SendCall {
            tool_call_key: key.clone(),
            content_sha256: content_sha256.to_owned(),
            send_outcome,
        };
        let refused = |refusal| -> Result<SendOutcome, StoreError> {
            let outcome = SendOutcome::Refused(refusal);
            self.record_send_call(call(outcome.clone()))?;
            Ok(outcome)
        };
        let Some(mut stored) = self.stored_metaflow(target)? else {
            return refused(SendRefusal::UnknownRecipient(target.clone()));
        };
        if stored.metaflow.state == MetaflowState::Ended {
            return refused(SendRefusal::RecipientEnded(target.clone()));
        }
        // Message holds the target while it delivers; Lock refuses another
        // sender then, and so does the send tool.
        if self
            .stored_metaflow_lock(target)?
            .is_some_and(|held| held.lock.until > now)
        {
            return refused(SendRefusal::Held);
        }
        stored.metaflow.queue.push(QueuedRequest {
            sent_by: SentBy::Sender(sender.clone()),
            request,
        });
        let outcome = SendOutcome::Queued(Queued_Data {
            sender,
            target: target.clone(),
        });
        let commit = self
            .engine
            .begin_atomic_commit()
            .mutate(self.metaflows, stored)
            .assert(self.send_calls, call(outcome.clone()));
        self.engine.commit_atomic(commit)?;
        // This is durable enqueue, never a claim of native delivery or wake.
        Ok(outcome)
    }
}
