//! The send tool's calls. Each call a Codex thread makes of Flow's send
//! tool is kept once handled, keyed by its thread, turn and call id, with
//! the digest of what it asked and the outcome it was answered with. A
//! queued call's row is written in the commit that queues its request, so a
//! call the app-server replays after a crash or a reconnect finds its row
//! and is answered from it, its request queued once. The rows are their own
//! table, so a store written before them opens with the table added and
//! holds none.
//!
//! A store before 0.35.22 keeps its calls as v1 rows, whose refusals could
//! name an ended recipient; each is carried into the current shape, and a
//! call refused for an ended recipient refuses the upgrade, the store left
//! as it was: no metaflow is ended any more, and no current refusal says
//! what it said.

use super::{
    FlowStore, ManagesMetaflowState, NamesLiveFlow, ReadsFlowRows, ReadsLaunchAttempt, StoreError,
    metaflow::{SleepsMetaflows, TakesLaunchAddress},
    opened_thread::RecordsOpenedThreads,
};
use crate::generated::send_tool::{
    Queued_Data, SendCall, SendOutcome, SendRefusal, Target, ToolCallKey,
};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use sema_engine::{
    Assertion, Decision, EngineRecord, FamilyName, MemoryDefinition, MemoryVersion, QueryPlan,
    RecordKey, SchemaHash, TableDescriptor, TableName, UpgradeFrom,
};
use signal_flow::{Address, HarnessKind, QueuedRequest, Request, SentBy};

pub(super) const SEND_CALL_TABLE_NAME: TableName = TableName::new("flow_nexus_send_calls");
pub(super) const SEND_CALL_FAMILY: &str = "flow-nexus-send-call";
pub(super) const SEND_CALL_SCHEMA: &str = "flow-nexus-send-call-v3";

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

impl MemoryDefinition for SendCall {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(3)
    }

    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label(SEND_CALL_SCHEMA)
    }
}

/// A send call as Flow kept it before oversized native prompts were refused.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub struct SendCallV2 {
    tool_call_key: ToolCallKey,
    content_sha256: String,
    send_outcome: SendOutcomeV2,
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
enum SendOutcomeV2 {
    Queued(Queued_Data),
    Refused(SendRefusalV2),
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
enum SendRefusalV2 {
    UnknownTool,
    Malformed,
    ThreadUnowned,
    FlowEnded,
    NotAwake,
    KeyReused,
    UnknownRecipient(Target),
    Held,
    Store,
}

impl EngineRecord for SendCallV2 {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.tool_call_key.record_key_text())
    }
}

impl MemoryDefinition for SendCallV2 {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(2)
    }

    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label("flow-nexus-send-call-v2")
    }
}

impl UpgradeFrom<SendCallV2> for SendCall {
    type Refusal = std::convert::Infallible;

    fn upgrade_from(previous: SendCallV2) -> Decision<Self, Self::Refusal> {
        let send_outcome = match previous.send_outcome {
            SendOutcomeV2::Queued(queued) => SendOutcome::Queued(queued),
            SendOutcomeV2::Refused(refusal) => SendOutcome::Refused(match refusal {
                SendRefusalV2::UnknownTool => SendRefusal::UnknownTool,
                SendRefusalV2::Malformed => SendRefusal::Malformed,
                SendRefusalV2::ThreadUnowned => SendRefusal::ThreadUnowned,
                SendRefusalV2::FlowEnded => SendRefusal::FlowEnded,
                SendRefusalV2::NotAwake => SendRefusal::NotAwake,
                SendRefusalV2::KeyReused => SendRefusal::KeyReused,
                SendRefusalV2::UnknownRecipient(target) => SendRefusal::UnknownRecipient(target),
                SendRefusalV2::Held => SendRefusal::Held,
                SendRefusalV2::Store => SendRefusal::Store,
            }),
        };
        Decision::Ready(Self {
            tool_call_key: previous.tool_call_key,
            content_sha256: previous.content_sha256,
            send_outcome,
        })
    }
}

/// A call as Flow kept it before 0.35.22.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub struct SendCallV1 {
    tool_call_key: ToolCallKey,
    content_sha256: String,
    send_outcome: SendOutcomeV1,
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
enum SendOutcomeV1 {
    Queued(Queued_Data),
    Refused(SendRefusalV1),
}

/// The send tool's refusal as 0.35.21 archived it, RecipientEnded between
/// UnknownRecipient and Held.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
enum SendRefusalV1 {
    UnknownTool,
    Malformed,
    ThreadUnowned,
    FlowEnded,
    NotAwake,
    KeyReused,
    UnknownRecipient(Target),
    RecipientEnded(Target),
    Held,
    Store,
}

impl EngineRecord for SendCallV1 {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.tool_call_key.record_key_text())
    }
}

impl MemoryDefinition for SendCallV1 {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(1)
    }

    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label("flow-nexus-send-call-v1")
    }
}

/// A kept call refused for an ended recipient.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EndedRecipientCall;

impl UpgradeFrom<SendCallV1> for SendCall {
    type Refusal = EndedRecipientCall;

    fn upgrade_from(previous: SendCallV1) -> Decision<Self, Self::Refusal> {
        let send_outcome = match previous.send_outcome {
            SendOutcomeV1::Queued(queued) => SendOutcome::Queued(queued),
            SendOutcomeV1::Refused(refusal) => SendOutcome::Refused(match refusal {
                SendRefusalV1::UnknownTool => SendRefusal::UnknownTool,
                SendRefusalV1::Malformed => SendRefusal::Malformed,
                SendRefusalV1::ThreadUnowned => SendRefusal::ThreadUnowned,
                SendRefusalV1::FlowEnded => SendRefusal::FlowEnded,
                SendRefusalV1::NotAwake => SendRefusal::NotAwake,
                SendRefusalV1::KeyReused => SendRefusal::KeyReused,
                SendRefusalV1::UnknownRecipient(target) => SendRefusal::UnknownRecipient(target),
                SendRefusalV1::RecipientEnded(_) => {
                    return Decision::Refused(EndedRecipientCall);
                }
                SendRefusalV1::Held => SendRefusal::Held,
                SendRefusalV1::Store => SendRefusal::Store,
            }),
        };
        Decision::Ready(Self {
            tool_call_key: previous.tool_call_key,
            content_sha256: previous.content_sha256,
            send_outcome,
        })
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
        .with_predecessor::<SendCallV1>()
        .with_predecessor::<SendCallV2>()
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
    /// commit: an absent target is UnknownRecipient, one Message holds an
    /// unexpired lock on is Held; a refused call is recorded alone.
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

#[cfg(test)]
mod tests {
    use super::{
        KeepsSendCalls, SendCallV1, SendCallV2, SendOutcomeV1, SendOutcomeV2, SendRefusalV1,
        SendRefusalV2,
    };
    use crate::generated::send_tool::{SendCall, SendOutcome, SendRefusal, ToolCallKey};
    use crate::store::{FlowStore, OpensFlowStore};
    use sema_engine::{
        Assertion, Engine, EngineOpen, FamilyName, SchemaHash, SchemaVersion, TableDescriptor,
        TableName,
    };

    fn key(call_id: &str) -> ToolCallKey {
        ToolCallKey {
            thread_id: "thread-1".into(),
            turn_id: "turn-1".into(),
            call_id: call_id.into(),
        }
    }

    fn seed_v1(path: &std::path::Path, calls: Vec<SendCallV1>) {
        let mut engine =
            Engine::open(EngineOpen::new(path, SchemaVersion::new(1))).expect("store opens");
        let table = engine
            .register_table(TableDescriptor::new(
                TableName::new("flow_nexus_send_calls"),
                FamilyName::new("flow-nexus-send-call"),
                SchemaHash::for_label("flow-nexus-send-call-v1"),
            ))
            .expect("v1 send call family registers");
        for call in calls {
            engine
                .assert(Assertion::new(table, call))
                .expect("v1 call persists");
        }
    }

    fn seed_v2(path: &std::path::Path, calls: Vec<SendCallV2>) {
        let mut engine =
            Engine::open(EngineOpen::new(path, SchemaVersion::new(1))).expect("store opens");
        let table = engine
            .register_table(TableDescriptor::new(
                TableName::new("flow_nexus_send_calls"),
                FamilyName::new("flow-nexus-send-call"),
                SchemaHash::for_label("flow-nexus-send-call-v2"),
            ))
            .expect("v2 send call family registers");
        for call in calls {
            engine
                .assert(Assertion::new(table, call))
                .expect("v2 call persists");
        }
    }

    #[test]
    fn v2_send_calls_upgrade_unchanged_to_v3() {
        let directory = tempfile::tempdir().expect("store directory");
        let path = directory.path().join("flow.sema");
        seed_v2(
            &path,
            vec![SendCallV2 {
                tool_call_key: key("held"),
                content_sha256: "digest".into(),
                send_outcome: SendOutcomeV2::Refused(SendRefusalV2::Held),
            }],
        );
        let store = FlowStore::open(&path).expect("store opens");
        assert_eq!(
            store.send_call(&key("held")).unwrap(),
            Some(SendCall {
                tool_call_key: key("held"),
                content_sha256: "digest".into(),
                send_outcome: SendOutcome::Refused(SendRefusal::Held),
            })
        );
    }

    /// A 0.35.21 call opens in the current shape: Held and Store, after the
    /// removed RecipientEnded, keep their meaning.
    #[test]
    fn v1_send_calls_upgrade_without_the_ended_recipient() {
        let directory = tempfile::tempdir().expect("store directory");
        let path = directory.path().join("flow.sema");
        let v1 = |call_id: &str, refusal| SendCallV1 {
            tool_call_key: key(call_id),
            content_sha256: "digest".into(),
            send_outcome: SendOutcomeV1::Refused(refusal),
        };
        seed_v1(
            &path,
            vec![
                v1("held", SendRefusalV1::Held),
                v1("store", SendRefusalV1::Store),
            ],
        );
        let store = FlowStore::open(&path).expect("store opens");
        for (call_id, refusal) in [("held", SendRefusal::Held), ("store", SendRefusal::Store)] {
            assert_eq!(
                store.send_call(&key(call_id)).unwrap(),
                Some(SendCall {
                    tool_call_key: key(call_id),
                    content_sha256: "digest".into(),
                    send_outcome: SendOutcome::Refused(refusal),
                })
            );
        }
    }

    /// A kept call refused for an ended recipient refuses the open.
    #[test]
    fn a_v1_call_refused_for_an_ended_recipient_refuses_the_open() {
        let directory = tempfile::tempdir().expect("store directory");
        let path = directory.path().join("flow.sema");
        seed_v1(
            &path,
            vec![SendCallV1 {
                tool_call_key: key("ended"),
                content_sha256: "digest".into(),
                send_outcome: SendOutcomeV1::Refused(SendRefusalV1::RecipientEnded(
                    signal_flow::Address {
                        aspect: signal_flow::Aspect::Mind,
                        topic: "flow".into(),
                        layer: signal_flow::Layer::Secondary,
                    },
                )),
            }],
        );
        let Err(error) = FlowStore::open(&path) else {
            panic!("an ended recipient call opens")
        };
        assert!(
            error.to_string().contains("ended recipient"),
            "the refusal says why: {error}"
        );
    }
}
