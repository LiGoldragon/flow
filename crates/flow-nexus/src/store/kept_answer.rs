//! Answers routed by the Nexus. A Result or a Done goes to the sender of the
//! request it names, whatever Address its Send writes. A request whose
//! sender is no Address (Unbound, Flow, Unknown) has nowhere to be answered
//! to: its answer is kept with it, the request closed in the same commit,
//! and `Answer` reads it by the request's id.

use super::logical_delivery::{MintsRequestIds, RequestClosure, RequestIdBinding};
use super::queue_delivery::AccountsQueueDelivery;
use super::{AnnouncesLaunchChanges, FlowStore, StoreError};
pub(crate) use crate::generated::memory::KeptAnswer;
use sema_engine::{
    EngineRecord, FamilyName, MemoryDefinition, MemoryVersion, QueryPlan, RecordKey, SchemaHash,
    TableDescriptor, TableName,
};
use signal_flow::{Address, Message, Request, RequestId, SentBy};

pub(super) const KEPT_ANSWER_TABLE_NAME: TableName = TableName::new("flow_nexus_kept_answers");
pub(super) const KEPT_ANSWER_FAMILY: &str = "flow-nexus-kept-answer";
pub(super) const KEPT_ANSWER_SCHEMA: &str = "flow-nexus-kept-answer-v1";

impl EngineRecord for KeptAnswer {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.request_id.0.clone())
    }
}

impl MemoryDefinition for KeptAnswer {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(1)
    }

    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label(KEPT_ANSWER_SCHEMA)
    }
}

pub(super) trait DescribesKeptAnswerTable {
    fn descriptor() -> TableDescriptor<KeptAnswer>;
}

impl DescribesKeptAnswerTable for KeptAnswer {
    fn descriptor() -> TableDescriptor<KeptAnswer> {
        TableDescriptor::new(
            KEPT_ANSWER_TABLE_NAME,
            FamilyName::new(KEPT_ANSWER_FAMILY),
            SchemaHash::for_label(KEPT_ANSWER_SCHEMA),
        )
    }
}

/// Where an answer goes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum AnswerRoute {
    /// The message answers no request.
    Unanswering,
    /// The sender of the request it answers.
    To(Address),
    /// The request's sender is no Address: the answer is kept with it.
    Kept(RequestIdBinding),
}

/// The marker a kept answer closes its request with, in place of the
/// logical delivery an answer delivered elsewhere has.
pub(crate) trait MarksKeptAnswer {
    fn kept_marker(&self) -> signal_flow::LogicalDeliveryId;
}

impl MarksKeptAnswer for RequestId {
    fn kept_marker(&self) -> signal_flow::LogicalDeliveryId {
        signal_flow::LogicalDeliveryId(format!("kept:{}", self.0))
    }
}

pub(crate) trait RoutesAnswers {
    /// Where `message` goes when it answers a request: the sender of that
    /// request, or kept with it. The binding is the request's as it stands.
    fn answer_route(&self, message: &Message) -> Result<AnswerRoute, StoreError>;
    /// Keeps `answer`, sent by `answerer`, with the request `binding`
    /// names, and closes that request, a Task's reminder with it, in one
    /// commit.
    fn keep_answer(
        &self,
        binding: RequestIdBinding,
        answerer: &SentBy,
        answer: &Message,
    ) -> Result<(), StoreError>;
    /// The answer kept with the request `request_id`, if one is.
    fn kept_answer(&self, request_id: &RequestId) -> Result<Option<KeptAnswer>, StoreError>;
}

impl RoutesAnswers for FlowStore {
    fn answer_route(&self, message: &Message) -> Result<AnswerRoute, StoreError> {
        let request = match message {
            Message::HardAbrupt(request)
            | Message::MiddleAbrupt(request)
            | Message::Soft(request) => request,
        };
        let answered = match request {
            Request::Result(flow_ethos::ResultPayload::Answered(answer)) => &answer.request_id,
            Request::Done(flow_ethos::DonePayload::Answered(answer)) => &answer.request_id,
            _ => return Ok(AnswerRoute::Unanswering),
        };
        let Some(binding) = self.request_binding(answered)? else {
            return Ok(AnswerRoute::Unanswering);
        };
        let sender = self
            .retained_logical_requests()?
            .into_iter()
            .find(|retained| retained.entry.logical_delivery_id == binding.logical_delivery_id)
            .map(|retained| retained.entry.sent_by);
        Ok(match sender {
            Some(SentBy::Sender(address)) => AnswerRoute::To(address),
            Some(SentBy::Unbound | SentBy::Flow | SentBy::Unknown) => AnswerRoute::Kept(binding),
            None => AnswerRoute::Unanswering,
        })
    }

    fn keep_answer(
        &self,
        mut binding: RequestIdBinding,
        answerer: &SentBy,
        answer: &Message,
    ) -> Result<(), StoreError> {
        use crate::message_projection::ProjectsAdmittedMessage;
        let _guard = self
            .logical_deliveries
            .write_gate
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let request_id = binding.request_id.clone();
        let kept = KeptAnswer {
            request_id: request_id.clone(),
            visible_native_input: answer.admitted_input(&request_id, answerer),
        };
        binding.request_closure = RequestClosure::Answered(request_id.kept_marker());
        let reminder_key = RecordKey::new(binding.logical_delivery_id.0.clone());
        let task_reminder = !self
            .engine
            .match_records(QueryPlan::key(self.reminders, reminder_key.clone()))?
            .records()
            .is_empty();
        let commit = self
            .engine
            .begin_atomic_commit()
            .mutate(self.logical_deliveries.request_ids, binding)
            .assert(self.kept_answers, kept);
        let commit = if task_reminder {
            commit.retract(self.reminders, reminder_key)
        } else {
            commit
        };
        self.engine.commit_atomic(commit)?;
        self.launch_changes.announce();
        Ok(())
    }

    fn kept_answer(&self, request_id: &RequestId) -> Result<Option<KeptAnswer>, StoreError> {
        Ok(self
            .engine
            .match_records(QueryPlan::key(
                self.kept_answers,
                RecordKey::new(request_id.0.clone()),
            ))?
            .records()
            .first()
            .cloned())
    }
}
