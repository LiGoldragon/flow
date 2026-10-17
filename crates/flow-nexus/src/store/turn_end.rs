//! Durable native-turn completion requests.
//!
//! The Stop hook runs before Codex appends its final `task_complete` row.
//! Flow therefore keeps the exact session, turn and transcript it was
//! asked to observe.  A completed row is retained so a repeated hook cannot
//! cause the same native turn to drain work twice.

use super::{FlowStore, ReadsFlowStore, StoreError};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use sema_engine::{
    Assertion, EngineRecord, FamilyName, KeyedMutation, QueryPlan, RecordKey, SchemaHash,
    TableDescriptor, TableName,
};
use signal_flow::TurnEndRequest;

const TURN_END_TABLE_NAME: TableName = TableName::new("flow_nexus_turn_ends");

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub enum TurnEndState {
    Pending,
    Completed,
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub struct TurnEndRecord {
    pub flow_id: String,
    pub request: TurnEndRequest,
    pub state: TurnEndState,
}

impl TurnEndRecord {
    fn key_of(request: &TurnEndRequest) -> RecordKey {
        RecordKey::new(format!(
            "{}:{}{}",
            request.session_id.len(),
            request.session_id,
            request.turn_id
        ))
    }
}

impl EngineRecord for TurnEndRecord {
    fn record_key(&self) -> RecordKey {
        Self::key_of(&self.request)
    }
}

pub trait DescribesTurnEndTable {
    fn descriptor() -> TableDescriptor<TurnEndRecord>;
}

impl DescribesTurnEndTable for TurnEndRecord {
    fn descriptor() -> TableDescriptor<TurnEndRecord> {
        TableDescriptor::new(
            TURN_END_TABLE_NAME,
            FamilyName::new("flow-nexus-turn-end"),
            SchemaHash::for_label("flow-nexus-turn-end-v1"),
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TurnEndQueue {
    Queued,
    AlreadyQueued,
    Conflict,
}

pub trait KeepsTurnEnds {
    fn flow_for_session(&self, session_id: &str) -> Result<Option<String>, StoreError>;
    fn queue_turn_end(
        &self,
        flow_id: &str,
        request: TurnEndRequest,
    ) -> Result<TurnEndQueue, StoreError>;
    fn pending_turn_ends(&self) -> Result<Vec<TurnEndRecord>, StoreError>;
    fn complete_turn_end(&self, request: &TurnEndRequest) -> Result<bool, StoreError>;
    fn has_completed_turn(&self, flow_id: &str) -> Result<bool, StoreError>;
}

impl KeepsTurnEnds for FlowStore {
    fn flow_for_session(&self, session_id: &str) -> Result<Option<String>, StoreError> {
        let flows = self.engine.match_records(QueryPlan::all(self.flows))?;
        let mut matching = flows
            .records()
            .iter()
            .filter(|flow| flow.thread_id.as_deref() == Some(session_id));
        let first = matching.next().map(|flow| flow.flow_id.clone());
        if matching.next().is_some() {
            return Err(StoreError::StateInvariant);
        }
        Ok(first)
    }

    fn queue_turn_end(
        &self,
        flow_id: &str,
        request: TurnEndRequest,
    ) -> Result<TurnEndQueue, StoreError> {
        let key = TurnEndRecord::key_of(&request);
        let records = self
            .engine
            .match_records(QueryPlan::key(self.turn_ends, key))?
            .records()
            .to_vec();
        match records.as_slice() {
            [] => {
                self.engine.assert(Assertion::new(
                    self.turn_ends,
                    TurnEndRecord {
                        flow_id: flow_id.to_owned(),
                        request,
                        state: TurnEndState::Pending,
                    },
                ))?;
                Ok(TurnEndQueue::Queued)
            }
            [record] if record.flow_id == flow_id && record.request == request => {
                Ok(TurnEndQueue::AlreadyQueued)
            }
            [_] => Ok(TurnEndQueue::Conflict),
            _ => Err(StoreError::StateInvariant),
        }
    }

    fn pending_turn_ends(&self) -> Result<Vec<TurnEndRecord>, StoreError> {
        Ok(self
            .engine
            .match_records(QueryPlan::all(self.turn_ends))?
            .records()
            .iter()
            .filter(|record| record.state == TurnEndState::Pending)
            .cloned()
            .collect())
    }

    fn complete_turn_end(&self, request: &TurnEndRequest) -> Result<bool, StoreError> {
        let key = TurnEndRecord::key_of(request);
        let records = self
            .engine
            .match_records(QueryPlan::key(self.turn_ends, key.clone()))?
            .records()
            .to_vec();
        let [record] = records.as_slice() else {
            return Ok(false);
        };
        if record.request != *request || record.state == TurnEndState::Completed {
            return Ok(false);
        }
        let mut completed = record.clone();
        completed.state = TurnEndState::Completed;
        self.engine
            .mutate_keyed(KeyedMutation::new(self.turn_ends, key, completed))?;
        Ok(true)
    }

    fn has_completed_turn(&self, flow_id: &str) -> Result<bool, StoreError> {
        Ok(self
            .engine
            .match_records(QueryPlan::all(self.turn_ends))?
            .records()
            .iter()
            .any(|record| record.flow_id == flow_id && record.state == TurnEndState::Completed))
    }
}
