//! How big each flow's context was when its harness last stopped: the
//! tokens its native session carried into its last turn, read from the
//! Claude session's transcript when the flow's hook reports `Stopped`, or
//! from a Codex thread's token usage as its app-server reports it. One row
//! per flow, keyed by its FlowId, rewritten at each reading. It is judged
//! against the refresh threshold of the flow's model: a flow at or past it
//! is given no new message and is replaced as it goes idle.
//!
//! The row lives in its own table, so a store written before context sizes
//! were kept opens with the table added and its flows simply have none.

use super::{FlowStore, ManagesMetaflowState, ReadsFlowRoles, ReadsLaunchAttempt, StoreError};
use meta_signal_flow::{CompactionRefusal, ContextBelowThreshold};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use sema_engine::{
    Assertion, EngineRecord, FamilyName, KeyedMutation, QueryPlan, RecordKey, SchemaHash,
    TableDescriptor, TableName, TableReference,
};

pub(super) const FLOW_CONTEXT_SIZE_TABLE_NAME: TableName =
    TableName::new("flow_nexus_flow_context_sizes");
pub(super) const FLOW_CONTEXT_SIZE_FAMILY: &str = "flow-nexus-flow-context-size";
pub(super) const FLOW_CONTEXT_SIZE_SCHEMA: &str = "flow-nexus-flow-context-size-v1";

/// One flow's context size at its last Stop, in tokens.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub struct FlowContextSize {
    pub flow_id: String,
    pub tokens: i64,
}

impl EngineRecord for FlowContextSize {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.flow_id.clone())
    }
}

pub(super) trait DescribesContextSizeTable {
    fn descriptor() -> TableDescriptor<FlowContextSize>;
}

impl DescribesContextSizeTable for FlowContextSize {
    fn descriptor() -> TableDescriptor<FlowContextSize> {
        TableDescriptor::new(
            FLOW_CONTEXT_SIZE_TABLE_NAME,
            FamilyName::new(FLOW_CONTEXT_SIZE_FAMILY),
            SchemaHash::for_label(FLOW_CONTEXT_SIZE_SCHEMA),
        )
    }
}

pub trait RecordsContextSize {
    /// Writes the flow's context size at its Stop, replacing the last one.
    fn record_context_size(&self, flow_id: &str, tokens: i64) -> Result<(), StoreError>;
    /// The flow's context size at its last Stop; None when none was read.
    fn context_size(&self, flow_id: &str) -> Result<Option<i64>, StoreError>;
}

trait ReadsContextSizeRow {
    fn context_size_row(&self, flow_id: &str) -> Result<Option<FlowContextSize>, StoreError>;
    fn context_sizes(&self) -> TableReference<FlowContextSize>;
}

impl ReadsContextSizeRow for FlowStore {
    fn context_sizes(&self) -> TableReference<FlowContextSize> {
        self.context_sizes
    }

    fn context_size_row(&self, flow_id: &str) -> Result<Option<FlowContextSize>, StoreError> {
        let records = self
            .engine
            .match_records(QueryPlan::key(
                self.context_sizes(),
                RecordKey::new(flow_id),
            ))?
            .records()
            .to_vec();
        match records.as_slice() {
            [] => Ok(None),
            [row] => Ok(Some(row.clone())),
            _ => Err(StoreError::StateInvariant),
        }
    }
}

impl RecordsContextSize for FlowStore {
    fn record_context_size(&self, flow_id: &str, tokens: i64) -> Result<(), StoreError> {
        let row = FlowContextSize {
            flow_id: flow_id.into(),
            tokens,
        };
        if self.context_size_row(flow_id)?.is_some() {
            self.engine.mutate_keyed(KeyedMutation::new(
                self.context_sizes(),
                RecordKey::new(flow_id),
                row,
            ))?;
        } else {
            self.engine
                .assert(Assertion::new(self.context_sizes(), row))?;
        }
        Ok(())
    }

    fn context_size(&self, flow_id: &str) -> Result<Option<i64>, StoreError> {
        Ok(self.context_size_row(flow_id)?.map(|row| row.tokens))
    }
}

/// The model a flow runs: the one its role records, else the one its
/// launch named.
pub trait NamesFlowModel {
    fn flow_model(&self, flow_id: &str) -> Result<Option<String>, StoreError>;
}

impl NamesFlowModel for FlowStore {
    fn flow_model(&self, flow_id: &str) -> Result<Option<String>, StoreError> {
        if let Some(role) = self.role(flow_id)? {
            return Ok(Some(role.model_name));
        }
        Ok(match self.launch_requests_bound_to(flow_id)?.pop() {
            Some(launch_request_id) => self
                .launch_attempt(&launch_request_id)?
                .map(|attempt| attempt.launch_profile.model_name),
            None => None,
        })
    }
}

/// Where a flow's last measured context stands against its model's
/// refresh threshold.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ThresholdStanding {
    /// Its model has no threshold configured, or Flow knows no model for it.
    NoThreshold,
    /// No context size is recorded for it.
    Unmeasured,
    /// Below the threshold.
    Below { context: i64, refresh: i64 },
    /// At or past the threshold: it is given no new message and is
    /// replaced as it goes idle.
    Past { context: i64, refresh: i64 },
}

pub trait JudgesThreshold {
    fn threshold_standing(&self, flow_id: &str) -> Result<ThresholdStanding, StoreError>;
    /// Whether the flow is at or past its model's refresh threshold.
    fn past_threshold(&self, flow_id: &str) -> Result<bool, StoreError> {
        Ok(matches!(
            self.threshold_standing(flow_id)?,
            ThresholdStanding::Past { .. }
        ))
    }
}

impl JudgesThreshold for FlowStore {
    fn threshold_standing(&self, flow_id: &str) -> Result<ThresholdStanding, StoreError> {
        let threshold = match self.flow_model(flow_id)? {
            Some(model) => self.configured_threshold(&model)?,
            None => None,
        };
        let Some(threshold) = threshold else {
            return Ok(ThresholdStanding::NoThreshold);
        };
        let Some(context) = self.context_size(flow_id)? else {
            return Ok(ThresholdStanding::Unmeasured);
        };
        let refresh = threshold.refresh;
        Ok(if context < refresh {
            ThresholdStanding::Below { context, refresh }
        } else {
            ThresholdStanding::Past { context, refresh }
        })
    }
}

/// Compaction is an emergency measure, refused while the context is below
/// a threshold: a flow is compacted only when its last measured context is
/// at or above the refresh threshold configured for its model, the point
/// past which the flow should already have been refreshed.
pub trait JudgesCompaction {
    /// Why a Compact of this flow is refused; None when it may compact.
    fn compaction_refusal(&self, flow_id: &str) -> Result<Option<CompactionRefusal>, StoreError>;
}

impl JudgesCompaction for FlowStore {
    fn compaction_refusal(&self, flow_id: &str) -> Result<Option<CompactionRefusal>, StoreError> {
        Ok(match self.threshold_standing(flow_id)? {
            ThresholdStanding::NoThreshold => Some(CompactionRefusal::NoThreshold),
            ThresholdStanding::Unmeasured => Some(CompactionRefusal::Unmeasured),
            ThresholdStanding::Below { context, refresh } => {
                Some(CompactionRefusal::BelowThreshold(ContextBelowThreshold {
                    context_tokens: context,
                    refresh_tokens: refresh,
                }))
            }
            ThresholdStanding::Past { .. } => None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::RecordsContextSize;
    use crate::store::{FlowStore, OpensFlowStore};

    #[test]
    fn a_flow_context_size_is_rewritten_at_each_stop_and_kept_across_a_reopen() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("flow.sema");
        {
            let store = FlowStore::open(&path).unwrap();
            assert_eq!(store.context_size("5a4d0b").unwrap(), None);
            store.record_context_size("5a4d0b", 120_000).unwrap();
            store.record_context_size("5a4d0b", 151_000).unwrap();
        }
        let store = FlowStore::open(&path).unwrap();
        assert_eq!(store.context_size("5a4d0b").unwrap(), Some(151_000));
        assert_eq!(store.context_size("0a0a0a").unwrap(), None);
    }
}
