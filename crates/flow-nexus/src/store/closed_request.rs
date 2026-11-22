//! A request closed by CloseRequest: archived with no answer and no one
//! told, the process that closed it recorded beside it. The family is new
//! in 0.60.0: a store written before holds none and is opened with it
//! registered empty.

use super::FlowStore;
#[cfg(test)]
use super::StoreError;
pub(crate) use crate::generated::memory::{ClosedRequest, Closer};
#[cfg(test)]
use sema_engine::QueryPlan;
use sema_engine::{
    AtomicCommit, EngineRecord, FamilyName, MemoryDefinition, MemoryVersion, RecordKey, SchemaHash,
    TableDescriptor, TableName,
};
use signal_flow::{LogicalDeliveryId, RequestId};

pub(super) const CLOSED_REQUEST_TABLE: &str = "flow_nexus_closed_requests";
pub(super) const CLOSED_REQUEST_FAMILY: &str = "flow-nexus-closed-request";
pub(super) const CLOSED_REQUEST_SCHEMA: &str = "flow-nexus-closed-request-v1";

impl EngineRecord for ClosedRequest {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.logical_delivery_id.0.clone())
    }
}

impl MemoryDefinition for ClosedRequest {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(1)
    }

    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label(CLOSED_REQUEST_SCHEMA)
    }
}

pub(super) trait DescribesClosedRequestTable {
    fn descriptor() -> TableDescriptor<ClosedRequest>;
}

impl DescribesClosedRequestTable for ClosedRequest {
    fn descriptor() -> TableDescriptor<ClosedRequest> {
        TableDescriptor::new(
            TableName::new(CLOSED_REQUEST_TABLE),
            FamilyName::new(CLOSED_REQUEST_FAMILY),
            SchemaHash::for_label(CLOSED_REQUEST_SCHEMA),
        )
    }
}

/// How an open request is ended with no answer. Cancel withdraws
/// unreached work; CloseRequest records its caller without sending a prompt.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum RequestEnding {
    Cancelled(Option<signal_flow::Address>),
    Closed(Closer),
}

pub(crate) trait MarksEndedReminder {
    /// The marker a reminder not yet sent is closed by, in place of the
    /// logical delivery that would have carried it.
    fn reminder_marker(&self, request_id: &RequestId) -> LogicalDeliveryId;
}

impl MarksEndedReminder for RequestEnding {
    fn reminder_marker(&self, request_id: &RequestId) -> LogicalDeliveryId {
        match self {
            RequestEnding::Cancelled(_) => LogicalDeliveryId(format!("cancelled:{}", request_id.0)),
            RequestEnding::Closed(_) => LogicalDeliveryId(format!("closed:{}", request_id.0)),
        }
    }
}

pub(crate) trait RecordsClosedRequests {
    /// The commit an ending of the request bound to `logical_delivery_id`
    /// begins with: a closed request's record of who closed it, nothing
    /// for a cancelled one.
    fn ending_commit(
        &self,
        ending: &RequestEnding,
        logical_delivery_id: &LogicalDeliveryId,
        request_id: &RequestId,
    ) -> AtomicCommit;
    /// The record of the request bound to `logical_delivery_id`, when
    /// CloseRequest closed it.
    #[cfg(test)]
    fn closed_request(
        &self,
        logical_delivery_id: &LogicalDeliveryId,
    ) -> Result<Option<ClosedRequest>, StoreError>;
}

impl RecordsClosedRequests for FlowStore {
    fn ending_commit(
        &self,
        ending: &RequestEnding,
        logical_delivery_id: &LogicalDeliveryId,
        request_id: &RequestId,
    ) -> AtomicCommit {
        let commit = self.engine.begin_atomic_commit();
        match ending {
            RequestEnding::Cancelled(_) => commit,
            RequestEnding::Closed(closer) => commit.assert(
                self.closed_requests,
                ClosedRequest {
                    logical_delivery_id: logical_delivery_id.clone(),
                    request_id: request_id.clone(),
                    closer: closer.clone(),
                },
            ),
        }
    }

    #[cfg(test)]
    fn closed_request(
        &self,
        logical_delivery_id: &LogicalDeliveryId,
    ) -> Result<Option<ClosedRequest>, StoreError> {
        Ok(self
            .engine
            .match_records(QueryPlan::key(
                self.closed_requests,
                RecordKey::new(logical_delivery_id.0.clone()),
            ))?
            .records()
            .first()
            .cloned())
    }
}
