//! The Codex thread a launch opened through the app-server before its TUI
//! attached. A remote-resumed Codex TUI never reports its session to Herdr,
//! so a launch recovered after its harness start binds to this thread, the
//! recognized pane and the flow's claim. The row is keyed by the launch
//! request, written once, in its own table, so a store written before it
//! opens with the table added and holds none.
//!
//! The row is written after the thread opens and before its first turn is
//! started, so it is also the thread's authority while its launch runs: a
//! send call the first turn makes is sent from the Address of the launch
//! that opened the thread (`store::send_call`).

use super::{FlowStore, StoreError};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use sema_engine::{
    Assertion, EngineRecord, FamilyName, QueryPlan, RecordKey, SchemaHash, TableDescriptor,
    TableName,
};

pub(super) const OPENED_THREAD_TABLE_NAME: TableName =
    TableName::new("flow_nexus_launch_opened_threads");
pub(super) const OPENED_THREAD_FAMILY: &str = "flow-nexus-launch-opened-thread";
pub(super) const OPENED_THREAD_SCHEMA: &str = "flow-nexus-launch-opened-thread-v1";

/// The thread a launch request's Spawn opened.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub struct OpenedLaunchThread {
    pub launch_request_id: String,
    pub native_session_id: String,
}

impl EngineRecord for OpenedLaunchThread {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.launch_request_id.clone())
    }
}

pub(super) trait DescribesOpenedThreadTable {
    fn descriptor() -> TableDescriptor<OpenedLaunchThread>;
}

impl DescribesOpenedThreadTable for OpenedLaunchThread {
    fn descriptor() -> TableDescriptor<OpenedLaunchThread> {
        TableDescriptor::new(
            OPENED_THREAD_TABLE_NAME,
            FamilyName::new(OPENED_THREAD_FAMILY),
            SchemaHash::for_label(OPENED_THREAD_SCHEMA),
        )
    }
}

pub(crate) trait RecordsOpenedThreads {
    /// Records the thread a launch request opened: true once it is held
    /// (the same thread again is left as it is), false when the request
    /// already holds another thread.
    fn record_opened_thread(
        &self,
        launch_request_id: &str,
        native_session_id: &str,
    ) -> Result<bool, StoreError>;
    /// The thread a launch request opened, when one was recorded.
    fn opened_thread(&self, launch_request_id: &str) -> Result<Option<String>, StoreError>;
    /// The launch request that opened `native_session_id`, when one did.
    fn thread_launch(&self, native_session_id: &str) -> Result<Option<String>, StoreError>;
}

impl RecordsOpenedThreads for FlowStore {
    fn record_opened_thread(
        &self,
        launch_request_id: &str,
        native_session_id: &str,
    ) -> Result<bool, StoreError> {
        match self.opened_thread(launch_request_id)? {
            Some(held) => Ok(held == native_session_id),
            None => {
                self.engine.assert(Assertion::new(
                    self.opened_threads,
                    OpenedLaunchThread {
                        launch_request_id: launch_request_id.to_owned(),
                        native_session_id: native_session_id.to_owned(),
                    },
                ))?;
                Ok(true)
            }
        }
    }

    fn opened_thread(&self, launch_request_id: &str) -> Result<Option<String>, StoreError> {
        let records = self
            .engine
            .match_records(QueryPlan::key(
                self.opened_threads,
                RecordKey::new(launch_request_id),
            ))?
            .records()
            .to_vec();
        match records.as_slice() {
            [] => Ok(None),
            [opened] => Ok(Some(opened.native_session_id.clone())),
            _ => Err(StoreError::StateInvariant),
        }
    }

    fn thread_launch(&self, native_session_id: &str) -> Result<Option<String>, StoreError> {
        let launches = self
            .engine
            .match_records(QueryPlan::all(self.opened_threads))?
            .records()
            .iter()
            .filter(|opened| {
                !native_session_id.is_empty() && opened.native_session_id == native_session_id
            })
            .map(|opened| opened.launch_request_id.clone())
            .collect::<Vec<_>>();
        match launches.as_slice() {
            [] => Ok(None),
            [launch] => Ok(Some(launch.clone())),
            _ => Err(StoreError::StateInvariant),
        }
    }
}
