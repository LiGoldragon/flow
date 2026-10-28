//! Narrow operational state exposed to configured effects.
//!
//! This facade deliberately has no configuration read or raw store escape.

use crate::generated::send_tool::Target;
use crate::generated::send_tool::{SendCall, SendOutcome, SendRefusal, ToolCallKey};
use crate::store::logical_delivery::RecordsLogicalDeliveries;
use crate::store::opened_thread::RecordsOpenedThreads;
use crate::store::send_call::KeepsSendCalls;
use crate::store::{FlowStore, ManagesMetaflowState, ReadsLaunchAttempt, StoreError};
use meta_signal_flow::Sender;
use signal_flow::{Address, Delivery, LaunchAttempt, Message, Metaflow};

#[derive(Clone, Copy)]
pub(crate) struct OperationalStore<'a> {
    store: &'a FlowStore,
}

impl<'a> OperationalStore<'a> {
    pub(super) fn new(store: &'a FlowStore) -> Self {
        Self { store }
    }

    pub(crate) fn thread_sender(
        &self,
        thread: &str,
    ) -> Result<Result<Address, SendRefusal>, StoreError> {
        self.store.thread_sender(thread)
    }
    pub(crate) fn list_metaflows(&self) -> Result<Vec<Metaflow>, StoreError> {
        self.store.list_metaflows()
    }
    pub(crate) fn thread_launch(&self, thread: &str) -> Result<Option<String>, StoreError> {
        self.store.thread_launch(thread)
    }
    pub(crate) fn launch_attempt(
        &self,
        request: &str,
    ) -> Result<Option<LaunchAttempt>, StoreError> {
        self.store.launch_attempt(request)
    }
    pub(crate) fn send_call(&self, key: &ToolCallKey) -> Result<Option<SendCall>, StoreError> {
        self.store.send_call(key)
    }
    pub(crate) fn record_send_call(&self, call: SendCall) -> Result<(), StoreError> {
        self.store.record_send_call(call)
    }
    pub(crate) fn accept_tool_send(
        &self,
        key: &ToolCallKey,
        content_sha256: &str,
        sender: Sender,
        target: &Target,
        delivery: Delivery,
        message: Message,
        now: i64,
    ) -> Result<SendOutcome, StoreError> {
        self.store
            .accept_tool_send(key, content_sha256, sender, target, delivery, message, now)
    }
}
