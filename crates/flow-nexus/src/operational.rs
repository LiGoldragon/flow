//! Narrow operational state exposed to configured effects.
//!
//! This facade deliberately has no configuration read or raw store escape.

use crate::generated::send_tool::Target;
use crate::generated::send_tool::{SendCall, SendOutcome, SendRefusal, ToolCallKey};
use crate::store::context::RecordsContextSize;
use crate::store::events::{EventRecording, RecordsHarnessEvents};
use crate::store::logical_delivery::RecordsLogicalDeliveries;
use crate::store::metaflow::{ResolvesRemembering, SleepsMetaflows};
use crate::store::opened_thread::RecordsOpenedThreads;
use crate::store::send_call::KeepsSendCalls;
use crate::store::{
    ConfirmsStartedFlow, FlowRegistration, FlowStore, LaunchOutcome, ManagesMetaflowState,
    ReadsFlowRows, ReadsLaunchAttempt, RecordsFlowLifecycle, RecordsLaunchOutcome,
    RecordsNativeLaunchBinding, RecordsNativeLaunchIntent, RecordsPromptDeliveryIntent,
    RecordsPromptDeliveryResult, RecordsRegistrationAcknowledgement, RecordsReplacement,
    RegistersFlowIdentity, Replacement, ReservesLaunchAttempt, StoreError,
};
use meta_signal_flow::Sender;
use signal_flow::{
    Address, Caller, ComposedLaunch, Delivery, Event, FlowNode, LaunchAttempt,
    LaunchAttemptReservation, LaunchProfile, Message, Metaflow, NativeLaunchBinding,
    NativeLaunchIntent, Origin, PromptDeliveryIntent, PromptDeliveryResult,
    RegistrationAcknowledgement, Response,
};

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
    pub(crate) fn remembered_flows(
        &self,
        profile: &LaunchProfile,
    ) -> Result<Vec<String>, StoreError> {
        self.store.remembered_flows(profile)
    }
    pub(crate) fn register_flow_in_role(
        &self,
        node: FlowNode,
        role: Caller,
    ) -> Result<FlowRegistration, StoreError> {
        self.store.register_flow_in_role(node, role)
    }
    pub(crate) fn confirm_started(&self, flow_id: &str) -> Result<Response, StoreError> {
        self.store.confirm_started(flow_id)
    }
    pub(crate) fn record_opened_thread(
        &self,
        request: &str,
        thread: &str,
    ) -> Result<bool, StoreError> {
        self.store.record_opened_thread(request, thread)
    }
    pub(crate) fn reserve_launch_attempt(
        &self,
        launch: &ComposedLaunch,
        origin: Origin,
    ) -> Result<LaunchAttemptReservation, StoreError> {
        self.store.reserve_launch_attempt(launch, origin)
    }
    pub(crate) fn hold_reserved_flow(&self, flow_id: &str) -> Result<(), StoreError> {
        self.store.hold_reserved_flow(flow_id)
    }
    pub(crate) fn release_reserved_flow(&self, flow_id: &str) -> Result<(), StoreError> {
        self.store.release_reserved_flow(flow_id)
    }
    pub(crate) fn record_native_launch_intent(
        &self,
        value: NativeLaunchIntent,
    ) -> Result<bool, StoreError> {
        self.store.record_native_launch_intent(value)
    }
    pub(crate) fn record_native_launch_binding(
        &self,
        value: NativeLaunchBinding,
    ) -> Result<bool, StoreError> {
        self.store.record_native_launch_binding(value)
    }
    pub(crate) fn record_registration_acknowledgement(
        &self,
        value: RegistrationAcknowledgement,
    ) -> Result<bool, StoreError> {
        self.store.record_registration_acknowledgement(value)
    }
    pub(crate) fn record_prompt_delivery_intent(
        &self,
        value: PromptDeliveryIntent,
    ) -> Result<bool, StoreError> {
        self.store.record_prompt_delivery_intent(value)
    }
    pub(crate) fn record_prompt_delivery_result(
        &self,
        value: PromptDeliveryResult,
    ) -> Result<bool, StoreError> {
        self.store.record_prompt_delivery_result(value)
    }
    pub(crate) fn record_active(&self, flow_id: &str) -> Result<bool, StoreError> {
        self.store.record_active(flow_id)
    }
    pub(crate) fn record_stopped(&self, flow_id: &str) -> Result<bool, StoreError> {
        self.store.record_stopped(flow_id)
    }
    pub(crate) fn record_retired(&self, flow_id: &str) -> Result<bool, StoreError> {
        self.store.record_retired(flow_id)
    }
    pub(crate) fn record_exited(&self, flow_id: &str) -> Result<bool, StoreError> {
        self.store.record_exited(flow_id)
    }
    pub(crate) fn flow_node(&self, flow_id: &str) -> Result<Option<FlowNode>, StoreError> {
        self.store.flow_node(flow_id)
    }
    pub(crate) fn record_replacement(&self, value: Replacement) -> Result<(), StoreError> {
        self.store.record_replacement(value)
    }
    pub(crate) fn withdraw_replacement(&self, request: &str) -> Result<(), StoreError> {
        self.store.withdraw_replacement(request)
    }
    pub(crate) fn record_launch_outcome(
        &self,
        request: &str,
        outcome: LaunchOutcome,
    ) -> Result<(), StoreError> {
        self.store.record_launch_outcome(request, outcome)
    }
    pub(crate) fn sleep_metaflow(
        &self,
        address: &Address,
        flow_id: &str,
    ) -> Result<bool, StoreError> {
        self.store.sleep_metaflow(address, flow_id)
    }
    pub(crate) fn awaken_metaflow(
        &self,
        address: &Address,
        flow_id: &str,
        delivered: usize,
    ) -> Result<bool, StoreError> {
        self.store.awaken_metaflow(address, flow_id, delivered)
    }
    pub(crate) fn record_context_size(&self, flow_id: &str, tokens: i64) -> Result<(), StoreError> {
        self.store.record_context_size(flow_id, tokens)
    }
    pub(crate) fn record_event(
        &self,
        flow_id: &str,
        event: Event,
    ) -> Result<EventRecording, StoreError> {
        self.store.record_event(flow_id, event)
    }
}
