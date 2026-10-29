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

pub(crate) trait RecordsOperationalState<'a> {
    fn new(store: &'a FlowStore) -> Self;
    fn thread_sender(&self, thread: &str) -> Result<Result<Address, SendRefusal>, StoreError>;
    fn list_metaflows(&self) -> Result<Vec<Metaflow>, StoreError>;
    fn thread_launch(&self, thread: &str) -> Result<Option<String>, StoreError>;
    fn launch_attempt(&self, request: &str) -> Result<Option<LaunchAttempt>, StoreError>;
    fn send_call(&self, key: &ToolCallKey) -> Result<Option<SendCall>, StoreError>;
    fn record_send_call(&self, call: SendCall) -> Result<(), StoreError>;
    fn accept_tool_send(
        &self,
        key: &ToolCallKey,
        content_sha256: &str,
        sender: Sender,
        target: &Target,
        delivery: Delivery,
        message: Message,
        now: i64,
    ) -> Result<SendOutcome, StoreError>;
    fn remembered_flows(&self, profile: &LaunchProfile) -> Result<Vec<String>, StoreError>;
    fn register_flow_in_role(
        &self,
        node: FlowNode,
        role: Caller,
    ) -> Result<FlowRegistration, StoreError>;
    fn confirm_started(&self, flow_id: &str) -> Result<Response, StoreError>;
    fn record_opened_thread(&self, request: &str, thread: &str) -> Result<bool, StoreError>;
    fn reserve_launch_attempt(
        &self,
        launch: &ComposedLaunch,
        origin: Origin,
    ) -> Result<LaunchAttemptReservation, StoreError>;
    fn hold_reserved_flow(&self, flow_id: &str) -> Result<(), StoreError>;
    fn release_reserved_flow(&self, flow_id: &str) -> Result<(), StoreError>;
    fn record_native_launch_intent(&self, value: NativeLaunchIntent) -> Result<bool, StoreError>;
    fn record_native_launch_binding(&self, value: NativeLaunchBinding) -> Result<bool, StoreError>;
    fn record_registration_acknowledgement(
        &self,
        value: RegistrationAcknowledgement,
    ) -> Result<bool, StoreError>;
    fn record_prompt_delivery_intent(
        &self,
        value: PromptDeliveryIntent,
    ) -> Result<bool, StoreError>;
    fn record_prompt_delivery_result(
        &self,
        value: PromptDeliveryResult,
    ) -> Result<bool, StoreError>;
    fn record_active(&self, flow_id: &str) -> Result<bool, StoreError>;
    fn record_stopped(&self, flow_id: &str) -> Result<bool, StoreError>;
    fn record_retired(&self, flow_id: &str) -> Result<bool, StoreError>;
    fn record_exited(&self, flow_id: &str) -> Result<bool, StoreError>;
    fn flow_node(&self, flow_id: &str) -> Result<Option<FlowNode>, StoreError>;
    fn flow_nodes(&self) -> Result<Vec<FlowNode>, StoreError>;
    fn record_replacement(&self, value: Replacement) -> Result<(), StoreError>;
    fn withdraw_replacement(&self, request: &str) -> Result<(), StoreError>;
    fn record_launch_outcome(
        &self,
        request: &str,
        outcome: LaunchOutcome,
    ) -> Result<(), StoreError>;
    fn sleep_metaflow(&self, address: &Address, flow_id: &str) -> Result<bool, StoreError>;
    fn awaken_metaflow(
        &self,
        address: &Address,
        flow_id: &str,
        delivered: usize,
    ) -> Result<bool, StoreError>;
    fn record_context_size(&self, flow_id: &str, tokens: i64) -> Result<(), StoreError>;
    fn record_event(&self, flow_id: &str, event: Event) -> Result<EventRecording, StoreError>;
}

impl<'a> RecordsOperationalState<'a> for OperationalStore<'a> {
    fn new(store: &'a FlowStore) -> Self {
        Self { store }
    }

    fn thread_sender(&self, thread: &str) -> Result<Result<Address, SendRefusal>, StoreError> {
        self.store.thread_sender(thread)
    }
    fn list_metaflows(&self) -> Result<Vec<Metaflow>, StoreError> {
        self.store.list_metaflows()
    }
    fn thread_launch(&self, thread: &str) -> Result<Option<String>, StoreError> {
        self.store.thread_launch(thread)
    }
    fn launch_attempt(&self, request: &str) -> Result<Option<LaunchAttempt>, StoreError> {
        self.store.launch_attempt(request)
    }
    fn send_call(&self, key: &ToolCallKey) -> Result<Option<SendCall>, StoreError> {
        self.store.send_call(key)
    }
    fn record_send_call(&self, call: SendCall) -> Result<(), StoreError> {
        self.store.record_send_call(call)
    }
    fn accept_tool_send(
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
    fn remembered_flows(&self, profile: &LaunchProfile) -> Result<Vec<String>, StoreError> {
        self.store.remembered_flows(profile)
    }
    fn register_flow_in_role(
        &self,
        node: FlowNode,
        role: Caller,
    ) -> Result<FlowRegistration, StoreError> {
        self.store.register_flow_in_role(node, role)
    }
    fn confirm_started(&self, flow_id: &str) -> Result<Response, StoreError> {
        self.store.confirm_started(flow_id)
    }
    fn record_opened_thread(&self, request: &str, thread: &str) -> Result<bool, StoreError> {
        self.store.record_opened_thread(request, thread)
    }
    fn reserve_launch_attempt(
        &self,
        launch: &ComposedLaunch,
        origin: Origin,
    ) -> Result<LaunchAttemptReservation, StoreError> {
        self.store.reserve_launch_attempt(launch, origin)
    }
    fn hold_reserved_flow(&self, flow_id: &str) -> Result<(), StoreError> {
        self.store.hold_reserved_flow(flow_id)
    }
    fn release_reserved_flow(&self, flow_id: &str) -> Result<(), StoreError> {
        self.store.release_reserved_flow(flow_id)
    }
    fn record_native_launch_intent(&self, value: NativeLaunchIntent) -> Result<bool, StoreError> {
        self.store.record_native_launch_intent(value)
    }
    fn record_native_launch_binding(&self, value: NativeLaunchBinding) -> Result<bool, StoreError> {
        self.store.record_native_launch_binding(value)
    }
    fn record_registration_acknowledgement(
        &self,
        value: RegistrationAcknowledgement,
    ) -> Result<bool, StoreError> {
        self.store.record_registration_acknowledgement(value)
    }
    fn record_prompt_delivery_intent(
        &self,
        value: PromptDeliveryIntent,
    ) -> Result<bool, StoreError> {
        self.store.record_prompt_delivery_intent(value)
    }
    fn record_prompt_delivery_result(
        &self,
        value: PromptDeliveryResult,
    ) -> Result<bool, StoreError> {
        self.store.record_prompt_delivery_result(value)
    }
    fn record_active(&self, flow_id: &str) -> Result<bool, StoreError> {
        self.store.record_active(flow_id)
    }
    fn record_stopped(&self, flow_id: &str) -> Result<bool, StoreError> {
        self.store.record_stopped(flow_id)
    }
    fn record_retired(&self, flow_id: &str) -> Result<bool, StoreError> {
        self.store.record_retired(flow_id)
    }
    fn record_exited(&self, flow_id: &str) -> Result<bool, StoreError> {
        self.store.record_exited(flow_id)
    }
    fn flow_node(&self, flow_id: &str) -> Result<Option<FlowNode>, StoreError> {
        self.store.flow_node(flow_id)
    }
    fn flow_nodes(&self) -> Result<Vec<FlowNode>, StoreError> {
        self.store.flow_nodes()
    }
    fn record_replacement(&self, value: Replacement) -> Result<(), StoreError> {
        self.store.record_replacement(value)
    }
    fn withdraw_replacement(&self, request: &str) -> Result<(), StoreError> {
        self.store.withdraw_replacement(request)
    }
    fn record_launch_outcome(
        &self,
        request: &str,
        outcome: LaunchOutcome,
    ) -> Result<(), StoreError> {
        self.store.record_launch_outcome(request, outcome)
    }
    fn sleep_metaflow(&self, address: &Address, flow_id: &str) -> Result<bool, StoreError> {
        self.store.sleep_metaflow(address, flow_id)
    }
    fn awaken_metaflow(
        &self,
        address: &Address,
        flow_id: &str,
        delivered: usize,
    ) -> Result<bool, StoreError> {
        self.store.awaken_metaflow(address, flow_id, delivered)
    }
    fn record_context_size(&self, flow_id: &str, tokens: i64) -> Result<(), StoreError> {
        self.store.record_context_size(flow_id, tokens)
    }
    fn record_event(&self, flow_id: &str, event: Event) -> Result<EventRecording, StoreError> {
        self.store.record_event(flow_id, event)
    }
}
