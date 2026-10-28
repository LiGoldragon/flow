//! Flow's send tool for Codex flows. Every Codex thread Flow opens declares
//! one dynamic tool, `send` (see `codex::OpensCodexThread`). The app-server
//! sends each call as a server request to every connection subscribed to
//! the thread, and the first answer wins. The Nexus keeps one connection
//! per live Codex thread it holds, subscribed by `thread/resume` when the
//! Nexus starts, when the thread's flow appears, and after every reconnect,
//! so a call left pending is replayed to it. That connection starts no turn
//! and carries no model content; it is the one intended responder. That the
//! first answer wins is how the app-server behaves, not a guarantee: any
//! process of the same user that subscribes to the thread can answer first.
//!
//! A launch's first turn may call the tool before the launch's flow is
//! Awake: a Codex launch records the thread it opened as its own when it
//! opens it, and keeps the connection that opened the thread, which is
//! subscribed to its server requests from `thread/start` on. The first turn
//! is started at Submit, once the TUI attached to the idle thread and the
//! pane bound; while Submit runs, the launch, holding the dispatch gate,
//! answers that connection's calls as the gate's holder. A call made after
//! the launch gave the gate back is the Nexus's responders'.
//!
//! A call is handled under the dispatch gate. Its sender is read from
//! Flow's records each time, a replayed call included: the one live flow
//! Flow holds on the calling thread, and the metaflow Awake as it; before
//! that flow is Awake, the Address of the unsettled launch that opened the
//! thread, while no other flow or launch holds it. Its
//! request is durably admitted with its explicit delivery and priority, and
//! the accepted call and logical-delivery identity are recorded in the same
//! commit. A replay is answered from its record and cannot create another
//! logical delivery. The answer is the typed acceptance as a datom.
//!
//! Whether the target wakes is persisted in the queue entry's explicit
//! Delivery policy, never inferred from the answer. A Nexus checks that
//! policy after admission and on restart.

use crate::caller::NamesRootThread;
use crate::codex::{
    CodexAdapter, ExchangesOnSubscription, ProxyEvent, ResponderSignal, SelectsCodexEndpoint,
    SubscribesThread, SubscriptionEnd, ThreadConnection, ThreadSubscription,
};
use crate::codex_turns::{ReadsThreadStatus, ReadsTurnEvent, SettlesRestingCodexSeats, TurnEvent};
use crate::compacting::{ReadsCodexContextSignal, WatchesCompactions};
use crate::generated::send_tool::{
    AnswersToolCall, DeclaresSendTool, HandlesSendCall, ParamsCarrying, ReadsSendArguments,
    ReadsToolCall, SendArguments, SendCall, SendInvocation, SendKind, SendOutcome, SendPriority,
    SendRefusal, ToolCall, ToolCallCarrying, ToolCallKey, ToolName,
};
use crate::herdr::launch::ClaimsNativeIdentity;
use crate::launching::NamesTopic;
use crate::sending::{AdmitsSendPayload, MintsId};
use crate::store::logical_delivery::RecordsLogicalDeliveries;
use crate::store::opened_thread::RecordsOpenedThreads;
use crate::store::send_call::KeepsSendCalls;
use crate::store::{
    ManagesMetaflowState, NamesLiveFlow, ReadsFlowRoles, ReadsFlowRows, ReadsLaunchAttempt,
};
use crate::waking::{RestsEndedAwake, SleepsAndWakesSeats};
use crate::{HoldsStopRequest, RunningNexus, store::AnnouncesLaunchChanges};
use datom_codec::Datomizable;
use protos::{Compactable, Protosizable};
use sha2::{Digest, Sha256};
use signal_flow::{Address, Aspect, Delivery, HarnessKind, Layer, Message, Request};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::mpsc,
    thread::Scope,
    time::{Duration, Instant},
};

/// The tool every Codex thread Flow opens declares.
pub struct SendTool;

impl SendTool {
    pub(crate) fn tool_spec_with(texts: &meta_signal_flow::Texts) -> serde_json::Value {
        let descriptions = &texts.send_tool_descriptions;
        assert_eq!(descriptions.len(), 5, "authored send-tool descriptions");
        serde_json::json!({
            "type": "function",
            "name": "send",
            "description": descriptions[0],
            "inputSchema": {
                "type": "object",
                "properties": {
                    "target": {
                        "type": "object", "description": descriptions[1],
                        "properties": {
                            "aspect": { "type": "string", "enum": ["Psyche", "Mind", "Field"] },
                            "topic": { "type": "string", "minLength": 1 },
                            "layer": { "type": "string", "enum": ["Primary", "Secondary", "Tertiary", "Quaternary"] }
                        },
                        "required": ["aspect", "topic", "layer"], "additionalProperties": false
                    },
                    "delivery": { "type": "string", "enum": ["Wake", "Queue"], "description": descriptions[2] },
                    "priority": { "type": "string", "enum": ["HardAbrupt", "MiddleAbrupt", "Soft"], "description": descriptions[3] },
                    "kind": { "type": "string", "enum": ["Order", "Question", "Result", "Notice"], "description": descriptions[4] },
                    "body": { "type": "string", "minLength": 1 }
                },
                "required": ["target", "delivery", "priority", "kind", "body"],
                "additionalProperties": false
            }
        })
    }
}

impl DeclaresSendTool for SendTool {
    fn tool_name() -> ToolName {
        "send".into()
    }

    fn tool_spec() -> serde_json::Value {
        Self::tool_spec_with(&crate::store::texts::seed_texts().expect("authored texts seed"))
    }
}

impl ParamsCarrying for serde_json::Value {
    fn params(&self) -> serde_json::Value {
        self.clone()
    }
}

impl ToolCallCarrying for ToolCall {
    fn tool_call(&self) -> ToolCall {
        self.clone()
    }
}

impl ReadsToolCall for ToolCall {
    fn read_tool_call<N: ParamsCarrying>(input: N) -> Option<ToolCall> {
        let params = input.params();
        let text = |field: &str| {
            params
                .get(field)
                .and_then(serde_json::Value::as_str)
                .filter(|value| !value.is_empty())
                .map(str::to_owned)
        };
        Some(ToolCall {
            tool_call_key: ToolCallKey {
                thread_id: text("threadId")?,
                turn_id: text("turnId")?,
                call_id: text("callId")?,
            },
            tool_name: text("tool")?,
            // Serialized back from the parsed value: the same arguments read
            // the same, however the app-server spaced them.
            arguments: serde_json::to_string(params.get("arguments")?).ok()?,
        })
    }
}

/// Gives each invocation a unique, typed journal identity without carrying
/// the potentially large request body into the journal.
pub trait DescribesSendInvocation {
    fn send_invocation(&self, outcome: SendOutcome) -> SendInvocation;
}

impl DescribesSendInvocation for ToolCall {
    fn send_invocation(&self, outcome: SendOutcome) -> SendInvocation {
        SendInvocation {
            send_invocation_id: "send-invocation".minted_id(),
            tool_call_key: self.tool_call_key.clone(),
            tool_name: self.tool_name.clone(),
            content_sha256: self.content_sha256(),
            send_outcome: outcome,
        }
    }
}

/// Writes one dynamic Send invocation's typed result to the process journal.
pub trait JournalsSendInvocations {
    fn journal_send_invocation(&self, call: &ToolCall, outcome: SendOutcome) -> SendOutcome;
}

impl JournalsSendInvocations for RunningNexus {
    fn journal_send_invocation(&self, call: &ToolCall, outcome: SendOutcome) -> SendOutcome {
        let invocation = call.send_invocation(outcome.clone());
        let datom = invocation.datomize(Vec::new()).protosize().compact();
        eprintln!("flow-nexus: {datom}");
        outcome
    }
}

/// The typed values of a send call's JSON arguments, each read strictly:
/// a name outside its enumeration, a missing or empty string, or a field
/// the schema does not name reads as nothing.
trait ReadsSendFields {
    fn aspect(value: &serde_json::Value) -> Option<Aspect>;
    fn layer(value: &serde_json::Value) -> Option<Layer>;
    fn delivery(value: &serde_json::Value) -> Option<Delivery>;
    fn priority(value: &serde_json::Value) -> Option<SendPriority>;
    fn kind(value: &serde_json::Value) -> Option<SendKind>;
    fn only_fields(value: &serde_json::Value, fields: &[&str]) -> bool;
}

impl ReadsSendFields for ToolCall {
    fn aspect(value: &serde_json::Value) -> Option<Aspect> {
        match value.as_str()? {
            "Psyche" => Some(Aspect::Psyche),
            "Mind" => Some(Aspect::Mind),
            "Field" => Some(Aspect::Field),
            _ => None,
        }
    }

    fn layer(value: &serde_json::Value) -> Option<Layer> {
        match value.as_str()? {
            "Primary" => Some(Layer::Primary),
            "Secondary" => Some(Layer::Secondary),
            "Tertiary" => Some(Layer::Tertiary),
            "Quaternary" => Some(Layer::Quaternary),
            _ => None,
        }
    }

    fn kind(value: &serde_json::Value) -> Option<SendKind> {
        match value.as_str()? {
            "Order" => Some(SendKind::Order),
            "Question" => Some(SendKind::Question),
            "Result" => Some(SendKind::Result),
            "Notice" => Some(SendKind::Notice),
            _ => None,
        }
    }

    fn delivery(value: &serde_json::Value) -> Option<Delivery> {
        match value.as_str()? {
            "Wake" => Some(Delivery::Wake),
            "Queue" => Some(Delivery::Queue),
            _ => None,
        }
    }

    fn priority(value: &serde_json::Value) -> Option<SendPriority> {
        match value.as_str()? {
            "HardAbrupt" => Some(SendPriority::HardAbrupt),
            "MiddleAbrupt" => Some(SendPriority::MiddleAbrupt),
            "Soft" => Some(SendPriority::Soft),
            _ => None,
        }
    }

    fn only_fields(value: &serde_json::Value, fields: &[&str]) -> bool {
        value
            .as_object()
            .is_some_and(|object| object.keys().all(|key| fields.contains(&key.as_str())))
    }
}

impl ReadsSendArguments for ToolCall {
    fn send_arguments(&self) -> Option<SendArguments> {
        let arguments = serde_json::from_str::<serde_json::Value>(&self.arguments).ok()?;
        if !Self::only_fields(
            &arguments,
            &["target", "delivery", "priority", "kind", "body"],
        ) {
            return None;
        }
        let target = arguments.get("target")?;
        if !Self::only_fields(target, &["aspect", "topic", "layer"]) {
            return None;
        }
        let topic = target
            .get("topic")?
            .as_str()
            .filter(|topic| !topic.is_empty())?;
        let body = arguments
            .get("body")?
            .as_str()
            .filter(|body| !body.is_empty())?;
        Some(SendArguments {
            target: Address {
                aspect: Self::aspect(target.get("aspect")?)?,
                topic: topic.to_owned(),
                layer: Self::layer(target.get("layer")?)?,
            },
            delivery: Self::delivery(arguments.get("delivery")?)?,
            send_priority: Self::priority(arguments.get("priority")?)?,
            send_kind: Self::kind(arguments.get("kind")?)?,
            body: body.to_owned(),
        })
    }

    fn content_sha256(&self) -> String {
        let mut digest = Sha256::new();
        digest.update(self.tool_name.as_bytes());
        digest.update([0]);
        digest.update(self.arguments.as_bytes());
        digest
            .finalize()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect()
    }
}

/// The request a send call queues: its kind carrying its body.
pub trait BecomesRequest {
    fn request(&self) -> Request;
}

impl BecomesRequest for SendArguments {
    fn request(&self) -> Request {
        let body = self.body.clone();
        match &self.send_kind {
            SendKind::Order => Request::Order(body),
            SendKind::Question => Request::Question(body),
            SendKind::Result => Request::Result(body),
            SendKind::Notice => Request::Notice(body),
        }
    }
}

trait BecomesMessage {
    fn message(&self) -> Message;
}

impl BecomesMessage for SendArguments {
    fn message(&self) -> Message {
        let request = self.request();
        match &self.send_priority {
            SendPriority::HardAbrupt => Message::HardAbrupt(request),
            SendPriority::MiddleAbrupt => Message::MiddleAbrupt(request),
            SendPriority::Soft => Message::Soft(request),
        }
    }
}

impl AnswersToolCall for SendOutcome {
    fn tool_response(&self) -> serde_json::Value {
        serde_json::json!({
            "success": matches!(self, SendOutcome::Accepted(_)),
            "contentItems": [{
                "type": "inputText",
                "text": self.datomize(Vec::new()).protosize().compact()
            }]
        })
    }
}

/// A refusal as the answer to a call.
trait RefusesSendCall {
    fn refused(refusal: SendRefusal) -> SendOutcome {
        SendOutcome::Refused(refusal)
    }
}

impl RefusesSendCall for RunningNexus {}

impl HandlesSendCall for RunningNexus {
    fn handle_send_call<N: ToolCallCarrying>(&self, input: N) -> SendOutcome {
        let call = input.tool_call();
        let outcome = {
            let _turn = self
                .dispatch_gate
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            self.handle_held_send_call(call)
        };
        if let SendOutcome::Accepted(accepted) = &outcome
            && accepted.send_acceptance.delivery_state == signal_flow::DeliveryState::Queued
        {
            self.wake_due(accepted.target.clone());
        }
        outcome
    }

    fn handle_held_send_call<N: ToolCallCarrying>(&self, input: N) -> SendOutcome {
        let call = input.tool_call();
        let outcome = (|| {
            if call.tool_name != SendTool::tool_name() {
                return Self::refused(SendRefusal::UnknownTool);
            }
            let key = &call.tool_call_key;
            let store_refused = |error: crate::store::StoreError| {
                eprintln!(
                    "flow-nexus: send call {} of thread {} refused by the store: {error}",
                    key.call_id, key.thread_id
                );
                Self::refused(SendRefusal::Store)
            };
            let configuration = match self.store.committed_configuration_snapshot() {
                Ok(Some(committed)) => committed.snapshot,
                Ok(None) => return Self::refused(SendRefusal::Store),
                Err(error) => return store_refused(error),
            };
            let codex_endpoints = crate::codex::CodexEndpoints::configured(&configuration)
                .with_texts(configuration.texts.clone());
            // The sender is read again for every call, a replayed one included.
            let root_thread = codex_endpoints.root_thread(&key.thread_id);
            let sender = match self.store.thread_sender(&root_thread) {
                Ok(Ok(sender)) => sender,
                Ok(Err(refusal)) => return Self::refused(refusal),
                Err(error) => return store_refused(error),
            };
            let sender_flow_id = match self.store.list_metaflows() {
                Ok(metaflows) => metaflows
                    .into_iter()
                    .find_map(|metaflow| (metaflow.address == sender).then_some(metaflow.state)),
                Err(error) => return store_refused(error),
            };
            let sender_flow_id = match sender_flow_id {
                Some(flow_ethos::State::Awake(flow_id)) => Some(flow_id),
                _ => match self.store.thread_launch(&root_thread) {
                    Ok(Some(launch_request_id)) => {
                        match self.store.launch_attempt(&launch_request_id) {
                            Ok(Some(attempt)) => attempt
                                .native_launch_binding_option
                                .map(|binding| binding.flow_id),
                            Ok(None) => None,
                            Err(error) => return store_refused(error),
                        }
                    }
                    Ok(None) => None,
                    Err(error) => return store_refused(error),
                },
            };
            let sender_flow_id = sender_flow_id.or_else(|| {
                self.herdr
                    .claim_flow_identity(&HarnessKind::Codex, &root_thread)
                    .ok()
            });
            let Some(sender_flow_id) = sender_flow_id else {
                return Self::refused(SendRefusal::NotAwake);
            };
            let sender = meta_signal_flow::Sender {
                address: sender,
                flow_id: sender_flow_id,
            };
            let content_sha256 = call.content_sha256();
            match self.store.send_call(key) {
                Ok(Some(recorded)) if recorded.content_sha256 == content_sha256 => {
                    return recorded.send_outcome;
                }
                Ok(Some(_)) => return Self::refused(SendRefusal::KeyReused),
                Ok(None) => {}
                Err(error) => return store_refused(error),
            }
            // A refusal of the call's content is kept as its answer, so a
            // replay is answered the same.
            let refused_kept = |refusal: SendRefusal, content_sha256: String| {
                let outcome = SendOutcome::Refused(refusal);
                match self.store.record_send_call(SendCall {
                    tool_call_key: key.clone(),
                    content_sha256,
                    send_outcome: outcome.clone(),
                }) {
                    Ok(()) => outcome,
                    Err(error) => store_refused(error),
                }
            };
            let Some(arguments) = call.send_arguments() else {
                return refused_kept(SendRefusal::Malformed, content_sha256);
            };
            // The target's topic is an uncapitalized camelCase identifier,
            // taken as written: its capitals are never lowered.
            if !arguments.target.topic.as_str().is_camel_case_topic() {
                return refused_kept(
                    SendRefusal::TopicRefused(arguments.target.topic.clone()),
                    content_sha256,
                );
            }
            let Some(now) = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .ok()
                .and_then(|duration| i64::try_from(duration.as_secs()).ok())
            else {
                return Self::refused(SendRefusal::Store);
            };
            let message = arguments.message();
            if let Some(signal_flow::Refusal::Oversized(size)) =
                message.oversized_refusal(&sender.address, configuration.limits.send_body as usize)
            {
                return refused_kept(SendRefusal::Oversized(size), content_sha256);
            }
            let outcome = self
                .store
                .accept_tool_send(
                    key,
                    &content_sha256,
                    sender,
                    &arguments.target,
                    arguments.delivery,
                    message,
                    now,
                )
                .unwrap_or_else(store_refused);
            if matches!(outcome, SendOutcome::Accepted(_)) {
                // Restore a formerly-awake target only after its logical
                // delivery and exact tool-call result are durable.
                self.rest_ended_awake(&arguments.target);
            }
            outcome
        })();
        self.journal_send_invocation(&call, outcome)
    }
}

/// How long a responder waits before it reconnects: from the first wait,
/// doubling after each connection that fails, to the longest.
pub(crate) trait PacesReconnection {
    /// Waits `wait` unless told to stop first; true when told to stop.
    fn stopped_within(inbox: &mpsc::Receiver<ResponderSignal>, wait: Duration) -> bool {
        let deadline = Instant::now() + wait;
        loop {
            let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
                return false;
            };
            match inbox.recv_timeout(remaining) {
                Ok(ResponderSignal::Stop) | Err(mpsc::RecvTimeoutError::Disconnected) => {
                    return true;
                }
                // Events of a connection already closed.
                Ok(ResponderSignal::Proxy(..)) => {}
                Err(mpsc::RecvTimeoutError::Timeout) => return false,
            }
        }
    }
}

/// The responder of one thread: its subscribed connection, kept, and each
/// call it brings handled and answered.
pub(crate) trait RespondsToSendCalls {
    /// Keeps a connection subscribed to `thread` until told to stop,
    /// reconnecting whenever it closes.
    fn respond_for(
        &self,
        thread: &str,
        adapter: &CodexAdapter,
        signals: mpsc::Sender<ResponderSignal>,
        inbox: mpsc::Receiver<ResponderSignal>,
    );
    /// Handles one server request the connection brought, when it is a
    /// tool call, and answers it; then, whether the answer was written or
    /// not, asks Flow to wake the target whose persisted delivery policy is
    /// Wake. Err when the answer could not be written.
    fn answer_request(
        &self,
        subscription: &mut ThreadSubscription,
        request: serde_json::Value,
    ) -> Result<(), crate::codex::CodexAdapterUnavailable>;
}

impl PacesReconnection for RunningNexus {}

impl RespondsToSendCalls for RunningNexus {
    fn respond_for(
        &self,
        thread: &str,
        adapter: &CodexAdapter,
        signals: mpsc::Sender<ResponderSignal>,
        inbox: mpsc::Receiver<ResponderSignal>,
    ) {
        let mut generation = 0;
        let first_wait = Duration::from_millis(self.timings.send_tool_first_wait as u64);
        let longest_wait = Duration::from_millis(self.timings.send_tool_longest_wait as u64);
        let mut wait = first_wait;
        loop {
            generation += 1;
            match adapter.subscribe_thread(thread, generation, &signals, &inbox) {
                Err(SubscriptionEnd::Stopped) => return,
                Err(SubscriptionEnd::Failed(error)) => {
                    eprintln!("flow-nexus: send tool for thread {thread} not subscribed: {error}");
                    if Self::stopped_within(&inbox, wait) {
                        return;
                    }
                    wait = (wait * 2).min(longest_wait);
                    continue;
                }
                Ok(mut subscription) => {
                    wait = first_wait;
                    let early = std::mem::take(&mut subscription.early);
                    let mut open = early.into_iter().all(|request| {
                        self.answer_request(&mut subscription, request)
                            .inspect_err(|error| {
                                eprintln!("flow-nexus: send tool for thread {thread}: {error}")
                            })
                            .is_ok()
                    });
                    // The thread's status as the resume answered it: newer
                    // than every event that came before the answer.
                    if let Some(status) = subscription
                        .resumed
                        .take()
                        .and_then(|status| status.thread_status())
                    {
                        self.codex_events(thread, vec![TurnEvent::Resumed(status)]);
                    }
                    while open {
                        match subscription.next_event(&inbox, None) {
                            Err(SubscriptionEnd::Stopped) => {
                                subscription.close();
                                return;
                            }
                            Ok(ProxyEvent::Reply(Ok(message))) => {
                                if let Err(error) = self.answer_request(&mut subscription, message)
                                {
                                    eprintln!("flow-nexus: send tool for thread {thread}: {error}");
                                    open = false;
                                }
                            }
                            Ok(ProxyEvent::Upgraded | ProxyEvent::Ping(_)) => {}
                            Ok(ProxyEvent::Reply(Err(error)))
                            | Err(SubscriptionEnd::Failed(error)) => {
                                eprintln!(
                                    "flow-nexus: send tool for thread {thread} reconnects: {error}"
                                );
                                open = false;
                            }
                        }
                    }
                    subscription.close();
                    if Self::stopped_within(&inbox, wait) {
                        return;
                    }
                }
            }
        }
    }

    fn answer_request(
        &self,
        subscription: &mut ThreadSubscription,
        request: serde_json::Value,
    ) -> Result<(), crate::codex::CodexAdapterUnavailable> {
        if let Some(thread) = subscription.thread.clone()
            && let Some(event) = request.turn_event(&thread)
        {
            self.codex_events(&thread, vec![event]);
            return Ok(());
        }
        if let Some(thread) = subscription.thread.clone()
            && let Some(signal) = request.context_signal(&thread)
        {
            self.codex_context_signal(&thread, signal);
            return Ok(());
        }
        if request.get("method").and_then(serde_json::Value::as_str) != Some("item/tool/call") {
            return Ok(());
        }
        let Some(id) = request.get("id").cloned() else {
            return Ok(());
        };
        let outcome = match request
            .get("params")
            .cloned()
            .and_then(ToolCall::read_tool_call::<serde_json::Value>)
        {
            Some(call) => self.handle_send_call(call),
            None => Self::refused(SendRefusal::Malformed),
        };
        let answered = subscription.answer(id, outcome.tool_response());
        if let SendOutcome::Accepted(accepted) = outcome {
            self.wake_due(accepted.target);
        }
        answered
    }
}

/// The responder of the thread a running Codex launch opened: the
/// connection that opened it, subscribed to its server requests from
/// `thread/start`, so a first turn that calls the tool while the launch
/// submits it is served. The launch holds the dispatch gate throughout, so
/// the launch, as the gate's holder, reads and answers this connection's
/// calls while it waits (`ServesLaunchCalls::serving_launch_calls`).
pub(crate) type LaunchResponder = ThreadConnection;

/// The responder of the launch running now, and the targets whose Wake
/// policy must be considered once the launch has given the gate back.
#[derive(Default)]
pub struct LaunchCalls {
    responder: std::sync::Mutex<Option<LaunchResponder>>,
    deferred_wakes: std::sync::Mutex<Vec<Address>>,
}

/// A running Codex launch serves its own thread's calls.
pub(crate) trait ServesLaunchCalls {
    /// How long one read of the launch's connection waits before the launch
    /// looks again whether the work it waits on has ended.
    /// Keeps the connection that opened the launch's thread as its
    /// responder.
    fn hold_launch_responder(&self, responder: LaunchResponder);
    /// Runs `work`, an outside wait that writes nothing to the store, while
    /// the caller, holding the dispatch gate, answers the calls the
    /// launch's connection brings. Without a launch responder, `work` runs
    /// alone.
    fn serving_launch_calls<T: Send>(&self, work: impl FnOnce() -> T + Send) -> T;
    /// Handles one server request of the launch's connection as the gate's
    /// holder and answers it.
    fn answer_launch_call(
        &self,
        responder: &mut LaunchResponder,
        request: serde_json::Value,
    ) -> Result<(), crate::codex::CodexAdapterUnavailable>;
    /// At the launch's end, whatever it came to: answers the calls already
    /// read, then closes the connection. A later call is the Nexus's
    /// responders' (`ServesSendTool`).
    fn close_launch_responder(&self);
    /// Off the dispatch gate, considers every target admitted during launch.
    fn wake_deferred(&self);
}

impl ServesLaunchCalls for RunningNexus {
    fn hold_launch_responder(&self, responder: LaunchResponder) {
        self.close_launch_responder();
        *self
            .launch_calls
            .responder
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(responder);
    }

    fn serving_launch_calls<T: Send>(&self, work: impl FnOnce() -> T + Send) -> T {
        let taken = self
            .launch_calls
            .responder
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take();
        let Some(mut responder) = taken else {
            return work();
        };
        let mut open = true;
        let outcome = std::thread::scope(|scope| {
            let running = scope.spawn(work);
            // The calls that arrived before the subscription was answered.
            for request in std::mem::take(&mut responder.subscription.early) {
                if let Err(error) = self.answer_launch_call(&mut responder, request) {
                    eprintln!("flow-nexus: launch send tool: {error}");
                    open = false;
                }
            }
            while open && !running.is_finished() {
                let deadline = Instant::now()
                    + Duration::from_millis(self.timings.send_tool_serving_slice as u64);
                match responder
                    .subscription
                    .next_event(&responder.inbox, Some(deadline))
                {
                    Ok(ProxyEvent::Reply(Ok(message))) => {
                        if let Err(error) = self.answer_launch_call(&mut responder, message) {
                            eprintln!("flow-nexus: launch send tool: {error}");
                            open = false;
                        }
                    }
                    Ok(ProxyEvent::Upgraded | ProxyEvent::Ping(_)) => {}
                    Err(SubscriptionEnd::Failed(
                        crate::codex::CodexAdapterUnavailable::TimedOut,
                    )) => {}
                    Ok(ProxyEvent::Reply(Err(error))) | Err(SubscriptionEnd::Failed(error)) => {
                        eprintln!("flow-nexus: launch send tool connection ended: {error}");
                        open = false;
                    }
                    Err(SubscriptionEnd::Stopped) => open = false,
                }
            }
            running
                .join()
                .unwrap_or_else(|panic| std::panic::resume_unwind(panic))
        });
        if open {
            *self
                .launch_calls
                .responder
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(responder);
        } else {
            responder.subscription.close();
        }
        outcome
    }

    fn answer_launch_call(
        &self,
        responder: &mut LaunchResponder,
        request: serde_json::Value,
    ) -> Result<(), crate::codex::CodexAdapterUnavailable> {
        // The launch holds the dispatch gate: its thread's turn events are
        // observed as the gate's holder.
        if let Some(thread) = responder.subscription.thread.clone()
            && let Some(event) = request.turn_event(&thread)
        {
            self.observe_codex_events(&thread, vec![event]);
            return Ok(());
        }
        // A first turn's context size is kept as any turn's is. A
        // compaction in it is not raised: raising starts a flow, which
        // waits on the gate this launch holds.
        if let Some(thread) = responder.subscription.thread.clone()
            && let Some(signal @ crate::compacting::CodexContextSignal::TokenUsage(_)) =
                request.context_signal(&thread)
        {
            self.codex_context_signal(&thread, signal);
            return Ok(());
        }
        if request.get("method").and_then(serde_json::Value::as_str) != Some("item/tool/call") {
            return Ok(());
        }
        let Some(id) = request.get("id").cloned() else {
            return Ok(());
        };
        let outcome = match request
            .get("params")
            .cloned()
            .and_then(ToolCall::read_tool_call::<serde_json::Value>)
        {
            Some(call) => self.handle_held_send_call(call),
            None => Self::refused(SendRefusal::Malformed),
        };
        let answered = responder.subscription.answer(id, outcome.tool_response());
        if let SendOutcome::Accepted(accepted) = outcome {
            self.launch_calls
                .deferred_wakes
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .push(accepted.target);
        }
        answered
    }

    fn close_launch_responder(&self) {
        let taken = self
            .launch_calls
            .responder
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take();
        let Some(mut responder) = taken else {
            return;
        };
        for request in std::mem::take(&mut responder.subscription.early) {
            let _ = self.answer_launch_call(&mut responder, request);
        }
        let deadline =
            Instant::now() + Duration::from_millis(self.timings.send_tool_serving_slice as u64);
        while let Ok(ProxyEvent::Reply(Ok(message))) = responder
            .subscription
            .next_event(&responder.inbox, Some(deadline))
        {
            if self.answer_launch_call(&mut responder, message).is_err() {
                break;
            }
        }
        responder.subscription.close();
    }

    fn wake_deferred(&self) {
        let wakes = std::mem::take(
            &mut *self
                .launch_calls
                .deferred_wakes
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()),
        );
        for target in wakes {
            self.wake_due(target);
        }
    }
}

/// What the calls replayed on an interrupted launch's thread were answered.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct InterruptedCalls {
    /// Calls the earlier Nexus had committed, answered from their records:
    /// nothing queued again.
    pub answered_from_record: usize,
    /// Calls with no record, refused and never queued.
    pub refused: usize,
}

/// The calls left pending on the thread of a launch an earlier Nexus never
/// finished, on a connection that resumes the thread (the app-server
/// replays a pending call to it). Nothing is admitted here: a call the
/// earlier Nexus committed before it could answer is answered from its
/// record; any other call is refused.
pub(crate) trait RefusesInterruptedCalls {
    /// How long the resumed connection is read for replayed calls.
    /// Answers every call the app-server replays on `thread` within the
    /// window: from its record when one holds the same call, else refused
    /// (`ThreadUnowned`; `KeyReused` when its key's record holds another
    /// call).
    fn refuse_interrupted_calls(&self, thread: &str, adapter: &CodexAdapter) -> InterruptedCalls;
    /// The answer to one replayed call; true when it came from its record.
    fn interrupted_call_outcome(&self, thread: &str, call: Option<ToolCall>)
    -> (SendOutcome, bool);
}

impl RefusesInterruptedCalls for RunningNexus {
    fn refuse_interrupted_calls(&self, thread: &str, adapter: &CodexAdapter) -> InterruptedCalls {
        let mut tally = InterruptedCalls::default();
        let (signals, inbox) = mpsc::channel();
        let mut subscription = match adapter.subscribe_thread(thread, 1, &signals, &inbox) {
            Ok(subscription) => subscription,
            Err(SubscriptionEnd::Failed(error)) => {
                eprintln!("flow-nexus: interrupted launch thread {thread} not resumed: {error}");
                return tally;
            }
            Err(SubscriptionEnd::Stopped) => return tally,
        };
        let mut answer = |subscription: &mut ThreadSubscription, request: serde_json::Value| {
            if request.get("method").and_then(serde_json::Value::as_str) != Some("item/tool/call") {
                return true;
            }
            let Some(id) = request.get("id").cloned() else {
                return true;
            };
            let call = request
                .get("params")
                .cloned()
                .and_then(ToolCall::read_tool_call::<serde_json::Value>);
            let (outcome, recorded) = self.interrupted_call_outcome(thread, call);
            if recorded {
                tally.answered_from_record += 1;
            } else {
                tally.refused += 1;
            }
            subscription.answer(id, outcome.tool_response()).is_ok()
        };
        let early = std::mem::take(&mut subscription.early);
        let mut open = early
            .into_iter()
            .all(|request| answer(&mut subscription, request));
        let deadline =
            Instant::now() + Duration::from_millis(self.timings.send_tool_replay_window as u64);
        while open {
            match subscription.next_event(&inbox, Some(deadline)) {
                Ok(ProxyEvent::Reply(Ok(message))) => open = answer(&mut subscription, message),
                Ok(ProxyEvent::Upgraded | ProxyEvent::Ping(_)) => {}
                _ => open = false,
            }
        }
        subscription.close();
        tally
    }

    fn interrupted_call_outcome(
        &self,
        thread: &str,
        call: Option<ToolCall>,
    ) -> (SendOutcome, bool) {
        let Some(call) = call else {
            return (Self::refused(SendRefusal::ThreadUnowned), false);
        };
        let (outcome, recorded) = if call.tool_name != SendTool::tool_name()
            || call.tool_call_key.thread_id != thread
        {
            (Self::refused(SendRefusal::ThreadUnowned), false)
        } else {
            match self.store.send_call(&call.tool_call_key) {
                Ok(Some(recorded)) if recorded.content_sha256 == call.content_sha256() => {
                    (recorded.send_outcome, true)
                }
                Ok(Some(_)) => (Self::refused(SendRefusal::KeyReused), false),
                Ok(None) => (Self::refused(SendRefusal::ThreadUnowned), false),
                Err(error) => {
                    eprintln!(
                        "flow-nexus: replayed call {} of thread {thread} refused by the store: {error}",
                        call.tool_call_key.call_id
                    );
                    (Self::refused(SendRefusal::Store), false)
                }
            }
        };
        (self.journal_send_invocation(&call, outcome), recorded)
    }
}

/// The responders the Nexus keeps, one per live Codex thread it holds, each
/// on a thread of its own with the sender that stops it.
#[derive(Default)]
pub struct SendToolResponders {
    responders: BTreeMap<String, mpsc::Sender<ResponderSignal>>,
    /// Threads whose flow names no model a Codex endpoint serves, each
    /// logged once.
    unserved: BTreeSet<String>,
}

/// Keeps the responders in step with Flow's records.
pub trait KeepsSendToolResponders {
    /// The live Codex threads Flow holds, each with the adapter of the
    /// endpoint its flow's model selects.
    fn wanted(&mut self, nexus: &RunningNexus) -> BTreeMap<String, CodexAdapter>;
    /// Starts a responder for each wanted thread that has none and stops
    /// each responder whose thread is no longer wanted.
    fn pass<'scope, 'env>(&mut self, nexus: &'env RunningNexus, scope: &'scope Scope<'scope, 'env>);
    fn stop_all(&mut self);
}

impl KeepsSendToolResponders for SendToolResponders {
    fn wanted(&mut self, nexus: &RunningNexus) -> BTreeMap<String, CodexAdapter> {
        let nodes = match nexus.store.flow_nodes() {
            Ok(nodes) => nodes,
            Err(error) => {
                eprintln!("flow-nexus: send tool threads unreadable: {error}");
                return BTreeMap::new();
            }
        };
        let mut wanted = BTreeMap::new();
        for node in nodes {
            if node.harness_kind != HarnessKind::Codex
                || !node.flow_lifecycle.is_live()
                || node.session_id.is_empty()
            {
                continue;
            }
            let adapter = nexus
                .store
                .role(&node.flow_id)
                .ok()
                .flatten()
                .and_then(|role| nexus.codex_endpoints.adapter_for(&role.model_name).ok());
            match adapter {
                Some(adapter) => {
                    wanted.insert(node.session_id, adapter);
                }
                None => {
                    if self.unserved.insert(node.session_id.clone()) {
                        eprintln!(
                            "flow-nexus: send tool has no Codex endpoint for flow {} thread {}",
                            node.flow_id, node.session_id
                        );
                    }
                }
            }
        }
        wanted
    }

    fn pass<'scope, 'env>(
        &mut self,
        nexus: &'env RunningNexus,
        scope: &'scope Scope<'scope, 'env>,
    ) {
        let wanted = self.wanted(nexus);
        self.responders.retain(|thread, stop| {
            let keep = wanted.contains_key(thread);
            if !keep {
                let _ = stop.send(ResponderSignal::Stop);
            }
            keep
        });
        for (thread, adapter) in wanted {
            if self.responders.contains_key(&thread) {
                continue;
            }
            let (signals, inbox) = mpsc::channel();
            self.responders.insert(thread.clone(), signals.clone());
            scope.spawn(move || nexus.respond_for(&thread, &adapter, signals, inbox));
        }
    }

    fn stop_all(&mut self) {
        for stop in self.responders.values() {
            let _ = stop.send(ResponderSignal::Stop);
        }
        self.responders.clear();
    }
}

/// The Nexus's send tool, for its whole life: the responders are brought in
/// step with Flow's records at its start and at every change the store
/// announces, and all are stopped when the Nexus stops.
pub trait ServesSendTool {
    fn serve_send_tool<'scope, 'env>(&'env self, scope: &'scope Scope<'scope, 'env>);
}

impl ServesSendTool for RunningNexus {
    fn serve_send_tool<'scope, 'env>(&'env self, scope: &'scope Scope<'scope, 'env>) {
        let changes = self.store.launch_changes.clone();
        let mut responders = SendToolResponders::default();
        while !self.stopping.requested() {
            let seen = changes.current();
            responders.pass(self, scope);
            changes.after(seen);
        }
        responders.stop_all();
    }
}
