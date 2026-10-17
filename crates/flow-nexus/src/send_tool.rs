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
//! A call is handled under the dispatch gate. Its sender is read from
//! Flow's records each time, a replayed call included: the one live flow
//! Flow holds on the calling thread, and the metaflow Awake as it. Its
//! request is queued for the target as a Lock and a Deliver from Message
//! would queue it, and the call and its outcome are recorded in the commit
//! that queues it (`store::send_call`), so a replayed call is answered from
//! its record and queued once. The answer is the outcome as a datom.

use crate::codex::{
    CodexAdapter, ExchangesOnSubscription, ProxyEvent, ResponderSignal, SelectsCodexEndpoint,
    SubscribesThread, SubscriptionEnd, ThreadSubscription,
};
use crate::generated::send_tool::{
    AnswersToolCall, DeclaresSendTool, HandlesSendCall, ParamsCarrying, ReadsSendArguments,
    ReadsToolCall, SendAnswer, SendArguments, SendCall, SendKind, SendOutcome, SendRefusal,
    ToolCall, ToolCallCarrying, ToolCallKey, ToolName,
};
use crate::store::send_call::{KeepsSendCalls, SendEnqueuing};
use crate::store::{NamesLiveFlow, ReadsFlowRoles, ReadsFlowRows};
use crate::waking::{RestsEndedAwake, SleepsAndWakesSeats, WakePolicy};
use crate::{HoldsStopRequest, RunningNexus, store::AnnouncesLaunchChanges};
use datom_codec::Datomizable;
use protos::{Compactable, Protosizable};
use sha2::{Digest, Sha256};
use signal_flow::{Address, Aspect, HarnessKind, Layer, Request};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::mpsc,
    thread::Scope,
    time::{Duration, Instant},
};

/// The tool every Codex thread Flow opens declares.
pub struct SendTool;

impl DeclaresSendTool for SendTool {
    fn tool_name() -> ToolName {
        "send".into()
    }

    fn tool_spec() -> serde_json::Value {
        serde_json::json!({
            "type": "function",
            "name": Self::tool_name(),
            "description": "Send one request from this flow to the flow at a metaflow Address. Flow sends it from this flow's own Address, which it reads from its records, and queues it for the target. The answer is the outcome as a datom: Queued, or Refused with the reason.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "target": {
                        "type": "object",
                        "description": "The Address of the receiving metaflow.",
                        "properties": {
                            "aspect": { "type": "string", "enum": ["Psyche", "Mind", "Field"] },
                            "topic": { "type": "string", "minLength": 1 },
                            "layer": {
                                "type": "string",
                                "enum": ["Primary", "Secondary", "Tertiary", "Quaternary"]
                            }
                        },
                        "required": ["aspect", "topic", "layer"],
                        "additionalProperties": false
                    },
                    "kind": {
                        "type": "string",
                        "enum": ["Order", "Question", "Result", "Notice"],
                        "description": "Order: work for the target. Question: a question it answers. Result: a result it awaits. Notice: an error or blocker affecting its work."
                    },
                    "body": { "type": "string", "minLength": 1 }
                },
                "required": ["target", "kind", "body"],
                "additionalProperties": false
            }
        })
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

/// The typed values of a send call's JSON arguments, each read strictly:
/// a name outside its enumeration, a missing or empty string, or a field
/// the schema does not name reads as nothing.
trait ReadsSendFields {
    fn aspect(value: &serde_json::Value) -> Option<Aspect>;
    fn layer(value: &serde_json::Value) -> Option<Layer>;
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

    fn only_fields(value: &serde_json::Value, fields: &[&str]) -> bool {
        value
            .as_object()
            .is_some_and(|object| object.keys().all(|key| fields.contains(&key.as_str())))
    }
}

impl ReadsSendArguments for ToolCall {
    fn send_arguments(&self) -> Option<SendArguments> {
        let arguments = serde_json::from_str::<serde_json::Value>(&self.arguments).ok()?;
        if !Self::only_fields(&arguments, &["target", "kind", "body"]) {
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
        match self.send_kind {
            SendKind::Order => Request::Order(body),
            SendKind::Question => Request::Question(body),
            SendKind::Result => Request::Result(body),
            SendKind::Notice => Request::Notice(body),
        }
    }
}

impl AnswersToolCall for SendOutcome {
    fn tool_response(&self) -> serde_json::Value {
        serde_json::json!({
            "success": matches!(self, SendOutcome::Queued(_)),
            "contentItems": [{
                "type": "inputText",
                "text": self.datomize(Vec::new()).protosize().compact()
            }]
        })
    }
}

/// A refusal as the answer to a call, waking nothing.
trait RefusesSendCall {
    fn refused(refusal: SendRefusal) -> SendAnswer {
        SendAnswer {
            send_outcome: SendOutcome::Refused(refusal),
            wake: None,
        }
    }
}

impl RefusesSendCall for RunningNexus {}

impl HandlesSendCall for RunningNexus {
    fn handle_send_call<N: ToolCallCarrying>(&self, input: N) -> SendAnswer {
        let call = input.tool_call();
        if call.tool_name != SendTool::tool_name() {
            return Self::refused(SendRefusal::UnknownTool);
        }
        let _turn = self
            .dispatch_gate
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let key = &call.tool_call_key;
        let store_refused = |error: crate::store::StoreError| {
            eprintln!(
                "flow-nexus: send call {} of thread {} refused by the store: {error}",
                key.call_id, key.thread_id
            );
            Self::refused(SendRefusal::Store)
        };
        // The sender is read again for every call, a replayed one included.
        let sender = match self.store.thread_sender(&key.thread_id) {
            Ok(Ok(sender)) => sender,
            Ok(Err(refusal)) => return Self::refused(refusal),
            Err(error) => return store_refused(error),
        };
        let content_sha256 = call.content_sha256();
        match self.store.send_call(key) {
            Ok(Some(recorded)) if recorded.content_sha256 == content_sha256 => {
                return SendAnswer {
                    send_outcome: recorded.send_outcome,
                    wake: None,
                };
            }
            Ok(Some(_)) => return Self::refused(SendRefusal::KeyReused),
            Ok(None) => {}
            Err(error) => return store_refused(error),
        }
        let Some(arguments) = call.send_arguments() else {
            let outcome = SendOutcome::Refused(SendRefusal::Malformed);
            return match self.store.record_send_call(SendCall {
                tool_call_key: key.clone(),
                content_sha256,
                send_outcome: outcome.clone(),
            }) {
                Ok(()) => SendAnswer {
                    send_outcome: outcome,
                    wake: None,
                },
                Err(error) => store_refused(error),
            };
        };
        let Some(now) = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .ok()
            .and_then(|duration| i64::try_from(duration.as_secs()).ok())
        else {
            return Self::refused(SendRefusal::Store);
        };
        let request = arguments.request();
        let wakes = Self::wakes_for(&request);
        // Queued for a metaflow left Awake as an ended flow, the request
        // wakes it as it would an Asleep one.
        self.rest_ended_awake(&arguments.target);
        match self.store.queue_send(
            key,
            &content_sha256,
            sender,
            &arguments.target,
            request,
            now,
        ) {
            Ok(SendEnqueuing {
                outcome,
                recipient_asleep,
            }) => SendAnswer {
                wake: (matches!(outcome, SendOutcome::Queued(_))
                    && recipient_asleep
                    && wakes
                    && Self::wakes_layer(&arguments.target.layer))
                .then(|| arguments.target.clone()),
                send_outcome: outcome,
            },
            Err(error) => store_refused(error),
        }
    }
}

/// How long a responder waits before it reconnects: from the first wait,
/// doubling after each connection that fails, to the longest.
pub(crate) trait PacesReconnection {
    const FIRST_WAIT: Duration = Duration::from_secs(1);
    const LONGEST_WAIT: Duration = Duration::from_secs(60);
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
    /// tool call, and answers it; then wakes the Asleep target it queued
    /// for. Err when the answer could not be written.
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
        let mut wait = Self::FIRST_WAIT;
        loop {
            generation += 1;
            match adapter.subscribe_thread(thread, generation, &signals, &inbox) {
                Err(SubscriptionEnd::Stopped) => return,
                Err(SubscriptionEnd::Failed(error)) => {
                    eprintln!("flow-nexus: send tool for thread {thread} not subscribed: {error}");
                    if Self::stopped_within(&inbox, wait) {
                        return;
                    }
                    wait = (wait * 2).min(Self::LONGEST_WAIT);
                    continue;
                }
                Ok(mut subscription) => {
                    wait = Self::FIRST_WAIT;
                    let early = std::mem::take(&mut subscription.early);
                    let mut open = early.into_iter().all(|request| {
                        self.answer_request(&mut subscription, request)
                            .inspect_err(|error| {
                                eprintln!("flow-nexus: send tool for thread {thread}: {error}")
                            })
                            .is_ok()
                    });
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
        if request.get("method").and_then(serde_json::Value::as_str) != Some("item/tool/call") {
            return Ok(());
        }
        let Some(id) = request.get("id").cloned() else {
            return Ok(());
        };
        let answer = match request
            .get("params")
            .cloned()
            .and_then(ToolCall::read_tool_call::<serde_json::Value>)
        {
            Some(call) => self.handle_send_call(call),
            None => Self::refused(SendRefusal::Malformed),
        };
        subscription.answer(id, answer.send_outcome.tool_response())?;
        if let Some(target) = answer.wake {
            self.wake_due(target);
        }
        Ok(())
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
