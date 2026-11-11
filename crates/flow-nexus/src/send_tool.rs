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
//! request is durably admitted with its priority, and
//! the accepted call and logical-delivery identity are recorded in the same
//! commit. A replay is answered from its record and cannot create another
//! logical delivery. The answer is the typed acceptance as a datom.
//!
//! The recipient state and priority determine submission: an Awake seat
//! receives HardAbrupt and MiddleAbrupt now, and Soft at its next idle event
//! unless it rests already; Asleep retains Soft and wakes for either abrupt
//! priority.

use crate::caller::NamesRootThread;
use crate::codex::{
    CodexAdapter, ExchangesOnSubscription, ProxyEvent, ResponderSignal, SelectsCodexEndpoint,
    SubscribesThread, SubscriptionEnd, ThreadConnection, ThreadSubscription,
};
use crate::codex_turns::{ReadsThreadStatus, ReadsTurnEvent, SettlesRestingCodexSeats, TurnEvent};
use crate::compacting::{ReadsCodexContextSignal, WatchesCompactions};
use crate::event_wait::{NameWatch, WatchesName};
use crate::generated::send_tool::{
    AnswersToolCall, DeclaresSendTool, HandlesSendCall, ParamsCarrying, ReadsSendArguments,
    ReadsToolCall, SendArguments, SendCall, SendInvocation, SendKind, SendOutcome, SendPriority,
    SendRefusal, ToolCall, ToolCallCarrying, ToolCallKey, ToolName,
};
use crate::launching::NamesTopic;
use crate::live::HoldsLiveValue;
use crate::sending::{AdmitsSendPayload, MintsId};
use crate::store::logical_delivery::RecordsLogicalDeliveries;
use crate::store::send_call::KeepsSendCalls;
use crate::store::{NamesLiveFlow, ReadsFlowRoles, ReadsFlowRows};
use crate::submission::NotesAppServerMessage;
use crate::waking::{RestsEndedAwake, SleepsAndWakesSeats};
use crate::{HoldsStopRequest, RunningNexus, store::AnnouncesLaunchChanges};
use datom_codec::Datomizable;
use protos::{Compactable, Protosizable};
use sha2::{Digest, Sha256};
use signal_flow::{Address, Aspect, HarnessKind, Message, Model, Request};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, mpsc},
    thread::Scope,
    time::Instant,
};

/// The tool every Codex thread Flow opens declares.
pub struct SendTool;

impl DeclaresSendTool for SendTool {
    fn tool_name() -> ToolName {
        crate::generated::send_tool::ToolName("send".into())
    }

    fn tool_spec() -> serde_json::Value {
        serde_json::json!({
            "type": "function",
            "name": Self::tool_name().0,
            "description": "Send one request from this flow to the flow at a metaflow Address. Priority determines interruption and whether an asleep recipient wakes. Result answers the request id supplied in request. The answer is a durable logical-delivery outcome or a typed refusal.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "target": {
                        "type": "object",
                        "description": "The Address of the receiving metaflow.",
                        "properties": {
                            "aspect": { "type": "string", "enum": ["Psyche", "Mind", "Field"] },
                            "topic": { "type": "string", "minLength": 1 },
                            "model": {
                                "type": "string",
                                "enum": ["Fable", "Opus", "Sonnet", "Haiku", "Astra", "Sol", "Luna", "Terra"]
                            }
                        },
                        "required": ["aspect", "topic", "model"],
                        "additionalProperties": false
                    },
                    "priority": {
                        "type": "string",
                        "enum": ["HardAbrupt", "MiddleAbrupt", "Soft"],
                        "description": "How the admitted message is delivered to the recipient."
                    },
                    "kind": {
                        "type": "string",
                        "enum": ["Order", "Question", "Result", "Notice"],
                        "description": "Order: work for the target. Question: a question it answers. Result: a result it awaits. Notice: an error or blocker affecting its work."
                    },
                    "body": { "type": "string", "minLength": 1 },
                    "request": {
                        "type": "string",
                        "minLength": 1,
                        "description": "The request id answered by a Result. Present exactly for Result."
                    }
                },
                "required": ["target", "priority", "kind", "body"],
                "allOf": [{
                    "if": {
                        "properties": { "kind": { "const": "Result" } },
                        "required": ["kind"]
                    },
                    "then": { "required": ["request"] },
                    "else": { "not": { "required": ["request"] } }
                }],
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
                thread_id: crate::generated::send_tool::ThreadId(text("threadId")?),
                turn_id: crate::generated::send_tool::TurnId(text("turnId")?),
                call_id: crate::generated::send_tool::CallId(text("callId")?),
            },
            tool_name: crate::generated::send_tool::ToolName(text("tool")?),
            // Serialized back from the parsed value: the same arguments read
            // the same, however the app-server spaced them.
            arguments: crate::generated::send_tool::Arguments(
                serde_json::to_string(params.get("arguments")?).ok()?,
            ),
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
            send_invocation_id: crate::generated::send_tool::SendInvocationId(
                "send-invocation".minted_id(),
            ),
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
        let _ = call;
        let status = match &outcome {
            SendOutcome::Accepted(_) => "Accepted",
            SendOutcome::LegacyUnknown(_) => "LegacyUnknown",
            SendOutcome::Archived(_) => "Archived",
            SendOutcome::Refused(_) => "Refused",
            SendOutcome::Kept(_) => "Kept",
        };
        eprintln!("flow-nexus: send tool invocation {status}");
        outcome
    }
}

/// The typed values of a send call's JSON arguments, each read strictly:
/// a name outside its enumeration, a missing or empty string, or a field
/// the schema does not name reads as nothing.
trait ReadsSendFields {
    fn aspect(value: &serde_json::Value) -> Option<Aspect>;
    fn model(value: &serde_json::Value) -> Option<Model>;
    fn priority(value: &serde_json::Value) -> Option<SendPriority>;
    fn kind(value: &serde_json::Value, request: Option<&serde_json::Value>) -> Option<SendKind>;
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

    fn model(value: &serde_json::Value) -> Option<Model> {
        match value.as_str()? {
            "Fable" => Some(Model::Fable),
            "Opus" => Some(Model::Opus),
            "Sonnet" => Some(Model::Sonnet),
            "Haiku" => Some(Model::Haiku),
            "Astra" => Some(Model::Astra),
            "Sol" => Some(Model::Sol),
            "Luna" => Some(Model::Luna),
            "Terra" => Some(Model::Terra),
            _ => None,
        }
    }

    fn kind(value: &serde_json::Value, request: Option<&serde_json::Value>) -> Option<SendKind> {
        match value.as_str()? {
            "Order" => Some(SendKind::Order),
            "Question" => Some(SendKind::Question),
            "Result" => Some(SendKind::Result(signal_flow::RequestId(
                request?.as_str()?.to_owned(),
            ))),
            "Notice" => Some(SendKind::Notice),
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
        let arguments = serde_json::from_str::<serde_json::Value>(&self.arguments.0).ok()?;
        if !Self::only_fields(
            &arguments,
            &["target", "priority", "kind", "body", "request"],
        ) {
            return None;
        }
        let target = arguments.get("target")?;
        if !Self::only_fields(target, &["aspect", "topic", "model"]) {
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
        let request = arguments.get("request");
        let send_kind = Self::kind(arguments.get("kind")?, request)?;
        if matches!(send_kind, SendKind::Result(_)) != request.is_some() {
            return None;
        }
        Some(SendArguments {
            target: crate::generated::send_tool::Target(Address {
                aspect: Self::aspect(target.get("aspect")?)?,
                topic: signal_flow::Topic(topic.to_owned()),
                model: Self::model(target.get("model")?)?,
            }),
            send_priority: Self::priority(arguments.get("priority")?)?,
            send_kind,
            body: crate::generated::send_tool::Body(body.to_owned()),
        })
    }

    fn content_sha256(&self) -> crate::generated::send_tool::ContentSha256 {
        let mut digest = Sha256::new();
        digest.update(self.tool_name.0.as_bytes());
        digest.update([0]);
        digest.update(self.arguments.0.as_bytes());
        crate::generated::send_tool::ContentSha256(
            digest
                .finalize()
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect(),
        )
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
            SendKind::Order => Request::Order(body.0),
            SendKind::Question => Request::Question(body.0),
            SendKind::Result(request_id) => Request::Result(flow_ethos::ResultPayload::Answered(
                flow_ethos::ResultPayload_Answered_Data {
                    logical_delivery_id: flow_ethos::LogicalDeliveryId(String::new()),
                    request_id: request_id.clone(),
                    string: body.0,
                },
            )),
            SendKind::Notice => Request::Notice(body.0),
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

/// The typed tool answer a flow sees. The complete `SendOutcome` remains in
/// Memory; this projection carries the metaflows and state without the
/// private logical-delivery identity or the sender's run identity.
#[derive(datom_codec::Datomizable)]
struct VisibleToolAccepted {
    sender: signal_flow::SentBy,
    target: Address,
    visible_send_acceptance: signal_flow::VisibleSendAcceptance,
}

#[derive(datom_codec::Datomizable)]
struct VisibleToolLegacyUnknown {
    sender: signal_flow::SentBy,
    target: Address,
}

#[derive(datom_codec::Datomizable)]
struct VisibleToolKept {
    sender: signal_flow::SentBy,
    request_id: signal_flow::RequestId,
}

#[derive(datom_codec::Datomizable)]
enum VisibleToolOutcome {
    Accepted(VisibleToolAccepted),
    LegacyUnknown(VisibleToolLegacyUnknown),
    Archived(VisibleToolLegacyUnknown),
    Refused(SendRefusal),
    Kept(VisibleToolKept),
}

trait ProjectsToolOutcome {
    fn visible_tool_outcome(&self) -> VisibleToolOutcome;
}

impl ProjectsToolOutcome for SendOutcome {
    fn visible_tool_outcome(&self) -> VisibleToolOutcome {
        match self {
            SendOutcome::Accepted(value) => VisibleToolOutcome::Accepted(VisibleToolAccepted {
                sender: value.sent_by.clone(),
                target: value.target.clone().0,
                visible_send_acceptance: signal_flow::VisibleSendAcceptance {
                    delivery_cursor: value.send_acceptance.delivery_cursor,
                    request_id: value.send_acceptance.request_id.clone(),
                    visible_delivery_state:
                        crate::message_projection::ProjectsDeliveryState::visible_delivery_state(
                            &value.send_acceptance.delivery_state,
                        ),
                },
            }),
            SendOutcome::Archived(value) => {
                VisibleToolOutcome::Archived(VisibleToolLegacyUnknown {
                    sender: value.sent_by.clone(),
                    target: value.target.clone().0,
                })
            }
            SendOutcome::LegacyUnknown(value) => {
                VisibleToolOutcome::LegacyUnknown(VisibleToolLegacyUnknown {
                    sender: value.sent_by.clone(),
                    target: value.target.clone().0,
                })
            }
            SendOutcome::Refused(value) => VisibleToolOutcome::Refused(value.clone()),
            SendOutcome::Kept(value) => VisibleToolOutcome::Kept(VisibleToolKept {
                sender: value.sent_by.clone(),
                request_id: value.request_id.clone(),
            }),
        }
    }
}

impl AnswersToolCall for SendOutcome {
    fn tool_response(&self) -> serde_json::Value {
        serde_json::json!({
            "success": matches!(self, SendOutcome::Accepted(_) | SendOutcome::Kept(_)),
            "contentItems": [{
                "type": "inputText",
                "text": self.visible_tool_outcome().datomize(Vec::new()).protosize().compact()
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

pub(crate) struct SendCallAnswer {
    pub(crate) outcome: SendOutcome,
    _pending_answer: Option<crate::store::logical_delivery::PendingAnswerHold>,
}

pub(crate) trait HandlesSendCallAnswers {
    fn handle_send_call_answer<N: ToolCallCarrying>(&self, input: N) -> SendCallAnswer;
}

impl HandlesSendCallAnswers for RunningNexus {
    fn handle_send_call_answer<N: ToolCallCarrying>(&self, input: N) -> SendCallAnswer {
        let call = input.tool_call();
        let call_key = call.tool_call_key.clone();
        let content_sha256 = call.content_sha256();
        let previously_recorded = self
            .store
            .send_call(&call.tool_call_key)
            .ok()
            .flatten()
            .is_some();
        let (outcome, pending_answer) = {
            let _turn = self
                .dispatch_gate
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let outcome = self.handle_held_send_call(call);
            let pending_answer = if let SendOutcome::Accepted(accepted) = &outcome {
                use crate::store::logical_delivery::HoldsAdmissionAnswers;
                Some(
                    self.store
                        .hold_admission_answer(&accepted.send_acceptance.request_id),
                )
            } else {
                None
            };
            (outcome, pending_answer)
        };
        if !previously_recorded
            && let SendOutcome::Accepted(accepted) = &outcome
            && accepted.send_acceptance.delivery_state == signal_flow::DeliveryState::Queued
        {
            self.wake_due(accepted.target.clone().0);
        }
        let outcome = match outcome {
            SendOutcome::Accepted(mut accepted) => {
                if let Ok(Some(snapshot)) = self
                    .store
                    .logical_delivery_snapshot(&accepted.send_acceptance.logical_delivery_id)
                    && snapshot.delivery_state != accepted.send_acceptance.delivery_state
                {
                    accepted.send_acceptance.delivery_state = snapshot.delivery_state;
                    accepted.send_acceptance.delivery_cursor = snapshot.delivery_cursor;
                    let refreshed = SendOutcome::Accepted(accepted);
                    let _ = self.store.update_send_call(SendCall {
                        tool_call_key: call_key,
                        content_sha256,
                        send_outcome: refreshed.clone(),
                    });
                    refreshed
                } else {
                    SendOutcome::Accepted(accepted)
                }
            }
            other => other,
        };
        SendCallAnswer {
            outcome,
            _pending_answer: pending_answer,
        }
    }
}

impl HandlesSendCall for RunningNexus {
    fn handle_send_call<N: ToolCallCarrying>(&self, input: N) -> SendOutcome {
        self.handle_send_call_answer(input).outcome
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
                    key.call_id.0, key.thread_id.0
                );
                Self::refused(SendRefusal::Store)
            };
            // The sender is read again for every call, a replayed one
            // included: a thread no live flow of an Awake metaflow runs
            // sends as Unbound.
            let root_thread = self.codex_endpoints.current().root_thread(&key.thread_id.0);
            let sender = match self.store.thread_sender(&root_thread) {
                Ok(sender) => sender,
                Err(error) => return store_refused(error),
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
                    content_sha256: crate::generated::send_tool::ContentSha256(content_sha256),
                    send_outcome: outcome.clone(),
                }) {
                    Ok(()) => outcome,
                    Err(error) => store_refused(error),
                }
            };
            let Some(arguments) = call.send_arguments() else {
                return refused_kept(SendRefusal::Malformed, content_sha256.0);
            };
            // The target's topic is an uncapitalized camelCase identifier,
            // taken as written: its capitals are never lowered.
            if !arguments.target.0.topic.0.as_str().is_camel_case_topic() {
                return refused_kept(
                    SendRefusal::TopicRefused(arguments.target.0.topic.clone()),
                    content_sha256.0,
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
            if let Some(signal_flow::Refusal::Oversized(size)) = message.oversized_refusal(&sender)
            {
                return refused_kept(SendRefusal::Oversized(size), content_sha256.0);
            }
            // A Result goes to the sender of the request it names, whatever
            // target the call writes; one whose sender is no Address is
            // kept with the request.
            let target = {
                use crate::store::kept_answer::{AnswerRoute, RoutesAnswers};
                match self.store.answer_route(&message) {
                    Ok(AnswerRoute::Unanswering) => arguments.target.clone(),
                    Ok(AnswerRoute::To(address)) => crate::generated::send_tool::Target(address),
                    Ok(AnswerRoute::Kept(binding)) => {
                        let request_id = binding.request_id.clone();
                        let outcome = match self.store.keep_answer(binding, &sender, &message) {
                            Ok(()) => SendOutcome::Kept(crate::generated::send_tool::Kept_Data {
                                sent_by: sender.clone(),
                                request_id,
                            }),
                            Err(error) => return store_refused(error),
                        };
                        return match self.store.record_send_call(SendCall {
                            tool_call_key: key.clone(),
                            content_sha256,
                            send_outcome: outcome.clone(),
                        }) {
                            Ok(()) => outcome,
                            Err(error) => store_refused(error),
                        };
                    }
                    Err(error) => return store_refused(error),
                }
            };
            let logged = message.clone();
            let admitted_at = std::time::SystemTime::now();
            let outcome = self
                .store
                .accept_tool_send(key, &content_sha256.0, sender, &target, message, now)
                .unwrap_or_else(store_refused);
            // Each admission is logged with its kind; the send tool
            // holds no reminder, so a Task it admits names no deadline.
            if matches!(&outcome, SendOutcome::Accepted(_)) {
                use crate::admission::{Admission, LogsAdmission};
                let (signal_flow::Message::HardAbrupt(request)
                | signal_flow::Message::MiddleAbrupt(request)
                | signal_flow::Message::Soft(request)) = &logged;
                eprintln!(
                    "{}",
                    Admission {
                        request,
                        deadline: None,
                        at: admitted_at,
                    }
                    .log_line()
                );
                // Restore a formerly-awake target only after its logical
                // delivery and exact tool-call result are durable.
                self.rest_ended_awake(&target.0);
            }
            outcome
        })();
        self.journal_send_invocation(&call, outcome)
    }
}

/// When a responder connects again: once the app-server's control socket
/// appears at its name. The socket is watched before every attempt, so an
/// appearance while the attempt runs is not missed; an attempt that fails
/// waits for the next appearance, and nothing is tried again on a timer.
/// Codex's app-server (app-server-transport, 0.149) binds its socket in
/// place, made at its name (`IN_CREATE`), and sets its mode to 0600 once it
/// listens (`IN_ATTRIB`); a socket renamed into its name (`IN_MOVED_TO`)
/// is read the same.
pub(crate) trait AwaitsAppServerSocket {
    /// The watch of the socket of `adapter`, its cues sent to `signals` as
    /// `SocketAppeared(generation)`; None when the socket cannot be
    /// watched, so only the order to stop is waited for.
    fn socket_watch(
        adapter: &CodexAdapter,
        generation: u64,
        signals: &mpsc::Sender<ResponderSignal>,
    ) -> Option<NameWatch> {
        let signals = signals.clone();
        NameWatch::arm(
            std::path::Path::new(&adapter.socket),
            Box::new(move || {
                signals
                    .send(ResponderSignal::SocketAppeared(generation))
                    .is_ok()
            }),
        )
        .inspect_err(|error| {
            eprintln!(
                "flow-nexus: Codex app-server socket {} is not watched: {error}",
                adapter.socket
            );
        })
        .ok()
    }

    /// Waits for the socket's appearance seen by `generation`'s watch, or
    /// for the order to stop; true when told to stop.
    fn stopped_before_appearance(inbox: &mpsc::Receiver<ResponderSignal>, generation: u64) -> bool {
        loop {
            match inbox.recv() {
                Ok(ResponderSignal::Stop) | Err(mpsc::RecvError) => return true,
                Ok(ResponderSignal::SocketAppeared(seen)) if seen == generation => return false,
                // Events of a connection already closed, and cues of an
                // earlier attempt's watch.
                Ok(
                    ResponderSignal::Proxy(..)
                    | ResponderSignal::WorkEnded
                    | ResponderSignal::SocketAppeared(_),
                ) => {}
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

impl AwaitsAppServerSocket for RunningNexus {}

impl RespondsToSendCalls for RunningNexus {
    fn respond_for(
        &self,
        thread: &str,
        adapter: &CodexAdapter,
        signals: mpsc::Sender<ResponderSignal>,
        inbox: mpsc::Receiver<ResponderSignal>,
    ) {
        let notes: Arc<dyn NotesAppServerMessage> = self.seat_submissions.clone();
        let mut generation = 0;
        loop {
            generation += 1;
            // Armed before the look: the socket appearing while this
            // attempt runs is the cue for the next one.
            let watch = Self::socket_watch(adapter, generation, &signals);
            match adapter.subscribe_thread(thread, generation, &signals, &inbox, &notes) {
                Err(SubscriptionEnd::Stopped) => return,
                // No launch work is served beside a responder of its own.
                Err(SubscriptionEnd::WorkEnded) => continue,
                Err(SubscriptionEnd::Failed(error)) => {
                    eprintln!("flow-nexus: send tool for thread {thread} not subscribed: {error}");
                    if Self::stopped_before_appearance(&inbox, generation) {
                        return;
                    }
                    continue;
                }
                Ok(mut subscription) => {
                    drop(watch);
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
                            Err(SubscriptionEnd::WorkEnded) => {}
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
                    // The connection closed: the next attempt is made at
                    // once, its own watch armed first.
                    subscription.close();
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
        let answer = match request
            .get("params")
            .cloned()
            .and_then(ToolCall::read_tool_call::<serde_json::Value>)
        {
            Some(call) => self.handle_send_call_answer(call),
            None => SendCallAnswer {
                outcome: Self::refused(SendRefusal::Malformed),
                _pending_answer: None,
            },
        };
        let answered = subscription.answer(id, answer.outcome.tool_response());
        if let SendOutcome::Accepted(accepted) = answer.outcome {
            self.wake_due(accepted.target.0);
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
/// policy must be considered once the gate is given back: those admitted
/// during a launch, and the recipients of answers queued under the gate.
#[derive(Default)]
pub struct LaunchCalls {
    responder: std::sync::Mutex<Option<LaunchResponder>>,
    deferred_wakes: std::sync::Mutex<Vec<Address>>,
}

/// A running Codex launch serves its own thread's calls.
pub(crate) trait ServesLaunchCalls {
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
    /// Under the dispatch gate: keeps `target` to be considered for its wake
    /// once the gate is given back.
    fn defer_wake(&self, target: Address);
    /// Off the dispatch gate, considers every target deferred under it.
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
        // The work's end reaches the connection's inbox as an event of its
        // own, so the launch waits on the connection and the work together.
        let ended = responder._signals.clone();
        let outcome = std::thread::scope(|scope| {
            let running = scope.spawn(move || {
                let outcome = work();
                let _ = ended.send(ResponderSignal::WorkEnded);
                outcome
            });
            // The calls that arrived before the subscription was answered.
            for request in std::mem::take(&mut responder.subscription.early) {
                if let Err(error) = self.answer_launch_call(&mut responder, request) {
                    eprintln!("flow-nexus: launch send tool: {error}");
                    open = false;
                }
            }
            // Served until the work's end is read, so that event never
            // stays behind in the inbox of a responder that is kept.
            while open {
                match responder.subscription.next_event(&responder.inbox, None) {
                    Ok(ProxyEvent::Reply(Ok(message))) => {
                        if let Err(error) = self.answer_launch_call(&mut responder, message) {
                            eprintln!("flow-nexus: launch send tool: {error}");
                            open = false;
                        }
                    }
                    Ok(ProxyEvent::Upgraded | ProxyEvent::Ping(_)) => {}
                    Err(SubscriptionEnd::WorkEnded) => break,
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
        let answer = match request
            .get("params")
            .cloned()
            .and_then(ToolCall::read_tool_call::<serde_json::Value>)
        {
            Some(call) => {
                let outcome = self.handle_held_send_call(call);
                let pending_answer = if let SendOutcome::Accepted(accepted) = &outcome {
                    use crate::store::logical_delivery::HoldsAdmissionAnswers;
                    Some(
                        self.store
                            .hold_admission_answer(&accepted.send_acceptance.request_id),
                    )
                } else {
                    None
                };
                SendCallAnswer {
                    outcome,
                    _pending_answer: pending_answer,
                }
            }
            None => SendCallAnswer {
                outcome: Self::refused(SendRefusal::Malformed),
                _pending_answer: None,
            },
        };
        let answered = responder
            .subscription
            .answer(id, answer.outcome.tool_response());
        if let SendOutcome::Accepted(accepted) = answer.outcome {
            self.defer_wake(accepted.target.0);
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
        // The calls already read are those already in the inbox: they are
        // taken without waiting for any more.
        let read_by_now = Instant::now();
        while let Ok(ProxyEvent::Reply(Ok(message))) = responder
            .subscription
            .next_event(&responder.inbox, Some(read_by_now))
        {
            if self.answer_launch_call(&mut responder, message).is_err() {
                break;
            }
        }
        responder.subscription.close();
    }

    fn defer_wake(&self, target: Address) {
        let mut deferred = self
            .launch_calls
            .deferred_wakes
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if !deferred.contains(&target) {
            deferred.push(target);
        }
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
///
/// The app-server marks no end of the replay: it answers `thread/resume`
/// first and replays the pending calls after it. Both are written by the
/// thread's listener task, which takes each `thread/resume` of a running
/// thread in turn, its replay included, before the next. So the end is a
/// second `thread/resume` of the same thread on the same connection: its
/// answer comes after every call the first one replayed. A request any
/// other task answers (`thread/read`, say) has no such order.
///
/// The app-server reads a connection's messages in order, and takes each
/// answer to its request before it reads the next message. So once the
/// answers are written, a `thread/read`'s answer shows they were read, and
/// the connection is closed only then.
pub(crate) trait RefusesInterruptedCalls {
    /// The request id of the `thread/resume` that closes the replay.
    const REPLAY_BARRIER: u64 = 3;
    /// The request id of the `thread/read` that shows the answers read.
    const ANSWERS_READ: u64 = 4;
    /// Answers every call the app-server replays on `thread` before the
    /// barrier's answer: from its record when one holds the same call,
    /// else refused (`ThreadUnowned`; `KeyReused` when its key's record
    /// holds another call).
    fn refuse_interrupted_calls(&self, thread: &str, adapter: &CodexAdapter) -> InterruptedCalls;
    /// The answer to one replayed call; true when it came from its record.
    fn interrupted_call_outcome(&self, thread: &str, call: Option<ToolCall>)
    -> (SendOutcome, bool);
}

impl RefusesInterruptedCalls for RunningNexus {
    fn refuse_interrupted_calls(&self, thread: &str, adapter: &CodexAdapter) -> InterruptedCalls {
        let mut tally = InterruptedCalls::default();
        let (signals, inbox) = mpsc::channel();
        let notes: Arc<dyn NotesAppServerMessage> = self.seat_submissions.clone();
        let mut subscription = match adapter.subscribe_thread(thread, 1, &signals, &inbox, &notes) {
            Ok(subscription) => subscription,
            Err(SubscriptionEnd::Failed(error)) => {
                eprintln!("flow-nexus: interrupted launch thread {thread} not resumed: {error}");
                return tally;
            }
            Err(SubscriptionEnd::Stopped | SubscriptionEnd::WorkEnded) => return tally,
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
        // Every replayed call arrives before the barrier's answer, kept in
        // `early` in the order it came; the calls are answered after it.
        let barrier = subscription.exchange(
            &inbox,
            Self::REPLAY_BARRIER,
            "thread/resume",
            serde_json::json!({ "threadId": thread, "excludeTurns": true }),
        );
        if let Err(SubscriptionEnd::Failed(error)) = &barrier {
            eprintln!(
                "flow-nexus: interrupted launch thread {thread}: replay end not seen ({error}); the calls read are answered"
            );
        }
        let replayed = std::mem::take(&mut subscription.early);
        let written = replayed
            .into_iter()
            .all(|request| answer(&mut subscription, request));
        // What the barrier's own resume replays after its answer are the
        // calls just answered; they are not answered twice.
        if written
            && let Err(SubscriptionEnd::Failed(error)) = subscription.exchange(
                &inbox,
                Self::ANSWERS_READ,
                "thread/read",
                serde_json::json!({ "threadId": thread, "includeTurns": false }),
            )
        {
            eprintln!(
                "flow-nexus: interrupted launch thread {thread}: answers not shown read ({error})"
            );
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
            || call.tool_call_key.thread_id.0 != thread
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
                        call.tool_call_key.call_id.0
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
                || node.session_id.0.is_empty()
            {
                continue;
            }
            let adapter = nexus
                .store
                .role(&node.flow_id.0)
                .ok()
                .flatten()
                .and_then(|role| {
                    nexus
                        .codex_endpoints
                        .current()
                        .adapter_for(&role.model_name.0)
                        .ok()
                });
            match adapter {
                Some(adapter) => {
                    wanted.insert(node.session_id.0, adapter);
                }
                None => {
                    if self.unserved.insert(node.session_id.0.clone()) {
                        eprintln!(
                            "flow-nexus: send tool has no Codex endpoint for flow {} thread {}",
                            node.flow_id.0, node.session_id.0
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
