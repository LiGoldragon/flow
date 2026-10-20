//! A flow's own Send, Launch and BindSeat on the ordinary socket.
//!
//! None of them carries who asks: the Nexus reads the calling process from
//! its own socket. A process in a Herdr pane is the flow bound in that pane;
//! a command a Codex app-server started is the flow of the thread the
//! app-server named in that command's environment.
//!
//! Send queues a request for a metaflow, or types it into the recipient's
//! pane when its Priority is abrupt and the recipient's flow is live. Launch starts a
//! metaflow from its declaration in Flow's configuration. BindSeat binds the
//! seat the caller runs in, with the FlowId its native session is claimed
//! under, as the metaflow's flow. A Codex subagent's command binds the seat
//! of its session's root thread, and a Codex seat Herdr reports no session
//! for runs the thread its pane resumes in its foreground.

use crate::RunningNexus;
use crate::caller::{
    CallerProcess, KnowsHerdrServers, LocatesCallerPane, LocatesCodexThread, NamesRootThread,
};
use crate::codex::SelectsCodexEndpoint;
use crate::delivery::DeliversMessages;
use crate::herdr::ReadsHerdrPanes;
use crate::herdr::launch::ClaimsNativeIdentity;
use crate::metaflow::IdentifiesPeerProcess;
use crate::store::metaflow::{SleepsMetaflows, TakesLaunchAddress};
use crate::store::metaflow_declaration::ReadsMetaflowDeclarations;
use crate::store::send_call::KeepsSendCalls;
use crate::store::sent::{NamesCodexAppServers, QueuesSentRequests};
use crate::store::{FlowRegistration, NamesConfiguredModel, RegistersExistingFlow};
use crate::waking::{PerformsWakeAndDrain, RestsEndedAwake, SleepsAndWakesSeats, WakePolicy};
use datom_codec::Datomizable;
use protos::{Compactable, Protosizable};
use signal_flow::{
    Address, Aspect, Caller, EndpointSelection, FlowLifecycle, FlowNode, HarnessKind, HerdrRoute,
    Delivery, HerdrRouteSelection, Launch_Data, LaunchProfile, Layer, Message, Origin, Process,
    QueuedRequest, Recipient, Refusal, RememberedFlow, Request, Response, RouteReadiness,
    Send_Data, SentBy, StartRejection, Unknown_Data,
};
use std::sync::atomic::{AtomicU64, Ordering};

/// Names the metaflow the calling process runs in.
pub trait IdentifiesSender {
    fn sending_address(&self, peer: &Process) -> Result<Address, Box<Response>>;
}

impl IdentifiesSender for RunningNexus {
    fn sending_address(&self, peer: &Process) -> Result<Address, Box<Response>> {
        let unidentified = || Box::new(Response::Refused(Refusal::Unidentified(peer.clone())));
        match self.identify_peer_process(peer.clone()) {
            Ok(Response::Identified(address)) => return Ok(address),
            Ok(_) => {}
            Err(error) => {
                return Err(Box::new(Response::Refused(Refusal::Store(
                    error.to_string(),
                ))));
            }
        }
        let Ok(process_id) = u32::try_from(peer.pid) else {
            return Err(unidentified());
        };
        let app_servers = self
            .store
            .codex_app_servers()
            .map_err(|error| Box::new(Response::Refused(Refusal::Store(error.to_string()))))?;
        let Some(thread) = (CallerProcess { process_id }).codex_thread(&app_servers) else {
            return Err(unidentified());
        };
        match self.store.thread_sender(&thread) {
            Ok(Ok(address)) => Ok(address),
            Ok(Err(_)) => Err(unidentified()),
            Err(error) => Err(Box::new(Response::Refused(Refusal::Store(
                error.to_string(),
            )))),
        }
    }
}

/// One counter for the ids Flow mints for its own sends and launches.
static SENT: AtomicU64 = AtomicU64::new(0);

/// A prefix that mints a unique id under it.
pub trait MintsId {
    fn minted_id(&self) -> String;
}

impl MintsId for str {
    fn minted_id(&self) -> String {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|elapsed| elapsed.as_nanos())
            .unwrap_or_default();
        format!(
            "{self}-{nanos:x}-{:x}",
            SENT.fetch_add(1, Ordering::Relaxed)
        )
    }
}

/// The layer above, in the same aspect and topic.
pub trait NamesLayerAbove {
    fn above(&self) -> Option<Layer>;
}

impl NamesLayerAbove for Layer {
    fn above(&self) -> Option<Layer> {
        match self {
            Layer::Primary => None,
            Layer::Secondary => Some(Layer::Primary),
            Layer::Tertiary => Some(Layer::Secondary),
            Layer::Quaternary => Some(Layer::Tertiary),
        }
    }
}

/// What a Send answers.
pub struct SendAnswer {
    pub response: Response,
}

impl From<Response> for SendAnswer {
    fn from(response: Response) -> Self {
        Self { response }
    }
}

pub trait SendsRequests {
    /// Answers a Send from `peer`, under the dispatch gate.
    fn send_request(&self, send: Send_Data, peer: Option<&Process>) -> SendAnswer;
    /// Delivers every durable request currently queued at `address`.
    fn deliver_all(&self, address: Address, peer: Option<&Process>) -> Response;
    /// The flow the metaflow at `address` is Awake as, when it is live.
    fn live_awake_flow(&self, address: &Address) -> Option<String>;
}

impl SendsRequests for RunningNexus {
    fn send_request(&self, send: Send_Data, peer: Option<&Process>) -> SendAnswer {
        let _turn = self
            .dispatch_gate
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let Some(peer) = peer else {
            return Response::Refused(Refusal::Unidentified(Process { pid: 0, started: 0 })).into();
        };
        let Some(now) = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .ok()
            .and_then(|elapsed| i64::try_from(elapsed.as_secs()).ok())
        else {
            return Response::Refused(Refusal::Store("clock is outside the lease range".into()))
                .into();
        };
        let sender = match self.sending_address(peer) {
            Ok(sender) => sender,
            Err(refusal) => return (*refusal).into(),
        };
        let delivery = send.delivery;
        let recipient = match send.recipient {
            Recipient::Address(address) => address,
            Recipient::Up => match sender.layer.above() {
                Some(layer) => Address {
                    aspect: sender.aspect.clone(),
                    topic: sender.topic.clone(),
                    layer,
                },
                None => return Response::Refused(Refusal::NoneAbove).into(),
            },
        };
        // A metaflow left Awake as an ended flow is Asleep for what follows.
        self.rest_ended_awake(&recipient);
        let (abrupt, request) = match send.message {
            Message::HardAbrupt(request) => (Some(true), request),
            Message::MiddleAbrupt(request) => (Some(false), request),
            Message::Soft(request) => (None, request),
        };
        if let Some(hard) = abrupt
            && let Some(flow_id) = self.live_awake_flow(&recipient)
            && let Some(response) = self.deliver_abruptly(&sender, &flow_id, hard, &request)
        {
            return response.into();
        }
        let answer = match self.store.queue_sent(&sender, &recipient, request, now) {
            Ok(Response::Queued) => SendAnswer { response: Response::Queued },
            Ok(response) => response.into(),
            Err(error) => Response::Refused(Refusal::Store(error.to_string())).into(),
        };
        let queued = answer.response == Response::Queued;
        drop(_turn);
        if queued && delivery == Delivery::Wake {
            self.wake_due(recipient);
        }
        answer
    }

    fn live_awake_flow(&self, address: &Address) -> Option<String> {
        let metaflow = self.store.metaflow(address).ok()??;
        let flow_ethos::State::Awake(flow_id) = metaflow.state else {
            return None;
        };
        self.store
            .flow_is_live(&flow_id)
            .ok()
            .filter(|live| *live)
            .map(|_| flow_id)
    }

    fn deliver_all(&self, address: Address, peer: Option<&Process>) -> Response {
        let turn = self
            .dispatch_gate
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let Some(peer) = peer else {
            return Response::Refused(Refusal::Unidentified(Process { pid: 0, started: 0 }));
        };
        if let Err(refusal) = self.sending_address(peer) {
            return *refusal;
        }
        self.rest_ended_awake(&address);
        let asleep = match self.store.metaflow(&address) {
            Ok(Some(metaflow)) => {
                if metaflow.queue.is_empty() {
                    return Response::Delivered;
                }
                metaflow.state == flow_ethos::State::Asleep
            }
            Ok(None) => {
                return Response::Refused(Refusal::Unknown(Unknown_Data::Address(address)));
            }
            Err(error) => return Response::Refused(Refusal::Store(error.to_string())),
        };
        if !asleep {
            self.drain_all(address.clone());
        }
        drop(turn);
        if asleep {
            self.wake_due(address.clone());
        }
        match self.store.metaflow(&address) {
            Ok(Some(metaflow)) if metaflow.queue.is_empty() => Response::Delivered,
            Ok(Some(_)) => Response::Queued,
            Ok(None) => Response::Refused(Refusal::Unknown(Unknown_Data::Address(address))),
            Err(error) => Response::Refused(Refusal::Store(error.to_string())),
        }
    }
}

trait DeliversAbruptly {
    /// Types the request into the live flow's pane under its Priority head;
    /// None when the pane refused it, so the caller queues it instead.
    fn deliver_abruptly(
        &self,
        sender: &Address,
        flow_id: &str,
        hard: bool,
        request: &Request,
    ) -> Option<Response>;
}

impl DeliversAbruptly for RunningNexus {
    fn deliver_abruptly(
        &self,
        sender: &Address,
        flow_id: &str,
        hard: bool,
        request: &Request,
    ) -> Option<Response> {
        let text = QueuedRequest {
            sent_by: SentBy::Sender(sender.clone()),
            request: request.clone(),
        }
        .datomize(Vec::new())
        .protosize()
        .compact();
        let sending_flow = self
            .store
            .metaflow(sender)
            .ok()
            .flatten()
            .and_then(|metaflow| match metaflow.state {
                flow_ethos::State::Awake(flow_id) => Some(flow_id),
                flow_ethos::State::Asleep => None,
            });
        let letter = meta_signal_flow::Letter {
            message_id: "send".minted_id(),
            sender: match sending_flow {
                Some(flow_id) => meta_signal_flow::Sender::Flow(flow_id),
                None => meta_signal_flow::Sender::Owner,
            },
            content: meta_signal_flow::Content::Text(text),
        };
        let message = if hard {
            meta_signal_flow::Message::HardAbrupt(letter)
        } else {
            meta_signal_flow::Message::MiddleAbrupt(letter)
        };
        match self.deliver(meta_signal_flow::DeliveryRequest {
            delivery_id: "send-delivery".minted_id(),
            flow_id: flow_id.to_owned(),
            message,
        }) {
            meta_signal_flow::Response::Delivered(_) => Some(Response::Delivered),
            refused => {
                eprintln!("flow-nexus: abrupt send to {flow_id} queued instead: {refused:?}");
                None
            }
        }
    }
}

/// The effort a seat of one Address runs at: medium everywhere, low for an
/// OpenAI Quaternary.
pub trait ChoosesEffort {
    fn effort_of(address: &Address) -> &'static str {
        match (&address.layer, &address.aspect) {
            (Layer::Quaternary, Aspect::Mind | Aspect::Field) => "low",
            _ => "medium",
        }
    }
}

impl ChoosesEffort for RunningNexus {}

/// The Herdr session a launch opens its pane in when its caller runs in
/// none.
const DEFAULT_HERDR_SESSION: &str = "default";

pub trait LaunchesDeclared {
    /// The Start, or the Replace of the caller's own flow, a Launch from
    /// `caller` names; Err with the refusal when the Address is undeclared
    /// or its model unconfigured. It only reads; the Start or Replace it
    /// names is dispatched as any other.
    fn declared_launch(
        &self,
        launch: Launch_Data,
        caller: Option<&Process>,
    ) -> Result<signal_flow::Query, Box<Response>>;
}

impl LaunchesDeclared for RunningNexus {
    fn declared_launch(
        &self,
        launch: Launch_Data,
        caller: Option<&Process>,
    ) -> Result<signal_flow::Query, Box<Response>> {
        let store_refused = |error: crate::store::StoreError| {
            Box::new(Response::Refused(Refusal::Store(error.to_string())))
        };
        let address = launch.address;
        let Some(declaration) = self
            .store
            .metaflow_declaration(&address)
            .map_err(store_refused)?
        else {
            return Err(Box::new(Response::StartRejected(
                StartRejection::Undeclared,
            )));
        };
        let Some(model_name) = self
            .store
            .configured_model(&address)
            .map_err(store_refused)?
        else {
            return Err(Box::new(Response::Refused(Refusal::NotConfigured)));
        };
        // A flow launching its own Address is replaced by the launch.
        let predecessor = caller
            .and_then(|process| self.sending_address(process).ok())
            .filter(|sender| *sender == address)
            .and_then(|_| self.live_awake_flow(&address));
        let herdr_session_name = caller
            .and_then(|process| u32::try_from(process.pid).ok())
            .and_then(|process_id| CallerProcess { process_id }.caller_pane(&self.herdr))
            .map(|pane| pane.herdr_session_name)
            .unwrap_or_else(|| DEFAULT_HERDR_SESSION.to_owned());
        let profile = LaunchProfile {
            launch_request_id: "launch".minted_id(),
            system_modules: declaration.system_modules,
            user_modules: declaration.user_modules,
            address: address.clone(),
            model_name,
            effort: Self::effort_of(&address).to_owned(),
            flow_id_option: predecessor.clone(),
            remembered_flow_vector: predecessor
                .iter()
                .map(|flow_id| RememberedFlow {
                    flow_id: flow_id.clone(),
                    remembering_depth: 1,
                })
                .collect(),
            herdr_session_name,
            leave_outs_option: None,
            instruction_prompt: launch.instruction_prompt,
        };
        Ok(match predecessor {
            Some(_) => signal_flow::Query::Replace(profile),
            None => signal_flow::Query::Start(profile),
        })
    }
}

/// One Herdr agent, as a session snapshot shows it.
struct SeenSeat {
    herdr_session_name: String,
    agent: serde_json::Value,
}

trait FindsSeat {
    /// The agent in the caller's pane, or, for a command a Codex
    /// app-server started, the one agent running the root thread of the
    /// caller's thread, with the Codex thread the seat runs. A Codex agent
    /// Herdr reports no session for runs the thread its pane's foreground
    /// resumes.
    fn seat_of(&self, caller: CallerProcess) -> Option<(SeenSeat, Option<String>)>;
}

/// A Codex agent's thread, when Herdr reports no session for it.
trait ReadsUnreportedThread {
    /// The one thread a Codex agent's pane resumes in its foreground, when
    /// the agent reports no session; None otherwise.
    fn unreported_thread(
        &self,
        herdr_session_name: &str,
        agent: &serde_json::Value,
    ) -> Option<String>;
}

impl ReadsUnreportedThread for RunningNexus {
    fn unreported_thread(
        &self,
        herdr_session_name: &str,
        agent: &serde_json::Value,
    ) -> Option<String> {
        let text = |key: &str| agent.get(key).and_then(serde_json::Value::as_str);
        if text("agent") != Some("codex") || agent.pointer("/agent_session/value").is_some() {
            return None;
        }
        let route = HerdrRoute {
            herdr_session_name: herdr_session_name.to_owned(),
            herdr_agent_name: text("name").unwrap_or_default().to_owned(),
            herdr_pane_id: text("pane_id")?.to_owned(),
            herdr_terminal_id: text("terminal_id")?.to_owned(),
        };
        let threads = self.herdr.foreground_threads(&route)?;
        let mut threads = threads.into_iter();
        match (threads.next(), threads.next()) {
            (Some(thread), None) => Some(thread),
            _ => None,
        }
    }
}

impl FindsSeat for RunningNexus {
    fn seat_of(&self, caller: CallerProcess) -> Option<(SeenSeat, Option<String>)> {
        let agents_of = |session: &str| -> Vec<serde_json::Value> {
            self.herdr
                .session_snapshot(session)
                .and_then(|snapshot| {
                    snapshot
                        .pointer("/result/snapshot/agents")
                        .and_then(serde_json::Value::as_array)
                        .cloned()
                })
                .unwrap_or_default()
        };
        if let Some(pane) = caller.caller_pane(&self.herdr) {
            let agent = agents_of(&pane.herdr_session_name)
                .into_iter()
                .find(|agent| {
                    agent.get("pane_id").and_then(serde_json::Value::as_str)
                        == Some(pane.herdr_pane_id.as_str())
                })?;
            let thread = self.unreported_thread(&pane.herdr_session_name, &agent);
            return Some((
                SeenSeat {
                    herdr_session_name: pane.herdr_session_name,
                    agent,
                },
                thread,
            ));
        }
        let app_servers = self.store.codex_app_servers().ok()?;
        let thread = self
            .codex_endpoints
            .root_thread(&caller.codex_thread(&app_servers)?);
        let mut seats = Vec::new();
        for session in self.herdr.herdr_sessions()? {
            for agent in agents_of(&session.name) {
                let runs_thread = match agent
                    .pointer("/agent_session/value")
                    .and_then(serde_json::Value::as_str)
                {
                    Some(reported) => reported == thread,
                    None => {
                        self.unreported_thread(&session.name, &agent).as_deref()
                            == Some(thread.as_str())
                    }
                };
                if runs_thread {
                    seats.push(SeenSeat {
                        herdr_session_name: session.name.clone(),
                        agent,
                    });
                }
            }
        }
        let mut seats = seats.into_iter();
        match (seats.next(), seats.next()) {
            (Some(seat), None) => Some((seat, Some(thread))),
            _ => None,
        }
    }
}

pub trait BindsSeats {
    /// Binds the seat `peer` runs in as the metaflow at `address`.
    fn bind_seat(&self, address: Address, peer: Option<&Process>) -> Response;
}

impl BindsSeats for RunningNexus {
    fn bind_seat(&self, address: Address, peer: Option<&Process>) -> Response {
        let _turn = self
            .dispatch_gate
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let Some(peer) = peer else {
            return Response::Refused(Refusal::Unidentified(Process { pid: 0, started: 0 }));
        };
        let unidentified = || Response::Refused(Refusal::Unidentified(peer.clone()));
        let Some(process_id) = u32::try_from(peer.pid).ok() else {
            return unidentified();
        };
        let Some((seat, thread)) = self.seat_of(CallerProcess { process_id }) else {
            return unidentified();
        };
        let text = |pointer: &str| {
            seat.agent
                .pointer(pointer)
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned)
        };
        let harness_kind = match text("/agent").as_deref() {
            Some("claude") => HarnessKind::Claude,
            Some("codex") => HarnessKind::Codex,
            _ => return unidentified(),
        };
        let Some(native_session_id) = text("/agent_session/value").or(thread) else {
            return unidentified();
        };
        let (Some(herdr_pane_id), Some(herdr_terminal_id)) =
            (text("/pane_id"), text("/terminal_id"))
        else {
            return unidentified();
        };
        let herdr_agent_name = text("/name").unwrap_or_default();
        let Ok(flow_id) = self
            .herdr
            .claim_flow_identity(&harness_kind, &native_session_id)
        else {
            return unidentified();
        };
        let model_name = self
            .store
            .configured_model(&address)
            .ok()
            .flatten()
            .unwrap_or_default();
        let endpoint_selection = match harness_kind {
            HarnessKind::Codex => match self.codex_endpoints.endpoint_for(&model_name) {
                Ok(endpoint) => EndpointSelection::Available(signal_flow::Available_Data {
                    endpoint_path: endpoint.socket.clone(),
                    route_readiness: RouteReadiness::Ready,
                }),
                Err(_) => EndpointSelection::Unavailable,
            },
            HarnessKind::Claude => EndpointSelection::Unavailable,
        };
        let node = FlowNode {
            flow_id: flow_id.clone(),
            session_id: native_session_id,
            harness_kind,
            endpoint_selection,
            herdr_route_selection: HerdrRouteSelection::Available(HerdrRoute {
                herdr_session_name: seat.herdr_session_name,
                herdr_agent_name,
                herdr_pane_id,
                herdr_terminal_id,
            }),
            origin: Origin::Unbound,
            flow_lifecycle: FlowLifecycle::Pending,
        };
        let role = Caller {
            flow_id: flow_id.clone(),
            aspect: address.aspect.clone(),
            power_level: Self::power_of(&address.layer),
            model_name,
        };
        match self.store.register_existing_flow(node, role) {
            Ok(FlowRegistration::Registered(_)) => {}
            Ok(FlowRegistration::ConflictingBinding) => {
                return Response::Refused(Refusal::Taken(address));
            }
            Err(error) => return Response::Refused(Refusal::Store(error.to_string())),
        }
        // A metaflow left Awake as an ended flow is taken over.
        self.rest_ended_awake(&address);
        match self.store.address_taking(&address, &flow_id, None, 0) {
            Ok(crate::store::metaflow::AddressTaking::Takes(_)) => {}
            Ok(crate::store::metaflow::AddressTaking::Refused(_)) => {
                return Response::Refused(Refusal::Taken(address));
            }
            Err(error) => return Response::Refused(Refusal::Store(error.to_string())),
        }
        match self.store.awaken_metaflow(&address, &flow_id, 0) {
            Ok(true) => Response::Bound(flow_id),
            Ok(false) => Response::Refused(Refusal::Taken(address)),
            Err(error) => Response::Refused(Refusal::Store(error.to_string())),
        }
    }
}
