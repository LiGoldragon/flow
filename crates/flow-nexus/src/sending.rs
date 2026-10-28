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
use crate::binding::{ReadsProcessIdentity, VerifiesAdoptedSeat};
use crate::caller::{
    BindsCallerPane, CallerProcess, KnowsHerdrServers, LocatesCallerPane, LocatesCodexThread,
    NamesRootThread,
};
use crate::composition::JoinsPurpose;
use crate::delivery::body::RendersNativeContent;
use crate::herdr::ReadsHerdrPanes;
use crate::herdr::launch::ClaimsNativeIdentity;
use crate::metaflow::IdentifiesPeerProcess;
use crate::store::address_hold::HoldsLaunchAddress;
use crate::store::context::JudgesThreshold;
use crate::store::logical_delivery::RecordsLogicalDeliveries;
use crate::store::metaflow::{
    MutatesMetaflows, PreparesDeclaredMetaflow, SleepsMetaflows, TakesLaunchAddress,
};
use crate::store::metaflow_declaration::ReadsMetaflowDeclarations;
use crate::store::queue_delivery::AccountsQueueDelivery;
use crate::store::send_call::KeepsSendCalls;
use crate::store::send_outcome::RecordsSendOutcomes;
use crate::store::sent::NamesCodexAppServers;
use crate::store::{
    ExistingSeatAdoption, FlowStore, ManagesMetaflowState, NamesConfiguredModel, NamesLiveFlow,
    ProcessSenderResolution, ReadsFlowMemory, ReadsFlowRows, RegistersExistingFlow,
};
use crate::waking::{
    HoldsWakingMetaflows, PerformsWakeAndDrain, ReconcilesQueueDelivery, RestsEndedAwake,
    SleepsAndWakesSeats,
};
use datom_codec::Datomizable;
use protos::{Compactable, Protosizable};
use signal_flow::{
    Address, Aspect, Bind_Data, Caller, Delivery, HarnessKind, HerdrRoute, Launch_Data,
    LaunchProfile, Layer, Message, Process, Recipient, Refusal, Request, Response, SendPayloadSize,
    SendRequest, StartRejection, Unknown_Data,
};
use std::sync::atomic::{AtomicU64, Ordering};

#[cfg(test)]
pub const MAX_SEND_PAYLOAD_BYTES: usize = 64 * 1024;
/// Linux accepts at most 131072 bytes in one argv string, including its NUL.
/// Herdr carries one canonical cohort as one argument, so Flow reserves the
/// final byte for that terminator. Individual messages retain their separate
/// 64 KiB admission bound above.
pub const MAX_NATIVE_COHORT_BYTES: usize = 128 * 1024 - 1;

/// Measures a Request as it would appear in the recipient's native prompt.
pub trait MeasuresSendPayload {
    fn native_prompt_bytes(&self, sender: &Address) -> usize;
}

impl MeasuresSendPayload for Request {
    fn native_prompt_bytes(&self, sender: &Address) -> usize {
        Message::Soft(self.clone()).native_prompt_bytes(sender)
    }
}

impl MeasuresSendPayload for Message {
    fn native_prompt_bytes(&self, sender: &Address) -> usize {
        let _ = sender;
        self.native_content().len()
    }
}

/// Refuses a Send whose complete native prompt rendering exceeds Flow's budget.
pub trait AdmitsSendPayload {
    fn oversized_refusal(&self, sender: &Address, maximum_bytes: usize) -> Option<Refusal>;
}

impl AdmitsSendPayload for Request {
    fn oversized_refusal(&self, sender: &Address, maximum_bytes: usize) -> Option<Refusal> {
        let actual_bytes = self.native_prompt_bytes(sender);
        (actual_bytes > maximum_bytes).then_some(Refusal::Oversized(SendPayloadSize {
            actual_bytes: i64::try_from(actual_bytes).unwrap_or(i64::MAX),
            maximum_bytes: maximum_bytes as i64,
        }))
    }
}

impl AdmitsSendPayload for Message {
    fn oversized_refusal(&self, sender: &Address, maximum_bytes: usize) -> Option<Refusal> {
        let actual_bytes = self.native_prompt_bytes(sender);
        (actual_bytes > maximum_bytes).then_some(Refusal::Oversized(SendPayloadSize {
            actual_bytes: i64::try_from(actual_bytes).unwrap_or(i64::MAX),
            maximum_bytes: maximum_bytes as i64,
        }))
    }
}

/// Names the metaflow the calling process runs in.
pub trait IdentifiesSender {
    fn sending_sender(
        &self,
        configured: &crate::ConfiguredUse,
        peer: &Process,
    ) -> Result<meta_signal_flow::Sender, Box<Response>>;
    fn sending_address(
        &self,
        configured: &crate::ConfiguredUse,
        peer: &Process,
    ) -> Result<Address, Box<Response>>;
}

impl IdentifiesSender for RunningNexus {
    fn sending_sender(
        &self,
        configured: &crate::ConfiguredUse,
        peer: &Process,
    ) -> Result<meta_signal_flow::Sender, Box<Response>> {
        let unidentified = || Box::new(Response::Refused(Refusal::Unidentified(peer.clone())));
        match self.store.identify_process_sender(peer.clone()) {
            Ok(ProcessSenderResolution::Sender(sender)) => return Ok(sender),
            Ok(
                ProcessSenderResolution::MissingFlowIdentity
                | ProcessSenderResolution::StaleFlowIdentity,
            ) => return Err(unidentified()),
            Ok(ProcessSenderResolution::NoBinding) => {}
            Err(error) => {
                return Err(Box::new(Response::Refused(Refusal::Store(
                    error.to_string(),
                ))));
            }
        }
        let Ok(process_id) = u32::try_from(peer.pid) else {
            return Err(unidentified());
        };
        if let Some(pane) = (CallerProcess { process_id }).caller_pane(&configured.herdr)
            && let Ok(caller) = self.bound_caller(configured, &pane)
        {
            let address = match self.identify_peer_process_configured(configured, peer.clone()) {
                Ok(Response::Identified(address)) => address,
                Ok(_) => return Err(unidentified()),
                Err(error) => {
                    return Err(Box::new(Response::Refused(Refusal::Store(
                        error.to_string(),
                    ))));
                }
            };
            match self.store.metaflow_awake_as(&caller.flow_id) {
                Ok(Some(metaflow)) if metaflow.address == address => {
                    return Ok(meta_signal_flow::Sender {
                        address,
                        flow_id: caller.flow_id,
                    });
                }
                Ok(_) => return Err(unidentified()),
                Err(error) => {
                    return Err(Box::new(Response::Refused(Refusal::Store(
                        error.to_string(),
                    ))));
                }
            }
        }
        let app_servers = self
            .store
            .codex_app_servers()
            .map_err(|error| Box::new(Response::Refused(Refusal::Store(error.to_string()))))?;
        let Some(thread) = (CallerProcess { process_id }).codex_thread(&app_servers) else {
            return Err(unidentified());
        };
        let root_thread = configured.codex_endpoints.root_thread(&thread);
        let address = match self.store.thread_sender(&root_thread) {
            Ok(Ok(address)) => address,
            Ok(Err(_)) => return Err(unidentified()),
            Err(error) => {
                return Err(Box::new(Response::Refused(Refusal::Store(
                    error.to_string(),
                ))));
            }
        };
        let nodes = self
            .store
            .flow_nodes()
            .map_err(|error| Box::new(Response::Refused(Refusal::Store(error.to_string()))))?;
        let mut owners = nodes.into_iter().filter(|node| {
            node.harness_kind == signal_flow::HarnessKind::Codex
                && node.session_id == root_thread
                && node.flow_lifecycle.is_live()
        });
        match (owners.next(), owners.next()) {
            (Some(owner), None) => Ok(meta_signal_flow::Sender {
                address,
                flow_id: owner.flow_id,
            }),
            (None, None) => Err(unidentified()),
            _ => Err(Box::new(Response::Refused(Refusal::Store(
                "multiple live flows own the authenticated Codex thread".into(),
            )))),
        }
    }

    fn sending_address(
        &self,
        configured: &crate::ConfiguredUse,
        peer: &Process,
    ) -> Result<Address, Box<Response>> {
        self.sending_sender(configured, peer)
            .map(|sender| sender.address)
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
pub struct PendingSend {
    pub address: Address,
    pub delivery: Delivery,
}

pub struct SendAnswer {
    pub response: Response,
    pub pending: Option<PendingSend>,
}

impl From<Response> for SendAnswer {
    fn from(response: Response) -> Self {
        Self {
            response,
            pending: None,
        }
    }
}

pub trait SendsRequests {
    /// Answers a Send from `peer`, under the dispatch gate.
    fn send_request(
        &self,
        configured: &crate::ConfiguredUse,
        send: SendRequest,
        peer: Option<&Process>,
    ) -> SendAnswer;
    /// Delivers every durable request currently queued at `address`.
    fn deliver_all(
        &self,
        configured: &crate::ConfiguredUse,
        address: Address,
        peer: Option<&Process>,
    ) -> Response;
    /// The flow the metaflow at `address` is Awake as, when it is live.
    fn live_awake_flow(&self, address: &Address) -> Option<String>;
}

impl SendsRequests for RunningNexus {
    fn send_request(
        &self,
        configured: &crate::ConfiguredUse,
        send: SendRequest,
        peer: Option<&Process>,
    ) -> SendAnswer {
        use sha2::{Digest, Sha256};

        let abrupt = !matches!(&send.message, Message::Soft(_));
        let direct = abrupt && send.delivery == Delivery::Wake;
        let digest_text = send.datomize(Vec::new()).protosize().compact();
        let request_digest = format!("{:x}", Sha256::digest(digest_text.as_bytes()));
        let recipient = match &send.recipient {
            Recipient::Address(address) => Some(address.clone()),
            Recipient::Up => peer
                .and_then(|peer| self.sending_address(configured, peer).ok())
                .and_then(|sender| {
                    sender.layer.above().map(|layer| Address {
                        aspect: sender.aspect,
                        topic: sender.topic,
                        layer,
                    })
                }),
        };
        let sender = peer.and_then(|peer| self.sending_address(configured, peer).ok());
        let mut answer = (|| -> SendAnswer {
            let _turn = self
                .dispatch_gate
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            // The recipient's topic is input, checked before anything else:
            // an uncapitalized camelCase identifier, kept as written.
            if let Recipient::Address(address) = &send.recipient {
                use crate::launching::NamesTopic;
                if !address.topic.as_str().is_camel_case_topic() {
                    return Response::Refused(Refusal::TopicRefused(address.topic.clone())).into();
                }
            }
            let Some(peer) = peer else {
                return Response::Refused(Refusal::Unidentified(Process { pid: 0, started: 0 }))
                    .into();
            };
            let Some(_now) = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .ok()
                .and_then(|elapsed| i64::try_from(elapsed.as_secs()).ok())
            else {
                return Response::Refused(Refusal::Store(
                    "clock is outside the lease range".into(),
                ))
                .into();
            };
            let sender = match self.sending_sender(configured, peer) {
                Ok(sender) => sender,
                Err(refusal) => return (*refusal).into(),
            };
            let delivery = send.delivery.clone();
            let recipient = match send.recipient {
                Recipient::Address(address) => address,
                Recipient::Up => match sender.address.layer.above() {
                    Some(layer) => Address {
                        aspect: sender.address.aspect.clone(),
                        topic: sender.address.topic.clone(),
                        layer,
                    },
                    None => return Response::Refused(Refusal::NoneAbove).into(),
                },
            };
            self.rest_ended_awake(&configured, &recipient);
            // Own the address until the direct call has a receipt or a
            // durable non-retryable failure. Background drain/wake may not
            // submit this same request independently. A metaflow another
            // holds takes the request into its queue instead.
            let direct_lease = if direct {
                match self.direct_send_lease(configured, &recipient) {
                    Ok(lease) => lease,
                    Err(refusal) => return (*refusal).into(),
                }
            } else {
                None
            };
            let direct = direct_lease.is_some();
            let admitted = self.store.accept_logical_delivery(
                &recipient,
                sender,
                delivery.clone(),
                send.message,
                None,
            );
            drop(_turn);
            match admitted {
                Ok(Response::SendAccepted(acceptance)) if direct => self.complete_direct_send(
                    configured,
                    &recipient,
                    acceptance.logical_delivery_id,
                ),
                Ok(response) => {
                    let pending =
                        matches!(response, Response::SendAccepted(_)).then(|| PendingSend {
                            address: recipient,
                            delivery,
                        });
                    SendAnswer { response, pending }
                }
                Err(error) => Response::Refused(Refusal::Store(error.to_string())).into(),
            }
        })();
        if (direct || abrupt)
            && let Response::Refused(refusal) = &answer.response
        {
            answer.response = Response::Failed(signal_flow::SendFailure {
                logical_delivery_id_option: None,
                send_failure_reason: signal_flow::SendFailureReason::Refused(refusal.clone()),
            });
        }
        match self.store.record_send_outcome(
            peer.cloned(),
            sender,
            recipient,
            request_digest,
            answer.response.clone(),
        ) {
            Ok(_) => answer,
            Err(error) => {
                eprintln!("send outcome journal failed: {error}");
                // The logical acceptance/event is already durable. Do not
                // turn an admitted request into a false admission refusal.
                answer
            }
        }
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

    fn deliver_all(
        &self,
        configured: &crate::ConfiguredUse,
        address: Address,
        peer: Option<&Process>,
    ) -> Response {
        let turn = self
            .dispatch_gate
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let Some(peer) = peer else {
            return Response::Refused(Refusal::Unidentified(Process { pid: 0, started: 0 }));
        };
        if let Err(refusal) = self.sending_address(configured, peer) {
            return *refusal;
        }
        self.rest_ended_awake(&configured, &address);
        let asleep = match self.store.metaflow(&address) {
            Ok(Some(metaflow)) => {
                if metaflow.queue.is_empty() {
                    return Response::Drained(signal_flow::DrainOutcome {
                        address: address.clone(),
                        drain_state: signal_flow::DrainState::Delivered,
                    });
                }
                metaflow.state == flow_ethos::State::Asleep
            }
            Ok(None) => {
                return Response::Refused(Refusal::Unknown(Unknown_Data::Address(address)));
            }
            Err(error) => return Response::Refused(Refusal::Store(error.to_string())),
        };
        if let Err(error) = self.store.force_queue_wake(&address) {
            return Response::Refused(Refusal::Store(error.to_string()));
        }
        if !asleep {
            self.drain_all_configured(configured, address.clone());
        }
        drop(turn);
        if asleep {
            self.wake_due_configured(configured, address.clone());
        }
        match self.store.metaflow(&address) {
            Ok(Some(metaflow)) if metaflow.queue.is_empty() => {
                Response::Drained(signal_flow::DrainOutcome {
                    address: address.clone(),
                    drain_state: signal_flow::DrainState::Delivered,
                })
            }
            Ok(Some(metaflow)) => Response::Drained(signal_flow::DrainOutcome {
                address: address.clone(),
                drain_state: signal_flow::DrainState::Held(
                    metaflow
                        .queue
                        .iter()
                        .map(|entry| entry.logical_delivery_id.clone())
                        .collect(),
                ),
            }),
            Ok(None) => Response::Refused(Refusal::Unknown(Unknown_Data::Address(address))),
            Err(error) => Response::Refused(Refusal::Store(error.to_string())),
        }
    }
}

/// The Herdr session a launch opens its pane in when its caller runs in
/// none.
const DEFAULT_HERDR_SESSION: &str = "default";

pub trait LaunchesDeclared {
    /// The Start a Launch from `caller` names, or, when the metaflow is
    /// Awake as a live flow, whoever the caller is, the Replace of that
    /// flow; Err with the refusal when the Address is undeclared or its
    /// model unconfigured. It only reads; the Start or Replace it names is
    /// dispatched as any other.
    fn declared_launch(
        &self,
        configured: &crate::ConfiguredUse,
        launch: Launch_Data,
        caller: Option<&Process>,
    ) -> Result<signal_flow::Query, Box<Response>>;
}

impl LaunchesDeclared for RunningNexus {
    fn declared_launch(
        &self,
        configured: &crate::ConfiguredUse,
        launch: Launch_Data,
        caller: Option<&Process>,
    ) -> Result<signal_flow::Query, Box<Response>> {
        let store_refused = |error: crate::store::StoreError| {
            Box::new(Response::Refused(Refusal::Store(error.to_string())))
        };
        let address = launch.address;
        // The topic is input: checked before the declaration is looked up.
        {
            use crate::launching::NamesTopic;
            if !address.topic.as_str().is_camel_case_topic() {
                return Err(Box::new(Response::StartRejected(
                    StartRejection::TopicRefused(address.topic.clone()),
                )));
            }
        }
        let configuration = &configured.snapshot;
        let Some(declaration) = configuration
            .configured_metaflows
            .iter()
            .find(|declaration| declaration.address == address)
            .cloned()
        else {
            return Err(Box::new(Response::StartRejected(
                StartRejection::Undeclared,
            )));
        };
        let Some(model) = configuration
            .configured_models
            .iter()
            .find(|model| model.layer == address.layer && model.aspect == address.aspect)
        else {
            return Err(Box::new(Response::Refused(Refusal::NotConfigured)));
        };
        let model_name = model.model_name.clone();
        let effort = model.effort.clone();
        // A Launch addressed to an Awake metaflow refreshes it: the flow it
        // is Awake as is replaced by the launch, never typed into.
        let predecessor = self.live_awake_flow(&address);
        let herdr_session_name = caller
            .and_then(|process| u32::try_from(process.pid).ok())
            .and_then(|process_id| CallerProcess { process_id }.caller_pane(&configured.herdr))
            .map(|pane| pane.herdr_session_name)
            .unwrap_or_else(|| DEFAULT_HERDR_SESSION.to_owned());
        let profile = LaunchProfile {
            launch_request_id: "launch".minted_id(),
            system_modules: declaration.system_modules.clone(),
            user_modules: declaration.user_modules.clone(),
            address: address.clone(),
            model_name,
            effort,
            flow_id_option: predecessor.clone(),
            remembering: launch.remembering,
            herdr_session_name,
            left_out_option: None,
            tool_set: declaration.tool_set.clone(),
            // The declaration's purpose is composed ahead of the brief.
            instruction_prompt: crate::composition::with_purpose_configured(
                &launch.instruction_prompt,
                &declaration.purpose,
                &configuration.texts,
            ),
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
    fn seat_of(
        &self,
        configured: &crate::ConfiguredUse,
        caller: CallerProcess,
    ) -> Option<(SeenSeat, Option<String>)>;
}

/// A Codex agent's thread, when Herdr reports no session for it.
trait ReadsUnreportedThread {
    /// The one thread a Codex agent's pane resumes in its foreground, when
    /// the agent reports no session; None otherwise.
    fn unreported_thread(
        &self,
        configured: &crate::ConfiguredUse,
        herdr_session_name: &str,
        agent: &serde_json::Value,
    ) -> Option<String>;
}

impl ReadsUnreportedThread for RunningNexus {
    fn unreported_thread(
        &self,
        configured: &crate::ConfiguredUse,
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
        let threads = configured.herdr.foreground_threads(&route)?;
        let mut threads = threads.into_iter();
        match (threads.next(), threads.next()) {
            (Some(thread), None) => Some(thread),
            _ => None,
        }
    }
}

impl FindsSeat for RunningNexus {
    fn seat_of(
        &self,
        configured: &crate::ConfiguredUse,
        caller: CallerProcess,
    ) -> Option<(SeenSeat, Option<String>)> {
        let agents_of = |session: &str| -> Vec<serde_json::Value> {
            configured
                .herdr
                .session_snapshot(session)
                .and_then(|snapshot| {
                    snapshot
                        .pointer("/result/snapshot/agents")
                        .and_then(serde_json::Value::as_array)
                        .cloned()
                })
                .unwrap_or_default()
        };
        if let Some(pane) = caller.caller_pane(&configured.herdr) {
            let agent = agents_of(&pane.herdr_session_name)
                .into_iter()
                .find(|agent| {
                    agent.get("pane_id").and_then(serde_json::Value::as_str)
                        == Some(pane.herdr_pane_id.as_str())
                })?;
            if agent.get("agent").and_then(serde_json::Value::as_str) != Some("codex") {
                return None;
            }
            let app_servers = self.store.codex_app_servers().ok()?;
            let caller_thread = configured
                .codex_endpoints
                .root_thread(&caller.codex_thread(&app_servers)?);
            let seat_thread = agent
                .pointer("/agent_session/value")
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned)
                .or_else(|| self.unreported_thread(configured, &pane.herdr_session_name, &agent))?;
            if seat_thread != caller_thread {
                return None;
            }
            let thread = Some(caller_thread);
            return Some((
                SeenSeat {
                    herdr_session_name: pane.herdr_session_name,
                    agent,
                },
                thread,
            ));
        }
        let app_servers = self.store.codex_app_servers().ok()?;
        let thread = configured
            .codex_endpoints
            .root_thread(&caller.codex_thread(&app_servers)?);
        let mut seats = Vec::new();
        for session in configured.herdr.herdr_sessions()? {
            for agent in agents_of(&session.name) {
                let runs_thread = match agent
                    .pointer("/agent_session/value")
                    .and_then(serde_json::Value::as_str)
                {
                    Some(reported) => reported == thread,
                    None => {
                        self.unreported_thread(configured, &session.name, &agent)
                            .as_deref()
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
    fn bind_seat(
        &self,
        configured: &crate::ConfiguredUse,
        address: Address,
        peer: Option<&Process>,
    ) -> Response;
}

impl BindsSeats for RunningNexus {
    fn bind_seat(
        &self,
        configured: &crate::ConfiguredUse,
        address: Address,
        peer: Option<&Process>,
    ) -> Response {
        let _turn = self
            .dispatch_gate
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let Some(peer) = peer else {
            return Response::Refused(Refusal::Unidentified(Process { pid: 0, started: 0 }));
        };
        let unidentified = || Response::Refused(Refusal::Unidentified(peer.clone()));
        if !FlowStore::process_is_live(peer) {
            return unidentified();
        }
        let mut missing_flow_identity = false;
        // A seat Flow already knows binds idempotently through the same
        // kernel-authenticated resolver as Send. Claude children do not
        // carry a Codex thread or resume command.
        match self.identify_peer_process_configured(configured, peer.clone()) {
            Ok(Response::Identified(known)) if known == address => {
                match self.store.identify_process_sender(peer.clone()) {
                    Ok(ProcessSenderResolution::Sender(sender)) => {
                        return Response::Bound(sender.flow_id);
                    }
                    // A migrated binding without a FlowId is not proof of
                    // current ownership. Continue through the full native
                    // session, foreground-process and identity-claim checks
                    // below; binding writes the proved FlowId.
                    Ok(ProcessSenderResolution::MissingFlowIdentity) => {
                        missing_flow_identity = true;
                    }
                    Ok(ProcessSenderResolution::NoBinding) => {
                        if let Some(flow_id) = self.live_awake_flow(&address) {
                            return Response::Bound(flow_id);
                        }
                    }
                    Ok(ProcessSenderResolution::StaleFlowIdentity) => return unidentified(),
                    Err(error) => {
                        return Response::Refused(Refusal::Store(error.to_string()));
                    }
                }
            }
            Err(error) => return Response::Refused(Refusal::Store(error.to_string())),
            _ => {}
        }
        let Some(process_id) = u32::try_from(peer.pid).ok() else {
            return unidentified();
        };
        let Some((seat, thread)) = self.seat_of(&configured, CallerProcess { process_id }) else {
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
        let Some(native_session_id) = text("/agent_session/value").or(thread.clone()) else {
            return unidentified();
        };
        let (
            Some(herdr_workspace_id),
            Some(herdr_pane_id),
            Some(herdr_terminal_id),
            Some(herdr_tab_id),
        ) = (
            text("/workspace_id"),
            text("/pane_id"),
            text("/terminal_id"),
            text("/tab_id"),
        )
        else {
            return unidentified();
        };
        let herdr_agent_name = text("/name").unwrap_or_default();
        let model_name = configured
            .snapshot
            .configured_models
            .iter()
            .find(|model| model.layer == address.layer && model.aspect == address.aspect)
            .map(|model| model.model_name.clone())
            .unwrap_or_default();
        if model_name.is_empty() || thread.as_deref() != Some(native_session_id.as_str()) {
            return unidentified();
        }
        let route = HerdrRoute {
            herdr_session_name: seat.herdr_session_name.clone(),
            herdr_agent_name: herdr_agent_name.clone(),
            herdr_pane_id: herdr_pane_id.clone(),
            herdr_terminal_id: herdr_terminal_id.clone(),
        };
        let Some(mut foreground) = configured.herdr.foreground_processes(&route) else {
            return unidentified();
        };
        foreground.retain(|process| {
            process
                .arguments
                .windows(2)
                .any(|pair| pair[0] == "resume" && pair[1] == native_session_id)
        });
        let [foreground] = foreground.as_slice() else {
            return unidentified();
        };
        let Some(process_identity) = meta_signal_flow::ProcessIdentity::of(foreground.process_id)
        else {
            return unidentified();
        };
        let Ok(working_directory) =
            std::fs::canonicalize(format!("/proc/{}/cwd", foreground.process_id))
        else {
            return unidentified();
        };
        let Ok(flow_id) = configured
            .herdr
            .claim_flow_identity(&harness_kind, &native_session_id)
        else {
            return unidentified();
        };
        let binding = meta_signal_flow::FlowBinding {
            flow_id: flow_id.clone(),
            model_name: model_name.clone(),
            harness_kind: harness_kind.clone(),
            native_session_id: native_session_id.clone(),
            herdr_workspace_id,
            herdr_pane_id: herdr_pane_id.clone(),
            herdr_tab_id,
            herdr_terminal_id: herdr_terminal_id.clone(),
            herdr_agent_name: herdr_agent_name.clone(),
            process_identity,
            working_directory: working_directory.to_string_lossy().into_owned(),
        };
        let role = Caller {
            flow_id: flow_id.clone(),
            aspect: address.aspect.clone(),
            layer: address.layer.clone(),
            model_name,
        };
        match self.verified_seat_node(&seat.herdr_session_name, &address, &binding) {
            Err(_) => unidentified(),
            Ok(node) => {
                if missing_flow_identity {
                    return self
                        .store
                        .bind_metaflow(
                            Bind_Data {
                                process: peer.clone(),
                                address,
                            },
                            peer,
                            Some(flow_id),
                        )
                        .unwrap_or_else(|error| {
                            Response::Refused(Refusal::Store(error.to_string()))
                        });
                }
                match self
                    .store
                    .adopt_existing_flow(node, role, address.clone(), &binding)
                {
                    Ok(ExistingSeatAdoption::Bound(_)) => Response::Bound(flow_id),
                    Ok(ExistingSeatAdoption::Refused(_)) => {
                        Response::Refused(Refusal::Taken(address))
                    }
                    Err(error) => Response::Refused(Refusal::Store(error.to_string())),
                }
            }
        }
    }
}

/// A direct reply is based on native receipt evidence, never admission,
/// placement, an interrupt, or a successful bridge submission.
pub(crate) trait CompletesDirectSends {
    fn direct_receipt_watch(
        &self,
        configured: &crate::ConfiguredUse,
        address: &Address,
    ) -> Option<notify::RecommendedWatcher>;
    /// The caller holds the dispatch gate until admission completes. None
    /// while the metaflow is held, by a refresh, a threshold refresh
    /// starting its successor or a waking: the request is then queued on
    /// the metaflow for the flow the holder leaves Awake, never typed into
    /// an ending or starting pane.
    fn direct_send_lease(
        &self,
        configured: &crate::ConfiguredUse,
        address: &Address,
    ) -> Result<Option<crate::waking::WakingLease<'_>>, Box<Response>>;
    fn complete_direct_send(
        &self,
        configured: &crate::ConfiguredUse,
        address: &Address,
        logical_id: String,
    ) -> SendAnswer;
}

impl CompletesDirectSends for RunningNexus {
    fn direct_receipt_watch(
        &self,
        configured: &crate::ConfiguredUse,
        address: &Address,
    ) -> Option<notify::RecommendedWatcher> {
        use crate::store::{AnnouncesLaunchChanges, ReadsFlowRows};
        use crate::waking::ReadsClaudeDelivery;
        use notify::Watcher;
        let flow_id = self.live_awake_flow(address)?;
        let node = self.store.flow_node(&flow_id).ok()??;
        if node.harness_kind != HarnessKind::Claude {
            return None;
        }
        let transcript = self.claude_transcript_configured(configured, &node)?;
        let directory = transcript.parent()?.to_path_buf();
        let changes = self.store.launch_changes.clone();
        let mut watcher =
            notify::recommended_watcher(move |event: notify::Result<notify::Event>| {
                if let Ok(event) = event
                    && matches!(
                        event.kind,
                        notify::EventKind::Create(_)
                            | notify::EventKind::Modify(_)
                            | notify::EventKind::Remove(_)
                    )
                    && event.paths.iter().any(|path| path == &transcript)
                {
                    changes.announce();
                }
            })
            .ok()?;
        watcher
            .watch(&directory, notify::RecursiveMode::NonRecursive)
            .ok()?;
        Some(watcher)
    }
    fn direct_send_lease(
        &self,
        configured: &crate::ConfiguredUse,
        address: &Address,
    ) -> Result<Option<crate::waking::WakingLease<'_>>, Box<Response>> {
        use signal_flow::{SendFailure, SendFailureReason};
        let failed = |reason| {
            Box::new(Response::Failed(SendFailure {
                logical_delivery_id_option: None,
                send_failure_reason: reason,
            }))
        };
        let Some(lease) = self.waking.hold(address) else {
            return Ok(None);
        };
        if !self
            .store
            .prepare_declared_metaflow(address)
            .map_err(|error| {
                failed(SendFailureReason::Refused(Refusal::Store(
                    error.to_string(),
                )))
            })?
        {
            return Err(failed(SendFailureReason::RecipientUnavailable(
                address.clone(),
            )));
        }
        let flow_id = self.live_awake_flow(address);
        let model_is_configured = configured
            .snapshot
            .configured_models
            .iter()
            .any(|model| model.layer == address.layer && model.aspect == address.aspect);
        if flow_id.is_none()
            && (!matches!(self.store.metaflow_declaration(address), Ok(Some(_)))
                || !model_is_configured)
        {
            return Err(failed(SendFailureReason::RecipientUnavailable(
                address.clone(),
            )));
        }
        match self.store.metaflow(address) {
            Ok(Some(metaflow)) if metaflow.queue.is_empty() => {}
            Ok(_) => return Ok(None),
            Err(error) => {
                return Err(failed(SendFailureReason::Refused(Refusal::Store(
                    error.to_string(),
                ))));
            }
        }
        if !matches!(self.store.outstanding_delivery(address), Ok(None))
            || flow_id.as_ref().is_some_and(|flow_id| {
                !matches!(self.store.address_hold(flow_id), Ok(None))
                    || !matches!(self.store.past_threshold(flow_id), Ok(false))
            })
        {
            return Ok(None);
        }
        Ok(Some(lease))
    }
    fn complete_direct_send(
        &self,
        configured: &crate::ConfiguredUse,
        address: &Address,
        logical_id: String,
    ) -> SendAnswer {
        use crate::store::AnnouncesLaunchChanges;
        use signal_flow::{DeliveryState, SendFailure, SendFailureReason, SendReceipt};
        use std::time::{Duration, Instant};
        let send_wait = configured.snapshot.timings.send_wait;
        // A configured sleeping recipient starts through the existing
        // fresh Curriculum composition. The Address lease prevents another
        // wake or submission while this direct operation owns its request.
        if self.live_awake_flow(address).is_none() {
            let _ = self.wake_configured(&configured, address.clone());
        }
        // Subscribe before input. File events only schedule the existing
        // exact session/inode/cursor/content correlation, never delivery.
        let _receipt_watch = self.direct_receipt_watch(&configured, address);
        let outcome = {
            let _turn = self.dispatch_gate.lock().unwrap_or_else(|e| e.into_inner());
            self.drain_all_configured(configured, address.clone())
        };
        // This budget bounds waiting, not evidence. Expiry is uncertainty,
        // never proof of absence and never permission for a new attempt.
        let deadline = Instant::now() + Duration::from_millis(send_wait as u64);
        let changes = self.store.launch_changes.clone();
        loop {
            let seen = changes.current();
            let state = {
                let _turn = self.dispatch_gate.lock().unwrap_or_else(|e| e.into_inner());
                if let Ok(Some(attempt)) = self.store.outstanding_delivery(address) {
                    let _ = self.reconcile_delivery_configured(&configured, &attempt);
                }
                self.store.logical_delivery_snapshot(&logical_id)
            };
            if let Ok(Some(snapshot)) = state {
                match snapshot.delivery_state {
                    DeliveryState::Delivered(native_receipt) => {
                        return Response::Sent(SendReceipt {
                            logical_delivery_id: logical_id,
                            native_receipt,
                        })
                        .into();
                    }
                    DeliveryState::Attempting | DeliveryState::Uncertain
                        if Instant::now() < deadline && changes.until(seen, deadline) =>
                    {
                        continue;
                    }
                    _ => {}
                }
            }
            break;
        }
        let observed = format!("{outcome:?}");
        let state = {
            let _turn = self.dispatch_gate.lock().unwrap_or_else(|e| e.into_inner());
            // A native transcript can land without a harness hook. One
            // final correlated reconciliation follows the bounded event
            // wait; timeout alone is never evidence of absence.
            if let Ok(Some(attempt)) = self.store.outstanding_delivery(address) {
                let _ = self.reconcile_delivery_configured(&configured, &attempt);
            }
            self.store
                .finish_failed_direct(address, &logical_id, &observed)
        };
        let reason = match state {
            Ok(DeliveryState::Delivered(native_receipt)) => {
                return Response::Sent(SendReceipt {
                    logical_delivery_id: logical_id,
                    native_receipt,
                })
                .into();
            }
            Ok(DeliveryState::Uncertain | DeliveryState::Attempting) => {
                SendFailureReason::ReceiptUncertain
            }
            Ok(_) => SendFailureReason::NotSent(observed),
            Err(error) => SendFailureReason::Refused(Refusal::Store(error.to_string())),
        };
        Response::Failed(SendFailure {
            logical_delivery_id_option: Some(logical_id),
            send_failure_reason: reason,
        })
        .into()
    }
}
