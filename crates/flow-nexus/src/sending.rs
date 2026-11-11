//! Send, Launch and BindSeat on the ordinary socket, from any caller.
//!
//! A Send carries no sender: the Nexus reads the calling process from its
//! own socket to fill it in. A process in a Herdr pane is the flow bound in
//! that pane; a command a Codex app-server started is the flow of the
//! thread the app-server named in that command's environment; any other
//! caller sends as Unbound and is served.
//!
//! Send queues a request for a metaflow, or types it into the recipient's
//! pane when its Priority is abrupt and the recipient's flow is live. Launch starts a
//! metaflow from its declaration in Flow's configuration. BindSeat binds the
//! seat in the pane it names, with the FlowId its native session is claimed
//! under, as the metaflow's flow; a Codex seat Herdr reports no session for
//! runs the thread its pane resumes in its foreground.

use crate::RunningNexus;
use crate::binding::{ReadsProcessIdentity, VerifiesAdoptedSeat};
use crate::caller::{AuthenticatesProcessAuthority, ProcessAuthority};
use crate::composition::JoinsPurpose;
use crate::generated::operation::{Failed_Data, Operation, Outcome};
use crate::harness_command::{
    HarnessCommandResources, RehearsesHarnessResources, RendersShellCommand,
};
use crate::herdr::ReadsHerdrPanes;
use crate::herdr::launch::ClaimsNativeIdentity;
use crate::herdr::launch::ConstructsHarnessCommand;
use crate::launching::ReadsHarnessArguments;
use crate::model::RunsModel;
use crate::performing::Performs;
use crate::store::address_hold::HoldsLaunchAddress;
use crate::store::logical_delivery::RecordsLogicalDeliveries;
use crate::store::metaflow::{PreparesDeclaredMetaflow, SleepsMetaflows, TakesLaunchAddress};
use crate::store::metaflow_declaration::ReadsMetaflowDeclarations;
use crate::store::queue_delivery::AccountsQueueDelivery;
use crate::store::send_outcome::RecordsSendOutcomes;
use crate::store::{ExistingSeatAdoption, NamesConfiguredModel, RegistersExistingFlow};
use crate::waking::{
    HoldsSoftForIdle, HoldsWakingMetaflows, PerformsWakeAndDrain, ReconcilesQueueDelivery,
    RestsEndedAwake, SleepsAndWakesSeats,
};
use datom_codec::Datomizable;
use protos::{Compactable, Protosizable};
use signal_flow::{
    Address, CallerIdentity, HarnessKind, HerdrRoute, LaunchProfile, LaunchRequest, Message,
    PrivateSendResponse, Process, Refusal, Request, Response, SendPayloadSize, SendRequest,
    SendableMessage, SentBy, StartRejection, Unknown_Data,
};
use std::sync::atomic::{AtomicU64, Ordering};

/// Flow's maximum UTF-8 byte length for one rendered native Send prompt.
pub const MAX_SEND_PAYLOAD_BYTES: usize = 64 * 1024;

/// What a message is sent as: a sender's Address names itself, and a
/// SentBy is the sender already.
pub trait SendsAs {
    fn sent_by(&self) -> SentBy;
}

impl SendsAs for Address {
    fn sent_by(&self) -> SentBy {
        SentBy::Sender(self.clone())
    }
}

impl SendsAs for SentBy {
    fn sent_by(&self) -> SentBy {
        self.clone()
    }
}

/// Measures a Request as it would appear in the recipient's native prompt.
pub trait MeasuresSendPayload {
    fn native_prompt_bytes(&self, sender: &impl SendsAs) -> usize;
}

impl MeasuresSendPayload for Request {
    fn native_prompt_bytes(&self, sender: &impl SendsAs) -> usize {
        Message::Soft(self.clone()).native_prompt_bytes(sender)
    }
}

impl MeasuresSendPayload for Message {
    fn native_prompt_bytes(&self, sender: &impl SendsAs) -> usize {
        const PLACEHOLDER_REQUEST_ID: &str = "0000";
        let request_id = flow_ethos::RequestId(PLACEHOLDER_REQUEST_ID.to_owned());
        vec![
            crate::message_projection::ProjectsAdmittedMessage::admitted_input(
                self,
                &request_id,
                &sender.sent_by(),
            ),
        ]
        .datomize(Vec::new())
        .protosize()
        .compact()
        .len()
    }
}

/// Refuses a Send whose complete native prompt rendering exceeds Flow's budget.
pub trait AdmitsSendPayload {
    fn oversized_refusal(&self, sender: &impl SendsAs) -> Option<Refusal>;
}

impl AdmitsSendPayload for Request {
    fn oversized_refusal(&self, sender: &impl SendsAs) -> Option<Refusal> {
        let actual_bytes = self.native_prompt_bytes(sender);
        (actual_bytes > MAX_SEND_PAYLOAD_BYTES).then_some(Refusal::Oversized(SendPayloadSize {
            actual_bytes: signal_flow::ActualBytes(i64::try_from(actual_bytes).unwrap_or(i64::MAX)),
            maximum_bytes: signal_flow::MaximumBytes(MAX_SEND_PAYLOAD_BYTES as i64),
        }))
    }
}

impl AdmitsSendPayload for Message {
    fn oversized_refusal(&self, sender: &impl SendsAs) -> Option<Refusal> {
        let actual_bytes = self.native_prompt_bytes(sender);
        (actual_bytes > MAX_SEND_PAYLOAD_BYTES).then_some(Refusal::Oversized(SendPayloadSize {
            actual_bytes: signal_flow::ActualBytes(i64::try_from(actual_bytes).unwrap_or(i64::MAX)),
            maximum_bytes: signal_flow::MaximumBytes(MAX_SEND_PAYLOAD_BYTES as i64),
        }))
    }
}

/// The sender a calling process's message is filled in with: the metaflow
/// it runs in, or Unbound for a caller in no flow. It refuses nobody; only
/// a store that cannot be read answers a refusal.
pub trait IdentifiesSender {
    fn sender_of(&self, peer: &Process) -> Result<SentBy, Box<Response>>;
}

impl IdentifiesSender for RunningNexus {
    fn sender_of(&self, peer: &Process) -> Result<SentBy, Box<Response>> {
        let refused = |error: crate::store::StoreError| {
            Box::new(Response::Refused(Refusal::Store(error.to_string())))
        };
        Ok(match self.process_authority(peer).map_err(refused)? {
            ProcessAuthority::Bound(origin) => self
                .store
                .metaflow_awake_as(&origin.flow_id.0)
                .map_err(refused)?
                .map_or(SentBy::Unbound, |metaflow| SentBy::Sender(metaflow.address)),
            ProcessAuthority::Sending(address) => SentBy::Sender(address),
            ProcessAuthority::Unbound => SentBy::Unbound,
        })
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

/// What a Send answers.
pub struct PendingSend {
    pub address: Address,
}

pub struct SendAnswer {
    pub response: Response,
    pub private_response: PrivateSendResponse,
    pub pending: Option<PendingSend>,
    pub(crate) _pending_answer: Option<crate::store::logical_delivery::PendingAnswerHold>,
}

impl From<PrivateSendResponse> for SendAnswer {
    fn from(private_response: PrivateSendResponse) -> Self {
        Self {
            response: crate::message_projection::ProjectsSendResponse::visible_send_response(
                &private_response,
            ),
            private_response,
            pending: None,
            _pending_answer: None,
        }
    }
}

/// Positive completion evidence for a synchronous direct send. `Read` is
/// stronger than native placement: the authenticated answer already named
/// this request. It therefore answers Sent without inventing a native
/// receipt, just as Delivered does.
pub(crate) trait AnswersCompletedDirectSend {
    fn completed_direct_response(
        &self,
        acceptance: &signal_flow::SendAcceptance,
    ) -> Option<PrivateSendResponse>;
}

impl AnswersCompletedDirectSend for signal_flow::DeliveryState {
    fn completed_direct_response(
        &self,
        acceptance: &signal_flow::SendAcceptance,
    ) -> Option<PrivateSendResponse> {
        if matches!(
            self,
            signal_flow::DeliveryState::Delivered(_) | signal_flow::DeliveryState::Read
        ) {
            Some(PrivateSendResponse::Sent(signal_flow::SendReceipt {
                logical_delivery_id: acceptance.logical_delivery_id.clone(),
                request_id: acceptance.request_id.clone(),
            }))
        } else {
            None
        }
    }
}

pub trait SendsRequests {
    /// Answers a Send from `peer`, under the dispatch gate.
    fn send_request(&self, send: SendRequest, peer: Option<&Process>) -> SendAnswer;
    /// Delivers every durable request currently queued at `address`,
    /// whoever asks.
    fn deliver_all(&self, address: Address) -> Response;
    /// The flow the metaflow at `address` is Awake as, when it is live.
    fn live_awake_flow(&self, address: &Address) -> Option<String>;
}

impl SendsRequests for RunningNexus {
    fn send_request(&self, send: SendRequest, peer: Option<&Process>) -> SendAnswer {
        use sha2::{Digest, Sha256};

        let abrupt = !matches!(&send.sendable_message, SendableMessage::Soft(_));
        let digest_text = send.datomize(Vec::new()).protosize().compact();
        let request_digest = format!("{:x}", Sha256::digest(digest_text.as_bytes()));
        // The sender is read when the Send arrives. A refresh may hold the
        // address until its successor is committed; that transition cannot
        // make the already-arrived caller cease to be this request's sender.
        // A Send no process stands behind is Unbound, as a caller in no flow.
        let observed_sender = match peer {
            Some(peer) => self.sender_of(peer),
            None => Ok(SentBy::Unbound),
        };
        // The sender the outcome is recorded with: Unknown when it could
        // not be read.
        let recorded_sender = observed_sender.clone().unwrap_or(SentBy::Unknown);
        let mut recipient = Some(send.address.clone());
        let mut pending_answer = None;
        let mut answer = (|| -> SendAnswer {
            let _turn = self
                .dispatch_gate
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            // The recipient's topic is input, checked before anything else:
            // an uncapitalized camelCase identifier, kept as written.
            {
                use crate::launching::NamesTopic;
                if !send.address.topic.0.as_str().is_camel_case_topic() {
                    return PrivateSendResponse::Refused(Refusal::TopicRefused(
                        send.address.topic.clone(),
                    ))
                    .into();
                }
            }
            let Some(_now) = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .ok()
                .and_then(|elapsed| i64::try_from(elapsed.as_secs()).ok())
            else {
                return PrivateSendResponse::Refused(Refusal::Store(
                    "clock is outside the lease range".into(),
                ))
                .into();
            };
            let sender = match observed_sender.as_ref() {
                Ok(sender) => sender.clone(),
                Err(refusal) => match refusal.as_ref() {
                    Response::Refused(reason) => {
                        return PrivateSendResponse::Refused(reason.clone()).into();
                    }
                    response => {
                        return PrivateSendResponse::Refused(Refusal::Store(format!(
                            "sender identification returned {response:?}"
                        )))
                        .into();
                    }
                },
            };
            let delivery = if abrupt {
                flow_ethos::Delivery::Wake
            } else {
                flow_ethos::Delivery::Queue
            };
            let message = match self
                .store
                .resolve_sendable_message(send.sendable_message.clone())
            {
                Ok(Ok(message)) => message,
                Ok(Err(refusal)) => return PrivateSendResponse::Refused(refusal).into(),
                Err(error) => {
                    return PrivateSendResponse::Refused(Refusal::Store(error.to_string())).into();
                }
            };
            if let Some(refusal) = message.oversized_refusal(&sender) {
                return PrivateSendResponse::Refused(refusal).into();
            }
            // A Result or Done goes to the sender of the request it names,
            // whatever Address the Send writes. One whose sender is no
            // Address is kept with the request, answered Sent with its id.
            let recipient = {
                use crate::store::kept_answer::{AnswerRoute, MarksKeptAnswer, RoutesAnswers};
                match self.store.answer_route(&message) {
                    Ok(AnswerRoute::Unanswering) => send.address.clone(),
                    Ok(AnswerRoute::To(address)) => {
                        recipient = Some(address.clone());
                        address
                    }
                    Ok(AnswerRoute::Kept(binding)) => {
                        recipient = None;
                        let request_id = binding.request_id.clone();
                        return match self.store.keep_answer(binding, &sender, &message) {
                            Ok(()) => PrivateSendResponse::Sent(signal_flow::SendReceipt {
                                logical_delivery_id: request_id.kept_marker(),
                                request_id,
                            })
                            .into(),
                            Err(error) => {
                                PrivateSendResponse::Refused(Refusal::Store(error.to_string()))
                                    .into()
                            }
                        };
                    }
                    Err(error) => {
                        return PrivateSendResponse::Refused(Refusal::Store(error.to_string()))
                            .into();
                    }
                }
            };
            self.rest_ended_awake(&recipient);
            // Own the address until the direct call has a receipt or a
            // durable non-retryable failure. Background drain/wake may not
            // submit this same request independently. A metaflow another
            // holds takes the request into its queue instead. A Soft request
            // to a seat in its turn is queued for the seat's idle event.
            let direct_requested = abrupt
                || self
                    .live_awake_flow(&recipient)
                    .is_some_and(|flow_id| self.rests_now(&flow_id));
            let direct_lease = if direct_requested {
                match self.direct_send_lease(&recipient) {
                    Ok(lease) => lease,
                    Err(refusal) => return (*refusal).into(),
                }
            } else {
                None
            };
            let admitted_at = std::time::SystemTime::now();
            let admitted = self.store.accept_logical_delivery(
                &recipient,
                sender,
                delivery.clone(),
                message.clone(),
                None,
            );
            // Each admission is logged with its kind, and a Task's
            // Minutes with the deadline its first reminder is due at.
            if let Ok(PrivateSendResponse::SendAccepted(acceptance)) = &admitted {
                use crate::store::logical_delivery::HoldsAdmissionAnswers;
                pending_answer = Some(self.store.hold_admission_answer(&acceptance.request_id));
                use crate::admission::{Admission, LogsAdmission};
                use crate::store::reminder::RecordsReminders;
                let (Message::HardAbrupt(request)
                | Message::MiddleAbrupt(request)
                | Message::Soft(request)) = &message;
                let deadline = matches!(request, Request::Task(_))
                    .then(|| self.store.reminder(&acceptance.logical_delivery_id.0))
                    .and_then(Result::ok)
                    .flatten()
                    .and_then(|reminder| reminder.due);
                eprintln!(
                    "{}",
                    Admission {
                        request,
                        deadline,
                        at: admitted_at,
                    }
                    .log_line()
                );
            }
            drop(_turn);
            match (admitted, direct_lease) {
                (Ok(PrivateSendResponse::SendAccepted(acceptance)), Some(lease)) => {
                    self.complete_direct_send(&recipient, acceptance, &lease)
                }
                (Ok(private_response), _) => {
                    let pending = matches!(private_response, PrivateSendResponse::SendAccepted(_))
                        .then(|| PendingSend { address: recipient });
                    let response =
                        crate::message_projection::ProjectsSendResponse::visible_send_response(
                            &private_response,
                        );
                    SendAnswer {
                        response,
                        private_response,
                        pending,
                        _pending_answer: None,
                    }
                }
                (Err(error), _) => {
                    PrivateSendResponse::Refused(Refusal::Store(error.to_string())).into()
                }
            }
        })();
        answer._pending_answer = pending_answer;
        if abrupt && let PrivateSendResponse::Refused(refusal) = &answer.private_response {
            answer.private_response =
                PrivateSendResponse::Failed(signal_flow::SendFailure::Unadmitted(
                    signal_flow::SendFailureReason::Refused(refusal.clone()),
                ));
            answer.response =
                crate::message_projection::ProjectsSendResponse::visible_send_response(
                    &answer.private_response,
                );
        }
        match self.store.record_send_outcome(
            peer.cloned(),
            recorded_sender,
            recipient,
            request_digest,
            answer.private_response.clone(),
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
        (self
            .store
            .flow_is_live(&flow_id.0)
            .ok()
            .filter(|live| *live)
            .map(|_| flow_id))
        .map(|value| value.0)
    }

    fn deliver_all(&self, address: Address) -> Response {
        let turn = self
            .dispatch_gate
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        self.rest_ended_awake(&address);
        let asleep = match self.store.metaflow(&address) {
            Ok(Some(metaflow)) => {
                if metaflow.queue.0.is_empty() {
                    return Response::Drained(signal_flow::VisibleDrainOutcome {
                        address: address.clone(),
                        visible_drain_state: signal_flow::VisibleDrainState::Delivered,
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
            self.drain_all(address.clone());
        }
        drop(turn);
        if asleep {
            self.wake_due(address.clone());
        }
        match self.store.metaflow(&address) {
            Ok(Some(metaflow)) if metaflow.queue.0.is_empty() => {
                Response::Drained(signal_flow::VisibleDrainOutcome {
                    address: address.clone(),
                    visible_drain_state: signal_flow::VisibleDrainState::Delivered,
                })
            }
            Ok(Some(_metaflow)) => Response::Drained(signal_flow::VisibleDrainOutcome {
                address: address.clone(),
                visible_drain_state: signal_flow::VisibleDrainState::Held,
            }),
            Ok(None) => Response::Refused(Refusal::Unknown(Unknown_Data::Address(address))),
            Err(error) => Response::Refused(Refusal::Store(error.to_string())),
        }
    }
}

/// The effort a seat of one Address runs at: its model's.
pub trait ChoosesEffort {
    fn effort_of(address: &Address) -> &'static str {
        address.model.effort()
    }
}

impl ChoosesEffort for RunningNexus {}

pub trait LaunchesDeclared {
    /// The Start a Launch names, or, when the metaflow is Awake as a live
    /// flow, whoever asks, the Replace of that flow; Err with the refusal
    /// when the Address is undeclared or its model unconfigured. It only
    /// reads; the Start or Replace it names is dispatched as any other.
    fn declared_launch(&self, launch: LaunchRequest) -> Result<signal_flow::Query, Box<Response>>;
    /// The profile and predecessor state one declared request reads, in the
    /// configured Herdr session. A rehearsal uses semantic identities and
    /// performs no Herdr read.
    fn declared_profile(
        &self,
        launch: LaunchRequest,
        rehearsal: bool,
    ) -> Result<(LaunchProfile, Option<String>), Box<Response>>;
}

impl LaunchesDeclared for RunningNexus {
    fn declared_launch(&self, launch: LaunchRequest) -> Result<signal_flow::Query, Box<Response>> {
        let (profile, predecessor) = self.declared_profile(launch, false)?;
        Ok(match predecessor {
            Some(_) => signal_flow::Query::Replace(profile),
            None => signal_flow::Query::Start(profile),
        })
    }

    fn declared_profile(
        &self,
        launch: LaunchRequest,
        rehearsal: bool,
    ) -> Result<(LaunchProfile, Option<String>), Box<Response>> {
        let store_refused = |error: crate::store::StoreError| {
            Box::new(Response::Refused(Refusal::Store(error.to_string())))
        };
        let address = launch.address;
        // The topic is input: checked before the declaration is looked up.
        {
            use crate::launching::NamesTopic;
            if !address.topic.0.as_str().is_camel_case_topic() {
                return Err(Box::new(Response::StartRejected(
                    StartRejection::TopicRefused(address.topic.clone()),
                )));
            }
        }
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
        // A Launch addressed to an Awake metaflow refreshes it: the flow it
        // is Awake as is replaced by the launch, never typed into.
        let predecessor = self.live_awake_flow(&address);
        // Every launch opens in the one configured session, whoever asks.
        let herdr_session_name = if rehearsal {
            "<session>".to_owned()
        } else {
            use crate::launching::NamesLaunchSession;
            self.launch_session().map_err(store_refused)?.0
        };
        let profile = LaunchProfile {
            launch_request_id: if rehearsal {
                signal_flow::LaunchRequestId("rehearse-launch".into())
            } else {
                signal_flow::LaunchRequestId("launch".minted_id())
            },
            system_modules: declaration.system_modules,
            user_modules: declaration.user_modules,
            address: address.clone(),
            model_name: signal_flow::ModelName(model_name),
            effort: signal_flow::Effort(Self::effort_of(&address).to_owned()),
            flow_id_option: predecessor.clone().map(flow_ethos::FlowId),
            remembering: launch.remembering,
            herdr_session_name: signal_flow::HerdrSessionName(herdr_session_name),
            leave_outs_option: None,
            flow_kind: declaration.flow_kind,
            tool_set: declaration.tool_set,
            // The declaration's purpose is composed ahead of the brief.
            instruction_prompt: signal_flow::InstructionPrompt(
                launch
                    .instruction_prompt
                    .0
                    .with_purpose(&declaration.purpose.0),
            ),
        };
        Ok((profile, predecessor))
    }
}

/// Renders one declared launch's final native invocation without creating
/// any resource or writing any state.
pub(crate) trait RehearsesDeclared {
    fn rehearse_declared(&self, launch: LaunchRequest) -> Response;
}

impl RehearsesDeclared for RunningNexus {
    fn rehearse_declared(&self, launch: LaunchRequest) -> Response {
        let rejected = |response: Response| match response {
            Response::StartRejected(rejection) => Response::RehearseRejected(rejection),
            Response::Refused(_) => {
                Response::RehearseRejected(StartRejection::LaunchPersistenceRefused)
            }
            _ => Response::RehearseRejected(StartRejection::LaunchPersistenceRefused),
        };
        let (profile, _) = match self.declared_profile(launch, true) {
            Ok(profile) => profile,
            Err(response) => return rejected(*response),
        };
        let harness_arguments = match self.harness_arguments(&profile.address) {
            Ok(arguments) => arguments,
            Err(rejection) => return Response::RehearseRejected(rejection),
        };
        let composed = match self.perform(Operation::Compose(profile)) {
            Outcome::Composed(composed) => composed,
            Outcome::Failed(Failed_Data::ContextRefused(refusal)) => {
                return Response::RehearseRejected(StartRejection::ContextRefused(refusal));
            }
            Outcome::Failed(Failed_Data::LeaveOutRefused(refusal)) => {
                return Response::RehearseRejected(StartRejection::LeaveOutRefused(refusal));
            }
            Outcome::Failed(Failed_Data::CompositionRefused(refusal)) => {
                return Response::RehearseRejected(StartRejection::CompositionRefused(refusal));
            }
            _ => {
                return Response::RehearseRejected(StartRejection::LaunchPersistenceRefused);
            }
        };
        let resources = HarnessCommandResources::rehearsal(&composed.harness_kind);
        match self
            .herdr
            .harness_command(&composed, resources, &harness_arguments)
        {
            Ok(command) => Response::Rehearsed(signal_flow::Command(command.shell_command())),
            Err(failure) => {
                let cause = match failure {
                    crate::herdr::launch::SpawnFailure::Refused(cause) => cause,
                    crate::herdr::launch::SpawnFailure::Unattempted(_) => {
                        signal_flow::SpawnRefusalCause::CommandTooLong
                    }
                };
                Response::RehearseRejected(StartRejection::HarnessSpawnRefused(
                    signal_flow::HarnessSpawnRefusal {
                        spawn_refusal_cause: cause,
                        opened_thread_option: None,
                    },
                ))
            }
        }
    }
}

/// One Herdr agent, as a session snapshot shows it.
pub(crate) struct SeenSeat {
    herdr_session_name: String,
    agent: serde_json::Value,
}

/// What the caller's pane showed when no seat could be named in it: raw
/// evidence, never an identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SeatEvidence {
    /// The caller's pane; None when the caller is in no pane Flow reads.
    pub(crate) pane: Option<crate::caller::CallerPane>,
    /// The label Herdr shows for that pane.
    pub(crate) pane_label: Option<String>,
    /// The native session the pane's agent runs, as far as it was read.
    pub(crate) native_session: Option<String>,
    /// When it was read, in seconds since the Unix epoch.
    pub(crate) observed_at: u64,
}

/// What a refusal of an unnamed caller holds: the evidence and the process,
/// never an identity.
pub(crate) trait RefusesUnidentified {
    /// A caller no pane or seat was read for: its process, observed now.
    fn unidentified(process: Process) -> Self;
    /// A caller whose pane showed `evidence`.
    fn unidentified_with(process: Process, evidence: SeatEvidence) -> Self;
}

impl RefusesUnidentified for Refusal {
    fn unidentified(process: Process) -> Self {
        Self::unidentified_with(
            process,
            SeatEvidence {
                pane: None,
                pane_label: None,
                native_session: None,
                observed_at: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|elapsed| elapsed.as_secs())
                    .unwrap_or_default(),
            },
        )
    }

    fn unidentified_with(process: Process, evidence: SeatEvidence) -> Self {
        Self::Unidentified(evidence.typed(process))
    }
}

/// Seat evidence in the contract's words, with the process it was read for.
pub(crate) trait TypesSeatEvidence {
    fn typed(self, process: Process) -> signal_flow::SeatEvidence;
}

impl TypesSeatEvidence for SeatEvidence {
    fn typed(self, process: Process) -> signal_flow::SeatEvidence {
        signal_flow::SeatEvidence {
            seat_pane_option: self.pane.map(|pane| signal_flow::SeatPane {
                herdr_session_name: signal_flow::HerdrSessionName(pane.herdr_session_name),
                herdr_pane_id: signal_flow::HerdrPaneId(pane.herdr_pane_id),
            }),
            pane_label_option: self.pane_label.map(signal_flow::PaneLabel),
            native_session_id_option: self.native_session.map(signal_flow::NativeSessionId),
            observed_at: signal_flow::ObservedAt(
                i64::try_from(self.observed_at).unwrap_or(i64::MAX),
            ),
            process,
        }
    }
}

pub(crate) trait FindsSeat {
    /// The agent Herdr shows in `pane`, with the native session it runs:
    /// the session Herdr reports, else, for a Codex agent, the one thread
    /// its pane's foreground resumes.
    fn seat_of(&self, pane: &crate::caller::CallerPane)
    -> Result<(SeenSeat, String), SeatEvidence>;
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
            herdr_session_name: signal_flow::HerdrSessionName(herdr_session_name.to_owned()),
            herdr_agent_name: signal_flow::HerdrAgentName(
                text("name").unwrap_or_default().to_owned(),
            ),
            herdr_pane_id: signal_flow::HerdrPaneId(text("pane_id")?.to_owned()),
            herdr_terminal_id: signal_flow::HerdrTerminalId(text("terminal_id")?.to_owned()),
        };
        let threads = self.herdr.foreground_threads(&route)?;
        let mut threads = threads.into_iter();
        match (threads.next(), threads.next()) {
            (Some(thread), None) => Some(thread),
            _ => None,
        }
    }
}

/// Raw evidence of what a pane showed, read now.
pub(crate) trait GathersSeatEvidence {
    /// The pane, the label Herdr shows for it, the native session read
    /// there, and the time.
    fn seat_evidence(
        &self,
        pane: Option<crate::caller::CallerPane>,
        native_session: Option<String>,
    ) -> SeatEvidence;
}

impl GathersSeatEvidence for RunningNexus {
    fn seat_evidence(
        &self,
        pane: Option<crate::caller::CallerPane>,
        native_session: Option<String>,
    ) -> SeatEvidence {
        SeatEvidence {
            pane_label: pane.as_ref().and_then(|pane| {
                self.herdr
                    .pane_label(&pane.herdr_session_name, &pane.herdr_pane_id)
            }),
            pane,
            native_session,
            observed_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|elapsed| elapsed.as_secs())
                .unwrap_or_default(),
        }
    }
}

/// The Claude process a pane's agent runs, read from its foreground.
trait ReadsClaudeSeat {
    /// The one foreground process of `route` Claude keeps a session file
    /// for, with that session, which agrees with the session Herdr reports
    /// when it reports one; else, with no such file, the one foreground
    /// process, with the session Herdr reports. None otherwise.
    fn claude_process(&self, route: &HerdrRoute, reported: Option<&str>) -> Option<(u32, String)>;
}

/// The route of the agent a session snapshot shows.
trait RoutesAgent {
    fn agent_route(&self, herdr_session_name: &str) -> HerdrRoute;
}

impl RoutesAgent for serde_json::Value {
    fn agent_route(&self, herdr_session_name: &str) -> HerdrRoute {
        let text = |key: &str| {
            self.get(key)
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default()
                .to_owned()
        };
        HerdrRoute {
            herdr_session_name: signal_flow::HerdrSessionName(herdr_session_name.to_owned()),
            herdr_agent_name: signal_flow::HerdrAgentName(text("name")),
            herdr_pane_id: signal_flow::HerdrPaneId(text("pane_id")),
            herdr_terminal_id: signal_flow::HerdrTerminalId(text("terminal_id")),
        }
    }
}

impl ReadsClaudeSeat for RunningNexus {
    fn claude_process(&self, route: &HerdrRoute, reported: Option<&str>) -> Option<(u32, String)> {
        use crate::claude::ReadsClaudeSessions;
        let foreground = self.herdr.foreground_processes(route).unwrap_or_default();
        let recorded = foreground
            .iter()
            .filter_map(|process| {
                self.herdr
                    .claude_sessions
                    .session_of(process.process_id)
                    .map(|session| (process.process_id, session))
            })
            .collect::<Vec<_>>();
        match (recorded.as_slice(), foreground.as_slice(), reported) {
            ([(process_id, session)], _, Some(reported)) if session == reported => {
                Some((*process_id, session.clone()))
            }
            ([(process_id, session)], _, None) => Some((*process_id, session.clone())),
            ([], [only], Some(reported)) => Some((only.process_id, reported.to_owned())),
            _ => None,
        }
    }
}

impl FindsSeat for RunningNexus {
    fn seat_of(
        &self,
        pane: &crate::caller::CallerPane,
    ) -> Result<(SeenSeat, String), SeatEvidence> {
        let Some(agent) = self
            .herdr
            .session_snapshot(&pane.herdr_session_name)
            .and_then(|snapshot| {
                snapshot
                    .pointer("/result/snapshot/agents")
                    .and_then(serde_json::Value::as_array)
                    .cloned()
            })
            .unwrap_or_default()
            .into_iter()
            .find(|agent| {
                agent.get("pane_id").and_then(serde_json::Value::as_str)
                    == Some(pane.herdr_pane_id.as_str())
            })
        else {
            return Err(self.seat_evidence(Some(pane.clone()), None));
        };
        let reported = agent
            .pointer("/agent_session/value")
            .and_then(serde_json::Value::as_str)
            .map(str::to_owned);
        let session = match agent.get("agent").and_then(serde_json::Value::as_str) {
            Some("claude") => self
                .claude_process(
                    &agent.agent_route(&pane.herdr_session_name),
                    reported.as_deref(),
                )
                .map(|(_, session)| session),
            Some("codex") => reported
                .clone()
                .or_else(|| self.unreported_thread(&pane.herdr_session_name, &agent)),
            _ => None,
        };
        match session {
            Some(session) => Ok((
                SeenSeat {
                    herdr_session_name: pane.herdr_session_name.clone(),
                    agent,
                },
                session,
            )),
            None => Err(self.seat_evidence(Some(pane.clone()), reported)),
        }
    }
}

pub trait BindsSeats {
    /// Binds the seat in the named pane as the metaflow at `address`, from
    /// any caller.
    fn bind_seat(&self, request: signal_flow::BindSeat_Data) -> Response;
    /// Adopts that seat only when the native session it runs is the one
    /// named.
    fn adopt(&self, request: signal_flow::Adopt_Data) -> Response;
}

impl BindsSeats for RunningNexus {
    fn bind_seat(&self, request: signal_flow::BindSeat_Data) -> Response {
        self.adopt_seat(request.address, &request.seat_pane, None)
    }

    fn adopt(&self, request: signal_flow::Adopt_Data) -> Response {
        match self.adopt_seat(
            request.address,
            &request.seat_pane,
            Some(request.native_session_id.0.as_str()),
        ) {
            Response::Bound(flow_id) => Response::Adopted(flow_id),
            response => response,
        }
    }
}

/// A refusal's process when no process is named: the seat's evidence
/// stands for the seat, whoever asked.
trait NamesNoProcess {
    fn none() -> Self;
}

impl NamesNoProcess for Process {
    fn none() -> Self {
        Process {
            pid: flow_ethos::generated::library::Pid(0),
            started: flow_ethos::generated::library::Started(0),
        }
    }
}

/// Binds the seat of a named pane to its Address, idempotently for a seat
/// Flow already knows.
trait AdoptsSeat {
    fn adopt_seat(
        &self,
        address: Address,
        pane: &signal_flow::SeatPane,
        expected_session: Option<&str>,
    ) -> Response;
}

impl AdoptsSeat for RunningNexus {
    fn adopt_seat(
        &self,
        address: Address,
        pane: &signal_flow::SeatPane,
        expected_session: Option<&str>,
    ) -> Response {
        let _turn = self
            .dispatch_gate
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        // A restart holds the Address: no seat binds to it meanwhile.
        if !matches!(
            crate::store::restart::RecordsRestarts::restart_at(&*self.store, &address),
            Ok(None)
        ) {
            return Response::Refused(Refusal::Locked);
        }
        let pane = crate::caller::CallerPane {
            herdr_session_name: pane.herdr_session_name.0.clone(),
            herdr_pane_id: pane.herdr_pane_id.0.clone(),
        };
        // The flow the Address is Awake as, still shown in this pane, binds
        // idempotently.
        if expected_session.is_none()
            && let Some(flow_id) = self.live_awake_flow(&address)
            && let Ok(Some(node)) = crate::store::ReadsFlowRows::flow_node(&*self.store, &flow_id)
            && let signal_flow::HerdrRouteSelection::Available(route) = &node.herdr_route_selection
            && route.herdr_session_name.0 == pane.herdr_session_name
            && route.herdr_pane_id.0 == pane.herdr_pane_id
            && self.herdr.pane_presence(&node) == crate::herdr::PanePresence::Present
        {
            return Response::Bound(signal_flow::FlowId(flow_id));
        }
        // A pane no seat is named in is refused, and what it showed is
        // logged as it was read, never made into an identity.
        let refused = |evidence: SeatEvidence| {
            eprintln!(
                "flow-nexus: BindSeat {address:?} names no seat in pane {:?}: label {:?}, session {:?}, at {}",
                evidence.pane, evidence.pane_label, evidence.native_session, evidence.observed_at
            );
            Response::Refused(Refusal::unidentified_with(Process::none(), evidence))
        };
        let (seat, native_session_id) = match self.seat_of(&pane) {
            Ok(found) => found,
            Err(evidence) => return refused(evidence),
        };
        let unidentified =
            || refused(self.seat_evidence(Some(pane.clone()), Some(native_session_id.clone())));
        if expected_session.is_some_and(|expected| expected != native_session_id) {
            return unidentified();
        }
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
        let model_name = self
            .store
            .configured_model(&address)
            .ok()
            .flatten()
            .unwrap_or_default();
        if model_name.is_empty() {
            return unidentified();
        }
        let route = HerdrRoute {
            herdr_session_name: signal_flow::HerdrSessionName(seat.herdr_session_name.clone()),
            herdr_agent_name: signal_flow::HerdrAgentName(herdr_agent_name.clone()),
            herdr_pane_id: signal_flow::HerdrPaneId(herdr_pane_id.clone()),
            herdr_terminal_id: signal_flow::HerdrTerminalId(herdr_terminal_id.clone()),
        };
        // A Codex seat's process resumes its thread; a Claude seat's is the
        // one Claude process of the pane's foreground.
        let seat_process = match harness_kind {
            HarnessKind::Codex => {
                let Some(foreground) = self.herdr.foreground_processes(&route) else {
                    return unidentified();
                };
                let resuming = foreground
                    .iter()
                    .filter(|process| {
                        process
                            .arguments
                            .windows(2)
                            .any(|pair| pair[0] == "resume" && pair[1] == native_session_id)
                    })
                    .collect::<Vec<_>>();
                let [process] = resuming.as_slice() else {
                    return unidentified();
                };
                process.process_id
            }
            HarnessKind::Claude => {
                let reported = text("/agent_session/value");
                match self.claude_process(&route, reported.as_deref()) {
                    Some((process_id, _)) => process_id,
                    None => return unidentified(),
                }
            }
        };
        let Some(process_identity) = meta_signal_flow::ProcessIdentity::of(seat_process) else {
            return unidentified();
        };
        let Ok(working_directory) = std::fs::canonicalize(format!("/proc/{seat_process}/cwd"))
        else {
            return unidentified();
        };
        let Ok(flow_id) = self
            .herdr
            .claim_flow_identity(&harness_kind, &native_session_id)
        else {
            return unidentified();
        };
        // A seat the Address is Awake as already binds idempotently.
        if self.live_awake_flow(&address).as_deref() == Some(flow_id.as_str()) {
            return Response::Bound(signal_flow::FlowId(flow_id));
        }
        let binding = meta_signal_flow::FlowBinding {
            flow_id: signal_flow::FlowId(flow_id.clone()),
            aspect: address.aspect.clone(),
            model_name: signal_flow::ModelName(model_name.clone()),
            harness_kind: harness_kind.clone(),
            native_session_id: signal_flow::NativeSessionId(native_session_id.clone()),
            herdr_workspace_id: signal_flow::HerdrWorkspaceId(herdr_workspace_id),
            herdr_pane_id: signal_flow::HerdrPaneId(herdr_pane_id.clone()),
            herdr_tab_id: meta_signal_flow::HerdrTabId(herdr_tab_id),
            herdr_terminal_id: signal_flow::HerdrTerminalId(herdr_terminal_id.clone()),
            herdr_agent_name: signal_flow::HerdrAgentName(herdr_agent_name.clone()),
            process_identity,
            working_directory: meta_signal_flow::WorkingDirectory(
                working_directory.to_string_lossy().into_owned(),
            ),
        };
        let role = CallerIdentity {
            flow_id: flow_ethos::FlowId(flow_id.clone()),
            aspect: address.aspect.clone(),
            model_name: flow_ethos::ModelName(model_name),
        };
        match self.verified_seat_node(&seat.herdr_session_name, &address, &binding) {
            Err(reason) => {
                eprintln!(
                    "flow-nexus: BindSeat {address:?}: the seat's binding is refused: {reason:?}"
                );
                unidentified()
            }
            Ok(node) => match self
                .store
                .adopt_existing_flow(node, role, address.clone(), &binding)
            {
                Ok(ExistingSeatAdoption::Bound(_)) => Response::Bound(signal_flow::FlowId(flow_id)),
                Ok(ExistingSeatAdoption::Refused(_)) => Response::Refused(Refusal::Taken(address)),
                Err(error) => Response::Refused(Refusal::Store(error.to_string())),
            },
        }
    }
}

/// A direct reply is based on native receipt evidence, never admission,
/// placement, an interrupt, or a successful bridge submission.
pub(crate) trait CompletesDirectSends {
    fn direct_receipt_watch(&self, address: &Address) -> Option<notify::RecommendedWatcher>;
    /// The caller holds the dispatch gate until admission completes. None
    /// while the metaflow is held, by a refresh, a threshold refresh
    /// starting its successor or a waking: the request is then queued on
    /// the metaflow for the flow the holder leaves Awake, never typed into
    /// an ending or starting pane.
    fn direct_send_lease(
        &self,
        address: &Address,
    ) -> Result<Option<crate::waking::WakingLease<'_>>, Box<PrivateSendResponse>>;
    fn complete_direct_send(
        &self,
        address: &Address,
        acceptance: signal_flow::SendAcceptance,
        lease: &crate::waking::WakingLease<'_>,
    ) -> SendAnswer;
    /// The attempt still to settle that carried `logical_id`: the one
    /// outstanding, else the latest parked once its payload was typed.
    fn carrying_attempt(
        &self,
        address: &Address,
        logical_id: &str,
    ) -> Option<crate::store::queue_delivery::QueueDeliveryAttempt>;
}

impl CompletesDirectSends for RunningNexus {
    fn direct_receipt_watch(&self, address: &Address) -> Option<notify::RecommendedWatcher> {
        use crate::store::{AnnouncesLaunchChanges, ReadsFlowRows};
        use crate::waking::ReadsClaudeDelivery;
        use notify::Watcher;
        let flow_id = self.live_awake_flow(address)?;
        let node = self.store.flow_node(&flow_id).ok()??;
        if node.harness_kind != HarnessKind::Claude {
            return None;
        }
        let transcript = self.claude_transcript(&node)?;
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
        address: &Address,
    ) -> Result<Option<crate::waking::WakingLease<'_>>, Box<PrivateSendResponse>> {
        use signal_flow::{SendFailure, SendFailureReason};
        let failed =
            |reason| Box::new(PrivateSendResponse::Failed(SendFailure::Unadmitted(reason)));
        let Some(lease) = self.waking.hold(address) else {
            return Ok(None);
        };
        // A refresh owns the Address until its predecessor has become the
        // same metaflow Asleep.  Work admitted after that hold releases
        // belongs to this existing metaflow even when starting its next flow
        // will need configuration that is presently absent.  Configuration
        // is only the prerequisite for creating a metaflow that does not yet
        // exist; wake feasibility is judged after durable admission.
        let existing = self.store.metaflow(address).map_err(|error| {
            failed(SendFailureReason::Refused(Refusal::Store(
                error.to_string(),
            )))
        })?;
        if existing.is_none()
            && !self
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
        if existing.is_none()
            && flow_id.is_none()
            && (!matches!(self.store.metaflow_declaration(address), Ok(Some(_)))
                || !matches!(self.store.configured_model(address), Ok(Some(_))))
        {
            return Err(failed(SendFailureReason::RecipientUnavailable(
                address.clone(),
            )));
        }
        match self.store.metaflow(address) {
            Ok(Some(_)) => {}
            Ok(None) => return Ok(None),
            Err(error) => {
                return Err(failed(SendFailureReason::Refused(Refusal::Store(
                    error.to_string(),
                ))));
            }
        }
        if !matches!(self.store.outstanding_delivery(address), Ok(None))
            || flow_id.as_ref().is_some_and(|flow_id| {
                !matches!(self.store.address_hold(flow_id), Ok(None))
                    // A restart holds the Address: the request queues for
                    // the resumed seat.
                    || !matches!(
                        crate::store::restart::RecordsRestarts::restart_record(
                            &*self.store,
                            flow_id
                        ),
                        Ok(None)
                    )
            })
        {
            return Ok(None);
        }
        Ok(Some(lease))
    }
    fn carrying_attempt(
        &self,
        address: &Address,
        logical_id: &str,
    ) -> Option<crate::store::queue_delivery::QueueDeliveryAttempt> {
        if let Ok(Some(attempt)) = self.store.outstanding_delivery(address) {
            return Some(attempt);
        }
        self.store
            .unsettled_deliveries(address)
            .ok()?
            .into_iter()
            .rev()
            .find(|attempt| attempt.request_id == logical_id)
    }
    fn complete_direct_send(
        &self,
        address: &Address,
        acceptance: signal_flow::SendAcceptance,
        lease: &crate::waking::WakingLease<'_>,
    ) -> SendAnswer {
        use crate::store::AnnouncesLaunchChanges;
        use signal_flow::{DeliveryState, SendFailure, SendFailureReason};
        use std::time::{Duration, Instant};
        // A configured sleeping recipient starts through the existing
        // fresh Curriculum composition. The Address lease prevents another
        // wake or submission while this direct operation owns its request.
        if self.live_awake_flow(address).is_none() {
            let _ = self.wake(address.clone());
        }
        // Subscribe before input. File events only schedule the existing
        // exact session/inode/cursor/content correlation, never delivery.
        let _receipt_watch = self.direct_receipt_watch(address);
        let outcome = {
            let _turn = self.dispatch_gate.lock().unwrap_or_else(|e| e.into_inner());
            crate::waking::DrainsHeldSeats::drain_all_held(self, address.clone(), lease)
        };
        let logical_id = acceptance.logical_delivery_id.clone();
        // Waiting is a disposition, not a failed attempt: admission already
        // made the request durable. Preserve its exact acceptance while no
        // native attempt exists, and let the ordinary pending path schedule
        // the Address again after this reply.
        if matches!(
            outcome,
            crate::generated::operation::Outcome::WaitingForTurn(_)
        ) && matches!(
            self.store.logical_delivery_snapshot(&logical_id),
            Ok(Some(signal_flow::DeliverySnapshot {
                delivery_state: DeliveryState::Queued,
                ..
            }))
        ) && self.carrying_attempt(address, &logical_id.0).is_none()
        {
            let private_response = PrivateSendResponse::SendAccepted(acceptance);
            return SendAnswer {
                response: crate::message_projection::ProjectsSendResponse::visible_send_response(
                    &private_response,
                ),
                private_response,
                pending: Some(PendingSend {
                    address: address.clone(),
                }),
                _pending_answer: None,
            };
        }
        // This budget bounds waiting, not evidence. Expiry is uncertainty,
        // never proof of absence and never permission for a new attempt.
        let deadline = Instant::now() + Duration::from_secs(10);
        let changes = self.store.launch_changes.clone();
        loop {
            let seen = changes.current();
            let state = {
                let _turn = self.dispatch_gate.lock().unwrap_or_else(|e| e.into_inner());
                if let Some(attempt) = self.carrying_attempt(address, &logical_id.0) {
                    let _ = self.reconcile_delivery(&attempt);
                }
                self.store.logical_delivery_snapshot(&logical_id)
            };
            if let Ok(Some(snapshot)) = state {
                if let Some(response) = snapshot
                    .delivery_state
                    .completed_direct_response(&acceptance)
                {
                    return response.into();
                }
                match snapshot.delivery_state {
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
        if matches!(
            self.store.logical_delivery_snapshot(&logical_id),
            Ok(Some(signal_flow::DeliverySnapshot {
                delivery_state: DeliveryState::Queued,
                ..
            }))
        ) && self.carrying_attempt(address, &logical_id.0).is_none()
        {
            // Starting the next flow failed before any native submission.
            // The existing metaflow still owns the same admitted request;
            // report this attempt truthfully without parking or discarding
            // work that a later configured wake can take up.
            let rejection = if matches!(self.store.metaflow_declaration(address), Ok(None)) {
                signal_flow::StartRejection::Undeclared
            } else {
                signal_flow::StartRejection::CompositionRefused(
                    signal_flow::CompositionRefusal::Unconfigured,
                )
            };
            if let Ok(Some(metaflow)) = self.store.metaflow(address) {
                let _ = self.store.mark_queue_retryable(
                    &metaflow.queue.0,
                    signal_flow::RetryCause::WakeRefused(rejection),
                );
            }
            return PrivateSendResponse::Failed(SendFailure::Admitted(
                signal_flow::SendFailure_Admitted_Data {
                    logical_delivery_id: logical_id,
                    request_id: acceptance.request_id.clone(),
                    send_failure_reason: SendFailureReason::NotSent(observed),
                },
            ))
            .into();
        }
        let state = {
            let _turn = self.dispatch_gate.lock().unwrap_or_else(|e| e.into_inner());
            // A native transcript can land without a harness hook. One
            // final correlated reconciliation follows the bounded event
            // wait; timeout alone is never evidence of absence.
            if let Some(attempt) = self.carrying_attempt(address, &logical_id.0) {
                let _ = self.reconcile_delivery(&attempt);
            }
            self.store
                .finish_failed_direct(address, &logical_id.0, &observed)
        };
        if let Ok(delivery_state) = &state
            && let Some(response) = delivery_state.completed_direct_response(&acceptance)
        {
            return response.into();
        }
        let reason = match state {
            Ok(delivery_state @ (DeliveryState::Uncertain | DeliveryState::Attempting)) => {
                return PrivateSendResponse::SendAccepted(signal_flow::SendAcceptance {
                    logical_delivery_id: logical_id,
                    request_id: acceptance.request_id.clone(),
                    delivery_cursor: acceptance.delivery_cursor,
                    delivery_state,
                })
                .into();
            }
            Ok(_) => SendFailureReason::NotSent(observed),
            Err(error) => SendFailureReason::Refused(Refusal::Store(error.to_string())),
        };
        PrivateSendResponse::Failed(SendFailure::Admitted(
            signal_flow::SendFailure_Admitted_Data {
                logical_delivery_id: logical_id,
                request_id: acceptance.request_id.clone(),
                send_failure_reason: reason,
            },
        ))
        .into()
    }
}
