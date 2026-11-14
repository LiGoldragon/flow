//! The ordinary Message bridge: Bind, Identify, IdentifyThread, Lock,
//! Deliver, Release, Acknowledge and ArchiveRequest, each taken from any
//! caller.
use crate::caller::{BindsCallerPane, CallerProcess, LocatesCallerPane, NamesRootThread};
use crate::live::HoldsLiveValue;
use crate::sending::RefusesUnidentified;
use crate::sending::{CompletesDirectSends, IdentifiesSender, SendsRequests};
use crate::store::send_outcome::RecordsSendOutcomes;
use crate::store::send_server::NamesThreadMetaflow;
use crate::store::{ManagesMetaflowState, StoreError};
use crate::waking::{HoldsSoftForIdle, RestsEndedAwake};
use crate::{RunningNexus, store::metaflow::MutatesMetaflows};
use datom_codec::Datomizable;
use protos::{Compactable, Protosizable};
use sha2::{Digest, Sha256};
use signal_flow::{IdentifyThread_Data, Input, Output, Process, Refusal};

/// A metaflow dispatch's answer.
pub(crate) struct MetaflowAnswer {
    pub response: Output,
    pub private_response: signal_flow::PrivateSendResponse,
    pub(crate) _pending_answer: Option<crate::store::logical_delivery::PendingAnswerHold>,
}

impl From<signal_flow::PrivateSendResponse> for MetaflowAnswer {
    fn from(private_response: signal_flow::PrivateSendResponse) -> Self {
        Self {
            response: crate::message_projection::ProjectsSendResponse::visible_send_response(
                &private_response,
            ),
            private_response,
            _pending_answer: None,
        }
    }
}

impl From<Output> for MetaflowAnswer {
    fn from(response: Output) -> Self {
        Self {
            response,
            private_response: signal_flow::PrivateSendResponse::Refused(Refusal::NotMessage),
            _pending_answer: None,
        }
    }
}

/// Names the metaflow a Codex thread runs as.
pub(crate) trait IdentifiesThread {
    /// The Address of the metaflow Awake as the one live Codex flow whose
    /// thread Flow recorded at its Start; any other thread is Unidentified.
    /// The shared app-server is never bound, and the process's environment
    /// is never read.
    fn identify_thread(&self, request: IdentifyThread_Data) -> Result<Output, StoreError>;
}

impl IdentifiesThread for RunningNexus {
    fn identify_thread(&self, request: IdentifyThread_Data) -> Result<Output, StoreError> {
        let IdentifyThread_Data {
            process,
            native_session_id,
        } = request;
        let root_thread = self
            .codex_endpoints
            .current()
            .root_thread(&native_session_id.0);
        match self.store.thread_metaflow(&root_thread)? {
            Some(metaflow) if !self.rest_ended_awake(&metaflow.address) => {
                Ok(Output::ThreadIdentified(metaflow.address))
            }
            _ => Ok(Output::Refused(Refusal::unidentified(process))),
        }
    }
}

pub(crate) trait DispatchesMetaflowPeer {
    fn dispatch_metaflow_peer(&self, query: Input, peer: Option<&Process>) -> MetaflowAnswer;
}

impl DispatchesMetaflowPeer for RunningNexus {
    fn dispatch_metaflow_peer(&self, query: Input, peer: Option<&Process>) -> MetaflowAnswer {
        let _turn = self
            .dispatch_gate
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let now = match std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .ok()
            .and_then(|duration| i64::try_from(duration.as_secs()).ok())
        {
            Some(now) => now,
            None => {
                return Output::Refused(Refusal::Store("clock is outside the lease range".into()))
                    .into();
            }
        };
        let result = if let Input::Bind(binding) = query {
            // A process running in a flow Flow launched binds its Address
            // Awake as that flow, so the flow's own Stop drains its queue.
            let launched = u32::try_from(binding.process.pid.0)
                .ok()
                .and_then(|process_id| CallerProcess { process_id }.caller_pane(&self.herdr))
                .and_then(|pane| self.bound_caller(&pane).ok())
                .map(|caller| caller.flow_id);
            return match self.store.bind_metaflow(binding, launched) {
                Ok(response) => response.into(),
                Err(error) => Output::Refused(Refusal::Store(error.to_string())).into(),
            };
        } else if let Input::ArchiveRequest(request_id) = query {
            // Any caller archives any request by its id.
            use crate::store::logical_delivery::RecordsLogicalDeliveries;
            return match self.store.archive_request(&request_id.0) {
                Ok(Ok(())) => Output::RequestArchived(request_id).into(),
                Ok(Err(rejection)) => Output::ArchiveRejected(rejection).into(),
                Err(_) => {
                    Output::ArchiveRejected(signal_flow::ArchiveRejection::StoreRefused).into()
                }
            };
        } else {
            match query {
                // Identify names the metaflow the named process sends as;
                // a process in no flow is Unidentified.
                Input::Identify(process) => Ok(match self.sender_of(&process) {
                    Ok(signal_flow::SentBy::Sender(address)) => Output::Identified(address),
                    Ok(_) => Output::Refused(Refusal::unidentified(process)),
                    Err(response) => *response,
                }),
                Input::IdentifyThread(request) => self.identify_thread(request),
                Input::Lock(request) => self.store.lock_metaflow(request, now),
                Input::Acknowledge(keys) => {
                    use crate::store::logical_delivery::RecordsLogicalDeliveries;
                    return match self.store.acknowledge_deliver_calls(&keys) {
                        Ok(()) => Output::Acknowledged.into(),
                        Err(error) => Output::Refused(Refusal::Store(error.to_string())).into(),
                    };
                }
                Input::Deliver(request) => {
                    let digest_text = request.datomize(Vec::new()).protosize().compact();
                    let digest = format!("{:x}", Sha256::digest(digest_text.as_bytes()));
                    let sender = request.lock.first_address.clone();
                    let recipient = request.lock.second_address.clone();
                    use crate::store::logical_delivery::RecordsLogicalDeliveries;
                    match self.store.deliver_call(&request.deliver_key) {
                        Ok(Some(call))
                            if call.sender.0 == sender && call.request_digest.0 == digest =>
                        {
                            use crate::store::logical_delivery::HoldsAdmissionAnswers;
                            let pending = self
                                .store
                                .hold_admission_answer(&call.send_acceptance.request_id);
                            let mut answer: MetaflowAnswer =
                                signal_flow::PrivateSendResponse::SendAccepted(
                                    call.send_acceptance,
                                )
                                .into();
                            answer._pending_answer = Some(pending);
                            return answer;
                        }
                        Ok(Some(_)) => return Output::DeliverKeyReused.into(),
                        Ok(None) => {}
                        Err(error) => {
                            return Output::Refused(Refusal::Store(error.to_string())).into();
                        }
                    }
                    self.rest_ended_awake(&request.lock.second_address);
                    let abrupt = !matches!(
                        &request.sendable_message,
                        signal_flow::SendableMessage::Soft(_)
                    );
                    // A Soft request to a seat in its turn is queued for the
                    // seat's idle event.
                    let direct_requested = abrupt
                        || self
                            .live_awake_flow(&recipient)
                            .is_some_and(|flow_id| self.rests_now(&flow_id));
                    // A metaflow another holds takes the request into its
                    // queue instead.
                    let direct_lease = if direct_requested {
                        match self.direct_send_lease(&recipient) {
                            Ok(lease) => lease,
                            Err(refusal) => return (*refusal).into(),
                        }
                    } else {
                        None
                    };
                    let mut response: MetaflowAnswer = match self.store.queue_metaflow(request, now)
                    {
                        Ok(response) => response.into(),
                        Err(error) => signal_flow::PrivateSendResponse::Refused(Refusal::Store(
                            error.to_string(),
                        ))
                        .into(),
                    };
                    if let signal_flow::PrivateSendResponse::SendAccepted(acceptance) =
                        &response.private_response
                    {
                        use crate::store::logical_delivery::HoldsAdmissionAnswers;
                        response._pending_answer =
                            Some(self.store.hold_admission_answer(&acceptance.request_id));
                    }
                    drop(_turn);
                    if let Some(lease) = direct_lease {
                        match &response.private_response {
                            signal_flow::PrivateSendResponse::SendAccepted(accepted) => {
                                // Deliver answers the admission which Message can
                                // recover by its key. Native delivery advances the
                                // observed state; it never replaces that answer.
                                let _ =
                                    self.complete_direct_send(&recipient, accepted.clone(), &lease);
                            }
                            signal_flow::PrivateSendResponse::Refused(refusal) => {
                                response.private_response =
                                    signal_flow::PrivateSendResponse::Failed(
                                        signal_flow::SendFailure::Unadmitted(
                                            signal_flow::SendFailureReason::Refused(
                                                refusal.clone(),
                                            ),
                                        ),
                                    );
                                response.response =
                                    crate::message_projection::ProjectsSendResponse::visible_send_response(
                                        &response.private_response,
                                    );
                            }
                            _ => {}
                        }
                    }
                    return match self.store.record_send_outcome(
                        peer.cloned(),
                        signal_flow::SentBy::Sender(sender),
                        Some(recipient),
                        digest,
                        response.private_response.clone(),
                    ) {
                        Ok(_) => response,
                        Err(error) => Output::Refused(Refusal::Store(error.to_string())).into(),
                    };
                }
                Input::Release(lock) => self.store.release_metaflow_lock(lock, now),
                _ => {
                    return Output::Refused(Refusal::Store(
                        "a query the metaflow bridge does not serve".into(),
                    ))
                    .into();
                }
            }
        };
        result
            .unwrap_or_else(|error| Output::Refused(Refusal::Store(error.to_string())))
            .into()
    }
}
