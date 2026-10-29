//! The ordinary Message bridge uses the accepted socket's kernel identity.
use crate::caller::{
    BindsCallerPane, BindsCallerWith, CallerProcess, LocatesCallerPane, NamesRootThread,
};
use crate::sending::{CompletesDirectSends, IdentifiesSender};
use crate::store::metaflow::SleepsMetaflows;
use crate::store::send_outcome::RecordsSendOutcomes;
use crate::store::send_server::{NamesThreadMetaflow, SendServerTrust, TrustsSendServers};
use crate::store::{FlowStore, ManagesMetaflowState, ReadsFlowMemory, StoreError};
use crate::waking::RestsEndedAwake;
use crate::{ConfiguredUse, RunningNexus, store::metaflow::MutatesMetaflows};
use datom_codec::Datomizable;
use protos::{Compactable, Protosizable};
use sha2::{Digest, Sha256};
use signal_flow::{IdentifyThread_Data, Process, Query, Refusal, Response};

/// A metaflow dispatch's answer.
pub(crate) struct MetaflowAnswer {
    pub response: Response,
}

impl From<Response> for MetaflowAnswer {
    fn from(response: Response) -> Self {
        Self { response }
    }
}

/// Names the metaflow a process Message names runs in.
pub(crate) trait IdentifiesPeerProcess {
    /// A process bound by Bind, or descended from one, names that binding's
    /// Address. Otherwise a process running in the pane of a flow Flow
    /// launched names the Address its launch took: the pane is found from
    /// the Herdr marks the process or its nearest marked ancestor carries,
    /// its session, when only the pane ID is marked, from the socket its
    /// Herdr server ancestor listens on,
    /// the pane must hold exactly that one flow in the registry with the
    /// live Herdr snapshot still showing its binding, and the metaflow must
    /// be Awake as it. The process must still run with the start the
    /// kernel gave Message, before and after its marks are read.
    fn identify_peer_process_configured(
        &self,
        configured: &ConfiguredUse,
        process: Process,
    ) -> Result<Response, StoreError>;

    #[cfg(test)]
    fn identify_peer_process(&self, process: Process) -> Result<Response, StoreError>;
}

impl IdentifiesPeerProcess for RunningNexus {
    fn identify_peer_process_configured(
        &self,
        configured: &ConfiguredUse,
        process: Process,
    ) -> Result<Response, StoreError> {
        match self.store.identify_process(process.clone())? {
            Response::Refused(Refusal::Unidentified(_)) => {}
            identified => return Ok(identified),
        }
        let unidentified = || Ok(Response::Refused(Refusal::Unidentified(process.clone())));
        if !FlowStore::process_is_live(&process) {
            return unidentified();
        }
        let Some(pane) = u32::try_from(process.pid)
            .ok()
            .and_then(|process_id| CallerProcess { process_id }.caller_pane(&configured.herdr))
        else {
            return unidentified();
        };
        if !FlowStore::process_is_live(&process) {
            return unidentified();
        }
        let Ok(caller) = self.bound_caller_with(&configured.herdr, &pane) else {
            return unidentified();
        };
        match self.store.metaflow_awake_as(&caller.flow_id)? {
            Some(metaflow) if !self.rest_ended_awake(configured, &metaflow.address) => {
                Ok(Response::Identified(metaflow.address))
            }
            _ => unidentified(),
        }
    }

    #[cfg(test)]
    fn identify_peer_process(&self, process: Process) -> Result<Response, StoreError> {
        let configured = match self.test_configured_use() {
            Ok(configured) => configured,
            Err(_) => return Ok(Response::Refused(Refusal::NotConfigured)),
        };
        self.identify_peer_process_configured(&configured, process)
    }
}

/// Names the metaflow a Codex thread runs as, for the MCP send server
/// serving it.
pub(crate) trait IdentifiesThread {
    /// The process Message names must run a configured send server under
    /// its paired Codex app-server (`TrustsSendServers`): otherwise
    /// NotSendServer, or NotConfigured while no send server is configured.
    /// The thread then names the Address of the metaflow Awake as the one
    /// live Codex flow whose thread Flow recorded at its Start; any other
    /// thread is Unidentified. The shared app-server is never bound, and the
    /// process's environment is never read.
    fn identify_thread_configured(
        &self,
        configured: &ConfiguredUse,
        request: IdentifyThread_Data,
    ) -> Result<Response, StoreError>;

    #[cfg(test)]
    fn identify_thread(&self, request: IdentifyThread_Data) -> Result<Response, StoreError>;
}

impl IdentifiesThread for RunningNexus {
    fn identify_thread_configured(
        &self,
        configured: &ConfiguredUse,
        request: IdentifyThread_Data,
    ) -> Result<Response, StoreError> {
        let IdentifyThread_Data {
            process,
            native_session_id,
        } = request;
        match self.store.send_server_trust(&process)? {
            SendServerTrust::Unconfigured => return Ok(Response::Refused(Refusal::NotConfigured)),
            SendServerTrust::Untrusted => {
                return Ok(Response::Refused(Refusal::NotSendServer(process)));
            }
            SendServerTrust::Trusted => {}
        }
        let root_thread = configured.codex_endpoints.root_thread(&native_session_id);
        match self.store.thread_metaflow(&root_thread)? {
            Some(metaflow) if !self.rest_ended_awake(configured, &metaflow.address) => {
                Ok(Response::Identified(metaflow.address))
            }
            _ => Ok(Response::Refused(Refusal::Unidentified(process))),
        }
    }

    #[cfg(test)]
    fn identify_thread(&self, request: IdentifyThread_Data) -> Result<Response, StoreError> {
        let configured = match self.test_configured_use() {
            Ok(configured) => configured,
            Err(_) => return Ok(Response::Refused(Refusal::NotConfigured)),
        };
        self.identify_thread_configured(&configured, request)
    }
}

pub(crate) trait DispatchesMetaflowPeer {
    fn dispatch_metaflow_peer_configured(
        &self,
        configured: &ConfiguredUse,
        query: Query,
        peer: Option<&Process>,
    ) -> MetaflowAnswer;

    #[cfg(test)]
    fn dispatch_metaflow_peer(&self, query: Query, peer: Option<&Process>) -> MetaflowAnswer;
}

impl DispatchesMetaflowPeer for RunningNexus {
    fn dispatch_metaflow_peer_configured(
        &self,
        configured: &ConfiguredUse,
        query: Query,
        peer: Option<&Process>,
    ) -> MetaflowAnswer {
        let _turn = self
            .dispatch_gate
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let Some(peer) = peer else {
            return Response::Refused(Refusal::NotMessage).into();
        };
        let now = match std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .ok()
            .and_then(|duration| i64::try_from(duration.as_secs()).ok())
        {
            Some(now) => now,
            None => {
                return Response::Refused(Refusal::Store(
                    "clock is outside the lease range".into(),
                ))
                .into();
            }
        };
        let result = if let Query::Bind(binding) = query {
            // A peer running in a flow Flow launched binds its Address
            // Awake as that flow, so the flow's own Stop drains its queue.
            let launched = u32::try_from(peer.pid)
                .ok()
                .and_then(|process_id| CallerProcess { process_id }.caller_pane(&configured.herdr))
                .and_then(|pane| self.bound_caller_with(&configured.herdr, &pane).ok())
                .map(|caller| caller.flow_id);
            return match self.store.bind_metaflow(binding, peer, launched) {
                Ok(response) => response.into(),
                Err(error) => Response::Refused(Refusal::Store(error.to_string())).into(),
            };
        } else {
            match self.store.message_peer(peer) {
                Ok(true) => {}
                Ok(false) => return Response::Refused(Refusal::NotMessage).into(),
                Err(error) => return Response::Refused(Refusal::Store(error.to_string())).into(),
            }
            match query {
                Query::Identify(process) => Ok(match self.sending_address(&configured, &process) {
                    Ok(address) => Response::Identified(address),
                    Err(response) => *response,
                }),
                Query::IdentifyThread(request) => {
                    self.identify_thread_configured(configured, request)
                }
                Query::Lock(request) => self.store.lock_metaflow(request, now),
                Query::Deliver(request) => {
                    let digest_text = request.datomize(Vec::new()).protosize().compact();
                    let digest = format!("{:x}", Sha256::digest(digest_text.as_bytes()));
                    let sender = request.lock.sender.clone();
                    let recipient = request.lock.address.clone();
                    self.rest_ended_awake(&configured, &request.lock.address);
                    let abrupt = !matches!(&request.message, signal_flow::Message::Soft(_));
                    let direct = abrupt && request.delivery == signal_flow::Delivery::Wake;
                    // A metaflow another holds takes the request into its
                    // queue instead.
                    let direct_lease = if direct {
                        match self.direct_send_lease(&configured, &recipient) {
                            Ok(lease) => lease,
                            Err(refusal) => return (*refusal).into(),
                        }
                    } else {
                        None
                    };
                    let direct = direct_lease.is_some();
                    let mut response: MetaflowAnswer = match self.store.queue_metaflow(request, now)
                    {
                        Ok(response) => response.into(),
                        Err(error) => Response::Refused(Refusal::Store(error.to_string())).into(),
                    };
                    drop(_turn);
                    if direct {
                        response.response = match response.response {
                            Response::SendAccepted(accepted) => {
                                self.complete_direct_send(
                                    &configured,
                                    &recipient,
                                    accepted.logical_delivery_id,
                                )
                                .response
                            }
                            Response::Refused(refusal) => {
                                Response::Failed(signal_flow::SendFailure {
                                    logical_delivery_id_option: None,
                                    send_failure_reason: signal_flow::SendFailureReason::Refused(
                                        refusal,
                                    ),
                                })
                            }
                            other => other,
                        };
                    }
                    return match self.store.record_send_outcome(
                        Some(peer.clone()),
                        Some(sender),
                        Some(recipient),
                        digest,
                        response.response.clone(),
                    ) {
                        Ok(_) => response,
                        Err(error) => Response::Refused(Refusal::Store(error.to_string())).into(),
                    };
                }
                Query::Release(lock) => self.store.release_metaflow_lock(lock, now),
                _ => return Response::Refused(Refusal::NotMessage).into(),
            }
        };
        result
            .unwrap_or_else(|error| Response::Refused(Refusal::Store(error.to_string())))
            .into()
    }

    #[cfg(test)]
    fn dispatch_metaflow_peer(&self, query: Query, peer: Option<&Process>) -> MetaflowAnswer {
        let configured = match self.test_configured_use() {
            Ok(configured) => configured,
            Err(_) => return Response::Refused(Refusal::NotConfigured).into(),
        };
        self.dispatch_metaflow_peer_configured(&configured, query, peer)
    }
}
