//! The ordinary Message bridge uses the accepted socket's kernel identity.
use crate::caller::{BindsCallerPane, CallerProcess, LocatesCallerPane};
use crate::store::metaflow::SleepsMetaflows;
use crate::store::send_server::{NamesThreadMetaflow, SendServerTrust, TrustsSendServers};
use crate::store::{FlowStore, ManagesMetaflowState, ReadsFlowMemory, StoreError};
use crate::waking::RestsEndedAwake;
use crate::{RunningNexus, store::metaflow::MutatesMetaflows};
use signal_flow::{Address, IdentifyThread_Data, Process, Query, Refusal, Response};

/// A metaflow dispatch's answer, and the recipient a Deliver queued for.
/// Whether it wakes is read from Flow's records after the answer is written
/// or failed to be (`SleepsAndWakesSeats::wake_due`), off the dispatch gate.
pub(crate) struct MetaflowAnswer {
    pub response: Response,
    pub wake: Option<Address>,
}

impl From<Response> for MetaflowAnswer {
    fn from(response: Response) -> Self {
        Self {
            response,
            wake: None,
        }
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
    fn identify_peer_process(&self, process: Process) -> Result<Response, StoreError>;
}

impl IdentifiesPeerProcess for RunningNexus {
    fn identify_peer_process(&self, process: Process) -> Result<Response, StoreError> {
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
            .and_then(|process_id| CallerProcess { process_id }.caller_pane(&self.herdr))
        else {
            return unidentified();
        };
        if !FlowStore::process_is_live(&process) {
            return unidentified();
        }
        let Ok(caller) = self.bound_caller(&pane) else {
            return unidentified();
        };
        match self.store.metaflow_awake_as(&caller.flow_id)? {
            Some(metaflow) if !self.rest_ended_awake(&metaflow.address) => {
                Ok(Response::Identified(metaflow.address))
            }
            _ => unidentified(),
        }
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
    fn identify_thread(&self, request: IdentifyThread_Data) -> Result<Response, StoreError>;
}

impl IdentifiesThread for RunningNexus {
    fn identify_thread(&self, request: IdentifyThread_Data) -> Result<Response, StoreError> {
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
        match self.store.thread_metaflow(&native_session_id)? {
            Some(metaflow) if !self.rest_ended_awake(&metaflow.address) => {
                Ok(Response::Identified(metaflow.address))
            }
            _ => Ok(Response::Refused(Refusal::Unidentified(process))),
        }
    }
}

pub(crate) trait DispatchesMetaflowPeer {
    fn dispatch_metaflow_peer(&self, query: Query, peer: Option<&Process>) -> MetaflowAnswer;
}

impl DispatchesMetaflowPeer for RunningNexus {
    fn dispatch_metaflow_peer(&self, query: Query, peer: Option<&Process>) -> MetaflowAnswer {
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
            // Awake as that flow, so the flow's own Stop can put it to sleep.
            let launched = u32::try_from(peer.pid)
                .ok()
                .and_then(|process_id| CallerProcess { process_id }.caller_pane(&self.herdr))
                .and_then(|pane| self.bound_caller(&pane).ok())
                .map(|caller| caller.flow_id);
            self.store.bind_metaflow(binding, peer, launched)
        } else if let Query::End(address) = query {
            match self.store.identify_process(peer.clone()) {
                Ok(Response::Identified(caller)) if caller == address => {
                    self.store.end_metaflow(address, now)
                }
                Ok(_) => return Response::Refused(Refusal::OffRoute).into(),
                Err(error) => return Response::Refused(Refusal::Store(error.to_string())).into(),
            }
        } else {
            match self.store.message_peer(peer) {
                Ok(true) => {}
                Ok(false) => return Response::Refused(Refusal::NotMessage).into(),
                Err(error) => return Response::Refused(Refusal::Store(error.to_string())).into(),
            }
            match query {
                Query::Identify(process) => self.identify_peer_process(process),
                Query::IdentifyThread(request) => self.identify_thread(request),
                Query::Lock(request) => self.store.lock_metaflow(request, now),
                Query::Deliver(request) => {
                    let address = request.lock.address.clone();
                    // Queued for a metaflow left Awake as an ended flow, the
                    // request wakes it as it would an Asleep one.
                    self.rest_ended_awake(&address);
                    return match self.store.queue_metaflow(request, now) {
                        Ok(response) => MetaflowAnswer {
                            wake: (response == Response::Queued).then_some(address),
                            response,
                        },
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
}
