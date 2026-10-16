//! The ordinary Message bridge uses the accepted socket's kernel identity.
use crate::caller::{BindsCallerPane, CallerProcess, LocatesCallerPane};
use crate::store::metaflow::SleepsMetaflows;
use crate::store::{FlowStore, ManagesMetaflowState, ReadsFlowMemory, StoreError};
use crate::waking::{RestsEndedAwake, WakePolicy};
use crate::{RunningNexus, store::metaflow::MutatesMetaflows};
use signal_flow::{Address, Process, Query, Refusal, Response};

/// A metaflow dispatch's answer, and the Asleep recipient a queued request
/// is due to wake. The wake itself runs after the answer is written, off the
/// dispatch gate.
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
            .and_then(|process_id| CallerProcess { process_id }.caller_pane())
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
                .and_then(|process_id| CallerProcess { process_id }.caller_pane())
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
                Query::Lock(request) => self.store.lock_metaflow(request, now),
                Query::Deliver(request) => {
                    let address = request.lock.address.clone();
                    let wakes = Self::wakes_for(&request.request);
                    // Queued for a metaflow left Awake as an ended flow, the
                    // request wakes it as it would an Asleep one.
                    self.rest_ended_awake(&address);
                    return match self.store.queue_metaflow(request, now) {
                        Ok(enqueuing) => MetaflowAnswer {
                            wake: (enqueuing.response == Response::Queued
                                && enqueuing.recipient_asleep
                                && wakes
                                && Self::wakes_layer(&address.layer))
                            .then_some(address),
                            response: enqueuing.response,
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
