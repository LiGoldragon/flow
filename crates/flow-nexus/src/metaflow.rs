//! The ordinary Message bridge uses the accepted socket's kernel identity.
use crate::caller::{BindsCallerPane, CallerProcess, LocatesCallerPane};
use crate::store::ManagesMetaflowState;
use crate::waking::WakePolicy;
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
                Query::Identify(process) => self.store.identify_process(process),
                Query::Lock(request) => self.store.lock_metaflow(request, now),
                Query::Deliver(request) => {
                    let address = request.lock.address.clone();
                    let wakes = Self::wakes_for(&request.request);
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
