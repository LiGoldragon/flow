//! The ordinary Message bridge uses the accepted socket's kernel identity.
use crate::store::ManagesMetaflowState;
use crate::{RunningNexus, store::metaflow::MutatesMetaflows};
use signal_flow::{Process, Query, Refusal, Response};

pub(crate) trait DispatchesMetaflowPeer {
    fn dispatch_metaflow_peer(&self, query: Query, peer: Option<&Process>) -> Response;
}

impl DispatchesMetaflowPeer for RunningNexus {
    fn dispatch_metaflow_peer(&self, query: Query, peer: Option<&Process>) -> Response {
        let _turn = self
            .dispatch_gate
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let Some(peer) = peer else {
            return Response::Refused(Refusal::NotMessage);
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
                ));
            }
        };
        let result = if let Query::Bind(binding) = query {
            self.store.bind_metaflow(binding, peer)
        } else if let Query::End(address) = query {
            match self.store.identify_process(peer.clone()) {
                Ok(Response::Identified(caller)) if caller == address => {
                    self.store.end_metaflow(address, now)
                }
                Ok(_) => return Response::Refused(Refusal::OffRoute),
                Err(error) => return Response::Refused(Refusal::Store(error.to_string())),
            }
        } else {
            match self.store.message_peer(peer) {
                Ok(true) => {}
                Ok(false) => return Response::Refused(Refusal::NotMessage),
                Err(error) => return Response::Refused(Refusal::Store(error.to_string())),
            }
            match query {
                Query::Identify(process) => self.store.identify_process(process),
                Query::Lock(request) => self.store.lock_metaflow(request, now),
                Query::Deliver(request) => self.store.queue_metaflow(request, now),
                Query::Release(lock) => self.store.release_metaflow_lock(lock, now),
                _ => return Response::Refused(Refusal::NotMessage),
            }
        };
        result.unwrap_or_else(|error| Response::Refused(Refusal::Store(error.to_string())))
    }
}
