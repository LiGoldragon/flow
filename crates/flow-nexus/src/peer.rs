//! Who is on the meta socket, and whom a process is.
//!
//! ResolvePeer names the flow a given process runs in, the way
//! ResolveCaller names an ordinary connection's own peer: Message reads its
//! own peer from the kernel and asks Flow whose it is.
//!
//! The meta socket admits the configured Message Nexus executable, a
//! process in no flow's pane (the owner, at a terminal or in a service), and
//! a flow whose aspect is in MetaAspects. Both sockets are `0600` under one
//! Unix user, and a process that double-forks out of its pane loses its
//! ancestry and reads as the owner: this gate stops accidents and model
//! mistakes, not an adversary (F4).

use crate::RunningNexus;
use crate::binding::ChecksProcessIdentity;
use crate::caller::{CallerPane, CallerProcess, LocatesCallerPane, ReadsProcess, ResolvesCaller};
use crate::store::ManagesMetaflowState;
use crate::store::ReadsFlowRoles;
use crate::store::delivery::RecordsDeliveries;
use meta_signal_flow::{MetaRefusal, ProcessIdentity, Response};
use signal_flow::CallerResolutionRejection;
use std::fs;

/// Names the flow a process runs in.
pub trait ResolvesPeer {
    fn resolve_peer(&self, identity: &ProcessIdentity) -> Response;
}

impl ResolvesPeer for RunningNexus {
    fn resolve_peer(&self, identity: &ProcessIdentity) -> Response {
        // The identity must still name the same live process (user and start
        // time), so a reused process ID names no one.
        if !identity.is_live_process() {
            return Response::PeerResolutionRejected(CallerResolutionRejection::CallerUnknown);
        }
        let Ok(process_id) = u32::try_from(identity.process_id) else {
            return Response::PeerResolutionRejected(CallerResolutionRejection::CallerUnknown);
        };
        let pane = CallerProcess { process_id }.caller_pane(&self.herdr);
        match self.resolve_caller(pane, None) {
            signal_flow::Response::CallerResolved(caller) => Response::PeerResolved(caller),
            signal_flow::Response::CallerResolutionRejected(rejection) => {
                Response::PeerResolutionRejected(rejection)
            }
            _ => Response::PeerResolutionRejected(CallerResolutionRejection::CallerUnknown),
        }
    }
}

/// Decides whether a meta peer may be answered.
pub trait AdmitsMetaPeer {
    /// None when admitted; the refusal otherwise.
    fn meta_refusal(&self, peer: Option<CallerProcess>) -> Option<MetaRefusal>;
}

impl AdmitsMetaPeer for RunningNexus {
    fn meta_refusal(&self, peer: Option<CallerProcess>) -> Option<MetaRefusal> {
        let Some(peer) = peer else {
            return Some(MetaRefusal::PeerUnknown);
        };
        let (message_binary, meta_aspects) = match self.store.configuration_snapshot() {
            Ok(Some(snapshot)) => (
                snapshot.nexus.message_nexus_binary,
                snapshot.nexus.meta_aspects,
            ),
            Ok(None) => match self.store.delivery_configuration() {
                Ok(legacy) => (legacy.message_nexus_path, legacy.meta_aspects),
                Err(_) => return Some(MetaRefusal::PeerUnknown),
            },
            Err(_) => return Some(MetaRefusal::PeerUnknown),
        };
        if !message_binary.is_empty() && peer.executable() == fs::canonicalize(&message_binary).ok()
        {
            return None;
        }
        let pane = peer.caller_pane(&self.herdr);
        let caller = match self.resolve_caller(pane.clone(), None) {
            signal_flow::Response::CallerResolved(caller) => caller,
            // In a pane that holds a flow whose role Flow does not know: a
            // flow still, never the owner.
            _ if pane.as_ref().is_some_and(|pane| self.pane_holds_flow(pane)) => {
                return Some(MetaRefusal::PeerUnknown);
            }
            // In no pane, or in a pane that holds no flow: the owner.
            _ => return None,
        };
        if meta_aspects.contains(&caller.aspect) {
            None
        } else {
            Some(MetaRefusal::PeerNotAuthorized(caller))
        }
    }
}

/// Whether a pane holds a live flow, whatever its role.
trait FindsFlowInPane {
    fn pane_holds_flow(&self, pane: &CallerPane) -> bool;
}

impl FindsFlowInPane for RunningNexus {
    fn pane_holds_flow(&self, pane: &CallerPane) -> bool {
        self.store
            .flows_in_pane(&pane.herdr_session_name, &pane.herdr_pane_id)
            .map(|flows| !flows.is_empty())
            // A store that cannot say is not taken to mean no flow.
            .unwrap_or(true)
    }
}
