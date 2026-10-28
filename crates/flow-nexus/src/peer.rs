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
use crate::binding::{ChecksProcessIdentity, ReadsProcessIdentity};
use crate::caller::{CallerProcess, LocatesCallerPane, ReadsProcess};
use meta_signal_flow::{MetaRefusal, ProcessIdentity, ResolverIdentity, Response};
use signal_flow::CallerResolutionRejection;
use std::fs;

/// Names the flow a process runs in.
pub trait ResolvesPeer {
    fn resolve_peer(&self, identity: &ProcessIdentity) -> Response;
}

impl ResolvesPeer for RunningNexus {
    fn resolve_peer(&self, identity: &ProcessIdentity) -> Response {
        let Ok(view) = self.herdr_operation_view() else {
            return Response::PeerResolutionRejected(CallerResolutionRejection::CallerUnknown);
        };
        // The identity must still name the same live process (user and start
        // time), so a reused process ID names no one.
        if !identity.is_live_process() {
            return Response::PeerResolutionRejected(CallerResolutionRejection::CallerUnknown);
        }
        let Ok(process_id) = u32::try_from(identity.process_id) else {
            return Response::PeerResolutionRejected(CallerResolutionRejection::CallerUnknown);
        };
        let pane = CallerProcess { process_id }.caller_pane(&view.herdr);
        match self.resolve_caller_with(&view.herdr, pane, None) {
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
        let Ok(view) = self.herdr_operation_view() else {
            return Some(MetaRefusal::PeerUnknown);
        };
        let message_binary = &view
            .configuration
            .snapshot
            .configured_nexus
            .message_nexus_binary;
        let meta_aspects = &view.configuration.snapshot.configured_nexus.meta_aspects;
        if !message_binary.is_empty() && peer.executable() == fs::canonicalize(message_binary).ok()
        {
            return None;
        }
        let pane = peer.caller_pane(&view.herdr);
        let caller = match self.resolve_caller_with(&view.herdr, pane.clone(), None) {
            signal_flow::Response::CallerResolved(caller) => caller,
            // An unbound pane is admitted under its process identity.
            _ => return None,
        };
        if meta_aspects.contains(&caller.aspect) {
            None
        } else {
            Some(MetaRefusal::PeerNotAuthorized(caller))
        }
    }
}

/// Who the admitted meta peer is, for the records that name who asked.
pub trait NamesMetaPeer {
    /// The peer's process, and the flow it runs in when it runs in one; None
    /// when the process cannot be read.
    fn meta_resolver(&self, peer: Option<CallerProcess>) -> Option<ResolverIdentity>;
}

impl NamesMetaPeer for RunningNexus {
    fn meta_resolver(&self, peer: Option<CallerProcess>) -> Option<ResolverIdentity> {
        let view = self.herdr_operation_view().ok()?;
        let peer = peer?;
        let process_identity = ProcessIdentity::of(peer.process_id)?;
        let flow_id_option =
            match self.resolve_caller_with(&view.herdr, peer.caller_pane(&view.herdr), None) {
                signal_flow::Response::CallerResolved(caller) => Some(caller.flow_id),
                _ => None,
            };
        Some(ResolverIdentity {
            process_identity,
            flow_id_option,
        })
    }
}
