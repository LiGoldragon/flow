//! Who is on the meta socket, and whom a process is.
//!
//! ResolvePeer names the flow a given process runs in, the way
//! ResolveCaller names an ordinary connection's own peer: Message reads its
//! own peer from the kernel and asks Flow whose it is.
//!
//! The meta socket answers every peer alike: the Nexus runs for its owner
//! and keeps nobody out. The peer is read only for the records that name
//! who asked, a queue resolution's resolver.

use crate::RunningNexus;
use crate::binding::{ChecksProcessIdentity, ReadsProcessIdentity};
use crate::caller::{CallerPane, CallerProcess, LocatesCallerPane, ResolvesCaller};
use crate::store::ReadsFlowRoles;
use meta_signal_flow::{Output, ProcessIdentity, ResolverIdentity};
use signal_flow::CallerResolutionRejection;

/// Names the flow a process runs in.
pub trait ResolvesPeer {
    fn resolve_peer(&self, identity: &ProcessIdentity) -> Output;
}

impl ResolvesPeer for RunningNexus {
    fn resolve_peer(&self, identity: &ProcessIdentity) -> Output {
        // The identity must still name the same live process (user and start
        // time), so a reused process ID names no one.
        if !identity.is_live_process() {
            return Output::PeerResolutionRejected(CallerResolutionRejection::CallerUnknown);
        }
        let Ok(process_id) = u32::try_from(identity.process_id.0) else {
            return Output::PeerResolutionRejected(CallerResolutionRejection::CallerUnknown);
        };
        let pane = CallerProcess { process_id }.caller_pane(&self.herdr);
        match self.resolve_caller(pane, None) {
            signal_flow::Output::CallerResolved(caller) => Output::PeerResolved(caller),
            signal_flow::Output::CallerResolutionRejected(rejection) => {
                Output::PeerResolutionRejected(rejection)
            }
            _ => Output::PeerResolutionRejected(CallerResolutionRejection::CallerUnknown),
        }
    }
}

/// Who the meta peer is, for the records that name who asked.
pub trait NamesMetaPeer {
    /// The peer's process, and the flow it runs in when it runs in one; a
    /// peer whose process cannot be read is process 0, in no flow.
    fn meta_resolver(&self, peer: Option<CallerProcess>) -> ResolverIdentity;
}

impl NamesMetaPeer for RunningNexus {
    fn meta_resolver(&self, peer: Option<CallerProcess>) -> ResolverIdentity {
        let unread = ResolverIdentity {
            process_identity: ProcessIdentity {
                process_id: meta_signal_flow::ProcessId(0),
                process_user_id: meta_signal_flow::ProcessUserId(0),
                process_start_token: meta_signal_flow::ProcessStartToken(String::new()),
            },
            flow_id_option: None,
        };
        let Some(peer) = peer else {
            return unread;
        };
        let Some(process_identity) = ProcessIdentity::of(peer.process_id) else {
            return unread;
        };
        let flow_id_option = match self.resolve_caller(peer.caller_pane(&self.herdr), None) {
            signal_flow::Output::CallerResolved(caller) => Some(caller.flow_id),
            _ => None,
        };
        ResolverIdentity {
            process_identity,
            flow_id_option,
        }
    }
}

/// Whether a pane holds a live flow, whatever its role.
pub(crate) trait FindsFlowInPane {
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
