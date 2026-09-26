//! Who called: ResolveCaller names the flow bound to the process at the
//! other end of an ordinary connection.
//!
//! The kernel names the peer process (`SO_PEERCRED`); the caller's word is
//! never read. Herdr's snapshot carries no process IDs, so the pane is found
//! the way Herdr itself marks it: every process Herdr starts in a pane
//! inherits `HERDR_SESSION` and `HERDR_PANE_ID`. The peer's environment is
//! read from `/proc`, and when the peer does not carry them (an environment
//! the harness scrubbed) its ancestors are read in turn. The pane found must
//! hold exactly one routable flow in the registry, and the live Herdr
//! snapshot must still show that flow's binding (session, pane, terminal and
//! harness), so a pane ID Herdr reused for a new terminal names no one.
//!
//! Send takes this up next: its connection resolves the caller the same way
//! and carries the Caller as the sender of the message.

use crate::RunningNexus;
use crate::herdr::PanePresence;
use crate::store::{ReadsFlowRoles, StoreError};
use signal_flow::{Caller, CallerResolutionRejection, FlowId, FlowNode, Response};
use std::{fs, os::unix::net::UnixStream};

/// A process on this host, by its kernel process ID.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CallerProcess {
    pub process_id: u32,
}

/// The Herdr pane a process runs in, as Herdr marked it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CallerPane {
    pub herdr_session_name: String,
    pub herdr_pane_id: String,
}

/// Names the process at the other end of a connection, as the kernel knows it.
pub trait IdentifiesPeer {
    fn peer_process(&self) -> Option<CallerProcess>;
}

impl IdentifiesPeer for UnixStream {
    fn peer_process(&self) -> Option<CallerProcess> {
        let credentials = rustix::net::sockopt::socket_peercred(self).ok()?;
        let process_id = u32::try_from(credentials.pid.as_raw_nonzero().get()).ok()?;
        Some(CallerProcess { process_id })
    }
}

/// What `/proc` says of one process.
pub trait ReadsProcess: Sized {
    /// The pane Herdr marked in the process's environment, when it carries
    /// both marks.
    fn marked_pane(&self) -> Option<CallerPane>;
    /// The process's parent; none for init or a process already gone.
    fn parent(&self) -> Option<Self>;
}

impl ReadsProcess for CallerProcess {
    fn marked_pane(&self) -> Option<CallerPane> {
        let environment = fs::read(format!("/proc/{}/environ", self.process_id)).ok()?;
        let mut herdr_session_name = None;
        let mut herdr_pane_id = None;
        for entry in environment.split(|byte| *byte == 0) {
            let entry = String::from_utf8_lossy(entry);
            if let Some(value) = entry.strip_prefix("HERDR_SESSION=") {
                herdr_session_name = Some(value.to_owned());
            } else if let Some(value) = entry.strip_prefix("HERDR_PANE_ID=") {
                herdr_pane_id = Some(value.to_owned());
            }
        }
        Some(CallerPane {
            herdr_session_name: herdr_session_name.filter(|value| !value.is_empty())?,
            herdr_pane_id: herdr_pane_id.filter(|value| !value.is_empty())?,
        })
    }

    fn parent(&self) -> Option<Self> {
        let stat = fs::read_to_string(format!("/proc/{}/stat", self.process_id)).ok()?;
        let (_, fields) = stat.rsplit_once(") ")?;
        let process_id = fields.split_whitespace().nth(1)?.parse::<u32>().ok()?;
        (process_id > 1).then_some(Self { process_id })
    }
}

/// Finds the pane a process runs in: its own marks, else its nearest
/// marked ancestor's.
pub trait LocatesCallerPane: ReadsProcess {
    /// Ancestors read before giving up; a pane's process tree is shallow.
    const ANCESTRY_LIMIT: usize = 64;

    fn caller_pane(&self) -> Option<CallerPane> {
        if let Some(pane) = self.marked_pane() {
            return Some(pane);
        }
        let mut ancestor = self.parent()?;
        for _ in 0..Self::ANCESTRY_LIMIT {
            if let Some(pane) = ancestor.marked_pane() {
                return Some(pane);
            }
            ancestor = ancestor.parent()?;
        }
        None
    }
}

impl LocatesCallerPane for CallerProcess {}

/// Answers ResolveCaller for the pane a connection's peer runs in.
pub trait ResolvesCaller {
    fn resolve_caller(&self, pane: Option<CallerPane>, claim: Option<FlowId>) -> Response;
}

impl ResolvesCaller for RunningNexus {
    fn resolve_caller(&self, pane: Option<CallerPane>, claim: Option<FlowId>) -> Response {
        let caller = pane
            .ok_or(CallerResolutionRejection::CallerUnknown)
            .and_then(|pane| self.bound_caller(&pane));
        match (caller, claim) {
            (Err(rejection), _) => Response::CallerResolutionRejected(rejection),
            (Ok(caller), Some(claimed)) if claimed != caller.flow_id => {
                Response::CallerResolutionRejected(CallerResolutionRejection::CallerMismatch(
                    caller,
                ))
            }
            (Ok(caller), _) => Response::CallerResolved(caller),
        }
    }
}

/// The one flow a pane holds, with its role.
trait BindsCallerPane {
    fn bound_caller(&self, pane: &CallerPane) -> Result<Caller, CallerResolutionRejection>;
}

impl BindsCallerPane for RunningNexus {
    fn bound_caller(&self, pane: &CallerPane) -> Result<Caller, CallerResolutionRejection> {
        let unknown = |_: StoreError| CallerResolutionRejection::CallerUnknown;
        let mut present = self
            .store
            .flows_in_pane(&pane.herdr_session_name, &pane.herdr_pane_id)
            .map_err(unknown)?
            .into_iter()
            .filter(|node: &FlowNode| self.herdr.pane_presence(node) == PanePresence::Present);
        // No binding, or more than one, names no caller.
        let (Some(node), None) = (present.next(), present.next()) else {
            return Err(CallerResolutionRejection::CallerUnknown);
        };
        self.store
            .role(&node.flow_id)
            .map_err(unknown)?
            .ok_or(CallerResolutionRejection::CallerUnknown)
    }
}
