//! Who called: ResolveCaller names the flow bound to the process at the
//! other end of an ordinary connection.
//!
//! The kernel names the peer process (`SO_PEERCRED`); the caller's word is
//! never read. Herdr's snapshot carries no process IDs, so the pane is found
//! the way Herdr itself marks it: every process Herdr starts in a pane
//! inherits `HERDR_PANE_ID`, and in a named session also `HERDR_SESSION`.
//! The peer's environment is read from `/proc`, and when the peer does not
//! carry the marks (an environment the harness scrubbed) its ancestors are
//! read in turn. Herdr's default session exports no `HERDR_SESSION`, so a
//! pane ID alone names its session through the kernel: the walk goes on up
//! to the Herdr server, the process that listens on a session's socket as
//! `herdr session list` names it, and the session is that one. The server is
//! known by the socket it serves, never by its binary: a Herdr reached by
//! another path, or upgraded while its server runs, serves the same socket.
//! A server listening on the sockets of more than one session, a pane ID
//! that changes on the way up, or no server at all names no pane. The pane found must hold exactly one routable flow
//! in the registry, and the live Herdr snapshot must still show that flow's
//! binding (session, pane, terminal and harness), so a pane ID Herdr reused
//! for a new terminal names no one.
//!
//! ResolvePeer (meta) answers the same question for a process named by
//! Message, which reads its own peer from the kernel.

use crate::RunningNexus;
use crate::generated::starter::{CallerProcessing, DerivesOrigin};
use crate::herdr::PanePresence;
use crate::herdr::ReadsHerdrPanes;
use crate::store::{ReadsFlowRoles, ReadsFlowRows, StoreError};
use signal_flow::{Caller, CallerResolutionRejection, FlowId, FlowNode, Response};
use std::collections::BTreeSet;
use std::{fs, os::unix::net::UnixStream, path::PathBuf};

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

/// One Herdr session, at the socket its server listens on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HerdrSession {
    pub name: String,
    pub socket: PathBuf,
}

/// What Herdr says of its servers.
pub trait KnowsHerdrServers {
    /// Every session Herdr lists, running or not; None when Herdr cannot
    /// be read.
    fn herdr_sessions(&self) -> Option<Vec<HerdrSession>>;
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
    /// The pane ID Herdr marked in the process's environment.
    fn marked_pane_id(&self) -> Option<String>;
    /// The process's parent; none for init or a process already gone.
    fn parent(&self) -> Option<Self>;
    /// The executable the process runs, canonical.
    fn executable(&self) -> Option<PathBuf>;
    /// The named Unix sockets the process listens on, as the kernel lists
    /// them: each listening socket among its open descriptors, at the path
    /// it was bound to.
    fn listening_sockets(&self) -> Vec<PathBuf>;
}

/// One `HERDR_*` mark in a process's environment.
trait ReadsEnvironmentMark {
    fn environment_mark(&self, name: &str) -> Option<String>;
}

impl ReadsEnvironmentMark for CallerProcess {
    fn environment_mark(&self, name: &str) -> Option<String> {
        let environment = fs::read(format!("/proc/{}/environ", self.process_id)).ok()?;
        let prefix = format!("{name}=");
        environment
            .split(|byte| *byte == 0)
            .filter_map(|entry| {
                String::from_utf8_lossy(entry)
                    .strip_prefix(prefix.as_str())
                    .map(str::to_owned)
            })
            .next_back()
            .filter(|value| !value.is_empty())
    }
}

impl ReadsProcess for CallerProcess {
    fn marked_pane(&self) -> Option<CallerPane> {
        Some(CallerPane {
            herdr_session_name: self.environment_mark("HERDR_SESSION")?,
            herdr_pane_id: self.environment_mark("HERDR_PANE_ID")?,
        })
    }

    fn marked_pane_id(&self) -> Option<String> {
        self.environment_mark("HERDR_PANE_ID")
    }

    fn parent(&self) -> Option<Self> {
        let stat = fs::read_to_string(format!("/proc/{}/stat", self.process_id)).ok()?;
        let (_, fields) = stat.rsplit_once(") ")?;
        let process_id = fields.split_whitespace().nth(1)?.parse::<u32>().ok()?;
        (process_id > 1).then_some(Self { process_id })
    }

    fn executable(&self) -> Option<PathBuf> {
        fs::canonicalize(format!("/proc/{}/exe", self.process_id)).ok()
    }

    fn listening_sockets(&self) -> Vec<PathBuf> {
        // __SO_ACCEPTCON: the socket listens.
        const ACCEPTS_CONNECTIONS: u32 = 0x0001_0000;
        let inodes: BTreeSet<String> = fs::read_dir(format!("/proc/{}/fd", self.process_id))
            .into_iter()
            .flatten()
            .filter_map(|entry| fs::read_link(entry.ok()?.path()).ok())
            .filter_map(|target| {
                target
                    .to_str()?
                    .strip_prefix("socket:[")?
                    .strip_suffix(']')
                    .map(str::to_owned)
            })
            .collect();
        let table =
            fs::read_to_string(format!("/proc/{}/net/unix", self.process_id)).unwrap_or_default();
        table
            .lines()
            .skip(1)
            .filter_map(|row| {
                let columns: Vec<&str> = row.split_whitespace().collect();
                let flags = u32::from_str_radix(columns.get(3)?, 16).ok()?;
                let path = columns.get(7)?;
                (flags & ACCEPTS_CONNECTIONS != 0
                    && inodes.contains(*columns.get(6)?)
                    && path.starts_with('/'))
                .then(|| PathBuf::from(path))
            })
            .collect()
    }
}

/// Finds the pane a process runs in: its own marks, else its nearest
/// marked ancestor's.
pub trait LocatesCallerPane: ReadsProcess {
    /// Ancestors read before giving up; a pane's process tree is shallow.
    const ANCESTRY_LIMIT: usize = 64;

    /// The pane of the nearest process carrying both Herdr marks.
    fn marked_caller_pane(&self) -> Option<CallerPane> {
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

    /// The pane of the nearest marked process. A process carrying only
    /// `HERDR_PANE_ID` (Herdr's default session) is placed in the one
    /// session whose socket its Herdr server ancestor listens on.
    fn caller_pane(&self, herdr: &impl KnowsHerdrServers) -> Option<CallerPane> {
        if let Some(pane) = self.marked_pane() {
            return Some(pane);
        }
        let mut pane_id = self.marked_pane_id();
        let mut sessions = None;
        let mut ancestor = self.parent()?;
        for _ in 0..Self::ANCESTRY_LIMIT {
            if let Some(pane_id) = &pane_id {
                if sessions.is_none() {
                    sessions = Some(ListedSessions::read(herdr)?);
                }
                match sessions.as_ref()?.served_by(&ancestor) {
                    ServedSessions::One(herdr_session_name) => {
                        return Some(CallerPane {
                            herdr_session_name,
                            herdr_pane_id: pane_id.to_owned(),
                        });
                    }
                    ServedSessions::Several => return None,
                    ServedSessions::None => {}
                }
                // Every marked process between the peer and its server
                // names the same pane.
                match (ancestor.marked_pane(), ancestor.marked_pane_id()) {
                    (Some(pane), _) if &pane.herdr_pane_id == pane_id => return Some(pane),
                    (_, Some(other)) if &other != pane_id => return None,
                    _ => {}
                }
            } else if let Some(pane) = ancestor.marked_pane() {
                return Some(pane);
            } else {
                pane_id = ancestor.marked_pane_id();
            }
            ancestor = ancestor.parent()?;
        }
        None
    }
}

/// The sessions Herdr lists, each at the socket its server listens on,
/// canonical; a session whose socket does not exist is left out.
pub struct ListedSessions {
    sessions: Vec<(String, PathBuf)>,
}

/// Which listed sessions a process serves.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ServedSessions {
    /// It listens on no listed session's socket: it is no Herdr server.
    None,
    /// The Herdr server of this one session.
    One(String),
    /// It listens on the sockets of more than one session.
    Several,
}

impl ListedSessions {
    fn read(herdr: &impl KnowsHerdrServers) -> Option<Self> {
        let sessions = herdr
            .herdr_sessions()?
            .into_iter()
            .filter_map(|session| {
                fs::canonicalize(&session.socket)
                    .ok()
                    .map(|socket| (session.name, socket))
            })
            .collect();
        Some(Self { sessions })
    }

    /// The sessions whose socket the process listens on.
    fn served_by(&self, process: &impl ReadsProcess) -> ServedSessions {
        let listening: BTreeSet<PathBuf> = process
            .listening_sockets()
            .iter()
            .filter_map(|socket| fs::canonicalize(socket).ok())
            .collect();
        let served: BTreeSet<&String> = self
            .sessions
            .iter()
            .filter(|(_, socket)| listening.contains(socket))
            .map(|(name, _)| name)
            .collect();
        let mut served = served.into_iter();
        match (served.next(), served.next()) {
            (None, _) => ServedSessions::None,
            (Some(name), None) => ServedSessions::One(name.clone()),
            (Some(_), Some(_)) => ServedSessions::Several,
        }
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

/// The origin of what a calling process asks: its pane's one bound flow
/// with that flow's native session, as Identify resolves a caller, or
/// Unbound when it is no bound flow (a deploy script, a shell).
impl DerivesOrigin for RunningNexus {
    fn origin_of<N: CallerProcessing>(&self, input: N) -> signal_flow::Origin {
        input
            .caller_process()
            .and_then(|process| process.caller_pane(&self.herdr))
            .and_then(|pane| self.bound_caller(&pane).ok())
            .and_then(|caller| self.store.flow_node(&caller.flow_id).ok().flatten())
            .map(|node| {
                signal_flow::Origin::Flow(signal_flow::FlowOrigin {
                    flow_id: node.flow_id,
                    session_id: node.session_id,
                })
            })
            .unwrap_or(signal_flow::Origin::Unbound)
    }
}

/// The one flow a pane holds, with its role.
pub(crate) trait BindsCallerPane {
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
