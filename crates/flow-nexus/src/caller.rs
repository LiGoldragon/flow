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
use crate::live::HoldsLiveValue;
use crate::peer::FindsFlowInPane;
use crate::store::metaflow::SleepsMetaflows;
use crate::store::send_call::KeepsSendCalls;
use crate::store::sent::NamesCodexAppServers;
use crate::store::{
    FlowStore, ManagesMetaflowState, NamesLiveFlow, ReadsFlowMemory, ReadsFlowRoles, ReadsFlowRows,
    StoreError,
};
use signal_flow::{
    Address, CallerIdentity, CallerResolutionRejection, FlowId, FlowNode, FlowOrigin, HarnessKind,
    Process, Response,
};
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
    /// session whose socket its Herdr server ancestor listens on. The
    /// server is recognised by that socket alone, never by the path of its
    /// executable, so no wrapper or PATH entry named `herdr` can hide it.
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

/// Reads Herdr's sessions and names the ones a process serves.
pub trait ServesListedSessions: Sized {
    /// The sessions Herdr lists now; None when Herdr cannot be read.
    fn read(herdr: &impl KnowsHerdrServers) -> Option<Self>;
    /// The sessions whose socket the process listens on.
    fn served_by(&self, process: &impl ReadsProcess) -> ServedSessions;
}

impl ServesListedSessions for ListedSessions {
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

/// Finds the Codex thread a process runs a command for: the Codex app-server
/// names the thread in the environment of each command it starts, so the
/// thread is read from the nearest ancestor whose parent is a configured
/// Codex app-server, never from the caller's own environment.
pub trait LocatesCodexThread: ReadsProcess {
    /// Ancestors read before giving up.
    const THREAD_ANCESTRY_LIMIT: usize = 64;

    fn codex_thread(&self, app_servers: &BTreeSet<PathBuf>) -> Option<String>;

    /// Whether a configured Codex app-server is an ancestor, independently
    /// of whether its child carries a usable thread.
    fn has_codex_app_server_ancestor(&self, app_servers: &BTreeSet<PathBuf>) -> bool;
}

impl LocatesCodexThread for CallerProcess {
    fn codex_thread(&self, app_servers: &BTreeSet<PathBuf>) -> Option<String> {
        if app_servers.is_empty() {
            return None;
        }
        let mut process = *self;
        for _ in 0..Self::THREAD_ANCESTRY_LIMIT {
            let parent = process.parent()?;
            if parent
                .executable()
                .is_some_and(|executable| app_servers.contains(&executable))
            {
                return process.environment_mark("CODEX_THREAD_ID");
            }
            process = parent;
        }
        None
    }

    fn has_codex_app_server_ancestor(&self, app_servers: &BTreeSet<PathBuf>) -> bool {
        if app_servers.is_empty() {
            return false;
        }
        let mut process = *self;
        for _ in 0..Self::THREAD_ANCESTRY_LIMIT {
            let Some(parent) = process.parent() else {
                return false;
            };
            if parent
                .executable()
                .is_some_and(|executable| app_servers.contains(&executable))
            {
                return true;
            }
            process = parent;
        }
        false
    }
}

/// Deny-owner markers carried by a peer or one of its ancestors. These
/// markers grant no flow or Address authority.
trait ReadsProcessAuthorityHints: ReadsProcess {
    fn has_codex_hint(&self) -> bool;
    fn has_herdr_hint(&self) -> bool;
}

impl ReadsProcessAuthorityHints for CallerProcess {
    fn has_codex_hint(&self) -> bool {
        let mut process = Some(*self);
        for _ in 0..Self::ANCESTRY_LIMIT {
            let Some(current) = process else {
                return false;
            };
            if current.environment_mark("CODEX_THREAD_ID").is_some() {
                return true;
            }
            process = current.parent();
        }
        false
    }

    fn has_herdr_hint(&self) -> bool {
        let mut process = Some(*self);
        for _ in 0..Self::ANCESTRY_LIMIT {
            let Some(current) = process else {
                return false;
            };
            if current.environment_mark("HERDR_SESSION").is_some()
                || current.environment_mark("HERDR_PANE_ID").is_some()
            {
                return true;
            }
            process = current.parent();
        }
        false
    }
}

/// Names the thread a Codex flow's seat runs for a thread of that flow.
/// A subagent's command carries its own thread, not its flow's: the
/// subagent's rollout opens with a `session_meta` whose `session_id` names
/// the root thread of the session it was spawned in, at any depth.
pub trait NamesRootThread {
    /// The root thread the one rollout of `thread` names; `thread` itself
    /// when no single rollout under a configured transcript root names it,
    /// or when its rollout names no other session.
    fn root_thread(&self, thread: &str) -> String;
}

impl NamesRootThread for crate::codex::CodexEndpoints {
    fn root_thread(&self, thread: &str) -> String {
        use crate::herdr::launch::{NamesCodexThread, ReadsLaunchFiles};
        use std::io::BufRead;
        let roots: BTreeSet<&PathBuf> = [&self.stable.transcript_root, &self.next.transcript_root]
            .into_iter()
            .collect();
        let mut found = Vec::new();
        for root in roots {
            let _ = root.collect_native_transcripts(
                thread,
                &signal_flow::HarnessKind::Codex,
                0,
                &mut found,
            );
        }
        let [rollout] = found.as_slice() else {
            return thread.to_owned();
        };
        let mut first = String::new();
        let meta = fs::File::open(rollout)
            .map(std::io::BufReader::new)
            .and_then(|mut reader| reader.read_line(&mut first))
            .ok()
            .and_then(|_| serde_json::from_str::<serde_json::Value>(&first).ok())
            .filter(|row| row.names_thread(thread));
        meta.as_ref()
            .and_then(|row| row.pointer("/payload/session_id"))
            .and_then(serde_json::Value::as_str)
            .filter(|session| !session.is_empty())
            .unwrap_or(thread)
            .to_owned()
    }
}

/// Authority proven from one live kernel peer and Flow's current records.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProcessAuthority {
    /// A live process carrying no registered, pane or native-tool evidence.
    Owner,
    /// The peer belongs to the one live flow currently owning its Address.
    Bound(FlowOrigin),
    /// Address-only Send authority: an explicitly registered process or a
    /// Codex launch before its native flow is bound.
    Sending(Address),
    /// Known process-authority evidence failed, went stale or conflicted.
    Rejected,
}

/// Authenticates an ordinary peer once for every process-authorized operation.
pub trait AuthenticatesProcessAuthority {
    fn process_authority(&self, peer: &Process) -> Result<ProcessAuthority, StoreError>;
}

trait ResolvesProcessAuthorityEvidence {
    fn authority_for_flow(&self, flow_id: &str) -> Result<ProcessAuthority, StoreError>;
    fn bound_address(&self, origin: &FlowOrigin) -> Result<Option<Address>, StoreError>;
    fn live_empty_pane(&self, pane: &CallerPane) -> bool;
}

impl ResolvesProcessAuthorityEvidence for RunningNexus {
    fn authority_for_flow(&self, flow_id: &str) -> Result<ProcessAuthority, StoreError> {
        let Some(node) = self.store.flow_node(flow_id)? else {
            return Ok(ProcessAuthority::Rejected);
        };
        if !node.flow_lifecycle.is_live() || self.store.metaflow_awake_as(flow_id)?.is_none() {
            return Ok(ProcessAuthority::Rejected);
        }
        Ok(ProcessAuthority::Bound(FlowOrigin {
            flow_id: node.flow_id,
            session_id: node.session_id,
        }))
    }

    fn bound_address(&self, origin: &FlowOrigin) -> Result<Option<Address>, StoreError> {
        Ok(self
            .store
            .metaflow_awake_as(&origin.flow_id.0)?
            .map(|metaflow| metaflow.address))
    }

    fn live_empty_pane(&self, pane: &CallerPane) -> bool {
        !self.pane_holds_flow(pane)
            && self
                .herdr
                .session_snapshot(&pane.herdr_session_name)
                .and_then(|snapshot| {
                    snapshot
                        .pointer("/result/snapshot/panes")
                        .and_then(serde_json::Value::as_array)
                        .map(|panes| {
                            panes.iter().any(|shown| {
                                shown.get("pane_id").and_then(serde_json::Value::as_str)
                                    == Some(pane.herdr_pane_id.as_str())
                            })
                        })
                })
                .unwrap_or(false)
    }
}

impl AuthenticatesProcessAuthority for RunningNexus {
    fn process_authority(&self, peer: &Process) -> Result<ProcessAuthority, StoreError> {
        if !FlowStore::process_is_live(peer) {
            return Ok(ProcessAuthority::Rejected);
        }
        let Some(process_id) = u32::try_from(peer.pid.0).ok() else {
            return Ok(ProcessAuthority::Rejected);
        };
        let process = CallerProcess { process_id };
        let codex_hint = process.has_codex_hint();
        let herdr_hint = process.has_herdr_hint();
        let mut bound = Vec::new();
        let mut sending = Vec::new();
        let mut rejected = false;
        let mut empty_operator_pane = false;

        if let Response::Identified(address) = self.store.identify_process(peer.clone())? {
            sending.push(address);
        }
        match process.caller_pane(&self.herdr) {
            Some(pane) if self.pane_holds_flow(&pane) => match self.bound_caller(&pane) {
                Ok(caller) => match self.authority_for_flow(&caller.flow_id.0)? {
                    ProcessAuthority::Bound(origin) => bound.push(origin),
                    _ => rejected = true,
                },
                Err(_) => rejected = true,
            },
            Some(pane) if self.live_empty_pane(&pane) => empty_operator_pane = true,
            Some(_) => rejected = true,
            None if process.marked_caller_pane().is_some()
                || process.marked_pane_id().is_some() =>
            {
                rejected = true;
            }
            None => {}
        }

        let app_servers = self.store.codex_app_servers()?;
        if process.has_codex_app_server_ancestor(&app_servers) {
            match process.codex_thread(&app_servers) {
                Some(thread) => {
                    let root_thread = self.codex_endpoints.current().root_thread(&thread);
                    let live = self
                        .store
                        .flow_nodes()?
                        .into_iter()
                        .filter(|node| {
                            node.harness_kind == HarnessKind::Codex
                                && node.session_id.0 == root_thread
                                && node.flow_lifecycle.is_live()
                        })
                        .collect::<Vec<_>>();
                    match live.as_slice() {
                        [node] => match self.authority_for_flow(&node.flow_id.0)? {
                            ProcessAuthority::Bound(origin) => bound.push(origin),
                            _ => rejected = true,
                        },
                        [] => match self.store.launching_address(&root_thread)? {
                            Some(address) => sending.push(address),
                            None => rejected = true,
                        },
                        _ => rejected = true,
                    }
                }
                None => rejected = true,
            }
        } else if codex_hint {
            // Empty or mismatched SendServers cannot turn a recognizable
            // native tool into owner authority.
            rejected = true;
        }
        if !FlowStore::process_is_live(peer) {
            return Ok(ProcessAuthority::Rejected);
        }

        let bound_agrees = bound
            .first()
            .is_none_or(|first| bound.iter().all(|origin| origin == first));
        let sending_agrees = sending
            .first()
            .is_none_or(|first| sending.iter().all(|address| address == first));
        if rejected || !bound_agrees || !sending_agrees {
            return Ok(ProcessAuthority::Rejected);
        }
        match (bound.into_iter().next(), sending.into_iter().next()) {
            (Some(origin), Some(address))
                if self.bound_address(&origin)?.as_ref() == Some(&address) =>
            {
                Ok(ProcessAuthority::Bound(origin))
            }
            (Some(_), Some(_)) => Ok(ProcessAuthority::Rejected),
            (Some(origin), None) => Ok(ProcessAuthority::Bound(origin)),
            (None, Some(address)) => Ok(ProcessAuthority::Sending(address)),
            (None, None) if codex_hint || (herdr_hint && !empty_operator_pane) => {
                Ok(ProcessAuthority::Rejected)
            }
            (None, None) => Ok(ProcessAuthority::Owner),
        }
    }
}

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
            .and_then(|caller| self.store.flow_node(&caller.flow_id.0).ok().flatten())
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
    fn bound_caller(&self, pane: &CallerPane) -> Result<CallerIdentity, CallerResolutionRejection>;
}

impl BindsCallerPane for RunningNexus {
    fn bound_caller(&self, pane: &CallerPane) -> Result<CallerIdentity, CallerResolutionRejection> {
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
            .role(&node.flow_id.0)
            .map_err(unknown)?
            .ok_or(CallerResolutionRejection::CallerUnknown)
    }
}
