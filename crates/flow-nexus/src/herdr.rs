//! Herdr roster validation for durable Flow routes.

use crate::claude::ProjectsClaudeReadiness;
use crate::codex::SelectsCodexEndpoint;
pub(crate) mod claude_paste;
pub mod launch;
pub mod pane;
pub mod reservation;

use crate::codex::{CodexEndpoint, CodexEndpoints};
use std::collections::BTreeSet;
use std::{
    fs,
    path::{Path, PathBuf},
    process::{Command, Output},
};

use signal_flow::{
    EndpointSelection, FlowNode, HarnessKind, HerdrRoute, HerdrRouteSelection, Origin,
    PaneCloseFailure, PaneCloseOutcome, RouteReadiness,
};

/// Reads Herdr's documented session snapshot and validates one complete route.
///
/// A route is keyed on what Herdr binds for the pane's life: the session,
/// the pane id and the terminal id. The agent name is a label the running
/// harness may change (a flow renames itself after launch); it is re-read
/// from the snapshot, never matched.
pub trait ReadsHerdrRoster {
    /// The route as Herdr shows it now, carrying the agent's current name,
    /// or None when the binding is gone or not ready for a prompt.
    fn current_route(&self, node: &FlowNode) -> Option<HerdrRoute>;

    fn route_is_available(&self, node: &FlowNode) -> bool {
        self.current_route(node).is_some()
    }
}

/// What a fresh Herdr snapshot says of a Flow's recorded pane.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PanePresence {
    /// The snapshot shows the recorded binding: the pane is there to close.
    Present,
    /// The snapshot shows the recorded pane and terminal with no agent on
    /// it: a pane whose harness never appeared. It is there to close.
    Bare,
    /// The Flow has no recorded pane, or a readable snapshot shows neither
    /// the binding nor its pane: nothing is left to close.
    Absent,
    /// Herdr could not be read, its snapshot carries no agent roster, or the
    /// pane ID is shown under a different binding: the pane's fate is not
    /// known, so it is neither closed nor counted as reaped.
    Unknown,
}

/// What a fresh Herdr snapshot proves of whether the pane recorded for a
/// Flow is still that Flow's own: the recorded pane and terminal running
/// the recorded native session. A pane is closed only when it is `Owned`
/// or `Bare`; every other answer leaves it as it is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PaneOwnership {
    /// The agent at the recorded pane and terminal, of the recorded
    /// harness, reports the recorded native session; or, a Codex agent
    /// reporting none, one of the pane's foreground processes runs
    /// `resume <thread>` for the recorded thread.
    Owned,
    /// The recorded pane and terminal show no agent: no native session runs
    /// there.
    Bare,
    /// A readable snapshot shows neither the recorded binding nor its pane.
    Absent,
    /// Another native session runs at the recorded pane: its agent reports
    /// another session, a Codex agent's foreground process resumes another
    /// thread, or an agent of another terminal or harness holds the pane.
    Foreign,
    /// The agent there reports no session and nothing proves the recorded
    /// one runs: a Claude agent, or a Codex agent whose foreground
    /// processes do not show its thread.
    Unproven,
    /// No native session is recorded to prove ownership by.
    Unidentified,
    /// Herdr could not be read, or its snapshot carries no agent roster.
    Unknown,
}

/// Closes one Herdr pane a fresh snapshot shows is the Flow's own
/// (`PaneOwnership::Owned` or `Bare`). Every write into a pane is the pane
/// writer's (see `pane` and `crate::delivery`).
pub trait OperatesHerdrPane {
    fn close(&self, node: &FlowNode) -> PaneCloseOutcome;
}

/// Names the directory a harness writes its native transcripts under.
pub trait LocatesNativeTranscripts {
    fn native_transcript_root(
        &self,
        harness: &HarnessKind,
        model_name: &str,
    ) -> Result<PathBuf, String>;
}

impl LocatesNativeTranscripts for HerdrCli {
    fn native_transcript_root(
        &self,
        harness: &HarnessKind,
        model_name: &str,
    ) -> Result<PathBuf, String> {
        match harness {
            HarnessKind::Codex => self
                .codex_endpoints
                .endpoint_for(model_name)
                .map(|endpoint| endpoint.transcript_root.clone())
                .map_err(|error| error.to_string()),
            HarnessKind::Claude => Ok(self.claude_transcript_root.clone()),
        }
    }
}

/// The production Herdr roster reader.
pub struct HerdrCli {
    pub(crate) executable: PathBuf,
    flow_id_executable: PathBuf,
    flows_root: PathBuf,
    codex_endpoints: CodexEndpoints,
    pub(crate) timings: meta_signal_flow::Timings,
    pub(crate) limits: meta_signal_flow::Limits,
    pub(crate) harnesses: Vec<meta_signal_flow::Harness>,
    claude_transcript_root: PathBuf,
    /// The Claude daemon's records, read for a daemon-owned session's
    /// readiness.
    pub(crate) claude_daemon: crate::claude::ClaudeDaemon,
    /// The `flow-hook` Claude Code runs on SessionStart, PostToolUse and
    /// Stop in every Claude flow Flow launches: the one installed beside
    /// this Nexus's own executable.
    pub(crate) harness_hook: PathBuf,
    /// This Nexus's own ordinary socket, the one it serves: every Claude
    /// pane it launches gets it as `FLOW_SOCKET`, so the `flow` CLI that
    /// `flow-hook` runs reports to the Nexus that launched the flow, not to
    /// whatever Nexus the default path under the pane's runtime directory
    /// names.
    pub(crate) ordinary_socket: PathBuf,
    /// The in-memory file of each Claude start's composed system prompt,
    /// by launch request, held from the start until its session has bound.
    pub(crate) held_system_prompts:
        std::sync::Mutex<std::collections::HashMap<String, std::fs::File>>,
    /// One blocking, launch-owned readiness endpoint per Flow-owned Claude
    /// pane. SessionStart sends one typed unit readiness signal to it;
    /// process and session identity are verified separately. Binding consumes
    /// the endpoint once and removes it.
    pub(crate) flow_owned_claude_readiness: std::sync::Mutex<
        std::collections::HashMap<String, (std::os::unix::net::UnixDatagram, std::path::PathBuf)>,
    >,
    pub(crate) flow_owned_claude_readiness_root: PathBuf,
    /// The exact configured installation typed into each reserved pane,
    /// retained across spawn and bind so binding does not select
    /// configuration a second time.
    pub(crate) flow_owned_claude_programs:
        std::sync::Mutex<std::collections::HashMap<String, PathBuf>>,
    /// The stable kernel identity selected when a Flow-owned Claude binds,
    /// keyed by Herdr's pane. A Nexus restart deliberately loses this
    /// authority: a later Stop refuses rather than selecting a fresh PID by
    /// matching text.
    pub(crate) flow_owned_claude_processes:
        std::sync::Mutex<std::collections::HashMap<String, OwnedClaudeProcess>>,
}

#[derive(Debug)]
pub(crate) struct OwnedClaudeProcess {
    pub(crate) process_id: u32,
    pub(crate) process_start: u64,
    pub(crate) process_executable: PathBuf,
    pub(crate) configured_executable: PathBuf,
    pub(crate) session_id: String,
    pub(crate) pidfd: i32,
}

impl Drop for OwnedClaudeProcess {
    fn drop(&mut self) {
        // SAFETY: this descriptor is created by pidfd_open and uniquely
        // owned by this record.
        unsafe { libc::close(self.pidfd) };
    }
}

impl Default for HerdrCli {
    fn default() -> Self {
        use crate::store::{LaysOutDefaults, ReadsAnchors};
        let defaults = crate::store::DefaultConfiguration::from_environment();
        let workspace_root = defaults.source_root();
        let flows_root = workspace_root.join("flows");
        Self {
            executable: PathBuf::from("herdr"),
            flow_id_executable: PathBuf::from("flow-id"),
            flows_root: flows_root.clone(),
            codex_endpoints: CodexEndpoints {
                endpoints: Vec::new(),
                harnesses: Vec::new(),
                timeout: std::time::Duration::from_millis(
                    crate::store::timings_limits::seed_timings()
                        .expect("authored timings seed")
                        .app_server_timeout as u64,
                ),
                workspace_root: workspace_root.clone(),
                limits: crate::store::timings_limits::seed_limits().expect("authored limits seed"),
            },
            timings: crate::store::timings_limits::seed_timings().expect("authored timings seed"),
            limits: crate::store::timings_limits::seed_limits().expect("authored limits seed"),
            harnesses: Vec::new(),
            claude_transcript_root: PathBuf::new(),
            claude_daemon: crate::claude::ClaudeDaemon::under(Path::new("")),
            harness_hook: std::env::current_exe()
                .ok()
                .and_then(|path| path.parent().map(|directory| directory.join("flow-hook")))
                .unwrap_or_else(|| PathBuf::from("flow-hook")),
            ordinary_socket: defaults.ordinary_socket_path(),
            held_system_prompts: Default::default(),
            flow_owned_claude_readiness: Default::default(),
            flow_owned_claude_readiness_root: defaults
                .ordinary_socket_path()
                .parent()
                .unwrap_or_else(|| Path::new("/"))
                .to_path_buf(),
            flow_owned_claude_programs: Default::default(),
            flow_owned_claude_processes: Default::default(),
        }
    }
}

/// Verifies the stable claim minted by `flow-id` for a harness session.
pub trait VerifiesFlowClaim {
    fn identity_is_claimed(&self, node: &FlowNode) -> bool;
}

impl ReadsHerdrRoster for HerdrCli {
    fn current_route(&self, node: &FlowNode) -> Option<HerdrRoute> {
        let HerdrRouteSelection::Available(route) = &node.herdr_route_selection else {
            return None;
        };
        if !self.identity_is_claimed(node) {
            return None;
        }
        let snapshot = self.snapshot(route)?;
        if !snapshot.has_route(route, &node.harness_kind) {
            return None;
        }
        let herdr_agent_name = snapshot.live_agent_name(route, &node.harness_kind)?;
        Some(HerdrRoute {
            herdr_agent_name,
            ..route.clone()
        })
    }
}

impl OperatesHerdrPane for HerdrCli {
    fn close(&self, node: &FlowNode) -> PaneCloseOutcome {
        let HerdrRouteSelection::Available(route) = &node.herdr_route_selection else {
            return PaneCloseOutcome::Refused(PaneCloseFailure::IdentityUnavailable);
        };
        // A reserved Claude is Flow-owned rather than a Herdr-managed
        // agent.  Stop the exact native session Flow selected, wait for the
        // kernel to reap that process identity, and only then ask Herdr to
        // close the pane it owns.  Closing the pane is not used as a proxy
        // for stopping the native process.
        let ownership = self.pane_ownership(node);
        let retained_flow_owned_claude = node.harness_kind == HarnessKind::Claude
            && self
                .flow_owned_claude_processes
                .lock()
                .is_ok_and(|processes| processes.contains_key(&route.herdr_pane_id));
        if retained_flow_owned_claude {
            match self.stop_flow_owned_claude(route, &node.session_id) {
                Ok(OwnedProcessDisposition::Stopped) => {
                    return self.close_flow_owned_claude_pane(route);
                }
                Ok(OwnedProcessDisposition::AlreadyExited) => {
                    let foreground = match self.foreground_processes(route) {
                        Some(foreground) => foreground,
                        None => {
                            return PaneCloseOutcome::Refused(
                                PaneCloseFailure::IdentityUnavailable,
                            );
                        }
                    };
                    if !foreground.is_empty() {
                        return PaneCloseOutcome::Refused(PaneCloseFailure::IdentityMismatch);
                    }
                    return match ownership {
                        PaneOwnership::Owned | PaneOwnership::Bare => {
                            self.close_flow_owned_claude_pane(route)
                        }
                        PaneOwnership::Absent => {
                            PaneCloseOutcome::Refused(PaneCloseFailure::IdentityUnavailable)
                        }
                        PaneOwnership::Foreign => {
                            PaneCloseOutcome::Refused(PaneCloseFailure::IdentityMismatch)
                        }
                        PaneOwnership::Unproven
                        | PaneOwnership::Unidentified
                        | PaneOwnership::Unknown => {
                            PaneCloseOutcome::Refused(PaneCloseFailure::IdentityUnavailable)
                        }
                    };
                }
                Err(failure) => return PaneCloseOutcome::Refused(failure),
            }
        }
        // A Flow-launched Claude without its retained kernel identity is a
        // Nexus-restart uncertainty. Current argv or Herdr ownership never
        // recreates authority to stop it.
        if node.harness_kind == HarnessKind::Claude && matches!(node.origin, Origin::Flow(_)) {
            return PaneCloseOutcome::Refused(PaneCloseFailure::IdentityUnavailable);
        }
        // A pane is closed when Herdr shows it running the Flow's own native
        // session, or running none, whatever its readiness and whether its
        // FlowId claim was given back: closing needs the pane, not a route a
        // prompt could take. A pane now running another session is never
        // closed, and a Flow with no recorded session closes nothing.
        match ownership {
            PaneOwnership::Owned | PaneOwnership::Bare => self.close_owned_pane(route),
            PaneOwnership::Absent => PaneCloseOutcome::Closed,
            PaneOwnership::Foreign => PaneCloseOutcome::Refused(PaneCloseFailure::IdentityMismatch),
            PaneOwnership::Unproven | PaneOwnership::Unidentified | PaneOwnership::Unknown => {
                PaneCloseOutcome::Refused(PaneCloseFailure::IdentityUnavailable)
            }
        }
    }
}

impl HerdrCli {
    #[cfg(test)]
    pub(crate) fn retain_exited_fixture_claude(&self, pane_id: &str, session_id: &str) {
        let mut process = Command::new("sleep")
            .arg("60")
            .spawn()
            .expect("fixture Claude process starts");
        let process_id = process.id();
        let process_start = process_start(process_id).expect("fixture Claude start identity");
        let process_executable = fs::canonicalize(format!("/proc/{process_id}/exe"))
            .expect("fixture Claude executable identity");
        // SAFETY: pidfd_open has no pointer arguments. The inserted owned
        // process closes this descriptor when the fixture adapter drops.
        let pidfd = unsafe { libc::syscall(libc::SYS_pidfd_open, process_id, 0) as i32 };
        assert!(pidfd >= 0, "fixture Claude pidfd opens");
        process.kill().expect("fixture Claude exits");
        process.wait().expect("fixture Claude is reaped");
        self.flow_owned_claude_processes
            .lock()
            .expect("fixture Claude process ownership")
            .insert(
                pane_id.to_owned(),
                OwnedClaudeProcess {
                    process_id,
                    process_start,
                    process_executable: process_executable.clone(),
                    configured_executable: process_executable,
                    session_id: session_id.to_owned(),
                    pidfd,
                },
            );
    }

    fn close_pane(&self, route: &HerdrRoute) -> bool {
        Command::new(&self.executable)
            .args([
                "--session",
                route.herdr_session_name.as_str(),
                "pane",
                "close",
                route.herdr_pane_id.as_str(),
            ])
            .output()
            .is_ok_and(|output| output.status.success())
    }

    fn close_owned_pane(&self, route: &HerdrRoute) -> PaneCloseOutcome {
        if self.close_pane(route) {
            PaneCloseOutcome::Closed
        } else {
            PaneCloseOutcome::Refused(PaneCloseFailure::HerdrCloseFailed)
        }
    }

    fn close_flow_owned_claude_pane(&self, route: &HerdrRoute) -> PaneCloseOutcome {
        let outcome = self.close_owned_pane(route);
        if outcome == PaneCloseOutcome::Closed
            && let Ok(mut processes) = self.flow_owned_claude_processes.lock()
        {
            processes.remove(&route.herdr_pane_id);
        }
        outcome
    }

    /// Stops the one actual foreground process carrying Flow's selected
    /// Claude session. A pidfd keeps the selected kernel process identity
    /// stable across signalling and provides an event-driven exit witness.
    fn stop_flow_owned_claude(
        &self,
        route: &HerdrRoute,
        session: &str,
    ) -> Result<OwnedProcessDisposition, PaneCloseFailure> {
        if session.is_empty() {
            return Err(PaneCloseFailure::IdentityUnavailable);
        }
        let owned = self
            .flow_owned_claude_processes
            .lock()
            .map_err(|_| PaneCloseFailure::IdentityUnavailable)?;
        let Some(process) = owned.get(&route.herdr_pane_id) else {
            // A restarted Nexus has no stable launch identity. Never recover
            // authority from a current argv match.
            return Err(PaneCloseFailure::IdentityUnavailable);
        };
        if process.session_id != session {
            return Err(PaneCloseFailure::IdentityMismatch);
        }
        if pidfd_exited(process.pidfd, 0) {
            // The retained identity reports that the original exited. A
            // fresh matching foreground process is not its replacement,
            // and closing the pane could kill that unrelated process.
            return Ok(OwnedProcessDisposition::AlreadyExited);
        }
        let observed_start = process_start(process.process_id);
        let observed_executable = fs::canonicalize(format!("/proc/{}/exe", process.process_id));
        if observed_start.is_none() || observed_executable.is_err() {
            return if pidfd_exited(process.pidfd, 0) {
                Ok(OwnedProcessDisposition::AlreadyExited)
            } else {
                Err(PaneCloseFailure::IdentityUnavailable)
            };
        }
        let exact_process = retained_process_matches(
            process.process_start,
            &process.process_executable,
            observed_start,
            observed_executable.ok(),
        );
        if !exact_process {
            return Err(PaneCloseFailure::IdentityMismatch);
        }
        let configured_association = fs::canonicalize(&process.configured_executable)
            .map_err(|_| PaneCloseFailure::IdentityUnavailable)?;
        if configured_association != process.configured_executable {
            return Err(PaneCloseFailure::IdentityMismatch);
        }
        let corroborated = self
            .foreground_processes(route)
            .ok_or(PaneCloseFailure::IdentityUnavailable)?
            .iter()
            .any(|current| {
                current.process_id == process.process_id
                    && current
                        .arguments
                        .windows(2)
                        .any(|pair| pair[0] == "--session-id" && pair[1] == process.session_id)
            });
        if !corroborated {
            return Err(PaneCloseFailure::IdentityMismatch);
        }
        let timeout =
            u64::try_from(self.timings.stop_wait).map_err(|_| PaneCloseFailure::StopTimedOut)?;
        match stop_owned_process(process.pidfd, timeout) {
            Ok(ProcessStop::Graceful | ProcessStop::Forced) => Ok(OwnedProcessDisposition::Stopped),
            Ok(ProcessStop::AlreadyExited) => Ok(OwnedProcessDisposition::AlreadyExited),
            Err(ProcessStopFailure::IdentityUnavailable) => {
                Err(PaneCloseFailure::IdentityUnavailable)
            }
            Err(ProcessStopFailure::TimedOut) => Err(PaneCloseFailure::StopTimedOut),
        }
    }
}

fn retained_process_matches(
    recorded_start: u64,
    recorded_executable: &Path,
    observed_start: Option<u64>,
    observed_executable: Option<PathBuf>,
) -> bool {
    observed_start == Some(recorded_start)
        && observed_executable.as_deref() == Some(recorded_executable)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProcessStop {
    Graceful,
    Forced,
    AlreadyExited,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProcessStopFailure {
    IdentityUnavailable,
    TimedOut,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OwnedProcessDisposition {
    Stopped,
    AlreadyExited,
}

/// Signals one stable Linux process identity and blocks on its kernel exit
/// event. A native process that does not honor SIGTERM is compensated with
/// SIGKILL; a timeout never becomes a successful stop.
fn stop_process(
    process_id: u32,
    timeout_milliseconds: u64,
) -> Result<ProcessStop, ProcessStopFailure> {
    let process_id =
        i32::try_from(process_id).map_err(|_| ProcessStopFailure::IdentityUnavailable)?;
    // SAFETY: pidfd_open has no pointer arguments. The returned owned
    // descriptor is closed on every path below.
    let pidfd = unsafe { libc::syscall(libc::SYS_pidfd_open, process_id, 0) as i32 };
    if pidfd < 0 {
        return Err(ProcessStopFailure::IdentityUnavailable);
    }
    let stopped = stop_owned_process(pidfd, timeout_milliseconds);
    // SAFETY: pidfd is an owned descriptor returned by pidfd_open.
    unsafe { libc::close(pidfd) };
    stopped
}

fn stop_owned_process(
    pidfd: i32,
    timeout_milliseconds: u64,
) -> Result<ProcessStop, ProcessStopFailure> {
    let signal = |signal: i32| {
        // SAFETY: pidfd_send_signal is called with the live pidfd above,
        // a signal number, and no siginfo payload or flags.
        unsafe {
            libc::syscall(
                libc::SYS_pidfd_send_signal,
                pidfd,
                signal,
                std::ptr::null::<libc::siginfo_t>(),
                0,
            ) == 0
        }
    };
    let timeout = i32::try_from(timeout_milliseconds).map_err(|_| ProcessStopFailure::TimedOut)?;
    if pidfd_exited(pidfd, 0) {
        return Ok(ProcessStop::AlreadyExited);
    }
    if !signal(libc::SIGTERM) {
        return if pidfd_exited(pidfd, 0) {
            Ok(ProcessStop::AlreadyExited)
        } else {
            Err(ProcessStopFailure::IdentityUnavailable)
        };
    }
    if pidfd_exited(pidfd, timeout) {
        Ok(ProcessStop::Graceful)
    } else if !signal(libc::SIGKILL) {
        if pidfd_exited(pidfd, 0) {
            Ok(ProcessStop::Graceful)
        } else {
            Err(ProcessStopFailure::IdentityUnavailable)
        }
    } else if pidfd_exited(pidfd, timeout) {
        Ok(ProcessStop::Forced)
    } else {
        Err(ProcessStopFailure::TimedOut)
    }
}

fn pidfd_exited(pidfd: i32, timeout: i32) -> bool {
    let mut event = libc::pollfd {
        fd: pidfd,
        events: libc::POLLIN,
        revents: 0,
    };
    // SAFETY: event points to one initialized pollfd for the blocking call.
    (unsafe { libc::poll(&mut event, 1, timeout) == 1 })
        && event.revents & (libc::POLLIN | libc::POLLHUP) != 0
}

pub(crate) fn process_start(process_id: u32) -> Option<u64> {
    let stat = fs::read_to_string(format!("/proc/{process_id}/stat")).ok()?;
    let after_name = stat.rsplit_once(") ")?.1;
    after_name.split_whitespace().nth(19)?.parse().ok()
}

/// Herdr's answer to one `agent prompt`, read for what it proves about the
/// bound pane. Herdr prints a success reply on stdout and an error reply on
/// stderr, each one JSON object.
struct PromptReply {
    output: Output,
}

trait ReadsPromptReply {
    /// The wait that observes the recipient react: Herdr 0.8.2 requires a
    /// lifecycle change within five seconds of an accepted prompt to a
    /// non-working agent, else `agent_prompt_stalled`; any observed state
    /// then matches.
    /// Herdr 0.8.2 error codes answered before any input is sent to the
    /// pane (`handle_agent_prompt` and the pre-prompt `agent get`). Every
    /// other failure may follow typed input and is Uncertain.
    const BEFORE_INPUT: [&'static str; 6] = [
        "agent_blocked",
        "agent_not_found",
        "agent_not_ready",
        "agent_target_ambiguous",
        "empty_agent_prompt",
        "agent_prompt_failed",
    ];
    fn refused_before_input(&self) -> bool;
    /// Whether Herdr answered `agent_prompted` for the exact binding the
    /// route names. The reply carries that pane's own AgentInfo, so the pane
    /// and terminal it names are the observation itself. The agent's `name`
    /// is a label the harness need not carry at all, and is never read here.
    fn prompted_binding(&self, route: &HerdrRoute) -> bool;
}

impl ReadsPromptReply for PromptReply {
    fn refused_before_input(&self) -> bool {
        !self.output.status.success()
            && serde_json::from_slice::<serde_json::Value>(&self.output.stderr)
                .ok()
                .and_then(|reply| {
                    reply
                        .pointer("/error/code")
                        .and_then(serde_json::Value::as_str)
                        .map(|code| Self::BEFORE_INPUT.contains(&code))
                })
                .unwrap_or(false)
    }

    fn prompted_binding(&self, route: &HerdrRoute) -> bool {
        self.output.status.success()
            && serde_json::from_slice::<serde_json::Value>(&self.output.stdout).is_ok_and(|reply| {
                reply
                    .pointer("/result/type")
                    .and_then(serde_json::Value::as_str)
                    == Some("agent_prompted")
                    && reply
                        .pointer("/result/agent/pane_id")
                        .and_then(serde_json::Value::as_str)
                        == Some(route.herdr_pane_id.as_str())
                    && reply
                        .pointer("/result/agent/terminal_id")
                        .and_then(serde_json::Value::as_str)
                        == Some(route.herdr_terminal_id.as_str())
            })
    }
}

pub trait ConfiguresHerdrCli {
    fn with_installations(
        self,
        nexus: &meta_signal_flow::ConfigureNexus,
        harnesses: &[meta_signal_flow::Harness],
    ) -> Self;
    fn with_codex_endpoints(self, codex_endpoints: CodexEndpoints) -> Self;
    /// The ordinary socket this Nexus serves, as its store configures it.
    fn with_ordinary_socket(self, ordinary_socket: &std::path::Path) -> Self;
    /// Re-roots the flows directory on the configured source root.
    fn with_source_root(self, source_root: &std::path::Path) -> Self;
    fn with_runtime_settings(
        self,
        timings: meta_signal_flow::Timings,
        limits: meta_signal_flow::Limits,
    ) -> Self;
    #[cfg(test)]
    fn at(executable: PathBuf, flows_root: PathBuf) -> Self;
}

impl ConfiguresHerdrCli for HerdrCli {
    fn with_installations(
        mut self,
        nexus: &meta_signal_flow::ConfigureNexus,
        harnesses: &[meta_signal_flow::Harness],
    ) -> Self {
        self.executable = PathBuf::from(&nexus.executables.herdr);
        self.flow_id_executable = PathBuf::from(&nexus.executables.flow_id_claimer);
        self.harness_hook = PathBuf::from(&nexus.executables.hook);
        self.flows_root = PathBuf::from(&nexus.flows_root);
        self.harnesses = harnesses.to_vec();
        if let Some(claude) = harnesses
            .iter()
            .find(|harness| harness.harness_kind == HarnessKind::Claude)
        {
            self.claude_transcript_root = PathBuf::from(&claude.home).join("projects");
            self.claude_daemon = crate::claude::ClaudeDaemon::under(&PathBuf::from(&claude.home));
        }
        self
    }
    fn with_codex_endpoints(mut self, codex_endpoints: CodexEndpoints) -> Self {
        self.codex_endpoints = codex_endpoints;
        self
    }

    fn with_ordinary_socket(mut self, ordinary_socket: &std::path::Path) -> Self {
        self.ordinary_socket = ordinary_socket.to_path_buf();
        self.flow_owned_claude_readiness_root = ordinary_socket
            .parent()
            .unwrap_or_else(|| Path::new("/"))
            .to_path_buf();
        self
    }

    fn with_source_root(mut self, source_root: &std::path::Path) -> Self {
        self.flows_root = source_root.join("flows");
        self
    }

    fn with_runtime_settings(
        mut self,
        timings: meta_signal_flow::Timings,
        limits: meta_signal_flow::Limits,
    ) -> Self {
        self.timings = timings;
        self.limits = limits;
        self
    }

    #[cfg(test)]
    fn at(executable: PathBuf, flows_root: PathBuf) -> Self {
        let fixture_root = flows_root
            .parent()
            .expect("fixture flows root has a parent")
            .join("native-transcripts");
        let mut timings =
            crate::store::timings_limits::seed_timings().expect("authored timings seed");
        timings.launch_rest_wait = 1000;
        timings.title_readback_interval = 1;
        Self {
            executable,
            flow_id_executable: fixture_root.join("flow-id"),
            flows_root: flows_root.clone(),
            codex_endpoints: CodexEndpoints {
                endpoints: vec![
                    CodexEndpoint {
                        client_path: PathBuf::from("/fixture/codex"),
                        home: fixture_root.join("codex-home"),
                        socket: "/tmp/stable-codex.sock".into(),
                        transcript_root: fixture_root.join("codex"),
                        model_names: BTreeSet::from([
                            "model-current".into(),
                            "fixture-model".into(),
                        ]),
                    },
                    CodexEndpoint {
                        client_path: PathBuf::from("/fixture/codex-next"),
                        home: fixture_root.join("codex-next-home"),
                        socket: "/tmp/next-codex.sock".into(),
                        transcript_root: fixture_root.join("codex-next"),
                        model_names: BTreeSet::from(["gpt-6-sol".into(), "gpt-6-luna".into()]),
                    },
                ],
                harnesses: Vec::new(),
                timeout: std::time::Duration::from_millis(100),
                workspace_root: flows_root
                    .parent()
                    .expect("fixture flows root has a parent")
                    .to_path_buf(),
                limits: crate::store::timings_limits::seed_limits().expect("authored limits seed"),
            },
            timings,
            limits: crate::store::timings_limits::seed_limits().expect("authored limits seed"),
            harnesses: Vec::new(),
            claude_transcript_root: fixture_root.join("claude"),
            claude_daemon: crate::claude::ClaudeDaemon::under(&fixture_root.join("claude-home")),
            harness_hook: PathBuf::from("/fixture/bin/flow-hook"),
            ordinary_socket: PathBuf::from("/fixture/run/flow-next/flow/flow.sock"),
            held_system_prompts: Default::default(),
            flow_owned_claude_readiness: Default::default(),
            flow_owned_claude_readiness_root: fixture_root.to_path_buf(),
            flow_owned_claude_programs: Default::default(),
            flow_owned_claude_processes: Default::default(),
        }
    }
}

pub trait ReadsHerdrPanes {
    fn validate_registration(&self, node: &FlowNode) -> bool;
    /// Whether the Flow's recorded pane is still in Herdr. An unreadable
    /// Herdr is `Unknown`, never `Absent`.
    fn pane_presence(&self, node: &FlowNode) -> PanePresence;
    /// Whether the Flow's recorded pane still runs its recorded native
    /// session (`PaneOwnership`).
    fn pane_ownership(&self, node: &FlowNode) -> PaneOwnership;
    /// Whether a foreground process of the pane runs `resume <thread>`;
    /// None when Herdr's process information cannot be read.
    fn foreground_resumes(&self, route: &HerdrRoute, thread: &str) -> Option<bool> {
        self.foreground_threads(route)
            .map(|threads| threads.contains(thread))
    }
    /// Every thread a foreground process of the pane resumes, as Herdr
    /// lists the pane's foreground processes and the kernel their
    /// arguments; None when Herdr's process information cannot be read.
    fn foreground_threads(&self, route: &HerdrRoute) -> Option<BTreeSet<String>>;
    /// Foreground processes and their kernel-read command lines for the exact pane.
    fn foreground_processes(&self, route: &HerdrRoute) -> Option<Vec<ForegroundProcess>> {
        let _ = route;
        None
    }
    /// Whether the exact pane currently has one actual foreground process
    /// carrying Flow's selected Claude session. Ambiguity and unreadable
    /// process state are false.
    fn flow_owned_claude_session_is_running(&self, node: &FlowNode) -> bool {
        let _ = node;
        false
    }
    fn refresh_route(&self, node: FlowNode) -> FlowNode;
    fn snapshot(&self, route: &HerdrRoute) -> Option<serde_json::Value>;
    /// The snapshot of one Herdr session.
    fn session_snapshot(&self, herdr_session_name: &str) -> Option<serde_json::Value>;
    /// Whether a pane of this Herdr session runs the flow's native session
    /// now; None when Herdr cannot be read. A pane whose agent reports the
    /// session runs it. A remote-resumed Codex TUI reports none: the pane
    /// Flow recorded for the flow, Herdr recognizing a Codex agent in it,
    /// runs the flow's opened thread unless it reports another session.
    fn holds_native_session(&self, herdr_session_name: &str, node: &FlowNode) -> Option<bool>;
    /// What this Herdr session shows in pane `herdr_pane_id` of an agent
    /// of `harness_kind`: its route, its terminal and agent name as Herdr
    /// reports them; that it shows no such agent there; or that it could
    /// not be read.
    fn pane_route(
        &self,
        herdr_session_name: &str,
        herdr_pane_id: &str,
        harness_kind: &HarnessKind,
    ) -> PaneRoute;
}

/// What a Herdr session shows in one pane of an agent of one harness.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PaneRoute {
    /// The agent is there, at this route.
    Shown(HerdrRoute),
    /// A readable snapshot shows no agent of the harness in the pane.
    NotShown,
    /// The session's snapshot could not be read.
    Unreadable,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForegroundProcess {
    pub process_id: u32,
    pub arguments: Vec<String>,
}

impl ReadsHerdrPanes for HerdrCli {
    fn flow_owned_claude_session_is_running(&self, node: &FlowNode) -> bool {
        let HerdrRouteSelection::Available(route) = &node.herdr_route_selection else {
            return false;
        };
        if node.harness_kind != HarnessKind::Claude || node.session_id.is_empty() {
            return false;
        }
        let Some(processes) = self.foreground_processes(route) else {
            return false;
        };
        processes
            .iter()
            .filter(|process| {
                process
                    .arguments
                    .windows(2)
                    .any(|pair| pair[0] == "--session-id" && pair[1] == node.session_id)
            })
            .count()
            == 1
    }

    fn validate_registration(&self, node: &FlowNode) -> bool {
        let HerdrRouteSelection::Available(route) = &node.herdr_route_selection else {
            return false;
        };
        self.identity_is_claimed(node)
            && self
                .snapshot(route)
                .is_some_and(|snapshot| snapshot.has_binding(route, &node.harness_kind))
    }

    fn pane_presence(&self, node: &FlowNode) -> PanePresence {
        let HerdrRouteSelection::Available(route) = &node.herdr_route_selection else {
            return PanePresence::Absent;
        };
        let Some(snapshot) = self.snapshot(route) else {
            return PanePresence::Unknown;
        };
        let Some(agents) = snapshot
            .pointer("/result/snapshot/agents")
            .and_then(serde_json::Value::as_array)
        else {
            return PanePresence::Unknown;
        };
        if agents
            .iter()
            .any(|agent| agent.matches_binding(route, &node.harness_kind))
        {
            return PanePresence::Present;
        }
        if agents.iter().any(|agent| {
            agent.get("pane_id").and_then(serde_json::Value::as_str)
                == Some(route.herdr_pane_id.as_str())
        }) {
            return PanePresence::Unknown;
        }
        let shown = snapshot
            .pointer("/result/snapshot/panes")
            .and_then(serde_json::Value::as_array)
            .is_some_and(|panes| {
                panes.iter().any(|pane| {
                    pane.get("pane_id").and_then(serde_json::Value::as_str)
                        == Some(route.herdr_pane_id.as_str())
                        && pane.get("terminal_id").and_then(serde_json::Value::as_str)
                            == Some(route.herdr_terminal_id.as_str())
                })
            });
        if shown {
            return PanePresence::Bare;
        }
        PanePresence::Absent
    }

    fn pane_ownership(&self, node: &FlowNode) -> PaneOwnership {
        let HerdrRouteSelection::Available(route) = &node.herdr_route_selection else {
            return PaneOwnership::Absent;
        };
        if node.session_id.is_empty() {
            return PaneOwnership::Unidentified;
        }
        let Some(snapshot) = self.snapshot(route) else {
            return PaneOwnership::Unknown;
        };
        let Some(agents) = snapshot
            .pointer("/result/snapshot/agents")
            .and_then(serde_json::Value::as_array)
        else {
            return PaneOwnership::Unknown;
        };
        if let Some(agent) = agents
            .iter()
            .find(|agent| agent.matches_binding(route, &node.harness_kind))
        {
            let session_source = agent
                .pointer("/agent_session/source")
                .and_then(serde_json::Value::as_str);
            return match (
                agent
                    .pointer("/agent_session/value")
                    .and_then(serde_json::Value::as_str),
                &node.harness_kind,
            ) {
                // A Flow lifecycle report names the selected Claude session
                // but does not make Herdr its process owner. Its actual argv
                // remains the ownership proof and Flow remains responsible
                // for stopping it.
                (Some(session), HarnessKind::Claude)
                    if session == node.session_id && session_source == Some("flow") =>
                {
                    PaneOwnership::Unproven
                }
                (Some(session), _) if session == node.session_id => PaneOwnership::Owned,
                (Some(_), _) => PaneOwnership::Foreign,
                (None, HarnessKind::Codex) => {
                    match self.foreground_resumes(route, &node.session_id) {
                        Some(true) => PaneOwnership::Owned,
                        Some(false) => PaneOwnership::Unproven,
                        None => PaneOwnership::Unknown,
                    }
                }
                (None, HarnessKind::Claude) => PaneOwnership::Unproven,
            };
        }
        if agents.iter().any(|agent| {
            agent.get("pane_id").and_then(serde_json::Value::as_str)
                == Some(route.herdr_pane_id.as_str())
        }) {
            return PaneOwnership::Foreign;
        }
        match self.pane_presence(node) {
            PanePresence::Bare => PaneOwnership::Bare,
            PanePresence::Absent => PaneOwnership::Absent,
            PanePresence::Present | PanePresence::Unknown => PaneOwnership::Unknown,
        }
    }

    fn foreground_threads(&self, route: &HerdrRoute) -> Option<BTreeSet<String>> {
        self.foreground_processes(route).map(|processes| {
            processes
                .into_iter()
                .flat_map(|process| {
                    process
                        .arguments
                        .windows(2)
                        .filter(|pair| pair[0] == "resume" && !pair[1].is_empty())
                        .map(|pair| pair[1].clone())
                        .collect::<Vec<_>>()
                })
                .collect()
        })
    }

    fn foreground_processes(&self, route: &HerdrRoute) -> Option<Vec<ForegroundProcess>> {
        use crate::herdr::launch::RunsHerdr;
        let processes = self
            .run_json(&[
                "--session".into(),
                route.herdr_session_name.clone(),
                "pane".into(),
                "process-info".into(),
                "--pane".into(),
                route.herdr_pane_id.clone(),
            ])
            .ok()?;
        let information = processes.pointer("/result/process_info")?;
        if information
            .get("pane_id")
            .and_then(serde_json::Value::as_str)
            != Some(route.herdr_pane_id.as_str())
        {
            return None;
        }
        let foreground = information
            .get("foreground_processes")
            .and_then(serde_json::Value::as_array)?;
        Some(
            foreground
                .iter()
                .filter_map(|process| {
                    let process_id = u32::try_from(process.get("pid")?.as_u64()?).ok()?;
                    let cmdline = fs::read(format!("/proc/{process_id}/cmdline")).ok()?;
                    let arguments = cmdline
                        .split(|byte| *byte == 0)
                        .filter(|argument| !argument.is_empty())
                        .map(|argument| String::from_utf8_lossy(argument).into_owned())
                        .collect();
                    Some(ForegroundProcess {
                        process_id,
                        arguments,
                    })
                })
                .collect(),
        )
    }

    fn refresh_route(&self, mut node: FlowNode) -> FlowNode {
        let had_persisted_route = matches!(
            node.herdr_route_selection,
            HerdrRouteSelection::Available(_)
        );
        if !had_persisted_route {
            return node;
        }
        match self.current_route(&node) {
            // The agent's current name is reported; the binding is the ids.
            Some(route) => node.herdr_route_selection = HerdrRouteSelection::Available(route),
            None => {
                node.herdr_route_selection = HerdrRouteSelection::Unavailable;
                if let EndpointSelection::Available(endpoint) = &mut node.endpoint_selection {
                    endpoint.route_readiness = RouteReadiness::Parked;
                }
            }
        }
        node
    }

    fn snapshot(&self, route: &HerdrRoute) -> Option<serde_json::Value> {
        self.session_snapshot(&route.herdr_session_name)
    }

    fn holds_native_session(&self, herdr_session_name: &str, node: &FlowNode) -> Option<bool> {
        let snapshot = self.session_snapshot(herdr_session_name)?;
        let agents = snapshot
            .pointer("/result/snapshot/agents")
            .and_then(serde_json::Value::as_array)?;
        // The pane Flow recorded for a Codex flow that opened its thread, in
        // this Herdr session.
        let recorded = match (&node.harness_kind, &node.herdr_route_selection) {
            (HarnessKind::Codex, HerdrRouteSelection::Available(route))
                if route.herdr_session_name == herdr_session_name
                    && !node.session_id.is_empty() =>
            {
                Some(route)
            }
            _ => None,
        };
        Some(agents.iter().any(|agent| {
            match agent
                .pointer("/agent_session/value")
                .and_then(serde_json::Value::as_str)
            {
                Some(session) => session == node.session_id,
                None => {
                    recorded.is_some_and(|route| agent.matches_binding(route, &node.harness_kind))
                }
            }
        }))
    }

    fn pane_route(
        &self,
        herdr_session_name: &str,
        herdr_pane_id: &str,
        harness_kind: &HarnessKind,
    ) -> PaneRoute {
        let expected_harness = match harness_kind {
            HarnessKind::Codex => "codex",
            HarnessKind::Claude => "claude",
        };
        let Some(snapshot) = self.session_snapshot(herdr_session_name) else {
            return PaneRoute::Unreadable;
        };
        let Some(agents) = snapshot
            .pointer("/result/snapshot/agents")
            .and_then(serde_json::Value::as_array)
        else {
            return PaneRoute::Unreadable;
        };
        let Some(agent) = agents.iter().find(|agent| {
            agent.get("pane_id").and_then(serde_json::Value::as_str) == Some(herdr_pane_id)
                && agent.get("agent").and_then(serde_json::Value::as_str) == Some(expected_harness)
        }) else {
            return PaneRoute::NotShown;
        };
        let Some(herdr_terminal_id) = agent
            .get("terminal_id")
            .and_then(serde_json::Value::as_str)
            .filter(|terminal| !terminal.is_empty())
        else {
            return PaneRoute::NotShown;
        };
        PaneRoute::Shown(HerdrRoute {
            herdr_session_name: herdr_session_name.to_owned(),
            herdr_agent_name: agent
                .get("name")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default()
                .to_owned(),
            herdr_pane_id: herdr_pane_id.to_owned(),
            herdr_terminal_id: herdr_terminal_id.to_owned(),
        })
    }

    fn session_snapshot(&self, herdr_session_name: &str) -> Option<serde_json::Value> {
        let output = Command::new(&self.executable)
            .args(["--session", herdr_session_name, "api", "snapshot"])
            .output()
            .ok()?;
        output
            .status
            .success()
            .then(|| serde_json::from_slice(&output.stdout).ok())
            .flatten()
    }
}

impl crate::caller::KnowsHerdrServers for HerdrCli {
    /// `herdr session list --json` names each session with its socket.
    fn herdr_sessions(&self) -> Option<Vec<crate::caller::HerdrSession>> {
        let output = Command::new(&self.executable)
            .args(["session", "list", "--json"])
            .output()
            .ok()?;
        if !output.status.success() {
            return None;
        }
        let listing: serde_json::Value = serde_json::from_slice(&output.stdout).ok()?;
        listing
            .get("sessions")?
            .as_array()?
            .iter()
            .map(|session| {
                Some(crate::caller::HerdrSession {
                    name: session.get("name")?.as_str()?.to_owned(),
                    socket: PathBuf::from(session.get("socket_path")?.as_str()?),
                })
            })
            .collect()
    }
}

/// A Herdr `pane snapshot` reply, read for the agents it shows.
pub trait ReadsHerdrSnapshot {
    /// Whether the snapshot shows the bound agent able to take a prompt.
    fn has_route(&self, route: &HerdrRoute, harness_kind: &HarnessKind) -> bool;
    /// Whether the snapshot shows the bound agent at all.
    fn has_binding(&self, route: &HerdrRoute, harness_kind: &HarnessKind) -> bool;
    /// The name the bound agent carries now.
    fn live_agent_name(&self, route: &HerdrRoute, harness_kind: &HarnessKind) -> Option<String>;
}

impl ReadsHerdrSnapshot for serde_json::Value {
    fn has_route(&self, route: &HerdrRoute, harness_kind: &HarnessKind) -> bool {
        self.pointer("/result/snapshot/agents")
            .and_then(serde_json::Value::as_array)
            .is_some_and(|agents| {
                agents.iter().any(|agent| {
                    agent.matches_binding(route, harness_kind)
                        && matches!(
                            agent
                                .get("agent_status")
                                .and_then(serde_json::Value::as_str),
                            Some("idle" | "done" | "working")
                        )
                        && agent.permits_prompt()
                })
            })
    }

    fn has_binding(&self, route: &HerdrRoute, harness_kind: &HarnessKind) -> bool {
        self.pointer("/result/snapshot/agents")
            .and_then(serde_json::Value::as_array)
            .is_some_and(|agents| {
                agents
                    .iter()
                    .any(|agent| agent.matches_binding(route, harness_kind))
            })
    }

    fn live_agent_name(&self, route: &HerdrRoute, harness_kind: &HarnessKind) -> Option<String> {
        self.pointer("/result/snapshot/agents")
            .and_then(serde_json::Value::as_array)?
            .iter()
            .find(|agent| agent.matches_binding(route, harness_kind))?
            .get("name")
            .and_then(serde_json::Value::as_str)
            .filter(|name| !name.is_empty())
            .map(str::to_owned)
    }
}

/// One agent of a Herdr snapshot.
pub trait ReadsHerdrAgent {
    /// The binding is the pane id and terminal id Herdr assigned (the
    /// session is the snapshot's own) and the harness kind; the agent name
    /// is not part of it, since a running flow may rename its agent.
    fn matches_binding(&self, route: &HerdrRoute, harness_kind: &HarnessKind) -> bool;
    /// Herdr 0.8.2 reports a harness at rest after a turn as `done`, the
    /// same prompt-accepting state as `idle`, and omits `interactive_ready`
    /// for many live panes (every rested Codex pane observed, some Claude
    /// panes). The flag therefore gates a prompt only when Herdr reports it:
    /// absent permits, `true` permits, anything else refuses.
    fn permits_prompt(&self) -> bool;
}

impl ReadsHerdrAgent for serde_json::Value {
    fn matches_binding(&self, route: &HerdrRoute, harness_kind: &HarnessKind) -> bool {
        let expected_harness = match harness_kind {
            HarnessKind::Codex => "codex",
            HarnessKind::Claude => "claude",
        };
        self.get("pane_id").and_then(serde_json::Value::as_str)
            == Some(route.herdr_pane_id.as_str())
            && self.get("terminal_id").and_then(serde_json::Value::as_str)
                == Some(route.herdr_terminal_id.as_str())
            && self.get("agent").and_then(serde_json::Value::as_str) == Some(expected_harness)
    }

    fn permits_prompt(&self) -> bool {
        match self.get("interactive_ready") {
            None | Some(serde_json::Value::Null) => true,
            Some(reported) => reported.as_bool() == Some(true),
        }
    }
}

impl VerifiesFlowClaim for HerdrCli {
    fn identity_is_claimed(&self, node: &FlowNode) -> bool {
        if node.flow_id.is_empty()
            || !node
                .flow_id
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
        {
            return false;
        }
        let marker_path = self.flows_root.join(format!(".{}.flow-id", node.flow_id));
        let Ok(metadata) = fs::symlink_metadata(&marker_path) else {
            return false;
        };
        if metadata.file_type().is_symlink() || !metadata.is_file() {
            return false;
        }
        let Ok(marker) = fs::read_to_string(&marker_path) else {
            return false;
        };
        FlowClaim::decode(&marker).is_some_and(|claim| {
            claim.alias == node.flow_id && claim.harness_kind == node.harness_kind
        })
    }
}

struct FlowClaim {
    harness_kind: HarnessKind,
    identity: String,
    alias: String,
}

trait DecodesFlowClaim {
    fn decode(marker: &str) -> Option<Self>
    where
        Self: Sized;
}

impl DecodesFlowClaim for FlowClaim {
    fn decode(marker: &str) -> Option<Self> {
        let mut lines = marker.lines();
        if lines.next()? != "version=1" {
            return None;
        }
        let harness_kind = match lines.next()? {
            "harness=codex" => HarnessKind::Codex,
            "harness=claude" => HarnessKind::Claude,
            _ => return None,
        };
        let identity = lines.next()?.strip_prefix("identity=")?.to_owned();
        let alias = lines.next()?.strip_prefix("alias=")?.to_owned();
        let optional_version = lines.next();
        let claude_version_is_valid =
            match optional_version {
                None | Some("uuid-version=uuid-v4") => identity.as_bytes().get(12) == Some(&b'4'),
                Some("uuid-version=uuid-v5") => identity.as_bytes().get(12) == Some(&b'5'),
                Some(_) => false,
            } && matches!(identity.as_bytes().get(16), Some(b'8' | b'9' | b'a' | b'b'));
        if lines.next().is_some()
            || identity.len() != 32
            || !identity
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
            || alias.is_empty()
            || !alias
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
            || !matches!(
                (&harness_kind, optional_version),
                (HarnessKind::Codex, None) | (HarnessKind::Claude, _)
            )
            || matches!(harness_kind, HarnessKind::Claude) && !claude_version_is_valid
        {
            return None;
        }
        Some(Self {
            harness_kind,
            identity,
            alias,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{
        HerdrCli, OperatesHerdrPane, ProcessStop, ProcessStopFailure, VerifiesFlowClaim,
        pidfd_exited, stop_owned_process, stop_process,
    };
    use crate::herdr::ConfiguresHerdrCli;
    use crate::herdr::ReadsHerdrPanes;
    use crate::herdr::ReadsHerdrSnapshot;
    use crate::herdr::pane::ReadsAgentStatus;
    use signal_flow::{
        EndpointSelection, FlowLifecycle, FlowNode, FlowOrigin, HarnessKind, HerdrRoute,
        HerdrRouteSelection, Origin,
    };
    use std::{os::unix::process::CommandExt, path::PathBuf, process::Command};

    /// Whether the snapshot's one agent is at rest, as the writer reads it.
    fn idle(snapshot: &serde_json::Value) -> bool {
        matches!(
            snapshot
                .pointer("/result/snapshot/agents/0/agent_status")
                .and_then(serde_json::Value::as_str)
                .agent_state(),
            signal_flow::AgentState::Idle | signal_flow::AgentState::Done
        )
    }

    fn route() -> HerdrRoute {
        HerdrRoute {
            herdr_session_name: "messaging-build".into(),
            herdr_agent_name: "recipient".into(),
            herdr_pane_id: "w1:p2".into(),
            herdr_terminal_id: "term-current".into(),
        }
    }

    #[test]
    fn current_idle_blank_agent_binding_is_available() {
        let snapshot = serde_json::json!({"result":{"snapshot":{"agents":[{
            "agent":"claude", "agent_status":"idle", "interactive_ready":true,
            "name":"recipient", "pane_id":"w1:p2", "terminal_id":"term-current"
        }]}}});
        assert!(snapshot.has_route(&route(), &HarnessKind::Claude));
    }

    #[test]
    fn working_interactive_agent_binding_remains_available_to_message() {
        let snapshot = serde_json::json!({"result":{"snapshot":{"agents":[{
            "agent":"claude", "agent_status":"working", "interactive_ready":true,
            "name":"recipient", "pane_id":"w1:p2", "terminal_id":"term-current"
        }]}}});
        assert!(snapshot.has_binding(&route(), &HarnessKind::Claude));
        assert!(snapshot.has_route(&route(), &HarnessKind::Claude));
    }

    #[test]
    fn replaced_waiting_or_noninteractive_agent_binding_is_unavailable() {
        for snapshot in [
            serde_json::json!({"result":{"snapshot":{"agents":[{
                "agent":"claude", "agent_status":"idle", "interactive_ready":true,
                "name":"recipient", "pane_id":"w1:p2", "terminal_id":"term-replaced"
            }]}}}),
            serde_json::json!({"result":{"snapshot":{"agents":[{
                "agent":"claude", "agent_status":"idle", "interactive_ready":false,
                "name":"recipient", "pane_id":"w1:p2", "terminal_id":"term-current"
            }]}}}),
            serde_json::json!({"result":{"snapshot":{"agents":[{
                "agent":"claude", "agent_status":"waiting", "interactive_ready":true,
                "name":"recipient", "pane_id":"w1:p2", "terminal_id":"term-current"
            }]}}}),
        ] {
            assert!(!snapshot.has_route(&route(), &HarnessKind::Claude));
        }
    }

    fn codex_route() -> HerdrRoute {
        HerdrRoute {
            herdr_session_name: "messaging-build".into(),
            herdr_agent_name: "field-sol-b7da5d".into(),
            herdr_pane_id: "wQ:pT".into(),
            herdr_terminal_id: "term_65c41aac961f978".into(),
        }
    }

    /// The shape Herdr 0.8.2 `api snapshot` reported for a rested Codex
    /// pane on 2026-09-25: status `done`, no `interactive_ready` key.
    fn done_codex_snapshot(interactive_ready: Option<bool>) -> serde_json::Value {
        let mut agent = serde_json::json!({
            "agent":"codex", "agent_status":"done",
            "name":"field-sol-b7da5d", "pane_id":"wQ:pT", "workspace_id":"wQ",
            "tab_id":"wQ:tP", "terminal_id":"term_65c41aac961f978",
            "cwd":"/home/li/primary", "foreground_cwd":"/home/li/primary"
        });
        if let Some(ready) = interactive_ready {
            agent["interactive_ready"] = ready.into();
        }
        serde_json::json!({"result":{"snapshot":{"agents":[agent]}}})
    }

    #[test]
    fn done_codex_pane_without_readiness_flag_is_available() {
        let snapshot = done_codex_snapshot(None);
        assert!(snapshot.has_route(&codex_route(), &HarnessKind::Codex));
        assert!(idle(&snapshot));
    }

    #[test]
    fn done_codex_pane_with_reported_readiness_follows_the_flag() {
        assert!(done_codex_snapshot(Some(true)).has_route(&codex_route(), &HarnessKind::Codex));
        assert!(!done_codex_snapshot(Some(false)).has_route(&codex_route(), &HarnessKind::Codex));
    }

    #[test]
    fn missing_codex_agent_is_unavailable() {
        let snapshot = serde_json::json!({"result":{"snapshot":{"agents":[{
            "agent":"codex", "agent_status":"done",
            "name":"field-luna-e71dab", "pane_id":"wQ:pV", "terminal_id":"term_65c41cd7bd31479"
        }]}}});
        assert!(!snapshot.has_route(&codex_route(), &HarnessKind::Codex));
        let empty = serde_json::json!({"result":{"snapshot":{"agents":[]}}});
        assert!(!empty.has_route(&codex_route(), &HarnessKind::Codex));
    }

    #[test]
    fn working_pane_is_routable_but_not_idle() {
        let snapshot = serde_json::json!({"result":{"snapshot":{"agents":[{
            "agent":"codex", "agent_status":"working",
            "name":"field-sol-b7da5d", "pane_id":"wQ:pT", "terminal_id":"term_65c41aac961f978"
        }]}}});
        assert!(snapshot.has_route(&codex_route(), &HarnessKind::Codex));
        assert!(!idle(&snapshot));
    }

    #[test]
    fn claude_idle_ready_snapshot_is_unchanged() {
        let snapshot = serde_json::json!({"result":{"snapshot":{"agents":[{
            "agent":"claude", "agent_status":"idle", "interactive_ready":true,
            "name":"recipient", "pane_id":"w1:p2", "terminal_id":"term-current"
        }]}}});
        assert!(snapshot.has_route(&route(), &HarnessKind::Claude));
        assert!(idle(&snapshot));
    }

    #[test]
    fn imported_native_session_uses_the_existing_harness_claim_marker() {
        let flows = tempfile::tempdir().expect("temporary flows root");
        let mut node = FlowNode {
            flow_id: "1ac573".into(),
            session_id: "f52d95a1-857f-49ab-8c6f-3aa0a9db826b".into(),
            harness_kind: HarnessKind::Claude,
            endpoint_selection: EndpointSelection::Unavailable,
            herdr_route_selection: HerdrRouteSelection::Available(route()),
            origin: Origin::Flow(FlowOrigin {
                flow_id: "1ac573".into(),
                session_id: "f52d95a1-857f-49ab-8c6f-3aa0a9db826b".into(),
            }),
            flow_lifecycle: FlowLifecycle::Active,
        };
        std::fs::write(
            flows.path().join(".1ac573.flow-id"),
            "version=1\nharness=claude\nidentity=1ac573e8952240aba04c317ab1790728\nalias=1ac573\nuuid-version=uuid-v4\n",
        )
        .expect("claim marker");
        let herdr = HerdrCli::at(PathBuf::from("herdr"), flows.path().into());
        assert!(herdr.identity_is_claimed(&node));
        node.flow_id = "unknown".into();
        assert!(!herdr.identity_is_claimed(&node));
    }

    #[test]
    fn stale_persisted_route_parks_native_fallback_but_native_only_rows_are_unchanged() {
        let herdr = HerdrCli::at(
            PathBuf::from("missing-herdr-fixture"),
            PathBuf::from("/missing-flows-fixture"),
        );
        let mut node = FlowNode {
            flow_id: "1ac573".into(),
            session_id: "f52d95a1-857f-49ab-8c6f-3aa0a9db826b".into(),
            harness_kind: HarnessKind::Claude,
            endpoint_selection: EndpointSelection::Available(signal_flow::Available_Data {
                endpoint_path: "/tmp/native-fallback.sock".into(),
                route_readiness: signal_flow::RouteReadiness::Ready,
            }),
            herdr_route_selection: HerdrRouteSelection::Available(route()),
            origin: Origin::Flow(FlowOrigin {
                flow_id: "1ac573".into(),
                session_id: "f52d95a1-857f-49ab-8c6f-3aa0a9db826b".into(),
            }),
            flow_lifecycle: FlowLifecycle::Active,
        };
        let stale = herdr.refresh_route(node.clone());
        assert_eq!(
            stale.herdr_route_selection,
            HerdrRouteSelection::Unavailable
        );
        assert!(matches!(
            stale.endpoint_selection,
            EndpointSelection::Available(signal_flow::Available_Data {
                route_readiness: signal_flow::RouteReadiness::Parked,
                ..
            })
        ));

        node.herdr_route_selection = HerdrRouteSelection::Unavailable;
        assert!(matches!(
            herdr.refresh_route(node).endpoint_selection,
            EndpointSelection::Available(signal_flow::Available_Data {
                route_readiness: signal_flow::RouteReadiness::Ready,
                ..
            })
        ));
    }

    #[test]
    fn flow_owned_process_stop_waits_for_the_exact_kernel_exit() {
        let mut process = Command::new("sleep")
            .arg("60")
            .spawn()
            .expect("sleep starts");
        assert_eq!(stop_process(process.id(), 1000), Ok(ProcessStop::Graceful));
        assert!(process.wait().expect("sleep reaped").code().is_none());
    }

    #[test]
    fn flow_owned_process_stop_forces_a_term_refusing_process_without_orphaning_it() {
        let mut command = Command::new("sleep");
        command.arg("60");
        // SAFETY: this runs after fork in the child and changes only the
        // child's SIGTERM disposition before exec.
        unsafe {
            command.pre_exec(|| {
                libc::signal(libc::SIGTERM, libc::SIG_IGN);
                Ok(())
            });
        }
        let mut process = command.spawn().expect("term-refusing sleep starts");
        assert_eq!(stop_process(process.id(), 10), Ok(ProcessStop::Forced));
        assert!(process.wait().expect("sleep reaped").code().is_none());
    }

    #[test]
    fn an_exited_owned_process_is_never_replaced_by_a_new_matching_process() {
        let mut original = Command::new("sleep").arg("0.01").spawn().unwrap();
        // SAFETY: pidfd_open has no pointer arguments.
        let pidfd = unsafe { libc::syscall(libc::SYS_pidfd_open, original.id(), 0) as i32 };
        assert!(pidfd >= 0);
        original.wait().unwrap();
        assert!(pidfd_exited(pidfd, 1000));

        let mut replacement = Command::new("sleep").arg("60").spawn().unwrap();
        assert_eq!(
            stop_owned_process(pidfd, 10),
            Ok(ProcessStop::AlreadyExited)
        );
        assert!(replacement.try_wait().unwrap().is_none());
        replacement.kill().unwrap();
        replacement.wait().unwrap();
        // SAFETY: this test owns the pidfd.
        unsafe { libc::close(pidfd) };
    }

    #[test]
    fn an_exited_owned_process_does_not_stop_a_new_matching_foreground_process() {
        use std::os::unix::fs::PermissionsExt as _;

        let root = tempfile::tempdir().unwrap();
        let mut replacement = Command::new("sleep").arg("60").spawn().unwrap();

        let mut original = Command::new("sleep").arg("0.01").spawn().unwrap();
        let original_start = super::process_start(original.id()).unwrap();
        let original_executable =
            std::fs::canonicalize(format!("/proc/{}/exe", original.id())).unwrap();
        // SAFETY: pidfd_open has no pointer arguments.
        let pidfd = unsafe { libc::syscall(libc::SYS_pidfd_open, original.id(), 0) as i32 };
        assert!(pidfd >= 0);
        original.wait().unwrap();

        let herdr = root.path().join("herdr");
        std::fs::write(
            &herdr,
            format!(
                "#!/bin/sh\ncase \"$3 $4\" in\n  \"api snapshot\") printf '%s\\n' '{{\"result\":{{\"snapshot\":{{\"agents\":[],\"panes\":[{{\"pane_id\":\"w1:p2\",\"terminal_id\":\"term-current\"}}]}}}}}}' ;;\n  \"pane process-info\") printf '%s\\n' '{{\"result\":{{\"process_info\":{{\"pane_id\":\"w1:p2\",\"foreground_processes\":[{{\"pid\":{}}}]}}}}}}' ;;\n  \"pane close\") touch '{}' ;;\nesac\n",
                replacement.id(),
                root.path().join("closed").display()
            ),
        )
        .unwrap();
        let mut permissions = std::fs::metadata(&herdr).unwrap().permissions();
        permissions.set_mode(0o755);
        std::fs::set_permissions(&herdr, permissions).unwrap();
        let adapter = HerdrCli::at(herdr, root.path().join("flows"));
        adapter.flow_owned_claude_processes.lock().unwrap().insert(
            route().herdr_pane_id.clone(),
            super::OwnedClaudeProcess {
                process_id: original.id(),
                process_start: original_start,
                process_executable: original_executable.clone(),
                configured_executable: original_executable,
                session_id: "native-session".into(),
                pidfd,
            },
        );

        let node = FlowNode {
            flow_id: "owner".into(),
            session_id: "native-session".into(),
            harness_kind: HarnessKind::Claude,
            endpoint_selection: EndpointSelection::Unavailable,
            herdr_route_selection: HerdrRouteSelection::Available(route()),
            origin: Origin::Flow(FlowOrigin {
                flow_id: "owner".into(),
                session_id: "native-session".into(),
            }),
            flow_lifecycle: FlowLifecycle::Active,
        };
        assert_eq!(
            adapter.close(&node),
            signal_flow::PaneCloseOutcome::Refused(signal_flow::PaneCloseFailure::IdentityMismatch)
        );
        assert!(replacement.try_wait().unwrap().is_none());
        assert!(!root.path().join("closed").exists());
        replacement.kill().unwrap();
        replacement.wait().unwrap();
    }

    #[test]
    fn an_exited_owned_process_closes_only_a_positively_owned_bare_pane() {
        use std::os::unix::fs::PermissionsExt as _;

        for (name, panes, expected, closes) in [
            (
                "bare",
                "[{\"pane_id\":\"w1:p2\",\"terminal_id\":\"term-current\"}]",
                signal_flow::PaneCloseOutcome::Closed,
                true,
            ),
            (
                "absent",
                "[]",
                signal_flow::PaneCloseOutcome::Refused(
                    signal_flow::PaneCloseFailure::IdentityUnavailable,
                ),
                false,
            ),
        ] {
            let root = tempfile::tempdir().unwrap();
            let mut original = Command::new("sleep").arg("0.01").spawn().unwrap();
            let original_start = super::process_start(original.id()).unwrap();
            let original_executable =
                std::fs::canonicalize(format!("/proc/{}/exe", original.id())).unwrap();
            // SAFETY: pidfd_open has no pointer arguments.
            let pidfd = unsafe { libc::syscall(libc::SYS_pidfd_open, original.id(), 0) as i32 };
            assert!(pidfd >= 0);
            original.wait().unwrap();
            assert!(super::pidfd_exited(pidfd, 1000));

            let closed = root.path().join(format!("closed-{name}"));
            let herdr = root.path().join("herdr");
            let snapshot =
                r#"{"result":{"snapshot":{"agents":[],"panes":PANES}}}"#.replace("PANES", panes);
            let process_info =
                r#"{"result":{"process_info":{"pane_id":"w1:p2","foreground_processes":[]}}}"#;
            std::fs::write(
                &herdr,
                format!(
                    "#!/bin/sh\ncase \"$3 $4\" in\n  \"api snapshot\") printf '%s\\n' '{snapshot}' ;;\n  \"pane process-info\") printf '%s\\n' '{process_info}' ;;\n  \"pane close\") touch '{}' ;;\nesac\n",
                    closed.display()
                ),
            )
            .unwrap();
            let mut permissions = std::fs::metadata(&herdr).unwrap().permissions();
            permissions.set_mode(0o755);
            std::fs::set_permissions(&herdr, permissions).unwrap();
            let adapter = HerdrCli::at(herdr, root.path().join("flows"));
            adapter.flow_owned_claude_processes.lock().unwrap().insert(
                route().herdr_pane_id.clone(),
                super::OwnedClaudeProcess {
                    process_id: original.id(),
                    process_start: original_start,
                    process_executable: original_executable.clone(),
                    configured_executable: original_executable,
                    session_id: "native-session".into(),
                    pidfd,
                },
            );
            let node = FlowNode {
                flow_id: "owner".into(),
                session_id: "native-session".into(),
                harness_kind: HarnessKind::Claude,
                endpoint_selection: EndpointSelection::Unavailable,
                herdr_route_selection: HerdrRouteSelection::Available(route()),
                origin: Origin::Flow(FlowOrigin {
                    flow_id: "owner".into(),
                    session_id: "native-session".into(),
                }),
                flow_lifecycle: FlowLifecycle::Active,
            };
            assert_eq!(adapter.close(&node), expected, "{name}");
            assert_eq!(closed.exists(), closes, "{name}");
        }
    }

    #[test]
    fn an_invalid_bounded_stop_refuses_as_timed_out_without_signalling_or_closing() {
        use std::os::unix::fs::PermissionsExt as _;

        let root = tempfile::tempdir().unwrap();
        let program = root.path().join("claude");
        std::fs::write(&program, "#!/bin/sh\nsleep 60\n").unwrap();
        let mut permissions = std::fs::metadata(&program).unwrap().permissions();
        permissions.set_mode(0o755);
        std::fs::set_permissions(&program, permissions).unwrap();
        let mut process = Command::new(&program)
            .args(["--session-id", "native-session", "--model", "fixture-model"])
            .spawn()
            .unwrap();
        let process_start = super::process_start(process.id()).unwrap();
        let process_executable =
            std::fs::canonicalize(format!("/proc/{}/exe", process.id())).unwrap();
        let configured_executable = std::fs::canonicalize(&program).unwrap();
        // SAFETY: pidfd_open has no pointer arguments.
        let pidfd = unsafe { libc::syscall(libc::SYS_pidfd_open, process.id(), 0) as i32 };
        assert!(pidfd >= 0);

        let closed = root.path().join("closed");
        let herdr = root.path().join("herdr");
        std::fs::write(
            &herdr,
            format!(
                "#!/bin/sh\ncase \"$3 $4\" in\n  \"api snapshot\") printf '%s\\n' '{{\"result\":{{\"snapshot\":{{\"agents\":[],\"panes\":[{{\"pane_id\":\"w1:p2\",\"terminal_id\":\"term-current\"}}]}}}}}}' ;;\n  \"pane process-info\") printf '%s\\n' '{{\"result\":{{\"process_info\":{{\"pane_id\":\"w1:p2\",\"foreground_processes\":[{{\"pid\":{}}}]}}}}}}' ;;\n  \"pane close\") touch '{}' ;;\nesac\n",
                process.id(),
                closed.display()
            ),
        )
        .unwrap();
        let mut permissions = std::fs::metadata(&herdr).unwrap().permissions();
        permissions.set_mode(0o755);
        std::fs::set_permissions(&herdr, permissions).unwrap();
        let mut adapter = HerdrCli::at(herdr, root.path().join("flows"));
        adapter.timings.stop_wait = -1;
        adapter.flow_owned_claude_processes.lock().unwrap().insert(
            route().herdr_pane_id.clone(),
            super::OwnedClaudeProcess {
                process_id: process.id(),
                process_start,
                process_executable,
                configured_executable,
                session_id: "native-session".into(),
                pidfd,
            },
        );
        let node = FlowNode {
            flow_id: "owner".into(),
            session_id: "native-session".into(),
            harness_kind: HarnessKind::Claude,
            endpoint_selection: EndpointSelection::Unavailable,
            herdr_route_selection: HerdrRouteSelection::Available(route()),
            origin: Origin::Flow(FlowOrigin {
                flow_id: "owner".into(),
                session_id: "native-session".into(),
            }),
            flow_lifecycle: FlowLifecycle::Active,
        };
        assert_eq!(
            adapter.close(&node),
            signal_flow::PaneCloseOutcome::Refused(signal_flow::PaneCloseFailure::StopTimedOut)
        );
        assert!(process.try_wait().unwrap().is_none());
        assert!(!closed.exists());
        process.kill().unwrap();
        process.wait().unwrap();
    }

    #[test]
    fn a_reused_numeric_pid_with_another_start_identity_is_not_the_owned_process() {
        let executable = PathBuf::from("/fixture/claude");
        assert!(!super::retained_process_matches(
            100,
            &executable,
            Some(101),
            Some(executable.clone()),
        ));
        assert!(super::retained_process_matches(
            100,
            &executable,
            Some(100),
            Some(executable.clone()),
        ));
    }

    #[test]
    fn a_reused_pid_identity_refuses_without_signalling_or_closing_the_pane() {
        use std::os::unix::fs::PermissionsExt as _;

        let root = tempfile::tempdir().unwrap();
        let mut process = Command::new("sh")
            .args([
                "-c",
                "trap '' TERM; while :; do sleep 1; done",
                "claude",
                "--session-id",
                "native-session",
            ])
            .spawn()
            .unwrap();
        let actual_start = super::process_start(process.id()).unwrap();
        let process_executable =
            std::fs::canonicalize(format!("/proc/{}/exe", process.id())).unwrap();
        // SAFETY: pidfd_open has no pointer arguments. The retained record
        // owns this descriptor until the adapter is dropped.
        let pidfd = unsafe { libc::syscall(libc::SYS_pidfd_open, process.id(), 0) as i32 };
        assert!(pidfd >= 0);

        let closed = root.path().join("closed");
        let herdr = root.path().join("herdr");
        std::fs::write(
            &herdr,
            format!(
                "#!/bin/sh\ncase \"$3 $4\" in\n  \"api snapshot\") printf '%s\\n' '{{\"result\":{{\"snapshot\":{{\"agents\":[],\"panes\":[{{\"pane_id\":\"w1:p2\",\"terminal_id\":\"term-current\"}}]}}}}}}' ;;\n  \"pane close\") touch '{}' ;;\nesac\n",
                closed.display()
            ),
        )
        .unwrap();
        let mut permissions = std::fs::metadata(&herdr).unwrap().permissions();
        permissions.set_mode(0o755);
        std::fs::set_permissions(&herdr, permissions).unwrap();
        let adapter = HerdrCli::at(herdr, root.path().join("flows"));
        adapter.flow_owned_claude_processes.lock().unwrap().insert(
            route().herdr_pane_id.clone(),
            super::OwnedClaudeProcess {
                process_id: process.id(),
                process_start: actual_start + 1,
                process_executable: process_executable.clone(),
                configured_executable: process_executable,
                session_id: "native-session".into(),
                pidfd,
            },
        );
        let node = FlowNode {
            flow_id: "owner".into(),
            session_id: "native-session".into(),
            harness_kind: HarnessKind::Claude,
            endpoint_selection: EndpointSelection::Unavailable,
            herdr_route_selection: HerdrRouteSelection::Available(route()),
            origin: Origin::Flow(FlowOrigin {
                flow_id: "owner".into(),
                session_id: "native-session".into(),
            }),
            flow_lifecycle: FlowLifecycle::Active,
        };

        assert_eq!(
            adapter.close(&node),
            signal_flow::PaneCloseOutcome::Refused(signal_flow::PaneCloseFailure::IdentityMismatch)
        );
        assert!(process.try_wait().unwrap().is_none());
        assert!(!closed.exists());
        process.kill().unwrap();
        process.wait().unwrap();
    }

    #[test]
    fn a_lost_retained_process_identity_refuses_without_closing_the_pane() {
        use std::os::unix::fs::PermissionsExt as _;

        let root = tempfile::tempdir().unwrap();
        let closed = root.path().join("closed");
        let herdr = root.path().join("herdr");
        std::fs::write(
            &herdr,
            format!(
                "#!/bin/sh\ncase \"$3 $4\" in\n  \"api snapshot\") printf '%s\\n' '{{\"result\":{{\"snapshot\":{{\"agents\":[],\"panes\":[{{\"pane_id\":\"w1:p2\",\"terminal_id\":\"term-current\"}}]}}}}}}' ;;\n  \"pane close\") touch '{}' ;;\nesac\n",
                closed.display()
            ),
        )
        .unwrap();
        let mut permissions = std::fs::metadata(&herdr).unwrap().permissions();
        permissions.set_mode(0o755);
        std::fs::set_permissions(&herdr, permissions).unwrap();
        let adapter = HerdrCli::at(herdr, root.path().join("flows"));
        let node = FlowNode {
            flow_id: "owner".into(),
            session_id: "native-session".into(),
            harness_kind: HarnessKind::Claude,
            endpoint_selection: EndpointSelection::Unavailable,
            herdr_route_selection: HerdrRouteSelection::Available(route()),
            origin: Origin::Flow(FlowOrigin {
                flow_id: "owner".into(),
                session_id: "native-session".into(),
            }),
            flow_lifecycle: FlowLifecycle::Active,
        };

        assert_eq!(
            adapter.close(&node),
            signal_flow::PaneCloseOutcome::Refused(
                signal_flow::PaneCloseFailure::IdentityUnavailable
            )
        );
        assert!(!closed.exists());
    }

    #[test]
    fn a_stop_wait_outside_poll_range_is_refused_without_signalling() {
        let mut process = Command::new("sleep").arg("60").spawn().unwrap();
        assert_eq!(
            stop_process(process.id(), i32::MAX as u64 + 1),
            Err(ProcessStopFailure::TimedOut)
        );
        assert!(process.try_wait().unwrap().is_none());
        process.kill().unwrap();
        process.wait().unwrap();
    }
}
