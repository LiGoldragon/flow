//! Herdr roster validation for durable Flow routes.

use crate::claude::ProjectsClaudeReadiness;
use crate::codex::SelectsCodexEndpoint;
pub mod launch;
pub mod pane;
pub mod reservation;

use crate::codex::{CodexEndpoint, CodexEndpoints};
use std::collections::BTreeSet;
use std::{
    fs,
    path::PathBuf,
    process::{Command, Output},
};

use signal_flow::{
    EndpointSelection, FlowNode, HarnessKind, HerdrRoute, HerdrRouteSelection, RouteReadiness,
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

/// Closes one Herdr pane whose recorded binding a fresh snapshot shows. Every write into a pane is the pane
/// writer's (see `pane` and `crate::delivery`).
pub trait OperatesHerdrPane {
    fn close(&self, node: &FlowNode) -> bool;
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
    /// The binary Herdr servers run, when it is not the executable this
    /// Nexus runs Herdr by, as resolved through `PATH` at each reading.
    server_binary: Option<PathBuf>,
    flow_id_executable: PathBuf,
    flows_root: PathBuf,
    codex_endpoints: CodexEndpoints,
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
}

impl Default for HerdrCli {
    fn default() -> Self {
        use crate::store::{LaysOutDefaults, ReadsAnchors};
        let defaults = crate::store::DefaultConfiguration::from_environment();
        let home = defaults.home.clone();
        // The Codex endpoints here are placeholders: the Nexus replaces them
        // with the configured ones (`with_codex_endpoints`).
        let codex_home = home.join(".codex");
        let claude_home = std::env::var_os("CLAUDE_CONFIG_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|| home.join(".claude"));
        let workspace_root = defaults.source_root();
        let flows_root = workspace_root.join("flows");
        Self {
            executable: PathBuf::from("herdr"),
            server_binary: None,
            flow_id_executable: PathBuf::from("flow-id"),
            flows_root: flows_root.clone(),
            codex_endpoints: CodexEndpoints {
                stable: CodexEndpoint {
                    client_path: PathBuf::from("/fixture/codex"),
                    home: codex_home.clone(),
                    socket: home
                        .join(".codex/app-server-control/app-server-control.sock")
                        .to_string_lossy()
                        .into_owned(),
                    transcript_root: codex_home.join("sessions"),
                    model_names: BTreeSet::from(["gpt-5.6-sol".into()]),
                },
                next: CodexEndpoint {
                    client_path: PathBuf::from("/fixture/codex-next"),
                    home: home.join(".codex-next"),
                    socket: home
                        .join(".codex-next/app-server-control/app-server-control.sock")
                        .to_string_lossy()
                        .into_owned(),
                    transcript_root: home.join(".codex-next/sessions"),
                    model_names: BTreeSet::from([
                        "gpt-6-astra".into(),
                        "gpt-6-sol".into(),
                        "gpt-6-luna".into(),
                    ]),
                },
                timeout: std::time::Duration::from_secs(10),
                workspace_root: workspace_root.clone(),
            },
            claude_transcript_root: claude_home.join("projects"),
            claude_daemon: crate::claude::ClaudeDaemon::under(&claude_home),
            harness_hook: std::env::current_exe()
                .ok()
                .and_then(|path| path.parent().map(|directory| directory.join("flow-hook")))
                .unwrap_or_else(|| PathBuf::from("flow-hook")),
            ordinary_socket: defaults.ordinary_socket_path(),
            held_system_prompts: Default::default(),
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
    fn close(&self, node: &FlowNode) -> bool {
        let HerdrRouteSelection::Available(route) = &node.herdr_route_selection else {
            return false;
        };
        // A pane is closed when Herdr still shows the recorded binding there,
        // whatever its readiness and whether its FlowId claim was given back:
        // closing needs the pane, not a route a prompt could take.
        if !matches!(
            self.pane_presence(node),
            PanePresence::Present | PanePresence::Bare
        ) {
            return false;
        }
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
    const OBSERVATION: [&'static str; 11] = [
        "--wait",
        "--until",
        "working",
        "--until",
        "idle",
        "--until",
        "done",
        "--until",
        "blocked",
        "--timeout",
        "10000",
    ];
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
    fn with_codex_endpoints(self, codex_endpoints: CodexEndpoints) -> Self;
    /// The ordinary socket this Nexus serves, as its store configures it.
    fn with_ordinary_socket(self, ordinary_socket: &std::path::Path) -> Self;
    /// Re-roots the flows directory on the configured source root.
    fn with_source_root(self, source_root: &std::path::Path) -> Self;
    #[cfg(test)]
    fn at(executable: PathBuf, flows_root: PathBuf) -> Self;
}

impl ConfiguresHerdrCli for HerdrCli {
    fn with_codex_endpoints(mut self, codex_endpoints: CodexEndpoints) -> Self {
        self.codex_endpoints = codex_endpoints;
        self
    }

    fn with_ordinary_socket(mut self, ordinary_socket: &std::path::Path) -> Self {
        self.ordinary_socket = ordinary_socket.to_path_buf();
        self
    }

    fn with_source_root(mut self, source_root: &std::path::Path) -> Self {
        self.flows_root = source_root.join("flows");
        self
    }

    #[cfg(test)]
    fn at(executable: PathBuf, flows_root: PathBuf) -> Self {
        let fixture_root = flows_root
            .parent()
            .expect("fixture flows root has a parent")
            .join("native-transcripts");
        Self {
            executable,
            // The test process stands as the fixture Herdr server.
            server_binary: std::env::current_exe()
                .ok()
                .and_then(|binary| fs::canonicalize(binary).ok()),
            flow_id_executable: fixture_root.join("flow-id"),
            flows_root: flows_root.clone(),
            codex_endpoints: CodexEndpoints {
                stable: CodexEndpoint {
                    client_path: PathBuf::from("/fixture/codex"),
                    home: fixture_root.join("codex-home"),
                    socket: "/tmp/stable-codex.sock".into(),
                    transcript_root: fixture_root.join("codex"),
                    model_names: BTreeSet::from(["model-current".into(), "fixture-model".into()]),
                },
                next: CodexEndpoint {
                    client_path: PathBuf::from("/fixture/codex-next"),
                    home: fixture_root.join("codex-next-home"),
                    socket: "/tmp/next-codex.sock".into(),
                    transcript_root: fixture_root.join("codex-next"),
                    model_names: BTreeSet::from(["gpt-6-sol".into(), "gpt-6-luna".into()]),
                },
                timeout: std::time::Duration::from_millis(100),
                workspace_root: flows_root
                    .parent()
                    .expect("fixture flows root has a parent")
                    .to_path_buf(),
            },
            claude_transcript_root: fixture_root.join("claude"),
            claude_daemon: crate::claude::ClaudeDaemon::under(&fixture_root.join("claude-home")),
            harness_hook: PathBuf::from("/fixture/bin/flow-hook"),
            ordinary_socket: PathBuf::from("/fixture/run/flow-next/flow/flow.sock"),
            held_system_prompts: Default::default(),
        }
    }
}

pub trait ReadsHerdrPanes {
    fn validate_registration(&self, node: &FlowNode) -> bool;
    /// Whether the Flow's recorded pane is still in Herdr. An unreadable
    /// Herdr is `Unknown`, never `Absent`.
    fn pane_presence(&self, node: &FlowNode) -> PanePresence;
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
    /// What a fresh snapshot says of whether the pane Flow recorded for
    /// this flow runs the flow's own native session now, under its FlowId
    /// claim.
    fn own_session(&self, node: &FlowNode) -> OwnSession;
}

/// What a fresh Herdr snapshot says of whether the pane Flow recorded for a
/// flow runs the flow's own native session.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OwnSession {
    /// The recorded pane shows the flow's harness running its session: its
    /// agent reports the session, or, for Codex, reports none.
    Runs,
    /// Flow holds no pane for the flow, or no native session.
    NoRecordedPane,
    /// The flow's FlowId claim is not held.
    Unclaimed,
    /// Herdr could not be read.
    HerdrUnreadable,
    /// The snapshot shows no agent of the flow's harness at the recorded
    /// pane and terminal: the pane is gone.
    PaneGone,
    /// The recorded pane's agent reports another native session.
    OtherSession,
    /// The recorded pane's Claude agent reports no native session.
    SessionUnreported,
}

impl ReadsHerdrPanes for HerdrCli {
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

    fn own_session(&self, node: &FlowNode) -> OwnSession {
        let HerdrRouteSelection::Available(route) = &node.herdr_route_selection else {
            return OwnSession::NoRecordedPane;
        };
        if node.session_id.is_empty() {
            return OwnSession::NoRecordedPane;
        }
        if !self.identity_is_claimed(node) {
            return OwnSession::Unclaimed;
        }
        let Some(agents) = self
            .snapshot(route)
            .as_ref()
            .and_then(|snapshot| snapshot.pointer("/result/snapshot/agents"))
            .and_then(serde_json::Value::as_array)
            .cloned()
        else {
            return OwnSession::HerdrUnreadable;
        };
        let Some(agent) = agents
            .iter()
            .find(|agent| agent.matches_binding(route, &node.harness_kind))
        else {
            return OwnSession::PaneGone;
        };
        match agent
            .pointer("/agent_session/value")
            .and_then(serde_json::Value::as_str)
        {
            Some(session) if session == node.session_id => OwnSession::Runs,
            Some(_) => OwnSession::OtherSession,
            // A remote-resumed Codex TUI reports none: its claimed thread and
            // recognized pane are the proof.
            None if node.harness_kind == HarnessKind::Codex => OwnSession::Runs,
            None => OwnSession::SessionUnreported,
        }
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

    fn herdr_binary(&self) -> Option<PathBuf> {
        if let Some(binary) = &self.server_binary {
            return Some(binary.clone());
        }
        if self.executable.components().count() > 1 {
            return fs::canonicalize(&self.executable).ok();
        }
        std::env::split_paths(&std::env::var_os("PATH")?)
            .map(|directory| directory.join(&self.executable))
            .find(|candidate| candidate.is_file())
            .and_then(|found| fs::canonicalize(found).ok())
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
    use super::{HerdrCli, VerifiesFlowClaim};
    use crate::herdr::ConfiguresHerdrCli;
    use crate::herdr::ReadsHerdrPanes;
    use crate::herdr::ReadsHerdrSnapshot;
    use crate::herdr::pane::ReadsAgentStatus;
    use signal_flow::{
        EndpointSelection, FlowLifecycle, FlowNode, FlowOrigin, HarnessKind, HerdrRoute,
        HerdrRouteSelection, Origin,
    };
    use std::path::PathBuf;

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
}
