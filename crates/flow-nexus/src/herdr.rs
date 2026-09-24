//! Herdr roster validation for durable Flow routes.

pub mod launch;

use crate::codex::{CodexEndpoint, CodexEndpoints};
use std::collections::BTreeSet;
use std::{fs, path::PathBuf, process::Command};

use signal_flow::{
    EndpointSelection, FlowNode, HarnessKind, HerdrRoute, HerdrRouteSelection, RouteReadiness,
};

/// Reads Herdr's documented session snapshot and validates one complete route.
pub trait ReadsHerdrRoster {
    fn route_is_available(&self, node: &FlowNode) -> bool;
}

/// The production Herdr roster reader.
pub struct HerdrCli {
    executable: PathBuf,
    flow_id_executable: PathBuf,
    flows_root: PathBuf,
    codex_endpoints: CodexEndpoints,
    claude_transcript_root: PathBuf,
    /// Native Claude skill catalogs, highest-precedence first.
    claude_skill_roots: Vec<PathBuf>,
}

impl Default for HerdrCli {
    fn default() -> Self {
        let home = std::env::var_os("HOME")
            .map(PathBuf::from)
            .unwrap_or_else(|| panic!("HOME must name the configured user root"));
        let codex_home = std::env::var_os("CODEX_HOME")
            .map(PathBuf::from)
            .unwrap_or_else(|| home.join(".codex"));
        let claude_home = std::env::var_os("CLAUDE_CONFIG_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(|| home.join(".claude"));
        let workspace_root = std::env::var_os("FLOW_SOURCE_ROOT")
            .map(PathBuf::from)
            .unwrap_or_else(|| panic!("FLOW_SOURCE_ROOT must name the configured workspace root"));
        let flows_root = workspace_root.join("flows");
        let mut claude_skill_roots = Vec::new();
        if let Some(enterprise) = std::env::var_os("CLAUDE_ENTERPRISE_SKILLS_DIR") {
            claude_skill_roots.push(PathBuf::from(enterprise));
        }
        claude_skill_roots.push(claude_home.join("skills"));
        claude_skill_roots.push(workspace_root.join(".claude/skills"));
        Self {
            executable: PathBuf::from("herdr"),
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
                    model_names: BTreeSet::from(["gpt-5.6-terra".into()]),
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
            claude_skill_roots,
        }
    }
}

/// Verifies the stable claim minted by `flow-id` for a harness session.
pub trait VerifiesFlowClaim {
    fn identity_is_claimed(&self, node: &FlowNode) -> bool;
}

impl ReadsHerdrRoster for HerdrCli {
    fn route_is_available(&self, node: &FlowNode) -> bool {
        let HerdrRouteSelection::Available(route) = &node.herdr_route_selection else {
            return false;
        };
        if !self.identity_is_claimed(node) {
            return false;
        }
        self.snapshot(route).is_some_and(|snapshot| {
            HerdrCli::snapshot_has_route(&snapshot, route, &node.harness_kind)
        })
    }
}

impl HerdrCli {
    pub fn with_codex_endpoints(mut self, codex_endpoints: CodexEndpoints) -> Self {
        self.codex_endpoints = codex_endpoints;
        self
    }

    #[cfg(test)]
    pub(crate) fn at(executable: PathBuf, flows_root: PathBuf) -> Self {
        let fixture_root = flows_root
            .parent()
            .expect("fixture flows root has a parent")
            .join("native-transcripts");
        Self {
            executable,
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
            claude_skill_roots: vec![fixture_root.join("claude-skills")],
        }
    }

    pub fn validate_registration(&self, node: &FlowNode) -> bool {
        let HerdrRouteSelection::Available(route) = &node.herdr_route_selection else {
            return false;
        };
        self.identity_is_claimed(node)
            && self.snapshot(route).is_some_and(|snapshot| {
                HerdrCli::snapshot_has_binding(&snapshot, route, &node.harness_kind)
            })
    }

    pub fn refresh_route(&self, mut node: FlowNode) -> FlowNode {
        let had_persisted_route = matches!(
            node.herdr_route_selection,
            HerdrRouteSelection::Available(_)
        );
        if had_persisted_route && !self.route_is_available(&node) {
            node.herdr_route_selection = HerdrRouteSelection::Unavailable;
            if let EndpointSelection::Available(endpoint) = &mut node.endpoint_selection {
                endpoint.route_readiness = RouteReadiness::Parked;
            }
        }
        node
    }

    fn snapshot(&self, route: &HerdrRoute) -> Option<serde_json::Value> {
        let output = Command::new(&self.executable)
            .args([
                "--session",
                route.herdr_session_name.as_str(),
                "api",
                "snapshot",
            ])
            .output()
            .ok()?;
        output
            .status
            .success()
            .then(|| serde_json::from_slice(&output.stdout).ok())
            .flatten()
    }

    fn snapshot_has_route(
        snapshot: &serde_json::Value,
        route: &HerdrRoute,
        harness_kind: &HarnessKind,
    ) -> bool {
        snapshot
            .pointer("/result/snapshot/agents")
            .and_then(serde_json::Value::as_array)
            .is_some_and(|agents| {
                agents.iter().any(|agent| {
                    HerdrCli::agent_matches_binding(agent, route, harness_kind)
                        && matches!(
                            agent
                                .get("agent_status")
                                .and_then(serde_json::Value::as_str),
                            Some("idle" | "working")
                        )
                        && agent
                            .get("interactive_ready")
                            .and_then(serde_json::Value::as_bool)
                            == Some(true)
                })
            })
    }

    fn snapshot_has_binding(
        snapshot: &serde_json::Value,
        route: &HerdrRoute,
        harness_kind: &HarnessKind,
    ) -> bool {
        snapshot
            .pointer("/result/snapshot/agents")
            .and_then(serde_json::Value::as_array)
            .is_some_and(|agents| {
                agents
                    .iter()
                    .any(|agent| HerdrCli::agent_matches_binding(agent, route, harness_kind))
            })
    }

    fn agent_matches_binding(
        agent: &serde_json::Value,
        route: &HerdrRoute,
        harness_kind: &HarnessKind,
    ) -> bool {
        let expected_harness = match harness_kind {
            HarnessKind::Codex => "codex",
            HarnessKind::Claude => "claude",
        };
        agent.get("name").and_then(serde_json::Value::as_str)
            == Some(route.herdr_agent_name.as_str())
            && agent.get("pane_id").and_then(serde_json::Value::as_str)
                == Some(route.herdr_pane_id.as_str())
            && agent.get("terminal_id").and_then(serde_json::Value::as_str)
                == Some(route.herdr_terminal_id.as_str())
            && agent.get("agent").and_then(serde_json::Value::as_str) == Some(expected_harness)
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
    use signal_flow::{
        EndpointSelection, FlowLifecycle, FlowNode, HarnessKind, HerdrRoute, HerdrRouteSelection,
        OriginClue,
    };
    use std::path::PathBuf;

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
        assert!(HerdrCli::snapshot_has_route(
            &snapshot,
            &route(),
            &HarnessKind::Claude
        ));
    }

    #[test]
    fn working_interactive_agent_binding_remains_available_to_message() {
        let snapshot = serde_json::json!({"result":{"snapshot":{"agents":[{
            "agent":"claude", "agent_status":"working", "interactive_ready":true,
            "name":"recipient", "pane_id":"w1:p2", "terminal_id":"term-current"
        }]}}});
        assert!(HerdrCli::snapshot_has_binding(
            &snapshot,
            &route(),
            &HarnessKind::Claude
        ));
        assert!(HerdrCli::snapshot_has_route(
            &snapshot,
            &route(),
            &HarnessKind::Claude
        ));
    }

    #[test]
    fn replaced_waiting_or_noninteractive_agent_binding_is_unavailable() {
        for snapshot in [
            serde_json::json!({"result":{"snapshot":{"agents":[{
                "agent":"claude", "agent_status":"idle", "interactive_ready":true,
                "name":"recipient", "pane_id":"w1:p2", "terminal_id":"term-replaced"
            }]}}}),
            serde_json::json!({"result":{"snapshot":{"agents":[{
                "agent":"claude", "agent_status":"idle",
                "name":"recipient", "pane_id":"w1:p2", "terminal_id":"term-current"
            }]}}}),
            serde_json::json!({"result":{"snapshot":{"agents":[{
                "agent":"claude", "agent_status":"waiting", "interactive_ready":true,
                "name":"recipient", "pane_id":"w1:p2", "terminal_id":"term-current"
            }]}}}),
        ] {
            assert!(!HerdrCli::snapshot_has_route(
                &snapshot,
                &route(),
                &HarnessKind::Claude
            ));
        }
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
            origin_clue: OriginClue {
                flow_id: "1ac573".into(),
                session_id: "f52d95a1-857f-49ab-8c6f-3aa0a9db826b".into(),
                turn_id: "unavailable".into(),
            },
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
            origin_clue: OriginClue {
                flow_id: "1ac573".into(),
                session_id: "f52d95a1-857f-49ab-8c6f-3aa0a9db826b".into(),
                turn_id: "unavailable".into(),
            },
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
