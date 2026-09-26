//! The Herdr operations Flow's pane writer is made of. Each is one Herdr
//! call; the sequence, the lease and the grading belong to the writer.
//!
//! Witnessed of Herdr 0.8.2 (2026-09-25, disposable session and panes):
//! `agent prompt` wraps its text in bracketed paste (`ESC[200~` … `ESC[201~`)
//! exactly when the pane's program enabled mode 2004, and sends the submitting
//! CR as a separate write after it. Both Claude Code and Codex CLI enable mode
//! 2004. Herdr does not strip an embedded `ESC[201~` or CR; the body refusal
//! does. A prompt to a working Codex 0.153 lands as a steer "submitted after
//! next tool call" and is taken in the same turn.

use super::{HerdrCli, PromptReply, VerifiesFlowClaim};
use signal_flow::{AgentState, FlowNode, HarnessKind, HerdrRoute, HerdrRouteSelection};
use std::process::Command;

/// What a fresh Herdr snapshot shows of a flow's bound agent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PaneAgent {
    /// The binding is present. `ready` is false when Herdr reports the agent
    /// not interactively ready, which no write may go into.
    Present {
        agent_state: AgentState,
        ready: bool,
    },
    /// A readable snapshot shows no such binding, or the flow has no route.
    Absent,
    /// Herdr could not be read, or the flow's claim is not held.
    Unreadable,
}

/// What became of one text placed into a pane.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Placement {
    /// Herdr refused before any input reached the pane.
    Refused,
    /// Herdr accepted the text for the exact pane; `observed` when the
    /// recipient was then seen reacting on it.
    Placed { observed: bool },
    /// Input may have reached the pane, but what followed is not known.
    Uncertain,
}

/// The pane operations Flow's writer uses, each one Herdr call.
pub trait WritesPane {
    fn pane_agent(&self, node: &FlowNode) -> PaneAgent;
    /// Whether the composer holds no text of its own: `None` when the screen
    /// could not be read or shows no composer.
    fn composer_is_blank(&self, route: &HerdrRoute, harness_kind: &HarnessKind) -> Option<bool>;
    /// Presses keys in the pane, in order. False when Herdr refused them.
    fn press(&self, route: &HerdrRoute, keys: &[String]) -> bool;
    /// Types `text` into the pane through `agent prompt`, which submits it.
    /// With `observe`, waits for the recipient's reaction as well.
    fn place(&self, route: &HerdrRoute, text: &str, observe: bool) -> Placement;
    /// Waits, boundedly, for a working agent to leave Working.
    fn left_working(&self, route: &HerdrRoute) -> bool;
}

impl HerdrCli {
    /// How long an interrupt is given to show: an interrupted turn stops
    /// within a second or two.
    const INTERRUPT_WAIT_MILLISECONDS: &'static str = "5000";
    /// The composer is at the bottom of the screen.
    const COMPOSER_LINES: &'static str = "12";
}

impl WritesPane for HerdrCli {
    fn pane_agent(&self, node: &FlowNode) -> PaneAgent {
        let HerdrRouteSelection::Available(route) = &node.herdr_route_selection else {
            return PaneAgent::Absent;
        };
        if !self.identity_is_claimed(node) {
            return PaneAgent::Unreadable;
        }
        let Some(snapshot) = self.snapshot(route) else {
            return PaneAgent::Unreadable;
        };
        let Some(agents) = snapshot
            .pointer("/result/snapshot/agents")
            .and_then(serde_json::Value::as_array)
        else {
            return PaneAgent::Unreadable;
        };
        let Some(agent) = agents
            .iter()
            .find(|agent| HerdrCli::agent_matches_binding(agent, route, &node.harness_kind))
        else {
            // The pane ID under another binding is not this flow's pane
            // gone: its fate is not known, as `pane_presence` holds too.
            let pane_reused = agents.iter().any(|agent| {
                agent.get("pane_id").and_then(serde_json::Value::as_str)
                    == Some(route.herdr_pane_id.as_str())
            });
            return if pane_reused {
                PaneAgent::Unreadable
            } else {
                PaneAgent::Absent
            };
        };
        PaneAgent::Present {
            agent_state: HerdrCli::agent_state_of(
                agent
                    .get("agent_status")
                    .and_then(serde_json::Value::as_str),
            ),
            ready: HerdrCli::agent_readiness_permits_prompt(agent),
        }
    }

    fn composer_is_blank(&self, route: &HerdrRoute, harness_kind: &HarnessKind) -> Option<bool> {
        let output = Command::new(&self.executable)
            .args([
                "--session",
                route.herdr_session_name.as_str(),
                "agent",
                "read",
                route.herdr_pane_id.as_str(),
                "--source",
                "visible",
                "--lines",
                Self::COMPOSER_LINES,
                "--format",
                "ansi",
            ])
            .output()
            .ok()?;
        if !output.status.success() {
            return None;
        }
        Composer::of(harness_kind).is_blank(&String::from_utf8_lossy(&output.stdout))
    }

    fn press(&self, route: &HerdrRoute, keys: &[String]) -> bool {
        if keys.is_empty() {
            return true;
        }
        let mut command = Command::new(&self.executable);
        command.args([
            "--session",
            route.herdr_session_name.as_str(),
            "pane",
            "send-keys",
            route.herdr_pane_id.as_str(),
        ]);
        command.args(keys);
        command.status().is_ok_and(|status| status.success())
    }

    fn place(&self, route: &HerdrRoute, text: &str, observe: bool) -> Placement {
        let mut command = Command::new(&self.executable);
        command.args([
            "--session",
            route.herdr_session_name.as_str(),
            "agent",
            "prompt",
            route.herdr_pane_id.as_str(),
            text,
        ]);
        if observe {
            command.args(PromptReply::OBSERVATION);
        }
        let Ok(output) = command.output() else {
            return Placement::Refused;
        };
        let reply = PromptReply { output };
        if reply.refused_before_input() {
            return Placement::Refused;
        }
        if !reply.prompted_pane(&route.herdr_pane_id) {
            return Placement::Uncertain;
        }
        Placement::Placed { observed: observe }
    }

    fn left_working(&self, route: &HerdrRoute) -> bool {
        Command::new(&self.executable)
            .args([
                "--session",
                route.herdr_session_name.as_str(),
                "agent",
                "wait",
                route.herdr_pane_id.as_str(),
                "--until",
                "idle",
                "--until",
                "done",
                "--until",
                "blocked",
                "--timeout",
                Self::INTERRUPT_WAIT_MILLISECONDS,
            ])
            .status()
            .is_ok_and(|status| status.success())
    }
}

impl HerdrCli {
    /// Herdr's agent status as Flow's AgentState; a word Herdr adds later is
    /// Unknown until it is named.
    pub fn agent_state_of(status: Option<&str>) -> AgentState {
        match status {
            Some("idle") => AgentState::Idle,
            Some("working") => AgentState::Working,
            Some("blocked") => AgentState::Blocked,
            Some("done") => AgentState::Done,
            _ => AgentState::Unknown,
        }
    }
}

/// A harness's composer as its screen shows it. The prompt glyph is harness
/// knowledge of the version witnessed, not a setup value, so it lives here.
struct Composer {
    glyph: char,
}

impl Composer {
    fn of(harness_kind: &HarnessKind) -> Self {
        match harness_kind {
            HarnessKind::Claude => Self { glyph: '❯' },
            HarnessKind::Codex => Self { glyph: '›' },
        }
    }

    /// The composer is the last line opening with the glyph. It is blank
    /// when nothing follows the glyph but a placeholder, which both harnesses
    /// render dim (SGR 2); text a person typed is not dim.
    fn is_blank(&self, screen: &str) -> Option<bool> {
        let line = screen
            .lines()
            .map(StyledLine::from)
            .rfind(|line| line.visible().trim_start().starts_with(self.glyph))?;
        Some(line.blank_after(self.glyph))
    }
}

/// One screen line as characters, each marked dim or not.
struct StyledLine {
    characters: Vec<(char, bool)>,
}

impl From<&str> for StyledLine {
    fn from(line: &str) -> Self {
        let mut characters = Vec::new();
        let mut dim = false;
        let mut rest = line.chars().peekable();
        while let Some(character) = rest.next() {
            if character != '\u{1b}' {
                characters.push((character, dim));
                continue;
            }
            if rest.peek() != Some(&'[') {
                continue;
            }
            rest.next();
            let mut parameters = String::new();
            let mut final_byte = None;
            for character in rest.by_ref() {
                if ('\u{40}'..='\u{7e}').contains(&character) {
                    final_byte = Some(character);
                    break;
                }
                parameters.push(character);
            }
            if final_byte != Some('m') {
                continue;
            }
            for parameter in parameters.split(';') {
                match parameter {
                    "" | "0" | "22" => dim = false,
                    "2" => dim = true,
                    _ => {}
                }
            }
        }
        Self { characters }
    }
}

impl StyledLine {
    fn visible(&self) -> String {
        self.characters
            .iter()
            .map(|(character, _)| *character)
            .collect()
    }

    fn blank_after(&self, glyph: char) -> bool {
        self.characters
            .iter()
            .skip_while(|(character, _)| *character != glyph)
            .skip(1)
            .all(|(character, dim)| *dim || character.is_whitespace())
    }
}

#[cfg(test)]
mod tests {
    use super::Composer;
    use signal_flow::HarnessKind;

    /// Codex 0.153.4 as `agent read --format ansi` showed it, 2026-09-25.
    const CODEX_PLACEHOLDER: &str =
        "\u{1b}[0m\u{1b}[1m›\u{1b}[0m \u{1b}[0m\u{1b}[2mAsk Codex to do anything\u{1b}[0m\r";
    const CODEX_DRAFT: &str = "\u{1b}[0m\u{1b}[1m›\u{1b}[0m draft words\r";

    #[test]
    fn a_dim_placeholder_is_a_blank_composer_and_typed_text_is_not() {
        let codex = Composer::of(&HarnessKind::Codex);
        let screen = |composer: &str| {
            format!(
                "› Run the shell command\n\n• Working\n{composer}\n\n  gpt-6-astra low · /tmp\n"
            )
        };
        assert_eq!(codex.is_blank(&screen(CODEX_PLACEHOLDER)), Some(true));
        assert_eq!(codex.is_blank(&screen(CODEX_DRAFT)), Some(false));
        assert_eq!(codex.is_blank(&screen("\u{1b}[1m›\u{1b}[0m ")), Some(true));
        assert_eq!(codex.is_blank("no composer on this screen\n"), None);
    }

    #[test]
    fn a_claude_composer_reads_by_its_own_glyph() {
        let claude = Composer::of(&HarnessKind::Claude);
        assert_eq!(claude.is_blank("───\n❯ \n───\n"), Some(true));
        assert_eq!(claude.is_blank("───\n❯ half a thought\n───\n"), Some(false));
        assert_eq!(claude.is_blank(CODEX_PLACEHOLDER), None);
    }
}
