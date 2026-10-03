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
use crate::herdr::ReadsHerdrAgent;
use crate::herdr::ReadsHerdrPanes;
use crate::herdr::ReadsPromptReply;
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
    /// The composer is read this many times, each `SUBMISSION_SPACING` after
    /// the last: long enough for Herdr's CR, sent 300 ms after the text, and
    /// a harness's render of a large paste.
    const SUBMISSION_READS: usize = 12;
    const SUBMISSION_SPACING: std::time::Duration = std::time::Duration::from_millis(350);
    /// Each press is given `INTERRUPT_WAIT_MILLISECONDS` to show.
    const INTERRUPT_PRESSES: usize = 3;
    /// How long each interrupt press is given to show: an interrupted turn
    /// stops within a second or two.
    const INTERRUPT_WAIT_MILLISECONDS: &'static str = "3000";
    /// The composer is at the bottom of the screen.
    const COMPOSER_LINES: &'static str = "12";

    fn pane_agent(&self, node: &FlowNode) -> PaneAgent;
    /// Whether the composer holds no text of its own: `None` when the screen
    /// could not be read or shows no composer.
    /// The text the composer holds, empty when it holds none of its own:
    /// `None` when the screen could not be read or shows no composer. Only
    /// the composer's first line is read.
    fn composer_text(&self, route: &HerdrRoute, harness_kind: &HarnessKind) -> Option<String>;
    /// Whether the composer holds no text of its own.
    fn composer_is_blank(&self, route: &HerdrRoute, harness_kind: &HarnessKind) -> Option<bool> {
        self.composer_text(route, harness_kind)
            .map(|text| text.is_empty())
    }
    /// Presses keys in the pane, in order. False when Herdr refused them.
    fn press(&self, route: &HerdrRoute, keys: &[String]) -> bool;
    /// Types `text` into the pane through `agent prompt`, which submits it.
    /// With `observe`, waits for the recipient's reaction as well.
    fn place(&self, route: &HerdrRoute, text: &str, observe: bool) -> Placement;
    /// Waits, boundedly, for a working agent to leave Working.
    fn left_working(&self, route: &HerdrRoute) -> bool;

    /// Whether the text just placed is seen to leave the composer: the
    /// submission itself. `agent prompt` sends its submitting CR as a
    /// separate write 300 ms after the text, and its `agent_prompted`
    /// answers any lifecycle change, which need not be this text's.
    ///
    /// Herdr offers no event for a composer emptying, so the composer is
    /// read again, a bounded number of times, until it is seen blank: an
    /// exception to subscribing, taken because there is nothing to
    /// subscribe to.
    fn submission(&self, route: &HerdrRoute, harness_kind: &HarnessKind) -> Submission {
        let mut submission = Submission::Unreadable;
        for _ in 0..Self::SUBMISSION_READS {
            // The first read waits too, past Herdr's delayed CR.
            std::thread::sleep(Self::SUBMISSION_SPACING);
            match self.composer_is_blank(route, harness_kind) {
                Some(true) => return Submission::Seen,
                Some(false) => submission = Submission::Unseen,
                None => {}
            }
        }
        submission
    }

    /// Presses the composer's submit key once.
    fn submit(&self, route: &HerdrRoute, harness_kind: &HarnessKind) -> bool {
        self.press(route, &Composer::of(harness_kind).submit_keys())
    }

    /// Takes back the letter an interrupt put back into the composer.
    ///
    /// Witnessed of Claude Code 2.1.280 (Haiku 4.5, e167d8 sandbox runs
    /// fms-9a3e2b and fms-d70a61, 2026-09-26): a HardAbrupt's `esc esc`
    /// pressed before the turn's first response cancels that turn and puts
    /// its prompt, the letter just Presented, back into the composer; with
    /// vim editing the second `esc` leaves the composer in NORMAL mode,
    /// where `ctrl+u` stops short of the cursor's own character. One
    /// `ctrl+c` empties a composer holding text in either mode; it is
    /// pressed only while the composer is seen holding a letter, since a
    /// second one into an empty composer would quit Claude.
    fn retract(&self, route: &HerdrRoute, harness_kind: &HarnessKind) -> bool {
        self.press(route, &Composer::of(harness_kind).retract_keys())
    }

    /// Takes back a text of `lines` lines the composer still holds: a kill
    /// to the line's start, then a join onto the line above, for each line.
    fn take_back(&self, route: &HerdrRoute, harness_kind: &HarnessKind, lines: usize) -> bool {
        self.press(route, &Composer::of(harness_kind).take_back_keys(lines))
    }

    /// Presses the interrupt keys into a working agent until it is seen
    /// leaving Working, at most `INTERRUPT_PRESSES` times.
    ///
    /// Witnessed of Claude Code 2.1.280 (Haiku 4.5, e167d8 sandbox and a
    /// disposable pane, 2026-09-26): `esc esc` pressed as a turn begins is
    /// ignored and the agent keeps working through a 90 s command, while the
    /// same keys pressed again a moment later stop it. The keys go only into
    /// an agent still seen Working, never into a resting one.
    fn interrupt(&self, route: &HerdrRoute, keys: &[String]) -> Interruption {
        for press in 0..Self::INTERRUPT_PRESSES {
            if !self.press(route, keys) {
                return if press == 0 {
                    Interruption::Refused
                } else {
                    Interruption::Unobserved
                };
            }
            if self.left_working(route) {
                return Interruption::Observed;
            }
        }
        Interruption::Unobserved
    }
}

/// What became of an interrupt pressed into a working agent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Interruption {
    /// Herdr refused the first keys: nothing reached the pane.
    Refused,
    /// The agent was seen leaving Working.
    Observed,
    /// Keys reached the pane, and the agent was still Working after them.
    Unobserved,
}

/// Whether a placed text was seen to leave the composer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Submission {
    /// The composer was seen blank: the text was submitted.
    Seen,
    /// The composer was seen still holding text.
    Unseen,
    /// The composer could not be read.
    Unreadable,
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
            .find(|agent| agent.matches_binding(route, &node.harness_kind))
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
            agent_state: agent
                .get("agent_status")
                .and_then(serde_json::Value::as_str)
                .agent_state(),
            ready: agent.permits_prompt(),
        }
    }

    fn composer_text(&self, route: &HerdrRoute, harness_kind: &HarnessKind) -> Option<String> {
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
        Composer::of(harness_kind).text(&String::from_utf8_lossy(&output.stdout))
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
        // Herdr's reply is read for its status only; it never reaches the
        // Nexus's own output.
        command.output().is_ok_and(|output| output.status.success())
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
        if !reply.prompted_binding(route) {
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
            .output()
            .is_ok_and(|output| output.status.success())
    }
}

/// Herdr's agent status, read as Flow's AgentState.
pub trait ReadsAgentStatus {
    /// A word Herdr adds later is Unknown until it is named.
    fn agent_state(self) -> AgentState;
}

impl ReadsAgentStatus for Option<&str> {
    fn agent_state(self) -> AgentState {
        match self {
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
    /// What empties a composer an interrupt put a letter back into.
    retraction: Retraction,
}

/// How a harness's composer is emptied of a letter an interrupt restored.
enum Retraction {
    /// One key that empties the composer in any edit mode.
    Key(&'static str),
    /// Line by line, as `take_back_keys`.
    LineByLine,
}

trait KeysComposer {
    /// The key that submits the composer, as Herdr's own CR does.
    fn submit_keys(&self) -> Vec<String>;
    /// Witnessed of Claude Code 2.1.280 and Codex 0.153 (e167d8 sandbox,
    /// 2026-09-26): `ctrl+u` kills from the cursor to the line's start and
    /// `backspace` at a line's start joins it onto the line above; neither
    /// interrupts a working turn, as `esc` and `ctrl+c` would. The cursor
    /// rests at the end of a placed text.
    /// See `WritesPane::retract`. Codex has no vim mode: its composer is
    /// taken back line by line, generously, since a restored text's length
    /// is not known and the keys do nothing to an empty composer.
    fn retract_keys(&self) -> Vec<String>;
    const RETRACTED_LINES: usize = 32;
    fn take_back_keys(&self, lines: usize) -> Vec<String>;
}

impl KeysComposer for Composer {
    fn submit_keys(&self) -> Vec<String> {
        vec!["enter".to_owned()]
    }

    fn retract_keys(&self) -> Vec<String> {
        match self.retraction {
            Retraction::Key(key) => vec![key.to_owned()],
            Retraction::LineByLine => self.take_back_keys(Self::RETRACTED_LINES),
        }
    }

    fn take_back_keys(&self, lines: usize) -> Vec<String> {
        (0..lines.max(1))
            .flat_map(|_| ["ctrl+u".to_owned(), "backspace".to_owned()])
            .collect()
    }
}

trait ReadsComposer {
    fn of(harness_kind: &HarnessKind) -> Self;
    /// The composer is the last line opening with the glyph. It holds no
    /// text when nothing follows the glyph but a placeholder, which both
    /// harnesses render dim (SGR 2); text a person typed is not dim.
    fn text(&self, screen: &str) -> Option<String>;
    #[cfg(test)]
    fn is_blank(&self, screen: &str) -> Option<bool>;
}

impl ReadsComposer for Composer {
    fn of(harness_kind: &HarnessKind) -> Self {
        match harness_kind {
            HarnessKind::Claude => Self {
                glyph: '❯',
                retraction: Retraction::Key("ctrl+c"),
            },
            HarnessKind::Codex => Self {
                glyph: '›',
                retraction: Retraction::LineByLine,
            },
        }
    }

    fn text(&self, screen: &str) -> Option<String> {
        let line = screen
            .lines()
            .map(StyledLine::from)
            .rfind(|line| line.visible().trim_start().starts_with(self.glyph))?;
        Some(line.text_after(self.glyph))
    }

    #[cfg(test)]
    fn is_blank(&self, screen: &str) -> Option<bool> {
        self.text(screen).map(|text| text.is_empty())
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

trait ReadsStyledLine {
    fn visible(&self) -> String;
    /// What follows the glyph, trimmed, with dim placeholder text left out.
    fn text_after(&self, glyph: char) -> String;
}

impl ReadsStyledLine for StyledLine {
    fn visible(&self) -> String {
        self.characters
            .iter()
            .map(|(character, _)| *character)
            .collect()
    }

    fn text_after(&self, glyph: char) -> String {
        self.characters
            .iter()
            .skip_while(|(character, _)| *character != glyph)
            .skip(1)
            .filter(|(_, dim)| !*dim)
            .map(|(character, _)| *character)
            .collect::<String>()
            .trim()
            .to_owned()
    }
}

#[cfg(test)]
mod tests {
    use super::Composer;
    use crate::herdr::pane::ReadsComposer;
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
