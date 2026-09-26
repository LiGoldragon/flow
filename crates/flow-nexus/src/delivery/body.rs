//! The body refusal, enforced at the writer: Flow is the last gate and the
//! only one that knows the recipient's harness.
//!
//! Three layers. Two make a harness command impossible by construction; the
//! third reports intent.
//! 1. Head first: Flow renders the typed Message, so the pane text always
//!    begins `HardAbrupt.`, `MiddleAbrupt.` or `Soft.`, and no harness reads
//!    a capital as a command.
//! 2. No keys in the text: no C0 control but LF and TAB, no DEL, no C1. That
//!    excludes CR (a submit), ESC (an interrupt, or the `ESC[201~` that ends
//!    a bracketed paste), Ctrl-C and Ctrl-D. Herdr 0.8.2 `agent prompt` was
//!    witnessed passing an embedded `ESC[201~` and CR through unchanged
//!    inside its own paste brackets, so this layer is what keeps them out.
//! 3. Intent: a Content whose first line is a harness command (a sigil of
//!    the recipient's profile, then a command word) is refused, telling the
//!    sender to use Command. `!` is refused whatever follows it.

use datom_codec::Datomizable;
use meta_signal_flow::{BodyRefusal, Content, HarnessProfile, Letter, Message};
use protos::{Protosizable, Textualizable};

/// The text a Message becomes in a pane.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PaneText {
    text: String,
}

impl PaneText {
    pub fn as_str(&self) -> &str {
        &self.text
    }
}

/// Renders a Message as the text typed into a pane.
///
/// Exception, noted here: the Nexus thinks in typed values and speaks no
/// text on its wire, but a pane is a text boundary like a CLI. Flow renders
/// this one type itself (F6) so that the Priority head is always the first
/// byte in the pane; the rendering is the Message's own datom.
pub trait RendersPaneText {
    fn pane_text(&self) -> PaneText;
    fn letter(&self) -> &Letter;
}

impl RendersPaneText for Message {
    fn pane_text(&self) -> PaneText {
        PaneText {
            text: self.datomize(Vec::new()).protosize().textualize(),
        }
    }

    fn letter(&self) -> &Letter {
        match self {
            Message::HardAbrupt(letter) | Message::MiddleAbrupt(letter) | Message::Soft(letter) => {
                letter
            }
        }
    }
}

/// Answers what the writer would refuse of a Message for one harness.
pub trait VetsBody {
    fn refusal(&self, profile: &HarnessProfile) -> Option<BodyRefusal>;
}

impl VetsBody for Message {
    fn refusal(&self, profile: &HarnessProfile) -> Option<BodyRefusal> {
        let content = &self.letter().content;
        if content.is_empty() {
            return Some(BodyRefusal::EmptyBody);
        }
        if let Some(offset) = self.pane_text().control_character_offset() {
            return Some(BodyRefusal::ControlCharacter(offset));
        }
        content
            .strings()
            .into_iter()
            .find_map(|text| CommandLine::of(text, profile))
            .map(BodyRefusal::HarnessCommand)
    }
}

trait ReadsContent {
    fn is_empty(&self) -> bool;
    fn strings(&self) -> Vec<&str>;
}

impl ReadsContent for Content {
    /// A Text with nothing but whitespace is empty; a Psyche relay is empty
    /// when the psyche's own words are.
    fn is_empty(&self) -> bool {
        match self {
            Content::Text(text) => text.trim().is_empty(),
            Content::Psyche(psyche) => psyche.psyche_verbatim.trim().is_empty(),
        }
    }

    fn strings(&self) -> Vec<&str> {
        match self {
            Content::Text(text) => vec![text],
            Content::Psyche(psyche) => vec![&psyche.psyche_context, &psyche.psyche_verbatim],
        }
    }
}

trait FindsControlCharacters {
    /// The byte offset, in the pane text, of the first character that is a
    /// key rather than text.
    fn control_character_offset(&self) -> Option<i64>;
}

impl FindsControlCharacters for PaneText {
    fn control_character_offset(&self) -> Option<i64> {
        self.text
            .char_indices()
            .find(|(_, character)| {
                let code = u32::from(*character);
                (code < 0x20 && *character != '\n' && *character != '\t')
                    || code == 0x7f
                    || (0x80..=0x9f).contains(&code)
            })
            .and_then(|(offset, _)| i64::try_from(offset).ok())
    }
}

/// A first line that a harness would read as one of its commands.
struct CommandLine;

impl CommandLine {
    fn of(text: &str, profile: &HarnessProfile) -> Option<String> {
        let line = text.lines().next().unwrap_or_default().trim_start();
        let sigil = profile
            .command_sigil_vector
            .iter()
            .find(|sigil| !sigil.is_empty() && line.starts_with(sigil.as_str()))?;
        // Shell mode runs whatever follows the sigil.
        if sigil == "!" {
            return Some(line.to_owned());
        }
        let rest = &line[sigil.len()..];
        let mut characters = rest.char_indices();
        let (_, first) = characters.next()?;
        if !first.is_ascii_lowercase() {
            return None;
        }
        let end = characters
            .find(|(_, character)| {
                !(character.is_ascii_lowercase()
                    || character.is_ascii_digit()
                    || matches!(character, ':' | '_' | '-'))
            })
            .map(|(index, _)| index)
            .unwrap_or(rest.len());
        // A command word ends at a space or the end of the line; anything
        // else (a path's next `/`, a period) is not a command.
        let after = rest[end..].chars().next();
        matches!(after, None | Some(' ')).then(|| line.to_owned())
    }
}

#[cfg(test)]
mod tests {
    use super::{RendersPaneText, VetsBody};
    use crate::store::DefaultConfiguration;
    use meta_signal_flow::{BodyRefusal, Content, Letter, Message, Psyche_Data, Sender};
    use signal_flow::HarnessKind;

    fn soft(text: &str) -> Message {
        Message::Soft(Letter {
            message_id: "m-7f3a2c".into(),
            sender: Sender::Flow("e167d8".into()),
            content: Content::Text(text.into()),
        })
    }

    fn refusal(message: &Message, harness: HarnessKind) -> Option<BodyRefusal> {
        message.refusal(&DefaultConfiguration::harness_profile(&harness))
    }

    #[test]
    fn the_pane_text_begins_with_the_priority_head() {
        assert_eq!(
            soft("Stage 1 is deployed; run the tier tests.")
                .pane_text()
                .as_str(),
            "Soft.{ m-7f3a2c Flow.e167d8 Text.«Stage 1 is deployed; run the tier tests.» }"
        );
        let hard = Message::HardAbrupt(Letter {
            message_id: "m-81b0e4".into(),
            sender: Sender::Owner,
            content: Content::Text("/compact".into()),
        });
        assert!(
            hard.pane_text()
                .as_str()
                .starts_with("HardAbrupt.{ m-81b0e4 Owner ")
        );
    }

    #[test]
    fn harness_commands_are_refused_by_sigil_and_word() {
        for (text, harness) in [
            ("/compact", HarnessKind::Claude),
            ("  /compact now", HarnessKind::Codex),
            ("!ls", HarnessKind::Codex),
            ("! anything at all", HarnessKind::Claude),
            ("#remember this", HarnessKind::Claude),
            ("/review:fast\nsecond line", HarnessKind::Codex),
        ] {
            assert_eq!(
                refusal(&soft(text), harness.clone()),
                Some(BodyRefusal::HarnessCommand(
                    text.lines().next().unwrap().trim_start().into()
                )),
                "{text} to {harness:?}"
            );
        }
    }

    #[test]
    fn paths_later_lines_and_other_harnesses_sigils_pass() {
        for (text, harness) in [
            ("/home/li/x is the path", HarnessKind::Claude),
            ("/home/li/x", HarnessKind::Codex),
            ("see this\n/compact", HarnessKind::Claude),
            ("#hashtag", HarnessKind::Codex),
            ("/Compact is capitalised", HarnessKind::Claude),
            ("/.hidden", HarnessKind::Claude),
            ("plain words\twith a tab", HarnessKind::Codex),
        ] {
            assert_eq!(refusal(&soft(text), harness.clone()), None, "{text}");
        }
    }

    #[test]
    fn keys_are_refused_at_their_offset_in_the_pane_text() {
        let head = "Soft.{ m-7f3a2c Flow.e167d8 Text.«".len() as i64;
        for (text, at) in [
            // Each text holds a space, so it is rendered in guillemets.
            ("a \u{1b}b", 2),
            ("paste \u{1b}[201~out", 6),
            ("line\rsubmit", 4),
            ("x \u{3}", 2),
            ("x \u{4}", 2),
            ("del \u{7f}", 4),
            ("c1 \u{9b}", 3),
        ] {
            assert_eq!(
                refusal(&soft(text), HarnessKind::Claude),
                Some(BodyRefusal::ControlCharacter(head + at)),
                "{text:?}"
            );
        }
    }

    #[test]
    fn an_empty_body_is_refused() {
        assert_eq!(
            refusal(&soft("  \n "), HarnessKind::Codex),
            Some(BodyRefusal::EmptyBody)
        );
        let relay = Message::MiddleAbrupt(Letter {
            message_id: "m-90c1aa".into(),
            sender: Sender::Owner,
            content: Content::Psyche(Psyche_Data {
                psyche_context: "context".into(),
                psyche_verbatim: " ".into(),
            }),
        });
        assert_eq!(
            refusal(&relay, HarnessKind::Claude),
            Some(BodyRefusal::EmptyBody)
        );
    }
}
