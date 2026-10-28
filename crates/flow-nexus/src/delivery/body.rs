//! The body refusal, enforced at the writer: Flow is the last gate and the
//! only one that knows the recipient's harness.
//!
//! Three layers. Two make a harness command impossible by construction; the
//! third reports intent.
//! 1. Typed Body only: the complete priority-headed Message is rendered as
//!    datom, including its kind and Flow-resolved sender.
//! 2. No keys in the text: no C0 control but LF and TAB, no DEL, no C1. That
//!    excludes CR (a submit), ESC (an interrupt, or the `ESC[201~` that ends
//!    a bracketed paste), Ctrl-C and Ctrl-D. Herdr 0.8.2 `agent prompt` was
//!    witnessed passing an embedded `ESC[201~` and CR through unchanged
//!    inside its own paste brackets, so this layer is what keeps them out.
//! 3. Intent: content whose first line is a harness command (a sigil of the
//!    recipient's profile, then a command word) is refused, telling the
//!    sender to use Command. `!` is refused whatever follows it.

use datom_codec::Datomizable;
use meta_signal_flow::{Body, BodyRefusal, HarnessProfile, Letter, Message};
use protos::{Compactable, Protosizable};

/// Projects an ordinary wire message and its authenticated flow identity to
/// the exact typed datom delivered to a pane.
pub(crate) trait TypesPaneMessage {
    fn typed_pane_message(&self, sender: meta_signal_flow::Sender) -> Message;
}
impl TypesPaneMessage for signal_flow::Message {
    fn typed_pane_message(&self, sender: meta_signal_flow::Sender) -> Message {
        let (priority, request) = match self {
            signal_flow::Message::HardAbrupt(request) => (0, request),
            signal_flow::Message::MiddleAbrupt(request) => (1, request),
            signal_flow::Message::Soft(request) => (2, request),
        };
        let (kind, body) = match request {
            signal_flow::Request::Order(text) => {
                (meta_signal_flow::Kind::Order, Body::Words(text.clone()))
            }
            signal_flow::Request::Result(text) => {
                (meta_signal_flow::Kind::Result, Body::Words(text.clone()))
            }
            signal_flow::Request::Notice(text) => {
                (meta_signal_flow::Kind::Notice, Body::Words(text.clone()))
            }
            signal_flow::Request::Question(text) => {
                (meta_signal_flow::Kind::Question, Body::Words(text.clone()))
            }
            signal_flow::Request::Psyche(data) => (
                meta_signal_flow::Kind::Psyche,
                Body::Psyche(meta_signal_flow::PsycheWords {
                    first_string: data.context.clone(),
                    second_string: data.verbatim.clone(),
                }),
            ),
            signal_flow::Request::Psyches(data) => (
                meta_signal_flow::Kind::Psyche,
                Body::Psyches(
                    data.iter()
                        .map(|said| meta_signal_flow::PsycheWords {
                            first_string: said.context.clone(),
                            second_string: said.verbatim.clone(),
                        })
                        .collect(),
                ),
            ),
        };
        let letter = Letter { kind, sender, body };
        match priority {
            0 => Message::HardAbrupt(letter),
            1 => Message::MiddleAbrupt(letter),
            _ => Message::Soft(letter),
        }
    }
}
/// The text a Message becomes in a pane.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PaneText {
    text: String,
}

/// The pane text as typed.
pub trait ShowsPaneText {
    fn as_str(&self) -> &str;
}

impl ShowsPaneText for PaneText {
    fn as_str(&self) -> &str {
        &self.text
    }
}

/// Renders a Message as the text typed into a pane.
pub trait RendersPaneText {
    fn pane_text(&self) -> PaneText;
    fn letter(&self) -> &Letter;
}

impl RendersPaneText for Message {
    fn pane_text(&self) -> PaneText {
        PaneText {
            text: vec![self.clone()]
                .datomize(Vec::new())
                .protosize()
                .compact(),
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

/// Recognises a typed letter envelope in composer text.
pub trait RecognizesLetter {
    fn opens_a_letter(&self) -> bool;
}

impl RecognizesLetter for str {
    fn opens_a_letter(&self) -> bool {
        [
            "HardAbrupt.{ ",
            "MiddleAbrupt.{ ",
            "Soft.{ ",
            "[ HardAbrupt.{ ",
            "[ MiddleAbrupt.{ ",
            "[ Soft.{ ",
        ]
        .iter()
        .any(|head| self.starts_with(head))
    }
}

/// Answers what the writer would refuse of a Message for one harness.
pub trait VetsBody {
    fn refusal(&self, profile: &HarnessProfile) -> Option<BodyRefusal>;
}

impl VetsBody for Message {
    fn refusal(&self, profile: &HarnessProfile) -> Option<BodyRefusal> {
        let body = &self.letter().body;
        if body.is_empty() {
            return Some(BodyRefusal::EmptyBody);
        }
        if let Some(offset) = self.pane_text().control_character_offset() {
            return Some(BodyRefusal::ControlCharacter(offset));
        }
        body.strings()
            .into_iter()
            .find_map(|text| profile.command_line(&text))
            .map(BodyRefusal::HarnessCommand)
    }
}

trait ReadsBody {
    fn is_empty(&self) -> bool;
    fn strings(&self) -> Vec<String>;
}

impl ReadsBody for Body {
    /// Words with nothing but whitespace are empty; a Psyche relay is empty
    /// when the psyche's own words are.
    fn is_empty(&self) -> bool {
        match self {
            Body::Words(text) => text.trim().is_empty(),
            Body::Psyche(psyche) => psyche.second_string.trim().is_empty(),
            Body::Psyches(psyches) => {
                psyches.is_empty()
                    || psyches
                        .iter()
                        .all(|psyche| psyche.second_string.trim().is_empty())
            }
        }
    }

    fn strings(&self) -> Vec<String> {
        match self {
            Body::Words(text) => vec![text.clone()],
            Body::Psyche(psyche) => vec![psyche.first_string.clone(), psyche.second_string.clone()],
            Body::Psyches(psyches) => psyches
                .iter()
                .flat_map(|psyche| [psyche.first_string.clone(), psyche.second_string.clone()])
                .collect(),
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

/// Reads, in a text's first line, a command the harness would run.
trait ReadsCommandLine {
    /// The first line, when the harness would read it as one of its
    /// commands.
    fn command_line(&self, text: &str) -> Option<String>;
}

impl ReadsCommandLine for HarnessProfile {
    fn command_line(&self, text: &str) -> Option<String> {
        let line = text.lines().next().unwrap_or_default().trim_start();
        let sigil = self
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
    use super::{RecognizesLetter, RendersPaneText, VetsBody};
    use crate::delivery::body::ShowsPaneText;
    use crate::store::delivery::DefaultsHarnessProfile;
    use meta_signal_flow::{Body, BodyRefusal, Kind, Letter, Message, PsycheWords, Sender};
    use signal_flow::HarnessKind;

    fn soft(text: &str) -> Message {
        Message::Soft(Letter {
            kind: Kind::Notice,
            sender: Sender {
                address: signal_flow::Address {
                    aspect: signal_flow::Aspect::Psyche,
                    topic: "core".into(),
                    layer: signal_flow::Layer::Secondary,
                },
                flow_id: "e167d8".into(),
            },
            body: Body::Words(text.into()),
        })
    }

    fn refusal(message: &Message, harness: HarnessKind) -> Option<BodyRefusal> {
        message.refusal(&harness.default_profile())
    }

    #[test]
    fn the_pane_text_begins_with_the_priority_head() {
        assert_eq!(
            soft("Stage 1 is deployed; run the tier tests.")
                .pane_text()
                .as_str(),
            "[ Soft.{ Notice { { Psyche core Secondary } e167d8 } Words.«Stage 1 is deployed; run the tier tests.» } ]"
        );
        let hard = Message::HardAbrupt(Letter {
            kind: Kind::Order,
            sender: soft("x").letter().sender.clone(),
            body: Body::Words("/compact".into()),
        });
        assert!(
            hard.pane_text()
                .as_str()
                .starts_with("[ HardAbrupt.{ Order { { Psyche core Secondary } e167d8 } ")
        );
    }

    #[test]
    fn recognizes_flow_minted_abrupt_send_letters() {
        for prefix in ["HardAbrupt", "MiddleAbrupt"] {
            let text =
                format!("{prefix}.{{ Order {{ {{ Psyche core Secondary }} e167d8 }} Words.work }}");
            assert!(text.as_str().opens_a_letter(), "{text}");
        }
    }

    #[test]
    fn recognizes_the_actual_durable_logical_identity_shape() {
        assert!(
            !"{ logical-delivery-00000000000000000001 Owner Soft.Order.work }".opens_a_letter()
        );
        assert!(
            !"{ logical-delivery-0000000000000000000x Owner Soft.Order.work }".opens_a_letter()
        );
        assert!(
            !"{ logical-delivery-000000000000000000010 Owner Soft.Order.work }".opens_a_letter()
        );
    }

    #[test]
    fn complete_message_renders_kind_sender_and_body() {
        let text = "keep «quotes» and { braces }\nand preserve the second line";
        let message = Message::HardAbrupt(Letter {
            kind: Kind::Order,
            sender: soft("x").letter().sender.clone(),
            body: Body::Words(text.into()),
        });
        assert!(
            message
                .pane_text()
                .as_str()
                .starts_with("[ HardAbrupt.{ Order ")
        );
        assert!(message.pane_text().as_str().contains("Words.«keep"));
        assert_eq!(refusal(&message, HarnessKind::Codex), None);
        assert!(!text.opens_a_letter());
    }

    #[test]
    fn native_content_commands_and_control_keys_are_still_refused() {
        let native = |text: &str| {
            Message::HardAbrupt(Letter {
                kind: Kind::Order,
                sender: soft("x").letter().sender.clone(),
                body: Body::Words(text.into()),
            })
        };
        assert_eq!(
            refusal(&native("/compact now"), HarnessKind::Claude),
            Some(BodyRefusal::HarnessCommand("/compact now".into()))
        );
        let control = native("a\u{1b}b");
        assert_eq!(
            refusal(&control, HarnessKind::Claude),
            Some(BodyRefusal::ControlCharacter(
                control.pane_text().as_str().find('\u{1b}').unwrap() as i64
            ))
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
        for (text, _at) in [
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
                Some(BodyRefusal::ControlCharacter(
                    i64::try_from(
                        soft(text)
                            .pane_text()
                            .as_str()
                            .find(|c: char| {
                                let code = u32::from(c);
                                (code < 0x20 && c != '\n' && c != '\t')
                                    || code == 0x7f
                                    || (0x80..=0x9f).contains(&code)
                            })
                            .unwrap()
                    )
                    .unwrap()
                )),
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
            kind: Kind::Psyche,
            sender: soft("x").letter().sender.clone(),
            body: Body::Psyche(PsycheWords {
                first_string: "context".into(),
                second_string: " ".into(),
            }),
        });
        assert_eq!(
            refusal(&relay, HarnessKind::Claude),
            Some(BodyRefusal::EmptyBody)
        );
    }
}
