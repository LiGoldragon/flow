//! The body refusal, enforced at the writer: Flow is the last gate and the
//! only one that knows the recipient's harness.
//!
//! Three layers. Two make a harness command impossible by construction; the
//! third reports intent.
//! 1. Delivered NativeInputs are rendered as one vector of their datoms:
//!    logical delivery id, sender and request under its priority, so the
//!    recipient can name each.
//! 2. No keys in the text: no C0 control but LF and TAB, no DEL, no C1. That
//!    excludes CR (a submit), ESC (an interrupt, or the `ESC[201~` that ends
//!    a bracketed paste), Ctrl-C and Ctrl-D. Herdr 0.8.2 `agent prompt` was
//!    witnessed passing an embedded `ESC[201~` and CR through unchanged
//!    inside its own paste brackets, so this layer is what keeps them out.
//! 3. Intent: content whose first line is a harness command (a sigil of the
//!    recipient's profile, then a command word) is refused, telling the
//!    sender to use Command. `!` is refused whatever follows it.

use datom_codec::Datomizable;
use meta_signal_flow::{BodyRefusal, Content, HarnessProfile, Letter, Message};
use protos::{Compactable, Protosizable};

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

/// The exact user-facing text carried by one native message, without its
/// priority, sender, logical identity, or datom wrapper.
pub trait RendersNativeContent {
    fn native_content(&self) -> String;
}

impl RendersNativeContent for signal_flow::Request {
    fn native_content(&self) -> String {
        match self {
            signal_flow::Request::Order(text)
            | signal_flow::Request::Question(text)
            | signal_flow::Request::Result(text)
            | signal_flow::Request::Notice(text) => text.clone(),
            signal_flow::Request::Task(task) => {
                format!("Task, {} minutes:\n{}", task.minutes, task.string)
            }
            signal_flow::Request::Done(done) => {
                format!("Done {}:\n{}", done.logical_delivery_id, done.string)
            }
            signal_flow::Request::Psyche(said) if said.context.is_empty() => said.verbatim.clone(),
            signal_flow::Request::Psyche(said) if said.verbatim.is_empty() => said.context.clone(),
            signal_flow::Request::Psyche(said) => format!("{}\n{}", said.context, said.verbatim),
            signal_flow::Request::Psyches(said) => said
                .iter()
                .map(
                    |said| match (said.context.is_empty(), said.verbatim.is_empty()) {
                        (true, _) => said.verbatim.clone(),
                        (_, true) => said.context.clone(),
                        (false, false) => format!("{}\n{}", said.context, said.verbatim),
                    },
                )
                .collect::<Vec<_>>()
                .join("\n"),
        }
    }
}

impl RendersNativeContent for signal_flow::Message {
    fn native_content(&self) -> String {
        match self {
            signal_flow::Message::HardAbrupt(request)
            | signal_flow::Message::MiddleAbrupt(request)
            | signal_flow::Message::Soft(request) => request.native_content(),
        }
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
            text: match &self.letter().content {
                // A delivered message is typed as a cohort of one: a vector
                // holding its datom, its logical delivery id, who sent it,
                // and its request under its priority, as a queue's hand-over
                // types it, so its receipt reads the same.
                Content::NativeInput(input) => vec![input.clone()]
                    .datomize(Vec::new())
                    .protosize()
                    .compact(),
                _ => self.datomize(Vec::new()).protosize().compact(),
            },
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

/// Recognises only a typed letter envelope in composer text. Bare NativeInput
/// content has no such marker and is excluded by its caller.
pub trait RecognizesLetter {
    fn opens_a_letter(&self) -> bool;
}

impl RecognizesLetter for str {
    fn opens_a_letter(&self) -> bool {
        [
            "HardAbrupt.{ m-",
            "MiddleAbrupt.{ m-",
            "Soft.{ m-",
            "HardAbrupt.{ send-",
            "MiddleAbrupt.{ send-",
            "Soft.{ send-",
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
            .find_map(|text| profile.command_line(&text))
            .map(BodyRefusal::HarnessCommand)
    }
}

trait ReadsContent {
    fn is_empty(&self) -> bool;
    fn strings(&self) -> Vec<String>;
}

impl ReadsContent for Content {
    /// A Text with nothing but whitespace is empty; a Psyche relay is empty
    /// when the psyche's own words are.
    fn is_empty(&self) -> bool {
        match self {
            Content::NativeInput(input) => input.message.native_content().trim().is_empty(),
            Content::Text(text) => text.trim().is_empty(),
            Content::Psyche(psyche) => psyche.psyche_verbatim.trim().is_empty(),
        }
    }

    fn strings(&self) -> Vec<String> {
        match self {
            Content::NativeInput(input) => vec![input.message.native_content()],
            Content::Text(text) => vec![text.clone()],
            Content::Psyche(psyche) => {
                vec![
                    psyche.psyche_context.clone(),
                    psyche.psyche_verbatim.clone(),
                ]
            }
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
    use crate::composition::RendersQueuedWork;
    use crate::delivery::body::ShowsPaneText;
    use crate::store::delivery::DefaultsHarnessProfile;
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
        message.refusal(&harness.default_profile())
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
    fn recognizes_flow_minted_abrupt_send_letters() {
        for prefix in ["HardAbrupt", "MiddleAbrupt"] {
            let text = format!("{prefix}.{{ send-19a2-3 Owner Text.«work» }}");
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

    fn budget() -> datom_codec::Budget {
        datom_codec::Budget {
            remaining: 65536,
            reader: protos::ReaderBudget { remaining: 65536 },
            depth: 0,
            maximum_depth: 1024,
        }
    }

    /// A delivered message is typed as its datom, and a cohort as one
    /// vector of them: each reads back as the very NativeInput delivered,
    /// its id, sender, priority and request with the body's quotes, braces
    /// and line kept.
    #[test]
    fn native_input_renders_as_its_datom_and_a_cohort_as_one_vector() {
        use datom_codec::{Actualizing, Potential};
        let text = "keep «quotes» and { braces }\nand preserve the second line";
        let input = signal_flow::NativeInput {
            logical_delivery_id: "logical-delivery-00000000000000000001".into(),
            sent_by: signal_flow::SentBy::Sender(signal_flow::Address {
                aspect: signal_flow::Aspect::Psyche,
                topic: "flow".into(),
                layer: signal_flow::Layer::Secondary,
            }),
            message: signal_flow::Message::HardAbrupt(signal_flow::Request::Task(
                flow_ethos::Task_Data {
                    minutes: 40,
                    string: text.into(),
                },
            )),
        };
        let message = Message::HardAbrupt(Letter {
            message_id: "logical-delivery-00000000000000000001".into(),
            sender: Sender::Owner,
            content: Content::NativeInput(input.clone()),
        });
        let typed = message.pane_text();
        assert!(
            typed
                .as_str()
                .starts_with("[ { logical-delivery-00000000000000000001 ")
        );
        assert_eq!(
            Potential::<Vec<signal_flow::NativeInput>>::from(typed.as_str().to_owned())
                .actualize(&mut budget())
                .unwrap(),
            vec![input.clone()]
        );
        assert_eq!(refusal(&message, HarnessKind::Codex), None);
        assert!(!typed.as_str().opens_a_letter());
        let second = signal_flow::NativeInput {
            logical_delivery_id: "logical-delivery-00000000000000000002".into(),
            sent_by: signal_flow::SentBy::Flow,
            message: signal_flow::Message::Soft(signal_flow::Request::Notice("n".into())),
        };
        let entries = [input.clone(), second.clone()].map(|input| signal_flow::QueueEntry {
            logical_delivery_id: input.logical_delivery_id,
            sent_by: input.sent_by,
            delivery: signal_flow::Delivery::Queue,
            message: input.message,
        });
        let cohort = entries.prompt_text();
        assert!(cohort.starts_with("[ { "));
        assert_eq!(
            Potential::<Vec<signal_flow::NativeInput>>::from(cohort)
                .actualize(&mut budget())
                .unwrap(),
            vec![input, second]
        );
    }

    #[test]
    fn native_content_commands_and_control_keys_are_still_refused() {
        let native = |text: &str| {
            Message::HardAbrupt(Letter {
                message_id: "native-content".into(),
                sender: Sender::Owner,
                content: Content::NativeInput(signal_flow::NativeInput {
                    logical_delivery_id: "native-content".into(),
                    sent_by: signal_flow::SentBy::Flow,
                    message: signal_flow::Message::HardAbrupt(signal_flow::Request::Order(
                        text.into(),
                    )),
                }),
            })
        };
        assert_eq!(
            refusal(&native("/compact now"), HarnessKind::Claude),
            Some(BodyRefusal::HarnessCommand("/compact now".into()))
        );
        let escaped = native("a\u{1b}b");
        let offset = escaped
            .pane_text()
            .as_str()
            .find('\u{1b}')
            .expect("the key is typed") as i64;
        assert_eq!(
            refusal(&escaped, HarnessKind::Claude),
            Some(BodyRefusal::ControlCharacter(offset))
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
