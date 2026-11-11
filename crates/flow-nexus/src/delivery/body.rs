//! The body refusal, enforced at the writer: Flow is the last gate and the
//! only one that knows the recipient's harness.
//!
//! Three layers. Two make a harness command impossible by construction; the
//! third reports intent.
//! 1. A Waking is rendered as one vector of its visible input datoms, oldest
//!    first: each carries its sender and request under its own priority. The
//!    private delivery identity and the recipient already holding the prompt
//!    are not repeated in that text.
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

/// The living's words as text: what they were about, then the words, a line
/// apart; either alone when the other is empty.
pub(crate) struct LivingWords<'a>(pub &'a flow_ethos::Context, pub &'a flow_ethos::Verbatim);

/// Renders the living's words as the text a prompt carries.
pub(crate) trait RendersLivingWords {
    fn text(&self) -> String;
}

impl RendersLivingWords for LivingWords<'_> {
    fn text(&self) -> String {
        match (self.0.0.is_empty(), self.1.0.is_empty()) {
            (true, _) => self.1.0.clone(),
            (_, true) => self.0.0.clone(),
            (false, false) => format!("{}\n{}", self.0.0, self.1.0),
        }
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
            | signal_flow::Request::Notice(text) => text.clone(),
            signal_flow::Request::Result(result) => match result {
                flow_ethos::ResultPayload::Answered(result) => result.string.clone(),
                flow_ethos::ResultPayload::Historical(text) => text.clone(),
            },
            signal_flow::Request::Reminder(reminder) => reminder.string.clone(),
            signal_flow::Request::Task(task) => {
                format!("Task, {} minutes:\n{}", task.minutes.0, task.string)
            }
            signal_flow::Request::Done(done) => match done {
                flow_ethos::DonePayload::Answered(done) => format!("Done:\n{}", done.string),
                flow_ethos::DonePayload::Historical(done) => format!("Done:\n{}", done.string),
            },
            signal_flow::Request::Living(living) => {
                LivingWords(&living.context, &living.verbatim).text()
            }
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

impl RendersNativeContent for signal_flow::VisibleRequest {
    fn native_content(&self) -> String {
        match self {
            signal_flow::VisibleRequest::Order(text)
            | signal_flow::VisibleRequest::Question(text)
            | signal_flow::VisibleRequest::Notice(text) => text.clone(),
            signal_flow::VisibleRequest::Result(result) => match result {
                signal_flow::VisibleResultPayload::Answered(result) => result.string.clone(),
                signal_flow::VisibleResultPayload::Historical(text) => text.clone(),
            },
            signal_flow::VisibleRequest::Task(task) => {
                format!("Task, {} minutes:\n{}", task.minutes.0, task.string)
            }
            signal_flow::VisibleRequest::Done(done) => match done {
                signal_flow::VisibleDonePayload::Answered(done) => {
                    format!("Done:\n{}", done.string)
                }
                signal_flow::VisibleDonePayload::Historical(text) => format!("Done:\n{text}"),
            },
            signal_flow::VisibleRequest::Living(living) => {
                LivingWords(&living.context, &living.verbatim).text()
            }
            signal_flow::VisibleRequest::Reminder(value) => value.string.clone(),
        }
    }
}

impl RendersNativeContent for signal_flow::DeliveredMessage {
    fn native_content(&self) -> String {
        match self {
            signal_flow::DeliveredMessage::HardAbrupt(request)
            | signal_flow::DeliveredMessage::MiddleAbrupt(request)
            | signal_flow::DeliveredMessage::Soft(request) => match request {
                signal_flow::DeliveredRequest::Order(value) => value.string.clone(),
                signal_flow::DeliveredRequest::Question(value) => value.string.clone(),
                signal_flow::DeliveredRequest::Notice(value) => value.string.clone(),
                signal_flow::DeliveredRequest::Result(value) => value.string.clone(),
                signal_flow::DeliveredRequest::Done(value) => value.string.clone(),
                signal_flow::DeliveredRequest::Reminder(value) => value.string.clone(),
                signal_flow::DeliveredRequest::Task(value) => {
                    format!("Task, {} minutes:\n{}", value.minutes.0, value.string)
                }
                signal_flow::DeliveredRequest::Living(value) => {
                    LivingWords(&value.context, &value.verbatim).text()
                }
                signal_flow::DeliveredRequest::PriorResult(value)
                | signal_flow::DeliveredRequest::PriorDone(value) => value.clone(),
            },
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
                // A Waking is typed as itself alone: one vector of its
                // inputs' datoms, each its logical delivery id, who sent it
                // and its request under its own priority, as a queue's
                // hand-over types it, so its receipt reads the same.
                Content::Waking(inputs) => inputs.datomize(Vec::new()).protosize().compact(),
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
            return Some(BodyRefusal::ControlCharacter(meta_signal_flow::ByteOffset(
                offset,
            )));
        }
        content
            .strings()
            .into_iter()
            .find_map(|text| profile.command_line(&text))
            .map(|line| BodyRefusal::HarnessCommand(meta_signal_flow::CommandLine(line)))
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
            Content::Waking(inputs) => inputs
                .iter()
                .all(|input| input.delivered_message.native_content().trim().is_empty()),
            Content::Text(text) => text.trim().is_empty(),
            Content::Living(living) => living.verbatim.0.trim().is_empty(),
        }
    }

    fn strings(&self) -> Vec<String> {
        match self {
            Content::Waking(inputs) => inputs
                .iter()
                .map(|input| input.delivered_message.native_content())
                .collect(),
            Content::Text(text) => vec![text.clone()],
            Content::Living(living) => {
                vec![living.context.clone().0, living.verbatim.clone().0]
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
            .find(|sigil| !sigil.0.is_empty() && line.starts_with(sigil.0.as_str()))?;
        // Shell mode runs whatever follows the sigil.
        if sigil.0 == "!" {
            return Some(line.to_owned());
        }
        let rest = &line[sigil.0.len()..];
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
    use crate::message_projection::ProjectsQueueEntry;
    use crate::store::delivery::DefaultsHarnessProfile;
    use meta_signal_flow::{BodyRefusal, Content, Letter, Living_Data, Message};
    use signal_flow::HarnessKind;

    fn soft(text: &str) -> Message {
        Message::Soft(Letter {
            visible_sender: signal_flow::VisibleSender::Notification,
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
            "Soft.{ Notification Text.«Stage 1 is deployed; run the tier tests.» }"
        );
        let hard = Message::HardAbrupt(Letter {
            visible_sender: signal_flow::VisibleSender::Notification,
            content: Content::Text("/compact".into()),
        });
        assert!(
            hard.pane_text()
                .as_str()
                .starts_with("HardAbrupt.{ Notification ")
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
            reader: protos::ReaderBudget::try_from(65536).expect("a constant reader budget"),
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
            logical_delivery_id: flow_ethos::LogicalDeliveryId(
                "logical-delivery-00000000000000000001".into(),
            ),
            sent_by: signal_flow::SentBy::Sender(signal_flow::Address {
                aspect: signal_flow::Aspect::Psyche,
                topic: flow_ethos::Topic("flow".into()),
                model: signal_flow::Model::Opus,
            }),
            message: signal_flow::Message::HardAbrupt(signal_flow::Request::Task(
                flow_ethos::AnswerableTask {
                    minutes: flow_ethos::Minutes(40),
                    string: text.into(),
                },
            )),
        };
        let message = Message::HardAbrupt(Letter {
            visible_sender: signal_flow::VisibleSender::Notification,
            content: Content::Waking(vec![
                signal_flow::QueueEntry {
                    logical_delivery_id: input.logical_delivery_id.clone(),
                    request_id: flow_ethos::RequestId("test".into()),
                    sent_by: input.sent_by.clone(),
                    delivery: flow_ethos::Delivery::Wake,
                    message: input.message.clone(),
                }
                .visible_input(&signal_flow::Address {
                    aspect: signal_flow::Aspect::Psyche,
                    topic: flow_ethos::Topic("flow".into()),
                    model: signal_flow::Model::Opus,
                }),
            ]),
        });
        let typed = message.pane_text();
        assert!(
            typed
                .as_str()
                .starts_with("[ { Sender.{ Psyche flow Opus } ")
        );
        assert_eq!(
            Potential::<Vec<signal_flow::VisibleNativeInput>>::from(typed.as_str().to_owned())
                .actualize(&mut budget())
                .unwrap(),
            match &message {
                Message::HardAbrupt(letter) => match &letter.content {
                    Content::Waking(inputs) => inputs.clone(),
                    _ => unreachable!(),
                },
                _ => unreachable!(),
            }
        );
        assert_eq!(refusal(&message, HarnessKind::Codex), None);
        assert!(!typed.as_str().opens_a_letter());
        let second = signal_flow::NativeInput {
            logical_delivery_id: flow_ethos::LogicalDeliveryId(
                "logical-delivery-00000000000000000002".into(),
            ),
            sent_by: signal_flow::SentBy::Flow,
            message: signal_flow::Message::Soft(signal_flow::Request::Notice("n".into())),
        };
        let entries = [input.clone(), second.clone()].map(|input| signal_flow::QueueEntry {
            logical_delivery_id: input.logical_delivery_id,
            request_id: flow_ethos::RequestId("test".into()),
            sent_by: input.sent_by,
            delivery: flow_ethos::Delivery::Queue,
            message: input.message,
        });
        let cohort = entries.prompt_text(&signal_flow::Address {
            aspect: signal_flow::Aspect::Psyche,
            topic: flow_ethos::Topic("flow".into()),
            model: signal_flow::Model::Opus,
        });
        assert!(cohort.starts_with("[ { "));
        assert_eq!(
            Potential::<Vec<signal_flow::VisibleNativeInput>>::from(cohort)
                .actualize(&mut budget())
                .unwrap(),
            entries
                .iter()
                .map(|entry| {
                    crate::message_projection::ProjectsQueueEntry::visible_input(
                        entry,
                        &signal_flow::Address {
                            aspect: signal_flow::Aspect::Psyche,
                            topic: flow_ethos::Topic("flow".into()),
                            model: signal_flow::Model::Opus,
                        },
                    )
                })
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn native_content_commands_and_control_keys_are_still_refused() {
        let native = |text: &str| {
            Message::HardAbrupt(Letter {
                visible_sender: signal_flow::VisibleSender::Notification,
                content: Content::Waking(vec![
                    signal_flow::QueueEntry {
                        logical_delivery_id: flow_ethos::LogicalDeliveryId("native-content".into()),
                        request_id: flow_ethos::RequestId("test".into()),
                        sent_by: signal_flow::SentBy::Flow,
                        delivery: flow_ethos::Delivery::Wake,
                        message: signal_flow::Message::HardAbrupt(signal_flow::Request::Order(
                            text.into(),
                        )),
                    }
                    .visible_input(&signal_flow::Address {
                        aspect: signal_flow::Aspect::Psyche,
                        topic: flow_ethos::Topic("flow".into()),
                        model: signal_flow::Model::Opus,
                    }),
                ]),
            })
        };
        assert_eq!(
            refusal(&native("/compact now"), HarnessKind::Claude),
            Some(BodyRefusal::HarnessCommand(meta_signal_flow::CommandLine(
                "/compact now".into()
            )))
        );
        let escaped = native("a\u{1b}b");
        let offset = escaped
            .pane_text()
            .as_str()
            .find('\u{1b}')
            .expect("the key is typed") as i64;
        assert_eq!(
            refusal(&escaped, HarnessKind::Claude),
            Some(BodyRefusal::ControlCharacter(meta_signal_flow::ByteOffset(
                offset
            )))
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
                Some(BodyRefusal::HarnessCommand(meta_signal_flow::CommandLine(
                    text.lines().next().unwrap().trim_start().into()
                ))),
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
        let head = "Soft.{ Notification Text.«".len() as i64;
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
                Some(BodyRefusal::ControlCharacter(meta_signal_flow::ByteOffset(
                    head + at
                ))),
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
            visible_sender: signal_flow::VisibleSender::Notification,
            content: Content::Living(Living_Data {
                context: flow_ethos::Context("context".into()),
                verbatim: flow_ethos::Verbatim(" ".into()),
            }),
        });
        assert_eq!(
            refusal(&relay, HarnessKind::Claude),
            Some(BodyRefusal::EmptyBody)
        );
    }
}
