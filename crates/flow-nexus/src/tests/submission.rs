//! A delivery is Presented only when its submission is seen: the letter
//! left the composer. Against a fixture Herdr whose composer keeps what
//! `agent prompt` typed until a key takes it away, as Claude Code's did in
//! the e167d8 sandbox (fms-9a3e2b, Haiku 4.5, 2.1.280): Herdr answered
//! `agent_prompted` after a state change, and the letter sat typed and
//! unsubmitted, so every later letter to that pane found its composer
//! occupied and was parked for good.

use super::{ControlsHerdrSnapshot, NexusFixture, PromptFixture};
use crate::Dispatches;
use crate::fixture_executable::{FixtureExecutable, InstallsScript};
use meta_signal_flow::{
    Content, Delivery, DeliveryGrade, DeliveryRejection, DeliveryRequest, InterruptWitness, Letter,
    Message, Query as MetaQuery, Response as MetaResponse, Sender,
};
use signal_flow::{FlowLifecycle, HarnessKind};
use std::fs;

/// What the fixture composer does with the keys Flow presses into it.
#[derive(Clone, Copy)]
enum Composer {
    /// `enter` submits what the composer holds.
    SubmitsOnEnter,
    /// `enter` is lost; `ctrl+u` then `backspace` takes the text away.
    ClearsOnKill,
    /// Nothing takes the text away.
    Holds,
}

/// How a harness's own composer shows a letter an interrupt restored, and
/// which key empties it. Codex 0.153 and Claude Code 2.1.280 as the e167d8
/// sandbox showed them, 2026-09-26: Claude renders a non-breaking space
/// after its glyph, and one `ctrl+c` empties it in either vim mode, where
/// `ctrl+u` after the second `esc` would stop short of the cursor.
#[derive(Clone, Copy)]
enum FixtureComposer {
    Codex,
    Claude,
}

impl FixtureComposer {
    /// The `printf` format the fixture's `agent read` prints the held text
    /// with: the harness's glyph, its spacing, then the text.
    fn occupied_line(self) -> &'static str {
        match self {
            Self::Codex => "› %s\\n",
            Self::Claude => "❯\u{a0}%s\\n",
        }
    }

    /// The key the fixture composer empties on, and nothing else does.
    fn empties_on(self) -> &'static str {
        match self {
            Self::Codex => "ctrl+u",
            Self::Claude => "ctrl+c",
        }
    }

    fn harness_kind(self) -> HarnessKind {
        match self {
            Self::Codex => HarnessKind::Codex,
            Self::Claude => HarnessKind::Claude,
        }
    }

    /// The harness as Herdr's snapshot names it, which the binding must
    /// agree with before the flow is registered at all.
    fn herdr_agent(self) -> &'static str {
        match self {
            Self::Codex => "codex",
            Self::Claude => "claude",
        }
    }

    /// A session identity of the shape the flow claim admits for this
    /// harness: Claude's is read as a UUID and must be version 4 with an
    /// RFC variant nibble, Codex's is its own.
    fn session_id(self) -> &'static str {
        match self {
            Self::Codex => "01a0b22c-e24f-7452-9940-64490878680f",
            Self::Claude => "01a0b22c-e24f-4452-9940-64490878680f",
        }
    }
}

impl NexusFixture {
    fn deliver_soft(&self, delivery_id: &str, text: &str) -> MetaResponse {
        self.nexus
            .dispatch_meta(MetaQuery::Deliver(DeliveryRequest {
                delivery_id: delivery_id.into(),
                flow_id: "908786".into(),
                message: Message::Soft(Letter {
                    message_id: super::FIXTURE_MESSAGE_ID.into(),
                    sender: Sender::Owner,
                    content: Content::Text(text.into()),
                }),
            }))
    }

    /// Makes the fixture composer keep what `agent prompt` types, as a
    /// harness that lost the submitting CR does, and answer keys as
    /// `composer` says.
    fn composer_keeps_the_letter(&self, composer: Composer) {
        let held = self.directory.path().join("composer.txt");
        let log = self.directory.path().join("herdr-operations.log");
        let take = format!("rm -f '{}'", held.display());
        let (on_enter, on_kill) = match composer {
            Composer::SubmitsOnEnter => (take.as_str(), ":"),
            Composer::ClearsOnKill => (":", take.as_str()),
            Composer::Holds => (":", ":"),
        };
        let body = fs::read_to_string(&self.snapshot_program).expect("Herdr fixture");
        let body = body
            .replacen(
                "\"agent prompt\") printf",
                &format!("\"agent prompt\") printf '%s' \"$6\" > '{}'; printf", held.display()),
                1,
            )
            .replacen(
                "\"agent read\") printf '%s\\n' '❯ ' '› ' ;;",
                &format!(
                    "\"agent read\") if [ -f '{0}' ]; then printf '› %s\\n' \"$(cat '{0}')\"; else printf '%s\\n' '❯ ' '› '; fi ;;",
                    held.display()
                ),
                1,
            )
            .replacen(
                "  \"pane close\"|",
                &format!(
                    "  \"pane send-keys\") printf '%s\\n' \"$*\" >> '{}'; case \"$6\" in enter) {on_enter} ;; ctrl+u) {on_kill} ;; esac ;;\n  \"pane close\"|",
                    log.display()
                ),
                1,
            );
        FixtureExecutable {
            path: self.snapshot_program.clone(),
        }
        .install(&body);
    }

    /// Makes the fixture's interrupt put `restored` back into the composer,
    /// as Claude Code does with the prompt of a turn cancelled before its
    /// first response, and the harness's own retract key take it out.
    fn interrupt_restores(&self, harness: FixtureComposer, restored: &str) {
        let held = self.directory.path().join("composer.txt");
        let log = self.directory.path().join("herdr-operations.log");
        let body = fs::read_to_string(&self.snapshot_program).expect("Herdr fixture");
        let body = body
            .replacen(
                "\"agent read\") printf '%s\\n' '❯ ' '› ' ;;",
                &format!(
                    "\"agent read\") if [ -f '{0}' ]; then printf '{1}' \"$(cat '{0}')\"; else printf '%s\\n' '❯ ' '› '; fi ;;",
                    held.display(),
                    harness.occupied_line(),
                ),
                1,
            )
            .replacen(
                "  \"pane close\"|",
                &format!(
                    "  \"pane send-keys\") printf '%s\\n' \"$*\" >> '{1}'; case \"$6\" in esc) printf '%s' '{3}' > '{0}' ;; {2}) rm -f '{0}' ;; esac ;;\n  \"pane close\"|",
                    held.display(),
                    log.display(),
                    harness.empties_on(),
                    restored,
                ),
                1,
            );
        FixtureExecutable {
            path: self.snapshot_program.clone(),
        }
        .install(&body);
    }

    fn composer_holds(&self) -> Option<String> {
        fs::read_to_string(self.directory.path().join("composer.txt")).ok()
    }

    fn key_presses(&self) -> Vec<String> {
        fs::read_to_string(self.directory.path().join("herdr-operations.log"))
            .unwrap_or_default()
            .lines()
            .filter_map(|line| line.split_once(" pane send-keys w1:p3 "))
            .map(|(_, keys)| keys.to_owned())
            .collect()
    }

    /// The Herdr snapshot agent of a working recipient under `harness`.
    fn working_agent(&self, harness: FixtureComposer) -> serde_json::Value {
        let mut agent = self.current_agent();
        agent["agent"] = serde_json::Value::String(harness.herdr_agent().to_owned());
        agent
    }

    /// Registers the fixture flow as running under `harness`, so the
    /// writer reaches for that harness's glyph, interrupt and retract key.
    fn register_under(&self, harness: FixtureComposer) {
        fs::write(
            self.directory.path().join("flows/.908786.flow-id"),
            format!(
                "version=1\nharness={}\nidentity={}\nalias=908786\n",
                harness.herdr_agent(),
                harness.session_id().replace('-', ""),
            ),
        )
        .expect("fixture flow claim");
        let mut node = self.node();
        node.harness_kind = harness.harness_kind();
        node.session_id = harness.session_id().into();
        node.origin_clue.session_id = harness.session_id().into();
        node.flow_lifecycle = FlowLifecycle::Active;
        assert!(matches!(
            self.nexus.dispatch_meta(MetaQuery::RegisterFlow(node)),
            MetaResponse::FlowRegistered(_)
        ));
    }

    fn resting_recipient(&self, composer: Composer) {
        self.accept_pane_operations(vec![self.settled_agent()], PromptFixture::Prompted("w1:p3"));
        self.composer_keeps_the_letter(composer);
        self.register_with(FlowLifecycle::Active);
    }
}

fn delivered(delivery_id: &str, delivery_grade: DeliveryGrade) -> MetaResponse {
    MetaResponse::Delivered(Delivery {
        delivery_id: delivery_id.into(),
        flow_id: "908786".into(),
        interrupt_witness: InterruptWitness::NotRequested,
        delivery_grade,
    })
}

#[test]
fn a_letter_herdr_calls_prompted_but_the_composer_still_holds_is_not_presented() {
    let fixture = NexusFixture::new();
    fixture.resting_recipient(Composer::SubmitsOnEnter);
    // Herdr answered agent_prompted after a state change; the composer
    // kept the letter until Flow pressed the submit key once itself. The
    // reaction Herdr saw was not this letter's, so it is Transported.
    assert_eq!(
        fixture.deliver_soft("stuck-1", "when you rest"),
        delivered("stuck-1", DeliveryGrade::Transported)
    );
    assert_eq!(fixture.key_presses(), ["enter"]);
    assert_eq!(fixture.composer_holds(), None);
}

#[test]
fn a_letter_that_will_not_submit_is_taken_back_and_parked() {
    let fixture = NexusFixture::new();
    fixture.resting_recipient(Composer::ClearsOnKill);
    assert_eq!(
        fixture.deliver_soft("stuck-2", "first line\nsecond line"),
        MetaResponse::DeliveryRejected(DeliveryRejection::ComposerOccupied)
    );
    // One submit, then a kill-line and a join for each line of the letter.
    assert_eq!(
        fixture.key_presses(),
        ["enter", "ctrl+u backspace ctrl+u backspace"]
    );
    assert_eq!(fixture.composer_holds(), None, "the pane is free again");
    // Nothing of a taken-back letter is kept: Message may deliver it again
    // under the same DeliveryId, and this time it submits.
    fixture.accept_pane_operations(
        vec![fixture.settled_agent()],
        PromptFixture::Prompted("w1:p3"),
    );
    fixture.composer_keeps_the_letter(Composer::SubmitsOnEnter);
    assert!(matches!(
        fixture.deliver_soft("stuck-2", "first line\nsecond line"),
        MetaResponse::Delivered(_)
    ));
}

#[test]
fn a_letter_that_can_be_neither_submitted_nor_taken_back_is_uncertain_and_lets_the_pane_go() {
    let fixture = NexusFixture::new();
    fixture.resting_recipient(Composer::Holds);
    assert_eq!(
        fixture.deliver_soft("stuck-3", "when you rest"),
        delivered("stuck-3", DeliveryGrade::Uncertain)
    );
    // The lease is let go: the next write to the pane is answered at once,
    // refused because the composer holds text, instead of waiting.
    assert_eq!(
        fixture.deliver_soft("stuck-4", "another"),
        MetaResponse::DeliveryRejected(DeliveryRejection::ComposerOccupied)
    );
}

#[test]
fn a_letter_seen_leaving_the_composer_is_presented_with_no_key_pressed() {
    let fixture = NexusFixture::new();
    fixture.accept_pane_operations(
        vec![fixture.settled_agent()],
        PromptFixture::Prompted("w1:p3"),
    );
    fixture.register_with(FlowLifecycle::Active);
    assert_eq!(
        fixture.deliver_soft("clean-1", "when you rest"),
        delivered("clean-1", DeliveryGrade::Presented)
    );
    assert!(fixture.key_presses().is_empty());
}

/// Herdr's `agent_prompt_stalled` follows typed input: the letter may sit
/// in the composer. It is submitted there too, and the pane is let go.
#[test]
fn a_stalled_prompt_left_in_the_composer_is_submitted() {
    let fixture = NexusFixture::new();
    fixture.accept_pane_operations(
        vec![fixture.settled_agent()],
        PromptFixture::FailedAfterInput("agent_prompt_stalled"),
    );
    fixture.composer_keeps_the_letter(Composer::SubmitsOnEnter);
    fixture.register_with(FlowLifecycle::Active);
    assert_eq!(
        fixture.deliver_soft("stalled-1", "when you rest"),
        delivered("stalled-1", DeliveryGrade::Transported)
    );
    assert_eq!(fixture.key_presses(), ["enter"]);
    assert_eq!(fixture.composer_holds(), None);
}

fn hard_abrupt(text: &str) -> Message {
    Message::HardAbrupt(Letter {
        message_id: super::FIXTURE_MESSAGE_ID.into(),
        sender: Sender::Owner,
        content: Content::Text(text.into()),
    })
}

/// e167d8 sandbox (fms-9a3e2b, fms-d70a61): a HardAbrupt's interrupt,
/// pressed before Claude's first response, put the Soft letter just
/// Presented back into the composer. Flow then refused the HardAbrupt as
/// ComposerOccupied, Message parked it, and the restored letter held the
/// pane against every later letter.
#[test]
fn a_letter_an_interrupt_put_back_is_taken_out_and_the_hard_abrupt_lands() {
    let fixture = NexusFixture::new();
    fixture.accept_pane_operations(
        vec![fixture.current_agent()],
        PromptFixture::Prompted("w1:p3"),
    );
    fixture.interrupt_restores(
        FixtureComposer::Codex,
        "Soft.{ m-18d8ebf6d76a13da009 Owner Text.«sleep» }",
    );
    fixture.register_under(FixtureComposer::Codex);
    assert_eq!(
        fixture
            .nexus
            .dispatch_meta(MetaQuery::Deliver(DeliveryRequest {
                delivery_id: "hard-restored".into(),
                flow_id: "908786".into(),
                message: hard_abrupt("stop"),
            })),
        MetaResponse::Delivered(Delivery {
            delivery_id: "hard-restored".into(),
            flow_id: "908786".into(),
            interrupt_witness: InterruptWitness::Observed,
            delivery_grade: DeliveryGrade::Transported,
        })
    );
    let presses = fixture.key_presses();
    assert_eq!(presses[0], "esc", "Codex's interrupt");
    assert!(presses[1].starts_with("ctrl+u backspace"), "{presses:?}");
    assert_eq!(presses.len(), 2);
    assert_eq!(
        fixture.typed().as_deref(),
        Some("HardAbrupt.{ m-7f3a2c Owner Text.stop }")
    );
}

/// The same path under Claude Code, which is where it was witnessed and
/// where none of the Codex keys would serve: `esc esc` is the interrupt,
/// the restored letter comes back behind `❯` and a non-breaking space, and
/// `Retraction::Key("ctrl+c")` empties the composer with one press —
/// Claude's vim NORMAL mode, which the second `esc` leaves it in, is what
/// makes the line-by-line take-back unfit here.
#[test]
fn a_letter_claudes_interrupt_put_back_is_taken_out_by_one_ctrl_c() {
    let fixture = NexusFixture::new();
    fixture.accept_pane_operations(
        vec![fixture.working_agent(FixtureComposer::Claude)],
        PromptFixture::Prompted("w1:p3"),
    );
    fixture.interrupt_restores(
        FixtureComposer::Claude,
        "Soft.{ m-18d8ebf6d76a13da009 Owner Text.«sleep» }",
    );
    fixture.register_under(FixtureComposer::Claude);
    assert_eq!(
        fixture
            .nexus
            .dispatch_meta(MetaQuery::Deliver(DeliveryRequest {
                delivery_id: "claude-restored".into(),
                flow_id: "908786".into(),
                message: hard_abrupt("stop"),
            })),
        MetaResponse::Delivered(Delivery {
            delivery_id: "claude-restored".into(),
            flow_id: "908786".into(),
            interrupt_witness: InterruptWitness::Observed,
            delivery_grade: DeliveryGrade::Transported,
        })
    );
    assert_eq!(
        fixture.key_presses(),
        ["esc esc", "ctrl+c", "enter"],
        "Claude's interrupt, one retract press and no more, then its submit"
    );
    assert_eq!(fixture.composer_holds(), None, "the pane is free again");
    assert_eq!(
        fixture.typed().as_deref(),
        Some("HardAbrupt.{ m-7f3a2c Owner Text.stop }")
    );
}

/// A second `ctrl+c` into an empty Claude composer quits Claude, so the key
/// goes in only while the composer is seen holding a letter of Flow's own.
/// A text that is not a letter is the person's: the HardAbrupt is refused
/// and nothing is pressed after the interrupt.
#[test]
fn a_draft_claudes_interrupt_put_back_is_never_taken_out() {
    let fixture = NexusFixture::new();
    fixture.accept_pane_operations(
        vec![fixture.working_agent(FixtureComposer::Claude)],
        PromptFixture::Prompted("w1:p3"),
    );
    fixture.interrupt_restores(FixtureComposer::Claude, "half a thought of my own");
    fixture.register_under(FixtureComposer::Claude);
    assert_eq!(
        fixture
            .nexus
            .dispatch_meta(MetaQuery::Deliver(DeliveryRequest {
                delivery_id: "claude-draft".into(),
                flow_id: "908786".into(),
                message: hard_abrupt("stop"),
            })),
        MetaResponse::DeliveryRejected(DeliveryRejection::ComposerOccupied)
    );
    assert_eq!(fixture.key_presses(), ["esc esc"]);
    assert_eq!(
        fixture.composer_holds().as_deref(),
        Some("half a thought of my own")
    );
    assert_eq!(fixture.typed(), None);
}

#[test]
fn a_draft_an_interrupt_put_back_is_never_taken_out() {
    let fixture = NexusFixture::new();
    fixture.accept_pane_operations(
        vec![fixture.current_agent()],
        PromptFixture::Prompted("w1:p3"),
    );
    fixture.interrupt_restores(FixtureComposer::Codex, "half a thought of my own");
    fixture.register_under(FixtureComposer::Codex);
    assert_eq!(
        fixture
            .nexus
            .dispatch_meta(MetaQuery::Deliver(DeliveryRequest {
                delivery_id: "hard-draft".into(),
                flow_id: "908786".into(),
                message: hard_abrupt("stop"),
            })),
        MetaResponse::DeliveryRejected(DeliveryRejection::ComposerOccupied)
    );
    assert_eq!(fixture.key_presses(), ["esc"]);
    assert_eq!(
        fixture.composer_holds().as_deref(),
        Some("half a thought of my own")
    );
    assert_eq!(fixture.typed(), None);
}
