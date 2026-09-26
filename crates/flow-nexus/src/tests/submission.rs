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
use signal_flow::FlowLifecycle;
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
    /// first response, and `ctrl+u` take it out.
    fn interrupt_restores(&self, restored: &str) {
        let held = self.directory.path().join("composer.txt");
        let log = self.directory.path().join("herdr-operations.log");
        let body = fs::read_to_string(&self.snapshot_program).expect("Herdr fixture");
        let body = body
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
                    "  \"pane send-keys\") printf '%s\\n' \"$*\" >> '{1}'; case \"$6\" in esc) printf '%s' '{2}' > '{0}' ;; ctrl+u) rm -f '{0}' ;; esac ;;\n  \"pane close\"|",
                    held.display(),
                    log.display(),
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
    fixture.interrupt_restores("Soft.{ m-18d8ebf6d76a13da009 Owner Text.«sleep» }");
    fixture.register_with(FlowLifecycle::Active);
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

#[test]
fn a_draft_an_interrupt_put_back_is_never_taken_out() {
    let fixture = NexusFixture::new();
    fixture.accept_pane_operations(
        vec![fixture.current_agent()],
        PromptFixture::Prompted("w1:p3"),
    );
    fixture.interrupt_restores("half a thought of my own");
    fixture.register_with(FlowLifecycle::Active);
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
