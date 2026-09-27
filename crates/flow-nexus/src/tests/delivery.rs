//! Deliver, Vet, Command and Observe.Agent against a fixture Herdr that
//! logs every call. The fixture's answers are Herdr 0.8.2's witnessed
//! shapes; what each test expects is written out by hand.

use super::{
    ControlsHerdrSnapshot, NexusFixture, PromptFixture, Sent, middle_abrupt, prompts, rendered,
};
use crate::fixture_executable::{FixtureExecutable, InstallsScript};
use crate::{Dispatches, peer::AdmitsMetaPeer};
use meta_signal_flow::{
    BodyRefusal, CommandGrade, CommandOutcome, CommandRejection, CommandRequest, Content, Delivery,
    DeliveryGrade, DeliveryRejection, DeliveryRequest, HarnessCommand, InterruptWitness, Letter,
    Message, Query as MetaQuery, Response as MetaResponse, Sender,
};
use signal_flow::{AgentObservation, AgentState, FlowLifecycle, ObserveSelection, Query, Response};
use std::fs;

fn letter(text: &str) -> Letter {
    Letter {
        message_id: super::FIXTURE_MESSAGE_ID.into(),
        sender: Sender::Flow("e167d8".into()),
        content: Content::Text(text.into()),
    }
}

impl NexusFixture {
    fn deliver(&self, delivery_id: &str, message: Message) -> MetaResponse {
        self.nexus
            .dispatch_meta(MetaQuery::Deliver(DeliveryRequest {
                delivery_id: delivery_id.into(),
                flow_id: "908786".into(),
                message,
            }))
    }

    fn blocked_agent(&self) -> serde_json::Value {
        let mut agent = self.current_agent();
        agent["agent_status"] = serde_json::Value::String("blocked".into());
        agent
    }

    /// Rewrites the fixture's composer line: what `agent read` shows.
    fn show_composer(&self, line: &str) {
        let body = fs::read_to_string(&self.snapshot_program).expect("Herdr fixture");
        let body = body.replacen("'❯ ' '› '", &format!("'{line}'"), 1);
        FixtureExecutable {
            path: self.snapshot_program.clone(),
        }
        .install(&body);
    }

    /// Makes the fixture's `agent wait` time out `misses` times before the
    /// agent is seen leaving Working; `None` never shows it leaving.
    fn leave_working_after(&self, misses: Option<usize>) {
        let log = self.directory.path().join("herdr-operations.log");
        let count = self.directory.path().join("agent-waits");
        let leaves = match misses {
            Some(misses) => format!("[ \"$n\" -ge {misses} ]"),
            None => "exit 1".to_owned(),
        };
        let body = fs::read_to_string(&self.snapshot_program).expect("Herdr fixture");
        let body = body
            .replacen("|\"agent wait\") printf", ") printf", 1)
            .replacen(
                "  *) exit 64 ;;",
                &format!(
                    "  \"agent wait\") printf '%s\\n' \"$*\" >> '{}'; n=$(cat '{}' 2>/dev/null || echo 0); echo $((n+1)) > '{}'; {leaves} ;;\n  *) exit 64 ;;",
                    log.display(),
                    count.display(),
                    count.display(),
                ),
                1,
            );
        FixtureExecutable {
            path: self.snapshot_program.clone(),
        }
        .install(&body);
    }

    fn operations(&self) -> String {
        fs::read_to_string(self.directory.path().join("herdr-operations.log")).unwrap_or_default()
    }
}

#[test]
fn soft_waits_for_a_resting_recipient_and_types_nothing_to_a_working_one() {
    let fixture = NexusFixture::new();
    let log = fixture.accept_pane_operations(
        vec![fixture.current_agent()],
        PromptFixture::Prompted("w1:p3"),
    );
    fixture.register_with(FlowLifecycle::Active);
    assert_eq!(
        fixture.deliver("soft-1", Message::Soft(letter("when you rest"))),
        MetaResponse::DeliveryRejected(DeliveryRejection::RecipientWorking)
    );
    assert!(prompts(&log).is_empty());
    assert_eq!(fixture.typed(), None);

    let fixture = NexusFixture::new();
    fixture.accept_pane_operations(
        vec![fixture.settled_agent()],
        PromptFixture::Prompted("w1:p3"),
    );
    fixture.register_with(FlowLifecycle::Active);
    assert_eq!(
        fixture.deliver("soft-2", Message::Soft(letter("when you rest"))),
        MetaResponse::Delivered(Delivery {
            delivery_id: "soft-2".into(),
            flow_id: "908786".into(),
            interrupt_witness: InterruptWitness::NotRequested,
            delivery_grade: DeliveryGrade::Presented,
        })
    );
    assert_eq!(
        fixture.typed().as_deref(),
        Some("Soft.{ m-7f3a2c Flow.e167d8 Text.«when you rest» }")
    );
}

#[test]
fn a_blocked_recipient_is_refused_at_every_tier() {
    for message in [
        Message::HardAbrupt(letter("stop")),
        Message::MiddleAbrupt(letter("note")),
        Message::Soft(letter("later")),
    ] {
        let fixture = NexusFixture::new();
        let log = fixture.accept_pane_operations(
            vec![fixture.blocked_agent()],
            PromptFixture::Prompted("w1:p3"),
        );
        fixture.register_with(FlowLifecycle::Active);
        assert_eq!(
            fixture.deliver("blocked", message),
            MetaResponse::DeliveryRejected(DeliveryRejection::RecipientBlocked)
        );
        assert!(fixture.operations().is_empty(), "no key and no text");
        assert!(prompts(&log).is_empty());
    }
}

#[test]
fn a_draft_in_the_composer_is_never_appended_to() {
    let fixture = NexusFixture::new();
    fixture.accept_pane_operations(
        vec![fixture.settled_agent()],
        PromptFixture::Prompted("w1:p3"),
    );
    fixture.show_composer("\u{1b}[1m›\u{1b}[0m half a thought");
    fixture.register_with(FlowLifecycle::Active);
    assert_eq!(
        fixture.send("a note"),
        Sent::Refused(DeliveryRejection::ComposerOccupied)
    );
    assert_eq!(fixture.typed(), None);
}

#[test]
fn hard_abrupt_interrupts_a_working_recipient_before_typing() {
    let fixture = NexusFixture::new();
    fixture.accept_pane_operations(
        vec![fixture.current_agent()],
        PromptFixture::Prompted("w1:p3"),
    );
    fixture.register_with(FlowLifecycle::Active);
    assert_eq!(
        fixture.deliver("hard-1", Message::HardAbrupt(letter("stop the build"))),
        MetaResponse::Delivered(Delivery {
            delivery_id: "hard-1".into(),
            flow_id: "908786".into(),
            interrupt_witness: InterruptWitness::Observed,
            delivery_grade: DeliveryGrade::Transported,
        })
    );
    let operations: Vec<String> = fixture.operations().lines().map(str::to_owned).collect();
    // Codex's profile: one Escape, then the text, which `agent prompt`
    // submits itself.
    assert_eq!(
        operations,
        vec![
            "--session messaging-build pane send-keys w1:p3 esc".to_owned(),
            "--session messaging-build agent wait w1:p3 --until idle --until done --until blocked --timeout 3000".to_owned(),
            "--session messaging-build agent prompt w1:p3 HardAbrupt.{ m-7f3a2c Flow.e167d8 Text.«stop the build» }".to_owned(),
        ]
    );
}

/// e167d8 sandbox: Claude Code ignored `esc esc` pressed as its turn began
/// (Haiku 4.5, 2.1.280), stayed Working, and the letter queued behind a
/// 90 s command. Pressed again while it still works, it stops.
#[test]
fn hard_abrupt_presses_the_interrupt_again_while_the_recipient_still_works() {
    let fixture = NexusFixture::new();
    fixture.accept_pane_operations(
        vec![fixture.current_agent()],
        PromptFixture::Prompted("w1:p3"),
    );
    fixture.leave_working_after(Some(1));
    fixture.register_with(FlowLifecycle::Active);
    let MetaResponse::Delivered(delivery) =
        fixture.deliver("hard-again", Message::HardAbrupt(letter("stop now")))
    else {
        panic!("the letter is typed after the interrupt")
    };
    assert_eq!(delivery.interrupt_witness, InterruptWitness::Observed);
    let wait = "--session messaging-build agent wait w1:p3 --until idle --until done --until blocked --timeout 3000";
    let press = "--session messaging-build pane send-keys w1:p3 esc";
    assert_eq!(
        fixture.operations().lines().collect::<Vec<_>>(),
        vec![
            press,
            wait,
            press,
            wait,
            "--session messaging-build agent prompt w1:p3 HardAbrupt.{ m-7f3a2c Flow.e167d8 Text.«stop now» }",
        ]
    );
}

#[test]
fn an_interrupt_that_never_shows_is_pressed_a_bounded_number_of_times() {
    let fixture = NexusFixture::new();
    fixture.accept_pane_operations(
        vec![fixture.current_agent()],
        PromptFixture::Prompted("w1:p3"),
    );
    fixture.leave_working_after(None);
    fixture.register_with(FlowLifecycle::Active);
    let MetaResponse::Delivered(delivery) =
        fixture.deliver("hard-stuck", Message::HardAbrupt(letter("stop now")))
    else {
        panic!("the letter is still typed")
    };
    assert_eq!(delivery.interrupt_witness, InterruptWitness::Unobserved);
    let operations = fixture.operations();
    assert_eq!(operations.matches("pane send-keys w1:p3 esc").count(), 3);
    assert_eq!(operations.matches("agent wait").count(), 3);
}

#[test]
fn a_resting_recipient_is_not_interrupted() {
    let fixture = NexusFixture::new();
    fixture.accept_pane_operations(
        vec![fixture.settled_agent()],
        PromptFixture::Prompted("w1:p3"),
    );
    fixture.register_with(FlowLifecycle::Active);
    let MetaResponse::Delivered(delivery) =
        fixture.deliver("hard-2", Message::HardAbrupt(letter("now")))
    else {
        panic!("a resting recipient takes the text")
    };
    assert_eq!(delivery.interrupt_witness, InterruptWitness::NotRequested);
    assert!(!fixture.operations().contains("send-keys"));
}

#[test]
fn bodies_carrying_commands_or_keys_are_refused_and_nothing_is_typed() {
    for (text, refusal) in [
        ("/compact", BodyRefusal::HarnessCommand("/compact".into())),
        ("!ls -la", BodyRefusal::HarnessCommand("!ls -la".into())),
        (
            "end \u{1b}[201~ of paste",
            BodyRefusal::ControlCharacter("MiddleAbrupt.{ m-7f3a2c Owner Text.«end ".len() as i64),
        ),
        (
            "one\rtwo",
            BodyRefusal::ControlCharacter("MiddleAbrupt.{ m-7f3a2c Owner Text.«one".len() as i64),
        ),
        ("   ", BodyRefusal::EmptyBody),
    ] {
        let fixture = NexusFixture::new();
        let log = fixture.accept_pane_operations(
            vec![fixture.settled_agent()],
            PromptFixture::Prompted("w1:p3"),
        );
        fixture.register_with(FlowLifecycle::Active);
        assert_eq!(
            fixture.send(text),
            Sent::Refused(DeliveryRejection::BodyRefused(refusal.clone())),
            "{text:?}"
        );
        assert_eq!(
            fixture.nexus.dispatch_meta(MetaQuery::Vet(DeliveryRequest {
                delivery_id: "vet".into(),
                flow_id: "908786".into(),
                message: middle_abrupt(text),
            })),
            MetaResponse::DeliveryRejected(DeliveryRejection::BodyRefused(refusal)),
            "{text:?}"
        );
        assert!(prompts(&log).is_empty(), "{text:?}");
    }
}

#[test]
fn a_path_and_a_later_command_line_are_ordinary_text() {
    for text in ["/home/li/primary is the root", "see below\n/compact"] {
        let fixture = NexusFixture::new();
        fixture.accept_pane_operations(
            vec![fixture.settled_agent()],
            PromptFixture::Prompted("w1:p3"),
        );
        fixture.register_with(FlowLifecycle::Active);
        assert_eq!(
            fixture.nexus.dispatch_meta(MetaQuery::Vet(DeliveryRequest {
                delivery_id: "vet".into(),
                flow_id: "908786".into(),
                message: middle_abrupt(text),
            })),
            MetaResponse::Vetted("908786".into())
        );
        assert_eq!(
            fixture.send(text),
            Sent::Graded(DeliveryGrade::Presented),
            "{text:?}"
        );
        assert_eq!(fixture.typed(), Some(rendered(text)));
    }
}

#[test]
fn a_repeated_delivery_answers_its_outcome_and_types_nothing_more() {
    let fixture = NexusFixture::new();
    let log = fixture.accept_pane_operations(
        vec![fixture.settled_agent()],
        PromptFixture::Prompted("w1:p3"),
    );
    fixture.register_with(FlowLifecycle::Active);
    let first = fixture.deliver("once", middle_abrupt("only once"));
    let second = fixture.deliver("once", middle_abrupt("only once"));
    assert!(matches!(first, MetaResponse::Delivered(_)));
    assert_eq!(first, second);
    assert_eq!(prompts(&log).len(), 1);
}

#[test]
fn a_delivery_a_crash_left_under_its_lease_settles_uncertain_and_is_never_retried() {
    use crate::store::delivery::{LeaseStep, PaneLease, RecordsDeliveries};
    use crate::store::{FlowStore, OpensFlowStore};
    let directory = tempfile::tempdir().expect("store directory");
    let path = directory.path().join("flow.sema");
    {
        let store = FlowStore::open(&path).expect("store");
        store
            .record_lease_step(PaneLease {
                delivery_id: "crashed".into(),
                flow_id: "908786".into(),
                herdr_pane_id: "w1:p3".into(),
                lease_step: LeaseStep::Placed,
            })
            .expect("lease row");
    }
    let store = FlowStore::open(&path).expect("reopened store");
    assert_eq!(
        store.delivery("crashed").expect("delivery read"),
        Some(Delivery {
            delivery_id: "crashed".into(),
            flow_id: "908786".into(),
            interrupt_witness: InterruptWitness::Unobserved,
            delivery_grade: DeliveryGrade::Uncertain,
        })
    );
    assert!(
        store
            .settle_interrupted_deliveries()
            .expect("no rows left")
            .is_empty()
    );
}

#[test]
fn two_deliveries_to_one_pane_never_interleave() {
    let fixture: &'static NexusFixture = Box::leak(Box::new(NexusFixture::new()));
    fixture.accept_pane_operations(
        vec![fixture.settled_agent()],
        PromptFixture::Prompted("w1:p3"),
    );
    fixture.register_with(FlowLifecycle::Active);
    // Each prompt writes begin, pauses, writes end.
    let journal = fixture.directory.path().join("journal");
    let body = fs::read_to_string(&fixture.snapshot_program).expect("Herdr fixture");
    let body = body.replacen(
        "\"agent prompt\") ",
        &format!(
            "\"agent prompt\") printf 'begin %s\\n' \"$6\" >> '{0}'; sleep 0.2; printf 'end %s\\n' \"$6\" >> '{0}'; ",
            journal.display()
        ),
        1,
    );
    FixtureExecutable {
        path: fixture.snapshot_program.clone(),
    }
    .install(&body);
    let writers: Vec<_> = ["first", "second", "third"]
        .into_iter()
        .map(|text| std::thread::spawn(move || fixture.send(text)))
        .collect();
    for writer in writers {
        assert!(matches!(writer.join().unwrap(), Sent::Graded(_)));
    }
    let lines: Vec<String> = fs::read_to_string(journal)
        .unwrap()
        .lines()
        .map(str::to_owned)
        .collect();
    assert_eq!(lines.len(), 6);
    for pair in lines.chunks(2) {
        assert!(pair[0].starts_with("begin "), "{lines:?}");
        assert_eq!(pair[1], pair[0].replacen("begin", "end", 1), "{lines:?}");
    }
}

#[test]
fn a_pending_flow_seen_reacting_to_a_delivery_is_active() {
    let fixture = NexusFixture::new();
    fixture.accept_pane_operations(
        vec![fixture.settled_agent()],
        PromptFixture::Prompted("w1:p3"),
    );
    fixture.register_with(FlowLifecycle::Pending);
    assert_eq!(
        fixture.send("hello"),
        Sent::Graded(DeliveryGrade::Presented)
    );
    assert_eq!(fixture.stored_lifecycle(), FlowLifecycle::Active);
}

#[test]
fn commands_interrupt_a_working_agent_and_compact_through_the_writer() {
    let fixture = NexusFixture::new();
    let log = fixture.accept_pane_operations(
        vec![fixture.current_agent()],
        PromptFixture::Prompted("w1:p3"),
    );
    fixture.register_with(FlowLifecycle::Active);
    assert_eq!(
        fixture
            .nexus
            .dispatch_meta(MetaQuery::Command(CommandRequest {
                flow_id: "908786".into(),
                harness_command: HarnessCommand::Interrupt,
            })),
        MetaResponse::Commanded(CommandOutcome {
            flow_id: "908786".into(),
            harness_command: HarnessCommand::Interrupt,
            command_grade: CommandGrade::Observed,
        })
    );
    assert_eq!(
        fixture
            .nexus
            .dispatch_meta(MetaQuery::Command(CommandRequest {
                flow_id: "908786".into(),
                harness_command: HarnessCommand::Compact,
            })),
        MetaResponse::Commanded(CommandOutcome {
            flow_id: "908786".into(),
            harness_command: HarnessCommand::Compact,
            command_grade: CommandGrade::Transported,
        })
    );
    assert_eq!(
        prompts(&log),
        vec!["--session messaging-build agent prompt w1:p3 /compact".to_owned()]
    );

    let resting = NexusFixture::new();
    resting.accept_pane_operations(
        vec![resting.settled_agent()],
        PromptFixture::Prompted("w1:p3"),
    );
    resting.register_with(FlowLifecycle::Active);
    assert_eq!(
        resting
            .nexus
            .dispatch_meta(MetaQuery::Command(CommandRequest {
                flow_id: "908786".into(),
                harness_command: HarnessCommand::Interrupt,
            })),
        MetaResponse::CommandRejected(CommandRejection::NotDelivered),
        "nothing to interrupt, and no Escape into a resting agent"
    );
    assert!(!resting.operations().contains("send-keys"));
}

#[test]
fn observe_agent_opens_on_the_state_herdr_shows() {
    let fixture = NexusFixture::new();
    fixture.accept_pane_operations(
        vec![fixture.current_agent()],
        PromptFixture::Prompted("w1:p3"),
    );
    fixture.register_with(FlowLifecycle::Active);
    assert_eq!(
        fixture
            .nexus
            .dispatch(Query::Observe(ObserveSelection::Agent("908786".into()))),
        Response::AgentObserved(AgentObservation {
            flow_id: "908786".into(),
            agent_state: AgentState::Working,
        })
    );
    assert_eq!(
        fixture
            .nexus
            .dispatch(Query::Observe(ObserveSelection::Agent("unheld".into()))),
        Response::AgentObserved(AgentObservation {
            flow_id: "unheld".into(),
            agent_state: AgentState::Gone,
        })
    );
}

#[test]
fn the_meta_socket_admits_the_owner_and_refuses_an_unreadable_peer() {
    let fixture = NexusFixture::new();
    fixture.set_agents(vec![]);
    assert_eq!(
        fixture.nexus.meta_refusal(None),
        Some(meta_signal_flow::MetaRefusal::PeerUnknown)
    );
    // This test process runs in no flow's pane: it is the owner.
    let own = crate::caller::CallerProcess {
        process_id: std::process::id(),
    };
    assert_eq!(fixture.nexus.meta_refusal(Some(own)), None);
}

#[test]
fn a_flow_outside_meta_aspects_is_refused_the_meta_socket() {
    let fixture = NexusFixture::new();
    super::bind_existing_caller(&fixture, "mind-live", "pane-1");
    fixture.set_agents(vec![super::codex_agent_in("pane-1", "terminal-pane-1")]);
    let peer = PanePeer::spawn("pane-1");
    let refusal = fixture
        .nexus
        .meta_refusal(Some(crate::caller::CallerProcess {
            process_id: peer.0.id(),
        }));
    assert_eq!(
        refusal,
        Some(meta_signal_flow::MetaRefusal::PeerNotAuthorized(
            super::mind_live()
        ))
    );
}

#[test]
fn a_flow_whose_role_flow_does_not_know_is_not_taken_for_the_owner() {
    let fixture = NexusFixture::new();
    fixture.accept_pane_operations(
        vec![fixture.current_agent()],
        PromptFixture::Prompted("w1:p3"),
    );
    // Registered through RegisterFlow, which records no role.
    fixture.register_with(FlowLifecycle::Active);
    let peer = PanePeer::spawn("w1:p3");
    assert_eq!(
        fixture
            .nexus
            .meta_refusal(Some(crate::caller::CallerProcess {
                process_id: peer.0.id(),
            })),
        Some(meta_signal_flow::MetaRefusal::PeerUnknown)
    );
}

/// A process marked as running in a pane of the fixture session. `sleep` is
/// spawned directly rather than through a shell that execs it, and its marks
/// are waited for before it is used as a peer.
struct PanePeer(std::process::Child);

impl PanePeer {
    fn spawn(pane: &str) -> Self {
        let child = std::process::Command::new("sleep")
            .arg("30")
            .env("HERDR_SESSION", "messaging-build")
            .env("HERDR_PANE_ID", pane)
            .spawn()
            .expect("marked peer");
        // A peer is only a peer once its marks are readable: see
        // SettlesItsMarks, and why exec alone does not settle them.
        super::SettlesItsMarks::settled_pane(&crate::caller::CallerProcess {
            process_id: child.id(),
        });
        Self(child)
    }
}

impl Drop for PanePeer {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
