//! Deliver, Vet, Command and Observe.Agent against a fixture Herdr that
//! logs every call. The fixture's answers are Herdr 0.8.2's witnessed
//! shapes; what each test expects is written out by hand.

use super::{
    ControlsHerdrSnapshot, NexusFixture, PromptFixture, Sent, adoption_resolver, existing_binding,
    flow_container, middle_abrupt, prompts, rendered, with_bound_world,
};
use crate::fixture_executable::{FixtureExecutable, InstallsScript};
use crate::{Dispatches, peer::AdmitsMetaPeer};
use meta_signal_flow::{
    BodyRefusal, CommandGrade, CommandOutcome, CommandRejection, CommandRequest, Content, Delivery,
    DeliveryGrade, DeliveryRejection, DeliveryRequest, HarnessCommand, InterruptWitness, Letter,
    Message, Query as MetaQuery, Response as MetaResponse,
};
use signal_flow::{AgentObservation, AgentState, FlowLifecycle, ObserveSelection, Query, Response};
use std::fs;

fn letter(text: &str) -> Letter {
    Letter {
        visible_sender: signal_flow::VisibleSender::Notification,
        content: Content::Text(text.into()),
    }
}

impl NexusFixture {
    fn deliver(&self, delivery_id: &str, message: Message) -> MetaResponse {
        self.deliver_to("908786", delivery_id, message)
    }

    fn deliver_to(&self, flow_id: &str, delivery_id: &str, message: Message) -> MetaResponse {
        self.nexus
            .dispatch_meta(MetaQuery::Deliver(DeliveryRequest {
                delivery_id: delivery_id.into(),
                flow_id: flow_id.into(),
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

    /// The fixture flow Awake as { Psyche flow Opus }, running
    /// claude-opus-5-5, whose refresh threshold is `refresh` tokens.
    fn at_secondary_refreshing_at(&self, refresh: i64) {
        use crate::store::ManagesMetaflowState as _;
        use crate::store::SeedsRole as _;
        use crate::store::metaflow::SeedsMetaflow as _;
        self.nexus.store.seed_role(signal_flow::Caller {
            flow_id: "908786".into(),
            aspect: signal_flow::Aspect::Psyche,
            model_name: "claude-opus-5-5".into(),
        });
        self.nexus.store.seed_metaflow(signal_flow::Metaflow {
            address: signal_flow::Address {
                aspect: signal_flow::Aspect::Psyche,
                topic: "flow".into(),
                model: signal_flow::Model::Opus,
            },
            state: flow_ethos::State::Awake("908786".into()),
            past: vec![],
            queue: vec![],
        });
        self.nexus
            .store
            .configure_change(meta_signal_flow::ConfigurationChange::Thresholds(vec![
                meta_signal_flow::ConfigureThreshold {
                    model_name: "claude-opus-5-5".into(),
                    refresh,
                },
            ]))
            .expect("threshold configured");
    }

    fn command(&self, harness_command: HarnessCommand) -> MetaResponse {
        self.nexus.dispatch_meta(MetaQuery::Command(CommandRequest {
            flow_id: "908786".into(),
            harness_command,
        }))
    }
}

/// A Soft Send waits for an idle recipient. To a seat in its turn nothing
/// is typed and the request stays queued, accepted; a seat whose turn has
/// ended has had its idle event, and the same Send is typed at once, without
/// an interrupt.
#[test]
fn soft_waits_for_an_idle_recipient() {
    use super::waking::{send_soft, soft_recipient, stored};
    let fixture = NexusFixture::new();
    let (_caller, peer) = soft_recipient(&fixture, false);
    let response = send_soft(&fixture, &peer, "when you rest");
    assert!(
        matches!(response, Response::SendAccepted(_)),
        "{response:?}"
    );
    assert!(prompts(&fixture.directory.path().join("herdr-operations.log")).is_empty());
    assert_eq!(fixture.typed(), None);
    assert_eq!(
        stored(&fixture, signal_flow::Model::Sol).queue.len(),
        1,
        "held for the idle event"
    );
    assert_eq!(
        stored(&fixture, signal_flow::Model::Sol).queue[0].message,
        signal_flow::Message::Soft(signal_flow::Request::Order("when you rest".into()))
    );

    let fixture = NexusFixture::new();
    let (_caller, peer) = soft_recipient(&fixture, true);
    send_soft(&fixture, &peer, "when you rest");
    let typed = fixture.typed().expect("typed into the resting seat");
    assert!(typed.contains("when you rest"), "{typed}");
    assert!(
        !fixture.operations().contains("send-keys"),
        "no interrupt: {}",
        fixture.operations()
    );
    assert!(stored(&fixture, signal_flow::Model::Sol).queue.is_empty());
}

/// Both native harnesses use one pane delivery path at every priority and
/// seat state. Hard alone interrupts a Working recipient; every other case
/// places the prompt without an interrupt.
#[test]
fn both_harnesses_deliver_every_priority_and_only_hard_interrupts_working() {
    use crate::store::RegistersFlowIdentity;
    for harness_kind in [
        signal_flow::HarnessKind::Claude,
        signal_flow::HarnessKind::Codex,
    ] {
        for working in [false, true] {
            for (priority, message) in [
                ("hard", Message::HardAbrupt(letter("hard"))),
                ("middle", Message::MiddleAbrupt(letter("middle"))),
                ("soft", Message::Soft(letter("soft"))),
            ] {
                let fixture = NexusFixture::new();
                let (flow_id, mut agent) = if harness_kind == signal_flow::HarnessKind::Claude {
                    super::waking::claude_seat(&fixture, 40_000);
                    (
                        super::waking::CLAUDE_SEAT,
                        super::waking::claude_agent(&fixture),
                    )
                } else {
                    fixture
                        .nexus
                        .store
                        .register_flow(fixture.node())
                        .expect("delivery target registered");
                    ("908786", fixture.current_agent())
                };
                agent["agent_status"] =
                    serde_json::Value::String(if working { "working" } else { "idle" }.into());
                let harness_name = match harness_kind {
                    signal_flow::HarnessKind::Claude => "claude",
                    signal_flow::HarnessKind::Codex => "codex",
                };
                fixture.accept_pane_operations(vec![agent], PromptFixture::Prompted("w1:p3"));

                let response = fixture.deliver_to(
                    flow_id,
                    &format!("{harness_name}-{working}-{priority}"),
                    message,
                );
                let MetaResponse::Delivered(delivery) = response else {
                    panic!("{harness_name} {working} {priority}: {response:?}")
                };
                let expected = if working && priority == "hard" {
                    InterruptWitness::Observed
                } else {
                    InterruptWitness::NotRequested
                };
                assert_eq!(
                    delivery.interrupt_witness, expected,
                    "{harness_name} {working} {priority}"
                );
                assert!(
                    fixture
                        .typed()
                        .is_some_and(|typed| typed.contains(priority)),
                    "{harness_name} {working} {priority}"
                );
            }
        }
    }
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

/// A draft in a working recipient's composer refuses a HardAbrupt before
/// any key: an interrupt would only cancel the turn the recipient is in
/// (Psyche core Secondary cdbe86, 2026-10-23: each queued HardAbrupt
/// cancelled the living's newest prompt and was refused all the same).
#[test]
fn hard_abrupt_refuses_an_occupied_composer_before_any_key() {
    let fixture = NexusFixture::new();
    fixture.accept_pane_operations(
        vec![fixture.current_agent()],
        PromptFixture::Prompted("w1:p3"),
    );
    fixture.show_composer("\u{1b}[1m›\u{1b}[0m half a thought");
    fixture.register_with(FlowLifecycle::Active);
    assert_eq!(
        fixture.deliver("hard-draft", Message::HardAbrupt(letter("stop now"))),
        MetaResponse::DeliveryRejected(DeliveryRejection::ComposerOccupied)
    );
    let operations = fixture.operations();
    assert!(
        !operations.contains("send-keys"),
        "no interrupt: {operations}"
    );
    assert!(
        !operations.contains("agent prompt"),
        "draft preserved: {operations}"
    );
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
        MetaResponse::Delivered(meta_signal_flow::VisibleDelivery {
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
            "--session messaging-build agent prompt w1:p3 HardAbrupt.{ Notification Text.«stop the build» }".to_owned(),
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
            "--session messaging-build agent prompt w1:p3 HardAbrupt.{ Notification Text.«stop now» }",
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
            BodyRefusal::ControlCharacter("MiddleAbrupt.{ Notification Text.«end ".len() as i64),
        ),
        (
            "one\rtwo",
            BodyRefusal::ControlCharacter("MiddleAbrupt.{ Notification Text.«one".len() as i64),
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
            MetaResponse::Vetted
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
fn a_recorded_delivery_answers_while_refresh_custody_is_held_without_input() {
    use crate::waking::HoldsWakingMetaflows;
    use signal_flow::{Address, Aspect, Model};
    let fixture = NexusFixture::new();
    let log = fixture.accept_pane_operations(
        vec![fixture.settled_agent()],
        PromptFixture::Prompted("w1:p3"),
    );
    fixture.register_with(FlowLifecycle::Active);
    fixture.at_secondary_refreshing_at(200_000);
    let first = fixture.deliver("refresh-receipt", middle_abrupt("only once"));
    assert!(matches!(first, MetaResponse::Delivered(_)));
    let address = Address {
        aspect: Aspect::Psyche,
        topic: "flow".into(),
        model: Model::Opus,
    };
    let lease = fixture
        .nexus
        .waking
        .hold(&address)
        .expect("refresh owns the address");
    let second = fixture.deliver("refresh-receipt", middle_abrupt("only once"));
    assert_eq!(first, second);
    drop(lease);
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
    fixture.at_secondary_refreshing_at(200_000);
    {
        use crate::store::context::RecordsContextSize as _;
        fixture
            .nexus
            .store
            .record_context_size("908786", 230_000)
            .expect("context size recorded");
    }
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

/// Compaction is an emergency measure: a Compact is refused, before any
/// key reaches the pane, until the flow's last measured context reaches the
/// refresh threshold of its model; at the threshold it is placed.
#[test]
fn compact_is_refused_below_the_refresh_threshold_and_placed_at_it() {
    use crate::store::context::RecordsContextSize as _;
    use meta_signal_flow::{CompactionRefusal, ContextBelowThreshold};
    let fixture = NexusFixture::new();
    let log = fixture.accept_pane_operations(
        vec![fixture.settled_agent()],
        PromptFixture::Prompted("w1:p3"),
    );
    fixture.register_with(FlowLifecycle::Active);
    let refused =
        |refusal| MetaResponse::CommandRejected(CommandRejection::CompactionRefused(refusal));

    // Flow knows no model for the flow.
    assert_eq!(
        fixture.command(HarnessCommand::Compact),
        refused(CompactionRefusal::NoThreshold)
    );
    fixture.at_secondary_refreshing_at(200_000);
    assert_eq!(
        fixture.command(HarnessCommand::Compact),
        refused(CompactionRefusal::Unmeasured),
        "no Stop has recorded the flow's context"
    );
    fixture
        .nexus
        .store
        .record_context_size("908786", 199_999)
        .expect("context size recorded");
    assert_eq!(
        fixture.command(HarnessCommand::Compact),
        refused(CompactionRefusal::BelowThreshold(ContextBelowThreshold {
            context_tokens: 199_999,
            refresh_tokens: 200_000,
        }))
    );
    assert!(prompts(&log).is_empty(), "nothing reached the pane");
    assert!(!fixture.operations().contains("send-keys"));

    fixture
        .nexus
        .store
        .record_context_size("908786", 200_000)
        .expect("context size recorded");
    assert_eq!(
        fixture.command(HarnessCommand::Compact),
        MetaResponse::Commanded(CommandOutcome {
            flow_id: "908786".into(),
            harness_command: HarnessCommand::Compact,
            command_grade: CommandGrade::Observed,
        })
    );
    assert_eq!(
        prompts(&log),
        vec![
            "--session messaging-build agent prompt w1:p3 /compact --wait --until working --until idle --until done --until blocked --timeout 10000"
                .to_owned()
        ]
    );
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
    super::bind_existing_caller(&fixture, "a001", "pane-1");
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

/// A process marked as running in a pane of the fixture session, in Herdr's
/// default-session shape: `HERDR_PANE_ID` alone, under the test process
/// standing as the Herdr server. `sleep` is spawned directly rather than
/// through a shell that execs it, and its mark is waited for before it is
/// used as a peer.
struct PanePeer(std::process::Child);

impl PanePeer {
    fn spawn(pane: &str) -> Self {
        let child = std::process::Command::new("sleep")
            .arg("30")
            .env_remove("HERDR_SESSION")
            .env("HERDR_PANE_ID", pane)
            .spawn()
            .expect("marked peer");
        // A peer is only a peer once its marks are readable: see
        // SettlesItsMarks, and why exec alone does not settle them.
        super::SettlesItsMarks::settled_pane_id(&crate::caller::CallerProcess {
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

/// A flow past its model's refresh threshold continues receiving ordinary
/// delivery until an actual refresh is in motion.
#[test]
fn a_deliver_into_a_flow_past_its_threshold_reaches_the_pane() {
    use crate::store::context::RecordsContextSize as _;
    let fixture = NexusFixture::new();
    let log = fixture.accept_pane_operations(
        vec![fixture.settled_agent()],
        PromptFixture::Prompted("w1:p3"),
    );
    fixture.register_with(FlowLifecycle::Active);
    fixture.at_secondary_refreshing_at(200_000);
    fixture
        .nexus
        .store
        .record_context_size("908786", 200_000)
        .expect("context size recorded");
    assert!(matches!(
        fixture
            .nexus
            .dispatch_meta(MetaQuery::Deliver(meta_signal_flow::DeliveryRequest {
                delivery_id: "delivery-past".into(),
                flow_id: "908786".into(),
                message: middle_abrupt("one more thing"),
            })),
        MetaResponse::Delivered(_)
    ));
    assert!(!prompts(&log).is_empty(), "payload reached the pane");
    assert!(matches!(
        fixture
            .nexus
            .dispatch_meta(MetaQuery::Deliver(meta_signal_flow::DeliveryRequest {
                delivery_id: "warning-past".into(),
                flow_id: "908786".into(),
                message: Message::HardAbrupt(letter("warning")),
            })),
        MetaResponse::Delivered(_)
    ));
    assert!(
        prompts(&log)
            .iter()
            .any(|prompt| prompt.contains("warning"))
    );
}

#[test]
fn direct_delivery_is_refused_while_refresh_owns_address() {
    use crate::waking::HoldsWakingMetaflows;
    use signal_flow::{Address, Aspect, Model};
    let fixture = NexusFixture::new();
    let log = fixture.accept_pane_operations(
        vec![fixture.settled_agent()],
        PromptFixture::Prompted("w1:p3"),
    );
    fixture.register_with(FlowLifecycle::Active);
    fixture.at_secondary_refreshing_at(200_000);
    let address = Address {
        aspect: Aspect::Psyche,
        topic: "flow".into(),
        model: Model::Opus,
    };
    let lease = fixture
        .nexus
        .waking
        .hold(&address)
        .expect("refresh custody");
    assert!(matches!(
        fixture.deliver("held-ordinary", middle_abrupt("held")),
        MetaResponse::DeliveryRejected(DeliveryRejection::RouteUnavailable)
    ));
    assert!(matches!(
        fixture.deliver("held-warning", Message::HardAbrupt(letter("warning"))),
        MetaResponse::DeliveryRejected(DeliveryRejection::RouteUnavailable)
    ));
    assert!(prompts(&log).is_empty());
    drop(lease);
    assert!(matches!(
        fixture.deliver("released", middle_abrupt("released")),
        MetaResponse::Delivered(_)
    ));
    assert_eq!(prompts(&log).len(), 1);
}

/// Direct Deliver owns the address before it enters Herdr.  A concurrent
/// refresh therefore observes Locked while the pane operation is blocked,
/// rather than closing the predecessor beneath an in-flight delivery.
#[test]
fn direct_delivery_custody_blocks_refresh_until_pane_input_finishes() {
    use crate::DispatchesSerially;
    use crate::fixture_executable::{FixtureExecutable, InstallsScript};
    use signal_flow::{Origin, Query, RefreshRejection};
    use std::time::{Duration, Instant};

    let fixture: &'static NexusFixture = Box::leak(Box::new(NexusFixture::new()));
    fixture.accept_pane_operations(
        vec![fixture.settled_agent()],
        PromptFixture::Prompted("w1:p3"),
    );
    fixture.register_with(FlowLifecycle::Active);
    fixture.at_secondary_refreshing_at(200_000);
    let reached = fixture.directory.path().join("direct-prompt-reached");
    let release = fixture.directory.path().join("direct-prompt-release");
    let body = fs::read_to_string(&fixture.snapshot_program).expect("Herdr fixture");
    let pattern = "\"agent prompt\") ";
    assert!(body.contains(pattern), "{body}");
    FixtureExecutable {
        path: fixture.snapshot_program.clone(),
    }
    .install(&body.replacen(
        pattern,
        &format!(
            "\"agent prompt\") : > '{}'; while [ ! -e '{}' ]; do sleep 0.02; done; ",
            reached.display(),
            release.display(),
        ),
        1,
    ));

    let address = signal_flow::Address {
        aspect: signal_flow::Aspect::Psyche,
        topic: "flow".into(),
        model: signal_flow::Model::Opus,
    };
    let (delivery, refresh) = std::thread::scope(|scope| {
        let delivery =
            scope.spawn(|| fixture.deliver("custody-race", middle_abrupt("custody race")));
        let deadline = Instant::now() + Duration::from_secs(20);
        while !reached.exists() {
            assert!(Instant::now() < deadline, "direct input never started");
            std::thread::sleep(Duration::from_millis(10));
        }
        let refresh = fixture
            .nexus
            .dispatch_serially(Query::Refresh(address.clone()), Origin::Unbound.into());
        assert_eq!(
            refresh,
            signal_flow::Response::RefreshRejected(RefreshRejection::Locked)
        );
        fs::write(&release, "").expect("release direct pane input");
        (delivery.join().expect("direct delivery"), refresh)
    });
    assert!(matches!(delivery, meta_signal_flow::Response::Delivered(_)));
    assert_eq!(
        refresh,
        signal_flow::Response::RefreshRejected(RefreshRejection::Locked)
    );
    assert_eq!(
        fixture.typed(),
        Some("MiddleAbrupt.{ Notification Text.«custody race» }".into())
    );
    let after = fixture
        .nexus
        .dispatch_serially(Query::Refresh(address), Origin::Unbound.into());
    assert!(!matches!(
        after,
        signal_flow::Response::RefreshRejected(RefreshRejection::Locked)
    ));
}

/// The public meta dispatcher serializes a direct Deliver and a real
/// MetaBindExisting.  Binding cannot create custody while pane input is in
/// flight; once that input completes, the same public route performs the
/// verified binding.
#[test]
fn public_direct_delivery_finishes_before_a_concurrent_existing_bind() {
    use crate::DispatchesSerially;
    use crate::fixture_executable::{FixtureExecutable, InstallsScript};
    use std::os::unix::net::UnixListener;
    use std::time::{Duration, Instant};

    let fixture: &'static NexusFixture = Box::leak(Box::new(NexusFixture::new()));
    let mut binding = existing_binding("908786", "w1:p3");
    binding.native_session_id = "01a0b22c-e24f-7452-9940-64490878680f".into();
    binding.herdr_workspace_id = "w1".into();
    binding.herdr_tab_id = "w1:t1".into();
    binding.herdr_terminal_id = "term_65bb7f87270cb3".into();
    binding.herdr_agent_name = "psyche-mind-astra".into();
    let socket_path = fixture.directory.path().join("serial-bind.sock");
    let _listener = UnixListener::bind(&socket_path).expect("live fixture container");
    let reached = fixture.directory.path().join("serial-deliver-reached");
    let release = fixture.directory.path().join("serial-deliver-release");

    struct ReleasesPublicDelivery(std::path::PathBuf);

    impl Drop for ReleasesPublicDelivery {
        fn drop(&mut self) {
            let _ = fs::write(&self.0, "");
        }
    }

    let (delivered, bound) = with_bound_world(fixture, &[&binding], || {
        // Registration validates the native claim and live pane supplied by
        // this world; doing it before the world exists is truthfully refused.
        fixture.register_with(FlowLifecycle::Active);
        let body = fs::read_to_string(&fixture.snapshot_program).expect("bound Herdr world");
        let pattern = "  *) exit 64 ;;";
        assert!(body.contains(pattern), "{body}");
        FixtureExecutable {
            path: fixture.snapshot_program.clone(),
        }
        .install(&body.replacen(
            pattern,
            &format!(
                "  \"agent read\") printf '%s\\n' '› ' ;;\n  \"agent prompt\") : > '{reached}'; while [ ! -e '{release}' ]; do sleep 0.02; done; printf '%s' \"$6\" > '{typed}'; printf '%s\\n' '{{\"id\":\"cli:agent:prompt\",\"result\":{{\"type\":\"agent_prompted\",\"agent\":{{\"pane_id\":\"w1:p3\"}}}}}}' ;;\n{pattern}",
                reached = reached.display(),
                release = release.display(),
                typed = fixture.directory.path().join("pane-typed.txt").display(),
            ),
            1,
        ));
        std::thread::scope(|scope| {
            let _release_before_join = ReleasesPublicDelivery(release.clone());
            let (delivery_answered, observe_delivery_answer) = std::sync::mpsc::channel();
            let delivery_fixture: &'static NexusFixture = fixture;
            let delivering = scope.spawn(move || {
                let response = delivery_fixture.nexus.dispatch_meta_serially(
                    MetaQuery::Deliver(DeliveryRequest {
                        delivery_id: "public-serial-delivery".into(),
                        flow_id: "908786".into(),
                        message: middle_abrupt("serialized delivery"),
                    }),
                    None,
                );
                delivery_answered
                    .send(format!("{response:?}"))
                    .expect("Deliver response observed");
                response
            });
            let deadline = Instant::now() + Duration::from_secs(20);
            while !reached.exists() {
                if let Ok(response) = observe_delivery_answer.try_recv() {
                    panic!("public Deliver answered before Herdr input: {response}");
                }
                assert!(
                    Instant::now() < deadline,
                    "public Deliver never entered Herdr"
                );
                std::thread::sleep(Duration::from_millis(10));
            }
            assert!(fixture.nexus.dispatch_gate.try_lock().is_err());
            let binding_for_thread = binding.clone();
            let socket_for_thread = socket_path.clone();
            let (attempted, observe_attempt) = std::sync::mpsc::channel();
            let binding_attempt = scope.spawn(move || {
                attempted.send(()).expect("Bind attempt observed");
                fixture.nexus.dispatch_meta_serially(
                    MetaQuery::MetaBindExisting(meta_signal_flow::MetaBindExisting {
                        flow_container: flow_container(&socket_for_thread),
                        address: signal_flow::Address {
                            aspect: signal_flow::Aspect::Mind,
                            topic: "core".into(),
                            model: signal_flow::Model::Sol,
                        },
                        flow_binding: binding_for_thread,
                    }),
                    Some(adoption_resolver()),
                )
            });
            observe_attempt
                .recv_timeout(Duration::from_secs(2))
                .expect("Bind reached public dispatcher");
            assert!(fixture.nexus.dispatch_gate.try_lock().is_err());
            fs::write(&release, "").expect("release public Deliver");
            (
                delivering.join().expect("Deliver answered"),
                binding_attempt.join().expect("Bind answered"),
            )
        })
    });
    assert!(
        matches!(delivered, MetaResponse::Delivered(_)),
        "{delivered:?}"
    );
    assert!(
        matches!(bound, MetaResponse::BoundExisting(ref answer)
            if matches!(answer.flow_binding_result, meta_signal_flow::FlowBindingResult::Bound(_))),
        "{bound:?}"
    );
    assert_eq!(
        fixture.typed(),
        Some("MiddleAbrupt.{ Notification Text.«serialized delivery» }".into())
    );
}
