//! The override panel at the root of the meta Signal: each override does what it names
//! and records its transition, the cause Overridden; Hold stands the
//! Nexus's clocks, deliveries and launches while every query still answers,
//! and Resume takes up what is owed.

use super::refreshing::refreshable_with_successor;
use super::waking::{
    CLAUDE_SEAT, ReadsFreshWorld, WORKING_CODEX_THREAD, WakesFreshClaude, address,
    append_main_rows, claude_agent, claude_seat, codex_seat_in_fresh_world, end_turn, ended_claude,
    maintain, metaflow, stored,
};
use super::{ControlsHerdrSnapshot, NexusFixture, PromptFixture};
use crate::Dispatches;
use crate::generated::memory::{Cause, Transition};
use crate::generated::overriding::{Holding, RemembersOverrides};
use crate::reminding::RemindsCallers;
use crate::store::logical_delivery::RecordsLogicalDeliveries;
use crate::store::metaflow::{SeedsMetaflow, SleepsMetaflows};
use crate::store::reminder::RecordsReminders;
use crate::store::{ManagesMetaflowState, ReadsFlowRows};
use crate::waking::{AtStop, SleepsAndWakesSeats};
use flow_ethos::State;
use meta_signal_flow as meta;
use signal_flow::{FlowLifecycle, Message, Metaflow, Model, PrivateSendResponse, Request, SentBy};

/// One override, an input at the root of the meta Signal, and its answer.
fn overridden(fixture: &NexusFixture, input: meta::Input) -> meta::Output {
    fixture.nexus.dispatch_meta(input)
}

fn seat_id() -> flow_ethos::FlowId {
    flow_ethos::FlowId(CLAUDE_SEAT.into())
}

/// The override transitions Flow's Memory holds for `flow_id`, oldest
/// first: those with the cause Overridden.
fn transitions(fixture: &NexusFixture, flow_id: &str) -> Vec<Transition> {
    fixture
        .nexus
        .store
        .transitions()
        .unwrap()
        .into_iter()
        .find(|flow| flow.flow_id.0 == flow_id)
        .map(|flow| flow.transition_vector)
        .unwrap_or_default()
        .into_iter()
        .filter(|transition| transition.cause == Cause::Overridden)
        .collect()
}

/// The last transition held for `flow_id` is the override's, into `state`,
/// at the seat's Sol Address.
fn overridden_into(fixture: &NexusFixture, flow_id: &str, state: meta::State) {
    let held = transitions(fixture, flow_id);
    let last = held.last().expect("the override's transition is recorded");
    assert_eq!(last.state, state, "{held:?}");
    assert_eq!(last.cause, Cause::Overridden, "{held:?}");
    assert!(last.moment.0 > 0, "the moment is taken: {held:?}");
    let flow = fixture
        .nexus
        .store
        .transitions()
        .unwrap()
        .into_iter()
        .find(|flow| flow.flow_id.0 == flow_id)
        .unwrap();
    assert_eq!(flow.address, address(Model::Sol));
}

/// The Claude seat Awake at the Sol Address, resting in its live pane, its
/// last report its rest, its metaflow Maintained.
fn resting_seat(fixture: &NexusFixture) {
    fixture.accept_pane_operations(
        vec![claude_agent(fixture)],
        PromptFixture::Prompted("w1:p3"),
    );
    claude_seat(fixture, 40_000);
    append_main_rows(fixture, &[end_turn("idle-turn")]);
    fixture.nexus.store.seed_metaflow(metaflow(
        Model::Sol,
        State::Awake(seat_id()),
        vec![],
        vec![],
    ));
    maintain(fixture, address(Model::Sol));
    use crate::generated::transitions::{ReportedEvent, ReportsEvents};
    fixture.nexus.report_event(ReportedEvent {
        flow_id: seat_id(),
        event: crate::generated::memory::Event::Idle(signal_flow::Tokens(40_000)),
    });
}

/// An Order queued for the seat, from Flow.
fn queue_order(fixture: &NexusFixture, text: &str) {
    assert!(matches!(
        fixture.nexus.store.accept_logical_delivery(
            &address(Model::Sol),
            SentBy::Flow,
            flow_ethos::Delivery::Wake,
            Message::MiddleAbrupt(Request::Order(text.into())),
            None,
        ),
        Ok(PrivateSendResponse::SendAccepted(_))
    ));
}

#[test]
fn set_state_corrects_the_state_held_and_records_its_transition() {
    let fixture = NexusFixture::new();
    resting_seat(&fixture);

    assert_eq!(
        overridden(
            &fixture,
            meta::Input::SetState(meta::SetState_Data {
                flow_id: seat_id(),
                state: meta::State::Crystallizing,
            })
        ),
        meta::Output::StateSet(meta::StateSet_Data {
            flow_id: seat_id(),
            state: meta::State::Crystallizing,
        })
    );
    overridden_into(&fixture, CLAUDE_SEAT, meta::State::Crystallizing);

    let unknown = flow_ethos::FlowId("0000ff".into());
    assert_eq!(
        overridden(
            &fixture,
            meta::Input::SetState(meta::SetState_Data {
                flow_id: unknown.clone(),
                state: meta::State::Resting,
            })
        ),
        meta::Output::Refused(meta::Refused_Data::UnknownFlow(unknown))
    );
    assert_eq!(transitions(&fixture, CLAUDE_SEAT).len(), 1);
}

#[test]
fn crystallize_now_asks_the_flow_for_its_handover_and_records_crystallizing() {
    let fixture = NexusFixture::new();
    resting_seat(&fixture);
    // Held, so the ask stays queued where it can be read.
    assert_eq!(overridden(&fixture, meta::Input::Hold), meta::Output::Held);

    assert_eq!(
        overridden(&fixture, meta::Input::CrystallizeNow(seat_id())),
        meta::Output::Asked(seat_id())
    );
    overridden_into(&fixture, CLAUDE_SEAT, meta::State::Crystallizing);
    let queued = stored(&fixture, Model::Sol).queue.0;
    assert!(
        queued.iter().any(|entry| entry.sent_by == SentBy::Flow
            && matches!(
                &entry.message,
                Message::MiddleAbrupt(Request::Notice(text))
                    if text.contains(&format!("flows/{CLAUDE_SEAT}/handover.md\nRefresh.{{ "))
            )),
        "the flow is sent its handover warning: {queued:?}"
    );
    assert!(fixture.typed().is_none(), "a held Nexus types nothing");

    // A flow with no harness has nobody to ask.
    fixture
        .nexus
        .store
        .sleep_metaflow(&address(Model::Sol), CLAUDE_SEAT)
        .unwrap();
    assert_eq!(
        overridden(&fixture, meta::Input::CrystallizeNow(seat_id())),
        meta::Output::Refused(meta::Refused_Data::Unharnessed(seat_id()))
    );
}

#[test]
fn renew_starts_a_fresh_flow_and_closes_the_old_one() {
    let (fixture, _) = refreshable_with_successor(
        meta_signal_flow::Continuity::OnDemand,
        flow_ethos::Delivery::Queue,
    );

    assert_eq!(
        overridden(&fixture, meta::Input::Renew(address(Model::Sol))),
        meta::Output::Renewed(flow_ethos::FlowId("908786".into()))
    );
    super::reaping::finish_reaps(&fixture);
    let renewed = stored(&fixture, Model::Sol);
    assert_eq!(
        renewed.state,
        State::Awake(flow_ethos::FlowId("908786".into()))
    );
    assert_eq!(renewed.past, flow_ethos::Past(vec![seat_id()]));
    assert_eq!(
        fixture
            .nexus
            .store
            .flow_node(CLAUDE_SEAT)
            .unwrap()
            .unwrap()
            .flow_lifecycle,
        FlowLifecycle::Stopped
    );
    overridden_into(&fixture, CLAUDE_SEAT, meta::State::Asleep);
    // The old flow rested: no key went into its pane, which is closed.
    let calls = std::fs::read_to_string(fixture.directory.path().join("herdr-calls.log"))
        .unwrap_or_default();
    assert!(!calls.contains("send-keys"), "{calls}");
    assert!(calls.contains("pane close w1:p3"), "{calls}");

    assert_eq!(
        overridden(&fixture, meta::Input::Renew(address(Model::Haiku))),
        meta::Output::Refused(meta::Refused_Data::UnknownAddress(address(Model::Haiku)))
    );
}

/// Renew answers its fresh flow's Started: the old Claude flow, seen
/// Working, is pressed its harness's interrupt keys, and then its pane is
/// closed, so no turn of it runs on against tool calls refused FlowEnded.
#[test]
fn renew_interrupts_a_working_claude_flow_and_then_closes_its_pane() {
    let (fixture, _) = refreshable_with_successor(
        meta_signal_flow::Continuity::OnDemand,
        flow_ethos::Delivery::Queue,
    );
    let mut working = claude_agent(&fixture);
    working["agent_status"] = serde_json::Value::String("working".into());
    super::waking::claude_prompt_taken(&fixture);
    let calls = fixture.herdr_for_replacement_taking_keys(vec![working]);

    assert_eq!(
        overridden(&fixture, meta::Input::Renew(address(Model::Sol))),
        meta::Output::Renewed(flow_ethos::FlowId("908786".into()))
    );
    let calls = std::fs::read_to_string(calls).unwrap_or_default();
    let lines = calls.lines().collect::<Vec<_>>();
    let pressed = lines
        .iter()
        .position(|call| call.contains("pane send-keys w1:p3"))
        .unwrap_or_else(|| panic!("the old flow's turn is interrupted: {calls}"));
    let closed = lines
        .iter()
        .position(|call| call.contains("pane close w1:p3"))
        .unwrap_or_else(|| panic!("the old flow's pane is closed: {calls}"));
    assert!(pressed < closed, "interrupted, then closed: {calls}");
    assert!(
        !lines
            .iter()
            .any(|call| call.contains("send-keys") && call.contains("w1:p1")),
        "the fresh flow is pressed nothing: {calls}"
    );
    super::reaping::finish_reaps(&fixture);
    assert_eq!(
        stored(&fixture, Model::Sol).state,
        State::Awake(flow_ethos::FlowId("908786".into()))
    );
    overridden_into(&fixture, CLAUDE_SEAT, meta::State::Asleep);
}

/// Renew answers its fresh flow's Started: the old Codex flow's running
/// turn, which goes on in its app-server whatever its pane, is interrupted
/// there (turn/interrupt), and then its pane is closed.
#[test]
fn renew_interrupts_a_codex_flow_s_running_turn_in_its_app_server_and_closes_its_pane() {
    use super::send_tool::{RunsFakeAppServer, accepting_codex_endpoint, serve_running_turn};
    use crate::store::RegistersFlowIdentity;
    let mut fixture = NexusFixture::new();
    let world = codex_seat_in_fresh_world(&mut fixture);
    let seat = fixture
        .nexus
        .store
        .flow_node("abc123")
        .unwrap()
        .expect("the Codex seat is held");
    assert!(matches!(
        fixture.nexus.store.register_flow_in_role(
            seat,
            signal_flow::CallerIdentity {
                flow_id: flow_ethos::FlowId("abc123".into()),
                aspect: flow_ethos::Aspect::Mind,
                model_name: flow_ethos::ModelName("gpt-sol".into()),
            },
        ),
        Ok(crate::store::FlowRegistration::Registered(_))
    ));
    let fake = accepting_codex_endpoint(&mut fixture, "gpt-sol", WORKING_CODEX_THREAD);
    serve_running_turn(&fake, 1..=4, "turn-running");

    let renewed = overridden(&fixture, meta::Input::Renew(address(Model::Sol)));
    let meta::Output::Renewed(successor) = &renewed else {
        panic!("{renewed:?}; Herdr calls: {}", world.calls());
    };
    assert_ne!(successor.0, "abc123");
    assert_eq!(world.claude_starts(), 1, "{}", world.calls());
    let interrupts = (1..=fake.runs())
        .flat_map(|run| fake.written_on(run, |_| true))
        .filter(|message| message["method"] == "turn/interrupt")
        .collect::<Vec<_>>();
    assert_eq!(interrupts.len(), 1, "{interrupts:?}");
    assert_eq!(interrupts[0]["params"]["threadId"], WORKING_CODEX_THREAD);
    assert_eq!(interrupts[0]["params"]["turnId"], "turn-running");
    assert!(
        world.calls().contains("pane close w1:p3"),
        "the old flow's pane is closed: {}",
        world.calls()
    );
    assert!(
        !world.calls().contains("send-keys"),
        "a Codex turn is interrupted in its app-server, not its pane: {}",
        world.calls()
    );
}

#[test]
fn close_closes_the_flow_s_harness_and_records_it_asleep() {
    let fixture = NexusFixture::new();
    resting_seat(&fixture);

    assert_eq!(
        overridden(&fixture, meta::Input::Close(seat_id())),
        meta::Output::Closed(seat_id())
    );
    assert_eq!(
        fixture
            .nexus
            .store
            .flow_node(CLAUDE_SEAT)
            .unwrap()
            .unwrap()
            .flow_lifecycle,
        FlowLifecycle::Retired
    );
    overridden_into(&fixture, CLAUDE_SEAT, meta::State::Asleep);
}

#[test]
fn deliver_hands_the_permaflow_its_queue_now_and_records_its_transition() {
    let fixture = NexusFixture::new();
    resting_seat(&fixture);
    let mut idle = stored(&fixture, Model::Sol);
    idle.queue = metaflow(
        Model::Sol,
        State::Awake(seat_id()),
        vec![],
        vec![Request::Notice("Read the new ruling.".into())],
    )
    .queue;
    fixture.nexus.store.seed_metaflow(idle);

    assert_eq!(
        overridden(&fixture, meta::Input::Deliver(address(Model::Sol))),
        meta::Output::Delivered(address(Model::Sol))
    );
    let typed = fixture
        .typed()
        .expect("the queue is typed into the live pane");
    assert!(typed.contains("Read the new ruling."), "{typed}");
    assert_eq!(transitions(&fixture, CLAUDE_SEAT).len(), 1);
    assert_eq!(
        transitions(&fixture, CLAUDE_SEAT)[0].cause,
        Cause::Overridden
    );

    assert_eq!(
        overridden(&fixture, meta::Input::Deliver(address(Model::Haiku))),
        meta::Output::Refused(meta::Refused_Data::UnknownAddress(address(Model::Haiku)))
    );
}

#[test]
fn unlock_releases_the_permaflow_s_lock_and_records_its_transition() {
    use crate::store::metaflow::MutatesMetaflows;
    let fixture = NexusFixture::new();
    resting_seat(&fixture);
    fixture
        .nexus
        .store
        .seed_metaflow(metaflow(Model::Astra, State::Asleep, vec![], vec![]));
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    assert!(matches!(
        fixture
            .nexus
            .store
            .lock_metaflow(
                signal_flow::Lock_Data {
                    first_address: address(Model::Astra),
                    second_address: address(Model::Sol),
                },
                now,
            )
            .unwrap(),
        signal_flow::Output::Locked(_)
    ));

    assert_eq!(
        overridden(&fixture, meta::Input::Unlock(address(Model::Sol))),
        meta::Output::Unlocked(address(Model::Sol))
    );
    assert!(
        fixture
            .nexus
            .store
            .stored_metaflow_lock(&address(Model::Sol))
            .unwrap()
            .is_none(),
        "the lock is released"
    );
    let held = transitions(&fixture, CLAUDE_SEAT);
    assert_eq!(held.len(), 1);
    assert_eq!(held[0].cause, Cause::Overridden);
    assert_eq!(held[0].state, meta::State::Resting, "the seat rests");
}

#[test]
fn forget_removes_the_record_of_a_flow_with_no_harness_and_records_it() {
    let fixture = NexusFixture::new();
    resting_seat(&fixture);

    assert_eq!(
        overridden(&fixture, meta::Input::Forget(seat_id())),
        meta::Output::Refused(meta::Refused_Data::Harnessed(seat_id())),
        "a flow with a harness is closed first"
    );
    assert_eq!(
        overridden(&fixture, meta::Input::Close(seat_id())),
        meta::Output::Closed(seat_id())
    );
    fixture
        .nexus
        .store
        .sleep_metaflow(&address(Model::Sol), CLAUDE_SEAT)
        .ok();

    assert_eq!(
        overridden(&fixture, meta::Input::Forget(seat_id())),
        meta::Output::Forgotten(seat_id())
    );
    assert!(
        fixture
            .nexus
            .store
            .flow_node(CLAUDE_SEAT)
            .unwrap()
            .is_none()
    );
    assert!(
        !stored(&fixture, Model::Sol).past.0.contains(&seat_id()),
        "it leaves its metaflow's Past"
    );
    let held = transitions(&fixture, CLAUDE_SEAT);
    assert_eq!(held.len(), 2, "Close's transition, then Forget's: {held:?}");
    assert_eq!(held[1].state, meta::State::Asleep);
    assert_eq!(held[1].cause, Cause::Overridden);

    assert_eq!(
        overridden(&fixture, meta::Input::Forget(seat_id())),
        meta::Output::Refused(meta::Refused_Data::UnknownFlow(seat_id()))
    );
}

/// While held, a seat at its rest is handed nothing, a queued request is
/// typed nowhere and a Send is queued, not typed; Inspect, Queues and
/// Configuration still answer. Each switch records every live seat's
/// transition. Resume hands the seat what queued.
#[test]
fn hold_stops_deliveries_while_queries_answer_and_resume_hands_them_over() {
    let fixture = NexusFixture::new();
    resting_seat(&fixture);

    assert_eq!(overridden(&fixture, meta::Input::Hold), meta::Output::Held);
    assert!(matches!(
        fixture.nexus.store.holding().unwrap(),
        Holding::Held(_)
    ));
    assert_eq!(transitions(&fixture, CLAUDE_SEAT).len(), 1);
    queue_order(&fixture, "Held back.");
    assert_eq!(fixture.nexus.settle_stopped(CLAUDE_SEAT), AtStop::Idle);
    fixture.nexus.drain_awake(&address(Model::Sol));
    fixture.nexus.wake_due(address(Model::Sol));
    assert!(fixture.typed().is_none(), "a held Nexus types nothing");
    assert_eq!(stored(&fixture, Model::Sol).queue.0.len(), 1);

    assert!(matches!(
        fixture
            .nexus
            .dispatch_meta(meta_signal_flow::Input::Inspect(address(Model::Sol))),
        meta_signal_flow::Output::Inspected(_)
    ));
    assert!(matches!(
        fixture.nexus.dispatch(signal_flow::Input::Queues),
        signal_flow::Output::Queues(_)
    ));
    assert!(matches!(
        fixture
            .nexus
            .dispatch_meta(meta_signal_flow::Input::Configuration),
        meta_signal_flow::Output::Configuration(_)
    ));

    assert_eq!(
        overridden(&fixture, meta::Input::Resume),
        meta::Output::Resumed
    );
    assert_eq!(fixture.nexus.store.holding().unwrap(), Holding::Acting);
    let held = transitions(&fixture, CLAUDE_SEAT);
    assert_eq!(held.len(), 2, "Hold's and Resume's transitions: {held:?}");
    assert!(
        held.iter()
            .all(|transition| transition.cause == Cause::Overridden)
    );
    let typed = fixture.typed().expect("Resume hands the seat what queued");
    assert!(typed.contains("Held back."), "{typed}");
}

/// While held, an Asleep metaflow holding an Order is not woken: nothing is
/// launched. Resume wakes it.
#[test]
fn hold_stops_launches_and_resume_starts_what_is_owed() {
    let mut fixture = NexusFixture::new();
    let _seat = ended_claude(&fixture);
    let at = address(Model::Astra);
    fixture
        .nexus
        .store
        .seed_metaflow(metaflow(Model::Sol, State::Asleep, vec!["5ec0d0"], vec![]));
    fixture.nexus.store.seed_metaflow(metaflow(
        Model::Astra,
        State::Asleep,
        vec!["abc123"],
        vec![Request::Order("Carry the design on.".into())],
    ));
    super::waking::declare_on_demand(&fixture, at.clone());
    let world = fixture.fresh_claude_world();
    fixture
        .nexus
        .store
        .configure_change(meta_signal_flow::ConfigurationChange::Models(vec![
            meta_signal_flow::ConfigureModel {
                model: Model::Astra,
                native: meta_signal_flow::Native("claude-fixture".into()),
            },
        ]))
        .expect("the Primary's model configured");
    assert!(fixture.nexus.wake_is_due(&stored(&fixture, Model::Astra)));

    assert_eq!(overridden(&fixture, meta::Input::Hold), meta::Output::Held);
    fixture.nexus.wake_due(at.clone());
    fixture.nexus.recover_wakes();
    assert_eq!(stored(&fixture, Model::Astra).state, State::Asleep);
    assert_eq!(
        world.claude_starts(),
        0,
        "nothing is launched: {}",
        world.calls()
    );

    assert_eq!(
        overridden(&fixture, meta::Input::Resume),
        meta::Output::Resumed
    );
    assert!(
        matches!(stored(&fixture, Model::Astra).state, State::Awake(_)),
        "Resume wakes what is owed: {}",
        world.calls()
    );
    assert_eq!(world.claude_starts(), 1, "{}", world.calls());
}

/// While held, a reminder that falls due is not sent; Resume lets the
/// clock run, and the next pass sends it.
#[test]
fn hold_stands_the_clocks_and_resume_lets_them_run() {
    let fixture = NexusFixture::new();
    let sender = flow_ethos::Address {
        aspect: flow_ethos::Aspect::Psyche,
        topic: flow_ethos::Topic("reminded".into()),
        model: Model::Haiku,
    };
    let recipient = flow_ethos::Address {
        aspect: flow_ethos::Aspect::Mind,
        topic: flow_ethos::Topic("remindedWork".into()),
        model: Model::Sol,
    };
    for address in [&sender, &recipient] {
        fixture.nexus.store.seed_metaflow(Metaflow {
            address: address.clone(),
            state: State::Asleep,
            past: flow_ethos::Past(vec![]),
            queue: flow_ethos::Queue(vec![]),
        });
    }
    let PrivateSendResponse::SendAccepted(acceptance) = fixture
        .nexus
        .store
        .accept_logical_delivery(
            &recipient,
            SentBy::Sender(sender.clone()),
            flow_ethos::Delivery::Queue,
            Message::Soft(Request::Task(flow_ethos::AnswerableTask {
                minutes: flow_ethos::Minutes(10),
                string: "Review the fix".into(),
            })),
            None,
        )
        .unwrap()
    else {
        panic!("the task is admitted")
    };
    let mut reminder = fixture
        .nexus
        .store
        .reminder(&acceptance.logical_delivery_id.0)
        .unwrap()
        .expect("the task holds its reminder");
    reminder.due_option = Some(crate::generated::memory::Due(0));
    fixture.nexus.store.hold_reminder(reminder).unwrap();

    assert_eq!(overridden(&fixture, meta::Input::Hold), meta::Output::Held);
    fixture.nexus.send_due_reminders();
    assert!(
        fixture
            .nexus
            .store
            .metaflow(&sender)
            .unwrap()
            .unwrap()
            .queue
            .0
            .is_empty(),
        "a held clock sends nothing"
    );

    assert_eq!(
        overridden(&fixture, meta::Input::Resume),
        meta::Output::Resumed
    );
    fixture.nexus.send_due_reminders();
    assert_eq!(
        fixture
            .nexus
            .store
            .metaflow(&sender)
            .unwrap()
            .unwrap()
            .queue
            .0
            .len(),
        1,
        "the reminder is sent once the clock runs"
    );
}

#[test]
fn permaflow_hold_queues_every_priority_and_release_drains_oldest_first_once() {
    use crate::generated::overriding::HeedsOverrides;
    let fixture = NexusFixture::new();
    resting_seat(&fixture);
    let at = address(Model::Sol);
    let before = fixture.nexus.store.flow_node(CLAUDE_SEAT).unwrap();
    assert_eq!(
        overridden(&fixture, meta::Input::HoldPermaflow(at.clone())),
        meta::Output::PermaflowHeld(at.clone())
    );
    for message in [
        Message::Soft(Request::Notice("first-held-notice".into())),
        Message::MiddleAbrupt(Request::Order("second-held-order".into())),
        Message::HardAbrupt(Request::Order("third-held-order".into())),
    ] {
        assert!(matches!(
            fixture.nexus.store.accept_logical_delivery(
                &at,
                SentBy::Flow,
                flow_ethos::Delivery::Wake,
                message,
                None
            ),
            Ok(PrivateSendResponse::SendAccepted(_))
        ));
        fixture.nexus.wake_due(at.clone());
        fixture.nexus.drain_awake(&at);
        assert_eq!(fixture.nexus.settle_stopped(CLAUDE_SEAT), AtStop::Idle);
    }
    assert!(fixture.nexus.permaflow_held(at.clone()));
    assert_eq!(fixture.nexus.store.flow_node(CLAUDE_SEAT).unwrap(), before);
    assert!(fixture.typed().is_none());
    let queued = stored(&fixture, Model::Sol).queue.0;
    assert_eq!(queued.len(), 3);
    assert!(
        matches!(overridden(&fixture, meta::Input::Inspect(at.clone())),
        meta::Output::Inspected(snapshot) if snapshot.permaflow_hold == meta::PermaflowHold::Held)
    );
    assert_eq!(
        overridden(&fixture, meta::Input::ReleasePermaflow(at.clone())),
        meta::Output::PermaflowReleased(at.clone())
    );
    let typed = fixture.typed().expect("release delivers held work");
    let first = typed.find("first-held-notice").unwrap();
    let second = typed.find("second-held-order").unwrap();
    let third = typed.find("third-held-order").unwrap();
    assert!(first < second && second < third, "{typed}");
    assert_eq!(
        overridden(&fixture, meta::Input::ReleasePermaflow(at.clone())),
        meta::Output::PermaflowReleased(at)
    );
    assert_eq!(
        fixture.typed().unwrap(),
        typed,
        "idempotent release repeats no delivery"
    );
    assert!(!fixture.nexus.permaflow_held(address(Model::Sol)));
}

#[test]
fn permaflow_hold_survives_a_nexus_restart_with_its_queue_and_pane_unchanged() {
    use crate::generated::overriding::HeedsOverrides;
    let mut fixture = NexusFixture::new();
    resting_seat(&fixture);
    let at = address(Model::Sol);
    overridden(&fixture, meta::Input::HoldPermaflow(at.clone()));
    queue_order(&fixture, "retained-across-restart");
    let before = stored(&fixture, Model::Sol);
    fixture.restarted();
    fixture.nexus.recover_wakes();
    assert!(fixture.nexus.permaflow_held(at.clone()));
    assert_eq!(stored(&fixture, Model::Sol), before);
    assert!(fixture.typed().is_none());
    assert_eq!(
        overridden(&fixture, meta::Input::ReleasePermaflow(at.clone())),
        meta::Output::PermaflowReleased(at)
    );
    assert!(fixture.typed().unwrap().contains("retained-across-restart"));
}

#[test]
fn held_permaflow_refuses_direct_controls_without_touching_the_seat() {
    use crate::generated::overriding::{AdmitsPermaflowControl, Subject};
    let fixture = NexusFixture::new();
    resting_seat(&fixture);
    let at = address(Model::Sol);
    overridden(&fixture, meta::Input::HoldPermaflow(at.clone()));
    let before = fixture.nexus.store.flow_node(CLAUDE_SEAT).unwrap();
    for input in [
        meta::Input::Close(seat_id()),
        meta::Input::Renew(at.clone()),
        meta::Input::Restart(seat_id()),
        meta::Input::Deliver(at.clone()),
    ] {
        assert_eq!(
            overridden(&fixture, input),
            meta::Output::Refused(meta::Refused_Data::PermaflowHeld(at.clone()))
        );
    }
    assert!(
        matches!(fixture.nexus.ordinary_control(Subject::Named(seat_id())),
        Err(signal_flow::Output::Refused(signal_flow::Refusal::PermaflowHeld(held))) if held == at)
    );
    assert_eq!(fixture.nexus.store.flow_node(CLAUDE_SEAT).unwrap(), before);
    assert!(fixture.typed().is_none());
}

#[test]
fn releasing_one_permaflow_while_global_hold_stands_keeps_its_work_queued() {
    let fixture = NexusFixture::new();
    resting_seat(&fixture);
    let at = address(Model::Sol);
    overridden(&fixture, meta::Input::HoldPermaflow(at.clone()));
    queue_order(&fixture, "held-by-global-switch");
    overridden(&fixture, meta::Input::Hold);
    overridden(&fixture, meta::Input::ReleasePermaflow(at));
    assert!(fixture.typed().is_none());
    assert_eq!(stored(&fixture, Model::Sol).queue.0.len(), 1);
    overridden(&fixture, meta::Input::Resume);
    assert!(fixture.typed().unwrap().contains("held-by-global-switch"));
}

#[test]
fn permaflow_hold_leaves_other_addresses_due_to_wake() {
    use crate::generated::memory::SeatStanding;
    use crate::store::current_seat::RecordsCurrentSeats;
    let fixture = NexusFixture::new();
    for (model, last) in [(Model::Sol, "a1b2c3"), (Model::Astra, "d4e5f6")] {
        fixture
            .nexus
            .store
            .record_observation(last, Some(SeatStanding::Ended))
            .unwrap();
        fixture.nexus.store.seed_metaflow(metaflow(
            model.clone(),
            State::Asleep,
            vec![last],
            vec![Request::Order("address work".into())],
        ));
        assert!(fixture.nexus.wake_is_due(&stored(&fixture, model)));
    }
    overridden(&fixture, meta::Input::HoldPermaflow(address(Model::Sol)));
    assert!(fixture.nexus.wake_is_due(&stored(&fixture, Model::Astra)));
    assert!(!fixture.nexus.wake_is_due(&stored(&fixture, Model::Sol)));
}

#[test]
fn repeated_hold_keeps_the_same_queue_and_inspect_exposes_release() {
    let fixture = NexusFixture::new();
    resting_seat(&fixture);
    let at = address(Model::Sol);
    overridden(&fixture, meta::Input::HoldPermaflow(at.clone()));
    queue_order(&fixture, "held-once");
    let before = stored(&fixture, Model::Sol);
    overridden(&fixture, meta::Input::HoldPermaflow(at.clone()));
    assert_eq!(stored(&fixture, Model::Sol), before);
    overridden(&fixture, meta::Input::ReleasePermaflow(at.clone()));
    assert!(matches!(overridden(&fixture, meta::Input::Inspect(at)),
        meta::Output::Inspected(snapshot) if snapshot.permaflow_hold == meta::PermaflowHold::Released));
}

#[test]
fn sends_at_all_priorities_are_admitted_but_never_typed_while_permaflow_held() {
    let fixture = NexusFixture::new();
    resting_seat(&fixture);
    let at = address(Model::Sol);
    overridden(&fixture, meta::Input::HoldPermaflow(at.clone()));
    for message in [
        signal_flow::SendableMessage::Soft(signal_flow::SendableRequest::Notice("soft".into())),
        signal_flow::SendableMessage::MiddleAbrupt(signal_flow::SendableRequest::Order(
            "middle".into(),
        )),
        signal_flow::SendableMessage::HardAbrupt(signal_flow::SendableRequest::Order(
            "hard".into(),
        )),
    ] {
        let answer = fixture
            .nexus
            .dispatch(signal_flow::Input::Send(signal_flow::SendRequest {
                address: at.clone(),
                sendable_message: message,
            }));
        assert!(
            matches!(answer, signal_flow::Output::SendAccepted(_)),
            "{answer:?}"
        );
        assert!(fixture.typed().is_none());
    }
    assert_eq!(stored(&fixture, Model::Sol).queue.0.len(), 3);
}

#[test]
fn hold_acknowledgment_waits_for_the_admitted_effect_to_end() {
    use crate::generated::overriding::{
        HeedsOverrides, SerializesPermaflowEffects, SerializesPermaflowHolds,
    };
    use crate::store::AnnouncesLaunchChanges;
    let fixture = NexusFixture::new();
    resting_seat(&fixture);
    let at = address(Model::Sol);
    let effect = fixture.nexus.waking.effect(at.clone());
    let changes = fixture.nexus.store.launch_changes.clone();
    let seen = changes.current();
    let (sent, received) = std::sync::mpsc::channel();
    std::thread::scope(|scope| {
        scope.spawn(|| {
            let result = fixture.nexus.establish_permaflow_hold(at.clone());
            sent.send(result).unwrap();
        });
        let changed = changes.until(
            seen,
            std::time::Instant::now() + std::time::Duration::from_secs(5),
        );
        let held = fixture.nexus.permaflow_held(at.clone());
        let waiting = matches!(
            received.try_recv(),
            Err(std::sync::mpsc::TryRecvError::Empty)
        );
        drop(effect);
        let answer = received.recv_timeout(std::time::Duration::from_secs(5));
        assert!(
            changed && held && waiting,
            "durable Hold precedes its acknowledgment"
        );
        assert!(matches!(answer, Ok(Ok(true))), "{answer:?}");
    });
}

#[test]
fn maintained_permaflow_without_a_flow_stays_down_until_release_even_after_restart() {
    use crate::store::successor::{OwesSuccessors, SuccessionCause, SuccessorIntent};
    let mut fixture = NexusFixture::new();
    let _seat = ended_claude(&fixture);
    let at = address(Model::Sol);
    fixture
        .nexus
        .store
        .seed_metaflow(metaflow(Model::Sol, State::Asleep, vec!["abc123"], vec![]));
    maintain(&fixture, at.clone());
    fixture
        .nexus
        .store
        .owe_successor(SuccessorIntent {
            address: at.clone(),
            predecessor: crate::generated::memory::Predecessor("abc123".into()),
            launch_request_id: signal_flow::LaunchRequestId("held-exit-successor".into()),
            succession_cause: SuccessionCause::Exit,
        })
        .unwrap();
    let world = fixture.fresh_claude_world();
    overridden(&fixture, meta::Input::HoldPermaflow(at.clone()));
    fixture.nexus.wake_due(at.clone());
    fixture.restarted();
    fixture.nexus.recover_wakes();
    assert_eq!(world.claude_starts(), 0, "{}", world.calls());
    assert_eq!(stored(&fixture, Model::Sol).state, State::Asleep);
    overridden(&fixture, meta::Input::ReleasePermaflow(at));
    assert_eq!(world.claude_starts(), 1, "{}", world.calls());
    assert!(matches!(
        stored(&fixture, Model::Sol).state,
        State::Awake(_)
    ));
}

#[test]
fn explicit_launch_is_parked_atomically_and_resumed_once_by_release() {
    use crate::generated::overriding::ParksHeldLaunches;
    use crate::launching::LaunchesFlows;
    use crate::store::ReadsLaunchAttempt;
    let mut fixture = NexusFixture::new();
    let seat = ended_claude(&fixture);
    let at = address(Model::Sol);
    fixture
        .nexus
        .store
        .seed_metaflow(metaflow(Model::Sol, State::Asleep, vec!["abc123"], vec![]));
    let world = fixture.fresh_claude_world();
    let mut profile = seat.profile;
    profile.launch_request_id = signal_flow::LaunchRequestId("held-explicit-launch".into());
    overridden(&fixture, meta::Input::HoldPermaflow(at.clone()));
    let pending = fixture
        .nexus
        .start(profile.clone(), signal_flow::Origin::Unbound);
    assert!(
        matches!(pending, signal_flow::Output::LaunchPending(_)),
        "{pending:?}"
    );
    let attempt = fixture
        .nexus
        .store
        .launch_attempt(&profile.launch_request_id.0)
        .unwrap()
        .unwrap();
    assert_eq!(
        attempt.launch_attempt_phase,
        signal_flow::LaunchAttemptPhase::Reserved
    );
    assert!(attempt.native_launch_intent_option.is_none());
    assert!(
        fixture
            .nexus
            .store
            .marked_held_launch(profile.launch_request_id.clone())
            .unwrap()
    );
    assert_eq!(world.claude_starts(), 0, "{}", world.calls());
    let replay = fixture
        .nexus
        .start(profile.clone(), signal_flow::Origin::Unbound);
    assert!(matches!(replay, signal_flow::Output::LaunchPending(_)));
    assert_eq!(fixture.nexus.store.held_launches().unwrap().len(), 1);
    let mut conflicting = profile.clone();
    conflicting.instruction_prompt.0.push_str(" changed");
    assert_eq!(
        fixture
            .nexus
            .start(conflicting, signal_flow::Origin::Unbound),
        signal_flow::Output::StartRejected(signal_flow::StartRejection::LaunchRequestConflict)
    );
    fixture.restarted();
    fixture.nexus.recover_wakes();
    assert_eq!(world.claude_starts(), 0);
    overridden(&fixture, meta::Input::ReleasePermaflow(at.clone()));
    assert_eq!(world.claude_starts(), 1, "{}", world.calls());
    assert!(
        !fixture
            .nexus
            .store
            .marked_held_launch(profile.launch_request_id)
            .unwrap()
    );
    overridden(&fixture, meta::Input::ReleasePermaflow(at));
    assert_eq!(world.claude_starts(), 1, "{}", world.calls());
}

#[test]
fn held_refresh_records_the_order_without_replacing_the_resting_flow() {
    use crate::generated::memory::RefreshCaller;
    use crate::store::ordered_replacement::{OrdersReplacements, ReplacementOrder};
    use crate::waking::RefreshesMetaflows;
    let fixture = NexusFixture::new();
    resting_seat(&fixture);
    let at = address(Model::Sol);
    overridden(&fixture, meta::Input::HoldPermaflow(at.clone()));
    let caller = RefreshCaller::Elsewhere(signal_flow::Origin::Unbound);
    let response = fixture.nexus.refresh(&at, &caller);
    assert!(
        matches!(response, signal_flow::Output::RefreshQueued(_)),
        "{response:?}"
    );
    let order = fixture
        .nexus
        .store
        .ordered_replacement(&at)
        .unwrap()
        .unwrap();
    assert_eq!(order.replacement_order, ReplacementOrder::Refresh(caller));
    assert_eq!(stored(&fixture, Model::Sol).state, State::Awake(seat_id()));
    assert!(fixture.typed().is_none());
}

#[test]
fn held_scheduled_reminder_queues_once_across_restart_and_release_delivers_it() {
    use crate::store::scheduled_reminder::SchedulesReminders;
    let mut fixture = NexusFixture::new();
    resting_seat(&fixture);
    let at = address(Model::Sol);
    overridden(&fixture, meta::Input::HoldPermaflow(at.clone()));
    let request = fixture
        .nexus
        .store
        .schedule_reminder(at.clone(), SentBy::Flow, 0, "held-clock-request".into())
        .unwrap()
        .unwrap();
    fixture.nexus.send_due_reminders();
    assert!(fixture.typed().is_none());
    assert_eq!(stored(&fixture, Model::Sol).state, State::Awake(seat_id()));
    assert_eq!(stored(&fixture, Model::Sol).queue.0.len(), 1);
    assert!(
        fixture
            .nexus
            .store
            .scheduled_reminder(&request)
            .unwrap()
            .is_none()
    );
    let queued = stored(&fixture, Model::Sol).queue;
    fixture.restarted();
    fixture.nexus.send_due_reminders();
    fixture.nexus.recover_wakes();
    assert!(fixture.typed().is_none());
    assert_eq!(stored(&fixture, Model::Sol).queue, queued);
    overridden(&fixture, meta::Input::ReleasePermaflow(at));
    let typed = fixture
        .typed()
        .expect("release delivers the scheduled reminder");
    assert!(typed.contains("held-clock-request"), "{typed}");
    assert_eq!(typed.matches("held-clock-request").count(), 1, "{typed}");
}

#[test]
fn held_task_sender_keeps_due_task_reminder_queued_until_its_next_hand_over() {
    let fixture = NexusFixture::new();
    resting_seat(&fixture);
    let sender = address(Model::Sol);
    let recipient = address(Model::Astra);
    fixture
        .nexus
        .store
        .seed_metaflow(metaflow(Model::Astra, State::Asleep, vec![], vec![]));
    let PrivateSendResponse::SendAccepted(accepted) = fixture
        .nexus
        .store
        .accept_logical_delivery(
            &recipient,
            SentBy::Sender(sender.clone()),
            flow_ethos::Delivery::Queue,
            Message::Soft(Request::Task(flow_ethos::AnswerableTask {
                minutes: flow_ethos::Minutes(10),
                string: "held-task-reminder".into(),
            })),
            None,
        )
        .unwrap()
    else {
        panic!("the task is admitted")
    };
    let mut reminder = fixture
        .nexus
        .store
        .reminder(&accepted.logical_delivery_id.0)
        .unwrap()
        .unwrap();
    reminder.due_option = Some(crate::generated::memory::Due(0));
    fixture.nexus.store.hold_reminder(reminder).unwrap();
    overridden(&fixture, meta::Input::HoldPermaflow(sender.clone()));
    fixture.nexus.send_due_reminders();
    assert!(fixture.typed().is_none());
    let queued = stored(&fixture, Model::Sol).queue;
    assert_eq!(queued.0.len(), 1);
    fixture.nexus.send_due_reminders();
    assert_eq!(stored(&fixture, Model::Sol).queue, queued);
    overridden(&fixture, meta::Input::ReleasePermaflow(sender.clone()));
    // A Task's reminder from Flow is no waking request: released, it waits
    // in the queue for its sender's next hand-over.
    assert!(fixture.typed().is_none());
    assert_eq!(stored(&fixture, Model::Sol).queue, queued);
    assert!(matches!(
        fixture.nexus.store.accept_logical_delivery(
            &sender,
            SentBy::Flow,
            flow_ethos::Delivery::Wake,
            Message::Soft(Request::Order("Carry on.".into())),
            None,
        ),
        Ok(PrivateSendResponse::SendAccepted(_))
    ));
    fixture.nexus.wake_due(sender);
    let typed = fixture
        .typed()
        .expect("the next hand-over carries the due task reminder");
    assert!(
        typed.contains(&format!("Reminder.{{ {} ", accepted.request_id.0)),
        "the reminder names the task by its id: {typed}"
    );
    assert!(
        !typed.contains("held-task-reminder"),
        "the reminder does not repeat the task's text: {typed}"
    );
}

#[test]
fn handover_warning_uses_the_held_queue_and_release_preserves_its_once_only_witness() {
    use crate::handover_reminding::RemindsHandoverCallers;
    use crate::store::SeedsRole;
    use crate::store::context::RecordsContextSize;
    use crate::store::handover::HoldsHandovers;
    use crate::store::handover_reminders::SchedulesHandoverReminders;
    let fixture = NexusFixture::new();
    resting_seat(&fixture);
    let at = address(Model::Sol);
    fixture.nexus.store.seed_role(signal_flow::CallerIdentity {
        flow_id: seat_id(),
        aspect: signal_flow::Aspect::Mind,
        model_name: signal_flow::ModelName("held-warning-model".into()),
    });
    fixture
        .nexus
        .store
        .configure_change(meta::ConfigurationChange::Thresholds(vec![
            meta::ConfigureThreshold {
                model_name: signal_flow::ModelName("held-warning-model".into()),
                refresh_threshold: meta::RefreshThreshold(101),
            },
        ]))
        .unwrap();
    fixture
        .nexus
        .store
        .configure_change(meta::ConfigurationChange::HandoverPolicies(vec![
            meta::HandoverPolicy {
                model_name: signal_flow::ModelName("held-warning-model".into()),
                warning_share_left: meta::WarningShareLeft(15),
                reminder_text: meta::ReminderText("held-handover-warning".into()),
                every_minutes: meta::EveryMinutes(5),
            },
        ]))
        .unwrap();
    let handover = fixture
        .nexus
        .store
        .ensure_live_handover(&at, CLAUDE_SEAT)
        .unwrap();
    fixture
        .nexus
        .store
        .record_context_size(CLAUDE_SEAT, 86)
        .unwrap();
    overridden(&fixture, meta::Input::HoldPermaflow(at.clone()));
    fixture.nexus.send_due_handover_reminders(10);
    assert!(fixture.typed().is_none());
    let queued = stored(&fixture, Model::Sol).queue;
    assert_eq!(queued.0.len(), 1);
    fixture.nexus.send_due_handover_reminders(1210);
    assert_eq!(stored(&fixture, Model::Sol).queue, queued);
    overridden(&fixture, meta::Input::ReleasePermaflow(at.clone()));
    let typed = fixture
        .typed()
        .expect("release delivers the handover warning");
    assert!(typed.contains("held-handover-warning"), "{typed}");
    fixture.nexus.send_due_handover_reminders(2410);
    assert_eq!(fixture.typed().unwrap(), typed);
    assert_eq!(
        fixture
            .nexus
            .store
            .handover_reminder(&at, handover.handover_generation.0)
            .unwrap()
            .unwrap()
            .sent,
        crate::generated::memory::Sent(1)
    );
}

#[test]
fn refused_parked_launch_is_terminal_and_never_reexecuted_after_release_or_restart() {
    use crate::generated::overriding::ParksHeldLaunches;
    use crate::launching::LaunchesFlows;
    use crate::store::metaflow_declaration::ReadsMetaflowDeclarations;
    use crate::store::{NamesConfiguredModel, RecordsLaunchOutcome};
    for change_model in [false, true] {
        let mut fixture = NexusFixture::new();
        let seat = ended_claude(&fixture);
        let at = address(Model::Sol);
        fixture.nexus.store.seed_metaflow(metaflow(
            Model::Sol,
            State::Asleep,
            vec!["abc123"],
            vec![],
        ));
        let world = fixture.fresh_claude_world();
        let mut profile = seat.profile;
        profile.launch_request_id = signal_flow::LaunchRequestId("refused-parked-launch".into());
        fixture
            .nexus
            .store
            .configure_change(meta::ConfigurationChange::Metaflows(vec![
                meta::ConfigureMetaflow {
                    effort: meta_signal_flow::Effort::Default,
                    address: at.clone(),
                    root_option: None,
                    leave_outs: signal_flow::LeaveOuts(vec![]),
                    system_modules: profile.system_modules.clone(),
                    prompt_modules: profile.prompt_modules.clone(),
                    flow_kind: profile.flow_kind.clone(),
                    tool_set: profile.tool_set.clone(),
                    continuity: meta::Continuity::OnDemand,
                    refresh_mode: meta_signal_flow::RefreshMode::Automatic,
                    purpose: meta::Purpose(String::new()),
                    records: meta::Records(vec![]),
                },
            ]))
            .unwrap();
        overridden(&fixture, meta::Input::HoldPermaflow(at.clone()));
        assert!(matches!(
            fixture
                .nexus
                .start(profile.clone(), signal_flow::Origin::Unbound),
            signal_flow::Output::LaunchPending(_)
        ));
        let original_model = fixture.nexus.store.configured_model(&at).unwrap().unwrap();
        let original_declaration = fixture
            .nexus
            .store
            .metaflow_declaration(&at)
            .unwrap()
            .unwrap();
        if change_model {
            fixture
                .nexus
                .store
                .configure_change(meta::ConfigurationChange::Models(vec![
                    meta::ConfigureModel {
                        model: Model::Sol,
                        native: meta::Native("changed-model".into()),
                    },
                ]))
                .unwrap();
        } else {
            let mut declaration = original_declaration.clone();
            declaration.root_option = Some(meta::Root(
                fixture
                    .directory
                    .path()
                    .join("absent-launch-root")
                    .to_string_lossy()
                    .into_owned(),
            ));
            fixture
                .nexus
                .store
                .configure_change(meta::ConfigurationChange::Metaflows(vec![declaration]))
                .unwrap();
        }
        overridden(&fixture, meta::Input::ReleasePermaflow(at.clone()));
        let outcome = fixture
            .nexus
            .store
            .launch_outcome(&profile.launch_request_id.0)
            .unwrap()
            .expect("a resumed refusal settles the parked request");
        if change_model {
            assert_eq!(
                outcome,
                crate::store::LaunchOutcome::StartRejected(
                    signal_flow::StartRejection::LaunchRequestConflict
                )
            );
        } else {
            assert_eq!(
                outcome,
                crate::store::LaunchOutcome::StartRejected(
                    signal_flow::StartRejection::CompositionRefused(
                        signal_flow::CompositionRefusal::Unconfigured
                    )
                )
            );
        }
        assert!(
            !fixture
                .nexus
                .store
                .marked_held_launch(profile.launch_request_id.clone())
                .unwrap()
        );
        assert_eq!(world.claude_starts(), 0, "{}", world.calls());
        // Restore a launchable configuration: a terminal request stays terminal.
        fixture
            .nexus
            .store
            .configure_change(meta::ConfigurationChange::Models(vec![
                meta::ConfigureModel {
                    model: Model::Sol,
                    native: meta::Native(original_model),
                },
            ]))
            .unwrap();
        fixture
            .nexus
            .store
            .configure_change(meta::ConfigurationChange::Metaflows(vec![
                original_declaration,
            ]))
            .unwrap();
        overridden(&fixture, meta::Input::ReleasePermaflow(at.clone()));
        fixture.restarted();
        fixture.nexus.recover_wakes();
        overridden(&fixture, meta::Input::ReleasePermaflow(at));
        assert_eq!(
            fixture
                .nexus
                .store
                .launch_outcome(&profile.launch_request_id.0)
                .unwrap(),
            Some(outcome)
        );
        assert_eq!(world.claude_starts(), 0, "{}", world.calls());
    }
}

#[test]
fn release_takes_up_an_orphaned_launch_order_once_without_a_nexus_restart() {
    use crate::generated::memory::{OrderedLaunch, OrderedReplacement, Predecessor, PromptSha256};
    use crate::store::ordered_replacement::{OrdersReplacements, ReplacementOrder};
    let mut fixture = NexusFixture::new();
    let seat = ended_claude(&fixture);
    let at = address(Model::Sol);
    fixture
        .nexus
        .store
        .seed_metaflow(metaflow(Model::Sol, State::Asleep, vec!["abc123"], vec![]));
    let world = fixture.fresh_claude_world();
    let mut profile = seat.profile;
    profile.launch_request_id = signal_flow::LaunchRequestId("held-orphaned-launch".into());
    overridden(&fixture, meta::Input::HoldPermaflow(at.clone()));
    let order = OrderedReplacement {
        address: at.clone(),
        predecessor: Predecessor("abc123".into()),
        replacement_order: ReplacementOrder::Launch(Box::new(OrderedLaunch {
            launch_profile: profile,
            origin: signal_flow::Origin::Unbound,
            prompt_sha256: PromptSha256(String::new()),
        })),
    };
    fixture
        .nexus
        .store
        .order_replacement(order.clone())
        .unwrap();
    fixture.nexus.recover_wakes();
    assert_eq!(world.claude_starts(), 0, "{}", world.calls());
    assert_eq!(
        fixture.nexus.store.ordered_replacement(&at).unwrap(),
        Some(order)
    );
    overridden(&fixture, meta::Input::ReleasePermaflow(at.clone()));
    assert_eq!(world.claude_starts(), 1, "{}", world.calls());
    assert!(matches!(
        stored(&fixture, Model::Sol).state,
        State::Awake(_)
    ));
    assert!(
        fixture
            .nexus
            .store
            .ordered_replacement(&at)
            .unwrap()
            .is_none()
    );
    overridden(&fixture, meta::Input::ReleasePermaflow(at));
    assert_eq!(world.claude_starts(), 1, "{}", world.calls());
}

#[test]
fn a_retained_settled_marker_is_removed_without_executing_its_launch() {
    use crate::generated::operation::{LaunchReservation, Operation, Outcome};
    use crate::generated::overriding::ParksHeldLaunches;
    use crate::launching::LaunchesFlows;
    use crate::performing::Performs;
    use crate::store::{LaunchOutcome, RecordsLaunchOutcome};
    let mut fixture = NexusFixture::new();
    let seat = ended_claude(&fixture);
    let at = address(Model::Sol);
    fixture
        .nexus
        .store
        .seed_metaflow(metaflow(Model::Sol, State::Asleep, vec!["abc123"], vec![]));
    let world = fixture.fresh_claude_world();
    let mut profile = seat.profile;
    profile.launch_request_id = signal_flow::LaunchRequestId("settled-held-marker".into());
    overridden(&fixture, meta::Input::HoldPermaflow(at.clone()));
    let Outcome::Composed(launch) = fixture.nexus.perform(Operation::Compose(profile.clone()))
    else {
        panic!("the retained launch composes");
    };
    let outcome =
        LaunchOutcome::StartRejected(signal_flow::StartRejection::LaunchPersistenceRefused);
    // Seed the state an older writer could leave: terminal outcome plus parked marker.
    fixture
        .nexus
        .store
        .record_launch_outcome(&profile.launch_request_id.0, outcome.clone())
        .unwrap();
    assert!(matches!(
        fixture
            .nexus
            .store
            .park_held_launch(LaunchReservation {
                composed_launch: launch,
                origin: signal_flow::Origin::Unbound,
            })
            .unwrap(),
        signal_flow::LaunchAttemptReservation::Reserved(_)
    ));
    fixture.restarted();
    overridden(&fixture, meta::Input::ReleasePermaflow(at.clone()));
    assert_eq!(world.claude_starts(), 0, "{}", world.calls());
    assert!(
        !fixture
            .nexus
            .store
            .marked_held_launch(profile.launch_request_id.clone())
            .unwrap()
    );
    assert_eq!(
        fixture
            .nexus
            .store
            .launch_outcome(&profile.launch_request_id.0)
            .unwrap(),
        Some(outcome)
    );
    assert_eq!(
        fixture.nexus.start(profile, signal_flow::Origin::Unbound),
        signal_flow::Output::StartRejected(signal_flow::StartRejection::LaunchPersistenceRefused)
    );
    overridden(&fixture, meta::Input::ReleasePermaflow(at));
    assert_eq!(world.claude_starts(), 0, "{}", world.calls());
}

#[test]
fn a_parked_prompt_fingerprint_conflict_is_terminal_without_a_profile_conflict() {
    use crate::generated::operation::{LaunchReservation, Operation, Outcome};
    use crate::generated::overriding::ParksHeldLaunches;
    use crate::launching::{LaunchesFlows, NamesLaunchSession};
    use crate::performing::Performs;
    use crate::store::{
        LaunchOutcome, NamesConfiguredModel, ReadsLaunchAttempt, RecordsLaunchOutcome,
    };
    let mut fixture = NexusFixture::new();
    let seat = ended_claude(&fixture);
    let at = address(Model::Sol);
    fixture
        .nexus
        .store
        .seed_metaflow(metaflow(Model::Sol, State::Asleep, vec!["abc123"], vec![]));
    let world = fixture.fresh_claude_world();
    let mut profile = seat.profile;
    profile.launch_request_id = signal_flow::LaunchRequestId("changed-parked-prompt".into());
    profile.herdr_session_name = fixture.nexus.launch_session().unwrap();
    profile.model_name =
        signal_flow::ModelName(fixture.nexus.store.configured_model(&at).unwrap().unwrap());
    overridden(&fixture, meta::Input::HoldPermaflow(at.clone()));
    let Outcome::Composed(mut launch) = fixture.nexus.perform(Operation::Compose(profile.clone()))
    else {
        panic!("the parked launch composes");
    };
    let current_fingerprint = launch.first_prompt_payload.prompt_sha256.clone();
    // Preserve the exact profile, with the fingerprint of earlier prompt content.
    launch.first_prompt_payload.prompt_sha256 = signal_flow::PromptSha256("0".repeat(64));
    assert_ne!(
        launch.first_prompt_payload.prompt_sha256,
        current_fingerprint
    );
    fixture
        .nexus
        .store
        .park_held_launch(LaunchReservation {
            composed_launch: launch,
            origin: signal_flow::Origin::Unbound,
        })
        .unwrap();
    assert_eq!(
        fixture
            .nexus
            .store
            .launch_attempt(&profile.launch_request_id.0)
            .unwrap()
            .unwrap()
            .launch_profile,
        profile
    );
    overridden(&fixture, meta::Input::ReleasePermaflow(at.clone()));
    assert_eq!(
        fixture
            .nexus
            .store
            .launch_outcome(&profile.launch_request_id.0)
            .unwrap(),
        Some(LaunchOutcome::StartRejected(
            signal_flow::StartRejection::LaunchRequestConflict
        ))
    );
    assert!(
        !fixture
            .nexus
            .store
            .marked_held_launch(profile.launch_request_id.clone())
            .unwrap()
    );
    assert_eq!(world.claude_starts(), 0, "{}", world.calls());
    fixture.restarted();
    overridden(&fixture, meta::Input::ReleasePermaflow(at));
    assert_eq!(
        fixture.nexus.start(profile, signal_flow::Origin::Unbound),
        signal_flow::Output::StartRejected(signal_flow::StartRejection::LaunchRequestConflict)
    );
    assert_eq!(world.claude_starts(), 0, "{}", world.calls());
}

#[test]
fn rebind_cannot_move_a_pane_while_its_confirmed_owner_command_writes() {
    use crate::DispatchesSerially;
    use crate::event_wait::AwaitsPath;
    use crate::fixture_executable::{FixtureExecutable, HoldsLatch, InstallsScript, Latch};
    use std::fs;
    use std::time::{Duration, Instant};

    let fixture = NexusFixture::new();
    resting_seat(&fixture);
    super::waking::claude_prompt_taken(&fixture);
    let mut old = claude_agent(&fixture);
    old["agent_status"] = "working".into();
    let mut alternate = old.clone();
    alternate["pane_id"] = "w1:p9".into();
    alternate["terminal_id"] = "term_moved".into();
    alternate["name"] = "mind-flow-moved".into();
    let log =
        fixture.accept_pane_operations(vec![old, alternate], PromptFixture::Prompted("w1:p3"));
    let started = fixture.directory.path().join("owner-command-before-key");
    let release = Latch::closed(fixture.directory.path().join("owner-command-key-release"));
    let script = fs::read_to_string(&fixture.snapshot_program).unwrap();
    assert!(script.contains("\"agent read\") "));
    FixtureExecutable {
        path: fixture.snapshot_program.clone(),
    }
    .install(&script.replacen(
        "\"agent read\") ",
        &format!(
            "\"agent read\") : > '{}'; : < '{}'; ",
            started.display(),
            release.path.display()
        ),
        1,
    ));
    let rebind = || {
        fixture.nexus.dispatch_meta_serially(
            meta::Input::Rebind(meta::Rebind_Data {
                address: address(Model::Sol),
                rebinding: meta::Rebinding::Pane(signal_flow::HerdrPaneId("w1:p9".into())),
            }),
            None,
        )
    };
    std::thread::scope(|scope| {
        let command = scope.spawn(|| {
            fixture.nexus.dispatch_meta_serially(
                meta::Input::Command(meta::CommandRequest {
                    flow_id: seat_id(),
                    harness_command_kind: meta::HarnessCommandKind::Interrupt,
                }),
                None,
            )
        });
        if !started.appeared_by(Instant::now() + Duration::from_secs(15)) {
            release.release();
            panic!("the confirmed owner command did not reach its pane read");
        }
        let concurrent = rebind();
        let before = fs::read_to_string(&log).unwrap_or_default();
        release.release();
        let command = command.join().unwrap();
        assert!(
            matches!(
                concurrent,
                meta::Output::Refused(meta::Refused_Data::StoreRefused(_))
            ),
            "{concurrent:?}"
        );
        assert!(!before.contains("send-keys"));
        assert!(matches!(command, meta::Output::Commanded(_)), "{command:?}");
    });
    assert_eq!(rebind(), meta::Output::Rebound(address(Model::Sol)));
    let after = fs::read_to_string(&log).unwrap();
    assert_eq!(
        after
            .lines()
            .filter(|line| line.contains("send-keys w1:p3"))
            .count(),
        1,
        "{after}"
    );
    assert!(!after.contains("send-keys w1:p9"));
}

#[test]
fn ordinary_send_replies_before_its_own_barred_herdr_prompt() {
    use super::GreetsOrdinarySocket;
    use crate::event_wait::AwaitsPath;
    use crate::fixture_executable::{FixtureExecutable, HoldsLatch, InstallsScript, Latch};
    use crate::{CarriesSignalFrames, Connection, ServesConnection};
    use std::os::unix::net::UnixStream;
    use std::time::{Duration, Instant};

    let fixture = NexusFixture::new();
    resting_seat(&fixture);
    let recipient = address(Model::Sol);
    let reaches = fixture.directory.path().join("own-prompt-reached");
    let release = Latch::closed(fixture.directory.path().join("own-prompt-release"));
    let script = std::fs::read_to_string(&fixture.snapshot_program).unwrap();
    let arm = "\"agent prompt\") ";
    assert!(script.contains(arm));
    FixtureExecutable {
        path: fixture.snapshot_program.clone(),
    }
    .install(&script.replacen(
        arm,
        &format!(
            "{arm} if [ ! -e '{}' ]; then : > '{}'; : < '{}'; fi; ",
            reaches.display(),
            reaches.display(),
            release.path.display()
        ),
        1,
    ));

    let (peer, mut client) = UnixStream::pair().unwrap();
    client
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    std::thread::scope(|scope| {
        let fixture = &fixture;
        let server = scope.spawn(move || {
            Connection::accepted(peer)
                .unwrap()
                .serve_ordinary(&fixture.nexus)
        });
        client.greet_ordinary();
        client
            .write_query(&signal_flow::Input::Send(signal_flow::SendRequest {
                address: recipient.clone(),
                sendable_message: super::sendable_message(Message::MiddleAbrupt(Request::Order(
                    "reply before own prompt".into(),
                ))),
            }))
            .unwrap();
        let reached = reaches.appeared_by(Instant::now() + Duration::from_secs(15));
        let reply = if reached {
            Some(client.read_response())
        } else {
            None
        };
        release.release();
        let Some(Ok(signal_flow::Output::SendAccepted(accepted))) = reply else {
            panic!("no durable admission before the Herdr barrier released: {reply:?}");
        };
        assert_eq!(
            accepted.visible_delivery_state,
            signal_flow::VisibleDeliveryState::Queued
        );
        assert!(server.join().unwrap().is_ok());
        let prompts = super::prompts(&fixture.directory.path().join("herdr-operations.log"));
        assert!(prompts[0].contains(&accepted.request_id.0));
        assert_eq!(
            prompts
                .iter()
                .filter(|prompt| prompt.contains(&accepted.request_id.0))
                .count(),
            1
        );
    });
}

#[test]
fn a_reply_left_unwritten_fences_only_its_recipient_and_reopens_in_order() {
    use crate::sending::SendsRequests;
    use crate::store::logical_delivery::HoldsAdmissionAnswers;
    use signal_flow::SendRequest;

    let mut fixture = NexusFixture::new();
    resting_seat(&fixture);
    let recipient = address(Model::Sol);
    let unrelated = address(Model::Astra);
    fixture
        .nexus
        .store
        .seed_metaflow(metaflow(Model::Astra, State::Asleep, vec![], vec![]));

    let older = fixture.nexus.admit_ordinary_send(
        SendRequest {
            address: recipient.clone(),
            sendable_message: super::sendable_message(Message::MiddleAbrupt(Request::Order(
                "oldest, reply unwritten".into(),
            ))),
        },
        None,
    );
    let signal_flow::Output::SendAccepted(oldest) = &older.response else {
        panic!("expected durable admission before writing its reply");
    };
    let oldest_id = oldest.request_id.clone();
    assert!(fixture.nexus.store.pane_input_waits_for_reply(&oldest_id));

    // Retain the first connection's post-reply work without running it. A
    // following reply has completed and its wake is now allowed to run.
    let newer = fixture.nexus.admit_ordinary_send(
        SendRequest {
            address: recipient.clone(),
            sendable_message: super::sendable_message(Message::MiddleAbrupt(Request::Order(
                "second, behind first".into(),
            ))),
        },
        None,
    );
    let signal_flow::Output::SendAccepted(following) = &newer.response else {
        panic!("the second reply should not wait for the first");
    };
    let following_id = following.request_id.clone();
    drop(newer);
    fixture.nexus.wake_due(recipient.clone());
    assert!(fixture.typed().is_none());

    let independent = fixture.nexus.admit_ordinary_send(
        SendRequest {
            address: unrelated,
            sendable_message: super::sendable_message(Message::Soft(Request::Notice(
                "independent recipient".into(),
            ))),
        },
        None,
    );
    let signal_flow::Output::SendAccepted(accepted) = &independent.response else {
        panic!("an unrelated admission should not wait");
    };
    assert_eq!(
        accepted.visible_delivery_state,
        signal_flow::VisibleDeliveryState::Queued
    );
    drop(independent);
    let ids = |fixture: &NexusFixture| {
        stored(fixture, Model::Sol)
            .queue
            .0
            .into_iter()
            .map(|entry| entry.request_id)
            .collect::<Vec<_>>()
    };
    assert_eq!(ids(&fixture), [oldest_id.clone(), following_id.clone()]);

    // Drop the Nexus's in-memory store while the old adapter still hasn't
    // written a reply or executed post-reply work.
    fixture.restarted();
    assert!(!fixture.nexus.store.pane_input_waits_for_reply(&oldest_id));
    assert_eq!(ids(&fixture), [oldest_id.clone(), following_id.clone()]);
    fixture.nexus.recover_wakes();
    drop(older);
    let prompts = super::prompts(&fixture.directory.path().join("herdr-operations.log"));
    assert_eq!(prompts.len(), 1, "{prompts:?}");
    let first = prompts[0].find(&oldest_id.0).expect("oldest recovered");
    let second = prompts[0]
        .find(&following_id.0)
        .expect("following recovered");
    assert!(first < second);
}

#[test]
fn held_socket_admissions_survive_disconnect_and_reopen_with_the_same_receipt_ids() {
    use super::GreetsOrdinarySocket;
    use crate::{CarriesSignalFrames, Connection, ServesConnection};
    use signal_flow::{Input, Output, SendRequest, VisibleDeliveryState};
    use std::os::unix::net::UnixStream;
    use std::time::Duration;

    let mut fixture = NexusFixture::new();
    resting_seat(&fixture);
    fixture.accept_pane_operations(
        vec![claude_agent(&fixture)],
        PromptFixture::PromptedUnrecorded("w1:p3"),
    );
    let address = address(Model::Sol);
    assert_eq!(
        overridden(&fixture, meta::Input::HoldPermaflow(address.clone())),
        meta::Output::PermaflowHeld(address.clone())
    );

    let send = |message: Message, read_reply: bool| {
        let (peer, mut client) = UnixStream::pair().unwrap();
        client
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        std::thread::scope(|scope| {
            let fixture = &fixture;
            let server = scope.spawn(move || {
                Connection::accepted(peer)
                    .unwrap()
                    .serve_ordinary(&fixture.nexus)
            });
            client.greet_ordinary();
            client
                .write_query(&Input::Send(SendRequest {
                    address: address.clone(),
                    sendable_message: super::sendable_message(message),
                }))
                .unwrap();
            let response = read_reply.then(|| client.read_response().unwrap());
            drop(client);
            let _ = server.join().unwrap();
            response
        })
    };

    let Some(Output::SendAccepted(first)) = send(
        Message::MiddleAbrupt(Request::Order("retained first socket input".into())),
        true,
    ) else {
        panic!("held send must answer admission");
    };
    assert_eq!(first.visible_delivery_state, VisibleDeliveryState::Queued);
    send(
        Message::MiddleAbrupt(Request::Order("retained disconnected socket input".into())),
        false,
    );
    let before = stored(&fixture, Model::Sol);
    assert_eq!(before.queue.0.len(), 2);
    assert_eq!(before.queue.0[0].request_id, first.request_id);
    assert!(fixture.typed().is_none());
    fixture.restarted();
    fixture.nexus.recover_wakes();
    assert_eq!(stored(&fixture, Model::Sol), before);

    assert_eq!(
        overridden(&fixture, meta::Input::ReleasePermaflow(address.clone())),
        meta::Output::PermaflowReleased(address.clone())
    );

    let (peer, mut observer) = UnixStream::pair().unwrap();
    observer
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    std::thread::scope(|scope| {
        let server = scope.spawn(|| {
            Connection::accepted(peer)
                .unwrap()
                .serve_ordinary(&fixture.nexus)
        });
        observer.greet_ordinary();
        observer.write_query(&Input::ObserveSends(None)).unwrap();
        let Output::SendsObserved(observed) = observer.read_response().unwrap() else {
            panic!("expected observer snapshot")
        };
        for entry in &before.queue.0 {
            let matching = observed
                .visible_delivery_snapshot_vector
                .iter()
                .filter(|item| item.request_id == entry.request_id)
                .collect::<Vec<_>>();
            assert_eq!(matching.len(), 1, "{matching:?}");
            assert!(
                matches!(
                    matching[0].visible_delivery_state,
                    VisibleDeliveryState::Delivered
                ),
                "{matching:?}"
            );
            assert!(matching[0].delivery_cursor > first.delivery_cursor);
        }
        drop(observer);
        // Closing the live observation can race its next socket write.
        let _ = server.join().unwrap();
    });
    fixture.nexus.recover_wakes();
    let prompts = super::prompts(&fixture.directory.path().join("herdr-operations.log"));
    assert_eq!(prompts.len(), 1, "{prompts:?}");
    assert!(
        before
            .queue
            .0
            .iter()
            .all(|entry| prompts[0].contains(&entry.request_id.0))
    );
}

#[test]
fn socket_admission_stays_accepted_when_herdr_refuses_before_input() {
    use super::GreetsOrdinarySocket;
    use crate::{CarriesSignalFrames, Connection, ServesConnection};
    use signal_flow::{DeliveryState, Input, Output, SendRequest, VisibleDeliveryState};
    use std::os::unix::net::UnixStream;
    use std::time::Duration;

    let fixture = NexusFixture::new();
    resting_seat(&fixture);
    fixture.accept_pane_operations(
        vec![claude_agent(&fixture)],
        PromptFixture::RefusedBeforeInput("agent_not_found"),
    );
    let (peer, mut client) = UnixStream::pair().unwrap();
    client
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let acceptance = std::thread::scope(|scope| {
        let server = scope.spawn(|| {
            Connection::accepted(peer)
                .unwrap()
                .serve_ordinary(&fixture.nexus)
        });
        client.greet_ordinary();
        client
            .write_query(&Input::Send(SendRequest {
                address: address(Model::Sol),
                sendable_message: super::sendable_message(Message::MiddleAbrupt(Request::Order(
                    "do not lose this ID".into(),
                ))),
            }))
            .unwrap();
        let Output::SendAccepted(accepted) = client.read_response().unwrap() else {
            panic!("expected durable acceptance")
        };
        assert!(server.join().unwrap().is_ok());
        accepted
    });
    assert_eq!(
        acceptance.visible_delivery_state,
        VisibleDeliveryState::Queued
    );
    let queued = stored(&fixture, Model::Sol).queue.0;
    assert_eq!(queued.len(), 1);
    assert_eq!(queued[0].request_id, acceptance.request_id);
    let state = fixture
        .nexus
        .store
        .logical_delivery_snapshot(&queued[0].logical_delivery_id)
        .unwrap()
        .unwrap();
    // A write refused before any key leaves the request queued as it was
    // admitted.
    assert_eq!(state.delivery_state, DeliveryState::Queued);
    assert!(fixture.typed().is_none());
}
