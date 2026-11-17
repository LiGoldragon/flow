//! The override panel on the meta socket: each override does what it names
//! and records its transition, the cause Overridden; Hold stands the
//! Nexus's clocks, deliveries and launches while every query still answers,
//! and Resume takes up what is owed.

use super::refreshing::refreshable_with_successor;
use super::waking::{
    CLAUDE_SEAT, ReadsFreshWorld, WakesFreshClaude, address, append_main_rows, claude_agent,
    claude_seat, end_turn, ended_claude, maintain, metaflow, stored,
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
use meta_signal_flow::panel;
use signal_flow::{FlowLifecycle, Message, Metaflow, Model, PrivateSendResponse, Request, SentBy};

/// One override, answered by the panel.
fn overridden(fixture: &NexusFixture, input: panel::Input) -> panel::Output {
    match fixture
        .nexus
        .dispatch_meta(meta_signal_flow::Input::Override(input))
    {
        meta_signal_flow::Output::Overridden(output) => output,
        other => panic!("an override is answered by the panel: {other:?}"),
    }
}

fn seat_id() -> flow_ethos::FlowId {
    flow_ethos::FlowId(CLAUDE_SEAT.into())
}

/// The transitions Flow's Memory holds for `flow_id`, oldest first.
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
}

/// The last transition held for `flow_id` is the override's, into `state`,
/// at the seat's Sol Address.
fn overridden_into(fixture: &NexusFixture, flow_id: &str, state: panel::State) {
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
/// metaflow Maintained.
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
            panel::Input::SetState(panel::SetState_Data {
                flow_id: seat_id(),
                state: panel::State::Crystallizing,
            })
        ),
        panel::Output::StateSet(panel::StateSet_Data {
            flow_id: seat_id(),
            state: panel::State::Crystallizing,
        })
    );
    overridden_into(&fixture, CLAUDE_SEAT, panel::State::Crystallizing);

    let unknown = flow_ethos::FlowId("0000ff".into());
    assert_eq!(
        overridden(
            &fixture,
            panel::Input::SetState(panel::SetState_Data {
                flow_id: unknown.clone(),
                state: panel::State::Resting,
            })
        ),
        panel::Output::Refused(panel::Refused_Data::UnknownFlow(unknown))
    );
    assert_eq!(transitions(&fixture, CLAUDE_SEAT).len(), 1);
}

#[test]
fn crystallize_now_asks_the_flow_for_its_handover_and_records_crystallizing() {
    let fixture = NexusFixture::new();
    resting_seat(&fixture);
    // Held, so the ask stays queued where it can be read.
    assert_eq!(
        overridden(&fixture, panel::Input::Hold),
        panel::Output::Held
    );

    assert_eq!(
        overridden(&fixture, panel::Input::CrystallizeNow(seat_id())),
        panel::Output::Asked(seat_id())
    );
    overridden_into(&fixture, CLAUDE_SEAT, panel::State::Crystallizing);
    let queued = stored(&fixture, Model::Sol).queue.0;
    assert!(
        queued.iter().any(|entry| entry.sent_by == SentBy::Flow
            && matches!(
                &entry.message,
                Message::MiddleAbrupt(Request::Notice(text))
                    if text.ends_with(&format!("flows/{CLAUDE_SEAT}/handover.md"))
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
        overridden(&fixture, panel::Input::CrystallizeNow(seat_id())),
        panel::Output::Refused(panel::Refused_Data::Unharnessed(seat_id()))
    );
}

#[test]
fn renew_starts_a_fresh_flow_and_closes_the_old_one() {
    let (fixture, _) = refreshable_with_successor(
        meta_signal_flow::Continuity::OnDemand,
        flow_ethos::Delivery::Queue,
    );

    assert_eq!(
        overridden(&fixture, panel::Input::Renew(address(Model::Sol))),
        panel::Output::Renewed(flow_ethos::FlowId("908786".into()))
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
    overridden_into(&fixture, CLAUDE_SEAT, panel::State::Asleep);

    assert_eq!(
        overridden(&fixture, panel::Input::Renew(address(Model::Haiku))),
        panel::Output::Refused(panel::Refused_Data::UnknownAddress(address(Model::Haiku)))
    );
}

#[test]
fn close_closes_the_flow_s_harness_and_records_it_asleep() {
    let fixture = NexusFixture::new();
    resting_seat(&fixture);

    assert_eq!(
        overridden(&fixture, panel::Input::Close(seat_id())),
        panel::Output::Closed(seat_id())
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
    overridden_into(&fixture, CLAUDE_SEAT, panel::State::Asleep);
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
        overridden(&fixture, panel::Input::Deliver(address(Model::Sol))),
        panel::Output::Delivered(address(Model::Sol))
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
        overridden(&fixture, panel::Input::Deliver(address(Model::Haiku))),
        panel::Output::Refused(panel::Refused_Data::UnknownAddress(address(Model::Haiku)))
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
        overridden(&fixture, panel::Input::Unlock(address(Model::Sol))),
        panel::Output::Unlocked(address(Model::Sol))
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
    assert_eq!(held[0].state, panel::State::Resting, "the seat rests");
}

#[test]
fn forget_removes_the_record_of_a_flow_with_no_harness_and_records_it() {
    let fixture = NexusFixture::new();
    resting_seat(&fixture);

    assert_eq!(
        overridden(&fixture, panel::Input::Forget(seat_id())),
        panel::Output::Refused(panel::Refused_Data::Harnessed(seat_id())),
        "a flow with a harness is closed first"
    );
    assert_eq!(
        overridden(&fixture, panel::Input::Close(seat_id())),
        panel::Output::Closed(seat_id())
    );
    fixture
        .nexus
        .store
        .sleep_metaflow(&address(Model::Sol), CLAUDE_SEAT)
        .ok();

    assert_eq!(
        overridden(&fixture, panel::Input::Forget(seat_id())),
        panel::Output::Forgotten(seat_id())
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
    assert_eq!(held[1].state, panel::State::Asleep);
    assert_eq!(held[1].cause, Cause::Overridden);

    assert_eq!(
        overridden(&fixture, panel::Input::Forget(seat_id())),
        panel::Output::Refused(panel::Refused_Data::UnknownFlow(seat_id()))
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

    assert_eq!(
        overridden(&fixture, panel::Input::Hold),
        panel::Output::Held
    );
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
        overridden(&fixture, panel::Input::Resume),
        panel::Output::Resumed
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

    assert_eq!(
        overridden(&fixture, panel::Input::Hold),
        panel::Output::Held
    );
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
        overridden(&fixture, panel::Input::Resume),
        panel::Output::Resumed
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

    assert_eq!(
        overridden(&fixture, panel::Input::Hold),
        panel::Output::Held
    );
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
        overridden(&fixture, panel::Input::Resume),
        panel::Output::Resumed
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
