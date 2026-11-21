//! A task's reminder goes to the sender Flow recorded when it admitted the
//! task, and to no one else. No model is chosen by rank, and nothing a
//! reminder queues is a task of its own: no reminder follows a reminder.

use super::NexusFixture;
use crate::reminding::RemindsCallers;
use crate::store::ManagesMetaflowState;
use crate::store::logical_delivery::RecordsLogicalDeliveries;
use crate::store::metaflow::{SeedsMetaflow, SleepsMetaflows};
use crate::store::queue_delivery::AccountsQueueDelivery;
use crate::store::reminder::{RecordsReminders, TASK_REMINDERS};
use flow_ethos::{Address, Aspect, Model, Request};
use signal_flow::{Message, Metaflow, PrivateSendResponse, QueueEntry, SentBy};

fn at(aspect: Aspect, topic: &str, model: Model) -> Address {
    Address {
        aspect,
        topic: flow_ethos::Topic(topic.into()),
        model,
    }
}

fn reminders(queue: &[QueueEntry]) -> Vec<(SentBy, flow_ethos::Reminder_Data)> {
    queue
        .iter()
        .map(|entry| match &entry.message {
            Message::Soft(Request::Reminder(reminder)) => {
                (entry.sent_by.clone(), reminder.clone())
            }
            other => panic!("a task's reminder is a Soft Reminder: {other:?}"),
        })
        .collect()
}

/// The reminder of the task answered `task`: its id and one short line.
fn reminder_of(task: &flow_ethos::RequestId, line: &str) -> flow_ethos::Reminder_Data {
    flow_ethos::Reminder_Data {
        request_id: task.clone(),
        string: line.into(),
    }
}

/// The last reminder of a Task sent by a Haiku seat queues, for that very
/// seat, the reminder alone, from Flow, with no failure after it. No Opus,
/// nor any other seat, is told; the task's recipient keeps only the task;
/// the reminder is spent; and a second pass queues nothing more.
#[test]
fn a_tasks_last_reminder_goes_alone_to_the_tasks_own_sender() {
    let fixture = NexusFixture::new();
    let sender = at(Aspect::Psyche, "reminded", Model::Haiku);
    let recipient = at(Aspect::Mind, "remindedWork", Model::Sol);
    for address in [&sender, &recipient] {
        fixture.nexus.store.seed_metaflow(Metaflow {
            address: address.clone(),
            state: flow_ethos::State::Asleep,
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
    let key = acceptance.logical_delivery_id.clone();
    let mut reminder = fixture
        .nexus
        .store
        .reminder(&key.0)
        .unwrap()
        .expect("the task holds its reminder");
    assert_eq!(reminder.address, sender, "the sender Flow recorded");
    assert_eq!(
        reminder.recipient,
        crate::generated::memory::Recipient(recipient.clone())
    );
    reminder.sent = crate::generated::memory::Sent(TASK_REMINDERS - 1);
    reminder.due_option = Some(crate::generated::memory::Due(0));
    fixture.nexus.store.hold_reminder(reminder).unwrap();

    fixture.nexus.send_due_reminders();

    let told = fixture
        .nexus
        .store
        .metaflow(&sender)
        .unwrap()
        .unwrap()
        .queue;
    assert_eq!(
        reminders(&told.0),
        vec![(
            SentBy::Flow,
            reminder_of(
                &acceptance.request_id,
                "The task to Mind remindedWork Sol has no Done after 10 minutes."
            )
        )],
        "the reminder names the task by its id, not by its text"
    );
    let spent = fixture
        .nexus
        .store
        .reminder(&key.0)
        .unwrap()
        .expect("the task stays open");
    assert_eq!((spent.sent.0, spent.due_option), (TASK_REMINDERS, None));
    for entry in &told.0 {
        assert_eq!(
            fixture
                .nexus
                .store
                .reminder(&entry.logical_delivery_id.0)
                .unwrap(),
            None,
            "a reminder holds no reminder of its own"
        );
    }
    let work = fixture
        .nexus
        .store
        .metaflow(&recipient)
        .unwrap()
        .unwrap()
        .queue;
    assert_eq!(work.0.len(), 1, "the recipient keeps only the task");
    assert_eq!(work.0[0].logical_delivery_id, key);
    let held = fixture
        .nexus
        .store
        .list_metaflows()
        .unwrap()
        .into_iter()
        .map(|metaflow| metaflow.address)
        .collect::<Vec<_>>();
    assert_eq!(held.len(), 2, "no other seat is told: {held:?}");
    assert!(
        fixture
            .nexus
            .store
            .metaflow(&at(Aspect::Psyche, "reminded", Model::Opus))
            .unwrap()
            .is_none(),
        "no model is chosen by rank"
    );

    fixture.nexus.send_due_reminders();
    assert_eq!(
        fixture
            .nexus
            .store
            .metaflow(&sender)
            .unwrap()
            .unwrap()
            .queue,
        told,
        "nothing more is reminded"
    );
}

/// A Task's sender is reminded once, when its minutes pass: the reminder
/// is then spent, with no due time, and a second pass reminds no more.
#[test]
fn a_task_is_reminded_once_when_its_minutes_pass() {
    let fixture = NexusFixture::new();
    let sender = at(Aspect::Psyche, "remindedOnce", Model::Haiku);
    let recipient = at(Aspect::Mind, "remindedOnceWork", Model::Sol);
    for address in [&sender, &recipient] {
        fixture.nexus.store.seed_metaflow(Metaflow {
            address: address.clone(),
            state: flow_ethos::State::Asleep,
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
    let key = acceptance.logical_delivery_id.clone();
    let mut reminder = fixture
        .nexus
        .store
        .reminder(&key.0)
        .unwrap()
        .expect("the task holds its reminder");
    assert_eq!(reminder.sent.0, 0, "no reminder is sent yet");
    // Its minutes pass.
    reminder.due_option = Some(crate::generated::memory::Due(0));
    fixture.nexus.store.hold_reminder(reminder).unwrap();

    fixture.nexus.send_due_reminders();
    fixture.nexus.send_due_reminders();

    let told = fixture
        .nexus
        .store
        .metaflow(&sender)
        .unwrap()
        .unwrap()
        .queue;
    assert_eq!(
        reminders(&told.0),
        vec![(
            SentBy::Flow,
            reminder_of(
                &acceptance.request_id,
                "The task to Mind remindedOnceWork Sol has no Done after 10 minutes."
            )
        )]
    );
    let spent = fixture
        .nexus
        .store
        .reminder(&key.0)
        .unwrap()
        .expect("the task stays open");
    assert_eq!(
        (spent.sent.0, spent.due_option),
        (1, None),
        "no second reminder falls due"
    );
}

#[test]
fn archiving_an_ended_task_retracts_its_reminder_atomically() {
    let fixture = NexusFixture::new();
    let sender = at(Aspect::Psyche, "archiving", Model::Haiku);
    let recipient = at(Aspect::Mind, "archivedTask", Model::Sol);
    fixture.nexus.store.seed_metaflow(Metaflow {
        address: recipient.clone(),
        state: flow_ethos::State::Asleep,
        past: flow_ethos::Past(vec![]),
        queue: flow_ethos::Queue(vec![]),
    });
    let PrivateSendResponse::SendAccepted(acceptance) = fixture
        .nexus
        .store
        .accept_logical_delivery(
            &recipient,
            SentBy::Sender(sender.clone()),
            flow_ethos::Delivery::Queue,
            Message::Soft(Request::Task(flow_ethos::AnswerableTask {
                minutes: flow_ethos::Minutes(10),
                string: "Archive the completed task".into(),
            })),
            None,
        )
        .unwrap()
    else {
        panic!("the task is admitted")
    };
    assert!(
        fixture
            .nexus
            .store
            .reminder(&acceptance.logical_delivery_id.0)
            .unwrap()
            .is_some(),
        "an open Task holds its reminder"
    );
    assert_eq!(
        fixture
            .nexus
            .store
            .withdraw_queued(&recipient, &acceptance.logical_delivery_id.0)
            .unwrap(),
        Ok(())
    );
    assert_eq!(
        fixture
            .nexus
            .store
            .archive_request(&acceptance.request_id.0)
            .unwrap(),
        Ok(())
    );
    assert!(
        fixture
            .nexus
            .store
            .reminder(&acceptance.logical_delivery_id.0)
            .unwrap()
            .is_none(),
        "the same archive commit retracts the Task reminder"
    );
}

/// A seat that sends a task to itself is reminded as any sender: the
/// reminder goes once to it, no failure follows it, and no reminder
/// follows a reminder.
#[test]
fn a_task_a_seat_sent_itself_is_reminded_once_with_no_loop() {
    let fixture = NexusFixture::new();
    let seat = at(Aspect::Mind, "selfTask", Model::Sol);
    fixture.nexus.store.seed_metaflow(Metaflow {
        address: seat.clone(),
        state: flow_ethos::State::Asleep,
        past: flow_ethos::Past(vec![]),
        queue: flow_ethos::Queue(vec![]),
    });
    let PrivateSendResponse::SendAccepted(acceptance) = fixture
        .nexus
        .store
        .accept_logical_delivery(
            &seat,
            SentBy::Sender(seat.clone()),
            flow_ethos::Delivery::Queue,
            Message::Soft(Request::Task(flow_ethos::AnswerableTask {
                minutes: flow_ethos::Minutes(5),
                string: "Close the loop".into(),
            })),
            None,
        )
        .unwrap()
    else {
        panic!("the task is admitted")
    };
    let key = acceptance.logical_delivery_id;
    let mut reminder = fixture.nexus.store.reminder(&key.0).unwrap().unwrap();
    reminder.sent = crate::generated::memory::Sent(TASK_REMINDERS - 1);
    reminder.due_option = Some(crate::generated::memory::Due(0));
    fixture.nexus.store.hold_reminder(reminder).unwrap();

    fixture.nexus.send_due_reminders();
    fixture.nexus.send_due_reminders();

    let queue = fixture.nexus.store.metaflow(&seat).unwrap().unwrap().queue;
    assert_eq!(queue.0.len(), 2, "the task and its one reminder");
    assert_eq!(queue.0[0].logical_delivery_id, key);
    assert_eq!(
        reminders(&queue.0[1..]),
        vec![(
            SentBy::Flow,
            reminder_of(
                &acceptance.request_id,
                "The task to Mind selfTask Sol has no Done after 5 minutes."
            )
        )],
        "the reminder is told once"
    );
    assert_eq!(
        fixture
            .nexus
            .store
            .reminder(&key.0)
            .unwrap()
            .map(|spent| spent.due_option),
        Some(None),
        "the reminder is spent"
    );
}
