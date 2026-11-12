//! A task's reminders go to the sender Flow recorded when it admitted the
//! task, and with the last of them its failure goes to that same sender.
//! No model is chosen by rank, and nothing a reminder queues is a task of
//! its own: no reminder follows a reminder.

use super::NexusFixture;
use crate::reminding::RemindsCallers;
use crate::store::ManagesMetaflowState;
use crate::store::logical_delivery::RecordsLogicalDeliveries;
use crate::store::metaflow::{SeedsMetaflow, SleepsMetaflows};
use crate::store::queue_delivery::AccountsQueueDelivery;
use crate::store::reminder::{RecordsReminders, TASK_REMINDERS};
use flow_ethos::{Address, Aspect, Model, Request};
use signal_flow::{Message, Permaflow, PrivateSendResponse, QueueEntry, SentBy};

fn at(aspect: Aspect, topic: &str, model: Model) -> Address {
    Address {
        aspect,
        topic: flow_ethos::Topic(topic.into()),
        model,
    }
}

fn notices(queue: &[QueueEntry]) -> Vec<(SentBy, String)> {
    queue
        .iter()
        .map(|entry| match &entry.message {
            Message::Soft(Request::Notice(notice)) => (entry.sent_by.clone(), notice.clone()),
            other => panic!("a reminder is a Soft Notice: {other:?}"),
        })
        .collect()
}

/// The last reminder of a Task sent by a Haiku seat queues, for that very
/// seat, the fourth reminder and then the task's failure, both from Flow.
/// No Opus, nor any other seat, is told; the task's recipient keeps only
/// the task; the reminder is spent; and a second pass queues nothing more.
#[test]
fn a_tasks_last_reminder_sends_its_failure_to_the_tasks_own_sender() {
    let fixture = NexusFixture::new();
    let sender = at(Aspect::Psyche, "reminded", Model::Haiku);
    let recipient = at(Aspect::Mind, "remindedWork", Model::Sol);
    for address in [&sender, &recipient] {
        fixture.nexus.store.seed_metaflow(Permaflow {
            address: address.clone(),
            permaflow_state: flow_ethos::PermaflowState::Asleep,
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
    assert_eq!(reminder.recipient, recipient);
    reminder.sent = TASK_REMINDERS - 1;
    reminder.due = Some(0);
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
        notices(&told.0),
        vec![
            (
                SentBy::Flow,
                "Reminder 4 of 4: the task of 10 minutes, Review the fix, to Mind remindedWork Sol has no Done after 40 minutes.".to_owned()
            ),
            (
                SentBy::Flow,
                "Failure: the task of 10 minutes, Review the fix, from Psyche reminded Haiku to Mind remindedWork Sol has no Done after 4 reminders.".to_owned()
            ),
        ]
    );
    let spent = fixture
        .nexus
        .store
        .reminder(&key.0)
        .unwrap()
        .expect("the task stays open");
    assert_eq!((spent.sent, spent.due), (TASK_REMINDERS, None));
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

#[test]
fn archiving_an_ended_task_retracts_its_reminder_atomically() {
    let fixture = NexusFixture::new();
    let sender = at(Aspect::Psyche, "archiving", Model::Haiku);
    let recipient = at(Aspect::Mind, "archivedTask", Model::Sol);
    fixture.nexus.store.seed_metaflow(Permaflow {
        address: recipient.clone(),
        permaflow_state: flow_ethos::PermaflowState::Asleep,
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

/// A seat that sends a task to itself is reminded, and told its failure,
/// as any sender: the failure goes once to it, and no reminder follows.
#[test]
fn a_task_a_seat_sent_itself_fails_to_it_once_with_no_loop() {
    let fixture = NexusFixture::new();
    let seat = at(Aspect::Mind, "selfTask", Model::Sol);
    fixture.nexus.store.seed_metaflow(Permaflow {
        address: seat.clone(),
        permaflow_state: flow_ethos::PermaflowState::Asleep,
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
    reminder.sent = TASK_REMINDERS - 1;
    reminder.due = Some(0);
    fixture.nexus.store.hold_reminder(reminder).unwrap();

    fixture.nexus.send_due_reminders();
    fixture.nexus.send_due_reminders();

    let queue = fixture.nexus.store.metaflow(&seat).unwrap().unwrap().queue;
    assert_eq!(
        queue.0.len(),
        3,
        "the task, its last reminder and its failure"
    );
    assert_eq!(queue.0[0].logical_delivery_id, key);
    let failures = notices(&queue.0[1..])
        .into_iter()
        .filter(|(_, notice)| notice.starts_with("Failure:"))
        .count();
    assert_eq!(failures, 1, "the failure is told once");
    assert_eq!(
        fixture
            .nexus
            .store
            .reminder(&key.0)
            .unwrap()
            .map(|spent| spent.due),
        Some(None),
        "the reminder is spent"
    );
}
