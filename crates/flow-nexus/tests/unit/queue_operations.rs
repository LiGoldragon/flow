//! The operations that end, empty, force and read a request, a reminder or
//! a queue (Cancel, Silence, Clear, ClearAll, Push, Queues, Hot), and the
//! answers every caller gets alike: the owner at a terminal and a caller in
//! no flow are served as a flow is.

use super::waking::SpawnedProcess;
use super::{ControlsHerdrSnapshot, NexusFixture};
use crate::Dispatches;
use crate::queue_operations::OperatesOnRequestsAndQueues;
use crate::sending::SendsRequests;
use crate::store::logical_delivery::{MintsRequestIds, RecordsLogicalDeliveries};
use crate::store::metaflow::{SeedsMetaflow, SleepsMetaflows};
use crate::store::reminder::RecordsReminders;
use crate::store::turn_end::RecordsTurnEnds;
use crate::store::{FlowStore, ReadsFlowMemory};
use flow_ethos::State;
use signal_flow::{
    Address, Aspect, DeliveredMessage, DeliveredRequest, DeliveryState, HarnessKind, Input,
    Message, Metaflow, Model, NativeReceipt, Origin, Output, PrivateSendResponse, Request,
    RequestRejection, SendAcceptance, SendRequest, SentBy, TurnEnd, VisibleNativeInput,
    VisibleSender,
};

fn at(topic: &str) -> Address {
    Address {
        aspect: Aspect::Mind,
        topic: flow_ethos::Topic(topic.into()),
        model: Model::Sol,
    }
}

fn asleep(fixture: &NexusFixture, address: &Address) {
    fixture.nexus.store.seed_metaflow(Metaflow {
        address: address.clone(),
        state: State::Asleep,
        past: flow_ethos::Past(vec![]),
        queue: flow_ethos::Queue(vec![]),
    });
}

fn awake_as(fixture: &NexusFixture, address: &Address, flow_id: &flow_ethos::FlowId) {
    fixture.nexus.store.seed_metaflow(Metaflow {
        address: address.clone(),
        state: State::Awake(flow_id.clone()),
        past: flow_ethos::Past(vec![]),
        queue: flow_ethos::Queue(vec![]),
    });
}

fn origin_of(flow_id: flow_ethos::FlowId) -> Origin {
    Origin::Flow(signal_flow::FlowOrigin {
        flow_id,
        session_id: signal_flow::SessionId("queue-operations-session".into()),
    })
}

/// Admits `request` for `recipient` from `sender`, queued.
fn admitted(
    fixture: &NexusFixture,
    recipient: &Address,
    sender: SentBy,
    request: Request,
) -> SendAcceptance {
    let PrivateSendResponse::SendAccepted(accepted) = fixture
        .nexus
        .store
        .accept_logical_delivery(
            recipient,
            sender,
            flow_ethos::Delivery::Queue,
            Message::Soft(request),
            None,
        )
        .expect("admission")
    else {
        panic!("admitted")
    };
    accepted
}

/// Marks `accepted` delivered and takes it out of its queue, as its seat's
/// receipt would.
fn delivered(fixture: &NexusFixture, recipient: &Address, accepted: &SendAcceptance) {
    fixture
        .nexus
        .store
        .transition_logical_delivery(
            &accepted.logical_delivery_id,
            DeliveryState::Delivered(NativeReceipt {
                harness_kind: HarnessKind::Codex,
                native_session_id: signal_flow::NativeSessionId("operations-session".into()),
                native_prompt_id: signal_flow::NativePromptId("operations-prompt".into()),
            }),
        )
        .expect("delivered");
    let entry = queued(fixture, recipient)
        .into_iter()
        .find(|entry| entry.logical_delivery_id == accepted.logical_delivery_id)
        .expect("the request is queued");
    assert!(
        fixture
            .nexus
            .store
            .remove_oldest_queued(recipient, &entry)
            .expect("the queue reads")
    );
}

fn queued(fixture: &NexusFixture, address: &Address) -> Vec<flow_ethos::QueueEntry> {
    fixture
        .nexus
        .store
        .metaflow(address)
        .expect("metaflow reads")
        .expect("metaflow held")
        .queue
        .0
}

fn notices(fixture: &NexusFixture, address: &Address) -> Vec<String> {
    queued(fixture, address)
        .into_iter()
        .filter_map(|entry| match entry.message {
            Message::Soft(Request::Notice(text)) if entry.sent_by == SentBy::Flow => Some(text),
            _ => None,
        })
        .collect()
}

#[test]
fn only_the_work_receiver_address_or_its_next_seat_may_complete_work() {
    use crate::store::kept_answer::{AnswerRoute, RoutesAnswers};

    let fixture = NexusFixture::new();
    let requester = at("completionRequester");
    let worker = at("completionWorker");
    let unrelated = at("completionOther");
    let old = flow_ethos::FlowId("oldNative".into());
    let next = flow_ethos::FlowId("newNative".into());
    asleep(&fixture, &requester);
    asleep(&fixture, &unrelated);
    awake_as(&fixture, &worker, &old);
    let work = admitted(
        &fixture,
        &worker,
        SentBy::Sender(requester.clone()),
        work_with(flow_ethos::WorkNotificationPolicy::Destinations(vec![
            flow_ethos::Notified {
                notification_type: flow_ethos::NotificationType::WorkCancelled,
                address_vector: vec![requester.clone()],
            },
        ])),
    );
    delivered(&fixture, &worker, &work);
    let done = Message::Soft(Request::Done(flow_ethos::DonePayload::Answered(
        flow_ethos::DonePayload_Answered_Data {
            logical_delivery_id: work.logical_delivery_id.clone(),
            request_id: work.request_id.clone(),
            string: "Reviewed.".into(),
        },
    )));
    let store = &fixture.nexus.store;
    let before = store.request_binding(&work.request_id).unwrap();
    let admit_answer = |actor: SentBy| {
        store
            .accept_logical_delivery(
                &requester,
                actor,
                flow_ethos::Delivery::Wake,
                done.clone(),
                None,
            )
            .unwrap()
    };
    for actor in [
        SentBy::Unbound,
        SentBy::Sender(unrelated),
        SentBy::Sender(requester.clone()),
    ] {
        assert!(matches!(
            store.answer_route(&done, &actor).unwrap(),
            AnswerRoute::Unentitled
        ));
        assert_eq!(
            admit_answer(actor),
            PrivateSendResponse::Refused(signal_flow::Refusal::Unresolved)
        );
        assert_eq!(store.request_binding(&work.request_id).unwrap(), before);
    }
    awake_as(&fixture, &worker, &next);
    let successor = SentBy::Sender(worker.clone());
    assert!(
        matches!(store.answer_route(&done, &successor).unwrap(), AnswerRoute::To(address) if address == requester)
    );
    assert!(matches!(
        admit_answer(successor),
        PrivateSendResponse::SendAccepted(_)
    ));
    assert_ne!(store.request_binding(&work.request_id).unwrap(), before);
    use crate::store::work_notices::KeepsWorkNotices as _;
    let outcome = store.work_outcome(&work.request_id).unwrap().unwrap();
    assert_eq!(
        outcome.work_notification.notification_type,
        flow_ethos::NotificationType::WorkDone
    );
    assert_eq!(outcome.work_notification.request_id, work.request_id);
    assert!(
        outcome.address_vector.is_empty(),
        "unlisted WorkDone does not default"
    );
    assert_eq!(store.pending_work_notices().unwrap(), vec![]);
    assert_eq!(
        admit_answer(SentBy::Sender(worker)),
        PrivateSendResponse::Refused(signal_flow::Refusal::Unresolved)
    );
}

/// Every caller gets the same refusal when the native receipt proves that
/// the worker took the work but the Nexus has no exclusive execution owner.
#[test]
fn reached_work_preserves_receipts_for_every_cancel_caller() {
    let fixture = NexusFixture::new();
    let sender = at("cancelSender");
    let recipient = at("cancelRecipient");
    let outsider = at("cancelOutsider");
    let sender_flow = flow_ethos::FlowId("senderFlow".into());
    let recipient_flow = flow_ethos::FlowId("recipientFlow".into());
    let outsider_flow = flow_ethos::FlowId("outsiderFlow".into());
    awake_as(&fixture, &sender, &sender_flow);
    awake_as(&fixture, &recipient, &recipient_flow);
    awake_as(&fixture, &outsider, &outsider_flow);
    let work = admitted(
        &fixture,
        &recipient,
        SentBy::Sender(sender.clone()),
        Request::Question("Review the drain fix.".into()),
    );
    delivered(&fixture, &recipient, &work);

    let store = &fixture.nexus.store;
    let before = store
        .logical_delivery_snapshot(&work.logical_delivery_id)
        .unwrap();
    let binding = store.request_binding(&work.request_id).unwrap().unwrap();
    for caller in [
        Origin::Unbound,
        origin_of(sender_flow),
        origin_of(recipient_flow),
        origin_of(outsider_flow),
    ] {
        assert_eq!(
            fixture
                .nexus
                .dispatch_from(Input::Cancel(work.request_id.clone()), caller),
            Output::CancelRejected(RequestRejection::ControlUnavailable)
        );
        assert_eq!(
            store.request_binding(&work.request_id).unwrap(),
            Some(binding.clone())
        );
        assert_eq!(
            store
                .logical_delivery_snapshot(&work.logical_delivery_id)
                .unwrap(),
            before
        );
    }
    for address in [&sender, &recipient, &outsider] {
        assert!(queued(&fixture, address).is_empty());
    }
    assert!(fixture.typed().is_none());
}

/// A flow that cancels the reminder it asked for is told nothing beyond
/// its answer Cancelled.
#[test]
fn cancel_does_not_notice_the_asker_flow_of_its_own_reminder() {
    use crate::store::scheduled_reminder::SchedulesReminders;
    let fixture = NexusFixture::new();
    let reminded = at("ownReminderSeat");
    let asker = at("ownReminderAsker");
    let caller_flow = flow_ethos::FlowId("ownReminderFlow".into());
    asleep(&fixture, &reminded);
    awake_as(&fixture, &asker, &caller_flow);
    let Output::Reminded(request_id) = fixture.nexus.remind(
        signal_flow::Remind_Data {
            address: reminded.clone(),
            minutes: flow_ethos::Minutes(5),
            string: "Later.".into(),
        },
        SentBy::Sender(asker.clone()),
    ) else {
        panic!("a reminder is kept")
    };

    assert_eq!(
        fixture
            .nexus
            .dispatch_from(Input::Cancel(request_id.clone()), origin_of(caller_flow)),
        Output::Cancelled(request_id.clone())
    );
    assert_eq!(
        fixture.nexus.store.scheduled_reminder(&request_id).unwrap(),
        None
    );
    assert!(notices(&fixture, &asker).is_empty());
    assert!(queued(&fixture, &reminded).is_empty());
}

/// A Notice takes no answer and is finished once it reaches its recipient,
/// delivered or uncertain: Cancel and CloseRequest naming it are refused
/// Closed, neither end is told, and its id stays as it was.
#[test]
fn a_notice_that_reached_its_recipient_is_closed_to_cancel_and_close_request() {
    let fixture = NexusFixture::new();
    let sender = at("noticeSender");
    let recipient = at("noticeRecipient");
    asleep(&fixture, &sender);
    asleep(&fixture, &recipient);
    let delivered_notice = admitted(
        &fixture,
        &recipient,
        SentBy::Sender(sender.clone()),
        Request::Notice("The revision is ready.".into()),
    );
    delivered(&fixture, &recipient, &delivered_notice);
    let uncertain_notice = admitted(
        &fixture,
        &recipient,
        SentBy::Sender(sender.clone()),
        Request::Notice("The gate is open.".into()),
    );
    fixture
        .nexus
        .store
        .transition_logical_delivery(
            &uncertain_notice.logical_delivery_id,
            DeliveryState::Uncertain,
        )
        .expect("the Notice is placed without its receipt");
    let recipient_queue = queued(&fixture, &recipient);

    for notice in [&delivered_notice, &uncertain_notice] {
        let binding = fixture
            .nexus
            .store
            .request_binding(&notice.request_id)
            .unwrap();
        assert_eq!(
            fixture
                .nexus
                .dispatch(Input::Cancel(notice.request_id.clone())),
            Output::CancelRejected(RequestRejection::Closed)
        );
        assert_eq!(
            fixture
                .nexus
                .dispatch(Input::CloseRequest(notice.request_id.clone())),
            Output::CloseRequestRejected(RequestRejection::Closed)
        );
        assert_eq!(
            fixture
                .nexus
                .store
                .request_binding(&notice.request_id)
                .unwrap(),
            binding
        );
        assert_eq!(closed_record(&fixture, &notice.logical_delivery_id), None);
    }
    assert!(queued(&fixture, &sender).is_empty());
    assert_eq!(queued(&fixture, &recipient), recipient_queue);
}

/// Delivers `request`, which takes no answer, from one flow to another
/// and checks it finished: Cancel and CloseRequest naming it are refused
/// Closed, neither end is told, and its id and its queue stay as they were.
fn assert_closed_to_cancel_and_close_request_once_delivered(request: Request) {
    let fixture = NexusFixture::new();
    let sender = at("noAnswerSender");
    let recipient = at("noAnswerRecipient");
    asleep(&fixture, &sender);
    asleep(&fixture, &recipient);
    let reached = admitted(
        &fixture,
        &recipient,
        SentBy::Sender(sender.clone()),
        request,
    );
    delivered(&fixture, &recipient, &reached);
    let binding = fixture
        .nexus
        .store
        .request_binding(&reached.request_id)
        .unwrap();

    assert_eq!(
        fixture
            .nexus
            .dispatch(Input::Cancel(reached.request_id.clone())),
        Output::CancelRejected(RequestRejection::Closed)
    );
    assert_eq!(
        fixture
            .nexus
            .dispatch(Input::CloseRequest(reached.request_id.clone())),
        Output::CloseRequestRejected(RequestRejection::Closed)
    );
    assert_eq!(
        fixture
            .nexus
            .store
            .request_binding(&reached.request_id)
            .unwrap(),
        binding
    );
    assert_eq!(closed_record(&fixture, &reached.logical_delivery_id), None);
    assert!(queued(&fixture, &sender).is_empty());
    assert!(queued(&fixture, &recipient).is_empty());
}

/// An Order takes no answer: delivered, it is finished, as a Notice is.
#[test]
fn an_order_that_reached_its_recipient_is_closed_to_cancel_and_close_request() {
    assert_closed_to_cancel_and_close_request_once_delivered(Request::Order(
        "Review the drain fix.".into(),
    ));
}

/// A Living takes no answer: delivered, it is finished, as a Notice is.
#[test]
fn a_living_that_reached_its_recipient_is_closed_to_cancel_and_close_request() {
    assert_closed_to_cancel_and_close_request_once_delivered(Request::Living(
        flow_ethos::Living_Data {
            context: flow_ethos::Context("the drain".into()),
            verbatim: flow_ethos::Verbatim("Fix it today.".into()),
        },
    ));
}

/// A Result, a Done and a Reminder take no answer either: of every kind,
/// only a Question and a Task do, at every priority.
#[test]
fn only_a_question_and_a_task_take_an_answer() {
    use crate::store::logical_delivery::TakesAnswer;
    let answered = [
        Request::Question("Which revision?".into()),
        Request::Task(flow_ethos::AnswerableTask {
            minutes: flow_ethos::Minutes(30),
            string: "Land the line.".into(),
        }),
    ];
    let unanswered = [
        Request::Order("Review the drain fix.".into()),
        Request::Living(flow_ethos::Living_Data {
            context: flow_ethos::Context("the drain".into()),
            verbatim: flow_ethos::Verbatim("Fix it today.".into()),
        }),
        Request::Result(flow_ethos::ResultPayload::Historical("result".into())),
        Request::Notice("The revision is ready.".into()),
        Request::Done(flow_ethos::DonePayload::Historical(
            flow_ethos::Historical_Data {
                logical_delivery_id: flow_ethos::LogicalDeliveryId("logical-task".into()),
                string: "done".into(),
            },
        )),
        Request::Reminder(flow_ethos::Reminder_Data {
            request_id: flow_ethos::RequestId("rmnd".into()),
            string: "reminder".into(),
        }),
    ];
    for request in answered {
        assert!(Message::HardAbrupt(request.clone()).takes_answer());
        assert!(Message::MiddleAbrupt(request.clone()).takes_answer());
        assert!(Message::Soft(request).takes_answer());
    }
    for request in unanswered {
        assert!(!Message::HardAbrupt(request.clone()).takes_answer());
        assert!(!Message::MiddleAbrupt(request.clone()).takes_answer());
        assert!(!Message::Soft(request).takes_answer());
    }
}

/// A Notice still queued never reached its recipient: Cancel takes it out
/// of its queue without prompting anyone.
#[test]
fn cancel_withdraws_a_queued_notice_silently() {
    let fixture = NexusFixture::new();
    let sender = at("queuedNoticeSender");
    let recipient = at("queuedNoticeRecipient");
    asleep(&fixture, &sender);
    asleep(&fixture, &recipient);
    let notice = admitted(
        &fixture,
        &recipient,
        SentBy::Sender(sender.clone()),
        Request::Notice("The revision is ready.".into()),
    );

    assert_eq!(
        fixture
            .nexus
            .dispatch(Input::Cancel(notice.request_id.clone())),
        Output::Cancelled(notice.request_id.clone())
    );
    assert!(queued(&fixture, &recipient).is_empty());
    assert!(notices(&fixture, &sender).is_empty());
}

/// A Task still queued is cancelled: it leaves its queue, its reminder goes
/// with it, and neither party receives a prompt.
#[test]
fn cancel_takes_a_queued_task_out_with_its_reminder() {
    let fixture = NexusFixture::new();
    let sender = at("taskSender");
    let recipient = at("taskRecipient");
    asleep(&fixture, &sender);
    asleep(&fixture, &recipient);
    let task = admitted(
        &fixture,
        &recipient,
        SentBy::Sender(sender.clone()),
        Request::Task(flow_ethos::AnswerableTask {
            minutes: flow_ethos::Minutes(30),
            string: "Land the line.".into(),
        }),
    );
    assert!(
        fixture
            .nexus
            .store
            .reminder(&task.logical_delivery_id.0)
            .unwrap()
            .is_some()
    );
    assert_eq!(
        fixture.nexus.cancel(&task.request_id, &Origin::Unbound),
        Output::Cancelled(task.request_id.clone())
    );
    assert!(queued(&fixture, &recipient).is_empty(), "it left the queue");
    assert_eq!(
        fixture
            .nexus
            .store
            .reminder(&task.logical_delivery_id.0)
            .unwrap(),
        None,
        "its reminder went with it"
    );
    assert!(notices(&fixture, &sender).is_empty());
    assert!(notices(&fixture, &recipient).is_empty());
}

/// A parked Uncertain Task counts as reached for cancellation.
#[test]
fn cancel_of_a_parked_uncertain_task_preserves_its_identity_without_replay() {
    use crate::store::queue_delivery::{
        AccountsQueueDelivery, NativeHistoryAnchor, PrepareQueueDelivery,
    };
    let fixture = NexusFixture::new();
    let sender = at("uncertainCancelSender");
    let recipient = at("uncertainCancelRecipient");
    let flow = flow_ethos::FlowId("uncertainCancelSeat".into());
    asleep(&fixture, &sender);
    awake_as(&fixture, &recipient, &flow);
    let task = admitted(
        &fixture,
        &recipient,
        SentBy::Sender(sender.clone()),
        Request::Task(flow_ethos::AnswerableTask {
            minutes: flow_ethos::Minutes(30),
            string: "Read this.".into(),
        }),
    );
    let store = &fixture.nexus.store;
    let PrepareQueueDelivery::Prepared(attempt) = store
        .prepare_oldest_delivery(
            &recipient,
            &flow.0,
            "uncertain-thread",
            HarnessKind::Codex,
            NativeHistoryAnchor::CodexTurns(None),
        )
        .unwrap()
    else {
        panic!("the request prepared");
    };
    assert!(
        store
            .mark_delivery_uncertain(&attempt.queue_attempt_id.0)
            .unwrap()
    );
    assert!(
        store
            .park_uncertain_delivery(&attempt.queue_attempt_id.0)
            .unwrap()
    );
    let before = store
        .logical_delivery_snapshot(&task.logical_delivery_id)
        .unwrap();
    let binding = store.request_binding(&task.request_id).unwrap().unwrap();
    assert_eq!(
        fixture.nexus.cancel(&task.request_id, &Origin::Unbound),
        Output::CancelRejected(RequestRejection::ControlUnavailable)
    );
    assert_eq!(
        store.request_binding(&task.request_id).unwrap(),
        Some(binding)
    );
    assert_eq!(
        store
            .logical_delivery_snapshot(&task.logical_delivery_id)
            .unwrap(),
        before
    );
    assert!(notices(&fixture, &sender).is_empty());
    assert!(notices(&fixture, &recipient).is_empty());
    assert!(fixture.typed().is_none());
}

#[test]
fn cancel_of_abandoned_work_is_silent() {
    let fixture = NexusFixture::new();
    let sender = at("abandonedCancelSender");
    let recipient = at("abandonedCancelRecipient");
    asleep(&fixture, &sender);
    asleep(&fixture, &recipient);
    let work = admitted(
        &fixture,
        &recipient,
        SentBy::Sender(sender.clone()),
        Request::Question("Check the gate.".into()),
    );
    let store = &fixture.nexus.store;
    let entry = queued(&fixture, &recipient).remove(0);
    assert!(store.remove_oldest_queued(&recipient, &entry).unwrap());
    store
        .transition_logical_delivery(&work.logical_delivery_id, DeliveryState::Abandoned)
        .unwrap();
    assert_eq!(
        fixture.nexus.cancel(&work.request_id, &Origin::Unbound),
        Output::Cancelled(work.request_id.clone())
    );
    assert!(queued(&fixture, &sender).is_empty());
    assert!(queued(&fixture, &recipient).is_empty());
    assert!(store.request_binding(&work.request_id).unwrap().is_none());
}

/// Silence stops a Task's reminders and leaves the Task open; a Silence of
/// an Order is no Task's, and an id no request holds is unknown.
#[test]
fn silence_stops_a_tasks_reminders_and_leaves_it_open() {
    let fixture = NexusFixture::new();
    let sender = at("silenceSender");
    let recipient = at("silenceRecipient");
    asleep(&fixture, &sender);
    asleep(&fixture, &recipient);
    let task = admitted(
        &fixture,
        &recipient,
        SentBy::Sender(sender.clone()),
        Request::Task(flow_ethos::AnswerableTask {
            minutes: flow_ethos::Minutes(30),
            string: "Watch the gate.".into(),
        }),
    );
    let order = admitted(
        &fixture,
        &recipient,
        SentBy::Sender(sender),
        Request::Order("Read it.".into()),
    );
    assert_eq!(
        fixture
            .nexus
            .dispatch(Input::Silence(task.request_id.clone())),
        Output::Silenced(task.request_id.clone())
    );
    assert_eq!(
        fixture
            .nexus
            .store
            .reminder(&task.logical_delivery_id.0)
            .unwrap(),
        None
    );
    assert!(
        fixture
            .nexus
            .store
            .request_binding(&task.request_id)
            .unwrap()
            .is_some(),
        "the Task stays open"
    );
    assert_eq!(
        fixture.nexus.dispatch(Input::Silence(order.request_id)),
        Output::SilenceRejected(RequestRejection::NotTask)
    );
    assert_eq!(
        fixture
            .nexus
            .dispatch(Input::Silence(flow_ethos::RequestId("none".into()))),
        Output::SilenceRejected(RequestRejection::UnknownRequest)
    );
}

/// Clear empties one queue and answers the ids that left; ClearAll empties
/// every queue; Queues reads what is queued, each request with its id, its
/// sender and its priority, as it would land in the prompt.
#[test]
fn clear_and_clear_all_empty_queues_that_queues_reads() {
    let fixture = NexusFixture::new();
    let first = at("clearFirst");
    let second = at("clearSecond");
    asleep(&fixture, &first);
    asleep(&fixture, &second);
    let a = admitted(
        &fixture,
        &first,
        SentBy::Unbound,
        Request::Order("a".into()),
    );
    let b = admitted(
        &fixture,
        &first,
        SentBy::Sender(at("clearAsker")),
        Request::Order("b".into()),
    );
    let c = admitted(
        &fixture,
        &second,
        SentBy::Unbound,
        Request::Order("c".into()),
    );

    let Output::Queues(held) = fixture.nexus.dispatch(Input::Queues) else {
        panic!("Queues reads")
    };
    let first_queue = held
        .iter()
        .find(|queue| queue.address == first)
        .expect("the first queue holds something");
    assert_eq!(
        first_queue.visible_native_input_vector,
        vec![
            VisibleNativeInput {
                visible_sender: VisibleSender::Unbound,
                delivered_message: DeliveredMessage::Soft(DeliveredRequest::Order(
                    signal_flow::Order_Data {
                        request_id: a.request_id.clone(),
                        string: "a".into(),
                    }
                )),
            },
            VisibleNativeInput {
                visible_sender: VisibleSender::Sender(at("clearAsker")),
                delivered_message: DeliveredMessage::Soft(DeliveredRequest::Order(
                    signal_flow::Order_Data {
                        request_id: b.request_id.clone(),
                        string: "b".into(),
                    }
                )),
            },
        ]
    );

    assert_eq!(
        fixture.nexus.dispatch(Input::Clear(first.clone())),
        Output::Cleared(vec![a.request_id, b.request_id])
    );
    assert!(queued(&fixture, &first).is_empty());
    assert_eq!(
        fixture.nexus.dispatch(Input::ClearAll),
        Output::AllCleared(vec![c.request_id])
    );
    assert!(queued(&fixture, &second).is_empty());
    assert_eq!(
        fixture.nexus.dispatch(Input::Queues),
        Output::Queues(vec![])
    );
    assert_eq!(
        fixture.nexus.dispatch(Input::Clear(at("neverHeld"))),
        Output::Refused(signal_flow::Refusal::Unknown(
            signal_flow::Unknown_Data::Address(at("neverHeld"))
        ))
    );
}

/// Push delivers a queue: an Address Flow holds no metaflow for is
/// unknown, an empty queue pushes nothing, and an Asleep metaflow holding a
/// Notice alone keeps its queue, the Notice still waiting for a waking.
#[test]
fn push_forces_a_queue_and_answers_what_was_delivered() {
    let fixture = NexusFixture::new();
    let recipient = at("pushRecipient");
    asleep(&fixture, &recipient);
    let push = |address: &Address, priority| {
        fixture.nexus.dispatch(Input::Push(signal_flow::Push_Data {
            address: address.clone(),
            priority,
        }))
    };
    assert_eq!(
        push(&at("neverHeld"), signal_flow::Priority::HardAbrupt),
        Output::Refused(signal_flow::Refusal::Unknown(
            signal_flow::Unknown_Data::Address(at("neverHeld"))
        ))
    );
    assert_eq!(
        push(&recipient, signal_flow::Priority::HardAbrupt),
        Output::Pushed(vec![])
    );
    let held = admitted(
        &fixture,
        &recipient,
        SentBy::Unbound,
        Request::Notice("held".into()),
    );
    assert_eq!(
        push(&recipient, signal_flow::Priority::HardAbrupt),
        Output::Pushed(vec![]),
        "a Notice alone wakes nothing; nothing was delivered"
    );
    let entries = queued(&fixture, &recipient);
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].request_id, held.request_id);
    assert_eq!(entries[0].delivery, flow_ethos::Delivery::Queue);
}

/// Hot names every live flow's Address by the minutes since its own last
/// turn ended: a flow none of whose turns has ended, the warmest, first.
#[test]
fn hot_reads_each_live_flow_by_its_last_turn_end() {
    use super::waking::{CLAUDE_SEAT, claude_seat};
    let fixture = NexusFixture::new();
    claude_seat(&fixture, 100);
    let seat = super::waking::address(Model::Sol);
    fixture.nexus.store.seed_metaflow(super::waking::metaflow(
        Model::Sol,
        State::Awake(flow_ethos::FlowId(CLAUDE_SEAT.into())),
        vec![],
        vec![],
    ));
    assert_eq!(
        fixture.nexus.hot(),
        Output::Hot(vec![signal_flow::Warmth {
            address: seat.clone(),
            minutes_option: None,
        }])
    );
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    fixture
        .nexus
        .store
        .record_turn_end(TurnEnd {
            address: seat.clone(),
            harness_kind: HarnessKind::Claude,
            flow_id: flow_ethos::FlowId(CLAUDE_SEAT.into()),
            observed_at: signal_flow::ObservedAt(i64::try_from(now).unwrap() - 600),
        })
        .unwrap();
    assert_eq!(
        fixture.nexus.dispatch(Input::Hot),
        Output::Hot(vec![signal_flow::Warmth {
            address: seat,
            minutes_option: Some(flow_ethos::Minutes(10)),
        }])
    );
    assert_eq!(
        fixture.nexus.store.last_turn_ends().unwrap().len(),
        1,
        "the per-Address record is not one of the harnesses' turn ends"
    );
}

/// The owner at a terminal and a caller in no flow are served as a flow
/// is: a Send from either is accepted as Unbound, a DeliverAll and an
/// ObserveSends answer alike. An unbound Question owes a Result; an Order
/// does not, even after delivery.
#[test]
fn the_owner_and_an_unbound_caller_are_served_as_a_flow_is() {
    use crate::sending::IdentifiesSender;
    let fixture = NexusFixture::new();
    let recipient = at("servedRecipient");
    asleep(&fixture, &recipient);
    // The test process is the owner at a terminal: in no flow's pane.
    let owner = FlowStore::process_identity_for_pid(std::process::id()).unwrap();
    let shell = SpawnedProcess(
        std::process::Command::new("sleep")
            .arg("30")
            .spawn()
            .unwrap(),
    );
    let unbound = FlowStore::process_identity_for_pid(shell.0.id()).unwrap();
    for caller in [&owner, &unbound] {
        assert_eq!(fixture.nexus.sender_of(caller).unwrap(), SentBy::Unbound);
        let answer = fixture.nexus.send_request(
            SendRequest {
                address: recipient.clone(),
                sendable_message: super::sendable_message(Message::Soft(Request::Order(
                    "From a shell.".into(),
                ))),
            },
            Some(caller),
        );
        assert!(
            matches!(answer.response, Output::SendAccepted(_)),
            "{:?}",
            answer.response
        );
    }
    let entries = queued(&fixture, &recipient);
    assert_eq!(entries.len(), 2);
    assert!(entries.iter().all(|entry| entry.sent_by == SentBy::Unbound));
    assert_eq!(
        fixture.nexus.dispatch(Input::DeliverAll(recipient.clone())),
        fixture
            .nexus
            .dispatch_from(Input::DeliverAll(recipient.clone()), Origin::Unbound),
    );
    let Output::SendsObserved(observed) = fixture.nexus.dispatch(Input::ObserveSends(None)) else {
        panic!("every send is observed")
    };
    assert!(
        observed
            .visible_delivery_snapshot_vector
            .iter()
            .filter(|snapshot| snapshot.visible_sender == signal_flow::VisibleSender::Unbound)
            .count()
            >= 2
    );

    // A delivered unbound Order takes no Result. An unbound Question
    // resolves for the kept-answer path, whoever answers.
    assert!(matches!(
        fixture.nexus.clear(&recipient),
        Output::Cleared(cleared) if cleared.len() == 2
    ));
    let order = admitted(
        &fixture,
        &recipient,
        SentBy::Unbound,
        Request::Order("Answer anyone.".into()),
    );
    delivered(&fixture, &recipient, &order);
    assert!(matches!(
        fixture
            .nexus
            .store
            .resolve_sendable_message(signal_flow::SendableMessage::Soft(
                signal_flow::SendableRequest::Result(signal_flow::SendableRequest_Result_Data {
                    request_id: order.request_id.clone(),
                    string: "no answer owed".into(),
                },)
            ),),
        Ok(Err(signal_flow::Refusal::Unresolved)),
    ));
    let question = admitted(
        &fixture,
        &recipient,
        SentBy::Unbound,
        Request::Question("Answer the unbound question.".into()),
    );
    delivered(&fixture, &recipient, &question);
    assert!(matches!(
        fixture
            .nexus
            .store
            .resolve_sendable_message(signal_flow::SendableMessage::Soft(
                signal_flow::SendableRequest::Result(signal_flow::SendableRequest_Result_Data {
                    request_id: question.request_id.clone(),
                    string: "done".into(),
                },)
            ),),
        Ok(Ok(Message::Soft(Request::Result(_))))
    ));
}

/// The Send of a Result, naming `request_id`, written to `written`.
fn answer_to(written: &Address, request_id: &flow_ethos::RequestId) -> SendRequest {
    SendRequest {
        address: written.clone(),
        sendable_message: signal_flow::SendableMessage::Soft(signal_flow::SendableRequest::Result(
            signal_flow::SendableRequest_Result_Data {
                request_id: request_id.clone(),
                string: "Reviewed; green.".into(),
            },
        )),
    }
}

/// A Result goes to the sender of the request it names, whatever Address
/// its Send writes: the Nexus routes it, and the Address named is given
/// nothing.
#[test]
fn a_result_goes_to_the_requests_sender_whatever_address_it_names() {
    let fixture = NexusFixture::new();
    let asker = at("routeAsker");
    let answerer = at("routeAnswerer");
    let elsewhere = at("routeElsewhere");
    for address in [&asker, &answerer, &elsewhere] {
        asleep(&fixture, address);
    }
    let question = admitted(
        &fixture,
        &answerer,
        SentBy::Sender(asker.clone()),
        Request::Question("Is the drain fix green?".into()),
    );
    delivered(&fixture, &answerer, &question);
    let answer = fixture
        .nexus
        .send_request(answer_to(&elsewhere, &question.request_id), None);
    assert!(
        matches!(answer.response, Output::SendAccepted(_) | Output::Sent(_)),
        "{:?}",
        answer.response
    );
    assert!(queued(&fixture, &elsewhere).is_empty());
    let routed = queued(&fixture, &asker);
    assert_eq!(routed.len(), 1);
    assert!(matches!(
        &routed[0].message,
        Message::Soft(Request::Result(flow_ethos::ResultPayload::Answered(result)))
            if result.request_id == question.request_id
    ));
    assert_eq!(
        fixture
            .nexus
            .dispatch(Input::Answer(question.request_id.clone())),
        Output::AnswerRejected(signal_flow::AnswerRejection::Routed)
    );
}

/// A request whose sender is no Address keeps its answer: the Send is
/// answered Sent with the request's id, nothing is queued anywhere, the
/// request is closed, and Answer reads the answer with its sender and
/// priority. Before it, Answer says the request is open; an id no request
/// holds is unknown.
#[test]
fn an_answer_to_an_unbound_request_is_kept_and_read_by_answer() {
    let fixture = NexusFixture::new();
    let answerer = at("keptAnswerer");
    let elsewhere = at("keptElsewhere");
    asleep(&fixture, &answerer);
    asleep(&fixture, &elsewhere);
    let question = admitted(
        &fixture,
        &answerer,
        SentBy::Unbound,
        Request::Question("Which revision?".into()),
    );
    delivered(&fixture, &answerer, &question);
    assert_eq!(
        fixture
            .nexus
            .dispatch(Input::Answer(question.request_id.clone())),
        Output::AnswerRejected(signal_flow::AnswerRejection::Open)
    );
    let answer = fixture
        .nexus
        .send_request(answer_to(&elsewhere, &question.request_id), None);
    assert_eq!(
        answer.response,
        Output::Sent(signal_flow::VisibleSendReceipt(question.request_id.clone()))
    );
    assert!(queued(&fixture, &elsewhere).is_empty());
    assert!(queued(&fixture, &answerer).is_empty());
    assert_eq!(
        fixture
            .nexus
            .dispatch(Input::Answer(question.request_id.clone())),
        Output::Answered(VisibleNativeInput {
            visible_sender: VisibleSender::Unbound,
            delivered_message: DeliveredMessage::Soft(DeliveredRequest::Result(
                signal_flow::DeliveredRequest_Result_Data {
                    request_id: question.request_id.clone(),
                    string: "Reviewed; green.".into(),
                }
            )),
        })
    );
    // The request is closed: a second answer resolves nothing.
    assert_eq!(
        fixture
            .nexus
            .send_request(answer_to(&elsewhere, &question.request_id), None)
            .response,
        Output::SendRefused(signal_flow::VisibleRefusal::Unresolved)
    );
    assert_eq!(
        fixture
            .nexus
            .dispatch(Input::Answer(flow_ethos::RequestId("zzzz".into()))),
        Output::AnswerRejected(signal_flow::AnswerRejection::UnknownRequest)
    );
}

/// Remind keeps a reminder for the Address it names and answers its id;
/// when its minutes lapse the Address is sent MiddleAbrupt
/// `Reminder.{ id words }` from whoever asked, marked Wake so a sleeping
/// Address is woken for it, and its request closes.
#[test]
fn remind_sends_its_address_a_reminder_when_its_minutes_lapse() {
    use crate::reminding::RemindsCallers;
    use crate::store::scheduled_reminder::SchedulesReminders;
    let fixture = NexusFixture::new();
    let reminded = at("remindedSeat");
    let asker = at("remindAsker");
    asleep(&fixture, &reminded);
    asleep(&fixture, &asker);
    let Output::Reminded(request_id) = fixture.nexus.remind(
        signal_flow::Remind_Data {
            address: reminded.clone(),
            minutes: flow_ethos::Minutes(30),
            string: "Check the gate.".into(),
        },
        SentBy::Sender(asker.clone()),
    ) else {
        panic!("a reminder is kept")
    };
    assert!(queued(&fixture, &reminded).is_empty(), "not yet due");
    assert!(
        fixture
            .nexus
            .store
            .scheduled_reminder(&request_id)
            .unwrap()
            .is_some(),
        "the reminder is kept"
    );
    // Its minutes lapse.
    fixture
        .nexus
        .store
        .rewind_scheduled_reminder(&request_id, 0)
        .unwrap();
    fixture.nexus.send_due_reminders();
    let sent = queued(&fixture, &reminded);
    assert!(
        sent.iter()
            .any(|entry| entry.sent_by == SentBy::Sender(asker.clone())
                && entry.delivery == flow_ethos::Delivery::Wake
                && entry.message
                    == Message::MiddleAbrupt(Request::Reminder(flow_ethos::Reminder_Data {
                        request_id: request_id.clone(),
                        string: "Check the gate.".into(),
                    }))),
        "{sent:?}"
    );
    assert_eq!(
        fixture.nexus.store.scheduled_reminder(&request_id).unwrap(),
        None
    );
    assert_eq!(
        fixture.nexus.dispatch(Input::Cancel(request_id)),
        Output::CancelRejected(RequestRejection::Closed)
    );
}

/// A reminder is a request like any other: Cancel ends it before it is
/// sent, and its asker is told. Its Minutes meets a Task's rule, and an
/// Address Flow holds no metaflow for is refused.
#[test]
fn cancel_ends_a_reminder_and_remind_refuses_what_a_task_refuses() {
    use crate::store::scheduled_reminder::SchedulesReminders;
    let fixture = NexusFixture::new();
    let reminded = at("cancelledReminder");
    let asker = at("cancellingAsker");
    asleep(&fixture, &reminded);
    asleep(&fixture, &asker);
    let remind = |minutes: i64, address: &Address| signal_flow::Remind_Data {
        address: address.clone(),
        minutes: flow_ethos::Minutes(minutes),
        string: "Later.".into(),
    };
    let Output::Reminded(request_id) = fixture
        .nexus
        .remind(remind(5, &reminded), SentBy::Sender(asker.clone()))
    else {
        panic!("a reminder is kept")
    };
    assert_eq!(
        fixture.nexus.dispatch(Input::Cancel(request_id.clone())),
        Output::Cancelled(request_id.clone())
    );
    assert_eq!(
        fixture.nexus.store.scheduled_reminder(&request_id).unwrap(),
        None
    );
    assert!(notices(&fixture, &asker).is_empty());
    assert!(notices(&fixture, &reminded).is_empty());
    assert!(queued(&fixture, &reminded).is_empty());
    assert_eq!(
        fixture.nexus.remind(remind(0, &reminded), SentBy::Unbound),
        Output::Refused(signal_flow::Refusal::MinutesRefused(
            signal_flow::MinutesRefusal::Zero
        ))
    );
    assert_eq!(
        fixture
            .nexus
            .remind(remind(5, &at("noSuchSeat")), SentBy::Unbound),
        Output::Refused(signal_flow::Refusal::Unknown(
            signal_flow::Unknown_Data::Address(at("noSuchSeat"))
        ))
    );
}

/// The record CloseRequest left for the request bound to
/// `logical_delivery_id`.
fn closed_record(
    fixture: &NexusFixture,
    logical_delivery_id: &signal_flow::LogicalDeliveryId,
) -> Option<crate::store::closed_request::ClosedRequest> {
    use crate::store::closed_request::RecordsClosedRequests;
    fixture
        .nexus
        .store
        .closed_request(logical_delivery_id)
        .expect("the closed requests read")
}

/// A delivered Question is closed by anyone with no answer: its id archived,
/// no Notice queued for its sender or its recipient, and its closer
/// recorded. A Result naming it afterwards is refused Unresolved, and a
/// second CloseRequest names no request.
#[test]
fn close_request_closes_a_delivered_question_and_tells_no_one() {
    let fixture = NexusFixture::new();
    let sender = at("closeSender");
    let recipient = at("closeRecipient");
    let elsewhere = at("closeElsewhere");
    for address in [&sender, &recipient, &elsewhere] {
        asleep(&fixture, address);
    }
    let order = admitted(
        &fixture,
        &recipient,
        SentBy::Sender(sender.clone()),
        Request::Question("Review the drain fix.".into()),
    );
    delivered(&fixture, &recipient, &order);

    assert_eq!(
        fixture
            .nexus
            .dispatch(Input::CloseRequest(order.request_id.clone())),
        Output::RequestClosed(order.request_id.clone())
    );
    assert_eq!(
        fixture
            .nexus
            .store
            .request_binding(&order.request_id)
            .unwrap(),
        None,
        "the request's id is closed"
    );
    assert!(
        queued(&fixture, &sender).is_empty(),
        "the sender is told nothing"
    );
    assert!(
        queued(&fixture, &recipient).is_empty(),
        "the recipient is told nothing"
    );
    assert_eq!(
        closed_record(&fixture, &order.logical_delivery_id),
        Some(crate::store::closed_request::ClosedRequest {
            logical_delivery_id: order.logical_delivery_id.clone(),
            request_id: order.request_id.clone(),
            closer: crate::store::closed_request::Closer(None),
        }),
        "no calling process stands behind a dispatch"
    );
    assert_eq!(
        fixture
            .nexus
            .send_request(answer_to(&elsewhere, &order.request_id), None)
            .response,
        Output::SendRefused(signal_flow::VisibleRefusal::Unresolved),
        "a Result finds the request closed"
    );
    assert!(queued(&fixture, &sender).is_empty());
    assert_eq!(
        fixture
            .nexus
            .dispatch(Input::CloseRequest(order.request_id.clone())),
        Output::CloseRequestRejected(RequestRejection::UnknownRequest)
    );
}

/// A delivered Task is closed with its reminder, no one told; a Done
/// naming it afterwards is refused Unresolved.
#[test]
fn close_request_closes_a_delivered_task_with_its_reminder() {
    let fixture = NexusFixture::new();
    let sender = at("closeTaskSender");
    let recipient = at("closeTaskRecipient");
    asleep(&fixture, &sender);
    asleep(&fixture, &recipient);
    let task = admitted(
        &fixture,
        &recipient,
        SentBy::Sender(sender.clone()),
        Request::Task(flow_ethos::AnswerableTask {
            minutes: flow_ethos::Minutes(30),
            string: "Land the line.".into(),
        }),
    );
    delivered(&fixture, &recipient, &task);
    assert!(
        fixture
            .nexus
            .store
            .reminder(&task.logical_delivery_id.0)
            .unwrap()
            .is_some()
    );
    assert_eq!(
        fixture.nexus.close_request(&task.request_id, None),
        Output::RequestClosed(task.request_id.clone())
    );
    assert_eq!(
        fixture
            .nexus
            .store
            .reminder(&task.logical_delivery_id.0)
            .unwrap(),
        None,
        "its reminder went with it"
    );
    assert!(queued(&fixture, &sender).is_empty());
    assert!(queued(&fixture, &recipient).is_empty());
    let done = SendRequest {
        address: recipient.clone(),
        sendable_message: signal_flow::SendableMessage::Soft(signal_flow::SendableRequest::Done(
            signal_flow::SendableRequest_Done_Data {
                request_id: task.request_id.clone(),
                string: "Landed.".into(),
            },
        )),
    };
    assert_eq!(
        fixture.nexus.send_request(done, None).response,
        Output::SendRefused(signal_flow::VisibleRefusal::Unresolved),
        "a Done finds the Task closed"
    );
}

/// A Task still queued is closed: it leaves its queue with its reminder,
/// and no one is told.
#[test]
fn close_request_takes_a_queued_task_out_and_tells_no_one() {
    let fixture = NexusFixture::new();
    let sender = at("closeQueuedSender");
    let recipient = at("closeQueuedRecipient");
    asleep(&fixture, &sender);
    asleep(&fixture, &recipient);
    let task = admitted(
        &fixture,
        &recipient,
        SentBy::Sender(sender.clone()),
        Request::Task(flow_ethos::AnswerableTask {
            minutes: flow_ethos::Minutes(30),
            string: "Land the line.".into(),
        }),
    );
    assert_eq!(
        fixture.nexus.close_request(&task.request_id, None),
        Output::RequestClosed(task.request_id.clone())
    );
    assert!(queued(&fixture, &recipient).is_empty(), "it left the queue");
    assert!(queued(&fixture, &sender).is_empty(), "no one is told");
    assert_eq!(
        fixture
            .nexus
            .store
            .reminder(&task.logical_delivery_id.0)
            .unwrap(),
        None
    );
}

/// CloseRequest is refused as Cancel is: an id no request holds is
/// UnknownRequest, and a request already answered is Closed. A request
/// with an unfinished attempt can be closed.
#[test]
fn close_request_is_refused_as_cancel_is() {
    use crate::store::queue_delivery::{
        AccountsQueueDelivery, NativeHistoryAnchor, NativeTranscriptCursor, PrepareQueueDelivery,
    };
    let fixture = NexusFixture::new();
    let sender = at("refusedSender");
    let recipient = at("refusedRecipient");
    asleep(&fixture, &sender);
    asleep(&fixture, &recipient);

    assert_eq!(
        fixture
            .nexus
            .dispatch(Input::CloseRequest(flow_ethos::RequestId("zzzz".into()))),
        Output::CloseRequestRejected(RequestRejection::UnknownRequest)
    );

    let answered = admitted(
        &fixture,
        &recipient,
        SentBy::Sender(sender.clone()),
        Request::Question("Answered already.".into()),
    );
    delivered(&fixture, &recipient, &answered);
    assert!(matches!(
        fixture
            .nexus
            .send_request(answer_to(&sender, &answered.request_id), None)
            .response,
        Output::SendAccepted(_) | Output::Sent(_)
    ));
    assert_eq!(
        fixture
            .nexus
            .dispatch(Input::CloseRequest(answered.request_id.clone())),
        Output::CloseRequestRejected(RequestRejection::Closed)
    );

    // A seat Awake as unsettled-flow is handed a request whose receipt is
    // not yet settled.
    let seat = at("refusedSeat");
    fixture.nexus.store.seed_metaflow(Metaflow {
        address: seat.clone(),
        state: State::Awake(flow_ethos::FlowId("unsettled-flow".into())),
        past: flow_ethos::Past(vec![]),
        queue: flow_ethos::Queue(vec![]),
    });
    let unsettled = admitted(
        &fixture,
        &seat,
        SentBy::Sender(sender.clone()),
        Request::Order("Still being handed over.".into()),
    );
    let PrepareQueueDelivery::Prepared(attempt) = fixture
        .nexus
        .store
        .prepare_oldest_delivery(
            &seat,
            "unsettled-flow",
            "unsettled-flow-session",
            HarnessKind::Claude,
            NativeHistoryAnchor::ClaudeTranscript(NativeTranscriptCursor {
                device: crate::generated::memory::Device(0),
                inode: crate::generated::memory::Inode(0),
                offset: crate::generated::memory::Offset(0),
            }),
        )
        .expect("the oldest request prepares")
    else {
        panic!("the request is prepared");
    };
    assert_eq!(
        fixture
            .nexus
            .dispatch(Input::CloseRequest(unsettled.request_id.clone())),
        Output::RequestClosed(unsettled.request_id.clone())
    );
    assert!(
        fixture
            .nexus
            .store
            .request_binding(&unsettled.request_id)
            .unwrap()
            .is_none()
    );
    assert!(closed_record(&fixture, &unsettled.logical_delivery_id).is_some());
    assert!(matches!(
        fixture.nexus.store.queue_delivery_attempt(&attempt.queue_attempt_id.0).unwrap()
            .unwrap().queue_delivery_phase,
        crate::store::queue_delivery::QueueDeliveryPhase::Abandoned(meta_signal_flow::AbandonmentRecord {
            abandonment_cause: meta_signal_flow::AbandonmentCause::RequestEnded(id), ..
        }) if id == unsettled.request_id
    ));
    assert!(queued(&fixture, &seat).is_empty());
}

/// A reminder not yet sent is closed where it waits: it is never sent,
/// its asker is told nothing, and its closer is recorded.
#[test]
fn close_request_ends_a_reminder_and_tells_its_asker_nothing() {
    use crate::store::scheduled_reminder::{MarksScheduledReminder, SchedulesReminders};
    let fixture = NexusFixture::new();
    let reminded = at("closedReminder");
    let asker = at("closingAsker");
    asleep(&fixture, &reminded);
    asleep(&fixture, &asker);
    let Output::Reminded(request_id) = fixture.nexus.remind(
        signal_flow::Remind_Data {
            address: reminded.clone(),
            minutes: flow_ethos::Minutes(5),
            string: "Later.".into(),
        },
        SentBy::Sender(asker.clone()),
    ) else {
        panic!("a reminder is kept")
    };
    assert_eq!(
        fixture
            .nexus
            .dispatch(Input::CloseRequest(request_id.clone())),
        Output::RequestClosed(request_id.clone())
    );
    assert_eq!(
        fixture.nexus.store.scheduled_reminder(&request_id).unwrap(),
        None
    );
    assert!(
        queued(&fixture, &asker).is_empty(),
        "its asker is told nothing"
    );
    assert!(queued(&fixture, &reminded).is_empty());
    assert!(closed_record(&fixture, &request_id.scheduled_marker()).is_some());
    assert_eq!(
        fixture
            .nexus
            .dispatch(Input::CloseRequest(request_id.clone())),
        Output::CloseRequestRejected(RequestRejection::Closed)
    );
}

/// The process that closed a request is recorded as the connection showed
/// it: a CloseRequest over the ordinary socket from this test's process
/// records this process.
#[test]
fn close_request_records_the_process_its_connection_showed() {
    use super::GreetsOrdinarySocket;
    use crate::{CarriesSignalFrames, Connection, ServesConnection};
    use std::os::unix::net::UnixStream;
    use std::time::Duration;
    let fixture = NexusFixture::new();
    let sender = at("socketCloseSender");
    let recipient = at("socketCloseRecipient");
    asleep(&fixture, &sender);
    asleep(&fixture, &recipient);
    let order = admitted(
        &fixture,
        &recipient,
        SentBy::Sender(sender.clone()),
        Request::Question("Review the drain fix.".into()),
    );
    delivered(&fixture, &recipient, &order);
    let (peer, mut client) = UnixStream::pair().unwrap();
    client
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    std::thread::scope(|scope| {
        let server = scope.spawn(|| {
            Connection::accepted(peer)
                .unwrap()
                .serve_ordinary(&fixture.nexus)
        });
        client.greet_ordinary();
        client
            .write_query(&Input::CloseRequest(order.request_id.clone()))
            .unwrap();
        assert_eq!(
            client.read_response().unwrap(),
            Output::RequestClosed(order.request_id.clone())
        );
        assert!(server.join().unwrap().is_ok());
    });
    let this_process =
        FlowStore::process_identity_for_pid(std::process::id()).expect("this process is read");
    assert_eq!(
        closed_record(&fixture, &order.logical_delivery_id).map(|record| record.closer),
        Some(crate::store::closed_request::Closer(Some(this_process)))
    );
    assert!(queued(&fixture, &sender).is_empty());
    assert!(queued(&fixture, &recipient).is_empty());
}

#[test]
fn abandoning_a_queue_head_is_terminal_and_the_next_request_stays_queued() {
    use crate::store::queue_delivery::{
        AccountsQueueDelivery, NativeHistoryAnchor, PrepareQueueDelivery,
    };
    use meta_signal_flow as meta;
    let fixture = NexusFixture::new();
    let address = at("abandonQueueHead");
    let flow = flow_ethos::FlowId("old-seat".into());
    awake_as(&fixture, &address, &flow);
    let task = admitted(
        &fixture,
        &address,
        SentBy::Unbound,
        Request::Task(flow_ethos::AnswerableTask {
            minutes: flow_ethos::Minutes(30),
            string: "Read this.".into(),
        }),
    );
    let next = admitted(
        &fixture,
        &address,
        SentBy::Unbound,
        Request::Order("Then this.".into()),
    );
    let PrepareQueueDelivery::Prepared(attempt) = fixture
        .nexus
        .store
        .prepare_oldest_delivery(
            &address,
            &flow.0,
            "old-thread",
            HarnessKind::Codex,
            NativeHistoryAnchor::CodexTurns(None),
        )
        .unwrap()
    else {
        panic!("the first request prepared")
    };
    let abandon = |id, why: &str| {
        fixture
            .nexus
            .dispatch_meta(meta::Input::AbandonQueueDelivery(
                meta::AbandonQueueDelivery_Data {
                    queue_attempt_id: id,
                    queue_resolution_reason: meta::QueueResolutionReason(why.into()),
                },
            ))
    };
    assert_eq!(
        abandon(
            meta::QueueAttemptId("unknown-attempt".into()),
            "Stop this attempt."
        ),
        meta::Output::AbandonQueueDeliveryRejected(
            meta::AbandonQueueDeliveryRejected_Data::UnknownAttempt
        )
    );
    assert_eq!(
        abandon(attempt.queue_attempt_id.clone(), " "),
        meta::Output::AbandonQueueDeliveryRejected(
            meta::AbandonQueueDeliveryRejected_Data::EmptyReason
        )
    );
    assert!(matches!(
        abandon(attempt.queue_attempt_id.clone(), "The owner closed the seat."),
        meta::Output::QueueDeliveryAbandoned(meta::QueueDeliveryAbandoned_Data { address: found, .. })
            if found == address
    ));
    assert_eq!(
        abandon(attempt.queue_attempt_id, "The owner closed the seat."),
        meta::Output::AbandonQueueDeliveryRejected(
            meta::AbandonQueueDeliveryRejected_Data::AlreadyFinished
        )
    );
    assert_eq!(
        fixture
            .nexus
            .store
            .logical_delivery_snapshot(&task.logical_delivery_id)
            .unwrap()
            .unwrap()
            .delivery_state,
        DeliveryState::Abandoned
    );
    assert!(
        fixture
            .nexus
            .store
            .request_binding(&task.request_id)
            .unwrap()
            .is_some()
    );
    assert!(matches!(
        fixture.nexus.dispatch_meta(meta::Input::ReadQueueDeliveries(address.clone())),
        meta::Output::QueueDeliveriesRead(read)
            if read.visible_queue_delivery_vector.iter().any(|delivery| matches!(
                delivery.visible_queue_delivery_standing,
                meta::VisibleQueueDeliveryStanding::Abandoned(meta::VisibleAbandonmentRecord {
                    visible_abandonment_cause: meta::VisibleAbandonmentCause::Owner(_), ..
                })
            ))
    ));
    assert_eq!(
        queued(&fixture, &address)[0].logical_delivery_id,
        next.logical_delivery_id
    );
    assert!(fixture.typed().is_none());
}

#[test]
fn startup_settles_stranded_attempts_without_waking_their_metaflow() {
    use crate::generated::memory::{NativeReceiptText, TurnId};
    use crate::store::delivery_retirement::SettlesRetiredDeliveries;
    use crate::store::logical_delivery::HoldsOpenRequests;
    use crate::store::queue_delivery::{
        AccountsQueueDelivery, NativeDeliveryReceipt, NativeHistoryAnchor, PrepareQueueDelivery,
        QueueDeliveryPhase,
    };
    use crate::waking::SleepsAndWakesSeats;
    for receipt_recorded in [false, true] {
        let fixture = NexusFixture::new();
        let address = at("strandedAttempt");
        let flow = flow_ethos::FlowId("retired-seat".into());
        awake_as(&fixture, &address, &flow);
        let task = admitted(
            &fixture,
            &address,
            SentBy::Unbound,
            Request::Task(flow_ethos::AnswerableTask {
                minutes: flow_ethos::Minutes(30),
                string: "Read this.".into(),
            }),
        );
        let PrepareQueueDelivery::Prepared(attempt) = fixture
            .nexus
            .store
            .prepare_oldest_delivery(
                &address,
                &flow.0,
                "retired-thread",
                HarnessKind::Codex,
                NativeHistoryAnchor::CodexTurns(None),
            )
            .unwrap()
        else {
            panic!("the request prepared")
        };
        assert!(
            fixture
                .nexus
                .store
                .mark_delivery_uncertain(&attempt.queue_attempt_id.0)
                .unwrap()
        );
        assert!(
            fixture
                .nexus
                .store
                .park_uncertain_delivery(&attempt.queue_attempt_id.0)
                .unwrap()
        );
        if receipt_recorded {
            assert!(
                fixture
                    .nexus
                    .store
                    .record_delivery_receipt(
                        &attempt.queue_attempt_id.0,
                        NativeDeliveryReceipt {
                            native_request_id: attempt.native_request_id,
                            thread_id: attempt.thread_id,
                            turn_id: TurnId("recorded-receipt".into()),
                            native_receipt_text: NativeReceiptText(
                                "recorded before retirement".into()
                            ),
                        }
                    )
                    .unwrap()
            );
        }
        fixture.nexus.store.seed_metaflow(Metaflow {
            address: address.clone(),
            state: State::Asleep,
            past: flow_ethos::Past(vec![flow.clone()]),
            queue: flow_ethos::Queue(vec![]),
        });
        fixture.nexus.settle_stranded_deliveries().unwrap();
        let state = fixture
            .nexus
            .store
            .logical_delivery_snapshot(&task.logical_delivery_id)
            .unwrap()
            .unwrap()
            .delivery_state;
        if receipt_recorded {
            assert!(matches!(state, DeliveryState::Delivered(_)));
        } else {
            assert_eq!(state, DeliveryState::Abandoned);
            assert!(matches!(
                fixture.nexus.store.queue_delivery_attempt(&attempt.queue_attempt_id.0).unwrap().unwrap().queue_delivery_phase,
                QueueDeliveryPhase::Abandoned(meta_signal_flow::AbandonmentRecord {
                    abandonment_cause: meta_signal_flow::AbandonmentCause::FlowRetired(retired), ..
                }) if retired == flow
            ));
        }
        let open = fixture.nexus.store.open_requests(&address).unwrap();
        assert_eq!(open.len(), 1);
        assert_eq!(open[0].request_id, task.request_id);
        let asleep = fixture.nexus.store.metaflow(&address).unwrap().unwrap();
        assert_eq!(asleep.state, State::Asleep);
        assert!(!fixture.nexus.wake_is_due(&asleep));
        assert!(fixture.typed().is_none());
        assert_eq!(
            fixture
                .nexus
                .dispatch(Input::CloseRequest(task.request_id.clone())),
            Output::RequestClosed(task.request_id)
        );
    }
}

fn work_with(policy: flow_ethos::WorkNotificationPolicy) -> Request {
    Request::Work(flow_ethos::WorkRequest {
        string: "Read the witness.".into(),
        work_notification_policy: policy,
    })
}

fn set_work_default(fixture: &NexusFixture, kind: flow_ethos::NotificationType, address: &Address) {
    let change = meta_signal_flow::ConfigurationChange::DefaultNotificationDestination(
        meta_signal_flow::DefaultNotificationDestination {
            notification_type: kind,
            address: address.clone(),
        },
    );
    assert_eq!(
        fixture
            .nexus
            .dispatch_meta(meta_signal_flow::Input::Configure(change.clone())),
        meta_signal_flow::Output::Configured(change),
    );
}

fn work_notices_here(
    fixture: &NexusFixture,
    address: &Address,
) -> Vec<flow_ethos::WorkNotification> {
    queued(fixture, address)
        .into_iter()
        .filter_map(|entry| match entry.message {
            Message::Soft(Request::WorkNotification(value)) => {
                assert_eq!(entry.delivery, flow_ethos::Delivery::Queue);
                assert_eq!(entry.sent_by, SentBy::Flow);
                Some(value)
            }
            _ => None,
        })
        .collect()
}

#[test]
fn work_notice_uses_defaults_at_commit_and_retains_exact_route_across_recovery() {
    use crate::store::work_notices::KeepsWorkNotices as _;
    use crate::work_notices::DeliversWorkNotices as _;
    use flow_ethos::{NotificationType, WorkNotification, WorkNotificationPolicy};

    let fixture = NexusFixture::new();
    let store = &fixture.nexus.store;
    let worker = at("noticeWorker");
    let sender = at("noticeSender");
    let original = at("oldDefault");
    let selected = at("currentDefault");
    let following = at("laterDefault");
    for address in [&worker, &sender, &original, &selected, &following] {
        asleep(&fixture, address);
    }
    let default = || {
        store
            .accept_logical_delivery(
                &worker,
                SentBy::Sender(sender.clone()),
                flow_ethos::Delivery::Queue,
                Message::Soft(work_with(WorkNotificationPolicy::Default)),
                None,
            )
            .unwrap()
    };
    assert_eq!(
        default(),
        PrivateSendResponse::Refused(signal_flow::Refusal::MissingDefaultNotificationDestination(
            NotificationType::WorkDone
        ))
    );
    set_work_default(&fixture, NotificationType::WorkDone, &original);
    assert_eq!(
        default(),
        PrivateSendResponse::Refused(signal_flow::Refusal::MissingDefaultNotificationDestination(
            NotificationType::WorkCancelled
        ))
    );
    assert!(queued(&fixture, &worker).is_empty());
    set_work_default(&fixture, NotificationType::WorkCancelled, &original);
    let PrivateSendResponse::SendAccepted(work) = default() else {
        panic!("both configured types, one accepted request");
    };
    assert!(store.pending_work_notices().unwrap().is_empty());
    set_work_default(&fixture, NotificationType::WorkCancelled, &selected);
    assert_eq!(
        fixture.nexus.cancel(&work.request_id, &Origin::Unbound),
        Output::Cancelled(work.request_id.clone()),
    );
    set_work_default(&fixture, NotificationType::WorkCancelled, &following);
    assert_eq!(
        store
            .work_outcome(&work.request_id)
            .unwrap()
            .unwrap()
            .address_vector,
        vec![selected.clone()],
    );
    fixture.nexus.deliver_work_notices();
    fixture.nexus.deliver_work_notices();
    assert_eq!(
        work_notices_here(&fixture, &selected),
        vec![WorkNotification {
            request_id: work.request_id,
            notification_type: NotificationType::WorkCancelled,
            address: sender,
        }]
    );
    assert!(work_notices_here(&fixture, &original).is_empty());
    assert!(work_notices_here(&fixture, &following).is_empty());
    assert!(store.pending_work_notices().unwrap().is_empty());
    assert_eq!(
        store.metaflow(&selected).unwrap().unwrap().state,
        State::Asleep
    );
    assert!(fixture.typed().is_none());
}

#[test]
fn explicit_work_routes_leave_the_acting_sender_silent_and_designer_inspectable() {
    use crate::store::work_notices::KeepsWorkNotices as _;
    use crate::work_notices::DeliversWorkNotices as _;
    use flow_ethos::{NotificationType, Notified, WorkNotificationPolicy};

    let fixture = NexusFixture::new();
    let worker = at("overrideWorker");
    let sender = at("overrideSender");
    let designer = at("overrideDesigner");
    let cold = at("coldObserver");
    let sender_flow = flow_ethos::FlowId("overrideSenderFlow".into());
    awake_as(&fixture, &sender, &sender_flow);
    for address in [&worker, &designer, &cold] {
        asleep(&fixture, address);
    }
    let designer_policy = meta_signal_flow::ConfigurationChange::DesignerNoticePolicy(
        meta_signal_flow::DesignerNoticePolicy(vec![designer.clone()]),
    );
    assert_eq!(
        fixture
            .nexus
            .dispatch_meta(meta_signal_flow::Input::Configure(designer_policy.clone())),
        meta_signal_flow::Output::Configured(designer_policy)
    );
    let notices = vec![Notified {
        notification_type: NotificationType::WorkCancelled,
        address_vector: vec![sender.clone(), designer.clone(), cold.clone(), cold.clone()],
    }];
    let store = &fixture.nexus.store;
    let from_shell = store
        .accept_logical_delivery(
            &worker,
            SentBy::Unbound,
            flow_ethos::Delivery::Queue,
            Message::Soft(work_with(WorkNotificationPolicy::Destinations(
                notices.clone(),
            ))),
            None,
        )
        .unwrap();
    assert_eq!(
        from_shell,
        PrivateSendResponse::Refused(signal_flow::Refusal::UnaddressedNotificationSender)
    );
    let accepted = admitted(
        &fixture,
        &worker,
        SentBy::Sender(sender.clone()),
        work_with(WorkNotificationPolicy::Destinations(notices)),
    );
    assert_eq!(
        fixture
            .nexus
            .cancel(&accepted.request_id, &origin_of(sender_flow)),
        Output::Cancelled(accepted.request_id.clone())
    );
    fixture.nexus.deliver_work_notices();
    let outcome = store.work_outcome(&accepted.request_id).unwrap().unwrap();
    assert_eq!(outcome.address_vector, vec![designer.clone(), cold.clone()]);
    assert_eq!(outcome.work_notification.address, sender);
    assert_eq!(
        work_notices_here(&fixture, &cold),
        vec![outcome.work_notification]
    );
    assert!(work_notices_here(&fixture, &sender).is_empty());
    assert!(work_notices_here(&fixture, &designer).is_empty());
    assert!(fixture.typed().is_none());
}

#[test]
fn quiet_work_accepts_an_unbound_sender_and_emits_no_terminal_notification() {
    use crate::store::work_notices::KeepsWorkNotices as _;
    use crate::work_notices::DeliversWorkNotices as _;
    use flow_ethos::WorkNotificationPolicy;

    let fixture = NexusFixture::new();
    let worker = at("quietWorker");
    asleep(&fixture, &worker);
    let accepted = admitted(
        &fixture,
        &worker,
        SentBy::Unbound,
        work_with(WorkNotificationPolicy::Quiet),
    );
    assert_eq!(
        fixture.nexus.cancel(&accepted.request_id, &Origin::Unbound),
        Output::Cancelled(accepted.request_id.clone())
    );
    fixture.nexus.deliver_work_notices();
    assert!(
        fixture
            .nexus
            .store
            .work_outcome(&accepted.request_id)
            .unwrap()
            .is_none()
    );
    assert!(
        fixture
            .nexus
            .store
            .pending_work_notices()
            .unwrap()
            .is_empty()
    );
}

#[test]
fn configured_designer_inspects_routine_mail_admitted_before_and_after_the_policy() {
    use crate::store::work_notices::KeepsWorkNotices as _;

    let fixture = NexusFixture::new();
    let designer = at("designerInspection");
    asleep(&fixture, &designer);
    let queued_before = admitted(
        &fixture,
        &designer,
        SentBy::Flow,
        Request::Notice("Already queued when the policy takes effect.".into()),
    );
    let actual_work = admitted(
        &fixture,
        &designer,
        SentBy::Unbound,
        Request::Order("Actual requested work stays queued.".into()),
    );
    let change = meta_signal_flow::ConfigurationChange::DesignerNoticePolicy(
        meta_signal_flow::DesignerNoticePolicy(vec![designer.clone()]),
    );
    assert_eq!(
        fixture
            .nexus
            .dispatch_meta(meta_signal_flow::Input::Configure(change.clone())),
        meta_signal_flow::Output::Configured(change)
    );
    let admitted_after = admitted(
        &fixture,
        &designer,
        SentBy::Flow,
        Request::Notice("Inspection-only admission.".into()),
    );
    assert_eq!(admitted_after.delivery_state, DeliveryState::InspectionOnly);

    let store = &fixture.nexus.store;
    store
        .reserve_routine_notices_for_inspection(&designer)
        .unwrap();
    store
        .reserve_routine_notices_for_inspection(&designer)
        .unwrap();
    assert_eq!(queued(&fixture, &designer).len(), 1);
    assert_eq!(
        queued(&fixture, &designer)[0].logical_delivery_id,
        actual_work.logical_delivery_id
    );
    assert_eq!(
        store
            .logical_delivery_snapshot(&queued_before.logical_delivery_id)
            .unwrap()
            .unwrap()
            .delivery_state,
        DeliveryState::InspectionOnly,
    );
    assert!(
        store
            .request_binding(&queued_before.request_id)
            .unwrap()
            .is_some()
    );
    assert!(fixture.typed().is_none());
}

struct StopsWorkNoticeWitness<'a>(&'a crate::RunningNexus);

impl Drop for StopsWorkNoticeWitness<'_> {
    fn drop(&mut self) {
        use crate::StopsNexus;
        self.0.stop(&[]);
    }
}

fn ordinary_work_notice_reply(
    fixture: &NexusFixture,
    input: Input,
    process: signal_flow::Process,
) -> Output {
    use crate::CarriesSignalFrames;
    use crate::connection_adapter::AdaptsDecodedConnection;
    let (peer, mut caller) = std::os::unix::net::UnixStream::pair().unwrap();
    crate::Connection {
        peer,
        process: Some(process),
    }
    .serve_decoded_ordinary(&fixture.nexus, input)
    .expect("ordinary connection served");
    caller.read_response().expect("typed ordinary response")
}

fn awaiting_visible_work_done(
    fixture: &NexusFixture,
    requester: &Address,
    selected: &Address,
    policy: flow_ethos::WorkNotificationPolicy,
) -> SendAcceptance {
    let worker = at("core");
    super::bind_existing_caller(fixture, "a001", "pane-1");
    fixture.set_agents(vec![super::codex_agent_in("pane-1", "terminal-pane-1")]);
    for address in [requester, selected] {
        asleep(fixture, address);
    }
    let work = admitted(
        fixture,
        &worker,
        SentBy::Sender(requester.clone()),
        work_with(policy),
    );
    delivered(fixture, &worker, &work);
    work
}

fn done_notice_policy(selected: &Address) -> flow_ethos::WorkNotificationPolicy {
    flow_ethos::WorkNotificationPolicy::Destinations(vec![flow_ethos::Notified {
        notification_type: flow_ethos::NotificationType::WorkDone,
        address_vector: vec![selected.clone()],
    }])
}

fn done_for(work: &SendAcceptance) -> signal_flow::SendableMessage {
    signal_flow::SendableMessage::MiddleAbrupt(signal_flow::SendableRequest::Done(
        signal_flow::SendableRequest_Done_Data {
            request_id: work.request_id.clone(),
            string: "Requested work finished.".into(),
        },
    ))
}

fn await_outbox_admission(fixture: &NexusFixture, address: &Address) -> bool {
    use crate::store::AnnouncesLaunchChanges;
    fixture.nexus.store.launch_changes.until_holds(
        std::time::Instant::now() + std::time::Duration::from_secs(10),
        &mut || !work_notices_here(fixture, address).is_empty(),
    )
}

#[test]
fn deliver_done_wakes_the_outbox_subscriber_without_a_second_request() {
    use crate::store::AnnouncesLaunchChanges;
    use crate::work_notices::DeliversWorkNotices;
    let fixture = NexusFixture::new();
    let requester = at("deliverRequester");
    let selected = at("deliverNotice");
    let (_message, process) = super::waking::message_peer(&fixture);
    let work = awaiting_visible_work_done(
        &fixture,
        &requester,
        &selected,
        done_notice_policy(&selected),
    );
    let Output::Locked(lock) = ordinary_work_notice_reply(
        &fixture,
        Input::Lock(signal_flow::Lock_Data {
            first_address: at("core"),
            second_address: requester,
        }),
        process.clone(),
    ) else {
        panic!("the Message caller locks the reply route")
    };
    let deliver = Input::Deliver(signal_flow::Deliver_Data {
        lock,
        deliver_key: super::deliver_key(401),
        sendable_message: done_for(&work),
    });
    let seen = fixture.nexus.store.work_notice_changes.current();
    let (answer, arrived, replay, after_replay) = std::thread::scope(|scope| {
        let _stop = StopsWorkNoticeWitness(&fixture.nexus);
        scope.spawn(|| fixture.nexus.serve_work_notice_changes_after(seen));
        let answer = ordinary_work_notice_reply(&fixture, deliver.clone(), process.clone());
        let arrived = await_outbox_admission(&fixture, &selected);
        let replay = ordinary_work_notice_reply(&fixture, deliver, process);
        let after_replay = fixture.nexus.store.work_notice_changes.current();
        (answer, arrived, replay, after_replay)
    });
    assert!(matches!(answer, Output::SendAccepted(_)), "{answer:?}");
    assert!(
        arrived,
        "the selected destination admitted before the replay or another request"
    );
    assert_eq!(answer, replay);
    assert_eq!(after_replay, seen + 1);
    let notices = work_notices_here(&fixture, &selected);
    assert_eq!(notices.len(), 1);
    assert_eq!(notices[0].request_id, work.request_id);
    assert_eq!(
        fixture
            .nexus
            .store
            .metaflow(&selected)
            .unwrap()
            .unwrap()
            .state,
        State::Asleep
    );
    assert!(
        fixture.typed().is_none(),
        "an outcome notice never wakes the sleeping destination"
    );
}

#[test]
fn failed_admitted_send_done_still_dispatches_its_exact_notice() {
    use crate::store::AnnouncesLaunchChanges;
    use crate::work_notices::DeliversWorkNotices;
    let fixture = NexusFixture::new();
    let requester = at("unconfiguredReply");
    let selected = at("directNotice");
    let work = awaiting_visible_work_done(
        &fixture,
        &requester,
        &selected,
        done_notice_policy(&selected),
    );
    let caller = super::MarkedProcess::spawn("sleep", &["30"], "pane-1");
    let process = FlowStore::process_identity_for_pid(caller.0.id()).unwrap();
    let input = Input::Send(SendRequest {
        address: requester,
        sendable_message: done_for(&work),
    });
    let seen = fixture.nexus.store.work_notice_changes.current();
    let (answer, arrived) = std::thread::scope(|scope| {
        let _stop = StopsWorkNoticeWitness(&fixture.nexus);
        scope.spawn(|| fixture.nexus.serve_work_notice_changes_after(seen));
        let answer = ordinary_work_notice_reply(&fixture, input, process);
        let arrived = await_outbox_admission(&fixture, &selected);
        (answer, arrived)
    });
    assert!(
        matches!(
            answer,
            Output::Failed(signal_flow::VisibleSendFailure::Admitted(_))
        ),
        "{answer:?}"
    );
    assert!(
        arrived,
        "the outbox is offered even though the direct reply could not start its target"
    );
    let notices = work_notices_here(&fixture, &selected);
    assert_eq!(notices.len(), 1);
    assert_eq!(notices[0].request_id, work.request_id);
    // The stop wakes the waiter only to close; there was one commit event.
    assert_eq!(fixture.nexus.store.work_notice_changes.current(), seen + 2);
}

#[test]
fn reopen_after_commit_dispatches_only_the_original_outbox_member() {
    use crate::store::work_notices::KeepsWorkNotices;
    use crate::work_notices::DeliversWorkNotices;
    let mut fixture = NexusFixture::new();
    let requester = at("restartReply");
    let selected = at("restartNotice");
    let work = awaiting_visible_work_done(
        &fixture,
        &requester,
        &selected,
        done_notice_policy(&selected),
    );
    let caller = super::MarkedProcess::spawn("sleep", &["30"], "pane-1");
    let process = FlowStore::process_identity_for_pid(caller.0.id()).unwrap();
    let reply = ordinary_work_notice_reply(
        &fixture,
        Input::Send(SendRequest {
            address: requester,
            sendable_message: done_for(&work),
        }),
        process,
    );
    assert!(
        matches!(
            reply,
            Output::Failed(signal_flow::VisibleSendFailure::Admitted(_))
        ),
        "{reply:?}"
    );
    assert_eq!(fixture.nexus.store.pending_work_notices().unwrap().len(), 1);
    assert!(
        work_notices_here(&fixture, &selected).is_empty(),
        "the subscriber did not run before the stop"
    );
    fixture.restarted();
    let arrived = std::thread::scope(|scope| {
        let _stop = StopsWorkNoticeWitness(&fixture.nexus);
        scope.spawn(|| fixture.nexus.serve_work_notices());
        await_outbox_admission(&fixture, &selected)
    });
    assert!(arrived);
    assert!(
        fixture
            .nexus
            .store
            .pending_work_notices()
            .unwrap()
            .is_empty()
    );
    let first = work_notices_here(&fixture, &selected);
    assert_eq!(first.len(), 1);
    assert_eq!(first[0].request_id, work.request_id);
    fixture.restarted();
    fixture.nexus.deliver_work_notices();
    assert_eq!(work_notices_here(&fixture, &selected), first);
}

#[test]
fn quiet_done_commits_without_ringing_the_work_notice_subscriber() {
    use crate::store::AnnouncesLaunchChanges;
    use crate::store::work_notices::KeepsWorkNotices;
    let fixture = NexusFixture::new();
    let requester = at("quietReply");
    let selected = at("quietControl");
    let work = awaiting_visible_work_done(
        &fixture,
        &requester,
        &selected,
        flow_ethos::WorkNotificationPolicy::Quiet,
    );
    let caller = super::MarkedProcess::spawn("sleep", &["30"], "pane-1");
    let process = FlowStore::process_identity_for_pid(caller.0.id()).unwrap();
    let seen = fixture.nexus.store.work_notice_changes.current();
    let answer = ordinary_work_notice_reply(
        &fixture,
        Input::Send(SendRequest {
            address: requester,
            sendable_message: done_for(&work),
        }),
        process,
    );
    assert!(
        matches!(
            answer,
            Output::Failed(signal_flow::VisibleSendFailure::Admitted(_))
        ),
        "{answer:?}"
    );
    assert!(matches!(
        fixture
            .nexus
            .store
            .request_binding(&work.request_id)
            .unwrap()
            .unwrap()
            .request_closure,
        crate::generated::memory::RequestClosure::Answered(_),
    ));
    assert_eq!(fixture.nexus.store.work_notice_changes.current(), seen);
    assert!(
        fixture
            .nexus
            .store
            .pending_work_notices()
            .unwrap()
            .is_empty()
    );
    assert!(work_notices_here(&fixture, &selected).is_empty());
}
