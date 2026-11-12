//! The operations that end, empty, force and read a request, a reminder or
//! a queue (Cancel, Silence, Clear, ClearAll, Push, Queues, Hot), and the
//! answers every caller gets alike: the owner at a terminal and a caller in
//! no flow are served as a flow is.

use super::NexusFixture;
use super::waking::SpawnedProcess;
use crate::Dispatches;
use crate::queue_operations::OperatesOnRequestsAndQueues;
use crate::sending::SendsRequests;
use crate::store::logical_delivery::{MintsRequestIds, RecordsLogicalDeliveries};
use crate::store::metaflow::{SeedsMetaflow, SleepsMetaflows};
use crate::store::reminder::RecordsReminders;
use crate::store::turn_end::RecordsTurnEnds;
use crate::store::{FlowStore, ReadsFlowMemory};
use flow_ethos::PermaflowState;
use signal_flow::{
    Address, Aspect, DeliveredMessage, DeliveredRequest, DeliveryState, HarnessKind, Message,
    Model, NativeReceipt, Origin, Permaflow, PrivateSendResponse, Query, Request, RequestRejection,
    Response, SendAcceptance, SendRequest, SentBy, TurnEnd, VisibleNativeInput, VisibleSender,
};

fn at(topic: &str) -> Address {
    Address {
        aspect: Aspect::Mind,
        topic: flow_ethos::Topic(topic.into()),
        model: Model::Sol,
    }
}

fn asleep(fixture: &NexusFixture, address: &Address) {
    fixture.nexus.store.seed_metaflow(Permaflow {
        address: address.clone(),
        permaflow_state: State::Asleep,
        past: flow_ethos::Past(vec![]),
        queue: flow_ethos::Queue(vec![]),
    });
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

/// A delivered Order is cancelled by anyone: closed, its id archived, and
/// its sender and its recipient each told by a Soft Notice from Flow. A
/// second Cancel names no request.
#[test]
fn cancel_closes_a_delivered_order_and_tells_both_ends() {
    let fixture = NexusFixture::new();
    let sender = at("cancelSender");
    let recipient = at("cancelRecipient");
    asleep(&fixture, &sender);
    asleep(&fixture, &recipient);
    let order = admitted(
        &fixture,
        &recipient,
        SentBy::Sender(sender.clone()),
        Request::Order("Review the drain fix.".into()),
    );
    delivered(&fixture, &recipient, &order);

    assert_eq!(
        fixture
            .nexus
            .dispatch(Query::Cancel(order.request_id.clone())),
        Response::Cancelled(order.request_id.clone())
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
    let told = format!("Request {} was cancelled.", order.request_id.0);
    assert_eq!(notices(&fixture, &sender), vec![told.clone()]);
    assert_eq!(notices(&fixture, &recipient), vec![told]);
    assert_eq!(
        fixture
            .nexus
            .dispatch(Query::Cancel(order.request_id.clone())),
        Response::CancelRejected(RequestRejection::UnknownRequest)
    );
}

/// A Task still queued is cancelled: it leaves its queue, its reminder goes
/// with it, and only its sender is told, the recipient never having had it.
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
        fixture.nexus.cancel(&task.request_id),
        Response::Cancelled(task.request_id.clone())
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
    assert_eq!(notices(&fixture, &sender).len(), 1);
    assert!(notices(&fixture, &recipient).is_empty());
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
            .dispatch(Query::Silence(task.request_id.clone())),
        Response::Silenced(task.request_id.clone())
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
        fixture.nexus.dispatch(Query::Silence(order.request_id)),
        Response::SilenceRejected(RequestRejection::NotTask)
    );
    assert_eq!(
        fixture
            .nexus
            .dispatch(Query::Silence(flow_ethos::RequestId("none".into()))),
        Response::SilenceRejected(RequestRejection::UnknownRequest)
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

    let Response::Queues(held) = fixture.nexus.dispatch(Query::Queues) else {
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
        fixture.nexus.dispatch(Query::Clear(first.clone())),
        Response::Cleared(vec![a.request_id, b.request_id])
    );
    assert!(queued(&fixture, &first).is_empty());
    assert_eq!(
        fixture.nexus.dispatch(Query::ClearAll),
        Response::Cleared(vec![c.request_id])
    );
    assert!(queued(&fixture, &second).is_empty());
    assert_eq!(
        fixture.nexus.dispatch(Query::Queues),
        Response::Queues(vec![])
    );
    assert_eq!(
        fixture.nexus.dispatch(Query::Clear(at("neverHeld"))),
        Response::Refused(signal_flow::Refusal::Unknown(
            signal_flow::Unknown_Data::Address(at("neverHeld"))
        ))
    );
}

/// Push wakes every entry of a queue: an Address Flow holds no metaflow for
/// is unknown, an empty queue pushes nothing, and an Asleep metaflow that
/// cannot be woken keeps its queue, each entry now waking it.
#[test]
fn push_forces_a_queue_and_answers_what_was_delivered() {
    let fixture = NexusFixture::new();
    let recipient = at("pushRecipient");
    asleep(&fixture, &recipient);
    let push = |address: &Address, priority| {
        fixture.nexus.dispatch(Query::Push(signal_flow::Push_Data {
            address: address.clone(),
            priority,
        }))
    };
    assert_eq!(
        push(&at("neverHeld"), signal_flow::Priority::HardAbrupt),
        Response::Refused(signal_flow::Refusal::Unknown(
            signal_flow::Unknown_Data::Address(at("neverHeld"))
        ))
    );
    assert_eq!(
        push(&recipient, signal_flow::Priority::HardAbrupt),
        Response::Pushed(vec![])
    );
    let held = admitted(
        &fixture,
        &recipient,
        SentBy::Unbound,
        Request::Notice("held".into()),
    );
    assert_eq!(
        push(&recipient, signal_flow::Priority::HardAbrupt),
        Response::Pushed(vec![]),
        "a metaflow with no launch record is not woken; nothing was delivered"
    );
    let entries = queued(&fixture, &recipient);
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].request_id, held.request_id);
    assert_eq!(entries[0].delivery, flow_ethos::Delivery::Wake);
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
        Response::Hot(vec![signal_flow::Warmth {
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
        fixture.nexus.dispatch(Query::Hot),
        Response::Hot(vec![signal_flow::Warmth {
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
/// ObserveSends answer alike, and an Unbound request is open until it is
/// answered, to whomever the answer names.
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
            matches!(answer.response, Response::SendAccepted(_)),
            "{:?}",
            answer.response
        );
    }
    let entries = queued(&fixture, &recipient);
    assert_eq!(entries.len(), 2);
    assert!(entries.iter().all(|entry| entry.sent_by == SentBy::Unbound));
    assert_eq!(
        fixture.nexus.dispatch(Query::DeliverAll(recipient.clone())),
        fixture
            .nexus
            .dispatch_from(Query::DeliverAll(recipient.clone()), Origin::Unbound),
    );
    let Response::SendsObserved(observed) = fixture.nexus.dispatch(Query::ObserveSends(None))
    else {
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

    // An Order from no Address is answered to whom the answer names, by
    // whoever answers.
    assert!(matches!(
        fixture.nexus.clear(&recipient),
        Response::Cleared(cleared) if cleared.len() == 2
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
    let order = admitted(
        &fixture,
        &answerer,
        SentBy::Sender(asker.clone()),
        Request::Order("Review the drain fix.".into()),
    );
    delivered(&fixture, &answerer, &order);
    let answer = fixture
        .nexus
        .send_request(answer_to(&elsewhere, &order.request_id), None);
    assert!(
        matches!(
            answer.response,
            Response::SendAccepted(_) | Response::Sent(_)
        ),
        "{:?}",
        answer.response
    );
    assert!(queued(&fixture, &elsewhere).is_empty());
    let routed = queued(&fixture, &asker);
    assert_eq!(routed.len(), 1);
    assert!(matches!(
        &routed[0].message,
        Message::Soft(Request::Result(flow_ethos::ResultPayload::Answered(result)))
            if result.request_id == order.request_id
    ));
    assert_eq!(
        fixture
            .nexus
            .dispatch(Query::Answer(order.request_id.clone())),
        Response::AnswerRejected(signal_flow::AnswerRejection::Routed)
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
            .dispatch(Query::Answer(question.request_id.clone())),
        Response::AnswerRejected(signal_flow::AnswerRejection::Open)
    );
    let answer = fixture
        .nexus
        .send_request(answer_to(&elsewhere, &question.request_id), None);
    assert_eq!(
        answer.response,
        Response::Sent(signal_flow::VisibleSendReceipt(question.request_id.clone()))
    );
    assert!(queued(&fixture, &elsewhere).is_empty());
    assert!(queued(&fixture, &answerer).is_empty());
    assert_eq!(
        fixture
            .nexus
            .dispatch(Query::Answer(question.request_id.clone())),
        Response::Answered(VisibleNativeInput {
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
        Response::SendRefused(signal_flow::VisibleRefusal::Unresolved)
    );
    assert_eq!(
        fixture
            .nexus
            .dispatch(Query::Answer(flow_ethos::RequestId("zzzz".into()))),
        Response::AnswerRejected(signal_flow::AnswerRejection::UnknownRequest)
    );
}

/// Remind keeps a reminder for the Address it names and answers its id;
/// when its minutes lapse the Address is sent MiddleAbrupt
/// `Reminder.{ id words }` from whoever asked, and its request closes.
#[test]
fn remind_sends_its_address_a_reminder_when_its_minutes_lapse() {
    use crate::reminding::RemindsCallers;
    use crate::store::scheduled_reminder::SchedulesReminders;
    let fixture = NexusFixture::new();
    let reminded = at("remindedSeat");
    let asker = at("remindAsker");
    asleep(&fixture, &reminded);
    asleep(&fixture, &asker);
    let Response::Reminded(request_id) = fixture.nexus.remind(
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
        fixture.nexus.dispatch(Query::Cancel(request_id)),
        Response::CancelRejected(RequestRejection::Closed)
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
    let Response::Reminded(request_id) = fixture
        .nexus
        .remind(remind(5, &reminded), SentBy::Sender(asker.clone()))
    else {
        panic!("a reminder is kept")
    };
    assert_eq!(
        fixture.nexus.dispatch(Query::Cancel(request_id.clone())),
        Response::Cancelled(request_id.clone())
    );
    assert_eq!(
        fixture.nexus.store.scheduled_reminder(&request_id).unwrap(),
        None
    );
    assert_eq!(
        notices(&fixture, &asker),
        vec![format!("Request {} was cancelled.", request_id.0)]
    );
    assert!(queued(&fixture, &reminded).is_empty());
    assert_eq!(
        fixture.nexus.remind(remind(0, &reminded), SentBy::Unbound),
        Response::Refused(signal_flow::Refusal::MinutesRefused(
            signal_flow::MinutesRefusal::Zero
        ))
    );
    assert_eq!(
        fixture
            .nexus
            .remind(remind(5, &at("noSuchSeat")), SentBy::Unbound),
        Response::Refused(signal_flow::Refusal::Unknown(
            signal_flow::Unknown_Data::Address(at("noSuchSeat"))
        ))
    );
}
