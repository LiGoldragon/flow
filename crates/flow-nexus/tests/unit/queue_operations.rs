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
            .dispatch(Input::Cancel(order.request_id.clone())),
        Output::Cancelled(order.request_id.clone())
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
            .dispatch(Input::Cancel(order.request_id.clone())),
        Output::CancelRejected(RequestRejection::UnknownRequest)
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

    // An Order from no Address is answered to whom the answer names, by
    // whoever answers.
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
            if result.request_id == order.request_id
    ));
    assert_eq!(
        fixture
            .nexus
            .dispatch(Input::Answer(order.request_id.clone())),
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
    assert_eq!(
        notices(&fixture, &asker),
        vec![format!("Request {} was cancelled.", request_id.0)]
    );
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

/// A delivered Order is closed by anyone with no answer: its id archived,
/// no Notice queued for its sender or its recipient, and its closer
/// recorded. A Result naming it afterwards is refused Unresolved, and a
/// second CloseRequest names no request.
#[test]
fn close_request_closes_a_delivered_order_and_tells_no_one() {
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
        Request::Order("Review the drain fix.".into()),
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
/// UnknownRequest, a request already answered is Closed, and one whose
/// delivery is still being settled is NotYet and stays open.
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
        Request::Order("Answered already.".into()),
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
    assert!(matches!(
        fixture
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
            .expect("the oldest request prepares"),
        PrepareQueueDelivery::Prepared(_)
    ));
    assert_eq!(
        fixture
            .nexus
            .dispatch(Input::CloseRequest(unsettled.request_id.clone())),
        Output::CloseRequestRejected(RequestRejection::NotYet)
    );
    assert!(
        fixture
            .nexus
            .store
            .request_binding(&unsettled.request_id)
            .unwrap()
            .is_some(),
        "a refused close leaves the request open"
    );
    assert_eq!(
        closed_record(&fixture, &unsettled.logical_delivery_id),
        None
    );
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
        Request::Order("Review the drain fix.".into()),
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

/// Lets the reminder of `task` fall due now and sends it: a Soft Notice
/// from Flow queued for `sender`, the Task's sender, and answered here.
fn reminder_falls_due(
    fixture: &NexusFixture,
    sender: &Address,
    task: &SendAcceptance,
) -> flow_ethos::QueueEntry {
    use crate::reminding::RemindsCallers;
    let before = queued(fixture, sender);
    let mut reminder = fixture
        .nexus
        .store
        .reminder(&task.logical_delivery_id.0)
        .expect("the reminders read")
        .expect("the Task is reminded");
    reminder.due_option = Some(crate::generated::memory::Due(0));
    fixture
        .nexus
        .store
        .hold_reminder(reminder)
        .expect("the reminder is held");
    fixture.nexus.send_due_reminders();
    let mut sent = queued(fixture, sender)
        .into_iter()
        .filter(|entry| !before.contains(entry))
        .collect::<Vec<_>>();
    assert_eq!(sent.len(), 1, "one reminder is queued for the sender");
    let reminder = sent.remove(0);
    assert_eq!(reminder.sent_by, SentBy::Flow);
    assert!(matches!(
        reminder.message,
        Message::Soft(Request::Notice(_))
    ));
    reminder
}

/// A Task from `sender` delivered to `recipient`, with its first reminder
/// fallen due and queued for `sender`.
fn reminded_task(
    fixture: &NexusFixture,
    sender: &Address,
    recipient: &Address,
) -> (SendAcceptance, flow_ethos::QueueEntry) {
    asleep(fixture, sender);
    asleep(fixture, recipient);
    let task = admitted(
        fixture,
        recipient,
        SentBy::Sender(sender.clone()),
        Request::Task(flow_ethos::AnswerableTask {
            minutes: flow_ethos::Minutes(30),
            string: "Land the line.".into(),
        }),
    );
    delivered(fixture, recipient, &task);
    let reminder = reminder_falls_due(fixture, sender, &task);
    (task, reminder)
}

fn delivery_state(
    fixture: &NexusFixture,
    logical_delivery_id: &signal_flow::LogicalDeliveryId,
) -> Option<DeliveryState> {
    fixture
        .nexus
        .store
        .logical_delivery_snapshot(logical_delivery_id)
        .expect("the deliveries read")
        .map(|snapshot| snapshot.delivery_state)
}

/// A Task closed by CloseRequest takes its reminder with it: the reminder
/// queued for its sender and not yet delivered leaves the queue unsent and
/// ends Withdrawn, and neither side is told anything.
#[test]
fn close_request_withdraws_the_queued_reminder_of_its_task() {
    let fixture = NexusFixture::new();
    let sender = at("remindedCloseSender");
    let recipient = at("remindedCloseRecipient");
    let (task, reminder) = reminded_task(&fixture, &sender, &recipient);

    assert_eq!(
        fixture
            .nexus
            .dispatch(Input::CloseRequest(task.request_id.clone())),
        Output::RequestClosed(task.request_id.clone())
    );
    assert!(
        queued(&fixture, &sender).is_empty(),
        "the reminder left the sender's queue and no Notice took its place"
    );
    assert!(
        queued(&fixture, &recipient).is_empty(),
        "the recipient is told nothing"
    );
    assert_eq!(
        delivery_state(&fixture, &reminder.logical_delivery_id),
        Some(DeliveryState::Withdrawn),
        "the reminder is never delivered"
    );
}

/// A Done closes its Task, and the Task's reminder still queued for its
/// sender is withdrawn: the sender is given the Done alone.
#[test]
fn a_done_withdraws_the_queued_reminder_of_its_task() {
    let fixture = NexusFixture::new();
    let sender = at("remindedDoneSender");
    let recipient = at("remindedDoneRecipient");
    let (task, reminder) = reminded_task(&fixture, &sender, &recipient);
    let done = SendRequest {
        address: recipient.clone(),
        sendable_message: signal_flow::SendableMessage::Soft(signal_flow::SendableRequest::Done(
            signal_flow::SendableRequest_Done_Data {
                request_id: task.request_id.clone(),
                string: "Landed.".into(),
            },
        )),
    };

    assert!(matches!(
        fixture.nexus.send_request(done, None).response,
        Output::SendAccepted(_) | Output::Sent(_)
    ));
    let left = queued(&fixture, &sender);
    assert_eq!(left.len(), 1, "the sender holds the Done alone: {left:?}");
    assert!(matches!(
        &left[0].message,
        Message::Soft(Request::Done(flow_ethos::DonePayload::Answered(_)))
    ));
    assert!(notices(&fixture, &sender).is_empty());
    assert_eq!(
        delivery_state(&fixture, &reminder.logical_delivery_id),
        Some(DeliveryState::Withdrawn),
        "the reminder is never delivered"
    );
}

/// A Cancel closes the Task and withdraws its queued reminder; its own
/// Notice of the cancellation still reaches the sender and the recipient.
#[test]
fn cancel_withdraws_the_queued_reminder_of_its_task_and_keeps_its_notice() {
    let fixture = NexusFixture::new();
    let sender = at("remindedCancelSender");
    let recipient = at("remindedCancelRecipient");
    let (task, reminder) = reminded_task(&fixture, &sender, &recipient);

    assert_eq!(
        fixture
            .nexus
            .dispatch(Input::Cancel(task.request_id.clone())),
        Output::Cancelled(task.request_id.clone())
    );
    let cancelled = format!("Request {} was cancelled.", task.request_id.0);
    assert_eq!(notices(&fixture, &sender), vec![cancelled.clone()]);
    assert_eq!(notices(&fixture, &recipient), vec![cancelled]);
    assert_eq!(
        delivery_state(&fixture, &reminder.logical_delivery_id),
        Some(DeliveryState::Withdrawn),
        "the reminder is never delivered"
    );
}

/// A reminder already delivered is left as it was when its Task closes;
/// the next one, still queued, is withdrawn.
#[test]
fn close_request_leaves_a_delivered_reminder_and_withdraws_the_queued_one() {
    let fixture = NexusFixture::new();
    let sender = at("remindedTwiceSender");
    let recipient = at("remindedTwiceRecipient");
    let (task, first) = reminded_task(&fixture, &sender, &recipient);
    delivered(
        &fixture,
        &sender,
        &SendAcceptance {
            logical_delivery_id: first.logical_delivery_id.clone(),
            request_id: first.request_id.clone(),
            delivery_cursor: signal_flow::DeliveryCursor(0),
            delivery_state: DeliveryState::Queued,
        },
    );
    let second = reminder_falls_due(&fixture, &sender, &task);

    assert_eq!(
        fixture
            .nexus
            .dispatch(Input::CloseRequest(task.request_id.clone())),
        Output::RequestClosed(task.request_id.clone())
    );
    assert!(
        matches!(
            delivery_state(&fixture, &first.logical_delivery_id),
            Some(DeliveryState::Delivered(_))
        ),
        "the delivered reminder is untouched"
    );
    assert_eq!(
        delivery_state(&fixture, &second.logical_delivery_id),
        Some(DeliveryState::Withdrawn),
        "the queued reminder is never delivered"
    );
    assert!(queued(&fixture, &sender).is_empty());
    assert!(queued(&fixture, &recipient).is_empty());
}

/// A Done the send tool admits ends its Task's reminders in the same commit,
/// as a Done sent by Send does: no reminder of the Task is held after it,
/// and none falls due.
#[test]
fn a_done_through_the_send_tool_removes_its_tasks_reminder() {
    use crate::generated::send_tool::{CallId, SendOutcome, Target, ThreadId, ToolCallKey, TurnId};
    let fixture = NexusFixture::new();
    let sender = at("toolDoneSender");
    let recipient = at("toolDoneRecipient");
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
            .is_some(),
        "the Task is reminded"
    );

    let outcome = fixture
        .nexus
        .store
        .accept_tool_send(
            &ToolCallKey {
                thread_id: ThreadId("tool-done-thread".into()),
                turn_id: TurnId("tool-done-turn".into()),
                call_id: CallId("tool-done-call".into()),
            },
            "tool-done-content",
            SentBy::Sender(recipient.clone()),
            &Target(sender.clone()),
            Message::Soft(Request::Done(flow_ethos::DonePayload::Answered(
                flow_ethos::DonePayload_Answered_Data {
                    logical_delivery_id: task.logical_delivery_id.clone(),
                    request_id: task.request_id.clone(),
                    string: "Landed.".into(),
                },
            ))),
            0,
        )
        .expect("the tool send is admitted");
    assert!(matches!(outcome, SendOutcome::Accepted(_)), "{outcome:?}");
    assert_eq!(
        fixture
            .nexus
            .store
            .reminder(&task.logical_delivery_id.0)
            .unwrap(),
        None,
        "the Done removed the Task's reminder"
    );
    assert!(
        fixture
            .nexus
            .store
            .due_reminders(i64::MAX)
            .unwrap()
            .is_empty(),
        "no reminder of the Task falls due"
    );
}

/// Silence ends a Task's reminders and withdraws the one already queued
/// for its sender and not yet delivered; the Task stays open and no one is
/// told.
#[test]
fn silence_withdraws_the_queued_reminder_of_its_task() {
    let fixture = NexusFixture::new();
    let sender = at("remindedSilenceSender");
    let recipient = at("remindedSilenceRecipient");
    let (task, reminder) = reminded_task(&fixture, &sender, &recipient);

    assert_eq!(
        fixture
            .nexus
            .dispatch(Input::Silence(task.request_id.clone())),
        Output::Silenced(task.request_id.clone())
    );
    assert!(
        queued(&fixture, &sender).is_empty(),
        "the reminder left the sender's queue and no Notice took its place"
    );
    assert!(
        queued(&fixture, &recipient).is_empty(),
        "the recipient is told nothing"
    );
    assert_eq!(
        delivery_state(&fixture, &reminder.logical_delivery_id),
        Some(DeliveryState::Withdrawn),
        "the reminder is never delivered"
    );
    assert_eq!(
        fixture
            .nexus
            .store
            .reminder(&task.logical_delivery_id.0)
            .unwrap(),
        None,
        "no reminder follows"
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
}

/// Exercises the JSON parser and actual handler, before routing an answer,
/// rather than passing an already-private answer straight to the store.
fn answer_through_tool(
    fixture: &NexusFixture,
    kind: &str,
    request_id: &signal_flow::RequestId,
    call_id: &str,
) -> crate::generated::send_tool::SendOutcome {
    use crate::generated::send_tool::HandlesSendCall;
    let mut call = super::send_tool::tool_call("answer-thread", call_id, "Landed.");
    call.arguments = crate::generated::send_tool::Arguments(
        serde_json::json!({
            "target": { "aspect": "Mind", "topic": "unusedTarget", "model": "Luna" },
            "priority": "Soft", "kind": kind, "body": "Landed.", "request": request_id.0
        })
        .to_string(),
    );
    fixture.nexus.handle_held_send_call(call)
}

#[test]
fn result_cannot_close_a_task_through_the_tool_or_ordinary_send() {
    use crate::generated::send_tool::{SendOutcome, SendRefusal};
    use crate::store::logical_delivery::RequestClosure;
    for origin in [
        SentBy::Sender(at("taskAnswerSender")),
        SentBy::Unbound,
        SentBy::Flow,
        SentBy::Unknown,
    ] {
        let fixture = NexusFixture::new();
        let recipient = at("taskAnswerRecipient");
        asleep(&fixture, &at("taskAnswerSender"));
        asleep(&fixture, &recipient);
        let task = admitted(
            &fixture,
            &recipient,
            origin,
            Request::Task(flow_ethos::AnswerableTask {
                minutes: flow_ethos::Minutes(30),
                string: "Land the line.".into(),
            }),
        );
        delivered(&fixture, &recipient, &task);
        assert_eq!(
            answer_through_tool(&fixture, "Result", &task.request_id, "wrong-kind"),
            SendOutcome::Refused(SendRefusal::Unresolved)
        );
        assert_eq!(
            fixture
                .nexus
                .send_request(
                    SendRequest {
                        address: recipient.clone(),
                        sendable_message: signal_flow::SendableMessage::Soft(
                            signal_flow::SendableRequest::Result(
                                signal_flow::SendableRequest_Result_Data {
                                    request_id: task.request_id.clone(),
                                    string: "Landed.".into()
                                }
                            )
                        ),
                    },
                    None
                )
                .response,
            Output::SendRefused(signal_flow::VisibleRefusal::Unresolved)
        );
        assert_eq!(
            fixture
                .nexus
                .store
                .request_binding(&task.request_id)
                .unwrap()
                .unwrap()
                .request_closure,
            RequestClosure::Open
        );
        assert!(
            fixture
                .nexus
                .store
                .reminder(&task.logical_delivery_id.0)
                .unwrap()
                .is_some()
        );
        assert!(queued(&fixture, &at("taskAnswerSender")).is_empty());
        let done = answer_through_tool(&fixture, "Done", &task.request_id, "right-kind");
        assert!(
            matches!(done, SendOutcome::Accepted(_) | SendOutcome::Kept(_)),
            "{done:?}"
        );
        assert!(matches!(
            fixture
                .nexus
                .store
                .request_binding(&task.request_id)
                .unwrap()
                .unwrap()
                .request_closure,
            RequestClosure::Answered(_)
        ));
        assert_eq!(
            fixture
                .nexus
                .store
                .reminder(&task.logical_delivery_id.0)
                .unwrap(),
            None
        );
    }
}

#[test]
fn done_after_silence_closes_the_open_task_through_both_paths_and_origins() {
    use crate::generated::send_tool::{SendOutcome, SendRefusal};
    use crate::store::logical_delivery::RequestClosure;
    for tool in [false, true] {
        for origin in [
            SentBy::Sender(at("silencedAnswerSender")),
            SentBy::Unbound,
            SentBy::Flow,
            SentBy::Unknown,
        ] {
            let kept = !matches!(&origin, SentBy::Sender(_));
            let fixture = NexusFixture::new();
            let recipient = at("silencedAnswerRecipient");
            asleep(&fixture, &at("silencedAnswerSender"));
            asleep(&fixture, &recipient);
            let task = admitted(
                &fixture,
                &recipient,
                origin,
                Request::Task(flow_ethos::AnswerableTask {
                    minutes: flow_ethos::Minutes(30),
                    string: "Land the line.".into(),
                }),
            );
            delivered(&fixture, &recipient, &task);
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
            assert_eq!(
                fixture
                    .nexus
                    .store
                    .request_binding(&task.request_id)
                    .unwrap()
                    .unwrap()
                    .request_closure,
                RequestClosure::Open
            );
            if tool {
                let outcome = answer_through_tool(&fixture, "Done", &task.request_id, "done");
                if kept {
                    assert!(matches!(outcome, SendOutcome::Kept(_)), "{outcome:?}");
                } else {
                    assert!(matches!(outcome, SendOutcome::Accepted(_)), "{outcome:?}");
                }
                assert_eq!(
                    answer_through_tool(&fixture, "Done", &task.request_id, "done"),
                    outcome,
                    "a replay admits nothing twice"
                );
            } else {
                let outcome = fixture
                    .nexus
                    .send_request(
                        SendRequest {
                            address: recipient.clone(),
                            sendable_message: signal_flow::SendableMessage::Soft(
                                signal_flow::SendableRequest::Done(
                                    signal_flow::SendableRequest_Done_Data {
                                        request_id: task.request_id.clone(),
                                        string: "Landed.".into(),
                                    },
                                ),
                            ),
                        },
                        None,
                    )
                    .response;
                assert!(
                    matches!(outcome, Output::SendAccepted(_) | Output::Sent(_)),
                    "{outcome:?}"
                );
            }
            assert!(matches!(
                fixture
                    .nexus
                    .store
                    .request_binding(&task.request_id)
                    .unwrap()
                    .unwrap()
                    .request_closure,
                RequestClosure::Answered(_)
            ));
            assert_eq!(
                answer_through_tool(&fixture, "Done", &task.request_id, "done-again"),
                SendOutcome::Refused(SendRefusal::Unresolved)
            );
            assert!(
                fixture
                    .nexus
                    .store
                    .due_reminders(i64::MAX)
                    .unwrap()
                    .is_empty()
            );
            if !kept {
                assert_eq!(queued(&fixture, &at("silencedAnswerSender")).len(), 1);
            }
        }
    }
}

#[test]
fn tool_answers_require_an_open_delivered_request_of_the_right_kind() {
    use crate::generated::send_tool::{SendOutcome, SendRefusal};
    let fixture = NexusFixture::new();
    let sender = at("answerValidationSender");
    let recipient = at("answerValidationRecipient");
    asleep(&fixture, &sender);
    asleep(&fixture, &recipient);
    let order = admitted(
        &fixture,
        &recipient,
        SentBy::Sender(sender.clone()),
        Request::Order("Land the line.".into()),
    );
    assert_eq!(
        answer_through_tool(&fixture, "Result", &order.request_id, "undelivered"),
        SendOutcome::Refused(SendRefusal::Unresolved)
    );
    delivered(&fixture, &recipient, &order);
    assert_eq!(
        answer_through_tool(&fixture, "Done", &order.request_id, "done-order"),
        SendOutcome::Refused(SendRefusal::Unresolved)
    );
    let accepted = answer_through_tool(&fixture, "Result", &order.request_id, "result-order");
    assert!(matches!(accepted, SendOutcome::Accepted(_)), "{accepted:?}");
    assert_eq!(
        answer_through_tool(&fixture, "Result", &order.request_id, "closed-order"),
        SendOutcome::Refused(SendRefusal::Unresolved)
    );
    assert_eq!(
        answer_through_tool(
            &fixture,
            "Result",
            &signal_flow::RequestId("zzzz".into()),
            "unknown"
        ),
        SendOutcome::Refused(SendRefusal::Unresolved)
    );
    let answers = queued(&fixture, &sender);
    assert_eq!(answers.len(), 1);
    let Message::Soft(Request::Result(flow_ethos::ResultPayload::Answered(answer))) =
        &answers[0].message
    else {
        panic!("a routed Result")
    };
    assert_eq!(
        answer.logical_delivery_id, order.logical_delivery_id,
        "the resolver records the original logical identity"
    );
}
