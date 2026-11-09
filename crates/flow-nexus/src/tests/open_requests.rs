//! A request is open from its delivery until the Result or Done that
//! answers it, and it is the permaflow's, not the flow's: a successor's
//! first prompt carries, after the handover, every request delivered to its
//! predecessors and still open, under the id it was delivered with, and the
//! same Result or Done answers it. A Task's minutes keep running across the
//! refresh.

use super::NexusFixture;
use super::waking::{
    FreshWorld, ReadsFreshWorld, WakesFreshClaude, address, launched_claude_ended_by, maintain,
    metaflow, rested_at_stop,
};
use crate::Dispatches;
use crate::composition::RendersQueuedWork;
use crate::store::FlowStore;
use crate::store::RecordsLaunchOutcome as _;
use crate::store::logical_delivery::{HoldsOpenRequests, RecordsLogicalDeliveries};
use crate::store::metaflow::{SeedsMetaflow, SleepsMetaflows as _};
use crate::store::queue_delivery::{
    AccountsQueueDelivery, NativeDeliveryReceipt, NativeHistoryAnchor, NativeTranscriptCursor,
    PrepareQueueDelivery, QueueDeliveryAttempt,
};
use crate::store::reminder::RecordsReminders;
use flow_ethos::{AnswerableTask, State};
use signal_flow::{
    Address, Aspect, DeliveryState, Message, Model, PrivateSendResponse, Query, QueueEntry,
    Refreshed, Refusal, Request, Response, SendAcceptance, SendableMessage, SendableRequest,
    SentBy,
};
use std::fs;

/// The handover the predecessor abc123 leaves.
const HANDOVER: &str = "# Handover\n\nThe build-base move is half done.";

/// The Psyche core Secondary: the sender of every request here.
fn psyche_core() -> Address {
    Address {
        aspect: Aspect::Psyche,
        topic: "core".into(),
        model: Model::Opus,
    }
}

/// What a delivery test needs of a store: admission, then the oldest queued
/// request delivered to the flow the metaflow is Awake as.
trait DeliversRequests {
    /// Admits `request` from `sender` to `recipient`, Soft and queued.
    fn admitted(&self, recipient: &Address, sender: &Address, request: Request) -> SendAcceptance;
    /// Hands the oldest queued request of `address` to `flow_id`.
    fn handed_over(&self, address: &Address, flow_id: &str) -> QueueDeliveryAttempt;
    /// Delivers the oldest queued request of `address` to `flow_id`: handed
    /// over, its receipt recorded, and settled out of the queue.
    fn delivered(&self, address: &Address, flow_id: &str);
    /// Resolves and admits `message` from `answerer` to `requester`, as a
    /// Send from `answerer` is: refused, or accepted.
    fn answered(
        &self,
        answerer: &Address,
        requester: &Address,
        message: SendableMessage,
    ) -> Result<SendAcceptance, Refusal>;
}

impl DeliversRequests for FlowStore {
    fn admitted(&self, recipient: &Address, sender: &Address, request: Request) -> SendAcceptance {
        match self
            .accept_logical_delivery(
                recipient,
                SentBy::Sender(sender.clone()),
                flow_ethos::Delivery::Queue,
                Message::Soft(request),
                None,
            )
            .expect("admission")
        {
            PrivateSendResponse::SendAccepted(accepted) => accepted,
            refused => panic!("the request is admitted: {refused:?}"),
        }
    }

    fn handed_over(&self, address: &Address, flow_id: &str) -> QueueDeliveryAttempt {
        match self
            .prepare_oldest_delivery(
                address,
                flow_id,
                &format!("{flow_id}-session"),
                signal_flow::HarnessKind::Claude,
                NativeHistoryAnchor::ClaudeTranscript(NativeTranscriptCursor {
                    device: 0,
                    inode: 0,
                    offset: 0,
                }),
            )
            .expect("the oldest request prepares")
        {
            PrepareQueueDelivery::Prepared(attempt) => attempt,
            other => panic!("the oldest request is handed over: {other:?}"),
        }
    }

    fn delivered(&self, address: &Address, flow_id: &str) {
        let attempt = self.handed_over(address, flow_id);
        assert!(
            self.record_delivery_receipt(
                &attempt.attempt_id,
                NativeDeliveryReceipt {
                    native_request_id: attempt.native_request_id,
                    thread_id: attempt.thread_id.clone(),
                    turn_id: format!("{}-turn", attempt.attempt_id),
                    native_receipt: "{\"result\":{}}".into(),
                },
            )
            .expect("the receipt records")
        );
        assert!(
            self.settle_recorded_delivery(&attempt.attempt_id)
                .expect("the delivery settles")
        );
    }

    fn answered(
        &self,
        answerer: &Address,
        requester: &Address,
        message: SendableMessage,
    ) -> Result<SendAcceptance, Refusal> {
        let message = self
            .resolve_sendable_message(answerer, requester, message)
            .expect("the answer resolves")?;
        match self
            .accept_logical_delivery(
                requester,
                SentBy::Sender(answerer.clone()),
                flow_ethos::Delivery::Queue,
                message,
                None,
            )
            .expect("the answer is admitted or refused, never a store fault")
        {
            PrivateSendResponse::SendAccepted(accepted) => Ok(accepted),
            PrivateSendResponse::Refused(refusal) => Err(refusal),
            other => panic!("an answer is accepted or refused: {other:?}"),
        }
    }
}

fn result(request_id: &str) -> SendableMessage {
    SendableMessage::Soft(SendableRequest::Result(
        signal_flow::SendableRequest_Result_Data {
            request_id: request_id.into(),
            string: "Landed.".into(),
        },
    ))
}

fn done(request_id: &str) -> SendableMessage {
    SendableMessage::Soft(SendableRequest::Done(
        signal_flow::SendableRequest_Done_Data {
            request_id: request_id.into(),
            string: "Moved.".into(),
        },
    ))
}

/// The queue entry a Soft request from `sender` was admitted as.
fn entry(accepted: &SendAcceptance, sender: &Address, request: Request) -> QueueEntry {
    QueueEntry {
        logical_delivery_id: accepted.logical_delivery_id.clone(),
        request_id: accepted.request_id.clone(),
        sent_by: SentBy::Sender(sender.clone()),
        delivery: flow_ethos::Delivery::Queue,
        message: Message::Soft(request),
    }
}

/// The Maintained Mind flow Secondary, Awake as the Claude flow abc123,
/// resting, with its handover left, ready to be refreshed in the fresh
/// world.
fn resting_seat() -> (NexusFixture, FreshWorld) {
    let mut fixture = NexusFixture::new();
    let seat = launched_claude_ended_by(&fixture, |_| true);
    rested_at_stop(&fixture, "abc123");
    let directory = fixture.directory.path().join("flows/abc123");
    fs::create_dir_all(&directory).unwrap();
    fs::write(directory.join("handover.md"), HANDOVER).unwrap();
    let world = fixture.fresh_claude_world();
    fixture
        .nexus
        .store
        .record_launch_outcome(
            &seat.profile.launch_request_id,
            crate::store::LaunchOutcome::Started(crate::LaunchedRecord {
                flow_id: "abc123".into(),
                session_id: seat.session.clone(),
                origin: signal_flow::Origin::Unbound,
            }),
        )
        .unwrap();
    // Herdr shows abc123's agent in its pane, w1:p3, until it is closed.
    fs::write(world.state.join("restart-world"), "").unwrap();
    fs::write(world.state.join("old-pane"), "w1:p3").unwrap();
    fs::write(
        world.state.join("old-agent"),
        serde_json::json!({
            "name": seat.profile.launch_request_id,
            "agent": "claude",
            "agent_status": "idle",
            "workspace_id": "w1",
            "tab_id": "w1:t1",
            "pane_id": "w1:p3",
            "terminal_id": "term-old",
            "interactive_ready": true,
            "agent_session": {
                "source": "herdr:claude", "agent": "claude",
                "kind": "id", "value": seat.session,
            }
        })
        .to_string(),
    )
    .unwrap();
    maintain(&fixture, address(Model::Sol));
    fixture.nexus.store.seed_metaflow(metaflow(
        Model::Sol,
        State::Awake("abc123".into()),
        vec![],
        vec![],
    ));
    fixture.nexus.store.seed_metaflow(signal_flow::Metaflow {
        address: psyche_core(),
        state: State::Asleep,
        past: vec![],
        queue: vec![],
    });
    (fixture, world)
}

/// An Order and then a 240-minute Task delivered to abc123 and left open,
/// and an Order it answered, are carried over a refresh: the successor's
/// first prompt holds, after the handover, the two open ones, oldest first,
/// under the ids they were delivered with. The successor's Done with the
/// Task's id is admitted as abc123's would have been, a second is
/// Unresolved, and the Task's deadline never moved.
#[test]
fn a_successor_is_given_its_predecessors_open_requests_and_answers_them_by_their_ids() {
    let (fixture, world) = resting_seat();
    let store = &fixture.nexus.store;
    let at = address(Model::Sol);
    let sender = psyche_core();

    let order = Request::Order("Land the Git server line.".into());
    let open_order = store.admitted(&at, &sender, order.clone());
    store.delivered(&at, "abc123");
    let task = Request::Task(AnswerableTask {
        minutes: 240,
        string: "Move every active Rust repository onto build-base.".into(),
    });
    let open_task = store.admitted(&at, &sender, task.clone());
    store.delivered(&at, "abc123");
    let answered = store.admitted(&at, &sender, Request::Order("Answered already.".into()));
    store.delivered(&at, "abc123");
    store
        .answered(&at, &sender, result(&answered.request_id))
        .expect("abc123 answers its Order");
    let open = vec![
        entry(&open_order, &sender, order),
        entry(&open_task, &sender, task),
    ];
    assert_eq!(store.open_requests(&at).unwrap(), open);
    let deadline = store
        .reminder(&open_task.logical_delivery_id)
        .unwrap()
        .expect("the Task's reminder is held");
    assert!(deadline.due.is_some(), "{deadline:?}");

    assert_eq!(
        fixture.nexus.dispatch(Query::Refresh(at.clone())),
        Response::Refreshed(Refreshed {
            address: at.clone(),
            flow_id: "abc123".into(),
        }),
        "Herdr calls: {}",
        world.calls()
    );
    assert_eq!(world.claude_starts(), 1, "{}", world.calls());
    let successor = match store.metaflow(&at).unwrap().expect("metaflow").state {
        State::Awake(flow_id) => flow_id,
        State::Asleep => panic!("the successor is Awake"),
    };
    assert_ne!(successor, "abc123");
    let first = world
        .user_turns(&fixture)
        .into_iter()
        .next()
        .expect("the successor's first turn");
    // The delivered form: each request under its id, the Task with its
    // minutes, the older Order first.
    let delivered = open.prompt_text(&at);
    let order_at = delivered.find("Soft.Order.{").expect("the Order");
    let task_at = delivered.find("Soft.Task.{").expect("the Task");
    assert!(order_at < task_at, "oldest first: {delivered}");
    assert!(
        delivered[task_at..].contains(&open_task.request_id)
            && delivered[task_at..].contains(" 240 ")
            && delivered[order_at..task_at].contains(&open_order.request_id),
        "{delivered}"
    );
    assert!(
        first.ends_with(&format!("{HANDOVER}\n\n{delivered}")),
        "the open requests follow the handover, last: {first}"
    );

    // The deadline is the Task's: the refresh neither restarted nor
    // extended it.
    assert_eq!(
        store.reminder(&open_task.logical_delivery_id).unwrap(),
        Some(deadline)
    );
    // Only a flow of the Address answers: the successor, by the same id.
    assert_eq!(
        store.answered(&sender, &sender, done(&open_task.request_id)),
        Err(Refusal::Unresolved)
    );
    store
        .answered(&at, &sender, done(&open_task.request_id))
        .expect("the successor's Done is admitted");
    assert_eq!(
        store.answered(&at, &sender, done(&open_task.request_id)),
        Err(Refusal::Unresolved),
        "a second Done finds the Task closed"
    );
    assert_eq!(
        store.reminder(&open_task.logical_delivery_id).unwrap(),
        None,
        "the Done closed the Task's reminders"
    );
    assert_eq!(store.open_requests(&at).unwrap(), open[..1].to_vec());
}

/// A flow that sends itself an Order and answers it: while the Order is
/// queued, never handed over, the Result is Unresolved; once its hand-over
/// is uncertain, the Result proves it read, and its admission takes the
/// Order out of the queue and queues the Result in the same commit, the
/// Address's metaflow row written once. A second Result is Unresolved.
#[test]
fn a_flow_s_result_to_its_own_order_writes_its_metaflow_once() {
    let fixture = NexusFixture::new();
    let store = &fixture.nexus.store;
    let at = Address {
        aspect: Aspect::Mind,
        topic: "flowNexus".into(),
        model: Model::Opus,
    };
    store.seed_metaflow(signal_flow::Metaflow {
        address: at.clone(),
        state: State::Awake("a11ce5".into()),
        past: vec![],
        queue: vec![],
    });
    let order = store.admitted(&at, &at, Request::Order("Land the fix.".into()));
    assert_eq!(
        store.answered(&at, &at, result(&order.request_id)),
        Err(Refusal::Unresolved),
        "a queued Order is not yet answerable"
    );

    let attempt = store.handed_over(&at, "a11ce5");
    assert!(store.mark_delivery_uncertain(&attempt.attempt_id).unwrap());
    let answer = store
        .answered(&at, &at, result(&order.request_id))
        .expect("the Result to its own uncertain Order is admitted");
    let queue = store.metaflow(&at).unwrap().expect("metaflow").queue;
    assert_eq!(
        queue
            .iter()
            .map(|entry| entry.logical_delivery_id.clone())
            .collect::<Vec<_>>(),
        vec![answer.logical_delivery_id.clone()],
        "the Order left the queue and the Result is queued"
    );
    assert_eq!(
        store
            .logical_delivery_snapshot(&order.logical_delivery_id)
            .unwrap()
            .expect("the Order's delivery")
            .delivery_state,
        DeliveryState::Read
    );
    assert_eq!(
        store.answered(&at, &at, result(&order.request_id)),
        Err(Refusal::Unresolved)
    );
}
