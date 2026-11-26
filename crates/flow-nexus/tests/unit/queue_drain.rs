//! A Claude seat's queue drains on its own turn ends: its hook's Stop that
//! names its registered session and transcript, confirmed by the main
//! `end_turn` its transcript records, whatever Herdr's screen shows while
//! background agents run. A hand-over is recorded from the pane writer's
//! own result in the commit that takes it from the queue, and nothing is
//! typed twice.

use super::waking::{
    CLAUDE_SEAT, CLAUDE_SESSION, address, append_main_rows, claude_agent, claude_seat, claude_stop,
    end_turn, stored, working_claude_agent,
};
use super::{ControlsHerdrSnapshot, NexusFixture, PromptFixture};
use crate::DispatchesSerially;
use crate::generated::operation::{Failed_Data, Operation, Outcome};
use crate::performing::Performs;
use crate::reporting::RecordsReports;
use crate::sending::SendsRequests;
use crate::store::current_seat::RecordsCurrentSeats;
use crate::store::logical_delivery::{RecordsLogicalDeliveries, RecordsQueueWrites};
use crate::store::metaflow::SeedsMetaflow;
use crate::store::queue_delivery::{
    AccountsQueueDelivery, NativeHistoryAnchor, QueueDeliveryAttempt, QueueDeliveryPhase,
    RequestState, SeedsStockQueueAttempts,
};
use crate::waking::{HoldsWakingMetaflows, SettlesStopsAtConfirmation, SleepsAndWakesSeats};
use flow_ethos::Delivery;
use flow_ethos::State;
use meta_signal_flow::{ProcessIdentity, ResolverIdentity};
use signal_flow::{
    Address, Aspect, Event, Message, Metaflow, Model, NativeStop, Output, QueueEntry, Report_Data,
    Request, SentBy,
};

#[derive(Clone, Debug, PartialEq, Eq)]
struct TestRequest {
    logical_delivery_id: String,
    sent_by: SentBy,
    request: Request,
    delivery: Delivery,
}

fn from_core(body: &str) -> TestRequest {
    use sha2::Digest;
    TestRequest {
        logical_delivery_id: format!("test-{:x}", sha2::Sha256::digest(body.as_bytes())),
        sent_by: SentBy::Sender(Address {
            aspect: Aspect::Psyche,
            topic: flow_ethos::Topic("core".into()),
            model: Model::Opus,
        }),
        request: Request::Order(body.into()),
        delivery: flow_ethos::Delivery::Wake,
    }
}

fn queue_entry(request: &TestRequest) -> QueueEntry {
    QueueEntry {
        logical_delivery_id: flow_ethos::LogicalDeliveryId(request.logical_delivery_id.clone()),
        request_id: flow_ethos::RequestId(request.logical_delivery_id[5..9].into()),
        sent_by: request.sent_by.clone(),
        delivery: request.delivery.clone(),
        message: Message::Soft(request.request.clone()),
    }
}

/// The fixture Claude seat Awake at the Secondary address, holding `queue`.
fn seat(fixture: &NexusFixture, queue: Vec<TestRequest>) {
    claude_seat(fixture, 40_000);
    fixture.nexus.store.seed_metaflow(Metaflow {
        address: address(Model::Sol),
        state: State::Awake(flow_ethos::FlowId(CLAUDE_SEAT.into())),
        past: flow_ethos::Past(vec![]),
        queue: flow_ethos::Queue(
            queue
                .into_iter()
                .map(|request| QueueEntry {
                    request_id: flow_ethos::RequestId(request.logical_delivery_id[5..9].into()),
                    logical_delivery_id: flow_ethos::LogicalDeliveryId(request.logical_delivery_id),
                    sent_by: request.sent_by,
                    delivery: request.delivery,
                    message: Message::Soft(request.request),
                })
                .collect(),
        ),
    });
}

/// A report as the hook sends it: recorded, answered, then acted on.
fn report(fixture: &NexusFixture, event: Event) {
    assert_eq!(
        fixture.nexus.report(Report_Data {
            flow_id: flow_ethos::FlowId(CLAUDE_SEAT.into()),
            process_option: None,
            event: event.clone(),
        }),
        Output::Reported
    );
    fixture.nexus.after_report(
        &flow_ethos::FlowId(CLAUDE_SEAT.to_owned().to_owned()),
        &event,
    );
}

fn foreign_stop() -> Event {
    Event::Stopped(NativeStop {
        native_session_id_option: Some(signal_flow::NativeSessionId(
            "0ther000-0000-4000-8000-000000000009".into(),
        )),
        transcript_path_option: Some(signal_flow::TranscriptPath("/elsewhere/other.jsonl".into())),
        last_assistant_message_option: signal_flow::LastAssistantMessageOption(None),
        background_work: signal_flow::BackgroundWork(vec![]),
    })
}

fn queue(fixture: &NexusFixture) -> Vec<TestRequest> {
    stored(fixture, Model::Sol)
        .queue
        .0
        .into_iter()
        .map(|entry| TestRequest {
            logical_delivery_id: entry.logical_delivery_id.0,
            sent_by: entry.sent_by,
            delivery: entry.delivery,
            request: match entry.message {
                Message::HardAbrupt(request)
                | Message::MiddleAbrupt(request)
                | Message::Soft(request) => request,
            },
        })
        .collect()
}

fn typed_count(fixture: &NexusFixture, body: &str) -> usize {
    fixture.typed().unwrap_or_default().matches(body).count()
}

#[test]
fn logical_delivery_snapshots_are_sender_filtered_replayable_and_idempotent() {
    let fixture = NexusFixture::new();
    let target = address(Model::Sol);
    fixture.nexus.store.seed_metaflow(Metaflow {
        address: target.clone(),
        state: State::Asleep,
        past: flow_ethos::Past(vec![]),
        queue: flow_ethos::Queue(vec![]),
    });
    let sender = Address {
        aspect: Aspect::Psyche,
        topic: flow_ethos::Topic("core".into()),
        model: Model::Opus,
    };
    let other_sender = Address {
        aspect: Aspect::Mind,
        topic: flow_ethos::Topic("core".into()),
        model: Model::Sol,
    };
    let signal_flow::PrivateSendResponse::SendAccepted(accepted) = fixture
        .nexus
        .store
        .accept_logical_delivery(
            &target,
            SentBy::Sender(sender.clone()),
            flow_ethos::Delivery::Queue,
            Message::Soft(Request::Notice("cursor witness".into())),
            None,
        )
        .unwrap()
    else {
        panic!("admission is accepted")
    };

    let first = fixture
        .nexus
        .store
        .transition_logical_delivery(
            &accepted.logical_delivery_id,
            signal_flow::DeliveryState::Attempting,
        )
        .unwrap();
    let duplicate = fixture
        .nexus
        .store
        .transition_logical_delivery(
            &accepted.logical_delivery_id,
            signal_flow::DeliveryState::Attempting,
        )
        .unwrap();
    assert_eq!(duplicate, first, "identical transition is idempotent");

    let current = fixture
        .nexus
        .store
        .read_logical_delivery_snapshots(&sender, None)
        .unwrap();
    assert_eq!(current.delivery_cursor, first.delivery_cursor);
    assert_eq!(current.delivery_snapshot_vector, vec![first.clone()]);

    let replay = fixture
        .nexus
        .store
        .read_logical_delivery_snapshots(&sender, Some(accepted.delivery_cursor.0))
        .unwrap();
    assert_eq!(replay.delivery_cursor, first.delivery_cursor);
    assert_eq!(replay.delivery_snapshot_vector, vec![first]);

    let filtered = fixture
        .nexus
        .store
        .read_logical_delivery_snapshots(&other_sender, None)
        .unwrap();
    assert_eq!(filtered.delivery_cursor, current.delivery_cursor);
    assert!(filtered.delivery_snapshot_vector.is_empty());
}

/// The seat's main turn ended while a background agent still runs: its
/// sidechain rows follow the main `end_turn` and Herdr shows Working. Its
/// own Stop hands over its whole queue as one payload. A foreign Stop does
/// nothing.
#[test]
fn a_claude_main_end_turn_with_a_running_sidechain_drains_on_its_own_stop() {
    let fixture = NexusFixture::new();
    fixture.accept_pane_operations(
        vec![working_claude_agent(&fixture)],
        PromptFixture::Prompted("w1:p3"),
    );
    let first = from_core("first-request-body");
    let second = from_core("second-request-body");
    seat(&fixture, vec![first.clone(), second.clone()]);
    append_main_rows(
        &fixture,
        &[
            serde_json::json!({"type":"user","sessionId":CLAUDE_SESSION,"uuid":"u1",
                "message":{"content":"work"}}),
            end_turn("main-ended"),
            serde_json::json!({"type":"assistant","sessionId":CLAUDE_SESSION,"uuid":"side",
                "isSidechain":true,"message":{"stop_reason":"tool_use"}}),
        ],
    );

    report(&fixture, foreign_stop());
    assert_eq!(typed_count(&fixture, "first-request-body"), 0);
    assert_eq!(queue(&fixture), vec![first.clone(), second.clone()]);

    report(&fixture, claude_stop(&fixture));
    assert_eq!(typed_count(&fixture, "first-request-body"), 1);
    assert_eq!(typed_count(&fixture, "second-request-body"), 1);
    assert!(
        fixture
            .typed()
            .unwrap_or_default()
            .contains("Sender.{ Psyche core Opus }")
    );
    assert_eq!(queue(&fixture), vec![], "both in one payload");
    assert_eq!(
        stored(&fixture, Model::Sol).state,
        State::Awake(flow_ethos::FlowId(CLAUDE_SEAT.into()))
    );
}

#[test]
fn a_missing_observation_preserves_even_an_abrupt_order_without_pane_input() {
    let fixture = NexusFixture::new();
    let first = from_core("held until the source answers");
    seat(&fixture, vec![first]);
    fixture
        .nexus
        .store
        .record_observation(CLAUDE_SEAT, None)
        .unwrap();
    let target = address(Model::Sol);
    let response = fixture.nexus.admit_ordinary_send(
        signal_flow::SendRequest {
            address: target.clone(),
            sendable_message: signal_flow::SendableMessage::MiddleAbrupt(
                signal_flow::SendableRequest::Order("no-record order".into()),
            ),
        },
        None,
    );
    assert!(matches!(response.response, Output::SendAccepted(_)));
    drop(response);
    fixture.nexus.wake_due(target.clone());
    fixture.nexus.drain_awake(&target);
    assert_eq!(stored(&fixture, Model::Sol).queue.0.len(), 2);
    assert_eq!(typed_count(&fixture, "no-record order"), 0);
    assert_eq!(typed_count(&fixture, "held until the source answers"), 0);
    assert!(matches!(
        fixture.nexus.store.current_seat(CLAUDE_SEAT).unwrap(),
        crate::store::current_seat::CurrentSeatRead::NoRecord
    ));
    use crate::delivery::{FindsDeliveryTarget, TargetRefusal};
    use crate::generated::memory::SeatStanding;
    use crate::observe_agent::ObservesAgent;
    assert!(matches!(
        fixture.nexus.delivery_target(CLAUDE_SEAT),
        Err(TargetRefusal::Unconfirmed)
    ));
    assert!(
        matches!(fixture.nexus.agent_observation(CLAUDE_SEAT), Output::AgentObserved(observation) if observation.agent_state == signal_flow::AgentState::Unknown)
    );
    assert_eq!(
        fixture.nexus.perform(Operation::Drain(target.clone())),
        Outcome::Failed(Failed_Data::NoRecord)
    );
    fixture
        .nexus
        .store
        .record_observation(CLAUDE_SEAT, Some(SeatStanding::Ended))
        .unwrap();
    assert!(matches!(
        fixture.nexus.delivery_target(CLAUDE_SEAT),
        Err(TargetRefusal::FlowExited)
    ));
    assert!(
        matches!(fixture.nexus.agent_observation(CLAUDE_SEAT), Output::AgentObserved(observation) if observation.agent_state == signal_flow::AgentState::Gone)
    );
    assert_eq!(
        fixture.nexus.perform(Operation::Drain(target)),
        Outcome::Failed(Failed_Data::NotAwake)
    );
    assert_eq!(stored(&fixture, Model::Sol).queue.0.len(), 2);
}

fn await_stalled_writer(release: &std::path::Path) -> bool {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    let entered = release.with_extension("entered");
    while !entered.exists() && std::time::Instant::now() < deadline {
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    entered.exists()
}

#[test]
fn a_hung_queue_pane_write_leaves_admission_hooks_and_meta_running() {
    let fixture = NexusFixture::new();
    let release = fixture.directory.path().join("release-writer");
    fixture.accept_pane_operations(
        vec![working_claude_agent(&fixture)],
        PromptFixture::Stalled("w1:p3", &release),
    );
    let first = from_core("stalling-pane-input");
    seat(&fixture, vec![first.clone()]);
    append_main_rows(&fixture, &[end_turn("before blocked write")]);
    let target = address(Model::Sol);
    std::thread::scope(|scope| {
        let writer = scope.spawn(|| report(&fixture, claude_stop(&fixture)));
        let was_in_flight = await_stalled_writer(&release)
            && matches!(
                fixture
                    .nexus
                    .store
                    .queue_request_state(&first.logical_delivery_id),
                Ok(Some(RequestState::InFlight))
            );
        let (sent, received) = std::sync::mpsc::channel();
        let fixture = &fixture;
        let target = &target;
        scope.spawn(move || {
            let admission = fixture.nexus.admit_ordinary_send(
                signal_flow::SendRequest {
                    address: target.clone(),
                    sendable_message: signal_flow::SendableMessage::Soft(
                        signal_flow::SendableRequest::Notice("while writer waits".into()),
                    ),
                },
                None,
            );
            report(
                fixture,
                Event::PromptSubmitted(signal_flow::NativePrompt {
                    native_session_id_option: Some(signal_flow::NativeSessionId(
                        CLAUDE_SESSION.into(),
                    )),
                    prompt_id_option: None,
                    prompt_sha256: signal_flow::PromptSha256("0".repeat(64)),
                }),
            );
            let meta = fixture
                .nexus
                .dispatch_meta_serially(meta_signal_flow::Input::Inspect(target.clone()), None);
            sent.send((admission.response, meta)).unwrap();
        });
        let reply = received.recv_timeout(std::time::Duration::from_secs(5));
        // Always let the child exit, including when a regression held the gate.
        std::fs::write(&release, "").unwrap();
        writer.join().unwrap();
        assert!(
            was_in_flight,
            "writer began before its in-flight mark was durable"
        );
        let (admission, meta) = reply.expect("all three replies while the writer is stalled");
        assert!(matches!(admission, Output::SendAccepted(_)));
        assert!(matches!(meta, meta_signal_flow::Output::Inspected(_)));
    });
    assert_eq!(typed_count(&fixture, "stalling-pane-input"), 1);
    assert_eq!(typed_count(&fixture, "while writer waits"), 0);
}

#[test]
fn an_abrupt_request_admitted_during_a_stalled_write_drains_once_without_another_kick() {
    let fixture = NexusFixture::new();
    let release = fixture.directory.path().join("release-writer");
    fixture.accept_pane_operations(
        vec![working_claude_agent(&fixture)],
        PromptFixture::Stalled("w1:p3", &release),
    );
    seat(&fixture, vec![from_core("first blocked input")]);
    append_main_rows(&fixture, &[end_turn("before blocked write")]);
    let target = address(Model::Sol);
    std::thread::scope(|scope| {
        let writer = scope.spawn(|| report(&fixture, claude_stop(&fixture)));
        let entered = await_stalled_writer(&release);
        let (sent, received) = std::sync::mpsc::channel();
        let fixture = &fixture;
        let target = &target;
        scope.spawn(move || {
            let crate::sending::SendAnswer {
                response,
                pending,
                _pending_answer: pending_answer,
                ..
            } = fixture.nexus.admit_ordinary_send(
                signal_flow::SendRequest {
                    address: target.clone(),
                    sendable_message: signal_flow::SendableMessage::MiddleAbrupt(
                        signal_flow::SendableRequest::Order("abrupt input behind writer".into()),
                    ),
                },
                None,
            );
            let admitted = matches!(response, Output::SendAccepted(_));
            // The reply has finished writing. Release the pending admission
            // permit and run its one normal wake check while the writer
            // still owns the Address. Do not kick the seat again.
            if let Some(pending) = pending {
                drop(pending_answer);
                drop(pending.pane_input);
                fixture.nexus.wake_due(pending.address);
            } else {
                panic!("abrupt admission did not schedule a post-reply wake");
            }
            sent.send(admitted).unwrap();
        });
        let admitted = received.recv_timeout(std::time::Duration::from_secs(5));
        let typed_while_stalled = typed_count(fixture, "abrupt input behind writer");
        std::fs::write(&release, "").unwrap();
        writer.join().unwrap();
        assert!(entered, "the first pane write reached its latch");
        assert!(admitted.expect("the admission and its wake check completed while stalled"));
        assert_eq!(typed_while_stalled, 0);
    });
    let typed = fixture.typed().unwrap_or_default();
    assert_eq!(typed_count(&fixture, "first blocked input"), 1);
    assert_eq!(typed_count(&fixture, "abrupt input behind writer"), 1);
    assert!(typed.find("first blocked input") < typed.find("abrupt input behind writer"));
    assert!(queue(&fixture).is_empty());
}

fn reopen(fixture: &mut NexusFixture) {
    use crate::store::{FlowStore, OpensFlowStore, SetsNativeOwnerEnvironmentForTests};
    let path = fixture.directory.path().join("flow.sema");
    let placeholder = FlowStore::open(&fixture.directory.path().join("placeholder.sema")).unwrap();
    drop(std::mem::replace(
        &mut fixture.nexus.store,
        placeholder.into(),
    ));
    let mut store = FlowStore::open(&path).unwrap();
    store.set_native_owner_environment_for_tests(crate::store::NativeOwnerEnvironment {
        host_id: "isolated-test-host".into(),
        boot_id: "isolated-test-boot".into(),
    });
    fixture.nexus.store = store.into();
}

fn mark(fixture: &NexusFixture, request: &TestRequest) {
    let _turn = fixture.nexus.dispatch_gate.lock().unwrap();
    assert!(
        fixture
            .nexus
            .store
            .begin_queue_write(&address(Model::Sol), CLAUDE_SEAT, &[queue_entry(request)],)
            .unwrap()
    );
}

#[test]
fn restart_settles_an_in_flight_request_uncertain_without_typing_it() {
    let mut fixture = NexusFixture::new();
    fixture.accept_pane_operations(
        vec![working_claude_agent(&fixture)],
        PromptFixture::Prompted("w1:p3"),
    );
    let request = from_core("in-flight across restart");
    seat(&fixture, vec![request.clone()]);
    mark(&fixture, &request);

    reopen(&mut fixture);
    let id = flow_ethos::LogicalDeliveryId(request.logical_delivery_id);
    let snapshot = fixture
        .nexus
        .store
        .logical_delivery_snapshot(&id)
        .unwrap()
        .unwrap();
    assert_eq!(
        snapshot.delivery_state,
        signal_flow::DeliveryState::Uncertain
    );
    assert!(queue(&fixture).is_empty());
    assert!(matches!(
        fixture.nexus.perform(Operation::Drain(address(Model::Sol))),
        Outcome::Drained(entries) if entries.is_empty()
    ));
    assert_eq!(typed_count(&fixture, "in-flight across restart"), 0);

    reopen(&mut fixture);
    assert_eq!(
        fixture.nexus.store.logical_delivery_snapshot(&id).unwrap(),
        Some(snapshot)
    );
}

#[test]
fn restart_settles_a_queued_stock_attempt_without_typing_it() {
    use crate::generated::memory::{
        Device, Inode, NativeRequestId, NativeTranscriptCursor, Offset, ThreadId,
    };
    let mut fixture = NexusFixture::new();
    let first = from_core("ordinary queued input");
    let old = from_core("old native attempt");
    fixture.accept_pane_operations(
        vec![working_claude_agent(&fixture)],
        PromptFixture::Prompted("w1:p3"),
    );
    seat(&fixture, vec![first.clone(), old.clone()]);
    let target = address(Model::Sol);
    let attempt_id = meta_signal_flow::QueueAttemptId("0000000000000003".into());
    fixture
        .nexus
        .store
        .seed_stock_queue_attempt(QueueDeliveryAttempt {
            queue_request_id: meta_signal_flow::QueueRequestId(old.logical_delivery_id.clone()),
            queue_attempt_id: attempt_id.clone(),
            native_request_id: NativeRequestId(3),
            flow_id: flow_ethos::FlowId(CLAUDE_SEAT.into()),
            thread_id: ThreadId(CLAUDE_SESSION.into()),
            harness_kind: signal_flow::HarnessKind::Claude,
            native_history_anchor: NativeHistoryAnchor::ClaudeTranscript(NativeTranscriptCursor {
                device: Device(1),
                inode: Inode(2),
                offset: Offset(0),
            }),
            address: target.clone(),
            queue_evidence: meta_signal_flow::QueueEvidence::Logical(queue_entry(&old)),
            queue_delivery_phase: QueueDeliveryPhase::Prepared,
        });

    reopen(&mut fixture);
    assert_eq!(queue(&fixture), vec![first]);
    assert!(matches!(
        fixture
            .nexus
            .store
            .queue_delivery_attempt(&attempt_id.0)
            .unwrap()
            .unwrap()
            .queue_delivery_phase,
        QueueDeliveryPhase::Uncertain(_)
    ));
    assert_eq!(
        fixture
            .nexus
            .store
            .logical_delivery_snapshot(&flow_ethos::LogicalDeliveryId(old.logical_delivery_id))
            .unwrap()
            .unwrap()
            .delivery_state,
        signal_flow::DeliveryState::Uncertain
    );
    assert!(matches!(
        fixture.nexus.perform(Operation::Drain(target)),
        Outcome::Drained(entries) if entries.len() == 1
    ));
    assert_eq!(typed_count(&fixture, "ordinary queued input"), 1);
    assert_eq!(typed_count(&fixture, "old native attempt"), 0);
}

#[test]
fn a_fresh_wake_does_not_select_a_pane_writers_request() {
    let fixture = NexusFixture::new();
    let request = from_core("not a fresh launch input");
    seat(&fixture, vec![request.clone()]);
    mark(&fixture, &request);
    // As after the original seat exits and its writer has no committed outcome.
    let target = address(Model::Sol);
    fixture.nexus.store.seed_metaflow(Metaflow {
        state: State::Asleep,
        ..stored(&fixture, Model::Sol)
    });
    let _lease = fixture.nexus.waking.hold(&target).unwrap();
    assert!(matches!(
        fixture.nexus.perform(Operation::Wake(target)),
        Outcome::Failed(Failed_Data::StoreRefused)
    ));
    assert_eq!(queue(&fixture), vec![request]);
    assert_eq!(typed_count(&fixture, "not a fresh launch input"), 0);
}

/// Two queued requests go into the pane at one turn end as one payload, in
/// order, each typed once. The pane writer's own result is recorded in the
/// commit that takes both from the queue: the seat took the payload, so
/// both are Delivered at once, and no later turn end types them again.
#[test]
fn a_hand_over_is_recorded_from_the_pane_writers_own_result() {
    let fixture = NexusFixture::new();
    fixture.accept_pane_operations(
        vec![working_claude_agent(&fixture)],
        PromptFixture::Prompted("w1:p3"),
    );
    let first = from_core("first-request-body");
    let second = from_core("second-request-body");
    seat(&fixture, vec![first.clone(), second.clone()]);
    append_main_rows(&fixture, &[end_turn("before")]);

    report(&fixture, claude_stop(&fixture));
    assert_eq!(typed_count(&fixture, "first-request-body"), 1);
    assert_eq!(typed_count(&fixture, "second-request-body"), 1);
    assert_eq!(queue(&fixture), vec![], "both in one payload");
    let SentBy::Sender(sender) = first.sent_by.clone() else {
        panic!("queued request has a sender")
    };
    let recorded = fixture
        .nexus
        .store
        .read_logical_delivery_snapshots(&sender, None)
        .unwrap();
    for request in [&first, &second] {
        assert!(recorded.delivery_snapshot_vector.iter().any(|snapshot| {
            snapshot.logical_delivery_id.0 == request.logical_delivery_id
                && matches!(
                    &snapshot.delivery_state,
                    signal_flow::DeliveryState::Delivered(receipt)
                        if receipt.native_session_id.0 == CLAUDE_SESSION
                )
        }));
    }

    append_main_rows(&fixture, &[end_turn("after")]);
    report(&fixture, claude_stop(&fixture));
    assert_eq!(typed_count(&fixture, "first-request-body"), 1, "no replay");
    assert_eq!(typed_count(&fixture, "second-request-body"), 1, "no replay");
}

/// A Soft Notice does not start a turn: a resting seat holding only Soft
/// Notices is handed nothing at its turn end, and the Notice rides with the
/// next waking request, in one payload, oldest first.
#[test]
fn a_soft_notice_alone_starts_no_turn_and_rides_with_the_next_order() {
    let fixture = NexusFixture::new();
    fixture.accept_pane_operations(
        vec![working_claude_agent(&fixture)],
        PromptFixture::Prompted("w1:p3"),
    );
    let notice = TestRequest {
        request: Request::Notice("notice-body".into()),
        delivery: flow_ethos::Delivery::Queue,
        ..from_core("notice-body")
    };
    seat(&fixture, vec![notice.clone()]);
    append_main_rows(&fixture, &[end_turn("before")]);

    report(&fixture, claude_stop(&fixture));
    assert_eq!(typed_count(&fixture, "notice-body"), 0, "no turn started");
    assert_eq!(queue(&fixture), vec![notice.clone()]);

    let order = from_core("order-body");
    fixture.nexus.store.seed_metaflow(Metaflow {
        address: address(Model::Sol),
        state: State::Awake(flow_ethos::FlowId(CLAUDE_SEAT.into())),
        past: flow_ethos::Past(vec![]),
        queue: flow_ethos::Queue(vec![queue_entry(&notice), queue_entry(&order)]),
    });
    append_main_rows(&fixture, &[end_turn("again")]);
    report(&fixture, claude_stop(&fixture));
    assert_eq!(typed_count(&fixture, "notice-body"), 1);
    assert_eq!(typed_count(&fixture, "order-body"), 1);
    let typed = fixture.typed().unwrap_or_default();
    assert!(typed.find("notice-body") < typed.find("order-body"));
    assert_eq!(queue(&fixture), vec![], "both in one payload");
}

/// A hand-over Herdr refuses before any text reaches the pane leaves its
/// request queued, and the next turn end hands it over once.
#[test]
fn a_hand_over_refused_before_input_stays_queued_and_is_handed_over_again() {
    let fixture = NexusFixture::new();
    fixture.accept_pane_operations(
        vec![claude_agent(&fixture)],
        PromptFixture::RefusedBeforeInput("agent_not_found"),
    );
    let first = from_core("first-request-body");
    seat(&fixture, vec![first.clone()]);
    append_main_rows(&fixture, &[end_turn("before")]);
    report(&fixture, claude_stop(&fixture));
    assert_eq!(typed_count(&fixture, "first-request-body"), 0);
    assert_eq!(queue(&fixture), vec![first.clone()]);

    fixture.accept_pane_operations(
        vec![claude_agent(&fixture)],
        PromptFixture::Prompted("w1:p3"),
    );
    report(&fixture, claude_stop(&fixture));
    assert_eq!(typed_count(&fixture, "first-request-body"), 1);
    assert_eq!(queue(&fixture), vec![]);
}

/// A Stop of the seat's own session recorded before its launch was
/// confirmed is honoured at confirmation, as a Codex completion is; a Stop
/// of another session is not.
#[test]
fn a_claude_turn_end_recorded_before_confirmation_is_honoured_at_it() {
    let fixture = NexusFixture::new();
    fixture.accept_pane_operations(
        vec![claude_agent(&fixture)],
        PromptFixture::Prompted("w1:p3"),
    );
    let first = from_core("first-request-body");
    claude_seat(&fixture, 40_000);
    // Not yet confirmed: no metaflow is Awake as the seat.
    fixture.nexus.store.seed_metaflow(Metaflow {
        address: address(Model::Sol),
        state: State::Asleep,
        past: flow_ethos::Past(vec![]),
        queue: flow_ethos::Queue(vec![queue_entry(&first)]),
    });
    append_main_rows(&fixture, &[end_turn("first-turn")]);
    report(&fixture, claude_stop(&fixture));
    report(&fixture, foreign_stop());
    assert_eq!(typed_count(&fixture, "first-request-body"), 0);

    // Confirmation records it Awake and notes its recorded turn end.
    fixture.nexus.store.seed_metaflow(Metaflow {
        address: address(Model::Sol),
        state: State::Awake(flow_ethos::FlowId(CLAUDE_SEAT.into())),
        past: flow_ethos::Past(vec![]),
        queue: flow_ethos::Queue(vec![queue_entry(&first)]),
    });
    {
        let _gate = fixture.nexus.dispatch_gate.lock().unwrap();
        fixture.nexus.note_stop_at_confirmation(CLAUDE_SEAT);
    }
    fixture.nexus.settle_stops_at_confirmation();
    assert_eq!(typed_count(&fixture, "first-request-body"), 1);
    assert_eq!(queue(&fixture), vec![]);
}

/// Only another session's Stop landed before confirmation, and the seat's
/// transcript shows its turn still running: nothing is noted and nothing
/// typed.
#[test]
fn a_foreign_stop_before_confirmation_is_not_a_turn_end() {
    let fixture = NexusFixture::new();
    fixture.accept_pane_operations(
        vec![claude_agent(&fixture)],
        PromptFixture::Prompted("w1:p3"),
    );
    let first = from_core("first-request-body");
    claude_seat(&fixture, 40_000);
    // The seat took its first request's prompt: it is in its turn.
    super::waking::claude_prompt_taken(&fixture);
    fixture.nexus.store.seed_metaflow(Metaflow {
        address: address(Model::Sol),
        state: State::Awake(flow_ethos::FlowId(CLAUDE_SEAT.into())),
        past: flow_ethos::Past(vec![]),
        queue: flow_ethos::Queue(vec![queue_entry(&first)]),
    });
    append_main_rows(
        &fixture,
        &[
            serde_json::json!({"type":"user","sessionId":CLAUDE_SESSION,"uuid":"u1",
            "message":{"content":"still working"}}),
        ],
    );
    assert_eq!(
        fixture.nexus.report(Report_Data {
            flow_id: flow_ethos::FlowId(CLAUDE_SEAT.into()),
            process_option: None,
            event: foreign_stop(),
        }),
        Output::Reported
    );
    {
        let _gate = fixture.nexus.dispatch_gate.lock().unwrap();
        fixture.nexus.note_stop_at_confirmation(CLAUDE_SEAT);
    }
    fixture.nexus.settle_stops_at_confirmation();
    assert_eq!(typed_count(&fixture, "first-request-body"), 0);
    assert_eq!(queue(&fixture), vec![first]);
}

pub(super) fn resolver() -> ResolverIdentity {
    ResolverIdentity {
        process_identity: ProcessIdentity {
            process_id: meta_signal_flow::ProcessId(4242),
            process_user_id: meta_signal_flow::ProcessUserId(1001),
            process_start_token: meta_signal_flow::ProcessStartToken("777".into()),
        },
        flow_id_option: Some(flow_ethos::FlowId("aa034f".into())),
    }
}
