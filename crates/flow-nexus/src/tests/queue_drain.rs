//! A Claude seat's queue drains on its own turn ends: its hook's Stop that
//! names its registered session and transcript, confirmed by the main
//! `end_turn` its transcript records, whatever Herdr's screen shows while
//! background agents run. A typed request settles on its exact receipt:
//! the seat's UserPromptSubmit digest, or the transcript row that took it.
//! Nothing is typed twice, a hand-over is reconciled from the transcript
//! after a crash, and what cannot be decided is read with
//! ReadQueueDeliveries.

use super::waking::{
    CLAUDE_SEAT, CLAUDE_SESSION, address, append_main_rows, claude_agent, claude_seat, claude_stop,
    end_turn, stored, working_claude_agent,
};
use super::{ControlsHerdrSnapshot, NexusFixture, PromptFixture};
use crate::Dispatches;
use crate::composition::RendersQueuedWork;
use crate::generated::operation::Outcome;
use crate::reporting::RecordsReports;
use crate::store::logical_delivery::RecordsLogicalDeliveries;
use crate::store::metaflow::SeedsMetaflow;
use crate::store::queue_delivery::AccountsQueueDelivery;
use crate::waking::{PerformsWakeAndDrain, ReconcilesQueueDelivery, SettlesStopsAtConfirmation};
use flow_ethos::State;
use flow_native::DigestsPrompt;
use meta_signal_flow::{
    ProcessIdentity, Query as MetaQuery, QueueDeliveryResolution, QueueDeliveryStanding,
    QueueResolutionOutcome, ResolveQueueDeliveryRejected_Data, ResolverIdentity,
    Response as MetaResponse, Undecided,
};
use signal_flow::{
    Address, Aspect, Delivery, Event, Layer, Message, Metaflow, NativePrompt, NativeStop,
    QueueEntry, Report_Data, Request, Response, SentBy,
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
            topic: "core".into(),
            layer: Layer::Secondary,
        }),
        request: Request::Order(body.into()),
        delivery: Delivery::Wake,
    }
}

/// The text a hand-over types for one queued request.
fn typed_text(request: &TestRequest) -> String {
    [queue_entry(request)].prompt_text()
}

fn queue_entry(request: &TestRequest) -> QueueEntry {
    QueueEntry {
        logical_delivery_id: request.logical_delivery_id.clone(),
        sent_by: request.sent_by.clone(),
        delivery: request.delivery.clone(),
        message: Message::Soft(request.request.clone()),
    }
}

/// The fixture Claude seat Awake at the Secondary address, holding `queue`.
fn seat(fixture: &NexusFixture, queue: Vec<TestRequest>) {
    claude_seat(fixture, 40_000);
    fixture.nexus.store.seed_metaflow(Metaflow {
        address: address(Layer::Secondary),
        state: State::Awake(CLAUDE_SEAT.into()),
        past: vec![],
        queue: queue
            .into_iter()
            .map(|request| QueueEntry {
                logical_delivery_id: request.logical_delivery_id,
                sent_by: request.sent_by,
                delivery: request.delivery,
                message: Message::Soft(request.request),
            })
            .collect(),
    });
}

/// A report as the hook sends it: recorded, answered, then acted on.
fn report(fixture: &NexusFixture, event: Event) {
    assert_eq!(
        fixture.nexus.report(Report_Data {
            flow_id: CLAUDE_SEAT.into(),
            event: event.clone(),
        }),
        Response::Reported
    );
    fixture.nexus.after_report(&CLAUDE_SEAT.to_owned(), &event);
}

fn foreign_stop() -> Event {
    Event::Stopped(NativeStop {
        native_session_id_option: Some("0ther000-0000-4000-8000-000000000009".into()),
        transcript_path_option: Some("/elsewhere/other.jsonl".into()),
        last_assistant_message_option: None,
    })
}

fn submitted(session: &str, prompt_id: &str, text: &str) -> Event {
    Event::PromptSubmitted(NativePrompt {
        native_session_id_option: Some(session.into()),
        prompt_id_option: Some(prompt_id.into()),
        prompt_sha256: text.prompt_sha256(),
    })
}

fn queue(fixture: &NexusFixture) -> Vec<TestRequest> {
    stored(fixture, Layer::Secondary)
        .queue
        .into_iter()
        .map(|entry| TestRequest {
            logical_delivery_id: entry.logical_delivery_id,
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

fn standings(fixture: &NexusFixture) -> Vec<QueueDeliveryStanding> {
    match fixture
        .nexus
        .dispatch_meta(meta_signal_flow::Query::ReadQueueDeliveries(address(
            Layer::Secondary,
        ))) {
        meta_signal_flow::Response::QueueDeliveriesRead(read) => read
            .queue_delivery_vector
            .into_iter()
            .map(|delivery| delivery.queue_delivery_standing)
            .collect(),
        other => panic!("not read: {other:?}"),
    }
}

#[test]
fn logical_delivery_snapshots_are_sender_filtered_replayable_and_idempotent() {
    let fixture = NexusFixture::new();
    let target = address(Layer::Secondary);
    fixture.nexus.store.seed_metaflow(Metaflow {
        address: target.clone(),
        state: State::Asleep,
        past: vec![],
        queue: vec![],
    });
    let sender = Address {
        aspect: Aspect::Psyche,
        topic: "core".into(),
        layer: Layer::Secondary,
    };
    let other_sender = Address {
        aspect: Aspect::Mind,
        topic: "core".into(),
        layer: Layer::Secondary,
    };
    let Response::SendAccepted(accepted) = fixture
        .nexus
        .store
        .accept_logical_delivery(
            &target,
            SentBy::Sender(sender.clone()),
            Delivery::Queue,
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
        .read_logical_delivery_snapshots(&sender, Some(accepted.delivery_cursor))
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
/// its own Stop hands over its oldest request; the Prompted fixture records
/// the exact native user row. A foreign Stop does nothing.
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
    assert_eq!(typed_count(&fixture, "second-request-body"), 0);
    assert!(
        !fixture
            .typed()
            .unwrap_or_default()
            .contains("Psyche core Secondary")
    );
    assert_eq!(queue(&fixture), vec![second]);
    assert_eq!(
        stored(&fixture, Layer::Secondary).state,
        State::Awake(CLAUDE_SEAT.into())
    );
}

/// An unprovable first hand-over stays Uncertain and is not replayed, while
/// the second request is delivered and receipted. DeliverAll does not claim
/// the whole cohort finished until a late exact receipt settles the first.
#[test]
fn two_queued_claude_requests_drain_in_order_once_each_on_their_receipts() {
    let fixture = NexusFixture::new();
    fixture.accept_pane_operations(
        vec![working_claude_agent(&fixture)],
        PromptFixture::PromptedUnrecorded("w1:p3"),
    );
    let first = from_core("first-request-body");
    let second = from_core("second-request-body");
    seat(&fixture, vec![first.clone(), second.clone()]);
    append_main_rows(&fixture, &[end_turn("before")]);

    report(&fixture, claude_stop(&fixture));
    assert_eq!(typed_count(&fixture, "first-request-body"), 1);
    assert_eq!(
        queue(&fixture),
        vec![first.clone(), second.clone()],
        "typed, not yet received"
    );

    // The same turn end again: the unresolved logical request remains
    // uncertain in its journal but is parked out of the active queue.
    report(&fixture, claude_stop(&fixture));
    assert_eq!(typed_count(&fixture, "first-request-body"), 1);
    assert_eq!(typed_count(&fixture, "second-request-body"), 1);
    assert_eq!(queue(&fixture), vec![second.clone()]);
    assert!(matches!(
        standings(&fixture).as_slice(),
        [
            QueueDeliveryStanding::Uncertain(Some(Undecided::AbsenceUnprovable)),
            QueueDeliveryStanding::Uncertain(_),
        ]
    ));

    // The exact prompt in another session is not this seat's receipt.
    report(
        &fixture,
        submitted(
            "0ther000-0000-4000-8000-000000000009",
            "elsewhere",
            &typed_text(&first),
        ),
    );
    assert_eq!(queue(&fixture), vec![second.clone()]);

    append_main_rows(
        &fixture,
        &[
            serde_json::json!({"type":"user","sessionId":CLAUDE_SESSION,"uuid":"u-second",
            "message":{"content":format!("\n\n<pasted_content id=\"04ef\">\n{}\n</pasted_content id=\"04ef\">\n", typed_text(&second))}}),
            end_turn("second-received"),
        ],
    );
    report(&fixture, claude_stop(&fixture));
    assert_eq!(
        queue(&fixture),
        vec![],
        "the exact native receipt settles the later hand-over"
    );
    let SentBy::Sender(first_sender) = first.sent_by.clone() else {
        panic!("first queued request has a sender")
    };
    let settled = fixture
        .nexus
        .store
        .read_logical_delivery_snapshots(&first_sender, None)
        .unwrap();
    assert!(settled.delivery_snapshot_vector.iter().any(|snapshot| {
        snapshot.logical_delivery_id == first.logical_delivery_id
            && matches!(
                &snapshot.delivery_state,
                signal_flow::DeliveryState::Uncertain
            )
    }));
    assert!(settled.delivery_snapshot_vector.iter().any(|snapshot| {
        snapshot.logical_delivery_id == second.logical_delivery_id
            && matches!(
                &snapshot.delivery_state,
                signal_flow::DeliveryState::Delivered(_)
            )
    }));
    report(
        &fixture,
        submitted(CLAUDE_SESSION, "prompt-first", &typed_text(&first)),
    );
    assert!(queue(&fixture).is_empty());
    assert_eq!(
        fixture
            .nexus
            .store
            .read_logical_delivery_snapshots(&first_sender, None)
            .unwrap(),
        settled,
        "duplicate exact native receipt changes neither state nor cursor"
    );
    assert_eq!(
        typed_count(&fixture, "second-request-body"),
        1,
        "the second request was handed over once and then receipted"
    );

    assert!(
        !matches!(
            fixture.nexus.drain_all(address(Layer::Secondary)),
            Outcome::Drained(_)
        ),
        "DeliverAll cannot report the cohort complete while the first delivery is uncertain"
    );
    assert_eq!(typed_count(&fixture, "first-request-body"), 1, "no replay");
    assert!(queue(&fixture).is_empty());

    // A later exact receipt can still correlate the parked attempt without
    // putting it back into the active queue or handing it over again.
    append_main_rows(
        &fixture,
        &[
            serde_json::json!({"type":"user","sessionId":CLAUDE_SESSION,"uuid":"u-first-late",
            "message":{"content":format!("\n\n<pasted_content id=\"04ee\">\n{}\n</pasted_content id=\"04ee\">\n", typed_text(&first))}}),
        ],
    );
    report(
        &fixture,
        submitted(CLAUDE_SESSION, "prompt-first-late", &typed_text(&first)),
    );
    let settled_late = fixture
        .nexus
        .store
        .read_logical_delivery_snapshots(&first_sender, None)
        .unwrap();
    assert!(
        settled_late
            .delivery_snapshot_vector
            .iter()
            .any(|snapshot| {
                snapshot.logical_delivery_id == first.logical_delivery_id
                    && matches!(
                        &snapshot.delivery_state,
                        signal_flow::DeliveryState::Delivered(_)
                    )
            })
    );
    assert!(queue(&fixture).is_empty());
    assert_eq!(typed_count(&fixture, "first-request-body"), 1);
    assert_eq!(typed_count(&fixture, "second-request-body"), 1);
    let typed = fixture.typed().unwrap_or_default();
    assert!(typed.find("first-request-body") < typed.find("second-request-body"));
    assert_eq!(typed.matches("first-request-body").count(), 1);
    assert_eq!(typed.matches("second-request-body").count(), 1);
    assert!(!typed.contains("Psyche core Secondary"));
    assert!(standings(&fixture).is_empty());
}

/// A background task's notification takes the main turn before the typed
/// request: its prompt is no receipt, the request waits in Claude Code's
/// input queue, and the turn end after the notification finds it pending.
/// When a later row takes it, it settles and the next is handed over.
#[test]
fn a_task_notification_that_takes_the_turn_first_settles_nothing() {
    let fixture = NexusFixture::new();
    fixture.accept_pane_operations(
        vec![working_claude_agent(&fixture)],
        PromptFixture::PromptedUnrecorded("w1:p3"),
    );
    let first = from_core("first-request-body");
    let second = from_core("second-request-body");
    seat(&fixture, vec![first.clone(), second.clone()]);
    append_main_rows(&fixture, &[end_turn("before")]);
    report(&fixture, claude_stop(&fixture));
    assert_eq!(typed_count(&fixture, "first-request-body"), 1);

    let notification = "<task-notification>\n<status>completed</status>\n</task-notification>";
    report(
        &fixture,
        submitted(CLAUDE_SESSION, "prompt-notice", notification),
    );
    assert_eq!(queue(&fixture), vec![first.clone(), second.clone()]);

    let queued = format!(
        "<pasted_content id=\"q1\">\n{}\n</pasted_content id=\"q1\">",
        typed_text(&first)
    );
    append_main_rows(
        &fixture,
        &[
            serde_json::json!({"type":"user","sessionId":CLAUDE_SESSION,"uuid":"notice",
                "origin":{"kind":"task-notification"},"message":{"content":notification}}),
            serde_json::json!({"type":"queue-operation","operation":"enqueue",
                "sessionId":CLAUDE_SESSION,"content":queued}),
            end_turn("notice-answered"),
        ],
    );
    report(&fixture, claude_stop(&fixture));
    assert_eq!(queue(&fixture), vec![first.clone(), second.clone()]);
    assert_eq!(typed_count(&fixture, "first-request-body"), 1);
    assert_eq!(typed_count(&fixture, "second-request-body"), 0);
    assert_eq!(
        standings(&fixture),
        vec![QueueDeliveryStanding::Uncertain(Some(
            Undecided::PendingInHarness
        ))]
    );

    append_main_rows(
        &fixture,
        &[
            serde_json::json!({"type":"queue-operation","operation":"dequeue",
                "sessionId":CLAUDE_SESSION}),
            serde_json::json!({"type":"user","sessionId":CLAUDE_SESSION,"uuid":"u-first",
                "message":{"content":queued}}),
            end_turn("first-answered"),
        ],
    );
    report(&fixture, claude_stop(&fixture));
    assert_eq!(typed_count(&fixture, "first-request-body"), 1);
    assert_eq!(typed_count(&fixture, "second-request-body"), 1);
    assert_eq!(
        queue(&fixture),
        vec![second],
        "the first settled; the second typed"
    );
}

/// The Nexus stopped between typing a request and recording its receipt.
/// While it was down the seat took the request and finished its turn. At
/// the next start the transcript settles the request, which is never typed
/// again, and the seat's recorded turn end hands it the next.
#[test]
fn a_hand_over_a_crash_left_unreceived_is_reconciled_at_start_without_a_second_typing() {
    let fixture = NexusFixture::new();
    fixture.accept_pane_operations(
        vec![claude_agent(&fixture)],
        PromptFixture::PromptedUnrecorded("w1:p3"),
    );
    let first = from_core("first-request-body");
    let second = from_core("second-request-body");
    seat(&fixture, vec![first.clone(), second.clone()]);
    append_main_rows(&fixture, &[end_turn("before")]);
    report(&fixture, claude_stop(&fixture));
    assert_eq!(typed_count(&fixture, "first-request-body"), 1);

    append_main_rows(
        &fixture,
        &[
            serde_json::json!({"type":"user","sessionId":CLAUDE_SESSION,"uuid":"u-first",
            "message":{"content":format!(
                "\n\n<pasted_content id=\"04ee\">\n{}\n</pasted_content id=\"04ee\">\n",
                typed_text(&first)
            )}}),
            end_turn("first-answered"),
        ],
    );
    // What a restart does: the store makes every receipt-less attempt
    // Uncertain, and the Nexus reconciles them from native history.
    assert_eq!(
        fixture
            .nexus
            .store
            .recover_outstanding_deliveries()
            .unwrap()
            .len(),
        1
    );
    fixture.nexus.recover_deliveries();
    assert_eq!(
        typed_count(&fixture, "first-request-body"),
        1,
        "never again"
    );
    assert_eq!(typed_count(&fixture, "second-request-body"), 1);
    assert_eq!(queue(&fixture), vec![second]);
}

/// A hand-over Herdr refuses before any text reaches the pane is proved
/// absent, authorizing one retry. The Prompted fixture records its native
/// USER row, so the retry settles exactly once.
#[test]
fn a_hand_over_refused_before_input_is_absent_and_handed_over_again() {
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
    assert_eq!(
        standings(&fixture),
        vec![QueueDeliveryStanding::Absent(
            meta_signal_flow::AbsenceProof::HandOverRefused
        )]
    );

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
        address: address(Layer::Secondary),
        state: State::Asleep,
        past: vec![],
        queue: vec![queue_entry(&first)],
    });
    append_main_rows(&fixture, &[end_turn("first-turn")]);
    report(&fixture, claude_stop(&fixture));
    report(&fixture, foreign_stop());
    assert_eq!(typed_count(&fixture, "first-request-body"), 0);

    // Confirmation records it Awake and notes its recorded turn end.
    fixture.nexus.store.seed_metaflow(Metaflow {
        address: address(Layer::Secondary),
        state: State::Awake(CLAUDE_SEAT.into()),
        past: vec![],
        queue: vec![queue_entry(&first)],
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
    fixture.nexus.store.seed_metaflow(Metaflow {
        address: address(Layer::Secondary),
        state: State::Awake(CLAUDE_SEAT.into()),
        past: vec![],
        queue: vec![queue_entry(&first)],
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
            flow_id: CLAUDE_SEAT.into(),
            event: foreign_stop(),
        }),
        Response::Reported
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
            process_id: 4242,
            process_user_id: 1001,
            process_start_token: "777".into(),
        },
        flow_id_option: Some("aa034f".into()),
    }
}

/// The attempt id of the seat's one unsettled hand-over.
fn attempt_id(fixture: &NexusFixture) -> String {
    match fixture
        .nexus
        .dispatch_meta(MetaQuery::ReadQueueDeliveries(address(Layer::Secondary)))
    {
        MetaResponse::QueueDeliveriesRead(read) => {
            let Some(delivery) = read.queue_delivery_vector.first() else {
                panic!("at least one unsettled hand-over: {read:?}")
            };
            delivery.queue_attempt_id.clone()
        }
        other => panic!("not read: {other:?}"),
    }
}

fn resolve(
    fixture: &NexusFixture,
    attempt: &str,
    outcome: QueueResolutionOutcome,
    reason: &str,
) -> MetaResponse {
    fixture.nexus.dispatch_meta_from(
        MetaQuery::ResolveQueueDelivery(QueueDeliveryResolution {
            queue_attempt_id: attempt.into(),
            queue_resolution_outcome: outcome,
            queue_resolution_reason: reason.into(),
        }),
        Some(resolver()),
    )
}

/// The resolution as the meta socket serves it: the gate given back after
/// the commit, a resting seat handed its request at once.
fn resolve_served(
    fixture: &NexusFixture,
    attempt: &str,
    outcome: QueueResolutionOutcome,
) -> MetaResponse {
    use crate::DispatchesSerially;
    fixture.nexus.dispatch_meta_serially(
        MetaQuery::ResolveQueueDelivery(QueueDeliveryResolution {
            queue_attempt_id: attempt.into(),
            queue_resolution_outcome: outcome,
            queue_resolution_reason: "the owner looked".into(),
        }),
        Some(resolver()),
    )
}

/// A Claude hand-over typed and never seen again, its Stop reconciled to
/// Uncertain(AbsenceUnprovable). Returns the attempt id.
fn stuck_hand_over(fixture: &NexusFixture, queue: Vec<TestRequest>) -> String {
    let expected_attempts = queue.len();
    fixture.accept_pane_operations(
        vec![working_claude_agent(fixture)],
        PromptFixture::PromptedUnrecorded("w1:p3"),
    );
    seat(fixture, queue);
    append_main_rows(fixture, &[end_turn("before")]);
    report(fixture, claude_stop(fixture));
    report(fixture, claude_stop(fixture));
    let standings = standings(fixture);
    assert_eq!(standings.len(), expected_attempts);
    assert!(matches!(
        standings.first(),
        Some(QueueDeliveryStanding::Uncertain(Some(
            Undecided::AbsenceUnprovable
        )))
    ));
    assert!(
        standings
            .iter()
            .skip(1)
            .all(|standing| matches!(standing, QueueDeliveryStanding::Uncertain(_)))
    );
    attempt_id(fixture)
}

/// The owner cannot resolve an uncertain hand-over as Delivered: only an
/// exact native receipt can settle the queue entry.
#[test]
fn an_uncertain_hand_over_cannot_be_resolved_delivered_without_a_native_receipt() {
    let fixture = NexusFixture::new();
    let first = from_core("first-request-body");
    let second = from_core("second-request-body");
    let attempt = stuck_hand_over(&fixture, vec![first.clone(), second.clone()]);

    assert_eq!(
        resolve(
            &fixture,
            &attempt,
            QueueResolutionOutcome::Delivered,
            "the pane shows it taken"
        ),
        MetaResponse::ResolveQueueDeliveryRejected(ResolveQueueDeliveryRejected_Data::NotOldest,)
    );
    assert_eq!(queue(&fixture), vec![second.clone()]);
    assert!(matches!(
        standings(&fixture).as_slice(),
        [
            QueueDeliveryStanding::Uncertain(Some(Undecided::AbsenceUnprovable)),
            QueueDeliveryStanding::Uncertain(_),
        ]
    ));

    report(&fixture, claude_stop(&fixture));
    assert_eq!(typed_count(&fixture, "first-request-body"), 1);
    assert_eq!(typed_count(&fixture, "second-request-body"), 1);
}

/// A parked uncertain entry cannot be owner-resolved to Absent and replayed.
#[test]
fn a_parked_absence_unprovable_hand_over_is_never_replayed_by_owner_resolution() {
    let fixture = NexusFixture::new();
    let first = from_core("first-request-body");
    let second = from_core("second-request-body");
    let attempt = stuck_hand_over(&fixture, vec![first.clone(), second.clone()]);
    assert_eq!(typed_count(&fixture, "first-request-body"), 1);

    assert!(matches!(
        resolve(
            &fixture,
            &attempt,
            QueueResolutionOutcome::Absent,
            "composer was empty"
        ),
        MetaResponse::ResolveQueueDeliveryRejected(ResolveQueueDeliveryRejected_Data::NotOldest)
    ));
    assert!(matches!(
        resolve(
            &fixture,
            &attempt,
            QueueResolutionOutcome::Absent,
            "composer was empty"
        ),
        MetaResponse::ResolveQueueDeliveryRejected(ResolveQueueDeliveryRejected_Data::NotOldest)
    ));
    assert_eq!(
        typed_count(&fixture, "first-request-body"),
        1,
        "parked delivery is not replayed"
    );
    assert_eq!(typed_count(&fixture, "second-request-body"), 1);
    assert_eq!(queue(&fixture), vec![second]);
    assert!(matches!(
        standings(&fixture).as_slice(),
        [
            QueueDeliveryStanding::Uncertain(Some(Undecided::AbsenceUnprovable)),
            QueueDeliveryStanding::Uncertain(_),
        ]
    ));
}

#[test]
fn a_resolution_is_refused_without_a_peer_for_an_unknown_or_non_uncertain_attempt() {
    let fixture = NexusFixture::new();
    let first = from_core("first-request-body");
    let attempt = stuck_hand_over(&fixture, vec![first.clone()]);

    assert_eq!(
        resolve(
            &fixture,
            "queue-attempt-nothing",
            QueueResolutionOutcome::Delivered,
            "x"
        ),
        MetaResponse::ResolveQueueDeliveryRejected(
            ResolveQueueDeliveryRejected_Data::UnknownAttempt
        )
    );
    assert_eq!(
        fixture
            .nexus
            .dispatch_meta(MetaQuery::ResolveQueueDelivery(QueueDeliveryResolution {
                queue_attempt_id: attempt.clone(),
                queue_resolution_outcome: QueueResolutionOutcome::Delivered,
                queue_resolution_reason: "x".into(),
            })),
        MetaResponse::MetaRefused(meta_signal_flow::MetaRefusal::PeerUnknown)
    );
    assert!(queue(&fixture).is_empty(), "uncertain entry is parked");

    assert_eq!(
        resolve(&fixture, &attempt, QueueResolutionOutcome::Delivered, "x"),
        MetaResponse::ResolveQueueDeliveryRejected(ResolveQueueDeliveryRejected_Data::NotOldest)
    );
    assert!(matches!(
        resolve(&fixture, &attempt, QueueResolutionOutcome::Absent, "again"),
        MetaResponse::ResolveQueueDeliveryRejected(ResolveQueueDeliveryRejected_Data::NotOldest)
    ));
}

/// A resting Claude seat does not replay a parked, unresolved request when
/// an owner reports Absent without native proof.
#[test]
fn a_resting_claude_seat_does_not_replay_a_parked_uncertain_request() {
    let fixture = NexusFixture::new();
    let first = from_core("first-request-body");
    let attempt = stuck_hand_over(&fixture, vec![first.clone()]);
    assert_eq!(typed_count(&fixture, "first-request-body"), 1);

    assert!(matches!(
        resolve_served(&fixture, &attempt, QueueResolutionOutcome::Absent),
        MetaResponse::ResolveQueueDeliveryRejected(ResolveQueueDeliveryRejected_Data::NotOldest)
    ));
    assert_eq!(typed_count(&fixture, "first-request-body"), 1);
    report(&fixture, claude_stop(&fixture));
    assert_eq!(typed_count(&fixture, "first-request-body"), 1);
    assert!(queue(&fixture).is_empty());
}

/// A seat in a turn (a prompt after its last end_turn) is not handed the
/// request by the resolution; its turn end does.
#[test]
fn a_working_claude_seat_resolved_absent_waits_for_its_turn_end() {
    let fixture = NexusFixture::new();
    let first = from_core("first-request-body");
    let attempt = stuck_hand_over(&fixture, vec![first.clone()]);
    append_main_rows(
        &fixture,
        &[
            serde_json::json!({"type":"user","sessionId":CLAUDE_SESSION,"uuid":"u9",
        "message":{"content":"still working"}}),
        ],
    );

    assert_eq!(
        resolve_served(&fixture, &attempt, QueueResolutionOutcome::Absent),
        MetaResponse::ResolveQueueDeliveryRejected(ResolveQueueDeliveryRejected_Data::NotOldest)
    );
    assert_eq!(typed_count(&fixture, "first-request-body"), 1);
    assert!(queue(&fixture).is_empty());

    append_main_rows(&fixture, &[end_turn("done")]);
    report(&fixture, claude_stop(&fixture));
    assert_eq!(typed_count(&fixture, "first-request-body"), 1);
}

/// An owner cannot resolve the parked first hand-over or replay it as
/// Delivered; the later queued Soft request proceeds independently.
#[test]
fn a_resting_claude_seat_does_not_advance_after_unwitnessed_delivered_resolution() {
    let fixture = NexusFixture::new();
    let first = from_core("first-request-body");
    let second = from_core("second-request-body");
    let attempt = stuck_hand_over(&fixture, vec![first.clone(), second.clone()]);

    assert_eq!(
        resolve_served(&fixture, &attempt, QueueResolutionOutcome::Delivered),
        MetaResponse::ResolveQueueDeliveryRejected(ResolveQueueDeliveryRejected_Data::NotOldest)
    );
    assert_eq!(typed_count(&fixture, "first-request-body"), 1);
    assert_eq!(typed_count(&fixture, "second-request-body"), 1);
    assert_eq!(queue(&fixture), vec![second]);
}
