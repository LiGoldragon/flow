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
use crate::reporting::RecordsReports;
use crate::store::metaflow::SeedsMetaflow;
use crate::store::queue_delivery::AccountsQueueDelivery;
use crate::waking::{ReconcilesQueueDelivery, SettlesStopsAtConfirmation};
use flow_ethos::State;
use flow_native::DigestsPrompt;
use meta_signal_flow::{
    ProcessIdentity, Query as MetaQuery, QueueDeliveryResolution, QueueDeliveryResolved_Data,
    QueueDeliveryStanding, QueueResolutionOutcome, ResolveQueueDeliveryRejected_Data,
    ResolverIdentity, Response as MetaResponse, Undecided,
};
use signal_flow::{
    Address, Aspect, Event, Layer, Metaflow, NativePrompt, NativeStop, QueuedRequest, Report_Data,
    Request, Response, SentBy,
};

fn from_core(body: &str) -> QueuedRequest {
    QueuedRequest {
        sent_by: SentBy::Sender(Address {
            aspect: Aspect::Psyche,
            topic: "core".into(),
            layer: Layer::Secondary,
        }),
        request: Request::Order(body.into()),
    }
}

/// The text a hand-over types for one queued request.
fn typed_text(request: &QueuedRequest) -> String {
    [request.clone()].prompt_text()
}

/// The fixture Claude seat Awake at the Secondary address, holding `queue`.
fn seat(fixture: &NexusFixture, queue: Vec<QueuedRequest>) {
    claude_seat(fixture, 40_000);
    fixture.nexus.store.seed_metaflow(Metaflow {
        address: address(Layer::Secondary),
        state: State::Awake(CLAUDE_SEAT.into()),
        past: vec![],
        queue,
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
    })
}

fn submitted(session: &str, prompt_id: &str, text: &str) -> Event {
    Event::PromptSubmitted(NativePrompt {
        native_session_id_option: Some(session.into()),
        prompt_id_option: Some(prompt_id.into()),
        prompt_sha256: text.prompt_sha256(),
    })
}

fn queue(fixture: &NexusFixture) -> Vec<QueuedRequest> {
    stored(fixture, Layer::Secondary).queue
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

/// The seat's main turn ended while a background agent still runs: its
/// sidechain rows follow the main `end_turn` and Herdr shows Working. Its
/// own Stop hands it the oldest request; a Stop of another session does
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
    assert_eq!(typed_count(&fixture, "second-request-body"), 0);
    assert!(
        fixture
            .typed()
            .unwrap_or_default()
            .contains("Sender.{ Psyche core Secondary }")
    );
    assert_eq!(queue(&fixture), vec![second], "settled on its receipt");
    assert_eq!(
        stored(&fixture, Layer::Secondary).state,
        State::Awake(CLAUDE_SEAT.into())
    );
}

/// Two requests drain in order, once each, senders intact: the first
/// settles on its UserPromptSubmit receipt, the second is handed over at
/// the turn end after it. A repeated Stop, a repeated receipt and a prompt
/// of another session change nothing.
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

    // The same turn end again: the outstanding hand-over is reconciled,
    // nothing is typed again, and why it is undecided is readable.
    report(&fixture, claude_stop(&fixture));
    assert_eq!(typed_count(&fixture, "first-request-body"), 1);
    assert_eq!(typed_count(&fixture, "second-request-body"), 0);
    assert_eq!(
        standings(&fixture),
        vec![QueueDeliveryStanding::Uncertain(Some(
            Undecided::AbsenceUnprovable
        ))]
    );

    // The exact prompt in another session is not this seat's receipt.
    report(
        &fixture,
        submitted(
            "0ther000-0000-4000-8000-000000000009",
            "elsewhere",
            &typed_text(&first),
        ),
    );
    assert_eq!(queue(&fixture), vec![first.clone(), second.clone()]);

    report(
        &fixture,
        submitted(CLAUDE_SESSION, "prompt-first", &typed_text(&first)),
    );
    assert_eq!(
        queue(&fixture),
        vec![second.clone()],
        "settled on its receipt"
    );
    report(
        &fixture,
        submitted(CLAUDE_SESSION, "prompt-first", &typed_text(&first)),
    );
    assert_eq!(queue(&fixture), vec![second.clone()]);
    assert_eq!(typed_count(&fixture, "second-request-body"), 0);

    // The first request's own turn runs and ends.
    append_main_rows(
        &fixture,
        &[
            serde_json::json!({"type":"user","sessionId":CLAUDE_SESSION,"uuid":"u-first",
                "message":{"content":typed_text(&first)}}),
            end_turn("first-answered"),
        ],
    );
    report(&fixture, claude_stop(&fixture));
    assert_eq!(typed_count(&fixture, "second-request-body"), 1);
    report(
        &fixture,
        submitted(CLAUDE_SESSION, "prompt-second", &typed_text(&second)),
    );
    assert_eq!(queue(&fixture), vec![]);
    assert_eq!(typed_count(&fixture, "first-request-body"), 1);
    assert_eq!(typed_count(&fixture, "second-request-body"), 1);
    let typed = fixture.typed().unwrap_or_default();
    assert!(typed.find("first-request-body") < typed.find("second-request-body"));
    assert_eq!(typed.matches("Sender.{ Psyche core Secondary }").count(), 2);
    assert_eq!(standings(&fixture), vec![]);
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
/// absent: a new attempt carries the request at the next turn end.
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
        queue: vec![first.clone()],
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
        queue: vec![first.clone()],
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
        queue: vec![first.clone()],
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

fn resolver() -> ResolverIdentity {
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
            let [delivery] = read.queue_delivery_vector.as_slice() else {
                panic!("one unsettled hand-over: {read:?}")
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

/// A Claude hand-over typed and never seen again, its Stop reconciled to
/// Uncertain(AbsenceUnprovable). Returns the attempt id.
fn stuck_hand_over(fixture: &NexusFixture, queue: Vec<QueuedRequest>) -> String {
    fixture.accept_pane_operations(
        vec![working_claude_agent(fixture)],
        PromptFixture::PromptedUnrecorded("w1:p3"),
    );
    seat(fixture, queue);
    append_main_rows(fixture, &[end_turn("before")]);
    report(fixture, claude_stop(fixture));
    report(fixture, claude_stop(fixture));
    assert_eq!(
        standings(fixture),
        vec![QueueDeliveryStanding::Uncertain(Some(
            Undecided::AbsenceUnprovable
        ))]
    );
    attempt_id(fixture)
}

/// The owner resolves a stuck hand-over as Delivered: the request leaves the
/// queue, the next is handed over at the next turn end, and the record names
/// the peer, the reason and the time.
#[test]
fn an_uncertain_hand_over_resolved_delivered_leaves_the_queue_with_its_record() {
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
        MetaResponse::QueueDeliveryResolved(QueueDeliveryResolved_Data {
            address: address(Layer::Secondary),
            queue_delivery_resolution: QueueDeliveryResolution {
                queue_attempt_id: attempt.clone(),
                queue_resolution_outcome: QueueResolutionOutcome::Delivered,
                queue_resolution_reason: "the pane shows it taken".into(),
            },
        })
    );
    assert_eq!(queue(&fixture), vec![second]);
    let read = standings(&fixture);
    let [QueueDeliveryStanding::Resolved(record)] = read.as_slice() else {
        panic!("the resolution is read: {read:?}")
    };
    assert_eq!(
        record.queue_resolution_outcome,
        QueueResolutionOutcome::Delivered
    );
    assert_eq!(record.resolver_identity, resolver());
    assert_eq!(record.queue_resolution_reason, "the pane shows it taken");
    assert!(record.unix_seconds > 1_700_000_000);

    report(&fixture, claude_stop(&fixture));
    assert_eq!(
        typed_count(&fixture, "first-request-body"),
        1,
        "never again"
    );
    assert_eq!(typed_count(&fixture, "second-request-body"), 1);
}

/// Resolved Absent keeps the request queued and the next turn end hands it
/// over in exactly one new attempt, which is itself reconciled and not
/// typed twice.
#[test]
fn an_uncertain_hand_over_resolved_absent_is_followed_by_exactly_one_new_attempt() {
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
        MetaResponse::QueueDeliveryResolved(_)
    ));
    assert_eq!(queue(&fixture), vec![first.clone(), second]);
    assert_eq!(
        typed_count(&fixture, "first-request-body"),
        1,
        "never automatic"
    );

    report(&fixture, claude_stop(&fixture));
    assert_eq!(typed_count(&fixture, "first-request-body"), 2);
    assert_eq!(typed_count(&fixture, "second-request-body"), 0);
    report(&fixture, claude_stop(&fixture));
    assert_eq!(
        typed_count(&fixture, "first-request-body"),
        2,
        "exactly one"
    );
    let standings = standings(&fixture);
    let [
        QueueDeliveryStanding::Resolved(record),
        QueueDeliveryStanding::Uncertain(_),
    ] = standings.as_slice()
    else {
        panic!("one resolved attempt and one new attempt: {standings:?}")
    };
    assert_eq!(
        record.queue_resolution_outcome,
        QueueResolutionOutcome::Absent
    );
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
    assert_eq!(queue(&fixture), vec![first]);

    resolve(&fixture, &attempt, QueueResolutionOutcome::Delivered, "x");
    assert_eq!(
        resolve(&fixture, &attempt, QueueResolutionOutcome::Absent, "again"),
        MetaResponse::ResolveQueueDeliveryRejected(ResolveQueueDeliveryRejected_Data::NotUncertain)
    );
}
