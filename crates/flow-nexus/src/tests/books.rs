//! Every flow answers the living's words with a book. A Claude seat's own
//! Stop, while its newest prompt that owes a book has no book published
//! after it, is recorded as `StopHeld` and answered `TurnHeld` with what is
//! owed; once a book is published the next Stop ends the turn. A flow's
//! message and a background task's notice owe no book; a queued hand-over
//! owes one when it hands over the living's psyche. A held Stop is no turn
//! end: it hands the seat no queued work.

use super::ControlsHerdrSnapshot;
use super::NexusFixture;
use super::PromptFixture;
use super::waking::{
    CLAUDE_SEAT, CLAUDE_SESSION, address, append_main_rows, claude_seat, claude_stop, end_turn,
    stored, working_claude_agent,
};
use crate::composition::RendersQueuedWork;
use crate::reporting::RecordsReports;
use crate::store::events::RecordsHarnessEvents;
use crate::store::metaflow::SeedsMetaflow;
use crate::waking::CorrelatesNativeEvents;
use flow_ethos::State;
use flow_native::DigestsPrompt;
use signal_flow::{
    Aspect, Event, Layer, Metaflow, NativePrompt, PromptOrigin, Publication, QueuedRequest,
    Report_Data, Request, Response, Said, SentBy,
};

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

/// Flow's answer to one report of the seat, as the hook receives it.
fn answer(fixture: &NexusFixture, event: Event) -> Response {
    fixture.nexus.report(Report_Data {
        flow_id: CLAUDE_SEAT.into(),
        event,
    })
}

/// A report answered, then acted on, as the Nexus serves it.
fn served(fixture: &NexusFixture, event: Event) -> Response {
    let response = answer(fixture, event.clone());
    if response == Response::Reported {
        fixture.nexus.after_report(&CLAUDE_SEAT.to_owned(), &event);
    }
    response
}

fn prompt(text: &str, origin: PromptOrigin) -> Event {
    Event::PromptSubmitted(NativePrompt {
        native_session_id_option: Some(CLAUDE_SESSION.into()),
        prompt_id_option: Some(format!("prompt-{}", &text.prompt_sha256()[..8])),
        prompt_sha256: text.prompt_sha256(),
        prompt_origin: origin,
    })
}

fn published(session: &str) -> Event {
    Event::Published(Publication {
        native_session_id_option: Some(session.into()),
        prompt_id_option: None,
        artifact_url: "https://claude.ai/code/artifact/4fcc3326".into(),
    })
}

fn held(owed: &str) -> Response {
    Response::TurnHeld(format!(
        "Make the book for the living's words before ending: {owed}"
    ))
}

fn last_event(fixture: &NexusFixture) -> Event {
    fixture
        .nexus
        .store
        .events(CLAUDE_SEAT)
        .unwrap()
        .unwrap()
        .pop()
        .unwrap()
}

fn psyche_from_core(verbatim: &str) -> QueuedRequest {
    QueuedRequest {
        sent_by: SentBy::Sender(signal_flow::Address {
            aspect: Aspect::Psyche,
            topic: "core".into(),
            layer: Layer::Secondary,
        }),
        request: Request::Psyche(Said {
            context: "the living, on books".into(),
            verbatim: verbatim.into(),
        }),
    }
}

fn order_from_core(body: &str) -> QueuedRequest {
    QueuedRequest {
        sent_by: SentBy::Sender(signal_flow::Address {
            aspect: Aspect::Psyche,
            topic: "core".into(),
            layer: Layer::Secondary,
        }),
        request: Request::Order(body.into()),
    }
}

/// The living typed to the seat: its Stop is held, recorded as StopHeld,
/// until it publishes a book; then the Stop ends the turn.
#[test]
fn typed_words_hold_the_stop_until_a_book_is_published() {
    let fixture = NexusFixture::new();
    seat(&fixture, vec![]);
    assert_eq!(
        answer(&fixture, prompt("explain the store", PromptOrigin::Typed)),
        Response::Reported
    );
    append_main_rows(&fixture, &[end_turn("answered-without-a-book")]);
    let stop = claude_stop(&fixture);
    assert_eq!(
        answer(&fixture, stop.clone()),
        held("the living's words typed to you")
    );
    let Event::Stopped(native) = stop.clone() else {
        unreachable!()
    };
    assert_eq!(last_event(&fixture), Event::StopHeld(native.clone()));
    assert!(!fixture.nexus.turn_end_recorded(CLAUDE_SEAT));

    // Held again while nothing is published.
    assert_eq!(
        answer(&fixture, stop.clone()),
        held("the living's words typed to you")
    );
    assert_eq!(
        answer(&fixture, published(CLAUDE_SESSION)),
        Response::Reported
    );
    assert_eq!(answer(&fixture, stop), Response::Reported);
    assert_eq!(last_event(&fixture), Event::Stopped(native));
    assert!(fixture.nexus.turn_end_recorded(CLAUDE_SEAT));
}

/// The living's words relayed as psyche owe a book as typed words do.
#[test]
fn psyche_holds_the_stop() {
    let fixture = NexusFixture::new();
    seat(&fixture, vec![]);
    answer(
        &fixture,
        prompt(
            "#psyche [\"Psyche core Secondary\" \"books\" \"make one\"]",
            PromptOrigin::Psyche,
        ),
    );
    assert_eq!(
        answer(&fixture, claude_stop(&fixture)),
        held("the living's words relayed to you as psyche")
    );
}

/// A flow's message and a background task's notice owe no book: the Stop
/// after them ends the turn.
#[test]
fn a_message_or_a_notice_does_not_hold_the_stop() {
    let fixture = NexusFixture::new();
    seat(&fixture, vec![]);
    answer(
        &fixture,
        prompt(
            "#msg [\"Mind flow Secondary\" \"built\"]",
            PromptOrigin::Message,
        ),
    );
    assert_eq!(answer(&fixture, claude_stop(&fixture)), Response::Reported);
    answer(
        &fixture,
        prompt(
            "<task-notification>\n<status>completed</status>\n</task-notification>",
            PromptOrigin::Notification,
        ),
    );
    assert_eq!(answer(&fixture, claude_stop(&fixture)), Response::Reported);
}

/// A message or a notice after the living's words does not discharge them,
/// and a book another session published is not this flow's.
#[test]
fn what_follows_the_living_s_words_does_not_discharge_them() {
    let fixture = NexusFixture::new();
    seat(&fixture, vec![]);
    answer(&fixture, prompt("write it up", PromptOrigin::Typed));
    answer(
        &fixture,
        prompt(
            "#msg [\"Mind flow Secondary\" \"x\"]",
            PromptOrigin::Message,
        ),
    );
    answer(
        &fixture,
        prompt(
            "<task-notification></task-notification>",
            PromptOrigin::Notification,
        ),
    );
    answer(&fixture, published("0ther000-0000-4000-8000-000000000009"));
    assert_eq!(
        answer(&fixture, claude_stop(&fixture)),
        held("the living's words typed to you")
    );
    // A book published while the message was handled discharges the
    // living's words before it.
    answer(&fixture, published(CLAUDE_SESSION));
    answer(
        &fixture,
        prompt(
            "#msg [\"Mind flow Secondary\" \"y\"]",
            PromptOrigin::Message,
        ),
    );
    assert_eq!(answer(&fixture, claude_stop(&fixture)), Response::Reported);
}

/// A Stop of another session (a harness the seat started under its
/// FLOW_ID) is never held.
#[test]
fn a_foreign_stop_is_not_held() {
    let fixture = NexusFixture::new();
    seat(&fixture, vec![]);
    answer(&fixture, prompt("write it up", PromptOrigin::Typed));
    assert_eq!(
        answer(
            &fixture,
            Event::Stopped(signal_flow::NativeStop {
                native_session_id_option: Some("0ther000-0000-4000-8000-000000000009".into()),
                transcript_path_option: Some("/elsewhere/other.jsonl".into()),
            })
        ),
        Response::Reported
    );
}

/// A held Stop hands the seat no queued work; the Stop after the book
/// does.
#[test]
fn a_held_stop_does_not_drain_the_queue() {
    let fixture = NexusFixture::new();
    fixture.accept_pane_operations(
        vec![working_claude_agent(&fixture)],
        PromptFixture::Prompted("w1:p3"),
    );
    let first = order_from_core("first-request-body");
    seat(&fixture, vec![first.clone()]);
    answer(&fixture, prompt("explain the queue", PromptOrigin::Typed));
    append_main_rows(&fixture, &[end_turn("answered-without-a-book")]);

    assert_eq!(
        served(&fixture, claude_stop(&fixture)),
        held("the living's words typed to you")
    );
    assert!(
        !fixture
            .typed()
            .unwrap_or_default()
            .contains("first-request-body")
    );
    assert_eq!(stored(&fixture, Layer::Secondary).queue, vec![first]);

    served(&fixture, published(CLAUDE_SESSION));
    assert_eq!(served(&fixture, claude_stop(&fixture)), Response::Reported);
    assert_eq!(
        fixture
            .typed()
            .unwrap_or_default()
            .matches("first-request-body")
            .count(),
        1
    );
}

/// A queued hand-over owes a book when it hands over the living's psyche,
/// and none when it hands over an order.
#[test]
fn a_queued_hand_over_of_psyche_holds_the_stop() {
    for (request, body, expected) in [
        (
            psyche_from_core("make a book of every answer"),
            "make a book of every answer",
            held("the living's psyche in the queued work handed to you"),
        ),
        (
            order_from_core("rebuild the store"),
            "rebuild the store",
            Response::Reported,
        ),
    ] {
        let fixture = NexusFixture::new();
        fixture.accept_pane_operations(
            vec![working_claude_agent(&fixture)],
            PromptFixture::PromptedUnrecorded("w1:p3"),
        );
        seat(&fixture, vec![request.clone()]);
        append_main_rows(&fixture, &[end_turn("before")]);
        // Nothing is owed yet: the turn ends and the request is handed over.
        assert_eq!(served(&fixture, claude_stop(&fixture)), Response::Reported);
        assert_eq!(fixture.typed().unwrap_or_default().matches(body).count(), 1);
        let text = [request].prompt_text();
        served(&fixture, prompt(&text, PromptOrigin::Queued));
        assert_eq!(answer(&fixture, claude_stop(&fixture)), expected);
    }
}
