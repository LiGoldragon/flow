//! No pane outlives its flow: in the event that ends a flow — Stop, Retire,
//! a refresh, a replacement's reap, an exit — Flow closes the flow's
//! harness and its Herdr pane where they are still there. A pane already
//! gone is nothing to close, and a pane now running another session is
//! never closed.

use super::waking::{CLAUDE_SEAT, CLAUDE_SESSION, address, claude_agent, claude_seat};
use super::{ControlsHerdrSnapshot, NexusFixture, PromptFixture};
use crate::Dispatches;
use crate::reporting::RecordsReports;
use crate::store::metaflow::{SeedsMetaflow, SleepsMetaflows};
use crate::store::{NamesLiveFlow, ReadsFlowRows};
use flow_ethos::State;
use signal_flow::{Event, FlowLifecycle, Layer, Metaflow, Report_Data, Response};
use std::fs;

/// The session the harness goes on in after /clear.
const CLEARED_SUCCESSOR_SESSION: &str = "c1a0de00-0000-4000-8000-000000000002";

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

fn session(fixture: &NexusFixture) -> String {
    fixture
        .nexus
        .store
        .flow_node(CLAUDE_SEAT)
        .unwrap()
        .expect("the flow's row is kept")
        .session_id
}

fn state(fixture: &NexusFixture) -> State {
    fixture
        .nexus
        .store
        .metaflow(&address(Layer::Secondary))
        .unwrap()
        .expect("metaflow held")
        .state
}

fn closed(fixture: &NexusFixture) -> bool {
    fs::read_to_string(fixture.directory.path().join("herdr-operations.log"))
        .unwrap_or_default()
        .contains("pane close w1:p3")
}

fn lifecycle(fixture: &NexusFixture) -> FlowLifecycle {
    fixture
        .nexus
        .store
        .flow_node(CLAUDE_SEAT)
        .unwrap()
        .expect("the flow's row is kept")
        .flow_lifecycle
}

/// The Claude seat Awake at the Secondary address, its pane live in Herdr
/// running its own session.
fn awake_seat(fixture: &NexusFixture) {
    fixture.accept_pane_operations(
        vec![claude_agent(fixture)],
        PromptFixture::Prompted("w1:p3"),
    );
    claude_seat(fixture, 40_000);
    fixture.nexus.store.seed_metaflow(Metaflow {
        address: address(Layer::Secondary),
        state: State::Awake(CLAUDE_SEAT.into()),
        past: vec![],
        queue: vec![],
    });
}

/// A retired flow's pane, still there running its own session, is closed
/// in the Retire: the flow is Retired, its row kept, and no harness of it
/// is left running.
#[test]
fn after_retire_the_flow_s_pane_is_closed() {
    let fixture = NexusFixture::new();
    awake_seat(&fixture);

    let meta_signal_flow::Response::FlowRetired(retired) = fixture
        .nexus
        .dispatch_meta(meta_signal_flow::Query::Retire(CLAUDE_SEAT.into()))
    else {
        panic!("a live flow is retired")
    };
    assert_eq!(retired.flow_lifecycle, FlowLifecycle::Retired);
    assert!(closed(&fixture), "the retired flow's pane is closed");
    assert!(fixture.typed().is_none(), "nothing is typed");
}

/// A flow whose own session end its hook reports is recorded Exited, and
/// its pane, still there, is closed in the same event.
#[test]
fn a_flow_whose_session_ended_has_its_pane_closed() {
    let fixture = NexusFixture::new();
    awake_seat(&fixture);

    fixture.nexus.after_report(
        &CLAUDE_SEAT.to_owned(),
        &Event::Ended(Some(CLAUDE_SESSION.into())),
    );

    assert_eq!(lifecycle(&fixture), FlowLifecycle::Exited);
    assert!(closed(&fixture), "the exited flow's pane is closed");
    assert_eq!(
        fixture
            .nexus
            .store
            .metaflow(&address(Layer::Secondary))
            .unwrap()
            .expect("metaflow held")
            .state,
        State::Asleep
    );
}

/// A flow whose pane Herdr shows gone is recorded Exited and nothing is
/// closed: there is no pane left.
#[test]
fn a_flow_whose_pane_is_gone_exits_with_nothing_to_close() {
    let fixture = NexusFixture::new();
    awake_seat(&fixture);
    fixture.accept_pane_operations(vec![], PromptFixture::Prompted("w1:p3"));

    fixture.nexus.after_report(
        &CLAUDE_SEAT.to_owned(),
        &Event::Ended(Some(CLAUDE_SESSION.into())),
    );

    assert_eq!(lifecycle(&fixture), FlowLifecycle::Exited);
    assert!(!closed(&fixture), "no pane is left to close");
}

/// A session /clear ended is a session change, not an exit: the flow is
/// not recorded Exited and its pane is not closed. The start that follows
/// rebinds the flow, its FlowId kept, to the session it names, so the end
/// of the old session is no longer its own and the end of the new one is.
#[test]
fn a_cleared_session_is_not_an_exit_and_the_next_start_rebinds_the_flow() {
    let fixture = NexusFixture::new();
    awake_seat(&fixture);
    let live = lifecycle(&fixture);
    assert!(live.is_live());

    report(&fixture, Event::Cleared(Some(CLAUDE_SESSION.into())));
    assert_eq!(lifecycle(&fixture), live, "a cleared session is no exit");
    assert!(!closed(&fixture), "the cleared flow's pane stays");
    assert_eq!(state(&fixture), State::Awake(CLAUDE_SEAT.into()));
    assert_eq!(session(&fixture), CLAUDE_SESSION);

    report(
        &fixture,
        Event::Started(Some(CLEARED_SUCCESSOR_SESSION.into())),
    );
    assert_eq!(session(&fixture), CLEARED_SUCCESSOR_SESSION);
    assert_eq!(lifecycle(&fixture), live);
    assert_eq!(state(&fixture), State::Awake(CLAUDE_SEAT.into()));

    report(&fixture, Event::Ended(Some(CLAUDE_SESSION.into())));
    assert_eq!(lifecycle(&fixture), live, "the old session is not its own");
    report(
        &fixture,
        Event::Ended(Some(CLEARED_SUCCESSOR_SESSION.into())),
    );
    assert_eq!(lifecycle(&fixture), FlowLifecycle::Exited);
}

/// A start that follows no clear of the flow's own session rebinds
/// nothing, whatever session it names.
#[test]
fn a_start_after_no_clear_of_its_own_session_rebinds_nothing() {
    let fixture = NexusFixture::new();
    awake_seat(&fixture);

    report(
        &fixture,
        Event::Started(Some(CLEARED_SUCCESSOR_SESSION.into())),
    );
    assert_eq!(session(&fixture), CLAUDE_SESSION);

    report(
        &fixture,
        Event::Cleared(Some("0ther000-0000-4000-8000-000000000009".into())),
    );
    report(
        &fixture,
        Event::Started(Some(CLEARED_SUCCESSOR_SESSION.into())),
    );
    assert_eq!(session(&fixture), CLAUDE_SESSION);
    assert!(lifecycle(&fixture).is_live());
}
