//! No pane outlives its flow: in the event that ends a flow — Stop, Retire,
//! a refresh, a replacement's reap, an exit — Flow closes the flow's
//! harness and its Herdr pane where they are still there. A pane already
//! gone is nothing to close, and a pane now running another session is
//! never closed.

use super::waking::{CLAUDE_SEAT, CLAUDE_SESSION, address, claude_agent, claude_seat};
use super::{ControlsHerdrSnapshot, NexusFixture, PromptFixture};
use crate::Dispatches;
use crate::reporting::RecordsReports;
use crate::store::ReadsFlowRows;
use crate::store::metaflow::{SeedsMetaflow, SleepsMetaflows};
use flow_ethos::State;
use signal_flow::{Event, FlowLifecycle, Layer, Metaflow};
use std::fs;

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
