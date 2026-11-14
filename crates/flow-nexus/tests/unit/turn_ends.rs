//! Observe.TurnEnds: the subscriber receives the last turn end of each
//! harness on open, then one TurnEnded for each turn end, a Claude seat's
//! Stop in its own session or a Codex thread's turn completed, and nothing
//! for any other event.

use super::waking::{
    CLAUDE_SEAT, address, background_task, claude_agent, claude_seat, claude_stop, claude_stop_with,
};
use super::{ControlsHerdrSnapshot, NexusFixture, PromptFixture};
use crate::Dispatches;
use crate::codex_turns::{SettlesRestingCodexSeats, ThreadStatus, TurnEvent};
use crate::reporting::RecordsReports;
use crate::store::metaflow::SeedsMetaflow;
use crate::store::{FlowStore, ReadsFlowRows};
use crate::turn_ends::ObservesTurnEnds;
use flow_ethos::State;
use signal_flow::{
    BackgroundTaskKind, Event, HarnessKind, HerdrRouteSelection, Metaflow, Model, NativeStop,
    ObserveSelection, Query, Report_Data, Response, TurnEnd,
};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;

const CODEX_SEAT: &str = "908786";
const CODEX_THREAD: &str = "01a0b22c-e24f-7452-9940-64490878680f";

/// A Claude seat Awake at { Mind flow Sol } and a Codex seat Awake at
/// { Mind flow Luna }, nothing queued for either.
fn two_seats(fixture: &NexusFixture) {
    use crate::store::RegistersFlowIdentity;
    fixture.accept_pane_operations(
        vec![claude_agent(fixture)],
        PromptFixture::Prompted("w1:p3"),
    );
    claude_seat(fixture, 40_000);
    fixture.nexus.store.seed_metaflow(Metaflow {
        address: address(Model::Sol),
        state: State::Awake(flow_ethos::FlowId(CLAUDE_SEAT.into())),
        past: flow_ethos::Past(vec![]),
        queue: flow_ethos::Queue(vec![]),
    });
    let mut codex = fixture.node();
    assert_eq!(codex.flow_id.0, CODEX_SEAT);
    assert_eq!(codex.session_id.0, CODEX_THREAD);
    assert_eq!(codex.harness_kind, HarnessKind::Codex);
    if let HerdrRouteSelection::Available(route) = &mut codex.herdr_route_selection {
        route.herdr_pane_id = signal_flow::HerdrPaneId("w1:p4".into());
        route.herdr_terminal_id = signal_flow::HerdrTerminalId("terminal-codex".into());
    }
    assert!(matches!(
        fixture.nexus.store.register_flow(codex),
        Ok(crate::store::FlowRegistration::Registered(_))
    ));
    fixture.nexus.store.seed_metaflow(Metaflow {
        address: address(Model::Luna),
        state: State::Awake(flow_ethos::FlowId(CODEX_SEAT.into())),
        past: flow_ethos::Past(vec![]),
        queue: flow_ethos::Queue(vec![]),
    });
}

/// A report of the Claude seat as its hook sends it: recorded, answered,
/// then acted on.
fn report(fixture: &NexusFixture, event: Event) {
    assert_eq!(
        fixture.nexus.report(Report_Data {
            flow_id: flow_ethos::FlowId(CLAUDE_SEAT.into()),
            process_option: None,
            event: event.clone(),
        }),
        Response::Reported
    );
    fixture.nexus.after_report(
        &flow_ethos::FlowId(CLAUDE_SEAT.to_owned().to_owned()),
        &event,
    );
}

fn codex(fixture: &NexusFixture, events: Vec<TurnEvent>) {
    fixture.nexus.codex_events(CODEX_THREAD, events);
}

/// The turn end `frame` carries, checked to be the one of `flow_id`, at
/// `model`'s Address, on `harness`.
fn turn_end(frame: Response, flow_id: &str, model: Model, harness: HarnessKind) -> TurnEnd {
    let Response::TurnEnded(turn_end) = frame else {
        panic!("a turn end is sent as TurnEnded, not {frame:?}");
    };
    assert_eq!(turn_end.flow_id.0, flow_id);
    assert_eq!(turn_end.address, address(model));
    assert_eq!(turn_end.harness_kind, harness);
    assert!(turn_end.observed_at > signal_flow::ObservedAt(0));
    turn_end
}

/// The observer's peer going: dropped, it ends the observer, so a failed
/// assertion ends the test rather than leaving the observer waiting.
struct ObserverGoes<'a> {
    disconnected: &'a AtomicBool,
    fixture: &'a NexusFixture,
}

impl Drop for ObserverGoes<'_> {
    fn drop(&mut self) {
        self.disconnected.store(true, Ordering::Release);
        crate::store::AnnouncesLaunchChanges::announce(&self.fixture.nexus.store.launch_changes);
    }
}

/// The state a new subscription opens on.
fn opening(fixture: &NexusFixture) -> Response {
    let mut observed = Vec::new();
    let ended = fixture
        .nexus
        .observe_turn_ends(&AtomicBool::new(false), &mut |frame| {
            observed.push(frame.clone());
            Err("the observer has what it came for".into())
        });
    assert!(ended.is_err());
    assert_eq!(observed.len(), 1);
    observed.remove(0)
}

/// The subscriber gets the state on open, then each turn end once, in
/// order, and nothing for a tool use, a Stop in another session, a turn
/// started, a completion seen again or an idle status. Each non-event is
/// followed by a turn end of the other harness, so a frame it sent would be
/// the next one read.
#[test]
fn a_turn_end_subscriber_receives_the_state_on_open_then_each_turn_end_and_nothing_else() {
    let fixture = NexusFixture::new();
    two_seats(&fixture);
    let disconnected = AtomicBool::new(false);
    let (frames, received) = mpsc::channel();
    std::thread::scope(|scope| {
        let observer = scope.spawn(|| {
            fixture
                .nexus
                .observe_turn_ends(&disconnected, &mut |frame| {
                    frames
                        .send(frame.clone())
                        .map_err(|error| error.to_string())
                })
        });
        let goes = ObserverGoes {
            disconnected: &disconnected,
            fixture: &fixture,
        };
        // A frame that never comes fails the test at this bound rather than
        // hanging it; every frame here is sent as its event is handled.
        let next = || {
            received
                .recv_timeout(std::time::Duration::from_secs(30))
                .expect("the observer sends")
        };

        assert_eq!(next(), Response::TurnEndsObserved(vec![]));

        report(&fixture, Event::ToolUsed("Bash".into()));
        report(
            &fixture,
            Event::Stopped(NativeStop {
                native_session_id_option: Some(signal_flow::NativeSessionId(
                    "0ther000-0000-4000-8000-000000000009".into(),
                )),
                transcript_path_option: Some(signal_flow::TranscriptPath(
                    "/elsewhere/other.jsonl".into(),
                )),
                last_assistant_message_option: signal_flow::LastAssistantMessageOption(None),
                background_work: signal_flow::BackgroundWork(vec![]),
            }),
        );
        codex(&fixture, vec![TurnEvent::Started("turn-1".into())]);
        codex(&fixture, vec![TurnEvent::Completed("turn-1".into())]);
        turn_end(next(), CODEX_SEAT, Model::Luna, HarnessKind::Codex);

        codex(&fixture, vec![TurnEvent::Completed("turn-1".into())]);
        codex(&fixture, vec![TurnEvent::Status(ThreadStatus::Idle)]);
        report(&fixture, claude_stop(&fixture));
        turn_end(next(), CLAUDE_SEAT, Model::Sol, HarnessKind::Claude);

        // A Stop with work in flight ends a turn as any other.
        report(
            &fixture,
            claude_stop_with(
                &fixture,
                vec![background_task("m1", BackgroundTaskKind::Monitor)],
            ),
        );
        let claude = turn_end(next(), CLAUDE_SEAT, Model::Sol, HarnessKind::Claude);

        codex(
            &fixture,
            vec![
                TurnEvent::Started("turn-2".into()),
                TurnEvent::Completed("turn-2".into()),
            ],
        );
        let codex_end = turn_end(next(), CODEX_SEAT, Model::Luna, HarnessKind::Codex);

        drop(goes);
        assert!(observer.join().expect("the observer ends").is_err());
        assert_eq!(
            received.try_iter().collect::<Vec<_>>(),
            vec![],
            "nothing more was sent"
        );

        // A new subscriber opens on the last turn end of each harness,
        // Claude before Codex; a direct Observe answers the same.
        let last = Response::TurnEndsObserved(vec![claude, codex_end]);
        assert_eq!(opening(&fixture), last);
        assert_eq!(
            fixture
                .nexus
                .dispatch(Query::Observe(ObserveSelection::TurnEnds)),
            last
        );
    });
}

/// The last turn end of each harness is kept in Memory: a store opened again
/// opens a subscription on it.
#[test]
fn the_state_a_turn_end_subscription_opens_on_outlives_a_reopened_store() {
    use crate::store::{OpensFlowStore, SetsNativeOwnerEnvironmentForTests};
    let mut fixture = NexusFixture::new();
    two_seats(&fixture);
    assert_eq!(opening(&fixture), Response::TurnEndsObserved(vec![]));
    report(&fixture, claude_stop(&fixture));
    let Response::TurnEndsObserved(before) = opening(&fixture) else {
        panic!("the opening frame is TurnEndsObserved");
    };
    assert_eq!(before.len(), 1);
    assert_eq!(before[0].flow_id.0, CLAUDE_SEAT);

    let placeholder = FlowStore::open(&fixture.directory.path().join("placeholder.sema"))
        .expect("placeholder store");
    drop(std::mem::replace(
        &mut fixture.nexus.store,
        placeholder.into(),
    ));
    let mut reopened =
        FlowStore::open(&fixture.directory.path().join("flow.sema")).expect("store reopens");
    reopened.set_native_owner_environment_for_tests(crate::store::NativeOwnerEnvironment {
        host_id: "isolated-test-host".into(),
        boot_id: "isolated-test-boot".into(),
    });
    fixture.nexus.store = reopened.into();
    assert!(
        fixture
            .nexus
            .store
            .flow_node(CLAUDE_SEAT)
            .unwrap()
            .is_some(),
        "the reopened store is the fixture's"
    );
    assert_eq!(opening(&fixture), Response::TurnEndsObserved(before));
}
