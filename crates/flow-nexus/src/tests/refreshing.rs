//! A refresh ordered from outside: `Refresh.Address` ends the flow the
//! metaflow is Awake as without giving it a turn. A Required metaflow, or
//! one holding a waking request, has its successor started and confirmed
//! before the ending flow is reaped; any other is left Asleep with that flow
//! in Past and its queue kept, and its successor runs on the next waking
//! request. While the refresh holds the metaflow, nothing reaches the ending
//! flow.

use super::refresh_and_compaction::{declare_seat, owe_staged_successor, staged_refresh_successor};
use super::waking::{
    CLAUDE_SEAT, ReadsFreshWorld, WakesFreshClaude, address, claude_agent, claude_seat,
    launched_claude_ended_by,
};
use super::{
    ControlsHerdrSnapshot, MarkedProcess, NexusFixture, PromptFixture, bind_existing_caller,
    codex_agent_in,
};
use crate::fixture_executable::{FixtureExecutable, InstallsScript};
use crate::sending::SendsRequests;
use crate::store::metaflow::{SeedsMetaflow, SleepsMetaflows};
use crate::store::successor::OwesSuccessors;
use crate::store::{FlowStore, ManagesMetaflowState, ReadsFlowMemory, ReadsFlowRows};
use crate::waking::{HoldsWakingMetaflows, SleepsAndWakesSeats};
use crate::{Dispatches, DispatchesSerially};
use flow_ethos::State;
use meta_signal_flow::{Availability, ConfigurationChange, ConfigureMetaflow};
use signal_flow::{
    Address, Delivery, FlowLifecycle, HerdrRouteSelection, Layer, Message, Metaflow, Origin, Query,
    Recipient, RefreshRejection, Refreshed, Refusal, Request, Response, SendRequest, SentBy,
    Unknown_Data,
};
use std::fs;
use std::time::Duration;

fn seat_address() -> Address {
    address(Layer::Secondary)
}

/// The seat's metaflow is declared OnDemand, with modules the fixture
/// context verifies, so a wake can compose its successor.
fn declare(fixture: &NexusFixture, at: Address) {
    fixture
        .nexus
        .store
        .configure_change(ConfigurationChange::Metaflows(vec![ConfigureMetaflow {
            address: at,
            system_modules: vec!["spirit-core".into(), "intent-flow".into()],
            user_modules: vec!["operation-main-flow".into()],
            flow_kind: signal_flow::FlowKind::Regular,
            tool_set: signal_flow::ToolSet::Default,
            availability: Availability::OnDemand,
            purpose: String::new(),
            records: vec![],
        }]))
        .expect("metaflow declared");
}

/// The Claude seat Awake at the Secondary address, its pane live in Herdr,
/// holding `queue`.
fn awake_seat(fixture: &NexusFixture, queue: Vec<Request>) {
    claude_seat(fixture, 40_000);
    fixture.nexus.store.seed_metaflow(Metaflow {
        address: seat_address(),
        state: State::Awake(CLAUDE_SEAT.into()),
        past: vec![],
        queue: queue
            .into_iter()
            .enumerate()
            .map(|(index, request)| signal_flow::QueueEntry {
                logical_delivery_id: format!("refreshing-test-{index}"),
                sent_by: SentBy::Unknown,
                delivery: Delivery::Queue,
                message: Message::Soft(request),
            })
            .collect(),
    });
}

fn stored(fixture: &NexusFixture) -> Metaflow {
    fixture
        .nexus
        .store
        .metaflow(&seat_address())
        .expect("metaflow readable")
        .expect("metaflow held")
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

fn requests(metaflow: &Metaflow) -> Vec<Request> {
    metaflow
        .queue
        .iter()
        .map(|entry| match &entry.message {
            Message::HardAbrupt(request)
            | Message::MiddleAbrupt(request)
            | Message::Soft(request) => request.clone(),
        })
        .collect()
}

/// An Awake metaflow is refreshed: its flow's pane is closed with nothing
/// typed into it, the flow is recorded Stopped and joins Past, the metaflow
/// is Asleep with its queue kept, and no successor is started.
#[test]
fn a_refresh_ends_the_awake_flow_without_a_turn_and_leaves_the_metaflow_asleep() {
    let fixture = NexusFixture::new();
    fixture.accept_pane_operations(
        vec![claude_agent(&fixture)],
        PromptFixture::Prompted("w1:p3"),
    );
    awake_seat(
        &fixture,
        vec![Request::Order("Kept for the next flow.".into())],
    );
    declare(&fixture, seat_address());

    assert_eq!(
        fixture
            .nexus
            .dispatch_serially(Query::Refresh(seat_address()), Origin::Unbound),
        Response::Refreshed(Refreshed {
            address: seat_address(),
            flow_id: CLAUDE_SEAT.into(),
        })
    );
    assert!(closed(&fixture), "the flow's pane is closed");
    assert!(
        fixture.typed().is_none(),
        "the ending flow is given no turn"
    );
    assert_eq!(lifecycle(&fixture), FlowLifecycle::Stopped);
    let metaflow = stored(&fixture);
    assert_eq!(metaflow.state, State::Asleep, "no successor is started");
    assert_eq!(metaflow.past, vec![CLAUDE_SEAT.to_owned()]);
    assert_eq!(
        requests(&metaflow),
        vec![Request::Order("Kept for the next flow.".into())],
        "the queue stays on the metaflow"
    );
    assert!(
        !fixture.nexus.waking.is_waking(&seat_address()),
        "the lock is released"
    );
}

/// A metaflow that is not Awake has no flow to end: an Asleep one, and one
/// declared but never held, are refused NotAwake; an Address neither held
/// nor declared is unknown.
#[test]
fn a_refresh_of_a_metaflow_not_awake_is_refused_not_awake() {
    let fixture = NexusFixture::new();
    fixture.accept_pane_operations(
        vec![claude_agent(&fixture)],
        PromptFixture::Prompted("w1:p3"),
    );
    claude_seat(&fixture, 40_000);
    let asleep = Metaflow {
        address: seat_address(),
        state: State::Asleep,
        past: vec![CLAUDE_SEAT.into()],
        queue: vec![],
    };
    fixture.nexus.store.seed_metaflow(asleep.clone());
    assert_eq!(
        fixture.nexus.dispatch(Query::Refresh(seat_address())),
        Response::RefreshRejected(RefreshRejection::NotAwake)
    );
    assert_eq!(stored(&fixture), asleep, "nothing is written");

    let declared = address(Layer::Tertiary);
    declare(&fixture, declared.clone());
    assert_eq!(
        fixture.nexus.dispatch(Query::Refresh(declared)),
        Response::RefreshRejected(RefreshRejection::NotAwake)
    );

    let unknown = address(Layer::Quaternary);
    assert_eq!(
        fixture.nexus.dispatch(Query::Refresh(unknown.clone())),
        Response::Refused(Refusal::Unknown(Unknown_Data::Address(unknown)))
    );
    assert!(!closed(&fixture));
}

/// While a refresh or a wake holds the metaflow, another refresh is refused
/// Locked and changes nothing; once the hold is released it runs.
#[test]
fn a_refresh_is_refused_locked_while_another_holds_the_metaflow() {
    let fixture = NexusFixture::new();
    fixture.accept_pane_operations(
        vec![claude_agent(&fixture)],
        PromptFixture::Prompted("w1:p3"),
    );
    awake_seat(&fixture, vec![]);

    let held = fixture
        .nexus
        .waking
        .hold(&seat_address())
        .expect("the first hold is taken");
    assert_eq!(
        fixture
            .nexus
            .dispatch_serially(Query::Refresh(seat_address()), Origin::Unbound),
        Response::RefreshRejected(RefreshRejection::Locked)
    );
    assert!(!closed(&fixture), "nothing is closed while the hold stands");
    assert_eq!(stored(&fixture).state, State::Awake(CLAUDE_SEAT.into()));
    assert_eq!(lifecycle(&fixture), FlowLifecycle::Active);

    drop(held);
    assert!(matches!(
        fixture
            .nexus
            .dispatch_serially(Query::Refresh(seat_address()), Origin::Unbound),
        Response::Refreshed(_)
    ));
    assert_eq!(stored(&fixture).state, State::Asleep);
}

/// A Send that arrives while the refresh is closing the ending flow's pane
/// is never typed into that pane: it waits until the metaflow is Asleep and
/// is queued there. The next waking request starts a fresh successor, which
/// holds the whole queue; the ending flow stays Stopped in Past.
#[test]
fn a_send_during_a_refresh_queues_for_the_successor_and_never_reaches_the_ending_flow() {
    const ENDING: &str = "abc123";
    let mut fixture = NexusFixture::new();
    bind_existing_caller(&fixture, "a001", "pane-1");
    // The ending flow was launched, so a wake composes its successor from
    // that launch.
    launched_claude_ended_by(&fixture, |_| true);
    fixture.nexus.store.seed_metaflow(Metaflow {
        address: seat_address(),
        state: State::Awake(ENDING.into()),
        past: vec![],
        queue: vec![],
    });
    let ending = fixture.nexus.store.flow_node(ENDING).unwrap().unwrap();
    let HerdrRouteSelection::Available(route) = &ending.herdr_route_selection else {
        panic!("the ending flow has its pane");
    };
    let log = fixture.accept_pane_operations(
        vec![
            codex_agent_in("pane-1", "terminal-pane-1"),
            serde_json::json!({
                "agent":"claude", "agent_status":"idle", "name":route.herdr_agent_name,
                "workspace_id":"w1", "pane_id":route.herdr_pane_id,
                "terminal_id":route.herdr_terminal_id,
                "agent_session":{"source":"herdr:claude","agent":"claude","kind":"id",
                    "value":ending.session_id},
            }),
        ],
        PromptFixture::Prompted("w1:p3"),
    );
    // The fixture Herdr's close of the ending pane announces itself, then
    // waits for the test to release it: the refresh holds the metaflow, its
    // flow not yet recorded ended, for as long as the test needs.
    let closing = fixture.directory.path().join("close-reached");
    let release = fixture.directory.path().join("close-released");
    let script = fs::read_to_string(&fixture.snapshot_program).expect("fixture Herdr");
    let pattern = "  \"pane close\"|\"pane send-keys\"|\"agent wait\")";
    assert!(script.contains(pattern), "{script}");
    FixtureExecutable {
        path: fixture.snapshot_program.clone(),
    }
    .install(&script.replacen(
        pattern,
        &format!(
            "  \"pane close\") : > '{closing}'; while [ ! -e '{release}' ]; do sleep 0.02; done; printf '%s\\n' \"$*\" >> '{log}' ;;\n{pattern}",
            closing = closing.display(),
            release = release.display(),
            log = log.display(),
        ),
        1,
    ));
    let caller = MarkedProcess::spawn("sleep", &["30"], "pane-1");
    let peer = FlowStore::process_identity_for_pid(caller.0.id()).unwrap();
    let request = Request::Order("For the successor.".into());
    let ended = |fixture: &NexusFixture| {
        fixture
            .nexus
            .store
            .flow_node(ENDING)
            .unwrap()
            .expect("the flow's row is kept")
            .flow_lifecycle
    };

    let (refreshed, sent) = std::thread::scope(|scope| {
        let refreshing = scope.spawn(|| {
            fixture
                .nexus
                .dispatch_serially(Query::Refresh(seat_address()), Origin::Unbound)
        });
        let deadline = std::time::Instant::now() + Duration::from_secs(20);
        while !closing.exists() {
            assert!(std::time::Instant::now() < deadline, "the close never ran");
            std::thread::sleep(Duration::from_millis(10));
        }
        // The refresh holds the metaflow and its flow is still Awake.
        assert!(fixture.nexus.waking.is_waking(&seat_address()));
        assert_eq!(stored(&fixture).state, State::Awake(ENDING.into()));
        let (answered, answer) = std::sync::mpsc::channel();
        let (nexus, peer, request) = (&fixture.nexus, &peer, &request);
        let sending = scope.spawn(move || {
            let answer = nexus.send_request(
                SendRequest {
                    recipient: Recipient::Address(seat_address()),
                    delivery: Delivery::Wake,
                    message: Message::Soft(request.clone()),
                },
                Some(peer),
            );
            answered.send(()).expect("the test waits for the answer");
            answer
        });
        assert!(
            answer.recv_timeout(Duration::from_millis(500)).is_err(),
            "the Send is not admitted while the refresh holds the metaflow"
        );
        assert!(fixture.typed().is_none(), "nothing reaches the ending flow");
        fs::write(&release, "").expect("close released");
        (
            refreshing.join().expect("refresh answered"),
            sending.join().expect("send answered"),
        )
    });

    assert_eq!(
        refreshed,
        Response::Refreshed(Refreshed {
            address: seat_address(),
            flow_id: ENDING.into(),
        })
    );
    assert!(
        matches!(sent.response, Response::SendAccepted(_)),
        "{:?}",
        sent.response
    );
    let pending = sent.pending.expect("the Send asks for a wake");
    assert_eq!(pending.delivery, Delivery::Wake);
    assert!(
        fs::read_to_string(&log)
            .unwrap_or_default()
            .contains("pane close w1:p3"),
        "the ending flow's pane is closed"
    );
    assert!(
        fixture.typed().is_none(),
        "nothing was typed into the ending flow"
    );
    assert_eq!(ended(&fixture), FlowLifecycle::Stopped);
    let asleep = stored(&fixture);
    assert_eq!(asleep.state, State::Asleep);
    assert_eq!(asleep.past, vec![ENDING.to_owned()]);
    assert_eq!(requests(&asleep), vec![request.clone()]);

    // The waking request the Send asked for starts the successor.
    let world = fixture.fresh_claude_world();
    fixture.nexus.wake_due(pending.address);
    let woken = stored(&fixture);
    let State::Awake(successor) = woken.state.clone() else {
        panic!("the successor is woken; Herdr calls: {}", world.calls());
    };
    assert_ne!(successor, ENDING, "a fresh flow, never the one that ended");
    assert_eq!(world.claude_starts(), 1, "{}", world.calls());
    assert_eq!(woken.past, vec![ENDING.to_owned()]);
    assert_eq!(
        requests(&woken),
        vec![request],
        "the queue waits on the metaflow for its successor"
    );
    assert!(
        fixture.typed().is_none(),
        "the ending flow was handed nothing"
    );
    assert_eq!(ended(&fixture), FlowLifecycle::Stopped);
}

/// The fixture Herdr's answer to `pane close` becomes `answer`, a shell
/// fragment run in its place.
fn close_answers(fixture: &NexusFixture, answer: &str) {
    let script = fs::read_to_string(&fixture.snapshot_program).expect("fixture Herdr");
    let pattern = "  \"pane close\"|\"pane send-keys\"|\"agent wait\")";
    assert!(script.contains(pattern), "{script}");
    FixtureExecutable {
        path: fixture.snapshot_program.clone(),
    }
    .install(&script.replacen(
        pattern,
        &format!("  \"pane close\") {answer} ;;\n{pattern}"),
        1,
    ));
}

/// A refresh whose close Herdr refuses answers CloseRefused, and one whose
/// Herdr cannot be read answers HerdrUnavailable: Refresh's own rejections.
/// Either leaves the flow Awake and live, and releases the lock.
#[test]
fn a_refused_close_or_an_unreadable_herdr_answers_refresh_s_own_rejection() {
    let fixture = NexusFixture::new();
    fixture.accept_pane_operations(
        vec![claude_agent(&fixture)],
        PromptFixture::Prompted("w1:p3"),
    );
    awake_seat(&fixture, vec![]);
    declare(&fixture, seat_address());
    close_answers(&fixture, "exit 1");
    assert_eq!(
        fixture.nexus.dispatch(Query::Refresh(seat_address())),
        Response::RefreshRejected(RefreshRejection::CloseRefused)
    );
    assert_eq!(stored(&fixture).state, State::Awake(CLAUDE_SEAT.into()));
    assert_eq!(lifecycle(&fixture), FlowLifecycle::Active);
    assert!(!fixture.nexus.waking.is_waking(&seat_address()));

    // Herdr lists its sessions, but the snapshot of the flow's own fails.
    let script = fs::read_to_string(&fixture.snapshot_program).expect("fixture Herdr");
    let cases = "case \"$3 $4\" in\n";
    assert!(script.contains(cases), "{script}");
    FixtureExecutable {
        path: fixture.snapshot_program.clone(),
    }
    .install(&script.replacen(
        cases,
        &format!("{cases}  \"api snapshot\") echo 'herdr: server unreachable' >&2; exit 1 ;;\n"),
        1,
    ));
    assert_eq!(
        fixture.nexus.dispatch(Query::Refresh(seat_address())),
        Response::RefreshRejected(RefreshRejection::HerdrUnavailable)
    );
    assert_eq!(stored(&fixture).state, State::Awake(CLAUDE_SEAT.into()));
    assert_eq!(lifecycle(&fixture), FlowLifecycle::Active);
    assert!(!fixture.nexus.waking.is_waking(&seat_address()));
}

/// The Herdr calls the replacement fixture logged that would type into a
/// pane.
fn typing_calls(fixture: &NexusFixture) -> Vec<String> {
    fs::read_to_string(fixture.directory.path().join("herdr-calls.log"))
        .unwrap_or_default()
        .lines()
        .filter(|call| {
            call.contains("agent prompt")
                || call.contains("send-keys")
                || call.contains("send-text")
        })
        .map(str::to_owned)
        .collect()
}

/// The Claude seat Awake at the Secondary address, its metaflow declared
/// as `availability`, holding one Order queued with `delivery`, and its
/// successor staged so a Replace of `LaunchProfile` resumes it and finds it
/// confirmed.
fn refreshable_with_successor(
    availability: Availability,
    delivery: Delivery,
) -> (NexusFixture, signal_flow::LaunchProfile) {
    let fixture = NexusFixture::new();
    claude_seat(&fixture, 40_000);
    let mut successor = staged_refresh_successor(&fixture);
    successor.stage_to_ambiguity(&fixture.nexus);
    successor.write_receipt();
    declare_seat(
        &fixture,
        availability,
        vec!["spirit-core".into(), "intent-flow".into()],
    );
    fixture.nexus.store.seed_metaflow(Metaflow {
        address: seat_address(),
        state: State::Awake(CLAUDE_SEAT.into()),
        past: vec![],
        queue: vec![signal_flow::QueueEntry {
            logical_delivery_id: "refreshing-successor-0".into(),
            sent_by: SentBy::Unknown,
            delivery,
            message: Message::Soft(Request::Order("For the successor.".into())),
        }],
    });
    owe_staged_successor(&fixture);
    fixture.herdr_for_replacement_showing(0, vec![claude_agent(&fixture)]);
    (fixture, successor.request())
}

/// A refresh of a Required metaflow, or of one whose queue holds a request
/// that asks for a wake, starts the successor in the same event: it is
/// started and its receipt confirmed before the ending flow is reaped, the
/// metaflow is Awake as it, the queue is kept for it, and the answer still
/// names the flow that ended. Nothing is typed into the ending flow.
#[test]
fn a_refresh_owed_a_successor_starts_it_before_the_ending_flow_is_reaped() {
    for (availability, delivery) in [
        (Availability::Required, Delivery::Queue),
        (Availability::OnDemand, Delivery::Wake),
    ] {
        let (fixture, _) = refreshable_with_successor(availability.clone(), delivery.clone());
        assert_eq!(
            fixture.nexus.dispatch(Query::Refresh(seat_address())),
            Response::Refreshed(Refreshed {
                address: seat_address(),
                flow_id: CLAUDE_SEAT.into(),
            }),
            "{availability:?} {delivery:?}"
        );
        let metaflow = stored(&fixture);
        assert_eq!(
            metaflow.state,
            State::Awake("908786".into()),
            "the successor is Awake: {availability:?} {delivery:?}"
        );
        assert_eq!(metaflow.past, vec![CLAUDE_SEAT.to_owned()]);
        assert_eq!(
            requests(&metaflow),
            vec![Request::Order("For the successor.".into())],
            "the queue waits for the successor"
        );
        assert_eq!(lifecycle(&fixture), FlowLifecycle::Stopped);
        assert!(
            fs::read_to_string(fixture.directory.path().join("herdr-calls.log"))
                .unwrap_or_default()
                .contains("pane close w1:p3"),
            "the ending flow's pane is closed"
        );
        assert_eq!(typing_calls(&fixture), Vec::<String>::new());
        assert_eq!(
            fixture
                .nexus
                .store
                .successor_intent(&seat_address())
                .unwrap(),
            None,
            "the successor owed is given"
        );
        assert!(!fixture.nexus.waking.is_waking(&seat_address()));
    }
}
