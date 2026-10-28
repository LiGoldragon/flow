//! A refresh ordered from outside: `Refresh.Address` ends the flow the
//! metaflow is Awake as without giving it a turn. A Maintained metaflow, or
//! one holding a waking request, has its successor started and confirmed
//! before the ending flow is reaped; any other is left Asleep with that flow
//! in Past and its queue kept, and its successor runs on the next waking
//! request. A Launch addressed to an Awake metaflow is such a refresh. While
//! the metaflow is held, nothing reaches the ending flow and a Send queues.

use super::refresh_and_compaction::{
    SUCCESSOR_REQUEST, declare_seat, owe_staged_successor, staged_refresh_successor,
};
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
use crate::store::context::JudgesThreshold;
use crate::store::metaflow::{SeedsMetaflow, SleepsMetaflows};
use crate::store::successor::{OwesSuccessors, SuccessionCause, SuccessorIntent};
use crate::store::{FlowStore, ManagesMetaflowState, ReadsFlowMemory, ReadsFlowRows};
use crate::waking::{HoldsWakingMetaflows, SleepsAndWakesSeats};
use crate::{Dispatches, DispatchesSerially};
use flow_ethos::State;
use meta_signal_flow::{ConfigurationChange, ConfigureMetaflow, Continuity};
use signal_flow::{
    Address, Delivery, FlowLifecycle, HerdrRouteSelection, Layer, Message, Metaflow, Origin, Query,
    Recipient, RefreshRejection, Refreshed, Refusal, ReplaceRejection, Request, Response,
    SendRequest, SentBy, Unknown_Data,
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
            tool_set: signal_flow::ToolSet::Default,
            continuity: Continuity::OnDemand,
            purpose: String::new(),
            records: vec![],
            left_out: vec![],
            requires: vec![],
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
                sent_by: SentBy::Sender(seat_address()),
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
                &nexus.configured_use().unwrap(),
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
pub(super) fn close_answers(fixture: &NexusFixture, answer: &str) {
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
        Response::RefreshRejected(RefreshRejection::CloseRefused(
            signal_flow::PaneCloseFailure::HerdrCloseFailed,
        ))
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
/// as `continuity`, holding one Order queued with `delivery`, and its
/// successor staged so a Replace of `LaunchProfile` resumes it and finds it
/// confirmed.
fn refreshable_with_successor(
    continuity: Continuity,
    delivery: Delivery,
) -> (NexusFixture, signal_flow::LaunchProfile) {
    refreshable_with_staged_successor(continuity, delivery, true)
}

/// As [`refreshable_with_successor`], the successor's receipt written only
/// when `confirmed`: without it the successor's launch stays ambiguous.
fn refreshable_with_staged_successor(
    continuity: Continuity,
    delivery: Delivery,
    confirmed: bool,
) -> (NexusFixture, signal_flow::LaunchProfile) {
    let fixture = NexusFixture::new();
    claude_seat(&fixture, 40_000);
    let mut successor = staged_refresh_successor(&fixture);
    successor.stage_to_ambiguity(&fixture.nexus);
    if confirmed {
        successor.write_receipt();
    }
    declare_seat(
        &fixture,
        continuity,
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

/// A refresh of a Maintained metaflow, or of one whose queue holds a request
/// that asks for a wake, starts the successor in the same event: it is
/// started and its receipt confirmed before the ending flow is reaped, the
/// metaflow is Awake as it, the queue is kept for it, and the answer still
/// names the flow that ended. Nothing is typed into the ending flow.
#[test]
fn a_refresh_owed_a_successor_starts_it_before_the_ending_flow_is_reaped() {
    for (continuity, delivery) in [
        (Continuity::Maintained, Delivery::Queue),
        (Continuity::OnDemand, Delivery::Wake),
    ] {
        let (fixture, _) = refreshable_with_successor(continuity.clone(), delivery.clone());
        assert_eq!(
            fixture.nexus.dispatch(Query::Refresh(seat_address())),
            Response::Refreshed(Refreshed {
                address: seat_address(),
                flow_id: CLAUDE_SEAT.into(),
            }),
            "{continuity:?} {delivery:?}"
        );
        let metaflow = stored(&fixture);
        assert_eq!(
            metaflow.state,
            State::Awake("908786".into()),
            "the successor is Awake: {continuity:?} {delivery:?}"
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

/// A refresh whose owed successor's launch is still ambiguous when the
/// confirmation wait ends answers SuccessorUnconfirmed, never Locked: the
/// ending flow stays Awake, live and unreaped, the successor stays owed,
/// and the lock is released.
#[test]
fn a_refresh_whose_successor_stays_unconfirmed_answers_successor_unconfirmed() {
    let (fixture, _) =
        refreshable_with_staged_successor(Continuity::Maintained, Delivery::Queue, false);
    assert_eq!(
        fixture.nexus.dispatch(Query::Refresh(seat_address())),
        Response::RefreshRejected(RefreshRejection::SuccessorUnconfirmed)
    );
    assert_eq!(stored(&fixture).state, State::Awake(CLAUDE_SEAT.into()));
    assert_eq!(lifecycle(&fixture), FlowLifecycle::Active);
    assert!(
        !fs::read_to_string(fixture.directory.path().join("herdr-calls.log"))
            .unwrap_or_default()
            .contains("pane close w1:p3"),
        "the ending flow's pane stays open"
    );
    assert!(
        fixture
            .nexus
            .store
            .successor_intent(&seat_address())
            .unwrap()
            .is_some(),
        "the successor stays owed"
    );
    assert!(!fixture.nexus.waking.is_waking(&seat_address()));
}

/// A HardAbrupt Send that asks for a wake, arriving while a refresh, a
/// threshold refresh or a waking holds the metaflow, is queued on the
/// metaflow and answered accepted: never RecipientBusy, and never typed
/// into the pane of the flow the holder is ending or starting.
#[test]
fn an_abrupt_send_to_a_held_metaflow_is_queued_never_refused_busy() {
    let fixture = NexusFixture::new();
    bind_existing_caller(&fixture, "a001", "pane-1");
    fixture.accept_pane_operations(
        vec![
            codex_agent_in("pane-1", "terminal-pane-1"),
            claude_agent(&fixture),
        ],
        PromptFixture::Prompted("w1:p3"),
    );
    awake_seat(&fixture, vec![]);
    declare(&fixture, seat_address());
    let caller = MarkedProcess::spawn("sleep", &["30"], "pane-1");
    let peer = FlowStore::process_identity_for_pid(caller.0.id()).unwrap();
    let request = Request::Order("Arrived under the hold.".into());

    let held = fixture
        .nexus
        .waking
        .hold(&seat_address())
        .expect("the holder takes the metaflow");
    let sent = fixture.nexus.send_request(
        &fixture.nexus.configured_use().unwrap(),
        SendRequest {
            recipient: Recipient::Address(seat_address()),
            delivery: Delivery::Wake,
            message: Message::HardAbrupt(request.clone()),
        },
        Some(&peer),
    );
    assert!(
        matches!(sent.response, Response::SendAccepted(_)),
        "{:?}",
        sent.response
    );
    let pending = sent.pending.expect("the queued Send asks for a wake");
    assert_eq!(pending.delivery, Delivery::Wake);
    let metaflow = stored(&fixture);
    assert_eq!(requests(&metaflow), vec![request]);
    assert!(matches!(metaflow.queue[0].message, Message::HardAbrupt(_)));
    assert!(fixture.typed().is_none(), "nothing is typed under the hold");
    // The holder's own drain finds the metaflow held: nothing is typed.
    fixture.nexus.wake_due(pending.address);
    assert!(fixture.typed().is_none());
    drop(held);
}

/// A HardAbrupt or MiddleAbrupt Send that asks for a wake, to a seat whose
/// queue is not empty, whose delivery is outstanding, or which is past its
/// context threshold, is queued and answered accepted: a Send is never
/// refused for the seat's state.
#[test]
fn an_abrupt_wake_send_to_a_busy_seat_is_queued_never_refused() {
    use crate::store::context::JudgesThreshold as _;
    use crate::store::context::RecordsContextSize as _;
    use crate::store::queue_delivery::{
        AccountsQueueDelivery, NativeHistoryAnchor, PrepareQueueDelivery,
    };
    use signal_flow::{DeliveryState, HarnessKind};
    #[derive(Clone, Copy, Debug)]
    enum Busy {
        QueueNotEmpty,
        DeliveryOutstanding,
        PastThreshold,
    }
    for busy in [
        Busy::QueueNotEmpty,
        Busy::DeliveryOutstanding,
        Busy::PastThreshold,
    ] {
        for abrupt in [
            Message::HardAbrupt as fn(Request) -> Message,
            Message::MiddleAbrupt,
        ] {
            let fixture = NexusFixture::new();
            bind_existing_caller(&fixture, "a001", "pane-1");
            fixture.accept_pane_operations(
                vec![
                    codex_agent_in("pane-1", "terminal-pane-1"),
                    claude_agent(&fixture),
                ],
                PromptFixture::Prompted("w1:p3"),
            );
            awake_seat(
                &fixture,
                match busy {
                    Busy::PastThreshold => vec![],
                    _ => vec![Request::Order("Already waiting.".into())],
                },
            );
            declare(&fixture, seat_address());
            match busy {
                Busy::QueueNotEmpty => {}
                Busy::DeliveryOutstanding => {
                    let PrepareQueueDelivery::Prepared(_) = fixture
                        .nexus
                        .store
                        .prepare_oldest_delivery(
                            &seat_address(),
                            CLAUDE_SEAT,
                            "c1a0de00-0000-4000-8000-000000000001",
                            HarnessKind::Claude,
                            NativeHistoryAnchor::CodexTurns(None),
                        )
                        .expect("prepared")
                    else {
                        panic!("the oldest request is prepared")
                    };
                    assert!(
                        fixture
                            .nexus
                            .store
                            .outstanding_delivery(&seat_address())
                            .unwrap()
                            .is_some()
                    );
                }
                Busy::PastThreshold => {
                    super::refresh_and_compaction::seat_refreshing_at(&fixture, 200_000);
                    fixture
                        .nexus
                        .store
                        .record_context_size(CLAUDE_SEAT, 200_000)
                        .unwrap();
                    assert!(fixture.nexus.store.past_threshold(CLAUDE_SEAT).unwrap());
                }
            }
            let queued_before = stored(&fixture).queue.len();
            let caller = MarkedProcess::spawn("sleep", &["30"], "pane-1");
            let peer = FlowStore::process_identity_for_pid(caller.0.id()).unwrap();
            let sent = fixture.nexus.send_request(
                &fixture.nexus.configured_use().unwrap(),
                SendRequest {
                    recipient: Recipient::Address(seat_address()),
                    delivery: Delivery::Wake,
                    message: abrupt(Request::Order("Arrived while busy.".into())),
                },
                Some(&peer),
            );
            let Response::SendAccepted(acceptance) = &sent.response else {
                panic!("{busy:?}: {:?}", sent.response)
            };
            assert_eq!(acceptance.delivery_state, DeliveryState::Queued, "{busy:?}");
            assert_eq!(stored(&fixture).queue.len(), queued_before + 1, "{busy:?}");
        }
    }
}

/// A Primary accepts only its own Secondary. A current admission always has
/// an authenticated Sender, so neither a Primary nor another topic's
/// Secondary can enter its queue unsigned.
#[test]
fn a_send_to_a_primary_refuses_every_sender_but_its_own_secondary() {
    use crate::store::logical_delivery::RecordsLogicalDeliveries as _;
    let fixture = NexusFixture::new();
    let primary = |topic: &str| Address {
        aspect: signal_flow::Aspect::Mind,
        topic: topic.into(),
        layer: Layer::Primary,
    };
    let target = primary("flow");
    fixture.nexus.store.seed_metaflow(Metaflow {
        address: target.clone(),
        state: State::Asleep,
        past: vec![],
        queue: vec![],
    });
    for sender in [primary("other"), seat_address()] {
        for request in [
            Request::Order("An order.".into()),
            Request::Result("A result.".into()),
        ] {
            let response = fixture
                .nexus
                .store
                .accept_logical_delivery(
                    &target,
                    meta_signal_flow::Sender {
                        address: sender.clone(),
                        flow_id: "authenticated-sender".into(),
                    },
                    Delivery::Queue,
                    Message::Soft(request),
                    None,
                )
                .expect("admission is decided before mutating the queue");
            if sender == seat_address() {
                assert!(
                    matches!(response, Response::SendAccepted(_)),
                    "{response:?}"
                );
            } else {
                assert!(
                    matches!(
                        response,
                        Response::Refused(Refusal::PrimaryRequiresOwnSecondary(_))
                    ),
                    "{sender:?}: {response:?}"
                );
            }
        }
    }
    assert_eq!(
        fixture
            .nexus
            .store
            .metaflow(&target)
            .unwrap()
            .unwrap()
            .queue
            .len(),
        2,
        "only the own Secondary's two requests enter"
    );
}

/// A Launch addressed to a metaflow Awake as a live flow, from any caller,
/// names the Replace of that flow: no AddressAwake, and the flow it is
/// Awake as is the predecessor.
#[test]
fn a_launch_to_an_awake_metaflow_names_the_replace_of_its_flow() {
    use crate::sending::LaunchesDeclared;
    let fixture = NexusFixture::new();
    declare_seat(
        &fixture,
        Continuity::OnDemand,
        vec!["spirit-core".into(), "intent-flow".into()],
    );
    awake_seat(&fixture, vec![]);
    let Ok(Query::Replace(profile)) = fixture.nexus.declared_launch(
        &fixture.nexus.configured_use().unwrap(),
        signal_flow::Launch_Data {
            address: seat_address(),
            remembering: signal_flow::Remembering::Predecessors(1),
            instruction_prompt: "Take up the work.".into(),
        },
        None,
    ) else {
        panic!("a Launch to an Awake metaflow is a Replace");
    };
    assert_eq!(profile.flow_id_option, Some(CLAUDE_SEAT.to_owned()));
    assert_eq!(profile.address, seat_address());
    assert_eq!(profile.instruction_prompt, "Take up the work.");
}

/// A Replace of an Awake metaflow's flow, as a Launch names it, runs under
/// the metaflow's lock: refused Locked while another holds it; while it
/// reaps, the metaflow is held, and a drain that asks meanwhile types
/// nothing into the predecessor's pane; the queue is kept for the
/// successor.
#[test]
fn a_launch_ordered_replace_holds_the_metaflow_and_types_nothing_into_its_predecessor() {
    let (fixture, profile) = refreshable_with_successor(Continuity::OnDemand, Delivery::Queue);

    let held = fixture
        .nexus
        .waking
        .hold(&seat_address())
        .expect("another holds the metaflow");
    assert_eq!(
        fixture.nexus.dispatch(Query::Replace(profile.clone())),
        Response::ReplaceRejected(ReplaceRejection::Locked)
    );
    assert_eq!(lifecycle(&fixture), FlowLifecycle::Active);
    drop(held);

    // The predecessor's close announces itself, then waits for the test.
    let closing = fixture.directory.path().join("close-reached");
    let release = fixture.directory.path().join("close-released");
    let script = fs::read_to_string(&fixture.snapshot_program).expect("fixture Herdr");
    let pattern = "  *\" pane close \"*) exit 0 ;;";
    assert!(script.contains(pattern), "{script}");
    FixtureExecutable {
        path: fixture.snapshot_program.clone(),
    }
    .install(&script.replacen(
        pattern,
        &format!(
            "  *\" pane close \"*) : > '{closing}'; while [ ! -e '{release}' ]; do sleep 0.02; done; exit 0 ;;",
            closing = closing.display(),
            release = release.display(),
        ),
        1,
    ));

    let replaced = std::thread::scope(|scope| {
        let replacing = scope.spawn(|| fixture.nexus.dispatch(Query::Replace(profile.clone())));
        let deadline = std::time::Instant::now() + Duration::from_secs(20);
        while !closing.exists() {
            assert!(std::time::Instant::now() < deadline, "the reap never ran");
            std::thread::sleep(Duration::from_millis(10));
        }
        assert!(
            fixture.nexus.waking.is_waking(&seat_address()),
            "the replacement holds the metaflow"
        );
        let draining = scope.spawn(|| fixture.nexus.drain_awake(&seat_address()));
        std::thread::sleep(Duration::from_millis(200));
        assert_eq!(typing_calls(&fixture), Vec::<String>::new());
        fs::write(&release, "").expect("close released");
        draining.join().expect("drain asked");
        replacing.join().expect("replace answered")
    });

    assert!(
        matches!(&replaced, Response::Replaced(replaced) if replaced.flow_id == CLAUDE_SEAT),
        "{replaced:?}"
    );
    assert_eq!(lifecycle(&fixture), FlowLifecycle::Stopped);
    let metaflow = stored(&fixture);
    assert_eq!(metaflow.state, State::Awake("908786".into()));
    assert_eq!(
        requests(&metaflow),
        vec![Request::Order("For the successor.".into())]
    );
    assert_eq!(
        typing_calls(&fixture),
        Vec::<String>::new(),
        "nothing is typed into the predecessor"
    );
    assert!(!fixture.nexus.waking.is_waking(&seat_address()));
}

/// A refresh owed a successor that cannot start answers Refresh's own
/// rejection, never the replacement's answer: Undeclared when no model is
/// configured for the successor, SuccessorRefused with the Replace's
/// rejection when its launch is refused. Either leaves the flow Awake and
/// live, owes nothing, and releases the lock.
#[test]
fn a_successor_that_cannot_start_answers_refresh_s_own_rejection() {
    let fixture = NexusFixture::new();
    fixture.accept_pane_operations(
        vec![claude_agent(&fixture)],
        PromptFixture::Prompted("w1:p3"),
    );
    awake_seat(&fixture, vec![]);
    let mut waking = stored(&fixture);
    waking.queue = vec![signal_flow::QueueEntry {
        logical_delivery_id: "refreshing-wake-0".into(),
        sent_by: SentBy::Unknown,
        delivery: Delivery::Wake,
        message: Message::Soft(Request::Order("Wake the successor.".into())),
    }];
    fixture.nexus.store.seed_metaflow(waking);
    // Declared, with no model configured for its layer and aspect.
    declare(&fixture, seat_address());
    assert_eq!(
        fixture.nexus.dispatch(Query::Refresh(seat_address())),
        Response::RefreshRejected(RefreshRejection::Undeclared)
    );
    assert_eq!(stored(&fixture).state, State::Awake(CLAUDE_SEAT.into()));
    assert_eq!(lifecycle(&fixture), FlowLifecycle::Active);
    assert!(!closed(&fixture));
    assert!(!fixture.nexus.waking.is_waking(&seat_address()));

    // Maintained, its declared module absent from the verified context: the
    // successor's launch is refused before any native seat starts.
    declare_seat(
        &fixture,
        Continuity::Maintained,
        vec!["missing-module".into()],
    );
    let refused = fixture.nexus.dispatch(Query::Refresh(seat_address()));
    assert!(
        matches!(
            &refused,
            Response::RefreshRejected(RefreshRejection::SuccessorRefused(
                ReplaceRejection::LaunchRefused(_)
            ))
        ),
        "{refused:?}"
    );
    assert_eq!(stored(&fixture).state, State::Awake(CLAUDE_SEAT.into()));
    assert_eq!(lifecycle(&fixture), FlowLifecycle::Active);
    assert!(!closed(&fixture));
    assert_eq!(
        fixture
            .nexus
            .store
            .successor_intent(&seat_address())
            .unwrap(),
        None,
        "a refused successor is not owed"
    );
    assert!(!fixture.nexus.waking.is_waking(&seat_address()));
}

/// A Launch-ordered Replace owes its successor until it is confirmed: the
/// intent is recorded before the launch and given with the confirmation.
#[test]
fn a_launch_ordered_replace_owes_its_successor_until_it_is_confirmed() {
    let (fixture, profile) = refreshable_with_successor(Continuity::OnDemand, Delivery::Queue);
    fixture
        .nexus
        .store
        .forget_successor(&seat_address())
        .unwrap();
    let replaced = fixture.nexus.dispatch(Query::Replace(profile));
    assert!(
        matches!(&replaced, Response::Replaced(replaced) if replaced.flow_id == CLAUDE_SEAT),
        "{replaced:?}"
    );
    assert_eq!(
        fixture
            .nexus
            .store
            .successor_intent(&seat_address())
            .unwrap(),
        None,
        "the successor owed is given"
    );
}

/// A successor an earlier Nexus owed after a Refresh ordered from outside,
/// or after a Replace, and did not see confirmed is started when the Nexus
/// starts, though the predecessor is nowhere near a refresh threshold: it
/// is confirmed before the predecessor is reaped, the metaflow is Awake as
/// it, and nothing is typed into the predecessor.
#[test]
fn a_nexus_start_starts_a_successor_owed_after_a_refresh_or_a_replace() {
    for cause in [SuccessionCause::Ordered, SuccessionCause::Replace] {
        let (fixture, _) = refreshable_with_successor(Continuity::Maintained, Delivery::Queue);
        fixture
            .nexus
            .store
            .owe_successor(SuccessorIntent {
                address: seat_address(),
                predecessor: CLAUDE_SEAT.into(),
                cause: cause.clone(),
                launch_request_id: SUCCESSOR_REQUEST.into(),
            })
            .expect("successor owed");
        assert!(
            !fixture.nexus.store.past_threshold(CLAUDE_SEAT).unwrap(),
            "the predecessor is not past any threshold"
        );

        fixture.nexus.recover_wakes();

        let metaflow = stored(&fixture);
        assert_eq!(
            metaflow.state,
            State::Awake("908786".into()),
            "the successor is Awake: {cause:?}"
        );
        assert_eq!(metaflow.past, vec![CLAUDE_SEAT.to_owned()], "{cause:?}");
        assert_eq!(lifecycle(&fixture), FlowLifecycle::Stopped, "{cause:?}");
        assert!(
            fs::read_to_string(fixture.directory.path().join("herdr-calls.log"))
                .unwrap_or_default()
                .contains("pane close w1:p3"),
            "the predecessor's pane is closed: {cause:?}"
        );
        assert!(
            typing_calls(&fixture)
                .iter()
                .all(|call| !call.contains("w1:p3")),
            "nothing is typed into the predecessor: {cause:?}"
        );
        assert_eq!(
            fixture
                .nexus
                .store
                .successor_intent(&seat_address())
                .unwrap(),
            None,
            "the successor owed is given: {cause:?}"
        );
        assert!(!fixture.nexus.waking.is_waking(&seat_address()));
    }
}
