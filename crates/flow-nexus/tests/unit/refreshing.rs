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
use super::send_tool::accepting_codex_endpoint;
use super::waking::{
    CLAUDE_SEAT, ReadsFreshWorld, WakesFreshClaude, address, claude_agent, claude_seat,
    launched_claude_ended_by,
};
use super::{
    ControlsHerdrSnapshot, MarkedProcess, NexusFixture, PromptFixture, bind_existing_caller,
    codex_agent_in,
};
use crate::event_wait::AwaitsPath;
use crate::fixture_executable::{FixtureExecutable, HoldsLatch, InstallsScript, Latch};
use crate::generated::operation::{Operation, Outcome};
use crate::handover_reminding::RemindsHandoverCallers;
use crate::performing::Performs;
use crate::sending::{IdentifiesSender, SendsRequests};
use crate::store::AnnouncesLaunchChanges as _;
use crate::store::SeedsRole;
use crate::store::context::{JudgesThreshold, RecordsContextSize};
use crate::store::handover::HoldsHandovers as _;
use crate::store::handover_reminders::SchedulesHandoverReminders as _;
use crate::store::logical_delivery::RecordsLogicalDeliveries as _;
use crate::store::metaflow::{MutatesMetaflows, SeedsMetaflow, SleepsMetaflows};
use crate::store::successor::{OwesSuccessors, SuccessionCause, SuccessorIntent};
use crate::store::{FlowStore, ManagesMetaflowState, ReadsFlowMemory, ReadsFlowRows};
use crate::waking::{HoldsWakingMetaflows, SleepsAndWakesSeats};
use crate::{Dispatches, DispatchesSerially};
use flow_ethos::Delivery;
use flow_ethos::State;
use meta_signal_flow::{
    ConfigurationChange, ConfigureMetaflow, ConfigureThreshold, Continuity, HandoverPolicy,
};
use signal_flow::{
    Address, FlowLifecycle, HerdrRouteSelection, Input, Message, Metaflow, Model, Origin, Output,
    RefreshRejection, Refreshed, Refusal, ReplaceRejection, Request, SendRequest, SentBy,
    Unknown_Data,
};
use std::fs;
use std::time::Duration;

/// Releases a fixture operation even when an assertion unwinds before the
/// scoped worker threads are joined. Cleanup is idempotent and cannot replace
/// the assertion that exposed it.
/// Releases a fixture's latch whatever the test came to, so a scope joins.
struct ReleasesFixtureOnDrop<'latch>(&'latch Latch);

impl Drop for ReleasesFixtureOnDrop<'_> {
    fn drop(&mut self) {
        self.0.release();
    }
}

fn seat_address() -> Address {
    address(Model::Sol)
}

/// The seat's metaflow is declared OnDemand, with modules the fixture
/// context verifies, so a wake can compose its successor.
fn declare(fixture: &NexusFixture, at: Address) {
    fixture
        .nexus
        .store
        .configure_change(ConfigurationChange::Metaflows(vec![ConfigureMetaflow {
            leave_outs: signal_flow::LeaveOuts(Vec::new()),
            root_option: None,
            address: at,
            system_modules: signal_flow::SystemModules(vec![
                signal_flow::ModuleIdentity("spirit:core:0".into()),
                signal_flow::ModuleIdentity("intent:flow:0".into()),
            ]),
            prompt_modules: signal_flow::PromptModules(vec![signal_flow::ModuleIdentity(
                "operation:main-flow:0".into(),
            )]),
            flow_kind: signal_flow::FlowKind::Regular,
            tool_set: signal_flow::ToolSet::Default,
            continuity: Continuity::OnDemand,
            purpose: meta_signal_flow::Purpose(String::new()),
            records: meta_signal_flow::Records(vec![]),
        }]))
        .expect("metaflow declared");
}

/// The Claude seat Awake at the Secondary address, its pane live in Herdr,
/// holding `queue`.
fn awake_seat(fixture: &NexusFixture, queue: Vec<Request>) {
    claude_seat(fixture, 40_000);
    fixture.nexus.store.seed_metaflow(Metaflow {
        address: seat_address(),
        state: State::Awake(flow_ethos::FlowId(CLAUDE_SEAT.into())),
        past: flow_ethos::Past(vec![]),
        queue: flow_ethos::Queue(
            queue
                .into_iter()
                .enumerate()
                .map(|(index, request)| signal_flow::QueueEntry {
                    logical_delivery_id: flow_ethos::LogicalDeliveryId(format!(
                        "refreshing-test-{index}"
                    )),
                    request_id: flow_ethos::RequestId("test".into()),
                    sent_by: SentBy::Unknown,
                    delivery: flow_ethos::Delivery::Queue,
                    message: Message::Soft(request),
                })
                .collect(),
        ),
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
        .0
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
            .dispatch_serially(Input::Refresh(seat_address()), Origin::Unbound),
        Output::Refreshed(Refreshed {
            address: seat_address(),
            flow_id: flow_ethos::FlowId(CLAUDE_SEAT.into()),
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
    assert_eq!(
        metaflow.past,
        flow_ethos::Past(vec![flow_ethos::FlowId(CLAUDE_SEAT.to_owned())])
    );
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
        past: flow_ethos::Past(vec![flow_ethos::FlowId(CLAUDE_SEAT.into())]),
        queue: flow_ethos::Queue(vec![]),
    };
    fixture.nexus.store.seed_metaflow(asleep.clone());
    assert_eq!(
        fixture.nexus.dispatch(Input::Refresh(seat_address())),
        Output::RefreshRejected(RefreshRejection::NotAwake)
    );
    assert_eq!(stored(&fixture), asleep, "nothing is written");

    let declared = address(Model::Luna);
    declare(&fixture, declared.clone());
    assert_eq!(
        fixture.nexus.dispatch(Input::Refresh(declared)),
        Output::RefreshRejected(RefreshRejection::NotAwake)
    );

    let unknown = address(Model::Terra);
    assert_eq!(
        fixture.nexus.dispatch(Input::Refresh(unknown.clone())),
        Output::Refused(Refusal::Unknown(Unknown_Data::Address(unknown)))
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
            .dispatch_serially(Input::Refresh(seat_address()), Origin::Unbound),
        Output::RefreshRejected(RefreshRejection::Locked)
    );
    assert!(!closed(&fixture), "nothing is closed while the hold stands");
    assert_eq!(
        stored(&fixture).state,
        State::Awake(flow_ethos::FlowId(CLAUDE_SEAT.into()))
    );
    assert_eq!(lifecycle(&fixture), FlowLifecycle::Active);

    drop(held);
    assert!(matches!(
        fixture
            .nexus
            .dispatch_serially(Input::Refresh(seat_address()), Origin::Unbound),
        Output::Refreshed(_)
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
    // The ending flow was launched, so a wake composes its successor from
    // that launch.
    launched_claude_ended_by(&fixture, |_| true);
    super::waking::rested_at_stop(&fixture, ENDING);
    fixture.nexus.store.seed_metaflow(Metaflow {
        address: seat_address(),
        state: State::Awake(flow_ethos::FlowId(ENDING.into())),
        past: flow_ethos::Past(vec![]),
        queue: flow_ethos::Queue(vec![]),
    });
    let ending = fixture.nexus.store.flow_node(ENDING).unwrap().unwrap();
    let HerdrRouteSelection::Available(route) = &ending.herdr_route_selection else {
        panic!("the ending flow has its pane");
    };
    let world = fixture.fresh_claude_world();
    let caller_agent = codex_agent_in("pane-1", "terminal-pane-1");
    let fresh_script = fs::read_to_string(&fixture.snapshot_program)
        .expect("fresh Herdr")
        .replacen(
            "  *\"api snapshot\"*) a=; [ -f \"$S/harness\" ] && a=$(agent);",
            &format!(
                "  *\"api snapshot\"*) a='{caller_agent}'; [ -f \"$S/harness\" ] && a=\"$a,$(agent)\";"
            ),
            1,
        );
    // Bind the exact kernel caller directly to a separate metaflow. This
    // leaves the recipient's Claude model declaration untouched.
    let sender_address = Address {
        aspect: signal_flow::Aspect::Mind,
        topic: flow_ethos::Topic("sender".into()),
        model: Model::Sol,
    };
    let peer = FlowStore::process_identity_for_pid(std::process::id()).unwrap();
    assert_eq!(
        fixture
            .nexus
            .store
            .bind_metaflow(
                signal_flow::Bind_Data {
                    address: sender_address.clone(),
                    process: peer.clone(),
                },
                Some(flow_ethos::FlowId("a001".into())),
            )
            .unwrap(),
        Output::Bound(flow_ethos::FlowId("a001".into()))
    );
    let log = fixture.accept_pane_operations(
        vec![
            caller_agent,
            serde_json::json!({
                "agent":"claude", "agent_status":"idle", "name":route.herdr_agent_name.0,
                "workspace_id":"w1", "pane_id":route.herdr_pane_id.0,
                "terminal_id":route.herdr_terminal_id.0,
                "agent_session":{"source":"herdr:claude","agent":"claude","kind":"id",
                    "value":ending.session_id.0},
            }),
        ],
        PromptFixture::Prompted("w1:p3"),
    );
    // The fixture Herdr's close of the ending pane announces itself, then
    // waits for the test to release it: the refresh holds the metaflow, its
    // flow not yet recorded ended, for as long as the test needs.
    let closing = fixture.directory.path().join("close-reached");
    let release = Latch::closed(fixture.directory.path().join("close-released"));
    let script = fs::read_to_string(&fixture.snapshot_program).expect("fixture Herdr");
    let pattern = "  \"pane close\"|\"pane send-keys\"|\"agent wait\")";
    assert!(script.contains(pattern), "{script}");
    FixtureExecutable {
        path: fixture.snapshot_program.clone(),
    }
    .install(&script.replacen(
        pattern,
        &format!(
            "  \"pane close\") : > '{closing}'; : < '{release}'; printf '%s\\n' \"$*\" >> '{log}' ;;\n{pattern}",
            closing = closing.display(),
            release = release.path.display(),
            log = log.display(),
        ),
        1,
    ));
    assert_eq!(
        fixture.nexus.sender_of(&peer).unwrap(),
        signal_flow::SentBy::Sender(sender_address.clone()),
        "the caller is authenticated before the recipient transition starts"
    );
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
        let _release_on_drop = ReleasesFixtureOnDrop(&release);
        let refreshing = scope.spawn(|| {
            fixture
                .nexus
                .dispatch_serially(Input::Refresh(seat_address()), Origin::Unbound)
        });
        assert!(
            closing.appeared_by(std::time::Instant::now() + Duration::from_secs(20)),
            "the close never ran"
        );
        // The refresh holds the metaflow and its flow is still Awake.
        assert!(fixture.nexus.waking.is_waking(&seat_address()));
        assert_eq!(
            stored(&fixture).state,
            State::Awake(flow_ethos::FlowId(ENDING.into()))
        );
        let (answered, answer) = std::sync::mpsc::channel();
        let (nexus, peer, request) = (&fixture.nexus, &peer, &request);
        let sending = scope.spawn(move || {
            let answer = nexus.send_request(
                SendRequest {
                    address: seat_address(),
                    sendable_message: super::sendable_message(Message::MiddleAbrupt(
                        request.clone(),
                    )),
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
        // The request has arrived and is waiting behind the refresh. The
        // world now changes to the one in which the successor is launched;
        // its authenticated sender belongs to the request already in hand.
        FixtureExecutable {
            path: fixture.snapshot_program.clone(),
        }
        .install(&fresh_script);
        assert!(fixture.typed().is_none(), "nothing reaches the ending flow");
        release.release();
        (
            refreshing.join().expect("refresh answered"),
            sending.join().expect("send answered"),
        )
    });

    assert_eq!(
        refreshed,
        Output::Refreshed(Refreshed {
            address: seat_address(),
            flow_id: flow_ethos::FlowId(ENDING.into()),
        })
    );
    assert!(
        matches!(sent.response, Output::Sent(_)),
        "{:?}",
        sent.response
    );
    assert!(sent.pending.is_none());
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
    // The waking request starts the successor before it answers Sent.
    let woken = stored(&fixture);
    let State::Awake(successor) = woken.state.clone() else {
        panic!("the successor is woken; Herdr calls: {}", world.calls());
    };
    assert_ne!(
        successor.0, ENDING,
        "a fresh flow, never the one that ended"
    );
    assert_eq!(world.claude_starts(), 1, "{}", world.calls());
    assert_eq!(
        woken.past,
        flow_ethos::Past(vec![flow_ethos::FlowId(ENDING.to_owned())])
    );
    assert!(
        requests(&woken).is_empty(),
        "the successor's accepted first prompt settles its fixed queue batch"
    );
    let turns = world.user_turns(&fixture);
    assert_eq!(turns.len(), 1, "the wake submits one first turn: {turns:?}");
    assert!(
        turns[0].contains("For the successor."),
        "the queued request is in the successor's first prompt: {turns:?}"
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
    super::refresh_and_compaction::seat_refreshing_at(&fixture, 1);
    fixture
        .nexus
        .store
        .record_context_size(CLAUDE_SEAT, 40_000)
        .unwrap();
    assert!(fixture.nexus.store.past_threshold(CLAUDE_SEAT).unwrap());
    close_answers(&fixture, "exit 1");
    assert_eq!(
        fixture.nexus.dispatch(Input::Refresh(seat_address())),
        Output::RefreshRejected(RefreshRejection::CloseRefused)
    );
    assert_eq!(
        stored(&fixture).state,
        State::Awake(flow_ethos::FlowId(CLAUDE_SEAT.into()))
    );
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
        fixture.nexus.dispatch(Input::Refresh(seat_address())),
        Output::RefreshRejected(RefreshRejection::HerdrUnavailable)
    );
    assert_eq!(
        stored(&fixture).state,
        State::Awake(flow_ethos::FlowId(CLAUDE_SEAT.into()))
    );
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
        vec!["spirit:core:0".into(), "intent:flow:0".into()],
    );
    fixture.nexus.store.seed_metaflow(Metaflow {
        address: seat_address(),
        state: State::Awake(flow_ethos::FlowId(CLAUDE_SEAT.into())),
        past: flow_ethos::Past(vec![]),
        queue: flow_ethos::Queue(vec![signal_flow::QueueEntry {
            logical_delivery_id: flow_ethos::LogicalDeliveryId("refreshing-successor-0".into()),
            request_id: flow_ethos::RequestId("test".into()),
            sent_by: SentBy::Unknown,
            delivery: delivery.clone(),
            message: match &delivery {
                Delivery::Wake => signal_flow::Message::MiddleAbrupt(signal_flow::Request::Order(
                    "For the successor.".into(),
                )),
                Delivery::Queue => signal_flow::Message::Soft(signal_flow::Request::Order(
                    "For the successor.".into(),
                )),
            },
        }]),
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
        (Continuity::Maintained, flow_ethos::Delivery::Queue),
        (Continuity::OnDemand, flow_ethos::Delivery::Wake),
    ] {
        let (fixture, _) = refreshable_with_successor(continuity.clone(), delivery.clone());
        // The successor started and receives; the ending flow's reap is owed
        // and answered pending at once, and its worker closes its pane.
        let refreshed = fixture.nexus.dispatch(Input::Refresh(seat_address()));
        assert!(
            matches!(
                &refreshed,
                Output::ReapPending(pending)
                    if pending.address == seat_address()
                        && pending.predecessor.0.0 == CLAUDE_SEAT
                        && pending.reap_standing == signal_flow::ReapStanding::Owed
            ),
            "{continuity:?} {delivery:?}: {refreshed:?}"
        );
        super::reaping::finish_reaps(&fixture);
        let metaflow = stored(&fixture);
        assert_eq!(
            metaflow.state,
            State::Awake(flow_ethos::FlowId("908786".into())),
            "the successor is Awake: {continuity:?} {delivery:?}"
        );
        assert_eq!(
            metaflow.past,
            flow_ethos::Past(vec![flow_ethos::FlowId(CLAUDE_SEAT.to_owned())])
        );
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
    let (fixture, _) = refreshable_with_staged_successor(
        Continuity::Maintained,
        flow_ethos::Delivery::Queue,
        false,
    );
    assert_eq!(
        fixture.nexus.dispatch(Input::Refresh(seat_address())),
        Output::RefreshRejected(RefreshRejection::SuccessorUnconfirmed)
    );
    assert_eq!(
        stored(&fixture).state,
        State::Awake(flow_ethos::FlowId(CLAUDE_SEAT.into()))
    );
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
        SendRequest {
            address: seat_address(),
            sendable_message: super::sendable_message(Message::HardAbrupt(request.clone())),
        },
        Some(&peer),
    );
    assert!(
        matches!(sent.response, Output::SendAccepted(_)),
        "{:?}",
        sent.response
    );
    let pending = sent.pending.expect("the queued Send asks for a wake");
    let metaflow = stored(&fixture);
    assert_eq!(requests(&metaflow), vec![request]);
    assert!(matches!(
        metaflow.queue.0[0].message,
        Message::HardAbrupt(_)
    ));
    assert!(fixture.typed().is_none(), "nothing is typed under the hold");
    // The holder's own drain finds the metaflow held: nothing is typed.
    fixture.nexus.wake_due(pending.address);
    assert!(fixture.typed().is_none());
    drop(held);
}

/// A HardAbrupt or MiddleAbrupt Send to an ordinary Awake queue drains its
/// whole cohort immediately. An outstanding delivery or threshold refresh
/// still owns the Address, so the newly admitted work waits behind it.
#[test]
fn an_abrupt_wake_send_to_a_busy_seat_is_queued_never_refused() {
    use crate::store::context::JudgesThreshold as _;
    use crate::store::context::RecordsContextSize as _;
    use crate::store::logical_delivery::RecordsLogicalDeliveries as _;
    use crate::store::queue_delivery::{
        AccountsQueueDelivery, NativeHistoryAnchor, PrepareQueueDelivery,
    };
    use signal_flow::HarnessKind;
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
                    Busy::QueueNotEmpty | Busy::DeliveryOutstanding | Busy::PastThreshold => {
                        vec![Request::Order("Already waiting.".into())]
                    }
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
            let queued_before = stored(&fixture).queue.0.len();
            let caller = MarkedProcess::spawn("sleep", &["30"], "pane-1");
            let peer = FlowStore::process_identity_for_pid(caller.0.id()).unwrap();
            let sent = fixture.nexus.send_request(
                SendRequest {
                    address: seat_address(),
                    sendable_message: super::sendable_message(abrupt(Request::Order(
                        "Arrived while busy.".into(),
                    ))),
                },
                Some(&peer),
            );
            match busy {
                Busy::QueueNotEmpty => {
                    assert!(
                        matches!(sent.response, Output::Sent(_)),
                        "{:?}",
                        sent.response
                    );
                    assert!(
                        stored(&fixture).queue.0.is_empty(),
                        "the exact receipt settles all"
                    );
                }
                Busy::DeliveryOutstanding => {
                    let Output::SendAccepted(acceptance) = &sent.response else {
                        panic!("{busy:?}: {:?}", sent.response)
                    };
                    assert_eq!(
                        acceptance.visible_delivery_state,
                        signal_flow::VisibleDeliveryState::Queued,
                        "{busy:?}"
                    );
                    assert_eq!(
                        stored(&fixture).queue.0.len(),
                        queued_before + 1,
                        "{busy:?}"
                    );
                }
                Busy::PastThreshold => {
                    let Output::Sent(_) = &sent.response else {
                        panic!("{busy:?}: {:?}", sent.response)
                    };
                    let signal_flow::PrivateSendResponse::Sent(receipt) = &sent.private_response
                    else {
                        panic!("{busy:?}: {:?}", sent.private_response)
                    };
                    assert!(stored(&fixture).queue.0.is_empty(), "{busy:?}");
                    let typed = fixture.typed().expect("post-threshold cohort reached pane");
                    assert!(typed.contains("Already waiting."), "{typed}");
                    assert!(typed.contains("Arrived while busy."), "{typed}");
                    for logical_id in [
                        "refreshing-test-0".to_owned(),
                        receipt.logical_delivery_id.clone().0,
                    ] {
                        let snapshot = fixture
                            .nexus
                            .store
                            .logical_delivery_snapshot(&flow_ethos::LogicalDeliveryId(
                                logical_id.to_owned(),
                            ))
                            .unwrap()
                            .unwrap();
                        assert!(matches!(
                            snapshot.delivery_state,
                            signal_flow::DeliveryState::Delivered(_)
                        ));
                    }
                }
            }
        }
    }
}

/// A Send between Primaries and from a Secondary to its Primary is queued
/// like any other: no route refuses it.
#[test]
fn a_send_to_a_primary_is_accepted_from_a_primary_and_from_a_secondary() {
    use crate::store::logical_delivery::RecordsLogicalDeliveries as _;
    let fixture = NexusFixture::new();
    let primary = |topic: &str| Address {
        aspect: signal_flow::Aspect::Mind,
        topic: flow_ethos::Topic(topic.into()),
        model: Model::Astra,
    };
    let target = primary("flow");
    fixture.nexus.store.seed_metaflow(Metaflow {
        address: target.clone(),
        state: State::Asleep,
        past: flow_ethos::Past(vec![]),
        queue: flow_ethos::Queue(vec![]),
    });
    for sender in [primary("other"), seat_address()] {
        for request in [
            Request::Order("An order.".into()),
            Request::Result(flow_ethos::ResultPayload::Historical("A result.".into())),
        ] {
            let response = fixture
                .nexus
                .store
                .accept_logical_delivery(
                    &target,
                    SentBy::Sender(sender.clone()),
                    flow_ethos::Delivery::Queue,
                    Message::Soft(request),
                    None,
                )
                .expect("accepted");
            assert!(
                matches!(response, signal_flow::PrivateSendResponse::SendAccepted(_)),
                "{sender:?}: {response:?}"
            );
        }
    }
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
        vec!["spirit:core:0".into(), "intent:flow:0".into()],
    );
    awake_seat(&fixture, vec![]);
    let Ok(Input::Replace(profile)) = fixture.nexus.declared_launch(signal_flow::LaunchRequest {
        address: seat_address(),
        remembering: signal_flow::Remembering::Predecessors(signal_flow::RememberingDepth(1)),
        instruction_prompt: signal_flow::InstructionPrompt("Take up the work.".into()),
    }) else {
        panic!("a Launch to an Awake metaflow is a Replace");
    };
    assert_eq!(
        profile.flow_id_option,
        Some(flow_ethos::FlowId(CLAUDE_SEAT.to_owned()))
    );
    assert_eq!(profile.address, seat_address());
    assert_eq!(
        profile.instruction_prompt,
        signal_flow::InstructionPrompt("Take up the work.".into())
    );
}

/// A Replace of an Awake metaflow's flow, as a Launch names it, runs under
/// the metaflow's lock: refused Locked while another holds it; while it
/// reaps, the metaflow is held, and a drain that asks meanwhile types
/// nothing into the predecessor's pane; the queue is kept for the
/// successor.
#[test]
fn a_launch_ordered_replace_holds_the_metaflow_and_types_nothing_into_its_predecessor() {
    let (fixture, profile) =
        refreshable_with_successor(Continuity::OnDemand, flow_ethos::Delivery::Queue);

    let held = fixture
        .nexus
        .waking
        .hold(&seat_address())
        .expect("another holds the metaflow");
    assert_eq!(
        fixture.nexus.dispatch(Input::Replace(profile.clone())),
        Output::ReplaceRejected(ReplaceRejection::Locked)
    );
    assert_eq!(lifecycle(&fixture), FlowLifecycle::Active);
    drop(held);

    // The Replace answers once its successor is confirmed and the reap is
    // owed: no Herdr call of the reap runs in it, and a drain asked after it
    // hands the queue to the successor, never the predecessor.
    let replaced = fixture.nexus.dispatch(Input::Replace(profile.clone()));
    assert!(
        matches!(&replaced, Output::ReapPending(pending)
            if pending.predecessor.0.0 == CLAUDE_SEAT
                && pending.reap_standing == signal_flow::ReapStanding::Owed),
        "{replaced:?}"
    );
    assert!(
        !fs::read_to_string(fixture.directory.path().join("herdr-calls.log"))
            .unwrap_or_default()
            .contains("pane close w1:p3"),
        "the reap's close is its worker's, outside the Replace"
    );
    assert_eq!(lifecycle(&fixture), FlowLifecycle::Stopped);
    let metaflow = stored(&fixture);
    assert_eq!(
        metaflow.state,
        State::Awake(flow_ethos::FlowId("908786".into()))
    );
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
    super::reaping::finish_reaps(&fixture);
    assert_eq!(
        typing_calls(&fixture),
        Vec::<String>::new(),
        "nothing is typed into the predecessor while its reap settles"
    );
}

/// A timer created by the production scheduler remains durable while an
/// actual replacement holds the metaflow. Its next warning is admitted and
/// advanced before that hold is released, then the same logical delivery is
/// drained exactly once to the successor rather than typed into the ending
/// predecessor.
#[test]
fn scheduled_warning_admitted_during_replace_drains_once_to_the_successor() {
    use crate::message_projection::ProjectsQueueEntry;
    use datom_codec::{Actualizing, Potential};

    let (mut fixture, profile) =
        refreshable_with_successor(Continuity::OnDemand, flow_ethos::Delivery::Queue);
    let fixture_only_startup = stored(&fixture)
        .queue
        .0
        .into_iter()
        .next()
        .expect("replacement fixture startup entry");
    assert!(
        fixture
            .nexus
            .store
            .remove_oldest_queued(&seat_address(), &fixture_only_startup)
            .unwrap()
    );
    fixture.nexus.store.seed_role(signal_flow::CallerIdentity {
        flow_id: flow_ethos::FlowId(CLAUDE_SEAT.into()),
        aspect: signal_flow::Aspect::Mind,
        model_name: flow_ethos::ModelName("fixture-model".into()),
    });
    fixture
        .nexus
        .store
        .configure_change(ConfigurationChange::Thresholds(vec![ConfigureThreshold {
            model_name: flow_ethos::ModelName("fixture-model".into()),
            refresh_threshold: meta_signal_flow::RefreshThreshold(100),
        }]))
        .unwrap();
    fixture
        .nexus
        .store
        .configure_change(ConfigurationChange::HandoverPolicies(vec![
            HandoverPolicy {
                model_name: flow_ethos::ModelName("fixture-model".into()),
                warning_share_left: meta_signal_flow::WarningShareLeft(20),
                reminder_text: meta_signal_flow::ReminderText(
                    "flow Keep.{ WhatIAmStillTryingToDo «…» }".into(),
                ),
                every_minutes: meta_signal_flow::EveryMinutes(5),
            },
        ]))
        .unwrap();
    fixture
        .nexus
        .store
        .record_context_size(CLAUDE_SEAT, 81)
        .unwrap();
    fixture
        .nexus
        .store
        .ensure_live_handover(&seat_address(), CLAUDE_SEAT)
        .unwrap();

    fixture.nexus.send_due_handover_reminders(10);
    let first = fixture
        .nexus
        .store
        .handover_reminder(&seat_address(), 0)
        .unwrap()
        .expect("the scheduler created the timer");
    assert_eq!((first.sent, first.next_due), (1, 310));
    let first_warning_id = stored(&fixture)
        .queue
        .0
        .iter()
        .find(|entry| {
            matches!(&entry.message, Message::MiddleAbrupt(Request::Notice(text))
                if text.contains("flow Keep."))
        })
        .expect("the scheduler durably admitted its first warning")
        .logical_delivery_id
        .clone();
    let predecessor_calls = typing_calls(&fixture).len();

    let closing = fixture.directory.path().join("warning-close-reached");
    let release = Latch::closed(fixture.directory.path().join("warning-close-release"));
    let script = fs::read_to_string(&fixture.snapshot_program).expect("fixture Herdr");
    let pattern = "  *\" pane close \"*) exit 0 ;;";
    assert!(script.contains(pattern), "{script}");
    FixtureExecutable {
        path: fixture.snapshot_program.clone(),
    }
    .install(&script.replacen(
        pattern,
        &format!(
            "  *\" pane close \"*) : > '{closing}'; : < '{release}'; exit 0 ;;",
            closing = closing.display(),
            release = release.path.display(),
        ),
        1,
    ));

    // The Replace answers once the reap is owed; the predecessor's close is
    // its worker's, and the warning is admitted while that close runs.
    let replaced = fixture.nexus.dispatch(Input::Replace(profile.clone()));
    let warning_id = std::thread::scope(|scope| {
        // Scope joins its children after this closure returns or unwinds.
        // Keep the release guard inside the closure so an assertion releases
        // the fixture's native close before that join can wait on it.
        let _release_before_join = ReleasesFixtureOnDrop(&release);
        let reaping = scope.spawn(|| super::reaping::finish_reaps(&fixture));
        let deadline = std::time::Instant::now() + Duration::from_secs(20);
        assert!(
            closing.appeared_by(deadline),
            "the reap never reached close"
        );
        let reminding = scope.spawn(|| fixture.nexus.send_due_handover_reminders(310));
        let admitted = || {
            stored(&fixture)
                .queue
                .0
                .iter()
                .find(|entry| {
                    entry.logical_delivery_id != first_warning_id
                        && matches!(&entry.message, Message::MiddleAbrupt(Request::Notice(text))
                        if text.contains("flow Keep."))
                })
                .map(|entry| entry.logical_delivery_id.clone())
        };
        // Asked again at each change the store announces.
        assert!(
            fixture
                .nexus
                .store
                .launch_changes
                .until_holds(deadline, &mut || admitted().is_some()),
            "warning was not durably admitted"
        );
        let warning_id = admitted().expect("the warning admitted");
        let timer = fixture
            .nexus
            .store
            .handover_reminder(&seat_address(), 0)
            .unwrap()
            .unwrap();
        assert_eq!((timer.sent, timer.next_due), (2, 610));
        let queued = fixture
            .nexus
            .store
            .logical_delivery_snapshot(&warning_id)
            .unwrap()
            .unwrap();
        assert!(matches!(
            queued.delivery_state,
            signal_flow::DeliveryState::Queued
        ));
        assert_eq!(typing_calls(&fixture).len(), predecessor_calls);
        release.release();
        reminding.join().expect("reminder tick completed");
        reaping.join().expect("the reap settled");
        warning_id
    });

    let Output::ReapPending(replaced) = replaced else {
        panic!("replacement answered: {replaced:?}")
    };
    assert_eq!(replaced.predecessor.0.0, CLAUDE_SEAT);
    let successor_flow_id = replaced.launched.flow_id;
    assert_ne!(successor_flow_id.0, CLAUDE_SEAT);
    assert_eq!(
        stored(&fixture).state,
        State::Awake(successor_flow_id.clone())
    );
    assert_eq!(typing_calls(&fixture).len(), predecessor_calls);
    let successor = fixture
        .nexus
        .store
        .flow_node(&successor_flow_id.0)
        .unwrap()
        .expect("replacement successor registered");
    let HerdrRouteSelection::Available(successor_route) = successor.herdr_route_selection.clone()
    else {
        panic!("replacement successor has no Herdr route: {successor:?}")
    };
    let codex = accepting_codex_endpoint(&mut fixture, "fixture-model", &successor.session_id.0);
    let successor_agent = serde_json::json!({
        "agent":"codex",
        "agent_status":"working",
        "interactive_ready":true,
        "name":successor_route.herdr_agent_name.0.clone(),
        "pane_id":successor_route.herdr_pane_id.0.clone(),
        "terminal_id":successor_route.herdr_terminal_id.0.clone(),
        "agent_session":{
            "source":"herdr:codex", "agent":"codex", "kind":"id",
            "value":successor.session_id.0.clone()
        }
    });
    let successor_operations = fixture.accept_pane_operations(
        vec![successor_agent],
        PromptFixture::Prompted(&successor_route.herdr_pane_id.0),
    );
    let successor_prompt = format!(
        "--session {} agent prompt {}",
        successor_route.herdr_session_name.0, successor_route.herdr_pane_id.0
    );
    let successor_input_count = || {
        fs::read_to_string(&successor_operations)
            .unwrap_or_default()
            .lines()
            .filter(|call| call.contains(&successor_prompt))
            .count()
    };
    let queued_cohort = stored(&fixture).queue;
    assert_eq!(
        queued_cohort
            .0
            .iter()
            .map(|entry| entry.logical_delivery_id.clone())
            .collect::<Vec<_>>(),
        vec![first_warning_id.clone(), warning_id.clone()],
        "the scheduler's two distinct warnings remain oldest first"
    );
    let expected_inputs = queued_cohort
        .0
        .iter()
        .map(|entry| entry.visible_input(&seat_address()))
        .collect::<Vec<_>>();
    assert_eq!(successor_input_count(), 0);
    assert_eq!(fixture.typed(), None);
    let drained = fixture.nexus.perform(Operation::Drain(seat_address()));
    let Outcome::Drained(drained_cohort) = drained else {
        panic!("warning cohort did not drain: {drained:?}")
    };
    assert_eq!(
        drained_cohort, queued_cohort.0,
        "the one drain parks both distinct logical IDs in queue order"
    );
    let cohort_input = fixture.typed().expect("warning cohort typed to successor");
    let mut budget = datom_codec::Budget {
        remaining: 65536,
        reader: protos::ReaderBudget::try_from(65536).expect("a constant reader budget"),
        depth: 0,
        maximum_depth: 1024,
    };
    let observed_inputs =
        Potential::<Vec<signal_flow::VisibleNativeInput>>::from(cohort_input.clone())
            .actualize(&mut budget)
            .unwrap_or_else(|error| panic!("{cohort_input}: {error:?}"));
    assert_eq!(
        observed_inputs, expected_inputs,
        "one native cohort carries both warning IDs' exact visible inputs, oldest first"
    );
    assert_eq!(successor_input_count(), 1);
    for warning in [&first_warning_id, &warning_id] {
        let placed = fixture
            .nexus
            .store
            .logical_delivery_snapshot(warning)
            .unwrap()
            .unwrap();
        assert!(
            matches!(placed.delivery_state, signal_flow::DeliveryState::Uncertain),
            "the cohort is durably placed, not fabricated as delivered: {placed:?}"
        );
    }
    let _ = fixture.nexus.perform(Operation::Drain(seat_address()));
    assert_eq!(fixture.typed(), Some(cohort_input));
    assert_eq!(
        successor_input_count(),
        1,
        "an extra drain produces no additional native input"
    );
    drop(codex);
    assert!(
        stored(&fixture).queue.0.is_empty(),
        "both correlated warning IDs leave the queue together"
    );
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
    waking.queue = flow_ethos::Queue(vec![signal_flow::QueueEntry {
        logical_delivery_id: flow_ethos::LogicalDeliveryId("refreshing-wake-0".into()),
        request_id: flow_ethos::RequestId("test".into()),
        sent_by: SentBy::Unknown,
        delivery: flow_ethos::Delivery::Wake,
        message: signal_flow::Message::MiddleAbrupt(signal_flow::Request::Order(
            "Wake the successor.".into(),
        )),
    }]);
    fixture.nexus.store.seed_metaflow(waking);
    // Declared, with no model configured for its model and aspect.
    declare(&fixture, seat_address());
    assert_eq!(
        fixture.nexus.dispatch(Input::Refresh(seat_address())),
        Output::RefreshRejected(RefreshRejection::Undeclared)
    );
    assert_eq!(
        stored(&fixture).state,
        State::Awake(flow_ethos::FlowId(CLAUDE_SEAT.into()))
    );
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
    let refused = fixture.nexus.dispatch(Input::Refresh(seat_address()));
    assert!(
        matches!(
            &refused,
            Output::RefreshRejected(RefreshRejection::SuccessorRefused(
                ReplaceRejection::LaunchRefused(_)
            ))
        ),
        "{refused:?}"
    );
    assert_eq!(
        stored(&fixture).state,
        State::Awake(flow_ethos::FlowId(CLAUDE_SEAT.into()))
    );
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
    let (fixture, profile) =
        refreshable_with_successor(Continuity::OnDemand, flow_ethos::Delivery::Queue);
    fixture
        .nexus
        .store
        .forget_successor(&seat_address())
        .unwrap();
    let replaced = fixture.nexus.dispatch(Input::Replace(profile));
    assert!(
        matches!(&replaced, Output::ReapPending(pending) if pending.predecessor.0.0 == CLAUDE_SEAT),
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
        let (fixture, _) =
            refreshable_with_successor(Continuity::Maintained, flow_ethos::Delivery::Queue);
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
        // The reap the start owed is its worker's.
        super::reaping::finish_reaps(&fixture);

        let metaflow = stored(&fixture);
        assert_eq!(
            metaflow.state,
            State::Awake(flow_ethos::FlowId("908786".into())),
            "the successor is Awake: {cause:?}"
        );
        assert_eq!(
            metaflow.past,
            flow_ethos::Past(vec![flow_ethos::FlowId(CLAUDE_SEAT.to_owned())]),
            "{cause:?}"
        );
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

/// The seat's own origin, as the Secondary's own seat asks.
fn own_seat() -> Origin {
    Origin::Flow(signal_flow::FlowOrigin {
        flow_id: flow_ethos::FlowId(CLAUDE_SEAT.into()),
        session_id: signal_flow::SessionId(super::waking::CLAUDE_SESSION.into()),
    })
}

/// A flow that refreshes itself is working: its Refresh answers
/// RefreshQueued at once and closes nothing. The seat stays Awake in its
/// pane; the Stop that ends its turn with nothing in flight is its idle
/// event, at which it is refreshed: its pane closed, its flow Stopped, its
/// metaflow Asleep, the order spent.
#[test]
fn a_self_refresh_of_a_working_seat_waits_for_its_idle_event_and_answers_refresh_queued() {
    use crate::store::ordered_replacement::OrdersReplacements;
    let fixture = NexusFixture::new();
    fixture.accept_pane_operations(
        vec![claude_agent(&fixture)],
        PromptFixture::Prompted("w1:p3"),
    );
    awake_seat(&fixture, vec![]);
    declare(&fixture, seat_address());
    super::waking::claude_prompt_taken(&fixture);

    assert_eq!(
        fixture
            .nexus
            .dispatch_from(Input::Refresh(seat_address()), own_seat()),
        Output::RefreshQueued(signal_flow::RefreshQueued {
            address: seat_address(),
            flow_id: flow_ethos::FlowId(CLAUDE_SEAT.into()),
        })
    );
    assert!(!closed(&fixture), "the working flow's pane stays open");
    assert_eq!(lifecycle(&fixture), FlowLifecycle::Active);
    assert_eq!(
        stored(&fixture).state,
        State::Awake(flow_ethos::FlowId(CLAUDE_SEAT.into()))
    );
    assert!(
        fixture
            .nexus
            .store
            .ordered_replacement(&seat_address())
            .unwrap()
            .is_some(),
        "the refresh is recorded"
    );
    assert!(!fixture.nexus.waking.is_waking(&seat_address()));

    super::waking::report_claude_stop(&fixture, vec![]);

    assert!(closed(&fixture), "the resting flow's pane is closed");
    assert!(
        fixture.typed().is_none(),
        "the ending flow is given no turn"
    );
    assert_eq!(lifecycle(&fixture), FlowLifecycle::Stopped);
    let metaflow = stored(&fixture);
    assert_eq!(metaflow.state, State::Asleep);
    assert_eq!(
        metaflow.past,
        flow_ethos::Past(vec![flow_ethos::FlowId(CLAUDE_SEAT.to_owned())])
    );
    assert_eq!(
        fixture
            .nexus
            .store
            .ordered_replacement(&seat_address())
            .unwrap(),
        None,
        "the order is spent"
    );
}

/// An ordered Refresh is carried out at the flow's next Stop though that
/// Stop names work in flight, a comment watch's monitor among it: the
/// background work is ended with the harness, as a refresh past the
/// threshold ends it. Its pane is closed, its flow Stopped, the order spent.
#[test]
fn an_ordered_refresh_is_carried_out_at_the_next_stop_ending_background_work() {
    use super::waking::background_task;
    use crate::store::ordered_replacement::OrdersReplacements;
    let fixture = NexusFixture::new();
    fixture.accept_pane_operations(
        vec![claude_agent(&fixture)],
        PromptFixture::Prompted("w1:p3"),
    );
    awake_seat(&fixture, vec![]);
    declare(&fixture, seat_address());
    super::waking::claude_prompt_taken(&fixture);
    assert!(matches!(
        fixture
            .nexus
            .dispatch_from(Input::Refresh(seat_address()), own_seat()),
        Output::RefreshQueued(_)
    ));

    super::waking::report_claude_stop(
        &fixture,
        vec![background_task(
            "m1",
            signal_flow::BackgroundTaskKind::Monitor,
        )],
    );

    assert!(closed(&fixture), "the flow is refreshed at this Stop");
    assert!(
        fixture.typed().is_none(),
        "the ending flow is given no turn"
    );
    assert_eq!(lifecycle(&fixture), FlowLifecycle::Stopped);
    assert_eq!(stored(&fixture).state, State::Asleep);
    assert_eq!(
        fixture
            .nexus
            .store
            .ordered_replacement(&seat_address())
            .unwrap(),
        None,
        "the order is spent"
    );
}

/// A Launch naming a working flow answers LaunchPending at once with the
/// attempt it waits as, its profile naming that flow, and LaunchStatus
/// answers the same while it waits. Nothing is closed, and a Stop with a
/// monitor still in flight is no rest: the Launch keeps waiting.
#[test]
fn a_replacing_launch_of_a_working_seat_answers_launch_pending_and_waits() {
    use super::waking::background_task;
    use crate::store::ordered_replacement::OrdersReplacements;
    let fixture = NexusFixture::new();
    fixture.accept_pane_operations(
        vec![claude_agent(&fixture)],
        PromptFixture::Prompted("w1:p3"),
    );
    declare_seat(
        &fixture,
        Continuity::OnDemand,
        vec!["spirit:core:0".into(), "intent:flow:0".into()],
    );
    awake_seat(&fixture, vec![]);
    super::waking::claude_prompt_taken(&fixture);

    let pending = fixture
        .nexus
        .dispatch(Input::Launch(signal_flow::LaunchRequest {
            address: seat_address(),
            remembering: signal_flow::Remembering::Predecessors(signal_flow::RememberingDepth(1)),
            instruction_prompt: signal_flow::InstructionPrompt("Take up the work.".into()),
        }));
    let Output::LaunchPending(attempt) = pending else {
        panic!("a Launch of a working flow waits: {pending:?}");
    };
    assert_eq!(
        attempt.launch_profile.flow_id_option,
        Some(flow_ethos::FlowId(CLAUDE_SEAT.to_owned()))
    );
    assert!(
        attempt
            .launch_profile
            .instruction_prompt
            .0
            .contains("Take up the work.")
    );
    assert_eq!(
        attempt.launch_attempt_phase,
        signal_flow::LaunchAttemptPhase::Reserved
    );
    assert_eq!(
        fixture
            .nexus
            .dispatch(Input::LaunchStatus(attempt.launch_request_id.clone())),
        Output::LaunchInspected(signal_flow::LaunchInspection {
            launch_attempt: attempt.clone(),
            launch_status_outcome_option: None,
            flow_node_option: None,
        })
    );
    assert!(!closed(&fixture), "the working flow's pane stays open");
    assert_eq!(lifecycle(&fixture), FlowLifecycle::Active);

    super::waking::report_claude_stop(
        &fixture,
        vec![background_task(
            "m1",
            signal_flow::BackgroundTaskKind::Monitor,
        )],
    );

    assert!(!closed(&fixture), "a monitor in flight is work in flight");
    assert_eq!(lifecycle(&fixture), FlowLifecycle::Active);
    assert_eq!(
        stored(&fixture).state,
        State::Awake(flow_ethos::FlowId(CLAUDE_SEAT.into()))
    );
    assert!(
        fixture
            .nexus
            .store
            .ordered_launch_request(&attempt.launch_request_id.0)
            .unwrap()
            .is_some(),
        "the Launch still waits"
    );
}

/// A refresh recorded for a working flow is kept by a Nexus that starts
/// again on the same Memory: the reopened store holds it, the new Nexus's
/// start keeps it while the flow works, and the flow's idle event carries
/// it out.
#[test]
fn the_ordered_replacement_survives_a_nexus_restart() {
    use crate::store::ordered_replacement::{OrdersReplacements, ReplacementOrder};
    use crate::store::{OpensFlowStore, SetsNativeOwnerEnvironmentForTests};
    use crate::waking::SleepsAndWakesSeats;
    let mut fixture = NexusFixture::new();
    fixture.accept_pane_operations(
        vec![claude_agent(&fixture)],
        PromptFixture::Prompted("w1:p3"),
    );
    awake_seat(&fixture, vec![]);
    declare(&fixture, seat_address());
    super::waking::claude_prompt_taken(&fixture);
    assert!(matches!(
        fixture.nexus.dispatch(Input::Refresh(seat_address())),
        Output::RefreshQueued(_)
    ));

    // The store closed and opened again at the same path.
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

    let order = fixture
        .nexus
        .store
        .ordered_replacement(&seat_address())
        .unwrap()
        .expect("the refresh survives");
    assert_eq!(order.predecessor, CLAUDE_SEAT);
    assert_eq!(order.order, ReplacementOrder::Refresh);

    fixture.nexus.recover_wakes();
    assert!(!closed(&fixture), "the working flow is not closed at start");
    assert!(
        fixture
            .nexus
            .store
            .ordered_replacement(&seat_address())
            .unwrap()
            .is_some(),
        "the refresh still waits"
    );

    super::waking::report_claude_stop(&fixture, vec![]);
    assert!(closed(&fixture), "the resting flow's pane is closed");
    assert_eq!(lifecycle(&fixture), FlowLifecycle::Stopped);
    assert_eq!(stored(&fixture).state, State::Asleep);
}
