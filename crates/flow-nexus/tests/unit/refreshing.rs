//! An explicit Refresh confirms a successor before retiring the predecessor,
//! including an OnDemand flow with no waking request. While the address is
//! held, a Send queues and nothing reaches the ending flow.

use super::refresh_and_compaction::{
    SUCCESSOR_REQUEST, declare_seat, owe_staged_successor, staged_refresh_successor,
};
use super::send_tool::accepting_codex_endpoint;
use super::waking::{CLAUDE_SEAT, ReadsFreshWorld, address, claude_agent, claude_seat};
use super::{
    ControlsHerdrSnapshot, MarkedProcess, NexusFixture, PromptFixture, bind_existing_caller,
    codex_agent_in,
};
use crate::fixture_executable::{FixtureExecutable, InstallsScript};
use crate::generated::operation::{Operation, Outcome};
use crate::performing::Performs;
use crate::sending::SendsRequests;
use crate::store::context::JudgesThreshold;
use crate::store::logical_delivery::RecordsLogicalDeliveries as _;
use crate::store::metaflow::{SeedsMetaflow, SleepsMetaflows};
use crate::store::successor::{OwesSuccessors, SuccessionCause, SuccessorIntent};
use crate::store::{FlowStore, ManagesMetaflowState, ReadsFlowMemory, ReadsFlowRows};
use crate::waking::{HoldsWakingMetaflows, SleepsAndWakesSeats};
use crate::{Dispatches, DispatchesSerially};
use flow_ethos::Delivery;
use flow_ethos::State;
use meta_signal_flow::{ConfigurationChange, ConfigureMetaflow, Continuity};
use signal_flow::{
    Address, FlowLifecycle, HerdrRouteSelection, Input, Message, Metaflow, Model, Origin, Output,
    RefreshRejection, Refusal, ReplaceRejection, Request, SendRequest, SentBy, Unknown_Data,
};
use std::fs;

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
            effort: meta_signal_flow::Effort::Default,
            leave_outs: signal_flow::LeaveOuts(Vec::new()),
            root_option: None,
            address: at,
            system_modules: signal_flow::SystemModules(vec![
                signal_flow::ModuleIdentity("spirit:core:0".into()),
                signal_flow::ModuleIdentity("intent:flow:0".into()),
            ]),
            prompt_modules: signal_flow::PromptModules(vec![signal_flow::ModuleIdentity(
                "knowledge:main-flow:0".into(),
            )]),
            flow_kind: signal_flow::FlowKind::Regular,
            tool_set: signal_flow::ToolSet::Default,
            continuity: Continuity::OnDemand,
            refresh_mode: meta_signal_flow::RefreshMode::Automatic,
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
    ["herdr-operations.log", "herdr-calls.log"]
        .into_iter()
        .any(|name| {
            fs::read_to_string(fixture.directory.path().join(name))
                .unwrap_or_default()
                .contains("pane close w1:p3")
        })
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

/// An explicit Refresh always starts the successor itself, even when an
/// OnDemand metaflow holds only Notices. The Notice keeps its original
/// queue identity for the successor, and nothing is typed into the ending
/// flow.
#[test]
fn an_explicit_refresh_starts_an_on_demand_successor_for_notices() {
    let notice_id = flow_ethos::LogicalDeliveryId("refresh-notice-kept".into());
    let notice_request = flow_ethos::RequestId("n0t1".into());
    let (fixture, _) = refreshable_with_queue(
        Continuity::OnDemand,
        vec![signal_flow::QueueEntry {
            logical_delivery_id: notice_id.clone(),
            request_id: notice_request.clone(),
            sent_by: SentBy::Unknown,
            delivery: Delivery::Queue,
            message: Message::Soft(Request::Notice("Kept for the successor.".into())),
        }],
        true,
    );
    fixture
        .nexus
        .store
        .configure_change(ConfigurationChange::Amend(meta_signal_flow::Amend_Data {
            address: seat_address(),
            change: meta_signal_flow::Change::RefreshMode(meta_signal_flow::RefreshMode::Manual),
        }))
        .unwrap();

    let refreshed = fixture
        .nexus
        .dispatch_serially(Input::Refresh(seat_address()), Origin::Unbound);
    assert!(
        matches!(&refreshed, Output::ReapPending(pending)
            if pending.address == seat_address()
                && pending.predecessor.0.0 == CLAUDE_SEAT),
        "{refreshed:?}"
    );
    super::reaping::finish_owned_reaps(&fixture);
    assert!(closed(&fixture), "the predecessor's pane is closed");
    assert!(
        fixture.typed().is_none(),
        "the ending flow is given no turn"
    );
    assert_eq!(lifecycle(&fixture), FlowLifecycle::Stopped);
    let metaflow = stored(&fixture);
    assert_eq!(
        metaflow.state,
        State::Awake(flow_ethos::FlowId("908786".into())),
        "the explicit Refresh starts its successor"
    );
    assert_eq!(
        metaflow.past,
        flow_ethos::Past(vec![flow_ethos::FlowId(CLAUDE_SEAT.to_owned())])
    );
    assert_eq!(metaflow.queue.0.len(), 1);
    assert_eq!(metaflow.queue.0[0].logical_delivery_id, notice_id);
    assert_eq!(metaflow.queue.0[0].request_id, notice_request);
    assert_eq!(
        requests(&metaflow),
        vec![Request::Notice("Kept for the successor.".into())]
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
    let (fixture, _) = refreshable_with_queue(Continuity::OnDemand, vec![], true);

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
    let refreshed = fixture
        .nexus
        .dispatch_serially(Input::Refresh(seat_address()), Origin::Unbound);
    assert!(matches!(refreshed, Output::ReapPending(_)), "{refreshed:?}");
    super::reaping::finish_owned_reaps(&fixture);
    assert_eq!(
        stored(&fixture).state,
        State::Awake(flow_ethos::FlowId("908786".into()))
    );
}

/// An explicit Refresh with no queue may compose an independently empty
/// Simplified successor. It opens and confirms that successor dry: no filler
/// prompt is invented before or after reopening the Nexus.
#[test]
fn an_explicit_refresh_confirms_an_empty_simplified_successor_dry() {
    let (mut fixture, world) = super::open_requests::resting_seat();
    std::fs::remove_file(fixture.directory.path().join("flows/abc123/handover.md"))
        .expect("the predecessor has no handover input");
    fixture
        .nexus
        .store
        .configure_change(ConfigurationChange::Amend(meta_signal_flow::Amend_Data {
            address: seat_address(),
            change: meta_signal_flow::Change::Kind(signal_flow::FlowKind::Simplified),
        }))
        .unwrap();

    let refreshed = fixture
        .nexus
        .dispatch_serially(Input::Refresh(seat_address()), Origin::Unbound);
    let Output::ReapPending(pending) = refreshed else {
        panic!("the explicit Refresh confirms its dry successor: {refreshed:?}")
    };
    assert_eq!(world.claude_starts(), 1, "{}", world.calls());
    assert!(world.user_turns(&fixture).is_empty(), "no filler prompt");
    assert_eq!(
        fixture
            .nexus
            .store
            .flow_node(&pending.launched.flow_id.0)
            .unwrap()
            .expect("the dry successor is registered")
            .flow_lifecycle,
        FlowLifecycle::Active
    );

    fixture.restarted();
    assert!(
        world.user_turns(&fixture).is_empty(),
        "reopening a dry successor types nothing"
    );
}

/// An explicit Refresh of an OnDemand metaflow with no queue and no owed
/// successor creates and confirms exactly one successor itself. The predecessor
/// is never given another turn and is reaped only after the successor is Awake.
#[test]
fn an_explicit_refresh_with_no_queue_creates_its_successor() {
    const ENDING: &str = "abc123";
    let (fixture, world) = super::open_requests::resting_seat();
    fixture
        .nexus
        .store
        .configure_change(ConfigurationChange::Amend(meta_signal_flow::Amend_Data {
            address: seat_address(),
            change: meta_signal_flow::Change::Continuity(Continuity::OnDemand),
        }))
        .unwrap();
    assert!(stored(&fixture).queue.0.is_empty());
    assert_eq!(
        fixture
            .nexus
            .store
            .successor_intent(&seat_address())
            .unwrap(),
        None,
        "no earlier event owes a successor"
    );

    let refreshed = fixture
        .nexus
        .dispatch_serially(Input::Refresh(seat_address()), Origin::Unbound);
    assert!(
        matches!(&refreshed, Output::ReapPending(pending)
            if pending.predecessor.0.0 == ENDING
                && pending.launched.flow_id.0 != ENDING
                && pending.reap_standing == signal_flow::ReapStanding::Owed),
        "{refreshed:?}"
    );
    assert_eq!(world.claude_starts(), 1, "{}", world.calls());
    let successor = stored(&fixture);
    assert!(
        matches!(&successor.state, State::Awake(flow_id) if flow_id.0 != ENDING),
        "the confirmed successor is current: {successor:?}"
    );
    assert!(successor.queue.0.is_empty());
    assert_eq!(
        fixture
            .nexus
            .store
            .successor_intent(&seat_address())
            .unwrap(),
        None,
        "confirmation gives the successor intent"
    );
    assert!(fixture.typed().is_none(), "the predecessor gets no turn");

    assert!(
        !world.calls().contains("pane close w1:p3"),
        "the owned reap, not Refresh, closes the predecessor"
    );
    assert_eq!(
        fixture
            .nexus
            .store
            .flow_node(ENDING)
            .unwrap()
            .expect("the predecessor row remains")
            .flow_lifecycle,
        FlowLifecycle::Stopped
    );
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
pub(super) fn refreshable_with_successor(
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
    refreshable_with_queue(
        continuity,
        vec![signal_flow::QueueEntry {
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
        }],
        confirmed,
    )
}

/// An Awake predecessor and a staged successor, with exactly `queue` kept
/// on their metaflow. This lets Refresh tests distinguish a successor owed
/// by the explicit operation from one owed by queued waking work.
fn refreshable_with_queue(
    continuity: Continuity,
    queue: Vec<signal_flow::QueueEntry>,
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
        queue: flow_ethos::Queue(queue),
    });
    owe_staged_successor(&fixture);
    fixture.herdr_for_replacement_showing(0, vec![claude_agent(&fixture)]);
    (fixture, successor.request())
}

/// A refresh of a Maintained metaflow, or of one whose queue holds a request
/// that asks for a wake, starts the successor in the same event: it is
/// started and its receipt confirmed before the ending flow is reaped, the
/// metaflow is Awake as it, the queue is its successor's, and the answer
/// still names the flow that ended. Nothing is typed into the ending flow.
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
        super::reaping::finish_owned_reaps(&fixture);
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
        assert!(
            typing_calls(&fixture)
                .iter()
                .all(|call| !call.contains("w1:p3")),
            "nothing is typed into the ending flow: {:?}",
            typing_calls(&fixture)
        );
        // The queue is the successor's: a Soft request waits for its idle
        // event, a MiddleAbrupt one is handed to it at once.
        let kept_for_the_successor = match delivery {
            flow_ethos::Delivery::Queue => vec![Request::Order("For the successor.".into())],
            flow_ethos::Delivery::Wake => Vec::new(),
        };
        assert_eq!(
            requests(&metaflow),
            kept_for_the_successor,
            "{continuity:?} {delivery:?}"
        );
        assert_eq!(lifecycle(&fixture), FlowLifecycle::Stopped);
        assert!(
            fs::read_to_string(fixture.directory.path().join("herdr-calls.log"))
                .unwrap_or_default()
                .contains("pane close w1:p3"),
            "the ending flow's pane is closed"
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
        assert!(!fixture.nexus.waking.is_waking(&seat_address()));
    }
}

/// A refresh whose owed successor's launch is still ambiguous when the
/// confirmation wait ends answers SuccessorUnconfirmed, never Locked: the
/// ending flow stays Awake, live and unreaped, the successor stays owed,
/// and the lock is released.
#[test]
fn a_refresh_whose_successor_stays_unconfirmed_answers_successor_unconfirmed() {
    let (fixture, _) = refreshable_with_queue(Continuity::OnDemand, vec![], false);
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
/// whole cohort immediately, and so does one to a seat past its refresh
/// threshold.
#[test]
fn an_abrupt_wake_send_to_a_busy_seat_is_queued_never_refused() {
    use crate::store::context::JudgesThreshold as _;
    use crate::store::context::RecordsContextSize as _;
    use crate::store::logical_delivery::RecordsLogicalDeliveries as _;
    #[derive(Clone, Copy, Debug)]
    enum Busy {
        QueueNotEmpty,
        PastThreshold,
    }
    for busy in [Busy::QueueNotEmpty, Busy::PastThreshold] {
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
                    Busy::QueueNotEmpty | Busy::PastThreshold => {
                        vec![Request::Order("Already waiting.".into())]
                    }
                },
            );
            declare(&fixture, seat_address());
            match busy {
                Busy::QueueNotEmpty => {}
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
                        "the writer's result settles all"
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

#[test]
fn an_idle_unwritable_claude_predecessor_yields_queued_work_to_its_ordered_successor() {
    use crate::generated::memory::{OrderedLaunch, OrderedReplacement, ReplacementOrder};
    use crate::generated::transitions::{ReportedEvent, ReportsEvents};
    use crate::reporting::RecordsReports;
    use crate::store::ordered_replacement::OrdersReplacements;

    let (mut fixture, profile) =
        refreshable_with_successor(Continuity::Maintained, Delivery::Queue);
    let at = seat_address();
    let startup_entry = stored(&fixture).queue.0.remove(0);
    assert!(
        fixture
            .nexus
            .store
            .remove_oldest_queued(&at, &startup_entry)
            .unwrap()
    );
    for body in ["First for the successor.", "Second for the successor."] {
        let admitted = fixture
            .nexus
            .store
            .accept_logical_delivery(
                &at,
                SentBy::Unknown,
                Delivery::Queue,
                Message::Soft(Request::Order(body.into())),
                None,
            )
            .unwrap();
        assert!(matches!(
            admitted,
            signal_flow::PrivateSendResponse::SendAccepted(_)
        ));
    }
    let retained = stored(&fixture).queue.0;
    fixture.nexus.report_event(ReportedEvent {
        flow_id: flow_ethos::FlowId(CLAUDE_SEAT.into()),
        event: crate::generated::memory::Event::Working(signal_flow::Tokens(40_000)),
    });
    fixture
        .nexus
        .store
        .order_replacement(OrderedReplacement {
            address: at.clone(),
            predecessor: crate::generated::memory::Predecessor(CLAUDE_SEAT.into()),
            replacement_order: ReplacementOrder::Launch(Box::new(OrderedLaunch {
                launch_profile: profile,
                origin: Origin::Unbound,
                prompt_sha256: crate::generated::memory::PromptSha256(String::new()),
            })),
        })
        .unwrap();
    let mut idle = claude_agent(&fixture);
    idle["agent_status"] = "idle".into();
    idle["interactive_ready"] = false.into();
    fixture.herdr_for_replacement_showing(0, vec![idle]);

    fixture.nexus.after_report(
        &flow_ethos::FlowId(CLAUDE_SEAT.into()),
        &super::waking::claude_stop(&fixture),
    );
    let successor_id = flow_ethos::FlowId("908786".into());
    let current = stored(&fixture);
    assert_eq!(current.state, State::Awake(successor_id.clone()));
    assert_eq!(current.queue.0, retained);
    assert_eq!(lifecycle(&fixture), FlowLifecycle::Stopped);
    assert!(
        fixture
            .nexus
            .store
            .ordered_replacement(&at)
            .unwrap()
            .is_none()
    );
    assert!(typing_calls(&fixture).is_empty());

    let successor = fixture
        .nexus
        .store
        .flow_node(&successor_id.0)
        .unwrap()
        .unwrap();
    let HerdrRouteSelection::Available(route) = successor.herdr_route_selection else {
        panic!("ordered successor has no route");
    };
    let _codex = accepting_codex_endpoint(&mut fixture, "fixture-model", &successor.session_id.0);
    let operations = fixture.accept_pane_operations(
        vec![fixture.successor_agent()],
        PromptFixture::Prompted(&route.herdr_pane_id.0),
    );
    assert_eq!(
        fixture.nexus.perform(Operation::Drain(at.clone())),
        Outcome::Drained(retained.clone())
    );
    let typed = fixture
        .typed()
        .expect("the successor takes the retained cohort");
    assert!(
        typed.find("First for the successor.").unwrap()
            < typed.find("Second for the successor.").unwrap()
    );
    for entry in &retained {
        assert_eq!(typed.matches(&entry.request_id.0).count(), 1);
    }
    assert!(stored(&fixture).queue.0.is_empty());
    let _ = fixture.nexus.perform(Operation::Drain(at));
    let calls = fs::read_to_string(operations).unwrap();
    assert_eq!(
        calls
            .lines()
            .filter(|call| call.contains("agent prompt"))
            .count(),
        1
    );
    assert!(calls.contains(&format!("agent prompt {}", route.herdr_pane_id.0)));
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
    for cause in [
        SuccessionCause::Ordered(crate::generated::memory::RefreshCaller::Elsewhere(
            Origin::Unbound,
        )),
        SuccessionCause::Replace,
    ] {
        let (fixture, _) =
            refreshable_with_successor(Continuity::Maintained, flow_ethos::Delivery::Queue);
        fixture
            .nexus
            .store
            .owe_successor(SuccessorIntent {
                address: seat_address(),
                predecessor: crate::generated::memory::Predecessor(CLAUDE_SEAT.into()),
                succession_cause: cause.clone(),
                launch_request_id: signal_flow::LaunchRequestId(SUCCESSOR_REQUEST.into()),
            })
            .expect("successor owed");
        assert!(
            !fixture.nexus.store.past_threshold(CLAUDE_SEAT).unwrap(),
            "the predecessor is not past any threshold"
        );

        fixture.nexus.recover_wakes();
        // The reap the start owed is its worker's.
        super::reaping::finish_owned_reaps(&fixture);

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
/// RefreshQueued at once and closes nothing. At its real idle event the
/// ordered Refresh starts and confirms the successor before reaping it,
/// even though the OnDemand metaflow has no queued work.
#[test]
fn a_self_refresh_of_a_working_seat_waits_for_its_idle_event_and_answers_refresh_queued() {
    use crate::store::ordered_replacement::OrdersReplacements;
    let (fixture, _) = refreshable_with_queue(Continuity::OnDemand, vec![], true);
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

    super::reaping::finish_owned_reaps(&fixture);
    assert!(
        closed(&fixture),
        "the predecessor's pane is closed by its reap"
    );
    assert!(
        fixture.typed().is_none(),
        "the ending flow is given no turn"
    );
    assert_eq!(lifecycle(&fixture), FlowLifecycle::Stopped);
    let metaflow = stored(&fixture);
    assert_eq!(
        metaflow.state,
        State::Awake(flow_ethos::FlowId("908786".into())),
        "the queued explicit Refresh starts its own successor"
    );
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
/// background work keeps it awake until a later authenticated Stop clears it.
#[test]
fn an_ordered_refresh_waits_for_the_stop_that_confirms_no_background_work() {
    use super::waking::background_task;
    use crate::store::ordered_replacement::OrdersReplacements;
    let (fixture, _) = refreshable_with_queue(Continuity::OnDemand, vec![], true);
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

    assert!(!closed(&fixture));
    assert_eq!(lifecycle(&fixture), FlowLifecycle::Active);
    assert!(
        fixture
            .nexus
            .store
            .ordered_replacement(&seat_address())
            .unwrap()
            .is_some()
    );
    super::waking::report_claude_stop(&fixture, vec![]);
    super::reaping::finish_owned_reaps(&fixture);
    assert!(closed(&fixture), "the predecessor is reaped at safe rest");
    assert!(
        fixture.typed().is_none(),
        "the ending flow is given no turn"
    );
    assert_eq!(lifecycle(&fixture), FlowLifecycle::Stopped);
    assert_eq!(
        stored(&fixture).state,
        State::Awake(flow_ethos::FlowId("908786".into()))
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

/// A Refresh's caller as the Nexus read it: a flow of another Address.
fn another_flows_caller() -> crate::generated::memory::RefreshCaller {
    crate::generated::memory::RefreshCaller::Elsewhere(Origin::Flow(signal_flow::FlowOrigin {
        flow_id: flow_ethos::FlowId("c0ffee".into()),
        session_id: signal_flow::SessionId("c0ffee-session".into()),
    }))
}

/// The store closed and opened again at the same path.
fn reopen_store(fixture: &mut NexusFixture) {
    use crate::store::{OpensFlowStore, SetsNativeOwnerEnvironmentForTests};
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
}

/// A refresh recorded for a working flow is kept by a Nexus that starts
/// again on the same Memory: the reopened store holds it with the caller it
/// was called by, the new Nexus's start keeps it while the flow works, and
/// the flow's idle event carries it out. A second caller of the waiting
/// Refresh does not replace the first.
#[test]
fn the_ordered_replacement_survives_a_nexus_restart() {
    use crate::store::ordered_replacement::{OrdersReplacements, ReplacementOrder};
    use crate::waking::{RefreshesMetaflows, SleepsAndWakesSeats};
    let (mut fixture, _) = refreshable_with_queue(Continuity::OnDemand, vec![], true);
    super::waking::claude_prompt_taken(&fixture);
    let caller = another_flows_caller();
    assert!(matches!(
        fixture.nexus.refresh(&seat_address(), &caller),
        Output::RefreshQueued(_)
    ));
    // Another caller of the waiting Refresh is answered queued and leaves
    // the recorded caller as it was.
    assert!(matches!(
        fixture.nexus.refresh(
            &seat_address(),
            &crate::generated::memory::RefreshCaller::Elsewhere(Origin::Unbound)
        ),
        Output::RefreshQueued(_)
    ));
    assert_eq!(
        fixture
            .nexus
            .store
            .ordered_replacement(&seat_address())
            .unwrap()
            .expect("the refresh is queued")
            .replacement_order,
        ReplacementOrder::Refresh(caller.clone()),
        "a queued refresh keeps the caller that queued it"
    );

    reopen_store(&mut fixture);

    let order = fixture
        .nexus
        .store
        .ordered_replacement(&seat_address())
        .unwrap()
        .expect("the refresh survives");
    assert_eq!(order.predecessor.0, CLAUDE_SEAT);
    assert_eq!(
        order.replacement_order,
        ReplacementOrder::Refresh(caller.clone()),
        "the reopened store keeps the caller"
    );

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
    super::reaping::finish_owned_reaps(&fixture);
    assert!(closed(&fixture), "the predecessor's pane is reaped");
    assert_eq!(lifecycle(&fixture), FlowLifecycle::Stopped);
    assert_eq!(
        stored(&fixture).state,
        State::Awake(flow_ethos::FlowId("908786".into()))
    );
}

/// The succession a queued Refresh causes carries the caller the Refresh was
/// called by, through the queue and a Nexus restart: the Refresh of a
/// working Maintained flow waits for its rest, a Nexus starts again on the
/// same Memory, the flow rests, and the successor owed (its launch not yet
/// confirmed) records the cause `Ordered` with that caller, which a reopened
/// store reads back whole.
#[test]
fn a_queued_refresh_hands_its_caller_to_the_succession_it_causes() {
    use crate::store::ordered_replacement::OrdersReplacements;
    use crate::waking::{RefreshesMetaflows, SleepsAndWakesSeats};
    let (mut fixture, _) = refreshable_with_staged_successor(
        Continuity::Maintained,
        flow_ethos::Delivery::Queue,
        false,
    );
    super::waking::claude_prompt_taken(&fixture);
    let caller = another_flows_caller();
    assert!(matches!(
        fixture.nexus.refresh(&seat_address(), &caller),
        Output::RefreshQueued(_)
    ));
    assert_eq!(
        fixture
            .nexus
            .store
            .successor_intent(&seat_address())
            .unwrap()
            .map(|intent| intent.succession_cause),
        Some(SuccessionCause::Refresh),
        "the fixture owes the successor for the threshold only, while the flow works"
    );

    reopen_store(&mut fixture);
    assert_eq!(
        fixture
            .nexus
            .store
            .ordered_replacement(&seat_address())
            .unwrap()
            .expect("the refresh waits across the restart")
            .replacement_order,
        crate::store::ordered_replacement::ReplacementOrder::Refresh(caller.clone()),
        "the reopened store holds it with its caller"
    );

    // The new Nexus's start and the flow's rest carry it out, whichever
    // comes first: its order is read from the reopened Memory.
    fixture.nexus.recover_wakes();
    super::waking::report_claude_stop(&fixture, vec![]);
    let owed = fixture
        .nexus
        .store
        .successor_intent(&seat_address())
        .unwrap()
        .expect("the successor stays owed, its launch unconfirmed");
    assert_eq!(owed.predecessor.0, CLAUDE_SEAT);
    assert_eq!(
        owed.succession_cause,
        SuccessionCause::Ordered(caller.clone()),
        "the succession carries the caller of the Refresh that caused it"
    );

    reopen_store(&mut fixture);
    assert_eq!(
        fixture
            .nexus
            .store
            .successor_intent(&seat_address())
            .unwrap()
            .expect("the owed successor survives")
            .succession_cause,
        SuccessionCause::Ordered(caller),
        "the reopened store keeps it"
    );
}

/// A Refresh called while its flow rests carries its caller straight into
/// the succession it causes.
#[test]
fn an_immediate_refresh_records_its_caller_on_the_succession_it_causes() {
    use crate::waking::RefreshesMetaflows;
    let (fixture, _) = refreshable_with_staged_successor(
        Continuity::Maintained,
        flow_ethos::Delivery::Queue,
        false,
    );
    let caller = another_flows_caller();
    assert_eq!(
        fixture.nexus.refresh(&seat_address(), &caller),
        Output::RefreshRejected(RefreshRejection::SuccessorUnconfirmed)
    );
    assert_eq!(
        fixture
            .nexus
            .store
            .successor_intent(&seat_address())
            .unwrap()
            .expect("the successor stays owed")
            .succession_cause,
        SuccessionCause::Ordered(caller)
    );
}

/// primary-k7jy: a Refresh is bound to its caller. A flow of the Address
/// that is no longer the one it is Awake as, a past flow among them,
/// refreshes nothing: it is refused CallerNotCurrent, naming itself and
/// the flow the Address is Awake as, None while it is Asleep.
#[test]
fn a_flow_no_longer_current_is_refused_caller_not_current() {
    use crate::caller::ReadsRefreshCaller;
    use crate::generated::memory::RefreshCaller;
    use crate::waking::RefreshesMetaflows;
    let fixture = NexusFixture::new();
    fixture.accept_pane_operations(
        vec![claude_agent(&fixture)],
        PromptFixture::Prompted("w1:p3"),
    );
    claude_seat(&fixture, 40_000);
    let ended = flow_ethos::FlowId("0e068b".into());
    let current = flow_ethos::FlowId(CLAUDE_SEAT.into());
    fixture.nexus.store.seed_metaflow(Metaflow {
        address: seat_address(),
        state: State::Awake(current.clone()),
        past: flow_ethos::Past(vec![ended.clone()]),
        queue: flow_ethos::Queue(vec![]),
    });
    let caller = fixture.nexus.origin_refresh_caller(
        Origin::Flow(signal_flow::FlowOrigin {
            flow_id: ended.clone(),
            session_id: signal_flow::SessionId("0e068b-thread".into()),
        }),
        &seat_address(),
    );
    assert_eq!(caller, RefreshCaller::AddressFlow(ended.clone()));
    assert_eq!(
        fixture.nexus.refresh(&seat_address(), &caller),
        Output::RefreshRejected(RefreshRejection::CallerNotCurrent(
            signal_flow::CallerNotCurrent_Data {
                flow_id: ended.clone(),
                flow_id_option: Some(current.clone()),
            }
        ))
    );
    assert!(!closed(&fixture), "the current flow is untouched");
    assert_eq!(stored(&fixture).state, State::Awake(current));

    fixture.nexus.store.seed_metaflow(Metaflow {
        address: seat_address(),
        state: State::Asleep,
        past: flow_ethos::Past(vec![ended.clone()]),
        queue: flow_ethos::Queue(vec![]),
    });
    assert_eq!(
        fixture.nexus.refresh(&seat_address(), &caller),
        Output::RefreshRejected(RefreshRejection::CallerNotCurrent(
            signal_flow::CallerNotCurrent_Data {
                flow_id: ended,
                flow_id_option: None,
            }
        ))
    );
}

/// primary-k7jy: the current flow refreshes itself, and any other caller,
/// a flow of another Address or a process in no flow, refreshes the flow
/// the Address is Awake as.
#[test]
fn the_current_flow_and_any_other_caller_refresh_the_current_flow() {
    use crate::caller::ReadsRefreshCaller;
    use crate::generated::memory::RefreshCaller;
    use crate::waking::RefreshesMetaflows;
    let elsewhere = signal_flow::FlowOrigin {
        flow_id: flow_ethos::FlowId("c0ffee".into()),
        session_id: signal_flow::SessionId("c0ffee-session".into()),
    };
    for caller in [
        RefreshCaller::AddressFlow(flow_ethos::FlowId(CLAUDE_SEAT.into())),
        RefreshCaller::Elsewhere(Origin::Unbound),
        RefreshCaller::Elsewhere(Origin::Flow(elsewhere.clone())),
    ] {
        let (fixture, _) = refreshable_with_queue(Continuity::OnDemand, vec![], true);
        let refreshed = fixture.nexus.refresh(&seat_address(), &caller);
        assert!(
            matches!(&refreshed, Output::ReapPending(pending)
                if pending.predecessor.0.0 == CLAUDE_SEAT),
            "{caller:?}: {refreshed:?}"
        );
        super::reaping::finish_owned_reaps(&fixture);
        assert!(closed(&fixture));
        assert_eq!(
            stored(&fixture).state,
            State::Awake(flow_ethos::FlowId("908786".into()))
        );
    }
    // A flow of another Address is no flow of this one.
    let fixture = NexusFixture::new();
    awake_seat(&fixture, vec![]);
    assert_eq!(
        fixture
            .nexus
            .origin_refresh_caller(Origin::Flow(elsewhere.clone()), &seat_address()),
        RefreshCaller::Elsewhere(Origin::Flow(elsewhere))
    );
    assert_eq!(
        fixture
            .nexus
            .origin_refresh_caller(Origin::Unbound, &seat_address()),
        RefreshCaller::Elsewhere(Origin::Unbound)
    );
}
