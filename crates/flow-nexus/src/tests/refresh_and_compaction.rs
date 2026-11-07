//! A flow past its model's refresh threshold is given no new message and is
//! replaced as it goes idle, reaped in the same event; a compaction its
//! harness made while it kept working raises a warning.

use super::waking::{
    CLAUDE_SEAT, append_main_rows, claude_agent, claude_seat, claude_stop, claude_transcript,
    end_turn,
};
use super::{ControlsHerdrSnapshot, NexusFixture, PromptFixture, StagedLaunch};
use crate::compacting::{CodexContextSignal, WatchesCompactions, WritesTranscriptTime};
use crate::composition::{ComposesLaunch, HarnessedProfile};
use crate::fixture_executable::{FixtureExecutable, InstallsScript};
use crate::reporting::RecordsReports;
use crate::store::compaction::{Investigation, RecordsCompactions};
use crate::store::context::{JudgesThreshold, RecordsContextSize, ThresholdStanding};
use crate::store::metaflow::{SeedsMetaflow, SleepsMetaflows};
use crate::store::successor::{OwesSuccessors, SuccessionCause, SuccessorIntent};
use crate::store::{ManagesMetaflowState, ReadsFlowRows, SeedsRole};
use crate::waking::{AtStop, SleepsAndWakesSeats};
use flow_ethos::State;
use meta_signal_flow::{
    ConfigurationChange, ConfigureMetaflow, ConfigureModel, ConfigureThreshold, Continuity,
};
use signal_flow::{
    Address, Aspect, Caller, FlowLifecycle, Message, Metaflow, Model, Request, SentBy,
};
use std::fs;
use std::io::Write;
use std::time::Duration;

const CLAUDE_MODEL: &str = "claude-opus-5-5";

fn seat_address() -> Address {
    Address {
        aspect: Aspect::Mind,
        topic: "flow".into(),
        model: Model::Sol,
    }
}

/// The Claude seat runs `CLAUDE_MODEL`, whose refresh threshold is
/// `refresh` tokens.
pub(super) fn seat_refreshing_at(fixture: &NexusFixture, refresh: i64) {
    fixture.nexus.store.seed_role(Caller {
        flow_id: CLAUDE_SEAT.into(),
        aspect: Aspect::Mind,
        model_name: CLAUDE_MODEL.into(),
    });
    fixture
        .nexus
        .store
        .configure_change(ConfigurationChange::Thresholds(vec![ConfigureThreshold {
            model_name: CLAUDE_MODEL.into(),
            refresh,
        }]))
        .expect("threshold configured");
}

fn seat_metaflow(fixture: &NexusFixture, queue: Vec<Request>) {
    fixture.nexus.store.seed_metaflow(seat_metaflow_with(queue));
}

fn seat_metaflow_with(queue: Vec<Request>) -> Metaflow {
    Metaflow {
        address: seat_address(),
        state: State::Awake(CLAUDE_SEAT.into()),
        past: vec![],
        queue: queue
            .into_iter()
            .enumerate()
            .map(|(index, request)| signal_flow::QueueEntry {
                logical_delivery_id: format!("refresh-test-{index}"),
                sent_by: SentBy::Unknown,
                delivery: flow_ethos::Delivery::Queue,
                message: Message::Soft(request),
            })
            .collect(),
    }
}

fn seat_metaflow_now(fixture: &NexusFixture) -> Metaflow {
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

/// The launch request a refresh's successor is staged under; the intent the
/// tests owe names it, so the refresh resumes the staged launch.
pub(super) const SUCCESSOR_REQUEST: &str = "refresh-c1a0de";

/// Declares the seat's metaflow, continuing as `continuity`, and the model
/// its model and aspect run.
pub(super) fn declare_seat(
    fixture: &NexusFixture,
    continuity: Continuity,
    system_modules: Vec<String>,
) {
    fixture
        .nexus
        .store
        .configure_change(ConfigurationChange::Metaflows(vec![ConfigureMetaflow {
            root_option: None,
            address: seat_address(),
            system_modules,
            user_modules: vec!["operation-main-flow".into()],
            flow_kind: signal_flow::FlowKind::Regular,
            tool_set: signal_flow::ToolSet::Default,
            continuity,
            purpose: String::new(),
            records: vec![],
        }]))
        .expect("metaflow declared");
    fixture
        .nexus
        .store
        .configure_change(ConfigurationChange::Models(vec![ConfigureModel {
            model: Model::Sol,
            native: "fixture-model".into(),
        }]))
        .expect("successor model configured");
}

/// The successor a refresh of the seat launches, staged as far as its
/// ambiguous first prompt, whose receipt the fixture then writes: the
/// refresh's Replace resumes it and finds it confirmed.
pub(super) fn staged_refresh_successor(fixture: &NexusFixture) -> StagedLaunch {
    let mut successor = fixture.staged_launch(SUCCESSOR_REQUEST, Some(CLAUDE_SEAT));
    successor.profile.address = seat_address();
    successor.profile.model_name = "fixture-model".into();
    successor.profile.herdr_session_name = "default".into();
    successor.profile.remembering = signal_flow::Remembering::Predecessors(1);
    successor.profile.instruction_prompt.clear();
    successor.composed = fixture
        .nexus
        .composer
        .compose(
            HarnessedProfile {
                remembered_flows: Vec::new(),
                harness_kind: signal_flow::HarnessKind::Codex,
                launch_profile: successor.profile.clone(),
            },
            Vec::new(),
        )
        .expect("refresh successor composes");
    successor
}

/// The refresh's successor is owed under the staged launch's request.
pub(super) fn owe_staged_successor(fixture: &NexusFixture) {
    fixture
        .nexus
        .store
        .owe_successor(SuccessorIntent {
            address: seat_address(),
            predecessor: CLAUDE_SEAT.into(),
            cause: SuccessionCause::Refresh,
            launch_request_id: SUCCESSOR_REQUEST.into(),
        })
        .expect("successor owed");
}

fn lifecycle(fixture: &NexusFixture, flow_id: &str) -> FlowLifecycle {
    fixture
        .nexus
        .store
        .flow_node(flow_id)
        .unwrap()
        .expect("the flow's row is kept")
        .flow_lifecycle
}

/// The threshold belongs to the model: a flow's last measured context is
/// judged against the threshold configured for the model it runs, and a
/// model with none configured is never refreshed.
#[test]
fn a_flow_s_context_is_judged_against_its_model_s_threshold() {
    let fixture = NexusFixture::new();
    claude_seat(&fixture, 150_000);
    assert_eq!(
        fixture.nexus.store.threshold_standing(CLAUDE_SEAT).unwrap(),
        ThresholdStanding::NoThreshold,
        "Flow knows no model for the seat"
    );
    seat_refreshing_at(&fixture, 200_000);
    assert_eq!(
        fixture.nexus.store.threshold_standing(CLAUDE_SEAT).unwrap(),
        ThresholdStanding::Unmeasured
    );
    fixture
        .nexus
        .store
        .record_context_size(CLAUDE_SEAT, 199_999)
        .unwrap();
    assert!(!fixture.nexus.store.past_threshold(CLAUDE_SEAT).unwrap());
    fixture
        .nexus
        .store
        .record_context_size(CLAUDE_SEAT, 200_000)
        .unwrap();
    assert!(fixture.nexus.store.past_threshold(CLAUDE_SEAT).unwrap());
    // Another model's threshold is not this flow's.
    fixture
        .nexus
        .store
        .configure_change(ConfigurationChange::Thresholds(vec![ConfigureThreshold {
            model_name: "gpt-6.1-sol".into(),
            refresh: 150_000,
        }]))
        .unwrap();
    assert_eq!(
        fixture.nexus.store.threshold_standing(CLAUDE_SEAT).unwrap(),
        ThresholdStanding::NoThreshold
    );
}

/// A seat that rests past its model's threshold is told nothing and handed
/// nothing: its pane is closed and its flow recorded Stopped, its metaflow
/// Asleep with the flow in Past, in the same event, and the request queued
/// for it stays queued for the metaflow's next flow.
#[test]
fn a_seat_resting_past_its_threshold_is_reaped_and_its_work_kept_for_the_next_flow() {
    let fixture = NexusFixture::new();
    fixture.accept_pane_operations(
        vec![claude_agent(&fixture)],
        PromptFixture::Prompted("w1:p3"),
    );
    claude_seat(&fixture, 230_000);
    append_main_rows(&fixture, &[end_turn("last-turn")]);
    seat_refreshing_at(&fixture, 200_000);
    declare_seat(
        &fixture,
        Continuity::OnDemand,
        vec!["spirit-core".into(), "intent-flow".into()],
    );
    seat_metaflow(
        &fixture,
        vec![Request::Order("Held for the next flow.".into())],
    );

    fixture
        .nexus
        .after_report(&CLAUDE_SEAT.to_owned(), &claude_stop(&fixture));
    assert_eq!(
        fixture.nexus.store.context_size(CLAUDE_SEAT).unwrap(),
        Some(230_000)
    );

    assert!(closed(&fixture), "the seat's pane is closed");
    assert!(
        fixture.typed().is_none(),
        "nothing is typed into a seat past its threshold"
    );
    let node = fixture
        .nexus
        .store
        .flow_node(CLAUDE_SEAT)
        .unwrap()
        .expect("the flow's row is kept");
    assert_eq!(node.flow_lifecycle, FlowLifecycle::Stopped);
    let metaflow = seat_metaflow_now(&fixture);
    assert_eq!(metaflow.state, State::Asleep);
    assert_eq!(metaflow.past, vec![CLAUDE_SEAT.to_owned()]);
    assert_eq!(
        metaflow
            .queue
            .iter()
            .map(|queued| match &queued.message {
                Message::HardAbrupt(request)
                | Message::MiddleAbrupt(request)
                | Message::Soft(request) => request,
            })
            .collect::<Vec<_>>(),
        [&Request::Order("Held for the next flow.".into())],
        "the work waits for the next flow"
    );
}

/// A seat past its threshold is refreshed at its Stop hook though Herdr
/// shows its pane Working and its transcript shows no ended turn: the Stop
/// is the idle fact, and Herdr's pane state never holds a refresh.
#[test]
fn a_seat_past_its_threshold_shown_working_is_refreshed_at_its_stop() {
    let fixture = NexusFixture::new();
    fixture.accept_pane_operations(
        vec![{
            let mut agent = claude_agent(&fixture);
            agent["agent_status"] = serde_json::Value::String("working".into());
            agent
        }],
        PromptFixture::Prompted("w1:p3"),
    );
    claude_seat(&fixture, 230_000);
    seat_refreshing_at(&fixture, 200_000);
    declare_seat(
        &fixture,
        Continuity::OnDemand,
        vec!["spirit-core".into(), "intent-flow".into()],
    );
    seat_metaflow(&fixture, vec![]);

    fixture
        .nexus
        .after_report(&CLAUDE_SEAT.to_owned(), &claude_stop(&fixture));

    assert!(closed(&fixture), "the working seat's pane is closed");
    assert_eq!(lifecycle(&fixture, CLAUDE_SEAT), FlowLifecycle::Stopped);
    let metaflow = seat_metaflow_now(&fixture);
    assert_eq!(metaflow.state, State::Asleep);
    assert_eq!(metaflow.past, vec![CLAUDE_SEAT.to_owned()]);
}

/// An OnDemand seat resting past its threshold whose successor cannot be
/// composed is not closed: its Address keeps its flow, which is handed what
/// is queued for it, and the refusal is told to no flow.
#[test]
fn a_seat_whose_successor_cannot_compose_stays_and_is_handed_its_queue() {
    let fixture = NexusFixture::new();
    fixture.accept_pane_operations(
        vec![claude_agent(&fixture)],
        PromptFixture::Prompted("w1:p3"),
    );
    claude_seat(&fixture, 230_000);
    append_main_rows(&fixture, &[end_turn("last-turn")]);
    seat_refreshing_at(&fixture, 200_000);
    // No declaration and no launch of the seat's names a successor.
    seat_metaflow(&fixture, vec![Request::Order("Still yours.".into())]);
    // The Astra of the seat's aspect and topic, and the Psyche core Opus
    // a refused successor was once told to, are held: neither is told.
    let held = [
        signal_flow::Address {
            model: Model::Astra,
            ..seat_address()
        },
        signal_flow::Address {
            aspect: Aspect::Psyche,
            topic: "core".into(),
            model: Model::Opus,
        },
    ];
    for address in &held {
        fixture.nexus.store.seed_metaflow(Metaflow {
            address: address.clone(),
            state: State::Asleep,
            past: vec![],
            queue: vec![],
        });
    }

    fixture
        .nexus
        .after_report(&CLAUDE_SEAT.to_owned(), &claude_stop(&fixture));

    assert!(!closed(&fixture), "the seat's pane stays open");
    assert!(
        fixture
            .typed()
            .is_some_and(|typed| typed.contains("Still yours.")),
        "the kept seat is handed its queue"
    );
    assert_eq!(
        seat_metaflow_now(&fixture).state,
        State::Awake(CLAUDE_SEAT.into())
    );
    // The refusal is told to no flow: no message is queued anywhere, and
    // only the seat's own metaflow and the two held are Flow's.
    let metaflows = crate::store::ManagesMetaflowState::list_metaflows(&fixture.nexus.store)
        .expect("metaflows list");
    assert_eq!(metaflows.len(), 3, "{metaflows:?}");
    for address in &held {
        let metaflow = fixture
            .nexus
            .store
            .metaflow(address)
            .unwrap()
            .expect("the held metaflow");
        assert!(metaflow.queue.is_empty(), "{address:?} is told nothing");
    }
}

/// An idle OnDemand seat with nothing queued closes its harness either way:
/// past its threshold it is refreshed, below it it sleeps, its metaflow
/// Asleep until work wakes its next flow.
#[test]
fn an_idle_on_demand_seat_is_refreshed_only_past_its_threshold() {
    for (tokens, refreshed) in [(199_999, false), (200_000, true)] {
        let fixture = NexusFixture::new();
        fixture.accept_pane_operations(
            vec![claude_agent(&fixture)],
            PromptFixture::Prompted("w1:p3"),
        );
        claude_seat(&fixture, tokens);
        append_main_rows(&fixture, &[end_turn("last-turn")]);
        seat_refreshing_at(&fixture, 200_000);
        declare_seat(
            &fixture,
            Continuity::OnDemand,
            vec!["spirit-core".into(), "intent-flow".into()],
        );
        seat_metaflow(&fixture, vec![]);
        fixture
            .nexus
            .store
            .record_context_size(CLAUDE_SEAT, tokens)
            .unwrap();
        let at_stop = fixture.nexus.settle_stopped(CLAUDE_SEAT);
        if refreshed {
            assert_eq!(at_stop, AtStop::Refreshed(seat_address()));
        } else {
            assert_eq!(at_stop, AtStop::Slept(seat_address()));
        }
        assert_eq!(seat_metaflow_now(&fixture).state, State::Asleep);
        assert!(closed(&fixture), "{tokens}");
    }
}

/// A Maintained metaflow is never left without a flow: its seat resting
/// past its threshold with nothing queued has its successor started and
/// confirmed first; only then is the predecessor recorded Stopped and its
/// pane closed, and the owed successor is given in the write that confirms
/// it.
#[test]
fn a_maintained_seat_refreshed_with_nothing_queued_starts_its_successor_first() {
    let fixture = NexusFixture::new();
    claude_seat(&fixture, 200_000);
    append_main_rows(&fixture, &[end_turn("last-turn")]);
    seat_refreshing_at(&fixture, 200_000);
    seat_metaflow(&fixture, vec![]);
    let mut successor = staged_refresh_successor(&fixture);
    successor.stage_to_ambiguity(&fixture.nexus);
    successor.write_receipt();
    declare_seat(
        &fixture,
        Continuity::Maintained,
        vec!["spirit-core".into(), "intent-flow".into()],
    );
    seat_metaflow(&fixture, vec![]);
    owe_staged_successor(&fixture);
    fixture.herdr_for_replacement_showing(0, vec![claude_agent(&fixture)]);

    assert_eq!(
        fixture.nexus.settle_stopped(CLAUDE_SEAT),
        AtStop::Refreshed(seat_address())
    );
    let metaflow = seat_metaflow_now(&fixture);
    assert_eq!(metaflow.state, State::Awake("908786".into()));
    assert_eq!(metaflow.past, vec![CLAUDE_SEAT.to_owned()]);
    assert_eq!(lifecycle(&fixture, CLAUDE_SEAT), FlowLifecycle::Stopped);
    assert!(closed(&fixture), "the predecessor's pane is closed");
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

/// A Maintained Claude seat past its threshold is replaced at its Stop even
/// while Herdr calls it Working. The confirmed successor exists before the
/// predecessor's pane close completes; after it completes, the predecessor
/// is Stopped and in Past and the metaflow is Awake as the successor.
#[test]
fn a_maintained_working_seat_refreshes_successor_first_at_its_stop() {
    let fixture = NexusFixture::new();
    claude_seat(&fixture, 230_000);
    seat_refreshing_at(&fixture, 200_000);
    let mut successor = staged_refresh_successor(&fixture);
    successor.stage_to_ambiguity(&fixture.nexus);
    successor.write_receipt();
    declare_seat(
        &fixture,
        Continuity::Maintained,
        vec!["spirit-core".into(), "intent-flow".into()],
    );
    seat_metaflow(&fixture, vec![]);
    owe_staged_successor(&fixture);
    let mut working = claude_agent(&fixture);
    working["agent_status"] = serde_json::Value::String("working".into());
    fixture.herdr_for_replacement_showing(0, vec![working]);

    // Hold the predecessor's pane close so the store can be observed at the
    // exact boundary: the successor is registered before close completes.
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

    let (at_stop, lifecycles) = std::thread::scope(|scope| {
        let stopping = scope.spawn(|| fixture.nexus.settle_stopped(CLAUDE_SEAT));
        let deadline = std::time::Instant::now() + Duration::from_secs(20);
        while !closing.exists() && std::time::Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(10));
        }
        let lifecycles = closing.exists().then(|| {
            (
                lifecycle(&fixture, "908786"),
                lifecycle(&fixture, CLAUDE_SEAT),
            )
        });
        // Release before any assertion or join: a failed observation can
        // never strand the scoped worker inside the fixture Herdr.
        fs::write(&release, "").expect("predecessor close released");
        (stopping.join().expect("Stop settled"), lifecycles)
    });

    let (successor_lifecycle, predecessor_lifecycle) =
        lifecycles.expect("the reap reached close before its bounded deadline");
    assert_eq!(
        successor_lifecycle,
        FlowLifecycle::Active,
        "the confirmed successor exists before predecessor close completes"
    );
    assert_eq!(
        predecessor_lifecycle,
        FlowLifecycle::Stopped,
        "the predecessor stops receiving before its pane closes"
    );
    assert_eq!(at_stop, AtStop::Refreshed(seat_address()));
    let metaflow = seat_metaflow_now(&fixture);
    assert_eq!(metaflow.state, State::Awake("908786".into()));
    assert_eq!(metaflow.past, vec![CLAUDE_SEAT.to_owned()]);
    assert_eq!(lifecycle(&fixture, CLAUDE_SEAT), FlowLifecycle::Stopped);
    assert!(closed(&fixture), "the predecessor's pane is closed");
}

/// An OnDemand metaflow holding a request that wakes it is refreshed
/// successor first too: the successor is confirmed before the predecessor
/// is reaped, and the predecessor is handed none of the work.
#[test]
fn an_on_demand_seat_refreshed_with_a_waking_request_starts_its_successor_first() {
    let fixture = NexusFixture::new();
    claude_seat(&fixture, 230_000);
    append_main_rows(&fixture, &[end_turn("last-turn")]);
    seat_refreshing_at(&fixture, 200_000);
    let mut successor = staged_refresh_successor(&fixture);
    successor.stage_to_ambiguity(&fixture.nexus);
    successor.write_receipt();
    declare_seat(
        &fixture,
        Continuity::OnDemand,
        vec!["spirit-core".into(), "intent-flow".into()],
    );
    let mut waking = seat_metaflow_with(vec![Request::Order("For the successor.".into())]);
    waking.queue[0].delivery = flow_ethos::Delivery::Wake;
    waking.queue[0].message = Message::MiddleAbrupt(Request::Order("For the successor.".into()));
    fixture.nexus.store.seed_metaflow(waking);
    owe_staged_successor(&fixture);
    fixture.herdr_for_replacement_showing(0, vec![claude_agent(&fixture)]);

    assert_eq!(
        fixture.nexus.settle_stopped(CLAUDE_SEAT),
        AtStop::Refreshed(seat_address())
    );
    assert_eq!(lifecycle(&fixture, CLAUDE_SEAT), FlowLifecycle::Stopped);
    assert!(closed(&fixture), "the predecessor's pane is closed");
    let metaflow = seat_metaflow_now(&fixture);
    assert_eq!(metaflow.state, State::Awake("908786".into()));
    assert!(
        fixture.typed().is_none(),
        "nothing is typed into the predecessor"
    );
}

/// A successor whose launch is refused leaves the predecessor Awake and
/// live, its pane open; the refused launch is not owed again under the same
/// request.
#[test]
fn a_refused_successor_launch_keeps_the_predecessor() {
    let fixture = NexusFixture::new();
    fixture.accept_pane_operations(
        vec![claude_agent(&fixture)],
        PromptFixture::Prompted("w1:p3"),
    );
    claude_seat(&fixture, 200_000);
    append_main_rows(&fixture, &[end_turn("last-turn")]);
    seat_refreshing_at(&fixture, 200_000);
    seat_metaflow(&fixture, vec![]);
    // The declared module is absent from the verified fixture context, so
    // the successor launch is refused before any native seat starts.
    declare_seat(
        &fixture,
        Continuity::Maintained,
        vec!["missing-module".into()],
    );

    assert!(matches!(
        fixture.nexus.settle_stopped(CLAUDE_SEAT),
        AtStop::Kept(crate::waking::KeptAtStop::RefreshLaunchRefused(_))
    ));
    assert!(!closed(&fixture));
    assert_eq!(lifecycle(&fixture, CLAUDE_SEAT), FlowLifecycle::Active);
    assert_eq!(
        seat_metaflow_now(&fixture).state,
        State::Awake(CLAUDE_SEAT.into())
    );
    assert_eq!(
        fixture
            .nexus
            .store
            .successor_intent(&seat_address())
            .unwrap(),
        None
    );
}

/// A Codex thread's context is measured from its app-server's token usage,
/// as a Claude seat's is from its transcript at each Stop.
#[test]
fn a_codex_flow_s_context_is_its_thread_s_last_input() {
    let fixture = NexusFixture::new();
    fixture.accept_pane_operations(
        vec![fixture.settled_agent()],
        PromptFixture::Prompted("w1:p3"),
    );
    fixture.register_with(FlowLifecycle::Active);
    let thread = fixture.node().session_id;
    fixture
        .nexus
        .codex_context_signal(&thread, CodexContextSignal::TokenUsage(160_000));
    assert_eq!(
        fixture.nexus.store.context_size("908786").unwrap(),
        Some(160_000)
    );
    // Another thread's usage is not this flow's.
    fixture
        .nexus
        .codex_context_signal("another-thread", CodexContextSignal::TokenUsage(5));
    assert_eq!(
        fixture.nexus.store.context_size("908786").unwrap(),
        Some(160_000)
    );
}

/// A Codex seat due a refresh remains live while its app-server thread is in
/// a turn. Its turn completion supplies the rest fact and only then may the
/// predecessor close and move to Past.
#[test]
fn a_codex_seat_due_a_refresh_waits_for_its_thread_to_rest() {
    use crate::codex_turns::{ThreadStatus, TracksCodexTurns, TurnEvent};

    let fixture = NexusFixture::new();
    fixture.accept_pane_operations(
        vec![fixture.settled_agent()],
        PromptFixture::Prompted("w1:p3"),
    );
    fixture.register_with(FlowLifecycle::Active);
    fixture.nexus.store.seed_role(Caller {
        flow_id: "908786".into(),
        aspect: Aspect::Mind,
        model_name: "fixture-model".into(),
    });
    fixture
        .nexus
        .store
        .configure_change(ConfigurationChange::Thresholds(vec![ConfigureThreshold {
            model_name: "fixture-model".into(),
            refresh: 200_000,
        }]))
        .expect("threshold configured");
    fixture
        .nexus
        .store
        .record_context_size("908786", 230_000)
        .expect("context recorded");
    declare_seat(
        &fixture,
        Continuity::OnDemand,
        vec!["spirit-core".into(), "intent-flow".into()],
    );
    fixture.nexus.store.seed_metaflow(Metaflow {
        address: seat_address(),
        state: State::Awake("908786".into()),
        past: vec![],
        queue: vec![],
    });
    let thread = fixture.node().session_id;
    fixture
        .nexus
        .codex_turns
        .observe(&thread, TurnEvent::Started("turn-1".into()));

    assert_eq!(
        fixture.nexus.settle_stopped("908786"),
        AtStop::Kept(crate::waking::KeptAtStop::InTurn)
    );
    assert_eq!(lifecycle(&fixture, "908786"), FlowLifecycle::Active);
    assert!(!closed(&fixture), "an in-turn Codex pane stays open");

    fixture
        .nexus
        .codex_turns
        .observe(&thread, TurnEvent::Status(ThreadStatus::Idle));
    // An idle status cannot overtake a named running turn.
    assert_eq!(
        fixture.nexus.settle_stopped("908786"),
        AtStop::Kept(crate::waking::KeptAtStop::InTurn)
    );
    fixture
        .nexus
        .codex_turns
        .observe(&thread, TurnEvent::Completed("turn-1".into()));
    assert_eq!(
        fixture.nexus.settle_stopped("908786"),
        AtStop::Refreshed(seat_address())
    );
    assert!(closed(&fixture), "the resting Codex pane may now close");
    assert_eq!(lifecycle(&fixture, "908786"), FlowLifecycle::Stopped);
    let metaflow = seat_metaflow_now(&fixture);
    assert_eq!(metaflow.state, State::Asleep);
    assert_eq!(metaflow.past, vec!["908786".to_owned()]);
}

fn now_transcript_time() -> String {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        .transcript_time()
}

fn append_boundary(fixture: &NexusFixture, uuid: &str, trigger: &str, timestamp: &str) {
    let mut file = fs::OpenOptions::new()
        .append(true)
        .open(claude_transcript(fixture))
        .unwrap();
    writeln!(
        file,
        "{}",
        serde_json::json!({
            "type": "system",
            "subtype": "compact_boundary",
            "uuid": uuid,
            "timestamp": timestamp,
            "isSidechain": false,
            "content": "Conversation compacted",
            "compactMetadata": {"trigger": trigger, "preTokens": 219_141}
        })
    )
    .unwrap();
}

/// A compaction the harness made on its own, while the flow kept working,
/// raises a warning once; one the flow was asked for raises nothing, and
/// one made before this Nexus watched the transcript raises nothing.
#[test]
fn a_compaction_during_continuous_work_is_recorded_once_without_an_order() {
    let fixture = NexusFixture::new();
    claude_seat(&fixture, 40_000);
    seat_metaflow(&fixture, vec![]);
    append_boundary(&fixture, "before-watch", "auto", "2020-01-01T00:00:00.000Z");
    append_boundary(&fixture, "asked", "manual", &now_transcript_time());
    append_boundary(&fixture, "continuous", "auto", &now_transcript_time());

    let started = signal_flow::Event::Started(None);
    fixture
        .nexus
        .after_report(&CLAUDE_SEAT.to_owned(), &started);
    fixture
        .nexus
        .after_report(&CLAUDE_SEAT.to_owned(), &started);

    let compactions = fixture.nexus.store.compactions_of(CLAUDE_SEAT).unwrap();
    assert_eq!(
        compactions
            .iter()
            .map(|compaction| compaction.boundary.as_str())
            .collect::<Vec<_>>(),
        ["continuous"]
    );
    assert_eq!(compactions[0].investigation, Investigation::NotConfigured);
    assert_eq!(compactions[0].address, Some(seat_address()));
}

/// With no Investigation seat configured the warning is logged and the
/// compaction recorded, and nothing else is started.
#[test]
fn a_compaction_with_no_investigation_seat_is_recorded_not_investigated() {
    let fixture = NexusFixture::new();
    claude_seat(&fixture, 40_000);
    seat_metaflow(&fixture, vec![]);
    append_boundary(&fixture, "continuous", "auto", &now_transcript_time());
    fixture
        .nexus
        .after_report(&CLAUDE_SEAT.to_owned(), &signal_flow::Event::Started(None));
    let compactions = fixture.nexus.store.compactions_of(CLAUDE_SEAT).unwrap();
    assert_eq!(compactions.len(), 1);
    assert_eq!(compactions[0].investigation, Investigation::NotConfigured);
}

/// A Codex compaction a Compact asked for is not continuous work; one the
/// thread made on its own is.
#[test]
fn a_codex_compaction_is_raised_unless_a_compact_asked_for_it() {
    let fixture = NexusFixture::new();
    fixture.accept_pane_operations(
        vec![fixture.settled_agent()],
        PromptFixture::Prompted("w1:p3"),
    );
    fixture.register_with(FlowLifecycle::Active);
    let thread = fixture.node().session_id;
    fixture.nexus.compaction_requested("908786");
    fixture
        .nexus
        .codex_context_signal(&thread, CodexContextSignal::Compacted("item-asked".into()));
    assert!(
        fixture
            .nexus
            .store
            .compactions_of("908786")
            .unwrap()
            .is_empty()
    );
    fixture
        .nexus
        .codex_context_signal(&thread, CodexContextSignal::Compacted("item-own".into()));
    let compactions = fixture.nexus.store.compactions_of("908786").unwrap();
    assert_eq!(
        compactions
            .iter()
            .map(|compaction| compaction.boundary.as_str())
            .collect::<Vec<_>>(),
        ["item-own"]
    );
}

/// A successor that is never confirmed leaves its predecessor Active, its
/// pane open and its metaflow Awake as it, the successor still owed.
#[test]
fn unconfirmed_successor_keeps_predecessor_unreaped() {
    let fixture = NexusFixture::new();
    fixture.accept_pane_operations(
        vec![claude_agent(&fixture)],
        PromptFixture::Prompted("w1:p3"),
    );
    claude_seat(&fixture, 200_000);
    append_main_rows(&fixture, &[end_turn("last-turn")]);
    seat_refreshing_at(&fixture, 200_000);
    seat_metaflow(&fixture, vec![]);
    let mut successor = staged_refresh_successor(&fixture);
    successor.stage_to_ambiguity(&fixture.nexus);
    // No write_receipt: the successor's first turn is never authenticated.
    declare_seat(
        &fixture,
        Continuity::Maintained,
        vec!["spirit-core".into(), "intent-flow".into()],
    );
    seat_metaflow(&fixture, vec![]);
    owe_staged_successor(&fixture);
    fixture.herdr_for_replacement_showing(0, vec![claude_agent(&fixture)]);

    let at_stop = fixture.nexus.settle_stopped(CLAUDE_SEAT);
    assert!(
        matches!(
            at_stop,
            AtStop::Kept(crate::waking::KeptAtStop::RefreshLaunchRefused(_))
        ),
        "{at_stop:?}"
    );
    assert!(!closed(&fixture), "the predecessor's pane stays open");
    assert_eq!(lifecycle(&fixture, CLAUDE_SEAT), FlowLifecycle::Active);
    assert_eq!(
        seat_metaflow_now(&fixture).state,
        State::Awake(CLAUDE_SEAT.into())
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
}

const FABLE_MODEL: &str = "claude-fable-5";

fn fable_address(topic: &str) -> Address {
    Address {
        aspect: Aspect::Psyche,
        topic: topic.into(),
        model: Model::Fable,
    }
}

/// The Claude seat runs `FABLE_MODEL` past its 200 000 token threshold,
/// Awake at `{ Psyche core Fable }` with one Order queued, its metaflow
/// declared OnDemand; `protected` is the configured ProtectedAddresses.
fn fable_seat_past_its_threshold(fixture: &NexusFixture, protected: Vec<Address>) {
    fixture.accept_pane_operations(
        vec![claude_agent(fixture)],
        PromptFixture::Prompted("w1:p3"),
    );
    claude_seat(fixture, 230_000);
    append_main_rows(fixture, &[end_turn("last-turn")]);
    fixture.nexus.store.seed_role(Caller {
        flow_id: CLAUDE_SEAT.into(),
        aspect: Aspect::Psyche,
        model_name: FABLE_MODEL.into(),
    });
    for change in [
        ConfigurationChange::Thresholds(vec![ConfigureThreshold {
            model_name: FABLE_MODEL.into(),
            refresh: 200_000,
        }]),
        ConfigurationChange::Metaflows(vec![ConfigureMetaflow {
            root_option: None,
            address: fable_address("core"),
            system_modules: vec!["spirit-core".into()],
            user_modules: vec!["operation-main-flow".into()],
            flow_kind: signal_flow::FlowKind::Regular,
            tool_set: signal_flow::ToolSet::Default,
            continuity: Continuity::OnDemand,
            purpose: String::new(),
            records: vec![],
        }]),
        ConfigurationChange::Models(vec![ConfigureModel {
            model: Model::Fable,
            native: FABLE_MODEL.into(),
        }]),
        ConfigurationChange::ProtectedAddresses(protected),
    ] {
        fixture
            .nexus
            .store
            .configure_change(change)
            .expect("configured");
    }
    let mut metaflow = seat_metaflow_with(vec![Request::Order("Kept for this flow.".into())]);
    metaflow.address = fable_address("core");
    fixture.nexus.store.seed_metaflow(metaflow);
    fixture
        .nexus
        .store
        .record_context_size(CLAUDE_SEAT, 230_000)
        .unwrap();
}

/// A protected Address's flow past its model's threshold stands Protected:
/// it is not past it for delivery, a Compact of it is refused as below it,
/// and at its Stop it is neither closed nor stopped, its metaflow Awake as
/// it and its queued work handed to it.
#[test]
fn a_protected_seat_past_its_threshold_is_kept_and_handed_its_work() {
    use crate::store::context::JudgesCompaction;
    let fixture = NexusFixture::new();
    fable_seat_past_its_threshold(&fixture, vec![fable_address("core")]);
    assert_eq!(
        fixture.nexus.store.threshold_standing(CLAUDE_SEAT).unwrap(),
        ThresholdStanding::Protected {
            context: 230_000,
            refresh: 200_000
        }
    );
    assert!(!fixture.nexus.store.past_threshold(CLAUDE_SEAT).unwrap());
    assert_eq!(
        fixture.nexus.store.compaction_refusal(CLAUDE_SEAT).unwrap(),
        Some(meta_signal_flow::CompactionRefusal::BelowThreshold(
            meta_signal_flow::ContextBelowThreshold {
                context_tokens: 230_000,
                refresh_tokens: 200_000,
            }
        ))
    );

    fixture
        .nexus
        .after_report(&CLAUDE_SEAT.to_owned(), &claude_stop(&fixture));

    assert!(!closed(&fixture), "the protected seat's pane stays");
    assert_eq!(lifecycle(&fixture, CLAUDE_SEAT), FlowLifecycle::Active);
    let metaflow = fixture
        .nexus
        .store
        .metaflow(&fable_address("core"))
        .unwrap()
        .expect("metaflow held");
    assert_eq!(metaflow.state, State::Awake(CLAUDE_SEAT.into()));
    assert!(metaflow.past.is_empty());
    assert!(
        fixture
            .typed()
            .is_some_and(|typed| typed.contains("Kept for this flow.")),
        "the queued work is handed to the protected seat: {:?}",
        fixture.typed()
    );
}

/// A Fable seat whose Address is not protected is refreshed past its
/// threshold as any seat is, though another Address is protected: its pane
/// closed, its flow Stopped, its metaflow Asleep with its work kept.
#[test]
fn an_unprotected_fable_seat_past_its_threshold_is_refreshed() {
    let fixture = NexusFixture::new();
    fable_seat_past_its_threshold(&fixture, vec![fable_address("other")]);
    assert_eq!(
        fixture.nexus.store.threshold_standing(CLAUDE_SEAT).unwrap(),
        ThresholdStanding::Past {
            context: 230_000,
            refresh: 200_000
        }
    );

    fixture
        .nexus
        .after_report(&CLAUDE_SEAT.to_owned(), &claude_stop(&fixture));

    assert!(closed(&fixture), "the seat's pane is closed");
    assert!(fixture.typed().is_none(), "nothing is typed into it");
    assert_eq!(lifecycle(&fixture, CLAUDE_SEAT), FlowLifecycle::Stopped);
    let metaflow = fixture
        .nexus
        .store
        .metaflow(&fable_address("core"))
        .unwrap()
        .expect("metaflow held");
    assert_eq!(metaflow.state, State::Asleep);
    assert_eq!(metaflow.past, vec![CLAUDE_SEAT.to_owned()]);
    assert_eq!(metaflow.queue.len(), 1, "the work waits for the next flow");
}
