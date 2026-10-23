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
use crate::reporting::RecordsReports;
use crate::store::compaction::{Investigation, RecordsCompactions};
use crate::store::context::{JudgesThreshold, RecordsContextSize, ThresholdStanding};
use crate::store::metaflow::{SeedsMetaflow, SleepsMetaflows};
use crate::store::metaflow_declaration::ReadsMetaflowDeclarations;
use crate::store::{ManagesMetaflowState, NamesConfiguredModel, ReadsFlowRows, SeedsRole};
use crate::waking::{AtStop, SleepsAndWakesSeats};
use flow_ethos::State;
use meta_signal_flow::{
    ConfigurationChange, ConfigureMetaflow, ConfigureModel, ConfigureThreshold,
};
use signal_flow::{
    Address, Aspect, Caller, Delivery, FlowLifecycle, Layer, Message, Metaflow, PowerLevel,
    Request, SentBy,
};
use std::fs;
use std::io::Write;

const CLAUDE_MODEL: &str = "claude-opus-5-5";

fn seat_address() -> Address {
    Address {
        aspect: Aspect::Mind,
        topic: "flow".into(),
        layer: Layer::Secondary,
    }
}

/// The Claude seat runs `CLAUDE_MODEL`, whose refresh threshold is
/// `refresh` tokens.
fn seat_refreshing_at(fixture: &NexusFixture, refresh: i64) {
    fixture.nexus.store.seed_role(Caller {
        flow_id: CLAUDE_SEAT.into(),
        aspect: Aspect::Mind,
        power_level: PowerLevel::Medium,
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
    fixture.nexus.store.seed_metaflow(Metaflow {
        address: seat_address(),
        state: State::Awake(CLAUDE_SEAT.into()),
        past: vec![],
        queue: queue
            .into_iter()
            .enumerate()
            .map(|(index, request)| signal_flow::QueueEntry {
                logical_delivery_id: format!("refresh-test-{index}"),
                sent_by: SentBy::Unknown,
                delivery: Delivery::Queue,
                message: Message::Soft(request),
            })
            .collect(),
    });
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

fn declare_refresh_successor(fixture: &NexusFixture) {
    fixture
        .nexus
        .store
        .configure_change(ConfigurationChange::Metaflows(vec![ConfigureMetaflow {
            address: seat_address(),
            system_modules: vec!["spirit-core".into(), "intent-flow".into()],
            user_modules: vec!["operation-main-flow".into()],
            flow_kind: signal_flow::FlowKind::Regular,
            tool_set: signal_flow::ToolSet::Default,
        }]))
        .expect("metaflow declared");
    fixture
        .nexus
        .store
        .configure_change(ConfigurationChange::Models(vec![ConfigureModel {
            layer: Layer::Secondary,
            aspect: Aspect::Mind,
            native: "fixture-model".into(),
        }]))
        .expect("successor model configured");
}

fn staged_refresh_successor(fixture: &NexusFixture) -> StagedLaunch {
    let mut successor = fixture.staged_launch("refresh-c1a0de", Some(CLAUDE_SEAT));
    successor.profile.address = seat_address();
    successor.profile.model_name = "fixture-model".into();
    successor.profile.herdr_session_name = "default".into();
    successor.profile.instruction_prompt.clear();
    successor.composed = fixture
        .nexus
        .composer
        .compose(
            HarnessedProfile {
                harness_kind: signal_flow::HarnessKind::Codex,
                launch_profile: successor.profile.clone(),
            },
            Vec::new(),
        )
        .expect("refresh successor composes");
    successor
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

/// Work already queued at refresh stays durable while the successor starts;
/// the predecessor receives none of it.
#[test]
fn a_seat_resting_past_its_threshold_is_reaped_and_its_work_kept_for_the_next_flow() {
    let fixture = NexusFixture::new();
    claude_seat(&fixture, 230_000);
    append_main_rows(&fixture, &[end_turn("last-turn")]);
    seat_refreshing_at(&fixture, 200_000);
    seat_metaflow(
        &fixture,
        vec![Request::Order("Held for the next flow.".into())],
    );
    let mut successor = staged_refresh_successor(&fixture);
    successor.stage_to_ambiguity(&fixture.nexus);
    successor.write_receipt();
    declare_refresh_successor(&fixture);
    seat_metaflow(
        &fixture,
        vec![Request::Order("Held for the next flow.".into())],
    );
    assert!(
        fixture
            .nexus
            .store
            .metaflow_declaration(&seat_address())
            .unwrap()
            .is_some()
    );
    assert_eq!(
        fixture
            .nexus
            .store
            .configured_model(&seat_address())
            .unwrap()
            .as_deref(),
        Some("fixture-model")
    );
    fixture.herdr_for_replacement_showing(0, vec![claude_agent(&fixture)]);

    fixture
        .nexus
        .after_report(&CLAUDE_SEAT.to_owned(), &claude_stop(&fixture));
    assert_eq!(
        fixture.nexus.store.context_size(CLAUDE_SEAT).unwrap(),
        Some(230_000)
    );

    assert!(closed(&fixture), "the predecessor's pane is closed");
    assert!(
        fixture.typed().is_none(),
        "nothing is typed into the predecessor past its threshold"
    );
    let node = fixture
        .nexus
        .store
        .flow_node(CLAUDE_SEAT)
        .unwrap()
        .expect("the flow's row is kept");
    assert_eq!(node.flow_lifecycle, FlowLifecycle::Stopped);
    let metaflow = seat_metaflow_now(&fixture);
    assert_eq!(metaflow.state, State::Awake("908786".into()));
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

/// An idle seat past its threshold starts and confirms its successor even
/// with nothing queued. Only then is the predecessor stopped and closed.
/// Below the threshold the same seat keeps its pane.
#[test]
fn an_idle_seat_is_refreshed_only_past_its_threshold() {
    for (tokens, refreshed) in [(199_999, false), (200_000, true)] {
        let fixture = NexusFixture::new();
        claude_seat(&fixture, tokens);
        append_main_rows(&fixture, &[end_turn("last-turn")]);
        seat_refreshing_at(&fixture, 200_000);
        seat_metaflow(&fixture, vec![]);
        if refreshed {
            let mut successor = staged_refresh_successor(&fixture);
            successor.stage_to_ambiguity(&fixture.nexus);
            successor.write_receipt();
            declare_refresh_successor(&fixture);
            seat_metaflow(&fixture, vec![]);
            fixture.herdr_for_replacement_showing(0, vec![claude_agent(&fixture)]);
        } else {
            fixture.accept_pane_operations(
                vec![claude_agent(&fixture)],
                PromptFixture::Prompted("w1:p3"),
            );
        }
        fixture
            .nexus
            .store
            .record_context_size(CLAUDE_SEAT, tokens)
            .unwrap();
        let at_stop = fixture.nexus.settle_stopped(CLAUDE_SEAT);
        if refreshed {
            assert_eq!(at_stop, AtStop::Refreshed(seat_address()));
            assert_eq!(
                seat_metaflow_now(&fixture).state,
                State::Awake("908786".into())
            );
            assert_eq!(
                fixture
                    .nexus
                    .store
                    .flow_node(CLAUDE_SEAT)
                    .unwrap()
                    .unwrap()
                    .flow_lifecycle,
                FlowLifecycle::Stopped
            );
        } else {
            assert_eq!(at_stop, AtStop::Idle);
            assert_eq!(
                seat_metaflow_now(&fixture).state,
                State::Awake(CLAUDE_SEAT.into())
            );
        }
        assert_eq!(closed(&fixture), refreshed, "{tokens}");
    }
}

#[test]
fn a_refused_refresh_launch_preserves_the_predecessor() {
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
    fixture
        .nexus
        .store
        .configure_change(ConfigurationChange::Metaflows(vec![ConfigureMetaflow {
            address: seat_address(),
            system_modules: vec!["missing-module".into()],
            user_modules: Vec::new(),
            flow_kind: signal_flow::FlowKind::Regular,
            tool_set: signal_flow::ToolSet::Default,
        }]))
        .unwrap();
    fixture
        .nexus
        .store
        .configure_change(ConfigurationChange::Models(vec![ConfigureModel {
            layer: Layer::Secondary,
            aspect: Aspect::Mind,
            native: CLAUDE_MODEL.into(),
        }]))
        .unwrap();

    assert!(matches!(
        fixture.nexus.settle_stopped(CLAUDE_SEAT),
        AtStop::Kept(crate::waking::KeptAtStop::RefreshLaunchRefused(_))
    ));
    assert!(!closed(&fixture));
    assert_eq!(
        fixture
            .nexus
            .store
            .flow_node(CLAUDE_SEAT)
            .unwrap()
            .unwrap()
            .flow_lifecycle,
        FlowLifecycle::Active
    );
    assert_eq!(
        seat_metaflow_now(&fixture).state,
        State::Awake(CLAUDE_SEAT.into())
    );
}

#[test]
fn startup_recovery_resumes_one_refresh_and_repeated_stop_is_idempotent() {
    let fixture = NexusFixture::new();
    claude_seat(&fixture, 200_000);
    append_main_rows(&fixture, &[end_turn("last-turn")]);
    seat_refreshing_at(&fixture, 200_000);
    seat_metaflow(&fixture, vec![]);
    let mut successor = staged_refresh_successor(&fixture);
    successor.reserve(&fixture.nexus);
    successor.record_native_intent(&fixture.nexus);
    successor.bind(&fixture.nexus);
    successor.register_and_acknowledge(&fixture.nexus);
    declare_refresh_successor(&fixture);
    seat_metaflow(&fixture, vec![]);
    fixture.herdr_for_replacement_showing(0, vec![claude_agent(&fixture)]);

    assert!(matches!(
        fixture.nexus.settle_stopped(CLAUDE_SEAT),
        AtStop::Kept(crate::waking::KeptAtStop::RefreshLaunchRefused(_))
    ));
    assert_eq!(
        fixture
            .nexus
            .store
            .flow_node(CLAUDE_SEAT)
            .unwrap()
            .unwrap()
            .flow_lifecycle,
        FlowLifecycle::Active,
        "an incomplete successor leaves its predecessor intact"
    );

    successor.record_prompt_intent(&fixture.nexus);
    successor.record_ambiguity(&fixture.nexus);
    successor.write_receipt();
    fixture.nexus.recover_wakes();
    assert_eq!(
        seat_metaflow_now(&fixture).state,
        State::Awake("908786".into())
    );
    assert_eq!(
        fixture.nexus.settle_stopped(CLAUDE_SEAT),
        AtStop::Kept(crate::waking::KeptAtStop::NotAwake)
    );
    let operations = fs::read_to_string(fixture.directory.path().join("herdr-calls.log")).unwrap();
    assert_eq!(operations.matches("pane close w1:p3").count(), 1);
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

    let started = signal_flow::Event::Started;
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
        .after_report(&CLAUDE_SEAT.to_owned(), &signal_flow::Event::Started);
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
