//! A flow past its model's refresh threshold is given no new message and is
//! replaced as it goes idle, reaped in the same event; a compaction its
//! harness made while it kept working raises a warning and an
//! investigation.

use super::waking::{
    CLAUDE_SEAT, append_main_rows, claude_agent, claude_seat, claude_stop, claude_transcript,
    end_turn,
};
use super::{ControlsHerdrSnapshot, NexusFixture, PromptFixture};
use crate::compacting::{CodexContextSignal, WatchesCompactions, WritesTranscriptTime};
use crate::reporting::RecordsReports;
use crate::store::compaction::{Investigation, RecordsCompactions};
use crate::store::context::{JudgesThreshold, RecordsContextSize, ThresholdStanding};
use crate::store::metaflow::{SeedsMetaflow, SleepsMetaflows};
use crate::store::{ManagesMetaflowState, ReadsFlowRows, SeedsRole};
use crate::waking::{AtStop, SleepsAndWakesSeats};
use flow_ethos::State;
use meta_signal_flow::{ConfigurationChange, ConfigureInvestigation, ConfigureThreshold};
use signal_flow::{
    Address, Aspect, Caller, FlowLifecycle, Layer, Metaflow, PowerLevel, QueuedRequest, Request,
    SentBy,
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

fn investigation_address() -> Address {
    Address {
        aspect: Aspect::Psyche,
        topic: "compaction".into(),
        layer: Layer::Tertiary,
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
            .map(|request| QueuedRequest {
                sent_by: SentBy::Unknown,
                request,
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
    fs::read_to_string(fixture.directory.path().join("herdr-operations.log"))
        .unwrap_or_default()
        .contains("pane close w1:p3")
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
            .map(|queued| &queued.request)
            .collect::<Vec<_>>(),
        [&Request::Order("Held for the next flow.".into())],
        "the work waits for the next flow"
    );
}

/// An idle seat past its threshold is replaced as it goes idle, with
/// nothing queued: it is reaped, and its metaflow sleeps until work wakes
/// its next flow. Below the threshold the same seat keeps its pane.
#[test]
fn an_idle_seat_is_refreshed_only_past_its_threshold() {
    for (tokens, refreshed) in [(199_999, false), (200_000, true)] {
        let fixture = NexusFixture::new();
        fixture.accept_pane_operations(
            vec![claude_agent(&fixture)],
            PromptFixture::Prompted("w1:p3"),
        );
        claude_seat(&fixture, tokens);
        append_main_rows(&fixture, &[end_turn("last-turn")]);
        seat_refreshing_at(&fixture, 200_000);
        seat_metaflow(&fixture, vec![]);
        fixture
            .nexus
            .store
            .record_context_size(CLAUDE_SEAT, tokens)
            .unwrap();
        let at_stop = fixture.nexus.settle_stopped(CLAUDE_SEAT);
        if refreshed {
            assert_eq!(at_stop, AtStop::Refreshed(seat_address()));
            assert_eq!(seat_metaflow_now(&fixture).state, State::Asleep);
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

fn configure_investigation(fixture: &NexusFixture) {
    fixture
        .nexus
        .store
        .configure_change(ConfigurationChange::Investigation(ConfigureInvestigation {
            address: investigation_address(),
            system_modules: vec!["spirit-core".into()],
            user_modules: vec!["knowledge-flow".into()],
            herdr_session_name: "default".into(),
        }))
        .expect("investigation configured");
}

/// A compaction the harness made on its own, while the flow kept working,
/// raises a warning once and gives the investigation to the configured
/// seat; one the flow was asked for raises nothing, and one made before
/// this Nexus watched the transcript raises nothing.
#[test]
fn a_compaction_during_continuous_work_is_investigated_once() {
    let fixture = NexusFixture::new();
    claude_seat(&fixture, 40_000);
    seat_metaflow(&fixture, vec![]);
    configure_investigation(&fixture);
    // The Investigation seat is Awake: the investigation is queued for it.
    fixture.nexus.store.seed_metaflow(Metaflow {
        address: investigation_address(),
        state: State::Awake("1nv3st".into()),
        past: vec![],
        queue: vec![],
    });
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
    assert_eq!(compactions[0].investigation, Investigation::Queued);
    assert_eq!(compactions[0].address, Some(seat_address()));
    let investigation = fixture
        .nexus
        .store
        .metaflow(&investigation_address())
        .unwrap()
        .unwrap();
    assert_eq!(investigation.queue.len(), 1, "queued once");
    let Request::Order(brief) = &investigation.queue[0].request else {
        panic!("an Order: {:?}", investigation.queue[0]);
    };
    assert_eq!(investigation.queue[0].sent_by, SentBy::Unknown);
    assert!(brief.contains(&format!("flow {CLAUDE_SEAT} ({{ Mind flow Secondary }})")));
    assert!(brief.contains("Psyche core Secondary"));
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
