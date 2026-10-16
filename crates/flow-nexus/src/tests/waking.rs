//! Sleep and wake against the fixture Herdr: a resting seat's pane is
//! closed and its native session kept; a queued request resumes that session
//! in a new pane with the same FlowId and the whole queue in one prompt; what
//! arrives during the wake is typed after it.

use super::{ControlsHerdrSnapshot, NexusFixture, PromptFixture};
use crate::RunningNexus;
use crate::composition::{ComposesFreshSeat, ComposesLaunch, FreshSeat, RendersQueuedWork};
use crate::fixture_executable::{FixtureExecutable, InstallsScript};
use crate::generated::operation::{Operation, Outcome};
use crate::herdr::launch::ChecksComposedLaunch;
use crate::herdr::pane::PaneAgent;
use crate::herdr::reservation::ChoosesNativeSession;
use crate::performing::Performs;
use crate::reporting::RecordsReports;
use crate::store::ManagesMetaflowState;
use crate::store::context::RecordsContextSize;
use crate::store::metaflow::{SeedsMetaflow, SleepsMetaflows};
use crate::store::{
    ReadsFlowRows, RecordsFlowLifecycle, RecordsNativeLaunchBinding, RecordsNativeLaunchIntent,
    RegistersFlowIdentity, ReservesLaunchAttempt,
};
use crate::waking::{
    AtStop, HoldsWakingMetaflows, KeptAtStop, SleepsAndWakesSeats, WakePlan, WakePolicy,
};
use flow_ethos::State;
use sha2::{Digest, Sha256};
use signal_flow::{
    Address, Aspect, EndpointSelection, FlowLifecycle, FlowNode, HarnessKind, HerdrRoute,
    HerdrRouteSelection, LaunchProfile, Layer, Metaflow, NativeLaunchBinding, NativeLaunchIntent,
    OriginClue, PowerLevel, Request, Source,
};
use std::fs;

fn address(layer: Layer) -> Address {
    Address {
        aspect: Aspect::Mind,
        topic: "flow".into(),
        layer,
    }
}

fn metaflow(layer: Layer, state: State, past: Vec<&str>, queue: Vec<Request>) -> Metaflow {
    Metaflow {
        address: address(layer),
        state,
        past: past.into_iter().map(String::from).collect(),
        queue,
    }
}

fn stored(fixture: &NexusFixture, layer: Layer) -> Metaflow {
    fixture
        .nexus
        .store
        .metaflow(&address(layer))
        .expect("metaflow readable")
        .expect("metaflow held")
}

fn closes(fixture: &NexusFixture) -> bool {
    fs::read_to_string(fixture.directory.path().join("herdr-operations.log"))
        .unwrap_or_default()
        .contains("pane close w1:p3")
}

/// A Claude seat `c1a0de` resting in pane w1:p3 of the fixture Herdr, its
/// claim written, its native transcript showing a context of `tokens`.
const CLAUDE_SEAT: &str = "c1a0de";
const CLAUDE_SESSION: &str = "c1a0de00-0000-4000-8000-000000000001";

fn claude_agent(fixture: &NexusFixture) -> serde_json::Value {
    let mut agent = fixture.settled_agent();
    agent["agent"] = serde_json::Value::String("claude".into());
    agent
}

fn claude_seat(fixture: &NexusFixture, tokens: i64) {
    let mut node = fixture.node();
    node.flow_id = CLAUDE_SEAT.into();
    node.session_id = CLAUDE_SESSION.into();
    node.harness_kind = HarnessKind::Claude;
    node.endpoint_selection = EndpointSelection::Unavailable;
    node.origin_clue.flow_id = CLAUDE_SEAT.into();
    fixture
        .nexus
        .store
        .register_flow(node)
        .expect("Claude seat registered");
    fs::write(
        fixture
            .directory
            .path()
            .join(format!("flows/.{CLAUDE_SEAT}.flow-id")),
        format!(
            "version=1\nharness=claude\nidentity={}\nalias={CLAUDE_SEAT}\nuuid-version=uuid-v4\n",
            CLAUDE_SESSION.replace('-', "")
        ),
    )
    .unwrap();
    let project = fixture
        .directory
        .path()
        .join("native-transcripts/claude/-home-li-primary");
    fs::create_dir_all(&project).unwrap();
    fs::write(
        project.join(format!("{CLAUDE_SESSION}.jsonl")),
        format!(
            "{}\n",
            serde_json::json!({"type":"assistant","message":{"usage":{
                "input_tokens": 3,
                "cache_read_input_tokens": tokens - 13,
                "cache_creation_input_tokens": 10,
                "output_tokens": 50
            }}})
        ),
    )
    .unwrap();
}

fn refresh_at(fixture: &NexusFixture, layer: Layer, refresh: i64) {
    fixture
        .nexus
        .store
        .configure_change(meta_signal_flow::ConfigurationChange::Threshold(
            meta_signal_flow::ConfigureThreshold {
                layer,
                handover: refresh / 2,
                refresh,
            },
        ))
        .expect("threshold configured");
}

#[test]
fn a_resting_claude_flow_that_stopped_writes_its_context_and_sleeps_keeping_its_session() {
    let fixture = NexusFixture::new();
    fixture.accept_pane_operations(
        vec![claude_agent(&fixture)],
        PromptFixture::Prompted("w1:p3"),
    );
    claude_seat(&fixture, 151_000);
    fixture.nexus.store.seed_metaflow(metaflow(
        Layer::Secondary,
        State::Awake(CLAUDE_SEAT.into()),
        vec![],
        vec![],
    ));

    fixture
        .nexus
        .after_report(&CLAUDE_SEAT.to_owned(), &signal_flow::Event::Stopped);

    assert_eq!(
        fixture.nexus.store.context_size(CLAUDE_SEAT).unwrap(),
        Some(151_000),
        "the context size is written at the Stop"
    );
    assert!(closes(&fixture), "the seat's pane is closed");
    let slept = stored(&fixture, Layer::Secondary);
    assert_eq!(slept.state, State::Asleep);
    assert_eq!(slept.past, vec![CLAUDE_SEAT.to_owned()]);
    let node = fixture
        .nexus
        .store
        .flow_node(CLAUDE_SEAT)
        .unwrap()
        .expect("flow row kept");
    assert_eq!(node.flow_lifecycle, FlowLifecycle::Stopped);
    assert_eq!(
        node.session_id, CLAUDE_SESSION,
        "the native session a wake resumes is kept"
    );
}

/// A seat that cannot be resumed never sleeps: a Codex seat, here resting
/// with nothing queued, stays awake with its pane open.
#[test]
fn a_resting_codex_seat_with_nothing_queued_stays_awake() {
    let fixture = NexusFixture::new();
    fixture.accept_pane_operations(
        vec![fixture.settled_agent()],
        PromptFixture::Prompted("w1:p3"),
    );
    fixture.register_with(FlowLifecycle::Active);
    fixture.nexus.store.seed_metaflow(metaflow(
        Layer::Secondary,
        State::Awake("908786".into()),
        vec![],
        vec![],
    ));
    assert_eq!(
        fixture.nexus.settle_stopped("908786"),
        AtStop::Kept(KeptAtStop::NotResumable)
    );
    assert_eq!(
        stored(&fixture, Layer::Secondary).state,
        State::Awake("908786".into())
    );
    assert!(!closes(&fixture));
    let mut claude = fixture.node();
    claude.harness_kind = HarnessKind::Claude;
    assert!(RunningNexus::sleeps_seat(&claude));
    claude.session_id.clear();
    assert!(!RunningNexus::sleeps_seat(&claude), "no session to resume");
    assert!(!RunningNexus::sleeps_seat(&fixture.node()), "Codex");
}

/// A seat that goes idle with work queued is handed the oldest request,
/// stays awake, and the rest waits for its next Stop.
#[test]
fn a_resting_seat_with_work_queued_is_handed_the_oldest_request_and_stays_awake() {
    let fixture = NexusFixture::new();
    fixture.accept_pane_operations(
        vec![claude_agent(&fixture)],
        PromptFixture::Prompted("w1:p3"),
    );
    claude_seat(&fixture, 40_000);
    fixture.nexus.store.seed_metaflow(metaflow(
        Layer::Secondary,
        State::Awake(CLAUDE_SEAT.into()),
        vec![],
        vec![
            Request::Notice("first".into()),
            Request::Result("second".into()),
        ],
    ));

    assert_eq!(
        fixture.nexus.settle_stopped(CLAUDE_SEAT),
        AtStop::Drained(Request::Notice("first".into()))
    );
    let kept = stored(&fixture, Layer::Secondary);
    assert_eq!(kept.state, State::Awake(CLAUDE_SEAT.into()));
    assert_eq!(kept.queue, vec![Request::Result("second".into())]);
    assert!(!closes(&fixture), "it does not sleep with work queued");
    let typed = fixture.typed().expect("typed into the pane");
    assert!(typed.contains("first"));
    assert!(!typed.contains("second"), "one request per Stop");
}

#[test]
fn a_flow_that_stopped_while_working_or_awaited_does_not_sleep() {
    // Herdr still shows the seat Working.
    let working = NexusFixture::new();
    working.accept_pane_operations(
        vec![working.current_agent()],
        PromptFixture::Prompted("w1:p3"),
    );
    working.register_with(FlowLifecycle::Active);
    working.nexus.store.seed_metaflow(metaflow(
        Layer::Secondary,
        State::Awake("908786".into()),
        vec![],
        vec![],
    ));
    assert!(matches!(
        working.nexus.settle_stopped("908786"),
        AtStop::Kept(KeptAtStop::NotResting(PaneAgent::Present {
            agent_state: signal_flow::AgentState::Working,
            ..
        }))
    ));
    assert_eq!(
        stored(&working, Layer::Secondary).state,
        State::Awake("908786".into())
    );
    assert!(!closes(&working));

    // Resting and unawaited, but a Primary.
    let primary = NexusFixture::new();
    primary.accept_pane_operations(
        vec![primary.settled_agent()],
        PromptFixture::Prompted("w1:p3"),
    );
    primary.register_with(FlowLifecycle::Active);
    primary.nexus.store.seed_metaflow(metaflow(
        Layer::Primary,
        State::Awake("908786".into()),
        vec![],
        vec![],
    ));
    assert_eq!(
        primary.nexus.settle_stopped("908786"),
        AtStop::Kept(KeptAtStop::LayerNeverSleeps)
    );
    assert!(!closes(&primary));

    // Resting and unawaited, but a wake holds its metaflow.
    let waking = NexusFixture::new();
    waking.accept_pane_operations(
        vec![waking.settled_agent()],
        PromptFixture::Prompted("w1:p3"),
    );
    waking.register_with(FlowLifecycle::Active);
    waking.nexus.store.seed_metaflow(metaflow(
        Layer::Secondary,
        State::Awake("908786".into()),
        vec![],
        vec![],
    ));
    let _lease = waking.nexus.waking.hold(&address(Layer::Secondary));
    assert_eq!(
        waking.nexus.settle_stopped("908786"),
        AtStop::Kept(KeptAtStop::Waking)
    );
    assert!(!closes(&waking));
}

#[test]
fn work_and_words_wake_a_secondary_and_nothing_wakes_a_primary() {
    assert!(RunningNexus::wakes_for(&Request::Order("go".into())));
    assert!(RunningNexus::wakes_for(&Request::Question("why".into())));
    assert!(RunningNexus::wakes_for(&Request::Psyche(
        flow_ethos::Said {
            context: "c".into(),
            verbatim: "v".into(),
        }
    )));
    assert!(!RunningNexus::wakes_for(&Request::Result("done".into())));
    assert!(!RunningNexus::wakes_for(&Request::Notice("fyi".into())));
    assert!(!RunningNexus::wakes_layer(&Layer::Primary));
    assert!(RunningNexus::wakes_layer(&Layer::Secondary));
    assert!(RunningNexus::sleeps_at(&signal_flow::AgentState::Idle));
    assert!(RunningNexus::sleeps_at(&signal_flow::AgentState::Done));
    assert!(!RunningNexus::sleeps_at(&signal_flow::AgentState::Working));
}

#[test]
fn the_store_sleeps_and_awakens_exactly_removing_only_what_the_woken_flow_was_handed() {
    let fixture = NexusFixture::new();
    let store = &fixture.nexus.store;
    let at = address(Layer::Tertiary);
    store.seed_metaflow(metaflow(
        Layer::Tertiary,
        State::Awake("aaaaaa".into()),
        vec![],
        vec![],
    ));
    assert!(
        !store.sleep_metaflow(&at, "bbbbbb").unwrap(),
        "not Awake as it"
    );
    assert!(store.sleep_metaflow(&at, "aaaaaa").unwrap());
    assert!(
        !store.sleep_metaflow(&at, "aaaaaa").unwrap(),
        "already Asleep"
    );
    let mut asleep = store.metaflow(&at).unwrap().unwrap();
    asleep.queue = vec![
        Request::Order("A".into()),
        Request::Order("B".into()),
        Request::Order("C".into()),
    ];
    store.seed_metaflow(asleep);
    assert!(
        !store.awaken_metaflow(&at, "aaaaaa", 4).unwrap(),
        "more handed over than queued"
    );
    assert!(store.awaken_metaflow(&at, "aaaaaa", 2).unwrap());
    let woken = store.metaflow(&at).unwrap().unwrap();
    assert_eq!(woken.state, State::Awake("aaaaaa".into()));
    assert!(woken.past.is_empty(), "a resumed flow is current, not Past");
    assert_eq!(
        woken.queue,
        vec![Request::Order("C".into())],
        "what arrived after the wake read the queue stays"
    );
    assert!(
        !store
            .remove_oldest_queued(&at, &Request::Order("A".into()))
            .unwrap(),
        "only the oldest, as typed, is removed"
    );
    assert!(
        store
            .remove_oldest_queued(&at, &Request::Order("C".into()))
            .unwrap()
    );
    assert!(store.metaflow(&at).unwrap().unwrap().queue.is_empty());
    assert_eq!(
        store.metaflow_awake_as("aaaaaa").unwrap().unwrap().address,
        at
    );
}

#[test]
fn the_context_size_against_the_refresh_threshold_chooses_resume_or_fresh() {
    assert!(RunningNexus::resumes(Some(99_999), Some(100_000)));
    assert!(!RunningNexus::resumes(Some(100_000), Some(100_000)));
    assert!(!RunningNexus::resumes(Some(250_000), Some(100_000)));
    assert!(RunningNexus::resumes(None, Some(100_000)), "size unread");
    assert!(RunningNexus::resumes(Some(250_000), None), "no threshold");

    let fixture = NexusFixture::new();
    resumable(&fixture);
    let asleep = metaflow(Layer::Secondary, State::Asleep, vec!["abc123"], vec![]);
    refresh_at(&fixture, Layer::Secondary, 200_000);
    fixture
        .nexus
        .store
        .record_context_size("abc123", 120_000)
        .unwrap();
    assert!(matches!(
        fixture.nexus.wake_plan(&asleep),
        WakePlan::Resume { .. }
    ));
    fixture
        .nexus
        .store
        .record_context_size("abc123", 230_000)
        .unwrap();
    assert_eq!(
        fixture.nexus.wake_plan(&asleep),
        WakePlan::Fresh {
            herdr_session_name: "fixture-session".into()
        },
        "too big to resume: it wakes fresh"
    );
}

#[test]
fn a_fresh_wake_composes_the_topic_modules_and_the_queued_work() {
    let fixture = NexusFixture::new();
    let module = fixture.directory.path().join("modules/vision-flow.md");
    fs::create_dir_all(module.parent().unwrap()).unwrap();
    fs::write(&module, b"module bytes\n").unwrap();
    let queue = [
        Request::Order("first".into()),
        Request::Question("second".into()),
    ];
    let text = queue.prompt_text();
    let seat = FreshSeat {
        address: address(Layer::Secondary),
        launch_request_id: "wake-0-1".into(),
        model_name: "claude-fixture".into(),
        harness_kind: HarnessKind::Claude,
        power_level: PowerLevel::Medium,
        effort: "medium".into(),
        herdr_session_name: "fixture-session".into(),
        modules: vec![Source {
            repository: "modules".into(),
            hash: "unchecked".into(),
            path: "vision-flow.md".into(),
        }],
        prompt: text.clone(),
    };
    let profile = fixture.nexus.composer.fresh_profile(&seat).unwrap();
    assert_eq!(profile.address, address(Layer::Secondary));
    assert_eq!(profile.instruction_prompt, text);
    assert_eq!(profile.launch_source_vector.len(), 1);
    assert_eq!(
        profile.launch_source_vector[0].source_path,
        "modules/vision-flow.md"
    );
    assert_eq!(
        profile.launch_source_vector[0].source_sha256,
        format!("{:x}", Sha256::digest(b"module bytes\n"))
    );
    let bundle = fs::read_to_string(&profile.system_prompt_bundle_file).unwrap();
    assert!(bundle.contains("Mind flow Secondary seat"));
    let composed = fixture.nexus.composer.compose(&profile).unwrap();
    let prompt = composed.first_prompt_payload.first_prompt_text;
    assert!(
        prompt.contains(
            &module
                .canonicalize()
                .unwrap()
                .to_string_lossy()
                .into_owned()
        )
    );
    let first = prompt.find("Order.first").expect("first request");
    let second = prompt.find("Question.second").expect("second request");
    assert!(first < second, "oldest first");
}

#[test]
fn an_asleep_codex_seat_without_a_resumable_session_wakes_fresh_in_its_session() {
    let fixture = NexusFixture::new();
    fixture.set_agents(vec![fixture.current_agent()]);
    fixture.register_with(FlowLifecycle::Active);
    let plan = fixture.nexus.wake_plan(&metaflow(
        Layer::Secondary,
        State::Asleep,
        vec!["908786"],
        vec![],
    ));
    assert_eq!(
        plan,
        WakePlan::Fresh {
            herdr_session_name: "messaging-build".into()
        }
    );
    assert_eq!(
        fixture
            .nexus
            .wake_plan(&metaflow(Layer::Secondary, State::Asleep, vec![], vec![])),
        WakePlan::Unplaceable
    );
}

/// The resumed seat: a Claude flow `abc123` that slept, its launch kept.
struct Resumable {
    profile: LaunchProfile,
    session: String,
}

const RESUMED_AGENT: &str = r#"{"agent":"claude","agent_status":"idle","interactive_ready":true,"name":"AGENT","pane_id":"w9:p1","tab_id":"w9:t1","terminal_id":"term-resumed","workspace_id":"w9","revision":1}"#;

fn resumable(fixture: &NexusFixture) -> Resumable {
    resumable_ended_by(fixture, |store| store.record_stopped("abc123").unwrap())
}

/// The resumable Claude flow abc123, ended by `end` rather than Stopped.
fn resumable_ended_by(
    fixture: &NexusFixture,
    end: impl FnOnce(&crate::store::FlowStore) -> bool,
) -> Resumable {
    let bundle = fixture.directory.path().join("resumed-bundle.md");
    fs::write(&bundle, b"resumed bundle\n").unwrap();
    let profile = LaunchProfile {
        launch_request_id: "resumed-request".into(),
        launch_source_vector: vec![],
        skill_name_vector: vec![],
        address: signal_flow::Address {
            aspect: Aspect::Mind,
            topic: "flow".into(),
            layer: signal_flow::Layer::Secondary,
        },
        power_level: PowerLevel::Medium,
        harness_kind: HarnessKind::Claude,
        model_name: "claude-fixture".into(),
        effort: "medium".into(),
        flow_id_option: None,
        remembered_flow_vector: vec![],
        herdr_session_name: "fixture-session".into(),
        system_prompt_bundle_file: bundle.to_string_lossy().into_owned(),
        instruction_prompt: "fixture instruction".into(),
    };
    let session = fixture
        .nexus
        .herdr
        .reserved_native_session_id(&profile)
        .expect("a Claude launch has a Flow-chosen session");
    // The launch that bound abc123, with its bundle copy.
    let composed = fixture.nexus.composer.compose(&profile).expect("composes");
    let origin = OriginClue {
        flow_id: "caller".into(),
        session_id: "caller-session".into(),
        turn_id: "caller-turn".into(),
    };
    fixture
        .nexus
        .store
        .reserve_launch_attempt(&composed, origin.clone())
        .expect("reserved");
    assert!(
        fixture
            .nexus
            .store
            .record_native_launch_intent(NativeLaunchIntent {
                launch_request_id: profile.launch_request_id.clone(),
                prompt_sha256: composed.first_prompt_payload.prompt_sha256.clone(),
                harness_kind: HarnessKind::Claude,
                model_name: profile.model_name.clone(),
                effort: profile.effort.clone(),
                skill_name_vector: vec![],
            })
            .unwrap()
    );
    let old_pane = signal_flow::HerdrPaneBinding {
        launch_request_id: profile.launch_request_id.clone(),
        herdr_session_name: "fixture-session".into(),
        herdr_agent_name: composed.launch_agent_name(),
        herdr_workspace_id: "w1".into(),
        herdr_pane_id: "w1:p3".into(),
        herdr_terminal_id: "term-old".into(),
    };
    assert!(
        fixture
            .nexus
            .store
            .record_native_launch_binding(NativeLaunchBinding {
                launch_request_id: profile.launch_request_id.clone(),
                flow_id: "abc123".into(),
                native_session_id: session.clone(),
                harness_kind: HarnessKind::Claude,
                herdr_pane_binding: old_pane,
            })
            .unwrap()
    );
    fixture
        .nexus
        .store
        .register_flow(FlowNode {
            flow_id: "abc123".into(),
            session_id: session.clone(),
            harness_kind: HarnessKind::Claude,
            endpoint_selection: EndpointSelection::Unavailable,
            herdr_route_selection: HerdrRouteSelection::Available(HerdrRoute {
                herdr_session_name: "fixture-session".into(),
                herdr_agent_name: composed.launch_agent_name(),
                herdr_pane_id: "w1:p3".into(),
                herdr_terminal_id: "term-old".into(),
            }),
            origin_clue: origin,
            flow_lifecycle: FlowLifecycle::Active,
        })
        .expect("registered");
    assert!(end(&fixture.nexus.store));
    fs::write(
        fixture.directory.path().join("flows/.abc123.flow-id"),
        format!(
            "version=1\nharness=claude\nidentity={}\nalias=abc123\nuuid-version=uuid-v5\n",
            session.replace('-', "")
        ),
    )
    .unwrap();
    let agent = RESUMED_AGENT.replace("AGENT", &composed.launch_agent_name());
    let agent_record = format!(
        r#"{{"result":{{"agent":{}}}}}"#,
        agent.replace(
            r#""revision":1}"#,
            &format!(
                r#""revision":1,"agent_session":{{"source":"herdr:claude","agent":"claude","kind":"id","value":"{session}"}}}}"#
            )
        )
    );
    let calls = fixture.directory.path().join("herdr-calls.log");
    let typed = fixture.directory.path().join("pane-typed.txt");
    let marker = fixture.directory.path().join("marker-output");
    let script = r#"#!/bin/sh
SESSIONS
printf '%s\n' "$*" >> 'CALLS'
case "$*" in
  *"workspace create"*) printf '%s\n' '{"result":{"workspace":{"workspace_id":"w9"},"root_pane":{"pane_id":"w9:p1","terminal_id":"term-resumed"}}}' ;;
  *"pane run"*) sh -c "$6" > 'MARKER' ;;
  *"pane wait-output"*) line=$(grep -F FLOW_CLAUDE_ENV_READY_ 'MARKER'); printf '{"result":{"pane_id":"w9:p1","matched_line":"%s"}}\n' "$line" ;;
  *"agent start"*) printf '%s\n' '{"result":{"agent":{"name":"x"}}}' ;;
  *"agent get"*) printf '%s\n' 'RECORD' ;;
  *"api snapshot"*) printf '%s\n' '{"id":"cli:api:snapshot","result":{"snapshot":{"agents":[AGENT],"protocol":20,"version":"0.8.2"},"type":"session_snapshot"}}' ;;
  *"agent wait"*) printf '%s\n' '{"result":{"status":"idle"}}' ;;
  *"agent read"*) printf '%s\n' '❯ ' ;;
  *"agent prompt /rename "*|*"agent prompt "*" /rename "*) printf '%s\n' '{"result":{"accepted":true}}' ;;
  *"agent prompt"*) printf '%s\n--\n' "$6" >> 'TYPED'; printf '%s\n' '{"id":"cli:agent:prompt","result":{"type":"agent_prompted","agent":AGENT}}' ;;
  *) exit 64 ;;
esac
"#
    .replace("SESSIONS", &fixture.session_listing())
    .replace("CALLS", &calls.display().to_string())
    .replace("MARKER", &marker.display().to_string())
    .replace("TYPED", &typed.display().to_string())
    .replace("RECORD", &agent_record)
    .replace("AGENT", &agent);
    FixtureExecutable {
        path: fixture.snapshot_program.clone(),
    }
    .install(&script);
    let transcripts = fixture.directory.path().join("native-transcripts");
    fs::create_dir_all(&transcripts).unwrap();
    FixtureExecutable {
        path: transcripts.join("flow-id"),
    }
    .install("#!/bin/sh\nprintf '%s\\n' abc123\n");
    Resumable { profile, session }
}

#[test]
fn a_queued_order_resumes_the_sleeping_session_with_its_flow_id_and_the_whole_queue() {
    let fixture = NexusFixture::new();
    let seat = resumable(&fixture);
    let at = address(Layer::Secondary);
    let queue = vec![
        Request::Order("A".into()),
        Request::Notice("B".into()),
        Request::Question("C".into()),
    ];
    fixture.nexus.store.seed_metaflow(metaflow(
        Layer::Secondary,
        State::Asleep,
        vec!["abc123"],
        queue.clone(),
    ));
    let lease = fixture.nexus.waking.hold(&at).expect("the wake holds it");
    assert!(
        fixture.nexus.waking.hold(&at).is_none(),
        "a second request starts no second wake"
    );

    assert_eq!(
        fixture.nexus.perform(Operation::Wake(at.clone())),
        Outcome::Woken("abc123".into()),
        "the sleeping flow resumes as itself"
    );
    // D arrives while the wake holds the metaflow.
    let mut woken = stored(&fixture, Layer::Secondary);
    assert!(
        woken.queue.is_empty(),
        "the handed-over queue left with Awoken"
    );
    woken.queue.push(Request::Order("D".into()));
    fixture.nexus.store.seed_metaflow(woken);
    fixture.nexus.drain_woken(&at, lease);
    assert!(!fixture.nexus.waking.is_waking(&at), "the hold is released");

    let calls = fs::read_to_string(fixture.directory.path().join("herdr-calls.log")).unwrap();
    let start = calls
        .lines()
        .find(|line| line.contains("agent start"))
        .expect("the harness started again");
    assert!(start.contains(&format!(" --resume {} ", seat.session)));
    assert!(!start.contains("--session-id"));
    assert!(
        calls
            .lines()
            .any(|line| line.contains("pane run") && line.contains("export FLOW_ID=abc123 "))
    );

    let typed = fs::read_to_string(fixture.directory.path().join("pane-typed.txt")).unwrap();
    let prompts = typed
        .split("\n--\n")
        .filter(|prompt| !prompt.is_empty())
        .collect::<Vec<_>>();
    assert_eq!(
        prompts.len(),
        2,
        "the queue in one prompt, then D: {prompts:?}"
    );
    assert_eq!(prompts[0], queue.prompt_text());
    assert_eq!(prompts[1], [Request::Order("D".into())].prompt_text());

    let woken = stored(&fixture, Layer::Secondary);
    assert_eq!(woken.state, State::Awake("abc123".into()));
    assert!(woken.past.is_empty());
    assert!(woken.queue.is_empty());
    let node = fixture.nexus.store.flow_node("abc123").unwrap().unwrap();
    assert_eq!(node.session_id, seat.session);
    assert!(node.flow_lifecycle.is_live_for_tests());
    assert_eq!(
        node.herdr_route_selection,
        HerdrRouteSelection::Available(HerdrRoute {
            herdr_session_name: "fixture-session".into(),
            herdr_agent_name: fixture
                .nexus
                .composer
                .compose(&seat.profile)
                .unwrap()
                .launch_agent_name(),
            herdr_pane_id: "w9:p1".into(),
            herdr_terminal_id: "term-resumed".into(),
        })
    );
}

#[test]
fn a_wake_that_cannot_start_puts_the_work_back_and_stays_asleep() {
    let fixture = NexusFixture::new();
    let at = address(Layer::Secondary);
    let queue = vec![Request::Order("A".into())];
    fixture.nexus.store.seed_metaflow(metaflow(
        Layer::Secondary,
        State::Asleep,
        vec![],
        queue.clone(),
    ));
    assert!(matches!(
        fixture.nexus.perform(Operation::Wake(at.clone())),
        Outcome::Failed(_)
    ));
    let asleep = stored(&fixture, Layer::Secondary);
    assert_eq!(asleep.state, State::Asleep);
    assert_eq!(asleep.queue, queue);
}

/// A wake the last Nexus did not finish left its work queued: the next
/// Nexus wakes the seat at its start, and the work leaves the queue only
/// with the write that records the seat Awake.
#[test]
fn a_nexus_start_wakes_a_seat_whose_wake_did_not_finish() {
    let fixture = NexusFixture::new();
    resumable(&fixture);
    let queue = vec![Request::Notice("B".into()), Request::Order("A".into())];
    fixture.nexus.store.seed_metaflow(metaflow(
        Layer::Secondary,
        State::Asleep,
        vec!["abc123"],
        queue.clone(),
    ));
    // Only a Result is queued for this one: nothing wakes it.
    fixture.nexus.store.seed_metaflow(metaflow(
        Layer::Tertiary,
        State::Asleep,
        vec![],
        vec![Request::Result("r".into())],
    ));

    fixture.nexus.recover_wakes();

    let woken = stored(&fixture, Layer::Secondary);
    assert_eq!(woken.state, State::Awake("abc123".into()));
    assert!(woken.queue.is_empty());
    let typed = fs::read_to_string(fixture.directory.path().join("pane-typed.txt")).unwrap();
    assert!(typed.starts_with(&queue.prompt_text()));
    assert_eq!(stored(&fixture, Layer::Tertiary).state, State::Asleep);
}

/// A wake is refused while a pane already runs the sleeping flow's native
/// session; the seat stays Asleep with its work queued.
#[test]
fn a_wake_is_refused_while_a_pane_already_runs_the_session() {
    let fixture = NexusFixture::new();
    let seat = resumable(&fixture);
    let at = address(Layer::Secondary);
    let queue = vec![Request::Order("A".into())];
    fixture.nexus.store.seed_metaflow(metaflow(
        Layer::Secondary,
        State::Asleep,
        vec!["abc123"],
        queue.clone(),
    ));
    let running = serde_json::json!({
        "agent": "claude",
        "agent_status": "idle",
        "name": "by-hand",
        "pane_id": "w4:p2",
        "terminal_id": "term-by-hand",
        "workspace_id": "w4",
        "agent_session": {"source": "herdr:claude", "agent": "claude", "kind": "id", "value": seat.session}
    });
    fixture.set_agents(vec![running]);

    assert_eq!(
        fixture.nexus.perform(Operation::Wake(at.clone())),
        Outcome::Failed(crate::generated::operation::Failed_Data::ConflictingBinding)
    );
    let asleep = stored(&fixture, Layer::Secondary);
    assert_eq!(asleep.state, State::Asleep);
    assert_eq!(asleep.queue, queue);
}

trait ReadsLiveness {
    fn is_live_for_tests(&self) -> bool;
}

impl ReadsLiveness for FlowLifecycle {
    fn is_live_for_tests(&self) -> bool {
        matches!(self, FlowLifecycle::Pending | FlowLifecycle::Active)
    }
}

/// A launch takes its Address by Bind's rules: an Address Awake as a live
/// flow is refused unless that flow is the predecessor the launch replaces;
/// one Awake as a flow no longer live, or Asleep, is taken, a displaced
/// flow joining Past; an Ended one is refused. A resumed flow leaves Past.
#[test]
fn a_launch_takes_its_address_by_binds_rules() {
    use crate::store::metaflow::{AddressTaking, TakesLaunchAddress};
    use signal_flow::StartRejection;
    let fixture = NexusFixture::new();
    let store = &fixture.nexus.store;
    let at = address(Layer::Tertiary);
    let taken = |predecessor: Option<&str>, flow_id: &str| match store
        .address_taking(&at, flow_id, predecessor, 0)
        .unwrap()
    {
        AddressTaking::Takes(_) => Ok(()),
        AddressTaking::Refused(rejection) => Err(rejection),
    };

    // No metaflow yet: taken, and recorded only by the write.
    assert_eq!(taken(None, "aaaaaa"), Ok(()));
    assert!(store.awaken_metaflow(&at, "aaaaaa", 0).unwrap());
    assert_eq!(
        stored(&fixture, Layer::Tertiary).state,
        State::Awake("aaaaaa".into())
    );

    // Awake as a flow with no live row: taken over, the old flow Past.
    assert!(store.awaken_metaflow(&at, "bbbbbb", 0).unwrap());
    let over = stored(&fixture, Layer::Tertiary);
    assert_eq!(over.state, State::Awake("bbbbbb".into()));
    assert_eq!(over.past, vec!["aaaaaa".to_owned()]);

    // Awake as a live flow: refused, but not to its successor.
    store
        .register_flow(fixture.node())
        .expect("live flow registered");
    store.seed_metaflow(metaflow(
        Layer::Tertiary,
        State::Awake("908786".into()),
        vec![],
        vec![],
    ));
    assert_eq!(taken(None, "cccccc"), Err(StartRejection::AddressAwake));
    assert!(!store.awaken_metaflow(&at, "cccccc", 0).unwrap());
    assert_eq!(taken(Some("908786"), "cccccc"), Ok(()));
    assert_eq!(
        taken(None, "908786"),
        Ok(()),
        "a flow keeps its own Address"
    );

    // Asleep: taken; the resumed flow leaves Past.
    store.seed_metaflow(metaflow(
        Layer::Tertiary,
        State::Asleep,
        vec!["dddddd"],
        vec![Request::Order("A".into())],
    ));
    assert!(store.awaken_metaflow(&at, "dddddd", 1).unwrap());
    let resumed = stored(&fixture, Layer::Tertiary);
    assert_eq!(resumed.state, State::Awake("dddddd".into()));
    assert!(resumed.past.is_empty());
    assert!(resumed.queue.is_empty());

    // Ended: refused.
    store.seed_metaflow(metaflow(Layer::Tertiary, State::Ended, vec![], vec![]));
    assert_eq!(taken(None, "eeeeee"), Err(StartRejection::AddressEnded));
}

/// A seat Flow launched sends through Message: Message names the sending
/// process, which runs in the launched seat's pane, and Flow identifies it
/// as the Address its launch took. Its Order to an Asleep seat is queued
/// and wakes that seat, which is handed the Order.
/// Message, running as its own process and an ancestor of no seat, bound
/// at Field message Primary as the configured Message Nexus binary.
fn message_peer(fixture: &NexusFixture) -> (super::MarkedProcess, signal_flow::Process) {
    use crate::metaflow::DispatchesMetaflowPeer;
    use crate::store::metaflow::MutatesMetaflows;
    use crate::store::{FlowStore, ReadsFlowMemory};
    use signal_flow::{Bind_Data, Response};
    // Message runs as its own process, an ancestor of no seat.
    let message_child = super::MarkedProcess::spawn("sleep", &["30"], "w0:p0");
    let message_executable = fs::canonicalize(format!("/proc/{}/exe", message_child.0.id()))
        .unwrap()
        .to_string_lossy()
        .into_owned();
    use crate::store::SeedsConfiguration;
    let settings = crate::store::DefaultConfiguration {
        home: fixture.directory.path().join("home"),
        runtime_directory: fixture.directory.path().join("runtime"),
    }
    .configuration();
    fixture
        .nexus
        .store
        .configure_change(meta_signal_flow::ConfigurationChange::Nexus(
            meta_signal_flow::ConfigureNexus {
                source_root: settings.source_root,
                stable_codex: settings.stable_codex,
                next_codex: settings.next_codex,
                harness_profiles: settings.harness_profile_vector,
                meta_aspects: settings.meta_aspects,
                message_nexus_path: settings.message_nexus_path,
                message_nexus_binary: message_executable,
                lease: 30,
            },
        ))
        .expect("Nexus configured");
    let message = FlowStore::process_identity_for_pid(message_child.0.id()).unwrap();
    let message_address = Address {
        aspect: Aspect::Field,
        topic: "message".into(),
        layer: Layer::Primary,
    };
    assert!(matches!(
        fixture
            .nexus
            .dispatch_metaflow_peer(
                signal_flow::Query::Bind(Bind_Data {
                    address: message_address,
                    process: message.clone(),
                }),
                Some(&message),
            )
            .response,
        Response::Bound(_)
    ));
    assert!(fixture.nexus.store.message_peer(&message).unwrap());
    (message_child, message)
}

#[test]
fn a_launched_seat_identifies_through_its_pane_and_its_order_wakes_an_asleep_seat() {
    identifies_and_wakes_by_an_order(State::Asleep);
}

/// A metaflow an older Nexus left Awake as its Stopped flow is read as
/// Asleep: the Order queued for it is recorded against an Asleep metaflow
/// and wakes it, resuming the Stopped session.
#[test]
fn a_metaflow_left_awake_as_an_ended_flow_is_woken_by_an_order() {
    identifies_and_wakes_by_an_order(State::Awake("abc123".into()));
}

fn identifies_and_wakes_by_an_order(recipient: State) {
    use crate::metaflow::DispatchesMetaflowPeer;
    use crate::store::metaflow::TakesLaunchAddress;
    use crate::store::{FlowStore, ReadsFlowMemory};
    use signal_flow::{Deliver_Data, Lock_Data, Recipient, Response};

    let fixture = NexusFixture::new();
    resumable(&fixture);
    let (_message_child, message) = message_peer(&fixture);

    // The sending seat: a flow Flow launched into pane w5:p1, its launch
    // having taken the Address Field flow Quaternary.
    let sender_address = Address {
        aspect: Aspect::Field,
        topic: "flow".into(),
        layer: Layer::Quaternary,
    };
    let sender_route = HerdrRoute {
        herdr_session_name: "messaging-build".into(),
        herdr_agent_name: "sender-agent".into(),
        herdr_pane_id: "w5:p1".into(),
        herdr_terminal_id: "term-sender".into(),
    };
    fixture
        .nexus
        .store
        .register_flow_in_role(
            FlowNode {
                flow_id: "5e4de7".into(),
                session_id: "5e4de700-0000-4000-8000-000000000001".into(),
                harness_kind: HarnessKind::Claude,
                endpoint_selection: EndpointSelection::Unavailable,
                herdr_route_selection: HerdrRouteSelection::Available(sender_route),
                origin_clue: OriginClue {
                    flow_id: "da2038".into(),
                    session_id: "field-session".into(),
                    turn_id: "field-turn".into(),
                },
                flow_lifecycle: FlowLifecycle::Active,
            },
            signal_flow::Caller {
                flow_id: "5e4de7".into(),
                aspect: Aspect::Field,
                power_level: PowerLevel::UltraLow,
                model_name: "haiku".into(),
            },
        )
        .expect("sender registered");
    assert!(matches!(
        fixture
            .nexus
            .store
            .address_taking(&sender_address, "5e4de7", None, 0)
            .unwrap(),
        crate::store::metaflow::AddressTaking::Takes(_)
    ));
    assert!(
        fixture
            .nexus
            .store
            .awaken_metaflow(&sender_address, "5e4de7", 0)
            .unwrap()
    );
    // Herdr shows the sender's pane beside the seat a wake resumes.
    let script = fs::read_to_string(&fixture.snapshot_program).unwrap();
    let sender_agent = r#"{"agent":"claude","agent_status":"working","name":"sender-agent","pane_id":"w5:p1","terminal_id":"term-sender","workspace_id":"w5"}"#;
    FixtureExecutable {
        path: fixture.snapshot_program.clone(),
    }
    .install(&script.replace(r#""agents":["#, &format!(r#""agents":[{sender_agent},"#)));

    // The process that sends runs in the sender's pane, its pane ID alone
    // marked, as in Herdr's default session.
    let sender = super::MarkedProcess::spawn("sleep", &["30"], "w5:p1");
    let sender_process = FlowStore::process_identity_for_pid(sender.0.id()).unwrap();
    assert_eq!(
        fixture
            .nexus
            .dispatch_metaflow_peer(
                signal_flow::Query::Identify(sender_process.clone()),
                Some(&message)
            )
            .response,
        Response::Identified(sender_address.clone())
    );
    // A process in no launched seat's pane names no one.
    let stranger = super::MarkedProcess::spawn("sleep", &["30"], "w8:p8");
    let stranger_process = FlowStore::process_identity_for_pid(stranger.0.id()).unwrap();
    assert!(matches!(
        fixture
            .nexus
            .dispatch_metaflow_peer(
                signal_flow::Query::Identify(stranger_process),
                Some(&message)
            )
            .response,
        Response::Refused(signal_flow::Refusal::Unidentified(_))
    ));

    // The seat the Order is for: Asleep, or left Awake as its Stopped flow.
    let at = address(Layer::Secondary);
    let past = if recipient == State::Asleep {
        vec!["abc123"]
    } else {
        vec![]
    };
    fixture
        .nexus
        .store
        .seed_metaflow(metaflow(Layer::Secondary, recipient, past, vec![]));
    let Response::Locked(lock) = fixture
        .nexus
        .dispatch_metaflow_peer(
            signal_flow::Query::Lock(Lock_Data {
                sender: sender_address.clone(),
                recipient: Recipient::Address(at.clone()),
            }),
            Some(&message),
        )
        .response
    else {
        panic!("the identified sender locks the recipient")
    };
    let order = Request::Order("Run the tier tests.".into());
    let answer = fixture.nexus.dispatch_metaflow_peer(
        signal_flow::Query::Deliver(Deliver_Data {
            lock,
            request: order.clone(),
        }),
        Some(&message),
    );
    assert_eq!(answer.response, Response::Queued);
    assert_eq!(answer.wake, Some(at.clone()), "the Order wakes the seat");

    fixture.nexus.wake_due(at.clone());
    let woken = stored(&fixture, Layer::Secondary);
    assert_eq!(woken.state, State::Awake("abc123".into()));
    assert!(woken.queue.is_empty());
    let typed = fs::read_to_string(fixture.directory.path().join("pane-typed.txt")).unwrap();
    assert!(typed.starts_with(&[order].prompt_text()));
}

/// A seat launched into the pane Message's marked processes name: its
/// binding witnessed and acknowledged, its first prompt typed, and no answer
/// of its yet recorded.
fn launched_unanswered(fixture: &NexusFixture, launch_request_id: &str) -> super::StagedLaunch {
    let mut launch = fixture.staged_launch(launch_request_id, None);
    launch.pane.herdr_session_name = "messaging-build".into();
    launch.pane.herdr_pane_id = "w5:p1".into();
    launch.pane.herdr_agent_name = "sender-agent".into();
    launch.pane.herdr_terminal_id = "term-sender".into();
    launch.binding.herdr_pane_binding = launch.pane.clone();
    launch.stage_to_ambiguity(&fixture.nexus);
    fixture.set_agents(vec![serde_json::json!({
        "agent":"codex","agent_status":"working","name":"sender-agent",
        "pane_id":"w5:p1","terminal_id":"term-sender","workspace_id":"w5"
    })]);
    launch
}

/// The living's rule is that a seat's first turn does real work: a seat
/// whose first tool call sends, before any answer of its is recorded, is
/// already identified at its Address and its Order is queued.
#[test]
fn a_launched_seat_sends_an_order_from_its_first_tool_call_before_any_answer() {
    use crate::metaflow::DispatchesMetaflowPeer;
    use crate::store::{FlowStore, ReadsFlowMemory, ReadsLaunchAttempt};
    use signal_flow::{Deliver_Data, LaunchAttemptPhase, Lock_Data, Recipient, Response};

    let fixture = NexusFixture::new();
    let (_message_child, message) = message_peer(&fixture);
    let launch = launched_unanswered(&fixture, "first-turn-request");
    let sender_address = launch.profile.address.clone();
    assert_eq!(
        fixture
            .nexus
            .store
            .launch_attempt("first-turn-request")
            .unwrap()
            .unwrap()
            .launch_attempt_phase,
        LaunchAttemptPhase::PromptAmbiguous,
        "no answer of the seat is recorded"
    );

    let first_tool_call = super::MarkedProcess::spawn("sleep", &["30"], "w5:p1");
    let process = FlowStore::process_identity_for_pid(first_tool_call.0.id()).unwrap();
    assert_eq!(
        fixture
            .nexus
            .dispatch_metaflow_peer(signal_flow::Query::Identify(process), Some(&message))
            .response,
        Response::Identified(sender_address.clone())
    );
    let at = address(Layer::Secondary);
    fixture.nexus.store.seed_metaflow(metaflow(
        Layer::Secondary,
        State::Awake("abc123".into()),
        vec![],
        vec![],
    ));
    let Response::Locked(lock) = fixture
        .nexus
        .dispatch_metaflow_peer(
            signal_flow::Query::Lock(Lock_Data {
                sender: sender_address,
                recipient: Recipient::Address(at.clone()),
            }),
            Some(&message),
        )
        .response
    else {
        panic!("the identified seat locks its recipient")
    };
    let order = Request::Order("Run the tier tests.".into());
    assert_eq!(
        fixture
            .nexus
            .dispatch_metaflow_peer(
                signal_flow::Query::Deliver(Deliver_Data {
                    lock,
                    request: order.clone(),
                }),
                Some(&message),
            )
            .response,
        Response::Queued
    );
    assert_eq!(stored(&fixture, Layer::Secondary).queue, vec![order]);
}

/// A launch refused before it was confirmed gives the Address back as it
/// found it, keeping what was queued meanwhile; a flow that ends unconfirmed
/// gives back an Address that had no metaflow by removing it.
#[test]
fn an_unconfirmed_launch_that_fails_or_ends_leaves_no_awake_registration() {
    use crate::launching::LaunchesFlows;
    use crate::store::address_hold::HoldsLaunchAddress;
    use signal_flow::{Response, StartRejection};

    let fixture = NexusFixture::new();
    // An Address that was Asleep: refused, it is Asleep again.
    let mut asleep = fixture.staged_launch("asleep-request", None);
    asleep.profile.address = address(Layer::Secondary);
    asleep.composed.launch_profile.address = address(Layer::Secondary);
    fixture.nexus.store.seed_metaflow(metaflow(
        Layer::Secondary,
        State::Asleep,
        vec!["abc123"],
        vec![Request::Order("A".into())],
    ));
    asleep.stage_to_ambiguity(&fixture.nexus);
    let held = stored(&fixture, Layer::Secondary);
    assert_eq!(held.state, State::Awake("908786".into()));
    assert_eq!(held.past, vec!["abc123".to_owned()]);
    let mut queued = held.clone();
    queued.queue.push(Request::Notice("B".into()));
    fixture.nexus.store.seed_metaflow(queued);

    assert_eq!(
        fixture.nexus.settle(
            "asleep-request",
            Response::StartRejected(StartRejection::NativeLaunchRefused)
        ),
        Response::StartRejected(StartRejection::NativeLaunchRefused)
    );
    assert_eq!(
        stored(&fixture, Layer::Secondary),
        metaflow(
            Layer::Secondary,
            State::Asleep,
            vec!["abc123"],
            vec![Request::Order("A".into()), Request::Notice("B".into())],
        )
    );
    assert_eq!(
        fixture.nexus.store.metaflow_awake_as("908786").unwrap(),
        None
    );
    assert_eq!(fixture.nexus.store.address_hold("908786").unwrap(), None);

    // An Address with no metaflow: the flow's pane goes before any answer.
    let fixture = NexusFixture::new();
    let mut absent = fixture.staged_launch("absent-request", None);
    let at = absent.profile.address.clone();
    absent.stage_to_ambiguity(&fixture.nexus);
    assert!(
        fixture
            .nexus
            .store
            .metaflow_awake_as("908786")
            .unwrap()
            .is_some()
    );
    assert!(fixture.nexus.store.record_exited("908786").unwrap());
    assert_eq!(fixture.nexus.store.metaflow(&at).unwrap(), None);
    assert_eq!(fixture.nexus.store.address_hold("908786").unwrap(), None);
}

/// A Herdr stand-in for the Claude seat in pane w1:p3 that shows it working
/// until an `agent wait` has run, and resting (`done`) after: Claude runs
/// its Stop hook while its spinner is still on screen. The wait first
/// waits, boundedly, for `released` to exist. Every call is logged.
fn herdr_working_until_waited(
    fixture: &NexusFixture,
    released: &std::path::Path,
) -> std::path::PathBuf {
    let log = fixture.directory.path().join("herdr-operations.log");
    let waited = fixture.directory.path().join("herdr-waited");
    let snapshot = |status: &str| {
        let mut agent = claude_agent(fixture);
        agent["agent_status"] = serde_json::Value::String(status.into());
        serde_json::json!({
            "id":"cli:api:snapshot",
            "result":{"snapshot":{"agents":[agent],"protocol":20,"version":"0.8.2"},
            "type":"session_snapshot"}
        })
    };
    let body = format!(
        "#!/bin/sh\nprintf '%s\\n' \"$*\" >> '{log}'\ncase \"$3 $4\" in\n  \"api snapshot\") if [ -e '{waited}' ]; then printf '%s\\n' '{done}'; else printf '%s\\n' '{working}'; fi ;;\n  \"agent wait\") i=0; while [ ! -e '{released}' ] && [ $i -lt 100 ]; do sleep 0.1; i=$((i+1)); done; if [ -e '{released}' ]; then echo 'wait released by the closed hook' >> '{log}'; else echo 'wait timed out' >> '{log}'; fi; : > '{waited}' ;;\n  \"pane close\") ;;\n  \"agent read\") printf '%s\\n' '❯ ' ;;\n  *) exit 64 ;;\nesac\n",
        log = log.display(),
        waited = waited.display(),
        released = released.display(),
        done = snapshot("done"),
        working = snapshot("working"),
    );
    FixtureExecutable {
        path: fixture.snapshot_program.clone(),
    }
    .install(&body);
    log
}

/// The live defect: a Claude seat's Stop hook runs while Herdr still shows
/// it working, and it shows done only a moment later, with no event. The
/// Stop waits, boundedly, for the seat to rest, then puts it to sleep.
#[test]
fn a_seat_herdr_shows_working_at_its_stop_sleeps_once_herdr_shows_it_done() {
    let fixture = NexusFixture::new();
    let released = fixture.directory.path().join("released");
    fs::write(&released, b"").unwrap();
    let log = herdr_working_until_waited(&fixture, &released);
    claude_seat(&fixture, 40_000);
    fixture.nexus.store.seed_metaflow(metaflow(
        Layer::Secondary,
        State::Awake(CLAUDE_SEAT.into()),
        vec![],
        vec![],
    ));

    assert_eq!(fixture.nexus.settle_stopped(CLAUDE_SEAT), AtStop::Slept);
    assert_eq!(stored(&fixture, Layer::Secondary).state, State::Asleep);
    let calls = fs::read_to_string(log).unwrap();
    let wait = calls
        .find("agent wait w1:p3 --until idle --until done --timeout")
        .expect("the Stop waited for the seat to rest");
    let close = calls.find("pane close w1:p3").expect("the pane closed");
    assert!(wait < close, "the wait comes before the sleep: {calls}");
}

/// Every early exit names its reason: a seat still working after the wait
/// says so, as does a seat nothing holds Awake.
#[test]
fn a_stop_that_leaves_its_seat_names_why() {
    let fixture = NexusFixture::new();
    fixture.accept_pane_operations(
        vec![{
            let mut agent = claude_agent(&fixture);
            agent["agent_status"] = serde_json::Value::String("working".into());
            agent
        }],
        PromptFixture::Prompted("w1:p3"),
    );
    claude_seat(&fixture, 40_000);
    assert_eq!(
        fixture.nexus.settle_stopped(CLAUDE_SEAT),
        AtStop::Kept(KeptAtStop::NotAwake)
    );
    fixture.nexus.store.seed_metaflow(metaflow(
        Layer::Secondary,
        State::Awake(CLAUDE_SEAT.into()),
        vec![],
        vec![],
    ));
    assert!(matches!(
        fixture.nexus.settle_stopped(CLAUDE_SEAT),
        AtStop::Kept(KeptAtStop::NotResting(PaneAgent::Present {
            agent_state: signal_flow::AgentState::Working,
            ..
        }))
    ));
    assert_eq!(
        stored(&fixture, Layer::Secondary).state,
        State::Awake(CLAUDE_SEAT.into())
    );
}

/// The hook's Report is answered and its connection closed before the
/// settling waits on Herdr: the harness waits on its hook, so a wait on
/// the harness while the hook is connected would wait on itself. Here the
/// wait cannot finish until the hook has seen its answer and the close.
#[test]
fn the_hook_has_its_answer_and_its_connection_closed_before_the_rest_wait_ends() {
    use crate::{CarriesSignalFrames, ServesOrdinary};
    use std::io::Read;
    use std::os::unix::net::UnixStream;
    use std::time::{Duration, Instant};

    let fixture: &'static NexusFixture = Box::leak(Box::new(NexusFixture::new()));
    let released = fixture.directory.path().join("hook-closed");
    let log = herdr_working_until_waited(fixture, &released);
    claude_seat(fixture, 40_000);
    fixture.nexus.store.seed_metaflow(metaflow(
        Layer::Secondary,
        State::Awake(CLAUDE_SEAT.into()),
        vec![],
        vec![],
    ));
    let socket = fixture.directory.path().join("ordinary.sock");
    let served = socket.clone();
    std::thread::spawn(move || fixture.nexus.serve_ordinary(&served));
    let mut hook = (0..200)
        .find_map(|_| {
            UnixStream::connect(&socket).ok().or_else(|| {
                std::thread::sleep(Duration::from_millis(10));
                None
            })
        })
        .expect("ordinary socket listens");
    hook.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    hook.write_query(&signal_flow::Query::Report(signal_flow::Report_Data {
        flow_id: CLAUDE_SEAT.into(),
        event: signal_flow::Event::Stopped,
    }))
    .unwrap();
    assert_eq!(
        hook.read_response().expect("the hook has its answer"),
        signal_flow::Response::Reported
    );
    let mut rest = Vec::new();
    assert_eq!(
        hook.read_to_end(&mut rest)
            .expect("the connection closes while the wait still runs"),
        0
    );
    fs::write(&released, b"").unwrap();

    let deadline = Instant::now() + Duration::from_secs(20);
    while stored(fixture, Layer::Secondary).state != State::Asleep {
        assert!(Instant::now() < deadline, "the seat never slept");
        std::thread::sleep(Duration::from_millis(20));
    }
    let calls = fs::read_to_string(log).unwrap();
    assert!(
        calls.contains("wait released by the closed hook"),
        "{calls}"
    );
}

/// A Stop that lands before its launch is confirmed finds the seat holding
/// only its Address and leaves it; the confirmation settles it.
#[test]
fn a_stop_before_confirmation_is_settled_when_the_launch_is_confirmed() {
    use crate::launching::PromotesAmbiguousLaunches;
    use crate::store::address_hold::HoldsLaunchAddress;
    use std::time::{Duration, Instant};

    let fixture: &'static NexusFixture = Box::leak(Box::new(NexusFixture::new()));
    let log = fixture.directory.path().join("herdr-calls.log");
    let typed = fixture.directory.path().join("pane-typed.txt");
    let resting = serde_json::json!({
        "agent":"codex",
        "agent_status":"idle",
        "interactive_ready":true,
        "name":"fixture-agent",
        "pane_id":"w1:p1",
        "revision":1,
        "terminal_id":"fixture-terminal",
        "workspace_id":"fixture-workspace"
    });
    let agent = serde_json::json!({"result":{"agent":{
        "name":"fixture-agent",
        "agent":"codex",
        "workspace_id":"fixture-workspace",
        "pane_id":"w1:p1",
        "terminal_id":"fixture-terminal",
        "agent_session":{
            "source":"herdr:codex",
            "agent":"codex",
            "kind":"id",
            "value":super::STAGED_SESSION
        }
    }}});
    let snapshot = serde_json::json!({
        "id":"cli:api:snapshot",
        "result":{"snapshot":{"agents":[resting.clone()],"protocol":20,"version":"0.8.2"},
        "type":"session_snapshot"}
    });
    let prompted = serde_json::json!({
        "id":"cli:agent:prompt",
        "result":{ "type":"agent_prompted", "agent":resting }
    });
    FixtureExecutable {
        path: fixture.snapshot_program.clone(),
    }
    .install(&format!(
        "#!/bin/sh\nprintf '%s\\n' \"$*\" >> '{log}'\ncase \"$*\" in\n  \"--session fixture-session agent get w1:p1\") printf '%s\\n' '{agent}' ;;\n  *\" api snapshot\") printf '%s\\n' '{snapshot}' ;;\n  *\" agent wait \"*) ;;\n  *\" agent prompt \"*) printf '%s' \"$6\" >> '{typed}'; printf '%s\\n' '{prompted}' ;;\n  *\" agent read \"*) printf '%s\\n' '› ' ;;\n  *) exit 64 ;;\nesac\n",
        log = log.display(),
        typed = typed.display(),
    ));
    let mut launch = fixture.staged_launch("stopped-early-request", None);
    let at = launch.profile.address.clone();
    launch.stage_to_ambiguity(&fixture.nexus);
    let mut held = fixture.nexus.store.metaflow(&at).unwrap().unwrap();
    assert_eq!(held.state, State::Awake("908786".into()));
    held.queue
        .push(Request::Notice("queued before confirmation".into()));
    fixture.nexus.store.seed_metaflow(held);

    assert_eq!(
        fixture.nexus.report(signal_flow::Report_Data {
            flow_id: "908786".into(),
            event: signal_flow::Event::Stopped,
        }),
        signal_flow::Response::Reported
    );
    assert_eq!(
        fixture.nexus.settle_stopped("908786"),
        AtStop::Kept(KeptAtStop::Unconfirmed),
        "the Stop finds the launch unconfirmed"
    );

    launch.write_receipt();
    std::thread::spawn(move || fixture.nexus.promote_ambiguous_launches());
    let deadline = Instant::now() + Duration::from_secs(20);
    while !fixture
        .nexus
        .store
        .metaflow(&at)
        .unwrap()
        .unwrap()
        .queue
        .is_empty()
    {
        assert!(
            Instant::now() < deadline,
            "the confirmation never settled the Stop: {}",
            fs::read_to_string(&log).unwrap_or_default()
        );
        std::thread::sleep(Duration::from_millis(20));
    }
    assert!(
        fs::read_to_string(&typed)
            .unwrap_or_default()
            .contains("queued before confirmation")
    );
    assert_eq!(fixture.nexus.store.address_hold("908786").unwrap(), None);
}

/// A topic never dies: the Stop of the flow its metaflow is Awake as
/// records the metaflow Asleep in the same write, the flow in Past and its
/// session kept, so a wake resumes it and a fresh Start may take the
/// Address.
#[test]
fn the_stop_of_a_confirmed_seat_leaves_its_metaflow_asleep_and_resumable() {
    use crate::store::metaflow::TakesLaunchAddress;
    let fixture = NexusFixture::new();
    fixture.nexus.store.seed_metaflow(metaflow(
        Layer::Secondary,
        State::Awake("abc123".into()),
        vec![],
        vec![],
    ));
    let resumable = resumable(&fixture);
    let slept = stored(&fixture, Layer::Secondary);
    assert_eq!(slept.state, State::Asleep);
    assert_eq!(slept.past, vec!["abc123".to_owned()]);
    assert!(matches!(
        fixture.nexus.wake_plan(&slept),
        WakePlan::Resume { .. }
    ));
    let mut fresh = resumable.profile.clone();
    fresh.launch_request_id = "fresh-after-stop".into();
    assert_eq!(
        fixture.nexus.store.launch_address_refusal(&fresh).unwrap(),
        None,
        "a fresh Start may take the Address"
    );
}

/// A Retired flow joins Past as provenance, its metaflow Asleep, but its
/// native session is never resumed: the queued Order wakes the seat fresh.
#[test]
fn a_retired_seat_sleeps_its_metaflow_and_its_order_wakes_it_fresh() {
    let fixture = NexusFixture::new();
    fixture.nexus.store.seed_metaflow(metaflow(
        Layer::Secondary,
        State::Awake("abc123".into()),
        vec![],
        vec![],
    ));
    resumable_ended_by(&fixture, |store| {
        store.record_active("abc123").unwrap() && store.record_retired("abc123").unwrap()
    });
    let mut retired = stored(&fixture, Layer::Secondary);
    assert_eq!(retired.state, State::Asleep);
    assert_eq!(retired.past, vec!["abc123".to_owned()]);
    retired.queue.push(Request::Order("Carry on.".into()));
    fixture.nexus.store.seed_metaflow(retired.clone());
    assert_eq!(
        fixture.nexus.wake_plan(&retired),
        WakePlan::Fresh {
            herdr_session_name: "fixture-session".into()
        },
        "a retired session wakes fresh, never resumed"
    );
    let mut node = fixture.node();
    node.harness_kind = HarnessKind::Claude;
    node.flow_lifecycle = FlowLifecycle::Exited;
    assert!(
        !RunningNexus::can_resume(&node),
        "an Exited flow wakes fresh"
    );
    node.flow_lifecycle = FlowLifecycle::Stopped;
    assert!(RunningNexus::can_resume(&node));
}

/// At the Nexus's start every metaflow left Awake as an ended flow is
/// recorded Asleep; one Awake as a flow Flow holds no row for, as Bind
/// names, is left as it is.
#[test]
fn a_nexus_start_puts_every_metaflow_left_awake_as_an_ended_flow_to_sleep() {
    let fixture = NexusFixture::new();
    resumable(&fixture);
    fixture.nexus.store.seed_metaflow(metaflow(
        Layer::Secondary,
        State::Awake("abc123".into()),
        vec![],
        vec![],
    ));
    fixture.nexus.store.seed_metaflow(metaflow(
        Layer::Tertiary,
        State::Awake("flow-0000000000000001".into()),
        vec![],
        vec![],
    ));
    fixture.nexus.recover_wakes();
    let rested = stored(&fixture, Layer::Secondary);
    assert_eq!(rested.state, State::Asleep);
    assert_eq!(rested.past, vec!["abc123".to_owned()]);
    assert_eq!(
        stored(&fixture, Layer::Tertiary).state,
        State::Awake("flow-0000000000000001".into())
    );
}
