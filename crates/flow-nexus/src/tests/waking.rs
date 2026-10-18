//! Sleep and wake against the fixture Herdr: a resting seat's pane is
//! closed and its native session kept; a queued request resumes that session
//! in a new pane with the same FlowId and the whole queue in one prompt; what
//! arrives during the wake is typed after it.

use super::{ControlsHerdrSnapshot, NexusFixture, PromptFixture};
use crate::RunningNexus;
use crate::composition::{ComposesLaunch, FreshSeat, RendersQueuedWork};
use crate::fixture_executable::{FixtureExecutable, InstallsScript};
use crate::generated::operation::{Operation, Outcome};
use crate::generated::starter::ProfilesFreshSeat;
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
    AtStop, FreshContext, HoldsWakingMetaflows, KeptAtStop, SleepsAndWakesSeats, WakePlan,
    WakePolicy,
};
use flow_ethos::State;
use signal_flow::{
    Address, Aspect, EndpointSelection, FlowLifecycle, FlowNode, FlowOrigin, HarnessKind,
    HerdrRoute, HerdrRouteSelection, LaunchProfile, Layer, Metaflow, NativeLaunchBinding,
    NativeLaunchIntent, Origin, PowerLevel, QueuedRequest, Request, SentBy,
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
        queue: queue.unsent(),
    }
}

/// A request as it is queued with no sender kept: what seeding a queue in
/// these tests stands for, and what a request queued before Flow kept
/// senders reads as.
trait Unsent {
    type Queued;
    fn unsent(self) -> Self::Queued;
}

impl Unsent for Request {
    type Queued = QueuedRequest;
    fn unsent(self) -> QueuedRequest {
        QueuedRequest {
            sent_by: SentBy::Unknown,
            request: self,
        }
    }
}

impl Unsent for Vec<Request> {
    type Queued = Vec<QueuedRequest>;
    fn unsent(self) -> Vec<QueuedRequest> {
        self.into_iter().map(Unsent::unsent).collect()
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
    agent["agent_session"] = serde_json::json!({
        "source":"herdr:claude","agent":"claude","kind":"id","value":CLAUDE_SESSION
    });
    agent
}

/// An agent that reports no native session, as a remote-resumed Codex TUI
/// shows in Herdr.
fn quiet_agent(fixture: &NexusFixture) -> serde_json::Value {
    let mut agent = fixture.settled_agent();
    if let Some(fields) = agent.as_object_mut() {
        fields.remove("agent_session");
    }
    agent
}

fn claude_seat(fixture: &NexusFixture, tokens: i64) {
    let mut node = fixture.node();
    node.flow_id = CLAUDE_SEAT.into();
    node.session_id = CLAUDE_SESSION.into();
    node.harness_kind = HarnessKind::Claude;
    node.endpoint_selection = EndpointSelection::Unavailable;
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
        .configure_change(meta_signal_flow::ConfigurationChange::Thresholds(vec![
            meta_signal_flow::ConfigureThreshold {
                layer,
                handover: refresh / 2,
                refresh,
            },
        ]))
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
        AtStop::Drained(Request::Notice("first".into()).unsent())
    );
    let kept = stored(&fixture, Layer::Secondary);
    assert_eq!(kept.state, State::Awake(CLAUDE_SEAT.into()));
    assert_eq!(kept.queue, vec![Request::Result("second".into())].unsent());
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
    ]
    .unsent();
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
        vec![Request::Order("C".into())].unsent(),
        "what arrived after the wake read the queue stays"
    );
    assert!(
        !store
            .remove_oldest_queued(&at, &Request::Order("A".into()).unsent())
            .unwrap(),
        "only the oldest, as typed, is removed"
    );
    assert!(
        store
            .remove_oldest_queued(&at, &Request::Order("C".into()).unsent())
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
        WakePlan::Fresh(FreshContext {
            herdr_session_name: "fixture-session".into(),
            system_modules: vec!["spirit-core".into(), "intent-flow".into()],
            user_modules: vec!["operation-main-flow".into()],
            leave_outs_option: None,
        }),
        "too big to resume: it wakes fresh"
    );
}

#[test]
fn a_fresh_wake_composes_the_last_launch_modules_and_the_queued_work() {
    let fixture = NexusFixture::new();
    let queue = [
        Request::Order("first".into()),
        Request::Question("second".into()),
    ];
    let text = queue.to_vec().unsent().prompt_text();
    let seat = FreshSeat {
        address: address(Layer::Secondary),
        launch_request_id: "wake-0-1".into(),
        model_name: "claude-fixture".into(),
        effort: "medium".into(),
        herdr_session_name: "fixture-session".into(),
        system_modules: vec!["spirit-core".into(), "intent-flow".into()],
        user_modules: vec!["operation-main-flow".into(), "knowledge-flow".into()],
        leave_outs_option: None,
        prompt: text.clone(),
    };
    let profile = seat.fresh_profile();
    assert_eq!(profile.address, address(Layer::Secondary));
    assert_eq!(profile.instruction_prompt, text);
    assert_eq!(profile.user_modules, seat.user_modules);
    let composed = fixture
        .nexus
        .composer
        .compose(
            crate::composition::HarnessedProfile {
                harness_kind: HarnessKind::Claude,
                launch_profile: profile.clone(),
            },
            Vec::<signal_flow::LeaveOut>::new(),
        )
        .unwrap();
    // The same full module text a Start of these identities carries, then
    // the queued work as the brief.
    let body = composed.first_prompt_payload.first_prompt_body;
    assert!(body.starts_with("These are your skills, given here; do not reload them."));
    assert!(body.contains("Main flow body.") && body.contains("Knowledge body."));
    assert!(
        composed
            .system_prompt
            .system_prompt_text
            .contains("Spirit body.")
    );
    let first = body.find("Order.first").expect("first request");
    let second = body.find("Question.second").expect("second request");
    assert!(first < second, "oldest first");
    assert!(!body.contains("seat"));
}

#[test]
fn an_asleep_seat_no_launch_of_record_names_is_unplaceable() {
    // A flow Flow holds with no launch of record (registered, not launched)
    // names no context to wake its metaflow with.
    let fixture = NexusFixture::new();
    fixture.set_agents(vec![fixture.current_agent()]);
    fixture.register_with(FlowLifecycle::Active);
    assert_eq!(
        fixture.nexus.wake_plan(&metaflow(
            Layer::Secondary,
            State::Asleep,
            vec!["908786"],
            vec![],
        )),
        WakePlan::Unplaceable
    );
    assert_eq!(
        fixture
            .nexus
            .wake_plan(&metaflow(Layer::Secondary, State::Asleep, vec![], vec![])),
        WakePlan::Unplaceable
    );
}

/// The resumed seat: a Claude flow `abc123` that slept, its launch kept.
pub(super) struct Resumable {
    profile: LaunchProfile,
    session: String,
}

const RESUMED_AGENT: &str = r#"{"agent":"claude","agent_status":"idle","interactive_ready":true,"name":"AGENT","pane_id":"w9:p1","tab_id":"w9:t1","terminal_id":"term-resumed","workspace_id":"w9","revision":1}"#;

pub(super) fn resumable(fixture: &NexusFixture) -> Resumable {
    resumable_ended_by(fixture, |store| store.record_stopped("abc123").unwrap())
}

/// The resumable Claude flow abc123, ended by `end` rather than Stopped.
fn resumable_ended_by(
    fixture: &NexusFixture,
    end: impl FnOnce(&crate::store::FlowStore) -> bool,
) -> Resumable {
    let profile = LaunchProfile {
        launch_request_id: "resumed-request".into(),
        system_modules: vec!["spirit-core".into(), "intent-flow".into()],
        user_modules: vec!["operation-main-flow".into()],
        address: signal_flow::Address {
            aspect: Aspect::Mind,
            topic: "flow".into(),
            layer: signal_flow::Layer::Secondary,
        },
        model_name: "claude-fixture".into(),
        effort: "medium".into(),
        flow_id_option: None,
        remembered_flow_vector: vec![],
        herdr_session_name: "fixture-session".into(),
        leave_outs_option: None,
        instruction_prompt: "fixture instruction".into(),
    };
    // The launch that bound abc123.
    let composed = fixture
        .nexus
        .composer
        .compose(
            crate::composition::HarnessedProfile {
                harness_kind: HarnessKind::Claude,
                launch_profile: profile.clone(),
            },
            Vec::<signal_flow::LeaveOut>::new(),
        )
        .expect("composes");
    let session = fixture
        .nexus
        .herdr
        .reserved_native_session_id(&composed)
        .expect("a Claude launch has a Flow-chosen session");
    let origin = Origin::Flow(FlowOrigin {
        flow_id: "caller".into(),
        session_id: "caller-session".into(),
    });
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
                system_prompt_sha256: composed.system_prompt.system_prompt_sha256.clone(),
                prompt_sha256_vector: composed
                    .first_prompt_payload
                    .prompt_part_vector
                    .iter()
                    .map(|part| part.prompt_sha256.clone())
                    .collect(),
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
            origin,
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
    // With the LIVE file, the snapshot shows the resumed pane running the
    // flow's own session; BUSY holds how many composer reads still show a
    // command not yet taken.
    let sessioned = agent.replace(
        r#""revision":1}"#,
        &format!(
            r#""revision":1,"agent_session":{{"source":"herdr:claude","agent":"claude","kind":"id","value":"{session}"}}}}"#
        ),
    );
    let live = fixture.directory.path().join("resumed-pane-live");
    let busy = fixture.directory.path().join("composer-busy-reads");
    let calls = fixture.directory.path().join("herdr-calls.log");
    let typed = fixture.directory.path().join("pane-typed.txt");
    let transcript = fixture
        .directory
        .path()
        .join("native-transcripts/claude/-home-li-primary")
        .join(format!("{session}.jsonl"));
    let marker = fixture.directory.path().join("marker-output");
    let script = r#"#!/bin/sh
SESSIONS
printf '%s\n' "$*" >> 'CALLS'
case "$*" in
  *"workspace create"*) printf '%s\n' '{"result":{"workspace":{"workspace_id":"w9"},"root_pane":{"pane_id":"w9:p1","terminal_id":"term-resumed"}}}' ;;
  *"pane run"*) sh -c "$6" > 'MARKER' ;;
  *"pane wait-output"*) line=$(grep -F FLOW_CLAUDE_ENV_READY_ 'MARKER'); printf '{"result":{"pane_id":"w9:p1","matched_line":"%s"}}\n' "$line" ;;
  *"agent start"*) prev=; for a in "$@"; do [ "$prev" = --system-prompt-file ] && cat "$a" > 'SYSTEMREAD'; prev=$a; done; [ -f 'ONSTARTFILE' ] && : > 'LIVE'; printf '%s\n' '{"result":{"agent":{"name":"x"}}}' ;;
  *"agent get"*) printf '%s\n' 'RECORD' ;;
  *"api snapshot"*) if [ -f 'LIVE' ]; then printf '%s\n' '{"id":"cli:api:snapshot","result":{"snapshot":{"agents":[SESSIONED],"protocol":20,"version":"0.8.2"},"type":"session_snapshot"}}'; else printf '%s\n' '{"id":"cli:api:snapshot","result":{"snapshot":{"agents":[AGENT],"protocol":20,"version":"0.8.2"},"type":"session_snapshot"}}'; fi ;;
  *"agent wait"*) printf '%s\n' '{"result":{"status":"idle"}}' ;;
  *"agent read"*) left=$(cat 'BUSY' 2>/dev/null || printf 0); if [ "$left" -gt 0 ]; then printf '%s' $((left - 1)) > 'BUSY'; printf '%s\n' '❯ /rename kept' ; else printf '%s\n' '❯ ' ; fi ;;
  *"pane close"*) printf '%s\n' '{"result":{}}' ;;
  *"agent prompt /rename "*|*"agent prompt "*" /rename "*) printf '%s\n' '{"result":{"accepted":true}}' ;;
  *"agent prompt"*) printf '%s\n--\n' "$6" >> 'TYPED'; escaped=$(printf '%s' "$6" | sed ':a;N;$!ba;s/\\/\\\\/g;s/"/\\"/g;s/\n/\\n/g'); printf '{"type":"user","sessionId":"RECEIPTSESSION","uuid":"fixture-%s","message":{"content":"%s"}}\n' "$(wc -l < 'TRANSCRIPT')" "$escaped" >> 'TRANSCRIPT'; printf '%s\n' '{"id":"cli:agent:prompt","result":{"type":"agent_prompted","agent":AGENT}}' ;;
  *) exit 64 ;;
esac
"#
    .replace(
        "ONSTARTFILE",
        &fixture
            .directory
            .path()
            .join("resume-reports-session")
            .display()
            .to_string(),
    )
    .replace("SESSIONS", &fixture.session_listing())
    .replace("CALLS", &calls.display().to_string())
    .replace(
        "SYSTEMREAD",
        &fixture
            .directory
            .path()
            .join("system-prompt-read.txt")
            .display()
            .to_string(),
    )
    .replace("MARKER", &marker.display().to_string())
    .replace("TYPED", &typed.display().to_string())
    .replace("TRANSCRIPT", &transcript.display().to_string())
    .replace("RECEIPTSESSION", &session)
    .replace("RECORD", &agent_record)
    .replace("SESSIONED", &sessioned)
    .replace("LIVE", &live.display().to_string())
    .replace("BUSY", &busy.display().to_string())
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
    fs::create_dir_all(transcript.parent().unwrap()).unwrap();
    fs::write(
        transcript,
        format!(
            "{}\n",
            serde_json::json!({
                "type": "assistant",
                "sessionId": session,
                "uuid": "fixture-end-turn",
                "message": { "stop_reason": "end_turn" }
            })
        ),
    )
    .unwrap();
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
    woken.queue.push(Request::Order("D".into()).unsent());
    fixture.nexus.store.seed_metaflow(woken);
    fixture.nexus.drain_woken(&at, lease);
    assert!(!fixture.nexus.waking.is_waking(&at), "the hold is released");

    let calls = fs::read_to_string(fixture.directory.path().join("herdr-calls.log")).unwrap();
    assert!(calls.contains("agent start"), "the harness started again");
    assert!(calls.contains(&format!(" --resume {} ", seat.session)));
    // The composed system prompt is read from the Nexus's in-memory file,
    // never typed: the start names its path, the harness reads it whole.
    let start = calls
        .lines()
        .find(|line| line.contains("agent start"))
        .expect("one start line");
    assert!(start.contains(&format!(
        " --system-prompt-file /proc/{}/fd/",
        std::process::id()
    )));
    assert!(!start.contains("These are your skills"));
    let read = fs::read_to_string(fixture.directory.path().join("system-prompt-read.txt"))
        .expect("the harness read its system prompt");
    assert!(read.contains("These are your skills"));
    assert!(!calls.contains("--session-id"));
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
    assert_eq!(prompts[0], queue.clone().unsent().prompt_text());
    assert_eq!(
        prompts[1],
        vec![Request::Order("D".into())].unsent().prompt_text()
    );

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
                .compose(
                    crate::composition::HarnessedProfile {
                        harness_kind: HarnessKind::Claude,
                        launch_profile: seat.profile.clone()
                    },
                    Vec::<signal_flow::LeaveOut>::new(),
                )
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
    assert_eq!(asleep.queue, queue.clone().unsent());
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

    // An Order queued for a Primary waits: nothing wakes a Primary.
    fixture.nexus.store.seed_metaflow(metaflow(
        Layer::Primary,
        State::Asleep,
        vec![],
        vec![Request::Order("p".into())],
    ));

    fixture.nexus.recover_wakes();
    // A second pass, as a second start would make, finds nothing due.
    fixture.nexus.recover_wakes();

    let woken = stored(&fixture, Layer::Secondary);
    assert_eq!(woken.state, State::Awake("abc123".into()));
    assert!(woken.queue.is_empty());
    let typed = fs::read_to_string(fixture.directory.path().join("pane-typed.txt")).unwrap();
    assert!(typed.starts_with(&queue.clone().unsent().prompt_text()));
    assert_eq!(
        herdr_calls(&fixture).matches("agent start").count(),
        1,
        "woken once"
    );
    assert_eq!(stored(&fixture, Layer::Tertiary).state, State::Asleep);
    let primary = stored(&fixture, Layer::Primary);
    assert_eq!(primary.state, State::Asleep);
    assert_eq!(primary.queue, vec![Request::Order("p".into())].unsent());
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
    assert_eq!(asleep.queue, queue.clone().unsent());
}

/// A Codex flow left in its recorded pane by a remote-resumed TUI: Herdr
/// recognizes the pane's Codex agent and reports no agent session.
fn codex_left_running(fixture: &NexusFixture) {
    let mut node = fixture.node();
    node.flow_lifecycle = FlowLifecycle::Active;
    fixture
        .nexus
        .store
        .register_flow(node)
        .expect("Codex seat registered");
    fixture
        .nexus
        .store
        .record_stopped("908786")
        .expect("Codex seat recorded Stopped");
    fixture.nexus.store.seed_metaflow(metaflow(
        Layer::Secondary,
        State::Asleep,
        vec!["908786"],
        vec![Request::Order("A".into())],
    ));
}

/// The wake check is Codex-aware: the pane Flow recorded for a Codex flow,
/// with Herdr recognizing a Codex agent in it and no session reported,
/// runs the flow's opened thread, so the wake is refused and the seat stays
/// Asleep with its work queued.
#[test]
fn a_wake_is_refused_while_a_remote_resumed_codex_tui_runs_the_thread() {
    let fixture = NexusFixture::new();
    codex_left_running(&fixture);
    fixture.set_agents(vec![fixture.settled_agent()]);
    assert_eq!(
        fixture
            .nexus
            .perform(Operation::Wake(address(Layer::Secondary))),
        Outcome::Failed(crate::generated::operation::Failed_Data::ConflictingBinding)
    );
    let asleep = stored(&fixture, Layer::Secondary);
    assert_eq!(asleep.state, State::Asleep);
    assert_eq!(asleep.queue, vec![Request::Order("A".into())].unsent());
}

/// A session Herdr does report for the recorded pane must be the flow's
/// thread: another thread there does not run the flow, so the wake is not
/// refused as a live session (it then has no launch of record to place).
#[test]
fn a_recorded_codex_pane_reporting_another_thread_does_not_hold_the_flow() {
    let fixture = NexusFixture::new();
    codex_left_running(&fixture);
    let mut other = fixture.settled_agent();
    other["agent_session"] = serde_json::json!({
        "source":"herdr:codex","agent":"codex","kind":"id",
        "value":"01a0b22c-0000-7000-8000-000000000000"
    });
    fixture.set_agents(vec![other]);
    assert_eq!(
        fixture
            .nexus
            .perform(Operation::Wake(address(Layer::Secondary))),
        Outcome::Failed(crate::generated::operation::Failed_Data::HerdrRefused)
    );
    assert_eq!(stored(&fixture, Layer::Secondary).state, State::Asleep);
}

/// The pane check itself: a Codex flow is held by its recorded pane's
/// Codex agent reporting no session or its own thread, and by any pane
/// reporting its thread; not by its recorded pane reporting another
/// thread, by a Codex agent without a session in another pane, by its
/// recorded pane in another Herdr session, or, for a Claude flow, by a
/// pane reporting no session.
#[test]
fn the_wake_pane_check_binds_a_codex_flow_by_pane_agent_and_thread() {
    use crate::herdr::ReadsHerdrPanes;
    let fixture = NexusFixture::new();
    let node = fixture.node();
    let held = |agents: Vec<serde_json::Value>, node: &FlowNode, session: &str| {
        fixture.set_agents(agents);
        fixture.nexus.herdr.holds_native_session(session, node)
    };
    let quiet = quiet_agent(&fixture);
    let reporting = |value: &str| {
        let mut agent = fixture.settled_agent();
        agent["agent_session"] = serde_json::json!({
            "source":"herdr:codex","agent":"codex","kind":"id","value":value
        });
        agent
    };
    let mut elsewhere = quiet_agent(&fixture);
    elsewhere["pane_id"] = serde_json::Value::String("w1:p9".into());
    elsewhere["terminal_id"] = serde_json::Value::String("term-elsewhere".into());
    let mut elsewhere_reporting = reporting(&node.session_id);
    elsewhere_reporting["pane_id"] = serde_json::Value::String("w1:p9".into());
    elsewhere_reporting["terminal_id"] = serde_json::Value::String("term-elsewhere".into());

    assert_eq!(
        held(vec![quiet.clone()], &node, "messaging-build"),
        Some(true)
    );
    assert_eq!(
        held(vec![reporting(&node.session_id)], &node, "messaging-build"),
        Some(true)
    );
    assert_eq!(
        held(vec![elsewhere_reporting], &node, "messaging-build"),
        Some(true)
    );
    assert_eq!(
        held(
            vec![reporting("01a0b22c-0000-7000-8000-000000000000")],
            &node,
            "messaging-build"
        ),
        Some(false)
    );
    assert_eq!(held(vec![elsewhere], &node, "messaging-build"), Some(false));
    assert_eq!(
        held(vec![quiet.clone()], &node, "other-session"),
        Some(false)
    );
    let mut claude = node.clone();
    claude.harness_kind = HarnessKind::Claude;
    let mut claude_agent = quiet;
    claude_agent["agent"] = serde_json::Value::String("claude".into());
    assert_eq!(
        held(vec![claude_agent], &claude, "messaging-build"),
        Some(false)
    );
}

/// The live-seat check names why a flow's recorded pane does not run its
/// own session: a stale pane (the recorded pane gone from Herdr), a missing
/// pane (none recorded), a pane running another thread or session, a Claude
/// pane reporting none, an unclaimed FlowId, an unreadable Herdr; a Codex
/// pane reporting no session runs the flow's thread.
#[test]
fn the_live_seat_check_names_why_a_recorded_pane_is_not_the_flows_own() {
    use crate::herdr::{OwnSession, ReadsHerdrPanes};
    let fixture = NexusFixture::new();
    let codex = fixture.node();
    let reporting = |harness: &str, value: &str| {
        let mut agent = quiet_agent(&fixture);
        agent["agent"] = serde_json::Value::String(harness.into());
        agent["agent_session"] = serde_json::json!({
            "source":format!("herdr:{harness}"),"agent":harness,"kind":"id","value":value
        });
        agent
    };
    let presence = |agents: Vec<serde_json::Value>, node: &FlowNode| {
        fixture.set_agents(agents);
        fixture.nexus.herdr.own_session(node)
    };

    // Codex.
    assert_eq!(
        presence(vec![quiet_agent(&fixture)], &codex),
        OwnSession::Runs
    );
    assert_eq!(
        presence(vec![reporting("codex", &codex.session_id)], &codex),
        OwnSession::Runs
    );
    assert_eq!(presence(vec![], &codex), OwnSession::PaneGone);
    let mut replaced = quiet_agent(&fixture);
    replaced["terminal_id"] = serde_json::Value::String("term-replaced".into());
    assert_eq!(presence(vec![replaced], &codex), OwnSession::PaneGone);
    assert_eq!(
        presence(
            vec![reporting("codex", "01a0b22c-0000-7000-8000-000000000000")],
            &codex
        ),
        OwnSession::OtherSession
    );
    let mut unrecorded = codex.clone();
    unrecorded.herdr_route_selection = HerdrRouteSelection::Unavailable;
    assert_eq!(
        presence(vec![quiet_agent(&fixture)], &unrecorded),
        OwnSession::NoRecordedPane
    );

    // Claude, in the same pane, claimed as the Claude seat.
    claude_seat(&fixture, 1_000);
    let mut claude = codex.clone();
    claude.flow_id = CLAUDE_SEAT.into();
    claude.session_id = CLAUDE_SESSION.into();
    claude.harness_kind = HarnessKind::Claude;
    assert_eq!(
        presence(vec![reporting("claude", CLAUDE_SESSION)], &claude),
        OwnSession::Runs
    );
    assert_eq!(
        presence(
            vec![{
                let mut agent = quiet_agent(&fixture);
                agent["agent"] = serde_json::Value::String("claude".into());
                agent
            }],
            &claude
        ),
        OwnSession::SessionUnreported
    );
    assert_eq!(
        presence(
            vec![reporting("claude", "c1a0de00-0000-4000-8000-00000000ffff")],
            &claude
        ),
        OwnSession::OtherSession
    );
    assert_eq!(presence(vec![], &claude), OwnSession::PaneGone);

    // An unclaimed FlowId and an unreadable Herdr.
    fs::remove_file(fixture.directory.path().join("flows/.908786.flow-id")).unwrap();
    assert_eq!(
        presence(vec![quiet_agent(&fixture)], &codex),
        OwnSession::Unclaimed
    );
    crate::fixture_executable::FixtureExecutable {
        path: fixture.snapshot_program.clone(),
    }
    .install("#!/bin/sh\nexit 64\n");
    assert_eq!(
        fixture.nexus.herdr.own_session(&claude),
        OwnSession::HerdrUnreadable
    );
}

/// A live Codex last flow whose recorded pane runs another thread is not
/// taken as the seat left live: the wake does not adopt it (here it then
/// has no launch to place), and the metaflow stays Asleep.
#[test]
fn a_live_codex_flow_whose_pane_runs_another_thread_is_not_adopted() {
    let fixture = NexusFixture::new();
    fixture
        .nexus
        .store
        .register_flow(fixture.node())
        .expect("Codex seat registered");
    fixture.nexus.store.seed_metaflow(metaflow(
        Layer::Secondary,
        State::Asleep,
        vec!["908786"],
        vec![Request::Order("A".into())],
    ));
    let mut other = fixture.settled_agent();
    other["agent_session"] = serde_json::json!({
        "source":"herdr:codex","agent":"codex","kind":"id",
        "value":"01a0b22c-0000-7000-8000-000000000000"
    });
    fixture.set_agents(vec![other]);
    assert_eq!(
        fixture
            .nexus
            .perform(Operation::Wake(address(Layer::Secondary))),
        Outcome::Failed(crate::generated::operation::Failed_Data::HerdrRefused)
    );
    assert_eq!(stored(&fixture, Layer::Secondary).state, State::Asleep);
}

pub(super) fn typed_prompts(fixture: &NexusFixture) -> Vec<String> {
    fs::read_to_string(fixture.directory.path().join("pane-typed.txt"))
        .unwrap_or_default()
        .split("\n--\n")
        .filter(|prompt| !prompt.is_empty())
        .map(str::to_owned)
        .collect()
}

pub(super) fn herdr_calls(fixture: &NexusFixture) -> String {
    fs::read_to_string(fixture.directory.path().join("herdr-calls.log")).unwrap_or_default()
}

/// A resumed session whose transcript already carries its title from its
/// first life is not renamed again, and its queue is typed.
#[test]
fn a_resume_whose_session_keeps_its_title_types_no_rename_and_delivers() {
    let fixture = NexusFixture::new();
    let seat = resumable(&fixture);
    let project = fixture
        .directory
        .path()
        .join("native-transcripts/claude/-home-li-primary");
    fs::create_dir_all(&project).unwrap();
    fs::write(
        project.join(format!("{}.jsonl", seat.session)),
        format!(
            "{}\n",
            serde_json::json!({"type":"custom-title","customTitle":"{ Mind flow Secondary abc123 }","sessionId":seat.session})
        ),
    )
    .unwrap();
    let at = address(Layer::Secondary);
    let queue = vec![Request::Order("A".into())];
    fixture.nexus.store.seed_metaflow(metaflow(
        Layer::Secondary,
        State::Asleep,
        vec!["abc123"],
        queue.clone(),
    ));
    assert_eq!(
        fixture.nexus.perform(Operation::Wake(at)),
        Outcome::Woken("abc123".into())
    );
    assert!(!herdr_calls(&fixture).contains("/rename"));
    assert_eq!(
        typed_prompts(&fixture),
        vec![queue.clone().unsent().prompt_text()]
    );
    let woken = stored(&fixture, Layer::Secondary);
    assert_eq!(woken.state, State::Awake("abc123".into()));
    assert!(woken.queue.is_empty());
}

/// A composer still holding a command when the work is due is waited for
/// until it clears; then the work is typed.
#[test]
fn a_resumed_composer_not_yet_clear_is_waited_for_before_the_work() {
    let fixture = NexusFixture::new();
    resumable(&fixture);
    fs::write(fixture.directory.path().join("composer-busy-reads"), "3").unwrap();
    let at = address(Layer::Secondary);
    let queue = vec![Request::Order("A".into())];
    fixture.nexus.store.seed_metaflow(metaflow(
        Layer::Secondary,
        State::Asleep,
        vec!["abc123"],
        queue.clone(),
    ));
    assert_eq!(
        fixture.nexus.perform(Operation::Wake(at)),
        Outcome::Woken("abc123".into())
    );
    assert_eq!(
        fs::read_to_string(fixture.directory.path().join("composer-busy-reads")).unwrap(),
        "0",
        "every busy read was taken before the work was typed"
    );
    let calls = herdr_calls(&fixture);
    let wait = calls
        .lines()
        .position(|line| line.contains("agent wait w9:p1"))
        .expect("the pane is waited for");
    let prompt = calls
        .lines()
        .position(|line| line.contains("agent prompt w9:p1"))
        .expect("the work is typed");
    assert!(wait < prompt);
    assert_eq!(
        typed_prompts(&fixture),
        vec![queue.clone().unsent().prompt_text()]
    );
}

/// A wake that fails once its pane is open closes that pane and records the
/// flow Stopped; its work stays queued and the metaflow Asleep, so the next
/// wake starts clean.
#[test]
fn a_wake_failing_after_its_pane_opened_closes_it_and_keeps_the_queue() {
    let fixture = NexusFixture::new();
    resumable(&fixture);
    // The resumed harness reports its session once started, so its pane is
    // proven the flow's own.
    fs::write(fixture.directory.path().join("resume-reports-session"), "").unwrap();
    // The composer never clears: nothing may be typed into it.
    fs::write(
        fixture.directory.path().join("composer-busy-reads"),
        "100000",
    )
    .unwrap();
    let at = address(Layer::Secondary);
    let queue = vec![Request::Order("A".into())];
    fixture.nexus.store.seed_metaflow(metaflow(
        Layer::Secondary,
        State::Asleep,
        vec!["abc123"],
        queue.clone(),
    ));
    assert_eq!(
        fixture.nexus.perform(Operation::Wake(at)),
        Outcome::Failed(crate::generated::operation::Failed_Data::HerdrRefused)
    );
    assert!(herdr_calls(&fixture).contains("pane close w9:p1"));
    assert!(typed_prompts(&fixture).is_empty());
    let node = fixture.nexus.store.flow_node("abc123").unwrap().unwrap();
    assert_eq!(node.flow_lifecycle, FlowLifecycle::Stopped);
    let asleep = stored(&fixture, Layer::Secondary);
    assert_eq!(asleep.state, State::Asleep);
    assert_eq!(asleep.past, vec!["abc123".to_owned()]);
    assert_eq!(asleep.queue, queue.clone().unsent());
}

/// The state a wake that failed after resuming left: the flow Pending in
/// its new pane, running its own session, the metaflow Asleep with the
/// work still queued.
fn stuck_resumed(fixture: &NexusFixture) -> Vec<Request> {
    resumable(fixture);
    let route = HerdrRoute {
        herdr_session_name: "fixture-session".into(),
        herdr_agent_name: "resumed-agent".into(),
        herdr_pane_id: "w9:p1".into(),
        herdr_terminal_id: "term-resumed".into(),
    };
    assert!(fixture.nexus.store.record_resumed("abc123", route).unwrap());
    fs::write(fixture.directory.path().join("resumed-pane-live"), "live").unwrap();
    let queue = vec![Request::Order("A".into()), Request::Order("B".into())];
    fixture.nexus.store.seed_metaflow(metaflow(
        Layer::Secondary,
        State::Asleep,
        vec!["abc123"],
        queue.clone(),
    ));
    queue
}

fn settled_and_delivered(fixture: &NexusFixture, queue: &[Request]) {
    let woken = stored(fixture, Layer::Secondary);
    assert_eq!(woken.state, State::Awake("abc123".into()));
    assert!(woken.past.is_empty());
    assert!(woken.queue.is_empty());
    let calls = herdr_calls(fixture);
    assert!(
        !calls.contains("workspace create"),
        "nothing opened: {calls}"
    );
    assert!(!calls.contains("agent start"), "nothing resumed: {calls}");
    assert_eq!(
        typed_prompts(fixture),
        queue
            .iter()
            .map(|request| vec![request.clone()].unsent().prompt_text())
            .collect::<Vec<_>>()
    );
}

/// The flow's own session live in the pane Flow recorded for it is woken
/// as it is by the next queued request, never refused as a conflict.
#[test]
fn a_seat_left_live_by_a_failed_wake_settles_on_the_next_wake() {
    let fixture = NexusFixture::new();
    let queue = stuck_resumed(&fixture);
    fixture.nexus.wake_due(address(Layer::Secondary));
    settled_and_delivered(&fixture, &queue);
}

/// The same seat settles at the Nexus's start.
#[test]
fn a_seat_left_live_by_a_failed_wake_settles_at_the_nexus_start() {
    let fixture = NexusFixture::new();
    let queue = stuck_resumed(&fixture);
    fixture.nexus.recover_wakes();
    settled_and_delivered(&fixture, &queue);
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
                origin: Origin::Flow(FlowOrigin {
                    flow_id: "da2038".into(),
                    session_id: "field-session".into(),
                }),
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
    // The woken flow reads the Order with the Sender its Lock named.
    let sent = QueuedRequest {
        sent_by: SentBy::Sender(sender_address.clone()),
        request: order.clone(),
    };
    assert!(typed.starts_with(&[sent].prompt_text()), "{typed}");
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
                sender: sender_address.clone(),
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
    assert_eq!(
        stored(&fixture, Layer::Secondary).queue,
        vec![QueuedRequest {
            sent_by: SentBy::Sender(sender_address),
            request: order
        }]
    );
}

/// A launch refused before it was confirmed gives the Address back as it
/// found it, keeping what was queued meanwhile; a flow that ends unconfirmed
/// gives back an Address that had no metaflow by removing it.
#[test]
fn an_unconfirmed_launch_that_fails_or_ends_leaves_no_awake_registration() {
    use crate::launching::LaunchesFlows;
    use crate::store::address_hold::HoldsLaunchAddress;
    use signal_flow::Response;

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
    queued.queue.push(Request::Notice("B".into()).unsent());
    fixture.nexus.store.seed_metaflow(queued);

    assert_eq!(
        fixture
            .nexus
            .settle("asleep-request", Response::StartRejected(naming_refused())),
        Response::StartRejected(naming_refused())
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

/// A fabricated Codex Stop before confirmation never stands in for an
/// app-server completion. Confirmation releases the Address hold, but the
/// queued request waits for an authentic native turn event.
#[test]
fn a_codex_stop_before_confirmation_does_not_fabricate_completion() {
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
        .push(Request::Notice("queued before confirmation".into()).unsent());
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
    while fixture
        .nexus
        .store
        .address_hold("908786")
        .unwrap()
        .is_some()
    {
        assert!(
            Instant::now() < deadline,
            "the confirmation never released its hold: {}",
            fs::read_to_string(&log).unwrap_or_default()
        );
        std::thread::sleep(Duration::from_millis(20));
    }
    assert_eq!(
        fixture.nexus.store.metaflow(&at).unwrap().unwrap().queue,
        vec![Request::Notice("queued before confirmation".into()).unsent()]
    );
    assert!(fs::read_to_string(&typed).unwrap_or_default().is_empty());
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
    retired
        .queue
        .push(Request::Order("Carry on.".into()).unsent());
    fixture.nexus.store.seed_metaflow(retired.clone());
    assert_eq!(
        fixture.nexus.wake_plan(&retired),
        WakePlan::Fresh(FreshContext {
            herdr_session_name: "fixture-session".into(),
            system_modules: vec!["spirit-core".into(), "intent-flow".into()],
            user_modules: vec!["operation-main-flow".into()],
            leave_outs_option: None,
        }),
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

fn fresh_after_refresh() -> WakePlan {
    WakePlan::Fresh(FreshContext {
        herdr_session_name: "fixture-session".into(),
        system_modules: vec!["spirit-core".into(), "intent-flow".into()],
        user_modules: vec!["operation-main-flow".into()],
        leave_outs_option: None,
    })
}

fn refresh(fixture: &NexusFixture, at: &Address) -> meta_signal_flow::Response {
    use crate::Dispatches;
    fixture
        .nexus
        .dispatch_meta(meta_signal_flow::Query::Refresh(at.clone()))
}

/// A Refresh makes the next wake fresh although the session is small enough
/// to resume, answers a repeat the same, keeps the queue and every
/// threshold, and is spent by the wake that follows.
#[test]
fn a_refresh_makes_exactly_the_next_wake_fresh_and_keeps_the_queue() {
    use crate::store::refresh::MarksFreshWakes;
    let fixture = NexusFixture::new();
    fixture.nexus.store.seed_metaflow(metaflow(
        Layer::Secondary,
        State::Awake("abc123".into()),
        vec![],
        vec![],
    ));
    resumable(&fixture);
    refresh_at(&fixture, Layer::Secondary, 200_000);
    fixture
        .nexus
        .store
        .record_context_size("abc123", 1_000)
        .unwrap();
    let at = address(Layer::Secondary);
    let mut asleep = stored(&fixture, Layer::Secondary);
    asleep.queue = vec![Request::Order("A".into()), Request::Order("B".into())].unsent();
    fixture.nexus.store.seed_metaflow(asleep.clone());
    assert!(
        matches!(fixture.nexus.wake_plan(&asleep), WakePlan::Resume { .. }),
        "without the mark the session resumes"
    );

    let accepted = meta_signal_flow::Response::Refreshed(at.clone());
    assert_eq!(refresh(&fixture, &at), accepted);
    assert_eq!(refresh(&fixture, &at), accepted, "idempotent while marked");
    assert!(fixture.nexus.store.fresh_wake_marked(&at).unwrap());
    assert_eq!(fixture.nexus.wake_plan(&asleep), fresh_after_refresh());
    assert_eq!(
        stored(&fixture, Layer::Secondary),
        asleep,
        "the queue and the past are untouched"
    );
    assert_eq!(
        fixture
            .nexus
            .store
            .configured_threshold(&Layer::Secondary)
            .unwrap()
            .map(|threshold| threshold.refresh),
        Some(200_000),
        "no threshold changed"
    );
    let other = address(Layer::Tertiary);
    assert!(
        !fixture.nexus.store.fresh_wake_marked(&other).unwrap(),
        "the mark is of one metaflow"
    );

    // The wake hands over the whole queue and records Awoken: the queue
    // leaves once and the mark with it.
    assert!(
        fixture
            .nexus
            .store
            .awaken_metaflow(&at, "fresh01", 2)
            .unwrap()
    );
    assert!(!fixture.nexus.store.fresh_wake_marked(&at).unwrap());
    let woken = stored(&fixture, Layer::Secondary);
    assert!(woken.queue.is_empty());
    assert_eq!(woken.state, State::Awake("fresh01".into()));

    // A later sleep of the resumable flow wakes by resume again.
    let mut again = metaflow(Layer::Secondary, State::Asleep, vec!["abc123"], vec![]);
    again.queue = vec![Request::Order("C".into())].unsent();
    assert!(matches!(
        fixture.nexus.wake_plan(&again),
        WakePlan::Resume { .. }
    ));
}

#[test]
fn a_refresh_is_refused_typed_unless_the_metaflow_is_asleep_with_a_launch_of_record() {
    use crate::store::refresh::MarksFreshWakes;
    use meta_signal_flow::{RefreshRejection, Response};
    let fixture = NexusFixture::new();
    let unknown = address(Layer::Tertiary);
    assert_eq!(
        refresh(&fixture, &unknown),
        Response::RefreshRejected(RefreshRejection::Unknown)
    );

    fixture.nexus.store.seed_metaflow(metaflow(
        Layer::Tertiary,
        State::Awake("aaaaaa".into()),
        vec![],
        vec![],
    ));
    assert_eq!(
        refresh(&fixture, &unknown),
        Response::RefreshRejected(RefreshRejection::Awake)
    );

    let ended = address(Layer::Quaternary);
    fixture
        .nexus
        .store
        .seed_metaflow(metaflow(Layer::Quaternary, State::Ended, vec![], vec![]));
    assert_eq!(
        refresh(&fixture, &ended),
        Response::RefreshRejected(RefreshRejection::Ended)
    );

    // Asleep, but no launch of record names its last flow, or it has no
    // last flow: a fresh wake has nothing to compose from.
    let unlaunched = address(Layer::Secondary);
    fixture.nexus.store.seed_metaflow(metaflow(
        Layer::Secondary,
        State::Asleep,
        vec!["908786"],
        vec![],
    ));
    assert_eq!(
        refresh(&fixture, &unlaunched),
        Response::RefreshRejected(RefreshRejection::NoLaunchProfile)
    );
    fixture
        .nexus
        .store
        .seed_metaflow(metaflow(Layer::Secondary, State::Asleep, vec![], vec![]));
    assert_eq!(
        refresh(&fixture, &unlaunched),
        Response::RefreshRejected(RefreshRejection::NoLaunchProfile)
    );
    for at in [&unknown, &ended, &unlaunched] {
        assert!(
            !fixture.nexus.store.fresh_wake_marked(at).unwrap(),
            "a refused Refresh marks nothing"
        );
    }
}

fn naming_refused() -> signal_flow::StartRejection {
    signal_flow::StartRejection::NativeLaunchRefused(signal_flow::NativeLaunchRefusal {
        native_launch_stage: signal_flow::NativeLaunchStage::Naming,
        native_launch_failure: signal_flow::NativeLaunchFailure::NotReady,
    })
}

/// A whole fresh launch run by a wake, against fakes of everything outside
/// the Nexus: a Herdr that opens one pane (w7:p1), starts the harness the
/// Nexus asks for in it and reports it as Herdr 0.8.2 does (a Claude pane
/// with its session, a remote-resumed Codex pane with none), labels it and
/// lists it; a Claude-side Herdr server socket that, given the typed first
/// part, writes the user turn and the flow's answer to the Claude
/// transcript; and a Codex app-server that opens one new thread, names it
/// and reads its settings back.
struct FreshWorld {
    state: std::path::PathBuf,
    codex_invocations: std::path::PathBuf,
}

const FRESH_THREAD: &str = "01a0b22c-f00d-7000-8000-00000000f7e5";
const FRESH_CODEX_FLOW: &str = "f7e5a1";

/// One server-to-client WebSocket text frame, as `printf %b` octal escapes.
fn websocket_frame(json: &str) -> String {
    let mut bytes = vec![0x81];
    match u8::try_from(json.len()) {
        Ok(short) if short < 126 => bytes.push(short),
        _ => {
            bytes.push(126);
            bytes.extend_from_slice(&u16::try_from(json.len()).unwrap().to_be_bytes());
        }
    }
    bytes.extend_from_slice(json.as_bytes());
    bytes.iter().map(|byte| format!("\\{byte:03o}")).collect()
}

trait BuildsFreshWorld {
    /// Installs the fake Herdr, the Claude-side Herdr server socket for the
    /// session `fixture-session`, and the Codex app-server, all with empty
    /// state.
    fn fresh_world(&mut self, codex_title: &str) -> FreshWorld;
}

impl BuildsFreshWorld for NexusFixture {
    fn fresh_world(&mut self, codex_title: &str) -> FreshWorld {
        use crate::herdr::ConfiguresHerdrCli;
        let root = self.directory.path().to_path_buf();
        let state = root.join("fresh-world");
        fs::create_dir_all(&state).unwrap();
        let claude_root = root.join("native-transcripts/claude");
        fs::create_dir_all(&claude_root).unwrap();
        let herdr = format!(
            r##"#!/bin/sh
{sessions}
S='{state}'
printf '%s\n' "$*" >> "$S/calls"
# The first pane is w7:p1 on terminal term-fresh; each later one is the next
# workspace, w8:p1 on term-fresh-8 and so on.
ws=$(cat "$S/workspace" 2>/dev/null || printf 7); pane="w$ws:p1"
term=term-fresh; [ "$ws" = 7 ] || term="term-fresh-$ws"
agent() {{
  harness=$(cat "$S/harness"); title=$(cat "$S/title" 2>/dev/null)
  session=''
  [ "$harness" = claude ] && session=$(printf ',"agent_session":{{"source":"herdr:claude","agent":"claude","kind":"id","value":"%s"}}' "$(cat "$S/session")")
  printf '{{"name":"%s","agent":"%s","agent_status":"idle","workspace_id":"w%s","pane_id":"%s","terminal_id":"%s","interactive_ready":true,"terminal_title_stripped":"%s"%s}}' "$(cat "$S/name")" "$harness" "$ws" "$pane" "$term" "$title" "$session"
}}
case "$*" in
  *"workspace create"*)
    if [ -f "$S/workspace" ]; then ws=$((ws + 1)); fi; printf '%s' "$ws" > "$S/workspace"
    term=term-fresh; [ "$ws" = 7 ] || term="term-fresh-$ws"
    printf '{{"result":{{"workspace":{{"workspace_id":"w%s"}},"root_pane":{{"pane_id":"w%s:p1","terminal_id":"%s"}}}}}}\n' "$ws" "$ws" "$term" ;;
  *"pane run"*) sh -c "$6" > "$S/marker" ;;
  *"pane wait-output"*) line=$(grep -F FLOW_CLAUDE_ENV_READY_ "$S/marker"); printf '{{"result":{{"pane_id":"%s","matched_line":"%s"}}}}\n' "$pane" "$line" ;;
  *"agent start"*)
    printf '%s' "$5" > "$S/name"; harness=codex; prev=
    for a in "$@"; do
      case "$prev" in
        --session-id) harness=claude; printf '%s' "$a" > "$S/session" ;;
        --name) printf '%s' "$a" > "$S/title" ;;
        resume) printf '%s' "$a" > "$S/session" ;;
      esac
      prev=$a
    done
    printf '%s' "$harness" > "$S/harness"
    # The Codex TUI runs `resume <thread>` in the pane's foreground.
    if [ "$harness" = codex ]; then
      sh -c 'sleep 120; :' resume "$(cat "$S/session")" < /dev/null > /dev/null 2>&1 &
      printf '%s' "$!" > "$S/pid"
    fi
    if [ "$harness" = claude ]; then
      printf '{{"type":"custom-title","customTitle":"%s","sessionId":"%s"}}\n' "$(cat "$S/title")" "$(cat "$S/session")" >> '{claude_root}/'"$(cat "$S/session")"'.jsonl'
    fi
    # As Herdr 0.8.2 does, a Codex start returns only once the agent is idle:
    # with START_WAITS, once its first turn has ended (TURN_DONE), else it
    # times out as Herdr does. START_REFUSED then refuses the start.
    if [ "$harness" = codex ] && [ -f "$S/start-waits" ]; then
      i=0; while [ ! -f "$S/turn-done" ] && [ "$i" -lt 200 ]; do sleep 0.05; i=$((i + 1)); done
      if [ ! -f "$S/turn-done" ]; then printf '%s\n' '{{"id":"cli:agent:start","error":{{"code":"timeout","message":"timed out waiting for agent startup"}}}}'; exit 1; fi
    fi
    if [ "$harness" = codex ] && [ -f "$S/start-refused" ]; then printf '%s\n' '{{"id":"cli:agent:start","error":{{"code":"agent_start_failed","message":"refused after the first turn"}}}}'; exit 1; fi
    printf '{{"result":{{"agent":{{"name":"%s"}}}}}}\n' "$5" ;;
  *"agent wait"*) printf '%s\n' '{{"result":{{"status":"idle"}}}}' ;;
  *"agent get"*) printf '{{"result":{{"agent":%s}}}}\n' "$(agent)" ;;
  *"api snapshot"*) a=; [ -f "$S/harness" ] && a=$(agent); printf '{{"id":"cli:api:snapshot","result":{{"snapshot":{{"agents":[%s],"protocol":20,"version":"0.8.2"}},"type":"session_snapshot"}}}}\n' "$a" ;;
  *"pane rename "*) printf '%s' "$6" > "$S/label"; printf '%s\n' '{{"result":{{}}}}' ;;
  *"pane get "*) printf '{{"result":{{"pane":{{"pane_id":"%s","terminal_id":"%s","label":"%s"}}}}}}\n' "$pane" "$term" "$(cat "$S/label")" ;;
  *"pane process-info"*) printf '{{"result":{{"process_info":{{"pane_id":"%s","foreground_processes":[{{"pid":%s}}]}}}}}}\n' "$pane" "$(cat "$S/pid" 2>/dev/null || printf 1)" ;;
  *"pane close"*) printf '%s\n' '{{"result":{{}}}}' ;;
  *) exit 64 ;;
esac
"##,
            sessions = self.session_listing(),
            state = state.display(),
            claude_root = claude_root.display(),
        );
        FixtureExecutable {
            path: self.snapshot_program.clone(),
        }
        .install(&herdr);

        // The Claude-side Herdr server: the typed part becomes the user turn,
        // and the flow answers it, in the session's own transcript.
        let socket = root.join("fresh-herdr.sock");
        let listener = std::os::unix::net::UnixListener::bind(&socket).unwrap();
        let served_state = state.clone();
        let served_root = claude_root.clone();
        std::thread::spawn(move || {
            use std::io::{BufRead, Write};
            for stream in listener.incoming() {
                let Ok(mut stream) = stream else { return };
                let mut line = String::new();
                if std::io::BufReader::new(&stream)
                    .read_line(&mut line)
                    .is_err()
                {
                    continue;
                }
                let request: serde_json::Value = serde_json::from_str(&line).unwrap_or_default();
                let text = request
                    .pointer("/params/text")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or_default();
                let session = fs::read_to_string(served_state.join("session")).unwrap_or_default();
                let rows = [
                    serde_json::json!({"type":"user","sessionId":session,"message":{"role":"user","content":text}}),
                    serde_json::json!({"type":"assistant","sessionId":session,"uuid":"fresh-answer","message":{"model":"claude-fixture","content":[{"type":"text","text":"Woken."}]}}),
                ];
                let _ = fs::OpenOptions::new()
                    .append(true)
                    .create(true)
                    .open(served_root.join(format!("{session}.jsonl")))
                    .and_then(|mut file| rows.iter().try_for_each(|row| writeln!(file, "{row}")));
                let _ = stream.write_all(
                    b"{\"id\":\"flow:prompt\",\"result\":{\"type\":\"agent_prompted\"}}\n",
                );
            }
        });
        self.list_herdr_sessions(&[("fixture-session", socket)]);

        // The Codex app-server: the send-tool probe, the opening, the naming
        // and the settings readback a fresh launch makes, in that order, each
        // its own proxy; with START_REFUSED, the account of the thread whose
        // start was refused in place of the naming. Each connection's client
        // frames are kept (record-N). The opening connection is subscribed
        // to the thread it started: with FIRST_CALL it sends that server
        // request once the first turn has started, as the app-server sends a
        // first turn's tool call to every connection subscribed to its
        // thread, and stays open.
        let invocations = state.join("codex-invocations");
        let opened = |id: u64| {
            websocket_frame(
                &serde_json::json!({"id":id,"result":{"thread":{"id":FRESH_THREAD},"model":"fixture-model","reasoningEffort":"medium","instructionSources":[]}})
                    .to_string(),
            )
        };
        let initialized = websocket_frame(r#"{"id":1,"result":{}}"#);
        let probe = [
            initialized.clone(),
            websocket_frame(
                r#"{"id":2,"error":{"code":-32600,"message":"thread/start.dynamicTools requires experimentalApi capability"}}"#,
            ),
        ]
        .concat();
        let opening = [
            initialized.clone(),
            opened(2),
            websocket_frame(r#"{"id":3,"result":{"turn":{"id":"turn-fresh"}}}"#),
        ]
        .concat();
        let naming = [
            initialized.clone(),
            websocket_frame(r#"{"id":2,"result":{}}"#),
            websocket_frame(
                &serde_json::json!({"id":3,"result":{"thread":{"id":FRESH_THREAD,"name":codex_title}}})
                    .to_string(),
            ),
        ]
        .concat();
        let readback = [initialized.clone(), opened(2)].concat();
        let account = [
            initialized,
            websocket_frame(r#"{"id":2,"result":{"data":[{"status":"completed"}]}}"#),
            websocket_frame(r#"{"id":3,"result":{}}"#),
        ]
        .concat();
        let app_server = root.join("fresh-codex");
        FixtureExecutable {
            path: app_server.clone(),
        }
        .install(&format!(
            "#!/bin/sh\nS='{state}'\nn=$(cat '{counter}' 2>/dev/null || printf 0); n=$((n + 1)); printf '%s' \"$n\" > '{counter}'\nwhile IFS= read -r line; do line=$(printf '%s' \"$line\" | tr -d '\\r'); [ -z \"$line\" ] && break; done\nexec 3<&0; cat <&3 > \"$S/record-$n\" &\nprintf '%b' 'HTTP/1.1 101 Switching Protocols\\r\\nUpgrade: websocket\\r\\nConnection: Upgrade\\r\\nSec-WebSocket-Accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo=\\r\\n\\r\\n'\ncase \"$n\" in\n  1) printf '%b' '{probe}' ;;\n  2) printf '%b' '{opening}'; [ -f \"$S/first-call\" ] && printf '%b' \"$(cat \"$S/first-call\")\"; exec sleep 30 ;;\n  3) if [ -f \"$S/start-refused\" ]; then printf '%b' '{account}'; else printf '%b' '{naming}'; fi ;;\n  4) printf '%b' '{readback}' ;;\n  *) exit 1 ;;\nesac\nexec sleep 5\n",
            state = state.display(),
            counter = invocations.display(),
        ));
        self.nexus.codex_endpoints.stable.client_path = app_server;
        // A Codex TUI is started only against an absolute endpoint socket.
        self.nexus.codex_endpoints.stable.socket =
            root.join("codex.sock").to_string_lossy().into_owned();
        let endpoints = self.nexus.codex_endpoints.clone();
        let flows = root.join("flows");
        let herdr = std::mem::replace(
            &mut self.nexus.herdr,
            crate::herdr::HerdrCli::at(self.snapshot_program.clone(), flows),
        );
        self.nexus.herdr = herdr.with_codex_endpoints(endpoints);
        FreshWorld {
            state,
            codex_invocations: invocations,
        }
    }
}

fn configure_secondary_model(fixture: &NexusFixture, native: &str) {
    use crate::store::SeedsConfiguration;
    // A wake reads its layer's model from the committed configuration,
    // which exists once the Nexus family is declared.
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
                message_nexus_binary: "/fixture/message-nexus".into(),
                lease: 30,
            },
        ))
        .expect("Nexus configured");
    fixture
        .nexus
        .store
        .configure_change(meta_signal_flow::ConfigurationChange::Models(vec![
            meta_signal_flow::ConfigureModel {
                layer: Layer::Secondary,
                native: native.into(),
            },
        ]))
        .expect("model configured");
}

/// A Refresh'd sleeping Codex address wakes fresh: the Nexus opens a new
/// thread through the app-server, attaches a TUI to it in a new pane, binds
/// the flow to that thread although Herdr reports no session, names it,
/// reads its settings back, reads the receipt from the new rollout, and
/// records the address Awake as the new flow. The old thread and its flow
/// are untouched: its rollout is byte-identical, nothing addresses it, and
/// the old flow stays Stopped on it.
#[test]
fn a_refreshed_codex_address_wakes_fresh_to_a_new_thread_and_binds_to_it() {
    use crate::title::{ShowsNativeTitle, TitlesFlow};
    let mut fixture = NexusFixture::new();
    // The old Codex flow 908786 on thread OLD, Stopped, its launch of record.
    let old = fixture.node();
    let old_thread = old.session_id.clone();
    let profile = LaunchProfile {
        launch_request_id: "old-codex-request".into(),
        system_modules: vec!["spirit-core".into(), "intent-flow".into()],
        user_modules: vec!["operation-main-flow".into()],
        address: address(Layer::Secondary),
        model_name: "fixture-model".into(),
        effort: "medium".into(),
        flow_id_option: None,
        remembered_flow_vector: vec![],
        herdr_session_name: "fixture-session".into(),
        leave_outs_option: None,
        instruction_prompt: "fixture instruction".into(),
    };
    let composed = fixture
        .nexus
        .composer
        .compose(
            crate::composition::HarnessedProfile {
                harness_kind: HarnessKind::Codex,
                launch_profile: profile.clone(),
            },
            Vec::<signal_flow::LeaveOut>::new(),
        )
        .unwrap();
    fixture
        .nexus
        .store
        .reserve_launch_attempt(&composed, Origin::Unbound)
        .unwrap();
    assert!(
        fixture
            .nexus
            .store
            .record_native_launch_intent(NativeLaunchIntent {
                launch_request_id: profile.launch_request_id.clone(),
                prompt_sha256: composed.first_prompt_payload.prompt_sha256.clone(),
                harness_kind: HarnessKind::Codex,
                model_name: profile.model_name.clone(),
                effort: profile.effort.clone(),
                system_prompt_sha256: composed.system_prompt.system_prompt_sha256.clone(),
                prompt_sha256_vector: composed.part_hashes(),
            })
            .unwrap()
    );
    assert!(
        fixture
            .nexus
            .store
            .record_native_launch_binding(NativeLaunchBinding {
                launch_request_id: profile.launch_request_id.clone(),
                flow_id: old.flow_id.clone(),
                native_session_id: old_thread.clone(),
                harness_kind: HarnessKind::Codex,
                herdr_pane_binding: signal_flow::HerdrPaneBinding {
                    launch_request_id: profile.launch_request_id.clone(),
                    herdr_session_name: "fixture-session".into(),
                    herdr_agent_name: composed.launch_agent_name(),
                    herdr_workspace_id: "w1".into(),
                    herdr_pane_id: "w1:p3".into(),
                    herdr_terminal_id: "term-old".into(),
                },
            })
            .unwrap()
    );
    let mut old_node = old.clone();
    old_node.herdr_route_selection = HerdrRouteSelection::Available(HerdrRoute {
        herdr_session_name: "fixture-session".into(),
        herdr_agent_name: composed.launch_agent_name(),
        herdr_pane_id: "w1:p3".into(),
        herdr_terminal_id: "term-old".into(),
    });
    fixture.nexus.store.register_flow(old_node).unwrap();
    assert!(fixture.nexus.store.record_stopped(&old.flow_id).unwrap());
    let rollouts = fixture.directory.path().join("native-transcripts/codex");
    fs::create_dir_all(&rollouts).unwrap();
    let old_rollout = rollouts.join(format!("rollout-{old_thread}.jsonl"));
    fs::write(
        &old_rollout,
        format!(
            "{}\n",
            serde_json::json!({"type":"session_meta","payload":{"id":old_thread}})
        ),
    )
    .unwrap();
    let old_bytes = fs::read(&old_rollout).unwrap();
    let queue = vec![Request::Order("A".into())];
    fixture.nexus.store.seed_metaflow(metaflow(
        Layer::Secondary,
        State::Asleep,
        vec![old.flow_id.as_str()],
        queue.clone(),
    ));
    configure_secondary_model(&fixture, "fixture-model");
    let at = address(Layer::Secondary);
    assert_eq!(
        refresh(&fixture, &at),
        meta_signal_flow::Response::Refreshed(at.clone())
    );

    // The fresh seat the wake composes, and so the first turn the new thread
    // holds, and the title it is named with.
    let fresh = FreshSeat {
        address: at.clone(),
        launch_request_id: "any".into(),
        model_name: "fixture-model".into(),
        effort: "medium".into(),
        herdr_session_name: "fixture-session".into(),
        system_modules: profile.system_modules.clone(),
        user_modules: profile.user_modules.clone(),
        leave_outs_option: None,
        prompt: queue.clone().unsent().prompt_text(),
    }
    .fresh_profile();
    let title = fresh.native_title(FRESH_CODEX_FLOW).unwrap();
    let first_part = fixture
        .nexus
        .composer
        .compose(
            crate::composition::HarnessedProfile {
                harness_kind: HarnessKind::Codex,
                launch_profile: fresh,
            },
            Vec::<signal_flow::LeaveOut>::new(),
        )
        .unwrap()
        .first_prompt_payload
        .prompt_part_vector[0]
        .prompt_part_text
        .clone();
    let world = fixture.fresh_world(title.as_str());
    let rows = [
        serde_json::json!({"type":"session_meta","payload":{"id":FRESH_THREAD}}),
        serde_json::json!({"type":"turn_context","payload":{"model":"fixture-model","effort":"medium","turn_id":"turn-fresh"}}),
        serde_json::json!({"type":"event_msg","payload":{"thread_id":FRESH_THREAD,"turn_id":"turn-fresh","item":{"type":"UserMessage","content":[{"type":"text","text":first_part}]}}}),
        serde_json::json!({"type":"response_item","payload":{"type":"message","role":"assistant","content":[{"type":"output_text","text":"Woken."}]}}),
    ];
    fs::write(
        rollouts.join(format!("rollout-{FRESH_THREAD}.jsonl")),
        rows.iter()
            .map(|row| format!("{row}\n"))
            .collect::<String>(),
    )
    .unwrap();
    // The new thread's FlowId claim, as `flow-id codex` makes it.
    FixtureExecutable {
        path: fixture.directory.path().join("native-transcripts/flow-id"),
    }
    .install(&format!(
        "#!/bin/sh\n[ \"$1\" = codex ] && [ \"$CODEX_SESSION_ID\" = '{FRESH_THREAD}' ] || exit 2\nprintf 'version=1\\nharness=codex\\nidentity={identity}\\nalias={FRESH_CODEX_FLOW}\\n' > \"$3/.{FRESH_CODEX_FLOW}.flow-id\"\nprintf '{FRESH_CODEX_FLOW}\\n'\n",
        identity = FRESH_THREAD.replace('-', ""),
    ));

    let outcome = fixture.nexus.perform(Operation::Wake(at.clone()));
    assert_eq!(
        outcome,
        Outcome::Woken(FRESH_CODEX_FLOW.into()),
        "Herdr calls: {}; app-server invocations: {:?}",
        fs::read_to_string(world.state.join("calls")).unwrap_or_default(),
        fs::read_to_string(&world.codex_invocations).ok()
    );

    let woken = stored(&fixture, Layer::Secondary);
    assert_eq!(woken.state, State::Awake(FRESH_CODEX_FLOW.into()));
    assert!(woken.queue.is_empty());
    assert_eq!(woken.past, vec![old.flow_id.clone()]);
    let new_node = fixture
        .nexus
        .store
        .flow_node(FRESH_CODEX_FLOW)
        .unwrap()
        .unwrap();
    assert_eq!(new_node.session_id, FRESH_THREAD);
    assert_eq!(new_node.harness_kind, HarnessKind::Codex);
    assert_eq!(new_node.flow_lifecycle, FlowLifecycle::Active);
    let old_after = fixture
        .nexus
        .store
        .flow_node(&old.flow_id)
        .unwrap()
        .unwrap();
    assert_eq!(old_after.session_id, old_thread);
    assert_eq!(old_after.flow_lifecycle, FlowLifecycle::Stopped);
    assert_eq!(fs::read(&old_rollout).unwrap(), old_bytes);
    // Exactly the probe, the opening, the naming and the readback reached
    // the app-server; the naming and the readback each read back the new
    // thread, so none addressed another.
    assert_eq!(fs::read_to_string(&world.codex_invocations).unwrap(), "4");
    let calls = fs::read_to_string(world.state.join("calls")).unwrap();
    assert!(calls.contains(&format!("resume {FRESH_THREAD}")), "{calls}");
    assert!(!calls.contains(&old_thread), "{calls}");
    assert_eq!(
        fs::read_to_string(world.state.join("label")).unwrap(),
        title.as_str()
    );
    use crate::store::refresh::MarksFreshWakes;
    assert!(!fixture.nexus.store.fresh_wake_marked(&at).unwrap());
}

/// The Claude equivalent: a Refresh'd sleeping Claude address wakes fresh
/// to a new native session Flow chose, not a resume of the old one; the new
/// flow binds to that session and the old flow keeps its own.
#[test]
fn a_refreshed_claude_address_wakes_fresh_to_a_new_session_and_binds_to_it() {
    let mut fixture = NexusFixture::new();
    let seat = resumable(&fixture);
    fixture.claims_flow_ids();
    let queue = vec![Request::Order("A".into())];
    fixture.nexus.store.seed_metaflow(metaflow(
        Layer::Secondary,
        State::Asleep,
        vec!["abc123"],
        queue,
    ));
    configure_secondary_model(&fixture, "claude-fixture");
    let at = address(Layer::Secondary);
    assert_eq!(
        refresh(&fixture, &at),
        meta_signal_flow::Response::Refreshed(at.clone())
    );
    let world = fixture.fresh_world("unused");

    let Outcome::Woken(flow_id) = fixture.nexus.perform(Operation::Wake(at.clone())) else {
        panic!(
            "the fresh Claude wake starts; Herdr calls: {}",
            fs::read_to_string(world.state.join("calls")).unwrap_or_default()
        );
    };
    assert_ne!(flow_id, "abc123");
    let woken = stored(&fixture, Layer::Secondary);
    assert_eq!(woken.state, State::Awake(flow_id.clone()));
    assert!(woken.queue.is_empty());
    let new_node = fixture.nexus.store.flow_node(&flow_id).unwrap().unwrap();
    let new_session = fs::read_to_string(world.state.join("session")).unwrap();
    assert_eq!(new_node.session_id, new_session);
    assert_ne!(new_session, seat.session);
    assert_eq!(new_node.harness_kind, HarnessKind::Claude);
    assert_eq!(new_node.flow_lifecycle, FlowLifecycle::Active);
    let old_after = fixture.nexus.store.flow_node("abc123").unwrap().unwrap();
    assert_eq!(old_after.session_id, seat.session);
    assert_eq!(old_after.flow_lifecycle, FlowLifecycle::Stopped);
    let calls = fs::read_to_string(world.state.join("calls")).unwrap();
    assert!(
        calls.contains(&format!("--session-id {new_session}")),
        "{calls}"
    );
    assert!(!calls.contains("--resume"), "{calls}");
    assert!(!calls.contains(&seat.session), "{calls}");
}

/// The first-turn send of a fresh Codex launch, against the fresh world: a
/// Codex seat at SENDER whose first turn calls the send tool before it is
/// ever idle, and the Refresh'd sleeping Claude seat abc123 the call
/// orders.
struct FirstTurnSend {
    world: FreshWorld,
    sender: LaunchProfile,
    recipient: Address,
}

/// The id of the first turn's tool call as the app-server sends it.
const FIRST_CALL_REQUEST: u64 = 77;
const FIRST_CALL_BODY: &str = "Wake and reply.";

fn first_call_key() -> crate::generated::send_tool::ToolCallKey {
    crate::generated::send_tool::ToolCallKey {
        thread_id: FRESH_THREAD.into(),
        turn_id: "turn-fresh".into(),
        call_id: "call-first".into(),
    }
}

trait SetsUpFirstTurnSend {
    /// The fresh world with the sender's first turn calling `send` towards
    /// `recipient`, Herdr's start returning only once that turn ended, the
    /// rollout and the FlowId claims a fresh Codex and a fresh Claude launch
    /// read.
    fn first_turn_send(&mut self, recipient: Address) -> FirstTurnSend;
}

impl SetsUpFirstTurnSend for NexusFixture {
    fn first_turn_send(&mut self, recipient: Address) -> FirstTurnSend {
        use crate::store::SeedsConfiguration;
        use crate::title::{ShowsNativeTitle, TitlesFlow};
        let sender = LaunchProfile {
            launch_request_id: "first-turn-send".into(),
            system_modules: vec!["spirit-core".into(), "intent-flow".into()],
            user_modules: vec!["operation-main-flow".into()],
            address: Address {
                aspect: Aspect::Field,
                topic: "send-proof".into(),
                layer: Layer::Tertiary,
            },
            model_name: "fixture-model".into(),
            effort: "medium".into(),
            flow_id_option: None,
            remembered_flow_vector: vec![],
            herdr_session_name: "fixture-session".into(),
            leave_outs_option: None,
            instruction_prompt: "Call the send tool once.".into(),
        };
        let settings = crate::store::DefaultConfiguration {
            home: self.directory.path().join("home"),
            runtime_directory: self.directory.path().join("runtime"),
        }
        .configuration();
        self.nexus
            .store
            .configure_change(meta_signal_flow::ConfigurationChange::Nexus(
                meta_signal_flow::ConfigureNexus {
                    source_root: settings.source_root,
                    stable_codex: settings.stable_codex,
                    next_codex: settings.next_codex,
                    harness_profiles: settings.harness_profile_vector,
                    meta_aspects: settings.meta_aspects,
                    message_nexus_path: settings.message_nexus_path,
                    message_nexus_binary: "/fixture/message-nexus".into(),
                    lease: 30,
                },
            ))
            .expect("Nexus configured");
        self.nexus
            .store
            .configure_change(meta_signal_flow::ConfigurationChange::Models(vec![
                meta_signal_flow::ConfigureModel {
                    layer: Layer::Secondary,
                    native: "claude-fixture".into(),
                },
                meta_signal_flow::ConfigureModel {
                    layer: Layer::Tertiary,
                    native: "fixture-model".into(),
                },
            ]))
            .expect("models configured");
        let first_part = self
            .nexus
            .composer
            .compose(
                crate::composition::HarnessedProfile {
                    harness_kind: HarnessKind::Codex,
                    launch_profile: sender.clone(),
                },
                Vec::<signal_flow::LeaveOut>::new(),
            )
            .unwrap()
            .first_prompt_payload
            .prompt_part_vector[0]
            .prompt_part_text
            .clone();
        let title = sender.native_title(FRESH_CODEX_FLOW).unwrap();
        let world = self.fresh_world(title.as_str());
        // Herdr 0.8.2: `agent start` returns once the agent is idle.
        fs::write(world.state.join("start-waits"), "").unwrap();
        let call = serde_json::json!({
            "id": FIRST_CALL_REQUEST,
            "method": "item/tool/call",
            "params": {
                "threadId": FRESH_THREAD,
                "turnId": "turn-fresh",
                "callId": "call-first",
                "tool": "send",
                "arguments": {
                    "target": {
                        "aspect": "Mind",
                        "topic": recipient.topic,
                        "layer": format!("{:?}", recipient.layer)
                    },
                    "kind": "Order",
                    "body": FIRST_CALL_BODY
                }
            }
        });
        fs::write(
            world.state.join("first-call"),
            websocket_frame(&call.to_string()),
        )
        .unwrap();
        let rollouts = self.directory.path().join("native-transcripts/codex");
        fs::create_dir_all(&rollouts).unwrap();
        let rows = [
            serde_json::json!({"type":"session_meta","payload":{"id":FRESH_THREAD}}),
            serde_json::json!({"type":"turn_context","payload":{"model":"fixture-model","effort":"medium","turn_id":"turn-fresh"}}),
            serde_json::json!({"type":"event_msg","payload":{"thread_id":FRESH_THREAD,"turn_id":"turn-fresh","item":{"type":"UserMessage","content":[{"type":"text","text":first_part}]}}}),
            serde_json::json!({"type":"response_item","payload":{"type":"message","role":"assistant","content":[{"type":"output_text","text":"Queued."}]}}),
        ];
        fs::write(
            rollouts.join(format!("rollout-{FRESH_THREAD}.jsonl")),
            rows.iter()
                .map(|row| format!("{row}\n"))
                .collect::<String>(),
        )
        .unwrap();
        // `flow-id codex` for the new thread, `flow-id claude` for a fresh
        // Claude session.
        FixtureExecutable {
            path: self.directory.path().join("native-transcripts/flow-id"),
        }
        .install(&format!(
            r#"#!/bin/sh
if [ "$1" = codex ]; then
  [ "$CODEX_SESSION_ID" = '{FRESH_THREAD}' ] || exit 2
  printf 'version=1\nharness=codex\nidentity={identity}\nalias={FRESH_CODEX_FLOW}\n' > "$3/.{FRESH_CODEX_FLOW}.flow-id"
  printf '{FRESH_CODEX_FLOW}\n'
  exit 0
fi
[ "$1" = claude ] && [ "$2" = --flows-root ] && [ "$4" = --parent-session ] || exit 2
identity=$(printf '%s' "$5" | tr -d -)
alias=$(printf '%s' "$identity" | cut -c1-6)
: > "$3/.$alias.flow-id.lock"
mkdir -p "$3/$alias"
printf 'version=1\nharness=claude\nidentity=%s\nalias=%s\nuuid-version=uuid-v5\n' "$identity" "$alias" > "$3/.$alias.flow-id"
printf '%s\n' "$alias"
"#,
            identity = FRESH_THREAD.replace('-', ""),
        ));
        FirstTurnSend {
            world,
            sender,
            recipient,
        }
    }
}

/// Stands for the model's first turn: once the answer to the tool call is
/// written on the launch responder's connection, the one that opened the
/// thread (record-2), the turn ends
/// and Herdr shows the seat idle. Returns that answer.
fn first_turn_ends_when_answered(
    state: std::path::PathBuf,
) -> std::thread::JoinHandle<Option<serde_json::Value>> {
    use super::send_tool::ReadsClientFrames;
    std::thread::spawn(move || {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
        while std::time::Instant::now() < deadline {
            let answer = state
                .join("record-2")
                .client_messages()
                .into_iter()
                .find(|message| {
                    message.get("id") == Some(&serde_json::json!(FIRST_CALL_REQUEST))
                        && message.get("result").is_some()
                });
            if let Some(answer) = answer {
                fs::write(state.join("turn-done"), "").unwrap();
                return Some(answer);
            }
            std::thread::sleep(std::time::Duration::from_millis(20));
        }
        None
    })
}

/// A fresh Codex launch whose first turn calls `send` before the seat is
/// ever idle, while Herdr's start waits for idle as Herdr 0.8.2 does: the
/// launch's own responder, subscribed before the first turn, answers the
/// call during the start; the request is queued once, sent from the
/// launching metaflow; the launch completes; and the Refresh'd sleeping
/// recipient wakes fresh once.
#[test]
fn a_first_turn_that_calls_send_before_idle_is_served_and_the_launch_completes() {
    use super::send_tool::ReadsClientFrames;
    use crate::DispatchesSerially;
    use crate::store::refresh::MarksFreshWakes;
    use crate::store::send_call::KeepsSendCalls;
    let mut fixture = NexusFixture::new();
    resumable(&fixture);
    let recipient = address(Layer::Secondary);
    fixture.nexus.store.seed_metaflow(metaflow(
        Layer::Secondary,
        State::Asleep,
        vec!["abc123"],
        vec![],
    ));
    let setup = fixture.first_turn_send(recipient.clone());
    assert_eq!(
        refresh(&fixture, &recipient),
        meta_signal_flow::Response::Refreshed(recipient.clone())
    );
    let turn = first_turn_ends_when_answered(setup.world.state.clone());

    let response = fixture.nexus.dispatch_serially(
        signal_flow::Query::Start(setup.sender.clone()),
        Origin::Unbound,
    );

    let answer = turn.join().unwrap();
    let calls = fs::read_to_string(setup.world.state.join("calls")).unwrap_or_default();
    let signal_flow::Response::Started(started) = &response else {
        panic!("the launch completes: {response:?}; Herdr calls: {calls}");
    };
    assert_eq!(started.flow_id, FRESH_CODEX_FLOW);
    // On the connection that opened the thread: the thread started, then
    // the first turn, then the answer to its call, during Herdr's start;
    // the thread was recorded as the launch's before the first turn.
    let methods = setup
        .world
        .state
        .join("record-2")
        .client_messages()
        .iter()
        .map(|message| match message.get("method") {
            Some(method) => method.as_str().unwrap_or_default().to_owned(),
            None => format!("answer {}", message["id"]),
        })
        .collect::<Vec<_>>();
    assert_eq!(
        methods,
        [
            "initialize",
            "initialized",
            "thread/start",
            "turn/start",
            &format!("answer {FIRST_CALL_REQUEST}")
        ],
        "{methods:?}"
    );
    {
        use crate::store::opened_thread::RecordsOpenedThreads;
        assert_eq!(
            fixture
                .nexus
                .store
                .opened_thread(&setup.sender.launch_request_id)
                .unwrap(),
            Some(FRESH_THREAD.to_owned())
        );
    }
    let answer = answer.expect("the first turn's call is answered");
    assert_eq!(
        answer.pointer("/result/success"),
        Some(&serde_json::json!(true))
    );
    // Queued once, from the launching metaflow.
    let recorded = fixture
        .nexus
        .store
        .send_call(&first_call_key())
        .unwrap()
        .expect("the call is recorded");
    assert_eq!(
        recorded.send_outcome,
        crate::generated::send_tool::SendOutcome::Queued(
            crate::generated::send_tool::Queued_Data {
                sender: setup.sender.address.clone(),
                target: setup.recipient.clone(),
            }
        )
    );
    // The recipient woke fresh, once: one Claude start, on a new session,
    // never a resume; the order was handed to it and its mark is spent.
    let woken = stored(&fixture, Layer::Secondary);
    let State::Awake(woken_flow) = &woken.state else {
        panic!("the recipient wakes: {woken:?}; Herdr calls: {calls}");
    };
    assert_ne!(woken_flow, "abc123");
    assert!(woken.queue.is_empty(), "{woken:?}");
    let claude_starts = calls
        .lines()
        .filter(|line| line.contains("agent start") && line.contains("--session-id"))
        .count();
    assert_eq!(claude_starts, 1, "{calls}");
    assert!(!calls.contains("--resume"), "{calls}");
    assert!(!fixture.nexus.store.fresh_wake_marked(&recipient).unwrap());
    let transcript = fs::read_to_string(
        fixture
            .directory
            .path()
            .join("native-transcripts/claude")
            .join(format!(
                "{}.jsonl",
                fs::read_to_string(setup.world.state.join("session")).unwrap()
            )),
    )
    .unwrap();
    assert!(transcript.contains(FIRST_CALL_BODY), "{transcript}");
}

/// The same first-turn call, but Herdr refuses the start once the turn has
/// ended: the launch is refused, the thread it opened is accounted for and
/// archived, and the call it answered stays recorded with its request
/// queued once from the launching metaflow.
#[test]
fn a_launch_refused_after_its_first_turn_called_send_archives_its_thread_and_keeps_the_call() {
    use super::send_tool::ReadsClientFrames;
    use crate::DispatchesSerially;
    use crate::store::send_call::KeepsSendCalls;
    let mut fixture = NexusFixture::new();
    let recipient = address(Layer::Primary);
    fixture
        .nexus
        .store
        .seed_metaflow(metaflow(Layer::Primary, State::Asleep, vec![], vec![]));
    let setup = fixture.first_turn_send(recipient.clone());
    fs::write(setup.world.state.join("start-refused"), "").unwrap();
    let turn = first_turn_ends_when_answered(setup.world.state.clone());

    let response = fixture.nexus.dispatch_serially(
        signal_flow::Query::Start(setup.sender.clone()),
        Origin::Unbound,
    );

    assert!(turn.join().unwrap().is_some(), "the call is answered");
    let signal_flow::Response::StartRejected(signal_flow::StartRejection::HarnessSpawnRefused(
        refusal,
    )) = &response
    else {
        panic!("the start is refused: {response:?}");
    };
    assert_eq!(
        refusal.opened_thread_option,
        Some(signal_flow::OpenedThread {
            native_session_id: FRESH_THREAD.into(),
            first_turn: signal_flow::FirstTurn::Answered,
            thread_archive: signal_flow::ThreadArchive::Archived,
        })
    );
    let account = setup.world.state.join("record-3").client_messages();
    assert!(
        account.iter().any(|message| {
            message.get("method").and_then(serde_json::Value::as_str) == Some("thread/archive")
        }),
        "{account:?}"
    );
    let recorded = fixture
        .nexus
        .store
        .send_call(&first_call_key())
        .unwrap()
        .expect("the call stays recorded");
    assert_eq!(
        recorded.send_outcome,
        crate::generated::send_tool::SendOutcome::Queued(
            crate::generated::send_tool::Queued_Data {
                sender: setup.sender.address.clone(),
                target: recipient.clone(),
            }
        )
    );
    assert_eq!(
        stored(&fixture, Layer::Primary).queue,
        vec![QueuedRequest {
            sent_by: SentBy::Sender(setup.sender.address.clone()),
            request: Request::Order(FIRST_CALL_BODY.into()),
        }]
    );
    // After the refusal the thread is no launch's: a call now is refused.
    assert_eq!(
        fixture.nexus.store.thread_sender(FRESH_THREAD).unwrap(),
        Err(crate::generated::send_tool::SendRefusal::ThreadUnowned)
    );
}

/// A Codex launch as a Nexus that exited mid-way left it: reserved, its
/// native intent recorded, its pane created and its thread recorded, its
/// first turn pending on a send call; nothing settled. Returns the
/// sender's profile and the state directory of the fakes.
fn interrupted_launch(fixture: &mut NexusFixture) -> FirstTurnSend {
    use crate::store::RecordsCreatedLaunchPane;
    use crate::store::opened_thread::RecordsOpenedThreads;
    let recipient = address(Layer::Secondary);
    fixture
        .nexus
        .store
        .seed_metaflow(metaflow(Layer::Secondary, State::Asleep, vec![], vec![]));
    let setup = fixture.first_turn_send(recipient);
    let composed = fixture
        .nexus
        .composer
        .compose(
            crate::composition::HarnessedProfile {
                harness_kind: HarnessKind::Codex,
                launch_profile: setup.sender.clone(),
            },
            Vec::<signal_flow::LeaveOut>::new(),
        )
        .unwrap();
    fixture
        .nexus
        .store
        .reserve_launch_attempt(&composed, Origin::Unbound)
        .unwrap();
    assert!(
        fixture
            .nexus
            .store
            .record_native_launch_intent(NativeLaunchIntent {
                launch_request_id: setup.sender.launch_request_id.clone(),
                prompt_sha256: composed.first_prompt_payload.prompt_sha256.clone(),
                harness_kind: HarnessKind::Codex,
                model_name: setup.sender.model_name.clone(),
                effort: setup.sender.effort.clone(),
                system_prompt_sha256: composed.system_prompt.system_prompt_sha256.clone(),
                prompt_sha256_vector: composed.part_hashes(),
            })
            .unwrap()
    );
    assert!(
        fixture
            .nexus
            .store
            .record_created_launch_pane(signal_flow::HerdrPaneBinding {
                launch_request_id: setup.sender.launch_request_id.clone(),
                herdr_session_name: "fixture-session".into(),
                herdr_agent_name: composed.launch_agent_name(),
                herdr_workspace_id: "w7".into(),
                herdr_pane_id: "w7:p1".into(),
                herdr_terminal_id: "term-fresh".into(),
            })
            .unwrap()
    );
    assert!(
        fixture
            .nexus
            .store
            .record_opened_thread(&setup.sender.launch_request_id, FRESH_THREAD)
            .unwrap()
    );
    // The app-server the restarted Nexus meets: the send-tool probe; the
    // connection that resumes the thread, to which the pending call is
    // replayed; and the account that archives the thread.
    let initialized = websocket_frame(r#"{"id":1,"result":{}}"#);
    let probe = [
        initialized.clone(),
        websocket_frame(
            r#"{"id":2,"error":{"code":-32600,"message":"thread/start.dynamicTools requires experimentalApi capability"}}"#,
        ),
    ]
    .concat();
    let resumed = [
        initialized.clone(),
        websocket_frame(r#"{"id":2,"result":{"thread":{}}}"#),
    ]
    .concat();
    let account = [
        initialized.clone(),
        websocket_frame(r#"{"id":2,"result":{"data":[{"status":"inProgress"}]}}"#),
        websocket_frame(r#"{"id":3,"result":{}}"#),
    ]
    .concat();
    let archive_refused = [
        initialized,
        websocket_frame(r#"{"id":2,"result":{"data":[{"status":"inProgress"}]}}"#),
        websocket_frame(r#"{"id":3,"error":{"code":-32603,"message":"archive refused"}}"#),
    ]
    .concat();
    let app_server = fixture.directory.path().join("restarted-codex");
    FixtureExecutable {
        path: app_server.clone(),
    }
    .install(&format!(
        "#!/bin/sh\nS='{state}'\nn=$(cat \"$S/restart-invocations\" 2>/dev/null || printf 0); n=$((n + 1)); printf '%s' \"$n\" > \"$S/restart-invocations\"\nwhile IFS= read -r line; do line=$(printf '%s' \"$line\" | tr -d '\\r'); [ -z \"$line\" ] && break; done\nexec 3<&0; cat <&3 > \"$S/restart-record-$n\" &\nprintf '%b' 'HTTP/1.1 101 Switching Protocols\\r\\nUpgrade: websocket\\r\\nConnection: Upgrade\\r\\nSec-WebSocket-Accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo=\\r\\n\\r\\n'\ncase \"$((n % 3))\" in\n  1) printf '%b' '{probe}' ;;\n  2) printf '%b' '{resumed}'; printf '%b' \"$(cat \"$S/first-call\")\"; exec sleep 30 ;;\n  0) if [ -e \"$S/archive-refused\" ]; then printf '%b' '{archive_refused}'; else printf '%b' '{account}'; fi ;;\nesac\nexec sleep 5\n",
        state = setup.world.state.display(),
    ));
    fixture.nexus.codex_endpoints.stable.client_path = app_server;
    setup
}

/// A Nexus restarted after a Codex launch had durably opened its thread
/// and its first turn's send call was pending: the launch is reconciled at
/// the start. The replayed call is refused and nothing is queued, the
/// thread is archived, and the launch is settled refused; no second launch
/// is made, a replayed Start answers the settlement, and a second start
/// finds nothing to reconcile.
#[test]
fn a_launch_interrupted_with_its_first_call_pending_is_refused_archived_and_settled_at_restart() {
    use super::send_tool::ReadsClientFrames;
    use crate::DispatchesSerially;
    use crate::launching::ReconcilesInterruptedLaunches;
    use crate::store::RecordsLaunchOutcome;
    use crate::store::send_call::KeepsSendCalls;
    let mut fixture = NexusFixture::new();
    let setup = interrupted_launch(&mut fixture);
    let state = setup.world.state.clone();

    fixture.nexus.reconcile_interrupted_launches();

    let replayed = state.join("restart-record-2").client_messages();
    let answer = replayed
        .iter()
        .find(|message| message.get("id") == Some(&serde_json::json!(FIRST_CALL_REQUEST)))
        .unwrap_or_else(|| panic!("the replayed call is answered: {replayed:?}"));
    assert_eq!(
        answer.pointer("/result/success"),
        Some(&serde_json::json!(false))
    );
    assert!(answer.to_string().contains("ThreadUnowned"), "{answer}");
    let archived = state.join("restart-record-3").client_messages();
    assert!(
        archived.iter().any(|message| {
            message.get("method").and_then(serde_json::Value::as_str) == Some("thread/archive")
        }),
        "{archived:?}"
    );
    assert!(stored(&fixture, Layer::Secondary).queue.is_empty());
    assert_eq!(
        fixture.nexus.store.send_call(&first_call_key()).unwrap(),
        None
    );
    let refused =
        signal_flow::StartRejection::NativeLaunchRefused(signal_flow::NativeLaunchRefusal {
            native_launch_stage: signal_flow::NativeLaunchStage::Spawn,
            native_launch_failure: signal_flow::NativeLaunchFailure::Unclassified,
        });
    assert_eq!(
        fixture
            .nexus
            .store
            .launch_outcome(&setup.sender.launch_request_id)
            .unwrap(),
        Some(crate::store::LaunchOutcome::StartRejected(refused.clone()))
    );
    assert_eq!(
        fixture.nexus.store.thread_sender(FRESH_THREAD).unwrap(),
        Err(crate::generated::send_tool::SendRefusal::ThreadUnowned)
    );
    // No second launch: a replayed Start answers the settlement, and a
    // second start reconciles nothing.
    assert_eq!(
        fixture.nexus.dispatch_serially(
            signal_flow::Query::Start(setup.sender.clone()),
            Origin::Unbound
        ),
        signal_flow::Response::StartRejected(refused)
    );
    fixture.nexus.reconcile_interrupted_launches();
    assert_eq!(
        fs::read_to_string(state.join("restart-invocations")).unwrap(),
        "3"
    );
    let calls = fs::read_to_string(state.join("calls")).unwrap_or_default();
    assert!(!calls.contains("agent start"), "{calls}");
    assert!(!calls.contains("workspace create"), "{calls}");
}

/// While a launch runs, its thread's calls are sent from its Address only
/// while no other flow holds that Address; and a launch settled is never
/// a sender.
#[test]
fn a_launch_whose_address_another_flow_holds_sends_nothing() {
    use crate::store::RecordsLaunchOutcome;
    use crate::store::send_call::KeepsSendCalls;
    let mut fixture = NexusFixture::new();
    let setup = interrupted_launch(&mut fixture);
    assert_eq!(
        fixture.nexus.store.thread_sender(FRESH_THREAD).unwrap(),
        Ok(setup.sender.address.clone()),
        "an unsettled launch whose Address is free sends from it"
    );
    let owner = fixture.node();
    fixture.nexus.store.register_flow(owner.clone()).unwrap();
    fixture.nexus.store.seed_metaflow(Metaflow {
        address: setup.sender.address.clone(),
        state: State::Awake(owner.flow_id.clone()),
        past: vec![],
        queue: vec![],
    });
    assert_eq!(
        fixture.nexus.store.thread_sender(FRESH_THREAD).unwrap(),
        Err(crate::generated::send_tool::SendRefusal::ThreadUnowned),
        "another live flow holds the Address"
    );
    let mut settled = NexusFixture::new();
    let setup = interrupted_launch(&mut settled);
    settled
        .nexus
        .store
        .record_launch_outcome(
            &setup.sender.launch_request_id,
            crate::store::LaunchOutcome::StartRejected(
                signal_flow::StartRejection::LaunchPersistenceRefused,
            ),
        )
        .unwrap();
    assert_eq!(
        settled.nexus.store.thread_sender(FRESH_THREAD).unwrap(),
        Err(crate::generated::send_tool::SendRefusal::ThreadUnowned),
        "a settled launch is no sender"
    );
}

/// A Herdr for the pane an interrupted launch created, w7:p1 on terminal
/// term-fresh of `fixture-session`: its snapshot shows the agents written to
/// `agents` beside that pane, its process information shows the pid written
/// to `pid` in the pane's foreground, and every call is logged to `calls`.
fn launch_pane_herdr(fixture: &NexusFixture) -> std::path::PathBuf {
    let directory = fixture.directory.path().join("launch-pane-herdr");
    fs::create_dir_all(&directory).unwrap();
    fs::write(directory.join("agents"), "[]").unwrap();
    fs::write(directory.join("pid"), "1").unwrap();
    let script = r#"#!/bin/sh
LISTING
D='DIRECTORY'
printf '%s\n' "$*" >> "$D/calls"
case "$3 $4" in
  "api snapshot") printf '{"id":"cli:api:snapshot","result":{"snapshot":{"agents":%s,"panes":[{"pane_id":"w7:p1","terminal_id":"term-fresh"}],"protocol":20,"version":"0.8.2"},"type":"session_snapshot"}}\n' "$(cat "$D/agents")" ;;
  "pane process-info") printf '{"result":{"process_info":{"pane_id":"%s","foreground_processes":[{"pid":%s}]}}}\n' "$6" "$(cat "$D/pid")" ;;
  "pane close") printf '%s\n' '{"result":{}}' ;;
  *) exit 64 ;;
esac
"#
    .replace("LISTING", &fixture.session_listing())
    .replace("DIRECTORY", &directory.display().to_string());
    FixtureExecutable {
        path: fixture.snapshot_program.clone(),
    }
    .install(&script);
    directory
}

/// The Codex agent Herdr shows at the launch's pane, reporting `session`
/// when one is given.
fn launch_pane_agent(session: Option<&str>) -> serde_json::Value {
    let mut agent = serde_json::json!({
        "agent":"codex","agent_status":"idle","name":"fixture-agent",
        "pane_id":"w7:p1","terminal_id":"term-fresh","workspace_id":"w7"
    });
    if let Some(session) = session {
        agent["agent_session"] = serde_json::json!({
            "source":"herdr:codex","agent":"codex","kind":"id","value":session
        });
    }
    agent
}

/// A process whose command line resumes `thread`, as a Codex TUI attached
/// to it runs; killed when dropped.
struct ResumingProcess(std::process::Child);

impl Drop for ResumingProcess {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

trait SpawnsResumingProcess {
    fn resuming(thread: &str) -> ResumingProcess;
}

impl SpawnsResumingProcess for ResumingProcess {
    fn resuming(thread: &str) -> ResumingProcess {
        ResumingProcess(
            std::process::Command::new("/bin/sh")
                .args(["-c", "sleep 60; :", "resume", thread])
                .stdin(std::process::Stdio::null())
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .spawn()
                .expect("resuming process"),
        )
    }
}

/// Teardown closes only a pane proven the launch's own. An interrupted
/// launch's pane, recorded for recovery, now runs a replacement session:
/// Herdr reports another session there, then reports none while the pane's
/// foreground process resumes another thread. Neither restart closes
/// anything; the launch is settled, its pane stays recorded and its pane
/// step Pending. Once the pane's foreground process resumes the launch's
/// own thread, the next start closes it and the cleanup completes.
#[test]
fn an_interrupted_launch_pane_running_a_replacement_session_is_never_closed() {
    use crate::launching::ReconcilesInterruptedLaunches;
    use crate::store::launch_cleanup::{
        CleanupStep, CompletesCleanup, KeepsLaunchCleanups, LaunchCleanup,
    };
    use crate::store::{ReadsCreatedLaunchPane, RecordsCreatedLaunchPane, RecordsLaunchOutcome};
    let mut fixture = NexusFixture::new();
    let setup = interrupted_launch(&mut fixture);
    let launch = setup.sender.launch_request_id.clone();
    fixture.nexus.store.request_harness_start(&launch).unwrap();
    let herdr = launch_pane_herdr(&fixture);
    let cleanup = |fixture: &NexusFixture| -> LaunchCleanup {
        fixture
            .nexus
            .store
            .launch_cleanup(&launch)
            .unwrap()
            .expect("the cleanup is recorded")
    };

    // A replacement session reported at the recorded pane and terminal.
    fs::write(
        herdr.join("agents"),
        serde_json::json!([launch_pane_agent(Some("replacement-thread"))]).to_string(),
    )
    .unwrap();
    fixture.nexus.reconcile_interrupted_launches();
    let calls = fs::read_to_string(herdr.join("calls")).unwrap_or_default();
    assert!(!calls.contains("pane close"), "{calls}");
    assert!(
        fixture
            .nexus
            .store
            .launch_outcome(&launch)
            .unwrap()
            .is_some(),
        "the launch is settled whatever its cleanup came to"
    );
    assert_eq!(cleanup(&fixture).thread_step, CleanupStep::Done);
    assert_eq!(cleanup(&fixture).pane_step, CleanupStep::Pending);
    assert!(
        fixture
            .nexus
            .store
            .recoverable_launch_pane(&launch)
            .unwrap()
            .is_some()
    );

    // A replacement reporting no session, its foreground process resuming
    // another thread.
    let other = ResumingProcess::resuming("another-thread");
    fs::write(
        herdr.join("agents"),
        serde_json::json!([launch_pane_agent(None)]).to_string(),
    )
    .unwrap();
    fs::write(herdr.join("pid"), other.0.id().to_string()).unwrap();
    fixture.nexus.reconcile_interrupted_launches();
    let calls = fs::read_to_string(herdr.join("calls")).unwrap_or_default();
    assert!(calls.contains("pane process-info"), "{calls}");
    assert!(!calls.contains("pane close"), "{calls}");
    assert_eq!(cleanup(&fixture).pane_step, CleanupStep::Pending);
    drop(other);

    // The pane's foreground process resumes the launch's own thread.
    let own = ResumingProcess::resuming(FRESH_THREAD);
    fs::write(herdr.join("pid"), own.0.id().to_string()).unwrap();
    fixture.nexus.reconcile_interrupted_launches();
    let calls = fs::read_to_string(herdr.join("calls")).unwrap_or_default();
    assert!(calls.contains("pane close w7:p1"), "{calls}");
    assert!(cleanup(&fixture).is_complete());
    assert_eq!(
        fixture
            .nexus
            .store
            .recoverable_launch_pane(&launch)
            .unwrap(),
        None
    );
    // The app-server was met once: the thread step was not taken again.
    assert_eq!(
        fs::read_to_string(setup.world.state.join("restart-invocations")).unwrap(),
        "3"
    );
}

/// The launch's settlement and its cleanup are apart. A restart whose
/// archive of the thread is refused, and whose Herdr cannot be read,
/// settles the launch but leaves both cleanup steps Pending; the next
/// start resumes them (the replayed call refused again, nothing queued),
/// the settlement unchanged, and only then is the cleanup complete.
#[test]
fn an_interrupted_launch_whose_cleanup_fails_is_settled_and_its_cleanup_resumed() {
    use super::send_tool::ReadsClientFrames;
    use crate::launching::ReconcilesInterruptedLaunches;
    use crate::store::launch_cleanup::{CleanupStep, CompletesCleanup, KeepsLaunchCleanups};
    use crate::store::{ReadsCreatedLaunchPane, RecordsCreatedLaunchPane, RecordsLaunchOutcome};
    let mut fixture = NexusFixture::new();
    let setup = interrupted_launch(&mut fixture);
    let state = setup.world.state.clone();
    let launch = setup.sender.launch_request_id.clone();
    fixture.nexus.store.request_harness_start(&launch).unwrap();
    fs::write(state.join("archive-refused"), "").unwrap();
    FixtureExecutable {
        path: fixture.snapshot_program.clone(),
    }
    .install("#!/bin/sh\nexit 64\n");

    fixture.nexus.reconcile_interrupted_launches();

    let settled = fixture.nexus.store.launch_outcome(&launch).unwrap();
    assert!(settled.is_some(), "the launch is settled");
    let cleanup = fixture
        .nexus
        .store
        .launch_cleanup(&launch)
        .unwrap()
        .unwrap();
    assert_eq!(cleanup.thread_step, CleanupStep::Pending, "archive refused");
    assert_eq!(cleanup.pane_step, CleanupStep::Pending, "Herdr unreadable");
    assert!(!cleanup.is_complete());
    assert!(
        fixture
            .nexus
            .store
            .recoverable_launch_pane(&launch)
            .unwrap()
            .is_some()
    );

    // The next start: the archive succeeds, and Herdr's snapshot shows
    // neither the pane nor its binding.
    fs::remove_file(state.join("archive-refused")).unwrap();
    FixtureExecutable {
        path: fixture.snapshot_program.clone(),
    }
    .install(&format!(
        "#!/bin/sh\n{}\ncase \"$3 $4\" in\n  \"api snapshot\") printf '%s\\n' '{{\"id\":\"cli:api:snapshot\",\"result\":{{\"snapshot\":{{\"agents\":[],\"panes\":[],\"protocol\":20,\"version\":\"0.8.2\"}},\"type\":\"session_snapshot\"}}}}' ;;\n  *) exit 64 ;;\nesac\n",
        fixture.session_listing()
    ));
    fixture.nexus.reconcile_interrupted_launches();

    assert_eq!(
        fixture.nexus.store.launch_outcome(&launch).unwrap(),
        settled
    );
    let replayed = state.join("restart-record-5").client_messages();
    assert!(
        replayed
            .iter()
            .any(|message| message.to_string().contains("ThreadUnowned")),
        "{replayed:?}"
    );
    assert!(stored(&fixture, Layer::Secondary).queue.is_empty());
    let archived = state.join("restart-record-6").client_messages();
    assert!(
        archived.iter().any(|message| {
            message.get("method").and_then(serde_json::Value::as_str) == Some("thread/archive")
        }),
        "{archived:?}"
    );
    let cleanup = fixture
        .nexus
        .store
        .launch_cleanup(&launch)
        .unwrap()
        .unwrap();
    assert!(cleanup.is_complete(), "{cleanup:?}");
    assert_eq!(
        fixture
            .nexus
            .store
            .recoverable_launch_pane(&launch)
            .unwrap(),
        None
    );
    // Complete and settled: a further start does nothing.
    fixture.nexus.reconcile_interrupted_launches();
    assert_eq!(
        fs::read_to_string(state.join("restart-invocations")).unwrap(),
        "6"
    );
}

/// A send call the earlier Nexus committed (its request queued and the call
/// recorded in one commit) before it could answer, then the Nexus exited:
/// the restarted Nexus answers the replayed call from its record, Queued,
/// and queues nothing more. The one request stays queued from the
/// launch's Address, and the target is due its wake, which the start's
/// `recover_wakes` (or the next request queued for it) takes.
#[test]
fn a_send_committed_before_its_reply_keeps_its_one_request_and_wake_across_a_restart() {
    use super::send_tool::ReadsClientFrames;
    use crate::generated::send_tool::{HandlesSendCall, ReadsToolCall, SendOutcome, ToolCall};
    use crate::launching::ReconcilesInterruptedLaunches;
    use crate::store::send_call::KeepsSendCalls;
    let mut fixture = NexusFixture::new();
    let setup = interrupted_launch(&mut fixture);
    let state = setup.world.state.clone();
    let recipient = setup.recipient.clone();
    let params = serde_json::json!({
        "threadId": FRESH_THREAD,
        "turnId": "turn-fresh",
        "callId": "call-first",
        "tool": "send",
        "arguments": {
            "target": {
                "aspect": "Mind",
                "topic": recipient.topic,
                "layer": format!("{:?}", recipient.layer)
            },
            "kind": "Order",
            "body": FIRST_CALL_BODY
        }
    });
    let call = ToolCall::read_tool_call(params).expect("the call reads");
    // The earlier Nexus commits the call; its answer never leaves.
    let committed = fixture.nexus.handle_send_call(call);
    let SendOutcome::Queued(queued) = &committed else {
        panic!("the call is committed Queued: {committed:?}")
    };
    assert_eq!(queued.sender, setup.sender.address);
    assert_eq!(stored(&fixture, Layer::Secondary).queue.len(), 1);

    // The Nexus exits: the store as it lies on disk opens in a new one.
    let app_server = fixture.nexus.codex_endpoints.stable.client_path.clone();
    let written = fixture.directory.path().join("flow.sema");
    let mut restarted = NexusFixture::with_store(|store| {
        fs::copy(&written, store).expect("the store as written");
    });
    restarted.nexus.codex_endpoints.stable.client_path = app_server;

    restarted.nexus.reconcile_interrupted_launches();

    let replayed = state.join("restart-record-2").client_messages();
    let answer = replayed
        .iter()
        .find(|message| message.get("id") == Some(&serde_json::json!(FIRST_CALL_REQUEST)))
        .unwrap_or_else(|| panic!("the replayed call is answered: {replayed:?}"));
    assert_eq!(
        answer.pointer("/result/success"),
        Some(&serde_json::json!(true))
    );
    assert!(answer.to_string().contains("Queued"), "{answer}");
    assert_eq!(
        restarted
            .nexus
            .store
            .send_call(&first_call_key())
            .unwrap()
            .map(|call| call.send_outcome),
        Some(committed.clone())
    );
    let target = stored(&restarted, Layer::Secondary);
    assert_eq!(target.queue.len(), 1, "exactly one enqueue");
    assert_eq!(
        target.queue[0].sent_by,
        SentBy::Sender(setup.sender.address.clone()),
        "its sender is kept"
    );
    assert_eq!(
        target.queue[0].request,
        Request::Order(FIRST_CALL_BODY.into())
    );
    assert!(
        restarted.nexus.wake_is_due(&target),
        "the target is due the wake the start's recover_wakes takes"
    );
}
