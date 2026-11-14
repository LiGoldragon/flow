//! A wake that resumes, against the fixture Herdr taught `--resume`: the
//! Claude flow abc123 slept at `{ Mind flow Sol }` on model claude-fixture,
//! its last turn end and context size recorded as its hooks record them.
//! A model with a resumption configured, the flow warm and below its
//! ceiling, wakes as abc123 on its own session; anything else wakes fresh.

use super::NexusFixture;
use super::waking::{
    LaunchedClaude, ReadsFreshWorld, WakesFreshClaude, address, ended_claude, metaflow, stored,
};
use crate::generated::operation::{Operation, Outcome};
use crate::performing::Performs;
use crate::store::context::RecordsContextSize;
use crate::store::metaflow::SeedsMetaflow;
use crate::store::turn_end::RecordsTurnEnds;
use crate::store::{ManagesMetaflowState, ReadsFlowRows};
use crate::waking::{HoldsWakingMetaflows, SleepsAndWakesSeats, WakePlan};
use flow_ethos::State;
use meta_signal_flow::{CacheWarmth, ConfigurationChange, ConfigureResumption, ResumeCeiling};
use signal_flow::{FlowLifecycle, HarnessKind, HerdrRouteSelection, Model, Request, TurnEnd};
use std::fs;

const FLOW: &str = "abc123";
/// The context abc123's session carried into its last turn.
const CONTEXT: i64 = 40_000;

/// What abc123's hooks recorded when it slept: its last turn ended
/// `idle_seconds` ago, its session carrying CONTEXT tokens.
trait RecordsLastTurn {
    fn last_turn(&self, idle_seconds: i64);
}

impl RecordsLastTurn for NexusFixture {
    fn last_turn(&self, idle_seconds: i64) {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        self.nexus
            .store
            .record_turn_end(TurnEnd {
                address: address(Model::Sol),
                harness_kind: HarnessKind::Claude,
                flow_id: flow_ethos::FlowId(FLOW.into()),
                observed_at: signal_flow::ObservedAt(i64::try_from(now).unwrap() - idle_seconds),
            })
            .unwrap();
        self.nexus.store.record_context_size(FLOW, CONTEXT).unwrap();
    }
}

/// Configures claude-fixture's resumption.
fn resumes(fixture: &NexusFixture, cache_warmth_minutes: i64, resume_ceiling: i64) {
    fixture
        .nexus
        .store
        .configure_change(ConfigurationChange::Resumptions(vec![
            ConfigureResumption {
                model_name: flow_ethos::ModelName("claude-fixture".into()),
                cache_warmth: CacheWarmth(cache_warmth_minutes),
                resume_ceiling: ResumeCeiling(resume_ceiling),
            },
        ]))
        .expect("the resumptions are configured");
}

/// abc123 slept with its last turn and context recorded; the metaflow is
/// Asleep with `queue`, in the fresh fixture world.
fn slept(fixture: &mut NexusFixture, queue: Vec<Request>) -> LaunchedClaude {
    let seat = ended_claude(fixture);
    fixture
        .nexus
        .store
        .seed_metaflow(metaflow(Model::Sol, State::Asleep, vec![FLOW], queue));
    seat
}

fn asleep() -> signal_flow::Metaflow {
    metaflow(Model::Sol, State::Asleep, vec![FLOW], vec![])
}

/// A model with no resumption configured wakes fresh whatever its last
/// flow recorded: the wake as it was before resumptions.
#[test]
fn a_model_with_no_resumption_wakes_fresh() {
    let mut fixture = NexusFixture::new();
    slept(&mut fixture, vec![]);
    fixture.fresh_claude_world();
    fixture.last_turn(0);
    assert!(matches!(
        fixture.nexus.wake_plan(&asleep()),
        WakePlan::Fresh(_)
    ));
    // Another model's resumption is not claude-fixture's.
    fixture
        .nexus
        .store
        .configure_change(ConfigurationChange::Resumptions(vec![
            ConfigureResumption {
                model_name: flow_ethos::ModelName("claude-other".into()),
                cache_warmth: CacheWarmth(60),
                resume_ceiling: ResumeCeiling(1_000_000),
            },
        ]))
        .unwrap();
    assert!(matches!(
        fixture.nexus.wake_plan(&asleep()),
        WakePlan::Fresh(_)
    ));
}

/// Warm and below its ceiling, the last flow is the one the wake resumes:
/// its FlowId, its own native session, its harness and model, with the
/// fresh context its launch is composed from.
#[test]
fn a_warm_flow_below_its_ceiling_is_resumed() {
    let mut fixture = NexusFixture::new();
    let seat = slept(&mut fixture, vec![]);
    fixture.fresh_claude_world();
    fixture.last_turn(60);
    resumes(&fixture, 5, CONTEXT + 1);
    let WakePlan::Resume(resume) = fixture.nexus.wake_plan(&asleep()) else {
        panic!("a warm flow below its ceiling resumes")
    };
    assert_eq!(resume.flow_id.0, FLOW);
    assert_eq!(resume.native_session_id.0, seat.session);
    assert_eq!(resume.harness_kind, HarnessKind::Claude);
    assert_eq!(resume.model_name, "claude-fixture");
    assert_eq!(
        WakePlan::Fresh(resume.fresh.clone()),
        fixture.nexus.fresh_plan(&asleep())
    );
}

/// Each condition the resumption names is needed: a last turn ended
/// CacheWarmth minutes ago or longer, a context at the ceiling, no turn end
/// of this flow, or no context size, wakes fresh.
#[test]
fn a_cold_flow_or_one_at_its_ceiling_wakes_fresh() {
    let fresh =
        |fixture: &NexusFixture| matches!(fixture.nexus.wake_plan(&asleep()), WakePlan::Fresh(_));
    // Cold: the cache warms for five minutes; the turn ended ten ago.
    let mut cold = NexusFixture::new();
    slept(&mut cold, vec![]);
    cold.fresh_claude_world();
    cold.last_turn(600);
    resumes(&cold, 5, 1_000_000);
    assert!(fresh(&cold));
    // A warmth of zero never resumes.
    resumes(&cold, 0, 1_000_000);
    cold.last_turn(0);
    assert!(fresh(&cold));
    // Warm, but the context is at the ceiling.
    let mut full = NexusFixture::new();
    slept(&mut full, vec![]);
    full.fresh_claude_world();
    full.last_turn(0);
    resumes(&full, 60, CONTEXT);
    assert!(fresh(&full));
    // Warm and small, but the last turn end is another flow's.
    let mut other = NexusFixture::new();
    slept(&mut other, vec![]);
    other.fresh_claude_world();
    other.last_turn(0);
    other
        .nexus
        .store
        .record_turn_end(TurnEnd {
            address: address(Model::Sol),
            harness_kind: HarnessKind::Claude,
            flow_id: flow_ethos::FlowId("def456".into()),
            observed_at: signal_flow::ObservedAt(0),
        })
        .unwrap();
    resumes(&other, 60, 1_000_000);
    assert!(fresh(&other));
    // Warm, but no context size was ever recorded.
    let mut unmeasured = NexusFixture::new();
    slept(&mut unmeasured, vec![]);
    unmeasured.fresh_claude_world();
    unmeasured
        .nexus
        .store
        .record_turn_end(TurnEnd {
            address: address(Model::Sol),
            harness_kind: HarnessKind::Claude,
            flow_id: flow_ethos::FlowId(FLOW.into()),
            observed_at: signal_flow::ObservedAt(
                i64::try_from(
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_secs(),
                )
                .unwrap(),
            ),
        })
        .unwrap();
    resumes(&unmeasured, 60, 1_000_000);
    assert!(fresh(&unmeasured));
}

/// A queued Order wakes a warm abc123 as itself: `claude --resume
/// <session>` in a new pane and no fresh session; the flow Active again
/// under its own FlowId, its route the new pane, its Address Awake as it
/// and out of Past; the queue then typed into it, oldest first.
#[test]
fn a_warm_wake_resumes_the_same_flow_and_hands_it_its_queue() {
    let mut fixture = NexusFixture::new();
    let seat = slept(
        &mut fixture,
        vec![Request::Order("A".into()), Request::Question("B".into())],
    );
    let world = fixture.fresh_claude_world();
    fixture.last_turn(30);
    resumes(&fixture, 5, CONTEXT + 1);
    let at = address(Model::Sol);
    let lease = fixture.nexus.waking.hold(&at).expect("the wake holds it");

    let Outcome::Woken(flow_id) = fixture.nexus.perform(Operation::Wake(at.clone())) else {
        panic!("the wake resumes; Herdr calls: {}", world.calls())
    };
    assert_eq!(flow_id.0, FLOW, "the flow that slept, resumed");
    fixture.nexus.drain_woken(&at, lease);

    let calls = world.calls();
    let starts = calls
        .lines()
        .filter(|line| line.contains("agent start"))
        .collect::<Vec<_>>();
    assert_eq!(starts.len(), 1, "{calls}");
    assert!(
        starts[0].contains(&format!("--resume {}", seat.session)),
        "{calls}"
    );
    assert!(!starts[0].contains("--session-id"), "{calls}");
    assert!(
        starts[0].contains("--name { Mind flow Sol abc123 }"),
        "{calls}"
    );
    assert_eq!(world.claude_starts(), 0, "no fresh session: {calls}");
    assert_eq!(world.session(), seat.session);

    let node = fixture.nexus.store.flow_node(FLOW).unwrap().unwrap();
    assert_eq!(node.flow_lifecycle, FlowLifecycle::Active);
    assert_eq!(node.session_id.0, seat.session);
    let HerdrRouteSelection::Available(route) = node.herdr_route_selection else {
        panic!("the resumed flow is routed")
    };
    assert_eq!(route.herdr_pane_id.0, "w7:p1");
    let woken = stored(&fixture, Model::Sol);
    assert_eq!(woken.state, State::Awake(flow_ethos::FlowId(FLOW.into())));
    assert!(woken.past.0.is_empty(), "{woken:?}");
    assert!(
        woken.queue.0.is_empty(),
        "the queue was handed over: {woken:?}"
    );
    let turns = world.user_turns(&fixture);
    let typed = turns.join("\n");
    assert!(
        typed.contains(" A }") && typed.contains(" B }"),
        "the queue is typed into the resumed session: {turns:?}"
    );
    assert!(
        typed.find(" A }") < typed.find(" B }"),
        "oldest first: {turns:?}"
    );
}

/// A resume Herdr refuses discards nothing it cannot see and wakes fresh:
/// a new flow on a new session, abc123 left Stopped in Past.
#[test]
fn a_resume_that_does_not_start_wakes_fresh() {
    let mut fixture = NexusFixture::new();
    let seat = slept(&mut fixture, vec![Request::Order("A".into())]);
    let world = fixture.fresh_claude_world();
    fs::write(world.state.join("resume-refused"), "").unwrap();
    fixture.last_turn(30);
    resumes(&fixture, 5, CONTEXT + 1);
    let at = address(Model::Sol);
    let lease = fixture.nexus.waking.hold(&at).expect("the wake holds it");

    let Outcome::Woken(flow_id) = fixture.nexus.perform(Operation::Wake(at.clone())) else {
        panic!("the wake starts fresh; Herdr calls: {}", world.calls())
    };
    fixture.nexus.drain_woken(&at, lease);
    assert_ne!(flow_id.0, FLOW, "a new flow");
    assert_eq!(world.claude_starts(), 1, "{}", world.calls());
    assert_ne!(world.session(), seat.session);
    let old = fixture.nexus.store.flow_node(FLOW).unwrap().unwrap();
    assert_eq!(old.flow_lifecycle, FlowLifecycle::Stopped);
    let woken = stored(&fixture, Model::Sol);
    assert_eq!(woken.state, State::Awake(flow_id));
    assert_eq!(
        woken.past,
        flow_ethos::Past(vec![flow_ethos::FlowId(FLOW.into())])
    );
}
