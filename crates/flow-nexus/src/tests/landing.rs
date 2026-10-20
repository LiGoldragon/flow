//! The approval pipeline against a real git repository, the fixture
//! context composer and the fixture Herdr: a commit on main that the
//! composer does not serve is recorded as a landing; its deploy is run and
//! witnessed only through the composer; a live flow loading a changed
//! module is marked due a refresh and ended at its rest, its queued work
//! waking a fresh flow; the change is queued as an Order to its
//! implementer; and Psyche core Secondary is told the outcome.

use super::waking::{CLAUDE_SEAT, claude_agent, claude_seat, metaflow};
use super::{ControlsHerdrSnapshot, NexusFixture, PromptFixture};
use crate::composition::{ComposesLaunch, HarnessedProfile, LaunchComposer, OpensLaunchComposer};
use crate::context::{ContextComposer, FixtureComposer};
use crate::fixture_executable::{FixtureExecutable, InstallsScript};
use crate::generated::landing::{KeepsLandings, Landing, Retrained, RunsLandings, Step};
use crate::store::metaflow::{SeedsMetaflow, SleepsMetaflows};
use crate::store::{
    ReadsFlowRows, RecordsNativeLaunchBinding, RecordsNativeLaunchIntent, ReservesLaunchAttempt,
};
use crate::waking::{AtStop, KeptAtStop, SleepsAndWakesSeats};
use flow_ethos::{LandingWatch, State};
use signal_flow::{
    Address, Aspect, FlowLifecycle, HarnessKind, HerdrPaneBinding, LaunchProfile, Layer,
    NativeLaunchBinding, NativeLaunchIntent, Origin, Request, SentBy,
};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{Arc, Mutex};

fn git(repository: &Path, arguments: &[&str]) -> String {
    let output = Command::new("git")
        .arg("-C")
        .arg(repository)
        .args([
            "-c",
            "user.name=Fixture",
            "-c",
            "user.email=fixture@example.invalid",
            "-c",
            "commit.gpgsign=false",
        ])
        .args(arguments)
        .output()
        .expect("git runs");
    assert!(
        output.status.success(),
        "git {arguments:?}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8_lossy(&output.stdout).trim().to_owned()
}

fn write(repository: &Path, path: &str, text: &str) {
    let file = repository.join(path);
    std::fs::create_dir_all(file.parent().unwrap()).unwrap();
    std::fs::write(file, text).unwrap();
}

/// A context repository whose first commit holds three modules and whose
/// second, the landing, changes `intent/flow.md`. Answers both commits.
fn context_repository(root: &Path) -> (PathBuf, String, String) {
    let repository = root.join("psyche-context");
    std::fs::create_dir_all(&repository).unwrap();
    git(&repository, &["init", "-q", "-b", "main"]);
    write(&repository, "spirit/core.md", "Spirit body.\n");
    write(&repository, "intent/flow.md", "Intent body.\n");
    write(&repository, "knowledge/flow.md", "Knowledge body.\n");
    write(&repository, "README.md", "The context repository.\n");
    git(&repository, &["add", "-A"]);
    git(&repository, &["commit", "-q", "-m", "First modules"]);
    let served = git(&repository, &["rev-parse", "HEAD"]);
    write(
        &repository,
        "intent/flow.md",
        "Intent body.\nA flow is retrained when its modules change.\n",
    );
    git(
        &repository,
        &["commit", "-q", "-am", "Land the retraining vision"],
    );
    let landed = git(&repository, &["rev-parse", "HEAD"]);
    (repository, served, landed)
}

/// The fixture composer, serving at `revision`, which the test moves as a
/// deploy would.
fn serve_at(fixture: &mut NexusFixture, revision: &str) -> Arc<Mutex<String>> {
    let mut composer = FixtureComposer::from(
        &[
            ("spirit-core", "Spirit body."),
            ("intent-flow", "Intent body."),
            ("knowledge-flow", "Knowledge body."),
            ("operation-main-flow", "Main flow body."),
        ][..],
    );
    let served = Arc::new(Mutex::new(revision.to_owned()));
    composer.revision = Arc::clone(&served);
    fixture.nexus.composer = LaunchComposer::with_context(ContextComposer::Fixture(composer));
    served
}

/// A deploy executable that records the datom it was given and answers.
fn deploy_script(fixture: &NexusFixture) -> (PathBuf, PathBuf) {
    let script = fixture.directory.path().join("context-deploy");
    let record = fixture.directory.path().join("deploy-requests");
    FixtureExecutable {
        path: script.clone(),
    }
    .install(&format!(
        "#!/bin/sh\nprintf '%s\\n' \"$1\" >> '{}'\necho Deployed\n",
        record.display()
    ));
    (script, record)
}

fn psyche_core() -> Address {
    Address {
        aspect: Aspect::Psyche,
        topic: "core".into(),
        layer: Layer::Secondary,
    }
}

fn mind(topic: &str) -> Address {
    Address {
        aspect: Aspect::Mind,
        topic: topic.into(),
        layer: Layer::Secondary,
    }
}

/// The seat's metaflow: one no implementation is routed to, so a refresh
/// of the seat wakes nothing.
fn seat_address() -> Address {
    Address {
        aspect: Aspect::Psyche,
        topic: "flow".into(),
        layer: Layer::Secondary,
    }
}

/// The Claude seat `c1a0de`, launched with these system and user modules
/// and Awake at Psyche flow Secondary, its pane showing it resting.
fn launched_seat(fixture: &NexusFixture, system_modules: Vec<String>, user_modules: Vec<String>) {
    fixture.accept_pane_operations(
        vec![claude_agent(fixture)],
        PromptFixture::Prompted("w1:p3"),
    );
    claude_seat(fixture, 40_000);
    let profile = LaunchProfile {
        launch_request_id: "seat-launch".into(),
        system_modules,
        user_modules,
        address: seat_address(),
        model_name: "claude-fixture".into(),
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
            HarnessedProfile {
                harness_kind: HarnessKind::Claude,
                launch_profile: profile.clone(),
            },
            Vec::<signal_flow::LeaveOut>::new(),
        )
        .expect("the seat's launch composes");
    fixture
        .nexus
        .store
        .reserve_launch_attempt(&composed, Origin::Unbound)
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
    assert!(
        fixture
            .nexus
            .store
            .record_native_launch_binding(NativeLaunchBinding {
                launch_request_id: profile.launch_request_id.clone(),
                flow_id: CLAUDE_SEAT.into(),
                native_session_id: super::waking::CLAUDE_SESSION.into(),
                harness_kind: HarnessKind::Claude,
                herdr_pane_binding: HerdrPaneBinding {
                    launch_request_id: profile.launch_request_id.clone(),
                    herdr_session_name: "fixture-session".into(),
                    herdr_agent_name: "fixture-agent".into(),
                    herdr_workspace_id: "w1".into(),
                    herdr_pane_id: "w1:p3".into(),
                    herdr_terminal_id: "term-1".into(),
                },
            })
            .unwrap()
    );
    let mut seated = metaflow(
        Layer::Secondary,
        State::Awake(CLAUDE_SEAT.into()),
        vec![],
        vec![],
    );
    seated.address = seat_address();
    fixture.nexus.store.seed_metaflow(seated);
}

fn seed_asleep(fixture: &NexusFixture, address: Address) {
    fixture.nexus.store.seed_metaflow(signal_flow::Metaflow {
        address,
        state: State::Asleep,
        past: vec![],
        queue: vec![],
    });
}

fn closed_pane(fixture: &NexusFixture) -> bool {
    std::fs::read_to_string(fixture.directory.path().join("herdr-operations.log"))
        .unwrap_or_default()
        .contains("pane close w1:p3")
}

fn the_landing(fixture: &NexusFixture, commit: &str) -> Landing {
    fixture
        .nexus
        .store
        .landing_row(commit)
        .unwrap()
        .expect("the landing is recorded")
}

/// A commit the composer does not serve is a landing. A deploy that leaves
/// the composer serving the old revision is a failed step, reported to
/// Psyche core Secondary as an Order; nothing is retrained or implemented
/// meanwhile. Once the composer serves the landing, the next pass witnesses
/// the deploy, marks the seat that loads the changed module, queues the
/// change to its implementer with its lines, and reports the Result.
#[test]
fn a_landing_is_deployed_retrained_implemented_and_reported() {
    let mut fixture = NexusFixture::new();
    let (repository, served, landed) = context_repository(fixture.directory.path());
    let revision = serve_at(&mut fixture, &served);
    let (script, requests) = deploy_script(&fixture);
    let watch = LandingWatch {
        context_repository: repository.to_string_lossy().into_owned(),
        deploy_binary: script.to_string_lossy().into_owned(),
    };
    launched_seat(
        &fixture,
        vec!["spirit-core".into(), "intent-flow".into()],
        vec![],
    );
    seed_asleep(&fixture, psyche_core());
    seed_asleep(&fixture, mind("core"));

    let recorded = fixture.nexus.record_landings(&watch).expect("recorded");
    assert_eq!(recorded.len(), 1);
    assert_eq!(recorded[0].commit, landed);
    assert_eq!(recorded[0].base, served);
    assert_eq!(recorded[0].changed_paths, vec!["intent/flow.md".to_owned()]);
    assert!(
        fixture.nexus.record_landings(&watch).unwrap().is_empty(),
        "a landing is recorded once"
    );

    // The deploy runs but the composer still serves the old revision.
    fixture.nexus.advance_landings(&watch).expect("advanced");
    assert_eq!(
        std::fs::read_to_string(&requests).unwrap(),
        format!("Deploy.{{ {} {landed} }}\n", repository.display())
    );
    let blocked = the_landing(&fixture, &landed);
    assert!(
        matches!(blocked.deploy, Step::Failed(_)),
        "{:?}",
        blocked.deploy
    );
    assert_eq!(blocked.retrain, Step::Pending);
    assert_eq!(blocked.implement, Step::Pending);
    assert!(!blocked.reported);
    let core = fixture
        .nexus
        .store
        .metaflow(&psyche_core())
        .unwrap()
        .unwrap();
    assert_eq!(core.queue.len(), 1);
    assert_eq!(core.queue[0].sent_by, SentBy::Sender(psyche_core()));
    let Request::Order(failure) = &core.queue[0].request else {
        panic!("a failed step is an Order: {:?}", core.queue[0].request)
    };
    assert!(failure.contains("Deploy failed"), "{failure}");
    assert!(
        fixture
            .nexus
            .store
            .refresh_due(CLAUDE_SEAT)
            .unwrap()
            .is_none(),
        "nothing is marked before the deploy is witnessed"
    );

    // The deploy takes effect: the composer serves the landing.
    *revision.lock().unwrap() = landed.clone();
    fixture.nexus.advance_landings(&watch).expect("advanced");
    let landing = the_landing(&fixture, &landed);
    assert!(
        matches!(landing.deploy, Step::Witnessed(_)),
        "{:?}",
        landing.deploy
    );
    assert!(
        matches!(landing.retrain, Step::Witnessed(_)),
        "{:?}",
        landing.retrain
    );
    assert!(
        matches!(landing.implement, Step::Witnessed(_)),
        "{:?}",
        landing.implement
    );
    assert!(landing.reported);
    assert_eq!(landing.owners, vec![mind("core")]);
    assert_eq!(landing.retrainings.len(), 1);
    assert_eq!(landing.retrainings[0].flow_id, CLAUDE_SEAT);
    assert_eq!(
        std::fs::read_to_string(&requests).unwrap().lines().count(),
        1,
        "a served landing is not deployed again"
    );

    // The implementer holds the change with its lines.
    let implementer = fixture
        .nexus
        .store
        .metaflow(&mind("core"))
        .unwrap()
        .unwrap();
    let Request::Order(order) = &implementer.queue[0].request else {
        panic!("the implementation is an Order")
    };
    assert!(order.contains(&landed), "{order}");
    assert!(order.contains("intent/flow.md"), "{order}");
    assert!(
        order.contains("+A flow is retrained when its modules change."),
        "{order}"
    );

    // Psyche core Secondary holds the Result after the failure Order.
    let core = fixture
        .nexus
        .store
        .metaflow(&psyche_core())
        .unwrap()
        .unwrap();
    let Request::Result(result) = &core.queue.last().unwrap().request else {
        panic!("a finished landing is a Result")
    };
    assert!(result.contains(&landed), "{result}");

    // The seat rested when it was marked, so it was refreshed at once.
    assert!(closed_pane(&fixture), "the refreshed seat's pane is closed");
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
    let seat = fixture
        .nexus
        .store
        .metaflow(&seat_address())
        .unwrap()
        .unwrap();
    assert_eq!(seat.state, State::Asleep);
    assert_eq!(seat.past, vec![CLAUDE_SEAT.to_owned()]);
    assert!(
        fixture
            .nexus
            .store
            .refresh_due(CLAUDE_SEAT)
            .unwrap()
            .is_none()
    );
    assert_eq!(
        the_landing(&fixture, &landed).retrainings[0].retrained,
        Retrained::Ended
    );
}

/// A seat that loads no changed module is left as it is.
#[test]
fn a_flow_loading_no_changed_module_is_not_marked() {
    let mut fixture = NexusFixture::new();
    let (repository, served, landed) = context_repository(fixture.directory.path());
    let revision = serve_at(&mut fixture, &served);
    let (script, _) = deploy_script(&fixture);
    let watch = LandingWatch {
        context_repository: repository.to_string_lossy().into_owned(),
        deploy_binary: script.to_string_lossy().into_owned(),
    };
    launched_seat(
        &fixture,
        vec!["spirit-core".into()],
        vec!["knowledge-flow".into()],
    );
    seed_asleep(&fixture, psyche_core());
    seed_asleep(&fixture, mind("core"));
    fixture.nexus.record_landings(&watch).unwrap();
    *revision.lock().unwrap() = landed.clone();
    fixture.nexus.advance_landings(&watch).unwrap();
    let landing = the_landing(&fixture, &landed);
    assert!(landing.retrainings.is_empty(), "{:?}", landing.retrain);
    assert!(
        fixture
            .nexus
            .store
            .refresh_due(CLAUDE_SEAT)
            .unwrap()
            .is_none()
    );
    assert!(!closed_pane(&fixture));
}

/// A seat due a refresh that Herdr shows working is kept, still due; at a
/// later rest it is ended, and work queued for its metaflow is not typed
/// into it.
#[test]
fn a_working_seat_due_a_refresh_is_ended_only_at_its_rest() {
    let mut fixture = NexusFixture::new();
    let (repository, served, landed) = context_repository(fixture.directory.path());
    let revision = serve_at(&mut fixture, &served);
    let (script, _) = deploy_script(&fixture);
    let watch = LandingWatch {
        context_repository: repository.to_string_lossy().into_owned(),
        deploy_binary: script.to_string_lossy().into_owned(),
    };
    launched_seat(&fixture, vec!["intent-flow".into()], vec![]);
    let mut working = claude_agent(&fixture);
    working["agent_status"] = serde_json::Value::String("working".into());
    fixture.accept_pane_operations(vec![working], PromptFixture::Prompted("w1:p3"));
    seed_asleep(&fixture, psyche_core());
    seed_asleep(&fixture, mind("core"));
    fixture.nexus.record_landings(&watch).unwrap();
    *revision.lock().unwrap() = landed.clone();
    fixture.nexus.advance_landings(&watch).unwrap();

    assert!(
        fixture
            .nexus
            .store
            .refresh_due(CLAUDE_SEAT)
            .unwrap()
            .is_some()
    );
    assert!(!closed_pane(&fixture), "a working seat is not closed");
    assert!(matches!(
        fixture.nexus.settle_stopped(CLAUDE_SEAT),
        AtStop::Kept(KeptAtStop::NotResting(_))
    ));
    assert!(
        fixture
            .nexus
            .store
            .refresh_due(CLAUDE_SEAT)
            .unwrap()
            .is_some()
    );

    // It rests.
    fixture.accept_pane_operations(
        vec![claude_agent(&fixture)],
        PromptFixture::Prompted("w1:p3"),
    );
    assert!(matches!(
        fixture.nexus.settle_stopped(CLAUDE_SEAT),
        AtStop::Refreshed(_)
    ));
    assert!(closed_pane(&fixture));
    assert!(
        fixture
            .nexus
            .store
            .refresh_due(CLAUDE_SEAT)
            .unwrap()
            .is_none()
    );
    assert_eq!(
        the_landing(&fixture, &landed).retrainings[0].retrained,
        Retrained::Ended
    );
}

/// A change whose implementer is not recorded is not dropped: the
/// implement step fails, nothing is queued, and Psyche core Secondary is
/// given the Order naming the missing metaflow.
#[test]
fn a_change_with_no_recorded_implementer_is_reported_blocked() {
    let mut fixture = NexusFixture::new();
    let (repository, served, landed) = context_repository(fixture.directory.path());
    let revision = serve_at(&mut fixture, &served);
    let (script, _) = deploy_script(&fixture);
    let watch = LandingWatch {
        context_repository: repository.to_string_lossy().into_owned(),
        deploy_binary: script.to_string_lossy().into_owned(),
    };
    seed_asleep(&fixture, psyche_core());
    fixture.nexus.record_landings(&watch).unwrap();
    *revision.lock().unwrap() = landed.clone();
    fixture.nexus.advance_landings(&watch).unwrap();
    let landing = the_landing(&fixture, &landed);
    let Step::Failed(reason) = &landing.implement else {
        panic!("an unowned change fails its step: {:?}", landing.implement)
    };
    assert!(reason.contains("Mind core Secondary"), "{reason}");
    assert!(!landing.reported);
    let core = fixture
        .nexus
        .store
        .metaflow(&psyche_core())
        .unwrap()
        .unwrap();
    let Request::Order(blocked) = &core.queue.last().unwrap().request else {
        panic!("a blocked landing is an Order")
    };
    assert!(blocked.contains("Implement failed"), "{blocked}");
}

/// One real landing, run where a build runs: the context repository is a
/// checkout of the real psyche-context whose main is ahead of the revision
/// a live Curriculum Nexus serves, and the deploy executable moves that
/// Curriculum to the commit it is given. The seat is the fixture Herdr's,
/// launched with real modules composed by that Curriculum. Every landing
/// row and every queue is printed as the witness.
///
/// `FLOW_LANDING_REPOSITORY` names the checkout, `FLOW_LANDING_DEPLOY` the
/// deploy executable, `FLOW_LANDING_CURRICULUM` the Curriculum socket and
/// `FLOW_LANDING_SYSTEM_MODULES` and `FLOW_LANDING_USER_MODULES` the seat's
/// modules, space-separated.
#[test]
#[ignore = "needs the FLOW_LANDING_ variables naming a staging repository, deploy, Curriculum and seat modules"]
fn a_real_landing_runs_against_a_live_curriculum() {
    use crate::context::{CurriculumComposer, OpensCurriculumComposer};
    let variable = |name: &str| std::env::var(name).unwrap_or_else(|_| panic!("{name} is set"));
    let mut fixture = NexusFixture::new();
    fixture.nexus.composer = LaunchComposer::with_context(ContextComposer::Curriculum(
        CurriculumComposer::at(variable("FLOW_LANDING_CURRICULUM").as_str()),
    ));
    let watch = LandingWatch {
        context_repository: variable("FLOW_LANDING_REPOSITORY"),
        deploy_binary: variable("FLOW_LANDING_DEPLOY"),
    };
    let modules = |name: &str| {
        variable(name)
            .split_whitespace()
            .map(str::to_owned)
            .collect::<Vec<_>>()
    };
    launched_seat(
        &fixture,
        modules("FLOW_LANDING_SYSTEM_MODULES"),
        modules("FLOW_LANDING_USER_MODULES"),
    );
    seed_asleep(&fixture, psyche_core());
    seed_asleep(&fixture, mind("core"));

    let recorded = fixture.nexus.record_landings(&watch).expect("recorded");
    assert!(!recorded.is_empty(), "main is ahead of what is served");
    let advanced = fixture.nexus.advance_landings(&watch).expect("advanced");
    for landing in &advanced {
        eprintln!("WITNESS landing {landing:#?}");
    }
    for address in [psyche_core(), mind("core"), seat_address()] {
        let metaflow = fixture.nexus.store.metaflow(&address).unwrap().unwrap();
        eprintln!(
            "WITNESS metaflow {:?} {} {:?}: {:?}, past {:?}",
            address.aspect, address.topic, address.layer, metaflow.state, metaflow.past
        );
        for queued in &metaflow.queue {
            eprintln!("WITNESS queued {queued:#?}");
        }
    }
    eprintln!(
        "WITNESS seat {CLAUDE_SEAT}: {:?}, pane closed {}",
        fixture
            .nexus
            .store
            .flow_node(CLAUDE_SEAT)
            .unwrap()
            .unwrap()
            .flow_lifecycle,
        closed_pane(&fixture)
    );
    for landing in &advanced {
        assert!(
            matches!(landing.deploy, Step::Witnessed(_)),
            "{:?}",
            landing.deploy
        );
        assert!(
            matches!(landing.implement, Step::Witnessed(_)),
            "{:?}",
            landing.implement
        );
        assert!(landing.reported);
    }
}
