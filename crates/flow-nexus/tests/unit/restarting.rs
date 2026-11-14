//! Restart, slice 1: a Claude flow, Awake and resting, restarted from
//! outside, against the fixture Herdr taught `--resume`. The flow abc123
//! rests Awake at `{ Mind flow Sol }` in pane w1:p3, its harness a live
//! process that pane shows running its session; a restart closes it and
//! resumes the same session in the next pane the fixture makes, w7:p1.
//! Every restart record, seat birth and queue here is Flow's own durable
//! Memory; a Nexus death is a reopen of that store.

use super::NexusFixture;
use super::waking::{
    FreshWorld, LaunchedClaude, ReadsFreshWorld, WakesFreshClaude, address,
    launched_claude_ended_by, maintain, metaflow, stored,
};
use crate::Dispatches;
use crate::fixture_executable::{FixtureExecutable, InstallsScript};
use crate::generated::memory::SeatBirth;
use crate::generated::operation::{Operation, Outcome, Record_Data};
use crate::live::HoldsLiveValue;
use crate::performing::Performs;
use crate::reporting::RecordsReports;
use crate::restarting::{
    ExitDoubt, ExitEvidence, ExitProof, FencesSeatEvents, ProcFs, ProvesExit, ReadsProcessStatus,
    SeatWitness,
};
use crate::store::metaflow::{MutatesMetaflows, SeedsMetaflow};
use crate::store::restart::RecordsRestarts;
use crate::store::successor::OwesSuccessors;
use crate::store::{
    FlowStore, ManagesMetaflowState, ReadsFlowMemory, ReadsFlowRows, RegistersFlowIdentity,
};
use crate::waking::SleepsAndWakesSeats;
use flow_ethos::State;
use meta_signal_flow::{OldSeat, RestartFailure, RestartPhase, RestartRecord};
use signal_flow::{
    Address, Aspect, Event, FlowLifecycle, HerdrPaneBinding, HerdrRoute, HerdrRouteSelection,
    Model, NativeStop, Process, Query, Report_Data, Response, RestartRefusal, ResumeRefused_Data,
    StopRejection,
};
use std::{fs, path::Path, time::Duration};

const FLOW: &str = "abc123";
const OLD_PANE: &str = "w1:p3";
const NEW_PANE: &str = "w7:p1";
/// The pane the fixture makes next, where a restart reopens by itself.
const REOPENED_PANE: &str = "w8:p1";

/// The old harness: a shell whose argv names the session as Claude's does,
/// marked as Herdr marks a pane's processes, with a hook child whose
/// environment is scrubbed, the hook's pid in the file `$0` names.
const HARNESS: &str =
    "s=$(command -v sleep); env -i \"$s\" 120 & printf '%s' \"$!\" > \"$0\"; sleep 120; :";

/// The world a restart runs in: the fresh fixture Herdr, the seat it
/// restarts, and the old harness, reaped when it is closed.
struct RestartWorld {
    fresh: FreshWorld,
    seat: LaunchedClaude,
    /// The agent name abc123's route records.
    old_agent: String,
}

trait SeatsRestartWorld {
    /// abc123 launched, Active, Awake at `{ Mind flow Sol }` and resting in
    /// w1:p3; `alive` keeps its harness running there.
    fn restart_world(&mut self, alive: bool) -> RestartWorld;
    /// The store closed and opened again at the same path: what a Nexus
    /// that died leaves its successor.
    fn reopen_store(&mut self);
}

impl SeatsRestartWorld for NexusFixture {
    fn restart_world(&mut self, alive: bool) -> RestartWorld {
        // The world's Herdr session is the one the Nexus declares.
        self.declare_launch_session("fixture-session");
        let seat = launched_claude_ended_by(self, |_| true);
        self.nexus.store.seed_metaflow(metaflow(
            Model::Sol,
            State::Awake(flow_ethos::FlowId(FLOW.into())),
            vec![],
            vec![],
        ));
        let fresh = self.fresh_claude_world();
        fs::write(fresh.state.join("restart-world"), "").unwrap();
        if alive {
            let hook = fresh.state.join("old-hook-pid");
            let mut harness = std::process::Command::new("sh")
                .args(["-c", HARNESS])
                .arg(&hook)
                .args(["claude", "--session-id", &seat.session])
                .env("HERDR_PANE_ID", OLD_PANE)
                .env("HERDR_SESSION", "fixture-session")
                .stdin(std::process::Stdio::null())
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .spawn()
                .expect("old harness");
            fs::write(fresh.state.join("old-pid"), harness.id().to_string()).unwrap();
            // Reaped as soon as its pane is closed.
            std::thread::spawn(move || harness.wait());
            fs::write(fresh.state.join("old-pane"), OLD_PANE).unwrap();
            fs::write(
                fresh.state.join("old-agent"),
                serde_json::json!({
                    "name": seat.profile.launch_request_id.0,
                    "agent": "claude",
                    "agent_status": "idle",
                    "workspace_id": "w1",
                    "tab_id": "w1:t1",
                    "pane_id": OLD_PANE,
                    "terminal_id": "term-old",
                    "interactive_ready": true,
                    "agent_session": {
                        "source": "herdr:claude", "agent": "claude",
                        "kind": "id", "value": seat.session,
                    }
                })
                .to_string(),
            )
            .unwrap();
            await_path(&hook);
        }
        let HerdrRouteSelection::Available(route) = self
            .nexus
            .store
            .flow_node(FLOW)
            .unwrap()
            .unwrap()
            .herdr_route_selection
        else {
            panic!("abc123 is routed")
        };
        RestartWorld {
            fresh,
            seat,
            old_agent: route.herdr_agent_name.0,
        }
    }

    fn reopen_store(&mut self) {
        use crate::store::{OpensFlowStore, SetsNativeOwnerEnvironmentForTests};
        let placeholder = FlowStore::open(&self.directory.path().join("placeholder.sema"))
            .expect("placeholder store");
        drop(std::mem::replace(&mut self.nexus.store, placeholder.into()));
        let mut reopened =
            FlowStore::open(&self.directory.path().join("flow.sema")).expect("store reopens");
        reopened.set_native_owner_environment_for_tests(crate::store::NativeOwnerEnvironment {
            host_id: "isolated-test-host".into(),
            boot_id: "isolated-test-boot".into(),
        });
        self.nexus.store = reopened.into();
    }
}

/// Whether a process the kernel knew has ended: gone, another process at
/// its pid, or a zombie its reaper has not yet collected.
fn ended(process: &Process) -> bool {
    match fs::read_to_string(format!("/proc/{}/stat", process.pid.0)) {
        Err(_) => true,
        Ok(stat) => stat.contains(") Z") || !FlowStore::process_is_live(process),
    }
}

/// Waits, bounded, for a file a fixture process writes: read at each
/// change in its directory.
fn await_path(path: &Path) {
    use crate::event_wait::AwaitsPath;
    assert!(
        path.appeared_by(std::time::Instant::now() + Duration::from_secs(10)),
        "{} never appeared",
        path.display()
    );
}

/// The pid a fixture process wrote, read at each change in the file's
/// directory until it parses, bounded.
fn pid_in(path: &Path) -> u32 {
    use crate::event_wait::AwaitsDirectory;
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    let written = || {
        fs::read_to_string(path)
            .unwrap_or_default()
            .trim()
            .parse::<u32>()
            .ok()
    };
    let directory = path.parent().expect("the pid file's directory");
    assert!(
        directory.until_holds(deadline, &mut || written().is_some()),
        "no pid written at {}",
        path.display()
    );
    written().expect("the pid written")
}

impl RestartWorld {
    /// The birth a hook of the resumed harness names: that harness's, once
    /// its hook has run.
    fn new_hook(&self) -> Process {
        pid_in(&self.fresh.state.join("hook-pid"));
        FlowStore::process_identity_for_pid(pid_in(&self.fresh.state.join("pid")))
            .expect("resumed harness running")
    }

    /// The birth a hook of the old harness names: that harness's, read
    /// while it runs.
    fn old_hook(&self) -> Process {
        pid_in(&self.fresh.state.join("old-hook-pid"));
        self.old_harness()
    }

    fn old_harness(&self) -> Process {
        FlowStore::process_identity_for_pid(pid_in(&self.fresh.state.join("old-pid")))
            .expect("old harness running")
    }

    /// Every Herdr call naming `fragment`.
    fn calls_naming(&self, fragment: &str) -> Vec<String> {
        self.fresh
            .calls()
            .lines()
            .filter(|line| line.contains(fragment))
            .map(str::to_owned)
            .collect()
    }

    fn resumes(&self) -> Vec<String> {
        self.calls_naming("agent start")
            .into_iter()
            .filter(|line| line.contains("--resume"))
            .collect()
    }
}

fn restarted(fixture: &NexusFixture) -> Response {
    fixture
        .nexus
        .dispatch(Query::Restart(flow_ethos::FlowId(FLOW.into())))
}

fn record(fixture: &NexusFixture) -> Option<RestartRecord> {
    fixture.nexus.store.restart_record(FLOW).unwrap()
}

fn node(fixture: &NexusFixture) -> signal_flow::FlowNode {
    fixture
        .nexus
        .store
        .flow_node(FLOW)
        .unwrap()
        .expect("abc123")
}

fn route_pane(fixture: &NexusFixture) -> String {
    match node(fixture).herdr_route_selection {
        HerdrRouteSelection::Available(route) => route.herdr_pane_id.0,
        HerdrRouteSelection::Unavailable => String::new(),
    }
}

/// A report as the ordinary socket serves it, naming the harness `birth`:
/// recorded, then acted on only when it names the bound seat's harness.
fn report_from(fixture: &NexusFixture, event: Event, birth: Option<Process>) -> bool {
    let acts = fixture.nexus.report_acts(FLOW, birth.as_ref());
    assert_eq!(
        fixture.nexus.report(Report_Data {
            flow_id: flow_ethos::FlowId(FLOW.into()),
            process_option: birth,
            event: event.clone(),
        }),
        Response::Reported
    );
    if acts {
        fixture
            .nexus
            .after_attributed_report(&flow_ethos::FlowId(FLOW.to_owned().to_owned()), &event);
    }
    acts
}

fn stop_of(world: &RestartWorld, fixture: &NexusFixture) -> Event {
    Event::Stopped(NativeStop {
        native_session_id_option: Some(signal_flow::NativeSessionId(world.seat.session.clone())),
        transcript_path_option: Some(signal_flow::TranscriptPath(
            fixture
                .directory
                .path()
                .join("native-transcripts/claude/-home-li-primary")
                .join(format!("{}.jsonl", world.seat.session))
                .to_string_lossy()
                .into_owned(),
        )),
        last_assistant_message_option: signal_flow::LastAssistantMessageOption(None),
        background_work: signal_flow::BackgroundWork(vec![]),
    })
}

fn sender() -> Address {
    Address {
        aspect: Aspect::Psyche,
        topic: flow_ethos::Topic("core".into()),
        model: Model::Opus,
    }
}

/// A process bound to the sender Address, as Bind binds one; dropped with
/// the test.
struct BoundSender(std::process::Child, Process);

impl Drop for BoundSender {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn bound_sender(fixture: &NexusFixture) -> BoundSender {
    let child = std::process::Command::new("sleep")
        .arg("60")
        .spawn()
        .expect("sender process");
    let process = FlowStore::process_identity_for_pid(child.id()).expect("sender identity");
    assert!(matches!(
        fixture.nexus.store.bind_metaflow(
            signal_flow::Bind_Data {
                address: sender(),
                process: process.clone(),
            },
            None,
        ),
        Ok(Response::Bound(_))
    ));
    BoundSender(child, process)
}

/// Sends a Soft Order to `{ Mind flow Sol }` from the bound sender, then
/// reads its wake as the ordinary socket does once it has answered.
fn send(fixture: &NexusFixture, from: &BoundSender, body: &str) -> Response {
    use crate::sending::SendsRequests;
    use crate::waking::SleepsAndWakesSeats;
    let answer = fixture.nexus.send_request(
        signal_flow::SendRequest {
            address: address(Model::Sol),
            sendable_message: signal_flow::SendableMessage::Soft(
                signal_flow::SendableRequest::Order(body.into()),
            ),
        },
        Some(&from.1),
    );
    if let Some(pending) = answer.pending {
        fixture.nexus.wake_due(pending.address);
    }
    answer.response
}

/// How many user turns of the session's transcript carry `body`: each one
/// a typing the seat received.
fn received(world: &RestartWorld, fixture: &NexusFixture, body: &str) -> usize {
    world
        .fresh
        .user_turns(fixture)
        .iter()
        .filter(|turn| turn.contains(body))
        .count()
}

/// abc123's restart as a Nexus writes it: its old route is the route the
/// flow was launched in, w1:p3.
fn spawning_record(world: &RestartWorld, pane: HerdrPaneBinding) -> RestartRecord {
    RestartRecord {
        flow_id: flow_ethos::FlowId(FLOW.into()),
        native_session_id: signal_flow::NativeSessionId(world.seat.session.clone()),
        address: address(Model::Sol),
        old_seat: OldSeat {
            herdr_route: HerdrRoute {
                herdr_session_name: signal_flow::HerdrSessionName("fixture-session".into()),
                herdr_agent_name: signal_flow::HerdrAgentName(world.old_agent.clone()),
                herdr_pane_id: signal_flow::HerdrPaneId(OLD_PANE.into()),
                herdr_terminal_id: signal_flow::HerdrTerminalId("term-old".into()),
            },
            process_option: None,
        },
        restart_phase: RestartPhase::Spawning(pane),
        attempts: meta_signal_flow::Attempts(1),
    }
}

/// What the fixture Herdr makes as a restart's next pane.
fn next_pane(agent: &str) -> HerdrPaneBinding {
    HerdrPaneBinding {
        launch_request_id: signal_flow::LaunchRequestId("restart-dead-nexus".into()),
        herdr_session_name: signal_flow::HerdrSessionName("fixture-session".into()),
        herdr_agent_name: signal_flow::HerdrAgentName(agent.into()),
        herdr_workspace_id: signal_flow::HerdrWorkspaceId("w7".into()),
        herdr_pane_id: signal_flow::HerdrPaneId(NEW_PANE.into()),
        herdr_terminal_id: signal_flow::HerdrTerminalId("term-fresh".into()),
    }
}

/// What a Nexus that died left in Herdr: the pane it made, and, when
/// `resumed`, the harness it resumed there.
fn left_by_dead_nexus(fixture: &NexusFixture, world: &RestartWorld, resumed: bool) {
    let herdr = |arguments: &[&str]| {
        assert!(
            std::process::Command::new(&fixture.snapshot_program)
                .args(arguments)
                .output()
                .expect("fixture Herdr")
                .status
                .success()
        );
    };
    herdr(&["--session", "fixture-session", "workspace", "create"]);
    if resumed {
        herdr(&[
            "--session",
            "fixture-session",
            "agent",
            "start",
            "restart-agent",
            "--kind",
            "claude",
            "--pane",
            NEW_PANE,
            "--",
            "--resume",
            &world.seat.session,
        ]);
        world.new_hook();
    }
}

/// abc123 Awake and resting is restarted as its own session: Restarted, the
/// same FlowId; `claude --resume <session>` in a new pane, with this
/// Nexus's hook socket, the title, model and effort its Address configures;
/// nothing typed; the old pane closed; the flow Active and Awake.
#[test]
fn restart_awake_resting_claude_resumes_same_session() {
    let mut fixture = NexusFixture::new();
    let world = fixture.restart_world(true);
    let old = world.old_harness();

    assert_eq!(
        restarted(&fixture),
        Response::Restarted(flow_ethos::FlowId(FLOW.into()))
    );

    let resumes = world.resumes();
    assert_eq!(resumes.len(), 1, "{}", world.fresh.calls());
    let resume = &resumes[0];
    assert!(resume.contains(&format!("--pane {NEW_PANE}")), "{resume}");
    assert!(
        resume.contains(&format!("--resume {}", world.seat.session)),
        "{resume}"
    );
    assert!(!resume.contains("--session-id"), "{resume}");
    assert!(
        resume.contains("/fixture/run/flow-next/flow/flow.sock"),
        "the hook reports to this Nexus: {resume}"
    );
    assert!(
        resume.contains("--name { Mind flow Sol abc123 }"),
        "{resume}"
    );
    assert!(resume.contains("--model claude-fixture"), "{resume}");
    assert!(
        resume.contains(&format!(
            "--effort {}",
            <crate::RunningNexus as crate::sending::ChoosesEffort>::effort_of(&address(Model::Sol))
        )),
        "{resume}"
    );
    assert!(
        world.calls_naming("agent prompt").is_empty(),
        "nothing is submitted: {}",
        world.fresh.calls()
    );
    assert!(world.fresh.user_turns(&fixture).is_empty());
    assert_eq!(
        world.calls_naming(&format!("pane close {OLD_PANE}")).len(),
        1
    );
    let close = world
        .fresh
        .calls()
        .lines()
        .position(|line| line.contains(&format!("pane close {OLD_PANE}")))
        .unwrap();
    let create = world
        .fresh
        .calls()
        .lines()
        .position(|line| line.contains("workspace create"))
        .unwrap();
    assert!(close < create, "the old harness closes before the new pane");
    assert!(ended(&old), "the old harness exited");

    let after = node(&fixture);
    assert_eq!(after.session_id.0, world.seat.session);
    assert_eq!(after.flow_lifecycle, FlowLifecycle::Active);
    assert_eq!(route_pane(&fixture), NEW_PANE);
    assert_eq!(
        stored(&fixture, Model::Sol).state,
        State::Awake(flow_ethos::FlowId(FLOW.into()))
    );
    assert_eq!(record(&fixture), None);
    let birth = fixture
        .nexus
        .store
        .seat_birth(FLOW)
        .unwrap()
        .expect("birth");
    assert_eq!(
        Some(birth.process),
        FlowStore::process_identity_for_pid(pid_in(&world.fresh.state.join("pid")))
    );
}

/// A working seat's Restart answers RestartQueued and closes nothing; at
/// its next Stop, background work still in flight, the restart is carried
/// out as a resting seat's is: the old pane closed, the same session
/// resumed in a new pane and bound, the order gone.
#[test]
fn restart_of_a_working_seat_is_carried_out_at_its_next_stop() {
    use crate::store::ordered_replacement::{OrdersReplacements, ReplacementOrder};
    let mut fixture = NexusFixture::new();
    let world = fixture.restart_world(true);
    let old = world.old_harness();
    let agent = world.fresh.state.join("old-agent");
    let shown = fs::read_to_string(&agent).unwrap();
    fs::write(
        &agent,
        shown.replace("\"agent_status\":\"idle\"", "\"agent_status\":\"working\""),
    )
    .unwrap();
    assert_eq!(
        restarted(&fixture),
        Response::RestartQueued(flow_ethos::FlowId(FLOW.into()))
    );
    assert!(world.calls_naming("pane close").is_empty());
    assert!(world.resumes().is_empty());
    assert!(matches!(
        fixture.nexus.store.ordered_replacement(&address(Model::Sol)),
        Ok(Some(order)) if order.replacement_order == ReplacementOrder::Restart && order.predecessor.0 == FLOW
    ));
    // Asked again, it stays queued.
    assert_eq!(
        restarted(&fixture),
        Response::RestartQueued(flow_ethos::FlowId(FLOW.into()))
    );

    // Its Stop names work in flight; the restart runs at it all the same.
    let mut stop = stop_of(&world, &fixture);
    if let Event::Stopped(native) = &mut stop {
        native.background_work = signal_flow::BackgroundWork(vec![signal_flow::BackgroundTask {
            background_task_id: signal_flow::BackgroundTaskId("watch".into()),
            background_task_kind: signal_flow::BackgroundTaskKind::Monitor,
            background_task_status: signal_flow::BackgroundTaskStatus("running".into()),
            description: signal_flow::Description("a comment watch".into()),
        }]);
    }
    assert!(report_from(&fixture, stop, Some(world.old_hook())));
    let resumes = world.resumes();
    assert_eq!(resumes.len(), 1, "{}", world.fresh.calls());
    assert!(resumes[0].contains(&format!("--resume {}", world.seat.session)));
    assert_eq!(
        world.calls_naming(&format!("pane close {OLD_PANE}")).len(),
        1
    );
    assert!(ended(&old), "the old harness exited");
    assert_eq!(route_pane(&fixture), NEW_PANE);
    assert_eq!(record(&fixture), None);
    assert_eq!(
        fixture
            .nexus
            .store
            .ordered_replacement(&address(Model::Sol))
            .unwrap(),
        None
    );
    assert_eq!(
        stored(&fixture, Model::Sol).state,
        State::Awake(flow_ethos::FlowId(FLOW.into()))
    );
}

/// A Send while the restart holds its spawn queues, and nothing reaches any
/// pane; once bound, the queue is handed to the resumed seat exactly once,
/// and a second drain hands it nothing more.
#[test]
fn restart_arrival_queued_delivered_once() {
    use crate::restarting::RestartsFlows;
    let mut fixture = NexusFixture::new();
    let world = fixture.restart_world(true);
    let from = bound_sender(&fixture);
    use crate::fixture_executable::{HoldsLatch, Latch};
    let release_start = Latch::closed(world.fresh.state.join("release-start"));
    fs::write(world.fresh.state.join("hold-start"), "").unwrap();

    std::thread::scope(|scope| {
        let restarting = scope.spawn(|| fixture.nexus.restart(FLOW));
        await_path(&world.fresh.state.join("start-held"));
        assert!(matches!(
            record(&fixture).map(|record| record.restart_phase),
            Some(RestartPhase::Spawning(_))
        ));
        let queued = send(&fixture, &from, "arrived during the restart");
        assert!(
            matches!(
                &queued,
                Response::SendAccepted(acceptance)
                    if acceptance.visible_delivery_state == signal_flow::VisibleDeliveryState::Queued
            ),
            "{queued:?}"
        );
        assert_eq!(stored(&fixture, Model::Sol).queue.0.len(), 1);
        assert!(world.calls_naming("agent prompt").is_empty());
        release_start.release();
        assert_eq!(
            restarting.join().unwrap(),
            Response::Restarted(flow_ethos::FlowId(FLOW.into()))
        );
    });

    assert_eq!(received(&world, &fixture, "arrived during the restart"), 1);
    assert!(
        world
            .calls_naming("agent prompt")
            .iter()
            .all(|line| !line.contains(OLD_PANE)),
        "the old seat receives nothing: {}",
        world.fresh.calls()
    );
    assert!(stored(&fixture, Model::Sol).queue.0.is_empty());
    fixture.nexus.drain_awake(&address(Model::Sol));
    assert_eq!(received(&world, &fixture, "arrived during the restart"), 1);
}

/// After the bind, the old seat's late Stopped and SessionEnd, from its hook
/// process, act on nothing, nor does a report no caller stands behind, nor
/// an Observe's Gone of the old pane: the flow stays Active and Awake, and a
/// Maintained metaflow owes no successor.
#[test]
fn restart_old_session_end_fenced() {
    let mut fixture = NexusFixture::new();
    let world = fixture.restart_world(true);
    maintain(&fixture, address(Model::Sol));
    let old_hook = world.old_hook();
    let old_witness = fixture.nexus.seat_witness(FLOW).expect("witness");
    assert_eq!(
        restarted(&fixture),
        Response::Restarted(flow_ethos::FlowId(FLOW.into()))
    );

    // The old hook outlived its harness, as a SessionEnd hook does, and
    // names that harness's birth.
    let ended = Event::Ended(Some(signal_flow::NativeSessionId(
        world.seat.session.clone(),
    )));
    assert!(!report_from(
        &fixture,
        stop_of(&world, &fixture),
        Some(old_hook.clone())
    ));
    assert!(!report_from(
        &fixture,
        ended.clone(),
        Some(old_hook.clone())
    ));
    assert!(!report_from(&fixture, ended.clone(), None));
    use crate::observe_agent::RecordsWitnessedExit;
    fixture
        .nexus
        .exited_as_witnessed_by(FLOW, Some(&old_witness));

    assert_eq!(node(&fixture).flow_lifecycle, FlowLifecycle::Active);
    assert_eq!(
        stored(&fixture, Model::Sol).state,
        State::Awake(flow_ethos::FlowId(FLOW.into()))
    );
    assert_eq!(
        fixture
            .nexus
            .store
            .successor_intent(&address(Model::Sol))
            .unwrap(),
        None
    );
    assert!(
        world
            .calls_naming(&format!("pane close {NEW_PANE}"))
            .is_empty()
    );
}

/// After Restarted the new seat's own exit ends the flow as today: its
/// hook's SessionEnd, attributed by the hook's ancestry though Claude
/// scrubbed the hook's environment; and, in a second world, an Observe's
/// Gone of the pane the new seat runs in.
#[test]
fn restart_new_seat_real_exit_ends() {
    use crate::observe_agent::RecordsWitnessedExit;
    let mut fixture = NexusFixture::new();
    let world = fixture.restart_world(true);
    assert_eq!(
        restarted(&fixture),
        Response::Restarted(flow_ethos::FlowId(FLOW.into()))
    );
    let hook = world.new_hook();
    assert!(report_from(
        &fixture,
        Event::Ended(Some(signal_flow::NativeSessionId(
            world.seat.session.clone()
        ))),
        Some(hook)
    ));
    assert_eq!(node(&fixture).flow_lifecycle, FlowLifecycle::Exited);
    assert_eq!(stored(&fixture, Model::Sol).state, State::Asleep);

    let mut second = NexusFixture::new();
    let _world = second.restart_world(true);
    assert_eq!(
        restarted(&second),
        Response::Restarted(flow_ethos::FlowId(FLOW.into()))
    );
    let witness: SeatWitness = second.nexus.seat_witness(FLOW).expect("witness");
    assert_eq!(witness.route.herdr_pane_id.0, NEW_PANE);
    second.nexus.exited_as_witnessed_by(FLOW, Some(&witness));
    assert_eq!(node(&second).flow_lifecycle, FlowLifecycle::Exited);
}

/// The resumed seat's hook reports to this Nexus and acts: the hook command
/// names this Nexus's socket, a Stop no caller stands behind settles
/// nothing, and the same Stop from the resumed seat's hook drains its queue.
#[test]
fn restart_resumed_hooks_report_to_live_nexus() {
    let mut fixture = NexusFixture::new();
    let world = fixture.restart_world(true);
    assert_eq!(
        restarted(&fixture),
        Response::Restarted(flow_ethos::FlowId(FLOW.into()))
    );
    assert!(world.resumes()[0].contains("/fixture/run/flow-next/flow/flow.sock"));
    let mut queued = stored(&fixture, Model::Sol);
    queued.queue = flow_ethos::Queue(vec![signal_flow::QueueEntry {
        logical_delivery_id: flow_ethos::LogicalDeliveryId("after-restart".into()),
        request_id: flow_ethos::RequestId("test".into()),
        sent_by: signal_flow::SentBy::Unknown,
        delivery: flow_ethos::Delivery::Wake,
        message: signal_flow::Message::Soft(signal_flow::Request::Order(
            "for the resumed seat".into(),
        )),
    }]);
    fixture.nexus.store.seed_metaflow(queued);

    assert!(!report_from(&fixture, stop_of(&world, &fixture), None));
    assert_eq!(received(&world, &fixture, "for the resumed seat"), 0);
    assert!(report_from(
        &fixture,
        stop_of(&world, &fixture),
        Some(world.new_hook())
    ));
    assert_eq!(received(&world, &fixture, "for the resumed seat"), 1);
    let events = crate::store::events::RecordsHarnessEvents::events(&*fixture.nexus.store, FLOW)
        .unwrap()
        .unwrap();
    assert_eq!(events.len(), 2, "both reports are recorded");
}

/// Herdr refuses the close: CloseRefused, the record Failed and the flow as
/// it was in its pane. A retry re-observes the old pane and closes it, never
/// skipping to Closed, and resumes.
#[test]
fn restart_close_refused_flow_unchanged() {
    let mut fixture = NexusFixture::new();
    let world = fixture.restart_world(true);
    let old = world.old_harness();
    fs::write(world.fresh.state.join("close-refused"), "").unwrap();

    assert_eq!(
        restarted(&fixture),
        Response::RestartRefused(RestartRefusal::CloseRefused(flow_ethos::FlowId(
            FLOW.into()
        )))
    );
    let failed = record(&fixture).expect("record");
    assert_eq!(
        failed.restart_phase,
        RestartPhase::Failed(RestartFailure::CloseRefused)
    );
    assert_eq!(failed.old_seat.process_option, Some(old.clone()));
    assert!(FlowStore::process_is_live(&old), "the old harness runs on");
    assert_eq!(route_pane(&fixture), OLD_PANE);
    assert_eq!(node(&fixture).flow_lifecycle, FlowLifecycle::Active);
    assert!(world.calls_naming("workspace create").is_empty());

    fs::remove_file(world.fresh.state.join("close-refused")).unwrap();
    assert_eq!(
        restarted(&fixture),
        Response::Restarted(flow_ethos::FlowId(FLOW.into()))
    );
    assert_eq!(
        world.calls_naming(&format!("pane close {OLD_PANE}")).len(),
        2,
        "the retry closes the old pane again: {}",
        world.fresh.calls()
    );
    assert!(ended(&old));
    assert_eq!(route_pane(&fixture), NEW_PANE);
}

/// The resume is refused once its harness runs, and refused again when the
/// restart reopens it by itself: ResumeRefused, the record Failed, each
/// pane discarded, the session and queue kept. The owner's Stop records it
/// Stopped, and a retry, the last harness proven exited, then resumes it
/// and hands it its queue once.
#[test]
fn restart_resume_refused_custody_kept() {
    let mut fixture = NexusFixture::new();
    let world = fixture.restart_world(true);
    let mut queued = stored(&fixture, Model::Sol);
    queued.queue = flow_ethos::Queue(vec![signal_flow::QueueEntry {
        logical_delivery_id: flow_ethos::LogicalDeliveryId("kept-through-refusal".into()),
        request_id: flow_ethos::RequestId("test".into()),
        sent_by: signal_flow::SentBy::Unknown,
        delivery: flow_ethos::Delivery::Wake,
        message: signal_flow::Message::Soft(signal_flow::Request::Order("kept work".into())),
    }]);
    fixture.nexus.store.seed_metaflow(queued);
    fs::write(world.fresh.state.join("refused-after-start"), "").unwrap();

    assert_eq!(
        restarted(&fixture),
        Response::RestartRefused(RestartRefusal::ResumeRefused(ResumeRefused_Data {
            flow_id: flow_ethos::FlowId(FLOW.into()),
            string: "SpawnRefused".into(),
        }))
    );
    assert_eq!(
        record(&fixture).map(|record| record.restart_phase),
        Some(RestartPhase::Failed(RestartFailure::ResumeRefused(
            "SpawnRefused".into()
        )))
    );
    assert_eq!(
        world.calls_naming(&format!("pane close {NEW_PANE}")).len(),
        1
    );
    assert_eq!(node(&fixture).session_id.0, world.seat.session);
    assert_eq!(stored(&fixture, Model::Sol).queue.0.len(), 1);
    fixture.nexus.drain_awake(&address(Model::Sol));
    assert_eq!(received(&world, &fixture, "kept work"), 0, "custody kept");

    assert_eq!(
        fixture
            .nexus
            .dispatch(Query::Stop(flow_ethos::FlowId(FLOW.into()))),
        Response::Stopped(flow_ethos::FlowId(FLOW.into()))
    );
    assert_eq!(
        record(&fixture).map(|record| record.restart_phase),
        Some(RestartPhase::Failed(RestartFailure::Stopped))
    );
    assert_eq!(node(&fixture).flow_lifecycle, FlowLifecycle::Active);
    assert_eq!(stored(&fixture, Model::Sol).queue.0.len(), 1);

    fs::remove_file(world.fresh.state.join("refused-after-start")).unwrap();
    assert_eq!(
        restarted(&fixture),
        Response::Restarted(flow_ethos::FlowId(FLOW.into()))
    );
    assert_eq!(received(&world, &fixture, "kept work"), 1);
    assert_eq!(record(&fixture), None);
}

/// The old pane is already gone and no harness of it is seen or recorded:
/// a pane's absence proves no exit, so the restart is refused
/// ExitUnwitnessed before any write: no record, nothing closed or started,
/// abc123 Active and Awake in w1:p3 as it was.
#[test]
fn restart_first_old_pane_gone_unrecorded_refused_unwitnessed() {
    let mut fixture = NexusFixture::new();
    let world = fixture.restart_world(false);
    let before = node(&fixture);
    assert_eq!(restarted(&fixture), resume_refused("ExitUnwitnessed"));
    assert_eq!(record(&fixture), None, "nothing written");
    assert_eq!(fixture.nexus.store.seat_birth(FLOW).unwrap(), None);
    assert_eq!(node(&fixture), before);
    assert_eq!(route_pane(&fixture), OLD_PANE);
    assert!(world.resumes().is_empty(), "{}", world.fresh.calls());
    assert!(
        world.calls_naming("pane close").is_empty(),
        "{}",
        world.fresh.calls()
    );
}

/// The old pane is gone and the harness recorded for its seat still runs,
/// born when the record says: refused ExitUnproven before any write, the
/// harness untouched, no record, nothing closed or started, abc123 and its
/// recorded birth as they were.
#[test]
fn restart_first_old_pane_gone_harness_alive_refused() {
    let mut fixture = NexusFixture::new();
    let world = fixture.restart_world(false);
    let running = live_process();
    let birth = born_at_route(&fixture, running.1.clone());
    let before = node(&fixture);
    assert_eq!(restarted(&fixture), resume_refused("ExitUnproven"));
    assert_eq!(record(&fixture), None, "nothing written");
    assert_eq!(fixture.nexus.store.seat_birth(FLOW).unwrap(), Some(birth));
    assert_eq!(node(&fixture), before);
    assert!(FlowStore::process_is_live(&running.1), "harness untouched");
    assert!(world.resumes().is_empty(), "{}", world.fresh.calls());
    assert!(
        world.calls_naming("pane close").is_empty(),
        "{}",
        world.fresh.calls()
    );
}

/// The old pane is gone and the harness recorded for its seat is proven
/// exited by its birth: the restart closes nothing and resumes the same
/// session.
#[test]
fn restart_first_old_pane_gone_harness_exited_proceeds() {
    let mut fixture = NexusFixture::new();
    let world = fixture.restart_world(false);
    born_at_route(&fixture, exited_process());
    assert_eq!(
        restarted(&fixture),
        Response::Restarted(flow_ethos::FlowId(FLOW.into()))
    );
    assert!(
        world.calls_naming("pane close").is_empty(),
        "{}",
        world.fresh.calls()
    );
    assert_eq!(world.resumes().len(), 1);
    assert_eq!(route_pane(&fixture), NEW_PANE);
    assert_eq!(node(&fixture).session_id.0, world.seat.session);
    assert_eq!(record(&fixture), None);
}

/// A restart bound abc123's seat at the route it runs in now, `process` its
/// harness: its SeatBirth, recorded as a bind records it.
fn born_at_route(fixture: &NexusFixture, process: Process) -> SeatBirth {
    let HerdrRouteSelection::Available(route) = node(fixture).herdr_route_selection else {
        panic!("abc123 is routed")
    };
    let pane = HerdrPaneBinding {
        launch_request_id: signal_flow::LaunchRequestId("restart-earlier".into()),
        herdr_session_name: route.herdr_session_name.clone(),
        herdr_agent_name: route.herdr_agent_name.clone(),
        herdr_workspace_id: signal_flow::HerdrWorkspaceId("w1".into()),
        herdr_pane_id: route.herdr_pane_id.clone(),
        herdr_terminal_id: route.herdr_terminal_id.clone(),
    };
    let store = &fixture.nexus.store;
    store
        .record_restart(RestartRecord {
            restart_phase: RestartPhase::Spawning(pane),
            ..spawning_record_at(fixture)
        })
        .unwrap();
    let birth = SeatBirth {
        flow_id: flow_ethos::FlowId(FLOW.into()),
        herdr_route: route,
        process,
    };
    assert!(store.rebind_restarted(birth.clone()).unwrap());
    assert_eq!(record(fixture), None);
    birth
}

/// abc123's restart record, its old seat its current route, no harness.
fn spawning_record_at(fixture: &NexusFixture) -> RestartRecord {
    let node = node(fixture);
    let HerdrRouteSelection::Available(route) = node.herdr_route_selection else {
        panic!("abc123 is routed")
    };
    RestartRecord {
        flow_id: flow_ethos::FlowId(FLOW.into()),
        native_session_id: signal_flow::NativeSessionId(node.session_id.0),
        address: address(Model::Sol),
        old_seat: OldSeat {
            herdr_route: route,
            process_option: None,
        },
        restart_phase: RestartPhase::Closing,
        attempts: meta_signal_flow::Attempts(1),
    }
}

/// A flow Flow does not hold is refused UnknownFlow, and nothing is closed.
#[test]
fn restart_unknown_flow() {
    let mut fixture = NexusFixture::new();
    let world = fixture.restart_world(true);
    assert_eq!(
        fixture
            .nexus
            .dispatch(Query::Restart(flow_ethos::FlowId("fade00".into()))),
        Response::RestartRefused(RestartRefusal::UnknownFlow(flow_ethos::FlowId(
            "fade00".into()
        )))
    );
    assert!(world.calls_naming("pane close").is_empty());
    assert_eq!(fixture.nexus.store.restart_records().unwrap(), vec![]);
}

/// A live Claude flow with no native session recorded is refused
/// NoHeldSession, and nothing is closed.
#[test]
fn restart_no_held_session() {
    let mut fixture = NexusFixture::new();
    let world = fixture.restart_world(true);
    let mut unheld = node(&fixture);
    unheld.flow_id = flow_ethos::FlowId("5e5510".into());
    unheld.session_id = signal_flow::SessionId(String::new());
    unheld.herdr_route_selection = HerdrRouteSelection::Available(HerdrRoute {
        herdr_session_name: signal_flow::HerdrSessionName("fixture-session".into()),
        herdr_agent_name: signal_flow::HerdrAgentName("unheld".into()),
        herdr_pane_id: signal_flow::HerdrPaneId("w2:p9".into()),
        herdr_terminal_id: signal_flow::HerdrTerminalId("term-unheld".into()),
    });
    assert!(matches!(
        fixture.nexus.store.register_flow(unheld).unwrap(),
        crate::store::FlowRegistration::Registered(_)
    ));
    assert_eq!(
        fixture
            .nexus
            .dispatch(Query::Restart(flow_ethos::FlowId("5e5510".into()))),
        Response::RestartRefused(RestartRefusal::NoHeldSession(flow_ethos::FlowId(
            "5e5510".into()
        )))
    );
    assert!(world.calls_naming("pane close").is_empty());
    assert_eq!(fixture.nexus.store.restart_records().unwrap(), vec![]);
}

/// A flow whose transcript is gone is refused TranscriptAbsent, and nothing
/// is closed.
#[test]
fn restart_transcript_absent() {
    let mut fixture = NexusFixture::new();
    let world = fixture.restart_world(true);
    fs::remove_file(
        fixture
            .directory
            .path()
            .join("native-transcripts/claude/-home-li-primary")
            .join(format!("{}.jsonl", world.seat.session)),
    )
    .unwrap();
    assert_eq!(
        restarted(&fixture),
        Response::RestartRefused(RestartRefusal::TranscriptAbsent(flow_ethos::FlowId(
            FLOW.into()
        )))
    );
    assert!(world.calls_naming("pane close").is_empty());
    assert_eq!(record(&fixture), None);
}

/// A flow whose Address a wake holds, or whose restart is under way, is
/// refused Busy, and nothing is closed or written.
#[test]
fn restart_busy() {
    use crate::waking::HoldsWakingMetaflows;
    let mut fixture = NexusFixture::new();
    let world = fixture.restart_world(true);
    {
        let _wake = fixture.nexus.waking.hold(&address(Model::Sol)).unwrap();
        assert_eq!(
            restarted(&fixture),
            Response::RestartRefused(RestartRefusal::Busy(flow_ethos::FlowId(FLOW.into())))
        );
    }
    assert_eq!(record(&fixture), None);
    let under_way = RestartRecord {
        restart_phase: RestartPhase::Closed,
        ..spawning_record(&world, next_pane("restart-agent"))
    };
    fixture
        .nexus
        .store
        .record_restart(under_way.clone())
        .unwrap();
    assert_eq!(
        restarted(&fixture),
        Response::RestartRefused(RestartRefusal::Busy(flow_ethos::FlowId(FLOW.into())))
    );
    assert_eq!(record(&fixture), Some(under_way));
    assert!(world.calls_naming("pane close").is_empty());
}

/// Each resumed pane, the first and the one the restart reopens by itself,
/// reports another session: ResumeRefused BindMismatch, the record Failed,
/// each pane discarded, and no new identity minted: the flow keeps its
/// session and no FlowId is claimed.
#[test]
fn restart_bind_mismatch() {
    let mut fixture = NexusFixture::new();
    let world = fixture.restart_world(true);
    let flows_before = fixture.nexus.store.flow_nodes().unwrap().len();
    let claims_before =
        fs::read_to_string(fixture.directory.path().join("flow-id-calls")).unwrap_or_default();
    fs::write(
        world.fresh.state.join("report-session"),
        "f0f0f0f0-0000-4000-8000-00000000f0f0",
    )
    .unwrap();

    assert_eq!(
        restarted(&fixture),
        Response::RestartRefused(RestartRefusal::ResumeRefused(ResumeRefused_Data {
            flow_id: flow_ethos::FlowId(FLOW.into()),
            string: "BindMismatch".into(),
        }))
    );
    assert_eq!(
        record(&fixture).map(|record| (record.restart_phase, record.attempts.0)),
        Some((RestartPhase::Failed(RestartFailure::BindMismatch), 2))
    );
    assert_eq!(world.resumes().len(), 2);
    assert_eq!(
        world.calls_naming(&format!("pane close {NEW_PANE}")).len(),
        1
    );
    assert_eq!(
        world
            .calls_naming(&format!("pane close {REOPENED_PANE}"))
            .len(),
        1
    );
    assert_eq!(node(&fixture).session_id.0, world.seat.session);
    assert_eq!(
        fixture.nexus.store.flow_nodes().unwrap().len(),
        flows_before
    );
    assert_eq!(
        fs::read_to_string(fixture.directory.path().join("flow-id-calls")).unwrap_or_default(),
        claims_before
    );
    assert_eq!(fixture.nexus.store.seat_birth(FLOW).unwrap(), None);
}

/// Inspect shows the restart record in each phase as Flow holds it, and
/// changes nothing.
#[test]
fn restart_inspect_shows_record() {
    let mut fixture = NexusFixture::new();
    let world = fixture.restart_world(true);
    let inspect = || {
        let meta_signal_flow::Response::Inspected(inspected) = fixture
            .nexus
            .dispatch_meta(meta_signal_flow::Query::Inspect(address(Model::Sol)))
        else {
            panic!("the metaflow is inspected")
        };
        inspected.restart_record_option
    };
    assert_eq!(inspect(), None);
    let pane = next_pane("restart-agent");
    for phase in [
        RestartPhase::Closing,
        RestartPhase::Closed,
        RestartPhase::Opening,
        RestartPhase::Spawning(pane.clone()),
        RestartPhase::Failed(RestartFailure::CloseUnproven),
        RestartPhase::Interrupted(Some(pane.clone())),
        RestartPhase::Interrupted(None),
    ] {
        let held = RestartRecord {
            restart_phase: phase,
            attempts: meta_signal_flow::Attempts(3),
            ..spawning_record(&world, pane.clone())
        };
        fixture.nexus.store.record_restart(held.clone()).unwrap();
        assert_eq!(inspect(), Some(held.clone()));
        assert_eq!(inspect(), Some(held.clone()));
        assert_eq!(record(&fixture), Some(held));
    }
    assert!(
        world
            .fresh
            .calls()
            .lines()
            .all(|line| line.contains("api snapshot")
                || line.contains("process-info")
                || line.contains("session list"))
    );
}

/// A Nexus reopened at each phase takes it up: Closing closes the old pane
/// again and resumes; Closed resumes; Opening is Interrupted with no pane
/// made; Spawning whose pane shows no seat is Interrupted with no harness
/// started; Spawning whose pane runs the session binds it without starting
/// another; Failed whose bound is spent stays Failed.
#[test]
fn restart_nexus_death_each_phase() {
    let reopened_at = |phase: &str| {
        let mut fixture = NexusFixture::new();
        let world = fixture.restart_world(phase == "Closing");
        let pane = next_pane("restart-agent");
        let at = match phase {
            "Closing" => RestartPhase::Closing,
            "Closed" => RestartPhase::Closed,
            "Opening" => RestartPhase::Opening,
            "Failed" => RestartPhase::Failed(RestartFailure::ResumeRefused("Spawn".into())),
            _ => RestartPhase::Spawning(pane.clone()),
        };
        let mut left = RestartRecord {
            restart_phase: at,
            ..spawning_record(&world, pane.clone())
        };
        if phase == "Failed" {
            left.attempts = meta_signal_flow::Attempts(2);
        }
        if phase == "Closing" {
            left.old_seat.process_option = Some(world.old_harness());
        }
        match phase {
            "SpawningEmpty" => left_by_dead_nexus(&fixture, &world, false),
            "SpawningLive" => left_by_dead_nexus(&fixture, &world, true),
            _ => {}
        }
        fixture.nexus.store.record_restart(left).unwrap();
        fixture.reopen_store();
        fixture.nexus.recover_wakes();
        (fixture, world)
    };

    let (fixture, world) = reopened_at("Closing");
    assert_eq!(
        world.calls_naming(&format!("pane close {OLD_PANE}")).len(),
        1
    );
    assert_eq!(world.resumes().len(), 1);
    assert_eq!(record(&fixture), None);
    assert_eq!(route_pane(&fixture), NEW_PANE);

    let (fixture, world) = reopened_at("Closed");
    assert_eq!(world.resumes().len(), 1);
    assert_eq!(record(&fixture), None);
    assert_eq!(route_pane(&fixture), NEW_PANE);

    let (fixture, world) = reopened_at("Opening");
    assert_eq!(
        record(&fixture).map(|record| record.restart_phase),
        Some(RestartPhase::Interrupted(None))
    );
    assert!(world.calls_naming("workspace create").is_empty());
    assert!(world.resumes().is_empty());

    let (fixture, world) = reopened_at("SpawningEmpty");
    assert_eq!(
        record(&fixture).map(|record| record.restart_phase),
        Some(RestartPhase::Interrupted(Some(next_pane("restart-agent"))))
    );
    assert!(world.calls_naming("agent start").is_empty(), "no respawn");
    assert_eq!(
        restarted(&fixture),
        Response::RestartRefused(RestartRefusal::Interrupted(flow_ethos::FlowId(FLOW.into())))
    );

    let (fixture, world) = reopened_at("SpawningLive");
    assert_eq!(
        world.calls_naming("agent start").len(),
        1,
        "only the dead Nexus's"
    );
    assert_eq!(record(&fixture), None);
    assert_eq!(route_pane(&fixture), NEW_PANE);

    let (fixture, world) = reopened_at("Failed");
    assert_eq!(
        record(&fixture).map(|record| record.restart_phase),
        Some(RestartPhase::Failed(RestartFailure::ResumeRefused(
            "Spawn".into()
        )))
    );
    assert!(world.calls_naming("agent start").is_empty());
    fixture.nexus.drain_awake(&address(Model::Sol));
}

/// Bound commits the binding and removes the lock in one write, then the
/// queue goes through the ordinary custody. A Nexus that dies before that
/// write binds the seat its pane runs when it starts again and hands the
/// queue over once; one that dies after it hands the queue over once at its
/// start; a later start hands nothing again.
#[test]
fn restart_crash_around_bind_commit_delivers_once() {
    for committed in [false, true] {
        let mut fixture = NexusFixture::new();
        let world = fixture.restart_world(false);
        let pane = next_pane("restart-agent");
        let mut queued = stored(&fixture, Model::Sol);
        queued.queue = flow_ethos::Queue(vec![signal_flow::QueueEntry {
            logical_delivery_id: flow_ethos::LogicalDeliveryId("around-the-commit".into()),
            request_id: flow_ethos::RequestId("test".into()),
            sent_by: signal_flow::SentBy::Unknown,
            delivery: flow_ethos::Delivery::Wake,
            message: signal_flow::Message::Soft(signal_flow::Request::Order(
                "queued around the commit".into(),
            )),
        }]);
        fixture.nexus.store.seed_metaflow(queued);
        left_by_dead_nexus(&fixture, &world, true);
        fixture
            .nexus
            .store
            .record_restart(spawning_record(&world, pane.clone()))
            .unwrap();
        if committed {
            let process =
                FlowStore::process_identity_for_pid(pid_in(&world.fresh.state.join("pid")))
                    .unwrap();
            assert_eq!(
                fixture
                    .nexus
                    .perform(Operation::Record(Record_Data::Rebound(SeatBirth {
                        flow_id: flow_ethos::FlowId(FLOW.into()),
                        herdr_route: HerdrRoute {
                            herdr_session_name: signal_flow::HerdrSessionName(
                                "fixture-session".into()
                            ),
                            herdr_agent_name: signal_flow::HerdrAgentName("restart-agent".into()),
                            herdr_pane_id: signal_flow::HerdrPaneId(NEW_PANE.into()),
                            herdr_terminal_id: signal_flow::HerdrTerminalId("term-fresh".into()),
                        },
                        process,
                    }))),
                Outcome::Recorded
            );
            assert_eq!(record(&fixture), None, "the lock went with the binding");
        }
        assert_eq!(received(&world, &fixture, "queued around the commit"), 0);
        fixture.reopen_store();
        fixture.nexus.recover_wakes();
        assert_eq!(
            received(&world, &fixture, "queued around the commit"),
            1,
            "committed {committed}: {}",
            world.fresh.calls()
        );
        assert_eq!(record(&fixture), None);
        assert_eq!(route_pane(&fixture), NEW_PANE);
        fixture.reopen_store();
        fixture.nexus.recover_wakes();
        assert_eq!(received(&world, &fixture, "queued around the commit"), 1);
        assert!(stored(&fixture, Model::Sol).queue.0.is_empty());
    }
}

/// An Interrupted restart whose pane is unknown cannot be shown quiesced:
/// Stop answers SpawnUnproven and keeps it, and Retire does not free its
/// Address. One whose recorded pane Herdr shows is closed by the Stop, and
/// only then recorded Stopped; a retry resumes it.
#[test]
fn restart_interrupted_stop_needs_quiesced_spawn() {
    let mut fixture = NexusFixture::new();
    let world = fixture.restart_world(false);
    let unknown = RestartRecord {
        restart_phase: RestartPhase::Interrupted(None),
        ..spawning_record(&world, next_pane("restart-agent"))
    };
    fixture.nexus.store.record_restart(unknown.clone()).unwrap();
    assert_eq!(
        fixture
            .nexus
            .dispatch(Query::Stop(flow_ethos::FlowId(FLOW.into()))),
        Response::StopRejected(StopRejection::SpawnUnproven)
    );
    assert_eq!(record(&fixture), Some(unknown));
    assert_eq!(
        fixture
            .nexus
            .dispatch_meta(meta_signal_flow::Query::Retire(flow_ethos::FlowId(
                FLOW.into()
            ))),
        meta_signal_flow::Response::RetireRejected(
            meta_signal_flow::RetireRejection::RestartUnresolved
        )
    );
    assert_eq!(node(&fixture).flow_lifecycle, FlowLifecycle::Active);
    assert_eq!(
        restarted(&fixture),
        Response::RestartRefused(RestartRefusal::Interrupted(flow_ethos::FlowId(FLOW.into())))
    );

    left_by_dead_nexus(&fixture, &world, true);
    let pane = next_pane("restart-agent");
    fixture
        .nexus
        .store
        .record_restart(RestartRecord {
            restart_phase: RestartPhase::Interrupted(Some(pane.clone())),
            ..spawning_record(&world, pane)
        })
        .unwrap();
    let spawned =
        FlowStore::process_identity_for_pid(pid_in(&world.fresh.state.join("pid"))).unwrap();
    assert_eq!(
        fixture
            .nexus
            .dispatch(Query::Stop(flow_ethos::FlowId(FLOW.into()))),
        Response::Stopped(flow_ethos::FlowId(FLOW.into()))
    );
    assert_eq!(
        record(&fixture).map(|record| record.restart_phase),
        Some(RestartPhase::Failed(RestartFailure::Stopped))
    );
    assert!(
        !world
            .calls_naming(&format!("pane close {NEW_PANE}"))
            .is_empty()
    );
    assert!(
        ended(&spawned),
        "the interrupted spawn's harness is stopped"
    );
    assert_eq!(
        restarted(&fixture),
        Response::Restarted(flow_ethos::FlowId(FLOW.into()))
    );
}

/// abc123's metaflow declared with `system_modules`, the launch's own user
/// modules, and `root_option`.
fn declared(fixture: &NexusFixture, system_modules: &[&str], root_option: Option<String>) {
    fixture
        .nexus
        .store
        .configure_change(meta_signal_flow::ConfigurationChange::Metaflows(vec![
            meta_signal_flow::ConfigureMetaflow {
                root_option: root_option.map(meta_signal_flow::Root),
                address: address(Model::Sol),
                system_modules: signal_flow::SystemModules(
                    system_modules
                        .iter()
                        .map(|&module| signal_flow::ModuleIdentity(module.into()))
                        .collect(),
                ),
                user_modules: signal_flow::UserModules(vec![signal_flow::ModuleIdentity(
                    "operation:main-flow:0".into(),
                )]),
                flow_kind: signal_flow::FlowKind::Regular,
                tool_set: signal_flow::ToolSet::Default,
                continuity: meta_signal_flow::Continuity::OnDemand,
                purpose: meta_signal_flow::Purpose(String::new()),
                records: meta_signal_flow::Records(vec![]),
            },
        ]))
        .expect("metaflow declared");
}

/// abc123's launch declared as it was launched: it composes.
fn declared_composable(fixture: &NexusFixture) {
    declared(fixture, &["spirit:core:0", "intent:flow:0"], None);
}

/// abc123's metaflow declared naming a module Curriculum does not hold.
fn declared_uncomposable(fixture: &NexusFixture) {
    declared(fixture, &["vision:absent:0"], None);
}

fn harness_flags(fixture: &NexusFixture, flags: &str) {
    fixture
        .nexus
        .store
        .configure_change(meta_signal_flow::ConfigurationChange::HarnessFlags(vec![
            meta_signal_flow::ConfigureHarnessFlags {
                address: address(Model::Sol),
                harness_flag_text: meta_signal_flow::HarnessFlagText(flags.into()),
            },
        ]))
        .expect("harness flags configured");
}

fn resume_refused(category: &str) -> Response {
    Response::RestartRefused(RestartRefusal::ResumeRefused(ResumeRefused_Data {
        flow_id: flow_ethos::FlowId(FLOW.into()),
        string: category.into(),
    }))
}

/// The old seat exactly as it was before the refused restart: its harness
/// `old` running, abc123 Active and Awake in w1:p3, the restart record
/// `record`, and every Herdr call a read, so no pane was closed, opened or
/// typed into.
fn old_seat_untouched(
    world: &RestartWorld,
    fixture: &NexusFixture,
    old: &Process,
    kept: Option<RestartRecord>,
) {
    assert!(FlowStore::process_is_live(old), "the old harness runs on");
    assert_eq!(route_pane(fixture), OLD_PANE);
    assert_eq!(node(fixture).session_id.0, world.seat.session);
    assert_eq!(node(fixture).flow_lifecycle, FlowLifecycle::Active);
    assert_eq!(
        stored(fixture, Model::Sol).state,
        State::Awake(flow_ethos::FlowId(FLOW.into()))
    );
    assert_eq!(record(fixture), kept);
    assert!(
        world
            .fresh
            .calls()
            .lines()
            .all(|line| line.contains("api snapshot")
                || line.contains("process-info")
                || line.contains("session list")),
        "only reads reach Herdr: {}",
        world.fresh.calls()
    );
    assert!(world.fresh.user_turns(fixture).is_empty(), "nothing typed");
}

/// The resume's composition is refused (its declaration names a module
/// Curriculum does not hold): ResumeRefused NotComposed, answered before
/// anything is closed or written. No Closing, no Failed; the old harness,
/// its pane and its process as they were, nothing typed.
#[test]
fn restart_composition_refused_old_harness_untouched() {
    let mut fixture = NexusFixture::new();
    let world = fixture.restart_world(true);
    let old = world.old_harness();
    declared_uncomposable(&fixture);

    assert_eq!(restarted(&fixture), resume_refused("NotComposed"));
    old_seat_untouched(&world, &fixture, &old, None);
}

/// The declared root is no directory: ResumeRefused RootRefused, before
/// anything is closed or written; the old harness as it was.
#[test]
fn restart_root_refused_old_harness_untouched() {
    let mut fixture = NexusFixture::new();
    let world = fixture.restart_world(true);
    let old = world.old_harness();
    let absent = fixture.directory.path().join("absent-root");
    declared(
        &fixture,
        &["spirit:core:0", "intent:flow:0"],
        Some(absent.to_string_lossy().into_owned()),
    );

    assert_eq!(restarted(&fixture), resume_refused("RootRefused"));
    old_seat_untouched(&world, &fixture, &old, None);
}

/// The configured harness flags do not parse: ResumeRefused
/// MalformedHarnessFlags, before anything is closed or written; the old
/// harness as it was.
#[test]
fn restart_harness_flags_refused_old_harness_untouched() {
    let mut fixture = NexusFixture::new();
    let world = fixture.restart_world(true);
    let old = world.old_harness();
    harness_flags(&fixture, "'unfinished");

    assert_eq!(restarted(&fixture), resume_refused("MalformedHarnessFlags"));
    old_seat_untouched(&world, &fixture, &old, None);
}

/// The resume's command line, built with its title, settings and session
/// as the spawn builds it, is longer than Herdr types: ResumeRefused
/// CommandTooLong, before anything is closed or written; the old harness
/// as it was.
#[test]
fn restart_command_line_refused_old_harness_untouched() {
    let mut fixture = NexusFixture::new();
    let world = fixture.restart_world(true);
    let old = world.old_harness();
    harness_flags(&fixture, &format!("--append {}", "x".repeat(4096)));

    assert_eq!(restarted(&fixture), resume_refused("CommandTooLong"));
    old_seat_untouched(&world, &fixture, &old, None);
}

/// Composed first, a restart still closes the old harness once, before the
/// new pane, then resumes the same session with `--resume`, its hooks aimed
/// at this Nexus's socket and no other.
#[test]
fn restart_precomposed_success_closes_once_resumes_same_session() {
    let mut fixture = NexusFixture::new();
    let world = fixture.restart_world(true);
    let old = world.old_harness();
    declared_composable(&fixture);

    assert_eq!(
        restarted(&fixture),
        Response::Restarted(flow_ethos::FlowId(FLOW.into()))
    );

    let calls = world.fresh.calls();
    let closes: Vec<usize> = calls
        .lines()
        .enumerate()
        .filter(|(_, line)| line.contains(&format!("pane close {OLD_PANE}")))
        .map(|(at, _)| at)
        .collect();
    assert_eq!(closes.len(), 1, "{calls}");
    let create = calls
        .lines()
        .position(|line| line.contains("workspace create"))
        .expect("a new pane");
    assert!(
        closes[0] < create,
        "the old harness closes before the new pane"
    );
    assert!(ended(&old));
    let resumes = world.resumes();
    assert_eq!(resumes.len(), 1, "{calls}");
    let resume = &resumes[0];
    assert!(
        resume.contains(&format!("--resume {}", world.seat.session)),
        "{resume}"
    );
    assert!(!resume.contains("--session-id"), "{resume}");
    let sockets = resume.matches(".sock").count();
    assert!(sockets > 0, "{resume}");
    assert_eq!(
        resume
            .matches("/fixture/run/flow-next/flow/flow.sock")
            .count(),
        sockets,
        "the hooks report to this Nexus only: {resume}"
    );
    assert_eq!(node(&fixture).session_id.0, world.seat.session);
    assert_eq!(route_pane(&fixture), NEW_PANE);
    assert_eq!(record(&fixture), None);
}

/// A restart refused before the close, retried once composition works,
/// restarts as any restart does: one close, one resume of the same session.
#[test]
fn restart_retry_after_composition_refused_restarts() {
    let mut fixture = NexusFixture::new();
    let world = fixture.restart_world(true);
    let old = world.old_harness();
    declared_uncomposable(&fixture);
    assert_eq!(restarted(&fixture), resume_refused("NotComposed"));
    old_seat_untouched(&world, &fixture, &old, None);

    declared_composable(&fixture);
    assert_eq!(
        restarted(&fixture),
        Response::Restarted(flow_ethos::FlowId(FLOW.into()))
    );
    assert_eq!(
        world.calls_naming(&format!("pane close {OLD_PANE}")).len(),
        1
    );
    assert!(ended(&old));
    assert_eq!(world.resumes().len(), 1);
    assert!(world.resumes()[0].contains(&format!("--resume {}", world.seat.session)));
    assert_eq!(route_pane(&fixture), NEW_PANE);
    assert_eq!(record(&fixture), None);
}

/// The Nexus died once Closing was written: its successor composes again,
/// then closes the old pane once and resumes the same session, as before.
#[test]
fn restart_crash_after_closing_reconciles() {
    let mut fixture = NexusFixture::new();
    let world = fixture.restart_world(true);
    let old = world.old_harness();
    let mut left = RestartRecord {
        restart_phase: RestartPhase::Closing,
        ..spawning_record(&world, next_pane("restart-agent"))
    };
    left.old_seat.process_option = Some(old.clone());
    fixture.nexus.store.record_restart(left).unwrap();
    fixture.reopen_store();
    fixture.nexus.recover_wakes();

    assert_eq!(
        world.calls_naming(&format!("pane close {OLD_PANE}")).len(),
        1
    );
    assert!(ended(&old));
    assert_eq!(world.resumes().len(), 1);
    assert!(world.resumes()[0].contains(&format!("--resume {}", world.seat.session)));
    assert_eq!(record(&fixture), None);
    assert_eq!(route_pane(&fixture), NEW_PANE);
}

/// The Nexus died once Closing was written, and its successor cannot
/// compose the resume: the record is Failed NotComposed and the old pane is
/// not touched; a retry once composition works closes it and resumes.
#[test]
fn restart_crash_after_closing_composition_refused_old_harness_untouched() {
    let mut fixture = NexusFixture::new();
    let world = fixture.restart_world(true);
    let old = world.old_harness();
    let mut left = RestartRecord {
        restart_phase: RestartPhase::Closing,
        ..spawning_record(&world, next_pane("restart-agent"))
    };
    left.old_seat.process_option = Some(old.clone());
    fixture.nexus.store.record_restart(left.clone()).unwrap();
    declared_uncomposable(&fixture);
    fixture.reopen_store();
    fixture.nexus.recover_wakes();

    let failed = RestartRecord {
        restart_phase: RestartPhase::Failed(RestartFailure::ResumeRefused("NotComposed".into())),
        ..left
    };
    old_seat_untouched(&world, &fixture, &old, Some(failed));

    declared_composable(&fixture);
    assert_eq!(
        restarted(&fixture),
        Response::Restarted(flow_ethos::FlowId(FLOW.into()))
    );
    assert_eq!(
        world.calls_naming(&format!("pane close {OLD_PANE}")).len(),
        1
    );
    assert!(ended(&old));
    assert_eq!(world.resumes().len(), 1);
    assert_eq!(record(&fixture), None);
}

/// What 0.53.0 left: the old pane closed, its harness exited, then the
/// resume not composed, the record Failed NotComposed. Retried while
/// composition is still refused, the record stays exactly as it was;
/// retried once it composes, the same session is resumed with `--resume`,
/// nothing closed again.
#[test]
fn restart_failed_not_composed_by_0530_retry_resumes_same_session() {
    let mut fixture = NexusFixture::new();
    let world = fixture.restart_world(true);
    let old = world.old_harness();
    assert!(
        std::process::Command::new(&fixture.snapshot_program)
            .args(["--session", "fixture-session", "pane", "close", OLD_PANE])
            .output()
            .expect("fixture Herdr")
            .status
            .success()
    );
    assert!(ended(&old), "0.53.0 closed the old harness");
    let left = RestartRecord {
        restart_phase: RestartPhase::Failed(RestartFailure::ResumeRefused("NotComposed".into())),
        old_seat: OldSeat {
            process_option: Some(old.clone()),
            ..spawning_record(&world, next_pane("restart-agent")).old_seat
        },
        ..spawning_record(&world, next_pane("restart-agent"))
    };
    fixture.nexus.store.record_restart(left.clone()).unwrap();

    declared_uncomposable(&fixture);
    assert_eq!(restarted(&fixture), resume_refused("NotComposed"));
    assert_eq!(record(&fixture), Some(left));
    assert!(world.resumes().is_empty());

    declared_composable(&fixture);
    assert_eq!(
        restarted(&fixture),
        Response::Restarted(flow_ethos::FlowId(FLOW.into()))
    );
    assert_eq!(
        world.calls_naming(&format!("pane close {OLD_PANE}")).len(),
        1,
        "only 0.53.0's close: {}",
        world.fresh.calls()
    );
    let resumes = world.resumes();
    assert_eq!(resumes.len(), 1);
    assert!(
        resumes[0].contains(&format!("--resume {}", world.seat.session)),
        "{}",
        resumes[0]
    );
    assert_eq!(node(&fixture).session_id.0, world.seat.session);
    assert_eq!(route_pane(&fixture), NEW_PANE);
    assert_eq!(record(&fixture), None);
}

// 0.53.1: a restart whose resume fails after the close reopens the same
// session by itself, once, and only once nothing holds the session.

/// Whether a pid the fixture wrote has ended: gone, or a zombie.
fn pid_ended(pid: u32) -> bool {
    match fs::read_to_string(format!("/proc/{pid}/stat")) {
        Err(_) => true,
        Ok(stat) => stat.contains(") Z"),
    }
}

/// Every harness the fixture Herdr resumed, in order.
fn resumed_pids(world: &RestartWorld) -> Vec<u32> {
    fs::read_to_string(world.fresh.state.join("resumed-pids"))
        .unwrap_or_default()
        .lines()
        .map(|pid| pid.parse().expect("a pid"))
        .collect()
}

/// No resume started while an earlier resumed harness, or the old one,
/// still ran.
fn never_two_live(world: &RestartWorld) {
    let overlap = world.fresh.state.join("overlap");
    assert!(
        !overlap.exists(),
        "a resume started while {} still ran",
        fs::read_to_string(&overlap).unwrap_or_default()
    );
}

/// `body` queued for abc123's Address.
fn queued_for_seat(fixture: &NexusFixture, body: &str) {
    let mut queued = stored(fixture, Model::Sol);
    queued.queue = flow_ethos::Queue(vec![signal_flow::QueueEntry {
        logical_delivery_id: flow_ethos::LogicalDeliveryId(body.into()),
        request_id: flow_ethos::RequestId("test".into()),
        sent_by: signal_flow::SentBy::Unknown,
        delivery: flow_ethos::Delivery::Wake,
        message: signal_flow::Message::Soft(signal_flow::Request::Order(body.into())),
    }]);
    fixture.nexus.store.seed_metaflow(queued);
}

/// The pane a resume that failed past the close ran in, discarded, as its
/// Failed record names it.
const FAILED_PANE: &str = "w6:p1";

/// The failed pane, and `process_option`, the harness Flow saw there.
fn failed_seat(process_option: Option<Process>) -> OldSeat {
    OldSeat {
        herdr_route: HerdrRoute {
            herdr_session_name: signal_flow::HerdrSessionName("fixture-session".into()),
            herdr_agent_name: signal_flow::HerdrAgentName("restart-agent".into()),
            herdr_pane_id: signal_flow::HerdrPaneId(FAILED_PANE.into()),
            herdr_terminal_id: signal_flow::HerdrTerminalId("term-failed".into()),
        },
        process_option,
    }
}

/// A process the kernel knew, by its birth, since ended and reaped.
fn exited_process() -> Process {
    let mut child = std::process::Command::new("sleep")
        .arg("60")
        .spawn()
        .expect("a process");
    let process = FlowStore::process_identity_for_pid(child.id()).expect("its birth");
    child.kill().unwrap();
    child.wait().unwrap();
    process
}

/// What a Nexus leaves when its resume failed after the close at run
/// `attempts`, its pane discarded: the old seat that pane, with
/// `process_option`, the harness Flow saw there.
fn failed_at(
    world: &RestartWorld,
    attempts: i64,
    process_option: Option<Process>,
) -> RestartRecord {
    RestartRecord {
        restart_phase: RestartPhase::Failed(RestartFailure::ResumeRefused("SpawnRefused".into())),
        attempts: meta_signal_flow::Attempts(attempts),
        old_seat: failed_seat(process_option),
        ..spawning_record(world, next_pane("restart-agent"))
    }
}

/// What a Nexus leaves when its resume failed after the close, its pane
/// discarded and the harness Flow saw there exited, at run `attempts`.
fn failed_after_close(world: &RestartWorld, attempts: i64) -> RestartRecord {
    failed_at(world, attempts, Some(exited_process()))
}

/// The restart ran twice: `claude --resume <the same session>` in w7:p1,
/// then in w8:p1, made only once w7:p1 was discarded, no harness started
/// while another ran; abc123 bound in w8:p1 to the second harness, its
/// session kept and its lock gone.
fn reopened_once(world: &RestartWorld, fixture: &NexusFixture) {
    let resumes = world.resumes();
    assert_eq!(resumes.len(), 2, "{}", world.fresh.calls());
    for (resume, pane) in resumes.iter().zip([NEW_PANE, REOPENED_PANE]) {
        assert!(
            resume.contains(&format!("--resume {}", world.seat.session)),
            "{resume}"
        );
        assert!(!resume.contains("--session-id"), "{resume}");
        assert!(resume.contains(&format!("--pane {pane}")), "{resume}");
    }
    let calls = world.fresh.calls();
    let at = |fragment: &str| -> Vec<usize> {
        calls
            .lines()
            .enumerate()
            .filter(|(_, line)| line.contains(fragment))
            .map(|(at, _)| at)
            .collect()
    };
    let creates = at("workspace create");
    let discarded = at(&format!("pane close {NEW_PANE}"));
    assert_eq!(creates.len(), 2, "{calls}");
    assert_eq!(discarded.len(), 1, "{calls}");
    assert!(
        creates[0] < discarded[0] && discarded[0] < creates[1],
        "the failed pane is gone before the next is made: {calls}"
    );
    never_two_live(world);
    assert_eq!(record(fixture), None);
    assert_eq!(route_pane(fixture), REOPENED_PANE);
    assert_eq!(node(fixture).session_id.0, world.seat.session);
    assert_eq!(node(fixture).flow_lifecycle, FlowLifecycle::Active);
    let birth = fixture
        .nexus
        .store
        .seat_birth(FLOW)
        .unwrap()
        .expect("birth");
    assert_eq!(
        Some(birth.process),
        FlowStore::process_identity_for_pid(pid_in(&world.fresh.state.join("pid")))
    );
}

/// The first resume is refused after the old harness is closed, its
/// harness seen running in the pane: the restart discards its pane, that
/// harness proven exited by its birth, and reopens the same session by
/// itself with the launch it composed, answering Restarted. One start per
/// run, two in all, never two harnesses at once; the old pane closed once.
#[test]
fn restart_spawn_refused_once_reopens_same_session() {
    let mut fixture = NexusFixture::new();
    let world = fixture.restart_world(true);
    let old = world.old_harness();
    queued_for_seat(&fixture, "handed over after the reopen");
    fs::write(world.fresh.state.join("refused-after-start-once"), "").unwrap();

    assert_eq!(
        restarted(&fixture),
        Response::Restarted(flow_ethos::FlowId(FLOW.into()))
    );

    reopened_once(&world, &fixture);
    assert_eq!(
        world.calls_naming(&format!("pane close {OLD_PANE}")).len(),
        1
    );
    assert!(ended(&old));
    let pids = resumed_pids(&world);
    assert_eq!(pids.len(), 2, "one harness per run");
    assert!(pid_ended(pids[0]), "the refused start's harness exited");
    assert_eq!(
        received(&world, &fixture, "handed over after the reopen"),
        1
    );
    fixture.nexus.drain_awake(&address(Model::Sol));
    assert_eq!(
        received(&world, &fixture, "handed over after the reopen"),
        1
    );
}

/// The first resumed pane reports another session: its harness is
/// discarded and seen exited, then the restart reopens the same session by
/// itself and binds it. No new identity is minted.
#[test]
fn restart_bind_mismatch_once_reopens_same_session() {
    let mut fixture = NexusFixture::new();
    let world = fixture.restart_world(true);
    let flows_before = fixture.nexus.store.flow_nodes().unwrap().len();
    let claims_before =
        fs::read_to_string(fixture.directory.path().join("flow-id-calls")).unwrap_or_default();
    fs::write(
        world.fresh.state.join("report-session-once"),
        "f0f0f0f0-0000-4000-8000-00000000f0f0",
    )
    .unwrap();

    assert_eq!(
        restarted(&fixture),
        Response::Restarted(flow_ethos::FlowId(FLOW.into()))
    );

    reopened_once(&world, &fixture);
    let pids = resumed_pids(&world);
    assert_eq!(pids.len(), 2, "one harness per run");
    assert!(pid_ended(pids[0]), "the mismatched harness exited");
    assert!(!pid_ended(pids[1]), "the bound harness runs");
    assert_eq!(
        fixture.nexus.store.flow_nodes().unwrap().len(),
        flows_before
    );
    assert_eq!(
        fs::read_to_string(fixture.directory.path().join("flow-id-calls")).unwrap_or_default(),
        claims_before
    );
}

/// Every resume is refused once its harness runs: the restart's own run
/// and the one reopen the bound gives it, two starts, each harness proven
/// exited, then Failed with its runs counted, the session, the Address and
/// the queue kept and nothing delivered. A Nexus started again starts
/// nothing more. The owner's retry resumes the same session and hands it
/// the queue once.
#[test]
fn restart_failure_every_time_spends_bound_custody_kept() {
    let mut fixture = NexusFixture::new();
    let world = fixture.restart_world(true);
    queued_for_seat(&fixture, "kept through the bound");
    fs::write(world.fresh.state.join("refused-after-start"), "").unwrap();

    assert_eq!(restarted(&fixture), resume_refused("SpawnRefused"));
    let failed = record(&fixture).expect("record");
    assert_eq!(
        failed.restart_phase,
        RestartPhase::Failed(RestartFailure::ResumeRefused("SpawnRefused".into()))
    );
    assert_eq!(
        failed.attempts,
        meta_signal_flow::Attempts(2),
        "the restart's run and one reopen"
    );
    assert_eq!(world.resumes().len(), 2, "one start per run");
    for pane in [NEW_PANE, REOPENED_PANE] {
        assert_eq!(world.calls_naming(&format!("pane close {pane}")).len(), 1);
    }
    assert_eq!(node(&fixture).session_id.0, world.seat.session);
    assert_eq!(node(&fixture).flow_lifecycle, FlowLifecycle::Active);
    assert_eq!(stored(&fixture, Model::Sol).queue.0.len(), 1);
    fixture.nexus.drain_awake(&address(Model::Sol));
    assert_eq!(received(&world, &fixture, "kept through the bound"), 0);

    fixture.reopen_store();
    fixture.nexus.recover_wakes();
    assert_eq!(record(&fixture), Some(failed), "the bound is spent");
    assert_eq!(world.resumes().len(), 2);
    assert_eq!(received(&world, &fixture, "kept through the bound"), 0);

    fs::remove_file(world.fresh.state.join("refused-after-start")).unwrap();
    assert_eq!(
        restarted(&fixture),
        Response::Restarted(flow_ethos::FlowId(FLOW.into()))
    );
    let resumes = world.resumes();
    assert_eq!(resumes.len(), 3);
    assert!(resumes[2].contains(&format!("--resume {}", world.seat.session)));
    assert_eq!(record(&fixture), None);
    assert_eq!(received(&world, &fixture, "kept through the bound"), 1);
    fixture.nexus.drain_awake(&address(Model::Sol));
    assert_eq!(received(&world, &fixture, "kept through the bound"), 1);
    assert!(stored(&fixture, Model::Sol).queue.0.is_empty());
    never_two_live(&world);
}

/// A failed resume whose pane Herdr will not show closed may still have a
/// start in flight there, its harness unobserved: Interrupted, and nothing
/// is started again, by this Nexus or by the next at its start.
#[test]
fn restart_start_in_flight_not_respawned() {
    let mut fixture = NexusFixture::new();
    let world = fixture.restart_world(false);
    born_at_route(&fixture, exited_process());
    fs::write(world.fresh.state.join("close-refused"), "").unwrap();
    fs::write(world.fresh.state.join("resume-refused"), "").unwrap();

    assert_eq!(
        restarted(&fixture),
        Response::RestartRefused(RestartRefusal::Interrupted(flow_ethos::FlowId(FLOW.into())))
    );
    let interrupted = record(&fixture).expect("record");
    assert!(
        matches!(
            &interrupted.restart_phase,
            RestartPhase::Interrupted(Some(pane)) if pane.herdr_pane_id.0 == NEW_PANE
        ),
        "{interrupted:?}"
    );
    assert_eq!(world.resumes().len(), 1);

    fixture.reopen_store();
    fixture.nexus.recover_wakes();
    assert_eq!(record(&fixture), Some(interrupted));
    assert_eq!(world.resumes().len(), 1, "no respawn");
    assert_eq!(world.calls_naming("workspace create").len(), 1);
}

/// A Nexus that died after a resume failed past the close, its pane
/// discarded and the record Failed with the bound unspent, the harness it
/// saw there exited: the next Nexus, the seat proven quiescent, reopens the
/// same session at its start and hands it its queue once; a later start
/// does nothing more. One whose reopen is refused again ends Failed with
/// the bound spent, its old seat the pane that failed, and the start after
/// it starts nothing.
#[test]
fn restart_nexus_crash_after_post_close_failure_recovers() {
    let mut fixture = NexusFixture::new();
    let world = fixture.restart_world(false);
    queued_for_seat(&fixture, "queued before the crash");
    fixture
        .nexus
        .store
        .record_restart(failed_after_close(&world, 1))
        .unwrap();
    fixture.reopen_store();
    fixture.nexus.recover_wakes();

    let resumes = world.resumes();
    assert_eq!(resumes.len(), 1, "{}", world.fresh.calls());
    assert!(resumes[0].contains(&format!("--resume {}", world.seat.session)));
    assert!(resumes[0].contains(&format!("--pane {NEW_PANE}")));
    assert_eq!(record(&fixture), None);
    assert_eq!(route_pane(&fixture), NEW_PANE);
    assert_eq!(node(&fixture).session_id.0, world.seat.session);
    assert_eq!(received(&world, &fixture, "queued before the crash"), 1);
    fixture.reopen_store();
    fixture.nexus.recover_wakes();
    assert_eq!(world.resumes().len(), 1);
    assert_eq!(received(&world, &fixture, "queued before the crash"), 1);

    let mut fixture = NexusFixture::new();
    let world = fixture.restart_world(false);
    fixture
        .nexus
        .store
        .record_restart(failed_after_close(&world, 1))
        .unwrap();
    fs::write(world.fresh.state.join("resume-refused"), "").unwrap();
    fixture.reopen_store();
    fixture.nexus.recover_wakes();
    assert_eq!(world.resumes().len(), 1);
    let spent = record(&fixture).expect("record");
    assert_eq!(
        (spent.restart_phase.clone(), spent.attempts.0),
        (
            RestartPhase::Failed(RestartFailure::ResumeRefused("SpawnRefused".into())),
            2
        )
    );
    assert_eq!(spent.old_seat.herdr_route.herdr_pane_id.0, NEW_PANE);
    assert_eq!(spent.old_seat.process_option, None, "no harness ran there");
    fixture.reopen_store();
    fixture.nexus.recover_wakes();
    assert_eq!(world.resumes().len(), 1, "the bound holds across starts");
    assert_eq!(record(&fixture), Some(spent));
}

/// The owner's Stop while the bound is unspent. A Failed record a dead
/// Nexus left before its reopen is Stopped: the next start reopens nothing,
/// and the owner's retry resumes the same session and hands it its queue
/// once. While a reopen's spawn runs, Stop answers Restarting and changes
/// nothing; the reopen then binds.
#[test]
fn restart_owner_stop_during_bound() {
    use crate::restarting::RestartsFlows;
    let mut fixture = NexusFixture::new();
    let world = fixture.restart_world(false);
    queued_for_seat(&fixture, "kept through the Stop");
    fixture
        .nexus
        .store
        .record_restart(failed_after_close(&world, 1))
        .unwrap();
    assert_eq!(
        fixture
            .nexus
            .dispatch(Query::Stop(flow_ethos::FlowId(FLOW.into()))),
        Response::Stopped(flow_ethos::FlowId(FLOW.into()))
    );
    let stopped = record(&fixture).expect("record");
    assert_eq!(
        (stopped.restart_phase.clone(), stopped.attempts.0),
        (RestartPhase::Failed(RestartFailure::Stopped), 1)
    );
    fixture.reopen_store();
    fixture.nexus.recover_wakes();
    assert_eq!(record(&fixture), Some(stopped), "Stopped is the owner's");
    assert!(world.resumes().is_empty());
    assert_eq!(node(&fixture).flow_lifecycle, FlowLifecycle::Active);
    assert_eq!(stored(&fixture, Model::Sol).queue.0.len(), 1);
    assert_eq!(received(&world, &fixture, "kept through the Stop"), 0);
    assert_eq!(
        restarted(&fixture),
        Response::Restarted(flow_ethos::FlowId(FLOW.into()))
    );
    assert_eq!(world.resumes().len(), 1);
    assert_eq!(record(&fixture), None);
    assert_eq!(received(&world, &fixture, "kept through the Stop"), 1);

    let mut fixture = NexusFixture::new();
    let world = fixture.restart_world(false);
    fixture
        .nexus
        .store
        .record_restart(failed_after_close(&world, 1))
        .unwrap();
    use crate::fixture_executable::{HoldsLatch, Latch};
    let release_start = Latch::closed(world.fresh.state.join("release-start"));
    fs::write(world.fresh.state.join("hold-start"), "").unwrap();
    std::thread::scope(|scope| {
        let recovering = scope.spawn(|| fixture.nexus.recover_restarts());
        await_path(&world.fresh.state.join("start-held"));
        let spawning = record(&fixture).expect("record");
        assert!(
            matches!(spawning.restart_phase, RestartPhase::Spawning(_)),
            "{spawning:?}"
        );
        assert_eq!(spawning.attempts, meta_signal_flow::Attempts(2));
        assert_eq!(
            fixture
                .nexus
                .dispatch(Query::Stop(flow_ethos::FlowId(FLOW.into()))),
            Response::StopRejected(StopRejection::Restarting)
        );
        assert_eq!(record(&fixture), Some(spawning));
        release_start.release();
        recovering.join().unwrap();
    });
    assert_eq!(record(&fixture), None);
    assert_eq!(world.resumes().len(), 1);
    assert_eq!(route_pane(&fixture), NEW_PANE);
}

/// A Failed record from before the close, its old harness still running in
/// its pane: a Nexus start reopens nothing and touches nothing, only reads
/// reaching Herdr. Once the old harness is gone, the next start resumes the
/// same session by itself, closing nothing.
#[test]
fn restart_failed_before_close_taken_up_once_old_harness_gone() {
    let mut fixture = NexusFixture::new();
    let world = fixture.restart_world(true);
    let old = world.old_harness();
    let left = RestartRecord {
        restart_phase: RestartPhase::Failed(RestartFailure::CloseRefused),
        old_seat: OldSeat {
            process_option: Some(old.clone()),
            ..spawning_record(&world, next_pane("restart-agent")).old_seat
        },
        ..spawning_record(&world, next_pane("restart-agent"))
    };
    fixture.nexus.store.record_restart(left.clone()).unwrap();
    fixture.reopen_store();
    fixture.nexus.recover_wakes();
    old_seat_untouched(&world, &fixture, &old, Some(left));

    assert!(
        std::process::Command::new(&fixture.snapshot_program)
            .args(["--session", "fixture-session", "pane", "close", OLD_PANE])
            .output()
            .expect("fixture Herdr")
            .status
            .success()
    );
    assert!(ended(&old));
    fixture.reopen_store();
    fixture.nexus.recover_wakes();
    let resumes = world.resumes();
    assert_eq!(resumes.len(), 1, "{}", world.fresh.calls());
    assert!(resumes[0].contains(&format!("--resume {}", world.seat.session)));
    assert_eq!(
        world.calls_naming(&format!("pane close {OLD_PANE}")).len(),
        1,
        "only the close by hand"
    );
    assert_eq!(record(&fixture), None);
    assert_eq!(route_pane(&fixture), NEW_PANE);
    never_two_live(&world);
}

// 0.53.1: a reopen, by itself or the owner's, needs the failed seat's
// harness recorded and proven exited by its birth: a harness can outlive
// its pane.

/// A process that runs until the test drops it.
struct LiveProcess(std::process::Child, Process);

impl Drop for LiveProcess {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn live_process() -> LiveProcess {
    let child = std::process::Command::new("sleep")
        .arg("60")
        .spawn()
        .expect("a process");
    let process = FlowStore::process_identity_for_pid(child.id()).expect("its birth");
    LiveProcess(child, process)
}

/// Ends a process the fixture Herdr started and left running, and waits,
/// bounded, until the kernel shows it ended.
fn killed(process: &Process) {
    assert!(
        std::process::Command::new("kill")
            .arg(process.pid.0.to_string())
            .status()
            .expect("kill")
            .success()
    );
    // Its exit is awaited as its pidfd; a pid already gone has ended.
    use crate::event_wait::{AwaitsDescriptors, OpensExitDescriptor};
    use std::os::fd::AsFd;
    if !ended(process)
        && let Ok(pid) = u32::try_from(process.pid.0)
        && let Ok(exit) = pid.exit_descriptor()
    {
        let _ = [exit.as_fd()].ready(Some(std::time::Instant::now() + Duration::from_secs(10)));
    }
    assert!(ended(process), "the orphan never ended");
}

/// abc123's restart whose first resumed pane reports another session and
/// whose discard takes the pane down but leaves its harness running: the
/// answer the restart gave, and that orphan, by its birth.
fn orphaned(world: &RestartWorld, fixture: &NexusFixture) -> (Response, Process) {
    queued_for_seat(fixture, "held until a reopen is proven");
    fs::write(
        world.fresh.state.join("report-session-once"),
        "f0f0f0f0-0000-4000-8000-00000000f0f0",
    )
    .unwrap();
    fs::write(world.fresh.state.join("orphan-on-close"), "").unwrap();
    let answer = restarted(fixture);
    fs::remove_file(world.fresh.state.join("orphan-on-close")).unwrap();
    let pids = resumed_pids(world);
    assert_eq!(pids.len(), 1, "one harness, the failed run's");
    let orphan = FlowStore::process_identity_for_pid(pids[0]).expect("the orphan runs");
    (answer, orphan)
}

/// The Failed record the orphaned run wrote: its old seat the discarded
/// pane w7:p1 and the orphan's birth, its run counted once.
fn orphan_record(world: &RestartWorld, fixture: &NexusFixture, orphan: &Process) -> RestartRecord {
    let failed = record(fixture).expect("record");
    assert_eq!(
        (failed.restart_phase.clone(), failed.attempts.0),
        (RestartPhase::Failed(RestartFailure::BindMismatch), 1)
    );
    assert_eq!(failed.native_session_id.0, world.seat.session);
    assert_eq!(failed.old_seat.herdr_route.herdr_pane_id.0, NEW_PANE);
    assert_eq!(
        failed.old_seat.herdr_route.herdr_terminal_id,
        signal_flow::HerdrTerminalId("term-fresh".into())
    );
    assert_eq!(failed.old_seat.process_option.as_ref(), Some(orphan));
    failed
}

/// Nothing reopened and nothing delivered: one resume, one pane made, the
/// session, the Address and the queue held.
fn not_reopened(world: &RestartWorld, fixture: &NexusFixture) {
    assert_eq!(world.resumes().len(), 1, "{}", world.fresh.calls());
    assert_eq!(world.calls_naming("workspace create").len(), 1);
    assert_eq!(node(fixture).session_id.0, world.seat.session);
    assert_eq!(node(fixture).flow_lifecycle, FlowLifecycle::Active);
    assert_eq!(stored(fixture, Model::Sol).queue.0.len(), 1);
    assert_eq!(received(world, fixture, "held until a reopen is proven"), 0);
}

/// The failed pane is gone but its harness runs on, holding the session:
/// the restart does not reopen by itself, its record Failed and naming that
/// harness by its birth, and the owner's retry is refused ExitUnproven,
/// the record as it was.
#[test]
fn restart_live_orphan_not_reopened_in_turn() {
    let mut fixture = NexusFixture::new();
    let world = fixture.restart_world(true);
    let (answer, orphan) = orphaned(&world, &fixture);

    assert_eq!(answer, resume_refused("BindMismatch"));
    let failed = orphan_record(&world, &fixture, &orphan);
    assert!(FlowStore::process_is_live(&orphan), "the orphan runs on");
    assert!(world.calls_naming(&format!("pane close {NEW_PANE}")).len() == 1);
    not_reopened(&world, &fixture);

    assert_eq!(restarted(&fixture), resume_refused("ExitUnproven"));
    assert_eq!(record(&fixture), Some(failed));
    not_reopened(&world, &fixture);
    killed(&orphan);
}

/// The Nexus that left the orphaned record dies: the next one reads the
/// orphan's birth from the record, finds it running, and reopens nothing,
/// though no pane holds the session; the start after that alike.
#[test]
fn restart_live_orphan_not_reopened_at_start() {
    let mut fixture = NexusFixture::new();
    let world = fixture.restart_world(true);
    let (_, orphan) = orphaned(&world, &fixture);
    let failed = orphan_record(&world, &fixture, &orphan);

    for _ in 0..2 {
        fixture.reopen_store();
        fixture.nexus.recover_wakes();
        assert_eq!(record(&fixture), Some(failed.clone()));
        assert!(FlowStore::process_is_live(&orphan), "the orphan runs on");
        not_reopened(&world, &fixture);
    }
    killed(&orphan);
}

/// The resume is refused before any harness is seen in its pane: nothing
/// proves no harness holds the session, so the restart stays Failed, its
/// old seat the discarded pane and no harness, at its start and after a
/// Nexus start alike. The owner's retry is refused ExitUnwitnessed and
/// changes nothing; the owner's Stop stands, and a retry after it is
/// refused alike. The session, Address and queue are held throughout.
#[test]
fn restart_missing_witness_not_reopened_retry_refused() {
    let mut fixture = NexusFixture::new();
    let world = fixture.restart_world(true);
    queued_for_seat(&fixture, "held until a reopen is proven");
    fs::write(world.fresh.state.join("resume-refused-once"), "").unwrap();

    assert_eq!(restarted(&fixture), resume_refused("SpawnRefused"));
    let failed = record(&fixture).expect("record");
    assert_eq!(
        (failed.restart_phase.clone(), failed.attempts.0),
        (
            RestartPhase::Failed(RestartFailure::ResumeRefused("SpawnRefused".into())),
            1
        )
    );
    assert_eq!(failed.old_seat.herdr_route.herdr_pane_id.0, NEW_PANE);
    assert_eq!(failed.old_seat.process_option, None);
    assert!(resumed_pids(&world).is_empty(), "no harness was seen");
    not_reopened(&world, &fixture);

    fixture.reopen_store();
    fixture.nexus.recover_wakes();
    assert_eq!(record(&fixture), Some(failed.clone()));
    not_reopened(&world, &fixture);

    assert_eq!(restarted(&fixture), resume_refused("ExitUnwitnessed"));
    assert_eq!(record(&fixture), Some(failed.clone()));
    not_reopened(&world, &fixture);

    assert_eq!(
        fixture
            .nexus
            .dispatch(Query::Stop(flow_ethos::FlowId(FLOW.into()))),
        Response::Stopped(flow_ethos::FlowId(FLOW.into()))
    );
    let stopped = record(&fixture).expect("record");
    assert_eq!(
        stopped,
        RestartRecord {
            restart_phase: RestartPhase::Failed(RestartFailure::Stopped),
            ..failed
        }
    );
    assert_eq!(restarted(&fixture), resume_refused("ExitUnwitnessed"));
    assert_eq!(record(&fixture), Some(stopped));
    not_reopened(&world, &fixture);
}

/// The orphan of the failed pane exits: the next Nexus start proves it
/// exited by the birth the record holds and reopens the same session by
/// itself, `claude --resume <session>` in the next pane, never beside the
/// orphan; the flow bound there, its queue handed over once.
#[test]
fn restart_recorded_harness_exited_reopens_same_session() {
    let mut fixture = NexusFixture::new();
    let world = fixture.restart_world(true);
    let (_, orphan) = orphaned(&world, &fixture);
    orphan_record(&world, &fixture, &orphan);
    killed(&orphan);

    fixture.reopen_store();
    fixture.nexus.recover_wakes();
    let resumes = world.resumes();
    assert_eq!(resumes.len(), 2, "{}", world.fresh.calls());
    assert!(
        resumes[1].contains(&format!("--resume {}", world.seat.session)),
        "{}",
        resumes[1]
    );
    assert!(!resumes[1].contains("--session-id"), "{}", resumes[1]);
    assert!(
        resumes[1].contains(&format!("--pane {REOPENED_PANE}")),
        "{}",
        resumes[1]
    );
    never_two_live(&world);
    assert_eq!(record(&fixture), None);
    assert_eq!(route_pane(&fixture), REOPENED_PANE);
    assert_eq!(node(&fixture).session_id.0, world.seat.session);
    assert_eq!(
        fixture
            .nexus
            .store
            .seat_birth(FLOW)
            .unwrap()
            .map(|birth| birth.process),
        FlowStore::process_identity_for_pid(pid_in(&world.fresh.state.join("pid")))
    );
    assert_eq!(
        received(&world, &fixture, "held until a reopen is proven"),
        1
    );
}

/// The recorded harness's pid names a running process. Born when the
/// record says, it is the harness, running: nothing is reopened and only
/// reads reach Herdr. Born at another time, the recorded harness has
/// exited and its pid was taken again: the restart reopens the same
/// session.
#[test]
fn restart_pid_reused_exited_by_birth_only() {
    for reused in [false, true] {
        let mut fixture = NexusFixture::new();
        let world = fixture.restart_world(false);
        let running = live_process();
        let recorded = Process {
            pid: running.1.pid,
            started: flow_ethos::Started(running.1.started.0 + i64::from(reused)),
        };
        let left = failed_at(&world, 1, Some(recorded));
        fixture.nexus.store.record_restart(left.clone()).unwrap();
        fixture.reopen_store();
        fixture.nexus.recover_wakes();

        assert!(FlowStore::process_is_live(&running.1));
        if reused {
            let resumes = world.resumes();
            assert_eq!(resumes.len(), 1, "{}", world.fresh.calls());
            assert!(resumes[0].contains(&format!("--resume {}", world.seat.session)));
            assert_eq!(record(&fixture), None);
            assert_eq!(route_pane(&fixture), NEW_PANE);
        } else {
            assert_eq!(record(&fixture), Some(left));
            assert!(world.resumes().is_empty());
            assert!(
                world
                    .fresh
                    .calls()
                    .lines()
                    .all(|line| line.contains("api snapshot")
                        || line.contains("process-info")
                        || line.contains("session list")),
                "only reads reach Herdr: {}",
                world.fresh.calls()
            );
        }
    }
}

/// At its start a Nexus decides a Failed record by the harness the record
/// names, nothing else: the failed pane gone and no pane holding the
/// session in each case, a record naming no harness and one naming a
/// running harness stay as they are, nothing made or started; one naming a
/// harness that exited reopens the same session.
#[test]
fn restart_startup_recovery_uses_recorded_identity() {
    for named in ["none", "running", "exited"] {
        let mut fixture = NexusFixture::new();
        let world = fixture.restart_world(false);
        let running = live_process();
        let process_option = match named {
            "none" => None,
            "running" => Some(running.1.clone()),
            _ => Some(exited_process()),
        };
        let left = failed_at(&world, 1, process_option);
        fixture.nexus.store.record_restart(left.clone()).unwrap();
        fixture.reopen_store();
        assert_eq!(record(&fixture), Some(left.clone()), "durable as written");
        fixture.nexus.recover_wakes();

        if named == "exited" {
            assert_eq!(world.resumes().len(), 1, "{}", world.fresh.calls());
            assert_eq!(record(&fixture), None);
        } else {
            assert_eq!(record(&fixture), Some(left), "{named}");
            assert!(world.calls_naming("workspace create").is_empty(), "{named}");
            assert!(world.calls_naming("agent start").is_empty(), "{named}");
        }
    }
}

/// Where the old Nexus's hooks report: the place an adopted seat's hooks
/// were written for, the fixture Herdr's default.
const OLD_SOCKET: &str = "/fixture/run/flow-next/flow/flow.sock";
/// Where the new Nexus serves its hooks, as its store configures it.
const NEW_SOCKET: &str = "/fixture/run/flow-0530/flow/flow.sock";

/// A second Nexus beside the fixture's: its own store at `store` in the
/// fixture directory, its own hook socket, and the one fixture Herdr, its
/// flows root and its native transcripts shared with the first.
trait OpensSecondNexus {
    fn second_nexus(&self, store: &str, ordinary_socket: &str) -> crate::RunningNexus;
}

impl OpensSecondNexus for NexusFixture {
    fn second_nexus(&self, store: &str, ordinary_socket: &str) -> crate::RunningNexus {
        use crate::composition::OpensLaunchComposer;
        use crate::herdr::ConfiguresHerdrCli;
        use crate::store::{
            ConfiguresFlowStore, ManagesMetaflowState, OpensFlowStore, SeedsConfiguration,
            SetsNativeOwnerEnvironmentForTests,
        };
        let root = self.directory.path();
        let mut opened = FlowStore::open(&root.join(store)).expect("second store");
        let mut runtime = opened.runtime_configuration().expect("runtime");
        runtime.source_root = flow_ethos::SourceRoot(root.to_string_lossy().into_owned());
        opened.configure_runtime(runtime).expect("source root");
        opened.set_native_owner_environment_for_tests(crate::store::NativeOwnerEnvironment {
            host_id: "isolated-test-host".into(),
            boot_id: "isolated-test-boot".into(),
        });
        let settings = crate::store::DefaultConfiguration {
            home: root.join("home"),
            runtime_directory: root.join("runtime"),
        }
        .configuration();
        opened
            .configure_change(meta_signal_flow::ConfigurationChange::Nexus(
                meta_signal_flow::ConfigureNexus {
                    source_root: flow_ethos::SourceRoot(root.to_string_lossy().into_owned()),
                    first_codex_endpoint: settings.stable_codex,
                    second_codex_endpoint: settings.next_codex,
                    harness_profiles: flow_ethos::HarnessProfiles(settings.harness_profile_vector),
                    meta_aspects: flow_ethos::MetaAspects(settings.meta_aspects),
                    message_nexus_path_option: Some(flow_ethos::MessageNexusPath(
                        settings.message_nexus_path.clone(),
                    )),
                    message_nexus_binary_option: Some(flow_ethos::MessageNexusBinary(
                        "/fixture/message-nexus".into(),
                    )),
                    curriculum_nexus_path: meta_signal_flow::CurriculumNexusPath(
                        "/fixture/curriculum.sock".into(),
                    ),
                    lease: flow_ethos::Lease(30),
                    presentation_prompt_option: None,
                    herdr_session_name: signal_flow::HerdrSessionName("fixture-session".into()),
                },
            ))
            .expect("Nexus configured");
        opened
            .configure_change(meta_signal_flow::ConfigurationChange::Models(vec![
                meta_signal_flow::ConfigureModel {
                    model: Model::Sol,
                    native: meta_signal_flow::Native("claude-fixture".into()),
                },
            ]))
            .expect("model configured");
        opened
            .configure_change(meta_signal_flow::ConfigurationChange::Metaflows(vec![
                meta_signal_flow::ConfigureMetaflow {
                    root_option: None,
                    address: address(Model::Sol),
                    system_modules: signal_flow::SystemModules(vec![
                        signal_flow::ModuleIdentity("spirit:core:0".into()),
                        signal_flow::ModuleIdentity("intent:flow:0".into()),
                    ]),
                    user_modules: signal_flow::UserModules(vec![signal_flow::ModuleIdentity(
                        "operation:main-flow:0".into(),
                    )]),
                    flow_kind: signal_flow::FlowKind::Regular,
                    tool_set: signal_flow::ToolSet::Default,
                    continuity: meta_signal_flow::Continuity::Maintained,
                    purpose: meta_signal_flow::Purpose(String::new()),
                    records: meta_signal_flow::Records(vec![]),
                },
            ]))
            .expect("metaflow declared");
        crate::RunningNexus {
            store: opened.into(),
            codex_endpoints: crate::live::Live::holding(
                (*self.nexus.codex_endpoints.current()).clone(),
            ),
            herdr: crate::herdr::HerdrCli::at(self.snapshot_program.clone(), root.join("flows"))
                .with_codex_endpoints((*self.nexus.codex_endpoints.current()).clone())
                .with_ordinary_socket(Path::new(ordinary_socket)),
            composer: crate::live::Live::holding(crate::composition::LaunchComposer::with_context(
                crate::context::ContextComposer(std::sync::Arc::new(
                    crate::context::FixtureComposer::from(
                        &[
                            ("spirit:core:0", "Spirit body."),
                            ("intent:flow:0", "Intent body."),
                            ("operation:main-flow:0", "Main flow body."),
                            ("knowledge:flow:0", "Knowledge body."),
                        ][..],
                    ),
                )),
            )),
            dispatch_gate: std::sync::Mutex::new(()),
            pane_leases: crate::delivery::lease::PaneLeases::default(),
            waking: crate::waking::WakingMetaflows::default(),
            stopped_at_confirmation: crate::waking::StoppedAtConfirmation::default(),
            refresh_refusals: crate::waking::RefreshRefusals::default(),
            pane_watch_rearms: Default::default(),
            pane_watch_hooks: Default::default(),
            stopping: crate::Stopping::default(),
            launch_calls: crate::send_tool::LaunchCalls::default(),
            codex_turns: crate::codex_turns::CodexTurns::default(),
            turn_ends: crate::turn_ends::TurnEndSubscribers::default(),
            compactions: crate::compacting::CompactionWatch::default(),
            seat_submissions: std::sync::Arc::default(),
            configuration_changes: Default::default(),
        }
    }
}

/// `flow-id claude` as it claims a session already claimed: the claim
/// whose identity is the session's answers its alias; a session never
/// claimed gets a new claim, its alias the identity's first six.
const IDEMPOTENT_FLOW_ID: &str = r#"#!/bin/sh
[ "$1" = claude ] && [ "$2" = --flows-root ] && [ "$4" = --parent-session ] || exit 2
identity=$(printf '%s' "$5" | tr -d -)
for claim in "$3"/.*.flow-id; do
  if grep -qx "identity=$identity" "$claim" 2>/dev/null; then sed -n 's/^alias=//p' "$claim"; exit 0; fi
done
alias=$(printf '%s' "$identity" | cut -c1-6)
: > "$3/.$alias.flow-id.lock"
mkdir -p "$3/$alias"
printf 'version=1\nharness=claude\nidentity=%s\nalias=%s\nuuid-version=uuid-v5\n' "$identity" "$alias" > "$3/.$alias.flow-id"
printf '%s\n' "$alias"
"#;

/// A report served by `nexus`'s ordinary socket naming the harness
/// `birth`: recorded, then acted on only when `nexus` attributes it.
fn reported_to(nexus: &crate::RunningNexus, event: Event, birth: Option<Process>) -> bool {
    let acts = nexus.report_acts(FLOW, birth.as_ref());
    assert_eq!(
        nexus.report(Report_Data {
            flow_id: flow_ethos::FlowId(FLOW.into()),
            process_option: birth,
            event: event.clone(),
        }),
        Response::Reported
    );
    if acts {
        nexus.after_attributed_report(&flow_ethos::FlowId(FLOW.to_owned().to_owned()), &event);
    }
    acts
}

/// The cutover, with both Nexuses live. The old Nexus (the fixture's, its
/// hooks at OLD_SOCKET) launched abc123 in w1:p3, Awake and Maintained. A
/// new Nexus, its own store and its hooks at NEW_SOCKET, Adopts that running
/// seat from the seat's own process and then Restarts it. The old Nexus
/// then gets everything the old seat and its own automatic behavior give
/// it, holding a message queued for abc123: the old hook's late Stop and
/// SessionEnd, which it acts on, recording the exit and owing its
/// Maintained metaflow a successor; an Observe's Gone of w1:p3; a further
/// maintained wake and drain. Every wake of the old side is refused while
/// the resumed session runs, so it launches nothing and keeps what it
/// holds; its Herdr calls are reads only. The new Nexus gets the old
/// hook's reports and an Observe's Gone of the adopted seat; none acts.
/// Through all of it the new custody stands: abc123 Active in w7:p1 on the
/// same session and claim, Awake, its seat birth unchanged, no restart,
/// exit or successor recorded; and the resumed seat's hooks, aimed at
/// NEW_SOCKET only, act on the new Nexus.
#[test]
fn restart_cutover_two_nexus() {
    use crate::sending::BindsSeats;
    use crate::store::metaflow::SleepsMetaflows;
    let mut fixture = NexusFixture::new();
    let world = fixture.restart_world(true);
    maintain(&fixture, address(Model::Sol));
    FixtureExecutable {
        path: fixture.directory.path().join("native-transcripts/flow-id"),
    }
    .install(IDEMPOTENT_FLOW_ID);
    let claim = fixture.directory.path().join("flows/.abc123.flow-id");
    let claimed = fs::read(&claim).unwrap();
    let old_hook = world.old_hook();
    let old_witness = fixture.nexus.seat_witness(FLOW).expect("old witness");
    let new = fixture.second_nexus("flow-0530.sema", NEW_SOCKET);
    let new_node =
        |nexus: &crate::RunningNexus| nexus.store.flow_node(FLOW).unwrap().expect("abc123 held");

    // The new Nexus adopts the running seat from the seat's own process.
    assert_eq!(
        new.adopt(signal_flow::Adopt_Data {
            address: address(Model::Sol),
            seat_pane: old_seat_pane(),
            native_session_id: signal_flow::NativeSessionId(world.seat.session.clone()),
        }),
        Response::Adopted(flow_ethos::FlowId(FLOW.into()))
    );
    assert_eq!(new_node(&new).session_id.0, world.seat.session);
    assert_eq!(
        new.store
            .metaflow(&address(Model::Sol))
            .unwrap()
            .unwrap()
            .state,
        State::Awake(flow_ethos::FlowId(FLOW.into()))
    );
    let adopted_witness = new.seat_witness(FLOW).expect("adopted witness");
    assert_eq!(adopted_witness.route.herdr_pane_id.0, OLD_PANE);

    // And restarts it: the same session resumed in w7:p1, its hooks aimed
    // at the new Nexus alone.
    assert_eq!(
        new.dispatch(Query::Restart(flow_ethos::FlowId(FLOW.into()))),
        Response::Restarted(flow_ethos::FlowId(FLOW.into()))
    );
    let resumes = world.resumes();
    assert_eq!(resumes.len(), 1, "{}", world.fresh.calls());
    assert!(
        resumes[0].contains(&format!("--resume {}", world.seat.session)),
        "{}",
        resumes[0]
    );
    assert!(resumes[0].contains(&format!("--pane {NEW_PANE}")));
    assert!(resumes[0].contains(NEW_SOCKET), "{}", resumes[0]);
    assert!(!resumes[0].contains(OLD_SOCKET), "{}", resumes[0]);
    let bound = new_node(&new);
    assert_eq!(bound.session_id.0, world.seat.session);
    assert_eq!(bound.flow_lifecycle, FlowLifecycle::Active);
    let birth = new.store.seat_birth(FLOW).unwrap().expect("birth");
    assert_eq!(birth.herdr_route.herdr_pane_id.0, NEW_PANE);
    let new_hook = world.new_hook();
    let resumed =
        FlowStore::process_identity_for_pid(pid_in(&world.fresh.state.join("pid"))).unwrap();
    assert_eq!(birth.process, resumed);

    // The resumed seat's hook reaches the new Nexus and acts: its Stop
    // hands the new seat what was queued for it there.
    let mut queued = new.store.metaflow(&address(Model::Sol)).unwrap().unwrap();
    queued.queue = flow_ethos::Queue(vec![signal_flow::QueueEntry {
        logical_delivery_id: flow_ethos::LogicalDeliveryId("cutover-queued".into()),
        request_id: flow_ethos::RequestId("test".into()),
        sent_by: signal_flow::SentBy::Unknown,
        delivery: flow_ethos::Delivery::Wake,
        message: signal_flow::Message::Soft(signal_flow::Request::Order(
            "for the resumed seat".into(),
        )),
    }]);
    new.store.seed_metaflow(queued);
    assert!(reported_to(
        &new,
        stop_of(&world, &fixture),
        Some(new_hook.clone())
    ));
    assert_eq!(received(&world, &fixture, "for the resumed seat"), 1);

    // A message the old Nexus holds for abc123, queued there during the
    // cutover.
    let mut held = stored(&fixture, Model::Sol);
    held.queue = flow_ethos::Queue(vec![signal_flow::QueueEntry {
        logical_delivery_id: flow_ethos::LogicalDeliveryId("held-by-the-old-nexus".into()),
        request_id: flow_ethos::RequestId("test".into()),
        sent_by: signal_flow::SentBy::Unknown,
        delivery: flow_ethos::Delivery::Wake,
        message: signal_flow::Message::Soft(signal_flow::Request::Order(
            "queued at the old Nexus".into(),
        )),
    }]);
    fixture.nexus.store.seed_metaflow(held);

    // From here every Herdr call is the old side's, or the new Nexus
    // reading.
    let before_old = world.fresh.calls().lines().count();

    // The old hook's late reports reach the old Nexus, which holds abc123
    // in w1:p3 with no restart of its own and acts on them: the Stop, then
    // the SessionEnd, which records the exit and wakes the Maintained
    // metaflow's successor; then an Observe's Gone of w1:p3, and a further
    // maintained wake and drain.
    assert!(reported_to(
        &fixture.nexus,
        stop_of(&world, &fixture),
        Some(old_hook.clone())
    ));
    assert!(reported_to(
        &fixture.nexus,
        Event::Ended(Some(signal_flow::NativeSessionId(
            world.seat.session.clone()
        ))),
        Some(old_hook.clone())
    ));
    {
        use crate::observe_agent::RecordsWitnessedExit;
        fixture
            .nexus
            .exited_as_witnessed_by(FLOW, Some(&old_witness));
    }
    fixture.nexus.wake_due(address(Model::Sol));
    fixture.nexus.drain_awake(&address(Model::Sol));
    assert_eq!(
        node(&fixture).flow_lifecycle,
        FlowLifecycle::Exited,
        "the old Nexus recorded the old seat's exit"
    );
    // Its Maintained metaflow owes a successor, and every wake of it is
    // refused while the resumed session runs in Herdr: nothing is launched,
    // and what it holds stays with it.
    let old_metaflow = stored(&fixture, Model::Sol);
    assert_eq!(old_metaflow.state, State::Asleep);
    assert_eq!(old_metaflow.queue.0.len(), 1);
    assert!(matches!(
        fixture.nexus.store.successor_intent(&address(Model::Sol)),
        Ok(Some(intent)) if intent.predecessor.0 == FLOW
    ));

    // The same old reports reach the new Nexus, and an Observe's Gone of
    // the seat it adopted: none acts.
    assert!(!reported_to(
        &new,
        Event::Ended(Some(signal_flow::NativeSessionId(
            world.seat.session.clone()
        ))),
        Some(old_hook.clone())
    ));
    assert!(!reported_to(
        &new,
        Event::Ended(Some(signal_flow::NativeSessionId(
            world.seat.session.clone()
        ))),
        None
    ));
    {
        use crate::observe_agent::RecordsWitnessedExit;
        new.exited_as_witnessed_by(FLOW, Some(&adopted_witness));
    }

    // Nothing the old side did touched the new seat.
    let calls = world.fresh.calls();
    let old_side: Vec<&str> = calls.lines().skip(before_old).collect();
    eprintln!("old side's Herdr calls:\n{}", old_side.join("\n"));
    for line in &old_side {
        assert!(!line.contains(NEW_PANE), "the old side named w7:p1: {line}");
        assert!(
            !line.contains(&world.seat.session),
            "the old side named the resumed session: {line}"
        );
        assert!(!line.contains("--resume"), "{line}");
        for effect in [
            "pane close",
            "workspace create",
            "agent start",
            "agent prompt",
            "pane run",
        ] {
            assert!(
                !line.contains(effect),
                "the old side acted on Herdr: {line}"
            );
        }
    }
    assert_eq!(received(&world, &fixture, "queued at the old Nexus"), 0);
    assert_eq!(received(&world, &fixture, "for the resumed seat"), 1);
    assert_eq!(fs::read(&claim).unwrap(), claimed, "the claim stands");
    assert!(FlowStore::process_is_live(&resumed), "the new seat runs on");

    // The new custody stands, and the new seat's hooks still act there.
    let after = new_node(&new);
    assert_eq!(after.session_id.0, world.seat.session);
    assert_eq!(after.flow_lifecycle, FlowLifecycle::Active);
    assert!(matches!(
        &after.herdr_route_selection,
        HerdrRouteSelection::Available(route) if route.herdr_pane_id.0 == NEW_PANE
    ));
    assert_eq!(
        new.store
            .metaflow(&address(Model::Sol))
            .unwrap()
            .unwrap()
            .state,
        State::Awake(flow_ethos::FlowId(FLOW.into()))
    );
    assert_eq!(new.store.restart_record(FLOW).unwrap(), None);
    assert_eq!(
        new.store.successor_intent(&address(Model::Sol)).unwrap(),
        None
    );
    assert_eq!(new.store.seat_birth(FLOW).unwrap(), Some(birth));
    assert!(new.report_acts(FLOW, Some(&new_hook)));
}

/// abc123's Address, `{ Mind flow Sol }`, protected.
fn protect(fixture: &NexusFixture) {
    fixture
        .nexus
        .store
        .configure_change(meta_signal_flow::ConfigurationChange::ProtectedAddresses(
            vec![address(Model::Sol)],
        ))
        .expect("protected");
}

/// Another flow, as the Nexus derives its origin from its process.
fn another_flow() -> signal_flow::Origin {
    signal_flow::Origin::Flow(signal_flow::FlowOrigin {
        flow_id: flow_ethos::FlowId("0f0e1d".into()),
        session_id: signal_flow::SessionId("another-session".into()),
    })
}

/// The pane the old harness runs in, as a BindSeat or an Adopt names it.
fn old_seat_pane() -> signal_flow::SeatPane {
    signal_flow::SeatPane {
        herdr_session_name: signal_flow::HerdrSessionName("fixture-session".into()),
        herdr_pane_id: signal_flow::HerdrPaneId(OLD_PANE.into()),
    }
}

/// A protected Address's flow is restarted from another flow on the ordinary
/// socket, as any flow is: closed, and its own session resumed in a new
/// pane. Protection keeps it from its threshold's refresh, nothing else.
#[test]
fn restart_protected_from_another_flow_accepted() {
    let mut fixture = NexusFixture::new();
    let world = fixture.restart_world(true);
    let old = world.old_harness();
    protect(&fixture);
    assert_eq!(
        fixture.nexus.dispatch_from(
            Query::Restart(flow_ethos::FlowId(FLOW.into())),
            another_flow()
        ),
        Response::Restarted(flow_ethos::FlowId(FLOW.into()))
    );
    assert_eq!(world.resumes().len(), 1, "{}", world.fresh.calls());
    assert!(ended(&old), "the old harness exited");
    assert_eq!(route_pane(&fixture), NEW_PANE);
    assert_eq!(node(&fixture).session_id.0, world.seat.session);
    assert_eq!(record(&fixture), None);
}

/// A flow's own Restart is served as anybody's: the seat rests, so it is
/// restarted. Nothing refuses a flow its own restart.
#[test]
fn restart_asked_by_the_flow_itself_is_served() {
    let mut fixture = NexusFixture::new();
    let world = fixture.restart_world(true);
    protect(&fixture);
    let own = signal_flow::Origin::Flow(signal_flow::FlowOrigin {
        flow_id: flow_ethos::FlowId(FLOW.into()),
        session_id: signal_flow::SessionId(world.seat.session.clone()),
    });
    assert_eq!(
        fixture
            .nexus
            .dispatch_from(Query::Restart(flow_ethos::FlowId(FLOW.into())), own),
        Response::Restarted(flow_ethos::FlowId(FLOW.into()))
    );
    assert_eq!(route_pane(&fixture), NEW_PANE);
    assert_eq!(record(&fixture), None);
}

/// The meta surface restarts a protected Address's flow: it is closed and
/// its own session resumed in a new pane, as an unprotected flow's restart
/// from outside is.
#[test]
fn restart_protected_from_meta_accepted() {
    let mut fixture = NexusFixture::new();
    let world = fixture.restart_world(true);
    let old = world.old_harness();
    protect(&fixture);
    assert_eq!(
        fixture
            .nexus
            .dispatch_meta(meta_signal_flow::Query::Restart(flow_ethos::FlowId(
                FLOW.into()
            ))),
        meta_signal_flow::Response::Restarted(flow_ethos::FlowId(FLOW.into()))
    );
    assert_eq!(world.resumes().len(), 1, "{}", world.fresh.calls());
    assert!(ended(&old), "the old harness exited");
    assert_eq!(route_pane(&fixture), NEW_PANE);
    assert_eq!(node(&fixture).session_id.0, world.seat.session);
    assert_eq!(record(&fixture), None);
}

/// A protected Address's flow is stopped by another flow on the ordinary
/// socket, and retired over the meta socket by another flow, as any flow
/// is.
#[test]
fn stop_and_retire_protected_from_another_flow_accepted() {
    let mut fixture = NexusFixture::new();
    let _world = fixture.restart_world(true);
    protect(&fixture);
    assert_eq!(
        fixture
            .nexus
            .dispatch_from(Query::Stop(flow_ethos::FlowId(FLOW.into())), another_flow()),
        Response::Stopped(flow_ethos::FlowId(FLOW.into()))
    );
    assert_eq!(node(&fixture).flow_lifecycle, FlowLifecycle::Stopped);

    let mut retiring = NexusFixture::new();
    let _world = retiring.restart_world(true);
    protect(&retiring);
    let mut resolver = super::queue_drain::resolver();
    resolver.flow_id_option = Some(flow_ethos::FlowId("0f0e1d".into()));
    assert!(matches!(
        retiring.nexus.dispatch_meta_from(
            meta_signal_flow::Query::Retire(flow_ethos::FlowId(FLOW.into())),
            Some(resolver)
        ),
        meta_signal_flow::Response::FlowRetired(_)
    ));
    assert_eq!(node(&retiring).flow_lifecycle, FlowLifecycle::Retired);
}
// The 0.53.0 incident (b5a9's tests, brought into 0.53.1): seven resting
// Claude seats Adopted by the new Nexus were Restarted; each old harness was
// closed, then the resumed launch was not composed, because the Curriculum
// running (0380, its main line) took only a Layer as a caller Address's
// third word and 0.53.0 sent a Model. 0.53.1 composes before it closes, so
// the same refusal closes nothing and writes nothing; and Flow reads
// Curriculum's schema as 478c writes it, its fifth Outcome `Rejected`.

/// The third words Curriculum's startup check takes: a Layer on its main
/// line (0380, the live service), a Model on 3f74.
const LAYER_WORDS: [&str; 4] = ["Primary", "Secondary", "Tertiary", "Quaternary"];
const MODEL_WORDS: [&str; 8] = [
    "Fable", "Opus", "Sonnet", "Haiku", "Astra", "Sol", "Luna", "Terra",
];
/// Curriculum's answer to a caller whose third word it does not take.
const INVALID_ADDRESS: &str = "invalid metaflow address";
/// The store of the new Nexus, beside the fixture's.
const NEW_STORE: &str = "flow-0530.sema";

/// A Curriculum Nexus on a Unix socket in the fixture directory, speaking
/// the frames Flow's `CurriculumComposer` speaks: one big-endian u32
/// length-prefixed rkyv `Query` in, one `Outcome` out, in 478c's schema. A
/// caller whose third word passes its check is composed from the fixture's
/// module bodies; any other is answered `Rejected` with `INVALID_ADDRESS`,
/// as 0380 answered it, and a module it lacks `Rejected` with
/// `missing module <identity>`, as 478c does. Every selection it is asked
/// is kept.
struct SocketCurriculum {
    socket: std::path::PathBuf,
    accepts_models: std::sync::Arc<std::sync::atomic::AtomicBool>,
    asked: std::sync::Arc<std::sync::Mutex<Vec<crate::generated::curriculum::StartupSelection>>>,
    stopping: std::sync::Arc<std::sync::atomic::AtomicBool>,
    server: Option<std::thread::JoinHandle<()>>,
}

impl SocketCurriculum {
    /// Serving at `socket`, taking Layer words only, as 0380 does.
    fn serving(socket: std::path::PathBuf) -> Self {
        use std::sync::{Arc, Mutex, atomic::AtomicBool};
        let listener = std::os::unix::net::UnixListener::bind(&socket).expect("Curriculum socket");
        let accepts_models = Arc::new(AtomicBool::new(false));
        let asked = Arc::new(Mutex::new(Vec::new()));
        let stopping = Arc::new(AtomicBool::new(false));
        let server = {
            let (accepts_models, asked, stopping) =
                (accepts_models.clone(), asked.clone(), stopping.clone());
            std::thread::spawn(move || {
                use std::io::{Read, Write};
                use std::sync::atomic::Ordering;
                for stream in listener.incoming() {
                    if stopping.load(Ordering::SeqCst) {
                        return;
                    }
                    let Ok(mut stream) = stream else { continue };
                    let mut prefix = [0_u8; 4];
                    if stream.read_exact(&mut prefix).is_err() {
                        continue;
                    }
                    let mut frame = vec![0_u8; u32::from_be_bytes(prefix) as usize];
                    if stream.read_exact(&mut frame).is_err() {
                        continue;
                    }
                    let outcome =
                        Self::answer(&frame, accepts_models.load(Ordering::SeqCst), &asked);
                    let answer = rkyv::to_bytes::<rkyv::rancor::Error>(&outcome).unwrap();
                    let _ = stream
                        .write_all(&(answer.len() as u32).to_be_bytes())
                        .and_then(|_| stream.write_all(&answer));
                }
            })
        };
        Self {
            socket,
            accepts_models,
            asked,
            stopping,
            server: Some(server),
        }
    }

    fn answer(
        frame: &[u8],
        accepts_models: bool,
        asked: &std::sync::Mutex<Vec<crate::generated::curriculum::StartupSelection>>,
    ) -> crate::generated::curriculum::Outcome {
        use crate::context::{ComposesFlowContext, ContextRequest, FixtureComposer};
        use crate::generated::curriculum::{Outcome, Query, Target};
        let Ok(Query::ComposeStartup(selection)) =
            rkyv::from_bytes::<Query, rkyv::rancor::Error>(frame)
        else {
            return Outcome::Rejected("unexpected query".into());
        };
        asked.lock().unwrap().push(selection.clone());
        let words: &[&str] = if accepts_models {
            &MODEL_WORDS
        } else {
            &LAYER_WORDS
        };
        if !words.contains(&selection.caller_metaflow.third_string.as_str()) {
            return Outcome::Rejected(INVALID_ADDRESS.into());
        }
        let request = ContextRequest {
            address: address(Model::Sol),
            harness_kind: match selection.target {
                Target::Codex => signal_flow::HarnessKind::Codex,
                _ => signal_flow::HarnessKind::Claude,
            },
            system_modules: signal_flow::SystemModules(
                selection
                    .system_modules
                    .0
                    .into_iter()
                    .map(|identity| signal_flow::ModuleIdentity(identity.0))
                    .collect(),
            ),
            user_modules: signal_flow::UserModules(
                selection
                    .user_modules
                    .0
                    .into_iter()
                    .map(|identity| signal_flow::ModuleIdentity(identity.0))
                    .collect(),
            ),
            remembered_flows: crate::generated::starter::RememberedFlows(
                selection
                    .flow_id_vector
                    .into_iter()
                    .map(|flow| flow_ethos::FlowId(flow.0))
                    .collect(),
            ),
        };
        let bodies = FixtureComposer::from(
            &[
                ("spirit:core:0", "Spirit body."),
                ("intent:flow:0", "Intent body."),
                ("operation:main-flow:0", "Main flow body."),
                ("knowledge:flow:0", "Knowledge body."),
            ][..],
        );
        match bodies.compose_flow_context(&request) {
            Ok(composition) => Outcome::StartupComposed(composition),
            Err(signal_flow::ContextRefusal::ModuleMissing(Some(identity))) => {
                Outcome::Rejected(format!("missing module {identity}", identity = identity.0))
            }
            Err(refusal) => Outcome::Rejected(format!("{refusal:?}")),
        }
    }

    /// From here on it takes Model words, as 3f74 does.
    fn accept_models(&self) {
        self.accepts_models
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }

    /// The production composer, aimed at this socket.
    fn composer(&self) -> crate::composition::LaunchComposer {
        use crate::composition::OpensLaunchComposer;
        use crate::context::OpensCurriculumComposer;
        crate::composition::LaunchComposer::with_context(crate::context::ContextComposer(
            std::sync::Arc::new(crate::context::CurriculumComposer::at(
                self.socket.to_string_lossy().as_ref(),
            )),
        ))
    }

    /// Each caller Address asked, as its three words.
    fn callers(&self) -> Vec<[String; 3]> {
        self.asked
            .lock()
            .unwrap()
            .iter()
            .map(|selection| {
                let caller = &selection.caller_metaflow;
                [
                    caller.first_string.clone(),
                    caller.second_string.clone(),
                    caller.third_string.clone(),
                ]
            })
            .collect()
    }
}

impl Drop for SocketCurriculum {
    fn drop(&mut self) {
        self.stopping
            .store(true, std::sync::atomic::Ordering::SeqCst);
        let _ = std::os::unix::net::UnixStream::connect(&self.socket);
        if let Some(server) = self.server.take() {
            let _ = server.join();
        }
    }
}

/// What the restart must not change of the flow: its FlowId and native
/// session, the model it actually runs, its transcript, how many flows the
/// Nexus holds, the claims on disk and the seat birth it holds.
#[derive(Debug, Clone, PartialEq, Eq)]
struct HeldIdentity {
    flow_id: String,
    session: String,
    model: Option<String>,
    transcript: Option<std::path::PathBuf>,
    flows: usize,
    claims: Vec<(String, Vec<u8>)>,
    birth: Option<SeatBirth>,
}

fn held_identity(fixture: &NexusFixture, nexus: &crate::RunningNexus) -> HeldIdentity {
    use crate::store::{ReadsFlowRoles, ReadsLaunchAttempt};
    use crate::waking::ReadsClaudeDelivery;
    let node = nexus.store.flow_node(FLOW).unwrap().expect("abc123 held");
    let model = match nexus.store.role(FLOW) {
        Ok(Some(role)) => Some(role.model_name),
        _ => nexus
            .store
            .launch_requests_bound_to(FLOW)
            .unwrap()
            .pop()
            .and_then(|request| nexus.store.composable_launch_profile(&request).unwrap())
            .map(|profile| profile.model_name),
    };
    let mut claims: Vec<(String, Vec<u8>)> = fs::read_dir(fixture.directory.path().join("flows"))
        .unwrap()
        .filter_map(Result::ok)
        .map(|entry| entry.file_name().to_string_lossy().into_owned())
        .filter(|name| name.ends_with(".flow-id"))
        .map(|name| {
            let bytes = fs::read(fixture.directory.path().join("flows").join(&name)).unwrap();
            (name, bytes)
        })
        .collect();
    claims.sort();
    HeldIdentity {
        flow_id: node.flow_id.clone().0,
        session: node.session_id.clone().0,
        model: model.map(|value| value.0),
        transcript: nexus.claude_transcript(&node),
        flows: nexus.store.flow_nodes().unwrap().len(),
        claims,
        birth: nexus.store.seat_birth(FLOW).unwrap(),
    }
}

/// The new Nexus's store closed and opened again at its path.
fn reopen_new(fixture: &NexusFixture, nexus: &mut crate::RunningNexus) {
    use crate::store::{OpensFlowStore, SetsNativeOwnerEnvironmentForTests};
    let placeholder = FlowStore::open(&fixture.directory.path().join("new-placeholder.sema"))
        .expect("placeholder store");
    drop(std::mem::replace(&mut nexus.store, placeholder.into()));
    let mut reopened =
        FlowStore::open(&fixture.directory.path().join(NEW_STORE)).expect("store reopens");
    reopened.set_native_owner_environment_for_tests(crate::store::NativeOwnerEnvironment {
        host_id: "isolated-test-host".into(),
        boot_id: "isolated-test-boot".into(),
    });
    nexus.store = reopened.into();
}

/// The answer the live restarts got.
fn not_composed() -> Response {
    Response::RestartRefused(RestartRefusal::ResumeRefused(ResumeRefused_Data {
        flow_id: flow_ethos::FlowId(FLOW.into()),
        string: "NotComposed".into(),
    }))
}

fn new_metaflow(nexus: &crate::RunningNexus) -> signal_flow::Metaflow {
    use crate::store::metaflow::SleepsMetaflows;
    nexus
        .store
        .metaflow(&address(Model::Sol))
        .unwrap()
        .expect("{ Mind flow Sol }")
}

/// The live cutover, up to the Restart: the old Nexus (the fixture's)
/// launched abc123, Awake and resting in w1:p3; the new Nexus, its own
/// store and its hooks at NEW_SOCKET, composing through the production
/// composer against a socket Curriculum that takes only Layer words,
/// Adopts that seat from the seat's own process.
fn adopted_on_new(
    fixture: &mut NexusFixture,
) -> (RestartWorld, crate::RunningNexus, SocketCurriculum) {
    use crate::sending::BindsSeats;
    let world = fixture.restart_world(true);
    FixtureExecutable {
        path: fixture.directory.path().join("native-transcripts/flow-id"),
    }
    .install(IDEMPOTENT_FLOW_ID);
    let curriculum = SocketCurriculum::serving(fixture.directory.path().join("curriculum.sock"));
    let mut new = fixture.second_nexus(NEW_STORE, NEW_SOCKET);
    *new.composer.edit() = curriculum.composer();
    assert_eq!(
        new.adopt(signal_flow::Adopt_Data {
            address: address(Model::Sol),
            seat_pane: old_seat_pane(),
            native_session_id: signal_flow::NativeSessionId(world.seat.session.clone()),
        }),
        Response::Adopted(flow_ethos::FlowId(FLOW.into()))
    );
    (world, new, curriculum)
}

/// A restart the Curriculum will not compose is refused before anything is
/// closed or written, and a retry recovers it once Curriculum takes Model
/// words. Refused NotComposed: no record, before or after a store reopen;
/// the old harness running in its pane, nothing closed, opened or typed;
/// the FlowId, native session, actual model, transcript, claims and queue
/// unchanged. A retry while still refused is refused alike and closes
/// nothing. Once Model words are taken, a retry answers Restarted: the old
/// pane closed once, `claude --resume <the same session>` with hooks naming
/// only the new Nexus's socket, and the queue handed over once.
#[test]
fn restart_not_composed_refused_then_recovered() {
    let mut fixture = NexusFixture::new();
    let (world, mut new, curriculum) = adopted_on_new(&mut fixture);
    let old = world.old_harness();
    let before = held_identity(&fixture, &new);
    assert_eq!(before.flow_id, FLOW);
    assert_eq!(before.session, world.seat.session);
    assert!(before.model.is_some() && before.transcript.is_some());
    let mut queued = new_metaflow(&new);
    queued.queue = flow_ethos::Queue(vec![signal_flow::QueueEntry {
        logical_delivery_id: flow_ethos::LogicalDeliveryId("held-through-not-composed".into()),
        request_id: flow_ethos::RequestId("test".into()),
        sent_by: signal_flow::SentBy::Unknown,
        delivery: flow_ethos::Delivery::Wake,
        message: signal_flow::Message::Soft(signal_flow::Request::Order(
            "held through NotComposed".into(),
        )),
    }]);
    new.store.seed_metaflow(queued);

    assert_eq!(
        new.dispatch(Query::Restart(flow_ethos::FlowId(FLOW.into()))),
        not_composed()
    );
    assert_eq!(
        curriculum.callers(),
        vec![["Mind".to_owned(), "flow".to_owned(), "Sol".to_owned()]],
        "the caller names its Model"
    );
    let untouched = |world: &RestartWorld, fixture: &NexusFixture| {
        for effect in [
            "pane close",
            "workspace create",
            "agent start",
            "agent prompt",
            "pane run",
        ] {
            assert!(
                world.calls_naming(effect).is_empty(),
                "{effect}: {}",
                world.fresh.calls()
            );
        }
        assert!(world.fresh.user_turns(fixture).is_empty(), "nothing typed");
        assert!(
            FlowStore::process_is_live(&world.old_harness()),
            "the old harness runs on"
        );
    };
    assert_eq!(
        new.store.restart_record(FLOW).unwrap(),
        None,
        "nothing written"
    );
    untouched(&world, &fixture);
    assert_eq!(held_identity(&fixture, &new), before);
    assert_eq!(new_metaflow(&new).queue.0.len(), 1);
    assert_eq!(
        new_metaflow(&new).state,
        State::Awake(flow_ethos::FlowId(FLOW.into()))
    );

    // A Nexus that died here leaves its successor nothing to take up.
    reopen_new(&fixture, &mut new);
    assert_eq!(new.store.restart_record(FLOW).unwrap(), None);
    assert_eq!(held_identity(&fixture, &new), before);
    assert_eq!(new_metaflow(&new).queue.0.len(), 1);
    untouched(&world, &fixture);

    // A retry while Curriculum still refuses: refused alike, nothing closed.
    assert_eq!(
        new.dispatch(Query::Restart(flow_ethos::FlowId(FLOW.into()))),
        not_composed()
    );
    assert_eq!(new.store.restart_record(FLOW).unwrap(), None);
    untouched(&world, &fixture);
    assert_eq!(held_identity(&fixture, &new), before);
    assert_eq!(new_metaflow(&new).queue.0.len(), 1);
    assert_eq!(curriculum.callers().len(), 2);

    // Curriculum takes Model words, as 3f74 and 478c do: the retry resumes.
    curriculum.accept_models();
    assert_eq!(
        new.dispatch(Query::Restart(flow_ethos::FlowId(FLOW.into()))),
        Response::Restarted(flow_ethos::FlowId(FLOW.into()))
    );
    assert_eq!(
        world.calls_naming(&format!("pane close {OLD_PANE}")).len(),
        1,
        "the old pane is closed once, by the restart that resumes"
    );
    assert!(ended(&old), "the old harness exited");
    let resumes = world.resumes();
    assert_eq!(resumes.len(), 1, "{}", world.fresh.calls());
    let resume = &resumes[0];
    assert!(
        resume.contains(&format!("--resume {}", world.seat.session)),
        "{resume}"
    );
    assert!(!resume.contains("--session-id"), "{resume}");
    assert!(resume.contains(&format!("--pane {NEW_PANE}")), "{resume}");
    assert!(resume.contains(NEW_SOCKET), "{resume}");
    assert!(!resume.contains(OLD_SOCKET), "{resume}");
    assert_eq!(
        resume.matches("flow.sock").count(),
        resume.matches(NEW_SOCKET).count(),
        "every hook socket is the new Nexus's: {resume}"
    );
    assert_eq!(world.calls_naming("agent start").len(), 1);
    assert_eq!(new.store.restart_record(FLOW).unwrap(), None);
    assert_eq!(received(&world, &fixture, "held through NotComposed"), 1);
    assert!(new_metaflow(&new).queue.0.is_empty());
    new.drain_awake(&address(Model::Sol));
    assert_eq!(received(&world, &fixture, "held through NotComposed"), 1);

    let after = held_identity(&fixture, &new);
    assert_eq!(after.flow_id, before.flow_id);
    assert_eq!(after.session, before.session);
    assert_eq!(after.model, before.model);
    assert_eq!(after.transcript, before.transcript);
    assert_eq!(after.flows, before.flows);
    assert_eq!(after.claims, before.claims, "no new native identity");
    let node = new.store.flow_node(FLOW).unwrap().unwrap();
    assert_eq!(node.flow_lifecycle, FlowLifecycle::Active);
    assert!(matches!(
        &node.herdr_route_selection,
        HerdrRouteSelection::Available(route) if route.herdr_pane_id.0 == NEW_PANE
    ));
    assert_eq!(
        after.birth.map(|birth| birth.herdr_route.herdr_pane_id),
        Some(signal_flow::HerdrPaneId(NEW_PANE.to_owned()))
    );
    assert_eq!(
        new_metaflow(&new).state,
        State::Awake(flow_ethos::FlowId(FLOW.into()))
    );
}

/// The owner's Stop after a restart refused NotComposed is an ordinary
/// Stop, since the refusal left no record: the old pane closed once, the
/// flow Stopped with its FlowId and session kept, its queue kept on its
/// metaflow, nothing started or typed. A Restart of the stopped flow, the
/// last of its Asleep metaflow, answers Asleep: it has no harness to close,
/// and nothing is done.
#[test]
fn restart_not_composed_owner_stop() {
    let mut fixture = NexusFixture::new();
    let (world, new, curriculum) = adopted_on_new(&mut fixture);
    let old = world.old_harness();
    let mut queued = new_metaflow(&new);
    queued.queue = flow_ethos::Queue(vec![signal_flow::QueueEntry {
        logical_delivery_id: flow_ethos::LogicalDeliveryId("kept-through-stop".into()),
        request_id: flow_ethos::RequestId("test".into()),
        sent_by: signal_flow::SentBy::Unknown,
        delivery: flow_ethos::Delivery::Wake,
        message: signal_flow::Message::Soft(signal_flow::Request::Order(
            "kept through the Stop".into(),
        )),
    }]);
    new.store.seed_metaflow(queued);
    assert_eq!(
        new.dispatch(Query::Restart(flow_ethos::FlowId(FLOW.into()))),
        not_composed()
    );
    assert!(world.calls_naming("pane close").is_empty());
    assert_eq!(new.store.restart_record(FLOW).unwrap(), None);

    assert_eq!(
        new.dispatch(Query::Stop(flow_ethos::FlowId(FLOW.into()))),
        Response::Stopped(flow_ethos::FlowId(FLOW.into()))
    );
    assert_eq!(new.store.restart_record(FLOW).unwrap(), None);
    let node = new.store.flow_node(FLOW).unwrap().unwrap();
    assert_eq!(node.flow_lifecycle, FlowLifecycle::Stopped);
    assert_eq!(node.session_id.0, world.seat.session);
    assert_eq!(new_metaflow(&new).queue.0.len(), 1, "the queue is kept");
    assert_eq!(
        world.calls_naming(&format!("pane close {OLD_PANE}")).len(),
        1
    );
    assert!(ended(&old), "the old harness exited");
    assert!(world.calls_naming("agent start").is_empty());
    assert!(world.calls_naming("agent prompt").is_empty());
    assert_eq!(received(&world, &fixture, "kept through the Stop"), 0);

    curriculum.accept_models();
    assert_eq!(
        new.dispatch(Query::Restart(flow_ethos::FlowId(FLOW.into()))),
        Response::RestartRefused(RestartRefusal::Asleep(flow_ethos::FlowId(FLOW.into())))
    );
    assert!(world.resumes().is_empty());
}

/// A resumed launch's composition, against a real Curriculum Nexus: for each seat named in the file `RESTART_PROOF_SEATS` (a line of
/// `flow <tab> Aspect topic Model <tab> native model <tab> system modules
/// <tab> user modules`), the launch profile Restart builds is composed
/// through `perform(Compose)` with the production composer aimed at the
/// socket `RESTART_PROOF_CURRICULUM_SOCKET`, and `RESTART_PROOF_EXPECT`
/// (`refused` or `composed`) says what that Curriculum must answer. The raw
/// answer frame to the same selection is printed and, when refused,
/// compared byte for byte with the `Rejected` frame Flow reads it as.
#[test]
#[ignore = "needs a real Curriculum Nexus socket; run by hand"]
fn restart_composition_against_curriculum_socket() {
    use crate::composition::{OpensLaunchComposer, PlacesModuleByKind};
    use crate::context::{ContextRequest, OpensCurriculumComposer, SelectsStartup};
    use crate::generated::curriculum::{Outcome as CurriculumOutcome, Query as CurriculumQuery};
    use crate::generated::operation::Failed_Data;
    use std::io::{Read, Write};
    let socket = std::env::var("RESTART_PROOF_CURRICULUM_SOCKET").expect("socket");
    let expect = std::env::var("RESTART_PROOF_EXPECT").expect("refused or composed");
    let seats = fs::read_to_string(std::env::var("RESTART_PROOF_SEATS").expect("seats")).unwrap();
    let mut fixture = NexusFixture::new();
    *fixture.nexus.composer.edit() =
        crate::composition::LaunchComposer::with_context(crate::context::ContextComposer(
            std::sync::Arc::new(crate::context::CurriculumComposer::at(socket.as_str())),
        ));
    let words = |text: &str| {
        text.split_whitespace()
            .map(|word| signal_flow::ModuleIdentity(word.to_owned()))
            .collect::<Vec<_>>()
    };
    let mut composed = 0;
    for line in seats.lines().filter(|line| !line.trim().is_empty()) {
        let fields: Vec<&str> = line.split('\t').collect();
        let [flow, caller, native, system, user] = fields[..] else {
            panic!("seat line: {line}")
        };
        let caller = words(caller);
        let address = Address {
            aspect: match caller[0].0.as_str() {
                "Psyche" => Aspect::Psyche,
                "Mind" => Aspect::Mind,
                _ => Aspect::Field,
            },
            topic: flow_ethos::Topic(caller[1].clone().0),
            model: match caller[2].0.as_str() {
                "Fable" => Model::Fable,
                "Opus" => Model::Opus,
                "Sonnet" => Model::Sonnet,
                "Haiku" => Model::Haiku,
                "Astra" => Model::Astra,
                "Sol" => Model::Sol,
                other => panic!("model {other}"),
            },
        };
        // The profile `resumed_launch` builds from a declaration.
        let profile = signal_flow::LaunchProfile {
            launch_request_id: signal_flow::LaunchRequestId(format!("restart-{flow}-1-proof")),
            system_modules: signal_flow::SystemModules(words(system)),
            user_modules: signal_flow::UserModules(words(user)),
            remembering: signal_flow::Remembering::Predecessors(signal_flow::RememberingDepth(0)),
            address: address.clone(),
            model_name: flow_ethos::ModelName(native.to_owned()),
            effort: signal_flow::Effort(
                <crate::RunningNexus as crate::sending::ChoosesEffort>::effort_of(&address).into(),
            ),
            flow_id_option: None,
            herdr_session_name: signal_flow::HerdrSessionName("fixture-session".into()),
            leave_outs_option: None,
            flow_kind: signal_flow::FlowKind::Regular,
            tool_set: signal_flow::ToolSet::Default,
            instruction_prompt: signal_flow::InstructionPrompt(String::new()),
        };
        let outcome = fixture.nexus.perform(Operation::Compose(profile.clone()));
        // The same selection, sent raw: the frame Curriculum answers.
        let (system_modules, user_modules): (Vec<_>, Vec<_>) = profile
            .system_modules
            .0
            .iter()
            .chain(profile.user_modules.0.iter())
            .cloned()
            .partition(|identity| identity.0.as_str().placed_in_system_prompt());
        let selection = ContextRequest {
            address: address.clone(),
            harness_kind: signal_flow::HarnessKind::Claude,
            system_modules: signal_flow::SystemModules(system_modules),
            user_modules: signal_flow::UserModules(user_modules),
            remembered_flows: crate::generated::starter::RememberedFlows(vec![]),
        }
        .startup_selection();
        let query = rkyv::to_bytes::<rkyv::rancor::Error>(&CurriculumQuery::ComposeStartup(
            selection.clone(),
        ))
        .unwrap();
        let mut stream = std::os::unix::net::UnixStream::connect(&socket).unwrap();
        stream
            .write_all(&(query.len() as u32).to_be_bytes())
            .unwrap();
        stream.write_all(&query).unwrap();
        let mut prefix = [0_u8; 4];
        stream.read_exact(&mut prefix).unwrap();
        let mut frame = vec![0_u8; u32::from_be_bytes(prefix) as usize];
        stream.read_exact(&mut frame).unwrap();
        let shown = match &outcome {
            Outcome::Composed(launch) => format!(
                "Composed: system prompt {} bytes, first prompt {} bytes, address {:?}",
                launch.system_prompt.system_prompt_text.0.len(),
                launch.first_prompt_payload.first_prompt_body.0.len(),
                launch.launch_profile.address
            ),
            other => format!("{other:?}"),
        };
        eprintln!(
            "seat {flow} caller {:?} system {:?} user {:?}\n  perform(Compose): {shown}\n  raw answer frame: {} bytes",
            [
                &selection.caller_metaflow.first_string,
                &selection.caller_metaflow.second_string,
                &selection.caller_metaflow.third_string,
            ],
            selection.system_modules,
            selection.user_modules,
            frame.len()
        );
        match expect.as_str() {
            "refused" => {
                let read = rkyv::from_bytes::<CurriculumOutcome, rkyv::rancor::Error>(&frame)
                    .expect("Flow reads the frame");
                let CurriculumOutcome::Rejected(text) = &read else {
                    panic!("seat {flow}: {read:?}")
                };
                assert_eq!(
                    outcome,
                    Outcome::Failed(Failed_Data::ContextRefused(
                        signal_flow::ContextRefusal::ComposerRejected(text.clone())
                    )),
                    "seat {flow}"
                );
                let flow_reads = rkyv::to_bytes::<rkyv::rancor::Error>(&read).unwrap();
                assert_eq!(
                    frame,
                    flow_reads.to_vec(),
                    "seat {flow}: the frame Flow reads"
                );
                eprintln!("  frame equals Flow's Outcome::Rejected({text:?})");
            }
            _ => {
                let Outcome::Composed(launch) = &outcome else {
                    panic!("seat {flow}: {outcome:?}")
                };
                assert_eq!(launch.launch_profile.address, address);
                for module in profile
                    .system_modules
                    .0
                    .iter()
                    .chain(profile.user_modules.0.iter())
                {
                    let marker = format!("<!-- module: {module} -->", module = module.0);
                    assert!(
                        launch.system_prompt.system_prompt_text.0.contains(&marker)
                            || launch
                                .first_prompt_payload
                                .first_prompt_body
                                .0
                                .contains(&marker),
                        "seat {flow}: {module} composed",
                        module = module.0
                    );
                }
                composed += 1;
            }
        }
    }
    eprintln!("expect {expect}: {composed} composed");
}

// The proof of a recorded harness's exit, fed case by case through the
// process-status seam: only a pid not found, another birth at the pid, or a
// zombie or dead state proves it; everything else is unproven.

/// A kernel fed one answer for every stat read, with `pid_max` as given,
/// counting the reads.
struct FedKernel {
    stat: Result<String, std::io::ErrorKind>,
    pid_max: Option<u32>,
    reads: std::cell::Cell<usize>,
}

impl ReadsProcessStatus for FedKernel {
    fn process_stat(&self, _pid: u32) -> std::io::Result<String> {
        self.reads.set(self.reads.get() + 1);
        self.stat.clone().map_err(std::io::Error::from)
    }

    fn pid_max(&self) -> Option<u32> {
        self.pid_max
    }
}

/// The pid_max the fed kernel reports unless a case says otherwise.
const FED_PID_MAX: u32 = 4_194_304;

/// The recorded harness every case asks about: pid 4242, born at tick 777.
const RECORDED: Process = Process {
    pid: flow_ethos::Pid(4242),
    started: flow_ethos::Started(777),
};

/// A kernel whose stat read answers `stat`.
fn fed(stat: Result<String, std::io::ErrorKind>) -> FedKernel {
    FedKernel {
        stat,
        pid_max: Some(FED_PID_MAX),
        reads: std::cell::Cell::new(0),
    }
}

/// `/proc/4242/stat` as the kernel writes it, in `state`, started at
/// `started`; the command holds a `) ` as a command may.
fn stat_line(state: &str, started: &str) -> String {
    format!(
        "4242 (claude) x) {state} 1 4242 4242 0 -1 4194560 10 0 0 0 5 2 0 0 20 0 3 0 {started} 1000 200 18446744073709551615"
    )
}

#[test]
fn restart_exit_proof_not_found_proves_exit() {
    let kernel = fed(Err(std::io::ErrorKind::NotFound));
    assert_eq!(
        RECORDED.exit_proof(&kernel),
        ExitProof::Exited(ExitEvidence::Gone)
    );
    assert_eq!(kernel.reads.get(), 1);
}

#[test]
fn restart_exit_proof_other_birth_proves_exit() {
    for state in ["R", "S", "D", "Z"] {
        assert_eq!(
            RECORDED.exit_proof(&fed(Ok(stat_line(state, "778")))),
            ExitProof::Exited(ExitEvidence::Reborn),
            "{state}"
        );
    }
}

#[test]
fn restart_exit_proof_zombie_and_dead_prove_exit() {
    for state in ["Z", "X"] {
        assert_eq!(
            RECORDED.exit_proof(&fed(Ok(stat_line(state, "777")))),
            ExitProof::Exited(ExitEvidence::Defunct),
            "{state}"
        );
    }
}

#[test]
fn restart_exit_proof_permission_denied_unproven() {
    assert_eq!(
        RECORDED.exit_proof(&fed(Err(std::io::ErrorKind::PermissionDenied))),
        ExitProof::Unproven(ExitDoubt::PermissionDenied)
    );
}

#[test]
fn restart_exit_proof_io_error_unproven() {
    for kind in [
        std::io::ErrorKind::Interrupted,
        std::io::ErrorKind::InvalidData,
        std::io::ErrorKind::UnexpectedEof,
        std::io::ErrorKind::Other,
    ] {
        assert_eq!(
            RECORDED.exit_proof(&fed(Err(kind))),
            ExitProof::Unproven(ExitDoubt::Unreadable),
            "{kind:?}"
        );
    }
}

#[test]
fn restart_exit_proof_malformed_stat_unproven() {
    for stat in [
        "",
        "4242 claude Z 777",
        "4242 (claude) ",
        "4242 (claude) Zz 1 2",
    ] {
        assert_eq!(
            RECORDED.exit_proof(&fed(Ok(stat.into()))),
            ExitProof::Unproven(ExitDoubt::Malformed),
            "{stat:?}"
        );
    }
}

#[test]
fn restart_exit_proof_unparseable_start_time_unproven() {
    for state in ["S", "Z", "X"] {
        for started in ["", "seven", "-777", "7.77"] {
            let stat = if started.is_empty() {
                "4242 (claude) Z 1 4242".replace('Z', state)
            } else {
                stat_line(state, started)
            };
            assert_eq!(
                RECORDED.exit_proof(&fed(Ok(stat.clone()))),
                ExitProof::Unproven(ExitDoubt::StartUnparsed),
                "{stat:?}"
            );
        }
    }
}

/// A pid that cannot name a process is never read: the kernel here would
/// answer not found, which would otherwise prove the exit.
#[test]
fn restart_exit_proof_invalid_pid_unproven() {
    let beyond = i64::from(FED_PID_MAX) + 1;
    for pid in [0, -1, -4242, beyond, i64::from(u32::MAX) + 1, i64::MAX] {
        let kernel = fed(Err(std::io::ErrorKind::NotFound));
        let process = Process {
            pid: flow_ethos::Pid(pid),
            started: flow_ethos::Started(777),
        };
        assert_eq!(
            process.exit_proof(&kernel),
            ExitProof::Unproven(ExitDoubt::InvalidPid),
            "{pid}"
        );
        assert_eq!(kernel.reads.get(), 0, "{pid}");
    }
    let unread = FedKernel {
        pid_max: None,
        ..fed(Err(std::io::ErrorKind::NotFound))
    };
    assert_eq!(
        RECORDED.exit_proof(&unread),
        ExitProof::Unproven(ExitDoubt::InvalidPid)
    );
    assert_eq!(unread.reads.get(), 0);
}

/// A recorded birth no kernel start time can be is never compared: the
/// kernel here would answer not found.
#[test]
fn restart_exit_proof_unparsed_birth_unproven() {
    let kernel = fed(Err(std::io::ErrorKind::NotFound));
    let process = Process {
        pid: flow_ethos::Pid(4242),
        started: flow_ethos::Started(-1),
    };
    assert_eq!(
        process.exit_proof(&kernel),
        ExitProof::Unproven(ExitDoubt::BirthUnparsed)
    );
    assert_eq!(kernel.reads.get(), 0);
}

#[test]
fn restart_exit_proof_live_states_same_birth_running() {
    for state in ["R", "S", "D"] {
        assert_eq!(
            RECORDED.exit_proof(&fed(Ok(stat_line(state, "777")))),
            ExitProof::Running,
            "{state}"
        );
    }
}

/// The production reader is the kernel's /proc: a running process is
/// Running, and once ended and reaped it is Gone.
#[test]
fn restart_exit_proof_proc_reads_the_kernel() {
    let running = live_process();
    assert_eq!(running.1.exit_proof(&ProcFs), ExitProof::Running);
    let ended = exited_process();
    assert!(
        matches!(ended.exit_proof(&ProcFs), ExitProof::Exited(_)),
        "{:?}",
        ended.exit_proof(&ProcFs)
    );
}
