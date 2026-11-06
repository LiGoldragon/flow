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
use crate::caller::{CallerPane, CallerProcess, LocatesCallerPane, ReadsProcess};
use crate::generated::memory::SeatBirth;
use crate::generated::operation::{Operation, Outcome, Record_Data};
use crate::performing::Performs;
use crate::reporting::RecordsReports;
use crate::restarting::{FencesSeatEvents, SeatWitness};
use crate::store::metaflow::{MutatesMetaflows, SeedsMetaflow};
use crate::store::restart::RecordsRestarts;
use crate::store::successor::OwesSuccessors;
use crate::store::{FlowStore, ReadsFlowMemory, ReadsFlowRows, RegistersFlowIdentity};
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
        let seat = launched_claude_ended_by(self, |_| true);
        self.nexus.store.seed_metaflow(metaflow(
            Model::Sol,
            State::Awake(FLOW.into()),
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
                    "name": seat.profile.launch_request_id,
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
            old_agent: route.herdr_agent_name,
        }
    }

    fn reopen_store(&mut self) {
        use crate::store::{OpensFlowStore, SetsNativeOwnerEnvironmentForTests};
        let placeholder = FlowStore::open(&self.directory.path().join("placeholder.sema"))
            .expect("placeholder store");
        drop(std::mem::replace(&mut self.nexus.store, placeholder));
        let mut reopened =
            FlowStore::open(&self.directory.path().join("flow.sema")).expect("store reopens");
        reopened.set_native_owner_environment_for_tests(crate::store::NativeOwnerEnvironment {
            host_id: "isolated-test-host".into(),
            boot_id: "isolated-test-boot".into(),
        });
        self.nexus.store = reopened;
    }
}

/// Whether a process the kernel knew has ended: gone, another process at
/// its pid, or a zombie its reaper has not yet collected.
fn ended(process: &Process) -> bool {
    match fs::read_to_string(format!("/proc/{}/stat", process.pid)) {
        Err(_) => true,
        Ok(stat) => stat.contains(") Z") || !FlowStore::process_is_live(process),
    }
}

/// Waits, bounded, for a file a fixture process writes.
fn await_path(path: &Path) {
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while !path.exists() {
        assert!(
            std::time::Instant::now() < deadline,
            "{} never appeared",
            path.display()
        );
        std::thread::sleep(Duration::from_millis(2));
    }
}

/// The pid a fixture process wrote.
fn pid_in(path: &Path) -> u32 {
    await_path(path);
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        if let Ok(pid) = fs::read_to_string(path).unwrap_or_default().trim().parse() {
            return pid;
        }
        assert!(std::time::Instant::now() < deadline, "no pid written");
        std::thread::sleep(Duration::from_millis(2));
    }
}

impl RestartWorld {
    /// The resumed harness's hook process.
    fn new_hook(&self) -> CallerProcess {
        CallerProcess {
            process_id: pid_in(&self.fresh.state.join("hook-pid")),
        }
    }

    /// The old harness's hook process.
    fn old_hook(&self) -> CallerProcess {
        CallerProcess {
            process_id: pid_in(&self.fresh.state.join("old-hook-pid")),
        }
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
    fixture.nexus.dispatch(Query::Restart(FLOW.into()))
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
        HerdrRouteSelection::Available(route) => route.herdr_pane_id,
        HerdrRouteSelection::Unavailable => String::new(),
    }
}

/// A report as the ordinary socket serves it: attributed while its caller
/// waits, recorded, then acted on only when attributed.
fn report_from(fixture: &NexusFixture, event: Event, caller: Option<CallerProcess>) -> bool {
    let acts = fixture.nexus.report_acts(FLOW, caller);
    assert_eq!(
        fixture.nexus.report(Report_Data {
            flow_id: FLOW.into(),
            event: event.clone(),
        }),
        Response::Reported
    );
    if acts {
        fixture
            .nexus
            .after_attributed_report(&FLOW.to_owned(), &event);
    }
    acts
}

fn stop_of(world: &RestartWorld, fixture: &NexusFixture) -> Event {
    Event::Stopped(NativeStop {
        native_session_id_option: Some(world.seat.session.clone()),
        transcript_path_option: Some(
            fixture
                .directory
                .path()
                .join("native-transcripts/claude/-home-li-primary")
                .join(format!("{}.jsonl", world.seat.session))
                .to_string_lossy()
                .into_owned(),
        ),
        last_assistant_message_option: None,
    })
}

fn sender() -> Address {
    Address {
        aspect: Aspect::Psyche,
        topic: "core".into(),
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
            &process,
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
            recipient: address(Model::Sol),
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
        flow_id: FLOW.into(),
        native_session_id: world.seat.session.clone(),
        address: address(Model::Sol),
        old_seat: OldSeat {
            herdr_route: HerdrRoute {
                herdr_session_name: "fixture-session".into(),
                herdr_agent_name: world.old_agent.clone(),
                herdr_pane_id: OLD_PANE.into(),
                herdr_terminal_id: "term-old".into(),
            },
            process_option: None,
        },
        restart_phase: RestartPhase::Spawning(pane),
        attempts: 1,
    }
}

/// What the fixture Herdr makes as a restart's next pane.
fn next_pane(agent: &str) -> HerdrPaneBinding {
    HerdrPaneBinding {
        launch_request_id: "restart-dead-nexus".into(),
        herdr_session_name: "fixture-session".into(),
        herdr_agent_name: agent.into(),
        herdr_workspace_id: "w7".into(),
        herdr_pane_id: NEW_PANE.into(),
        herdr_terminal_id: "term-fresh".into(),
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

    assert_eq!(restarted(&fixture), Response::Restarted(FLOW.into()));

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
    assert_eq!(after.session_id, world.seat.session);
    assert_eq!(after.flow_lifecycle, FlowLifecycle::Active);
    assert_eq!(route_pane(&fixture), NEW_PANE);
    assert_eq!(
        stored(&fixture, Model::Sol).state,
        State::Awake(FLOW.into())
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

/// A Send while the restart holds its spawn queues, and nothing reaches any
/// pane; once bound, the queue is handed to the resumed seat exactly once,
/// and a second drain hands it nothing more.
#[test]
fn restart_arrival_queued_delivered_once() {
    use crate::restarting::RestartsFlows;
    let mut fixture = NexusFixture::new();
    let world = fixture.restart_world(true);
    let from = bound_sender(&fixture);
    fs::write(world.fresh.state.join("hold-start"), "").unwrap();

    std::thread::scope(|scope| {
        let restarting = scope.spawn(|| fixture.nexus.restart(FLOW, &signal_flow::Origin::Unbound));
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
        assert_eq!(stored(&fixture, Model::Sol).queue.len(), 1);
        assert!(world.calls_naming("agent prompt").is_empty());
        fs::write(world.fresh.state.join("release-start"), "").unwrap();
        assert_eq!(restarting.join().unwrap(), Response::Restarted(FLOW.into()));
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
    assert!(stored(&fixture, Model::Sol).queue.is_empty());
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
    assert_eq!(restarted(&fixture), Response::Restarted(FLOW.into()));

    // The old hook outlived its harness, as a SessionEnd hook does.
    assert!(
        FlowStore::process_identity_for_pid(old_hook.process_id).is_some(),
        "the old hook still runs"
    );
    let ended = Event::Ended(Some(world.seat.session.clone()));
    assert!(!report_from(
        &fixture,
        stop_of(&world, &fixture),
        Some(old_hook)
    ));
    assert!(!report_from(&fixture, ended.clone(), Some(old_hook)));
    assert!(!report_from(&fixture, ended.clone(), None));
    use crate::observe_agent::RecordsWitnessedExit;
    fixture
        .nexus
        .exited_as_witnessed_by(FLOW, Some(&old_witness));

    assert_eq!(node(&fixture).flow_lifecycle, FlowLifecycle::Active);
    assert_eq!(
        stored(&fixture, Model::Sol).state,
        State::Awake(FLOW.into())
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
    assert_eq!(restarted(&fixture), Response::Restarted(FLOW.into()));
    let hook = world.new_hook();
    assert_eq!(hook.marked_pane_id(), None, "the hook's own marks are gone");
    assert_eq!(
        hook.caller_pane(&fixture.nexus.herdr),
        Some(CallerPane {
            herdr_session_name: "fixture-session".into(),
            herdr_pane_id: NEW_PANE.into(),
        })
    );
    assert!(report_from(
        &fixture,
        Event::Ended(Some(world.seat.session.clone())),
        Some(hook)
    ));
    assert_eq!(node(&fixture).flow_lifecycle, FlowLifecycle::Exited);
    assert_eq!(stored(&fixture, Model::Sol).state, State::Asleep);

    let mut second = NexusFixture::new();
    let _world = second.restart_world(true);
    assert_eq!(restarted(&second), Response::Restarted(FLOW.into()));
    let witness: SeatWitness = second.nexus.seat_witness(FLOW).expect("witness");
    assert_eq!(witness.route.herdr_pane_id, NEW_PANE);
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
    assert_eq!(restarted(&fixture), Response::Restarted(FLOW.into()));
    assert!(world.resumes()[0].contains("/fixture/run/flow-next/flow/flow.sock"));
    let mut queued = stored(&fixture, Model::Sol);
    queued.queue = vec![signal_flow::QueueEntry {
        logical_delivery_id: "after-restart".into(),
        sent_by: signal_flow::SentBy::Unknown,
        delivery: flow_ethos::Delivery::Wake,
        message: signal_flow::Message::Soft(signal_flow::Request::Order(
            "for the resumed seat".into(),
        )),
    }];
    fixture.nexus.store.seed_metaflow(queued);

    assert!(!report_from(&fixture, stop_of(&world, &fixture), None));
    assert_eq!(received(&world, &fixture, "for the resumed seat"), 0);
    assert!(report_from(
        &fixture,
        stop_of(&world, &fixture),
        Some(world.new_hook())
    ));
    assert_eq!(received(&world, &fixture, "for the resumed seat"), 1);
    let events = crate::store::events::RecordsHarnessEvents::events(&fixture.nexus.store, FLOW)
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
        Response::RestartRefused(RestartRefusal::CloseRefused(FLOW.into()))
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
    assert_eq!(restarted(&fixture), Response::Restarted(FLOW.into()));
    assert_eq!(
        world.calls_naming(&format!("pane close {OLD_PANE}")).len(),
        2,
        "the retry closes the old pane again: {}",
        world.fresh.calls()
    );
    assert!(ended(&old));
    assert_eq!(route_pane(&fixture), NEW_PANE);
}

/// The resume is refused: ResumeRefused, the record Failed, its pane
/// discarded, the session and queue kept. The owner's Stop records it
/// Stopped, and a retry then resumes it and hands it its queue once.
#[test]
fn restart_resume_refused_custody_kept() {
    let mut fixture = NexusFixture::new();
    let world = fixture.restart_world(true);
    let mut queued = stored(&fixture, Model::Sol);
    queued.queue = vec![signal_flow::QueueEntry {
        logical_delivery_id: "kept-through-refusal".into(),
        sent_by: signal_flow::SentBy::Unknown,
        delivery: flow_ethos::Delivery::Wake,
        message: signal_flow::Message::Soft(signal_flow::Request::Order("kept work".into())),
    }];
    fixture.nexus.store.seed_metaflow(queued);
    fs::write(world.fresh.state.join("resume-refused"), "").unwrap();

    assert_eq!(
        restarted(&fixture),
        Response::RestartRefused(RestartRefusal::ResumeRefused(ResumeRefused_Data {
            flow_id: FLOW.into(),
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
    assert_eq!(node(&fixture).session_id, world.seat.session);
    assert_eq!(stored(&fixture, Model::Sol).queue.len(), 1);
    fixture.nexus.drain_awake(&address(Model::Sol));
    assert_eq!(received(&world, &fixture, "kept work"), 0, "custody kept");

    assert_eq!(
        fixture.nexus.dispatch(Query::Stop(FLOW.into())),
        Response::Stopped(FLOW.into())
    );
    assert_eq!(
        record(&fixture).map(|record| record.restart_phase),
        Some(RestartPhase::Failed(RestartFailure::Stopped))
    );
    assert_eq!(node(&fixture).flow_lifecycle, FlowLifecycle::Active);
    assert_eq!(stored(&fixture, Model::Sol).queue.len(), 1);

    fs::remove_file(world.fresh.state.join("resume-refused")).unwrap();
    assert_eq!(restarted(&fixture), Response::Restarted(FLOW.into()));
    assert_eq!(received(&world, &fixture, "kept work"), 1);
    assert_eq!(record(&fixture), None);
}

/// The old pane is already gone and no spawn is recorded: the restart
/// closes nothing and resumes.
#[test]
fn restart_harness_already_dead() {
    let mut fixture = NexusFixture::new();
    let world = fixture.restart_world(false);
    assert_eq!(record(&fixture), None, "no spawn is in flight");
    assert_eq!(restarted(&fixture), Response::Restarted(FLOW.into()));
    assert!(
        world.calls_naming("pane close").is_empty(),
        "{}",
        world.fresh.calls()
    );
    assert_eq!(world.resumes().len(), 1);
    assert_eq!(route_pane(&fixture), NEW_PANE);
}

/// A flow Flow does not hold is refused UnknownFlow, and nothing is closed.
#[test]
fn restart_unknown_flow() {
    let mut fixture = NexusFixture::new();
    let world = fixture.restart_world(true);
    assert_eq!(
        fixture.nexus.dispatch(Query::Restart("fade00".into())),
        Response::RestartRefused(RestartRefusal::UnknownFlow("fade00".into()))
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
    unheld.flow_id = "5e5510".into();
    unheld.session_id = String::new();
    unheld.herdr_route_selection = HerdrRouteSelection::Available(HerdrRoute {
        herdr_session_name: "fixture-session".into(),
        herdr_agent_name: "unheld".into(),
        herdr_pane_id: "w2:p9".into(),
        herdr_terminal_id: "term-unheld".into(),
    });
    assert!(matches!(
        fixture.nexus.store.register_flow(unheld).unwrap(),
        crate::store::FlowRegistration::Registered(_)
    ));
    assert_eq!(
        fixture.nexus.dispatch(Query::Restart("5e5510".into())),
        Response::RestartRefused(RestartRefusal::NoHeldSession("5e5510".into()))
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
        Response::RestartRefused(RestartRefusal::TranscriptAbsent(FLOW.into()))
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
            Response::RestartRefused(RestartRefusal::Busy(FLOW.into()))
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
        Response::RestartRefused(RestartRefusal::Busy(FLOW.into()))
    );
    assert_eq!(record(&fixture), Some(under_way));
    assert!(world.calls_naming("pane close").is_empty());
}

/// The resumed pane reports another session: ResumeRefused BindMismatch,
/// the record Failed, the pane discarded, and no new identity minted: the
/// flow keeps its session and no FlowId is claimed.
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
            flow_id: FLOW.into(),
            string: "BindMismatch".into(),
        }))
    );
    assert_eq!(
        record(&fixture).map(|record| record.restart_phase),
        Some(RestartPhase::Failed(RestartFailure::BindMismatch))
    );
    assert_eq!(
        world.calls_naming(&format!("pane close {NEW_PANE}")).len(),
        1
    );
    assert_eq!(node(&fixture).session_id, world.seat.session);
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
            attempts: 3,
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
/// another; Failed stays Failed.
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
        Response::RestartRefused(RestartRefusal::Interrupted(FLOW.into()))
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
        queued.queue = vec![signal_flow::QueueEntry {
            logical_delivery_id: "around-the-commit".into(),
            sent_by: signal_flow::SentBy::Unknown,
            delivery: flow_ethos::Delivery::Wake,
            message: signal_flow::Message::Soft(signal_flow::Request::Order(
                "queued around the commit".into(),
            )),
        }];
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
                        flow_id: FLOW.into(),
                        herdr_route: HerdrRoute {
                            herdr_session_name: "fixture-session".into(),
                            herdr_agent_name: "restart-agent".into(),
                            herdr_pane_id: NEW_PANE.into(),
                            herdr_terminal_id: "term-fresh".into(),
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
        assert!(stored(&fixture, Model::Sol).queue.is_empty());
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
        fixture.nexus.dispatch(Query::Stop(FLOW.into())),
        Response::StopRejected(StopRejection::SpawnUnproven)
    );
    assert_eq!(record(&fixture), Some(unknown));
    assert_eq!(
        fixture
            .nexus
            .dispatch_meta(meta_signal_flow::Query::Retire(FLOW.into())),
        meta_signal_flow::Response::RetireRejected(
            meta_signal_flow::RetireRejection::RestartUnresolved
        )
    );
    assert_eq!(node(&fixture).flow_lifecycle, FlowLifecycle::Active);
    assert_eq!(
        restarted(&fixture),
        Response::RestartRefused(RestartRefusal::Interrupted(FLOW.into()))
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
        fixture.nexus.dispatch(Query::Stop(FLOW.into())),
        Response::Stopped(FLOW.into())
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
    assert_eq!(restarted(&fixture), Response::Restarted(FLOW.into()));
}

/// The adopted seat's native session, which another Nexus chose, and the
/// FlowId `flow-id` claims for it.
const ADOPTED_SESSION: &str = "5e55a0d0-1b2c-5d3e-8f40-a1b2c3d4e5f6";
const ADOPTED: &str = "5e55a0";
/// The socket the adopted seat's hooks were configured with by the Nexus
/// that launched it.
const OLD_SOCKET: &str = "/run/old-place/flow/flow.sock";

/// A Claude seat another Nexus launched, resting in w1:p3 with its hooks
/// aimed at that Nexus's socket, and `{ Mind flow Sol }` declared on demand
/// here; nothing of it is known to this Nexus yet. Answers the world and
/// the old harness, a shell whose argv names the session and the old
/// socket, marked as Herdr marks a pane's processes, with a hook child
/// whose environment is scrubbed.
fn adopted_world(fixture: &mut NexusFixture) -> (FreshWorld, Process) {
    use crate::store::ManagesMetaflowState;
    let fresh = fixture.fresh_claude_world();
    fixture
        .nexus
        .store
        .configure_change(meta_signal_flow::ConfigurationChange::Metaflows(vec![
            meta_signal_flow::ConfigureMetaflow {
                root_option: None,
                address: address(Model::Sol),
                system_modules: vec!["spirit-core".into(), "intent-flow".into()],
                user_modules: vec!["operation-main-flow".into()],
                flow_kind: signal_flow::FlowKind::Regular,
                tool_set: signal_flow::ToolSet::Default,
                continuity: meta_signal_flow::Continuity::OnDemand,
                purpose: String::new(),
                records: vec![],
            },
        ]))
        .expect("metaflow declared");
    fs::write(fresh.state.join("restart-world"), "").unwrap();
    let transcript = fixture
        .directory
        .path()
        .join("native-transcripts/claude/-home-li-primary")
        .join(format!("{ADOPTED_SESSION}.jsonl"));
    fs::write(
        transcript,
        format!(
            "{}\n",
            serde_json::json!({
                "type": "assistant",
                "sessionId": ADOPTED_SESSION,
                "uuid": "adopted-end-turn",
                "message": { "stop_reason": "end_turn" }
            })
        ),
    )
    .unwrap();
    let hook = fresh.state.join("old-hook-pid");
    let settings = serde_json::json!({
        "hooks": { "Stop": [ { "hooks": [ {
            "type": "command",
            "command": format!("flow-hook --socket {OLD_SOCKET}")
        } ] } ] }
    })
    .to_string();
    let mut harness = std::process::Command::new("sh")
        .args(["-c", HARNESS])
        .arg(&hook)
        .args([
            "claude",
            "--session-id",
            ADOPTED_SESSION,
            "--settings",
            &settings,
        ])
        .env("HERDR_PANE_ID", OLD_PANE)
        .env("HERDR_SESSION", "fixture-session")
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("old harness");
    let pid = harness.id();
    fs::write(fresh.state.join("old-pid"), pid.to_string()).unwrap();
    // Reaped as soon as its pane is closed.
    std::thread::spawn(move || harness.wait());
    fs::write(fresh.state.join("old-pane"), OLD_PANE).unwrap();
    fs::write(
        fresh.state.join("old-agent"),
        serde_json::json!({
            "name": "old-nexus-agent",
            "agent": "claude",
            "agent_status": "idle",
            "workspace_id": "w1",
            "tab_id": "w1:t1",
            "pane_id": OLD_PANE,
            "terminal_id": "term-old",
            "interactive_ready": true,
            "agent_session": {
                "source": "herdr:claude", "agent": "claude",
                "kind": "id", "value": ADOPTED_SESSION,
            }
        })
        .to_string(),
    )
    .unwrap();
    await_path(&hook);
    let process = FlowStore::process_identity_for_pid(pid).expect("old harness running");
    (fresh, process)
}

/// Adopt then Restart, the cutover: a Claude seat another Nexus launched,
/// its hooks aimed at that Nexus's socket, is adopted here and restarted
/// from outside. The resumed seat is the same FlowId on the same native
/// session, `claude --resume` with its hooks aimed at this Nexus's socket and
/// not the old one; its hook's Stop reaches this Nexus, acts, and hands it
/// the queue, while a late Stop of the old seat's hook acts on nothing.
#[test]
fn restart_adopted_seat_resumes_hooked_to_this_nexus() {
    use crate::sending::BindsSeats;
    use crate::store::ReadsLaunchAttempt;
    let mut fixture = NexusFixture::new();
    let (fresh, old) = adopted_world(&mut fixture);
    assert!(
        fs::read_to_string(format!("/proc/{}/cmdline", old.pid))
            .unwrap()
            .contains(OLD_SOCKET),
        "the adopted seat's hooks aim at the old socket"
    );
    assert_eq!(
        fixture.nexus.adopt(
            signal_flow::Adopt_Data {
                address: address(Model::Sol),
                native_session_id: ADOPTED_SESSION.into(),
            },
            Some(&old),
        ),
        Response::Adopted(ADOPTED.into())
    );
    assert_eq!(
        fixture
            .nexus
            .store
            .launch_requests_bound_to(ADOPTED)
            .unwrap(),
        Vec::<String>::new(),
        "this Nexus never launched it"
    );
    let old_hook = CallerProcess {
        process_id: pid_in(&fresh.state.join("old-hook-pid")),
    };

    assert_eq!(
        fixture.nexus.dispatch(Query::Restart(ADOPTED.into())),
        Response::Restarted(ADOPTED.into())
    );

    let resumes: Vec<String> = fresh
        .calls()
        .lines()
        .filter(|line| line.contains("agent start") && line.contains("--resume"))
        .map(str::to_owned)
        .collect();
    assert_eq!(resumes.len(), 1, "{}", fresh.calls());
    let resume = &resumes[0];
    assert!(resume.contains(&format!("--pane {NEW_PANE}")), "{resume}");
    assert!(
        resume.contains(&format!("--resume {ADOPTED_SESSION}")),
        "{resume}"
    );
    assert!(
        resume.contains("/fixture/run/flow-next/flow/flow.sock"),
        "the resumed hooks report to this Nexus: {resume}"
    );
    assert!(!resume.contains(OLD_SOCKET), "{resume}");
    assert!(
        resume.contains(&format!("--name {{ Mind flow Sol {ADOPTED} }}")),
        "{resume}"
    );
    assert!(ended(&old), "the adopted harness exited");
    let after = fixture
        .nexus
        .store
        .flow_node(ADOPTED)
        .unwrap()
        .expect("the adopted flow");
    assert_eq!(after.session_id, ADOPTED_SESSION);
    assert_eq!(after.flow_lifecycle, FlowLifecycle::Active);
    let HerdrRouteSelection::Available(route) = after.herdr_route_selection else {
        panic!("the resumed seat is routed")
    };
    assert_eq!(route.herdr_pane_id, NEW_PANE);
    assert_eq!(
        stored(&fixture, Model::Sol).state,
        State::Awake(ADOPTED.into())
    );
    assert_eq!(fixture.nexus.store.restart_record(ADOPTED).unwrap(), None);

    let mut queued = stored(&fixture, Model::Sol);
    queued.queue = vec![signal_flow::QueueEntry {
        logical_delivery_id: "after-adopted-restart".into(),
        sent_by: signal_flow::SentBy::Unknown,
        delivery: flow_ethos::Delivery::Wake,
        message: signal_flow::Message::Soft(signal_flow::Request::Order(
            "for the resumed adopted seat".into(),
        )),
    }];
    fixture.nexus.store.seed_metaflow(queued);
    let stop = Event::Stopped(NativeStop {
        native_session_id_option: Some(ADOPTED_SESSION.into()),
        transcript_path_option: Some(
            fixture
                .directory
                .path()
                .join("native-transcripts/claude/-home-li-primary")
                .join(format!("{ADOPTED_SESSION}.jsonl"))
                .to_string_lossy()
                .into_owned(),
        ),
        last_assistant_message_option: None,
    });
    let reported = |caller: Option<CallerProcess>| {
        let acts = fixture.nexus.report_acts(ADOPTED, caller);
        assert_eq!(
            fixture.nexus.report(Report_Data {
                flow_id: ADOPTED.into(),
                event: stop.clone(),
            }),
            Response::Reported
        );
        if acts {
            fixture
                .nexus
                .after_attributed_report(&ADOPTED.to_owned(), &stop);
        }
        acts
    };
    let delivered = || {
        fresh
            .user_turns(&fixture)
            .iter()
            .filter(|turn| turn.contains("for the resumed adopted seat"))
            .count()
    };
    assert!(!reported(Some(old_hook)), "the old seat's hook is fenced");
    assert_eq!(delivered(), 0);
    let new_hook = CallerProcess {
        process_id: pid_in(&fresh.state.join("hook-pid")),
    };
    assert!(reported(Some(new_hook)), "the resumed seat's hook acts");
    assert_eq!(delivered(), 1);
    assert_eq!(
        fixture
            .nexus
            .store
            .seat_birth(ADOPTED)
            .unwrap()
            .map(|birth| birth.process),
        FlowStore::process_identity_for_pid(pid_in(&fresh.state.join("pid")))
    );
}

/// abc123's harness is working: the main turn a prompt opened runs, and
/// Herdr shows the agent working.
fn set_working(world: &RestartWorld, fixture: &NexusFixture) {
    append_turn_row(
        world,
        fixture,
        serde_json::json!({
            "type": "user",
            "sessionId": world.seat.session,
            "uuid": "a-turn-runs",
            "message": { "content": "work" }
        }),
    );
    let agent_path = world.fresh.state.join("old-agent");
    let mut agent: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(&agent_path).unwrap()).unwrap();
    agent["agent_status"] = "working".into();
    fs::write(&agent_path, agent.to_string()).unwrap();
}

/// The turn that ran ends: its transcript records the main turn's
/// end_turn, as Claude writes it before its Stop hook runs.
fn turn_ends(world: &RestartWorld, fixture: &NexusFixture) {
    append_turn_row(
        world,
        fixture,
        serde_json::json!({
            "type": "assistant",
            "sessionId": world.seat.session,
            "uuid": "the-turn-ends",
            "message": { "stop_reason": "end_turn", "content": [{ "type": "text", "text": "done" }] }
        }),
    );
}

fn append_turn_row(world: &RestartWorld, fixture: &NexusFixture, row: serde_json::Value) {
    let transcript = fixture
        .directory
        .path()
        .join("native-transcripts/claude/-home-li-primary")
        .join(format!("{}.jsonl", world.seat.session));
    let mut rows = fs::read_to_string(&transcript).unwrap();
    rows.push_str(&format!("{row}\n"));
    fs::write(&transcript, rows).unwrap();
}

/// A report as the ordinary socket serves it, owed restarts included: its
/// caller is attributed while it waits, the report recorded, then it acts
/// when attributed to the bound seat, or runs the restart it was owed for.
/// Whether it acted or ran.
fn socket_report(fixture: &NexusFixture, event: Event, caller: Option<CallerProcess>) -> bool {
    use crate::restarting::RestartsFlows;
    let acts = fixture.nexus.report_acts(FLOW, caller);
    let owed = fixture.nexus.owed_to(FLOW, caller);
    assert_eq!(
        fixture.nexus.report(Report_Data {
            flow_id: FLOW.into(),
            event: event.clone(),
        }),
        Response::Reported
    );
    if acts {
        fixture
            .nexus
            .after_attributed_report(&FLOW.to_owned(), &event);
        return true;
    }
    owed.is_some_and(|owed| fixture.nexus.restart_at_stop(FLOW, &event, owed))
}

/// A process marked in abc123's pane as Herdr marks a pane's processes,
/// but not born of its harness: another seat's hook.
struct StrangerHook(std::process::Child);

impl Drop for StrangerHook {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

impl StrangerHook {
    fn caller(&self) -> CallerProcess {
        CallerProcess {
            process_id: self.0.id(),
        }
    }
}

fn stranger_hook() -> StrangerHook {
    StrangerHook(
        std::process::Command::new("sleep")
            .arg("60")
            .env("HERDR_PANE_ID", OLD_PANE)
            .env("HERDR_SESSION", "fixture-session")
            .spawn()
            .expect("stranger hook"),
    )
}

/// The record abc123's owed restart holds: Owed, naming the birth of the
/// old harness.
fn owed_record(fixture: &NexusFixture, world: &RestartWorld) -> RestartRecord {
    let held = record(fixture).expect("a restart is owed");
    assert!(
        matches!(held.restart_phase, RestartPhase::Owed(_)),
        "{held:?}"
    );
    assert_eq!(held.old_seat.process_option, Some(world.old_harness()));
    assert_eq!(held.old_seat.herdr_route.herdr_pane_id, OLD_PANE);
    held
}

/// Nothing of the restart has run: no pane closed, no harness resumed, the
/// old harness running in its pane and the flow as it was.
fn nothing_ran(fixture: &NexusFixture, world: &RestartWorld, old: &Process) {
    assert!(world.calls_naming("pane close").is_empty());
    assert!(world.resumes().is_empty());
    assert!(world.calls_naming("workspace create").is_empty());
    assert!(!ended(old), "the harness still runs");
    assert_eq!(route_pane(fixture), OLD_PANE);
    assert_eq!(node(fixture).flow_lifecycle, FlowLifecycle::Active);
    assert_eq!(stored(fixture, Model::Sol).state, State::Awake(FLOW.into()));
}

/// The owed restart ran: the old pane closed once and the same session
/// resumed once in the new pane, bound, the record gone.
fn restart_ran(fixture: &NexusFixture, world: &RestartWorld, old: &Process) {
    assert_eq!(
        world.calls_naming(&format!("pane close {OLD_PANE}")).len(),
        1
    );
    let resumes = world.resumes();
    assert_eq!(resumes.len(), 1, "{}", world.fresh.calls());
    assert!(
        resumes[0].contains(&format!("--resume {}", world.seat.session)),
        "{}",
        resumes[0]
    );
    assert!(resumes[0].contains(&format!("--pane {NEW_PANE}")));
    assert!(ended(old), "the old harness exited");
    assert_eq!(record(fixture), None);
    assert_eq!(route_pane(fixture), NEW_PANE);
    let after = node(fixture);
    assert_eq!(after.session_id, world.seat.session);
    assert_eq!(after.flow_lifecycle, FlowLifecycle::Active);
    assert_eq!(stored(fixture, Model::Sol).state, State::Awake(FLOW.into()));
}

/// abc123 is working, asked from outside: RestartOwed at once, its record
/// Owed and nothing closed. A working event of its own seat keeps it owed;
/// a Stop no caller stands behind, a Stop of another seat marked in its
/// pane, and a Stop of its own seat that arrived before Owed was written
/// run nothing. Its own seat's first real Stop after Owed runs the
/// restart: the same session resumed in a new pane.
#[test]
fn restart_working_owed_until_stop() {
    let mut fixture = NexusFixture::new();
    let world = fixture.restart_world(true);
    let old = world.old_harness();
    let old_hook = world.old_hook();
    set_working(&world, &fixture);
    // A Stop of the seat's own hook that arrived before the restart was
    // asked is attributed to no owed restart.
    assert_eq!(fixture.nexus.owed_to(FLOW, Some(old_hook)), None);

    assert_eq!(restarted(&fixture), Response::RestartOwed(FLOW.into()));
    let owed = owed_record(&fixture, &world);
    nothing_ran(&fixture, &world, &old);

    // The seat goes on working: it stays owed.
    assert!(!socket_report(
        &fixture,
        Event::ToolUsed("Bash".into()),
        Some(old_hook)
    ));
    assert!(!socket_report(
        &fixture,
        Event::PromptSubmitted(signal_flow::NativePrompt {
            native_session_id_option: Some(world.seat.session.clone()),
            prompt_id_option: None,
            prompt_sha256: "work".into(),
        }),
        Some(old_hook)
    ));
    assert_eq!(record(&fixture), Some(owed.clone()));
    // A Stop is never made up from nothing, nor taken from another seat.
    assert!(!socket_report(&fixture, stop_of(&world, &fixture), None));
    let stranger = stranger_hook();
    assert!(!socket_report(
        &fixture,
        stop_of(&world, &fixture),
        Some(stranger.caller())
    ));
    assert_eq!(record(&fixture), Some(owed));
    // A second Restart while owed is Busy.
    assert_eq!(
        restarted(&fixture),
        Response::RestartRefused(RestartRefusal::Busy(FLOW.into()))
    );
    nothing_ran(&fixture, &world, &old);

    turn_ends(&world, &fixture);
    assert!(socket_report(
        &fixture,
        stop_of(&world, &fixture),
        Some(old_hook)
    ));
    restart_ran(&fixture, &world, &old);
    assert!(
        world.calls_naming("agent prompt").is_empty(),
        "nothing is typed into the resumed seat: {}",
        world.fresh.calls()
    );
}

/// abc123 asks for its own restart, its origin derived from the calling
/// process as the ordinary socket derives it: RestartOwed before anything
/// closes its pane, though Herdr shows it resting; then its own Stop runs
/// the restart as for a working seat.
#[test]
fn restart_self_owed_answer_before_close() {
    use crate::generated::starter::DerivesOrigin;
    use crate::restarting::RestartsFlows;
    use crate::store::SeedsRole;
    let mut fixture = NexusFixture::new();
    let world = fixture.restart_world(true);
    let old = world.old_harness();
    let old_hook = world.old_hook();
    // abc123's role, as its launch registered it: what a caller in its pane
    // is resolved to.
    fixture.nexus.store.seed_role(signal_flow::Caller {
        flow_id: FLOW.into(),
        aspect: Aspect::Mind,
        model_name: "claude-fixture".into(),
    });
    assert_eq!(
        old_hook.caller_pane(&fixture.nexus.herdr),
        Some(CallerPane {
            herdr_session_name: "fixture-session".into(),
            herdr_pane_id: OLD_PANE.into(),
        })
    );
    let origin = fixture.nexus.origin_of(Some(old_hook));
    assert_eq!(
        origin,
        signal_flow::Origin::Flow(signal_flow::FlowOrigin {
            flow_id: FLOW.into(),
            session_id: world.seat.session.clone(),
        })
    );

    assert_eq!(
        fixture.nexus.restart(FLOW, &origin),
        Response::RestartOwed(FLOW.into())
    );
    owed_record(&fixture, &world);
    nothing_ran(&fixture, &world, &old);

    assert!(socket_report(
        &fixture,
        stop_of(&world, &fixture),
        Some(old_hook)
    ));
    restart_ran(&fixture, &world, &old);
}

/// abc123, past its model's refresh threshold, is owed a restart: its
/// correlated Stop restarts it and does not refresh it. Only a later real
/// Stop, of the resumed seat, refreshes it.
#[test]
fn restart_owed_before_threshold_refresh() {
    use crate::store::ManagesMetaflowState;
    use crate::store::context::RecordsContextSize;
    let mut fixture = NexusFixture::new();
    let world = fixture.restart_world(true);
    let old = world.old_harness();
    let old_hook = world.old_hook();
    fixture
        .nexus
        .store
        .configure_change(meta_signal_flow::ConfigurationChange::Thresholds(vec![
            meta_signal_flow::ConfigureThreshold {
                model_name: "claude-fixture".into(),
                refresh: 100_000,
                warning: 0,
            },
        ]))
        .expect("threshold configured");
    fixture
        .nexus
        .store
        .record_context_size(FLOW, 150_000)
        .unwrap();
    assert!(matches!(
        crate::store::context::JudgesThreshold::past_threshold(&fixture.nexus.store, FLOW),
        Ok(true)
    ));
    set_working(&world, &fixture);
    assert_eq!(restarted(&fixture), Response::RestartOwed(FLOW.into()));

    turn_ends(&world, &fixture);
    assert!(socket_report(
        &fixture,
        stop_of(&world, &fixture),
        Some(old_hook)
    ));
    restart_ran(&fixture, &world, &old);
    assert!(
        world
            .calls_naming(&format!("pane close {NEW_PANE}"))
            .is_empty(),
        "the resumed seat is not refreshed at the Stop that restarted it"
    );

    // The resumed seat's own Stop refreshes it: an OnDemand seat with
    // nothing queued is closed, its metaflow Asleep.
    assert!(socket_report(
        &fixture,
        stop_of(&world, &fixture),
        Some(world.new_hook())
    ));
    assert_eq!(
        world.calls_naming(&format!("pane close {NEW_PANE}")).len(),
        1,
        "{}",
        world.fresh.calls()
    );
    assert_eq!(stored(&fixture, Model::Sol).state, State::Asleep);
}

/// A Send while abc123's restart is owed queues and reaches no pane, nor
/// does its old seat's Stop hand it over; once the restart has run at that
/// Stop the queue is handed to the resumed seat, exactly once.
#[test]
fn restart_owed_before_drain() {
    let mut fixture = NexusFixture::new();
    let world = fixture.restart_world(true);
    let old = world.old_harness();
    let old_hook = world.old_hook();
    let from = bound_sender(&fixture);
    set_working(&world, &fixture);
    assert_eq!(restarted(&fixture), Response::RestartOwed(FLOW.into()));

    let queued = send(&fixture, &from, "arrived while owed");
    assert!(
        matches!(
            &queued,
            Response::SendAccepted(acceptance)
                if acceptance.visible_delivery_state == signal_flow::VisibleDeliveryState::Queued
        ),
        "{queued:?}"
    );
    assert_eq!(stored(&fixture, Model::Sol).queue.len(), 1);
    assert!(world.calls_naming("agent prompt").is_empty());

    turn_ends(&world, &fixture);
    assert!(socket_report(
        &fixture,
        stop_of(&world, &fixture),
        Some(old_hook)
    ));
    restart_ran(&fixture, &world, &old);
    assert_eq!(received(&world, &fixture, "arrived while owed"), 1);
    let prompts = world.calls_naming("agent prompt");
    assert!(!prompts.is_empty(), "{}", world.fresh.calls());
    assert!(
        prompts.iter().all(|line| !line.contains(OLD_PANE)),
        "the old seat receives nothing: {}",
        world.fresh.calls()
    );
    assert!(stored(&fixture, Model::Sol).queue.is_empty());
    fixture.nexus.drain_awake(&address(Model::Sol));
    assert_eq!(received(&world, &fixture, "arrived while owed"), 1);
}

/// A Nexus that dies while abc123's restart is owed: the Nexus reopened
/// finds it Owed as it was written, closes and resumes nothing, hands the
/// old seat nothing, and still waits for a real Stop; its seat's Stop then
/// runs it.
#[test]
fn restart_owed_nexus_death_still_waits() {
    let mut fixture = NexusFixture::new();
    let world = fixture.restart_world(true);
    let old = world.old_harness();
    let old_hook = world.old_hook();
    let from = bound_sender(&fixture);
    set_working(&world, &fixture);
    assert_eq!(restarted(&fixture), Response::RestartOwed(FLOW.into()));
    let owed = owed_record(&fixture, &world);
    let _ = send(&fixture, &from, "queued before the death");

    fixture.reopen_store();
    fixture.nexus.recover_wakes();
    assert_eq!(record(&fixture), Some(owed.clone()));
    nothing_ran(&fixture, &world, &old);
    assert!(world.calls_naming("agent prompt").is_empty());
    assert_eq!(stored(&fixture, Model::Sol).queue.len(), 1);
    assert!(!socket_report(&fixture, stop_of(&world, &fixture), None));
    assert_eq!(record(&fixture), Some(owed));

    turn_ends(&world, &fixture);
    assert!(socket_report(
        &fixture,
        stop_of(&world, &fixture),
        Some(old_hook)
    ));
    restart_ran(&fixture, &world, &old);
    assert_eq!(received(&world, &fixture, "queued before the death"), 1);
}

/// Inspect shows abc123's owed restart, Owed with the report position it
/// was written at, and changes nothing.
#[test]
fn restart_inspect_shows_owed() {
    let mut fixture = NexusFixture::new();
    let world = fixture.restart_world(true);
    let old = world.old_harness();
    set_working(&world, &fixture);
    assert_eq!(restarted(&fixture), Response::RestartOwed(FLOW.into()));
    let owed = owed_record(&fixture, &world);
    let inspect = || {
        let meta_signal_flow::Response::Inspected(inspected) = fixture
            .nexus
            .dispatch_meta(meta_signal_flow::Query::Inspect(address(Model::Sol)))
        else {
            panic!("the metaflow is inspected")
        };
        inspected.restart_record_option
    };
    assert_eq!(inspect(), Some(owed.clone()));
    assert_eq!(inspect(), Some(owed.clone()));
    assert_eq!(record(&fixture), Some(owed));
    nothing_ran(&fixture, &world, &old);
}

/// abc123's metaflow is Asleep, and a restart is asked from outside: it is
/// refused Unsupported, with no record written, nothing closed or resumed.
/// The running-seat cutover claims no Asleep support.
#[test]
fn restart_asleep_outside_refused_unsupported() {
    let mut fixture = NexusFixture::new();
    let world = fixture.restart_world(true);
    let old = world.old_harness();
    fixture
        .nexus
        .store
        .seed_metaflow(metaflow(Model::Sol, State::Asleep, vec![], vec![]));

    assert_eq!(
        restarted(&fixture),
        Response::RestartRefused(RestartRefusal::Unsupported(FLOW.into()))
    );
    assert_eq!(record(&fixture), None);
    assert!(world.calls_naming("pane close").is_empty());
    assert!(world.resumes().is_empty());
    assert!(world.calls_naming("workspace create").is_empty());
    assert!(!ended(&old), "the harness still runs");
    assert_eq!(route_pane(&fixture), OLD_PANE);
    assert_eq!(stored(&fixture, Model::Sol).state, State::Asleep);
}
