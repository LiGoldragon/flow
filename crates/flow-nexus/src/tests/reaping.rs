//! Flow 0.54.7's acceptance tests for what a launch owes once its successor
//! is confirmed: routing by the Address's succession, the reap owed at once
//! and finished outside the dispatch gate by its worker, the fresh-start
//! handover and its supersession, Inspect, the crash points and the failed
//! writes of safeguards-0547.md, and the worker's races. Each test names the
//! plan item it proves (plan-0547-final.md).
//!
//! The Herdr stand-ins: a fake `herdr` executable that answers `session
//! list`, `api snapshot` and `pane close` from files the test sets, logs
//! every call in order, can block until killed, and can hand each reap call
//! to a probe that reads whether the dispatch gate is free while the call
//! runs; and a Herdr socket that answers `events.subscribe`, can withhold
//! its acknowledgement, emits `pane_closed`, closes its streams, counts
//! subscriptions and logs each in the same order as the executable.

use super::{NexusFixture, StagedLaunch};
use crate::fixture_executable::{FixtureExecutable, InstallsScript};
use crate::reaping::{
    ArmedReap, PaneWatchEnd, PaneWatchLapse, ServesReapedPaneWatches, TakesUpObligations,
    WatchesReapedPane,
};
use crate::store::AltersFlowRowsForTests;
use crate::store::LaunchOutcome;
use crate::store::metaflow::{ReadsSuccession, Succession};
use crate::store::pending_handover::RecordsPendingHandovers;
use crate::store::pending_reap::RecordsPendingReaps;
use crate::store::{
    AnnouncesLaunchChanges, ReadsFlowRows, RecordsFlowLifecycle, RecordsLaunchOutcome,
};
use crate::store::{ConfirmsStartedFlow, OpensFlowStore, ReadsFlowRoles};
use crate::test_faults::{CrashPoint, InjectsCrashes, RefusedWrite};
use crate::waking::SleepsAndWakesSeats;
use crate::{Dispatches, HoldsStopRequest};
use signal_flow::{
    FlowLifecycle, HerdrRouteSelection, LaunchStatusOutcome, Query, ReapStanding, Response,
};
use std::collections::BTreeMap;
use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

pub(crate) const PREDECESSOR: &str = "fac697";
pub(crate) const SUCCESSOR: &str = "908786";
const PREDECESSOR_PANE: &str = "w1:p3";

/// Every row of every Flow family, sorted by key, and every catalog entry,
/// read from the closed store with no Nexus over it: the logical-rows-and-
/// catalog oracle of 0.54.6. Physical file bytes are never compared.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LogicalState {
    pub(crate) tables: BTreeMap<String, Vec<(String, Vec<u8>)>>,
    pub(crate) catalog: BTreeMap<String, Vec<u8>>,
}

impl LogicalState {
    pub(crate) fn read(path: &Path) -> Self {
        use redb::{ReadableDatabase as _, ReadableTable as _, TableHandle as _};
        let database = redb::ReadOnlyDatabase::open(path).expect("closed store opens read-only");
        let read = database.begin_read().expect("read transaction");
        let mut tables = BTreeMap::new();
        let names = read
            .list_tables()
            .expect("tables listed")
            .map(|table| table.name().to_owned())
            .collect::<Vec<_>>();
        for name in names {
            if !name.starts_with("flow_nexus_") {
                continue;
            }
            let definition = redb::TableDefinition::<String, &[u8]>::new(&name);
            let Ok(table) = read.open_table(definition) else {
                continue;
            };
            let mut rows = table
                .iter()
                .expect("rows read")
                .map(|row| {
                    let (key, value) = row.expect("row read");
                    (key.value(), value.value().to_vec())
                })
                .collect::<Vec<_>>();
            rows.sort();
            tables.insert(name, rows);
        }
        let catalog_table = read
            .open_table(redb::TableDefinition::<&str, &[u8]>::new(
                "__sema_engine_catalog",
            ))
            .expect("catalog read");
        let catalog = catalog_table
            .iter()
            .expect("catalog entries")
            .map(|entry| {
                let (key, value) = entry.expect("catalog entry");
                (key.value().to_owned(), value.value().to_vec())
            })
            .collect();
        Self { tables, catalog }
    }

    /// The rows of one table.
    pub(crate) fn rows(&self, table: &str) -> &[(String, Vec<u8>)] {
        self.tables.get(table).map(Vec::as_slice).unwrap_or(&[])
    }
}

/// The Herdr socket the reap worker subscribes on; a clone is a handle on
/// the same socket.
#[derive(Clone)]
pub(crate) struct HerdrEventsServer {
    shared: Arc<EventsShared>,
}

struct EventsShared {
    state: Mutex<EventsState>,
    changed: Condvar,
    log: PathBuf,
}

#[derive(Default)]
struct EventsState {
    subscriptions: usize,
    /// Streams whose subscriber closed its end.
    closed_by_subscriber: usize,
    withhold: bool,
    emitted: Vec<String>,
    close_streams: bool,
}

impl HerdrEventsServer {
    /// Serves `listener` from a thread of its own; each subscription is
    /// logged in `log`, the executable's call log.
    pub(crate) fn serving(listener: UnixListener, log: PathBuf) -> Self {
        let shared = Arc::new(EventsShared {
            state: Mutex::new(EventsState::default()),
            changed: Condvar::new(),
            log,
        });
        let serving = shared.clone();
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(stream) = stream else { return };
                let connection = serving.clone();
                std::thread::spawn(move || connection.serve(stream));
            }
        });
        Self { shared }
    }

    pub(crate) fn withhold_acknowledgement(&self) {
        self.shared.state.lock().unwrap().withhold = true;
    }

    pub(crate) fn release_acknowledgement(&self) {
        self.shared.state.lock().unwrap().withhold = false;
        self.shared.changed.notify_all();
    }

    /// Herdr's own event that the pane went, on every stream.
    pub(crate) fn emit_gone(&self, pane: &str) {
        let line = serde_json::json!({"event": "pane_closed", "data": {"pane_id": pane}});
        self.shared
            .state
            .lock()
            .unwrap()
            .emitted
            .push(line.to_string());
        self.shared.changed.notify_all();
    }

    pub(crate) fn close_streams(&self) {
        self.shared.state.lock().unwrap().close_streams = true;
        self.shared.changed.notify_all();
    }

    pub(crate) fn subscriptions(&self) -> usize {
        self.shared.state.lock().unwrap().subscriptions
    }

    /// Waits, bounded, until `holds` answers true of the server's state.
    pub(crate) fn wait_for(&self, what: &str, holds: impl Fn(usize, usize) -> bool) {
        let deadline = Instant::now() + Duration::from_secs(20);
        let mut state = self.shared.state.lock().unwrap();
        while !holds(state.subscriptions, state.closed_by_subscriber) {
            let remaining = deadline
                .checked_duration_since(Instant::now())
                .unwrap_or_else(|| panic!("the Herdr socket never saw {what}"));
            state = self
                .shared
                .changed
                .wait_timeout(state, remaining)
                .unwrap()
                .0;
        }
    }
}

trait ServesEventsStream {
    fn serve(&self, stream: UnixStream);
}

impl ServesEventsStream for EventsShared {
    fn serve(&self, stream: UnixStream) {
        let mut reader = BufReader::new(stream.try_clone().unwrap());
        let mut request = String::new();
        if reader.read_line(&mut request).unwrap_or(0) == 0 {
            return;
        }
        assert!(request.contains("events.subscribe"), "{request}");
        append(&self.log, "events.subscribe");
        {
            let mut state = self.state.lock().unwrap();
            state.subscriptions += 1;
            self.changed.notify_all();
            while state.withhold {
                state = self.changed.wait(state).unwrap();
            }
        }
        let mut writer = stream.try_clone().unwrap();
        if writer
            .write_all(b"{\"id\":\"flow-nexus:reaped-pane\",\"result\":{\"type\":\"subscription_started\"}}\n")
            .is_err()
        {
            return;
        }
        // The subscriber's own close is read on a thread of its own.
        let closing = stream.try_clone().unwrap();
        let (closed_sender, closed_receiver) = std::sync::mpsc::channel::<()>();
        std::thread::spawn(move || {
            let mut bytes = [0_u8; 64];
            let mut closing = closing;
            loop {
                match std::io::Read::read(&mut closing, &mut bytes) {
                    Ok(0) | Err(_) => break,
                    Ok(_) => {}
                }
            }
            let _ = closed_sender.send(());
        });
        let mut sent = 0;
        loop {
            let mut state = self.state.lock().unwrap();
            loop {
                if closed_receiver.try_recv().is_ok() {
                    state.closed_by_subscriber += 1;
                    self.changed.notify_all();
                    return;
                }
                if state.emitted.len() > sent || state.close_streams {
                    break;
                }
                state = self
                    .changed
                    .wait_timeout(state, Duration::from_millis(50))
                    .unwrap()
                    .0;
            }
            let lines = state.emitted[sent..].to_vec();
            let close = state.close_streams;
            drop(state);
            for line in lines {
                sent += 1;
                if writer.write_all(format!("{line}\n").as_bytes()).is_err() {
                    return;
                }
            }
            if close {
                let _ = writer.shutdown(std::net::Shutdown::Both);
                return;
            }
        }
    }
}

fn append(log: &Path, line: &str) {
    let mut file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(log)
        .expect("call log opens");
    writeln!(file, "{line}").expect("call log written");
}

/// What the fake `herdr` answers for the predecessor's pane.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PaneShown {
    /// The predecessor's own agent is there.
    Present,
    /// The pane is gone.
    Absent,
    /// The predecessor's pane id runs another native session.
    Occupied,
    /// Herdr cannot be read.
    Unreadable,
}

/// One world of a replacement: the predecessor fac697 at w1:p3 in the
/// session `messaging-build`, its staged successor 908786 at w1:p1 in
/// `fixture-session`, a fake Herdr executable and a Herdr socket.
pub(super) struct ReapWorld {
    pub(super) fixture: NexusFixture,
    pub(super) launch: StagedLaunch,
    pub(super) events: HerdrEventsServer,
    pub(super) log: PathBuf,
}

/// The files the fake Herdr reads, each in the fixture's directory.
trait NamesHerdrFiles {
    fn file(&self, name: &str) -> PathBuf;
}

impl NamesHerdrFiles for NexusFixture {
    fn file(&self, name: &str) -> PathBuf {
        self.directory.path().join(name)
    }
}

pub(super) trait StagesReap {
    /// A world whose launch `id` replaces fac697, staged to its receipt.
    fn staged(id: &str) -> Self;
    /// What the next `api snapshot` shows of the predecessor's pane.
    fn show(&self, shown: PaneShown);
    /// The exit status of the next `pane close`.
    fn close_exits(&self, status: i32);
    /// Makes the next snapshot, or close, block until killed.
    fn block_snapshot(&self, block: bool);
    fn block_close(&self, block: bool);
    /// Every Herdr call, and each subscription, in order.
    fn calls(&self) -> Vec<String>;
    fn closes(&self) -> usize;
    fn snapshots_of_predecessor(&self) -> usize;
    /// The Replace of the staged launch.
    fn replace(&self) -> Response;
    fn launch_status(&self) -> LaunchStatusOutcome;
    fn standing(&self) -> Option<ReapStanding>;
    fn lifecycle(&self, flow_id: &str) -> FlowLifecycle;
    fn vet(&self, flow_id: &str) -> meta_signal_flow::Response;
    /// Waits, bounded, for the store to show `holds`.
    fn until(&self, what: &str, holds: &mut dyn FnMut() -> bool);
}

impl StagesReap for ReapWorld {
    fn staged(id: &str) -> Self {
        use crate::store::metaflow::SeedsMetaflow;
        let fixture = NexusFixture::new();
        fixture.register_predecessor(PREDECESSOR);
        let mut launch = fixture.staged_launch(id, Some(PREDECESSOR));
        // The Address receives as the predecessor before its successor's
        // launch takes it.
        fixture.nexus.store.seed_metaflow(signal_flow::Metaflow {
            address: launch.profile.address.clone(),
            state: flow_ethos::State::Awake(signal_flow::FlowId(PREDECESSOR.into())),
            past: flow_ethos::Past(vec![]),
            queue: flow_ethos::Queue(vec![]),
        });
        launch.stage_to_ambiguity(&fixture.nexus);
        launch.write_receipt();
        let log = fixture.file("herdr-calls.log");
        let listener = UnixListener::bind(fixture.file("herdr-events.sock"))
            .expect("the fixture Herdr socket binds");
        let events = HerdrEventsServer::serving(listener, log.clone());
        fixture.list_herdr_sessions(&[
            ("messaging-build", fixture.file("herdr-events.sock")),
            (
                "fixture-session",
                fixture.file("herdr-fixture-session.sock"),
            ),
        ]);
        let world = Self {
            fixture,
            launch,
            events,
            log,
        };
        world.install_herdr();
        world.show(PaneShown::Present);
        world.close_exits(0);
        world
    }

    fn show(&self, shown: PaneShown) {
        let mut agents = vec![self.fixture.successor_agent()];
        match shown {
            PaneShown::Present => agents.push(self.fixture.current_agent()),
            PaneShown::Absent => {}
            PaneShown::Occupied => {
                let mut other = self.fixture.current_agent();
                other["agent_session"]["value"] = "another-native-session".into();
                agents.push(other);
            }
            PaneShown::Unreadable => {
                fs::write(self.fixture.file("snapshot.json"), "unreadable").unwrap();
                return;
            }
        }
        let snapshot = serde_json::json!({
            "id":"cli:api:snapshot",
            "result":{"snapshot":{"agents":agents,"protocol":20,"version":"0.8.2"},
            "type":"session_snapshot"}
        });
        fs::write(self.fixture.file("snapshot.json"), snapshot.to_string()).unwrap();
    }

    fn close_exits(&self, status: i32) {
        fs::write(self.fixture.file("close-status"), status.to_string()).unwrap();
    }

    fn block_snapshot(&self, block: bool) {
        let flag = self.fixture.file("block-snapshot");
        if block {
            fs::write(flag, "").unwrap();
        } else {
            let _ = fs::remove_file(flag);
        }
    }

    fn block_close(&self, block: bool) {
        let flag = self.fixture.file("block-close");
        if block {
            fs::write(flag, "").unwrap();
        } else {
            let _ = fs::remove_file(flag);
        }
    }

    fn calls(&self) -> Vec<String> {
        fs::read_to_string(&self.log)
            .unwrap_or_default()
            .lines()
            .map(str::to_owned)
            .collect()
    }

    fn closes(&self) -> usize {
        self.calls()
            .iter()
            .filter(|call| call.contains(&format!("pane close {PREDECESSOR_PANE}")))
            .count()
    }

    fn snapshots_of_predecessor(&self) -> usize {
        self.calls()
            .iter()
            .filter(|call| call.as_str() == "--session messaging-build api snapshot")
            .count()
    }

    fn replace(&self) -> Response {
        self.fixture
            .nexus
            .dispatch(Query::Replace(self.launch.request()))
    }

    fn launch_status(&self) -> LaunchStatusOutcome {
        match self.fixture.nexus.dispatch(Query::LaunchStatus(
            self.launch.profile.launch_request_id.clone(),
        )) {
            Response::LaunchInspected(inspection) => inspection
                .launch_status_outcome_option
                .expect("the launch has an outcome"),
            other => panic!("LaunchStatus answered {other:?}"),
        }
    }

    fn standing(&self) -> Option<ReapStanding> {
        self.fixture
            .nexus
            .store
            .pending_reap(PREDECESSOR)
            .unwrap()
            .map(|row| row.reap_standing)
    }

    fn lifecycle(&self, flow_id: &str) -> FlowLifecycle {
        self.fixture.lifecycle(flow_id)
    }

    fn vet(&self, flow_id: &str) -> meta_signal_flow::Response {
        self.fixture
            .nexus
            .dispatch_meta(meta_signal_flow::Query::Vet(
                meta_signal_flow::DeliveryRequest {
                    delivery_id: meta_signal_flow::DeliveryId("vet".into()),
                    flow_id: signal_flow::FlowId(flow_id.into()),
                    message: super::middle_abrupt("vetted"),
                },
            ))
    }

    fn until(&self, what: &str, holds: &mut dyn FnMut() -> bool) {
        // The store file and the Herdr call log are both in the fixture's
        // directory: each commit and each Herdr call is a change there.
        use crate::event_wait::AwaitsDirectory;
        assert!(
            self.fixture
                .directory
                .path()
                .until_holds(Instant::now() + Duration::from_secs(20), holds),
            "never: {what}"
        );
    }
}

trait InstallsReapHerdr {
    fn install_herdr(&self);
}

impl InstallsReapHerdr for ReapWorld {
    fn install_herdr(&self) {
        let file = |name: &str| self.fixture.file(name).display().to_string();
        let agent = serde_json::json!({"result":{"agent":{
            "name":"fixture-agent","agent":"codex","workspace_id":"fixture-workspace",
            "pane_id":"w1:p1","terminal_id":"fixture-terminal",
            "agent_session":{"source":"herdr:codex","agent":"codex","kind":"id",
                "value":super::STAGED_SESSION}
        }}});
        let probe = format!(
            "[ -p '{request}' ] && {{ printf '%s\\n' \"$*\" > '{request}'; read reply < '{reply}'; }}",
            request = file("gate-probe.request"),
            reply = file("gate-probe.reply"),
        );
        let body = format!(
            "#!/bin/sh\nprintf '%s\\n' \"$*\" >> '{log}'\n[ \"$1 $2 $3\" = \"session list --json\" ] && {{ cat '{sessions}'; exit 0; }}\ncase \"$*\" in\n  \"--session fixture-session agent get w1:p1\") printf '%s\\n' '{agent}' ;;\n  *\" api snapshot\") {probe}; [ -e '{block_snapshot}' ] && exec sleep 600; [ \"$(cat '{snapshot}')\" = unreadable ] && exit 1; cat '{snapshot}' ;;\n  *\" pane close \"*) {probe}; [ -e '{block_close}' ] && exec sleep 600; exit \"$(cat '{close}')\" ;;\n  *\" agent read \"*) printf '%s\\n' '› ' ;;\n  *) exit 64 ;;\nesac\n",
            log = self.log.display(),
            sessions = file("herdr-sessions.json"),
            agent = agent,
            probe = probe,
            block_snapshot = file("block-snapshot"),
            snapshot = file("snapshot.json"),
            block_close = file("block-close"),
            close = file("close-status"),
        );
        FixtureExecutable {
            path: self.fixture.snapshot_program.clone(),
        }
        .install(&body);
    }
}

/// The reap calls the fake Herdr hands to the probe, each with whether the
/// dispatch gate was free while it ran.
pub(super) struct GateProbe {
    seen: Arc<Mutex<Vec<(String, bool)>>>,
    request: PathBuf,
}

pub(super) trait ProbesGate {
    /// Arms the probe on `world`; its thread lives in `scope`.
    fn armed<'scope, 'env>(
        world: &'env ReapWorld,
        scope: &'scope std::thread::Scope<'scope, 'env>,
    ) -> Self;
    fn seen(&self) -> Vec<(String, bool)>;
    fn stop(&self);
}

impl ProbesGate for GateProbe {
    fn armed<'scope, 'env>(
        world: &'env ReapWorld,
        scope: &'scope std::thread::Scope<'scope, 'env>,
    ) -> Self {
        use std::os::unix::ffi::OsStrExt;
        let request = world.fixture.file("gate-probe.request");
        let reply = world.fixture.file("gate-probe.reply");
        for path in [&request, &reply] {
            let name = std::ffi::CString::new(path.as_os_str().as_bytes()).unwrap();
            // SAFETY: mkfifo reads a zero-terminated path that outlives it.
            assert_eq!(unsafe { libc::mkfifo(name.as_ptr(), 0o600) }, 0);
        }
        let seen = Arc::new(Mutex::new(Vec::new()));
        let recorded = seen.clone();
        let nexus = &world.fixture.nexus;
        let (requests, replies) = (request.clone(), reply.clone());
        // The probe holds both FIFOs open read and write, so no open of
        // either ever blocks and what is written stays until it is read.
        let open = |path: &Path| {
            fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(path)
                .expect("probe FIFO opens")
        };
        let request_end = open(&requests);
        let mut reply_end = open(&replies);
        scope.spawn(move || {
            let mut lines = BufReader::new(request_end);
            loop {
                let mut call = String::new();
                if lines.read_line(&mut call).unwrap_or(0) == 0 {
                    return;
                }
                let call = call.trim().to_owned();
                if call == "stop" {
                    return;
                }
                let free = nexus.dispatch_gate.try_lock().is_ok();
                recorded.lock().unwrap().push((call, free));
                let _ = reply_end.write_all(b"ok\n");
            }
        });
        Self { seen, request }
    }

    fn seen(&self) -> Vec<(String, bool)> {
        self.seen.lock().unwrap().clone()
    }

    fn stop(&self) {
        if let Ok(mut request) = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&self.request)
        {
            let _ = request.write_all(b"stop\n");
        }
        let _ = fs::remove_file(&self.request);
    }
}

/// Runs the reap supervisor for the duration of `body`, then stops the
/// Nexus and joins it; the ends of every worker it joined. Every worker
/// this supervisor spawned is joined exactly once.
pub(super) fn with_supervisor(
    fixture: &NexusFixture,
    body: impl FnOnce(),
) -> Vec<(String, PaneWatchEnd)> {
    let hooks = &fixture.nexus.pane_watch_hooks;
    hooks.spawned.lock().unwrap().clear();
    hooks.joined.lock().unwrap().clear();
    std::thread::scope(|scope| {
        let nexus = &fixture.nexus;
        let supervisor = scope.spawn(move || nexus.serve_reaped_pane_watches(scope));
        // The stop is asked whatever the body came to, so the scope joins.
        struct StopsOnDrop<'nexus>(&'nexus crate::RunningNexus);
        impl Drop for StopsOnDrop<'_> {
            fn drop(&mut self) {
                self.0.stopping.request();
                self.0.store.launch_changes.announce();
            }
        }
        let stops = StopsOnDrop(nexus);
        body();
        drop(stops);
        supervisor.join().expect("the supervisor returns");
    });
    let mut spawned = hooks.spawned.lock().unwrap().clone();
    let mut joined = hooks.joined.lock().unwrap().clone();
    spawned.sort();
    joined.sort();
    assert_eq!(joined, spawned, "every worker is joined exactly once");
    hooks.ends.lock().unwrap().clone()
}

/// The older tests' way to see a pending reap settled: the reap worker run
/// to its end for each owed row, on a Herdr socket that shows the pane's
/// going right after the subscription.
pub(super) fn finish_reaps(fixture: &NexusFixture) {
    use crate::observation_lifetime::{CancelsObservation, ObservationCancellation};
    let listener = fixture
        ._herdr_server
        .try_clone()
        .expect("the fixture Herdr socket");
    let events = HerdrEventsServer::serving(listener, fixture.file("herdr-calls.log"));
    for row in fixture.nexus.store.pending_reaps().unwrap() {
        let node = fixture
            .nexus
            .store
            .flow_node(&row.predecessor.0)
            .unwrap()
            .expect("the predecessor's row");
        let route = match node.herdr_route_selection {
            HerdrRouteSelection::Available(route) => Some(route),
            HerdrRouteSelection::Unavailable => None,
        };
        if let Some(route) = &route {
            events.emit_gone(&route.herdr_pane_id.0);
        }
        let cancellation =
            ObservationCancellation::open(fixture.nexus.store.launch_changes.clone()).unwrap();
        let end = fixture.nexus.watch_reaped_pane(
            &row.predecessor.0,
            &ArmedReap {
                launch_request_id: row.launch_request_id.clone(),
                route,
            },
            &cancellation,
        );
        assert_eq!(
            end,
            PaneWatchEnd::Acted,
            "the reap of {:?}",
            row.predecessor
        );
    }
}

fn replaced_answer(world: &ReapWorld, standing: ReapStanding) -> Response {
    Response::ReapPending(signal_flow::ReapPending {
        address: world.launch.profile.address.clone(),
        predecessor: signal_flow::Predecessor(signal_flow::FlowId(PREDECESSOR.into())),
        launched: match world.replace_answer_launched() {
            Some(launched) => launched,
            None => panic!("no launched successor"),
        },
        reap_standing: standing,
    })
}

trait ReadsLaunchedSuccessor {
    fn replace_answer_launched(&self) -> Option<signal_flow::Launched>;
}

impl ReadsLaunchedSuccessor for ReapWorld {
    fn replace_answer_launched(&self) -> Option<signal_flow::Launched> {
        let node = self.fixture.nexus.store.flow_node(SUCCESSOR).ok()??;
        Some(signal_flow::Launched {
            flow_id: node.flow_id,
            session_id: node.session_id,
            herdr_route_selection: node.herdr_route_selection,
            origin: node.origin,
        })
    }
}

fn is_reap_pending(response: &Response, standing: ReapStanding) -> bool {
    matches!(response, Response::ReapPending(pending)
        if pending.predecessor.0.0 == PREDECESSOR
            && pending.launched.flow_id.0 == SUCCESSOR
            && pending.reap_standing == standing)
}

fn vetted(response: &meta_signal_flow::Response) -> bool {
    matches!(response, meta_signal_flow::Response::Vetted)
}

fn delivery_rejected(
    response: &meta_signal_flow::Response,
    rejection: meta_signal_flow::DeliveryRejection,
) -> bool {
    matches!(response, meta_signal_flow::Response::DeliveryRejected(found) if *found == rejection)
}

fn inspect(world: &ReapWorld) -> Vec<meta_signal_flow::VisibleAttempt> {
    match world
        .fixture
        .nexus
        .dispatch_meta(meta_signal_flow::Query::Inspect(
            world.launch.profile.address.clone(),
        )) {
        meta_signal_flow::Response::Inspected(inspected) => inspected.visible_attempt_vector,
        other => panic!("Inspect answered {other:?}"),
    }
}

fn inspected_reap(world: &ReapWorld) -> Option<ReapStanding> {
    inspect(world)
        .into_iter()
        .find_map(|attempt| match attempt {
            meta_signal_flow::VisibleAttempt::Reap(reap) => Some(reap.reap_standing),
            _ => None,
        })
}

/// Test 10: a reap is answered pending as soon as it is owed, and its
/// worker closes the pane outside the gate, then settles on Herdr's event.
#[test]
fn a_reap_is_answered_pending_as_soon_as_it_is_owed_and_closed_outside_the_gate() {
    let world = ReapWorld::staged("reap-request");
    let ends = std::thread::scope(|scope| {
        let probe = GateProbe::armed(&world, scope);
        let ends = with_supervisor(&world.fixture, || {
            let answer = world.replace();
            assert!(is_reap_pending(&answer, ReapStanding::Owed), "{answer:?}");
            assert_eq!(answer, replaced_answer(&world, ReapStanding::Owed));
            assert!(matches!(
                world.launch_status(),
                LaunchStatusOutcome::ReapPending(_) | LaunchStatusOutcome::Replaced(_)
            ));
            assert_eq!(world.lifecycle(PREDECESSOR), FlowLifecycle::Stopped);
            assert!(delivery_rejected(
                &world.vet(PREDECESSOR),
                meta_signal_flow::DeliveryRejection::FlowStopped
            ));
            assert!(
                vetted(&world.vet(SUCCESSOR)),
                "the successor receives at once"
            );
            // The worker subscribes, snapshots, closes once.
            world
                .events
                .wait_for("the subscription", |subscribed, _| subscribed >= 1);
            world.until("the close", &mut || world.closes() == 1);
            world.events.emit_gone(PREDECESSOR_PANE);
            world.until("the reap settled", &mut || {
                world
                    .fixture
                    .nexus
                    .store
                    .pending_reap(PREDECESSOR)
                    .unwrap()
                    .is_none()
            });
        });
        probe.stop();
        let seen = probe.seen();
        assert!(!seen.is_empty(), "the reap's Herdr calls were probed");
        assert!(
            seen.iter().all(|(_, free)| *free),
            "every reap Herdr call ran with the gate free: {seen:?}"
        );
        ends
    });
    assert!(matches!(
        world.launch_status(),
        LaunchStatusOutcome::Replaced(replaced) if replaced.flow_id.0 == PREDECESSOR
    ));
    let calls = world.calls();
    let subscribe = calls
        .iter()
        .position(|call| call == "events.subscribe")
        .unwrap();
    let snapshot = calls
        .iter()
        .position(|call| call == "--session messaging-build api snapshot")
        .unwrap();
    let close = calls
        .iter()
        .position(|call| call.contains("pane close w1:p3"))
        .unwrap();
    assert!(subscribe < snapshot && snapshot < close, "{calls:?}");
    assert_eq!(world.closes(), 1);
    let generation = world
        .fixture
        .nexus
        .store
        .handover_memory(&world.launch.profile.address)
        .unwrap()
        .expect("the generation closed");
    assert_eq!(generation.current_flow_id.as_deref(), Some(SUCCESSOR));
    assert!(
        generation
            .closed
            .iter()
            .any(|closed| closed.predecessor == PREDECESSOR && closed.successor == SUCCESSOR)
    );
    assert!(
        ends.iter()
            .any(|(key, end)| key == PREDECESSOR && *end == PaneWatchEnd::Acted),
        "{ends:?}"
    );
}

use crate::store::handover::HoldsHandovers;

/// Test 10b: a refused close leaves the reap pending, the successor
/// delivered to, one close and none more until an event.
#[test]
fn a_refused_close_leaves_the_reap_pending_and_the_successor_delivered_to() {
    let world = ReapWorld::staged("refused-close");
    world.close_exits(1);
    with_supervisor(&world.fixture, || {
        assert!(is_reap_pending(&world.replace(), ReapStanding::Owed));
        world.until("the refused close recorded", &mut || {
            world.standing() == Some(ReapStanding::CloseRefused)
        });
        assert!(matches!(
            world.launch_status(),
            LaunchStatusOutcome::ReapPending(pending)
                if pending.reap_standing == ReapStanding::CloseRefused
        ));
        assert_eq!(inspected_reap(&world), Some(ReapStanding::CloseRefused));
        assert!(vetted(&world.vet(SUCCESSOR)));
        assert_eq!(world.closes(), 1);
        // Unrelated announcements close nothing more.
        for _ in 0..3 {
            world.fixture.nexus.store.launch_changes.announce();
        }
        assert_eq!(world.closes(), 1);
        assert_eq!(world.events.subscriptions(), 1);
    });
}

/// Test 11: from 10b, the successor's next rest makes no Herdr call; it
/// rearms the worker, which closes once more and settles on the event.
#[test]
fn a_pending_reap_is_taken_up_at_the_successors_next_rest() {
    let world = ReapWorld::staged("rest-rearms");
    world.close_exits(1);
    let ends = with_supervisor(&world.fixture, || {
        assert!(is_reap_pending(&world.replace(), ReapStanding::Owed));
        world.until("the refused close", &mut || {
            world.standing() == Some(ReapStanding::CloseRefused)
        });
        world.close_exits(0);
        let calls_before = world.calls().len();
        {
            let _turn = world.fixture.nexus.dispatch_gate.lock().unwrap();
            world.fixture.nexus.take_up_at_rest(SUCCESSOR);
            assert_eq!(world.calls().len(), calls_before, "the rest calls no Herdr");
        }
        world.until("a second close", &mut || world.closes() == 2);
        world.events.emit_gone(PREDECESSOR_PANE);
        world.until("the reap settled", &mut || world.standing().is_none());
    });
    assert!(matches!(
        world.launch_status(),
        LaunchStatusOutcome::Replaced(_)
    ));
    assert!(
        ends.iter().any(|(_, end)| *end
            == PaneWatchEnd::Cancelled(
                crate::observation_lifetime::ObservationCancellationCause::Rearmed
            )),
        "{ends:?}"
    );
}

/// Test 12: from 10b, the pane seen gone settles the reap with no close
/// after the event.
#[test]
fn a_pending_reap_is_taken_up_when_the_predecessors_pane_is_seen_gone() {
    let world = ReapWorld::staged("seen-gone");
    world.close_exits(1);
    let ends = with_supervisor(&world.fixture, || {
        assert!(is_reap_pending(&world.replace(), ReapStanding::Owed));
        world.until("the refused close", &mut || {
            world.standing() == Some(ReapStanding::CloseRefused)
        });
        let closes = world.closes();
        world.events.emit_gone(PREDECESSOR_PANE);
        world.until("the reap settled", &mut || world.standing().is_none());
        assert_eq!(world.closes(), closes, "no close after the event");
    });
    assert!(matches!(
        world.launch_status(),
        LaunchStatusOutcome::Replaced(_)
    ));
    assert!(ends.iter().any(|(_, end)| *end == PaneWatchEnd::Acted));
}

/// G1 (F1): a refused W3 leaves only the successor receiving; its rest, or
/// the Nexus start, writes W3, and the worker settles the reap.
#[test]
fn a_refused_reap_owing_commit_leaves_only_the_successor_receiving() {
    for restart in [false, true] {
        let mut world = ReapWorld::staged("refused-owing");
        world
            .fixture
            .nexus
            .store
            .faults
            .refuse_next(RefusedWrite::ReapOwing);
        let answer = world.replace();
        assert!(
            is_reap_pending(&answer, ReapStanding::Unrecorded),
            "{answer:?}"
        );
        assert_eq!(world.lifecycle(PREDECESSOR), FlowLifecycle::Active);
        assert!(delivery_rejected(
            &world.vet(PREDECESSOR),
            meta_signal_flow::DeliveryRejection::Superseded(signal_flow::FlowId(SUCCESSOR.into()))
        ));
        assert!(!world.fixture.routable(PREDECESSOR));
        assert!(
            world
                .fixture
                .nexus
                .store
                .flows_in_pane("messaging-build", PREDECESSOR_PANE)
                .unwrap()
                .is_empty()
        );
        assert!(vetted(&world.vet(SUCCESSOR)));
        assert!(world.standing().is_none());
        assert_eq!(
            world
                .fixture
                .nexus
                .store
                .launch_outcome(&world.launch.profile.launch_request_id.0)
                .unwrap(),
            None
        );
        let attempts = inspect(&world);
        assert!(
            attempts
                .iter()
                .any(|attempt| matches!(attempt, meta_signal_flow::VisibleAttempt::Launch(_)))
        );
        assert_eq!(inspected_reap(&world), Some(ReapStanding::Unrecorded));
        let calls = world.calls().len();
        assert!(is_reap_pending(&world.replace(), ReapStanding::Unrecorded));
        assert_eq!(
            world.calls().len(),
            calls,
            "a repeated Replace calls nothing"
        );
        if restart {
            world.fixture.restarted();
            world.fixture.nexus.recover_wakes();
        } else {
            let _turn = world.fixture.nexus.dispatch_gate.lock().unwrap();
            world.fixture.nexus.take_up_at_rest(SUCCESSOR);
        }
        assert_eq!(world.lifecycle(PREDECESSOR), FlowLifecycle::Stopped);
        assert_eq!(world.standing(), Some(ReapStanding::Owed));
        assert_eq!(world.closes(), 0, "W3 is written with no Herdr close");
        with_supervisor(&world.fixture, || {
            world.until("the close", &mut || world.closes() == 1);
            world.events.emit_gone(PREDECESSOR_PANE);
            world.until("the reap settled", &mut || world.standing().is_none());
        });
        assert!(matches!(
            world.launch_status(),
            LaunchStatusOutcome::Replaced(_)
        ));
    }
}

/// G2: the receiving flow switches at confirmation, and no reading ever
/// names two receiving flows at one Address.
#[test]
fn the_receiving_flow_switches_at_confirmation_and_never_names_two() {
    let world = ReapWorld::staged("succession");
    let store = &world.fixture.nexus.store;
    let address = world.launch.profile.address.clone();
    let succession = |flow: &str| store.succession(flow).unwrap();
    let accepted = |flow: &str| vetted(&world.vet(flow));
    // The staged launch is past W1: the hold names the successor.
    assert_eq!(succession(SUCCESSOR), Succession::Unconfirmed);
    assert!(delivery_rejected(
        &world.vet(SUCCESSOR),
        meta_signal_flow::DeliveryRejection::Unconfirmed
    ));
    assert!(
        store
            .flows_in_pane("fixture-session", "w1:p1")
            .unwrap()
            .iter()
            .any(|node| node.flow_id.0 == SUCCESSOR),
        "an unconfirmed seat identifies from its first tool call"
    );
    assert!(!(accepted(PREDECESSOR) && accepted(SUCCESSOR)));
    // W2 through the store's own confirmation.
    assert!(matches!(
        store.confirm_started(SUCCESSOR).unwrap(),
        Response::Started(_)
    ));
    assert_eq!(succession(SUCCESSOR), Succession::Receiving);
    assert_eq!(
        succession(PREDECESSOR),
        Succession::Superseded {
            address: address.clone(),
            by: signal_flow::FlowId(SUCCESSOR.into()),
        }
    );
    assert_eq!(
        store.receiving_flow(&address).unwrap(),
        Some(signal_flow::FlowId(SUCCESSOR.into()))
    );
    assert!(accepted(SUCCESSOR) && !accepted(PREDECESSOR));
    // W4: the successor ends.
    assert!(store.record_stopped(SUCCESSOR).unwrap());
    assert_eq!(
        succession(PREDECESSOR),
        Succession::Superseded {
            address: address.clone(),
            by: signal_flow::FlowId(SUCCESSOR.into()),
        }
    );
    assert_eq!(succession(SUCCESSOR), Succession::Unseated);
    assert!(!accepted(SUCCESSOR) && !accepted(PREDECESSOR));
    // A flow no metaflow names is Unseated, routed by its lifecycle.
    assert_eq!(succession("nobody"), Succession::Unseated);
}

/// G2, W2' and K0: a crash between acknowledgement and confirmation routes
/// to the predecessor; the hold given back leaves it receiving.
#[test]
fn a_crash_between_acknowledgement_and_confirmation_routes_to_the_predecessor() {
    let mut world = ReapWorld::staged("crash-w1");
    // The staged launch stands after W1, as a crash there leaves it.
    let first = world.fixture.restarted();
    assert_eq!(
        world.fixture.nexus.store.succession(PREDECESSOR).unwrap(),
        Succession::Receiving
    );
    assert!(vetted(&world.vet(PREDECESSOR)));
    assert!(delivery_rejected(
        &world.vet(SUCCESSOR),
        meta_signal_flow::DeliveryRejection::Unconfirmed
    ));
    assert!(world.standing().is_none());
    assert!(
        world
            .fixture
            .nexus
            .store
            .pending_handovers()
            .unwrap()
            .is_empty()
    );
    // A second restart changes nothing.
    let second = world.fixture.restarted();
    assert_eq!(first, second);
    // W2': the hold given back, the predecessor still receives.
    world.fixture.nexus.store.give_back_hold(SUCCESSOR);
    assert_eq!(
        world.fixture.nexus.store.succession(PREDECESSOR).unwrap(),
        Succession::Receiving
    );
    assert_eq!(
        world.fixture.nexus.store.succession(SUCCESSOR).unwrap(),
        Succession::Unseated
    );
}

/// The crash points of a replacement, K1, K2 and K5: each restart recovers
/// what the store owes, writes it once, and a second restart changes
/// nothing.
#[test]
fn a_crash_at_each_replacement_point_recovers_the_reap_once() {
    for point in [
        CrashPoint::AfterConfirm,
        CrashPoint::AfterReapOwed,
        CrashPoint::AfterReaped,
    ] {
        let mut world = ReapWorld::staged("crash-replace");
        world.fixture.nexus.store.faults.crash_at(point);
        if point == CrashPoint::AfterReaped {
            // K5: the worker's own Reaped commit is the one crashed after.
            assert!(is_reap_pending(&world.replace(), ReapStanding::Owed));
            with_supervisor(&world.fixture, || {
                world.until("the close", &mut || world.closes() == 1);
                world.events.emit_gone(PREDECESSOR_PANE);
                assert!(
                    world
                        .fixture
                        .nexus
                        .store
                        .faults
                        .fired_by(point, Instant::now() + Duration::from_secs(20))
                );
            });
        } else {
            let crashed =
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| world.replace()));
            assert!(
                crashed.is_err(),
                "{point:?}: the operation ends at the crash"
            );
        }
        let first = world.fixture.restarted();
        world.fixture.nexus.recover_wakes();
        match point {
            CrashPoint::AfterConfirm => {
                // K1: the derived reap's W3 is written at the start, with no
                // Herdr close.
                assert_eq!(world.closes(), 0, "{point:?}");
                assert_eq!(world.standing(), Some(ReapStanding::Owed), "{point:?}");
                assert_eq!(world.lifecycle(PREDECESSOR), FlowLifecycle::Stopped);
            }
            CrashPoint::AfterReapOwed => {
                // K2: nothing is written; the worker takes the pane step.
                assert_eq!(world.closes(), 0, "{point:?}");
                assert_eq!(world.standing(), Some(ReapStanding::Owed), "{point:?}");
            }
            CrashPoint::AfterReaped => {
                // K5: nothing owed; the stored outcome answers.
                assert!(world.standing().is_none(), "{point:?}");
                assert!(matches!(world.replace(), Response::Replaced(_)));
            }
            _ => unreachable!(),
        }
        assert!(vetted(&world.vet(SUCCESSOR)), "{point:?}");
        if point != CrashPoint::AfterReaped {
            with_supervisor(&world.fixture, || {
                world.until("the close", &mut || world.closes() == 1);
                world.events.emit_gone(PREDECESSOR_PANE);
                world.until("the reap settled", &mut || world.standing().is_none());
            });
            assert_eq!(world.closes(), 1, "{point:?}: one close");
            assert_ne!(
                first,
                world.fixture.restarted(),
                "{point:?}: the reap settled"
            );
        }
        assert!(matches!(
            world.launch_status(),
            LaunchStatusOutcome::Replaced(_)
        ));
        let settled = world.fixture.restarted();
        world.fixture.nexus.recover_wakes();
        let again = world.fixture.restarted();
        assert_eq!(settled, again, "{point:?}: a second start writes nothing");
    }
}

/// K3 and K12: a crash after the close, or after the worker saw the pane
/// gone and before it took the gate: the next start's worker sees the pane
/// absent in its opening snapshot and settles with no second close.
#[test]
fn a_crash_after_the_pane_closed_settles_from_the_opening_snapshot_without_a_second_close() {
    for point in [CrashPoint::AfterPaneClosed, CrashPoint::WatchAfterGone] {
        let mut world = ReapWorld::staged("crash-closed");
        assert!(is_reap_pending(&world.replace(), ReapStanding::Owed));
        world.fixture.nexus.store.faults.crash_at(point);
        let ends = with_supervisor(&world.fixture, || {
            world.until("the close", &mut || world.closes() == 1);
            if point == CrashPoint::WatchAfterGone {
                world.events.emit_gone(PREDECESSOR_PANE);
            }
            assert!(
                world
                    .fixture
                    .nexus
                    .store
                    .faults
                    .fired_by(point, Instant::now() + Duration::from_secs(20)),
                "{point:?}: the worker reached its crash"
            );
        });
        assert!(
            ends.iter().any(
                |(_, end)| *end == PaneWatchEnd::Lapsed(PaneWatchLapse::Panicked)
                    || matches!(end, PaneWatchEnd::Cancelled(_))
            ),
            "{point:?}: {ends:?}"
        );
        assert_eq!(world.standing(), Some(ReapStanding::Owed), "{point:?}");
        world.fixture.restarted();
        world.fixture.nexus.recover_wakes();
        world.show(PaneShown::Absent);
        with_supervisor(&world.fixture, || {
            world.until("the reap settled", &mut || world.standing().is_none());
        });
        assert_eq!(world.closes(), 1, "{point:?}: no second close");
        assert!(matches!(
            world.launch_status(),
            LaunchStatusOutcome::Replaced(_)
        ));
    }
}

/// W2: a pane gone before the watch is seen by its opening snapshot: no
/// close, one act.
#[test]
fn a_pane_gone_before_the_watch_is_seen_by_its_opening_snapshot() {
    let world = ReapWorld::staged("gone-before");
    world.show(PaneShown::Absent);
    with_supervisor(&world.fixture, || {
        assert!(is_reap_pending(&world.replace(), ReapStanding::Owed));
        world.until("the reap settled", &mut || world.standing().is_none());
    });
    assert_eq!(world.closes(), 0);
    assert!(matches!(
        world.launch_status(),
        LaunchStatusOutcome::Replaced(_)
    ));
}

/// W13 (corrected, tykj): a pane running another native session is never
/// closed and not counted gone; its own going settles the reap.
#[test]
fn a_pane_running_another_session_is_never_closed() {
    let world = ReapWorld::staged("occupied");
    world.show(PaneShown::Occupied);
    with_supervisor(&world.fixture, || {
        assert!(is_reap_pending(&world.replace(), ReapStanding::Owed));
        world.until("the opening snapshot", &mut || {
            world.snapshots_of_predecessor() >= 1
        });
        world
            .events
            .wait_for("the subscription", |subscribed, _| subscribed >= 1);
        assert_eq!(world.closes(), 0);
        assert!(matches!(
            world.launch_status(),
            LaunchStatusOutcome::ReapPending(_)
        ));
        world.events.emit_gone(PREDECESSOR_PANE);
        world.until("the reap settled", &mut || world.standing().is_none());
    });
    assert_eq!(world.closes(), 0, "another session's pane is never closed");
    assert!(matches!(
        world.launch_status(),
        LaunchStatusOutcome::Replaced(_)
    ));
}

/// W14 (corrected, tykj): an Unavailable route alone never settles a reap;
/// independent evidence (the predecessor's row Exited by its own event)
/// does, at the successor's next rest.
#[test]
fn an_unavailable_route_alone_never_settles_a_reap() {
    let world = ReapWorld::staged("unrouted");
    // The predecessor's route is not recorded.
    world.fixture.nexus.store.forget_route(PREDECESSOR);
    let ends = with_supervisor(&world.fixture, || {
        assert!(is_reap_pending(&world.replace(), ReapStanding::Owed));
        world.until("the typed standing", &mut || {
            world.standing() == Some(ReapStanding::HerdrUnavailable)
        });
        {
            let _turn = world.fixture.nexus.dispatch_gate.lock().unwrap();
            world.fixture.nexus.take_up_at_rest(SUCCESSOR);
        }
        assert_eq!(world.standing(), Some(ReapStanding::HerdrUnavailable));
        assert!(matches!(
            world.launch_status(),
            LaunchStatusOutcome::ReapPending(_)
        ));
    });
    assert!(
        ends.iter()
            .any(|(_, end)| *end == PaneWatchEnd::Lapsed(PaneWatchLapse::RouteUnrecorded)),
        "{ends:?}"
    );
    assert_eq!(world.snapshots_of_predecessor(), 0, "no Herdr call");
    assert_eq!(world.closes(), 0);
    // Independent evidence: the predecessor's own exit.
    world.fixture.nexus.store.mark_exited(PREDECESSOR);
    {
        let _turn = world.fixture.nexus.dispatch_gate.lock().unwrap();
        world.fixture.nexus.take_up_at_rest(SUCCESSOR);
    }
    assert!(world.standing().is_none());
    assert!(matches!(
        world.launch_status(),
        LaunchStatusOutcome::Replaced(_)
    ));
}

/// W3: one watch per pending reap, whatever is announced.
#[test]
fn one_watch_per_pending_reap() {
    let world = ReapWorld::staged("one-watch");
    world.show(PaneShown::Occupied);
    with_supervisor(&world.fixture, || {
        assert!(is_reap_pending(&world.replace(), ReapStanding::Owed));
        world
            .events
            .wait_for("the subscription", |subscribed, _| subscribed >= 1);
        for _ in 0..3 {
            world.fixture.nexus.store.launch_changes.announce();
        }
        assert!(
            world
                .fixture
                .nexus
                .store
                .reap_refused(PREDECESSOR, ReapStanding::CloseRefused)
                .unwrap()
        );
        world.fixture.nexus.store.launch_changes.announce();
        assert_eq!(world.events.subscriptions(), 1);
    });
}

/// W4: a retracted obligation cancels and joins its watch, with no write
/// from the worker.
#[test]
fn a_retracted_obligation_cancels_and_joins_its_watch() {
    let world = ReapWorld::staged("retracted");
    world.show(PaneShown::Occupied);
    let ends = with_supervisor(&world.fixture, || {
        assert!(is_reap_pending(&world.replace(), ReapStanding::Owed));
        world
            .events
            .wait_for("the subscription", |subscribed, _| subscribed >= 1);
        let attempts = world.fixture.nexus.store.faults.attempts().len();
        // The row is retracted through the store's own Reaped record.
        {
            let _turn = world.fixture.nexus.dispatch_gate.lock().unwrap();
            let closing = crate::store::handover::ClosesHandovers::nothing_closing(
                &*world.fixture.nexus.store,
            );
            world
                .fixture
                .nexus
                .store
                .reaped(PREDECESSOR, closing)
                .unwrap();
        }
        world
            .events
            .wait_for("the subscription closed", |_, closed| closed >= 1);
        assert_eq!(
            world.fixture.nexus.store.faults.attempts().len(),
            attempts + 1,
            "the worker wrote nothing"
        );
    });
    assert!(
        ends.iter().any(|(_, end)| *end
            == PaneWatchEnd::Cancelled(
                crate::observation_lifetime::ObservationCancellationCause::ObligationRetracted
            )),
        "{ends:?}"
    );
}

/// W5 and W6: the Nexus's stop cancels and joins every worker, the one
/// whose acknowledgement is withheld, the one waiting for events, one in a
/// snapshot that blocks until killed and one in a close that does; while
/// they block the gate is free.
#[test]
fn nexus_stop_cancels_and_joins_every_pane_watch() {
    for blocked in ["acknowledgement", "events", "snapshot", "close"] {
        let world = ReapWorld::staged("stopped");
        match blocked {
            "acknowledgement" => world.events.withhold_acknowledgement(),
            "events" => world.show(PaneShown::Occupied),
            "snapshot" => world.block_snapshot(true),
            "close" => world.block_close(true),
            _ => unreachable!(),
        }
        let ends = with_supervisor(&world.fixture, || {
            assert!(is_reap_pending(&world.replace(), ReapStanding::Owed));
            match blocked {
                "acknowledgement" | "events" => world
                    .events
                    .wait_for("the subscription", |subscribed, _| subscribed >= 1),
                "snapshot" => world.until("the blocking snapshot", &mut || {
                    world.snapshots_of_predecessor() >= 1
                }),
                "close" => world.until("the blocking close", &mut || world.closes() >= 1),
                _ => unreachable!(),
            }
            assert!(
                world.fixture.nexus.dispatch_gate.try_lock().is_ok(),
                "{blocked}: the gate is free while the worker waits on Herdr"
            );
        });
        assert!(
            ends.iter().any(|(_, end)| *end
                == PaneWatchEnd::Cancelled(
                    crate::observation_lifetime::ObservationCancellationCause::NexusStopped
                )),
            "{blocked}: {ends:?}"
        );
        assert_eq!(world.standing(), Some(ReapStanding::Owed), "{blocked}");
    }
}

/// W5's variant (R6): a stop announced between the supervisor's stop check
/// and its wait is not lost: the supervisor returns.
#[test]
fn a_stop_between_the_check_and_the_wait_is_not_lost() {
    let world = ReapWorld::staged("stop-race");
    let nexus = &world.fixture.nexus;
    *nexus.pane_watch_hooks.between.lock().unwrap() = Some(Box::new(|nexus| {
        nexus.stopping.request();
        nexus.store.launch_changes.announce();
    }));
    std::thread::scope(|scope| {
        let supervisor = scope.spawn(|| nexus.serve_reaped_pane_watches(scope));
        supervisor.join().expect("the supervisor returns");
    });
}

/// Lifecycle: a worker that lapses with no store change is joined by its
/// own completion, with no other event; the wake re-arms nothing.
#[test]
fn a_lapsed_worker_is_joined_by_its_own_completion() {
    use crate::reaping::RunsPaneWatchHooks;
    let world = ReapWorld::staged("own-completion");
    world.show(PaneShown::Occupied);
    let hooks = &world.fixture.nexus.pane_watch_hooks;
    let ends = with_supervisor(&world.fixture, || {
        assert!(is_reap_pending(&world.replace(), ReapStanding::Owed));
        world
            .events
            .wait_for("the subscription", |subscribed, _| subscribed >= 1);
        let attempts = world.fixture.nexus.store.faults.attempts().len();
        world.events.close_streams();
        assert!(
            hooks.workers_joined_by(1, Instant::now() + Duration::from_secs(20)),
            "the lapsed worker is joined by its own completion"
        );
        assert_eq!(
            world.fixture.nexus.store.faults.attempts().len(),
            attempts,
            "the lapse changed no store row"
        );
        for _ in 0..3 {
            world.fixture.nexus.store.launch_changes.announce();
        }
    });
    assert_eq!(
        ends,
        vec![(
            PREDECESSOR.to_owned(),
            PaneWatchEnd::Lapsed(PaneWatchLapse::StreamClosedPanePresent)
        )]
    );
    assert_eq!(hooks.spawned.lock().unwrap().len(), 1, "nothing re-armed");
    assert_eq!(world.events.subscriptions(), 1);
    assert_eq!(world.standing(), Some(ReapStanding::Owed));
}

/// Lifecycle, fault-injected order: a worker's completion lands after the
/// supervisor's pass and before its wait; it is queued and joined, not
/// lost.
#[test]
fn a_completion_between_the_pass_and_the_wait_is_not_lost() {
    use crate::reaping::RunsPaneWatchHooks;
    let world = ReapWorld::staged("completion-race");
    world.show(PaneShown::Occupied);
    let hooks = &world.fixture.nexus.pane_watch_hooks;
    let ends = with_supervisor(&world.fixture, || {
        assert!(is_reap_pending(&world.replace(), ReapStanding::Owed));
        world
            .events
            .wait_for("the subscription", |subscribed, _| subscribed >= 1);
        let events = world.events.clone();
        *hooks.between.lock().unwrap() = Some(Box::new(move |nexus| {
            events.close_streams();
            assert!(
                nexus
                    .pane_watch_hooks
                    .workers_ended_by(1, Instant::now() + Duration::from_secs(20)),
                "the worker completes between the pass and the wait"
            );
        }));
        world.fixture.nexus.store.launch_changes.announce();
        assert!(
            hooks.workers_joined_by(1, Instant::now() + Duration::from_secs(20)),
            "the completion announced in the window is joined"
        );
        assert!(hooks.between.lock().unwrap().is_none());
    });
    assert_eq!(
        ends,
        vec![(
            PREDECESSOR.to_owned(),
            PaneWatchEnd::Lapsed(PaneWatchLapse::StreamClosedPanePresent)
        )]
    );
    assert_eq!(hooks.spawned.lock().unwrap().len(), 1, "nothing re-armed");
}

/// Lifecycle: a worker cancelled by a rearm is joined once; its completion,
/// drained after, joins no other worker, and the worker armed in its place
/// keeps watching and acts.
#[test]
fn a_rearmed_worker_is_joined_once_and_its_late_completion_joins_no_other() {
    let world = ReapWorld::staged("rearmed-once");
    world.show(PaneShown::Occupied);
    let hooks = &world.fixture.nexus.pane_watch_hooks;
    let ends = with_supervisor(&world.fixture, || {
        assert!(is_reap_pending(&world.replace(), ReapStanding::Owed));
        world
            .events
            .wait_for("the subscription", |subscribed, _| subscribed >= 1);
        {
            let _turn = world.fixture.nexus.dispatch_gate.lock().unwrap();
            world.fixture.nexus.take_up_at_rest(SUCCESSOR);
        }
        world
            .events
            .wait_for("the rearmed subscription", |subscribed, _| subscribed >= 2);
        // Two passes in turn: the second runs wholly after the rearm's own
        // pass, so it has drained the first worker's completion.
        for _ in 0..2 {
            let (passed, pass) = std::sync::mpsc::channel();
            *hooks.between.lock().unwrap() = Some(Box::new(move |_| {
                let _ = passed.send(());
            }));
            world.fixture.nexus.store.launch_changes.announce();
            pass.recv_timeout(Duration::from_secs(20))
                .expect("a supervisor pass");
        }
        assert_eq!(
            hooks.joined.lock().unwrap().len(),
            1,
            "the first worker's late completion joins no other"
        );
        world.events.emit_gone(PREDECESSOR_PANE);
        world.until("the reap settled", &mut || world.standing().is_none());
    });
    assert_eq!(hooks.spawned.lock().unwrap().len(), 2);
    assert!(
        ends.iter().any(|(_, end)| *end
            == PaneWatchEnd::Cancelled(
                crate::observation_lifetime::ObservationCancellationCause::Rearmed
            )),
        "{ends:?}"
    );
    assert!(
        ends.iter().any(|(_, end)| *end == PaneWatchEnd::Acted),
        "{ends:?}"
    );
}

/// W7: a stream closed with the pane present lapses the watch; unrelated
/// announcements arm nothing; the successor's rest arms exactly one more.
#[test]
fn a_lapsed_watch_is_armed_again_only_by_a_bearing_event() {
    let world = ReapWorld::staged("lapsed");
    world.show(PaneShown::Occupied);
    let ends = with_supervisor(&world.fixture, || {
        assert!(is_reap_pending(&world.replace(), ReapStanding::Owed));
        world
            .events
            .wait_for("the subscription", |subscribed, _| subscribed >= 1);
        world.events.close_streams();
        use crate::reaping::RunsPaneWatchHooks;
        assert!(
            world
                .fixture
                .nexus
                .pane_watch_hooks
                .workers_ended_by(1, Instant::now() + Duration::from_secs(20)),
            "the worker lapses"
        );
        assert_eq!(
            world.snapshots_of_predecessor(),
            2,
            "one snapshot after the stream closed"
        );
        for _ in 0..2 {
            world.fixture.nexus.store.launch_changes.announce();
        }
        assert_eq!(world.events.subscriptions(), 1);
        {
            let _turn = world.fixture.nexus.dispatch_gate.lock().unwrap();
            world.fixture.nexus.take_up_at_rest(SUCCESSOR);
        }
        world
            .events
            .wait_for("the rearmed subscription", |subscribed, _| subscribed >= 2);
    });
    assert!(
        ends.iter()
            .any(|(_, end)| *end == PaneWatchEnd::Lapsed(PaneWatchLapse::StreamClosedPanePresent)),
        "{ends:?}"
    );
}

/// W10 (R13): a stale worker writes nothing, and the Address keeps one
/// receiving flow; the generation is not handed to the stale successor.
#[test]
fn a_stale_worker_writes_nothing_and_the_address_keeps_one_receiving_flow() {
    let world = ReapWorld::staged("stale");
    world.close_exits(1);
    with_supervisor(&world.fixture, || {
        assert!(is_reap_pending(&world.replace(), ReapStanding::Owed));
        world.until("the refused close", &mut || {
            world.standing() == Some(ReapStanding::CloseRefused)
        });
        let turn = world.fixture.nexus.dispatch_gate.lock().unwrap();
        world.events.emit_gone(PREDECESSOR_PANE);
        // While the test holds the gate, the row settles by the store's own
        // record, and its successor ends.
        let closing =
            crate::store::handover::ClosesHandovers::nothing_closing(&*world.fixture.nexus.store);
        world
            .fixture
            .nexus
            .store
            .reaped(PREDECESSOR, closing)
            .unwrap();
        assert!(world.fixture.nexus.store.record_stopped(SUCCESSOR).unwrap());
        let attempts = world.fixture.nexus.store.faults.attempts().len();
        drop(turn);
        world.until("the worker's end", &mut || {
            world
                .fixture
                .nexus
                .store
                .pending_reap(PREDECESSOR)
                .unwrap()
                .is_none()
        });
        world.fixture.nexus.store.launch_changes.announce();
        assert_eq!(
            world.fixture.nexus.store.faults.attempts().len(),
            attempts,
            "the stale worker writes nothing"
        );
    });
    let generation = world
        .fixture
        .nexus
        .store
        .handover_memory(&world.launch.profile.address)
        .unwrap();
    assert!(
        generation.is_none_or(|row| row.current_flow_id.as_deref() != Some(SUCCESSOR)),
        "the generation is not handed to a successor that no longer receives"
    );
    assert!(!vetted(&world.vet(SUCCESSOR)) && !vetted(&world.vet(PREDECESSOR)));
}

/// W11 (R15, F3): a refused worker commit keeps the successor the only
/// receiving flow; the entry lapses ActedRowStands; the successor's rest
/// runs step 3 again with no Herdr call.
#[test]
fn a_refused_worker_commit_keeps_the_successor_the_only_receiving_flow() {
    let world = ReapWorld::staged("refused-settle");
    let ends = with_supervisor(&world.fixture, || {
        world
            .fixture
            .nexus
            .store
            .faults
            .refuse_next(RefusedWrite::ReapSettle);
        assert!(is_reap_pending(&world.replace(), ReapStanding::Owed));
        world.until("the close", &mut || world.closes() == 1);
        world.events.emit_gone(PREDECESSOR_PANE);
        world.until("the refused settlement", &mut || {
            world.standing() == Some(ReapStanding::SettleUnrecorded)
        });
        assert_eq!(
            world.fixture.nexus.store.succession(SUCCESSOR).unwrap(),
            Succession::Receiving
        );
        assert!(vetted(&world.vet(SUCCESSOR)));
        assert!(delivery_rejected(
            &world.vet(PREDECESSOR),
            meta_signal_flow::DeliveryRejection::FlowStopped
        ));
        world.fixture.nexus.store.launch_changes.announce();
        let calls = world.calls().len();
        {
            let _turn = world.fixture.nexus.dispatch_gate.lock().unwrap();
            world.fixture.nexus.take_up_at_rest(SUCCESSOR);
        }
        assert_eq!(world.calls().len(), calls, "step 3 again calls no Herdr");
        assert!(world.standing().is_none());
    });
    assert!(matches!(
        world.launch_status(),
        LaunchStatusOutcome::Replaced(_)
    ));
    let _ = ends;
}

/// W8 (R8): a worker whose act is refused, its rewrite refused too, is not
/// armed again by its own announcement or unrelated ones.
#[test]
fn a_watch_whose_act_is_refused_is_not_armed_again_by_its_own_announcement() {
    let world = ReapWorld::staged("act-refused");
    let ends = with_supervisor(&world.fixture, || {
        let faults = &world.fixture.nexus.store.faults;
        faults.refuse_next(RefusedWrite::ReapSettle);
        faults.refuse_next(RefusedWrite::ReapRewrite);
        assert!(is_reap_pending(&world.replace(), ReapStanding::Owed));
        world.until("the close", &mut || world.closes() == 1);
        world.events.emit_gone(PREDECESSOR_PANE);
        world.until("the refused act", &mut || {
            world
                .fixture
                .nexus
                .store
                .faults
                .attempts()
                .iter()
                .filter(|attempt| *attempt == "ReapRefused")
                .count()
                >= 1
        });
        for _ in 0..5 {
            world.fixture.nexus.store.launch_changes.announce();
        }
        assert_eq!(world.events.subscriptions(), 1);
        assert_eq!(world.standing(), Some(ReapStanding::Owed));
    });
    assert!(
        ends.iter().any(|(_, end)| *end == PaneWatchEnd::Acted
            || *end == PaneWatchEnd::Lapsed(PaneWatchLapse::ActedRowStands)),
        "{ends:?}"
    );
}

/// W9 (R9): a panicked worker lapses and the pass does not arm it again.
#[test]
fn a_panicked_watch_lapses_and_is_not_rearmed_by_the_pass() {
    let world = ReapWorld::staged("panicked");
    world.show(PaneShown::Occupied);
    world
        .fixture
        .nexus
        .pane_watch_hooks
        .panic_at_event
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let ends = with_supervisor(&world.fixture, || {
        assert!(is_reap_pending(&world.replace(), ReapStanding::Owed));
        world
            .events
            .wait_for("the subscription", |subscribed, _| subscribed >= 1);
        world.events.emit_gone("another-pane");
        // The worker panics at the event; the next pass joins it Lapsed.
        assert!(
            world.fixture.nexus.store.launch_changes.until_holds(
                Instant::now() + Duration::from_secs(20),
                &mut || {
                    !world
                        .fixture
                        .nexus
                        .pane_watch_hooks
                        .panic_at_event
                        .load(std::sync::atomic::Ordering::SeqCst)
                }
            ),
            "the worker panics at its event"
        );
        for _ in 0..3 {
            world.fixture.nexus.store.launch_changes.announce();
        }
    });
    assert!(
        ends.iter()
            .any(|(_, end)| *end == PaneWatchEnd::Lapsed(PaneWatchLapse::Panicked)),
        "{ends:?}"
    );
    assert_eq!(world.events.subscriptions(), 1, "{ends:?}");
}

/// M2: a missing predecessor persists typed; no event retries it.
#[test]
fn a_missing_predecessor_persists_typed_and_no_event_retries_it() {
    let world = ReapWorld::staged("missing-predecessor");
    // The predecessor's row is read as missing at W3.
    world
        .fixture
        .nexus
        .store
        .faults
        .refuse_next(RefusedWrite::FlowRead);
    let answer = world.replace();
    assert!(
        is_reap_pending(&answer, ReapStanding::PredecessorUnknown),
        "an unread row is Unknown: {answer:?}"
    );
    // M3: the next rest reads it again, finds it live, records Stopped and
    // the reap Owed.
    {
        let _turn = world.fixture.nexus.dispatch_gate.lock().unwrap();
        world.fixture.nexus.take_up_at_rest(SUCCESSOR);
    }
    assert_eq!(world.standing(), Some(ReapStanding::Owed));
    assert_eq!(world.lifecycle(PREDECESSOR), FlowLifecycle::Stopped);
    // A row naming no flow: PredecessorMissing stands, untried.
    assert!(
        world
            .fixture
            .nexus
            .store
            .reap_refused(PREDECESSOR, ReapStanding::PredecessorMissing)
            .unwrap()
    );
    let attempts = world.fixture.nexus.store.faults.attempts().len();
    let calls = world.calls().len();
    with_supervisor(&world.fixture, || {
        for _ in 0..5 {
            world.fixture.nexus.store.launch_changes.announce();
        }
        let _turn = world.fixture.nexus.dispatch_gate.lock().unwrap();
        world.fixture.nexus.take_up_at_rest(SUCCESSOR);
        world.fixture.nexus.take_up_at_rest(SUCCESSOR);
    });
    assert_eq!(world.standing(), Some(ReapStanding::PredecessorMissing));
    assert_eq!(world.fixture.nexus.store.faults.attempts().len(), attempts);
    assert_eq!(world.calls().len(), calls, "no Herdr call");
    assert_eq!(world.events.subscriptions(), 0, "no watch");
    assert_eq!(
        inspected_reap(&world),
        Some(ReapStanding::PredecessorMissing)
    );
}

/// Test 19: a Nexus start recovers every obligation without looping: an
/// outcome-less replacement of a confirmed successor gets its W3, the
/// worker settles a reap whose pane is absent, and a second start writes
/// nothing.
#[test]
fn a_nexus_start_recovers_every_obligation_without_looping() {
    let mut world = ReapWorld::staged("start-recovers");
    world
        .fixture
        .nexus
        .store
        .faults
        .refuse_next(RefusedWrite::ReapOwing);
    assert!(is_reap_pending(&world.replace(), ReapStanding::Unrecorded));
    world.fixture.restarted();
    world.fixture.nexus.recover_wakes();
    assert_eq!(world.standing(), Some(ReapStanding::Owed));
    world.show(PaneShown::Absent);
    with_supervisor(&world.fixture, || {
        world.until("the reap settled", &mut || world.standing().is_none());
    });
    assert_eq!(world.closes(), 0);
    let settled = world.fixture.restarted();
    world.fixture.nexus.recover_wakes();
    let again = world.fixture.restarted();
    assert_eq!(settled, again);
}

/// The store of a replacement whose W3 was refused, closed, its
/// launch-outcome family rewritten as 0.54.6 wrote it, v9, with `rows`, and
/// the obligation families 0.54.7 adds dropped: the store as 0.54.6 left
/// it. The world and the store's path.
fn v9_world(
    id: &str,
    rows: impl FnOnce(&str) -> Vec<StoredLaunchOutcomeV9>,
) -> (ReapWorld, PathBuf) {
    let mut world = ReapWorld::staged(id);
    world
        .fixture
        .nexus
        .store
        .faults
        .refuse_next(RefusedWrite::ReapOwing);
    assert!(is_reap_pending(&world.replace(), ReapStanding::Unrecorded));
    let path = world.fixture.directory.path().join("flow.sema");
    let rows = rows(&world.launch.profile.launch_request_id.0);
    world.fixture.restarted();
    // The Nexus over it is closed again: the store is at rest.
    let scratch = world.fixture.directory.path().join("v9-scratch");
    fs::create_dir_all(&scratch).unwrap();
    let placeholder = crate::store::FlowStore::open(&scratch.join("placeholder.sema")).unwrap();
    drop(std::mem::replace(
        &mut world.fixture.nexus.store,
        placeholder.into(),
    ));
    v9_store(&path, rows);
    (world, path)
}

use crate::store::reap_upgrade::{
    LaunchOutcomeV9, ReplaceRejectionV9, StoredLaunchOutcomeV9, UpgradesReapMemory,
};

/// Rewrites the closed store at `path` as 0.54.6 left it: its launch
/// outcomes at v9, holding `rows`, and no obligation family.
pub(crate) fn v9_store(path: &Path, rows: Vec<StoredLaunchOutcomeV9>) {
    v9_store_at(path, rows, 1);
}

/// As [`v9_store`], for a store of Sema schema version `version`: 1 for a
/// test fixture's seeded store, 3 for a configured one.
pub(crate) fn v9_store_at(path: &Path, rows: Vec<StoredLaunchOutcomeV9>, version: u32) {
    use crate::store::reap_upgrade::LAUNCH_OUTCOME_SCHEMA_V9;
    use sema_engine::{Engine, EngineOpen, FamilyName, SchemaVersion, TableDescriptor};
    {
        let database = redb::Database::open(path).expect("the store opens raw");
        let write = database.begin_write().unwrap();
        {
            let mut catalog = write
                .open_table(redb::TableDefinition::<&str, &[u8]>::new(
                    "__sema_engine_catalog",
                ))
                .unwrap();
            for table in [
                "flow_nexus_launch_outcomes",
                "flow_nexus_pending_reaps",
                "flow_nexus_pending_handovers",
            ] {
                catalog.remove(table).unwrap();
            }
        }
        for table in [
            "flow_nexus_launch_outcomes",
            "flow_nexus_pending_reaps",
            "flow_nexus_pending_handovers",
        ] {
            let _ = write.delete_table(redb::TableDefinition::<String, &[u8]>::new(table));
        }
        write.commit().unwrap();
    }
    let mut engine = Engine::open(EngineOpen::new(path, SchemaVersion::new(version))).unwrap();
    let table = engine
        .register_table(TableDescriptor::<StoredLaunchOutcomeV9>::new(
            sema_engine::TableName::new("flow_nexus_launch_outcomes"),
            FamilyName::new("flow-nexus-launch-outcome"),
            sema_engine::SchemaHash::for_label(LAUNCH_OUTCOME_SCHEMA_V9),
        ))
        .unwrap();
    for row in rows {
        engine
            .assert(sema_engine::Assertion::new(table, row))
            .unwrap();
    }
}

/// An Engine over the closed store at `path` with the v10 family declared.
fn evolving_engine(path: &Path) -> sema_engine::Engine {
    use crate::store::reap_upgrade::DescribesReapUpgrade;
    let descriptor = <crate::store::FlowStore as DescribesReapUpgrade>::launch_outcome_descriptor();
    sema_engine::Engine::open(
        sema_engine::EngineOpen::new(path, sema_engine::SchemaVersion::new(1))
            .with_family(&descriptor),
    )
    .expect("the store opens with the v10 family declared")
}

/// Test 20: a v9 store with a refused reap upgrades in one commit: the
/// family at v10, the refused reap `ReapPending` with its PendingReap at
/// Owed and its predecessor Stopped, every other outcome as it was, no
/// PendingHandover; a second open is a no-op.
#[test]
fn a_v9_store_with_a_refused_reap_upgrades_in_one_commit() {
    use crate::generated::starter::LaunchedRecord;
    let (mut world, path) = v9_world("upgrade", |id| {
        let record = LaunchedRecord {
            flow_id: signal_flow::FlowId("0000aa".into()),
            session_id: signal_flow::SessionId("s".into()),
            origin: signal_flow::Origin::Unbound,
        };
        vec![
            StoredLaunchOutcomeV9 {
                launch_request_id: id.into(),
                launch_outcome: LaunchOutcomeV9::ReplaceRejected(ReplaceRejectionV9::ReapRefused(
                    signal_flow::StopRejection::PersistenceRefused,
                )),
            },
            StoredLaunchOutcomeV9 {
                launch_request_id: "other-started".into(),
                launch_outcome: LaunchOutcomeV9::Started(record.clone()),
            },
            StoredLaunchOutcomeV9 {
                launch_request_id: "other-replaced".into(),
                launch_outcome: LaunchOutcomeV9::Replaced(
                    crate::generated::starter::ReplacedRecord {
                        flow_id: signal_flow::FlowId("0000bb".into()),
                        launched_record: record,
                    },
                ),
            },
            StoredLaunchOutcomeV9 {
                launch_request_id: "other-rejected".into(),
                launch_outcome: LaunchOutcomeV9::ReplaceRejected(ReplaceRejectionV9::Locked),
            },
        ]
    });
    // The frozen v9 layout reads what 0.54.6 published: its signal-flow 60
    // ReplaceRejection, ReapRefused among its six variants.
    let published =
        signal_flow_60::ReplaceRejection::ReapRefused(signal_flow_60::StopRejection::CloseRefused);
    let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&published).unwrap();
    assert_eq!(
        rkyv::from_bytes::<ReplaceRejectionV9, rkyv::rancor::Error>(&bytes).unwrap(),
        ReplaceRejectionV9::ReapRefused(signal_flow::StopRejection::CloseRefused)
    );
    {
        let mut engine = evolving_engine(&path);
        crate::store::FlowStore::upgrade_reap_memory(&mut engine).expect("the upgrade lands");
    }
    let upgraded = LogicalState::read(&path);
    {
        let mut engine = evolving_engine(&path);
        crate::store::FlowStore::upgrade_reap_memory(&mut engine).expect("a second open");
    }
    assert_eq!(
        LogicalState::read(&path),
        upgraded,
        "a second open is a no-op"
    );
    world.fixture.restarted();
    let id = world.launch.profile.launch_request_id.0.clone();
    assert!(matches!(
        world.fixture.nexus.store.launch_outcome(&id).unwrap(),
        Some(LaunchOutcome::ReapPending(replaced))
            if replaced.flow_id.0 == PREDECESSOR && replaced.launched_record.flow_id.0 == SUCCESSOR
    ));
    assert_eq!(world.standing(), Some(ReapStanding::Owed));
    assert_eq!(world.lifecycle(PREDECESSOR), FlowLifecycle::Stopped);
    assert!(matches!(
        world
            .fixture
            .nexus
            .store
            .launch_outcome("other-started")
            .unwrap(),
        Some(LaunchOutcome::Started(_))
    ));
    assert!(matches!(
        world
            .fixture
            .nexus
            .store
            .launch_outcome("other-replaced")
            .unwrap(),
        Some(LaunchOutcome::Replaced(_))
    ));
    assert!(matches!(
        world
            .fixture
            .nexus
            .store
            .launch_outcome("other-rejected")
            .unwrap(),
        Some(LaunchOutcome::ReplaceRejected(
            signal_flow::ReplaceRejection::Locked
        ))
    ));
    assert!(
        world
            .fixture
            .nexus
            .store
            .pending_handovers()
            .unwrap()
            .is_empty()
    );
}

/// Test 20's refusal and interrupted variants: a refused reap whose attempt
/// is missing refuses the upgrade typed, and an upgrade interrupted after
/// its rows are written lands nothing; each time the logical rows and the
/// catalog are as they were, the v9 identity still registers, and no
/// obligation family is in the catalog. A clean open then upgrades.
#[test]
fn reap_upgrade_refusal_preserves_logical_rows_and_catalog() {
    let (_world, path) = v9_world("upgrade-refused", |_| {
        vec![StoredLaunchOutcomeV9 {
            launch_request_id: "orphaned-reap".into(),
            launch_outcome: LaunchOutcomeV9::ReplaceRejected(ReplaceRejectionV9::ReapRefused(
                signal_flow::StopRejection::PersistenceRefused,
            )),
        }]
    });
    let before = LogicalState::read(&path);
    {
        let mut engine = evolving_engine(&path);
        let refused = crate::store::FlowStore::upgrade_reap_memory(&mut engine);
        assert!(
            matches!(&refused, Err(crate::store::StoreError::ReapUpgrade(reason)) if reason.contains("AttemptMissing")),
            "{refused:?}"
        );
    }
    for _ in 0..2 {
        assert_eq!(LogicalState::read(&path), before);
        let mut engine = sema_engine::Engine::open(sema_engine::EngineOpen::new(
            &path,
            sema_engine::SchemaVersion::new(1),
        ))
        .unwrap();
        engine
            .register_table(sema_engine::TableDescriptor::<StoredLaunchOutcomeV9>::new(
                sema_engine::TableName::new("flow_nexus_launch_outcomes"),
                sema_engine::FamilyName::new("flow-nexus-launch-outcome"),
                sema_engine::SchemaHash::for_label(
                    crate::store::reap_upgrade::LAUNCH_OUTCOME_SCHEMA_V9,
                ),
            ))
            .expect("the v9 identity still registers");
    }
    assert!(!before.catalog.contains_key("flow_nexus_pending_reaps"));
    assert!(!before.catalog.contains_key("flow_nexus_pending_handovers"));

    // Interrupted: the fault fires after the rows are written.
    let (_world, path) = v9_world("upgrade-interrupted", |id| {
        vec![StoredLaunchOutcomeV9 {
            launch_request_id: id.into(),
            launch_outcome: LaunchOutcomeV9::ReplaceRejected(ReplaceRejectionV9::ReapRefused(
                signal_flow::StopRejection::CloseRefused,
            )),
        }]
    });
    let before = LogicalState::read(&path);
    {
        let mut engine = evolving_engine(&path);
        use sema_engine::MemoryUpgrades as _;
        engine.interrupt_upgrade_at(sema_engine::UpgradeFault::AfterRowsWritten);
        let interrupted = crate::store::FlowStore::upgrade_reap_memory(&mut engine);
        assert!(
            format!("{interrupted:?}").contains("UpgradeInterrupted"),
            "{interrupted:?}"
        );
    }
    assert_eq!(LogicalState::read(&path), before);
    {
        let mut engine = evolving_engine(&path);
        crate::store::FlowStore::upgrade_reap_memory(&mut engine).expect("a clean open upgrades");
    }
    let upgraded = LogicalState::read(&path);
    assert!(upgraded.catalog.contains_key("flow_nexus_pending_reaps"));
    assert!(
        upgraded
            .catalog
            .contains_key("flow_nexus_pending_handovers")
    );
    assert_eq!(upgraded.rows("flow_nexus_pending_reaps").len(), 1);
    {
        let mut engine = evolving_engine(&path);
        crate::store::FlowStore::upgrade_reap_memory(&mut engine).expect("idempotent");
    }
    assert_eq!(LogicalState::read(&path), upgraded);
}

/// Test 21: an older daemon refuses a 0.54.7 store: opening with the v9
/// descriptor answers FamilyIdentityMismatch, and the logical rows and the
/// catalog are unchanged; the v10 identity still registers.
#[test]
fn an_older_daemon_refuses_a_0_54_7_store() {
    let mut world = ReapWorld::staged("older-daemon");
    assert!(is_reap_pending(&world.replace(), ReapStanding::Owed));
    let path = world.fixture.directory.path().join("flow.sema");
    let before = world.fixture.restarted();
    let scratch = world.fixture.directory.path().join("older-scratch");
    fs::create_dir_all(&scratch).unwrap();
    let placeholder = crate::store::FlowStore::open(&scratch.join("placeholder.sema")).unwrap();
    drop(std::mem::replace(
        &mut world.fixture.nexus.store,
        placeholder.into(),
    ));
    assert_eq!(LogicalState::read(&path), before);
    assert_eq!(before.rows("flow_nexus_pending_reaps").len(), 1);
    let older = sema_engine::TableDescriptor::<StoredLaunchOutcomeV9>::new(
        sema_engine::TableName::new("flow_nexus_launch_outcomes"),
        sema_engine::FamilyName::new("flow-nexus-launch-outcome"),
        sema_engine::SchemaHash::for_label(crate::store::reap_upgrade::LAUNCH_OUTCOME_SCHEMA_V9),
    );
    let refused = sema_engine::Engine::open(
        sema_engine::EngineOpen::new(&path, sema_engine::SchemaVersion::new(1)).with_family(&older),
    );
    assert!(
        matches!(
            refused,
            Err(sema_engine::Error::FamilyIdentityMismatch { .. })
        ),
        "{:?}",
        refused.err()
    );
    for _ in 0..2 {
        assert_eq!(LogicalState::read(&path), before);
        let mut engine = sema_engine::Engine::open(sema_engine::EngineOpen::new(
            &path,
            sema_engine::SchemaVersion::new(1),
        ))
        .unwrap();
        use crate::store::reap_upgrade::DescribesReapUpgrade;
        engine
            .register_table(
                <crate::store::FlowStore as DescribesReapUpgrade>::launch_outcome_descriptor(),
            )
            .expect("the v10 identity still registers");
    }
}

/// The flow whose handover row an Address's next launch finds: X.
const OWNER: &str = "abc123";
/// The Address's last flow, stopped before the next launch: O.
const STOPPED: &str = "0d0d0d";

/// How the owner X stands when a launch judges it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Owner {
    Live,
    Ended,
    Missing,
}

/// An Address whose handover row X owns, its metaflow Asleep with X in
/// Past, as a Stop of X and a fresh Launch find it; launches are seated
/// through the store's own writes and settled through the launch's own
/// `settle`, as Start settles a confirmed flow.
struct HandoverWorld {
    fixture: NexusFixture,
    address: signal_flow::Address,
}

trait SeatsLaunches {
    fn at(owner: Owner) -> Self;
    /// W1 of a launch `id` of `flow`, replacing `predecessor` when named.
    fn acknowledged(&self, id: &str, flow: &str, predecessor: Option<&str>) -> StagedLaunch;
    /// W2 of `flow`; the confirmed Launched.
    fn confirmed(&self, flow: &str) -> signal_flow::Launched;
    /// W1, W2 and the settlement under the gate, as a Launch runs them.
    fn launched(&self, id: &str, flow: &str, predecessor: Option<&str>) -> Response;
    fn outcome(&self, id: &str) -> Option<LaunchOutcome>;
    fn pending(&self) -> Option<crate::PendingHandover>;
    fn rest(&self, flow: &str);
    fn generations(&self) -> Vec<(String, String)>;
    fn owner_now(&self) -> Option<String>;
    fn keep(&self, title: &str) -> Response;
    fn drop_section(&self, title: &str) -> Response;
}

impl SeatsLaunches for HandoverWorld {
    fn at(owner: Owner) -> Self {
        use crate::store::metaflow::SeedsMetaflow;
        let fixture = NexusFixture::new();
        let address = fixture.staged_launch("handover-at", None).profile.address;
        if owner != Owner::Missing {
            fixture.register_predecessor(OWNER);
            if owner == Owner::Ended {
                assert!(fixture.nexus.store.record_stopped(OWNER).unwrap());
            }
        }
        // The Address's last flow O was stopped; the handover row still
        // names X, as Stop and Launch left 751881's.
        fixture.register_predecessor(STOPPED);
        assert!(fixture.nexus.store.record_stopped(STOPPED).unwrap());
        fixture.nexus.store.seed_metaflow(signal_flow::Metaflow {
            address: address.clone(),
            state: flow_ethos::State::Asleep,
            past: flow_ethos::Past(vec![signal_flow::FlowId(STOPPED.into())]),
            queue: flow_ethos::Queue(vec![]),
        });
        fixture
            .nexus
            .store
            .ensure_live_handover(&address, OWNER)
            .unwrap();
        fixture
            .nexus
            .store
            .keep_handover(
                &address,
                &address,
                signal_flow::Section {
                    title: signal_flow::Title("owned".into()),
                    description: signal_flow::Description("X's".into()),
                    body: signal_flow::Body("X wrote this".into()),
                },
                signal_flow::Touch::Unknown,
            )
            .unwrap();
        Self { fixture, address }
    }

    fn acknowledged(&self, id: &str, flow: &str, predecessor: Option<&str>) -> StagedLaunch {
        let mut launch = self.fixture.staged_launch(id, predecessor);
        launch.profile.address = self.address.clone();
        launch.composed.launch_profile.address = self.address.clone();
        launch.binding.flow_id = signal_flow::FlowId(flow.into());
        launch.binding.native_session_id = signal_flow::NativeSessionId(format!("session-{flow}"));
        launch.pane.herdr_pane_id = signal_flow::HerdrPaneId(format!("pane-{flow}"));
        launch.binding.herdr_pane_binding = launch.pane.clone();
        launch.reserve(&self.fixture.nexus);
        launch.record_native_intent(&self.fixture.nexus);
        launch.bind(&self.fixture.nexus);
        launch.register_and_acknowledge(&self.fixture.nexus);
        launch
    }

    fn confirmed(&self, flow: &str) -> signal_flow::Launched {
        match self.fixture.nexus.store.confirm_started(flow).unwrap() {
            Response::Started(launched) => launched,
            other => panic!("{flow} confirmed as {other:?}"),
        }
    }

    fn launched(&self, id: &str, flow: &str, predecessor: Option<&str>) -> Response {
        use crate::launching::LaunchesFlows;
        let launch = self.acknowledged(id, flow, predecessor);
        if let Some(predecessor) = predecessor {
            self.fixture
                .nexus
                .store
                .record_replacement(crate::store::Replacement {
                    launch_request_id: launch.profile.launch_request_id.clone(),
                    predecessor: crate::generated::memory::Predecessor(predecessor.into()),
                })
                .unwrap();
        }
        let launched = self.confirmed(flow);
        let _turn = self.fixture.nexus.dispatch_gate.lock().unwrap();
        self.fixture.nexus.settle(id, Response::Started(launched))
    }

    fn outcome(&self, id: &str) -> Option<LaunchOutcome> {
        self.fixture.nexus.store.launch_outcome(id).unwrap()
    }

    fn pending(&self) -> Option<crate::PendingHandover> {
        self.fixture
            .nexus
            .store
            .pending_handover(&self.address)
            .unwrap()
    }

    fn rest(&self, flow: &str) {
        let _turn = self.fixture.nexus.dispatch_gate.lock().unwrap();
        self.fixture.nexus.take_up_at_rest(flow);
    }

    fn generations(&self) -> Vec<(String, String)> {
        self.fixture
            .nexus
            .store
            .handover_memory(&self.address)
            .unwrap()
            .map(|row| {
                row.closed
                    .into_iter()
                    .map(|closed| (closed.predecessor, closed.successor))
                    .collect()
            })
            .unwrap_or_default()
    }

    fn owner_now(&self) -> Option<String> {
        self.fixture
            .nexus
            .store
            .handover_memory(&self.address)
            .unwrap()
            .and_then(|row| row.current_flow_id)
    }

    fn keep(&self, title: &str) -> Response {
        self.fixture
            .nexus
            .dispatch(Query::Keep(signal_flow::Keep_Data {
                address: self.address.clone(),
                section: signal_flow::Section {
                    title: signal_flow::Title(title.into()),
                    description: signal_flow::Description("kept".into()),
                    body: signal_flow::Body("by the receiving flow".into()),
                },
            }))
    }

    fn drop_section(&self, title: &str) -> Response {
        self.fixture
            .nexus
            .dispatch(Query::Drop(signal_flow::Drop_Data {
                address: self.address.clone(),
                title: signal_flow::Title(title.into()),
            }))
    }
}

const N1: &str = "a1a1a1";
const N2: &str = "b2b2b2";
const S: &str = "c3c3c3";

fn handover_pending(
    response: &Response,
    owner: &str,
    refusal: &signal_flow::HandoverCloseRefusal,
) -> bool {
    matches!(response, Response::HandoverPending(pending)
        if pending.handover_owner.0.0 == owner && pending.handover_close_refusal == *refusal)
}

fn live(owner: &str) -> signal_flow::HandoverCloseRefusal {
    signal_flow::HandoverCloseRefusal::Owner(signal_flow::HandoverOwnerRefusal::Live(
        signal_flow::FlowId(owner.into()),
    ))
}

fn missing(owner: &str) -> signal_flow::HandoverCloseRefusal {
    signal_flow::HandoverCloseRefusal::Owner(signal_flow::HandoverOwnerRefusal::Missing(
        signal_flow::FlowId(owner.into()),
    ))
}

fn unknown(owner: &str) -> signal_flow::HandoverCloseRefusal {
    signal_flow::HandoverCloseRefusal::Owner(signal_flow::HandoverOwnerRefusal::Unknown(
        signal_flow::FlowId(owner.into()),
    ))
}

/// Test 14 (ruling 6): an owner that ended is told from one live, missing
/// or unread; each refusal keeps the successor receiving and the row
/// pending.
#[test]
fn handover_owner_ended_is_told_from_live_missing_and_unknown() {
    use crate::store::handover::{ClosesHandovers, WritesHandoverClosing};
    for (owner, expected) in [
        (Owner::Ended, None),
        (Owner::Live, Some(live(OWNER))),
        (Owner::Missing, Some(missing(OWNER))),
    ] {
        let world = HandoverWorld::at(owner);
        let closing = world
            .fixture
            .nexus
            .store
            .handover_closing(&world.address, "", N1);
        match (&closing, &expected) {
            (Ok(closing), None) => assert!(closing.moves(), "{owner:?}"),
            (Err(refusal), Some(expected)) => assert_eq!(refusal, expected, "{owner:?}"),
            _ => panic!("{owner:?}: {:?}", closing.as_ref().err()),
        }
        drop(closing);
        let answer = world.launched("judged", N1, None);
        match &expected {
            None => assert!(matches!(answer, Response::Started(_)), "{answer:?}"),
            Some(refusal) => {
                assert!(handover_pending(&answer, OWNER, refusal), "{answer:?}");
                assert!(world.pending().is_some());
                assert_eq!(
                    world.fixture.nexus.store.succession(N1).unwrap(),
                    Succession::Receiving
                );
            }
        }
    }
    // Retired and Exited are ended as Stopped is.
    for end in ["retired", "exited"] {
        let world = HandoverWorld::at(Owner::Live);
        if end == "retired" {
            assert!(world.fixture.nexus.store.record_retired(OWNER).unwrap());
        } else {
            world.fixture.nexus.store.mark_exited(OWNER);
        }
        assert!(matches!(
            world.launched("ended", N1, None),
            Response::Started(_)
        ));
        assert_eq!(
            world.generations(),
            vec![(OWNER.into(), N1.into())],
            "{end}"
        );
    }
    // An owner row that cannot be read is Unknown.
    let world = HandoverWorld::at(Owner::Live);
    world
        .fixture
        .nexus
        .store
        .faults
        .refuse_next(RefusedWrite::FlowRead);
    let answer = world.launched("unread", N1, None);
    assert!(
        handover_pending(&answer, OWNER, &unknown(OWNER)),
        "{answer:?}"
    );
}

/// Tests 15 and 16: a launch whose handover cannot move answers
/// HandoverPending and receives; its next rest, the owner ended, takes the
/// handover over.
#[test]
fn a_launch_whose_handover_cannot_move_answers_pending_and_its_rest_takes_it_up() {
    let world = HandoverWorld::at(Owner::Live);
    let answer = world.launched("pending", N1, None);
    assert!(handover_pending(&answer, OWNER, &live(OWNER)), "{answer:?}");
    assert!(matches!(
        world.outcome("pending"),
        Some(LaunchOutcome::HandoverPending(_))
    ));
    let row = world.pending().expect("the PendingHandover");
    assert_eq!(row.successor.0.0, N1);
    assert_eq!(
        world.fixture.nexus.store.succession(N1).unwrap(),
        Succession::Receiving
    );
    // Test 16.
    assert!(world.fixture.nexus.store.record_stopped(OWNER).unwrap());
    world.rest(N1);
    assert!(matches!(
        world.outcome("pending"),
        Some(LaunchOutcome::Started(_))
    ));
    assert!(world.pending().is_none());
    assert_eq!(world.generations(), vec![(OWNER.into(), N1.into())]);
    assert_eq!(world.owner_now().as_deref(), Some(N1));
}

/// Test 17 ([PSYCHE-3] (a)): the receiving flow's own handover write takes
/// the pending handover over before the write; the owner still live, it is
/// refused NotOwner, one retry per write; Drop as the first write does the
/// same; the derived handover (C2 refused before) runs C1 first.
#[test]
fn a_pending_handover_is_taken_up_by_the_started_flows_own_handover_write() {
    let not_owner = |response: &Response| {
        matches!(
            response,
            Response::KeepRefused(signal_flow::HandoverRefusal::NotOwner)
                | Response::DropRefused(signal_flow::HandoverRefusal::NotOwner)
        )
    };
    // The owner still live: refused, the row kept.
    let world = HandoverWorld::at(Owner::Live);
    world.launched("write", N1, None);
    let attempts = world.fixture.nexus.store.faults.attempts().len();
    assert!(not_owner(&world.keep("mine")));
    assert_eq!(world.pending().unwrap().handover_close_refusal, live(OWNER));
    assert!(
        world.fixture.nexus.store.faults.attempts().len() <= attempts + 1,
        "one retry per write"
    );
    // Keep, the owner ended: the transfer, then the write.
    assert!(world.fixture.nexus.store.record_stopped(OWNER).unwrap());
    assert!(matches!(world.keep("mine"), Response::Kept(_)));
    assert!(world.pending().is_none());
    assert_eq!(world.generations(), vec![(OWNER.into(), N1.into())]);
    // Drop as the first write.
    let world = HandoverWorld::at(Owner::Live);
    world.launched("drop", N1, None);
    assert!(world.fixture.nexus.store.record_stopped(OWNER).unwrap());
    assert!(matches!(world.drop_section("owned"), Response::Dropped(_)));
    assert!(world.pending().is_none());
    assert_eq!(world.generations(), vec![(OWNER.into(), N1.into())]);
    // The derived handover: C2 refused, no row; the write runs C1 first.
    let world = HandoverWorld::at(Owner::Live);
    world
        .fixture
        .nexus
        .store
        .faults
        .refuse_next(RefusedWrite::HandoverOwing);
    assert!(handover_pending(
        &world.launched("derived", N1, None),
        OWNER,
        &live(OWNER)
    ));
    assert!(world.pending().is_none() && world.outcome("derived").is_none());
    assert!(world.fixture.nexus.store.record_stopped(OWNER).unwrap());
    assert!(matches!(world.keep("mine"), Response::Kept(_)));
    assert!(matches!(
        world.outcome("derived"),
        Some(LaunchOutcome::Started(_))
    ));
    assert_eq!(world.generations(), vec![(OWNER.into(), N1.into())]);
}

/// Tests 13 and 18: a launch after a Stop takes the handover of the flow it
/// succeeds; an owner that ended (the 2bc724 cause) is closed by the next
/// succession, the generation naming its real owner.
#[test]
fn a_launch_after_a_stop_takes_the_handover_of_the_flow_it_succeeds() {
    let world = HandoverWorld::at(Owner::Ended);
    assert!(matches!(
        world.launched("after-stop", N1, None),
        Response::Started(_)
    ));
    assert_eq!(world.generations(), vec![(OWNER.into(), N1.into())]);
    assert_eq!(world.owner_now().as_deref(), Some(N1));
    // Test 13: N1 replaced by S, the owner N1 ended by then is the real one.
    let world = HandoverWorld::at(Owner::Ended);
    assert!(matches!(
        world.launched("first", N1, None),
        Response::Started(_)
    ));
    let answer = world.launched("second", S, Some(N1));
    assert!(matches!(&answer, Response::ReapPending(pending)
        if pending.predecessor.0.0 == N1 && pending.reap_standing == ReapStanding::Owed));
    // The pane seen gone: step 3 under the gate.
    {
        let _turn = world.fixture.nexus.dispatch_gate.lock().unwrap();
        assert!(matches!(
            world.fixture.nexus.settle_reap(N1),
            Some(Response::Replaced(_))
        ));
    }
    assert_eq!(
        world.generations(),
        vec![(OWNER.into(), N1.into()), (N1.into(), S.into())]
    );
    assert_eq!(world.owner_now().as_deref(), Some(S));
}

use crate::reaping::ReapsPredecessors;
use crate::store::RecordsReplacement;

/// H1 (F4, F5): a refused HandoverOwed keeps the obligation recoverable: the
/// next rest settles it; C1 refused with the owner ended answers
/// PersistenceRefused with the row; both refused, the Nexus start writes C2.
#[test]
fn a_refused_handover_owing_commit_keeps_the_obligation_recoverable() {
    let world = HandoverWorld::at(Owner::Live);
    world
        .fixture
        .nexus
        .store
        .faults
        .refuse_next(RefusedWrite::HandoverOwing);
    let answer = world.launched("refused-owing", N1, None);
    assert!(handover_pending(&answer, OWNER, &live(OWNER)), "{answer:?}");
    assert!(world.pending().is_none() && world.outcome("refused-owing").is_none());
    assert_eq!(
        world.fixture.nexus.store.succession(N1).unwrap(),
        Succession::Receiving
    );
    assert!(world.fixture.nexus.store.record_stopped(OWNER).unwrap());
    world.rest(N1);
    assert!(matches!(
        world.outcome("refused-owing"),
        Some(LaunchOutcome::Started(_))
    ));
    assert_eq!(world.generations(), vec![(OWNER.into(), N1.into())]);
    assert!(world.pending().is_none());

    // C1 refused, the owner ended: C2 at PersistenceRefused, with its row.
    let world = HandoverWorld::at(Owner::Ended);
    world
        .fixture
        .nexus
        .store
        .faults
        .refuse_next(RefusedWrite::StartedSettle);
    let answer = world.launched("refused-started", N1, None);
    assert!(
        handover_pending(
            &answer,
            OWNER,
            &signal_flow::HandoverCloseRefusal::PersistenceRefused
        ),
        "{answer:?}"
    );
    assert!(world.pending().is_some());
    world.rest(N1);
    assert!(matches!(
        world.outcome("refused-started"),
        Some(LaunchOutcome::Started(_))
    ));
    assert!(world.pending().is_none());

    // Both refused, the owner live: the Nexus start writes C2.
    let mut world = HandoverWorld::at(Owner::Live);
    world
        .fixture
        .nexus
        .store
        .faults
        .refuse_next(RefusedWrite::HandoverOwing);
    world.launched("refused-both", N1, None);
    assert!(world.outcome("refused-both").is_none());
    world.fixture.restarted();
    world.fixture.nexus.recover_wakes();
    assert!(matches!(
        world.outcome("refused-both"),
        Some(LaunchOutcome::HandoverPending(_))
    ));
    assert_eq!(world.pending().unwrap().handover_close_refusal, live(OWNER));
}

/// H2: a later start supersedes a pending handover and says so; with the
/// owner ended before it, one generation names the owner and the later
/// flow.
#[test]
fn a_later_start_supersedes_a_pending_handover_and_says_so() {
    for owner_ends in [false, true] {
        let world = HandoverWorld::at(Owner::Live);
        assert!(handover_pending(
            &world.launched("id1", N1, None),
            OWNER,
            &live(OWNER)
        ));
        assert!(world.fixture.nexus.store.record_stopped(N1).unwrap());
        if owner_ends {
            assert!(world.fixture.nexus.store.record_stopped(OWNER).unwrap());
        }
        let answer = world.launched("id2", N2, None);
        if owner_ends {
            assert!(matches!(answer, Response::Started(_)), "{answer:?}");
            assert_eq!(world.generations(), vec![(OWNER.into(), N2.into())]);
            assert!(world.pending().is_none());
        } else {
            assert!(handover_pending(&answer, OWNER, &live(OWNER)), "{answer:?}");
            let row = world.pending().expect("one row");
            assert_eq!(
                (row.successor.0.0.as_str(), row.launch_request_id.0.as_str()),
                (N2, "id2")
            );
        }
        assert!(matches!(
            world.outcome("id1"),
            Some(LaunchOutcome::HandoverSuperseded(superseded))
                if superseded.handover_owner.0.0 == OWNER
                    && superseded.launched_record.flow_id.0 == N1
                    && superseded.successor.0.0 == N2
        ));
        assert!(matches!(
            world.fixture.nexus.dispatch(Query::LaunchStatus(signal_flow::LaunchRequestId("id1".into()))),
            Response::LaunchInspected(inspection)
                if matches!(inspection.launch_status_outcome_option, Some(LaunchStatusOutcome::HandoverSuperseded(_)))
        ));
    }
}

/// H3 and H4: a replacement supersedes a pending handover, its reap then
/// carrying the handover; a later start settles a reap whose only step left
/// was the handover.
#[test]
fn a_replacement_supersedes_a_pending_handover_and_a_later_start_settles_its_reap() {
    let world = HandoverWorld::at(Owner::Live);
    assert!(handover_pending(
        &world.launched("id1", N1, None),
        OWNER,
        &live(OWNER)
    ));
    let answer = world.launched("replace", S, Some(N1));
    assert!(matches!(&answer, Response::ReapPending(pending) if pending.predecessor.0.0 == N1));
    // H3: the PendingHandover retracted, id1 superseded by S.
    assert!(world.pending().is_none());
    assert!(matches!(
        world.outcome("id1"),
        Some(LaunchOutcome::HandoverSuperseded(superseded)) if superseded.successor.0.0 == S
    ));
    // The pane seen gone: the handover step is refused, the owner live.
    {
        let _turn = world.fixture.nexus.dispatch_gate.lock().unwrap();
        world.fixture.nexus.settle_reap(N1);
    }
    let reap = world.fixture.nexus.store.pending_reap(N1).unwrap().unwrap();
    assert_eq!(
        reap.reap_standing,
        ReapStanding::HandoverRefused(live(OWNER))
    );
    assert_eq!(world.fixture.nexus.store.pending_reaps().unwrap().len(), 1);
    // H4: S ends; a later start at the Address settles the reap.
    assert!(world.fixture.nexus.store.record_stopped(S).unwrap());
    assert!(world.fixture.nexus.store.record_stopped(OWNER).unwrap());
    assert!(matches!(
        world.launched("later", N2, None),
        Response::Started(_)
    ));
    assert!(
        world
            .fixture
            .nexus
            .store
            .pending_reap(N1)
            .unwrap()
            .is_none()
    );
    assert!(matches!(
        world.outcome("replace"),
        Some(LaunchOutcome::Replaced(_))
    ));
    assert_eq!(world.generations(), vec![(OWNER.into(), N2.into())]);
}

/// H5: a refused superseding commit leaves every obligation standing; the
/// later flow's rest then supersedes.
#[test]
fn a_refused_superseding_commit_leaves_every_obligation_standing() {
    let world = HandoverWorld::at(Owner::Live);
    assert!(handover_pending(
        &world.launched("id1", N1, None),
        OWNER,
        &live(OWNER)
    ));
    assert!(world.fixture.nexus.store.record_stopped(N1).unwrap());
    world
        .fixture
        .nexus
        .store
        .faults
        .refuse_next(RefusedWrite::HandoverOwing);
    world.launched("id2", N2, None);
    assert!(matches!(
        world.outcome("id1"),
        Some(LaunchOutcome::HandoverPending(_))
    ));
    assert_eq!(world.pending().unwrap().successor.0.0, N1);
    assert_eq!(
        world.fixture.nexus.store.succession(N2).unwrap(),
        Succession::Receiving
    );
    world.rest(N2);
    assert!(matches!(
        world.outcome("id1"),
        Some(LaunchOutcome::HandoverSuperseded(_))
    ));
    assert_eq!(world.pending().unwrap().successor.0.0, N2);
}

/// M1 ([PSYCHE-2]): a missing owner persists typed, and no event retries
/// it: rests, the flow's own writes, a restart and announcements commit
/// nothing; a later launch's settlement supersedes it and records Missing
/// again.
#[test]
fn a_missing_owner_persists_typed_and_no_event_retries_it() {
    let mut world = HandoverWorld::at(Owner::Missing);
    let answer = world.launched("missing", N1, None);
    assert!(
        handover_pending(&answer, OWNER, &missing(OWNER)),
        "{answer:?}"
    );
    let attempts = world.fixture.nexus.store.faults.attempts().len();
    world.rest(N1);
    world.rest(N1);
    assert!(matches!(
        world.keep("mine"),
        Response::KeepRefused(signal_flow::HandoverRefusal::NotOwner)
    ));
    assert!(matches!(
        world.drop_section("owned"),
        Response::DropRefused(signal_flow::HandoverRefusal::NotOwner)
    ));
    assert_eq!(world.fixture.nexus.store.faults.attempts().len(), attempts);
    world.fixture.restarted();
    world.fixture.nexus.recover_wakes();
    with_supervisor(&world.fixture, || {
        for _ in 0..5 {
            world.fixture.nexus.store.launch_changes.announce();
        }
    });
    assert_eq!(world.fixture.nexus.store.faults.attempts().len(), 0);
    assert_eq!(
        world.pending().unwrap().handover_close_refusal,
        missing(OWNER)
    );
    assert!(matches!(
        world.outcome("missing"),
        Some(LaunchOutcome::HandoverPending(_))
    ));
    // A later launch supersedes it in its own settlement.
    assert!(world.fixture.nexus.store.record_stopped(N1).unwrap());
    assert!(handover_pending(
        &world.launched("later", N2, None),
        OWNER,
        &missing(OWNER)
    ));
    assert!(matches!(
        world.outcome("missing"),
        Some(LaunchOutcome::HandoverSuperseded(_))
    ));
    assert_eq!(world.pending().unwrap().successor.0.0, N2);
}

/// M3: an unread owner is judged again at the next event and, readable and
/// ended, closes the generation.
#[test]
fn an_unreadable_owner_or_predecessor_is_judged_again_at_the_next_event() {
    let world = HandoverWorld::at(Owner::Live);
    world
        .fixture
        .nexus
        .store
        .faults
        .refuse_next(RefusedWrite::FlowRead);
    assert!(handover_pending(
        &world.launched("unread", N1, None),
        OWNER,
        &unknown(OWNER)
    ));
    assert!(world.fixture.nexus.store.record_stopped(OWNER).unwrap());
    world.rest(N1);
    assert!(matches!(
        world.outcome("unread"),
        Some(LaunchOutcome::Started(_))
    ));
    assert_eq!(world.generations(), vec![(OWNER.into(), N1.into())]);
}

/// The fresh-start crash points K6 to K11, each recovered once.
#[test]
fn a_crash_at_each_fresh_start_point_is_recovered_once() {
    // K6: after W2 with a generation to move, for each owner standing; K8:
    // with nothing to move.
    for owner in [Owner::Ended, Owner::Live, Owner::Missing] {
        let mut world = HandoverWorld::at(owner);
        world.acknowledged("k6", N1, None);
        world
            .fixture
            .nexus
            .store
            .faults
            .crash_at(CrashPoint::AfterConfirm);
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| world.confirmed(N1))).is_err()
        );
        world.fixture.restarted();
        world.fixture.nexus.recover_wakes();
        match owner {
            Owner::Ended => {
                assert!(matches!(
                    world.outcome("k6"),
                    Some(LaunchOutcome::Started(_))
                ));
                assert_eq!(world.generations(), vec![(OWNER.into(), N1.into())]);
            }
            Owner::Live => assert_eq!(world.pending().unwrap().handover_close_refusal, live(OWNER)),
            Owner::Missing => {
                assert_eq!(
                    world.pending().unwrap().handover_close_refusal,
                    missing(OWNER)
                )
            }
        }
        let settled = world.fixture.restarted();
        world.fixture.nexus.recover_wakes();
        assert_eq!(world.fixture.restarted(), settled, "K6 {owner:?}: once");
    }
    // K8: no handover row, no past flow.
    {
        use crate::store::metaflow::SeedsMetaflow;
        let mut world = HandoverWorld::at(Owner::Ended);
        world.address = world
            .fixture
            .staged_launch("nothing-to-move", None)
            .profile
            .address;
        world
            .fixture
            .nexus
            .store
            .seed_metaflow(signal_flow::Metaflow {
                address: world.address.clone(),
                state: flow_ethos::State::Asleep,
                past: flow_ethos::Past(vec![]),
                queue: flow_ethos::Queue(vec![]),
            });
        world.acknowledged("k8", N1, None);
        world
            .fixture
            .nexus
            .store
            .faults
            .crash_at(CrashPoint::AfterConfirm);
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| world.confirmed(N1))).is_err()
        );
        world.fixture.restarted();
        world.fixture.nexus.recover_wakes();
        assert!(matches!(
            world.outcome("k8"),
            Some(LaunchOutcome::Started(_))
        ));
        assert!(world.generations().is_empty());
    }
    // K7: after C2; the first rest takes it up once the owner has ended.
    {
        let mut world = HandoverWorld::at(Owner::Live);
        world
            .fixture
            .nexus
            .store
            .faults
            .crash_at(CrashPoint::AfterHandoverOwed);
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(
                || world.launched("k7", N1, None)
            ))
            .is_err()
        );
        world.fixture.restarted();
        world.fixture.nexus.recover_wakes();
        assert_eq!(world.pending().unwrap().handover_close_refusal, live(OWNER));
        assert!(world.fixture.nexus.store.record_stopped(OWNER).unwrap());
        world.rest(N1);
        assert!(matches!(
            world.outcome("k7"),
            Some(LaunchOutcome::Started(_))
        ));
    }
    // K9: after C1: nothing owed.
    {
        let mut world = HandoverWorld::at(Owner::Ended);
        world
            .fixture
            .nexus
            .store
            .faults
            .crash_at(CrashPoint::AfterHandedOver);
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(
                || world.launched("k9", N1, None)
            ))
            .is_err()
        );
        let first = world.fixture.restarted();
        world.fixture.nexus.recover_wakes();
        assert!(matches!(
            world.outcome("k9"),
            Some(LaunchOutcome::Started(_))
        ));
        assert_eq!(world.fixture.restarted(), first, "K9: nothing written");
    }
    // K10: after a later launch's W2, while the row names N1; K11: after
    // the superseding commit.
    for point in [CrashPoint::AfterConfirm, CrashPoint::AfterHandoverOwed] {
        let mut world = HandoverWorld::at(Owner::Live);
        assert!(handover_pending(
            &world.launched("id1", N1, None),
            OWNER,
            &live(OWNER)
        ));
        assert!(world.fixture.nexus.store.record_stopped(N1).unwrap());
        world.fixture.nexus.store.faults.crash_at(point);
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(
                || world.launched("id2", N2, None)
            ))
            .is_err()
        );
        world.fixture.restarted();
        world.fixture.nexus.recover_wakes();
        assert!(
            matches!(
                world.outcome("id1"),
                Some(LaunchOutcome::HandoverSuperseded(_))
            ),
            "{point:?}"
        );
        assert_eq!(world.pending().unwrap().successor.0.0, N2, "{point:?}");
        let settled = world.fixture.restarted();
        world.fixture.nexus.recover_wakes();
        assert_eq!(world.fixture.restarted(), settled, "{point:?}: once");
    }
}

/// K4: after each reap refusal, its own event takes it up: HerdrUnavailable
/// and CloseRefused by a worker armed at the start, HandoverRefused,
/// SettleUnrecorded and PredecessorUnknown by the successor's first rest,
/// in the store alone.
#[test]
fn a_crash_after_each_reap_refusal_is_taken_up_by_its_own_event() {
    for standing in [
        ReapStanding::HerdrUnavailable,
        ReapStanding::CloseRefused,
        ReapStanding::SettleUnrecorded,
        ReapStanding::HandoverRefused(live(OWNER)),
        ReapStanding::PredecessorUnknown,
    ] {
        let mut world = ReapWorld::staged("refusal-restart");
        assert!(is_reap_pending(&world.replace(), ReapStanding::Owed));
        assert!(
            world
                .fixture
                .nexus
                .store
                .reap_refused(PREDECESSOR, standing.clone())
                .unwrap()
        );
        world.fixture.restarted();
        world.fixture.nexus.recover_wakes();
        assert_eq!(
            world.standing(),
            Some(standing.clone()),
            "the start writes nothing"
        );
        world.show(PaneShown::Absent);
        match &standing {
            ReapStanding::HerdrUnavailable | ReapStanding::CloseRefused => {
                with_supervisor(&world.fixture, || {
                    world.until("the reap settled", &mut || world.standing().is_none());
                });
            }
            _ => {
                let calls = world.calls().len();
                {
                    let _turn = world.fixture.nexus.dispatch_gate.lock().unwrap();
                    world.fixture.nexus.take_up_at_rest(SUCCESSOR);
                }
                assert_eq!(world.calls().len(), calls, "{standing:?}: the store alone");
                if standing == ReapStanding::PredecessorUnknown {
                    assert_eq!(world.standing(), Some(ReapStanding::Owed));
                } else {
                    assert!(world.standing().is_none(), "{standing:?}");
                }
            }
        }
    }
}

/// F2: a refused refusal rewrite keeps the earlier standing; F7: a refused
/// HandedOver writes PersistenceRefused and the next rest settles it.
#[test]
fn a_refused_rewrite_keeps_the_earlier_and_a_refused_settlement_is_retried_at_the_next_event() {
    // F2.
    let world = ReapWorld::staged("refused-rewrite");
    world.close_exits(1);
    world
        .fixture
        .nexus
        .store
        .faults
        .refuse_next(RefusedWrite::ReapRewrite);
    with_supervisor(&world.fixture, || {
        assert!(is_reap_pending(&world.replace(), ReapStanding::Owed));
        world.until("the refused rewrite", &mut || {
            world
                .fixture
                .nexus
                .store
                .faults
                .attempts()
                .iter()
                .any(|a| a == "ReapRefused")
        });
        assert_eq!(world.standing(), Some(ReapStanding::Owed));
    });
    // F7.
    let world = HandoverWorld::at(Owner::Live);
    assert!(handover_pending(
        &world.launched("f7", N1, None),
        OWNER,
        &live(OWNER)
    ));
    assert!(world.fixture.nexus.store.record_stopped(OWNER).unwrap());
    world
        .fixture
        .nexus
        .store
        .faults
        .refuse_next(RefusedWrite::HandedOver);
    world.rest(N1);
    assert_eq!(
        world.pending().unwrap().handover_close_refusal,
        signal_flow::HandoverCloseRefusal::PersistenceRefused
    );
    world.rest(N1);
    assert!(matches!(
        world.outcome("f7"),
        Some(LaunchOutcome::Started(_))
    ));
    assert!(world.pending().is_none());
    // F6: a refused HandoverRefused rewrite keeps the earlier refusal.
    let world = HandoverWorld::at(Owner::Live);
    assert!(handover_pending(
        &world.launched("f6", N1, None),
        OWNER,
        &live(OWNER)
    ));
    let faults = &world.fixture.nexus.store.faults;
    faults.refuse_next(RefusedWrite::FlowRead);
    faults.refuse_next(RefusedWrite::HandoverRewrite);
    world.rest(N1);
    assert_eq!(world.pending().unwrap().handover_close_refusal, live(OWNER));
}

/// I1 ([PSYCHE-1] (a)): Inspect lists each pending reap and handover at its
/// Address with Missing and Unknown typed, the derived ones included, and
/// writes nothing.
#[test]
fn inspect_lists_pending_reaps_and_handovers_with_missing_and_unknown_typed() {
    let handovers = |world: &HandoverWorld| -> Vec<signal_flow::HandoverCloseRefusal> {
        match world
            .fixture
            .nexus
            .dispatch_meta(meta_signal_flow::Query::Inspect(world.address.clone()))
        {
            meta_signal_flow::Response::Inspected(inspected) => inspected
                .visible_attempt_vector
                .into_iter()
                .filter_map(|attempt| match attempt {
                    meta_signal_flow::VisibleAttempt::Handover(handover) => {
                        Some(handover.handover_close_refusal)
                    }
                    _ => None,
                })
                .collect(),
            other => panic!("{other:?}"),
        }
    };
    for (owner, expected) in [(Owner::Live, live(OWNER)), (Owner::Missing, missing(OWNER))] {
        let world = HandoverWorld::at(owner);
        world.launched("inspected", N1, None);
        assert_eq!(handovers(&world), vec![expected], "{owner:?}");
    }
    let world = HandoverWorld::at(Owner::Live);
    world
        .fixture
        .nexus
        .store
        .faults
        .refuse_next(RefusedWrite::FlowRead);
    world.launched("inspected", N1, None);
    assert_eq!(handovers(&world), vec![unknown(OWNER)]);
    // The derived handover, its owner ended: only the write is owed.
    let mut world = HandoverWorld::at(Owner::Live);
    world
        .fixture
        .nexus
        .store
        .faults
        .refuse_next(RefusedWrite::HandoverOwing);
    world.launched("derived", N1, None);
    assert!(world.fixture.nexus.store.record_stopped(OWNER).unwrap());
    assert_eq!(
        handovers(&world),
        vec![signal_flow::HandoverCloseRefusal::PersistenceRefused]
    );
    // With its owner unread during Inspect.
    assert!(
        world
            .fixture
            .nexus
            .store
            .record_active(OWNER)
            .unwrap_or(false)
            || true
    );
    world
        .fixture
        .nexus
        .store
        .faults
        .refuse_next(RefusedWrite::FlowRead);
    assert_eq!(handovers(&world), vec![unknown(OWNER)]);
    // Inspect writes nothing and arms no watch.
    let before = world.fixture.restarted();
    handovers(&world);
    assert_eq!(world.fixture.restarted(), before);
    // The reaps: CloseRefused and PredecessorUnknown each as themselves.
    for standing in [ReapStanding::CloseRefused, ReapStanding::PredecessorUnknown] {
        let world = ReapWorld::staged("inspected-reap");
        assert!(is_reap_pending(&world.replace(), ReapStanding::Owed));
        assert!(
            world
                .fixture
                .nexus
                .store
                .reap_refused(PREDECESSOR, standing.clone())
                .unwrap()
        );
        assert_eq!(inspected_reap(&world), Some(standing));
    }
}

/// W1: the worker subscribes before its opening snapshot: with the
/// acknowledgement withheld no snapshot is taken; once it is given, the
/// snapshot and the close follow, and Herdr's event settles the reap.
#[test]
fn a_pane_watch_subscribes_before_its_opening_snapshot() {
    let world = ReapWorld::staged("subscribe-first");
    world.events.withhold_acknowledgement();
    with_supervisor(&world.fixture, || {
        assert!(is_reap_pending(&world.replace(), ReapStanding::Owed));
        world
            .events
            .wait_for("the subscription", |subscribed, _| subscribed >= 1);
        assert_eq!(
            world.snapshots_of_predecessor(),
            0,
            "no snapshot before the subscription"
        );
        world.events.release_acknowledgement();
        world.until("the close", &mut || world.closes() == 1);
        world.events.emit_gone(PREDECESSOR_PANE);
        world.until("the reap settled", &mut || world.standing().is_none());
    });
    let calls = world.calls();
    let order = |what: &str| calls.iter().position(|call| call.contains(what)).unwrap();
    assert!(order("events.subscribe") < order("messaging-build api snapshot"));
    assert!(order("messaging-build api snapshot") < order("pane close w1:p3"));
}

/// The opening snapshot unread: the reap is written HerdrUnavailable under
/// the gate and the worker waits on its subscription; Herdr's own event
/// settles it with no close.
#[test]
fn an_unreadable_snapshot_writes_herdr_unavailable_and_waits_for_the_event() {
    let world = ReapWorld::staged("unreadable");
    world.show(PaneShown::Unreadable);
    with_supervisor(&world.fixture, || {
        assert!(is_reap_pending(&world.replace(), ReapStanding::Owed));
        world.until("HerdrUnavailable", &mut || {
            world.standing() == Some(ReapStanding::HerdrUnavailable)
        });
        assert_eq!(inspected_reap(&world), Some(ReapStanding::HerdrUnavailable));
        world.events.emit_gone(PREDECESSOR_PANE);
        world.until("the reap settled", &mut || world.standing().is_none());
    });
    assert_eq!(world.closes(), 0);
    assert!(matches!(
        world.launch_status(),
        LaunchStatusOutcome::Replaced(_)
    ));
}

/// W12: no gated path of a reap calls Herdr: the Replace, the successor's
/// rest at each standing and the Nexus start each make their reap's Herdr
/// calls, if any, with the gate free; only the worker calls Herdr.
#[test]
fn no_gated_path_of_a_reap_calls_herdr() {
    let mut world = ReapWorld::staged("no-gated-herdr");
    world.close_exits(1);
    let reap_calls = |world: &ReapWorld| {
        world
            .calls()
            .into_iter()
            .filter(|call| {
                call == "--session messaging-build api snapshot"
                    || call.contains("pane close w1:p3")
            })
            .count()
    };
    // The Replace and the rest under the gate call nothing of the reap.
    assert!(is_reap_pending(&world.replace(), ReapStanding::Owed));
    assert_eq!(
        reap_calls(&world),
        0,
        "the Replace calls no Herdr of the reap"
    );
    for standing in [
        ReapStanding::Owed,
        ReapStanding::HerdrUnavailable,
        ReapStanding::CloseRefused,
        ReapStanding::SettleUnrecorded,
        ReapStanding::PredecessorUnknown,
        ReapStanding::PredecessorMissing,
    ] {
        assert!(
            world
                .fixture
                .nexus
                .store
                .reap_refused(PREDECESSOR, standing.clone())
                .unwrap()
        );
        let _turn = world.fixture.nexus.dispatch_gate.lock().unwrap();
        world.fixture.nexus.take_up_at_rest(SUCCESSOR);
        assert_eq!(
            reap_calls(&world),
            0,
            "{standing:?}: the rest calls no Herdr"
        );
        if world.standing().is_none() {
            break;
        }
    }
    // The start, its rows standing, calls nothing either; its worker calls
    // Herdr with the gate free.
    let mut world2 = ReapWorld::staged("no-gated-herdr-start");
    world2.close_exits(1);
    assert!(is_reap_pending(&world2.replace(), ReapStanding::Owed));
    world2.fixture.restarted();
    world2.fixture.nexus.recover_wakes();
    assert_eq!(reap_calls(&world2), 0, "the start calls no Herdr");
    std::thread::scope(|scope| {
        let probe = GateProbe::armed(&world2, scope);
        with_supervisor(&world2.fixture, || {
            world2.until("the worker's refused close", &mut || {
                world2.standing() == Some(ReapStanding::CloseRefused)
            });
        });
        probe.stop();
        let seen = probe.seen();
        assert!(!seen.is_empty());
        assert!(seen.iter().all(|(_, free)| *free), "{seen:?}");
    });
    world.fixture.restarted();
}

/// Test 20 through the Nexus's own configured open: a 0.54.6 store, its
/// launch outcomes at v9 and no obligation family, opens upgraded at once
/// (the families the upgrade registers are not declared to the open, which
/// would register them first); a refused reap whose launch has no attempt
/// refuses the open typed, the store's logical rows and catalog unchanged.
#[test]
fn a_0_54_6_store_opens_upgraded_through_the_configured_open() {
    use crate::generated::starter::LaunchedRecord;
    let configured = super::configuring::ConfiguredStore::new();
    let path = configured.path().to_path_buf();
    drop(crate::store::FlowStore::open_configured(&path).expect("the running store opens"));
    v9_store_at(
        &path,
        vec![StoredLaunchOutcomeV9 {
            launch_request_id: "settled-start".into(),
            launch_outcome: LaunchOutcomeV9::Started(LaunchedRecord {
                flow_id: signal_flow::FlowId("0000aa".into()),
                session_id: signal_flow::SessionId("s".into()),
                origin: signal_flow::Origin::Unbound,
            }),
        }],
        crate::store::FLOW_SCHEMA_VERSION,
    );
    let v9 = LogicalState::read(&path);
    assert!(!v9.catalog.contains_key("flow_nexus_pending_reaps"));
    let opened = crate::store::FlowStore::open_configured(&path).expect("the 0.54.6 store opens");
    assert!(matches!(
        opened.launch_outcome("settled-start").unwrap(),
        Some(LaunchOutcome::Started(_))
    ));
    assert!(opened.pending_reaps().unwrap().is_empty());
    drop(opened);
    let upgraded = LogicalState::read(&path);
    assert!(upgraded.catalog.contains_key("flow_nexus_pending_reaps"));
    assert!(
        upgraded
            .catalog
            .contains_key("flow_nexus_pending_handovers")
    );
    drop(crate::store::FlowStore::open_configured(&path).expect("a second open"));
    assert_eq!(
        LogicalState::read(&path),
        upgraded,
        "a second open is a no-op"
    );

    // A v9 family with no row, as a store that never launched holds it.
    let configured = super::configuring::ConfiguredStore::new();
    let path = configured.path().to_path_buf();
    drop(crate::store::FlowStore::open_configured(&path).expect("the running store opens"));
    v9_store_at(&path, Vec::new(), crate::store::FLOW_SCHEMA_VERSION);
    drop(crate::store::FlowStore::open_configured(&path).expect("an empty v9 family opens"));
    let empty = LogicalState::read(&path);
    assert!(empty.catalog.contains_key("flow_nexus_pending_reaps"));
    drop(crate::store::FlowStore::open_configured(&path).expect("a second open"));
    assert_eq!(LogicalState::read(&path), empty);

    // A refused reap whose launch has no attempt refuses the open.
    let configured = super::configuring::ConfiguredStore::new();
    let path = configured.path().to_path_buf();
    drop(crate::store::FlowStore::open_configured(&path).expect("the running store opens"));
    v9_store_at(
        &path,
        vec![StoredLaunchOutcomeV9 {
            launch_request_id: "orphaned-reap".into(),
            launch_outcome: LaunchOutcomeV9::ReplaceRejected(ReplaceRejectionV9::ReapRefused(
                signal_flow::StopRejection::RouteUnavailable,
            )),
        }],
        crate::store::FLOW_SCHEMA_VERSION,
    );
    let before = LogicalState::read(&path);
    let refused = crate::store::FlowStore::open_configured(&path);
    assert!(
        matches!(&refused, Err(crate::store::StoreError::ReapUpgrade(_))),
        "{:?}",
        refused.as_ref().err()
    );
    drop(refused);
    assert_eq!(LogicalState::read(&path), before);
}
