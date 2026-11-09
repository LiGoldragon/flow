//! The service is started with one datom, its place: its sockets in Runtime,
//! its memory in State. Configuration reaches the running Nexus through its
//! meta socket and takes effect at once; nothing is read from the
//! environment for either. Every wait here is on an event: the Nexus's
//! READY=1, an answer, or the child's exit.
use datom_codec::Datomizable;
use flow_nexus::CarriesSignalFrames;
use flow_nexus::place::{LaysOutPlace, Place};
use flow_nexus::store::ConfiguresOnlyStore;
use protos::{Compactable, Protosizable};
use std::{
    io::Read,
    os::unix::net::{UnixDatagram, UnixStream},
    path::{Path, PathBuf},
    process::{Child, Command, ExitStatus, Stdio},
    sync::mpsc,
    time::Duration,
};

/// How long a test waits for any one event before it fails.
const EVENT_WAIT: Duration = Duration::from_secs(120);

struct StartedNexus {
    child: Option<Child>,
    _notify: tempfile::TempDir,
}

impl Drop for StartedNexus {
    fn drop(&mut self) {
        if let Some(child) = self.child.as_mut() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

impl StartedNexus {
    fn child(&mut self) -> &mut Child {
        self.child.as_mut().expect("the child is held")
    }

    /// The child's exit, waited on by a thread that hands it over.
    fn exit(mut self) -> ExitStatus {
        let mut child = self.child.take().expect("the child is held");
        let (exited, exit) = mpsc::channel();
        std::thread::spawn(move || {
            let _ = exited.send(child.wait());
        });
        exit.recv_timeout(EVENT_WAIT)
            .expect("flow-nexus exits")
            .expect("child status")
    }

    /// Stops the Nexus as systemd does, and waits for its exit.
    fn terminate(mut self) -> ExitStatus {
        // SAFETY: kill only sends a signal to the child this test started.
        let sent = unsafe { libc::kill(self.child().id() as libc::pid_t, libc::SIGTERM) };
        assert_eq!(sent, 0, "SIGTERM sent");
        self.exit()
    }
}

/// The place `main` with its Runtime under `runtime` as the deployment
/// lays it out, `<runtime>/nexus/main`, and its State `state`.
fn place(runtime: &Path, state: &Path) -> Place {
    Place {
        place_name: "main".into(),
        runtime: runtime.join("nexus/main").to_string_lossy().into_owned(),
        state: state.to_string_lossy().into_owned(),
    }
}

/// The literal argv the shell writes, independent of Place's serializer.
/// These fixture paths are temporary ASCII paths without string delimiters.
fn place_argument(place: &Place) -> String {
    format!(
        "{{ «{}» «{}» «{}» }}",
        place.place_name, place.runtime, place.state
    )
}

/// Starts the Nexus in its place with `environment` added to an otherwise
/// empty environment and the notify socket systemd would give it.
fn start(home: &Path, place: &Place, environment: &[(&str, &str)]) -> (StartedNexus, UnixDatagram) {
    let notify_directory = tempfile::tempdir().expect("notify directory");
    let notify_path = notify_directory.path().join("notify.sock");
    let notify = UnixDatagram::bind(&notify_path).expect("the notify socket binds");
    notify
        .set_read_timeout(Some(EVENT_WAIT))
        .expect("the notify socket gives up eventually");
    let mut command = Command::new(env!("CARGO_BIN_EXE_flow-nexus"));
    command
        .env_clear()
        .env("HOME", home)
        .env("NOTIFY_SOCKET", &notify_path)
        .stderr(Stdio::piped());
    for (name, value) in environment {
        command.env(name, value);
    }
    let child = command
        .arg(place_argument(place))
        .spawn()
        .expect("flow-nexus spawns with its place");
    (
        StartedNexus {
            child: Some(child),
            _notify: notify_directory,
        },
        notify,
    )
}

/// Starts the Nexus and returns once it announced READY=1: both sockets
/// bound.
fn start_ready(home: &Path, place: &Place, environment: &[(&str, &str)]) -> StartedNexus {
    let (mut nexus, notify) = start(home, place, environment);
    let mut message = [0_u8; 64];
    match notify.recv(&mut message) {
        Ok(length) => assert_eq!(&message[..length], b"READY=1"),
        Err(error) => {
            let _ = nexus.child().kill();
            let mut stderr = String::new();
            if let Some(mut pipe) = nexus.child().stderr.take() {
                let _ = pipe.read_to_string(&mut stderr);
            }
            panic!("flow-nexus announced no readiness ({error}): {stderr}");
        }
    }
    nexus
}

fn endpoint(name: &str, home: &Path) -> meta_signal_flow::CodexEndpoint {
    meta_signal_flow::CodexEndpoint {
        client_path: format!("/opt/{name}-client"),
        home: home.join(name).to_string_lossy().into_owned(),
        control_socket_path: home
            .join(name)
            .join("control.sock")
            .to_string_lossy()
            .into_owned(),
        model_name_vector: vec![format!("{name}-model")],
    }
}

fn configuration(home: &Path) -> meta_signal_flow::ConfigureNexus {
    meta_signal_flow::ConfigureNexus {
        source_root: home.join("source").to_string_lossy().into_owned(),
        stable_codex: endpoint("stable", home),
        next_codex: endpoint("next", home),
        harness_profiles: vec![meta_signal_flow::HarnessProfile {
            harness_kind: signal_flow::HarnessKind::Codex,
            command_sigil_vector: vec!["/".into()],
            interrupt_keys: vec!["esc".into()],
            submit_keys: Vec::new(),
        }],
        meta_aspects: vec![flow_ethos::Aspect::Psyche, flow_ethos::Aspect::Mind],
        message_nexus_path_option: Some("/run/message.sock".into()),
        message_nexus_binary_option: Some("/opt/message-nexus".into()),
        curriculum_nexus_path: "/fixture/curriculum.sock".into(),
        lease: 60,
        presentation_prompt_option: None,
    }
}

/// A memory in the place's State holding a Nexus declaration, written in
/// this process: this build carries no configuration of its own.
fn configured(place: &Place, home: &Path) {
    flow_nexus::store::ConfigurationOnlyStore::open(&place.store())
        .expect("a new configuration-only store opens")
        .configure_change(meta_signal_flow::ConfigurationChange::Nexus(configuration(
            home,
        )))
        .expect("the Nexus configuration commits");
}

fn write_meta(peer: &mut UnixStream, query: &meta_signal_flow::Query) {
    let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(query).expect("meta query encodes");
    peer.write_frame(&bytes).expect("meta query written");
}

fn read_meta(peer: &mut UnixStream) -> meta_signal_flow::Response {
    rkyv::from_bytes::<meta_signal_flow::Response, rkyv::rancor::Error>(
        &peer.read_frame().expect("meta response"),
    )
    .expect("meta response decodes")
}

fn meta_call(place: &Place, query: &meta_signal_flow::Query) -> meta_signal_flow::Response {
    let mut peer = UnixStream::connect(place.meta_socket()).expect("the meta socket listens");
    write_meta(&mut peer, query);
    read_meta(&mut peer)
}

fn metaflows_listed(place: &Place) -> bool {
    let mut ordinary =
        UnixStream::connect(place.ordinary_socket()).expect("the ordinary socket listens");
    ordinary
        .write_query(&signal_flow::Query::Metaflows)
        .expect("Metaflows written");
    matches!(
        ordinary.read_response().expect("Metaflows answered"),
        signal_flow::Response::Listed(_)
    )
}

#[test]
fn two_places_serve_disjoint_sockets_and_memory() {
    let home_a = tempfile::tempdir().expect("home A");
    let home_b = tempfile::tempdir().expect("home B");
    let runtime_a = tempfile::tempdir().expect("runtime A");
    let runtime_b = tempfile::tempdir().expect("runtime B");
    let state_a = tempfile::tempdir().expect("state A");
    let state_b = tempfile::tempdir().expect("state B");
    let place_a = place(runtime_a.path(), state_a.path());
    let place_b = place(runtime_b.path(), state_b.path());
    configured(&place_a, home_a.path());
    configured(&place_b, home_b.path());

    let _nexus_a = start_ready(home_a.path(), &place_a, &[]);
    let _nexus_b = start_ready(home_b.path(), &place_b, &[]);
    for place in [&place_a, &place_b] {
        assert!(metaflows_listed(place));
    }
    let configuration_of =
        |place: &Place| match meta_call(place, &meta_signal_flow::Query::Configuration) {
            meta_signal_flow::Response::Configuration(configuration) => configuration,
            other => panic!("the place returns its Configuration, not {other:?}"),
        };
    assert_eq!(
        configuration_of(&place_a).nexus.source_root,
        home_a.path().join("source").to_string_lossy()
    );
    assert_eq!(
        configuration_of(&place_b).nexus.source_root,
        home_b.path().join("source").to_string_lossy()
    );
    assert_eq!(place_a.store(), state_a.path().join("flow.sema"));
    assert!(place_a.store().exists());
    assert!(place_b.store().exists());
    assert_eq!(
        place_a.ordinary_socket(),
        runtime_a.path().join("nexus/main/flow.sock")
    );
}

/// Under systemd's Type=notify the Nexus sends READY=1 to $NOTIFY_SOCKET,
/// and only once its sockets are bound: when the message arrives, each
/// socket accepts a connection and the ordinary one answers. The test waits
/// on the message itself, never re-trying a connection.
#[test]
fn the_nexus_notifies_readiness_only_after_its_socket_is_bound() {
    let home = tempfile::tempdir().expect("temporary home");
    let runtime = tempfile::tempdir().expect("temporary runtime directory");
    let state = tempfile::tempdir().expect("dedicated state directory");
    let place = place(runtime.path(), state.path());
    configured(&place, home.path());
    let _nexus = start_ready(home.path(), &place, &[]);
    UnixStream::connect(place.meta_socket()).expect("the meta socket listens at readiness");
    assert!(metaflows_listed(&place));
}

/// A Configure applied to a running Nexus takes effect without a restart,
/// and a subscription on the meta socket sees it: the configuration on
/// open, then the replaced value once it is committed.
#[test]
fn a_configure_applied_to_a_running_nexus_takes_effect_without_a_restart() {
    let home = tempfile::tempdir().expect("temporary home");
    let runtime = tempfile::tempdir().expect("temporary runtime directory");
    let state = tempfile::tempdir().expect("dedicated state directory");
    let place = place(runtime.path(), state.path());
    configured(&place, home.path());
    let nexus = start_ready(home.path(), &place, &[]);

    let mut subscription =
        UnixStream::connect(place.meta_socket()).expect("the meta socket listens");
    subscription
        .set_read_timeout(Some(EVENT_WAIT))
        .expect("the subscription gives up eventually");
    write_meta(
        &mut subscription,
        &meta_signal_flow::Query::ObserveConfiguration,
    );
    let meta_signal_flow::Response::Configuration(first) = read_meta(&mut subscription) else {
        panic!("the subscription opens with the configuration")
    };
    assert_eq!(first.nexus, configuration(home.path()));

    let mut replaced = configuration(home.path());
    replaced.source_root = home.path().join("elsewhere").to_string_lossy().into_owned();
    replaced.stable_codex.model_name_vector = vec!["stable-model-2".into()];
    assert_eq!(
        meta_call(
            &place,
            &meta_signal_flow::Query::Configure(meta_signal_flow::ConfigurationChange::Nexus(
                replaced.clone()
            ))
        ),
        meta_signal_flow::Response::Configured(meta_signal_flow::ConfigureOutcome::Replaced)
    );
    let meta_signal_flow::Response::Configuration(second) = read_meta(&mut subscription) else {
        panic!("the subscription sees the change")
    };
    assert_eq!(second.nexus, replaced);
    // The same Nexus, never restarted, answers with the replaced value.
    let meta_signal_flow::Response::Configuration(now) =
        meta_call(&place, &meta_signal_flow::Query::Configuration)
    else {
        panic!("the running Nexus answers its Configuration")
    };
    assert_eq!(now.nexus, replaced);
    drop(subscription);
    assert!(nexus.terminate().success());
}

/// Nothing in the environment configures the Nexus: FLOW_ variables, the
/// earlier FLOW_HOOK among them, change neither its memory nor its hook.
#[test]
fn environment_overrides_do_not_replace_the_explicit_configuration() {
    let home = tempfile::tempdir().expect("temporary home");
    let runtime = tempfile::tempdir().expect("temporary runtime directory");
    let state = tempfile::tempdir().expect("dedicated state directory");
    let place = place(runtime.path(), state.path());
    configured(&place, home.path());
    let nexus = start_ready(
        home.path(),
        &place,
        &[
            ("FLOW_SOURCE_ROOT", "/srv/elsewhere"),
            ("FLOW_CODEX_STABLE_CLIENT", "/opt/elsewhere-client"),
            ("FLOW_HOOK", "/opt/elsewhere/flow-hook"),
            ("XDG_RUNTIME_DIR", "/run/elsewhere"),
        ],
    );
    let meta_signal_flow::Response::Configuration(snapshot) =
        meta_call(&place, &meta_signal_flow::Query::Configuration)
    else {
        panic!("the running Nexus answers its Configuration")
    };
    assert_eq!(snapshot.nexus, configuration(home.path()));
    assert_eq!(
        snapshot.harness_hook,
        meta_signal_flow::HarnessHookConfiguration::Unconfigured
    );
    assert!(metaflows_listed(&place));
    assert!(nexus.terminate().success());
    assert!(
        !home.path().join(".local/state/flow/flow.sema").exists(),
        "FLOW_ variables and defaults do not redirect the place's memory"
    );
}

fn recovery_flag(store: &Path) -> u8 {
    std::fs::read(store)
        .expect("store bytes read")
        .get(9)
        .map_or(0, |god| god & 2)
}

fn plant_stale_socket(socket: &Path) {
    std::fs::create_dir_all(socket.parent().expect("socket directory")).expect("directory");
    drop(std::os::unix::net::UnixListener::bind(socket).expect("socket inode"));
    // A child forked by a concurrent test holds the listener until it execs.
    for _ in 0..500 {
        if UnixStream::connect(socket).is_err() {
            return;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    panic!("{} never became stale", socket.display())
}

#[test]
fn sigterm_stops_the_daemon_and_closes_its_store_cleanly() {
    let home = tempfile::tempdir().expect("temporary home");
    let runtime = tempfile::tempdir().expect("temporary runtime directory");
    let state = tempfile::tempdir().expect("dedicated state directory");
    let place = place(runtime.path(), state.path());
    let store = place.store();
    configured(&place, home.path());
    plant_stale_socket(&place.ordinary_socket());
    plant_stale_socket(&place.meta_socket());

    let nexus = start_ready(home.path(), &place, &[]);
    assert!(metaflows_listed(&place), "answered past the stale socket");
    assert_eq!(recovery_flag(&store), 2, "a running writer holds the flag");

    assert!(nexus.terminate().success());
    assert_eq!(recovery_flag(&store), 0, "the store closed cleanly");
    redb::ReadOnlyDatabase::open(&store).expect("read-only recognition opens");
    flow_nexus::store::ConfigurationOnlyStore::open(&store).expect("the store is recognized");
    assert!(!place.ordinary_socket().exists());
    assert!(!place.meta_socket().exists());
}

#[test]
fn a_live_nexus_socket_is_never_taken_over() {
    let home = tempfile::tempdir().expect("temporary home");
    let runtime = tempfile::tempdir().expect("temporary runtime directory");
    let state = tempfile::tempdir().expect("dedicated state directory");
    let other_state = tempfile::tempdir().expect("second state directory");
    let place = place(runtime.path(), state.path());
    configured(&place, home.path());
    let _nexus = start_ready(home.path(), &place, &[]);

    // A second Nexus at the same Runtime, with memory of its own.
    let intruding = Place {
        state: other_state.path().to_string_lossy().into_owned(),
        ..place.clone()
    };
    configured(&intruding, home.path());
    let (intruder, _notify) = start(home.path(), &intruding, &[]);
    assert!(
        !intruder.exit().success(),
        "a live socket is refused, and the intruder exits"
    );
    assert!(metaflows_listed(&place), "the live Nexus still answers");
}

/// A place whose directories are absent is made: the Runtime and the State
/// are created, private to their owner.
#[test]
fn a_nexus_makes_its_place_directories() {
    use std::os::unix::fs::PermissionsExt;
    let home = tempfile::tempdir().expect("temporary home");
    let runtime = tempfile::tempdir().expect("temporary runtime directory");
    let state_root = tempfile::tempdir().expect("state root");
    let state: PathBuf = state_root.path().join("nexus/main");
    let place = place(runtime.path(), &state);
    std::fs::create_dir_all(&state).expect("state directory for the fixture memory");
    configured(&place, home.path());
    std::fs::remove_dir_all(runtime.path().join("nexus")).ok();
    let nexus = start_ready(home.path(), &place, &[]);
    let mode = std::fs::metadata(place.runtime_directory())
        .expect("runtime directory made")
        .permissions()
        .mode();
    assert_eq!(mode & 0o777, 0o700);
    assert!(nexus.terminate().success());
}

#[cfg(feature = "executable-default-configuration")]
#[test]
fn configured_executable_seeds_only_its_fresh_typed_nexus_row() {
    let home = tempfile::tempdir().expect("temporary home");
    let runtime = tempfile::tempdir().expect("temporary runtime directory");
    let state = tempfile::tempdir().expect("dedicated state directory");
    let place = place(runtime.path(), state.path());

    // A fresh memory: the Nexus starts with the configuration built into it,
    // and binds and announces readiness before anything is configured.
    let nexus = start_ready(home.path(), &place, &[]);
    assert!(metaflows_listed(&place));
    assert!(nexus.terminate().success());

    let store = flow_nexus::store::ConfigurationOnlyStore::open(&place.store())
        .expect("configured store reopens");
    let snapshot = store
        .configuration_snapshot()
        .expect("snapshot reads")
        .expect("fresh executable seeded Nexus");
    assert_eq!(snapshot.nexus.lease, 60);
    assert!(snapshot.nexus.message_nexus_path_option.is_none());
    assert!(snapshot.nexus.message_nexus_binary_option.is_none());
}

#[test]
fn the_place_datom_round_trips_as_the_shell_writes_it() {
    let place = place(
        Path::new("/run/user/1001"),
        Path::new("/home/li/.local/state/nexus/main"),
    );
    assert_eq!(
        place.datomize(vec![]).protosize().compact(),
        "{ main /run/user/1001/nexus/main /home/li/.local/state/nexus/main }"
    );
}
