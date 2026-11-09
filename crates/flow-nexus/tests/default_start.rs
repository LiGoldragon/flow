//! The service is started with one datom, its place: its sockets in Runtime,
//! its memory in State. Configuration reaches the running Nexus through its
//! meta socket and takes effect at once; nothing is read from the
//! environment for either. Every wait here is on an event: the Nexus's
//! READY=1, an answer, or the child's exit, each bounded by EVENT_WAIT.
use datom_codec::Datomizable;
use flow_nexus::CarriesSignalFrames;
use flow_nexus::place::{LaysOutPlace, Place};
use flow_nexus::store::ConfiguresOnlyStore;
use protos::{Compactable, Protosizable};
use std::{
    io::Read,
    os::unix::net::{UnixDatagram, UnixStream},
    path::Path,
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

    /// The child's exit, waited on by a thread that hands it over. A child
    /// that has not exited within EVENT_WAIT is killed and fails the test.
    fn exit(mut self) -> ExitStatus {
        let mut child = self.child.take().expect("the child is held");
        let pid = child.id() as libc::pid_t;
        let (exited, exit) = mpsc::channel();
        std::thread::spawn(move || {
            let _ = exited.send(child.wait());
        });
        match exit.recv_timeout(EVENT_WAIT) {
            Ok(status) => status.expect("child status"),
            Err(error) => {
                // SAFETY: kill only sends a signal to the child this test
                // started, which has not been reaped.
                unsafe { libc::kill(pid, libc::SIGKILL) };
                panic!("flow-nexus did not exit within {EVENT_WAIT:?}: {error}")
            }
        }
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

/// The place `main`, its two directories made as the environment declares
/// them: the Nexus finds them and makes neither.
fn declared(runtime: &Path, state: &Path) -> Place {
    let place = place(runtime, state);
    for directory in [place.runtime_directory(), place.state_directory()] {
        std::fs::create_dir_all(directory).expect("the declared place directory");
    }
    place
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
    let place_a = declared(runtime_a.path(), state_a.path());
    let place_b = declared(runtime_b.path(), state_b.path());
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
    let place = declared(runtime.path(), state.path());
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
    let place = declared(runtime.path(), state.path());
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
    let place = declared(runtime.path(), state.path());
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

/// A stale socket at `socket`: one that never listened, so it is refused at
/// once, whatever child of a concurrent test still holds its descriptor.
fn plant_stale_socket(socket: &Path) {
    std::fs::create_dir_all(socket.parent().expect("socket directory")).expect("directory");
    plant_unlistened_socket(socket);
    assert!(
        UnixStream::connect(socket).is_err(),
        "{} is stale",
        socket.display()
    );
}

#[test]
fn sigterm_stops_the_daemon_and_closes_its_store_cleanly() {
    let home = tempfile::tempdir().expect("temporary home");
    let runtime = tempfile::tempdir().expect("temporary runtime directory");
    let state = tempfile::tempdir().expect("dedicated state directory");
    let place = declared(runtime.path(), state.path());
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
    let place = declared(runtime.path(), state.path());
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

/// A place whose Runtime or State is absent is refused: the Nexus exits
/// without serving and makes neither directory. The wait for the exit is
/// bounded by EVENT_WAIT; a Nexus that made its place and served instead
/// fails the test there.
#[test]
fn a_nexus_whose_place_is_absent_is_refused_and_makes_nothing() {
    let home = tempfile::tempdir().expect("temporary home");
    let runtime = tempfile::tempdir().expect("temporary runtime root");
    let state = tempfile::tempdir().expect("dedicated state directory");
    let place = place(runtime.path(), state.path());
    configured(&place, home.path());
    let (nexus, _notify) = start(home.path(), &place, &[]);
    assert!(!nexus.exit().success(), "an absent Runtime is refused");
    assert!(!runtime.path().join("nexus").exists(), "no Runtime is made");

    let state_root = tempfile::tempdir().expect("state root");
    let absent_state = state_root.path().join("nexus/main");
    let place = declared(runtime.path(), state.path());
    let place = Place {
        state: absent_state.to_string_lossy().into_owned(),
        ..place
    };
    let (nexus, _notify) = start(home.path(), &place, &[]);
    assert!(!nexus.exit().success(), "an absent State is refused");
    assert!(
        !state_root.path().join("nexus").exists(),
        "no State is made"
    );
    assert!(
        std::fs::read_dir(place.runtime_directory())
            .expect("the Runtime stands")
            .next()
            .is_none(),
        "the refused Nexus bound nothing in the Runtime"
    );
}

/// The place is shared with every other Nexus at it: a stopped Nexus
/// removes its own two sockets and nothing else, and both directories stay.
#[test]
fn a_stopped_nexus_removes_only_its_own_sockets() {
    let home = tempfile::tempdir().expect("temporary home");
    let runtime = tempfile::tempdir().expect("temporary runtime directory");
    let state = tempfile::tempdir().expect("dedicated state directory");
    let place = declared(runtime.path(), state.path());
    configured(&place, home.path());
    let others = [
        place.runtime_directory().join("energy.sock"),
        place.runtime_directory().join("energy-translator.sock"),
        place.state_directory().join("energy.rkyv"),
    ];
    for other in &others {
        std::fs::write(other, "another Nexus's").expect("another's file");
    }
    let nexus = start_ready(home.path(), &place, &[]);
    assert!(place.ordinary_socket().exists() && place.meta_socket().exists());
    assert!(nexus.terminate().success());
    assert!(!place.ordinary_socket().exists());
    assert!(!place.meta_socket().exists());
    for other in &others {
        assert_eq!(
            std::fs::read(other).expect("another's file stays"),
            b"another Nexus's"
        );
    }
    assert!(place.runtime_directory().is_dir() && place.state_directory().is_dir());
}

/// A socket bound at `path` that never listened, then closed, as a Nexus
/// that died leaves one: a connection to it is refused, whoever else holds
/// the descriptor.
fn plant_unlistened_socket(path: &Path) {
    use std::os::unix::ffi::OsStrExt;
    // SAFETY: the address is a valid sockaddr_un holding the path within its
    // bounds; the descriptor is closed before return.
    unsafe {
        let descriptor = libc::socket(libc::AF_UNIX, libc::SOCK_STREAM | libc::SOCK_CLOEXEC, 0);
        assert!(descriptor >= 0, "a socket opens");
        let mut address: libc::sockaddr_un = std::mem::zeroed();
        address.sun_family = libc::AF_UNIX as libc::sa_family_t;
        assert!(path.as_os_str().len() < address.sun_path.len());
        for (slot, byte) in address.sun_path.iter_mut().zip(path.as_os_str().as_bytes()) {
            *slot = *byte as libc::c_char;
        }
        let bound = libc::bind(
            descriptor,
            (&raw const address).cast::<libc::sockaddr>(),
            std::mem::size_of::<libc::sockaddr_un>() as libc::socklen_t,
        );
        assert_eq!(bound, 0, "the socket binds at {}", path.display());
        libc::close(descriptor);
    }
}

/// One inotify event on the place's Runtime, as a watching peer reads it.
#[derive(Debug)]
struct RuntimeEvent {
    mask: u32,
    name: String,
    /// For a name moved in: a connection made the moment the event was read.
    connected: Option<bool>,
}

/// Watches `directory` as a peer of the Nexus does, on a thread blocked in
/// read(2): every event is handed over, and on each name moved in a
/// connection is made at once.
fn watch_runtime(directory: &Path) -> mpsc::Receiver<RuntimeEvent> {
    use std::os::unix::ffi::OsStrExt;
    let path = std::ffi::CString::new(directory.as_os_str().as_bytes()).expect("path");
    // SAFETY: inotify_init1 takes no pointer; add_watch reads a
    // zero-terminated path that outlives the call.
    let descriptor = unsafe { libc::inotify_init1(libc::IN_CLOEXEC) };
    assert!(descriptor >= 0, "inotify opens");
    let mask = libc::IN_CREATE
        | libc::IN_MOVED_TO
        | libc::IN_MOVED_FROM
        | libc::IN_ATTRIB
        | libc::IN_DELETE;
    // SAFETY: see above.
    assert!(unsafe { libc::inotify_add_watch(descriptor, path.as_ptr(), mask) } >= 0);
    let directory = directory.to_owned();
    let (sent, events) = mpsc::channel();
    std::thread::spawn(move || {
        let mut buffer = [0_u8; 4096];
        loop {
            // SAFETY: read writes at most the buffer's length into it.
            let length =
                unsafe { libc::read(descriptor, buffer.as_mut_ptr().cast(), buffer.len()) };
            if length <= 0 {
                return;
            }
            let mut at = 0;
            while at < length as usize {
                // SAFETY: the kernel writes whole events; the header is read
                // unaligned from within the bytes read.
                let header = unsafe {
                    std::ptr::read_unaligned(buffer[at..].as_ptr().cast::<libc::inotify_event>())
                };
                let start = at + std::mem::size_of::<libc::inotify_event>();
                let name_bytes = &buffer[start..start + header.len as usize];
                let name = String::from_utf8_lossy(name_bytes)
                    .trim_end_matches('\0')
                    .to_owned();
                at = start + header.len as usize;
                let connected = (header.mask & libc::IN_MOVED_TO != 0)
                    .then(|| UnixStream::connect(directory.join(&name)).is_ok());
                if sent
                    .send(RuntimeEvent {
                        mask: header.mask,
                        name,
                        connected,
                    })
                    .is_err()
                {
                    return;
                }
            }
        }
    });
    events
}

/// A socket appears at its name only listening: each name is moved in,
/// never made, changed or removed in place, and a peer that sees it moved in
/// connects at once. A stale socket at a name from a crash is replaced by
/// the move, with no removal before it, and a leftover temporary name of
/// another process is neither served nor touched.
#[test]
fn each_socket_appears_at_its_name_only_listening() {
    let home = tempfile::tempdir().expect("temporary home");
    let runtime = tempfile::tempdir().expect("temporary runtime directory");
    let state = tempfile::tempdir().expect("dedicated state directory");
    let place = declared(runtime.path(), state.path());
    configured(&place, home.path());
    let names = ["flow.sock", "flow-meta.sock"];
    plant_unlistened_socket(&place.ordinary_socket());
    let leftovers = [".flow.sock.1", ".flow-meta.sock.1"]
        .map(|leftover| place.runtime_directory().join(leftover));
    for leftover in &leftovers {
        plant_unlistened_socket(leftover);
    }
    let events = watch_runtime(place.runtime_directory());
    let nexus = start_ready(home.path(), &place, &[]);

    // Every event up to both names moved in, in the order they came.
    let mut moved_in = Vec::new();
    while moved_in.len() < names.len() {
        let event = events
            .recv_timeout(EVENT_WAIT)
            .expect("the Nexus places its sockets");
        if !names.contains(&event.name.as_str()) {
            assert!(
                event.name.starts_with('.')
                    && !leftovers.iter().any(|left| left.ends_with(&event.name)),
                "only the Nexus's own temporary names change beside its sockets: {event:?}"
            );
            continue;
        }
        assert_eq!(
            event.mask & !libc::IN_ISDIR,
            libc::IN_MOVED_TO,
            "a name is only ever moved in: {event:?}"
        );
        assert_eq!(
            event.connected,
            Some(true),
            "listening when moved in: {event:?}"
        );
        moved_in.push(event.name);
    }
    moved_in.sort();
    assert_eq!(moved_in, ["flow-meta.sock", "flow.sock"]);
    assert!(metaflows_listed(&place));
    for leftover in &leftovers {
        assert!(leftover.exists(), "another's leftover stays");
        assert!(
            UnixStream::connect(leftover).is_err(),
            "a leftover is never served"
        );
    }
    let mut runtime_names = std::fs::read_dir(place.runtime_directory())
        .expect("the Runtime reads")
        .map(|entry| {
            entry
                .expect("an entry")
                .file_name()
                .to_string_lossy()
                .into_owned()
        })
        .collect::<Vec<_>>();
    runtime_names.sort();
    assert_eq!(
        runtime_names,
        [
            ".flow-meta.sock.1",
            ".flow.sock.1",
            "flow-meta.sock",
            "flow.sock"
        ],
        "no temporary name of the Nexus's own is left"
    );
    assert!(nexus.terminate().success());
    assert!(!place.ordinary_socket().exists() && !place.meta_socket().exists());
    assert!(leftovers.iter().all(|leftover| leftover.exists()));
}

#[cfg(feature = "executable-default-configuration")]
#[test]
fn configured_executable_seeds_only_its_fresh_typed_nexus_row() {
    let home = tempfile::tempdir().expect("temporary home");
    let runtime = tempfile::tempdir().expect("temporary runtime directory");
    let state = tempfile::tempdir().expect("dedicated state directory");
    let place = declared(runtime.path(), state.path());

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
