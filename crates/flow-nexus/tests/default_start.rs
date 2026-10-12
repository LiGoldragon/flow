//! The service takes an explicit typed Start and configuration reaches it
//! through the one-request meta listener before normal startup.
use datom_codec::Datomizable;
use flow_nexus::{
    CarriesSignalFrames,
    generated::startup::Start,
    store::OpensConfigurationOnlyStore,
};
use protos::{Compactable, Protosizable};
use std::{
    io::{Read, Write},
    os::unix::net::UnixStream,
    path::Path,
    process::{Child, Command, Stdio},
    time::Duration,
};

struct StartedNexus {
    child: Child,
}

impl Drop for StartedNexus {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn start(runtime: &Path, state: &Path) -> Start {
    Start {
        ordinary_socket_path: runtime
            .join("flow/flow.sock")
            .to_string_lossy()
            .into_owned(),
        meta_socket_path: runtime
            .join("flow/flow-meta.sock")
            .to_string_lossy()
            .into_owned(),
        store_path: state.join("flow.sema").to_string_lossy().into_owned(),
    }
}

fn spawn(home: &Path, runtime: &Path, start: &Start, configure_only: bool) -> StartedNexus {
    let mut command = Command::new(env!("CARGO_BIN_EXE_flow-nexus"));
    command
        .env_clear()
        .env("HOME", home)
        .env("XDG_RUNTIME_DIR", runtime)
        .stderr(Stdio::piped());
    if configure_only {
        command.arg("--configure-only");
    }
    command
        .arg(start.datomize(vec![]).protosize().compact())
        .spawn()
        .map(|child| StartedNexus { child })
        .expect("flow-nexus spawns with typed Start")
}

fn connect(socket: &Path, nexus: &mut StartedNexus) -> UnixStream {
    for _ in 0..500 {
        if let Ok(peer) = UnixStream::connect(socket) {
            return peer;
        }
        if let Some(status) = nexus.child.try_wait().expect("child status") {
            let mut stderr = String::new();
            if let Some(mut pipe) = nexus.child.stderr.take() {
                let _ = pipe.read_to_string(&mut stderr);
            }
            panic!("flow-nexus exited with {status}: {stderr}");
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    panic!("{} never listened", socket.display())
}

fn wait_successfully(nexus: &mut StartedNexus) {
    let status = nexus
        .child
        .wait()
        .expect("flow-nexus exits after Configure");
    if !status.success() {
        let mut stderr = String::new();
        if let Some(mut pipe) = nexus.child.stderr.take() {
            let _ = pipe.read_to_string(&mut stderr);
        }
        panic!("flow-nexus exited with {status}: {stderr}");
    }
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

fn configuration(start: &Start, home: &Path) -> meta_signal_flow::Configuration {
    meta_signal_flow::Configuration {
        ordinary_socket_path: start.ordinary_socket_path.clone(),
        meta_socket_path: start.meta_socket_path.clone(),
        source_root: home.join("source").to_string_lossy().into_owned(),
        stable_codex: endpoint("stable", home),
        next_codex: endpoint("next", home),
        harness_profile_vector: vec![meta_signal_flow::HarnessProfile {
            harness_kind: signal_flow::HarnessKind::Codex,
            command_sigil_vector: vec!["/".into()],
            interrupt_keys: vec!["esc".into()],
            submit_keys: Vec::new(),
        }],
        meta_aspects: vec![
            signal_flow::FlowAspect::Psyche,
            signal_flow::FlowAspect::Mind,
        ],
        message_nexus_path: "/opt/message-nexus".into(),
    }
}

fn configure_fresh(home: &Path, runtime: &Path, start: &Start) {
    let mut nexus = spawn(home, runtime, start, true);
    let mut meta = connect(Path::new(&start.meta_socket_path), &mut nexus);
    let desired = configuration(start, home);
    let query = meta_signal_flow::Query::Configure(desired.clone());
    let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&query).expect("Configure encodes");
    meta.write_all(&(bytes.len() as u32).to_be_bytes())
        .and_then(|_| meta.write_all(&bytes))
        .expect("Configure written");
    let mut length = [0; 4];
    meta.read_exact(&mut length).expect("Configure length");
    let mut reply = vec![0; u32::from_be_bytes(length) as usize];
    meta.read_exact(&mut reply).expect("Configure reply");
    let reply = rkyv::from_bytes::<meta_signal_flow::Response, rkyv::rancor::Error>(&reply)
        .expect("Configure reply decodes");
    assert!(matches!(
        reply,
        meta_signal_flow::Response::Configured(configured)
            if configured.configuration == desired
                && configured.activation == meta_signal_flow::Activation::NexusRestartRequired
    ));
    drop(meta);
    wait_successfully(&mut nexus);
}

#[test]
fn configure_only_commits_before_normal_start_uses_the_declared_store() {
    let home = tempfile::tempdir().expect("temporary home");
    let runtime = tempfile::tempdir().expect("temporary runtime directory");
    let state = tempfile::tempdir().expect("dedicated state directory");
    let start = start(runtime.path(), state.path());
    configure_fresh(home.path(), runtime.path(), &start);

    assert!(state.path().join("flow.sema").exists());
    assert!(
        !home.path().join(".local/state/flow/flow.sema").exists(),
        "typed Start does not open the HOME-derived store"
    );

    let mut nexus = spawn(home.path(), runtime.path(), &start, false);
    let mut ordinary = connect(Path::new(&start.ordinary_socket_path), &mut nexus);
    ordinary
        .write_query(&signal_flow::Query::List(signal_flow::ListRequest {}))
        .expect("List written");
    assert!(matches!(
        ordinary.read_response().expect("List answered"),
        signal_flow::Response::Listed(rows) if rows.is_empty()
    ));
}

#[test]
fn environment_overrides_do_not_replace_the_explicit_configuration() {
    use flow_nexus::store::{ConfigurationOnlyStore, OpensFlowStore};

    let home = tempfile::tempdir().expect("temporary home");
    let runtime = tempfile::tempdir().expect("temporary runtime directory");
    let state = tempfile::tempdir().expect("dedicated state directory");
    let start = start(runtime.path(), state.path());
    let mut nexus = Command::new(env!("CARGO_BIN_EXE_flow-nexus"))
        .env_clear()
        .env("HOME", home.path())
        .env("XDG_RUNTIME_DIR", runtime.path())
        .env("FLOW_SOURCE_ROOT", "/srv/elsewhere")
        .env("FLOW_CODEX_STABLE_CLIENT", "/opt/elsewhere-client")
        .arg("--configure-only")
        .arg(start.datomize(vec![]).protosize().compact())
        .stderr(Stdio::piped())
        .spawn()
        .map(|child| StartedNexus { child })
        .expect("flow-nexus spawns");
    let mut meta = connect(Path::new(&start.meta_socket_path), &mut nexus);
    let desired = configuration(&start, home.path());
    let query = meta_signal_flow::Query::Configure(desired.clone());
    let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&query).expect("Configure encodes");
    meta.write_all(&(bytes.len() as u32).to_be_bytes())
        .and_then(|_| meta.write_all(&bytes))
        .expect("Configure written");
    let mut length = [0; 4];
    meta.read_exact(&mut length).expect("Configure length");
    let mut reply = vec![0; u32::from_be_bytes(length) as usize];
    meta.read_exact(&mut reply).expect("Configure reply");
    let response = rkyv::from_bytes::<meta_signal_flow::Response, rkyv::rancor::Error>(&reply)
        .expect("Configure reply decodes");
    assert!(matches!(
        response,
        meta_signal_flow::Response::Configured(configured)
            if configured.configuration == desired
    ));
    drop(meta);
    wait_successfully(&mut nexus);

    let stored = ConfigurationOnlyStore::open(Path::new(&start.store_path))
        .expect("typed store reopens")
        .configured_configuration()
        .expect("configuration reads")
        .expect("whole configuration was committed");
    assert_eq!(stored, desired);
    assert_eq!(
        stored.source_root,
        home.path().join("source").to_string_lossy().into_owned()
    );
    assert_eq!(stored.stable_codex.client_path, "/opt/stable-client");
    assert!(
        !home.path().join(".local/state/flow/flow.sema").exists(),
        "FLOW_ variables and defaults do not redirect typed StorePath"
    );
}
