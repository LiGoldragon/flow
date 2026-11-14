//! The CLI is `flow-nexus-meta '<Input>'`: it takes the datom and nothing
//! else. The place is the Flow configuration's,
//! `$XDG_CONFIG_HOME/nexus/flow-nexus.datom`; the place itself is read from
//! that place's place file, `$XDG_CONFIG_HOME/nexus/<place>/flow-nexus.signal`,
//! and the CLI reaches the meta socket in the Runtime that file holds and no
//! other, whatever else the environment names, `XDG_RUNTIME_DIR` included.
//! With no place file it is refused, naming the file.
use std::{
    io::{Read, Write},
    os::unix::net::UnixListener,
    path::Path,
    process::{Command, Output},
};

/// The place `place_name`, its Runtime `runtime`, written to its place file
/// under the configuration home: where the Nexus's own place file lies.
fn write_place_file(configuration_home: &Path, place_name: &str, runtime: &Path) {
    use translator::place_file::{
        ConfigurationHome, LocatesConfigurationHome, LocatesPlaceFile, WritesPlaceFile,
    };
    use translator::placement::{NexusName, Place, PlaceName, Runtime, State};
    ConfigurationHome::under(Some(configuration_home.as_os_str()), None)
        .expect("the configuration home")
        .place_file(
            &NexusName("flow-nexus".into()),
            &PlaceName(place_name.into()),
        )
        .expect("the place file")
        .write_place(&Place {
            place_name: PlaceName(place_name.into()),
            runtime: Runtime(runtime.to_string_lossy().into_owned()),
            state: State(runtime.join("state").to_string_lossy().into_owned()),
        })
        .expect("the place file is written");
}

fn client(runtime: &Path, configuration_home: &Path, other: &Path, arguments: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_flow-nexus-meta"))
        .env_clear()
        .env("XDG_RUNTIME_DIR", runtime)
        .env("XDG_CONFIG_HOME", configuration_home)
        .env("FLOW_META_SOCKET", other)
        .args(arguments)
        .output()
        .expect("client runs")
}

#[test]
fn the_client_reaches_only_the_meta_socket_of_the_place_its_configuration_names() {
    let runtime = tempfile::tempdir().expect("temporary runtime directory");
    let configuration = tempfile::tempdir().expect("temporary configuration home");
    std::fs::create_dir_all(configuration.path().join("nexus")).expect("nexus configuration");
    std::fs::write(
        configuration.path().join("nexus/flow-nexus.datom"),
        "{ main }\n",
    )
    .expect("the configuration names main");
    // The Runtime of `main` is not the default under XDG_RUNTIME_DIR: its
    // place file holds it, and the default is where a decoy listens.
    let main = runtime.path().join("elsewhere/main");
    let default_main = runtime.path().join("nexus/main");
    std::fs::create_dir_all(&main).expect("the place's runtime directory");
    std::fs::create_dir_all(&default_main).expect("the default runtime directory");
    write_place_file(configuration.path(), "main", &main);
    let selected = main.join("flow-meta.sock");
    let other = runtime.path().join("other.sock");
    let listener = UnixListener::bind(&selected).expect("selected fixture binds");
    let default_listener =
        UnixListener::bind(default_main.join("flow-meta.sock")).expect("default fixture binds");
    let other_listener = UnixListener::bind(&other).expect("other fixture binds");
    for unreached in [&default_listener, &other_listener] {
        unreached
            .set_nonblocking(true)
            .expect("nonblocking fixture");
    }
    let answering = std::thread::spawn(move || {
        let (mut peer, _) = listener.accept().expect("the selected client connects");
        let mut length = [0; 4];
        use signal::Contracted;
        peer.read_exact(&mut length).expect("greeting length");
        let mut greeting = vec![0; u32::from_be_bytes(length) as usize];
        peer.read_exact(&mut greeting).expect("greeting");
        let greeting = rkyv::from_bytes::<signal::Handshake, rkyv::rancor::Error>(&greeting)
            .expect("the client sends a Signal greeting first");
        let receipt = meta_signal_flow::Input::receipt(&greeting);
        assert!(matches!(receipt, signal::HandshakeReceipt::Greeted(_)));
        let receipt = rkyv::to_bytes::<rkyv::rancor::Error>(&receipt).expect("receipt encodes");
        peer.write_all(&(receipt.len() as u32).to_be_bytes())
            .and_then(|_| peer.write_all(&receipt))
            .expect("receipt written");
        peer.read_exact(&mut length).expect("query length");
        let mut query = vec![0; u32::from_be_bytes(length) as usize];
        peer.read_exact(&mut query).expect("query");
        let reply = rkyv::to_bytes::<rkyv::rancor::Error>(&meta_signal_flow::Output::FlowRetired(
            retired(),
        ))
        .expect("reply encodes");
        peer.write_all(&(reply.len() as u32).to_be_bytes())
            .and_then(|_| peer.write_all(&reply))
            .expect("reply written");
    });
    let output = client(
        runtime.path(),
        configuration.path(),
        &other,
        &["Retire.fac697"],
    );
    assert!(output.status.success(), "{output:?}");
    answering.join().expect("selected fixture answered");
    assert!(
        String::from_utf8_lossy(&output.stdout).starts_with("FlowRetired."),
        "{output:?}"
    );

    // A typed place is a second argument, and refused.
    let refused = client(
        runtime.path(),
        configuration.path(),
        &other,
        &["main", "Retire.fac697"],
    );
    assert_eq!(refused.status.code(), Some(2), "{refused:?}");
    assert!(
        String::from_utf8_lossy(&refused.stderr).starts_with("usage: flow-nexus-meta '"),
        "{refused:?}"
    );

    // A configuration that is missing is refused, naming its path.
    let empty = tempfile::tempdir().expect("an empty configuration home");
    let refused = client(runtime.path(), empty.path(), &other, &["Retire.fac697"]);
    assert_eq!(refused.status.code(), Some(2), "{refused:?}");
    let stderr = String::from_utf8_lossy(&refused.stderr);
    assert!(
        stderr.starts_with("Missing.")
            && stderr.contains(
                &*empty
                    .path()
                    .join("nexus/flow-nexus.datom")
                    .to_string_lossy()
            ),
        "{stderr}"
    );
    for unreached in [&default_listener, &other_listener] {
        assert_eq!(
            unreached
                .accept()
                .expect_err("no fallback connection")
                .kind(),
            std::io::ErrorKind::WouldBlock
        );
    }

    // A place file that is missing is refused, naming it, with the default
    // Runtime listening and no XDG_RUNTIME_DIR needed.
    let unplaced = tempfile::tempdir().expect("a configuration home with no place file");
    std::fs::create_dir_all(unplaced.path().join("nexus")).expect("nexus configuration");
    std::fs::write(unplaced.path().join("nexus/flow-nexus.datom"), "{ main }\n")
        .expect("the configuration names main");
    let refused = Command::new(env!("CARGO_BIN_EXE_flow-nexus-meta"))
        .env_clear()
        .env("XDG_CONFIG_HOME", unplaced.path())
        .arg("Retire.fac697")
        .output()
        .expect("client runs");
    assert_eq!(refused.status.code(), Some(2), "{refused:?}");
    let stderr = String::from_utf8_lossy(&refused.stderr);
    assert!(
        stderr.starts_with("Missing.")
            && stderr.contains(
                &*unplaced
                    .path()
                    .join("nexus/main/flow-nexus.signal")
                    .to_string_lossy()
            ),
        "{stderr}"
    );
    let refused = client(runtime.path(), unplaced.path(), &other, &["Retire.fac697"]);
    assert_eq!(refused.status.code(), Some(2), "{refused:?}");
    assert_eq!(
        default_listener
            .accept()
            .expect_err("nothing falls back to the default runtime")
            .kind(),
        std::io::ErrorKind::WouldBlock
    );
}

fn retired() -> signal_flow::FlowNode {
    signal_flow::FlowNode {
        flow_id: signal_flow::FlowId("fac697".into()),
        session_id: signal_flow::SessionId("session-1".into()),
        harness_kind: signal_flow::HarnessKind::Claude,
        endpoint_selection: signal_flow::EndpointSelection::Unavailable,
        herdr_route_selection: signal_flow::HerdrRouteSelection::Unavailable,
        origin: signal_flow::Origin::Unbound,
        flow_lifecycle: signal_flow::FlowLifecycle::Retired,
    }
}
