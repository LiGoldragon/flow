//! The CLI is `flow-nexus-meta '<Query>'`: it takes the datom and nothing
//! else. The place is the Flow configuration's,
//! `$XDG_CONFIG_HOME/nexus/flow-nexus.datom`, resolved under
//! `$XDG_RUNTIME_DIR/nexus/` to that place's meta socket alone.
use std::{
    io::{Read, Write},
    os::unix::net::UnixListener,
    path::Path,
    process::{Command, Output},
};

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
    let main = runtime.path().join("nexus/main");
    std::fs::create_dir_all(&main).expect("the place's runtime directory");
    let selected = main.join("flow-meta.sock");
    let other = runtime.path().join("other.sock");
    let listener = UnixListener::bind(&selected).expect("selected fixture binds");
    let other_listener = UnixListener::bind(&other).expect("other fixture binds");
    other_listener
        .set_nonblocking(true)
        .expect("nonblocking fixture");
    let answering = std::thread::spawn(move || {
        let (mut peer, _) = listener.accept().expect("the selected client connects");
        let mut length = [0; 4];
        use signal::Contracted;
        peer.read_exact(&mut length).expect("greeting length");
        let mut greeting = vec![0; u32::from_be_bytes(length) as usize];
        peer.read_exact(&mut greeting).expect("greeting");
        let greeting = rkyv::from_bytes::<signal::Handshake, rkyv::rancor::Error>(&greeting)
            .expect("the client sends a Signal greeting first");
        let receipt = meta_signal_flow::Query::receipt(&greeting);
        assert!(matches!(receipt, signal::HandshakeReceipt::Greeted(_)));
        let receipt = rkyv::to_bytes::<rkyv::rancor::Error>(&receipt).expect("receipt encodes");
        peer.write_all(&(receipt.len() as u32).to_be_bytes())
            .and_then(|_| peer.write_all(&receipt))
            .expect("receipt written");
        peer.read_exact(&mut length).expect("query length");
        let mut query = vec![0; u32::from_be_bytes(length) as usize];
        peer.read_exact(&mut query).expect("query");
        let reply = rkyv::to_bytes::<rkyv::rancor::Error>(
            &meta_signal_flow::Response::FlowRetired(retired()),
        )
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
    assert_eq!(
        other_listener
            .accept()
            .expect_err("no fallback connection")
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
