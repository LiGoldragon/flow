//! The typed invocation selects exactly one Nexus socket.
use std::{
    io::{Read, Write},
    os::unix::net::UnixListener,
    process::Command,
};

#[test]
fn the_client_uses_only_the_explicit_socket() {
    let runtime = tempfile::tempdir().expect("temporary runtime directory");
    let selected = runtime.path().join("meta.sock");
    let other = runtime.path().join("other.sock");
    let listener = UnixListener::bind(&selected).expect("selected fixture binds");
    let other_listener = UnixListener::bind(&other).expect("other fixture binds");
    other_listener
        .set_nonblocking(true)
        .expect("nonblocking fixture");
    let answering = std::thread::spawn(move || {
        let (mut peer, _) = listener.accept().expect("the selected client connects");
        let mut length = [0; 4];
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
    let output = Command::new(env!("CARGO_BIN_EXE_flow-meta"))
        .env_clear()
        .env("FLOW_META_SOCKET", &other)
        .arg(format!("{{ {} {} }}", selected.display(), "Retire.fac697"))
        .output()
        .expect("client runs");
    assert!(output.status.success(), "{output:?}");
    answering.join().expect("selected fixture answered");
    assert!(
        String::from_utf8_lossy(&output.stdout).starts_with("FlowRetired."),
        "{output:?}"
    );
    assert_eq!(
        other_listener
            .accept()
            .expect_err("other socket is untouched")
            .kind(),
        std::io::ErrorKind::WouldBlock
    );

    let absent = runtime.path().join("absent.sock");
    let failed = Command::new(env!("CARGO_BIN_EXE_flow-meta"))
        .env_clear()
        .env("FLOW_META_SOCKET", &other)
        .arg(format!("{{ {} {} }}", absent.display(), "Retire.fac697"))
        .output()
        .expect("client runs");
    assert!(!failed.status.success(), "{failed:?}");
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
        flow_id: "fac697".into(),
        session_id: "session-1".into(),
        harness_kind: signal_flow::HarnessKind::Claude,
        endpoint_selection: signal_flow::EndpointSelection::Unavailable,
        herdr_route_selection: signal_flow::HerdrRouteSelection::Unavailable,
        origin: signal_flow::Origin::Unbound,
        flow_lifecycle: signal_flow::FlowLifecycle::Retired,
    }
}
