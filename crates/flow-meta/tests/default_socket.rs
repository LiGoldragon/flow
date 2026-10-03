//! With no socket chosen, the meta client reaches the Nexus's default meta
//! socket under the caller's `XDG_RUNTIME_DIR`.
use std::{
    io::{Read, Write},
    os::unix::net::UnixListener,
    process::Command,
};

#[test]
fn the_meta_client_reaches_the_default_meta_socket_under_the_runtime_directory() {
    let runtime = tempfile::tempdir().expect("temporary runtime directory");
    std::fs::create_dir(runtime.path().join("flow")).expect("socket directory");
    let listener = UnixListener::bind(runtime.path().join("flow/flow-meta.sock"))
        .expect("fixture Nexus binds");
    let answering = std::thread::spawn(move || {
        let (mut peer, _) = listener.accept().expect("the client connects");
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
        .env("XDG_RUNTIME_DIR", runtime.path())
        .arg("Retire.fac697")
        .output()
        .expect("flow-meta runs");
    assert!(output.status.success(), "{output:?}");
    answering.join().expect("fixture Nexus answered");
    assert!(
        String::from_utf8_lossy(&output.stdout).starts_with("FlowRetired."),
        "{output:?}"
    );
}

fn retired() -> signal_flow::FlowNode {
    signal_flow::FlowNode {
        flow_id: "fac697".into(),
        session_id: "session-1".into(),
        harness_kind: signal_flow::HarnessKind::Claude,
        endpoint_selection: signal_flow::EndpointSelection::Unavailable,
        herdr_route_selection: signal_flow::HerdrRouteSelection::Unavailable,
        origin_clue: signal_flow::OriginClue {
            flow_id: "fac697".into(),
            session_id: "session-1".into(),
            turn_id: "unavailable".into(),
        },
        flow_lifecycle: signal_flow::FlowLifecycle::Retired,
    }
}
