//! `flow-hook` reports to the Nexus its environment names in FLOW_SOCKET,
//! the one Flow exports into the pane of a flow it launches, even when the
//! default ordinary socket under the caller's runtime directory does not
//! exist (a seat launched by the next slot's Nexus).
use std::{
    io::{Read, Write},
    os::unix::net::UnixListener,
    process::{Command, Stdio},
};

#[test]
fn the_hook_reports_to_the_socket_its_launching_nexus_exported() {
    let root = tempfile::tempdir().expect("temporary root");
    let runtime = root.path().join("run");
    std::fs::create_dir_all(&runtime).expect("runtime directory");
    let launching = root.path().join("run/flow-next/flow");
    std::fs::create_dir_all(&launching).expect("launching Nexus's socket directory");
    let listener =
        UnixListener::bind(launching.join("flow.sock")).expect("fixture launching Nexus binds");
    // The query is handed over before the reply is written, so once the
    // hook has ended it is either here or was never sent.
    let (received, receiving) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let (mut peer, _) = listener.accept().expect("the hook's client connects");
        let mut length = [0; 4];
        peer.read_exact(&mut length).expect("query length");
        let mut query = vec![0; u32::from_be_bytes(length) as usize];
        peer.read_exact(&mut query).expect("query");
        received
            .send(
                rkyv::from_bytes::<signal_flow::Query, rkyv::rancor::Error>(&query)
                    .expect("the query is a signal-flow Query"),
            )
            .expect("the test receives the query");
        let reply = rkyv::to_bytes::<rkyv::rancor::Error>(&signal_flow::Response::Reported)
            .expect("reply encodes");
        peer.write_all(&(reply.len() as u32).to_be_bytes())
            .and_then(|_| peer.write_all(&reply))
            .expect("reply written");
    });
    let mut hook = Command::new(env!("CARGO_BIN_EXE_flow-hook"))
        .env_clear()
        .env("XDG_RUNTIME_DIR", &runtime)
        .env("FLOW_ID", "5a4d0b")
        .env("FLOW_SOCKET", launching.join("flow.sock"))
        .stdin(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("flow-hook runs");
    hook.stdin
        .take()
        .expect("hook stdin")
        .write_all(br#"{"hook_event_name":"Stop","session_id":"s"}"#)
        .expect("event written");
    let output = hook.wait_with_output().expect("flow-hook ends");
    assert!(output.status.success(), "{output:?}");
    assert_eq!(
        receiving
            .try_recv()
            .expect("the hook reached the launching Nexus"),
        signal_flow::Query::Report(signal_flow::Report_Data {
            flow_id: "5a4d0b".into(),
            event: signal_flow::Event::Stopped,
        })
    );
    assert_eq!(
        String::from_utf8_lossy(&output.stderr),
        "Stop\tReport.{ «5a4d0b» Stopped }\t0\tReported\n"
    );
    assert!(!runtime.join("flow/flow.sock").exists());
}
