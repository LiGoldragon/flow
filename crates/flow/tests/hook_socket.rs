//! `flow-hook` reports through its typed socket configuration and ignores a
//! different socket in the inherited environment.
use std::{
    io::{Read, Write},
    os::unix::net::UnixListener,
    process::{Command, Stdio},
};

#[test]
fn the_hook_reports_only_to_its_typed_socket_configuration() {
    let root = tempfile::tempdir().expect("temporary root");
    let runtime = root.path().join("run");
    std::fs::create_dir_all(&runtime).expect("runtime directory");
    let launching = root.path().join("run/flow-next/flow");
    std::fs::create_dir_all(&launching).expect("launching Nexus's socket directory");
    let listener =
        UnixListener::bind(launching.join("flow.sock")).expect("fixture launching Nexus binds");
    let other = UnixListener::bind(launching.join("other.sock")).expect("other Nexus binds");
    other.set_nonblocking(true).expect("nonblocking fixture");
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
        .arg(format!("{{ {} }}", launching.join("flow.sock").display()))
        .env_clear()
        .env("XDG_RUNTIME_DIR", &runtime)
        .env("FLOW_ID", "5a4d0b")
        .env("FLOW_SOCKET", launching.join("other.sock"))
        .stdin(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("flow-hook runs");
    hook.stdin
        .take()
        .expect("hook stdin")
        .write_all(br#"{"hook_event_name":"Stop","session_id":"s","transcript_path":"/t/s.jsonl"}"#)
        .expect("event written");
    let output = hook.wait_with_output().expect("flow-hook ends");
    assert!(output.status.success(), "{output:?}");
    assert_eq!(
        receiving
            .try_recv()
            .expect("the hook reached the launching Nexus"),
        signal_flow::Query::Report(signal_flow::Report_Data {
            flow_id: "5a4d0b".into(),
            event: signal_flow::Event::Stopped(signal_flow::NativeStop {
                native_session_id_option: Some("s".into()),
                transcript_path_option: Some("/t/s.jsonl".into()),
                last_assistant_message_option: None,
            }),
        })
    );
    assert_eq!(
        other
            .accept()
            .expect_err("other socket is untouched")
            .kind(),
        std::io::ErrorKind::WouldBlock
    );
    assert_eq!(
        String::from_utf8_lossy(&output.stderr),
        "Stop\tReport.{ 5a4d0b Stopped.{ Some.s Some./t/s.jsonl None } }\t0\tReported\n"
    );
    assert!(!runtime.join("flow/flow.sock").exists());
}
