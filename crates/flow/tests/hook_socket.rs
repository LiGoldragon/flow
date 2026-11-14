//! `flow-nexus-hook` reports through its typed socket configuration and ignores a
//! different socket in the inherited environment.
use std::{
    io::{Read, Write},
    os::unix::net::UnixListener,
    process::{Command, Stdio},
};

/// Reads the client's first frame as the Signal greeting and answers it as
/// Flow does: `Greeted` with this contract's digest, or, refusing, the typed
/// `GreetingRefused`. True when the frame was signal-flow's greeting.
fn answer_greeting(peer: &mut std::os::unix::net::UnixStream, greet: bool) -> bool {
    use signal_flow::Contracted;
    let mut length = [0; 4];
    peer.read_exact(&mut length).expect("greeting length");
    let mut frame = vec![0; u32::from_be_bytes(length) as usize];
    peer.read_exact(&mut frame).expect("greeting");
    let greeting = rkyv::from_bytes::<signal_flow::Handshake, rkyv::rancor::Error>(&frame)
        .expect("the first frame is a greeting");
    let receipt = if greet {
        signal_flow::Input::receipt(&greeting)
    } else {
        signal_flow::HandshakeReceipt::GreetingRefused(
            signal_flow::HandshakeRejection::ContractMismatch(signal_flow::ContractDigest(0)),
        )
    };
    let reply = rkyv::to_bytes::<rkyv::rancor::Error>(&receipt).expect("receipt encodes");
    peer.write_all(&(reply.len() as u32).to_be_bytes())
        .and_then(|_| peer.write_all(&reply))
        .expect("receipt written");
    greeting == signal_flow::Input::greeting()
}

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
        assert!(answer_greeting(&mut peer, true), "the hook greets first");
        let mut length = [0; 4];
        peer.read_exact(&mut length).expect("query length");
        let mut query = vec![0; u32::from_be_bytes(length) as usize];
        peer.read_exact(&mut query).expect("query");
        received
            .send(
                rkyv::from_bytes::<signal_flow::Input, rkyv::rancor::Error>(&query)
                    .expect("the query is a signal-flow Input"),
            )
            .expect("the test receives the query");
        let reply = rkyv::to_bytes::<rkyv::rancor::Error>(&signal_flow::Output::Reported)
            .expect("reply encodes");
        peer.write_all(&(reply.len() as u32).to_be_bytes())
            .and_then(|_| peer.write_all(&reply))
            .expect("reply written");
    });
    let mut hook = Command::new(env!("CARGO_BIN_EXE_flow-nexus-hook"))
        .arg(format!("{{ {} }}", launching.join("flow.sock").display()))
        .env_clear()
        .env("XDG_RUNTIME_DIR", &runtime)
        .env("FLOW_ID", "5a4d0b")
        .env("FLOW_SOCKET", launching.join("other.sock"))
        .stdin(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("flow-nexus-hook runs");
    hook.stdin
        .take()
        .expect("hook stdin")
        .write_all(
            br#"{"hook_event_name":"Stop","session_id":"s","transcript_path":"/t/s.jsonl","background_tasks":[]}"#,
        )
        .expect("event written");
    let output = hook.wait_with_output().expect("flow-nexus-hook ends");
    assert!(output.status.success(), "{output:?}");
    assert_eq!(
        receiving
            .try_recv()
            .expect("the hook reached the launching Nexus"),
        signal_flow::Input::Report(signal_flow::Report_Data {
            flow_id: signal_flow::FlowId("5a4d0b".into()),
            process_option: None,
            event: signal_flow::Event::Stopped(signal_flow::NativeStop {
                native_session_id_option: Some(signal_flow::NativeSessionId("s".into())),
                transcript_path_option: Some(signal_flow::TranscriptPath("/t/s.jsonl".into())),
                last_assistant_message_option: signal_flow::LastAssistantMessageOption(None),
                background_work: signal_flow::BackgroundWork(vec![]),
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
        "Stop\t{ 5a4d0b None Stopped.{ Some.s Some./t/s.jsonl [] } }\t0\tReported\n"
    );
    assert!(!runtime.join("flow/flow.sock").exists());
}

/// A Stop whose input names no background work does not decode: the hook
/// reports nothing to its Nexus, writes the typed refusal on stderr and
/// exits 1, the non-blocking error Claude Code shows; never 2, which would
/// keep the turn going.
#[test]
fn an_undecodable_stop_is_refused_visibly_and_reaches_no_nexus() {
    let root = tempfile::tempdir().expect("temporary root");
    let launching = root.path().join("run/flow");
    std::fs::create_dir_all(&launching).expect("launching Nexus's socket directory");
    let listener =
        UnixListener::bind(launching.join("flow.sock")).expect("fixture launching Nexus binds");
    listener.set_nonblocking(true).expect("nonblocking fixture");
    let mut hook = Command::new(env!("CARGO_BIN_EXE_flow-nexus-hook"))
        .arg(format!("{{ {} }}", launching.join("flow.sock").display()))
        .env_clear()
        .env("FLOW_ID", "5a4d0b")
        .stdin(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("flow-nexus-hook runs");
    hook.stdin
        .take()
        .expect("hook stdin")
        .write_all(br#"{"hook_event_name":"Stop","session_id":"s","transcript_path":"/t/s.jsonl"}"#)
        .expect("event written");
    let output = hook.wait_with_output().expect("flow-nexus-hook ends");
    assert_eq!(output.status.code(), Some(1), "{output:?}");
    assert_eq!(
        String::from_utf8_lossy(&output.stderr),
        "Stop\t-\t-\tRefused.BackgroundTasksAbsent\n"
    );
    assert_eq!(
        listener
            .accept()
            .expect_err("nothing reached the Nexus")
            .kind(),
        std::io::ErrorKind::WouldBlock
    );
}

#[test]
fn assistant_content_is_transport_only_for_every_diagnostic_outcome() {
    let marker = "hook-assistant-only-marker-雪";
    for body in [
        None,
        Some(String::new()),
        Some(marker.into()),
        Some(marker.repeat(16_384)),
    ] {
        for outcome in ["success", "transport-error", "invalid-configuration"] {
            let root = tempfile::tempdir().unwrap();
            let socket = root.path().join("flow.sock");
            let receiver = if outcome == "success" {
                let listener = UnixListener::bind(&socket).unwrap();
                Some(std::thread::spawn(move || {
                    let (mut peer, _) = listener.accept().unwrap();
                    assert!(answer_greeting(&mut peer, true));
                    let mut length = [0; 4];
                    peer.read_exact(&mut length).unwrap();
                    let mut bytes = vec![0; u32::from_be_bytes(length) as usize];
                    peer.read_exact(&mut bytes).unwrap();
                    let query = rkyv::from_bytes::<signal_flow::Input, rkyv::rancor::Error>(&bytes)
                        .unwrap();
                    let reply =
                        rkyv::to_bytes::<rkyv::rancor::Error>(&signal_flow::Output::Reported)
                            .unwrap();
                    peer.write_all(&(reply.len() as u32).to_be_bytes()).unwrap();
                    peer.write_all(&reply).unwrap();
                    query
                }))
            } else {
                None
            };
            let configuration = if outcome == "invalid-configuration" {
                "invalid".to_owned()
            } else {
                format!("{{ {} }}", socket.display())
            };
            let mut child = Command::new(env!("CARGO_BIN_EXE_flow-nexus-hook"))
                .arg(configuration)
                .env_clear()
                .env("FLOW_ID", "5a4d0b")
                .stdin(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
                .unwrap();
            let mut input = serde_json::json!({
                "hook_event_name": "Stop", "session_id": "session-metadata",
                "transcript_path": "/t/session-metadata", "background_tasks": [],
            });
            if let Some(body) = &body {
                input["last_assistant_message"] = body.clone().into();
            }
            child
                .stdin
                .take()
                .unwrap()
                .write_all(input.to_string().as_bytes())
                .unwrap();
            let output = child.wait_with_output().unwrap();
            assert!(output.status.success());
            let diagnostic = String::from_utf8(output.stderr).unwrap();
            assert!(
                !diagnostic.contains(marker),
                "{outcome}: assistant body reached stderr"
            );
            assert!(diagnostic.contains("session-metadata"), "{diagnostic}");
            assert!(
                !diagnostic.contains("Report."),
                "diagnostics serialize metadata, not the transport query"
            );
            match outcome {
                "success" => assert!(diagnostic.ends_with("\t0\tReported\n"), "{diagnostic}"),
                "transport-error" => assert!(diagnostic.contains("\t2\t"), "{diagnostic}"),
                "invalid-configuration" => assert!(
                    diagnostic.ends_with("invalid hook configuration\n"),
                    "{diagnostic}"
                ),
                _ => unreachable!(),
            }
            if let Some(receiver) = receiver {
                let signal_flow::Input::Report(report) = receiver.join().unwrap() else {
                    panic!("Report")
                };
                let signal_flow::Event::Stopped(stop) = report.event else {
                    panic!("Stop")
                };
                assert_eq!(
                    stop.last_assistant_message_option.0, body,
                    "ordinary60 body stays unchanged"
                );
            }
        }
    }
}

/// O4: against a Nexus that refuses its greeting the hook writes no Report,
/// prints its `2` line and exits 0, never blocking the turn.
#[test]
fn the_hook_stops_at_a_refused_greeting() {
    let root = tempfile::tempdir().expect("temporary root");
    let socket = root.path().join("flow.sock");
    let listener = UnixListener::bind(&socket).expect("fixture binds");
    let refusing = std::thread::spawn(move || {
        let (mut peer, _) = listener.accept().expect("the hook connects");
        assert!(answer_greeting(&mut peer, false));
        let mut rest = Vec::new();
        peer.read_to_end(&mut rest).expect("the hook closes");
        rest
    });
    let mut hook = Command::new(env!("CARGO_BIN_EXE_flow-nexus-hook"))
        .arg(format!("{{ {} }}", socket.display()))
        .env_clear()
        .env("FLOW_ID", "5a4d0b")
        .stdin(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("flow-nexus-hook runs");
    hook.stdin
        .take()
        .expect("hook stdin")
        .write_all(
            br#"{"hook_event_name":"Stop","session_id":"s","transcript_path":"/t/s.jsonl","background_tasks":[]}"#,
        )
        .expect("event written");
    let output = hook.wait_with_output().expect("flow-nexus-hook ends");
    assert!(output.status.success(), "{output:?}");
    let diagnostic = String::from_utf8_lossy(&output.stderr);
    assert!(diagnostic.contains("\t2\t"), "{diagnostic}");
    assert!(
        refusing.join().expect("fixture answered").is_empty(),
        "no Report after a refused greeting"
    );
}
