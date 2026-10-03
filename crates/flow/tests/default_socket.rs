//! With no socket chosen, the client reaches the Nexus's default ordinary
//! socket under the caller's `XDG_RUNTIME_DIR`.
use std::{
    io::{Read, Write},
    os::unix::net::UnixListener,
    process::Command,
};

#[test]
fn the_client_reaches_the_default_ordinary_socket_under_the_runtime_directory() {
    let runtime = tempfile::tempdir().expect("temporary runtime directory");
    std::fs::create_dir(runtime.path().join("flow")).expect("socket directory");
    let listener =
        UnixListener::bind(runtime.path().join("flow/flow.sock")).expect("fixture Nexus binds");
    let answering = std::thread::spawn(move || {
        let (mut peer, _) = listener.accept().expect("the client connects");
        let mut length = [0; 4];
        peer.read_exact(&mut length).expect("query length");
        let mut query = vec![0; u32::from_be_bytes(length) as usize];
        peer.read_exact(&mut query).expect("query");
        let reply = rkyv::to_bytes::<rkyv::rancor::Error>(&signal_flow::Response::Listed(vec![]))
            .expect("reply encodes");
        peer.write_all(&(reply.len() as u32).to_be_bytes())
            .and_then(|_| peer.write_all(&reply))
            .expect("reply written");
    });
    let output = Command::new(env!("CARGO_BIN_EXE_flow"))
        .env_clear()
        .env("XDG_RUNTIME_DIR", runtime.path())
        .arg("List.{ }")
        .output()
        .expect("flow runs");
    assert!(output.status.success(), "{output:?}");
    answering.join().expect("fixture Nexus answered");
    assert_eq!(String::from_utf8_lossy(&output.stdout).trim(), "Listed.[]");
}
