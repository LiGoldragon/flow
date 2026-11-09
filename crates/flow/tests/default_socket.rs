//! The CLI is `flow <place-name> '<Query>'`: the place name resolves under
//! `$XDG_RUNTIME_DIR/nexus/` to that place's ordinary socket and to no
//! other, whatever else the environment names.
use std::{
    io::{Read, Write},
    os::unix::net::UnixListener,
    process::Command,
};

#[test]
fn the_client_reaches_only_the_socket_of_the_place_it_names() {
    let runtime = tempfile::tempdir().expect("temporary runtime directory");
    let main = runtime.path().join("nexus/main");
    std::fs::create_dir_all(&main).expect("the place's runtime directory");
    let selected = main.join("flow.sock");
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
        let reply = rkyv::to_bytes::<rkyv::rancor::Error>(&signal_flow::Response::Listed(vec![]))
            .expect("reply encodes");
        peer.write_all(&(reply.len() as u32).to_be_bytes())
            .and_then(|_| peer.write_all(&reply))
            .expect("reply written");
    });
    let output = Command::new(env!("CARGO_BIN_EXE_flow"))
        .env_clear()
        .env("XDG_RUNTIME_DIR", runtime.path())
        .env("FLOW_SOCKET", &other)
        .args(["main", "Metaflows"])
        .output()
        .expect("client runs");
    assert!(output.status.success(), "{output:?}");
    answering.join().expect("selected fixture answered");
    assert_eq!(String::from_utf8_lossy(&output.stdout).trim(), "Listed.[]");

    // A place with no Nexus, a name that is no place, and no runtime root
    // at all each fail, and nothing falls back to another socket.
    for (place, runtime_directory) in [
        ("next", Some(runtime.path())),
        ("../other.sock", Some(runtime.path())),
        ("main", None),
    ] {
        let mut command = Command::new(env!("CARGO_BIN_EXE_flow"));
        command.env_clear().env("FLOW_SOCKET", &other);
        if let Some(directory) = runtime_directory {
            command.env("XDG_RUNTIME_DIR", directory);
        }
        let failed = command
            .args([place, "Metaflows"])
            .output()
            .expect("client runs");
        assert!(!failed.status.success(), "{place}: {failed:?}");
    }
    assert_eq!(
        other_listener
            .accept()
            .expect_err("other socket is untouched")
            .kind(),
        std::io::ErrorKind::WouldBlock
    );
}
