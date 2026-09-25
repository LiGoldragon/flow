//! The Nexus starts with no arguments, no Flow environment, and a fresh HOME.
use flow_nexus::Frame;
use std::{
    io::{Read, Write},
    os::unix::net::UnixStream,
    path::Path,
    process::{Command, Stdio},
    time::Duration,
};

struct StartedNexus {
    child: std::process::Child,
}

impl Drop for StartedNexus {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn connect(socket: &Path, nexus: &mut StartedNexus) -> UnixStream {
    for _ in 0..500 {
        if let Ok(peer) = UnixStream::connect(socket) {
            return peer;
        }
        if let Some(status) = nexus.child.try_wait().expect("child status") {
            let mut stderr = String::new();
            nexus
                .child
                .stderr
                .take()
                .map(|mut pipe| pipe.read_to_string(&mut stderr));
            panic!("flow-nexus exited with {status}: {stderr}");
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    panic!("{} never listened", socket.display())
}

#[test]
fn nexus_starts_from_defaults_and_answers_after_meta_configure() {
    let home = tempfile::tempdir().expect("temporary home");
    let runtime = tempfile::tempdir().expect("temporary runtime directory");
    let mut nexus = StartedNexus {
        child: Command::new(env!("CARGO_BIN_EXE_flow-nexus"))
            .env_clear()
            .env("HOME", home.path())
            .env("XDG_RUNTIME_DIR", runtime.path())
            .stderr(Stdio::piped())
            .spawn()
            .expect("flow-nexus spawns"),
    };
    let meta_socket = runtime.path().join("flow/flow-meta.sock");
    let ordinary_socket = runtime.path().join("flow/flow.sock");
    let configuration = meta_signal_flow::Configuration {
        ordinary_socket_path: ordinary_socket.to_string_lossy().into_owned(),
        meta_socket_path: meta_socket.to_string_lossy().into_owned(),
    };
    let mut meta = connect(&meta_socket, &mut nexus);
    let query = meta_signal_flow::Query::Configure(configuration.clone());
    let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&query).expect("configure encodes");
    meta.write_all(&(bytes.len() as u32).to_be_bytes())
        .and_then(|_| meta.write_all(&bytes))
        .expect("configure written");
    let mut length = [0; 4];
    meta.read_exact(&mut length).expect("configure length");
    let mut reply = vec![0; u32::from_be_bytes(length) as usize];
    meta.read_exact(&mut reply).expect("configure reply");
    let reply = rkyv::from_bytes::<meta_signal_flow::Response, rkyv::rancor::Error>(&reply)
        .expect("configure reply decodes");
    assert!(matches!(reply, meta_signal_flow::Response::Configured(_)));

    let mut ordinary = connect(&ordinary_socket, &mut nexus);
    Frame::write_query(
        &mut ordinary,
        &signal_flow::Query::List(signal_flow::ListRequest {}),
    )
    .expect("list written");
    assert!(matches!(
        Frame::read_response(&mut ordinary).expect("list answered"),
        signal_flow::Response::Listed(rows) if rows.is_empty()
    ));
    assert!(home.path().join(".local/state/flow/flow.sema").exists());
}
