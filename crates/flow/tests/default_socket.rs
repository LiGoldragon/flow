//! The CLI is `flow-nexus '<Query>'`: it takes the datom and nothing else.
//! The place is its configuration's,
//! `$XDG_CONFIG_HOME/nexus/flow-nexus.datom`, resolved under `$XDG_RUNTIME_DIR/nexus/` to that place's ordinary socket
//! and to no other, whatever else the environment names.
use std::{
    io::{Read, Write},
    os::unix::net::UnixListener,
    path::Path,
    process::{Command, Output},
};

fn client(runtime: &Path, configuration_home: &Path, other: &Path, arguments: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_flow-nexus"))
        .env_clear()
        .env("XDG_RUNTIME_DIR", runtime)
        .env("XDG_CONFIG_HOME", configuration_home)
        .env("FLOW_SOCKET", other)
        .args(arguments)
        .output()
        .expect("client runs")
}

#[test]
fn the_client_reaches_only_the_socket_of_the_place_its_configuration_names() {
    let runtime = tempfile::tempdir().expect("temporary runtime directory");
    let configuration = tempfile::tempdir().expect("temporary configuration home");
    std::fs::create_dir_all(configuration.path().join("nexus")).expect("nexus configuration");
    std::fs::write(configuration.path().join("nexus/flow-nexus.datom"), "{ next }\n")
        .expect("the configuration names next");
    let next = runtime.path().join("nexus/next");
    let main = runtime.path().join("nexus/main");
    std::fs::create_dir_all(&next).expect("the place's runtime directory");
    std::fs::create_dir_all(&main).expect("another place's runtime directory");
    let selected = next.join("flow.sock");
    let other = runtime.path().join("other.sock");
    let listener = UnixListener::bind(&selected).expect("selected fixture binds");
    let main_listener = UnixListener::bind(main.join("flow.sock")).expect("main fixture binds");
    let other_listener = UnixListener::bind(&other).expect("other fixture binds");
    for unreached in [&main_listener, &other_listener] {
        unreached
            .set_nonblocking(true)
            .expect("nonblocking fixture");
    }
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
    let output = client(runtime.path(), configuration.path(), &other, &["Metaflows"]);
    assert!(output.status.success(), "{output:?}");
    answering.join().expect("selected fixture answered");
    assert_eq!(String::from_utf8_lossy(&output.stdout).trim(), "Listed.[]");

    // A place typed before the datom is a second argument, and refused:
    // no place is ever typed, and nothing is sent anywhere.
    for typed in [&["main", "Metaflows"][..], &["next", "Metaflows"][..]] {
        let refused = client(runtime.path(), configuration.path(), &other, typed);
        assert_eq!(refused.status.code(), Some(2), "{typed:?}: {refused:?}");
        assert!(
            String::from_utf8_lossy(&refused.stderr).starts_with("usage: flow-nexus '"),
            "{typed:?}: {refused:?}"
        );
    }
    for unreached in [&main_listener, &other_listener] {
        assert_eq!(
            unreached
                .accept()
                .expect_err("no other socket is reached")
                .kind(),
            std::io::ErrorKind::WouldBlock
        );
    }
}

#[test]
fn a_missing_configuration_is_refused_naming_its_path() {
    let runtime = tempfile::tempdir().expect("temporary runtime directory");
    let configuration = tempfile::tempdir().expect("temporary configuration home");
    let main = runtime.path().join("nexus/main");
    std::fs::create_dir_all(&main).expect("the place's runtime directory");
    let main_listener = UnixListener::bind(main.join("flow.sock")).expect("main fixture binds");
    main_listener
        .set_nonblocking(true)
        .expect("nonblocking fixture");
    let other = runtime.path().join("other.sock");
    let path = configuration.path().join("nexus/flow-nexus.datom");
    let refused = client(runtime.path(), configuration.path(), &other, &["Metaflows"]);
    assert_eq!(refused.status.code(), Some(2), "{refused:?}");
    let stderr = String::from_utf8_lossy(&refused.stderr);
    assert!(
        stderr.starts_with("Missing.") && stderr.contains(&*path.to_string_lossy()),
        "{stderr}"
    );

    // With no XDG_CONFIG_HOME the file is ~/.config/nexus/flow-nexus.datom.
    let home = tempfile::tempdir().expect("temporary home");
    let refused = Command::new(env!("CARGO_BIN_EXE_flow-nexus"))
        .env_clear()
        .env("XDG_RUNTIME_DIR", runtime.path())
        .env("HOME", home.path())
        .arg("Metaflows")
        .output()
        .expect("client runs");
    assert_eq!(refused.status.code(), Some(2), "{refused:?}");
    let stderr = String::from_utf8_lossy(&refused.stderr);
    let unset = home.path().join(".config/nexus/flow-nexus.datom");
    assert!(
        stderr.starts_with("Missing.") && stderr.contains(&*unset.to_string_lossy()),
        "{stderr}"
    );

    // A configuration naming no place is refused, naming the file too.
    std::fs::create_dir_all(path.parent().expect("nexus directory")).expect("nexus directory");
    std::fs::write(&path, "{ «../main» }\n").expect("an invalid configuration");
    let refused = client(runtime.path(), configuration.path(), &other, &["Metaflows"]);
    assert_eq!(refused.status.code(), Some(2), "{refused:?}");
    let stderr = String::from_utf8_lossy(&refused.stderr);
    assert!(
        stderr.starts_with("Invalid.{ ") && stderr.contains(&*path.to_string_lossy()),
        "{stderr}"
    );
    assert_eq!(
        main_listener
            .accept()
            .expect_err("nothing falls back to main")
            .kind(),
        std::io::ErrorKind::WouldBlock
    );
}
