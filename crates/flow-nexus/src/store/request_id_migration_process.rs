//! Actual accepted-predecessor Memory refusal through the production process.
//!
//! The exact accepted binary creates one store; its exact-source fixture
//! supplies a populated second store. Current Flow must refuse both before
//! publishing sockets and leave every byte and directory entry unchanged.

use super::{FlowStore, OpensFlowStore, StoreError};
use std::{
    path::{Path, PathBuf},
    process::{Child, Command, ExitStatus, Stdio},
    time::{Duration, Instant},
};

struct FlowChild {
    child: Child,
}

impl FlowChild {
    fn stop(mut self) {
        // SAFETY: this pid is the fixture child and Flow documents SIGTERM as
        // its orderly stop request.
        let _ = unsafe { libc::kill(self.child.id() as i32, libc::SIGTERM) };
        let status = self.wait_for_exit("Flow did not stop in ten seconds");
        assert!(status.success(), "Flow stopped with {status}");
    }

    fn wait_for_failure(mut self) -> ExitStatus {
        let status = self.wait_for_exit("refused Flow did not exit in ten seconds");
        assert!(!status.success(), "earlier Memory was unexpectedly served");
        status
    }

    fn wait_for_exit(&mut self, timeout: &str) -> ExitStatus {
        use crate::event_wait::{AwaitsDescriptors, OpensExitDescriptor};
        use std::os::fd::AsFd;
        // The child's exit is awaited as its pidfd, bounded by ten seconds;
        // a child already reaped needs no wait.
        if let Ok(exit) = self.child.id().exit_descriptor() {
            let _ = [exit.as_fd()].ready(Some(Instant::now() + Duration::from_secs(10)));
        }
        match self.child.try_wait().expect("fixture child status") {
            Some(status) => status,
            None => {
                let _ = self.child.kill();
                let _ = self.child.wait();
                panic!("{timeout}");
            }
        }
    }
}

impl Drop for FlowChild {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn required_path(name: &str) -> PathBuf {
    let path =
        PathBuf::from(std::env::var_os(name).unwrap_or_else(|| panic!("{name} is required")));
    assert_eq!(
        std::fs::canonicalize(&path).expect("migration binary canonicalizes"),
        path,
        "the migration check supplies an immutable canonical binary"
    );
    path
}

fn required_directory(name: &str) -> PathBuf {
    let path =
        PathBuf::from(std::env::var_os(name).unwrap_or_else(|| panic!("{name} is required")));
    assert!(path.is_dir(), "{name} names a directory");
    path
}

/// The accepted predecessor's Start: its sockets and its store named.
fn predecessor_start_argument(runtime: &Path, store: &Path) -> String {
    format!(
        "{{ «{}» «{}» «{}» None }}",
        runtime.join("flow.sock").display(),
        runtime.join("flow-meta.sock").display(),
        store.display(),
    )
}

/// The current Flow's place: its sockets in `runtime`, its memory, a
/// `flow.sema`, in the store's directory.
fn place_argument(runtime: &Path, store: &Path) -> String {
    assert_eq!(
        store.file_name().and_then(|name| name.to_str()),
        Some("flow.sema"),
        "a place keeps its memory as flow.sema"
    );
    format!(
        "{{ main «{}» «{}» }}",
        runtime.display(),
        store.parent().expect("the store has a directory").display(),
    )
}

fn spawn_flow(binary: &Path, home: &Path, runtime: &Path, store: &Path) -> FlowChild {
    spawn_with(binary, home, runtime, place_argument(runtime, store))
}

fn spawn_predecessor(binary: &Path, home: &Path, runtime: &Path, store: &Path) -> FlowChild {
    spawn_with(
        binary,
        home,
        runtime,
        predecessor_start_argument(runtime, store),
    )
}

fn spawn_with(binary: &Path, home: &Path, runtime: &Path, argument: String) -> FlowChild {
    let child = Command::new(binary)
        .env_clear()
        .env("HOME", home)
        .env("XDG_RUNTIME_DIR", runtime)
        .arg(argument)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("Flow starts");
    FlowChild { child }
}

/// The current Flow's socket, moved in at its name only once it listens:
/// awaited as that event, then connected.
fn wait_for_socket(path: &Path) {
    use crate::event_wait::AwaitsPath;
    assert!(
        path.appeared_by(Instant::now() + Duration::from_secs(30)),
        "{} never listened",
        path.display()
    );
    drop(UnixStream::connect(path).expect("the current Flow's socket listens"));
}

fn serve_predecessor_once(binary: &Path, home: &Path, runtime: &Path, store: &Path) {
    let child = spawn_predecessor(binary, home, runtime, store);
    wait_for_socket(&runtime.join("flow.sock"));
    child.stop();
}

struct PredecessorMemory {
    path: PathBuf,
}

trait RefusesEarlierMemory {
    fn prove_refusal(&self, binary: &Path, home: &Path);
}

impl RefusesEarlierMemory for PredecessorMemory {
    fn prove_refusal(&self, binary: &Path, home: &Path) {
        let before = std::fs::read(&self.path).expect("predecessor bytes read");
        let directory = self.path.parent().expect("Memory has a directory");
        let entries = || {
            let mut entries = std::fs::read_dir(directory)
                .expect("Memory directory reads")
                .map(|entry| entry.expect("Memory entry reads").file_name())
                .collect::<Vec<_>>();
            entries.sort();
            entries
        };
        let before_entries = entries();
        for _ in 0..2 {
            let runtime = tempfile::tempdir().expect("private current runtime");
            let diagnostic = tempfile::tempfile().expect("private diagnostic file");
            let child = Command::new(binary)
                .env_clear()
                .env("HOME", home)
                .env("XDG_RUNTIME_DIR", runtime.path())
                .arg(place_argument(runtime.path(), &self.path))
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(diagnostic.try_clone().expect("diagnostic handle clones"))
                .spawn()
                .expect("current production Flow starts");
            FlowChild { child }.wait_for_failure();
            use std::io::{Read, Seek};
            let mut diagnostic = diagnostic;
            diagnostic.rewind().expect("diagnostic rewinds");
            let mut text = String::new();
            diagnostic
                .read_to_string(&mut text)
                .expect("diagnostic reads");
            assert!(
                text.contains("earlier Flow's Memory"),
                "wrong refusal: {text}"
            );
            assert!(text.contains("Memory v4 alone"), "wrong schema: {text}");
            for socket in ["flow.sock", "flow-meta.sock"] {
                assert!(
                    !runtime.path().join(socket).exists(),
                    "refusal published {socket}"
                );
            }
            assert_eq!(std::fs::read(&self.path).unwrap(), before);
            assert_eq!(entries(), before_entries);
        }
        // The in-process boundary agrees with the production process and
        // also performs no write; a process exit alone could be unrelated.
        assert!(matches!(
            FlowStore::open_configured(&self.path),
            Err(StoreError::EarlierMemory { .. })
        ));
        assert_eq!(std::fs::read(&self.path).unwrap(), before);
        assert_eq!(entries(), before_entries);
    }
}

#[test]
#[ignore = "the dedicated Nix check supplies exact predecessor and current production binaries"]
fn actual_predecessor_memory_is_refused_without_writing() {
    let predecessor_revision = std::env::var("FLOW_MIGRATION_PREDECESSOR_REV").unwrap();
    assert_eq!(
        predecessor_revision,
        "f7d2336a7380206728cbd5aef2ebb3656d0cfd87"
    );
    let current_revision = std::env::var("FLOW_MIGRATION_CURRENT_REV").unwrap();
    assert_eq!(
        current_revision,
        std::env::var("FLOW_MIGRATION_SOURCE_REV").unwrap()
    );
    let predecessor = required_path("FLOW_MIGRATION_PREDECESSOR_BIN");
    let current = required_path("FLOW_MIGRATION_CURRENT_BIN");
    let fixture = required_directory("FLOW_MIGRATION_PREDECESSOR_STORE");
    let home = tempfile::tempdir().expect("private home");
    let exact_state = tempfile::tempdir().expect("exact predecessor state");
    let exact_path = exact_state.path().join("flow.sema");
    let predecessor_runtime = tempfile::tempdir().expect("predecessor runtime");
    serve_predecessor_once(
        &predecessor,
        home.path(),
        predecessor_runtime.path(),
        &exact_path,
    );
    PredecessorMemory { path: exact_path }.prove_refusal(&current, home.path());

    let populated_state = tempfile::tempdir().expect("populated predecessor state");
    let populated_path = populated_state.path().join("flow.sema");
    std::fs::copy(fixture.join("flow.sema"), &populated_path).expect("populated Memory copies");
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(&populated_path, std::fs::Permissions::from_mode(0o600))
        .expect("copied Memory is writable, so write protection cannot establish refusal");
    PredecessorMemory {
        path: populated_path,
    }
    .prove_refusal(&current, home.path());
    println!(
        "PREDECESSOR_REFUSAL_OK predecessor={predecessor_revision} current={current_revision}"
    );
}
