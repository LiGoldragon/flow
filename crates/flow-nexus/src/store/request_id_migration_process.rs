//! Actual-process predecessor-to-current Memory evolution proof.
//!
//! The dedicated Nix check supplies one exact configured predecessor Nexus
//! and the Nexus built from this source.  The predecessor creates and serves
//! the Memory; the current process evolves, serves and reopens that same
//! Memory.  This is deliberately separate from the source-frozen record
//! fixtures in `request_id_upgrade`.

use super::{
    DescribesRequestPolicyTable, FLOW_SCHEMA_VERSION, FlowStore, ManagesMetaflowState,
    OpensFlowStore, ReadsFlowMemory, RecordsLogicalDeliveries,
    StoredMetaRequestPolicyConfiguration,
};
use sema_engine::{Engine, EngineOpen, QueryPlan, SchemaVersion};
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
        assert!(
            !status.success(),
            "malformed Memory was unexpectedly served"
        );
        status
    }

    fn wait_for_exit(&mut self, timeout: &str) -> ExitStatus {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if let Some(status) = self.child.try_wait().expect("fixture child status") {
                return status;
            }
            if Instant::now() >= deadline {
                let _ = self.child.kill();
                let _ = self.child.wait();
                panic!("{timeout}");
            }
            std::thread::sleep(Duration::from_millis(10));
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

fn start_argument(runtime: &Path, store: &Path) -> String {
    format!(
        "{{ «{}» «{}» «{}» None }}",
        runtime.join("flow.sock").display(),
        runtime.join("flow-meta.sock").display(),
        store.display(),
    )
}

fn spawn_flow(binary: &Path, home: &Path, runtime: &Path, store: &Path) -> FlowChild {
    let child = Command::new(binary)
        .env_clear()
        .env("HOME", home)
        .env("XDG_RUNTIME_DIR", runtime)
        .arg(start_argument(runtime, store))
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("Flow starts");
    FlowChild { child }
}

fn wait_for_socket(path: &Path) {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if let Ok(peer) = std::os::unix::net::UnixStream::connect(path) {
            drop(peer);
            return;
        }
        assert!(
            Instant::now() < deadline,
            "{} never listened",
            path.display()
        );
        std::thread::sleep(Duration::from_millis(10));
    }
}

fn serve_once(binary: &Path, home: &Path, runtime: &Path, store: &Path) {
    let child = spawn_flow(binary, home, runtime, store);
    wait_for_socket(&runtime.join("flow.sock"));
    child.stop();
}

fn inject_malformed_request_policy(store_path: &Path) {
    let store = FlowStore::open_configured(store_path).expect("current Memory opens before fault");
    FlowStore::replace_configuration_family(
        &store.engine,
        store.meta_configuration_tables.request_policy,
        vec![StoredMetaRequestPolicyConfiguration {
            request_id_draws: 0,
            announcement_tries: 3,
        }],
    )
    .expect("test seam writes the malformed policy row");
}

fn request_policy_rows(store_path: &Path) -> Vec<StoredMetaRequestPolicyConfiguration> {
    let descriptor = StoredMetaRequestPolicyConfiguration::descriptor();
    let mut engine = Engine::open(
        EngineOpen::new(store_path, SchemaVersion::new(FLOW_SCHEMA_VERSION))
            .with_family(&descriptor),
    )
    .expect("request policy inspection opens");
    let table = engine
        .register_table(descriptor)
        .expect("request policy table registers");
    engine
        .match_records(QueryPlan::all(table))
        .expect("request policy reads")
        .records()
        .to_vec()
}

#[test]
#[ignore = "the dedicated Nix check supplies exact predecessor and current production binaries"]
fn actual_predecessor_memory_evolves_serves_and_refuses_malformed_policy_without_writing() {
    let predecessor_revision = std::env::var("FLOW_MIGRATION_PREDECESSOR_REV").unwrap();
    let current_revision = std::env::var("FLOW_MIGRATION_CURRENT_REV").unwrap();
    assert_eq!(
        predecessor_revision,
        "5a0e9fa4c2695b7e1b14a7b0b72160ac560e6d13"
    );
    assert_eq!(current_revision, env!("FLOW_MIGRATION_SOURCE_REV"));
    let predecessor = required_path("FLOW_MIGRATION_PREDECESSOR_BIN");
    let current = required_path("FLOW_MIGRATION_CURRENT_BIN");
    let home = tempfile::tempdir().expect("private home");
    let state = tempfile::tempdir().expect("private state");
    let store_path = state.path().join("flow.sema");

    let predecessor_runtime = tempfile::tempdir().expect("predecessor runtime");
    serve_once(
        &predecessor,
        home.path(),
        predecessor_runtime.path(),
        &store_path,
    );
    assert!(store_path.is_file(), "the predecessor process wrote Memory");

    let current_runtime = tempfile::tempdir().expect("current runtime");
    serve_once(&current, home.path(), current_runtime.path(), &store_path);
    let reopen_runtime = tempfile::tempdir().expect("reopen runtime");
    serve_once(&current, home.path(), reopen_runtime.path(), &store_path);

    let store = FlowStore::open_configured(&store_path).expect("evolved Memory reopens");
    let configuration = store
        .configuration_snapshot()
        .expect("configuration reads")
        .expect("predecessor seeded configuration");
    assert_eq!(configuration.request_id_draws, 64);
    assert_eq!(configuration.announcement_tries, 3);
    assert!(
        store
            .read_message_delivery_snapshots(Some(0))
            .expect("Message delivery history reads")
            .delivery_snapshot_vector
            .is_empty(),
        "an empty predecessor invents no delivery"
    );
    drop(store);

    inject_malformed_request_policy(&store_path);
    let malformed = vec![StoredMetaRequestPolicyConfiguration {
        request_id_draws: 0,
        announcement_tries: 3,
    }];
    assert_eq!(request_policy_rows(&store_path), malformed);
    let refused_runtime = tempfile::tempdir().expect("refused runtime");
    let status =
        spawn_flow(&current, home.path(), refused_runtime.path(), &store_path).wait_for_failure();
    assert_eq!(
        request_policy_rows(&store_path),
        malformed,
        "a failed staged open leaves the malformed policy row unchanged"
    );
    println!(
        "MIGRATION predecessor={predecessor_revision} current={current_revision} refused={status} actual-process old-write/current-serve/current-reopen/malformed-rollback"
    );
}
