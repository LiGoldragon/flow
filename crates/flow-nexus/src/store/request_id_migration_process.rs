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
    StoredMetaRequestPolicyConfiguration, TracksHeartbeat,
};
use crate::CarriesSignalFrames;
use crate::store::logical_delivery::{MintsRequestIds, RequestClosure, RequestIdBinding};
use crate::store::metaflow::SleepsMetaflows;
use crate::store::queue_delivery::{
    AccountsQueueDelivery, NativeDeliveryReceipt, NativeHistoryAnchor, PrepareQueueDelivery,
};
use sema_engine::{Engine, EngineOpen, QueryPlan, SchemaVersion};
use std::{
    os::unix::net::UnixStream,
    path::{Path, PathBuf},
    process::{Child, Command, ExitStatus, Stdio},
    time::{Duration, Instant},
};

fn migration_sender() -> signal_flow::Address {
    signal_flow::Address {
        aspect: signal_flow::Aspect::Mind,
        topic: flow_ethos::Topic("migrationSender".into()),
        model: signal_flow::Model::Sol,
    }
}

fn migration_recipient() -> signal_flow::Address {
    signal_flow::Address {
        aspect: signal_flow::Aspect::Field,
        topic: flow_ethos::Topic("migrationRecipient".into()),
        model: signal_flow::Model::Sol,
    }
}

fn migration_message() -> signal_flow::Address {
    signal_flow::Address {
        aspect: signal_flow::Aspect::Field,
        topic: flow_ethos::Topic("message".into()),
        model: signal_flow::Model::Astra,
    }
}

fn own_process() -> signal_flow::Process {
    let stat = std::fs::read_to_string("/proc/self/stat").expect("own process stat reads");
    let (_, fields) = stat.rsplit_once(") ").expect("own process stat fields");
    signal_flow::Process {
        pid: flow_ethos::Pid(i64::from(std::process::id())),
        started: flow_ethos::Started(
            fields
                .split_whitespace()
                .nth(19)
                .and_then(|field| field.parse().ok())
                .expect("own process start time reads"),
        ),
    }
}

fn socket_query(socket: &Path, query: signal_flow::Input) -> signal_flow::Output {
    let mut peer = UnixStream::connect(socket).expect("current Flow socket connects");
    // Every ordinary connection greets the current Flow first.
    {
        use signal_flow::Contracted;
        peer.write_frame(
            &rkyv::to_bytes::<rkyv::rancor::Error>(&signal_flow::Input::greeting())
                .expect("greeting encodes"),
        )
        .expect("greeting writes");
        let receipt = rkyv::from_bytes::<signal_flow::HandshakeReceipt, rkyv::rancor::Error>(
            &peer.read_frame().expect("greeting receipt reads"),
        )
        .expect("receipt decodes");
        assert_eq!(
            receipt,
            signal_flow::HandshakeReceipt::Greeted(signal_flow::Input::contract_digest())
        );
    }
    peer.write_query(&query).expect("socket query writes");
    peer.read_response().expect("socket response reads")
}

fn bind_socket(socket: &Path, address: signal_flow::Address) {
    assert!(matches!(
        socket_query(
            socket,
            signal_flow::Input::Bind(signal_flow::Bind_Data {
                address,
                process: own_process(),
            }),
        ),
        signal_flow::Output::Bound(_)
    ));
}

#[test]
#[ignore = "the migration process test starts this authenticated socket client"]
fn current_socket_client_serves_request_operations() {
    let socket = PathBuf::from(std::env::var_os("FLOW_MIGRATION_SOCKET").unwrap());
    match std::env::var("FLOW_MIGRATION_CLIENT_MODE")
        .unwrap()
        .as_str()
    {
        "sender" => {
            bind_socket(&socket, migration_sender());
            let order = socket_query(
                &socket,
                signal_flow::Input::Send(signal_flow::SendRequest {
                    address: migration_recipient(),
                    sendable_message: signal_flow::SendableMessage::Soft(
                        signal_flow::SendableRequest::Order("socket order".into()),
                    ),
                }),
            );
            let signal_flow::Output::SendAccepted(order) = order else {
                panic!("socket Order is admitted: {order:?}")
            };
            let task = socket_query(
                &socket,
                signal_flow::Input::Send(signal_flow::SendRequest {
                    address: migration_recipient(),
                    sendable_message: signal_flow::SendableMessage::Soft(
                        signal_flow::SendableRequest::Task(signal_flow::AnswerableTask {
                            minutes: flow_ethos::Minutes(5),
                            string: "socket task".into(),
                        }),
                    ),
                }),
            );
            let signal_flow::Output::SendAccepted(task) = task else {
                panic!("socket Task is admitted: {task:?}")
            };
            assert!(matches!(
                socket_query(
                    &socket,
                    signal_flow::Input::Send(signal_flow::SendRequest {
                        address: migration_recipient(),
                        sendable_message: signal_flow::SendableMessage::Soft(
                            signal_flow::SendableRequest::Notice("socket notice".into()),
                        ),
                    }),
                ),
                signal_flow::Output::SendAccepted(_)
            ));
            println!("SOCKET_ORDER={}", order.request_id.0);
            println!("SOCKET_TASK={}", task.request_id.0);
        }
        "recipient" => {
            bind_socket(&socket, migration_recipient());
            let order: flow_ethos::RequestId =
                flow_ethos::RequestId(std::env::var("FLOW_MIGRATION_ORDER").unwrap());
            let task: flow_ethos::RequestId =
                flow_ethos::RequestId(std::env::var("FLOW_MIGRATION_TASK").unwrap());
            let result = socket_query(
                &socket,
                signal_flow::Input::Send(signal_flow::SendRequest {
                    address: migration_sender(),
                    sendable_message: signal_flow::SendableMessage::Soft(
                        signal_flow::SendableRequest::Result(
                            signal_flow::SendableRequest_Result_Data {
                                request_id: order,
                                string: "socket result".into(),
                            },
                        ),
                    ),
                }),
            );
            let signal_flow::Output::SendAccepted(result) = result else {
                panic!("socket Result is admitted: {result:?}")
            };
            let done = socket_query(
                &socket,
                signal_flow::Input::Send(signal_flow::SendRequest {
                    address: migration_sender(),
                    sendable_message: signal_flow::SendableMessage::Soft(
                        signal_flow::SendableRequest::Done(
                            signal_flow::SendableRequest_Done_Data {
                                request_id: task,
                                string: "socket done".into(),
                            },
                        ),
                    ),
                }),
            );
            let signal_flow::Output::SendAccepted(done) = done else {
                panic!("socket Done is admitted: {done:?}")
            };
            println!("SOCKET_RESULT={}", result.request_id.0);
            println!("SOCKET_DONE={}", done.request_id.0);
        }
        "archive" => {
            bind_socket(&socket, migration_sender());
            let archived: flow_ethos::RequestId =
                flow_ethos::RequestId(std::env::var("FLOW_MIGRATION_ARCHIVE_REQUEST").unwrap());
            assert_eq!(
                socket_query(
                    &socket,
                    signal_flow::Input::ArchiveRequest(archived.clone()),
                ),
                signal_flow::Output::RequestArchived(archived)
            );
        }
        // Another than the request's sender archives it: any caller may.
        "archive-foreign" => {
            bind_socket(&socket, migration_recipient());
            let archived: flow_ethos::RequestId =
                flow_ethos::RequestId(std::env::var("FLOW_MIGRATION_ARCHIVE_REQUEST").unwrap());
            assert_eq!(
                socket_query(
                    &socket,
                    signal_flow::Input::ArchiveRequest(archived.clone())
                ),
                signal_flow::Output::RequestArchived(archived)
            );
        }
        // Once archived, the id names no request, to a caller in no flow as
        // to any other.
        "archive-unbound" => {
            let archived = std::env::var("FLOW_MIGRATION_ARCHIVE_REQUEST").unwrap();
            assert_eq!(
                socket_query(
                    &socket,
                    signal_flow::Input::ArchiveRequest(flow_ethos::RequestId(archived))
                ),
                signal_flow::Output::ArchiveRejected(signal_flow::ArchiveRejection::UnknownRequest)
            );
        }
        "archive-message" => {
            bind_socket(&socket, migration_message());
            let archived = std::env::var("FLOW_MIGRATION_ARCHIVE_REQUEST").unwrap();
            assert_eq!(
                socket_query(
                    &socket,
                    signal_flow::Input::ArchiveRequest(flow_ethos::RequestId(archived))
                ),
                signal_flow::Output::ArchiveRejected(signal_flow::ArchiveRejection::UnknownRequest)
            );
        }
        "archive-unknown" => {
            bind_socket(&socket, migration_sender());
            assert_eq!(
                socket_query(
                    &socket,
                    signal_flow::Input::ArchiveRequest(flow_ethos::RequestId(
                        "unknown-request-id".into()
                    )),
                ),
                signal_flow::Output::ArchiveRejected(signal_flow::ArchiveRejection::UnknownRequest)
            );
        }
        // A request Flow raised itself is archived by anyone once it is
        // settled; still queued, it is answered by that state.
        "archive-flow-owned" => {
            bind_socket(&socket, migration_sender());
            let archived = std::env::var("FLOW_MIGRATION_ARCHIVE_REQUEST").unwrap();
            assert_eq!(
                socket_query(
                    &socket,
                    signal_flow::Input::ArchiveRequest(flow_ethos::RequestId(archived))
                ),
                signal_flow::Output::ArchiveRejected(signal_flow::ArchiveRejection::NotYet)
            );
        }
        mode => panic!("unknown migration client mode {mode}"),
    }
}

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

fn fixture_ids(path: &Path) -> std::collections::BTreeMap<String, String> {
    std::fs::read_to_string(path)
        .expect("predecessor fixture ids read")
        .lines()
        .map(|line| {
            let (name, value) = line.split_once('=').expect("fixture id has a name");
            (name.to_owned(), value.to_owned())
        })
        .collect()
}

fn request_binding_for(store: &FlowStore, logical_id: &str) -> RequestIdBinding {
    let rows = store
        .engine
        .match_records(QueryPlan::all(store.logical_deliveries.request_ids))
        .expect("request bindings read");
    rows.records()
        .iter()
        .find(|binding| binding.logical_delivery_id.0 == logical_id)
        .cloned()
        .unwrap_or_else(|| panic!("request binding for {logical_id}"))
}

fn deliver_oldest_migrated(
    store: &FlowStore,
    address: &signal_flow::Address,
    expected_logical_id: &str,
    label: &str,
) {
    let PrepareQueueDelivery::Prepared(attempt) = store
        .prepare_oldest_delivery(
            address,
            "current-delivery-flow",
            &format!("{label}-thread"),
            signal_flow::HarnessKind::Codex,
            NativeHistoryAnchor::CodexTurns(None),
        )
        .expect("migrated delivery prepares")
    else {
        panic!("the expected migrated request is the oldest prepared delivery")
    };
    let meta_signal_flow::QueueEvidence::Logical(entry) = &attempt.queue_evidence else {
        panic!("the migrated request retains its logical queue evidence")
    };
    assert_eq!(entry.logical_delivery_id.0, expected_logical_id);
    assert!(
        store
            .record_delivery_receipt(
                &attempt.attempt_id,
                NativeDeliveryReceipt {
                    native_request_id: attempt.native_request_id,
                    thread_id: attempt.thread_id.clone(),
                    turn_id: format!("{label}-turn"),
                    native_receipt: format!(
                        r#"{{"id":{},"result":{{}}}}"#,
                        attempt.native_request_id
                    ),
                },
            )
            .expect("migrated delivery receipt records")
    );
    assert!(
        store
            .settle_recorded_delivery(&attempt.attempt_id)
            .expect("migrated delivered request leaves its queue")
    );
}

fn deliver_through_migrated(
    store: &FlowStore,
    address: &signal_flow::Address,
    expected_logical_id: &str,
    label: &str,
) {
    for ordinal in 0..32 {
        let PrepareQueueDelivery::Prepared(attempt) = store
            .prepare_oldest_delivery(
                address,
                "current-delivery-flow",
                &format!("{label}-{ordinal}-thread"),
                signal_flow::HarnessKind::Codex,
                NativeHistoryAnchor::CodexTurns(None),
            )
            .expect("migrated delivery prefix prepares")
        else {
            panic!("the expected migrated request remains in the delivery prefix")
        };
        let meta_signal_flow::QueueEvidence::Logical(entry) = &attempt.queue_evidence else {
            panic!("the migrated request retains its logical queue evidence")
        };
        let delivered_logical_id = entry.logical_delivery_id.clone();
        assert!(
            store
                .record_delivery_receipt(
                    &attempt.attempt_id,
                    NativeDeliveryReceipt {
                        native_request_id: attempt.native_request_id,
                        thread_id: attempt.thread_id.clone(),
                        turn_id: format!("{label}-{ordinal}-turn"),
                        native_receipt: format!(
                            r#"{{"id":{},"result":{{}}}}"#,
                            attempt.native_request_id
                        ),
                    },
                )
                .expect("migrated delivery prefix receipt records")
        );
        assert!(
            store
                .settle_recorded_delivery(&attempt.attempt_id)
                .expect("migrated delivered prefix leaves its queue")
        );
        if delivered_logical_id.0 == expected_logical_id {
            return;
        }
    }
    panic!("the expected migrated request is within the bounded delivery prefix")
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

fn serve_once(binary: &Path, home: &Path, runtime: &Path, store: &Path) {
    let child = spawn_flow(binary, home, runtime, store);
    wait_for_socket(&runtime.join("flow.sock"));
    child.stop();
}

fn serve_predecessor_once(binary: &Path, home: &Path, runtime: &Path, store: &Path) {
    let child = spawn_predecessor(binary, home, runtime, store);
    wait_for_socket(&runtime.join("flow.sock"));
    child.stop();
}

fn run_socket_client(
    socket: &Path,
    mode: &str,
    environment: &[(&str, &str)],
) -> std::collections::BTreeMap<String, String> {
    let mut command = Command::new(std::env::current_exe().expect("test executable resolves"));
    command
        .arg("--exact")
        .arg("store::request_id_migration_process::current_socket_client_serves_request_operations")
        .arg("--ignored")
        .arg("--nocapture")
        .env("FLOW_MIGRATION_SOCKET", socket)
        .env("FLOW_MIGRATION_CLIENT_MODE", mode);
    for (name, value) in environment {
        command.env(name, value);
    }
    let output = command.output().expect("socket client runs");
    assert!(
        output.status.success(),
        "socket client {mode} failed: stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
    String::from_utf8(output.stdout)
        .expect("socket client output is text")
        .lines()
        .filter_map(|line| line.split_once('='))
        .map(|(name, value)| (name.to_owned(), value.to_owned()))
        .collect()
}

fn inject_malformed_request_policy(store_path: &Path) {
    let store = FlowStore::open_configured(store_path).expect("current Memory opens before fault");
    FlowStore::replace_configuration_family(
        &store.engine,
        store.meta_configuration_tables.request_policy,
        vec![StoredMetaRequestPolicyConfiguration {
            draw_bound: meta_signal_flow::DrawBound(0),
            try_bound: meta_signal_flow::TryBound(3),
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
        "f7d2336a7380206728cbd5aef2ebb3656d0cfd87"
    );
    let stamped_revision =
        std::env::var("FLOW_MIGRATION_SOURCE_REV").unwrap_or_else(|_| "unstamped".to_owned());
    assert_eq!(current_revision, stamped_revision);
    let predecessor = required_path("FLOW_MIGRATION_PREDECESSOR_BIN");
    let predecessor_fixture = required_directory("FLOW_MIGRATION_PREDECESSOR_STORE");
    let current = required_path("FLOW_MIGRATION_CURRENT_BIN");
    let home = tempfile::tempdir().expect("private home");
    let state = tempfile::tempdir().expect("private state");
    let store_path = state.path().join("flow.sema");

    // Bind the unmodified accepted executable too: it creates and serves its
    // own Memory. The nonempty fixture below is authored by a test-only
    // module compiled inside the same exact accepted source and using its
    // production FlowStore capabilities.
    let exact_binary_state = tempfile::tempdir().expect("exact predecessor state");
    let predecessor_runtime = tempfile::tempdir().expect("predecessor runtime");
    serve_predecessor_once(
        &predecessor,
        home.path(),
        predecessor_runtime.path(),
        &exact_binary_state.path().join("flow.sema"),
    );
    assert!(
        exact_binary_state.path().join("flow.sema").is_file(),
        "the exact predecessor process wrote Memory"
    );
    std::fs::copy(predecessor_fixture.join("flow.sema"), &store_path)
        .expect("the exact-source nonempty predecessor Memory copies");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&store_path, std::fs::Permissions::from_mode(0o600))
            .expect("the private copied Memory is writable by the migration process");
    }
    let ids = fixture_ids(&predecessor_fixture.join("fixture.ids"));
    assert!(store_path.is_file(), "the predecessor fixture wrote Memory");

    let current_runtime = tempfile::tempdir().expect("current runtime");
    serve_once(&current, home.path(), current_runtime.path(), &store_path);
    let reopen_runtime = tempfile::tempdir().expect("reopen runtime");
    serve_once(&current, home.path(), reopen_runtime.path(), &store_path);

    let store = FlowStore::open_configured(&store_path).expect("evolved Memory reopens");
    use super::events::RecordsHarnessEvents;
    let events = store
        .events("MiXeD-v4-key")
        .expect("upgraded events read")
        .expect("original key held");
    assert_eq!(
        events,
        vec![
            super::events::RecordedHarnessEvent::Started(Some(signal_flow::NativeSessionId(
                "v4-session".into()
            ))),
            super::events::RecordedHarnessEvent::Stopped(super::events::RecordedStop {
                native_session_id_option: Some(signal_flow::NativeSessionId("v4-session".into())),
                transcript_path_option: Some(signal_flow::TranscriptPath("/t/v4-session".into())),
                background_work: signal_flow::BackgroundWork(vec![]),
            }),
            super::events::RecordedHarnessEvent::ToolUsed("Bash".into()),
        ]
    );
    let row = store
        .engine
        .match_records(QueryPlan::key(
            store.event_tables.events,
            sema_engine::RecordKey::new("MiXeD-v4-key"),
        ))
        .unwrap()
        .records()[0]
        .clone();
    assert_eq!(row.flow_id.0, "MiXeD-v4-key");
    let archived = rkyv::to_bytes::<rkyv::rancor::Error>(&row).unwrap();
    assert!(
        !archived
            .windows("V4-ASSISTANT-MARKER-雪".len())
            .any(|part| part == "V4-ASSISTANT-MARKER-雪".as_bytes())
    );
    let configuration = store
        .configuration_snapshot()
        .expect("configuration reads")
        .expect("predecessor seeded configuration");
    assert_eq!(
        configuration.configured_request_id_draws,
        meta_signal_flow::ConfiguredRequestIdDraws(meta_signal_flow::DrawBound(64))
    );
    assert_eq!(
        configuration.configured_announcement_tries,
        meta_signal_flow::ConfiguredAnnouncementTries(meta_signal_flow::TryBound(3))
    );
    let sender = signal_flow::Address {
        aspect: signal_flow::Aspect::Mind,
        topic: flow_ethos::Topic("migrationSender".into()),
        model: signal_flow::Model::Sol,
    };
    let recipient = signal_flow::Address {
        aspect: signal_flow::Aspect::Field,
        topic: flow_ethos::Topic("migrationRecipient".into()),
        model: signal_flow::Model::Sol,
    };
    let singleton_recipient = signal_flow::Address {
        aspect: signal_flow::Aspect::Field,
        topic: flow_ethos::Topic("migrationSingletonRecipient".into()),
        model: signal_flow::Model::Sol,
    };
    let singleton = request_binding_for(&store, &ids["uncertain_order"]);
    assert_eq!(
        singleton.closure,
        RequestClosure::Answered(flow_ethos::LogicalDeliveryId(
            ids["historical_result"].clone()
        )),
        "the singleton historical Result closes its uncertain Order"
    );
    let before_duplicate = std::fs::read(&store_path).expect("Memory reads before duplicate");
    assert_eq!(
        store
            .resolve_sendable_message(signal_flow::SendableMessage::Soft(
                signal_flow::SendableRequest::Result(signal_flow::SendableRequest_Result_Data {
                    request_id: singleton.request_id.clone(),
                    string: "duplicate".into(),
                },)
            ),)
            .expect("duplicate historical Result reads"),
        Err(signal_flow::Refusal::Unresolved)
    );
    assert_eq!(
        std::fs::read(&store_path).expect("Memory reads after duplicate"),
        before_duplicate,
        "the duplicate historical Result writes nothing"
    );
    let heartbeat = store.heartbeat_standings().expect("heartbeat rows read");
    assert_eq!(heartbeat.len(), 1);
    assert_eq!(heartbeat[0].address, singleton_recipient);
    assert_eq!(heartbeat[0].baseline_sent, 6);
    assert_eq!(heartbeat[0].latest_sent, 6);
    assert_eq!(
        heartbeat[0].outstanding, None,
        "upgrade atomically clears a terminal predecessor heartbeat"
    );
    assert!(
        store
            .logical_delivery_snapshot(&flow_ethos::LogicalDeliveryId(ids["heartbeat"].clone()))
            .expect("current heartbeat index reads")
            .is_none(),
        "the terminal predecessor heartbeat is absent from current delivery indexes"
    );
    let prior = store
        .engine
        .match_records(QueryPlan::all(store.request_id_upgrade.prior_deliveries))
        .expect("prior deliveries read");
    assert!(
        prior
            .records()
            .iter()
            .any(|row| row.logical_delivery_id == ids["heartbeat"]),
        "the terminal predecessor heartbeat moves only to PriorDelivery"
    );
    assert!(matches!(
        store
            .simplified_reply_receipt("open-simplified")
            .expect("open simplified reply reads"),
        super::logical_delivery::SimplifiedReplyStanding::Unreceived(_)
    ));
    assert!(matches!(
        store
            .simplified_reply_receipt("completed-simplified")
            .expect("completed simplified reply reads"),
        super::logical_delivery::SimplifiedReplyStanding::NoneOwed
    ));

    assert!(
        store
            .awaken_metaflow(&recipient, "current-delivery-flow", 0)
            .expect("migrated recipient awakens for current serving")
    );
    let open_order = request_binding_for(&store, &ids["open_order"]);
    deliver_oldest_migrated(&store, &recipient, &ids["open_order"], "current-order");
    let open_task = request_binding_for(&store, &ids["open_task"]);
    deliver_oldest_migrated(&store, &recipient, &ids["open_task"], "current-task");
    assert!(
        store
            .awaken_metaflow(&sender, "current-delivery-flow", 0)
            .expect("migrated sender awakens for current serving")
    );
    deliver_through_migrated(
        &store,
        &sender,
        &ids["open_reply"],
        "current-open-reply-input",
    );
    deliver_through_migrated(
        &store,
        &sender,
        &ids["completed_reply"],
        "current-completed-reply-input",
    );
    let signal_flow::PrivateSendResponse::SendAccepted(notice) = store
        .accept_logical_delivery(
            &sender,
            flow_ethos::SentBy::Sender(sender.clone()),
            flow_ethos::Delivery::Queue,
            flow_ethos::Message::Soft(flow_ethos::Request::Notice("current send".into())),
            None,
        )
        .expect("current Send admits")
    else {
        panic!("current Send accepted")
    };
    let after_acceptance = store
        .read_logical_delivery_snapshots(&sender, Some(notice.delivery_cursor.0))
        .expect("cursor resume reads");
    assert!(after_acceptance.delivery_cursor >= notice.delivery_cursor);
    drop(store);

    let post_operation_runtime = tempfile::tempdir().expect("post-operation runtime");
    let current_process = spawn_flow(
        &current,
        home.path(),
        post_operation_runtime.path(),
        &store_path,
    );
    let current_socket = post_operation_runtime.path().join("flow.sock");
    wait_for_socket(&current_socket);
    let _served = run_socket_client(&current_socket, "sender", &[]);
    let answered = run_socket_client(
        &current_socket,
        "recipient",
        &[
            ("FLOW_MIGRATION_ORDER", open_order.request_id.0.as_str()),
            ("FLOW_MIGRATION_TASK", open_task.request_id.0.as_str()),
        ],
    );
    current_process.stop();
    let between_processes = FlowStore::open_configured(&store_path)
        .expect("socket answers reopen before archive serving");
    assert!(
        between_processes
            .awaken_metaflow(&sender, "current-delivery-flow", 0)
            .expect("socket answer target awakens for settlement")
    );
    for (name, label) in [
        ("SOCKET_RESULT", "socket-result"),
        ("SOCKET_DONE", "socket-done"),
    ] {
        let binding = between_processes
            .request_binding(&flow_ethos::RequestId(answered[name].clone()))
            .unwrap()
            .expect("socket answer retains its request binding");
        deliver_through_migrated(
            &between_processes,
            &sender,
            &binding.logical_delivery_id.0,
            label,
        );
    }
    let mut nexus_configuration = between_processes
        .configuration_snapshot()
        .expect("configuration reads for Message peer negative")
        .expect("configuration remains complete")
        .configured_nexus;
    nexus_configuration.0.message_nexus_binary_option = Some(flow_ethos::MessageNexusBinary(
        std::fs::canonicalize(std::env::current_exe().expect("test executable resolves"))
            .expect("test executable canonicalizes")
            .to_string_lossy()
            .into_owned(),
    ));
    between_processes
        .configure_change(meta_signal_flow::ConfigurationChange::Nexus(
            nexus_configuration.0,
        ))
        .expect("test Message peer executable configures");
    drop(between_processes);
    let archive_runtime = tempfile::tempdir().expect("archive runtime");
    let current_process = spawn_flow(&current, home.path(), archive_runtime.path(), &store_path);
    let current_socket = archive_runtime.path().join("flow.sock");
    wait_for_socket(&current_socket);
    for mode in [
        "archive-unknown",
        "archive-foreign",
        "archive-unbound",
        "archive-message",
    ] {
        run_socket_client(
            &current_socket,
            mode,
            &[(
                "FLOW_MIGRATION_ARCHIVE_REQUEST",
                open_order.request_id.0.as_str(),
            )],
        );
    }
    // The heartbeat the current process admits at its first beat (one
    // second) has no event outside that process: it is a Flow-admitted
    // delivery, and ObserveDeliveries carries Message-admitted ones only, so
    // the beat is given one bounded wait.
    std::thread::sleep(Duration::from_millis(1_750));
    current_process.stop();
    let reopened = FlowStore::open_configured(&store_path).expect("operated Memory reopens");
    assert!(
        reopened
            .request_binding(&open_order.request_id)
            .unwrap()
            .is_none()
    );
    for answered_request in [&answered["SOCKET_RESULT"], &answered["SOCKET_DONE"]] {
        let binding = reopened
            .request_binding(&flow_ethos::RequestId(answered_request.to_owned()))
            .unwrap()
            .expect("socket answer retains its request binding");
        assert!(
            reopened
                .logical_delivery_snapshot(&binding.logical_delivery_id)
                .unwrap()
                .is_some()
        );
    }
    assert!(
        reopened
            .logical_delivery_snapshot(&notice.logical_delivery_id)
            .unwrap()
            .is_some()
    );
    let heartbeat = reopened
        .heartbeat_standings()
        .expect("ticked heartbeat reopens")
        .into_iter()
        .find(|standing| standing.address == singleton_recipient)
        .expect("moved heartbeat standing remains");
    let fresh_heartbeat = heartbeat
        .outstanding
        .as_deref()
        .expect("the current process admitted a fresh heartbeat");
    assert_ne!(fresh_heartbeat, ids["heartbeat"]);
    let ordinary_sent = reopened
        .delivery_purposes()
        .expect("purposes reopen")
        .into_iter()
        .filter(|row| {
            row.address == singleton_recipient
                && row.purpose == crate::heartbeat::DeliveryPurpose::Ordinary
        })
        .count() as i64;
    let fresh_request = reopened
        .retained_logical_requests()
        .expect("fresh heartbeat request reopens")
        .into_iter()
        .find(|retained| retained.entry.logical_delivery_id.0 == fresh_heartbeat)
        .expect("fresh heartbeat request is retained");
    let flow_ethos::Message::Soft(flow_ethos::Request::Notice(reminder)) =
        fresh_request.entry.message
    else {
        panic!("the fresh heartbeat is a soft Notice")
    };
    assert!(
        reminder.contains(&format!(
            "messages sent (accepted, since purpose-family deployment/start) 6→{ordinary_sent}"
        )),
        "the fresh tick compares the moved count to current evidence: {reminder}"
    );
    assert_eq!(
        heartbeat.baseline_sent, ordinary_sent,
        "admitting the fresh heartbeat advances the sample baseline"
    );
    assert_eq!(heartbeat.latest_sent, ordinary_sent);
    let fresh_heartbeat = fresh_heartbeat.to_owned();
    let fresh_heartbeat_request_id = request_binding_for(&reopened, &fresh_heartbeat).request_id;
    drop(reopened);

    let flow_owned_runtime = tempfile::tempdir().expect("Flow-owned archive runtime");
    let flow_owned_process = spawn_flow(
        &current,
        home.path(),
        flow_owned_runtime.path(),
        &store_path,
    );
    let flow_owned_socket = flow_owned_runtime.path().join("flow.sock");
    wait_for_socket(&flow_owned_socket);
    run_socket_client(
        &flow_owned_socket,
        "archive-flow-owned",
        &[(
            "FLOW_MIGRATION_ARCHIVE_REQUEST",
            fresh_heartbeat_request_id.0.as_str(),
        )],
    );
    flow_owned_process.stop();
    let after_flow_owned =
        FlowStore::open_configured(&store_path).expect("Flow-owned archive refusal reopens");
    assert!(
        after_flow_owned
            .logical_delivery_snapshot(&flow_ethos::LogicalDeliveryId(fresh_heartbeat.to_owned()))
            .expect("Flow-owned heartbeat snapshot reads")
            .is_some()
    );
    assert!(
        after_flow_owned
            .request_binding(&fresh_heartbeat_request_id)
            .expect("Flow-owned heartbeat binding reads")
            .is_some()
    );
    drop(after_flow_owned);

    inject_malformed_request_policy(&store_path);
    let malformed = vec![StoredMetaRequestPolicyConfiguration {
        draw_bound: meta_signal_flow::DrawBound(0),
        try_bound: meta_signal_flow::TryBound(3),
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
