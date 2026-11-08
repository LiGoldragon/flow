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
        topic: "migrationSender".into(),
        model: signal_flow::Model::Sol,
    }
}

fn migration_recipient() -> signal_flow::Address {
    signal_flow::Address {
        aspect: signal_flow::Aspect::Field,
        topic: "migrationRecipient".into(),
        model: signal_flow::Model::Sol,
    }
}

fn own_process() -> signal_flow::Process {
    let stat = std::fs::read_to_string("/proc/self/stat").expect("own process stat reads");
    let (_, fields) = stat.rsplit_once(") ").expect("own process stat fields");
    signal_flow::Process {
        pid: i64::from(std::process::id()),
        started: fields
            .split_whitespace()
            .nth(19)
            .and_then(|field| field.parse().ok())
            .expect("own process start time reads"),
    }
}

fn socket_query(socket: &Path, query: signal_flow::Query) -> signal_flow::Response {
    let mut peer = UnixStream::connect(socket).expect("current Flow socket connects");
    peer.write_query(&query).expect("socket query writes");
    peer.read_response().expect("socket response reads")
}

fn bind_socket(socket: &Path, address: signal_flow::Address) {
    assert!(matches!(
        socket_query(
            socket,
            signal_flow::Query::Bind(signal_flow::Bind_Data {
                address,
                process: own_process(),
            }),
        ),
        signal_flow::Response::Bound(_)
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
                signal_flow::Query::Send(signal_flow::SendRequest {
                    recipient: migration_recipient(),
                    sendable_message: signal_flow::SendableMessage::Soft(
                        signal_flow::SendableRequest::Order("socket order".into()),
                    ),
                }),
            );
            let signal_flow::Response::SendAccepted(order) = order else {
                panic!("socket Order is admitted: {order:?}")
            };
            let task = socket_query(
                &socket,
                signal_flow::Query::Send(signal_flow::SendRequest {
                    recipient: migration_recipient(),
                    sendable_message: signal_flow::SendableMessage::Soft(
                        signal_flow::SendableRequest::Task(signal_flow::AnswerableTask {
                            minutes: 5,
                            string: "socket task".into(),
                        }),
                    ),
                }),
            );
            let signal_flow::Response::SendAccepted(task) = task else {
                panic!("socket Task is admitted: {task:?}")
            };
            assert!(matches!(
                socket_query(
                    &socket,
                    signal_flow::Query::Send(signal_flow::SendRequest {
                        recipient: migration_recipient(),
                        sendable_message: signal_flow::SendableMessage::Soft(
                            signal_flow::SendableRequest::Notice("socket notice".into()),
                        ),
                    }),
                ),
                signal_flow::Response::SendAccepted(_)
            ));
            let archived: flow_ethos::RequestId =
                std::env::var("FLOW_MIGRATION_ARCHIVE_REQUEST").unwrap();
            assert_eq!(
                socket_query(
                    &socket,
                    signal_flow::Query::ArchiveRequest(archived.clone()),
                ),
                signal_flow::Response::RequestArchived(archived)
            );
            println!("SOCKET_ORDER={}", order.request_id);
            println!("SOCKET_TASK={}", task.request_id);
        }
        "recipient" => {
            bind_socket(&socket, migration_recipient());
            let order: flow_ethos::RequestId = std::env::var("FLOW_MIGRATION_ORDER").unwrap();
            let task: flow_ethos::RequestId = std::env::var("FLOW_MIGRATION_TASK").unwrap();
            assert!(matches!(
                socket_query(
                    &socket,
                    signal_flow::Query::Send(signal_flow::SendRequest {
                        recipient: migration_sender(),
                        sendable_message: signal_flow::SendableMessage::Soft(
                            signal_flow::SendableRequest::Result(
                                signal_flow::SendableRequest_Result_Data {
                                    request_id: order,
                                    string: "socket result".into(),
                                },
                            ),
                        ),
                    }),
                ),
                signal_flow::Response::SendAccepted(_)
            ));
            assert!(matches!(
                socket_query(
                    &socket,
                    signal_flow::Query::Send(signal_flow::SendRequest {
                        recipient: migration_sender(),
                        sendable_message: signal_flow::SendableMessage::Soft(
                            signal_flow::SendableRequest::Done(
                                signal_flow::SendableRequest_Done_Data {
                                    request_id: task,
                                    string: "socket done".into(),
                                },
                            ),
                        ),
                    }),
                ),
                signal_flow::Response::SendAccepted(_)
            ));
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
        .find(|binding| binding.logical_delivery_id == logical_id)
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
    assert_eq!(entry.logical_delivery_id, expected_logical_id);
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
        if delivered_logical_id == expected_logical_id {
            return;
        }
    }
    panic!("the expected migrated request is within the bounded delivery prefix")
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

fn spawn_configuration_only(binary: &Path, home: &Path, runtime: &Path, store: &Path) -> FlowChild {
    let child = Command::new(binary)
        .env_clear()
        .env("HOME", home)
        .env("XDG_RUNTIME_DIR", runtime)
        .arg("--configure-only")
        .arg(start_argument(runtime, store))
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("configuration-only Flow starts");
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
        "f7d2336a7380206728cbd5aef2ebb3656d0cfd87"
    );
    #[allow(clippy::manual_unwrap_or)]
    let stamped_revision = match option_env!("FLOW_MIGRATION_SOURCE_REV") {
        Some(revision) => revision,
        None => "unstamped",
    };
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
    serve_once(
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
    let configuration = store
        .configuration_snapshot()
        .expect("configuration reads")
        .expect("predecessor seeded configuration");
    assert_eq!(configuration.request_id_draws, 64);
    assert_eq!(configuration.announcement_tries, 3);
    let sender = signal_flow::Address {
        aspect: signal_flow::Aspect::Mind,
        topic: "migrationSender".into(),
        model: signal_flow::Model::Sol,
    };
    let recipient = signal_flow::Address {
        aspect: signal_flow::Aspect::Field,
        topic: "migrationRecipient".into(),
        model: signal_flow::Model::Sol,
    };
    let singleton_recipient = signal_flow::Address {
        aspect: signal_flow::Aspect::Field,
        topic: "migrationSingletonRecipient".into(),
        model: signal_flow::Model::Sol,
    };
    let singleton = request_binding_for(&store, &ids["uncertain_order"]);
    assert_eq!(
        singleton.closure,
        RequestClosure::Answered(ids["historical_result"].clone()),
        "the singleton historical Result closes its uncertain Order"
    );
    let before_duplicate = std::fs::read(&store_path).expect("Memory reads before duplicate");
    assert_eq!(
        store
            .resolve_sendable_message(
                &singleton_recipient,
                &sender,
                signal_flow::SendableMessage::Soft(signal_flow::SendableRequest::Result(
                    signal_flow::SendableRequest_Result_Data {
                        request_id: singleton.request_id.clone(),
                        string: "duplicate".into(),
                    },
                )),
            )
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
        heartbeat[0].outstanding.as_deref(),
        Some(ids["heartbeat"].as_str())
    );
    assert!(
        store
            .delivery_purposes()
            .expect("purposes read")
            .iter()
            .any(|row| row.logical_delivery_id == ids["heartbeat"]
                && row.purpose == crate::heartbeat::DeliveryPurpose::Heartbeat)
    );
    store
        .transition_logical_delivery(
            &ids["heartbeat"],
            signal_flow::DeliveryState::Failed(signal_flow::Refusal::Direct(
                "migration fixture completes the moved heartbeat".into(),
            )),
        )
        .expect("moved heartbeat becomes terminal before the current tick");
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
    let resolved_result = store
        .resolve_sendable_message(
            &recipient,
            &sender,
            signal_flow::SendableMessage::Soft(signal_flow::SendableRequest::Result(
                signal_flow::SendableRequest_Result_Data {
                    request_id: open_order.request_id.clone(),
                    string: "current result".into(),
                },
            )),
        )
        .expect("current Result resolution reads")
        .expect("current Result resolves");
    let signal_flow::PrivateSendResponse::SendAccepted(result) = store
        .accept_logical_delivery(
            &sender,
            flow_ethos::SentBy::Sender(recipient.clone()),
            flow_ethos::Delivery::Queue,
            resolved_result,
            None,
        )
        .expect("current Result admits")
    else {
        panic!("current Result accepted")
    };
    let open_task = request_binding_for(&store, &ids["open_task"]);
    deliver_oldest_migrated(&store, &recipient, &ids["open_task"], "current-task");
    let resolved_done = store
        .resolve_sendable_message(
            &recipient,
            &sender,
            signal_flow::SendableMessage::Soft(signal_flow::SendableRequest::Done(
                signal_flow::SendableRequest_Done_Data {
                    request_id: open_task.request_id.clone(),
                    string: "current done".into(),
                },
            )),
        )
        .expect("current Done resolution reads")
        .expect("current Done resolves");
    let signal_flow::PrivateSendResponse::SendAccepted(done) = store
        .accept_logical_delivery(
            &sender,
            flow_ethos::SentBy::Sender(recipient.clone()),
            flow_ethos::Delivery::Queue,
            resolved_done,
            None,
        )
        .expect("current Done admits")
    else {
        panic!("current Done accepted")
    };
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
    deliver_through_migrated(
        &store,
        &sender,
        &result.logical_delivery_id,
        "current-result",
    );
    deliver_through_migrated(&store, &sender, &done.logical_delivery_id, "current-done");
    let signal_flow::PrivateSendResponse::SendAccepted(notice) = store
        .accept_logical_delivery(
            &recipient,
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
        .read_logical_delivery_snapshots(&sender, Some(notice.delivery_cursor))
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
    let served = run_socket_client(
        &current_socket,
        "sender",
        &[(
            "FLOW_MIGRATION_ARCHIVE_REQUEST",
            open_order.request_id.as_str(),
        )],
    );
    run_socket_client(
        &current_socket,
        "recipient",
        &[
            ("FLOW_MIGRATION_ORDER", served["SOCKET_ORDER"].as_str()),
            ("FLOW_MIGRATION_TASK", served["SOCKET_TASK"].as_str()),
        ],
    );
    std::thread::sleep(Duration::from_millis(1_750));
    current_process.stop();
    let reopened = FlowStore::open_configured(&store_path).expect("operated Memory reopens");
    assert!(
        reopened
            .request_binding(&open_order.request_id)
            .unwrap()
            .is_none()
    );
    assert!(
        reopened
            .logical_delivery_snapshot(&result.logical_delivery_id)
            .unwrap()
            .is_some()
    );
    assert!(
        reopened
            .logical_delivery_snapshot(&done.logical_delivery_id)
            .unwrap()
            .is_some()
    );
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
    assert_eq!(heartbeat.baseline_sent, 6, "the moved baseline is retained");
    let ordinary_sent = reopened
        .delivery_purposes()
        .expect("purposes reopen")
        .into_iter()
        .filter(|row| {
            row.address == singleton_recipient
                && row.purpose == crate::heartbeat::DeliveryPurpose::Ordinary
        })
        .count() as i64;
    assert_eq!(heartbeat.latest_sent, ordinary_sent);
    drop(reopened);

    inject_malformed_request_policy(&store_path);
    let malformed = vec![StoredMetaRequestPolicyConfiguration {
        request_id_draws: 0,
        announcement_tries: 3,
    }];
    assert_eq!(request_policy_rows(&store_path), malformed);
    let refused_runtime = tempfile::tempdir().expect("refused runtime");
    let status =
        spawn_configuration_only(&current, home.path(), refused_runtime.path(), &store_path)
            .wait_for_failure();
    assert_eq!(
        request_policy_rows(&store_path),
        malformed,
        "a failed staged open leaves the malformed policy row unchanged"
    );
    println!(
        "MIGRATION predecessor={predecessor_revision} current={current_revision} refused={status} actual-process old-write/current-serve/current-reopen/malformed-rollback"
    );
}
