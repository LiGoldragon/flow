//! Joint proof that the production Flow and Message processes share one durable delivery fact.

use super::{FlowStore, OpensFlowStore, RecordsLogicalDeliveries};
use crate::CarriesSignalFrames;
use message_nexus::{
    frame::FramedStream,
    generated::memory::Standard,
    request_tracking::Classification,
    store::{KeepsRequestTracking, MessageStore, OpensStore},
};
use signal_flow::{
    Address, Aspect, Bind_Data, Model, Process, Query as FlowQuery, Response as FlowResponse,
    SendRequest, SendableMessage, SendableRequest,
};
use std::{
    os::unix::net::UnixStream,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};

struct JointChild {
    child: Child,
    name: &'static str,
}

impl JointChild {
    fn stop(mut self) {
        // SAFETY: this pid belongs to the fixture child, and both Nexuses
        // document SIGTERM as their orderly stop request.
        let _ = unsafe { libc::kill(self.child.id() as i32, libc::SIGTERM) };
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if let Some(status) = self.child.try_wait().expect("fixture child status") {
                assert!(status.success(), "{} exited with {status}", self.name);
                return;
            }
            if Instant::now() >= deadline {
                let _ = self.child.kill();
                let _ = self.child.wait();
                panic!("{} did not stop in ten seconds", self.name);
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    }
}

impl Drop for JointChild {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn required_path(name: &str) -> PathBuf {
    let value = std::env::var_os(name).unwrap_or_else(|| panic!("{name} is required"));
    let path = PathBuf::from(value);
    assert_eq!(
        std::fs::canonicalize(&path).expect("joint binary canonicalizes"),
        path,
        "the joint check supplies an immutable canonical binary"
    );
    path
}

fn process() -> Process {
    let stat = std::fs::read_to_string("/proc/self/stat").expect("own stat");
    let (_, fields) = stat.rsplit_once(") ").expect("stat fields");
    let started = fields
        .split_whitespace()
        .nth(19)
        .and_then(|field| field.parse().ok())
        .expect("start time");
    Process {
        pid: i64::from(std::process::id()),
        started,
    }
}

/// Flow's place: its sockets in `<runtime>/flow`, where Message's default
/// configuration looks for Flow, and its memory in `state`.
fn place(runtime: &Path, state: &Path) -> crate::place::Place {
    crate::place::Place {
        place_name: "main".into(),
        runtime: runtime.join("flow").to_string_lossy().into_owned(),
        state: state.to_string_lossy().into_owned(),
    }
}

fn place_argument(place: &crate::place::Place) -> String {
    format!(
        "{{ «{}» «{}» «{}» }}",
        place.place_name, place.runtime, place.state
    )
}

fn spawn_flow(
    binary: &Path,
    home: &Path,
    runtime: &Path,
    place: &crate::place::Place,
) -> JointChild {
    let child = Command::new(binary)
        .env_clear()
        .env("HOME", home)
        .env("XDG_RUNTIME_DIR", runtime)
        .arg(place_argument(place))
        .stderr(Stdio::inherit())
        .spawn()
        .expect("Flow starts");
    JointChild {
        child,
        name: "Flow",
    }
}

fn spawn_message(binary: &Path, home: &Path, runtime: &Path) -> JointChild {
    let child = Command::new(binary)
        .env_clear()
        .env("HOME", home)
        .env("XDG_RUNTIME_DIR", runtime)
        .stderr(Stdio::inherit())
        .spawn()
        .expect("Message starts");
    JointChild {
        child,
        name: "Message",
    }
}

fn wait_for_socket(path: &Path) -> UnixStream {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if let Ok(peer) = UnixStream::connect(path) {
            return peer;
        }
        assert!(
            Instant::now() < deadline,
            "{} never listened",
            path.display()
        );
        std::thread::sleep(Duration::from_millis(10));
    }
}

fn endpoint(name: &str, home: &Path) -> meta_signal_flow::CodexEndpoint {
    meta_signal_flow::CodexEndpoint {
        client_path: format!("/fixture/{name}-client"),
        home: home.join(name).to_string_lossy().into_owned(),
        control_socket_path: home
            .join(name)
            .join("control.sock")
            .to_string_lossy()
            .into_owned(),
        model_name_vector: vec![format!("{name}-model")],
    }
}

/// The Flow memory declared before Flow starts, written in this process:
/// the Nexus Message is admitted as, and the Curriculum socket.
fn configure_flow(message_binary: &Path, home: &Path, runtime: &Path, place: &crate::place::Place) {
    use crate::place::LaysOutPlace;
    use crate::store::{ConfigurationOnlyStore, ConfiguresOnlyStore};
    // The place's directories, declared as the environment declares them:
    // Flow finds them and makes neither.
    std::fs::create_dir_all(place.state_directory()).expect("Flow state");
    std::fs::create_dir_all(place.runtime_directory()).expect("Flow runtime");
    let change = meta_signal_flow::ConfigurationChange::Nexus(meta_signal_flow::ConfigureNexus {
        source_root: home.join("source").to_string_lossy().into_owned(),
        stable_codex: endpoint("stable", home),
        next_codex: endpoint("next", home),
        harness_profiles: vec![meta_signal_flow::HarnessProfile {
            harness_kind: signal_flow::HarnessKind::Codex,
            command_sigil_vector: vec!["/".into()],
            interrupt_keys: vec!["esc".into()],
            submit_keys: Vec::new(),
        }],
        meta_aspects: vec![flow_ethos::Aspect::Psyche, flow_ethos::Aspect::Mind],
        message_nexus_path_option: Some(
            runtime
                .join("message/message.sock")
                .to_string_lossy()
                .into_owned(),
        ),
        message_nexus_binary_option: Some(message_binary.to_string_lossy().into_owned()),
        curriculum_nexus_path: runtime
            .join("curriculum.sock")
            .to_string_lossy()
            .into_owned(),
        lease: 60,
        presentation_prompt_option: None,
    });
    assert_eq!(
        ConfigurationOnlyStore::open(&place.store())
            .expect("Flow memory opens")
            .configure_change(change)
            .expect("Configure commits"),
        meta_signal_flow::ConfigureOutcome::Replaced
    );
}

fn bind_sender(flow_socket: &Path, sender: &Address) {
    let mut peer = wait_for_socket(flow_socket);
    peer.write_query(&FlowQuery::Bind(Bind_Data {
        address: sender.clone(),
        process: process(),
    }))
    .expect("Bind writes");
    assert!(matches!(
        peer.read_response().expect("Bind answers"),
        FlowResponse::Bound(_)
    ));
}

fn send_through_message(
    message_socket: &Path,
    recipient: Address,
) -> signal_flow::VisibleSendAcceptance {
    let mut peer = wait_for_socket(message_socket);
    FramedStream::write_frame(
        &mut peer,
        &signal_message::Query::Send(SendRequest {
            recipient,
            sendable_message: SendableMessage::Soft(SendableRequest::Notice(
                "joint durable delivery".into(),
            )),
        }),
    )
    .expect("Message Send writes");
    let signal_message::Response::SendAccepted(acceptance) =
        FramedStream::read_frame::<signal_message::Response>(&mut peer)
            .expect("Message admission answers")
    else {
        panic!("Message returns the typed admission")
    };
    acceptance
}

fn message_tracking(home: &Path, runtime: &Path) -> message_nexus::store::DurableRequestTracking {
    let store = MessageStore::open(
        &home.join(".local/state/message/message.sema"),
        Standard {
            configuration: signal_message::Configuration {
                ordinary: runtime
                    .join("message/message.sock")
                    .to_string_lossy()
                    .into_owned(),
                meta: runtime
                    .join("message/message-owner.sock")
                    .to_string_lossy()
                    .into_owned(),
                flow: runtime
                    .join("flow/flow.sock")
                    .to_string_lossy()
                    .into_owned(),
            },
            meta_configured: false,
        },
    )
    .expect("Message Memory reopens");
    store.request_tracking().expect("Message tracking reads")
}

#[test]
#[ignore = "the dedicated Nix check supplies exact production Flow and Message binaries"]
fn real_flow_and_message_share_the_durable_delivery_ledger() {
    #[allow(clippy::manual_unwrap_or)]
    let stamped_revision = match option_env!("FLOW_MESSAGE_JOINT_SOURCE_REV") {
        Some(revision) => revision,
        None => "unstamped",
    };
    assert_eq!(
        std::env::var("FLOW_MESSAGE_JOINT_FLOW_REV").unwrap(),
        stamped_revision
    );
    assert_eq!(
        std::env::var("FLOW_MESSAGE_JOINT_MESSAGE_REV").unwrap(),
        "3d0fcbcdb829b09f80192d055d12e3af2754c9d0"
    );
    let flow_binary = required_path("FLOW_MESSAGE_JOINT_FLOW_BIN");
    let message_binary = required_path("FLOW_MESSAGE_JOINT_MESSAGE_BIN");
    let home = tempfile::tempdir().expect("private home");
    let runtime = tempfile::tempdir().expect("private runtime");
    let state = tempfile::tempdir().expect("private Flow state");
    let place = place(runtime.path(), state.path());
    let flow_socket = &runtime.path().join("flow/flow.sock");
    let message_socket = runtime.path().join("message/message.sock");
    configure_flow(&message_binary, home.path(), runtime.path(), &place);

    let flow = spawn_flow(&flow_binary, home.path(), runtime.path(), &place);
    let sender = Address {
        aspect: Aspect::Mind,
        topic: "jointSender".into(),
        model: Model::Sol,
    };
    bind_sender(flow_socket, &sender);
    let message = spawn_message(&message_binary, home.path(), runtime.path());
    wait_for_socket(&message_socket);
    let message_address = Address {
        aspect: Aspect::Field,
        topic: "message".into(),
        model: Model::Astra,
    };
    let acceptance = send_through_message(&message_socket, message_address);
    message.stop();

    let recovered = spawn_message(&message_binary, home.path(), runtime.path());
    wait_for_socket(&message_socket);
    recovered.stop();
    let tracking = message_tracking(home.path(), runtime.path());
    assert_eq!(tracking.ledger.len(), 1);
    assert!(tracking.pending.is_empty());
    assert_eq!(tracking.classified_events.len(), 1);
    assert_eq!(
        tracking.classified_events[0].classification,
        Classification::MessageRelay
    );
    assert!(tracking.observation_cursor >= Some(acceptance.delivery_cursor));
    let key = tracking.ledger[0].deliver_key.clone();
    let identity = tracking.store_identity.clone().expect("Flow identity held");
    let cursor = tracking.observation_cursor;
    flow.stop();

    let store = FlowStore::open_configured(&state.path().join("flow.sema"))
        .expect("the exact Flow Memory reopens");
    assert!(
        store
            .deliver_call(&key)
            .expect("Deliver call reads")
            .is_none(),
        "the real authenticated Acknowledge retracted Flow's replay row"
    );
    let history = store
        .read_message_delivery_snapshots(Some(0))
        .expect("Message-private history reads");
    assert_eq!(history.store_identity, identity);
    assert_eq!(history.delivery_snapshot_vector.len(), 1);
    assert_eq!(
        history.delivery_snapshot_vector[0].request_id,
        acceptance.request_id
    );
    drop(store);

    let flow = spawn_flow(&flow_binary, home.path(), runtime.path(), &place);
    drop(wait_for_socket(flow_socket));
    let message = spawn_message(&message_binary, home.path(), runtime.path());
    wait_for_socket(&message_socket);
    message.stop();
    flow.stop();
    let resumed = message_tracking(home.path(), runtime.path());
    assert_eq!(resumed.store_identity.as_deref(), Some(identity.as_str()));
    assert_eq!(resumed.observation_cursor, cursor);
    assert_eq!(resumed.classified_events, tracking.classified_events);
    assert_eq!(resumed.ledger.len(), 1);
}
