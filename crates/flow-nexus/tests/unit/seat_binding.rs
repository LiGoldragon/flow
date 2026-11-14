//! BindSeat from a Codex flow's own command. A remote Codex TUI's commands
//! run under the shared app-server, in no pane; the only account of their
//! flow is the thread Codex stamps on them, and a subagent's command carries
//! the subagent's thread. Its rollout names the root thread of its session,
//! and the seat is the one Herdr agent running that root: the agent that
//! reports it, or a Codex agent that reports no session and whose pane
//! resumes it in its foreground.

use super::NexusFixture;
use super::waking::SpawnedProcess;
use crate::event_wait::AwaitsPath;
use crate::fixture_executable::{FixtureExecutable, HoldsLatch, InstallsScript, Latch};
use crate::sending::{BindsSeats, IdentifiesSender, SendsRequests};
use crate::store::AnnouncesLaunchChanges;
use crate::store::metaflow::{MutatesMetaflows, SleepsMetaflows};
use crate::store::{FlowStore, ManagesMetaflowState, ReadsFlowMemory};
use signal_flow::{Address, Aspect, Model, Output, Refusal};
use std::fs;
use std::path::PathBuf;
use std::process::{Command, Stdio};

const ROOT: &str = "01a15071-6dd2-76c3-8ab7-945877549a01";
const SUBAGENT: &str = "01a15580-745e-7711-8d69-9636ed43d23e";
const ORPHAN: &str = "01a15580-0000-7000-8000-000000000001";

impl NexusFixture {
    /// The fixture's own process is the configured Codex app-server.
    fn serves_codex_commands(&self) {
        let app_server = fs::canonicalize(std::env::current_exe().unwrap())
            .unwrap()
            .to_string_lossy()
            .into_owned();
        self.nexus
            .store
            .configure_change(meta_signal_flow::ConfigurationChange::SendServers(vec![
                meta_signal_flow::ConfigureSendServer {
                    codex_app_server_binary: meta_signal_flow::CodexAppServerBinary(app_server),
                    mcp_send_server_binary: meta_signal_flow::McpSendServerBinary(
                        "/fixture/send-server".into(),
                    ),
                },
            ]))
            .unwrap();
    }

    /// A rollout whose session meta names `thread` in `session`.
    fn rollout(&self, thread: &str, session: &str) {
        let root = self.directory.path().join("native-transcripts/codex-next");
        fs::create_dir_all(&root).unwrap();
        fs::write(
            root.join(format!("rollout-2026-10-19T12-50-44-{thread}.jsonl")),
            format!(
                "{}\n",
                serde_json::json!({"type":"session_meta","payload":{"id":thread,"session_id":session}})
            ),
        )
        .unwrap();
    }

    /// A `flow-id` stand-in holding the one claim of the root thread.
    fn claims_the_root_only(&self) {
        let helper_root = self.directory.path().join("native-transcripts");
        fs::create_dir_all(&helper_root).unwrap();
        FixtureExecutable {
            path: helper_root.join("flow-id"),
        }
        .install(&format!(
            "#!/bin/sh\n[ \"$1\" = codex ] && [ \"$CODEX_SESSION_ID\" = '{ROOT}' ] || exit 2\nidentity=$(printf '%s' \"$CODEX_SESSION_ID\" | tr -d '-')\nprintf 'version=1\\nharness=codex\\nidentity=%s\\nalias=877549\\n' \"$identity\" > \"$3/.877549.flow-id\"\nprintf '877549\\n'\n"
        ));
    }
}

/// A Codex agent in `pane` that reports no session, as Herdr shows a
/// remote-resumed Codex TUI.
fn unreported_codex(pane: &str) -> serde_json::Value {
    serde_json::json!({
        "agent":"codex","agent_status":"done","name":"mind_context_secondary_877549",
        "pane_id":pane,"terminal_id":format!("term-{pane}"),"workspace_id":"w12",
        "tab_id":format!("tab-{pane}")
    })
}

/// A command the app-server runs for `thread`, in no pane.
fn command_of(thread: &str) -> (SpawnedProcess, signal_flow::Process) {
    use crate::exec_witness::SpawnsWitnessedExec;
    // Spawned so that the spawn returns once its exec is complete: its
    // environment, the thread it names, is readable from then on.
    let command = SpawnedProcess(
        std::process::Command::new("sleep")
            .arg("30")
            .env_remove("HERDR_PANE_ID")
            .env_remove("HERDR_SESSION")
            .env("CODEX_THREAD_ID", thread)
            .spawn_after_exec()
            .unwrap(),
    );
    let process = FlowStore::process_identity_for_pid(command.0.id()).unwrap();
    (command, process)
}

/// One real `flow-nexus` CLI invocation below three live shell hops. The outer
/// shell is the configured app-server's child and carries the Codex thread;
/// the wrapper writes its PID, then execs the CLI without changing it.
struct CodexCliWorld {
    fixture: &'static NexusFixture,
    socket: PathBuf,
    flow: PathBuf,
    shell: PathBuf,
    next_call: std::cell::Cell<u64>,
    _tui: SpawnedProcess,
}

struct CodexCliCall {
    process: signal_flow::Process,
    answer: String,
    codex_thread: Option<String>,
    /// The Address the caller sends as; None for a caller in no flow,
    /// which sends as Unbound.
    sending_address: Option<Address>,
}

/// The seat pane every CodexCliWorld's Herdr shows.
fn codex_pane() -> signal_flow::SeatPane {
    signal_flow::SeatPane {
        herdr_session_name: signal_flow::HerdrSessionName("messaging-build".into()),
        herdr_pane_id: signal_flow::HerdrPaneId("w12:p1".into()),
    }
}

fn bind_seat(address: Address) -> signal_flow::Input {
    signal_flow::Input::BindSeat(signal_flow::BindSeat_Data {
        address,
        seat_pane: codex_pane(),
    })
}

impl CodexCliWorld {
    fn new(configured_app_server: bool, seat_model: Model) -> Self {
        use crate::ServesOrdinary;

        let fixture: &'static NexusFixture = Box::leak(Box::new(NexusFixture::new()));
        fixture
            .nexus
            .store
            .configure_change(meta_signal_flow::ConfigurationChange::Models(vec![
                meta_signal_flow::ConfigureModel {
                    model: Model::Terra,
                    native: meta_signal_flow::Native("gpt-6-sol".into()),
                },
                meta_signal_flow::ConfigureModel {
                    model: Model::Luna,
                    native: meta_signal_flow::Native("gpt-6-luna".into()),
                },
            ]))
            .unwrap();
        if configured_app_server {
            fixture.serves_codex_commands();
        }
        fixture.list_herdr_sessions(&[(
            "messaging-build",
            fixture
                .directory
                .path()
                .join("herdr-config/sessions/messaging-build/herdr.sock"),
        )]);
        fixture.claims_the_root_only();
        fixture.rollout(ROOT, ROOT);
        let tui = SpawnedProcess(
            Command::new("sh")
                .args(["-c", "sleep 30; :", "codex", "resume", ROOT])
                .spawn()
                .unwrap(),
        );
        super::herdr_world(fixture, vec![unreported_codex("w12:p1")], tui.0.id());

        let executable = std::env::current_exe().expect("test executable");
        let binaries = executable
            .ancestors()
            .skip(1)
            .find(|directory| directory.join("flow-nexus").is_file())
            .unwrap_or_else(|| panic!("no flow-nexus CLI above {}", executable.display()));
        let flow = fs::canonicalize(binaries.join("flow-nexus")).unwrap();
        let shell = fixture.directory.path().join("caller-shell.sh");
        let script = fixture.directory.path().join("caller-script.sh");
        let wrapper = fixture.directory.path().join("caller-wrapper.sh");
        FixtureExecutable {
            path: shell.clone(),
        }
        .install(&format!(
            "#!/bin/sh\nprintf '%s\\n' \"$$\" > \"$1/shell.pid\"\nsh '{}' \"$@\"\n",
            script.display()
        ));
        FixtureExecutable { path: script }.install(&format!(
            "#!/bin/sh\nprintf '%s\\n' \"$$\" > \"$1/script.pid\"\nsh '{}' \"$@\"\n",
            wrapper.display()
        ));
        FixtureExecutable { path: wrapper }.install(&format!(
            "#!/bin/sh\nprintf '%s\\n' \"$$\" > \"$1/wrapper.pid\"\n: > \"$1/ready\"\n: < \"$1/go\"\nexec '{}' \"$2\"\n",
            flow.display()
        ));

        // The seat's place under the fixture's runtime root, where the
        // client resolves it.
        let place = fixture.directory.path().join(format!(
            "run/nexus/{}",
            match seat_model {
                Model::Terra => "terra",
                Model::Luna => "luna",
                _ => "other",
            }
        ));
        fs::create_dir_all(&place).unwrap();
        let socket = place.join("flow.sock");
        let served = socket.clone();
        std::thread::spawn(move || fixture.nexus.serve_ordinary(&served));
        // The name is moved in only once the socket listens.
        assert!(
            socket.appeared_by(std::time::Instant::now() + std::time::Duration::from_secs(5)),
            "ordinary socket listens"
        );
        Self {
            fixture,
            socket,
            flow,
            shell,
            next_call: std::cell::Cell::new(0),
            _tui: tui,
        }
    }

    /// Runs the production client while the dispatch gate keeps its kernel
    /// peer alive. Returns that exact peer identity and its printed answer.
    fn call(&self, query: signal_flow::Input, thread: Option<&str>) -> CodexCliCall {
        use crate::caller::{CallerProcess, LocatesCodexThread, ReadsProcess};
        use crate::store::sent::NamesCodexAppServers;
        use datom_codec::Datomizable;
        use protos::{Compactable, Protosizable};

        let number = self.next_call.get() + 1;
        self.next_call.set(number);
        let call = self
            .fixture
            .directory
            .path()
            .join(format!("cli-call-{number}"));
        fs::create_dir(&call).unwrap();
        let query = query.datomize(vec![]).protosize().compact();
        let place = self.socket.parent().expect("the socket lies in its place");
        let place_name = place
            .file_name()
            .and_then(|name| name.to_str())
            .expect("a place name");
        let runtime = place
            .parent()
            .and_then(std::path::Path::parent)
            .expect("the place lies under the runtime root");
        // The client reads its place from its configuration.
        let configuration = call.join("config");
        fs::create_dir_all(configuration.join("nexus")).unwrap();
        fs::write(
            configuration.join("nexus/flow-nexus.datom"),
            format!("{{ {place_name} }}\n"),
        )
        .unwrap();
        let gate = self.fixture.nexus.dispatch_gate.lock().unwrap();
        let mut command = Command::new("sh");
        command
            .arg(&self.shell)
            .arg(&call)
            .arg(query)
            .env("XDG_RUNTIME_DIR", runtime)
            .env("XDG_CONFIG_HOME", &configuration)
            .env_remove("HERDR_PANE_ID")
            .env_remove("HERDR_SESSION")
            .env_remove("FLOW_ID")
            .env_remove("FLOW_DIRECTORY")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        match thread {
            Some(thread) => {
                command.env("CODEX_THREAD_ID", thread);
            }
            None => {
                command.env_remove("CODEX_THREAD_ID");
            }
        }
        let go = Latch::closed(call.join("go"));
        let child = command.spawn().unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        assert!(
            call.join("ready").appeared_by(deadline),
            "wrapper becomes ready"
        );
        let pid = |name: &str| {
            fs::read_to_string(call.join(name))
                .unwrap()
                .trim()
                .parse::<u32>()
                .unwrap()
        };
        let shell_pid = pid("shell.pid");
        let script_pid = pid("script.pid");
        let wrapper_pid = pid("wrapper.pid");
        // The wrapper waits behind `go`; traced before it is let go, it is
        // seen again once its exec of the CLI is complete.
        let exec = {
            use crate::exec_witness::{SeizesUntilExec, Tracee};
            Tracee::seized(wrapper_pid).expect("the wrapper is traced until its exec")
        };
        go.release();
        crate::exec_witness::SeizesUntilExec::exec_completed(exec)
            .expect("the wrapper execs the real flow-nexus CLI");
        let cli = CallerProcess {
            process_id: wrapper_pid,
        };
        assert_eq!(
            cli.executable().as_ref(),
            Some(&self.flow),
            "wrapper execs the real flow-nexus CLI"
        );
        assert_eq!(
            cli.parent().map(|parent| parent.process_id),
            Some(script_pid)
        );
        let script = cli.parent().unwrap();
        assert_eq!(
            script.parent().map(|parent| parent.process_id),
            Some(shell_pid)
        );
        let shell = script.parent().unwrap();
        assert_eq!(
            shell.parent().map(|parent| parent.process_id),
            Some(std::process::id()),
            "the configured test executable is the real kernel ancestor"
        );
        let process = FlowStore::process_identity_for_pid(wrapper_pid).unwrap();
        let app_servers = self.fixture.nexus.store.codex_app_servers().unwrap();
        let codex_thread = cli.codex_thread(&app_servers);
        let sending_address = match self.fixture.nexus.sender_of(&process) {
            Ok(signal_flow::SentBy::Sender(address)) => Some(address),
            _ => None,
        };
        drop(gate);
        let output = child.wait_with_output().unwrap();
        assert!(
            output.status.success(),
            "real flow-nexus CLI failed: {output:?}"
        );
        assert!(
            output.stderr.is_empty(),
            "real flow-nexus CLI stderr: {output:?}"
        );
        CodexCliCall {
            process,
            answer: String::from_utf8(output.stdout).unwrap().trim().to_owned(),
            codex_thread,
            sending_address,
        }
    }
}

#[test]
fn a_codex_seat_herdr_reports_no_session_for_binds_from_its_named_pane() {
    let fixture = NexusFixture::new();
    fixture
        .nexus
        .store
        .configure_change(meta_signal_flow::ConfigurationChange::Models(vec![
            meta_signal_flow::ConfigureModel {
                model: Model::Sol,
                native: meta_signal_flow::Native("gpt-6-sol".into()),
            },
        ]))
        .unwrap();
    // One running session: the fixture's Herdr shows the same agents in
    // every session it is asked for.
    fixture.list_herdr_sessions(&[(
        "messaging-build",
        fixture
            .directory
            .path()
            .join("herdr-config/sessions/messaging-build/herdr.sock"),
    )]);
    fixture.serves_codex_commands();
    fixture.claims_the_root_only();
    fixture.rollout(ROOT, ROOT);
    fixture.rollout(SUBAGENT, ROOT);
    fixture.rollout(ORPHAN, "01a15580-0000-7000-8000-0000000000ff");
    let address = Address {
        aspect: Aspect::Mind,
        topic: flow_ethos::Topic("context".into()),
        model: Model::Sol,
    };
    // The TUI in the seat's pane resumes the root thread.
    let tui = SpawnedProcess(
        std::process::Command::new("sh")
            .args(["-c", "sleep 30; :", "codex", "resume", ROOT])
            .spawn()
            .unwrap(),
    );
    let (_subagent_command, subagent) = command_of(SUBAGENT);
    let (_orphan_command, orphan) = command_of(ORPHAN);

    super::herdr_world(&fixture, vec![unreported_codex("w12:p1")], tui.0.id());
    // A pane Herdr shows no agent in names no seat.
    let pane = |pane: &str| signal_flow::SeatPane {
        herdr_session_name: signal_flow::HerdrSessionName("messaging-build".into()),
        herdr_pane_id: signal_flow::HerdrPaneId(pane.into()),
    };
    assert!(matches!(
        fixture.nexus.bind_seat(signal_flow::BindSeat_Data {
            address: address.clone(),
            seat_pane: pane("w12:p9"),
        }),
        Output::Refused(Refusal::Unidentified(_))
    ));
    // The pane's seat binds under the root's claim, whoever asks: the
    // thread its foreground resumes is its session. Adoption announces the
    // new live Codex thread so the Nexus's responder subscribes to it and
    // its first rest can drain already queued work.
    let before_adoption = fixture.nexus.store.launch_changes.current();
    assert_eq!(
        fixture.nexus.bind_seat(signal_flow::BindSeat_Data {
            address: address.clone(),
            seat_pane: pane("w12:p1"),
        }),
        Output::SeatBound(flow_ethos::FlowId("877549".into()))
    );
    assert!(
        fixture.nexus.store.launch_changes.current() > before_adoption,
        "adoption wakes the Codex responder supervisor"
    );
    // A subagent's command sends as its flow's Address: the sender is
    // filled in from its root thread. Message's Identify reads the same.
    assert_eq!(
        fixture.nexus.sender_of(&subagent).unwrap(),
        signal_flow::SentBy::Sender(address.clone())
    );
    use crate::metaflow::DispatchesMetaflowPeer;
    let (_message_child, message) = super::waking::message_peer(&fixture);
    assert_eq!(
        fixture
            .nexus
            .dispatch_metaflow_peer(
                signal_flow::Input::Identify(subagent.clone()),
                Some(&message)
            )
            .response,
        Output::Identified(address.clone())
    );
    // A command whose thread no flow runs sends as Unbound; Identify names
    // no Address for it.
    assert_eq!(
        fixture.nexus.sender_of(&orphan).unwrap(),
        signal_flow::SentBy::Unbound
    );
    super::assert_unidentified(
        fixture
            .nexus
            .dispatch_metaflow_peer(signal_flow::Input::Identify(orphan.clone()), Some(&message))
            .response,
        &orphan,
    );
    let awake = fixture.nexus.store.metaflow_awake_as("877549").unwrap();
    assert_eq!(awake.map(|metaflow| metaflow.address), Some(address));
}

/// The same real CLI peer is authenticated once for Send, Keep and Drop
/// through its configured Codex app-server ancestry.
/// A real CLI caller is answered alike whoever it is: BindSeat binds the
/// pane it names, from a Codex command or from a caller in no flow; Send
/// fills in its sender, the thread's Address or Unbound, and is accepted
/// either way; Keep and Drop write the named Address's handover, whoever
/// asks, and answer by the Address's state alone.
#[test]
fn a_real_cli_caller_is_answered_alike_whoever_it_is() {
    use crate::store::handover::HoldsHandovers;
    use crate::store::metaflow::SeedsMetaflow;
    use crate::store::send_outcome::RecordsSendOutcomes;
    use crate::store::sent::NamesCodexAppServers;
    use crate::store::{ReadsFlowRoles, ReadsFlowRows};
    use flow_ethos::State;
    use signal_flow::{Input, Message, Request, Section, SendRequest};

    let terra = super::waking::address(Model::Terra);
    let luna = super::waking::address(Model::Luna);
    let keep = |address: Address, title: &str| {
        Input::Keep(signal_flow::Keep_Data {
            address,
            section: Section {
                title: signal_flow::Title(title.into()),
                description: signal_flow::Description("caller attribution".into()),
                body: signal_flow::Body("test only".into()),
            },
        })
    };
    let send = |text: &str| {
        Input::Send(SendRequest {
            address: luna.clone(),
            sendable_message: super::sendable_message(Message::Soft(Request::Notice(text.into()))),
        })
    };

    // A Codex command of the seat's thread binds the pane it names.
    let configured = CodexCliWorld::new(true, Model::Terra);
    let app_servers = configured.fixture.nexus.store.codex_app_servers().unwrap();
    assert_eq!(
        app_servers,
        [fs::canonicalize(std::env::current_exe().unwrap()).unwrap()]
            .into_iter()
            .collect()
    );
    let bind = configured.call(bind_seat(terra.clone()), Some(ROOT));
    assert_eq!(bind.answer, "SeatBound.877549");
    assert_eq!(bind.codex_thread.as_deref(), Some(ROOT));
    let node = configured
        .fixture
        .nexus
        .store
        .flow_node("877549")
        .unwrap()
        .expect("BindSeat registered the live Codex seat");
    assert_eq!(node.session_id.0, ROOT);
    assert_eq!(node.harness_kind, signal_flow::HarnessKind::Codex);
    assert!(
        configured
            .fixture
            .nexus
            .store
            .role("877549")
            .unwrap()
            .is_some()
    );
    configured
        .fixture
        .nexus
        .store
        .seed_metaflow(super::waking::metaflow(
            Model::Luna,
            State::Asleep,
            vec![],
            vec![],
        ));
    let sent = configured.call(send("from the seat"), Some(ROOT));
    assert!(sent.answer.starts_with("SendAccepted."), "{}", sent.answer);
    assert_eq!(sent.sending_address, Some(terra.clone()));
    let outcomes = configured.fixture.nexus.store.read_send_outcomes().unwrap();
    assert_eq!(
        outcomes
            .last()
            .and_then(|outcome| outcome.process_option.clone()),
        Some(sent.process.clone()),
        "the production ordinary socket recorded the exact live kernel peer"
    );
    // The seat and a caller in no flow write the same handover alike.
    let kept = configured.call(keep(terra.clone(), "prior"), Some(ROOT));
    assert!(kept.answer.starts_with("Kept."), "{}", kept.answer);
    let kept_unbound = configured.call(keep(terra.clone(), "diagnostic"), None);
    assert_eq!(kept_unbound.sending_address, None);
    assert!(
        kept_unbound.answer.starts_with("Kept."),
        "{}",
        kept_unbound.answer
    );
    let dropped = configured.call(
        Input::Drop(signal_flow::Drop_Data {
            address: terra.clone(),
            title: signal_flow::Title("diagnostic".into()),
        }),
        None,
    );
    assert!(dropped.answer.starts_with("Dropped."), "{}", dropped.answer);
    assert_eq!(
        configured
            .fixture
            .nexus
            .store
            .handover_headings(&terra)
            .unwrap()
            .into_iter()
            .map(|heading| heading.title.0)
            .collect::<Vec<_>>(),
        vec!["prior"]
    );
    // A caller in no flow sends as Unbound and is served.
    let unbound_send = configured.call(send("from a shell"), None);
    assert!(
        unbound_send.answer.starts_with("SendAccepted."),
        "{}",
        unbound_send.answer
    );
    assert_eq!(unbound_send.sending_address, None);

    // With no app-server configured the same command names no thread, and
    // is answered all the same: it binds the pane it names and sends as
    // Unbound.
    let unconfigured = CodexCliWorld::new(false, Model::Terra);
    let unconfigured_bind = unconfigured.call(bind_seat(terra.clone()), Some(ROOT));
    assert_eq!(unconfigured_bind.answer, "SeatBound.877549");
    assert_eq!(unconfigured_bind.codex_thread, None);
    assert_eq!(unconfigured_bind.sending_address, None);
    unconfigured
        .fixture
        .nexus
        .store
        .seed_metaflow(super::waking::metaflow(
            Model::Luna,
            State::Asleep,
            vec![],
            vec![],
        ));
    let unconfigured_send = unconfigured.call(send("unconfigured"), Some(ROOT));
    assert!(
        unconfigured_send.answer.starts_with("SendAccepted."),
        "{}",
        unconfigured_send.answer
    );

    // An Adopt naming another native session than the pane's seat runs is
    // refused by the seat's state, whoever asks.
    let mismatched = CodexCliWorld::new(true, Model::Terra);
    let mismatch = mismatched.call(
        Input::Adopt(signal_flow::Adopt_Data {
            address: terra.clone(),
            seat_pane: codex_pane(),
            native_session_id: signal_flow::NativeSessionId("different-native-session".into()),
        }),
        Some(ROOT),
    );
    assert!(
        mismatch.answer.starts_with("Refused.Unidentified."),
        "{}",
        mismatch.answer
    );
    assert_eq!(mismatched.fixture.nexus.live_awake_flow(&terra), None);

    // A Keep of an Address Awake as no flow is refused by that state alone,
    // to the Luna seat and to a caller in no flow alike.
    let wrong_seat = CodexCliWorld::new(true, Model::Luna);
    let luna_bind = wrong_seat.call(bind_seat(luna.clone()), Some(ROOT));
    assert_eq!(luna_bind.answer, "SeatBound.877549");
    let luna_keep = wrong_seat.call(keep(terra.clone(), "diagnostic"), Some(ROOT));
    assert_eq!(luna_keep.sending_address, Some(luna.clone()));
    assert_eq!(luna_keep.answer, "KeepRefused.UnknownFlow");
    let unbound_keep = wrong_seat.call(keep(terra.clone(), "diagnostic"), None);
    assert_eq!(unbound_keep.sending_address, None);
    assert_eq!(unbound_keep.answer, "KeepRefused.UnknownFlow");
    assert!(
        wrong_seat
            .fixture
            .nexus
            .store
            .handover_headings(&terra)
            .unwrap()
            .is_empty()
    );
}

/// Address-only process registration remains the sender a process sends as
/// across a later native owner, never that owner's FlowOrigin. Nothing it
/// asks depends on it: a Keep of the Address writes the Address's flow's
/// handover and a Stop of a protected Address's flow stops it, as for any
/// caller.
#[test]
fn a_registered_address_process_is_answered_as_any_caller() {
    use crate::Dispatches;
    use crate::caller::{AuthenticatesProcessAuthority, ProcessAuthority};
    use crate::store::handover::HoldsHandovers;
    use crate::store::metaflow::SeedsMetaflow;
    use crate::store::{ReadsFlowRows, RegistersFlowIdentity};
    use flow_ethos::State;
    use signal_flow::{FlowLifecycle, HarnessKind, Input, Metaflow, Origin, Section, SentBy};

    let fixture = NexusFixture::new();
    let address = super::waking::address(Model::Terra);
    let child = SpawnedProcess(Command::new("sleep").arg("30").spawn().unwrap());
    let peer = FlowStore::process_identity_for_pid(child.0.id()).unwrap();
    assert!(matches!(
        fixture.nexus.store.bind_metaflow(
            signal_flow::Bind_Data {
                address: address.clone(),
                process: peer.clone(),
            },
            None,
        ),
        Ok(Output::Bound(_))
    ));
    assert_eq!(
        fixture.nexus.process_authority(&peer).unwrap(),
        ProcessAuthority::Sending(address.clone())
    );
    assert_eq!(
        fixture.nexus.sender_of(&peer).unwrap(),
        SentBy::Sender(address.clone())
    );

    let mut successor = fixture.node();
    successor.flow_id = flow_ethos::FlowId("successor".into());
    successor.session_id = signal_flow::SessionId("successor-native-session".into());
    successor.harness_kind = HarnessKind::Codex;
    successor.origin = Origin::Unbound;
    successor.flow_lifecycle = FlowLifecycle::Active;
    fixture.nexus.store.register_flow(successor).unwrap();
    fixture.nexus.store.seed_metaflow(Metaflow {
        address: address.clone(),
        state: State::Awake(flow_ethos::FlowId("successor".into())),
        past: flow_ethos::Past(vec![]),
        queue: flow_ethos::Queue(vec![]),
    });
    fixture
        .nexus
        .store
        .configure_change(meta_signal_flow::ConfigurationChange::ProtectedAddresses(
            vec![address.clone()],
        ))
        .unwrap();

    assert_eq!(
        fixture.nexus.process_authority(&peer).unwrap(),
        ProcessAuthority::Sending(address.clone()),
        "the old Address binder is never promoted to the successor"
    );
    let keep = Input::Keep(signal_flow::Keep_Data {
        address: address.clone(),
        section: Section {
            title: signal_flow::Title("written".into()),
            description: signal_flow::Description("any caller".into()),
            body: signal_flow::Body("lands".into()),
        },
    });
    let Output::Kept(kept) = fixture.nexus.dispatch(keep) else {
        panic!("a Keep of an Awake Address is taken from any caller")
    };
    assert_eq!(kept.address, address);
    assert_eq!(
        fixture
            .nexus
            .store
            .handover_headings(&address)
            .unwrap()
            .into_iter()
            .map(|heading| heading.title.0)
            .collect::<Vec<_>>(),
        vec!["written"]
    );
    // A protected Address's flow is stopped as any flow is: here its pane
    // is no route, which is the answer every caller gets.
    let stopped = fixture
        .nexus
        .dispatch(Input::Stop(flow_ethos::FlowId("successor".into())));
    assert_ne!(
        stopped,
        Output::Refused(signal_flow::Refusal::Store("protected".into()))
    );
    assert!(
        !matches!(
            stopped,
            Output::Refused(signal_flow::Refusal::Unidentified(_))
        ),
        "{stopped:?}"
    );
    assert_eq!(
        fixture.nexus.dispatch_from(
            Input::Stop(flow_ethos::FlowId("successor".into())),
            Origin::Flow(signal_flow::FlowOrigin {
                flow_id: flow_ethos::FlowId("successor".into()),
                session_id: signal_flow::SessionId("successor-native-session".into()),
            }),
        ),
        fixture
            .nexus
            .dispatch(Input::Stop(flow_ethos::FlowId("successor".into()))),
        "the flow itself and a caller in no flow are answered alike"
    );
    assert!(
        fixture
            .nexus
            .store
            .flow_node("successor")
            .unwrap()
            .is_some()
    );
}

/// What a caller sends as is filled in, never refused: a Codex command with
/// no live flow on its thread, a stale identity and the operator at a
/// terminal are all Unbound.
#[test]
fn a_caller_no_record_names_is_unbound() {
    use crate::caller::{AuthenticatesProcessAuthority, ProcessAuthority};

    let unconfigured = NexusFixture::new();
    let (_marked_child, marked) = command_of("unknown-native-session");
    assert_eq!(
        unconfigured.nexus.process_authority(&marked).unwrap(),
        ProcessAuthority::Unbound,
    );
    assert_eq!(
        unconfigured.nexus.sender_of(&marked).unwrap(),
        signal_flow::SentBy::Unbound
    );

    let configured = NexusFixture::new();
    configured.serves_codex_commands();
    let (_unknown_child, unknown) = command_of("unknown-native-session");
    assert_eq!(
        configured.nexus.process_authority(&unknown).unwrap(),
        ProcessAuthority::Unbound,
    );
    let mut stale = unknown;
    stale.started.0 += 1;
    assert_eq!(
        configured.nexus.process_authority(&stale).unwrap(),
        ProcessAuthority::Unbound
    );
    let operator = FlowStore::process_identity_for_pid(std::process::id()).unwrap();
    assert_eq!(
        configured.nexus.process_authority(&operator).unwrap(),
        ProcessAuthority::Unbound
    );
    assert_eq!(
        configured.nexus.sender_of(&operator).unwrap(),
        signal_flow::SentBy::Unbound
    );
}

#[test]
fn a_known_claude_seat_binds_from_its_named_pane_whoever_asks() {
    use super::ControlsHerdrSnapshot;
    use super::waking::{CLAUDE_SEAT, claude_agent, claude_seat, metaflow};
    use crate::store::metaflow::SeedsMetaflow;
    use crate::store::{ReadsFlowRows, RegistersFlowIdentity};
    let fixture = NexusFixture::new();
    claude_seat(&fixture, 100);
    let mut role = super::mind_live();
    role.flow_id = flow_ethos::FlowId(CLAUDE_SEAT.into());
    let node = fixture.nexus.store.flow_node(CLAUDE_SEAT).unwrap().unwrap();
    fixture
        .nexus
        .store
        .register_flow_in_role(node, role)
        .unwrap();
    fixture.set_agents(vec![claude_agent(&fixture)]);
    fixture.nexus.store.seed_metaflow(metaflow(
        Model::Sol,
        flow_ethos::State::Awake(flow_ethos::FlowId(CLAUDE_SEAT.into())),
        vec![],
        vec![],
    ));
    let address = Address {
        aspect: Aspect::Mind,
        topic: flow_ethos::Topic("flow".into()),
        model: Model::Sol,
    };
    // Whoever asks, the seat the Address is Awake as binds idempotently
    // from the pane it lives in.
    let pane = |pane: &str| signal_flow::SeatPane {
        herdr_session_name: signal_flow::HerdrSessionName("messaging-build".into()),
        herdr_pane_id: signal_flow::HerdrPaneId(pane.into()),
    };
    for _ in 0..2 {
        assert_eq!(
            fixture.nexus.bind_seat(signal_flow::BindSeat_Data {
                address: address.clone(),
                seat_pane: pane("w1:p3"),
            }),
            Output::SeatBound(flow_ethos::FlowId(CLAUDE_SEAT.into()))
        );
    }
    // A pane Herdr shows no agent in names no seat.
    assert!(matches!(
        fixture.nexus.bind_seat(signal_flow::BindSeat_Data {
            address,
            seat_pane: pane("unknown-pane"),
        }),
        Output::Refused(Refusal::Unidentified(_))
    ));
}

/// A session id the Claude harness assigned: a UUIDv4 Flow never chose.
const HARNESS_SESSION: &str = "7c3e9a51-2b8d-4f60-9e1a-5d4c3b2a1f00";
/// Its Flow ID: the first six characters of the session id, as the
/// `flow-id` helper derives it.
const HARNESS_FLOW_ID: &str = "7c3e9a";

impl NexusFixture {
    /// A `flow-id` stand-in claiming a Claude session as the real helper
    /// does: its identity is the session id without hyphens, its alias the
    /// first six characters.
    fn claims_claude_sessions(&self) {
        let helper_root = self.directory.path().join("native-transcripts");
        fs::create_dir_all(&helper_root).unwrap();
        FixtureExecutable {
            path: helper_root.join("flow-id"),
        }
        .install(
            "#!/bin/sh\n[ \"$1\" = claude ] && [ \"$2\" = --flows-root ] && [ \"$4\" = --parent-session ] || exit 2\nidentity=$(printf '%s' \"$5\" | tr -d -)\nalias=$(printf '%s' \"$identity\" | cut -c1-6)\nprintf 'version=1\\nharness=claude\\nidentity=%s\\nalias=%s\\nuuid-version=uuid-v4\\n' \"$identity\" \"$alias\" > \"$3/.$alias.flow-id\"\nprintf '%s\\n' \"$alias\"\n",
        );
    }

    /// Claude's own record of the session `process_id` runs, under the
    /// fixture's Claude home, as Claude writes it: the process, its start,
    /// the session.
    fn claude_session_file(&self, process_id: u32, session: &str) {
        let sessions = self
            .directory
            .path()
            .join("native-transcripts/claude-home/sessions");
        fs::create_dir_all(&sessions).unwrap();
        let started = FlowStore::process_identity_for_pid(process_id)
            .unwrap()
            .started;
        fs::write(
            sessions.join(format!("{process_id}.json")),
            serde_json::json!({
                "pid": process_id, "sessionId": session, "procStart": started.0.to_string(),
                "cwd": "/home/li/primary", "kind": "interactive", "entrypoint": "cli"
            })
            .to_string(),
        )
        .unwrap();
    }

    /// The fixture Herdr: `agents` in every session, each pane labelled
    /// `label`, its foreground the one process `foreground`.
    fn herdr_panes(&self, agents: Vec<serde_json::Value>, label: &str, foreground: u32) {
        let snapshot = serde_json::json!({
            "id":"cli:api:snapshot",
            "result":{"snapshot":{"agents":agents,"protocol":20,"version":"0.8.2"},
            "type":"session_snapshot"}
        });
        FixtureExecutable {
            path: self.snapshot_program.clone(),
        }
        .install(&format!(
            "#!/bin/sh\n{}\ncase \"$3 $4\" in\n  \"api snapshot\") printf '%s\\n' '{}' ;;\n  \"pane process-info\") printf '{{\"result\":{{\"process_info\":{{\"pane_id\":\"%s\",\"foreground_processes\":[{{\"pid\":{}}}]}}}}}}\\n' \"$6\" ;;\n  \"pane get\") printf '{{\"result\":{{\"pane\":{{\"pane_id\":\"%s\",\"label\":\"{}\"}},\"type\":\"pane_info\"}}}}\\n' \"$5\" ;;\n  *) exit 64 ;;\nesac\n",
            self.session_listing(),
            snapshot,
            foreground,
            label,
        ));
    }

    /// The Claude model configured for Psyche Secondaries.
    fn configures_claude_secondaries(&self) {
        self.nexus
            .store
            .configure_change(meta_signal_flow::ConfigurationChange::Models(vec![
                meta_signal_flow::ConfigureModel {
                    model: Model::Opus,
                    native: meta_signal_flow::Native("claude-opus-5-5".into()),
                },
            ]))
            .unwrap();
    }
}

/// A Claude agent in pane w1:p3 as Herdr records it; it reports `session`
/// when Herdr read one.
fn claude_in_pane(session: Option<&str>) -> serde_json::Value {
    let mut agent = serde_json::json!({
        "agent":"claude","agent_status":"idle","executable":"claude",
        "name":"claude-6aafcaae640d8089d70ae839","display_agent":"{ Psyche flow Opus }",
        "pane_id":"w1:p3","terminal_id":"term_c1a0de","workspace_id":"w1","tab_id":"w1:t1"
    });
    if let Some(session) = session {
        agent["agent_session"] = serde_json::json!({
            "source":"herdr:claude","agent":"claude","kind":"id","value":session
        });
    }
    agent
}

fn psyche_flow_secondary() -> Address {
    Address {
        aspect: Aspect::Psyche,
        topic: flow_ethos::Topic("flow".into()),
        model: Model::Opus,
    }
}

/// The pane every Claude seat test's Herdr shows the seat in, or another.
fn claude_pane(pane: &str) -> signal_flow::SeatPane {
    signal_flow::SeatPane {
        herdr_session_name: signal_flow::HerdrSessionName("messaging-build".into()),
        herdr_pane_id: signal_flow::HerdrPaneId(pane.into()),
    }
}

/// On an empty store, BindSeat naming a Claude pane adopts the seat, from
/// any caller: the pane's Herdr agent record, the Claude session the record
/// reports (or Claude's own session file names) and the process running it
/// make the seat, bound to the address under the session's Flow ID. The
/// seat then sends as that Address.
#[test]
fn a_claude_seat_binds_on_an_empty_store_under_its_sessions_flow_id() {
    use super::MarkedProcess;
    use crate::sending::SendsRequests;
    use crate::store::ReadsFlowRows;
    for reported in [Some(HARNESS_SESSION), None] {
        let fixture = NexusFixture::new();
        fixture.configures_claude_secondaries();
        fixture.claims_claude_sessions();
        let address = psyche_flow_secondary();
        let claude = MarkedProcess::spawn("sleep", &["30"], "w1:p3");
        fixture.claude_session_file(claude.0.id(), HARNESS_SESSION);
        fixture.herdr_panes(
            vec![claude_in_pane(reported)],
            "{ Psyche flow Opus }",
            claude.0.id(),
        );
        assert_eq!(
            fixture.nexus.bind_seat(signal_flow::BindSeat_Data {
                address: address.clone(),
                seat_pane: claude_pane("w1:p3"),
            }),
            Output::SeatBound(flow_ethos::FlowId(HARNESS_FLOW_ID.into())),
            "{reported:?}: the Claude seat binds under its session's Flow ID"
        );
        let peer = FlowStore::process_identity_for_pid(claude.0.id()).unwrap();
        assert_eq!(
            fixture.nexus.sender_of(&peer).unwrap(),
            signal_flow::SentBy::Sender(address.clone())
        );
        let node = fixture
            .nexus
            .store
            .flow_node(HARNESS_FLOW_ID)
            .unwrap()
            .expect("the seat's flow is registered");
        assert_eq!(node.session_id.0, HARNESS_SESSION);
        assert_eq!(node.harness_kind, signal_flow::HarnessKind::Claude);
        assert_eq!(
            fixture.nexus.live_awake_flow(&address).as_deref(),
            Some(HARNESS_FLOW_ID)
        );
    }
}

/// Adopt binds the named pane's seat only when its native session is the
/// one supplied. The session is checked before the known-seat idempotency
/// shortcut, so the same live seat cannot be adopted under a different
/// session after its first adoption.
#[test]
fn a_claude_seat_is_adopted_only_under_its_own_session() {
    use super::MarkedProcess;
    use crate::sending::SendsRequests;
    let fixture = NexusFixture::new();
    fixture.configures_claude_secondaries();
    fixture.claims_claude_sessions();
    let address = psyche_flow_secondary();
    let claude = MarkedProcess::spawn("sleep", &["30"], "w1:p3");
    fixture.claude_session_file(claude.0.id(), HARNESS_SESSION);
    fixture.herdr_panes(
        vec![claude_in_pane(Some(HARNESS_SESSION))],
        "{ Psyche flow Opus }",
        claude.0.id(),
    );
    let request = |session: &str| signal_flow::Adopt_Data {
        address: address.clone(),
        seat_pane: claude_pane("w1:p3"),
        native_session_id: signal_flow::NativeSessionId(session.into()),
    };

    assert!(matches!(
        fixture.nexus.adopt(request("different-session")),
        Output::Refused(Refusal::Unidentified(_))
    ));
    assert_eq!(fixture.nexus.live_awake_flow(&address), None);

    for _ in 0..2 {
        assert_eq!(
            fixture.nexus.adopt(request(HARNESS_SESSION)),
            Output::Adopted(flow_ethos::FlowId(HARNESS_FLOW_ID.into()))
        );
    }
    assert!(matches!(
        fixture.nexus.adopt(request("different-session")),
        Output::Refused(Refusal::Unidentified(_))
    ));
    assert_eq!(
        fixture.nexus.live_awake_flow(&address).as_deref(),
        Some(HARNESS_FLOW_ID)
    );
}

/// A pane Herdr holds no agent record for names no seat: BindSeat refuses,
/// and what the pane showed is kept as raw evidence (its pane, its label,
/// no session, the time it was read), never made into an identity. The
/// refusal names no process: it is the same whoever asked.
#[test]
fn a_pane_with_no_agent_record_refuses_with_its_evidence() {
    use super::MarkedProcess;
    use crate::caller::CallerPane;
    use crate::sending::{FindsSeat, SeatEvidence};
    let fixture = NexusFixture::new();
    fixture.configures_claude_secondaries();
    fixture.claims_claude_sessions();
    let shell = MarkedProcess::spawn("sleep", &["30"], "w1:p9");
    fixture.herdr_panes(
        vec![claude_in_pane(Some(HARNESS_SESSION))],
        "zsh",
        shell.0.id(),
    );
    let before = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let reply = fixture.nexus.bind_seat(signal_flow::BindSeat_Data {
        address: psyche_flow_secondary(),
        seat_pane: claude_pane("w1:p9"),
    });
    let Output::Refused(Refusal::Unidentified(refused)) = reply else {
        panic!("a pane with no agent record names no seat: {reply:?}");
    };
    let Err(evidence) = fixture.nexus.seat_of(&CallerPane {
        herdr_session_name: "messaging-build".into(),
        herdr_pane_id: "w1:p9".into(),
    }) else {
        panic!("a pane with no agent record names no seat");
    };
    let after = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    assert!((before..=after).contains(&evidence.observed_at));
    assert_eq!(
        evidence,
        SeatEvidence {
            pane: Some(CallerPane {
                herdr_session_name: "messaging-build".into(),
                herdr_pane_id: "w1:p9".into(),
            }),
            pane_label: Some("zsh".into()),
            native_session: None,
            observed_at: evidence.observed_at,
        }
    );
    // The reply carries that evidence, typed, with no process.
    assert_eq!(
        refused,
        signal_flow::SeatEvidence {
            seat_pane_option: Some(signal_flow::SeatPane {
                herdr_session_name: signal_flow::HerdrSessionName("messaging-build".into()),
                herdr_pane_id: signal_flow::HerdrPaneId("w1:p9".into()),
            }),
            pane_label_option: Some(signal_flow::PaneLabel("zsh".into())),
            native_session_id_option: None,
            observed_at: refused.observed_at,
            process: signal_flow::Process {
                pid: flow_ethos::generated::library::Pid(0),
                started: flow_ethos::generated::library::Started(0),
            },
        }
    );
    assert!(
        (i64::try_from(before).unwrap()..=i64::try_from(after).unwrap())
            .contains(&refused.observed_at.0)
    );
    use crate::store::ReadsFlowRows;
    assert_eq!(
        fixture.nexus.store.flow_node(HARNESS_FLOW_ID).unwrap(),
        None
    );
}
