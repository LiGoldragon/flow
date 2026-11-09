//! BindSeat from a Codex flow's own command. A remote Codex TUI's commands
//! run under the shared app-server, in no pane; the only account of their
//! flow is the thread Codex stamps on them, and a subagent's command carries
//! the subagent's thread. Its rollout names the root thread of its session,
//! and the seat is the one Herdr agent running that root: the agent that
//! reports it, or a Codex agent that reports no session and whose pane
//! resumes it in its foreground.

use super::NexusFixture;
use super::waking::SpawnedProcess;
use crate::event_wait::{AwaitsDirectory, AwaitsPath};
use crate::fixture_executable::{FixtureExecutable, HoldsLatch, InstallsScript, Latch};
use crate::sending::{BindsSeats, IdentifiesSender};
use crate::store::AnnouncesLaunchChanges;
use crate::store::metaflow::{MutatesMetaflows, SleepsMetaflows};
use crate::store::{FlowStore, ManagesMetaflowState, ReadsFlowMemory};
use signal_flow::{Address, Aspect, Model, Refusal, Response};
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
                    codex_app_server_binary: app_server,
                    mcp_send_server_binary: "/fixture/send-server".into(),
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
    sending_address: Option<Address>,
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
                    native: "gpt-6-sol".into(),
                },
                meta_signal_flow::ConfigureModel {
                    model: Model::Luna,
                    native: "gpt-6-luna".into(),
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
    fn call(&self, query: signal_flow::Query, thread: Option<&str>) -> CodexCliCall {
        use crate::caller::{CallerProcess, LocatesCodexThread, ReadsProcess};
        use crate::sending::IdentifiesSender;
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
        let sending_address = self.fixture.nexus.sending_address(&process).ok();
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
fn a_codex_subagent_binds_its_flows_seat_which_herdr_reports_no_session_for() {
    let fixture = NexusFixture::new();
    fixture
        .nexus
        .store
        .configure_change(meta_signal_flow::ConfigurationChange::Models(vec![
            meta_signal_flow::ConfigureModel {
                model: Model::Sol,
                native: "gpt-6-sol".into(),
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
        topic: "context".into(),
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

    // Two panes resuming the root name no single seat.
    super::herdr_world(
        &fixture,
        vec![unreported_codex("w12:p1"), unreported_codex("w12:p2")],
        tui.0.id(),
    );
    super::assert_unidentified(
        fixture.nexus.bind_seat(address.clone(), Some(&subagent)),
        &subagent,
    );

    super::herdr_world(&fixture, vec![unreported_codex("w12:p1")], tui.0.id());
    // A thread whose session no pane runs names no seat.
    super::assert_unidentified(
        fixture.nexus.bind_seat(address.clone(), Some(&orphan)),
        &orphan,
    );
    // The subagent's command binds its flow's seat, under the root's claim.
    // Adoption announces the new live Codex thread so the Nexus's responder
    // subscribes to it and its first rest can drain already queued work.
    let before_adoption = fixture.nexus.store.launch_changes.current();
    assert_eq!(
        fixture.nexus.bind_seat(address.clone(), Some(&subagent)),
        Response::Bound("877549".into())
    );
    assert!(
        fixture.nexus.store.launch_changes.current() > before_adoption,
        "adoption wakes the Codex responder supervisor"
    );
    assert_eq!(fixture.nexus.sending_address(&subagent).unwrap(), address);
    // Message's authenticated Identify bridge uses the same root-aware
    // kernel caller resolution as direct Flow Send, never a claimed seat.
    use crate::metaflow::DispatchesMetaflowPeer;
    let (_message_child, message) = super::waking::message_peer(&fixture);
    assert_eq!(
        fixture
            .nexus
            .dispatch_metaflow_peer(
                signal_flow::Query::Identify(subagent.clone()),
                Some(&message)
            )
            .response,
        Response::Identified(address.clone())
    );
    super::assert_unidentified(
        fixture
            .nexus
            .dispatch_metaflow_peer(signal_flow::Query::Identify(orphan.clone()), Some(&message))
            .response,
        &orphan,
    );
    let awake = fixture.nexus.store.metaflow_awake_as("877549").unwrap();
    assert_eq!(awake.map(|metaflow| metaflow.address), Some(address));
}

/// The same real CLI peer is authenticated once for Send, Keep and Drop
/// through its configured Codex app-server ancestry.
#[test]
fn ordinary_codex_cli_send_keep_and_drop_share_process_authority() {
    use crate::sending::SendsRequests;
    use crate::store::ReadsFlowRows;
    use crate::store::handover::HoldsHandovers;
    use crate::store::metaflow::SeedsMetaflow;
    use crate::store::send_outcome::RecordsSendOutcomes;
    use crate::store::sent::NamesCodexAppServers;
    use flow_ethos::State;
    use signal_flow::{Message, Query, Request, Section, SendRequest};

    let terra = super::waking::address(Model::Terra);
    let luna = super::waking::address(Model::Luna);
    let configured = CodexCliWorld::new(true, Model::Terra);
    let app_servers = configured.fixture.nexus.store.codex_app_servers().unwrap();
    assert_eq!(
        app_servers,
        [fs::canonicalize(std::env::current_exe().unwrap()).unwrap()]
            .into_iter()
            .collect()
    );

    let bind = configured.call(Query::BindSeat(terra.clone()), Some(ROOT));
    assert_eq!(bind.answer, "Bound.877549");
    assert_eq!(bind.codex_thread.as_deref(), Some(ROOT));
    let node = configured
        .fixture
        .nexus
        .store
        .flow_node("877549")
        .unwrap()
        .expect("BindSeat registered the live Codex seat");
    assert_eq!(node.session_id, ROOT);
    assert_eq!(node.harness_kind, signal_flow::HarnessKind::Codex);
    assert_eq!(
        configured.fixture.nexus.live_awake_flow(&terra).as_deref(),
        Some("877549")
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
    let send_query = Query::Send(SendRequest {
        recipient: luna.clone(),
        sendable_message: super::sendable_message(Message::Soft(Request::Notice(
            "caller attribution diagnostic".into(),
        ))),
    });
    let send = configured.call(send_query, Some(ROOT));
    assert!(send.answer.starts_with("SendAccepted."), "{}", send.answer);
    assert_eq!(send.sending_address, Some(terra.clone()));
    let outcomes = configured.fixture.nexus.store.read_send_outcomes().unwrap();
    assert_eq!(
        outcomes
            .last()
            .and_then(|outcome| outcome.process_option.clone()),
        Some(send.process.clone()),
        "the production ordinary socket recorded the exact live kernel peer"
    );

    let keep = |address: Address| {
        Query::Keep(signal_flow::Keep_Data {
            address,
            section: Section {
                title: "diagnostic".into(),
                description: "caller attribution".into(),
                body: "test only".into(),
            },
        })
    };
    let prior_call = configured.call(
        Query::Keep(signal_flow::Keep_Data {
            address: terra.clone(),
            section: Section {
                title: "prior".into(),
                description: "must remain".into(),
                body: "existing handover".into(),
            },
        }),
        Some(ROOT),
    );
    assert!(
        prior_call.answer.starts_with("Kept."),
        "{}",
        prior_call.answer
    );
    let keep_call = configured.call(keep(terra.clone()), Some(ROOT));
    assert_eq!(keep_call.sending_address, Some(terra.clone()));
    assert!(
        keep_call.answer.starts_with("Kept."),
        "{}",
        keep_call.answer
    );
    let drop_call = configured.call(
        Query::Drop(signal_flow::Drop_Data {
            address: terra.clone(),
            title: "diagnostic".into(),
        }),
        Some(ROOT),
    );
    assert_eq!(drop_call.sending_address, Some(terra.clone()));
    assert!(
        drop_call.answer.starts_with("Dropped."),
        "{}",
        drop_call.answer
    );
    assert_eq!(
        configured
            .fixture
            .nexus
            .store
            .handover_headings(&terra)
            .unwrap()
            .into_iter()
            .map(|heading| heading.title)
            .collect::<Vec<_>>(),
        vec!["prior"]
    );

    // No SendServers authority: the identical real ancestry has no Codex
    // thread, BindSeat and Send refuse it, and neither registers a seat nor
    // mutates the target handover.
    let unconfigured = CodexCliWorld::new(false, Model::Terra);
    let unconfigured_bind = unconfigured.call(Query::BindSeat(terra.clone()), Some(ROOT));
    assert!(
        unconfigured_bind
            .answer
            .starts_with("Refused.Unidentified."),
        "{}",
        unconfigured_bind.answer
    );
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
    let unconfigured_send = unconfigured.call(
        Query::Send(SendRequest {
            recipient: luna.clone(),
            sendable_message: super::sendable_message(Message::Soft(Request::Notice(
                "unconfigured authority diagnostic".into(),
            ))),
        }),
        Some(ROOT),
    );
    assert!(
        unconfigured_send
            .answer
            .starts_with("SendRefused.Unidentified."),
        "{}",
        unconfigured_send.answer
    );
    let unconfigured_outcomes = unconfigured
        .fixture
        .nexus
        .store
        .read_send_outcomes()
        .unwrap();
    assert_eq!(
        unconfigured_outcomes
            .last()
            .and_then(|outcome| outcome.process_option.clone()),
        Some(unconfigured_send.process)
    );
    assert_eq!(unconfigured.fixture.nexus.live_awake_flow(&terra), None);
    assert!(
        unconfigured
            .fixture
            .nexus
            .store
            .handover_headings(&terra)
            .unwrap()
            .is_empty()
    );

    // A configured caller cannot adopt a different native session.
    let mismatched = CodexCliWorld::new(true, Model::Terra);
    let mismatch = mismatched.call(
        Query::Adopt(signal_flow::Adopt_Data {
            address: terra.clone(),
            native_session_id: "different-native-session".into(),
        }),
        Some(ROOT),
    );
    assert!(
        mismatch.answer.starts_with("Refused.Unidentified."),
        "{}",
        mismatch.answer
    );
    assert_eq!(mismatch.codex_thread.as_deref(), Some(ROOT));
    assert_eq!(mismatched.fixture.nexus.live_awake_flow(&terra), None);
    assert!(
        mismatched
            .fixture
            .nexus
            .store
            .handover_headings(&terra)
            .unwrap()
            .is_empty()
    );

    // A real Luna seat asking to Keep Terra is isolated from the positive
    // Terra case. Its exact peer authenticates as Luna and reaches the
    // unchanged ownership check.
    let wrong_seat = CodexCliWorld::new(true, Model::Luna);
    let luna_bind = wrong_seat.call(Query::BindSeat(luna.clone()), Some(ROOT));
    assert_eq!(luna_bind.answer, "Bound.877549");
    let luna_keep = wrong_seat.call(keep(terra.clone()), Some(ROOT));
    assert_eq!(luna_keep.sending_address, Some(luna.clone()));
    assert_eq!(luna_keep.answer, "KeepRefused.NotOwner");
    let luna_drop = wrong_seat.call(
        Query::Drop(signal_flow::Drop_Data {
            address: terra.clone(),
            title: "diagnostic".into(),
        }),
        Some(ROOT),
    );
    assert_eq!(luna_drop.answer, "DropRefused.NotOwner");
    assert!(
        wrong_seat
            .fixture
            .nexus
            .store
            .handover_headings(&terra)
            .unwrap()
            .is_empty()
    );

    // A driver carrying neither a pane nor a Codex thread stays unbound and
    // cannot mutate the Terra handover.
    let unbound = CodexCliWorld::new(true, Model::Terra);
    let unbound_keep = unbound.call(keep(terra.clone()), None);
    assert_eq!(unbound_keep.sending_address, None);
    assert_eq!(unbound_keep.codex_thread, None);
    assert_eq!(unbound_keep.answer, "KeepRefused.UnknownCaller");
    let unbound_drop = unbound.call(
        Query::Drop(signal_flow::Drop_Data {
            address: terra.clone(),
            title: "diagnostic".into(),
        }),
        None,
    );
    assert_eq!(unbound_drop.answer, "DropRefused.UnknownCaller");
    assert_eq!(unbound.fixture.nexus.live_awake_flow(&terra), None);
    assert!(
        unbound
            .fixture
            .nexus
            .store
            .handover_headings(&terra)
            .unwrap()
            .is_empty()
    );
}

/// Address-only process registration remains Send authority across a later
/// native owner, but it never becomes that owner's FlowOrigin or protected
/// Stop/Retire authority.
#[test]
fn a_registered_address_process_never_inherits_its_successor_flow() {
    use crate::Dispatches;
    use crate::caller::{AuthenticatesProcessAuthority, ProcessAuthority};
    use crate::protecting::{ChecksAsker, GuardedAct, GuardsProtectedAddresses};
    use crate::store::handover::HoldsHandovers;
    use crate::store::metaflow::SeedsMetaflow;
    use crate::store::{ReadsFlowRows, RegistersFlowIdentity};
    use flow_ethos::State;
    use signal_flow::{FlowLifecycle, HarnessKind, Metaflow, Origin, Query, Section};

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
            &peer,
            None,
        ),
        Ok(Response::Bound(_))
    ));
    assert_eq!(
        fixture.nexus.process_authority(&peer).unwrap(),
        ProcessAuthority::Sending(address.clone())
    );
    assert_eq!(fixture.nexus.sending_address(&peer).unwrap(), address);

    let mut successor = fixture.node();
    successor.flow_id = "successor".into();
    successor.session_id = "successor-native-session".into();
    successor.harness_kind = HarnessKind::Codex;
    successor.origin = Origin::Unbound;
    successor.flow_lifecycle = FlowLifecycle::Active;
    fixture.nexus.store.register_flow(successor).unwrap();
    fixture.nexus.store.seed_metaflow(Metaflow {
        address: address.clone(),
        state: State::Awake("successor".into()),
        past: vec![],
        queue: vec![],
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
    assert_eq!(fixture.nexus.sending_address(&peer).unwrap(), address);
    let asker = fixture.nexus.asker_of(Some(&peer));
    assert_eq!(asker.origin(), Origin::Unbound);
    assert!(!asker.may(GuardedAct::Stop, "successor"));
    assert!(!asker.may(GuardedAct::Retire, "successor"));
    assert_eq!(
        fixture.nexus.dispatch_asked(
            Query::Keep(signal_flow::Keep_Data {
                address: address.clone(),
                section: Section {
                    title: "forbidden".into(),
                    description: "old binder".into(),
                    body: "must not land".into(),
                },
            }),
            asker.clone(),
        ),
        Response::KeepRefused(signal_flow::HandoverRefusal::UnknownCaller)
    );
    assert_eq!(
        fixture
            .nexus
            .dispatch_asked(Query::Stop("successor".into()), asker),
        Response::StopRejected(signal_flow::StopRejection::Protected)
    );
    assert!(
        fixture
            .nexus
            .store
            .handover_headings(&address)
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        fixture
            .nexus
            .store
            .flow_node("successor")
            .unwrap()
            .unwrap()
            .flow_lifecycle,
        FlowLifecycle::Active
    );
}

/// Native/tool markers and stale kernel identities can only deny owner.
#[test]
fn failed_process_authority_evidence_never_becomes_owner() {
    use crate::caller::{AuthenticatesProcessAuthority, ProcessAuthority};
    use crate::protecting::{Asker, GuardsProtectedAddresses};

    let unconfigured = NexusFixture::new();
    let (_marked_child, marked) = command_of("unknown-native-session");
    assert_eq!(
        unconfigured.nexus.process_authority(&marked).unwrap(),
        ProcessAuthority::Rejected,
        "a CODEX_THREAD_ID is deny-owner evidence even with empty SendServers"
    );
    assert_eq!(
        unconfigured.nexus.asker_of(Some(&marked)),
        Asker::Ordinary(signal_flow::Origin::Unbound)
    );

    let configured = NexusFixture::new();
    configured.serves_codex_commands();
    let (_unknown_child, unknown) = command_of("unknown-native-session");
    assert_eq!(
        configured.nexus.process_authority(&unknown).unwrap(),
        ProcessAuthority::Rejected,
        "configured ancestry with no live root is known failed evidence"
    );
    assert_eq!(
        configured.nexus.asker_of(Some(&unknown)),
        Asker::Ordinary(signal_flow::Origin::Unbound)
    );

    let mut stale = unknown;
    stale.started += 1;
    assert_eq!(
        configured.nexus.process_authority(&stale).unwrap(),
        ProcessAuthority::Rejected
    );
    assert_eq!(
        configured.nexus.asker_of(Some(&stale)),
        Asker::Ordinary(signal_flow::Origin::Unbound)
    );

    // The fixture process itself is the service/terminal operator here. A
    // child would be below the configured app-server by construction and
    // must be rejected even if its own environment were scrubbed.
    let operator = FlowStore::process_identity_for_pid(std::process::id()).unwrap();
    assert_eq!(
        configured.nexus.process_authority(&operator).unwrap(),
        ProcessAuthority::Owner
    );
    assert_eq!(configured.nexus.asker_of(Some(&operator)), Asker::Owner);
}

#[test]
fn a_known_claude_seat_binds_from_root_and_child_without_codex_marks() {
    use super::waking::{CLAUDE_SEAT, claude_agent, claude_seat, metaflow};
    use super::{ControlsHerdrSnapshot, MarkedProcess};
    use crate::store::metaflow::SeedsMetaflow;
    use crate::store::{ReadsFlowRows, RegistersFlowIdentity};
    let fixture = NexusFixture::new();
    claude_seat(&fixture, 100);
    let mut role = super::mind_live();
    role.flow_id = CLAUDE_SEAT.into();
    let node = fixture.nexus.store.flow_node(CLAUDE_SEAT).unwrap().unwrap();
    fixture
        .nexus
        .store
        .register_flow_in_role(node, role)
        .unwrap();
    fixture.set_agents(vec![claude_agent(&fixture)]);
    fixture.nexus.store.seed_metaflow(metaflow(
        Model::Sol,
        flow_ethos::State::Awake(CLAUDE_SEAT.into()),
        vec![],
        vec![],
    ));
    let address = Address {
        aspect: Aspect::Mind,
        topic: "flow".into(),
        model: Model::Sol,
    };
    let child_file = fixture.directory.path().join("claude-command-child");
    let script = format!("sleep 30 & echo $! > {}; wait; :", child_file.display());
    let root = MarkedProcess::spawn("sh", &["-c", &script], "w1:p3");
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    let written = || {
        fs::read_to_string(&child_file)
            .ok()
            .and_then(|text| text.trim().parse::<u32>().ok())
    };
    assert!(
        fixture
            .directory
            .path()
            .until_holds(deadline, &mut || written().is_some()),
        "the child's pid is written"
    );
    let child_pid = written().expect("the child's pid");
    for pid in [root.0.id(), child_pid] {
        let peer = FlowStore::process_identity_for_pid(pid).unwrap();
        assert_eq!(
            fixture.nexus.bind_seat(address.clone(), Some(&peer)),
            Response::Bound(CLAUDE_SEAT.into())
        );
        let mut forged = peer;
        forged.started += 1;
        assert!(matches!(
            fixture.nexus.bind_seat(address.clone(), Some(&forged)),
            Response::Refused(Refusal::Unidentified(_))
        ));
    }
    let stranger = MarkedProcess::spawn("sleep", &["30"], "unknown-pane");
    let unknown = FlowStore::process_identity_for_pid(stranger.0.id()).unwrap();
    assert!(matches!(
        fixture.nexus.bind_seat(address, Some(&unknown)),
        Response::Refused(Refusal::Unidentified(_))
    ));
    let _ = std::process::Command::new("kill")
        .arg(child_pid.to_string())
        .status();
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
                "pid": process_id, "sessionId": session, "procStart": started.to_string(),
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
                    native: "claude-opus-5-5".into(),
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
        topic: "flow".into(),
        model: Model::Opus,
    }
}

/// On an empty store, BindSeat from a Claude pane adopts the seat: the
/// caller's pane, its Herdr agent record, the Claude session the record
/// reports (or Claude's own session file names) and the process running it
/// make the seat, bound to the address under the session's Flow ID. A
/// process in the same pane that is not the Claude process's own refuses.
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
        let stranger = MarkedProcess::spawn("sleep", &["30"], "w1:p3");
        let stranger = FlowStore::process_identity_for_pid(stranger.0.id()).unwrap();
        assert!(
            matches!(
                fixture.nexus.bind_seat(address.clone(), Some(&stranger)),
                Response::Refused(Refusal::Unidentified(_))
            ),
            "{reported:?}: a process the Claude process did not start names no seat"
        );

        let peer = FlowStore::process_identity_for_pid(claude.0.id()).unwrap();
        assert_eq!(
            fixture.nexus.bind_seat(address.clone(), Some(&peer)),
            Response::Bound(HARNESS_FLOW_ID.into()),
            "{reported:?}: the Claude seat binds under its session's Flow ID"
        );
        assert_eq!(fixture.nexus.sending_address(&peer).unwrap(), address);
        let node = fixture
            .nexus
            .store
            .flow_node(HARNESS_FLOW_ID)
            .unwrap()
            .expect("the seat's flow is registered");
        assert_eq!(node.session_id, HARNESS_SESSION);
        assert_eq!(node.harness_kind, signal_flow::HarnessKind::Claude);
        assert_eq!(
            fixture.nexus.live_awake_flow(&address).as_deref(),
            Some(HARNESS_FLOW_ID)
        );
    }
}

/// Adopt authenticates an unbound socket peer before adding it. The supplied
/// session is checked before the known-seat idempotency shortcut, so the same
/// live seat cannot use a different session after its first adoption.
#[test]
fn an_unbound_claude_seat_adopts_only_its_authenticated_session() {
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
    let peer = FlowStore::process_identity_for_pid(claude.0.id()).unwrap();
    let request = |session: &str| signal_flow::Adopt_Data {
        address: address.clone(),
        native_session_id: session.into(),
    };

    assert!(matches!(
        fixture
            .nexus
            .adopt(request("different-session"), Some(&peer)),
        Response::Refused(Refusal::Unidentified(_))
    ));
    assert_eq!(fixture.nexus.live_awake_flow(&address), None);

    for _ in 0..2 {
        assert_eq!(
            fixture.nexus.adopt(request(HARNESS_SESSION), Some(&peer)),
            Response::Adopted(HARNESS_FLOW_ID.into())
        );
    }
    assert!(matches!(
        fixture
            .nexus
            .adopt(request("different-session"), Some(&peer)),
        Response::Refused(Refusal::Unidentified(_))
    ));
    assert_eq!(
        fixture.nexus.live_awake_flow(&address).as_deref(),
        Some(HARNESS_FLOW_ID)
    );
}

/// A pane Herdr holds no agent record for names no seat: BindSeat refuses,
/// and what the pane showed is kept as raw evidence (its pane, its label,
/// no session, the time it was read), never made into an identity.
#[test]
fn a_pane_with_no_agent_record_refuses_with_its_evidence() {
    use super::MarkedProcess;
    use crate::caller::{CallerPane, CallerProcess};
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
    let peer = FlowStore::process_identity_for_pid(shell.0.id()).unwrap();
    let before = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let reply = fixture
        .nexus
        .bind_seat(psyche_flow_secondary(), Some(&peer));
    let Response::Refused(Refusal::Unidentified(refused)) = reply else {
        panic!("a pane with no agent record names no seat: {reply:?}");
    };
    assert_eq!(refused.process, peer);
    let Err(evidence) = fixture.nexus.seat_of(CallerProcess {
        process_id: shell.0.id(),
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
    // The reply carries that evidence, typed, with the process.
    assert_eq!(
        refused,
        signal_flow::SeatEvidence {
            seat_pane_option: Some(signal_flow::SeatPane {
                herdr_session_name: "messaging-build".into(),
                herdr_pane_id: "w1:p9".into(),
            }),
            pane_label_option: Some("zsh".into()),
            native_session_id_option: None,
            observed_at: refused.observed_at,
            process: peer.clone(),
        }
    );
    assert!(
        (i64::try_from(before).unwrap()..=i64::try_from(after).unwrap())
            .contains(&refused.observed_at)
    );
    use crate::store::ReadsFlowRows;
    assert_eq!(
        fixture.nexus.store.flow_node(HARNESS_FLOW_ID).unwrap(),
        None
    );
}
