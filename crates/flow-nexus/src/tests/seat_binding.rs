//! BindSeat from a Codex flow's own command. A remote Codex TUI's commands
//! run under the shared app-server, in no pane; the only account of their
//! flow is the thread Codex stamps on them, and a subagent's command carries
//! the subagent's thread. Its rollout names the root thread of its session,
//! and the seat is the one Herdr agent running that root: the agent that
//! reports it, or a Codex agent that reports no session and whose pane
//! resumes it in its foreground.

use super::NexusFixture;
use super::waking::SpawnedProcess;
use crate::fixture_executable::{FixtureExecutable, InstallsScript};
use crate::sending::{BindsSeats, IdentifiesSender};
use crate::store::AnnouncesLaunchChanges;
use crate::store::metaflow::SleepsMetaflows;
use crate::store::{FlowStore, ManagesMetaflowState, ReadsFlowMemory};
use signal_flow::{Address, Aspect, Layer, Refusal, Response};
use std::fs;

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
    let command = SpawnedProcess(
        std::process::Command::new("sleep")
            .arg("30")
            .env_remove("HERDR_PANE_ID")
            .env_remove("HERDR_SESSION")
            .env("CODEX_THREAD_ID", thread)
            .spawn()
            .unwrap(),
    );
    let process = FlowStore::process_identity_for_pid(command.0.id()).unwrap();
    (command, process)
}

#[test]
fn a_codex_subagent_binds_its_flows_seat_which_herdr_reports_no_session_for() {
    let fixture = NexusFixture::new();
    fixture
        .nexus
        .store
        .configure_change(meta_signal_flow::ConfigurationChange::Models(vec![
            meta_signal_flow::ConfigureModel {
                layer: Layer::Secondary,
                aspect: Aspect::Mind,
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
        layer: Layer::Secondary,
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
        Layer::Secondary,
        flow_ethos::State::Awake(CLAUDE_SEAT.into()),
        vec![],
        vec![],
    ));
    let address = Address {
        aspect: Aspect::Mind,
        topic: "flow".into(),
        layer: Layer::Secondary,
    };
    let child_file = fixture.directory.path().join("claude-command-child");
    let script = format!("sleep 30 & echo $! > {}; wait; :", child_file.display());
    let root = MarkedProcess::spawn("sh", &["-c", &script], "w1:p3");
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    let child_pid = loop {
        if let Ok(text) = fs::read_to_string(&child_file)
            && let Ok(pid) = text.trim().parse::<u32>()
        {
            break pid;
        }
        assert!(std::time::Instant::now() < deadline);
        std::thread::yield_now();
    };
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
                    layer: Layer::Secondary,
                    aspect: Aspect::Psyche,
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
        "name":"claude-6aafcaae640d8089d70ae839","display_agent":"{ Psyche flow Secondary }",
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
        layer: Layer::Secondary,
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
            "{ Psyche flow Secondary }",
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
