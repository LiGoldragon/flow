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
    assert_eq!(
        fixture.nexus.bind_seat(address.clone(), Some(&subagent)),
        Response::Refused(Refusal::Unidentified(subagent.clone()))
    );

    super::herdr_world(&fixture, vec![unreported_codex("w12:p1")], tui.0.id());
    // A thread whose session no pane runs names no seat.
    assert_eq!(
        fixture.nexus.bind_seat(address.clone(), Some(&orphan)),
        Response::Refused(Refusal::Unidentified(orphan.clone()))
    );
    // The subagent's command binds its flow's seat, under the root's claim.
    assert_eq!(
        fixture.nexus.bind_seat(address.clone(), Some(&subagent)),
        Response::Bound("877549".into())
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
    assert_eq!(
        fixture
            .nexus
            .dispatch_metaflow_peer(signal_flow::Query::Identify(orphan.clone()), Some(&message))
            .response,
        Response::Refused(Refusal::Unidentified(orphan.clone()))
    );
    let awake = fixture.nexus.store.metaflow_awake_as("877549").unwrap();
    assert_eq!(awake.map(|metaflow| metaflow.address), Some(address));
}
