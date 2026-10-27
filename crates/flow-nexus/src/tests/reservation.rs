//! A Claude launch's FlowId exists before its harness: Reserve claims it
//! for the session Flow chose and holds the flow in Memory, so the
//! harness hook's first `Report` lands even though the flow is not yet
//! registered. A FlowId Flow never reserved or registered stays refused,
//! and a launch refused after Reserve gives back the flow and the claim.

use super::NexusFixture;
use crate::Dispatches;
use crate::fixture_executable::{FixtureExecutable, InstallsScript};
use meta_signal_flow::{Query as MetaQuery, Response as MetaResponse};
use signal_flow::{Event, Query, Report_Data, Response, StartRejection};
use std::fs;

impl NexusFixture {
    /// A `flow-id` stand-in that claims a Claude session as the real one
    /// does for an unclaimed lane: the alias is the identity's first six
    /// hex digits, the marker is written under the flows root, the alias
    /// printed. Every call is logged.
    pub(super) fn claims_flow_ids(&self) {
        let helper_root = self.directory.path().join("native-transcripts");
        fs::create_dir_all(&helper_root).expect("flow-id root");
        let calls = self.directory.path().join("flow-id-calls");
        FixtureExecutable {
            path: helper_root.join("flow-id"),
        }
        .install(&format!(
            r#"#!/bin/sh
printf '%s\n' "$*" >> '{calls}'
[ "$1" = claude ] && [ "$2" = --flows-root ] && [ "$4" = --parent-session ] || exit 2
identity=$(printf '%s' "$5" | tr -d -)
alias=$(printf '%s' "$identity" | cut -c1-6)
: > "$3/.$alias.flow-id.lock"
mkdir -p "$3/$alias"
printf 'version=1\nharness=claude\nidentity=%s\nalias=%s\nuuid-version=uuid-v5\n' "$identity" "$alias" > "$3/.$alias.flow-id"
printf '%s\n' "$alias"
"#,
            calls = calls.display()
        ));
    }

    /// A Herdr stand-in that logs every call and refuses it.
    fn herdr_logging_every_call(&self) -> std::path::PathBuf {
        let log = self.directory.path().join("herdr-every-call.log");
        FixtureExecutable {
            path: self.snapshot_program.clone(),
        }
        .install(&format!(
            "#!/bin/sh\nprintf '%s\\n' \"$*\" >> '{}'\nexit 64\n",
            log.display()
        ));
        log
    }

    fn flow_id_calls(&self) -> Vec<String> {
        fs::read_to_string(self.directory.path().join("flow-id-calls"))
            .unwrap_or_default()
            .lines()
            .map(str::to_owned)
            .collect()
    }

    fn report(&self, flow_id: &str, event: Event) -> Response {
        self.nexus.dispatch(Query::Report(Report_Data {
            flow_id: flow_id.into(),
            event,
        }))
    }
}

#[test]
fn a_claude_launch_refused_after_reserve_gives_back_its_flow_and_claim() {
    let fixture = NexusFixture::new();
    fixture.claims_flow_ids();
    // Herdr cannot open the pane: the launch is refused after Reserve,
    // before any harness exists.
    let _ = fs::remove_file(&fixture.snapshot_program);
    let mut launch = fixture.staged_launch("reserved-request", None);
    launch.profile.model_name = "claude-fixture".into();
    fixture.configure_model_of(&launch.profile);
    assert_eq!(
        fixture.nexus.dispatch(Query::Start(launch.request())),
        Response::StartRejected(StartRejection::NativeLaunchRefused(
            signal_flow::NativeLaunchRefusal {
                native_launch_stage: signal_flow::NativeLaunchStage::Open,
                native_launch_failure: signal_flow::NativeLaunchFailure::AdapterRefused,
            }
        ))
    );

    let calls = fixture.flow_id_calls();
    assert_eq!(calls.len(), 1, "one claim, at Reserve: {calls:?}");
    let fields = calls[0].split(' ').collect::<Vec<_>>();
    assert_eq!(fields[0], "claude");
    assert_eq!(fields[3], "--parent-session");
    let flows_root = std::path::Path::new(fields[2]);
    let session = fields[4];
    // The session Flow chose is a canonical UUIDv5: version nibble 5,
    // RFC 4122 variant.
    assert_eq!(session.len(), 36);
    assert_eq!(&session[14..15], "5");
    assert!(matches!(&session[19..20], "8" | "9" | "a" | "b"));
    let flow_id = session.replace('-', "")[..6].to_owned();

    // The refused launch's flow is no longer held: its hook's Report is
    // refused like any unknown FlowId, and the claim is given back.
    assert_eq!(
        fixture
            .nexus
            .dispatch_meta(MetaQuery::ReadEvents(flow_id.clone())),
        MetaResponse::ReadEventsRejected(meta_signal_flow::ReadEventsRejected_Data::UnknownFlow)
    );
    assert_eq!(
        fixture.report(&flow_id, Event::Started(None)),
        Response::Refused(signal_flow::Refusal::Unknown(
            signal_flow::Unknown_Data::FlowId(flow_id.clone())
        ))
    );
    assert!(!flows_root.join(format!(".{flow_id}.flow-id")).exists());
    assert!(!flows_root.join(&flow_id).exists());

    // The settled launch is not reserved again.
    assert_eq!(
        fixture.nexus.dispatch(Query::Start(launch.request())),
        Response::StartRejected(StartRejection::NativeLaunchRefused(
            signal_flow::NativeLaunchRefusal {
                native_launch_stage: signal_flow::NativeLaunchStage::Open,
                native_launch_failure: signal_flow::NativeLaunchFailure::AdapterRefused,
            }
        ))
    );
    assert_eq!(fixture.flow_id_calls().len(), 1);
}

#[test]
fn a_claude_launch_whose_flow_id_cannot_be_claimed_is_refused_before_any_pane() {
    let fixture = NexusFixture::new();
    // No flow-id helper: the claim at Reserve fails.
    let mut launch = fixture.staged_launch("unclaimed-request", None);
    launch.profile.model_name = "claude-fixture".into();
    fixture.configure_model_of(&launch.profile);
    let log = fixture.herdr_logging_every_call();
    assert_eq!(
        fixture.nexus.dispatch(Query::Start(launch.request())),
        Response::StartRejected(StartRejection::BindingRefused(
            signal_flow::BindingRefusal::ClaimUnavailable,
        ))
    );
    assert_eq!(fs::read_to_string(log).unwrap_or_default(), "");
}

#[test]
fn a_codex_launch_reserves_no_flow_id() {
    let fixture = NexusFixture::new();
    fixture.claims_flow_ids();
    let _ = fs::remove_file(&fixture.snapshot_program);
    let launch = fixture.staged_launch("codex-request", None);
    assert_eq!(
        fixture.nexus.dispatch(Query::Start(launch.request())),
        Response::StartRejected(StartRejection::NativeLaunchRefused(
            signal_flow::NativeLaunchRefusal {
                native_launch_stage: signal_flow::NativeLaunchStage::Open,
                native_launch_failure: signal_flow::NativeLaunchFailure::AdapterRefused,
            }
        ))
    );
    assert!(fixture.flow_id_calls().is_empty());
}

/// Herdr refuses a Claude start: the Start is answered HarnessSpawnRefused
/// carrying Herdr's own error code and message, LaunchStatus keeps them, and
/// the composed system prompt never reached Herdr's command.
#[test]
fn a_claude_start_herdr_refuses_is_answered_with_herdrs_own_refusal() {
    let fixture = NexusFixture::new();
    fixture.claims_flow_ids();
    let log = fixture.directory.path().join("herdr-calls.log");
    let marker = fixture.directory.path().join("marker-output");
    FixtureExecutable {
        path: fixture.snapshot_program.clone(),
    }
    .install(&format!(
        r#"#!/bin/sh
printf '%s\n' "$*" >> '{log}'
case "$*" in
  *"workspace create"*) printf '%s\n' '{{"result":{{"workspace":{{"workspace_id":"w7"}},"root_pane":{{"pane_id":"w7:p1","terminal_id":"term-native"}}}}}}' ;;
  *"pane run"*) case "$6" in unset\ *) sh -c "$6" > '{marker}' ;; export\ FLOW_CLAUDE_READY_SOCKET=*) sh -c "$6" ;; *) printf '%s\n' '{{"error":{{"code":"invalid_agent_argument","message":"agent arguments cannot be encoded safely for the target shell"}},"id":"cli:pane:run"}}' >&2; exit 1 ;; esac ;;
  *"pane wait-output"*) line=$(grep -F FLOW_CLAUDE_ENV_READY_ '{marker}'); printf '{{"result":{{"pane_id":"w7:p1","matched_line":"%s"}}}}\n' "$line" ;;
  *"pane close"*) printf '%s\n' '{{"result":{{}}}}' ;;
  *) exit 64 ;;
esac
"#,
        log = log.display(),
        marker = marker.display(),
    ));
    let mut launch = fixture.staged_launch("herdr-refused-request", None);
    launch.profile.model_name = "claude-fixture".into();
    fixture.configure_model_of(&launch.profile);
    let expected = StartRejection::HarnessSpawnRefused(signal_flow::HarnessSpawnRefusal {
        spawn_refusal_cause: signal_flow::SpawnRefusalCause::Herdr(signal_flow::HerdrRefusal {
            herdr_error_code_option: Some("invalid_agent_argument".into()),
            herdr_error_message: "agent arguments cannot be encoded safely for the target shell"
                .into(),
        }),
        opened_thread_option: None,
    });
    assert_eq!(
        fixture.nexus.dispatch(Query::Start(launch.request())),
        Response::StartRejected(expected.clone())
    );
    let Response::LaunchInspected(inspection) = fixture
        .nexus
        .dispatch(Query::LaunchStatus("herdr-refused-request".into()))
    else {
        panic!("the refused launch is inspected");
    };
    assert_eq!(
        inspection.launch_status_outcome_option,
        Some(signal_flow::LaunchStatusOutcome::StartRejected(expected))
    );
    let calls = fs::read_to_string(&log).expect("Herdr calls");
    let start = calls
        .lines()
        .find(|line| line.contains("pane run") && line.contains("/fixture/claude"))
        .expect("the start reached Herdr");
    assert!(start.contains(" '--system-prompt-file' '/proc/"));
    assert!(!start.contains("--system-prompt "));
}

impl NexusFixture {
    /// A Codex launch left at its native intent with its pane created and
    /// its harness start requested, as a Nexus that died after Spawn left
    /// it; Herdr shows the pane's Codex agent ready and reports no agent
    /// session, as for a remote-resumed TUI; the `flow-id` stand-in claims
    /// the fixture's Codex flow for exactly the staged thread.
    fn codex_launch_left_after_spawn(&self, launch_request_id: &str) -> super::StagedLaunch {
        use crate::herdr::launch::ChecksComposedLaunch;
        use crate::store::RecordsCreatedLaunchPane;
        let mut launch = self.staged_launch(launch_request_id, None);
        // The pane Open created carries the launch's own agent name.
        launch.pane.herdr_agent_name = launch.composed.launch_agent_name();
        launch.reserve(&self.nexus);
        launch.record_native_intent(&self.nexus);
        assert!(
            self.nexus
                .store
                .record_created_launch_pane(launch.pane.clone())
                .unwrap()
        );
        assert!(
            self.nexus
                .store
                .request_harness_start(launch_request_id)
                .unwrap()
                .is_some()
        );
        let agent = serde_json::json!({"result":{"agent":{
            "name":launch.pane.herdr_agent_name,
            "agent":"codex",
            "workspace_id":"fixture-workspace",
            "pane_id":"w1:p1",
            "terminal_id":"fixture-terminal",
            "interactive_ready":true
        }}});
        FixtureExecutable {
            path: self.snapshot_program.clone(),
        }
        .install(&format!(
            "#!/bin/sh\ncase \"$3 $4\" in\n  \"agent wait\") ;;\n  \"agent get\") printf '%s\\n' '{agent}' ;;\n  *) exit 64 ;;\nesac\n"
        ));
        let helper_root = self.directory.path().join("native-transcripts");
        fs::create_dir_all(&helper_root).expect("flow-id root");
        FixtureExecutable {
            path: helper_root.join("flow-id"),
        }
        .install(&format!(
            "#!/bin/sh\n[ \"$1\" = codex ] && [ \"$2\" = --flows-root ] && [ \"$CODEX_SESSION_ID\" = '{}' ] || exit 2\nprintf '908786\\n'\n",
            super::STAGED_SESSION
        ));
        launch
    }
}

/// A Codex launch recovered after its harness start binds to the thread its
/// Spawn recorded: Herdr reports no session for the remote-resumed TUI, and
/// the recognized pane, the claim and that thread are the binding.
#[test]
fn a_codex_launch_recovered_after_its_start_binds_to_the_recorded_thread() {
    use crate::store::opened_thread::RecordsOpenedThreads;
    let fixture = NexusFixture::new();
    let launch = fixture.codex_launch_left_after_spawn("recovered-request");
    assert!(
        fixture
            .nexus
            .store
            .record_opened_thread("recovered-request", super::STAGED_SESSION)
            .unwrap()
    );
    let Response::LaunchPending(attempt) = fixture.nexus.dispatch(Query::Start(launch.request()))
    else {
        panic!("the recovered launch is pending with its binding");
    };
    assert_eq!(
        attempt.launch_attempt_phase,
        signal_flow::LaunchAttemptPhase::NativeBound
    );
    let binding = attempt
        .native_launch_binding_option
        .expect("the recovered binding is recorded");
    assert_eq!(binding.native_session_id, super::STAGED_SESSION);
    assert_eq!(binding.flow_id, "908786");
}

/// With no thread recorded, the same recovery has only Herdr's report,
/// which names no session: the Start is refused at Bind, the stage and
/// category on the wire, and LaunchStatus reports the same.
#[test]
fn a_codex_launch_recovered_without_its_thread_is_refused_at_bind_on_the_wire() {
    let fixture = NexusFixture::new();
    let launch = fixture.codex_launch_left_after_spawn("unrecorded-request");
    let expected = StartRejection::NativeLaunchRefused(signal_flow::NativeLaunchRefusal {
        native_launch_stage: signal_flow::NativeLaunchStage::Bind,
        native_launch_failure: signal_flow::NativeLaunchFailure::NativeSessionMissing,
    });
    assert_eq!(
        fixture.nexus.dispatch(Query::Start(launch.request())),
        Response::StartRejected(expected.clone())
    );
    let Response::LaunchInspected(inspection) = fixture
        .nexus
        .dispatch(Query::LaunchStatus("unrecorded-request".into()))
    else {
        panic!("the refused launch is inspected");
    };
    assert_eq!(
        inspection.launch_status_outcome_option,
        Some(signal_flow::LaunchStatusOutcome::StartRejected(expected))
    );
}

/// A registration refused at a stage is settled with that stage, and
/// LaunchStatus reports it: here the native target was not exact.
#[test]
fn a_refused_registration_is_reported_with_its_stage() {
    use crate::herdr::launch::{NativeTargetRefusal, RegistrationRefusal};
    use crate::launching::LaunchesFlows;
    let fixture = NexusFixture::new();
    let launch = fixture.staged_launch("registration-request", None);
    launch.reserve(&fixture.nexus);
    let refused = RegistrationRefusal::NativeTarget(NativeTargetRefusal::IdentityInexact);
    let expected = StartRejection::RegistrationRefused((&refused).into());
    assert_eq!(
        expected,
        StartRejection::RegistrationRefused(signal_flow::RegistrationRefusal::NativeTarget(
            signal_flow::NativeTargetRefusal::IdentityInexact
        ))
    );
    assert_eq!(
        fixture.nexus.settle(
            "registration-request",
            Response::StartRejected(expected.clone())
        ),
        Response::StartRejected(expected.clone())
    );
    let Response::LaunchInspected(inspection) = fixture
        .nexus
        .dispatch(Query::LaunchStatus("registration-request".into()))
    else {
        panic!("the refused launch is inspected");
    };
    assert_eq!(
        inspection.launch_status_outcome_option,
        Some(signal_flow::LaunchStatusOutcome::StartRejected(expected))
    );
}

/// A Codex Start naming a Claude leave-out is refused at composition, before
/// Flow reserves its launch or asks Herdr for anything.
#[test]
fn a_codex_start_naming_a_claude_leave_out_is_refused_before_reserve_or_open() {
    use crate::store::ReadsLaunchAttempt;
    let fixture = NexusFixture::new();
    fixture.claims_flow_ids();
    let log = fixture.herdr_logging_every_call();
    let mut launch = fixture.staged_launch("codex-claude-leave-out", None);
    launch.profile.leave_outs_option = Some(vec![signal_flow::LeaveOut::ClaudeMd]);
    assert_eq!(
        fixture.nexus.dispatch(Query::Start(launch.request())),
        Response::StartRejected(StartRejection::LeaveOutRefused(
            signal_flow::LeaveOutRefusal::NotForHarness(signal_flow::LeaveOut::ClaudeMd)
        ))
    );
    assert_eq!(
        fixture
            .nexus
            .store
            .launch_attempt("codex-claude-leave-out")
            .expect("attempts read"),
        None,
        "no launch was reserved"
    );
    assert!(fixture.flow_id_calls().is_empty());
    assert_eq!(fs::read_to_string(log).unwrap_or_default(), "");
}
