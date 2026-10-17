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
    assert_eq!(
        fixture.nexus.dispatch(Query::Start(launch.request())),
        Response::StartRejected(StartRejection::NativeLaunchRefused)
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
        fixture.report(&flow_id, Event::Started),
        Response::Refused(signal_flow::Refusal::Unknown(
            signal_flow::Unknown_Data::FlowId(flow_id.clone())
        ))
    );
    assert!(!flows_root.join(format!(".{flow_id}.flow-id")).exists());
    assert!(!flows_root.join(&flow_id).exists());

    // The settled launch is not reserved again.
    assert_eq!(
        fixture.nexus.dispatch(Query::Start(launch.request())),
        Response::StartRejected(StartRejection::NativeLaunchRefused)
    );
    assert_eq!(fixture.flow_id_calls().len(), 1);
}

#[test]
fn a_claude_launch_whose_flow_id_cannot_be_claimed_is_refused_before_any_pane() {
    let fixture = NexusFixture::new();
    // No flow-id helper: the claim at Reserve fails.
    let mut launch = fixture.staged_launch("unclaimed-request", None);
    launch.profile.model_name = "claude-fixture".into();
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
        Response::StartRejected(StartRejection::NativeLaunchRefused)
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
  *"pane run"*) sh -c "$6" > '{marker}' ;;
  *"pane wait-output"*) line=$(grep -F FLOW_CLAUDE_ENV_READY_ '{marker}'); printf '{{"result":{{"pane_id":"w7:p1","matched_line":"%s"}}}}\n' "$line" ;;
  *"agent start"*) printf '%s\n' '{{"error":{{"code":"invalid_agent_argument","message":"agent arguments cannot be encoded safely for the target shell"}},"id":"cli:agent:start"}}' >&2; exit 1 ;;
  *"pane close"*) printf '%s\n' '{{"result":{{}}}}' ;;
  *) exit 64 ;;
esac
"#,
        log = log.display(),
        marker = marker.display(),
    ));
    let mut launch = fixture.staged_launch("herdr-refused-request", None);
    launch.profile.model_name = "claude-fixture".into();
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
        .find(|line| line.contains("agent start"))
        .expect("the start reached Herdr");
    assert!(start.contains(" --system-prompt-file /proc/"));
    assert!(!start.contains("--system-prompt "));
}
