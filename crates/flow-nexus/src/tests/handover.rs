//! A successor's first prompt ends with its predecessor's handover: a
//! refresh's successor and a waking's successor are given
//! `flows/<predecessor>/handover.md` of the launch root, whole, as the last
//! bytes of the prompt. An absent file leaves the prompt as it was; a file
//! that cannot be read is logged and the launch goes on without it. A Restart
//! names no predecessor and has no first prompt, so it is given none.

use super::NexusFixture;
use super::waking::{
    ReadsFreshWorld, WakesFreshClaude, address, ended_claude, launched_claude_ended_by, metaflow,
};
use crate::generated::operation::{Operation, Outcome};
use crate::handover::{Handover, HandoverFile, YieldsHandover};
use crate::performing::Performs;
use crate::store::ManagesMetaflowState as _;
use crate::store::RecordsLaunchOutcome as _;
use crate::store::metaflow::SeedsMetaflow;
use crate::waking::HoldsWakingMetaflows;
use flow_ethos::State;
use meta_signal_flow::{ConfigurationChange, ConfigureMetaflow, Continuity};
use signal_flow::{Model, Query, Refreshed, Request, Response};
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;

/// The handover the tests leave: markdown with a heading, non-ASCII text,
/// blank lines, indentation and a trailing blank line, so a trimmed or
/// re-encoded copy differs from it.
const TEXT: &str = "# Handover\n\nÉtat: the work is `half done` — résumé below.\n\n    indented\n\n- one\n- two\n\n";

/// What a predecessor left under `flows/<flow>/`.
#[derive(Clone, Copy)]
enum Left<'a> {
    Nothing,
    Text(&'a [u8]),
    /// A directory where the file should be: it cannot be read.
    Directory,
}

fn leave(root: &Path, flow: &str, left: Left<'_>) {
    let directory = root.join("flows").join(flow);
    match left {
        Left::Nothing => {}
        Left::Text(bytes) => {
            fs::create_dir_all(&directory).unwrap();
            fs::write(directory.join("handover.md"), bytes).unwrap();
        }
        Left::Directory => fs::create_dir_all(directory.join("handover.md")).unwrap(),
    }
}

/// The first prompt of the fresh flow a waking starts after the flow
/// abc123 slept, with `left` under abc123.
fn woken_first_turn(left: Left<'_>) -> String {
    let mut fixture = NexusFixture::new();
    ended_claude(&fixture);
    leave(fixture.directory.path(), "abc123", left);
    let at = address(Model::Sol);
    fixture.nexus.store.seed_metaflow(metaflow(
        Model::Sol,
        State::Asleep,
        vec!["abc123"],
        vec![Request::Order("A".into())],
    ));
    let world = fixture.fresh_claude_world();
    let lease = fixture.nexus.waking.hold(&at).expect("the wake holds it");
    let Outcome::Woken(_) = fixture.nexus.perform(Operation::Wake(at)) else {
        panic!(
            "the wake starts a fresh flow; Herdr calls: {}",
            world.calls()
        );
    };
    drop(lease);
    let turns = world.user_turns(&fixture);
    turns.into_iter().next().expect("the first turn")
}

/// The first prompt of the successor a Refresh of a Maintained metaflow
/// starts, with `left` under the ending flow abc123.
fn refreshed_first_turn(left: Left<'_>) -> String {
    let mut fixture = NexusFixture::new();
    let seat = launched_claude_ended_by(&fixture, |_| true);
    leave(fixture.directory.path(), "abc123", left);
    let at = address(Model::Sol);
    let world = fixture.fresh_claude_world();
    // The ending flow's own launch has settled, as it has in a running Nexus.
    fixture
        .nexus
        .store
        .record_launch_outcome(
            &seat.profile.launch_request_id,
            crate::store::LaunchOutcome::Started(crate::LaunchedRecord {
                flow_id: "abc123".into(),
                session_id: seat.session.clone(),
                origin: signal_flow::Origin::Unbound,
            }),
        )
        .unwrap();
    // Herdr shows the ending flow's agent in its pane, w1:p3, until the pane
    // is closed.
    fs::write(world.state.join("restart-world"), "").unwrap();
    fs::write(world.state.join("old-pane"), "w1:p3").unwrap();
    fs::write(
        world.state.join("old-agent"),
        serde_json::json!({
            "name": seat.profile.launch_request_id,
            "agent": "claude",
            "agent_status": "idle",
            "workspace_id": "w1",
            "tab_id": "w1:t1",
            "pane_id": "w1:p3",
            "terminal_id": "term-old",
            "interactive_ready": true,
            "agent_session": {
                "source": "herdr:claude", "agent": "claude",
                "kind": "id", "value": seat.session,
            }
        })
        .to_string(),
    )
    .unwrap();
    fixture
        .nexus
        .store
        .configure_change(ConfigurationChange::Metaflows(vec![ConfigureMetaflow {
            root_option: None,
            address: at.clone(),
            system_modules: vec!["spirit-core".into(), "intent-flow".into()],
            user_modules: vec!["operation-main-flow".into()],
            flow_kind: signal_flow::FlowKind::Regular,
            tool_set: signal_flow::ToolSet::Default,
            continuity: Continuity::Maintained,
            purpose: String::new(),
            records: vec![],
        }]))
        .expect("metaflow declared");
    fixture.nexus.store.seed_metaflow(metaflow(
        Model::Sol,
        State::Awake("abc123".into()),
        vec![],
        vec![],
    ));
    use crate::Dispatches;
    assert_eq!(
        fixture.nexus.dispatch(Query::Refresh(at.clone())),
        Response::Refreshed(Refreshed {
            address: at,
            flow_id: "abc123".into(),
        }),
        "Herdr calls: {}",
        world.calls()
    );
    assert_eq!(world.claude_starts(), 1, "{}", world.calls());
    let turns = world.user_turns(&fixture);
    turns.into_iter().next().expect("the first turn")
}

#[test]
fn a_waking_successor_is_given_its_predecessors_handover_last_whole() {
    let without = woken_first_turn(Left::Nothing);
    assert!(!without.contains("Handover"), "{without}");
    let with = woken_first_turn(Left::Text(TEXT.as_bytes()));
    assert_eq!(with, format!("{without}\n\n{TEXT}"));
}

#[test]
fn a_refresh_successor_is_given_its_predecessors_handover_last_whole() {
    let without = refreshed_first_turn(Left::Nothing);
    assert!(!without.contains("Handover"), "{without}");
    let with = refreshed_first_turn(Left::Text(TEXT.as_bytes()));
    // The message already ends in a blank line.
    assert!(without.ends_with("\n\n"), "{without:?}");
    assert_eq!(with, format!("{without}{TEXT}"));
    assert!(with.ends_with(TEXT));
}

#[test]
fn a_successor_whose_predecessor_left_no_file_has_its_first_prompt_unchanged() {
    let woken = woken_first_turn(Left::Nothing);
    assert!(woken.contains("Order.A"), "{woken}");
    // An empty file is none left.
    assert_eq!(woken_first_turn(Left::Text(b"")), woken);
    let refreshed = refreshed_first_turn(Left::Nothing);
    assert_eq!(refreshed_first_turn(Left::Text(b"")), refreshed);
}

#[test]
fn a_handover_that_cannot_be_read_does_not_stop_the_successor() {
    let woken = woken_first_turn(Left::Nothing);
    assert_eq!(woken_first_turn(Left::Directory), woken);
    assert_eq!(woken_first_turn(Left::Text(b"not utf-8 \xff\xfe")), woken);
    let refreshed = refreshed_first_turn(Left::Nothing);
    assert_eq!(refreshed_first_turn(Left::Directory), refreshed);
    assert_eq!(
        refreshed_first_turn(Left::Text(b"not utf-8 \xff\xfe")),
        refreshed
    );
}

/// The Compose operation of a profile that names `predecessor`, as the
/// Nexus performs it for a launch, on the harness the model derives.
fn composed(
    fixture: &NexusFixture,
    model_name: &str,
    predecessor: Option<&str>,
    change: impl FnOnce(&mut signal_flow::LaunchProfile),
) -> signal_flow::ComposedLaunch {
    let mut profile = fixture
        .staged_launch("handover-compose", predecessor)
        .profile;
    profile.model_name = model_name.into();
    change(&mut profile);
    match fixture.nexus.perform(Operation::Compose(profile)) {
        Outcome::Composed(launch) => launch,
        refused => panic!("the launch is not composed: {refused:?}"),
    }
}

#[test]
fn the_handover_is_the_last_bytes_of_the_first_prompt_on_either_harness() {
    for model in ["claude-fixture", "fixture-model"] {
        let fixture = NexusFixture::new();
        let without = composed(&fixture, model, Some("abc123"), |_| {});
        leave(
            fixture.directory.path(),
            "abc123",
            Left::Text(TEXT.as_bytes()),
        );
        let with = composed(&fixture, model, Some("abc123"), |_| {});
        let before = &without.first_prompt_payload.first_prompt_body;
        let body = &with.first_prompt_payload.first_prompt_body;
        assert!(body.ends_with(TEXT), "{model}: {body}");
        assert_eq!(body, &format!("{before}\n\n{TEXT}"), "{model}");
        assert!(
            before.contains("fixture instruction"),
            "the brief precedes the handover: {before}"
        );
        // The hash is of the whole message, handover included.
        assert_ne!(
            with.first_prompt_payload.prompt_sha256,
            without.first_prompt_payload.prompt_sha256
        );
        assert_eq!(
            with.target_receipt_request.prompt_sha256,
            with.first_prompt_payload.prompt_sha256
        );
        // The system prompt is not touched.
        assert_eq!(with.system_prompt, without.system_prompt);
    }
}

#[test]
fn a_launch_naming_no_predecessor_is_given_no_handover() {
    // A Restart's profile names no predecessor, though the flow it resumes
    // left a handover: the resumed session has no first prompt.
    let fixture = NexusFixture::new();
    leave(
        fixture.directory.path(),
        "abc123",
        Left::Text(TEXT.as_bytes()),
    );
    let launch = composed(&fixture, "claude-fixture", None, |profile| {
        profile.instruction_prompt = String::new();
        profile.remembering = signal_flow::Remembering::Predecessors(0);
    });
    assert!(
        !launch
            .first_prompt_payload
            .first_prompt_body
            .contains("État"),
        "{}",
        launch.first_prompt_payload.first_prompt_body
    );
}

#[test]
fn a_handover_larger_than_the_rest_of_the_message_is_the_last_whole_part() {
    // The message does not fit one: the handover is a piece of its own,
    // cut off nowhere, in the last part.
    let handover = "x".repeat(1_047_300);
    let fixture = NexusFixture::new();
    leave(
        fixture.directory.path(),
        "abc123",
        Left::Text(handover.as_bytes()),
    );
    // A brief of its own size, so the rest is more than the margin the
    // handover leaves under one message.
    let launch = composed(&fixture, "claude-fixture", Some("abc123"), |profile| {
        profile.instruction_prompt = "y".repeat(2_000);
    });
    let parts = &launch.first_prompt_payload.prompt_part_vector;
    assert_eq!(parts.len(), 2, "the message is sent in two parts");
    let last = &parts[1].prompt_part_text;
    assert!(last.starts_with("Startup message, part 2 of 2, the last."));
    assert!(last.ends_with(&handover));
    assert!(!parts[0].prompt_part_text.contains("xxxxxxxx"));
    assert!(
        launch
            .first_prompt_payload
            .first_prompt_body
            .ends_with(&handover)
    );
}

#[test]
fn a_simplified_flow_is_given_the_handover_after_its_first_message_only() {
    let fixture = NexusFixture::new();
    leave(
        fixture.directory.path(),
        "abc123",
        Left::Text(TEXT.as_bytes()),
    );
    let simplify = |instruction: &'static str| {
        move |profile: &mut signal_flow::LaunchProfile| {
            profile.flow_kind = signal_flow::FlowKind::Simplified;
            profile.tool_set = signal_flow::ToolSet::Explicit(Vec::new());
            profile.instruction_prompt = instruction.into();
        }
    };
    let launch = composed(
        &fixture,
        "claude-fixture",
        Some("abc123"),
        simplify("Judge this."),
    );
    assert_eq!(
        launch.first_prompt_payload.first_prompt_body,
        format!("Judge this.\n\n{TEXT}")
    );
    // No first message, no turn to add it to.
    let launch = composed(&fixture, "claude-fixture", Some("abc123"), simplify(""));
    assert!(launch.first_prompt_payload.first_prompt_body.is_empty());
    assert!(launch.first_prompt_payload.prompt_part_vector.is_empty());
}

#[test]
fn the_file_source_reads_a_predecessors_file_under_the_launch_root_only() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    let source = HandoverFile;
    assert_eq!(source.handover("abc123", root), Handover::Absent);
    leave(root, "abc123", Left::Text(TEXT.as_bytes()));
    assert_eq!(
        source.handover("abc123", root),
        Handover::Text(TEXT.to_owned())
    );
    // Another flow's file, and another root's, are not this one's.
    assert_eq!(source.handover("def456", root), Handover::Absent);
    let elsewhere = tempfile::tempdir().unwrap();
    assert_eq!(
        source.handover("abc123", elsewhere.path()),
        Handover::Absent
    );
    // An empty file is none.
    leave(root, "def456", Left::Text(b""));
    assert_eq!(source.handover("def456", root), Handover::Absent);
    // A file that is not UTF-8, or is not a file, is unreadable.
    leave(root, "aaa111", Left::Text(b"\xff"));
    assert!(matches!(
        source.handover("aaa111", root),
        Handover::Unreadable(_)
    ));
    leave(root, "bbb222", Left::Directory);
    assert!(matches!(
        source.handover("bbb222", root),
        Handover::Unreadable(_)
    ));
    // A predecessor that does not name a flow is never joined to a path.
    for name in ["", "..", "../abc123", "a/b", ".hidden"] {
        assert!(
            matches!(source.handover(name, root), Handover::Unreadable(_)),
            "{name:?}"
        );
    }
}

#[test]
fn a_file_the_nexus_may_not_read_is_unreadable() {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    leave(root, "abc123", Left::Text(TEXT.as_bytes()));
    let file = root.join("flows/abc123/handover.md");
    fs::set_permissions(&file, fs::Permissions::from_mode(0o000)).unwrap();
    // A process that may read it anyway (root) shows nothing to refuse.
    let readable = fs::read(&file).is_ok();
    let reading = HandoverFile.handover("abc123", root);
    fs::set_permissions(&file, fs::Permissions::from_mode(0o644)).unwrap();
    if !readable {
        assert!(matches!(reading, Handover::Unreadable(_)), "{reading:?}");
    }
}
