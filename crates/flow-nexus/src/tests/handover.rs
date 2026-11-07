//! A successor's first prompt ends with its predecessor's handover: a
//! refresh's successor and a waking's successor are given
//! `flows/<predecessor>/handover.md` of the launch root, whole, as the last
//! bytes of the prompt. An absent file leaves the prompt as it was; a file
//! that exists and cannot be read whole, a predecessor that names no flow and
//! a root that cannot be resolved refuse the launch `HandoverUnreadable`,
//! naming the predecessor and the fault, and leave the predecessor's seat as
//! it was. A Restart names no predecessor and has no first prompt, so it is
//! given none.

use super::NexusFixture;
use super::waking::{
    FreshWorld, ReadsFreshWorld, WakesFreshClaude, address, ended_claude, launched_claude_ended_by,
    metaflow,
};
use crate::generated::operation::{Failed_Data, Operation, Outcome};
use crate::handover::{Handover, HandoverFile, YieldsHandover};
use crate::performing::Performs;
use crate::store::ManagesMetaflowState as _;
use crate::store::RecordsLaunchOutcome as _;
use crate::store::metaflow::{SeedsMetaflow, SleepsMetaflows as _};
use crate::waking::HoldsWakingMetaflows;
use flow_ethos::State;
use meta_signal_flow::{ConfigurationChange, ConfigureMetaflow, Continuity};
use signal_flow::{
    CompositionRefusal, HandoverFault, HandoverUnreadable_Data, Model, Query, Refreshed, Request,
    Response,
};
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
    /// The handover, a file the Nexus may not read (mode 000).
    Forbidden,
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
        Left::Forbidden => {
            fs::create_dir_all(&directory).unwrap();
            let file = directory.join("handover.md");
            fs::write(&file, TEXT).unwrap();
            fs::set_permissions(&file, fs::Permissions::from_mode(0o000)).unwrap();
        }
    }
}

/// Whether a mode-000 file refuses this process: a process that may read it
/// anyway (root) has no permission refusal to show.
fn permissions_refuse() -> bool {
    let directory = tempfile::tempdir().unwrap();
    leave(directory.path(), "abc123", Left::Forbidden);
    fs::read(directory.path().join("flows/abc123/handover.md")).is_err()
}

/// Whether a fault is the one a case is refused for.
type Fault = fn(&HandoverFault) -> bool;

/// Each way a handover exists and cannot be read whole, with the fault it
/// is refused for. The permission refusal is left out where it is not one.
fn unreadable() -> Vec<(Left<'static>, Fault)> {
    let mut cases: Vec<(Left<'static>, Fault)> = vec![
        (Left::Text(b"not utf-8 \xff\xfe"), |fault| {
            *fault == HandoverFault::NotUtf8
        }),
        (
            Left::Directory,
            |fault| matches!(fault, HandoverFault::Unreadable(text) if text.ends_with("flows/abc123/handover.md: Is a directory (os error 21)")),
        ),
    ];
    if permissions_refuse() {
        cases.push((Left::Forbidden, |fault| {
            matches!(fault, HandoverFault::Unreadable(text) if text.ends_with("flows/abc123/handover.md: Permission denied (os error 13)"))
        }));
    }
    cases
}

/// The refusal a launch naming `predecessor` is composed into.
fn refused_for(predecessor: &str, fault: HandoverFault) -> CompositionRefusal {
    CompositionRefusal::HandoverUnreadable(HandoverUnreadable_Data {
        flow_id: predecessor.into(),
        handover_fault: fault,
    })
}

/// A waking of the metaflow whose last flow is `past`'s last, after the
/// flow abc123 slept, with `left` under abc123 and Order.A queued; the
/// metaflow `declared` with abc123's modules, or woken as abc123 launched.
fn woken(left: Left<'_>, past: Vec<&str>, declared: bool) -> (NexusFixture, FreshWorld, Outcome) {
    let mut fixture = NexusFixture::new();
    ended_claude(&fixture);
    leave(fixture.directory.path(), "abc123", left);
    let at = address(Model::Sol);
    if declared {
        declare(&fixture, None);
    }
    fixture.nexus.store.seed_metaflow(metaflow(
        Model::Sol,
        State::Asleep,
        past,
        vec![Request::Order("A".into())],
    ));
    let world = fixture.fresh_claude_world();
    let lease = fixture.nexus.waking.hold(&at).expect("the wake holds it");
    let outcome = fixture.nexus.perform(Operation::Wake(at));
    drop(lease);
    (fixture, world, outcome)
}

/// The first prompt of the fresh flow a waking starts after the flow
/// abc123 slept, with `left` under abc123.
fn woken_first_turn(left: Left<'_>) -> String {
    let (fixture, world, outcome) = woken(left, vec!["abc123"], false);
    let Outcome::Woken(_) = outcome else {
        panic!(
            "the wake starts a fresh flow: {outcome:?}; Herdr calls: {}",
            world.calls()
        );
    };
    let turns = world.user_turns(&fixture);
    turns.into_iter().next().expect("the first turn")
}

/// A waking refused for `refusal` leaves the metaflow Asleep with Order.A
/// queued, and starts no harness.
fn assert_asleep_with_its_queue(
    (fixture, world, outcome): (NexusFixture, FreshWorld, Outcome),
    refusal: impl FnOnce(&CompositionRefusal) -> bool,
) {
    match &outcome {
        Outcome::Failed(Failed_Data::CompositionRefused(refused)) if refusal(refused) => {}
        _ => panic!("the wake is refused: {outcome:?}"),
    }
    let metaflow = fixture
        .nexus
        .store
        .metaflow(&address(Model::Sol))
        .unwrap()
        .expect("the metaflow");
    assert_eq!(metaflow.state, State::Asleep);
    assert_eq!(
        metaflow
            .queue
            .iter()
            .map(|entry| entry.message.clone())
            .collect::<Vec<_>>(),
        vec![signal_flow::Message::MiddleAbrupt(Request::Order(
            "A".into()
        ))],
        "the queue is kept"
    );
    assert_eq!(world.claude_starts(), 0, "{}", world.calls());
}

/// The metaflow declared Maintained with abc123's modules, under
/// `root_option`.
fn declare(fixture: &NexusFixture, root_option: Option<String>) {
    fixture
        .nexus
        .store
        .configure_change(ConfigurationChange::Metaflows(vec![ConfigureMetaflow {
            root_option,
            address: address(Model::Sol),
            system_modules: vec!["spirit-core".into(), "intent-flow".into()],
            user_modules: vec!["operation-main-flow".into()],
            flow_kind: signal_flow::FlowKind::Regular,
            tool_set: signal_flow::ToolSet::Default,
            continuity: Continuity::Maintained,
            purpose: String::new(),
            records: vec![],
        }]))
        .expect("metaflow declared");
}

/// A Refresh of a Maintained metaflow Awake as abc123, with `left` under
/// abc123 and the metaflow's root `root_option`.
fn refreshed(left: Left<'_>, root_option: Option<String>) -> (NexusFixture, FreshWorld, Response) {
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
    declare(&fixture, root_option);
    fixture.nexus.store.seed_metaflow(metaflow(
        Model::Sol,
        State::Awake("abc123".into()),
        vec![],
        vec![],
    ));
    use crate::Dispatches;
    let response = fixture.nexus.dispatch(Query::Refresh(at));
    (fixture, world, response)
}

/// The first prompt of the successor a Refresh of a Maintained metaflow
/// starts, with `left` under the ending flow abc123.
fn refreshed_first_turn(left: Left<'_>) -> String {
    let (fixture, world, response) = refreshed(left, None);
    assert_eq!(
        response,
        Response::Refreshed(Refreshed {
            address: address(Model::Sol),
            flow_id: "abc123".into(),
        }),
        "Herdr calls: {}",
        world.calls()
    );
    assert_eq!(world.claude_starts(), 1, "{}", world.calls());
    let turns = world.user_turns(&fixture);
    turns.into_iter().next().expect("the first turn")
}

/// A Refresh refused for `refusal` keeps abc123 running: its metaflow
/// Awake as abc123, holding its Address, no successor owed or started, and
/// its pane and harness untouched, nothing closed and nothing typed.
fn assert_predecessor_kept(
    (fixture, world, response): (NexusFixture, FreshWorld, Response),
    refusal: impl FnOnce(&signal_flow::StartRejection) -> bool,
) {
    use crate::store::ReadsFlowRows as _;
    use crate::store::successor::OwesSuccessors as _;
    use signal_flow::{RefreshRejection, ReplaceRejection};
    match &response {
        Response::RefreshRejected(RefreshRejection::SuccessorRefused(
            ReplaceRejection::LaunchRefused(rejection),
        )) if refusal(rejection) => {}
        _ => panic!("the refresh is refused: {response:?}"),
    }
    let at = address(Model::Sol);
    let metaflow = fixture
        .nexus
        .store
        .metaflow(&at)
        .unwrap()
        .expect("metaflow");
    assert_eq!(metaflow.state, State::Awake("abc123".into()));
    assert_eq!(
        fixture
            .nexus
            .store
            .flow_node("abc123")
            .unwrap()
            .map(|node| node.flow_lifecycle),
        Some(signal_flow::FlowLifecycle::Active)
    );
    assert_eq!(fixture.nexus.store.successor_intent(&at).unwrap(), None);
    let calls = world.calls();
    assert_eq!(world.claude_starts(), 0, "{calls}");
    for step in ["pane close", "agent prompt", "agent start"] {
        assert!(!calls.contains(step), "{step}: {calls}");
    }
    assert!(
        world.state.join("old-agent").exists(),
        "the old harness runs on: {calls}"
    );
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
fn a_waking_whose_handover_cannot_be_read_is_refused_asleep_with_its_queue() {
    for (left, fault) in unreadable() {
        assert_asleep_with_its_queue(
            woken(left, vec!["abc123"], false),
            |refused| matches!(refused, CompositionRefusal::HandoverUnreadable(HandoverUnreadable_Data { flow_id, handover_fault }) if flow_id == "abc123" && fault(handover_fault)),
        );
    }
}

#[test]
fn a_refresh_whose_handover_cannot_be_read_keeps_its_predecessor() {
    for (left, fault) in unreadable() {
        assert_predecessor_kept(
            refreshed(left, None),
            |rejection| matches!(rejection, signal_flow::StartRejection::CompositionRefused(CompositionRefusal::HandoverUnreadable(HandoverUnreadable_Data { flow_id, handover_fault })) if flow_id == "abc123" && fault(handover_fault)),
        );
    }
}

#[test]
fn a_waking_after_a_predecessor_naming_no_flow_is_refused_asleep_with_its_queue() {
    // The metaflow's last flow is not a FlowId: it is never joined to a
    // path, and the waking is refused naming it.
    assert_asleep_with_its_queue(
        woken(Left::Nothing, vec!["abc123", "../abc123"], true),
        |refused| *refused == refused_for("../abc123", HandoverFault::InvalidPredecessor),
    );
}

#[test]
fn a_launch_whose_root_cannot_be_resolved_is_refused_naming_its_predecessor() {
    // The Compose of a successor whose declared root is gone.
    let fixture = NexusFixture::new();
    let gone = tempfile::tempdir().unwrap();
    let root = gone.path().to_string_lossy().into_owned();
    drop(gone);
    let mut profile = fixture
        .staged_launch("handover-compose", Some("abc123"))
        .profile;
    profile.model_name = "claude-fixture".into();
    fixture
        .nexus
        .store
        .configure_change(ConfigurationChange::Metaflows(vec![ConfigureMetaflow {
            root_option: Some(root.clone()),
            address: profile.address.clone(),
            system_modules: profile.system_modules.clone(),
            user_modules: profile.user_modules.clone(),
            flow_kind: signal_flow::FlowKind::Regular,
            tool_set: signal_flow::ToolSet::Default,
            continuity: Continuity::Maintained,
            purpose: String::new(),
            records: vec![],
        }]))
        .expect("metaflow declared");
    assert_eq!(
        fixture.nexus.perform(Operation::Compose(profile.clone())),
        Outcome::Failed(Failed_Data::CompositionRefused(refused_for(
            "abc123",
            HandoverFault::RootUnresolved
        )))
    );
    // Naming no predecessor, the same launch reads no handover and composes.
    profile.flow_id_option = None;
    assert!(matches!(
        fixture.nexus.perform(Operation::Compose(profile)),
        Outcome::Composed(_)
    ));

    // A Refresh checks its successor's root before it composes: the gone
    // root refuses it as before, the predecessor kept.
    assert_predecessor_kept(refreshed(Left::Nothing, Some(root)), |rejection| {
        *rejection
            == signal_flow::StartRejection::CompositionRefused(CompositionRefusal::Unconfigured)
    });
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
    assert_eq!(source.handover(&address(Model::Sol), "abc123", root), Handover::Absent);
    leave(root, "abc123", Left::Text(TEXT.as_bytes()));
    assert_eq!(
        source.handover(&address(Model::Sol), "abc123", root),
        Handover::Text(TEXT.to_owned())
    );
    // Another flow's file, and another root's, are not this one's.
    assert_eq!(source.handover(&address(Model::Sol), "def456", root), Handover::Absent);
    let elsewhere = tempfile::tempdir().unwrap();
    assert_eq!(
        source.handover(&address(Model::Sol), "abc123", elsewhere.path()),
        Handover::Absent
    );
    // An empty file is none.
    leave(root, "def456", Left::Text(b""));
    assert_eq!(source.handover(&address(Model::Sol), "def456", root), Handover::Absent);
    // A file that is not UTF-8, or is not a file, is unreadable.
    leave(root, "aaa111", Left::Text(b"\xff"));
    assert_eq!(
        source.handover(&address(Model::Sol), "aaa111", root),
        Handover::Unreadable(HandoverFault::NotUtf8)
    );
    leave(root, "bbb222", Left::Directory);
    assert_eq!(
        source.handover(&address(Model::Sol), "bbb222", root),
        Handover::Unreadable(HandoverFault::Unreadable(format!(
            "{}: Is a directory (os error 21)",
            root.join("flows/bbb222/handover.md").display()
        )))
    );
    // A predecessor that does not name a flow is never joined to a path.
    for name in ["", "..", "../abc123", "a/b", ".hidden"] {
        assert_eq!(
            source.handover(name, root),
            Handover::Unreadable(HandoverFault::InvalidPredecessor),
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
    let reading = HandoverFile.handover(&address(Model::Sol), "abc123", root);
    fs::set_permissions(&file, fs::Permissions::from_mode(0o644)).unwrap();
    if !readable {
        assert_eq!(
            reading,
            Handover::Unreadable(HandoverFault::Unreadable(format!(
                "{}: Permission denied (os error 13)",
                file.display()
            )))
        );
    }
}

/// An OnDemand seat resting with nothing queued is closed only once its
/// successor composes: a successor refused its handover (one not UTF-8, or
/// a root gone) keeps the seat Awake in its pane, nothing closed.
#[test]
fn an_on_demand_seat_whose_successor_is_refused_its_handover_is_not_closed() {
    use super::refresh_and_compaction::{declare_seat, seat_refreshing_at};
    use super::waking::{CLAUDE_SEAT, append_main_rows, claude_agent, claude_seat, end_turn};
    use super::{ControlsHerdrSnapshot as _, PromptFixture};
    use crate::store::context::RecordsContextSize as _;
    use crate::waking::{AtStop, KeptAtStop, SleepsAndWakesSeats};
    for gone_root in [false, true] {
        let fixture = NexusFixture::new();
        fixture.accept_pane_operations(
            vec![claude_agent(&fixture)],
            PromptFixture::Prompted("w1:p3"),
        );
        claude_seat(&fixture, 1_000);
        append_main_rows(&fixture, &[end_turn("last-turn")]);
        seat_refreshing_at(&fixture, 200_000);
        fixture
            .nexus
            .store
            .record_context_size(CLAUDE_SEAT, 1_000)
            .unwrap();
        declare_seat(
            &fixture,
            Continuity::OnDemand,
            vec!["spirit-core".into(), "intent-flow".into()],
        );
        let fault = if gone_root {
            let gone = tempfile::tempdir().unwrap();
            let root = gone.path().to_string_lossy().into_owned();
            drop(gone);
            fixture
                .nexus
                .store
                .configure_change(ConfigurationChange::Metaflows(vec![ConfigureMetaflow {
                    root_option: Some(root),
                    address: address(Model::Sol),
                    system_modules: vec!["spirit-core".into(), "intent-flow".into()],
                    user_modules: vec!["operation-main-flow".into()],
                    flow_kind: signal_flow::FlowKind::Regular,
                    tool_set: signal_flow::ToolSet::Default,
                    continuity: Continuity::OnDemand,
                    purpose: String::new(),
                    records: vec![],
                }]))
                .expect("metaflow declared");
            HandoverFault::RootUnresolved
        } else {
            leave(
                fixture.directory.path(),
                CLAUDE_SEAT,
                Left::Text(b"not utf-8 \xff\xfe"),
            );
            HandoverFault::NotUtf8
        };
        fixture.nexus.store.seed_metaflow(metaflow(
            Model::Sol,
            State::Awake(CLAUDE_SEAT.into()),
            vec![],
            vec![],
        ));

        assert_eq!(
            fixture.nexus.settle_stopped(CLAUDE_SEAT),
            AtStop::Kept(KeptAtStop::SuccessorUncomposable(Box::new(
                Outcome::Failed(Failed_Data::CompositionRefused(refused_for(
                    CLAUDE_SEAT,
                    fault
                )))
            )))
        );
        let metaflow = fixture
            .nexus
            .store
            .metaflow(&address(Model::Sol))
            .unwrap()
            .expect("metaflow");
        assert_eq!(metaflow.state, State::Awake(CLAUDE_SEAT.into()));
        for name in ["herdr-operations.log", "herdr-calls.log"] {
            let calls = fs::read_to_string(fixture.directory.path().join(name)).unwrap_or_default();
            assert!(!calls.contains("pane close"), "{name}: {calls}");
        }
    }
}
