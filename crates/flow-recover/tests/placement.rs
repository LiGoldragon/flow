//! A transcript root written here, placed. The Bind and Past lines are
//! written out, not computed through the codec: the Bind line is the meta
//! Signal's Bind input as meta-signal-flow 0.95.0 writes it.

use flow_ethos::{Address, FlowId};
use flow_recover::{
    AsTranscriptPath, Contest, Mismatched_Data, Placement, PlacesSessions, PrintsDatom, ReadsDatom,
    Recovery, SessionId, SharedFlow_Data, Unordered_Data,
};
use std::{
    fs,
    path::Path,
    time::{Duration, SystemTime},
};

fn recovery(root: &Path, depth: u32) -> Recovery {
    Recovery::from_datom_text(&format!(
        "{{ {} {depth} {{ «.jsonl» attachment type prompt_snapshot systemPrompt sessionId timestamp }} {{ «Metaflow:» «Flow ID:» }} }}",
        root.display()
    ))
    .expect("a Recovery datom")
}

/// A session whose snapshot names the address and flow id, written in the
/// session `session`, whose last timed record is at `last`, and whose very
/// last record carries no time.
fn session(address: &str, flow_id: &str, session: &str, last: &str) -> String {
    let prompt = format!("Modules first.\n\nMetaflow: {address}\nModel: x\nFlow ID: {flow_id}\n");
    let snapshot = serde_json::json!({
        "type": "attachment",
        "sessionId": session,
        "timestamp": "2026-01-01T00:00:00.000Z",
        "attachment": { "type": "prompt_snapshot", "systemPrompt": ["skills", prompt] }
    });
    let reply = serde_json::json!({ "type": "assistant", "sessionId": session, "timestamp": last });
    let closing =
        serde_json::json!({ "type": "cost-state", "sessionId": session, "timestamp": null });
    format!("{{\"type\":\"user\"}}\n{snapshot}\n{reply}\n{closing}\n")
}

/// A file written `age` seconds ago, so its modification time is never
/// what orders it.
fn write(file: &Path, content: &str, age: u64) {
    fs::write(file, content).unwrap();
    let written = SystemTime::now() - Duration::from_secs(age);
    fs::File::options()
        .write(true)
        .open(file)
        .unwrap()
        .set_modified(written)
        .unwrap();
}

fn lines(placements: &[Placement]) -> Vec<String> {
    placements.iter().map(PrintsDatom::datom_text).collect()
}

fn address(text: &str) -> Address {
    Address::from_datom_text(text).unwrap()
}

#[test]
fn the_flow_whose_last_record_is_newest_binds_and_its_elders_are_past() {
    let root = tempfile::tempdir().unwrap();
    let project = root.path().join("-home-li-primary");
    fs::create_dir_all(project.join("aaaaaa11/subagents")).unwrap();
    let older = project.join("aaaaaa11.jsonl");
    let newer = project.join("bbbbbb22.jsonl");
    let other = project.join("cccccc33.jsonl");
    let bare = project.join("dddddd44.jsonl");
    let unknown_model = project.join("eeeeee55.jsonl");
    let mind = "{ Mind flowNexus Opus }";
    // The older flow's file was modified last; its last record is older.
    write(
        &older,
        &session(mind, "aaaaaa", "aaaaaa11", "2026-03-01T10:00:00.000Z"),
        0,
    );
    write(
        &newer,
        &session(mind, "bbbbbb", "bbbbbb22", "2026-03-02T10:00:00.000Z"),
        600,
    );
    write(
        &other,
        &session(
            "{ Psyche test Haiku }",
            "cccccc",
            "cccccc33",
            "2026-03-01T00:00:00.000Z",
        ),
        60,
    );
    write(
        &bare,
        "{\"type\":\"user\",\"message\":\"Metaflow: { Psyche test Opus }\"}\n",
        60,
    );
    write(
        &unknown_model,
        &session(
            "{ Psyche audit Primary }",
            "eeeeee",
            "eeeeee55",
            "2026-03-01T00:00:00.000Z",
        ),
        60,
    );
    write(
        &project.join("aaaaaa11/subagents/agent-1.jsonl"),
        &session(
            "{ Field x Opus }",
            "ffffff",
            "ffffff66",
            "2026-03-01T00:00:00.000Z",
        ),
        0,
    );
    write(
        &project.join("notes.txt"),
        &session(
            "{ Field y Opus }",
            "gggggg",
            "gggggg77",
            "2026-03-01T00:00:00.000Z",
        ),
        0,
    );

    let at = |file: &Path| file.display().to_string();
    assert_eq!(
        lines(&recovery(root.path(), 1).placements().unwrap()),
        vec![
            format!("Bind.{{ {{ Mind flowNexus Opus }} {} }}", at(&newer)),
            format!("Bind.{{ {{ Psyche test Haiku }} {} }}", at(&other)),
            format!("Past.{{ {{ Mind flowNexus Opus }} aaaaaa {} }}", at(&older)),
            format!("Unidentified.{}", at(&bare)),
            format!("Unidentified.{}", at(&unknown_model)),
        ]
    );
}

#[test]
fn depth_reads_the_directories_below() {
    let root = tempfile::tempdir().unwrap();
    let nested = root.path().join("project/ffffff66/subagents");
    fs::create_dir_all(&nested).unwrap();
    let file = nested.join("ffffff66.jsonl");
    write(
        &file,
        &session(
            "{ Field x Opus }",
            "ffffff",
            "ffffff66",
            "2026-03-01T00:00:00.000Z",
        ),
        0,
    );
    assert!(recovery(root.path(), 1).placements().unwrap().is_empty());
    assert_eq!(
        lines(&recovery(root.path(), 3).placements().unwrap()),
        vec![format!("Bind.{{ {{ Field x Opus }} {} }}", file.display())]
    );
}

#[test]
fn a_flow_id_that_does_not_start_its_session_is_mismatched() {
    let root = tempfile::tempdir().unwrap();
    let project = root.path().join("project");
    fs::create_dir_all(&project).unwrap();
    let wrong = project.join("hhhhhh88.jsonl");
    let longer = project.join("iiiiiiii99.jsonl");
    write(
        &wrong,
        &session(
            "{ Mind flowNexus Opus }",
            "jjjjjj",
            "hhhhhh88",
            "2026-03-01T00:00:00.000Z",
        ),
        0,
    );
    // The flow id's length is the file's own: eight characters here.
    write(
        &longer,
        &session(
            "{ Mind flowNexus Opus }",
            "iiiiiiii",
            "iiiiiiii99",
            "2026-02-01T00:00:00.000Z",
        ),
        0,
    );
    assert_eq!(
        recovery(root.path(), 1).placements().unwrap(),
        vec![
            Placement::Bind(flow_recover::Bind_Data {
                address: address("{ Mind flowNexus Opus }"),
                transcript_path: longer.transcript_path(),
            }),
            Placement::Mismatched(Mismatched_Data {
                transcript_path: wrong.transcript_path(),
                flow_id: FlowId("jjjjjj".into()),
                session_id_option: Some(SessionId("hhhhhh88".into())),
            }),
        ]
    );
}

#[test]
fn two_files_of_one_flow_are_contested_and_do_not_bind() {
    let root = tempfile::tempdir().unwrap();
    let one = root.path().join("one");
    let other = root.path().join("other");
    fs::create_dir_all(&one).unwrap();
    fs::create_dir_all(&other).unwrap();
    let first = one.join("kkkkkk11.jsonl");
    let second = other.join("kkkkkk11.jsonl");
    let elder = one.join("llllll22.jsonl");
    let mind = "{ Mind flowNexus Opus }";
    write(
        &first,
        &session(mind, "kkkkkk", "kkkkkk11", "2026-03-02T00:00:00.000Z"),
        0,
    );
    write(
        &second,
        &session(mind, "kkkkkk", "kkkkkk11", "2026-03-03T00:00:00.000Z"),
        0,
    );
    write(
        &elder,
        &session(mind, "llllll", "llllll22", "2026-03-01T00:00:00.000Z"),
        0,
    );
    assert_eq!(
        recovery(root.path(), 1).placements().unwrap(),
        vec![
            Placement::Past(flow_recover::Past_Data {
                address: address(mind),
                flow_id: FlowId("llllll".into()),
                transcript_path: elder.transcript_path(),
            }),
            Placement::Contested(Contest::SharedFlow(SharedFlow_Data {
                flow_id: FlowId("kkkkkk".into()),
                transcript_path_vector: vec![first.transcript_path(), second.transcript_path()],
            })),
        ]
    );
}

#[test]
fn newest_flows_whose_times_tie_are_contested_and_do_not_bind() {
    let root = tempfile::tempdir().unwrap();
    let project = root.path().join("project");
    fs::create_dir_all(&project).unwrap();
    let one = project.join("mmmmmm11.jsonl");
    let other = project.join("nnnnnn22.jsonl");
    let mind = "{ Mind flowNexus Opus }";
    write(
        &one,
        &session(mind, "mmmmmm", "mmmmmm11", "2026-03-02T00:00:00.000Z"),
        0,
    );
    write(
        &other,
        &session(mind, "nnnnnn", "nnnnnn22", "2026-03-02T00:00:00.000Z"),
        600,
    );
    assert_eq!(
        recovery(root.path(), 1).placements().unwrap(),
        vec![Placement::Contested(Contest::Unordered(Unordered_Data {
            address: address(mind),
            transcript_path_vector: vec![one.transcript_path(), other.transcript_path()],
        }))]
    );
}

#[test]
fn a_root_that_cannot_be_read_is_refused() {
    let root = tempfile::tempdir().unwrap();
    assert!(
        recovery(&root.path().join("absent"), 1)
            .placements()
            .is_err()
    );
}
