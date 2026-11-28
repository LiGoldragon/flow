//! A transcript root written here, placed. The expected texts are written
//! out, not computed through the codec: the Bind line is the meta Signal's
//! Bind input as meta-signal-flow 0.95.0 writes it.

use flow_recover::{PlacesSessions, PrintsDatom, ReadsDatom, Recovery};
use std::{
    fs,
    path::Path,
    time::{Duration, SystemTime},
};

fn recovery(root: &Path) -> Recovery {
    Recovery::from_datom_text(&format!(
        "{{ {} {{ «.jsonl» attachment type prompt_snapshot systemPrompt }} {{ «Metaflow:» «Flow ID:» }} }}",
        root.display()
    ))
    .expect("a Recovery datom")
}

fn snapshot(address: &str, flow_id: &str) -> String {
    let prompt = format!("Modules first.\n\nMetaflow: {address}\nModel: x\nFlow ID: {flow_id}\n");
    let record = serde_json::json!({
        "type": "attachment",
        "attachment": { "type": "prompt_snapshot", "systemPrompt": ["skills", prompt] }
    });
    format!("{{\"type\":\"user\"}}\n{record}\n")
}

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

#[test]
fn newest_claim_binds_and_the_rest_are_reported() {
    let root = tempfile::tempdir().unwrap();
    let project = root.path().join("-home-li-primary");
    fs::create_dir_all(project.join("aaaaaa11/subagents")).unwrap();
    let older = project.join("aaaaaa11.jsonl");
    let newer = project.join("bbbbbb22.jsonl");
    let other = project.join("cccccc33.jsonl");
    let bare = project.join("dddddd44.jsonl");
    let unknown_model = project.join("eeeeee55.jsonl");
    write(&older, &snapshot("{ Mind flowNexus Opus }", "aaaaaa"), 600);
    write(&newer, &snapshot("{ Mind flowNexus Opus }", "bbbbbb"), 60);
    write(&other, &snapshot("{ Psyche test Haiku }", "cccccc"), 60);
    write(
        &bare,
        "{\"type\":\"user\",\"message\":\"Metaflow: { Psyche test Opus }\"}\n",
        60,
    );
    write(
        &unknown_model,
        &snapshot("{ Psyche audit Primary }", "eeeeee"),
        60,
    );
    write(
        &project.join("aaaaaa11/subagents/agent-1.jsonl"),
        &snapshot("{ Field x Opus }", "ffffff"),
        0,
    );
    write(
        &project.join("notes.txt"),
        &snapshot("{ Field y Opus }", "gggggg"),
        0,
    );

    let lines: Vec<String> = recovery(root.path())
        .placements()
        .unwrap()
        .iter()
        .map(PrintsDatom::datom_text)
        .collect();
    let at = |file: &Path| file.display().to_string();
    assert_eq!(
        lines,
        vec![
            format!("Bind.{{ {{ Mind flowNexus Opus }} {} }}", at(&newer)),
            format!("Bind.{{ {{ Psyche test Haiku }} {} }}", at(&other)),
            format!(
                "Contested.{{ {{ Mind flowNexus Opus }} [ {} ] }}",
                at(&older)
            ),
            format!("Unidentified.{}", at(&bare)),
            format!("Unidentified.{}", at(&unknown_model)),
        ]
    );
}

#[test]
fn a_root_that_cannot_be_read_is_refused() {
    let root = tempfile::tempdir().unwrap();
    assert!(recovery(&root.path().join("absent")).placements().is_err());
}
