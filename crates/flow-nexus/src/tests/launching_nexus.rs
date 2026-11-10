//! A Nexus opens in its place: its memory in State, the socket its flows
//! report to in Runtime. Durable Nexus settings come from the committed Meta
//! configuration snapshot, and a Configure applies to the running Nexus.

use crate::herdr::launch::{ChecksComposedLaunch, PreparesClaudePane, tests::launch};
use crate::live::HoldsLiveValue;
use crate::place::{LaysOutPlace, Place};
use crate::store::ConfiguresOnlyStore;
use crate::{Dispatches, OpensRunningNexus, RunningNexus, store::ConfigurationOnlyStore};
use meta_signal_flow::{ConfigurationChange, ConfigureNexus, ConfigureOutcome};
use signal_flow::HarnessKind;
use std::path::{Path, PathBuf};

/// The place `name` under a fixture's runtime and state roots, as the
/// deployment lays it out: `<runtime>/nexus/<name>`, `<state>/nexus/<name>`.
fn place(root: &Path, name: &str) -> Place {
    Place {
        place_name: crate::generated::library::PlaceName(name.into()),
        runtime: crate::generated::library::Runtime(
            root.join("run/nexus")
                .join(name)
                .to_string_lossy()
                .into_owned(),
        ),
        state: crate::generated::library::State(
            root.join("state/nexus")
                .join(name)
                .to_string_lossy()
                .into_owned(),
        ),
    }
}

fn nexus_configuration(root: &Path) -> ConfigureNexus {
    ConfigureNexus {
        source_root: flow_ethos::SourceRoot(root.to_string_lossy().into_owned()),
        first_codex_endpoint: endpoint(root, "stable"),
        second_codex_endpoint: endpoint(root, "next"),
        harness_profiles: flow_ethos::HarnessProfiles(vec![]),
        meta_aspects: flow_ethos::MetaAspects(vec![meta_signal_flow::Aspect::Psyche]),
        message_nexus_path_option: Some(flow_ethos::MessageNexusPath(
            root.join("message.sock").to_string_lossy().into_owned(),
        )),
        message_nexus_binary_option: Some(flow_ethos::MessageNexusBinary(
            root.join("message-nexus").to_string_lossy().into_owned(),
        )),
        curriculum_nexus_path: meta_signal_flow::CurriculumNexusPath(
            root.join("run/curriculum/curriculum.sock")
                .to_string_lossy()
                .into_owned(),
        ),
        lease: flow_ethos::Lease(60),
        presentation_prompt_option: None,
    }
}

/// A configured memory in the place's State.
fn configured(place: &Place, root: &Path) {
    std::fs::create_dir_all(place.state_directory()).expect("state directory");
    let config = ConfigurationOnlyStore::open(&place.store()).expect("config store");
    config
        .configure_change(ConfigurationChange::Nexus(nexus_configuration(root)))
        .expect("whole typed Nexus configuration commits");
}

/// Every hook command a Claude flow is launched with.
fn hook_commands(nexus: &RunningNexus) -> Vec<String> {
    let settings: serde_json::Value = serde_json::from_str(
        &nexus
            .herdr
            .claude_flag_settings(&launch(HarnessKind::Claude).lean_start()),
    )
    .expect("settings JSON");
    [
        "SessionStart",
        "PostToolUse",
        "UserPromptSubmit",
        "Stop",
        "SessionEnd",
    ]
    .into_iter()
    .map(|event| {
        settings["hooks"][event][0]["hooks"][0]["command"]
            .as_str()
            .expect("hook command")
            .to_owned()
    })
    .collect()
}

#[test]
fn a_launch_receives_the_socket_of_the_place_the_nexus_opened_in() {
    let directory = tempfile::tempdir().expect("fixture root");
    let main = place(directory.path(), "main");
    configured(&main, directory.path());
    let nexus = RunningNexus::open(&main).expect("configured Nexus opens");
    assert_eq!(
        nexus.herdr.ordinary_socket,
        directory.path().join("run/nexus/main/flow.sock")
    );
    // The context composer asks the Curriculum socket the configuration names.
    let composer = format!("{:?}", nexus.composer);
    assert!(
        composer.contains(&format!(
            "socket: {:?}",
            directory.path().join("run/curriculum/curriculum.sock")
        )),
        "{composer}"
    );
    drop(nexus);

    // The same memory opened at another place serves that place's socket.
    let elsewhere = Place {
        runtime: crate::generated::library::Runtime(
            directory
                .path()
                .join("run/nexus/next")
                .to_string_lossy()
                .into_owned(),
        ),
        ..main.clone()
    };
    let reopened = RunningNexus::open(&elsewhere).expect("configured Nexus reopens");
    assert_eq!(
        reopened.herdr.ordinary_socket,
        directory.path().join("run/nexus/next/flow.sock")
    );
}

/// The hook settings of every Claude flow a Nexus at the main place
/// launches name the main place's socket, `$XDG_RUNTIME_DIR/nexus/main/
/// flow.sock`, and no versioned directory: a later version started at main
/// serves that same socket, so a flow keeps reporting to whichever version
/// is main.
#[test]
fn the_hook_settings_name_the_main_place_socket() {
    let directory = tempfile::tempdir().expect("fixture root");
    let main = place(directory.path(), "main");
    configured(&main, directory.path());
    let nexus = RunningNexus::open(&main).expect("configured Nexus opens");
    let socket = directory.path().join("run/nexus/main/flow.sock");
    for command in hook_commands(&nexus) {
        assert!(
            command.ends_with(&format!("'{{ {} }}'", socket.display())),
            "{command}"
        );
    }
}

/// The hook command comes from the configuration's HarnessHook, applied to
/// the running Nexus at once: no environment is read, and no restart is
/// needed. Unconfigured, it is the flow-nexus-hook beside the executable.
#[test]
fn a_configured_harness_hook_takes_effect_in_the_running_nexus_without_a_restart() {
    let directory = tempfile::tempdir().expect("fixture root");
    let main = place(directory.path(), "main");
    configured(&main, directory.path());
    let nexus = RunningNexus::open(&main).expect("configured Nexus opens");
    let beside = std::env::current_exe()
        .expect("test executable")
        .parent()
        .expect("executable directory")
        .join("flow-nexus-hook");
    assert_eq!(*nexus.herdr.harness_hook.current(), beside);
    let stable = PathBuf::from("/home/someone/.local/bin/flow-nexus-hook");
    assert_eq!(
        nexus.dispatch_meta(meta_signal_flow::Query::Configure(
            ConfigurationChange::HarnessHook(meta_signal_flow::generated::signal::Path(
                stable.to_string_lossy().into_owned()
            ))
        )),
        meta_signal_flow::Response::Configured(ConfigureOutcome::Replaced)
    );
    assert_eq!(*nexus.herdr.harness_hook.current(), stable);
    for command in hook_commands(&nexus) {
        assert!(
            command.starts_with(&format!("'{}' ", stable.display())),
            "{command}"
        );
    }
    drop(nexus);
    // The memory keeps it: the next start writes the same hook.
    let reopened = RunningNexus::open(&main).expect("configured Nexus reopens");
    assert_eq!(*reopened.herdr.harness_hook.current(), stable);
}

fn endpoint(root: &std::path::Path, name: &str) -> meta_signal_flow::CodexEndpoint {
    meta_signal_flow::CodexEndpoint {
        client_path: flow_ethos::ClientPath(root.join(name).to_string_lossy().into_owned()),
        home: flow_ethos::Home(
            root.join(format!("{name}-home"))
                .to_string_lossy()
                .into_owned(),
        ),
        control_socket_path: flow_ethos::ControlSocketPath(
            root.join(format!("{name}.sock"))
                .to_string_lossy()
                .into_owned(),
        ),
        model_name_vector: vec![flow_ethos::ModelName(format!("{name}-model"))],
    }
}
