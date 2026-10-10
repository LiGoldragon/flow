//! The ordinary socket is supplied by the typed Start input. Durable Nexus
//! settings come from the committed Meta configuration snapshot.

use crate::{OpensRunningNexus, RunningNexus, store::ConfigurationOnlyStore};
use meta_signal_flow::{ConfigureNexus, ConfigurationChange};
use signal_flow::Start;

#[test]
fn a_launch_receives_the_socket_from_each_typed_start() {
    let directory = tempfile::tempdir().expect("fixture root");
    let defaults = crate::store::DefaultConfiguration {
        home: directory.path().join("home"),
        runtime_directory: directory.path().join("run/flow-next"),
    };
    std::fs::create_dir_all(defaults.state_directory()).expect("state directory");
    let config = ConfigurationOnlyStore::open(&defaults.store_path()).expect("config store");
    config
        .configure_change(ConfigurationChange::Nexus(ConfigureNexus {
            source_root: directory.path().to_string_lossy().into_owned(),
            stable_codex: endpoint(directory.path(), "stable"),
            next_codex: endpoint(directory.path(), "next"),
            harness_profiles: vec![],
            meta_aspects: vec![meta_signal_flow::FlowAspect::Psyche],
            message_nexus_path: directory
                .path()
                .join("message.sock")
                .to_string_lossy()
                .into_owned(),
            message_nexus_binary: directory
                .path()
                .join("message-nexus")
                .to_string_lossy()
                .into_owned(),
            lease: 60,
        }))
        .expect("whole typed Nexus configuration commits");
    drop(config);

    let first_start = Start {
        ordinary_socket_path: directory
            .path()
            .join("run/flow-next/flow.sock")
            .to_string_lossy()
            .into_owned(),
        meta_socket_path: directory
            .path()
            .join("run/flow-next/flow-meta.sock")
            .to_string_lossy()
            .into_owned(),
    };
    let nexus = RunningNexus::open(&defaults, &first_start).expect("configured Nexus opens");
    assert_eq!(
        nexus.herdr.ordinary_socket,
        directory.path().join("run/flow-next/flow.sock")
    );
    drop(nexus);

    let second_start = Start {
        ordinary_socket_path: directory
            .path()
            .join("elsewhere/ordinary.sock")
            .to_string_lossy()
            .into_owned(),
        meta_socket_path: first_start.meta_socket_path,
    };
    let reopened = RunningNexus::open(&defaults, &second_start).expect("configured Nexus reopens");
    assert_eq!(
        reopened.herdr.ordinary_socket,
        directory.path().join("elsewhere/ordinary.sock")
    );
}

fn endpoint(root: &std::path::Path, name: &str) -> meta_signal_flow::CodexEndpoint {
    meta_signal_flow::CodexEndpoint {
        client_path: root.join(name).to_string_lossy().into_owned(),
        home: root.join(format!("{name}-home")).to_string_lossy().into_owned(),
        control_socket_path: root
            .join(format!("{name}.sock"))
            .to_string_lossy()
            .into_owned(),
        model_name_vector: vec![format!("{name}-model")],
    }
}
