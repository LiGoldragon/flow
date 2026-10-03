//! A launched flow's hook reports to the Nexus that launched it: the Nexus
//! hands every Claude launch the ordinary socket its own store configures,
//! the one it serves, never the default path under some other runtime
//! directory.

use crate::store::{ConfiguresFlowStore, DefaultConfiguration};
use crate::{OpensRunningNexus, RunningNexus};

#[test]
fn a_launch_carries_the_ordinary_socket_the_store_configures() {
    let directory = tempfile::tempdir().expect("fixture root");
    // The next slot's layout: the Nexus's runtime directory is not the one
    // a pane's shell has.
    let defaults = DefaultConfiguration {
        home: directory.path().join("home"),
        runtime_directory: directory.path().join("run/flow-next"),
    };
    let nexus = RunningNexus::open(&defaults).expect("fresh Nexus opens");
    assert_eq!(
        nexus.herdr.ordinary_socket,
        directory.path().join("run/flow-next/flow/flow.sock")
    );
    let mut configuration = nexus.store.configuration().expect("configuration");
    configuration.ordinary_socket_path = directory
        .path()
        .join("elsewhere/ordinary.sock")
        .to_string_lossy()
        .into_owned();
    nexus.store.configure(configuration).expect("configured");
    drop(nexus);
    let reopened = RunningNexus::open(&defaults).expect("populated Nexus opens");
    assert_eq!(
        reopened.herdr.ordinary_socket,
        directory.path().join("elsewhere/ordinary.sock")
    );
}
