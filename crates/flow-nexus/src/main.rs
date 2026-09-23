use flow_nexus::{
    OpensRunningNexus, RunningNexus, ServesMeta, ServesOrdinary, store::ConfiguresFlowStore,
};
use std::sync::Arc;
use std::{path::{Path, PathBuf}, time::Duration};
fn main() {
    std::fs::create_dir_all("/home/li/.local/state/flow")
        .unwrap_or_else(|error| panic!("Flow Nexus state directory: {error}"));
    std::fs::create_dir_all("/run/user/1001/flow")
        .unwrap_or_else(|error| panic!("Flow Nexus runtime directory: {error}"));
    let source_root = std::env::var_os("FLOW_SOURCE_ROOT")
        .map(PathBuf::from)
        .unwrap_or_else(|| panic!("FLOW_SOURCE_ROOT must name the configured launch source root"));
    let nexus = RunningNexus::open(
        Path::new("/home/li/.local/state/flow/flow.sema"),
        "/home/li/.codex/app-server-control/app-server-control.sock".into(),
        "gpt-5.6-terra".into(),
        Duration::from_secs(10),
        source_root,
    )
    .unwrap_or_else(|error| panic!("Flow Nexus store: {error}"));
    let nexus = Arc::new(nexus);
    let configuration = nexus.store.configuration().unwrap();
    let ordinary = configuration.ordinary_socket_path.clone();
    let ordinary_nexus = Arc::clone(&nexus);
    std::thread::spawn(move || {
        ordinary_nexus.serve_ordinary(Path::new(&ordinary)).unwrap();
    });
    nexus
        .serve_meta(Path::new(&configuration.meta_socket_path))
        .unwrap_or_else(|error| panic!("Flow Nexus could not serve: {error}"));
}
