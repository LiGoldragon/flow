use flow_nexus::{
    OpensRunningNexus, RunningNexus, ServesMeta, ServesOrdinary, store::ConfiguresFlowStore,
};
use std::sync::Arc;
use std::{path::Path, time::Duration};
fn main() {
    let nexus = RunningNexus::open(
        Path::new("/home/li/primary/flow/flow.sema"),
        "/home/li/.codex/app-server-control/app-server-control.sock".into(),
        "gpt-5.4".into(),
        Duration::from_secs(10),
    )
    .unwrap_or_else(|error| panic!("Flow Nexus store: {error}"));
    let nexus = Arc::new(nexus);
    let configuration = nexus.store.configuration().unwrap();
    let ordinary = configuration.ordinary_socket.clone();
    let ordinary_nexus = Arc::clone(&nexus);
    std::thread::spawn(move || {
        ordinary_nexus.serve_ordinary(Path::new(&ordinary)).unwrap();
    });
    nexus
        .serve_meta(Path::new(&configuration.meta_socket))
        .unwrap_or_else(|error| panic!("Flow Nexus could not serve: {error}"));
}
