use flow_nexus::{OpensRunningNexus, RunningNexus, ServesOrdinary};
use std::{path::Path, time::Duration};
fn main() {
    let nexus = RunningNexus::open(
        Path::new("/home/li/primary/flow/flow.sema"),
        "/home/li/.codex/app-server-control/app-server-control.sock".into(),
        "gpt-5.4".into(),
        Duration::from_secs(10),
    )
    .unwrap_or_else(|error| panic!("Flow Nexus store: {error}"));
    nexus
        .serve_ordinary(Path::new("/tmp/flow-nexus.sock"))
        .unwrap_or_else(|error| panic!("Flow Nexus could not serve: {error}"));
}
