use flow_nexus::{NexusCore, ServesSignal};
use std::path::Path;
fn main() {
    let mut nexus = NexusCore::default();
    nexus
        .serve(Path::new("/tmp/flow-nexus.sock"))
        .unwrap_or_else(|error| panic!("Flow Nexus could not serve: {error}"));
}
