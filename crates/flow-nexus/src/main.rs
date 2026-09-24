use flow_nexus::{
    OpensRunningNexus, RunningNexus, ServesMeta, ServesOrdinary,
    codex::{CodexEndpoint, CodexEndpoints},
    store::ConfiguresFlowStore,
};
use std::{
    collections::BTreeSet,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

fn required_path(name: &str) -> PathBuf {
    let path = std::env::var_os(name)
        .map(PathBuf::from)
        .unwrap_or_else(|| panic!("{name} must be configured"));
    assert!(path.is_absolute(), "{name} must be absolute");
    path
}

fn required_models(name: &str) -> BTreeSet<String> {
    let value = std::env::var(name).unwrap_or_else(|_| panic!("{name} must be configured"));
    let models = value
        .split(',')
        .map(str::trim)
        .filter(|model| !model.is_empty())
        .map(str::to_owned)
        .collect::<BTreeSet<_>>();
    assert!(!models.is_empty(), "{name} must select at least one model");
    models
}

fn endpoint(prefix: &str) -> CodexEndpoint {
    let client_path = required_path(&format!("FLOW_CODEX_{prefix}_CLIENT"));
    let socket = required_path(&format!("FLOW_CODEX_{prefix}_SOCKET"));
    let home = required_path(&format!("FLOW_CODEX_{prefix}_HOME"));
    CodexEndpoint {
        client_path,
        home: home.clone(),
        socket: socket
            .into_os_string()
            .into_string()
            .unwrap_or_else(|_| panic!("FLOW_CODEX_{prefix}_SOCKET must be valid UTF-8")),
        transcript_root: home.join("sessions"),
        model_names: required_models(&format!("FLOW_CODEX_{prefix}_MODELS")),
    }
}

fn main() {
    std::fs::create_dir_all("/home/li/.local/state/flow")
        .unwrap_or_else(|error| panic!("Flow Nexus state directory: {error}"));
    std::fs::create_dir_all("/run/user/1001/flow")
        .unwrap_or_else(|error| panic!("Flow Nexus runtime directory: {error}"));
    let source_root = std::env::var_os("FLOW_SOURCE_ROOT")
        .map(PathBuf::from)
        .unwrap_or_else(|| panic!("FLOW_SOURCE_ROOT must name the configured launch source root"));
    let codex_endpoints = CodexEndpoints {
        stable: endpoint("STABLE"),
        next: endpoint("NEXT"),
        timeout: Duration::from_secs(10),
        workspace_root: source_root.clone(),
    };
    assert!(
        codex_endpoints
            .stable
            .model_names
            .is_disjoint(&codex_endpoints.next.model_names),
        "stable and next Codex model selections must be disjoint"
    );
    let nexus = RunningNexus::open(
        Path::new("/home/li/.local/state/flow/flow.sema"),
        codex_endpoints,
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
