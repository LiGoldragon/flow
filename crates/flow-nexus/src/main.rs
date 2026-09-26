use flow_nexus::{
    OpensRunningNexus, RunningNexus, ServesMeta, ServesOrdinary,
    launching::PromotesAmbiguousLaunches,
    store::{ConfiguresFlowStore, DefaultConfiguration, DeploymentOverrides},
};
use std::{path::Path, process::ExitCode, sync::Arc};

/// `--version` alone answers the Cargo package version before any
/// configuration is read; every other argument vector starts the daemon.
fn version_answer(arguments: &[String]) -> Option<String> {
    match arguments {
        [only] if only == "--version" => Some(format!("flow-nexus {}", env!("CARGO_PKG_VERSION"))),
        _ => None,
    }
}

// Startup failures end the process with a nonzero status so the service
// manager restarts it; a failing serving thread does the same.
fn main() -> ExitCode {
    if let Some(version) = version_answer(&std::env::args().skip(1).collect::<Vec<_>>()) {
        println!("{version}");
        return ExitCode::SUCCESS;
    }
    let defaults = DefaultConfiguration::from_environment();
    for directory in [defaults.state_directory(), defaults.socket_directory()] {
        if let Err(error) = std::fs::create_dir_all(&directory) {
            eprintln!("flow-nexus: cannot create {}: {error}", directory.display());
            return ExitCode::FAILURE;
        }
    }
    let nexus = match RunningNexus::open(&defaults, &DeploymentOverrides::from_environment()) {
        Ok(nexus) => Arc::new(nexus),
        Err(error) => {
            eprintln!(
                "flow-nexus: store {}: {error}",
                defaults.store_path().display()
            );
            return ExitCode::FAILURE;
        }
    };
    let configuration = match nexus.store.configuration() {
        Ok(configuration) => configuration,
        Err(error) => {
            eprintln!("flow-nexus: configuration: {error}");
            return ExitCode::FAILURE;
        }
    };
    for socket in [
        &configuration.ordinary_socket_path,
        &configuration.meta_socket_path,
    ] {
        if let Some(directory) = Path::new(socket).parent()
            && let Err(error) = std::fs::create_dir_all(directory)
        {
            eprintln!("flow-nexus: cannot create {}: {error}", directory.display());
            return ExitCode::FAILURE;
        }
    }
    // A receipt that arrives after StartAmbiguous promotes its launch with
    // no subscriber and no second Start.
    let promoting_nexus = Arc::clone(&nexus);
    std::thread::spawn(move || promoting_nexus.promote_ambiguous_launches());
    let ordinary = configuration.ordinary_socket_path.clone();
    let ordinary_nexus = Arc::clone(&nexus);
    std::thread::spawn(move || {
        if let Err(error) = ordinary_nexus.serve_ordinary(Path::new(&ordinary)) {
            eprintln!("flow-nexus: ordinary socket {ordinary}: {error}");
            std::process::exit(1);
        }
    });
    match nexus.serve_meta(Path::new(&configuration.meta_socket_path)) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!(
                "flow-nexus: meta socket {}: {error}",
                configuration.meta_socket_path
            );
            ExitCode::FAILURE
        }
    }
}

#[cfg(test)]
mod tests {
    use super::version_answer;

    #[test]
    fn version_answers_the_cargo_package_version() {
        assert_eq!(
            version_answer(&["--version".into()]),
            Some(format!("flow-nexus {}", env!("CARGO_PKG_VERSION")))
        );
        assert_eq!(version_answer(&[]), None);
        assert_eq!(version_answer(&["--version".into(), "x".into()]), None);
    }
}
