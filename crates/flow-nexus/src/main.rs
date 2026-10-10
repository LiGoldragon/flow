use flow_nexus::{
    CarriesSignalFrames, OpensRunningNexus, RunningNexus, ServesMeta, ServesOrdinary,
    caller::{IdentifiesPeer, LocatesCallerPane},
    launching::PromotesAmbiguousLaunches,
    store::{
        ConfigurationOnlyStore, ConfiguresFlowStore, DefaultConfiguration, LaysOutDefaults,
        ReadsAnchors,
    },
};
use std::{
    fs,
    os::unix::{
        fs::{FileTypeExt, PermissionsExt},
        net::{UnixListener, UnixStream},
    },
    path::{Path, PathBuf},
    process::ExitCode,
    sync::Arc,
    time::Duration,
};

use meta_signal_flow::{
    Activation, Configuration, ConfigureRejection, Configured, MetaRefusal, Query as MetaQuery,
    Response as MetaResponse,
};

/// `--version` answers before configuration is read; `--configure-only`
/// serves one durable Configure request and exits; other arguments start the daemon.
trait AnswersVersion {
    fn version_answer(&self) -> Option<String>;
}

impl AnswersVersion for [String] {
    fn version_answer(&self) -> Option<String> {
        match self {
            [only] if only == "--version" => {
                Some(format!("flow-nexus {}", env!("CARGO_PKG_VERSION")))
            }
            _ => None,
        }
    }
}

fn admits_configuration_peer(
    peer_present: bool,
    configured_message_executable: bool,
    marked_herdr_pane: bool,
) -> bool {
    peer_present && (configured_message_executable || !marked_herdr_pane)
}

fn configuration_peer_refusal(
    peer_present: bool,
    configured_message_executable: bool,
    marked_herdr_pane: bool,
) -> Option<MetaRefusal> {
    (!admits_configuration_peer(
        peer_present,
        configured_message_executable,
        marked_herdr_pane,
    ))
    .then_some(MetaRefusal::PeerUnknown)
}

fn peer_is_authorized_for_configuration(
    peer: Option<flow_nexus::caller::CallerProcess>,
    message_nexus_path: &str,
) -> Result<Option<MetaRefusal>, String> {
    let Some(peer) = peer else {
        return Ok(configuration_peer_refusal(false, false, false));
    };
    if !message_nexus_path.is_empty() {
        let configured = fs::canonicalize(message_nexus_path).ok();
        let executable = fs::canonicalize(format!("/proc/{}/exe", peer.process_id)).ok();
        if configured.is_some() && executable == configured {
            return Ok(configuration_peer_refusal(true, true, false));
        }
    }
    // The ordinary Nexus resolves a marked Herdr pane to one live, exactly
    // bound Flow and checks its role against MetaAspects. This bootstrap has
    // intentionally not constructed that runtime or its reconciliation
    // readers, so a pane peer cannot be proved and is refused. An unmarked
    // peer is the same owner case admitted by the normal meta gate.
    Ok(configuration_peer_refusal(
        true,
        false,
        peer.caller_pane().is_some(),
    ))
}

fn configuration_listener(socket: &Path) -> Result<UnixListener, String> {
    if let Some(directory) = socket
        .parent()
        .filter(|directory| !directory.as_os_str().is_empty())
    {
        fs::create_dir_all(directory)
            .map_err(|error| format!("cannot create {}: {error}", directory.display()))?;
    }
    match fs::symlink_metadata(socket) {
        Ok(metadata) if metadata.file_type().is_socket() => match UnixStream::connect(socket) {
            Ok(_peer) => {
                return Err(format!(
                    "meta socket {} is already accepting connections",
                    socket.display()
                ));
            }
            Err(error) if error.kind() == std::io::ErrorKind::ConnectionRefused => {
                fs::remove_file(socket).map_err(|error| {
                    format!("cannot remove stale socket {}: {error}", socket.display())
                })?;
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(format!(
                    "cannot establish whether meta socket {} is stale: {error}",
                    socket.display()
                ));
            }
        },
        Ok(_) => {
            return Err(format!(
                "meta socket path {} exists and is not a socket",
                socket.display()
            ));
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => {
            return Err(format!(
                "cannot inspect meta socket {}: {error}",
                socket.display()
            ));
        }
    }
    let listener = UnixListener::bind(socket)
        .map_err(|error| format!("cannot bind meta socket {}: {error}", socket.display()))?;
    if let Err(error) = fs::set_permissions(socket, fs::Permissions::from_mode(0o600)) {
        drop(listener);
        let _ = fs::remove_file(socket);
        return Err(format!(
            "cannot protect meta socket {}: {error}",
            socket.display()
        ));
    }
    Ok(listener)
}

fn serve_one_configuration(
    listener: UnixListener,
    store: &ConfigurationOnlyStore,
    message_nexus_path: &str,
) -> Result<(), String> {
    let (mut peer, _) = listener.accept().map_err(|error| error.to_string())?;
    peer.set_read_timeout(Some(Duration::from_secs(5)))
        .and_then(|_| peer.set_write_timeout(Some(Duration::from_secs(5))))
        .map_err(|error| error.to_string())?;
    let query = peer.read_meta_query()?;
    let refusal = peer_is_authorized_for_configuration(peer.peer_process(), message_nexus_path)?;
    if let Some(refusal) = refusal {
        peer.write_meta_response(&MetaResponse::MetaRefused(refusal))?;
        return Err("meta configuration peer was not provably authorized".into());
    }
    let MetaQuery::Configure(configuration) = query else {
        return Err("configuration-only listener accepts one Configure request".into());
    };
    let response = match store.configure(configuration) {
        Ok(configuration) => MetaResponse::Configured(Configured {
            configuration,
            activation: Activation::NexusRestartRequired,
        }),
        Err(_) => MetaResponse::ConfigureRejected(ConfigureRejection::StoreRefused),
    };
    let configured = matches!(&response, MetaResponse::Configured(_));
    peer.write_meta_response(&response)?;
    if configured {
        Ok(())
    } else {
        Err("configuration transaction was refused".into())
    }
}

fn open_configuration_listener(
    defaults: &DefaultConfiguration,
) -> Result<(ConfigurationOnlyStore, Configuration, UnixListener), String> {
    fs::create_dir_all(defaults.state_directory()).map_err(|error| {
        format!(
            "cannot create {}: {error}",
            defaults.state_directory().display()
        )
    })?;
    let store = ConfigurationOnlyStore::open(&defaults.store_path())
        .map_err(|error| format!("store {}: {error}", defaults.store_path().display()))?;
    let current = store
        .configuration(defaults)
        .map_err(|error| format!("configuration: {error}"))?;
    // The database handle above owns the same-store exclusive writer before
    // this path may remove a stale socket or bind the listener. Keep the
    // current endpoint fixed while Configure writes any new endpoint value.
    let socket = PathBuf::from(&current.meta_socket_path);
    let listener = configuration_listener(&socket)?;
    Ok((store, current, listener))
}

fn run_configuration_only(defaults: &DefaultConfiguration) -> Result<(), String> {
    let (store, current, listener) = open_configuration_listener(defaults)?;
    serve_one_configuration(listener, &store, &current.message_nexus_path)
}

// Startup failures end the process with a nonzero status so the service
// manager restarts it; a failing serving thread does the same.
fn main() -> ExitCode {
    let arguments = std::env::args().skip(1).collect::<Vec<_>>();
    if let Some(version) = arguments.version_answer() {
        println!("{version}");
        return ExitCode::SUCCESS;
    }
    let defaults = DefaultConfiguration::from_environment();
    if matches!(arguments.as_slice(), [only] if only == "--configure-only") {
        return match run_configuration_only(&defaults) {
            Ok(()) => ExitCode::SUCCESS,
            Err(error) => {
                eprintln!("flow-nexus: configuration-only: {error}");
                ExitCode::FAILURE
            }
        };
    }
    for directory in [defaults.state_directory(), defaults.socket_directory()] {
        if let Err(error) = std::fs::create_dir_all(&directory) {
            eprintln!("flow-nexus: cannot create {}: {error}", directory.display());
            return ExitCode::FAILURE;
        }
    }
    let nexus = match RunningNexus::open(&defaults) {
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
    use super::{
        AnswersVersion, configuration_peer_refusal, open_configuration_listener,
        serve_one_configuration,
    };
    use flow_nexus::{
        CarriesSignalFrames,
        store::{
            ConfigurationOnlyStore, DefaultConfiguration, LaysOutDefaults, SeedsConfiguration,
        },
    };
    use meta_signal_flow::{Query as MetaQuery, Response as MetaResponse};
    use std::{
        os::unix::{
            fs::FileTypeExt,
            net::{UnixListener, UnixStream},
        },
        path::PathBuf,
    };

    #[test]
    fn version_answers_the_cargo_package_version() {
        assert_eq!(
            (&["--version".to_owned()] as &[String]).version_answer(),
            Some(format!("flow-nexus {}", env!("CARGO_PKG_VERSION")))
        );
        assert_eq!((&[] as &[String]).version_answer(), None);
        assert_eq!(
            (&["--version".to_owned(), "x".to_owned()] as &[String]).version_answer(),
            None
        );
    }

    #[test]
    fn configuration_peer_gate_refuses_unknown_and_pane_peers() {
        assert!(matches!(
            configuration_peer_refusal(false, false, false),
            Some(meta_signal_flow::MetaRefusal::PeerUnknown)
        ));
        assert!(matches!(
            configuration_peer_refusal(true, false, true),
            Some(meta_signal_flow::MetaRefusal::PeerUnknown)
        ));
        assert!(configuration_peer_refusal(true, false, false).is_none());
        assert!(configuration_peer_refusal(true, true, true).is_none());
    }

    #[test]
    fn configuration_lock_is_acquired_before_stale_socket_removal() {
        let directory = tempfile::tempdir().expect("temporary home");
        let defaults = DefaultConfiguration {
            home: directory.path().join("home"),
            runtime_directory: directory.path().join("runtime"),
        };
        std::fs::create_dir_all(defaults.state_directory()).expect("state directory");
        let store = ConfigurationOnlyStore::open(&defaults.store_path()).expect("store lock");
        let socket = defaults.meta_socket_path();
        std::fs::create_dir_all(socket.parent().expect("socket parent")).expect("socket directory");
        let stale = UnixListener::bind(&socket).expect("create socket inode");
        drop(stale);
        assert!(UnixStream::connect(&socket).is_err(), "socket is stale");

        assert!(open_configuration_listener(&defaults).is_err());
        assert!(
            std::fs::symlink_metadata(&socket)
                .expect("socket inode remains")
                .file_type()
                .is_socket()
        );
        drop(store);
    }

    #[test]
    fn configuration_listener_answers_one_request_and_exits() {
        let directory = tempfile::tempdir().expect("temporary home");
        let defaults = DefaultConfiguration {
            home: directory.path().join("home"),
            runtime_directory: directory.path().join("runtime"),
        };
        let executable = std::env::current_exe()
            .expect("test executable")
            .canonicalize()
            .expect("canonical test executable");
        let (store, current, listener) =
            open_configuration_listener(&defaults).expect("configuration listener opens");
        let socket = PathBuf::from(&current.meta_socket_path);
        let ordinary_socket = PathBuf::from(&current.ordinary_socket_path);
        assert!(
            UnixStream::connect(&ordinary_socket).is_err(),
            "configuration-only startup does not expose the ordinary adapter"
        );
        let mut configuration = defaults.configuration();
        configuration.message_nexus_path = executable.to_string_lossy().into_owned();
        configuration.meta_socket_path = socket.to_string_lossy().into_owned();
        let message_path = configuration.message_nexus_path.clone();
        let server =
            std::thread::spawn(move || serve_one_configuration(listener, &store, &message_path));

        let mut peer = UnixStream::connect(&socket).expect("connect to config-only listener");
        let bytes =
            rkyv::to_bytes::<rkyv::rancor::Error>(&MetaQuery::Configure(configuration.clone()))
                .expect("encode one Configure request");
        peer.write_frame(&bytes)
            .expect("send one Configure request");
        let reply = peer.read_frame().expect("read Configure response");
        let response = rkyv::from_bytes::<MetaResponse, rkyv::rancor::Error>(&reply)
            .expect("decode Configure response");
        assert!(matches!(
            response,
            MetaResponse::Configured(meta_signal_flow::Configured {
                configuration: stored,
                activation: meta_signal_flow::Activation::NexusRestartRequired,
            }) if stored == configuration
        ));
        assert!(server.join().expect("server thread joins").is_ok());
        assert!(
            UnixStream::connect(&socket).is_err(),
            "one-request listener has exited"
        );
    }
}
