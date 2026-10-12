use flow_nexus::{
    CarriesSignalFrames, OpensRunningNexus, RunningNexus, ServesMeta, ServesOrdinary,
    caller::{CallerProcess, IdentifiesPeer, LocatesCallerPane},
    generated::startup::{Start, StartRefusal, StoreRefusal},
    launching::PromotesAmbiguousLaunches,
    store::{
        ConfigurationOnlyStore, ConfiguresFlowStore, DefaultConfiguration, ReadsAnchors,
        StoreError,
    },
};
use datom_codec::{Actualizing, Budget, Datomizable, Potential};
use protos::{Compactable, Protosizable, ReaderBudget};
use std::{
    fs,
    os::unix::{
        fs::{FileTypeExt, PermissionsExt},
        net::{UnixListener, UnixStream},
    },
    path::Path,
    process::ExitCode,
    sync::Arc,
    time::Duration,
};

use meta_signal_flow::{
    Activation, ConfigureRejection, Configured, MetaRefusal, Query as MetaQuery,
    Response as MetaResponse,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StartupMode {
    ConfigureOnly,
    Run,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct StartupInvocation {
    mode: StartupMode,
    start: Start,
}

trait ParsesStartupArguments {
    fn parse_startup_arguments(arguments: &[String]) -> Result<StartupInvocation, String>;
}

impl ParsesStartupArguments for StartupInvocation {
    fn parse_startup_arguments(arguments: &[String]) -> Result<StartupInvocation, String> {
        let (mode, datom) = match arguments {
            [datom] => (StartupMode::Run, datom.as_str()),
            [only, datom] if only == "--configure-only" => {
                (StartupMode::ConfigureOnly, datom.as_str())
            }
            _ => return Err("usage: flow-nexus [--configure-only] '<one Start datom>'".into()),
        };
        let mut budget = Budget {
            remaining: 65_536,
            reader: ReaderBudget { remaining: 65_536 },
            depth: 0,
            maximum_depth: 1_024,
        };
        let start = Potential::<Start>::from(datom.to_owned())
            .actualize(&mut budget)
            .map_err(|error| format!("invalid Flow Nexus Start: {error:?}"))?;
        for (name, path) in [
            ("OrdinarySocketPath", start.ordinary_socket_path.as_str()),
            ("MetaSocketPath", start.meta_socket_path.as_str()),
            ("StorePath", start.store_path.as_str()),
        ] {
            if path.is_empty() || !Path::new(path).is_absolute() {
                return Err(format!("Start.{name} must be a nonempty absolute path"));
            }
        }
        if start.ordinary_socket_path == start.meta_socket_path
            || start.store_path == start.ordinary_socket_path
            || start.store_path == start.meta_socket_path
        {
            return Err("Start socket paths and StorePath must be distinct".into());
        }
        Ok(StartupInvocation { mode, start })
    }
}

trait EncodesStartupRefusal {
    fn refusal_datom(&self) -> Option<String>;
}

impl EncodesStartupRefusal for StoreError {
    fn refusal_datom(&self) -> Option<String> {
        let StoreError::UnrecognizedStore { path, reason } = self else {
            return None;
        };
        Some(
            StartRefusal::Store(StoreRefusal {
                path: path.clone(),
                reason: reason.clone(),
            })
            .datomize(vec![])
            .protosize()
            .compact(),
        )
    }
}

enum StartupFailure {
    Store(StoreError),
    Other(String),
}

trait CreatesExplicitStoreParent {
    fn create_store_parent(&self) -> Result<(), String>;
}

impl CreatesExplicitStoreParent for Path {
    fn create_store_parent(&self) -> Result<(), String> {
        let parent = self
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .ok_or_else(|| "Start.StorePath must name a file inside a directory".to_owned())?;
        fs::create_dir_all(parent)
            .map_err(|error| format!("cannot create store directory {}: {error}", parent.display()))
    }
}

/// `--version` is the sole invocation without a typed Start input.
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

struct ConfigurationPeerPolicy;

trait RefusesUnidentifiedConfigurationPeer {
    fn refusal(
        peer_present: bool,
        configured_message_executable: bool,
        marked_herdr_pane: bool,
    ) -> Option<MetaRefusal>;
}

impl RefusesUnidentifiedConfigurationPeer for ConfigurationPeerPolicy {
    fn refusal(
        peer_present: bool,
        configured_message_executable: bool,
        marked_herdr_pane: bool,
    ) -> Option<MetaRefusal> {
        let admitted = peer_present && (configured_message_executable || !marked_herdr_pane);
        (!admitted).then_some(MetaRefusal::PeerUnknown)
    }
}

trait AuthorizesConfigurationPeer {
    fn configuration_refusal(&self, message_nexus_path: &str) -> Option<MetaRefusal>;
}

impl AuthorizesConfigurationPeer for CallerProcess {
    fn configuration_refusal(&self, message_nexus_path: &str) -> Option<MetaRefusal> {
        if !message_nexus_path.is_empty() {
            let configured = fs::canonicalize(message_nexus_path).ok();
            let executable = fs::canonicalize(format!("/proc/{}/exe", self.process_id)).ok();
            if configured.is_some() && executable == configured {
                return ConfigurationPeerPolicy::refusal(true, true, false);
            }
        }
        // The ordinary Nexus resolves a marked Herdr pane to one live,
        // exactly bound Flow and checks its role against MetaAspects. This
        // bootstrap has not constructed that runtime, so it cannot prove a
        // pane peer; preserve the baseline unmarked-owner admission.
        ConfigurationPeerPolicy::refusal(true, false, self.caller_pane().is_some())
    }
}

trait BindsConfigurationSocket {
    fn configuration_listener(&self) -> Result<UnixListener, String>;
}

impl BindsConfigurationSocket for Path {
    fn configuration_listener(&self) -> Result<UnixListener, String> {
        let socket = self;
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
}

trait ServesOneConfiguration {
    fn serve_one_configuration(
        listener: UnixListener,
        store: &Self,
        message_nexus_path: &str,
    ) -> Result<(), String>;
}

impl ServesOneConfiguration for ConfigurationOnlyStore {
    fn serve_one_configuration(
        listener: UnixListener,
        store: &Self,
        message_nexus_path: &str,
    ) -> Result<(), String> {
        let (mut peer, _) = listener.accept().map_err(|error| error.to_string())?;
        peer.set_read_timeout(Some(Duration::from_secs(5)))
            .and_then(|_| peer.set_write_timeout(Some(Duration::from_secs(5))))
            .map_err(|error| error.to_string())?;
        let query = peer.read_meta_query()?;
        let refusal = peer
            .peer_process()
            .map(|process| process.configuration_refusal(message_nexus_path))
            .unwrap_or(Some(MetaRefusal::PeerUnknown));
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
}

trait OpensConfigurationListener {
    fn open_configuration_listener(
        &self,
    ) -> Result<(ConfigurationOnlyStore, String, UnixListener), StartupFailure>;
}

impl OpensConfigurationListener for Start {
    fn open_configuration_listener(
        &self,
    ) -> Result<(ConfigurationOnlyStore, String, UnixListener), StartupFailure> {
        let store_path = Path::new(&self.store_path);
        store_path
            .create_store_parent()
            .map_err(StartupFailure::Other)?;
        let store = ConfigurationOnlyStore::open(store_path).map_err(StartupFailure::Store)?;
        let message_nexus_path = match store
            .configured_configuration()
            .map_err(StartupFailure::Store)?
        {
            Some(current)
                if current.ordinary_socket_path == self.ordinary_socket_path
                    && current.meta_socket_path == self.meta_socket_path =>
            {
                current.message_nexus_path
            }
            Some(_) => {
                return Err(StartupFailure::Other(
                    "typed Start socket paths disagree with committed configuration".into(),
                ));
            }
            None => String::new(),
        };
        // The database handle above owns the same-store exclusive writer
        // before this path may remove a stale socket or bind the listener.
        // Keep the typed Start endpoint fixed while Configure writes any new
        // endpoint value.
        let listener = Path::new(&self.meta_socket_path)
            .configuration_listener()
            .map_err(StartupFailure::Other)?;
        Ok((store, message_nexus_path, listener))
    }
}

trait RunsConfigurationOnly {
    fn run_configuration_only(&self) -> Result<(), StartupFailure>;
}

impl RunsConfigurationOnly for Start {
    fn run_configuration_only(&self) -> Result<(), StartupFailure> {
        let (store, message_nexus_path, listener) = self.open_configuration_listener()?;
        ConfigurationOnlyStore::serve_one_configuration(listener, &store, &message_nexus_path)
            .map_err(StartupFailure::Other)
    }
}

trait ReportsStoreFailure {
    fn report_store_failure(self, store_path: &Path) -> ExitCode;
}

impl ReportsStoreFailure for StoreError {
    fn report_store_failure(self, store_path: &Path) -> ExitCode {
        if let Some(refusal) = self.refusal_datom() {
            println!("{refusal}");
        } else {
            eprintln!("flow-nexus: store {}: {self}", store_path.display());
        }
        ExitCode::FAILURE
    }
}

// Startup failures end the process with a nonzero status so the service
// manager restarts it; a failing serving thread does the same.
fn main() -> ExitCode {
    let arguments = std::env::args().skip(1).collect::<Vec<_>>();
    if let Some(version) = arguments.version_answer() {
        println!("{version}");
        return ExitCode::SUCCESS;
    }
    let invocation = match StartupInvocation::parse_startup_arguments(&arguments) {
        Ok(invocation) => invocation,
        Err(error) => {
            eprintln!("flow-nexus: {error}");
            return ExitCode::FAILURE;
        }
    };
    let defaults = DefaultConfiguration::from_environment();
    if invocation.mode == StartupMode::ConfigureOnly {
        return match invocation.start.run_configuration_only() {
            Ok(()) => ExitCode::SUCCESS,
            Err(StartupFailure::Store(error)) => {
                error.report_store_failure(Path::new(&invocation.start.store_path))
            }
            Err(StartupFailure::Other(error)) => {
                eprintln!("flow-nexus: configuration-only: {error}");
                ExitCode::FAILURE
            }
        };
    }
    let store_path = Path::new(&invocation.start.store_path);
    if let Err(error) = store_path.create_store_parent() {
        eprintln!("flow-nexus: {error}");
        return ExitCode::FAILURE;
    }
    let nexus = match RunningNexus::open_started(&defaults, &invocation.start) {
        Ok(nexus) => Arc::new(nexus),
        Err(error) => return error.report_store_failure(store_path),
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
        AnswersVersion, ConfigurationPeerPolicy, OpensConfigurationListener,
        ParsesStartupArguments, RefusesUnidentifiedConfigurationPeer, ServesOneConfiguration,
        StartupMode, StartupInvocation,
    };
    use flow_nexus::{
        CarriesSignalFrames,
        generated::startup::Start,
        store::{
            ConfigurationOnlyStore, DefaultConfiguration, LaysOutDefaults, SeedsConfiguration,
        },
    };
    use datom_codec::Datomizable;
    use protos::{Compactable, Protosizable};
    use meta_signal_flow::{Query as MetaQuery, Response as MetaResponse};
    use std::{
        os::unix::{
            fs::FileTypeExt,
            net::{UnixListener, UnixStream},
        },
        path::{Path, PathBuf},
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
    fn startup_requires_one_typed_start_and_no_trailing_datoms() {
        let start = Start {
            ordinary_socket_path: "/tmp/flow/ordinary.sock".into(),
            meta_socket_path: "/tmp/flow/meta.sock".into(),
            store_path: "/tmp/flow-state/flow.sema".into(),
        };
        let datom = start.datomize(vec![]).protosize().compact();
        let run = StartupInvocation::parse_startup_arguments(std::slice::from_ref(&datom))
            .expect("one Start datom");
        assert_eq!(run.mode, StartupMode::Run);
        assert_eq!(run.start, start);

        let configure = StartupInvocation::parse_startup_arguments(&[
            "--configure-only".into(),
            datom.clone(),
        ])
            .expect("configure-only plus one Start datom");
        assert_eq!(configure.mode, StartupMode::ConfigureOnly);
        assert!(StartupInvocation::parse_startup_arguments(&[]).is_err());
        assert!(
            StartupInvocation::parse_startup_arguments(&[datom.clone(), datom.clone()]).is_err()
        );
        assert!(
            StartupInvocation::parse_startup_arguments(&[format!("{datom} {datom}")]).is_err()
        );
        let relative = Start {
            store_path: "flow.sema".into(),
            ..start
        }
        .datomize(vec![])
        .protosize()
        .compact();
        assert!(StartupInvocation::parse_startup_arguments(&[relative]).is_err());
    }

    #[test]
    fn configuration_peer_gate_refuses_unknown_and_pane_peers() {
        assert!(matches!(
            ConfigurationPeerPolicy::refusal(false, false, false),
            Some(meta_signal_flow::MetaRefusal::PeerUnknown)
        ));
        assert!(matches!(
            ConfigurationPeerPolicy::refusal(true, false, true),
            Some(meta_signal_flow::MetaRefusal::PeerUnknown)
        ));
        assert!(ConfigurationPeerPolicy::refusal(true, false, false).is_none());
        assert!(ConfigurationPeerPolicy::refusal(true, true, true).is_none());
    }

    #[test]
    fn configuration_lock_is_acquired_before_stale_socket_removal() {
        let directory = tempfile::tempdir().expect("temporary home");
        let defaults = DefaultConfiguration {
            home: directory.path().join("home"),
            runtime_directory: directory.path().join("runtime"),
        };
        let start = Start {
            ordinary_socket_path: defaults
                .ordinary_socket_path()
                .to_string_lossy()
                .into_owned(),
            meta_socket_path: defaults.meta_socket_path().to_string_lossy().into_owned(),
            store_path: defaults.store_path().to_string_lossy().into_owned(),
        };
        std::fs::create_dir_all(defaults.state_directory()).expect("state directory");
        let store = ConfigurationOnlyStore::open(&defaults.store_path()).expect("store lock");
        let socket = defaults.meta_socket_path();
        std::fs::create_dir_all(socket.parent().expect("socket parent"))
            .expect("socket directory");
        let stale = UnixListener::bind(&socket).expect("create socket inode");
        drop(stale);
        assert!(UnixStream::connect(&socket).is_err(), "socket is stale");

        assert!(start.open_configuration_listener().is_err());
        assert!(
            std::fs::symlink_metadata(&socket)
                .expect("socket inode remains")
                .file_type()
                .is_socket()
        );
        drop(store);
    }

    #[test]
    fn committed_socket_identity_is_checked_before_stale_socket_removal() {
        let directory = tempfile::tempdir().expect("temporary state");
        let defaults = DefaultConfiguration {
            home: directory.path().join("home"),
            runtime_directory: directory.path().join("runtime"),
        };
        let start = Start {
            ordinary_socket_path: directory
                .path()
                .join("runtime/flow/ordinary.sock")
                .to_string_lossy()
                .into_owned(),
            meta_socket_path: directory
                .path()
                .join("runtime/flow/meta.sock")
                .to_string_lossy()
                .into_owned(),
            store_path: directory
                .path()
                .join("state/flow.sema")
                .to_string_lossy()
                .into_owned(),
        };
        std::fs::create_dir_all(Path::new(&start.store_path).parent().unwrap())
            .expect("store parent");
        let store = ConfigurationOnlyStore::open(Path::new(&start.store_path))
            .expect("configuration store opens");
        let mut configuration = defaults.configuration();
        configuration.ordinary_socket_path = start.ordinary_socket_path.clone();
        configuration.meta_socket_path = start.meta_socket_path.clone();
        store.configure(configuration).expect("config commits");
        drop(store);

        let different = Start {
            meta_socket_path: directory
                .path()
                .join("runtime/flow/other-meta.sock")
                .to_string_lossy()
                .into_owned(),
            ..start
        };
        let socket = Path::new(&different.meta_socket_path);
        std::fs::create_dir_all(socket.parent().unwrap()).expect("meta socket parent");
        let stale = UnixListener::bind(socket).expect("create stale socket inode");
        drop(stale);

        assert!(different.open_configuration_listener().is_err());
        assert!(
            std::fs::symlink_metadata(socket)
                .expect("stale inode remains")
                .file_type()
                .is_socket()
        );
    }

    #[test]
    fn configuration_listener_answers_one_request_and_exits() {
        let directory = tempfile::tempdir().expect("temporary home");
        let defaults = DefaultConfiguration {
            home: directory.path().join("home"),
            runtime_directory: directory.path().join("runtime"),
        };
        let start = Start {
            ordinary_socket_path: defaults
                .ordinary_socket_path()
                .to_string_lossy()
                .into_owned(),
            meta_socket_path: defaults.meta_socket_path().to_string_lossy().into_owned(),
            store_path: directory
                .path()
                .join("isolated-state/flow.sema")
                .to_string_lossy()
                .into_owned(),
        };
        let executable = std::env::current_exe()
            .expect("test executable")
            .canonicalize()
            .expect("canonical test executable");
        let (store, message_path, listener) =
            start
                .open_configuration_listener()
                .expect("configuration listener opens");
        assert!(store.configured_configuration().unwrap().is_none());
        assert!(message_path.is_empty());
        let socket = PathBuf::from(&start.meta_socket_path);
        let ordinary_socket = PathBuf::from(&start.ordinary_socket_path);
        assert!(
            UnixStream::connect(&ordinary_socket).is_err(),
            "configuration-only startup does not expose the ordinary adapter"
        );
        let mut configuration = defaults.configuration();
        configuration.message_nexus_path = executable.to_string_lossy().into_owned();
        configuration.meta_socket_path = socket.to_string_lossy().into_owned();
        let message_path = configuration.message_nexus_path.clone();
        let server =
            std::thread::spawn(move || {
                ConfigurationOnlyStore::serve_one_configuration(
                    listener,
                    &store,
                    &message_path,
                )
            });

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
