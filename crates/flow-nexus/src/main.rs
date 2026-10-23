use datom_codec::{Actualizing, Budget, Datomizable, Potential};
use flow_nexus::store::ConfiguresOnlyStore;
use flow_nexus::{
    CarriesSignalFrames, ClaimedSocket, ClaimsSocket, HoldsStopRequest, OpensRunningNexus,
    RunningNexus, ServesMeta, ServesOrdinary, StopsNexus, TerminationSignals,
    WatchesTerminationSignals,
    caller::{IdentifiesPeer, LocatesCallerPane},
    launching::ReconcilesInterruptedLaunches,
    send_tool::ServesSendTool,
    store::{ConfigurationOnlyStore, DefaultConfiguration, ReadsAnchors, StoreError},
};
use protos::{Compactable, Protosizable, ReaderBudget};
use std::{
    fs,
    os::unix::net::{UnixListener, UnixStream},
    path::Path,
    process::ExitCode,
    sync::atomic::{AtomicBool, Ordering},
    time::Duration,
};

use meta_signal_flow::{
    ConfigureRejection, MetaRefusal, Query as MetaQuery, Response as MetaResponse,
};

/// `--version` is the only command that runs without a typed Start datom.
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

trait ParsesStart {
    fn parse_start(&self) -> Result<flow_ethos::Start, String>;
}

impl ParsesStart for [String] {
    fn parse_start(&self) -> Result<flow_ethos::Start, String> {
        let [source] = self else {
            return Err(
                "usage: flow-nexus [--configure-only] '{ «ordinary-socket» «meta-socket» «store-path» Some.{ «context-repository» «deploy-executable» } }'"
                    .into(),
            );
        };
        let mut budget = Budget {
            remaining: 65_536,
            reader: ReaderBudget { remaining: 65_536 },
            depth: 0,
            maximum_depth: 1_024,
        };
        Potential::<flow_ethos::Start>::from(source.clone())
            .actualize(&mut budget)
            .map_err(|error| format!("invalid Flow Start datom: {error:?}"))
    }
}

#[derive(Debug)]
enum ConfigureOnlyError {
    Store(StoreError),
    Other(String),
}

impl std::fmt::Display for ConfigureOnlyError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Store(error) => write!(formatter, "{error}"),
            Self::Other(error) => formatter.write_str(error),
        }
    }
}

fn main() -> ExitCode {
    let arguments = std::env::args().skip(1).collect::<Vec<_>>();
    if let Some(version) = arguments.version_answer() {
        println!("{version}");
        return ExitCode::SUCCESS;
    }
    // Before any thread starts, so a stop always reaches the watching thread.
    let signals = TerminationSignals::block();
    let defaults = DefaultConfiguration::from_environment();
    if matches!(
        arguments.first().map(String::as_str),
        Some("--configure-only")
    ) {
        let start = match arguments.get(1..).unwrap_or_default().parse_start() {
            Ok(start) => start,
            Err(error) => {
                eprintln!("{error}");
                return ExitCode::FAILURE;
            }
        };
        return match ConfigurationStartup::run_configuration_only(&start, &signals) {
            Ok(()) => ExitCode::SUCCESS,
            Err(ConfigureOnlyError::Store(StoreError::UnrecognizedMemory { path, reason })) => {
                ConfigurationStartup::report_unrecognized_store(&path, &reason);
                ExitCode::FAILURE
            }
            Err(error) => {
                eprintln!("flow-nexus: configuration-only: {error}");
                ExitCode::FAILURE
            }
        };
    }
    let start = match arguments.parse_start() {
        Ok(start) => start,
        Err(error) => {
            eprintln!("{error}");
            return ExitCode::FAILURE;
        }
    };
    let nexus = match RunningNexus::open(&defaults, &start) {
        Ok(nexus) => nexus,
        Err(StoreError::UnrecognizedMemory { path, reason }) => {
            ConfigurationStartup::report_unrecognized_store(&path, &reason);
            return ExitCode::FAILURE;
        }
        Err(error) => {
            eprintln!("flow-nexus: store {}: {error}", start.store_path);
            return ExitCode::FAILURE;
        }
    };
    for socket in [&start.ordinary_socket_path, &start.meta_socket_path] {
        if let Some(directory) = Path::new(socket).parent()
            && !directory.as_os_str().is_empty()
            && let Err(error) = fs::create_dir_all(directory)
        {
            eprintln!("flow-nexus: cannot create {}: {error}", directory.display());
            return ExitCode::FAILURE;
        }
    }
    let ordinary = Path::new(&start.ordinary_socket_path);
    let meta = Path::new(&start.meta_socket_path);
    let sockets = [ordinary, meta];
    let failed = AtomicBool::new(false);
    let serve = |surface: &str, socket: &Path, served: Result<(), String>| {
        if let Err(error) = served {
            eprintln!("flow-nexus: {surface} socket {}: {error}", socket.display());
            failed.store(true, Ordering::SeqCst);
            nexus.stop(&sockets);
        }
    };
    // A Codex launch the last Nexus left mid-way, its first turn perhaps
    // pending on a send call, is refused and settled before anything is
    // served.
    nexus.reconcile_interrupted_launches();
    // Every thread borrows the Nexus inside this scope, so when a stop has
    // ended them all the Nexus drops here and its store closes cleanly.
    std::thread::scope(|scope| {
        // One connection per live Codex thread answers its send-tool calls.
        let send_tool_nexus = &nexus;
        scope.spawn(move || send_tool_nexus.serve_send_tool(scope));
        scope.spawn(|| {
            if signals.watch(&|| nexus.stopping.requested()) {
                nexus.stop(&sockets);
            }
        });
        scope.spawn(|| serve("ordinary", ordinary, nexus.serve_ordinary(ordinary)));
        serve("meta", meta, nexus.serve_meta(meta));
    });
    drop(nexus);
    if failed.load(Ordering::SeqCst) {
        ExitCode::FAILURE
    } else {
        ExitCode::SUCCESS
    }
}

struct ConfigurationStartup;

trait RunsConfigurationStartup {
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
        (!ConfigurationStartup::admits_configuration_peer(
            peer_present,
            configured_message_executable,
            marked_herdr_pane,
        ))
        .then_some(MetaRefusal::PeerUnknown)
    }

    fn peer_is_authorized_for_configuration(
        peer: Option<flow_nexus::caller::CallerProcess>,
        message_nexus_binary: &str,
    ) -> Option<MetaRefusal> {
        let Some(peer) = peer else {
            return ConfigurationStartup::configuration_peer_refusal(false, false, false);
        };
        if !message_nexus_binary.is_empty() {
            let configured = fs::canonicalize(message_nexus_binary).ok();
            let executable = fs::canonicalize(format!("/proc/{}/exe", peer.process_id)).ok();
            if configured.is_some() && executable == configured {
                return ConfigurationStartup::configuration_peer_refusal(true, true, false);
            }
        }
        // Configuration-only startup has not built the ordinary pane readers, so
        // it can admit the owner or configured Message but cannot prove a pane
        // peer's configured MetaAspects membership.
        ConfigurationStartup::configuration_peer_refusal(
            true,
            false,
            peer.marked_caller_pane().is_some(),
        )
    }

    fn configuration_listener(socket: &Path) -> Result<UnixListener, String> {
        ClaimedSocket::bind(socket)
    }

    fn serve_one_configuration(
        listener: UnixListener,
        store: &ConfigurationOnlyStore,
        message_nexus_binary: &str,
        stopping: &AtomicBool,
    ) -> Result<(), String> {
        let (mut peer, _) = listener.accept().map_err(|error| error.to_string())?;
        // A stop wakes this accept with its own connection.
        if stopping.load(Ordering::SeqCst) {
            return Err("stopped by signal before any configuration arrived".into());
        }
        peer.set_read_timeout(Some(Duration::from_secs(5)))
            .and_then(|_| peer.set_write_timeout(Some(Duration::from_secs(5))))
            .map_err(|error| error.to_string())?;
        let query = peer.read_meta_query()?;
        if let Some(refusal) = ConfigurationStartup::peer_is_authorized_for_configuration(
            peer.peer_process(),
            message_nexus_binary,
        ) {
            peer.write_meta_response(&MetaResponse::MetaRefused(refusal))?;
            return Err("meta configuration peer was not provably authorized".into());
        }
        let response = match query {
            MetaQuery::Configure(change) => match store.configure_change(change) {
                Ok(outcome) => MetaResponse::Configured(outcome),
                Err(StoreError::DuplicateConfigurationEntry) => {
                    MetaResponse::ConfigureRejected(ConfigureRejection::DuplicateEntry)
                }
                Err(StoreError::CodexModelsOverlap) => {
                    MetaResponse::ConfigureRejected(ConfigureRejection::CodexModelsOverlap)
                }
                Err(StoreError::LeaveOutNotForHarness) => {
                    MetaResponse::ConfigureRejected(ConfigureRejection::LeaveOutNotForHarness)
                }
                Err(StoreError::TopicRefused(topic)) => {
                    MetaResponse::ConfigureRejected(ConfigureRejection::TopicRefused(topic))
                }
                Err(error) => {
                    MetaResponse::Refused(meta_signal_flow::StoreRefusal::Store(error.to_string()))
                }
            },
            _ => {
                return Err("configuration-only listener accepts one Configure request".into());
            }
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
        start: &flow_ethos::Start,
    ) -> Result<(ConfigurationOnlyStore, String, UnixListener), ConfigureOnlyError> {
        let store_path = Path::new(&start.store_path);
        let store = ConfigurationOnlyStore::open(store_path).map_err(ConfigureOnlyError::Store)?;
        let message_nexus_binary = store
            .configuration_snapshot()
            .map_err(ConfigureOnlyError::Store)?
            .map(|snapshot| snapshot.nexus.message_nexus_binary)
            .unwrap_or_default();
        // Configure cannot retarget the socket while this one-request process is
        // serving. The store's same-file writer is held before stale-socket
        // removal or binding; this listener uses only the caller's typed Start.
        let listener =
            ConfigurationStartup::configuration_listener(Path::new(&start.meta_socket_path))
                .map_err(ConfigureOnlyError::Other)?;
        Ok((store, message_nexus_binary, listener))
    }

    fn run_configuration_only(
        start: &flow_ethos::Start,
        signals: &TerminationSignals,
    ) -> Result<(), ConfigureOnlyError> {
        let (store, message_nexus_binary, listener) =
            ConfigurationStartup::open_configuration_listener(start)?;
        let socket = Path::new(&start.meta_socket_path);
        let stopping = AtomicBool::new(false);
        let finished = AtomicBool::new(false);
        let served = std::thread::scope(|scope| {
            scope.spawn(|| {
                if signals.watch(&|| finished.load(Ordering::SeqCst)) {
                    stopping.store(true, Ordering::SeqCst);
                    let _ = UnixStream::connect(socket);
                }
            });
            let served = ConfigurationStartup::serve_one_configuration(
                listener,
                &store,
                &message_nexus_binary,
                &stopping,
            );
            finished.store(true, Ordering::SeqCst);
            served
        });
        let _ = fs::remove_file(socket);
        // The store closes here, whether the request committed or not.
        drop(store);
        served.map_err(ConfigureOnlyError::Other)
    }

    fn start_store_refusal_datom(path: &str, reason: &str) -> Result<String, String> {
        let path = path.to_owned().datomize(vec![]).protosize().compact();
        let reason = reason.to_owned().datomize(vec![]).protosize().compact();
        // StartRefusal is the Rust root type; the Store payload is its datom.
        let source = format!("Store.{{ {path} {reason} }}");
        let mut budget = Budget {
            remaining: 65_536,
            reader: ReaderBudget { remaining: 65_536 },
            depth: 0,
            maximum_depth: 1_024,
        };
        Potential::<flow_ethos::StartRefusal>::from(source)
            .actualize(&mut budget)
            .map(|refusal| refusal.datomize(vec![]).protosize().compact())
            .map_err(|error| format!("cannot encode typed Flow Start refusal: {error:?}"))
    }

    fn report_unrecognized_store(path: &str, reason: &str) {
        match ConfigurationStartup::start_store_refusal_datom(path, reason) {
            Ok(refusal) => println!("{refusal}"),
            Err(error) => eprintln!("flow-nexus: {error}"),
        }
    }
}

impl RunsConfigurationStartup for ConfigurationStartup {}

#[cfg(test)]
mod tests {
    use super::{AnswersVersion, ParsesStart};
    use super::{ConfigurationStartup, RunsConfigurationStartup};
    use datom_codec::{Actualizing, Budget, Datomizable, Potential};
    use flow_nexus::store::ConfiguresOnlyStore;
    use flow_nexus::{CarriesSignalFrames, store::ConfigurationOnlyStore};
    use meta_signal_flow::{
        ConfigurationChange, ConfigureNexus, Query as MetaQuery, Response as MetaResponse,
    };
    use protos::{Compactable, Protosizable, ReaderBudget};
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
    fn start_is_one_typed_datom_and_has_no_positional_socket_override() {
        let expected = flow_ethos::Start {
            ordinary_socket_path: "/run/user/test/flow.sock".into(),
            meta_socket_path: "/run/user/test/flow-meta.sock".into(),
            store_path: "/home/test/.local/state/flow-next/flow.sema".into(),
            landing_watch_option: Some(flow_ethos::LandingWatch {
                context_repository: "/git/psyche-context".into(),
                deploy_binary: "/nix/store/x-context-deploy/bin/context-deploy".into(),
            }),
        };
        let datom = expected.datomize(vec![]).protosize().compact();
        assert_eq!(
            [datom.clone()].parse_start().expect("Start datom parses"),
            expected
        );
        assert!([datom, "ignored.sock".into()].parse_start().is_err());
        assert!(Vec::<String>::new().parse_start().is_err());
        assert!(["Start.{".into()].parse_start().is_err());
    }

    #[test]
    fn literal_start_argument_is_an_unheaded_struct() {
        let expected = flow_ethos::Start {
            ordinary_socket_path: "/run/flow.sock".into(),
            meta_socket_path: "/run/flow-meta.sock".into(),
            store_path: "/state/flow.sema".into(),
            landing_watch_option: None,
        };
        for literal in [
            "{ /run/flow.sock /run/flow-meta.sock /state/flow.sema None }",
            "{ «/run/flow.sock» «/run/flow-meta.sock» «/state/flow.sema» None }",
        ] {
            assert_eq!([literal.to_owned()].parse_start().unwrap(), expected);
        }
        assert!(
            ["Start.{ /run/flow.sock /run/flow-meta.sock /state/flow.sema None }".into()]
                .parse_start()
                .is_err()
        );
        assert!(
            ["{ /run/flow.sock /run/flow-meta.sock /state/flow.sema }".into()]
                .parse_start()
                .is_err(),
            "every position is written, the landing watch included"
        );
        let watched = ["{ /run/flow.sock /run/flow-meta.sock /state/flow.sema Some.{ /git/psyche-context /nix/store/x/bin/context-deploy } }".to_owned()]
            .parse_start()
            .expect("a landing watch parses");
        assert_eq!(
            watched.landing_watch_option,
            Some(flow_ethos::LandingWatch {
                context_repository: "/git/psyche-context".into(),
                deploy_binary: "/nix/store/x/bin/context-deploy".into(),
            })
        );
        let spaced = ["{ «/run/o s» «/run/m s» «/state/f s» None }".to_owned()]
            .parse_start()
            .expect("delimited path strings preserve spaces");
        assert_eq!(spaced.ordinary_socket_path, "/run/o s");
        assert_eq!(spaced.meta_socket_path, "/run/m s");
        assert_eq!(spaced.store_path, "/state/f s");
    }

    #[test]
    fn usage_example_can_be_passed_as_one_start_argument() {
        let usage = Vec::<String>::new().parse_start().unwrap_err();
        let example = usage
            .split_once('\'')
            .and_then(|(_, example)| example.strip_suffix('\''))
            .expect("usage contains one shell-quoted argument");
        let parsed = [example.to_owned()]
            .parse_start()
            .expect("the advertised argument has the supported Start shape");
        assert_eq!(parsed.ordinary_socket_path, "ordinary-socket");
        assert_eq!(parsed.meta_socket_path, "meta-socket");
        assert_eq!(parsed.store_path, "store-path");
        assert_eq!(
            parsed.landing_watch_option,
            Some(flow_ethos::LandingWatch {
                context_repository: "context-repository".into(),
                deploy_binary: "deploy-executable".into(),
            })
        );
    }

    #[test]
    fn unrecognized_store_refusal_is_one_typed_datom_with_its_path_and_reason() {
        let refusal = ConfigurationStartup::start_store_refusal_datom(
            "/home/test/old flow.sema",
            "Sema schema version is Some(1), expected 2",
        )
        .expect("Start refusal is an authored Flow datom");
        let mut budget = Budget {
            remaining: 65_536,
            reader: ReaderBudget { remaining: 65_536 },
            depth: 0,
            maximum_depth: 1_024,
        };
        let parsed = Potential::<flow_ethos::StartRefusal>::from(refusal.clone())
            .actualize(&mut budget)
            .expect("refusal datom parses as the authored type");
        assert!(refusal.starts_with("Store.{ "));
        assert!(!refusal.starts_with("StartRefusal."));
        assert!(!refusal.starts_with("Refused.Store."));
        assert_eq!(
            parsed.datomize(vec![]).protosize().compact(),
            refusal,
            "the path with spaces and the exact observed reason survive canonical output"
        );
    }

    #[test]
    fn configuration_peer_gate_refuses_unknown_and_pane_peers() {
        assert!(matches!(
            ConfigurationStartup::configuration_peer_refusal(false, false, false),
            Some(meta_signal_flow::MetaRefusal::PeerUnknown)
        ));
        assert!(matches!(
            ConfigurationStartup::configuration_peer_refusal(true, false, true),
            Some(meta_signal_flow::MetaRefusal::PeerUnknown)
        ));
        assert!(ConfigurationStartup::configuration_peer_refusal(true, false, false).is_none());
        assert!(ConfigurationStartup::configuration_peer_refusal(true, true, true).is_none());
    }

    #[test]
    fn configuration_lock_is_acquired_before_stale_socket_removal() {
        let directory = tempfile::tempdir().expect("temporary home");
        let start = flow_ethos::Start {
            ordinary_socket_path: directory
                .path()
                .join("custom/flow.sock")
                .to_string_lossy()
                .into_owned(),
            meta_socket_path: directory
                .path()
                .join("custom/meta.sock")
                .to_string_lossy()
                .into_owned(),
            store_path: directory
                .path()
                .join("custom/flow.sema")
                .to_string_lossy()
                .into_owned(),
            landing_watch_option: None,
        };
        let store = ConfigurationOnlyStore::open(Path::new(&start.store_path)).expect("store lock");
        let socket = PathBuf::from(&start.meta_socket_path);
        std::fs::create_dir_all(socket.parent().expect("socket parent")).expect("socket directory");
        let stale = UnixListener::bind(&socket).expect("create socket inode");
        drop(stale);
        assert!(UnixStream::connect(&socket).is_err(), "socket is stale");

        assert!(ConfigurationStartup::open_configuration_listener(&start).is_err());
        assert!(
            std::fs::symlink_metadata(&socket)
                .expect("socket inode remains")
                .file_type()
                .is_socket()
        );
        drop(store);
    }

    fn nexus_change(binary: &str, message_socket: &str) -> ConfigurationChange {
        let codex = |model: &str| meta_signal_flow::CodexEndpoint {
            client_path: "/nix/store/codex/bin/codex".into(),
            home: "/tmp/codex".into(),
            control_socket_path: "/tmp/codex/app-server.sock".into(),
            model_name_vector: vec![model.into()],
        };
        ConfigurationChange::Nexus(ConfigureNexus {
            source_root: "/tmp/source".into(),
            stable_codex: codex("gpt-stable-test"),
            next_codex: codex("gpt-next-test"),
            harness_profiles: vec![],
            meta_aspects: vec![meta_signal_flow::Aspect::Psyche],
            message_nexus_path: message_socket.into(),
            message_nexus_binary: binary.into(),
            lease: 60,
        })
    }

    #[test]
    fn configuration_listener_commits_one_typed_change_and_exits() {
        let directory = tempfile::tempdir().expect("temporary home");
        let executable = std::env::current_exe()
            .expect("test executable")
            .canonicalize()
            .expect("canonical test executable");
        let start = flow_ethos::Start {
            ordinary_socket_path: directory
                .path()
                .join("flow.sock")
                .to_string_lossy()
                .into_owned(),
            meta_socket_path: directory
                .path()
                .join("runtime/flow-meta.sock")
                .to_string_lossy()
                .into_owned(),
            store_path: directory
                .path()
                .join("store/configured.flow.sema")
                .to_string_lossy()
                .into_owned(),
            landing_watch_option: None,
        };
        let (store, message_binary, listener) =
            ConfigurationStartup::open_configuration_listener(&start)
                .expect("configuration listener opens");
        assert!(
            message_binary.is_empty(),
            "fresh stores have no Message identity"
        );
        let socket = PathBuf::from(&start.meta_socket_path);
        let ordinary_socket = PathBuf::from(&start.ordinary_socket_path);
        assert!(
            UnixStream::connect(&ordinary_socket).is_err(),
            "configuration-only startup does not expose the ordinary adapter"
        );
        let change = nexus_change(&executable.to_string_lossy(), "/tmp/message.sock");
        let expected_nexus = match &change {
            ConfigurationChange::Nexus(nexus) => nexus.clone(),
            _ => unreachable!(),
        };
        let server = std::thread::spawn(move || {
            ConfigurationStartup::serve_one_configuration(
                listener,
                &store,
                &message_binary,
                &std::sync::atomic::AtomicBool::new(false),
            )
        });

        let mut peer = UnixStream::connect(&socket).expect("connect to config-only listener");
        let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&MetaQuery::Configure(change))
            .expect("encode one Configure request");
        peer.write_frame(&bytes)
            .expect("send one Configure request");
        let reply = peer.read_frame().expect("read Configure response");
        let response = rkyv::from_bytes::<MetaResponse, rkyv::rancor::Error>(&reply)
            .expect("decode Configure response");
        assert!(matches!(
            response,
            MetaResponse::Configured(meta_signal_flow::ConfigureOutcome::Replaced)
        ));
        assert!(server.join().expect("server thread joins").is_ok());
        assert_eq!(
            super::ConfigurationOnlyStore::open(Path::new(&start.store_path))
                .expect("reopen committed configuration")
                .configuration_snapshot()
                .expect("snapshot reads")
                .expect("configuration is present")
                .nexus,
            expected_nexus
        );
        assert!(
            UnixStream::connect(&socket).is_err(),
            "one-request listener has exited"
        );

        let (store, configured_message_binary, listener) =
            ConfigurationStartup::open_configuration_listener(&start)
                .expect("the same typed Start reopens the configuration listener");
        assert_eq!(
            configured_message_binary,
            executable.to_string_lossy().into_owned()
        );
        let server = std::thread::spawn(move || {
            ConfigurationStartup::serve_one_configuration(
                listener,
                &store,
                &configured_message_binary,
                &std::sync::atomic::AtomicBool::new(false),
            )
        });
        let mut peer = UnixStream::connect(&socket).expect("reconnect to same typed Start");
        let mut changed_nexus = expected_nexus.clone();
        changed_nexus.message_nexus_binary = "/nix/store/other-message/bin/message".into();
        let changed = ConfigurationChange::Nexus(changed_nexus.clone());
        let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&MetaQuery::Configure(changed))
            .expect("encode changed Nexus configuration");
        peer.write_frame(&bytes)
            .expect("send changed Nexus configuration");
        let reply = peer.read_frame().expect("read replacement response");
        let response = rkyv::from_bytes::<MetaResponse, rkyv::rancor::Error>(&reply)
            .expect("decode replacement response");
        assert!(matches!(
            response,
            MetaResponse::Configured(meta_signal_flow::ConfigureOutcome::Replaced)
        ));
        assert!(server.join().expect("second server joins").is_ok());
        assert_eq!(
            super::ConfigurationOnlyStore::open(Path::new(&start.store_path))
                .expect("reopen replaced configuration")
                .configuration_snapshot()
                .expect("snapshot rereads")
                .expect("configuration remains present")
                .nexus,
            changed_nexus,
            "a declared Nexus replaces the committed Nexus whole"
        );
    }
}
