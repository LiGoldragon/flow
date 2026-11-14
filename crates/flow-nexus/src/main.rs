use datom_codec::{Actualizing, Budget, Datomizable, Potential};
use flow_nexus::place::{FindsPlace, LaysOutPlace, PlaceName, PlaceRefusal};
use flow_nexus::placed_socket::{PlacedSocket, PlacesSocket};
use flow_nexus::{
    AnnouncesReadiness, OpensRunningNexus, Readiness, RunningNexus, ServesMeta, ServesOrdinary,
    StopsNexus, TerminationSignals, WatchesTerminationSignals,
    launching::ReconcilesInterruptedLaunches, send_tool::ServesSendTool, store::StoreError,
    waking::SleepsAndWakesSeats,
};
use protos::{Compactable, Protosizable, ReaderBudget};
use std::{
    fs,
    path::Path,
    process::ExitCode,
    sync::atomic::{AtomicBool, Ordering},
};

/// `--version` is the only command that runs without a place name.
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

/// A Nexus is started with one datom, its place's name. Its place, where
/// its sockets and its memory are, is read from its place file before any
/// socket opens, or lies at its defaults where there is no file; the
/// executable holds no configuration, and a new memory starts unconfigured.
trait ParsesPlaceName {
    fn parse_place_name(&self) -> Result<PlaceName, String>;
}

impl ParsesPlaceName for [String] {
    fn parse_place_name(&self) -> Result<PlaceName, String> {
        let [source] = self else {
            return Err("usage: flow-nexus-daemon «place-name»".into());
        };
        let mut budget = Budget {
            remaining: 65_536,
            reader: ReaderBudget::try_from(65_536).expect("a constant reader budget"),
            depth: 0,
            maximum_depth: 1_024,
        };
        Potential::<PlaceName>::from(source.clone())
            .actualize(&mut budget)
            .map_err(|error| format!("invalid place name datom: {error:?}"))
    }
}

/// A place refused, as the Nexus reports it: a place file's refusal as its
/// datom, naming the file.
trait ReportsPlaceRefusal {
    fn report(&self) -> String;
}

impl ReportsPlaceRefusal for PlaceRefusal {
    fn report(&self) -> String {
        match self {
            PlaceRefusal::File(refusal) => refusal.datomize(vec![]).protosize().compact(),
            PlaceRefusal::Defaults(error) => {
                format!("no place file, and no default place: {error}")
            }
        }
    }
}

/// A place's two directories are declared by the environment and shared
/// with every other Nexus at the place: the Nexus finds them and never makes
/// or removes them.
trait FindsPlaceDirectory {
    /// The directory exists; otherwise the Nexus does not start.
    fn find(&self, place_name: &str) -> Result<(), String>;
}

impl FindsPlaceDirectory for Path {
    fn find(&self, place_name: &str) -> Result<(), String> {
        if self.is_dir() {
            Ok(())
        } else {
            Err(format!(
                "place {place_name}: {} is not a directory; the place is declared by the environment",
                self.display()
            ))
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
    let place_name = match arguments.parse_place_name() {
        Ok(place_name) => place_name,
        Err(error) => {
            eprintln!("{error}");
            return ExitCode::FAILURE;
        }
    };
    let place = match place_name.find_place() {
        Ok(place) => place,
        Err(refusal) => {
            eprintln!("flow-nexus: {}", refusal.report());
            return ExitCode::FAILURE;
        }
    };
    for directory in [place.runtime_directory(), place.state_directory()] {
        if let Err(error) = directory.find(&place.place_name.0) {
            eprintln!("flow-nexus: {error}");
            return ExitCode::FAILURE;
        }
    }
    let nexus = match RunningNexus::open(&place) {
        Ok(nexus) => nexus,
        Err(StoreError::UnrecognizedMemory { path, reason }) => {
            StartupRefusal::report_unrecognized_store(&path, &reason);
            return ExitCode::FAILURE;
        }
        Err(error) => {
            eprintln!("flow-nexus: store {}: {error}", place.store().display());
            return ExitCode::FAILURE;
        }
    };
    let ordinary = place.ordinary_socket();
    let meta = place.meta_socket();
    let ordinary = ordinary.as_path();
    let meta = meta.as_path();
    let sockets = [ordinary, meta];
    let failed = AtomicBool::new(false);
    let serve = |surface: &str, socket: &Path, served: Result<(), String>| {
        if let Err(error) = served {
            eprintln!("flow-nexus: {surface} socket {}: {error}", socket.display());
            failed.store(true, Ordering::SeqCst);
            nexus.stop(&sockets);
        }
    };
    // Both sockets are placed, each at its name only once it listens,
    // before anything is served, and readiness is announced only once they
    // are: a unit ordered after this one, or a peer that saw a name moved
    // in, finds both listening.
    let bound = |socket: &Path| {
        PlacedSocket::at(socket)
            .and_then(|placed| placed.claim())
            .inspect_err(|error| eprintln!("flow-nexus: {error}"))
    };
    let Ok(ordinary_listener) = bound(ordinary) else {
        return ExitCode::FAILURE;
    };
    let Ok(meta_listener) = bound(meta) else {
        drop(ordinary_listener);
        let _ = fs::remove_file(ordinary);
        return ExitCode::FAILURE;
    };
    // A Codex launch the last Nexus left mid-way, its first turn perhaps
    // pending on a send call, is refused and settled before anything is
    // served.
    nexus.reconcile_interrupted_launches();
    if let Err(error) = Readiness::announce_ready() {
        eprintln!("flow-nexus: readiness not announced: {error}");
    }
    // Every thread borrows the Nexus inside this scope, so when a stop has
    // ended them all the Nexus drops here and its store closes cleanly.
    std::thread::scope(|scope| {
        // One connection per live Codex thread answers its send-tool calls.
        let send_tool_nexus = &nexus;
        scope.spawn(move || send_tool_nexus.serve_send_tool(scope));
        // One reap worker per pending reap whose pane step is owed takes the
        // reap's Herdr steps outside the dispatch gate.
        let reap_nexus = &nexus;
        scope.spawn(move || {
            use flow_nexus::reaping::ServesReapedPaneWatches;
            reap_nexus.serve_reaped_pane_watches(scope)
        });
        // Work the last Nexus owed is taken up while the sockets serve:
        // queued deliveries are reconciled, every successor owed is started
        // or resumed, and every metaflow due a wake is woken.
        scope.spawn(|| nexus.recover_wakes());
        // Reminders held in Memory are sent as they fall due.
        scope.spawn(|| {
            use flow_nexus::reminding::RemindsCallers;
            nexus.keep_reminding()
        });
        scope.spawn(|| {
            use flow_nexus::heartbeat::RunsHeartbeat;
            nexus.keep_heartbeating()
        });
        scope.spawn(|| {
            if signals.watch(&nexus.stopping) {
                nexus.stop(&sockets);
            }
        });
        scope.spawn(|| {
            serve(
                "ordinary",
                ordinary,
                nexus.serve_ordinary_on(ordinary_listener, ordinary),
            )
        });
        serve("meta", meta, nexus.serve_meta_on(meta_listener, meta));
    });
    drop(nexus);
    if failed.load(Ordering::SeqCst) {
        ExitCode::FAILURE
    } else {
        ExitCode::SUCCESS
    }
}

/// A store this Flow does not open, refused as one typed datom.
struct StartupRefusal;

trait RefusesStartup {
    fn start_store_refusal_datom(path: &str, reason: &str) -> Result<String, String> {
        let path = path.to_owned().datomize(vec![]).protosize().compact();
        let reason = reason.to_owned().datomize(vec![]).protosize().compact();
        // StartRefusal is the Rust root type; the Store payload is its datom.
        let source = format!("Store.{{ {path} {reason} }}");
        let mut budget = Budget {
            remaining: 65_536,
            reader: ReaderBudget::try_from(65_536).expect("a constant reader budget"),
            depth: 0,
            maximum_depth: 1_024,
        };
        Potential::<flow_ethos::StartRefusal>::from(source)
            .actualize(&mut budget)
            .map(|refusal| refusal.datomize(vec![]).protosize().compact())
            .map_err(|error| format!("cannot encode typed Flow Start refusal: {error:?}"))
    }

    fn report_unrecognized_store(path: &str, reason: &str) {
        match StartupRefusal::start_store_refusal_datom(path, reason) {
            Ok(refusal) => println!("{refusal}"),
            Err(error) => eprintln!("flow-nexus: {error}"),
        }
    }
}

impl RefusesStartup for StartupRefusal {}

#[cfg(test)]
mod tests {
    use super::{AnswersVersion, ParsesPlaceName, RefusesStartup, StartupRefusal};
    use datom_codec::{Actualizing, Budget, Datomizable, Potential};
    use flow_nexus::place::PlaceName;
    use protos::{Compactable, Protosizable, ReaderBudget};

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

    /// The Nexus is started with one datom, its place's name, as the shell
    /// writes it: no Runtime, State or override beside it.
    #[test]
    fn a_nexus_is_started_with_one_place_name_datom() {
        let main = PlaceName("main".into());
        for literal in ["main", "«main»"] {
            assert_eq!([literal.to_owned()].parse_place_name().unwrap(), main);
        }
        let datom = main.datomize(vec![]).protosize().compact();
        assert_eq!([datom.clone()].parse_place_name().unwrap(), main);
        assert!([datom, "next".into()].parse_place_name().is_err());
        assert!(Vec::<String>::new().parse_place_name().is_err());
        assert!(
            ["{ main /run/user/1001/nexus/main /home/li/.local/state/nexus/main }".into()]
                .parse_place_name()
                .is_err(),
            "the earlier place datom is no place name"
        );
    }

    #[test]
    fn unrecognized_store_refusal_is_one_typed_datom_with_its_path_and_reason() {
        let refusal = StartupRefusal::start_store_refusal_datom(
            "/home/test/old flow.sema",
            "Sema schema version is Some(1), expected 2",
        )
        .expect("Start refusal is an authored Flow datom");
        let mut budget = Budget {
            remaining: 65_536,
            reader: ReaderBudget::try_from(65_536).expect("a constant reader budget"),
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
}
