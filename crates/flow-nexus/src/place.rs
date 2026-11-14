//! The Nexus's place: its sockets in Runtime, its memory in State. The Place
//! is the one every Nexus shares (translator's placement), read before any
//! socket opens from the place file,
//! `$XDG_CONFIG_HOME/nexus/<place-name>/flow-nexus.signal`, found by the place
//! name and the configuration home and by nothing else: with no place file the
//! Nexus is refused, naming the file (flow-place).

use flow_ethos::place::{NamesPlaceSockets, PlaceSockets};
pub use flow_place::{FindsFlowPlace, ReportsPlaceRefusal};
use std::path::{Path, PathBuf};
pub use translator::placement::{NexusName, Place, PlaceFileRefusal, PlaceName, Runtime, State};

/// What a place lays out on disk.
pub trait LaysOutPlace {
    /// The memory's file name in State.
    const STORE_FILE: &'static str = "flow.sema";
    fn runtime_directory(&self) -> &Path;
    fn state_directory(&self) -> &Path;
    /// `<Runtime>/flow.sock`: the socket every flow this Nexus launches
    /// reports to.
    fn ordinary_socket(&self) -> PathBuf {
        PlaceSockets::ordinary_in(self.runtime_directory())
    }
    /// `<Runtime>/flow-meta.sock`.
    fn meta_socket(&self) -> PathBuf {
        PlaceSockets::meta_in(self.runtime_directory())
    }
    /// `<State>/flow.sema`.
    fn store(&self) -> PathBuf {
        self.state_directory().join(Self::STORE_FILE)
    }
}

impl LaysOutPlace for Place {
    fn runtime_directory(&self) -> &Path {
        Path::new(&self.runtime.0)
    }

    fn state_directory(&self) -> &Path {
        Path::new(&self.state.0)
    }
}

#[cfg(test)]
mod tests {
    use super::{FindsFlowPlace, LaysOutPlace, Place, PlaceFileRefusal, PlaceName, Runtime, State};
    use std::path::Path;
    use translator::place_file::{
        ConfigurationHome, LocatesConfigurationHome, LocatesPlaceFile, WritesPlaceFile,
    };

    fn main_place() -> Place {
        Place {
            place_name: PlaceName("main".into()),
            runtime: Runtime("/run/user/1001/nexus/main".into()),
            state: State("/home/li/.local/state/nexus/main".into()),
        }
    }

    #[test]
    fn a_place_holds_its_sockets_in_runtime_and_its_memory_in_state() {
        let place = main_place();
        assert_eq!(
            place.ordinary_socket(),
            Path::new("/run/user/1001/nexus/main/flow.sock")
        );
        assert_eq!(
            place.meta_socket(),
            Path::new("/run/user/1001/nexus/main/flow-meta.sock")
        );
        assert_eq!(
            place.store(),
            Path::new("/home/li/.local/state/nexus/main/flow.sema")
        );
    }

    /// The place file lies in its place under the configuration home, named
    /// after the Nexus; the place it holds is the place found, and one
    /// holding another place is refused.
    #[test]
    fn the_place_is_read_from_its_place_file() {
        let configuration = tempfile::tempdir().expect("configuration home");
        let home = ConfigurationHome::under(Some(configuration.path().as_os_str()), None)
            .expect("configuration home");
        let main = PlaceName("main".into());
        let file = home
            .place_file(&PlaceName::nexus_name(), &main)
            .expect("located");
        assert_eq!(
            file.path,
            configuration.path().join("nexus/main/flow-nexus.signal")
        );
        assert!(matches!(
            main.find_flow_place_under(&home),
            Err(PlaceFileRefusal::Missing(_))
        ));
        file.write_place(&main_place()).expect("written");
        assert_eq!(main.find_flow_place_under(&home), Ok(main_place()));
        let next = PlaceName("next".into());
        let next_file = home
            .place_file(&PlaceName::nexus_name(), &next)
            .expect("located");
        std::fs::create_dir_all(next_file.path.parent().unwrap()).unwrap();
        std::fs::copy(&file.path, &next_file.path).unwrap();
        assert!(matches!(
            next.find_flow_place_under(&home),
            Err(PlaceFileRefusal::Invalid(_))
        ));
    }
}
