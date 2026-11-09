//! The Nexus's place, the one datom it is started with: its sockets in
//! Runtime, its memory in State.

pub use crate::generated::library::Place;
use flow_ethos::place::{NamesPlaceSockets, PlaceSockets};
use std::path::{Path, PathBuf};

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
        Path::new(&self.runtime)
    }

    fn state_directory(&self) -> &Path {
        Path::new(&self.state)
    }
}

#[cfg(test)]
mod tests {
    use super::{LaysOutPlace, Place};
    use std::path::Path;

    #[test]
    fn a_place_holds_its_sockets_in_runtime_and_its_memory_in_state() {
        let place = Place {
            place_name: "main".into(),
            runtime: "/run/user/1001/nexus/main".into(),
            state: "/home/li/.local/state/nexus/main".into(),
        };
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
}
