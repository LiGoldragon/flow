//! Where a Flow Nexus lives: the sockets it makes in its place's Runtime
//! directory, and the root a client resolves a place name under.
//!
//! A Nexus is started with its place and reads nothing from the
//! environment. A client is named after the Nexus and given the place name:
//! the place's Runtime is `$XDG_RUNTIME_DIR/nexus/<place-name>/`, the root
//! taken from the standard variable and from nothing else.

use std::path::{Path, PathBuf};

/// The socket file names a Flow Nexus makes in its Runtime directory.
pub struct PlaceSockets;

pub trait NamesPlaceSockets {
    /// The ordinary socket.
    const ORDINARY: &'static str;
    /// The meta socket.
    const META: &'static str;
    fn ordinary_in(runtime: &Path) -> PathBuf {
        runtime.join(Self::ORDINARY)
    }
    fn meta_in(runtime: &Path) -> PathBuf {
        runtime.join(Self::META)
    }
}

impl NamesPlaceSockets for PlaceSockets {
    const ORDINARY: &'static str = "flow.sock";
    const META: &'static str = "flow-meta.sock";
}

/// The directory every place's Runtime lies under by default:
/// `$XDG_RUNTIME_DIR/nexus`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeRoot {
    pub directory: PathBuf,
}

pub trait ResolvesPlace: Sized {
    const NEXUS_DIRECTORY: &'static str = "nexus";
    /// The root under the given runtime directory: the value of
    /// `XDG_RUNTIME_DIR`, which must be an absolute path.
    fn under(runtime_directory: Option<&std::ffi::OsStr>) -> Result<Self, String>;
    /// The Runtime directory of the named place. A place name is one path
    /// component: not empty, not `.` or `..`, holding no `/`.
    fn place(&self, place_name: &str) -> Result<PathBuf, String>;
}

impl ResolvesPlace for RuntimeRoot {
    fn under(runtime_directory: Option<&std::ffi::OsStr>) -> Result<Self, String> {
        let Some(directory) = runtime_directory.map(PathBuf::from) else {
            return Err("XDG_RUNTIME_DIR is not set: no place can be resolved".into());
        };
        if !directory.is_absolute() {
            return Err(format!(
                "XDG_RUNTIME_DIR {} is not absolute: no place can be resolved",
                directory.display()
            ));
        }
        Ok(Self {
            directory: directory.join(Self::NEXUS_DIRECTORY),
        })
    }

    fn place(&self, place_name: &str) -> Result<PathBuf, String> {
        if place_name.is_empty()
            || place_name == "."
            || place_name == ".."
            || place_name.contains('/')
        {
            return Err(format!("{place_name:?} is not a place name"));
        }
        Ok(self.directory.join(place_name))
    }
}

#[cfg(test)]
mod tests {
    use super::{NamesPlaceSockets, PlaceSockets, ResolvesPlace, RuntimeRoot};
    use std::{ffi::OsStr, path::Path};

    #[test]
    fn a_place_name_resolves_under_the_runtime_root_to_its_sockets() {
        let root = RuntimeRoot::under(Some(OsStr::new("/run/user/1001"))).unwrap();
        let main = root.place("main").unwrap();
        assert_eq!(main, Path::new("/run/user/1001/nexus/main"));
        assert_eq!(
            PlaceSockets::ordinary_in(&main),
            Path::new("/run/user/1001/nexus/main/flow.sock")
        );
        assert_eq!(
            PlaceSockets::meta_in(&root.place("next").unwrap()),
            Path::new("/run/user/1001/nexus/next/flow-meta.sock")
        );
        for refused in ["", ".", "..", "main/../next", "/main"] {
            assert!(root.place(refused).is_err(), "{refused:?}");
        }
        assert!(RuntimeRoot::under(None).is_err());
        assert!(RuntimeRoot::under(Some(OsStr::new("run/user/1001"))).is_err());
    }
}
