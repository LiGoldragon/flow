//! Where a Flow Nexus lives: the socket file names it makes in its place's
//! Runtime directory, and the configuration a client reads the place name
//! from.
//!
//! A client is named after the Nexus and takes its datom and nothing else:
//! the place name is its configuration's, one datom at
//! `$XDG_CONFIG_HOME/nexus/flow-nexus.datom`
//! (`~/.config/nexus/flow-nexus.datom` when the variable is unset). The
//! place itself, and with it the Runtime holding the sockets, is read from
//! the Nexus's place file by that name (flow-place), so the place is held
//! once and no client reads `XDG_RUNTIME_DIR`. No place is ever typed.

use crate::generated::library::Reason;
pub use crate::generated::library::{CliConfiguration, ConfigurationFault, ConfigurationRefusal};
use std::{
    ffi::OsStr,
    path::{Path, PathBuf},
};

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

/// What a place name must be to stand in a path.
#[cfg(feature = "datom")]
trait ChecksPlaceName {
    /// A place name is one path component: not empty, not `.` or `..`,
    /// holding no `/`.
    fn check(&self) -> Result<(), String>;
}

#[cfg(feature = "datom")]
impl ChecksPlaceName for crate::generated::library::PlaceName {
    fn check(&self) -> Result<(), String> {
        let name = self.0.as_str();
        if name.is_empty() || name == "." || name == ".." || name.contains('/') {
            return Err(format!("{name:?} is not a place name"));
        }
        Ok(())
    }
}

/// The file a Flow CLI's configuration is read from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigurationFile {
    pub path: PathBuf,
}

pub trait LocatesConfiguration: Sized {
    /// The Nexus the configuration is named after: every Flow CLI, the
    /// ordinary and the meta, reads the same file.
    const NEXUS_NAME: &'static str = "flow-nexus";
    const NEXUS_DIRECTORY: &'static str = "nexus";
    /// `<configuration home>/nexus/flow-nexus.datom`: the configuration home
    /// is the value of `XDG_CONFIG_HOME`, else, unset or empty,
    /// `<HOME>/.config`; either must be an absolute path.
    fn under(
        configuration_home: Option<&OsStr>,
        home: Option<&OsStr>,
    ) -> Result<Self, ConfigurationRefusal>;
}

impl LocatesConfiguration for ConfigurationFile {
    fn under(
        configuration_home: Option<&OsStr>,
        home: Option<&OsStr>,
    ) -> Result<Self, ConfigurationRefusal> {
        // An empty XDG_CONFIG_HOME is an unset one, as the standard has it.
        let configuration_home = configuration_home.filter(|directory| !directory.is_empty());
        let directory = match (configuration_home, home) {
            (Some(directory), _) => PathBuf::from(directory),
            (None, Some(home)) => PathBuf::from(home).join(".config"),
            (None, None) => {
                return Err(ConfigurationRefusal::Unlocated(Reason(
                    "neither XDG_CONFIG_HOME nor HOME is set".into(),
                )));
            }
        };
        if !directory.is_absolute() {
            return Err(ConfigurationRefusal::Unlocated(Reason(format!(
                "the configuration home {} is not absolute",
                directory.display()
            ))));
        }
        Ok(Self {
            path: directory
                .join(Self::NEXUS_DIRECTORY)
                .join(format!("{}.datom", Self::NEXUS_NAME)),
        })
    }
}

#[cfg(feature = "datom")]
pub trait ReadsCliConfiguration {
    /// The configuration the file holds: a file that is missing, cannot be
    /// read, or holds no `CliConfiguration` naming a place is refused.
    fn read(&self) -> Result<CliConfiguration, ConfigurationRefusal>;
}

#[cfg(feature = "datom")]
impl ReadsCliConfiguration for ConfigurationFile {
    fn read(&self) -> Result<CliConfiguration, ConfigurationRefusal> {
        use datom_codec::{Actualizing, Budget, Potential};
        let path = crate::generated::library::Path(self.path.to_string_lossy().into_owned());
        let fault = |reason: String| ConfigurationFault {
            path: path.clone(),
            reason: Reason(reason),
        };
        let source = match std::fs::read_to_string(&self.path) {
            Ok(source) => source,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Err(ConfigurationRefusal::Missing(path));
            }
            Err(error) => {
                return Err(ConfigurationRefusal::Unreadable(fault(error.to_string())));
            }
        };
        let mut budget = Budget {
            remaining: 65_536,
            reader: protos::ReaderBudget::try_from(65_536).expect("a constant reader budget"),
            depth: 0,
            maximum_depth: 64,
        };
        let configuration = Potential::<CliConfiguration>::from(source.trim().to_owned())
            .actualize(&mut budget)
            .map_err(|error| ConfigurationRefusal::Invalid(fault(format!("{error:?}"))))?;
        configuration
            .place_name
            .check()
            .map_err(|reason| ConfigurationRefusal::Invalid(fault(reason)))?;
        Ok(configuration)
    }
}

#[cfg(test)]
mod tests {
    use super::{NamesPlaceSockets, PlaceSockets};
    use std::{ffi::OsStr, path::Path};

    #[test]
    fn a_place_s_sockets_lie_in_its_runtime() {
        let runtime = Path::new("/run/user/1001/nexus/main");
        assert_eq!(
            PlaceSockets::ordinary_in(runtime),
            Path::new("/run/user/1001/nexus/main/flow.sock")
        );
        assert_eq!(
            PlaceSockets::meta_in(runtime),
            Path::new("/run/user/1001/nexus/main/flow-meta.sock")
        );
    }

    #[test]
    fn the_configuration_lies_under_the_configuration_home_else_under_home() {
        use super::{ConfigurationFile, ConfigurationRefusal, LocatesConfiguration};
        assert_eq!(
            ConfigurationFile::under(Some(OsStr::new("/c")), Some(OsStr::new("/home/li")))
                .unwrap()
                .path,
            Path::new("/c/nexus/flow-nexus.datom")
        );
        for unset in [None, Some(OsStr::new(""))] {
            assert_eq!(
                ConfigurationFile::under(unset, Some(OsStr::new("/home/li")))
                    .unwrap()
                    .path,
                Path::new("/home/li/.config/nexus/flow-nexus.datom")
            );
        }
        for (configuration_home, home) in [
            (None, None),
            (Some(OsStr::new("c")), Some(OsStr::new("/home/li"))),
            (None, Some(OsStr::new("home/li"))),
        ] {
            assert!(matches!(
                ConfigurationFile::under(configuration_home, home),
                Err(ConfigurationRefusal::Unlocated(_))
            ));
        }
    }

    /// A directory of the test's own under the system's temporary root,
    /// removed when the test ends.
    #[cfg(feature = "datom")]
    struct Scratch(std::path::PathBuf);

    #[cfg(feature = "datom")]
    impl Scratch {
        fn new(name: &str) -> Self {
            let directory =
                std::env::temp_dir().join(format!("flow-ethos-{name}-{}", std::process::id()));
            let _ = std::fs::remove_dir_all(&directory);
            std::fs::create_dir_all(directory.join("nexus")).unwrap();
            Self(directory)
        }
    }

    #[cfg(feature = "datom")]
    impl Drop for Scratch {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    #[cfg(feature = "datom")]
    #[test]
    fn the_configuration_names_the_place_and_a_bad_one_is_refused_naming_its_path() {
        use super::{
            CliConfiguration, ConfigurationFault, ConfigurationFile, ConfigurationRefusal,
            LocatesConfiguration, ReadsCliConfiguration,
        };
        use crate::generated::library::{Path as PathText, PlaceName};
        let scratch = Scratch::new("configuration");
        let file = ConfigurationFile::under(Some(scratch.0.as_os_str()), None).unwrap();
        let path = PathText(file.path.to_string_lossy().into_owned());
        assert_eq!(
            file.read(),
            Err(ConfigurationRefusal::Missing(path.clone()))
        );
        std::fs::write(&file.path, "{ next }\n").unwrap();
        assert_eq!(
            file.read(),
            Ok(CliConfiguration {
                place_name: PlaceName("next".into())
            })
        );
        for invalid in ["{ next main }", "{ .. }", "{ }", ""] {
            std::fs::write(&file.path, invalid).unwrap();
            assert!(
                matches!(
                    file.read(),
                    Err(ConfigurationRefusal::Invalid(ConfigurationFault { path: named, .. }))
                        if named == path
                ),
                "{invalid:?}: {:?}",
                file.read()
            );
        }
        std::fs::remove_file(&file.path).unwrap();
        std::fs::create_dir(&file.path).unwrap();
        assert!(matches!(
            file.read(),
            Err(ConfigurationRefusal::Unreadable(ConfigurationFault { path: named, .. }))
                if named == path
        ));
    }
}
