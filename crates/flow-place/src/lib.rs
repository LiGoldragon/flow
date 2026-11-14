//! How Flow finds its place. A Flow Nexus and its translator, the CLIs
//! `flow-nexus` and `flow-nexus-meta`, read nothing at start but the Place,
//! and the Place is read from the place file alone:
//! `$XDG_CONFIG_HOME/nexus/<place-name>/flow-nexus.signal`
//! (`$HOME/.config` when the variable is unset or empty), found by the place
//! name and the standard configuration home. The Nexus is started with the
//! place name; a CLI takes it from its configuration, `flow-nexus.datom`.
//! Each then has the Place's Runtime, where the sockets are, and the Nexus its
//! State, where the memory is. There is no fallback: with no place file, or
//! one that cannot be read as this place's, the daemon and the CLIs refuse,
//! naming the file.

pub use translator::place_file::{ConfigurationHome, LocatesConfigurationHome};
pub use translator::placement::{Place, PlaceFileRefusal, PlaceName};

use datom_codec::Datomizable;
use flow_ethos::place::{ConfigurationFile, LocatesConfiguration, ReadsCliConfiguration};
use protos::{Compactable, Protosizable};
use std::{
    ffi::OsString,
    path::{Path, PathBuf},
};
use translator::{place_file::FindsPlace, placement::NexusName};

/// A place found by its name: the Place its Flow place file holds.
pub trait FindsFlowPlace {
    /// The Nexus the place file is named after, `flow-nexus`: the name its
    /// CLI's configuration bears, so a Nexus and its CLIs read one file.
    fn nexus_name() -> NexusName {
        NexusName(<ConfigurationFile as LocatesConfiguration>::NEXUS_NAME.into())
    }
    /// The place the file holds under the configuration home given; with no
    /// file, the refusal naming it.
    fn find_flow_place_under(&self, home: &ConfigurationHome) -> Result<Place, PlaceFileRefusal>;
    /// `find_flow_place_under` this process's configuration home
    /// (`XDG_CONFIG_HOME`, else `HOME`'s `.config`).
    fn find_flow_place(&self) -> Result<Place, PlaceFileRefusal> {
        self.find_flow_place_under(&ConfigurationHome::of_process()?)
    }
}

impl FindsFlowPlace for PlaceName {
    fn find_flow_place_under(&self, home: &ConfigurationHome) -> Result<Place, PlaceFileRefusal> {
        FindsPlace::find_flow_place_under(self, &Self::nexus_name(), home)
    }
}

/// A refusal as the Nexus and its CLIs report it: the refusal's datom,
/// naming the file.
pub trait ReportsPlaceRefusal {
    fn report(&self) -> String;
}

impl ReportsPlaceRefusal for PlaceFileRefusal {
    fn report(&self) -> String {
        self.datomize(vec![]).protosize().compact()
    }
}

/// The environment a Flow CLI reads, and nothing else of it: the standard
/// configuration home, where the CLI's configuration and the place file lie.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct CliEnvironment {
    pub configuration_home: Option<OsString>,
    pub home: Option<OsString>,
}

pub trait ReadsCliEnvironment {
    /// `XDG_CONFIG_HOME` and `HOME` of this process.
    fn of_process() -> Self;
}

impl ReadsCliEnvironment for CliEnvironment {
    fn of_process() -> Self {
        Self {
            configuration_home: std::env::var_os("XDG_CONFIG_HOME"),
            home: std::env::var_os("HOME"),
        }
    }
}

/// The Runtime directory of the place a CLI reaches.
pub trait ResolvesCliPlace {
    /// A refusal is the text the CLI prints: a refusal as its datom, naming
    /// the file at fault.
    fn place_runtime(&self) -> Result<PathBuf, String>;
}

impl ResolvesCliPlace for CliEnvironment {
    /// The place name is the CLI configuration's,
    /// `<configuration home>/nexus/flow-nexus.datom`; the Runtime is the one
    /// the Place holds, read from the place file that name locates. A
    /// missing configuration or place file is refused, naming it.
    fn place_runtime(&self) -> Result<PathBuf, String> {
        let located = |refusal: PlaceFileRefusal| refusal.report();
        let configuration =
            ConfigurationFile::under(self.configuration_home.as_deref(), self.home.as_deref())
                .and_then(|file| file.read())
                .map_err(|refusal| refusal.datomize(vec![]).protosize().compact())?;
        let home =
            ConfigurationHome::under(self.configuration_home.as_deref(), self.home.as_deref())
                .map_err(located)?;
        let place = PlaceName(configuration.place_name.0)
            .find_flow_place_under(&home)
            .map_err(located)?;
        Ok(Path::new(&place.runtime.0).to_path_buf())
    }
}

#[cfg(test)]
mod tests {
    use super::{
        CliEnvironment, ConfigurationHome, FindsFlowPlace, LocatesConfigurationHome, Place,
        PlaceFileRefusal, PlaceName, ResolvesCliPlace,
    };
    use std::path::Path;
    use translator::{
        place_file::{LocatesPlaceFile, WritesPlaceFile},
        placement::{FilePath, Runtime, State},
    };

    fn place(name: &str, runtime: &str) -> Place {
        Place {
            place_name: PlaceName(name.into()),
            runtime: Runtime(runtime.into()),
            state: State(format!("/home/li/.local/state/nexus/{name}")),
        }
    }

    #[test]
    fn the_place_is_found_in_the_place_file_under_the_configuration_home_alone() {
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
        // No file, no place: the refusal names the file.
        assert_eq!(
            main.find_flow_place_under(&home),
            Err(PlaceFileRefusal::Missing(FilePath(
                file.path.to_string_lossy().into_owned()
            )))
        );
        let held = place("main", "/elsewhere/runtime");
        file.write_place(&held).expect("written");
        assert_eq!(main.find_flow_place_under(&home), Ok(held));
    }

    #[test]
    fn a_cli_reaches_the_runtime_its_place_file_holds_and_refuses_without_one() {
        let configuration = tempfile::tempdir().expect("configuration home");
        let environment = CliEnvironment {
            configuration_home: Some(configuration.path().as_os_str().to_owned()),
            home: None,
        };
        let named = configuration.path().join("nexus/flow-nexus.datom");
        // No configuration: refused naming it.
        let refused = environment.place_runtime().unwrap_err();
        assert!(
            refused.starts_with("Missing.") && refused.contains(&*named.to_string_lossy()),
            "{refused}"
        );
        std::fs::create_dir_all(configuration.path().join("nexus")).unwrap();
        std::fs::write(&named, "{ next }\n").unwrap();
        // Its place named, no place file: refused naming the place file.
        let file_path = configuration.path().join("nexus/next/flow-nexus.signal");
        let refused = environment.place_runtime().unwrap_err();
        assert!(
            refused.starts_with("Missing.") && refused.contains(&*file_path.to_string_lossy()),
            "{refused}"
        );
        // The Runtime is the place file's, whatever the standard runtime
        // directory is.
        let home = ConfigurationHome::under(Some(configuration.path().as_os_str()), None).unwrap();
        home.place_file(&PlaceName::nexus_name(), &PlaceName("next".into()))
            .unwrap()
            .write_place(&place("next", "/not/the/default/runtime"))
            .unwrap();
        assert_eq!(
            environment.place_runtime(),
            Ok(Path::new("/not/the/default/runtime").to_path_buf())
        );
    }
}
