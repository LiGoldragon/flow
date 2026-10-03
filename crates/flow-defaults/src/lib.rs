//! The one home of Flow's default layout: every path the Nexus starts from
//! and every client looks for, anchored on the user's home and runtime
//! directory. The Nexus seeds a new store from it and takes changes only over
//! its meta socket; the clients reach the Nexus's default sockets through it.

use std::{
    os::unix::fs::MetadataExt,
    path::{Path, PathBuf},
};

/// The two anchors the default configuration is derived from: the user's
/// home (`HOME`, else the password database) and runtime directory
/// (`XDG_RUNTIME_DIR`, else `/run/user/<uid>`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DefaultConfiguration {
    pub home: PathBuf,
    pub runtime_directory: PathBuf,
}

/// One Codex app-server endpoint as the defaults name it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DefaultCodexEndpoint {
    pub client_path: String,
    pub home: String,
    pub socket: String,
    pub model_names: Vec<String>,
}

/// The process's own user, the one whose anchors are read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProcessUser {
    pub user_id: u32,
}

/// Reads who this process runs as and where its home lies.
pub trait IdentifiesProcessUser: Sized {
    fn of_this_process() -> Self;
    /// The home the password database names for this user.
    fn password_database_home(&self) -> Option<PathBuf>;
    /// The runtime directory the platform gives this user when none is set.
    fn platform_runtime_directory(&self) -> PathBuf;
}

impl IdentifiesProcessUser for ProcessUser {
    fn of_this_process() -> Self {
        Self {
            user_id: std::fs::metadata("/proc/self")
                .map(|metadata| metadata.uid())
                .unwrap_or(0),
        }
    }

    fn password_database_home(&self) -> Option<PathBuf> {
        let database = std::fs::read_to_string("/etc/passwd").ok()?;
        let user_id = self.user_id.to_string();
        database.lines().find_map(|line| {
            let fields = line.split(':').collect::<Vec<_>>();
            (fields.len() >= 6 && fields[2] == user_id).then(|| PathBuf::from(fields[5]))
        })
    }

    fn platform_runtime_directory(&self) -> PathBuf {
        PathBuf::from(format!("/run/user/{}", self.user_id))
    }
}

/// Reads the two anchors from the process's environment.
pub trait ReadsAnchors: Sized {
    fn from_environment() -> Self;
}

impl ReadsAnchors for DefaultConfiguration {
    fn from_environment() -> Self {
        let user = ProcessUser::of_this_process();
        let home = std::env::var_os("HOME")
            .map(PathBuf::from)
            .filter(|home| home.is_absolute())
            .or_else(|| user.password_database_home())
            .unwrap_or_else(|| PathBuf::from("/"));
        let runtime_directory = std::env::var_os("XDG_RUNTIME_DIR")
            .map(PathBuf::from)
            .filter(|directory| directory.is_absolute())
            .unwrap_or_else(|| user.platform_runtime_directory());
        Self {
            home,
            runtime_directory,
        }
    }
}

/// Every default path, derived from the anchors and nothing else; no path
/// names a particular user.
pub trait LaysOutDefaults {
    const STATE_DIRECTORY: &str = ".local/state/flow";
    const STORE_FILE: &str = "flow.sema";
    const LAUNCH_BUNDLE_DIRECTORY: &str = "launch-bundles";
    const SOCKET_DIRECTORY: &str = "flow";
    const ORDINARY_SOCKET: &str = "flow.sock";
    const META_SOCKET: &str = "flow-meta.sock";
    const SOURCE_ROOT: &str = "primary";
    const STABLE_CODEX_CLIENT: &str = "codex-stable-flow-client";
    const STABLE_CODEX_HOME: &str = ".codex";
    const STABLE_CODEX_MODELS: [&str; 3] = ["gpt-5.6-terra", "gpt-5.6-sol", "gpt-5.6-luna"];
    const NEXT_CODEX_CLIENT: &str = "codex-next-flow-client";
    const NEXT_CODEX_HOME: &str = ".codex-next";
    const NEXT_CODEX_MODELS: [&str; 3] = ["gpt-6-sol", "gpt-6-luna", "gpt-6-astra"];
    const CODEX_CONTROL_SOCKET: &str = "app-server-control/app-server-control.sock";

    fn home(&self) -> &Path;
    fn runtime_directory(&self) -> &Path;

    fn state_directory(&self) -> PathBuf {
        self.home().join(Self::STATE_DIRECTORY)
    }

    /// Where the Nexus writes each launch's own copy of the system-prompt
    /// bundle.
    fn launch_bundle_directory(&self) -> PathBuf {
        self.state_directory().join(Self::LAUNCH_BUNDLE_DIRECTORY)
    }

    fn store_path(&self) -> PathBuf {
        self.state_directory().join(Self::STORE_FILE)
    }

    fn socket_directory(&self) -> PathBuf {
        self.runtime_directory().join(Self::SOCKET_DIRECTORY)
    }

    fn ordinary_socket_path(&self) -> PathBuf {
        self.socket_directory().join(Self::ORDINARY_SOCKET)
    }

    fn meta_socket_path(&self) -> PathBuf {
        self.socket_directory().join(Self::META_SOCKET)
    }

    fn source_root(&self) -> PathBuf {
        self.home().join(Self::SOURCE_ROOT)
    }

    fn stable_codex(&self) -> DefaultCodexEndpoint {
        self.codex_endpoint(
            Self::STABLE_CODEX_CLIENT,
            Self::STABLE_CODEX_HOME,
            &Self::STABLE_CODEX_MODELS,
        )
    }

    fn next_codex(&self) -> DefaultCodexEndpoint {
        self.codex_endpoint(
            Self::NEXT_CODEX_CLIENT,
            Self::NEXT_CODEX_HOME,
            &Self::NEXT_CODEX_MODELS,
        )
    }

    fn codex_endpoint(&self, client: &str, home: &str, models: &[&str]) -> DefaultCodexEndpoint {
        let home = self.home().join(home);
        DefaultCodexEndpoint {
            client_path: client.into(),
            socket: home
                .join(Self::CODEX_CONTROL_SOCKET)
                .to_string_lossy()
                .into_owned(),
            home: home.to_string_lossy().into_owned(),
            model_names: models.iter().map(|model| (*model).to_owned()).collect(),
        }
    }
}

impl LaysOutDefaults for DefaultConfiguration {
    fn home(&self) -> &Path {
        &self.home
    }

    fn runtime_directory(&self) -> &Path {
        &self.runtime_directory
    }
}

#[cfg(test)]
mod tests {
    use super::{DefaultConfiguration, LaysOutDefaults};
    use std::path::Path;

    #[test]
    fn sockets_and_store_hang_from_the_two_anchors() {
        let defaults = DefaultConfiguration {
            home: "/home/someone".into(),
            runtime_directory: "/run/user/4242".into(),
        };
        assert_eq!(
            defaults.ordinary_socket_path(),
            Path::new("/run/user/4242/flow/flow.sock")
        );
        assert_eq!(
            defaults.meta_socket_path(),
            Path::new("/run/user/4242/flow/flow-meta.sock")
        );
        assert_eq!(
            defaults.store_path(),
            Path::new("/home/someone/.local/state/flow/flow.sema")
        );
        assert_eq!(
            defaults.stable_codex().socket,
            "/home/someone/.codex/app-server-control/app-server-control.sock"
        );
    }
}
