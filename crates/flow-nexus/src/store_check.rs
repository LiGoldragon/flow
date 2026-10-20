//! Whether this Flow opens a store, judged before a deploy activates it.
//!
//! Flow's store refuses a Flow older than the identities it holds, so a
//! deploy that activates such a Flow leaves the Nexus refusing its store
//! at every start. The check opens a private copy of the store as the
//! Nexus would, upgrades included, and never touches the store a running
//! Nexus holds. A copy taken while the Nexus wrote may be torn; a refusal
//! that is not the store's identity is judged once more on a fresh copy.

use crate::generated::startup::{ChecksStore, StoreCheck, StoreCheckRequest, StoreRefusal};
use crate::store::{FlowStore, OpensFlowStore, StoreError};
use std::fs;
use std::os::unix::fs::DirBuilderExt;
use std::path::{Path, PathBuf};

/// A private copy of a store, removed with its directory when dropped.
struct StoreCopy {
    directory: PathBuf,
    path: PathBuf,
}

trait CopiesStore: Sized {
    fn copied(store: &Path, attempt: u32) -> std::io::Result<Self>;
}

impl CopiesStore for StoreCopy {
    fn copied(store: &Path, attempt: u32) -> std::io::Result<Self> {
        let directory =
            std::env::temp_dir().join(format!("flow-store-check-{}-{attempt}", std::process::id()));
        if directory.exists() {
            fs::remove_dir_all(&directory)?;
        }
        fs::DirBuilder::new().mode(0o700).create(&directory)?;
        let path = directory.join("flow.sema");
        let copy = Self { directory, path };
        fs::copy(store, &copy.path)?;
        Ok(copy)
    }
}

impl Drop for StoreCopy {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.directory);
    }
}

/// How one open of a copy ended.
enum CopyOpen {
    Opened,
    /// The store holds an identity this Flow does not author.
    Unrecognized(String),
    /// Anything else, a torn copy among the possibilities.
    Failed(String),
}

trait OpensCopy {
    fn open_copy(&self, attempt: u32) -> CopyOpen;
}

impl OpensCopy for Path {
    fn open_copy(&self, attempt: u32) -> CopyOpen {
        let copy = match StoreCopy::copied(self, attempt) {
            Ok(copy) => copy,
            Err(error) => {
                return CopyOpen::Failed(format!("the store could not be copied: {error}"));
            }
        };
        match FlowStore::open_configured(&copy.path) {
            Ok(store) => {
                drop(store);
                CopyOpen::Opened
            }
            Err(StoreError::UnrecognizedMemory { reason, .. }) => CopyOpen::Unrecognized(reason),
            Err(error) => CopyOpen::Failed(error.to_string()),
        }
    }
}

impl ChecksStore for StoreCheckRequest {
    fn check(&self) -> StoreCheck {
        let store = Path::new(&self.store_path);
        if !store.exists() {
            return StoreCheck::Absent(self.store_path.clone());
        }
        let refused = |reason: String| {
            StoreCheck::Refused(StoreRefusal {
                path: self.store_path.clone(),
                reason,
            })
        };
        match store.open_copy(1) {
            CopyOpen::Opened => StoreCheck::Opens(self.store_path.clone()),
            CopyOpen::Unrecognized(reason) => refused(reason),
            CopyOpen::Failed(_) => match store.open_copy(2) {
                CopyOpen::Opened => StoreCheck::Opens(self.store_path.clone()),
                CopyOpen::Unrecognized(reason) | CopyOpen::Failed(reason) => refused(reason),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::DefaultConfiguration;

    /// A store as a deployment leaves it: configured, then opened by the
    /// Nexus once.
    fn configured_store(directory: &Path) -> PathBuf {
        use crate::store::{ConfigurationOnlyStore, ConfiguresOnlyStore, SeedsConfiguration};
        let path = directory.join("flow.sema");
        let settings = DefaultConfiguration {
            home: directory.join("home"),
            runtime_directory: directory.join("runtime"),
        }
        .configuration();
        let config = ConfigurationOnlyStore::open(&path).unwrap();
        config
            .configure_change(meta_signal_flow::ConfigurationChange::Nexus(
                meta_signal_flow::ConfigureNexus {
                    source_root: settings.source_root,
                    stable_codex: settings.stable_codex,
                    next_codex: settings.next_codex,
                    harness_profiles: settings.harness_profile_vector,
                    meta_aspects: settings.meta_aspects,
                    message_nexus_path: settings.message_nexus_path,
                    message_nexus_binary: "/fixture/message-nexus".into(),
                    lease: 60,
                },
            ))
            .unwrap();
        drop(config);
        drop(FlowStore::open_configured(&path).unwrap());
        path
    }

    fn request(path: &Path) -> StoreCheckRequest {
        StoreCheckRequest {
            store_path: path.to_string_lossy().into_owned(),
        }
    }

    #[test]
    fn a_store_this_flow_opens_is_checked_open_and_left_untouched() {
        let directory = tempfile::tempdir().unwrap();
        let path = configured_store(directory.path());
        let before = fs::read(&path).unwrap();
        assert_eq!(
            request(&path).check(),
            StoreCheck::Opens(path.to_string_lossy().into_owned())
        );
        assert_eq!(fs::read(&path).unwrap(), before);
    }

    #[test]
    fn no_store_is_absent() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("flow.sema");
        assert_eq!(
            request(&path).check(),
            StoreCheck::Absent(path.to_string_lossy().into_owned())
        );
    }

    /// A store holding a family identity this Flow does not author, as a
    /// later Flow leaves one, is refused with the reason the Nexus would
    /// give, and left untouched.
    #[test]
    fn a_store_a_later_flow_wrote_is_refused() {
        use sema_engine::{
            Engine, EngineOpen, EngineRecord, FamilyName, RecordKey, SchemaHash, SchemaVersion,
            TableDescriptor, TableName,
        };
        #[derive(
            rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Debug, Clone, PartialEq, Eq,
        )]
        struct LaterRow {
            key: String,
        }
        impl EngineRecord for LaterRow {
            fn record_key(&self) -> RecordKey {
                RecordKey::new(self.key.clone())
            }
        }
        let directory = tempfile::tempdir().unwrap();
        let path = configured_store(directory.path());
        {
            let mut engine = Engine::open(EngineOpen::new(&path, SchemaVersion::new(2))).unwrap();
            engine
                .register_table(TableDescriptor::<LaterRow>::new(
                    TableName::new("flow_nexus_from_a_later_flow"),
                    FamilyName::new("flow-nexus-from-a-later-flow"),
                    SchemaHash::for_label("flow-nexus-from-a-later-flow-v1"),
                ))
                .unwrap();
        }
        let before = fs::read(&path).unwrap();
        let StoreCheck::Refused(refusal) = request(&path).check() else {
            panic!("a later store is refused")
        };
        assert_eq!(refusal.path, path.to_string_lossy());
        assert!(
            refusal.reason.contains("flow_nexus_from_a_later_flow"),
            "{}",
            refusal.reason
        );
        assert_eq!(fs::read(&path).unwrap(), before);
    }
}
