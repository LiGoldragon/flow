//! An open that changes a store's families lands whole or not at all.
//!
//! Sema Engine lands each family upgrade, each re-keying and each new
//! family registration in a write transaction of its own, and offers no
//! transaction that spans families; one open of an older store is therefore
//! several commits. Such an open runs first on a copy beside the store, and
//! only when that open has succeeded does the copy replace the store, by one
//! rename. A refusal, an error or a crash before the rename leaves the store
//! byte for byte as it was; after it, the store is wholly upgraded.

use std::fs::File;
use std::path::{Path, PathBuf};

use redb::{ReadableDatabase as _, ReadableTable as _, TableHandle as _};
use sema_engine::TableRegistration;

use super::{
    ADDED_LATER_TABLES, CONFIGURATION_ONLY_ADDED_TABLES, CONFIGURATION_ONLY_TABLES, FlowStore,
    RETIRED_TABLES, RUNNING_TABLES, ReadsFlowMemory, SEMA_CATALOG_TABLE, StoreError,
};

/// The tables one entry point registers, and so may add or upgrade.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum UpgradeScope {
    /// The configuration-only open: the configuration tables, the
    /// leave-out defaults and the send servers.
    ConfigurationOnly,
    /// The Nexus's open: every table a running store holds.
    Running,
}

trait NamesScopeTables {
    fn tables(&self) -> Vec<&'static str>;
}

impl NamesScopeTables for UpgradeScope {
    fn tables(&self) -> Vec<&'static str> {
        match self {
            UpgradeScope::ConfigurationOnly => CONFIGURATION_ONLY_TABLES
                .iter()
                .chain(CONFIGURATION_ONLY_ADDED_TABLES)
                .copied()
                .collect(),
            UpgradeScope::Running => RUNNING_TABLES
                .iter()
                .chain(ADDED_LATER_TABLES)
                .copied()
                .collect(),
        }
    }
}

/// A copy of the store an open upgrades in place of the store itself.
///
/// While it stands it holds the store's whole-file lock, the lock every
/// redb open of the store also takes, so no other open reads or writes the
/// store under it. Dropped without landing, it removes its copy.
pub(crate) struct StagedUpgrade {
    store: PathBuf,
    copy: PathBuf,
    exclusion: Option<File>,
    keeps_copy: bool,
}

pub(crate) trait StagesUpgrade: Sized {
    /// Runs `open` on a staged copy when opening `store` in `scope` would
    /// change it, and lands the copy over the store once `open` has
    /// returned. `None` when the store is absent or already current; the
    /// store is then opened in place alone.
    fn run<T>(
        store: &Path,
        scope: UpgradeScope,
        open: impl FnOnce(&Path) -> Result<T, StoreError>,
    ) -> Result<Option<T>, StoreError>;

    /// Stages a copy when opening `store` in `scope` would change it.
    fn begin(store: &Path, scope: UpgradeScope) -> Result<Option<Self>, StoreError>;

    fn copy(&self) -> &Path;

    /// Removes the copies a crash left beside `store`. The caller holds the
    /// store's whole-file lock, so no live staging open owns one.
    fn sweep(
        store: &Path,
        failed: &dyn Fn(&str, &dyn std::fmt::Display) -> StoreError,
    ) -> Result<(), StoreError>;

    fn copy_prefix(store: &Path) -> Result<String, StoreError>;

    /// Replaces the store with the upgraded copy in one rename.
    fn land(self) -> Result<(), StoreError>;
}

impl StagesUpgrade for StagedUpgrade {
    fn run<T>(
        store: &Path,
        scope: UpgradeScope,
        open: impl FnOnce(&Path) -> Result<T, StoreError>,
    ) -> Result<Option<T>, StoreError> {
        let Some(staged) = Self::begin(store, scope)? else {
            return Ok(None);
        };
        // The copy's Engine is dropped when `open` returns, before landing.
        let opened = open(staged.copy())?;
        staged.land()?;
        Ok(Some(opened))
    }

    fn begin(store: &Path, scope: UpgradeScope) -> Result<Option<Self>, StoreError> {
        let failed = |action: &str, error: &dyn std::fmt::Display| {
            StoreError::StagedUpgrade(format!("{action} {}: {error}", store.display()))
        };
        if !store.exists() {
            return Ok(None);
        }
        let held = || StoreError::StoreHeld {
            path: store.display().to_string(),
        };
        // Every open clears the copies a crash left, whether or not it
        // upgrades; the lock is released again before the store is read,
        // since a read opens the store under the same whole-file lock.
        {
            let exclusion = File::open(store).map_err(|error| failed("cannot open", &error))?;
            match exclusion.try_lock() {
                Ok(()) => {}
                Err(std::fs::TryLockError::WouldBlock) => return Err(held()),
                Err(error) => return Err(failed("cannot lock", &error)),
            }
            Self::sweep(store, &failed)?;
            // Released by name, not by closing: a descriptor a concurrent
            // fork still holds would keep the lock until its exec.
            exclusion
                .unlock()
                .map_err(|error| failed("cannot unlock", &error))?;
        }
        match redb::ReadOnlyDatabase::open(store) {
            Ok(reader) => {
                if !FlowStore::upgrade_pending(&reader, scope)? {
                    return Ok(None);
                }
            }
            // A store left uncleanly cannot be read without repair; its copy
            // is repaired by the open that upgrades it.
            Err(redb::DatabaseError::RepairAborted) => {}
            Err(redb::DatabaseError::DatabaseAlreadyOpen) => return Err(held()),
            Err(error) => return Err(failed("cannot read", &error)),
        }
        let exclusion = File::open(store).map_err(|error| failed("cannot open", &error))?;
        match exclusion.try_lock() {
            Ok(()) => {}
            Err(std::fs::TryLockError::WouldBlock) => return Err(held()),
            Err(error) => return Err(failed("cannot lock", &error)),
        }
        Self::sweep(store, &failed)?;
        let prefix = Self::copy_prefix(store)?;
        let directory = store.parent().ok_or(StoreError::InvalidStorePath)?;
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|elapsed| elapsed.as_nanos())
            .unwrap_or_default();
        let copy = directory.join(format!("{prefix}{}-{nanos}", std::process::id()));
        let staged = Self {
            store: store.to_path_buf(),
            copy,
            exclusion: Some(exclusion),
            keeps_copy: false,
        };
        std::fs::copy(store, &staged.copy).map_err(|error| failed("cannot copy", &error))?;
        Ok(Some(staged))
    }

    fn copy(&self) -> &Path {
        &self.copy
    }

    fn sweep(
        store: &Path,
        failed: &dyn Fn(&str, &dyn std::fmt::Display) -> StoreError,
    ) -> Result<(), StoreError> {
        let prefix = Self::copy_prefix(store)?;
        let directory = store.parent().ok_or(StoreError::InvalidStorePath)?;
        for entry in std::fs::read_dir(directory).map_err(|error| failed("cannot list", &error))? {
            let entry = entry.map_err(|error| failed("cannot list", &error))?;
            if entry.file_name().to_string_lossy().starts_with(&prefix) {
                std::fs::remove_file(entry.path())
                    .map_err(|error| failed("cannot remove a stale copy of", &error))?;
            }
        }
        Ok(())
    }

    fn copy_prefix(store: &Path) -> Result<String, StoreError> {
        let name = store.file_name().ok_or(StoreError::InvalidStorePath)?;
        Ok(format!(".{}.upgrade-", name.to_string_lossy()))
    }

    fn land(mut self) -> Result<(), StoreError> {
        let failed = |action: &str, error: &dyn std::fmt::Display| {
            StoreError::StagedUpgrade(format!("{action} {}: {error}", self.store.display()))
        };
        File::open(&self.copy)
            .and_then(|copy| copy.sync_all())
            .map_err(|error| failed("cannot sync the upgraded copy of", &error))?;
        std::fs::rename(&self.copy, &self.store)
            .map_err(|error| failed("cannot replace", &error))?;
        self.keeps_copy = true;
        if let Some(directory) = self.store.parent() {
            File::open(directory)
                .and_then(|directory| directory.sync_all())
                .map_err(|error| failed("cannot sync the directory of", &error))?;
        }
        Ok(())
    }
}

#[cfg(test)]
pub(crate) trait InterruptsStagedUpgrade {
    /// Ends the staging as a crash would: the locks fall with the process
    /// and the copy stays, never landed.
    fn crash(self);
}

#[cfg(test)]
impl InterruptsStagedUpgrade for StagedUpgrade {
    fn crash(mut self) {
        self.keeps_copy = true;
    }
}

impl Drop for StagedUpgrade {
    fn drop(&mut self) {
        if !self.keeps_copy {
            let _ = std::fs::remove_file(&self.copy);
        }
        if let Some(exclusion) = self.exclusion.take() {
            let _ = exclusion.unlock();
        }
    }
}

trait ReadsPendingUpgrade {
    fn upgrade_pending(
        reader: &redb::ReadOnlyDatabase,
        scope: UpgradeScope,
    ) -> Result<bool, StoreError>;
}

impl ReadsPendingUpgrade for FlowStore {
    /// Whether opening in `scope` would write the catalog: a retired table
    /// to drop, or a table of the scope absent or registered under an
    /// identity other than its current one.
    fn upgrade_pending(
        reader: &redb::ReadOnlyDatabase,
        scope: UpgradeScope,
    ) -> Result<bool, StoreError> {
        let failed = |error: &dyn std::fmt::Display| {
            StoreError::StagedUpgrade(format!("cannot read the family catalog: {error}"))
        };
        let read = reader.begin_read().map_err(|error| failed(&error))?;
        let retired = |name: &str| RETIRED_TABLES.iter().any(|(table, _, _)| *table == name);
        if read
            .list_tables()
            .map_err(|error| failed(&error))?
            .any(|table| retired(table.name()))
        {
            return Ok(true);
        }
        let catalog = read
            .open_table(SEMA_CATALOG_TABLE)
            .map_err(|error| failed(&error))?;
        let mut current = std::collections::BTreeSet::new();
        for entry in catalog.iter().map_err(|error| failed(&error))? {
            let (table_key, bytes) = entry.map_err(|error| failed(&error))?;
            let table_key = table_key.value();
            if retired(table_key) {
                return Ok(true);
            }
            let registration =
                rkyv::from_bytes::<TableRegistration, rkyv::rancor::Error>(bytes.value())
                    .map_err(|error| failed(&error))?;
            let identity = registration.identity();
            let is_current = FlowStore::current_flow_family_identity(table_key).is_some_and(
                |(family, schema_hash)| {
                    identity.table_name() == table_key
                        && identity.family().as_str() == family
                        && identity.schema_hash() == schema_hash
                },
            );
            if is_current {
                current.insert(table_key.to_owned());
            }
        }
        Ok(scope.tables().iter().any(|table| !current.contains(*table)))
    }
}
