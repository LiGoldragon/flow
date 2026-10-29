//! The meta Nexus family: the one declared `ConfigureNexus`, the sockets and
//! endpoints a Nexus serves and reaches. A declaration archived before it
//! named the Curriculum socket opens naming the socket Curriculum serves by
//! default, `$XDG_RUNTIME_DIR/curriculum/curriculum.sock` of the process
//! that opens the store.

use super::StoreError;
use flow_defaults::{DefaultConfiguration, LaysOutDefaults, ReadsAnchors};
use meta_signal_flow::ConfigureNexus;
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use sema_engine::{
    Decision, Engine, EngineRecord, FamilyName, MemoryDefinition, MemoryUpgrades, MemoryVersion,
    RecordKey, SchemaHash, TableDescriptor, TableReference, UpgradeFrom, UpgradeOutcome,
};

pub(super) const META_NEXUS_CONFIGURATION_FAMILY: &str = "flow-nexus-meta-nexus-configuration";
pub(super) const META_NEXUS_CONFIGURATION_SCHEMA: &str = "flow-nexus-meta-nexus-configuration-v3";

/// The declared Nexus, one row.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct StoredMetaNexusConfiguration {
    pub(crate) nexus: ConfigureNexus,
}

impl EngineRecord for StoredMetaNexusConfiguration {
    fn record_key(&self) -> RecordKey {
        RecordKey::new("nexus")
    }
}

impl MemoryDefinition for StoredMetaNexusConfiguration {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(3)
    }

    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label(META_NEXUS_CONFIGURATION_SCHEMA)
    }
}

/// The Nexus declaration before harness installations owned endpoints and pane profiles.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(super) struct ConfigureNexusV2 {
    pub(super) source_root: String,
    pub(super) stable_codex: meta_signal_flow::CodexEndpoint,
    pub(super) next_codex: meta_signal_flow::CodexEndpoint,
    pub(super) harness_profiles: Vec<meta_signal_flow::HarnessProfile>,
    pub(super) meta_aspects: Vec<signal_flow::Aspect>,
    pub(super) message_nexus_path: String,
    pub(super) message_nexus_binary: String,
    pub(super) curriculum_nexus_path: String,
    pub(super) lease: i64,
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(super) struct StoredMetaNexusConfigurationV2 {
    pub(super) nexus: ConfigureNexusV2,
}
impl EngineRecord for StoredMetaNexusConfigurationV2 {
    fn record_key(&self) -> RecordKey {
        RecordKey::new("nexus")
    }
}
impl MemoryDefinition for StoredMetaNexusConfigurationV2 {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(2)
    }
    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label("flow-nexus-meta-nexus-configuration-v2")
    }
}

/// The declared Nexus as archived before it named the Curriculum socket.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(super) struct ConfigureNexusV1 {
    pub(super) source_root: String,
    pub(super) stable_codex: meta_signal_flow::CodexEndpoint,
    pub(super) next_codex: meta_signal_flow::CodexEndpoint,
    pub(super) harness_profiles: Vec<meta_signal_flow::HarnessProfile>,
    pub(super) meta_aspects: Vec<signal_flow::Aspect>,
    pub(super) message_nexus_path: String,
    pub(super) message_nexus_binary: String,
    pub(super) lease: i64,
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(super) struct StoredMetaNexusConfigurationV1 {
    pub(super) nexus: ConfigureNexusV1,
}

impl EngineRecord for StoredMetaNexusConfigurationV1 {
    fn record_key(&self) -> RecordKey {
        RecordKey::new("nexus")
    }
}

impl MemoryDefinition for StoredMetaNexusConfigurationV1 {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(1)
    }

    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label("flow-nexus-meta-nexus-configuration-v1")
    }
}

/// Where a Nexus whose declaration named no Curriculum socket reached it:
/// the socket Curriculum serves under the opening process's runtime
/// directory.
pub(crate) trait DefaultsCurriculumSocket {
    fn curriculum_socket(&self) -> String;
}

impl DefaultsCurriculumSocket for DefaultConfiguration {
    fn curriculum_socket(&self) -> String {
        self.curriculum_socket_path().to_string_lossy().into_owned()
    }
}

impl UpgradeFrom<StoredMetaNexusConfigurationV2> for StoredMetaNexusConfiguration {
    type Refusal = std::convert::Infallible;
    fn upgrade_from(previous: StoredMetaNexusConfigurationV2) -> Decision<Self, Self::Refusal> {
        Decision::Ready(StoredMetaNexusConfiguration::current(previous.nexus))
    }
}

impl UpgradeFrom<StoredMetaNexusConfigurationV1> for StoredMetaNexusConfiguration {
    type Refusal = std::convert::Infallible;
    fn upgrade_from(previous: StoredMetaNexusConfigurationV1) -> Decision<Self, Self::Refusal> {
        Decision::Ready(StoredMetaNexusConfiguration::current(
            previous
                .nexus
                .with_curriculum(DefaultConfiguration::from_environment().curriculum_socket()),
        ))
    }
}

pub(super) trait MigratesNexusConfiguration {
    fn resolve_program(name: &str) -> String;
    fn current(previous: ConfigureNexusV2) -> StoredMetaNexusConfiguration;
    fn legacy_nexus(path: &std::path::Path) -> Result<Option<ConfigureNexusV2>, StoreError>;
}

impl MigratesNexusConfiguration for StoredMetaNexusConfiguration {
    fn resolve_program(name: &str) -> String {
        std::env::var_os("PATH")
            .into_iter()
            .flat_map(|paths| std::env::split_paths(&paths).collect::<Vec<_>>())
            .map(|directory| directory.join(name))
            .find(|path| path.is_file())
            .unwrap_or_else(|| std::path::PathBuf::from(name))
            .to_string_lossy()
            .into_owned()
    }

    fn current(previous: ConfigureNexusV2) -> StoredMetaNexusConfiguration {
        let hook = std::env::current_exe()
            .ok()
            .and_then(|path| path.parent().map(|p| p.join("flow-hook")))
            .filter(|path| path.is_file())
            .map(|path| path.to_string_lossy().into_owned())
            .unwrap_or_else(|| Self::resolve_program("flow-hook"));
        StoredMetaNexusConfiguration {
            nexus: ConfigureNexus {
                flows_root: std::path::Path::new(&previous.source_root)
                    .join("flows")
                    .to_string_lossy()
                    .into_owned(),
                source_root: previous.source_root,
                meta_aspects: previous.meta_aspects,
                message_nexus_path: previous.message_nexus_path,
                message_nexus_binary: previous.message_nexus_binary,
                curriculum_nexus_path: previous.curriculum_nexus_path,
                lease: previous.lease,
                executables: meta_signal_flow::Executables {
                    herdr: Self::resolve_program("herdr"),
                    flow_id_claimer: Self::resolve_program("flow-id"),
                    hook,
                },
                default_herdr_session: "default".into(),
            },
        }
    }

    /// Reads the v1/v2 Nexus row before Engine upgrades its table, so the fields
    /// that move to the Harness family can be written there without loss.
    fn legacy_nexus(path: &std::path::Path) -> Result<Option<ConfigureNexusV2>, StoreError> {
        use redb::{ReadableDatabase as _, ReadableTable as _};
        if !path.exists() {
            return Ok(None);
        }
        // This runs only inside the mutating open (on its staged copy when an
        // upgrade is needed). A recovery-required redb must be opened writable
        // so redb can repair it before we read the legacy row; recognition itself
        // remains read-only and never calls this function.
        let database = redb::Database::open(path)
            .map_err(|error| StoreError::NexusUpgrade(error.to_string()))?;
        let read = database
            .begin_read()
            .map_err(|error| StoreError::NexusUpgrade(error.to_string()))?;
        let table = match read.open_table(redb::TableDefinition::<String, &[u8]>::new(
            super::META_NEXUS_CONFIGURATION_TABLE_NAME.as_str(),
        )) {
            Ok(table) => table,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
            Err(error) => return Err(StoreError::NexusUpgrade(error.to_string())),
        };
        let Some(bytes) = table
            .get("nexus".to_owned())
            .map_err(|error| StoreError::NexusUpgrade(error.to_string()))?
        else {
            return Ok(None);
        };
        if let Ok(row) =
            rkyv::from_bytes::<StoredMetaNexusConfigurationV2, rkyv::rancor::Error>(bytes.value())
        {
            return Ok(Some(row.nexus));
        }
        if let Ok(row) =
            rkyv::from_bytes::<StoredMetaNexusConfigurationV1, rkyv::rancor::Error>(bytes.value())
        {
            return Ok(Some(row.nexus.with_curriculum(
                DefaultConfiguration::from_environment().curriculum_socket(),
            )));
        }
        Ok(None)
    }
}

/// The archived declaration carried whole, naming the Curriculum socket.
pub(super) trait NamesCurriculumSocket {
    fn with_curriculum(self, curriculum_nexus_path: String) -> ConfigureNexusV2;
}

impl NamesCurriculumSocket for ConfigureNexusV1 {
    fn with_curriculum(self, curriculum_nexus_path: String) -> ConfigureNexusV2 {
        ConfigureNexusV2 {
            source_root: self.source_root,
            stable_codex: self.stable_codex,
            next_codex: self.next_codex,
            harness_profiles: self.harness_profiles,
            meta_aspects: self.meta_aspects,
            message_nexus_path: self.message_nexus_path,
            message_nexus_binary: self.message_nexus_binary,
            curriculum_nexus_path,
            lease: self.lease,
        }
    }
}

/// The Nexus family's descriptor, from v1 on.
pub(crate) trait DescribesNexusConfigurationTable {
    fn descriptor() -> TableDescriptor<StoredMetaNexusConfiguration>;
}

impl DescribesNexusConfigurationTable for StoredMetaNexusConfiguration {
    fn descriptor() -> TableDescriptor<StoredMetaNexusConfiguration> {
        TableDescriptor::new(
            super::META_NEXUS_CONFIGURATION_TABLE_NAME,
            FamilyName::new(META_NEXUS_CONFIGURATION_FAMILY),
            StoredMetaNexusConfiguration::schema_hash(),
        )
        .with_predecessor::<StoredMetaNexusConfigurationV1>()
        .with_predecessor::<StoredMetaNexusConfigurationV2>()
    }
}

pub(crate) trait UpgradesNexusConfiguration {
    fn upgraded_nexus_configuration(
        &mut self,
        descriptor: TableDescriptor<StoredMetaNexusConfiguration>,
    ) -> Result<TableReference<StoredMetaNexusConfiguration>, StoreError>;
}

impl UpgradesNexusConfiguration for Engine {
    fn upgraded_nexus_configuration(
        &mut self,
        descriptor: TableDescriptor<StoredMetaNexusConfiguration>,
    ) -> Result<TableReference<StoredMetaNexusConfiguration>, StoreError> {
        let v1 = TableDescriptor::<StoredMetaNexusConfigurationV1>::new(
            super::META_NEXUS_CONFIGURATION_TABLE_NAME,
            FamilyName::new(META_NEXUS_CONFIGURATION_FAMILY),
            StoredMetaNexusConfigurationV1::schema_hash(),
        )
        .family_identity();
        let family = self
            .catalog()
            .family_identity(&super::META_NEXUS_CONFIGURATION_TABLE_NAME)
            .cloned();
        let outcome = if family.as_ref() == Some(&v1) {
            self.upgrade_table::<StoredMetaNexusConfigurationV1, StoredMetaNexusConfiguration>(
                descriptor,
            )
        } else {
            self.upgrade_table::<StoredMetaNexusConfigurationV2, StoredMetaNexusConfiguration>(
                descriptor,
            )
        };
        match outcome {
            UpgradeOutcome::Upgraded(receipt) => Ok(receipt.table()),
            UpgradeOutcome::AlreadyCurrent(table) => Ok(table),
            UpgradeOutcome::Refused(refusal) => match *refusal.reason() {},
            UpgradeOutcome::Failed(failure) => {
                Err(StoreError::NexusUpgrade(format!("{failure:?}")))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ConfigureNexusV1, NamesCurriculumSocket, StoredMetaNexusConfiguration,
        StoredMetaNexusConfigurationV1,
    };
    use sema_engine::{Decision, UpgradeFrom};

    fn archived() -> ConfigureNexusV1 {
        ConfigureNexusV1 {
            source_root: "/srv/source".into(),
            stable_codex: meta_signal_flow::CodexEndpoint {
                client_path: "stable-client".into(),
                home: "/home/someone/.codex".into(),
                control_socket_path: "/home/someone/.codex/control.sock".into(),
                model_name_vector: vec!["gpt-5.6-sol".into()],
            },
            next_codex: meta_signal_flow::CodexEndpoint {
                client_path: "next-client".into(),
                home: "/home/someone/.codex-next".into(),
                control_socket_path: "/home/someone/.codex-next/control.sock".into(),
                model_name_vector: vec![],
            },
            harness_profiles: vec![],
            meta_aspects: vec![
                signal_flow::Aspect::Psyche,
                signal_flow::Aspect::Mind,
                signal_flow::Aspect::Field,
            ],
            message_nexus_path: "/srv/message.sock".into(),
            message_nexus_binary: "/usr/bin/message-nexus".into(),
            lease: 60,
        }
    }

    /// A declaration archived before the Curriculum socket keeps every value
    /// it held and names the socket it is given.
    #[test]
    fn an_archived_nexus_keeps_its_values_and_names_the_curriculum_socket() {
        let upgraded =
            archived().with_curriculum("/run/user/4242/curriculum/curriculum.sock".into());
        assert_eq!(upgraded.source_root, "/srv/source");
        assert_eq!(upgraded.message_nexus_path, "/srv/message.sock");
        assert_eq!(upgraded.message_nexus_binary, "/usr/bin/message-nexus");
        assert_eq!(upgraded.lease, 60);
        assert_eq!(upgraded.stable_codex.model_name_vector, ["gpt-5.6-sol"]);
        assert_eq!(
            upgraded.curriculum_nexus_path,
            "/run/user/4242/curriculum/curriculum.sock"
        );
    }

    /// The upgrade names the socket Curriculum serves under the opening
    /// process's runtime directory.
    #[test]
    fn the_upgrade_names_curriculum_under_the_runtime_directory() {
        let Decision::Ready(upgraded) =
            StoredMetaNexusConfiguration::upgrade_from(StoredMetaNexusConfigurationV1 {
                nexus: archived(),
            });
        assert!(
            upgraded
                .nexus
                .curriculum_nexus_path
                .ends_with("/curriculum/curriculum.sock"),
            "{}",
            upgraded.nexus.curriculum_nexus_path
        );
    }
}
