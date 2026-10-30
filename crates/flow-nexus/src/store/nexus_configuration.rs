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
const META_NEXUS_CONFIGURATION_V2_SCHEMA: &str = "flow-nexus-meta-nexus-configuration-v2";

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

/// The 0.42 Nexus declaration, before the abandoned Message executable
/// identity became optional.
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
        SchemaHash::for_label(META_NEXUS_CONFIGURATION_V2_SCHEMA)
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
        let previous = previous.nexus;
        Decision::Ready(Self {
            nexus: ConfigureNexus {
                source_root: previous.source_root,
                stable_codex: previous.stable_codex,
                next_codex: previous.next_codex,
                harness_profiles: previous.harness_profiles,
                meta_aspects: previous.meta_aspects,
                message_nexus_path_option: (!previous.message_nexus_path.is_empty())
                    .then_some(previous.message_nexus_path),
                message_nexus_binary_option: (!previous.message_nexus_binary.is_empty())
                    .then_some(previous.message_nexus_binary),
                curriculum_nexus_path: previous.curriculum_nexus_path,
                lease: previous.lease,
            },
        })
    }
}

impl UpgradeFrom<StoredMetaNexusConfigurationV1> for StoredMetaNexusConfigurationV2 {
    type Refusal = std::convert::Infallible;
    fn upgrade_from(previous: StoredMetaNexusConfigurationV1) -> Decision<Self, Self::Refusal> {
        Decision::Ready(
            previous
                .nexus
                .with_curriculum(DefaultConfiguration::from_environment().curriculum_socket()),
        )
    }
}

impl UpgradeFrom<StoredMetaNexusConfigurationV1> for StoredMetaNexusConfiguration {
    type Refusal = std::convert::Infallible;
    fn upgrade_from(previous: StoredMetaNexusConfigurationV1) -> Decision<Self, Self::Refusal> {
        let v2 = previous
            .nexus
            .with_curriculum(DefaultConfiguration::from_environment().curriculum_socket());
        match StoredMetaNexusConfiguration::upgrade_from(v2) {
            Decision::Ready(current) => Decision::Ready(current),
            Decision::Refused(refusal) => match refusal {},
        }
    }
}

/// The archived declaration carried whole, naming the Curriculum socket.
pub(super) trait NamesCurriculumSocket {
    fn with_curriculum(self, curriculum_nexus_path: String) -> StoredMetaNexusConfigurationV2;
}

impl NamesCurriculumSocket for ConfigureNexusV1 {
    fn with_curriculum(self, curriculum_nexus_path: String) -> StoredMetaNexusConfigurationV2 {
        StoredMetaNexusConfigurationV2 {
            nexus: ConfigureNexusV2 {
                source_root: self.source_root,
                stable_codex: self.stable_codex,
                next_codex: self.next_codex,
                harness_profiles: self.harness_profiles,
                meta_aspects: self.meta_aspects,
                message_nexus_path: self.message_nexus_path,
                message_nexus_binary: self.message_nexus_binary,
                curriculum_nexus_path,
                lease: self.lease,
            },
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
        let from_v1 = descriptor.clone();
        match self.upgrade_table::<StoredMetaNexusConfigurationV2, StoredMetaNexusConfiguration>(
            descriptor,
        ) {
            UpgradeOutcome::Upgraded(receipt) => Ok(receipt.table()),
            UpgradeOutcome::AlreadyCurrent(table) => Ok(table),
            UpgradeOutcome::Refused(refusal) => match *refusal.reason() {},
            UpgradeOutcome::Failed(failure)
                if failure.phase() == sema_engine::StoragePhase::Admission =>
            {
                match self
                    .upgrade_table::<StoredMetaNexusConfigurationV1, StoredMetaNexusConfiguration>(
                        from_v1.with_predecessor::<StoredMetaNexusConfigurationV1>(),
                    ) {
                    UpgradeOutcome::Upgraded(receipt) => Ok(receipt.table()),
                    UpgradeOutcome::AlreadyCurrent(table) => Ok(table),
                    UpgradeOutcome::Refused(refusal) => match *refusal.reason() {},
                    UpgradeOutcome::Failed(failure) => {
                        Err(StoreError::NexusUpgrade(format!("{failure:?}")))
                    }
                }
            }
            UpgradeOutcome::Failed(failure) => {
                Err(StoreError::NexusUpgrade(format!("{failure:?}")))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ConfigureNexusV1, DefaultsCurriculumSocket, NamesCurriculumSocket,
        StoredMetaNexusConfiguration, StoredMetaNexusConfigurationV2,
    };
    use flow_defaults::{DefaultConfiguration, ReadsAnchors};
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
            meta_aspects: vec![signal_flow::Aspect::Psyche],
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
        assert_eq!(upgraded.nexus.source_root, "/srv/source");
        assert_eq!(upgraded.nexus.message_nexus_path, "/srv/message.sock");
        assert_eq!(
            upgraded.nexus.message_nexus_binary,
            "/usr/bin/message-nexus"
        );
        assert_eq!(upgraded.nexus.lease, 60);
        assert_eq!(
            upgraded.nexus.stable_codex.model_name_vector,
            ["gpt-5.6-sol"]
        );
        assert_eq!(
            upgraded.nexus.curriculum_nexus_path,
            "/run/user/4242/curriculum/curriculum.sock"
        );
    }

    #[test]
    fn v2_message_endpoint_and_binary_upgrade_as_options() {
        let v2 = archived().with_curriculum("/run/curriculum.sock".into());
        let Decision::Ready(upgraded) = StoredMetaNexusConfiguration::upgrade_from(v2);
        assert_eq!(
            upgraded.nexus.message_nexus_path_option.as_deref(),
            Some("/srv/message.sock")
        );
        assert_eq!(
            upgraded.nexus.message_nexus_binary_option.as_deref(),
            Some("/usr/bin/message-nexus")
        );
    }

    #[test]
    fn v2_empty_message_endpoint_and_binary_become_absent() {
        let mut v2: StoredMetaNexusConfigurationV2 =
            archived().with_curriculum("/run/curriculum.sock".into());
        v2.nexus.message_nexus_path.clear();
        v2.nexus.message_nexus_binary.clear();
        let Decision::Ready(upgraded) = StoredMetaNexusConfiguration::upgrade_from(v2);
        assert!(upgraded.nexus.message_nexus_path_option.is_none());
        assert!(upgraded.nexus.message_nexus_binary_option.is_none());
    }

    /// The upgrade names the socket Curriculum serves under the opening
    /// process's runtime directory.
    #[test]
    fn the_upgrade_names_curriculum_under_the_runtime_directory() {
        let v2 = archived()
            .with_curriculum(DefaultConfiguration::from_environment().curriculum_socket());
        let Decision::Ready(upgraded) = StoredMetaNexusConfiguration::upgrade_from(v2);
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
