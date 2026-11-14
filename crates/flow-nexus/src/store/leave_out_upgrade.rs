//! The permaflow declarations as 0.54.7 wrote them (v6), upgraded to v7 in
//! one atomic evolution, each taking its leave-outs from the leave-out
//! family 0.55.0 retires.
//!
//! Until 0.55.0 a permaflow's leave-outs were a family of their own, one
//! entry per Address and harness. In 0.55.0 a permaflow is one definition
//! and carries its leave-outs (`ConfigureMetaflow.leave_outs`). Each v6
//! declaration takes the entry of its Address for the harness its Model
//! runs on, derived as a launch derives it: the native model the Models
//! family names for that Model, on Codex when one of the declared Nexus's
//! Codex endpoints lists it, on Claude otherwise. An entry with no
//! permaflow at its Address, an entry for the other harness, and every
//! entry of a permaflow whose harness cannot be derived (no native model,
//! or no Nexus) are dropped, each named in the daemon's log and in the
//! fold this step answers. The same evolution retracts every row of the
//! retired family, which stays in the catalog, empty and read by nothing;
//! a new store never has it.
//!
//! An older daemon declares v6 alone and is refused at open by the v7
//! identity; rollback is the restore of the store copy taken before the
//! deploy.

use super::metaflow_declaration::{
    DescribesMetaflowDeclarationTable, META_METAFLOW_CONFIGURATION_FAMILY,
    META_METAFLOW_CONFIGURATION_TABLE_NAME, StoredMetaMetaflowConfiguration,
};
use super::nexus_configuration::{DescribesNexusConfigurationTable, StoredMetaNexusConfiguration};
use super::{
    DescribesModelTable, FlowStore, META_LEAVE_OUT_CONFIGURATION_TABLE_NAME, ReadsFlowMemory,
    StoreError, StoredMetaModelConfiguration,
};
use meta_signal_flow::{ConfigureMetaflow, Continuity, Purpose, Records, Root};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use sema_engine::{
    Decision, Engine, EngineRecord, FamilyName, MemoryDefinition, MemoryVersion, QueryPlan,
    RecordKey, SchemaHash, TableDescriptor, UpgradeFrom,
};
use signal_flow::{Address, FlowKind, HarnessKind, LeaveOuts, SystemModules, ToolSet, UserModules};

/// The permaflow family's schema label in 0.54.7.
pub(crate) const META_METAFLOW_CONFIGURATION_SCHEMA_V6: &str =
    "flow-nexus-meta-metaflow-configuration-v6";
/// The retired leave-out family's schema label, from 0.52.0 to 0.54.7.
pub(crate) const META_LEAVE_OUT_CONFIGURATION_SCHEMA_V3: &str =
    "flow-nexus-meta-leave-out-configuration-v3";
pub(crate) const META_LEAVE_OUT_CONFIGURATION_FAMILY: &str =
    "flow-nexus-meta-leave-out-configuration";

/// A permaflow declaration as 0.54.7 stored it: no leave-outs.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct ConfigureMetaflowV6 {
    pub(crate) address: Address,
    pub(crate) root_option: Option<Root>,
    pub(crate) system_modules: SystemModules,
    pub(crate) user_modules: UserModules,
    pub(crate) flow_kind: FlowKind,
    pub(crate) tool_set: ToolSet,
    pub(crate) continuity: Continuity,
    pub(crate) purpose: Purpose,
    pub(crate) records: Records,
}

/// One permaflow declaration row as 0.54.7 stored it.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct StoredMetaMetaflowConfigurationV6 {
    pub(crate) metaflow: ConfigureMetaflowV6,
}

impl EngineRecord for StoredMetaMetaflowConfigurationV6 {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(FlowStore::address_record_key(&self.metaflow.address))
    }
}

impl MemoryDefinition for StoredMetaMetaflowConfigurationV6 {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(6)
    }

    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label(META_METAFLOW_CONFIGURATION_SCHEMA_V6)
    }
}

/// One leave-out entry as the retired family stored it: the leave-outs of
/// one Address started on one harness.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct ConfigureLeaveOutV3 {
    pub(crate) address: Address,
    pub(crate) harness_kind: HarnessKind,
    pub(crate) leave_outs: LeaveOuts,
}

/// One row of the retired leave-out family.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct StoredMetaLeaveOutConfigurationV3 {
    pub(crate) leave_out: ConfigureLeaveOutV3,
}

impl EngineRecord for StoredMetaLeaveOutConfigurationV3 {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(FlowStore::leave_out_record_key(
            &self.leave_out.address,
            &self.leave_out.harness_kind,
        ))
    }
}

impl MemoryDefinition for StoredMetaLeaveOutConfigurationV3 {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(3)
    }

    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label(META_LEAVE_OUT_CONFIGURATION_SCHEMA_V3)
    }
}

/// The descriptors the step reads and writes through.
pub(crate) trait DescribesLeaveOutUpgrade {
    /// The v7 permaflow family, evolving from v6.
    fn evolving_metaflow_descriptor() -> TableDescriptor<StoredMetaMetaflowConfiguration>;
    /// The retired leave-out family, as it stands in a 0.54.7 store.
    fn retired_leave_out_descriptor() -> TableDescriptor<StoredMetaLeaveOutConfigurationV3>;
}

impl DescribesLeaveOutUpgrade for FlowStore {
    fn evolving_metaflow_descriptor() -> TableDescriptor<StoredMetaMetaflowConfiguration> {
        <StoredMetaMetaflowConfiguration as DescribesMetaflowDeclarationTable>::descriptor()
    }

    fn retired_leave_out_descriptor() -> TableDescriptor<StoredMetaLeaveOutConfigurationV3> {
        TableDescriptor::new(
            META_LEAVE_OUT_CONFIGURATION_TABLE_NAME,
            FamilyName::new(META_LEAVE_OUT_CONFIGURATION_FAMILY),
            StoredMetaLeaveOutConfigurationV3::schema_hash(),
        )
    }
}

/// Why a retired leave-out entry was dropped, not folded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum LeaveOutDropped {
    /// No permaflow is declared at its Address.
    NoPermaflow,
    /// Its permaflow's Model runs on the other harness.
    OtherHarness(HarnessKind),
    /// Its permaflow's harness cannot be derived: no native model is
    /// configured for its Model, or no Nexus is declared.
    HarnessUnknown,
}

/// What the step did: each permaflow that took leave-outs, with the harness
/// they are of, and each retired entry dropped, with why.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct LeaveOutFold {
    pub(crate) folded: Vec<(Address, HarnessKind)>,
    pub(crate) dropped: Vec<(Address, HarnessKind, LeaveOutDropped)>,
}

/// A v6 row upgraded alone, as the identity-only upgrade of a family with
/// no leave-out to fold reads one: it takes no leave-out.
impl UpgradeFrom<StoredMetaMetaflowConfigurationV6> for StoredMetaMetaflowConfiguration {
    type Refusal = std::convert::Infallible;

    fn upgrade_from(before: StoredMetaMetaflowConfigurationV6) -> Decision<Self, Self::Refusal> {
        Decision::Ready(Self {
            metaflow: before.metaflow.with_leave_outs(LeaveOuts(Vec::new())),
        })
    }
}

/// A v6 declaration carrying the leave-outs it takes.
pub(crate) trait TakesLeaveOuts {
    fn with_leave_outs(self, leave_outs: LeaveOuts) -> ConfigureMetaflow;
}

impl TakesLeaveOuts for ConfigureMetaflowV6 {
    fn with_leave_outs(self, leave_outs: LeaveOuts) -> ConfigureMetaflow {
        ConfigureMetaflow {
            address: self.address,
            root_option: self.root_option,
            leave_outs,
            system_modules: self.system_modules,
            user_modules: self.user_modules,
            flow_kind: self.flow_kind,
            tool_set: self.tool_set,
            continuity: self.continuity,
            purpose: self.purpose,
            records: self.records,
        }
    }
}

/// The startup step, run before the permaflow family is registered.
pub(crate) trait UpgradesLeaveOutMemory {
    /// Upgrades a v6 store in one evolution; a current or new store is
    /// left as it is and answers an empty fold.
    fn upgrade_leave_out_memory(engine: &mut Engine) -> Result<LeaveOutFold, StoreError>;
}

impl UpgradesLeaveOutMemory for FlowStore {
    fn upgrade_leave_out_memory(engine: &mut Engine) -> Result<LeaveOutFold, StoreError> {
        let stored = engine.list_tables().into_iter().find(|registration| {
            registration.identity().table_name() == META_METAFLOW_CONFIGURATION_TABLE_NAME.as_str()
        });
        // A store with no permaflow family yet, or one at v7, is current.
        let Some(stored) = stored else {
            return Ok(LeaveOutFold::default());
        };
        if stored.identity().schema_hash() != StoredMetaMetaflowConfigurationV6::schema_hash() {
            return Ok(LeaveOutFold::default());
        }
        let descriptor = Self::evolving_metaflow_descriptor();
        let inventory = engine.inspect_predecessor::<
            StoredMetaMetaflowConfigurationV6,
            StoredMetaMetaflowConfiguration,
        >(&descriptor)?;
        let retired_exists = engine.list_tables().iter().any(|registration| {
            registration.identity().table_name() == META_LEAVE_OUT_CONFIGURATION_TABLE_NAME.as_str()
        });
        let retired = match retired_exists {
            true => Some(engine.register_table(Self::retired_leave_out_descriptor())?),
            false => None,
        };
        let entries = match retired {
            Some(table) => engine
                .match_records(QueryPlan::all(table))?
                .records()
                .to_vec(),
            None => Vec::new(),
        };
        // Nothing to fold and nothing to retract: the identity alone moves.
        if entries.is_empty() && inventory.rows().is_empty() {
            use sema_engine::MemoryUpgrades as _;
            return match engine.upgrade_table::<
                StoredMetaMetaflowConfigurationV6,
                StoredMetaMetaflowConfiguration,
            >(descriptor)
            {
                sema_engine::UpgradeOutcome::Upgraded(_)
                | sema_engine::UpgradeOutcome::AlreadyCurrent(_) => Ok(LeaveOutFold::default()),
                sema_engine::UpgradeOutcome::Refused(refusal) => {
                    Err(StoreError::LeaveOutUpgrade(format!("{refusal:?}")))
                }
                sema_engine::UpgradeOutcome::Failed(failure) => {
                    Err(StoreError::LeaveOutUpgrade(format!("{failure:?}")))
                }
            };
        }
        // The harness each Model runs on, read from the Models and Nexus
        // families as they stand (both current already).
        let models = engine.register_table(StoredMetaModelConfiguration::descriptor())?;
        let nexus = engine.register_table(StoredMetaNexusConfiguration::descriptor())?;
        let models = engine
            .match_records(QueryPlan::all(models))?
            .records()
            .to_vec();
        let nexus = engine
            .match_records(QueryPlan::all(nexus))?
            .records()
            .first()
            .map(|stored| stored.nexus.clone());
        let harness_of = |address: &Address| -> Option<HarnessKind> {
            use crate::generated::starter::DerivesHarness as _;
            let nexus = nexus.as_ref()?;
            let native = models
                .iter()
                .find(|stored| stored.model.model == address.model)?
                .model
                .native
                .0
                .clone();
            let runtime = super::RuntimeConfiguration::from(nexus);
            Some(crate::codex::CodexEndpoints::from(&runtime).harness_for(native.as_str()))
        };
        let mut fold = LeaveOutFold::default();
        let mut rows = Vec::new();
        for (key, row) in inventory.rows() {
            if *key != row.record_key() {
                return Err(StoreError::LeaveOutUpgrade(format!(
                    "a permaflow row's key differs from its Address: {:?}",
                    row.metaflow.address
                )));
            }
            let harness = harness_of(&row.metaflow.address);
            let taken = entries.iter().find(|entry| {
                entry.leave_out.address == row.metaflow.address
                    && Some(&entry.leave_out.harness_kind) == harness.as_ref()
            });
            if let Some(entry) = taken {
                fold.folded.push((
                    entry.leave_out.address.clone(),
                    entry.leave_out.harness_kind.clone(),
                ));
            }
            rows.push(StoredMetaMetaflowConfiguration {
                metaflow: row.metaflow.clone().with_leave_outs(
                    taken
                        .map(|entry| entry.leave_out.leave_outs.clone())
                        .unwrap_or(LeaveOuts(Vec::new())),
                ),
            });
        }
        for entry in &entries {
            let address = &entry.leave_out.address;
            let harness_kind = &entry.leave_out.harness_kind;
            if fold
                .folded
                .iter()
                .any(|(folded, kind)| folded == address && kind == harness_kind)
            {
                continue;
            }
            let reason = match inventory
                .rows()
                .iter()
                .any(|(_, row)| &row.metaflow.address == address)
            {
                false => LeaveOutDropped::NoPermaflow,
                true => match harness_of(address) {
                    Some(harness) => LeaveOutDropped::OtherHarness(harness),
                    None => LeaveOutDropped::HarnessUnknown,
                },
            };
            eprintln!(
                "flow-nexus: store upgrade: the leave-outs of {address:?} on {harness_kind:?} are dropped ({reason:?}): {:?}",
                entry.leave_out.leave_outs.0
            );
            fold.dropped
                .push((address.clone(), harness_kind.clone(), reason));
        }
        for (address, harness_kind) in &fold.folded {
            eprintln!(
                "flow-nexus: store upgrade: the leave-outs of {address:?} on {harness_kind:?} are its permaflow's"
            );
        }
        let mut evolution = engine
            .begin_atomic_evolution()
            .evolve(inventory, descriptor, rows);
        if let Some(table) = retired {
            for entry in &entries {
                evolution = evolution.retract(table, entry.record_key());
            }
        }
        engine.commit_atomic_evolution(evolution)?;
        Ok(fold)
    }
}

/// The retired family's identity, which a 0.55.0 store may still hold,
/// empty, after its upgrade.
pub(crate) trait NamesRetiredLeaveOutFamily {
    fn retired_leave_out_identity() -> (&'static str, SchemaHash);
}

impl NamesRetiredLeaveOutFamily for FlowStore {
    fn retired_leave_out_identity() -> (&'static str, SchemaHash) {
        (
            META_LEAVE_OUT_CONFIGURATION_FAMILY,
            StoredMetaLeaveOutConfigurationV3::schema_hash(),
        )
    }
}

/// The permaflow family's 0.54.7 identity, which a store not yet upgraded
/// holds.
pub(crate) trait NamesMetaflowDeclarationV6 {
    fn metaflow_declaration_v6_identity() -> (&'static str, SchemaHash);
}

impl NamesMetaflowDeclarationV6 for FlowStore {
    fn metaflow_declaration_v6_identity() -> (&'static str, SchemaHash) {
        (
            META_METAFLOW_CONFIGURATION_FAMILY,
            StoredMetaMetaflowConfigurationV6::schema_hash(),
        )
    }
}
