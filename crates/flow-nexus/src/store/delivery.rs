//! What Flow keeps of the panes it writes: each settled Delivery by its
//! DeliveryId, the lease row a delivery holds while it types, and the
//! delivery half of the configuration (harness profiles, the aspects the
//! meta socket admits, the Message Nexus path).
//!
//! A lease row exists only while a delivery is typing. It records how far
//! the key sequence went (`LeaseStep`). A row still present when the store
//! opens is a delivery a crash interrupted: it settles `Uncertain` then and
//! there and is never retried, since what reached the pane is not known.

use super::queue_delivery::AccountsQueueDelivery;
use super::{FlowStore, StoreError};
use meta_signal_flow::{
    Delivery, DeliveryGrade, DeliveryId, DeliveryOutcome, DeliveryResolution, HarnessProfile,
    InterruptWitness, MessageNexusPath, MetaAspects, ResolveDeliveryRejected_Data,
};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use sema_engine::{
    Assertion, Decision, EngineRecord, FamilyName, KeyedMutation, MemoryDefinition, MemoryUpgrades,
    MemoryVersion, QueryPlan, RecordKey, Retraction, SchemaHash, TableDescriptor, TableName,
    TableReference, UpgradeFrom,
};
use signal_flow::HarnessKind;

pub(super) const DELIVERY_TABLE_NAME: TableName = TableName::new("flow_nexus_deliveries");
pub(super) const PANE_LEASE_TABLE_NAME: TableName = TableName::new("flow_nexus_pane_leases");
pub(super) const DELIVERY_CONFIGURATION_TABLE_NAME: TableName =
    TableName::new("flow_nexus_delivery_configuration");
pub(super) const DELIVERY_CONFIGURATION_KEY: &str = "delivery";

/// A settled delivery, keyed by its DeliveryId: what a repeat answers.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub struct StoredDelivery {
    pub delivery: Delivery,
}

impl MemoryDefinition for StoredDelivery {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(2)
    }
    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label("flow-nexus-delivery-v2")
    }
}

/// A delivery before owners could resolve an uncertain write as explicitly
/// not presented. Frozen for the v1-to-v2 store upgrade.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub enum DeliveryGradeV1 {
    Transported,
    Presented,
    Uncertain,
}
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub struct DeliveryV1 {
    pub delivery_id: DeliveryId,
    pub flow_id: String,
    pub interrupt_witness: InterruptWitness,
    pub delivery_grade: DeliveryGradeV1,
}
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub struct StoredDeliveryV1 {
    pub delivery: DeliveryV1,
}
impl EngineRecord for StoredDeliveryV1 {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.delivery.delivery_id.clone())
    }
}
impl MemoryDefinition for StoredDeliveryV1 {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(1)
    }
    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label("flow-nexus-delivery-v1")
    }
}
impl UpgradeFrom<StoredDeliveryV1> for StoredDelivery {
    type Refusal = std::convert::Infallible;
    fn upgrade_from(previous: StoredDeliveryV1) -> Decision<Self, Self::Refusal> {
        let old = previous.delivery;
        Decision::Ready(Self {
            delivery: Delivery {
                delivery_id: old.delivery_id,
                flow_id: old.flow_id,
                interrupt_witness: old.interrupt_witness,
                delivery_grade: match old.delivery_grade {
                    DeliveryGradeV1::Transported => DeliveryGrade::Transported,
                    DeliveryGradeV1::Presented => DeliveryGrade::Presented,
                    DeliveryGradeV1::Uncertain => DeliveryGrade::Uncertain,
                },
            },
        })
    }
}

impl EngineRecord for StoredDelivery {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.delivery.delivery_id.clone())
    }
}

/// How far a leased key sequence went.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeaseStep {
    /// The lease is held; nothing has been sent to the pane yet.
    Acquired,
    /// The interrupt keys were sent.
    Interrupted,
    /// Herdr accepted the text.
    Placed,
    /// The submit keys were sent.
    Submitted,
}

/// The Sema row of a delivery under the lease.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub struct PaneLease {
    pub delivery_id: DeliveryId,
    pub flow_id: String,
    pub herdr_pane_id: String,
    pub lease_step: LeaseStep,
}

impl EngineRecord for PaneLease {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.delivery_id.clone())
    }
}

trait SettlesInterruptedLease {
    /// What a delivery interrupted at this row settles as.
    fn uncertain(&self) -> Delivery;
}

impl SettlesInterruptedLease for PaneLease {
    fn uncertain(&self) -> Delivery {
        Delivery {
            delivery_id: self.delivery_id.clone(),
            flow_id: self.flow_id.clone(),
            interrupt_witness: match self.lease_step {
                LeaseStep::Acquired => InterruptWitness::NotRequested,
                LeaseStep::Interrupted | LeaseStep::Placed | LeaseStep::Submitted => {
                    InterruptWitness::Unobserved
                }
            },
            delivery_grade: DeliveryGrade::Uncertain,
        }
    }
}

impl PaneLease {
    pub(crate) fn uncertain_delivery(&self) -> Delivery {
        self.uncertain()
    }
}

/// The delivery half of the meta Configuration, kept in its own record so a
/// store written before it existed reads unchanged and is seeded on open.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub struct DeliveryConfiguration {
    pub harness_profile_vector: Vec<HarnessProfile>,
    pub meta_aspects: MetaAspects,
    pub message_nexus_path: MessageNexusPath,
}

impl EngineRecord for DeliveryConfiguration {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(DELIVERY_CONFIGURATION_KEY)
    }
}

impl From<&crate::store::Configuration> for DeliveryConfiguration {
    fn from(configuration: &crate::store::Configuration) -> Self {
        Self {
            harness_profile_vector: configuration.harness_profile_vector.clone(),
            meta_aspects: configuration.meta_aspects.clone(),
            message_nexus_path: configuration.message_nexus_path.clone(),
        }
    }
}

/// The profile Flow types with for a harness.
pub trait SelectsHarnessProfile {
    /// A harness configured with no profile falls back to its default one.
    fn profile(&self, harness_kind: &HarnessKind) -> HarnessProfile;
}

impl SelectsHarnessProfile for DeliveryConfiguration {
    fn profile(&self, harness_kind: &HarnessKind) -> HarnessProfile {
        self.harness_profile_vector
            .iter()
            .find(|profile| &profile.harness_kind == harness_kind)
            .cloned()
            .unwrap_or_else(|| harness_kind.default_profile())
    }
}

/// A harness's default keymaps and command sigils.
pub trait DefaultsHarnessProfile {
    fn default_profile(&self) -> HarnessProfile;
}

impl DefaultsHarnessProfile for HarnessKind {
    /// The keymaps and command sigils as witnessed on this cluster: Claude
    /// runs in vim mode, so its interrupt is two Escapes, and a HardAbrupt
    /// prompt is followed by one Enter; Codex interrupts on one Escape and
    /// `agent prompt` submits for it. Claude's `#` is its memory mode.
    fn default_profile(&self) -> HarnessProfile {
        let keys = |keys: &[&str]| keys.iter().map(|key| (*key).to_owned()).collect();
        match self {
            HarnessKind::Claude => HarnessProfile {
                harness_kind: HarnessKind::Claude,
                command_sigil_vector: keys(&["/", "!", "#"]),
                interrupt_keys: keys(&["esc", "esc"]),
                submit_keys: keys(&["enter"]),
            },
            HarnessKind::Codex => HarnessProfile {
                harness_kind: HarnessKind::Codex,
                command_sigil_vector: keys(&["/", "!"]),
                interrupt_keys: keys(&["esc"]),
                submit_keys: Vec::new(),
            },
        }
    }
}

/// The delivery tables, registered beside the flow tables.
pub struct DeliveryTables {
    pub deliveries: TableReference<StoredDelivery>,
    pub pane_leases: TableReference<PaneLease>,
}

pub trait RegistersDeliveryTables: Sized {
    fn register(engine: &mut sema_engine::Engine) -> Result<Self, StoreError>;
}

impl RegistersDeliveryTables for DeliveryTables {
    fn register(engine: &mut sema_engine::Engine) -> Result<Self, StoreError> {
        let delivery_descriptor = TableDescriptor::new(
            DELIVERY_TABLE_NAME,
            FamilyName::new("flow-nexus-delivery"),
            SchemaHash::for_label("flow-nexus-delivery-v2"),
        )
        .with_predecessor::<StoredDeliveryV1>();
        let deliveries =
            match engine.upgrade_table::<StoredDeliveryV1, StoredDelivery>(delivery_descriptor) {
                sema_engine::UpgradeOutcome::Upgraded(receipt) => receipt.table(),
                sema_engine::UpgradeOutcome::AlreadyCurrent(table) => table,
                sema_engine::UpgradeOutcome::Refused(refusal) => match *refusal.reason() {},
                sema_engine::UpgradeOutcome::Failed(failure) => {
                    return Err(StoreError::NexusUpgrade(format!(
                        "delivery upgrade: {failure:?}"
                    )));
                }
            };
        Ok(Self {
            deliveries,
            pane_leases: engine.register_table(TableDescriptor::new(
                PANE_LEASE_TABLE_NAME,
                FamilyName::new("flow-nexus-pane-lease"),
                SchemaHash::for_label("flow-nexus-pane-lease-v1"),
            ))?,
        })
    }
}

pub trait ResolvesDeliveries {
    fn resolve_delivery(
        &self,
        resolution: DeliveryResolution,
    ) -> Result<Result<Delivery, ResolveDeliveryRejected_Data>, StoreError>;
}

impl ResolvesDeliveries for FlowStore {
    fn resolve_delivery(
        &self,
        resolution: DeliveryResolution,
    ) -> Result<Result<Delivery, ResolveDeliveryRejected_Data>, StoreError> {
        if resolution.queue_resolution_reason.trim().is_empty() {
            return Ok(Err(ResolveDeliveryRejected_Data::EmptyReason));
        }
        let Some(mut delivery) = self.delivery(&resolution.delivery_id)? else {
            return Ok(Err(ResolveDeliveryRejected_Data::UnknownDelivery));
        };
        if delivery.delivery_grade != DeliveryGrade::Uncertain {
            return Ok(Err(ResolveDeliveryRejected_Data::NotUncertain));
        }
        delivery.delivery_grade = match resolution.delivery_outcome {
            DeliveryOutcome::Presented => DeliveryGrade::Presented,
            DeliveryOutcome::NotPresented => DeliveryGrade::NotPresented,
        };
        self.engine.mutate_keyed(KeyedMutation::new(
            self.delivery_tables.deliveries,
            RecordKey::new(delivery.delivery_id.clone()),
            StoredDelivery {
                delivery: delivery.clone(),
            },
        ))?;
        Ok(Ok(delivery))
    }
}

/// Reads and writes what Flow keeps of its pane writes.
pub trait RecordsDeliveries {
    fn delivery(&self, delivery_id: &str) -> Result<Option<Delivery>, StoreError>;
    /// Writes or advances the lease row of a delivery that is typing.
    fn record_lease_step(&self, lease: PaneLease) -> Result<(), StoreError>;
    /// Settles a delivery: its outcome is kept and its lease row removed,
    /// in one commit.
    fn settle_delivery(&self, delivery: Delivery) -> Result<(), StoreError>;
    /// Removes the lease row of a delivery that typed nothing.
    fn release_lease(&self, delivery_id: &str) -> Result<(), StoreError>;
    /// Settles every lease row a crash left behind as Uncertain.
    fn settle_interrupted_deliveries(&self) -> Result<Vec<Delivery>, StoreError>;
}

impl RecordsDeliveries for FlowStore {
    fn delivery(&self, delivery_id: &str) -> Result<Option<Delivery>, StoreError> {
        let records = self
            .engine
            .match_records(QueryPlan::key(
                self.delivery_tables.deliveries,
                RecordKey::new(delivery_id),
            ))?
            .records()
            .to_vec();
        match records.as_slice() {
            [] => Ok(None),
            [stored] => Ok(Some(stored.delivery.clone())),
            _ => Err(StoreError::StateInvariant),
        }
    }

    fn record_lease_step(&self, lease: PaneLease) -> Result<(), StoreError> {
        let present = !self
            .engine
            .match_records(QueryPlan::key(
                self.delivery_tables.pane_leases,
                lease.record_key(),
            ))?
            .records()
            .is_empty();
        if present {
            self.engine.mutate_keyed(KeyedMutation::new(
                self.delivery_tables.pane_leases,
                lease.record_key(),
                lease,
            ))?;
        } else {
            self.engine
                .assert(Assertion::new(self.delivery_tables.pane_leases, lease))?;
        }
        Ok(())
    }

    fn settle_delivery(&self, delivery: Delivery) -> Result<(), StoreError> {
        let key = RecordKey::new(delivery.delivery_id.clone());
        let leased = !self
            .engine
            .match_records(QueryPlan::key(
                self.delivery_tables.pane_leases,
                key.clone(),
            ))?
            .records()
            .is_empty();
        let mut commit = self
            .engine
            .begin_atomic_commit()
            .assert(self.delivery_tables.deliveries, StoredDelivery { delivery });
        if leased {
            commit = commit.retract(self.delivery_tables.pane_leases, key);
        }
        self.engine.commit_atomic(commit)?;
        Ok(())
    }

    fn release_lease(&self, delivery_id: &str) -> Result<(), StoreError> {
        let key = RecordKey::new(delivery_id);
        if !self
            .engine
            .match_records(QueryPlan::key(
                self.delivery_tables.pane_leases,
                key.clone(),
            ))?
            .records()
            .is_empty()
        {
            self.engine
                .retract(Retraction::new(self.delivery_tables.pane_leases, key))?;
        }
        Ok(())
    }

    fn settle_interrupted_deliveries(&self) -> Result<Vec<Delivery>, StoreError> {
        let leases = self
            .engine
            .match_records(QueryPlan::all(self.delivery_tables.pane_leases))?
            .records()
            .to_vec();
        let mut settled = Vec::with_capacity(leases.len());
        for lease in leases {
            if let Some(delivery) = self.settle_interrupted_typed_cohort(&lease.delivery_id)? {
                settled.push(delivery);
                continue;
            }
            let delivery = lease.uncertain();
            self.settle_delivery(delivery.clone())?;
            settled.push(delivery);
        }
        Ok(settled)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn v1_delivery_upgrade_preserves_each_old_grade() {
        for (old, expected) in [
            (DeliveryGradeV1::Transported, DeliveryGrade::Transported),
            (DeliveryGradeV1::Presented, DeliveryGrade::Presented),
            (DeliveryGradeV1::Uncertain, DeliveryGrade::Uncertain),
        ] {
            let previous = StoredDeliveryV1 {
                delivery: DeliveryV1 {
                    delivery_id: "delivery".into(),
                    flow_id: "flow".into(),
                    interrupt_witness: InterruptWitness::Unobserved,
                    delivery_grade: old,
                },
            };
            let Decision::Ready(upgraded) = StoredDelivery::upgrade_from(previous);
            assert_eq!(upgraded.delivery.delivery_grade, expected);
        }
    }
}
