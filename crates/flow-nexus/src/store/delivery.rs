//! What Flow keeps of the panes it writes: each settled Delivery by its
//! DeliveryId, the lease row a delivery holds while it types, and the
//! delivery half of the configuration (harness profiles, the aspects the
//! meta socket admits, the Message Nexus path).
//!
//! A lease row exists only while a delivery is typing. It records how far
//! the key sequence went (`LeaseStep`). A row still present when the store
//! opens is a delivery a crash interrupted: it settles `Uncertain` then and
//! there and is never retried, since what reached the pane is not known.

use super::{DefaultConfiguration, FlowStore, StoreError};
use meta_signal_flow::{
    Delivery, DeliveryGrade, DeliveryId, HarnessProfile, InterruptWitness, MessageNexusPath,
    MetaAspects,
};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use sema_engine::{
    Assertion, EngineRecord, FamilyName, KeyedMutation, QueryPlan, RecordKey, Retraction,
    SchemaHash, TableDescriptor, TableName, TableReference,
};
use signal_flow::{FlowAspect, HarnessKind};

pub(super) const DELIVERY_TABLE_NAME: TableName = TableName::new("flow_nexus_deliveries");
pub(super) const PANE_LEASE_TABLE_NAME: TableName = TableName::new("flow_nexus_pane_leases");
pub(super) const DELIVERY_CONFIGURATION_TABLE_NAME: TableName =
    TableName::new("flow_nexus_delivery_configuration");
const DELIVERY_CONFIGURATION_KEY: &str = "delivery";

/// A settled delivery, keyed by its DeliveryId: what a repeat answers.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub struct StoredDelivery {
    pub delivery: Delivery,
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

impl PaneLease {
    /// What a delivery interrupted at this row settles as.
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

impl From<&meta_signal_flow::Configuration> for DeliveryConfiguration {
    fn from(configuration: &meta_signal_flow::Configuration) -> Self {
        Self {
            harness_profile_vector: configuration.harness_profile_vector.clone(),
            meta_aspects: configuration.meta_aspects.clone(),
            message_nexus_path: configuration.message_nexus_path.clone(),
        }
    }
}

impl DeliveryConfiguration {
    /// The profile Flow types with for a harness. A harness configured with
    /// no profile falls back to its default one.
    pub fn profile(&self, harness_kind: &HarnessKind) -> HarnessProfile {
        self.harness_profile_vector
            .iter()
            .find(|profile| &profile.harness_kind == harness_kind)
            .cloned()
            .unwrap_or_else(|| DefaultConfiguration::harness_profile(harness_kind))
    }
}

impl DefaultConfiguration {
    /// The keymaps and command sigils as witnessed on this cluster: Claude
    /// runs in vim mode, so its interrupt is two Escapes, and a HardAbrupt
    /// prompt is followed by one Enter; Codex interrupts on one Escape and
    /// `agent prompt` submits for it. Claude's `#` is its memory mode.
    pub fn harness_profile(harness_kind: &HarnessKind) -> HarnessProfile {
        let keys = |keys: &[&str]| keys.iter().map(|key| (*key).to_owned()).collect();
        match harness_kind {
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

    /// Psyche seats deploy and manage flows, so they alone among flows
    /// reach the meta socket by default. No Message Nexus executable is
    /// admitted by path until one is configured.
    pub fn delivery_configuration(&self) -> DeliveryConfiguration {
        DeliveryConfiguration {
            harness_profile_vector: vec![
                Self::harness_profile(&HarnessKind::Claude),
                Self::harness_profile(&HarnessKind::Codex),
            ],
            meta_aspects: vec![FlowAspect::Psyche],
            message_nexus_path: String::new(),
        }
    }
}

/// The delivery tables, registered beside the flow tables.
pub struct DeliveryTables {
    pub deliveries: TableReference<StoredDelivery>,
    pub pane_leases: TableReference<PaneLease>,
    pub configuration: TableReference<DeliveryConfiguration>,
}

impl DeliveryTables {
    pub fn register(engine: &mut sema_engine::Engine) -> Result<Self, StoreError> {
        Ok(Self {
            deliveries: engine.register_table(TableDescriptor::new(
                DELIVERY_TABLE_NAME,
                FamilyName::new("flow-nexus-delivery"),
                SchemaHash::for_label("flow-nexus-delivery-v1"),
            ))?,
            pane_leases: engine.register_table(TableDescriptor::new(
                PANE_LEASE_TABLE_NAME,
                FamilyName::new("flow-nexus-pane-lease"),
                SchemaHash::for_label("flow-nexus-pane-lease-v1"),
            ))?,
            configuration: engine.register_table(TableDescriptor::new(
                DELIVERY_CONFIGURATION_TABLE_NAME,
                FamilyName::new("flow-nexus-delivery-configuration"),
                SchemaHash::for_label("flow-nexus-delivery-configuration-v1"),
            ))?,
        })
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
    fn delivery_configuration(&self) -> Result<DeliveryConfiguration, StoreError>;
    fn configure_delivery(&self, configuration: DeliveryConfiguration) -> Result<(), StoreError>;
    /// Seeds the delivery configuration on a store that has none.
    fn seed_delivery_configuration(
        &self,
        defaults: &DefaultConfiguration,
    ) -> Result<(), StoreError>;
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
            let delivery = lease.uncertain();
            self.settle_delivery(delivery.clone())?;
            settled.push(delivery);
        }
        Ok(settled)
    }

    fn delivery_configuration(&self) -> Result<DeliveryConfiguration, StoreError> {
        let records = self
            .engine
            .match_records(QueryPlan::key(
                self.delivery_tables.configuration,
                RecordKey::new(DELIVERY_CONFIGURATION_KEY),
            ))?
            .records()
            .to_vec();
        match records.as_slice() {
            [configuration] => Ok(configuration.clone()),
            _ => Err(StoreError::StateInvariant),
        }
    }

    fn configure_delivery(&self, configuration: DeliveryConfiguration) -> Result<(), StoreError> {
        self.engine.mutate_keyed(KeyedMutation::new(
            self.delivery_tables.configuration,
            RecordKey::new(DELIVERY_CONFIGURATION_KEY),
            configuration,
        ))?;
        Ok(())
    }

    fn seed_delivery_configuration(
        &self,
        defaults: &DefaultConfiguration,
    ) -> Result<(), StoreError> {
        if self
            .engine
            .match_records(QueryPlan::key(
                self.delivery_tables.configuration,
                RecordKey::new(DELIVERY_CONFIGURATION_KEY),
            ))?
            .records()
            .is_empty()
        {
            self.engine.assert(Assertion::new(
                self.delivery_tables.configuration,
                defaults.delivery_configuration(),
            ))?;
        }
        Ok(())
    }
}
