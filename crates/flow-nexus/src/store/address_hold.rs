//! A launched seat's hold on its Address. A launch takes the Address its
//! profile names in the write that acknowledges the seat's witnessed native
//! binding, before its first prompt is typed, so the seat is identified
//! from its first tool call. Until the launch is confirmed the taking is a
//! hold: the row keeps the metaflow as the launch found it. Confirmation
//! keeps the Address and drops the hold; a launch refused, or a flow that
//! ends before it was confirmed, gives the Address back, the metaflow
//! returning to what the hold kept (absent when there was none) with any
//! request queued meanwhile still queued.
//!
//! A hold behind a flow that ended without giving it back, as an older
//! Nexus could leave it, is given back by the next Start at its Address.
//!
//! A confirmed flow that ends (Stopped, Retired or Exited) while its
//! metaflow is Awake as it puts the metaflow to sleep in the same write,
//! the flow joining Past: a topic never dies, its flows are turned off.
//!
//! The row lives in its own table, so a store written before holds were
//! kept opens with the table added and holds nothing.

use super::message_v41::MetaflowV41;
use super::metaflow::{AddressTaking, SleepsMetaflows, TakesLaunchAddress, WritesTakenAddress};
use super::{
    FlowStore, ManagesMetaflowState, MetaflowState, ReadsFlowMemory, StoreError, StoredMetaflow,
};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use sema_engine::{
    AtomicCommit, Decision, EngineRecord, FamilyName, MemoryDefinition, MemoryVersion, QueryPlan,
    RecordKey, SchemaHash, TableDescriptor, TableName, UpgradeFrom,
};
use signal_flow::{Address, LaunchProfile, Metaflow, StartRejection};

pub(super) const ADDRESS_HOLD_TABLE_NAME: TableName = TableName::new("flow_nexus_address_holds");
pub(super) const ADDRESS_HOLD_FAMILY: &str = "flow-nexus-address-hold";
pub(super) const ADDRESS_HOLD_SCHEMA: &str = "flow-nexus-address-hold-v5";

/// The Address a launched flow holds, and the metaflow it found there.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub struct AddressHold {
    pub flow_id: String,
    pub address: Address,
    pub found: Option<Metaflow>,
}

impl EngineRecord for AddressHold {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.flow_id.clone())
    }
}

impl MemoryDefinition for AddressHold {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(5)
    }

    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label("flow-nexus-address-hold-v5")
    }
}

/// The current hold through Flow 0.41, whose found metaflow embeds the old
/// Result(String) request shape.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub struct AddressHoldV4 {
    flow_id: String,
    address: Address,
    found: Option<MetaflowV41>,
}

impl EngineRecord for AddressHoldV4 {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.flow_id.clone())
    }
}

impl MemoryDefinition for AddressHoldV4 {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(4)
    }

    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label("flow-nexus-address-hold-v4")
    }
}

impl UpgradeFrom<AddressHoldV4> for AddressHold {
    type Refusal = std::convert::Infallible;

    fn upgrade_from(previous: AddressHoldV4) -> Decision<Self, Self::Refusal> {
        Decision::Ready(Self {
            flow_id: previous.flow_id,
            address: previous.address,
            found: previous.found.map(Into::into),
        })
    }
}

/// An Address hold as Flow 0.35.22 to 0.37.2 kept it, before `SentBy`
/// named Flow. The variant is appended, so its bytes read as the current
/// hold; the v3-to-v4 upgrade moves it unchanged.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub struct AddressHoldV3 {
    flow_id: String,
    address: Address,
    found: Option<MetaflowV41>,
}

impl EngineRecord for AddressHoldV3 {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.flow_id.clone())
    }
}

impl MemoryDefinition for AddressHoldV3 {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(3)
    }

    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label("flow-nexus-address-hold-v3")
    }
}

impl UpgradeFrom<AddressHoldV3> for AddressHold {
    type Refusal = std::convert::Infallible;

    fn upgrade_from(previous: AddressHoldV3) -> Decision<Self, Self::Refusal> {
        Decision::Ready(Self {
            flow_id: previous.flow_id,
            address: previous.address,
            found: previous.found.map(Into::into),
        })
    }
}

/// An Address hold as Flow 0.35.7 to 0.35.21 kept it, the metaflow it
/// found possibly Ended. It is read only by the v2-to-v4 upgrade, which
/// carries an ended metaflow Asleep.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub struct AddressHoldV2 {
    flow_id: String,
    address: Address,
    found: Option<super::MetaflowBeforeIdle>,
}

impl EngineRecord for AddressHoldV2 {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.flow_id.clone())
    }
}

impl MemoryDefinition for AddressHoldV2 {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(2)
    }

    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label("flow-nexus-address-hold-v2")
    }
}

impl UpgradeFrom<AddressHoldV2> for AddressHold {
    type Refusal = std::convert::Infallible;

    fn upgrade_from(previous: AddressHoldV2) -> Decision<Self, Self::Refusal> {
        Decision::Ready(Self {
            flow_id: previous.flow_id,
            address: previous.address,
            found: previous.found.map(Metaflow::from),
        })
    }
}

/// An Address hold as Flow kept it before 0.35.7, the metaflow it found
/// holding bare requests. It is read only by the v1-to-v4 upgrade.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub struct AddressHoldV1 {
    flow_id: String,
    address: Address,
    found: Option<super::MetaflowBeforeSenders>,
}

impl EngineRecord for AddressHoldV1 {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.flow_id.clone())
    }
}

impl MemoryDefinition for AddressHoldV1 {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(1)
    }

    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label("flow-nexus-address-hold-v1")
    }
}

impl UpgradeFrom<AddressHoldV1> for AddressHold {
    type Refusal = std::convert::Infallible;

    fn upgrade_from(previous: AddressHoldV1) -> Decision<Self, Self::Refusal> {
        Decision::Ready(Self {
            flow_id: previous.flow_id,
            address: previous.address,
            found: previous.found.map(Metaflow::from),
        })
    }
}

pub(super) trait DescribesAddressHoldTable {
    fn descriptor() -> TableDescriptor<AddressHold>;
}

impl DescribesAddressHoldTable for AddressHold {
    fn descriptor() -> TableDescriptor<AddressHold> {
        TableDescriptor::new(
            ADDRESS_HOLD_TABLE_NAME,
            FamilyName::new(ADDRESS_HOLD_FAMILY),
            AddressHold::schema_hash(),
        )
        .with_predecessor::<AddressHoldV4>()
        .with_predecessor::<AddressHoldV1>()
        .with_predecessor::<AddressHoldV2>()
        .with_predecessor::<AddressHoldV3>()
    }
}

/// The writes that take, keep and give back a launch's Address. Each adds
/// to an atomic commit the caller writes with the change it belongs to.
pub(crate) trait HoldsLaunchAddress {
    /// Takes the profile's Address as `flow_id` and holds it, under the
    /// rules a launch takes its Address by; the rejection when it cannot.
    fn held(
        &self,
        profile: &LaunchProfile,
        flow_id: &str,
        commit: AtomicCommit,
    ) -> Result<Result<AtomicCommit, StartRejection>, StoreError>;
    /// The launch of `flow_id` is confirmed: the Address stays taken and
    /// the hold goes.
    fn kept(&self, flow_id: &str, commit: AtomicCommit) -> Result<AtomicCommit, StoreError>;
    /// The launch of `flow_id` came to nothing: a metaflow still Awake as
    /// it returns to what the hold found, keeping its queue, and the hold
    /// goes. Without a hold nothing is written.
    fn given_back(&self, flow_id: &str, commit: AtomicCommit) -> Result<AtomicCommit, StoreError>;
    /// `flow_id` ended: its hold is given back when it has one; otherwise a
    /// metaflow Awake as it is recorded Asleep, the flow joining Past, and
    /// every lock that metaflow holds as a sender is retracted, so a
    /// recipient is never held by a sender that has ended.
    fn ended_at_address(
        &self,
        flow_id: &str,
        commit: AtomicCommit,
    ) -> Result<AtomicCommit, StoreError>;
    /// Retracts every lock the metaflow at `sender` holds as a sender.
    fn sender_locks_released(
        &self,
        sender: &Address,
        commit: AtomicCommit,
    ) -> Result<AtomicCommit, StoreError>;
    /// The hold `flow_id` has, if any.
    fn address_hold(&self, flow_id: &str) -> Result<Option<AddressHold>, StoreError>;
    /// Gives back, each in its own write, every hold on `address` whose
    /// flow has ended or was never registered, as an older Nexus that did
    /// not give a hold back with its flow's end left it; the flows whose
    /// holds were given back.
    fn give_back_ended_holds(&self, address: &Address) -> Result<Vec<String>, StoreError>;
}

impl HoldsLaunchAddress for FlowStore {
    fn held(
        &self,
        profile: &LaunchProfile,
        flow_id: &str,
        commit: AtomicCommit,
    ) -> Result<Result<AtomicCommit, StartRejection>, StoreError> {
        if self.address_hold(flow_id)?.is_some() {
            return Ok(Ok(commit));
        }
        let taken = match self.address_taking(
            &profile.address,
            flow_id,
            profile.flow_id_option.as_deref(),
            0,
        )? {
            AddressTaking::Takes(taken) => taken,
            AddressTaking::Refused(rejection) => return Ok(Err(rejection)),
        };
        let found = self
            .stored_metaflow(&profile.address)?
            .map(|stored| stored.metaflow);
        let hold = AddressHold {
            flow_id: flow_id.to_owned(),
            address: profile.address.clone(),
            found,
        };
        Ok(Ok(taken
            .written(self, commit)
            .assert(self.address_holds, hold)))
    }

    fn kept(&self, flow_id: &str, commit: AtomicCommit) -> Result<AtomicCommit, StoreError> {
        use super::successor::OwesSuccessors;
        // A confirmed flow is the successor its Address was owed, if any.
        Ok(match self.address_hold(flow_id)? {
            Some(hold) => self.successor_given(
                &hold.address,
                commit.retract(self.address_holds, RecordKey::new(flow_id)),
            )?,
            None => commit,
        })
    }

    fn given_back(&self, flow_id: &str, commit: AtomicCommit) -> Result<AtomicCommit, StoreError> {
        let Some(hold) = self.address_hold(flow_id)? else {
            return Ok(commit);
        };
        let commit = commit.retract(self.address_holds, RecordKey::new(flow_id));
        let Some(current) = self.stored_metaflow(&hold.address)? else {
            return Ok(commit);
        };
        // A metaflow another binding or an End changed since is left as it is.
        if current.metaflow.state != MetaflowState::Awake(flow_id.to_owned()) {
            return Ok(commit);
        }
        let queue = current.metaflow.queue;
        Ok(match hold.found {
            Some(found) => commit.mutate(
                self.metaflows,
                StoredMetaflow {
                    metaflow: Metaflow { queue, ..found },
                },
            ),
            None if queue.is_empty() => commit.retract(
                self.metaflows,
                RecordKey::new(FlowStore::address_record_key(&hold.address)),
            ),
            None => commit.mutate(
                self.metaflows,
                StoredMetaflow {
                    metaflow: Metaflow {
                        address: hold.address,
                        state: MetaflowState::Asleep,
                        past: vec![],
                        queue,
                    },
                },
            ),
        })
    }

    fn ended_at_address(
        &self,
        flow_id: &str,
        commit: AtomicCommit,
    ) -> Result<AtomicCommit, StoreError> {
        if self.address_hold(flow_id)?.is_some() {
            return self.given_back(flow_id, commit);
        }
        let Some(mut metaflow) = self.metaflow_awake_as(flow_id)? else {
            return Ok(commit);
        };
        let address = metaflow.address.clone();
        metaflow.state = MetaflowState::Asleep;
        metaflow.past.retain(|past| past != flow_id);
        metaflow.past.push(flow_id.to_owned());
        let commit = commit.mutate(self.metaflows, StoredMetaflow { metaflow });
        self.sender_locks_released(&address, commit)
    }

    fn sender_locks_released(
        &self,
        sender: &Address,
        commit: AtomicCommit,
    ) -> Result<AtomicCommit, StoreError> {
        let held = self
            .engine
            .match_records(QueryPlan::all(self.metaflow_locks))?
            .records()
            .iter()
            .filter(|row| &row.lock.sender == sender)
            .map(|row| row.lock.address.clone())
            .collect::<Vec<_>>();
        Ok(held.into_iter().fold(commit, |commit, recipient| {
            commit.retract(
                self.metaflow_locks,
                RecordKey::new(FlowStore::address_record_key(&recipient)),
            )
        }))
    }

    fn address_hold(&self, flow_id: &str) -> Result<Option<AddressHold>, StoreError> {
        let records = self
            .engine
            .match_records(QueryPlan::key(self.address_holds, RecordKey::new(flow_id)))?
            .records()
            .to_vec();
        match records.as_slice() {
            [] => Ok(None),
            [hold] => Ok(Some(hold.clone())),
            _ => Err(StoreError::StateInvariant),
        }
    }

    fn give_back_ended_holds(&self, address: &Address) -> Result<Vec<String>, StoreError> {
        let holds = self
            .engine
            .match_records(QueryPlan::all(self.address_holds))?
            .records()
            .to_vec();
        let mut given = Vec::new();
        for hold in holds {
            if &hold.address != address || self.flow_is_live(&hold.flow_id)? {
                continue;
            }
            let commit = self.given_back(&hold.flow_id, self.engine.begin_atomic_commit())?;
            self.engine.commit_atomic(commit)?;
            given.push(hold.flow_id);
        }
        Ok(given)
    }
}

/// Writes a hold and its Awake metaflow as an older Nexus left them behind
/// a flow it then ended without giving the Address back.
#[cfg(test)]
pub(crate) trait LeavesStaleHold {
    fn leave_stale_hold(&self, flow_id: &str, address: &Address);
}

#[cfg(test)]
impl LeavesStaleHold for FlowStore {
    fn leave_stale_hold(&self, flow_id: &str, address: &Address) {
        let commit = self
            .engine
            .begin_atomic_commit()
            .assert(
                self.metaflows,
                StoredMetaflow {
                    metaflow: Metaflow {
                        address: address.clone(),
                        state: MetaflowState::Awake(flow_id.to_owned()),
                        past: vec![],
                        queue: vec![],
                    },
                },
            )
            .assert(
                self.address_holds,
                AddressHold {
                    flow_id: flow_id.to_owned(),
                    address: address.clone(),
                    found: None,
                },
            );
        self.engine
            .commit_atomic(commit)
            .expect("stale hold written");
    }
}
