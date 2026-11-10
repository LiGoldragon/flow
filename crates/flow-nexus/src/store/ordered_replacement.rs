//! The replacements ordered from outside that wait for their flow's idle
//! event. A Refresh.Address, or a Launch or Replace naming a live flow,
//! never closes a working flow: when the flow does not rest, the order is
//! recorded here, keyed by its metaflow's Address, and carried out at the
//! flow's idle event, its Stop or its Codex thread's turn completed, once
//! the flow rests. A Launch's order holds its whole profile, its origin and
//! the digest of the first prompt it composed to, so a Nexus that starts
//! again carries it out as it was asked. The flow still receives what is
//! sent to it until it rests.

use super::{AnnouncesLaunchChanges, FlowStore, ReadsFlowMemory, StoreError};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use sema_engine::{
    AtomicCommit, EngineRecord, FamilyName, MemoryDefinition, MemoryVersion, QueryPlan, RecordKey,
    SchemaHash, TableDescriptor, TableName,
};
use signal_flow::{Address, LaunchAttempt, LaunchAttemptPhase, LaunchProfile, Origin};

pub(super) const ORDERED_REPLACEMENT_TABLE_NAME: TableName =
    TableName::new("flow_nexus_ordered_replacements");
pub(super) const ORDERED_REPLACEMENT_FAMILY: &str = "flow-nexus-ordered-replacement";
pub(super) const ORDERED_REPLACEMENT_SCHEMA: &str = "flow-nexus-ordered-replacement-v1";

/// A Launch, or a Replace, that names a working flow: its profile names
/// that flow as its predecessor.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub struct OrderedLaunch {
    pub launch_profile: LaunchProfile,
    pub origin: Origin,
    /// The first prompt's digest as the profile composed when ordered.
    pub prompt_sha256: String,
}

/// What is to replace the flow once it rests.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub enum ReplacementOrder {
    /// A Refresh.Address: the flow ends without a turn, and its successor is
    /// what any refresh starts.
    Refresh,
    /// A Launch or a Replace: its own profile is the successor.
    Launch(Box<OrderedLaunch>),
}

/// A replacement of `predecessor`, the flow the metaflow at `address` is
/// Awake as, waiting for that flow to rest.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub struct OrderedReplacement {
    pub address: Address,
    pub predecessor: String,
    pub order: ReplacementOrder,
}

impl EngineRecord for OrderedReplacement {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(FlowStore::address_record_key(&self.address))
    }
}

impl MemoryDefinition for OrderedReplacement {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(1)
    }

    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label(ORDERED_REPLACEMENT_SCHEMA)
    }
}

pub(super) trait DescribesOrderedReplacementTable {
    fn descriptor() -> TableDescriptor<OrderedReplacement>;
}

impl DescribesOrderedReplacementTable for OrderedReplacement {
    fn descriptor() -> TableDescriptor<OrderedReplacement> {
        TableDescriptor::new(
            ORDERED_REPLACEMENT_TABLE_NAME,
            FamilyName::new(ORDERED_REPLACEMENT_FAMILY),
            SchemaHash::for_label(ORDERED_REPLACEMENT_SCHEMA),
        )
    }
}

/// The launch request an ordered Launch answers by.
pub(crate) trait NamesOrderedLaunch {
    fn ordered_launch(&self) -> Option<&OrderedLaunch>;
}

impl NamesOrderedLaunch for OrderedReplacement {
    fn ordered_launch(&self) -> Option<&OrderedLaunch> {
        match &self.order {
            ReplacementOrder::Launch(launch) => Some(&**launch),
            ReplacementOrder::Refresh => None,
        }
    }
}

/// The attempt a waiting Launch answers as: LaunchPending, and
/// Observe.Launch until its replacement is under way. Nothing of it is
/// launched yet; its launch request is held by the order alone.
pub(crate) trait ShowsWaitingAttempt {
    fn waiting_attempt(&self) -> LaunchAttempt;
}

impl ShowsWaitingAttempt for OrderedLaunch {
    fn waiting_attempt(&self) -> LaunchAttempt {
        LaunchAttempt {
            launch_request_id: self.launch_profile.launch_request_id.clone(),
            launch_profile: self.launch_profile.clone(),
            prompt_sha256: signal_flow::PromptSha256(self.prompt_sha256.clone()),
            origin: self.origin.clone(),
            launch_attempt_phase: LaunchAttemptPhase::Reserved,
            native_launch_intent_option: None,
            native_launch_binding_option: None,
            registration_acknowledgement_option: None,
            prompt_delivery_intent_option: None,
            prompt_delivery_result_option: None,
        }
    }
}

pub(crate) trait OrdersReplacements {
    /// The replacement waiting at `address`, if any.
    fn ordered_replacement(
        &self,
        address: &Address,
    ) -> Result<Option<OrderedReplacement>, StoreError>;
    /// Every replacement waiting.
    fn ordered_replacements(&self) -> Result<Vec<OrderedReplacement>, StoreError>;
    /// The waiting Launch whose profile carries `launch_request_id`.
    fn ordered_launch_request(
        &self,
        launch_request_id: &str,
    ) -> Result<Option<OrderedReplacement>, StoreError>;
    /// Records the order, in its own write. A launch observer re-reads.
    fn order_replacement(&self, order: OrderedReplacement) -> Result<(), StoreError>;
    /// Retracts the order at `address`, if one is recorded, in its own
    /// write. A launch observer re-reads.
    fn forget_ordered_replacement(&self, address: &Address) -> Result<(), StoreError>;
}

impl OrdersReplacements for FlowStore {
    fn ordered_replacement(
        &self,
        address: &Address,
    ) -> Result<Option<OrderedReplacement>, StoreError> {
        let records = self
            .engine
            .match_records(QueryPlan::key(
                self.ordered_replacements,
                RecordKey::new(FlowStore::address_record_key(address)),
            ))?
            .records()
            .to_vec();
        match records.as_slice() {
            [] => Ok(None),
            [order] => Ok(Some(order.clone())),
            _ => Err(StoreError::StateInvariant),
        }
    }

    fn ordered_replacements(&self) -> Result<Vec<OrderedReplacement>, StoreError> {
        Ok(self
            .engine
            .match_records(QueryPlan::all(self.ordered_replacements))?
            .records()
            .to_vec())
    }

    fn ordered_launch_request(
        &self,
        launch_request_id: &str,
    ) -> Result<Option<OrderedReplacement>, StoreError> {
        Ok(self.ordered_replacements()?.into_iter().find(|order| {
            order.ordered_launch().is_some_and(|launch| {
                launch.launch_profile.launch_request_id.0 == launch_request_id
            })
        }))
    }

    fn order_replacement(&self, order: OrderedReplacement) -> Result<(), StoreError> {
        let commit: AtomicCommit = match self.ordered_replacement(&order.address)? {
            Some(_) => self
                .engine
                .begin_atomic_commit()
                .mutate(self.ordered_replacements, order),
            None => self
                .engine
                .begin_atomic_commit()
                .assert(self.ordered_replacements, order),
        };
        self.engine.commit_atomic(commit)?;
        self.launch_changes.announce();
        Ok(())
    }

    fn forget_ordered_replacement(&self, address: &Address) -> Result<(), StoreError> {
        if self.ordered_replacement(address)?.is_none() {
            return Ok(());
        }
        let commit = self.engine.begin_atomic_commit().retract(
            self.ordered_replacements,
            RecordKey::new(FlowStore::address_record_key(address)),
        );
        self.engine.commit_atomic(commit)?;
        self.launch_changes.announce();
        Ok(())
    }
}
