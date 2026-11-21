//! Where each queue delivery attempt belongs: one row per membership, keyed
//! by its family, its owner and the attempt, so a reader visits one owner's
//! attempts in a bounded key range and reads each attempt by its key. Every
//! attempt write passes through `commit_attempt`, which writes the attempt
//! and the memberships that change with it in the caller's one commit.
//!
//! The family is new in 0.60.3. A store written before holds attempts and no
//! memberships: at its first open the memberships of every attempt are built
//! in one finite pass, before anything reads them, and the pass is recorded
//! as built in the same commit, so every later open skips it.

use super::queue_delivery::{EndsQueueDeliveryPhase, QueueDeliveryAttempt, QueueDeliveryPhase};
use super::{FlowStore, ReadsFlowMemory, StoreError};
pub(crate) use crate::generated::memory::{
    AddressAttempt, AttemptMembership, AttemptMembershipBuilt, ByLogicalDelivery_Data,
    IndexedAttempts,
};
use meta_signal_flow::QueueEvidence;
use sema_engine::{
    AtomicCommit, Engine, EngineRecord, FamilyName, KeyRange, KeyRangeLimit, MemoryDefinition,
    MemoryVersion, QueryPlan, RecordKey, SchemaHash, TableDescriptor, TableName, TableReference,
};
use signal_flow::Address;
use std::collections::BTreeSet;

pub(super) const ATTEMPT_MEMBERSHIP_TABLE: &str = "flow_nexus_queue_attempt_memberships";
pub(super) const ATTEMPT_MEMBERSHIP_FAMILY: &str = "flow-nexus-queue-attempt-membership";
pub(super) const ATTEMPT_MEMBERSHIP_SCHEMA: &str = "flow-nexus-queue-attempt-membership-v1";
pub(super) const ATTEMPT_MEMBERSHIP_BUILT_TABLE: &str = "flow_nexus_queue_attempt_membership_built";
pub(super) const ATTEMPT_MEMBERSHIP_BUILT_FAMILY: &str =
    "flow-nexus-queue-attempt-membership-built";
pub(super) const ATTEMPT_MEMBERSHIP_BUILT_SCHEMA: &str =
    "flow-nexus-queue-attempt-membership-built-v1";
const BUILT_KEY: &str = "built";
/// How many attempts the pre-serving build decodes at once.
const BUILD_PAGE: usize = 256;
/// Above every character a key holds: the inclusive end of an owner's range.
const RANGE_END: char = '\u{10FFFF}';

/// The owner whose attempts one membership range holds.
pub(crate) enum MembershipOwner<'owner> {
    LogicalDelivery(&'owner str),
    Address(&'owner Address),
    UnsettledAt(&'owner Address),
    /// Every unsettled attempt, at every Address.
    Unsettled,
    UnfinishedAt(&'owner Address),
    /// Every unfinished attempt, at every Address.
    Unfinished,
}

/// The key range one owner's memberships fill.
pub(crate) trait Ranges {
    fn prefix(&self) -> String;
    fn range(&self) -> KeyRange;
}

impl Ranges for MembershipOwner<'_> {
    fn prefix(&self) -> String {
        let at = |address: &Address| <FlowStore as ReadsFlowMemory>::address_record_key(address);
        match self {
            Self::LogicalDelivery(id) => format!("logical:{}:{}:", id.len(), id),
            Self::Address(address) => format!("address:{}:", at(address)),
            Self::UnsettledAt(address) => format!("unsettled:{}:", at(address)),
            Self::Unsettled => "unsettled:".to_owned(),
            Self::UnfinishedAt(address) => format!("unfinished:{}:", at(address)),
            Self::Unfinished => "unfinished:".to_owned(),
        }
    }

    fn range(&self) -> KeyRange {
        let prefix = self.prefix();
        KeyRange::between(
            RecordKey::new(prefix.clone()),
            RecordKey::new(format!("{prefix}{RANGE_END}")),
        )
    }
}

/// The attempt a membership names and the owner it belongs to.
pub(crate) trait NamesItsAttempt {
    fn attempt_id(&self) -> &str;
    fn owner(&self) -> MembershipOwner<'_>;
}

impl NamesItsAttempt for AttemptMembership {
    fn attempt_id(&self) -> &str {
        match self {
            Self::ByLogicalDelivery(member) => &member.queue_attempt_id.0,
            Self::ByAddress(member) | Self::Unsettled(member) | Self::Unfinished(member) => {
                &member.queue_attempt_id.0
            }
        }
    }

    fn owner(&self) -> MembershipOwner<'_> {
        match self {
            Self::ByLogicalDelivery(member) => {
                MembershipOwner::LogicalDelivery(&member.logical_delivery_id.0)
            }
            Self::ByAddress(member) => MembershipOwner::Address(&member.address),
            Self::Unsettled(member) => MembershipOwner::UnsettledAt(&member.address),
            Self::Unfinished(member) => MembershipOwner::UnfinishedAt(&member.address),
        }
    }
}

impl EngineRecord for AttemptMembership {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(format!("{}{}", self.owner().prefix(), self.attempt_id()))
    }
}

impl MemoryDefinition for AttemptMembership {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(1)
    }

    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label(ATTEMPT_MEMBERSHIP_SCHEMA)
    }
}

impl EngineRecord for AttemptMembershipBuilt {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(BUILT_KEY)
    }
}

impl MemoryDefinition for AttemptMembershipBuilt {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(1)
    }

    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label(ATTEMPT_MEMBERSHIP_BUILT_SCHEMA)
    }
}

/// The memberships an attempt holds, read off the attempt alone.
pub(crate) trait BelongsToOwners {
    fn memberships(&self) -> Vec<AttemptMembership>;
}

impl BelongsToOwners for QueueDeliveryAttempt {
    fn memberships(&self) -> Vec<AttemptMembership> {
        let at_address = AddressAttempt {
            address: self.address.clone(),
            queue_attempt_id: self.queue_attempt_id.clone(),
        };
        let mut memberships = Vec::with_capacity(4);
        // A legacy attempt has no authoritative logical delivery.
        if let QueueEvidence::Logical(entry) = &self.queue_evidence {
            memberships.push(AttemptMembership::ByLogicalDelivery(
                ByLogicalDelivery_Data {
                    logical_delivery_id: entry.logical_delivery_id.clone(),
                    queue_attempt_id: self.queue_attempt_id.clone(),
                },
            ));
        }
        if !matches!(
            self.queue_delivery_phase,
            QueueDeliveryPhase::Settled(_) | QueueDeliveryPhase::Read { .. }
        ) {
            memberships.push(AttemptMembership::Unsettled(at_address.clone()));
        }
        if !self.queue_delivery_phase.is_finished() {
            memberships.push(AttemptMembership::Unfinished(at_address.clone()));
        }
        memberships.push(AttemptMembership::ByAddress(at_address));
        memberships
    }
}

pub(crate) struct AttemptMembershipTables {
    pub(super) memberships: TableReference<AttemptMembership>,
    pub(super) built: TableReference<AttemptMembershipBuilt>,
}

pub(crate) trait DeclaresAttemptMembershipTables: Sized {
    fn register(engine: &mut Engine) -> Result<Self, StoreError>;
}

impl DeclaresAttemptMembershipTables for AttemptMembershipTables {
    fn register(engine: &mut Engine) -> Result<Self, StoreError> {
        Ok(Self {
            memberships: engine.register_table(TableDescriptor::new(
                TableName::new(ATTEMPT_MEMBERSHIP_TABLE),
                FamilyName::new(ATTEMPT_MEMBERSHIP_FAMILY),
                AttemptMembership::schema_hash(),
            ))?,
            built: engine.register_table(TableDescriptor::new(
                TableName::new(ATTEMPT_MEMBERSHIP_BUILT_TABLE),
                FamilyName::new(ATTEMPT_MEMBERSHIP_BUILT_FAMILY),
                AttemptMembershipBuilt::schema_hash(),
            ))?,
        })
    }
}

pub(crate) trait KeepsAttemptMemberships {
    /// Writes `attempt`, asserted when new and mutated otherwise, and the
    /// memberships that change between the stored attempt and it, into
    /// `commit`.
    fn commit_attempt(
        &self,
        commit: AtomicCommit,
        attempt: QueueDeliveryAttempt,
    ) -> Result<AtomicCommit, StoreError>;
    /// The attempts of one owner, in attempt-id order, each read by its key.
    fn member_attempts(
        &self,
        owner: MembershipOwner<'_>,
    ) -> Result<Vec<QueueDeliveryAttempt>, StoreError>;
    /// The stored attempt of that id.
    fn stored_attempt(&self, attempt_id: &str) -> Result<Option<QueueDeliveryAttempt>, StoreError>;
    /// Builds the memberships of every stored attempt once, before anything
    /// reads them; a store already built is left as it is.
    fn build_attempt_memberships(&self) -> Result<(), StoreError>;
}

impl KeepsAttemptMemberships for FlowStore {
    fn commit_attempt(
        &self,
        commit: AtomicCommit,
        attempt: QueueDeliveryAttempt,
    ) -> Result<AtomicCommit, StoreError> {
        let previous = self.stored_attempt(&attempt.queue_attempt_id.0)?;
        let before: BTreeSet<String> = previous
            .iter()
            .flat_map(BelongsToOwners::memberships)
            .map(|membership| membership.record_key().to_owned_string())
            .collect();
        let after = attempt.memberships();
        let after_keys: BTreeSet<String> = after
            .iter()
            .map(|membership| membership.record_key().to_owned_string())
            .collect();
        let tables = &self.attempt_memberships;
        let mut commit = match previous {
            Some(_) => commit.mutate(self.queue_delivery.attempts, attempt),
            None => commit.assert(self.queue_delivery.attempts, attempt),
        };
        for gone in before.difference(&after_keys) {
            commit = commit.retract(tables.memberships, RecordKey::new(gone.clone()));
        }
        for membership in after {
            if !before.contains(&membership.record_key().to_owned_string()) {
                commit = commit.assert(tables.memberships, membership);
            }
        }
        Ok(commit)
    }

    fn member_attempts(
        &self,
        owner: MembershipOwner<'_>,
    ) -> Result<Vec<QueueDeliveryAttempt>, StoreError> {
        // The range holds one owner's rows alone: it is visited whole.
        let members = self
            .engine
            .match_first_in_key_range(QueryPlan::first_in_key_range(
                self.attempt_memberships.memberships,
                owner.range(),
                KeyRangeLimit::new(usize::MAX),
            ))?;
        let mut attempts = Vec::with_capacity(members.records().len());
        for member in members.records() {
            attempts.push(
                self.stored_attempt(member.attempt_id())?
                    .ok_or(StoreError::StateInvariant)?,
            );
        }
        // Keys of a whole family run Address by Address.
        attempts.sort_by(|left, right| left.queue_attempt_id.cmp(&right.queue_attempt_id));
        #[cfg(test)]
        assert_eq!(
            attempts,
            self.whole_read_of(&owner)?,
            "the membership range answers as the whole read did"
        );
        Ok(attempts)
    }

    fn stored_attempt(&self, attempt_id: &str) -> Result<Option<QueueDeliveryAttempt>, StoreError> {
        #[cfg(test)]
        self.attempt_reads
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let rows = self.engine.match_records(QueryPlan::key(
            self.queue_delivery.attempts,
            RecordKey::new(attempt_id),
        ))?;
        match rows.records() {
            [] => Ok(None),
            [attempt] => Ok(Some(attempt.clone())),
            _ => Err(StoreError::StateInvariant),
        }
    }

    fn build_attempt_memberships(&self) -> Result<(), StoreError> {
        let tables = &self.attempt_memberships;
        if !self
            .engine
            .match_records(QueryPlan::key(tables.built, RecordKey::new(BUILT_KEY)))?
            .records()
            .is_empty()
        {
            return Ok(());
        }
        let mut commit = self.engine.begin_atomic_commit();
        let mut indexed = 0_i64;
        let mut start: Option<RecordKey> = None;
        loop {
            let page = self
                .engine
                .match_first_in_key_range(QueryPlan::first_in_key_range(
                    self.queue_delivery.attempts,
                    KeyRange::new(start.clone(), None),
                    KeyRangeLimit::new(BUILD_PAGE),
                ))?;
            let attempts = page.records();
            for attempt in attempts {
                for membership in attempt.memberships() {
                    commit = commit.assert(tables.memberships, membership);
                }
                indexed += 1;
            }
            match attempts.last() {
                Some(last) if attempts.len() == BUILD_PAGE => {
                    // The next page starts just past the last key read.
                    start = Some(RecordKey::new(format!(
                        "{}\u{0}",
                        last.record_key().to_owned_string()
                    )));
                }
                _ => break,
            }
        }
        let commit = commit.assert(
            tables.built,
            AttemptMembershipBuilt {
                indexed_attempts: IndexedAttempts(indexed),
            },
        );
        self.engine.commit_atomic(commit)?;
        Ok(())
    }
}

#[cfg(test)]
impl FlowStore {
    /// What the whole read answered for an owner before memberships: every
    /// attempt filtered, in attempt-id order. The oracle every membership
    /// read is held to in tests.
    fn whole_read_of(
        &self,
        owner: &MembershipOwner<'_>,
    ) -> Result<Vec<QueueDeliveryAttempt>, StoreError> {
        let unsettled = |attempt: &QueueDeliveryAttempt| {
            !matches!(
                attempt.queue_delivery_phase,
                QueueDeliveryPhase::Settled(_) | QueueDeliveryPhase::Read { .. }
            )
        };
        let mut attempts: Vec<QueueDeliveryAttempt> = self
            .engine
            .match_records(QueryPlan::all(self.queue_delivery.attempts))?
            .records()
            .iter()
            .filter(|attempt| match owner {
                MembershipOwner::LogicalDelivery(id) => matches!(
                    &attempt.queue_evidence,
                    QueueEvidence::Logical(entry) if entry.logical_delivery_id.0 == *id
                ),
                MembershipOwner::Address(address) => attempt.address == **address,
                MembershipOwner::UnsettledAt(address) => {
                    attempt.address == **address && unsettled(attempt)
                }
                MembershipOwner::Unsettled => unsettled(attempt),
                MembershipOwner::UnfinishedAt(address) => {
                    attempt.address == **address && !attempt.queue_delivery_phase.is_finished()
                }
                MembershipOwner::Unfinished => !attempt.queue_delivery_phase.is_finished(),
            })
            .cloned()
            .collect();
        attempts.sort_by(|left, right| left.queue_attempt_id.cmp(&right.queue_attempt_id));
        Ok(attempts)
    }

    /// Every membership row, by key.
    pub(crate) fn membership_keys(&self) -> BTreeSet<String> {
        self.engine
            .match_records(QueryPlan::all(self.attempt_memberships.memberships))
            .unwrap()
            .records()
            .iter()
            .map(|membership| membership.record_key().to_owned_string())
            .collect()
    }

    /// The membership keys the stored attempts hold.
    pub(crate) fn expected_membership_keys(&self) -> BTreeSet<String> {
        self.engine
            .match_records(QueryPlan::all(self.queue_delivery.attempts))
            .unwrap()
            .records()
            .iter()
            .flat_map(BelongsToOwners::memberships)
            .map(|membership| membership.record_key().to_owned_string())
            .collect()
    }

    pub(crate) fn attempt_reads(&self) -> usize {
        self.attempt_reads
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    pub(crate) fn membership_built(&self) -> Option<AttemptMembershipBuilt> {
        self.engine
            .match_records(QueryPlan::key(
                self.attempt_memberships.built,
                RecordKey::new(BUILT_KEY),
            ))
            .unwrap()
            .records()
            .first()
            .cloned()
    }

    /// Removes every membership and the built record, leaving the store as
    /// a store written before 0.60.3 holds its attempts.
    pub(crate) fn forget_memberships(&self) {
        let mut commit = self.engine.begin_atomic_commit();
        for key in self.membership_keys() {
            commit = commit.retract(self.attempt_memberships.memberships, RecordKey::new(key));
        }
        if self.membership_built().is_some() {
            commit = commit.retract(self.attempt_memberships.built, RecordKey::new(BUILT_KEY));
        }
        self.engine.commit_atomic(commit).unwrap();
    }
}
