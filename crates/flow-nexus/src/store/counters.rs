//! The id counters: the next logical delivery, queue attempt and send
//! outcome a running store allocates. They are declared only while the
//! store is empty — no logical delivery, no queue attempt and no send
//! outcome recorded — so a new store continues the ids of the store it
//! replaces, and never below a counter as it stands.

use super::logical_delivery::LogicalDeliveryIds;
use super::queue_delivery::QueueAttemptIds;
use super::send_outcome::SendOutcomeIds;
use super::*;
use sema_engine::AtomicCommit;

/// One id family's counter as it stands: the next id it allocates, and
/// whether its counter row is written yet.
pub(crate) struct IdCounter {
    pub(crate) next: i64,
    pub(crate) written: bool,
}

/// One family of ids a running store allocates.
pub(crate) trait CountsIds {
    /// Holds the family's writes, where it has a gate of its own, until the
    /// guard drops.
    fn hold_writes(&self) -> Option<std::sync::MutexGuard<'_, ()>>;
    fn id_counter(&self) -> Result<IdCounter, StoreError>;
    /// Whether any id of the family has been recorded.
    fn ids_recorded(&self) -> Result<bool, StoreError>;
    /// Adds to `commit` the counter naming `next` as the family's next id.
    fn declare_next_id(
        &self,
        commit: AtomicCommit,
        counter: &IdCounter,
        next: i64,
    ) -> Result<AtomicCommit, StoreError>;
}

pub(crate) trait ConfiguresCounters {
    /// The next id of each family, as the store stands.
    fn counters(&self) -> Result<ConfigureCounters, StoreError>;
    /// Declares the next id of each family on an empty store, in one commit.
    fn configure_counters(
        &self,
        declared: ConfigureCounters,
    ) -> Result<ConfigureOutcome, StoreError>;
}

impl ConfiguresCounters for FlowStore {
    fn counters(&self) -> Result<ConfigureCounters, StoreError> {
        Ok(ConfigureCounters {
            logical_delivery: LogicalDeliveryIds(self).id_counter()?.next,
            queue_attempt: QueueAttemptIds(self).id_counter()?.next,
            send_outcome: SendOutcomeIds(self).id_counter()?.next,
        })
    }

    fn configure_counters(
        &self,
        declared: ConfigureCounters,
    ) -> Result<ConfigureOutcome, StoreError> {
        let logical = LogicalDeliveryIds(self);
        let queue = QueueAttemptIds(self);
        let sent = SendOutcomeIds(self);
        let _logical_writes = logical.hold_writes();
        let _queue_writes = queue.hold_writes();
        let _sent_writes = sent.hold_writes();
        if logical.ids_recorded()? || queue.ids_recorded()? || sent.ids_recorded()? {
            return Err(StoreError::CountersRefused(CountersRefusal::NotEmpty));
        }
        let logical_counter = logical.id_counter()?;
        let queue_counter = queue.id_counter()?;
        let sent_counter = sent.id_counter()?;
        let standing = ConfigureCounters {
            logical_delivery: logical_counter.next,
            queue_attempt: queue_counter.next,
            send_outcome: sent_counter.next,
        };
        if declared.logical_delivery < standing.logical_delivery
            || declared.queue_attempt < standing.queue_attempt
            || declared.send_outcome < standing.send_outcome
        {
            return Err(StoreError::CountersRefused(CountersRefusal::Lowered(
                standing,
            )));
        }
        if declared == standing {
            return Ok(ConfigureOutcome::Unchanged);
        }
        let commit = self.engine.begin_atomic_commit();
        let commit =
            logical.declare_next_id(commit, &logical_counter, declared.logical_delivery)?;
        let commit = queue.declare_next_id(commit, &queue_counter, declared.queue_attempt)?;
        let commit = sent.declare_next_id(commit, &sent_counter, declared.send_outcome)?;
        self.engine.commit_atomic(commit)?;
        Ok(ConfigureOutcome::Replaced)
    }
}
