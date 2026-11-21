//! The queue attempt 0.60.4 stored before an attempt could be Abandoned.

use super::queue_delivery::{DeclaresQueueDeliveryTables, QueueDeliveryTables};
use super::{FlowStore, StoreError};
use crate::generated::memory::{
    AbsenceRecord, NativeDeliveryReceipt, NativeHistoryAnchor, NativeRequestId,
    QueueDeliveryAttempt, QueueDeliveryPhase, Read_Data, ThreadId,
};
use meta_signal_flow::{
    QueueAttemptId, QueueEvidence, QueueRequestId, QueueResolutionRecord, Undecided,
};
use rkyv::{Archive, Deserialize, Serialize};
use sema_engine::{
    Decision, Engine, EngineRecord, MemoryDefinition, MemoryUpgrades, MemoryVersion, RecordKey,
    SchemaHash, UpgradeFrom, UpgradeOutcome,
};
use signal_flow::{Address, FlowId, HarnessKind};

pub(super) const TABLE: &str = "flow_nexus_queue_delivery_attempts";
pub(super) const FAMILY: &str = "flow-nexus-queue-delivery-attempt";
pub(super) const SCHEMA_V10: &str = "flow-nexus-queue-delivery-attempt-v10";
pub(super) const SCHEMA_V11: &str = "flow-nexus-queue-delivery-attempt-v11";

#[derive(Archive, Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub(crate) enum QueueDeliveryPhaseV10 {
    Prepared,
    Uncertain(Option<Undecided>),
    ReceiptRecorded(NativeDeliveryReceipt),
    Settled(NativeDeliveryReceipt),
    Read(Read_Data),
    Absent(AbsenceRecord),
    Resolved(QueueResolutionRecord),
}

impl From<QueueDeliveryPhaseV10> for QueueDeliveryPhase {
    fn from(before: QueueDeliveryPhaseV10) -> Self {
        match before {
            QueueDeliveryPhaseV10::Prepared => Self::Prepared,
            QueueDeliveryPhaseV10::Uncertain(why) => Self::Uncertain(why),
            QueueDeliveryPhaseV10::ReceiptRecorded(receipt) => Self::ReceiptRecorded(receipt),
            QueueDeliveryPhaseV10::Settled(receipt) => Self::Settled(receipt),
            QueueDeliveryPhaseV10::Read(read) => Self::Read(read),
            QueueDeliveryPhaseV10::Absent(absence) => Self::Absent(absence),
            QueueDeliveryPhaseV10::Resolved(record) => Self::Resolved(record),
        }
    }
}

#[derive(Archive, Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub(crate) struct QueueDeliveryAttemptV10 {
    pub(crate) queue_request_id: QueueRequestId,
    pub(crate) queue_attempt_id: QueueAttemptId,
    pub(crate) native_request_id: NativeRequestId,
    pub(crate) flow_id: FlowId,
    pub(crate) thread_id: ThreadId,
    pub(crate) harness_kind: HarnessKind,
    pub(crate) native_history_anchor: NativeHistoryAnchor,
    pub(crate) address: Address,
    pub(crate) queue_evidence: QueueEvidence,
    pub(crate) queue_delivery_phase: QueueDeliveryPhaseV10,
}

impl EngineRecord for QueueDeliveryAttemptV10 {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.queue_attempt_id.0.clone())
    }
}

impl MemoryDefinition for QueueDeliveryAttemptV10 {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(10)
    }

    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label(SCHEMA_V10)
    }
}

impl UpgradeFrom<QueueDeliveryAttemptV10> for QueueDeliveryAttempt {
    type Refusal = std::convert::Infallible;

    fn upgrade_from(before: QueueDeliveryAttemptV10) -> Decision<Self, Self::Refusal> {
        Decision::Ready(Self {
            queue_request_id: before.queue_request_id,
            queue_attempt_id: before.queue_attempt_id,
            native_request_id: before.native_request_id,
            flow_id: before.flow_id,
            thread_id: before.thread_id,
            harness_kind: before.harness_kind,
            native_history_anchor: before.native_history_anchor,
            address: before.address,
            queue_evidence: before.queue_evidence,
            queue_delivery_phase: before.queue_delivery_phase.into(),
        })
    }
}

pub(super) trait UpgradesDeliveryAttempts {
    fn upgrade_delivery_attempts(engine: &mut Engine) -> Result<(), StoreError>;
}

impl UpgradesDeliveryAttempts for FlowStore {
    fn upgrade_delivery_attempts(engine: &mut Engine) -> Result<(), StoreError> {
        let before = engine.list_tables().into_iter().any(|registration| {
            let identity = registration.identity();
            identity.table_name() == TABLE
                && identity.family().as_str() == FAMILY
                && identity.schema_hash() == QueueDeliveryAttemptV10::schema_hash()
        });
        if !before {
            return Ok(());
        }
        let (_, _, descriptor) = QueueDeliveryTables::descriptors();
        match engine.upgrade_table::<QueueDeliveryAttemptV10, QueueDeliveryAttempt>(descriptor) {
            UpgradeOutcome::Upgraded(_) | UpgradeOutcome::AlreadyCurrent(_) => Ok(()),
            UpgradeOutcome::Refused(refusal) => match *refusal.reason() {},
            UpgradeOutcome::Failed(failure) => {
                Err(StoreError::DeliveryAttemptUpgrade(format!("{failure:?}")))
            }
        }
    }
}
