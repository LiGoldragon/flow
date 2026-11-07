//! Exact 0.53.2 Memory codecs and the one atomic request-id evolution.
//!
//! The predecessor records use source-frozen contract crates generated from
//! Flow 85bf0f1, signal-flow 3596923 and meta-signal-flow b36597a.  They are
//! data only: no predecessor behavior is linked into the Nexus.

use super::StoreError;
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use sema_engine::{
    Engine, EngineRecord, FamilyName, MemoryDefinition, MemoryVersion, RecordKey, SchemaHash,
    TableDescriptor, TableName, TableReference,
};

use flow_ethos::{Address, LogicalDeliveryId, RequestId};
use flow_ethos_before_request_ids as old_flow;
use meta_signal_flow_before_request_ids as old_meta;
use signal_flow_before_request_ids as old_signal;

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct PriorCurrentLogicalDelivery {
    pub(crate) snapshot: old_signal::DeliverySnapshot,
}
impl EngineRecord for PriorCurrentLogicalDelivery {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.snapshot.logical_delivery_id.clone())
    }
}
impl MemoryDefinition for PriorCurrentLogicalDelivery {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(6)
    }
    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label("flow-nexus-logical-delivery-current-v6")
    }
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct PriorLogicalDeliveryEvent {
    pub(crate) snapshot: old_signal::DeliverySnapshot,
}
impl EngineRecord for PriorLogicalDeliveryEvent {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(format!("{:020}", self.snapshot.delivery_cursor))
    }
}
impl MemoryDefinition for PriorLogicalDeliveryEvent {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(6)
    }
    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label("flow-nexus-logical-delivery-event-v6")
    }
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct PriorStoredMetaflow {
    pub(crate) metaflow: old_flow::Metaflow,
}
impl EngineRecord for PriorStoredMetaflow {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(address_key(&self.metaflow.address))
    }
}
impl MemoryDefinition for PriorStoredMetaflow {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(7)
    }
    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label("flow-nexus-metaflow-v7")
    }
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct PriorAddressHold {
    pub(crate) flow_id: String,
    pub(crate) address: old_flow::Address,
    pub(crate) found: Option<old_flow::Metaflow>,
}
impl EngineRecord for PriorAddressHold {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.flow_id.clone())
    }
}
impl MemoryDefinition for PriorAddressHold {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(6)
    }
    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label("flow-nexus-address-hold-v6")
    }
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) enum PriorRequestState {
    Queued,
    Settled,
    Unsent,
    Parked,
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct PriorStoredDeliveryRequest {
    pub(crate) request_id: String,
    pub(crate) address: old_flow::Address,
    pub(crate) queue_evidence: old_meta::QueueEvidence,
    pub(crate) state: PriorRequestState,
}
impl EngineRecord for PriorStoredDeliveryRequest {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.request_id.clone())
    }
}
impl MemoryDefinition for PriorStoredDeliveryRequest {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(7)
    }
    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label("flow-nexus-queue-delivery-request-v7")
    }
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct PriorNativeDeliveryReceipt {
    pub(crate) native_request_id: u64,
    pub(crate) thread_id: String,
    pub(crate) turn_id: String,
    pub(crate) native_receipt: String,
}
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct PriorNativeTranscriptCursor {
    pub(crate) device: u64,
    pub(crate) inode: u64,
    pub(crate) offset: u64,
}
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) enum PriorNativeHistoryAnchor {
    ClaudeTranscript(PriorNativeTranscriptCursor),
    CodexTurns(Option<String>),
}
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct PriorAbsenceRecord {
    pub(crate) proof: old_meta::AbsenceProof,
    pub(crate) observation: String,
}
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) enum PriorQueueDeliveryPhase {
    Prepared,
    Uncertain(Option<old_meta::Undecided>),
    ReceiptRecorded(PriorNativeDeliveryReceipt),
    Settled(PriorNativeDeliveryReceipt),
    Absent(PriorAbsenceRecord),
    Resolved(old_meta::QueueResolutionRecord),
}
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct PriorQueueDeliveryAttempt {
    pub(crate) request_id: String,
    pub(crate) attempt_id: String,
    pub(crate) native_request_id: u64,
    pub(crate) flow_id: String,
    pub(crate) thread_id: String,
    pub(crate) harness_kind: old_flow::HarnessKind,
    pub(crate) history_anchor: PriorNativeHistoryAnchor,
    pub(crate) address: old_flow::Address,
    pub(crate) queue_evidence: old_meta::QueueEvidence,
    pub(crate) phase: PriorQueueDeliveryPhase,
}
impl EngineRecord for PriorQueueDeliveryAttempt {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.attempt_id.clone())
    }
}
impl MemoryDefinition for PriorQueueDeliveryAttempt {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(8)
    }
    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label("flow-nexus-queue-delivery-attempt-v8")
    }
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct PriorStoredSendOutcome {
    pub(crate) record: old_meta::SendOutcomeRecord,
}
impl EngineRecord for PriorStoredSendOutcome {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.record.send_outcome_id.clone())
    }
}
impl MemoryDefinition for PriorStoredSendOutcome {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(7)
    }
    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label("flow-nexus-send-outcome-v7")
    }
}

pub(crate) type PriorSendCall = crate::generated::send_tool_before_request_ids::SendCall;
impl EngineRecord for PriorSendCall {
    fn record_key(&self) -> RecordKey {
        let key = &self.tool_call_key;
        RecordKey::new(format!(
            "{}:{}{}:{}{}:{}",
            key.thread_id.len(),
            key.thread_id,
            key.turn_id.len(),
            key.turn_id,
            key.call_id.len(),
            key.call_id
        ))
    }
}
impl MemoryDefinition for PriorSendCall {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(7)
    }
    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label("flow-nexus-send-call-v7")
    }
}

/// The exact pre-id delivery record moved out of all live indexes by upgrade.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct PriorDelivery {
    pub(crate) logical_delivery_id: String,
    pub(crate) current: PriorCurrentLogicalDelivery,
    pub(crate) events: Vec<PriorLogicalDeliveryEvent>,
    pub(crate) request: PriorStoredDeliveryRequest,
    pub(crate) attempts: Vec<PriorQueueDeliveryAttempt>,
    pub(crate) outcomes: Vec<PriorStoredSendOutcome>,
}
impl EngineRecord for PriorDelivery {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.logical_delivery_id.clone())
    }
}
impl MemoryDefinition for PriorDelivery {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(1)
    }
    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label("flow-nexus-delivery-before-ids-v1")
    }
}

pub(crate) const PRIOR_DELIVERY_TABLE_NAME: TableName =
    TableName::new("flow_nexus_deliveries_before_ids");
pub(crate) fn prior_delivery_descriptor() -> TableDescriptor<PriorDelivery> {
    TableDescriptor::new(
        PRIOR_DELIVERY_TABLE_NAME,
        FamilyName::new("flow-nexus-delivery-before-ids"),
        PriorDelivery::schema_hash(),
    )
}

fn address_key(address: &old_flow::Address) -> String {
    format!(
        "{}:{}:{}:{}",
        aspect_key(&address.aspect),
        address.topic.len(),
        address.topic,
        model_key(&address.model)
    )
}

fn aspect_key(aspect: &old_flow::Aspect) -> &'static str {
    match aspect {
        old_flow::Aspect::Psyche => "Psyche",
        old_flow::Aspect::Mind => "Mind",
        old_flow::Aspect::Field => "Field",
    }
}

fn model_key(model: &old_flow::Model) -> &'static str {
    match model {
        old_flow::Model::Fable => "Fable",
        old_flow::Model::Opus => "Opus",
        old_flow::Model::Sonnet => "Sonnet",
        old_flow::Model::Haiku => "Haiku",
        old_flow::Model::Astra => "Astra",
        old_flow::Model::Sol => "Sol",
        old_flow::Model::Luna => "Luna",
        old_flow::Model::Terra => "Terra",
    }
}

/// A bounded part of the post-upgrade notice for tasks that were already open.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct TaskAnnouncement {
    pub(crate) recipient: Address,
    pub(crate) part: u64,
    pub(crate) tasks: Vec<LogicalDeliveryId>,
    pub(crate) tries: u64,
    pub(crate) status: TaskAnnouncementStatus,
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) enum TaskAnnouncementStatus {
    Pending(Option<signal_flow::Refusal>),
    Admitted(String),
    Delivered(String),
    Failed(String),
    Closed,
}

impl EngineRecord for TaskAnnouncement {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(part_key(&self.recipient, self.part))
    }
}
impl MemoryDefinition for TaskAnnouncement {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(1)
    }
    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label("flow-nexus-task-announcement-v1")
    }
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct AssignedAnswer {
    pub(crate) answer: LogicalDeliveryId,
    pub(crate) order: LogicalDeliveryId,
    pub(crate) order_id: Option<RequestId>,
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct OpenOrder {
    pub(crate) order: LogicalDeliveryId,
    pub(crate) order_id: RequestId,
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct AssignmentDecision {
    pub(crate) recipient: Address,
    pub(crate) assigned: Vec<AssignedAnswer>,
    pub(crate) open: Vec<OpenOrder>,
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct UpgradeAssignment {
    pub(crate) sender: Address,
    pub(crate) part: u64,
    pub(crate) decisions: Vec<AssignmentDecision>,
    pub(crate) tries: u64,
    pub(crate) status: TaskAnnouncementStatus,
}

impl EngineRecord for UpgradeAssignment {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(part_key(&self.sender, self.part))
    }
}
impl MemoryDefinition for UpgradeAssignment {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(1)
    }
    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label("flow-nexus-upgrade-assignment-v1")
    }
}

pub(crate) const TASK_ANNOUNCEMENT_TABLE_NAME: TableName =
    TableName::new("flow_nexus_task_announcements");
pub(crate) const UPGRADE_ASSIGNMENT_TABLE_NAME: TableName =
    TableName::new("flow_nexus_upgrade_assignments");

pub(crate) fn task_announcement_descriptor() -> TableDescriptor<TaskAnnouncement> {
    TableDescriptor::new(
        TASK_ANNOUNCEMENT_TABLE_NAME,
        FamilyName::new("flow-nexus-task-announcement"),
        TaskAnnouncement::schema_hash(),
    )
}

pub(crate) fn upgrade_assignment_descriptor() -> TableDescriptor<UpgradeAssignment> {
    TableDescriptor::new(
        UPGRADE_ASSIGNMENT_TABLE_NAME,
        FamilyName::new("flow-nexus-upgrade-assignment"),
        UpgradeAssignment::schema_hash(),
    )
}

fn part_key(address: &Address, part: u64) -> String {
    let address = current_address_key(address);
    format!("{}:{address}:{part:020}", address.len())
}

fn current_address_key(address: &Address) -> String {
    format!(
        "{}:{}:{}:{}",
        match address.aspect {
            flow_ethos::Aspect::Psyche => "Psyche",
            flow_ethos::Aspect::Mind => "Mind",
            flow_ethos::Aspect::Field => "Field",
        },
        address.topic.len(),
        address.topic,
        address.model.name()
    )
}

pub(crate) struct RequestIdUpgradeDescriptors {
    pub(crate) prior_deliveries: TableDescriptor<PriorDelivery>,
    pub(crate) task_announcements: TableDescriptor<TaskAnnouncement>,
    pub(crate) upgrade_assignments: TableDescriptor<UpgradeAssignment>,
}

pub(crate) struct RequestIdUpgradeTables {
    pub(crate) prior_deliveries: TableReference<PriorDelivery>,
    pub(crate) task_announcements: TableReference<TaskAnnouncement>,
    pub(crate) upgrade_assignments: TableReference<UpgradeAssignment>,
}

pub(crate) trait DeclaresRequestIdUpgradeTables: Sized {
    fn descriptors() -> RequestIdUpgradeDescriptors;
    fn register(
        engine: &mut Engine,
        descriptors: RequestIdUpgradeDescriptors,
    ) -> Result<Self, StoreError>;
}

impl DeclaresRequestIdUpgradeTables for RequestIdUpgradeTables {
    fn descriptors() -> RequestIdUpgradeDescriptors {
        RequestIdUpgradeDescriptors {
            prior_deliveries: prior_delivery_descriptor(),
            task_announcements: task_announcement_descriptor(),
            upgrade_assignments: upgrade_assignment_descriptor(),
        }
    }

    fn register(
        engine: &mut Engine,
        descriptors: RequestIdUpgradeDescriptors,
    ) -> Result<Self, StoreError> {
        Ok(Self {
            prior_deliveries: engine.register_table(descriptors.prior_deliveries)?,
            task_announcements: engine.register_table(descriptors.task_announcements)?,
            upgrade_assignments: engine.register_table(descriptors.upgrade_assignments)?,
        })
    }
}
