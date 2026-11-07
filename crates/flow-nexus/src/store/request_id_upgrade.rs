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
        current_model_key(&address.model)
    )
}

fn current_model_key(model: &flow_ethos::Model) -> &'static str {
    match model {
        flow_ethos::Model::Fable => "Fable",
        flow_ethos::Model::Opus => "Opus",
        flow_ethos::Model::Sonnet => "Sonnet",
        flow_ethos::Model::Haiku => "Haiku",
        flow_ethos::Model::Astra => "Astra",
        flow_ethos::Model::Sol => "Sol",
        flow_ethos::Model::Luna => "Luna",
        flow_ethos::Model::Terra => "Terra",
    }
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

macro_rules! recode_unchanged {
    ($value:expr, $current:ty) => {{
        let bytes = rkyv::to_bytes::<rkyv::rancor::Error>($value)
            .map_err(|_| StoreError::StateInvariant)?;
        rkyv::from_bytes::<$current, rkyv::rancor::Error>(&bytes)
            .map_err(|_| StoreError::StateInvariant)
    }};
}

pub(crate) fn current_address(value: &old_flow::Address) -> Result<Address, StoreError> {
    recode_unchanged!(value, Address)
}

pub(crate) fn current_sent_by(value: &old_flow::SentBy) -> Result<flow_ethos::SentBy, StoreError> {
    recode_unchanged!(value, flow_ethos::SentBy)
}

pub(crate) fn current_message(
    value: &old_flow::Message,
) -> Result<flow_ethos::Message, StoreError> {
    Ok(match value {
        old_flow::Message::HardAbrupt(request) => {
            flow_ethos::Message::HardAbrupt(current_request(request)?)
        }
        old_flow::Message::MiddleAbrupt(request) => {
            flow_ethos::Message::MiddleAbrupt(current_request(request)?)
        }
        old_flow::Message::Soft(request) => flow_ethos::Message::Soft(current_request(request)?),
    })
}

pub(crate) fn current_request(
    value: &old_flow::Request,
) -> Result<flow_ethos::Request, StoreError> {
    Ok(match value {
        old_flow::Request::Order(words) => flow_ethos::Request::Order(words.clone()),
        old_flow::Request::Question(words) => flow_ethos::Request::Question(words.clone()),
        old_flow::Request::Psyche(said) => {
            flow_ethos::Request::Psyche(recode_unchanged!(said, flow_ethos::Said)?)
        }
        old_flow::Request::Psyches(saids) => flow_ethos::Request::Psyches(
            saids
                .iter()
                .map(|said| recode_unchanged!(said, flow_ethos::Said))
                .collect::<Result<Vec<_>, _>>()?,
        ),
        old_flow::Request::Result(result) => flow_ethos::Request::Result(match result {
            old_flow::ResultPayload::Answered(answered) => {
                flow_ethos::ResultPayload::Historical(answered.string.clone())
            }
            old_flow::ResultPayload::Historical(words) => {
                flow_ethos::ResultPayload::Historical(words.clone())
            }
        }),
        old_flow::Request::Notice(words) => flow_ethos::Request::Notice(words.clone()),
        old_flow::Request::Task(task) => {
            flow_ethos::Request::Task(recode_unchanged!(task, flow_ethos::AnswerableTask)?)
        }
        old_flow::Request::Done(done) => flow_ethos::Request::Done(match done {
            old_flow::DonePayload::Answered(answered) => {
                flow_ethos::DonePayload::Historical(flow_ethos::Historical_Data {
                    logical_delivery_id: answered.logical_delivery_id.clone(),
                    string: answered.string.clone(),
                })
            }
            old_flow::DonePayload::Historical(historical) => flow_ethos::DonePayload::Historical(
                recode_unchanged!(historical, flow_ethos::Historical_Data)?,
            ),
        }),
    })
}

pub(crate) fn current_queue_entry(
    value: &old_flow::QueueEntry,
    request_id: RequestId,
) -> Result<flow_ethos::QueueEntry, StoreError> {
    Ok(flow_ethos::QueueEntry {
        logical_delivery_id: value.logical_delivery_id.clone(),
        request_id,
        sent_by: current_sent_by(&value.sent_by)?,
        delivery: recode_unchanged!(&value.delivery, flow_ethos::Delivery)?,
        message: current_message(&value.message)?,
    })
}

pub(crate) fn current_metaflow(
    value: &old_flow::Metaflow,
    request_ids: &std::collections::BTreeMap<String, RequestId>,
) -> Result<flow_ethos::Metaflow, StoreError> {
    Ok(flow_ethos::Metaflow {
        address: current_address(&value.address)?,
        state: recode_unchanged!(&value.state, flow_ethos::State)?,
        past: value.past.clone(),
        queue: value
            .queue
            .iter()
            .map(|entry| {
                let request_id = request_ids
                    .get(&entry.logical_delivery_id)
                    .cloned()
                    .ok_or(StoreError::StateInvariant)?;
                current_queue_entry(entry, request_id)
            })
            .collect::<Result<Vec<_>, _>>()?,
    })
}

pub(crate) fn current_refusal(
    value: &old_signal::Refusal,
) -> Result<signal_flow::Refusal, StoreError> {
    Ok(match value {
        old_signal::Refusal::Locked => signal_flow::Refusal::Locked,
        old_signal::Refusal::Held(lock) => {
            signal_flow::Refusal::Held(recode_unchanged!(lock, flow_ethos::Lock)?)
        }
        old_signal::Refusal::Awake(flow_id) => signal_flow::Refusal::Awake(flow_id.clone()),
        old_signal::Refusal::Asleep => signal_flow::Refusal::Asleep,
        old_signal::Refusal::Lapsed => signal_flow::Refusal::Lapsed,
        old_signal::Refusal::Unknown(unknown) => signal_flow::Refusal::Unknown(match unknown {
            old_signal::Unknown_Data::Address(address) => {
                signal_flow::Unknown_Data::Address(current_address(address)?)
            }
            old_signal::Unknown_Data::Lock(lock) => {
                signal_flow::Unknown_Data::Lock(recode_unchanged!(lock, flow_ethos::Lock)?)
            }
            old_signal::Unknown_Data::FlowId(flow_id) => {
                signal_flow::Unknown_Data::FlowId(flow_id.clone())
            }
            old_signal::Unknown_Data::Key(key) => {
                signal_flow::Unknown_Data::Key(recode_unchanged!(key, flow_ethos::Key)?)
            }
        }),
        old_signal::Refusal::NotConfigured => signal_flow::Refusal::NotConfigured,
        old_signal::Refusal::NotMessage => signal_flow::Refusal::NotMessage,
        old_signal::Refusal::Taken(address) => {
            signal_flow::Refusal::Taken(current_address(address)?)
        }
        old_signal::Refusal::Unidentified(evidence) => signal_flow::Refusal::Unidentified(
            recode_unchanged!(evidence, signal_flow::SeatEvidence)?,
        ),
        old_signal::Refusal::Store(words) => signal_flow::Refusal::Store(words.clone()),
        old_signal::Refusal::NotSendServer(process) => {
            signal_flow::Refusal::NotSendServer(recode_unchanged!(process, flow_ethos::Process)?)
        }
        old_signal::Refusal::Oversized(size) => {
            signal_flow::Refusal::Oversized(recode_unchanged!(size, signal_flow::SendPayloadSize)?)
        }
        old_signal::Refusal::Direct(words) => signal_flow::Refusal::Direct(words.clone()),
        old_signal::Refusal::TopicRefused(topic) => {
            signal_flow::Refusal::TopicRefused(topic.clone())
        }
        old_signal::Refusal::NoOpenTask(_)
        | old_signal::Refusal::Unresolved
        | old_signal::Refusal::Ambiguous => signal_flow::Refusal::Unresolved,
        old_signal::Refusal::MinutesRefused(minutes) => signal_flow::Refusal::MinutesRefused(
            recode_unchanged!(minutes, signal_flow::MinutesRefusal)?,
        ),
    })
}

pub(crate) fn current_delivery_state(
    value: &old_signal::DeliveryState,
) -> Result<signal_flow::DeliveryState, StoreError> {
    Ok(match value {
        old_signal::DeliveryState::Queued => signal_flow::DeliveryState::Queued,
        old_signal::DeliveryState::Attempting => signal_flow::DeliveryState::Attempting,
        old_signal::DeliveryState::Retryable(cause) => signal_flow::DeliveryState::Retryable(
            recode_unchanged!(cause, signal_flow::RetryCause)?,
        ),
        old_signal::DeliveryState::Uncertain => signal_flow::DeliveryState::Uncertain,
        old_signal::DeliveryState::Delivered(receipt) => signal_flow::DeliveryState::Delivered(
            recode_unchanged!(receipt, signal_flow::NativeReceipt)?,
        ),
        old_signal::DeliveryState::Failed(refusal) => {
            signal_flow::DeliveryState::Failed(current_refusal(refusal)?)
        }
        old_signal::DeliveryState::Withdrawn => signal_flow::DeliveryState::Withdrawn,
    })
}

pub(crate) fn current_snapshot(
    value: &old_signal::DeliverySnapshot,
    request_id: RequestId,
) -> Result<signal_flow::DeliverySnapshot, StoreError> {
    Ok(signal_flow::DeliverySnapshot {
        delivery_cursor: value.delivery_cursor,
        logical_delivery_id: value.logical_delivery_id.clone(),
        request_id,
        sent_by: current_sent_by(&value.sent_by)?,
        address: current_address(&value.address)?,
        delivery_state: current_delivery_state(&value.delivery_state)?,
    })
}

pub(crate) fn current_queue_evidence(
    value: &old_meta::QueueEvidence,
    request_ids: &std::collections::BTreeMap<String, RequestId>,
) -> Result<meta_signal_flow::QueueEvidence, StoreError> {
    Ok(match value {
        old_meta::QueueEvidence::Logical(entry) => {
            let request_id = request_ids
                .get(&entry.logical_delivery_id)
                .cloned()
                .ok_or(StoreError::StateInvariant)?;
            meta_signal_flow::QueueEvidence::Logical(current_queue_entry(entry, request_id)?)
        }
        old_meta::QueueEvidence::Legacy(legacy) => {
            meta_signal_flow::QueueEvidence::Legacy(meta_signal_flow::QueueEvidence_Legacy_Data {
                sent_by: current_sent_by(&legacy.sent_by)?,
                request: current_request(&legacy.request)?,
            })
        }
    })
}

pub(crate) fn current_stored_request(
    value: &PriorStoredDeliveryRequest,
    request_ids: &std::collections::BTreeMap<String, RequestId>,
) -> Result<super::queue_delivery::StoredDeliveryRequest, StoreError> {
    Ok(super::queue_delivery::StoredDeliveryRequest {
        request_id: value.request_id.clone(),
        address: current_address(&value.address)?,
        queue_evidence: current_queue_evidence(&value.queue_evidence, request_ids)?,
        state: match value.state {
            PriorRequestState::Queued => super::queue_delivery::RequestState::Queued,
            PriorRequestState::Settled => super::queue_delivery::RequestState::Settled,
            PriorRequestState::Unsent => super::queue_delivery::RequestState::Unsent,
            PriorRequestState::Parked => super::queue_delivery::RequestState::Parked,
        },
    })
}

fn current_native_receipt(
    value: &PriorNativeDeliveryReceipt,
) -> super::queue_delivery::NativeDeliveryReceipt {
    super::queue_delivery::NativeDeliveryReceipt {
        native_request_id: value.native_request_id,
        thread_id: value.thread_id.clone(),
        turn_id: value.turn_id.clone(),
        native_receipt: value.native_receipt.clone(),
    }
}

pub(crate) fn current_attempt(
    value: &PriorQueueDeliveryAttempt,
    request_ids: &std::collections::BTreeMap<String, RequestId>,
) -> Result<super::queue_delivery::QueueDeliveryAttempt, StoreError> {
    use super::queue_delivery::{
        AbsenceRecord, NativeHistoryAnchor, NativeTranscriptCursor, QueueDeliveryAttempt,
        QueueDeliveryPhase,
    };
    let history_anchor = match &value.history_anchor {
        PriorNativeHistoryAnchor::ClaudeTranscript(cursor) => {
            NativeHistoryAnchor::ClaudeTranscript(NativeTranscriptCursor {
                device: cursor.device,
                inode: cursor.inode,
                offset: cursor.offset,
            })
        }
        PriorNativeHistoryAnchor::CodexTurns(turn) => NativeHistoryAnchor::CodexTurns(turn.clone()),
    };
    let phase = match &value.phase {
        PriorQueueDeliveryPhase::Prepared => QueueDeliveryPhase::Prepared,
        PriorQueueDeliveryPhase::Uncertain(undecided) => QueueDeliveryPhase::Uncertain(
            undecided
                .as_ref()
                .map(|value| recode_unchanged!(value, meta_signal_flow::Undecided))
                .transpose()?,
        ),
        PriorQueueDeliveryPhase::ReceiptRecorded(receipt) => {
            QueueDeliveryPhase::ReceiptRecorded(current_native_receipt(receipt))
        }
        PriorQueueDeliveryPhase::Settled(receipt) => {
            QueueDeliveryPhase::Settled(current_native_receipt(receipt))
        }
        PriorQueueDeliveryPhase::Absent(absence) => QueueDeliveryPhase::Absent(AbsenceRecord {
            proof: recode_unchanged!(&absence.proof, meta_signal_flow::AbsenceProof)?,
            observation: absence.observation.clone(),
        }),
        PriorQueueDeliveryPhase::Resolved(record) => QueueDeliveryPhase::Resolved(
            recode_unchanged!(record, meta_signal_flow::QueueResolutionRecord)?,
        ),
    };
    Ok(QueueDeliveryAttempt {
        request_id: value.request_id.clone(),
        attempt_id: value.attempt_id.clone(),
        native_request_id: value.native_request_id,
        flow_id: value.flow_id.clone(),
        thread_id: value.thread_id.clone(),
        harness_kind: recode_unchanged!(&value.harness_kind, flow_ethos::HarnessKind)?,
        history_anchor,
        address: current_address(&value.address)?,
        queue_evidence: current_queue_evidence(&value.queue_evidence, request_ids)?,
        phase,
    })
}

fn current_send_failure_reason(
    value: &old_signal::SendFailureReason,
) -> Result<signal_flow::SendFailureReason, StoreError> {
    Ok(match value {
        old_signal::SendFailureReason::RecipientUnavailable(address) => {
            signal_flow::SendFailureReason::RecipientUnavailable(current_address(address)?)
        }
        old_signal::SendFailureReason::QueueNotPermitted => {
            signal_flow::SendFailureReason::QueueNotPermitted
        }
        old_signal::SendFailureReason::Refused(refusal) => {
            signal_flow::SendFailureReason::Refused(current_refusal(refusal)?)
        }
        old_signal::SendFailureReason::ReceiptUncertain => {
            signal_flow::SendFailureReason::ReceiptUncertain
        }
        old_signal::SendFailureReason::NotSent(words) => {
            signal_flow::SendFailureReason::NotSent(words.clone())
        }
    })
}

fn current_acceptance(
    value: &old_signal::SendAcceptance,
    request_id: RequestId,
) -> Result<signal_flow::SendAcceptance, StoreError> {
    Ok(signal_flow::SendAcceptance {
        logical_delivery_id: value.logical_delivery_id.clone(),
        request_id,
        delivery_cursor: value.delivery_cursor,
        delivery_state: current_delivery_state(&value.delivery_state)?,
    })
}

pub(crate) fn current_send_outcome(
    value: &PriorStoredSendOutcome,
    request_ids: &std::collections::BTreeMap<String, RequestId>,
) -> Result<super::send_outcome::StoredSendOutcome, StoreError> {
    use meta_signal_flow::{OrdinarySendOutcome, PriorSendOutcome, SendOutcomeRecord};
    let old = &value.record;
    let ordinary_send_outcome = match &old.ordinary_send_outcome {
        old_meta::OrdinarySendOutcome::Accepted(acceptance) => {
            let id = request_ids
                .get(&acceptance.logical_delivery_id)
                .cloned()
                .ok_or(StoreError::StateInvariant)?;
            OrdinarySendOutcome::Accepted(current_acceptance(acceptance, id)?)
        }
        old_meta::OrdinarySendOutcome::Refused(refusal) => {
            OrdinarySendOutcome::Refused(current_refusal(refusal)?)
        }
        old_meta::OrdinarySendOutcome::PriorAdmission(prior) => {
            OrdinarySendOutcome::PriorAdmission(match prior {
                old_meta::PriorSendOutcome::Queued => PriorSendOutcome::Queued,
                old_meta::PriorSendOutcome::Delivered => PriorSendOutcome::Delivered,
            })
        }
        old_meta::OrdinarySendOutcome::Sent(receipt) => {
            let id = request_ids
                .get(&receipt.logical_delivery_id)
                .cloned()
                .ok_or(StoreError::StateInvariant)?;
            OrdinarySendOutcome::Sent(signal_flow::SendReceipt {
                logical_delivery_id: receipt.logical_delivery_id.clone(),
                request_id: id,
            })
        }
        old_meta::OrdinarySendOutcome::Failed(failure) => {
            let reason = current_send_failure_reason(&failure.send_failure_reason)?;
            OrdinarySendOutcome::Failed(match &failure.logical_delivery_id_option {
                Some(logical_delivery_id) => {
                    let id = request_ids
                        .get(logical_delivery_id)
                        .cloned()
                        .ok_or(StoreError::StateInvariant)?;
                    signal_flow::SendFailure::Admitted(signal_flow::SendFailure_Admitted_Data {
                        logical_delivery_id: logical_delivery_id.clone(),
                        request_id: id,
                        send_failure_reason: reason,
                    })
                }
                None => signal_flow::SendFailure::Unadmitted(reason),
            })
        }
    };
    Ok(super::send_outcome::StoredSendOutcome {
        record: SendOutcomeRecord {
            send_outcome_id: old.send_outcome_id.clone(),
            process_option: old
                .process_option
                .as_ref()
                .map(|value| recode_unchanged!(value, signal_flow::Process))
                .transpose()?,
            first_address_option: old
                .first_address_option
                .as_ref()
                .map(current_address)
                .transpose()?,
            second_address_option: old
                .second_address_option
                .as_ref()
                .map(current_address)
                .transpose()?,
            request_digest: old.request_digest.clone(),
            ordinary_send_outcome,
        },
    })
}

fn current_tool_call_key(
    value: &crate::generated::send_tool_before_request_ids::ToolCallKey,
) -> crate::generated::send_tool::ToolCallKey {
    crate::generated::send_tool::ToolCallKey {
        thread_id: value.thread_id.clone(),
        turn_id: value.turn_id.clone(),
        call_id: value.call_id.clone(),
    }
}

fn current_send_refusal(
    value: &crate::generated::send_tool_before_request_ids::SendRefusal,
) -> Result<crate::generated::send_tool::SendRefusal, StoreError> {
    use crate::generated::{send_tool as current, send_tool_before_request_ids as old};
    Ok(match value {
        old::SendRefusal::UnknownTool => current::SendRefusal::UnknownTool,
        old::SendRefusal::Malformed => current::SendRefusal::Malformed,
        old::SendRefusal::ThreadUnowned => current::SendRefusal::ThreadUnowned,
        old::SendRefusal::FlowEnded => current::SendRefusal::FlowEnded,
        old::SendRefusal::NotAwake => current::SendRefusal::NotAwake,
        old::SendRefusal::KeyReused => current::SendRefusal::KeyReused,
        old::SendRefusal::UnknownRecipient(target) => {
            current::SendRefusal::UnknownRecipient(current_address(target)?)
        }
        old::SendRefusal::Held => current::SendRefusal::Held,
        old::SendRefusal::Oversized(size) => {
            current::SendRefusal::Oversized(recode_unchanged!(size, signal_flow::SendPayloadSize)?)
        }
        old::SendRefusal::Store => current::SendRefusal::Store,
        old::SendRefusal::TopicRefused(topic) => current::SendRefusal::TopicRefused(topic.clone()),
    })
}

pub(crate) fn current_send_call(
    value: &PriorSendCall,
    request_ids: &std::collections::BTreeMap<String, RequestId>,
    archived: bool,
) -> Result<crate::generated::send_tool::SendCall, StoreError> {
    use crate::generated::{send_tool as current, send_tool_before_request_ids as old};
    let send_outcome = match &value.send_outcome {
        old::SendOutcome::Accepted(accepted) if archived => {
            current::SendOutcome::Archived(current::Archived_Data {
                sender: current_address(&accepted.sender)?,
                target: current_address(&accepted.target)?,
            })
        }
        old::SendOutcome::Accepted(accepted) => {
            let logical_delivery_id = &accepted.send_acceptance.logical_delivery_id;
            let request_id = request_ids
                .get(logical_delivery_id)
                .cloned()
                .ok_or(StoreError::StateInvariant)?;
            current::SendOutcome::Accepted(current::Accepted_Data {
                sender: current_address(&accepted.sender)?,
                target: current_address(&accepted.target)?,
                send_acceptance: current_acceptance(&accepted.send_acceptance, request_id)?,
            })
        }
        old::SendOutcome::LegacyUnknown(unknown) => {
            current::SendOutcome::LegacyUnknown(current::LegacyUnknown_Data {
                sender: current_address(&unknown.sender)?,
                target: current_address(&unknown.target)?,
            })
        }
        old::SendOutcome::Refused(refusal) => {
            current::SendOutcome::Refused(current_send_refusal(refusal)?)
        }
    };
    Ok(current::SendCall {
        tool_call_key: current_tool_call_key(&value.tool_call_key),
        content_sha256: value.content_sha256.clone(),
        send_outcome,
    })
}

pub(crate) struct RequestIdEvolutionDescriptors {
    pub(crate) metaflows: TableDescriptor<super::StoredMetaflow>,
    pub(crate) address_holds: TableDescriptor<super::address_hold::AddressHold>,
    pub(crate) send_calls: TableDescriptor<crate::generated::send_tool::SendCall>,
    pub(crate) requests: TableDescriptor<super::queue_delivery::StoredDeliveryRequest>,
    pub(crate) attempts: TableDescriptor<super::queue_delivery::QueueDeliveryAttempt>,
    pub(crate) outcomes: TableDescriptor<super::send_outcome::StoredSendOutcome>,
    pub(crate) current: TableDescriptor<super::logical_delivery::CurrentLogicalDelivery>,
    pub(crate) events: TableDescriptor<super::logical_delivery::LogicalDeliveryEvent>,
    pub(crate) request_ids: TableDescriptor<super::logical_delivery::RequestIdBinding>,
    pub(crate) deliver_calls: TableDescriptor<super::logical_delivery::DeliverCall>,
    pub(crate) store_identity: TableDescriptor<super::logical_delivery::StoredStoreIdentity>,
    pub(crate) prior_deliveries: TableDescriptor<PriorDelivery>,
    pub(crate) task_announcements: TableDescriptor<TaskAnnouncement>,
    pub(crate) upgrade_assignments: TableDescriptor<UpgradeAssignment>,
    pub(crate) request_policy: TableDescriptor<super::StoredMetaRequestPolicyConfiguration>,
}

fn prior_logical_id(evidence: &old_meta::QueueEvidence) -> Option<&str> {
    match evidence {
        old_meta::QueueEvidence::Logical(entry) => Some(&entry.logical_delivery_id),
        old_meta::QueueEvidence::Legacy(_) => None,
    }
}

fn outcome_logical_id(outcome: &PriorStoredSendOutcome) -> Option<&str> {
    match &outcome.record.ordinary_send_outcome {
        old_meta::OrdinarySendOutcome::Accepted(value) => Some(&value.logical_delivery_id),
        old_meta::OrdinarySendOutcome::Sent(value) => Some(&value.logical_delivery_id),
        old_meta::OrdinarySendOutcome::Failed(value) => value.logical_delivery_id_option.as_deref(),
        old_meta::OrdinarySendOutcome::Refused(_)
        | old_meta::OrdinarySendOutcome::PriorAdmission(_) => None,
    }
}

fn call_logical_id(call: &PriorSendCall) -> Option<&str> {
    match &call.send_outcome {
        crate::generated::send_tool_before_request_ids::SendOutcome::Accepted(value) => {
            Some(&value.send_acceptance.logical_delivery_id)
        }
        _ => None,
    }
}

fn prior_request<'a>(request: &'a PriorStoredDeliveryRequest) -> Option<&'a old_flow::Request> {
    match &request.queue_evidence {
        old_meta::QueueEvidence::Logical(entry) => Some(match &entry.message {
            old_flow::Message::HardAbrupt(request)
            | old_flow::Message::MiddleAbrupt(request)
            | old_flow::Message::Soft(request) => request,
        }),
        old_meta::QueueEvidence::Legacy(value) => Some(&value.request),
    }
}

fn prior_sender(request: &PriorStoredDeliveryRequest) -> Option<&old_flow::Address> {
    match &request.queue_evidence {
        old_meta::QueueEvidence::Logical(entry) => match &entry.sent_by {
            old_flow::SentBy::Sender(address) => Some(address),
            old_flow::SentBy::Unknown | old_flow::SentBy::Flow => None,
        },
        old_meta::QueueEvidence::Legacy(value) => match &value.sent_by {
            old_flow::SentBy::Sender(address) => Some(address),
            old_flow::SentBy::Unknown | old_flow::SentBy::Flow => None,
        },
    }
}

fn attempt_unfinished(attempt: &PriorQueueDeliveryAttempt) -> bool {
    matches!(
        attempt.phase,
        PriorQueueDeliveryPhase::Prepared
            | PriorQueueDeliveryPhase::Uncertain(_)
            | PriorQueueDeliveryPhase::ReceiptRecorded(_)
    )
}

fn state_is_held(state: &old_signal::DeliveryState) -> bool {
    matches!(
        state,
        old_signal::DeliveryState::Queued
            | old_signal::DeliveryState::Attempting
            | old_signal::DeliveryState::Retryable(_)
            | old_signal::DeliveryState::Uncertain
    )
}

fn first_event_cursor(events: &[PriorLogicalDeliveryEvent]) -> Option<i64> {
    events
        .iter()
        .map(|event| event.snapshot.delivery_cursor)
        .min()
}

fn first_delivered_cursor(events: &[PriorLogicalDeliveryEvent]) -> Option<i64> {
    events
        .iter()
        .filter(|event| {
            matches!(
                event.snapshot.delivery_state,
                old_signal::DeliveryState::Delivered(_)
            )
        })
        .map(|event| event.snapshot.delivery_cursor)
        .min()
}

/// Evolves the exact accepted 0.53.2 running Memory in one engine commit.
/// Every check and projection is completed before the commit is built.
#[allow(clippy::too_many_arguments)]
pub(crate) fn evolve_request_id_memory(
    engine: &mut Engine,
    descriptors: RequestIdEvolutionDescriptors,
    reminders: &[super::reminder::Reminder],
    simplified_replies: &[super::logical_delivery::SimplifiedReplyBinding],
    heartbeat_table: TableReference<super::StoredHeartbeatStanding>,
    heartbeat_rows: &[super::StoredHeartbeatStanding],
    drawer: &dyn super::logical_delivery::DrawsRequestId,
) -> Result<(), StoreError> {
    use super::logical_delivery::{
        CurrentLogicalDelivery, LogicalDeliveryEvent, RequestClosure, RequestIdBinding,
        StoredStoreIdentity,
    };
    use std::collections::{BTreeMap, BTreeSet};

    let current_inventory = engine
        .inspect_predecessor::<PriorCurrentLogicalDelivery, CurrentLogicalDelivery>(
            &descriptors.current,
        )?;
    let event_inventory = engine
        .inspect_predecessor::<PriorLogicalDeliveryEvent, LogicalDeliveryEvent>(
            &descriptors.events,
        )?;
    let metaflow_inventory = engine
        .inspect_predecessor::<PriorStoredMetaflow, super::StoredMetaflow>(
            &descriptors.metaflows,
        )?;
    let hold_inventory = engine
        .inspect_predecessor::<PriorAddressHold, super::address_hold::AddressHold>(
            &descriptors.address_holds,
        )?;
    let request_inventory = engine.inspect_predecessor::<
        PriorStoredDeliveryRequest,
        super::queue_delivery::StoredDeliveryRequest,
    >(&descriptors.requests)?;
    let attempt_inventory = engine.inspect_predecessor::<
        PriorQueueDeliveryAttempt,
        super::queue_delivery::QueueDeliveryAttempt,
    >(&descriptors.attempts)?;
    let outcome_inventory = engine
        .inspect_predecessor::<PriorStoredSendOutcome, super::send_outcome::StoredSendOutcome>(
            &descriptors.outcomes,
        )?;
    let call_inventory = engine
        .inspect_predecessor::<PriorSendCall, crate::generated::send_tool::SendCall>(
            &descriptors.send_calls,
        )?;

    let mut currents = BTreeMap::new();
    for (key, row) in current_inventory.rows() {
        if *key != row.record_key()
            || currents
                .insert(row.snapshot.logical_delivery_id.clone(), row.clone())
                .is_some()
        {
            return Err(StoreError::StateInvariant);
        }
    }
    let mut events: BTreeMap<String, Vec<PriorLogicalDeliveryEvent>> = BTreeMap::new();
    let mut event_cursors = BTreeSet::new();
    for (key, row) in event_inventory.rows() {
        if *key != row.record_key()
            || !event_cursors.insert(row.snapshot.delivery_cursor)
            || !currents.contains_key(&row.snapshot.logical_delivery_id)
        {
            return Err(StoreError::StateInvariant);
        }
        events
            .entry(row.snapshot.logical_delivery_id.clone())
            .or_default()
            .push(row.clone());
    }
    for (logical_id, current) in &currents {
        let delivery_events = events
            .get_mut(logical_id)
            .ok_or(StoreError::StateInvariant)?;
        delivery_events.sort_by_key(|event| event.snapshot.delivery_cursor);
        if first_event_cursor(delivery_events).is_none()
            || delivery_events.last().map(|event| &event.snapshot) != Some(&current.snapshot)
        {
            return Err(StoreError::StateInvariant);
        }
    }

    let mut requests = BTreeMap::new();
    for (key, row) in request_inventory.rows() {
        if *key != row.record_key()
            || !currents.contains_key(&row.request_id)
            || requests
                .insert(row.request_id.clone(), row.clone())
                .is_some()
        {
            return Err(StoreError::StateInvariant);
        }
        if let Some(logical_id) = prior_logical_id(&row.queue_evidence)
            && logical_id != row.request_id
        {
            return Err(StoreError::StateInvariant);
        }
    }
    if currents
        .keys()
        .any(|logical_id| !requests.contains_key(logical_id))
    {
        return Err(StoreError::StateInvariant);
    }

    let mut attempts: BTreeMap<String, Vec<PriorQueueDeliveryAttempt>> = BTreeMap::new();
    for (key, row) in attempt_inventory.rows() {
        if *key != row.record_key() || !currents.contains_key(&row.request_id) {
            return Err(StoreError::StateInvariant);
        }
        if let Some(logical_id) = prior_logical_id(&row.queue_evidence)
            && logical_id != row.request_id
        {
            return Err(StoreError::StateInvariant);
        }
        attempts
            .entry(row.request_id.clone())
            .or_default()
            .push(row.clone());
    }

    let mut outcomes: BTreeMap<String, Vec<PriorStoredSendOutcome>> = BTreeMap::new();
    let mut unbound_outcomes = Vec::new();
    for (key, row) in outcome_inventory.rows() {
        if *key != row.record_key() {
            return Err(StoreError::StateInvariant);
        }
        match outcome_logical_id(row) {
            Some(logical_id) if currents.contains_key(logical_id) => {
                outcomes
                    .entry(logical_id.to_owned())
                    .or_default()
                    .push(row.clone());
            }
            Some(_) => return Err(StoreError::StateInvariant),
            None => unbound_outcomes.push(row.clone()),
        }
    }
    for (_, row) in call_inventory.rows() {
        if let Some(logical_id) = call_logical_id(row)
            && !currents.contains_key(logical_id)
        {
            return Err(StoreError::StateInvariant);
        }
    }
    for reminder in reminders {
        if !currents.contains_key(&reminder.key) {
            return Err(StoreError::StateInvariant);
        }
    }
    for binding in simplified_replies {
        if !currents.contains_key(&binding.input_delivery_id) {
            return Err(StoreError::StateInvariant);
        }
    }
    for standing in heartbeat_rows {
        if standing
            .outstanding
            .as_ref()
            .is_some_and(|logical_id| !currents.contains_key(logical_id))
        {
            return Err(StoreError::StateInvariant);
        }
    }
    for (_, metaflow) in metaflow_inventory.rows() {
        for entry in &metaflow.metaflow.queue {
            let current = currents
                .get(&entry.logical_delivery_id)
                .ok_or(StoreError::StateInvariant)?;
            if !state_is_held(&current.snapshot.delivery_state) {
                return Err(StoreError::StateInvariant);
            }
        }
    }

    let reminder_tasks: BTreeSet<_> = reminders.iter().map(|row| row.key.clone()).collect();
    let reply_inputs: BTreeSet<_> = simplified_replies
        .iter()
        .filter(|row| !row.completed)
        .map(|row| row.input_delivery_id.clone())
        .collect();
    let mut held = BTreeSet::new();
    for (logical_id, current) in &currents {
        if state_is_held(&current.snapshot.delivery_state)
            || attempts
                .get(logical_id)
                .is_some_and(|values| values.iter().any(attempt_unfinished))
            || reminder_tasks.contains(logical_id)
            || reply_inputs.contains(logical_id)
        {
            held.insert(logical_id.clone());
        }
    }

    const REQUEST_ID_SPACE: usize = 36 * 36 * 36 * 36;
    if held.len() > REQUEST_ID_SPACE {
        return Err(StoreError::StateInvariant);
    }
    let mut request_ids = BTreeMap::new();
    let mut used = BTreeSet::new();
    for logical_id in &held {
        let mut selected = None;
        for _ in 0..super::SEEDED_REQUEST_ID_DRAWS {
            let id = drawer.draw_request_id()?;
            if id.len() != 4
                || !id
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || byte.is_ascii_lowercase())
            {
                return Err(StoreError::StateInvariant);
            }
            if used.insert(id.clone()) {
                selected = Some(id);
                break;
            }
        }
        request_ids.insert(
            logical_id.clone(),
            selected.ok_or(StoreError::StateInvariant)?,
        );
    }

    let mut closures: BTreeMap<String, RequestClosure> = held
        .iter()
        .map(|logical_id| (logical_id.clone(), RequestClosure::Open))
        .collect();
    for (answer_id, request) in &requests {
        if let Some(old_flow::Request::Done(old_flow::DonePayload::Answered(done))) =
            prior_request(request)
            && held.contains(&done.logical_delivery_id)
        {
            closures.insert(
                done.logical_delivery_id.clone(),
                RequestClosure::Answered(answer_id.clone()),
            );
        }
    }
    for binding in simplified_replies.iter().filter(|row| row.completed) {
        if held.contains(&binding.input_delivery_id) {
            closures.insert(
                binding.input_delivery_id.clone(),
                RequestClosure::Answered(format!(
                    "simplified-result-{}-{}",
                    binding.flow_id, binding.input_delivery_id
                )),
            );
        }
    }

    let current_rows = held
        .iter()
        .map(|logical_id| {
            let old = currents.get(logical_id).ok_or(StoreError::StateInvariant)?;
            Ok(CurrentLogicalDelivery {
                snapshot: current_snapshot(
                    &old.snapshot,
                    request_ids
                        .get(logical_id)
                        .cloned()
                        .ok_or(StoreError::StateInvariant)?,
                )?,
                archived: None,
            })
        })
        .collect::<Result<Vec<_>, StoreError>>()?;
    let event_rows = held
        .iter()
        .flat_map(|logical_id| {
            events
                .get(logical_id)
                .into_iter()
                .flatten()
                .map(|event| (logical_id, event))
        })
        .map(|(logical_id, event)| {
            Ok(LogicalDeliveryEvent {
                snapshot: current_snapshot(
                    &event.snapshot,
                    request_ids
                        .get(logical_id)
                        .cloned()
                        .ok_or(StoreError::StateInvariant)?,
                )?,
            })
        })
        .collect::<Result<Vec<_>, StoreError>>()?;
    let request_rows = held
        .iter()
        .map(|logical_id| {
            current_stored_request(
                requests.get(logical_id).ok_or(StoreError::StateInvariant)?,
                &request_ids,
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    let attempt_rows = held
        .iter()
        .flat_map(|logical_id| attempts.get(logical_id).into_iter().flatten())
        .map(|attempt| current_attempt(attempt, &request_ids))
        .collect::<Result<Vec<_>, _>>()?;
    let mut outcome_rows = unbound_outcomes
        .iter()
        .map(|outcome| current_send_outcome(outcome, &request_ids))
        .collect::<Result<Vec<_>, _>>()?;
    for logical_id in &held {
        for outcome in outcomes.get(logical_id).into_iter().flatten() {
            outcome_rows.push(current_send_outcome(outcome, &request_ids)?);
        }
    }

    let metaflow_rows = metaflow_inventory
        .rows()
        .iter()
        .map(|(_, old)| {
            Ok(super::StoredMetaflow {
                metaflow: current_metaflow(&old.metaflow, &request_ids)?,
            })
        })
        .collect::<Result<Vec<_>, StoreError>>()?;
    let hold_rows = hold_inventory
        .rows()
        .iter()
        .map(|(_, old)| {
            Ok(super::address_hold::AddressHold {
                flow_id: old.flow_id.clone(),
                address: current_address(&old.address)?,
                found: old
                    .found
                    .as_ref()
                    .map(|value| current_metaflow(value, &request_ids))
                    .transpose()?,
            })
        })
        .collect::<Result<Vec<_>, StoreError>>()?;
    let call_rows = call_inventory
        .rows()
        .iter()
        .map(|(_, call)| {
            let archived = call_logical_id(call).is_some_and(|id| !held.contains(id));
            current_send_call(call, &request_ids, archived)
        })
        .collect::<Result<Vec<_>, _>>()?;

    let mut prior_rows = Vec::new();
    for logical_id in currents
        .keys()
        .filter(|logical_id| !held.contains(*logical_id))
    {
        prior_rows.push(PriorDelivery {
            logical_delivery_id: logical_id.clone(),
            current: currents
                .get(logical_id)
                .cloned()
                .ok_or(StoreError::StateInvariant)?,
            events: events
                .get(logical_id)
                .cloned()
                .ok_or(StoreError::StateInvariant)?,
            request: requests
                .get(logical_id)
                .cloned()
                .ok_or(StoreError::StateInvariant)?,
            attempts: attempts.get(logical_id).cloned().unwrap_or_default(),
            outcomes: outcomes.get(logical_id).cloned().unwrap_or_default(),
        });
    }
    let binding_rows = held
        .iter()
        .map(|logical_id| RequestIdBinding {
            request_id: request_ids[logical_id].clone(),
            logical_delivery_id: logical_id.clone(),
            closure: closures
                .get(logical_id)
                .cloned()
                .unwrap_or(RequestClosure::Open),
        })
        .collect::<Vec<_>>();

    let mut announcement_groups: BTreeMap<String, (Address, Vec<String>)> = BTreeMap::new();
    for reminder in reminders {
        if !held.contains(&reminder.key)
            || !matches!(
                closures.get(&reminder.key),
                None | Some(RequestClosure::Open)
            )
        {
            continue;
        }
        let current = &currents[&reminder.key].snapshot.delivery_state;
        if matches!(
            current,
            old_signal::DeliveryState::Attempting
                | old_signal::DeliveryState::Uncertain
                | old_signal::DeliveryState::Delivered(_)
        ) {
            announcement_groups
                .entry(current_address_key(&reminder.recipient))
                .or_insert_with(|| (reminder.recipient.clone(), Vec::new()))
                .1
                .push(reminder.key.clone());
        }
    }
    let task_announcements = announcement_groups
        .into_iter()
        .map(|(_, (recipient, mut tasks))| {
            tasks.sort();
            TaskAnnouncement {
                recipient,
                part: 0,
                tasks,
                tries: 0,
                status: TaskAnnouncementStatus::Pending(None),
            }
        })
        .collect::<Vec<_>>();

    let mut identity = String::with_capacity(16);
    for _ in 0..4 {
        identity.push_str(&drawer.draw_request_id()?);
    }
    if identity.len() != 16
        || !identity
            .bytes()
            .all(|byte| byte.is_ascii_digit() || byte.is_ascii_lowercase())
    {
        return Err(StoreError::StateInvariant);
    }

    let mut evolution = engine
        .begin_atomic_evolution()
        .evolve(current_inventory, descriptors.current, current_rows)
        .evolve(event_inventory, descriptors.events, event_rows)
        .evolve(metaflow_inventory, descriptors.metaflows, metaflow_rows)
        .evolve(hold_inventory, descriptors.address_holds, hold_rows)
        .evolve(request_inventory, descriptors.requests, request_rows)
        .evolve(attempt_inventory, descriptors.attempts, attempt_rows)
        .evolve(outcome_inventory, descriptors.outcomes, outcome_rows)
        .evolve(call_inventory, descriptors.send_calls, call_rows)
        .register(descriptors.request_ids, binding_rows)
        .register(descriptors.deliver_calls, Vec::new())
        .register(
            descriptors.store_identity,
            vec![StoredStoreIdentity {
                store_identity: identity,
            }],
        )
        .register(descriptors.prior_deliveries, prior_rows)
        .register(descriptors.task_announcements, task_announcements)
        .register(descriptors.upgrade_assignments, Vec::new())
        .register(
            descriptors.request_policy,
            vec![super::StoredMetaRequestPolicyConfiguration {
                request_id_draws: super::SEEDED_REQUEST_ID_DRAWS,
                announcement_tries: super::SEEDED_ANNOUNCEMENT_TRIES,
            }],
        );
    for standing in heartbeat_rows {
        if standing
            .outstanding
            .as_ref()
            .is_some_and(|logical_id| !held.contains(logical_id))
        {
            let mut cleared = standing.clone();
            cleared.outstanding = None;
            evolution = evolution.mutate(heartbeat_table, cleared);
        }
    }
    engine.commit_atomic_evolution(evolution)?;
    Ok(())
}
