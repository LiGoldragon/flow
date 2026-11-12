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

const TASK_ANNOUNCEMENT_HEADER: &str = "Your open tasks; close each with Done by its id:";
const ASSIGNMENT_HEADER: &str = "Identical Orders answered before ids; each answer was given to the earliest delivered. If one is wrong, archive the open one or send it again:";
const LONGEST_TASK_ID: &str = "0000";
const LONGEST_ASSIGNMENT_ID: &str = "archived";

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

/// One flow's harness events as 0.53.2 stored them (events v4): a Stop
/// carries no background work.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct PriorFlowEvents {
    pub(crate) flow_id: String,
    pub(crate) event_vector: Vec<old_signal::Event>,
}
impl EngineRecord for PriorFlowEvents {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.flow_id.clone())
    }
}
impl MemoryDefinition for PriorFlowEvents {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(4)
    }
    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label("flow-nexus-flow-events-v4")
    }
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct PriorStoredMetaflow {
    pub(crate) metaflow: old_flow::Permaflow,
}
impl EngineRecord for PriorStoredMetaflow {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.metaflow.address.address_key())
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
    pub(crate) found: Option<old_flow::Permaflow>,
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
/// The table each upgrade record family is kept in.
pub(crate) trait DescribesUpgradeTable: Sized {
    fn descriptor() -> TableDescriptor<Self>;
}

impl DescribesUpgradeTable for PriorDelivery {
    fn descriptor() -> TableDescriptor<Self> {
        TableDescriptor::new(
            PRIOR_DELIVERY_TABLE_NAME,
            FamilyName::new("flow-nexus-delivery-before-ids"),
            PriorDelivery::schema_hash(),
        )
    }
}

/// A predecessor address as the 0.53.2 Memory keyed it.
pub(crate) trait KeysPriorAddress {
    fn address_key(&self) -> String;
    /// The key under which an Order and the answers that may close it meet:
    /// the Order's recipient, its sender and its words.
    fn answer_group_key(&self, sender: &old_flow::Address, words: &str) -> String;
}

impl KeysPriorAddress for old_flow::Address {
    fn address_key(&self) -> String {
        format!(
            "{}:{}:{}:{}",
            self.aspect.key_word(),
            self.topic.len(),
            self.topic,
            self.model.key_word()
        )
    }

    fn answer_group_key(&self, sender: &old_flow::Address, words: &str) -> String {
        let recipient = self.address_key();
        let sender = sender.address_key();
        format!(
            "{}:{recipient}:{}:{sender}:{}:{words}",
            recipient.len(),
            sender.len(),
            words.len()
        )
    }
}

/// The word an address key spells a variant with.
pub(crate) trait NamesKeyWord {
    fn key_word(&self) -> &'static str;
}

impl NamesKeyWord for old_flow::Aspect {
    fn key_word(&self) -> &'static str {
        match self {
            old_flow::Aspect::Psyche => "Psyche",
            old_flow::Aspect::Mind => "Mind",
            old_flow::Aspect::Field => "Field",
        }
    }
}

impl NamesKeyWord for old_flow::Model {
    fn key_word(&self) -> &'static str {
        match self {
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
        RecordKey::new(self.recipient.part_key(self.part))
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
        RecordKey::new(self.sender.part_key(self.part))
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

impl DescribesUpgradeTable for TaskAnnouncement {
    fn descriptor() -> TableDescriptor<Self> {
        TableDescriptor::new(
            TASK_ANNOUNCEMENT_TABLE_NAME,
            FamilyName::new("flow-nexus-task-announcement"),
            TaskAnnouncement::schema_hash(),
        )
    }
}

impl DescribesUpgradeTable for UpgradeAssignment {
    fn descriptor() -> TableDescriptor<Self> {
        TableDescriptor::new(
            UPGRADE_ASSIGNMENT_TABLE_NAME,
            FamilyName::new("flow-nexus-upgrade-assignment"),
            UpgradeAssignment::schema_hash(),
        )
    }
}

/// A current address as the upgrade's notice rows key it.
pub(crate) trait KeysCurrentAddress {
    fn current_address_key(&self) -> String;
    /// The key of one bounded part of a notice to this address.
    fn part_key(&self, part: u64) -> String;
}

impl KeysCurrentAddress for Address {
    fn current_address_key(&self) -> String {
        format!(
            "{}:{}:{}:{}",
            match self.aspect {
                flow_ethos::Aspect::Psyche => "Psyche",
                flow_ethos::Aspect::Mind => "Mind",
                flow_ethos::Aspect::Field => "Field",
            },
            self.topic.0.len(),
            self.topic.0,
            self.model.key_word()
        )
    }

    fn part_key(&self, part: u64) -> String {
        let address = self.current_address_key();
        format!("{}:{address}:{part:020}", address.len())
    }
}

impl NamesKeyWord for flow_ethos::Model {
    fn key_word(&self) -> &'static str {
        match self {
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

#[derive(Clone)]
pub(crate) enum UpgradeNoticeAdmission {
    Task(TaskAnnouncement),
    Assignment(UpgradeAssignment),
}

pub(crate) trait RunsRequestIdAnnouncements {
    fn run_request_id_announcements(&self) -> Result<(), StoreError>;
}

impl RunsRequestIdAnnouncements for super::FlowStore {
    fn run_request_id_announcements(&self) -> Result<(), StoreError> {
        use super::logical_delivery::RequestClosure;
        use super::queue_delivery::AccountsQueueDelivery;
        use sema_engine::{KeyedMutation, QueryPlan};

        let policy = self.engine.match_records(QueryPlan::all(
            self.meta_configuration_tables.request_policy,
        ))?;
        let [policy] = policy.records() else {
            return Err(StoreError::StateInvariant);
        };
        let retained = self.retained_logical_requests()?;
        let prior = self
            .engine
            .match_records(QueryPlan::all(self.request_id_upgrade.prior_deliveries))?;
        let bindings = self
            .engine
            .match_records(QueryPlan::all(self.logical_deliveries.request_ids))?;
        let currents = self
            .engine
            .match_records(QueryPlan::all(self.logical_deliveries.current))?;
        let open = |logical_id: &str| {
            bindings.records().iter().any(|binding| {
                binding.logical_delivery_id.0 == logical_id
                    && binding.closure == RequestClosure::Open
            }) && currents.records().iter().any(|current| {
                current.snapshot.logical_delivery_id.0 == logical_id && current.archived.is_none()
            })
        };
        let request_id = |logical_id: &str| {
            bindings
                .records()
                .iter()
                .find(|binding| binding.logical_delivery_id.0 == logical_id)
                .map(|binding| binding.request_id.clone())
        };
        let task_words = |logical_id: &str| {
            retained.iter().find_map(|value| {
                if value.entry.logical_delivery_id.0 != logical_id {
                    return None;
                }
                match &value.entry.message {
                    flow_ethos::Message::HardAbrupt(flow_ethos::Request::Task(task))
                    | flow_ethos::Message::MiddleAbrupt(flow_ethos::Request::Task(task))
                    | flow_ethos::Message::Soft(flow_ethos::Request::Task(task)) => {
                        Some(task.string.clone())
                    }
                    _ => None,
                }
            })
        };
        let order_words = |logical_id: &str| {
            retained
                .iter()
                .find_map(|value| {
                    if value.entry.logical_delivery_id.0 != logical_id {
                        return None;
                    }
                    match &value.entry.message {
                        flow_ethos::Message::HardAbrupt(flow_ethos::Request::Order(words))
                        | flow_ethos::Message::MiddleAbrupt(flow_ethos::Request::Order(words))
                        | flow_ethos::Message::Soft(flow_ethos::Request::Order(words)) => {
                            Some(words.clone())
                        }
                        _ => None,
                    }
                })
                .or_else(|| {
                    prior.records().iter().find_map(|value| {
                        (value.logical_delivery_id == logical_id)
                            .then(|| value.request.prior_order_words())
                            .flatten()
                    })
                })
        };

        let task_rows = self
            .engine
            .match_records(QueryPlan::all(self.request_id_upgrade.task_announcements))?;
        for mut row in task_rows.records().to_vec() {
            if self.settle_notice_status(&mut row.status, row.tries, policy.announcement_tries.0)? {
                self.engine.mutate_keyed(KeyedMutation::new(
                    self.request_id_upgrade.task_announcements,
                    row.record_key(),
                    row,
                ))?;
                continue;
            }
            if !matches!(row.status, TaskAnnouncementStatus::Pending(_)) {
                continue;
            }
            let mut lines = Vec::new();
            for logical_id in &row.tasks {
                if open(&logical_id.0) {
                    let id = request_id(&logical_id.0).ok_or(StoreError::StateInvariant)?;
                    let words = task_words(&logical_id.0).ok_or(StoreError::StateInvariant)?;
                    lines.push(
                        TASK_ANNOUNCEMENT_HEADER
                            .render_bounded_line(&format!("{id} ", id = id.0), &words),
                    );
                }
            }
            if lines.is_empty() {
                row.status = TaskAnnouncementStatus::Closed;
                self.engine.mutate_keyed(KeyedMutation::new(
                    self.request_id_upgrade.task_announcements,
                    row.record_key(),
                    row,
                ))?;
                continue;
            }
            let text = format!("{TASK_ANNOUNCEMENT_HEADER}\n{}", lines.join("\n"));
            self.admit_upgrade_notice(
                row.recipient.clone(),
                text,
                UpgradeNoticeAdmission::Task(row),
            )?;
        }

        let assignment_rows = self
            .engine
            .match_records(QueryPlan::all(self.request_id_upgrade.upgrade_assignments))?;
        for mut row in assignment_rows.records().to_vec() {
            if self.settle_notice_status(&mut row.status, row.tries, policy.announcement_tries.0)? {
                self.engine.mutate_keyed(KeyedMutation::new(
                    self.request_id_upgrade.upgrade_assignments,
                    row.record_key(),
                    row,
                ))?;
                continue;
            }
            if !matches!(row.status, TaskAnnouncementStatus::Pending(_)) {
                continue;
            }
            let has_open = row.decisions.iter().any(|decision| {
                decision
                    .assigned
                    .iter()
                    .any(|assigned| open(&assigned.order.0))
                    || decision.open.iter().any(|order| open(&order.order.0))
            });
            if !has_open {
                row.status = TaskAnnouncementStatus::Closed;
                self.engine.mutate_keyed(KeyedMutation::new(
                    self.request_id_upgrade.upgrade_assignments,
                    row.record_key(),
                    row,
                ))?;
                continue;
            }
            let mut lines = Vec::new();
            for decision in &row.decisions {
                for assigned in &decision.assigned {
                    let id = request_id(&assigned.order.0)
                        .map(|id| id.0)
                        .unwrap_or_else(|| "archived".into());
                    let words = order_words(&assigned.order.0).ok_or(StoreError::StateInvariant)?;
                    lines.push(
                        ASSIGNMENT_HEADER.render_bounded_line(&format!("{id} answered "), &words),
                    );
                }
                for order in &decision.open {
                    let id = request_id(&order.order.0)
                        .map(|id| id.0)
                        .unwrap_or_else(|| "archived".into());
                    let words = order_words(&order.order.0).ok_or(StoreError::StateInvariant)?;
                    lines.push(
                        ASSIGNMENT_HEADER.render_bounded_line(&format!("{id} open "), &words),
                    );
                }
            }
            let text = format!("{ASSIGNMENT_HEADER}\n{}", lines.join("\n"));
            self.admit_upgrade_notice(
                row.sender.clone(),
                text,
                UpgradeNoticeAdmission::Assignment(row),
            )?;
        }
        Ok(())
    }
}

/// One upgrade notice row's delivery: settled from the delivery it was
/// admitted as, or admitted as a new one.
pub(crate) trait SettlesUpgradeNotices {
    fn settle_notice_status(
        &self,
        status: &mut TaskAnnouncementStatus,
        tries: u64,
        bound: i64,
    ) -> Result<bool, StoreError>;
    fn admit_upgrade_notice(
        &self,
        address: Address,
        text: String,
        transition: UpgradeNoticeAdmission,
    ) -> Result<(), StoreError>;
}

impl SettlesUpgradeNotices for super::FlowStore {
    fn settle_notice_status(
        &self,
        status: &mut TaskAnnouncementStatus,
        tries: u64,
        bound: i64,
    ) -> Result<bool, StoreError> {
        use super::logical_delivery::RecordsLogicalDeliveries;
        let TaskAnnouncementStatus::Admitted(logical_id) = status else {
            return Ok(false);
        };
        let Some(snapshot) =
            self.logical_delivery_snapshot(&flow_ethos::LogicalDeliveryId(logical_id.clone()))?
        else {
            return Err(StoreError::StateInvariant);
        };
        match snapshot.delivery_state {
            signal_flow::DeliveryState::Delivered(_) | signal_flow::DeliveryState::Read => {
                *status = TaskAnnouncementStatus::Delivered(logical_id.clone());
                Ok(true)
            }
            signal_flow::DeliveryState::Failed(_) | signal_flow::DeliveryState::Withdrawn => {
                *status = if i64::try_from(tries).map_err(|_| StoreError::StateInvariant)? < bound {
                    TaskAnnouncementStatus::Pending(None)
                } else {
                    TaskAnnouncementStatus::Failed(logical_id.clone())
                };
                Ok(true)
            }
            signal_flow::DeliveryState::Queued
            | signal_flow::DeliveryState::Attempting
            | signal_flow::DeliveryState::Retryable(_)
            | signal_flow::DeliveryState::Uncertain
            | signal_flow::DeliveryState::Archived => Ok(false),
        }
    }

    fn admit_upgrade_notice(
        &self,
        address: Address,
        text: String,
        transition: UpgradeNoticeAdmission,
    ) -> Result<(), StoreError> {
        use super::logical_delivery::{LogicalDeliveryTransition, RecordsLogicalDeliveries};
        use sema_engine::KeyedMutation;
        let response = self.accept_logical_delivery_for_with_transition(
            &address,
            flow_ethos::SentBy::Flow,
            flow_ethos::Delivery::Queue,
            flow_ethos::Message::Soft(flow_ethos::Request::Notice(text)),
            None,
            LogicalDeliveryTransition {
                purpose: crate::heartbeat::DeliveryPurpose::Ordinary,
                handover_reminder: None,
                deliver_call: None,
                upgrade_notice: Some(transition.clone()),
            },
        )?;
        if let signal_flow::PrivateSendResponse::Refused(refusal) = response {
            match transition {
                UpgradeNoticeAdmission::Task(mut row) => {
                    if row.status != TaskAnnouncementStatus::Pending(Some(refusal.clone())) {
                        row.status = TaskAnnouncementStatus::Pending(Some(refusal));
                        self.engine.mutate_keyed(KeyedMutation::new(
                            self.request_id_upgrade.task_announcements,
                            row.record_key(),
                            row,
                        ))?;
                    }
                }
                UpgradeNoticeAdmission::Assignment(mut row) => {
                    if row.status != TaskAnnouncementStatus::Pending(Some(refusal.clone())) {
                        row.status = TaskAnnouncementStatus::Pending(Some(refusal));
                        self.engine.mutate_keyed(KeyedMutation::new(
                            self.request_id_upgrade.upgrade_assignments,
                            row.record_key(),
                            row,
                        ))?;
                    }
                }
            }
        }
        Ok(())
    }
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
            prior_deliveries: PriorDelivery::descriptor(),
            task_announcements: TaskAnnouncement::descriptor(),
            upgrade_assignments: UpgradeAssignment::descriptor(),
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

/// A predecessor value recoded into its current contract.
pub(crate) trait BecomesCurrent {
    type Current;
    fn current(&self) -> Result<Self::Current, StoreError>;
}

/// A predecessor value recoded with the request id the evolution drew for
/// its logical delivery.
pub(crate) trait BecomesCurrentWithRequestId {
    type Current;
    fn current_with_request_id(&self, request_id: RequestId) -> Result<Self::Current, StoreError>;
}

/// A predecessor value recoded with the request ids the evolution drew, by
/// logical delivery.
pub(crate) trait BecomesCurrentWithRequestIds {
    type Current;
    fn current_with_request_ids(
        &self,
        request_ids: &std::collections::BTreeMap<String, RequestId>,
    ) -> Result<Self::Current, StoreError>;
}

impl BecomesCurrent for old_flow::Address {
    type Current = Address;
    fn current(&self) -> Result<Address, StoreError> {
        recode_unchanged!(self, Address)
    }
}

impl BecomesCurrent for old_flow::SentBy {
    type Current = flow_ethos::SentBy;
    fn current(&self) -> Result<flow_ethos::SentBy, StoreError> {
        recode_unchanged!(self, flow_ethos::SentBy)
    }
}

impl BecomesCurrent for old_flow::Message {
    type Current = flow_ethos::Message;
    fn current(&self) -> Result<flow_ethos::Message, StoreError> {
        Ok(match self {
            old_flow::Message::HardAbrupt(request) => {
                flow_ethos::Message::HardAbrupt(request.current()?)
            }
            old_flow::Message::MiddleAbrupt(request) => {
                flow_ethos::Message::MiddleAbrupt(request.current()?)
            }
            old_flow::Message::Soft(request) => flow_ethos::Message::Soft(request.current()?),
        })
    }
}

impl BecomesCurrent for old_flow::Request {
    type Current = flow_ethos::Request;
    fn current(&self) -> Result<flow_ethos::Request, StoreError> {
        Ok(match self {
            old_flow::Request::Order(words) => flow_ethos::Request::Order(words.clone()),
            old_flow::Request::Question(words) => flow_ethos::Request::Question(words.clone()),
            old_flow::Request::Psyche(said) => {
                flow_ethos::Request::Living(recode_unchanged!(said, flow_ethos::Living_Data)?)
            }
            old_flow::Request::Psyches(saids) => {
                use crate::store::living_upgrade::JoinsSayings;
                flow_ethos::Request::Living(
                    saids
                        .iter()
                        .map(|said| recode_unchanged!(said, flow_ethos::Living_Data))
                        .collect::<Result<Vec<_>, _>>()?
                        .joined(),
                )
            }
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
                        logical_delivery_id: flow_ethos::LogicalDeliveryId(
                            answered.logical_delivery_id.clone(),
                        ),
                        string: answered.string.clone(),
                    })
                }
                old_flow::DonePayload::Historical(historical) => {
                    flow_ethos::DonePayload::Historical(recode_unchanged!(
                        historical,
                        flow_ethos::Historical_Data
                    )?)
                }
            }),
        })
    }
}

impl BecomesCurrentWithRequestId for old_flow::QueueEntry {
    type Current = flow_ethos::QueueEntry;
    fn current_with_request_id(
        &self,
        request_id: RequestId,
    ) -> Result<flow_ethos::QueueEntry, StoreError> {
        Ok(flow_ethos::QueueEntry {
            logical_delivery_id: flow_ethos::LogicalDeliveryId(self.logical_delivery_id.clone()),
            request_id,
            sent_by: self.sent_by.current()?,
            delivery: recode_unchanged!(&self.delivery, flow_ethos::Delivery)?,
            message: self.message.current()?,
        })
    }
}

impl BecomesCurrentWithRequestIds for old_flow::Permaflow {
    type Current = flow_ethos::Permaflow;
    fn current_with_request_ids(
        &self,
        request_ids: &std::collections::BTreeMap<String, RequestId>,
    ) -> Result<flow_ethos::Permaflow, StoreError> {
        Ok(flow_ethos::Permaflow {
            address: self.address.current()?,
            permaflow_state: recode_unchanged!(&self.state, flow_ethos::PermaflowState)?,
            past: flow_ethos::Past(self.past.iter().cloned().map(flow_ethos::FlowId).collect()),
            queue: flow_ethos::Queue(
                self.queue
                    .iter()
                    .map(|entry| {
                        let request_id = request_ids
                            .get(&entry.logical_delivery_id)
                            .cloned()
                            .ok_or(StoreError::StateInvariant)?;
                        entry.current_with_request_id(request_id)
                    })
                    .collect::<Result<Vec<_>, _>>()?,
            ),
        })
    }
}

/// A 0.53.2 harness event as 0.53.3 holds it: a Stop carries no background
/// work, as every v4 Stop was read as its flow's rest.
impl BecomesCurrent for old_signal::Event {
    type Current = super::events::RecordedHarnessEvent;
    fn current(&self) -> Result<super::events::RecordedHarnessEvent, StoreError> {
        Ok(match self {
            old_signal::Event::Started(session) => super::events::RecordedHarnessEvent::Started(
                session.clone().map(signal_flow::NativeSessionId),
            ),
            old_signal::Event::ToolUsed(tool) => {
                super::events::RecordedHarnessEvent::ToolUsed(tool.clone())
            }
            old_signal::Event::Stopped(stop) => {
                super::events::RecordedHarnessEvent::Stopped(super::events::RecordedStop {
                    native_session_id_option: stop
                        .native_session_id_option
                        .clone()
                        .map(signal_flow::NativeSessionId),
                    transcript_path_option: stop
                        .transcript_path_option
                        .clone()
                        .map(signal_flow::TranscriptPath),
                    background_work: signal_flow::BackgroundWork(Vec::new()),
                })
            }
            old_signal::Event::PromptSubmitted(prompt) => {
                super::events::RecordedHarnessEvent::PromptSubmitted(signal_flow::NativePrompt {
                    native_session_id_option: prompt
                        .native_session_id_option
                        .clone()
                        .map(signal_flow::NativeSessionId),
                    prompt_id_option: prompt.prompt_id_option.clone().map(signal_flow::PromptId),
                    prompt_sha256: signal_flow::PromptSha256(prompt.prompt_sha256.clone()),
                })
            }
            old_signal::Event::Ended(session) => super::events::RecordedHarnessEvent::Ended(
                session.clone().map(signal_flow::NativeSessionId),
            ),
            old_signal::Event::Cleared(session) => super::events::RecordedHarnessEvent::Cleared(
                session.clone().map(signal_flow::NativeSessionId),
            ),
        })
    }
}

impl BecomesCurrent for PriorFlowEvents {
    type Current = super::events::FlowEvents;
    fn current(&self) -> Result<super::events::FlowEvents, StoreError> {
        Ok(super::events::FlowEvents {
            flow_id: signal_flow::FlowId(self.flow_id.clone()),
            recorded_harness_event_vector: self
                .event_vector
                .iter()
                .map(BecomesCurrent::current)
                .collect::<Result<_, _>>()?,
        })
    }
}

impl BecomesCurrent for old_signal::Refusal {
    type Current = signal_flow::Refusal;
    fn current(&self) -> Result<signal_flow::Refusal, StoreError> {
        Ok(match self {
            old_signal::Refusal::Locked => signal_flow::Refusal::Locked,
            old_signal::Refusal::Held(lock) => {
                signal_flow::Refusal::Held(recode_unchanged!(lock, flow_ethos::Lock)?)
            }
            old_signal::Refusal::Awake(flow_id) => {
                signal_flow::Refusal::Awake(signal_flow::FlowId(flow_id.clone()))
            }
            old_signal::Refusal::Asleep => signal_flow::Refusal::Asleep,
            old_signal::Refusal::Lapsed => signal_flow::Refusal::Lapsed,
            old_signal::Refusal::Unknown(unknown) => signal_flow::Refusal::Unknown(match unknown {
                old_signal::Unknown_Data::Address(address) => {
                    signal_flow::Unknown_Data::Address(address.current()?)
                }
                old_signal::Unknown_Data::Lock(lock) => {
                    signal_flow::Unknown_Data::Lock(recode_unchanged!(lock, flow_ethos::Lock)?)
                }
                old_signal::Unknown_Data::FlowId(flow_id) => {
                    signal_flow::Unknown_Data::FlowId(signal_flow::FlowId(flow_id.clone()))
                }
                old_signal::Unknown_Data::Key(key) => {
                    signal_flow::Unknown_Data::Key(recode_unchanged!(key, flow_ethos::Key)?)
                }
            }),
            old_signal::Refusal::NotConfigured => signal_flow::Refusal::NotConfigured,
            old_signal::Refusal::NotMessage => signal_flow::Refusal::NotMessage,
            old_signal::Refusal::Taken(address) => signal_flow::Refusal::Taken(address.current()?),
            old_signal::Refusal::Unidentified(evidence) => signal_flow::Refusal::Unidentified(
                recode_unchanged!(evidence, signal_flow::SeatEvidence)?,
            ),
            old_signal::Refusal::Store(words) => signal_flow::Refusal::Store(words.clone()),
            old_signal::Refusal::NotSendServer(process) => signal_flow::Refusal::NotSendServer(
                recode_unchanged!(process, flow_ethos::Process)?,
            ),
            old_signal::Refusal::Oversized(size) => signal_flow::Refusal::Oversized(
                recode_unchanged!(size, signal_flow::SendPayloadSize)?,
            ),
            old_signal::Refusal::Direct(words) => signal_flow::Refusal::Direct(words.clone()),
            old_signal::Refusal::TopicRefused(topic) => {
                signal_flow::Refusal::TopicRefused(signal_flow::Topic(topic.clone()))
            }
            old_signal::Refusal::NoOpenTask(_)
            | old_signal::Refusal::Unresolved
            | old_signal::Refusal::Ambiguous => signal_flow::Refusal::Unresolved,
            old_signal::Refusal::MinutesRefused(minutes) => signal_flow::Refusal::MinutesRefused(
                recode_unchanged!(minutes, signal_flow::MinutesRefusal)?,
            ),
        })
    }
}

impl BecomesCurrent for old_signal::DeliveryState {
    type Current = signal_flow::DeliveryState;
    fn current(&self) -> Result<signal_flow::DeliveryState, StoreError> {
        Ok(match self {
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
                signal_flow::DeliveryState::Failed(refusal.current()?)
            }
            old_signal::DeliveryState::Withdrawn => signal_flow::DeliveryState::Withdrawn,
        })
    }
}

impl BecomesCurrentWithRequestId for old_signal::DeliverySnapshot {
    type Current = signal_flow::DeliverySnapshot;
    fn current_with_request_id(
        &self,
        request_id: RequestId,
    ) -> Result<signal_flow::DeliverySnapshot, StoreError> {
        Ok(signal_flow::DeliverySnapshot {
            delivery_cursor: signal_flow::DeliveryCursor(self.delivery_cursor),
            logical_delivery_id: signal_flow::LogicalDeliveryId(self.logical_delivery_id.clone()),
            request_id,
            sent_by: self.sent_by.current()?,
            address: self.address.current()?,
            delivery_state: self.delivery_state.current()?,
        })
    }
}

impl BecomesCurrentWithRequestIds for old_meta::QueueEvidence {
    type Current = meta_signal_flow::QueueEvidence;
    fn current_with_request_ids(
        &self,
        request_ids: &std::collections::BTreeMap<String, RequestId>,
    ) -> Result<meta_signal_flow::QueueEvidence, StoreError> {
        Ok(match self {
            old_meta::QueueEvidence::Logical(entry) => {
                let request_id = request_ids
                    .get(&entry.logical_delivery_id)
                    .cloned()
                    .ok_or(StoreError::StateInvariant)?;
                meta_signal_flow::QueueEvidence::Logical(entry.current_with_request_id(request_id)?)
            }
            old_meta::QueueEvidence::Legacy(legacy) => meta_signal_flow::QueueEvidence::Legacy(
                meta_signal_flow::QueueEvidence_Legacy_Data {
                    sent_by: legacy.sent_by.current()?,
                    request: legacy.request.current()?,
                },
            ),
        })
    }
}

impl BecomesCurrentWithRequestIds for PriorStoredDeliveryRequest {
    type Current = super::queue_delivery::StoredDeliveryRequest;
    fn current_with_request_ids(
        &self,
        request_ids: &std::collections::BTreeMap<String, RequestId>,
    ) -> Result<super::queue_delivery::StoredDeliveryRequest, StoreError> {
        Ok(super::queue_delivery::StoredDeliveryRequest {
            request_id: self.request_id.clone(),
            address: self.address.current()?,
            queue_evidence: self.queue_evidence.current_with_request_ids(request_ids)?,
            state: match self.state {
                PriorRequestState::Queued => super::queue_delivery::RequestState::Queued,
                PriorRequestState::Settled => super::queue_delivery::RequestState::Settled,
                PriorRequestState::Unsent => super::queue_delivery::RequestState::Unsent,
                PriorRequestState::Parked => super::queue_delivery::RequestState::Parked,
            },
        })
    }
}

impl From<&PriorNativeDeliveryReceipt> for super::queue_delivery::NativeDeliveryReceipt {
    fn from(value: &PriorNativeDeliveryReceipt) -> Self {
        super::queue_delivery::NativeDeliveryReceipt {
            native_request_id: value.native_request_id,
            thread_id: value.thread_id.clone(),
            turn_id: value.turn_id.clone(),
            native_receipt: value.native_receipt.clone(),
        }
    }
}

impl BecomesCurrentWithRequestIds for PriorQueueDeliveryAttempt {
    type Current = super::queue_delivery::QueueDeliveryAttempt;
    fn current_with_request_ids(
        &self,
        request_ids: &std::collections::BTreeMap<String, RequestId>,
    ) -> Result<super::queue_delivery::QueueDeliveryAttempt, StoreError> {
        use super::queue_delivery::{
            AbsenceRecord, NativeHistoryAnchor, NativeTranscriptCursor, QueueDeliveryAttempt,
            QueueDeliveryPhase,
        };
        let history_anchor = match &self.history_anchor {
            PriorNativeHistoryAnchor::ClaudeTranscript(cursor) => {
                NativeHistoryAnchor::ClaudeTranscript(NativeTranscriptCursor {
                    device: cursor.device,
                    inode: cursor.inode,
                    offset: cursor.offset,
                })
            }
            PriorNativeHistoryAnchor::CodexTurns(turn) => {
                NativeHistoryAnchor::CodexTurns(turn.clone())
            }
        };
        let phase = match &self.phase {
            PriorQueueDeliveryPhase::Prepared => QueueDeliveryPhase::Prepared,
            PriorQueueDeliveryPhase::Uncertain(undecided) => QueueDeliveryPhase::Uncertain(
                undecided
                    .as_ref()
                    .map(|value| recode_unchanged!(value, meta_signal_flow::Undecided))
                    .transpose()?,
            ),
            PriorQueueDeliveryPhase::ReceiptRecorded(receipt) => {
                QueueDeliveryPhase::ReceiptRecorded(receipt.into())
            }
            PriorQueueDeliveryPhase::Settled(receipt) => {
                QueueDeliveryPhase::Settled(receipt.into())
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
            request_id: self.request_id.clone(),
            attempt_id: self.attempt_id.clone(),
            native_request_id: self.native_request_id,
            flow_id: self.flow_id.clone(),
            thread_id: self.thread_id.clone(),
            harness_kind: recode_unchanged!(&self.harness_kind, flow_ethos::HarnessKind)?,
            history_anchor,
            address: self.address.current()?,
            queue_evidence: self.queue_evidence.current_with_request_ids(request_ids)?,
            phase,
        })
    }
}

impl BecomesCurrent for old_signal::SendFailureReason {
    type Current = signal_flow::SendFailureReason;
    fn current(&self) -> Result<signal_flow::SendFailureReason, StoreError> {
        Ok(match self {
            old_signal::SendFailureReason::RecipientUnavailable(address) => {
                signal_flow::SendFailureReason::RecipientUnavailable(address.current()?)
            }
            old_signal::SendFailureReason::QueueNotPermitted => {
                signal_flow::SendFailureReason::QueueNotPermitted
            }
            old_signal::SendFailureReason::Refused(refusal) => {
                signal_flow::SendFailureReason::Refused(refusal.current()?)
            }
            old_signal::SendFailureReason::ReceiptUncertain => {
                signal_flow::SendFailureReason::ReceiptUncertain
            }
            old_signal::SendFailureReason::NotSent(words) => {
                signal_flow::SendFailureReason::NotSent(words.clone())
            }
        })
    }
}

impl BecomesCurrentWithRequestId for old_signal::SendAcceptance {
    type Current = signal_flow::SendAcceptance;
    fn current_with_request_id(
        &self,
        request_id: RequestId,
    ) -> Result<signal_flow::SendAcceptance, StoreError> {
        Ok(signal_flow::SendAcceptance {
            logical_delivery_id: signal_flow::LogicalDeliveryId(self.logical_delivery_id.clone()),
            request_id,
            delivery_cursor: signal_flow::DeliveryCursor(self.delivery_cursor),
            delivery_state: self.delivery_state.current()?,
        })
    }
}

impl BecomesCurrentWithRequestIds for PriorStoredSendOutcome {
    type Current = super::send_outcome::StoredSendOutcome;
    fn current_with_request_ids(
        &self,
        request_ids: &std::collections::BTreeMap<String, RequestId>,
    ) -> Result<super::send_outcome::StoredSendOutcome, StoreError> {
        use meta_signal_flow::{OrdinarySendOutcome, PriorSendOutcome, SendOutcomeRecord};
        let old = &self.record;
        let ordinary_send_outcome = match &old.ordinary_send_outcome {
            old_meta::OrdinarySendOutcome::Accepted(acceptance) => {
                let id = request_ids
                    .get(&acceptance.logical_delivery_id)
                    .cloned()
                    .ok_or(StoreError::StateInvariant)?;
                OrdinarySendOutcome::Accepted(acceptance.current_with_request_id(id)?)
            }
            old_meta::OrdinarySendOutcome::Refused(refusal) => {
                OrdinarySendOutcome::Refused(refusal.current()?)
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
                    logical_delivery_id: signal_flow::LogicalDeliveryId(
                        receipt.logical_delivery_id.clone(),
                    ),
                    request_id: id,
                })
            }
            old_meta::OrdinarySendOutcome::Failed(failure) => {
                let reason = failure.send_failure_reason.current()?;
                OrdinarySendOutcome::Failed(match &failure.logical_delivery_id_option {
                    Some(logical_delivery_id) => {
                        let id = request_ids
                            .get(logical_delivery_id)
                            .cloned()
                            .ok_or(StoreError::StateInvariant)?;
                        signal_flow::SendFailure::Admitted(signal_flow::SendFailure_Admitted_Data {
                            logical_delivery_id: signal_flow::LogicalDeliveryId(
                                logical_delivery_id.clone(),
                            ),
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
                send_outcome_id: meta_signal_flow::SendOutcomeId(old.send_outcome_id.clone()),
                process_option: old
                    .process_option
                    .as_ref()
                    .map(|value| recode_unchanged!(value, signal_flow::Process))
                    .transpose()?,
                sent_by: old
                    .first_address_option
                    .as_ref()
                    .map(old_flow::Address::current)
                    .transpose()?
                    .map_or(signal_flow::SentBy::Unknown, signal_flow::SentBy::Sender),
                address_option: old
                    .second_address_option
                    .as_ref()
                    .map(old_flow::Address::current)
                    .transpose()?,
                request_digest: meta_signal_flow::RequestDigest(old.request_digest.clone()),
                ordinary_send_outcome,
            },
        })
    }
}

impl From<&crate::generated::send_tool_before_request_ids::ToolCallKey>
    for crate::generated::send_tool::ToolCallKey
{
    fn from(value: &crate::generated::send_tool_before_request_ids::ToolCallKey) -> Self {
        crate::generated::send_tool::ToolCallKey {
            thread_id: crate::generated::send_tool::ThreadId(value.thread_id.clone()),
            turn_id: crate::generated::send_tool::TurnId(value.turn_id.clone()),
            call_id: crate::generated::send_tool::CallId(value.call_id.clone()),
        }
    }
}

impl BecomesCurrent for crate::generated::send_tool_before_request_ids::SendRefusal {
    type Current = crate::generated::send_tool::SendRefusal;
    fn current(&self) -> Result<crate::generated::send_tool::SendRefusal, StoreError> {
        use crate::generated::{send_tool as current, send_tool_before_request_ids as old};
        Ok(match self {
            old::SendRefusal::UnknownTool => current::SendRefusal::UnknownTool,
            old::SendRefusal::Malformed => current::SendRefusal::Malformed,
            old::SendRefusal::ThreadUnowned => current::SendRefusal::ThreadUnowned,
            old::SendRefusal::FlowEnded => current::SendRefusal::FlowEnded,
            old::SendRefusal::NotAwake => current::SendRefusal::NotAwake,
            old::SendRefusal::KeyReused => current::SendRefusal::KeyReused,
            old::SendRefusal::UnknownRecipient(target) => current::SendRefusal::UnknownRecipient(
                crate::generated::send_tool::Target(target.current()?),
            ),
            old::SendRefusal::Held => current::SendRefusal::Held,
            old::SendRefusal::Oversized(size) => current::SendRefusal::Oversized(
                recode_unchanged!(size, signal_flow::SendPayloadSize)?,
            ),
            old::SendRefusal::Store => current::SendRefusal::Store,
            old::SendRefusal::TopicRefused(topic) => {
                current::SendRefusal::TopicRefused(signal_flow::Topic(topic.clone()))
            }
        })
    }
}

/// A predecessor send call recoded; an accepted call whose delivery the
/// evolution archived becomes Archived.
pub(crate) trait BecomesCurrentSendCall {
    fn current_send_call(
        &self,
        request_ids: &std::collections::BTreeMap<String, RequestId>,
        archived: bool,
    ) -> Result<crate::generated::send_tool::SendCall, StoreError>;
}

impl BecomesCurrentSendCall for PriorSendCall {
    fn current_send_call(
        &self,
        request_ids: &std::collections::BTreeMap<String, RequestId>,
        archived: bool,
    ) -> Result<crate::generated::send_tool::SendCall, StoreError> {
        use crate::generated::{send_tool as current, send_tool_before_request_ids as old};
        let send_outcome = match &self.send_outcome {
            old::SendOutcome::Accepted(accepted) if archived => {
                current::SendOutcome::Archived(current::Archived_Data {
                    sent_by: signal_flow::SentBy::Sender(accepted.sender.current()?),
                    target: crate::generated::send_tool::Target(accepted.target.current()?),
                })
            }
            old::SendOutcome::Accepted(accepted) => {
                let logical_delivery_id = &accepted.send_acceptance.logical_delivery_id;
                let request_id = request_ids
                    .get(logical_delivery_id)
                    .cloned()
                    .ok_or(StoreError::StateInvariant)?;
                current::SendOutcome::Accepted(current::Accepted_Data {
                    sent_by: signal_flow::SentBy::Sender(accepted.sender.current()?),
                    target: crate::generated::send_tool::Target(accepted.target.current()?),
                    send_acceptance: accepted
                        .send_acceptance
                        .current_with_request_id(request_id)?,
                })
            }
            old::SendOutcome::LegacyUnknown(unknown) => {
                current::SendOutcome::LegacyUnknown(current::LegacyUnknown_Data {
                    sent_by: signal_flow::SentBy::Sender(unknown.sender.current()?),
                    target: crate::generated::send_tool::Target(unknown.target.current()?),
                })
            }
            old::SendOutcome::Refused(refusal) => current::SendOutcome::Refused(refusal.current()?),
        };
        Ok(current::SendCall {
            tool_call_key: (&self.tool_call_key).into(),
            content_sha256: crate::generated::send_tool::ContentSha256(self.content_sha256.clone()),
            send_outcome,
        })
    }
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
    pub(crate) flow_events: TableDescriptor<super::events::FlowEvents>,
}

/// The logical delivery a predecessor row names, when it names one.
pub(crate) trait NamesPriorLogicalDelivery {
    fn prior_logical_id(&self) -> Option<&str>;
}

impl NamesPriorLogicalDelivery for old_meta::QueueEvidence {
    fn prior_logical_id(&self) -> Option<&str> {
        match self {
            old_meta::QueueEvidence::Logical(entry) => Some(&entry.logical_delivery_id),
            old_meta::QueueEvidence::Legacy(_) => None,
        }
    }
}

impl NamesPriorLogicalDelivery for PriorStoredSendOutcome {
    fn prior_logical_id(&self) -> Option<&str> {
        match &self.record.ordinary_send_outcome {
            old_meta::OrdinarySendOutcome::Accepted(value) => Some(&value.logical_delivery_id),
            old_meta::OrdinarySendOutcome::Sent(value) => Some(&value.logical_delivery_id),
            old_meta::OrdinarySendOutcome::Failed(value) => {
                value.logical_delivery_id_option.as_deref()
            }
            old_meta::OrdinarySendOutcome::Refused(_)
            | old_meta::OrdinarySendOutcome::PriorAdmission(_) => None,
        }
    }
}

impl NamesPriorLogicalDelivery for PriorSendCall {
    fn prior_logical_id(&self) -> Option<&str> {
        match &self.send_outcome {
            crate::generated::send_tool_before_request_ids::SendOutcome::Accepted(value) => {
                Some(&value.send_acceptance.logical_delivery_id)
            }
            _ => None,
        }
    }
}

/// What a predecessor delivery request asked, and who asked it.
pub(crate) trait ReadsPriorRequest {
    fn prior_request(&self) -> Option<&old_flow::Request>;
    fn prior_order_words(&self) -> Option<String>;
    fn prior_task_words(&self) -> Option<String>;
    fn prior_sender(&self) -> Option<&old_flow::Address>;
}

impl ReadsPriorRequest for PriorStoredDeliveryRequest {
    fn prior_request(&self) -> Option<&old_flow::Request> {
        match &self.queue_evidence {
            old_meta::QueueEvidence::Logical(entry) => Some(match &entry.message {
                old_flow::Message::HardAbrupt(request)
                | old_flow::Message::MiddleAbrupt(request)
                | old_flow::Message::Soft(request) => request,
            }),
            old_meta::QueueEvidence::Legacy(value) => Some(&value.request),
        }
    }

    fn prior_order_words(&self) -> Option<String> {
        match self.prior_request() {
            Some(old_flow::Request::Order(words)) => Some(words.clone()),
            _ => None,
        }
    }

    fn prior_task_words(&self) -> Option<String> {
        match self.prior_request() {
            Some(old_flow::Request::Task(task)) => Some(task.string.clone()),
            _ => None,
        }
    }

    fn prior_sender(&self) -> Option<&old_flow::Address> {
        match &self.queue_evidence {
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
}

/// A notice header, and the lines and parts a notice under it is bounded to,
/// so each part fits one prompt.
pub(crate) trait BoundsNoticeParts {
    fn render_bounded_line(&self, prefix: &str, words: &str) -> String;
    fn partition_rows<T: Clone>(&self, rows: impl IntoIterator<Item = (T, String)>) -> Vec<Vec<T>>;
}

impl BoundsNoticeParts for str {
    fn render_bounded_line(&self, prefix: &str, words: &str) -> String {
        let maximum = crate::sending::MAX_SEND_PAYLOAD_BYTES;
        let fixed = self.len().saturating_add(1).saturating_add(prefix.len());
        let available = maximum.saturating_sub(fixed);
        if words.len() <= available {
            return format!("{prefix}{words}");
        }
        const ELLIPSIS: &str = "…";
        let keep = available.saturating_sub(ELLIPSIS.len());
        let mut boundary = keep.min(words.len());
        while boundary > 0 && !words.is_char_boundary(boundary) {
            boundary -= 1;
        }
        format!("{prefix}{}{ELLIPSIS}", &words[..boundary])
    }

    fn partition_rows<T: Clone>(&self, rows: impl IntoIterator<Item = (T, String)>) -> Vec<Vec<T>> {
        let mut parts = Vec::new();
        let mut current = Vec::new();
        let mut bytes = self.len();
        for (item, line) in rows {
            let addition = 1usize.saturating_add(line.len());
            if !current.is_empty()
                && bytes.saturating_add(addition) > crate::sending::MAX_SEND_PAYLOAD_BYTES
            {
                parts.push(std::mem::take(&mut current));
                bytes = self.len();
            }
            bytes = bytes.saturating_add(addition);
            current.push(item);
        }
        if !current.is_empty() {
            parts.push(current);
        }
        parts
    }
}

/// Whether a predecessor attempt was still under way.
pub(crate) trait EndsPriorAttempt {
    fn is_unfinished(&self) -> bool;
}

impl EndsPriorAttempt for PriorQueueDeliveryAttempt {
    fn is_unfinished(&self) -> bool {
        matches!(
            self.phase,
            PriorQueueDeliveryPhase::Prepared
                | PriorQueueDeliveryPhase::Uncertain(_)
                | PriorQueueDeliveryPhase::ReceiptRecorded(_)
        )
    }
}

/// Whether a predecessor delivery state still held its delivery open.
pub(crate) trait HoldsPriorDelivery {
    fn is_held(&self) -> bool;
}

impl HoldsPriorDelivery for old_signal::DeliveryState {
    fn is_held(&self) -> bool {
        matches!(
            self,
            old_signal::DeliveryState::Queued
                | old_signal::DeliveryState::Attempting
                | old_signal::DeliveryState::Retryable(_)
                | old_signal::DeliveryState::Uncertain
        )
    }
}

/// The cursors that order one predecessor delivery's events.
pub(crate) trait OrdersPriorEvents {
    fn first_event_cursor(&self) -> Option<i64>;
    fn first_delivered_cursor(&self) -> Option<i64>;
}

impl OrdersPriorEvents for [PriorLogicalDeliveryEvent] {
    fn first_event_cursor(&self) -> Option<i64> {
        self.iter()
            .map(|event| event.snapshot.delivery_cursor)
            .min()
    }

    fn first_delivered_cursor(&self) -> Option<i64> {
        self.iter()
            .filter(|event| {
                matches!(
                    event.snapshot.delivery_state,
                    old_signal::DeliveryState::Delivered(_)
                )
            })
            .map(|event| event.snapshot.delivery_cursor)
            .min()
    }
}

/// Evolves the exact accepted 0.53.2 running Memory in one engine commit.
/// Every check and projection is completed before the commit is built.
pub(crate) trait EvolvesRequestIdMemory {
    #[allow(clippy::too_many_arguments)]
    fn evolve_request_id_memory(
        self,
        engine: &mut Engine,
        reminders: &[super::reminder::Reminder],
        simplified_replies: &[super::logical_delivery::SimplifiedReplyBinding],
        purposes: &[super::logical_delivery::LogicalDeliveryPurpose],
        heartbeat_table: TableReference<super::StoredHeartbeatStanding>,
        heartbeat_rows: &[super::StoredHeartbeatStanding],
        drawer: &dyn super::logical_delivery::DrawsRequestId,
    ) -> Result<(), StoreError>;
}

impl EvolvesRequestIdMemory for RequestIdEvolutionDescriptors {
    #[allow(clippy::too_many_arguments)]
    fn evolve_request_id_memory(
        self,
        engine: &mut Engine,
        reminders: &[super::reminder::Reminder],
        simplified_replies: &[super::logical_delivery::SimplifiedReplyBinding],
        purposes: &[super::logical_delivery::LogicalDeliveryPurpose],
        heartbeat_table: TableReference<super::StoredHeartbeatStanding>,
        heartbeat_rows: &[super::StoredHeartbeatStanding],
        drawer: &dyn super::logical_delivery::DrawsRequestId,
    ) -> Result<(), StoreError> {
        let descriptors = self;
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
        let flow_event_inventory = engine
            .inspect_predecessor::<PriorFlowEvents, super::events::FlowEvents>(
                &descriptors.flow_events,
            )?;
        let mut flow_event_rows = Vec::new();
        for (key, row) in flow_event_inventory.rows() {
            if *key != row.record_key() {
                return Err(StoreError::StateInvariant);
            }
            flow_event_rows.push(row.current()?);
        }

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
            if delivery_events.first_event_cursor().is_none()
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
            if let Some(logical_id) = row.queue_evidence.prior_logical_id()
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
            if let Some(logical_id) = row.queue_evidence.prior_logical_id()
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
            match row.prior_logical_id() {
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
            if let Some(logical_id) = row.prior_logical_id()
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
        for purpose in purposes {
            if !currents.contains_key(&purpose.logical_delivery_id) {
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
        let queue_is_live = |metaflow: &old_flow::Permaflow| -> Result<(), StoreError> {
            for entry in &metaflow.queue {
                let current = currents
                    .get(&entry.logical_delivery_id)
                    .ok_or(StoreError::StateInvariant)?;
                if !current.snapshot.delivery_state.is_held() {
                    return Err(StoreError::StateInvariant);
                }
            }
            Ok(())
        };
        for (_, metaflow) in metaflow_inventory.rows() {
            queue_is_live(&metaflow.metaflow)?;
        }
        // A launch hold carries the metaflow exactly as it was found.  Giving
        // that snapshot back would restore its queue, so it obeys the same live
        // queue invariant as the ordinary metaflow row.
        for (_, hold) in hold_inventory.rows() {
            if let Some(found) = &hold.found {
                queue_is_live(found)?;
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
            if current.snapshot.delivery_state.is_held()
                || attempts
                    .get(logical_id)
                    .is_some_and(|values| values.iter().any(|attempt| attempt.is_unfinished()))
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
                if id.0.len() != 4
                    || !id
                        .0
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
                request.prior_request()
                && held.contains(&done.logical_delivery_id)
            {
                closures.insert(
                    done.logical_delivery_id.clone(),
                    RequestClosure::Answered(signal_flow::LogicalDeliveryId(answer_id.clone())),
                );
            }
        }
        for binding in simplified_replies.iter().filter(|row| row.completed) {
            if held.contains(&binding.input_delivery_id) {
                closures.insert(
                    binding.input_delivery_id.clone(),
                    RequestClosure::Answered(LogicalDeliveryId(format!(
                        "simplified-result-{}-{}",
                        binding.flow_id, binding.input_delivery_id
                    ))),
                );
            }
        }

        // Reconstruct the 0.53.2 answer matcher by immutable delivery order.  Its
        // mutable current cursor is deliberately not an ordering key here.
        type PriorOrderGroup = (
            old_flow::Address,
            old_flow::Address,
            Vec<(String, Option<i64>, i64)>,
        );
        let mut order_groups: BTreeMap<String, PriorOrderGroup> = BTreeMap::new();
        let mut answer_groups: BTreeMap<String, Vec<(String, i64)>> = BTreeMap::new();
        for (logical_id, request) in &requests {
            let admission = events
                .get(logical_id)
                .ok_or(StoreError::StateInvariant)?
                .first_event_cursor()
                .ok_or(StoreError::StateInvariant)?;
            match request.prior_request() {
                Some(old_flow::Request::Order(words)) => {
                    let Some(sender) = request.prior_sender() else {
                        continue;
                    };
                    let recipient = &request.address;
                    order_groups
                        .entry(recipient.answer_group_key(sender, words))
                        .or_insert_with(|| (recipient.clone(), sender.clone(), Vec::new()))
                        .2
                        .push((
                            logical_id.clone(),
                            events[logical_id].first_delivered_cursor(),
                            admission,
                        ));
                }
                Some(old_flow::Request::Result(old_flow::ResultPayload::Answered(answer))) => {
                    let old_flow::Answerable::Order(words) = &answer.answerable else {
                        continue;
                    };
                    let Some(recipient) = request.prior_sender() else {
                        continue;
                    };
                    let sender = &request.address;
                    answer_groups
                        .entry(recipient.answer_group_key(sender, words))
                        .or_default()
                        .push((logical_id.clone(), admission));
                }
                _ => {}
            }
        }
        let mut assignment_decisions: BTreeMap<
            String,
            (Address, Vec<(String, AssignmentDecision)>),
        > = BTreeMap::new();
        for (group_key, (recipient, sender, mut orders)) in order_groups {
            let mut answers = answer_groups.remove(&group_key).unwrap_or_default();
            // Every historical answer closes the one earlier Order it matched,
            // including a singleton group.  Only the ambiguity report needs two
            // or more Orders; closure reconstruction is independent of it.
            let reports_ambiguity = orders.len() >= 2 && !answers.is_empty();
            orders.sort_by_key(|(_, delivered, admission)| {
                (
                    delivered.is_none(),
                    delivered.unwrap_or(*admission),
                    *admission,
                )
            });
            answers.sort_by_key(|(_, admission)| *admission);
            let mut used = BTreeSet::new();
            let mut assigned = Vec::new();
            for (answer, answer_key) in answers {
                let selected = orders
                    .iter()
                    .find(|(order, _, admission)| *admission < answer_key && !used.contains(order));
                let Some((order, _, _)) = selected else {
                    continue;
                };
                used.insert(order.clone());
                if held.contains(order) {
                    closures.insert(
                        order.clone(),
                        RequestClosure::Answered(signal_flow::LogicalDeliveryId(answer.clone())),
                    );
                }
                assigned.push(AssignedAnswer {
                    answer: flow_ethos::LogicalDeliveryId(answer),
                    order: flow_ethos::LogicalDeliveryId(order.clone()),
                    order_id: request_ids.get(order).cloned(),
                });
            }
            if !reports_ambiguity {
                continue;
            }
            let open = orders
                .iter()
                .filter(|(order, _, _)| held.contains(order) && !used.contains(order))
                .map(|(order, _, _)| OpenOrder {
                    order: flow_ethos::LogicalDeliveryId(order.clone()),
                    order_id: request_ids[order].clone(),
                })
                .collect();
            let sender = sender.current()?;
            let recipient = recipient.current()?;
            let first_order = orders
                .iter()
                .map(|(order, _, _)| order)
                .min()
                .cloned()
                .ok_or(StoreError::StateInvariant)?;
            assignment_decisions
                .entry(sender.current_address_key())
                .or_insert_with(|| (sender, Vec::new()))
                .1
                .push((
                    first_order,
                    AssignmentDecision {
                        recipient,
                        assigned,
                        open,
                    },
                ));
        }

        let current_rows = held
            .iter()
            .map(|logical_id| {
                let old = currents.get(logical_id).ok_or(StoreError::StateInvariant)?;
                Ok(CurrentLogicalDelivery {
                    snapshot: old.snapshot.current_with_request_id(
                        request_ids
                            .get(logical_id)
                            .cloned()
                            .ok_or(StoreError::StateInvariant)?,
                    )?,
                    archived: None,
                    admission: super::logical_delivery::DeliveryAdmission::HistoricalUnknown,
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
                    .map(move |event| (logical_id, event))
            })
            .map(|(logical_id, event)| {
                Ok(LogicalDeliveryEvent {
                    snapshot: event.snapshot.current_with_request_id(
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
                requests
                    .get(logical_id)
                    .ok_or(StoreError::StateInvariant)?
                    .current_with_request_ids(&request_ids)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let attempt_rows = held
            .iter()
            .flat_map(|logical_id| attempts.get(logical_id).into_iter().flatten())
            .map(|attempt| attempt.current_with_request_ids(&request_ids))
            .collect::<Result<Vec<_>, _>>()?;
        let mut outcome_rows = unbound_outcomes
            .iter()
            .map(|outcome| outcome.current_with_request_ids(&request_ids))
            .collect::<Result<Vec<_>, _>>()?;
        for logical_id in &held {
            for outcome in outcomes.get(logical_id).into_iter().flatten() {
                outcome_rows.push(outcome.current_with_request_ids(&request_ids)?);
            }
        }

        let metaflow_rows = metaflow_inventory
            .rows()
            .iter()
            .map(|(_, old)| {
                Ok(super::StoredMetaflow {
                    metaflow: old.metaflow.current_with_request_ids(&request_ids)?,
                })
            })
            .collect::<Result<Vec<_>, StoreError>>()?;
        let hold_rows = hold_inventory
            .rows()
            .iter()
            .map(|(_, old)| {
                Ok(super::address_hold::AddressHold {
                    flow_id: old.flow_id.clone(),
                    address: old.address.current()?,
                    found: old
                        .found
                        .as_ref()
                        .map(|value| value.current_with_request_ids(&request_ids))
                        .transpose()?,
                })
            })
            .collect::<Result<Vec<_>, StoreError>>()?;
        let call_rows = call_inventory
            .rows()
            .iter()
            .map(|(_, call)| {
                let archived = call.prior_logical_id().is_some_and(|id| !held.contains(id));
                call.current_send_call(&request_ids, archived)
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
                logical_delivery_id: signal_flow::LogicalDeliveryId(logical_id.clone()),
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
                    .entry(reminder.recipient.current_address_key())
                    .or_insert_with(|| (reminder.recipient.clone(), Vec::new()))
                    .1
                    .push(reminder.key.clone());
            }
        }
        let mut task_announcements = Vec::new();
        for (_, (recipient, mut tasks)) in announcement_groups {
            tasks.sort();
            let rows = tasks
                .into_iter()
                .map(|logical_id| {
                    let words = requests
                        .get(&logical_id)
                        .ok_or(StoreError::StateInvariant)?
                        .prior_task_words()
                        .ok_or(StoreError::StateInvariant)?;
                    let line = TASK_ANNOUNCEMENT_HEADER
                        .render_bounded_line(&format!("{LONGEST_TASK_ID} "), &words);
                    Ok((flow_ethos::LogicalDeliveryId(logical_id), line))
                })
                .collect::<Result<Vec<_>, StoreError>>()?;
            for (part, tasks) in TASK_ANNOUNCEMENT_HEADER
                .partition_rows(rows)
                .into_iter()
                .enumerate()
            {
                task_announcements.push(TaskAnnouncement {
                    recipient: recipient.clone(),
                    part: u64::try_from(part).map_err(|_| StoreError::StateInvariant)?,
                    tasks,
                    tries: 0,
                    status: TaskAnnouncementStatus::Pending(None),
                });
            }
        }
        let mut upgrade_assignments = Vec::new();
        for (_, (sender, mut decisions)) in assignment_decisions {
            decisions.sort_by(|left, right| left.0.cmp(&right.0));
            let mut rows = Vec::new();
            for (_, decision) in decisions {
                for assigned in decision.assigned {
                    let words = requests
                        .get(&assigned.order.0)
                        .ok_or(StoreError::StateInvariant)?
                        .prior_order_words()
                        .ok_or(StoreError::StateInvariant)?;
                    let line = ASSIGNMENT_HEADER
                        .render_bounded_line(&format!("{LONGEST_ASSIGNMENT_ID} answered "), &words);
                    rows.push((
                        AssignmentDecision {
                            recipient: decision.recipient.clone(),
                            assigned: vec![assigned],
                            open: Vec::new(),
                        },
                        line,
                    ));
                }
                for open in decision.open {
                    let words = requests
                        .get(&open.order.0)
                        .ok_or(StoreError::StateInvariant)?
                        .prior_order_words()
                        .ok_or(StoreError::StateInvariant)?;
                    let line = ASSIGNMENT_HEADER
                        .render_bounded_line(&format!("{LONGEST_ASSIGNMENT_ID} open "), &words);
                    rows.push((
                        AssignmentDecision {
                            recipient: decision.recipient.clone(),
                            assigned: Vec::new(),
                            open: vec![open],
                        },
                        line,
                    ));
                }
            }
            for (part, decisions) in ASSIGNMENT_HEADER
                .partition_rows(rows)
                .into_iter()
                .enumerate()
            {
                upgrade_assignments.push(UpgradeAssignment {
                    sender: sender.clone(),
                    part: u64::try_from(part).map_err(|_| StoreError::StateInvariant)?,
                    decisions,
                    tries: 0,
                    status: TaskAnnouncementStatus::Pending(None),
                });
            }
        }

        let mut identity = String::with_capacity(16);
        for _ in 0..4 {
            identity.push_str(&drawer.draw_request_id()?.0);
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
            .evolve(
                flow_event_inventory,
                descriptors.flow_events,
                flow_event_rows,
            )
            .register(descriptors.request_ids, binding_rows)
            .register(descriptors.deliver_calls, Vec::new())
            .register(
                descriptors.store_identity,
                vec![StoredStoreIdentity {
                    store_identity: signal_flow::StoreIdentity(identity),
                }],
            )
            .register(descriptors.prior_deliveries, prior_rows)
            .register(descriptors.task_announcements, task_announcements)
            .register(descriptors.upgrade_assignments, upgrade_assignments)
            .register(
                descriptors.request_policy,
                vec![super::StoredMetaRequestPolicyConfiguration {
                    request_id_draws: meta_signal_flow::DrawBound(super::SEEDED_REQUEST_ID_DRAWS),
                    announcement_tries: meta_signal_flow::TryBound(
                        super::SEEDED_ANNOUNCEMENT_TRIES,
                    ),
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::address_hold::DescribesAddressHoldTable;
    use crate::store::logical_delivery::{
        DeclaresLogicalDeliveryTables, DrawsRequestId, LogicalDeliveryTables,
    };
    use crate::store::queue_delivery::{DeclaresQueueDeliveryTables, QueueDeliveryTables};
    use crate::store::send_call::DescribesSendCallTable;
    use crate::store::send_outcome::{DeclaresSendOutcomeTables, SendOutcomeTables};
    use crate::store::{
        DescribesHeartbeatStandingTable, DescribesRequestPolicyTable, StoredHeartbeatStanding,
        StoredMetaRequestPolicyConfiguration,
    };
    use sema_engine::{Assertion, EngineOpen, QueryPlan, SchemaVersion};
    use std::collections::VecDeque;
    use std::sync::Mutex;

    struct ScriptedDrawer(Mutex<VecDeque<RequestId>>);

    impl ScriptedDrawer {
        fn new(values: &[&str]) -> Self {
            Self(Mutex::new(
                values
                    .iter()
                    .map(|value| flow_ethos::RequestId((*value).into()))
                    .collect(),
            ))
        }
    }

    impl DrawsRequestId for ScriptedDrawer {
        fn draw_request_id(&self) -> Result<RequestId, StoreError> {
            self.0
                .lock()
                .map_err(|_| StoreError::StateInvariant)?
                .pop_front()
                .ok_or(StoreError::StateInvariant)
        }
    }

    fn predecessor_descriptor<Predecessor, Current>(
        current: &TableDescriptor<Current>,
    ) -> TableDescriptor<Predecessor>
    where
        Predecessor: MemoryDefinition,
    {
        TableDescriptor::new(
            *current.name(),
            current.family().clone(),
            Predecessor::schema_hash(),
        )
    }

    fn evolution_descriptors() -> RequestIdEvolutionDescriptors {
        let logical = LogicalDeliveryTables::descriptors();
        let queue = QueueDeliveryTables::descriptors();
        let outcomes = SendOutcomeTables::descriptors();
        let upgrade = RequestIdUpgradeTables::descriptors();
        RequestIdEvolutionDescriptors {
            metaflows: <crate::store::FlowStore as crate::store::DescribesMetaflowTable>::metaflow_descriptor(),
            address_holds: crate::store::address_hold::AddressHold::descriptor(),
            send_calls: crate::generated::send_tool::SendCall::descriptor(),
            requests: queue.1,
            attempts: queue.2,
            outcomes: outcomes.outcomes,
            current: logical.current,
            events: logical.events,
            request_ids: logical.request_ids,
            deliver_calls: logical.deliver_calls,
            store_identity: logical.store_identity,
            prior_deliveries: upgrade.prior_deliveries,
            task_announcements: upgrade.task_announcements,
            upgrade_assignments: upgrade.upgrade_assignments,
            request_policy: StoredMetaRequestPolicyConfiguration::descriptor(),
            flow_events: <crate::store::events::FlowEvents as crate::store::events::DescribesEventTable>::descriptor(),
        }
    }

    struct OldTables {
        metaflows: TableReference<PriorStoredMetaflow>,
        address_holds: TableReference<PriorAddressHold>,
        current: TableReference<PriorCurrentLogicalDelivery>,
        events: TableReference<PriorLogicalDeliveryEvent>,
        requests: TableReference<PriorStoredDeliveryRequest>,
        attempts: TableReference<PriorQueueDeliveryAttempt>,
        outcomes: TableReference<PriorStoredSendOutcome>,
        send_calls: TableReference<PriorSendCall>,
        flow_events: TableReference<PriorFlowEvents>,
    }

    fn old_engine(
        path: &std::path::Path,
        descriptors: &RequestIdEvolutionDescriptors,
        heartbeat: &TableDescriptor<StoredHeartbeatStanding>,
    ) -> (Engine, OldTables) {
        let mut engine = Engine::open(EngineOpen::new(
            path,
            SchemaVersion::new(crate::store::FLOW_SCHEMA_VERSION),
        ))
        .expect("predecessor Memory opens");
        let current = engine
            .register_table(predecessor_descriptor::<PriorCurrentLogicalDelivery, _>(
                &descriptors.current,
            ))
            .expect("old current registers");
        let events = engine
            .register_table(predecessor_descriptor::<PriorLogicalDeliveryEvent, _>(
                &descriptors.events,
            ))
            .expect("old events register");
        let metaflows = engine
            .register_table(predecessor_descriptor::<PriorStoredMetaflow, _>(
                &descriptors.metaflows,
            ))
            .expect("old metaflows register");
        let address_holds = engine
            .register_table(predecessor_descriptor::<PriorAddressHold, _>(
                &descriptors.address_holds,
            ))
            .expect("old holds register");
        let requests = engine
            .register_table(predecessor_descriptor::<PriorStoredDeliveryRequest, _>(
                &descriptors.requests,
            ))
            .expect("old requests register");
        let attempts = engine
            .register_table(predecessor_descriptor::<PriorQueueDeliveryAttempt, _>(
                &descriptors.attempts,
            ))
            .expect("old attempts register");
        let outcomes = engine
            .register_table(predecessor_descriptor::<PriorStoredSendOutcome, _>(
                &descriptors.outcomes,
            ))
            .expect("old outcomes register");
        let send_calls = engine
            .register_table(predecessor_descriptor::<PriorSendCall, _>(
                &descriptors.send_calls,
            ))
            .expect("old calls register");
        let flow_events = engine
            .register_table(predecessor_descriptor::<PriorFlowEvents, _>(
                &descriptors.flow_events,
            ))
            .expect("old flow events register");
        engine
            .register_table(heartbeat.clone())
            .expect("unchanged heartbeat registers");
        (
            engine,
            OldTables {
                metaflows,
                address_holds,
                current,
                events,
                requests,
                attempts,
                outcomes,
                send_calls,
                flow_events,
            },
        )
    }

    fn old_address(aspect: old_flow::Aspect, topic: &str) -> old_flow::Address {
        old_flow::Address {
            aspect,
            topic: topic.into(),
            model: old_flow::Model::Sol,
        }
    }

    fn old_snapshot(
        cursor: i64,
        logical_id: &str,
        sender: &old_flow::Address,
        recipient: &old_flow::Address,
        state: old_signal::DeliveryState,
    ) -> old_signal::DeliverySnapshot {
        old_signal::DeliverySnapshot {
            delivery_cursor: cursor,
            logical_delivery_id: logical_id.into(),
            sent_by: old_flow::SentBy::Sender(sender.clone()),
            address: recipient.clone(),
            delivery_state: state,
        }
    }

    fn old_request(
        logical_id: &str,
        sender: &old_flow::Address,
        recipient: &old_flow::Address,
        request: old_flow::Request,
        state: PriorRequestState,
    ) -> PriorStoredDeliveryRequest {
        PriorStoredDeliveryRequest {
            request_id: logical_id.into(),
            address: recipient.clone(),
            queue_evidence: old_meta::QueueEvidence::Logical(old_flow::QueueEntry {
                logical_delivery_id: logical_id.into(),
                sent_by: old_flow::SentBy::Sender(sender.clone()),
                delivery: old_flow::Delivery::Queue,
                message: old_flow::Message::Soft(request),
            }),
            state,
        }
    }

    #[test]
    fn upgrade_report_lines_are_utf8_safe_and_parts_stay_inside_the_prompt_budget() {
        let words = "é".repeat(crate::sending::MAX_SEND_PAYLOAD_BYTES);
        let first = TASK_ANNOUNCEMENT_HEADER.render_bounded_line("0000 ", &words);
        assert!(first.ends_with('…'));
        assert!(
            TASK_ANNOUNCEMENT_HEADER.len() + 1 + first.len()
                <= crate::sending::MAX_SEND_PAYLOAD_BYTES
        );
        let second = TASK_ANNOUNCEMENT_HEADER.render_bounded_line("0001 ", &words);
        let parts = TASK_ANNOUNCEMENT_HEADER.partition_rows([("first", first), ("second", second)]);
        assert_eq!(parts, vec![vec!["first"], vec!["second"]]);
    }

    /// A 0.53.2 Memory's harness events (events v4) evolve with the rest of
    /// it into events v6: every stored Stop carries no background work, and
    /// every other event, and their order, is kept.
    #[test]
    fn the_events_migration_converts_a_stored_v4_stop() {
        use crate::store::events::FlowEvents;

        let directory = tempfile::tempdir().expect("temporary Memory");
        let path = directory.path().join("flow.sema");
        let descriptors = evolution_descriptors();
        let heartbeat = StoredHeartbeatStanding::descriptor();
        let (predecessor, tables) = old_engine(&path, &descriptors, &heartbeat);
        let stop = old_signal::Event::Stopped(old_signal::NativeStop {
            native_session_id_option: Some("c0ffee00-0000-4000-8000-000000000001".into()),
            transcript_path_option: Some("/t/c0ffee00.jsonl".into()),
            last_assistant_message_option: Some("done".into()),
        });
        predecessor
            .assert(Assertion::new(
                tables.flow_events,
                PriorFlowEvents {
                    flow_id: "5a4d0b".into(),
                    event_vector: vec![
                        old_signal::Event::Started(Some(
                            "c0ffee00-0000-4000-8000-000000000001".into(),
                        )),
                        old_signal::Event::ToolUsed("Bash".into()),
                        stop,
                        old_signal::Event::PromptSubmitted(old_signal::NativePrompt {
                            native_session_id_option: Some(
                                "c0ffee00-0000-4000-8000-000000000001".into(),
                            ),
                            prompt_id_option: None,
                            prompt_sha256: "ab12".into(),
                        }),
                        old_signal::Event::Cleared(None),
                    ],
                },
            ))
            .expect("old flow events store");
        drop(predecessor);

        let mut engine = Engine::open(
            EngineOpen::new(&path, SchemaVersion::new(crate::store::FLOW_SCHEMA_VERSION))
                .with_family(&descriptors.current)
                .with_family(&descriptors.events)
                .with_family(&descriptors.metaflows)
                .with_family(&descriptors.address_holds)
                .with_family(&descriptors.requests)
                .with_family(&descriptors.attempts)
                .with_family(&descriptors.outcomes)
                .with_family(&descriptors.send_calls)
                .with_family(&descriptors.flow_events)
                .with_family(&heartbeat),
        )
        .expect("predecessor catalog opens for evolution");
        let heartbeat_table = engine.register_table(heartbeat).expect("heartbeat opens");
        let flow_events = descriptors.flow_events.clone();
        descriptors
            .evolve_request_id_memory(
                &mut engine,
                &[],
                &[],
                &[],
                heartbeat_table,
                &[],
                &ScriptedDrawer::new(&["a001", "a002", "a003", "a004"]),
            )
            .expect("events evolution commits");
        let table = engine
            .register_table(flow_events)
            .expect("events v6 register");
        let rows = engine
            .match_records(QueryPlan::all(table))
            .expect("events read");
        let [row] = rows.records() else {
            panic!("one flow's events")
        };
        assert_eq!(
            *row,
            FlowEvents {
                flow_id: signal_flow::FlowId("5a4d0b".into()),
                recorded_harness_event_vector: vec![
                    super::super::events::RecordedHarnessEvent::Started(Some(
                        signal_flow::NativeSessionId("c0ffee00-0000-4000-8000-000000000001".into())
                    )),
                    super::super::events::RecordedHarnessEvent::ToolUsed("Bash".into()),
                    super::super::events::RecordedHarnessEvent::Stopped(
                        super::super::events::RecordedStop {
                            native_session_id_option: Some(signal_flow::NativeSessionId(
                                "c0ffee00-0000-4000-8000-000000000001".into()
                            )),
                            transcript_path_option: Some(signal_flow::TranscriptPath(
                                "/t/c0ffee00.jsonl".into()
                            )),
                            background_work: signal_flow::BackgroundWork(vec![]),
                        }
                    ),
                    super::super::events::RecordedHarnessEvent::PromptSubmitted(
                        signal_flow::NativePrompt {
                            native_session_id_option: Some(signal_flow::NativeSessionId(
                                "c0ffee00-0000-4000-8000-000000000001".into()
                            )),
                            prompt_id_option: None,
                            prompt_sha256: signal_flow::PromptSha256("ab12".into()),
                        }
                    ),
                    super::super::events::RecordedHarnessEvent::Cleared(None),
                ],
            }
        );
    }

    #[test]
    fn a_single_uncertain_order_with_a_historical_result_evolves_closed() {
        use crate::store::logical_delivery::{OpensAnswerBinding, RequestClosure};

        let directory = tempfile::tempdir().expect("temporary Memory");
        let path = directory.path().join("flow.sema");
        let descriptors = evolution_descriptors();
        let heartbeat = StoredHeartbeatStanding::descriptor();
        let (predecessor, tables) = old_engine(&path, &descriptors, &heartbeat);
        let sender = old_address(old_flow::Aspect::Mind, "singletonSender");
        let recipient = old_address(old_flow::Aspect::Field, "singletonRecipient");
        let native = old_signal::NativeReceipt {
            harness_kind: old_flow::HarnessKind::Codex,
            native_session_id: "answer-session".into(),
            native_prompt_id: "answer-turn".into(),
        };
        let order_queued = old_snapshot(
            1,
            "singleton-order",
            &sender,
            &recipient,
            old_signal::DeliveryState::Queued,
        );
        let order_uncertain = old_snapshot(
            2,
            "singleton-order",
            &sender,
            &recipient,
            old_signal::DeliveryState::Uncertain,
        );
        let answer_queued = old_snapshot(
            3,
            "singleton-answer",
            &recipient,
            &sender,
            old_signal::DeliveryState::Queued,
        );
        let answer_delivered = old_snapshot(
            4,
            "singleton-answer",
            &recipient,
            &sender,
            old_signal::DeliveryState::Delivered(native),
        );
        for snapshot in [order_uncertain.clone(), answer_delivered.clone()] {
            predecessor
                .assert(Assertion::new(
                    tables.current,
                    PriorCurrentLogicalDelivery { snapshot },
                ))
                .expect("old current stores");
        }
        for snapshot in [
            order_queued,
            order_uncertain,
            answer_queued,
            answer_delivered,
        ] {
            predecessor
                .assert(Assertion::new(
                    tables.events,
                    PriorLogicalDeliveryEvent { snapshot },
                ))
                .expect("old event stores");
        }
        for request in [
            old_request(
                "singleton-order",
                &sender,
                &recipient,
                old_flow::Request::Order("one historical order".into()),
                PriorRequestState::Settled,
            ),
            old_request(
                "singleton-answer",
                &recipient,
                &sender,
                old_flow::Request::Result(old_flow::ResultPayload::Answered(
                    old_flow::ResultPayload_Answered_Data {
                        answerable: old_flow::Answerable::Order("one historical order".into()),
                        string: "already answered".into(),
                    },
                )),
                PriorRequestState::Settled,
            ),
        ] {
            predecessor
                .assert(Assertion::new(tables.requests, request))
                .expect("old request stores");
        }
        drop(predecessor);

        let mut engine = Engine::open(
            EngineOpen::new(&path, SchemaVersion::new(crate::store::FLOW_SCHEMA_VERSION))
                .with_family(&descriptors.current)
                .with_family(&descriptors.events)
                .with_family(&descriptors.metaflows)
                .with_family(&descriptors.address_holds)
                .with_family(&descriptors.requests)
                .with_family(&descriptors.attempts)
                .with_family(&descriptors.outcomes)
                .with_family(&descriptors.send_calls)
                .with_family(&descriptors.flow_events)
                .with_family(&heartbeat),
        )
        .expect("predecessor catalog opens for evolution");
        let heartbeat_table = engine.register_table(heartbeat).expect("heartbeat opens");
        descriptors
            .evolve_request_id_memory(
                &mut engine,
                &[],
                &[],
                &[],
                heartbeat_table,
                &[],
                &ScriptedDrawer::new(&["s001", "0000", "0001", "0002", "0003"]),
            )
            .expect("singleton evolution commits");

        let binding_table = engine
            .register_table(evolution_descriptors().request_ids)
            .expect("request bindings register");
        let before = engine
            .match_records(QueryPlan::all(binding_table))
            .expect("request bindings read")
            .records()
            .to_vec();
        let [binding] = before.as_slice() else {
            panic!("only the held singleton Order receives a request id")
        };
        assert_eq!(binding.request_id, RequestId("s001".into()));
        assert_eq!(
            binding.closure,
            RequestClosure::Answered(LogicalDeliveryId("singleton-answer".into()))
        );
        assert_eq!(
            Some(binding.clone()).open_answer_binding(),
            Err(signal_flow::Refusal::Unresolved),
            "a duplicate Result after upgrade is unresolved"
        );
        assert_eq!(
            engine
                .match_records(QueryPlan::all(binding_table))
                .expect("request bindings reread")
                .records(),
            before,
            "duplicate resolution is a pure refusal and writes nothing"
        );
    }

    #[test]
    fn an_address_hold_cannot_restore_an_ended_queue_entry_retained_by_an_attempt() {
        let directory = tempfile::tempdir().expect("temporary Memory");
        let path = directory.path().join("flow.sema");
        let descriptors = evolution_descriptors();
        let heartbeat = StoredHeartbeatStanding::descriptor();
        let (predecessor, tables) = old_engine(&path, &descriptors, &heartbeat);
        let sender = old_address(old_flow::Aspect::Mind, "holdSender");
        let recipient = old_address(old_flow::Aspect::Field, "holdRecipient");
        let native = old_signal::NativeReceipt {
            harness_kind: old_flow::HarnessKind::Codex,
            native_session_id: "ended-session".into(),
            native_prompt_id: "ended-turn".into(),
        };
        let snapshot = old_snapshot(
            1,
            "ended-but-attempting",
            &sender,
            &recipient,
            old_signal::DeliveryState::Delivered(native),
        );
        let request = old_request(
            "ended-but-attempting",
            &sender,
            &recipient,
            old_flow::Request::Order("never restore this queue entry".into()),
            PriorRequestState::Settled,
        );
        let old_meta::QueueEvidence::Logical(queue_entry) = request.queue_evidence.clone() else {
            panic!("the authentic old request carries its queue entry")
        };
        predecessor
            .assert(Assertion::new(
                tables.current,
                PriorCurrentLogicalDelivery {
                    snapshot: snapshot.clone(),
                },
            ))
            .expect("old current stores");
        predecessor
            .assert(Assertion::new(
                tables.events,
                PriorLogicalDeliveryEvent { snapshot },
            ))
            .expect("old event stores");
        predecessor
            .assert(Assertion::new(tables.requests, request.clone()))
            .expect("old request stores");
        predecessor
            .assert(Assertion::new(
                tables.attempts,
                PriorQueueDeliveryAttempt {
                    request_id: "ended-but-attempting".into(),
                    attempt_id: "unfinished-attempt".into(),
                    native_request_id: 7,
                    flow_id: "held-flow".into(),
                    thread_id: "held-thread".into(),
                    harness_kind: old_flow::HarnessKind::Codex,
                    history_anchor: PriorNativeHistoryAnchor::CodexTurns(None),
                    address: recipient.clone(),
                    queue_evidence: request.queue_evidence,
                    phase: PriorQueueDeliveryPhase::Prepared,
                },
            ))
            .expect("unfinished old attempt stores");
        predecessor
            .assert(Assertion::new(
                tables.address_holds,
                PriorAddressHold {
                    flow_id: "held-flow".into(),
                    address: recipient.clone(),
                    found: Some(old_flow::Permaflow {
                        address: recipient,
                        permaflow_state: old_flow::State::Asleep,
                        past: Vec::new(),
                        queue: vec![queue_entry],
                    }),
                },
            ))
            .expect("old address hold stores");
        drop(predecessor);

        let mut engine = Engine::open(
            EngineOpen::new(&path, SchemaVersion::new(crate::store::FLOW_SCHEMA_VERSION))
                .with_family(&descriptors.current)
                .with_family(&descriptors.events)
                .with_family(&descriptors.metaflows)
                .with_family(&descriptors.address_holds)
                .with_family(&descriptors.requests)
                .with_family(&descriptors.attempts)
                .with_family(&descriptors.outcomes)
                .with_family(&descriptors.send_calls)
                .with_family(&descriptors.flow_events)
                .with_family(&heartbeat),
        )
        .expect("predecessor catalog opens for validation");
        let heartbeat_table = engine.register_table(heartbeat).expect("heartbeat opens");
        // Opening the predecessor and registering its existing heartbeat
        // family may advance engine bookkeeping.  The refusal oracle begins
        // after that ordinary open work, immediately before evolution.
        let before = std::fs::read(&path).expect("Memory reads before refusal");
        assert!(matches!(
            descriptors.evolve_request_id_memory(
                &mut engine,
                &[],
                &[],
                &[],
                heartbeat_table,
                &[],
                &ScriptedDrawer::new(&["h001"]),
            ),
            Err(StoreError::StateInvariant)
        ));
        assert_eq!(
            std::fs::read(&path).expect("Memory reads after refusal"),
            before,
            "the rejected address-hold queue leaves predecessor Memory unchanged"
        );
    }

    #[test]
    fn evolution_uses_first_delivery_order_and_keeps_original_order_words() {
        let directory = tempfile::tempdir().expect("temporary Memory");
        let path = directory.path().join("flow.sema");
        let descriptors = evolution_descriptors();
        let heartbeat = StoredHeartbeatStanding::descriptor();
        let (predecessor, tables) = old_engine(&path, &descriptors, &heartbeat);
        let sender = old_address(old_flow::Aspect::Mind, "sender");
        let recipient = old_address(old_flow::Aspect::Field, "recipient");
        let native = old_signal::NativeReceipt {
            harness_kind: old_flow::HarnessKind::Codex,
            native_session_id: "session".into(),
            native_prompt_id: "prompt".into(),
        };
        let first_queued = old_snapshot(
            1,
            "order-first",
            &sender,
            &recipient,
            old_signal::DeliveryState::Queued,
        );
        let first_delivered = old_snapshot(
            2,
            "order-first",
            &sender,
            &recipient,
            old_signal::DeliveryState::Delivered(native.clone()),
        );
        let second_queued = old_snapshot(
            3,
            "order-second",
            &sender,
            &recipient,
            old_signal::DeliveryState::Queued,
        );
        let answer_queued = old_snapshot(
            4,
            "answer",
            &recipient,
            &sender,
            old_signal::DeliveryState::Queued,
        );
        let answer_delivered = old_snapshot(
            5,
            "answer",
            &recipient,
            &sender,
            old_signal::DeliveryState::Delivered(native),
        );
        for snapshot in [
            first_delivered.clone(),
            second_queued.clone(),
            answer_delivered.clone(),
        ] {
            predecessor
                .assert(Assertion::new(
                    tables.current,
                    PriorCurrentLogicalDelivery { snapshot },
                ))
                .expect("old current stores");
        }
        for snapshot in [
            first_queued,
            first_delivered,
            second_queued,
            answer_queued,
            answer_delivered,
        ] {
            predecessor
                .assert(Assertion::new(
                    tables.events,
                    PriorLogicalDeliveryEvent { snapshot },
                ))
                .expect("old event stores");
        }
        for request in [
            old_request(
                "order-first",
                &sender,
                &recipient,
                old_flow::Request::Order("same original words".into()),
                PriorRequestState::Settled,
            ),
            old_request(
                "order-second",
                &sender,
                &recipient,
                old_flow::Request::Order("same original words".into()),
                PriorRequestState::Queued,
            ),
            old_request(
                "answer",
                &recipient,
                &sender,
                old_flow::Request::Result(old_flow::ResultPayload::Answered(
                    old_flow::ResultPayload_Answered_Data {
                        answerable: old_flow::Answerable::Order("same original words".into()),
                        string: "answer words".into(),
                    },
                )),
                PriorRequestState::Settled,
            ),
        ] {
            predecessor
                .assert(Assertion::new(tables.requests, request))
                .expect("old request stores");
        }
        drop(predecessor);

        let mut engine = Engine::open(
            EngineOpen::new(&path, SchemaVersion::new(crate::store::FLOW_SCHEMA_VERSION))
                .with_family(&descriptors.current)
                .with_family(&descriptors.events)
                .with_family(&descriptors.metaflows)
                .with_family(&descriptors.address_holds)
                .with_family(&descriptors.requests)
                .with_family(&descriptors.attempts)
                .with_family(&descriptors.outcomes)
                .with_family(&descriptors.send_calls)
                .with_family(&descriptors.flow_events)
                .with_family(&heartbeat),
        )
        .expect("predecessor catalog opens for evolution");
        let heartbeat_table = engine.register_table(heartbeat).expect("heartbeat opens");
        descriptors
            .evolve_request_id_memory(
                &mut engine,
                &[],
                &[],
                &[],
                heartbeat_table,
                &[],
                &ScriptedDrawer::new(&["a001", "0000", "0001", "0002", "0003"]),
            )
            .expect("R6 evolution commits");
        let descriptors = evolution_descriptors();
        let assignment_table = engine
            .register_table(descriptors.upgrade_assignments)
            .expect("assignments register");
        let assignments = engine
            .match_records(QueryPlan::all(assignment_table))
            .expect("assignments read");
        let [assignment] = assignments.records() else {
            panic!("one assignment part")
        };
        assert_eq!(assignment.decisions.len(), 2);
        assert_eq!(
            assignment.decisions[0].assigned[0].order,
            LogicalDeliveryId("order-first".into())
        );
        assert_eq!(assignment.decisions[0].assigned[0].order_id, None);
        assert_eq!(
            assignment.decisions[1].open[0].order,
            LogicalDeliveryId("order-second".into())
        );
        assert_eq!(
            assignment.decisions[1].open[0].order_id,
            RequestId("a001".into())
        );
        let prior_table = engine
            .register_table(descriptors.prior_deliveries)
            .expect("prior deliveries register");
        let prior = engine
            .match_records(QueryPlan::all(prior_table))
            .expect("prior deliveries read");
        assert_eq!(prior.records().len(), 2);
        assert_eq!(
            prior
                .records()
                .iter()
                .find(|row| row.logical_delivery_id == "order-first")
                .expect("first Order archived")
                .request
                .prior_order_words()
                .as_deref(),
            Some("same original words")
        );
        let current_table = engine
            .register_table(descriptors.current)
            .expect("current deliveries register");
        let evolved_current = engine
            .match_records(QueryPlan::all(current_table))
            .expect("current deliveries read");
        assert!(evolved_current.records().iter().all(|row| {
            row.admission == crate::store::logical_delivery::DeliveryAdmission::HistoricalUnknown
        }));
    }

    #[test]
    fn evolution_orders_undelivered_orders_assigns_each_answer_once_and_never_assigns_a_later_order()
     {
        let directory = tempfile::tempdir().expect("temporary Memory");
        let path = directory.path().join("flow.sema");
        let descriptors = evolution_descriptors();
        let heartbeat = StoredHeartbeatStanding::descriptor();
        let (predecessor, tables) = old_engine(&path, &descriptors, &heartbeat);
        let sender = old_address(old_flow::Aspect::Mind, "sender");
        let recipient = old_address(old_flow::Aspect::Field, "recipient");
        let native = old_signal::NativeReceipt {
            harness_kind: old_flow::HarnessKind::Codex,
            native_session_id: "session".into(),
            native_prompt_id: "prompt".into(),
        };
        let early = old_snapshot(
            1,
            "order-early",
            &sender,
            &recipient,
            old_signal::DeliveryState::Queued,
        );
        let late = old_snapshot(
            2,
            "order-late",
            &sender,
            &recipient,
            old_signal::DeliveryState::Queued,
        );
        let answer_queued = old_snapshot(
            3,
            "answer",
            &recipient,
            &sender,
            old_signal::DeliveryState::Queued,
        );
        let answer_delivered = old_snapshot(
            4,
            "answer",
            &recipient,
            &sender,
            old_signal::DeliveryState::Delivered(native),
        );
        let second_answer_queued = old_snapshot(
            5,
            "answer-second",
            &recipient,
            &sender,
            old_signal::DeliveryState::Queued,
        );
        let second_answer_delivered = old_snapshot(
            6,
            "answer-second",
            &recipient,
            &sender,
            old_signal::DeliveryState::Delivered(old_signal::NativeReceipt {
                harness_kind: old_flow::HarnessKind::Codex,
                native_session_id: "second-session".into(),
                native_prompt_id: "second-prompt".into(),
            }),
        );
        let after_answer = old_snapshot(
            7,
            "order-after-answer",
            &sender,
            &recipient,
            old_signal::DeliveryState::Queued,
        );
        for snapshot in [
            early.clone(),
            late.clone(),
            answer_delivered.clone(),
            second_answer_delivered.clone(),
            after_answer.clone(),
        ] {
            predecessor
                .assert(Assertion::new(
                    tables.current,
                    PriorCurrentLogicalDelivery { snapshot },
                ))
                .expect("old current stores");
        }
        for snapshot in [
            early,
            late,
            answer_queued,
            answer_delivered,
            second_answer_queued,
            second_answer_delivered,
            after_answer,
        ] {
            predecessor
                .assert(Assertion::new(
                    tables.events,
                    PriorLogicalDeliveryEvent { snapshot },
                ))
                .expect("old event stores");
        }
        for (logical_id, state, request) in [
            (
                "order-early",
                PriorRequestState::Queued,
                old_flow::Request::Order("same words".into()),
            ),
            (
                "order-late",
                PriorRequestState::Queued,
                old_flow::Request::Order("same words".into()),
            ),
            (
                "order-after-answer",
                PriorRequestState::Queued,
                old_flow::Request::Order("same words".into()),
            ),
            (
                "answer",
                PriorRequestState::Settled,
                old_flow::Request::Result(old_flow::ResultPayload::Answered(
                    old_flow::ResultPayload_Answered_Data {
                        answerable: old_flow::Answerable::Order("same words".into()),
                        string: "answer words".into(),
                    },
                )),
            ),
            (
                "answer-second",
                PriorRequestState::Settled,
                old_flow::Request::Result(old_flow::ResultPayload::Answered(
                    old_flow::ResultPayload_Answered_Data {
                        answerable: old_flow::Answerable::Order("same words".into()),
                        string: "second answer words".into(),
                    },
                )),
            ),
        ] {
            let (from, to) = if logical_id.starts_with("answer") {
                (&recipient, &sender)
            } else {
                (&sender, &recipient)
            };
            predecessor
                .assert(Assertion::new(
                    tables.requests,
                    old_request(logical_id, from, to, request, state),
                ))
                .expect("old request stores");
        }
        drop(predecessor);

        let mut engine = Engine::open(
            EngineOpen::new(&path, SchemaVersion::new(crate::store::FLOW_SCHEMA_VERSION))
                .with_family(&descriptors.current)
                .with_family(&descriptors.events)
                .with_family(&descriptors.metaflows)
                .with_family(&descriptors.address_holds)
                .with_family(&descriptors.requests)
                .with_family(&descriptors.attempts)
                .with_family(&descriptors.outcomes)
                .with_family(&descriptors.send_calls)
                .with_family(&descriptors.flow_events)
                .with_family(&heartbeat),
        )
        .expect("predecessor catalog opens for evolution");
        let heartbeat_table = engine.register_table(heartbeat).expect("heartbeat opens");
        descriptors
            .evolve_request_id_memory(
                &mut engine,
                &[],
                &[],
                &[],
                heartbeat_table,
                &[],
                &ScriptedDrawer::new(&["a001", "a002", "a003", "b001", "b002", "b003", "b004"]),
            )
            .expect("fallback-order evolution commits");
        let assignment_table = engine
            .register_table(evolution_descriptors().upgrade_assignments)
            .expect("assignments register");
        let assignments = engine
            .match_records(QueryPlan::all(assignment_table))
            .expect("assignments read");
        let [assignment] = assignments.records() else {
            panic!("one assignment part")
        };
        assert_eq!(assignment.decisions.len(), 3);
        let assigned = assignment
            .decisions
            .iter()
            .flat_map(|decision| &decision.assigned)
            .collect::<Vec<_>>();
        assert_eq!(assigned.len(), 2);
        assert_eq!(assigned[0].order, LogicalDeliveryId("order-early".into()));
        assert_eq!(assigned[1].order, LogicalDeliveryId("order-late".into()));
        assert_eq!(
            assignment
                .decisions
                .iter()
                .flat_map(|decision| &decision.open)
                .map(|open| open.order.0.as_str())
                .collect::<std::collections::BTreeSet<_>>(),
            std::collections::BTreeSet::from(["order-after-answer"])
        );
    }

    #[test]
    fn evolution_classifies_every_hold_and_clears_only_moved_heartbeat_references() {
        let directory = tempfile::tempdir().expect("temporary Memory");
        let path = directory.path().join("flow.sema");
        let descriptors = evolution_descriptors();
        let heartbeat = StoredHeartbeatStanding::descriptor();
        let (mut predecessor, tables) = old_engine(&path, &descriptors, &heartbeat);
        let heartbeat_table = predecessor
            .register_table(heartbeat.clone())
            .expect("old heartbeat registers again");
        let sender = old_address(old_flow::Aspect::Mind, "sender");
        let recipient = old_address(old_flow::Aspect::Field, "recipient");
        let native = old_signal::NativeReceipt {
            harness_kind: old_flow::HarnessKind::Codex,
            native_session_id: "session".into(),
            native_prompt_id: "prompt".into(),
        };
        let cases = [
            (
                1,
                "queued-held",
                old_signal::DeliveryState::Queued,
                old_flow::Request::Notice("queued".into()),
                PriorRequestState::Queued,
            ),
            (
                2,
                "delivered-moved",
                old_signal::DeliveryState::Delivered(native.clone()),
                old_flow::Request::Notice("finished".into()),
                PriorRequestState::Settled,
            ),
            (
                3,
                "task-held-by-reminder",
                old_signal::DeliveryState::Delivered(native.clone()),
                old_flow::Request::Task(old_flow::AnswerableTask {
                    minutes: 5,
                    string: "unfinished task".into(),
                }),
                PriorRequestState::Settled,
            ),
            (
                4,
                "input-held-by-reply",
                old_signal::DeliveryState::Delivered(native),
                old_flow::Request::Question("unfinished reply".into()),
                PriorRequestState::Settled,
            ),
        ];
        for (cursor, logical_id, state, request, request_state) in cases {
            let snapshot = old_snapshot(cursor, logical_id, &sender, &recipient, state);
            predecessor
                .assert(Assertion::new(
                    tables.current,
                    PriorCurrentLogicalDelivery {
                        snapshot: snapshot.clone(),
                    },
                ))
                .expect("old current stores");
            predecessor
                .assert(Assertion::new(
                    tables.events,
                    PriorLogicalDeliveryEvent { snapshot },
                ))
                .expect("old event stores");
            predecessor
                .assert(Assertion::new(
                    tables.requests,
                    old_request(logical_id, &sender, &recipient, request, request_state),
                ))
                .expect("old request stores");
        }
        let heartbeat_row = StoredHeartbeatStanding {
            address: recipient.current().expect("address recodes"),
            baseline_awake: 1,
            baseline_working: None,
            baseline_working_complete: true,
            baseline_sent: 1,
            baseline_queue: 0,
            latest_awake: 1,
            latest_working: None,
            latest_working_complete: true,
            latest_sent: 1,
            latest_queue: 0,
            outstanding: Some("delivered-moved".into()),
        };
        predecessor
            .assert(Assertion::new(heartbeat_table, heartbeat_row.clone()))
            .expect("old heartbeat stores");
        drop(predecessor);

        let reminder = super::super::reminder::Reminder {
            key: "task-held-by-reminder".into(),
            address: sender.current().expect("sender recodes"),
            recipient: recipient.current().expect("recipient recodes"),
            minutes: 5,
            due: Some(123),
            sent: 0,
        };
        let reply = super::super::logical_delivery::SimplifiedReplyBinding {
            flow_id: "reply-flow".into(),
            input_delivery_id: "input-held-by-reply".into(),
            source: sender.current().expect("source recodes"),
            recipient: recipient.current().expect("recipient recodes"),
            completed: false,
        };
        let purposes = [
            super::super::logical_delivery::LogicalDeliveryPurpose {
                logical_delivery_id: "queued-held".into(),
                address: recipient.current().expect("recipient recodes"),
                purpose: crate::heartbeat::DeliveryPurpose::Ordinary,
            },
            super::super::logical_delivery::LogicalDeliveryPurpose {
                logical_delivery_id: "delivered-moved".into(),
                address: recipient.current().expect("recipient recodes"),
                purpose: crate::heartbeat::DeliveryPurpose::Ordinary,
            },
        ];
        let mut engine = Engine::open(
            EngineOpen::new(&path, SchemaVersion::new(crate::store::FLOW_SCHEMA_VERSION))
                .with_family(&descriptors.current)
                .with_family(&descriptors.events)
                .with_family(&descriptors.metaflows)
                .with_family(&descriptors.address_holds)
                .with_family(&descriptors.requests)
                .with_family(&descriptors.attempts)
                .with_family(&descriptors.outcomes)
                .with_family(&descriptors.send_calls)
                .with_family(&descriptors.flow_events)
                .with_family(&heartbeat),
        )
        .expect("predecessor catalog opens for evolution");
        let heartbeat_table = engine
            .register_table(heartbeat.clone())
            .expect("heartbeat opens");
        descriptors
            .evolve_request_id_memory(
                &mut engine,
                std::slice::from_ref(&reminder),
                std::slice::from_ref(&reply),
                &purposes,
                heartbeat_table,
                std::slice::from_ref(&heartbeat_row),
                &ScriptedDrawer::new(&["h001", "h002", "h003", "0000", "0001", "0002", "0003"]),
            )
            .expect("classification evolution commits");

        let descriptors = evolution_descriptors();
        let current_table = engine
            .register_table(descriptors.current)
            .expect("current registers");
        let current = engine
            .match_records(QueryPlan::all(current_table))
            .expect("current reads");
        assert_eq!(current.records().len(), 3);
        assert!(current.records().iter().all(|row| {
            row.snapshot.logical_delivery_id.0 != "delivered-moved"
                && row.admission
                    == super::super::logical_delivery::DeliveryAdmission::HistoricalUnknown
        }));
        let prior_table = engine
            .register_table(descriptors.prior_deliveries)
            .expect("prior deliveries register");
        let prior = engine
            .match_records(QueryPlan::all(prior_table))
            .expect("prior deliveries read");
        assert_eq!(prior.records().len(), 1);
        assert_eq!(prior.records()[0].logical_delivery_id, "delivered-moved");
        let binding_table = engine
            .register_table(descriptors.request_ids)
            .expect("bindings register");
        let bindings = engine
            .match_records(QueryPlan::all(binding_table))
            .expect("bindings read");
        assert_eq!(bindings.records().len(), 3);
        let announcement_table = engine
            .register_table(descriptors.task_announcements)
            .expect("announcements register");
        let announcements = engine
            .match_records(QueryPlan::all(announcement_table))
            .expect("announcements read");
        assert_eq!(announcements.records().len(), 1);
        assert_eq!(
            announcements.records()[0].tasks,
            vec![flow_ethos::LogicalDeliveryId(
                "task-held-by-reminder".into()
            )]
        );
        let heartbeat_rows = engine
            .match_records(QueryPlan::all(heartbeat_table))
            .expect("heartbeat reads");
        assert_eq!(heartbeat_rows.records()[0].outstanding, None);
    }

    #[test]
    fn evolution_rewrites_every_nonempty_held_delivery_family_with_one_request_id() {
        let directory = tempfile::tempdir().expect("temporary Memory");
        let path = directory.path().join("flow.sema");
        let descriptors = evolution_descriptors();
        let heartbeat = StoredHeartbeatStanding::descriptor();
        let (predecessor, tables) = old_engine(&path, &descriptors, &heartbeat);
        let sender = old_address(old_flow::Aspect::Mind, "sender");
        let recipient = old_address(old_flow::Aspect::Field, "recipient");
        let logical_id = "held-with-references";
        let snapshot = old_snapshot(
            1,
            logical_id,
            &sender,
            &recipient,
            old_signal::DeliveryState::Uncertain,
        );
        let request = old_request(
            logical_id,
            &sender,
            &recipient,
            old_flow::Request::Notice("held words".into()),
            PriorRequestState::Queued,
        );
        let old_meta::QueueEvidence::Logical(queue_entry) = &request.queue_evidence else {
            panic!("helper creates logical evidence")
        };
        let queue_entry = queue_entry.clone();
        predecessor
            .assert(Assertion::new(
                tables.current,
                PriorCurrentLogicalDelivery {
                    snapshot: snapshot.clone(),
                },
            ))
            .expect("old current stores");
        predecessor
            .assert(Assertion::new(
                tables.events,
                PriorLogicalDeliveryEvent { snapshot },
            ))
            .expect("old event stores");
        predecessor
            .assert(Assertion::new(tables.requests, request.clone()))
            .expect("old request stores");
        predecessor
            .assert(Assertion::new(
                tables.attempts,
                PriorQueueDeliveryAttempt {
                    request_id: logical_id.into(),
                    attempt_id: "attempt".into(),
                    native_request_id: 7,
                    flow_id: "flow".into(),
                    thread_id: "thread".into(),
                    harness_kind: old_flow::HarnessKind::Codex,
                    history_anchor: PriorNativeHistoryAnchor::CodexTurns(None),
                    address: recipient.clone(),
                    queue_evidence: request.queue_evidence.clone(),
                    phase: PriorQueueDeliveryPhase::Uncertain(None),
                },
            ))
            .expect("old attempt stores");
        predecessor
            .assert(Assertion::new(
                tables.outcomes,
                PriorStoredSendOutcome {
                    record: old_meta::SendOutcomeRecord {
                        send_outcome_id: "outcome".into(),
                        process_option: None,
                        first_address_option: Some(sender.clone()),
                        second_address_option: Some(recipient.clone()),
                        request_digest: "digest".into(),
                        ordinary_send_outcome: old_meta::OrdinarySendOutcome::Accepted(
                            old_signal::SendAcceptance {
                                logical_delivery_id: logical_id.into(),
                                delivery_cursor: 1,
                                delivery_state: old_signal::DeliveryState::Uncertain,
                            },
                        ),
                    },
                },
            ))
            .expect("old outcome stores");
        let tool_key = crate::generated::send_tool_before_request_ids::ToolCallKey {
            thread_id: "thread".into(),
            turn_id: "turn".into(),
            call_id: "call".into(),
        };
        predecessor
            .assert(Assertion::new(
                tables.send_calls,
                PriorSendCall {
                    tool_call_key: tool_key,
                    content_sha256: "sha".into(),
                    send_outcome:
                        crate::generated::send_tool_before_request_ids::SendOutcome::Accepted(
                            crate::generated::send_tool_before_request_ids::Accepted_Data {
                                sender: sender.clone(),
                                target: recipient.clone(),
                                send_acceptance: old_signal::SendAcceptance {
                                    logical_delivery_id: logical_id.into(),
                                    delivery_cursor: 1,
                                    delivery_state: old_signal::DeliveryState::Uncertain,
                                },
                            },
                        ),
                },
            ))
            .expect("old send call stores");
        let metaflow = old_flow::Permaflow {
            address: recipient.clone(),
            permaflow_state: old_flow::State::Asleep,
            past: Vec::new(),
            queue: vec![queue_entry],
        };
        predecessor
            .assert(Assertion::new(
                tables.metaflows,
                PriorStoredMetaflow {
                    metaflow: metaflow.clone(),
                },
            ))
            .expect("old metaflow stores");
        predecessor
            .assert(Assertion::new(
                tables.address_holds,
                PriorAddressHold {
                    flow_id: "flow".into(),
                    address: recipient,
                    found: Some(metaflow),
                },
            ))
            .expect("old address hold stores");
        drop(predecessor);

        let mut engine = Engine::open(
            EngineOpen::new(&path, SchemaVersion::new(crate::store::FLOW_SCHEMA_VERSION))
                .with_family(&descriptors.current)
                .with_family(&descriptors.events)
                .with_family(&descriptors.metaflows)
                .with_family(&descriptors.address_holds)
                .with_family(&descriptors.requests)
                .with_family(&descriptors.attempts)
                .with_family(&descriptors.outcomes)
                .with_family(&descriptors.send_calls)
                .with_family(&descriptors.flow_events)
                .with_family(&heartbeat),
        )
        .expect("predecessor catalog opens for evolution");
        let heartbeat_table = engine.register_table(heartbeat).expect("heartbeat opens");
        descriptors
            .evolve_request_id_memory(
                &mut engine,
                &[],
                &[],
                &[],
                heartbeat_table,
                &[],
                &ScriptedDrawer::new(&["held", "0000", "0001", "0002", "0003"]),
            )
            .expect("nonempty-family evolution commits");

        let descriptors = evolution_descriptors();
        let metaflows = engine
            .register_table(descriptors.metaflows)
            .expect("metaflows register");
        let metaflows = engine
            .match_records(QueryPlan::all(metaflows))
            .expect("metaflows read");
        assert_eq!(
            metaflows.records()[0].metaflow.queue.0[0].request_id,
            RequestId("held".into())
        );
        let holds = engine
            .register_table(descriptors.address_holds)
            .expect("holds register");
        let holds = engine
            .match_records(QueryPlan::all(holds))
            .expect("holds read");
        assert_eq!(
            holds.records()[0]
                .found
                .as_ref()
                .expect("found kept")
                .queue
                .0[0]
                .request_id,
            RequestId("held".into())
        );
        let attempts = engine
            .register_table(descriptors.attempts)
            .expect("attempts register");
        let attempts = engine
            .match_records(QueryPlan::all(attempts))
            .expect("attempts read");
        let meta_signal_flow::QueueEvidence::Logical(entry) = &attempts.records()[0].queue_evidence
        else {
            panic!("logical evidence stays logical")
        };
        assert_eq!(entry.request_id, RequestId("held".into()));
        let outcomes = engine
            .register_table(descriptors.outcomes)
            .expect("outcomes register");
        let outcomes = engine
            .match_records(QueryPlan::all(outcomes))
            .expect("outcomes read");
        let meta_signal_flow::OrdinarySendOutcome::Accepted(acceptance) =
            &outcomes.records()[0].record.ordinary_send_outcome
        else {
            panic!("accepted outcome stays accepted")
        };
        assert_eq!(acceptance.request_id, RequestId("held".into()));
        let calls = engine
            .register_table(descriptors.send_calls)
            .expect("send calls register");
        let calls = engine
            .match_records(QueryPlan::all(calls))
            .expect("send calls read");
        let crate::generated::send_tool::SendOutcome::Accepted(accepted) =
            &calls.records()[0].send_outcome
        else {
            panic!("accepted call stays accepted")
        };
        assert_eq!(
            accepted.send_acceptance.request_id,
            RequestId("held".into())
        );
    }

    #[test]
    fn every_resolved_reference_family_refuses_a_missing_current_without_writing() {
        for family in [
            "request",
            "attempt",
            "outcome",
            "send-call",
            "metaflow",
            "address-hold",
            "reminder",
            "simplified-reply",
            "purpose",
            "heartbeat",
        ] {
            let directory = tempfile::tempdir().expect("temporary Memory");
            let path = directory.path().join("flow.sema");
            let descriptors = evolution_descriptors();
            let heartbeat = StoredHeartbeatStanding::descriptor();
            let (mut predecessor, tables) = old_engine(&path, &descriptors, &heartbeat);
            let heartbeat_table = predecessor
                .register_table(heartbeat.clone())
                .expect("old heartbeat registers again");
            let sender = old_address(old_flow::Aspect::Mind, "sender");
            let recipient = old_address(old_flow::Aspect::Field, "recipient");
            let missing_request = old_request(
                "missing",
                &sender,
                &recipient,
                old_flow::Request::Notice("missing".into()),
                PriorRequestState::Queued,
            );
            let old_meta::QueueEvidence::Logical(queue_entry) = &missing_request.queue_evidence
            else {
                panic!("helper creates logical evidence")
            };
            let _ = match family {
                "request" => Some(predecessor
                    .assert(Assertion::new(tables.requests, missing_request.clone()))
                    .expect("orphan request stores")),
                "attempt" => Some(predecessor
                    .assert(Assertion::new(
                        tables.attempts,
                        PriorQueueDeliveryAttempt {
                            request_id: "missing".into(),
                            attempt_id: "attempt".into(),
                            native_request_id: 1,
                            flow_id: "flow".into(),
                            thread_id: "thread".into(),
                            harness_kind: old_flow::HarnessKind::Codex,
                            history_anchor: PriorNativeHistoryAnchor::CodexTurns(None),
                            address: recipient.clone(),
                            queue_evidence: missing_request.queue_evidence.clone(),
                            phase: PriorQueueDeliveryPhase::Prepared,
                        },
                    ))
                    .expect("orphan attempt stores")),
                "outcome" => Some(predecessor
                    .assert(Assertion::new(
                        tables.outcomes,
                        PriorStoredSendOutcome {
                            record: old_meta::SendOutcomeRecord {
                                send_outcome_id: "outcome".into(),
                                process_option: None,
                                first_address_option: Some(sender.clone()),
                                second_address_option: Some(recipient.clone()),
                                request_digest: "digest".into(),
                                ordinary_send_outcome: old_meta::OrdinarySendOutcome::Accepted(
                                    old_signal::SendAcceptance {
                                        logical_delivery_id: "missing".into(),
                                        delivery_cursor: 1,
                                        delivery_state: old_signal::DeliveryState::Queued,
                                    },
                                ),
                            },
                        },
                    ))
                    .expect("orphan outcome stores")),
                "send-call" => Some(predecessor
                    .assert(Assertion::new(
                        tables.send_calls,
                        PriorSendCall {
                            tool_call_key:
                                crate::generated::send_tool_before_request_ids::ToolCallKey {
                                    thread_id: "thread".into(),
                                    turn_id: "turn".into(),
                                    call_id: "call".into(),
                                },
                            content_sha256: "sha".into(),
                            send_outcome: crate::generated::send_tool_before_request_ids::SendOutcome::Accepted(
                                crate::generated::send_tool_before_request_ids::Accepted_Data {
                                    sender: sender.clone(),
                                    target: recipient.clone(),
                                    send_acceptance: old_signal::SendAcceptance {
                                        logical_delivery_id: "missing".into(),
                                        delivery_cursor: 1,
                                        delivery_state: old_signal::DeliveryState::Queued,
                                    },
                                },
                            ),
                        },
                    ))
                    .expect("orphan send call stores")),
                "metaflow" => Some(predecessor
                    .assert(Assertion::new(
                        tables.metaflows,
                        PriorStoredMetaflow {
                            metaflow: old_flow::Permaflow {
                                address: recipient.clone(),
                                permaflow_state: old_flow::State::Asleep,
                                past: Vec::new(),
                                queue: vec![queue_entry.clone()],
                            },
                        },
                    ))
                    .expect("orphan metaflow queue stores")),
                "address-hold" => Some(predecessor
                    .assert(Assertion::new(
                        tables.address_holds,
                        PriorAddressHold {
                            flow_id: "flow".into(),
                            address: recipient.clone(),
                            found: Some(old_flow::Permaflow {
                                address: recipient.clone(),
                                permaflow_state: old_flow::State::Asleep,
                                past: Vec::new(),
                                queue: vec![queue_entry.clone()],
                            }),
                        },
                    ))
                    .expect("orphan address hold stores")),
                "heartbeat" => Some(predecessor
                    .assert(Assertion::new(
                        heartbeat_table,
                        StoredHeartbeatStanding {
                            address: recipient.current().expect("address recodes"),
                            baseline_awake: 0,
                            baseline_working: None,
                            baseline_working_complete: true,
                            baseline_sent: 0,
                            baseline_queue: 0,
                            latest_awake: 0,
                            latest_working: None,
                            latest_working_complete: true,
                            latest_sent: 0,
                            latest_queue: 0,
                            outstanding: Some("missing".into()),
                        },
                    ))
                    .expect("orphan heartbeat stores")),
                "reminder" | "simplified-reply" | "purpose" => None,
                _ => unreachable!(),
            };
            drop(predecessor);

            let reminder = super::super::reminder::Reminder {
                key: "missing".into(),
                address: sender.current().expect("address recodes"),
                recipient: recipient.current().expect("address recodes"),
                minutes: 1,
                due: Some(1),
                sent: 0,
            };
            let reply = super::super::logical_delivery::SimplifiedReplyBinding {
                flow_id: "flow".into(),
                input_delivery_id: "missing".into(),
                source: sender.current().expect("address recodes"),
                recipient: recipient.current().expect("address recodes"),
                completed: false,
            };
            let purpose = super::super::logical_delivery::LogicalDeliveryPurpose {
                logical_delivery_id: "missing".into(),
                address: recipient.current().expect("address recodes"),
                purpose: crate::heartbeat::DeliveryPurpose::Ordinary,
            };
            let heartbeat_row = StoredHeartbeatStanding {
                address: recipient.current().expect("address recodes"),
                baseline_awake: 0,
                baseline_working: None,
                baseline_working_complete: true,
                baseline_sent: 0,
                baseline_queue: 0,
                latest_awake: 0,
                latest_working: None,
                latest_working_complete: true,
                latest_sent: 0,
                latest_queue: 0,
                outstanding: Some("missing".into()),
            };
            let mut engine = Engine::open(
                EngineOpen::new(&path, SchemaVersion::new(crate::store::FLOW_SCHEMA_VERSION))
                    .with_family(&descriptors.current)
                    .with_family(&descriptors.events)
                    .with_family(&descriptors.metaflows)
                    .with_family(&descriptors.address_holds)
                    .with_family(&descriptors.requests)
                    .with_family(&descriptors.attempts)
                    .with_family(&descriptors.outcomes)
                    .with_family(&descriptors.send_calls)
                    .with_family(&descriptors.flow_events)
                    .with_family(&heartbeat),
            )
            .expect("predecessor catalog opens for validation");
            let heartbeat_table = engine.register_table(heartbeat).expect("heartbeat opens");
            let before = std::fs::read(&path).expect("Memory reads before refusal");
            let result = descriptors.evolve_request_id_memory(
                &mut engine,
                if family == "reminder" {
                    std::slice::from_ref(&reminder)
                } else {
                    &[]
                },
                if family == "simplified-reply" {
                    std::slice::from_ref(&reply)
                } else {
                    &[]
                },
                if family == "purpose" {
                    std::slice::from_ref(&purpose)
                } else {
                    &[]
                },
                heartbeat_table,
                if family == "heartbeat" {
                    std::slice::from_ref(&heartbeat_row)
                } else {
                    &[]
                },
                &ScriptedDrawer::new(&["0000", "0001", "0002", "0003"]),
            );
            assert!(
                matches!(result, Err(StoreError::StateInvariant)),
                "{family} refuses its missing current"
            );
            assert_eq!(
                std::fs::read(&path).expect("Memory reads after refusal"),
                before,
                "{family} refusal writes nothing"
            );
        }
    }

    #[test]
    fn evolved_task_announcements_partition_and_current_reopen_changes_nothing() {
        let directory = tempfile::tempdir().expect("temporary Memory");
        let path = directory.path().join("flow.sema");
        let descriptors = evolution_descriptors();
        let heartbeat = StoredHeartbeatStanding::descriptor();
        let (predecessor, tables) = old_engine(&path, &descriptors, &heartbeat);
        let sender = old_address(old_flow::Aspect::Mind, "sender");
        let recipient = old_address(old_flow::Aspect::Field, "recipient");
        let native = old_signal::NativeReceipt {
            harness_kind: old_flow::HarnessKind::Codex,
            native_session_id: "session".into(),
            native_prompt_id: "prompt".into(),
        };
        let words = "é".repeat(crate::sending::MAX_SEND_PAYLOAD_BYTES / 2);
        for (cursor, logical_id) in [(1, "task-one"), (2, "task-two")] {
            let snapshot = old_snapshot(
                cursor,
                logical_id,
                &sender,
                &recipient,
                old_signal::DeliveryState::Delivered(native.clone()),
            );
            predecessor
                .assert(Assertion::new(
                    tables.current,
                    PriorCurrentLogicalDelivery {
                        snapshot: snapshot.clone(),
                    },
                ))
                .expect("old current stores");
            predecessor
                .assert(Assertion::new(
                    tables.events,
                    PriorLogicalDeliveryEvent { snapshot },
                ))
                .expect("old event stores");
            predecessor
                .assert(Assertion::new(
                    tables.requests,
                    old_request(
                        logical_id,
                        &sender,
                        &recipient,
                        old_flow::Request::Task(old_flow::AnswerableTask {
                            minutes: 1,
                            string: format!("{logical_id}-{words}"),
                        }),
                        PriorRequestState::Settled,
                    ),
                ))
                .expect("old task stores");
        }
        drop(predecessor);
        let current_sender = sender.current().expect("sender recodes");
        let current_recipient = recipient.current().expect("recipient recodes");
        let reminders = ["task-one", "task-two"].map(|key| super::super::reminder::Reminder {
            key: key.into(),
            address: current_sender.clone(),
            recipient: current_recipient.clone(),
            minutes: 1,
            due: Some(1),
            sent: 0,
        });

        let mut engine = Engine::open(
            EngineOpen::new(&path, SchemaVersion::new(crate::store::FLOW_SCHEMA_VERSION))
                .with_family(&descriptors.current)
                .with_family(&descriptors.events)
                .with_family(&descriptors.metaflows)
                .with_family(&descriptors.address_holds)
                .with_family(&descriptors.requests)
                .with_family(&descriptors.attempts)
                .with_family(&descriptors.outcomes)
                .with_family(&descriptors.send_calls)
                .with_family(&descriptors.flow_events)
                .with_family(&heartbeat),
        )
        .expect("predecessor catalog opens for evolution");
        let heartbeat_table = engine
            .register_table(heartbeat.clone())
            .expect("heartbeat opens");
        descriptors
            .evolve_request_id_memory(
                &mut engine,
                &reminders,
                &[],
                &[],
                heartbeat_table,
                &[],
                &ScriptedDrawer::new(&["t001", "t002", "0000", "0001", "0002", "0003"]),
            )
            .expect("task evolution commits");
        let descriptors = evolution_descriptors();
        let announcement_table = engine
            .register_table(descriptors.task_announcements.clone())
            .expect("announcements register");
        let announcements = engine
            .match_records(QueryPlan::all(announcement_table))
            .expect("announcements read");
        assert_eq!(announcements.records().len(), 2);
        assert_eq!(
            announcements
                .records()
                .iter()
                .map(|row| (row.part, row.tasks.clone()))
                .collect::<Vec<_>>(),
            vec![
                (0, vec![LogicalDeliveryId("task-one".into())]),
                (1, vec![LogicalDeliveryId("task-two".into())])
            ]
        );
        let expected_announcements = announcements.records().to_vec();
        drop(announcements);
        drop(engine);

        let mut reopened = Engine::open(
            EngineOpen::new(&path, SchemaVersion::new(crate::store::FLOW_SCHEMA_VERSION))
                .with_family(&descriptors.current)
                .with_family(&descriptors.events)
                .with_family(&descriptors.metaflows)
                .with_family(&descriptors.address_holds)
                .with_family(&descriptors.requests)
                .with_family(&descriptors.attempts)
                .with_family(&descriptors.outcomes)
                .with_family(&descriptors.send_calls)
                .with_family(&descriptors.flow_events)
                .with_family(&descriptors.request_ids)
                .with_family(&descriptors.deliver_calls)
                .with_family(&descriptors.store_identity)
                .with_family(&descriptors.prior_deliveries)
                .with_family(&descriptors.task_announcements)
                .with_family(&descriptors.upgrade_assignments)
                .with_family(&descriptors.request_policy)
                .with_family(&heartbeat),
        )
        .expect("current catalog reopens without evolution");
        let announcement_table = reopened
            .register_table(descriptors.task_announcements)
            .expect("reopened announcements register");
        let reopened_announcements = reopened
            .match_records(QueryPlan::all(announcement_table))
            .expect("reopened announcements read");
        assert_eq!(reopened_announcements.records(), expected_announcements);
        drop(reopened);
    }

    #[test]
    fn orphaned_predecessor_event_refuses_evolution_without_changing_memory() {
        let directory = tempfile::tempdir().expect("temporary Memory");
        let path = directory.path().join("flow.sema");
        let descriptors = evolution_descriptors();
        let heartbeat = StoredHeartbeatStanding::descriptor();
        let (predecessor, tables) = old_engine(&path, &descriptors, &heartbeat);
        let sender = old_address(old_flow::Aspect::Mind, "sender");
        let recipient = old_address(old_flow::Aspect::Field, "recipient");
        predecessor
            .assert(Assertion::new(
                tables.events,
                PriorLogicalDeliveryEvent {
                    snapshot: old_snapshot(
                        1,
                        "orphan",
                        &sender,
                        &recipient,
                        old_signal::DeliveryState::Queued,
                    ),
                },
            ))
            .expect("orphaned predecessor event stores");
        drop(predecessor);

        let mut engine = Engine::open(
            EngineOpen::new(&path, SchemaVersion::new(crate::store::FLOW_SCHEMA_VERSION))
                .with_family(&descriptors.current)
                .with_family(&descriptors.events)
                .with_family(&descriptors.metaflows)
                .with_family(&descriptors.address_holds)
                .with_family(&descriptors.requests)
                .with_family(&descriptors.attempts)
                .with_family(&descriptors.outcomes)
                .with_family(&descriptors.send_calls)
                .with_family(&descriptors.flow_events)
                .with_family(&heartbeat),
        )
        .expect("predecessor catalog opens for validation");
        let heartbeat_table = engine.register_table(heartbeat).expect("heartbeat opens");
        let before = std::fs::read(&path).expect("Memory bytes read before evolution");
        assert!(matches!(
            descriptors.evolve_request_id_memory(
                &mut engine,
                &[],
                &[],
                &[],
                heartbeat_table,
                &[],
                &ScriptedDrawer::new(&["0000", "0001", "0002", "0003"]),
            ),
            Err(StoreError::StateInvariant)
        ));
        let after = std::fs::read(&path).expect("Memory bytes read after refused evolution");
        assert_eq!(
            after, before,
            "prevalidation refusal commits no catalog or row change"
        );
    }

    #[test]
    fn predecessor_request_id_collision_exhaustion_leaves_memory_byte_identical() {
        let directory = tempfile::tempdir().expect("temporary Memory");
        let path = directory.path().join("flow.sema");
        let descriptors = evolution_descriptors();
        let heartbeat = StoredHeartbeatStanding::descriptor();
        let (predecessor, tables) = old_engine(&path, &descriptors, &heartbeat);
        let sender = old_address(old_flow::Aspect::Mind, "sender");
        let recipient = old_address(old_flow::Aspect::Field, "recipient");
        for (cursor, logical_id) in [(1, "first"), (2, "second")] {
            let snapshot = old_snapshot(
                cursor,
                logical_id,
                &sender,
                &recipient,
                old_signal::DeliveryState::Queued,
            );
            predecessor
                .assert(Assertion::new(
                    tables.current,
                    PriorCurrentLogicalDelivery {
                        snapshot: snapshot.clone(),
                    },
                ))
                .expect("old current stores");
            predecessor
                .assert(Assertion::new(
                    tables.events,
                    PriorLogicalDeliveryEvent { snapshot },
                ))
                .expect("old event stores");
            predecessor
                .assert(Assertion::new(
                    tables.requests,
                    old_request(
                        logical_id,
                        &sender,
                        &recipient,
                        old_flow::Request::Notice(logical_id.into()),
                        PriorRequestState::Queued,
                    ),
                ))
                .expect("old request stores");
        }
        drop(predecessor);

        let mut engine = Engine::open(
            EngineOpen::new(&path, SchemaVersion::new(crate::store::FLOW_SCHEMA_VERSION))
                .with_family(&descriptors.current)
                .with_family(&descriptors.events)
                .with_family(&descriptors.metaflows)
                .with_family(&descriptors.address_holds)
                .with_family(&descriptors.requests)
                .with_family(&descriptors.attempts)
                .with_family(&descriptors.outcomes)
                .with_family(&descriptors.send_calls)
                .with_family(&descriptors.flow_events)
                .with_family(&heartbeat),
        )
        .expect("predecessor catalog opens for validation");
        let heartbeat_table = engine.register_table(heartbeat).expect("heartbeat opens");
        let before = std::fs::read(&path).expect("Memory bytes read before evolution");
        let draws = vec![
            "same";
            usize::try_from(super::super::SEEDED_REQUEST_ID_DRAWS + 1)
                .expect("configured draw bound fits memory")
        ];
        assert!(matches!(
            descriptors.evolve_request_id_memory(
                &mut engine,
                &[],
                &[],
                &[],
                heartbeat_table,
                &[],
                &ScriptedDrawer::new(&draws),
            ),
            Err(StoreError::StateInvariant)
        ));
        let after = std::fs::read(&path).expect("Memory bytes read after refused evolution");
        assert_eq!(
            after, before,
            "request-id exhaustion commits no catalog or row change"
        );
    }

    #[test]
    fn an_empty_exact_predecessor_evolves_once_and_reopens_wholly_current() {
        let directory = tempfile::tempdir().expect("temporary Memory");
        let path = directory.path().join("flow.sema");
        let descriptors = evolution_descriptors();
        let heartbeat = StoredHeartbeatStanding::descriptor();

        let mut predecessor = Engine::open(EngineOpen::new(
            &path,
            SchemaVersion::new(crate::store::FLOW_SCHEMA_VERSION),
        ))
        .expect("predecessor Memory opens");
        predecessor
            .register_table(predecessor_descriptor::<PriorCurrentLogicalDelivery, _>(
                &descriptors.current,
            ))
            .expect("old current registers");
        predecessor
            .register_table(predecessor_descriptor::<PriorLogicalDeliveryEvent, _>(
                &descriptors.events,
            ))
            .expect("old events register");
        predecessor
            .register_table(predecessor_descriptor::<PriorStoredMetaflow, _>(
                &descriptors.metaflows,
            ))
            .expect("old metaflows register");
        predecessor
            .register_table(predecessor_descriptor::<PriorAddressHold, _>(
                &descriptors.address_holds,
            ))
            .expect("old holds register");
        predecessor
            .register_table(predecessor_descriptor::<PriorStoredDeliveryRequest, _>(
                &descriptors.requests,
            ))
            .expect("old requests register");
        predecessor
            .register_table(predecessor_descriptor::<PriorQueueDeliveryAttempt, _>(
                &descriptors.attempts,
            ))
            .expect("old attempts register");
        predecessor
            .register_table(predecessor_descriptor::<PriorStoredSendOutcome, _>(
                &descriptors.outcomes,
            ))
            .expect("old outcomes register");
        predecessor
            .register_table(predecessor_descriptor::<PriorSendCall, _>(
                &descriptors.send_calls,
            ))
            .expect("old calls register");
        predecessor
            .register_table(predecessor_descriptor::<PriorFlowEvents, _>(
                &descriptors.flow_events,
            ))
            .expect("old flow events register");
        predecessor
            .register_table(heartbeat.clone())
            .expect("unchanged heartbeat registers");
        drop(predecessor);

        let mut engine = Engine::open(
            EngineOpen::new(&path, SchemaVersion::new(crate::store::FLOW_SCHEMA_VERSION))
                .with_family(&descriptors.current)
                .with_family(&descriptors.events)
                .with_family(&descriptors.metaflows)
                .with_family(&descriptors.address_holds)
                .with_family(&descriptors.requests)
                .with_family(&descriptors.attempts)
                .with_family(&descriptors.outcomes)
                .with_family(&descriptors.send_calls)
                .with_family(&descriptors.flow_events)
                .with_family(&heartbeat),
        )
        .expect("exact predecessor catalog is recognized");
        let heartbeat_table = engine
            .register_table(heartbeat.clone())
            .expect("heartbeat opens");
        descriptors
            .evolve_request_id_memory(
                &mut engine,
                &[],
                &[],
                &[],
                heartbeat_table,
                &[],
                &ScriptedDrawer::new(&["0000", "0001", "0002", "0003"]),
            )
            .expect("one atomic evolution commits");
        drop(engine);

        let descriptors = evolution_descriptors();
        let mut reopened = Engine::open(
            EngineOpen::new(&path, SchemaVersion::new(crate::store::FLOW_SCHEMA_VERSION))
                .with_family(&descriptors.current)
                .with_family(&descriptors.events)
                .with_family(&descriptors.metaflows)
                .with_family(&descriptors.address_holds)
                .with_family(&descriptors.requests)
                .with_family(&descriptors.attempts)
                .with_family(&descriptors.outcomes)
                .with_family(&descriptors.send_calls)
                .with_family(&descriptors.flow_events)
                .with_family(&descriptors.request_ids)
                .with_family(&descriptors.deliver_calls)
                .with_family(&descriptors.store_identity)
                .with_family(&descriptors.prior_deliveries)
                .with_family(&descriptors.task_announcements)
                .with_family(&descriptors.upgrade_assignments)
                .with_family(&descriptors.request_policy)
                .with_family(&heartbeat),
        )
        .expect("current catalog reopens");
        let identity_table = reopened
            .register_table(descriptors.store_identity)
            .expect("identity registers");
        let identities = reopened
            .match_records(QueryPlan::all(identity_table))
            .expect("identity reads");
        assert_eq!(
            identities.records()[0].store_identity,
            signal_flow::StoreIdentity("0000000100020003".into())
        );
        let policy_table = reopened
            .register_table(descriptors.request_policy)
            .expect("policy registers");
        let policy = reopened
            .match_records(QueryPlan::all(policy_table))
            .expect("policy reads");
        assert_eq!(policy.records().len(), 1);
        assert_eq!(
            policy.records()[0].request_id_draws,
            meta_signal_flow::DrawBound(64)
        );
        assert_eq!(
            policy.records()[0].announcement_tries,
            meta_signal_flow::TryBound(3)
        );
    }
}
