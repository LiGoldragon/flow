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
                binding.logical_delivery_id == logical_id && binding.closure == RequestClosure::Open
            }) && currents.records().iter().any(|current| {
                current.snapshot.logical_delivery_id == logical_id && current.archived.is_none()
            })
        };
        let request_id = |logical_id: &str| {
            bindings
                .records()
                .iter()
                .find(|binding| binding.logical_delivery_id == logical_id)
                .map(|binding| binding.request_id.clone())
        };
        let task_words = |logical_id: &str| {
            retained.iter().find_map(|value| {
                if value.entry.logical_delivery_id != logical_id {
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
                    if value.entry.logical_delivery_id != logical_id {
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
                            .then(|| prior_order_words(&value.request))
                            .flatten()
                    })
                })
        };

        let task_rows = self
            .engine
            .match_records(QueryPlan::all(self.request_id_upgrade.task_announcements))?;
        for mut row in task_rows.records().to_vec() {
            if settle_notice_status(self, &mut row.status, row.tries, policy.announcement_tries)? {
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
                if open(logical_id) {
                    let id = request_id(logical_id).ok_or(StoreError::StateInvariant)?;
                    let words = task_words(logical_id).ok_or(StoreError::StateInvariant)?;
                    lines.push(render_bounded_line(
                        TASK_ANNOUNCEMENT_HEADER,
                        &format!("{id} "),
                        &words,
                    ));
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
            admit_upgrade_notice(
                self,
                row.recipient.clone(),
                text,
                UpgradeNoticeAdmission::Task(row),
            )?;
        }

        let assignment_rows = self
            .engine
            .match_records(QueryPlan::all(self.request_id_upgrade.upgrade_assignments))?;
        for mut row in assignment_rows.records().to_vec() {
            if settle_notice_status(self, &mut row.status, row.tries, policy.announcement_tries)? {
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
                    .any(|assigned| open(&assigned.order))
                    || decision.open.iter().any(|order| open(&order.order))
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
                    let id = request_id(&assigned.order)
                        .map(|id| id.to_string())
                        .unwrap_or_else(|| "archived".into());
                    let words = order_words(&assigned.order).ok_or(StoreError::StateInvariant)?;
                    lines.push(render_bounded_line(
                        ASSIGNMENT_HEADER,
                        &format!("{id} answered "),
                        &words,
                    ));
                }
                for order in &decision.open {
                    let id = request_id(&order.order)
                        .map(|id| id.to_string())
                        .unwrap_or_else(|| "archived".into());
                    let words = order_words(&order.order).ok_or(StoreError::StateInvariant)?;
                    lines.push(render_bounded_line(
                        ASSIGNMENT_HEADER,
                        &format!("{id} open "),
                        &words,
                    ));
                }
            }
            let text = format!("{ASSIGNMENT_HEADER}\n{}", lines.join("\n"));
            admit_upgrade_notice(
                self,
                row.sender.clone(),
                text,
                UpgradeNoticeAdmission::Assignment(row),
            )?;
        }
        Ok(())
    }
}

fn settle_notice_status(
    store: &super::FlowStore,
    status: &mut TaskAnnouncementStatus,
    tries: u64,
    bound: i64,
) -> Result<bool, StoreError> {
    use super::logical_delivery::RecordsLogicalDeliveries;
    let TaskAnnouncementStatus::Admitted(logical_id) = status else {
        return Ok(false);
    };
    let Some(snapshot) = store.logical_delivery_snapshot(logical_id)? else {
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
    store: &super::FlowStore,
    address: Address,
    text: String,
    transition: UpgradeNoticeAdmission,
) -> Result<(), StoreError> {
    use super::logical_delivery::{LogicalDeliveryTransition, RecordsLogicalDeliveries};
    use sema_engine::KeyedMutation;
    let response = store.accept_logical_delivery_for_with_transition(
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
                    store.engine.mutate_keyed(KeyedMutation::new(
                        store.request_id_upgrade.task_announcements,
                        row.record_key(),
                        row,
                    ))?;
                }
            }
            UpgradeNoticeAdmission::Assignment(mut row) => {
                if row.status != TaskAnnouncementStatus::Pending(Some(refusal.clone())) {
                    row.status = TaskAnnouncementStatus::Pending(Some(refusal));
                    store.engine.mutate_keyed(KeyedMutation::new(
                        store.request_id_upgrade.upgrade_assignments,
                        row.record_key(),
                        row,
                    ))?;
                }
            }
        }
    }
    Ok(())
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

fn prior_order_words(request: &PriorStoredDeliveryRequest) -> Option<String> {
    match prior_request(request) {
        Some(old_flow::Request::Order(words)) => Some(words.clone()),
        _ => None,
    }
}

fn prior_task_words(request: &PriorStoredDeliveryRequest) -> Option<String> {
    match prior_request(request) {
        Some(old_flow::Request::Task(task)) => Some(task.string.clone()),
        _ => None,
    }
}

fn render_bounded_line(header: &str, prefix: &str, words: &str) -> String {
    let maximum = crate::sending::MAX_SEND_PAYLOAD_BYTES;
    let fixed = header.len().saturating_add(1).saturating_add(prefix.len());
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

fn partition_rows<T: Clone>(
    header: &str,
    rows: impl IntoIterator<Item = (T, String)>,
) -> Vec<Vec<T>> {
    let mut parts = Vec::new();
    let mut current = Vec::new();
    let mut bytes = header.len();
    for (item, line) in rows {
        let addition = 1usize.saturating_add(line.len());
        if !current.is_empty()
            && bytes.saturating_add(addition) > crate::sending::MAX_SEND_PAYLOAD_BYTES
        {
            parts.push(std::mem::take(&mut current));
            bytes = header.len();
        }
        bytes = bytes.saturating_add(addition);
        current.push(item);
    }
    if !current.is_empty() {
        parts.push(current);
    }
    parts
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

fn answer_group_key(
    recipient: &old_flow::Address,
    sender: &old_flow::Address,
    words: &str,
) -> String {
    let recipient = address_key(recipient);
    let sender = address_key(sender);
    format!(
        "{}:{recipient}:{}:{sender}:{}:{words}",
        recipient.len(),
        sender.len(),
        words.len()
    )
}

/// Evolves the exact accepted 0.53.2 running Memory in one engine commit.
/// Every check and projection is completed before the commit is built.
#[allow(clippy::too_many_arguments)]
pub(crate) fn evolve_request_id_memory(
    engine: &mut Engine,
    descriptors: RequestIdEvolutionDescriptors,
    reminders: &[super::reminder::Reminder],
    simplified_replies: &[super::logical_delivery::SimplifiedReplyBinding],
    purposes: &[super::logical_delivery::LogicalDeliveryPurpose],
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

    // Reconstruct the 0.53.2 answer matcher by immutable delivery order.  Its
    // mutable current cursor is deliberately not an ordering key here.
    let mut order_groups: BTreeMap<
        String,
        (
            old_flow::Address,
            old_flow::Address,
            Vec<(String, Option<i64>, i64)>,
        ),
    > = BTreeMap::new();
    let mut answer_groups: BTreeMap<String, Vec<(String, i64)>> = BTreeMap::new();
    for (logical_id, request) in &requests {
        let admission =
            first_event_cursor(events.get(logical_id).ok_or(StoreError::StateInvariant)?)
                .ok_or(StoreError::StateInvariant)?;
        match prior_request(request) {
            Some(old_flow::Request::Order(words)) => {
                let Some(sender) = prior_sender(request) else {
                    continue;
                };
                let recipient = &request.address;
                order_groups
                    .entry(answer_group_key(recipient, sender, words))
                    .or_insert_with(|| (recipient.clone(), sender.clone(), Vec::new()))
                    .2
                    .push((
                        logical_id.clone(),
                        first_delivered_cursor(&events[logical_id]),
                        admission,
                    ));
            }
            Some(old_flow::Request::Result(old_flow::ResultPayload::Answered(answer))) => {
                let old_flow::Answerable::Order(words) = &answer.answerable else {
                    continue;
                };
                let Some(recipient) = prior_sender(request) else {
                    continue;
                };
                let sender = &request.address;
                answer_groups
                    .entry(answer_group_key(recipient, sender, words))
                    .or_default()
                    .push((logical_id.clone(), admission));
            }
            _ => {}
        }
    }
    let mut assignment_decisions: BTreeMap<String, (Address, Vec<(String, AssignmentDecision)>)> =
        BTreeMap::new();
    for (group_key, (recipient, sender, mut orders)) in order_groups {
        let mut answers = answer_groups.remove(&group_key).unwrap_or_default();
        if orders.len() < 2 || answers.is_empty() {
            continue;
        }
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
                closures.insert(order.clone(), RequestClosure::Answered(answer.clone()));
            }
            assigned.push(AssignedAnswer {
                answer,
                order: order.clone(),
                order_id: request_ids.get(order).cloned(),
            });
        }
        let open = orders
            .iter()
            .filter(|(order, _, _)| held.contains(order) && !used.contains(order))
            .map(|(order, _, _)| OpenOrder {
                order: order.clone(),
                order_id: request_ids[order].clone(),
            })
            .collect();
        let sender = current_address(&sender)?;
        let recipient = current_address(&recipient)?;
        let first_order = orders
            .iter()
            .map(|(order, _, _)| order)
            .min()
            .cloned()
            .ok_or(StoreError::StateInvariant)?;
        assignment_decisions
            .entry(current_address_key(&sender))
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
                snapshot: current_snapshot(
                    &old.snapshot,
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
    let mut task_announcements = Vec::new();
    for (_, (recipient, mut tasks)) in announcement_groups {
        tasks.sort();
        let rows = tasks
            .into_iter()
            .map(|logical_id| {
                let words = prior_task_words(
                    requests
                        .get(&logical_id)
                        .ok_or(StoreError::StateInvariant)?,
                )
                .ok_or(StoreError::StateInvariant)?;
                let line = render_bounded_line(
                    TASK_ANNOUNCEMENT_HEADER,
                    &format!("{LONGEST_TASK_ID} "),
                    &words,
                );
                Ok((logical_id, line))
            })
            .collect::<Result<Vec<_>, StoreError>>()?;
        for (part, tasks) in partition_rows(TASK_ANNOUNCEMENT_HEADER, rows)
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
                let words = prior_order_words(
                    requests
                        .get(&assigned.order)
                        .ok_or(StoreError::StateInvariant)?,
                )
                .ok_or(StoreError::StateInvariant)?;
                let line = render_bounded_line(
                    ASSIGNMENT_HEADER,
                    &format!("{LONGEST_ASSIGNMENT_ID} answered "),
                    &words,
                );
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
                let words = prior_order_words(
                    requests
                        .get(&open.order)
                        .ok_or(StoreError::StateInvariant)?,
                )
                .ok_or(StoreError::StateInvariant)?;
                let line = render_bounded_line(
                    ASSIGNMENT_HEADER,
                    &format!("{LONGEST_ASSIGNMENT_ID} open "),
                    &words,
                );
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
        for (part, decisions) in partition_rows(ASSIGNMENT_HEADER, rows)
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
        .register(descriptors.upgrade_assignments, upgrade_assignments)
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
                values.iter().map(|value| (*value).into()).collect(),
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
            metaflows: TableDescriptor::new(
                crate::store::METAFLOW_TABLE_NAME,
                FamilyName::new("flow-nexus-metaflow"),
                crate::store::StoredMetaflow::schema_hash(),
            )
            .with_predecessor::<PriorStoredMetaflow>(),
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
        let first = render_bounded_line(TASK_ANNOUNCEMENT_HEADER, "0000 ", &words);
        assert!(first.ends_with('…'));
        assert!(
            TASK_ANNOUNCEMENT_HEADER.len() + 1 + first.len()
                <= crate::sending::MAX_SEND_PAYLOAD_BYTES
        );
        let second = render_bounded_line(TASK_ANNOUNCEMENT_HEADER, "0001 ", &words);
        let parts = partition_rows(
            TASK_ANNOUNCEMENT_HEADER,
            [("first", first), ("second", second)],
        );
        assert_eq!(parts, vec![vec!["first"], vec!["second"]]);
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
                .with_family(&heartbeat),
        )
        .expect("predecessor catalog opens for evolution");
        let heartbeat_table = engine.register_table(heartbeat).expect("heartbeat opens");
        evolve_request_id_memory(
            &mut engine,
            descriptors,
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
        assert_eq!(assignment.decisions[0].assigned[0].order, "order-first");
        assert_eq!(assignment.decisions[0].assigned[0].order_id, None);
        assert_eq!(assignment.decisions[1].open[0].order, "order-second");
        assert_eq!(assignment.decisions[1].open[0].order_id, "a001");
        let prior_table = engine
            .register_table(descriptors.prior_deliveries)
            .expect("prior deliveries register");
        let prior = engine
            .match_records(QueryPlan::all(prior_table))
            .expect("prior deliveries read");
        assert_eq!(prior.records().len(), 2);
        assert_eq!(
            prior_order_words(
                &prior
                    .records()
                    .iter()
                    .find(|row| row.logical_delivery_id == "order-first")
                    .expect("first Order archived")
                    .request
            )
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
    fn evolution_orders_undelivered_orders_by_admission_and_never_assigns_a_later_order() {
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
        let after_answer = old_snapshot(
            5,
            "order-after-answer",
            &sender,
            &recipient,
            old_signal::DeliveryState::Queued,
        );
        for snapshot in [
            early.clone(),
            late.clone(),
            answer_delivered.clone(),
            after_answer.clone(),
        ] {
            predecessor
                .assert(Assertion::new(
                    tables.current,
                    PriorCurrentLogicalDelivery { snapshot },
                ))
                .expect("old current stores");
        }
        for snapshot in [early, late, answer_queued, answer_delivered, after_answer] {
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
        ] {
            let (from, to) = if logical_id == "answer" {
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
                .with_family(&heartbeat),
        )
        .expect("predecessor catalog opens for evolution");
        let heartbeat_table = engine.register_table(heartbeat).expect("heartbeat opens");
        evolve_request_id_memory(
            &mut engine,
            descriptors,
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
        assert_eq!(assigned.len(), 1);
        assert_eq!(assigned[0].order, "order-early");
        assert_eq!(
            assignment
                .decisions
                .iter()
                .flat_map(|decision| &decision.open)
                .map(|open| open.order.as_str())
                .collect::<std::collections::BTreeSet<_>>(),
            std::collections::BTreeSet::from(["order-after-answer", "order-late"])
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
            address: current_address(&recipient).expect("address recodes"),
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
            address: current_address(&sender).expect("sender recodes"),
            recipient: current_address(&recipient).expect("recipient recodes"),
            minutes: 5,
            due: Some(123),
            sent: 0,
        };
        let reply = super::super::logical_delivery::SimplifiedReplyBinding {
            flow_id: "reply-flow".into(),
            input_delivery_id: "input-held-by-reply".into(),
            source: current_address(&sender).expect("source recodes"),
            recipient: current_address(&recipient).expect("recipient recodes"),
            completed: false,
        };
        let purposes = [
            super::super::logical_delivery::LogicalDeliveryPurpose {
                logical_delivery_id: "queued-held".into(),
                address: current_address(&recipient).expect("recipient recodes"),
                purpose: crate::heartbeat::DeliveryPurpose::Ordinary,
            },
            super::super::logical_delivery::LogicalDeliveryPurpose {
                logical_delivery_id: "delivered-moved".into(),
                address: current_address(&recipient).expect("recipient recodes"),
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
                .with_family(&heartbeat),
        )
        .expect("predecessor catalog opens for evolution");
        let heartbeat_table = engine
            .register_table(heartbeat.clone())
            .expect("heartbeat opens");
        evolve_request_id_memory(
            &mut engine,
            descriptors,
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
            row.snapshot.logical_delivery_id != "delivered-moved"
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
            vec!["task-held-by-reminder"]
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
        let metaflow = old_flow::Metaflow {
            address: recipient.clone(),
            state: old_flow::State::Asleep,
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
                .with_family(&heartbeat),
        )
        .expect("predecessor catalog opens for evolution");
        let heartbeat_table = engine.register_table(heartbeat).expect("heartbeat opens");
        evolve_request_id_memory(
            &mut engine,
            descriptors,
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
        assert_eq!(metaflows.records()[0].metaflow.queue[0].request_id, "held");
        let holds = engine
            .register_table(descriptors.address_holds)
            .expect("holds register");
        let holds = engine
            .match_records(QueryPlan::all(holds))
            .expect("holds read");
        assert_eq!(
            holds.records()[0].found.as_ref().expect("found kept").queue[0].request_id,
            "held"
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
        assert_eq!(entry.request_id, "held");
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
        assert_eq!(acceptance.request_id, "held");
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
        assert_eq!(accepted.send_acceptance.request_id, "held");
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
                .with_family(&heartbeat),
        )
        .expect("predecessor catalog opens for validation");
        let heartbeat_table = engine.register_table(heartbeat).expect("heartbeat opens");
        let before = std::fs::read(&path).expect("Memory bytes read before evolution");
        assert!(matches!(
            evolve_request_id_memory(
                &mut engine,
                descriptors,
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
            evolve_request_id_memory(
                &mut engine,
                descriptors,
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
                .with_family(&heartbeat),
        )
        .expect("exact predecessor catalog is recognized");
        let heartbeat_table = engine
            .register_table(heartbeat.clone())
            .expect("heartbeat opens");
        evolve_request_id_memory(
            &mut engine,
            descriptors,
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
        assert_eq!(identities.records()[0].store_identity, "0000000100020003");
        let policy_table = reopened
            .register_table(descriptors.request_policy)
            .expect("policy registers");
        let policy = reopened
            .match_records(QueryPlan::all(policy_table))
            .expect("policy reads");
        assert_eq!(policy.records().len(), 1);
        assert_eq!(policy.records()[0].request_id_draws, 64);
        assert_eq!(policy.records()[0].announcement_tries, 3);
    }
}
