//! The Memory 0.54.2 wrote, read and upgraded in place by 0.54.3.
//!
//! 0.54.3 changes the stored shape of seven families:
//! - the living's words are `Living.{ Context Verbatim }` where a request
//!   carried `Psyche.Said`, and `Psyches` is gone, so every request kind
//!   after it moves up one tag: the metaflows, the address holds and the
//!   queue-delivery requests and attempts, which hold requests;
//! - a send tool call's outcome and an ordinary Send's outcome record their
//!   sender as `SentBy`, where they held an Address;
//! - the declared Nexus names the one Herdr session every launch opens in.
//!
//! Each family declares its 0.54.2 identity as a predecessor and is
//! upgraded on open, row by row, before anything is registered: a request
//! keeps its kind and its words, a `Psyches` becomes one Living holding its
//! sayings, a recorded sender Address becomes `Sender`, an absent one
//! `Unknown`, and the Nexus takes Herdr's `default` session, which the
//! deployment's next `Configure.Nexus` replaces. The predecessor records
//! below are the 0.54.2 shapes, built from the current component types
//! wherever the bytes are the same.

use super::queue_delivery::{
    NativeHistoryAnchor, QueueDeliveryAttempt, QueueDeliveryPhase, RequestState,
    StoredDeliveryRequest,
};
use super::{StoreError, StoredMetaflow};
use crate::generated::send_tool::{
    ContentSha256, SendCall, SendOutcome, SendRefusal, Target, ToolCallKey,
};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use sema_engine::{
    Decision, Engine, EngineRecord, MemoryDefinition, MemoryUpgrades, MemoryVersion, RecordKey,
    SchemaHash, UpgradeFrom, UpgradeOutcome,
};
use signal_flow::{Address, HarnessKind, SentBy};

/// Several of the living's sayings as one Living: their contexts, then
/// their words, each kept whole, a blank line apart. One saying is itself.
pub(crate) trait JoinsSayings {
    fn joined(self) -> flow_ethos::Living_Data;
}

impl JoinsSayings for Vec<flow_ethos::Living_Data> {
    fn joined(self) -> flow_ethos::Living_Data {
        let join = |parts: Vec<String>| parts.join("\n\n");
        flow_ethos::Living_Data {
            context: flow_ethos::Context(join(
                self.iter().map(|saying| saying.context.0.clone()).collect(),
            )),
            verbatim: flow_ethos::Verbatim(join(
                self.iter()
                    .map(|saying| saying.verbatim.0.clone())
                    .collect(),
            )),
        }
    }
}

/// A request as 0.54.2 stored it.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) enum RequestBeforeLiving {
    Order(String),
    Question(String),
    Psyche(flow_ethos::Living_Data),
    Psyches(Vec<flow_ethos::Living_Data>),
    Result(flow_ethos::ResultPayload),
    Notice(String),
    Task(flow_ethos::AnswerableTask),
    Done(flow_ethos::DonePayload),
    Reminder(flow_ethos::Reminder_Data),
}

/// A message as 0.54.2 stored it: a request under its priority.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) enum MessageBeforeLiving {
    HardAbrupt(RequestBeforeLiving),
    MiddleAbrupt(RequestBeforeLiving),
    Soft(RequestBeforeLiving),
}

/// A queue entry as 0.54.2 stored it.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct QueueEntryBeforeLiving {
    pub(crate) logical_delivery_id: flow_ethos::LogicalDeliveryId,
    pub(crate) request_id: flow_ethos::RequestId,
    pub(crate) sent_by: SentBy,
    pub(crate) delivery: flow_ethos::Delivery,
    pub(crate) message: MessageBeforeLiving,
}

/// A metaflow as 0.54.2 stored it.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct MetaflowBeforeLiving {
    pub(crate) address: Address,
    pub(crate) state: flow_ethos::State,
    pub(crate) past: flow_ethos::Past,
    pub(crate) queue: QueueBeforeLiving,
}

/// A metaflow's queue as 0.54.2 stored it.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct QueueBeforeLiving(pub(crate) Vec<QueueEntryBeforeLiving>);

/// A queue delivery's evidence as 0.54.2 stored it.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) enum QueueEvidenceBeforeLiving {
    Logical(QueueEntryBeforeLiving),
    Legacy(LegacyEvidenceBeforeLiving),
}

/// Legacy queue evidence as 0.54.2 stored it.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct LegacyEvidenceBeforeLiving {
    pub(crate) sent_by: SentBy,
    pub(crate) request: RequestBeforeLiving,
}

/// What 0.54.2 stored is read into what 0.54.3 stores.
pub(crate) trait BecomesLiving {
    type Current;
    fn living(self) -> Self::Current;
}

impl BecomesLiving for RequestBeforeLiving {
    type Current = flow_ethos::Request;
    fn living(self) -> flow_ethos::Request {
        use flow_ethos::Request;
        match self {
            Self::Order(words) => Request::Order(words),
            Self::Question(words) => Request::Question(words),
            Self::Psyche(saying) => Request::Living(saying),
            Self::Psyches(sayings) => Request::Living(sayings.joined()),
            Self::Result(result) => Request::Result(result),
            Self::Notice(words) => Request::Notice(words),
            Self::Task(task) => Request::Task(task),
            Self::Done(done) => Request::Done(done),
            Self::Reminder(reminder) => Request::Reminder(reminder),
        }
    }
}

impl BecomesLiving for MessageBeforeLiving {
    type Current = flow_ethos::Message;
    fn living(self) -> flow_ethos::Message {
        use flow_ethos::Message;
        match self {
            Self::HardAbrupt(request) => Message::HardAbrupt(request.living()),
            Self::MiddleAbrupt(request) => Message::MiddleAbrupt(request.living()),
            Self::Soft(request) => Message::Soft(request.living()),
        }
    }
}

impl BecomesLiving for QueueEntryBeforeLiving {
    type Current = flow_ethos::QueueEntry;
    fn living(self) -> flow_ethos::QueueEntry {
        flow_ethos::QueueEntry {
            logical_delivery_id: self.logical_delivery_id,
            request_id: self.request_id,
            sent_by: self.sent_by,
            delivery: self.delivery,
            message: self.message.living(),
        }
    }
}

impl BecomesLiving for MetaflowBeforeLiving {
    type Current = flow_ethos::Metaflow;
    fn living(self) -> flow_ethos::Metaflow {
        flow_ethos::Metaflow {
            address: self.address,
            state: self.state,
            past: self.past,
            queue: flow_ethos::Queue(
                self.queue
                    .0
                    .into_iter()
                    .map(BecomesLiving::living)
                    .collect(),
            ),
        }
    }
}

impl BecomesLiving for QueueEvidenceBeforeLiving {
    type Current = meta_signal_flow::QueueEvidence;
    fn living(self) -> meta_signal_flow::QueueEvidence {
        match self {
            Self::Logical(entry) => meta_signal_flow::QueueEvidence::Logical(entry.living()),
            Self::Legacy(legacy) => meta_signal_flow::QueueEvidence::Legacy(
                meta_signal_flow::QueueEvidence_Legacy_Data {
                    sent_by: legacy.sent_by,
                    request: legacy.request.living(),
                },
            ),
        }
    }
}

/// Declares one 0.54.2 record: its key and its identity.
macro_rules! before_living {
    ($record:ident, $version:expr, $label:expr, |$value:ident| $key:expr) => {
        impl EngineRecord for $record {
            fn record_key(&self) -> RecordKey {
                let $value = self;
                RecordKey::new($key)
            }
        }
        impl MemoryDefinition for $record {
            fn memory_version() -> MemoryVersion {
                MemoryVersion::new($version)
            }
            fn schema_hash() -> SchemaHash {
                SchemaHash::for_label($label)
            }
        }
    };
}

/// A metaflow row as 0.54.2 stored it.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct StoredMetaflowBeforeLiving {
    pub(crate) metaflow: MetaflowBeforeLiving,
}
before_living!(
    StoredMetaflowBeforeLiving,
    8,
    "flow-nexus-metaflow-v8",
    |row| <super::FlowStore as super::ReadsFlowMemory>::address_record_key(&row.metaflow.address)
);
impl UpgradeFrom<StoredMetaflowBeforeLiving> for StoredMetaflow {
    type Refusal = std::convert::Infallible;
    fn upgrade_from(before: StoredMetaflowBeforeLiving) -> Decision<Self, Self::Refusal> {
        Decision::Ready(StoredMetaflow {
            metaflow: before.metaflow.living(),
        })
    }
}

/// An address hold as 0.54.2 stored it.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct AddressHoldBeforeLiving {
    pub(crate) flow_id: String,
    pub(crate) address: Address,
    pub(crate) found: Option<MetaflowBeforeLiving>,
}
before_living!(
    AddressHoldBeforeLiving,
    7,
    "flow-nexus-address-hold-v7",
    |row| row.flow_id.clone()
);
impl UpgradeFrom<AddressHoldBeforeLiving> for super::address_hold::AddressHold {
    type Refusal = std::convert::Infallible;
    fn upgrade_from(before: AddressHoldBeforeLiving) -> Decision<Self, Self::Refusal> {
        Decision::Ready(super::address_hold::AddressHold {
            flow_id: flow_ethos::FlowId(before.flow_id),
            address: before.address,
            metaflow_option: before.found.map(BecomesLiving::living),
        })
    }
}

/// A queue-delivery request as 0.54.2 stored it.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct DeliveryRequestBeforeLiving {
    pub(crate) request_id: String,
    pub(crate) address: Address,
    pub(crate) queue_evidence: QueueEvidenceBeforeLiving,
    pub(crate) state: RequestState,
}
before_living!(
    DeliveryRequestBeforeLiving,
    8,
    "flow-nexus-queue-delivery-request-v8",
    |row| row.request_id.clone()
);
impl UpgradeFrom<DeliveryRequestBeforeLiving> for StoredDeliveryRequest {
    type Refusal = std::convert::Infallible;
    fn upgrade_from(before: DeliveryRequestBeforeLiving) -> Decision<Self, Self::Refusal> {
        Decision::Ready(StoredDeliveryRequest {
            queue_request_id: meta_signal_flow::QueueRequestId(before.request_id),
            address: before.address,
            queue_evidence: before.queue_evidence.living(),
            request_state: before.state,
        })
    }
}

/// A queue-delivery attempt as 0.54.2 stored it.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct DeliveryAttemptBeforeLiving {
    pub(crate) request_id: String,
    pub(crate) attempt_id: String,
    pub(crate) native_request_id: u64,
    pub(crate) flow_id: String,
    pub(crate) thread_id: String,
    pub(crate) harness_kind: HarnessKind,
    pub(crate) history_anchor: NativeHistoryAnchor,
    pub(crate) address: Address,
    pub(crate) queue_evidence: QueueEvidenceBeforeLiving,
    pub(crate) phase: QueueDeliveryPhase,
}
before_living!(
    DeliveryAttemptBeforeLiving,
    9,
    "flow-nexus-queue-delivery-attempt-v9",
    |row| row.attempt_id.clone()
);
impl UpgradeFrom<DeliveryAttemptBeforeLiving> for QueueDeliveryAttempt {
    type Refusal = std::convert::Infallible;
    fn upgrade_from(before: DeliveryAttemptBeforeLiving) -> Decision<Self, Self::Refusal> {
        Decision::Ready(QueueDeliveryAttempt {
            queue_request_id: meta_signal_flow::QueueRequestId(before.request_id),
            queue_attempt_id: meta_signal_flow::QueueAttemptId(before.attempt_id),
            native_request_id: crate::generated::memory::NativeRequestId(
                before.native_request_id as i64,
            ),
            flow_id: flow_ethos::FlowId(before.flow_id),
            thread_id: crate::generated::memory::ThreadId(before.thread_id),
            harness_kind: before.harness_kind,
            native_history_anchor: before.history_anchor,
            address: before.address,
            queue_evidence: before.queue_evidence.living(),
            queue_delivery_phase: before.phase,
        })
    }
}

/// A send tool call's outcome as 0.54.2 stored it: its sender an Address.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) enum SendOutcomeBeforeOrigin {
    Accepted(AcceptedBeforeOrigin),
    LegacyUnknown(TargetBeforeOrigin),
    Archived(TargetBeforeOrigin),
    Refused(SendRefusal),
}

/// An accepted call's outcome as 0.54.2 stored it.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct AcceptedBeforeOrigin {
    pub(crate) sender: Address,
    pub(crate) target: Target,
    pub(crate) send_acceptance: signal_flow::SendAcceptance,
}

/// A legacy or archived call's outcome as 0.54.2 stored it.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct TargetBeforeOrigin {
    pub(crate) sender: Address,
    pub(crate) target: Target,
}

/// A send tool call as 0.54.2 stored it.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct SendCallBeforeOrigin {
    pub(crate) tool_call_key: ToolCallKey,
    pub(crate) content_sha256: ContentSha256,
    pub(crate) send_outcome: SendOutcomeBeforeOrigin,
}
before_living!(SendCallBeforeOrigin, 8, "flow-nexus-send-call-v8", |row| {
    use super::send_call::KeysToolCall;
    row.tool_call_key.record_key_text()
});
impl UpgradeFrom<SendCallBeforeOrigin> for SendCall {
    type Refusal = std::convert::Infallible;
    fn upgrade_from(before: SendCallBeforeOrigin) -> Decision<Self, Self::Refusal> {
        use crate::generated::send_tool::{Accepted_Data, Archived_Data, LegacyUnknown_Data};
        let send_outcome = match before.send_outcome {
            SendOutcomeBeforeOrigin::Accepted(accepted) => SendOutcome::Accepted(Accepted_Data {
                sent_by: SentBy::Sender(accepted.sender),
                target: accepted.target,
                send_acceptance: accepted.send_acceptance,
            }),
            SendOutcomeBeforeOrigin::LegacyUnknown(legacy) => {
                SendOutcome::LegacyUnknown(LegacyUnknown_Data {
                    sent_by: SentBy::Sender(legacy.sender),
                    target: legacy.target,
                })
            }
            SendOutcomeBeforeOrigin::Archived(archived) => SendOutcome::Archived(Archived_Data {
                sent_by: SentBy::Sender(archived.sender),
                target: archived.target,
            }),
            SendOutcomeBeforeOrigin::Refused(refusal) => SendOutcome::Refused(refusal),
        };
        Decision::Ready(SendCall {
            tool_call_key: before.tool_call_key,
            content_sha256: before.content_sha256,
            send_outcome,
        })
    }
}

/// An ordinary Send's outcome as 0.54.2 recorded it: its sender, when it
/// had one, an Address.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct SendOutcomeRecordBeforeOrigin {
    pub(crate) send_outcome_id: meta_signal_flow::SendOutcomeId,
    pub(crate) process_option: Option<signal_flow::Process>,
    pub(crate) sender: Option<Address>,
    pub(crate) recipient: Option<Address>,
    pub(crate) request_digest: meta_signal_flow::RequestDigest,
    pub(crate) ordinary_send_outcome: meta_signal_flow::OrdinarySendOutcome,
}

/// The ordinary Send outcome row as 0.54.2 stored it.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct StoredSendOutcomeBeforeOrigin {
    pub(crate) record: SendOutcomeRecordBeforeOrigin,
}
before_living!(
    StoredSendOutcomeBeforeOrigin,
    8,
    "flow-nexus-send-outcome-v8",
    |row| row.record.send_outcome_id.clone().0
);
impl UpgradeFrom<StoredSendOutcomeBeforeOrigin> for super::send_outcome::StoredSendOutcome {
    type Refusal = std::convert::Infallible;
    fn upgrade_from(before: StoredSendOutcomeBeforeOrigin) -> Decision<Self, Self::Refusal> {
        let record = before.record;
        Decision::Ready(super::send_outcome::StoredSendOutcome {
            send_outcome_record: meta_signal_flow::SendOutcomeRecord {
                send_outcome_id: record.send_outcome_id,
                process_option: record.process_option,
                sent_by: record.sender.map_or(SentBy::Unknown, SentBy::Sender),
                address_option: record.recipient,
                request_digest: record.request_digest,
                ordinary_send_outcome: record.ordinary_send_outcome,
            },
        })
    }
}

/// The declared Nexus as 0.54.2 stored it: no Herdr session.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct ConfigureNexusBeforeSession {
    pub(crate) source_root: flow_ethos::SourceRoot,
    pub(crate) first_codex_endpoint: flow_ethos::CodexEndpoint,
    pub(crate) second_codex_endpoint: flow_ethos::CodexEndpoint,
    pub(crate) harness_profiles: flow_ethos::HarnessProfiles,
    pub(crate) meta_aspects: flow_ethos::MetaAspects,
    pub(crate) message_nexus_path_option: Option<flow_ethos::MessageNexusPath>,
    pub(crate) message_nexus_binary_option: Option<flow_ethos::MessageNexusBinary>,
    pub(crate) curriculum_nexus_path: meta_signal_flow::CurriculumNexusPath,
    pub(crate) lease: flow_ethos::Lease,
    pub(crate) presentation_prompt_option: Option<meta_signal_flow::PresentationPrompt>,
}

/// The Nexus row as 0.54.2 stored it.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct StoredNexusBeforeSession {
    pub(crate) nexus: ConfigureNexusBeforeSession,
}
before_living!(
    StoredNexusBeforeSession,
    4,
    "flow-nexus-meta-nexus-configuration-v4",
    |_row| "nexus".to_owned()
);

/// The Herdr session an upgraded Nexus opens launches in until the
/// deployment's `Configure.Nexus` names its own: Herdr's default session,
/// where 0.54.2 opened a launch no flow's pane asked for.
pub(crate) const UPGRADED_HERDR_SESSION: &str = "default";

impl UpgradeFrom<StoredNexusBeforeSession>
    for super::nexus_configuration::StoredMetaNexusConfiguration
{
    type Refusal = std::convert::Infallible;
    fn upgrade_from(before: StoredNexusBeforeSession) -> Decision<Self, Self::Refusal> {
        let nexus = before.nexus;
        Decision::Ready(super::nexus_configuration::StoredMetaNexusConfiguration {
            configure_nexus: meta_signal_flow::ConfigureNexus {
                source_root: nexus.source_root,
                first_codex_endpoint: nexus.first_codex_endpoint,
                second_codex_endpoint: nexus.second_codex_endpoint,
                harness_profiles: nexus.harness_profiles,
                meta_aspects: nexus.meta_aspects,
                message_nexus_path_option: nexus.message_nexus_path_option,
                message_nexus_binary_option: nexus.message_nexus_binary_option,
                curriculum_nexus_path: nexus.curriculum_nexus_path,
                lease: nexus.lease,
                presentation_prompt_option: nexus.presentation_prompt_option,
                herdr_session_name: signal_flow::HerdrSessionName(
                    UPGRADED_HERDR_SESSION.to_owned(),
                ),
            },
        })
    }
}

/// What one family's upgrade from its 0.54.2 identity came to: a family
/// already current is left as it is, and one the store does not hold is
/// registered.
pub(crate) trait SettlesUpgrade {
    fn settled(self, family: &str) -> Result<(), StoreError>;
}

impl<Current, Refusal: std::fmt::Debug> SettlesUpgrade for UpgradeOutcome<Current, Refusal> {
    fn settled(self, family: &str) -> Result<(), StoreError> {
        match self {
            UpgradeOutcome::Upgraded(_) | UpgradeOutcome::AlreadyCurrent(_) => Ok(()),
            UpgradeOutcome::Refused(refusal) => Err(StoreError::LivingUpgrade(format!(
                "{family} refused: {refusal:?}"
            ))),
            UpgradeOutcome::Failed(failure) => Err(StoreError::LivingUpgrade(format!(
                "{family} failed: {failure:?}"
            ))),
        }
    }
}

/// The 0.54.2 identity of a family 0.54.3 upgrades, by its table: what a
/// store 0.54.2 wrote holds there and 0.54.3 opens.
pub(crate) trait NamesIdentityBeforeLiving {
    fn identity_before_living(table_key: &str) -> Option<(&'static str, SchemaHash)>;
}

impl NamesIdentityBeforeLiving for super::FlowStore {
    fn identity_before_living(table_key: &str) -> Option<(&'static str, SchemaHash)> {
        let (family, label) = match table_key {
            "flow_nexus_metaflows" => ("flow-nexus-metaflow", "flow-nexus-metaflow-v8"),
            "flow_nexus_address_holds" => ("flow-nexus-address-hold", "flow-nexus-address-hold-v7"),
            "flow_nexus_queue_delivery_requests" => (
                "flow-nexus-queue-delivery-request",
                "flow-nexus-queue-delivery-request-v8",
            ),
            "flow_nexus_queue_delivery_attempts" => (
                "flow-nexus-queue-delivery-attempt",
                "flow-nexus-queue-delivery-attempt-v9",
            ),
            "flow_nexus_send_calls" => ("flow-nexus-send-call", "flow-nexus-send-call-v8"),
            "flow_nexus_send_outcomes" => ("flow-nexus-send-outcome", "flow-nexus-send-outcome-v8"),
            "flow_nexus_meta_nexus_configuration" => (
                "flow-nexus-meta-nexus-configuration",
                "flow-nexus-meta-nexus-configuration-v4",
            ),
            _ => return None,
        };
        Some((family, SchemaHash::for_label(label)))
    }
}

/// Upgrades every family 0.54.3 changes that the store holds at its 0.54.2
/// identity. The configuration-only store holds the Nexus alone.
pub(crate) trait UpgradesBeforeLiving {
    fn upgrade_configuration_before_living(engine: &mut Engine) -> Result<(), StoreError>;
    fn upgrade_running_before_living(engine: &mut Engine) -> Result<(), StoreError>;
}

impl UpgradesBeforeLiving for super::FlowStore {
    fn upgrade_configuration_before_living(engine: &mut Engine) -> Result<(), StoreError> {
        use super::nexus_configuration::{
            DescribesNexusConfigurationTable, StoredMetaNexusConfiguration,
        };
        engine
            .upgrade_table::<StoredNexusBeforeSession, StoredMetaNexusConfiguration>(
                StoredMetaNexusConfiguration::descriptor(),
            )
            .settled("the Nexus")
    }

    fn upgrade_running_before_living(engine: &mut Engine) -> Result<(), StoreError> {
        use super::DescribesMetaflowTable;
        use super::address_hold::{AddressHold, DescribesAddressHoldTable};
        use super::queue_delivery::{DeclaresQueueDeliveryTables, QueueDeliveryTables};
        use super::send_call::DescribesSendCallTable;
        use super::send_outcome::{
            DeclaresSendOutcomeTables, SendOutcomeTables, StoredSendOutcome,
        };
        Self::upgrade_configuration_before_living(engine)?;
        engine
            .upgrade_table::<StoredMetaflowBeforeLiving, StoredMetaflow>(
                <super::FlowStore as DescribesMetaflowTable>::metaflow_descriptor(),
            )
            .settled("the metaflows")?;
        engine
            .upgrade_table::<AddressHoldBeforeLiving, AddressHold>(AddressHold::descriptor())
            .settled("the address holds")?;
        let (_, requests, attempts) = QueueDeliveryTables::descriptors();
        engine
            .upgrade_table::<DeliveryRequestBeforeLiving, StoredDeliveryRequest>(requests)
            .settled("the queue-delivery requests")?;
        engine
            .upgrade_table::<DeliveryAttemptBeforeLiving, QueueDeliveryAttempt>(attempts)
            .settled("the queue-delivery attempts")?;
        engine
            .upgrade_table::<SendCallBeforeOrigin, SendCall>(SendCall::descriptor())
            .settled("the send calls")?;
        engine
            .upgrade_table::<StoredSendOutcomeBeforeOrigin, StoredSendOutcome>(
                SendOutcomeTables::descriptors().outcomes,
            )
            .settled("the send outcomes")?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::DescribesMetaflowTable;
    use crate::store::send_call::DescribesSendCallTable;
    use sema_engine::{
        Assertion, EngineOpen, FamilyName, QueryPlan, SchemaVersion, TableDescriptor,
    };

    fn address(topic: &str) -> Address {
        Address {
            aspect: signal_flow::Aspect::Mind,
            topic: flow_ethos::Topic(topic.into()),
            model: signal_flow::Model::Sol,
        }
    }

    fn saying(context: &str, verbatim: &str) -> flow_ethos::Living_Data {
        flow_ethos::Living_Data {
            context: flow_ethos::Context(context.into()),
            verbatim: flow_ethos::Verbatim(verbatim.into()),
        }
    }

    fn entry(id: &str, message: MessageBeforeLiving) -> QueueEntryBeforeLiving {
        QueueEntryBeforeLiving {
            logical_delivery_id: flow_ethos::LogicalDeliveryId(format!("logical-{id}")),
            request_id: flow_ethos::RequestId(id.into()),
            sent_by: SentBy::Unbound,
            delivery: flow_ethos::Delivery::Queue,
            message,
        }
    }

    /// Every request kind 0.54.2 could hold, in its 0.54.2 tags.
    fn requests_before() -> Vec<RequestBeforeLiving> {
        vec![
            RequestBeforeLiving::Order("order".into()),
            RequestBeforeLiving::Question("question".into()),
            RequestBeforeLiving::Psyche(saying("the drain", "Fix it today.")),
            RequestBeforeLiving::Psyches(vec![saying("first", "One."), saying("second", "Two.")]),
            RequestBeforeLiving::Result(flow_ethos::ResultPayload::Historical("result".into())),
            RequestBeforeLiving::Notice("notice".into()),
            RequestBeforeLiving::Task(flow_ethos::AnswerableTask {
                minutes: flow_ethos::Minutes(30),
                string: "task".into(),
            }),
            RequestBeforeLiving::Done(flow_ethos::DonePayload::Historical(
                flow_ethos::Historical_Data {
                    logical_delivery_id: flow_ethos::LogicalDeliveryId("logical-task".into()),
                    string: "done".into(),
                },
            )),
            RequestBeforeLiving::Reminder(flow_ethos::Reminder_Data {
                request_id: flow_ethos::RequestId("rmnd".into()),
                string: "reminder".into(),
            }),
        ]
    }

    /// The same requests as 0.54.3 holds them: each kind kept, Psyche the
    /// one saying of Living, Psyches its sayings joined into one.
    fn requests_now() -> Vec<flow_ethos::Request> {
        use flow_ethos::Request;
        vec![
            Request::Order("order".into()),
            Request::Question("question".into()),
            Request::Living(saying("the drain", "Fix it today.")),
            Request::Living(saying("first\n\nsecond", "One.\n\nTwo.")),
            Request::Result(flow_ethos::ResultPayload::Historical("result".into())),
            Request::Notice("notice".into()),
            Request::Task(flow_ethos::AnswerableTask {
                minutes: flow_ethos::Minutes(30),
                string: "task".into(),
            }),
            Request::Done(flow_ethos::DonePayload::Historical(
                flow_ethos::Historical_Data {
                    logical_delivery_id: flow_ethos::LogicalDeliveryId("logical-task".into()),
                    string: "done".into(),
                },
            )),
            Request::Reminder(flow_ethos::Reminder_Data {
                request_id: flow_ethos::RequestId("rmnd".into()),
                string: "reminder".into(),
            }),
        ]
    }

    /// A metaflow queue 0.54.2 stored, every request kind in its old tags,
    /// opens in 0.54.3 upgraded: each request keeps its kind, its id and
    /// its words; Psyche is Living, Psyches one Living of its sayings. A
    /// send call's sender Address and a Send outcome's become `SentBy`. A
    /// second open finds it all current.
    #[test]
    fn a_0542_memory_opens_upgraded_with_every_request_kept() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("flow-0542.sema");
        let queue = requests_before()
            .into_iter()
            .enumerate()
            .map(|(index, request)| {
                entry(&format!("r{index:03}"), MessageBeforeLiving::Soft(request))
            })
            .collect::<Vec<_>>();
        let metaflow = StoredMetaflowBeforeLiving {
            metaflow: MetaflowBeforeLiving {
                address: address("upgraded"),
                state: flow_ethos::State::Asleep,
                past: flow_ethos::Past(vec![]),
                queue: QueueBeforeLiving(queue),
            },
        };
        let key = ToolCallKey {
            thread_id: crate::generated::send_tool::ThreadId("thread".into()),
            turn_id: crate::generated::send_tool::TurnId("turn".into()),
            call_id: crate::generated::send_tool::CallId("call".into()),
        };
        let call = SendCallBeforeOrigin {
            tool_call_key: key.clone(),
            content_sha256: ContentSha256("digest".into()),
            send_outcome: SendOutcomeBeforeOrigin::LegacyUnknown(TargetBeforeOrigin {
                sender: address("caller"),
                target: Target(address("upgraded")),
            }),
        };
        let outcome = StoredSendOutcomeBeforeOrigin {
            record: SendOutcomeRecordBeforeOrigin {
                send_outcome_id: meta_signal_flow::SendOutcomeId("send-outcome-1".into()),
                process_option: None,
                sender: None,
                recipient: Some(address("upgraded")),
                request_digest: meta_signal_flow::RequestDigest("abc".into()),
                ordinary_send_outcome: meta_signal_flow::OrdinarySendOutcome::Refused(
                    signal_flow::Refusal::Lapsed,
                ),
            },
        };
        {
            let mut engine = Engine::open(EngineOpen::new(&path, SchemaVersion::new(1))).unwrap();
            let metaflows = engine
                .register_table(TableDescriptor::<StoredMetaflowBeforeLiving>::new(
                    super::super::METAFLOW_TABLE_NAME,
                    FamilyName::new("flow-nexus-metaflow"),
                    StoredMetaflowBeforeLiving::schema_hash(),
                ))
                .unwrap();
            let calls = engine
                .register_table(TableDescriptor::<SendCallBeforeOrigin>::new(
                    super::super::send_call::SEND_CALL_TABLE_NAME,
                    FamilyName::new("flow-nexus-send-call"),
                    SendCallBeforeOrigin::schema_hash(),
                ))
                .unwrap();
            let outcomes = engine
                .register_table(TableDescriptor::<StoredSendOutcomeBeforeOrigin>::new(
                    sema_engine::TableName::new("flow_nexus_send_outcomes"),
                    FamilyName::new("flow-nexus-send-outcome"),
                    StoredSendOutcomeBeforeOrigin::schema_hash(),
                ))
                .unwrap();
            engine.assert(Assertion::new(metaflows, metaflow)).unwrap();
            engine.assert(Assertion::new(calls, call)).unwrap();
            engine.assert(Assertion::new(outcomes, outcome)).unwrap();
        }
        for _ in 0..2 {
            let mut engine = Engine::open(EngineOpen::new(&path, SchemaVersion::new(1))).unwrap();
            <super::super::FlowStore as UpgradesBeforeLiving>::upgrade_running_before_living(
                &mut engine,
            )
            .unwrap();
            let metaflows = engine
                .register_table(
                    <super::super::FlowStore as DescribesMetaflowTable>::metaflow_descriptor(),
                )
                .unwrap();
            let stored = engine
                .match_records(QueryPlan::all(metaflows))
                .unwrap()
                .records()
                .to_vec();
            assert_eq!(stored.len(), 1);
            let requests = stored[0]
                .metaflow
                .queue
                .0
                .iter()
                .map(|entry| match &entry.message {
                    flow_ethos::Message::Soft(request) => request.clone(),
                    other => panic!("a Soft request upgraded as {other:?}"),
                })
                .collect::<Vec<_>>();
            assert_eq!(requests, requests_now());
            assert_eq!(
                stored[0].metaflow.queue.0[3].request_id,
                flow_ethos::RequestId("r003".into())
            );
            let calls = engine.register_table(SendCall::descriptor()).unwrap();
            let call = engine
                .match_records(QueryPlan::all(calls))
                .unwrap()
                .records()[0]
                .clone();
            assert_eq!(
                call.send_outcome,
                SendOutcome::LegacyUnknown(crate::generated::send_tool::LegacyUnknown_Data {
                    sent_by: SentBy::Sender(address("caller")),
                    target: Target(address("upgraded")),
                })
            );
            assert_eq!(call.tool_call_key, key);
            let outcomes = engine
                .register_table(
                    <super::super::send_outcome::SendOutcomeTables as super::super::send_outcome::DeclaresSendOutcomeTables>::descriptors()
                        .outcomes,
                )
                .unwrap();
            let outcome = engine
                .match_records(QueryPlan::all(outcomes))
                .unwrap()
                .records()[0]
                .clone();
            assert_eq!(outcome.send_outcome_record.sent_by, SentBy::Unknown);
            assert_eq!(
                outcome.send_outcome_record.address_option,
                Some(address("upgraded"))
            );
        }
    }

    /// The Nexus 0.54.2 declared opens with Herdr's `default` session,
    /// every other field as it was declared.
    #[test]
    fn a_0542_nexus_takes_the_default_herdr_session() {
        let endpoint = flow_ethos::CodexEndpoint {
            client_path: flow_ethos::ClientPath("/codex".into()),
            home: flow_ethos::Home("/home".into()),
            control_socket_path: flow_ethos::ControlSocketPath("/socket".into()),
            model_name_vector: vec![],
        };
        let before = StoredNexusBeforeSession {
            nexus: ConfigureNexusBeforeSession {
                source_root: flow_ethos::SourceRoot("/source".into()),
                first_codex_endpoint: endpoint.clone(),
                second_codex_endpoint: endpoint,
                harness_profiles: flow_ethos::HarnessProfiles(vec![]),
                meta_aspects: flow_ethos::MetaAspects(vec![]),
                message_nexus_path_option: None,
                message_nexus_binary_option: None,
                curriculum_nexus_path: meta_signal_flow::CurriculumNexusPath("/c.sock".into()),
                lease: flow_ethos::Lease(60),
                presentation_prompt_option: None,
            },
        };
        let Decision::Ready(now) =
            super::super::nexus_configuration::StoredMetaNexusConfiguration::upgrade_from(before);
        assert_eq!(now.configure_nexus.herdr_session_name.0, "default");
        assert_eq!(now.configure_nexus.source_root.0, "/source");
        assert_eq!(now.configure_nexus.lease.0, 60);
    }
}
