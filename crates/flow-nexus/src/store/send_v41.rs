//! A send's answers as signal-flow 35.0.0 archived them, before Unresolved and Ambiguous were added to Refusal. This file freezes the exact persisted v0.41 send outcome shape for upgrade only.

use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use signal_flow::{
    DeliverySnapshot, DeliveryState, NativeReceipt, Refusal, RetryCause, SeatEvidence,
    SendAcceptance, SendFailure, SendFailureReason, SendPayloadSize, Unknown_Data,
};

/// The v0.41 Refusal before answer-correlation outcomes.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) enum RefusalV41 {
    Locked,
    Held(flow_ethos::Lock),
    Awake(flow_ethos::FlowId),
    Asleep,
    Lapsed,
    Unknown(Unknown_Data),
    NoLayer,
    NotConfigured,
    NoneAbove,
    NotMessage,
    Taken(flow_ethos::Address),
    Unidentified(SeatEvidence),
    Store(String),
    NotSendServer(flow_ethos::Process),
    Oversized(SendPayloadSize),
    Direct(String),
    TopicRefused(flow_ethos::Topic),
    NoOpenTask(flow_ethos::LogicalDeliveryId),
    MinutesRefused(signal_flow::MinutesRefusal),
}

impl From<RefusalV41> for Refusal {
    fn from(refusal: RefusalV41) -> Self {
        match refusal {
            RefusalV41::Locked => Self::Locked,
            RefusalV41::Held(lock) => Self::Held(lock),
            RefusalV41::Awake(flow_id) => Self::Awake(flow_id),
            RefusalV41::Asleep => Self::Asleep,
            RefusalV41::Lapsed => Self::Lapsed,
            RefusalV41::Unknown(unknown) => Self::Unknown(unknown),
            RefusalV41::NoLayer => Self::NoLayer,
            RefusalV41::NotConfigured => Self::NotConfigured,
            RefusalV41::NoneAbove => Self::NoneAbove,
            RefusalV41::NotMessage => Self::NotMessage,
            RefusalV41::Taken(address) => Self::Taken(address),
            RefusalV41::Unidentified(evidence) => Self::Unidentified(evidence),
            RefusalV41::Store(reason) => Self::Store(reason),
            RefusalV41::NotSendServer(process) => Self::NotSendServer(process),
            RefusalV41::Oversized(size) => Self::Oversized(size),
            RefusalV41::Direct(reason) => Self::Direct(reason),
            RefusalV41::TopicRefused(topic) => Self::TopicRefused(topic),
            RefusalV41::NoOpenTask(id) => Self::NoOpenTask(id),
            RefusalV41::MinutesRefused(refusal) => Self::MinutesRefused(refusal),
        }
    }
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) enum DeliveryStateV41 {
    Queued,
    Attempting,
    Retryable(RetryCause),
    Uncertain,
    Delivered(NativeReceipt),
    Failed(RefusalV41),
    Withdrawn,
}

impl From<DeliveryStateV41> for DeliveryState {
    fn from(state: DeliveryStateV41) -> Self {
        match state {
            DeliveryStateV41::Queued => Self::Queued,
            DeliveryStateV41::Attempting => Self::Attempting,
            DeliveryStateV41::Retryable(cause) => Self::Retryable(cause),
            DeliveryStateV41::Uncertain => Self::Uncertain,
            DeliveryStateV41::Delivered(receipt) => Self::Delivered(receipt),
            DeliveryStateV41::Failed(refusal) => Self::Failed(refusal.into()),
            DeliveryStateV41::Withdrawn => Self::Withdrawn,
        }
    }
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct DeliverySnapshotV41 {
    pub(crate) delivery_cursor: i64,
    pub(crate) logical_delivery_id: String,
    pub(crate) sent_by: flow_ethos::SentBy,
    pub(crate) address: flow_ethos::Address,
    pub(crate) delivery_state: DeliveryStateV41,
}

impl From<DeliverySnapshotV41> for DeliverySnapshot {
    fn from(snapshot: DeliverySnapshotV41) -> Self {
        Self {
            delivery_cursor: snapshot.delivery_cursor,
            logical_delivery_id: snapshot.logical_delivery_id,
            sent_by: snapshot.sent_by,
            address: snapshot.address,
            delivery_state: snapshot.delivery_state.into(),
        }
    }
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct SendAcceptanceV41 {
    pub(crate) logical_delivery_id: String,
    pub(crate) delivery_cursor: i64,
    pub(crate) delivery_state: DeliveryStateV41,
}

impl From<SendAcceptanceV41> for SendAcceptance {
    fn from(acceptance: SendAcceptanceV41) -> Self {
        Self {
            logical_delivery_id: acceptance.logical_delivery_id,
            delivery_cursor: acceptance.delivery_cursor,
            delivery_state: acceptance.delivery_state.into(),
        }
    }
}

/// `SendFailureReason` holding a `RefusalV41`.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) enum SendFailureReasonV41 {
    RecipientUnavailable(flow_ethos::Address),
    QueueNotPermitted,
    Refused(RefusalV41),
    ReceiptUncertain,
    NotSent(String),
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct SendFailureV41 {
    pub(crate) logical_delivery_id_option: Option<String>,
    pub(crate) send_failure_reason: SendFailureReasonV41,
}

impl From<SendFailureV41> for SendFailure {
    fn from(failure: SendFailureV41) -> Self {
        Self {
            logical_delivery_id_option: failure.logical_delivery_id_option,
            send_failure_reason: match failure.send_failure_reason {
                SendFailureReasonV41::RecipientUnavailable(address) => {
                    SendFailureReason::RecipientUnavailable(address)
                }
                SendFailureReasonV41::QueueNotPermitted => SendFailureReason::QueueNotPermitted,
                SendFailureReasonV41::Refused(refusal) => {
                    SendFailureReason::Refused(refusal.into())
                }
                SendFailureReasonV41::ReceiptUncertain => SendFailureReason::ReceiptUncertain,
                SendFailureReasonV41::NotSent(reason) => SendFailureReason::NotSent(reason),
            },
        }
    }
}
