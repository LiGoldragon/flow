//! A send's answers as signal-flow 33.0.0 archived them, before an abrupt
//! request could be queued: `SendFailureReason` with `QueueNotPermitted`.
//! The logical-delivery and send-outcome families kept these shapes under
//! their previous labels; each is read only by its family's upgrade. A
//! stored `QueueNotPermitted` becomes `NotSent.«queue not permitted»`; every
//! other value is carried unchanged.

use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use signal_flow::{
    DeliverySnapshot, DeliveryState, NativeReceipt, Refusal, RetryCause, SendAcceptance,
    SendFailure, SendFailureReason, SendPayloadSize, Unknown_Data,
};

/// `Refusal` as signal-flow 33 archived it.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) enum RefusalV33 {
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
    Unidentified(flow_ethos::Process),
    Store(String),
    NotSendServer(flow_ethos::Process),
    Oversized(SendPayloadSize),
    Direct(String),
    TopicRefused(flow_ethos::Topic),
}

impl From<RefusalV33> for Refusal {
    fn from(refusal: RefusalV33) -> Self {
        match refusal {
            RefusalV33::Locked => Self::Locked,
            RefusalV33::Held(lock) => Self::Held(lock),
            RefusalV33::Awake(flow_id) => Self::Awake(flow_id),
            RefusalV33::Asleep => Self::Asleep,
            RefusalV33::Lapsed => Self::Lapsed,
            RefusalV33::Unknown(unknown) => Self::Unknown(unknown),
            RefusalV33::NoLayer => Self::NoLayer,
            RefusalV33::NotConfigured => Self::NotConfigured,
            RefusalV33::NoneAbove => Self::NoneAbove,
            RefusalV33::NotMessage => Self::NotMessage,
            RefusalV33::Taken(address) => Self::Taken(address),
            RefusalV33::Unidentified(process) => Self::Unidentified(process),
            RefusalV33::Store(reason) => Self::Store(reason),
            RefusalV33::NotSendServer(process) => Self::NotSendServer(process),
            RefusalV33::Oversized(size) => Self::Oversized(size),
            RefusalV33::Direct(reason) => Self::Direct(reason),
            RefusalV33::TopicRefused(topic) => Self::TopicRefused(topic),
        }
    }
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) enum DeliveryStateV33 {
    Queued,
    Attempting,
    Retryable(RetryCause),
    Uncertain,
    Delivered(NativeReceipt),
    Failed(RefusalV33),
    Withdrawn,
}

impl From<DeliveryStateV33> for DeliveryState {
    fn from(state: DeliveryStateV33) -> Self {
        match state {
            DeliveryStateV33::Queued => Self::Queued,
            DeliveryStateV33::Attempting => Self::Attempting,
            DeliveryStateV33::Retryable(cause) => Self::Retryable(cause),
            DeliveryStateV33::Uncertain => Self::Uncertain,
            DeliveryStateV33::Delivered(receipt) => Self::Delivered(receipt),
            DeliveryStateV33::Failed(refusal) => Self::Failed(refusal.into()),
            DeliveryStateV33::Withdrawn => Self::Withdrawn,
        }
    }
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct DeliverySnapshotV33 {
    pub(crate) delivery_cursor: i64,
    pub(crate) logical_delivery_id: String,
    pub(crate) sent_by: flow_ethos::SentBy,
    pub(crate) address: flow_ethos::Address,
    pub(crate) delivery_state: DeliveryStateV33,
}

impl From<DeliverySnapshotV33> for DeliverySnapshot {
    fn from(snapshot: DeliverySnapshotV33) -> Self {
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
pub(crate) struct SendAcceptanceV33 {
    pub(crate) logical_delivery_id: String,
    pub(crate) delivery_cursor: i64,
    pub(crate) delivery_state: DeliveryStateV33,
}

impl From<SendAcceptanceV33> for SendAcceptance {
    fn from(acceptance: SendAcceptanceV33) -> Self {
        Self {
            logical_delivery_id: acceptance.logical_delivery_id,
            delivery_cursor: acceptance.delivery_cursor,
            delivery_state: acceptance.delivery_state.into(),
        }
    }
}

/// `SendFailureReason` with `QueueNotPermitted`.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) enum SendFailureReasonV33 {
    RecipientUnavailable(flow_ethos::Address),
    QueueNotPermitted,
    Refused(RefusalV33),
    ReceiptUncertain,
    NotSent(String),
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct SendFailureV33 {
    pub(crate) logical_delivery_id_option: Option<String>,
    pub(crate) send_failure_reason: SendFailureReasonV33,
}

impl From<SendFailureV33> for SendFailure {
    fn from(failure: SendFailureV33) -> Self {
        Self {
            logical_delivery_id_option: failure.logical_delivery_id_option,
            send_failure_reason: match failure.send_failure_reason {
                SendFailureReasonV33::RecipientUnavailable(address) => {
                    SendFailureReason::RecipientUnavailable(address)
                }
                SendFailureReasonV33::QueueNotPermitted => {
                    SendFailureReason::NotSent("queue not permitted".into())
                }
                SendFailureReasonV33::Refused(refusal) => {
                    SendFailureReason::Refused(refusal.into())
                }
                SendFailureReasonV33::ReceiptUncertain => SendFailureReason::ReceiptUncertain,
                SendFailureReasonV33::NotSent(reason) => SendFailureReason::NotSent(reason),
            },
        }
    }
}
