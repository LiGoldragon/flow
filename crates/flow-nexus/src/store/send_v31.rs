//! A send's answers as signal-flow 31.0.0 archived them, before a queued
//! request could be withdrawn and a Send refused for its recipient's
//! topic: `DeliveryState` without `Withdrawn`, `Refusal` without
//! `TopicRefused`. The logical-delivery, send-outcome and send-call
//! families kept these shapes under their previous labels; each is read
//! only by its family's upgrade, which carries every value unchanged.

use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use signal_flow::{
    DeliverySnapshot, DeliveryState, NativeReceipt, Refusal, RetryCause, SendAcceptance,
    SendFailure, SendFailureReason, SendPayloadSize, Unknown_Data,
};

/// `Refusal` before `TopicRefused`.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) enum RefusalV31 {
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
    OffRoute,
    Oversized(SendPayloadSize),
    Direct(String),
}

/// `DeliveryState` before `Withdrawn`, its refusal before `TopicRefused`.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) enum DeliveryStateV31 {
    Queued,
    Attempting,
    Retryable(RetryCause),
    Uncertain,
    Delivered(NativeReceipt),
    Failed(RefusalV31),
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct DeliverySnapshotV31 {
    pub(crate) delivery_cursor: i64,
    pub(crate) logical_delivery_id: String,
    pub(crate) sent_by: flow_ethos::SentBy,
    pub(crate) address: flow_ethos::Address,
    pub(crate) delivery_state: DeliveryStateV31,
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct SendAcceptanceV31 {
    pub(crate) logical_delivery_id: String,
    pub(crate) delivery_cursor: i64,
    pub(crate) delivery_state: DeliveryStateV31,
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) enum SendFailureReasonV31 {
    RecipientUnavailable(flow_ethos::Address),
    QueueNotPermitted,
    RecipientBusy(flow_ethos::Address),
    Refused(RefusalV31),
    ReceiptUncertain,
    NotSent(String),
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct SendFailureV31 {
    pub(crate) logical_delivery_id_option: Option<String>,
    pub(crate) send_failure_reason: SendFailureReasonV31,
}

impl From<RefusalV31> for Refusal {
    fn from(refusal: RefusalV31) -> Self {
        match refusal {
            RefusalV31::Locked => Self::Locked,
            RefusalV31::Held(lock) => Self::Held(lock),
            RefusalV31::Awake(flow_id) => Self::Awake(flow_id),
            RefusalV31::Asleep => Self::Asleep,
            RefusalV31::Lapsed => Self::Lapsed,
            RefusalV31::Unknown(unknown) => Self::Unknown(unknown),
            RefusalV31::NoLayer => Self::NoLayer,
            RefusalV31::NotConfigured => Self::NotConfigured,
            RefusalV31::NoneAbove => Self::NoneAbove,
            RefusalV31::NotMessage => Self::NotMessage,
            RefusalV31::Taken(address) => Self::Taken(address),
            RefusalV31::Unidentified(process) => Self::Unidentified(process),
            RefusalV31::Store(reason) => Self::Store(reason),
            RefusalV31::NotSendServer(process) => Self::NotSendServer(process),
            RefusalV31::OffRoute => Self::OffRoute,
            RefusalV31::Oversized(size) => Self::Oversized(size),
            RefusalV31::Direct(reason) => Self::Direct(reason),
        }
    }
}

impl From<DeliveryStateV31> for DeliveryState {
    fn from(state: DeliveryStateV31) -> Self {
        match state {
            DeliveryStateV31::Queued => Self::Queued,
            DeliveryStateV31::Attempting => Self::Attempting,
            DeliveryStateV31::Retryable(cause) => Self::Retryable(cause),
            DeliveryStateV31::Uncertain => Self::Uncertain,
            DeliveryStateV31::Delivered(receipt) => Self::Delivered(receipt),
            DeliveryStateV31::Failed(refusal) => Self::Failed(refusal.into()),
        }
    }
}

impl From<DeliverySnapshotV31> for DeliverySnapshot {
    fn from(snapshot: DeliverySnapshotV31) -> Self {
        Self {
            delivery_cursor: snapshot.delivery_cursor,
            logical_delivery_id: snapshot.logical_delivery_id,
            sent_by: snapshot.sent_by,
            address: snapshot.address,
            delivery_state: snapshot.delivery_state.into(),
        }
    }
}

impl From<SendAcceptanceV31> for SendAcceptance {
    fn from(acceptance: SendAcceptanceV31) -> Self {
        Self {
            logical_delivery_id: acceptance.logical_delivery_id,
            delivery_cursor: acceptance.delivery_cursor,
            delivery_state: acceptance.delivery_state.into(),
        }
    }
}

impl From<SendFailureV31> for SendFailure {
    fn from(failure: SendFailureV31) -> Self {
        Self {
            logical_delivery_id_option: failure.logical_delivery_id_option,
            send_failure_reason: match failure.send_failure_reason {
                SendFailureReasonV31::RecipientUnavailable(address) => {
                    SendFailureReason::RecipientUnavailable(address)
                }
                SendFailureReasonV31::QueueNotPermitted => SendFailureReason::QueueNotPermitted,
                SendFailureReasonV31::RecipientBusy(address) => {
                    SendFailureReason::RecipientBusy(address)
                }
                SendFailureReasonV31::Refused(refusal) => {
                    SendFailureReason::Refused(refusal.into())
                }
                SendFailureReasonV31::ReceiptUncertain => SendFailureReason::ReceiptUncertain,
                SendFailureReasonV31::NotSent(reason) => SendFailureReason::NotSent(reason),
            },
        }
    }
}
