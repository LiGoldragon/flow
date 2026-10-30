//! A send's answers as signal-flow 32.0.0 archived them, before a Send was
//! never refused for its route or its recipient's state: `Refusal` with
//! `OffRoute`, `SendFailureReason` with `RecipientBusy`. The
//! logical-delivery, send-outcome and send-call families kept these shapes
//! under their previous labels; each is read only by its family's upgrade.
//! A stored `OffRoute` upgrades to `Direct.«off route»` and a stored
//! `RecipientBusy` to `NotSent.«recipient busy { <address> }»`; every other
//! value is carried unchanged.

use super::send_v34::UpgradesUnidentified;
use datom_codec::Datomizable;
use protos::{Compactable, Protosizable};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use signal_flow::{
    DeliverySnapshot, DeliveryState, NativeReceipt, Refusal, RetryCause, SendAcceptance,
    SendFailure, SendFailureReason, SendPayloadSize, Unknown_Data,
};

/// What a stored `OffRoute` becomes.
pub(crate) trait UpgradesOffRoute {
    fn from_off_route() -> Self;
}

impl UpgradesOffRoute for Refusal {
    fn from_off_route() -> Self {
        Self::Direct("off route".into())
    }
}

/// What a stored `RecipientBusy` becomes, naming the seat as the old row did.
pub(crate) trait UpgradesRecipientBusy {
    fn from_recipient_busy(address: &flow_ethos::Address) -> Self;
}

impl UpgradesRecipientBusy for SendFailureReason {
    fn from_recipient_busy(address: &flow_ethos::Address) -> Self {
        Self::NotSent(format!(
            "recipient busy {}",
            address.datomize(Vec::new()).protosize().compact()
        ))
    }
}

/// `Refusal` with `OffRoute`.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) enum RefusalV32 {
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
    TopicRefused(flow_ethos::Topic),
}

impl From<RefusalV32> for Refusal {
    fn from(refusal: RefusalV32) -> Self {
        match refusal {
            RefusalV32::Locked => Self::Locked,
            RefusalV32::Held(lock) => Self::Held(lock),
            RefusalV32::Awake(flow_id) => Self::Awake(flow_id),
            RefusalV32::Asleep => Self::Asleep,
            RefusalV32::Lapsed => Self::Lapsed,
            RefusalV32::Unknown(unknown) => Self::Unknown(unknown),
            RefusalV32::NoLayer => Self::NoLayer,
            RefusalV32::NotConfigured => Self::NotConfigured,
            RefusalV32::NoneAbove => Self::NoneAbove,
            RefusalV32::NotMessage => Self::NotMessage,
            RefusalV32::Taken(address) => Self::Taken(address),
            RefusalV32::Unidentified(process) => Self::from_unidentified(process),
            RefusalV32::Store(reason) => Self::Store(reason),
            RefusalV32::NotSendServer(process) => Self::NotSendServer(process),
            RefusalV32::OffRoute => Self::from_off_route(),
            RefusalV32::Oversized(size) => Self::Oversized(size),
            RefusalV32::Direct(reason) => Self::Direct(reason),
            RefusalV32::TopicRefused(topic) => Self::TopicRefused(topic),
        }
    }
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) enum DeliveryStateV32 {
    Queued,
    Attempting,
    Retryable(RetryCause),
    Uncertain,
    Delivered(NativeReceipt),
    Failed(RefusalV32),
    Withdrawn,
}

impl From<DeliveryStateV32> for DeliveryState {
    fn from(state: DeliveryStateV32) -> Self {
        match state {
            DeliveryStateV32::Queued => Self::Queued,
            DeliveryStateV32::Attempting => Self::Attempting,
            DeliveryStateV32::Retryable(cause) => Self::Retryable(cause),
            DeliveryStateV32::Uncertain => Self::Uncertain,
            DeliveryStateV32::Delivered(receipt) => Self::Delivered(receipt),
            DeliveryStateV32::Failed(refusal) => Self::Failed(refusal.into()),
            DeliveryStateV32::Withdrawn => Self::Withdrawn,
        }
    }
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct DeliverySnapshotV32 {
    pub(crate) delivery_cursor: i64,
    pub(crate) logical_delivery_id: String,
    pub(crate) sent_by: flow_ethos::SentBy,
    pub(crate) address: flow_ethos::Address,
    pub(crate) delivery_state: DeliveryStateV32,
}

impl From<DeliverySnapshotV32> for DeliverySnapshot {
    fn from(snapshot: DeliverySnapshotV32) -> Self {
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
pub(crate) struct SendAcceptanceV32 {
    pub(crate) logical_delivery_id: String,
    pub(crate) delivery_cursor: i64,
    pub(crate) delivery_state: DeliveryStateV32,
}

impl From<SendAcceptanceV32> for SendAcceptance {
    fn from(acceptance: SendAcceptanceV32) -> Self {
        Self {
            logical_delivery_id: acceptance.logical_delivery_id,
            delivery_cursor: acceptance.delivery_cursor,
            delivery_state: acceptance.delivery_state.into(),
        }
    }
}

/// `SendFailureReason` with `RecipientBusy`.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) enum SendFailureReasonV32 {
    RecipientUnavailable(flow_ethos::Address),
    QueueNotPermitted,
    RecipientBusy(flow_ethos::Address),
    Refused(RefusalV32),
    ReceiptUncertain,
    NotSent(String),
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct SendFailureV32 {
    pub(crate) logical_delivery_id_option: Option<String>,
    pub(crate) send_failure_reason: SendFailureReasonV32,
}

impl From<SendFailureV32> for SendFailure {
    fn from(failure: SendFailureV32) -> Self {
        Self {
            logical_delivery_id_option: failure.logical_delivery_id_option,
            send_failure_reason: match failure.send_failure_reason {
                SendFailureReasonV32::RecipientUnavailable(address) => {
                    SendFailureReason::RecipientUnavailable(address)
                }
                SendFailureReasonV32::QueueNotPermitted => SendFailureReason::QueueNotPermitted,
                SendFailureReasonV32::RecipientBusy(address) => {
                    SendFailureReason::from_recipient_busy(&address)
                }
                SendFailureReasonV32::Refused(refusal) => {
                    SendFailureReason::Refused(refusal.into())
                }
                SendFailureReasonV32::ReceiptUncertain => SendFailureReason::ReceiptUncertain,
                SendFailureReasonV32::NotSent(reason) => SendFailureReason::NotSent(reason),
            },
        }
    }
}
