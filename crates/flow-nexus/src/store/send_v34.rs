//! A send's answers as signal-flow 34.0.0 archived them, before an
//! `Unidentified` refusal carried the evidence of its seat: `Refusal` with
//! `Unidentified(Process)`. The logical-delivery and send-outcome families
//! kept these shapes under their previous labels; each is read only by its
//! family's upgrade. A stored `Unidentified` upgrades to the evidence no
//! pane, no label and no session, observed at 0, with the stored process;
//! every other value is carried unchanged.

use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use signal_flow::{
    DeliverySnapshot, DeliveryState, NativeReceipt, Refusal, RetryCause, SeatEvidence,
    SendAcceptance, SendFailure, SendFailureReason, SendPayloadSize, Unknown_Data,
};

/// What a stored `Unidentified(Process)` becomes.
pub(crate) trait UpgradesUnidentified {
    fn from_unidentified(process: flow_ethos::Process) -> Self;
}

impl UpgradesUnidentified for Refusal {
    fn from_unidentified(process: flow_ethos::Process) -> Self {
        Self::Unidentified(SeatEvidence {
            seat_pane_option: None,
            pane_label_option: None,
            native_session_id_option: None,
            observed_at: 0,
            process,
        })
    }
}

/// `Refusal` with `Unidentified(Process)`.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) enum RefusalV34 {
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
    NoOpenTask(flow_ethos::LogicalDeliveryId),
    MinutesRefused(signal_flow::MinutesRefusal),
}

impl From<RefusalV34> for Refusal {
    fn from(refusal: RefusalV34) -> Self {
        match refusal {
            RefusalV34::Locked => Self::Locked,
            RefusalV34::Held(lock) => Self::Held(lock),
            RefusalV34::Awake(flow_id) => Self::Awake(flow_id),
            RefusalV34::Asleep => Self::Asleep,
            RefusalV34::Lapsed => Self::Lapsed,
            RefusalV34::Unknown(unknown) => Self::Unknown(unknown),
            RefusalV34::NoLayer => Self::NoLayer,
            RefusalV34::NotConfigured => Self::NotConfigured,
            RefusalV34::NoneAbove => Self::NoneAbove,
            RefusalV34::NotMessage => Self::NotMessage,
            RefusalV34::Taken(address) => Self::Taken(address),
            RefusalV34::Unidentified(process) => Self::from_unidentified(process),
            RefusalV34::Store(reason) => Self::Store(reason),
            RefusalV34::NotSendServer(process) => Self::NotSendServer(process),
            RefusalV34::Oversized(size) => Self::Oversized(size),
            RefusalV34::Direct(reason) => Self::Direct(reason),
            RefusalV34::TopicRefused(topic) => Self::TopicRefused(topic),
            RefusalV34::NoOpenTask(id) => Self::NoOpenTask(id),
            RefusalV34::MinutesRefused(refusal) => Self::MinutesRefused(refusal),
        }
    }
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) enum DeliveryStateV34 {
    Queued,
    Attempting,
    Retryable(RetryCause),
    Uncertain,
    Delivered(NativeReceipt),
    Failed(RefusalV34),
    Withdrawn,
}

impl From<DeliveryStateV34> for DeliveryState {
    fn from(state: DeliveryStateV34) -> Self {
        match state {
            DeliveryStateV34::Queued => Self::Queued,
            DeliveryStateV34::Attempting => Self::Attempting,
            DeliveryStateV34::Retryable(cause) => Self::Retryable(cause),
            DeliveryStateV34::Uncertain => Self::Uncertain,
            DeliveryStateV34::Delivered(receipt) => Self::Delivered(receipt),
            DeliveryStateV34::Failed(refusal) => Self::Failed(refusal.into()),
            DeliveryStateV34::Withdrawn => Self::Withdrawn,
        }
    }
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct DeliverySnapshotV34 {
    pub(crate) delivery_cursor: i64,
    pub(crate) logical_delivery_id: String,
    pub(crate) sent_by: flow_ethos::SentBy,
    pub(crate) address: flow_ethos::Address,
    pub(crate) delivery_state: DeliveryStateV34,
}

impl From<DeliverySnapshotV34> for DeliverySnapshot {
    fn from(snapshot: DeliverySnapshotV34) -> Self {
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
pub(crate) struct SendAcceptanceV34 {
    pub(crate) logical_delivery_id: String,
    pub(crate) delivery_cursor: i64,
    pub(crate) delivery_state: DeliveryStateV34,
}

impl From<SendAcceptanceV34> for SendAcceptance {
    fn from(acceptance: SendAcceptanceV34) -> Self {
        Self {
            logical_delivery_id: acceptance.logical_delivery_id,
            delivery_cursor: acceptance.delivery_cursor,
            delivery_state: acceptance.delivery_state.into(),
        }
    }
}

/// `SendFailureReason` holding a `RefusalV34`.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) enum SendFailureReasonV34 {
    RecipientUnavailable(flow_ethos::Address),
    QueueNotPermitted,
    Refused(RefusalV34),
    ReceiptUncertain,
    NotSent(String),
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct SendFailureV34 {
    pub(crate) logical_delivery_id_option: Option<String>,
    pub(crate) send_failure_reason: SendFailureReasonV34,
}

impl From<SendFailureV34> for SendFailure {
    fn from(failure: SendFailureV34) -> Self {
        Self {
            logical_delivery_id_option: failure.logical_delivery_id_option,
            send_failure_reason: match failure.send_failure_reason {
                SendFailureReasonV34::RecipientUnavailable(address) => {
                    SendFailureReason::RecipientUnavailable(address)
                }
                SendFailureReasonV34::QueueNotPermitted => SendFailureReason::QueueNotPermitted,
                SendFailureReasonV34::Refused(refusal) => {
                    SendFailureReason::Refused(refusal.into())
                }
                SendFailureReasonV34::ReceiptUncertain => SendFailureReason::ReceiptUncertain,
                SendFailureReasonV34::NotSent(reason) => SendFailureReason::NotSent(reason),
            },
        }
    }
}
