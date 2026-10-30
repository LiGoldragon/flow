//! Public message projections. Private logical identities stay in Flow's
//! Memory and routing values; a flow receives only typed metaflow identities
//! and the request it can act on.

use flow_ethos::{DonePayload, Message, Request, ResultPayload, SentBy};
use signal_flow::{
    VisibleDonePayload, VisibleDonePayload_Answered_Data, VisibleMessage, VisibleNativeInput,
    VisibleRequest, VisibleResultPayload, VisibleResultPayload_Answered_Data, VisibleSender,
};

pub(crate) fn visible_refusal(refusal: &signal_flow::Refusal) -> signal_flow::VisibleRefusal {
    use signal_flow::{Refusal, Unknown_Data, VisibleRefusal, VisibleUnknown};
    match refusal {
        Refusal::Locked => VisibleRefusal::Locked,
        Refusal::Held(value) => VisibleRefusal::Held(value.clone()),
        Refusal::Awake(_) => VisibleRefusal::Awake,
        Refusal::Asleep => VisibleRefusal::Asleep,
        Refusal::Lapsed => VisibleRefusal::Lapsed,
        Refusal::Unknown(value) => VisibleRefusal::Unknown(match value {
            Unknown_Data::Address(value) => VisibleUnknown::Address(value.clone()),
            Unknown_Data::Lock(value) => VisibleUnknown::Lock(value.clone()),
            Unknown_Data::FlowId(_) => VisibleUnknown::Flow,
            Unknown_Data::Key(value) => VisibleUnknown::Key(value.clone()),
        }),
        Refusal::NoLayer => VisibleRefusal::NoLayer,
        Refusal::NotConfigured => VisibleRefusal::NotConfigured,
        Refusal::NoneAbove => VisibleRefusal::NoneAbove,
        Refusal::NotMessage => VisibleRefusal::NotMessage,
        Refusal::Taken(value) => VisibleRefusal::Taken(value.clone()),
        Refusal::Unidentified(value) => VisibleRefusal::Unidentified(value.clone()),
        Refusal::Store(value) => VisibleRefusal::Store(value.clone()),
        Refusal::NotSendServer(value) => VisibleRefusal::NotSendServer(value.clone()),
        Refusal::Oversized(value) => VisibleRefusal::Oversized(value.clone()),
        Refusal::Direct(value) => VisibleRefusal::Direct(value.clone()),
        Refusal::TopicRefused(value) => VisibleRefusal::TopicRefused(value.clone()),
        Refusal::NoOpenTask(_) => VisibleRefusal::Unresolved,
        Refusal::Unresolved => VisibleRefusal::Unresolved,
        Refusal::Ambiguous => VisibleRefusal::Ambiguous,
        Refusal::MinutesRefused(value) => VisibleRefusal::MinutesRefused(value.clone()),
    }
}

pub(crate) fn visible_delivery_state(
    state: &signal_flow::DeliveryState,
) -> signal_flow::VisibleDeliveryState {
    use signal_flow::{DeliveryState, VisibleDeliveryState};
    match state {
        DeliveryState::Queued => VisibleDeliveryState::Queued,
        DeliveryState::Attempting => VisibleDeliveryState::Attempting,
        DeliveryState::Retryable(value) => VisibleDeliveryState::Retryable(value.clone()),
        DeliveryState::Uncertain => VisibleDeliveryState::Uncertain,
        DeliveryState::Delivered(value) => VisibleDeliveryState::Delivered(value.clone()),
        DeliveryState::Failed(value) => VisibleDeliveryState::Failed(visible_refusal(value)),
        DeliveryState::Withdrawn => VisibleDeliveryState::Withdrawn,
    }
}

pub(crate) fn visible_send_failure(
    failure: &signal_flow::SendFailure,
) -> signal_flow::VisibleSendFailure {
    use signal_flow::{SendFailureReason, VisibleSendFailureReason};
    let visible_send_failure_reason = match &failure.send_failure_reason {
        SendFailureReason::RecipientUnavailable(value) => {
            VisibleSendFailureReason::RecipientUnavailable(value.clone())
        }
        SendFailureReason::QueueNotPermitted => VisibleSendFailureReason::QueueNotPermitted,
        SendFailureReason::Refused(value) => {
            VisibleSendFailureReason::Refused(visible_refusal(value))
        }
        SendFailureReason::ReceiptUncertain => VisibleSendFailureReason::ReceiptUncertain,
        SendFailureReason::NotSent(value) => VisibleSendFailureReason::NotSent(value.clone()),
    };
    signal_flow::VisibleSendFailure {
        visible_send_failure_reason,
    }
}

pub(crate) fn visible_send_response(
    response: &signal_flow::PrivateSendResponse,
) -> signal_flow::Response {
    use signal_flow::{PrivateSendResponse, Response};
    match response {
        PrivateSendResponse::SendAccepted(value) => {
            Response::SendAccepted(signal_flow::VisibleSendAcceptance {
                delivery_cursor: value.delivery_cursor,
                visible_delivery_state: visible_delivery_state(&value.delivery_state),
            })
        }
        PrivateSendResponse::Refused(value) => Response::SendRefused(visible_refusal(value)),
        PrivateSendResponse::Sent(value) => Response::Sent(value.native_receipt.clone()),
        PrivateSendResponse::Failed(value) => Response::Failed(visible_send_failure(value)),
    }
}

pub(crate) trait ProjectsMessage {
    fn visible_message(&self) -> VisibleMessage;
}

fn visible_request(request: &Request) -> VisibleRequest {
    match request {
        Request::Order(value) => VisibleRequest::Order(value.clone()),
        Request::Question(value) => VisibleRequest::Question(value.clone()),
        Request::Psyche(value) => VisibleRequest::Psyche(value.clone()),
        Request::Psyches(value) => VisibleRequest::Psyches(value.clone()),
        Request::Result(ResultPayload::Answered(value)) => VisibleRequest::Result(
            VisibleResultPayload::Answered(VisibleResultPayload_Answered_Data {
                answerable: value.answerable.clone(),
                string: value.string.clone(),
            }),
        ),
        Request::Result(ResultPayload::Historical(value)) => {
            VisibleRequest::Result(VisibleResultPayload::Historical(value.clone()))
        }
        Request::Notice(value) => VisibleRequest::Notice(value.clone()),
        Request::Task(value) => VisibleRequest::Task(value.clone()),
        Request::Done(DonePayload::Answered(value)) => VisibleRequest::Done(
            VisibleDonePayload::Answered(VisibleDonePayload_Answered_Data {
                answerable: value.answerable.clone(),
                string: value.string.clone(),
            }),
        ),
        Request::Done(DonePayload::Historical(value)) => {
            VisibleRequest::Done(VisibleDonePayload::Historical(value.string.clone()))
        }
    }
}

impl ProjectsMessage for Message {
    fn visible_message(&self) -> VisibleMessage {
        match self {
            Message::HardAbrupt(request) => VisibleMessage::HardAbrupt(visible_request(request)),
            Message::MiddleAbrupt(request) => {
                VisibleMessage::MiddleAbrupt(visible_request(request))
            }
            Message::Soft(request) => VisibleMessage::Soft(visible_request(request)),
        }
    }
}

pub(crate) trait ProjectsQueueEntry {
    fn visible_input(&self, recipient: &flow_ethos::Address) -> VisibleNativeInput;
}

impl ProjectsQueueEntry for flow_ethos::QueueEntry {
    fn visible_input(&self, recipient: &flow_ethos::Address) -> VisibleNativeInput {
        let visible_sender = match &self.sent_by {
            SentBy::Sender(address) => VisibleSender::Sender(address.clone()),
            SentBy::Unknown => VisibleSender::Unknown,
            SentBy::Flow => VisibleSender::Nexus,
        };
        VisibleNativeInput {
            visible_sender,
            address: recipient.clone(),
            visible_message: self.message.visible_message(),
        }
    }
}

pub(crate) fn visible_delivery_snapshot(
    value: &signal_flow::DeliverySnapshot,
) -> signal_flow::VisibleDeliverySnapshot {
    signal_flow::VisibleDeliverySnapshot {
        delivery_cursor: value.delivery_cursor,
        visible_sender: match &value.sent_by {
            SentBy::Sender(address) => VisibleSender::Sender(address.clone()),
            SentBy::Unknown => VisibleSender::Unknown,
            SentBy::Flow => VisibleSender::Nexus,
        },
        address: value.address.clone(),
        visible_delivery_state: visible_delivery_state(&value.delivery_state),
    }
}

pub(crate) fn visible_send_observation(
    value: &signal_flow::SendObservation,
) -> signal_flow::VisibleSendObservation {
    signal_flow::VisibleSendObservation {
        delivery_cursor: value.delivery_cursor,
        visible_delivery_snapshot_vector: value
            .delivery_snapshot_vector
            .iter()
            .map(visible_delivery_snapshot)
            .collect(),
    }
}

pub(crate) fn visible_delivery(
    value: &meta_signal_flow::Delivery,
) -> meta_signal_flow::VisibleDelivery {
    meta_signal_flow::VisibleDelivery {
        interrupt_witness: value.interrupt_witness.clone(),
        delivery_grade: value.delivery_grade.clone(),
    }
}

fn visible_queue_resolution_record(
    value: &meta_signal_flow::QueueResolutionRecord,
) -> meta_signal_flow::VisibleQueueResolutionRecord {
    meta_signal_flow::VisibleQueueResolutionRecord {
        queue_resolution_outcome: value.queue_resolution_outcome.clone(),
        visible_resolver_identity: value.resolver_identity.process_identity.clone(),
        queue_resolution_reason: value.queue_resolution_reason.clone(),
        unix_seconds: value.unix_seconds,
    }
}

pub(crate) fn visible_queue_resolution(
    value: &meta_signal_flow::QueueDeliveryResolution,
) -> meta_signal_flow::VisibleQueueDeliveryResolution {
    meta_signal_flow::VisibleQueueDeliveryResolution {
        queue_resolution_outcome: value.queue_resolution_outcome.clone(),
        queue_resolution_reason: value.queue_resolution_reason.clone(),
    }
}

pub(crate) fn visible_queue_rejection(
    value: meta_signal_flow::ResolveQueueDeliveryRejection,
) -> meta_signal_flow::VisibleResolveQueueDeliveryRejected {
    use meta_signal_flow::{
        ResolveQueueDeliveryRejection as Private, VisibleResolveQueueDeliveryRejected as Visible,
    };
    match value {
        Private::UnknownAttempt => Visible::UnknownAttempt,
        Private::NotUncertain => Visible::NotUncertain,
        Private::NotOldest => Visible::NotOldest,
        Private::EmptyReason => Visible::EmptyReason,
        Private::StoreRefused => Visible::StoreRefused,
        Private::NativeReceiptRequired => Visible::NativeReceiptRequired,
        Private::LegacyUncorrelated => Visible::LegacyUncorrelated,
        Private::AlreadyResolved(value) => {
            Visible::AlreadyResolved(visible_queue_resolution_record(&value))
        }
        Private::AlreadyDelivered(value) => Visible::AlreadyDelivered(value),
    }
}

fn visible_queue_standing(
    value: &meta_signal_flow::QueueDeliveryStanding,
) -> meta_signal_flow::VisibleQueueDeliveryStanding {
    use meta_signal_flow::{
        QueueDeliveryStanding as Private, VisibleQueueDeliveryStanding as Visible,
    };
    match value {
        Private::Prepared => Visible::Prepared,
        Private::Uncertain(value) => Visible::Uncertain(value.clone()),
        Private::ReceiptRecorded => Visible::ReceiptRecorded,
        Private::Absent(value) => Visible::Absent(value.clone()),
        Private::Resolved(value) => Visible::Resolved(visible_queue_resolution_record(value)),
    }
}

pub(crate) fn visible_queue_evidence(
    value: &meta_signal_flow::QueueEvidence,
    recipient: &flow_ethos::Address,
) -> meta_signal_flow::VisibleQueueEvidence {
    match value {
        meta_signal_flow::QueueEvidence::Logical(entry) => {
            meta_signal_flow::VisibleQueueEvidence::Logical(entry.visible_input(recipient))
        }
        meta_signal_flow::QueueEvidence::Legacy(value) => {
            let sender = match &value.sent_by {
                SentBy::Sender(address) => VisibleSender::Sender(address.clone()),
                SentBy::Unknown => VisibleSender::Unknown,
                SentBy::Flow => VisibleSender::Nexus,
            };
            meta_signal_flow::VisibleQueueEvidence::Legacy(
                meta_signal_flow::VisibleQueueEvidence_Legacy_Data {
                    visible_sender: sender,
                    visible_request: visible_request(&value.request),
                },
            )
        }
    }
}

pub(crate) fn visible_queue_delivery(
    value: &meta_signal_flow::QueueDelivery,
    recipient: &flow_ethos::Address,
) -> meta_signal_flow::VisibleQueueDelivery {
    meta_signal_flow::VisibleQueueDelivery {
        native_session_id: value.native_session_id.clone(),
        harness_kind: value.harness_kind.clone(),
        visible_queue_evidence: visible_queue_evidence(&value.queue_evidence, recipient),
        visible_queue_delivery_standing: visible_queue_standing(&value.queue_delivery_standing),
    }
}

fn visible_metaflow(value: &flow_ethos::Metaflow) -> meta_signal_flow::VisibleMetaflow {
    meta_signal_flow::VisibleMetaflow {
        address: value.address.clone(),
        state: value.state.clone(),
        flow_id_vector: value.past.clone(),
        visible_native_input_vector: value
            .queue
            .iter()
            .map(|entry| entry.visible_input(&value.address))
            .collect(),
    }
}

pub(crate) fn visible_inspection(
    value: crate::store::repair::Inspection,
) -> meta_signal_flow::Inspected_Data {
    let address = value.metaflow.address.clone();
    meta_signal_flow::Inspected_Data {
        visible_metaflow: visible_metaflow(&value.metaflow),
        binding: value.binding,
        lock_vector: value.lock_vector,
        visible_attempt_vector: value
            .attempt_vector
            .into_iter()
            .map(|attempt| match attempt {
                meta_signal_flow::Attempt::Queued(delivery) => {
                    meta_signal_flow::VisibleAttempt::Queued(visible_queue_delivery(
                        &delivery, &address,
                    ))
                }
                meta_signal_flow::Attempt::Launch(launch) => {
                    meta_signal_flow::VisibleAttempt::Launch(launch)
                }
            })
            .collect(),
    }
}

pub(crate) fn visible_legacy_archive_entry(
    value: &meta_signal_flow::LegacyArchiveEntry,
) -> meta_signal_flow::VisibleLegacyArchiveEntry {
    meta_signal_flow::VisibleLegacyArchiveEntry {
        address: value.address.clone(),
        visible_queue_evidence: visible_queue_evidence(&value.queue_evidence, &value.address),
    }
}

pub(crate) fn visible_send_outcome_record(
    value: &meta_signal_flow::SendOutcomeRecord,
) -> meta_signal_flow::VisibleSendOutcomeRecord {
    use meta_signal_flow::{OrdinarySendOutcome as Private, VisibleOrdinarySendOutcome as Visible};
    let visible_ordinary_send_outcome = match &value.ordinary_send_outcome {
        Private::Accepted(value) => Visible::Accepted(signal_flow::VisibleSendAcceptance {
            delivery_cursor: value.delivery_cursor,
            visible_delivery_state: visible_delivery_state(&value.delivery_state),
        }),
        Private::Refused(value) => Visible::Refused(visible_refusal(value)),
        Private::PriorAdmission(value) => Visible::PriorAdmission(value.clone()),
        Private::Sent(value) => Visible::Sent(value.native_receipt.clone()),
        Private::Failed(value) => Visible::Failed(visible_send_failure(value)),
    };
    meta_signal_flow::VisibleSendOutcomeRecord {
        process_option: value.process_option.clone(),
        first_address_option: value.first_address_option.clone(),
        second_address_option: value.second_address_option.clone(),
        request_digest: value.request_digest.clone(),
        visible_ordinary_send_outcome,
    }
}
