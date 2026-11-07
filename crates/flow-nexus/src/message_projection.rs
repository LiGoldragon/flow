//! Public message projections. Private logical identities stay in Flow's
//! Memory and routing values; a flow receives only typed metaflow identities
//! and the request it can act on.

use flow_ethos::{DonePayload, Message, Request, ResultPayload, SentBy};
use signal_flow::{
    DeliveredMessage, DeliveredRequest, VisibleDonePayload, VisibleDonePayload_Answered_Data,
    VisibleNativeInput, VisibleRequest, VisibleResultPayload, VisibleResultPayload_Answered_Data,
    VisibleSender,
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
        Refusal::NotConfigured => VisibleRefusal::NotConfigured,
        Refusal::NotMessage => VisibleRefusal::NotMessage,
        Refusal::Taken(value) => VisibleRefusal::Taken(value.clone()),
        Refusal::Unidentified(value) => VisibleRefusal::Unidentified(value.clone()),
        Refusal::Store(value) => VisibleRefusal::Store(value.clone()),
        Refusal::NotSendServer(value) => VisibleRefusal::NotSendServer(value.clone()),
        Refusal::Oversized(value) => VisibleRefusal::Oversized(value.clone()),
        Refusal::Direct(value) => VisibleRefusal::Direct(value.clone()),
        Refusal::TopicRefused(value) => VisibleRefusal::TopicRefused(value.clone()),
        Refusal::Unresolved => VisibleRefusal::Unresolved,
        Refusal::RequestIdSpaceFull => VisibleRefusal::RequestIdSpaceFull,
        Refusal::RequestIdDrawsCollided => VisibleRefusal::RequestIdDrawsCollided,
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
        DeliveryState::Delivered(_) => VisibleDeliveryState::Delivered,
        DeliveryState::Read => VisibleDeliveryState::Read,
        DeliveryState::Failed(value) => VisibleDeliveryState::Failed(visible_refusal(value)),
        DeliveryState::Withdrawn => VisibleDeliveryState::Withdrawn,
        DeliveryState::Archived => VisibleDeliveryState::Archived,
    }
}

pub(crate) fn visible_send_failure(
    failure: &signal_flow::SendFailure,
) -> signal_flow::VisibleSendFailure {
    use signal_flow::{
        SendFailure, SendFailureReason, VisibleSendFailure, VisibleSendFailureReason,
    };
    fn reason(value: &SendFailureReason) -> VisibleSendFailureReason {
        match value {
            SendFailureReason::RecipientUnavailable(value) => {
                VisibleSendFailureReason::RecipientUnavailable(value.clone())
            }
            SendFailureReason::QueueNotPermitted => VisibleSendFailureReason::QueueNotPermitted,
            SendFailureReason::Refused(value) => {
                VisibleSendFailureReason::Refused(visible_refusal(value))
            }
            SendFailureReason::ReceiptUncertain => VisibleSendFailureReason::ReceiptUncertain,
            SendFailureReason::NotSent(value) => VisibleSendFailureReason::NotSent(value.clone()),
        }
    }
    match failure {
        SendFailure::Unadmitted(value) => VisibleSendFailure::Unadmitted(reason(value)),
        SendFailure::Admitted(value) => {
            VisibleSendFailure::Admitted(signal_flow::VisibleSendFailure_Admitted_Data {
                request_id: value.request_id.clone(),
                visible_send_failure_reason: reason(&value.send_failure_reason),
            })
        }
    }
}

pub(crate) fn visible_send_response(
    response: &signal_flow::PrivateSendResponse,
) -> signal_flow::Response {
    use signal_flow::{PrivateSendResponse, Response};
    match response {
        PrivateSendResponse::SendAccepted(value) => {
            Response::SendAccepted(signal_flow::VisibleSendAcceptance {
                request_id: value.request_id.clone(),
                delivery_cursor: value.delivery_cursor,
                visible_delivery_state: visible_delivery_state(&value.delivery_state),
            })
        }
        PrivateSendResponse::Refused(value) => Response::SendRefused(visible_refusal(value)),
        PrivateSendResponse::Sent(value) => Response::Sent(value.request_id.clone()),
        PrivateSendResponse::Failed(value) => Response::Failed(visible_send_failure(value)),
    }
}

fn visible_request(request: &Request) -> VisibleRequest {
    match request {
        Request::Order(value) => VisibleRequest::Order(value.clone()),
        Request::Question(value) => VisibleRequest::Question(value.clone()),
        Request::Psyche(value) => VisibleRequest::Psyche(value.clone()),
        Request::Psyches(value) => VisibleRequest::Psyches(value.clone()),
        Request::Result(ResultPayload::Answered(value)) => VisibleRequest::Result(
            VisibleResultPayload::Answered(VisibleResultPayload_Answered_Data {
                request_id: value.request_id.clone(),
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
                request_id: value.request_id.clone(),
                string: value.string.clone(),
            }),
        ),
        Request::Done(DonePayload::Historical(value)) => {
            VisibleRequest::Done(VisibleDonePayload::Historical(value.string.clone()))
        }
        Request::Reminder(value) => {
            VisibleRequest::Reminder(signal_flow::VisibleRequest_Reminder_Data {
                request_id: value.request_id.clone(),
                string: value.string.clone(),
            })
        }
    }
}

fn delivered_request(request_id: &flow_ethos::RequestId, request: &Request) -> DeliveredRequest {
    match request {
        Request::Order(value) => DeliveredRequest::Order(signal_flow::Order_Data {
            request_id: request_id.clone(),
            string: value.clone(),
        }),
        Request::Question(value) => DeliveredRequest::Question(signal_flow::Question_Data {
            request_id: request_id.clone(),
            string: value.clone(),
        }),
        Request::Psyche(value) => DeliveredRequest::Psyche(signal_flow::Psyche_Data {
            request_id: request_id.clone(),
            said: value.clone(),
        }),
        Request::Psyches(value) => DeliveredRequest::Psyches(signal_flow::Psyches_Data {
            request_id: request_id.clone(),
            said_vector: value.clone(),
        }),
        Request::Result(ResultPayload::Answered(value)) => {
            DeliveredRequest::Result(signal_flow::DeliveredRequest_Result_Data {
                request_id: value.request_id.clone(),
                string: value.string.clone(),
            })
        }
        Request::Result(ResultPayload::Historical(value)) => {
            DeliveredRequest::PriorResult(value.clone())
        }
        Request::Notice(value) => DeliveredRequest::Notice(signal_flow::Notice_Data {
            request_id: request_id.clone(),
            string: value.clone(),
        }),
        Request::Task(value) => DeliveredRequest::Task(signal_flow::Task_Data {
            request_id: request_id.clone(),
            minutes: value.minutes,
            string: value.string.clone(),
        }),
        Request::Done(DonePayload::Answered(value)) => {
            DeliveredRequest::Done(signal_flow::DeliveredRequest_Done_Data {
                request_id: value.request_id.clone(),
                string: value.string.clone(),
            })
        }
        Request::Done(DonePayload::Historical(value)) => {
            DeliveredRequest::PriorDone(value.string.clone())
        }
        Request::Reminder(value) => {
            DeliveredRequest::Reminder(signal_flow::DeliveredRequest_Reminder_Data {
                request_id: value.request_id.clone(),
                string: value.string.clone(),
            })
        }
    }
}

pub(crate) trait ProjectsQueueEntry {
    fn visible_input(&self, recipient: &flow_ethos::Address) -> VisibleNativeInput;
}

impl ProjectsQueueEntry for flow_ethos::QueueEntry {
    fn visible_input(&self, _recipient: &flow_ethos::Address) -> VisibleNativeInput {
        let visible_sender = match &self.sent_by {
            SentBy::Sender(address) => VisibleSender::Sender(address.clone()),
            SentBy::Unknown => VisibleSender::Unknown,
            SentBy::Flow => VisibleSender::Notification,
        };
        VisibleNativeInput {
            visible_sender,
            delivered_message: match &self.message {
                Message::HardAbrupt(request) => {
                    DeliveredMessage::HardAbrupt(delivered_request(&self.request_id, request))
                }
                Message::MiddleAbrupt(request) => {
                    DeliveredMessage::MiddleAbrupt(delivered_request(&self.request_id, request))
                }
                Message::Soft(request) => {
                    DeliveredMessage::Soft(delivered_request(&self.request_id, request))
                }
            },
        }
    }
}

pub(crate) fn visible_delivery_snapshot(
    value: &signal_flow::DeliverySnapshot,
) -> signal_flow::VisibleDeliverySnapshot {
    signal_flow::VisibleDeliverySnapshot {
        request_id: value.request_id.clone(),
        delivery_cursor: value.delivery_cursor,
        visible_sender: match &value.sent_by {
            SentBy::Sender(address) => VisibleSender::Sender(address.clone()),
            SentBy::Unknown => VisibleSender::Unknown,
            SentBy::Flow => VisibleSender::Notification,
        },
        address: value.address.clone(),
        visible_delivery_state: visible_delivery_state(&value.delivery_state),
    }
}

pub(crate) fn visible_send_observation(
    value: &signal_flow::SendObservation,
) -> signal_flow::VisibleSendObservation {
    signal_flow::VisibleSendObservation {
        store_identity: value.store_identity.clone(),
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
        Private::AlreadyDelivered => Visible::AlreadyDelivered,
        Private::AlreadyRead => Visible::AlreadyRead,
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
                SentBy::Flow => VisibleSender::Notification,
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
        restart_record_option: value.restart_record_option,
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
            request_id: value.request_id.clone(),
            delivery_cursor: value.delivery_cursor,
            visible_delivery_state: visible_delivery_state(&value.delivery_state),
        }),
        Private::Refused(value) => Visible::Refused(visible_refusal(value)),
        Private::PriorAdmission(value) => Visible::PriorAdmission(value.clone()),
        Private::Sent(value) => Visible::Sent(value.request_id.clone()),
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

#[cfg(test)]
mod tests {
    use super::visible_delivery_state;
    use signal_flow::{DeliveryState, HarnessKind, NativeReceipt, VisibleDeliveryState};

    #[test]
    fn visible_delivered_state_omits_native_receipt() {
        let private = DeliveryState::Delivered(NativeReceipt {
            harness_kind: HarnessKind::Codex,
            native_session_id: "private-session".into(),
            native_prompt_id: "private-prompt".into(),
        });
        assert_eq!(
            visible_delivery_state(&private),
            VisibleDeliveryState::Delivered
        );
    }
}
