//! A request, a reminder and a queue are each ended, emptied, forced or
//! read by an operation of their own, asked by anyone on the ordinary
//! socket: Cancel, Silence, Clear, ClearAll, Push, Queues and Hot; a
//! reminder is asked for with Remind, and a kept answer read with Answer.
//!
//! Each operation's name lives in the contract and in its one dispatch arm;
//! the methods here are named for what they do.

use crate::RunningNexus;
use crate::store::logical_delivery::{
    HoldsOpenRequests, MintsRequestIds, RecordsLogicalDeliveries, RequestClosure,
};
use crate::store::metaflow::{SleepsMetaflows, TakesLaunchAddress};
use crate::store::queue_delivery::AccountsQueueDelivery;
use crate::store::turn_end::RecordsTurnEnds;
use crate::store::{ManagesMetaflowState, StoreError};
use crate::waking::{
    DrainsHeldSeats, HoldsWakingMetaflows, PushesAtPriority, RestsEndedAwake, SleepsAndWakesSeats,
};
use signal_flow::{
    Address, AnswerRejection, ArchiveRejection, Message, Output, Priority, Queue, Refusal,
    Remind_Data, Request, RequestId, RequestRejection, SentBy, Unknown_Data, Warmth,
};

pub trait OperatesOnRequestsAndQueues {
    /// Closes an open request: one still queued leaves its queue, it is
    /// archived with its id and a Task's reminder, and its sender, and its
    /// recipient when it reached it, are told by a Soft Notice.
    fn cancel(&self, request_id: &RequestId) -> Output;
    /// Stops a Task's reminders; the Task stays open.
    fn silence(&self, request_id: &RequestId) -> Output;
    /// Everything queued for `address` leaves its queue.
    fn clear(&self, address: &Address) -> Output;
    /// Every queue is emptied.
    fn clear_all(&self) -> Output;
    /// The whole queue of `address` is delivered now, as one message at
    /// `priority`; an Asleep metaflow is woken with it.
    fn push(&self, address: &Address, priority: &Priority) -> Output;
    /// Every queue that holds something, oldest first.
    fn queues(&self) -> Output;
    /// Every live flow's Address, by the minutes since its last turn ended.
    fn hot(&self) -> Output;
    /// Keeps a reminder for the Address `remind` names, sent from
    /// `sent_by` when its minutes lapse; answers the request id it is
    /// named by.
    fn remind(&self, remind: Remind_Data, sent_by: SentBy) -> Output;
    /// The answer kept with the request `request_id`.
    fn answer(&self, request_id: &RequestId) -> Output;
}

/// The steps the operations share.
trait EmptiesQueues {
    /// Takes every request queued for `address` out of its queue, answering
    /// the ids that left. Called under the dispatch gate.
    fn withdraw_all(&self, address: &Address) -> Result<Vec<RequestId>, StoreError>;
    /// Queues a Soft Notice from Flow for `address`.
    fn notify(&self, address: &Address, text: String) -> Result<(), StoreError>;
    /// The request ids queued for `address` now.
    fn queued_ids(&self, address: &Address) -> Result<Vec<RequestId>, StoreError>;
}

impl EmptiesQueues for RunningNexus {
    fn withdraw_all(&self, address: &Address) -> Result<Vec<RequestId>, StoreError> {
        let Some(metaflow) = self.store.metaflow(address)? else {
            return Ok(Vec::new());
        };
        let mut withdrawn = Vec::new();
        for entry in metaflow.queue.0 {
            if let Ok(()) = self
                .store
                .withdraw_queued(address, &entry.logical_delivery_id.0)?
            {
                withdrawn.push(entry.request_id);
            }
        }
        Ok(withdrawn)
    }

    fn notify(&self, address: &Address, text: String) -> Result<(), StoreError> {
        self.store
            .accept_logical_delivery(
                address,
                SentBy::Flow,
                flow_ethos::Delivery::Queue,
                Message::Soft(Request::Notice(text)),
                None,
            )
            .map(|_| ())
    }

    fn queued_ids(&self, address: &Address) -> Result<Vec<RequestId>, StoreError> {
        Ok(self
            .store
            .metaflow(address)?
            .map(|metaflow| {
                metaflow
                    .queue
                    .0
                    .into_iter()
                    .map(|entry| entry.request_id)
                    .collect()
            })
            .unwrap_or_default())
    }
}

impl OperatesOnRequestsAndQueues for RunningNexus {
    fn cancel(&self, request_id: &RequestId) -> Output {
        use crate::store::scheduled_reminder::SchedulesReminders;
        let refused = |rejection| Output::CancelRejected(rejection);
        let mut told = Vec::new();
        {
            let _turn = self
                .dispatch_gate
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            // A reminder not yet sent is ended where it waits; it reached
            // no one, so only its asker is told.
            match self.store.scheduled_reminder(request_id) {
                Ok(Some(reminder)) => {
                    let cancelled =
                        signal_flow::LogicalDeliveryId(format!("cancelled:{}", request_id.0));
                    if self
                        .store
                        .retire_scheduled_reminder(&reminder, cancelled)
                        .is_err()
                    {
                        return refused(RequestRejection::StoreRefused);
                    }
                    if let SentBy::Sender(asker) = &reminder.sent_by {
                        let text = format!("Request {} was cancelled.", request_id.0);
                        if let Err(error) = self.notify(asker, text) {
                            eprintln!(
                                "flow-nexus: a cancellation notice for {asker:?} was not queued: {error}"
                            );
                        }
                        told.push(asker.clone());
                    }
                    drop(_turn);
                    for address in told {
                        self.wake_due(address);
                    }
                    return Output::Cancelled(request_id.clone());
                }
                Ok(None) => {}
                Err(_) => return refused(RequestRejection::StoreRefused),
            }
            let binding = match self.store.request_binding(request_id) {
                Ok(Some(binding)) => binding,
                Ok(None) => return refused(RequestRejection::UnknownRequest),
                Err(_) => return refused(RequestRejection::StoreRefused),
            };
            if binding.request_closure != RequestClosure::Open {
                return refused(RequestRejection::Closed);
            }
            let retained = match self.store.retained_logical_requests() {
                Ok(retained) => retained.into_iter().find(|request| {
                    request.entry.logical_delivery_id == binding.logical_delivery_id
                }),
                Err(_) => return refused(RequestRejection::StoreRefused),
            };
            let Some(retained) = retained else {
                return refused(RequestRejection::UnknownRequest);
            };
            let reached = match self
                .store
                .delivered_to_be_answered(&binding.logical_delivery_id)
            {
                Ok(reached) => reached,
                Err(_) => return refused(RequestRejection::StoreRefused),
            };
            let queued = match self.store.metaflow(&retained.address) {
                Ok(metaflow) => metaflow.is_some_and(|metaflow| {
                    metaflow
                        .queue
                        .0
                        .iter()
                        .any(|entry| entry.logical_delivery_id == binding.logical_delivery_id)
                }),
                Err(_) => return refused(RequestRejection::StoreRefused),
            };
            // A request still queued that never reached its seat leaves the
            // queue; one that reached it is closed where it stands.
            if queued
                && !reached
                && self
                    .store
                    .withdraw_queued(&retained.address, &binding.logical_delivery_id.0)
                    .is_err()
            {
                return refused(RequestRejection::StoreRefused);
            }
            match self.store.archive_request(&request_id.0) {
                Ok(Ok(())) => {}
                Ok(Err(ArchiveRejection::UnknownRequest)) => {
                    return refused(RequestRejection::UnknownRequest);
                }
                Ok(Err(ArchiveRejection::NotYet)) => return refused(RequestRejection::NotYet),
                Ok(Err(ArchiveRejection::StoreRefused)) | Err(_) => {
                    return refused(RequestRejection::StoreRefused);
                }
            }
            let text = format!("Request {} was cancelled.", request_id.0);
            if let SentBy::Sender(sender) = &retained.entry.sent_by {
                told.push(sender.clone());
            }
            if reached && !told.contains(&retained.address) {
                told.push(retained.address.clone());
            }
            for address in &told {
                if let Err(error) = self.notify(address, text.clone()) {
                    eprintln!(
                        "flow-nexus: a cancellation notice for {address:?} was not queued: {error}"
                    );
                }
            }
        }
        for address in told {
            self.wake_due(address);
        }
        Output::Cancelled(request_id.clone())
    }

    fn silence(&self, request_id: &RequestId) -> Output {
        use crate::store::reminder::RecordsReminders;
        let refused = |rejection| Output::SilenceRejected(rejection);
        let binding = match self.store.request_binding(request_id) {
            Ok(Some(binding)) => binding,
            Ok(None) => return refused(RequestRejection::UnknownRequest),
            Err(_) => return refused(RequestRejection::StoreRefused),
        };
        if binding.request_closure != RequestClosure::Open {
            return refused(RequestRejection::Closed);
        }
        let task = match self.store.retained_logical_requests() {
            Ok(retained) => retained.into_iter().any(|request| {
                request.entry.logical_delivery_id == binding.logical_delivery_id
                    && matches!(
                        &request.entry.message,
                        Message::HardAbrupt(Request::Task(_))
                            | Message::MiddleAbrupt(Request::Task(_))
                            | Message::Soft(Request::Task(_))
                    )
            }),
            Err(_) => return refused(RequestRejection::StoreRefused),
        };
        if !task {
            return refused(RequestRejection::NotTask);
        }
        match self.store.close_reminder(&binding.logical_delivery_id.0) {
            Ok(_) => Output::Silenced(request_id.clone()),
            Err(_) => refused(RequestRejection::StoreRefused),
        }
    }

    fn clear(&self, address: &Address) -> Output {
        let _turn = self
            .dispatch_gate
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        match self.store.metaflow(address) {
            Ok(Some(_)) => {}
            Ok(None) => {
                return Output::Refused(Refusal::Unknown(Unknown_Data::Address(address.clone())));
            }
            Err(error) => return Output::Refused(Refusal::Store(error.to_string())),
        }
        match self.withdraw_all(address) {
            Ok(withdrawn) => Output::Cleared(withdrawn),
            Err(error) => Output::Refused(Refusal::Store(error.to_string())),
        }
    }

    fn clear_all(&self) -> Output {
        let _turn = self
            .dispatch_gate
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let metaflows = match self.store.list_metaflows() {
            Ok(metaflows) => metaflows,
            Err(error) => return Output::Refused(Refusal::Store(error.to_string())),
        };
        let mut withdrawn = Vec::new();
        for metaflow in metaflows {
            match self.withdraw_all(&metaflow.address) {
                Ok(mut ids) => withdrawn.append(&mut ids),
                Err(error) => return Output::Refused(Refusal::Store(error.to_string())),
            }
        }
        Output::AllCleared(withdrawn)
    }

    fn push(&self, address: &Address, priority: &Priority) -> Output {
        let (before, asleep) = {
            let _turn = self
                .dispatch_gate
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            self.rest_ended_awake(address);
            let metaflow = match self.store.metaflow(address) {
                Ok(Some(metaflow)) => metaflow,
                Ok(None) => {
                    return Output::Refused(Refusal::Unknown(Unknown_Data::Address(
                        address.clone(),
                    )));
                }
                Err(error) => return Output::Refused(Refusal::Store(error.to_string())),
            };
            if metaflow.queue.0.is_empty() {
                return Output::Pushed(Vec::new());
            }
            let before = metaflow
                .queue
                .0
                .iter()
                .map(|entry| entry.request_id.clone())
                .collect::<Vec<_>>();
            // Every entry wakes, so an Asleep metaflow is woken with it.
            if let Err(error) = self.store.force_queue_wake(address) {
                return Output::Refused(Refusal::Store(error.to_string()));
            }
            let asleep = metaflow.state == flow_ethos::State::Asleep;
            if !asleep && let Some(lease) = self.waking.hold(address) {
                let _ = self.drain_all_held(address.clone(), &lease.pushed_at(priority.clone()));
            }
            (before, asleep)
        };
        if asleep {
            self.wake_due(address.clone());
        }
        match self.queued_ids(address) {
            Ok(after) => Output::Pushed(
                before
                    .into_iter()
                    .filter(|request_id| !after.contains(request_id))
                    .collect(),
            ),
            Err(error) => Output::Refused(Refusal::Store(error.to_string())),
        }
    }

    fn queues(&self) -> Output {
        use crate::message_projection::ProjectsQueueEntry;
        let metaflows = match self.store.list_metaflows() {
            Ok(metaflows) => metaflows,
            Err(error) => return Output::Refused(Refusal::Store(error.to_string())),
        };
        Output::Queues(
            metaflows
                .into_iter()
                .filter(|metaflow| !metaflow.queue.0.is_empty())
                .map(|metaflow| Queue {
                    // Each as it would land in the prompt: its sender and
                    // priority with the request.
                    visible_native_input_vector: metaflow
                        .queue
                        .0
                        .iter()
                        .map(|entry| entry.visible_input(&metaflow.address))
                        .collect(),
                    address: metaflow.address,
                })
                .collect(),
        )
    }

    fn hot(&self) -> Output {
        use crate::reminding::CountsUnixSeconds;
        let metaflows = match self.store.list_metaflows() {
            Ok(metaflows) => metaflows,
            Err(error) => return Output::Refused(Refusal::Store(error.to_string())),
        };
        let now = std::time::SystemTime::now().seconds_since_epoch();
        let mut warm = Vec::new();
        for metaflow in metaflows {
            let flow_ethos::State::Awake(flow_id) = &metaflow.state else {
                continue;
            };
            match self.store.flow_is_live(&flow_id.0) {
                Ok(true) => {}
                Ok(false) => continue,
                Err(error) => return Output::Refused(Refusal::Store(error.to_string())),
            }
            let minutes = match self.store.last_turn_end_of(&metaflow.address) {
                // The last turn end held is this flow's own, not a
                // predecessor's.
                Ok(Some(turn_end)) if &turn_end.flow_id == flow_id => Some(flow_ethos::Minutes(
                    now.saturating_sub(turn_end.observed_at.0).max(0) / 60,
                )),
                Ok(_) => None,
                Err(error) => return Output::Refused(Refusal::Store(error.to_string())),
            };
            warm.push(Warmth {
                address: metaflow.address,
                minutes_option: minutes,
            });
        }
        // A flow none of whose turns has ended yet is the warmest.
        warm.sort_by_key(|warmth| warmth.minutes_option.as_ref().map(|minutes| minutes.0));
        Output::Hot(warm)
    }

    fn remind(&self, remind: Remind_Data, sent_by: SentBy) -> Output {
        use crate::reminding::CountsUnixSeconds;
        use crate::store::reminder::RulesMinutes;
        use crate::store::scheduled_reminder::SchedulesReminders;
        let address = remind.address;
        {
            use crate::launching::NamesTopic;
            if !address.topic.0.as_str().is_camel_case_topic() {
                return Output::Refused(Refusal::TopicRefused(address.topic.clone()));
            }
        }
        let now = std::time::SystemTime::now().seconds_since_epoch();
        // A reminder's Minutes meets a Task's rule: above zero, and a
        // deadline inside the clock.
        if let Err(refusal) = remind.minutes.0.ruled_interval(now) {
            return Output::Refused(Refusal::MinutesRefused(refusal));
        }
        let due = now.saturating_add(remind.minutes.0.saturating_mul(60));
        let _turn = self
            .dispatch_gate
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        match self.store.metaflow(&address) {
            Ok(Some(_)) => {}
            Ok(None) => {
                return Output::Refused(Refusal::Unknown(Unknown_Data::Address(address)));
            }
            Err(error) => return Output::Refused(Refusal::Store(error.to_string())),
        }
        match self
            .store
            .schedule_reminder(address, sent_by, due, remind.string)
        {
            Ok(Ok(request_id)) => Output::Reminded(request_id),
            Ok(Err(refusal)) => Output::Refused(refusal),
            Err(error) => Output::Refused(Refusal::Store(error.to_string())),
        }
    }

    fn answer(&self, request_id: &RequestId) -> Output {
        use crate::store::kept_answer::RoutesAnswers;
        let refused = |rejection| Output::AnswerRejected(rejection);
        match self.store.kept_answer(request_id) {
            Ok(Some(kept)) => return Output::Answered(kept.visible_native_input),
            Ok(None) => {}
            Err(_) => return refused(AnswerRejection::StoreRefused),
        }
        match self.store.request_binding(request_id) {
            Ok(Some(binding)) if binding.request_closure == RequestClosure::Open => {
                refused(AnswerRejection::Open)
            }
            Ok(Some(_)) => refused(AnswerRejection::Routed),
            Ok(None) => refused(AnswerRejection::UnknownRequest),
            Err(_) => refused(AnswerRejection::StoreRefused),
        }
    }
}
