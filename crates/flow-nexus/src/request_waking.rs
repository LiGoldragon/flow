//! What wakes a sleeping permaflow: the request's kind. A request for an
//! Asleep metaflow, an Order, Question, Task, Living, Result or Done, starts
//! a fresh flow with the metaflow's whole queue, oldest first; a Notice
//! alone waits in the queue for the next waking. A Reminder wakes by the
//! mark Flow records when it queues it: a Reminder a flow asked for with
//! Remind is marked Wake, as the request its asker would have sent; a
//! Task's reminder, from Flow, is marked Queue and waits for its sender's
//! next waking, as a Notice does.
//!
//! Priority has no part in the decision. It says how an Awake seat receives
//! the request: HardAbrupt interrupts, MiddleAbrupt arrives at the next tool
//! boundary, Soft at the seat's rest.
//!
//! Every place that decides whether a sleeping permaflow wakes asks this one
//! trait: the Send and the locked Deliver as they choose to start the
//! recipient at once, the queue a wake is due for (`wake_is_due`, which the
//! Codex send tool, Remind, a Push and the Nexus's start all reach), and the
//! open requests a refresh carries to the successor it owes.
//!
//! A queue entry records the decision as its `Delivery`, Wake or Queue, when
//! it is admitted. The owner's `Deliver`, the hand override, marks every
//! entry it finds Wake (`force_queue_wake`), so an Asleep metaflow it is
//! asked to deliver is woken whatever its queue holds; a `Push` adds no mark
//! and wakes by the kinds the queue holds. A queued entry wakes by its kind
//! or by its mark.

use flow_ethos::Delivery;
use signal_flow::{Message, QueueEntry, Request, SendableMessage, SendableRequest};

/// Whether a request wakes the sleeping permaflow it is queued for.
pub trait WakesSleepingPermaflow {
    fn wakes_sleeping_permaflow(&self) -> bool;

    /// How the request is recorded in its metaflow's queue: Wake when it
    /// wakes a sleeping permaflow, Queue when it waits for the next waking.
    fn queued_delivery(&self) -> Delivery {
        if self.wakes_sleeping_permaflow() {
            Delivery::Wake
        } else {
            Delivery::Queue
        }
    }
}

impl WakesSleepingPermaflow for Request {
    fn wakes_sleeping_permaflow(&self) -> bool {
        match self {
            Request::Order(_)
            | Request::Question(_)
            | Request::Living(_)
            | Request::Result(_)
            | Request::Task(_)
            | Request::Done(_) => true,
            Request::Notice(_) | Request::Reminder(_) => false,
        }
    }
}

impl WakesSleepingPermaflow for Message {
    fn wakes_sleeping_permaflow(&self) -> bool {
        let (Message::HardAbrupt(request)
        | Message::MiddleAbrupt(request)
        | Message::Soft(request)) = self;
        request.wakes_sleeping_permaflow()
    }
}

impl WakesSleepingPermaflow for SendableRequest {
    fn wakes_sleeping_permaflow(&self) -> bool {
        match self {
            SendableRequest::Order(_)
            | SendableRequest::Question(_)
            | SendableRequest::Living(_)
            | SendableRequest::Result(_)
            | SendableRequest::Task(_)
            | SendableRequest::Done(_) => true,
            SendableRequest::Notice(_) => false,
        }
    }
}

impl WakesSleepingPermaflow for SendableMessage {
    fn wakes_sleeping_permaflow(&self) -> bool {
        let (SendableMessage::HardAbrupt(request)
        | SendableMessage::MiddleAbrupt(request)
        | SendableMessage::Soft(request)) = self;
        request.wakes_sleeping_permaflow()
    }
}

impl WakesSleepingPermaflow for QueueEntry {
    fn wakes_sleeping_permaflow(&self) -> bool {
        self.message.wakes_sleeping_permaflow() || self.delivery == Delivery::Wake
    }
}

/// Whether a queue holds a request that wakes its sleeping permaflow.
pub trait HoldsWakingRequest {
    fn holds_waking_request(&self) -> bool;
}

impl HoldsWakingRequest for [QueueEntry] {
    fn holds_waking_request(&self) -> bool {
        self.iter()
            .any(WakesSleepingPermaflow::wakes_sleeping_permaflow)
    }
}
