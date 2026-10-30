//! Reminders: a caller's Remind, and the reminders of a Task left with no
//! Done. Each is held in Flow's Memory (`store::reminder`), so a refresh of
//! the reminded metaflow or a restart of the Nexus keeps it. A reminder due
//! is queued for its metaflow as Soft.Notice with Wake, as from Flow, and
//! woken or handed over as any waking request is.

use crate::HoldsStopRequest;
use crate::RunningNexus;
use crate::sending::{IdentifiesSender, MintsId};
use crate::store::logical_delivery::RecordsLogicalDeliveries;
use crate::store::reminder::{
    AdvancesReminder, ReadsInstant, RecordsReminders, Reminder, Reminding,
};
use crate::waking::SleepsAndWakesSeats;
use signal_flow::{
    Address, Delivery, Message, Process, Refusal, Remind_Data, Request, Response, SentBy,
};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// How often Flow looks for reminders due.
const REMINDER_TICK: Duration = Duration::from_secs(5);

/// A time as Unix seconds.
pub trait CountsUnixSeconds {
    fn seconds_since_epoch(&self) -> i64;
}

impl CountsUnixSeconds for SystemTime {
    fn seconds_since_epoch(&self) -> i64 {
        self.duration_since(UNIX_EPOCH)
            .map(|elapsed| elapsed.as_secs() as i64)
            .unwrap_or_default()
    }
}

pub trait RemindsCallers {
    /// Holds a Remind for the calling process's metaflow.
    fn remind(&self, remind: Remind_Data, peer: Option<&Process>) -> Response;
    /// Queues every reminder due now, and wakes or hands over to each
    /// metaflow reminded. Takes the dispatch gate; the caller holds none.
    fn send_due_reminders(&self);
    /// Sends reminders as they fall due, until the Nexus stops.
    fn keep_reminding(&self);
}

impl RemindsCallers for RunningNexus {
    fn remind(&self, remind: Remind_Data, peer: Option<&Process>) -> Response {
        let Some(due) = remind.instant.as_str().unix_seconds() else {
            return Response::Refused(Refusal::InstantUnreadable(remind.instant));
        };
        let Some(peer) = peer else {
            return Response::Refused(Refusal::Unidentified(Process { pid: 0, started: 0 }));
        };
        let _turn = self
            .dispatch_gate
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let address = match self.sending_address(peer) {
            Ok(address) => address,
            Err(refusal) => return *refusal,
        };
        match self.store.hold_reminder(Reminder {
            key: MintsId::minted_id("remind"),
            address,
            reminding: Reminding::Remind(remind.body),
            due: Some(due),
            sent: 0,
        }) {
            Ok(()) => Response::Reminded,
            Err(error) => Response::Refused(Refusal::Store(error.to_string())),
        }
    }

    fn send_due_reminders(&self) {
        let now = SystemTime::now().seconds_since_epoch();
        let mut reminded: Vec<Address> = Vec::new();
        {
            let _turn = self
                .dispatch_gate
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let due = match self.store.due_reminders(now) {
                Ok(due) => due,
                Err(error) => {
                    eprintln!("flow-nexus: reminders unreadable: {error}");
                    return;
                }
            };
            for reminder in due {
                let (sent, next) = reminder.advanced(now);
                let mut notices = vec![(sent.address.clone(), sent.notice.clone())];
                notices.extend(sent.failure.clone());
                for (address, notice) in notices {
                    match self.store.accept_logical_delivery(
                        &address,
                        SentBy::Flow,
                        Delivery::Wake,
                        Message::Soft(Request::Notice(notice)),
                        None,
                    ) {
                        Ok(Response::SendAccepted(_)) => {
                            if !reminded.contains(&address) {
                                reminded.push(address);
                            }
                        }
                        answer => eprintln!(
                            "flow-nexus: reminder {} not queued for {address:?}: {answer:?}",
                            reminder.key
                        ),
                    }
                }
                let kept = match next {
                    Some(next) => self.store.hold_reminder(next),
                    None => self.store.close_reminder(&reminder.key).map(|_| ()),
                };
                if let Err(error) = kept {
                    eprintln!(
                        "flow-nexus: reminder {} not advanced: {error}",
                        reminder.key
                    );
                }
            }
        }
        for address in reminded {
            self.wake_due(address);
        }
    }

    fn keep_reminding(&self) {
        while !self.stopping.requested() {
            self.send_due_reminders();
            let mut waited = Duration::ZERO;
            while waited < REMINDER_TICK && !self.stopping.requested() {
                std::thread::sleep(Duration::from_millis(250));
                waited += Duration::from_millis(250);
            }
        }
    }
}
