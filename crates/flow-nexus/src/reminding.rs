//! Reminders: the reminders of a Task left with no Done, and the reminders
//! asked for by Remind. Each is held in Flow's Memory (`store::reminder`,
//! `store::scheduled_reminder`), so a refresh of the reminded metaflow or a
//! restart of the Nexus keeps it. A Task's reminder due is queued for its
//! metaflow as Soft.Notice, as from Flow; a reminder asked for is queued for
//! the Address it names as MiddleAbrupt `Reminder.{ RequestId String }`,
//! from whoever asked. Each is woken or handed over as any waking request
//! is.

use crate::HoldsStopRequest;
use crate::RunningNexus;
use crate::handover_archive::AccessesExistingGitHandoverArchive;
use crate::store::logical_delivery::RecordsLogicalDeliveries;
use crate::store::queue_delivery::AccountsQueueDelivery;
use crate::store::reminder::{AdvancesReminder, RecordsReminders};
use crate::waking::SleepsAndWakesSeats;
use signal_flow::{Address, Message, Request, SentBy};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

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
    /// Queues every reminder due now, and wakes or hands over to each
    /// metaflow reminded. Takes the dispatch gate; the caller holds none.
    fn send_due_reminders(&self);
    /// Sends reminders as they fall due, until the Nexus stops.
    fn keep_reminding(&self);
    /// When the earliest held reminder or handover reminder falls due
    /// after now. One already due and still held could not be sent; it is
    /// tried again at the next announced change, not on a timer.
    fn next_reminder_due(&self) -> Option<std::time::Instant>;
}

impl RemindsCallers for RunningNexus {
    fn send_due_reminders(&self) {
        use crate::handover_reminding::RemindsHandoverCallers;
        use crate::reminding::ArchivesHandoverOnTick;
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
            let retained = match self.store.retained_logical_requests() {
                Ok(retained) => retained,
                Err(error) => {
                    eprintln!("flow-nexus: retained tasks unreadable: {error}");
                    return;
                }
            };
            for reminder in due {
                let Some(task_words) = retained.iter().find_map(|request| {
                    if request.entry.logical_delivery_id.0 != reminder.key {
                        return None;
                    }
                    let request = match &request.entry.message {
                        Message::HardAbrupt(request)
                        | Message::MiddleAbrupt(request)
                        | Message::Soft(request) => request,
                    };
                    match request {
                        Request::Task(task) => Some(task.string.as_str()),
                        _ => None,
                    }
                }) else {
                    eprintln!("flow-nexus: a reminder's retained task was unreadable");
                    continue;
                };
                let (sent, next) = reminder.advanced(task_words, now);
                // The last reminder's failure goes to the same sender, after
                // it: a Notice from Flow, which no reminder follows.
                let notices = std::iter::once(sent.notice.clone()).chain(sent.failure.clone());
                for notice in notices {
                    let address = sent.address.clone();
                    match self.store.accept_logical_delivery(
                        &address,
                        SentBy::Flow,
                        flow_ethos::Delivery::Queue,
                        Message::Soft(Request::Notice(notice)),
                        None,
                    ) {
                        Ok(signal_flow::PrivateSendResponse::SendAccepted(_)) => {
                            if !reminded.contains(&address) {
                                reminded.push(address);
                            }
                        }
                        _answer => {
                            eprintln!("flow-nexus: a reminder was not queued for {address:?}")
                        }
                    }
                }
                let kept = match next {
                    Some(next) => self.store.hold_reminder(next),
                    None => self.store.close_reminder(&reminder.key).map(|_| ()),
                };
                if let Err(error) = kept {
                    eprintln!("flow-nexus: a reminder was not advanced: {error}");
                }
            }
            // A reminder asked for reaches the Address it names at
            // MiddleAbrupt, from whoever asked, and its request closes.
            use crate::store::scheduled_reminder::SchedulesReminders;
            let asked = match self.store.due_scheduled_reminders(now) {
                Ok(asked) => asked,
                Err(error) => {
                    eprintln!("flow-nexus: asked reminders unreadable: {error}");
                    Vec::new()
                }
            };
            for reminder in asked {
                let address = reminder.address.clone();
                let closed_by = match self.store.accept_logical_delivery(
                    &address,
                    reminder.sent_by.clone(),
                    flow_ethos::Delivery::Wake,
                    Message::MiddleAbrupt(Request::Reminder(flow_ethos::Reminder_Data {
                        request_id: reminder.request_id.clone(),
                        string: reminder.text.0.clone(),
                    })),
                    None,
                ) {
                    Ok(signal_flow::PrivateSendResponse::SendAccepted(acceptance)) => {
                        if !reminded.contains(&address) {
                            reminded.push(address);
                        }
                        acceptance.logical_delivery_id
                    }
                    answer => {
                        eprintln!(
                            "flow-nexus: reminder {} was not queued for {address:?}: {answer:?}",
                            reminder.request_id.0
                        );
                        signal_flow::LogicalDeliveryId(format!("unsent:{}", reminder.request_id.0))
                    }
                };
                if let Err(error) = self.store.retire_scheduled_reminder(&reminder, closed_by) {
                    eprintln!("flow-nexus: an asked reminder was not retired: {error}");
                }
            }
        }
        self.send_due_handover_reminders(SystemTime::now().seconds_since_epoch());
        self.retire_handover_archives();
        for address in reminded {
            self.wake_due(address);
        }
    }

    fn keep_reminding(&self) {
        use crate::store::AnnouncesLaunchChanges;
        // Reminders move on two events: a change the store announces (a
        // reminder held or closed, a context size, a closed generation, a
        // Configure, the stop) and the earliest due time arriving. The
        // thread blocks on the announcement, bounded by that due time.
        while !self.stopping.requested() {
            let seen = self.store.reminder_changes.current();
            self.send_due_reminders();
            match self.next_reminder_due() {
                Some(due) => {
                    self.store.reminder_changes.until(seen, due);
                }
                None => {
                    self.store.reminder_changes.after(seen);
                }
            }
        }
    }

    fn next_reminder_due(&self) -> Option<std::time::Instant> {
        use crate::store::handover_reminders::SchedulesHandoverReminders;
        let now = SystemTime::now().seconds_since_epoch();
        use crate::store::scheduled_reminder::SchedulesReminders;
        let due = [
            self.store.next_reminder_due(now).ok().flatten(),
            self.store.next_handover_reminder_due(now).ok().flatten(),
            self.store.next_scheduled_reminder_due(now).ok().flatten(),
        ]
        .into_iter()
        .flatten()
        .min()?;
        let ahead = u64::try_from(due.saturating_sub(now)).unwrap_or_default();
        Some(std::time::Instant::now() + Duration::from_secs(ahead))
    }
}

pub trait ArchivesHandoverOnTick {
    fn retire_handover_archives(&self);
}

impl ArchivesHandoverOnTick for RunningNexus {
    fn retire_handover_archives(&self) {
        use crate::handover_archive::{ExistingGitHandoverArchive, RetiresHandoverGenerations};
        use crate::store::handover::HoldsHandovers;
        use crate::store::handover_configuration::ReadsHandoverConfiguration;
        let Ok(addresses) = self.store.handover_addresses_with_closed() else {
            return;
        };
        if addresses.is_empty() {
            return;
        }
        let Ok(archive) = self.store.handover_archive() else {
            eprintln!("flow-nexus: handover archive configuration unreadable");
            return;
        };
        let meta_signal_flow::HandoverArchiveConfiguration::Configured(path) = archive else {
            eprintln!(
                "flow-nexus: handover archive is ArchiveUnconfigured; closed generations retained"
            );
            return;
        };
        let Ok(count_configuration) = self.store.handover_count() else {
            eprintln!("flow-nexus: handover count configuration unreadable");
            return;
        };
        let meta_signal_flow::HandoverCountConfiguration::Configured(count) = count_configuration
        else {
            eprintln!("flow-nexus: handover count is Unconfigured; closed generations retained");
            return;
        };
        let Ok(deadline_ms) = self.store.publication_deadline() else {
            eprintln!("flow-nexus: publication deadline configuration unreadable");
            return;
        };
        let Ok(deadline_ms) = u64::try_from(deadline_ms.0) else {
            eprintln!(
                "flow-nexus: publication deadline is unrepresentable; closed generations retained"
            );
            return;
        };
        if deadline_ms == 0 {
            eprintln!(
                "flow-nexus: publication deadline is non-positive; closed generations retained"
            );
            return;
        }
        let publisher = ExistingGitHandoverArchive::new(path.0)
            .with_deadline(Duration::from_millis(deadline_ms));
        for address in addresses {
            if let Err(error) =
                self.retire_handover_generations(&address, count as usize, &publisher)
            {
                eprintln!("flow-nexus: handover archive retry refused for {address:?}: {error}");
            }
        }
    }
}
