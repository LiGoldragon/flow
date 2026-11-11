//! Logical-delivery subscriptions, resumed by durable cursor, the same to
//! every caller: ObserveSends follows every send, ObserveSender one
//! sender's, ObserveDeliveries those Message admitted.
use crate::metaflow::IdentifiesThread;
use crate::sending::IdentifiesSender;
use crate::store::AnnouncesLaunchChanges;
use crate::store::logical_delivery::RecordsLogicalDeliveries;
use crate::{HoldsStopRequest, RunningNexus};
use signal_flow::{ObserveSender_Data, Refusal, Response, SentBy};
use std::sync::atomic::{AtomicBool, Ordering};

/// Which sends an observation follows: every one, one sender's, or those
/// Message admitted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SendSelection {
    /// Every send, whoever sent it: ObserveSends.
    Every,
    /// The sends of one sender: ObserveSender.
    Sent(SentBy),
    /// The deliveries Message admitted: ObserveDeliveries.
    Message,
}

pub(crate) trait ObservesLogicalSends {
    /// The sender a forwarded ObserveSender names: the thread's metaflow
    /// when it names a Codex thread, else what the named process sends as.
    fn observation_sender(&self, forwarded: &ObserveSender_Data) -> Result<SentBy, Box<Response>>;
    fn observe_delivery_changes(
        &self,
        selection: &SendSelection,
        cursor: Option<i64>,
        disconnected: &AtomicBool,
        send: &mut dyn FnMut(&Response) -> Result<(), String>,
    ) -> Result<(), String>;
}

impl ObservesLogicalSends for RunningNexus {
    fn observation_sender(&self, forwarded: &ObserveSender_Data) -> Result<SentBy, Box<Response>> {
        if let Some(thread) = &forwarded.native_session_id_option {
            match self.identify_thread(signal_flow::IdentifyThread_Data {
                process: forwarded.process.clone(),
                native_session_id: thread.clone(),
            }) {
                Ok(Response::Identified(address)) => Ok(SentBy::Sender(address)),
                Ok(_) => Ok(SentBy::Unbound),
                Err(error) => Err(Box::new(Response::Refused(Refusal::Store(
                    error.to_string(),
                )))),
            }
        } else {
            self.sender_of(&forwarded.process)
        }
    }

    fn observe_delivery_changes(
        &self,
        selection: &SendSelection,
        mut cursor: Option<i64>,
        disconnected: &AtomicBool,
        send: &mut dyn FnMut(&Response) -> Result<(), String>,
    ) -> Result<(), String> {
        let changes = self.store.launch_changes.clone();
        let mut opened = false;
        loop {
            let seen = changes.current();
            if disconnected.load(Ordering::Acquire) {
                return Err("the observing peer disconnected".into());
            }
            if self.stopping.requested() {
                return Err("the Nexus is stopping".into());
            }
            let observation = match selection {
                SendSelection::Every => self.store.read_every_delivery_snapshot(cursor),
                SendSelection::Sent(sender) => {
                    self.store.read_logical_delivery_snapshots(sender, cursor)
                }
                SendSelection::Message => self.store.read_message_delivery_snapshots(cursor),
            };
            let observation = match observation {
                Ok(observation) => observation,
                Err(error) => return send(&Response::Refused(Refusal::Store(error.to_string()))),
            };
            cursor = Some(observation.delivery_cursor.0);
            if disconnected.load(Ordering::Acquire) {
                return Err("the observing peer disconnected".into());
            }
            if !opened {
                send(&Response::SendsObserved(
                    crate::message_projection::ProjectsSendObservation::visible_send_observation(
                        &observation,
                    ),
                ))?;
                opened = true;
            } else {
                for snapshot in observation.delivery_snapshot_vector {
                    send(&Response::SendChanged(
                        crate::message_projection::ProjectsDeliverySnapshot::visible_delivery_snapshot(&snapshot),
                    ))?;
                }
            }
            // The same existing condition serves launch and delivery commits.
            // A spurious launch wake only rereads the durable cursor.
            changes.after(seen);
        }
    }
}
