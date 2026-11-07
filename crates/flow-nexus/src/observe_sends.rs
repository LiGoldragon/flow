//! Authenticated logical-delivery subscriptions, resumed by durable cursor.
use crate::sending::IdentifiesSender;
use crate::store::AnnouncesLaunchChanges;
use crate::store::logical_delivery::RecordsLogicalDeliveries;
use crate::{HoldsStopRequest, RunningNexus};
use signal_flow::{Address, Process, Refusal, Response};
use std::sync::atomic::{AtomicBool, Ordering};

pub(crate) trait ObservesLogicalSends {
    fn observation_sender(&self, peer: &Process) -> Result<Address, Box<Response>>;
    fn observe_sends(
        &self,
        sender: &Address,
        cursor: Option<i64>,
        disconnected: &AtomicBool,
        send: &mut dyn FnMut(&Response) -> Result<(), String>,
    ) -> Result<(), String>;
    fn observe_deliveries(
        &self,
        cursor: Option<i64>,
        disconnected: &AtomicBool,
        send: &mut dyn FnMut(&Response) -> Result<(), String>,
    ) -> Result<(), String>;
    fn observe_delivery_changes(
        &self,
        sender: Option<&Address>,
        cursor: Option<i64>,
        disconnected: &AtomicBool,
        send: &mut dyn FnMut(&Response) -> Result<(), String>,
    ) -> Result<(), String>;
}

impl ObservesLogicalSends for RunningNexus {
    fn observation_sender(&self, peer: &Process) -> Result<Address, Box<Response>> {
        self.sending_address(peer)
    }

    fn observe_sends(
        &self,
        sender: &Address,
        cursor: Option<i64>,
        disconnected: &AtomicBool,
        send: &mut dyn FnMut(&Response) -> Result<(), String>,
    ) -> Result<(), String> {
        self.observe_delivery_changes(Some(sender), cursor, disconnected, send)
    }

    fn observe_deliveries(
        &self,
        cursor: Option<i64>,
        disconnected: &AtomicBool,
        send: &mut dyn FnMut(&Response) -> Result<(), String>,
    ) -> Result<(), String> {
        self.observe_delivery_changes(None, cursor, disconnected, send)
    }

    fn observe_delivery_changes(
        &self,
        sender: Option<&Address>,
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
            let observation = match sender {
                Some(sender) => self.store.read_logical_delivery_snapshots(sender, cursor),
                None => self.store.read_message_delivery_snapshots(cursor),
            };
            let observation = match observation {
                Ok(observation) => observation,
                Err(error) => return send(&Response::Refused(Refusal::Store(error.to_string()))),
            };
            cursor = Some(observation.delivery_cursor);
            if disconnected.load(Ordering::Acquire) {
                return Err("the observing peer disconnected".into());
            }
            if !opened {
                send(&Response::SendsObserved(
                    crate::message_projection::visible_send_observation(&observation),
                ))?;
                opened = true;
            } else {
                for snapshot in observation.delivery_snapshot_vector {
                    send(&Response::SendChanged(
                        crate::message_projection::visible_delivery_snapshot(&snapshot),
                    ))?;
                }
            }
            // The same existing condition serves launch and delivery commits.
            // A spurious launch wake only rereads the durable cursor.
            changes.after(seen);
        }
    }
}
