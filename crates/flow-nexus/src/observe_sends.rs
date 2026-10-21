//! Authenticated sender lifecycle subscription, resumed by its durable cursor.
use crate::metaflow::IdentifiesThread;
use crate::sending::IdentifiesSender;
use crate::store::AnnouncesLaunchChanges;
use crate::store::logical_delivery::RecordsLogicalDeliveries;
use crate::store::metaflow::MutatesMetaflows;
use crate::{HoldsStopRequest, RunningNexus};
use signal_flow::{Address, ObserveSender_Data, Process, Refusal, Response};
use std::sync::atomic::{AtomicBool, Ordering};

pub(crate) trait ObservesLogicalSends {
    fn observation_sender(
        &self,
        peer: &Process,
        forwarded: Option<&ObserveSender_Data>,
    ) -> Result<Address, Response>;
    fn observe_sends(
        &self,
        sender: &Address,
        cursor: Option<i64>,
        disconnected: &AtomicBool,
        send: &mut dyn FnMut(&Response) -> Result<(), String>,
    ) -> Result<(), String>;
}

impl ObservesLogicalSends for RunningNexus {
    fn observation_sender(
        &self,
        peer: &Process,
        forwarded: Option<&ObserveSender_Data>,
    ) -> Result<Address, Response> {
        let Some(forwarded) = forwarded else {
            return self.sending_address(peer).map_err(|response| *response);
        };
        match self.store.message_peer(peer) {
            Ok(true) => {}
            Ok(false) => return Err(Response::Refused(Refusal::NotMessage)),
            Err(error) => return Err(Response::Refused(Refusal::Store(error.to_string()))),
        }
        if let Some(thread) = &forwarded.native_session_id_option {
            match self.identify_thread(signal_flow::IdentifyThread_Data {
                process: forwarded.process.clone(),
                native_session_id: thread.clone(),
            }) {
                Ok(Response::Identified(address)) => Ok(address),
                Ok(refusal) => Err(refusal),
                Err(error) => Err(Response::Refused(Refusal::Store(error.to_string()))),
            }
        } else {
            self.sending_address(&forwarded.process)
                .map_err(|response| *response)
        }
    }

    fn observe_sends(
        &self,
        sender: &Address,
        mut cursor: Option<i64>,
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
            let observation = match self.store.read_logical_delivery_snapshots(sender, cursor) {
                Ok(observation) => observation,
                Err(error) => return send(&Response::Refused(Refusal::Store(error.to_string()))),
            };
            cursor = Some(observation.delivery_cursor);
            if disconnected.load(Ordering::Acquire) {
                return Err("the observing peer disconnected".into());
            }
            if !opened {
                send(&Response::SendsObserved(observation))?;
                opened = true;
            } else {
                for snapshot in observation.delivery_snapshot_vector {
                    send(&Response::SendChanged(snapshot))?;
                }
            }
            // The same existing condition serves launch and delivery commits.
            // A spurious launch wake only rereads the durable cursor.
            changes.after(seen);
        }
    }
}
