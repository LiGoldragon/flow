//! Observe.Metaflows: the metaflows as `Metaflows` lists them on open, then,
//! at each commit that changes them, each metaflow no longer as last sent,
//! as it now stands, and the Address of each no longer listed, until the
//! observer goes or the Nexus stops.
//!
//! The subscriber sleeps on `metaflow_changes`, which Memory advances at
//! each commit to a metaflow's row or its declaration (see
//! `store::metaflow_watch`); nothing is re-read on a timer. The count is
//! taken before each read, so a commit after the read wakes it again: no
//! change is missed, and what was already sent is not sent again. Commits
//! that land before a read are sent as the state they leave.

use crate::store::metaflow_watch::WatchesMetaflows;
use crate::store::{AnnouncesLaunchChanges, StoreError};
use crate::{HoldsStopRequest, RunningNexus};
use flow_ethos::Metaflow;
use signal_flow::Response;
use std::sync::atomic::{AtomicBool, Ordering};

/// What the subscriber last sent of the metaflows.
struct SentMetaflows {
    metaflows: Vec<Metaflow>,
}

trait SendsMetaflowChanges {
    /// Sends `MetaflowChanged` for each of `listed` not as last sent, then
    /// `MetaflowGone` for each sent that `listed` no longer holds; `listed`
    /// is then what was sent.
    fn send_changes(
        &mut self,
        listed: Vec<Metaflow>,
        send: &mut dyn FnMut(&Response) -> Result<(), String>,
    ) -> Result<(), String>;
}

impl SendsMetaflowChanges for SentMetaflows {
    fn send_changes(
        &mut self,
        listed: Vec<Metaflow>,
        send: &mut dyn FnMut(&Response) -> Result<(), String>,
    ) -> Result<(), String> {
        for metaflow in &listed {
            if !self.metaflows.contains(metaflow) {
                send(&Response::MetaflowChanged(metaflow.clone()))?;
            }
        }
        for metaflow in &self.metaflows {
            if !listed.iter().any(|now| now.address == metaflow.address) {
                send(&Response::MetaflowGone(metaflow.address.clone()))?;
            }
        }
        self.metaflows = listed;
        Ok(())
    }
}

pub trait ObservesMetaflows {
    /// Sends `MetaflowsObserved` with the metaflows as `Metaflows` lists
    /// them, then each change, until the observer goes or the Nexus stops.
    fn observe_metaflows(
        &self,
        disconnected: &AtomicBool,
        send: &mut dyn FnMut(&Response) -> Result<(), String>,
    ) -> Result<(), String>;
}

impl ObservesMetaflows for RunningNexus {
    fn observe_metaflows(
        &self,
        disconnected: &AtomicBool,
        send: &mut dyn FnMut(&Response) -> Result<(), String>,
    ) -> Result<(), String> {
        let refused =
            |error: StoreError| Response::Refused(signal_flow::Refusal::Store(error.to_string()));
        if let Err(error) = self.store.watch_metaflows() {
            let message = error.to_string();
            send(&refused(error))?;
            return Err(message);
        }
        let changes = self.store.metaflow_changes.clone();
        let mut sent: Option<SentMetaflows> = None;
        loop {
            let seen = changes.current();
            if disconnected.load(Ordering::Acquire) {
                return Err("the observing peer disconnected".into());
            }
            if self.stopping.requested() {
                return Err("the Nexus is stopping".into());
            }
            match (self.store.available_metaflows(), sent.as_mut()) {
                // A commit landed during the read; it was announced, so the
                // wait below returns at once and the read is made again.
                (Err(StoreError::MetaflowReadViewChanged), _) => {}
                (Err(error), None) => {
                    let message = error.to_string();
                    send(&refused(error))?;
                    return Err(message);
                }
                (Err(error), Some(_)) => return Err(error.to_string()),
                (Ok(listed), None) => {
                    send(&Response::MetaflowsObserved(listed.clone()))?;
                    sent = Some(SentMetaflows { metaflows: listed });
                }
                (Ok(listed), Some(sent)) => sent.send_changes(listed, send)?,
            }
            changes.after(seen);
        }
    }
}
