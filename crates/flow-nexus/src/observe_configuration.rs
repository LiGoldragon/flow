//! ObserveConfiguration: the configuration as `Configuration` answers it,
//! then again each time a Configure replaces a family, until the peer goes
//! or the Nexus stops. The subscriber sleeps until a change is announced;
//! nothing re-reads on a timer.

use crate::RunningNexus;
use crate::store::{AnnouncesLaunchChanges, ManagesMetaflowState};
use meta_signal_flow::{Output, StoreRefusal};
use std::sync::atomic::{AtomicBool, Ordering};

pub trait ObservesConfiguration {
    /// The configuration as it stands: `Configuration`, or `Unconfigured`
    /// while no Nexus is declared.
    fn configuration_response(&self) -> Output;
    fn observe_configuration(
        &self,
        disconnected: &AtomicBool,
        send: &mut dyn FnMut(&Output) -> Result<(), String>,
    ) -> Result<(), String>;
}

impl ObservesConfiguration for RunningNexus {
    fn configuration_response(&self) -> Output {
        match self.store.configuration_snapshot() {
            Ok(Some(snapshot)) => Output::Configuration(snapshot),
            Ok(None) => Output::Unconfigured,
            Err(error) => Output::Refused(StoreRefusal::Store(error.to_string())),
        }
    }

    fn observe_configuration(
        &self,
        disconnected: &AtomicBool,
        send: &mut dyn FnMut(&Output) -> Result<(), String>,
    ) -> Result<(), String> {
        use crate::HoldsStopRequest;
        let changes = self.configuration_changes.clone();
        let mut sent: Option<Output> = None;
        loop {
            let seen = changes.current();
            if disconnected.load(Ordering::Acquire) {
                return Err("the observing peer disconnected".into());
            }
            if self.stopping.requested() {
                return Err("the Nexus is stopping".into());
            }
            let response = self.configuration_response();
            if sent.as_ref() != Some(&response) {
                send(&response)?;
                sent = Some(response);
            }
            changes.after(seen);
        }
    }
}
