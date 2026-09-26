//! The pane lease: Flow's exclusive hold on one pane for a whole key
//! sequence (interrupt, text, submit), a Command, the brief continuation,
//! or a close. Two writes to one pane never interleave; writes to different
//! panes do not wait for each other. The lease lives inside the Nexus and is
//! never handed out on the wire, so no client can hold a pane.

use signal_flow::HerdrRoute;
use std::{
    collections::BTreeSet,
    sync::{Condvar, Mutex},
};

/// A pane as Herdr binds it for its life: the session, pane and terminal.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct LeasedPane {
    herdr_session_name: String,
    herdr_pane_id: String,
    herdr_terminal_id: String,
}

impl From<&HerdrRoute> for LeasedPane {
    fn from(route: &HerdrRoute) -> Self {
        Self {
            herdr_session_name: route.herdr_session_name.clone(),
            herdr_pane_id: route.herdr_pane_id.clone(),
            herdr_terminal_id: route.herdr_terminal_id.clone(),
        }
    }
}

/// The panes currently held, and the wait for one to be let go.
#[derive(Default)]
pub struct PaneLeases {
    held: Mutex<BTreeSet<LeasedPane>>,
    released: Condvar,
}

/// One held pane. Dropping it lets the pane go.
pub struct PaneLease<'leases> {
    leases: &'leases PaneLeases,
    pane: LeasedPane,
}

/// Takes the exclusive hold on a pane, waiting while another write holds it.
pub trait LeasesPanes {
    fn hold(&self, route: &HerdrRoute) -> PaneLease<'_>;
}

impl LeasesPanes for PaneLeases {
    fn hold(&self, route: &HerdrRoute) -> PaneLease<'_> {
        let pane = LeasedPane::from(route);
        let mut held = self
            .held
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        while held.contains(&pane) {
            held = self
                .released
                .wait(held)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
        }
        held.insert(pane.clone());
        PaneLease { leases: self, pane }
    }
}

impl Drop for PaneLease<'_> {
    fn drop(&mut self) {
        self.leases
            .held
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(&self.pane);
        self.leases.released.notify_all();
    }
}

#[cfg(test)]
mod tests {
    use super::{LeasesPanes, PaneLeases};
    use signal_flow::HerdrRoute;
    use std::sync::{Arc, Mutex};

    fn route(pane: &str) -> HerdrRoute {
        HerdrRoute {
            herdr_session_name: "fixture".into(),
            herdr_agent_name: "agent".into(),
            herdr_pane_id: pane.into(),
            herdr_terminal_id: format!("terminal-{pane}"),
        }
    }

    /// Each writer records its whole sequence as begin then end. Under the
    /// lease no sequence on one pane may begin while another is open.
    #[test]
    fn two_writes_to_one_pane_never_interleave() {
        let leases = Arc::new(PaneLeases::default());
        let journal = Arc::new(Mutex::new(Vec::new()));
        let writers: Vec<_> = (0..4)
            .map(|writer| {
                let leases = leases.clone();
                let journal = journal.clone();
                std::thread::spawn(move || {
                    for _ in 0..25 {
                        let _lease = leases.hold(&route("w1:p1"));
                        journal.lock().unwrap().push(("begin", writer));
                        std::thread::yield_now();
                        journal.lock().unwrap().push(("end", writer));
                    }
                })
            })
            .collect();
        for writer in writers {
            writer.join().unwrap();
        }
        let journal = journal.lock().unwrap();
        assert_eq!(journal.len(), 200);
        for pair in journal.chunks(2) {
            assert_eq!(pair[0].0, "begin");
            assert_eq!(pair[1], ("end", pair[0].1));
        }
    }

    #[test]
    fn a_held_pane_does_not_hold_another() {
        let leases = PaneLeases::default();
        let _first = leases.hold(&route("w1:p1"));
        let _second = leases.hold(&route("w1:p2"));
    }
}
