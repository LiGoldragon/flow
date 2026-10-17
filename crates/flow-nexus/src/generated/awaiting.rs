//! The hand-written side of the shared [`AwaitedPanes`]: the state is held
//! behind the generated handle's one lock, hidden from every module but
//! `generated`, so the capabilities that lock it live here, beside the
//! generated module.

use super::wake::{
    Addressed, AgentPaneEvent, AwaitedPane, AwaitedPanes, AwaitedPanes_State, HoldsAwaitedPanes,
    PaneEventing, Routed, Subscribing,
};
use crate::LaunchChanges;
use crate::store::AnnouncesLaunchChanges;
use signal_flow::Address;
use std::net::Shutdown;
use std::sync::{Arc, Mutex};

impl HoldsAwaitedPanes for AwaitedPanes {
    fn create() -> Self {
        Self {
            state: Arc::new(Mutex::new(AwaitedPanes_State {
                awaited_pane_vector: Vec::new(),
                pane_subscription_vector: Vec::new(),
                launch_changes: LaunchChanges::default(),
            })),
        }
    }

    fn await_pane<N: Addressed, O: Routed>(&self, input_0: N, input_1: O) -> bool {
        let address = input_0.address();
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state
            .awaited_pane_vector
            .retain(|awaited| awaited.address != address);
        state.awaited_pane_vector.push(AwaitedPane {
            address,
            herdr_route: input_1.route(),
        });
        state.launch_changes.announce();
        true
    }

    fn forget<N: Addressed>(&self, input: N) -> bool {
        let address = input.address();
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let before = state.awaited_pane_vector.len();
        state
            .awaited_pane_vector
            .retain(|awaited| awaited.address != address);
        state.awaited_pane_vector.len() != before
    }

    fn take<N: PaneEventing>(&self, input: N) -> Vec<Address> {
        let event = input.pane_event();
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let (taken, kept): (Vec<_>, Vec<_>) = std::mem::take(&mut state.awaited_pane_vector)
            .into_iter()
            .partition(|awaited| awaited.herdr_route.pane_event() == event);
        state.awaited_pane_vector = kept;
        taken.into_iter().map(|awaited| awaited.address).collect()
    }

    fn release<N: PaneEventing>(&self, input: N) -> bool {
        let event = input.pane_event();
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let before = state.awaited_pane_vector.len();
        state
            .awaited_pane_vector
            .retain(|awaited| awaited.herdr_route.pane_event() != event);
        state.awaited_pane_vector.len() != before
    }

    fn is_awaited<N: PaneEventing>(&self, input: N) -> bool {
        let event = input.pane_event();
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .awaited_pane_vector
            .iter()
            .any(|awaited| awaited.herdr_route.pane_event() == event)
    }

    fn awaited(&self) -> Vec<AwaitedPane> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .awaited_pane_vector
            .clone()
    }

    fn unsubscribed_panes(&self) -> Vec<AgentPaneEvent> {
        let state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let mut panes = state
            .awaited_pane_vector
            .iter()
            .map(|awaited| awaited.herdr_route.pane_event())
            .collect::<Vec<_>>();
        panes.sort_by(|left, right| {
            (&left.herdr_session_name, &left.pane_id)
                .cmp(&(&right.herdr_session_name, &right.pane_id))
        });
        panes.dedup();
        panes.retain(|pane| {
            !state
                .pane_subscription_vector
                .iter()
                .any(|subscription| subscription.event == *pane)
        });
        panes
    }

    fn subscribed<N: Subscribing>(&self, input: N) -> bool {
        let subscription = input.subscription();
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state
            .pane_subscription_vector
            .retain(|held| held.event != subscription.event);
        state.pane_subscription_vector.push(subscription);
        true
    }

    fn unsubscribed<N: PaneEventing>(&self, input: N) -> bool {
        let event = input.pane_event();
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let before = state.pane_subscription_vector.len();
        state
            .pane_subscription_vector
            .retain(|held| held.event != event);
        state.pane_subscription_vector.len() != before
    }

    fn close_all(&self) -> bool {
        let state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        for subscription in &state.pane_subscription_vector {
            let _ = subscription.handle.shutdown(Shutdown::Both);
        }
        state.launch_changes.announce();
        !state.pane_subscription_vector.is_empty()
    }

    fn changes(&self) -> LaunchChanges {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .launch_changes
            .clone()
    }
}
