//! Pane awaits: a refused wake whose recovery a Herdr agent event on one
//! pane can establish (`WakeRecovery::AwaitingPane`) waits on that pane, and
//! is tried again when Herdr announces `pane.agent_detected` or
//! `pane.agent_status_changed` for it.
//!
//! The wait is held in memory, keyed by the metaflow, and re-derived from
//! Flow's records: a Nexus that starts again wakes every metaflow due a wake,
//! and a wake refused for the same cause waits on the same pane again. A
//! wake refused for any other cause, or woken, replaces the wait.
//!
//! One Herdr event subscription runs per awaited pane, on a thread of the
//! Nexus's scope, opened when the wait is announced. Its read blocks with no
//! timeout; the subscription ends when the event it waited for leaves no
//! wake waiting on the pane, when Herdr closes it, or when the Nexus stops
//! and shuts every subscription down. On subscribing, and on each event for
//! the pane, the waiting wakes are taken and asked through `wake_due`, which
//! asks Flow's records whether each is still due and runs it through the one
//! wake serialization; the wake reads the pane's binding and the flow's
//! claim again. An event for another pane does nothing; a second event finds
//! nothing waiting once the first has woken the seat. A pane Herdr reports
//! closed or exited releases its waits: those wakes are tried again at the
//! next request queued for the metaflow or the Nexus's next start.

use crate::HoldsStopRequest;
use crate::RunningNexus;
#[allow(unexpected_cfgs, clippy::large_enum_variant)]
#[path = "generated/wake.rs"]
pub mod wake;
use crate::LaunchChanges;
use crate::observe_agent::{HerdrEvents, LocatesHerdrSession, PaneEvent, SubscribesHerdrEvents};
use crate::store::AnnouncesLaunchChanges;
use crate::waking::SleepsAndWakesSeats;
use signal_flow::{Address, HerdrRoute};
use std::net::Shutdown;
use std::os::unix::net::UnixStream;
use std::sync::Arc;
use std::sync::Mutex;
use std::thread::Scope;
use wake::{
    Addressed, AgentPaneEvent, AwaitedPane, AwaitedPanes_State, PaneEventing, Routed, Subscribing,
    WakesOnPaneEvent,
};
pub use wake::{AwaitedPanes, HoldsAwaitedPanes};

/// A route names the pane its events are about.
impl PaneEventing for HerdrRoute {
    fn pane_event(&self) -> AgentPaneEvent {
        AgentPaneEvent {
            herdr_session_name: self.herdr_session_name.clone(),
            pane_id: self.herdr_pane_id.clone(),
        }
    }
}

impl PaneEventing for AgentPaneEvent {
    fn pane_event(&self) -> AgentPaneEvent {
        self.clone()
    }
}

/// One open Herdr event subscription: the pane it is about, and the handle
/// that shuts it down. The handle is shared, so the subscription is read
/// from the awaited panes without moving it.
#[derive(Debug, Clone)]
pub struct PaneSubscription {
    pub event: AgentPaneEvent,
    pub handle: Arc<UnixStream>,
}

impl Subscribing for PaneSubscription {
    fn subscription(&self) -> PaneSubscription {
        self.clone()
    }
}

impl Addressed for Address {
    fn address(&self) -> Address {
        self.clone()
    }
}

impl Routed for HerdrRoute {
    fn route(&self) -> HerdrRoute {
        self.clone()
    }
}

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

impl WakesOnPaneEvent for RunningNexus {
    fn wake_on_pane_event<N: PaneEventing>(&self, input: N) -> Vec<Address> {
        let event = input.pane_event();
        let waiting = self.awaited_panes.take(event.clone());
        for address in &waiting {
            // Still due, and woken once: `wake_due` asks Flow's records and
            // holds the metaflow while it wakes; a refusal waits again.
            self.wake_due(address.clone());
        }
        waiting
    }
}

/// The Nexus's pane awaits, for its whole life.
pub trait ServesPaneAwaits {
    /// Opens a subscription on each awaited pane that has none, at the
    /// start and each time a wait is announced, until the Nexus stops.
    fn serve_pane_awaits<'scope, 'env>(&'env self, scope: &'scope Scope<'scope, 'env>);
    /// One awaited pane's subscription: tries the waiting wakes again on
    /// subscribing and on each agent event for the pane, until none waits.
    fn await_pane_events(&self, pane: AgentPaneEvent, events: HerdrEvents);
}

impl ServesPaneAwaits for RunningNexus {
    fn serve_pane_awaits<'scope, 'env>(&'env self, scope: &'scope Scope<'scope, 'env>) {
        let changes = self.awaited_panes.changes();
        while !self.stopping.requested() {
            let seen = changes.current();
            for pane in self.awaited_panes.unsubscribed_panes() {
                let subscribed = self
                    .herdr
                    .session_socket(&pane.herdr_session_name)
                    .ok_or_else(|| "Herdr session socket not found".to_owned())
                    .and_then(|socket| HerdrEvents::subscribe_awaited(&socket, &pane.pane_id))
                    .and_then(|events| events.handle().map(|handle| (events, handle)));
                match subscribed {
                    Ok((events, handle)) => {
                        self.awaited_panes.subscribed(PaneSubscription {
                            event: pane.clone(),
                            handle: Arc::new(handle),
                        });
                        scope.spawn(move || self.await_pane_events(pane, events));
                    }
                    // The wait stays: the next request queued for the
                    // metaflow, or the Nexus's next start, tries it again.
                    Err(error) => eprintln!(
                        "flow-nexus: awaited pane {} of Herdr session {} not subscribed: {error}",
                        pane.pane_id, pane.herdr_session_name
                    ),
                }
            }
            changes.after(seen);
        }
        self.awaited_panes.close_all();
    }

    fn await_pane_events(&self, pane: AgentPaneEvent, mut events: HerdrEvents) {
        // What changed before the subscription opened is not announced:
        // the waiting wakes are tried once now.
        self.wake_on_pane_event(pane.clone());
        while self.awaited_panes.is_awaited(pane.clone()) && !self.stopping.requested() {
            match events.next_pane_event() {
                Ok(Some(PaneEvent::Detected | PaneEvent::Status(_))) => {
                    self.wake_on_pane_event(pane.clone());
                }
                Ok(Some(PaneEvent::Gone)) => {
                    self.awaited_panes.release(pane.clone());
                }
                Ok(None) | Err(_) => break,
            }
        }
        self.awaited_panes.unsubscribed(pane.clone());
    }
}
