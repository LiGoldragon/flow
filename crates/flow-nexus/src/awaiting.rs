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
use crate::generated::wake::{AgentPaneEvent, PaneEventing, WakesOnPaneEvent};
use crate::observe_agent::{HerdrEvents, LocatesHerdrSession, PaneEvent, SubscribesHerdrEvents};
use crate::store::{AnnouncesLaunchChanges, LaunchChanges};
use crate::waking::SleepsAndWakesSeats;
use signal_flow::{Address, HerdrRoute};
use std::collections::HashMap;
use std::net::Shutdown;
use std::os::unix::net::UnixStream;
use std::sync::Mutex;
use std::thread::Scope;

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

/// The panes refused wakes wait on, and the subscription open on each.
#[derive(Default)]
pub struct AwaitedPanes {
    /// Each waiting metaflow and the pane it waits on.
    awaited: Mutex<HashMap<Address, HerdrRoute>>,
    /// The open subscription of each awaited pane, by the handle that shuts
    /// it down.
    subscriptions: Mutex<HashMap<AgentPaneEvent, UnixStream>>,
    /// Announced each time a wait is recorded and when the Nexus stops.
    changes: LaunchChanges,
}

pub trait HoldsAwaitedPanes {
    /// The metaflow at `address` waits on `route`'s pane; announced.
    fn await_pane(&self, address: &Address, route: &HerdrRoute);
    /// The metaflow waits on no pane.
    fn forget(&self, address: &Address);
    /// Takes every metaflow waiting on the event's pane.
    fn take(&self, event: &AgentPaneEvent) -> Vec<Address>;
    /// Releases every wait on a pane that is gone.
    fn release(&self, event: &AgentPaneEvent);
    fn is_awaited(&self, event: &AgentPaneEvent) -> bool;
    /// Each waiting metaflow and its pane.
    fn awaited(&self) -> Vec<(Address, HerdrRoute)>;
    /// The awaited panes no subscription is open on.
    fn unsubscribed_panes(&self) -> Vec<AgentPaneEvent>;
    fn subscribed(&self, event: AgentPaneEvent, handle: UnixStream);
    fn unsubscribed(&self, event: &AgentPaneEvent);
    /// Shuts every open subscription down and announces it.
    fn close_all(&self);
    fn changes(&self) -> &LaunchChanges;
}

impl HoldsAwaitedPanes for AwaitedPanes {
    fn await_pane(&self, address: &Address, route: &HerdrRoute) {
        self.awaited
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(address.clone(), route.clone());
        self.changes.announce();
    }

    fn forget(&self, address: &Address) {
        self.awaited
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(address);
    }

    fn take(&self, event: &AgentPaneEvent) -> Vec<Address> {
        let mut awaited = self
            .awaited
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let taken = awaited
            .iter()
            .filter(|(_, route)| route.pane_event() == *event)
            .map(|(address, _)| address.clone())
            .collect::<Vec<_>>();
        for address in &taken {
            awaited.remove(address);
        }
        taken
    }

    fn release(&self, event: &AgentPaneEvent) {
        self.awaited
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .retain(|_, route| route.pane_event() != *event);
    }

    fn is_awaited(&self, event: &AgentPaneEvent) -> bool {
        self.awaited
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .values()
            .any(|route| route.pane_event() == *event)
    }

    fn awaited(&self) -> Vec<(Address, HerdrRoute)> {
        self.awaited
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .iter()
            .map(|(address, route)| (address.clone(), route.clone()))
            .collect()
    }

    fn unsubscribed_panes(&self) -> Vec<AgentPaneEvent> {
        let mut panes = self
            .awaited()
            .into_iter()
            .map(|(_, route)| route.pane_event())
            .collect::<Vec<_>>();
        panes.sort_by(|left, right| {
            (&left.herdr_session_name, &left.pane_id)
                .cmp(&(&right.herdr_session_name, &right.pane_id))
        });
        panes.dedup();
        let subscriptions = self
            .subscriptions
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        panes.retain(|pane| !subscriptions.contains_key(pane));
        panes
    }

    fn subscribed(&self, event: AgentPaneEvent, handle: UnixStream) {
        self.subscriptions
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(event, handle);
    }

    fn unsubscribed(&self, event: &AgentPaneEvent) {
        self.subscriptions
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(event);
    }

    fn close_all(&self) {
        for handle in self
            .subscriptions
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .values()
        {
            let _ = handle.shutdown(Shutdown::Both);
        }
        self.changes.announce();
    }

    fn changes(&self) -> &LaunchChanges {
        &self.changes
    }
}

impl WakesOnPaneEvent for RunningNexus {
    fn wake_on_pane_event<N: PaneEventing>(&self, input: N) -> Vec<Address> {
        let event = input.pane_event();
        let waiting = self.awaited_panes.take(&event);
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
        let changes = self.awaited_panes.changes().clone();
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
                        self.awaited_panes.subscribed(pane.clone(), handle);
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
        while self.awaited_panes.is_awaited(&pane) && !self.stopping.requested() {
            match events.next_pane_event() {
                Ok(Some(PaneEvent::Detected | PaneEvent::Status(_))) => {
                    self.wake_on_pane_event(pane.clone());
                }
                Ok(Some(PaneEvent::Gone)) => self.awaited_panes.release(&pane),
                Ok(None) | Err(_) => break,
            }
        }
        self.awaited_panes.unsubscribed(&pane);
    }
}
