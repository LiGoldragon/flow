//! Test-build faults: the one-shot crash switches and refusal switches the
//! obligation safeguards are proved with.
//!
//! A crash loses every in-memory state and keeps only the store. A test
//! arms one crash point; when the code reaches it, after the commit it
//! names has returned and before its announcement or any later step, the
//! operation ends there by unwinding. The test then drops its Nexus and
//! opens a new one over the same store (`restarted`), which is the Nexus
//! start. A refusal switch makes the next write it names fail before its
//! commit, as a refused commit does: sema-engine commits atomically, so a
//! commit that did not land is a write refused whole.

use std::collections::BTreeSet;
use std::sync::{Condvar, Mutex};

/// The points after a successor's confirmation a crash is injected at.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum CrashPoint {
    /// K0: W1 landed; the successor holds its Address, unconfirmed.
    AfterAcknowledgement,
    /// K1, K6, K8, and K10 at a later launch: W2 landed, before the
    /// settlement's first commit.
    AfterConfirm,
    /// K2: W3 landed, before any pane step.
    AfterReapOwed,
    /// K3: Herdr answered the close, before the Reaped commit.
    AfterPaneClosed,
    /// K4: a ReapRefused rewrite landed.
    AfterReapRefused,
    /// K5: Reaped landed.
    AfterReaped,
    /// K7, and K11 at a superseding one: HandoverOwed landed.
    AfterHandoverOwed,
    /// K9: HandedOver or StartedHandedOver landed.
    AfterHandedOver,
    /// K12: the reap worker saw the pane gone, before it takes the gate.
    WatchAfterGone,
}

/// The writes a refusal switch fails once.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum RefusedWrite {
    /// W3, ReapOwed.
    ReapOwing,
    /// A ReapRefused rewrite.
    ReapRewrite,
    /// Reaped.
    ReapSettle,
    /// The flow row read an owner or predecessor is judged by.
    FlowRead,
    /// C1, StartedHandedOver.
    StartedSettle,
    /// C2, HandoverOwed.
    HandoverOwing,
    /// A HandoverRefused rewrite.
    HandoverRewrite,
    /// HandedOver.
    HandedOver,
}

/// The armed switches of one store, each fired once.
#[derive(Debug, Default)]
pub(crate) struct StoreFaults {
    crashes: Mutex<BTreeSet<CrashPoint>>,
    refusals: Mutex<BTreeSet<RefusedWrite>>,
    /// Every obligation commit attempted, in order, for the counting hook.
    attempts: Mutex<Vec<String>>,
    /// Announced each time an armed crash fires.
    fired: Condvar,
}

/// The message a crash unwinds with, so a test tells it from a failure.
pub(crate) const CRASH_MESSAGE: &str = "flow-nexus test crash";

pub(crate) trait InjectsCrashes {
    /// Arms one crash at `point`.
    fn crash_at(&self, point: CrashPoint);
    /// Unwinds when `point` is armed, disarming it.
    fn crash_if_armed(&self, point: CrashPoint);
    /// Arms one refusal of `write`.
    fn refuse_next(&self, write: RefusedWrite);
    /// Whether `write` is refused now, disarming it.
    fn refused_now(&self, write: RefusedWrite) -> bool;
    /// Notes one obligation commit attempted, named.
    fn attempted(&self, name: &str);
    /// Every obligation commit attempted so far, in order.
    fn attempts(&self) -> Vec<String>;
    /// Waits, bounded, until `point` has fired; whether it did.
    fn fired_by(&self, point: CrashPoint, deadline: std::time::Instant) -> bool;
}

impl InjectsCrashes for StoreFaults {
    fn crash_at(&self, point: CrashPoint) {
        self.crashes
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(point);
    }

    fn crash_if_armed(&self, point: CrashPoint) {
        let armed = self
            .crashes
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(&point);
        if armed {
            self.fired.notify_all();
            std::panic::panic_any(format!("{CRASH_MESSAGE} at {point:?}"));
        }
    }

    fn refuse_next(&self, write: RefusedWrite) {
        self.refusals
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(write);
    }

    fn refused_now(&self, write: RefusedWrite) -> bool {
        self.refusals
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(&write)
    }

    fn attempted(&self, name: &str) {
        self.attempts
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push(name.to_owned());
    }

    fn attempts(&self) -> Vec<String> {
        self.attempts
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    fn fired_by(&self, point: CrashPoint, deadline: std::time::Instant) -> bool {
        let mut crashes = self
            .crashes
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        while crashes.contains(&point) {
            let Some(remaining) = deadline.checked_duration_since(std::time::Instant::now()) else {
                return false;
            };
            crashes = self
                .fired
                .wait_timeout(crashes, remaining)
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .0;
        }
        true
    }
}
