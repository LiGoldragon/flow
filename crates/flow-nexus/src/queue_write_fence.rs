//! Custody for queue writes that leave the global dispatch gate.
use std::cell::{Cell, RefCell};
use std::collections::HashSet;
use std::sync::{Condvar, Mutex, MutexGuard};

#[derive(Default)]
pub(crate) struct QueueWriteFence {
    transitions: Mutex<()>,
    state: Mutex<QueueWriteState>,
    changed: Condvar,
}

#[derive(Default)]
struct QueueWriteState {
    generation: u64,
    held: bool,
    reserved: usize,
    started: usize,
    attempts: HashSet<String>,
    requests: HashSet<String>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum QueueWriteStage {
    Reserved,
    Started,
    Revoked,
}

pub(crate) struct QueueWrite<'a> {
    fence: &'a QueueWriteFence,
    generation: u64,
    stage: Cell<QueueWriteStage>,
    prepared: RefCell<Option<PreparedWrite>>,
}

struct PreparedWrite {
    attempt: String,
    requests: Vec<String>,
}

/// Owns dispatch while reading and committing Memory, lends it while the
/// independently leased pane performs its input sequence.
pub(crate) struct QueueDispatch<'gate> {
    gate: &'gate Mutex<()>,
    turn: Option<MutexGuard<'gate, ()>>,
}

pub(crate) trait LendsQueueDispatch<'gate>: Sized {
    fn new(gate: &'gate Mutex<()>, turn: MutexGuard<'gate, ()>) -> Self;
    fn outside<Result>(&mut self, effect: impl FnOnce() -> Result) -> Result;
}

impl<'gate> LendsQueueDispatch<'gate> for QueueDispatch<'gate> {
    fn new(gate: &'gate Mutex<()>, turn: MutexGuard<'gate, ()>) -> Self {
        Self {
            gate,
            turn: Some(turn),
        }
    }

    fn outside<Result>(&mut self, effect: impl FnOnce() -> Result) -> Result {
        drop(self.turn.take());
        let result = effect();
        self.turn = Some(self.gate.lock().unwrap_or_else(|error| error.into_inner()));
        result
    }
}

pub(crate) trait CoordinatesQueueWrites {
    fn owns_attempt(&self, id: &str) -> bool;
    fn owns_request(&self, id: &str) -> bool;
    fn transition(&self) -> MutexGuard<'_, ()>;
    fn reserve(&self) -> Option<QueueWrite<'_>>;
    fn hold<Error>(&self, commit: impl FnOnce() -> Result<(), Error>) -> Result<(), Error>;
    fn resume(&self);
    fn settle_started(&self);
    fn settle_revoked(&self);
}

impl CoordinatesQueueWrites for QueueWriteFence {
    fn owns_attempt(&self, id: &str) -> bool {
        self.state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .attempts
            .contains(id)
    }

    fn owns_request(&self, id: &str) -> bool {
        self.state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .requests
            .contains(id)
    }

    fn transition(&self) -> MutexGuard<'_, ()> {
        self.transitions
            .lock()
            .unwrap_or_else(|error| error.into_inner())
    }

    fn reserve(&self) -> Option<QueueWrite<'_>> {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        if state.held {
            return None;
        }
        state.reserved += 1;
        Some(QueueWrite {
            fence: self,
            generation: state.generation,
            stage: Cell::new(QueueWriteStage::Reserved),
            prepared: RefCell::new(None),
        })
    }

    /// Serialize the durable Hold decision with the first key or byte.
    fn hold<Error>(&self, commit: impl FnOnce() -> Result<(), Error>) -> Result<(), Error> {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        commit()?;
        state.held = true;
        state.generation = state.generation.wrapping_add(1);
        self.changed.notify_all();
        Ok(())
    }

    /// The caller has made the new Acting state durable and all revoked
    /// preparations have released their reservations.
    fn resume(&self) {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        state.held = false;
    }

    fn settle_started(&self) {
        let state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        drop(
            self.changed
                .wait_while(state, |state| state.started != 0)
                .unwrap_or_else(|error| error.into_inner()),
        );
    }

    /// Before a held Nexus resumes its backlog, revoked preparations must
    /// have returned their queued work. The dispatch gate is free meanwhile.
    fn settle_revoked(&self) {
        let state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        drop(
            self.changed
                .wait_while(state, |state| state.held && state.reserved != 0)
                .unwrap_or_else(|error| error.into_inner()),
        );
    }
}

pub(crate) trait HoldsQueueWrite {
    fn prepared(&self, attempt: String, requests: Vec<String>);
    fn release_preparation(&self);
    fn start(&self) -> bool;
    fn revoked(&self) -> bool;
}

impl HoldsQueueWrite for QueueWrite<'_> {
    fn prepared(&self, attempt: String, requests: Vec<String>) {
        let mut state = self
            .fence
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        debug_assert!(self.prepared.borrow().is_none());
        let inserted = state.attempts.insert(attempt.clone());
        debug_assert!(inserted);
        for request in &requests {
            let inserted = state.requests.insert(request.clone());
            debug_assert!(inserted);
        }
        *self.prepared.borrow_mut() = Some(PreparedWrite { attempt, requests });
    }

    /// Native observation can resume as soon as the pane has returned and
    /// this writer is back under the dispatch gate. Hold still waits on
    /// the writer itself through the attempt's disposition.
    fn release_preparation(&self) {
        if let Some(prepared) = self.prepared.borrow_mut().take() {
            let mut state = self
                .fence
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            state.attempts.remove(&prepared.attempt);
            for request in prepared.requests {
                state.requests.remove(&request);
            }
        }
    }

    fn start(&self) -> bool {
        match self.stage.get() {
            QueueWriteStage::Started => return true,
            QueueWriteStage::Revoked => return false,
            QueueWriteStage::Reserved => {}
        }
        let mut state = self
            .fence
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if state.held || state.generation != self.generation {
            self.stage.set(QueueWriteStage::Revoked);
            return false;
        }
        state.started += 1;
        self.stage.set(QueueWriteStage::Started);
        true
    }

    fn revoked(&self) -> bool {
        match self.stage.get() {
            QueueWriteStage::Started => false,
            QueueWriteStage::Revoked => true,
            QueueWriteStage::Reserved => {
                let state = self
                    .fence
                    .state
                    .lock()
                    .unwrap_or_else(|error| error.into_inner());
                state.held || state.generation != self.generation
            }
        }
    }
}

impl Drop for QueueWrite<'_> {
    fn drop(&mut self) {
        self.release_preparation();
        let mut state = self
            .fence
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        state.reserved -= 1;
        if self.stage.get() == QueueWriteStage::Started {
            state.started -= 1;
        }
        self.fence.changed.notify_all();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc::channel;

    #[test]
    fn held_revokes_a_reserved_write_but_does_not_fence_other_addresses_before_hold() {
        let fence = QueueWriteFence::default();
        let first = fence.reserve().unwrap();
        let second = fence.reserve().unwrap();
        let not_started = fence.reserve().unwrap();
        assert!(first.start());
        assert!(second.start());
        fence.hold(|| Ok::<_, ()>(())).unwrap();
        assert!(!not_started.start());
        assert!(not_started.revoked());
        assert!(fence.reserve().is_none());
        std::thread::scope(|scope| {
            let (waiting, waiting_at) = channel();
            let (settled, replied) = channel();
            let custody = &fence;
            scope.spawn(move || {
                waiting.send(()).unwrap();
                custody.settle_started();
                settled.send(()).unwrap();
            });
            waiting_at.recv().unwrap();
            drop(first);
            assert!(replied.try_recv().is_err());
            drop(second);
            replied.recv().unwrap();
            drop(not_started);
        });
        fence.settle_revoked();
        fence.resume();
        assert!(fence.reserve().unwrap().start());
    }

    #[test]
    fn one_recipient_can_start_while_another_is_in_native_input() {
        use std::sync::mpsc::channel;
        let gate = Mutex::new(());
        let fence = QueueWriteFence::default();
        let (at_pane, pane_reached) = channel();
        let (settle_pane, can_settle) = channel();
        let (other_wrote, other_completed) = channel();
        let (hold_committed, hold_observed) = channel();
        let (held_answer, held_observed) = channel();
        let (reserved, reservation_reached) = channel();
        let (release, release_receiver) = channel();
        let (revoked, revoke_observed) = channel();
        std::thread::scope(|scope| {
            let (gate, fence) = (&gate, &fence);
            scope.spawn(move || {
                let mut dispatch = QueueDispatch::new(gate, gate.lock().unwrap());
                let write = fence.reserve().unwrap();
                dispatch.outside(|| {
                    assert!(write.start());
                    at_pane.send(()).unwrap();
                    can_settle.recv().unwrap();
                });
                drop(write);
            });
            pane_reached.recv().unwrap();
            scope.spawn(move || {
                let mut dispatch = QueueDispatch::new(gate, gate.lock().unwrap());
                let write = fence.reserve().unwrap();
                dispatch.outside(|| assert!(write.start()));
                other_wrote.send(()).unwrap();
            });
            other_completed.recv().unwrap();
            scope.spawn(move || {
                let mut dispatch = QueueDispatch::new(gate, gate.lock().unwrap());
                let write = fence.reserve().unwrap();
                write.prepared("pending".into(), vec!["one".into(), "two".into()]);
                dispatch.outside(|| {
                    reserved.send(()).unwrap();
                    release_receiver.recv().unwrap();
                    assert!(!write.start());
                });
                assert!(write.revoked());
                write.release_preparation();
                revoked.send(()).unwrap();
            });
            reservation_reached.recv().unwrap();
            scope.spawn(move || {
                let _transition = fence.transition();
                {
                    let _turn = gate.lock().unwrap();
                    fence.hold(|| Ok::<_, ()>(())).unwrap();
                    hold_committed.send(()).unwrap();
                }
                fence.settle_started();
                held_answer.send(()).unwrap();
            });
            hold_observed.recv().unwrap();
            assert!(matches!(
                held_observed.try_recv(),
                Err(std::sync::mpsc::TryRecvError::Empty)
            ));
            assert!(fence.owns_attempt("pending"));
            assert!(fence.owns_request("two"));
            settle_pane.send(()).unwrap();
            held_observed.recv().unwrap();
            // The Held answer depends on the started write, not on a
            // different reservation that is still in read-only preflight.
            release.send(()).unwrap();
        });
        revoke_observed.recv().unwrap();
        assert!(!fence.owns_attempt("pending"));
        assert!(!fence.owns_request("two"));
    }

    #[test]
    fn a_refused_hold_does_not_revoke_prior_custody() {
        let fence = QueueWriteFence::default();
        let queued = fence.reserve().unwrap();
        assert_eq!(fence.hold(|| Err("store refused")), Err("store refused"));
        assert!(queued.start());
    }
}
