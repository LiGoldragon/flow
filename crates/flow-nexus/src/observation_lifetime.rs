//! The lifetime of a streaming observation belongs to its connection.
use crate::event_wait::{Announcement, AnnouncesOnce, AwaitsDescriptors};
use crate::store::{AnnouncesLaunchChanges, LaunchChanges};
use crate::{Connection, HoldsStopRequest, RunningNexus, Stopping};
use std::os::fd::{AsFd, AsRawFd, BorrowedFd};
use std::os::unix::net::UnixStream;
use std::sync::{Arc, Mutex};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ObservationCancellationCause {
    PeerClosed,
    NexusStopped,
}

pub struct ObservationCancellation {
    cause: Mutex<Option<ObservationCancellationCause>>,
    announced: Announcement,
    changes: LaunchChanges,
}

pub trait CancelsObservation: Sized {
    fn open(changes: LaunchChanges) -> Result<Self, String>;
    fn cause(&self) -> Option<ObservationCancellationCause>;
    fn descriptor(&self) -> BorrowedFd<'_>;
    fn cancel(&self, cause: ObservationCancellationCause);
    fn check(&self) -> Result<(), String>;
}

/// Completion ends only the watcher; it does not cancel the observation.
struct ObservationLifetime {
    cancellation: Arc<ObservationCancellation>,
    completed: Arc<Announcement>,
}

trait WatchesObservation {
    fn watch(&self, peer: &UnixStream, stopping: &Stopping);
}

pub(crate) trait ServesCancellableObservation {
    fn serve_cancellable_observation(
        &mut self,
        nexus: &RunningNexus,
        observe: &mut dyn FnMut(&ObservationCancellation, &mut UnixStream) -> Result<(), String>,
    ) -> Result<(), String>;
}

impl CancelsObservation for ObservationCancellation {
    fn open(changes: LaunchChanges) -> Result<Self, String> {
        Ok(Self {
            cause: Mutex::new(None),
            announced: Announcement::open().map_err(|error| error.to_string())?,
            changes,
        })
    }

    fn cause(&self) -> Option<ObservationCancellationCause> {
        *self.cause.lock().unwrap_or_else(|error| error.into_inner())
    }

    fn descriptor(&self) -> BorrowedFd<'_> {
        self.announced.descriptor()
    }

    fn cancel(&self, cause: ObservationCancellationCause) {
        {
            let mut current = self.cause.lock().unwrap_or_else(|error| error.into_inner());
            if current.is_some() {
                return;
            }
            *current = Some(cause);
        }
        self.announced.announce();
        // The predicate in after_while is checked while holding the same
        // mutex announce acquires before notification. The cause lock is
        // released first, keeping the lock order acyclic.
        self.changes.announce();
    }

    fn check(&self) -> Result<(), String> {
        match self.cause() {
            None => Ok(()),
            Some(cause) => Err(format!("observation cancelled: {cause:?}")),
        }
    }
}

impl WatchesObservation for ObservationLifetime {
    fn watch(&self, peer: &UnixStream, stopping: &Stopping) {
        loop {
            let ready = match [
                self.completed.descriptor(),
                stopping.descriptor(),
                peer.as_fd(),
            ]
            .ready(None)
            {
                Ok(ready) => ready,
                Err(_) => {
                    self.cancellation
                        .cancel(ObservationCancellationCause::PeerClosed);
                    return;
                }
            };
            if ready[0] {
                return;
            }
            if ready[1] {
                self.cancellation
                    .cancel(ObservationCancellationCause::NexusStopped);
                return;
            }
            if ready[2] {
                let mut bytes = [0_u8; 256];
                // SAFETY: recv writes at most bytes.len() into the buffer;
                // DONTWAIT leaves the shared socket's blocking mode intact.
                let count = unsafe {
                    libc::recv(
                        peer.as_raw_fd(),
                        bytes.as_mut_ptr().cast(),
                        bytes.len(),
                        libc::MSG_DONTWAIT,
                    )
                };
                if count == 0
                    || (count < 0
                        && !matches!(
                            std::io::Error::last_os_error().kind(),
                            std::io::ErrorKind::WouldBlock | std::io::ErrorKind::Interrupted
                        ))
                {
                    self.cancellation
                        .cancel(ObservationCancellationCause::PeerClosed);
                    return;
                }
            }
        }
    }
}

impl Drop for ObservationLifetime {
    fn drop(&mut self) {
        self.completed.announce();
    }
}

impl ServesCancellableObservation for Connection {
    fn serve_cancellable_observation(
        &mut self,
        nexus: &RunningNexus,
        observe: &mut dyn FnMut(&ObservationCancellation, &mut UnixStream) -> Result<(), String>,
    ) -> Result<(), String> {
        let peer = self.peer.try_clone().map_err(|error| error.to_string())?;
        let cancellation = Arc::new(ObservationCancellation::open(
            nexus.store.launch_changes.clone(),
        )?);
        let completed = Arc::new(Announcement::open().map_err(|error| error.to_string())?);
        std::thread::scope(|scope| {
            let lifetime = ObservationLifetime {
                cancellation: Arc::clone(&cancellation),
                completed: Arc::clone(&completed),
            };
            let watcher_lifetime = ObservationLifetime {
                cancellation: Arc::clone(&cancellation),
                completed,
            };
            let watcher = std::thread::Builder::new()
                .name("flow-observer-peer".into())
                .spawn_scoped(scope, move || {
                    watcher_lifetime.watch(&peer, &nexus.stopping)
                })
                .map_err(|error| error.to_string())?;
            let result = observe(&cancellation, &mut self.peer);
            drop(lifetime);
            let joined = watcher
                .join()
                .map_err(|_| "observation watcher panicked".to_owned());
            result.and(joined)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc;
    use std::time::Duration;

    #[test]
    fn quiet_peer_close_and_stop_end_the_watcher_with_distinct_causes() {
        for expected in [
            ObservationCancellationCause::PeerClosed,
            ObservationCancellationCause::NexusStopped,
        ] {
            let (peer, client) = UnixStream::pair().unwrap();
            let stopping = Arc::new(Stopping::default());
            let cancellation =
                Arc::new(ObservationCancellation::open(LaunchChanges::default()).unwrap());
            let completed = Arc::new(Announcement::open().unwrap());
            let lifetime = ObservationLifetime {
                cancellation: Arc::clone(&cancellation),
                completed,
            };
            let stop = Arc::clone(&stopping);
            let (sent, received) = mpsc::channel();
            let watcher = std::thread::spawn(move || {
                lifetime.watch(&peer, &stop);
                sent.send(()).unwrap();
            });
            match expected {
                ObservationCancellationCause::PeerClosed => drop(client),
                ObservationCancellationCause::NexusStopped => stopping.request(),
            }
            received.recv_timeout(Duration::from_secs(3)).unwrap();
            watcher.join().unwrap();
            assert_eq!(cancellation.cause(), Some(expected));
        }
    }

    #[test]
    fn normal_completion_joins_a_watcher_even_before_it_starts_without_cancelling() {
        let (peer, _client) = UnixStream::pair().unwrap();
        let cancellation =
            Arc::new(ObservationCancellation::open(LaunchChanges::default()).unwrap());
        let completed = Arc::new(Announcement::open().unwrap());
        let lifetime = ObservationLifetime {
            cancellation: Arc::clone(&cancellation),
            completed: Arc::clone(&completed),
        };
        completed.announce();
        let (sent, received) = mpsc::channel();
        let watcher = std::thread::spawn(move || {
            lifetime.watch(&peer, &Stopping::default());
            sent.send(()).unwrap();
        });
        received.recv_timeout(Duration::from_secs(3)).unwrap();
        watcher.join().unwrap();
        assert_eq!(cancellation.cause(), None);
    }

    #[test]
    fn launch_cancellation_between_predicate_and_wait_is_not_lost() {
        let changes = LaunchChanges::default();
        let cancellation = Arc::new(ObservationCancellation::open(changes.clone()).unwrap());
        let (checked, checking) = mpsc::channel();
        let (release, released) = mpsc::channel();
        let (done, finished) = mpsc::channel();
        let seen = changes.current();
        let waiting = changes.clone();
        let observing = Arc::clone(&cancellation);
        let waiter = std::thread::spawn(move || {
            let once = Mutex::new(Some(released));
            waiting.after_while(seen, &|| {
                let continuing = observing.cause().is_none();
                if let Some(released) = once.lock().unwrap().take() {
                    checked.send(()).unwrap();
                    released.recv_timeout(Duration::from_secs(3)).unwrap();
                }
                continuing
            });
            done.send(()).unwrap();
        });
        checking.recv_timeout(Duration::from_secs(3)).unwrap();
        // Cancellation changes its predicate while the waiter holds the
        // notification mutex. Its announce must wait for the atomic wait.
        let cancelling = Arc::clone(&cancellation);
        let canceller =
            std::thread::spawn(move || cancelling.cancel(ObservationCancellationCause::PeerClosed));
        // The cause's descriptor is announced before the notifier locks.
        assert!(cancellation.descriptor().as_fd().as_raw_fd() >= 0);
        assert!(
            [cancellation.descriptor()]
                .ready(Some(std::time::Instant::now() + Duration::from_secs(3)))
                .unwrap()[0]
        );
        release.send(()).unwrap();
        finished.recv_timeout(Duration::from_secs(3)).unwrap();
        canceller.join().unwrap();
        waiter.join().unwrap();
        assert_eq!(
            cancellation.cause(),
            Some(ObservationCancellationCause::PeerClosed)
        );
        // A cancellation completed before entry cannot be missed either.
        changes.after_while(changes.current(), &|| cancellation.cause().is_none());
    }
}
