//! Herdr's own event stream: `events.subscribe` on a session's socket,
//! answered `subscription_started`, then one newline-delimited JSON event
//! per line. A wait on it blocks in poll(2) on the stream and any other
//! descriptor given with it; a deadline, where one is given, only bounds
//! how long an event may take.

use crate::event_wait::AwaitsDescriptors;
use crate::observation_lifetime::{CancelsObservation, ObservationCancellation};
use std::{
    io::{BufRead, BufReader, Write},
    os::fd::{AsFd, BorrowedFd},
    os::unix::net::UnixStream,
    path::Path,
    time::Instant,
};

/// One open subscription.
pub(crate) struct HerdrEvents {
    reader: BufReader<UnixStream>,
    /// What has arrived of the line not yet ended.
    pending: Vec<u8>,
}

/// What a wait on the stream came to.
#[derive(Debug)]
pub(crate) enum HerdrCue {
    /// One event, as Herdr sent it.
    Event(serde_json::Value),
    /// One of the other descriptors waited on with the stream is ready.
    Other,
    /// Herdr closed the stream.
    Closed,
    /// The deadline passed first.
    Passed,
}

pub(crate) trait SubscribesHerdrEvents: Sized {
    /// Subscribes to `subscriptions` (Herdr's `params.subscriptions`) under
    /// request `id`; the answer is awaited until `deadline`, when one is
    /// given.
    fn subscribe(
        socket: &Path,
        id: &str,
        subscriptions: serde_json::Value,
        deadline: Option<Instant>,
        cancellation: Option<&ObservationCancellation>,
    ) -> Result<Self, String>;
    /// The next event, or `others` ready, or the stream closed, or the
    /// deadline passed. Reads never block: the wait is the poll on the
    /// stream and `others` together.
    fn next_cue(
        &mut self,
        others: &[BorrowedFd<'_>],
        deadline: Option<Instant>,
        cancellation: Option<&ObservationCancellation>,
    ) -> Result<HerdrCue, String>;
}

impl SubscribesHerdrEvents for HerdrEvents {
    fn subscribe(
        socket: &Path,
        id: &str,
        subscriptions: serde_json::Value,
        deadline: Option<Instant>,
        cancellation: Option<&ObservationCancellation>,
    ) -> Result<Self, String> {
        if let Some(cancellation) = cancellation {
            cancellation.check()?;
        }
        let mut stream = UnixStream::connect(socket).map_err(|error| error.to_string())?;
        let request = serde_json::json!({
            "id": id,
            "method": "events.subscribe",
            "params": { "subscriptions": subscriptions }
        });
        stream
            .write_all(format!("{request}\n").as_bytes())
            .map_err(|error| error.to_string())?;
        stream
            .set_nonblocking(true)
            .map_err(|error| error.to_string())?;
        let mut events = Self {
            reader: BufReader::new(stream),
            pending: Vec::new(),
        };
        let started = match events.next_cue(&[], deadline, cancellation)? {
            HerdrCue::Event(started) => started,
            HerdrCue::Closed => return Err("Herdr closed the subscription unanswered".into()),
            HerdrCue::Passed | HerdrCue::Other => {
                return Err("Herdr did not answer the subscription".into());
            }
        };
        if started
            .pointer("/result/type")
            .and_then(serde_json::Value::as_str)
            != Some("subscription_started")
        {
            return Err(format!("Herdr refused the subscription: {started}"));
        }
        Ok(events)
    }

    fn next_cue(
        &mut self,
        others: &[BorrowedFd<'_>],
        deadline: Option<Instant>,
        cancellation: Option<&ObservationCancellation>,
    ) -> Result<HerdrCue, String> {
        loop {
            if let Some(cancellation) = cancellation {
                cancellation.check()?;
            }
            // Other readiness has priority even over data already buffered.
            if !others.is_empty()
                && others
                    .ready(Some(Instant::now()))
                    .map_err(|error| error.to_string())?
                    .iter()
                    .any(|ready| *ready)
            {
                return Ok(HerdrCue::Other);
            }
            if self.reader.buffer().is_empty() {
                let mut descriptors = vec![self.reader.get_ref().as_fd()];
                descriptors.extend_from_slice(others);
                if let Some(cancellation) = cancellation {
                    descriptors.push(cancellation.descriptor());
                }
                let ready = descriptors
                    .ready(deadline)
                    .map_err(|error| error.to_string())?;
                if let Some(cancellation) = cancellation {
                    cancellation.check()?;
                }
                if ready[1..1 + others.len()].iter().any(|ready| *ready) {
                    return Ok(HerdrCue::Other);
                }
                if !ready[0] {
                    return Ok(HerdrCue::Passed);
                }
            }
            // A read that would block keeps what it read in `pending`.
            match self.reader.read_until(b'\n', &mut self.pending) {
                Ok(0) => return Ok(HerdrCue::Closed),
                Ok(_) if self.pending.last() != Some(&b'\n') => return Ok(HerdrCue::Closed),
                Ok(_) => {}
                Err(error)
                    if matches!(
                        error.kind(),
                        std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                    ) =>
                {
                    continue;
                }
                Err(error) => return Err(error.to_string()),
            }
            let line = String::from_utf8_lossy(&std::mem::take(&mut self.pending)).into_owned();
            if let Ok(event) = serde_json::from_str::<serde_json::Value>(&line) {
                if let Some(cancellation) = cancellation {
                    cancellation.check()?;
                }
                return Ok(HerdrCue::Event(event));
            }
        }
    }
}

/// The pane a stream event is about.
pub(crate) trait NamesEventPane {
    /// `data.pane_id`, or `data.pane.pane_id` for an event carrying the
    /// whole pane (`pane.updated`).
    fn event_pane(&self) -> Option<&str>;
}

impl NamesEventPane for serde_json::Value {
    fn event_pane(&self) -> Option<&str> {
        let data = self.get("data")?;
        data.get("pane_id")
            .or_else(|| data.pointer("/pane/pane_id"))
            .and_then(serde_json::Value::as_str)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::observation_lifetime::ObservationCancellationCause;
    use crate::store::LaunchChanges;
    use std::sync::{Arc, mpsc};
    use std::time::Duration;

    #[test]
    fn withheld_acknowledgement_releases_on_each_cancellation_cause() {
        for cause in [
            ObservationCancellationCause::PeerClosed,
            ObservationCancellationCause::NexusStopped,
        ] {
            let directory = tempfile::tempdir().unwrap();
            let socket = directory.path().join("herdr.sock");
            let listener = std::os::unix::net::UnixListener::bind(&socket).unwrap();
            let cancellation =
                Arc::new(ObservationCancellation::open(LaunchChanges::default()).unwrap());
            let watching = Arc::clone(&cancellation);
            let (finished, result) = mpsc::channel();
            let observer = std::thread::spawn(move || {
                let answer = HerdrEvents::subscribe(
                    &socket,
                    "test",
                    serde_json::json!([]),
                    None,
                    Some(&watching),
                );
                finished.send(answer.err()).unwrap();
            });
            let (peer, _) = listener.accept().unwrap();
            peer.set_read_timeout(Some(Duration::from_secs(3))).unwrap();
            let mut request = String::new();
            BufReader::new(&peer).read_line(&mut request).unwrap();
            assert!(request.contains("events.subscribe"));
            cancellation.cancel(cause);
            let error = result
                .recv_timeout(Duration::from_secs(3))
                .unwrap()
                .unwrap();
            assert!(error.contains(&format!("{cause:?}")), "{error}");
            observer.join().unwrap();
            // The upstream remains open and never supplies its ack.
            drop(peer);
        }
    }

    #[test]
    fn buffered_events_yield_to_cancellation_and_other_readiness() {
        use crate::event_wait::{Announcement, AnnouncesOnce};
        for cancel in [true, false] {
            let (stream, mut upstream) = UnixStream::pair().unwrap();
            stream.set_nonblocking(true).unwrap();
            upstream.write_all(b"{\"one\":1}\n{\"two\":2}\n").unwrap();
            let cancellation = ObservationCancellation::open(LaunchChanges::default()).unwrap();
            let mut events = HerdrEvents {
                reader: BufReader::new(stream),
                pending: Vec::new(),
            };
            assert!(matches!(
                events.next_cue(&[], None, Some(&cancellation)).unwrap(),
                HerdrCue::Event(_)
            ));
            assert!(!events.reader.buffer().is_empty());
            if cancel {
                cancellation.cancel(ObservationCancellationCause::PeerClosed);
                assert!(events.next_cue(&[], None, Some(&cancellation)).is_err());
            } else {
                let ready = Announcement::open().unwrap();
                ready.announce();
                assert!(matches!(
                    events.next_cue(&[ready.descriptor()], None, None).unwrap(),
                    HerdrCue::Other
                ));
                assert!(matches!(
                    events.next_cue(&[], None, None).unwrap(),
                    HerdrCue::Event(_)
                ));
                drop(upstream);
                assert!(matches!(
                    events.next_cue(&[], None, None).unwrap(),
                    HerdrCue::Closed
                ));
            }
        }
    }
}
