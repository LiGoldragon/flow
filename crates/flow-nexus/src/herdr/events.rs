//! Herdr's own event stream: `events.subscribe` on a session's socket,
//! answered `subscription_started`, then one newline-delimited JSON event
//! per line. A wait on it blocks in poll(2) on the stream and any other
//! descriptor given with it; a deadline, where one is given, only bounds
//! how long an event may take.

use crate::event_wait::AwaitsDescriptors;
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
    ) -> Result<Self, String>;
    /// The next event, or `others` ready, or the stream closed, or the
    /// deadline passed. Reads never block: the wait is the poll on the
    /// stream and `others` together.
    fn next_cue(
        &mut self,
        others: &[BorrowedFd<'_>],
        deadline: Option<Instant>,
    ) -> Result<HerdrCue, String>;
}

impl SubscribesHerdrEvents for HerdrEvents {
    fn subscribe(
        socket: &Path,
        id: &str,
        subscriptions: serde_json::Value,
        deadline: Option<Instant>,
    ) -> Result<Self, String> {
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
        let started = match events.next_cue(&[], deadline)? {
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
    ) -> Result<HerdrCue, String> {
        loop {
            if self.reader.buffer().is_empty() {
                let mut descriptors = vec![self.reader.get_ref().as_fd()];
                descriptors.extend_from_slice(others);
                let ready = descriptors
                    .ready(deadline)
                    .map_err(|error| error.to_string())?;
                if ready[1..].iter().any(|ready| *ready) {
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
