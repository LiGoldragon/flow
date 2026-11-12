//! Herdr's own event stream: `events.subscribe` on a session's socket,
//! answered `subscription_started`, then one newline-delimited JSON event
//! per line. A wait on it blocks in poll(2) on the stream and any other
//! descriptor given with it; a deadline, where one is given, only bounds
//! how long an event may take.

use crate::event_wait::{
    AwaitsDescriptorInterests, AwaitsDescriptors, DescriptorInterest, DescriptorWait,
};
use crate::observation_lifetime::{CancelsObservation, ObservationCancellation};
use std::{
    io::{BufRead, BufReader, Write},
    os::fd::{AsFd, BorrowedFd},
    os::unix::net::UnixStream,
    path::Path,
    time::Instant,
};

/// Establishment, acknowledgement and event failures retain their kinds.
#[derive(Debug, thiserror::Error)]
pub(crate) enum HerdrSubscriptionFailure {
    #[error("observation cancelled: {0:?}")]
    Cancelled(crate::observation_lifetime::ObservationCancellationCause),
    #[error("Herdr subscription backlog saturated")]
    BacklogSaturated,
    #[error("Herdr subscription deadline passed")]
    DeadlinePassed,
    #[error("Herdr subscription I/O: {0}")]
    Io(#[from] std::io::Error),
    #[error("{0}")]
    Protocol(String),
}

/// Every transport phase receives the caller's explicit wait contract.
#[derive(Clone, Copy)]
struct HerdrWait<'a> {
    deadline: Option<Instant>,
    cancellation: Option<&'a ObservationCancellation>,
}

trait AwaitsHerdrTransport {
    fn check_cancellation(&self) -> Result<(), HerdrSubscriptionFailure>;
    fn check(&self) -> Result<(), HerdrSubscriptionFailure>;
    fn writable(&self, stream: &UnixStream) -> Result<(), HerdrSubscriptionFailure>;
}

#[derive(Debug)]
struct HerdrSubscriptionTransport {
    stream: UnixStream,
}

trait ConnectsHerdrSubscription: Sized {
    /// One nonblocking attempt. Saturation refuses without retaining a socket.
    fn connect(socket: &Path, wait: HerdrWait<'_>) -> Result<Self, HerdrSubscriptionFailure>;
}

struct PendingHerdrRequest {
    bytes: Vec<u8>,
    offset: usize,
}

trait WritesHerdrRequest {
    fn write_to(
        &mut self,
        stream: &mut UnixStream,
        wait: &impl AwaitsHerdrTransport,
    ) -> Result<(), HerdrSubscriptionFailure>;
}

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
    ) -> Result<Self, HerdrSubscriptionFailure>;
    /// The next event, or `others` ready, or the stream closed, or the
    /// deadline passed. Reads never block: the wait is the poll on the
    /// stream and `others` together.
    fn next_cue(
        &mut self,
        others: &[BorrowedFd<'_>],
        deadline: Option<Instant>,
        cancellation: Option<&ObservationCancellation>,
    ) -> Result<HerdrCue, HerdrSubscriptionFailure>;
}

impl AwaitsHerdrTransport for HerdrWait<'_> {
    fn check_cancellation(&self) -> Result<(), HerdrSubscriptionFailure> {
        if let Some(cause) = self.cancellation.and_then(CancelsObservation::cause) {
            return Err(HerdrSubscriptionFailure::Cancelled(cause));
        }
        Ok(())
    }

    fn check(&self) -> Result<(), HerdrSubscriptionFailure> {
        self.check_cancellation()?;
        if self
            .deadline
            .is_some_and(|deadline| Instant::now() >= deadline)
        {
            return Err(HerdrSubscriptionFailure::DeadlinePassed);
        }
        Ok(())
    }

    fn writable(&self, stream: &UnixStream) -> Result<(), HerdrSubscriptionFailure> {
        self.check()?;
        let mut descriptors = vec![DescriptorWait {
            descriptor: stream.as_fd(),
            interest: DescriptorInterest::Writable,
        }];
        if let Some(cancellation) = self.cancellation {
            descriptors.push(DescriptorWait {
                descriptor: cancellation.descriptor(),
                interest: DescriptorInterest::Readable,
            });
        }
        let ready = descriptors.ready_interests(self.deadline)?;
        self.check_cancellation()?;
        if !ready[0] {
            return Err(HerdrSubscriptionFailure::DeadlinePassed);
        }
        self.check()
    }
}

impl ConnectsHerdrSubscription for HerdrSubscriptionTransport {
    fn connect(socket: &Path, wait: HerdrWait<'_>) -> Result<Self, HerdrSubscriptionFailure> {
        use rustix::net::{AddressFamily, SocketAddrUnix, SocketFlags, SocketType};
        wait.check()?;
        let address = SocketAddrUnix::new(socket).map_err(std::io::Error::from)?;
        let descriptor = rustix::net::socket_with(
            AddressFamily::UNIX,
            SocketType::STREAM,
            SocketFlags::NONBLOCK | SocketFlags::CLOEXEC,
            None,
        )
        .map_err(std::io::Error::from)?;
        let connected = rustix::net::connect(&descriptor, &address);
        // Cancellation has priority over success and over transport refusal.
        wait.check()?;
        match connected {
            Ok(()) => Ok(Self {
                stream: UnixStream::from(descriptor),
            }),
            Err(rustix::io::Errno::AGAIN) => Err(HerdrSubscriptionFailure::BacklogSaturated),
            Err(error) => Err(std::io::Error::from(error).into()),
        }
    }
}

impl WritesHerdrRequest for PendingHerdrRequest {
    fn write_to(
        &mut self,
        stream: &mut UnixStream,
        wait: &impl AwaitsHerdrTransport,
    ) -> Result<(), HerdrSubscriptionFailure> {
        while self.offset < self.bytes.len() {
            wait.check()?;
            let written = stream.write(&self.bytes[self.offset..]);
            wait.check()?;
            match written {
                Ok(0) => return Err(std::io::Error::from(std::io::ErrorKind::WriteZero).into()),
                Ok(written) => self.offset += written,
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    wait.writable(stream)?
                }
                Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(error) => return Err(error.into()),
            }
        }
        wait.check()
    }
}

impl SubscribesHerdrEvents for HerdrEvents {
    fn subscribe(
        socket: &Path,
        id: &str,
        subscriptions: serde_json::Value,
        deadline: Option<Instant>,
        cancellation: Option<&ObservationCancellation>,
    ) -> Result<Self, HerdrSubscriptionFailure> {
        let wait = HerdrWait {
            deadline,
            cancellation,
        };
        let mut transport = HerdrSubscriptionTransport::connect(socket, wait)?;
        let request = serde_json::json!({
            "id": id,
            "method": "events.subscribe",
            "params": { "subscriptions": subscriptions }
        });
        PendingHerdrRequest {
            bytes: format!("{request}\n").into_bytes(),
            offset: 0,
        }
        .write_to(&mut transport.stream, &wait)?;
        let mut events = Self {
            reader: BufReader::new(transport.stream),
            pending: Vec::new(),
        };
        let started = match events.next_cue(&[], deadline, cancellation)? {
            HerdrCue::Event(started) => started,
            HerdrCue::Closed => {
                return Err(HerdrSubscriptionFailure::Protocol(
                    "Herdr closed the subscription unanswered".into(),
                ));
            }
            HerdrCue::Passed => return Err(HerdrSubscriptionFailure::DeadlinePassed),
            HerdrCue::Other => {
                return Err(HerdrSubscriptionFailure::Protocol(
                    "Herdr did not answer the subscription".into(),
                ));
            }
        };
        if started
            .pointer("/result/type")
            .and_then(serde_json::Value::as_str)
            != Some("subscription_started")
        {
            return Err(HerdrSubscriptionFailure::Protocol(format!(
                "Herdr refused the subscription: {started}"
            )));
        }
        Ok(events)
    }

    fn next_cue(
        &mut self,
        others: &[BorrowedFd<'_>],
        deadline: Option<Instant>,
        cancellation: Option<&ObservationCancellation>,
    ) -> Result<HerdrCue, HerdrSubscriptionFailure> {
        loop {
            if let Some(cancellation) = cancellation {
                HerdrWait {
                    deadline,
                    cancellation: Some(cancellation),
                }
                .check_cancellation()?;
            }
            // Other readiness has priority even over data already buffered.
            if !others.is_empty()
                && others
                    .ready(Some(Instant::now()))?
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
                let ready = descriptors.ready(deadline)?;
                if let Some(cancellation) = cancellation {
                    HerdrWait {
                        deadline,
                        cancellation: Some(cancellation),
                    }
                    .check_cancellation()?;
                }
                if ready[1..1 + others.len()].iter().any(|ready| *ready) {
                    return Ok(HerdrCue::Other);
                }
                if !ready[0] {
                    return Ok(HerdrCue::Passed);
                }
            }
            // A read that would block keeps what it read in `pending`.
            let read = self.reader.read_until(b'\n', &mut self.pending);
            HerdrWait {
                deadline,
                cancellation,
            }
            .check_cancellation()?;
            match read {
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
                Err(error) => return Err(error.into()),
            }
            let line = String::from_utf8_lossy(&std::mem::take(&mut self.pending)).into_owned();
            if let Ok(event) = serde_json::from_str::<serde_json::Value>(&line) {
                if let Some(cancellation) = cancellation {
                    HerdrWait {
                        deadline,
                        cancellation: Some(cancellation),
                    }
                    .check_cancellation()?;
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
            assert!(
                matches!(error, HerdrSubscriptionFailure::Cancelled(actual) if actual == cause)
            );
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
    struct WitnessedWriteWait<'a> {
        wait: HerdrWait<'a>,
        entered: mpsc::Sender<()>,
    }

    impl AwaitsHerdrTransport for WitnessedWriteWait<'_> {
        fn check_cancellation(&self) -> Result<(), HerdrSubscriptionFailure> {
            self.wait.check_cancellation()
        }
        fn check(&self) -> Result<(), HerdrSubscriptionFailure> {
            self.wait.check()
        }
        fn writable(&self, stream: &UnixStream) -> Result<(), HerdrSubscriptionFailure> {
            self.entered.send(()).unwrap();
            self.wait.writable(stream)
        }
    }

    #[test]
    fn one_connect_refuses_saturation_without_queueing_and_a_new_call_can_connect() {
        use rustix::net::{AddressFamily, SocketAddrUnix, SocketFlags, SocketType};
        let directory = tempfile::tempdir().unwrap();
        let socket = directory.path().join("herdr.sock");
        let descriptor = rustix::net::socket_with(
            AddressFamily::UNIX,
            SocketType::STREAM,
            SocketFlags::CLOEXEC,
            None,
        )
        .unwrap();
        rustix::net::bind(&descriptor, &SocketAddrUnix::new(&socket).unwrap()).unwrap();
        rustix::net::listen(&descriptor, 0).unwrap();
        let listener = std::os::unix::net::UnixListener::from(descriptor);
        let filler = UnixStream::connect(&socket).unwrap();
        let wait = HerdrWait {
            deadline: Some(Instant::now() + Duration::from_secs(3)),
            cancellation: None,
        };
        for _ in 0..10 {
            assert!(matches!(
                HerdrSubscriptionTransport::connect(&socket, wait),
                Err(HerdrSubscriptionFailure::BacklogSaturated)
            ));
        }
        let (accepted, _) = listener.accept().unwrap();
        assert!(
            ![listener.as_fd()].ready(Some(Instant::now())).unwrap()[0],
            "refused attempts left no pending connection"
        );
        let connected = HerdrSubscriptionTransport::connect(&socket, wait).unwrap();
        let (mut upstream, _) = listener.accept().unwrap();
        drop(connected);
        use std::io::Read;
        let mut byte = [0];
        assert_eq!(upstream.read(&mut byte).unwrap(), 0);
        drop(accepted);
        drop(filler);
    }

    #[test]
    fn backpressured_initial_write_preserves_both_typed_cancellation_causes() {
        for cause in [
            ObservationCancellationCause::PeerClosed,
            ObservationCancellationCause::NexusStopped,
        ] {
            let (mut stream, _upstream) = UnixStream::pair().unwrap();
            stream.set_nonblocking(true).unwrap();
            rustix::net::sockopt::set_socket_send_buffer_size(&stream, 4096).unwrap();
            let fill = [0_u8; 4096];
            loop {
                match stream.write(&fill) {
                    Ok(written) => assert!(written > 0),
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => break,
                    answer => panic!("unexpected buffer fill: {answer:?}"),
                }
            }
            let cancellation =
                Arc::new(ObservationCancellation::open(LaunchChanges::default()).unwrap());
            let observing = Arc::clone(&cancellation);
            let (entered, waiting) = mpsc::channel();
            let (done, finished) = mpsc::channel();
            let writer = std::thread::spawn(move || {
                let wait = WitnessedWriteWait {
                    wait: HerdrWait {
                        deadline: None,
                        cancellation: Some(&observing),
                    },
                    entered,
                };
                let mut request = PendingHerdrRequest {
                    bytes: b"request\n".to_vec(),
                    offset: 0,
                };
                let result = request.write_to(&mut stream, &wait);
                done.send(result).unwrap();
                assert_eq!(request.offset, 0);
            });
            waiting.recv_timeout(Duration::from_secs(3)).unwrap();
            cancellation.cancel(cause);
            assert!(
                matches!(finished.recv_timeout(Duration::from_secs(3)).unwrap(), Err(HerdrSubscriptionFailure::Cancelled(actual)) if actual == cause)
            );
            writer.join().unwrap();
        }
    }

    #[test]
    fn initial_write_resumes_by_readiness_and_sends_the_request_once() {
        use std::io::Read;
        let (mut stream, mut upstream) = UnixStream::pair().unwrap();
        stream.set_nonblocking(true).unwrap();
        rustix::net::sockopt::set_socket_send_buffer_size(&stream, 4096).unwrap();
        upstream
            .set_read_timeout(Some(Duration::from_secs(3)))
            .unwrap();
        let expected = vec![b'x'; 65536];
        let request_bytes = expected.clone();
        let (entered, waiting) = mpsc::channel();
        let writer = std::thread::spawn(move || {
            let wait = WitnessedWriteWait {
                wait: HerdrWait {
                    deadline: Some(Instant::now() + Duration::from_secs(3)),
                    cancellation: None,
                },
                entered,
            };
            let mut request = PendingHerdrRequest {
                bytes: request_bytes,
                offset: 0,
            };
            request.write_to(&mut stream, &wait).unwrap();
            assert_eq!(request.offset, request.bytes.len());
        });
        waiting.recv_timeout(Duration::from_secs(3)).unwrap();
        let mut received = Vec::new();
        upstream.read_to_end(&mut received).unwrap();
        writer.join().unwrap();
        assert_eq!(received, expected);
    }

    #[test]
    fn establishment_deadline_and_cancellation_priority_are_explicit() {
        let cancellation = ObservationCancellation::open(LaunchChanges::default()).unwrap();
        let wait = HerdrWait {
            deadline: Some(Instant::now()),
            cancellation: Some(&cancellation),
        };
        assert!(matches!(
            HerdrSubscriptionTransport::connect(Path::new("unused"), wait),
            Err(HerdrSubscriptionFailure::DeadlinePassed)
        ));
        cancellation.cancel(ObservationCancellationCause::PeerClosed);
        assert!(matches!(
            HerdrSubscriptionTransport::connect(Path::new("unused"), wait),
            Err(HerdrSubscriptionFailure::Cancelled(
                ObservationCancellationCause::PeerClosed
            ))
        ));
    }
}
