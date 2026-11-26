//! One wire subscription for a session socket, shared for the Nexus's life.
//! Events invalidate the transport view. A consumer re-reads ownership;
//! neither an edge nor a disconnected stream is evidence of native death.
use super::events::{
    HerdrCue, HerdrEvents, HerdrSubscriptionFailure, NamesEventPane, SubscribesHerdrEvents,
};
use crate::event_wait::{Announcement, AnnouncesOnce, AwaitsDescriptors, NameWatch, WatchesName};
use crate::observation_lifetime::{
    CancelsObservation, ObservationCancellation, ObservationCancellationCause,
};
use crate::store::{AnnouncesLaunchChanges, LaunchChanges};
use std::{
    collections::BTreeMap,
    os::fd::BorrowedFd,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    thread::JoinHandle,
    time::Instant,
};

#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
enum SourceState {
    #[default]
    Connecting,
    Connected,
    Unavailable,
}

struct SessionView {
    state: SourceState,
    revision: u64,
    opening: u64,
    panes: BTreeMap<String, u64>,
    next: Arc<Announcement>,
    notices: Arc<Mutex<Vec<LaunchChanges>>>,
}
trait AnnouncesSessionChanges {
    fn changed(&self, state: SourceState, pane: Option<&str>) -> std::io::Result<()>;
}
impl AnnouncesSessionChanges for Mutex<SessionView> {
    fn changed(&self, state: SourceState, pane: Option<&str>) -> std::io::Result<()> {
        let next = Arc::new(Announcement::open()?);
        let mut view = self.lock().unwrap_or_else(|e| e.into_inner());
        view.state = state;
        view.revision += 1;
        let seq = view.revision;
        if let Some(pane) = pane {
            view.panes.insert(pane.to_owned(), seq);
        } else {
            view.opening = seq;
            view.panes.clear();
        }
        for sink in view
            .notices
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
        {
            sink.announce();
        }
        std::mem::replace(&mut view.next, next).announce();
        Ok(())
    }
}

struct SessionOwner {
    view: Arc<Mutex<SessionView>>,
    stop: Arc<ObservationCancellation>,
    thread: Option<JoinHandle<()>>,
}
impl Drop for SessionOwner {
    fn drop(&mut self) {
        self.stop.cancel(ObservationCancellationCause::NexusStopped);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

#[derive(Default)]
pub(crate) struct SessionStreams {
    sessions: Mutex<BTreeMap<PathBuf, SessionOwner>>,
    notices: Arc<Mutex<Vec<LaunchChanges>>>,
}

pub(crate) struct SessionCursor {
    view: Arc<Mutex<SessionView>>,
    revision: Option<u64>,
    pane: Option<String>,
}

pub(crate) trait SharesSessionStreams {
    fn retain_listed(&self, sockets: &std::collections::BTreeSet<PathBuf>);
    fn is_connected(&self, socket: &Path) -> bool;
    fn announce_to(&self, notices: LaunchChanges);
    fn follow(
        &self,
        socket: &Path,
        pane: Option<&str>,
    ) -> Result<SessionCursor, HerdrSubscriptionFailure>;
}
impl SharesSessionStreams for SessionStreams {
    fn retain_listed(&self, sockets: &std::collections::BTreeSet<PathBuf>) {
        self.sessions
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .retain(|path, _| sockets.contains(path));
    }
    fn is_connected(&self, socket: &Path) -> bool {
        self.sessions
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(socket)
            .is_some_and(|owner| {
                owner.view.lock().unwrap_or_else(|e| e.into_inner()).state == SourceState::Connected
            })
    }
    fn announce_to(&self, notices: LaunchChanges) {
        self.notices
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(notices);
    }
    fn follow(
        &self,
        socket: &Path,
        pane: Option<&str>,
    ) -> Result<SessionCursor, HerdrSubscriptionFailure> {
        let mut sessions = self.sessions.lock().unwrap_or_else(|e| e.into_inner());
        if !sessions.contains_key(socket) {
            let view = Arc::new(Mutex::new(SessionView {
                state: SourceState::Connecting,
                revision: 0,
                opening: 0,
                panes: BTreeMap::new(),
                notices: self.notices.clone(),
                next: Arc::new(Announcement::open()?),
            }));
            let stop = Arc::new(
                ObservationCancellation::open(Default::default())
                    .map_err(HerdrSubscriptionFailure::Protocol)?,
            );
            let thread = {
                let cancel = stop.clone();
                let changes = view.clone();
                let socket = socket.to_owned();
                std::thread::Builder::new().name("herdr-session".into()).spawn(move || {
                    let mut retry_on_loss = true;
                    while cancel.cause().is_none() {
                        let Ok(appeared) = Announcement::open().map(Arc::new) else { break };
                        let cue = appeared.clone();
                        let watch = NameWatch::arm(&socket, Box::new(move || { cue.announce(); true }));
                        // Arm before connect. A replacement that occurs during
                        // a failed connection is the next attempt's cause.
                        let mut lost_connected_stream = false;
                        if let Ok(mut events) = HerdrEvents::subscribe(&socket, "flow-nexus:session",
                            serde_json::json!([{"type":"pane.updated"},{"type":"pane.created"},{"type":"pane.agent_detected"},{"type":"pane.closed"},{"type":"pane.exited"}]), None, Some(&cancel)) {
                            lost_connected_stream = true;
                            if changes.changed(SourceState::Connected,None).is_err() { break; }
                            while let Ok(HerdrCue::Event(event)) = events.next_cue(&[], None, Some(&cancel)) {
                                if changes.changed(SourceState::Connected,event.event_pane()).is_err() { return; }
                            }
                        }
                        let _ = changes.changed(SourceState::Unavailable,None);
                        if watch.is_err() || cancel.cause().is_some() { break; }
                        // A dropped healthy connection is itself a cause for one new
                        // attempt on the same pathname. An immediately flapping or
                        // refusing source must not sustain an autonomous retry loop.
                        if lost_connected_stream && retry_on_loss {
                            retry_on_loss = false;
                            continue;
                        }
                        if !matches!([cancel.descriptor(), appeared.descriptor()].ready(None), Ok(ready) if !ready[0] && ready[1]) { break; }
                        retry_on_loss = true;
                    }
                    let _ = changes.changed(SourceState::Unavailable,None);
                })?
            };
            sessions.insert(
                socket.to_owned(),
                SessionOwner {
                    view,
                    stop,
                    thread: Some(thread),
                },
            );
        }
        Ok(SessionCursor {
            view: sessions[socket].view.clone(),
            revision: None,
            pane: pane.map(str::to_owned),
        })
    }
}

#[derive(PartialEq, Eq, Debug)]
pub(crate) enum SessionCue {
    Changed,
    Unavailable,
    Deadline,
    Other,
}
pub(crate) trait ReadsSessionChanges {
    fn available(&self) -> bool;
    fn take_transport_change(&mut self) -> Option<bool>;
    fn next_change(
        &mut self,
        others: &[BorrowedFd<'_>],
        deadline: Option<Instant>,
        cancellation: Option<&ObservationCancellation>,
    ) -> Result<SessionCue, HerdrSubscriptionFailure>;
    fn connected(
        &mut self,
        deadline: Option<Instant>,
        cancellation: Option<&ObservationCancellation>,
    ) -> Result<(), HerdrSubscriptionFailure>;
}
impl ReadsSessionChanges for SessionCursor {
    fn available(&self) -> bool {
        self.view.lock().unwrap_or_else(|e| e.into_inner()).state == SourceState::Connected
    }
    fn take_transport_change(&mut self) -> Option<bool> {
        let view = self.view.lock().unwrap_or_else(|e| e.into_inner());
        let revision = self.pane.as_ref().map_or(view.revision, |pane| {
            view.panes.get(pane).copied().unwrap_or(view.opening)
        });
        if self.revision == Some(revision) {
            return None;
        }
        self.revision = Some(revision);
        match view.state {
            SourceState::Connecting => None,
            SourceState::Connected => Some(true),
            SourceState::Unavailable => Some(false),
        }
    }
    fn connected(
        &mut self,
        deadline: Option<Instant>,
        cancellation: Option<&ObservationCancellation>,
    ) -> Result<(), HerdrSubscriptionFailure> {
        loop {
            match self.next_change(&[], deadline, cancellation)? {
                SessionCue::Changed => return Ok(()),
                SessionCue::Deadline => return Err(HerdrSubscriptionFailure::DeadlinePassed),
                SessionCue::Unavailable | SessionCue::Other => {}
            }
        }
    }
    fn next_change(
        &mut self,
        others: &[BorrowedFd<'_>],
        deadline: Option<Instant>,
        cancellation: Option<&ObservationCancellation>,
    ) -> Result<SessionCue, HerdrSubscriptionFailure> {
        loop {
            if let Some(cause) = cancellation.and_then(CancelsObservation::cause) {
                return Err(HerdrSubscriptionFailure::Cancelled(cause));
            }
            let next = {
                let view = self.view.lock().unwrap_or_else(|e| e.into_inner());
                let revision = self.pane.as_ref().map_or(view.revision, |pane| {
                    view.panes.get(pane).copied().unwrap_or(view.opening)
                });
                if self.revision != Some(revision) {
                    self.revision = Some(revision);
                    match view.state {
                        SourceState::Connected => return Ok(SessionCue::Changed),
                        SourceState::Unavailable => return Ok(SessionCue::Unavailable),
                        SourceState::Connecting => {}
                    }
                }
                view.next.clone()
            };
            let mut fds = vec![next.descriptor()];
            fds.extend_from_slice(others);
            if let Some(cancel) = cancellation {
                fds.push(cancel.descriptor());
            }
            let ready = fds.ready(deadline)?;
            if let Some(cause) = cancellation.and_then(CancelsObservation::cause) {
                return Err(HerdrSubscriptionFailure::Cancelled(cause));
            }
            if ready[1..1 + others.len()].iter().any(|v| *v) {
                return Ok(SessionCue::Other);
            }
            if !ready[0] {
                return Ok(SessionCue::Deadline);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{BufRead, BufReader, Write};
    use std::os::fd::AsFd;
    use std::os::unix::net::UnixListener;
    use std::time::Duration;

    #[test]
    fn one_acknowledged_session_covers_opening_snapshot_and_fans_out_by_pane() {
        let temp = tempfile::tempdir().unwrap();
        let socket = temp.path().join("herdr.sock");
        let listener = UnixListener::bind(&socket).unwrap();
        let streams = SessionStreams::default();
        let mut one = streams.follow(&socket, Some("p1")).unwrap();
        let mut two = streams.follow(&socket, Some("p2")).unwrap();
        let deadline = || Some(Instant::now() + Duration::from_secs(3));
        assert_eq!([listener.as_fd()].ready(deadline()).unwrap(), [true]);
        let (mut wire, _) = listener.accept().unwrap();
        wire.set_read_timeout(Some(Duration::from_secs(3))).unwrap();
        let mut request = String::new();
        BufReader::new(&wire).read_line(&mut request).unwrap();
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&request).unwrap()["method"],
            "events.subscribe"
        );
        assert!(!one.available());
        writeln!(wire, r#"{{"result":{{"type":"subscription_started"}}}}"#).unwrap();
        one.connected(deadline(), None).unwrap();
        two.connected(deadline(), None).unwrap();
        assert_eq!(
            [listener.as_fd()].ready(Some(Instant::now())).unwrap(),
            [false]
        );
        // The owner already has the wire while the consumer takes its opening snapshot.
        writeln!(
            wire,
            r#"{{"event":"pane_closed","data":{{"pane":{{"pane_id":"p1"}}}}}}"#
        )
        .unwrap();
        assert_eq!(
            one.next_change(&[], deadline(), None).unwrap(),
            SessionCue::Changed
        );
        assert_eq!(
            two.next_change(&[], Some(Instant::now()), None).unwrap(),
            SessionCue::Deadline
        );
        drop(wire);
        assert_eq!(
            one.next_change(&[], deadline(), None).unwrap(),
            SessionCue::Unavailable
        );
        assert!(!one.available());
        // Healthy EOF starts one reconnect even if the listening name is unchanged.
        assert_eq!([listener.as_fd()].ready(deadline()).unwrap(), [true]);
        let (mut same, _) = listener.accept().unwrap();
        same.set_read_timeout(Some(Duration::from_secs(3))).unwrap();
        request.clear();
        BufReader::new(&same).read_line(&mut request).unwrap();
        writeln!(same, r#"{{"result":{{"type":"subscription_started"}}}}"#).unwrap();
        one.connected(deadline(), None).unwrap();
        drop(same);
        assert_eq!(
            one.next_change(&[], deadline(), None).unwrap(),
            SessionCue::Unavailable
        );
        assert_eq!(
            [listener.as_fd()]
                .ready(Some(Instant::now() + Duration::from_millis(50)))
                .unwrap(),
            [false]
        );
        // Further attempts require actual source availability.
        drop(listener);
        std::fs::remove_file(&socket).unwrap();
        let replacement = UnixListener::bind(&socket).unwrap();
        assert_eq!([replacement.as_fd()].ready(deadline()).unwrap(), [true]);
        let (mut wire, _) = replacement.accept().unwrap();
        wire.set_read_timeout(Some(Duration::from_secs(3))).unwrap();
        request.clear();
        BufReader::new(&wire).read_line(&mut request).unwrap();
        writeln!(wire, r#"{{"result":{{"type":"subscription_started"}}}}"#).unwrap();
        one.connected(deadline(), None).unwrap();
        two.connected(deadline(), None).unwrap();
    }
}
