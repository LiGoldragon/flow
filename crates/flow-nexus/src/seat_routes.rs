//! Bound routes hold their session stream even with no client observer.
//! An event invalidates transport ownership; only the native source can end
//! a seat. All validated Claude process descriptors share the same wait.
use crate::caller::KnowsHerdrServers;
use crate::claude::ReadsClaudeSessions;
use crate::claude_changes::{ClaudeDirectories, ReadsClaudeChanges};
use crate::event_wait::{AnnouncesOnce, AwaitsDescriptors, OpensExitDescriptor};
use crate::herdr::LocatesNativeTranscripts;
use crate::herdr::session_stream::{ReadsSessionChanges, SessionCursor, SharesSessionStreams};
use crate::herdr::{PaneOwnership, ReadsHerdrPanes};
use crate::observe_agent::RecordsWitnessedExit;
use crate::restarting::{FencesSeatEvents, SeatWitness};
use crate::seat_observation::ReadsNativeStanding;
use crate::store::current_seat::{CurrentSeatRead, RecordsCurrentSeats};
use crate::store::metaflow_watch::WatchesMetaflows;
use crate::store::restart::RecordsRestarts;
use crate::store::{AnnouncesLaunchChanges, LaunchChanges, ManagesMetaflowState, ReadsFlowRows};
use crate::{HoldsStopRequest, RunningNexus};
use std::collections::{BTreeMap, BTreeSet};
use std::os::fd::{AsFd, OwnedFd};
use std::path::PathBuf;

struct BoundTransport {
    seat: SeatWitness,
    socket: PathBuf,
    events: SessionCursor,
    ownership: PaneOwnership,
}
pub trait ServesBoundRoutes {
    fn serve_bound_routes(&self);
}
trait WatchesNativeProcess {
    fn watch_claude_process(
        &self,
        node: &signal_flow::FlowNode,
        seat: &SeatWitness,
    ) -> Option<OwnedFd>;
}
impl WatchesNativeProcess for RunningNexus {
    fn watch_claude_process(
        &self,
        node: &signal_flow::FlowNode,
        seat: &SeatWitness,
    ) -> Option<OwnedFd> {
        use crate::generated::memory::{CurrentSeat, SeatStanding};
        if !matches!(
            self.store.current_seat(&node.flow_id.0),
            Ok(CurrentSeatRead::Known(CurrentSeat {
                seat_standing: SeatStanding::Alive(_),
                ..
            }))
        ) {
            return None;
        }
        let session = &node.session_id.0;
        let registry = &self.herdr.claude_sessions;
        let opened = registry.session_process(session)?;
        if let Some(pid) = opened {
            if let Ok(fd) = pid.exit_descriptor() {
                // Revalidate the birth after opening the descriptor. A
                // recycled numeric PID is not a witness for the old seat.
                if registry.session_of(pid).as_deref() == Some(session) {
                    return Some(fd);
                }
            }
            if registry.session_process(session) != Some(None) {
                return None;
            }
        }
        self.exited_as_witnessed_by(&node.flow_id.0, Some(seat));
        None
    }
}

impl ServesBoundRoutes for RunningNexus {
    fn serve_bound_routes(&self) {
        let changes = LaunchChanges::default();
        if let Err(error) = self.store.watch_native_rests(changes.clone()) {
            eprintln!("flow-nexus: cannot observe bound routes: {error}");
            return;
        }
        self.herdr.session_streams.announce_to(changes.clone());
        let mut directories = self
            .herdr
            .native_transcript_root(&signal_flow::HarnessKind::Claude, "")
            .ok()
            .and_then(|projects| {
                ClaudeDirectories::watch(projects, self.herdr.claude_sessions.directory.clone())
                    .map_err(|err| eprintln!("flow-nexus: Claude observation unavailable: {err}"))
                    .ok()
            });
        let mut native_seats = BTreeMap::new();
        let mut views = BTreeMap::<String, BoundTransport>::new();
        while !self.stopping.requested() {
            let Ok(changed) = changes.next_descriptor() else {
                return;
            };
            let native = directories
                .as_mut()
                .and_then(|source| source.changes().ok());
            let mut present_native = BTreeSet::new();
            let sessions = self.herdr.herdr_sessions().map(|items| {
                items
                    .into_iter()
                    .map(|item| (item.name, item.socket))
                    .collect::<BTreeMap<_, _>>()
            });
            if let Some(sessions) = &sessions {
                self.herdr
                    .session_streams
                    .retain_listed(&sessions.values().cloned().collect());
            }
            let mut present = BTreeSet::new();
            let mut processes = Vec::new();
            let Ok(seats) = self.store.list_metaflows() else {
                return;
            };
            for seat in seats {
                let flow_ethos::State::Awake(id) = seat.state else {
                    continue;
                };
                let Ok(Some(node)) = self.store.flow_node(&id.0) else {
                    continue;
                };
                if node.flow_lifecycle != signal_flow::FlowLifecycle::Active {
                    continue;
                }
                if node.harness_kind == signal_flow::HarnessKind::Claude
                    && let Some(changes) = &native
                {
                    present_native.insert(id.0.clone());
                    let identity = (
                        node.session_id.clone(),
                        self.store.seat_birth(&id.0).ok().flatten(),
                    );
                    let first = native_seats.get(&id.0) != Some(&identity);
                    if first
                        || changes.topology
                        || changes.registry
                        || changes.sessions.contains(&node.session_id.0)
                    {
                        let _turn = self.dispatch_gate.lock().unwrap_or_else(|e| e.into_inner());
                        if matches!(self.store.restart_record(&id.0), Ok(None))
                            && self.store.flow_node(&id.0).ok().flatten() == Some(node.clone())
                            && self.store.seat_birth(&id.0).ok().flatten() == identity.1
                        {
                            let valid_birth = identity.1.as_ref().is_none_or(|birth| {
                                u32::try_from(birth.process.pid.0)
                                    .ok()
                                    .and_then(|pid| self.herdr.claude_sessions.session_of(pid))
                                    .as_deref()
                                    == Some(node.session_id.0.as_str())
                            });
                            let observed = valid_birth
                                .then(|| self.read_native_standing(&node))
                                .flatten();
                            // Ending a known process goes through the witnessed
                            // exit path below; file changes convey activity.
                            if !matches!(
                                observed,
                                Some(crate::generated::memory::SeatStanding::Ended)
                            ) {
                                let _ = self.store.record_observation(&id.0, observed);
                            }
                        }
                    }
                    native_seats.insert(id.0.clone(), identity);
                }
                let Some(witness) = self.seat_witness(&id.0) else {
                    continue;
                };
                if node.harness_kind == signal_flow::HarnessKind::Claude
                    && let Some(fd) = self.watch_claude_process(&node, &witness)
                {
                    processes.push(fd);
                }
                let route = &witness.route;
                let Some(socket) = sessions
                    .as_ref()
                    .and_then(|list| list.get(&route.herdr_session_name.0))
                    .cloned()
                else {
                    continue;
                };
                if views
                    .get(&id.0)
                    .is_none_or(|v| v.seat != witness || v.socket != socket)
                {
                    let Ok(events) = self
                        .herdr
                        .session_streams
                        .follow(&socket, Some(&route.herdr_pane_id.0))
                    else {
                        continue;
                    };
                    views.insert(
                        id.0.clone(),
                        BoundTransport {
                            seat: witness,
                            socket,
                            events,
                            ownership: PaneOwnership::Unknown,
                        },
                    );
                }
                present.insert(id.0.clone());
                let view = views.get_mut(&id.0).unwrap();
                if let Some(connected) = view.events.take_transport_change() {
                    // A reused pane gives Foreign. Loss, or even a known
                    // transport absence, never writes Ended or archives a thread.
                    view.ownership = if connected {
                        self.herdr.pane_ownership(&node)
                    } else {
                        PaneOwnership::Unknown
                    };
                }
            }
            native_seats.retain(|id, _| present_native.contains(id));
            views.retain(|id, _| present.contains(id));
            let mut descriptors = vec![self.stopping.descriptor(), changed.descriptor()];
            if let Some(source) = &directories {
                descriptors.push(source.descriptor.as_fd());
            }
            descriptors.extend(processes.iter().map(AsFd::as_fd));
            if descriptors.ready(None).is_err() {
                return;
            }
        }
    }
}
