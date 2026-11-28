//! Bound routes hold their session stream even with no client observer.
//! An event invalidates transport ownership. A Claude seat's state and its
//! end are its Harness Nexus's alone (`harness_subscription`): nothing here
//! observes or ends one.
use crate::caller::KnowsHerdrServers;
use crate::claude_changes::{ClaudeDirectories, ReadsClaudeChanges};
use crate::event_wait::{AnnouncesOnce, AwaitsDescriptors, OpensExitDescriptor};
use crate::herdr::LocatesNativeTranscripts;
use crate::herdr::session_stream::{ReadsSessionChanges, SessionCursor, SharesSessionStreams};
use crate::herdr::{PaneOwnership, ReadsHerdrPanes, ReadsHerdrRoster};
use crate::restarting::{FencesSeatEvents, SeatWitness};
use crate::store::metaflow_watch::WatchesMetaflows;
use crate::store::recipient_retries::RetriesRecipients;
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
    writable: bool,
}
pub trait ServesBoundRoutes {
    fn serve_bound_routes(&self);
}
pub(crate) trait WatchesBoundPeers {
    fn watch_bound_peer(
        &self,
        address: &signal_flow::Address,
        id: &signal_flow::FlowId,
        process: &signal_flow::Process,
        kernel: &impl crate::restarting::ReadsProcessStatus,
    ) -> Option<OwnedFd>;
}
impl WatchesBoundPeers for RunningNexus {
    fn watch_bound_peer(
        &self,
        address: &signal_flow::Address,
        id: &signal_flow::FlowId,
        process: &signal_flow::Process,
        kernel: &impl crate::restarting::ReadsProcessStatus,
    ) -> Option<OwnedFd> {
        use crate::restarting::{ExitProof, ProvesExit};
        use crate::store::metaflow::EndsOrdinaryBinding;
        match process.exit_proof(kernel) {
            ExitProof::Running => {
                let fd = u32::try_from(process.pid.0).ok()?.exit_descriptor().ok();
                match process.exit_proof(kernel) {
                    ExitProof::Running => return fd,
                    ExitProof::Unproven(_) => return None,
                    ExitProof::Exited(_) => {}
                }
            }
            ExitProof::Unproven(_) => return None,
            ExitProof::Exited(_) => {}
        }
        let _turn = self.dispatch_gate.lock().unwrap_or_else(|e| e.into_inner());
        if let Err(error) = self.store.end_bound_peer(address, id, process) {
            eprintln!("flow-nexus: could not settle ended ordinary binding: {error}");
        }
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
        use crate::event_wait::{NameWatch, WatchesName};
        use std::sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        };
        let projects = self
            .herdr
            .native_transcript_root(&signal_flow::HarnessKind::Claude, "")
            .ok();
        let registry = self.herdr.claude_sessions.directory.clone();
        let roots_changed = Arc::new(AtomicBool::new(true));
        // These parent/name witnesses are independent of the directory reader:
        // failure of that reader can never leave a bad fd in the wait set.
        let _root_watches: Vec<_> = projects
            .iter()
            .chain(std::iter::once(&registry))
            .filter_map(|path| {
                let ready = roots_changed.clone();
                let events = changes.clone();
                NameWatch::arm(
                    path,
                    Box::new(move || {
                        ready.store(true, Ordering::Release);
                        events.announce();
                        true
                    }),
                )
                .map_err(|e| eprintln!("flow-nexus: cannot watch native source name: {e}"))
                .ok()
            })
            .collect();
        let mut directories = None;
        let mut views = BTreeMap::<String, BoundTransport>::new();
        while !self.stopping.requested() {
            let Ok(changed) = changes.next_descriptor() else {
                return;
            };
            let reopened = roots_changed.swap(false, Ordering::AcqRel);
            if reopened {
                directories = projects.as_ref().and_then(|path| {
                    ClaudeDirectories::watch(path.clone(), registry.clone())
                        .map_err(|e| eprintln!("flow-nexus: cannot arm Claude directories: {e}"))
                        .ok()
                });
            }
            // A Claude seat's state is its Harness Nexus's to tell
            // (`harness_subscription`); a native change only wakes this
            // loop to follow the bound routes again.
            if let Some(Err(error)) = directories.as_mut().map(ReadsClaudeChanges::changes) {
                eprintln!("flow-nexus: lost native change reader: {error}");
                directories = None;
            }
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
                let node = match self.store.flow_node(&id.0) {
                    Ok(Some(node)) => node,
                    Ok(None) => {
                        if let Ok(Some(peer)) = self.store.bound_process(&seat.address)
                            && let Some(fd) = self.watch_bound_peer(
                                &seat.address,
                                &id,
                                &peer,
                                &crate::restarting::ProcFs,
                            )
                        {
                            processes.push(fd);
                        }
                        continue;
                    }
                    Err(_) => continue,
                };
                if node.flow_lifecycle != signal_flow::FlowLifecycle::Active {
                    continue;
                }
                let Some(witness) = self.seat_witness(&id.0) else {
                    continue;
                };
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
                            writable: false,
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
                    let writable = view.ownership == PaneOwnership::Owned
                        && self.herdr.route_is_available(&node);
                    if writable && !view.writable {
                        self.route_ready(&seat.address, &id.0);
                    }
                    view.writable = writable;
                }
            }
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
