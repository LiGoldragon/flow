//! The Claude Harness Nexus's subscription, the source of a Claude flow's
//! rest. When the daemon's configuration names the Claude Harness Nexus's
//! subscription socket (signal-harness), Flow subscribes to it: it sends
//! Subscribe, reads the Subscribed snapshot of every living session and then
//! each Changed as it happens. A rested turn end of a live Claude flow's
//! session is that flow's rest, taken below the report
//! (`RestsClaudeFlows`), with the context size the Harness Nexus last told
//! for the session; nothing is recorded as a harness event. While a socket
//! is configured the flow's Stop hook rests nothing (`reporting`). With no
//! socket configured the part never starts.
//!
//! A connection that ends after its Subscribed is subscribed again at once.
//! A socket that is absent, refuses the connection, or ends it before its
//! Subscribed is waited on: an inotify watch of its directory, armed before
//! each attempt, cues the next attempt when an entry is placed at its name.
//! The part ends when the Nexus stops.

use crate::RunningNexus;
use crate::event_wait::{Announcement, AnnouncesOnce, AwaitsDescriptors, NameWatch, WatchesName};
use crate::generated::startup::{
    DaemonConfiguration, DaemonConfigurationFault, DaemonConfigurationRefusal, Path as FaultPath,
    Reason,
};
use crate::place::{FindsFlowPlace, PlaceName, ReportsPlaceRefusal};
use crate::reporting::RestsClaudeFlows;
use crate::store::{NamesLiveFlow, ReadsFlowRows};
use crate::{HoldsStopRequest, Stopping};
use harness::{Event as HarnessEvent, Flow, Identifiable, Observed, State};
use signal_flow::HarnessKind;
use signal_harness::Changed;
use std::collections::{BTreeMap, BTreeSet};
use std::io::{self, Read, Write};
use std::os::fd::AsFd;
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use translator::place_file::{ConfigurationHome, LocatesConfigurationHome, LocatesPlaceFile};

/// The daemon's configuration file, beside its place file.
pub trait LocatesDaemonConfiguration {
    /// `<configuration home>/nexus/<place-name>/flow-nexus-daemon.datom`.
    const FILE_NAME: &'static str = "flow-nexus-daemon.datom";
    /// The configuration the place's file holds under `home`; every option
    /// unset when there is no file.
    fn daemon_configuration_under(
        &self,
        home: &ConfigurationHome,
    ) -> Result<DaemonConfiguration, DaemonConfigurationRefusal>;
    /// [`Self::daemon_configuration_under`] this process's configuration
    /// home.
    fn daemon_configuration(&self) -> Result<DaemonConfiguration, DaemonConfigurationRefusal>;
}

impl LocatesDaemonConfiguration for PlaceName {
    fn daemon_configuration_under(
        &self,
        home: &ConfigurationHome,
    ) -> Result<DaemonConfiguration, DaemonConfigurationRefusal> {
        let place_file = home
            .place_file(&Self::nexus_name(), self)
            .map_err(|refusal| {
                DaemonConfigurationRefusal::Invalid(home.directory.fault(refusal.report()))
            })?;
        place_file
            .path
            .with_file_name(Self::FILE_NAME)
            .read_daemon_configuration()
    }

    fn daemon_configuration(&self) -> Result<DaemonConfiguration, DaemonConfigurationRefusal> {
        let home = ConfigurationHome::of_process().map_err(|refusal| {
            DaemonConfigurationRefusal::Unreadable(DaemonConfigurationFault {
                path: FaultPath(String::new()),
                reason: Reason(refusal.report()),
            })
        })?;
        self.daemon_configuration_under(&home)
    }
}

/// A fault of the configuration file at a path.
trait FaultsDaemonConfiguration {
    fn fault(&self, reason: impl ToString) -> DaemonConfigurationFault;
}

impl FaultsDaemonConfiguration for Path {
    fn fault(&self, reason: impl ToString) -> DaemonConfigurationFault {
        DaemonConfigurationFault {
            path: FaultPath(self.to_string_lossy().into_owned()),
            reason: Reason(reason.to_string()),
        }
    }
}

/// One datom file read as the daemon's configuration.
pub trait ReadsDaemonConfiguration {
    /// A missing file is every option unset; a file that cannot be read,
    /// holds no `DaemonConfiguration`, or names a socket by a relative path
    /// is refused, naming the file.
    fn read_daemon_configuration(&self) -> Result<DaemonConfiguration, DaemonConfigurationRefusal>;
}

impl ReadsDaemonConfiguration for Path {
    fn read_daemon_configuration(&self) -> Result<DaemonConfiguration, DaemonConfigurationRefusal> {
        use datom_codec::{Actualizing, Budget, Potential};
        let source = match std::fs::read_to_string(self) {
            Ok(source) => source,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                return Ok(DaemonConfiguration {
                    claude_harness_subscription_path_option: None,
                });
            }
            Err(error) => {
                return Err(DaemonConfigurationRefusal::Unreadable(self.fault(error)));
            }
        };
        let mut budget = Budget {
            remaining: 65_536,
            reader: protos::ReaderBudget::try_from(65_536).expect("a constant reader budget"),
            depth: 0,
            maximum_depth: 64,
        };
        let configuration = Potential::<DaemonConfiguration>::from(source.trim().to_owned())
            .actualize(&mut budget)
            .map_err(|error| {
                DaemonConfigurationRefusal::Invalid(self.fault(format!("{error:?}")))
            })?;
        match &configuration.claude_harness_subscription_path_option {
            Some(socket) if !Path::new(&socket.0).is_absolute() => {
                Err(DaemonConfigurationRefusal::Invalid(self.fault(format!(
                    "the Claude Harness subscription socket {} is not an absolute path",
                    socket.0
                ))))
            }
            _ => Ok(configuration),
        }
    }
}

/// A refusal as the daemon reports it: its datom, naming the file.
pub trait ReportsDaemonConfigurationRefusal {
    fn report(&self) -> String;
}

impl ReportsDaemonConfigurationRefusal for DaemonConfigurationRefusal {
    fn report(&self) -> String {
        use datom_codec::Datomizable;
        use protos::{Compactable, Protosizable};
        self.datomize(vec![]).protosize().compact()
    }
}

/// What the subscription tells: every living session as its snapshot
/// shows it, or a change as it happened.
#[derive(Debug, Clone)]
pub enum HarnessObservation {
    Snapshot(Vec<Observed>),
    Changed(Changed),
}

/// Each kind of observation that rests no flow, logged the first time it is
/// met.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum UnmappedObservation {
    /// The snapshot of the living sessions; only its sizes are kept.
    Snapshot,
    Started,
    Resumed,
    PromptSubmitted,
    ToolUsed,
    PermissionAsked,
    Responded,
    InFlight,
    WorkStarted,
    WorkEnded,
    Compacted,
    Ended,
    /// A turn's end with background work still in flight: Waiting, not
    /// Rested.
    TurnEndUnrested,
    /// A rest of a session no live Claude flow is: no flow has its FlowId,
    /// or the one that has is not a live Claude flow whose registered
    /// session the Harness Nexus names by that FlowId.
    UnheldSession,
    /// The flow could not be read from Memory.
    FlowUnread,
    /// A restart holds the flow.
    Restarting,
}

/// A Claude flow's rest the subscription told: the flow, and the context
/// size the Harness Nexus last told for its session, when it told one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HarnessRest {
    pub flow_id: signal_flow::FlowId,
    pub context_size: Option<signal_flow::Tokens>,
}

/// The subscriber: the socket it subscribes to, the Nexus it rests flows
/// in, the context size the subscription last told for each session, and
/// the kinds of observation it has logged unmapped.
pub struct ClaudeHarnessSubscriber<'nexus> {
    pub nexus: &'nexus RunningNexus,
    pub socket: PathBuf,
    pub context_sizes: BTreeMap<flow_ethos::FlowId, harness::Tokens>,
    pub unmapped: BTreeSet<UnmappedObservation>,
}

pub trait MapsHarnessObservations {
    /// Keeps the context size each session was last told at: a snapshot
    /// replaces them all with its sessions' tokens, a `ContextSize` change
    /// sets its session's, and an `Ended` one drops it.
    fn keep_context_size(&mut self, observation: &HarnessObservation);
    /// The rest an observation stands for, or None.
    ///
    /// One case only: a Changed whose event is `TurnEnded`, `Interrupted`
    /// or `Failed` and whose State is `Flowing.Rested`, of the live Claude
    /// flow whose FlowId is the Changed's and whose registered session the
    /// Harness Nexus names by that FlowId (`Identifiable`, its first six
    /// characters), while no restart holds the flow. The Harness Nexus has
    /// matched the turn to that session, so its turn end is the flow's own;
    /// it names no harness process, so no seat birth is asked of it. The
    /// context size is the one last kept for the session.
    ///
    /// Open, each None and logged once per kind: every other event, a turn
    /// end still Waiting on background work, and a FlowId no live Claude
    /// flow is, which every session after a /clear is
    /// (flows/900ff3/flow-side-gap.md in the primary workspace). A prompt's
    /// receipt stays the flow's own UserPromptSubmit hook's.
    fn rest_for(&mut self, observation: &HarnessObservation) -> Option<HarnessRest>;
}

impl MapsHarnessObservations for ClaudeHarnessSubscriber<'_> {
    fn keep_context_size(&mut self, observation: &HarnessObservation) {
        match observation {
            HarnessObservation::Snapshot(sessions) => {
                self.context_sizes = sessions
                    .iter()
                    .map(|session| (session.flow_id.clone(), session.tokens))
                    .collect();
            }
            HarnessObservation::Changed(Changed {
                flow_id,
                event: HarnessEvent::ContextSize(tokens),
                ..
            }) => {
                self.context_sizes.insert(flow_id.clone(), *tokens);
            }
            HarnessObservation::Changed(Changed {
                flow_id,
                event: HarnessEvent::Ended(_),
                ..
            }) => {
                self.context_sizes.remove(flow_id);
            }
            HarnessObservation::Changed(_) => {}
        }
    }

    fn rest_for(&mut self, observation: &HarnessObservation) -> Option<HarnessRest> {
        use crate::restarting::FencesSeatEvents;
        let unmapped = |kind| -> Result<HarnessRest, UnmappedObservation> { Err(kind) };
        let mapped = match observation {
            HarnessObservation::Snapshot(_) => unmapped(UnmappedObservation::Snapshot),
            HarnessObservation::Changed(Changed {
                flow_id,
                event,
                state,
            }) => match event {
                HarnessEvent::TurnEnded | HarnessEvent::Interrupted | HarnessEvent::Failed(_)
                    if *state != State::Flowing(Flow::Rested) =>
                {
                    unmapped(UnmappedObservation::TurnEndUnrested)
                }
                HarnessEvent::TurnEnded | HarnessEvent::Interrupted | HarnessEvent::Failed(_) => {
                    match self.nexus.store.flow_node(&flow_id.0) {
                        Err(_) => unmapped(UnmappedObservation::FlowUnread),
                        Ok(Some(node))
                            if node.harness_kind == HarnessKind::Claude
                                && node.flow_lifecycle.is_live()
                                && harness::SessionId(node.session_id.0.clone()).identify()
                                    == *flow_id =>
                        {
                            if self.nexus.session_event_acts(&node.flow_id.0) {
                                Ok(HarnessRest {
                                    flow_id: node.flow_id,
                                    context_size: self
                                        .context_sizes
                                        .get(flow_id)
                                        .map(|tokens| signal_flow::Tokens(tokens.0)),
                                })
                            } else {
                                unmapped(UnmappedObservation::Restarting)
                            }
                        }
                        Ok(_) => unmapped(UnmappedObservation::UnheldSession),
                    }
                }
                HarnessEvent::Started => unmapped(UnmappedObservation::Started),
                HarnessEvent::Resumed => unmapped(UnmappedObservation::Resumed),
                HarnessEvent::PromptSubmitted => unmapped(UnmappedObservation::PromptSubmitted),
                HarnessEvent::ToolUsed => unmapped(UnmappedObservation::ToolUsed),
                HarnessEvent::PermissionAsked => unmapped(UnmappedObservation::PermissionAsked),
                HarnessEvent::Responded(_) => unmapped(UnmappedObservation::Responded),
                HarnessEvent::InFlight(_) => unmapped(UnmappedObservation::InFlight),
                HarnessEvent::WorkStarted(_) => unmapped(UnmappedObservation::WorkStarted),
                HarnessEvent::WorkEnded(_) => unmapped(UnmappedObservation::WorkEnded),
                // Kept for the session's next rest (`keep_context_size`).
                HarnessEvent::ContextSize(_) => return None,
                HarnessEvent::Compacted(_) => unmapped(UnmappedObservation::Compacted),
                HarnessEvent::Ended(_) => unmapped(UnmappedObservation::Ended),
            },
        };
        match mapped {
            Ok(rest) => Some(rest),
            Err(kind) => {
                if self.unmapped.insert(kind) {
                    eprintln!(
                        "flow-nexus: a Claude Harness observation of kind {kind:?} rests no flow; it is dropped, and this kind is logged only once"
                    );
                }
                None
            }
        }
    }
}

/// How one connection to the subscription ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubscriptionEnding {
    /// It ended after its Subscribed: subscribe again at once.
    Dropped,
    /// It was refused, or ended before its Subscribed, or sent what this
    /// contract does not read: wait for the socket to be placed anew.
    Unanswered,
    /// The Nexus is stopping.
    Stopping,
}

/// The subscription's frames on a stream: a little-endian u32 length, then
/// the rkyv archive of one signal-harness value.
pub trait FramesHarnessSubscription {
    /// The most one frame may hold.
    const FRAME_LIMIT: usize = 64 * 1024 * 1024;
    fn send_subscribe(&mut self) -> io::Result<()>;
    /// None is a stream that ended before a frame began.
    fn read_output(&mut self) -> io::Result<Option<signal_harness::Output>>;
}

impl FramesHarnessSubscription for UnixStream {
    fn send_subscribe(&mut self) -> io::Result<()> {
        let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&signal_harness::Input::Subscribe)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
        let length =
            u32::try_from(bytes.len()).map_err(|_| io::Error::from(io::ErrorKind::InvalidInput))?;
        self.write_all(&length.to_le_bytes())?;
        self.write_all(&bytes)?;
        self.flush()
    }

    fn read_output(&mut self) -> io::Result<Option<signal_harness::Output>> {
        let mut length = [0_u8; 4];
        match self.read_exact(&mut length) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(error) => return Err(error),
        }
        let length = u32::from_le_bytes(length) as usize;
        if length > Self::FRAME_LIMIT {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("a frame of {length} bytes"),
            ));
        }
        let mut bytes = vec![0_u8; length];
        self.read_exact(&mut bytes)?;
        rkyv::from_bytes::<signal_harness::Output, rkyv::rancor::Error>(&bytes)
            .map(Some)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
    }
}

/// A watch of the socket's name, armed before an attempt so that a socket
/// placed after the attempt is not missed.
pub struct SocketPlacement {
    appeared: Arc<Announcement>,
    _watch: NameWatch,
}

pub trait AwaitsSocketPlacement: Sized {
    fn arm(socket: &Path) -> io::Result<Self>;
    /// Blocks until an entry is placed at the socket's name, true, or the
    /// Nexus stops, false.
    fn placed_before_stop(&self, stopping: &Stopping) -> bool;
}

impl AwaitsSocketPlacement for SocketPlacement {
    fn arm(socket: &Path) -> io::Result<Self> {
        let appeared = Arc::new(Announcement::open()?);
        let cue = appeared.clone();
        let watch = NameWatch::arm(
            socket,
            Box::new(move || {
                cue.announce();
                false
            }),
        )?;
        Ok(Self {
            appeared,
            _watch: watch,
        })
    }

    fn placed_before_stop(&self, stopping: &Stopping) -> bool {
        match [self.appeared.descriptor(), stopping.descriptor()].ready(None) {
            Ok(ready) => !ready[1] && ready[0],
            Err(_) => false,
        }
    }
}

pub trait HoldsHarnessSubscription {
    /// Subscribes, and subscribes again each time a connection ends, until
    /// the Nexus stops.
    fn keep_subscribed(&mut self);
    /// The socket's name watched; None, logged, when it cannot be.
    fn placement(&self) -> Option<SocketPlacement>;
    /// One connection, from Subscribe to its end.
    fn subscribe_once(&mut self) -> SubscriptionEnding;
    /// One observation: its context size kept, and the rest it stands for
    /// taken (`RestsClaudeFlows`). Nothing is recorded as a harness event.
    fn take(&mut self, observation: HarnessObservation);
}

impl HoldsHarnessSubscription for ClaudeHarnessSubscriber<'_> {
    fn keep_subscribed(&mut self) {
        let nexus = self.nexus;
        // Each attempt is made with a watch armed before it, so a socket
        // placed after an unanswered attempt cues the next.
        let Some(mut placement) = self.placement() else {
            return;
        };
        let mut ending = self.subscribe_once();
        loop {
            match ending {
                SubscriptionEnding::Stopping => return,
                SubscriptionEnding::Dropped => {}
                SubscriptionEnding::Unanswered => {
                    if !placement.placed_before_stop(&nexus.stopping) {
                        return;
                    }
                }
            }
            let Some(armed) = self.placement() else {
                return;
            };
            placement = armed;
            ending = self.subscribe_once();
        }
    }

    fn placement(&self) -> Option<SocketPlacement> {
        SocketPlacement::arm(&self.socket)
            .inspect_err(|error| {
                eprintln!(
                    "flow-nexus: the Claude Harness subscription socket {} cannot be watched: {error}; the subscription ends",
                    self.socket.display()
                )
            })
            .ok()
    }

    fn subscribe_once(&mut self) -> SubscriptionEnding {
        let nexus = self.nexus;
        if nexus.stopping.requested() {
            return SubscriptionEnding::Stopping;
        }
        let mut stream = match UnixStream::connect(&self.socket) {
            Ok(stream) => stream,
            Err(error) => {
                eprintln!(
                    "flow-nexus: the Claude Harness subscription socket {} is not answering ({error}); it is waited for",
                    self.socket.display()
                );
                return SubscriptionEnding::Unanswered;
            }
        };
        if let Err(error) = stream.send_subscribe() {
            eprintln!(
                "flow-nexus: Subscribe was not sent to {}: {error}; the socket is waited for",
                self.socket.display()
            );
            return SubscriptionEnding::Unanswered;
        }
        let mut answered = false;
        let ended = |answered: bool| {
            if answered {
                SubscriptionEnding::Dropped
            } else {
                SubscriptionEnding::Unanswered
            }
        };
        loop {
            match [stream.as_fd(), nexus.stopping.descriptor()].ready(None) {
                Ok(ready) if ready[1] => return SubscriptionEnding::Stopping,
                Ok(_) => {}
                Err(_) => return ended(answered),
            }
            match stream.read_output() {
                Ok(Some(signal_harness::Output::Subscribed(sessions))) => {
                    if !answered {
                        eprintln!(
                            "flow-nexus: subscribed to the Claude Harness Nexus at {}, {} sessions living",
                            self.socket.display(),
                            sessions.len()
                        );
                    }
                    answered = true;
                    self.take(HarnessObservation::Snapshot(sessions));
                }
                Ok(Some(signal_harness::Output::Changed(changed))) => {
                    self.take(HarnessObservation::Changed(changed));
                }
                Ok(None) => {
                    eprintln!(
                        "flow-nexus: the Claude Harness subscription at {} ended",
                        self.socket.display()
                    );
                    return ended(answered);
                }
                Err(error) if error.kind() == io::ErrorKind::InvalidData => {
                    eprintln!(
                        "flow-nexus: the Claude Harness subscription at {} sent a frame this contract does not read ({error}); the socket is waited for",
                        self.socket.display()
                    );
                    return SubscriptionEnding::Unanswered;
                }
                Err(error) => {
                    eprintln!(
                        "flow-nexus: the Claude Harness subscription at {} failed: {error}",
                        self.socket.display()
                    );
                    return ended(answered);
                }
            }
        }
    }

    fn take(&mut self, observation: HarnessObservation) {
        self.keep_context_size(&observation);
        if let Some(HarnessRest {
            flow_id,
            context_size,
        }) = self.rest_for(&observation)
        {
            self.nexus.rest_claude_flow(&flow_id, context_size);
        }
    }
}

/// The part the daemon runs: the subscription its configuration names, or
/// nothing.
pub trait SubscribesToClaudeHarness {
    /// The Nexus as the daemon's configuration sets it: the subscription
    /// socket it names, which makes the subscription the one source of a
    /// Claude flow's rest (`RunningNexus::claude_harness_subscription`).
    fn configured_by(self, configuration: &DaemonConfiguration) -> Self;
    /// Returns at once when no socket is configured; otherwise holds the
    /// subscription until the Nexus stops.
    fn keep_subscribing_to_claude_harness(&self);
}

impl SubscribesToClaudeHarness for RunningNexus {
    fn configured_by(mut self, configuration: &DaemonConfiguration) -> Self {
        self.claude_harness_subscription = configuration
            .claude_harness_subscription_path_option
            .clone();
        self
    }

    fn keep_subscribing_to_claude_harness(&self) {
        let Some(socket) = &self.claude_harness_subscription else {
            return;
        };
        ClaudeHarnessSubscriber {
            nexus: self,
            socket: PathBuf::from(&socket.0),
            context_sizes: BTreeMap::new(),
            unmapped: BTreeSet::new(),
        }
        .keep_subscribed();
    }
}
