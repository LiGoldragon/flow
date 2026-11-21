//! The Claude Harness Nexus's subscription, the source of a Claude flow's
//! rest. When the daemon's configuration names the Claude Harness Nexus's
//! subscription socket (signal-harness), Flow subscribes to it: it sends
//! Subscribe, reads the Subscribed snapshot of every living session and then
//! each Changed as it happens, and keeps each session's State and context
//! size (`ClaudeHarnessSubscription`). The rest is a transition of State: a
//! live Claude flow's session whose State becomes Flowing.Rested, whatever
//! Event brought it there, is that flow's rest, taken below the report
//! (`RestsClaudeFlows`), with the context size the Harness Nexus last told
//! for the session; nothing is recorded as a harness event. The kept State
//! is also the judgement of the flow's turn that closing it at rest asks
//! (`JudgesClosingRest::claude_turn`). While a socket is configured the
//! flow's Stop hook rests nothing (`reporting`). With no socket configured
//! the part never starts.
//!
//! A connection that ends after its Subscribed is subscribed again at once.
//! A socket that is absent, refuses the connection, or ends it before its
//! Subscribed is waited on: an inotify watch of its directory, armed before
//! each attempt, cues the next attempt when an entry is placed at its name.
//! The part ends when the Nexus stops.

use crate::RunningNexus;
use crate::event_wait::{Announcement, AnnouncesOnce, AwaitsDescriptors, NameWatch, WatchesName};
use crate::generated::startup::{
    ClaudeHarnessSubscriptionPath, DaemonConfiguration, DaemonConfigurationFault,
    DaemonConfigurationRefusal, Path as FaultPath, Reason,
};
use crate::place::{FindsFlowPlace, PlaceName, ReportsPlaceRefusal};
use crate::reporting::RestsClaudeFlows;
use crate::store::{NamesLiveFlow, ReadsFlowRows};
use crate::waking::ClaudeTurn;
use crate::{HoldsStopRequest, Stopping};
use harness::{Event as HarnessEvent, Flow, Identifiable, Observed, State};
use signal_flow::HarnessKind;
use signal_harness::Changed;
use std::collections::{BTreeMap, BTreeSet};
use std::io::{self, Read, Write};
use std::os::fd::AsFd;
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
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

/// A living session as the subscription last told it: its State, and the
/// context size it last told for it, when it told one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionStanding {
    pub state: State,
    pub context_size: Option<harness::Tokens>,
}

/// What shows a session came to rest.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RestEvidence {
    /// A Changed brought its State to Flowing.Rested from another State,
    /// whatever its Event: a turn end, an interruption, a failure, the end
    /// of its last background work, a resumption.
    Became,
    /// A snapshot shows it Flowing.Rested: it came to rest before this
    /// connection, perhaps while none was open.
    FoundRested,
}

/// A session the subscription told came to rest, with the context size it
/// last told for it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionRest {
    pub session: flow_ethos::FlowId,
    pub context_size: Option<harness::Tokens>,
    pub evidence: RestEvidence,
}

/// The Claude Harness subscription the daemon's configuration names: its
/// socket, and each living session as the subscription last told it, keyed
/// by the FlowId the Harness Nexus names it by. Its sessions are written by
/// the subscriber alone and read by the judgement of a Claude flow's turn
/// (`JudgesClosingRest::claude_turn`).
#[derive(Debug)]
pub struct ClaudeHarnessSubscription {
    pub socket: ClaudeHarnessSubscriptionPath,
    pub sessions: Mutex<BTreeMap<flow_ethos::FlowId, SessionStanding>>,
}

impl From<ClaudeHarnessSubscriptionPath> for ClaudeHarnessSubscription {
    fn from(socket: ClaudeHarnessSubscriptionPath) -> Self {
        Self {
            socket,
            sessions: Mutex::new(BTreeMap::new()),
        }
    }
}

/// The subscription's judgement of a session's turn.
pub trait JudgesHarnessTurns {
    /// Flowing.Rested is Rested, Flowing.Waiting ended with work in
    /// flight, and every other State, and a session the subscription has
    /// not told or no connection holds now, Running.
    fn turn_of(&self, session: &flow_ethos::FlowId) -> ClaudeTurn;
}

impl JudgesHarnessTurns for ClaudeHarnessSubscription {
    fn turn_of(&self, session: &flow_ethos::FlowId) -> ClaudeTurn {
        match self
            .sessions
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(session)
            .map(|standing| &standing.state)
        {
            Some(State::Flowing(Flow::Rested)) => ClaudeTurn::Rested,
            Some(State::Flowing(Flow::Waiting)) => ClaudeTurn::EndedWithWorkInFlight,
            _ => ClaudeTurn::Running,
        }
    }
}

pub trait KeepsHarnessSessions {
    /// Takes one observation into the sessions and answers the sessions it
    /// shows came to rest. A snapshot replaces every session with its own,
    /// each Rested one found rested. A Changed sets its session's State, a
    /// `ContextSize` its context size too, and an `Ended` drops the
    /// session; its session became rested when its State is Flowing.Rested
    /// and was not before. A Changed that leaves a session Rested shows no
    /// rest.
    fn keep(&self, observation: &HarnessObservation) -> Vec<SessionRest>;
    /// Forgets every session: once a connection ends nothing is known of
    /// them until the next snapshot.
    fn forget(&self);
}

impl KeepsHarnessSessions for ClaudeHarnessSubscription {
    fn keep(&self, observation: &HarnessObservation) -> Vec<SessionRest> {
        let mut sessions = self
            .sessions
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        match observation {
            HarnessObservation::Snapshot(observed) => {
                *sessions = observed
                    .iter()
                    .map(|session| {
                        (
                            session.flow_id.clone(),
                            SessionStanding {
                                state: session.state.clone(),
                                context_size: Some(session.tokens),
                            },
                        )
                    })
                    .collect();
                observed
                    .iter()
                    .filter(|session| session.state == State::Flowing(Flow::Rested))
                    .map(|session| SessionRest {
                        session: session.flow_id.clone(),
                        context_size: Some(session.tokens),
                        evidence: RestEvidence::FoundRested,
                    })
                    .collect()
            }
            HarnessObservation::Changed(Changed {
                flow_id,
                event: HarnessEvent::Ended(_),
                ..
            }) => {
                sessions.remove(flow_id);
                vec![]
            }
            HarnessObservation::Changed(Changed {
                flow_id,
                event,
                state,
            }) => {
                let standing = sessions
                    .entry(flow_id.clone())
                    .or_insert_with(|| SessionStanding {
                        state: State::Dry,
                        context_size: None,
                    });
                let was_rested = standing.state == State::Flowing(Flow::Rested);
                standing.state = state.clone();
                if let HarnessEvent::ContextSize(tokens) = event {
                    standing.context_size = Some(*tokens);
                }
                if !was_rested && *state == State::Flowing(Flow::Rested) {
                    vec![SessionRest {
                        session: flow_id.clone(),
                        context_size: standing.context_size,
                        evidence: RestEvidence::Became,
                    }]
                } else {
                    vec![]
                }
            }
        }
    }

    fn forget(&self) {
        self.sessions
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clear();
    }
}

/// Why a session's rest rests no flow, each logged the first time it is
/// met.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum UnheldRest {
    /// No flow has the session's FlowId, or the one that has is not a live
    /// Claude flow whose registered session the Harness Nexus names by that
    /// FlowId.
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

/// The subscriber: the Nexus it rests flows in, the subscription whose
/// sessions it keeps, its socket, and the kinds of unheld rest it has
/// logged.
pub struct ClaudeHarnessSubscriber<'nexus> {
    pub nexus: &'nexus RunningNexus,
    pub subscription: &'nexus ClaudeHarnessSubscription,
    pub socket: PathBuf,
    pub unheld: BTreeSet<UnheldRest>,
}

pub trait MapsHarnessRests {
    /// The flow's rest a session's rest stands for, or None.
    ///
    /// The flow is the live Claude flow whose FlowId is the session's and
    /// whose registered session the Harness Nexus names by that FlowId
    /// (`Identifiable`, its first six characters), while no restart holds
    /// it. The Harness Nexus has matched the session's events to that
    /// session, so its rest is the flow's own; it names no harness
    /// process, so no seat birth is asked of it.
    ///
    /// A session found rested in a snapshot is the flow's rest only when
    /// Flow's own record does not already show the flow Resting: a rest
    /// missed while no connection was open is taken, a rest already taken
    /// is not taken again.
    ///
    /// Open, None and logged once per kind: a FlowId no live Claude flow
    /// is, which every session after a /clear is
    /// (flows/900ff3/flow-side-gap.md in the primary workspace). A prompt's
    /// receipt stays the flow's own UserPromptSubmit hook's.
    fn rest_of(&mut self, rest: &SessionRest) -> Option<HarnessRest>;
}

impl MapsHarnessRests for ClaudeHarnessSubscriber<'_> {
    fn rest_of(&mut self, rest: &SessionRest) -> Option<HarnessRest> {
        use crate::generated::transitions::{AskedFlow, RecordsTransitions};
        use crate::restarting::FencesSeatEvents;
        let mapped = match self.nexus.store.flow_node(&rest.session.0) {
            Err(_) => Err(UnheldRest::FlowUnread),
            Ok(Some(node))
                if node.harness_kind == HarnessKind::Claude
                    && node.flow_lifecycle.is_live()
                    && harness::SessionId(node.session_id.0.clone()).identify() == rest.session =>
            {
                if !self.nexus.session_event_acts(&node.flow_id.0) {
                    Err(UnheldRest::Restarting)
                } else if rest.evidence == RestEvidence::FoundRested
                    && matches!(
                        self.nexus.store.state_of(AskedFlow(node.flow_id.clone())),
                        Ok(Some(meta_signal_flow::State::Resting))
                    )
                {
                    return None;
                } else {
                    Ok(HarnessRest {
                        flow_id: node.flow_id,
                        context_size: rest
                            .context_size
                            .map(|tokens| signal_flow::Tokens(tokens.0)),
                    })
                }
            }
            Ok(_) => Err(UnheldRest::UnheldSession),
        };
        match mapped {
            Ok(rest) => Some(rest),
            Err(kind) => {
                if self.unheld.insert(kind) {
                    eprintln!(
                        "flow-nexus: a Claude Harness session's rest of kind {kind:?} rests no flow; it is dropped, and this kind is logged only once"
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
    /// One observation: kept in the subscription's sessions, and each
    /// flow's rest it shows taken (`RestsClaudeFlows`), after the sessions
    /// hold it. Nothing is recorded as a harness event.
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
            self.subscription.forget();
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
        for rest in self.subscription.keep(&observation) {
            if let Some(HarnessRest {
                flow_id,
                context_size,
            }) = self.rest_of(&rest)
            {
                self.nexus.rest_claude_flow(&flow_id, context_size);
            }
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
            .clone()
            .map(ClaudeHarnessSubscription::from);
        self
    }

    fn keep_subscribing_to_claude_harness(&self) {
        let Some(subscription) = &self.claude_harness_subscription else {
            return;
        };
        ClaudeHarnessSubscriber {
            nexus: self,
            subscription,
            socket: PathBuf::from(&subscription.socket.0),
            unheld: BTreeSet::new(),
        }
        .keep_subscribed();
    }
}
