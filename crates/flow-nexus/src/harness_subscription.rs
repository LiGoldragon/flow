//! The Claude Harness Nexus's subscription, taken as reports. When the
//! daemon's configuration names the Claude Harness Nexus's subscription
//! socket (signal-harness), Flow subscribes to it: it sends Subscribe, reads
//! the Subscribed snapshot of every living session and then each Changed as
//! it happens, and turns each into the Report it stands for, which is
//! recorded and acted on as a hook's Report is (`reporting`). With no socket
//! configured the part never starts.
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
use crate::reporting::RecordsReports;
use crate::store::{NamesLiveFlow, ReadsFlowRows};
use crate::waking::ReadsClaudeDelivery;
use crate::{HoldsStopRequest, Stopping};
use harness::{Event as HarnessEvent, Flow, Observed, State};
use signal_flow::{HarnessKind, Report_Data};
use signal_harness::Changed;
use std::collections::BTreeSet;
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

/// What the subscription tells: a session as the snapshot shows it, or a
/// change as it happened.
#[derive(Debug, Clone)]
pub enum HarnessObservation {
    Standing(Observed),
    Changed(Changed),
}

/// Each kind of observation that stands for no Report yet, logged the
/// first time it is met.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum UnmappedObservation {
    Standing,
    Started,
    Resumed,
    PromptSubmitted,
    ToolUsed,
    PermissionAsked,
    Responded,
    InFlight,
    WorkStarted,
    WorkEnded,
    ContextSize,
    Compacted,
    Ended,
    /// A turn's end with background work still in flight: Waiting, not
    /// Rested.
    TurnEndUnrested,
    /// A rest of a session no live Claude flow's registered session begins
    /// with.
    UnheldSession,
    /// A rest of a session more than one live Claude flow's registered
    /// session begins with.
    SeveralFlows,
    /// The flows could not be read from Memory.
    FlowsUnread,
    /// The flow's session has no transcript Flow can name.
    TranscriptUnnamed,
}

/// The subscriber: the socket it subscribes to, the Nexus its reports are
/// recorded in, and the kinds of observation it has logged unmapped.
pub struct ClaudeHarnessSubscriber<'nexus> {
    pub nexus: &'nexus RunningNexus,
    pub socket: PathBuf,
    pub unmapped: BTreeSet<UnmappedObservation>,
}

pub trait MapsHarnessObservations {
    /// The Report an observation stands for, or None.
    ///
    /// Filled for one case only: a Changed whose event is `TurnEnded`,
    /// `Interrupted` or `Failed` and whose State is `Flowing.Rested`, for
    /// the one live Claude flow whose registered session id begins with the
    /// Changed's FlowId, is that flow's `Stopped`, naming its session and
    /// the transcript `correlated_stop` compares against, with no
    /// background work and no last message.
    ///
    /// Open, each None and logged once per kind, until a harness Event
    /// carries what Flow's effects key on (flows/900ff3/flow-side-gap.md in
    /// the primary workspace): the snapshot's sessions; `PromptSubmitted`,
    /// which lacks the prompt's digest and session a hand-over's receipt
    /// needs; `Started` and `Ended`, which lack the session Flow rebinds to
    /// or ends on, and a session after a /clear, which the Harness Nexus
    /// names by a FlowId Flow holds for no flow; `ToolUsed`, which lacks
    /// the tool's name; and every event Flow's Report has no place for.
    fn report_for(&mut self, observation: &HarnessObservation) -> Option<Report_Data>;
}

impl MapsHarnessObservations for ClaudeHarnessSubscriber<'_> {
    fn report_for(&mut self, observation: &HarnessObservation) -> Option<Report_Data> {
        let unmapped = |kind| -> Result<Report_Data, UnmappedObservation> { Err(kind) };
        let mapped = match observation {
            HarnessObservation::Standing(_) => unmapped(UnmappedObservation::Standing),
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
                    match self.nexus.store.flow_nodes() {
                        Err(_) => unmapped(UnmappedObservation::FlowsUnread),
                        Ok(nodes) => {
                            let held = nodes
                                .into_iter()
                                .filter(|node| {
                                    node.harness_kind == HarnessKind::Claude
                                        && node.flow_lifecycle.is_live()
                                        && !flow_id.0.is_empty()
                                        && node.session_id.0.starts_with(&flow_id.0)
                                })
                                .collect::<Vec<_>>();
                            match held.as_slice() {
                                [] => unmapped(UnmappedObservation::UnheldSession),
                                [node] => match self.nexus.claude_transcript(node) {
                                    None => unmapped(UnmappedObservation::TranscriptUnnamed),
                                    Some(transcript) => Ok(Report_Data {
                                        flow_id: node.flow_id.clone(),
                                        process_option: None,
                                        event: signal_flow::Event::Stopped(
                                            signal_flow::NativeStop {
                                                native_session_id_option: Some(
                                                    signal_flow::NativeSessionId(
                                                        node.session_id.0.clone(),
                                                    ),
                                                ),
                                                transcript_path_option: Some(
                                                    signal_flow::TranscriptPath(
                                                        transcript.to_string_lossy().into_owned(),
                                                    ),
                                                ),
                                                last_assistant_message_option:
                                                    signal_flow::LastAssistantMessageOption(None),
                                                background_work: signal_flow::BackgroundWork(
                                                    Vec::new(),
                                                ),
                                            },
                                        ),
                                    }),
                                },
                                _ => unmapped(UnmappedObservation::SeveralFlows),
                            }
                        }
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
                HarnessEvent::ContextSize(_) => unmapped(UnmappedObservation::ContextSize),
                HarnessEvent::Compacted(_) => unmapped(UnmappedObservation::Compacted),
                HarnessEvent::Ended(_) => unmapped(UnmappedObservation::Ended),
            },
        };
        match mapped {
            Ok(report) => Some(report),
            Err(kind) => {
                if self.unmapped.insert(kind) {
                    eprintln!(
                        "flow-nexus: a Claude Harness observation of kind {kind:?} stands for no Report; it is dropped, and this kind is logged only once"
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
    /// One observation, taken as the Report it stands for: recorded, then
    /// acted on as a hook's Report is.
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
                    for session in sessions {
                        self.take(HarnessObservation::Standing(session));
                    }
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
        let Some(report) = self.report_for(&observation) else {
            return;
        };
        let flow_id = report.flow_id.clone();
        let event = report.event.clone();
        match self.nexus.report(report) {
            signal_flow::Output::Reported => self.nexus.after_report(&flow_id, &event),
            refused => eprintln!(
                "flow-nexus: the Claude Harness report for {flow_id} was not recorded: {refused:?}",
                flow_id = flow_id.0
            ),
        }
    }
}

/// The part the daemon runs: the subscription its configuration names, or
/// nothing.
pub trait SubscribesToClaudeHarness {
    /// Returns at once when no socket is configured; otherwise holds the
    /// subscription until the Nexus stops.
    fn keep_subscribing_to_claude_harness(&self, configuration: &DaemonConfiguration);
}

impl SubscribesToClaudeHarness for RunningNexus {
    fn keep_subscribing_to_claude_harness(&self, configuration: &DaemonConfiguration) {
        let Some(socket) = &configuration.claude_harness_subscription_path_option else {
            return;
        };
        ClaudeHarnessSubscriber {
            nexus: self,
            socket: PathBuf::from(&socket.0),
            unmapped: BTreeSet::new(),
        }
        .keep_subscribed();
    }
}
