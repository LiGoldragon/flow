//! The Harness Nexuses' witness of each flow, held beside Flow's own
//! sources and deciding nothing. When the daemon's configuration names
//! Harness Nexus subscription sockets (signal-harness), Flow dials each one,
//! sends Subscribe once per connection, and reads its Subscribed snapshot of
//! every session as observed now and then each Changed as it happens,
//! holding each flow's harness state as the last of them
//! (`HarnessWitnesses`). A socket's Subscribed replaces every witness that
//! socket gave; a Changed updates one, its tokens too on ContextSize; a flow
//! told Ended stays until that socket's next Subscribed.
//!
//! When a Changed moves a flow's harness state to a new value its comparison
//! with Flow's own state falls due GraceSeconds later; at that moment, if
//! the harness state is still that value and Flow records a state for the
//! flow, the two are compared (`JudgesHarnessAgreement`) and a disagreement
//! is kept in a ring of DisagreementCapacity entries, the oldest dropped. The
//! comparisons are waited on as events: one thread blocks on the witnesses'
//! change announcement and the stop, bounded by the earliest due moment.
//! Meta `ObserveHarness` reads it all (`ObservesHarness`). No rest, drain,
//! wake or close is taken from any of it.
//!
//! A connection that ends after its Subscribed is dialed again at once. A
//! socket that is absent, refuses the connection, or ends it before its
//! Subscribed is waited on: an inotify watch of its directory, armed before
//! each attempt, cues the next attempt when an entry is placed at its name.
//! Every part ends when the Nexus stops. With no configuration file nothing
//! of this runs.

use crate::RunningNexus;
use crate::event_wait::{Announcement, AnnouncesOnce, AwaitsDescriptors, NameWatch, WatchesName};
use crate::generated::startup::{
    DaemonConfiguration, DaemonConfigurationFault, DaemonConfigurationRefusal, Path as FaultPath,
    Reason,
};
use crate::place::{FindsFlowPlace, PlaceName, ReportsPlaceRefusal};
use crate::store::{AnnouncesLaunchChanges, LaunchChanges};
use crate::{HoldsStopRequest, Stopping};
use harness::{Event, Flow, Observed, State, Tokens};
use meta_signal_flow::{
    HarnessDisagreement, HarnessObserved_Data, HarnessSocket, HarnessTokens, HarnessWitness, Seen,
};
use signal_flow::FlowId;
use signal_harness::Changed;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::io::{self, Read, Write};
use std::os::fd::AsFd;
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::thread::Scope;
use std::time::{Duration, Instant};
use translator::place_file::{ConfigurationHome, LocatesConfigurationHome, LocatesPlaceFile};

/// The daemon's configuration file, beside its place file.
pub trait LocatesDaemonConfiguration {
    /// `<configuration home>/nexus/<place-name>/flow-nexus-daemon.datom`.
    const FILE_NAME: &'static str = "flow-nexus-daemon.datom";
    /// The configuration the place's file holds under `home`; None when
    /// there is no file.
    fn daemon_configuration_under(
        &self,
        home: &ConfigurationHome,
    ) -> Result<Option<DaemonConfiguration>, DaemonConfigurationRefusal>;
    /// [`Self::daemon_configuration_under`] this process's configuration
    /// home.
    fn daemon_configuration(
        &self,
    ) -> Result<Option<DaemonConfiguration>, DaemonConfigurationRefusal>;
}

impl LocatesDaemonConfiguration for PlaceName {
    fn daemon_configuration_under(
        &self,
        home: &ConfigurationHome,
    ) -> Result<Option<DaemonConfiguration>, DaemonConfigurationRefusal> {
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

    fn daemon_configuration(
        &self,
    ) -> Result<Option<DaemonConfiguration>, DaemonConfigurationRefusal> {
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
    /// A missing file is None; a file that cannot be read, holds no
    /// `DaemonConfiguration`, names a socket by a relative path or twice, or
    /// holds a negative grace or capacity is refused, naming the file.
    fn read_daemon_configuration(
        &self,
    ) -> Result<Option<DaemonConfiguration>, DaemonConfigurationRefusal>;
}

impl ReadsDaemonConfiguration for Path {
    fn read_daemon_configuration(
        &self,
    ) -> Result<Option<DaemonConfiguration>, DaemonConfigurationRefusal> {
        use datom_codec::{Actualizing, Budget, Potential};
        let source = match std::fs::read_to_string(self) {
            Ok(source) => source,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
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
        let invalid = |reason: String| Err(DaemonConfigurationRefusal::Invalid(self.fault(reason)));
        let mut named = BTreeSet::new();
        for socket in &configuration.harness_subscription_path_vector {
            if !Path::new(&socket.0).is_absolute() {
                return invalid(format!(
                    "the Harness Nexus subscription socket {} is not an absolute path",
                    socket.0
                ));
            }
            if !named.insert(&socket.0) {
                return invalid(format!(
                    "the Harness Nexus subscription socket {} is named twice",
                    socket.0
                ));
            }
        }
        if configuration.grace_seconds.0 < 0 {
            return invalid(format!(
                "the grace of {} seconds is negative",
                configuration.grace_seconds.0
            ));
        }
        if configuration.disagreement_capacity.0 < 0 {
            return invalid(format!(
                "the disagreement capacity {} is negative",
                configuration.disagreement_capacity.0
            ));
        }
        Ok(Some(configuration))
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

/// What a subscription tells: every session as its snapshot shows it, or a
/// change as it happened.
#[derive(Debug, Clone)]
pub enum HarnessObservation {
    Snapshot(Vec<Observed>),
    Changed(Changed),
}

/// One flow as one socket last told it: the last event, the state it left
/// the flow in, and the last context size told for it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Witnessed {
    pub event: Event,
    pub state: State,
    pub tokens: Tokens,
}

/// The comparison of one flow's harness state with Flow's own, due at a
/// moment: the state the Changed moved the flow to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DueComparison {
    pub due: Instant,
    pub socket: HarnessSocket,
    pub flow_id: FlowId,
    pub state: State,
}

/// Everything the sockets have told: each flow's witness keyed by the
/// socket that told it and the flow, the comparisons not yet due, and the
/// disagreements kept, oldest first.
#[derive(Debug, Default)]
pub struct HarnessTold {
    pub witnesses: BTreeMap<(HarnessSocket, FlowId), Witnessed>,
    pub comparisons: Vec<DueComparison>,
    pub disagreements: VecDeque<HarnessDisagreement>,
}

/// The daemon configuration's subscriptions and what they have told. With
/// no configuration nothing is subscribed to and nothing is told. Written
/// by the subscribers and the judge alone; read by `ObserveHarness`.
#[derive(Default)]
pub struct HarnessWitnesses {
    pub configuration: Option<DaemonConfiguration>,
    pub told: Mutex<HarnessTold>,
    /// Announced at every witness change and every disagreement kept.
    pub changes: LaunchChanges,
}

/// A harness state judged against Flow's own state of the flow.
pub trait JudgesHarnessAgreement {
    /// None for Dry, which is not judged. Flowing.Working, Waiting and
    /// Blocked agree with Acting; Flowing.Rested with Resting; Ended with
    /// Asleep; Crystallizing with any Flowing state.
    fn agrees_with(&self, flow_state: &meta_signal_flow::State) -> Option<bool>;
}

impl JudgesHarnessAgreement for State {
    fn agrees_with(&self, flow_state: &meta_signal_flow::State) -> Option<bool> {
        use meta_signal_flow::State as FlowState;
        Some(match (self, flow_state) {
            (State::Dry, _) => return None,
            (State::Flowing(_), FlowState::Crystallizing) => true,
            (State::Flowing(Flow::Working | Flow::Waiting | Flow::Blocked), FlowState::Acting) => {
                true
            }
            (State::Flowing(Flow::Rested), FlowState::Resting) => true,
            (State::Ended, FlowState::Asleep) => true,
            _ => false,
        })
    }
}

pub trait KeepsHarnessWitnesses {
    /// Takes one observation a socket told at `now`. A snapshot replaces
    /// every witness of that socket, each carrying `InFlight` with the work
    /// the snapshot shows in flight. A Changed sets its flow's event and
    /// state, its tokens too on ContextSize (zero for a flow first told by a
    /// Changed other than ContextSize); when the state is new, its
    /// comparison falls due the configured grace after `now`, replacing the
    /// flow's comparison not yet due.
    fn take(&self, socket: &HarnessSocket, observation: HarnessObservation, now: Instant);
    /// Takes the comparisons due at `now` out, each kept only when its
    /// flow's witness still holds the state it was made for.
    fn due_comparisons(&self, now: Instant) -> Vec<DueComparison>;
    /// The earliest moment a comparison falls due.
    fn next_due(&self) -> Option<Instant>;
    /// Keeps one disagreement when its flow's witness still holds its
    /// harness state, dropping the oldest past the configured capacity.
    fn keep_disagreement(&self, disagreement: HarnessDisagreement);
    /// Every witness, by socket then flow.
    fn witnesses(&self) -> Vec<(HarnessSocket, FlowId, Witnessed)>;
    /// The disagreements kept, oldest first.
    fn disagreements(&self) -> Vec<HarnessDisagreement>;
}

/// The values the configuration sets, read where they are used.
trait ReadsWitnessConfiguration {
    fn grace(&self) -> Duration;
    fn capacity(&self) -> usize;
}

impl ReadsWitnessConfiguration for HarnessWitnesses {
    fn grace(&self) -> Duration {
        self.configuration
            .as_ref()
            .map(|configuration| {
                Duration::from_secs(u64::try_from(configuration.grace_seconds.0).unwrap_or(0))
            })
            .unwrap_or_default()
    }

    fn capacity(&self) -> usize {
        self.configuration
            .as_ref()
            .map(|configuration| {
                usize::try_from(configuration.disagreement_capacity.0).unwrap_or(0)
            })
            .unwrap_or_default()
    }
}

impl KeepsHarnessWitnesses for HarnessWitnesses {
    fn take(&self, socket: &HarnessSocket, observation: HarnessObservation, now: Instant) {
        let grace = self.grace();
        {
            let mut told = self
                .told
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            match observation {
                HarnessObservation::Snapshot(observed) => {
                    told.witnesses.retain(|(told_by, _), _| told_by != socket);
                    for session in observed {
                        told.witnesses.insert(
                            (socket.clone(), session.flow_id),
                            Witnessed {
                                event: Event::InFlight(session.background_work_vector),
                                state: session.state,
                                tokens: session.tokens,
                            },
                        );
                    }
                }
                HarnessObservation::Changed(Changed {
                    flow_id,
                    event,
                    state,
                }) => {
                    let key = (socket.clone(), flow_id.clone());
                    let moved = told
                        .witnesses
                        .get(&key)
                        .is_none_or(|witnessed| witnessed.state != state);
                    let tokens = match &event {
                        Event::ContextSize(tokens) => *tokens,
                        _ => told
                            .witnesses
                            .get(&key)
                            .map_or(Tokens(0), |witnessed| witnessed.tokens),
                    };
                    told.witnesses.insert(
                        key,
                        Witnessed {
                            event,
                            state: state.clone(),
                            tokens,
                        },
                    );
                    if moved {
                        // The flow's earlier move no longer stands: only the
                        // comparison of the state it moved to now falls due.
                        told.comparisons.retain(|pending| {
                            pending.socket != *socket || pending.flow_id != flow_id
                        });
                        told.comparisons.push(DueComparison {
                            due: now + grace,
                            socket: socket.clone(),
                            flow_id,
                            state,
                        });
                    }
                }
            }
        }
        self.changes.announce();
    }

    fn due_comparisons(&self, now: Instant) -> Vec<DueComparison> {
        let mut told = self
            .told
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let (due, waiting): (Vec<_>, Vec<_>) = std::mem::take(&mut told.comparisons)
            .into_iter()
            .partition(|comparison| comparison.due <= now);
        told.comparisons = waiting;
        due.into_iter()
            .filter(|comparison| {
                told.witnesses
                    .get(&(comparison.socket.clone(), comparison.flow_id.clone()))
                    .is_some_and(|witnessed| witnessed.state == comparison.state)
            })
            .collect()
    }

    fn next_due(&self) -> Option<Instant> {
        self.told
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .comparisons
            .iter()
            .map(|comparison| comparison.due)
            .min()
    }

    fn keep_disagreement(&self, disagreement: HarnessDisagreement) {
        let capacity = self.capacity();
        {
            let mut told = self
                .told
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let still = told
                .witnesses
                .get(&(
                    disagreement.harness_socket.clone(),
                    disagreement.flow_id.clone(),
                ))
                .is_some_and(|witnessed| witnessed.state == disagreement.harness_state);
            if !still || capacity == 0 {
                return;
            }
            while told.disagreements.len() >= capacity {
                told.disagreements.pop_front();
            }
            told.disagreements.push_back(disagreement);
        }
        self.changes.announce();
    }

    fn witnesses(&self) -> Vec<(HarnessSocket, FlowId, Witnessed)> {
        self.told
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .witnesses
            .iter()
            .map(|((socket, flow_id), witnessed)| {
                (socket.clone(), flow_id.clone(), witnessed.clone())
            })
            .collect()
    }

    fn disagreements(&self) -> Vec<HarnessDisagreement> {
        self.told
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .disagreements
            .iter()
            .cloned()
            .collect()
    }
}

/// The comparisons of the harness states with Flow's own, as they fall
/// due.
pub trait JudgesHarnessWitnesses {
    /// Compares every comparison due at `now`: a flow Flow records a state
    /// for whose harness state disagrees with it is kept as a disagreement,
    /// Seen at `seen`, in Unix seconds.
    fn judge_due_comparisons(&self, now: Instant, seen: i64);
    /// Judges each comparison as it falls due, until the Nexus stops.
    fn keep_judging(&self);
}

impl JudgesHarnessWitnesses for RunningNexus {
    fn judge_due_comparisons(&self, now: Instant, seen: i64) {
        use crate::generated::transitions::{AskedFlow, RecordsTransitions};
        for comparison in self.harness_witnesses.due_comparisons(now) {
            let flow_state = match self.store.state_of(AskedFlow(comparison.flow_id.clone())) {
                Ok(Some(flow_state)) => flow_state,
                Ok(None) => continue,
                Err(error) => {
                    eprintln!(
                        "flow-nexus: the state of flow {} could not be read to compare with its harness: {error}",
                        comparison.flow_id.0
                    );
                    continue;
                }
            };
            if comparison.state.agrees_with(&flow_state) == Some(false) {
                self.harness_witnesses
                    .keep_disagreement(HarnessDisagreement {
                        flow_id: comparison.flow_id,
                        harness_socket: comparison.socket,
                        seen: Seen(seen),
                        harness_state: comparison.state,
                        state_option: Some(flow_state),
                    });
            }
        }
    }

    fn keep_judging(&self) {
        use crate::reminding::CountsUnixSeconds;
        // The judge moves on two events: a change the witnesses announce
        // and the earliest due moment arriving; it blocks on the
        // announcement and the stop, bounded by that moment.
        while !self.stopping.requested() {
            let cue = match self.harness_witnesses.changes.next_descriptor() {
                Ok(cue) => cue,
                Err(error) => {
                    eprintln!(
                        "flow-nexus: the harness witnesses' changes cannot be awaited: {error}; no disagreement is judged"
                    );
                    return;
                }
            };
            self.judge_due_comparisons(
                Instant::now(),
                std::time::SystemTime::now().seconds_since_epoch(),
            );
            let next = self.harness_witnesses.next_due();
            match [cue.descriptor(), self.stopping.descriptor()].ready(next) {
                Ok(ready) if ready[1] => return,
                Ok(_) => {}
                Err(_) => return,
            }
        }
    }
}

/// Meta `ObserveHarness`.
pub trait ObservesHarness {
    /// Every witness of a flow Flow holds (its events row), each with
    /// Flow's own state of the flow now, and the disagreements kept, oldest
    /// first. A store that cannot be read refuses it.
    fn harness_observed(&self) -> Result<HarnessObserved_Data, meta_signal_flow::Output>;
}

impl ObservesHarness for RunningNexus {
    fn harness_observed(&self) -> Result<HarnessObserved_Data, meta_signal_flow::Output> {
        use crate::generated::transitions::{AskedFlow, RecordsTransitions};
        use crate::store::events::RecordsHarnessEvents;
        let refused = |error: crate::store::StoreError| {
            meta_signal_flow::Output::Refused(meta_signal_flow::Refused_Data::StoreRefused(
                meta_signal_flow::StoreFailure(error.to_string()),
            ))
        };
        let disagreements = self.harness_witnesses.disagreements();
        let mut harness_witness_vector = Vec::new();
        for (socket, flow_id, witnessed) in self.harness_witnesses.witnesses() {
            if self.store.events(&flow_id.0).map_err(refused)?.is_none() {
                continue;
            }
            let state_option = self
                .store
                .state_of(AskedFlow(flow_id.clone()))
                .map_err(refused)?;
            harness_witness_vector.push(HarnessWitness {
                flow_id,
                harness_socket: socket,
                harness_event: witnessed.event,
                harness_state: witnessed.state,
                harness_tokens: HarnessTokens(witnessed.tokens.0),
                state_option,
            });
        }
        Ok(HarnessObserved_Data {
            harness_witness_vector,
            harness_disagreement_vector: disagreements,
        })
    }
}

/// How one connection to a subscription ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubscriptionEnding {
    /// It ended after its Subscribed: dial again at once.
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

/// One socket's subscriber: the Nexus whose witnesses it writes, and the
/// socket it dials.
pub struct HarnessSubscriber<'nexus> {
    pub nexus: &'nexus RunningNexus,
    pub socket: HarnessSocket,
}

pub trait HoldsHarnessSubscription {
    /// The socket's path.
    fn path(&self) -> PathBuf;
    /// Subscribes, and subscribes again each time a connection ends, until
    /// the Nexus stops.
    fn keep_subscribed(&self);
    /// The socket's name watched; None, logged, when it cannot be.
    fn placement(&self) -> Option<SocketPlacement>;
    /// One connection, from Subscribe to its end.
    fn subscribe_once(&self) -> SubscriptionEnding;
}

impl HoldsHarnessSubscription for HarnessSubscriber<'_> {
    fn path(&self) -> PathBuf {
        PathBuf::from(&self.socket.0)
    }

    fn keep_subscribed(&self) {
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
        SocketPlacement::arm(&self.path())
            .inspect_err(|error| {
                eprintln!(
                    "flow-nexus: the Harness Nexus subscription socket {} cannot be watched: {error}; its subscription ends",
                    self.socket.0
                )
            })
            .ok()
    }

    fn subscribe_once(&self) -> SubscriptionEnding {
        let nexus = self.nexus;
        if nexus.stopping.requested() {
            return SubscriptionEnding::Stopping;
        }
        let mut stream = match UnixStream::connect(self.path()) {
            Ok(stream) => stream,
            Err(error) => {
                eprintln!(
                    "flow-nexus: the Harness Nexus subscription socket {} is not answering ({error}); it is waited for",
                    self.socket.0
                );
                return SubscriptionEnding::Unanswered;
            }
        };
        if let Err(error) = stream.send_subscribe() {
            eprintln!(
                "flow-nexus: Subscribe was not sent to {}: {error}; the socket is waited for",
                self.socket.0
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
            let observation = match stream.read_output() {
                Ok(Some(signal_harness::Output::Subscribed(sessions))) => {
                    if !answered {
                        eprintln!(
                            "flow-nexus: subscribed to the Harness Nexus at {}, {} sessions observed",
                            self.socket.0,
                            sessions.len()
                        );
                    }
                    answered = true;
                    HarnessObservation::Snapshot(sessions)
                }
                Ok(Some(signal_harness::Output::Changed(changed))) => {
                    HarnessObservation::Changed(changed)
                }
                Ok(None) => {
                    eprintln!(
                        "flow-nexus: the Harness Nexus subscription at {} ended",
                        self.socket.0
                    );
                    return ended(answered);
                }
                Err(error) if error.kind() == io::ErrorKind::InvalidData => {
                    eprintln!(
                        "flow-nexus: the Harness Nexus subscription at {} sent a frame this contract does not read ({error}); the socket is waited for",
                        self.socket.0
                    );
                    return SubscriptionEnding::Unanswered;
                }
                Err(error) => {
                    eprintln!(
                        "flow-nexus: the Harness Nexus subscription at {} failed: {error}",
                        self.socket.0
                    );
                    return ended(answered);
                }
            };
            nexus
                .harness_witnesses
                .take(&self.socket, observation, Instant::now());
        }
    }
}

/// The part the daemon runs: the subscriptions its configuration names, or
/// nothing.
pub trait SubscribesToHarnesses {
    /// The Nexus as the daemon's configuration sets it; None leaves every
    /// subscription off.
    fn configured_by(self, configuration: Option<DaemonConfiguration>) -> Self;
    /// Spawns into `scope` one subscriber thread per configured socket and
    /// one judge thread for their comparisons, each ending when the Nexus
    /// stops; spawns nothing when no socket is configured.
    fn witness_harnesses<'scope, 'env>(&'env self, scope: &'scope Scope<'scope, 'env>);
}

impl SubscribesToHarnesses for RunningNexus {
    fn configured_by(mut self, configuration: Option<DaemonConfiguration>) -> Self {
        self.harness_witnesses.configuration = configuration;
        self
    }

    fn witness_harnesses<'scope, 'env>(&'env self, scope: &'scope Scope<'scope, 'env>) {
        let Some(configuration) = &self.harness_witnesses.configuration else {
            return;
        };
        if configuration.harness_subscription_path_vector.is_empty() {
            return;
        }
        for path in &configuration.harness_subscription_path_vector {
            let subscriber = HarnessSubscriber {
                nexus: self,
                socket: HarnessSocket(path.0.clone()),
            };
            scope.spawn(move || subscriber.keep_subscribed());
        }
        scope.spawn(move || self.keep_judging());
    }
}
