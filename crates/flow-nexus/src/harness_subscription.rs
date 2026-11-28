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
use crate::store::current_seat::HarnessStanding;
use crate::store::{AnnouncesLaunchChanges, LaunchChanges};
use crate::{HoldsStopRequest, Stopping};
use harness::{Event, Flow, Observed, State, Tokens};
use meta_signal_flow::{
    HarnessDisagreement, HarnessObserved_Data, HarnessSocket, HarnessTokens, HarnessWitness, Seen,
};
use signal_flow::{FlowId, FlowNode};
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
            let held = match self.flow_told_as(&comparison.flow_id) {
                Ok(Some(held)) => held,
                Ok(None) => continue,
                Err(error) => {
                    eprintln!(
                        "flow-nexus: the flow of harness session {} could not be read to compare with its harness: {error}",
                        comparison.flow_id.0
                    );
                    continue;
                }
            };
            let flow_state = match self.store.state_of(AskedFlow(held)) {
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
    /// Every witness, each with Flow's own state of the flow it names now,
    /// None for a session Flow holds no flow for, and the disagreements
    /// kept, oldest first. A store that cannot be read refuses it.
    fn harness_observed(&self) -> Result<HarnessObserved_Data, meta_signal_flow::Output>;
    /// The flow Flow holds for a session a harness names: the live Claude
    /// flow of that session, else the flow of that very id (its events
    /// row), else None.
    fn flow_told_as(&self, told: &FlowId) -> Result<Option<FlowId>, crate::store::StoreError>;
}

impl ObservesHarness for RunningNexus {
    fn flow_told_as(&self, told: &FlowId) -> Result<Option<FlowId>, crate::store::StoreError> {
        use crate::store::events::RecordsHarnessEvents;
        if let Some(node) = self.claude_flow_named(told) {
            return Ok(Some(node.flow_id));
        }
        Ok(self.store.events(&told.0)?.map(|_| told.clone()))
    }

    fn harness_observed(&self) -> Result<HarnessObserved_Data, meta_signal_flow::Output> {
        use crate::generated::transitions::{AskedFlow, RecordsTransitions};
        let refused = |error: crate::store::StoreError| {
            meta_signal_flow::Output::Refused(meta_signal_flow::Refused_Data::StoreRefused(
                meta_signal_flow::StoreFailure(error.to_string()),
            ))
        };
        let disagreements = self.harness_witnesses.disagreements();
        let mut harness_witness_vector = Vec::new();
        for (socket, flow_id, witnessed) in self.harness_witnesses.witnesses() {
            let state_option = match self.flow_told_as(&flow_id).map_err(refused)? {
                Some(held) => self.store.state_of(AskedFlow(held)).map_err(refused)?,
                None => None,
            };
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

/// A turn end or rest a Harness Nexus told of a Claude flow Flow holds:
/// the flow and its session, settled off the subscriber's thread.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToldRest {
    pub flow_id: String,
    pub session: String,
}

/// What a told harness state is for a Claude flow's seat: None for Dry and
/// for the end of a session /clear ended, which goes on in the next one.
trait ReadsHarnessStanding {
    fn harness_standing(&self, event: Option<&Event>) -> Option<HarnessStanding>;
}

impl ReadsHarnessStanding for State {
    fn harness_standing(&self, event: Option<&Event>) -> Option<HarnessStanding> {
        match self {
            State::Dry => None,
            State::Flowing(Flow::Working | Flow::Waiting | Flow::Blocked) => {
                Some(HarnessStanding::Working)
            }
            State::Flowing(Flow::Rested) => Some(HarnessStanding::Rested),
            State::Ended => match event {
                Some(Event::Ended(harness::EndReason::Cleared)) => None,
                _ => Some(HarnessStanding::Ended),
            },
        }
    }
}

/// A Claude flow's state, taken from its Harness Nexus subscription alone.
pub trait FollowsClaudeHarness {
    /// The live Claude flow whose stored session the harness names by
    /// `told`, its first six characters. None when Flow holds none, or
    /// when two live flows share the name: that collision is logged and
    /// decides nothing for either.
    fn claude_flow_named(&self, told: &FlowId) -> Option<FlowNode>;
    /// Writes the seat row of every live Claude flow the observation tells
    /// of, under the restart fence, and returns the turn ends and rests it
    /// told: a seat told TurnEnded, whatever its state, or one that moved
    /// to Resting.
    fn follow_claude_harness(&self, observation: &HarnessObservation) -> Vec<ToldRest>;
    /// What a correlated hook Stop set going, set going by a told rest:
    /// launches waiting on a Stop are cued, the turn end is published, a
    /// simplified seat's answer is queued, and the rest is settled (drain,
    /// refresh ordered or due, sleep), remembered when Herdr does not yet
    /// show it.
    fn settle_told_rest(&self, rest: &ToldRest);
    /// Records the route of the Herdr pane whose Claude agent shows the
    /// rest's session when the flow's recorded route no longer shows it.
    fn follow_session_route(&self, rest: &ToldRest);
}

impl FollowsClaudeHarness for RunningNexus {
    fn claude_flow_named(&self, told: &FlowId) -> Option<FlowNode> {
        use crate::store::ReadsFlowRows;
        match self.store.live_claude_sessions_named(&told.0) {
            Ok(mut nodes) if nodes.len() == 1 => nodes.pop(),
            Ok(nodes) if nodes.is_empty() => None,
            Ok(nodes) => {
                eprintln!(
                    "flow-nexus: the Harness Nexus names session {} which live flows {:?} share; it is refused and decides nothing for either",
                    told.0,
                    nodes
                        .iter()
                        .map(|node| node.flow_id.0.as_str())
                        .collect::<Vec<_>>()
                );
                None
            }
            Err(error) => {
                eprintln!(
                    "flow-nexus: the flow of harness session {} could not be read: {error}",
                    told.0
                );
                None
            }
        }
    }

    fn follow_claude_harness(&self, observation: &HarnessObservation) -> Vec<ToldRest> {
        use crate::store::current_seat::RecordsHarnessStandings;
        use crate::store::restart::RecordsRestarts;
        let told: Vec<(&FlowId, Option<&Event>, &State)> = match observation {
            HarnessObservation::Snapshot(observed) => observed
                .iter()
                .map(|session| (&session.flow_id, None, &session.state))
                .collect(),
            HarnessObservation::Changed(changed) => {
                vec![(&changed.flow_id, Some(&changed.event), &changed.state)]
            }
        };
        let mut rests = Vec::new();
        for (flow_id, event, state) in told {
            let Some(standing) = state.harness_standing(event) else {
                continue;
            };
            let Some(node) = self.claude_flow_named(flow_id) else {
                continue;
            };
            let id = node.flow_id.0.clone();
            // A restart holds the flow: its seat is the restart's to write.
            if !matches!(self.store.restart_record(&id), Ok(None)) {
                eprintln!(
                    "flow-nexus: flow {id} is restarting; its harness's {state:?} decides nothing"
                );
                continue;
            }
            let moved = match self.store.record_harness_standing(&id, standing) {
                Ok(moved) => moved,
                Err(error) => {
                    eprintln!(
                        "flow-nexus: flow {id} has its harness's {state:?} unrecorded: {error}"
                    );
                    continue;
                }
            };
            // A turn's end is settled as a correlated hook Stop was, work in
            // flight or not: its size is read and its warning sent, and
            // only a seat at rest is drained, refreshed or put to sleep.
            if matches!(event, Some(Event::TurnEnded))
                || (standing == HarnessStanding::Rested && moved)
            {
                rests.push(ToldRest {
                    flow_id: id,
                    session: node.session_id.0,
                });
            }
        }
        rests
    }

    fn settle_told_rest(&self, rest: &ToldRest) {
        use crate::rest_watching::RemembersPendingRest;
        use crate::waking::{AtStop, KeptAtStop, QueuesSimplifiedAnswers, SleepsAndWakesSeats};
        // A pane resumed bare may be a new pane: its route follows the
        // session, so the rest's refresh and reap find the pane it runs in.
        self.follow_session_route(rest);
        // A launch sending its first message in parts waits on a rest
        // before its next part.
        self.store.launch_changes.announce();
        crate::turn_ends::RecordsTurnEnd::turn_ended(self, &rest.flow_id);
        {
            let _turn = self
                .dispatch_gate
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            self.queue_simplified_answer(&rest.flow_id);
        }
        if matches!(
            self.settle_native_rest(&rest.flow_id, &rest.session),
            AtStop::Kept(KeptAtStop::NotResting(_))
        ) {
            self.remember_pending_rest(&rest.flow_id, &rest.session);
        }
    }

    fn follow_session_route(&self, rest: &ToldRest) {
        use crate::herdr::ReadsHerdrRoster;
        use crate::session_binding::BindsSessions;
        use crate::store::ReadsFlowRows;
        use crate::store::repair::RepairsMetaflows;
        let Ok(Some(node)) = self.store.flow_node(&rest.flow_id) else {
            return;
        };
        if self.herdr.current_route(&node).is_some() {
            return;
        }
        let Some(route) = self.route_showing_session(&rest.session) else {
            return;
        };
        if matches!(&node.herdr_route_selection, signal_flow::HerdrRouteSelection::Available(recorded) if *recorded == route)
        {
            return;
        }
        match self.store.rebind_route(&rest.flow_id, route) {
            Ok(()) => eprintln!(
                "flow-nexus: flow {} follows its session {} to the pane that shows it",
                rest.flow_id, rest.session
            ),
            Err(error) => eprintln!(
                "flow-nexus: flow {} was not given the pane that shows its session: {error}",
                rest.flow_id
            ),
        }
    }
}

/// One socket's subscriber: the Nexus whose witnesses it writes, the
/// socket it dials, and where the rests it is told are settled.
pub struct HarnessSubscriber<'nexus> {
    pub nexus: &'nexus RunningNexus,
    pub socket: HarnessSocket,
    pub rests: std::sync::mpsc::Sender<ToldRest>,
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
                .take(&self.socket, observation.clone(), Instant::now());
            for rest in nexus.follow_claude_harness(&observation) {
                // The settling thread ends with the subscribers.
                let _ = self.rests.send(rest);
            }
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
        let (rests, told) = std::sync::mpsc::channel::<ToldRest>();
        for path in &configuration.harness_subscription_path_vector {
            let subscriber = HarnessSubscriber {
                nexus: self,
                socket: HarnessSocket(path.0.clone()),
                rests: rests.clone(),
            };
            scope.spawn(move || subscriber.keep_subscribed());
        }
        drop(rests);
        // A told rest may start a successor and wait on its launch: it is
        // settled here, so the subscribers go on reading meanwhile. This
        // thread ends once every subscriber has.
        scope.spawn(move || {
            for rest in told {
                self.settle_told_rest(&rest);
            }
        });
        scope.spawn(move || self.keep_judging());
    }
}
