//! Restart: a live flow's harness is closed and its own native session is
//! resumed in a new pane, the FlowId, session, Model and transcript kept,
//! with its Address's current launch configuration, modules and hooks aimed
//! at this Nexus, and no prompt typed. This slice restarts a Claude flow,
//! Awake and resting, asked from outside; every other case is refused
//! Unsupported.
//!
//! The RestartRecord is the restart's lock on the flow's Address, durable
//! in Memory and read by every admission: while it stands, a Send queues,
//! nothing is handed to the seat, no Stop the seat reports settles it, and
//! no end of the flow is recorded. Before Closing is written the resume is
//! composed and validated in full: its launch composed, its root and
//! harness flags read, its title, settings and command line built as the
//! spawn builds them. A refusal there closes nothing and writes nothing:
//! the old harness runs on and its record stays as it was. Each phase is
//! written before the effect it names: Closing before the old pane is
//! closed, Closed once the old pane is gone and its harness process proven
//! exited, Opening before the new pane is created, Spawning with that pane
//! before the harness is resumed in it. The resumed seat is bound in one
//! write that names its route and its SeatBirth and removes the record; its
//! queue then goes through the ordinary delivery custody.
//!
//! A failure keeps the session, the queue and the Address held, as
//! Failed, for a retry, which reconciles the old harness first, or for the
//! owner's Stop. A spawn whose outcome Flow cannot prove (a pane created and
//! not shown closed, or a Nexus that died after it wrote Opening or
//! Spawning) is Interrupted: never retried, never respawned from an observed
//! absence, and cleared only by a Stop that proves every pane holding the
//! session, and the restart's own pane, gone.
//!
//! The fence: once a restart has bound a seat, a harness event acts only
//! when its caller descends from that seat's harness process, by birth
//! (`Process.{ pid started }`) and lineage, and is marked in the seat's own
//! pane; an Observe's Gone acts only for the route and birth it was opened
//! on. A late event of the seat the restart replaced, or one of unknown
//! provenance, is recorded and acts on nothing.

use crate::RunningNexus;
use crate::caller::{CallerPane, CallerProcess, LocatesCallerPane};
use crate::generated::memory::SeatBirth;
use crate::generated::operation::{Failed_Data, Open_Data, Operation, Outcome, PaneLaunch};
use crate::herdr::launch::{HoldsSystemPrompt, WaitsForPaneRest};
use crate::herdr::pane::{PaneAgent, WritesPane};
use crate::herdr::{HerdrCli, PaneOwnership, PanePresence, ReadsHerdrPanes};
use crate::performing::Performs;
use crate::store::restart::RecordsRestarts;
use crate::store::{FlowStore, NamesLiveFlow, ReadsFlowMemory, ReadsFlowRows, StoreError};
use meta_signal_flow::{
    FlowBinding, FlowBindingRefusalReason, OldSeat, ProcessIdentity, RestartFailure, RestartPhase,
    RestartRecord,
};
use signal_flow::{
    FlowNode, HarnessKind, HerdrPaneBinding, HerdrRoute, HerdrRouteSelection, Origin, Process,
    Refusal, Response, RestartRefusal, ResumeRefused_Data, StopRejection,
};
use std::collections::BTreeSet;

/// What an Observe was opened on: the route the flow ran in, and the birth
/// of the seat a restart bound there, when one did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SeatWitness {
    pub route: HerdrRoute,
    pub birth: Option<Process>,
}

/// Restart as the Nexus serves it.
pub trait RestartsFlows {
    /// Restarts `flow_id`, as `origin` asked. Runs off the dispatch gate and
    /// takes it for each step, so a Send arriving meanwhile is admitted and
    /// queues.
    fn restart(&self, flow_id: &str, origin: &Origin) -> Response;
    /// Takes up every restart an earlier Nexus left: Closing and Closed are
    /// composed again first, then Closing is reconciled against the old
    /// pane and Closed resumes; Opening is Interrupted, and
    /// Spawning binds the seat its pane shows or is Interrupted. Failed and
    /// Interrupted stay as they are.
    fn recover_restarts(&self);
    /// The owner's Stop of a flow under a restart record, under the gate:
    /// every pane Herdr shows holding the session is closed, and the
    /// restart's own pane, then the record is Failed.Stopped, the FlowId,
    /// session and queue kept. A restart step running is Restarting; an
    /// Interrupted spawn not proven quiesced is SpawnUnproven.
    fn stop_restarting(&self, record: RestartRecord) -> Response;
}

/// The fence on a flow's harness events.
pub(crate) trait FencesSeatEvents {
    /// The route the flow runs in and the birth of the seat a restart bound.
    fn seat_witness(&self, flow_id: &str) -> Option<SeatWitness>;
    /// Whether a harness event of `flow_id` reported by `caller` may act:
    /// never while a restart holds the flow; for a seat a restart bound,
    /// only when the caller descends from its harness process and is marked
    /// in its pane; otherwise as before any restart.
    fn report_acts(&self, flow_id: &str, caller: Option<CallerProcess>) -> bool;
    /// Whether an exit witnessed of `witness` may be recorded: never while a
    /// restart holds the flow, and only while the flow still runs where and
    /// as the witness saw it. None witnesses the flow as it stands.
    fn witness_holds(&self, flow_id: &str, witness: Option<&SeatWitness>) -> bool;
}

/// Closes a pane a restart created, known by the pane and terminal Herdr
/// gave it, whatever runs in it, and answers whether Herdr shows it gone.
pub(crate) trait DiscardsCreatedPane {
    fn discard_created(&self, pane: &HerdrPaneBinding) -> bool;
}

/// What Herdr and the kernel show of a seat's harness.
trait FindsSeatProcesses {
    /// The foreground process of the pane that runs `session`: its argv
    /// names it after `--session-id` or `--resume`, or Claude's own record
    /// of the process names it.
    fn harness_process(&self, route: &HerdrRoute, session: &str) -> Option<Process>;
    /// Every pane of the Herdr session whose agent reports `session`; None
    /// when Herdr cannot be read.
    fn panes_holding(&self, herdr_session_name: &str, session: &str) -> Option<Vec<HerdrRoute>>;
}

/// Whether a process the kernel knew has ended.
trait ProvesExit {
    /// Gone from `/proc`, another process at its pid, or a zombie.
    fn has_exited(&self) -> bool;
}

impl ProvesExit for Process {
    fn has_exited(&self) -> bool {
        let Ok(pid) = u32::try_from(self.pid) else {
            return true;
        };
        let Ok(stat) = std::fs::read_to_string(format!("/proc/{pid}/stat")) else {
            return true;
        };
        let Some((_, fields)) = stat.rsplit_once(") ") else {
            return true;
        };
        let mut fields = fields.split_whitespace();
        let state = fields.next();
        let started = fields.nth(18).and_then(|field| field.parse::<i64>().ok());
        matches!(state, Some("Z" | "X")) || started != Some(self.started)
    }
}

impl FindsSeatProcesses for HerdrCli {
    fn harness_process(&self, route: &HerdrRoute, session: &str) -> Option<Process> {
        use crate::claude::ReadsClaudeSessions;
        let processes = self.foreground_processes(route)?;
        processes
            .iter()
            .find(|process| {
                process.arguments.windows(2).any(|pair| {
                    (pair[0] == "--session-id" || pair[0] == "--resume") && pair[1] == session
                }) || self
                    .claude_sessions
                    .session_of(process.process_id)
                    .as_deref()
                    == Some(session)
            })
            .and_then(|process| FlowStore::process_identity_for_pid(process.process_id))
    }

    fn panes_holding(&self, herdr_session_name: &str, session: &str) -> Option<Vec<HerdrRoute>> {
        let snapshot = self.session_snapshot(herdr_session_name)?;
        let agents = snapshot
            .pointer("/result/snapshot/agents")
            .and_then(serde_json::Value::as_array)?;
        let field = |agent: &serde_json::Value, key: &str| {
            agent
                .get(key)
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned)
        };
        Some(
            agents
                .iter()
                .filter(|agent| {
                    agent
                        .pointer("/agent_session/value")
                        .and_then(serde_json::Value::as_str)
                        == Some(session)
                })
                .filter_map(|agent| {
                    Some(HerdrRoute {
                        herdr_session_name: herdr_session_name.to_owned(),
                        herdr_agent_name: field(agent, "name").unwrap_or_default(),
                        herdr_pane_id: field(agent, "pane_id")?,
                        herdr_terminal_id: field(agent, "terminal_id")?,
                    })
                })
                .collect(),
        )
    }
}

/// Whether a readable snapshot shows the pane a restart created, by its pane
/// and terminal; None when Herdr cannot be read.
trait ShowsCreatedPane {
    fn shows_created(&self, pane: &HerdrPaneBinding) -> Option<bool>;
}

impl ShowsCreatedPane for HerdrCli {
    fn shows_created(&self, pane: &HerdrPaneBinding) -> Option<bool> {
        let snapshot = self.session_snapshot(&pane.herdr_session_name)?;
        let shown = |list: &str| {
            snapshot
                .pointer(list)
                .and_then(serde_json::Value::as_array)
                .is_some_and(|entries| {
                    entries.iter().any(|entry| {
                        entry.get("pane_id").and_then(serde_json::Value::as_str)
                            == Some(pane.herdr_pane_id.as_str())
                            && entry.get("terminal_id").and_then(serde_json::Value::as_str)
                                == Some(pane.herdr_terminal_id.as_str())
                    })
                })
        };
        snapshot.pointer("/result/snapshot/agents")?;
        Some(shown("/result/snapshot/agents") || shown("/result/snapshot/panes"))
    }
}

impl DiscardsCreatedPane for HerdrCli {
    fn discard_created(&self, pane: &HerdrPaneBinding) -> bool {
        match self.shows_created(pane) {
            None => return false,
            Some(false) => return true,
            Some(true) => {}
        }
        let closed = std::process::Command::new(&self.executable)
            .args([
                "--session",
                pane.herdr_session_name.as_str(),
                "pane",
                "close",
                pane.herdr_pane_id.as_str(),
            ])
            .output()
            .is_ok_and(|output| output.status.success());
        closed && self.shows_created(pane) == Some(false)
    }
}

/// What a restart resumes its session with, composed and validated before
/// the old harness is closed. It is held in memory from Closing through
/// Spawning, so the launch validated is the launch resumed; a Nexus that
/// takes up a restart another left composes it again, by the same steps,
/// before it touches the old seat.
struct ResumeLaunch {
    composed_launch: signal_flow::ComposedLaunch,
    root: meta_signal_flow::Root,
    harness_arguments: Vec<String>,
}

/// A restart under way, and the launch it resumes with.
struct PreparedRestart {
    record: RestartRecord,
    resume_launch: ResumeLaunch,
}

/// The steps of one restart, each run under the gate unless it says not.
trait RestartsInSteps {
    /// The checks, the resume composed and validated, then Closing written;
    /// a retry of a Failed restart is written Closing again, so its old
    /// harness is reconciled first. A refusal writes nothing.
    fn begun(&self, flow_id: &str, origin: &Origin) -> Result<PreparedRestart, Box<Response>>;
    /// A restart another Nexus left at Closing or Closed, its resume
    /// composed and validated again; refused, the record is Failed and
    /// nothing is closed.
    fn taken_up(&self, record: RestartRecord) -> Result<PreparedRestart, Box<Response>>;
    /// The old pane closed, or found gone, and its harness proven exited:
    /// Closed written. Anything else is Failed.
    fn closed_old(&self, prepared: PreparedRestart) -> Result<PreparedRestart, Box<Response>>;
    /// From Closed: Opening written, the pane opened and recorded Spawning,
    /// the harness resumed with the prepared launch and bound off the gate,
    /// then bound or abandoned under it.
    fn resumed(&self, prepared: PreparedRestart) -> Response;
    /// The launch a resume runs: the current declaration's modules, the
    /// flow's own Model, this Nexus's hooks; no brief. Composed, its root
    /// and harness flags read, and its command line rehearsed; nothing is
    /// written and no pane is touched.
    fn resume_launch(&self, record: &RestartRecord) -> Result<ResumeLaunch, RestartFailure>;
    /// The resume's command line built as the spawn builds it, with its
    /// title, its settings and its session, and checked as the spawn checks
    /// it; only the pane, which does not exist yet, is a placeholder.
    fn rehearsed(
        &self,
        record: &RestartRecord,
        launch: &signal_flow::ComposedLaunch,
        harness_arguments: &[String],
    ) -> Result<(), RestartFailure>;
    /// Off the gate: the pane holds the same FlowId, session and Model,
    /// verified as an adopted seat is, with the birth of its harness.
    fn verified_resume(
        &self,
        record: &RestartRecord,
        pane: &HerdrPaneBinding,
        model: &str,
    ) -> Result<SeatBirth, RestartFailure>;
    /// The resumed seat bound in one write; then its queue is handed to it,
    /// off the gate.
    fn bound(&self, record: &RestartRecord, birth: SeatBirth) -> Response;
    /// A resume that did not bind: its pane discarded and the record Failed,
    /// or, the pane not shown gone, Interrupted.
    fn abandoned(
        &self,
        record: RestartRecord,
        pane: HerdrPaneBinding,
        failure: RestartFailure,
    ) -> Response;
    fn written(&self, record: &RestartRecord) -> bool;
    fn failed(&self, record: RestartRecord, failure: RestartFailure) -> Response;
    fn interrupted(&self, record: RestartRecord, pane: Option<HerdrPaneBinding>) -> Response;
    /// The native model the flow runs: its role's, else its launch's.
    fn actual_model(&self, flow_id: &str) -> Option<String>;
    /// The flow's latest launch profile.
    fn last_profile(&self, flow_id: &str) -> Option<signal_flow::LaunchProfile>;
    /// Every pane Herdr shows holding the session closed, and `pane`
    /// discarded; whether Herdr then shows the session held nowhere.
    fn quiesced(&self, record: &RestartRecord, pane: Option<&HerdrPaneBinding>) -> bool;
    /// The flow's row as the restart sees it, at `route`.
    fn seat_node(&self, record: &RestartRecord, route: HerdrRoute) -> Option<FlowNode>;
}

/// A refusal of a restart, naming its flow.
trait AnswersRestartFailure {
    fn answer(&self, flow_id: &str) -> Response;
}

impl AnswersRestartFailure for RestartFailure {
    fn answer(&self, flow_id: &str) -> Response {
        let flow_id = flow_id.to_owned();
        Response::RestartRefused(match self {
            RestartFailure::CloseRefused | RestartFailure::CloseUnproven => {
                RestartRefusal::CloseRefused(flow_id)
            }
            RestartFailure::ResumeRefused(category) => {
                RestartRefusal::ResumeRefused(ResumeRefused_Data {
                    flow_id,
                    string: category.clone(),
                })
            }
            RestartFailure::BindMismatch => RestartRefusal::ResumeRefused(ResumeRefused_Data {
                flow_id,
                string: "BindMismatch".into(),
            }),
            RestartFailure::Stopped => RestartRefusal::Busy(flow_id),
        })
    }
}

/// A store that could not be written or read, as a restart answers it.
trait RefusesStoreForRestart {
    fn refused(self) -> Response;
}

impl RefusesStoreForRestart for StoreError {
    fn refused(self) -> Response {
        Response::Refused(Refusal::Store(self.to_string()))
    }
}

impl RestartsInSteps for RunningNexus {
    fn begun(&self, flow_id: &str, origin: &Origin) -> Result<PreparedRestart, Box<Response>> {
        use crate::store::NamesConfiguredModel;
        use crate::store::address_hold::HoldsLaunchAddress;
        use crate::store::metaflow::SleepsMetaflows;
        use crate::store::queue_delivery::AccountsQueueDelivery;
        use crate::waking::{HoldsWakingMetaflows, ReadsClaudeDelivery, WakePolicy};
        let refused = |refusal: fn(String) -> RestartRefusal| {
            Err(Box::new(Response::RestartRefused(refusal(
                flow_id.to_owned(),
            ))))
        };
        let node = match self.store.flow_node(flow_id) {
            Ok(Some(node)) => node,
            Ok(None) => return refused(RestartRefusal::UnknownFlow),
            Err(error) => return Err(Box::new(error.refused())),
        };
        // A flow asking for its own restart is owed one at its Stop, which
        // this slice does not serve.
        if matches!(origin, Origin::Flow(asking) if asking.flow_id == flow_id) {
            return refused(RestartRefusal::Unsupported);
        }
        match self.store.restart_record(flow_id) {
            Ok(None) => {}
            Ok(Some(record)) => {
                return match record.restart_phase {
                    RestartPhase::Failed(_) => {
                        let retried = RestartRecord {
                            restart_phase: RestartPhase::Closing,
                            attempts: record.attempts + 1,
                            ..record
                        };
                        // Refused here, the Failed record stays as it was.
                        let resume_launch = self
                            .resume_launch(&retried)
                            .map_err(|failure| Box::new(failure.answer(flow_id)))?;
                        if self.written(&retried) {
                            Ok(PreparedRestart {
                                record: retried,
                                resume_launch,
                            })
                        } else {
                            Err(Box::new(StoreError::StateInvariant.refused()))
                        }
                    }
                    RestartPhase::Interrupted(_) => refused(RestartRefusal::Interrupted),
                    _ => refused(RestartRefusal::Busy),
                };
            }
            Err(error) => return Err(Box::new(error.refused())),
        }
        // Asleep, Past, Codex: other slices.
        if !node.flow_lifecycle.is_live() || node.harness_kind != HarnessKind::Claude {
            return refused(RestartRefusal::Unsupported);
        }
        if node.session_id.is_empty() {
            return refused(RestartRefusal::NoHeldSession);
        }
        let HerdrRouteSelection::Available(route) = node.herdr_route_selection.clone() else {
            return refused(RestartRefusal::Unsupported);
        };
        let address = match self.store.metaflow_awake_as(flow_id) {
            Ok(Some(metaflow)) => metaflow.address,
            Ok(None) => return refused(RestartRefusal::Unsupported),
            Err(error) => return Err(Box::new(error.refused())),
        };
        if self.waking.is_waking(&address)
            || !matches!(self.store.address_hold(flow_id), Ok(None))
            || !matches!(self.store.outstanding_delivery(&address), Ok(None))
        {
            return refused(RestartRefusal::Busy);
        }
        if self.claude_transcript(&node).is_none() {
            return refused(RestartRefusal::TranscriptAbsent);
        }
        match (
            self.actual_model(flow_id),
            self.store.configured_model(&address),
        ) {
            (Some(actual), Ok(Some(configured))) if actual == configured => {}
            _ => {
                return Err(Box::new(Response::RestartRefused(
                    RestartRefusal::ResumeRefused(ResumeRefused_Data {
                        flow_id: flow_id.to_owned(),
                        string: "ModelDiffers".into(),
                    }),
                )));
            }
        }
        let presence = self.herdr.pane_presence(&node);
        match presence {
            PanePresence::Unknown => return refused(RestartRefusal::CloseRefused),
            // A working seat is restarted at its Stop, which this slice
            // does not serve.
            PanePresence::Present => {
                let resting = match self.herdr.pane_agent(&node) {
                    PaneAgent::Present { agent_state, .. } => Self::rests_at(&agent_state),
                    PaneAgent::Absent | PaneAgent::Unreadable => false,
                };
                if !resting || !self.claude_main_turn_ended(&node) {
                    return refused(RestartRefusal::Unsupported);
                }
            }
            PanePresence::Bare | PanePresence::Absent => {}
        }
        let process_option = match presence {
            PanePresence::Present | PanePresence::Bare => {
                self.herdr.harness_process(&route, &node.session_id)
            }
            PanePresence::Absent | PanePresence::Unknown => None,
        };
        let record = RestartRecord {
            flow_id: flow_id.to_owned(),
            native_session_id: node.session_id.clone(),
            address,
            old_seat: OldSeat {
                herdr_route: route,
                process_option,
            },
            restart_phase: RestartPhase::Closing,
            attempts: 1,
        };
        // Refused here, nothing is written and the old harness runs on.
        let resume_launch = self
            .resume_launch(&record)
            .map_err(|failure| Box::new(failure.answer(flow_id)))?;
        if !self.written(&record) {
            return Err(Box::new(StoreError::StateInvariant.refused()));
        }
        Ok(PreparedRestart {
            record,
            resume_launch,
        })
    }

    fn taken_up(&self, record: RestartRecord) -> Result<PreparedRestart, Box<Response>> {
        match self.resume_launch(&record) {
            Ok(resume_launch) => Ok(PreparedRestart {
                record,
                resume_launch,
            }),
            Err(failure) => Err(Box::new(self.failed(record, failure))),
        }
    }

    fn closed_old(&self, prepared: PreparedRestart) -> Result<PreparedRestart, Box<Response>> {
        use crate::delivery::lease::LeasesPanes;
        let PreparedRestart {
            record,
            resume_launch,
        } = prepared;
        let Some(old) = self.seat_node(&record, record.old_seat.herdr_route.clone()) else {
            return Err(Box::new(self.failed(record, RestartFailure::CloseUnproven)));
        };
        match self.herdr.pane_ownership(&old) {
            PaneOwnership::Owned | PaneOwnership::Bare => {
                let closed = {
                    let _lease = self.pane_leases.hold(&record.old_seat.herdr_route);
                    self.perform(Operation::Close(old.clone()))
                };
                if closed != Outcome::Closed {
                    return Err(Box::new(self.failed(record, RestartFailure::CloseRefused)));
                }
            }
            PaneOwnership::Absent => {}
            PaneOwnership::Unknown => {
                return Err(Box::new(self.failed(record, RestartFailure::CloseRefused)));
            }
            PaneOwnership::Foreign | PaneOwnership::Unproven | PaneOwnership::Unidentified => {
                return Err(Box::new(self.failed(record, RestartFailure::CloseUnproven)));
            }
        }
        // Closed only by positive identity: the pane gone, no pane holding
        // the session, and the harness process it showed exited.
        let gone = self.herdr.pane_presence(&old) == PanePresence::Absent
            && self
                .herdr
                .holds_native_session(&record.old_seat.herdr_route.herdr_session_name, &old)
                == Some(false)
            && record
                .old_seat
                .process_option
                .as_ref()
                .is_none_or(ProvesExit::has_exited);
        if !gone {
            return Err(Box::new(self.failed(record, RestartFailure::CloseUnproven)));
        }
        let closed = RestartRecord {
            restart_phase: RestartPhase::Closed,
            ..record
        };
        if !self.written(&closed) {
            return Err(Box::new(StoreError::StateInvariant.refused()));
        }
        Ok(PreparedRestart {
            record: closed,
            resume_launch,
        })
    }

    fn resumed(&self, prepared: PreparedRestart) -> Response {
        let PreparedRestart {
            record,
            resume_launch:
                ResumeLaunch {
                    composed_launch: launch,
                    root,
                    harness_arguments,
                },
        } = prepared;
        let flow_id = record.flow_id.clone();
        let pane: HerdrPaneBinding;
        let spawning = {
            let _turn = self
                .dispatch_gate
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            // The spawn's intent is durable before any pane exists.
            let opening = RestartRecord {
                restart_phase: RestartPhase::Opening,
                ..record
            };
            if !self.written(&opening) {
                return StoreError::StateInvariant.refused();
            }
            pane = match self.perform(Operation::Open(Open_Data {
                composed_launch: launch.clone(),
                root: root.clone(),
            })) {
                Outcome::Opened(pane) => pane,
                // Herdr may have made the pane before it refused: unproven.
                _ => return self.interrupted(opening, None),
            };
            let spawning = RestartRecord {
                restart_phase: RestartPhase::Spawning(pane.clone()),
                ..opening
            };
            if !self.written(&spawning) {
                return StoreError::StateInvariant.refused();
            }
            spawning
        };
        let model = launch.launch_profile.model_name.clone();
        let request = launch.launch_profile.launch_request_id.clone();
        let verified = match self.perform(Operation::Resume(PaneLaunch {
            composed_launch: launch,
            root,
            herdr_pane_binding: pane.clone(),
            flow_id_option: Some(flow_id.clone()),
            native_session_id_option: Some(spawning.native_session_id.clone()),
            harness_arguments,
        })) {
            Outcome::Spawned(_) => self.verified_resume(&spawning, &pane, &model),
            Outcome::Failed(Failed_Data::SpawnRefused(_)) => {
                Err(RestartFailure::ResumeRefused("SpawnRefused".into()))
            }
            _ => Err(RestartFailure::ResumeRefused("Spawn".into())),
        };
        self.herdr.release_system_prompt(&request);
        let _turn = self
            .dispatch_gate
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        // No other step writes a phase while this one runs: Stop answers
        // Restarting, and another Restart answers Busy.
        if !matches!(self.store.restart_record(&flow_id), Ok(Some(current)) if current == spawning)
        {
            return Response::RestartRefused(RestartRefusal::Busy(flow_id));
        }
        match verified {
            Ok(birth) => self.bound(&spawning, birth),
            Err(failure) => self.abandoned(spawning, pane, failure),
        }
    }

    fn resume_launch(&self, record: &RestartRecord) -> Result<ResumeLaunch, RestartFailure> {
        use crate::launching::ReadsHarnessArguments;
        use crate::store::metaflow_declaration::ReadsMetaflowDeclarations;
        let refused = |category: &str| RestartFailure::ResumeRefused(category.into());
        let last = self.last_profile(&record.flow_id);
        let declared = self
            .store
            .metaflow_declaration(&record.address)
            .map_err(|_| refused("StoreRefused"))?;
        let herdr_session_name = last
            .as_ref()
            .map(|profile| profile.herdr_session_name.clone())
            .unwrap_or_else(|| record.old_seat.herdr_route.herdr_session_name.clone());
        let (system_modules, user_modules, flow_kind, tool_set, leave_outs_option) =
            match (declared, last) {
                (Some(declared), _) => (
                    declared.system_modules,
                    declared.user_modules,
                    declared.flow_kind,
                    declared.tool_set,
                    None,
                ),
                (None, Some(profile)) => (
                    profile.system_modules,
                    profile.user_modules,
                    profile.flow_kind,
                    profile.tool_set,
                    profile.leave_outs_option,
                ),
                (None, None) => return Err(refused("Undeclared")),
            };
        let model_name = self
            .actual_model(&record.flow_id)
            .ok_or_else(|| refused("ModelUnknown"))?;
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|elapsed| elapsed.as_nanos())
            .unwrap_or_default();
        let profile = signal_flow::LaunchProfile {
            launch_request_id: format!("restart-{}-{}-{nanos}", record.flow_id, record.attempts),
            system_modules,
            user_modules,
            remembering: signal_flow::Remembering::Predecessors(0),
            address: record.address.clone(),
            model_name,
            effort: <Self as crate::sending::ChoosesEffort>::effort_of(&record.address).into(),
            flow_id_option: None,
            herdr_session_name,
            leave_outs_option,
            flow_kind,
            tool_set,
            instruction_prompt: String::new(),
        };
        let composed_launch = match self.perform(Operation::Compose(profile)) {
            Outcome::Composed(launch) => launch,
            _ => return Err(refused("NotComposed")),
        };
        let root = self
            .launch_root(&record.address)
            .map_err(|_| refused("RootRefused"))?;
        let harness_arguments = self
            .harness_arguments(&record.address)
            .map_err(|_| refused("MalformedHarnessFlags"))?;
        self.rehearsed(record, &composed_launch, &harness_arguments)?;
        Ok(ResumeLaunch {
            composed_launch,
            root,
            harness_arguments,
        })
    }

    fn rehearsed(
        &self,
        record: &RestartRecord,
        launch: &signal_flow::ComposedLaunch,
        harness_arguments: &[String],
    ) -> Result<(), RestartFailure> {
        use crate::harness_command::HarnessCommandResources;
        use crate::herdr::launch::{
            ChecksComposedLaunch, ConstructsHarnessCommand, NamesHerdrHarness, PreparesClaudePane,
            SpawnFailure,
        };
        use crate::title::{ShowsNativeTitle, TitlesFlow};
        let refused = |category: String| RestartFailure::ResumeRefused(category);
        // Only a Claude harness is resumed in a pane.
        if launch.harness_kind != HarnessKind::Claude {
            return Err(refused("HarnessMismatch".into()));
        }
        let title = launch
            .launch_profile
            .native_title(&record.flow_id)
            .map_err(|_| refused("TitleRefused".into()))?;
        let resources = HarnessCommandResources {
            herdr_session: launch.launch_profile.herdr_session_name.clone(),
            agent: launch.launch_agent_name(),
            // The pane is made only once the old harness is closed.
            pane: "<pane>".into(),
            program: launch.harness_kind.expected_harness().to_owned(),
            codex_socket: None,
            claude_settings: Some(self.herdr.claude_flag_settings(&launch.lean_start())),
            claude_title: Some(title.as_str().to_owned()),
            // The longest path a held system prompt is read by.
            prompt_fd: Some(format!("/proc/{}/fd/{}", std::process::id(), i32::MAX)),
            claude_session: None,
            claude_resume: Some(record.native_session_id.clone()),
            codex_title: None,
            thread: None,
        };
        self.herdr
            .harness_command(launch, resources, harness_arguments)
            .map(|_| ())
            .map_err(|failure| match failure {
                SpawnFailure::Refused(cause) => refused(format!("{cause:?}")),
                SpawnFailure::Unattempted(_) => refused("CommandUnformed".into()),
            })
    }

    fn verified_resume(
        &self,
        record: &RestartRecord,
        pane: &HerdrPaneBinding,
        model: &str,
    ) -> Result<SeatBirth, RestartFailure> {
        use crate::binding::{ReadsProcessIdentity, VerifiesAdoptedSeat};
        let refused = |category: &str| RestartFailure::ResumeRefused(category.into());
        self.herdr
            .await_pane_rest(pane)
            .map_err(|_| refused("NotReady"))?;
        let snapshot = self
            .herdr
            .session_snapshot(&pane.herdr_session_name)
            .ok_or_else(|| refused("HerdrUnreadable"))?;
        let field = |agent: &serde_json::Value, key: &str| {
            agent
                .get(key)
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned)
        };
        let agent = snapshot
            .pointer("/result/snapshot/agents")
            .and_then(serde_json::Value::as_array)
            .and_then(|agents| {
                agents.iter().find(|agent| {
                    field(agent, "pane_id").as_deref() == Some(pane.herdr_pane_id.as_str())
                        && field(agent, "terminal_id").as_deref()
                            == Some(pane.herdr_terminal_id.as_str())
                        && field(agent, "agent").as_deref() == Some("claude")
                })
            })
            .cloned()
            .ok_or_else(|| refused("PaneAbsent"))?;
        if agent
            .pointer("/agent_session/value")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|reported| reported != record.native_session_id)
        {
            return Err(RestartFailure::BindMismatch);
        }
        let route = HerdrRoute {
            herdr_session_name: pane.herdr_session_name.clone(),
            herdr_agent_name: field(&agent, "name")
                .unwrap_or_else(|| pane.herdr_agent_name.clone()),
            herdr_pane_id: pane.herdr_pane_id.clone(),
            herdr_terminal_id: pane.herdr_terminal_id.clone(),
        };
        let process = self
            .herdr
            .harness_process(&route, &record.native_session_id)
            .ok_or_else(|| refused("ProcessUnidentified"))?;
        let process_id = u32::try_from(process.pid).map_err(|_| refused("ProcessUnidentified"))?;
        let process_identity =
            ProcessIdentity::of(process_id).ok_or_else(|| refused("ProcessUnidentified"))?;
        let working_directory = std::fs::canonicalize(format!("/proc/{process_id}/cwd"))
            .ok()
            .and_then(|directory| directory.to_str().map(str::to_owned))
            .ok_or_else(|| refused("ProcessUnidentified"))?;
        let binding = FlowBinding {
            flow_id: record.flow_id.clone(),
            aspect: record.address.aspect.clone(),
            model_name: model.to_owned(),
            harness_kind: HarnessKind::Claude,
            native_session_id: record.native_session_id.clone(),
            herdr_workspace_id: pane.herdr_workspace_id.clone(),
            herdr_pane_id: pane.herdr_pane_id.clone(),
            herdr_tab_id: field(&agent, "tab_id").unwrap_or_default(),
            herdr_terminal_id: pane.herdr_terminal_id.clone(),
            herdr_agent_name: route.herdr_agent_name.clone(),
            process_identity,
            working_directory,
        };
        let node = self
            .verified_seat_node(&pane.herdr_session_name, &record.address, &binding)
            .map_err(|reason| match reason {
                FlowBindingRefusalReason::SessionMismatch
                | FlowBindingRefusalReason::NativeIdentityMismatch => RestartFailure::BindMismatch,
                other => RestartFailure::ResumeRefused(format!("{other:?}")),
            })?;
        let HerdrRouteSelection::Available(herdr_route) = node.herdr_route_selection else {
            return Err(refused("RouteUnavailable"));
        };
        Ok(SeatBirth {
            flow_id: record.flow_id.clone(),
            herdr_route,
            process,
        })
    }

    fn bound(&self, record: &RestartRecord, birth: SeatBirth) -> Response {
        use crate::generated::operation::Record_Data;
        let flow_id = record.flow_id.clone();
        if self.perform(Operation::Record(Record_Data::Rebound(birth))) != Outcome::Recorded {
            // The record stays Spawning: the next start binds by observation.
            return StoreError::StateInvariant.refused();
        }
        eprintln!("flow-nexus: flow {flow_id} restarted as its own session");
        Response::Restarted(flow_id)
    }

    fn abandoned(
        &self,
        record: RestartRecord,
        pane: HerdrPaneBinding,
        failure: RestartFailure,
    ) -> Response {
        eprintln!(
            "flow-nexus: restart of flow {} did not bind: {failure:?}",
            record.flow_id
        );
        if self.perform(Operation::Discard(pane.clone())) == Outcome::Closed {
            self.failed(record, failure)
        } else {
            self.interrupted(record, Some(pane))
        }
    }

    fn written(&self, record: &RestartRecord) -> bool {
        use crate::generated::operation::Record_Data;
        self.perform(Operation::Record(Record_Data::Restart(record.clone()))) == Outcome::Recorded
    }

    fn failed(&self, record: RestartRecord, failure: RestartFailure) -> Response {
        let flow_id = record.flow_id.clone();
        let failed = RestartRecord {
            restart_phase: RestartPhase::Failed(failure.clone()),
            ..record
        };
        if !self.written(&failed) {
            return StoreError::StateInvariant.refused();
        }
        failure.answer(&flow_id)
    }

    fn interrupted(&self, record: RestartRecord, pane: Option<HerdrPaneBinding>) -> Response {
        let flow_id = record.flow_id.clone();
        eprintln!("flow-nexus: restart of flow {flow_id} is interrupted; only a Stop clears it");
        let interrupted = RestartRecord {
            restart_phase: RestartPhase::Interrupted(pane),
            ..record
        };
        if !self.written(&interrupted) {
            return StoreError::StateInvariant.refused();
        }
        Response::RestartRefused(RestartRefusal::Interrupted(flow_id))
    }

    fn actual_model(&self, flow_id: &str) -> Option<String> {
        use crate::store::ReadsFlowRoles;
        match self.store.role(flow_id) {
            Ok(Some(role)) => Some(role.model_name),
            _ => self.last_profile(flow_id).map(|profile| profile.model_name),
        }
    }

    fn last_profile(&self, flow_id: &str) -> Option<signal_flow::LaunchProfile> {
        use crate::store::ReadsLaunchAttempt;
        let launch_request_id = self.store.launch_requests_bound_to(flow_id).ok()?.pop()?;
        self.store
            .composable_launch_profile(&launch_request_id)
            .ok()
            .flatten()
    }

    fn quiesced(&self, record: &RestartRecord, pane: Option<&HerdrPaneBinding>) -> bool {
        use crate::delivery::lease::LeasesPanes;
        let Some(node) = self.seat_node(record, record.old_seat.herdr_route.clone()) else {
            return false;
        };
        let sessions: BTreeSet<String> =
            std::iter::once(record.old_seat.herdr_route.herdr_session_name.clone())
                .chain(pane.map(|pane| pane.herdr_session_name.clone()))
                .collect();
        for herdr_session_name in &sessions {
            let Some(holding) = self
                .herdr
                .panes_holding(herdr_session_name, &record.native_session_id)
            else {
                return false;
            };
            for route in holding {
                let Some(held) = self.seat_node(record, route.clone()) else {
                    return false;
                };
                let closed = {
                    let _lease = self.pane_leases.hold(&route);
                    self.perform(Operation::Close(held))
                };
                if closed != Outcome::Closed {
                    return false;
                }
            }
        }
        if let Some(pane) = pane
            && self.perform(Operation::Discard(pane.clone())) != Outcome::Closed
        {
            return false;
        }
        sessions.iter().all(|herdr_session_name| {
            self.herdr.holds_native_session(herdr_session_name, &node) == Some(false)
        })
    }

    fn seat_node(&self, record: &RestartRecord, route: HerdrRoute) -> Option<FlowNode> {
        let node = self.store.flow_node(&record.flow_id).ok()??;
        Some(FlowNode {
            session_id: record.native_session_id.clone(),
            herdr_route_selection: HerdrRouteSelection::Available(route),
            ..node
        })
    }
}

impl RestartsFlows for RunningNexus {
    fn restart(&self, flow_id: &str, origin: &Origin) -> Response {
        let closed = {
            let _turn = self
                .dispatch_gate
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            self.begun(flow_id, origin)
                .and_then(|prepared| self.closed_old(prepared))
        };
        let response = match closed {
            Ok(closed) => self.resumed(closed),
            Err(response) => return *response,
        };
        // The queue that waited goes through the ordinary delivery custody.
        if let Response::Restarted(_) = &response
            && let Ok(Some(metaflow)) =
                crate::store::metaflow::SleepsMetaflows::metaflow_awake_as(&self.store, flow_id)
        {
            crate::waking::SleepsAndWakesSeats::drain_awake(self, &metaflow.address);
        }
        response
    }

    fn recover_restarts(&self) {
        let records = {
            let _turn = self
                .dispatch_gate
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            self.store.restart_records().unwrap_or_default()
        };
        for record in records {
            let flow_id = record.flow_id.clone();
            let response = match record.restart_phase.clone() {
                // Composed before the old pane is touched again: refused,
                // the record is Failed and the old harness runs on.
                RestartPhase::Closing => {
                    let closed = {
                        let _turn = self
                            .dispatch_gate
                            .lock()
                            .unwrap_or_else(|poisoned| poisoned.into_inner());
                        self.taken_up(record)
                            .and_then(|prepared| self.closed_old(prepared))
                    };
                    match closed {
                        Ok(closed) => self.resumed(closed),
                        Err(response) => *response,
                    }
                }
                RestartPhase::Closed => {
                    let prepared = {
                        let _turn = self
                            .dispatch_gate
                            .lock()
                            .unwrap_or_else(|poisoned| poisoned.into_inner());
                        self.taken_up(record)
                    };
                    match prepared {
                        Ok(prepared) => self.resumed(prepared),
                        Err(response) => *response,
                    }
                }
                // No pane was recorded: whether Herdr made one is unproven.
                RestartPhase::Opening => {
                    let _turn = self
                        .dispatch_gate
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner());
                    self.interrupted(record, None)
                }
                // The pane was recorded: it binds only to what it shows.
                RestartPhase::Spawning(pane) => {
                    let verified = self
                        .actual_model(&flow_id)
                        .ok_or(RestartFailure::ResumeRefused("ModelUnknown".into()))
                        .and_then(|model| self.verified_resume(&record, &pane, &model));
                    let _turn = self
                        .dispatch_gate
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner());
                    match verified {
                        Ok(birth) => self.bound(&record, birth),
                        Err(_) => self.interrupted(record, Some(pane)),
                    }
                }
                RestartPhase::Failed(_) | RestartPhase::Interrupted(_) => continue,
            };
            eprintln!("flow-nexus: restart of flow {flow_id} taken up at start: {response:?}");
        }
    }

    fn stop_restarting(&self, record: RestartRecord) -> Response {
        let pane = match &record.restart_phase {
            RestartPhase::Failed(_) => None,
            RestartPhase::Interrupted(Some(pane)) => Some(pane.clone()),
            RestartPhase::Interrupted(None) => {
                return Response::StopRejected(StopRejection::SpawnUnproven);
            }
            RestartPhase::Closing
            | RestartPhase::Closed
            | RestartPhase::Opening
            | RestartPhase::Spawning(_) => {
                return Response::StopRejected(StopRejection::Restarting);
            }
        };
        if !self.quiesced(&record, pane.as_ref()) {
            return Response::StopRejected(match pane {
                Some(_) => StopRejection::SpawnUnproven,
                None => StopRejection::CloseRefused,
            });
        }
        let flow_id = record.flow_id.clone();
        let stopped = RestartRecord {
            restart_phase: RestartPhase::Failed(RestartFailure::Stopped),
            ..record
        };
        if !self.written(&stopped) {
            return Response::StopRejected(StopRejection::PersistenceRefused);
        }
        Response::Stopped(flow_id)
    }
}

impl FencesSeatEvents for RunningNexus {
    fn seat_witness(&self, flow_id: &str) -> Option<SeatWitness> {
        let node = self.store.flow_node(flow_id).ok()??;
        let HerdrRouteSelection::Available(route) = node.herdr_route_selection else {
            return None;
        };
        let birth = self.store.seat_birth(flow_id).ok()?;
        Some(SeatWitness {
            route,
            birth: birth.map(|birth| birth.process),
        })
    }

    fn report_acts(&self, flow_id: &str, caller: Option<CallerProcess>) -> bool {
        if !matches!(self.store.restart_record(flow_id), Ok(None)) {
            return false;
        }
        let birth = match self.store.seat_birth(flow_id) {
            Ok(None) => return true,
            Ok(Some(birth)) => birth,
            Err(_) => return false,
        };
        let route = &birth.herdr_route;
        // The birth names the seat only while the flow still runs there.
        let current = self.seat_witness(flow_id).is_some_and(|witness| {
            witness.route.herdr_session_name == route.herdr_session_name
                && witness.route.herdr_pane_id == route.herdr_pane_id
                && witness.route.herdr_terminal_id == route.herdr_terminal_id
        });
        let Some(caller) = caller.filter(|_| current) else {
            return false;
        };
        let Some(process) = FlowStore::process_identity_for_pid(caller.process_id) else {
            return false;
        };
        FlowStore::process_lineage(&process).contains(&birth.process)
            && caller.caller_pane(&self.herdr)
                == Some(CallerPane {
                    herdr_session_name: route.herdr_session_name.clone(),
                    herdr_pane_id: route.herdr_pane_id.clone(),
                })
    }

    fn witness_holds(&self, flow_id: &str, witness: Option<&SeatWitness>) -> bool {
        if !matches!(self.store.restart_record(flow_id), Ok(None)) {
            return false;
        }
        witness.is_none_or(|witness| self.seat_witness(flow_id).as_ref() == Some(witness))
    }
}
