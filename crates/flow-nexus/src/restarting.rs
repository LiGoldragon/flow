//! Restart: a live flow's harness is closed and its own native session is
//! resumed in a new pane, the FlowId, session, Model and transcript kept,
//! with its Address's current launch configuration, modules and hooks aimed
//! at this Nexus, and no prompt typed, whoever asks. A Claude flow resting
//! is restarted at once; a working one answers RestartQueued, the restart
//! recorded as an ordered replacement of its Address and carried out at
//! its next idle event, as an ordered refresh is, its background work
//! ended (`restart_at_idle_event`); an Interrupt forces that event. The
//! last flow of an Asleep metaflow has no harness to close: Restart answers
//! Asleep and does nothing. A past flow is Unsupported.
//!
//! The RestartRecord is the restart's lock on the flow's Address, durable
//! in Memory and read by every admission: while it stands, a Send queues,
//! nothing is handed to the seat, no Stop the seat reports settles it, and
//! no end of the flow is recorded. Before Closing is written the resume is
//! composed and validated in full: its launch composed, its root and
//! harness flags read, its title, settings and command line built as the
//! spawn builds them. A refusal there closes nothing and writes nothing:
//! the old harness runs on and its record stays as it was.
//!
//! A harness's exit is proven only positively: `/proc/<pid>` not found, a
//! parsed start time other than its recorded birth, or a parsed state of Z
//! or X. Anything else read, or not read, is unproven, and nothing proceeds
//! on it. A first restart names its old harness before any write: the one
//! the old pane shows, else the birth a restart recorded for that route.
//! None is refused `ExitUnwitnessed`; with the old pane gone, a harness not
//! proven exited is refused `ExitUnproven`. Each phase is
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
//! The record's old seat is the seat a reopen reconciles. Before a run's
//! pane is abandoned, Failed or Interrupted, the old seat is rewritten to
//! that pane's route and the harness Flow saw running the session there,
//! `Process.{ pid started }`, or none when Flow saw none; the Stop of an
//! Interrupted restart rewrites it to the pane it quiesces, as it saw it.
//!
//! A Failed restart reopens the same session by itself while its runs number
//! no more than `AUTOMATIC_REOPENS`, every run counted in its Attempts, the
//! owner's retries included. It reopens only once the seat is proven
//! quiescent, under the gate: the record still the Failed one, the old
//! seat's harness recorded and proven exited by its birth, the old seat's
//! pane gone, and no pane of the old seat's, the flow's or the resume's
//! Herdr session holding the session. A pid, or a pane's absence, proves no
//! exit: a harness can outlive its pane. A record whose harness was never
//! seen (`ExitUnwitnessed`) or is not proven exited (`ExitUnproven`) stays
//! Failed, logged so, its session, queue and Address held; the owner's
//! retry is refused `ResumeRefused` with that word, unless the old seat's
//! pane stands for the retry to close; the owner's Stop and Retire stand.
//! Closed is then written with the run counted, and the resume runs again
//! from Opening, with the launch already composed when the failure was this
//! Nexus's own, composed again when an earlier Nexus left it. A Failed
//! record is never written while a spawn may be in flight, and Stopped, the
//! owner's, is never reopened.
//!
//! The fence: once a restart has bound a seat, a harness event acts only
//! when it names that seat's harness by its birth (`Process.{ pid started }`,
//! carried in the Report its hook sends) while the flow still runs in that
//! seat's pane; an Observe's Gone acts only for the route and birth it was
//! opened on. A late event of the seat the restart replaced, or one naming
//! no harness, is recorded and acts on nothing. An event names where it came
//! from in its own data; the caller is not read.

use crate::RunningNexus;
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
    FlowNode, HarnessKind, HerdrPaneBinding, HerdrRoute, HerdrRouteSelection, Process, Refusal,
    Response, RestartRefusal, ResumeRefused_Data, StopRejection,
};
use std::collections::BTreeSet;
use std::time::Duration;

/// How many runs, beyond its first, a restart makes by itself after a
/// failure: one. A constant, since a configured bound would need a
/// configuration family of its own.
const AUTOMATIC_REOPENS: i64 = 1;

/// How long the harness a failed pane showed is given to exit once its pane
/// is discarded, before the session is opened again.
const SPAWNED_EXIT_WAIT: Duration = Duration::from_secs(3);

/// Why a Failed restart's old seat does not prove its harness exited; its
/// name is the category a refused retry answers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MissingExit {
    /// No harness of the seat was ever seen running the session.
    ExitUnwitnessed,
    /// The harness seen there is not proven exited by its birth.
    ExitUnproven,
}

/// The route of a pane a restart created.
trait RoutesCreatedPane {
    fn route(&self) -> HerdrRoute;
}

impl RoutesCreatedPane for HerdrPaneBinding {
    fn route(&self) -> HerdrRoute {
        HerdrRoute {
            herdr_session_name: self.herdr_session_name.clone(),
            herdr_agent_name: self.herdr_agent_name.clone(),
            herdr_pane_id: self.herdr_pane_id.clone(),
            herdr_terminal_id: self.herdr_terminal_id.clone(),
        }
    }
}

/// What an Observe was opened on: the route the flow ran in, and the birth
/// of the seat a restart bound there, when one did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SeatWitness {
    pub route: HerdrRoute,
    pub birth: Option<Process>,
}

/// Restart as the Nexus serves it.
pub trait RestartsFlows {
    /// Restarts `flow_id`, whoever asks. Runs off the dispatch gate and
    /// takes it for each step, so a Send arriving meanwhile is admitted and
    /// queues.
    fn restart(&self, flow_id: &str) -> Response;
    /// Restarts `flow_id` at its idle event, whatever its seat shows: the
    /// restart a working seat was answered RestartQueued for, its
    /// background work ended by the closing of its harness.
    fn restart_at_idle_event(&self, flow_id: &str) -> Response;
    /// Takes up every restart an earlier Nexus left: Closing and Closed are
    /// composed again first, then Closing is reconciled against the old
    /// pane and Closed resumes; Opening is Interrupted, and
    /// Spawning binds the seat its pane shows or is Interrupted. Failed
    /// reopens by itself under the bound once its old seat's recorded
    /// harness is proven exited by its birth and its seat quiescent, and
    /// otherwise stays, as Interrupted does.
    fn recover_restarts(&self);
    /// The owner's Stop of a flow under a restart record, under the gate:
    /// every pane Herdr shows holding the session is closed, and the
    /// restart's own pane, then the record is Failed.Stopped, the FlowId,
    /// session and queue kept, and an Interrupted spawn's pane, with the
    /// harness Flow saw there, its old seat. A restart step running is
    /// Restarting; an Interrupted spawn not proven quiesced is
    /// SpawnUnproven.
    fn stop_restarting(&self, record: RestartRecord) -> Response;
}

/// The fence on a flow's harness events.
pub(crate) trait FencesSeatEvents {
    /// The route the flow runs in and the birth of the seat a restart bound.
    fn seat_witness(&self, flow_id: &str) -> Option<SeatWitness>;
    /// Whether a harness event of `flow_id` that names the harness born as
    /// `birth` may act: never while a restart holds the flow; for a seat a
    /// restart bound, only when it names that seat's harness and the flow
    /// still runs in that seat's pane; otherwise as before any restart.
    fn report_acts(&self, flow_id: &str, birth: Option<&Process>) -> bool;
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
    /// names it after `--session-id` or `--resume` (Claude) or `resume` (a
    /// Codex TUI attached to its thread), or Claude's own record of the
    /// process names it.
    fn harness_process(&self, route: &HerdrRoute, session: &str) -> Option<Process>;
    /// Every pane of the Herdr session whose agent reports `session`; None
    /// when Herdr cannot be read.
    fn panes_holding(&self, herdr_session_name: &str, session: &str) -> Option<Vec<HerdrRoute>>;
}

/// What the kernel shows of a process Flow recorded by its birth. Only
/// `Exited` lets anything proceed on that process's end.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ExitProof {
    /// The process is proven ended.
    Exited(ExitEvidence),
    /// Born when the record says and in a live state: running.
    Running,
    /// Nothing read proves the process ended, nor that it runs.
    Unproven(ExitDoubt),
}

/// What proves a recorded process ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ExitEvidence {
    /// `/proc/<pid>` is not found.
    Gone,
    /// The process at its pid was born at another time.
    Reborn,
    /// The process at its pid, born when the record says, is a zombie (Z)
    /// or dead (X).
    Defunct,
}

/// Why what was read proves no exit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ExitDoubt {
    /// The pid is not positive, beyond the kernel's pid_max, or the
    /// pid_max is unread.
    InvalidPid,
    /// The recorded birth is not a start time the kernel gives.
    BirthUnparsed,
    /// `/proc/<pid>/stat` refused for permission.
    PermissionDenied,
    /// `/proc/<pid>/stat` failed otherwise than not found.
    Unreadable,
    /// The stat names no state after its command.
    Malformed,
    /// The stat's start time does not parse.
    StartUnparsed,
}

/// The kernel's view of processes, read through `/proc` in production and
/// fed case by case in tests.
pub(crate) trait ReadsProcessStatus {
    /// `/proc/<pid>/stat` whole, or the error reading it gave.
    fn process_stat(&self, pid: u32) -> std::io::Result<String>;
    /// The kernel's pid_max; None when unread.
    fn pid_max(&self) -> Option<u32>;
}

/// The kernel's `/proc`.
pub(crate) struct ProcFs;

impl ReadsProcessStatus for ProcFs {
    fn process_stat(&self, pid: u32) -> std::io::Result<String> {
        std::fs::read_to_string(format!("/proc/{pid}/stat"))
    }

    fn pid_max(&self) -> Option<u32> {
        std::fs::read_to_string("/proc/sys/kernel/pid_max")
            .ok()?
            .trim()
            .parse()
            .ok()
    }
}

/// Whether a process the kernel knew has ended, proven only positively.
pub(crate) trait ProvesExit {
    /// Exited only when `/proc/<pid>` is not found, its parsed start time
    /// differs from the recorded birth, or its parsed state is Z or X.
    fn exit_proof(&self, kernel: &impl ReadsProcessStatus) -> ExitProof;
    /// The proof now, or, while the process runs, the proof read once its
    /// exit is announced, waited on as its pidfd for at most `wait`.
    fn exit_proof_within(&self, kernel: &impl ReadsProcessStatus, wait: Duration) -> ExitProof;
}

impl ProvesExit for Process {
    fn exit_proof_within(&self, kernel: &impl ReadsProcessStatus, wait: Duration) -> ExitProof {
        use crate::event_wait::{AwaitsDescriptors, OpensExitDescriptor};
        use std::os::fd::AsFd;
        let proof = self.exit_proof(kernel);
        if !matches!(proof, ExitProof::Running) || wait.is_zero() {
            return proof;
        }
        let Ok(pid) = u32::try_from(self.pid.0) else {
            return proof;
        };
        let Ok(exit) = pid.exit_descriptor() else {
            // The pid is gone already, or no pidfd is given: read again.
            return self.exit_proof(kernel);
        };
        // The pidfd names the recorded process only if the proof still
        // reads it running once the pidfd is open.
        let proof = self.exit_proof(kernel);
        if !matches!(proof, ExitProof::Running) {
            return proof;
        }
        let _ = [exit.as_fd()].ready(Some(std::time::Instant::now() + wait));
        self.exit_proof(kernel)
    }

    fn exit_proof(&self, kernel: &impl ReadsProcessStatus) -> ExitProof {
        let Some(pid) = u32::try_from(self.pid.0)
            .ok()
            .filter(|pid| *pid > 0)
            .filter(|pid| kernel.pid_max().is_some_and(|pid_max| *pid <= pid_max))
        else {
            return ExitProof::Unproven(ExitDoubt::InvalidPid);
        };
        if self.started < flow_ethos::generated::library::Started(0) {
            return ExitProof::Unproven(ExitDoubt::BirthUnparsed);
        }
        let stat = match kernel.process_stat(pid) {
            Ok(stat) => stat,
            Err(error) => {
                return match error.kind() {
                    std::io::ErrorKind::NotFound => ExitProof::Exited(ExitEvidence::Gone),
                    std::io::ErrorKind::PermissionDenied => {
                        ExitProof::Unproven(ExitDoubt::PermissionDenied)
                    }
                    _ => ExitProof::Unproven(ExitDoubt::Unreadable),
                };
            }
        };
        let Some((_, fields)) = stat.rsplit_once(") ") else {
            return ExitProof::Unproven(ExitDoubt::Malformed);
        };
        let mut fields = fields.split_whitespace();
        let Some(state) = fields.next().filter(|state| state.chars().count() == 1) else {
            return ExitProof::Unproven(ExitDoubt::Malformed);
        };
        let Some(started) = fields.nth(18).and_then(|field| field.parse::<u64>().ok()) else {
            return ExitProof::Unproven(ExitDoubt::StartUnparsed);
        };
        if i64::try_from(started) != Ok(self.started.0) {
            ExitProof::Exited(ExitEvidence::Reborn)
        } else if matches!(state, "Z" | "X") {
            ExitProof::Exited(ExitEvidence::Defunct)
        } else {
            ExitProof::Running
        }
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
                    (pair[0] == "--session-id" || pair[0] == "--resume" || pair[0] == "resume")
                        && pair[1] == session
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
                        herdr_session_name: signal_flow::HerdrSessionName(
                            herdr_session_name.to_owned(),
                        ),
                        herdr_agent_name: signal_flow::HerdrAgentName(
                            field(agent, "name").unwrap_or_default(),
                        ),
                        herdr_pane_id: signal_flow::HerdrPaneId(field(agent, "pane_id")?),
                        herdr_terminal_id: signal_flow::HerdrTerminalId(field(
                            agent,
                            "terminal_id",
                        )?),
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
        let snapshot = self.session_snapshot(&pane.herdr_session_name.0)?;
        let shown = |list: &str| {
            snapshot
                .pointer(list)
                .and_then(serde_json::Value::as_array)
                .is_some_and(|entries| {
                    entries.iter().any(|entry| {
                        entry.get("pane_id").and_then(serde_json::Value::as_str)
                            == Some(pane.herdr_pane_id.0.as_str())
                            && entry.get("terminal_id").and_then(serde_json::Value::as_str)
                                == Some(pane.herdr_terminal_id.0.as_str())
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
                pane.herdr_session_name.0.as_str(),
                "pane",
                "close",
                pane.herdr_pane_id.0.as_str(),
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
/// before it touches the old seat. A reopen after a failure runs the same
/// launch again.
#[derive(Clone)]
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

/// A run not bound, its pane shown gone, and the record written Failed, its
/// old seat that pane and the harness it showed: the launch it ran and the
/// failure's answer.
struct FailedRun {
    failed: RestartRecord,
    resume_launch: ResumeLaunch,
    response: Response,
}

/// How one run of a resume ended.
enum Resumption {
    /// Bound, Interrupted, or refused: the answer.
    Answered(Box<Response>),
    Failed(Box<FailedRun>),
}

/// What became of a Failed restart's automatic reopen.
enum Reopening {
    /// Closed written with the run counted: the resume runs again.
    Reopened(Box<PreparedRestart>),
    /// Not taken, the record as it was: Stopped, the bound spent, the record
    /// no longer the Failed one, or the seat not proven quiescent.
    Kept,
    /// The resume refused again before any pane: Failed, the run counted.
    Refused(Box<Response>),
}

/// The steps of one restart, each run under the gate unless it says not.
trait RestartsInSteps {
    /// The checks, the resume composed and validated, then Closing written;
    /// a retry of a Failed restart is written Closing again, so its old
    /// harness is reconciled first. A refusal writes nothing.
    /// At an idle event the seat is restarted whatever it shows; asked
    /// otherwise, a working seat's restart is ordered for its next idle
    /// event and answered RestartQueued.
    fn begun(&self, flow_id: &str, at_idle_event: bool) -> Result<PreparedRestart, Box<Response>>;
    /// A working seat's restart ordered for its next idle event, answered
    /// RestartQueued; Busy when another replacement is ordered for its
    /// Address.
    fn queue_restart(&self, address: &signal_flow::Address, flow_id: &str) -> Response;
    /// The rest of a restart once its old harness is closed, or its
    /// refusal: the seat resumed, and its waiting queue handed over.
    fn restarted_after(
        &self,
        flow_id: &str,
        closed: Result<PreparedRestart, Box<Response>>,
    ) -> Response;
    /// A restart another Nexus left at Closing or Closed, its resume
    /// composed and validated again; refused, the record is Failed and
    /// nothing is closed.
    fn taken_up(&self, record: RestartRecord) -> Result<PreparedRestart, Box<Response>>;
    /// The old pane closed, or found gone, and its harness proven exited:
    /// Closed written. Anything else is Failed.
    fn closed_old(&self, prepared: PreparedRestart) -> Result<PreparedRestart, Box<Response>>;
    /// From Closed: each run resumed, and reopened by itself after a
    /// failure while the bound and the seat allow.
    fn resumed(&self, prepared: PreparedRestart) -> Response;
    /// One run from Closed: Opening written, the pane opened and recorded
    /// Spawning, the harness resumed with the prepared launch and bound off
    /// the gate, then bound or abandoned under it.
    fn resumed_once(&self, prepared: PreparedRestart) -> Resumption;
    /// A Failed restart opened again by itself: under the bound, its old
    /// seat's harness recorded and exited within `patience`, the record
    /// unchanged and the seat proven quiescent, Closed written with the run
    /// counted. `resume_launch` is the launch the failed run held; None, it
    /// is composed again.
    fn reopened(
        &self,
        failed: RestartRecord,
        resume_launch: Option<ResumeLaunch>,
        patience: Duration,
    ) -> Reopening;
    /// Whether the old seat's harness is recorded and, by its birth, exited
    /// now or within `patience`. Reads only.
    fn exit_witness(&self, record: &RestartRecord, patience: Duration) -> Result<(), MissingExit>;
    /// Whether, as Herdr and the kernel show it now, nothing holds the
    /// session: the old seat's harness recorded and exited, its pane gone,
    /// and no pane of the old seat's, the flow's or `herdr_session_name`
    /// Herdr session holding the session. Reads only.
    fn proven_quiescent(&self, record: &RestartRecord, herdr_session_name: &str) -> bool;
    /// The Herdr sessions a pane holding the session is looked for in: the
    /// old seat's, and the flow's own route's.
    fn seat_sessions(&self, record: &RestartRecord) -> BTreeSet<String>;
    /// The Herdr session a resume opens its pane in: the configured one,
    /// as every launch's.
    fn resume_session_name(&self, record: &RestartRecord) -> String;
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
    /// A resume that did not bind: its old seat rewritten to its pane and
    /// `spawned`, the harness that pane showed, then the pane discarded and
    /// the record Failed, or, the pane not shown gone, Interrupted.
    fn abandoned(
        &self,
        record: RestartRecord,
        pane: HerdrPaneBinding,
        failure: RestartFailure,
        resume_launch: ResumeLaunch,
        spawned: Option<Process>,
    ) -> Resumption;
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
                RestartRefusal::CloseRefused(signal_flow::FlowId(flow_id))
            }
            RestartFailure::ResumeRefused(category) => {
                RestartRefusal::ResumeRefused(ResumeRefused_Data {
                    flow_id: signal_flow::FlowId(flow_id),
                    string: category.clone(),
                })
            }
            RestartFailure::BindMismatch => RestartRefusal::ResumeRefused(ResumeRefused_Data {
                flow_id: signal_flow::FlowId(flow_id),
                string: "BindMismatch".into(),
            }),
            RestartFailure::Stopped => RestartRefusal::Busy(signal_flow::FlowId(flow_id)),
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
    fn queue_restart(&self, address: &signal_flow::Address, flow_id: &str) -> Response {
        use crate::store::ordered_replacement::{
            OrderedReplacement, OrdersReplacements, ReplacementOrder,
        };
        let flow = signal_flow::FlowId(flow_id.to_owned());
        match self.store.ordered_replacement(address) {
            Ok(Some(order))
                if order.predecessor == flow_id && order.order == ReplacementOrder::Restart =>
            {
                return Response::RestartQueued(flow);
            }
            Ok(Some(_)) => return Response::RestartRefused(RestartRefusal::Busy(flow)),
            Ok(None) => {}
            Err(error) => return error.refused(),
        }
        if let Err(error) = self.store.order_replacement(OrderedReplacement {
            address: address.clone(),
            predecessor: flow_id.to_owned(),
            order: ReplacementOrder::Restart,
        }) {
            return error.refused();
        }
        eprintln!("flow-nexus: flow {flow_id} works; its restart waits for its idle event");
        Response::RestartQueued(flow)
    }

    fn restarted_after(
        &self,
        flow_id: &str,
        closed: Result<PreparedRestart, Box<Response>>,
    ) -> Response {
        let response = match closed {
            Ok(closed) => self.resumed(closed),
            Err(response) => return *response,
        };
        // The queue that waited goes through the ordinary delivery custody.
        if let Response::Restarted(_) = &response
            && let Ok(Some(metaflow)) =
                crate::store::metaflow::SleepsMetaflows::metaflow_awake_as(&*self.store, flow_id)
        {
            crate::waking::SleepsAndWakesSeats::drain_awake(self, &metaflow.address);
        }
        response
    }

    fn begun(&self, flow_id: &str, at_idle_event: bool) -> Result<PreparedRestart, Box<Response>> {
        use crate::store::NamesConfiguredModel;
        use crate::store::address_hold::HoldsLaunchAddress;
        use crate::store::metaflow::SleepsMetaflows;
        use crate::store::queue_delivery::AccountsQueueDelivery;
        use crate::waking::{HoldsWakingMetaflows, ReadsClaudeDelivery, WakePolicy};
        let refused = |refusal: fn(signal_flow::FlowId) -> RestartRefusal| {
            Err(Box::new(Response::RestartRefused(refusal(
                signal_flow::FlowId(flow_id.to_owned()),
            ))))
        };
        let node = match self.store.flow_node(flow_id) {
            Ok(Some(node)) => node,
            Ok(None) => return refused(RestartRefusal::UnknownFlow),
            Err(error) => return Err(Box::new(error.refused())),
        };
        match self.store.restart_record(flow_id) {
            Ok(None) => {}
            Ok(Some(record)) => {
                return match record.restart_phase {
                    RestartPhase::Failed(_) => {
                        // The old seat's harness proven exited by its birth,
                        // or its pane standing for the retry to close and
                        // prove; refused, the Failed record stays as it was.
                        if let Err(missing) = self.exit_witness(&record, Duration::ZERO) {
                            let pane_stands = missing == MissingExit::ExitUnproven
                                && self
                                    .seat_node(&record, record.old_seat.herdr_route.clone())
                                    .is_some_and(|old| {
                                        self.herdr.pane_presence(&old) != PanePresence::Absent
                                    });
                            if !pane_stands {
                                eprintln!(
                                    "flow-nexus: retry of flow {flow_id}'s restart refused: {missing:?}"
                                );
                                return Err(Box::new(
                                    RestartFailure::ResumeRefused(format!("{missing:?}"))
                                        .answer(flow_id),
                                ));
                            }
                        }
                        let retried = RestartRecord {
                            restart_phase: RestartPhase::Closing,
                            attempts: meta_signal_flow::Attempts(record.attempts.0 + 1),
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
        // The last flow of an Asleep metaflow has no harness to close.
        if !node.flow_lifecycle.is_live() {
            use crate::store::ManagesMetaflowState;
            let asleep = match self.store.list_metaflows() {
                Ok(metaflows) => metaflows.iter().any(|metaflow| {
                    metaflow.permaflow_state == flow_ethos::PermaflowState::Asleep
                        && metaflow.past.0.last().is_some_and(|last| last.0 == flow_id)
                }),
                Err(error) => return Err(Box::new(error.refused())),
            };
            return if asleep {
                refused(RestartRefusal::Asleep)
            } else {
                refused(RestartRefusal::Unsupported)
            };
        }
        if node.session_id.0.is_empty() {
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
        // A Claude session resumes from its transcript; a Codex thread is
        // kept by its app-server.
        if node.harness_kind == HarnessKind::Claude && self.claude_transcript(&node).is_none() {
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
                        flow_id: signal_flow::FlowId(flow_id.to_owned()),
                        string: "ModelDiffers".into(),
                    }),
                )));
            }
        }
        let presence = self.herdr.pane_presence(&node);
        match presence {
            PanePresence::Unknown => return refused(RestartRefusal::CloseRefused),
            // A working seat is restarted at its next idle event: asked
            // now, the restart is ordered for it and answered RestartQueued.
            PanePresence::Present if !at_idle_event => {
                use crate::codex_turns::SettlesRestingCodexSeats;
                let resting = match node.harness_kind {
                    // A Codex seat rests as its app-server's thread does.
                    HarnessKind::Codex => {
                        !self.codex_in_turn(flow_id) && self.codex_resting(flow_id)
                    }
                    HarnessKind::Claude => {
                        (match self.herdr.pane_agent(&node) {
                            PaneAgent::Present { agent_state, .. } => Self::rests_at(&agent_state),
                            PaneAgent::Absent | PaneAgent::Unreadable => false,
                        }) && self.claude_main_turn_ended(&node)
                    }
                };
                if !resting {
                    return Err(Box::new(self.queue_restart(&address, flow_id)));
                }
            }
            PanePresence::Present => {}
            PanePresence::Bare | PanePresence::Absent => {}
        }
        // The harness the old pane shows running the session, else the
        // birth a restart recorded for the seat at this route. None proves
        // no exit; nor does a pane's absence: a harness can outlive its
        // pane. Refused here, nothing is written and the old state stands.
        let shown = match presence {
            PanePresence::Present | PanePresence::Bare => {
                self.herdr.harness_process(&route, &node.session_id.0)
            }
            PanePresence::Absent | PanePresence::Unknown => None,
        };
        let recorded = match self.store.seat_birth(flow_id) {
            Ok(birth) => birth
                .filter(|birth| {
                    birth.herdr_route.herdr_session_name == route.herdr_session_name
                        && birth.herdr_route.herdr_pane_id == route.herdr_pane_id
                        && birth.herdr_route.herdr_terminal_id == route.herdr_terminal_id
                })
                .map(|birth| birth.process),
            Err(error) => return Err(Box::new(error.refused())),
        };
        let Some(process) = shown.or(recorded) else {
            eprintln!(
                "flow-nexus: restart of flow {flow_id} refused: {:?}: no harness of pane {} is seen or recorded",
                MissingExit::ExitUnwitnessed,
                route.herdr_pane_id.0
            );
            return Err(Box::new(
                RestartFailure::ResumeRefused(format!("{:?}", MissingExit::ExitUnwitnessed))
                    .answer(flow_id),
            ));
        };
        // With its pane gone there is nothing to close: the harness is
        // proven exited by its birth now, or the restart is refused.
        if presence == PanePresence::Absent {
            let proof = process.exit_proof(&ProcFs);
            if !matches!(proof, ExitProof::Exited(_)) {
                eprintln!(
                    "flow-nexus: restart of flow {flow_id} refused: {:?}: its pane is gone and its harness, pid {} started {}, is {proof:?}",
                    MissingExit::ExitUnproven,
                    process.pid.0,
                    process.started.0
                );
                return Err(Box::new(
                    RestartFailure::ResumeRefused(format!("{:?}", MissingExit::ExitUnproven))
                        .answer(flow_id),
                ));
            }
        }
        let process_option = Some(process);
        let record = RestartRecord {
            flow_id: signal_flow::FlowId(flow_id.to_owned()),
            native_session_id: signal_flow::NativeSessionId(node.session_id.0.clone()),
            address,
            old_seat: OldSeat {
                herdr_route: route,
                process_option,
            },
            restart_phase: RestartPhase::Closing,
            attempts: meta_signal_flow::Attempts(1),
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
        // the session, and the recorded harness proven exited by its birth;
        // a seat with no harness recorded is never Closed.
        let gone = self.herdr.pane_presence(&old) == PanePresence::Absent
            && self
                .seat_sessions(&record)
                .iter()
                .all(|herdr_session_name| {
                    self.herdr.holds_native_session(herdr_session_name, &old) == Some(false)
                })
            && record
                .old_seat
                .process_option
                .as_ref()
                .is_some_and(|process| matches!(process.exit_proof(&ProcFs), ExitProof::Exited(_)));
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
        let mut prepared = prepared;
        loop {
            let run = match self.resumed_once(prepared) {
                Resumption::Answered(response) => return *response,
                Resumption::Failed(run) => *run,
            };
            match self.reopened(run.failed, Some(run.resume_launch), SPAWNED_EXIT_WAIT) {
                Reopening::Reopened(reopened) => prepared = *reopened,
                Reopening::Kept => return run.response,
                Reopening::Refused(refused) => return *refused,
            }
        }
    }

    fn resumed_once(&self, prepared: PreparedRestart) -> Resumption {
        let PreparedRestart {
            record,
            resume_launch,
        } = prepared;
        let ResumeLaunch {
            composed_launch: launch,
            root,
            harness_arguments,
        } = resume_launch.clone();
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
                return Resumption::Answered(Box::new(StoreError::StateInvariant.refused()));
            }
            pane = match self.perform(Operation::Open(Open_Data {
                composed_launch: launch.clone(),
                root: root.clone(),
            })) {
                Outcome::Opened(pane) => pane,
                // Herdr may have made the pane before it refused: unproven.
                _ => return Resumption::Answered(Box::new(self.interrupted(opening, None))),
            };
            let spawning = RestartRecord {
                restart_phase: RestartPhase::Spawning(pane.clone()),
                ..opening
            };
            if !self.written(&spawning) {
                return Resumption::Answered(Box::new(StoreError::StateInvariant.refused()));
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
            harness_arguments: crate::generated::operation::HarnessArguments(harness_arguments),
        })) {
            Outcome::Spawned(_) => self.verified_resume(&spawning, &pane, &model.0),
            Outcome::Failed(Failed_Data::SpawnRefused(_)) => {
                Err(RestartFailure::ResumeRefused("SpawnRefused".into()))
            }
            _ => Err(RestartFailure::ResumeRefused("Spawn".into())),
        };
        // The harness the pane shows running the session, seen before the
        // pane is discarded, so its exit is proven before any reopen.
        let spawned = match verified {
            Ok(_) => None,
            Err(_) => self
                .herdr
                .harness_process(&pane.route(), &spawning.native_session_id.0),
        };
        self.herdr.release_system_prompt(&request.0);
        let _turn = self
            .dispatch_gate
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        // No other step writes a phase while this one runs: Stop answers
        // Restarting, and another Restart answers Busy.
        if !matches!(self.store.restart_record(&flow_id.0), Ok(Some(current)) if current == spawning)
        {
            return Resumption::Answered(Box::new(Response::RestartRefused(RestartRefusal::Busy(
                flow_id,
            ))));
        }
        match verified {
            Ok(birth) => Resumption::Answered(Box::new(self.bound(&spawning, birth))),
            Err(failure) => self.abandoned(spawning, pane, failure, resume_launch, spawned),
        }
    }

    fn reopened(
        &self,
        failed: RestartRecord,
        resume_launch: Option<ResumeLaunch>,
        patience: Duration,
    ) -> Reopening {
        let flow_id = failed.flow_id.clone();
        let reopens = matches!(
            &failed.restart_phase,
            RestartPhase::Failed(failure) if *failure != RestartFailure::Stopped
        ) && failed.attempts <= meta_signal_flow::Attempts(AUTOMATIC_REOPENS);
        if !reopens {
            return Reopening::Kept;
        }
        // Off the gate: the old seat's harness, as recorded, given its
        // moment to exit; never seen, nothing proves it gone.
        if let Err(missing) = self.exit_witness(&failed, patience) {
            let pane = &failed.old_seat.herdr_route.herdr_pane_id;
            match &failed.old_seat.process_option {
                Some(process) => eprintln!(
                    "flow-nexus: restart of flow {flow_id} stays Failed: {missing:?}: the harness of pane {pane}, pid {} started {}, is not proven exited",
                    process.pid.0,
                    process.started.0,
                    flow_id = flow_id.0,
                    pane = pane.0
                ),
                None => eprintln!(
                    "flow-nexus: restart of flow {flow_id} stays Failed: {missing:?}: no harness of pane {pane} was seen running the session",
                    flow_id = flow_id.0,
                    pane = pane.0
                ),
            }
            return Reopening::Kept;
        }
        let _turn = self
            .dispatch_gate
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        // A Stop or a retry may have acted meanwhile.
        if !matches!(self.store.restart_record(&flow_id.0), Ok(Some(current)) if current == failed)
        {
            return Reopening::Kept;
        }
        let herdr_session_name = match &resume_launch {
            Some(resume_launch) => resume_launch
                .composed_launch
                .launch_profile
                .herdr_session_name
                .clone(),
            None => signal_flow::HerdrSessionName(self.resume_session_name(&failed)),
        };
        if !self.proven_quiescent(&failed, &herdr_session_name.0) {
            eprintln!(
                "flow-nexus: restart of flow {flow_id} stays Failed: its session is not proven held nowhere",
                flow_id = flow_id.0
            );
            return Reopening::Kept;
        }
        let reopening = RestartRecord {
            restart_phase: RestartPhase::Closed,
            attempts: meta_signal_flow::Attempts(failed.attempts.0 + 1),
            ..failed
        };
        let resume_launch = match resume_launch {
            Some(resume_launch) => resume_launch,
            None => match self.resume_launch(&reopening) {
                Ok(resume_launch) => resume_launch,
                Err(failure) => {
                    return Reopening::Refused(Box::new(self.failed(reopening, failure)));
                }
            },
        };
        if !self.written(&reopening) {
            return Reopening::Refused(Box::new(StoreError::StateInvariant.refused()));
        }
        eprintln!(
            "flow-nexus: restart of flow {flow_id} reopens its session by itself, run {}",
            reopening.attempts.0,
            flow_id = flow_id.0
        );
        Reopening::Reopened(Box::new(PreparedRestart {
            record: reopening,
            resume_launch,
        }))
    }

    fn exit_witness(&self, record: &RestartRecord, patience: Duration) -> Result<(), MissingExit> {
        match &record.old_seat.process_option {
            None => Err(MissingExit::ExitUnwitnessed),
            Some(process) => match process.exit_proof_within(&ProcFs, patience) {
                ExitProof::Exited(_) => Ok(()),
                ExitProof::Running | ExitProof::Unproven(_) => Err(MissingExit::ExitUnproven),
            },
        }
    }

    fn proven_quiescent(&self, record: &RestartRecord, herdr_session_name: &str) -> bool {
        let Some(old) = self.seat_node(record, record.old_seat.herdr_route.clone()) else {
            return false;
        };
        let mut sessions = self.seat_sessions(record);
        sessions.insert(herdr_session_name.to_owned());
        self.exit_witness(record, Duration::ZERO).is_ok()
            && self.herdr.pane_presence(&old) == PanePresence::Absent
            && sessions.iter().all(|herdr_session_name| {
                self.herdr.holds_native_session(herdr_session_name, &old) == Some(false)
            })
    }

    fn seat_sessions(&self, record: &RestartRecord) -> BTreeSet<String> {
        let flows = self
            .store
            .flow_node(&record.flow_id.0)
            .ok()
            .flatten()
            .and_then(|node| match node.herdr_route_selection {
                HerdrRouteSelection::Available(route) => Some(route.herdr_session_name),
                HerdrRouteSelection::Unavailable => None,
            });
        std::iter::once(record.old_seat.herdr_route.herdr_session_name.0.clone())
            .chain(flows.map(|name| name.0))
            .collect()
    }

    fn resume_session_name(&self, record: &RestartRecord) -> String {
        // The resumed seat opens in the one configured session, as every
        // launch does; the old seat's own when none can be read.
        use crate::launching::NamesLaunchSession;
        self.launch_session()
            .unwrap_or_else(|_| record.old_seat.herdr_route.herdr_session_name.clone())
            .0
    }

    fn resume_launch(&self, record: &RestartRecord) -> Result<ResumeLaunch, RestartFailure> {
        use crate::launching::{ReadsHarnessArguments, ResolvesLaunchRoot};
        use crate::store::metaflow_declaration::ReadsMetaflowDeclarations;
        let refused = |category: &str| RestartFailure::ResumeRefused(category.into());
        let last = self.last_profile(&record.flow_id.0);
        let declared = self
            .store
            .metaflow_declaration(&record.address)
            .map_err(|_| refused("StoreRefused"))?;
        let herdr_session_name = self.resume_session_name(record);
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
            .actual_model(&record.flow_id.0)
            .ok_or_else(|| refused("ModelUnknown"))?;
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|elapsed| elapsed.as_nanos())
            .unwrap_or_default();
        let profile = signal_flow::LaunchProfile {
            launch_request_id: signal_flow::LaunchRequestId(format!(
                "restart-{}-{}-{nanos}",
                record.flow_id.0, record.attempts.0
            )),
            system_modules,
            user_modules,
            remembering: signal_flow::Remembering::Predecessors(signal_flow::RememberingDepth(0)),
            address: record.address.clone(),
            model_name: signal_flow::ModelName(model_name),
            effort: signal_flow::Effort(
                <Self as crate::sending::ChoosesEffort>::effort_of(&record.address).into(),
            ),
            flow_id_option: None,
            herdr_session_name: signal_flow::HerdrSessionName(herdr_session_name),
            leave_outs_option,
            flow_kind,
            tool_set,
            instruction_prompt: signal_flow::InstructionPrompt(String::new()),
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
        let resources = match launch.harness_kind {
            HarnessKind::Claude => {
                let title = launch
                    .launch_profile
                    .native_title(&record.flow_id.0)
                    .map_err(|_| refused("TitleRefused".into()))?;
                HarnessCommandResources {
                    herdr_session: launch.launch_profile.herdr_session_name.clone().0,
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
                    claude_resume: Some(record.native_session_id.clone().0),
                    codex_title: None,
                    thread: None,
                }
            }
            // A Codex TUI attaches to the thread its app-server keeps.
            HarnessKind::Codex => {
                use crate::codex::SelectsCodexEndpoint;
                use crate::live::HoldsLiveValue;
                let endpoints = self.codex_endpoints.current();
                let endpoint = endpoints
                    .endpoint_for(&launch.launch_profile.model_name.0)
                    .map_err(|_| refused("CodexEndpointAbsent".into()))?;
                HarnessCommandResources {
                    herdr_session: launch.launch_profile.herdr_session_name.clone().0,
                    agent: launch.launch_agent_name(),
                    pane: "<pane>".into(),
                    program: endpoint.client_path.to_string_lossy().into_owned(),
                    codex_socket: Some(endpoint.socket.clone()),
                    claude_settings: None,
                    claude_title: None,
                    prompt_fd: None,
                    claude_session: None,
                    claude_resume: None,
                    codex_title: Some(crate::herdr::launch::CODEX_TERMINAL_TITLE.into()),
                    thread: Some(record.native_session_id.clone().0),
                }
            }
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
        use crate::herdr::launch::NamesHerdrHarness;
        let refused = |category: &str| RestartFailure::ResumeRefused(category.into());
        // The harness the flow ran, and runs again: Claude, or a Codex TUI.
        let harness_kind = self
            .store
            .flow_node(&record.flow_id.0)
            .ok()
            .flatten()
            .map(|node| node.harness_kind)
            .ok_or_else(|| refused("UnknownFlow"))?;
        self.herdr
            .await_pane_rest(pane)
            .map_err(|_| refused("NotReady"))?;
        let snapshot = self
            .herdr
            .session_snapshot(&pane.herdr_session_name.0)
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
                    field(agent, "pane_id").as_deref() == Some(pane.herdr_pane_id.0.as_str())
                        && field(agent, "terminal_id").as_deref()
                            == Some(pane.herdr_terminal_id.0.as_str())
                        && field(agent, "agent").as_deref() == Some(harness_kind.expected_harness())
                })
            })
            .cloned()
            .ok_or_else(|| refused("PaneAbsent"))?;
        if agent
            .pointer("/agent_session/value")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|reported| reported != record.native_session_id.0)
        {
            return Err(RestartFailure::BindMismatch);
        }
        let route = HerdrRoute {
            herdr_session_name: pane.herdr_session_name.clone(),
            herdr_agent_name: signal_flow::HerdrAgentName(
                field(&agent, "name").unwrap_or_else(|| pane.herdr_agent_name.clone().0),
            ),
            herdr_pane_id: pane.herdr_pane_id.clone(),
            herdr_terminal_id: pane.herdr_terminal_id.clone(),
        };
        let process = self
            .herdr
            .harness_process(&route, &record.native_session_id.0)
            .ok_or_else(|| refused("ProcessUnidentified"))?;
        let process_id =
            u32::try_from(process.pid.0).map_err(|_| refused("ProcessUnidentified"))?;
        let process_identity =
            ProcessIdentity::of(process_id).ok_or_else(|| refused("ProcessUnidentified"))?;
        let working_directory = std::fs::canonicalize(format!("/proc/{process_id}/cwd"))
            .ok()
            .and_then(|directory| directory.to_str().map(str::to_owned))
            .ok_or_else(|| refused("ProcessUnidentified"))?;
        let binding = FlowBinding {
            flow_id: record.flow_id.clone(),
            aspect: record.address.aspect.clone(),
            model_name: signal_flow::ModelName(model.to_owned()),
            harness_kind,
            native_session_id: record.native_session_id.clone(),
            herdr_workspace_id: pane.herdr_workspace_id.clone(),
            herdr_pane_id: pane.herdr_pane_id.clone(),
            herdr_tab_id: meta_signal_flow::HerdrTabId(field(&agent, "tab_id").unwrap_or_default()),
            herdr_terminal_id: pane.herdr_terminal_id.clone(),
            herdr_agent_name: route.herdr_agent_name.clone(),
            process_identity,
            working_directory: meta_signal_flow::WorkingDirectory(working_directory),
        };
        let node = self
            .verified_seat_node(&pane.herdr_session_name.0, &record.address, &binding)
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
        eprintln!(
            "flow-nexus: flow {flow_id} restarted as its own session",
            flow_id = flow_id.0
        );
        Response::Restarted(flow_id)
    }

    fn abandoned(
        &self,
        record: RestartRecord,
        pane: HerdrPaneBinding,
        failure: RestartFailure,
        resume_launch: ResumeLaunch,
        spawned: Option<Process>,
    ) -> Resumption {
        let flow_id = record.flow_id.clone();
        eprintln!(
            "flow-nexus: restart of flow {flow_id} did not bind: {failure:?}",
            flow_id = flow_id.0
        );
        // The seat to reconcile is now this pane and the harness it showed:
        // the old one was proven exited before Closed was written.
        let record = RestartRecord {
            old_seat: OldSeat {
                herdr_route: pane.route(),
                process_option: spawned,
            },
            ..record
        };
        if self.perform(Operation::Discard(pane.clone())) != Outcome::Closed {
            return Resumption::Answered(Box::new(self.interrupted(record, Some(pane))));
        }
        let failed = RestartRecord {
            restart_phase: RestartPhase::Failed(failure.clone()),
            ..record
        };
        if !self.written(&failed) {
            return Resumption::Answered(Box::new(StoreError::StateInvariant.refused()));
        }
        Resumption::Failed(Box::new(FailedRun {
            failed,
            resume_launch,
            response: failure.answer(&flow_id.0),
        }))
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
        failure.answer(&flow_id.0)
    }

    fn interrupted(&self, record: RestartRecord, pane: Option<HerdrPaneBinding>) -> Response {
        let flow_id = record.flow_id.clone();
        eprintln!(
            "flow-nexus: restart of flow {flow_id} is interrupted; only a Stop clears it",
            flow_id = flow_id.0
        );
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
            Ok(Some(role)) => Some(role.model_name.0),
            _ => self
                .last_profile(flow_id)
                .map(|profile| profile.model_name.0),
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
        let mut sessions = self.seat_sessions(record);
        sessions.extend(pane.map(|pane| pane.herdr_session_name.0.clone()));
        for herdr_session_name in &sessions {
            let Some(holding) = self
                .herdr
                .panes_holding(herdr_session_name, &record.native_session_id.0)
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
        let node = self.store.flow_node(&record.flow_id.0).ok()??;
        Some(FlowNode {
            session_id: signal_flow::SessionId(record.native_session_id.0.clone()),
            herdr_route_selection: HerdrRouteSelection::Available(route),
            ..node
        })
    }
}

impl RestartsFlows for RunningNexus {
    fn restart(&self, flow_id: &str) -> Response {
        let closed = {
            let _turn = self
                .dispatch_gate
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            self.begun(flow_id, false)
                .and_then(|prepared| self.closed_old(prepared))
        };
        self.restarted_after(flow_id, closed)
    }

    fn restart_at_idle_event(&self, flow_id: &str) -> Response {
        let closed = {
            let _turn = self
                .dispatch_gate
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            self.begun(flow_id, true)
                .and_then(|prepared| self.closed_old(prepared))
        };
        self.restarted_after(flow_id, closed)
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
                        .actual_model(&flow_id.0)
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
                // Reopened only under the bound, and only once the old
                // seat's recorded harness is exited and nothing holds the
                // session.
                RestartPhase::Failed(_) => match self.reopened(record, None, Duration::ZERO) {
                    Reopening::Reopened(prepared) => self.resumed(*prepared),
                    Reopening::Kept => continue,
                    Reopening::Refused(response) => *response,
                },
                RestartPhase::Interrupted(_) => continue,
            };
            eprintln!(
                "flow-nexus: restart of flow {flow_id} taken up at start: {response:?}",
                flow_id = flow_id.0
            );
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
        // An interrupted spawn's pane is the seat a later retry reconciles:
        // the harness it shows now, else the one recorded for it.
        let record = match &pane {
            Some(pane) => {
                let route = pane.route();
                let recorded = Some(&record.old_seat)
                    .filter(|old_seat| old_seat.herdr_route == route)
                    .and_then(|old_seat| old_seat.process_option.clone());
                let process_option = self
                    .herdr
                    .harness_process(&route, &record.native_session_id.0)
                    .or(recorded);
                RestartRecord {
                    old_seat: OldSeat {
                        herdr_route: route,
                        process_option,
                    },
                    ..record
                }
            }
            None => record,
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

    fn report_acts(&self, flow_id: &str, birth: Option<&Process>) -> bool {
        if !matches!(self.store.restart_record(flow_id), Ok(None)) {
            return false;
        }
        let seat = match self.store.seat_birth(flow_id) {
            Ok(None) => return true,
            Ok(Some(seat)) => seat,
            Err(_) => return false,
        };
        let route = &seat.herdr_route;
        // The birth names the seat only while the flow still runs there.
        let current = self.seat_witness(flow_id).is_some_and(|witness| {
            witness.route.herdr_session_name == route.herdr_session_name
                && witness.route.herdr_pane_id == route.herdr_pane_id
                && witness.route.herdr_terminal_id == route.herdr_terminal_id
        });
        current && birth == Some(&seat.process)
    }

    fn witness_holds(&self, flow_id: &str, witness: Option<&SeatWitness>) -> bool {
        if !matches!(self.store.restart_record(flow_id), Ok(None)) {
            return false;
        }
        witness.is_none_or(|witness| self.seat_witness(flow_id).as_ref() == Some(witness))
    }
}
