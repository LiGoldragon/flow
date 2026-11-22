//! Codex turns: whether an Awake Codex seat rests, read only from its
//! app-server's own lifecycle for its thread, and the settlement its rest
//! allows.
//!
//! A Codex seat runs no hook, so it never reports `Stopped`; its rest is
//! its thread's. Each thread has a turn ledger (vocabulary in
//! `ethos/turn_ledger.ethos`): its open and ended turns, its process-backed
//! work items, and the letters Flow typed into its pane that no receipt has
//! taken yet. Every fact only moves up, Unseen to Open to Ended, so the
//! ledger reads the same in whatever order the app-server's events reach
//! Flow, from whichever of its connections: the launch's opening
//! connection, the `turn/start` answer, the responder.
//!
//! A turn ends only by native evidence: its own `turn/completed`, a cut that
//! shows it not running, or `thread/closed`. A thread status moves nothing.
//! The cut is the `thread/resume` answer on the responder's subscribing
//! connection, carrying the thread's newest turn: the app-server builds it
//! and subscribes the connection in one step of the thread's listener, so
//! every event before it is reflected in it and every event after it
//! reaches that connection after it. An open turn greater than every turn a
//! cut lists is kept: it started after the cut, or Flow learned it from
//! `turn/start` before the listener did. A typed letter is pending until a
//! `userMessage` item whose only content is the letter's payload takes it,
//! or until the queue settles its attempt without one; two pending letters
//! of one text take none, and one item takes at most one letter.
//!
//! The thread rests only when its ledger is established by a cut of the
//! open subscription and holds no open turn, no running item and no
//! pending letter. The step into that standing settles the seat as a
//! `Stopped` report settles a Claude flow
//! (`SleepsAndWakesSeats::settle_stopped`); settlement takes the dispatch
//! gate and reads the standing again under it. A turn or a process that
//! never ends keeps the seat until it is stopped: nothing here interrupts.
//!
//! The ledger is memory only. A Nexus that starts again reads each live
//! thread from its responder's cut, and the letters still unsettled from
//! the queue (`recover_typed`).

use crate::RunningNexus;
use crate::generated::transitions::{
    After, Before, CodexFlowReport, CodexReport, ReportsEvents, Stepped_Data,
};
use crate::generated::turn_ledger::{
    BackgroundTerminal, CodexItemId, CodexTurnId, CodexTurnStatus, CutItem, LedgerEvent,
    LedgerEvidence, LedgerReading, LetterReceived_Data, PayloadText, PendingLetter,
    ProcessBacked_Data, Reconcile, RestReached, RetentionExceeded, SeenLetter, ThreadCut,
    ThreadStanding, TurnBound_Data, TurnReport, TurnsEnded, UnifiedExecProcessId, WorkItem,
};
use crate::store::address_hold::HoldsLaunchAddress;
use crate::store::metaflow::SleepsMetaflows;
use crate::store::{NamesLiveFlow, ReadsFlowRows};
use crate::turn_ends::RecordsTurnEnd;
use crate::waking::{
    AtStop, HoldsWakingMetaflows, NotesStopsAtConfirmation, ParksCohort, SleepsAndWakesSeats,
};
use meta_signal_flow::TurnRetentionBound;
use signal_flow::{Address, FlowId, FlowNode, HarnessKind};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Mutex;

/// One thread's ledger.
#[derive(Debug, Clone)]
struct ThreadLedger {
    open_turns: BTreeSet<CodexTurnId>,
    /// The newest ended turns, as many as the retention keeps.
    ended_turns: BTreeSet<CodexTurnId>,
    /// The greatest ended turn the ledger let go: the watermark.
    evicted_through: Option<CodexTurnId>,
    /// Process-backed items not seen ending, by item.
    running_items: BTreeMap<CodexItemId, ProcessBacked_Data>,
    /// The running items the thread closed over.
    orphaned_items: BTreeSet<CodexItemId>,
    /// Process-backed items seen ending, with their turn, while it is kept.
    ended_items: BTreeMap<CodexItemId, CodexTurnId>,
    letters: Vec<PendingLetter>,
    /// The userMessage items already read as receipts, with their turn.
    read_receipts: BTreeMap<CodexItemId, CodexTurnId>,
    evidence: LedgerEvidence,
    /// The turn or item turn that fell below the watermark since the last
    /// cut: the ledger stands Unreconciled until the next one.
    exceeded: RetentionExceeded,
}

impl Default for ThreadLedger {
    fn default() -> Self {
        Self {
            open_turns: BTreeSet::new(),
            ended_turns: BTreeSet::new(),
            evicted_through: None,
            running_items: BTreeMap::new(),
            orphaned_items: BTreeSet::new(),
            ended_items: BTreeMap::new(),
            letters: Vec::new(),
            read_receipts: BTreeMap::new(),
            evidence: LedgerEvidence::Unestablished,
            exceeded: RetentionExceeded(None),
        }
    }
}

/// What one event did, gathered while it is applied.
#[derive(Debug)]
struct LedgerStep {
    ended: Vec<CodexTurnId>,
    reconcile: Reconcile,
    exceeded: Option<CodexTurnId>,
}

impl Default for LedgerStep {
    fn default() -> Self {
        Self {
            ended: Vec::new(),
            reconcile: Reconcile(false),
            exceeded: None,
        }
    }
}

/// The ledger's own facts, each moved only up.
trait KeepsLedgerFacts {
    /// Whether the ledger holds the turn, open or ended.
    fn holds(&self, turn: &CodexTurnId) -> bool;
    /// A turn the ledger does not hold, at or below its watermark: an old
    /// turn's late report, or a turn it never saw.
    fn below_watermark(&self, turn: &CodexTurnId) -> bool;
    fn open_turn(&mut self, turn: CodexTurnId, step: &mut LedgerStep);
    fn end_turn(&mut self, turn: CodexTurnId, bound: &TurnRetentionBound, step: &mut LedgerStep);
    fn open_item(&mut self, item: ProcessBacked_Data, step: &mut LedgerStep);
    fn end_item(&mut self, item: CodexItemId, turn: CodexTurnId);
    fn see_letter(&mut self, seen: SeenLetter);
    fn evict(&mut self, bound: &TurnRetentionBound);
    /// The newest turn the ledger knows, open or ended.
    fn newest_turn(&self) -> Option<CodexTurnId>;
}

impl KeepsLedgerFacts for ThreadLedger {
    fn holds(&self, turn: &CodexTurnId) -> bool {
        self.open_turns.contains(turn) || self.ended_turns.contains(turn)
    }

    fn below_watermark(&self, turn: &CodexTurnId) -> bool {
        !self.holds(turn)
            && self
                .evicted_through
                .as_ref()
                .is_some_and(|watermark| turn <= watermark)
    }

    fn open_turn(&mut self, turn: CodexTurnId, step: &mut LedgerStep) {
        if self.holds(&turn) {
            return;
        }
        if self.below_watermark(&turn) {
            step.exceeded = Some(turn);
            return;
        }
        self.open_turns.insert(turn);
    }

    fn end_turn(&mut self, turn: CodexTurnId, bound: &TurnRetentionBound, step: &mut LedgerStep) {
        if self.ended_turns.contains(&turn) {
            return;
        }
        if self.below_watermark(&turn) {
            step.exceeded = Some(turn);
            return;
        }
        self.open_turns.remove(&turn);
        self.ended_turns.insert(turn.clone());
        step.ended.push(turn);
        self.evict(bound);
    }

    fn open_item(&mut self, item: ProcessBacked_Data, step: &mut LedgerStep) {
        if self.running_items.contains_key(&item.codex_item_id)
            || self.ended_items.contains_key(&item.codex_item_id)
        {
            return;
        }
        if self.below_watermark(&item.codex_turn_id) {
            step.exceeded = Some(item.codex_turn_id);
            return;
        }
        self.running_items.insert(item.codex_item_id.clone(), item);
    }

    fn end_item(&mut self, item: CodexItemId, turn: CodexTurnId) {
        let turn = self
            .running_items
            .remove(&item)
            .map(|running| running.codex_turn_id)
            .unwrap_or(turn);
        self.orphaned_items.remove(&item);
        if !self.below_watermark(&turn) {
            self.ended_items.insert(item, turn);
        }
    }

    fn see_letter(&mut self, seen: SeenLetter) {
        if self.read_receipts.contains_key(&seen.codex_item_id) {
            return;
        }
        let matching = self
            .letters
            .iter()
            .filter(|letter| letter.payload_text == seen.payload_text)
            .count();
        match matching {
            // A message Flow did not type: a person's, or a launch's.
            0 => {}
            1 => {
                self.letters
                    .retain(|letter| letter.payload_text != seen.payload_text);
                self.read_receipts
                    .insert(seen.codex_item_id, seen.codex_turn_id);
            }
            // Two letters of one text: the item names neither, so it takes
            // none, now or when it is seen again.
            _ => {
                self.read_receipts
                    .insert(seen.codex_item_id, seen.codex_turn_id);
            }
        }
    }

    fn evict(&mut self, bound: &TurnRetentionBound) {
        let kept = usize::try_from(bound.0).unwrap_or(0);
        while self.ended_turns.len() > kept {
            let Some(least) = self.ended_turns.pop_first() else {
                break;
            };
            if self
                .evicted_through
                .as_ref()
                .is_none_or(|watermark| least > *watermark)
            {
                self.evicted_through = Some(least);
            }
        }
        if let Some(watermark) = self.evicted_through.clone() {
            self.ended_items.retain(|_, turn| *turn > watermark);
            self.read_receipts.retain(|_, turn| *turn > watermark);
        }
    }

    fn newest_turn(&self) -> Option<CodexTurnId> {
        self.open_turns
            .iter()
            .chain(self.ended_turns.iter())
            .max()
            .cloned()
    }
}

/// The ledger read and moved by one event.
trait AppliesLedgerEvent {
    fn standing(&self) -> ThreadStanding;
    fn apply(&mut self, event: LedgerEvent, bound: &TurnRetentionBound) -> LedgerReading;
}

impl AppliesLedgerEvent for ThreadLedger {
    fn standing(&self) -> ThreadStanding {
        if self.evidence == LedgerEvidence::Unestablished || self.exceeded.0.is_some() {
            return ThreadStanding::Unreconciled;
        }
        if !self.open_turns.is_empty() {
            return ThreadStanding::Working(self.open_turns.iter().cloned().collect());
        }
        if !self.letters.is_empty() {
            return ThreadStanding::LetterAwaited(
                self.letters
                    .iter()
                    .map(|letter| letter.queue_attempt_id.clone())
                    .collect(),
            );
        }
        if !self.orphaned_items.is_empty() {
            return ThreadStanding::ItemsOrphaned(self.orphaned_items.iter().cloned().collect());
        }
        if !self.running_items.is_empty() {
            return ThreadStanding::ItemsRunning(self.running_items.keys().cloned().collect());
        }
        ThreadStanding::AtRest
    }

    fn apply(&mut self, event: LedgerEvent, bound: &TurnRetentionBound) -> LedgerReading {
        let before = self.standing();
        let mut step = LedgerStep::default();
        match event {
            LedgerEvent::TurnOpened(turn) => self.open_turn(turn, &mut step),
            LedgerEvent::TurnClosed(TurnReport { codex_turn_id, .. }) => {
                // Whatever its status: an interrupted or failed turn has
                // ended as a completed one has.
                self.end_turn(codex_turn_id, bound, &mut step)
            }
            LedgerEvent::ItemOpened(WorkItem::ProcessBacked(item)) => {
                self.open_item(item, &mut step)
            }
            // An item that runs inside its turn's task ends with it.
            LedgerEvent::ItemOpened(WorkItem::TurnBound(_)) => {}
            LedgerEvent::ItemClosed(
                WorkItem::ProcessBacked(ProcessBacked_Data {
                    codex_item_id,
                    codex_turn_id,
                    ..
                })
                | WorkItem::TurnBound(TurnBound_Data {
                    codex_item_id,
                    codex_turn_id,
                }),
            ) => self.end_item(codex_item_id, codex_turn_id),
            LedgerEvent::LetterTyped(letter) => {
                if !self
                    .letters
                    .iter()
                    .any(|pending| pending.queue_attempt_id == letter.queue_attempt_id)
                {
                    self.letters.push(letter);
                }
            }
            LedgerEvent::LetterSeen(seen) => self.see_letter(seen),
            LedgerEvent::LetterReceived(LetterReceived_Data {
                queue_attempt_id,
                codex_turn_id,
            }) => {
                // Taken only in a turn the ledger knows: a turn it has not
                // seen yet will show the letter's own item.
                if self.holds(&codex_turn_id)
                    || self
                        .evicted_through
                        .as_ref()
                        .is_some_and(|watermark| codex_turn_id <= *watermark)
                {
                    self.letters
                        .retain(|letter| letter.queue_attempt_id != queue_attempt_id);
                }
            }
            LedgerEvent::LetterSettled(queue_attempt_id) => {
                self.letters
                    .retain(|letter| letter.queue_attempt_id != queue_attempt_id);
            }
            LedgerEvent::Cut(ThreadCut {
                turn_report_vector,
                seen_letter_vector,
                cut_item_vector,
            }) => {
                self.exceeded = RetentionExceeded(None);
                let newest_listed = turn_report_vector
                    .iter()
                    .map(|report| report.codex_turn_id.clone())
                    .max();
                for report in turn_report_vector {
                    match report.codex_turn_status {
                        // The cut is the thread as its listener holds it:
                        // a turn it shows running runs.
                        CodexTurnStatus::InProgress => {
                            if !self.ended_turns.contains(&report.codex_turn_id) {
                                self.open_turns.insert(report.codex_turn_id);
                            }
                        }
                        CodexTurnStatus::Completed
                        | CodexTurnStatus::Interrupted
                        | CodexTurnStatus::Failed => {
                            if self.holds(&report.codex_turn_id)
                                || !self.below_watermark(&report.codex_turn_id)
                            {
                                self.end_turn(report.codex_turn_id, bound, &mut step);
                            }
                        }
                    }
                }
                // One turn is active at a time: an open turn less than a
                // listed one was not running at the cut.
                if let Some(newest) = newest_listed {
                    let superseded = self
                        .open_turns
                        .iter()
                        .filter(|open| **open < newest)
                        .cloned()
                        .collect::<Vec<_>>();
                    for turn in superseded {
                        self.end_turn(turn, bound, &mut step);
                    }
                }
                for seen in seen_letter_vector {
                    self.see_letter(seen);
                }
                for item in cut_item_vector {
                    match item {
                        CutItem::Running(WorkItem::ProcessBacked(item)) => {
                            if !self.ended_items.contains_key(&item.codex_item_id) {
                                self.running_items.insert(item.codex_item_id.clone(), item);
                            }
                        }
                        CutItem::Ended(
                            WorkItem::ProcessBacked(ProcessBacked_Data {
                                codex_item_id,
                                codex_turn_id,
                                ..
                            })
                            | WorkItem::TurnBound(TurnBound_Data {
                                codex_item_id,
                                codex_turn_id,
                            }),
                        ) => self.end_item(codex_item_id, codex_turn_id),
                        CutItem::Running(WorkItem::TurnBound(_)) => {}
                    }
                }
                self.evidence = LedgerEvidence::Established;
                // A cut is never itself a fact below the watermark.
                step.exceeded = None;
            }
            LedgerEvent::BackgroundTerminals(terminals) => {
                // Read after a cut: each process the app-server still keeps
                // is running work, its turn the newest the ledger knows.
                let turn = self
                    .newest_turn()
                    .unwrap_or_else(|| CodexTurnId(String::new()));
                for BackgroundTerminal {
                    codex_item_id,
                    unified_exec_process_id,
                } in terminals
                {
                    if !self.running_items.contains_key(&codex_item_id)
                        && !self.ended_items.contains_key(&codex_item_id)
                    {
                        self.running_items.insert(
                            codex_item_id.clone(),
                            ProcessBacked_Data {
                                codex_item_id,
                                codex_turn_id: turn.clone(),
                                unified_exec_process_id,
                            },
                        );
                    }
                }
            }
            LedgerEvent::SubscriptionEnded => {
                self.evidence = LedgerEvidence::Unestablished;
                step.reconcile = Reconcile(true);
            }
            LedgerEvent::ThreadClosed => {
                // The thread was unloaded once its shutdown completed: its
                // turns and their tasks ended with it. A process it kept was
                // signalled, its end never relayed: it is orphaned, not
                // ended.
                let open = self.open_turns.iter().cloned().collect::<Vec<_>>();
                for turn in open {
                    self.end_turn(turn, bound, &mut step);
                }
                self.orphaned_items
                    .extend(self.running_items.keys().cloned());
            }
        }
        if let Some(turn) = step.exceeded.clone() {
            self.exceeded = RetentionExceeded(Some(turn));
            step.reconcile = Reconcile(true);
        }
        let after = self.standing();
        LedgerReading {
            rest_reached: RestReached(
                before != ThreadStanding::AtRest && after == ThreadStanding::AtRest,
            ),
            thread_standing: after,
            turns_ended: TurnsEnded(step.ended),
            reconcile: step.reconcile,
            retention_exceeded: RetentionExceeded(step.exceeded),
        }
    }
}

/// Every Codex thread's ledger, keyed by thread id, and the retention every
/// ledger keeps.
#[derive(Debug)]
pub struct CodexTurns {
    threads: Mutex<HashMap<String, ThreadLedger>>,
    retention: Mutex<TurnRetentionBound>,
}

impl Default for CodexTurns {
    fn default() -> Self {
        Self {
            threads: Mutex::new(HashMap::new()),
            retention: Mutex::new(TurnRetentionBound(
                crate::store::SEEDED_ENDED_TURN_RETENTION,
            )),
        }
    }
}

pub trait TracksCodexTurns {
    /// Applies one event to the thread's ledger, under the dispatch gate
    /// the caller holds.
    fn apply(&self, thread: &str, event: LedgerEvent) -> LedgerReading;
    /// Where the thread stands; Unreconciled for a thread Flow holds no
    /// cut of.
    fn standing(&self, thread: &str) -> ThreadStanding;
    /// The configured retention: every ledger keeps that many ended turns
    /// from now, a ledger holding more letting the least go.
    fn retain(&self, bound: TurnRetentionBound);
    fn retention(&self) -> TurnRetentionBound;
}

impl TracksCodexTurns for CodexTurns {
    fn apply(&self, thread: &str, event: LedgerEvent) -> LedgerReading {
        let bound = self.retention();
        self.threads
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .entry(thread.to_owned())
            .or_default()
            .apply(event, &bound)
    }

    fn standing(&self, thread: &str) -> ThreadStanding {
        self.threads
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(thread)
            .map(AppliesLedgerEvent::standing)
            .unwrap_or(ThreadStanding::Unreconciled)
    }

    fn retain(&self, bound: TurnRetentionBound) {
        *self
            .retention
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = bound;
        for ledger in self
            .threads
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .values_mut()
        {
            ledger.evict(&bound);
        }
    }

    fn retention(&self) -> TurnRetentionBound {
        *self
            .retention
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

/// The ledger events one app-server message carries.
pub trait ReadsLedgerEvents {
    /// The events a JSON-RPC notification carries for `thread`: a turn's
    /// start or end, a work item's or a user message's, the thread's close.
    /// None for a request, an answer, a status, or another thread's event.
    fn ledger_events(&self, thread: &str) -> Vec<LedgerEvent>;
}

/// One thread item, as the ledger reads it.
pub trait ReadsThreadItem {
    /// The letter a `userMessage` item shows: its only content one text.
    fn seen_letter(&self, turn: &CodexTurnId) -> Option<SeenLetter>;
    /// The work an item runs: a command with a live process behind it is
    /// process-backed, any other work item ends with its turn.
    fn work_item(&self, turn: &CodexTurnId) -> Option<WorkItem>;
    /// Whether the item's own status says it still runs.
    fn runs(&self) -> bool;
}

/// A turn as an answer or a notification names it.
pub trait ReadsTurnReport {
    fn turn_report(&self) -> Option<TurnReport>;
}

/// The cut a `thread/resume` answer carries, and the terminals a
/// `thread/backgroundTerminals/list` answer names.
pub trait ReadsThreadCut {
    fn thread_cut(&self) -> Option<ThreadCut>;
    fn background_terminals(&self) -> Vec<BackgroundTerminal>;
}

impl ReadsTurnReport for serde_json::Value {
    fn turn_report(&self) -> Option<TurnReport> {
        let id = self
            .get("id")
            .and_then(serde_json::Value::as_str)
            .filter(|turn| !turn.is_empty())?;
        let status = match self.get("status").and_then(serde_json::Value::as_str)? {
            "completed" => CodexTurnStatus::Completed,
            "interrupted" => CodexTurnStatus::Interrupted,
            "failed" => CodexTurnStatus::Failed,
            "inProgress" => CodexTurnStatus::InProgress,
            _ => return None,
        };
        Some(TurnReport {
            codex_turn_id: CodexTurnId(id.to_owned()),
            codex_turn_status: status,
        })
    }
}

impl ReadsThreadItem for serde_json::Value {
    fn seen_letter(&self, turn: &CodexTurnId) -> Option<SeenLetter> {
        if self.get("type").and_then(serde_json::Value::as_str) != Some("userMessage") {
            return None;
        }
        let id = self
            .get("id")
            .and_then(serde_json::Value::as_str)
            .filter(|item| !item.is_empty())?;
        let text = match self
            .get("content")
            .and_then(serde_json::Value::as_array)?
            .as_slice()
        {
            [only] if only.get("type").and_then(serde_json::Value::as_str) == Some("text") => {
                only.get("text").and_then(serde_json::Value::as_str)?
            }
            _ => return None,
        };
        Some(SeenLetter {
            codex_turn_id: turn.clone(),
            codex_item_id: CodexItemId(id.to_owned()),
            payload_text: PayloadText(text.to_owned()),
        })
    }

    fn work_item(&self, turn: &CodexTurnId) -> Option<WorkItem> {
        let kind = self.get("type").and_then(serde_json::Value::as_str)?;
        let id = self
            .get("id")
            .and_then(serde_json::Value::as_str)
            .filter(|item| !item.is_empty())?;
        let item = CodexItemId(id.to_owned());
        let process = self
            .get("processId")
            .and_then(serde_json::Value::as_str)
            .filter(|process| !process.is_empty());
        match (kind, process) {
            ("commandExecution", Some(process)) => {
                Some(WorkItem::ProcessBacked(ProcessBacked_Data {
                    codex_item_id: item,
                    codex_turn_id: turn.clone(),
                    unified_exec_process_id: UnifiedExecProcessId(process.to_owned()),
                }))
            }
            (
                "commandExecution"
                | "mcpToolCall"
                | "dynamicToolCall"
                | "collabAgentToolCall"
                | "fileChange",
                _,
            ) => Some(WorkItem::TurnBound(TurnBound_Data {
                codex_item_id: item,
                codex_turn_id: turn.clone(),
            })),
            _ => None,
        }
    }

    fn runs(&self) -> bool {
        self.get("status").and_then(serde_json::Value::as_str) == Some("inProgress")
    }
}

impl ReadsLedgerEvents for serde_json::Value {
    fn ledger_events(&self, thread: &str) -> Vec<LedgerEvent> {
        if self.get("id").is_some() {
            return Vec::new();
        }
        let Some(params) = self.get("params") else {
            return Vec::new();
        };
        if params.get("threadId").and_then(serde_json::Value::as_str) != Some(thread) {
            return Vec::new();
        }
        let item_turn = || {
            params
                .get("turnId")
                .and_then(serde_json::Value::as_str)
                .filter(|turn| !turn.is_empty())
                .map(|turn| CodexTurnId(turn.to_owned()))
        };
        match self.get("method").and_then(serde_json::Value::as_str) {
            Some("turn/started") => params
                .get("turn")
                .and_then(ReadsTurnReport::turn_report)
                .map(|report| vec![LedgerEvent::TurnOpened(report.codex_turn_id)])
                .unwrap_or_default(),
            Some("turn/completed") => params
                .get("turn")
                .and_then(ReadsTurnReport::turn_report)
                .map(|report| vec![LedgerEvent::TurnClosed(report)])
                .unwrap_or_default(),
            Some(method @ ("item/started" | "item/completed")) => {
                let (Some(item), Some(turn)) = (params.get("item"), item_turn()) else {
                    return Vec::new();
                };
                let mut events = Vec::new();
                if let Some(seen) = item.seen_letter(&turn) {
                    events.push(LedgerEvent::LetterSeen(seen));
                }
                if let Some(work) = item.work_item(&turn) {
                    events.push(if method == "item/started" {
                        LedgerEvent::ItemOpened(work)
                    } else {
                        LedgerEvent::ItemClosed(work)
                    });
                }
                events
            }
            Some("thread/closed") => vec![LedgerEvent::ThreadClosed],
            _ => Vec::new(),
        }
    }
}

impl ReadsThreadCut for serde_json::Value {
    fn thread_cut(&self) -> Option<ThreadCut> {
        // Only the subscribing answer of a thread is a cut.
        self.get("thread")?;
        let turns = self
            .pointer("/initialTurnsPage/data")
            .and_then(serde_json::Value::as_array)
            .cloned()
            .unwrap_or_default();
        let mut cut = ThreadCut {
            turn_report_vector: Vec::new(),
            seen_letter_vector: Vec::new(),
            cut_item_vector: Vec::new(),
        };
        for turn in turns {
            let Some(report) = turn.turn_report() else {
                continue;
            };
            let items = turn
                .get("items")
                .and_then(serde_json::Value::as_array)
                .cloned()
                .unwrap_or_default();
            for item in items {
                if let Some(seen) = item.seen_letter(&report.codex_turn_id) {
                    cut.seen_letter_vector.push(seen);
                }
                if let Some(work) = item.work_item(&report.codex_turn_id) {
                    cut.cut_item_vector.push(if item.runs() {
                        CutItem::Running(work)
                    } else {
                        CutItem::Ended(work)
                    });
                }
            }
            cut.turn_report_vector.push(report);
        }
        Some(cut)
    }

    fn background_terminals(&self) -> Vec<BackgroundTerminal> {
        self.get("data")
            .and_then(serde_json::Value::as_array)
            .map(|terminals| {
                terminals
                    .iter()
                    .filter_map(|terminal| {
                        let item = terminal
                            .get("itemId")
                            .and_then(serde_json::Value::as_str)
                            .filter(|item| !item.is_empty())?;
                        let process = terminal
                            .get("processId")
                            .and_then(serde_json::Value::as_str)
                            .filter(|process| !process.is_empty())?;
                        Some(BackgroundTerminal {
                            codex_item_id: CodexItemId(item.to_owned()),
                            unified_exec_process_id: UnifiedExecProcessId(process.to_owned()),
                        })
                    })
                    .collect()
            })
            .unwrap_or_default()
    }
}

/// The settlement a Codex thread's rest allows.
pub trait SettlesRestingCodexSeats {
    /// Off the dispatch gate, from a responder: applies the events under
    /// the gate, then settles the Codex flow on the thread once if they
    /// brought it to rest, or left it resting with work queued; what the
    /// settlement did, or None when nothing was settled.
    fn codex_events(&self, thread: &str, events: Vec<LedgerEvent>) -> Option<AtStop>;
    /// Under the dispatch gate, as a launch holding it reads its own
    /// connection: applies the events, and notes the flow on the thread to
    /// be settled once the gate is given back when they left it resting.
    fn observe_codex_events(&self, thread: &str, events: Vec<LedgerEvent>);
    /// The live Codex flow on `thread`, when there is exactly one.
    fn codex_flow_on(&self, thread: &str) -> Option<FlowNode>;
    /// Whether the flow is a Codex flow whose thread stands at rest.
    fn codex_resting(&self, flow_id: &str) -> bool;
    /// Whether the flow is a Codex flow whose thread holds a turn, an item
    /// or a letter open.
    fn codex_in_turn(&self, flow_id: &str) -> bool;
    /// The Codex flow a metaflow is Awake as, its launch confirmed, no wake
    /// holding it, its thread resting: the flow whose rest is settled.
    fn resting_codex_seat(&self, address: &Address) -> Option<FlowId>;
    /// Logs a reading that reached below its ledger's watermark.
    fn note_retention(&self, thread: &str, reading: &LedgerReading);
}

impl SettlesRestingCodexSeats for RunningNexus {
    fn codex_events(&self, thread: &str, events: Vec<LedgerEvent>) -> Option<AtStop> {
        if events.is_empty() {
            return None;
        }
        let closed = events
            .iter()
            .any(|event| matches!(event, LedgerEvent::ThreadClosed));
        let announces_user_input = events.iter().any(|event| match event {
            LedgerEvent::LetterSeen(_) => true,
            LedgerEvent::Cut(cut) => !cut.seen_letter_vector.is_empty(),
            _ => false,
        });
        let (seat, ended, reported) = {
            let _turn = self
                .dispatch_gate
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let before = self.codex_turns.standing(thread);
            let readings = events
                .into_iter()
                .map(|event| self.codex_turns.apply(thread, event))
                .collect::<Vec<_>>();
            for reading in &readings {
                self.note_retention(thread, reading);
            }
            let ended = readings
                .iter()
                .map(|reading| reading.turns_ended.0.len())
                .sum::<usize>();
            let flow = self.codex_flow_on(thread);
            // What the batch says of the thread, recorded as its flow's
            // event (`transitions`).
            let reported = flow.as_ref().map(|node| CodexFlowReport {
                flow_id: node.flow_id.clone(),
                codex_report: if closed {
                    CodexReport::Closed
                } else {
                    CodexReport::Stepped(Stepped_Data {
                        before: Before(before.clone()),
                        after: After(self.codex_turns.standing(thread)),
                    })
                },
            });
            // The bound native item or reconnect cut tells the existing
            // anchored matcher to read receipts. An item or cut alone
            // neither settles an attempt nor authorizes another write.
            if let Some(node) = &flow
                && let Ok(Some(metaflow)) = self.store.metaflow_awake_as(&node.flow_id.0)
            {
                if announces_user_input {
                    self.settle_turn_receipts(&metaflow.address);
                } else if ended > 0
                    && matches!(
                        self.codex_turns.standing(thread),
                        ThreadStanding::LetterAwaited(_)
                    )
                {
                    self.reconcile_typed(&metaflow.address);
                }
            }
            let rested = readings.iter().any(|reading| reading.rest_reached.0)
                || (before != ThreadStanding::AtRest
                    && self.codex_turns.standing(thread) == ThreadStanding::AtRest);
            // A rest settles once whatever is queued; queued work at an
            // event that finds the thread already resting is drained.
            let seat = flow.as_ref().and_then(|node| {
                let metaflow = self.store.metaflow_awake_as(&node.flow_id.0).ok()??;
                (rested || !metaflow.queue.0.is_empty())
                    .then(|| self.resting_codex_seat(&metaflow.address))
                    .flatten()
            });
            (seat, flow.map(|node| (node.flow_id, ended)), reported)
        };
        if let Some(reported) = reported {
            self.report_codex(reported);
        }
        // Each turn the events ended is a turn end, sent before the
        // settlement it allows.
        if let Some((flow_id, ended)) = ended {
            for _ in 0..ended {
                self.turn_ended(&flow_id.0);
            }
        }
        // Settled as a Stop is: off the gate, which it takes, reading the
        // thread's standing again under it.
        seat.map(|flow_id| self.settle_stopped(&flow_id.0))
    }

    fn observe_codex_events(&self, thread: &str, events: Vec<LedgerEvent>) {
        let before = self.codex_turns.standing(thread);
        let closed = events
            .iter()
            .any(|event| matches!(event, LedgerEvent::ThreadClosed));
        let readings = events
            .into_iter()
            .map(|event| self.codex_turns.apply(thread, event))
            .collect::<Vec<_>>();
        for reading in &readings {
            self.note_retention(thread, reading);
        }
        if let Some(node) = self.codex_flow_on(thread) {
            self.report_codex(CodexFlowReport {
                flow_id: node.flow_id,
                codex_report: if closed {
                    CodexReport::Closed
                } else {
                    CodexReport::Stepped(Stepped_Data {
                        before: Before(before),
                        after: After(self.codex_turns.standing(thread)),
                    })
                },
            });
        }
        let ended = readings
            .iter()
            .map(|reading| reading.turns_ended.0.len())
            .sum::<usize>();
        if ended > 0
            && let Some(node) = self.codex_flow_on(thread)
        {
            for _ in 0..ended {
                self.turn_ended(&node.flow_id.0);
            }
        }
        if self.codex_turns.standing(thread) == ThreadStanding::AtRest
            && let Some(node) = self.codex_flow_on(thread)
            && matches!(self.store.address_hold(&node.flow_id.0), Ok(None))
            && matches!(self.store.metaflow_awake_as(&node.flow_id.0), Ok(Some(_)))
        {
            // An unconfirmed seat is noted by its confirmation instead.
            self.stopped_at_confirmation.note(&node.flow_id.0);
        }
    }

    fn codex_flow_on(&self, thread: &str) -> Option<FlowNode> {
        let nodes = self.store.flow_nodes().ok()?;
        let mut live = nodes.into_iter().filter(|node| {
            node.harness_kind == HarnessKind::Codex
                && !thread.is_empty()
                && node.session_id.0 == thread
                && node.flow_lifecycle.is_live()
        });
        let node = live.next()?;
        live.next().is_none().then_some(node)
    }

    fn codex_resting(&self, flow_id: &str) -> bool {
        matches!(
            self.store.flow_node(flow_id),
            Ok(Some(node)) if node.harness_kind == HarnessKind::Codex
                && !node.session_id.0.is_empty()
                && self.codex_turns.standing(&node.session_id.0) == ThreadStanding::AtRest
        )
    }

    fn codex_in_turn(&self, flow_id: &str) -> bool {
        matches!(
            self.store.flow_node(flow_id),
            Ok(Some(node)) if node.harness_kind == HarnessKind::Codex
                && matches!(
                    self.codex_turns.standing(&node.session_id.0),
                    ThreadStanding::Working(_)
                        | ThreadStanding::LetterAwaited(_)
                        | ThreadStanding::ItemsRunning(_)
                        | ThreadStanding::ItemsOrphaned(_)
                )
        )
    }

    fn resting_codex_seat(&self, address: &Address) -> Option<FlowId> {
        let metaflow = self.store.metaflow(address).ok()??;
        let flow_ethos::State::Awake(flow_id) = metaflow.state else {
            return None;
        };
        (!self.waking.is_waking(address)
            && matches!(self.store.address_hold(&flow_id.0), Ok(None))
            && self.codex_resting(&flow_id.0))
        .then_some(flow_id)
    }

    fn note_retention(&self, thread: &str, reading: &LedgerReading) {
        if let Some(turn) = &reading.retention_exceeded.0 {
            let flow = self
                .codex_flow_on(thread)
                .map(|node| node.flow_id.0)
                .unwrap_or_default();
            eprintln!(
                "flow-nexus: RetentionExceeded {{ flow {flow} thread {thread} turn {turn} bound {bound} }}",
                turn = turn.0,
                bound = self.codex_turns.retention().0,
            );
        }
    }
}
