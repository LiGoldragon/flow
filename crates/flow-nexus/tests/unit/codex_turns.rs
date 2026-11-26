//! A Codex thread's turn ledger (primary-k7jy): rest is a standing the
//! app-server's own lifecycle shows, reached the same in every order its
//! events arrive. Each order-dependent case runs in every permutation of
//! its events and ends in the same standing.

use crate::codex_turns::{CodexTurns, ReadsLedgerEvents, ReadsThreadCut, TracksCodexTurns};
use crate::generated::turn_ledger::{
    BackgroundTerminal, CodexItemId, CodexTurnId, CodexTurnStatus, CutItem, Interrupt, LedgerEvent,
    LedgerReading, LetterReceived_Data, PayloadText, PendingLetter, ProcessBacked_Data, SeenLetter,
    ThreadCut, ThreadStanding, TurnBound_Data, TurnReport, UnifiedExecProcessId, WorkItem,
};
use meta_signal_flow::{QueueAttemptId, TurnRetentionBound};

pub(crate) const THREAD: &str = "01a1db5d-thread";
// UUIDv7 turn ids: a later turn's is greater.
pub(crate) const T0: &str = "01a1db5d-0000";
pub(crate) const T1: &str = "01a1db5d-5979";
pub(crate) const T2: &str = "01a1db60-5c94";
pub(crate) const T3: &str = "01a1db61-0000";

pub(crate) fn turn(id: &str) -> CodexTurnId {
    CodexTurnId(id.into())
}

pub(crate) fn started(id: &str) -> LedgerEvent {
    LedgerEvent::TurnOpened(turn(id))
}

pub(crate) fn ended(id: &str, status: CodexTurnStatus) -> LedgerEvent {
    LedgerEvent::TurnClosed(TurnReport {
        codex_turn_id: turn(id),
        codex_turn_status: status,
    })
}

pub(crate) fn completed(id: &str) -> LedgerEvent {
    ended(id, CodexTurnStatus::Completed)
}

pub(crate) fn interrupted(id: &str) -> LedgerEvent {
    ended(id, CodexTurnStatus::Interrupted)
}

/// The responder's cut, listing `turns` with their status.
pub(crate) fn cut(turns: &[(&str, CodexTurnStatus)]) -> LedgerEvent {
    LedgerEvent::Cut(ThreadCut {
        turn_report_vector: turns
            .iter()
            .map(|(id, status)| TurnReport {
                codex_turn_id: turn(id),
                codex_turn_status: status.clone(),
            })
            .collect(),
        seen_letter_vector: Vec::new(),
        cut_item_vector: Vec::new(),
    })
}

/// A cut of a thread with no turn: the ledger established, nothing open.
pub(crate) fn idle() -> LedgerEvent {
    cut(&[])
}

pub(crate) fn typed(attempt: &str, text: &str, interrupt: Interrupt) -> LedgerEvent {
    LedgerEvent::LetterTyped(PendingLetter {
        queue_attempt_id: QueueAttemptId(attempt.into()),
        payload_text: PayloadText(text.into()),
        interrupt,
    })
}

/// A user message item in `turn` whose only content is `text`.
pub(crate) fn seen(turn_id: &str, item: &str, text: &str) -> LedgerEvent {
    LedgerEvent::LetterSeen(SeenLetter {
        codex_turn_id: turn(turn_id),
        codex_item_id: CodexItemId(item.into()),
        payload_text: PayloadText(text.into()),
    })
}

fn process(item: &str, turn_id: &str) -> WorkItem {
    WorkItem::ProcessBacked(ProcessBacked_Data {
        codex_item_id: CodexItemId(item.into()),
        codex_turn_id: turn(turn_id),
        unified_exec_process_id: UnifiedExecProcessId(format!("process-{item}")),
    })
}

fn turn_bound(item: &str, turn_id: &str) -> WorkItem {
    WorkItem::TurnBound(TurnBound_Data {
        codex_item_id: CodexItemId(item.into()),
        codex_turn_id: turn(turn_id),
    })
}

fn attempts(ids: &[&str]) -> Vec<QueueAttemptId> {
    ids.iter().map(|id| QueueAttemptId((*id).into())).collect()
}

fn working(ids: &[&str]) -> ThreadStanding {
    ThreadStanding::Working(ids.iter().map(|id| turn(id)).collect())
}

fn ledger_keeping(bound: i64) -> CodexTurns {
    let turns = CodexTurns::default();
    turns.retain(TurnRetentionBound(bound));
    turns
}

fn apply_all(turns: &CodexTurns, events: Vec<LedgerEvent>) -> Vec<LedgerReading> {
    events
        .into_iter()
        .map(|event| turns.apply(THREAD, event))
        .collect()
}

fn permutations(events: &[LedgerEvent]) -> Vec<Vec<LedgerEvent>> {
    if events.len() <= 1 {
        return vec![events.to_vec()];
    }
    let mut all = Vec::new();
    for index in 0..events.len() {
        let mut rest = events.to_vec();
        let first = rest.remove(index);
        for mut tail in permutations(&rest) {
            tail.insert(0, first.clone());
            all.push(tail);
        }
    }
    all
}

/// Runs `prefix`, then every permutation of `shuffled`, each on its own
/// ledger; every one ends in `expected`.
fn in_every_order(prefix: &[LedgerEvent], shuffled: &[LedgerEvent], expected: ThreadStanding) {
    for order in permutations(shuffled) {
        let turns = ledger_keeping(64);
        apply_all(&turns, prefix.to_vec());
        apply_all(&turns, order.clone());
        assert_eq!(turns.standing(THREAD), expected, "{order:?}");
    }
}

/// Runs `prefix`, then every permutation of `shuffled`: no step of any
/// order steps into rest.
fn never_rests_in_any_order(prefix: &[LedgerEvent], shuffled: &[LedgerEvent]) {
    for order in permutations(shuffled) {
        let turns = ledger_keeping(64);
        apply_all(&turns, prefix.to_vec());
        let readings = apply_all(&turns, order.clone());
        assert!(
            readings.iter().all(|reading| !reading.rest_reached.0),
            "no step of {order:?} rests"
        );
    }
}

/// Test 1, afc2ca: three letters steered into T1 and taken there, then a
/// HardAbrupt typed after the interrupt keys. T1's end and T2's start, in
/// either order, rest nothing: the HardAbrupt still waits for its receipt,
/// which comes in T2; T2's end is the one rest. Against the state machine
/// it replaces, T1's end rested the seat: the regression witness.
#[test]
fn a_steer_then_an_interrupt_rests_only_at_the_letter_s_own_turn_end() {
    let turns = ledger_keeping(64);
    let steps = vec![
        (cut(&[(T1, CodexTurnStatus::InProgress)]), working(&[T1])),
        (
            typed("1smj", "steer one", Interrupt::NotPressed),
            working(&[T1]),
        ),
        (seen(T1, "i1", "steer one"), working(&[T1])),
        (
            typed("a6qy", "steer two", Interrupt::NotPressed),
            working(&[T1]),
        ),
        (seen(T1, "i2", "steer two"), working(&[T1])),
        (
            typed("jb56", "steer three", Interrupt::NotPressed),
            working(&[T1]),
        ),
        (seen(T1, "i3", "steer three"), working(&[T1])),
        (typed("4ygn", "hard", Interrupt::Pressed), working(&[T1])),
        (
            interrupted(T1),
            ThreadStanding::LetterAwaited(attempts(&["4ygn"])),
        ),
        (started(T2), working(&[T2])),
        (seen(T2, "i4", "hard"), working(&[T2])),
        (completed(T2), ThreadStanding::AtRest),
    ];
    let last = steps.len() - 1;
    for (index, (event, standing)) in steps.into_iter().enumerate() {
        let reading = turns.apply(THREAD, event);
        assert_eq!(reading.thread_standing, standing, "step {index}");
        assert_eq!(reading.rest_reached.0, index == last, "step {index}");
        assert!(!reading.reconcile.0, "step {index}");
    }
    let letter_typed = [
        cut(&[(T1, CodexTurnStatus::InProgress)]),
        typed("4ygn", "hard", Interrupt::Pressed),
    ];
    in_every_order(
        &letter_typed,
        &[interrupted(T1), started(T2)],
        working(&[T2]),
    );
    never_rests_in_any_order(&letter_typed, &[interrupted(T1), started(T2)]);
}

/// Test 2: no turn's start takes a letter; its receipt or the queue's
/// settlement does.
#[test]
fn a_turn_start_takes_no_letter() {
    for taking in [
        seen(T2, "i1", "hard"),
        LedgerEvent::LetterSettled(QueueAttemptId("a".into())),
    ] {
        let turns = ledger_keeping(64);
        apply_all(
            &turns,
            vec![
                idle(),
                typed("a", "hard", Interrupt::Pressed),
                started(T2),
                completed(T2),
            ],
        );
        assert_eq!(
            turns.standing(THREAD),
            ThreadStanding::LetterAwaited(attempts(&["a"]))
        );
        let reading = turns.apply(THREAD, taking);
        assert_eq!(reading.thread_standing, ThreadStanding::AtRest);
        assert!(reading.rest_reached.0);
    }
}

/// Test 3: a steer the interrupt discarded has no receipt; it stays
/// awaited through the turn's end and a cut that shows the turn ended.
#[test]
fn a_discarded_steer_stays_awaited() {
    let turns = ledger_keeping(64);
    apply_all(
        &turns,
        vec![
            cut(&[(T1, CodexTurnStatus::InProgress)]),
            typed("a", "steer", Interrupt::NotPressed),
            interrupted(T1),
        ],
    );
    let awaited = ThreadStanding::LetterAwaited(attempts(&["a"]));
    assert_eq!(turns.standing(THREAD), awaited);
    let reading = turns.apply(THREAD, cut(&[(T1, CodexTurnStatus::Interrupted)]));
    assert_eq!(reading.thread_standing, awaited);
}

/// Test 4, F2: the first turn opened from turn/start's answer is kept by a
/// cut taken before the listener saw it, empty or listing a lesser turn.
#[test]
fn a_stale_cut_keeps_the_turn_turn_start_answered() {
    in_every_order(&[], &[started(T1), idle()], working(&[T1]));
    in_every_order(
        &[],
        &[started(T1), cut(&[(T0, CodexTurnStatus::Completed)])],
        working(&[T1]),
    );
}

/// Test 5: a cut listing a greater turn ends the lesser open one.
#[test]
fn a_cut_listing_a_greater_turn_ends_the_lesser() {
    let turns = ledger_keeping(64);
    apply_all(&turns, vec![idle(), started(T1)]);
    let reading = turns.apply(THREAD, cut(&[(T2, CodexTurnStatus::InProgress)]));
    assert_eq!(reading.thread_standing, working(&[T2]));
    assert_eq!(reading.turns_ended.0, vec![turn(T1)]);
}

/// Test 6, and acceptance (b): a start that another connection delivered
/// before the cut, and a letter Flow typed, both newer than the cut's
/// boundary, are kept whatever order they and the cut arrive in.
#[test]
fn a_cut_keeps_events_and_typing_newer_than_its_boundary() {
    in_every_order(
        &[],
        &[started(T3), cut(&[(T2, CodexTurnStatus::Completed)])],
        working(&[T3]),
    );
    in_every_order(
        &[],
        &[
            started(T3),
            typed("a", "letter", Interrupt::NotPressed),
            cut(&[(T2, CodexTurnStatus::Completed)]),
        ],
        working(&[T3]),
    );
    in_every_order(
        &[],
        &[
            started(T3),
            typed("a", "letter", Interrupt::NotPressed),
            cut(&[(T2, CodexTurnStatus::Completed)]),
            completed(T3),
        ],
        ThreadStanding::LetterAwaited(attempts(&["a"])),
    );
}

/// Test 7: a thread status moves nothing; it is no ledger event at all.
#[test]
fn a_thread_status_is_no_ledger_event() {
    for status in ["idle", "active", "notLoaded"] {
        let notification = serde_json::json!({
            "method": "thread/status/changed",
            "params": { "threadId": THREAD, "status": { "type": status } }
        });
        assert!(notification.ledger_events(THREAD).is_empty());
    }
}

/// Test 8: a process-backed item outlives its turn and ends with its own
/// completion; a late start of it changes nothing; a turn-bound item ends
/// with its turn.
#[test]
fn a_process_backed_item_outlives_its_turn() {
    let turns = ledger_keeping(64);
    apply_all(
        &turns,
        vec![
            idle(),
            started(T1),
            LedgerEvent::ItemOpened(process("x", T1)),
        ],
    );
    let reading = turns.apply(THREAD, completed(T1));
    assert_eq!(
        reading.thread_standing,
        ThreadStanding::ItemsRunning(vec![CodexItemId("x".into())])
    );
    assert!(!reading.rest_reached.0);
    let reading = turns.apply(THREAD, LedgerEvent::ItemClosed(process("x", T1)));
    assert!(reading.rest_reached.0);
    let reading = turns.apply(THREAD, LedgerEvent::ItemOpened(process("x", T1)));
    assert_eq!(reading.thread_standing, ThreadStanding::AtRest);
    apply_all(
        &turns,
        vec![started(T2), LedgerEvent::ItemOpened(turn_bound("y", T2))],
    );
    let reading = turns.apply(THREAD, completed(T2));
    assert_eq!(reading.thread_standing, ThreadStanding::AtRest);
    assert!(reading.rest_reached.0);
}

/// Test 9: the terminals read after a cut open items and never end one.
#[test]
fn background_terminals_open_items_and_end_none() {
    let turns = ledger_keeping(64);
    let terminal = BackgroundTerminal {
        codex_item_id: CodexItemId("z".into()),
        unified_exec_process_id: UnifiedExecProcessId("10873".into()),
    };
    apply_all(
        &turns,
        vec![idle(), LedgerEvent::BackgroundTerminals(vec![terminal])],
    );
    let running = ThreadStanding::ItemsRunning(vec![CodexItemId("z".into())]);
    assert_eq!(turns.standing(THREAD), running);
    turns.apply(THREAD, LedgerEvent::BackgroundTerminals(Vec::new()));
    assert_eq!(turns.standing(THREAD), running);
}

/// Test 10: thread/closed ends turns and turn-bound items; a process-backed
/// item is orphaned, never rest.
#[test]
fn a_closed_thread_ends_turns_and_orphans_processes() {
    let turns = ledger_keeping(64);
    apply_all(
        &turns,
        vec![
            idle(),
            started(T1),
            LedgerEvent::ItemOpened(turn_bound("y", T1)),
        ],
    );
    let reading = turns.apply(THREAD, LedgerEvent::ThreadClosed);
    assert_eq!(reading.thread_standing, ThreadStanding::AtRest);
    assert!(reading.rest_reached.0);

    let turns = ledger_keeping(64);
    apply_all(
        &turns,
        vec![
            idle(),
            started(T1),
            LedgerEvent::ItemOpened(process("x", T1)),
        ],
    );
    let reading = turns.apply(THREAD, LedgerEvent::ThreadClosed);
    assert_eq!(
        reading.thread_standing,
        ThreadStanding::ItemsOrphaned(vec![CodexItemId("x".into())])
    );
    assert!(!reading.rest_reached.0);
}

/// Test 11: with retention 0 a late start of an ended turn falls below the
/// watermark: the ledger stands Unreconciled, asks for a cut and names the
/// turn, never rest, until the next cut. With 1 it is a silent repeat.
/// Lowering the bound live raises the watermark.
#[test]
fn retention_bounds_repeats_and_exceeding_it_reconciles() {
    let turns = ledger_keeping(0);
    apply_all(&turns, vec![idle(), started(T1), completed(T1)]);
    let reading = turns.apply(THREAD, started(T1));
    assert_eq!(reading.thread_standing, ThreadStanding::Unreconciled);
    assert!(reading.reconcile.0);
    assert_eq!(reading.retention_exceeded.0, Some(turn(T1)));
    assert!(!reading.rest_reached.0);
    let reading = turns.apply(THREAD, idle());
    assert_eq!(reading.thread_standing, ThreadStanding::AtRest);

    let turns = ledger_keeping(1);
    apply_all(&turns, vec![idle(), started(T1), completed(T1)]);
    let reading = turns.apply(THREAD, started(T1));
    assert_eq!(reading.thread_standing, ThreadStanding::AtRest);
    assert!(!reading.reconcile.0);
    assert_eq!(reading.retention_exceeded.0, None);

    let turns = ledger_keeping(64);
    apply_all(
        &turns,
        vec![
            idle(),
            started(T1),
            completed(T1),
            started(T2),
            completed(T2),
        ],
    );
    assert_eq!(
        turns.apply(THREAD, completed(T1)).retention_exceeded.0,
        None
    );
    turns.retain(TurnRetentionBound(1));
    let reading = turns.apply(THREAD, completed(T1));
    assert_eq!(reading.retention_exceeded.0, Some(turn(T1)));
    assert_eq!(reading.thread_standing, ThreadStanding::Unreconciled);
}

/// Test 12: a subscription's end leaves the ledger Unreconciled, never at
/// rest, until the next subscription's cut.
#[test]
fn a_subscription_end_is_never_rest_until_the_next_cut() {
    let turns = ledger_keeping(64);
    apply_all(&turns, vec![idle()]);
    let reading = turns.apply(THREAD, LedgerEvent::SubscriptionEnded);
    assert_eq!(reading.thread_standing, ThreadStanding::Unreconciled);
    assert!(reading.reconcile.0);
    let reading = turns.apply(THREAD, completed(T1));
    assert_eq!(reading.thread_standing, ThreadStanding::Unreconciled);
    let reading = turns.apply(THREAD, idle());
    assert_eq!(reading.thread_standing, ThreadStanding::AtRest);
    assert!(reading.rest_reached.0);
    // A thread Flow holds no cut of rests nowhere.
    assert_eq!(
        CodexTurns::default().standing("unknown"),
        ThreadStanding::Unreconciled
    );
}

/// Acceptance (a): an item takes at most one letter, never a second letter
/// of the same text typed after it, and two pending letters of one text
/// take none.
#[test]
fn identical_payload_text_never_takes_the_wrong_or_several_letters() {
    let turns = ledger_keeping(64);
    apply_all(
        &turns,
        vec![
            cut(&[(T1, CodexTurnStatus::InProgress)]),
            typed("a1", "same", Interrupt::NotPressed),
            seen(T1, "i1", "same"),
            typed("a2", "same", Interrupt::NotPressed),
            // item/completed repeats item/started's item, and another
            // connection repeats both.
            seen(T1, "i1", "same"),
            seen(T1, "i1", "same"),
            completed(T1),
        ],
    );
    assert_eq!(
        turns.standing(THREAD),
        ThreadStanding::LetterAwaited(attempts(&["a2"]))
    );

    let turns = ledger_keeping(64);
    apply_all(
        &turns,
        vec![
            cut(&[(T1, CodexTurnStatus::InProgress)]),
            typed("a1", "same", Interrupt::NotPressed),
            typed("a2", "same", Interrupt::NotPressed),
            seen(T1, "i1", "same"),
            seen(T1, "i2", "same"),
            completed(T1),
        ],
    );
    assert_eq!(
        turns.standing(THREAD),
        ThreadStanding::LetterAwaited(attempts(&["a1", "a2"]))
    );
}

/// A receipt the queue recorded from history takes the letter only in a
/// turn the ledger knows; in one it has not seen, the letter waits for that
/// turn's own item.
#[test]
fn a_history_receipt_takes_a_letter_only_in_a_known_turn() {
    let received = |turn_id: &str| {
        LedgerEvent::LetterReceived(LetterReceived_Data {
            queue_attempt_id: QueueAttemptId("a".into()),
            codex_turn_id: turn(turn_id),
        })
    };
    let turns = ledger_keeping(64);
    apply_all(
        &turns,
        vec![idle(), typed("a", "letter", Interrupt::NotPressed)],
    );
    let reading = turns.apply(THREAD, received(T2));
    assert_eq!(
        reading.thread_standing,
        ThreadStanding::LetterAwaited(attempts(&["a"]))
    );
    apply_all(&turns, vec![started(T2)]);
    let reading = turns.apply(THREAD, received(T2));
    assert_eq!(reading.thread_standing, working(&[T2]));
}

/// The responder reads its events off the wire: a turn's start and end
/// with its status, a user message's one text, a command with a live
/// process, the thread's close; nothing from another thread, an answer, or
/// a status.
#[test]
fn the_wire_events_are_read_into_ledger_events() {
    let notification = |method: &str, params: serde_json::Value| serde_json::json!({ "method": method, "params": params });
    assert_eq!(
        notification(
            "turn/started",
            serde_json::json!({ "threadId": THREAD, "turn": { "id": T1, "status": "inProgress" } })
        )
        .ledger_events(THREAD),
        vec![started(T1)]
    );
    for (status, expected) in [
        ("interrupted", interrupted(T1)),
        ("completed", completed(T1)),
        ("failed", ended(T1, CodexTurnStatus::Failed)),
    ] {
        assert_eq!(
            notification(
                "turn/completed",
                serde_json::json!({ "threadId": THREAD, "turn": { "id": T1, "status": status } })
            )
            .ledger_events(THREAD),
            vec![expected]
        );
    }
    let user_message = serde_json::json!({
        "type": "userMessage", "id": "i1", "clientId": null,
        "content": [{ "type": "text", "text": "hard", "text_elements": [] }]
    });
    assert_eq!(
        notification(
            "item/completed",
            serde_json::json!({ "threadId": THREAD, "turnId": T2, "item": user_message })
        )
        .ledger_events(THREAD),
        vec![seen(T2, "i1", "hard")]
    );
    let command = serde_json::json!({
        "type": "commandExecution", "id": "call_a3e6d7", "status": "inProgress",
        "processId": "10873", "command": "sleep 300"
    });
    assert_eq!(
        notification(
            "item/started",
            serde_json::json!({ "threadId": THREAD, "turnId": T1, "item": command })
        )
        .ledger_events(THREAD),
        vec![LedgerEvent::ItemOpened(WorkItem::ProcessBacked(
            ProcessBacked_Data {
                codex_item_id: CodexItemId("call_a3e6d7".into()),
                codex_turn_id: turn(T1),
                unified_exec_process_id: UnifiedExecProcessId("10873".into()),
            }
        ))]
    );
    assert_eq!(
        notification("thread/closed", serde_json::json!({ "threadId": THREAD }))
            .ledger_events(THREAD),
        vec![LedgerEvent::ThreadClosed]
    );
    assert!(
        notification("thread/closed", serde_json::json!({ "threadId": "other" }))
            .ledger_events(THREAD)
            .is_empty()
    );
    assert!(
        serde_json::json!({ "id": 4, "result": { "turn": { "id": T1, "status": "inProgress" } } })
            .ledger_events(THREAD)
            .is_empty()
    );
}

/// The resume answer's newest turn is the cut, its letters and its work
/// items with them; the terminal list names each kept process.
#[test]
fn a_resume_answer_is_read_as_a_cut() {
    let answer = serde_json::json!({
        "thread": { "id": THREAD, "status": { "type": "active", "activeFlags": [] } },
        "initialTurnsPage": { "data": [{
            "id": T2, "status": "inProgress",
            "items": [
                { "type": "userMessage", "id": "i4",
                  "content": [{ "type": "text", "text": "hard", "text_elements": [] }] },
                { "type": "commandExecution", "id": "c1", "status": "inProgress", "processId": "77" },
                { "type": "commandExecution", "id": "c0", "status": "completed", "processId": "76" },
                { "type": "agentMessage", "id": "m1", "text": "ok" }
            ]
        }] }
    });
    assert_eq!(
        answer.thread_cut(),
        Some(ThreadCut {
            turn_report_vector: vec![TurnReport {
                codex_turn_id: turn(T2),
                codex_turn_status: CodexTurnStatus::InProgress,
            }],
            seen_letter_vector: vec![SeenLetter {
                codex_turn_id: turn(T2),
                codex_item_id: CodexItemId("i4".into()),
                payload_text: PayloadText("hard".into()),
            }],
            cut_item_vector: vec![
                CutItem::Running(WorkItem::ProcessBacked(ProcessBacked_Data {
                    codex_item_id: CodexItemId("c1".into()),
                    codex_turn_id: turn(T2),
                    unified_exec_process_id: UnifiedExecProcessId("77".into()),
                })),
                CutItem::Ended(WorkItem::ProcessBacked(ProcessBacked_Data {
                    codex_item_id: CodexItemId("c0".into()),
                    codex_turn_id: turn(T2),
                    unified_exec_process_id: UnifiedExecProcessId("76".into()),
                })),
            ],
        })
    );
    assert_eq!(serde_json::json!({ "error": {} }).thread_cut(), None);
    assert_eq!(
        serde_json::json!({ "data": [{ "itemId": "c1", "processId": "77", "command": "sleep" }] })
            .background_terminals(),
        vec![BackgroundTerminal {
            codex_item_id: CodexItemId("c1".into()),
            unified_exec_process_id: UnifiedExecProcessId("77".into()),
        }]
    );
}

/// Completed native turns do not erase uncertain letters. The exact
/// original cohort receipt releases every represented pending member,
/// including an older cohort, independently of terminal siblings in store.
#[test]
fn exact_receipts_release_pending_members_of_two_completed_cohorts() {
    let ledger = CodexTurns::default();
    ledger.apply(THREAD, idle());
    for (attempt, payload) in [
        ("older-a", "older-original"),
        ("older-b", "older-original"),
        ("newer-a", "newer-original"),
    ] {
        ledger.apply(THREAD, typed(attempt, payload, Interrupt::NotPressed));
    }
    ledger.apply(THREAD, started(T1));
    ledger.apply(THREAD, completed(T1));
    ledger.apply(THREAD, started(T2));
    ledger.apply(THREAD, completed(T2));
    assert!(matches!(
        ledger.standing(THREAD),
        ThreadStanding::LetterAwaited(_)
    ));
    ledger.apply(
        THREAD,
        LedgerEvent::LetterReceived(LetterReceived_Data {
            queue_attempt_id: QueueAttemptId("newer-a".into()),
            codex_turn_id: turn(T2),
        }),
    );
    assert!(
        matches!(ledger.standing(THREAD), ThreadStanding::LetterAwaited(_)),
        "older letters remain real obligations"
    );
    for attempt in ["older-a", "older-b"] {
        ledger.apply(
            THREAD,
            LedgerEvent::LetterReceived(LetterReceived_Data {
                queue_attempt_id: QueueAttemptId(attempt.into()),
                codex_turn_id: turn(T1),
            }),
        );
    }
    assert_eq!(ledger.standing(THREAD), ThreadStanding::AtRest);
}
