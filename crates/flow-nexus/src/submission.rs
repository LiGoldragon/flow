//! A seat's own word that its harness took a prompt: the submission, as the
//! seat reports it, kept by native session so a writer that typed into the
//! seat's pane waits for it instead of reading the composer again.
//!
//! Claude Code runs the flow's hook on UserPromptSubmit, and the hook
//! reports `PromptSubmitted` with the digest of the prompt Claude took
//! (`reporting`); Codex's app-server announces `turn/started` on the
//! thread to every connection subscribed to it, read here on the
//! connection's own reader as it arrives (`codex`), so a responder busy
//! elsewhere never holds it back. Placing a text, its submission and the
//! recipient consuming it are three observations: a witness here is the
//! second, the submission reaching the harness, and stands for neither of
//! the others.
//!
//! A writer takes a mark before it types, then waits, bounded, for a
//! witness after the mark; nothing is read again on a timer. The state is
//! memory only: a submission witnessed while no writer waits is kept only
//! as long as the last few of its session are.

use std::collections::{HashMap, VecDeque};
use std::sync::{Condvar, Mutex};
use std::time::Instant;

/// What a seat reported of one submission.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SeatSubmission {
    /// Claude Code's UserPromptSubmit: the SHA-256 of the prompt it took,
    /// its paste wrapper taken off.
    Prompt(String),
    /// Codex's `turn/started` on the thread: a turn began on input it took.
    TurnStarted,
}

/// Where a session's submissions stood when a writer was about to type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubmissionMark {
    session: String,
    seen: u64,
}

/// How many recent submissions of one session are kept for a mark taken
/// before them.
const KEPT_SUBMISSIONS: usize = 16;

#[derive(Debug, Default)]
struct SessionSubmissions {
    count: u64,
    recent: VecDeque<(u64, SeatSubmission)>,
}

/// Every session's recent submissions, and the announcement of each.
#[derive(Debug, Default)]
pub struct SeatSubmissions {
    sessions: Mutex<HashMap<String, SessionSubmissions>>,
    witnessed: Condvar,
}

pub trait WitnessesSubmissions {
    /// The mark a writer takes before it types into the seat of `session`.
    fn mark(&self, session: &str) -> SubmissionMark;
    /// Records a submission the seat of `session` reported, and wakes
    /// every writer waiting on that session.
    fn witness(&self, session: &str, submission: SeatSubmission);
    /// Blocks until a submission after `mark` that `wanted` accepts is
    /// witnessed, or `deadline` passes; whether one was.
    fn witnessed_by(
        &self,
        mark: &SubmissionMark,
        wanted: &dyn Fn(&SeatSubmission) -> bool,
        deadline: Instant,
    ) -> bool;
}

impl WitnessesSubmissions for SeatSubmissions {
    fn mark(&self, session: &str) -> SubmissionMark {
        let sessions = self
            .sessions
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        SubmissionMark {
            session: session.to_owned(),
            seen: sessions.get(session).map_or(0, |state| state.count),
        }
    }

    fn witness(&self, session: &str, submission: SeatSubmission) {
        if session.is_empty() {
            return;
        }
        let mut sessions = self
            .sessions
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let state = sessions.entry(session.to_owned()).or_default();
        state.count += 1;
        let count = state.count;
        state.recent.push_back((count, submission));
        if state.recent.len() > KEPT_SUBMISSIONS {
            state.recent.pop_front();
        }
        drop(sessions);
        self.witnessed.notify_all();
    }

    fn witnessed_by(
        &self,
        mark: &SubmissionMark,
        wanted: &dyn Fn(&SeatSubmission) -> bool,
        deadline: Instant,
    ) -> bool {
        if mark.session.is_empty() {
            return false;
        }
        let mut sessions = self
            .sessions
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        loop {
            let found = sessions.get(&mark.session).is_some_and(|state| {
                state
                    .recent
                    .iter()
                    .any(|(count, submission)| *count > mark.seen && wanted(submission))
            });
            if found {
                return true;
            }
            let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
                return false;
            };
            sessions = self
                .witnessed
                .wait_timeout(sessions, remaining)
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .0;
        }
    }
}

/// What a Codex app-server connection's reader is told of each message it
/// reads, on the reader's own thread, before the message is queued for the
/// connection's owner.
pub trait NotesAppServerMessage: Send + Sync {
    fn noted(&self, message: &serde_json::Value);
}

impl NotesAppServerMessage for SeatSubmissions {
    fn noted(&self, message: &serde_json::Value) {
        if message.get("id").is_some()
            || message.get("method").and_then(serde_json::Value::as_str) != Some("turn/started")
        {
            return;
        }
        if let Some(thread) = message
            .pointer("/params/threadId")
            .and_then(serde_json::Value::as_str)
        {
            self.witness(thread, SeatSubmission::TurnStarted);
        }
    }
}

/// A connection whose messages nobody waits on for a submission: a
/// launch's own opening connection, whose first turn Flow submits itself.
pub struct NothingNoted;

impl NotesAppServerMessage for NothingNoted {
    fn noted(&self, _message: &serde_json::Value) {}
}

#[cfg(test)]
mod tests {
    use super::{NotesAppServerMessage, SeatSubmission, SeatSubmissions, WitnessesSubmissions};
    use std::time::{Duration, Instant};

    #[test]
    fn a_submission_after_the_mark_answers_and_one_before_it_does_not() {
        let submissions = SeatSubmissions::default();
        submissions.witness("s", SeatSubmission::Prompt("before".into()));
        let mark = submissions.mark("s");
        let wanted = |digest: &str| {
            let digest = digest.to_owned();
            move |submission: &SeatSubmission| *submission == SeatSubmission::Prompt(digest.clone())
        };
        assert!(!submissions.witnessed_by(&mark, &wanted("before"), Instant::now()));
        std::thread::scope(|scope| {
            scope.spawn(|| submissions.witness("s", SeatSubmission::Prompt("after".into())));
            assert!(submissions.witnessed_by(
                &mark,
                &wanted("after"),
                Instant::now() + Duration::from_secs(30)
            ));
        });
        // Another session's submission is not this one's.
        let other = submissions.mark("t");
        submissions.witness("s", SeatSubmission::Prompt("third".into()));
        assert!(!submissions.witnessed_by(&other, &wanted("third"), Instant::now()));
    }

    #[test]
    fn a_turn_started_notification_is_its_threads_submission() {
        let submissions = SeatSubmissions::default();
        let mark = submissions.mark("thread-a");
        submissions.noted(&serde_json::json!({
            "method": "turn/completed",
            "params": { "threadId": "thread-a", "turn": { "id": "t1" } }
        }));
        submissions.noted(&serde_json::json!({
            "method": "turn/started",
            "params": { "threadId": "thread-b", "turn": { "id": "t2" } }
        }));
        let started = |submission: &SeatSubmission| *submission == SeatSubmission::TurnStarted;
        assert!(!submissions.witnessed_by(&mark, &started, Instant::now()));
        submissions.noted(&serde_json::json!({
            "method": "turn/started",
            "params": { "threadId": "thread-a", "turn": { "id": "t3" } }
        }));
        assert!(submissions.witnessed_by(&mark, &started, Instant::now()));
    }
}
