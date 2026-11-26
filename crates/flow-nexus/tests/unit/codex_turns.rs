//! Synthetic app-server wire inputs for turn-end and receipt tests.
use crate::generated::turn_ledger::{
    CodexTurnId, CodexTurnStatus, LedgerEvent, ThreadCut, TurnReport,
};

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
