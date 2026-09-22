//! Pure decision boundary for a reserved Flow start.
//!
//! The caller must persist each returned state before sending another native
//! command. This module does not start a thread, delete a reservation, or
//! claim that an ambiguous transport error means no native thread exists.

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PendingAttempt {
    pub flow_id: String,
    pub attempt_id: String,
    pub state: AttemptState,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum AttemptState {
    Reserved,
    Uncertain {
        evidence_id: String,
    },
    ThreadAccepted {
        thread_id: String,
        evidence_id: String,
    },
    Failed {
        evidence_id: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum StartObservation {
    /// A native adapter receipt proves that no thread was accepted.
    DefinitiveNoThread {
        flow_id: String,
        attempt_id: String,
        evidence_id: String,
    },
    /// An independently resolved exact native thread belongs to this attempt.
    ThreadAccepted {
        flow_id: String,
        attempt_id: String,
        thread_id: String,
        evidence_id: String,
    },
    /// Timeout, lost reply, or a failed persistence callback: acceptance unknown.
    Uncertain {
        flow_id: String,
        attempt_id: String,
        evidence_id: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Decision {
    Changed(AttemptState),
    Replayed(AttemptState),
    Unchanged(AttemptState),
    Rejected(Rejection),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Rejection {
    MissingIdentity,
    OtherFlow,
    OtherAttempt,
    ConflictingEvidence,
}

impl PendingAttempt {
    pub(crate) fn new(flow_id: String, attempt_id: String) -> Result<Self, Rejection> {
        if flow_id.is_empty() || attempt_id.is_empty() {
            return Err(Rejection::MissingIdentity);
        }
        Ok(Self {
            flow_id,
            attempt_id,
            state: AttemptState::Reserved,
        })
    }

    /// Calculate the next state without changing `self`. The store must write
    /// the returned state with its exact attempt key in one guarded mutation.
    pub(crate) fn observe(&self, observation: &StartObservation) -> Decision {
        if self.flow_id.is_empty() || self.attempt_id.is_empty() {
            return Decision::Rejected(Rejection::MissingIdentity);
        }
        let (flow_id, attempt_id, evidence_id) = match observation {
            StartObservation::DefinitiveNoThread {
                flow_id,
                attempt_id,
                evidence_id,
            }
            | StartObservation::ThreadAccepted {
                flow_id,
                attempt_id,
                evidence_id,
                ..
            }
            | StartObservation::Uncertain {
                flow_id,
                attempt_id,
                evidence_id,
            } => (flow_id, attempt_id, evidence_id),
        };
        if flow_id != &self.flow_id {
            return Decision::Rejected(Rejection::OtherFlow);
        }
        if attempt_id != &self.attempt_id {
            return Decision::Rejected(Rejection::OtherAttempt);
        }
        if evidence_id.is_empty() {
            return Decision::Rejected(Rejection::MissingIdentity);
        }
        if let StartObservation::ThreadAccepted { thread_id, .. } = observation {
            if thread_id.is_empty() {
                return Decision::Rejected(Rejection::MissingIdentity);
            }
        }

        let proposed = match observation {
            StartObservation::DefinitiveNoThread { evidence_id, .. } => AttemptState::Failed {
                evidence_id: evidence_id.clone(),
            },
            StartObservation::ThreadAccepted {
                thread_id,
                evidence_id,
                ..
            } => AttemptState::ThreadAccepted {
                thread_id: thread_id.clone(),
                evidence_id: evidence_id.clone(),
            },
            StartObservation::Uncertain { evidence_id, .. } => AttemptState::Uncertain {
                evidence_id: evidence_id.clone(),
            },
        };
        if self.state == proposed {
            return Decision::Replayed(proposed);
        }
        if let AttemptState::Uncertain {
            evidence_id: previous,
        } = &self.state
        {
            if let AttemptState::Failed { evidence_id: next }
            | AttemptState::ThreadAccepted {
                evidence_id: next, ..
            } = &proposed
            {
                if previous == next {
                    return Decision::Rejected(Rejection::ConflictingEvidence);
                }
            }
        }
        match (&self.state, &proposed) {
            (AttemptState::Reserved, _)
            | (AttemptState::Uncertain { .. }, AttemptState::Failed { .. })
            | (AttemptState::Uncertain { .. }, AttemptState::ThreadAccepted { .. }) => {
                Decision::Changed(proposed)
            }
            (AttemptState::Uncertain { .. }, AttemptState::Uncertain { .. }) => {
                Decision::Unchanged(self.state.clone())
            }
            (
                AttemptState::ThreadAccepted {
                    thread_id: current, ..
                },
                AttemptState::ThreadAccepted {
                    thread_id: observed,
                    ..
                },
            ) if current == observed => Decision::Unchanged(self.state.clone()),
            (AttemptState::Failed { .. }, AttemptState::Failed { .. }) => {
                Decision::Unchanged(self.state.clone())
            }
            (AttemptState::ThreadAccepted { .. }, AttemptState::Uncertain { .. })
            | (AttemptState::Failed { .. }, AttemptState::Uncertain { .. }) => {
                Decision::Unchanged(self.state.clone())
            }
            _ => Decision::Rejected(Rejection::ConflictingEvidence),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn attempt() -> PendingAttempt {
        PendingAttempt::new("flow-1".into(), "attempt-1".into()).unwrap()
    }

    #[test]
    fn uncertain_start_stays_reconcilable_across_reopen_and_replay() {
        let pending = attempt();
        let uncertain = StartObservation::Uncertain {
            flow_id: "flow-1".into(),
            attempt_id: "attempt-1".into(),
            evidence_id: "timeout-1".into(),
        };
        let Decision::Changed(state) = pending.observe(&uncertain) else {
            panic!("uncertain must persist");
        };
        let reopened = PendingAttempt {
            state: state.clone(),
            ..pending
        };
        assert_eq!(reopened.observe(&uncertain), Decision::Replayed(state));
        assert_eq!(
            reopened.observe(&StartObservation::Uncertain {
                flow_id: "flow-1".into(),
                attempt_id: "attempt-1".into(),
                evidence_id: "later-timeout".into(),
            }),
            Decision::Unchanged(reopened.state.clone())
        );
        assert_eq!(
            reopened.observe(&StartObservation::ThreadAccepted {
                flow_id: "flow-1".into(),
                attempt_id: "attempt-1".into(),
                thread_id: "native-1".into(),
                evidence_id: "native-read-1".into()
            }),
            Decision::Changed(AttemptState::ThreadAccepted {
                thread_id: "native-1".into(),
                evidence_id: "native-read-1".into()
            })
        );
    }

    #[test]
    fn terminal_failure_requires_definitive_no_thread_receipt() {
        let pending = attempt();
        let uncertain = StartObservation::Uncertain {
            flow_id: "flow-1".into(),
            attempt_id: "attempt-1".into(),
            evidence_id: "lost-reply".into(),
        };
        let Decision::Changed(state) = pending.observe(&uncertain) else {
            panic!("hold uncertainty");
        };
        assert!(matches!(state, AttemptState::Uncertain { .. }));
        let reopened = PendingAttempt { state, ..pending };
        assert_eq!(
            reopened.observe(&StartObservation::DefinitiveNoThread {
                flow_id: "flow-1".into(),
                attempt_id: "attempt-1".into(),
                evidence_id: "lost-reply".into(),
            }),
            Decision::Rejected(Rejection::ConflictingEvidence)
        );
        assert_eq!(
            reopened.observe(&StartObservation::ThreadAccepted {
                flow_id: "flow-1".into(),
                attempt_id: "attempt-1".into(),
                thread_id: "native-1".into(),
                evidence_id: "lost-reply".into(),
            }),
            Decision::Rejected(Rejection::ConflictingEvidence)
        );
        let refused = StartObservation::DefinitiveNoThread {
            flow_id: "flow-1".into(),
            attempt_id: "attempt-1".into(),
            evidence_id: "native-refusal".into(),
        };
        assert_eq!(
            reopened.observe(&refused),
            Decision::Changed(AttemptState::Failed {
                evidence_id: "native-refusal".into()
            })
        );
    }

    #[test]
    fn exact_attempt_and_conflict_guards_survive_replay() {
        let pending = attempt();
        assert_eq!(
            pending.observe(&StartObservation::DefinitiveNoThread {
                flow_id: "other-flow".into(),
                attempt_id: "attempt-1".into(),
                evidence_id: "receipt".into()
            }),
            Decision::Rejected(Rejection::OtherFlow)
        );
        assert_eq!(
            pending.observe(&StartObservation::DefinitiveNoThread {
                flow_id: "flow-1".into(),
                attempt_id: "another".into(),
                evidence_id: "receipt".into()
            }),
            Decision::Rejected(Rejection::OtherAttempt)
        );
        let accepted = AttemptState::ThreadAccepted {
            thread_id: "native-1".into(),
            evidence_id: "read-1".into(),
        };
        let reopened = PendingAttempt {
            state: accepted.clone(),
            ..pending
        };
        assert_eq!(
            reopened.observe(&StartObservation::DefinitiveNoThread {
                flow_id: "flow-1".into(),
                attempt_id: "attempt-1".into(),
                evidence_id: "refusal".into()
            }),
            Decision::Rejected(Rejection::ConflictingEvidence)
        );
        assert_eq!(
            reopened.observe(&StartObservation::ThreadAccepted {
                flow_id: "flow-1".into(),
                attempt_id: "attempt-1".into(),
                thread_id: "native-2".into(),
                evidence_id: "read-2".into()
            }),
            Decision::Rejected(Rejection::ConflictingEvidence)
        );
        assert_eq!(
            reopened.observe(&StartObservation::ThreadAccepted {
                flow_id: "flow-1".into(),
                attempt_id: "attempt-1".into(),
                thread_id: "native-1".into(),
                evidence_id: "read-1".into()
            }),
            Decision::Replayed(accepted.clone())
        );
        assert_eq!(
            reopened.observe(&StartObservation::ThreadAccepted {
                flow_id: "flow-1".into(),
                attempt_id: "attempt-1".into(),
                thread_id: "native-1".into(),
                evidence_id: "independent-read-2".into()
            }),
            Decision::Unchanged(accepted)
        );
    }
}
