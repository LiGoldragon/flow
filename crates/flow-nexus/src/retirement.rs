//! Idempotent retirement after a replacement has become deliverable.

use signal_flow::{
    ArchiveReceipt, CutoverReceipt, HerdrRoute, OldInactiveProof, OldRouteRemovalProof,
    ProcessIdentity, ProcessLiveness, RefreshAttempt, RefreshAttemptPhase, RefreshCompletion,
    RegistrationAbsence, ReplacementIdempotencyKey, Response,
};

/// Durable state used by retirement reconciliation. Phase changes are atomic
/// compare-and-swap writes of the complete attempt.
pub trait PersistsRetirement {
    fn refresh_attempt(
        &self,
        key: &ReplacementIdempotencyKey,
    ) -> Result<Option<RefreshAttempt>, RetirementError>;
    fn predecessor_route(&self, flow_id: &str) -> Result<HerdrRoute, RetirementError>;
    fn predecessor_process_identity(
        &self,
        flow_id: &str,
    ) -> Result<ProcessIdentity, RetirementError>;
    fn predecessor_native_session_id(&self, flow_id: &str) -> Result<String, RetirementError>;

    /// Records close intent before any external pane operation.
    fn begin_cutover(&self, expected: &RefreshAttempt) -> Result<RefreshAttempt, RetirementError>;
    fn complete_cutover(
        &self,
        expected: &RefreshAttempt,
        receipt: CutoverReceipt,
    ) -> Result<RefreshAttempt, RetirementError>;
}

pub trait ObservesRetirement {
    fn registered_process(
        &self,
        predecessor_flow_id: &str,
        route: &HerdrRoute,
        native_session_id: &str,
    ) -> Result<Option<ProcessIdentity>, RetirementError>;
    fn close_exact(
        &self,
        predecessor_flow_id: &str,
        route: &HerdrRoute,
        native_session_id: &str,
        expected: &ProcessIdentity,
    ) -> Result<(), RetirementError>;
    fn exact_process_is_alive(&self, expected: &ProcessIdentity) -> Result<bool, RetirementError>;
    /// Hashes and indexes the retained original transcript without copying it.
    fn archive_original_transcript(
        &self,
        predecessor_flow_id: &str,
        native_session_id: &str,
    ) -> Result<Option<ArchiveReceipt>, RetirementError>;
}

pub struct PersistedRetirementReconciler<'a, Store, Runtime> {
    store: &'a Store,
    runtime: &'a Runtime,
}

impl<'a, Store, Runtime> PersistedRetirementReconciler<'a, Store, Runtime>
where
    Store: PersistsRetirement,
    Runtime: ObservesRetirement,
{
    pub fn new(store: &'a Store, runtime: &'a Runtime) -> Self {
        Self { store, runtime }
    }

    pub fn reconcile(&self, key: &ReplacementIdempotencyKey) -> Result<Response, RetirementError> {
        let attempt = self
            .store
            .refresh_attempt(key)?
            .ok_or(RetirementError::RefreshAttemptMissing)?;
        let predecessor_id = &attempt.refresh_request.flow_id;
        let replacement_id = attempt
            .flow_id_option
            .as_ref()
            .ok_or(RetirementError::ReplacementNotReady)?;
        let ready = attempt
            .replacement_ready_proof_option
            .as_ref()
            .ok_or(RetirementError::ReplacementNotReady)?;

        if matches!(attempt.refresh_attempt_phase, RefreshAttemptPhase::Complete) {
            let receipt = attempt
                .cutover_receipt_option
                .clone()
                .ok_or(RetirementError::PersistenceInvariant)?;
            return Ok(Response::Refreshed(RefreshCompletion {
                replacement_idempotency_key: key.clone(),
                predecessor_flow_id: predecessor_id.clone(),
                replacement_flow_id: replacement_id.clone(),
                cutover_receipt: receipt,
            }));
        }
        if !matches!(
            attempt.refresh_attempt_phase,
            RefreshAttemptPhase::ReplacementReady | RefreshAttemptPhase::CutoverInProgress
        ) {
            return Ok(Response::RefreshProgress(attempt));
        }

        let route = self.store.predecessor_route(predecessor_id)?;
        let expected_process = self.store.predecessor_process_identity(predecessor_id)?;
        let native_session = self.store.predecessor_native_session_id(predecessor_id)?;
        let registered =
            self.runtime
                .registered_process(predecessor_id, &route, &native_session)?;
        let alive = self.runtime.exact_process_is_alive(&expected_process)?;

        if matches!(
            attempt.refresh_attempt_phase,
            RefreshAttemptPhase::ReplacementReady
        ) {
            match registered.as_ref() {
                Some(observed) if observed != &expected_process => {
                    return Err(RetirementError::IdentityChanged);
                }
                Some(_) if !alive => return Err(RetirementError::IdentityChanged),
                Some(_) => {
                    let persisted = self.store.begin_cutover(&attempt)?;
                    self.runtime.close_exact(
                        predecessor_id,
                        &route,
                        &native_session,
                        &expected_process,
                    )?;
                    return Ok(Response::RefreshProgress(persisted));
                }
                None if alive => return Ok(Response::RefreshProgress(attempt)),
                None => {}
            }
        } else {
            match registered.as_ref() {
                Some(observed) if observed != &expected_process => {
                    return Err(RetirementError::IdentityChanged);
                }
                Some(_) => return Ok(Response::RefreshProgress(attempt)),
                None if alive => return Ok(Response::RefreshProgress(attempt)),
                None => {}
            }
        }

        let Some(archive) = self
            .runtime
            .archive_original_transcript(predecessor_id, &native_session)?
        else {
            return Ok(Response::RefreshProgress(attempt));
        };
        let receipt = CutoverReceipt {
            predecessor_flow_id: predecessor_id.clone(),
            replacement_flow_id: replacement_id.clone(),
            replacement_ready_proof: ready.clone(),
            old_route_removal_proof: OldRouteRemovalProof {
                flow_id: predecessor_id.clone(),
                herdr_route: route,
            },
            old_inactive_proof: OldInactiveProof {
                flow_id: predecessor_id.clone(),
                process_identity: expected_process,
                registration_absence: RegistrationAbsence::Unregistered,
                process_liveness: ProcessLiveness::Dead,
            },
            archive_receipt: archive,
        };
        let completed = self.store.complete_cutover(&attempt, receipt.clone())?;
        if !matches!(
            completed.refresh_attempt_phase,
            RefreshAttemptPhase::Complete
        ) {
            return Err(RetirementError::PersistenceInvariant);
        }
        Ok(Response::Refreshed(RefreshCompletion {
            replacement_idempotency_key: key.clone(),
            predecessor_flow_id: predecessor_id.clone(),
            replacement_flow_id: replacement_id.clone(),
            cutover_receipt: receipt,
        }))
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum RetirementError {
    #[error("refresh attempt is absent")]
    RefreshAttemptMissing,
    #[error("replacement is not ready")]
    ReplacementNotReady,
    #[error("persisted refresh state is inconsistent")]
    PersistenceInvariant,
    #[error("predecessor observation was refused")]
    ObservationRefused,
    #[error("predecessor process identity changed")]
    IdentityChanged,
    #[error("predecessor close was refused")]
    CloseRefused,
    #[error("transcript archive was refused")]
    ArchiveRefused,
}

#[cfg(test)]
mod tests {
    use super::*;
    use signal_flow::{
        FlowAspect, HandoverSelection, HarnessKind, HerdrPaneBinding, InteractiveReadiness,
        LaunchProfile, NativeTargetReceipt, OriginClue, PowerLevel, RefreshPolicy, RefreshRequest,
        ReplacementReadyProof, TranscriptHandoverReference, TranscriptRole,
    };
    use std::cell::{Cell, RefCell};

    struct Store {
        attempt: RefCell<RefreshAttempt>,
        begin_count: Cell<usize>,
        complete_count: Cell<usize>,
    }

    impl PersistsRetirement for Store {
        fn refresh_attempt(
            &self,
            _: &ReplacementIdempotencyKey,
        ) -> Result<Option<RefreshAttempt>, RetirementError> {
            Ok(Some(self.attempt.borrow().clone()))
        }
        fn predecessor_route(&self, _: &str) -> Result<HerdrRoute, RetirementError> {
            Ok(route())
        }
        fn predecessor_process_identity(
            &self,
            _: &str,
        ) -> Result<ProcessIdentity, RetirementError> {
            Ok(process("old"))
        }
        fn predecessor_native_session_id(&self, _: &str) -> Result<String, RetirementError> {
            Ok("old-native".into())
        }
        fn begin_cutover(
            &self,
            expected: &RefreshAttempt,
        ) -> Result<RefreshAttempt, RetirementError> {
            if &*self.attempt.borrow() != expected {
                return Err(RetirementError::PersistenceInvariant);
            }
            self.begin_count.set(self.begin_count.get() + 1);
            let mut next = expected.clone();
            next.refresh_attempt_phase = RefreshAttemptPhase::CutoverInProgress;
            self.attempt.replace(next.clone());
            Ok(next)
        }
        fn complete_cutover(
            &self,
            expected: &RefreshAttempt,
            receipt: CutoverReceipt,
        ) -> Result<RefreshAttempt, RetirementError> {
            if &*self.attempt.borrow() != expected {
                return Err(RetirementError::PersistenceInvariant);
            }
            self.complete_count.set(self.complete_count.get() + 1);
            let mut next = expected.clone();
            next.refresh_attempt_phase = RefreshAttemptPhase::Complete;
            next.cutover_receipt_option = Some(receipt);
            self.attempt.replace(next.clone());
            Ok(next)
        }
    }

    struct Runtime {
        registered: Option<ProcessIdentity>,
        alive: bool,
        archive: Option<ArchiveReceipt>,
        close_count: Cell<usize>,
        archive_count: Cell<usize>,
    }

    impl ObservesRetirement for Runtime {
        fn registered_process(
            &self,
            _: &str,
            _: &HerdrRoute,
            _: &str,
        ) -> Result<Option<ProcessIdentity>, RetirementError> {
            Ok(self.registered.clone())
        }
        fn close_exact(
            &self,
            _: &str,
            _: &HerdrRoute,
            _: &str,
            _: &ProcessIdentity,
        ) -> Result<(), RetirementError> {
            self.close_count.set(self.close_count.get() + 1);
            Ok(())
        }
        fn exact_process_is_alive(&self, _: &ProcessIdentity) -> Result<bool, RetirementError> {
            Ok(self.alive)
        }
        fn archive_original_transcript(
            &self,
            _: &str,
            _: &str,
        ) -> Result<Option<ArchiveReceipt>, RetirementError> {
            self.archive_count.set(self.archive_count.get() + 1);
            Ok(self.archive.clone())
        }
    }

    fn process(token: &str) -> ProcessIdentity {
        ProcessIdentity {
            process_id: 42,
            process_user_id: 1001,
            process_start_token: token.into(),
        }
    }
    fn route() -> HerdrRoute {
        HerdrRoute {
            herdr_session_name: "mind".into(),
            herdr_agent_name: "old".into(),
            herdr_pane_id: "w1:p3".into(),
            herdr_terminal_id: "terminal".into(),
        }
    }
    fn key() -> ReplacementIdempotencyKey {
        ReplacementIdempotencyKey {
            flow_id: "old-flow".into(),
            transcript_record_sha256: "record".into(),
        }
    }
    fn attempt(phase: RefreshAttemptPhase) -> RefreshAttempt {
        RefreshAttempt {
            replacement_idempotency_key: key(),
            refresh_request: RefreshRequest {
                flow_id: "old-flow".into(),
                caller_flow_hint: "old-flow".into(),
                transcript_handover_reference: TranscriptHandoverReference {
                    harness_kind: HarnessKind::Codex,
                    native_session_id: "old-native".into(),
                    native_turn_id: "turn".into(),
                    transcript_item_id: "item".into(),
                    transcript_role: TranscriptRole::Assistant,
                    transcript_title: "Handoff — fixture".into(),
                    transcript_timestamp_seconds: 1_700_000_000,
                    transcript_record_sha256: "record".into(),
                    handover_selection: HandoverSelection::WholeMessage,
                },
                launch_profile: LaunchProfile {
                    launch_request_id: "launch".into(),
                    launch_source_vector: vec![],
                    skill_name_vector: vec![],
                    flow_aspect: FlowAspect::Mind,
                    power_level: PowerLevel::High,
                    harness_kind: HarnessKind::Codex,
                    model_name: "model".into(),
                    effort: "high".into(),
                    flow_id_option: Some("new-flow".into()),
                    remembered_flow_vector: vec![],
                    herdr_session_name: "mind".into(),
                    instruction_prompt: "prompt".into(),
                },
                origin_clue: OriginClue {
                    flow_id: "old-flow".into(),
                    session_id: "old-native".into(),
                    turn_id: "turn".into(),
                },
            },
            refresh_policy: RefreshPolicy {
                maximum_handover_age_seconds: 86_400,
            },
            refresh_attempt_phase: phase,
            flow_id_option: Some("new-flow".into()),
            caller_proof_option: None,
            replacement_ready_proof_option: Some(ReplacementReadyProof {
                herdr_pane_binding: HerdrPaneBinding {
                    launch_request_id: "launch".into(),
                    herdr_session_name: "mind".into(),
                    herdr_agent_name: "new".into(),
                    herdr_workspace_id: "workspace".into(),
                    herdr_pane_id: "w1:p4".into(),
                    herdr_terminal_id: "new-terminal".into(),
                },
                process_identity: process("new"),
                interactive_readiness: InteractiveReadiness::Ready,
                native_target_receipt: NativeTargetReceipt {
                    launch_request_id: "launch".into(),
                    prompt_sha256: "prompt".into(),
                    flow_id: "new-flow".into(),
                    native_session_id: "new-native".into(),
                    native_turn_id: "first".into(),
                    receipt_sha256: "receipt".into(),
                    model_name: "model".into(),
                    effort: "high".into(),
                    native_skill_selection_vector: vec![],
                },
            }),
            cutover_receipt_option: None,
        }
    }
    fn store(phase: RefreshAttemptPhase) -> Store {
        Store {
            attempt: RefCell::new(attempt(phase)),
            begin_count: Cell::new(0),
            complete_count: Cell::new(0),
        }
    }
    fn runtime(
        registered: Option<ProcessIdentity>,
        alive: bool,
        archive: Option<ArchiveReceipt>,
    ) -> Runtime {
        Runtime {
            registered,
            alive,
            archive,
            close_count: Cell::new(0),
            archive_count: Cell::new(0),
        }
    }
    fn archive() -> ArchiveReceipt {
        ArchiveReceipt {
            archive_path: "/transcripts/old.jsonl".into(),
            archive_sha256: "digest".into(),
            archive_index_id: "index".into(),
        }
    }

    #[test]
    fn persists_close_intent_before_exact_close() {
        let store = store(RefreshAttemptPhase::ReplacementReady);
        let runtime = runtime(Some(process("old")), true, None);
        let response = PersistedRetirementReconciler::new(&store, &runtime)
            .reconcile(&key())
            .unwrap();
        assert!(matches!(
            response,
            Response::RefreshProgress(RefreshAttempt {
                refresh_attempt_phase: RefreshAttemptPhase::CutoverInProgress,
                ..
            })
        ));
        assert_eq!((store.begin_count.get(), runtime.close_count.get()), (1, 1));
    }

    #[test]
    fn persisted_close_is_not_reissued_and_changed_identity_is_refused() {
        let store = store(RefreshAttemptPhase::CutoverInProgress);
        let runtime = runtime(None, true, None);
        let _ = PersistedRetirementReconciler::new(&store, &runtime)
            .reconcile(&key())
            .unwrap();
        assert_eq!(runtime.close_count.get(), 0);
        assert_eq!(runtime.archive_count.get(), 0);
        let changed = runtime(Some(process("different")), true, Some(archive()));
        store.attempt.borrow_mut().refresh_attempt_phase = RefreshAttemptPhase::ReplacementReady;
        assert_eq!(
            PersistedRetirementReconciler::new(&store, &changed).reconcile(&key()),
            Err(RetirementError::IdentityChanged)
        );
        assert_eq!(
            (changed.close_count.get(), changed.archive_count.get()),
            (0, 0)
        );
    }

    #[test]
    fn completion_requires_dead_unregistered_process_and_archive() {
        let store = store(RefreshAttemptPhase::CutoverInProgress);
        let runtime = runtime(None, false, Some(archive()));
        let response = PersistedRetirementReconciler::new(&store, &runtime)
            .reconcile(&key())
            .unwrap();
        assert!(matches!(response, Response::Refreshed(_)));
        assert_eq!(
            (
                store.complete_count.get(),
                runtime.close_count.get(),
                runtime.archive_count.get()
            ),
            (1, 0, 1)
        );
    }
}
