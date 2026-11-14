//! Model-call context events choose one durable warning stage. A native
//! effective limit or an explicitly provisional policy supplies the
//! denominator. The policy names the native usage projection and its lag.

use crate::generated::memory::{ContextWarningProgress, WarningStageProgress, WarningStageState};
use meta_signal_flow::{
    ContextWarningConfigurationRefusal as PolicyRefusal, ContextWarningPolicy, WarningBoundary,
    WarningStage,
};

pub(crate) struct WarningCrossing {
    pub(crate) selected: WarningStage,
    pub(crate) passed: Vec<WarningStage>,
}

pub(crate) trait JudgesContextWarnings {
    fn validate(&self) -> Result<(), PolicyRefusal>;
    fn crossing(
        &self,
        progress: &ContextWarningProgress,
        context_tokens: i64,
    ) -> Option<WarningCrossing>;
    fn boundary(&self, stage: &WarningStage) -> Option<&WarningBoundary>;
    fn emergency_reached(&self, context_tokens: i64) -> bool;
    fn warning_text(&self, stage: &WarningStage, context_tokens: i64) -> Option<String>;
}

pub(crate) trait TracksContextWarnings {
    fn initial(flow_id: signal_flow::FlowId) -> Self;
    fn unreached(&self, stage: &WarningStage) -> bool;
    fn admitted(
        self,
        crossing: WarningCrossing,
        delivery_id: signal_flow::LogicalDeliveryId,
    ) -> Self;
}

trait OrdersWarningStages {
    fn urgency(&self) -> u8;
}

trait TestsWarningBoundary {
    fn reached(&self, context_tokens: i64, compaction_tokens: i64) -> bool;
}

impl OrdersWarningStages for WarningStage {
    fn urgency(&self) -> u8 {
        match self {
            Self::Emergency => 2,
            Self::Middle => 1,
            Self::Early => 0,
        }
    }
}

impl TestsWarningBoundary for WarningBoundary {
    fn reached(&self, context_tokens: i64, compaction_tokens: i64) -> bool {
        context_tokens >= 0
            && compaction_tokens > 0
            && i128::from(context_tokens) * 100
                >= i128::from(compaction_tokens) * (100 - i128::from(self.remaining_percent.0))
    }
}

impl JudgesContextWarnings for ContextWarningPolicy {
    fn validate(&self) -> Result<(), PolicyRefusal> {
        if self.context_warning_limit.warning_tokens.0 <= 0 {
            return Err(PolicyRefusal::InvalidWarningTokens);
        }
        let mut seen = std::collections::HashSet::new();
        for boundary in &self.warning_boundary_vector {
            if !(1..100).contains(&boundary.remaining_percent.0) {
                return Err(PolicyRefusal::InvalidRemainingPercent);
            }
            if !seen.insert(boundary.warning_stage.clone()) {
                return Err(PolicyRefusal::DuplicateStage);
            }
            if boundary.warning_text.0.trim().is_empty() {
                return Err(PolicyRefusal::WarningTextRefused);
            }
        }
        let Some(early) = self.boundary(&WarningStage::Early) else {
            return Err(PolicyRefusal::MissingStage);
        };
        let Some(middle) = self.boundary(&WarningStage::Middle) else {
            return Err(PolicyRefusal::MissingStage);
        };
        let Some(emergency) = self.boundary(&WarningStage::Emergency) else {
            return Err(PolicyRefusal::MissingStage);
        };
        if !(early.remaining_percent.0 > middle.remaining_percent.0
            && middle.remaining_percent.0 > emergency.remaining_percent.0)
        {
            return Err(PolicyRefusal::InvalidStageOrder);
        }
        Ok(())
    }

    fn crossing(
        &self,
        progress: &ContextWarningProgress,
        context_tokens: i64,
    ) -> Option<WarningCrossing> {
        if self.validate().is_err() {
            return None;
        }
        let mut crossed: Vec<_> = self
            .warning_boundary_vector
            .iter()
            .filter(|boundary| {
                progress.unreached(&boundary.warning_stage)
                    && boundary.reached(context_tokens, self.context_warning_limit.warning_tokens.0)
            })
            .map(|boundary| boundary.warning_stage.clone())
            .collect();
        crossed.sort_by_key(OrdersWarningStages::urgency);
        let selected = crossed.pop()?;
        Some(WarningCrossing {
            selected,
            passed: crossed,
        })
    }

    fn boundary(&self, stage: &WarningStage) -> Option<&WarningBoundary> {
        self.warning_boundary_vector
            .iter()
            .find(|boundary| &boundary.warning_stage == stage)
    }

    fn emergency_reached(&self, context_tokens: i64) -> bool {
        self.validate().is_ok()
            && self
                .boundary(&WarningStage::Emergency)
                .is_some_and(|boundary| {
                    boundary.reached(context_tokens, self.context_warning_limit.warning_tokens.0)
                })
    }

    fn warning_text(&self, stage: &WarningStage, context_tokens: i64) -> Option<String> {
        self.validate().ok()?;
        let boundary = self.boundary(stage)?;
        let percent = i128::from(context_tokens.max(0)) * 100
            / i128::from(self.context_warning_limit.warning_tokens.0);
        Some(
            boundary
                .warning_text
                .0
                .replace("{percent}", &percent.to_string()),
        )
    }
}

impl TracksContextWarnings for ContextWarningProgress {
    fn initial(flow_id: signal_flow::FlowId) -> Self {
        Self {
            flow_id,
            warning_stage_progress_vector: [
                WarningStage::Emergency,
                WarningStage::Middle,
                WarningStage::Early,
            ]
            .into_iter()
            .map(|warning_stage| WarningStageProgress {
                warning_stage,
                warning_stage_state: WarningStageState::Unreached,
            })
            .collect(),
        }
    }

    fn unreached(&self, stage: &WarningStage) -> bool {
        self.warning_stage_progress_vector.iter().any(|progress| {
            &progress.warning_stage == stage
                && progress.warning_stage_state == WarningStageState::Unreached
        })
    }

    fn admitted(
        mut self,
        crossing: WarningCrossing,
        delivery_id: signal_flow::LogicalDeliveryId,
    ) -> Self {
        for progress in &mut self.warning_stage_progress_vector {
            if progress.warning_stage == crossing.selected {
                progress.warning_stage_state = WarningStageState::Admitted(delivery_id.clone());
            } else if crossing.passed.contains(&progress.warning_stage)
                && progress.warning_stage_state == WarningStageState::Unreached
            {
                progress.warning_stage_state = WarningStageState::Passed;
            }
        }
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use meta_signal_flow::{
        BoundaryIdentity, ContextBoundaryEvidence, ContextUsageProjection, ContextWarningLimit,
        CoverageGap, HarnessVersion, LimitBasis, RemainingPercent, WarningPriority, WarningText,
        WarningTokens,
    };

    fn fixture_policy() -> ContextWarningPolicy {
        ContextWarningPolicy {
            model_name: meta_signal_flow::ModelName("fixture-model".into()),
            context_warning_limit: ContextWarningLimit {
                limit_basis: LimitBasis::NativeEffectiveLimit,
                warning_tokens: WarningTokens(200_000),
                context_boundary_evidence: ContextBoundaryEvidence {
                    harness_kind: meta_signal_flow::HarnessKind::Codex,
                    harness_version: HarnessVersion("fixture-not-native-proof".into()),
                    transcript_path: signal_flow::TranscriptPath("/tmp/fixture.jsonl".into()),
                    boundary_identity: BoundaryIdentity("fixture-boundary".into()),
                    coverage_gap: CoverageGap("fixture-not-native-proof".into()),
                },
            },
            context_usage_projection: ContextUsageProjection::CodexLastTotal,
            warning_boundary_vector: vec![
                WarningBoundary {
                    warning_stage: WarningStage::Early,
                    remaining_percent: RemainingPercent(50),
                    warning_priority: WarningPriority::Soft,
                    warning_text: WarningText(
                        "you are at {percent} percent of your context".into(),
                    ),
                },
                WarningBoundary {
                    warning_stage: WarningStage::Middle,
                    remaining_percent: RemainingPercent(25),
                    warning_priority: WarningPriority::MiddleAbrupt,
                    warning_text: WarningText("write your handover now".into()),
                },
                WarningBoundary {
                    warning_stage: WarningStage::Emergency,
                    remaining_percent: RemainingPercent(10),
                    warning_priority: WarningPriority::HardAbrupt,
                    warning_text: WarningText(
                        "your handover right now, or you lose everything when you are refreshed."
                            .into(),
                    ),
                },
            ],
        }
    }

    #[test]
    fn one_event_crossing_every_stage_selects_only_emergency() {
        let policy = fixture_policy();
        let progress = ContextWarningProgress::initial(signal_flow::FlowId("old-flow".into()));
        let crossing = policy.crossing(&progress, 180_000).unwrap();
        assert_eq!(crossing.selected, WarningStage::Emergency);
        assert_eq!(
            crossing.passed,
            vec![WarningStage::Early, WarningStage::Middle]
        );
        let next = progress.admitted(crossing, signal_flow::LogicalDeliveryId("notice-1".into()));
        assert!(policy.crossing(&next, 190_000).is_none());
        assert!(
            next.warning_stage_progress_vector
                .iter()
                .filter(|row| row.warning_stage != WarningStage::Emergency)
                .all(|row| row.warning_stage_state == WarningStageState::Passed)
        );
    }

    #[test]
    fn stages_cross_once_at_their_exact_boundaries() {
        let policy = fixture_policy();
        let mut progress = ContextWarningProgress::initial(signal_flow::FlowId("flow".into()));
        assert!(policy.crossing(&progress, 99_999).is_none());
        struct StageCase {
            tokens: i64,
            stage: WarningStage,
        }
        for case in [
            StageCase {
                tokens: 100_000,
                stage: WarningStage::Early,
            },
            StageCase {
                tokens: 150_000,
                stage: WarningStage::Middle,
            },
            StageCase {
                tokens: 180_000,
                stage: WarningStage::Emergency,
            },
        ] {
            let StageCase { tokens, stage } = case;
            let crossing = policy.crossing(&progress, tokens).unwrap();
            assert_eq!(crossing.selected, stage);
            progress = progress.admitted(
                crossing,
                signal_flow::LogicalDeliveryId(format!("notice-{tokens}")),
            );
            assert!(policy.crossing(&progress, tokens).is_none());
        }
    }

    #[test]
    fn capacity_and_refresh_threshold_do_not_enter_native_denominator_math() {
        let mut policy = fixture_policy();
        policy.context_warning_limit.warning_tokens = WarningTokens(400_000);
        let progress = ContextWarningProgress::initial(signal_flow::FlowId("flow".into()));
        assert!(policy.crossing(&progress, 199_999).is_none());
        assert_eq!(
            policy.crossing(&progress, 200_000).unwrap().selected,
            WarningStage::Early
        );
        assert!(!policy.emergency_reached(359_999));
        assert!(policy.emergency_reached(360_000));
    }

    #[test]
    fn rendering_uses_the_measured_used_percent() {
        let policy = fixture_policy();
        assert_eq!(
            policy.warning_text(&WarningStage::Early, 108_000).unwrap(),
            "you are at 54 percent of your context"
        );
    }

    #[test]
    fn provisional_policy_uses_its_chosen_denominator_without_becoming_native() {
        let mut policy = fixture_policy();
        let evidence = policy
            .context_warning_limit
            .context_boundary_evidence
            .clone();
        policy.context_warning_limit = ContextWarningLimit {
            limit_basis: LimitBasis::ProvisionalPolicy,
            warning_tokens: WarningTokens(400_000),
            context_boundary_evidence: evidence,
        };
        let progress = ContextWarningProgress::initial(signal_flow::FlowId("flow".into()));
        assert!(policy.crossing(&progress, 199_999).is_none());
        assert_eq!(
            policy.crossing(&progress, 200_000).unwrap().selected,
            WarningStage::Early
        );
        assert_eq!(
            policy.context_warning_limit.limit_basis,
            LimitBasis::ProvisionalPolicy
        );
    }

    #[test]
    fn invalid_denominator_share_text_and_duplicate_stage_are_refused() {
        let mut policy = fixture_policy();
        policy.context_warning_limit.warning_tokens = WarningTokens(0);
        assert_eq!(policy.validate(), Err(PolicyRefusal::InvalidWarningTokens));
        policy.context_warning_limit.warning_tokens = WarningTokens(200_000);
        policy.warning_boundary_vector[0].remaining_percent = RemainingPercent(100);
        assert_eq!(
            policy.validate(),
            Err(PolicyRefusal::InvalidRemainingPercent)
        );
        policy.warning_boundary_vector[0].remaining_percent = RemainingPercent(50);
        policy.warning_boundary_vector[0].warning_text = WarningText(" ".into());
        assert_eq!(policy.validate(), Err(PolicyRefusal::WarningTextRefused));
        policy.warning_boundary_vector[0].warning_text = WarningText("context".into());
        policy
            .warning_boundary_vector
            .push(policy.warning_boundary_vector[0].clone());
        assert_eq!(policy.validate(), Err(PolicyRefusal::DuplicateStage));
    }

    #[test]
    fn negative_observation_does_not_cross_and_large_values_preserve_exact_math() {
        let mut policy = fixture_policy();
        let progress = ContextWarningProgress::initial(signal_flow::FlowId("flow".into()));
        assert!(policy.crossing(&progress, -1).is_none());
        assert!(!policy.emergency_reached(-1));
        policy.context_warning_limit.warning_tokens = WarningTokens(i64::MAX);
        assert!(policy.crossing(&progress, i64::MAX / 2).is_none());
        assert_eq!(
            policy
                .crossing(&progress, i64::MAX / 2 + 1)
                .unwrap()
                .selected,
            WarningStage::Early
        );
        assert_eq!(
            policy.crossing(&progress, i64::MAX).unwrap().selected,
            WarningStage::Emergency
        );
    }

    #[test]
    fn invalid_policy_is_refused_before_any_crossing() {
        let mut policy = fixture_policy();
        policy.warning_boundary_vector[1].remaining_percent = RemainingPercent(60);
        assert_eq!(policy.validate(), Err(PolicyRefusal::InvalidStageOrder));
        let progress = ContextWarningProgress::initial(signal_flow::FlowId("flow".into()));
        assert!(policy.crossing(&progress, 200_000).is_none());
        policy.warning_boundary_vector[1].remaining_percent = RemainingPercent(25);
        policy.warning_boundary_vector.pop();
        assert_eq!(policy.validate(), Err(PolicyRefusal::MissingStage));
    }
}
