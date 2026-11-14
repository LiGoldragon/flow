//! Durable one-shot launch pane recording, reconciled from e41621a.
use super::*;
pub(crate) use crate::generated::memory::StoredLaunchPane;
use signal_flow::HerdrPaneBinding;

impl EngineRecord for StoredLaunchPane {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.herdr_pane_binding.launch_request_id.clone().0)
    }
}

/// Records the exact pane created for an intent before the harness is asked
/// to start.  The record is one-shot and is never replaced by a retry.
pub trait RecordsCreatedLaunchPane {
    fn record_created_launch_pane(&self, pane: HerdrPaneBinding) -> Result<bool, StoreError>;
    fn request_harness_start(
        &self,
        launch_request_id: &str,
    ) -> Result<Option<HerdrPaneBinding>, StoreError>;
    fn orphan_launch_pane(&self, launch_request_id: &str) -> Result<bool, StoreError>;
}

/// Reads only a recoverable created pane. An orphan remains durable but is
/// intentionally not returned as a candidate for another external attempt.
pub trait ReadsCreatedLaunchPane {
    fn recoverable_launch_pane(
        &self,
        launch_request_id: &str,
    ) -> Result<Option<HerdrPaneBinding>, StoreError>;
}

impl RecordsCreatedLaunchPane for FlowStore {
    fn record_created_launch_pane(&self, pane: HerdrPaneBinding) -> Result<bool, StoreError> {
        let Some(attempt) = self.stored_launch_attempt(&pane.launch_request_id.0)? else {
            return Ok(false);
        };
        if attempt.launch_attempt.launch_attempt_phase
            != LaunchAttemptPhase::NativeLaunchIntentRecorded
            || self
                .stored_launch_pane(&pane.launch_request_id.0)?
                .is_some()
        {
            return Ok(false);
        }
        self.engine.assert(Assertion::new(
            self.launch_panes,
            StoredLaunchPane {
                herdr_pane_binding: pane,
                harness_start_requested: crate::generated::memory::HarnessStartRequested(false),
                orphaned: crate::generated::memory::Orphaned(false),
            },
        ))?;
        Ok(true)
    }

    fn request_harness_start(
        &self,
        launch_request_id: &str,
    ) -> Result<Option<HerdrPaneBinding>, StoreError> {
        let Some(mut stored) = self.stored_launch_pane(launch_request_id)? else {
            return Ok(None);
        };
        if stored.orphaned.0 || stored.harness_start_requested.0 {
            return Ok(None);
        }
        stored.harness_start_requested = crate::generated::memory::HarnessStartRequested(true);
        let pane = stored.herdr_pane_binding.clone();
        self.mutate_launch_pane(stored)?;
        Ok(Some(pane))
    }

    fn orphan_launch_pane(&self, launch_request_id: &str) -> Result<bool, StoreError> {
        let Some(mut stored) = self.stored_launch_pane(launch_request_id)? else {
            return Ok(false);
        };
        if stored.orphaned.0 {
            return Ok(true);
        }
        stored.orphaned = crate::generated::memory::Orphaned(true);
        self.mutate_launch_pane(stored)?;
        Ok(true)
    }
}

impl ReadsCreatedLaunchPane for FlowStore {
    fn recoverable_launch_pane(
        &self,
        launch_request_id: &str,
    ) -> Result<Option<HerdrPaneBinding>, StoreError> {
        Ok(self
            .stored_launch_pane(launch_request_id)?
            .filter(|stored| stored.harness_start_requested.0 && !stored.orphaned.0)
            .map(|stored| stored.herdr_pane_binding))
    }
}

trait StoresCreatedLaunchPane {
    fn stored_launch_pane(&self, id: &str) -> Result<Option<StoredLaunchPane>, StoreError>;
    fn mutate_launch_pane(&self, pane: StoredLaunchPane) -> Result<(), StoreError>;
}

impl StoresCreatedLaunchPane for FlowStore {
    fn stored_launch_pane(
        &self,
        launch_request_id: &str,
    ) -> Result<Option<StoredLaunchPane>, StoreError> {
        let records = self
            .engine
            .match_records(QueryPlan::key(
                self.launch_panes,
                RecordKey::new(launch_request_id),
            ))?
            .records()
            .to_vec();
        match records.as_slice() {
            [] => Ok(None),
            [pane] => Ok(Some(pane.clone())),
            _ => Err(StoreError::StateInvariant),
        }
    }
    fn mutate_launch_pane(&self, pane: StoredLaunchPane) -> Result<(), StoreError> {
        self.engine.mutate_keyed(KeyedMutation::new(
            self.launch_panes,
            RecordKey::new(pane.herdr_pane_binding.launch_request_id.clone().0),
            pane,
        ))?;
        Ok(())
    }
}
