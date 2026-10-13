//! Durable one-shot launch pane recording, reconciled from e41621a.
use super::*;
use signal_flow::HerdrPaneBinding;

/// The created native pane is distinct from an observed native identity.  It
/// lets a retry inspect the one pane that was already created without
/// creating a second external seat.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(super) struct StoredLaunchPane {
    pane: HerdrPaneBinding,
    harness_start_requested: bool,
    orphaned: bool,
}

impl EngineRecord for StoredLaunchPane {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.pane.launch_request_id.clone())
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
        let Some(attempt) = self.stored_launch_attempt(&pane.launch_request_id)? else {
            return Ok(false);
        };
        if attempt.attempt.launch_attempt_phase != LaunchAttemptPhase::NativeLaunchIntentRecorded
            || self.stored_launch_pane(&pane.launch_request_id)?.is_some()
        {
            return Ok(false);
        }
        self.engine.assert(Assertion::new(
            self.launch_panes,
            StoredLaunchPane {
                pane,
                harness_start_requested: false,
                orphaned: false,
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
        if stored.orphaned || stored.harness_start_requested {
            return Ok(None);
        }
        stored.harness_start_requested = true;
        let pane = stored.pane.clone();
        self.mutate_launch_pane(stored)?;
        Ok(Some(pane))
    }

    fn orphan_launch_pane(&self, launch_request_id: &str) -> Result<bool, StoreError> {
        let Some(mut stored) = self.stored_launch_pane(launch_request_id)? else {
            return Ok(false);
        };
        if stored.orphaned {
            return Ok(true);
        }
        stored.orphaned = true;
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
            .filter(|stored| stored.harness_start_requested && !stored.orphaned)
            .map(|stored| stored.pane))
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
            RecordKey::new(pane.pane.launch_request_id.clone()),
            pane,
        ))?;
        Ok(())
    }
}
