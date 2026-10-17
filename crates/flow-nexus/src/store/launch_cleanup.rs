//! The cleanup of a Codex launch an earlier Nexus left between opening its
//! thread and acknowledging its seat. Its outcome is settled once, refused;
//! its cleanup is a separate record with a step for the thread (replayed
//! calls answered, the thread accounted for and archived) and a step for
//! what it left in Herdr (its pane and any flow it registered taken down).
//! Each step is Done only once it succeeded, so a start that finds a step
//! Pending resumes it, whatever the launch's settlement. The row is written
//! before the launch is settled, keyed by the launch request, in its own
//! table, so a store written before it opens with the table added and holds
//! none.

use super::*;

pub(super) const LAUNCH_CLEANUP_TABLE_NAME: TableName =
    TableName::new("flow_nexus_launch_cleanups");
pub(super) const LAUNCH_CLEANUP_FAMILY: &str = "flow-nexus-launch-cleanup";
pub(super) const LAUNCH_CLEANUP_SCHEMA: &str = "flow-nexus-launch-cleanup-v1";

/// Where one step of a launch's cleanup stands.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, Copy, PartialEq, Eq)]
pub enum CleanupStep {
    /// Not yet succeeded: the next start takes it again.
    Pending,
    /// Succeeded; never taken again.
    Done,
}

/// The parts of a launch's cleanup.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CleanupPart {
    /// The thread: its replayed calls answered, it accounted for and
    /// archived.
    Thread,
    /// What it left in Herdr: its pane closed or gone, any flow it
    /// registered recorded Stopped.
    Pane,
}

/// The cleanup of one interrupted launch.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub struct LaunchCleanup {
    pub launch_request_id: String,
    /// The thread the launch opened.
    pub native_session_id: String,
    pub thread_step: CleanupStep,
    pub pane_step: CleanupStep,
}

impl EngineRecord for LaunchCleanup {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.launch_request_id.clone())
    }
}

/// Whether a cleanup is finished.
pub trait CompletesCleanup {
    fn is_complete(&self) -> bool;
    fn step(&self, part: CleanupPart) -> CleanupStep;
}

impl CompletesCleanup for LaunchCleanup {
    fn is_complete(&self) -> bool {
        self.thread_step == CleanupStep::Done && self.pane_step == CleanupStep::Done
    }

    fn step(&self, part: CleanupPart) -> CleanupStep {
        match part {
            CleanupPart::Thread => self.thread_step,
            CleanupPart::Pane => self.pane_step,
        }
    }
}

pub(super) trait DescribesLaunchCleanupTable {
    fn descriptor() -> TableDescriptor<LaunchCleanup>;
}

impl DescribesLaunchCleanupTable for LaunchCleanup {
    fn descriptor() -> TableDescriptor<LaunchCleanup> {
        TableDescriptor::new(
            LAUNCH_CLEANUP_TABLE_NAME,
            FamilyName::new(LAUNCH_CLEANUP_FAMILY),
            SchemaHash::for_label(LAUNCH_CLEANUP_SCHEMA),
        )
    }
}

pub(crate) trait KeepsLaunchCleanups {
    /// The cleanup of a launch: the one held, else a new one with every
    /// step Pending, written before it is returned.
    fn begin_launch_cleanup(
        &self,
        launch_request_id: &str,
        native_session_id: &str,
    ) -> Result<LaunchCleanup, StoreError>;
    /// The cleanup of a launch, when one was begun.
    fn launch_cleanup(&self, launch_request_id: &str) -> Result<Option<LaunchCleanup>, StoreError>;
    /// Records one step of a begun cleanup Done; the cleanup as it stands.
    fn finish_cleanup_step(
        &self,
        launch_request_id: &str,
        part: CleanupPart,
    ) -> Result<Option<LaunchCleanup>, StoreError>;
}

impl KeepsLaunchCleanups for FlowStore {
    fn begin_launch_cleanup(
        &self,
        launch_request_id: &str,
        native_session_id: &str,
    ) -> Result<LaunchCleanup, StoreError> {
        if let Some(held) = self.launch_cleanup(launch_request_id)? {
            return Ok(held);
        }
        let cleanup = LaunchCleanup {
            launch_request_id: launch_request_id.to_owned(),
            native_session_id: native_session_id.to_owned(),
            thread_step: CleanupStep::Pending,
            pane_step: CleanupStep::Pending,
        };
        self.engine
            .assert(Assertion::new(self.launch_cleanups, cleanup.clone()))?;
        Ok(cleanup)
    }

    fn launch_cleanup(&self, launch_request_id: &str) -> Result<Option<LaunchCleanup>, StoreError> {
        let records = self
            .engine
            .match_records(QueryPlan::key(
                self.launch_cleanups,
                RecordKey::new(launch_request_id),
            ))?
            .records()
            .to_vec();
        match records.as_slice() {
            [] => Ok(None),
            [cleanup] => Ok(Some(cleanup.clone())),
            _ => Err(StoreError::StateInvariant),
        }
    }

    fn finish_cleanup_step(
        &self,
        launch_request_id: &str,
        part: CleanupPart,
    ) -> Result<Option<LaunchCleanup>, StoreError> {
        let Some(mut cleanup) = self.launch_cleanup(launch_request_id)? else {
            return Ok(None);
        };
        if cleanup.step(part) == CleanupStep::Done {
            return Ok(Some(cleanup));
        }
        match part {
            CleanupPart::Thread => cleanup.thread_step = CleanupStep::Done,
            CleanupPart::Pane => cleanup.pane_step = CleanupStep::Done,
        }
        self.engine.mutate_keyed(KeyedMutation::new(
            self.launch_cleanups,
            RecordKey::new(launch_request_id),
            cleanup.clone(),
        ))?;
        Ok(Some(cleanup))
    }
}
