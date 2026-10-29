//! The singleton Timings and Limits configuration families. Their first
//! committed values are authored as Datom under `seed/`; after that every
//! value comes from the store's committed configuration snapshot.

use super::StoreError;
use datom_codec::{Actualizing, Budget, Potential};
use meta_signal_flow::{Limits, Timings};
use protos::ReaderBudget;
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use sema_engine::{
    Assertion, Decision, Engine, EngineRecord, FamilyName, MemoryDefinition, MemoryUpgrades,
    MemoryVersion, QueryPlan, RecordKey, SchemaHash, TableDescriptor, TableName, TableReference,
    UpgradeFrom, UpgradeOutcome,
};

pub(super) const TIMINGS_TABLE_NAME: TableName =
    TableName::new("flow_nexus_meta_timings_configuration");
pub(super) const LIMITS_TABLE_NAME: TableName =
    TableName::new("flow_nexus_meta_limits_configuration");

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct StoredTimings {
    pub(crate) timings: Timings,
}

impl EngineRecord for StoredTimings {
    fn record_key(&self) -> RecordKey {
        RecordKey::new("timings")
    }
}
impl MemoryDefinition for StoredTimings {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(2)
    }
    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label("flow-nexus-meta-timings-configuration-v2")
    }
}

/// Timings archived before native stop had its own configured duration.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct StoredTimingsV1 {
    pub(crate) timings: TimingsV1,
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct TimingsV1 {
    pub socket_timeout: i64,
    pub curriculum_timeout: i64,
    pub app_server_timeout: i64,
    pub send_wait: i64,
    pub prompt_reply_timeout: i64,
    pub launch_rest_wait: i64,
    pub pane_rest_wait: i64,
    pub agent_start_timeout: i64,
    pub environment_ready_timeout: i64,
    pub prompt_observation_timeout: i64,
    pub interrupt_wait: i64,
    pub submission_spacing: i64,
    pub title_readback_interval: i64,
    pub agent_observe_poll: i64,
    pub send_tool_first_wait: i64,
    pub send_tool_longest_wait: i64,
    pub send_tool_serving_slice: i64,
    pub send_tool_replay_window: i64,
    pub compaction_watch_margin: i64,
}

impl EngineRecord for StoredTimingsV1 {
    fn record_key(&self) -> RecordKey {
        RecordKey::new("timings")
    }
}

impl MemoryDefinition for StoredTimingsV1 {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(1)
    }
    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label("flow-nexus-meta-timings-configuration-v1")
    }
}

impl UpgradeFrom<StoredTimingsV1> for StoredTimings {
    type Refusal = std::convert::Infallible;

    fn upgrade_from(previous: StoredTimingsV1) -> Decision<Self, Self::Refusal> {
        let old = previous.timings;
        Decision::Ready(Self {
            timings: Timings {
                socket_timeout: old.socket_timeout,
                curriculum_timeout: old.curriculum_timeout,
                app_server_timeout: old.app_server_timeout,
                send_wait: old.send_wait,
                prompt_reply_timeout: old.prompt_reply_timeout,
                launch_rest_wait: old.launch_rest_wait,
                stop_wait: old.launch_rest_wait,
                pane_rest_wait: old.pane_rest_wait,
                agent_start_timeout: old.agent_start_timeout,
                environment_ready_timeout: old.environment_ready_timeout,
                prompt_observation_timeout: old.prompt_observation_timeout,
                interrupt_wait: old.interrupt_wait,
                submission_spacing: old.submission_spacing,
                title_readback_interval: old.title_readback_interval,
                agent_observe_poll: old.agent_observe_poll,
                send_tool_first_wait: old.send_tool_first_wait,
                send_tool_longest_wait: old.send_tool_longest_wait,
                send_tool_serving_slice: old.send_tool_serving_slice,
                send_tool_replay_window: old.send_tool_replay_window,
                compaction_watch_margin: old.compaction_watch_margin,
            },
        })
    }
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct StoredLimits {
    pub(crate) limits: Limits,
}
impl EngineRecord for StoredLimits {
    fn record_key(&self) -> RecordKey {
        RecordKey::new("limits")
    }
}
impl MemoryDefinition for StoredLimits {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(1)
    }
    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label("flow-nexus-meta-limits-configuration-v1")
    }
}

pub(crate) trait RegistersTimingAndLimitTables {
    fn upgraded_timings(
        &mut self,
        descriptor: TableDescriptor<StoredTimings>,
    ) -> Result<TableReference<StoredTimings>, StoreError>;
    fn register_limits(&mut self) -> Result<TableReference<StoredLimits>, StoreError>;
}

pub(crate) trait ProvidesTimingSeed {
    fn budget() -> Budget;
    fn seed_timings() -> Result<Timings, StoreError>;
    fn timings_descriptor() -> TableDescriptor<StoredTimings>;
}

impl ProvidesTimingSeed for StoredTimings {
    fn budget() -> Budget {
        Budget {
            remaining: 65_536,
            reader: ReaderBudget { remaining: 65_536 },
            depth: 0,
            maximum_depth: 1_024,
        }
    }

    fn seed_timings() -> Result<Timings, StoreError> {
        Potential::<Timings>::from(include_str!("../../seed/timings.datom"))
            .actualize(&mut Self::budget())
            .map_err(|error| {
                StoreError::ConfigurationSeed(format!("invalid timings seed: {error:?}"))
            })
    }

    fn timings_descriptor() -> TableDescriptor<StoredTimings> {
        TableDescriptor::new(
            TIMINGS_TABLE_NAME,
            FamilyName::new("flow-nexus-meta-timings-configuration"),
            StoredTimings::schema_hash(),
        )
        .with_predecessor::<StoredTimingsV1>()
    }
}

pub(crate) trait ProvidesLimitSeed {
    fn seed_limits() -> Result<Limits, StoreError>;
    fn limits_descriptor() -> TableDescriptor<StoredLimits>;
}

impl ProvidesLimitSeed for StoredLimits {
    fn seed_limits() -> Result<Limits, StoreError> {
        Potential::<Limits>::from(include_str!("../../seed/limits.datom"))
            .actualize(&mut StoredTimings::budget())
            .map_err(|error| {
                StoreError::ConfigurationSeed(format!("invalid limits seed: {error:?}"))
            })
    }

    fn limits_descriptor() -> TableDescriptor<StoredLimits> {
        TableDescriptor::new(
            LIMITS_TABLE_NAME,
            FamilyName::new("flow-nexus-meta-limits-configuration"),
            StoredLimits::schema_hash(),
        )
    }
}

pub(crate) trait SupportedNativeTimings {
    fn supported(&self) -> bool;
}

impl SupportedNativeTimings for Timings {
    fn supported(&self) -> bool {
        let supported =
            |milliseconds: i64| milliseconds >= 0 && i32::try_from(milliseconds).is_ok();
        supported(self.launch_rest_wait) && supported(self.stop_wait)
    }
}

impl RegistersTimingAndLimitTables for Engine {
    fn upgraded_timings(
        &mut self,
        descriptor: TableDescriptor<StoredTimings>,
    ) -> Result<TableReference<StoredTimings>, StoreError> {
        let table = match self.upgrade_table::<StoredTimingsV1, StoredTimings>(descriptor) {
            UpgradeOutcome::Upgraded(receipt) => receipt.table(),
            UpgradeOutcome::AlreadyCurrent(table) => table,
            UpgradeOutcome::Refused(problem) => match *problem.reason() {},
            UpgradeOutcome::Failed(problem) => {
                return Err(StoreError::StagedUpgrade(format!(
                    "timings family upgrade failed: {problem:?}"
                )));
            }
        };
        if self
            .match_records(QueryPlan::all(table))?
            .records()
            .is_empty()
        {
            let timings = StoredTimings::seed_timings()?;
            self.assert(Assertion::new(table, StoredTimings { timings }))?;
        }
        Ok(table)
    }

    fn register_limits(&mut self) -> Result<TableReference<StoredLimits>, StoreError> {
        let table = self.register_table(TableDescriptor::new(
            LIMITS_TABLE_NAME,
            FamilyName::new("flow-nexus-meta-limits-configuration"),
            StoredLimits::schema_hash(),
        ))?;
        if self
            .match_records(QueryPlan::all(table))?
            .records()
            .is_empty()
        {
            let limits = StoredLimits::seed_limits()?;
            self.assert(Assertion::new(table, StoredLimits { limits }))?;
        }
        Ok(table)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn old_timings(launch_rest_wait: i64) -> TimingsV1 {
        TimingsV1 {
            socket_timeout: 1,
            curriculum_timeout: 2,
            app_server_timeout: 3,
            send_wait: 4,
            prompt_reply_timeout: 5,
            launch_rest_wait,
            pane_rest_wait: 7,
            agent_start_timeout: 8,
            environment_ready_timeout: 9,
            prompt_observation_timeout: 10,
            interrupt_wait: 11,
            submission_spacing: 12,
            title_readback_interval: 13,
            agent_observe_poll: 14,
            send_tool_first_wait: 15,
            send_tool_longest_wait: 16,
            send_tool_serving_slice: 17,
            send_tool_replay_window: 18,
            compaction_watch_margin: 19,
        }
    }

    #[test]
    fn archived_timings_gain_their_previous_launch_wait_as_stop_wait() {
        let Decision::Ready(upgraded) = StoredTimings::upgrade_from(StoredTimingsV1 {
            timings: old_timings(42),
        });
        assert_eq!(upgraded.timings.launch_rest_wait, 42);
        assert_eq!(upgraded.timings.stop_wait, 42);
    }

    #[test]
    fn native_lifecycle_timings_reject_negative_and_poll_overflow_values() {
        let mut timings = StoredTimings::seed_timings().unwrap();
        assert!(timings.supported());
        timings.stop_wait = -1;
        assert!(!timings.supported());
        timings.stop_wait = i64::from(i32::MAX) + 1;
        assert!(!timings.supported());
        timings.stop_wait = 1;
        timings.launch_rest_wait = i64::from(i32::MAX) + 1;
        assert!(!timings.supported());
    }
}
