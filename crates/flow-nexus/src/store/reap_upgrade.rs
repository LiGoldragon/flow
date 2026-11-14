//! The launch-outcome family as 0.54.6 wrote it (v9), upgraded to v10 with
//! the two obligation families in one atomic evolution. v10 is the format
//! of 0.54.7 to 0.56.x, frozen in `recovery_upgrade`, whose step takes it
//! on to the current v11.
//!
//! v9 knew a refused reap only as `ReplaceRejected(ReapRefused(_))`, which
//! held its confirmed successor out of routing. In v10 that launch reads
//! `ReapPending`, and the reap it still owes is a PendingReap row: its
//! successor is the flow its attempt bound, its predecessor the one its
//! Replacement names, and its standing what the refusal said
//! (`RouteUnavailable` was Herdr unread, `CloseRefused` a refused close,
//! `PersistenceRefused` a write 0.54.6 did not name, so the reap is owed
//! whole). A predecessor still live is recorded Stopped in the same
//! evolution. Every other outcome maps to itself. No PendingHandover is
//! written: a handover owed by derivation is taken up at its receiving
//! flow's first rest.
//!
//! The upgrade is prevalidated before the evolution begins: a refused reap
//! whose launch has no attempt, no binding or no Replacement refuses it,
//! typed, and nothing is written. An older daemon declares v9 alone and is
//! refused at open by the v10 identity; rollback is the restore of the
//! store copy taken before the deploy.

use super::pending_handover::DescribesPendingHandoverTable;
use super::pending_reap::DescribesPendingReapTable;
use super::recovery_upgrade::{
    DescribesLaunchOutcomeTables, LaunchOutcomeV10, ReplaceRejectionV10, StoredLaunchOutcomeV10,
};
use super::{
    FLOW_LAUNCH_OUTCOME_TABLE_NAME, FlowLifecycle, FlowRecord, FlowStore, StoreError,
    StoredLaunchAttempt,
};
use crate::generated::memory::{
    PendingHandover, PendingReap, Predecessor, ReapResponsibility, Replacement, Responsible,
};
use crate::generated::starter::{LaunchedRecord, ReplacedRecord};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use sema_engine::{
    Engine, EngineRecord, MemoryDefinition, MemoryVersion, QueryPlan, RecordKey, SchemaHash,
};
use signal_flow::{ReapStanding, StartRejection, StopRejection};

/// The launch-outcome family's schema label in 0.54.6.
pub(crate) const LAUNCH_OUTCOME_SCHEMA_V9: &str = "flow-nexus-launch-outcome-v9";

/// `CompositionRefusal` as signal-flow 51.0.0 to 63.0.0 archived it: the
/// thirteen variants before the two recovery refusals 64.0.0 appends, whose
/// payload widens every rejection that holds one. The v9 and v10 launch
/// outcomes both hold it.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) enum CompositionRefusalV9 {
    InvalidLaunchRequestId,
    InvalidModelName,
    InvalidEffort,
    InvalidToolSet,
    InvalidHerdrSessionName,
    InvalidSystemModules,
    InvalidPromptModules,
    InvalidRemembering,
    InexactModel,
    Unconfigured,
    PresentationUnavailable,
    Unrecorded,
    HandoverUnreadable(signal_flow::HandoverUnreadable_Data),
}

/// `StartRejection` as 0.54.6 to 0.56.x stored it, holding the
/// CompositionRefusal of its day: the v9 and v10 layout is kept whole,
/// whatever signal-flow appends.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) enum StartRejectionV9 {
    CompositionRefused(CompositionRefusalV9),
    MalformedHarnessFlags(signal_flow::HarnessFlagsRefusal),
    LaunchRequestConflict,
    LaunchPersistenceRefused,
    NativeLaunchRefused(signal_flow::NativeLaunchRefusal),
    BindingRefused(signal_flow::BindingRefusal),
    RegistrationRefused(signal_flow::RegistrationRefusal),
    IntentPersistenceRefused,
    OriginUnavailable,
    AddressAwake,
    ContextRefused(signal_flow::ContextRefusal),
    LeaveOutRefused(signal_flow::LeaveOutRefusal),
    HarnessSpawnRefused(signal_flow::HarnessSpawnRefusal),
    ModelRefused(signal_flow::ModelRefusal),
    Undeclared,
    TopicRefused(signal_flow::Topic),
}

impl From<CompositionRefusalV9> for signal_flow::CompositionRefusal {
    fn from(refusal: CompositionRefusalV9) -> Self {
        use signal_flow::CompositionRefusal as Current;
        match refusal {
            CompositionRefusalV9::InvalidLaunchRequestId => Current::InvalidLaunchRequestId,
            CompositionRefusalV9::InvalidModelName => Current::InvalidModelName,
            CompositionRefusalV9::InvalidEffort => Current::InvalidEffort,
            CompositionRefusalV9::InvalidToolSet => Current::InvalidToolSet,
            CompositionRefusalV9::InvalidHerdrSessionName => Current::InvalidHerdrSessionName,
            CompositionRefusalV9::InvalidSystemModules => Current::InvalidSystemModules,
            CompositionRefusalV9::InvalidPromptModules => Current::InvalidPromptModules,
            CompositionRefusalV9::InvalidRemembering => Current::InvalidRemembering,
            CompositionRefusalV9::InexactModel => Current::InexactModel,
            CompositionRefusalV9::Unconfigured => Current::Unconfigured,
            CompositionRefusalV9::PresentationUnavailable => Current::PresentationUnavailable,
            CompositionRefusalV9::Unrecorded => Current::Unrecorded,
            CompositionRefusalV9::HandoverUnreadable(unreadable) => {
                Current::HandoverUnreadable(unreadable)
            }
        }
    }
}

impl From<StartRejectionV9> for StartRejection {
    fn from(rejection: StartRejectionV9) -> Self {
        match rejection {
            StartRejectionV9::CompositionRefused(refusal) => {
                StartRejection::CompositionRefused(refusal.into())
            }
            StartRejectionV9::MalformedHarnessFlags(refusal) => {
                StartRejection::MalformedHarnessFlags(refusal)
            }
            StartRejectionV9::LaunchRequestConflict => StartRejection::LaunchRequestConflict,
            StartRejectionV9::LaunchPersistenceRefused => StartRejection::LaunchPersistenceRefused,
            StartRejectionV9::NativeLaunchRefused(refusal) => {
                StartRejection::NativeLaunchRefused(refusal)
            }
            StartRejectionV9::BindingRefused(refusal) => StartRejection::BindingRefused(refusal),
            StartRejectionV9::RegistrationRefused(refusal) => {
                StartRejection::RegistrationRefused(refusal)
            }
            StartRejectionV9::IntentPersistenceRefused => StartRejection::IntentPersistenceRefused,
            StartRejectionV9::OriginUnavailable => StartRejection::OriginUnavailable,
            StartRejectionV9::AddressAwake => StartRejection::AddressAwake,
            StartRejectionV9::ContextRefused(refusal) => StartRejection::ContextRefused(refusal),
            StartRejectionV9::LeaveOutRefused(refusal) => StartRejection::LeaveOutRefused(refusal),
            StartRejectionV9::HarnessSpawnRefused(refusal) => {
                StartRejection::HarnessSpawnRefused(refusal)
            }
            StartRejectionV9::ModelRefused(refusal) => StartRejection::ModelRefused(refusal),
            StartRejectionV9::Undeclared => StartRejection::Undeclared,
            StartRejectionV9::TopicRefused(topic) => StartRejection::TopicRefused(topic),
        }
    }
}

/// `ReplaceRejection` as 0.54.6 stored it: six variants, `ReapRefused`
/// among them.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) enum ReplaceRejectionV9 {
    PredecessorAbsent,
    UnknownPredecessor,
    PredecessorStopped,
    LaunchRefused(StartRejectionV9),
    ReapRefused(StopRejection),
    Locked,
}

/// `LaunchOutcome` as 0.54.6 stored it: four variants.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) enum LaunchOutcomeV9 {
    Started(LaunchedRecord),
    Replaced(ReplacedRecord),
    StartRejected(StartRejectionV9),
    ReplaceRejected(ReplaceRejectionV9),
}

/// One stored launch outcome of v9.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct StoredLaunchOutcomeV9 {
    pub(crate) launch_request_id: String,
    pub(crate) launch_outcome: LaunchOutcomeV9,
}

impl EngineRecord for StoredLaunchOutcomeV9 {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.launch_request_id.clone())
    }
}

impl MemoryDefinition for StoredLaunchOutcomeV9 {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(9)
    }

    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label(LAUNCH_OUTCOME_SCHEMA_V9)
    }
}

/// Why a v9 store's upgrade was refused before anything was written.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ReapUpgradeRefusal {
    /// A refused reap's launch has no attempt.
    AttemptMissing(String),
    /// A refused reap's attempt bound no successor.
    BindingMissing(String),
    /// A refused reap's launch names no predecessor.
    ReplacementMissing(String),
    /// A row's key differs from the key its record names.
    KeyMismatch(String),
}

/// The startup step, run before any family is registered.
pub(crate) trait UpgradesReapMemory {
    /// Upgrades a v9 store in one evolution; a current or new store is
    /// left as it is.
    fn upgrade_reap_memory(engine: &mut Engine) -> Result<(), StoreError>;
}

/// The v10 outcome a v9 one becomes, and the PendingReap it owes. A
/// rejection keeps its v9 layout, which v10 holds too.
trait UpgradesLaunchOutcome {
    fn upgraded(
        self,
        replacement: Option<&Replacement>,
        attempt: Option<&StoredLaunchAttempt>,
    ) -> Result<(LaunchOutcomeV10, Option<PendingReap>), ReapUpgradeRefusal>;
}

impl UpgradesLaunchOutcome for StoredLaunchOutcomeV9 {
    fn upgraded(
        self,
        replacement: Option<&Replacement>,
        attempt: Option<&StoredLaunchAttempt>,
    ) -> Result<(LaunchOutcomeV10, Option<PendingReap>), ReapUpgradeRefusal> {
        let id = self.launch_request_id.clone();
        let rejection = match self.launch_outcome {
            LaunchOutcomeV9::Started(record) => {
                return Ok((LaunchOutcomeV10::Started(record), None));
            }
            LaunchOutcomeV9::Replaced(record) => {
                return Ok((LaunchOutcomeV10::Replaced(record), None));
            }
            LaunchOutcomeV9::StartRejected(rejection) => {
                return Ok((LaunchOutcomeV10::StartRejected(rejection), None));
            }
            LaunchOutcomeV9::ReplaceRejected(rejection) => rejection,
        };
        let refused = |rejection| Ok((LaunchOutcomeV10::ReplaceRejected(rejection), None));
        let stop = match rejection {
            ReplaceRejectionV9::PredecessorAbsent => {
                return refused(ReplaceRejectionV10::PredecessorAbsent);
            }
            ReplaceRejectionV9::UnknownPredecessor => {
                return refused(ReplaceRejectionV10::UnknownPredecessor);
            }
            ReplaceRejectionV9::PredecessorStopped => {
                return refused(ReplaceRejectionV10::PredecessorStopped);
            }
            ReplaceRejectionV9::LaunchRefused(start) => {
                return refused(ReplaceRejectionV10::LaunchRefused(start));
            }
            ReplaceRejectionV9::Locked => return refused(ReplaceRejectionV10::Locked),
            ReplaceRejectionV9::ReapRefused(stop) => stop,
        };
        let attempt = attempt.ok_or_else(|| ReapUpgradeRefusal::AttemptMissing(id.clone()))?;
        let binding = attempt
            .launch_attempt
            .native_launch_binding_option
            .as_ref()
            .ok_or_else(|| ReapUpgradeRefusal::BindingMissing(id.clone()))?;
        let replacement =
            replacement.ok_or_else(|| ReapUpgradeRefusal::ReplacementMissing(id.clone()))?;
        let replaced = ReplacedRecord {
            flow_id: signal_flow::FlowId(replacement.predecessor.0.clone()),
            launched_record: LaunchedRecord {
                flow_id: binding.flow_id.clone(),
                session_id: signal_flow::SessionId(binding.native_session_id.0.clone()),
                origin: attempt.launch_attempt.origin.clone(),
            },
        };
        let reap_standing = match stop {
            StopRejection::RouteUnavailable => ReapStanding::HerdrUnavailable,
            StopRejection::CloseRefused => ReapStanding::CloseRefused,
            // 0.54.6 did not record which write failed: the reap is owed whole.
            _ => ReapStanding::Owed,
        };
        Ok((
            LaunchOutcomeV10::ReapPending(replaced),
            Some(PendingReap {
                predecessor: Predecessor(replacement.predecessor.0.clone()),
                launch_request_id: signal_flow::LaunchRequestId(id),
                address: attempt.launch_attempt.launch_profile.address.clone(),
                reap_standing,
                // Owed by the successor until the evolution reads where
                // its Address stands (`owned_as_stands`).
                reap_responsibility: ReapResponsibility::LaunchSuccessor,
            }),
        ))
    }
}

/// Who owes an upgraded reap, by the state of its Address's metaflow.
trait OwnsUpgradedReap {
    /// `LaunchSuccessor` when the metaflow is Awake as the successor;
    /// `TakenOver` by another flow it is Awake as; `AddressHeld` when it is
    /// Asleep or has no row.
    fn owned_as_stands(
        self,
        successor: Option<&signal_flow::FlowId>,
        state: Option<&flow_ethos::State>,
    ) -> PendingReap;
}

impl OwnsUpgradedReap for PendingReap {
    fn owned_as_stands(
        self,
        successor: Option<&signal_flow::FlowId>,
        state: Option<&flow_ethos::State>,
    ) -> PendingReap {
        let reap_responsibility = match state {
            Some(flow_ethos::State::Awake(awake)) if Some(awake) == successor => {
                ReapResponsibility::LaunchSuccessor
            }
            Some(flow_ethos::State::Awake(awake)) => {
                ReapResponsibility::TakenOver(Responsible(awake.clone()))
            }
            Some(flow_ethos::State::Asleep) | None => ReapResponsibility::AddressHeld,
        };
        PendingReap {
            reap_responsibility,
            ..self
        }
    }
}

/// A v9 row upgraded alone, as the identity-only upgrade of an empty family
/// would read one: every outcome but a refused reap maps to itself, and a
/// refused reap, which owes a PendingReap no single row can write, refuses.
impl sema_engine::UpgradeFrom<StoredLaunchOutcomeV9> for StoredLaunchOutcomeV10 {
    type Refusal = ReapUpgradeRefusal;

    fn upgrade_from(before: StoredLaunchOutcomeV9) -> sema_engine::Decision<Self, Self::Refusal> {
        let launch_request_id = before.launch_request_id.clone();
        match before.upgraded(None, None) {
            Ok((launch_outcome, None)) => sema_engine::Decision::Ready(Self {
                launch_request_id: signal_flow::LaunchRequestId(launch_request_id),
                launch_outcome,
            }),
            Ok((_, Some(_))) => sema_engine::Decision::Refused(ReapUpgradeRefusal::AttemptMissing(
                launch_request_id,
            )),
            Err(refusal) => sema_engine::Decision::Refused(refusal),
        }
    }
}

impl UpgradesReapMemory for FlowStore {
    fn upgrade_reap_memory(engine: &mut Engine) -> Result<(), StoreError> {
        let refused = |refusal: ReapUpgradeRefusal| StoreError::ReapUpgrade(format!("{refusal:?}"));
        let stored = engine.list_tables().into_iter().find(|registration| {
            registration.identity().table_name() == FLOW_LAUNCH_OUTCOME_TABLE_NAME.as_str()
        });
        // A store with no launch outcome yet, or one already at v10, is
        // current: the families it lacks are registered as any new one is.
        let Some(stored) = stored else {
            return Ok(());
        };
        if stored.identity().schema_hash() != StoredLaunchOutcomeV9::schema_hash() {
            return Ok(());
        }
        let descriptor = Self::launch_outcome_v10_descriptor();
        let inventory = engine
            .inspect_predecessor::<StoredLaunchOutcomeV9, StoredLaunchOutcomeV10>(&descriptor)?;
        // A v9 family with no row owes nothing: an evolution of no row is
        // an empty commit, so its identity alone is upgraded, and the two
        // obligation families are registered empty with the rest.
        if inventory.rows().is_empty() {
            use sema_engine::MemoryUpgrades as _;
            return match engine
                .upgrade_table::<StoredLaunchOutcomeV9, StoredLaunchOutcomeV10>(descriptor)
            {
                sema_engine::UpgradeOutcome::Upgraded(_)
                | sema_engine::UpgradeOutcome::AlreadyCurrent(_) => Ok(()),
                sema_engine::UpgradeOutcome::Refused(refusal) => {
                    Err(StoreError::ReapUpgrade(format!("{refusal:?}")))
                }
                sema_engine::UpgradeOutcome::Failed(failure) => {
                    Err(StoreError::ReapUpgrade(format!("{failure:?}")))
                }
            };
        }
        // The rows the conversion reads: each family is current already.
        let attempts =
            engine.register_table(sema_engine::TableDescriptor::<StoredLaunchAttempt>::new(
                super::FLOW_LAUNCH_ATTEMPT_TABLE_NAME,
                sema_engine::FamilyName::new("flow-nexus-launch-attempt"),
                StoredLaunchAttempt::schema_hash(),
            ))?;
        let replacements =
            engine.register_table(sema_engine::TableDescriptor::<Replacement>::new(
                super::FLOW_REPLACEMENT_TABLE_NAME,
                sema_engine::FamilyName::new("flow-nexus-replacement"),
                SchemaHash::for_label("flow-nexus-replacement-v1"),
            ))?;
        let flows = engine.register_table(sema_engine::TableDescriptor::<FlowRecord>::new(
            super::FLOW_TABLE_NAME,
            sema_engine::FamilyName::new("flow-nexus-flow"),
            FlowRecord::schema_hash(),
        ))?;
        let metaflows =
            engine.register_table(sema_engine::TableDescriptor::<super::StoredMetaflow>::new(
                super::METAFLOW_TABLE_NAME,
                sema_engine::FamilyName::new("flow-nexus-metaflow"),
                super::StoredMetaflow::schema_hash(),
            ))?;
        let metaflow_rows = engine.match_records(QueryPlan::all(metaflows))?;
        let attempt_rows = engine.match_records(QueryPlan::all(attempts))?;
        let replacement_rows = engine.match_records(QueryPlan::all(replacements))?;
        let flow_rows = engine.match_records(QueryPlan::all(flows))?;
        let attempt_of = |id: &str| {
            attempt_rows
                .records()
                .iter()
                .find(|stored| stored.launch_attempt.launch_request_id.0 == id)
        };
        let replacement_of = |id: &str| {
            replacement_rows
                .records()
                .iter()
                .find(|replacement| replacement.launch_request_id.0 == id)
        };
        // Prevalidated whole before the evolution is begun.
        let mut outcomes = Vec::new();
        let mut reaps = Vec::new();
        for (key, row) in inventory.rows() {
            if *key != row.record_key() {
                return Err(refused(ReapUpgradeRefusal::KeyMismatch(
                    row.launch_request_id.clone(),
                )));
            }
            let id = row.launch_request_id.clone();
            let (outcome, reap) = row
                .clone()
                .upgraded(replacement_of(&id), attempt_of(&id))
                .map_err(refused)?;
            // The successor that 0.54.6 held out of routing may have ended
            // since: the row is owed as its Address stands now, read in
            // this same evolution (the invariant of `reaps_reassigned`).
            let reap = reap.map(|reap| {
                let successor = match &outcome {
                    LaunchOutcomeV10::ReapPending(replaced) => {
                        Some(replaced.launched_record.flow_id.clone())
                    }
                    _ => None,
                };
                let state = metaflow_rows
                    .records()
                    .iter()
                    .find(|stored| stored.metaflow.address == reap.address)
                    .map(|stored| stored.metaflow.state.clone());
                reap.owned_as_stands(successor.as_ref(), state.as_ref())
            });
            outcomes.push(StoredLaunchOutcomeV10 {
                launch_request_id: signal_flow::LaunchRequestId(id),
                launch_outcome: outcome,
            });
            reaps.extend(reap);
        }
        let mut stopped = Vec::new();
        for reap in &reaps {
            if let Some(flow) = flow_rows
                .records()
                .iter()
                .find(|flow| flow.flow_id.0 == reap.predecessor.0)
                && matches!(
                    flow.flow_lifecycle,
                    FlowLifecycle::Pending | FlowLifecycle::Active
                )
            {
                let mut flow = flow.clone();
                flow.flow_lifecycle = FlowLifecycle::Stopped;
                stopped.push(flow);
            }
        }
        let mut evolution = engine
            .begin_atomic_evolution()
            .evolve(inventory, descriptor, outcomes)
            .register(
                <PendingReap as DescribesPendingReapTable>::descriptor(),
                reaps,
            )
            .register(
                <PendingHandover as DescribesPendingHandoverTable>::descriptor(),
                Vec::new(),
            );
        for flow in stopped {
            evolution = evolution.mutate(flows, flow);
        }
        engine.commit_atomic_evolution(evolution)?;
        Ok(())
    }
}
