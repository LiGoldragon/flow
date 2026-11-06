//! The 0.52.0 upgrade of every Address-bearing family: an Address names
//! its Model where it named its Layer.
//!
//! An archived Layer and an archived Model are one byte in the same place,
//! so a row archived before 0.52.0 reads as the current row, every Address
//! in it naming the Claude model of its Layer's rank. Each family's
//! current definition takes its 0.51 definition as a predecessor of the
//! same bytes; once a family held under any earlier definition is
//! upgraded, every row is remodelled, each Address in it read as the Model
//! of that rank in its aspect's family, and moved from the key Flow kept it
//! under to its own, in one commit. The open is staged, so the store never
//! holds the upgrade without the remodelling.

use super::StoreError;
use crate::model_family::{NamesModelOfLayer, RanksInFamily};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use sema_engine::{
    Engine, EngineRecord, MemoryDefinition, MemoryVersion, QueryPlan, RecordKey, SchemaHash,
    TableDescriptor, TableReference,
};
use signal_flow::{Address, Aspect, Model};

/// A Layer as Flow archived it before 0.52.0.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) enum LayerBeforeModel {
    Primary,
    Secondary,
    Tertiary,
    Quaternary,
}

/// What a Layer named.
pub(crate) trait NamesLayer {
    /// The Layer's word, as Flow's keys wrote it.
    fn word(&self) -> &'static str;
    /// The Model of the Layer's rank in the aspect's family: Claude in
    /// Psyche, OpenAI in Mind and Field.
    fn model_in(&self, aspect: &Aspect) -> Model;
}

impl NamesLayer for LayerBeforeModel {
    fn word(&self) -> &'static str {
        match self {
            LayerBeforeModel::Primary => "Primary",
            LayerBeforeModel::Secondary => "Secondary",
            LayerBeforeModel::Tertiary => "Tertiary",
            LayerBeforeModel::Quaternary => "Quaternary",
        }
    }

    fn model_in(&self, aspect: &Aspect) -> Model {
        match self {
            LayerBeforeModel::Primary => Model::Fable,
            LayerBeforeModel::Secondary => Model::Opus,
            LayerBeforeModel::Tertiary => Model::Sonnet,
            LayerBeforeModel::Quaternary => Model::Haiku,
        }
        .of_rank_in(aspect)
    }
}

/// A row of an Address-bearing family as Flow archived it before 0.52.0:
/// the same bytes as the current row.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone)]
pub(crate) struct BeforeModel<Row> {
    pub(crate) row: Row,
}

/// A 0.51 row is kept under the key Flow kept it under then.
impl<Row: KeptBeforeModel> EngineRecord for BeforeModel<Row> {
    fn record_key(&self) -> RecordKey {
        self.row.key_before_model()
    }
}

/// The definition a family's rows carried before 0.52.0.
pub(crate) trait DefinedBeforeModel {
    fn version_before_model() -> MemoryVersion;
    fn schema_hash_before_model() -> SchemaHash;
}

impl<Row: DefinedBeforeModel> MemoryDefinition for BeforeModel<Row> {
    fn memory_version() -> MemoryVersion {
        Row::version_before_model()
    }

    fn schema_hash() -> SchemaHash {
        Row::schema_hash_before_model()
    }
}

/// A family's 0.51 row is carried as it is; its Addresses are remodelled
/// with the family's other rows once the upgrade lands.
#[macro_export]
macro_rules! upgrades_before_model {
    ($row:ty) => {
        impl sema_engine::UpgradeFrom<$crate::store::model_address::BeforeModel<$row>> for $row {
            type Refusal = std::convert::Infallible;

            fn upgrade_from(
                previous: $crate::store::model_address::BeforeModel<$row>,
            ) -> sema_engine::Decision<Self, Self::Refusal> {
                sema_engine::Decision::Ready(previous.row)
            }
        }
    };
}

/// A value whose Addresses were archived before 0.52.0, read again as
/// Models: each Address becomes the one its Layer named.
pub(crate) trait RemodelsAddresses {
    fn remodel(&mut self);
}

impl RemodelsAddresses for Address {
    fn remodel(&mut self) {
        *self = self.of_layer();
    }
}

impl<Part: RemodelsAddresses> RemodelsAddresses for Option<Part> {
    fn remodel(&mut self) {
        if let Some(part) = self {
            part.remodel();
        }
    }
}

impl<Part: RemodelsAddresses> RemodelsAddresses for Vec<Part> {
    fn remodel(&mut self) {
        for part in self {
            part.remodel();
        }
    }
}

impl<Part: RemodelsAddresses> RemodelsAddresses for Box<Part> {
    fn remodel(&mut self) {
        self.as_mut().remodel();
    }
}

/// The key a row was kept under before 0.52.0. A row whose key names no
/// Address keeps its key.
pub(crate) trait KeptBeforeModel: EngineRecord {
    fn key_before_model(&self) -> RecordKey {
        self.record_key()
    }
}

/// The remodelling of one family across the 0.52.0 upgrade.
pub(crate) trait RemodelsFamilies {
    /// Whether the store holds the family under a definition other than
    /// the current one, so its rows were archived before 0.52.0.
    fn held_before_model<Row>(&self, descriptor: &TableDescriptor<Row>) -> bool;

    /// Open the family across the 0.52.0 edge when the store holds its 0.51
    /// definition, else as `older` opens it; then remodel every row when
    /// the store held it under any earlier definition.
    fn upgraded_before_model<Row, Older>(
        &mut self,
        descriptor: TableDescriptor<Row>,
        older: Older,
    ) -> Result<TableReference<Row>, StoreError>
    where
        Older: FnOnce(&mut Engine, TableDescriptor<Row>) -> Result<TableReference<Row>, StoreError>,
        Row: RemodelsAddresses
            + KeptBeforeModel
            + DefinedBeforeModel
            + sema_engine::UpgradeFrom<BeforeModel<Row>, Refusal = std::convert::Infallible>
            + sema_engine::EngineStoredRecord
            + Clone
            + Send
            + Sync
            + 'static,
        BeforeModel<Row>: sema_engine::EngineStoredValue + Send + Sync + 'static,
        <BeforeModel<Row> as rkyv::Archive>::Archived: rkyv::Deserialize<
                BeforeModel<Row>,
                rkyv::api::high::HighDeserializer<rkyv::rancor::Error>,
            > + for<'validation> rkyv::bytecheck::CheckBytes<
                rkyv::rancor::Strategy<
                    rkyv::validation::Validator<
                        rkyv::validation::archive::ArchiveValidator<'validation>,
                        rkyv::validation::shared::SharedValidator,
                    >,
                    rkyv::rancor::Error,
                >,
            >,
        <Row as rkyv::Archive>::Archived: rkyv::Deserialize<Row, rkyv::api::high::HighDeserializer<rkyv::rancor::Error>>
            + for<'validation> rkyv::bytecheck::CheckBytes<
                rkyv::rancor::Strategy<
                    rkyv::validation::Validator<
                        rkyv::validation::archive::ArchiveValidator<'validation>,
                        rkyv::validation::shared::SharedValidator,
                    >,
                    rkyv::rancor::Error,
                >,
            >;

    /// Remodel every row of the family and move each to its own key, in
    /// one commit.
    fn remodel_family<Row>(&mut self, table: TableReference<Row>) -> Result<(), StoreError>
    where
        Row: RemodelsAddresses
            + KeptBeforeModel
            + sema_engine::EngineStoredRecord
            + Clone
            + Send
            + Sync
            + 'static,
        <Row as rkyv::Archive>::Archived: rkyv::Deserialize<Row, rkyv::api::high::HighDeserializer<rkyv::rancor::Error>>
            + for<'validation> rkyv::bytecheck::CheckBytes<
                rkyv::rancor::Strategy<
                    rkyv::validation::Validator<
                        rkyv::validation::archive::ArchiveValidator<'validation>,
                        rkyv::validation::shared::SharedValidator,
                    >,
                    rkyv::rancor::Error,
                >,
            >;
}

impl RemodelsFamilies for Engine {
    fn held_before_model<Row>(&self, descriptor: &TableDescriptor<Row>) -> bool {
        self.catalog()
            .family_identity(descriptor.name())
            .is_some_and(|stored| stored != &descriptor.family_identity())
    }

    fn upgraded_before_model<Row, Older>(
        &mut self,
        descriptor: TableDescriptor<Row>,
        older: Older,
    ) -> Result<TableReference<Row>, StoreError>
    where
        Older: FnOnce(&mut Engine, TableDescriptor<Row>) -> Result<TableReference<Row>, StoreError>,
        Row: RemodelsAddresses
            + KeptBeforeModel
            + DefinedBeforeModel
            + sema_engine::UpgradeFrom<BeforeModel<Row>, Refusal = std::convert::Infallible>
            + sema_engine::EngineStoredRecord
            + Clone
            + Send
            + Sync
            + 'static,
        BeforeModel<Row>: sema_engine::EngineStoredValue + Send + Sync + 'static,
        <BeforeModel<Row> as rkyv::Archive>::Archived: rkyv::Deserialize<
                BeforeModel<Row>,
                rkyv::api::high::HighDeserializer<rkyv::rancor::Error>,
            > + for<'validation> rkyv::bytecheck::CheckBytes<
                rkyv::rancor::Strategy<
                    rkyv::validation::Validator<
                        rkyv::validation::archive::ArchiveValidator<'validation>,
                        rkyv::validation::shared::SharedValidator,
                    >,
                    rkyv::rancor::Error,
                >,
            >,
        <Row as rkyv::Archive>::Archived: rkyv::Deserialize<Row, rkyv::api::high::HighDeserializer<rkyv::rancor::Error>>
            + for<'validation> rkyv::bytecheck::CheckBytes<
                rkyv::rancor::Strategy<
                    rkyv::validation::Validator<
                        rkyv::validation::archive::ArchiveValidator<'validation>,
                        rkyv::validation::shared::SharedValidator,
                    >,
                    rkyv::rancor::Error,
                >,
            >,
    {
        let held = self.held_before_model(&descriptor);
        let at_before_model = TableDescriptor::<BeforeModel<Row>>::new(
            *descriptor.name(),
            descriptor.family().clone(),
            Row::schema_hash_before_model(),
        )
        .family_identity();
        let table = if self.catalog().family_identity(descriptor.name()) == Some(&at_before_model) {
            match sema_engine::MemoryUpgrades::upgrade_table::<BeforeModel<Row>, Row>(
                self, descriptor,
            ) {
                sema_engine::UpgradeOutcome::Upgraded(receipt) => receipt.table(),
                sema_engine::UpgradeOutcome::AlreadyCurrent(table) => table,
                sema_engine::UpgradeOutcome::Refused(refusal) => match *refusal.reason() {},
                sema_engine::UpgradeOutcome::Failed(failure) => {
                    return Err(StoreError::ModelAddressUpgrade(format!("{failure:?}")));
                }
            }
        } else {
            older(self, descriptor)?
        };
        if held {
            self.remodel_family(table)?;
        }
        Ok(table)
    }

    fn remodel_family<Row>(&mut self, table: TableReference<Row>) -> Result<(), StoreError>
    where
        Row: RemodelsAddresses
            + KeptBeforeModel
            + sema_engine::EngineStoredRecord
            + Clone
            + Send
            + Sync
            + 'static,
        <Row as rkyv::Archive>::Archived: rkyv::Deserialize<Row, rkyv::api::high::HighDeserializer<rkyv::rancor::Error>>
            + for<'validation> rkyv::bytecheck::CheckBytes<
                rkyv::rancor::Strategy<
                    rkyv::validation::Validator<
                        rkyv::validation::archive::ArchiveValidator<'validation>,
                        rkyv::validation::shared::SharedValidator,
                    >,
                    rkyv::rancor::Error,
                >,
            >,
    {
        let rows = self
            .match_records(QueryPlan::all(table))?
            .records()
            .to_vec();
        if rows.is_empty() {
            return Ok(());
        }
        let mut commit = self.begin_atomic_commit();
        let mut moved = Vec::new();
        for row in rows {
            let kept = row.key_before_model();
            let mut remodelled = row;
            remodelled.remodel();
            // A row an older upgrade already moved to its own key is only
            // rewritten there; remodelling is idempotent.
            let still_kept = remodelled.record_key() != kept
                && !self
                    .match_records(QueryPlan::key(table, kept.clone()))?
                    .records()
                    .is_empty();
            if still_kept {
                commit = commit.retract(table, kept);
                moved.push(remodelled);
            } else {
                commit = commit.mutate(table, remodelled);
            }
        }
        for remodelled in moved {
            commit = commit.assert(table, remodelled);
        }
        self.commit_atomic(commit)?;
        Ok(())
    }
}

impl RemodelsAddresses for signal_flow::SentBy {
    fn remodel(&mut self) {
        if let signal_flow::SentBy::Sender(address) = self {
            address.remodel();
        }
    }
}

impl RemodelsAddresses for signal_flow::Lock {
    fn remodel(&mut self) {
        self.sender.remodel();
        self.address.remodel();
    }
}

impl RemodelsAddresses for signal_flow::QueueEntry {
    fn remodel(&mut self) {
        self.sent_by.remodel();
    }
}

impl RemodelsAddresses for signal_flow::Metaflow {
    fn remodel(&mut self) {
        self.address.remodel();
        self.queue.remodel();
    }
}

impl RemodelsAddresses for signal_flow::Unknown_Data {
    fn remodel(&mut self) {
        match self {
            signal_flow::Unknown_Data::Address(address) => address.remodel(),
            signal_flow::Unknown_Data::Lock(lock) => lock.remodel(),
            signal_flow::Unknown_Data::FlowId(_) | signal_flow::Unknown_Data::Key(_) => {}
        }
    }
}

impl RemodelsAddresses for signal_flow::Refusal {
    fn remodel(&mut self) {
        match self {
            signal_flow::Refusal::Held(lock) => lock.remodel(),
            signal_flow::Refusal::Unknown(unknown) => unknown.remodel(),
            signal_flow::Refusal::Taken(address) => address.remodel(),
            _ => {}
        }
    }
}

impl RemodelsAddresses for signal_flow::DeliveryState {
    fn remodel(&mut self) {
        if let signal_flow::DeliveryState::Failed(refusal) = self {
            refusal.remodel();
        }
    }
}

impl RemodelsAddresses for signal_flow::DeliverySnapshot {
    fn remodel(&mut self) {
        self.sent_by.remodel();
        self.address.remodel();
        self.delivery_state.remodel();
    }
}

impl RemodelsAddresses for signal_flow::SendAcceptance {
    fn remodel(&mut self) {
        self.delivery_state.remodel();
    }
}

impl RemodelsAddresses for signal_flow::SendFailureReason {
    fn remodel(&mut self) {
        match self {
            signal_flow::SendFailureReason::RecipientUnavailable(address) => address.remodel(),
            signal_flow::SendFailureReason::Refused(refusal) => refusal.remodel(),
            _ => {}
        }
    }
}

impl RemodelsAddresses for signal_flow::SendFailure {
    fn remodel(&mut self) {
        self.send_failure_reason.remodel();
    }
}

impl RemodelsAddresses for signal_flow::LaunchProfile {
    fn remodel(&mut self) {
        self.address.remodel();
    }
}

impl RemodelsAddresses for signal_flow::LaunchAttempt {
    fn remodel(&mut self) {
        self.launch_profile.remodel();
    }
}

impl RemodelsAddresses for meta_signal_flow::OrdinarySendOutcome {
    fn remodel(&mut self) {
        match self {
            meta_signal_flow::OrdinarySendOutcome::Accepted(acceptance) => acceptance.remodel(),
            meta_signal_flow::OrdinarySendOutcome::Refused(refusal) => refusal.remodel(),
            meta_signal_flow::OrdinarySendOutcome::Failed(failure) => failure.remodel(),
            meta_signal_flow::OrdinarySendOutcome::PriorAdmission(_)
            | meta_signal_flow::OrdinarySendOutcome::Sent(_) => {}
        }
    }
}

impl RemodelsAddresses for meta_signal_flow::SendOutcomeRecord {
    fn remodel(&mut self) {
        self.first_address_option.remodel();
        self.second_address_option.remodel();
        self.ordinary_send_outcome.remodel();
    }
}

impl RemodelsAddresses for meta_signal_flow::QueueEvidence {
    fn remodel(&mut self) {
        match self {
            meta_signal_flow::QueueEvidence::Logical(entry) => entry.remodel(),
            meta_signal_flow::QueueEvidence::Legacy(legacy) => legacy.sent_by.remodel(),
        }
    }
}

impl RemodelsAddresses for meta_signal_flow::ConfigureMetaflow {
    fn remodel(&mut self) {
        self.address.remodel();
    }
}

impl RemodelsAddresses for meta_signal_flow::ConfigureLeaveOut {
    fn remodel(&mut self) {
        self.address.remodel();
    }
}

impl RemodelsAddresses for meta_signal_flow::ConfigureHarnessFlags {
    fn remodel(&mut self) {
        self.address.remodel();
    }
}

impl RemodelsAddresses for meta_signal_flow::HeartbeatConfig {
    fn remodel(&mut self) {
        self.heartbeat_recipients.remodel();
    }
}

impl RemodelsAddresses for crate::generated::send_tool::SendOutcome {
    fn remodel(&mut self) {
        use crate::generated::send_tool::{SendOutcome, SendRefusal};
        match self {
            SendOutcome::Accepted(accepted) => {
                accepted.sender.remodel();
                accepted.target.remodel();
                accepted.send_acceptance.remodel();
            }
            SendOutcome::LegacyUnknown(legacy) => {
                legacy.sender.remodel();
                legacy.target.remodel();
            }
            SendOutcome::Refused(SendRefusal::UnknownRecipient(target)) => target.remodel(),
            SendOutcome::Refused(_) => {}
        }
    }
}
