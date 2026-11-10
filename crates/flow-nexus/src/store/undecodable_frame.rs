//! The frames on the ordinary socket that decoded as no Query, each
//! recorded with what Flow read of its caller: the flow bound to the
//! caller's seat, if one is, and the caller's seat and process. The Report
//! of a hook built on an earlier contract is the frame this is for: its
//! client cannot read the answer it is given, so the record is how the
//! refusal is seen, through Observe.Undecodable. Nothing in the frame is
//! read or kept.

use super::{AnnouncesLaunchChanges, FlowStore, StoreError};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use sema_engine::{
    EngineRecord, FamilyName, MemoryDefinition, MemoryVersion, QueryPlan, RecordKey, SchemaHash,
    TableDescriptor, TableName,
};
use signal_flow::UndecodableFrame;

pub(super) const UNDECODABLE_FRAME_TABLE_NAME: TableName =
    TableName::new("flow_nexus_undecodable_frames");
pub(super) const UNDECODABLE_FRAME_FAMILY: &str = "flow-nexus-undecodable-frame";
pub(super) const UNDECODABLE_FRAME_SCHEMA: &str = "flow-nexus-undecodable-frame-v1";

/// One refused frame, keyed by when it was refused and by whom, so records
/// read back oldest first.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub struct UndecodableRecord {
    /// Nanoseconds since the Unix epoch, zero-padded, then the caller's
    /// process id.
    pub key: String,
    pub frame: UndecodableFrame,
}

impl EngineRecord for UndecodableRecord {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.key.clone())
    }
}

impl MemoryDefinition for UndecodableRecord {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(1)
    }

    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label(UNDECODABLE_FRAME_SCHEMA)
    }
}

pub(super) trait DescribesUndecodableFrameTable {
    fn descriptor() -> TableDescriptor<UndecodableRecord>;
}

impl DescribesUndecodableFrameTable for UndecodableRecord {
    fn descriptor() -> TableDescriptor<UndecodableRecord> {
        TableDescriptor::new(
            UNDECODABLE_FRAME_TABLE_NAME,
            FamilyName::new(UNDECODABLE_FRAME_FAMILY),
            SchemaHash::for_label(UNDECODABLE_FRAME_SCHEMA),
        )
    }
}

pub(crate) trait RecordsUndecodableFrames {
    /// Records one refused frame, in its own write, and announces it to
    /// every observer.
    fn record_undecodable(&self, frame: UndecodableFrame) -> Result<(), StoreError>;
    /// Every refused frame recorded, oldest first.
    fn undecodable_frames(&self) -> Result<Vec<UndecodableFrame>, StoreError>;
}

impl RecordsUndecodableFrames for FlowStore {
    fn record_undecodable(&self, frame: UndecodableFrame) -> Result<(), StoreError> {
        let refused_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|elapsed| elapsed.as_nanos())
            .unwrap_or_default();
        let record = UndecodableRecord {
            key: format!("{refused_at:024}-{:010}", frame.seat_evidence.process.pid.0),
            frame,
        };
        let commit = self
            .engine
            .begin_atomic_commit()
            .assert(self.undecodable_frames, record);
        self.engine.commit_atomic(commit)?;
        self.launch_changes.announce();
        Ok(())
    }

    fn undecodable_frames(&self) -> Result<Vec<UndecodableFrame>, StoreError> {
        let mut records = self
            .engine
            .match_records(QueryPlan::all(self.undecodable_frames))?
            .records()
            .to_vec();
        records.sort_by(|left, right| left.key.cmp(&right.key));
        Ok(records.into_iter().map(|record| record.frame).collect())
    }
}
