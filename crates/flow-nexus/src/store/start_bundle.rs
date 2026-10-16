//! The system-prompt bundle an Address was last started with: the caller's
//! bytes, as Start read them, before the launch's own section was appended.
//! One row per Address, rewritten at each Start. A fresh wake uses it as the
//! woken seat's system prompt, so the spirit and intent modules the launcher
//! put in the system prompt survive every wake.
//!
//! The row lives in its own table, so a store written before start bundles
//! were kept opens with the table added and its Addresses simply have none.

use super::{FlowStore, ReadsFlowMemory, StoreError};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use sema_engine::{
    Assertion, EngineRecord, FamilyName, KeyedMutation, QueryPlan, RecordKey, SchemaHash,
    TableDescriptor, TableName, TableReference,
};
use signal_flow::Address;

pub(super) const START_BUNDLE_TABLE_NAME: TableName = TableName::new("flow_nexus_start_bundles");
pub(super) const START_BUNDLE_FAMILY: &str = "flow-nexus-start-bundle";
pub(super) const START_BUNDLE_SCHEMA: &str = "flow-nexus-start-bundle-v1";

/// The caller's bundle bytes of the last Start at an Address.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub struct StartBundle {
    pub address: Address,
    pub bytes: Vec<u8>,
}

impl EngineRecord for StartBundle {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(FlowStore::address_record_key(&self.address))
    }
}

pub(super) trait DescribesStartBundleTable {
    fn descriptor() -> TableDescriptor<StartBundle>;
}

impl DescribesStartBundleTable for StartBundle {
    fn descriptor() -> TableDescriptor<StartBundle> {
        TableDescriptor::new(
            START_BUNDLE_TABLE_NAME,
            FamilyName::new(START_BUNDLE_FAMILY),
            SchemaHash::for_label(START_BUNDLE_SCHEMA),
        )
    }
}

pub trait RecordsStartBundle {
    /// Writes the bundle bytes an Address was started with, replacing the
    /// last ones.
    fn record_start_bundle(&self, address: &Address, bytes: &[u8]) -> Result<(), StoreError>;
    /// The bundle bytes the Address was last started with; None when no
    /// Start recorded any.
    fn start_bundle(&self, address: &Address) -> Result<Option<Vec<u8>>, StoreError>;
}

trait ReadsStartBundleRow {
    fn start_bundle_row(&self, address: &Address) -> Result<Option<StartBundle>, StoreError>;
    fn start_bundles(&self) -> TableReference<StartBundle>;
}

impl ReadsStartBundleRow for FlowStore {
    fn start_bundles(&self) -> TableReference<StartBundle> {
        self.start_bundles
    }

    fn start_bundle_row(&self, address: &Address) -> Result<Option<StartBundle>, StoreError> {
        let records = self
            .engine
            .match_records(QueryPlan::key(
                self.start_bundles(),
                RecordKey::new(FlowStore::address_record_key(address)),
            ))?
            .records()
            .to_vec();
        match records.as_slice() {
            [] => Ok(None),
            [row] => Ok(Some(row.clone())),
            _ => Err(StoreError::StateInvariant),
        }
    }
}

impl RecordsStartBundle for FlowStore {
    fn record_start_bundle(&self, address: &Address, bytes: &[u8]) -> Result<(), StoreError> {
        let row = StartBundle {
            address: address.clone(),
            bytes: bytes.to_vec(),
        };
        if self.start_bundle_row(address)?.is_some() {
            self.engine.mutate_keyed(KeyedMutation::new(
                self.start_bundles(),
                RecordKey::new(FlowStore::address_record_key(address)),
                row,
            ))?;
        } else {
            self.engine
                .assert(Assertion::new(self.start_bundles(), row))?;
        }
        Ok(())
    }

    fn start_bundle(&self, address: &Address) -> Result<Option<Vec<u8>>, StoreError> {
        Ok(self.start_bundle_row(address)?.map(|row| row.bytes))
    }
}

#[cfg(test)]
mod tests {
    use super::RecordsStartBundle;
    use crate::store::{FlowStore, OpensFlowStore};
    use signal_flow::{Address, Aspect, Layer};

    fn address(layer: Layer) -> Address {
        Address {
            aspect: Aspect::Mind,
            topic: "flow".into(),
            layer,
        }
    }

    #[test]
    fn an_address_start_bundle_is_rewritten_at_each_start_and_kept_across_a_reopen() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("flow.sema");
        {
            let store = FlowStore::open(&path).unwrap();
            assert_eq!(
                store.start_bundle(&address(Layer::Secondary)).unwrap(),
                None
            );
            store
                .record_start_bundle(&address(Layer::Secondary), b"first\n")
                .unwrap();
            store
                .record_start_bundle(&address(Layer::Secondary), b"second\n")
                .unwrap();
        }
        let store = FlowStore::open(&path).unwrap();
        assert_eq!(
            store.start_bundle(&address(Layer::Secondary)).unwrap(),
            Some(b"second\n".to_vec())
        );
        assert_eq!(store.start_bundle(&address(Layer::Tertiary)).unwrap(), None);
    }
}
