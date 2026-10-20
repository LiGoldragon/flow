//! The approval pipeline's rows: one landing per context-repository commit
//! Flow saw land, with each step's state, and one refresh-due row per flow
//! Flow is to end at its next rest. Both tables are added by a store's
//! first open by this Flow; a store written before them opens with them
//! empty.
//!
//! The work a landing hands to its implementers, and the report it gives
//! Psyche core Secondary, are queued in the same commit that records them
//! on the landing's row, so a Nexus that dies between the two neither
//! loses the work nor queues it twice.

use super::{FlowStore, ManagesMetaflowState, StoreError};
use crate::generated::landing::{
    CommitNaming, FlowNaming, KeepsLandings, Landing, LandingCarrying, LandingOrdering, Landings,
    RefreshDue, RefreshDueCarrying, RefreshesDue,
};
use sema_engine::{
    Assertion, Engine, EngineRecord, FamilyName, KeyedMutation, QueryPlan, RecordKey, SchemaHash,
    TableDescriptor, TableName, TableReference,
};
use signal_flow::{Address, QueuedRequest, SentBy};

pub(super) const LANDING_TABLE_NAME: TableName = TableName::new("flow_nexus_landings");
pub(super) const LANDING_FAMILY: &str = "flow-nexus-landing";
pub(super) const LANDING_SCHEMA: &str = "flow-nexus-landing-v1";
pub(super) const REFRESH_DUE_TABLE_NAME: TableName = TableName::new("flow_nexus_refresh_due");
pub(super) const REFRESH_DUE_FAMILY: &str = "flow-nexus-refresh-due";
pub(super) const REFRESH_DUE_SCHEMA: &str = "flow-nexus-refresh-due-v1";

impl EngineRecord for Landing {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.commit.clone())
    }
}

impl EngineRecord for RefreshDue {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.flow_id.clone())
    }
}

/// The two tables, registered together.
#[derive(Debug, Clone, Copy)]
pub(crate) struct LandingTables {
    landings: TableReference<Landing>,
    refreshes_due: TableReference<RefreshDue>,
}

pub(crate) trait RegistersLandingTables {
    fn descriptors() -> (TableDescriptor<Landing>, TableDescriptor<RefreshDue>);
    fn register(engine: &mut Engine) -> Result<LandingTables, StoreError>;
}

impl RegistersLandingTables for LandingTables {
    fn descriptors() -> (TableDescriptor<Landing>, TableDescriptor<RefreshDue>) {
        (
            TableDescriptor::new(
                LANDING_TABLE_NAME,
                FamilyName::new(LANDING_FAMILY),
                SchemaHash::for_label(LANDING_SCHEMA),
            ),
            TableDescriptor::new(
                REFRESH_DUE_TABLE_NAME,
                FamilyName::new(REFRESH_DUE_FAMILY),
                SchemaHash::for_label(REFRESH_DUE_SCHEMA),
            ),
        )
    }

    fn register(engine: &mut Engine) -> Result<LandingTables, StoreError> {
        let (landings, refreshes_due) = Self::descriptors();
        Ok(LandingTables {
            landings: engine.register_table(landings)?,
            refreshes_due: engine.register_table(refreshes_due)?,
        })
    }
}

/// The metaflow and the report the pipeline speaks as. A landing's work
/// and its report are queued as Psyche core Secondary's, the flow that
/// lands the living's approvals and routes their implementation.
pub(crate) trait LandingSender {
    fn landing_sender() -> Address {
        Address {
            aspect: signal_flow::Aspect::Psyche,
            topic: "core".into(),
            layer: signal_flow::Layer::Secondary,
        }
    }
}

impl LandingSender for FlowStore {}

impl KeepsLandings for FlowStore {
    fn landing_row<N: CommitNaming>(&self, input: N) -> Result<Option<Landing>, StoreError> {
        let records = self
            .engine
            .match_records(QueryPlan::key(
                self.landing_tables.landings,
                RecordKey::new(input.commit()),
            ))?
            .records()
            .to_vec();
        match records.as_slice() {
            [] => Ok(None),
            [row] => Ok(Some(row.clone())),
            _ => Err(StoreError::StateInvariant),
        }
    }

    fn landing_rows(&self) -> Result<Landings, StoreError> {
        Ok(self
            .engine
            .match_records(QueryPlan::all(self.landing_tables.landings))?
            .records()
            .to_vec())
    }

    fn write_landing<N: LandingCarrying>(&self, input: N) -> Result<bool, StoreError> {
        let landing = input.carried_landing();
        let commit = landing.commit.clone();
        if self.landing_row(commit.as_str())?.is_some() {
            self.engine.mutate_keyed(KeyedMutation::new(
                self.landing_tables.landings,
                RecordKey::new(commit),
                landing,
            ))?;
            Ok(false)
        } else {
            self.engine
                .assert(Assertion::new(self.landing_tables.landings, landing))?;
            Ok(true)
        }
    }

    fn queue_landing_orders<N: LandingCarrying, O: LandingOrdering>(
        &self,
        input_0: N,
        input_1: O,
    ) -> Result<Vec<Address>, StoreError> {
        let landing = input_0.carried_landing();
        let exists = self.landing_row(landing.commit.as_str())?.is_some();
        let mut commit = self.engine.begin_atomic_commit();
        let mut queued = Vec::new();
        for order in input_1.landing_orders() {
            let Some(mut stored) = self.stored_metaflow(&order.address)? else {
                continue;
            };
            stored.metaflow.queue.push(QueuedRequest {
                sent_by: SentBy::Sender(Self::landing_sender()),
                request: order.request,
            });
            commit = commit.mutate(self.metaflows, stored);
            queued.push(order.address);
        }
        commit = if exists {
            commit.mutate(self.landing_tables.landings, landing)
        } else {
            commit.assert(self.landing_tables.landings, landing)
        };
        self.engine.commit_atomic(commit)?;
        // This is durable enqueue, never a claim of native delivery or wake.
        Ok(queued)
    }

    fn refresh_due<N: FlowNaming>(&self, input: N) -> Result<Option<RefreshDue>, StoreError> {
        let records = self
            .engine
            .match_records(QueryPlan::key(
                self.landing_tables.refreshes_due,
                RecordKey::new(input.flow_id()),
            ))?
            .records()
            .to_vec();
        match records.as_slice() {
            [] => Ok(None),
            [row] => Ok(Some(row.clone())),
            _ => Err(StoreError::StateInvariant),
        }
    }

    fn refreshes_due(&self) -> Result<RefreshesDue, StoreError> {
        Ok(self
            .engine
            .match_records(QueryPlan::all(self.landing_tables.refreshes_due))?
            .records()
            .to_vec())
    }

    fn mark_refresh_due<N: LandingCarrying, O: RefreshDueCarrying>(
        &self,
        input_0: N,
        input_1: O,
    ) -> Result<bool, StoreError> {
        let landing = input_0.carried_landing();
        let due = input_1.carried_refresh_due();
        let marked = self.refresh_due(due.flow_id.as_str())?.is_none();
        let landing_exists = self.landing_row(landing.commit.as_str())?.is_some();
        let mut commit = self.engine.begin_atomic_commit();
        if marked {
            commit = commit.assert(self.landing_tables.refreshes_due, due);
        }
        commit = if landing_exists {
            commit.mutate(self.landing_tables.landings, landing)
        } else {
            commit.assert(self.landing_tables.landings, landing)
        };
        self.engine.commit_atomic(commit)?;
        Ok(marked)
    }

    fn clear_refresh_due<N: FlowNaming>(&self, input: N) -> Result<bool, StoreError> {
        let flow_id = input.flow_id();
        if self.refresh_due(flow_id.as_str())?.is_none() {
            return Ok(false);
        }
        let commit = self
            .engine
            .begin_atomic_commit()
            .retract(self.landing_tables.refreshes_due, RecordKey::new(flow_id));
        self.engine.commit_atomic(commit)?;
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use crate::generated::landing::{
        KeepsLandings, Landing, LandingOrder, RefreshCause, RefreshDue, Retrained, Retraining, Step,
    };
    use crate::store::metaflow::SeedsMetaflow;
    use crate::store::metaflow::SleepsMetaflows;
    use crate::store::{FlowStore, OpensFlowStore};
    use flow_ethos::State;
    use signal_flow::{Address, Aspect, Layer, Metaflow, Request, SentBy};

    fn landing(commit: &str) -> Landing {
        Landing {
            commit: commit.into(),
            base: "6c8e266".into(),
            subject: "Land flow vision".into(),
            changed_paths: vec!["vision/flow.md".into()],
            deploy: Step::Pending,
            retrain: Step::Pending,
            retrainings: vec![],
            implement: Step::Pending,
            owners: vec![],
            reported: false,
        }
    }

    fn mind_core() -> Address {
        Address {
            aspect: Aspect::Mind,
            topic: "core".into(),
            layer: Layer::Secondary,
        }
    }

    #[test]
    fn a_landing_row_is_written_rewritten_and_kept_across_a_reopen() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("flow.sema");
        {
            let store = FlowStore::open(&path).unwrap();
            assert!(store.landing_row("2d9fbb6").unwrap().is_none());
            assert!(store.write_landing(landing("2d9fbb6")).unwrap());
            let mut deployed = landing("2d9fbb6");
            deployed.deploy = Step::Witnessed("served 2d9fbb6".into());
            assert!(!store.write_landing(deployed).unwrap());
        }
        let store = FlowStore::open(&path).unwrap();
        assert_eq!(
            store.landing_row("2d9fbb6").unwrap().unwrap().deploy,
            Step::Witnessed("served 2d9fbb6".into())
        );
        assert_eq!(store.landing_rows().unwrap().len(), 1);
    }

    #[test]
    fn a_refresh_due_mark_is_written_with_its_landing_once_and_cleared() {
        let directory = tempfile::tempdir().unwrap();
        let store = FlowStore::open(&directory.path().join("flow.sema")).unwrap();
        let mut marked = landing("2d9fbb6");
        marked.retrainings.push(Retraining {
            flow_id: "c1a0de".into(),
            address: mind_core(),
            retrained: Retrained::Marked,
        });
        let due = RefreshDue {
            flow_id: "c1a0de".into(),
            refresh_cause: RefreshCause::Landed("2d9fbb6".into()),
        };
        assert!(store.mark_refresh_due(marked.clone(), due.clone()).unwrap());
        assert!(!store.mark_refresh_due(marked.clone(), due.clone()).unwrap());
        assert_eq!(store.refresh_due("c1a0de").unwrap(), Some(due));
        assert_eq!(store.landing_row("2d9fbb6").unwrap(), Some(marked));
        assert!(store.clear_refresh_due("c1a0de").unwrap());
        assert!(!store.clear_refresh_due("c1a0de").unwrap());
        assert!(store.refreshes_due().unwrap().is_empty());
    }

    #[test]
    fn landing_orders_are_queued_with_the_landing_and_skip_an_unknown_metaflow() {
        let directory = tempfile::tempdir().unwrap();
        let store = FlowStore::open(&directory.path().join("flow.sema")).unwrap();
        store.seed_metaflow(Metaflow {
            address: mind_core(),
            state: State::Asleep,
            past: vec![],
            queue: vec![],
        });
        let mut implemented = landing("2d9fbb6");
        implemented.implement = Step::Witnessed("queued".into());
        let unknown = Address {
            aspect: Aspect::Mind,
            topic: "nowhere".into(),
            layer: Layer::Secondary,
        };
        let queued = store
            .queue_landing_orders(
                implemented.clone(),
                vec![
                    LandingOrder {
                        address: mind_core(),
                        request: Request::Order("Implement 2d9fbb6.".into()),
                    },
                    LandingOrder {
                        address: unknown,
                        request: Request::Order("Lost.".into()),
                    },
                ],
            )
            .unwrap();
        assert_eq!(queued, vec![mind_core()]);
        let metaflow = store.metaflow(&mind_core()).unwrap().unwrap();
        assert_eq!(metaflow.queue.len(), 1);
        assert_eq!(
            metaflow.queue[0].sent_by,
            SentBy::Sender(Address {
                aspect: Aspect::Psyche,
                topic: "core".into(),
                layer: Layer::Secondary,
            })
        );
        assert_eq!(store.landing_row("2d9fbb6").unwrap(), Some(implemented));
    }
}
