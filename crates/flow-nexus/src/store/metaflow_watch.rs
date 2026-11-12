//! What a metaflow subscriber reads and waits on. The metaflows a caller
//! sees are each Awake one and each asleep one still declared; they change
//! with a permaflow's row, its declaration, or its cohesive memberships.
//! Memory announces all three: the engine delivers each committed table change to a sink, in
//! the committing thread once the commit is made, and the sink advances
//! `metaflow_changes`. Nothing re-reads on a timer.

use super::metaflow_cohesion::StoredMetaflowCohesion;
use super::metaflow_declaration::{ReadsMetaflowDeclarations, StoredMetaMetaflowConfiguration};
use super::{
    AnnouncesLaunchChanges, FlowStore, LaunchChanges, ManagesMetaflowState, StoreError,
    StoredMetaflow,
};
use flow_ethos::{Permaflow, PermaflowState as State};
use sema_engine::{
    QueryPlan, SinkError, SubscriptionDeliveryMode, SubscriptionEvent, SubscriptionSink,
};
use std::sync::Arc;

/// Advances the count at each change the engine delivers to it.
struct MetaflowChangeSink {
    changes: LaunchChanges,
}

impl<RecordValue> SubscriptionSink<RecordValue> for MetaflowChangeSink {
    fn delivery_mode(&self) -> SubscriptionDeliveryMode {
        SubscriptionDeliveryMode::Inline
    }

    fn deliver(&self, event: SubscriptionEvent<RecordValue>) -> Result<(), SinkError> {
        if let SubscriptionEvent::Delta(_) = event {
            self.changes.announce();
        }
        Ok(())
    }
}

pub trait WatchesMetaflows {
    /// Has Memory announce `metaflow_changes` at each commit that changes a
    /// permaflow's row, its declaration, or a cohesive group. Subscribed once per store, at its
    /// first metaflow subscriber, for the store's life: the engine keeps
    /// every subscription it is given.
    fn watch_metaflows(&self) -> Result<(), StoreError>;
    /// The metaflows as `Metaflows` lists them: each Awake one, and each
    /// asleep one still declared. An undeclared sleeping row is history,
    /// not an Address an ordinary caller can launch; its past is kept but
    /// not listed.
    fn available_metaflows(&self) -> Result<Vec<Permaflow>, StoreError>;
}

impl WatchesMetaflows for FlowStore {
    fn watch_metaflows(&self) -> Result<(), StoreError> {
        let mut watched = self
            .metaflow_watch
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if *watched {
            return Ok(());
        }
        let sink = Arc::new(MetaflowChangeSink {
            changes: self.metaflow_changes.clone(),
        });
        let rows: Arc<dyn SubscriptionSink<StoredMetaflow>> = sink.clone();
        self.engine
            .subscribe(QueryPlan::all(self.metaflows), rows)?;
        let declarations: Arc<dyn SubscriptionSink<StoredMetaMetaflowConfiguration>> = sink.clone();
        self.engine.subscribe(
            QueryPlan::all(self.meta_configuration_tables.metaflows),
            declarations,
        )?;
        let cohesions: Arc<dyn SubscriptionSink<StoredMetaflowCohesion>> = sink;
        self.engine.subscribe(
            QueryPlan::all(self.meta_configuration_tables.cohesions),
            cohesions,
        )?;
        *watched = true;
        Ok(())
    }

    fn available_metaflows(&self) -> Result<Vec<Permaflow>, StoreError> {
        let mut available = Vec::new();
        for metaflow in self.list_metaflows()? {
            if matches!(metaflow.permaflow_state, State::Awake(_))
                || self.metaflow_declaration(&metaflow.address)?.is_some()
            {
                available.push(metaflow);
            }
        }
        Ok(available)
    }
}
