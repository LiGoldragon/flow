//! What Flow remembers of each flow's harness: the events its hooks
//! reported, in the order Flow recorded them, one row per flow keyed by its
//! FlowId (the vision's Memory `Flow.{ FlowId Voice State Vector<Event> }`,
//! the events half). The row lives in its own table, so a store written
//! before events existed opens unchanged and its flows simply hold none.
//!
//! An event is recorded only for a flow Flow holds: a FlowId with no flow
//! row is refused, never adopted (ruling 12 of flow f1c841).

use super::{FlowStore, ReadsFlowStore, StoreError};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use sema_engine::{
    Assertion, EngineRecord, FamilyName, KeyedMutation, QueryPlan, RecordKey, SchemaHash,
    TableDescriptor, TableName, TableReference,
};
use signal_flow::Event;
use std::sync::Mutex;

pub(super) const FLOW_EVENT_TABLE_NAME: TableName = TableName::new("flow_nexus_flow_events");

/// One flow's harness events, oldest first.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub struct FlowEvents {
    pub flow_id: String,
    pub event_vector: Vec<Event>,
}

impl EngineRecord for FlowEvents {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.flow_id.clone())
    }
}

/// The events table, and the hold that makes each append one
/// read-and-write: a harness may report two tool uses at once, and neither
/// may be lost.
pub struct EventTables {
    events: TableReference<FlowEvents>,
    append_gate: Mutex<()>,
}

pub trait RegistersEventTables: Sized {
    fn register(engine: &mut sema_engine::Engine) -> Result<Self, StoreError>;
}

impl RegistersEventTables for EventTables {
    fn register(engine: &mut sema_engine::Engine) -> Result<Self, StoreError> {
        Ok(Self {
            events: engine.register_table(TableDescriptor::new(
                FLOW_EVENT_TABLE_NAME,
                FamilyName::new("flow-nexus-flow-events"),
                SchemaHash::for_label("flow-nexus-flow-events-v1"),
            ))?,
            append_gate: Mutex::new(()),
        })
    }
}

/// What recording one event did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EventRecording {
    Recorded,
    /// No flow row carries this FlowId; nothing was written.
    UnknownFlow,
}

pub trait RecordsHarnessEvents {
    /// Appends one event to the flow's events, if Flow holds the flow.
    fn record_event(&self, flow_id: &str, event: Event) -> Result<EventRecording, StoreError>;
    /// The flow's events, oldest first; `None` when Flow does not hold the
    /// flow, an empty vector when it holds it and nothing was reported.
    fn events(&self, flow_id: &str) -> Result<Option<Vec<Event>>, StoreError>;
}

trait ReadsEventRow {
    fn event_row(&self, flow_id: &str) -> Result<Option<FlowEvents>, StoreError>;
}

impl ReadsEventRow for FlowStore {
    fn event_row(&self, flow_id: &str) -> Result<Option<FlowEvents>, StoreError> {
        let records = self
            .engine
            .match_records(QueryPlan::key(
                self.event_tables.events,
                RecordKey::new(flow_id),
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

impl RecordsHarnessEvents for FlowStore {
    fn record_event(&self, flow_id: &str, event: Event) -> Result<EventRecording, StoreError> {
        let _append = self
            .event_tables
            .append_gate
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if self.flow(flow_id)?.is_none() {
            return Ok(EventRecording::UnknownFlow);
        }
        match self.event_row(flow_id)? {
            Some(mut row) => {
                row.event_vector.push(event);
                self.engine.mutate_keyed(KeyedMutation::new(
                    self.event_tables.events,
                    RecordKey::new(flow_id),
                    row,
                ))?;
            }
            None => {
                self.engine.assert(Assertion::new(
                    self.event_tables.events,
                    FlowEvents {
                        flow_id: flow_id.into(),
                        event_vector: vec![event],
                    },
                ))?;
            }
        }
        Ok(EventRecording::Recorded)
    }

    fn events(&self, flow_id: &str) -> Result<Option<Vec<Event>>, StoreError> {
        if self.flow(flow_id)?.is_none() {
            return Ok(None);
        }
        Ok(Some(
            self.event_row(flow_id)?
                .map(|row| row.event_vector)
                .unwrap_or_default(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::{EventRecording, RecordsHarnessEvents};
    use crate::store::{FlowStore, OpensFlowStore, RegistersFlowIdentity};
    use signal_flow::{
        EndpointSelection, Event, FlowLifecycle, FlowNode, HarnessKind, HerdrRoute,
        HerdrRouteSelection, OriginClue,
    };

    fn node() -> FlowNode {
        FlowNode {
            flow_id: "5a4d0b".into(),
            session_id: "c0ffee00-0000-4000-8000-000000000001".into(),
            harness_kind: HarnessKind::Claude,
            endpoint_selection: EndpointSelection::Unavailable,
            herdr_route_selection: HerdrRouteSelection::Available(HerdrRoute {
                herdr_session_name: "sandbox".into(),
                herdr_agent_name: "sandbox-claude".into(),
                herdr_pane_id: "w1:p1".into(),
                herdr_terminal_id: "term-1".into(),
            }),
            origin_clue: OriginClue {
                flow_id: "5a4d0b".into(),
                session_id: "c0ffee00-0000-4000-8000-000000000001".into(),
                turn_id: "unavailable".into(),
            },
            flow_lifecycle: FlowLifecycle::Active,
        }
    }

    /// The events are the flow's Memory: kept in report order, kept across
    /// a reopen of the store, and never made for a flow Flow does not hold.
    #[test]
    fn a_flow_keeps_its_reported_events_in_order_across_a_reopen() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("flow.sema");
        {
            let store = FlowStore::open(&path).unwrap();
            assert!(matches!(
                store.register_flow(node()).unwrap(),
                crate::store::FlowRegistration::Registered(_)
            ));
            assert_eq!(store.events("5a4d0b").unwrap(), Some(vec![]));
            for event in [
                Event::Started,
                Event::ToolUsed("Bash".into()),
                Event::Stopped,
            ] {
                assert_eq!(
                    store.record_event("5a4d0b", event).unwrap(),
                    EventRecording::Recorded
                );
            }
            assert_eq!(
                store.record_event("0a0a0a", Event::Started).unwrap(),
                EventRecording::UnknownFlow
            );
        }
        let store = FlowStore::open(&path).unwrap();
        assert_eq!(
            store.events("5a4d0b").unwrap(),
            Some(vec![
                Event::Started,
                Event::ToolUsed("Bash".into()),
                Event::Stopped
            ])
        );
        assert_eq!(store.events("0a0a0a").unwrap(), None);
    }
}
