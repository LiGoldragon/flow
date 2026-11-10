//! What Flow remembers of each flow's harness: the events its hooks
//! reported, in the order Flow recorded them, one row per flow keyed by its
//! FlowId (the vision's Memory `Flow.{ FlowId Voice State Vector<Event> }`,
//! the events half).
//!
//! An event is recorded only for a flow Flow holds: a FlowId with no flow
//! row is refused, never adopted (ruling 12 of flow f1c841). Flow holds a
//! flow from the moment Reserve claims its FlowId, before its harness
//! starts and so before it is registered: Reserve writes the flow's empty
//! events row, and that row is what lets the harness's first `Started`
//! land.
//!
//! 0.53.3 (signal-flow 57.0.0) gives a Stop the background work its harness
//! had in flight, and stores events under v5. The 0.53.2 -> 0.53.3 upgrade
//! converts every stored v4 row with the rest of that Memory, each Stop
//! carrying no background work: a v4 Stop was read as the flow's rest.

use super::{FlowStore, ReadsFlowStore, StoreError};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use sema_engine::{
    Assertion, EngineRecord, FamilyName, KeyedMutation, MemoryDefinition, MemoryVersion, QueryPlan,
    RecordKey, Retraction, SchemaHash, TableDescriptor, TableName, TableReference,
};
use signal_flow::Event;
use std::sync::Mutex;

pub(super) const FLOW_EVENT_TABLE_NAME: TableName = TableName::new("flow_nexus_flow_events");
pub(super) const FLOW_EVENT_FAMILY: &str = "flow-nexus-flow-events";
pub(super) const FLOW_EVENT_SCHEMA: &str = "flow-nexus-flow-events-v5";

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

impl MemoryDefinition for FlowEvents {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(5)
    }

    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label(FLOW_EVENT_SCHEMA)
    }
}

pub(super) trait DescribesEventTable {
    fn descriptor() -> TableDescriptor<FlowEvents>;
}

impl DescribesEventTable for FlowEvents {
    fn descriptor() -> TableDescriptor<FlowEvents> {
        TableDescriptor::new(
            FLOW_EVENT_TABLE_NAME,
            FamilyName::new(FLOW_EVENT_FAMILY),
            SchemaHash::for_label(FLOW_EVENT_SCHEMA),
        )
        .with_predecessor::<super::request_id_upgrade::PriorFlowEvents>()
    }
}

/// The events table, and the hold that makes each append one
/// read-and-write: a harness may report two tool uses at once, and neither
/// may be lost.
pub struct EventTables {
    pub(super) events: TableReference<FlowEvents>,
    append_gate: Mutex<()>,
}

pub trait RegistersEventTables: Sized {
    fn register(engine: &mut sema_engine::Engine) -> Result<Self, StoreError>;
}

impl RegistersEventTables for EventTables {
    fn register(engine: &mut sema_engine::Engine) -> Result<Self, StoreError> {
        let events = engine.register_table(<FlowEvents as DescribesEventTable>::descriptor())?;
        Ok(Self {
            events,
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
    /// Holds a flow whose FlowId Reserve claimed: its events row, empty,
    /// unless it already has one.
    fn hold_reserved_flow(&self, flow_id: &str) -> Result<(), StoreError>;
    /// Lets go of a flow Reserve held whose launch was refused: its events
    /// row is retracted unless the flow was registered. Releasing a flow
    /// Flow does not hold changes nothing.
    fn release_reserved_flow(&self, flow_id: &str) -> Result<(), StoreError>;
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
        let row = self.event_row(flow_id)?;
        if row.is_none() && self.flow(flow_id)?.is_none() {
            return Ok(EventRecording::UnknownFlow);
        }
        match row {
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
        match self.event_row(flow_id)? {
            Some(row) => Ok(Some(row.event_vector)),
            None if self.flow(flow_id)?.is_some() => Ok(Some(Vec::new())),
            None => Ok(None),
        }
    }

    fn hold_reserved_flow(&self, flow_id: &str) -> Result<(), StoreError> {
        let _append = self
            .event_tables
            .append_gate
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if self.event_row(flow_id)?.is_none() {
            self.engine.assert(Assertion::new(
                self.event_tables.events,
                FlowEvents {
                    flow_id: flow_id.into(),
                    event_vector: Vec::new(),
                },
            ))?;
        }
        Ok(())
    }

    fn release_reserved_flow(&self, flow_id: &str) -> Result<(), StoreError> {
        let _append = self
            .event_tables
            .append_gate
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if self.flow(flow_id)?.is_none() && self.event_row(flow_id)?.is_some() {
            self.engine.retract(Retraction::new(
                self.event_tables.events,
                RecordKey::new(flow_id),
            ))?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{EventRecording, RecordsHarnessEvents};
    use crate::store::{FlowStore, OpensFlowStore, RegistersFlowIdentity};
    use signal_flow::{
        EndpointSelection, Event, FlowLifecycle, FlowNode, FlowOrigin, HarnessKind, HerdrRoute,
        HerdrRouteSelection, Origin,
    };

    fn node() -> FlowNode {
        FlowNode {
            flow_id: flow_ethos::FlowId("5a4d0b".into()),
            session_id: signal_flow::SessionId("c0ffee00-0000-4000-8000-000000000001".into()),
            harness_kind: HarnessKind::Claude,
            endpoint_selection: EndpointSelection::Unavailable,
            herdr_route_selection: HerdrRouteSelection::Available(HerdrRoute {
                herdr_session_name: signal_flow::HerdrSessionName("sandbox".into()),
                herdr_agent_name: signal_flow::HerdrAgentName("sandbox-claude".into()),
                herdr_pane_id: signal_flow::HerdrPaneId("w1:p1".into()),
                herdr_terminal_id: signal_flow::HerdrTerminalId("term-1".into()),
            }),
            origin: Origin::Flow(FlowOrigin {
                flow_id: flow_ethos::FlowId("5a4d0b".into()),
                session_id: signal_flow::SessionId("c0ffee00-0000-4000-8000-000000000001".into()),
            }),
            flow_lifecycle: FlowLifecycle::Active,
        }
    }

    fn stop() -> Event {
        Event::Stopped(signal_flow::NativeStop {
            native_session_id_option: Some(signal_flow::NativeSessionId(
                "c0ffee00-0000-4000-8000-000000000001".into(),
            )),
            transcript_path_option: Some(signal_flow::TranscriptPath("/t/c0ffee00.jsonl".into())),
            last_assistant_message_option: signal_flow::LastAssistantMessageOption(None),
            background_work: signal_flow::BackgroundWork(vec![]),
        })
    }

    /// A start naming its session, a tool use, a turn end, a cleared
    /// session and the start of the session after it.
    fn events() -> Vec<Event> {
        vec![
            Event::Started(Some(signal_flow::NativeSessionId(
                "c0ffee00-0000-4000-8000-000000000001".into(),
            ))),
            Event::ToolUsed("Bash".into()),
            stop(),
            Event::Cleared(Some(signal_flow::NativeSessionId(
                "c0ffee00-0000-4000-8000-000000000001".into(),
            ))),
            Event::Started(Some(signal_flow::NativeSessionId(
                "c0ffee00-0000-4000-8000-000000000002".into(),
            ))),
        ]
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
            for event in events() {
                assert_eq!(
                    store.record_event("5a4d0b", event).unwrap(),
                    EventRecording::Recorded
                );
            }
            assert_eq!(
                store.record_event("0a0a0a", Event::Started(None)).unwrap(),
                EventRecording::UnknownFlow
            );
        }
        let store = FlowStore::open(&path).unwrap();
        assert_eq!(store.events("5a4d0b").unwrap(), Some(events()));
        assert_eq!(store.events("0a0a0a").unwrap(), None);
    }

    /// A held flow whose launch was refused is let go: reports for it are
    /// refused again, and the release lasts across a reopen. A registered
    /// flow keeps its events.
    #[test]
    fn a_released_reserved_flow_is_no_longer_held() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("flow.sema");
        {
            let store = FlowStore::open(&path).unwrap();
            store.hold_reserved_flow("0b0b0b").unwrap();
            assert_eq!(store.events("0b0b0b").unwrap(), Some(vec![]));
            store.release_reserved_flow("0b0b0b").unwrap();
            assert_eq!(store.events("0b0b0b").unwrap(), None);
            assert_eq!(
                store.record_event("0b0b0b", Event::Started(None)).unwrap(),
                EventRecording::UnknownFlow
            );
            store.release_reserved_flow("0b0b0b").unwrap();
            assert!(matches!(
                store.register_flow(node()).unwrap(),
                crate::store::FlowRegistration::Registered(_)
            ));
            store.record_event("5a4d0b", Event::Started(None)).unwrap();
            store.release_reserved_flow("5a4d0b").unwrap();
        }
        let store = FlowStore::open(&path).unwrap();
        assert_eq!(store.events("0b0b0b").unwrap(), None);
        assert_eq!(
            store.events("5a4d0b").unwrap(),
            Some(vec![Event::Started(None)])
        );
    }
}
