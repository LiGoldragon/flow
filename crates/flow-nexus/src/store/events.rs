//! What Flow remembers of each flow's harness: the events its hooks
//! reported, in the order Flow recorded them, one row per flow keyed by its
//! FlowId (the vision's Memory `Flow.{ FlowId Voice State Vector<Event> }`,
//! the events half). The row lives in its own table, so a store written
//! before events existed opens unchanged and its flows simply hold none.
//!
//! An event is recorded only for a flow Flow holds: a FlowId with no flow
//! row is refused, never adopted (ruling 12 of flow f1c841). Flow holds a
//! flow from the moment Reserve claims its FlowId, before its harness
//! starts and so before it is registered: Reserve writes the flow's empty
//! events row, and that row is what lets the harness's first `Started`
//! land.
//!
//! A Stop recorded before Flow 0.35.19 carried no native identity: it is
//! upgraded to `Stopped.{ None None }`, which is kept as it was seen and
//! never taken for the flow's own turn end.

use super::{FlowStore, ReadsFlowStore, StoreError};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use memory_engine::{
    Assertion, Decision, EngineRecord, FamilyName, KeyedMutation, MemoryDefinition, MemoryUpgrades,
    MemoryVersion, QueryPlan, RecordKey, Retraction, SchemaHash, TableDescriptor, TableName,
    TableReference, UpgradeFrom, UpgradeOutcome,
};
use signal_flow::{Event, NativeStop};
use std::sync::Mutex;

pub(super) const FLOW_EVENT_TABLE_NAME: TableName = TableName::new("flow_nexus_flow_events");
pub(super) const FLOW_EVENT_FAMILY: &str = "flow-nexus-flow-events";
pub(super) const FLOW_EVENT_SCHEMA: &str = "flow-nexus-flow-events-v3";

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
        MemoryVersion::new(3)
    }

    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label(FLOW_EVENT_SCHEMA)
    }
}

/// A harness event as signal-flow 20.0.0 archived it: a Stop without the
/// native identity its hook was given.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub enum EventBeforeIdentity {
    Started,
    ToolUsed(String),
    Stopped,
}

/// One flow's events as Flow kept them before 0.35.19. It is read only by
/// the v1-to-v2 upgrade.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub struct FlowEventsV1 {
    pub flow_id: String,
    pub event_vector: Vec<EventBeforeIdentity>,
}

impl EngineRecord for FlowEventsV1 {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.flow_id.clone())
    }
}

impl MemoryDefinition for FlowEventsV1 {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(1)
    }

    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label("flow-nexus-flow-events-v1")
    }
}

/// The signal-flow 9 event archive immediately before Stop began carrying
/// Claude's final assistant message.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub struct FlowEventsV2 {
    pub flow_id: String,
    pub event_vector: Vec<EventV2>,
}
impl EngineRecord for FlowEventsV2 {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.flow_id.clone())
    }
}
impl MemoryDefinition for FlowEventsV2 {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(2)
    }
    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label("flow-nexus-flow-events-v2")
    }
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub enum EventV2 {
    Started,
    ToolUsed(String),
    Stopped(NativeStopV2),
    PromptSubmitted(signal_flow::NativePrompt),
}
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub struct NativeStopV2 {
    pub native_session_id_option: Option<String>,
    pub transcript_path_option: Option<String>,
}

impl From<EventV2> for Event {
    fn from(event: EventV2) -> Self {
        match event {
            EventV2::Started => Event::Started,
            EventV2::ToolUsed(tool) => Event::ToolUsed(tool),
            EventV2::PromptSubmitted(prompt) => Event::PromptSubmitted(prompt),
            EventV2::Stopped(stop) => Event::Stopped(NativeStop {
                native_session_id_option: stop.native_session_id_option,
                transcript_path_option: stop.transcript_path_option,
                last_assistant_message_option: None,
            }),
        }
    }
}

impl UpgradeFrom<FlowEventsV2> for FlowEvents {
    type Refusal = std::convert::Infallible;
    fn upgrade_from(previous: FlowEventsV2) -> Decision<Self, Self::Refusal> {
        Decision::Ready(Self {
            flow_id: previous.flow_id,
            event_vector: previous.event_vector.into_iter().map(Event::from).collect(),
        })
    }
}

impl From<EventBeforeIdentity> for Event {
    fn from(event: EventBeforeIdentity) -> Self {
        match event {
            EventBeforeIdentity::Started => Event::Started,
            EventBeforeIdentity::ToolUsed(tool) => Event::ToolUsed(tool),
            EventBeforeIdentity::Stopped => Event::Stopped(NativeStop {
                native_session_id_option: None,
                transcript_path_option: None,
                last_assistant_message_option: None,
            }),
        }
    }
}

impl UpgradeFrom<FlowEventsV1> for FlowEvents {
    type Refusal = std::convert::Infallible;

    fn upgrade_from(previous: FlowEventsV1) -> Decision<Self, Self::Refusal> {
        Decision::Ready(Self {
            flow_id: previous.flow_id,
            event_vector: previous.event_vector.into_iter().map(Event::from).collect(),
        })
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
        .with_predecessor::<FlowEventsV1>()
        .with_predecessor::<FlowEventsV2>()
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
    fn register(engine: &mut memory_engine::Engine) -> Result<Self, StoreError>;
}

impl RegistersEventTables for EventTables {
    fn register(engine: &mut memory_engine::Engine) -> Result<Self, StoreError> {
        let descriptor = <FlowEvents as DescribesEventTable>::descriptor();
        let from_v1 = descriptor.clone();
        let events = match engine.upgrade_table::<FlowEventsV2, FlowEvents>(descriptor) {
            UpgradeOutcome::Upgraded(receipt) => receipt.table(),
            UpgradeOutcome::AlreadyCurrent(table) => table,
            UpgradeOutcome::Refused(refusal) => match *refusal.reason() {},
            UpgradeOutcome::Failed(failure)
                if failure.phase() == memory_engine::StoragePhase::Admission =>
            {
                match engine.upgrade_table::<FlowEventsV1, FlowEvents>(from_v1) {
                    UpgradeOutcome::Upgraded(receipt) => receipt.table(),
                    UpgradeOutcome::AlreadyCurrent(table) => table,
                    UpgradeOutcome::Refused(refusal) => match *refusal.reason() {},
                    UpgradeOutcome::Failed(failure) => {
                        return Err(StoreError::EventUpgrade(format!("{failure:?}")));
                    }
                }
            }
            UpgradeOutcome::Failed(failure) => {
                return Err(StoreError::EventUpgrade(format!("{failure:?}")));
            }
        };
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
    use super::{
        EventRecording, EventV2, FlowEvents, FlowEventsV2, NativeStopV2, RecordsHarnessEvents,
    };
    use crate::store::{FlowStore, OpensFlowStore, RegistersFlowIdentity};
    use memory_engine::{Decision, UpgradeFrom};
    use signal_flow::{
        EndpointSelection, Event, FlowLifecycle, FlowNode, FlowOrigin, HarnessKind, HerdrRoute,
        HerdrRouteSelection, Origin,
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
            origin: Origin::Flow(FlowOrigin {
                flow_id: "5a4d0b".into(),
                session_id: "c0ffee00-0000-4000-8000-000000000001".into(),
            }),
            flow_lifecycle: FlowLifecycle::Active,
        }
    }

    fn stop() -> Event {
        Event::Stopped(signal_flow::NativeStop {
            native_session_id_option: Some("c0ffee00-0000-4000-8000-000000000001".into()),
            transcript_path_option: Some("/t/c0ffee00.jsonl".into()),
            last_assistant_message_option: None,
        })
    }

    #[test]
    fn v2_event_upgrade_preserves_order_and_every_event_with_empty_new_stop_field() {
        let old_stop = NativeStopV2 {
            native_session_id_option: Some("session".into()),
            transcript_path_option: Some("/tmp/session.jsonl".into()),
        };
        let prompt = signal_flow::NativePrompt {
            native_session_id_option: Some("session".into()),
            prompt_id_option: Some("prompt-1".into()),
            prompt_sha256: "sha256".into(),
        };
        let upgraded = match FlowEvents::upgrade_from(FlowEventsV2 {
            flow_id: "flow".into(),
            event_vector: vec![
                EventV2::Started,
                EventV2::ToolUsed("Bash".into()),
                EventV2::PromptSubmitted(prompt.clone()),
                EventV2::Stopped(old_stop.clone()),
            ],
        }) {
            Decision::Ready(upgraded) => upgraded,
            Decision::Refused(never) => match never {},
        };
        assert_eq!(upgraded.flow_id, "flow");
        assert_eq!(
            upgraded.event_vector,
            vec![
                Event::Started,
                Event::ToolUsed("Bash".into()),
                Event::PromptSubmitted(prompt),
                Event::Stopped(signal_flow::NativeStop {
                    native_session_id_option: old_stop.native_session_id_option,
                    transcript_path_option: old_stop.transcript_path_option,
                    last_assistant_message_option: None,
                }),
            ]
        );
    }

    /// The events are the flow's Memory: kept in report order, kept across
    /// a reopen of the store, and never made for a flow Flow does not hold.
    #[test]
    fn a_flow_keeps_its_reported_events_in_order_across_a_reopen() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("flow.memory");
        {
            let store = FlowStore::open(&path).unwrap();
            assert!(matches!(
                store.register_flow(node()).unwrap(),
                crate::store::FlowRegistration::Registered(_)
            ));
            assert_eq!(store.events("5a4d0b").unwrap(), Some(vec![]));
            for event in [Event::Started, Event::ToolUsed("Bash".into()), stop()] {
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
            Some(vec![Event::Started, Event::ToolUsed("Bash".into()), stop()])
        );
        assert_eq!(store.events("0a0a0a").unwrap(), None);
    }

    /// A held flow whose launch was refused is let go: reports for it are
    /// refused again, and the release lasts across a reopen. A registered
    /// flow keeps its events.
    #[test]
    fn a_released_reserved_flow_is_no_longer_held() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("flow.memory");
        {
            let store = FlowStore::open(&path).unwrap();
            store.hold_reserved_flow("0b0b0b").unwrap();
            assert_eq!(store.events("0b0b0b").unwrap(), Some(vec![]));
            store.release_reserved_flow("0b0b0b").unwrap();
            assert_eq!(store.events("0b0b0b").unwrap(), None);
            assert_eq!(
                store.record_event("0b0b0b", Event::Started).unwrap(),
                EventRecording::UnknownFlow
            );
            store.release_reserved_flow("0b0b0b").unwrap();
            assert!(matches!(
                store.register_flow(node()).unwrap(),
                crate::store::FlowRegistration::Registered(_)
            ));
            store.record_event("5a4d0b", Event::Started).unwrap();
            store.release_reserved_flow("5a4d0b").unwrap();
        }
        let store = FlowStore::open(&path).unwrap();
        assert_eq!(store.events("0b0b0b").unwrap(), None);
        assert_eq!(store.events("5a4d0b").unwrap(), Some(vec![Event::Started]));
    }
}
