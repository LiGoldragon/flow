//! The exact events-v5 layout written through 0.54.5, upgraded atomically.
//! The six tags, String row key and four Stop fields are frozen here.
//! Leaves come from the unchanged signal60 contract pinned by this release.
use super::events::{DescribesEventTable, FlowEvents, RecordedHarnessEvent, RecordedStop};
use super::{FlowStore, StoreError};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use sema_engine::{
    Decision, Engine, EngineRecord, MemoryDefinition, MemoryUpgrades, MemoryVersion, RecordKey,
    SchemaHash, UpgradeFrom, UpgradeOutcome,
};
use signal_flow::{
    BackgroundWork, FlowId, LastAssistantMessageOption, NativePrompt, NativeSessionId,
    TranscriptPath,
};

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct StopV5 {
    pub(crate) native_session_id_option: Option<NativeSessionId>,
    pub(crate) transcript_path_option: Option<TranscriptPath>,
    pub(crate) last_assistant_message_option: LastAssistantMessageOption,
    pub(crate) background_work: BackgroundWork,
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) enum EventV5 {
    Started(Option<NativeSessionId>),
    ToolUsed(String),
    Stopped(StopV5),
    PromptSubmitted(NativePrompt),
    Ended(Option<NativeSessionId>),
    Cleared(Option<NativeSessionId>),
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct FlowEventsV5 {
    pub(crate) flow_id: String,
    pub(crate) event_vector: Vec<EventV5>,
}

impl EngineRecord for FlowEventsV5 {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.flow_id.clone())
    }
}
impl MemoryDefinition for FlowEventsV5 {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(5)
    }
    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label("flow-nexus-flow-events-v5")
    }
}

/// An exact historical event loses its body on the way into current Memory.
trait RecordsHistoricalEvent {
    fn recorded(self) -> RecordedHarnessEvent;
}

/// The startup boundary runs before event registration or serving.
pub(crate) trait UpgradesEventMemory {
    fn upgrade_event_memory(engine: &mut Engine) -> Result<(), StoreError>;
}

impl RecordsHistoricalEvent for EventV5 {
    fn recorded(self) -> RecordedHarnessEvent {
        match self {
            Self::Started(session) => RecordedHarnessEvent::Started(session),
            Self::ToolUsed(tool) => RecordedHarnessEvent::ToolUsed(tool),
            Self::Stopped(stop) => RecordedHarnessEvent::Stopped(RecordedStop {
                native_session_id_option: stop.native_session_id_option,
                transcript_path_option: stop.transcript_path_option,
                background_work: stop.background_work,
            }),
            Self::PromptSubmitted(prompt) => RecordedHarnessEvent::PromptSubmitted(prompt),
            Self::Ended(session) => RecordedHarnessEvent::Ended(session),
            Self::Cleared(session) => RecordedHarnessEvent::Cleared(session),
        }
    }
}

impl UpgradeFrom<FlowEventsV5> for FlowEvents {
    type Refusal = std::convert::Infallible;
    fn upgrade_from(before: FlowEventsV5) -> Decision<Self, Self::Refusal> {
        Decision::Ready(Self {
            flow_id: FlowId(before.flow_id),
            recorded_harness_event_vector: before
                .event_vector
                .into_iter()
                .map(RecordsHistoricalEvent::recorded)
                .collect(),
        })
    }
}

impl UpgradesEventMemory for FlowStore {
    fn upgrade_event_memory(engine: &mut Engine) -> Result<(), StoreError> {
        match engine.upgrade_table::<FlowEventsV5, FlowEvents>(FlowEvents::descriptor()) {
            UpgradeOutcome::Upgraded(_) | UpgradeOutcome::AlreadyCurrent(_) => Ok(()),
            UpgradeOutcome::Refused(refusal) => {
                Err(StoreError::EventUpgrade(format!("{refusal:?}")))
            }
            UpgradeOutcome::Failed(failure) => {
                Err(StoreError::EventUpgrade(format!("{failure:?}")))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::events::{FLOW_EVENT_FAMILY, FLOW_EVENT_TABLE_NAME};
    use super::*;
    use sema_engine::{
        Assertion, EngineOpen, FamilyName, QueryPlan, SchemaVersion, TableDescriptor,
    };

    // This independent fixture is precisely the source layout published in
    // 886eefa8, using its unchanged pinned signal60 Event for archive evidence.
    #[derive(Archive, RkyvSerialize, RkyvDeserialize)]
    struct PublishedFlowEventsV5 {
        flow_id: String,
        event_vector: Vec<signal_flow::Event>,
    }

    #[test]
    fn frozen_v5_decodes_the_published_layout_and_preserves_every_tag() {
        use signal_flow::{Event, NativeStop};
        let published = PublishedFlowEventsV5 {
            flow_id: "MiXeD-original-key".into(),
            event_vector: vec![
                Event::Started(None),
                Event::ToolUsed("Bash".into()),
                Event::Stopped(NativeStop {
                    native_session_id_option: Some(NativeSessionId("session".into())),
                    transcript_path_option: Some(TranscriptPath("/t/session".into())),
                    last_assistant_message_option: LastAssistantMessageOption(Some(
                        "body-marker".into(),
                    )),
                    background_work: BackgroundWork(vec![]),
                }),
                Event::PromptSubmitted(NativePrompt {
                    native_session_id_option: None,
                    prompt_id_option: Some(signal_flow::PromptId("p1".into())),
                    prompt_sha256: signal_flow::PromptSha256("digest".into()),
                }),
                Event::Ended(None),
                Event::Cleared(None),
            ],
        };
        let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&published).unwrap();
        let frozen = rkyv::from_bytes::<FlowEventsV5, rkyv::rancor::Error>(&bytes).unwrap();
        assert_eq!(frozen.flow_id, published.flow_id);
        assert_eq!(frozen.event_vector.len(), 6);
        assert!(matches!(&frozen.event_vector[2], EventV5::Stopped(stop)
            if stop.last_assistant_message_option.0.as_deref() == Some("body-marker")));
        let Decision::Ready(current) = FlowEvents::upgrade_from(frozen);
        assert_eq!(current.flow_id.0, published.flow_id);
        use super::super::events::ProjectsRecordedHarnessEvent;
        assert_eq!(
            current.recorded_harness_event_vector,
            published
                .event_vector
                .iter()
                .map(ProjectsRecordedHarnessEvent::recorded)
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn v5_upgrade_preserves_keys_metadata_order_and_reopens_current_idempotently() {
        use signal_flow::{
            BackgroundTask, BackgroundTaskId, BackgroundTaskKind, BackgroundTaskStatus, Description,
        };
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("events.sema");
        let work = BackgroundWork(vec![BackgroundTask {
            background_task_id: BackgroundTaskId("job".into()),
            background_task_kind: BackgroundTaskKind::Shell,
            background_task_status: BackgroundTaskStatus("running".into()),
            description: Description("build metadata".into()),
        }]);
        let before = FlowEventsV5 {
            flow_id: "MiXeD-original-key".into(),
            event_vector: vec![
                EventV5::ToolUsed("Bash".into()),
                EventV5::Stopped(StopV5 {
                    native_session_id_option: Some(NativeSessionId("session".into())),
                    transcript_path_option: Some(TranscriptPath("/t/session".into())),
                    last_assistant_message_option: LastAssistantMessageOption(Some(
                        "removed-body-marker".repeat(2048),
                    )),
                    background_work: work.clone(),
                }),
                EventV5::Ended(None),
            ],
        };
        let empty = FlowEventsV5 {
            flow_id: "empty".into(),
            event_vector: vec![],
        };
        {
            let mut engine = Engine::open(EngineOpen::new(&path, SchemaVersion::new(1))).unwrap();
            let prior = engine
                .register_table(TableDescriptor::<FlowEventsV5>::new(
                    FLOW_EVENT_TABLE_NAME,
                    FamilyName::new(FLOW_EVENT_FAMILY),
                    FlowEventsV5::schema_hash(),
                ))
                .unwrap();
            engine
                .assert(Assertion::new(prior, before.clone()))
                .unwrap();
            engine.assert(Assertion::new(prior, empty)).unwrap();
        }
        for _ in 0..2 {
            let descriptor = FlowEvents::descriptor();
            let mut engine = Engine::open(
                EngineOpen::new(&path, SchemaVersion::new(1)).with_family(&descriptor),
            )
            .unwrap();
            FlowStore::upgrade_event_memory(&mut engine).unwrap();
            let table = engine.register_table(descriptor).unwrap();
            let rows = engine.match_records(QueryPlan::all(table)).unwrap();
            assert_eq!(rows.records().len(), 2);
            let row = rows
                .records()
                .iter()
                .find(|row| row.flow_id.0 == before.flow_id)
                .unwrap();
            assert_eq!(row.record_key(), before.record_key());
            assert_eq!(
                row.recorded_harness_event_vector,
                vec![
                    RecordedHarnessEvent::ToolUsed("Bash".into()),
                    RecordedHarnessEvent::Stopped(RecordedStop {
                        native_session_id_option: Some(NativeSessionId("session".into())),
                        transcript_path_option: Some(TranscriptPath("/t/session".into())),
                        background_work: work.clone(),
                    }),
                    RecordedHarnessEvent::Ended(None),
                ]
            );
            let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(row).unwrap();
            assert!(
                !bytes
                    .windows(b"removed-body-marker".len())
                    .any(|part| part == b"removed-body-marker")
            );
            assert!(
                rows.records()
                    .iter()
                    .find(|row| row.flow_id.0 == "empty")
                    .unwrap()
                    .recorded_harness_event_vector
                    .is_empty()
            );
        }
    }
}

#[cfg(test)]
mod admission_tests {
    use super::*;
    use sema_engine::{Assertion, EngineOpen, FamilyName, SchemaVersion, TableDescriptor};

    #[derive(Archive, RkyvSerialize, RkyvDeserialize, Clone)]
    struct UnsupportedEvents {
        flow_id: String,
    }
    impl EngineRecord for UnsupportedEvents {
        fn record_key(&self) -> RecordKey {
            RecordKey::new(self.flow_id.clone())
        }
    }
    impl MemoryDefinition for UnsupportedEvents {
        fn memory_version() -> MemoryVersion {
            MemoryVersion::new(5)
        }
        fn schema_hash() -> SchemaHash {
            SchemaHash::for_label("unsupported-event-layout")
        }
    }

    #[test]
    fn unsupported_event_identity_is_refused_before_open_changes_any_bytes() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("unsupported.sema");
        {
            let mut engine = Engine::open(EngineOpen::new(&path, SchemaVersion::new(1))).unwrap();
            let table = engine
                .register_table(TableDescriptor::<UnsupportedEvents>::new(
                    super::super::events::FLOW_EVENT_TABLE_NAME,
                    FamilyName::new(super::super::events::FLOW_EVENT_FAMILY),
                    UnsupportedEvents::schema_hash(),
                ))
                .unwrap();
            engine
                .assert(Assertion::new(
                    table,
                    UnsupportedEvents {
                        flow_id: "untouched".into(),
                    },
                ))
                .unwrap();
        }
        let before = std::fs::read(&path).unwrap();
        let descriptor = FlowEvents::descriptor();
        assert!(
            Engine::open(EngineOpen::new(&path, SchemaVersion::new(1)).with_family(&descriptor))
                .is_err()
        );
        assert_eq!(std::fs::read(&path).unwrap(), before);
    }

    #[test]
    fn new_empty_current_event_memory_is_idempotent() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("new.sema");
        for _ in 0..2 {
            let descriptor = FlowEvents::descriptor();
            let mut engine = Engine::open(
                EngineOpen::new(&path, SchemaVersion::new(1)).with_family(&descriptor),
            )
            .unwrap();
            FlowStore::upgrade_event_memory(&mut engine).unwrap();
            let table = engine.register_table(descriptor).unwrap();
            assert!(
                engine
                    .match_records(sema_engine::QueryPlan::all(table))
                    .unwrap()
                    .records()
                    .is_empty()
            );
        }
    }
}
