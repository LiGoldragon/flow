//! Durable Flow Nexus identity and dispatch state.
//!
//! The ordinary Signal contract remains the public boundary.  This module
//! owns its single `.sema` store and lowers a closed `signal_flow::Query`
//! into its typed, durable records.

use std::path::Path;

use meta_signal_flow::Configuration;
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use sema_engine::{
    Assertion, Engine, EngineOpen, EngineRecord, FamilyName, KeyedMutation, QueryPlan, RecordKey,
    SchemaHash, SchemaVersion, TableDescriptor, TableName, TableReference,
};
use signal_flow::{Origin, Query, Response};

const FLOW_TABLE_NAME: TableName = TableName::new("flow_nexus_flows");
const FLOW_STATE_TABLE_NAME: TableName = TableName::new("flow_nexus_state");
const FLOW_CONFIGURATION_TABLE_NAME: TableName = TableName::new("flow_nexus_configuration");
const STATE_KEY: &str = "identity";
const CONFIGURATION_KEY: &str = "configured";
const DEFAULT_ORDINARY_SOCKET: &str = "/tmp/flow-nexus.sock";
const DEFAULT_META_SOCKET: &str = "/tmp/flow-nexus-meta.sock";

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
struct FlowRecord {
    flow_id: String,
    flow_type: String,
    goal: String,
    owner_flow_id: String,
    origin: Origin,
    thread_id: String,
    generation: u64,
}

impl EngineRecord for FlowRecord {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.flow_id.clone())
    }
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
#[rkyv(derive(Debug))]
struct FlowStoreState {
    next_flow_number: u64,
}

impl EngineRecord for FlowStoreState {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(STATE_KEY)
    }
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
struct FlowStoreConfiguration {
    configuration: Configuration,
}

impl EngineRecord for FlowStoreConfiguration {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(CONFIGURATION_KEY)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("sema-engine operation failed: {0}")]
    Engine(#[from] sema_engine::Error),
    #[error("flow nexus state record is absent or duplicated")]
    StateInvariant,
}

pub struct FlowStore {
    engine: Engine,
    flows: TableReference<FlowRecord>,
    state: TableReference<FlowStoreState>,
    configuration: TableReference<FlowStoreConfiguration>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestartAuthorization {
    pub flow_id: String,
    pub authority_flow_id: String,
    pub thread_id: String,
}

/// The constructor is a trait operation so the Nexus's storage boundary is
/// part of its ontology rather than an unclassified helper.
pub trait OpensFlowStore {
    fn open(path: &Path) -> Result<Self, StoreError>
    where
        Self: Sized;
}

/// The only ordinary-socket operation that reaches durable Flow state.
pub trait AppliesFlowQuery {
    fn apply(&self, query: Query) -> Result<Response, StoreError>;
}

/// Records a start only after the adapter has obtained the daemon-owned thread ID.
pub trait RecordsStartedFlow {
    fn record_started(&self, query: Query, thread_id: String) -> Result<Response, StoreError>;
}

/// Reads the persisted daemon thread only after checking restart authority.
pub trait AuthorizesFlowRestart {
    fn authorize_restart(
        &self,
        flow_id: &str,
        authority_flow_id: &str,
    ) -> Result<Option<RestartAuthorization>, StoreError>;
}

/// Advances the generation after the adapter has accepted the resume turn.
pub trait RecordsRestartedFlow {
    fn record_restarted(&self, authorization: RestartAuthorization)
    -> Result<Response, StoreError>;
}

/// Configuration is durable policy and shares the Nexus's sole `.sema` store.
pub trait ConfiguresFlowStore {
    fn configuration(&self) -> Result<Configuration, StoreError>;
    fn configure(&self, configuration: Configuration) -> Result<(), StoreError>;
}

trait ReadsFlowStore {
    fn state(&self) -> Result<FlowStoreState, StoreError>;
    fn flow(&self, flow_id: &str) -> Result<Option<FlowRecord>, StoreError>;
    fn stored_configuration(&self) -> Result<FlowStoreConfiguration, StoreError>;
}

trait WritesFlowStore {
    fn record_start(
        &self,
        flow_type: String,
        goal: String,
        origin: Origin,
        thread_id: String,
    ) -> Result<Response, StoreError>;
    fn restart(&self, authorization: RestartAuthorization) -> Result<Response, StoreError>;
}

impl OpensFlowStore for FlowStore {
    fn open(path: &Path) -> Result<Self, StoreError> {
        let mut engine = Engine::open(EngineOpen::new(path, SchemaVersion::new(1)))?;
        let flows = engine.register_table(TableDescriptor::new(
            FLOW_TABLE_NAME,
            FamilyName::new("flow-nexus-flow"),
            SchemaHash::for_label("flow-nexus-flow-v1"),
        ))?;
        let state = engine.register_table(TableDescriptor::new(
            FLOW_STATE_TABLE_NAME,
            FamilyName::new("flow-nexus-state"),
            SchemaHash::for_label("flow-nexus-state-v1"),
        ))?;
        let configuration = engine.register_table(TableDescriptor::new(
            FLOW_CONFIGURATION_TABLE_NAME,
            FamilyName::new("flow-nexus-configuration"),
            SchemaHash::for_label("flow-nexus-configuration-v1"),
        ))?;
        let store = Self {
            engine,
            flows,
            state,
            configuration,
        };
        if store
            .engine
            .match_records(QueryPlan::key(store.state, RecordKey::new(STATE_KEY)))?
            .records()
            .is_empty()
        {
            store.engine.assert(Assertion::new(
                store.state,
                FlowStoreState {
                    next_flow_number: 1,
                },
            ))?;
        }
        if store
            .engine
            .match_records(QueryPlan::key(
                store.configuration,
                RecordKey::new(CONFIGURATION_KEY),
            ))?
            .records()
            .is_empty()
        {
            store.engine.assert(Assertion::new(
                store.configuration,
                FlowStoreConfiguration {
                    configuration: Configuration {
                        ordinary_socket: DEFAULT_ORDINARY_SOCKET.into(),
                        meta_socket: DEFAULT_META_SOCKET.into(),
                    },
                },
            ))?;
        }
        Ok(store)
    }
}

impl AppliesFlowQuery for FlowStore {
    fn apply(&self, query: Query) -> Result<Response, StoreError> {
        match query {
            Query::Start { .. } => Ok(Response::StartRejected),
            Query::Restart { .. } => Ok(Response::RestartRejected),
        }
    }
}

impl RecordsStartedFlow for FlowStore {
    fn record_started(&self, query: Query, thread_id: String) -> Result<Response, StoreError> {
        match query {
            Query::Start {
                flow_type,
                goal,
                origin,
            } if flow_type == "codex-medium" => {
                self.record_start(flow_type, goal, origin, thread_id)
            }
            Query::Start { .. } | Query::Restart { .. } => Ok(Response::StartRejected),
        }
    }
}

impl AuthorizesFlowRestart for FlowStore {
    fn authorize_restart(
        &self,
        flow_id: &str,
        authority_flow_id: &str,
    ) -> Result<Option<RestartAuthorization>, StoreError> {
        let Some(flow) = self.flow(flow_id)? else {
            return Ok(None);
        };
        if flow.owner_flow_id != authority_flow_id {
            return Ok(None);
        }
        Ok(Some(RestartAuthorization {
            flow_id: flow.flow_id,
            authority_flow_id: authority_flow_id.into(),
            thread_id: flow.thread_id,
        }))
    }
}

impl RecordsRestartedFlow for FlowStore {
    fn record_restarted(
        &self,
        authorization: RestartAuthorization,
    ) -> Result<Response, StoreError> {
        self.restart(authorization)
    }
}

impl ConfiguresFlowStore for FlowStore {
    fn configuration(&self) -> Result<Configuration, StoreError> {
        Ok(self.stored_configuration()?.configuration)
    }

    fn configure(&self, configuration: Configuration) -> Result<(), StoreError> {
        self.engine.mutate_keyed(KeyedMutation::new(
            self.configuration,
            RecordKey::new(CONFIGURATION_KEY),
            FlowStoreConfiguration { configuration },
        ))?;
        Ok(())
    }
}

impl ReadsFlowStore for FlowStore {
    fn state(&self) -> Result<FlowStoreState, StoreError> {
        let records = self
            .engine
            .match_records(QueryPlan::key(self.state, RecordKey::new(STATE_KEY)))?
            .records()
            .to_vec();
        match records.as_slice() {
            [state] => Ok(state.clone()),
            _ => Err(StoreError::StateInvariant),
        }
    }

    fn flow(&self, flow_id: &str) -> Result<Option<FlowRecord>, StoreError> {
        let records = self
            .engine
            .match_records(QueryPlan::key(self.flows, RecordKey::new(flow_id)))?
            .records()
            .to_vec();
        match records.as_slice() {
            [] => Ok(None),
            [flow] => Ok(Some(flow.clone())),
            _ => Err(StoreError::StateInvariant),
        }
    }

    fn stored_configuration(&self) -> Result<FlowStoreConfiguration, StoreError> {
        let records = self
            .engine
            .match_records(QueryPlan::key(
                self.configuration,
                RecordKey::new(CONFIGURATION_KEY),
            ))?
            .records()
            .to_vec();
        match records.as_slice() {
            [configuration] => Ok(configuration.clone()),
            _ => Err(StoreError::StateInvariant),
        }
    }
}

impl WritesFlowStore for FlowStore {
    fn record_start(
        &self,
        flow_type: String,
        goal: String,
        origin: Origin,
        thread_id: String,
    ) -> Result<Response, StoreError> {
        let state = self.state()?;
        let flow_id = format!("flow-{:016x}", state.next_flow_number);
        self.engine.commit_atomic(
            self.engine
                .begin_atomic_commit()
                .assert(
                    self.flows,
                    FlowRecord {
                        flow_id: flow_id.clone(),
                        flow_type,
                        goal,
                        owner_flow_id: origin.parent_flow_id.clone(),
                        origin: origin.clone(),
                        thread_id,
                        generation: 1,
                    },
                )
                .mutate(
                    self.state,
                    FlowStoreState {
                        next_flow_number: state.next_flow_number + 1,
                    },
                ),
        )?;
        Ok(Response::Started { flow_id, origin })
    }

    fn restart(&self, authorization: RestartAuthorization) -> Result<Response, StoreError> {
        let Some(mut flow) = self.flow(&authorization.flow_id)? else {
            return Ok(Response::RestartRejected);
        };
        if flow.owner_flow_id != authorization.authority_flow_id
            || flow.thread_id != authorization.thread_id
        {
            return Ok(Response::RestartRejected);
        }
        flow.generation += 1;
        let generation = flow.generation;
        self.engine.mutate_keyed(KeyedMutation::new(
            self.flows,
            RecordKey::new(authorization.flow_id.clone()),
            flow,
        ))?;
        Ok(Response::Restarted {
            flow_id: authorization.flow_id,
            generation,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{
        AuthorizesFlowRestart, ConfiguresFlowStore, FlowStore, OpensFlowStore,
        RecordsRestartedFlow, RecordsStartedFlow,
    };
    use meta_signal_flow::Configuration;
    use signal_flow::{Origin, Query, Response};

    struct StoreFixture {
        directory: tempfile::TempDir,
    }

    trait OpensFixtureStore {
        fn store(&self) -> FlowStore;
    }

    impl StoreFixture {
        fn new() -> Self {
            Self {
                directory: tempfile::tempdir().expect("temporary store directory"),
            }
        }
    }

    impl OpensFixtureStore for StoreFixture {
        fn store(&self) -> FlowStore {
            <FlowStore as OpensFlowStore>::open(&self.directory.path().join("flow.sema"))
                .expect("store opens")
        }
    }

    trait StartsFixtureFlow {
        fn start(&self, store: &FlowStore) -> String;
    }

    impl StartsFixtureFlow for StoreFixture {
        fn start(&self, store: &FlowStore) -> String {
            let response = store
                .record_started(
                    Query::Start {
                        flow_type: "codex-medium".into(),
                        goal: "persist this origin".into(),
                        origin: Origin {
                            parent_flow_id: "9fc62b".into(),
                            session: "session-1".into(),
                            turn: "turn-7".into(),
                        },
                    },
                    "thread-1".into(),
                )
                .expect("start persists");
            let Response::Started { flow_id, .. } = response else {
                panic!("start must be accepted")
            };
            flow_id
        }
    }

    #[test]
    fn reopen_recovers_origin_and_matching_authority_restarts() {
        let fixture = StoreFixture::new();
        let flow_id = fixture.start(&fixture.store());
        let reopened = fixture.store();
        assert_eq!(
            reopened
                .record_restarted(
                    reopened
                        .authorize_restart(&flow_id, "9fc62b")
                        .expect("authorization reads")
                        .expect("owner is authorized"),
                )
                .expect("restart persists"),
            Response::Restarted {
                flow_id,
                generation: 2,
            }
        );
    }

    #[test]
    fn mismatching_or_unknown_authority_is_rejected() {
        let fixture = StoreFixture::new();
        let store = fixture.store();
        let flow_id = fixture.start(&store);
        assert_eq!(
            store
                .authorize_restart(&flow_id, "another-flow")
                .expect("authorization evaluates"),
            None
        );
        assert_eq!(
            store
                .authorize_restart("flow-unknown", "9fc62b")
                .expect("unknown flow evaluates"),
            None
        );
    }

    #[test]
    fn configured_policy_is_recovered_from_the_same_store() {
        let fixture = StoreFixture::new();
        let store = fixture.store();
        let configuration = Configuration {
            ordinary_socket: "/tmp/ordinary-test.sock".into(),
            meta_socket: "/tmp/meta-test.sock".into(),
        };
        store
            .configure(configuration.clone())
            .expect("policy persists");
        drop(store);
        assert_eq!(
            fixture.store().configuration().expect("policy recovers"),
            configuration
        );
    }
}
