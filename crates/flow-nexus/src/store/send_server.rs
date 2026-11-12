//! The send servers Flow trusts to name the Codex thread they serve: the meta
//! SendServers family, one row per Codex app-server executable, each paired
//! with the MCP send-server executable that app-server's child must run.
//!
//! A process is a trusted send server when the executable the kernel reports
//! for it is a configured MCP send-server executable and the executable the
//! kernel reports for its parent is the Codex app-server executable paired
//! with it. Only the kernel's account of the two processes is read: never
//! their environment, and never a claim the request carries.

use super::metaflow::SleepsMetaflows;
use super::{FlowStore, NamesLiveFlow, ReadsFlowRows, StoreError};
use meta_signal_flow::ConfigureSendServer;
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use sema_engine::{EngineRecord, FamilyName, RecordKey, SchemaHash, TableDescriptor, TableName};
use signal_flow::{HarnessKind, Permaflow};

pub(super) const META_SEND_SERVER_CONFIGURATION_TABLE_NAME: TableName =
    TableName::new("flow_nexus_meta_send_server_configuration");
pub(super) const META_SEND_SERVER_CONFIGURATION_FAMILY: &str =
    "flow-nexus-meta-send-server-configuration";
pub(super) const META_SEND_SERVER_CONFIGURATION_SCHEMA: &str =
    "flow-nexus-meta-send-server-configuration-v1";

/// One trusted send server, keyed by its Codex app-server executable.
#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct StoredMetaSendServerConfiguration {
    pub send_server: ConfigureSendServer,
}

impl EngineRecord for StoredMetaSendServerConfiguration {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.send_server.codex_app_server_binary.clone().0)
    }
}

pub(super) trait DescribesSendServerTable {
    fn descriptor() -> TableDescriptor<StoredMetaSendServerConfiguration>;
}

impl DescribesSendServerTable for StoredMetaSendServerConfiguration {
    fn descriptor() -> TableDescriptor<StoredMetaSendServerConfiguration> {
        TableDescriptor::new(
            META_SEND_SERVER_CONFIGURATION_TABLE_NAME,
            FamilyName::new(META_SEND_SERVER_CONFIGURATION_FAMILY),
            SchemaHash::for_label(META_SEND_SERVER_CONFIGURATION_SCHEMA),
        )
    }
}

pub(crate) trait NamesThreadMetaflow {
    /// The metaflow Awake as the one live Codex flow whose thread Flow
    /// recorded at its Start as `thread_id`; None for an empty or unknown
    /// thread, a thread whose flow is no longer live, or one whose metaflow
    /// is not Awake as that flow.
    fn thread_metaflow(&self, thread_id: &str) -> Result<Option<Permaflow>, StoreError>;
}

impl NamesThreadMetaflow for FlowStore {
    fn thread_metaflow(&self, thread_id: &str) -> Result<Option<Permaflow>, StoreError> {
        if thread_id.is_empty() {
            return Ok(None);
        }
        let live = self
            .flow_nodes()?
            .into_iter()
            .filter(|node| {
                node.harness_kind == HarnessKind::Codex
                    && node.session_id.0 == thread_id
                    && node.flow_lifecycle.is_live()
            })
            .collect::<Vec<_>>();
        match live.as_slice() {
            [] => Ok(None),
            [flow] => self.metaflow_awake_as(&flow.flow_id.0),
            _ => Err(StoreError::StateInvariant),
        }
    }
}
