//! The send servers Flow trusts to name the Codex thread they serve: the meta
//! SendServers family, one row per Codex app-server executable, each paired
//! with the MCP send-server executable that app-server's child must run. A
//! store written before send servers were configured opens with the table
//! added and holds none.
//!
//! A process is a trusted send server when the executable the kernel reports
//! for it is a configured MCP send-server executable and the executable the
//! kernel reports for its parent is the Codex app-server executable paired
//! with it. Only the kernel's account of the two processes is read: never
//! their environment, and never a claim the request carries.

use super::metaflow::SleepsMetaflows;
use super::{FlowStore, NamesLiveFlow, ReadsFlowMemory, ReadsFlowRows, StoreError};
use meta_signal_flow::ConfigureSendServer;
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use memory_engine::{
    EngineRecord, FamilyName, QueryPlan, RecordKey, SchemaHash, TableDescriptor, TableName,
};
use signal_flow::{HarnessKind, Metaflow, Process};
use std::fs;

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
        RecordKey::new(self.send_server.codex_app_server_binary.clone())
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

/// What the configured send servers make of one process.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SendServerTrust {
    /// No send server is configured.
    Unconfigured,
    /// The process, or its parent, is not a configured pair's.
    Untrusted,
    /// The process runs a configured send server under its paired app-server.
    Trusted,
}

pub(crate) trait TrustsSendServers {
    /// Reads the process and its parent from the kernel and matches their
    /// executables against the configured pairs. The process must still run
    /// with the start it was named by, with the same parent, after its
    /// parent's executable is read.
    fn send_server_trust(&self, process: &Process) -> Result<SendServerTrust, StoreError>;
}

impl TrustsSendServers for FlowStore {
    fn send_server_trust(&self, process: &Process) -> Result<SendServerTrust, StoreError> {
        let send_servers = self
            .engine
            .match_records(QueryPlan::all(self.meta_configuration_tables.send_servers))?
            .records()
            .iter()
            .map(|stored| stored.send_server.clone())
            .collect::<Vec<_>>();
        if send_servers.is_empty() {
            return Ok(SendServerTrust::Unconfigured);
        }
        let Some((parent_pid, started)) = FlowStore::process_start(process.pid) else {
            return Ok(SendServerTrust::Untrusted);
        };
        if started != process.started {
            return Ok(SendServerTrust::Untrusted);
        }
        let Some(executable) = FlowStore::process_executable(process) else {
            return Ok(SendServerTrust::Untrusted);
        };
        let Some(parent) = u32::try_from(parent_pid)
            .ok()
            .and_then(FlowStore::process_identity_for_pid)
        else {
            return Ok(SendServerTrust::Untrusted);
        };
        let Some(parent_executable) = FlowStore::process_executable(&parent) else {
            return Ok(SendServerTrust::Untrusted);
        };
        // Still the same process under the same parent once both are read.
        if FlowStore::process_start(process.pid) != Some((parent_pid, started)) {
            return Ok(SendServerTrust::Untrusted);
        }
        let trusted = send_servers.iter().any(|send_server| {
            fs::canonicalize(&send_server.mcp_send_server_binary)
                .ok()
                .as_ref()
                == Some(&executable)
                && fs::canonicalize(&send_server.codex_app_server_binary)
                    .ok()
                    .as_ref()
                    == Some(&parent_executable)
        });
        Ok(if trusted {
            SendServerTrust::Trusted
        } else {
            SendServerTrust::Untrusted
        })
    }
}

pub(crate) trait NamesThreadMetaflow {
    /// The metaflow Awake as the one live Codex flow whose thread Flow
    /// recorded at its Start as `thread_id`; None for an empty or unknown
    /// thread, a thread whose flow is no longer live, or one whose metaflow
    /// is not Awake as that flow.
    fn thread_metaflow(&self, thread_id: &str) -> Result<Option<Metaflow>, StoreError>;
}

impl NamesThreadMetaflow for FlowStore {
    fn thread_metaflow(&self, thread_id: &str) -> Result<Option<Metaflow>, StoreError> {
        if thread_id.is_empty() {
            return Ok(None);
        }
        let live = self
            .flow_nodes()?
            .into_iter()
            .filter(|node| {
                node.harness_kind == HarnessKind::Codex
                    && node.session_id == thread_id
                    && node.flow_lifecycle.is_live()
            })
            .collect::<Vec<_>>();
        match live.as_slice() {
            [] => Ok(None),
            [flow] => self.metaflow_awake_as(&flow.flow_id),
            _ => Err(StoreError::StateInvariant),
        }
    }
}
