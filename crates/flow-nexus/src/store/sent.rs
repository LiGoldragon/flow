//! A request a flow sent through the ordinary Send, queued for its
//! recipient under the rules a Lock and a Deliver from Message follow: a
//! recipient Message holds an unexpired lock on is Held, and the request is
//! queued with the sender Flow read from the calling process.

use super::{FlowStore, StoreError};
use sema_engine::QueryPlan;

use std::{collections::BTreeSet, fs, path::PathBuf};

pub(crate) trait NamesCodexAppServers {
    /// The Codex app-server executables the SendServers family names,
    /// canonical: the processes whose commands name their thread.
    fn codex_app_servers(&self) -> Result<BTreeSet<PathBuf>, StoreError>;
}

impl NamesCodexAppServers for FlowStore {
    fn codex_app_servers(&self) -> Result<BTreeSet<PathBuf>, StoreError> {
        Ok(self
            .engine
            .match_records(QueryPlan::all(self.meta_configuration_tables.send_servers))?
            .records()
            .iter()
            .filter_map(|stored| fs::canonicalize(&stored.send_server.codex_app_server_binary).ok())
            .collect())
    }
}
