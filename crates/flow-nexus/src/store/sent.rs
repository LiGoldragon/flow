//! A request a flow sent through the ordinary Send, queued for its
//! recipient under the rules a Lock and a Deliver from Message follow: a
//! recipient Message holds an unexpired lock on is Held, and the request is
//! queued with the sender Flow read from the calling process.

use super::{FlowStore, ManagesMetaflowState, StoreError};
use sema_engine::QueryPlan;
use signal_flow::{Address, QueuedRequest, Refusal, Request, Response, SentBy, Unknown_Data};
use std::{collections::BTreeSet, fs, path::PathBuf};

pub(crate) trait QueuesSentRequests {
    /// Queues `request` from `sender` for the metaflow at `recipient`:
    /// Queued, or Refused as Unknown when no metaflow is there and Held
    /// while Message holds it.
    fn queue_sent(
        &self,
        sender: &Address,
        recipient: &Address,
        request: Request,
        now: i64,
    ) -> Result<Response, StoreError>;
}

impl QueuesSentRequests for FlowStore {
    fn queue_sent(
        &self,
        sender: &Address,
        recipient: &Address,
        request: Request,
        now: i64,
    ) -> Result<Response, StoreError> {
        let Some(mut stored) = self.stored_metaflow(recipient)? else {
            return Ok(Response::Refused(Refusal::Unknown(Unknown_Data::Address(
                recipient.clone(),
            ))));
        };
        if let Some(held) = self.stored_metaflow_lock(recipient)?
            && held.lock.until > now
        {
            return Ok(Response::Refused(Refusal::Held(held.lock)));
        }
        stored.metaflow.queue.push(QueuedRequest {
            sent_by: SentBy::Sender(sender.clone()),
            request,
        });
        let commit = self
            .engine
            .begin_atomic_commit()
            .mutate(self.metaflows, stored);
        self.engine.commit_atomic(commit)?;
        Ok(Response::Queued)
    }
}

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
