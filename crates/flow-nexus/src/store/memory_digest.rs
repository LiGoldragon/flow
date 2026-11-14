//! The digest of every record a store holds, each re-archived by the record
//! type this build declares: one line per record, its table, its key and the
//! SHA-256 of its archive. Two builds that print the same digest over copies
//! of one store read every stored row the same way and write it back to the
//! same bytes, so a change of a record's declaration that leaves the digest
//! as it was leaves the archive as it was.
//!
//! The digest is taken by an ignored test over a copy the caller names in
//! `FLOW_MEMORY_COPY`, written to `FLOW_MEMORY_DIGEST`; it never touches a
//! store a Nexus holds.

use super::{FlowStore, OpensFlowStore, StoreError};
use rkyv::api::high::HighDeserializer;
use rkyv::bytecheck::CheckBytes;
use rkyv::rancor::{self, Strategy};
use rkyv::validation::Validator;
use rkyv::validation::archive::ArchiveValidator;
use rkyv::validation::shared::SharedValidator;
use sema_engine::{EngineRecord, EngineStoredValue, QueryPlan, TableReference};
use sha2::{Digest, Sha256};

/// The lines one table contributes: `table key digest`, in key order.
pub(crate) trait DigestsTable {
    fn digest_table<RecordValue>(
        &self,
        name: &str,
        table: TableReference<RecordValue>,
        lines: &mut Vec<String>,
    ) -> Result<(), StoreError>
    where
        RecordValue: EngineStoredValue + EngineRecord,
        <RecordValue as rkyv::Archive>::Archived: rkyv::Deserialize<RecordValue, HighDeserializer<rancor::Error>>
            + for<'validation> CheckBytes<
                Strategy<Validator<ArchiveValidator<'validation>, SharedValidator>, rancor::Error>,
            >;
}

impl DigestsTable for FlowStore {
    fn digest_table<RecordValue>(
        &self,
        name: &str,
        table: TableReference<RecordValue>,
        lines: &mut Vec<String>,
    ) -> Result<(), StoreError>
    where
        RecordValue: EngineStoredValue + EngineRecord,
        <RecordValue as rkyv::Archive>::Archived: rkyv::Deserialize<RecordValue, HighDeserializer<rancor::Error>>
            + for<'validation> CheckBytes<
                Strategy<Validator<ArchiveValidator<'validation>, SharedValidator>, rancor::Error>,
            >,
    {
        let rows = self.engine.match_records(QueryPlan::all(table))?;
        let mut table_lines = Vec::new();
        for record in rows.records() {
            let bytes = rkyv::to_bytes::<rancor::Error>(record).expect("a read record archives");
            let digest = Sha256::digest(bytes.as_slice());
            let hex: String = digest.iter().map(|byte| format!("{byte:02x}")).collect();
            table_lines.push(format!(
                "{name} {:?} {} {hex}",
                record.record_key(),
                bytes.len()
            ));
        }
        table_lines.sort();
        lines.extend(table_lines);
        Ok(())
    }
}

/// Every table the store registers, digested.
pub(crate) trait DigestsMemory {
    fn memory_digest(&self) -> Result<Vec<String>, StoreError>;
}

impl DigestsMemory for FlowStore {
    fn memory_digest(&self) -> Result<Vec<String>, StoreError> {
        let mut lines = Vec::new();
        let lines_ref = &mut lines;
        self.digest_table("flows", self.flows, lines_ref)?;
        self.digest_table("native_owner_proofs", self.native_owner_proofs, lines_ref)?;
        self.digest_table("state", self.state, lines_ref)?;
        self.digest_table("metaflows", self.metaflows, lines_ref)?;
        self.digest_table("metaflow_processes", self.metaflow_processes, lines_ref)?;
        self.digest_table("metaflow_locks", self.metaflow_locks, lines_ref)?;
        self.digest_table("configuration", self.configuration, lines_ref)?;
        self.digest_table(
            "runtime_configuration",
            self.runtime_configuration,
            lines_ref,
        )?;
        self.digest_table("herdr_routes", self.herdr_routes, lines_ref)?;
        self.digest_table("launch_attempts", self.launch_attempts, lines_ref)?;
        self.digest_table("launch_panes", self.launch_panes, lines_ref)?;
        self.digest_table("launch_outcomes", self.launch_outcomes, lines_ref)?;
        self.digest_table("pending_reaps", self.pending_reaps, lines_ref)?;
        self.digest_table("pending_handovers", self.pending_handovers, lines_ref)?;
        self.digest_table("replacements", self.replacements, lines_ref)?;
        self.digest_table("roles", self.roles, lines_ref)?;
        let meta = &self.meta_configuration_tables;
        self.digest_table("meta.handover", meta.handover, lines_ref)?;
        self.digest_table("meta.nexus", meta.nexus, lines_ref)?;
        self.digest_table("meta.models", meta.models, lines_ref)?;
        self.digest_table("meta.thresholds", meta.thresholds, lines_ref)?;
        self.digest_table("meta.leave_outs", meta.leave_outs, lines_ref)?;
        self.digest_table("meta.send_servers", meta.send_servers, lines_ref)?;
        self.digest_table("meta.metaflows", meta.metaflows, lines_ref)?;
        self.digest_table("meta.investigation", meta.investigation, lines_ref)?;
        self.digest_table("meta.default_minutes", meta.default_minutes, lines_ref)?;
        self.digest_table("meta.request_policy", meta.request_policy, lines_ref)?;
        self.digest_table("meta.harness_flags", meta.harness_flags, lines_ref)?;
        self.digest_table("meta.heartbeat", meta.heartbeat, lines_ref)?;
        self.digest_table(
            "meta.protected_addresses",
            meta.protected_addresses,
            lines_ref,
        )?;
        self.digest_table("meta.harness_hook", meta.harness_hook, lines_ref)?;
        let delivery = &self.delivery_tables;
        self.digest_table("deliveries", delivery.deliveries, lines_ref)?;
        self.digest_table("pane_leases", delivery.pane_leases, lines_ref)?;
        self.digest_table("delivery_configuration", delivery.configuration, lines_ref)?;
        self.digest_table("events", self.event_tables.events, lines_ref)?;
        self.digest_table("context_sizes", self.context_sizes, lines_ref)?;
        self.digest_table("address_holds", self.address_holds, lines_ref)?;
        self.digest_table("opened_threads", self.opened_threads, lines_ref)?;
        self.digest_table("send_calls", self.send_calls, lines_ref)?;
        self.digest_table("launch_cleanups", self.launch_cleanups, lines_ref)?;
        self.digest_table("compactions", self.compactions, lines_ref)?;
        self.digest_table("reminders", self.reminders, lines_ref)?;
        self.digest_table("successor_intents", self.successor_intents, lines_ref)?;
        self.digest_table("ordered_replacements", self.ordered_replacements, lines_ref)?;
        self.digest_table("undecodable_frames", self.undecodable_frames, lines_ref)?;
        self.digest_table("turn_ends", self.turn_ends, lines_ref)?;
        self.digest_table("kept_answers", self.kept_answers, lines_ref)?;
        self.digest_table("scheduled_reminders", self.scheduled_reminders, lines_ref)?;
        let queue = &self.queue_delivery;
        self.digest_table("queue.counter", queue.counter, lines_ref)?;
        self.digest_table("queue.requests", queue.requests, lines_ref)?;
        self.digest_table("queue.attempts", queue.attempts, lines_ref)?;
        let outcomes = &self.send_outcomes;
        self.digest_table("send_outcomes.counter", outcomes.counter, lines_ref)?;
        self.digest_table("send_outcomes.outcomes", outcomes.outcomes, lines_ref)?;
        let logical = &self.logical_deliveries;
        self.digest_table("logical.counter", logical.counter, lines_ref)?;
        self.digest_table("logical.current", logical.current, lines_ref)?;
        self.digest_table("logical.events", logical.events, lines_ref)?;
        self.digest_table(
            "logical.simplified_replies",
            logical.simplified_replies,
            lines_ref,
        )?;
        self.digest_table("logical.purposes", logical.purposes, lines_ref)?;
        self.digest_table("logical.request_ids", logical.request_ids, lines_ref)?;
        self.digest_table("logical.deliver_calls", logical.deliver_calls, lines_ref)?;
        self.digest_table("logical.store_identity", logical.store_identity, lines_ref)?;
        let upgrade = &self.request_id_upgrade;
        self.digest_table(
            "upgrade.prior_deliveries",
            upgrade.prior_deliveries,
            lines_ref,
        )?;
        self.digest_table(
            "upgrade.task_announcements",
            upgrade.task_announcements,
            lines_ref,
        )?;
        self.digest_table(
            "upgrade.upgrade_assignments",
            upgrade.upgrade_assignments,
            lines_ref,
        )?;
        self.digest_table("heartbeat_standings", self.heartbeat_standings, lines_ref)?;
        self.digest_table("restarts", self.restarts, lines_ref)?;
        self.digest_table("seat_births", self.seat_births, lines_ref)?;
        self.digest_table("handovers", self.handovers, lines_ref)?;
        self.digest_table("handover_reminders", self.handover_reminders, lines_ref)?;
        Ok(lines)
    }
}

// The checks read a test only inside a `#[cfg(test)]` item.
#[cfg(test)]
mod tests {
    use super::*;

    /// The digest of the copy named in `FLOW_MEMORY_COPY`, written to
    /// `FLOW_MEMORY_DIGEST`.
    #[test]
    #[ignore = "reads a store copy the caller names"]
    fn digest_of_a_store_copy() {
        let copy =
            std::env::var_os("FLOW_MEMORY_COPY").expect("FLOW_MEMORY_COPY names a store copy");
        let output =
            std::env::var_os("FLOW_MEMORY_DIGEST").expect("FLOW_MEMORY_DIGEST names the output");
        let store =
            FlowStore::open_configured(std::path::Path::new(&copy)).expect("the copy opens");
        let lines = store.memory_digest().expect("every table reads");
        std::fs::write(output, lines.join("\n") + "\n").expect("the digest is written");
    }
}
