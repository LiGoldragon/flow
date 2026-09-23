//! Experimental native-main replacement contract.
//!
//! This module is deliberately not wired into the running Nexus yet.  The
//! privileged dispatcher lives in `lib.rs`, which has another owner.  The
//! integration test includes this module directly so the attestation and
//! durable-operation boundary can be compiled without crossing that lock.

#![allow(dead_code)]

use meta_signal_flow::{BindingRegistrationPhase, BindingRegistrationSubmission};
use serde_json::Value;
use signal_flow::{EndpointSelection, HarnessKind, HerdrRouteSelection};
use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
};

use crate::store::{DeliveryBinding, VerifiedBindingRegistration};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OperationId(pub String);

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplacementPhase {
    Requested,
    AdmissionHeld,
    Quiescent,
    UnknownBlocking,
    SuccessorPreparing,
    ReadinessVerified,
    ContinuityAccepted,
    BindingCommitted,
    AncestorRetirementPending,
    Completed,
    Held,
    Failed,
    ReconcileRequired,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DurableReplaceOperation {
    pub operation_id: OperationId,
    pub lifecycle_epoch: u64,
    pub expected_binding_generation: u64,
    pub phase: ReplacementPhase,
}

impl DurableReplaceOperation {
    pub fn hold_admission(&mut self) -> Result<(), ReplacementTransitionError> {
        if self.phase != ReplacementPhase::Requested {
            return Err(ReplacementTransitionError::OutOfOrder);
        }
        self.phase = ReplacementPhase::AdmissionHeld;
        Ok(())
    }

    pub fn record_readiness(&mut self) -> Result<(), ReplacementTransitionError> {
        if self.phase != ReplacementPhase::SuccessorPreparing {
            return Err(ReplacementTransitionError::OutOfOrder);
        }
        self.phase = ReplacementPhase::ReadinessVerified;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplacementTransitionError {
    OutOfOrder,
}

#[derive(Debug, Clone)]
pub struct NativeAttestationPolicy {
    pub receipt_path: PathBuf,
    pub claim_root: PathBuf,
    pub hm_root: PathBuf,
    pub proc_root: PathBuf,
    pub herdr_executable: PathBuf,
    pub expected_model: String,
    pub expected_effort: String,
    pub expected_title: String,
    pub expected_aspect: String,
    pub expected_power: String,
    pub required_skills: Vec<String>,
    pub process_id: i64,
    pub lifecycle_epoch: u64,
    pub expected_binding_generation: Option<u64>,
}

/// Privileged attester for native MAIN registration.
///
/// Authority is the configured exact receipt and process incarnation, not a
/// claim supplied through the ordinary socket.  Every independent source is
/// correlated before a value capable of mutating the delivery store exists.
pub struct ReceiptBackedNativeRegistrationAttester {
    policy: NativeAttestationPolicy,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NativeAttestationError {
    EvidenceUnavailable,
    CorrelationMismatch,
}

impl ReceiptBackedNativeRegistrationAttester {
    pub fn new(policy: NativeAttestationPolicy) -> Self {
        Self { policy }
    }

    pub fn attest(
        &self,
        submission: BindingRegistrationSubmission,
    ) -> Result<VerifiedBindingRegistration, NativeAttestationError> {
        let receipt = read_json(&self.policy.receipt_path)?;
        let node = &submission.flow_node;
        let expected_title = format!(
            "{} {} {}",
            self.policy.expected_aspect, self.policy.expected_power, node.flow_id
        );
        if expected_title != self.policy.expected_title
            || text(&receipt, "status") != Some("ready")
            || text(&receipt, "threadId") != Some(node.session_id.as_str())
            || text(&receipt, "canonicalFlowId") != Some(node.flow_id.as_str())
            || text(&receipt, "canonicalTitle") != Some(self.policy.expected_title.as_str())
            || text(&receipt, "model") != Some(self.policy.expected_model.as_str())
            || text(&receipt, "effort") != Some(self.policy.expected_effort.as_str())
            || receipt
                .pointer("/canonicalRole/aspect")
                .and_then(Value::as_str)
                != Some(self.policy.expected_aspect.as_str())
            || receipt
                .pointer("/canonicalRole/power")
                .and_then(Value::as_str)
                != Some(self.policy.expected_power.as_str())
            || receipt
                .pointer("/rolloutEvidence/sha256")
                .and_then(Value::as_str)
                != Some(submission.proof_digest.as_str())
            || !has_required_skills(&receipt, &self.policy.required_skills)
            || node.harness_kind != HarnessKind::Codex
        {
            return Err(NativeAttestationError::CorrelationMismatch);
        }

        self.verify_claim(&node.flow_id, &node.session_id)?;
        let hm = read_json(&self.policy.hm_root.join(format!("{}.json", node.flow_id)))?;
        let HerdrRouteSelection::Available(route) = &node.herdr_route_selection else {
            return Err(NativeAttestationError::CorrelationMismatch);
        };
        if text(&hm, "session") != Some(route.herdr_session_name.as_str())
            || text(&hm, "name") != Some(route.herdr_agent_name.as_str())
            || text(&hm, "pane_id") != Some(route.herdr_pane_id.as_str())
            || text(&hm, "terminal_id") != Some(route.herdr_terminal_id.as_str())
            || text(&hm, "agent") != Some("codex")
            || text(&hm, "native_thread") != Some(node.session_id.as_str())
        {
            return Err(NativeAttestationError::CorrelationMismatch);
        }
        self.verify_herdr(route)?;

        let process_start_time = self.verify_process(&node.session_id)?;
        let EndpointSelection::Available(endpoint) = &node.endpoint_selection else {
            return Err(NativeAttestationError::CorrelationMismatch);
        };
        let refresh_transition_id = match submission.binding_registration_phase {
            BindingRegistrationPhase::Bootstrap => None,
            BindingRegistrationPhase::Refresh(id) => Some(id),
        };
        Ok(VerifiedBindingRegistration {
            flow_id: node.flow_id.clone(),
            registration_id: submission.registration_id,
            refresh_transition_id,
            binding: DeliveryBinding {
                native_thread: node.session_id.clone(),
                harness_session: route.herdr_session_name.clone(),
                route_identity: format!(
                    "{}/{}/{}/{}",
                    route.herdr_session_name,
                    route.herdr_agent_name,
                    route.herdr_pane_id,
                    route.herdr_terminal_id
                ),
                endpoint_identity: endpoint.endpoint_path.clone(),
                process_pid: self.policy.process_id,
                process_start_time,
            },
            lifecycle_generation: self.policy.lifecycle_epoch,
            expected_binding_generation: self.policy.expected_binding_generation,
            readiness_receipt_id: submission.readiness_receipt_id,
            proof_digest: submission.proof_digest,
        })
    }

    fn verify_claim(
        &self,
        flow_id: &str,
        native_thread: &str,
    ) -> Result<(), NativeAttestationError> {
        let claim = fs::read_to_string(self.policy.claim_root.join(format!(".{flow_id}.flow-id")))
            .map_err(|_| NativeAttestationError::EvidenceUnavailable)?;
        let identity = native_thread.replace('-', "");
        let expected = format!("version=1\nharness=codex\nidentity={identity}\nalias={flow_id}\n");
        if claim != expected {
            return Err(NativeAttestationError::CorrelationMismatch);
        }
        Ok(())
    }

    fn verify_herdr(&self, route: &signal_flow::HerdrRoute) -> Result<(), NativeAttestationError> {
        let output = Command::new(&self.policy.herdr_executable)
            .args([
                "--session",
                route.herdr_session_name.as_str(),
                "api",
                "snapshot",
            ])
            .output()
            .map_err(|_| NativeAttestationError::EvidenceUnavailable)?;
        if !output.status.success() {
            return Err(NativeAttestationError::EvidenceUnavailable);
        }
        let snapshot: Value = serde_json::from_slice(&output.stdout)
            .map_err(|_| NativeAttestationError::EvidenceUnavailable)?;
        let matches = snapshot
            .pointer("/result/snapshot/agents")
            .and_then(Value::as_array)
            .is_some_and(|agents| {
                agents.iter().any(|agent| {
                    text(agent, "agent") == Some("codex")
                        && text(agent, "name") == Some(route.herdr_agent_name.as_str())
                        && text(agent, "pane_id") == Some(route.herdr_pane_id.as_str())
                        && text(agent, "terminal_id") == Some(route.herdr_terminal_id.as_str())
                        && matches!(text(agent, "agent_status"), Some("idle" | "working"))
                        && agent.get("interactive_ready").and_then(Value::as_bool) == Some(true)
                })
            });
        if !matches {
            return Err(NativeAttestationError::CorrelationMismatch);
        }
        Ok(())
    }

    fn verify_process(&self, native_thread: &str) -> Result<i64, NativeAttestationError> {
        let process = self
            .policy
            .proc_root
            .join(self.policy.process_id.to_string());
        let cmdline = fs::read(process.join("cmdline"))
            .map_err(|_| NativeAttestationError::EvidenceUnavailable)?;
        let args: Vec<&[u8]> = cmdline
            .split(|byte| *byte == 0)
            .filter(|arg| !arg.is_empty())
            .collect();
        let has = |needle: &str| args.iter().any(|arg| *arg == needle.as_bytes());
        if !has("resume")
            || !has(native_thread)
            || !has(&self.policy.expected_model)
            || !has(&format!(
                "model_reasoning_effort=\"{}\"",
                self.policy.expected_effort
            ))
        {
            return Err(NativeAttestationError::CorrelationMismatch);
        }
        let stat = fs::read_to_string(process.join("stat"))
            .map_err(|_| NativeAttestationError::EvidenceUnavailable)?;
        let after_command = stat
            .rsplit_once(") ")
            .map(|(_, fields)| fields)
            .ok_or(NativeAttestationError::EvidenceUnavailable)?;
        // Field 22 is process start time; fields after the command begin at field 3.
        after_command
            .split_whitespace()
            .nth(19)
            .and_then(|value| value.parse().ok())
            .ok_or(NativeAttestationError::EvidenceUnavailable)
    }
}

fn read_json(path: &Path) -> Result<Value, NativeAttestationError> {
    let bytes = fs::read(path).map_err(|_| NativeAttestationError::EvidenceUnavailable)?;
    serde_json::from_slice(&bytes).map_err(|_| NativeAttestationError::EvidenceUnavailable)
}

fn text<'a>(value: &'a Value, key: &str) -> Option<&'a str> {
    value.get(key).and_then(Value::as_str)
}

fn has_required_skills(receipt: &Value, required: &[String]) -> bool {
    let Some(skills) = receipt.get("skillManifest").and_then(Value::as_array) else {
        return false;
    };
    required.iter().all(|required_name| {
        skills
            .iter()
            .any(|skill| text(skill, "name") == Some(required_name.as_str()))
    })
}
