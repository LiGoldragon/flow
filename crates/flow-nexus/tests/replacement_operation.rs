mod store {
    pub use flow_nexus::store::*;
}

#[path = "../src/replacement_operation.rs"]
mod replacement_operation;

use flow_nexus::store::{
    AdmissionGate, AppliesFlowQuery, BeginRefresh, BeginRefreshOutcome, BootstrapBinding,
    BootstrapBindingOutcome, ManagesDeliveryPermits, OpensFlowStore, ReadyReattach,
    ReadyReattachOutcome, RegistersFlowIdentity, VerifiedBindingOutcome,
};
use meta_signal_flow::{BindingRegistrationPhase, BindingRegistrationSubmission};
use replacement_operation::{
    DurableReplaceOperation, NativeAttestationPolicy, OperationId,
    ReceiptBackedNativeRegistrationAttester, ReplacementPhase,
};
use signal_flow::{
    Available_Data, EndpointSelection, FlowLifecycle, FlowNode, HarnessKind, HerdrRoute,
    HerdrRouteSelection, OriginClue, Query, Response, RouteReadiness,
};
use std::{fs, os::unix::fs::PermissionsExt, path::Path};

const FLOW_ID: &str = "a11ce5";
const THREAD: &str = "01a0cefc-9744-43f3-8dfb-4e0eba11ce55";
const PROOF: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

fn write(path: &Path, value: impl AsRef<[u8]>) {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).unwrap();
    }
    fs::write(path, value).unwrap();
}

fn node(terminal: &str) -> FlowNode {
    FlowNode {
        flow_id: FLOW_ID.into(),
        session_id: THREAD.into(),
        harness_kind: HarnessKind::Codex,
        endpoint_selection: EndpointSelection::Available(Available_Data {
            endpoint_path: "/tmp/disposable-native.sock".into(),
            route_readiness: RouteReadiness::Ready,
        }),
        herdr_route_selection: HerdrRouteSelection::Available(HerdrRoute {
            herdr_session_name: "disposable".into(),
            herdr_agent_name: "field-disposable".into(),
            herdr_pane_id: "w9:p9".into(),
            herdr_terminal_id: terminal.into(),
        }),
        origin_clue: OriginClue {
            flow_id: "6fb948".into(),
            session_id: "controller-4639".into(),
            turn_id: "disposable-operation".into(),
        },
        flow_lifecycle: FlowLifecycle::Active,
    }
}

fn attester(root: &Path, terminal: &str) -> ReceiptBackedNativeRegistrationAttester {
    let receipt = root.join("receipt.json");
    write(
        &receipt,
        serde_json::to_vec(&serde_json::json!({
            "status": "ready",
            "threadId": THREAD,
            "canonicalFlowId": FLOW_ID,
            "canonicalTitle": "Field Medium a11ce5",
            "canonicalRole": { "aspect": "Field", "power": "Medium" },
            "model": "gpt-5.6-sol",
            "effort": "medium",
            "rolloutEvidence": { "sha256": PROOF },
            "skillManifest": [{ "name": "main-flow" }, { "name": "field" }]
        }))
        .unwrap(),
    );
    let claims = root.join("flows");
    write(
        &claims.join(format!(".{FLOW_ID}.flow-id")),
        format!(
            "version=1\nharness=codex\nidentity={}\nalias={FLOW_ID}\n",
            THREAD.replace('-', "")
        ),
    );
    let hm = root.join("hm");
    write(
        &hm.join(format!("{FLOW_ID}.json")),
        serde_json::to_vec(&serde_json::json!({
            "session": "disposable", "name": "field-disposable",
            "pane_id": "w9:p9", "terminal_id": terminal,
            "agent": "codex", "native_thread": THREAD
        }))
        .unwrap(),
    );
    let herdr = root.join("herdr");
    write(
        &herdr,
        format!(
            "#!/bin/sh\nprintf '%s\\n' '{{\"result\":{{\"snapshot\":{{\"agents\":[{{\"agent\":\"codex\",\"agent_status\":\"idle\",\"interactive_ready\":true,\"name\":\"field-disposable\",\"pane_id\":\"w9:p9\",\"terminal_id\":\"{terminal}\"}}]}}}}}}'\n"
        ),
    );
    fs::set_permissions(&herdr, fs::Permissions::from_mode(0o700)).unwrap();
    let proc_root = root.join("proc");
    let process = proc_root.join("4242");
    write(
        &process.join("cmdline"),
        format!(
            "codex\0resume\0{THREAD}\0-m\0gpt-5.6-sol\0-c\0model_reasoning_effort=\"medium\"\0"
        ),
    );
    write(
        &process.join("stat"),
        "4242 (codex worker) S 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 777 23\n",
    );
    ReceiptBackedNativeRegistrationAttester::new(NativeAttestationPolicy {
        receipt_path: receipt,
        claim_root: claims,
        hm_root: hm,
        proc_root,
        herdr_executable: herdr,
        expected_model: "gpt-5.6-sol".into(),
        expected_effort: "medium".into(),
        expected_title: "Field Medium a11ce5".into(),
        expected_aspect: "Field".into(),
        expected_power: "Medium".into(),
        required_skills: vec!["main-flow".into(), "field".into()],
        process_id: 4242,
        lifecycle_epoch: 1,
        expected_binding_generation: None,
    })
}

#[test]
fn validated_native_main_registers_resolves_and_enters_one_disposable_lifecycle_operation() {
    let fixture = tempfile::tempdir().unwrap();
    let store =
        <flow_nexus::store::FlowStore as OpensFlowStore>::open(&fixture.path().join("flow.sema"))
            .unwrap();
    let node = node("term-first");
    let verified = attester(fixture.path(), "term-first")
        .attest(BindingRegistrationSubmission {
            flow_node: node.clone(),
            registration_id: "registration-first".into(),
            binding_registration_phase: BindingRegistrationPhase::Bootstrap,
            readiness_receipt_id: "ready-first".into(),
            proof_digest: PROOF.into(),
        })
        .unwrap();
    store.register_flow(node.clone()).unwrap();
    assert_eq!(
        store.record_verified_binding(verified).unwrap(),
        VerifiedBindingOutcome::Recorded
    );
    let initialized = store
        .bootstrap_delivery_binding(BootstrapBinding {
            flow_id: FLOW_ID.into(),
            registration_id: "registration-first".into(),
        })
        .unwrap();
    assert!(matches!(
        initialized,
        BootstrapBindingOutcome::Initialized(ref state)
            if state.admission == AdmissionGate::Open && state.binding_generation == 1
    ));
    assert_eq!(
        store
            .apply(Query::ResolveRecipient(FLOW_ID.into()))
            .unwrap(),
        Response::RecipientResolved(node)
    );

    let mut operation = DurableReplaceOperation {
        operation_id: OperationId("replace-disposable-1".into()),
        lifecycle_epoch: 2,
        expected_binding_generation: 1,
        phase: ReplacementPhase::Requested,
    };
    operation.hold_admission().unwrap();
    assert_eq!(
        store
            .begin_refresh(BeginRefresh {
                flow_id: FLOW_ID.into(),
                expected_binding_generation: 1,
                transition_id: operation.operation_id.0.clone(),
            })
            .unwrap(),
        BeginRefreshOutcome::Held {
            active_permit: None
        }
    );

    operation.phase = ReplacementPhase::SuccessorPreparing;
    operation.record_readiness().unwrap();
    drop(attester(fixture.path(), "term-next"));
    let replacement_attester =
        ReceiptBackedNativeRegistrationAttester::new(NativeAttestationPolicy {
            expected_binding_generation: Some(1),
            lifecycle_epoch: 2,
            ..replacement_attester_policy(fixture.path())
        });
    let replacement = replacement_attester
        .attest(BindingRegistrationSubmission {
            flow_node: node("term-next"),
            registration_id: "registration-next".into(),
            binding_registration_phase: BindingRegistrationPhase::Refresh(
                operation.operation_id.0.clone(),
            ),
            readiness_receipt_id: "ready-next".into(),
            proof_digest: PROOF.into(),
        })
        .unwrap();
    assert_eq!(
        store.record_verified_binding(replacement).unwrap(),
        VerifiedBindingOutcome::Recorded
    );
    assert_eq!(
        store
            .ready_reattach(ReadyReattach {
                flow_id: FLOW_ID.into(),
                transition_id: operation.operation_id.0,
                expected_old_binding: match initialized {
                    BootstrapBindingOutcome::Initialized(state) => state.binding,
                    _ => unreachable!(),
                },
                expected_old_binding_generation: 1,
                registration_id: "registration-next".into(),
            })
            .unwrap(),
        ReadyReattachOutcome::Opened {
            binding_generation: 2
        }
    );
}

fn replacement_attester_policy(root: &Path) -> NativeAttestationPolicy {
    NativeAttestationPolicy {
        receipt_path: root.join("receipt.json"),
        claim_root: root.join("flows"),
        hm_root: root.join("hm"),
        proc_root: root.join("proc"),
        herdr_executable: root.join("herdr"),
        expected_model: "gpt-5.6-sol".into(),
        expected_effort: "medium".into(),
        expected_title: "Field Medium a11ce5".into(),
        expected_aspect: "Field".into(),
        expected_power: "Medium".into(),
        required_skills: vec!["main-flow".into(), "field".into()],
        process_id: 4242,
        lifecycle_epoch: 1,
        expected_binding_generation: None,
    }
}
