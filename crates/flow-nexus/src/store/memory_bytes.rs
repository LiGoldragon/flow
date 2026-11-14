//! Each Memory record Ethos now generates archives to the bytes its
//! hand-written declaration did. The declarations here are the hand ones as
//! they stood at 4282dda, frozen: a value is archived through the frozen
//! type and through the generated one, and the two archives are compared
//! byte for byte, and the frozen archive is read back as the generated type.

use rkyv::rancor;
use signal_flow::{Address, Aspect, Model, Topic};

/// The two archives of one value, the frozen declaration's and the
/// generated one's, are the same bytes, and the frozen bytes read back as
/// the generated value.
trait ArchivesAlike<Generated> {
    fn archives_alike(&self, generated: &Generated);
}

impl<Frozen, Generated> ArchivesAlike<Generated> for Frozen
where
    Frozen: for<'a> rkyv::Serialize<
            rkyv::api::high::HighSerializer<
                rkyv::util::AlignedVec,
                rkyv::ser::allocator::ArenaHandle<'a>,
                rancor::Error,
            >,
        >,
    Generated: for<'a> rkyv::Serialize<
            rkyv::api::high::HighSerializer<
                rkyv::util::AlignedVec,
                rkyv::ser::allocator::ArenaHandle<'a>,
                rancor::Error,
            >,
        > + rkyv::Archive
        + PartialEq
        + std::fmt::Debug,
    Generated::Archived: rkyv::Deserialize<Generated, rkyv::api::high::HighDeserializer<rancor::Error>>
        + for<'a> rkyv::bytecheck::CheckBytes<rkyv::api::high::HighValidator<'a, rancor::Error>>,
{
    fn archives_alike(&self, generated: &Generated) {
        let frozen = rkyv::to_bytes::<rancor::Error>(self).expect("the frozen value archives");
        let fresh =
            rkyv::to_bytes::<rancor::Error>(generated).expect("the generated value archives");
        assert_eq!(frozen.as_slice(), fresh.as_slice());
        let read = rkyv::from_bytes::<Generated, rancor::Error>(&frozen)
            .expect("the frozen bytes read as the generated type");
        assert_eq!(&read, generated);
    }
}

/// The declarations as they stood by hand, field for field.
mod frozen {
    use rkyv::{Archive, Deserialize, Serialize};
    use signal_flow::Address;

    #[derive(Archive, Serialize, Deserialize)]
    pub(super) struct StoredMetaInvestigationConfiguration {
        pub(super) investigation: Address,
    }

    #[derive(Archive, Serialize, Deserialize)]
    pub(super) struct StoredMetaDefaultMinutesConfiguration {
        pub(super) default_minutes: i64,
    }

    #[derive(Archive, Serialize, Deserialize)]
    pub(super) struct StoredMetaRequestPolicyConfiguration {
        pub(super) request_id_draws: meta_signal_flow::DrawBound,
        pub(super) announcement_tries: meta_signal_flow::TryBound,
    }

    #[derive(Archive, Serialize, Deserialize)]
    pub(super) struct StoredMetaHarnessHookConfiguration {
        pub(super) harness_hook: String,
    }

    #[derive(Archive, Serialize, Deserialize)]
    pub(super) struct StoredMetaHeartbeatConfiguration {
        pub(super) heartbeat: meta_signal_flow::HeartbeatConfig,
    }

    #[derive(Archive, Serialize, Deserialize)]
    pub(super) struct StoredMetaflowLock {
        pub(super) lock: signal_flow::Lock,
    }

    #[derive(Archive, Serialize, Deserialize)]
    pub(super) struct FlowStoreState {
        pub(super) next_flow_number: u64,
    }

    #[derive(Archive, Serialize, Deserialize)]
    pub(super) struct FlowStoreConfiguration {
        pub(super) ordinary_socket_path: String,
        pub(super) meta_socket_path: String,
    }
    #[derive(Archive, Serialize, Deserialize)]
    pub(super) enum FlowLifecycle {
        Pending,
        Active,
        Stopped,
        Retired,
        Exited,
    }

    #[derive(Archive, Serialize, Deserialize)]
    pub(super) struct FlowRecord {
        pub(super) flow_id: String,
        pub(super) flow_type: String,
        pub(super) origin: signal_flow::Origin,
        pub(super) thread_id: Option<String>,
        pub(super) harness_kind: signal_flow::HarnessKind,
        pub(super) endpoint_selection: signal_flow::EndpointSelection,
        pub(super) lifecycle: FlowLifecycle,
        pub(super) generation: u64,
    }

    #[derive(Archive, Serialize, Deserialize)]
    pub(super) struct NativeOwnerProof {
        pub(super) process_id: i64,
        pub(super) process_user_id: i64,
        pub(super) process_start_token: String,
        pub(super) host_id: String,
        pub(super) boot_id: String,
        pub(super) harness_kind: signal_flow::HarnessKind,
        pub(super) native_session_id: String,
    }

    #[derive(Archive, Serialize, Deserialize)]
    pub(super) struct NativeOwnerProofRecord {
        pub(super) flow_id: String,
        pub(super) proof: NativeOwnerProof,
    }

    #[derive(Archive, Serialize, Deserialize)]
    pub(super) struct StoredMetaflowProcess {
        pub(super) process: signal_flow::Process,
        pub(super) address: Address,
    }

    #[derive(Archive, Serialize, Deserialize)]
    pub(super) struct CodexEndpointConfiguration {
        pub(super) client_path: String,
        pub(super) home: String,
        pub(super) socket: String,
        pub(super) model_names: Vec<String>,
    }

    #[derive(Archive, Serialize, Deserialize)]
    pub(super) struct RuntimeConfiguration {
        pub(super) source_root: String,
        pub(super) stable_codex: CodexEndpointConfiguration,
        pub(super) next_codex: CodexEndpointConfiguration,
    }

    #[derive(Archive, Serialize, Deserialize)]
    pub(super) struct StoredHeartbeatStanding {
        pub(super) address: Address,
        pub(super) baseline_awake: i64,
        pub(super) baseline_working: Option<i64>,
        pub(super) baseline_working_complete: bool,
        pub(super) baseline_sent: i64,
        pub(super) baseline_queue: i64,
        pub(super) latest_awake: i64,
        pub(super) latest_working: Option<i64>,
        pub(super) latest_working_complete: bool,
        pub(super) latest_sent: i64,
        pub(super) latest_queue: i64,
        pub(super) outstanding: Option<String>,
    }

    #[derive(Archive, Serialize, Deserialize)]
    pub(super) enum DeliveryPurpose {
        Ordinary,
        Heartbeat,
    }

    #[derive(Archive, Serialize, Deserialize)]
    pub(super) struct LogicalDeliveryPurpose {
        pub(super) logical_delivery_id: String,
        pub(super) address: Address,
        pub(super) purpose: DeliveryPurpose,
    }

    #[derive(Archive, Serialize, Deserialize)]
    pub(super) enum LeaseStep {
        Acquired,
        Interrupted,
        Placed,
        Submitted,
    }

    #[derive(Archive, Serialize, Deserialize)]
    pub(super) struct PaneLease {
        pub(super) delivery_id: meta_signal_flow::DeliveryId,
        pub(super) flow_id: String,
        pub(super) herdr_pane_id: String,
        pub(super) lease_step: LeaseStep,
    }

    #[derive(Archive, Serialize, Deserialize)]
    pub(super) struct LogicalDeliveryCounter {
        pub(super) next_cursor: i64,
    }

    #[derive(Archive, Serialize, Deserialize)]
    pub(super) struct SimplifiedReplyBinding {
        pub(super) flow_id: String,
        pub(super) input_delivery_id: String,
        pub(super) source: Address,
        pub(super) recipient: Address,
        pub(super) completed: bool,
    }

    #[derive(Archive, Serialize, Deserialize)]
    pub(super) enum RequestClosure {
        Open,
        Answered(flow_ethos::LogicalDeliveryId),
    }

    #[derive(Archive, Serialize, Deserialize)]
    pub(super) struct RequestIdBinding {
        pub(super) request_id: flow_ethos::RequestId,
        pub(super) logical_delivery_id: flow_ethos::LogicalDeliveryId,
        pub(super) closure: RequestClosure,
    }

    #[derive(Archive, Serialize, Deserialize)]
    pub(super) struct DeliveryCounter {
        pub(super) next_request: u64,
        pub(super) next_attempt: u64,
    }

    #[derive(Archive, Serialize, Deserialize)]
    pub(super) struct SendOutcomeCounter {
        pub(super) next_id: u64,
    }

    #[derive(Archive, Serialize, Deserialize)]
    pub(super) enum RequestState {
        Queued,
        Settled,
        Unsent,
        Parked,
    }

    #[derive(Archive, Serialize, Deserialize)]
    pub(super) struct StoredDeliveryRequest {
        pub(super) request_id: String,
        pub(super) address: Address,
        pub(super) queue_evidence: meta_signal_flow::QueueEvidence,
        pub(super) state: RequestState,
    }

    #[derive(Archive, Serialize, Deserialize)]
    pub(super) struct NativeDeliveryReceipt {
        pub(super) native_request_id: u64,
        pub(super) thread_id: String,
        pub(super) turn_id: String,
        pub(super) native_receipt: String,
    }

    #[derive(Archive, Serialize, Deserialize)]
    pub(super) struct NativeTranscriptCursor {
        pub(super) device: u64,
        pub(super) inode: u64,
        pub(super) offset: u64,
    }

    #[derive(Archive, Serialize, Deserialize)]
    pub(super) enum NativeHistoryAnchor {
        ClaudeTranscript(NativeTranscriptCursor),
        CodexTurns(Option<String>),
    }

    #[derive(Archive, Serialize, Deserialize)]
    pub(super) struct AbsenceRecord {
        pub(super) proof: meta_signal_flow::AbsenceProof,
        pub(super) observation: String,
    }

    #[derive(Archive, Serialize, Deserialize)]
    pub(super) enum QueueDeliveryPhase {
        Prepared,
        Uncertain(Option<meta_signal_flow::Undecided>),
        ReceiptRecorded(NativeDeliveryReceipt),
        Settled(NativeDeliveryReceipt),
        Read {
            answer: String,
            late_receipt: Option<NativeDeliveryReceipt>,
        },
        Absent(AbsenceRecord),
        Resolved(meta_signal_flow::QueueResolutionRecord),
    }

    #[derive(Archive, Serialize, Deserialize)]
    pub(super) struct QueueDeliveryAttempt {
        pub(super) request_id: String,
        pub(super) attempt_id: String,
        pub(super) native_request_id: u64,
        pub(super) flow_id: String,
        pub(super) thread_id: String,
        pub(super) harness_kind: signal_flow::HarnessKind,
        pub(super) history_anchor: NativeHistoryAnchor,
        pub(super) address: Address,
        pub(super) queue_evidence: meta_signal_flow::QueueEvidence,
        pub(super) phase: QueueDeliveryPhase,
    }

    #[derive(Archive, Serialize, Deserialize)]
    pub(super) enum Investigation {
        Started(String),
        Queued,
        NotConfigured,
        Refused(String),
    }

    #[derive(Archive, Serialize, Deserialize)]
    pub(super) struct Compaction {
        pub(super) flow_id: String,
        pub(super) boundary: String,
        pub(super) address: Option<Address>,
        pub(super) investigation: Investigation,
    }

    #[derive(Archive, Serialize, Deserialize)]
    pub(super) struct FlowContextSize {
        pub(super) flow_id: String,
        pub(super) tokens: i64,
    }

    #[derive(Archive, Serialize, Deserialize)]
    pub(super) struct ClosedHandover {
        pub(super) generation: i64,
        pub(super) predecessor: String,
        pub(super) successor: String,
        pub(super) sections: Vec<signal_flow::Section>,
        pub(super) touch: Option<signal_flow::Touch>,
    }

    #[derive(Archive, Serialize, Deserialize)]
    pub(super) struct HandoverMemory {
        pub(super) address: Address,
        pub(super) current: Vec<signal_flow::Section>,
        pub(super) touch: Option<signal_flow::Touch>,
        pub(super) generation: i64,
        pub(super) current_flow_id: Option<String>,
        pub(super) closed: Vec<ClosedHandover>,
    }

    #[derive(Archive, Serialize, Deserialize)]
    pub(super) struct HandoverReminder {
        pub(super) address: Address,
        pub(super) generation: i64,
        pub(super) model_name: String,
        pub(super) opened_at: i64,
        pub(super) next_due: i64,
        pub(super) sent: i64,
        pub(super) every_minutes: i64,
        pub(super) reminder_text: String,
    }

    #[derive(Archive, Serialize, Deserialize)]
    pub(super) enum CleanupStep {
        Pending,
        Done,
    }

    #[derive(Archive, Serialize, Deserialize)]
    pub(super) struct LaunchCleanup {
        pub(super) launch_request_id: String,
        pub(super) native_session_id: String,
        pub(super) thread_step: CleanupStep,
        pub(super) pane_step: CleanupStep,
    }

    #[derive(Archive, Serialize, Deserialize)]
    pub(super) struct OpenedLaunchThread {
        pub(super) launch_request_id: String,
        pub(super) native_session_id: String,
    }

    #[derive(Archive, Serialize, Deserialize)]
    pub(super) enum ReplacementOrder {
        Refresh,
        Launch(Box<OrderedLaunch>),
        Restart,
    }

    #[derive(Archive, Serialize, Deserialize)]
    pub(super) struct OrderedLaunch {
        pub(super) launch_profile: signal_flow::LaunchProfile,
        pub(super) origin: signal_flow::Origin,
        pub(super) prompt_sha256: String,
    }

    #[derive(Archive, Serialize, Deserialize)]
    pub(super) struct OrderedReplacement {
        pub(super) address: Address,
        pub(super) predecessor: String,
        pub(super) order: ReplacementOrder,
    }

    #[derive(Archive, Serialize, Deserialize)]
    pub(super) struct Reminder {
        pub(super) key: String,
        pub(super) address: Address,
        pub(super) recipient: Address,
        pub(super) minutes: i64,
        pub(super) due: Option<i64>,
        pub(super) sent: i64,
    }

    #[derive(Archive, Serialize, Deserialize)]
    pub(super) enum SuccessionCause {
        Refresh,
        Exit,
        Ordered,
        Replace,
    }

    #[derive(Archive, Serialize, Deserialize)]
    pub(super) struct SuccessorIntent {
        pub(super) address: Address,
        pub(super) predecessor: String,
        pub(super) cause: SuccessionCause,
        pub(super) launch_request_id: String,
    }

    #[derive(Archive, Serialize, Deserialize)]
    pub(super) struct UndecodableRecord {
        pub(super) key: String,
        pub(super) frame: signal_flow::UndecodableFrame,
    }

    #[derive(Archive, Serialize, Deserialize)]
    pub(super) struct AddressHold {
        pub(super) flow_id: String,
        pub(super) address: Address,
        pub(super) found: Option<signal_flow::Metaflow>,
    }
}

/// An Address the samples share.
trait SamplesAddress {
    fn sample(topic: &str) -> Self;
}

impl SamplesAddress for Address {
    fn sample(topic: &str) -> Self {
        Address {
            aspect: Aspect::Mind,
            topic: Topic(topic.into()),
            model: Model::Sonnet,
        }
    }
}

// The checks read a test only inside a `#[cfg(test)]` item.
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn meta_configuration_records_archive_as_they_did() {
        use crate::generated::memory as generated;
        frozen::StoredMetaInvestigationConfiguration {
            investigation: Address::sample("investigation"),
        }
        .archives_alike(&generated::StoredMetaInvestigationConfiguration {
            address: Address::sample("investigation"),
        });
        frozen::StoredMetaDefaultMinutesConfiguration {
            default_minutes: 45,
        }
        .archives_alike(&generated::StoredMetaDefaultMinutesConfiguration {
            minutes: flow_ethos::Minutes(45),
        });
        frozen::StoredMetaRequestPolicyConfiguration {
            request_id_draws: meta_signal_flow::DrawBound(8),
            announcement_tries: meta_signal_flow::TryBound(3),
        }
        .archives_alike(&generated::StoredMetaRequestPolicyConfiguration {
            draw_bound: meta_signal_flow::DrawBound(8),
            try_bound: meta_signal_flow::TryBound(3),
        });
        frozen::StoredMetaHarnessHookConfiguration {
            harness_hook: "/run/current-system/sw/bin/flow-hook".into(),
        }
        .archives_alike(&generated::StoredMetaHarnessHookConfiguration {
            path: meta_signal_flow::generated::signal::Path(
                "/run/current-system/sw/bin/flow-hook".into(),
            ),
        });
        let heartbeat = meta_signal_flow::HeartbeatConfig {
            heartbeat_period_seconds: meta_signal_flow::HeartbeatPeriodSeconds(600),
            heartbeat_recipients: meta_signal_flow::HeartbeatRecipients(vec![Address::sample(
                "heartbeat",
            )]),
        };
        frozen::StoredMetaHeartbeatConfiguration {
            heartbeat: heartbeat.clone(),
        }
        .archives_alike(&generated::StoredMetaHeartbeatConfiguration {
            heartbeat_config: heartbeat,
        });
        let lock = signal_flow::Lock {
            first_address: Address::sample("first"),
            second_address: Address::sample("second"),
            until: flow_ethos::Until(1_800_000_000),
        };
        frozen::StoredMetaflowLock { lock: lock.clone() }
            .archives_alike(&generated::StoredMetaflowLock { lock });
    }

    #[test]
    fn store_identity_records_archive_as_they_did() {
        use crate::generated::memory as generated;
        frozen::FlowStoreState {
            next_flow_number: 4_242,
        }
        .archives_alike(&generated::FlowStoreState {
            next_flow_number: generated::NextFlowNumber(4_242),
        });
        frozen::FlowStoreConfiguration {
            ordinary_socket_path: "/run/user/1001/flow/ordinary.sock".into(),
            meta_socket_path: "/run/user/1001/flow/meta.sock".into(),
        }
        .archives_alike(&generated::FlowStoreConfiguration {
            ordinary_socket_path: signal_flow::OrdinarySocketPath(
                "/run/user/1001/flow/ordinary.sock".into(),
            ),
            meta_socket_path: signal_flow::MetaSocketPath("/run/user/1001/flow/meta.sock".into()),
        });
    }

    #[test]
    fn flow_records_archive_as_they_did() {
        use crate::generated::memory as generated;
        for (frozen_lifecycle, lifecycle, generation) in [
            (
                frozen::FlowLifecycle::Pending,
                signal_flow::FlowLifecycle::Pending,
                1,
            ),
            (
                frozen::FlowLifecycle::Active,
                signal_flow::FlowLifecycle::Active,
                2,
            ),
            (
                frozen::FlowLifecycle::Stopped,
                signal_flow::FlowLifecycle::Stopped,
                3,
            ),
            (
                frozen::FlowLifecycle::Retired,
                signal_flow::FlowLifecycle::Retired,
                4,
            ),
            (
                frozen::FlowLifecycle::Exited,
                signal_flow::FlowLifecycle::Exited,
                5,
            ),
        ] {
            frozen::FlowRecord {
                flow_id: "flow-00000000000000a1".into(),
                flow_type: "Mind:sonnet".into(),
                origin: signal_flow::Origin::Unbound,
                thread_id: Some("session-a1".into()),
                harness_kind: signal_flow::HarnessKind::Claude,
                endpoint_selection: signal_flow::EndpointSelection::Unavailable,
                lifecycle: frozen_lifecycle,
                generation,
            }
            .archives_alike(&generated::FlowRecord {
                flow_id: signal_flow::FlowId("flow-00000000000000a1".into()),
                flow_type: signal_flow::FlowType("Mind:sonnet".into()),
                origin: signal_flow::Origin::Unbound,
                session_id_option: Some(signal_flow::SessionId("session-a1".into())),
                harness_kind: signal_flow::HarnessKind::Claude,
                endpoint_selection: signal_flow::EndpointSelection::Unavailable,
                flow_lifecycle: lifecycle,
                generation: generated::Generation(generation as i64),
            });
        }
        frozen::NativeOwnerProofRecord {
            flow_id: "flow-b2".into(),
            proof: frozen::NativeOwnerProof {
                process_id: 4242,
                process_user_id: 1001,
                process_start_token: "88123".into(),
                host_id: "host".into(),
                boot_id: "boot".into(),
                harness_kind: signal_flow::HarnessKind::Codex,
                native_session_id: "thread-b2".into(),
            },
        }
        .archives_alike(&generated::NativeOwnerProofRecord {
            flow_id: signal_flow::FlowId("flow-b2".into()),
            native_owner_proof: generated::NativeOwnerProof {
                process_id: meta_signal_flow::ProcessId(4242),
                process_user_id: meta_signal_flow::ProcessUserId(1001),
                process_start_token: meta_signal_flow::ProcessStartToken("88123".into()),
                host_id: generated::HostId("host".into()),
                boot_id: generated::BootId("boot".into()),
                harness_kind: signal_flow::HarnessKind::Codex,
                native_session_id: signal_flow::NativeSessionId("thread-b2".into()),
            },
        });
        let process = signal_flow::Process {
            pid: flow_ethos::Pid(77),
            started: flow_ethos::Started(1_700_000_000),
        };
        frozen::StoredMetaflowProcess {
            process: process.clone(),
            address: Address::sample("process"),
        }
        .archives_alike(&generated::StoredMetaflowProcess {
            process,
            address: Address::sample("process"),
        });
    }

    #[test]
    fn runtime_and_heartbeat_records_archive_as_they_did() {
        use crate::generated::memory as generated;
        let frozen_endpoint = |name: &str| frozen::CodexEndpointConfiguration {
            client_path: format!("/opt/{name}/codex"),
            home: format!("/home/{name}"),
            socket: format!("/run/{name}.sock"),
            model_names: vec![format!("{name}-one"), format!("{name}-two")],
        };
        let endpoint = |name: &str| flow_ethos::CodexEndpoint {
            client_path: flow_ethos::ClientPath(format!("/opt/{name}/codex")),
            home: flow_ethos::Home(format!("/home/{name}")),
            control_socket_path: flow_ethos::ControlSocketPath(format!("/run/{name}.sock")),
            model_name_vector: vec![
                flow_ethos::ModelName(format!("{name}-one")),
                flow_ethos::ModelName(format!("{name}-two")),
            ],
        };
        frozen::RuntimeConfiguration {
            source_root: "/home/someone/primary".into(),
            stable_codex: frozen_endpoint("stable"),
            next_codex: frozen_endpoint("next"),
        }
        .archives_alike(&generated::RuntimeConfiguration {
            source_root: flow_ethos::SourceRoot("/home/someone/primary".into()),
            stable_codex: generated::StableCodex(endpoint("stable")),
            next_codex: generated::NextCodex(endpoint("next")),
        });
        frozen::StoredHeartbeatStanding {
            address: Address::sample("heartbeat"),
            baseline_awake: 1,
            baseline_working: Some(2),
            baseline_working_complete: true,
            baseline_sent: 3,
            baseline_queue: 4,
            latest_awake: 5,
            latest_working: None,
            latest_working_complete: false,
            latest_sent: 6,
            latest_queue: 7,
            outstanding: Some("logical-9".into()),
        }
        .archives_alike(&generated::StoredHeartbeatStanding {
            address: Address::sample("heartbeat"),
            baseline_awake: generated::BaselineAwake(1),
            baseline_working: generated::BaselineWorking(Some(2)),
            baseline_working_complete: generated::BaselineWorkingComplete(true),
            baseline_sent: generated::BaselineSent(3),
            baseline_queue: generated::BaselineQueue(4),
            latest_awake: generated::LatestAwake(5),
            latest_working: generated::LatestWorking(None),
            latest_working_complete: generated::LatestWorkingComplete(false),
            latest_sent: generated::LatestSent(6),
            latest_queue: generated::LatestQueue(7),
            logical_delivery_id_option: Some(signal_flow::LogicalDeliveryId("logical-9".into())),
        });
    }

    #[test]
    fn delivery_records_archive_as_they_did() {
        use crate::generated::memory as generated;
        for (frozen_purpose, purpose) in [
            (
                frozen::DeliveryPurpose::Ordinary,
                generated::DeliveryPurpose::Ordinary,
            ),
            (
                frozen::DeliveryPurpose::Heartbeat,
                generated::DeliveryPurpose::Heartbeat,
            ),
        ] {
            frozen::LogicalDeliveryPurpose {
                logical_delivery_id: "logical-1".into(),
                address: Address::sample("purpose"),
                purpose: frozen_purpose,
            }
            .archives_alike(&generated::LogicalDeliveryPurpose {
                logical_delivery_id: signal_flow::LogicalDeliveryId("logical-1".into()),
                address: Address::sample("purpose"),
                delivery_purpose: purpose,
            });
        }
        for (frozen_step, step) in [
            (frozen::LeaseStep::Acquired, generated::LeaseStep::Acquired),
            (
                frozen::LeaseStep::Interrupted,
                generated::LeaseStep::Interrupted,
            ),
            (frozen::LeaseStep::Placed, generated::LeaseStep::Placed),
            (
                frozen::LeaseStep::Submitted,
                generated::LeaseStep::Submitted,
            ),
        ] {
            frozen::PaneLease {
                delivery_id: meta_signal_flow::DeliveryId("delivery-1".into()),
                flow_id: "flow-1".into(),
                herdr_pane_id: "pane-7".into(),
                lease_step: frozen_step,
            }
            .archives_alike(&generated::PaneLease {
                delivery_id: meta_signal_flow::DeliveryId("delivery-1".into()),
                flow_id: signal_flow::FlowId("flow-1".into()),
                herdr_pane_id: signal_flow::HerdrPaneId("pane-7".into()),
                lease_step: step,
            });
        }
        frozen::LogicalDeliveryCounter { next_cursor: 912 }.archives_alike(
            &generated::LogicalDeliveryCounter {
                next_cursor: generated::NextCursor(912),
            },
        );
        frozen::DeliveryCounter {
            next_request: 77,
            next_attempt: 78,
        }
        .archives_alike(&generated::DeliveryCounter {
            next_request: generated::NextRequest(77),
            next_attempt: generated::NextAttempt(78),
        });
        frozen::SendOutcomeCounter { next_id: 3_979 }.archives_alike(
            &generated::SendOutcomeCounter {
                next_id: generated::NextId(3_979),
            },
        );
        frozen::SimplifiedReplyBinding {
            flow_id: "flow-s".into(),
            input_delivery_id: "logical-input".into(),
            source: Address::sample("source"),
            recipient: Address::sample("recipient"),
            completed: true,
        }
        .archives_alike(&generated::SimplifiedReplyBinding {
            flow_id: signal_flow::FlowId("flow-s".into()),
            input_delivery_id: generated::InputDeliveryId("logical-input".into()),
            source: generated::Source(Address::sample("source")),
            recipient: generated::Recipient(Address::sample("recipient")),
            completed: generated::Completed(true),
        });
        for (frozen_closure, closure) in [
            (
                frozen::RequestClosure::Open,
                generated::RequestClosure::Open,
            ),
            (
                frozen::RequestClosure::Answered(flow_ethos::LogicalDeliveryId("answer".into())),
                generated::RequestClosure::Answered(flow_ethos::LogicalDeliveryId("answer".into())),
            ),
        ] {
            frozen::RequestIdBinding {
                request_id: flow_ethos::RequestId("k7ph".into()),
                logical_delivery_id: flow_ethos::LogicalDeliveryId("logical-2".into()),
                closure: frozen_closure,
            }
            .archives_alike(&generated::RequestIdBinding {
                request_id: flow_ethos::RequestId("k7ph".into()),
                logical_delivery_id: flow_ethos::LogicalDeliveryId("logical-2".into()),
                request_closure: closure,
            });
        }
    }

    /// A queued entry the queue samples share.
    trait SamplesQueueEvidence {
        fn sample() -> Self;
    }

    impl SamplesQueueEvidence for meta_signal_flow::QueueEvidence {
        fn sample() -> Self {
            meta_signal_flow::QueueEvidence::Logical(flow_ethos::QueueEntry {
                logical_delivery_id: flow_ethos::LogicalDeliveryId("logical-q".into()),
                request_id: flow_ethos::RequestId("m2x9".into()),
                sent_by: signal_flow::SentBy::Sender(Address::sample("sender")),
                delivery: flow_ethos::Delivery::Wake,
                message: flow_ethos::Message::Soft(flow_ethos::Request::Notice(
                    "queued words".into(),
                )),
            })
        }
    }

    #[test]
    fn queue_delivery_records_archive_as_they_did() {
        use crate::generated::memory as generated;
        use meta_signal_flow::QueueEvidence;
        for (frozen_state, state) in [
            (
                frozen::RequestState::Queued,
                generated::RequestState::Queued,
            ),
            (
                frozen::RequestState::Settled,
                generated::RequestState::Settled,
            ),
            (
                frozen::RequestState::Unsent,
                generated::RequestState::Unsent,
            ),
            (
                frozen::RequestState::Parked,
                generated::RequestState::Parked,
            ),
        ] {
            frozen::StoredDeliveryRequest {
                request_id: "logical-q".into(),
                address: Address::sample("queue"),
                queue_evidence: QueueEvidence::sample(),
                state: frozen_state,
            }
            .archives_alike(&generated::StoredDeliveryRequest {
                queue_request_id: meta_signal_flow::QueueRequestId("logical-q".into()),
                address: Address::sample("queue"),
                queue_evidence: QueueEvidence::sample(),
                request_state: state,
            });
        }
        let frozen_receipt = || frozen::NativeDeliveryReceipt {
            native_request_id: 41,
            thread_id: "thread".into(),
            turn_id: "turn".into(),
            native_receipt: r#"{"id":41,"result":{}}"#.into(),
        };
        let receipt = || generated::NativeDeliveryReceipt {
            native_request_id: generated::NativeRequestId(41),
            thread_id: generated::ThreadId("thread".into()),
            turn_id: generated::TurnId("turn".into()),
            native_receipt_text: generated::NativeReceiptText(r#"{"id":41,"result":{}}"#.into()),
        };
        let phases = vec![
            (
                frozen::QueueDeliveryPhase::Prepared,
                generated::QueueDeliveryPhase::Prepared,
            ),
            (
                frozen::QueueDeliveryPhase::Uncertain(None),
                generated::QueueDeliveryPhase::Uncertain(None),
            ),
            (
                frozen::QueueDeliveryPhase::ReceiptRecorded(frozen_receipt()),
                generated::QueueDeliveryPhase::ReceiptRecorded(receipt()),
            ),
            (
                frozen::QueueDeliveryPhase::Settled(frozen_receipt()),
                generated::QueueDeliveryPhase::Settled(receipt()),
            ),
            (
                frozen::QueueDeliveryPhase::Read {
                    answer: "logical-answer".into(),
                    late_receipt: None,
                },
                generated::QueueDeliveryPhase::Read(generated::Read_Data {
                    answer: generated::Answer("logical-answer".into()),
                    late_receipt: generated::LateReceipt(None),
                }),
            ),
            (
                frozen::QueueDeliveryPhase::Read {
                    answer: "logical-answer".into(),
                    late_receipt: Some(frozen_receipt()),
                },
                generated::QueueDeliveryPhase::Read(generated::Read_Data {
                    answer: generated::Answer("logical-answer".into()),
                    late_receipt: generated::LateReceipt(Some(receipt())),
                }),
            ),
            (
                frozen::QueueDeliveryPhase::Absent(frozen::AbsenceRecord {
                    proof: meta_signal_flow::AbsenceProof::HistoryAbsent,
                    observation: "never reached".into(),
                }),
                generated::QueueDeliveryPhase::Absent(generated::AbsenceRecord {
                    absence_proof: meta_signal_flow::AbsenceProof::HistoryAbsent,
                    observation: generated::Observation("never reached".into()),
                }),
            ),
        ];
        let anchors = || {
            vec![
                (
                    frozen::NativeHistoryAnchor::ClaudeTranscript(frozen::NativeTranscriptCursor {
                        device: 66_311,
                        inode: u64::MAX - 5,
                        offset: 1_048_576,
                    }),
                    generated::NativeHistoryAnchor::ClaudeTranscript(
                        generated::NativeTranscriptCursor {
                            device: generated::Device(66_311),
                            inode: generated::Inode((u64::MAX - 5) as i64),
                            offset: generated::Offset(1_048_576),
                        },
                    ),
                ),
                (
                    frozen::NativeHistoryAnchor::CodexTurns(Some("turn-0".into())),
                    generated::NativeHistoryAnchor::CodexTurns(Some("turn-0".into())),
                ),
                (
                    frozen::NativeHistoryAnchor::CodexTurns(None),
                    generated::NativeHistoryAnchor::CodexTurns(None),
                ),
            ]
        };
        for (frozen_phase, phase) in phases {
            for (frozen_anchor, anchor) in anchors() {
                frozen::QueueDeliveryAttempt {
                    request_id: "logical-q".into(),
                    attempt_id: "queue-attempt-0000000000000029".into(),
                    native_request_id: 41,
                    flow_id: "flow-q".into(),
                    thread_id: "thread".into(),
                    harness_kind: signal_flow::HarnessKind::Claude,
                    history_anchor: frozen_anchor,
                    address: Address::sample("queue"),
                    queue_evidence: QueueEvidence::sample(),
                    phase: match &frozen_phase {
                        frozen::QueueDeliveryPhase::Prepared => {
                            frozen::QueueDeliveryPhase::Prepared
                        }
                        frozen::QueueDeliveryPhase::Uncertain(_) => {
                            frozen::QueueDeliveryPhase::Uncertain(None)
                        }
                        frozen::QueueDeliveryPhase::ReceiptRecorded(_) => {
                            frozen::QueueDeliveryPhase::ReceiptRecorded(frozen_receipt())
                        }
                        frozen::QueueDeliveryPhase::Settled(_) => {
                            frozen::QueueDeliveryPhase::Settled(frozen_receipt())
                        }
                        frozen::QueueDeliveryPhase::Read { late_receipt, .. } => {
                            frozen::QueueDeliveryPhase::Read {
                                answer: "logical-answer".into(),
                                late_receipt: late_receipt.as_ref().map(|_| frozen_receipt()),
                            }
                        }
                        frozen::QueueDeliveryPhase::Absent(_) => {
                            frozen::QueueDeliveryPhase::Absent(frozen::AbsenceRecord {
                                proof: meta_signal_flow::AbsenceProof::HistoryAbsent,
                                observation: "never reached".into(),
                            })
                        }
                        frozen::QueueDeliveryPhase::Resolved(record) => {
                            frozen::QueueDeliveryPhase::Resolved(record.clone())
                        }
                    },
                }
                .archives_alike(&generated::QueueDeliveryAttempt {
                    queue_request_id: meta_signal_flow::QueueRequestId("logical-q".into()),
                    queue_attempt_id: meta_signal_flow::QueueAttemptId(
                        "queue-attempt-0000000000000029".into(),
                    ),
                    native_request_id: generated::NativeRequestId(41),
                    flow_id: signal_flow::FlowId("flow-q".into()),
                    thread_id: generated::ThreadId("thread".into()),
                    harness_kind: signal_flow::HarnessKind::Claude,
                    native_history_anchor: anchor,
                    address: Address::sample("queue"),
                    queue_evidence: QueueEvidence::sample(),
                    queue_delivery_phase: phase.clone(),
                });
            }
        }
    }

    #[test]
    fn compaction_handover_and_reminder_records_archive_as_they_did() {
        use crate::generated::memory as generated;
        for (frozen_investigation, investigation) in [
            (
                frozen::Investigation::Started("brief".into()),
                generated::Investigation::Started("brief".into()),
            ),
            (
                frozen::Investigation::Queued,
                generated::Investigation::Queued,
            ),
            (
                frozen::Investigation::NotConfigured,
                generated::Investigation::NotConfigured,
            ),
            (
                frozen::Investigation::Refused("why".into()),
                generated::Investigation::Refused("why".into()),
            ),
        ] {
            frozen::Compaction {
                flow_id: "flow-c".into(),
                boundary: "boundary-uuid".into(),
                address: Some(Address::sample("compacted")),
                investigation: frozen_investigation,
            }
            .archives_alike(&generated::Compaction {
                flow_id: signal_flow::FlowId("flow-c".into()),
                boundary: generated::Boundary("boundary-uuid".into()),
                address_option: Some(Address::sample("compacted")),
                investigation,
            });
        }
        frozen::FlowContextSize {
            flow_id: "flow-c".into(),
            tokens: 180_000,
        }
        .archives_alike(&generated::FlowContextSize {
            flow_id: signal_flow::FlowId("flow-c".into()),
            tokens: signal_flow::Tokens(180_000),
        });
        frozen::HandoverMemory {
            address: Address::sample("handover"),
            current: Vec::new(),
            touch: None,
            generation: 3,
            current_flow_id: Some("flow-now".into()),
            closed: vec![frozen::ClosedHandover {
                generation: 2,
                predecessor: "flow-before".into(),
                successor: "flow-now".into(),
                sections: Vec::new(),
                touch: None,
            }],
        }
        .archives_alike(&generated::HandoverMemory {
            address: Address::sample("handover"),
            section_vector: Vec::new(),
            touch_option: None,
            handover_generation: signal_flow::HandoverGeneration(3),
            flow_id_option: Some(signal_flow::FlowId("flow-now".into())),
            closed_handover_vector: vec![generated::ClosedHandover {
                handover_generation: signal_flow::HandoverGeneration(2),
                predecessor: generated::Predecessor("flow-before".into()),
                successor: signal_flow::Successor(signal_flow::FlowId("flow-now".into())),
                section_vector: Vec::new(),
                touch_option: None,
            }],
        });
        frozen::HandoverReminder {
            address: Address::sample("handover"),
            generation: 3,
            model_name: "sonnet".into(),
            opened_at: 1_700_000_000,
            next_due: 1_700_000_600,
            sent: 1,
            every_minutes: 10,
            reminder_text: "hand over".into(),
        }
        .archives_alike(&generated::HandoverReminder {
            address: Address::sample("handover"),
            handover_generation: signal_flow::HandoverGeneration(3),
            model_name: signal_flow::ModelName("sonnet".into()),
            opened_at: generated::OpenedAt(1_700_000_000),
            next_due: generated::NextDue(1_700_000_600),
            sent: generated::Sent(1),
            every_minutes: meta_signal_flow::EveryMinutes(10),
            reminder_text: meta_signal_flow::ReminderText("hand over".into()),
        });
        for (due, generated_due) in [
            (Some(1_700_000_900), Some(generated::Due(1_700_000_900))),
            (None, None),
        ] {
            frozen::Reminder {
                key: "logical-task".into(),
                address: Address::sample("sender"),
                recipient: Address::sample("recipient"),
                minutes: 15,
                due,
                sent: 2,
            }
            .archives_alike(&generated::Reminder {
                logical_delivery_id: signal_flow::LogicalDeliveryId("logical-task".into()),
                address: Address::sample("sender"),
                recipient: generated::Recipient(Address::sample("recipient")),
                minutes: flow_ethos::Minutes(15),
                due_option: generated_due,
                sent: generated::Sent(2),
            });
        }
    }

    #[test]
    fn launch_and_succession_records_archive_as_they_did() {
        use crate::generated::memory as generated;
        for (frozen_thread, frozen_pane, thread, pane) in [
            (
                frozen::CleanupStep::Pending,
                frozen::CleanupStep::Done,
                generated::CleanupStep::Pending,
                generated::CleanupStep::Done,
            ),
            (
                frozen::CleanupStep::Done,
                frozen::CleanupStep::Pending,
                generated::CleanupStep::Done,
                generated::CleanupStep::Pending,
            ),
        ] {
            frozen::LaunchCleanup {
                launch_request_id: "launch-l".into(),
                native_session_id: "thread-l".into(),
                thread_step: frozen_thread,
                pane_step: frozen_pane,
            }
            .archives_alike(&generated::LaunchCleanup {
                launch_request_id: signal_flow::LaunchRequestId("launch-l".into()),
                native_session_id: signal_flow::NativeSessionId("thread-l".into()),
                thread_step: generated::ThreadStep(thread),
                pane_step: generated::PaneStep(pane),
            });
        }
        frozen::OpenedLaunchThread {
            launch_request_id: "launch-l".into(),
            native_session_id: "thread-l".into(),
        }
        .archives_alike(&generated::OpenedLaunchThread {
            launch_request_id: signal_flow::LaunchRequestId("launch-l".into()),
            native_session_id: signal_flow::NativeSessionId("thread-l".into()),
        });
        for (frozen_order, order) in [
            (
                frozen::ReplacementOrder::Refresh,
                generated::ReplacementOrder::Refresh,
            ),
            (
                frozen::ReplacementOrder::Restart,
                generated::ReplacementOrder::Restart,
            ),
        ] {
            frozen::OrderedReplacement {
                address: Address::sample("replaced"),
                predecessor: "flow-old".into(),
                order: frozen_order,
            }
            .archives_alike(&generated::OrderedReplacement {
                address: Address::sample("replaced"),
                predecessor: generated::Predecessor("flow-old".into()),
                replacement_order: order,
            });
        }
        for (frozen_cause, cause) in [
            (
                frozen::SuccessionCause::Refresh,
                generated::SuccessionCause::Refresh,
            ),
            (
                frozen::SuccessionCause::Exit,
                generated::SuccessionCause::Exit,
            ),
            (
                frozen::SuccessionCause::Ordered,
                generated::SuccessionCause::Ordered,
            ),
            (
                frozen::SuccessionCause::Replace,
                generated::SuccessionCause::Replace,
            ),
        ] {
            frozen::SuccessorIntent {
                address: Address::sample("succeeded"),
                predecessor: "flow-old".into(),
                cause: frozen_cause,
                launch_request_id: "launch-s".into(),
            }
            .archives_alike(&generated::SuccessorIntent {
                address: Address::sample("succeeded"),
                predecessor: generated::Predecessor("flow-old".into()),
                succession_cause: cause,
                launch_request_id: signal_flow::LaunchRequestId("launch-s".into()),
            });
        }
        let frame = signal_flow::UndecodableFrame {
            flow_id_option: Some(signal_flow::FlowId("flow-u".into())),
            seat_evidence: signal_flow::SeatEvidence {
                seat_pane_option: None,
                pane_label_option: None,
                native_session_id_option: None,
                observed_at: signal_flow::ObservedAt(1_700_000_000),
                process: signal_flow::Process {
                    pid: flow_ethos::Pid(99),
                    started: flow_ethos::Started(12),
                },
            },
        };
        frozen::UndecodableRecord {
            key: "000000001700000000000000000-0000000099".into(),
            frame: frame.clone(),
        }
        .archives_alike(&generated::UndecodableRecord {
            undecodable_key: generated::UndecodableKey(
                "000000001700000000000000000-0000000099".into(),
            ),
            undecodable_frame: frame,
        });
        frozen::AddressHold {
            flow_id: "flow-h".into(),
            address: Address::sample("held"),
            found: None,
        }
        .archives_alike(&generated::AddressHold {
            flow_id: signal_flow::FlowId("flow-h".into()),
            address: Address::sample("held"),
            metaflow_option: None,
        });
    }
}
