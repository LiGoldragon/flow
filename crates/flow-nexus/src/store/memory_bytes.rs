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
            harness_hook: "/run/current-system/sw/bin/flow-nexus-hook".into(),
        }
        .archives_alike(&generated::StoredMetaHarnessHookConfiguration {
            path: meta_signal_flow::generated::signal::Path(
                "/run/current-system/sw/bin/flow-nexus-hook".into(),
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
}
