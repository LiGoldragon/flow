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
}
