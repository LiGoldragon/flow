//! Configure declares one configuration family whole: the declaration
//! replaces the committed family in one commit and leaves every other row as
//! it was, so the effective family is always the last declaration. A refused
//! declaration changes nothing. The running Nexus takes a replaced value at
//! once, without a restart, and a subscription sees it.

use crate::generated::operation::{Operation, Outcome};
use crate::live::HoldsLiveValue;
use crate::performing::Performs;
use crate::store::metaflow::MutatesMetaflows;
use crate::store::{
    ConfigurationOnlyStore, ConfiguresOnlyStore, DefaultConfiguration, FlowStore,
    ManagesMetaflowState, OpensFlowStore, ReadsFlowRows, RegistersFlowIdentity, SeedsConfiguration,
};
use crate::{Dispatches, OpensRunningNexus, RunningNexus};
use meta_signal_flow::{
    ConfigurationChange, ConfigurationSnapshot, ConfigureHarnessFlags, ConfigureLeaveOut,
    ConfigureModel, ConfigureNexus, ConfigureOutcome, ConfigureRejection, ConfigureThreshold,
    HeartbeatConfig, Response as MetaResponse,
};
use signal_flow::{
    Address, Aspect, Bind_Data, EndpointSelection, FlowLifecycle, FlowNode, FlowOrigin,
    HarnessKind, HerdrRoute, HerdrRouteSelection, Lock_Data, Model, Origin, Process, Response,
};
use std::collections::BTreeSet;

/// This test process as Flow identifies a peer: its pid and its start time
/// in clock ticks, field 22 of `/proc/<pid>/stat`.
fn this_process() -> Process {
    let stat = std::fs::read_to_string("/proc/self/stat").expect("own stat");
    let (_, fields) = stat.rsplit_once(") ").expect("stat fields");
    let started = fields
        .split_whitespace()
        .nth(19)
        .and_then(|field| field.parse().ok())
        .map(flow_ethos::Started)
        .expect("start time");
    Process {
        pid: flow_ethos::Pid(i64::from(std::process::id())),
        started,
    }
}

#[test]
fn harness_flags_are_per_metaflow_persistent_and_absent_by_default() {
    let fixture = ConfiguredStore::new();
    let nexus = fixture.open();
    let address = Address {
        aspect: Aspect::Field,
        topic: flow_ethos::Topic("flagsTest".into()),
        model: Model::Sol,
    };
    assert!(
        nexus
            .store
            .configuration_snapshot()
            .unwrap()
            .unwrap()
            .configured_harness_flags
            .0
            .is_empty(),
        "an older configuration has the empty family"
    );
    assert_eq!(nexus.store.harness_flags(&address).unwrap(), "");
    assert_eq!(
        configure(
            &nexus,
            ConfigurationChange::HarnessFlags(vec![
                ConfigureHarnessFlags {
                    address: address.clone(),
                    harness_flag_text: meta_signal_flow::HarnessFlagText(
                        "--one 'two three'".into()
                    ),
                },
                ConfigureHarnessFlags {
                    address: address.clone(),
                    harness_flag_text: meta_signal_flow::HarnessFlagText("--duplicate".into()),
                },
            ])
        ),
        MetaResponse::ConfigureRejected(ConfigureRejection::DuplicateEntry)
    );
    assert_eq!(nexus.store.harness_flags(&address).unwrap(), "");
    assert_eq!(
        configure(
            &nexus,
            ConfigurationChange::HarnessFlags(vec![ConfigureHarnessFlags {
                address: address.clone(),
                harness_flag_text: meta_signal_flow::HarnessFlagText("--one 'two three'".into()),
            }])
        ),
        MetaResponse::Configured(ConfigureOutcome::Replaced)
    );
    assert_eq!(
        nexus.store.harness_flags(&address).unwrap(),
        "--one 'two three'"
    );
    drop(nexus);
    let reopened = fixture.open();
    assert_eq!(
        reopened.store.harness_flags(&address).unwrap(),
        "--one 'two three'"
    );
}

#[test]
fn heartbeat_configuration_requires_a_positive_period_and_unique_recipients() {
    let fixture = ConfiguredStore::new();
    let nexus = fixture.open();
    let recipient = Address {
        aspect: Aspect::Field,
        topic: flow_ethos::Topic("heartbeatTest".into()),
        model: Model::Sol,
    };
    assert_eq!(
        configure(
            &nexus,
            ConfigurationChange::Heartbeat(Some(HeartbeatConfig {
                heartbeat_period_seconds: meta_signal_flow::HeartbeatPeriodSeconds(60),
                heartbeat_recipients: meta_signal_flow::HeartbeatRecipients(vec![
                    recipient.clone(),
                    recipient
                ]),
            }))
        ),
        MetaResponse::ConfigureRejected(ConfigureRejection::DuplicateEntry),
    );
    assert_eq!(
        configure(
            &nexus,
            ConfigurationChange::Heartbeat(Some(HeartbeatConfig {
                heartbeat_period_seconds: meta_signal_flow::HeartbeatPeriodSeconds(0),
                heartbeat_recipients: meta_signal_flow::HeartbeatRecipients(vec![]),
            }))
        ),
        MetaResponse::ConfigureRejected(ConfigureRejection::StoreRefused),
    );
}

const TERRA: &str = "gpt-5.6-terra";
const SOL: &str = "gpt-5.6-sol";
const LUNA: &str = "gpt-5.6-luna";

pub(crate) struct ConfiguredStore {
    _directory: tempfile::TempDir,
    defaults: DefaultConfiguration,
    place: crate::place::Place,
    store_path: String,
}

impl ConfiguredStore {
    pub(crate) fn path(&self) -> &std::path::Path {
        std::path::Path::new(&self.store_path)
    }
    /// A configured store: Nexus, Models and Thresholds declared through
    /// Configure, a registered flow, Message bound and a lock held.
    pub(crate) fn new() -> Self {
        let directory = tempfile::tempdir().expect("temporary state");
        let defaults = DefaultConfiguration {
            home: directory.path().join("home"),
            runtime_directory: directory.path().join("runtime"),
        };
        let path = |name: &str| directory.path().join(name).to_string_lossy().into_owned();
        // The place keeps its memory at <State>/flow.sema.
        let place = crate::place::Place {
            place_name: crate::place::PlaceName("main".into()),
            runtime: crate::place::Runtime(path("runtime/nexus/main")),
            state: crate::place::State(
                directory.path().to_string_lossy().into_owned(),
            ),
        };
        let store_path = path("flow.sema");
        let configuration = ConfigurationOnlyStore::open(std::path::Path::new(&store_path))
            .expect("configuration store");
        for change in [
            ConfigurationChange::Nexus(Self::nexus(&defaults, &[TERRA, SOL, LUNA], 30)),
            ConfigurationChange::Models(vec![ConfigureModel {
                model: Model::Astra,
                native: meta_signal_flow::Native("gpt-6-astra".into()),
            }]),
            ConfigurationChange::Thresholds(vec![ConfigureThreshold {
                model_name: flow_ethos::ModelName("claude-fable-5-1".into()),
                refresh_threshold: meta_signal_flow::RefreshThreshold(200_000),
            }]),
        ] {
            configuration
                .configure_change(change)
                .expect("configuration commits");
        }
        drop(configuration);
        let store = FlowStore::open_configured(std::path::Path::new(&store_path))
            .expect("configured store opens");
        store
            .register_flow(Self::flow_node())
            .expect("flow registers");
        let peer = this_process();
        assert!(matches!(
            store
                .bind_metaflow(
                    Bind_Data {
                        address: Self::message(),
                        process: peer.clone(),
                    },
                    None,
                )
                .expect("bind reads"),
            Response::Bound(_)
        ));
        assert!(matches!(
            store
                .lock_metaflow(
                    Lock_Data {
                        first_address: Self::message(),
                        second_address: Self::message(),
                    },
                    100,
                )
                .expect("lock reads"),
            Response::Locked(_)
        ));
        drop(store);
        Self {
            _directory: directory,
            defaults,
            place,
            store_path,
        }
    }

    fn nexus(defaults: &DefaultConfiguration, stable: &[&str], lease: i64) -> ConfigureNexus {
        let settings = defaults.configuration();
        let mut stable_codex = settings.stable_codex;
        stable_codex.model_name_vector = stable
            .iter()
            .map(|model| flow_ethos::ModelName((*model).into()))
            .collect();
        ConfigureNexus {
            source_root: flow_ethos::SourceRoot(settings.source_root),
            first_codex_endpoint: stable_codex,
            second_codex_endpoint: settings.next_codex,
            harness_profiles: flow_ethos::HarnessProfiles(settings.harness_profile_vector),
            meta_aspects: flow_ethos::MetaAspects(settings.meta_aspects),
            message_nexus_path_option: Some(flow_ethos::MessageNexusPath(
                settings.message_nexus_path.clone(),
            )),
            message_nexus_binary_option: Some(flow_ethos::MessageNexusBinary(
                std::env::current_exe()
                    .expect("test executable")
                    .to_string_lossy()
                    .into_owned(),
            )),
            curriculum_nexus_path: meta_signal_flow::CurriculumNexusPath(
                "/fixture/curriculum.sock".into(),
            ),
            lease: flow_ethos::Lease(lease),
            presentation_prompt_option: None,
            herdr_session_name: signal_flow::HerdrSessionName("default".into()),
        }
    }

    fn message() -> Address {
        Address {
            aspect: Aspect::Field,
            topic: flow_ethos::Topic("message".into()),
            model: Model::Astra,
        }
    }

    fn flow_node() -> FlowNode {
        FlowNode {
            flow_id: flow_ethos::FlowId("da1e3f".into()),
            session_id: signal_flow::SessionId("claude-session".into()),
            harness_kind: HarnessKind::Claude,
            endpoint_selection: EndpointSelection::Unavailable,
            herdr_route_selection: HerdrRouteSelection::Available(HerdrRoute {
                herdr_session_name: signal_flow::HerdrSessionName("messaging-build".into()),
                herdr_agent_name: signal_flow::HerdrAgentName("recipient".into()),
                herdr_pane_id: signal_flow::HerdrPaneId("w1:p2".into()),
                herdr_terminal_id: signal_flow::HerdrTerminalId("term-current".into()),
            }),
            origin: Origin::Flow(FlowOrigin {
                flow_id: flow_ethos::FlowId("da1e3f".into()),
                session_id: signal_flow::SessionId("claude-session".into()),
            }),
            flow_lifecycle: FlowLifecycle::Active,
        }
    }

    fn open(&self) -> RunningNexus {
        RunningNexus::open(&self.place).expect("configured Nexus opens")
    }
}

/// Everything a store holds outside its Nexus value, read through the
/// operations that serve it.
#[derive(Debug, PartialEq)]
struct OtherRows {
    models: Vec<ConfigureModel>,
    thresholds: Vec<ConfigureThreshold>,
    flow: Option<FlowNode>,
    metaflows: Vec<signal_flow::Metaflow>,
    message_bound: bool,
    message_locked: bool,
}

fn other_rows(nexus: &RunningNexus) -> OtherRows {
    let ConfigurationSnapshot {
        configured_models: meta_signal_flow::ConfiguredModels(models),
        configured_thresholds: meta_signal_flow::ConfiguredThresholds(thresholds),
        ..
    } = snapshot(nexus);
    let peer = this_process();
    OtherRows {
        models,
        thresholds,
        flow: nexus.store.flow_node("da1e3f").expect("flow reads"),
        metaflows: nexus.store.list_metaflows().expect("metaflows read"),
        message_bound: nexus
            .store
            .bound_process(&ConfiguredStore::message())
            .expect("binding reads")
            == Some(peer),
        message_locked: nexus
            .store
            .stored_metaflow_lock(&ConfiguredStore::message())
            .expect("lock reads")
            .is_some(),
    }
}

fn snapshot(nexus: &RunningNexus) -> ConfigurationSnapshot {
    nexus
        .store
        .configuration_snapshot()
        .expect("snapshot reads")
        .expect("store configured")
}

fn stable_models(nexus: &RunningNexus) -> BTreeSet<String> {
    nexus.codex_endpoints.current().stable.model_names.clone()
}

fn configure(nexus: &RunningNexus, change: ConfigurationChange) -> MetaResponse {
    nexus.dispatch_meta(meta_signal_flow::Query::Configure(change))
}

fn quaternary_field() -> Address {
    Address {
        aspect: Aspect::Field,
        topic: flow_ethos::Topic("flow".into()),
        model: Model::Terra,
    }
}

/// A Configure value applied to a running Nexus takes effect without a
/// restart: the next launch reads the replaced Codex endpoints.
#[test]
fn configure_nexus_replaces_only_the_nexus_value_and_takes_effect_without_a_restart() {
    let fixture = ConfiguredStore::new();
    let nexus = fixture.open();
    let before = other_rows(&nexus);
    assert_eq!(before.models.len(), 1);
    assert_eq!(before.thresholds.len(), 1);
    assert!(before.flow.is_some() && before.message_bound && before.message_locked);
    assert_eq!(before.metaflows.len(), 1);

    let replaced = ConfiguredStore::nexus(&fixture.defaults, &[SOL, LUNA], 45);
    assert_eq!(
        configure(&nexus, ConfigurationChange::Nexus(replaced.clone())),
        MetaResponse::Configured(ConfigureOutcome::Replaced)
    );
    assert_eq!(
        snapshot(&nexus).configured_nexus.0,
        replaced,
        "the whole value replaced"
    );
    assert_eq!(other_rows(&nexus), before, "no other row changed");
    // The same declaration again writes nothing.
    assert_eq!(
        configure(&nexus, ConfigurationChange::Nexus(replaced.clone())),
        MetaResponse::Configured(ConfigureOutcome::Unchanged)
    );

    // The running Nexus serves the replaced endpoints at once.
    assert_eq!(
        stable_models(&nexus),
        BTreeSet::from([SOL.to_owned(), LUNA.to_owned()])
    );
    drop(nexus);

    // The next open serves the replaced value and still holds every row.
    let reopened = fixture.open();
    assert_eq!(
        stable_models(&reopened),
        BTreeSet::from([SOL.to_owned(), LUNA.to_owned()])
    );
    assert_eq!(snapshot(&reopened).configured_nexus.0, replaced);
    assert_eq!(other_rows(&reopened), before);
}

/// ObserveConfiguration sees the configuration on open, then the value a
/// Configure replaced, as it is applied: the subscriber sleeps until the
/// change is announced.
#[test]
fn a_subscription_sees_the_configuration_as_it_changes() {
    use crate::observe_configuration::ObservesConfiguration;
    use crate::store::AnnouncesLaunchChanges;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;
    let fixture = ConfiguredStore::new();
    let nexus = fixture.open();
    let replaced = ConfiguredStore::nexus(&fixture.defaults, &[SOL, LUNA], 45);
    let disconnected = AtomicBool::new(false);
    let (frames, received) = std::sync::mpsc::channel();
    std::thread::scope(|scope| {
        let observer = scope.spawn(|| {
            nexus.observe_configuration(&disconnected, &mut |response| {
                frames
                    .send(response.clone())
                    .map_err(|error| error.to_string())
            })
        });
        let MetaResponse::Configuration(first) = received
            .recv_timeout(Duration::from_secs(10))
            .expect("the configuration on open")
        else {
            panic!("a configured store answers its snapshot")
        };
        assert_ne!(first.configured_nexus.0, replaced);
        assert_eq!(
            configure(&nexus, ConfigurationChange::Nexus(replaced.clone())),
            MetaResponse::Configured(ConfigureOutcome::Replaced)
        );
        let MetaResponse::Configuration(second) = received
            .recv_timeout(Duration::from_secs(10))
            .expect("the configuration as it changed")
        else {
            panic!("the change is a snapshot")
        };
        assert_eq!(second.configured_nexus.0, replaced);
        disconnected.store(true, Ordering::Release);
        nexus.configuration_changes.announce();
        assert!(observer.join().expect("observer joins").is_err());
    });
}

#[test]
fn a_refused_declaration_changes_nothing() {
    let fixture = ConfiguredStore::new();
    let nexus = fixture.open();
    let before = snapshot(&nexus);
    let rows = other_rows(&nexus);
    let mut overlapping = ConfiguredStore::nexus(&fixture.defaults, &[SOL, LUNA], 45);
    overlapping
        .second_codex_endpoint
        .model_name_vector
        .push(flow_ethos::ModelName(LUNA.to_owned()));
    assert_eq!(
        configure(&nexus, ConfigurationChange::Nexus(overlapping)),
        MetaResponse::ConfigureRejected(ConfigureRejection::CodexModelsOverlap)
    );
    // One key named twice is not a family.
    let model = |native: &str| ConfigureModel {
        model: Model::Sol,
        native: meta_signal_flow::Native(native.into()),
    };
    assert_eq!(
        configure(
            &nexus,
            ConfigurationChange::Models(vec![model("claude-opus-5-5"), model("claude-sonnet-5-5")])
        ),
        MetaResponse::ConfigureRejected(ConfigureRejection::DuplicateEntry)
    );
    assert_eq!(snapshot(&nexus), before);
    assert_eq!(other_rows(&nexus), rows);
    drop(nexus);
    let reopened = fixture.open();
    assert_eq!(snapshot(&reopened), before);
    assert_eq!(other_rows(&reopened), rows);
}

#[test]
fn each_family_is_its_last_declaration() {
    let fixture = ConfiguredStore::new();
    let nexus = fixture.open();
    let nexus_before = snapshot(&nexus).configured_nexus;

    // In the order the snapshot reads them, by Model name.
    let models = vec![
        ConfigureModel {
            model: Model::Haiku,
            native: meta_signal_flow::Native("claude-haiku-5-5".into()),
        },
        ConfigureModel {
            model: Model::Opus,
            native: meta_signal_flow::Native("claude-opus-5-5".into()),
        },
        ConfigureModel {
            model: Model::Sol,
            native: meta_signal_flow::Native(SOL.into()),
        },
    ];
    assert_eq!(
        configure(&nexus, ConfigurationChange::Models(models.clone())),
        MetaResponse::Configured(ConfigureOutcome::Replaced)
    );
    // The Primary model the store held is gone: it is not declared.
    assert_eq!(snapshot(&nexus).configured_models.0, models);
    assert_eq!(
        configure(&nexus, ConfigurationChange::Models(models.clone())),
        MetaResponse::Configured(ConfigureOutcome::Unchanged)
    );

    let thresholds = vec![
        ConfigureThreshold {
            model_name: flow_ethos::ModelName("claude-opus-5-5".into()),
            refresh_threshold: meta_signal_flow::RefreshThreshold(200_000),
        },
        ConfigureThreshold {
            model_name: flow_ethos::ModelName("gpt-6.1-sol".into()),
            refresh_threshold: meta_signal_flow::RefreshThreshold(150_000),
        },
    ];
    assert_eq!(
        configure(&nexus, ConfigurationChange::Thresholds(thresholds.clone())),
        MetaResponse::Configured(ConfigureOutcome::Replaced)
    );
    assert_eq!(snapshot(&nexus).configured_thresholds.0, thresholds);

    let first = vec![
        ConfigureLeaveOut {
            address: quaternary_field(),
            harness_kind: HarnessKind::Claude,
            leave_outs: signal_flow::LeaveOuts(vec![signal_flow::LeaveOut::SkillTool]),
        },
        ConfigureLeaveOut {
            address: ConfiguredStore::message(),
            harness_kind: HarnessKind::Claude,
            leave_outs: signal_flow::LeaveOuts(vec![signal_flow::LeaveOut::ClaudeMd]),
        },
    ];
    assert_eq!(
        configure(&nexus, ConfigurationChange::LeaveOuts(first)),
        MetaResponse::Configured(ConfigureOutcome::Replaced)
    );
    let second = vec![ConfigureLeaveOut {
        address: quaternary_field(),
        harness_kind: HarnessKind::Claude,
        leave_outs: signal_flow::LeaveOuts(vec![
            signal_flow::LeaveOut::SkillTool,
            signal_flow::LeaveOut::WorkflowTool,
        ]),
    }];
    assert_eq!(
        configure(&nexus, ConfigurationChange::LeaveOuts(second.clone())),
        MetaResponse::Configured(ConfigureOutcome::Replaced)
    );
    assert_eq!(snapshot(&nexus).default_leave_outs.0, second);
    assert!(
        nexus
            .store
            .default_leave_outs(&ConfiguredStore::message(), &HarnessKind::Claude)
            .unwrap()
            .is_empty()
    );
    // A fresh store trusts no send server; a declaration names each pair.
    assert!(snapshot(&nexus).configured_send_servers.0.is_empty());
    let send_servers = vec![meta_signal_flow::ConfigureSendServer {
        codex_app_server_binary: meta_signal_flow::CodexAppServerBinary(
            "/nix/store/codex/bin/codex".into(),
        ),
        mcp_send_server_binary: meta_signal_flow::McpSendServerBinary(
            "/nix/store/send/bin/flow-send-mcp".into(),
        ),
    }];
    assert_eq!(
        configure(
            &nexus,
            ConfigurationChange::SendServers(send_servers.clone())
        ),
        MetaResponse::Configured(ConfigureOutcome::Replaced)
    );
    assert_eq!(snapshot(&nexus).configured_send_servers.0, send_servers);
    // The effective snapshot equals the last declaration of every family,
    // across a restart.
    drop(nexus);
    let reopened = fixture.open();
    assert_eq!(
        snapshot(&reopened),
        ConfigurationSnapshot {
            configured_nexus: nexus_before,
            configured_models: meta_signal_flow::ConfiguredModels(models),
            configured_thresholds: meta_signal_flow::ConfiguredThresholds(thresholds),
            default_leave_outs: meta_signal_flow::DefaultLeaveOuts(second),
            configured_send_servers: meta_signal_flow::ConfiguredSendServers(send_servers),
            configured_metaflows: meta_signal_flow::ConfiguredMetaflows(Vec::new()),
            configured_harness_flags: meta_signal_flow::ConfiguredHarnessFlags(Vec::new()),
            configured_investigation: meta_signal_flow::ConfiguredInvestigation(None),
            configured_default_minutes: meta_signal_flow::ConfiguredDefaultMinutes(
                flow_ethos::Minutes(crate::store::SEEDED_DEFAULT_MINUTES,)
            ),
            configured_heartbeat: meta_signal_flow::ConfiguredHeartbeat(None),
            configured_protected_addresses: meta_signal_flow::ConfiguredProtectedAddresses(vec![]),
            configured_handover_policies: meta_signal_flow::ConfiguredHandoverPolicies(
                meta_signal_flow::HandoverPoliciesConfiguration::Unconfigured
            ),
            configured_handover_archive: meta_signal_flow::ConfiguredHandoverArchive(
                meta_signal_flow::HandoverArchiveConfiguration::Unconfigured
            ),
            configured_handover_count: meta_signal_flow::ConfiguredHandoverCount(
                meta_signal_flow::HandoverCountConfiguration::Unconfigured
            ),
            configured_publication_deadline: meta_signal_flow::ConfiguredPublicationDeadline(
                meta_signal_flow::Duration(60_000)
            ),
            configured_counters: meta_signal_flow::ConfiguredCounters(
                meta_signal_flow::ConfigureCounters {
                    logical_delivery: meta_signal_flow::LogicalDelivery(1),
                    queue_attempt: meta_signal_flow::QueueAttempt(3),
                    send_outcome: meta_signal_flow::SendOutcome(1),
                }
            ),
            configured_request_id_draws: meta_signal_flow::ConfiguredRequestIdDraws(
                meta_signal_flow::DrawBound(64)
            ),
            configured_announcement_tries: meta_signal_flow::ConfiguredAnnouncementTries(
                meta_signal_flow::TryBound(3)
            ),
            configured_harness_hook: meta_signal_flow::ConfiguredHarnessHook(
                meta_signal_flow::HarnessHookConfiguration::Unconfigured
            ),
        }
    );
    // An empty declaration empties the family.
    assert_eq!(
        configure(&reopened, ConfigurationChange::LeaveOuts(Vec::new())),
        MetaResponse::Configured(ConfigureOutcome::Replaced)
    );
    assert!(snapshot(&reopened).default_leave_outs.0.is_empty());
    assert_eq!(
        configure(&reopened, ConfigurationChange::LeaveOuts(Vec::new())),
        MetaResponse::Configured(ConfigureOutcome::Unchanged)
    );
}

#[test]
fn a_launch_after_a_replaced_declaration_takes_the_new_defaults() {
    let fixture = ConfiguredStore::new();
    let mut nexus = fixture.open();
    *nexus.composer.edit() = crate::composition::tests::fixture_composer();
    let mut profile = crate::composition::tests::fixture_profile();
    profile.address = quaternary_field();
    // A first launch: it names no predecessor, so no handover is read under
    // the store's root.
    profile.flow_id_option = None;
    profile.model_name = flow_ethos::ModelName("claude-haiku-5-5".into());
    let composed_leave_outs =
        |nexus: &RunningNexus| match nexus.perform(Operation::Compose(profile.clone())) {
            Outcome::Composed(launch) => launch.leave_outs.0,
            outcome => panic!("launch not composed: {outcome:?}"),
        };
    let declare = |nexus: &RunningNexus, leave_outs: Vec<signal_flow::LeaveOut>| {
        configure(
            nexus,
            ConfigurationChange::LeaveOuts(vec![ConfigureLeaveOut {
                address: quaternary_field(),
                harness_kind: HarnessKind::Claude,
                leave_outs: signal_flow::LeaveOuts(leave_outs),
            }]),
        )
    };
    assert_eq!(
        declare(&nexus, vec![signal_flow::LeaveOut::WorkflowTool]),
        MetaResponse::Configured(ConfigureOutcome::Replaced)
    );
    assert_eq!(
        composed_leave_outs(&nexus),
        [signal_flow::LeaveOut::WorkflowTool]
    );
    // The changed default the deployment declares is the one the next launch
    // takes.
    assert_eq!(
        declare(
            &nexus,
            vec![
                signal_flow::LeaveOut::WorkflowTool,
                signal_flow::LeaveOut::SkillTool,
                signal_flow::LeaveOut::ClaudeMd
            ]
        ),
        MetaResponse::Configured(ConfigureOutcome::Replaced)
    );
    assert_eq!(
        composed_leave_outs(&nexus),
        [
            signal_flow::LeaveOut::WorkflowTool,
            signal_flow::LeaveOut::SkillTool,
            signal_flow::LeaveOut::ClaudeMd
        ]
    );
}

/// A launch names its model exactly: the transcript's model is compared with
/// it string for string. A Claude alias, which Claude Code resolves without
/// telling, and a name carrying a context suffix are not composed, so no
/// pane is opened and no prompt is typed for them; the full name the
/// transcript records is composed; a model a Codex endpoint lists is exact
/// by that list.
#[test]
fn a_launch_naming_a_claude_alias_is_not_composed_and_its_full_name_is() {
    use crate::generated::starter::DerivesHarness;
    let fixture = ConfiguredStore::new();
    let mut nexus = fixture.open();
    *nexus.composer.edit() = crate::composition::tests::fixture_composer();
    let mut profile = crate::composition::tests::fixture_profile();
    profile.address = quaternary_field();
    // A first launch: it names no predecessor, so no handover is read under
    // the store's root.
    profile.flow_id_option = None;
    for inexact in [
        "haiku",
        "claude-opus-5-5[1m]",
        "Claude-Haiku-5-5",
        "claude-",
    ] {
        profile.model_name = flow_ethos::ModelName(inexact.into());
        assert_eq!(
            nexus.perform(Operation::Compose(profile.clone())),
            Outcome::Failed(
                crate::generated::operation::Failed_Data::CompositionRefused(
                    signal_flow::CompositionRefusal::InexactModel
                )
            ),
            "{inexact}"
        );
    }
    profile.model_name = flow_ethos::ModelName("claude-haiku-5-5".into());
    assert!(matches!(
        nexus.perform(Operation::Compose(profile.clone())),
        Outcome::Composed(_)
    ));
    assert_eq!(
        nexus.codex_endpoints.current().harness_for(SOL),
        HarnessKind::Codex
    );
    assert!(nexus.codex_endpoints.current().launches_exactly(SOL));
    assert!(
        !nexus
            .codex_endpoints
            .current()
            .launches_exactly("gpt-6-unlisted")
    );
}

/// An entry naming a leave-out of another harness than its own is refused
/// whole: the declaration writes nothing, and the committed family stands.
#[test]
fn a_leave_out_default_naming_another_harness_is_refused_and_writes_nothing() {
    let fixture = ConfiguredStore::new();
    let nexus = fixture.open();
    let committed = vec![ConfigureLeaveOut {
        address: quaternary_field(),
        harness_kind: HarnessKind::Claude,
        leave_outs: signal_flow::LeaveOuts(vec![signal_flow::LeaveOut::ClaudeMd]),
    }];
    assert_eq!(
        configure(&nexus, ConfigurationChange::LeaveOuts(committed.clone())),
        MetaResponse::Configured(ConfigureOutcome::Replaced)
    );
    let before = snapshot(&nexus);
    let rows = other_rows(&nexus);
    for crossed in [
        ConfigureLeaveOut {
            address: ConfiguredStore::message(),
            harness_kind: HarnessKind::Codex,
            leave_outs: signal_flow::LeaveOuts(vec![
                signal_flow::LeaveOut::CodexAgentsMd,
                signal_flow::LeaveOut::ClaudeMd,
            ]),
        },
        ConfigureLeaveOut {
            address: ConfiguredStore::message(),
            harness_kind: HarnessKind::Claude,
            leave_outs: signal_flow::LeaveOuts(vec![signal_flow::LeaveOut::CodexSkillListing]),
        },
    ] {
        let declaration = vec![
            ConfigureLeaveOut {
                address: quaternary_field(),
                harness_kind: HarnessKind::Codex,
                leave_outs: signal_flow::LeaveOuts(vec![signal_flow::LeaveOut::CodexAgentsMd]),
            },
            crossed,
        ];
        assert_eq!(
            configure(&nexus, ConfigurationChange::LeaveOuts(declaration)),
            MetaResponse::ConfigureRejected(ConfigureRejection::LeaveOutNotForHarness)
        );
        assert_eq!(snapshot(&nexus), before);
        assert_eq!(other_rows(&nexus), rows);
    }
    // The configuration-only listener refuses the same entry the same way.
    drop(nexus);
    let configuration = ConfigurationOnlyStore::open(std::path::Path::new(&fixture.store_path))
        .expect("stopped store reopens");
    assert!(matches!(
        configuration.configure_change(ConfigurationChange::LeaveOuts(vec![ConfigureLeaveOut {
            address: quaternary_field(),
            harness_kind: HarnessKind::Codex,
            leave_outs: signal_flow::LeaveOuts(vec![signal_flow::LeaveOut::SkillTool]),
        }])),
        Err(crate::store::StoreError::LeaveOutNotForHarness)
    ));
    assert_eq!(
        configuration
            .configuration_snapshot()
            .expect("reads")
            .expect("configured")
            .default_leave_outs,
        committed
    );
}

/// A Start naming no leave-outs takes its metaflow's default for the
/// harness it starts on: a Codex Start whose metaflow has only a Claude
/// default takes none, and a Claude Start of the same metaflow takes that
/// default.
#[test]
fn a_codex_start_naming_none_takes_no_claude_default() {
    let fixture = ConfiguredStore::new();
    let mut nexus = fixture.open();
    *nexus.composer.edit() = crate::composition::tests::fixture_composer();
    assert_eq!(
        configure(
            &nexus,
            ConfigurationChange::LeaveOuts(vec![ConfigureLeaveOut {
                address: quaternary_field(),
                harness_kind: HarnessKind::Claude,
                leave_outs: signal_flow::LeaveOuts(vec![
                    signal_flow::LeaveOut::WorkflowTool,
                    signal_flow::LeaveOut::ClaudeMd,
                ]),
            }]),
        ),
        MetaResponse::Configured(ConfigureOutcome::Replaced)
    );
    let mut profile = crate::composition::tests::fixture_profile();
    profile.address = quaternary_field();
    // A first launch: it names no predecessor, so no handover is read under
    // the store's root.
    profile.flow_id_option = None;
    profile.leave_outs_option = None;
    profile.model_name = flow_ethos::ModelName(SOL.into());
    match nexus.perform(Operation::Compose(profile.clone())) {
        Outcome::Composed(launch) => {
            assert_eq!(launch.harness_kind, HarnessKind::Codex);
            assert!(launch.leave_outs.0.is_empty(), "{:?}", launch.leave_outs);
        }
        outcome => panic!("Codex launch not composed: {outcome:?}"),
    }
    profile.model_name = flow_ethos::ModelName("claude-haiku-5-5".into());
    match nexus.perform(Operation::Compose(profile)) {
        Outcome::Composed(launch) => assert_eq!(
            launch.leave_outs.0,
            [
                signal_flow::LeaveOut::WorkflowTool,
                signal_flow::LeaveOut::ClaudeMd
            ]
        ),
        outcome => panic!("Claude launch not composed: {outcome:?}"),
    }
}

#[test]
fn a_fresh_store_takes_its_first_nexus_declaration() {
    let directory = tempfile::tempdir().expect("temporary state");
    let defaults = DefaultConfiguration {
        home: directory.path().join("home"),
        runtime_directory: directory.path().join("runtime"),
    };
    let path = directory.path().join("flow.sema");
    let store = ConfigurationOnlyStore::open(&path).expect("fresh store");
    assert_eq!(store.configuration_snapshot().expect("reads"), None);
    let declared = ConfiguredStore::nexus(&defaults, &[SOL, LUNA], 45);
    assert_eq!(
        store
            .configure_change(ConfigurationChange::Nexus(declared.clone()))
            .expect("first declaration commits"),
        ConfigureOutcome::Replaced
    );
    assert_eq!(
        store
            .configuration_snapshot()
            .expect("reads")
            .map(|snapshot| snapshot.nexus),
        Some(declared)
    );
}

#[test]
fn a_store_configured_while_stopped_opens_with_the_replaced_value() {
    let fixture = ConfiguredStore::new();
    let path = std::path::Path::new(&fixture.store_path);
    let rows = other_rows(&fixture.open());
    let replaced = ConfiguredStore::nexus(&fixture.defaults, &[SOL, LUNA], 30);
    let configuration = ConfigurationOnlyStore::open(path).expect("stopped store reopens");
    assert_eq!(
        configuration
            .configure_change(ConfigurationChange::Nexus(replaced.clone()))
            .expect("stopped store takes the declaration"),
        ConfigureOutcome::Replaced
    );
    drop(configuration);
    let nexus = fixture.open();
    assert_eq!(snapshot(&nexus).configured_nexus.0, replaced);
    assert_eq!(other_rows(&nexus), rows);
    assert!(!stable_models(&nexus).contains(TERRA));
}

/// The default Minutes is a configuration family of its own, kept in
/// Memory: a new Memory holds the constant Flow seeds it with, Configure
/// replaces it whole and a reopen keeps it, and the Minutes rule a Task
/// meets at admission refuses a zero, a negative or an unreachable default
/// without changing it. Read and written as datom text, as flow-nexus-meta does.
#[test]
fn configure_default_minutes_is_seeded_declared_kept_and_ruled() {
    use datom_codec::{Actualizing, Budget, Datomizable, Potential};
    use protos::{Compactable, Protosizable, ReaderBudget};
    let ask = |nexus: &RunningNexus, text: &str| -> String {
        let mut budget = Budget {
            remaining: 65536,
            reader: ReaderBudget::try_from(65536).expect("a constant reader budget"),
            depth: 0,
            maximum_depth: 1024,
        };
        let query = Potential::<meta_signal_flow::Query>::from(text)
            .actualize(&mut budget)
            .unwrap_or_else(|error| panic!("{text}: {error:?}"));
        nexus
            .dispatch_meta(query)
            .datomize(vec![])
            .protosize()
            .compact()
    };
    let fixture = ConfiguredStore::new();
    let nexus = fixture.open();
    let seeded = ask(&nexus, "Configuration");
    assert!(
        seeded.ends_with(" None 30 None [] { 1 3 1 } 64 3 Unconfigured }"),
        "seeded at 30: {seeded}"
    );
    assert_eq!(
        ask(&nexus, "Configure.DefaultMinutes.45"),
        "Configured.Replaced"
    );
    assert_eq!(
        ask(&nexus, "Configure.DefaultMinutes.45"),
        "Configured.Unchanged"
    );
    for (declared, refused) in [
        ("0", "Zero"),
        ("-5", "Negative"),
        ("9223372036854775807", "Unreachable"),
    ] {
        assert_eq!(
            ask(&nexus, &format!("Configure.DefaultMinutes.{declared}")),
            format!("ConfigureRejected.MinutesRefused.{refused}")
        );
    }
    let declared = ask(&nexus, "Configuration");
    assert!(
        declared.ends_with(" None 45 None [] { 1 3 1 } 64 3 Unconfigured }"),
        "{declared}"
    );
    drop(nexus);
    let reopened = fixture.open();
    let kept = ask(&reopened, "Configuration");
    assert!(
        kept.ends_with(" None 45 None [] { 1 3 1 } 64 3 Unconfigured }"),
        "kept across a reopen: {kept}"
    );
}

/// The counters a Configure declared are kept in Memory: a reopen reads
/// them back as the snapshot's last field and allocates from them.
#[test]
fn configured_counters_are_kept_across_a_reopen() {
    use datom_codec::{Actualizing, Budget, Datomizable, Potential};
    use protos::{Compactable, Protosizable, ReaderBudget};
    let ask = |nexus: &RunningNexus, text: &str| -> String {
        let mut budget = Budget {
            remaining: 65536,
            reader: ReaderBudget::try_from(65536).expect("a constant reader budget"),
            depth: 0,
            maximum_depth: 1024,
        };
        let query = Potential::<meta_signal_flow::Query>::from(text)
            .actualize(&mut budget)
            .unwrap_or_else(|error| panic!("{text}: {error:?}"));
        nexus
            .dispatch_meta(query)
            .datomize(vec![])
            .protosize()
            .compact()
    };
    let fixture = ConfiguredStore::new();
    let nexus = fixture.open();
    assert_eq!(
        ask(&nexus, "Configure.Counters.{ 120 45 300 }"),
        "Configured.Replaced"
    );
    drop(nexus);
    let reopened = fixture.open();
    let kept = ask(&reopened, "Configuration");
    assert!(
        kept.ends_with(" None 30 None [] { 120 45 300 } 64 3 Unconfigured }"),
        "kept across a reopen: {kept}"
    );
    assert_eq!(
        ask(&reopened, "Configure.Counters.{ 119 45 300 }"),
        "ConfigureRejected.CountersRefused.Lowered.{ 120 45 300 }"
    );
}

/// ProtectedAddresses is one family: declared whole, one entry per Address,
/// read back in Address order, and kept across a reopen. An Address named
/// twice, or one whose topic is not camelCase, is refused and changes
/// nothing.
#[test]
fn protected_addresses_persist_across_reopen() {
    let fixture = ConfiguredStore::new();
    let nexus = fixture.open();
    let fable = Address {
        aspect: Aspect::Psyche,
        topic: flow_ethos::Topic("core".into()),
        model: Model::Fable,
    };
    let other = Address {
        aspect: Aspect::Mind,
        topic: flow_ethos::Topic("versionZero".into()),
        model: Model::Astra,
    };
    assert_eq!(
        snapshot(&nexus).configured_protected_addresses,
        meta_signal_flow::ConfiguredProtectedAddresses(Vec::<Address>::new())
    );
    assert_eq!(
        configure(
            &nexus,
            ConfigurationChange::ProtectedAddresses(vec![fable.clone(), other.clone()])
        ),
        MetaResponse::Configured(ConfigureOutcome::Replaced)
    );
    assert_eq!(
        configure(
            &nexus,
            ConfigurationChange::ProtectedAddresses(vec![other.clone(), fable.clone()])
        ),
        MetaResponse::Configured(ConfigureOutcome::Unchanged)
    );
    assert_eq!(
        configure(
            &nexus,
            ConfigurationChange::ProtectedAddresses(vec![fable.clone(), fable.clone()])
        ),
        MetaResponse::ConfigureRejected(ConfigureRejection::DuplicateEntry)
    );
    assert_eq!(
        configure(
            &nexus,
            ConfigurationChange::ProtectedAddresses(vec![Address {
                topic: flow_ethos::Topic("Not-camel".into()),
                ..fable.clone()
            }])
        ),
        MetaResponse::ConfigureRejected(ConfigureRejection::TopicRefused(flow_ethos::Topic(
            "Not-camel".into()
        )))
    );
    let declared = snapshot(&nexus).configured_protected_addresses;
    assert_eq!(declared.0.len(), 2);
    assert!(declared.0.contains(&fable) && declared.0.contains(&other));
    drop(nexus);
    let reopened = fixture.open();
    assert_eq!(snapshot(&reopened).configured_protected_addresses, declared);
    assert_eq!(
        configure(
            &reopened,
            ConfigurationChange::ProtectedAddresses(vec![fable.clone()])
        ),
        MetaResponse::Configured(ConfigureOutcome::Replaced)
    );
    drop(reopened);
    assert_eq!(
        snapshot(&fixture.open()).configured_protected_addresses,
        meta_signal_flow::ConfiguredProtectedAddresses(vec![fable])
    );
}

#[test]
fn request_policy_fields_replace_independently_validate_and_survive_reopen() {
    let fixture = ConfiguredStore::new();
    let nexus = fixture.open();
    let seeded = snapshot(&nexus);
    assert_eq!(
        seeded.configured_request_id_draws,
        meta_signal_flow::ConfiguredRequestIdDraws(meta_signal_flow::DrawBound(64))
    );
    assert_eq!(
        seeded.configured_announcement_tries,
        meta_signal_flow::ConfiguredAnnouncementTries(meta_signal_flow::TryBound(3))
    );

    assert_eq!(
        configure(
            &nexus,
            ConfigurationChange::AnnouncementTries(meta_signal_flow::TryBound(7))
        ),
        MetaResponse::Configured(ConfigureOutcome::Replaced)
    );
    let after_tries = snapshot(&nexus);
    assert_eq!(
        after_tries.configured_request_id_draws,
        meta_signal_flow::ConfiguredRequestIdDraws(meta_signal_flow::DrawBound(64))
    );
    assert_eq!(
        after_tries.configured_announcement_tries,
        meta_signal_flow::ConfiguredAnnouncementTries(meta_signal_flow::TryBound(7))
    );

    assert_eq!(
        configure(
            &nexus,
            ConfigurationChange::RequestIdDraws(meta_signal_flow::DrawBound(19))
        ),
        MetaResponse::Configured(ConfigureOutcome::Replaced)
    );
    let after_draws = snapshot(&nexus);
    assert_eq!(
        after_draws.configured_request_id_draws,
        meta_signal_flow::ConfiguredRequestIdDraws(meta_signal_flow::DrawBound(19))
    );
    assert_eq!(
        after_draws.configured_announcement_tries,
        meta_signal_flow::ConfiguredAnnouncementTries(meta_signal_flow::TryBound(7))
    );
    assert_eq!(
        configure(
            &nexus,
            ConfigurationChange::RequestIdDraws(meta_signal_flow::DrawBound(0))
        ),
        MetaResponse::ConfigureRejected(ConfigureRejection::RequestIdDrawsRefused)
    );
    assert_eq!(
        configure(
            &nexus,
            ConfigurationChange::AnnouncementTries(meta_signal_flow::TryBound(-1))
        ),
        MetaResponse::ConfigureRejected(ConfigureRejection::AnnouncementTriesRefused)
    );
    assert_eq!(snapshot(&nexus), after_draws);
    drop(nexus);
    let reopened = fixture.open();
    assert_eq!(snapshot(&reopened), after_draws);

    assert_eq!(
        configure(
            &reopened,
            ConfigurationChange::RequestIdDraws(meta_signal_flow::DrawBound(23))
        ),
        MetaResponse::Configured(ConfigureOutcome::Replaced)
    );
    assert_eq!(
        configure(
            &reopened,
            ConfigurationChange::AnnouncementTries(meta_signal_flow::TryBound(5))
        ),
        MetaResponse::Configured(ConfigureOutcome::Replaced)
    );
    let opposite = snapshot(&reopened);
    assert_eq!(
        opposite.configured_request_id_draws,
        meta_signal_flow::ConfiguredRequestIdDraws(meta_signal_flow::DrawBound(23))
    );
    assert_eq!(
        opposite.configured_announcement_tries,
        meta_signal_flow::ConfiguredAnnouncementTries(meta_signal_flow::TryBound(5))
    );
}
