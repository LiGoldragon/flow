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
    ConfigurationChange, ConfigurationSnapshot, ConfigureHarnessFlags, ConfigureMetaflow,
    ConfigureModel, ConfigureNexus, ConfigureRefusal, ConfigureThreshold, HeartbeatConfig,
    Output as MetaResponse,
};
use signal_flow::{
    Address, Aspect, Bind_Data, EndpointSelection, FlowLifecycle, FlowNode, FlowOrigin,
    HarnessKind, HerdrRoute, HerdrRouteSelection, Lock_Data, Model, Origin, Output, Process,
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
        MetaResponse::ConfigureRefused(ConfigureRefusal::DuplicateEntry)
    );
    assert_eq!(nexus.store.harness_flags(&address).unwrap(), "");
    configures(
        &nexus,
        ConfigurationChange::HarnessFlags(vec![ConfigureHarnessFlags {
            address: address.clone(),
            harness_flag_text: meta_signal_flow::HarnessFlagText("--one 'two three'".into()),
        }]),
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
        MetaResponse::ConfigureRefused(ConfigureRefusal::DuplicateEntry),
    );
    assert_eq!(
        configure(
            &nexus,
            ConfigurationChange::Heartbeat(Some(HeartbeatConfig {
                heartbeat_period_seconds: meta_signal_flow::HeartbeatPeriodSeconds(0),
                heartbeat_recipients: meta_signal_flow::HeartbeatRecipients(vec![]),
            }))
        ),
        MetaResponse::ConfigureRefused(ConfigureRefusal::StoreRefused),
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
            place_name: crate::generated::library::PlaceName("main".into()),
            runtime: crate::generated::library::Runtime(path("runtime/nexus/main")),
            state: crate::generated::library::State(
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
            Output::Bound(_)
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
            Output::Locked(_)
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
    nexus.dispatch_meta(meta_signal_flow::Input::Configure(change))
}

/// A Configure the Nexus takes is answered Configured with the change it
/// was given, echoed whole.
fn configures(nexus: &RunningNexus, change: ConfigurationChange) {
    assert_eq!(
        configure(nexus, change.clone()),
        MetaResponse::Configured(change)
    );
}

/// The store's Memory file, byte for byte as it stands.
fn store_bytes(fixture: &ConfiguredStore) -> Vec<u8> {
    std::fs::read(fixture.path()).expect("the store file reads")
}

/// A Configure that changes nothing is answered Configured with its change
/// echoed, as every Configure is, and writes nothing: the store's file is
/// byte-identical after it.
fn configures_nothing(
    fixture: &ConfiguredStore,
    nexus: &RunningNexus,
    change: ConfigurationChange,
) {
    let before = store_bytes(fixture);
    configures(nexus, change);
    assert!(
        store_bytes(fixture) == before,
        "the store is byte-identical"
    );
}

/// A permaflow at `address` with `leave_outs`, a versioned system module
/// and nothing else.
pub(crate) fn permaflow(
    address: Address,
    leave_outs: Vec<signal_flow::LeaveOut>,
) -> ConfigureMetaflow {
    ConfigureMetaflow {
        address,
        root_option: None,
        leave_outs: signal_flow::LeaveOuts(leave_outs),
        system_modules: signal_flow::SystemModules(vec![signal_flow::ModuleIdentity(
            "spirit:core:0".into(),
        )]),
        prompt_modules: signal_flow::PromptModules(Vec::new()),
        flow_kind: signal_flow::FlowKind::Regular,
        tool_set: signal_flow::ToolSet::Default,
        continuity: meta_signal_flow::Continuity::OnDemand,
        purpose: meta_signal_flow::Purpose(String::new()),
        records: meta_signal_flow::Records(Vec::new()),
    }
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
    configures(&nexus, ConfigurationChange::Nexus(replaced.clone()));
    assert_eq!(
        snapshot(&nexus).configured_nexus.0,
        replaced,
        "the whole value replaced"
    );
    assert_eq!(other_rows(&nexus), before, "no other row changed");
    // The same declaration again writes nothing.
    configures_nothing(
        &fixture,
        &nexus,
        ConfigurationChange::Nexus(replaced.clone()),
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
        let nexus = &nexus;
        let node = &node;
        let observer = scope.spawn(move || {
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
        configures(&nexus, ConfigurationChange::Nexus(replaced.clone()));
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
        MetaResponse::ConfigureRefused(ConfigureRefusal::CodexModelsOverlap)
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
        MetaResponse::ConfigureRefused(ConfigureRefusal::DuplicateEntry)
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
    configures(&nexus, ConfigurationChange::Models(models.clone()));
    // The Primary model the store held is gone: it is not declared.
    assert_eq!(snapshot(&nexus).configured_models.0, models);
    configures_nothing(
        &fixture,
        &nexus,
        ConfigurationChange::Models(models.clone()),
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
    configures(&nexus, ConfigurationChange::Thresholds(thresholds.clone()));
    assert_eq!(snapshot(&nexus).configured_thresholds.0, thresholds);

    // A permaflow carries its leave-outs: the Metaflows family declared
    // again replaces each permaflow whole, and one it leaves out is gone
    // with its leave-outs.
    let first = vec![
        permaflow(quaternary_field(), vec![signal_flow::LeaveOut::SkillTool]),
        permaflow(
            ConfiguredStore::message(),
            vec![signal_flow::LeaveOut::ClaudeMd],
        ),
    ];
    configures(&nexus, ConfigurationChange::Metaflows(first));
    let second = vec![permaflow(
        quaternary_field(),
        vec![
            signal_flow::LeaveOut::SkillTool,
            signal_flow::LeaveOut::WorkflowTool,
        ],
    )];
    configures(&nexus, ConfigurationChange::Metaflows(second.clone()));
    assert_eq!(snapshot(&nexus).configured_metaflows.0, second);
    assert!(
        nexus
            .store
            .declared_leave_outs(&ConfiguredStore::message())
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
    configures(
        &nexus,
        ConfigurationChange::SendServers(send_servers.clone()),
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
            configured_send_servers: meta_signal_flow::ConfiguredSendServers(send_servers),
            configured_metaflows: meta_signal_flow::ConfiguredMetaflows(second),
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
            configured_context_warning_policies: meta_signal_flow::ConfiguredContextWarningPolicies(
                vec![]
            ),
            configured_harness_hook_events: meta_signal_flow::ConfiguredHarnessHookEvents(vec![]),
            configured_harness_hook: meta_signal_flow::ConfiguredHarnessHook(
                meta_signal_flow::HarnessHookConfiguration::Unconfigured
            ),
        }
    );
    // An empty declaration empties the family.
    configures(&reopened, ConfigurationChange::Metaflows(Vec::new()));
    assert!(snapshot(&reopened).configured_metaflows.0.is_empty());
    configures_nothing(
        &fixture,
        &reopened,
        ConfigurationChange::Metaflows(Vec::new()),
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
    configures(
        &nexus,
        ConfigurationChange::Permaflow(permaflow(
            quaternary_field(),
            vec![signal_flow::LeaveOut::WorkflowTool],
        )),
    );
    assert_eq!(
        composed_leave_outs(&nexus),
        [signal_flow::LeaveOut::WorkflowTool]
    );
    // The leave-outs an Amend adds are the ones the next launch takes.
    for leave_out in [
        signal_flow::LeaveOut::SkillTool,
        signal_flow::LeaveOut::ClaudeMd,
    ] {
        configures(
            &nexus,
            ConfigurationChange::Amend(meta_signal_flow::Amend_Data {
                address: quaternary_field(),
                change: meta_signal_flow::Change::Add(meta_signal_flow::Element::LeaveOut(
                    leave_out,
                )),
            }),
        );
    }
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

/// A permaflow naming a leave-out of another harness than its Model runs
/// on is refused UnknownFeature, naming it, whether declared whole, in the
/// Metaflows family or added by an Amend: nothing is written. While the
/// Model has no native model configured the harness is not known and the
/// leave-outs are taken as declared.
#[test]
fn a_leave_out_of_another_harness_is_an_unknown_feature_and_writes_nothing() {
    let fixture = ConfiguredStore::new();
    let nexus = fixture.open();
    // Sol runs a model the stable Codex endpoint lists: on Codex.
    configures(
        &nexus,
        ConfigurationChange::Models(vec![
            ConfigureModel {
                model: Model::Astra,
                native: meta_signal_flow::Native("gpt-6-astra".into()),
            },
            ConfigureModel {
                model: Model::Sol,
                native: meta_signal_flow::Native(SOL.into()),
            },
        ]),
    );
    let codex = Address {
        aspect: Aspect::Mind,
        topic: flow_ethos::Topic("codexSeat".into()),
        model: Model::Sol,
    };
    configures(
        &nexus,
        ConfigurationChange::Permaflow(permaflow(
            codex.clone(),
            vec![signal_flow::LeaveOut::CodexAgentsMd],
        )),
    );
    let before = snapshot(&nexus);
    let rows = other_rows(&nexus);
    for refused in [
        ConfigurationChange::Permaflow(permaflow(
            codex.clone(),
            vec![
                signal_flow::LeaveOut::CodexAgentsMd,
                signal_flow::LeaveOut::ClaudeMd,
            ],
        )),
        ConfigurationChange::Metaflows(vec![
            permaflow(quaternary_field(), vec![signal_flow::LeaveOut::SkillTool]),
            permaflow(codex.clone(), vec![signal_flow::LeaveOut::ClaudeMd]),
        ]),
        ConfigurationChange::Amend(meta_signal_flow::Amend_Data {
            address: codex.clone(),
            change: meta_signal_flow::Change::Add(meta_signal_flow::Element::LeaveOut(
                signal_flow::LeaveOut::ClaudeMd,
            )),
        }),
    ] {
        assert_eq!(
            configure(&nexus, refused),
            MetaResponse::ConfigureRefused(ConfigureRefusal::UnknownFeature(
                signal_flow::LeaveOut::ClaudeMd
            ))
        );
        assert_eq!(snapshot(&nexus), before);
        assert_eq!(other_rows(&nexus), rows);
    }
    // Terra has no native model: its harness is not known yet.
    configures(
        &nexus,
        ConfigurationChange::Permaflow(permaflow(
            quaternary_field(),
            vec![signal_flow::LeaveOut::ClaudeMd],
        )),
    );
    // The configuration-only listener refuses the same entry the same way.
    drop(nexus);
    let configuration = ConfigurationOnlyStore::open(std::path::Path::new(&fixture.store_path))
        .expect("stopped store reopens");
    let committed = configuration
        .configuration_snapshot()
        .expect("reads")
        .expect("configured")
        .metaflows;
    assert!(matches!(
        configuration.configure_change(ConfigurationChange::Permaflow(permaflow(
            codex.clone(),
            vec![signal_flow::LeaveOut::SkillTool]
        ))),
        Err(crate::store::StoreError::UnknownFeature(
            signal_flow::LeaveOut::SkillTool
        ))
    ));
    assert_eq!(
        configuration
            .configuration_snapshot()
            .expect("reads")
            .expect("configured")
            .metaflows,
        committed
    );
}

/// A Start naming no leave-outs takes its permaflow's, which are of the
/// harness its Model runs on. A Start on the other harness, which only a
/// Models or Nexus change since the declaration makes, is refused rather
/// than started without them; a Start naming its own replaces them.
#[test]
fn a_start_takes_its_permaflow_s_leave_outs_and_another_harness_refuses_them() {
    let fixture = ConfiguredStore::new();
    let mut nexus = fixture.open();
    *nexus.composer.edit() = crate::composition::tests::fixture_composer();
    configures(
        &nexus,
        ConfigurationChange::Permaflow(permaflow(
            quaternary_field(),
            vec![
                signal_flow::LeaveOut::WorkflowTool,
                signal_flow::LeaveOut::ClaudeMd,
            ],
        )),
    );
    let mut profile = crate::composition::tests::fixture_profile();
    profile.address = quaternary_field();
    // A first launch: it names no predecessor, so no handover is read under
    // the store's root.
    profile.flow_id_option = None;
    profile.leave_outs_option = None;
    profile.model_name = flow_ethos::ModelName(SOL.into());
    assert_eq!(
        nexus.perform(Operation::Compose(profile.clone())),
        Outcome::Failed(crate::generated::operation::Failed_Data::LeaveOutRefused(
            signal_flow::LeaveOutRefusal::NotForHarness(signal_flow::LeaveOut::WorkflowTool)
        ))
    );
    let mut own = profile.clone();
    own.leave_outs_option = Some(signal_flow::LeaveOuts(vec![
        signal_flow::LeaveOut::CodexAgentsMd,
    ]));
    match nexus.perform(Operation::Compose(own)) {
        Outcome::Composed(launch) => {
            assert_eq!(launch.harness_kind, HarnessKind::Codex);
            assert_eq!(launch.leave_outs.0, [signal_flow::LeaveOut::CodexAgentsMd]);
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
        crate::store::ConfigurationCommit::Written
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
        crate::store::ConfigurationCommit::Written
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
        let query = Potential::<meta_signal_flow::Input>::from(text)
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
        "Configured.DefaultMinutes.45"
    );
    // The same declaration again answers the same echo and writes nothing.
    let stored = store_bytes(&fixture);
    assert_eq!(
        ask(&nexus, "Configure.DefaultMinutes.45"),
        "Configured.DefaultMinutes.45"
    );
    assert!(
        store_bytes(&fixture) == stored,
        "the store is byte-identical"
    );
    for (declared, refused) in [
        ("0", "Zero"),
        ("-5", "Negative"),
        ("9223372036854775807", "Unreachable"),
    ] {
        assert_eq!(
            ask(&nexus, &format!("Configure.DefaultMinutes.{declared}")),
            format!("ConfigureRefused.MinutesRefused.{refused}")
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
        let query = Potential::<meta_signal_flow::Input>::from(text)
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
        "Configured.Counters.{ 120 45 300 }"
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
        "ConfigureRefused.CountersRefused.Lowered.{ 120 45 300 }"
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
    configures(
        &nexus,
        ConfigurationChange::ProtectedAddresses(vec![fable.clone(), other.clone()]),
    );
    configures_nothing(
        &fixture,
        &nexus,
        ConfigurationChange::ProtectedAddresses(vec![other.clone(), fable.clone()]),
    );
    assert_eq!(
        configure(
            &nexus,
            ConfigurationChange::ProtectedAddresses(vec![fable.clone(), fable.clone()])
        ),
        MetaResponse::ConfigureRefused(ConfigureRefusal::DuplicateEntry)
    );
    assert_eq!(
        configure(
            &nexus,
            ConfigurationChange::ProtectedAddresses(vec![Address {
                topic: flow_ethos::Topic("Not-camel".into()),
                ..fable.clone()
            }])
        ),
        MetaResponse::ConfigureRefused(ConfigureRefusal::TopicRefused(flow_ethos::Topic(
            "Not-camel".into()
        )))
    );
    let declared = snapshot(&nexus).configured_protected_addresses;
    assert_eq!(declared.0.len(), 2);
    assert!(declared.0.contains(&fable) && declared.0.contains(&other));
    drop(nexus);
    let reopened = fixture.open();
    assert_eq!(snapshot(&reopened).configured_protected_addresses, declared);
    configures(
        &reopened,
        ConfigurationChange::ProtectedAddresses(vec![fable.clone()]),
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

    configures(
        &nexus,
        ConfigurationChange::AnnouncementTries(meta_signal_flow::TryBound(7)),
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

    configures(
        &nexus,
        ConfigurationChange::RequestIdDraws(meta_signal_flow::DrawBound(19)),
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
        MetaResponse::ConfigureRefused(ConfigureRefusal::RequestIdDrawsRefused)
    );
    assert_eq!(
        configure(
            &nexus,
            ConfigurationChange::AnnouncementTries(meta_signal_flow::TryBound(-1))
        ),
        MetaResponse::ConfigureRefused(ConfigureRefusal::AnnouncementTriesRefused)
    );
    assert_eq!(snapshot(&nexus), after_draws);
    drop(nexus);
    let reopened = fixture.open();
    assert_eq!(snapshot(&reopened), after_draws);

    configures(
        &reopened,
        ConfigurationChange::RequestIdDraws(meta_signal_flow::DrawBound(23)),
    );
    configures(
        &reopened,
        ConfigurationChange::AnnouncementTries(meta_signal_flow::TryBound(5)),
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

/// The two permaflows the Amend tests change one value of and keep: Alpha
/// on Codex (Sol runs a model the stable Codex endpoint lists), Beta on
/// Claude (Opus runs a Claude model). Models declared to say so.
fn amend_world() -> (
    ConfiguredStore,
    RunningNexus,
    ConfigureMetaflow,
    ConfigureMetaflow,
) {
    let fixture = ConfiguredStore::new();
    let nexus = fixture.open();
    configures(
        &nexus,
        ConfigurationChange::Models(vec![
            ConfigureModel {
                model: Model::Astra,
                native: meta_signal_flow::Native("gpt-6-astra".into()),
            },
            ConfigureModel {
                model: Model::Opus,
                native: meta_signal_flow::Native("claude-opus-5-5".into()),
            },
            ConfigureModel {
                model: Model::Sol,
                native: meta_signal_flow::Native(SOL.into()),
            },
        ]),
    );
    let mut alpha = permaflow(
        Address {
            aspect: Aspect::Mind,
            topic: flow_ethos::Topic("atomicTrialAlpha".into()),
            model: Model::Sol,
        },
        vec![signal_flow::LeaveOut::CodexAgentsMd],
    );
    alpha.purpose = meta_signal_flow::Purpose("Alpha before".into());
    alpha.prompt_modules =
        signal_flow::PromptModules(vec![signal_flow::ModuleIdentity("knowledge:flow:0".into())]);
    let mut beta = permaflow(
        Address {
            aspect: Aspect::Psyche,
            topic: flow_ethos::Topic("atomicTrialBeta".into()),
            model: Model::Opus,
        },
        vec![signal_flow::LeaveOut::ClaudeMd],
    );
    beta.purpose = meta_signal_flow::Purpose("Beta fixed".into());
    for declared in [&alpha, &beta] {
        configures(&nexus, ConfigurationChange::Permaflow(declared.clone()));
    }
    (fixture, nexus, alpha, beta)
}

fn amend(address: &Address, change: meta_signal_flow::Change) -> ConfigurationChange {
    ConfigurationChange::Amend(meta_signal_flow::Amend_Data {
        address: address.clone(),
        change,
    })
}

/// The snapshot `before` with one permaflow replaced by `changed`.
fn with_permaflow(
    before: &ConfigurationSnapshot,
    changed: &ConfigureMetaflow,
) -> ConfigurationSnapshot {
    let mut expected = before.clone();
    let row = expected
        .configured_metaflows
        .0
        .iter_mut()
        .find(|row| row.address == changed.address)
        .expect("the permaflow is declared");
    *row = changed.clone();
    expected
}

/// One Amend's Change and what it does to the permaflow it names.
type AmendStep = (
    meta_signal_flow::Change,
    Box<dyn Fn(&mut ConfigureMetaflow)>,
);

/// Configure.Amend changes one value of one permaflow alone: each Change
/// changes exactly its one field of that permaflow, and the whole rest of
/// the store, the other permaflow, every other family and the running
/// rows, reads as it did. The change is the declaration a next flow reads.
#[test]
fn each_amend_changes_exactly_its_one_value_and_nothing_else() {
    use meta_signal_flow::{Change, Element};
    let (_fixture, mut nexus, alpha, beta) = amend_world();
    *nexus.composer.edit() = crate::composition::tests::fixture_composer();
    let mut expected = alpha.clone();
    let steps: Vec<AmendStep> = vec![
        (
            Change::Purpose(meta_signal_flow::Purpose("Alpha after".into())),
            Box::new(|row: &mut ConfigureMetaflow| {
                row.purpose = meta_signal_flow::Purpose("Alpha after".into())
            }),
        ),
        (
            Change::Workspace(Some(meta_signal_flow::Root("/srv/alpha".into()))),
            Box::new(|row: &mut ConfigureMetaflow| {
                row.root_option = Some(meta_signal_flow::Root("/srv/alpha".into()))
            }),
        ),
        (
            Change::Kind(signal_flow::FlowKind::Simplified),
            Box::new(|row: &mut ConfigureMetaflow| {
                row.flow_kind = signal_flow::FlowKind::Simplified
            }),
        ),
        (
            Change::Continuity(meta_signal_flow::Continuity::Maintained),
            Box::new(|row: &mut ConfigureMetaflow| {
                row.continuity = meta_signal_flow::Continuity::Maintained
            }),
        ),
        (
            Change::Add(Element::SystemModule(signal_flow::ModuleIdentity(
                "intent:flow:0".into(),
            ))),
            Box::new(|row: &mut ConfigureMetaflow| {
                row.system_modules
                    .0
                    .push(signal_flow::ModuleIdentity("intent:flow:0".into()))
            }),
        ),
        (
            Change::Add(Element::PromptModule(signal_flow::ModuleIdentity(
                "operation:main-flow:0".into(),
            ))),
            Box::new(|row: &mut ConfigureMetaflow| {
                row.prompt_modules
                    .0
                    .push(signal_flow::ModuleIdentity("operation:main-flow:0".into()))
            }),
        ),
        (
            Change::Add(Element::LeaveOut(signal_flow::LeaveOut::CodexSkillListing)),
            Box::new(|row: &mut ConfigureMetaflow| {
                row.leave_outs
                    .0
                    .push(signal_flow::LeaveOut::CodexSkillListing)
            }),
        ),
        (
            Change::Remove(Element::PromptModule(signal_flow::ModuleIdentity(
                "knowledge:flow:0".into(),
            ))),
            Box::new(|row: &mut ConfigureMetaflow| {
                row.prompt_modules
                    .0
                    .retain(|held| held.0 != "knowledge:flow:0")
            }),
        ),
        (
            Change::Remove(Element::SystemModule(signal_flow::ModuleIdentity(
                "spirit:core:0".into(),
            ))),
            Box::new(|row: &mut ConfigureMetaflow| {
                row.system_modules
                    .0
                    .retain(|held| held.0 != "spirit:core:0")
            }),
        ),
        (
            Change::Remove(Element::LeaveOut(signal_flow::LeaveOut::CodexAgentsMd)),
            Box::new(|row: &mut ConfigureMetaflow| {
                row.leave_outs
                    .0
                    .retain(|held| held != &signal_flow::LeaveOut::CodexAgentsMd)
            }),
        ),
        (
            Change::Workspace(None),
            Box::new(|row: &mut ConfigureMetaflow| row.root_option = None),
        ),
    ];
    for (change, apply) in steps {
        let before = snapshot(&nexus);
        let rows = other_rows(&nexus);
        let amended = amend(&alpha.address, change.clone());
        assert_eq!(
            configure(&nexus, amended.clone()),
            MetaResponse::Configured(amended),
            "{change:?}"
        );
        apply(&mut expected);
        let after = snapshot(&nexus);
        assert_eq!(after, with_permaflow(&before, &expected), "{change:?}");
        assert_eq!(other_rows(&nexus), rows, "{change:?}");
        // The other permaflow, byte for byte as it was declared.
        let beta_after = after
            .configured_metaflows
            .0
            .iter()
            .find(|row| row.address == beta.address)
            .expect("beta stays");
        assert_eq!(
            rkyv::to_bytes::<rkyv::rancor::Error>(beta_after)
                .unwrap()
                .as_slice(),
            rkyv::to_bytes::<rkyv::rancor::Error>(&beta)
                .unwrap()
                .as_slice()
        );
        // The declaration a next flow of Alpha starts from is the amended one.
        {
            use crate::store::metaflow_declaration::ReadsMetaflowDeclarations;
            assert_eq!(
                nexus.store.metaflow_declaration(&alpha.address).unwrap(),
                Some(expected.clone())
            );
        }
        assert_eq!(
            nexus.store.declared_leave_outs(&alpha.address).unwrap(),
            expected.leave_outs.0
        );
    }
}

/// An Amend that would change nothing writes nothing, leaving the store
/// byte-identical, and still answers Configured with the Amend echoed: a
/// value the permaflow already holds, an element added that it holds, an
/// element removed that it does not.
#[test]
fn an_amend_that_changes_nothing_answers_its_echo_and_writes_nothing() {
    use meta_signal_flow::{Change, Element};
    let (fixture, nexus, alpha, _beta) = amend_world();
    let before = snapshot(&nexus);
    let rows = other_rows(&nexus);
    for change in [
        Change::Purpose(alpha.purpose.clone()),
        Change::Workspace(None),
        Change::Kind(signal_flow::FlowKind::Regular),
        Change::Continuity(meta_signal_flow::Continuity::OnDemand),
        // Held already: Curriculum is not asked (none is reachable here).
        Change::Add(Element::SystemModule(signal_flow::ModuleIdentity(
            "spirit:core:0".into(),
        ))),
        Change::Add(Element::PromptModule(signal_flow::ModuleIdentity(
            "knowledge:flow:0".into(),
        ))),
        Change::Add(Element::LeaveOut(signal_flow::LeaveOut::CodexAgentsMd)),
        Change::Remove(Element::SystemModule(signal_flow::ModuleIdentity(
            "vision:absent:0".into(),
        ))),
        Change::Remove(Element::PromptModule(signal_flow::ModuleIdentity(
            "vision:absent:0".into(),
        ))),
        Change::Remove(Element::LeaveOut(signal_flow::LeaveOut::CodexPermissions)),
    ] {
        let amended = amend(&alpha.address, change.clone());
        let stored = store_bytes(&fixture);
        assert_eq!(
            configure(&nexus, amended.clone()),
            MetaResponse::Configured(amended),
            "{change:?}"
        );
        assert!(
            store_bytes(&fixture) == stored,
            "{change:?}: the store is byte-identical"
        );
        assert_eq!(snapshot(&nexus), before, "{change:?}");
        assert_eq!(other_rows(&nexus), rows, "{change:?}");
    }
}

/// Each refusal of an Amend changes nothing: no permaflow at the Address,
/// a topic that is not camelCase, a module Curriculum does not hold or an
/// identity without its version, a Curriculum that cannot be asked, a
/// leave-out of another harness.
#[test]
fn each_amend_refusal_changes_nothing() {
    use meta_signal_flow::{Change, Element};
    let (_fixture, mut nexus, alpha, _beta) = amend_world();
    let before = snapshot(&nexus);
    let rows = other_rows(&nexus);
    let absent = signal_flow::ModuleIdentity("vision:absent:0".into());
    let refusals = || {
        vec![
            (
                amend(
                    &Address {
                        aspect: Aspect::Mind,
                        topic: flow_ethos::Topic("atomicTrialGamma".into()),
                        model: Model::Luna,
                    },
                    Change::Purpose(meta_signal_flow::Purpose("Gamma".into())),
                ),
                ConfigureRefusal::UnknownPermaflow(Address {
                    aspect: Aspect::Mind,
                    topic: flow_ethos::Topic("atomicTrialGamma".into()),
                    model: Model::Luna,
                }),
            ),
            (
                amend(
                    &Address {
                        aspect: Aspect::Mind,
                        topic: flow_ethos::Topic("Atomic_trial".into()),
                        model: Model::Luna,
                    },
                    Change::Purpose(meta_signal_flow::Purpose("Gamma".into())),
                ),
                ConfigureRefusal::TopicRefused(flow_ethos::Topic("Atomic_trial".into())),
            ),
            (
                amend(
                    &alpha.address,
                    Change::Add(Element::LeaveOut(signal_flow::LeaveOut::GitInstructions)),
                ),
                ConfigureRefusal::UnknownFeature(signal_flow::LeaveOut::GitInstructions),
            ),
            (
                amend(
                    &alpha.address,
                    Change::Add(Element::PromptModule(signal_flow::ModuleIdentity(
                        "knowledge:flow".into(),
                    ))),
                ),
                ConfigureRefusal::UnknownModule(signal_flow::ModuleIdentity(
                    "knowledge:flow".into(),
                )),
            ),
        ]
    };
    let refuse_each = |nexus: &RunningNexus| {
        for (change, rejection) in refusals() {
            assert_eq!(
                configure(nexus, change),
                MetaResponse::ConfigureRefused(rejection)
            );
        }
    };
    refuse_each(&nexus);
    // The Curriculum socket the fixture configures cannot be reached.
    assert_eq!(
        configure(
            &nexus,
            amend(
                &alpha.address,
                Change::Add(Element::SystemModule(absent.clone()))
            )
        ),
        MetaResponse::ConfigureRefused(ConfigureRefusal::CurriculumUnavailable)
    );
    // A Curriculum that answers, but does not hold the module.
    *nexus.composer.edit() = crate::composition::tests::fixture_composer();
    for change in [
        Change::Add(Element::SystemModule(absent.clone())),
        Change::Add(Element::PromptModule(absent.clone())),
    ] {
        assert_eq!(
            configure(&nexus, amend(&alpha.address, change)),
            MetaResponse::ConfigureRefused(ConfigureRefusal::UnknownModule(absent.clone()))
        );
    }
    refuse_each(&nexus);
    assert_eq!(snapshot(&nexus), before);
    assert_eq!(other_rows(&nexus), rows);
    // The store asked with no composer cannot resolve a module either.
    assert!(matches!(
        nexus.store.configure_change(amend(
            &alpha.address,
            Change::Add(Element::SystemModule(signal_flow::ModuleIdentity(
                "intent:flow:0".into()
            )))
        )),
        Err(crate::store::StoreError::CurriculumUnavailable)
    ));
    assert_eq!(snapshot(&nexus), before);
}

use crate::store::leave_out_upgrade::{
    ConfigureLeaveOutV3, ConfigureMetaflowV6, DescribesLeaveOutUpgrade, LeaveOutDropped,
    StoredMetaLeaveOutConfigurationV3, StoredMetaMetaflowConfigurationV6, UpgradesLeaveOutMemory,
};

/// The permaflows of the upgrade test, as 0.54.7 declared them: Alpha on
/// Codex, Beta on Claude, Delta on Terra, whose Model has no native model.
fn v6_permaflows() -> Vec<ConfigureMetaflowV6> {
    [
        (Aspect::Mind, "atomicTrialAlpha", Model::Sol),
        (Aspect::Psyche, "atomicTrialBeta", Model::Opus),
        (Aspect::Field, "atomicTrialDelta", Model::Terra),
    ]
    .into_iter()
    .map(|(aspect, topic, model)| ConfigureMetaflowV6 {
        address: Address {
            aspect,
            topic: flow_ethos::Topic(topic.into()),
            model,
        },
        root_option: None,
        system_modules: signal_flow::SystemModules(vec![signal_flow::ModuleIdentity(
            "spirit:core:0".into(),
        )]),
        prompt_modules: signal_flow::PromptModules(Vec::new()),
        flow_kind: signal_flow::FlowKind::Regular,
        tool_set: signal_flow::ToolSet::Default,
        continuity: meta_signal_flow::Continuity::OnDemand,
        purpose: meta_signal_flow::Purpose(format!("{topic} as 0.54.7 declared it")),
        records: meta_signal_flow::Records(Vec::new()),
    })
    .collect()
}

/// The leave-out family as 0.54.7 held it for those permaflows.
fn v3_leave_outs() -> Vec<ConfigureLeaveOutV3> {
    let address = |aspect, topic: &str, model| Address {
        aspect,
        topic: flow_ethos::Topic(topic.into()),
        model,
    };
    vec![
        ConfigureLeaveOutV3 {
            address: address(Aspect::Mind, "atomicTrialAlpha", Model::Sol),
            harness_kind: HarnessKind::Codex,
            leave_outs: signal_flow::LeaveOuts(vec![
                signal_flow::LeaveOut::CodexAgentsMd,
                signal_flow::LeaveOut::CodexSkillListing,
            ]),
        },
        ConfigureLeaveOutV3 {
            address: address(Aspect::Mind, "atomicTrialAlpha", Model::Sol),
            harness_kind: HarnessKind::Claude,
            leave_outs: signal_flow::LeaveOuts(vec![signal_flow::LeaveOut::ClaudeMd]),
        },
        ConfigureLeaveOutV3 {
            address: address(Aspect::Psyche, "atomicTrialBeta", Model::Opus),
            harness_kind: HarnessKind::Claude,
            leave_outs: signal_flow::LeaveOuts(vec![
                signal_flow::LeaveOut::ClaudeMd,
                signal_flow::LeaveOut::GitInstructions,
            ]),
        },
        ConfigureLeaveOutV3 {
            address: address(Aspect::Psyche, "atomicTrialGone", Model::Opus),
            harness_kind: HarnessKind::Claude,
            leave_outs: signal_flow::LeaveOuts(vec![signal_flow::LeaveOut::ClaudeMd]),
        },
        ConfigureLeaveOutV3 {
            address: address(Aspect::Field, "atomicTrialDelta", Model::Terra),
            harness_kind: HarnessKind::Claude,
            leave_outs: signal_flow::LeaveOuts(vec![signal_flow::LeaveOut::ClaudeMd]),
        },
    ]
}

/// A configured store, closed and rewritten as 0.54.7 left it: its
/// permaflow family at v6 holding `v6_permaflows`, and the leave-out family
/// at v3 holding `v3_leave_outs`. Models name Sol a Codex model and Opus a
/// Claude one; Terra none.
fn v6_store() -> ConfiguredStore {
    use sema_engine::{Assertion, Engine, EngineOpen, SchemaVersion};
    let fixture = ConfiguredStore::new();
    {
        let configuration = ConfigurationOnlyStore::open(fixture.path()).expect("store opens");
        configuration
            .configure_change(ConfigurationChange::Models(vec![
                ConfigureModel {
                    model: Model::Opus,
                    native: meta_signal_flow::Native("claude-opus-5-5".into()),
                },
                ConfigureModel {
                    model: Model::Sol,
                    native: meta_signal_flow::Native(SOL.into()),
                },
            ]))
            .expect("models declared");
    }
    {
        let database = redb::Database::open(fixture.path()).expect("the store opens raw");
        let write = database.begin_write().unwrap();
        {
            let mut catalog = write
                .open_table(redb::TableDefinition::<&str, &[u8]>::new(
                    "__sema_engine_catalog",
                ))
                .unwrap();
            catalog
                .remove("flow_nexus_meta_metaflow_configuration")
                .unwrap();
        }
        let _ = write.delete_table(redb::TableDefinition::<String, &[u8]>::new(
            "flow_nexus_meta_metaflow_configuration",
        ));
        write.commit().unwrap();
    }
    let mut engine = Engine::open(EngineOpen::new(
        fixture.path(),
        SchemaVersion::new(crate::store::FLOW_SCHEMA_VERSION),
    ))
    .unwrap();
    let permaflows = engine
        .register_table(sema_engine::TableDescriptor::<
            StoredMetaMetaflowConfigurationV6,
        >::new(
            sema_engine::TableName::new("flow_nexus_meta_metaflow_configuration"),
            sema_engine::FamilyName::new("flow-nexus-meta-metaflow-configuration"),
            <StoredMetaMetaflowConfigurationV6 as sema_engine::MemoryDefinition>::schema_hash(),
        ))
        .unwrap();
    for metaflow in v6_permaflows() {
        engine
            .assert(Assertion::new(
                permaflows,
                StoredMetaMetaflowConfigurationV6 { metaflow },
            ))
            .unwrap();
    }
    let leave_outs = engine
        .register_table(
            <crate::store::FlowStore as DescribesLeaveOutUpgrade>::retired_leave_out_descriptor(),
        )
        .unwrap();
    for leave_out in v3_leave_outs() {
        engine
            .assert(Assertion::new(
                leave_outs,
                StoredMetaLeaveOutConfigurationV3 { leave_out },
            ))
            .unwrap();
    }
    fixture
}

/// The retired leave-out family's rows, read from the closed store.
fn retired_rows(path: &std::path::Path) -> usize {
    use sema_engine::{Engine, EngineOpen, QueryPlan, SchemaVersion};
    let mut engine = Engine::open(EngineOpen::new(
        path,
        SchemaVersion::new(crate::store::FLOW_SCHEMA_VERSION),
    ))
    .unwrap();
    let table = engine
        .register_table(
            <crate::store::FlowStore as DescribesLeaveOutUpgrade>::retired_leave_out_descriptor(),
        )
        .unwrap();
    engine
        .match_records(QueryPlan::all(table))
        .unwrap()
        .records()
        .len()
}

/// The 0.54.7-to-0.55.0 upgrade, from a store holding the old shape: each
/// permaflow takes the leave-out entry of its Address for the harness its
/// Model runs on; an entry with no permaflow, one for the other harness,
/// and one whose permaflow's harness cannot be derived are dropped and
/// named; the retired family is emptied in the same commit; a second open
/// changes nothing.
#[test]
fn a_v6_store_folds_each_permaflow_s_leave_outs_and_names_what_it_drops() {
    use sema_engine::{Engine, EngineOpen, SchemaVersion};
    let fixture = v6_store();
    let fold = {
        let mut engine = Engine::open(
            EngineOpen::new(
                fixture.path(),
                SchemaVersion::new(crate::store::FLOW_SCHEMA_VERSION),
            )
            .with_family(
                &<crate::store::FlowStore as DescribesLeaveOutUpgrade>::evolving_metaflow_descriptor(),
            ),
        )
        .expect("the v6 store opens with the v7 family declared");
        <crate::store::FlowStore as UpgradesLeaveOutMemory>::upgrade_leave_out_memory(&mut engine)
            .expect("the upgrade commits")
    };
    let alpha = v6_permaflows()[0].address.clone();
    let beta = v6_permaflows()[1].address.clone();
    let delta = v6_permaflows()[2].address.clone();
    let gone = v3_leave_outs()[3].address.clone();
    assert_eq!(
        fold.folded,
        vec![
            (alpha.clone(), HarnessKind::Codex),
            (beta.clone(), HarnessKind::Claude)
        ]
    );
    // In the retired family's key order.
    assert_eq!(
        fold.dropped,
        vec![
            (
                delta.clone(),
                HarnessKind::Claude,
                LeaveOutDropped::HarnessUnknown
            ),
            (
                alpha.clone(),
                HarnessKind::Claude,
                LeaveOutDropped::OtherHarness(HarnessKind::Codex)
            ),
            (gone, HarnessKind::Claude, LeaveOutDropped::NoPermaflow),
        ]
    );
    assert_eq!(retired_rows(fixture.path()), 0);
    let nexus = fixture.open();
    let leave_outs = |address: &Address| nexus.store.declared_leave_outs(address).unwrap();
    assert_eq!(
        leave_outs(&alpha),
        [
            signal_flow::LeaveOut::CodexAgentsMd,
            signal_flow::LeaveOut::CodexSkillListing
        ]
    );
    assert_eq!(
        leave_outs(&beta),
        [
            signal_flow::LeaveOut::ClaudeMd,
            signal_flow::LeaveOut::GitInstructions
        ]
    );
    assert!(leave_outs(&delta).is_empty());
    // Every other field of every permaflow is as 0.54.7 declared it.
    let metaflows = snapshot(&nexus).configured_metaflows.0;
    for declared in v6_permaflows() {
        let upgraded = metaflows
            .iter()
            .find(|row| row.address == declared.address)
            .expect("each permaflow stays");
        assert_eq!(upgraded.root_option, declared.root_option);
        assert_eq!(upgraded.purpose, declared.purpose);
        assert_eq!(upgraded.system_modules, declared.system_modules);
        assert_eq!(upgraded.records, declared.records);
    }
    drop(nexus);
    // A second open finds the store current and changes nothing.
    let reopened = fixture.open();
    assert_eq!(snapshot(&reopened).configured_metaflows.0, metaflows);
}

/// The configuration-only open upgrades the same store by itself, before
/// any declaration is replayed.
#[test]
fn the_configuration_open_upgrades_a_v6_store_too() {
    let fixture = v6_store();
    let configuration =
        ConfigurationOnlyStore::open(fixture.path()).expect("the v6 store opens and upgrades");
    let metaflows = configuration
        .configuration_snapshot()
        .expect("reads")
        .expect("configured")
        .metaflows;
    assert_eq!(metaflows.len(), 3);
    let alpha = metaflows
        .iter()
        .find(|row| row.address.topic.0 == "atomicTrialAlpha")
        .unwrap();
    assert_eq!(
        alpha.leave_outs.0,
        [
            signal_flow::LeaveOut::CodexAgentsMd,
            signal_flow::LeaveOut::CodexSkillListing
        ]
    );
    drop(configuration);
    assert_eq!(retired_rows(fixture.path()), 0);
}

#[test]
fn warning_configuration_is_validated_persisted_and_removed_per_model() {
    use crate::context_warnings::tests::fixture_policy;
    use meta_signal_flow::{ContextWarningConfigurationRefusal, HarnessHookEvent, Removal};

    let fixture = ConfiguredStore::new();
    let nexus = fixture.open();
    let initial = snapshot(&nexus);
    assert!(initial.configured_context_warning_policies.0.is_empty());
    assert!(initial.configured_harness_hook_events.0.is_empty());
    let policy = fixture_policy();
    configures(
        &nexus,
        ConfigurationChange::ContextWarningPolicy(policy.clone()),
    );
    let mut other = policy.clone();
    other.model_name = signal_flow::ModelName("another-model".into());
    configures(
        &nexus,
        ConfigurationChange::ContextWarningPolicy(other.clone()),
    );
    let mut replacement = policy.clone();
    replacement.context_warning_limit.warning_tokens.0 += 1;
    configures(
        &nexus,
        ConfigurationChange::ContextWarningPolicy(replacement.clone()),
    );
    let before_refusal = snapshot(&nexus);
    let mut invalid = policy.clone();
    invalid.context_warning_limit.warning_tokens.0 = 0;
    assert_eq!(
        configure(&nexus, ConfigurationChange::ContextWarningPolicy(invalid)),
        MetaResponse::ConfigureRefused(ConfigureRefusal::ContextWarningConfigurationRefused(
            ContextWarningConfigurationRefusal::InvalidWarningTokens,
        )),
    );
    assert_eq!(snapshot(&nexus), before_refusal);
    assert_eq!(
        configure(
            &nexus,
            ConfigurationChange::HarnessHookEvents(vec![
                HarnessHookEvent::Stop,
                HarnessHookEvent::Stop
            ])
        ),
        MetaResponse::ConfigureRefused(ConfigureRefusal::ContextWarningConfigurationRefused(
            ContextWarningConfigurationRefusal::DuplicateHookEvent,
        )),
    );
    assert_eq!(snapshot(&nexus), before_refusal);
    let hooks = vec![HarnessHookEvent::PostToolUse, HarnessHookEvent::Stop];
    configures(
        &nexus,
        ConfigurationChange::HarnessHookEvents(hooks.clone()),
    );
    let expected = snapshot(&nexus);
    assert_eq!(
        expected.configured_context_warning_policies.0,
        vec![other.clone(), replacement]
    );
    assert_eq!(expected.configured_harness_hook_events.0, hooks);
    drop(nexus);
    let reopened = fixture.open();
    assert_eq!(snapshot(&reopened), expected);
    assert!(matches!(
        reopened.dispatch_meta(meta_signal_flow::Input::Remove(Removal::ContextWarning(
            policy.model_name
        ))),
        MetaResponse::Removed(_),
    ));
    assert_eq!(
        snapshot(&reopened).configured_context_warning_policies.0,
        vec![other]
    );
    configures(
        &reopened,
        ConfigurationChange::HarnessHookEvents(Vec::new()),
    );
    assert!(
        snapshot(&reopened)
            .configured_harness_hook_events
            .0
            .is_empty()
    );
}

/// Pure store/operation fixtures: no harness is launched and no submission
/// stands for receipt. Dispatch custody is explicit at each operation.
#[test]
fn warning_admission_binds_actual_delivery_and_survives_reopen_and_policy_removal() {
    use crate::context_warnings::tests::fixture_policy;
    use crate::generated::memory::WarningStageState;
    use crate::generated::operation::{
        ContextWarning_Data, Record_Data, Record_Data_Context_Data, Tokens,
    };
    use crate::store::SeedsRole;
    use crate::store::context_warnings::ReadsContextWarningProgress;
    use crate::store::metaflow::SleepsMetaflows;
    use crate::store::repair::RepairsMetaflows;
    use meta_signal_flow::WarningStage;

    let fixture = ConfiguredStore::new();
    let nexus = fixture.open();
    let flow_id = ConfiguredStore::flow_node().flow_id;
    let mut policy = fixture_policy();
    policy.context_usage_projection =
        meta_signal_flow::ContextUsageProjection::ClaudeCompletedInputWithCache;
    policy
        .context_warning_limit
        .context_boundary_evidence
        .harness_kind = HarnessKind::Claude;
    nexus.store.seed_role(signal_flow::CallerIdentity {
        flow_id: flow_id.clone(),
        aspect: Aspect::Field,
        model_name: policy.model_name.clone(),
    });
    assert_eq!(
        nexus
            .store
            .rebind_current_flow(&ConfiguredStore::message(), &flow_id.0)
            .unwrap(),
        Ok(())
    );
    configures(
        &nexus,
        ConfigurationChange::ContextWarningPolicy(policy.clone()),
    );
    let notice = {
        let _dispatch = nexus.dispatch_gate.lock().unwrap();
        assert_eq!(
            nexus.perform(Operation::Record(Record_Data::Context(
                Record_Data_Context_Data {
                    flow_id: flow_id.clone(),
                    tokens: Tokens(180_000),
                }
            ))),
            Outcome::Recorded
        );
        let Outcome::ContextWarningAdmitted(id) =
            nexus.perform(Operation::ContextWarning(ContextWarning_Data {
                flow_id: flow_id.clone(),
                tokens: Tokens(180_000),
            }))
        else {
            panic!("crossing admits one notice")
        };
        id
    };
    let progress = nexus.store.warning_progress(&flow_id.0).unwrap().unwrap();
    for row in &progress.warning_stage_progress_vector {
        assert_eq!(
            row.warning_stage_state,
            if row.warning_stage == WarningStage::Emergency {
                WarningStageState::Admitted(notice.clone())
            } else {
                WarningStageState::Passed
            }
        );
    }
    let queue = nexus
        .store
        .metaflow(&ConfiguredStore::message())
        .unwrap()
        .unwrap()
        .queue
        .0;
    assert_eq!(queue.len(), 1);
    assert_eq!(queue[0].logical_delivery_id, notice);
    assert_eq!(
        queue[0].message,
        flow_ethos::Message::HardAbrupt(flow_ethos::Request::Notice(
            policy
                .warning_boundary_vector
                .iter()
                .find(|row| row.warning_stage == WarningStage::Emergency)
                .unwrap()
                .warning_text
                .0
                .clone()
        ))
    );
    let before = store_bytes(&fixture);
    {
        let _dispatch = nexus.dispatch_gate.lock().unwrap();
        assert_eq!(
            nexus.perform(Operation::ContextWarning(ContextWarning_Data {
                flow_id: flow_id.clone(),
                tokens: Tokens(180_000),
            })),
            Outcome::ContextWarningUnchanged
        );
    }
    assert_eq!(store_bytes(&fixture), before);
    assert!(matches!(
        nexus.dispatch_meta(meta_signal_flow::Input::Remove(
            meta_signal_flow::Removal::ContextWarning(policy.model_name.clone())
        )),
        MetaResponse::Removed(_)
    ));
    assert_eq!(
        nexus.store.warning_progress(&flow_id.0).unwrap(),
        Some(progress.clone())
    );
    drop(nexus);
    let reopened = fixture.open();
    configures(&reopened, ConfigurationChange::ContextWarningPolicy(policy));
    let _dispatch = reopened.dispatch_gate.lock().unwrap();
    assert_eq!(
        reopened.perform(Operation::ContextWarning(ContextWarning_Data {
            flow_id: flow_id.clone(),
            tokens: Tokens(180_000),
        })),
        Outcome::ContextWarningUnchanged
    );
    assert_eq!(
        reopened.store.warning_progress(&flow_id.0).unwrap(),
        Some(progress)
    );
    assert_eq!(
        reopened
            .store
            .metaflow(&ConfiguredStore::message())
            .unwrap()
            .unwrap()
            .queue
            .0
            .len(),
        1
    );
}

#[test]
fn dispatch_orders_rebind_and_transport_free_warning_admission_both_ways() {
    use crate::context_warnings::{ObservesContextWithDispatch, tests::fixture_policy};
    use crate::store::SeedsRole;
    use crate::store::context::RecordsContextSize;
    use crate::store::context_warnings::ReadsContextWarningProgress;
    use crate::store::metaflow::SleepsMetaflows;
    use crate::store::repair::RepairsMetaflows;

    let fixture = ConfiguredStore::new();
    let nexus = fixture.open();
    let node = ConfiguredStore::flow_node();
    let mut policy = fixture_policy();
    policy.context_usage_projection =
        meta_signal_flow::ContextUsageProjection::ClaudeCompletedInputWithCache;
    policy
        .context_warning_limit
        .context_boundary_evidence
        .harness_kind = HarnessKind::Claude;
    nexus.store.seed_role(signal_flow::CallerIdentity {
        flow_id: node.flow_id.clone(),
        aspect: Aspect::Field,
        model_name: policy.model_name.clone(),
    });
    configures(&nexus, ConfigurationChange::ContextWarningPolicy(policy));
    let mut successor = node.clone();
    successor.flow_id = flow_ethos::FlowId("fresh-warning-flow".into());
    successor.session_id = signal_flow::SessionId("fresh-warning-session".into());
    successor.herdr_route_selection = HerdrRouteSelection::Unavailable;
    successor.origin = Origin::Flow(FlowOrigin {
        flow_id: successor.flow_id.clone(),
        session_id: successor.session_id.clone(),
    });
    nexus.store.register_flow(successor.clone()).unwrap();
    assert_eq!(
        nexus
            .store
            .rebind_current_flow(&ConfiguredStore::message(), &node.flow_id.0)
            .unwrap(),
        Ok(())
    );

    // Rebind owns dispatch first. The observation has already captured the
    // old native node, then waits for that turn; it records and admits nothing.
    std::thread::scope(|scope| {
        let dispatch = nexus.dispatch_gate.lock().unwrap();
        let (entered, ready) = std::sync::mpsc::channel();
        let nexus = &nexus;
        let node = &node;
        let observer = scope.spawn(move || {
            entered.send(()).unwrap();
            let _dispatch = nexus.dispatch_gate.lock().unwrap();
            nexus.observe_context_with_dispatch(node, 180_000);
        });
        ready.recv().unwrap();
        assert_eq!(
            nexus
                .store
                .rebind_current_flow(&ConfiguredStore::message(), &successor.flow_id.0)
                .unwrap(),
            Ok(())
        );
        drop(dispatch);
        observer.join().unwrap();
    });
    assert_eq!(nexus.store.context_size(&node.flow_id.0).unwrap(), None);
    assert_eq!(nexus.store.warning_progress(&node.flow_id.0).unwrap(), None);
    assert!(
        nexus
            .store
            .metaflow(&ConfiguredStore::message())
            .unwrap()
            .unwrap()
            .queue
            .0
            .is_empty()
    );

    // Admission owns dispatch first. Rebind waits, and the single committed
    // Notice is identified by the old FlowId's terminal stage record.
    assert_eq!(
        nexus
            .store
            .rebind_current_flow(&ConfiguredStore::message(), &node.flow_id.0)
            .unwrap(),
        Ok(())
    );
    std::thread::scope(|scope| {
        let dispatch = nexus.dispatch_gate.lock().unwrap();
        let (entered, ready) = std::sync::mpsc::channel();
        let nexus = &nexus;
        let successor = &successor;
        let rebind = scope.spawn(move || {
            entered.send(()).unwrap();
            let _dispatch = nexus.dispatch_gate.lock().unwrap();
            assert_eq!(
                nexus
                    .store
                    .rebind_current_flow(&ConfiguredStore::message(), &successor.flow_id.0)
                    .unwrap(),
                Ok(())
            );
        });
        ready.recv().unwrap();
        nexus.observe_context_with_dispatch(&node, 180_000);
        drop(dispatch);
        rebind.join().unwrap();
    });
    let progress = nexus
        .store
        .warning_progress(&node.flow_id.0)
        .unwrap()
        .unwrap();
    let queue = nexus
        .store
        .metaflow(&ConfiguredStore::message())
        .unwrap()
        .unwrap()
        .queue
        .0;
    assert_eq!(queue.len(), 1);
    assert!(
        progress
            .warning_stage_progress_vector
            .iter()
            .any(|row| row.warning_stage_state
                == crate::generated::memory::WarningStageState::Admitted(
                    queue[0].logical_delivery_id.clone()
                ))
    );
    assert_eq!(
        nexus.store.warning_progress(&successor.flow_id.0).unwrap(),
        None
    );
}
