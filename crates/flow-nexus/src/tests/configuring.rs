//! Configure declares one configuration family whole: the declaration
//! replaces the committed family in one commit and leaves every other row as
//! it was, so the effective family is always the last declaration. A refused
//! declaration changes nothing. The running Nexus keeps the Codex endpoints it
//! opened with; the next open serves the replaced ones.

use crate::generated::operation::{Operation, Outcome};
use crate::performing::Performs;
use crate::store::metaflow::MutatesMetaflows;
use crate::store::{
    ConfigurationOnlyStore, ConfiguresOnlyStore, DefaultConfiguration, FlowStore,
    ManagesMetaflowState, OpensFlowStore, ReadsFlowRows, RegistersFlowIdentity, SeedsConfiguration,
};
use crate::{Dispatches, OpensRunningNexus, RunningNexus};
use meta_signal_flow::{
    ConfigurationChange, ConfigurationSnapshot, ConfigureLeaveOut, ConfigureModel, ConfigureNexus,
    ConfigureOutcome, ConfigureRejection, ConfigureThreshold, Response as MetaResponse,
};
use signal_flow::{
    Address, Aspect, Bind_Data, EndpointSelection, FlowLifecycle, FlowNode, FlowOrigin,
    HarnessKind, HerdrRoute, HerdrRouteSelection, Layer, Lock_Data, Origin, Process, Recipient,
    Response,
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
        .expect("start time");
    Process {
        pid: i64::from(std::process::id()),
        started,
    }
}

const TERRA: &str = "gpt-5.6-terra";
const SOL: &str = "gpt-5.6-sol";
const LUNA: &str = "gpt-5.6-luna";

struct ConfiguredStore {
    _directory: tempfile::TempDir,
    defaults: DefaultConfiguration,
    start: flow_ethos::Start,
}

impl ConfiguredStore {
    /// A configured store: Nexus, Models and Thresholds declared through
    /// Configure, a registered flow, Message bound and a lock held.
    fn new() -> Self {
        let directory = tempfile::tempdir().expect("temporary state");
        let defaults = DefaultConfiguration {
            home: directory.path().join("home"),
            runtime_directory: directory.path().join("runtime"),
        };
        let path = |name: &str| directory.path().join(name).to_string_lossy().into_owned();
        let start = flow_ethos::Start {
            ordinary_socket_path: path("ordinary.sock"),
            meta_socket_path: path("meta.sock"),
            store_path: path("flow.sema"),
            landing_watch_option: None,
        };
        let configuration = ConfigurationOnlyStore::open(std::path::Path::new(&start.store_path))
            .expect("configuration store");
        for change in [
            ConfigurationChange::Nexus(Self::nexus(&defaults, &[TERRA, SOL, LUNA], 30)),
            ConfigurationChange::Models(vec![ConfigureModel {
                layer: Layer::Primary,
                aspect: Aspect::Mind,
                native: "gpt-6-astra".into(),
            }]),
            ConfigurationChange::Thresholds(vec![ConfigureThreshold {
                model_name: "claude-fable-5-1".into(),
                refresh: 200_000,
            }]),
        ] {
            configuration
                .configure_change(change)
                .expect("configuration commits");
        }
        drop(configuration);
        let store = FlowStore::open_configured(std::path::Path::new(&start.store_path))
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
                    &peer,
                    None,
                )
                .expect("bind reads"),
            Response::Bound(_)
        ));
        assert!(matches!(
            store
                .lock_metaflow(
                    Lock_Data {
                        sender: Self::message(),
                        recipient: Recipient::Address(Self::message()),
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
            start,
        }
    }

    fn nexus(defaults: &DefaultConfiguration, stable: &[&str], lease: i64) -> ConfigureNexus {
        let settings = defaults.configuration();
        let mut stable_codex = settings.stable_codex;
        stable_codex.model_name_vector = stable.iter().map(|model| (*model).into()).collect();
        ConfigureNexus {
            source_root: settings.source_root,
            stable_codex,
            next_codex: settings.next_codex,
            harness_profiles: settings.harness_profile_vector,
            meta_aspects: settings.meta_aspects,
            message_nexus_path: settings.message_nexus_path,
            message_nexus_binary: std::env::current_exe()
                .expect("test executable")
                .to_string_lossy()
                .into_owned(),
            curriculum_nexus_path: "/fixture/curriculum.sock".into(),
            lease,
        }
    }

    fn message() -> Address {
        Address {
            aspect: Aspect::Field,
            topic: "message".into(),
            layer: Layer::Primary,
        }
    }

    fn flow_node() -> FlowNode {
        FlowNode {
            flow_id: "da1e3f".into(),
            session_id: "claude-session".into(),
            harness_kind: HarnessKind::Claude,
            endpoint_selection: EndpointSelection::Unavailable,
            herdr_route_selection: HerdrRouteSelection::Available(HerdrRoute {
                herdr_session_name: "messaging-build".into(),
                herdr_agent_name: "recipient".into(),
                herdr_pane_id: "w1:p2".into(),
                herdr_terminal_id: "term-current".into(),
            }),
            origin: Origin::Flow(FlowOrigin {
                flow_id: "da1e3f".into(),
                session_id: "claude-session".into(),
            }),
            flow_lifecycle: FlowLifecycle::Active,
        }
    }

    fn open(&self) -> RunningNexus {
        RunningNexus::open(&self.defaults, &self.start).expect("configured Nexus opens")
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
        models, thresholds, ..
    } = snapshot(nexus);
    let peer = this_process();
    OtherRows {
        models,
        thresholds,
        flow: nexus.store.flow_node("da1e3f").expect("flow reads"),
        metaflows: nexus.store.list_metaflows().expect("metaflows read"),
        message_bound: nexus.store.message_peer(&peer).expect("binding reads"),
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
    nexus.codex_endpoints.stable.model_names.clone()
}

fn configure(nexus: &RunningNexus, change: ConfigurationChange) -> MetaResponse {
    nexus.dispatch_meta(meta_signal_flow::Query::Configure(change))
}

fn quaternary_field() -> Address {
    Address {
        aspect: Aspect::Field,
        topic: "flow".into(),
        layer: Layer::Quaternary,
    }
}

#[test]
fn configure_nexus_replaces_only_the_nexus_value_and_keeps_every_other_row() {
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
    assert_eq!(snapshot(&nexus).nexus, replaced, "the whole value replaced");
    assert_eq!(other_rows(&nexus), before, "no other row changed");
    // The same declaration again writes nothing.
    assert_eq!(
        configure(&nexus, ConfigurationChange::Nexus(replaced.clone())),
        MetaResponse::Configured(ConfigureOutcome::Unchanged)
    );

    // The running Nexus keeps the endpoints it opened with.
    assert!(stable_models(&nexus).contains(TERRA));
    drop(nexus);

    // The next open serves the replaced value and still holds every row.
    let reopened = fixture.open();
    assert_eq!(
        stable_models(&reopened),
        BTreeSet::from([SOL.to_owned(), LUNA.to_owned()])
    );
    assert_eq!(snapshot(&reopened).nexus, replaced);
    assert_eq!(other_rows(&reopened), before);
}

#[test]
fn a_refused_declaration_changes_nothing() {
    let fixture = ConfiguredStore::new();
    let nexus = fixture.open();
    let before = snapshot(&nexus);
    let rows = other_rows(&nexus);
    let mut overlapping = ConfiguredStore::nexus(&fixture.defaults, &[SOL, LUNA], 45);
    overlapping
        .next_codex
        .model_name_vector
        .push(LUNA.to_owned());
    assert_eq!(
        configure(&nexus, ConfigurationChange::Nexus(overlapping)),
        MetaResponse::ConfigureRejected(ConfigureRejection::CodexModelsOverlap)
    );
    // One key named twice is not a family.
    let model = |native: &str| ConfigureModel {
        layer: Layer::Secondary,
        aspect: Aspect::Psyche,
        native: native.into(),
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
    let nexus_before = snapshot(&nexus).nexus;

    // In the order the snapshot reads them, by layer then aspect name; one
    // layer holds a model for each aspect.
    let models = vec![
        ConfigureModel {
            layer: Layer::Quaternary,
            aspect: Aspect::Psyche,
            native: "claude-haiku-5-5".into(),
        },
        ConfigureModel {
            layer: Layer::Secondary,
            aspect: Aspect::Mind,
            native: SOL.into(),
        },
        ConfigureModel {
            layer: Layer::Secondary,
            aspect: Aspect::Psyche,
            native: "claude-opus-5-5".into(),
        },
    ];
    assert_eq!(
        configure(&nexus, ConfigurationChange::Models(models.clone())),
        MetaResponse::Configured(ConfigureOutcome::Replaced)
    );
    // The Primary model the store held is gone: it is not declared.
    assert_eq!(snapshot(&nexus).models, models);
    assert_eq!(
        configure(&nexus, ConfigurationChange::Models(models.clone())),
        MetaResponse::Configured(ConfigureOutcome::Unchanged)
    );

    let thresholds = vec![
        ConfigureThreshold {
            model_name: "claude-opus-5-5".into(),
            refresh: 200_000,
        },
        ConfigureThreshold {
            model_name: "gpt-6.1-sol".into(),
            refresh: 150_000,
        },
    ];
    assert_eq!(
        configure(&nexus, ConfigurationChange::Thresholds(thresholds.clone())),
        MetaResponse::Configured(ConfigureOutcome::Replaced)
    );
    assert_eq!(snapshot(&nexus).thresholds, thresholds);

    let first = vec![
        ConfigureLeaveOut {
            address: quaternary_field(),
            harness_kind: HarnessKind::Claude,
            leave_outs: vec![signal_flow::LeaveOut::SkillTool],
        },
        ConfigureLeaveOut {
            address: ConfiguredStore::message(),
            harness_kind: HarnessKind::Claude,
            leave_outs: vec![signal_flow::LeaveOut::ClaudeMd],
        },
    ];
    assert_eq!(
        configure(&nexus, ConfigurationChange::LeaveOuts(first)),
        MetaResponse::Configured(ConfigureOutcome::Replaced)
    );
    let second = vec![ConfigureLeaveOut {
        address: quaternary_field(),
        harness_kind: HarnessKind::Claude,
        leave_outs: vec![
            signal_flow::LeaveOut::SkillTool,
            signal_flow::LeaveOut::WorkflowTool,
        ],
    }];
    assert_eq!(
        configure(&nexus, ConfigurationChange::LeaveOuts(second.clone())),
        MetaResponse::Configured(ConfigureOutcome::Replaced)
    );
    assert_eq!(snapshot(&nexus).default_leave_outs, second);
    assert!(
        nexus
            .store
            .default_leave_outs(&ConfiguredStore::message(), &HarnessKind::Claude)
            .unwrap()
            .is_empty()
    );
    // A fresh store trusts no send server; a declaration names each pair.
    assert!(snapshot(&nexus).send_servers.is_empty());
    let send_servers = vec![meta_signal_flow::ConfigureSendServer {
        codex_app_server_binary: "/nix/store/codex/bin/codex".into(),
        mcp_send_server_binary: "/nix/store/send/bin/flow-send-mcp".into(),
    }];
    assert_eq!(
        configure(
            &nexus,
            ConfigurationChange::SendServers(send_servers.clone())
        ),
        MetaResponse::Configured(ConfigureOutcome::Replaced)
    );
    assert_eq!(snapshot(&nexus).send_servers, send_servers);
    // The effective snapshot equals the last declaration of every family,
    // across a restart.
    drop(nexus);
    let reopened = fixture.open();
    assert_eq!(
        snapshot(&reopened),
        ConfigurationSnapshot {
            nexus: nexus_before,
            models,
            thresholds,
            default_leave_outs: second,
            send_servers,
            metaflows: Vec::new(),
            investigation: None,
            default_minutes: crate::store::SEEDED_DEFAULT_MINUTES,
        }
    );
    // An empty declaration empties the family.
    assert_eq!(
        configure(&reopened, ConfigurationChange::LeaveOuts(Vec::new())),
        MetaResponse::Configured(ConfigureOutcome::Replaced)
    );
    assert!(snapshot(&reopened).default_leave_outs.is_empty());
    assert_eq!(
        configure(&reopened, ConfigurationChange::LeaveOuts(Vec::new())),
        MetaResponse::Configured(ConfigureOutcome::Unchanged)
    );
}

#[test]
fn a_launch_after_a_replaced_declaration_takes_the_new_defaults() {
    let fixture = ConfiguredStore::new();
    let mut nexus = fixture.open();
    nexus.composer = crate::composition::tests::fixture_composer();
    let mut profile = crate::composition::tests::fixture_profile();
    profile.address = quaternary_field();
    profile.model_name = "claude-haiku-5-5".into();
    let composed_leave_outs =
        |nexus: &RunningNexus| match nexus.perform(Operation::Compose(profile.clone())) {
            Outcome::Composed(launch) => launch.leave_outs,
            outcome => panic!("launch not composed: {outcome:?}"),
        };
    let declare = |nexus: &RunningNexus, leave_outs: Vec<signal_flow::LeaveOut>| {
        configure(
            nexus,
            ConfigurationChange::LeaveOuts(vec![ConfigureLeaveOut {
                address: quaternary_field(),
                harness_kind: HarnessKind::Claude,
                leave_outs,
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
    nexus.composer = crate::composition::tests::fixture_composer();
    let mut profile = crate::composition::tests::fixture_profile();
    profile.address = quaternary_field();
    for inexact in [
        "haiku",
        "claude-opus-5-5[1m]",
        "Claude-Haiku-5-5",
        "claude-",
    ] {
        profile.model_name = inexact.into();
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
    profile.model_name = "claude-haiku-5-5".into();
    assert!(matches!(
        nexus.perform(Operation::Compose(profile.clone())),
        Outcome::Composed(_)
    ));
    assert_eq!(nexus.codex_endpoints.harness_for(SOL), HarnessKind::Codex);
    assert!(nexus.codex_endpoints.launches_exactly(SOL));
    assert!(!nexus.codex_endpoints.launches_exactly("gpt-6-unlisted"));
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
        leave_outs: vec![signal_flow::LeaveOut::ClaudeMd],
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
            leave_outs: vec![
                signal_flow::LeaveOut::CodexAgentsMd,
                signal_flow::LeaveOut::ClaudeMd,
            ],
        },
        ConfigureLeaveOut {
            address: ConfiguredStore::message(),
            harness_kind: HarnessKind::Claude,
            leave_outs: vec![signal_flow::LeaveOut::CodexSkillListing],
        },
    ] {
        let declaration = vec![
            ConfigureLeaveOut {
                address: quaternary_field(),
                harness_kind: HarnessKind::Codex,
                leave_outs: vec![signal_flow::LeaveOut::CodexAgentsMd],
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
    let configuration =
        ConfigurationOnlyStore::open(std::path::Path::new(&fixture.start.store_path))
            .expect("stopped store reopens");
    assert!(matches!(
        configuration.configure_change(ConfigurationChange::LeaveOuts(vec![ConfigureLeaveOut {
            address: quaternary_field(),
            harness_kind: HarnessKind::Codex,
            leave_outs: vec![signal_flow::LeaveOut::SkillTool],
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
    nexus.composer = crate::composition::tests::fixture_composer();
    assert_eq!(
        configure(
            &nexus,
            ConfigurationChange::LeaveOuts(vec![ConfigureLeaveOut {
                address: quaternary_field(),
                harness_kind: HarnessKind::Claude,
                leave_outs: vec![
                    signal_flow::LeaveOut::WorkflowTool,
                    signal_flow::LeaveOut::ClaudeMd,
                ],
            }]),
        ),
        MetaResponse::Configured(ConfigureOutcome::Replaced)
    );
    let mut profile = crate::composition::tests::fixture_profile();
    profile.address = quaternary_field();
    profile.leave_outs_option = None;
    profile.model_name = SOL.into();
    match nexus.perform(Operation::Compose(profile.clone())) {
        Outcome::Composed(launch) => {
            assert_eq!(launch.harness_kind, HarnessKind::Codex);
            assert!(launch.leave_outs.is_empty(), "{:?}", launch.leave_outs);
        }
        outcome => panic!("Codex launch not composed: {outcome:?}"),
    }
    profile.model_name = "claude-haiku-5-5".into();
    match nexus.perform(Operation::Compose(profile)) {
        Outcome::Composed(launch) => assert_eq!(
            launch.leave_outs,
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
    let path = std::path::Path::new(&fixture.start.store_path);
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
    assert_eq!(snapshot(&nexus).nexus, replaced);
    assert_eq!(other_rows(&nexus), rows);
    assert!(!stable_models(&nexus).contains(TERRA));
}

/// The default Minutes is a configuration family of its own, kept in
/// Memory: a new Memory holds the constant Flow seeds it with, Configure
/// replaces it whole and a reopen keeps it, and the Minutes rule a Task
/// meets at admission refuses a zero, a negative or an unreachable default
/// without changing it. Read and written as datom text, as flow-meta does.
#[test]
fn configure_default_minutes_is_seeded_declared_kept_and_ruled() {
    use datom_codec::{Actualizing, Budget, Datomizable, Potential};
    use protos::{Compactable, Protosizable, ReaderBudget};
    let ask = |nexus: &RunningNexus, text: &str| -> String {
        let mut budget = Budget {
            remaining: 65536,
            reader: ReaderBudget { remaining: 65536 },
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
    assert!(seeded.ends_with(" None 30 }"), "seeded at 30: {seeded}");
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
    assert!(declared.ends_with(" None 45 }"), "{declared}");
    drop(nexus);
    let reopened = fixture.open();
    let kept = ask(&reopened, "Configuration");
    assert!(kept.ends_with(" None 45 }"), "kept across a reopen: {kept}");
}
