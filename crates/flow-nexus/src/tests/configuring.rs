//! Configure declares one configuration family whole: the declaration
//! replaces the committed family in one commit and leaves every other row as
//! it was, so the effective family is always the last declaration. A refused
//! declaration changes nothing. Each operation reads one coherent committed
//! generation, so a replacement is served by the next operation without a
//! Nexus restart.

use crate::generated::operation::{Operation, Outcome};
use crate::performing::Performs;
use crate::store::metaflow::MutatesMetaflows;
use crate::store::{
    ConfigurationOnlyStore, ConfiguresOnlyStore, DefaultConfiguration, FlowStore,
    ManagesMetaflowState, OpensFlowStore, ReadsFlowRows, RegistersFlowIdentity, SeedsConfiguration,
};
use crate::{Dispatches, OpensRunningNexus, ReadsCompleteConfiguration, RunningNexus};
use meta_signal_flow::{
    ConfigurationChange, ConfigurationEntry, ConfigurationSnapshot, ConfigureMetaflow,
    ConfigureModel, ConfigureNexus, ConfigureOutcome, ConfigureRejection, ConfigureSendServer,
    ConfigureThreshold, EntryKey, ModelKey_Data, Response as MetaResponse,
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
            ConfigurationChange::Harnesses(vec![
                Self::harness("codex", HarnessKind::Codex, &[TERRA, SOL, LUNA]),
                Self::harness("claude", HarnessKind::Claude, &["claude-haiku-5-5"]),
            ]),
            ConfigurationChange::Models(vec![ConfigureModel {
                layer: Layer::Primary,
                aspect: Aspect::Mind,
                model_name: "gpt-6-astra".into(),
                effort: "medium".into(),
            }]),
            ConfigurationChange::Thresholds(vec![ConfigureThreshold {
                model_name: "claude-fable-5-1".into(),
                refresh_tokens: 200_000,
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

    fn harness(name: &str, kind: HarnessKind, models: &[&str]) -> meta_signal_flow::Harness {
        meta_signal_flow::Harness {
            harness_name: name.into(),
            harness_kind: kind.clone(),
            harness_version: "fixture".into(),
            executable: "/opt/codex".into(),
            home: "/tmp/codex".into(),
            control_socket_path_option: (kind == HarnessKind::Codex)
                .then(|| "/tmp/codex.sock".into()),
            served_models: models
                .iter()
                .map(|model| meta_signal_flow::HarnessModel {
                    model_name: (*model).into(),
                    recorded: (*model).into(),
                })
                .collect(),
            modules: vec![],
            left_out: vec![],
            arguments: vec![],
            settings: vec![],
            parameters: vec![],
            unset: vec![],
            pane_profile: meta_signal_flow::PaneProfile {
                command_sigil_vector: if kind == HarnessKind::Claude {
                    vec!["/".into(), "!".into(), "#".into()]
                } else {
                    vec!["/".into(), "!".into()]
                },
                interrupt_keys: if kind == HarnessKind::Claude {
                    vec!["esc".into(), "esc".into()]
                } else {
                    vec!["esc".into()]
                },
                submit_keys: if kind == HarnessKind::Claude {
                    vec!["enter".into()]
                } else {
                    vec![]
                },
                compact_line: "/compact".into(),
                composer: meta_signal_flow::Composer {
                    glyph: if kind == HarnessKind::Claude {
                        "❯"
                    } else {
                        "›"
                    }
                    .into(),
                    retraction: if kind == HarnessKind::Claude {
                        meta_signal_flow::Retraction::Keys(vec!["ctrl+c".into()])
                    } else {
                        meta_signal_flow::Retraction::LineByLine(vec![
                            "ctrl+u".into(),
                            "backspace".into(),
                        ])
                    },
                    composer_lines: 12,
                    retracted_lines: 32,
                },
            },
        }
    }

    fn nexus(defaults: &DefaultConfiguration, stable: &[&str], lease: i64) -> ConfigureNexus {
        let settings = defaults.configuration();
        let _ = stable;
        ConfigureNexus {
            source_root: settings.source_root,
            flows_root: "/tmp/flows".into(),
            meta_aspects: settings.meta_aspects,
            message_nexus_path: settings.message_nexus_path,
            message_nexus_binary: std::env::current_exe()
                .expect("test executable")
                .to_string_lossy()
                .into_owned(),
            curriculum_nexus_path: "/fixture/curriculum.sock".into(),
            lease,
            executables: meta_signal_flow::Executables {
                herdr: "herdr".into(),
                flow_id_claimer: "flow-id".into(),
                hook: "flow-hook".into(),
            },
            default_herdr_session: "default".into(),
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
        configured_models: models,
        configured_thresholds: thresholds,
        ..
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
        .complete_configuration_snapshot()
        .expect("snapshot reads")
}

fn stable_models(nexus: &RunningNexus) -> BTreeSet<String> {
    snapshot(nexus)
        .configured_harnesses
        .into_iter()
        .find(|harness| harness.harness_name == "codex")
        .expect("configured Codex harness")
        .served_models
        .into_iter()
        .map(|served| served.model_name)
        .collect()
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
    assert_eq!(
        snapshot(&nexus).configured_nexus,
        replaced,
        "the whole value replaced"
    );
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
    assert!(stable_models(&reopened).contains(TERRA));
    assert_eq!(snapshot(&reopened).configured_nexus, replaced);
    assert_eq!(other_rows(&reopened), before);
}

#[test]
fn a_refused_declaration_changes_nothing() {
    let fixture = ConfiguredStore::new();
    let nexus = fixture.open();
    let before = snapshot(&nexus);
    let rows = other_rows(&nexus);
    let overlapping = vec![
        ConfiguredStore::harness("first", HarnessKind::Codex, &[SOL, LUNA]),
        ConfiguredStore::harness("second", HarnessKind::Codex, &[LUNA]),
    ];
    assert_eq!(
        configure(&nexus, ConfigurationChange::Harnesses(overlapping)),
        MetaResponse::ConfigureRejected(ConfigureRejection::ModelServedTwice(LUNA.into()))
    );
    // One key named twice is not a family.
    let model = |native: &str| ConfigureModel {
        layer: Layer::Secondary,
        aspect: Aspect::Psyche,
        model_name: native.into(),
        effort: "medium".into(),
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
fn a_fresh_store_takes_its_first_nexus_declaration() {
    let directory = tempfile::tempdir().expect("temporary state");
    let defaults = DefaultConfiguration {
        home: directory.path().join("home"),
        runtime_directory: directory.path().join("runtime"),
    };
    let path = directory.path().join("flow.sema");
    let store = ConfigurationOnlyStore::open(&path).expect("fresh store");
    let seeded = store
        .configuration_snapshot()
        .expect("reads")
        .expect("a fresh store commits its first complete configuration");
    assert!(seeded.configured_harnesses.is_empty());
    assert!(seeded.configured_models.is_empty());
    assert!(seeded.configured_thresholds.is_empty());
    assert!(seeded.configured_send_servers.is_empty());
    assert!(seeded.configured_metaflows.is_empty());
    assert_eq!(seeded.configured_investigation, None);
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
            .map(|snapshot| snapshot.configured_nexus),
        Some(declared)
    );
}

#[test]
fn each_keyed_family_changes_one_entry_and_removes_that_entry() {
    let fixture = ConfiguredStore::new();
    let nexus = fixture.open();
    let before_refusal = snapshot(&nexus);
    assert_eq!(
        nexus.dispatch_meta(meta_signal_flow::Query::ConfigureEntry(
            ConfigurationEntry::HarnessEntry(ConfiguredStore::harness(
                "overlapping",
                HarnessKind::Claude,
                &[TERRA],
            )),
        )),
        MetaResponse::ConfigureRejected(ConfigureRejection::ModelServedTwice(TERRA.into()))
    );
    assert_eq!(
        snapshot(&nexus),
        before_refusal,
        "a refused key writes nothing"
    );
    let model = ConfigureModel {
        layer: Layer::Quaternary,
        aspect: Aspect::Field,
        model_name: "claude-extra".into(),
        effort: "low".into(),
    };
    let threshold = ConfigureThreshold {
        model_name: "claude-extra".into(),
        refresh_tokens: 91_000,
    };
    let harness = ConfiguredStore::harness("fixture-extra", HarnessKind::Claude, &["claude-extra"]);
    let send_server = ConfigureSendServer {
        codex_app_server_binary: "/opt/codex-app-server-extra".into(),
        mcp_send_server_binary: "/opt/mcp-send-server-extra".into(),
    };
    let metaflow = ConfigureMetaflow {
        address: quaternary_field(),
        system_modules: vec!["spirit-core".into()],
        user_modules: vec![],
        tool_set: signal_flow::ToolSet::Default,
        continuity: meta_signal_flow::Continuity::OnDemand,
        purpose: "Exercises one configured entry.".into(),
        records: vec![],
        left_out: vec![],
        requires: vec![],
    };
    let entries = [
        ConfigurationEntry::ModelEntry(model.clone()),
        ConfigurationEntry::ThresholdEntry(threshold.clone()),
        ConfigurationEntry::HarnessEntry(harness.clone()),
        ConfigurationEntry::SendServerEntry(send_server.clone()),
        ConfigurationEntry::MetaflowEntry(metaflow.clone()),
    ];
    for entry in entries {
        assert_eq!(
            nexus.dispatch_meta(meta_signal_flow::Query::ConfigureEntry(entry.clone())),
            MetaResponse::Configured(ConfigureOutcome::Replaced)
        );
        assert_eq!(
            nexus.dispatch_meta(meta_signal_flow::Query::ConfigureEntry(entry)),
            MetaResponse::Configured(ConfigureOutcome::Unchanged)
        );
    }
    let configured = snapshot(&nexus);
    assert!(configured.configured_models.contains(&model));
    assert!(configured.configured_thresholds.contains(&threshold));
    assert!(configured.configured_harnesses.contains(&harness));
    assert!(configured.configured_send_servers.contains(&send_server));
    assert!(configured.configured_metaflows.contains(&metaflow));

    let keys = [
        EntryKey::ModelKey(ModelKey_Data {
            layer: model.layer.clone(),
            aspect: model.aspect.clone(),
        }),
        EntryKey::ThresholdKey(threshold.model_name.clone()),
        EntryKey::HarnessKey(harness.harness_name.clone()),
        EntryKey::SendServerKey(send_server.codex_app_server_binary.clone()),
        EntryKey::MetaflowKey(metaflow.address.clone()),
    ];
    for key in keys {
        assert_eq!(
            nexus.dispatch_meta(meta_signal_flow::Query::RemoveEntry(key.clone())),
            MetaResponse::Configured(ConfigureOutcome::Removed)
        );
        assert_eq!(
            nexus.dispatch_meta(meta_signal_flow::Query::RemoveEntry(key)),
            MetaResponse::ConfigureRejected(ConfigureRejection::UnknownEntry)
        );
    }
    drop(nexus);
    let reopened = fixture.open();
    let configured = snapshot(&reopened);
    assert!(!configured.configured_models.contains(&model));
    assert!(!configured.configured_thresholds.contains(&threshold));
    assert!(!configured.configured_harnesses.contains(&harness));
    assert!(!configured.configured_send_servers.contains(&send_server));
    assert!(!configured.configured_metaflows.contains(&metaflow));
}

#[test]
fn configuration_reads_the_exact_runtime_and_every_committed_family() {
    let fixture = ConfiguredStore::new();
    let nexus = fixture.open();
    let configured = snapshot(&nexus);
    assert_eq!(configured.runtime, fixture.start);
    assert_eq!(configured.configured_nexus.lease, 30);
    assert_eq!(configured.configured_harnesses.len(), 2);
    assert_eq!(configured.configured_models.len(), 1);
    assert_eq!(configured.configured_thresholds.len(), 1);
    assert!(configured.configured_send_servers.is_empty());
    assert!(configured.configured_metaflows.is_empty());
    assert_eq!(configured.configured_investigation, None);
    assert!(configured.timings.socket_timeout > 0);
    assert!(configured.limits.frame > 0);
    assert!(!configured.texts.launch_section.pending_launch.is_empty());
}

#[test]
fn a_complete_configuration_read_names_its_one_committed_generation() {
    let fixture = ConfiguredStore::new();
    let nexus = fixture.open();
    let before = nexus
        .store
        .committed_configuration_snapshot()
        .expect("reads one committed view")
        .expect("configured");
    let replacement = ConfigureModel {
        layer: Layer::Quaternary,
        aspect: Aspect::Field,
        model_name: "claude-generation-witness".into(),
        effort: "low".into(),
    };
    assert_eq!(
        nexus.dispatch_meta(meta_signal_flow::Query::ConfigureEntry(
            ConfigurationEntry::ModelEntry(replacement.clone()),
        )),
        MetaResponse::Configured(ConfigureOutcome::Replaced)
    );
    let after = nexus
        .store
        .committed_configuration_snapshot()
        .expect("reads the next committed view")
        .expect("configured");
    assert_ne!(after.generation, before.generation);
    assert!(after.snapshot.configured_models.contains(&replacement));

    let refused_generation = after.generation;
    assert_eq!(
        nexus.dispatch_meta(meta_signal_flow::Query::ConfigureEntry(
            ConfigurationEntry::HarnessEntry(ConfiguredStore::harness(
                "overlapping-generation-witness",
                HarnessKind::Claude,
                &[TERRA],
            )),
        )),
        MetaResponse::ConfigureRejected(ConfigureRejection::ModelServedTwice(TERRA.into()))
    );
    let after_refusal = nexus
        .store
        .committed_configuration_snapshot()
        .expect("reads after refusal")
        .expect("configured");
    assert_eq!(after_refusal.generation, refused_generation);
    assert_eq!(after_refusal.snapshot, after.snapshot);
}

#[test]
fn an_operation_view_stays_on_a_while_the_next_operation_takes_b() {
    let fixture = ConfiguredStore::new();
    let nexus = fixture.open();
    let a = nexus.configured_use().expect("operation A takes its view");
    let a_snapshot = a.snapshot.clone();

    let mut nexus_b = a_snapshot.configured_nexus.clone();
    nexus_b.executables.herdr = "/configured/herdr-b".into();
    nexus_b.curriculum_nexus_path = "/configured/curriculum-b.sock".into();
    nexus_b.lease += 1;
    assert_eq!(
        configure(&nexus, ConfigurationChange::Nexus(nexus_b.clone())),
        MetaResponse::Configured(ConfigureOutcome::Replaced)
    );
    let b = nexus.configured_use().expect("the next operation takes B");

    assert_ne!(a.generation, b.generation);
    assert_eq!(
        a.snapshot, a_snapshot,
        "paused A remains one immutable view"
    );
    assert_ne!(a.herdr.executable, b.herdr.executable);
    assert_eq!(
        b.herdr.executable,
        std::path::PathBuf::from("/configured/herdr-b")
    );
    assert_eq!(b.snapshot.configured_nexus, nexus_b);
    assert_eq!(
        a.codex_endpoints.endpoints[0].client_path, b.codex_endpoints.endpoints[0].client_path,
        "an independently unchanged family is shared by both complete views"
    );

    let refused_generation = b.generation;
    assert_eq!(
        nexus.dispatch_meta(meta_signal_flow::Query::ConfigureEntry(
            ConfigurationEntry::HarnessEntry(ConfiguredStore::harness(
                "overlapping-operation-b",
                HarnessKind::Claude,
                &[TERRA],
            )),
        )),
        MetaResponse::ConfigureRejected(ConfigureRejection::ModelServedTwice(TERRA.into()))
    );
    let after_refusal = nexus.configured_use().expect("view after refusal");
    assert_eq!(after_refusal.generation, refused_generation);
    assert_eq!(after_refusal.snapshot, b.snapshot);
    assert_eq!(after_refusal.herdr.executable, b.herdr.executable);
}

#[test]
fn a_store_configured_while_stopped_opens_with_the_replaced_value() {
    let fixture = ConfiguredStore::new();
    let path = std::path::Path::new(&fixture.start.store_path);
    let rows = other_rows(&fixture.open());
    let replaced = ConfiguredStore::nexus(&fixture.defaults, &[SOL, LUNA], 31);
    let configuration = ConfigurationOnlyStore::open(path).expect("stopped store reopens");
    assert_eq!(
        configuration
            .configure_change(ConfigurationChange::Nexus(replaced.clone()))
            .expect("stopped store takes the declaration"),
        ConfigureOutcome::Replaced
    );
    drop(configuration);
    let nexus = fixture.open();
    assert_eq!(snapshot(&nexus).configured_nexus, replaced);
    assert_eq!(other_rows(&nexus), rows);
    assert!(
        stable_models(&nexus).contains(TERRA),
        "replacing Nexus leaves the independently configured harness family whole"
    );
}
