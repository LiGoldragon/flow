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
    ConfigurationChange, ConfigurationSnapshot, ConfigureModel, ConfigureNexus, ConfigureOutcome,
    ConfigureRejection, ConfigureThreshold, Response as MetaResponse,
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
    snapshot(nexus)
        .harnesses
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
    assert!(stable_models(&reopened).contains(TERRA));
    assert_eq!(snapshot(&reopened).nexus, replaced);
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
    assert_eq!(snapshot(&nexus).nexus, replaced);
    assert_eq!(other_rows(&nexus), rows);
    assert!(
        stable_models(&nexus).contains(TERRA),
        "replacing Nexus leaves the independently configured harness family whole"
    );
}
