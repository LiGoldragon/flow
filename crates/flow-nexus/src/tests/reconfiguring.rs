//! Reconfigure replaces the committed Nexus value of a configured store as a
//! whole, in one commit, and leaves every other row as it was. A refused
//! value changes nothing. The running Nexus keeps the Codex endpoints it
//! opened with; the next open serves the replaced ones.

use crate::store::metaflow::{MutatesMetaflows, SleepsMetaflows};
use crate::store::{
    ConfigurationOnlyStore, ConfiguresOnlyStore, DefaultConfiguration, FlowStore,
    ManagesMetaflowState, OpensFlowStore, ReadsFlowRows, RegistersFlowIdentity, SeedsConfiguration,
    StoreError,
};
use crate::{Dispatches, OpensRunningNexus, RunningNexus};
use meta_signal_flow::{
    ConfigurationChange, ConfigurationSnapshot, ConfigureModel, ConfigureNexus, ConfigureThreshold,
    ReconfigureRejection,
};
use signal_flow::{
    Address, Aspect, Bind_Data, EndpointSelection, FlowLifecycle, FlowNode, HarnessKind,
    HerdrRoute, HerdrRouteSelection, Layer, Lock_Data, OriginClue, Process, Recipient, Response,
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
    /// A configured store: Nexus, Model and Threshold configured through
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
        };
        let configuration = ConfigurationOnlyStore::open(std::path::Path::new(&start.store_path))
            .expect("configuration store");
        for change in [
            ConfigurationChange::Nexus(Self::nexus(&defaults, &[TERRA, SOL, LUNA], 30)),
            ConfigurationChange::Model(ConfigureModel {
                layer: Layer::Primary,
                native: "gpt-6-astra".into(),
            }),
            ConfigurationChange::Threshold(ConfigureThreshold {
                layer: Layer::Primary,
                handover: 100_000,
                refresh: 200_000,
            }),
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
            origin_clue: OriginClue {
                flow_id: "da1e3f".into(),
                session_id: "claude-session".into(),
                turn_id: "unavailable".into(),
            },
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
    message_lock_held: bool,
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
        message_lock_held: nexus
            .store
            .lock_held(&ConfiguredStore::message(), 110)
            .expect("lock reads"),
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

#[test]
fn reconfigure_replaces_only_the_nexus_value_and_keeps_every_other_row() {
    let fixture = ConfiguredStore::new();
    let nexus = fixture.open();
    let before = other_rows(&nexus);
    assert_eq!(before.models.len(), 1);
    assert_eq!(before.thresholds.len(), 1);
    assert!(before.flow.is_some() && before.message_bound && before.message_lock_held);
    assert_eq!(before.metaflows.len(), 1);

    let replaced = ConfiguredStore::nexus(&fixture.defaults, &[SOL, LUNA], 45);
    assert_eq!(
        nexus.dispatch_meta(meta_signal_flow::Query::Reconfigure(replaced.clone())),
        meta_signal_flow::Response::Reconfigured
    );
    assert_eq!(snapshot(&nexus).nexus, replaced, "the whole value replaced");
    assert_eq!(other_rows(&nexus), before, "no other row changed");

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
fn a_refused_reconfigure_changes_nothing() {
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
        nexus.dispatch_meta(meta_signal_flow::Query::Reconfigure(overlapping)),
        meta_signal_flow::Response::ReconfigureRejected(ReconfigureRejection::CodexModelsOverlap)
    );
    assert_eq!(snapshot(&nexus), before);
    assert_eq!(other_rows(&nexus), rows);
    drop(nexus);
    let reopened = fixture.open();
    assert_eq!(snapshot(&reopened), before);
    assert_eq!(other_rows(&reopened), rows);
}

#[test]
fn reconfigure_needs_a_configured_store_and_configure_still_conflicts() {
    let directory = tempfile::tempdir().expect("temporary state");
    let defaults = DefaultConfiguration {
        home: directory.path().join("home"),
        runtime_directory: directory.path().join("runtime"),
    };
    let path = directory.path().join("flow.sema");
    let store = ConfigurationOnlyStore::open(&path).expect("fresh store");
    assert!(matches!(
        store.replace_nexus_configuration(ConfiguredStore::nexus(&defaults, &[SOL, LUNA], 45)),
        Err(StoreError::NotConfigured)
    ));
    assert_eq!(store.configuration_snapshot().expect("reads"), None);
    drop(store);

    // Configure.Nexus keeps refusing a differing value; Reconfigure is the
    // one replacement.
    let fixture = ConfiguredStore::new();
    let nexus = fixture.open();
    let before = snapshot(&nexus);
    assert_eq!(
        nexus.dispatch_meta(meta_signal_flow::Query::Configure(
            ConfigurationChange::Nexus(ConfiguredStore::nexus(&fixture.defaults, &[SOL, LUNA], 45))
        )),
        meta_signal_flow::Response::ConfigureRejected(
            meta_signal_flow::ConfigureRejection::Conflict
        )
    );
    assert_eq!(snapshot(&nexus), before);
}

#[test]
fn a_store_reconfigured_while_stopped_opens_with_the_replaced_value() {
    let fixture = ConfiguredStore::new();
    let path = std::path::Path::new(&fixture.start.store_path);
    let rows = other_rows(&fixture.open());
    let replaced = ConfiguredStore::nexus(&fixture.defaults, &[SOL, LUNA], 30);
    let configuration = ConfigurationOnlyStore::open(path).expect("0.28 store reopens");
    configuration
        .replace_nexus_configuration(replaced.clone())
        .expect("stopped store reconfigures");
    drop(configuration);
    let nexus = fixture.open();
    assert_eq!(snapshot(&nexus).nexus, replaced);
    assert_eq!(other_rows(&nexus), rows);
    assert!(!stable_models(&nexus).contains(TERRA));
}

#[test]
fn a_metaflow_default_leave_out_set_is_configured_read_and_snapshotted() {
    use crate::store::ManagesMetaflowState as _;
    let fixture = ConfiguredStore::new();
    let nexus = fixture.open();
    let address = Address {
        aspect: Aspect::Field,
        topic: "flow".into(),
        layer: Layer::Quaternary,
    };
    assert!(nexus.store.default_leave_outs(&address).unwrap().is_empty());
    let leave_out = meta_signal_flow::ConfigureLeaveOut {
        address: address.clone(),
        leave_outs: vec![
            signal_flow::LeaveOut::SkillTool,
            signal_flow::LeaveOut::WorkflowTool,
        ],
    };
    assert_eq!(
        nexus.dispatch_meta(meta_signal_flow::Query::Configure(
            ConfigurationChange::LeaveOut(leave_out.clone())
        )),
        meta_signal_flow::Response::Configured
    );
    assert_eq!(
        nexus.store.default_leave_outs(&address).unwrap(),
        leave_out.leave_outs
    );
    assert_eq!(snapshot(&nexus).default_leave_outs, vec![leave_out]);
}
