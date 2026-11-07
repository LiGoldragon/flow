use super::configuring::ConfiguredStore;
use crate::store::{ConfiguresOnlyStore, ManagesMetaflowState, OpensFlowStore};
use meta_signal_flow::ConfigurationChange;

#[test]
fn handover_count_zero_persists_through_configuration_and_running_reopen() {
    let fixture = ConfiguredStore::new();
    let path = fixture.path();
    let configuration =
        crate::store::ConfigurationOnlyStore::open(path).expect("configuration store opens");
    configuration
        .configure_change(ConfigurationChange::HandoverCount(0))
        .expect("zero count is accepted");
    drop(configuration);
    let running = crate::store::FlowStore::open_configured(path).expect("running store reopens");
    let snapshot = running.configuration_snapshot().expect("snapshot").unwrap();
    assert_eq!(
        snapshot.handover_count,
        meta_signal_flow::HandoverCountConfiguration::Configured(0)
    );
    drop(running);
    let reopened =
        crate::store::ConfigurationOnlyStore::open(path).expect("configuration store reopens");
    assert_eq!(
        reopened
            .configuration_snapshot()
            .expect("snapshot")
            .unwrap()
            .handover_count,
        meta_signal_flow::HandoverCountConfiguration::Configured(0)
    );
}
