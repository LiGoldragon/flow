use super::configuring::ConfiguredStore;
use crate::store::{ConfiguresOnlyStore, ManagesMetaflowState, OpensFlowStore};
use meta_signal_flow::ConfigurationChange;

fn policy() -> meta_signal_flow::HandoverPolicy {
    meta_signal_flow::HandoverPolicy {
        model_name: "claude-opus-5-5".into(),
        warning_share_left: 15,
        reminder_text: "Continue every five minutes.".into(),
        every_minutes: 5,
    }
}

#[test]
fn handover_policies_and_archive_persist_across_both_store_kinds_and_reopens() {
    let fixture = ConfiguredStore::new();
    let path = fixture.path();
    let configuration = crate::store::ConfigurationOnlyStore::open(path).unwrap();
    configuration
        .configure_change(ConfigurationChange::HandoverPolicies(vec![policy()]))
        .unwrap();
    configuration
        .configure_change(ConfigurationChange::HandoverArchive(
            "/srv/handover-archive".into(),
        ))
        .unwrap();
    drop(configuration);
    let running = crate::store::FlowStore::open_configured(path).unwrap();
    let expected = running.configuration_snapshot().unwrap().unwrap();
    assert_eq!(
        expected.handover_policies,
        meta_signal_flow::HandoverPoliciesConfiguration::Configured(vec![policy()])
    );
    assert_eq!(
        expected.handover_archive,
        meta_signal_flow::HandoverArchiveConfiguration::Configured("/srv/handover-archive".into())
    );
    drop(running);
    let reopened = crate::store::ConfigurationOnlyStore::open(path).unwrap();
    assert_eq!(
        reopened
            .configuration_snapshot()
            .unwrap()
            .unwrap()
            .handover_policies,
        expected.handover_policies
    );
}

#[test]
fn invalid_handover_policy_and_count_are_typed_and_atomic() {
    let fixture = ConfiguredStore::new();
    let path = fixture.path();
    let configuration = crate::store::ConfigurationOnlyStore::open(path).unwrap();
    let before = configuration.configuration_snapshot().unwrap();
    let mut invalid = policy();
    invalid.warning_share_left = 101;
    assert!(matches!(
        configuration
            .configure_change(ConfigurationChange::HandoverPolicies(vec![invalid.clone()])),
        Err(crate::store::StoreError::HandoverConfigurationRefused(
            meta_signal_flow::HandoverConfigurationRefusal::InvalidWarningShareLeft
        ))
    ));
    assert!(matches!(
        configuration.configure_change(ConfigurationChange::HandoverCount(-1)),
        Err(crate::store::StoreError::HandoverConfigurationRefused(
            meta_signal_flow::HandoverConfigurationRefusal::NegativeHandoverCount
        ))
    ));
    invalid.warning_share_left = -1;
    assert!(matches!(
        configuration.configure_change(ConfigurationChange::HandoverPolicies(vec![invalid])),
        Err(crate::store::StoreError::HandoverConfigurationRefused(
            meta_signal_flow::HandoverConfigurationRefusal::InvalidWarningShareLeft
        ))
    ));
    assert_eq!(configuration.configuration_snapshot().unwrap(), before);
}

#[test]
fn invalid_interval_and_duplicate_policy_are_typed_and_atomic() {
    let fixture = ConfiguredStore::new();
    let configuration = crate::store::ConfigurationOnlyStore::open(fixture.path()).unwrap();
    let before = configuration.configuration_snapshot().unwrap();
    let mut interval = policy();
    interval.every_minutes = 0;
    assert!(matches!(
        configuration.configure_change(ConfigurationChange::HandoverPolicies(vec![interval])),
        Err(crate::store::StoreError::HandoverConfigurationRefused(
            meta_signal_flow::HandoverConfigurationRefusal::InvalidEveryMinutes
        ))
    ));
    assert!(matches!(
        configuration.configure_change(ConfigurationChange::HandoverPolicies(vec![
            policy(),
            policy()
        ])),
        Err(crate::store::StoreError::HandoverConfigurationRefused(
            meta_signal_flow::HandoverConfigurationRefusal::DuplicatePolicy
        ))
    ));
    assert_eq!(configuration.configuration_snapshot().unwrap(), before);
}

#[test]
fn runtime_nexus_change_preserves_handover_and_all_sibling_families() {
    let fixture = ConfiguredStore::new();
    let path = fixture.path();
    let configuration = crate::store::ConfigurationOnlyStore::open(path).unwrap();
    configuration
        .configure_change(ConfigurationChange::HandoverPolicies(vec![policy()]))
        .unwrap();
    configuration
        .configure_change(ConfigurationChange::HandoverArchive("/srv/archive".into()))
        .unwrap();
    configuration
        .configure_change(ConfigurationChange::HandoverCount(0))
        .unwrap();
    drop(configuration);
    let running = crate::store::FlowStore::open_configured(path).unwrap();
    let before = running.configuration_snapshot().unwrap().unwrap();
    let mut nexus = before.nexus.clone();
    nexus.curriculum_nexus_path.push_str("-changed");
    running
        .configure_change(ConfigurationChange::Nexus(nexus.clone()))
        .unwrap();
    let after = running.configuration_snapshot().unwrap().unwrap();
    assert_eq!(after.nexus, nexus);
    let mut normalized = after.clone();
    normalized.nexus = before.nexus.clone();
    assert_eq!(normalized, before);
    drop(running);
    let reopened = crate::store::ConfigurationOnlyStore::open(path).unwrap();
    let persisted = reopened.configuration_snapshot().unwrap().unwrap();
    assert_eq!(persisted.handover_policies, before.handover_policies);
    assert_eq!(persisted.handover_archive, before.handover_archive);
    assert_eq!(persisted.handover_count, before.handover_count);
    assert_eq!(persisted.models, before.models);
    assert_eq!(persisted.thresholds, before.thresholds);
}

#[test]
fn current_handover_configuration_catalog_reopens_idempotently() {
    let fixture = ConfiguredStore::new();
    let path = fixture.path();
    let configuration = crate::store::ConfigurationOnlyStore::open(path).unwrap();
    configuration
        .configure_change(ConfigurationChange::HandoverPolicies(vec![policy()]))
        .unwrap();
    configuration
        .configure_change(ConfigurationChange::HandoverArchive("/srv/legacy".into()))
        .unwrap();
    configuration
        .configure_change(ConfigurationChange::HandoverCount(0))
        .unwrap();
    drop(configuration);
    for _ in 0..2 {
        let reopened = crate::store::ConfigurationOnlyStore::open(path).unwrap();
        let snapshot = reopened.configuration_snapshot().unwrap().unwrap();
        assert_eq!(
            snapshot.handover_policies,
            meta_signal_flow::HandoverPoliciesConfiguration::Configured(vec![policy()])
        );
        assert_eq!(
            snapshot.handover_archive,
            meta_signal_flow::HandoverArchiveConfiguration::Configured("/srv/legacy".into())
        );
        assert_eq!(
            snapshot.handover_count,
            meta_signal_flow::HandoverCountConfiguration::Configured(0)
        );
        drop(reopened);
    }
}

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

#[test]
fn publication_deadline_defaults_positive_and_persists_override() {
    let fixture = ConfiguredStore::new();
    let path = fixture.path();
    let configuration = crate::store::ConfigurationOnlyStore::open(path).unwrap();
    assert_eq!(
        configuration
            .configuration_snapshot()
            .unwrap()
            .unwrap()
            .publication_deadline,
        meta_signal_flow::PublicationDeadlineConfiguration::Configured(60_000)
    );
    configuration
        .configure_change(ConfigurationChange::PublicationDeadline(125))
        .unwrap();
    assert_eq!(
        configuration
            .configuration_snapshot()
            .unwrap()
            .unwrap()
            .publication_deadline,
        meta_signal_flow::PublicationDeadlineConfiguration::Configured(125)
    );
    drop(configuration);
    let reopened = crate::store::ConfigurationOnlyStore::open(path).unwrap();
    assert_eq!(
        reopened
            .configuration_snapshot()
            .unwrap()
            .unwrap()
            .publication_deadline,
        meta_signal_flow::PublicationDeadlineConfiguration::Configured(125)
    );
}

#[test]
fn publication_deadline_rejects_zero_and_negative_without_mutation() {
    let fixture = ConfiguredStore::new();
    let configuration = crate::store::ConfigurationOnlyStore::open(fixture.path()).unwrap();
    let before = configuration.configuration_snapshot().unwrap();
    for duration in [0, -1] {
        assert!(matches!(
            configuration.configure_change(ConfigurationChange::PublicationDeadline(duration)),
            Err(crate::store::StoreError::HandoverConfigurationRefused(
                meta_signal_flow::HandoverConfigurationRefusal::InvalidPublicationDeadline
            ))
        ));
    }
    assert_eq!(configuration.configuration_snapshot().unwrap(), before);
}
