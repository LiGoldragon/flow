//! Durable named cohesion, independent of permaflow declarations and runs.

use super::*;
use crate::launching::NamesTopic;
use flow_ethos::{Metaflow, MetaflowName};

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Debug, Clone, PartialEq, Eq)]
pub(crate) struct StoredMetaflowCohesion {
    pub metaflow: Metaflow,
}

impl EngineRecord for StoredMetaflowCohesion {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(self.metaflow.metaflow_name.0.clone())
    }
}

impl MemoryDefinition for StoredMetaflowCohesion {
    fn memory_version() -> MemoryVersion {
        MemoryVersion::new(1)
    }

    fn schema_hash() -> SchemaHash {
        SchemaHash::for_label("flow-nexus-metaflow-cohesion-v1")
    }
}

pub(crate) trait DescribesMetaflowCohesion {
    fn descriptor() -> TableDescriptor<StoredMetaflowCohesion>;
}

impl DescribesMetaflowCohesion for StoredMetaflowCohesion {
    fn descriptor() -> TableDescriptor<StoredMetaflowCohesion> {
        TableDescriptor::new(
            TableName::new("flow_nexus_metaflow_cohesions"),
            FamilyName::new("flow-nexus-metaflow-cohesion"),
            Self::schema_hash(),
        )
    }
}

pub(crate) trait ValidatesMetaflowName {
    fn validate(&self) -> Result<(), StoreError>;
}

impl ValidatesMetaflowName for MetaflowName {
    fn validate(&self) -> Result<(), StoreError> {
        if self.0.as_str().is_camel_case_topic() {
            Ok(())
        } else {
            Err(StoreError::MetaflowNameRefused(self.clone()))
        }
    }
}

pub(crate) trait CanonicalizesMetaflow {
    fn canonicalize(&mut self) -> Result<(), StoreError>;
}

impl CanonicalizesMetaflow for Metaflow {
    fn canonicalize(&mut self) -> Result<(), StoreError> {
        self.metaflow_name.validate()?;
        for address in &self.address_vector {
            if !address.topic.0.as_str().is_camel_case_topic() {
                return Err(StoreError::TopicRefused(address.topic.0.clone()));
            }
        }
        self.address_vector
            .sort_by_key(FlowStore::address_record_key);
        self.address_vector.dedup();
        Ok(())
    }
}

pub(crate) trait ConfiguresMetaflowCohesions {
    fn cohesion(
        &self,
        engine: &Engine,
        name: &MetaflowName,
    ) -> Result<Option<Metaflow>, StoreError>;
    fn set_cohesion(
        &self,
        engine: &Engine,
        metaflow: Metaflow,
    ) -> Result<ConfigureOutcome, StoreError>;
    fn join_cohesion(
        &self,
        engine: &Engine,
        join: meta_signal_flow::Join_Data,
    ) -> Result<ConfigureOutcome, StoreError>;
    fn leave_cohesion(
        &self,
        engine: &Engine,
        leave: meta_signal_flow::Leave_Data,
    ) -> Result<ConfigureOutcome, StoreError>;
    fn dissolve_cohesion(
        &self,
        engine: &Engine,
        name: MetaflowName,
    ) -> Result<ConfigureOutcome, StoreError>;
}

impl ConfiguresMetaflowCohesions for MetaConfigurationTables {
    fn cohesion(
        &self,
        engine: &Engine,
        name: &MetaflowName,
    ) -> Result<Option<Metaflow>, StoreError> {
        name.validate()?;
        let rows = engine.match_records(QueryPlan::key(
            self.cohesions,
            RecordKey::new(name.0.clone()),
        ))?;
        match rows.records() {
            [] => Ok(None),
            [row] => Ok(Some(row.metaflow.clone())),
            _ => Err(StoreError::StateInvariant),
        }
    }

    fn set_cohesion(
        &self,
        engine: &Engine,
        mut metaflow: Metaflow,
    ) -> Result<ConfigureOutcome, StoreError> {
        metaflow.canonicalize()?;
        FlowStore::replace_configuration_entry(
            engine,
            self.cohesions,
            StoredMetaflowCohesion { metaflow },
        )
    }

    fn join_cohesion(
        &self,
        engine: &Engine,
        join: meta_signal_flow::Join_Data,
    ) -> Result<ConfigureOutcome, StoreError> {
        let mut metaflow = self
            .cohesion(engine, &join.metaflow_name)?
            .ok_or(StoreError::UnknownMetaflow(join.metaflow_name))?;
        metaflow.address_vector.push(join.address);
        self.set_cohesion(engine, metaflow)
    }

    fn leave_cohesion(
        &self,
        engine: &Engine,
        leave: meta_signal_flow::Leave_Data,
    ) -> Result<ConfigureOutcome, StoreError> {
        let Some(mut metaflow) = self.cohesion(engine, &leave.metaflow_name)? else {
            return Ok(ConfigureOutcome::Unchanged);
        };
        metaflow
            .address_vector
            .retain(|address| address != &leave.address);
        self.set_cohesion(engine, metaflow)
    }

    fn dissolve_cohesion(
        &self,
        engine: &Engine,
        name: MetaflowName,
    ) -> Result<ConfigureOutcome, StoreError> {
        name.validate()?;
        FlowStore::remove_configuration_entry(engine, self.cohesions, RecordKey::new(name.0))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn address(topic: &str) -> Address {
        Address {
            aspect: Aspect::Mind,
            topic: flow_ethos::Topic(topic.into()),
            model: Model::Sol,
        }
    }

    #[test]
    fn canonicalization_retains_valid_undeclared_members_and_refuses_invalid_names_and_topics() {
        let first = address("firstPeer");
        let second = address("secondPeer");
        let mut metaflow = Metaflow {
            metaflow_name: MetaflowName("sharedWork".into()),
            address_vector: vec![second.clone(), first.clone(), second.clone()],
        };
        metaflow.canonicalize().unwrap();
        assert_eq!(metaflow.address_vector, vec![first, second]);
        for name in ["", "SharedWork", "shared-work", "shared work"] {
            assert!(matches!(
                MetaflowName(name.into()).validate(),
                Err(StoreError::MetaflowNameRefused(_))
            ));
        }
        metaflow.address_vector.push(address("InvalidTopic"));
        assert!(matches!(
            metaflow.canonicalize(),
            Err(StoreError::TopicRefused(_))
        ));
    }

    #[test]
    fn cohesion_changes_are_durable_idempotent_and_failed_changes_write_nothing() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("flow.sema");
        let name = MetaflowName("sharedWork".into());
        let first = address("firstPeer");
        let second = address("secondPeer");
        {
            let store = ConfigurationOnlyStore::open(&path).unwrap();
            assert_eq!(
                store
                    .configure_change(ConfigurationChange::Metaflow(Metaflow {
                        metaflow_name: name.clone(),
                        address_vector: vec![second.clone(), first.clone(), second.clone()],
                    }))
                    .unwrap(),
                ConfigureOutcome::Replaced
            );
            let marker = store.engine.current_database_marker().unwrap();
            assert_eq!(
                store
                    .configure_change(ConfigurationChange::Join(meta_signal_flow::Join_Data {
                        metaflow_name: name.clone(),
                        address: first.clone(),
                    }))
                    .unwrap(),
                ConfigureOutcome::Unchanged
            );
            assert_eq!(store.engine.current_database_marker().unwrap(), marker);
            for change in [
                ConfigurationChange::Join(meta_signal_flow::Join_Data {
                    metaflow_name: MetaflowName("missing".into()),
                    address: first.clone(),
                }),
                ConfigurationChange::Metaflow(Metaflow {
                    metaflow_name: MetaflowName("Invalid".into()),
                    address_vector: vec![],
                }),
                ConfigurationChange::Metaflow(Metaflow {
                    metaflow_name: name.clone(),
                    address_vector: vec![address("InvalidTopic")],
                }),
            ] {
                assert!(store.configure_change(change).is_err());
                assert_eq!(store.engine.current_database_marker().unwrap(), marker);
            }
        }
        let store = ConfigurationOnlyStore::open(&path).unwrap();
        let tables = &store.meta_configuration_tables;
        assert_eq!(
            tables
                .cohesion(&store.engine, &name)
                .unwrap()
                .unwrap()
                .address_vector,
            vec![first.clone(), second.clone()]
        );
        assert_eq!(
            store
                .configure_change(ConfigurationChange::Leave(meta_signal_flow::Leave_Data {
                    metaflow_name: name.clone(),
                    address: first.clone()
                }))
                .unwrap(),
            ConfigureOutcome::Replaced
        );
        assert_eq!(
            store
                .configure_change(ConfigurationChange::Leave(meta_signal_flow::Leave_Data {
                    metaflow_name: name.clone(),
                    address: first
                }))
                .unwrap(),
            ConfigureOutcome::Unchanged
        );
        assert_eq!(
            store
                .configure_change(ConfigurationChange::Dissolve(name.clone()))
                .unwrap(),
            ConfigureOutcome::Replaced
        );
        assert_eq!(
            store
                .configure_change(ConfigurationChange::Dissolve(name.clone()))
                .unwrap(),
            ConfigureOutcome::Unchanged
        );
        assert!(tables.cohesion(&store.engine, &name).unwrap().is_none());
        assert_eq!(
            store
                .configure_change(ConfigurationChange::Leave(meta_signal_flow::Leave_Data {
                    metaflow_name: name,
                    address: second
                }))
                .unwrap(),
            ConfigureOutcome::Unchanged
        );
    }
}
