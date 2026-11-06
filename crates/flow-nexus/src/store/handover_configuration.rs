use meta_signal_flow::{HandoverArchiveConfiguration,HandoverCountConfiguration,HandoverPoliciesConfiguration};
use rkyv::{Archive,Deserialize as RkyvDeserialize,Serialize as RkyvSerialize};
use sema_engine::{EngineRecord,FamilyName,MemoryDefinition,MemoryVersion,RecordKey,SchemaHash,TableDescriptor};
use sema_engine::TableName;
const HANDOVER_CONFIGURATION_TABLE_NAME: TableName = TableName::new("flow_nexus_meta_handover_configuration");
#[derive(Archive,RkyvSerialize,RkyvDeserialize,Debug,Clone,PartialEq,Eq)]
pub(crate) struct StoredHandoverConfiguration { pub policies:HandoverPoliciesConfiguration, pub archive:HandoverArchiveConfiguration, pub count:HandoverCountConfiguration }
impl EngineRecord for StoredHandoverConfiguration { fn record_key(&self)->RecordKey { RecordKey::new("handover") } }
impl MemoryDefinition for StoredHandoverConfiguration { fn memory_version()->MemoryVersion { MemoryVersion::new(1) } fn schema_hash()->SchemaHash { SchemaHash::for_label("flow-nexus-handover-configuration-v1") } }
pub(crate) trait DescribesHandoverConfiguration { fn descriptor()->TableDescriptor<StoredHandoverConfiguration>; }
impl DescribesHandoverConfiguration for StoredHandoverConfiguration { fn descriptor()->TableDescriptor<StoredHandoverConfiguration> { TableDescriptor::new(HANDOVER_CONFIGURATION_TABLE_NAME,FamilyName::new("flow-nexus-handover-configuration"),Self::schema_hash()) } }
pub(crate) fn unconfigured()->StoredHandoverConfiguration { StoredHandoverConfiguration { policies:HandoverPoliciesConfiguration::Unconfigured, archive:HandoverArchiveConfiguration::Unconfigured, count:HandoverCountConfiguration::Unconfigured } }
