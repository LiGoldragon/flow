#![allow(dead_code, non_camel_case_types, non_snake_case)]
#[rustfmt::skip]
pub type ModuleIdentity = String;
#[rustfmt::skip]
pub type RepositoryName = String;
#[rustfmt::skip]
pub type SourceRevision = String;
#[rustfmt::skip]
pub type SourceRoot = String;
#[rustfmt::skip]
pub type SourceRelativePath = String;
#[rustfmt::skip]
pub type ModuleType = String;
#[rustfmt::skip]
pub type ModuleSubject = String;
#[rustfmt::skip]
pub type DeclaredPlacement = String;
#[rustfmt::skip]
pub type SourceSha256 = String;
#[rustfmt::skip]
pub type BodySha256 = String;
#[rustfmt::skip]
pub type RenderedSha256 = String;
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum Target {
    Claude,
    Codex,
    Pi,
    OpenCode,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct CallerMetaflow {
    pub first_string: String,
    pub second_string: String,
    pub third_string: String,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum StartupPlacement {
    System,
    User,
}
#[rustfmt::skip]
pub type SystemModules = std::vec::Vec<ModuleIdentity>;
#[rustfmt::skip]
pub type UserModules = std::vec::Vec<ModuleIdentity>;
#[rustfmt::skip]
pub type OmittedModules = std::vec::Vec<ModuleIdentity>;
#[rustfmt::skip]
pub type FlowId = String;
#[rustfmt::skip]
pub type SystemText = String;
#[rustfmt::skip]
pub type UserText = String;
#[rustfmt::skip]
pub type RecordsText = String;
#[rustfmt::skip]
pub type SystemPayloadSha256 = String;
#[rustfmt::skip]
pub type UserPayloadSha256 = String;
#[rustfmt::skip]
pub type RecordsPayloadSha256 = String;
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct StartupSelection {
    pub caller_metaflow: CallerMetaflow,
    pub target: Target,
    pub system_modules: SystemModules,
    pub user_modules: UserModules,
    pub flow_id_vector: std::vec::Vec<FlowId>,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct ContextProvenance {
    pub module_identity: ModuleIdentity,
    pub repository_name: RepositoryName,
    pub source_revision: SourceRevision,
    pub source_relative_path: SourceRelativePath,
    pub source_sha256: SourceSha256,
    pub body_sha256: BodySha256,
    pub rendered_sha256: RenderedSha256,
    pub startup_placement: StartupPlacement,
    pub integer: i64,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct StartupComposition {
    pub system_text: SystemText,
    pub user_text: UserText,
    pub records_text: RecordsText,
    pub system_payload_sha256: SystemPayloadSha256,
    pub user_payload_sha256: UserPayloadSha256,
    pub records_payload_sha256: RecordsPayloadSha256,
    pub omitted_modules: OmittedModules,
    pub context_provenance_vector: std::vec::Vec<ContextProvenance>,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct ResolvedModule {
    pub module_identity: ModuleIdentity,
    pub repository_name: RepositoryName,
    pub source_revision: SourceRevision,
    pub source_root: SourceRoot,
    pub source_relative_path: SourceRelativePath,
    pub module_type: ModuleType,
    pub module_subject: ModuleSubject,
    pub declared_placement: DeclaredPlacement,
    pub source_sha256: SourceSha256,
    pub body_sha256: BodySha256,
    pub string: String,
    pub string_vector: std::vec::Vec<String>,
}
#[rustfmt::skip]
pub type DeploymentDestination = String;
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct RepositoryRevision {
    pub repository_name: RepositoryName,
    pub source_revision: SourceRevision,
}
#[rustfmt::skip]
pub type PublishedInventoryDigest = String;
#[rustfmt::skip]
pub type PublishedContentDigest = String;
#[rustfmt::skip]
pub type RendererVersion = String;
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct ProjectionCandidate {
    pub deployment_destination: DeploymentDestination,
    pub repository_revision_vector: std::vec::Vec<RepositoryRevision>,
    pub published_inventory_digest: PublishedInventoryDigest,
    pub published_content_digest: PublishedContentDigest,
    pub renderer_version: RendererVersion,
}
#[rustfmt::skip]
pub type RoleName = String;
#[rustfmt::skip]
pub type ModelId = String;
#[rustfmt::skip]
pub type DescriptionModule = ModuleIdentity;
#[rustfmt::skip]
pub type ContextModules = std::vec::Vec<ModuleIdentity>;
#[rustfmt::skip]
pub type DescriptionText = String;
#[rustfmt::skip]
pub type ContextText = String;
#[rustfmt::skip]
pub type DescriptionPayloadSha256 = String;
#[rustfmt::skip]
pub type ContextPayloadSha256 = String;
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum Permission {
    Restricted,
    Unrestricted,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum Effort {
    Low,
    Medium,
    High,
    Xhigh,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum Surface {
    ClaudeAgent,
    CodexAgent,
    PiAgent,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum RoleTools {
    Inherit,
    Explicit(std::vec::Vec<String>),
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum RoleCallers {
    Any,
    Only(std::vec::Vec<CallerMetaflow>),
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct RoleDefinition {
    pub role_name: RoleName,
    pub surface: Surface,
    pub model_id: ModelId,
    pub effort: Effort,
    pub permission: Permission,
    pub description_module: DescriptionModule,
    pub context_modules: ContextModules,
    pub role_tools: RoleTools,
    pub role_callers: RoleCallers,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct Roles {
    pub role_definition_vector: std::vec::Vec<RoleDefinition>,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum RolesDocument {
    Roles(Roles),
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct RoleSelection {
    pub role_name: RoleName,
    pub surface: Surface,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub struct RoleComposition {
    pub role_name: RoleName,
    pub surface: Surface,
    pub model_id: ModelId,
    pub effort: Effort,
    pub permission: Permission,
    pub description_text: DescriptionText,
    pub context_text: ContextText,
    pub role_tools: RoleTools,
    pub role_callers: RoleCallers,
    pub omitted_modules: OmittedModules,
    pub context_provenance_vector: std::vec::Vec<ContextProvenance>,
    pub description_payload_sha256: DescriptionPayloadSha256,
    pub context_payload_sha256: ContextPayloadSha256,
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum CliRequest {
    ResolveSkills(std::vec::Vec<ModuleIdentity>),
    ResolveSources(std::vec::Vec<ModuleIdentity>),
    ComposeStartup(StartupSelection),
    ComposeRole(RoleSelection),
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum Query {
    ResolveSkills(std::vec::Vec<ModuleIdentity>),
    ResolveSources(std::vec::Vec<ModuleIdentity>),
    ComposeStartup(StartupSelection),
    ComposeRole(RoleSelection),
}
#[rustfmt::skip]
#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "datom", derive(datom_codec::Datomizable, datom_codec::Composing))]
pub enum Outcome {
    ResolvedSkills(std::vec::Vec<ModuleIdentity>),
    ResolvedSources(std::vec::Vec<ResolvedModule>),
    StartupComposed(StartupComposition),
    RoleComposed(RoleComposition),
    Rejected(String),
}
