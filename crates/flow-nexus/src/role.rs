//! A flow's subagent roles: composed by Curriculum, verified, offered to
//! the Claude flows their caller policy permits, and guarded at dispatch.
//!
//! Flow selects each role by name (`LaysOutDefaults::SUBAGENT_ROLES`) and
//! asks Curriculum for it with `ComposeRole` on the `ClaudeAgent` surface.
//! The only way its text reaches a harness is through [`VerifiedRole`],
//! whose one constructor is [`VerifiesRole::verified_role`]: the answer
//! must name the role and surface asked for, both payload SHA-256s are
//! recomputed, the provenance is a manifest of unique modules in order,
//! the description is exactly the first module's rendering and the prompt
//! exactly the rendering of the rest, and every module comes from one of
//! Curriculum's three context repositories at one full revision per
//! repository.
//!
//! A starting or resumed Claude flow is given, in one inline `--agents`
//! argument, the definitions of the roles whose caller policy names its
//! Address exactly, case preserved. Each definition carries the role's
//! text and policy in the fields Claude Code 2.1.294 reads for a flag
//! agent (`description`, `prompt`, `model`, `effort`, `permissionMode`,
//! `tools`); a policy field with no native counterpart refuses the launch
//! rather than being dropped.
//!
//! The offer is not the guard. `flow-hook` runs on every PreToolUse of
//! the Agent tool and asks [`AuthorizesAgentCall::decide`]: a call naming
//! a role Flow selects is permitted only when it is made by the flow's
//! main thread, overrides neither model nor effort, and the calling flow,
//! as the Nexus resolves the kernel-named calling process, is one the
//! role's caller policy names. An unavailable composer or an unresolved
//! caller denies.

use crate::context::{CurriculumComposer, CutsRenderedText, ExchangesCurriculumFrames};
use crate::generated::curriculum::{
    CallerMetaflow, ContextProvenance, Effort, Outcome as CurriculumOutcome, Permission,
    Query as CurriculumQuery, RoleCallers, RoleComposition, RoleSelection, RoleTools,
    StartupPlacement, Surface,
};
pub use crate::generated::role::{
    Addressed, AgentCall, AgentCaller, AgentCalling, AgentDecision, AgentDenial,
    AuthorizesAgentCall, ComposesRole, ComposesSubagentRoles, OpensSubagentRoles,
    ReadsVerifiedRole, ResolvesAgentCaller, RoleComposing, RoleRefusal, RoleSelecting,
    SubagentRoleNames, VerifiesRole,
};
use crate::generated::starter::OpensCurriculumComposer;
use flow_defaults::{DefaultConfiguration, LaysOutDefaults, ReadsAnchors};
use sha2::{Digest, Sha256};
use signal_flow::{Address, Aspect, Layer};
use std::collections::{BTreeMap, BTreeSet};

/// Curriculum's three context repositories, as its provenance names them.
const CONTEXT_REPOSITORIES: [&str; 3] = ["psyche-context", "mind-context", "field-context"];

/// The composer a role is asked of: Curriculum, or, under test, roles
/// held in memory.
#[derive(Debug, Clone)]
pub enum RoleComposer {
    Curriculum(CurriculumComposer),
    #[cfg(test)]
    Fixture(FixtureRoles),
}

/// The roles Flow selects by name, and the composer they are asked of.
#[derive(Debug, Clone)]
pub struct SubagentRoles {
    pub(crate) composer: RoleComposer,
    pub(crate) names: SubagentRoleNames,
}

impl OpensSubagentRoles for SubagentRoles {
    fn from_environment() -> Self {
        Self {
            composer: RoleComposer::Curriculum(CurriculumComposer::from_environment()),
            names: DefaultConfiguration::from_environment().subagent_role_names(),
        }
    }
}

/// The metaflow tuple an Address is in a caller policy: aspect, topic and
/// layer as their names, the topic exactly as written.
impl From<&Address> for CallerMetaflow {
    fn from(address: &Address) -> Self {
        Self {
            first_string: match address.aspect {
                Aspect::Psyche => "Psyche",
                Aspect::Mind => "Mind",
                Aspect::Field => "Field",
            }
            .into(),
            second_string: address.topic.clone(),
            third_string: match address.layer {
                Layer::Primary => "Primary",
                Layer::Secondary => "Secondary",
                Layer::Tertiary => "Tertiary",
                Layer::Quaternary => "Quaternary",
            }
            .into(),
        }
    }
}

impl ComposesRole for CurriculumComposer {
    fn compose_role<N: RoleSelecting>(&self, input: N) -> Result<RoleComposition, RoleRefusal> {
        match self.exchange(CurriculumQuery::ComposeRole(input.role_selection())) {
            Ok(CurriculumOutcome::RoleComposed(composition)) => Ok(composition),
            Ok(CurriculumOutcome::Rejected(reason)) => {
                eprintln!("flow: Curriculum rejected the role request: {reason}");
                Err(RoleRefusal::ComposerRejected)
            }
            Ok(_) => Err(RoleRefusal::ComposerRejected),
            Err(error) => {
                eprintln!("flow: role composer unavailable: {error}");
                Err(RoleRefusal::ComposerUnavailable)
            }
        }
    }
}

impl ComposesRole for RoleComposer {
    fn compose_role<N: RoleSelecting>(&self, input: N) -> Result<RoleComposition, RoleRefusal> {
        match self {
            Self::Curriculum(composer) => composer.compose_role(input),
            #[cfg(test)]
            Self::Fixture(composer) => composer.compose_role(input),
        }
    }
}

/// A role composition Flow has verified. Its fields are private and its
/// only constructor is [`VerifiesRole::verified_role`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedRole {
    role_name: String,
    model_id: String,
    effort: Effort,
    permission: Permission,
    description_text: String,
    context_text: String,
    role_tools: RoleTools,
    role_callers: RoleCallers,
    provenance: Vec<ContextProvenance>,
}

/// The repository revisions a set of provenance rows names: one full
/// revision per repository, from Curriculum's three only.
trait NamesRevisions {
    fn revisions(&self) -> Option<BTreeMap<String, String>>;
}

impl NamesRevisions for [ContextProvenance] {
    fn revisions(&self) -> Option<BTreeMap<String, String>> {
        let hexadecimal = |text: &str, length: usize| {
            text.len() == length
                && text
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        };
        let mut revisions = BTreeMap::new();
        for row in self {
            if !CONTEXT_REPOSITORIES.contains(&row.repository_name.as_str())
                || !hexadecimal(&row.source_revision, 40)
                || !hexadecimal(&row.source_sha256, 64)
                || !hexadecimal(&row.body_sha256, 64)
                || !hexadecimal(&row.rendered_sha256, 64)
            {
                return None;
            }
            let earlier =
                revisions.insert(row.repository_name.clone(), row.source_revision.clone());
            if earlier.is_some_and(|earlier| earlier != row.source_revision) {
                return None;
            }
        }
        Some(revisions)
    }
}

impl VerifiesRole for VerifiedRole {
    fn verified_role<N: RoleSelecting, O: RoleComposing>(
        input_0: N,
        input_1: O,
    ) -> Result<Self, RoleRefusal> {
        let selection = input_0.role_selection();
        let composition = input_1.role_composition();
        if composition.role_name != selection.role_name || composition.surface != selection.surface
        {
            return Err(RoleRefusal::RoleMismatch);
        }
        let digest = |text: &str| format!("{:x}", Sha256::digest(text.as_bytes()));
        if digest(&composition.description_text) != composition.description_payload_sha256
            || digest(&composition.context_text) != composition.context_payload_sha256
        {
            return Err(RoleRefusal::PayloadMismatch);
        }
        let rows = &composition.context_provenance_vector;
        let mut identities = BTreeSet::new();
        let manifest = rows.len() >= 2
            && rows.iter().enumerate().all(|(ordinal, row)| {
                row.integer == ordinal as i64
                    && row.startup_placement == StartupPlacement::System
                    && identities.insert(row.module_identity.as_str())
            });
        let policy = !composition.model_id.is_empty()
            && match &composition.role_tools {
                RoleTools::Inherit => true,
                RoleTools::Explicit(tools) => {
                    let mut seen = BTreeSet::new();
                    tools
                        .iter()
                        .all(|tool| !tool.is_empty() && seen.insert(tool.as_str()))
                }
            }
            && match &composition.role_callers {
                RoleCallers::Any => true,
                RoleCallers::Only(callers) => !callers.is_empty(),
            };
        if !manifest || !policy {
            return Err(RoleRefusal::ManifestMismatch);
        }
        if rows[0].rendered_sha256 != composition.description_payload_sha256
            || composition
                .context_text
                .rendered_segments(&rows[1..])
                .is_none()
        {
            return Err(RoleRefusal::PayloadMismatch);
        }
        if rows.revisions().is_none() {
            return Err(RoleRefusal::ProvenanceMismatch);
        }
        Ok(Self {
            role_name: composition.role_name,
            model_id: composition.model_id,
            effort: composition.effort,
            permission: composition.permission,
            description_text: composition.description_text,
            context_text: composition.context_text,
            role_tools: composition.role_tools,
            role_callers: composition.role_callers,
            provenance: composition.context_provenance_vector,
        })
    }
}

impl ReadsVerifiedRole for VerifiedRole {
    fn role_name(&self) -> String {
        self.role_name.clone()
    }

    fn permits<N: Addressed>(&self, input: N) -> bool {
        match &self.role_callers {
            RoleCallers::Any => true,
            RoleCallers::Only(callers) => {
                let caller = CallerMetaflow::from(&input.addressed());
                callers.contains(&caller)
            }
        }
    }

    /// The role as a Claude Code flag agent. Model is passed as the role
    /// names it; effort as Claude's own level; Unrestricted as
    /// `bypassPermissions`; an explicit tool list as `tools`, and an
    /// inherited one by leaving `tools` out, which Claude reads as
    /// inheriting the parent's tools. Restricted, and a model of
    /// `inherit`, have no exact native counterpart and refuse.
    fn native_definition(&self) -> Result<serde_json::Value, RoleRefusal> {
        if self.model_id.eq_ignore_ascii_case("inherit") {
            return Err(RoleRefusal::UnmappableField);
        }
        let effort = match self.effort {
            Effort::Low => "low",
            Effort::Medium => "medium",
            Effort::High => "high",
            Effort::Xhigh => "xhigh",
        };
        let permission_mode = match self.permission {
            Permission::Unrestricted => "bypassPermissions",
            Permission::Restricted => return Err(RoleRefusal::UnmappableField),
        };
        let mut definition = serde_json::json!({
            "description": self.description_text,
            "prompt": self.context_text,
            "model": self.model_id,
            "effort": effort,
            "permissionMode": permission_mode,
        });
        if let RoleTools::Explicit(tools) = &self.role_tools {
            definition["tools"] = serde_json::json!(tools);
        }
        Ok(definition)
    }
}

impl ComposesSubagentRoles for SubagentRoles {
    fn governs<N: AgentCalling>(&self, input: N) -> bool {
        self.names.contains(&input.agent_call().subagent_type)
    }

    /// Every selected role, composed and verified, then kept when its
    /// caller policy permits the Address. One unavailable or unverifiable
    /// role refuses them all, as do two roles naming one repository at
    /// different revisions.
    fn permitted_roles<N: Addressed>(&self, input: N) -> Result<Vec<VerifiedRole>, RoleRefusal> {
        let address = input.addressed();
        let mut names = BTreeSet::new();
        let mut cohort = BTreeMap::new();
        let mut permitted = Vec::new();
        for name in &self.names {
            if !names.insert(name.as_str()) {
                return Err(RoleRefusal::ManifestMismatch);
            }
            let selection = RoleSelection {
                role_name: name.clone(),
                surface: Surface::ClaudeAgent,
            };
            let composition = self.composer.compose_role(&selection)?;
            let role = VerifiedRole::verified_role(&selection, composition)?;
            for (repository, revision) in role
                .provenance
                .revisions()
                .ok_or(RoleRefusal::ProvenanceMismatch)?
            {
                let earlier = cohort.insert(repository, revision.clone());
                if earlier.is_some_and(|earlier| earlier != revision) {
                    return Err(RoleRefusal::ProvenanceMismatch);
                }
            }
            if role.permits(&address) {
                permitted.push(role);
            }
        }
        Ok(permitted)
    }

    /// The one inline `--agents` JSON object, role name to definition;
    /// none when no role permits the Address.
    fn agents_argument<N: Addressed>(&self, input: N) -> Result<Option<String>, RoleRefusal> {
        let roles = self.permitted_roles(input)?;
        if roles.is_empty() {
            return Ok(None);
        }
        let mut agents = serde_json::Map::new();
        for role in &roles {
            agents.insert(role.role_name(), role.native_definition()?);
        }
        Ok(Some(serde_json::Value::Object(agents).to_string()))
    }
}

impl AuthorizesAgentCall for SubagentRoles {
    fn decide<N: AgentCalling, O: ResolvesAgentCaller>(
        &self,
        input_0: N,
        input_1: O,
    ) -> AgentDecision {
        let call = input_0.agent_call();
        if !self.governs(&call) {
            return AgentDecision::Ungoverned;
        }
        if call.nested {
            return AgentDecision::Denied(AgentDenial::NestedCaller);
        }
        if call.requests_model {
            return AgentDecision::Denied(AgentDenial::ModelOverridden);
        }
        if call.requests_effort {
            return AgentDecision::Denied(AgentDenial::EffortOverridden);
        }
        let selection = RoleSelection {
            role_name: call.subagent_type.clone(),
            surface: Surface::ClaudeAgent,
        };
        let Ok(role) = self
            .composer
            .compose_role(&selection)
            .and_then(|composition| VerifiedRole::verified_role(&selection, composition))
        else {
            return AgentDecision::Denied(AgentDenial::PolicyUnavailable);
        };
        match input_1.agent_caller() {
            AgentCaller::Unresolved => AgentDecision::Denied(AgentDenial::CallerUnresolved),
            AgentCaller::Resolved(address) if role.permits(&address) => AgentDecision::Permitted,
            AgentCaller::Resolved(_) => AgentDecision::Denied(AgentDenial::CallerNotPermitted),
        }
    }
}

/// Roles held in memory for tests, composed as Curriculum composes them:
/// each a definition and its modules' bodies, all from one repository
/// revision.
#[cfg(test)]
#[derive(Debug, Clone, Default)]
pub struct FixtureRoles {
    pub roles: BTreeMap<String, FixtureRole>,
}

#[cfg(test)]
#[derive(Debug, Clone)]
pub struct FixtureRole {
    pub model_id: String,
    pub effort: Effort,
    pub permission: Permission,
    pub modules: Vec<(String, String)>,
    pub role_tools: RoleTools,
    pub role_callers: RoleCallers,
}

#[cfg(test)]
impl ComposesRole for FixtureRoles {
    fn compose_role<N: RoleSelecting>(&self, input: N) -> Result<RoleComposition, RoleRefusal> {
        let selection = input.role_selection();
        let role = self
            .roles
            .get(&selection.role_name)
            .filter(|_| selection.surface == Surface::ClaudeAgent)
            .ok_or(RoleRefusal::ComposerRejected)?;
        let digest = |text: &str| format!("{:x}", Sha256::digest(text.as_bytes()));
        let description_text = role.modules[0].1.clone();
        let context = &role.modules[1..];
        let mut context_text = format!(
            "{}\n\nModule IDs: {}\n",
            crate::context::CONTEXT_OPENING,
            context
                .iter()
                .map(|(identity, _)| identity.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        );
        for (identity, body) in context {
            context_text.push_str(&format!(
                "\n<!-- module: {identity} -->\n{body}\n<!-- end-module: {identity} -->\n"
            ));
        }
        Ok(RoleComposition {
            role_name: selection.role_name.clone(),
            surface: Surface::ClaudeAgent,
            model_id: role.model_id.clone(),
            effort: role.effort.clone(),
            permission: role.permission.clone(),
            description_payload_sha256: digest(&description_text),
            context_payload_sha256: digest(&context_text),
            description_text,
            context_text,
            role_tools: role.role_tools.clone(),
            role_callers: role.role_callers.clone(),
            context_provenance_vector: role
                .modules
                .iter()
                .enumerate()
                .map(|(ordinal, (identity, body))| ContextProvenance {
                    module_identity: identity.clone(),
                    repository_name: "field-context".into(),
                    source_revision: "a".repeat(40),
                    source_relative_path: format!("{identity}.md"),
                    source_sha256: digest(body),
                    body_sha256: digest(body),
                    rendered_sha256: digest(body),
                    startup_placement: StartupPlacement::System,
                    integer: ordinal as i64,
                })
                .collect(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{
        AgentCall, AgentCaller, AgentDecision, AgentDenial, AuthorizesAgentCall, ComposesRole,
        ComposesSubagentRoles, FixtureRole, FixtureRoles, ReadsVerifiedRole, RoleComposer,
        RoleRefusal, SubagentRoles, VerifiedRole, VerifiesRole,
    };
    use crate::context::{CurriculumComposer, OpensCurriculumComposer};
    use crate::generated::curriculum::{
        CallerMetaflow, Effort, Permission, RoleCallers, RoleSelection, RoleTools, Surface,
    };
    use signal_flow::{Address, Aspect, Layer};

    fn comment_relay() -> FixtureRole {
        FixtureRole {
            model_id: "sonnet".into(),
            effort: Effort::Medium,
            permission: Permission::Unrestricted,
            modules: vec![
                (
                    "role-comment-relay-description".into(),
                    "Relays artifact comments.".into(),
                ),
                ("spirit-core".into(), "Spirit body.".into()),
                ("intent-context".into(), "Intent body.".into()),
                ("role-comment-relay".into(), "Relay body.".into()),
            ],
            role_tools: RoleTools::Explicit(vec![
                "Read".into(),
                "Bash".into(),
                "ToolSearch".into(),
                "ArtifactData".into(),
                "ArtifactComments".into(),
            ]),
            role_callers: RoleCallers::Only(vec![CallerMetaflow {
                first_string: "Psyche".into(),
                second_string: "core".into(),
                third_string: "Tertiary".into(),
            }]),
        }
    }

    fn roles() -> SubagentRoles {
        SubagentRoles {
            composer: RoleComposer::Fixture(FixtureRoles {
                roles: [("comment-relay".to_owned(), comment_relay())].into(),
            }),
            names: vec!["comment-relay".into()],
        }
    }

    fn address(aspect: Aspect, topic: &str, layer: Layer) -> Address {
        Address {
            aspect,
            topic: topic.into(),
            layer,
        }
    }

    fn selection() -> RoleSelection {
        RoleSelection {
            role_name: "comment-relay".into(),
            surface: Surface::ClaudeAgent,
        }
    }

    fn call() -> AgentCall {
        AgentCall {
            subagent_type: "comment-relay".into(),
            nested: false,
            requests_model: false,
            requests_effort: false,
        }
    }

    #[test]
    fn the_permitted_flow_is_offered_the_role_as_one_flag_agent() {
        let argument = roles()
            .agents_argument(address(Aspect::Psyche, "core", Layer::Tertiary))
            .unwrap()
            .expect("Psyche core Tertiary is offered comment-relay");
        let agents: serde_json::Value = serde_json::from_str(&argument).unwrap();
        let relay = &agents["comment-relay"];
        assert_eq!(relay["description"], "Relays artifact comments.");
        assert_eq!(relay["model"], "sonnet");
        assert_eq!(relay["effort"], "medium");
        assert_eq!(relay["permissionMode"], "bypassPermissions");
        assert_eq!(
            relay["tools"],
            serde_json::json!([
                "Read",
                "Bash",
                "ToolSearch",
                "ArtifactData",
                "ArtifactComments"
            ])
        );
        let prompt = relay["prompt"].as_str().unwrap();
        assert!(prompt.starts_with(crate::context::CONTEXT_OPENING));
        assert!(prompt.contains("\n<!-- module: role-comment-relay -->\nRelay body.\n"));
        assert_eq!(agents.as_object().unwrap().len(), 1);
    }

    #[test]
    fn a_flow_the_policy_does_not_name_exactly_is_offered_nothing() {
        for other in [
            address(Aspect::Psyche, "Core", Layer::Tertiary),
            address(Aspect::Psyche, "core", Layer::Secondary),
            address(Aspect::Mind, "core", Layer::Tertiary),
        ] {
            assert_eq!(roles().agents_argument(other), Ok(None));
        }
    }

    #[test]
    fn an_unavailable_composer_refuses_the_offer() {
        let roles = SubagentRoles {
            composer: RoleComposer::Curriculum(CurriculumComposer::at(
                "/nonexistent/curriculum.sock",
            )),
            names: vec!["comment-relay".into()],
        };
        assert_eq!(
            roles.agents_argument(address(Aspect::Psyche, "core", Layer::Tertiary)),
            Err(RoleRefusal::ComposerUnavailable)
        );
    }

    #[test]
    fn a_composition_that_differs_from_what_it_claims_is_refused() {
        let fixture = FixtureRoles {
            roles: [("comment-relay".to_owned(), comment_relay())].into(),
        };
        let composed = fixture.compose_role(selection()).unwrap();
        assert!(VerifiedRole::verified_role(selection(), composed.clone()).is_ok());

        let mut renamed = composed.clone();
        renamed.role_name = "other".into();
        assert_eq!(
            VerifiedRole::verified_role(selection(), renamed),
            Err(RoleRefusal::RoleMismatch)
        );

        let mut altered = composed.clone();
        altered.context_text.push('x');
        assert_eq!(
            VerifiedRole::verified_role(selection(), altered),
            Err(RoleRefusal::PayloadMismatch)
        );

        let mut slice = composed.clone();
        slice.context_provenance_vector[2].rendered_sha256 = "0".repeat(64);
        assert_eq!(
            VerifiedRole::verified_role(selection(), slice),
            Err(RoleRefusal::PayloadMismatch)
        );

        let mut duplicated = composed.clone();
        duplicated.context_provenance_vector[2].module_identity = "spirit-core".into();
        assert_eq!(
            VerifiedRole::verified_role(selection(), duplicated),
            Err(RoleRefusal::ManifestMismatch)
        );

        let mut foreign = composed.clone();
        foreign.context_provenance_vector[1].repository_name = "workspace".into();
        assert_eq!(
            VerifiedRole::verified_role(selection(), foreign),
            Err(RoleRefusal::ProvenanceMismatch)
        );

        let mut split = composed;
        split.context_provenance_vector[1].source_revision = "b".repeat(40);
        assert_eq!(
            VerifiedRole::verified_role(selection(), split),
            Err(RoleRefusal::ProvenanceMismatch)
        );
    }

    #[test]
    fn a_policy_field_with_no_native_counterpart_refuses() {
        let mut restricted = comment_relay();
        restricted.permission = Permission::Restricted;
        let roles = SubagentRoles {
            composer: RoleComposer::Fixture(FixtureRoles {
                roles: [("comment-relay".to_owned(), restricted)].into(),
            }),
            names: vec!["comment-relay".into()],
        };
        assert_eq!(
            roles.agents_argument(address(Aspect::Psyche, "core", Layer::Tertiary)),
            Err(RoleRefusal::UnmappableField)
        );
        let fixture = FixtureRoles {
            roles: [("comment-relay".to_owned(), comment_relay())].into(),
        };
        let mut inherit = fixture.compose_role(selection()).unwrap();
        inherit.model_id = "inherit".into();
        assert_eq!(
            VerifiedRole::verified_role(selection(), inherit)
                .unwrap()
                .native_definition(),
            Err(RoleRefusal::UnmappableField)
        );
    }

    #[test]
    fn an_agent_call_is_permitted_only_for_the_named_main_thread_caller() {
        let permitted = AgentCaller::Resolved(address(Aspect::Psyche, "core", Layer::Tertiary));
        assert_eq!(
            roles().decide(call(), permitted.clone()),
            AgentDecision::Permitted
        );
        let mut nested = call();
        nested.nested = true;
        assert_eq!(
            roles().decide(nested, permitted.clone()),
            AgentDecision::Denied(AgentDenial::NestedCaller)
        );
        let mut model = call();
        model.requests_model = true;
        assert_eq!(
            roles().decide(model, permitted.clone()),
            AgentDecision::Denied(AgentDenial::ModelOverridden)
        );
        let mut effort = call();
        effort.requests_effort = true;
        assert_eq!(
            roles().decide(effort, permitted.clone()),
            AgentDecision::Denied(AgentDenial::EffortOverridden)
        );
        assert_eq!(
            roles().decide(call(), AgentCaller::Unresolved),
            AgentDecision::Denied(AgentDenial::CallerUnresolved)
        );
        assert_eq!(
            roles().decide(
                call(),
                AgentCaller::Resolved(address(Aspect::Psyche, "Core", Layer::Tertiary))
            ),
            AgentDecision::Denied(AgentDenial::CallerNotPermitted)
        );
        let unavailable = SubagentRoles {
            composer: RoleComposer::Curriculum(CurriculumComposer::at(
                "/nonexistent/curriculum.sock",
            )),
            names: vec!["comment-relay".into()],
        };
        assert_eq!(
            unavailable.decide(call(), permitted.clone()),
            AgentDecision::Denied(AgentDenial::PolicyUnavailable)
        );
        let mut general = call();
        general.subagent_type = "general-purpose".into();
        general.nested = true;
        assert_eq!(
            roles().decide(general, permitted),
            AgentDecision::Ungoverned
        );
    }
}
