//! Consumer-side assembly for an already accepted native prompt module plan.
//!
//! Curriculum remains the source of module data. This module neither reads
//! Curriculum files nor assigns aspect, power, programming, or model tiers.

use thiserror::Error;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ModuleId(String);

impl ModuleId {
    pub(crate) fn parse(value: impl Into<String>) -> Result<Self, PromptModuleError> {
        let value = value.into();
        if value.trim().is_empty() {
            return Err(PromptModuleError::EmptyModuleId);
        }
        Ok(Self(value))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum NativePromptModule {
    Text {
        id: ModuleId,
        body: String,
    },
    Skill {
        id: ModuleId,
        name: String,
        path: String,
    },
}

impl NativePromptModule {
    fn native_input(&self) -> Result<serde_json::Value, PromptModuleError> {
        match self {
            Self::Text { body, .. } => {
                if body.trim().is_empty() {
                    return Err(PromptModuleError::EmptyTextModule);
                }
                Ok(serde_json::json!({ "type": "text", "text": body }))
            }
            Self::Skill { name, path, .. } => {
                if name.trim().is_empty() || path.trim().is_empty() {
                    return Err(PromptModuleError::IncompleteSkillModule);
                }
                Ok(serde_json::json!({ "type": "skill", "name": name, "path": path }))
            }
        }
    }

    fn id(&self) -> &ModuleId {
        match self {
            Self::Text { id, .. } | Self::Skill { id, .. } => id,
        }
    }
}

/// Exactly the modules accepted by upstream policy for one native launch.
///
/// The six fields encode cardinality only: the adapter receives one accepted
/// Spirit, Intent, aspect, power, programming, and launch-specific module. It
/// does not interpret their IDs or decide which programming content is core or
/// extended.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct AcceptedPromptModules {
    universal_spirit: Option<NativePromptModule>,
    universal_intent: Option<NativePromptModule>,
    aspect: Option<NativePromptModule>,
    power: Option<NativePromptModule>,
    programming: Option<NativePromptModule>,
    launch: Option<NativePromptModule>,
}

impl AcceptedPromptModules {
    pub(crate) fn empty() -> Self {
        Self {
            universal_spirit: None,
            universal_intent: None,
            aspect: None,
            power: None,
            programming: None,
            launch: None,
        }
    }

    pub(crate) fn accepted(
        universal_spirit: NativePromptModule,
        universal_intent: NativePromptModule,
        aspect: NativePromptModule,
        power: NativePromptModule,
        programming: NativePromptModule,
        launch: NativePromptModule,
    ) -> Result<Self, PromptModuleError> {
        let modules = [
            &universal_spirit,
            &universal_intent,
            &aspect,
            &power,
            &programming,
            &launch,
        ];
        for (index, module) in modules.iter().enumerate() {
            if modules[..index]
                .iter()
                .any(|prior| prior.id() == module.id())
            {
                return Err(PromptModuleError::DuplicateModuleId);
            }
            module.native_input()?;
        }
        Ok(Self {
            universal_spirit: Some(universal_spirit),
            universal_intent: Some(universal_intent),
            aspect: Some(aspect),
            power: Some(power),
            programming: Some(programming),
            launch: Some(launch),
        })
    }

    pub(crate) fn native_inputs(&self) -> Vec<serde_json::Value> {
        [
            self.universal_spirit.as_ref(),
            self.universal_intent.as_ref(),
            self.aspect.as_ref(),
            self.power.as_ref(),
            self.programming.as_ref(),
            self.launch.as_ref(),
        ]
        .into_iter()
        .flatten()
        .filter_map(|module| module.native_input().ok())
        .collect()
    }
}

#[derive(Debug, Error, Eq, PartialEq)]
pub(crate) enum PromptModuleError {
    #[error("a prompt module ID must not be empty")]
    EmptyModuleId,
    #[error("a text prompt module must not be empty")]
    EmptyTextModule,
    #[error("a native skill module requires both name and path")]
    IncompleteSkillModule,
    #[error("each accepted prompt module must have a distinct ID")]
    DuplicateModuleId,
}
