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

/// A correlation chosen by Flow before native launch. Every observation used
/// to make a binding handoff must carry this exact value.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct BindingCorrelation {
    flow_id: FlowIdentity,
    registration_id: RegistrationIdentity,
    refresh_transition: Option<RefreshTransitionIdentity>,
}

impl BindingCorrelation {
    pub(super) fn new(
        flow_id: FlowIdentity,
        registration_id: RegistrationIdentity,
        refresh_transition: Option<RefreshTransitionIdentity>,
    ) -> Self {
        Self {
            flow_id,
            registration_id,
            refresh_transition,
        }
    }

    pub(crate) fn flow_id(&self) -> &FlowIdentity {
        &self.flow_id
    }

    pub(crate) fn registration_id(&self) -> &RegistrationIdentity {
        &self.registration_id
    }

    pub(crate) fn refresh_transition(&self) -> Option<&RefreshTransitionIdentity> {
        self.refresh_transition.as_ref()
    }
}

macro_rules! opaque_identity {
    ($name:ident) => {
        #[derive(Clone, Debug, Eq, PartialEq)]
        pub(crate) struct $name(String);

        impl $name {
            pub(super) fn parse(value: impl Into<String>) -> Result<Self, BindingHandoffError> {
                let value = value.into();
                if value.trim().is_empty() {
                    return Err(BindingHandoffError::EmptyIdentity);
                }
                Ok(Self(value))
            }

            pub(crate) fn as_str(&self) -> &str {
                &self.0
            }
        }
    };
}

opaque_identity!(FlowIdentity);
opaque_identity!(RegistrationIdentity);
opaque_identity!(RefreshTransitionIdentity);
opaque_identity!(NativeThreadIdentity);
opaque_identity!(HarnessSessionIdentity);
opaque_identity!(RouteIdentity);
opaque_identity!(EndpointIdentity);
opaque_identity!(AcceptedProfileIdentity);
opaque_identity!(ContextReceiptIdentity);
opaque_identity!(ReadinessReceiptIdentity);
opaque_identity!(ProofDigest);

/// PID and start identity travel together so a reused PID is not accepted as
/// the native process observed by the adapter.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ProcessIncarnation {
    pid: u32,
    start_identity: String,
}

impl ProcessIncarnation {
    pub(super) fn new(
        pid: u32,
        start_identity: impl Into<String>,
    ) -> Result<Self, BindingHandoffError> {
        let start_identity = start_identity.into();
        if pid == 0 || start_identity.trim().is_empty() {
            return Err(BindingHandoffError::InvalidProcessIncarnation);
        }
        Ok(Self {
            pid,
            start_identity,
        })
    }

    pub(crate) fn pid(&self) -> u32 {
        self.pid
    }

    pub(crate) fn start_identity(&self) -> &str {
        &self.start_identity
    }
}

/// Observations collected by the adapter before it presents a candidate to
/// Flow. This stays private to the adapter module: callers can receive only a
/// `VerifiedNativeBindingHandoff`.
#[derive(Clone, Debug, Eq, PartialEq)]
struct ObservedNativeBinding {
    correlation: BindingCorrelation,
    native_thread: NativeThreadIdentity,
    harness_session: HarnessSessionIdentity,
    route: RouteIdentity,
    endpoint: EndpointIdentity,
    process: ProcessIncarnation,
    accepted_profile: AcceptedProfileIdentity,
    context_receipt: ContextReceiptIdentity,
    readiness_receipt: ReadinessReceiptIdentity,
    proof_digest: ProofDigest,
}

/// The adapter-owned validator is the only constructor for a verified
/// handoff. Its expected correlation is supplied from Flow's launch attempt;
/// the adapter must also have independently observed every native field.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct NativeBindingValidator {
    expected: BindingCorrelation,
}

impl NativeBindingValidator {
    pub(super) fn for_correlation(expected: BindingCorrelation) -> Self {
        Self { expected }
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn validate(
        &self,
        observed_correlation: BindingCorrelation,
        native_thread: NativeThreadIdentity,
        harness_session: HarnessSessionIdentity,
        route: RouteIdentity,
        endpoint: EndpointIdentity,
        process: ProcessIncarnation,
        accepted_profile: AcceptedProfileIdentity,
        context_receipt: ContextReceiptIdentity,
        readiness_receipt: ReadinessReceiptIdentity,
        proof_digest: ProofDigest,
    ) -> Result<VerifiedNativeBindingHandoff, BindingHandoffError> {
        if observed_correlation != self.expected {
            return Err(BindingHandoffError::CorrelationMismatch);
        }
        let observed = ObservedNativeBinding {
            correlation: observed_correlation,
            native_thread,
            harness_session,
            route,
            endpoint,
            process,
            accepted_profile,
            context_receipt,
            readiness_receipt,
            proof_digest,
        };
        Ok(VerifiedNativeBindingHandoff { observed })
    }
}

/// Private typed producer value for Flow's independent cross-check against
/// `FlowNode` before it records a verified binding.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct VerifiedNativeBindingHandoff {
    observed: ObservedNativeBinding,
}

impl VerifiedNativeBindingHandoff {
    pub(crate) fn correlation(&self) -> &BindingCorrelation {
        &self.observed.correlation
    }

    pub(crate) fn native_thread(&self) -> &NativeThreadIdentity {
        &self.observed.native_thread
    }

    pub(crate) fn harness_session(&self) -> &HarnessSessionIdentity {
        &self.observed.harness_session
    }

    pub(crate) fn route(&self) -> &RouteIdentity {
        &self.observed.route
    }

    pub(crate) fn endpoint(&self) -> &EndpointIdentity {
        &self.observed.endpoint
    }

    pub(crate) fn process(&self) -> &ProcessIncarnation {
        &self.observed.process
    }

    pub(crate) fn accepted_profile(&self) -> &AcceptedProfileIdentity {
        &self.observed.accepted_profile
    }

    pub(crate) fn context_receipt(&self) -> &ContextReceiptIdentity {
        &self.observed.context_receipt
    }

    pub(crate) fn readiness_receipt(&self) -> &ReadinessReceiptIdentity {
        &self.observed.readiness_receipt
    }

    pub(crate) fn proof_digest(&self) -> &ProofDigest {
        &self.observed.proof_digest
    }
}

#[derive(Debug, Error, Eq, PartialEq)]
pub(crate) enum BindingHandoffError {
    #[error("a binding identity must not be empty")]
    EmptyIdentity,
    #[error("a process incarnation requires a nonzero PID and start identity")]
    InvalidProcessIncarnation,
    #[error("adapter observations do not match the Flow launch correlation")]
    CorrelationMismatch,
}
