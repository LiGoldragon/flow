//! Consumer-side assembly for an already accepted native prompt module plan.
//!
//! Curriculum remains the source of module data. This module neither reads
//! Curriculum files nor assigns aspect, power, programming, or model tiers.

use signal_flow::HerdrRoute;
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
    start_time: i64,
}

impl ProcessIncarnation {
    /// Converts the authoritative unsigned native observation to the i64
    /// representation used by Flow's persisted delivery binding.
    pub(super) fn from_observed(pid: u32, start_time: u64) -> Result<Self, BindingHandoffError> {
        if pid == 0 {
            return Err(BindingHandoffError::InvalidProcessIncarnation);
        }
        let start_time =
            i64::try_from(start_time).map_err(|_| BindingHandoffError::ProcessStartOutOfRange)?;
        Ok(Self { pid, start_time })
    }

    pub(crate) fn pid(&self) -> u32 {
        self.pid
    }

    pub(crate) fn start_time(&self) -> i64 {
        self.start_time
    }
}

/// The durable Flow lifecycle generation supplied by the registered candidate.
/// The evidence verifier must independently confirm it is still current.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct LifecycleGeneration(u64);

impl LifecycleGeneration {
    pub(super) fn from_observed(value: u64) -> Self {
        Self(value)
    }

    pub(crate) fn value(self) -> u64 {
        self.0
    }
}

/// Observations collected by the adapter before it presents a candidate to
/// Flow. This stays private to the adapter module: callers can receive only a
/// `VerifiedNativeBindingHandoff`.
#[derive(Clone, Debug, Eq, PartialEq)]
struct CurrentNativeBindingEvidence {
    correlation: BindingCorrelation,
    lifecycle_generation: LifecycleGeneration,
    native_thread: NativeThreadIdentity,
    harness_session: HarnessSessionIdentity,
    route: HerdrRoute,
    endpoint: EndpointIdentity,
    process: ProcessIncarnation,
    accepted_profile: AcceptedProfileIdentity,
    context_receipt: ContextReceiptIdentity,
    readiness_receipt: ReadinessReceiptIdentity,
    proof_digest: ProofDigest,
}

impl CurrentNativeBindingEvidence {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn observed(
        correlation: BindingCorrelation,
        lifecycle_generation: LifecycleGeneration,
        native_thread: NativeThreadIdentity,
        harness_session: HarnessSessionIdentity,
        route: HerdrRoute,
        endpoint: EndpointIdentity,
        process: ProcessIncarnation,
        accepted_profile: AcceptedProfileIdentity,
        context_receipt: ContextReceiptIdentity,
        readiness_receipt: ReadinessReceiptIdentity,
        proof_digest: ProofDigest,
    ) -> Self {
        Self {
            correlation,
            lifecycle_generation,
            native_thread,
            harness_session,
            route,
            endpoint,
            process,
            accepted_profile,
            context_receipt,
            readiness_receipt,
            proof_digest,
        }
    }
}

/// Implemented by the adapter's native/Herdr receipt reader. Source-shaped
/// data cannot satisfy this trait by itself: every method is a current,
/// independent observation of the supplied evidence.
pub(super) trait VerifiesCurrentNativeBinding {
    fn lifecycle_generation_is_current(&self, evidence: &CurrentNativeBindingEvidence) -> bool;
    fn native_thread_is_current(&self, evidence: &CurrentNativeBindingEvidence) -> bool;
    fn harness_session_is_current(&self, evidence: &CurrentNativeBindingEvidence) -> bool;
    fn route_is_current(&self, evidence: &CurrentNativeBindingEvidence) -> bool;
    fn endpoint_is_current(&self, evidence: &CurrentNativeBindingEvidence) -> bool;
    fn process_is_current(&self, evidence: &CurrentNativeBindingEvidence) -> bool;
    fn profile_is_accepted(&self, evidence: &CurrentNativeBindingEvidence) -> bool;
    fn context_is_verified(&self, evidence: &CurrentNativeBindingEvidence) -> bool;
    fn readiness_is_current(&self, evidence: &CurrentNativeBindingEvidence) -> bool;
    fn proof_is_verified(&self, evidence: &CurrentNativeBindingEvidence) -> bool;
}

/// The adapter-owned validator is the only constructor for a verified
/// handoff. Its expected correlation is supplied from Flow's launch attempt;
/// the adapter must also have independently observed every native field.
pub(crate) struct NativeBindingValidator<V> {
    expected: BindingCorrelation,
    verifier: V,
}

impl<V: VerifiesCurrentNativeBinding> NativeBindingValidator<V> {
    pub(super) fn for_correlation(expected: BindingCorrelation, verifier: V) -> Self {
        Self { expected, verifier }
    }

    pub(super) fn validate(
        &self,
        evidence: CurrentNativeBindingEvidence,
    ) -> Result<VerifiedNativeBindingHandoff, BindingHandoffError> {
        if evidence.correlation != self.expected {
            return Err(BindingHandoffError::CorrelationMismatch);
        }
        if !self.verifier.lifecycle_generation_is_current(&evidence) {
            return Err(BindingHandoffError::LifecycleGenerationNotCurrent);
        }
        if !self.verifier.native_thread_is_current(&evidence) {
            return Err(BindingHandoffError::NativeThreadNotCurrent);
        }
        if !self.verifier.harness_session_is_current(&evidence) {
            return Err(BindingHandoffError::HarnessSessionNotCurrent);
        }
        if !self.verifier.route_is_current(&evidence) {
            return Err(BindingHandoffError::RouteNotCurrent);
        }
        if !self.verifier.endpoint_is_current(&evidence) {
            return Err(BindingHandoffError::EndpointNotCurrent);
        }
        if !self.verifier.process_is_current(&evidence) {
            return Err(BindingHandoffError::ProcessNotCurrent);
        }
        if !self.verifier.profile_is_accepted(&evidence) {
            return Err(BindingHandoffError::ProfileNotAccepted);
        }
        if !self.verifier.context_is_verified(&evidence) {
            return Err(BindingHandoffError::ContextNotVerified);
        }
        if !self.verifier.readiness_is_current(&evidence) {
            return Err(BindingHandoffError::ReadinessNotCurrent);
        }
        if !self.verifier.proof_is_verified(&evidence) {
            return Err(BindingHandoffError::ProofNotVerified);
        }
        Ok(VerifiedNativeBindingHandoff { evidence })
    }
}

/// Private typed producer value for Flow's independent cross-check against
/// `FlowNode` before it records a verified binding.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct VerifiedNativeBindingHandoff {
    evidence: CurrentNativeBindingEvidence,
}

impl VerifiedNativeBindingHandoff {
    pub(crate) fn correlation(&self) -> &BindingCorrelation {
        &self.evidence.correlation
    }

    pub(crate) fn lifecycle_generation(&self) -> LifecycleGeneration {
        self.evidence.lifecycle_generation
    }

    pub(crate) fn native_thread(&self) -> &NativeThreadIdentity {
        &self.evidence.native_thread
    }

    pub(crate) fn harness_session(&self) -> &HarnessSessionIdentity {
        &self.evidence.harness_session
    }

    pub(crate) fn route(&self) -> &HerdrRoute {
        &self.evidence.route
    }

    pub(crate) fn endpoint(&self) -> &EndpointIdentity {
        &self.evidence.endpoint
    }

    pub(crate) fn process(&self) -> &ProcessIncarnation {
        &self.evidence.process
    }

    pub(crate) fn accepted_profile(&self) -> &AcceptedProfileIdentity {
        &self.evidence.accepted_profile
    }

    pub(crate) fn context_receipt(&self) -> &ContextReceiptIdentity {
        &self.evidence.context_receipt
    }

    pub(crate) fn readiness_receipt(&self) -> &ReadinessReceiptIdentity {
        &self.evidence.readiness_receipt
    }

    pub(crate) fn proof_digest(&self) -> &ProofDigest {
        &self.evidence.proof_digest
    }
}

#[derive(Debug, Error, Eq, PartialEq)]
pub(crate) enum BindingHandoffError {
    #[error("a binding identity must not be empty")]
    EmptyIdentity,
    #[error("a process incarnation requires a nonzero PID and start identity")]
    InvalidProcessIncarnation,
    #[error("the observed process start time does not fit Flow's i64 binding field")]
    ProcessStartOutOfRange,
    #[error("adapter observations do not match the Flow launch correlation")]
    CorrelationMismatch,
    #[error("lifecycle generation is not current")]
    LifecycleGenerationNotCurrent,
    #[error("native thread is not current")]
    NativeThreadNotCurrent,
    #[error("harness session is not current")]
    HarnessSessionNotCurrent,
    #[error("Herdr route is not current")]
    RouteNotCurrent,
    #[error("endpoint is not current")]
    EndpointNotCurrent,
    #[error("PID/start incarnation is not current")]
    ProcessNotCurrent,
    #[error("profile is not accepted")]
    ProfileNotAccepted,
    #[error("native context is not verified")]
    ContextNotVerified,
    #[error("native readiness receipt is not current")]
    ReadinessNotCurrent,
    #[error("native proof digest is not verified")]
    ProofNotVerified,
}

#[cfg(test)]
mod binding_tests {
    use super::*;

    struct StaleReadiness;

    impl VerifiesCurrentNativeBinding for StaleReadiness {
        fn lifecycle_generation_is_current(&self, _: &CurrentNativeBindingEvidence) -> bool {
            true
        }
        fn native_thread_is_current(&self, _: &CurrentNativeBindingEvidence) -> bool {
            true
        }
        fn harness_session_is_current(&self, _: &CurrentNativeBindingEvidence) -> bool {
            true
        }
        fn route_is_current(&self, _: &CurrentNativeBindingEvidence) -> bool {
            true
        }
        fn endpoint_is_current(&self, _: &CurrentNativeBindingEvidence) -> bool {
            true
        }
        fn process_is_current(&self, _: &CurrentNativeBindingEvidence) -> bool {
            true
        }
        fn profile_is_accepted(&self, _: &CurrentNativeBindingEvidence) -> bool {
            true
        }
        fn context_is_verified(&self, _: &CurrentNativeBindingEvidence) -> bool {
            true
        }
        fn readiness_is_current(&self, _: &CurrentNativeBindingEvidence) -> bool {
            false
        }
        fn proof_is_verified(&self, _: &CurrentNativeBindingEvidence) -> bool {
            true
        }
    }

    #[test]
    fn stale_readiness_receipt_cannot_construct_a_verified_handoff() {
        let correlation = BindingCorrelation::new(
            FlowIdentity::parse("flow").expect("fixture flow"),
            RegistrationIdentity::parse("registration").expect("fixture registration"),
            None,
        );
        let evidence = CurrentNativeBindingEvidence::observed(
            correlation.clone(),
            LifecycleGeneration::from_observed(7),
            NativeThreadIdentity::parse("thread").expect("fixture thread"),
            HarnessSessionIdentity::parse("session").expect("fixture session"),
            HerdrRoute {
                herdr_session_name: "session".into(),
                herdr_agent_name: "agent".into(),
                herdr_pane_id: "pane".into(),
                herdr_terminal_id: "terminal".into(),
            },
            EndpointIdentity::parse("endpoint").expect("fixture endpoint"),
            ProcessIncarnation::from_observed(42, 1_700_000_000).expect("fixture process"),
            AcceptedProfileIdentity::parse("profile").expect("fixture profile"),
            ContextReceiptIdentity::parse("context").expect("fixture context"),
            ReadinessReceiptIdentity::parse("readiness").expect("fixture readiness"),
            ProofDigest::parse("digest").expect("fixture digest"),
        );
        let result =
            NativeBindingValidator::for_correlation(correlation, StaleReadiness).validate(evidence);
        assert_eq!(
            result.unwrap_err(),
            BindingHandoffError::ReadinessNotCurrent
        );
    }
}
