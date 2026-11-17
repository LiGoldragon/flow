//! A flow's context: the module identities its launch names, composed by
//! Curriculum and verified before any of its text enters a prompt.
//!
//! A launch profile names its context only by module identity, for the
//! system prompt and for the first USER message. Flow asks the context
//! composer for both texts and the provenance of every module in them, and
//! the only way that text reaches a prompt is through [`VerifiedContext`],
//! whose one constructor is [`VerifiesContext::verified`]: the payload
//! SHA-256s are recomputed, every requested identity must be in the
//! provenance at its placement, and each text must be exactly the rendering
//! of its modules, each module's slice matching its rendered SHA-256. No
//! path, file or inline context text is accepted anywhere on this route.
//!
//! The production composer is the Curriculum Nexus, asked over the socket
//! the Nexus's configuration names (`CurriculumNexusPath`),
//! with `ComposeStartup` from its contract, signal-curriculum-nexus, which
//! Curriculum compiles too; its selection names the flows a launch remembers
//! and whose composition carries their raw records. Its Output includes
//! `Rejected` with Curriculum's own text, which Flow
//! keeps whole in `ContextRefusal::ComposerRejected`, but for a remembered
//! flow's records missing, `RecordMissing`. Its answer names no
//! metaflow, so the metaflow check is Flow's own: the request carries the
//! launch's Address, and the composed launch keeps it with the identities
//! asked for it.

use crate::generated::emission::{CurriculumExchange, Emittable};
pub use crate::generated::starter::{
    ComposesFlowContext, ContextRequest, ContextRequesting, CutsRenderedText,
    OpensCurriculumComposer, ParsesCurriculumIdentity, ProvenanceListing, ReadsVerifiedContext,
    RendersCurriculumIdentity, SelectsStartup, StartupComposing, Textual, VerifiesContext,
};
use crate::model::RunsModel;
use sha2::{Digest, Sha256};
use signal_curriculum_nexus::{
    CallerMetaflow, ContextProvenance, Input as CurriculumQuery, Output as CurriculumOutcome,
    StartupComposition, StartupPlacement, StartupSelection, Target,
};
use signal_flow::{Address, Aspect, ContextRefusal, HarnessKind};
use std::path::PathBuf;

/// The Curriculum Nexus, asked over its Unix socket: one big-endian u32
/// length-prefixed rkyv `Query` frame out, one `Outcome` frame back.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CurriculumComposer {
    pub(crate) socket: PathBuf,
}
/// The opening line of a rendered context text.
pub const CONTEXT_OPENING: &str = "These are your skills, given here; do not reload them.";

/// A Curriculum composer at the socket a text names: the Nexus's
/// configured `CurriculumNexusPath`.
impl OpensCurriculumComposer for CurriculumComposer {
    fn at<N: Textual>(input: N) -> Self {
        Self {
            socket: PathBuf::from(input.text()),
        }
    }
}

/// Anything that composes a flow's context for a request: the Curriculum
/// Nexus in production, whatever a caller injects otherwise. It also
/// answers whether it holds one module, which a module added to a
/// permaflow is asked before it is written.
pub trait ComposesContext: ResolvesModule + std::fmt::Debug + Send + Sync {
    fn compose(&self, request: &ContextRequest) -> Result<StartupComposition, ContextRefusal>;
}

impl<T: ComposesFlowContext + ResolvesModule + std::fmt::Debug + Send + Sync> ComposesContext
    for T
{
    fn compose(&self, request: &ContextRequest) -> Result<StartupComposition, ContextRefusal> {
        self.compose_flow_context(request)
    }
}

/// What a context composer answers for one module identity: it holds the
/// module, it does not, or it could not be asked.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModuleStanding {
    Held,
    Unknown,
    Unreachable,
}

/// Whether the context composer holds one module.
pub trait ResolvesModule {
    fn resolve_module(&self, identity: &signal_flow::ModuleIdentity) -> ModuleStanding;
}

/// The composer of a store opened with none, for configuration only: it
/// can be asked nothing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NoContextComposer;

impl ResolvesModule for NoContextComposer {
    fn resolve_module(&self, _identity: &signal_flow::ModuleIdentity) -> ModuleStanding {
        ModuleStanding::Unreachable
    }
}

impl ResolvesModule for ContextComposer {
    fn resolve_module(&self, identity: &signal_flow::ModuleIdentity) -> ModuleStanding {
        self.0.resolve_module(identity)
    }
}

impl ParsesCurriculumIdentity for str {
    fn curriculum_identity(&self) -> Option<signal_curriculum_nexus::ModuleIdentity> {
        use signal_curriculum_nexus::{
            First, Last, ModuleIdentity, ModuleKind, ModuleSubject, Span_Data, Version, Versions,
        };
        let fields = self.split(':').collect::<Vec<_>>();
        let [kind, subject, version] = fields.as_slice() else {
            return None;
        };
        let module_kind = match *kind {
            "spirit" => ModuleKind::Spirit,
            "intent" => ModuleKind::Intent,
            "vision" => ModuleKind::Vision,
            "knowledge" => ModuleKind::Knowledge,
            "notion" => ModuleKind::Notion,
            "inquiry" => ModuleKind::Inquiry,
            "compensation" => ModuleKind::Compensation,
            "role" => ModuleKind::Role,
            "raw" => ModuleKind::Raw,
            _ => return None,
        };
        if subject.is_empty()
            || !subject
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
        {
            return None;
        }
        let number = |value: &str| -> Option<Version> {
            if value.is_empty()
                || (value != "0" && value.starts_with('0'))
                || !value.bytes().all(|b| b.is_ascii_digit())
            {
                return None;
            }
            Some(Version(value.parse().ok()?))
        };
        let versions = match version.split_once('-') {
            Some((first, last)) => {
                let first = number(first)?;
                let last = number(last)?;
                if first.0 >= last.0 {
                    return None;
                }
                Versions::Span(Span_Data {
                    first: First(first),
                    last: Last(last),
                })
            }
            None => Versions::Single(number(version)?),
        };
        Some(ModuleIdentity {
            module_kind,
            module_subject: ModuleSubject((*subject).into()),
            versions,
        })
    }
}

impl RendersCurriculumIdentity for signal_curriculum_nexus::ModuleIdentity {
    fn identity_text(&self) -> String {
        use signal_curriculum_nexus::{ModuleKind, Versions};
        let kind = match self.module_kind {
            ModuleKind::Spirit => "spirit",
            ModuleKind::Intent => "intent",
            ModuleKind::Vision => "vision",
            ModuleKind::Knowledge => "knowledge",
            ModuleKind::Notion => "notion",
            ModuleKind::Inquiry => "inquiry",
            ModuleKind::Compensation => "compensation",
            ModuleKind::Role => "role",
            ModuleKind::Raw => "raw",
        };
        let version = match &self.versions {
            Versions::Single(version) => version.0.to_string(),
            Versions::Span(span) => format!("{}-{}", span.first.0.0, span.last.0.0),
        };
        format!("{kind}:{}:{version}", self.module_subject.0)
    }

    fn matches_binding<N: Textual>(&self, input: N) -> bool {
        use signal_curriculum_nexus::Versions;
        // Validate actual identities too: wire scalars may carry invalid subjects or bounds.
        if self.identity_text().as_str().curriculum_identity().as_ref() != Some(self) {
            return false;
        }
        let Some(binding) = input.text().as_str().curriculum_identity() else {
            return false;
        };
        if self.module_kind != binding.module_kind || self.module_subject != binding.module_subject
        {
            return false;
        }
        match (&binding.versions, &self.versions) {
            (Versions::Single(version), Versions::Span(span)) => {
                span.first.0.0 <= version.0 && version.0 <= span.last.0.0
            }
            _ => self.versions == binding.versions,
        }
    }
}

/// The Curriculum Nexus resolves the one module by `ResolveSources`, as
/// composition does: an answer naming it is Held; Curriculum's own
/// "missing module" refusal is Unknown; a socket that cannot be reached, an
/// answer that cannot be read, or any other refusal is Unreachable.
impl ResolvesModule for CurriculumComposer {
    fn resolve_module(&self, identity: &signal_flow::ModuleIdentity) -> ModuleStanding {
        let Some(typed) = identity.0.as_str().curriculum_identity() else {
            return ModuleStanding::Unknown;
        };
        match CurriculumQuery::ResolveSources(vec![typed]).emit(self) {
            CurriculumExchange::Answered(CurriculumOutcome::SourcesResolved(modules))
                if modules
                    .iter()
                    .any(|module| module.module_identity.matches_binding(identity.0.as_str())) =>
            {
                ModuleStanding::Held
            }
            CurriculumExchange::Answered(CurriculumOutcome::SourcesResolved(_)) => {
                ModuleStanding::Unknown
            }
            CurriculumExchange::Answered(CurriculumOutcome::Rejected(reason))
                if reason.starts_with("missing module") =>
            {
                ModuleStanding::Unknown
            }
            CurriculumExchange::Answered(CurriculumOutcome::Rejected(reason)) => {
                eprintln!(
                    "flow-nexus: Curriculum could not resolve {}: {reason}",
                    identity.0
                );
                ModuleStanding::Unreachable
            }
            CurriculumExchange::Answered(_) => ModuleStanding::Unreachable,
            CurriculumExchange::Unexchanged(error) => {
                eprintln!(
                    "flow-nexus: Curriculum unreachable resolving {}: {error}",
                    identity.0
                );
                ModuleStanding::Unreachable
            }
        }
    }
}

/// The context composer a Nexus runs with: the Curriculum Nexus, injected
/// where the Nexus is opened.
#[derive(Debug, Clone)]
pub struct ContextComposer(pub std::sync::Arc<dyn ComposesContext>);

impl ComposesFlowContext for ContextComposer {
    fn compose_flow_context<N: ContextRequesting>(
        &self,
        input: N,
    ) -> Result<StartupComposition, ContextRefusal> {
        self.0.compose(&input.context_request())
    }
}

impl SelectsStartup for ContextRequest {
    fn startup_selection(&self) -> Result<StartupSelection, ContextRefusal> {
        let convert = |identity: &signal_flow::ModuleIdentity| {
            identity
                .0
                .as_str()
                .curriculum_identity()
                .ok_or_else(|| ContextRefusal::ModuleMissing(Some(identity.clone())))
        };
        Ok(StartupSelection {
            caller_metaflow: CallerMetaflow {
                first_string: match self.address.aspect {
                    Aspect::Psyche => "Psyche",
                    Aspect::Mind => "Mind",
                    Aspect::Field => "Field",
                }
                .into(),
                second_string: self.address.topic.clone().0,
                third_string: self.address.model.name().into(),
            },
            target: match self.harness_kind {
                HarnessKind::Claude => Target::Claude,
                HarnessKind::Codex => Target::Codex,
            },
            system_modules: signal_curriculum_nexus::SystemModules(
                self.system_modules
                    .0
                    .iter()
                    .map(convert)
                    .collect::<Result<Vec<_>, _>>()?,
            ),
            user_modules: signal_curriculum_nexus::UserModules(
                self.prompt_modules
                    .0
                    .iter()
                    .map(convert)
                    .collect::<Result<Vec<_>, _>>()?,
            ),
            flow_id_vector: self
                .remembered_flows
                .0
                .iter()
                .map(|flow| signal_curriculum_nexus::FlowId(flow.0.clone()))
                .collect(),
        })
    }
}

impl ComposesFlowContext for CurriculumComposer {
    fn compose_flow_context<N: ContextRequesting>(
        &self,
        input: N,
    ) -> Result<StartupComposition, ContextRefusal> {
        match CurriculumQuery::ComposeStartup(input.context_request().startup_selection()?)
            .emit(self)
        {
            CurriculumExchange::Answered(CurriculumOutcome::StartupComposed(composition)) => {
                Ok(composition)
            }
            // Curriculum names a remembered flow whose records it could not
            // read "record missing".
            CurriculumExchange::Answered(CurriculumOutcome::Rejected(reason))
                if reason.starts_with("record missing") =>
            {
                eprintln!("flow-nexus: Curriculum found no records: {reason}");
                Err(ContextRefusal::RecordMissing)
            }
            CurriculumExchange::Answered(CurriculumOutcome::Rejected(reason)) => {
                eprintln!("flow-nexus: Curriculum rejected the context request: {reason}");
                Err(ContextRefusal::ComposerRejected(reason))
            }
            CurriculumExchange::Answered(_) => {
                eprintln!("flow-nexus: Curriculum answered the context request with another reply");
                Err(ContextRefusal::ComposerRejected(
                    "Curriculum answered the context request with another reply".into(),
                ))
            }
            CurriculumExchange::Unexchanged(error) => {
                eprintln!("flow-nexus: context composer unavailable: {error}");
                Err(ContextRefusal::ComposerUnavailable)
            }
        }
    }
}

/// A composed context Flow has verified. Its fields are private and its only
/// constructor is [`VerifiesContext::verified`]: this is the guard that keeps
/// any text but the composer's verified answer out of a prompt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedContext {
    address: Address,
    system_text: String,
    user_segments: Vec<String>,
    records_text: String,
}

/// Cuts a rendered text back into its modules, checking each module's slice
/// against its rendered SHA-256 and the whole against the rendering.
impl CutsRenderedText for str {
    fn rendered_segments<N: ProvenanceListing>(&self, input: N) -> Option<Vec<String>> {
        let rows = input.provenance();
        if rows.is_empty() {
            return self.is_empty().then(Vec::new);
        }
        let opening = format!(
            "{}\n\nModule IDs: {}\n",
            CONTEXT_OPENING,
            rows.iter()
                .map(|row| row.module_identity.identity_text())
                .collect::<Vec<_>>()
                .join(", ")
        );
        let mut rest = self.strip_prefix(&opening)?;
        let mut segments = Vec::new();
        for (index, row) in rows.iter().enumerate() {
            let identity = row.module_identity.identity_text();
            if identity.as_str().curriculum_identity().as_ref() != Some(&row.module_identity) {
                return None;
            }
            let open = format!("\n<!-- module: {identity} -->\n", identity = identity);
            let close = format!("\n<!-- end-module: {identity} -->\n", identity = identity);
            let after_open = rest.strip_prefix(&open)?;
            let body_end = match rows.get(index + 1) {
                Some(next) => {
                    let next_open = format!(
                        "{close}\n<!-- module: {} -->\n",
                        next.module_identity.identity_text()
                    );
                    after_open.find(&next_open)?
                }
                None => after_open.strip_suffix(&close)?.len(),
            };
            let body = &after_open[..body_end];
            if format!("{:x}", Sha256::digest(body.as_bytes())) != row.rendered_sha256.0 {
                return None;
            }
            let segment = format!("{open}{body}{close}");
            rest = &rest[segment.len()..];
            segments.push(segment);
        }
        if !rest.is_empty() {
            return None;
        }
        segments[0].insert_str(0, &opening);
        Some(segments)
    }
}

impl VerifiesContext for VerifiedContext {
    fn verified<N: ContextRequesting, O: StartupComposing>(
        input_0: N,
        input_1: O,
    ) -> Result<Self, ContextRefusal> {
        let request = input_0.context_request();
        let composition = input_1.composition();
        let digest = |text: &str| format!("{:x}", Sha256::digest(text.as_bytes()));
        if digest(&composition.system_text.0) != composition.system_payload_sha256.0
            || digest(&composition.user_text.0) != composition.user_payload_sha256.0
            || digest(&composition.records_text.0) != composition.records_payload_sha256.0
        {
            return Err(ContextRefusal::PayloadMismatch);
        }
        // Records are composed only for a launch that remembers flows.
        if request.remembered_flows.0.is_empty() && !composition.records_text.0.is_empty() {
            return Err(ContextRefusal::PayloadMismatch);
        }
        let rows = &composition.context_provenance_vector;
        // The producer expands bindings/dependencies once per actual identity.
        // A distinct singleton and containing span remain distinct; repeating
        // one actual identity is refused even across placements.
        let mut actual_identities = std::collections::HashSet::new();
        if !rows
            .iter()
            .all(|row| actual_identities.insert(&row.module_identity))
        {
            return Err(ContextRefusal::PayloadMismatch);
        }
        let placed = |placement: StartupPlacement| -> Vec<ContextProvenance> {
            rows.iter()
                .filter(|row| row.startup_placement == placement)
                .cloned()
                .collect()
        };
        let system_rows = placed(StartupPlacement::System);
        let user_rows = placed(StartupPlacement::User);
        let missing = request
            .system_modules
            .0
            .iter()
            .zip(std::iter::repeat(&system_rows))
            .chain(
                request
                    .prompt_modules
                    .0
                    .iter()
                    .zip(std::iter::repeat(&user_rows)),
            )
            .find_map(|(identity, placed)| {
                (!placed
                    .iter()
                    .any(|row| row.module_identity.matches_binding(identity.0.as_str())))
                .then(|| identity.clone())
            });
        if let Some(identity) = missing {
            return Err(ContextRefusal::ModuleMissing(Some(identity)));
        }
        let ordered = |placed: &[ContextProvenance]| {
            placed
                .iter()
                .enumerate()
                .all(|(ordinal, row)| row.integer == ordinal as i64)
        };
        if !ordered(&system_rows) || !ordered(&user_rows) {
            return Err(ContextRefusal::PayloadMismatch);
        }
        if composition
            .system_text
            .0
            .rendered_segments(&system_rows)
            .is_none()
        {
            return Err(ContextRefusal::PayloadMismatch);
        }
        let user_segments = composition
            .user_text
            .0
            .rendered_segments(&user_rows)
            .ok_or(ContextRefusal::PayloadMismatch)?;
        Ok(Self {
            address: request.address.clone(),
            system_text: composition.system_text.0,
            user_segments,
            records_text: composition.records_text.0,
        })
    }
}

/// What a verified context holds: the Address it was composed for, the
/// system text, and the USER text cut where each module begins, the first
/// segment holding the opening lines with the first module. The segments'
/// concatenation is the USER text exactly; there are none when no module is
/// USER placed.
impl ReadsVerifiedContext for VerifiedContext {
    fn address(&self) -> Address {
        self.address.clone()
    }

    fn system_text(&self) -> String {
        self.system_text.clone()
    }

    fn user_segments(&self) -> Vec<String> {
        self.user_segments.clone()
    }

    /// The raw vision and notion records of the flows the launch
    /// remembers, as the composer read them; empty when it remembers none.
    fn records_text(&self) -> String {
        self.records_text.clone()
    }
}

/// A composer for tests: each identity's body is held in memory and
/// rendered as Curriculum renders it. An identity it does not hold is
/// rejected, as Curriculum rejects one outside its repositories.
#[cfg(test)]
#[derive(Debug, Clone, Default)]
pub struct FixtureComposer {
    pub bodies: std::collections::BTreeMap<String, String>,
    /// The raw records of each flow it holds, as the composer reads them
    /// from the records root; a remembered flow it does not hold logged
    /// none and composes nothing.
    pub records: std::collections::BTreeMap<String, String>,
}

#[cfg(test)]
impl From<&[(&str, &str)]> for FixtureComposer {
    fn from(modules: &[(&str, &str)]) -> Self {
        Self {
            bodies: modules
                .iter()
                .map(|(identity, body)| ((*identity).to_owned(), (*body).to_owned()))
                .collect(),
            records: std::collections::BTreeMap::new(),
        }
    }
}

#[cfg(test)]
impl ResolvesModule for FixtureComposer {
    fn resolve_module(&self, identity: &signal_flow::ModuleIdentity) -> ModuleStanding {
        match self.bodies.contains_key(&identity.0) {
            true => ModuleStanding::Held,
            false => ModuleStanding::Unknown,
        }
    }
}

#[cfg(test)]
impl ComposesFlowContext for FixtureComposer {
    fn compose_flow_context<N: ContextRequesting>(
        &self,
        input: N,
    ) -> Result<StartupComposition, ContextRefusal> {
        let request = input.context_request();
        let digest = |text: &str| format!("{:x}", Sha256::digest(text.as_bytes()));
        let mut rows = Vec::new();
        for (identities, placement) in [
            (&request.system_modules.0, StartupPlacement::System),
            (&request.prompt_modules.0, StartupPlacement::User),
        ] {
            for (ordinal, identity) in identities.iter().enumerate() {
                let body = self
                    .bodies
                    .get(&identity.0)
                    .ok_or_else(|| ContextRefusal::ModuleMissing(Some(identity.clone())))?;
                rows.push(ContextProvenance {
                    module_identity: identity
                        .0
                        .as_str()
                        .curriculum_identity()
                        .ok_or_else(|| ContextRefusal::ModuleMissing(Some(identity.clone())))?,
                    repository_name: signal_curriculum_nexus::RepositoryName(
                        "fixture-context".into(),
                    ),
                    source_revision: signal_curriculum_nexus::SourceRevision("0".repeat(40)),
                    source_relative_path: signal_curriculum_nexus::SourceRelativePath(format!(
                        "{}.md",
                        identity.0.replace(':', "/")
                    )),
                    source_sha256: signal_curriculum_nexus::SourceSha256(digest(body)),
                    body_sha256: signal_curriculum_nexus::BodySha256(digest(body)),
                    rendered_sha256: signal_curriculum_nexus::RenderedSha256(digest(body)),
                    startup_placement: placement.clone(),
                    integer: ordinal as i64,
                });
            }
        }
        // The text Curriculum renders for these modules in this order.
        let rendered = |identities: &[String]| {
            if identities.is_empty() {
                return String::new();
            }
            let mut text = format!(
                "{}\n\nModule IDs: {}\n",
                CONTEXT_OPENING,
                identities.join(", ")
            );
            for identity in identities {
                text.push_str(&format!(
                    "\n<!-- module: {identity} -->\n{}\n<!-- end-module: {identity} -->\n",
                    self.bodies[identity]
                ));
            }
            text
        };
        let texts = |identities: &[signal_flow::ModuleIdentity]| {
            identities
                .iter()
                .map(|identity| identity.0.clone())
                .collect::<Vec<_>>()
        };
        let system_text = rendered(&texts(&request.system_modules.0));
        let user_text = rendered(&texts(&request.prompt_modules.0));
        // A remembered flow that logged no record composes nothing.
        let records_text = request
            .remembered_flows
            .0
            .iter()
            .filter_map(|flow_id| self.records.get(&flow_id.0))
            .cloned()
            .collect::<String>();
        Ok(StartupComposition {
            system_payload_sha256: signal_curriculum_nexus::SystemPayloadSha256(digest(
                &system_text,
            )),
            user_payload_sha256: signal_curriculum_nexus::UserPayloadSha256(digest(&user_text)),
            records_payload_sha256: signal_curriculum_nexus::RecordsPayloadSha256(digest(
                &records_text,
            )),
            system_text: signal_curriculum_nexus::SystemText(system_text),
            user_text: signal_curriculum_nexus::UserText(user_text),
            records_text: signal_curriculum_nexus::RecordsText(records_text),
            context_provenance_vector: rows,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{
        CONTEXT_OPENING, ComposesFlowContext, ContextRequest, FixtureComposer,
        ParsesCurriculumIdentity, ReadsVerifiedContext, RendersCurriculumIdentity, SelectsStartup,
        VerifiedContext, VerifiesContext,
    };
    use signal_flow::{Address, Aspect, ContextRefusal, HarnessKind, Model};

    /// Records Flow did not ask for, or whose payload digest differs, are
    /// refused.
    #[test]
    fn fixture_provenance_names_the_versioned_source_path() {
        let request = request();
        let composition = fixture().compose_flow_context(&request).unwrap();
        for row in composition.context_provenance_vector {
            assert_eq!(
                row.source_relative_path.0,
                format!(
                    "{}.md",
                    row.module_identity.identity_text().replace(':', "/")
                ),
            );
        }
    }

    #[test]
    fn records_not_asked_for_or_not_matching_their_digest_are_refused() {
        use sha2::{Digest, Sha256};
        let composer = fixture();
        let mut composition = composer.compose_flow_context(request()).unwrap();
        composition.records_text =
            signal_curriculum_nexus::RecordsText("## an unasked record\n".into());
        composition.records_payload_sha256 =
            signal_curriculum_nexus::RecordsPayloadSha256(format!(
                "{:x}",
                Sha256::digest(composition.records_text.0.as_bytes())
            ));
        assert_eq!(
            VerifiedContext::verified(request(), composition.clone()),
            Err(ContextRefusal::PayloadMismatch)
        );
        let mut remembering = request();
        remembering.remembered_flows =
            crate::generated::starter::RememberedFlows(vec![flow_ethos::FlowId("36b71b".into())]);
        assert!(VerifiedContext::verified(&remembering, composition.clone()).is_ok());
        composition.records_payload_sha256 =
            signal_curriculum_nexus::RecordsPayloadSha256("0".repeat(64));
        assert_eq!(
            VerifiedContext::verified(&remembering, composition),
            Err(ContextRefusal::PayloadMismatch)
        );
    }

    fn request() -> ContextRequest {
        ContextRequest {
            address: Address {
                aspect: Aspect::Field,
                topic: flow_ethos::Topic("flow".into()),
                model: Model::Sol,
            },
            harness_kind: HarnessKind::Claude,
            system_modules: signal_flow::SystemModules(vec![
                signal_flow::ModuleIdentity("spirit:core:0".into()),
                signal_flow::ModuleIdentity("intent:flow:0".into()),
            ]),
            prompt_modules: signal_flow::PromptModules(vec![
                signal_flow::ModuleIdentity("knowledge:main-flow:0".into()),
                signal_flow::ModuleIdentity("knowledge:flow:0".into()),
            ]),
            remembered_flows: crate::generated::starter::RememberedFlows(vec![]),
        }
    }

    fn fixture() -> FixtureComposer {
        FixtureComposer::from(
            &[
                ("spirit:core:0", "Spirit body."),
                ("intent:flow:0", "Intent body."),
                (
                    "knowledge:main-flow:0",
                    "Main flow body.\n\nSecond paragraph.",
                ),
                ("knowledge:flow:0", "Knowledge body."),
            ][..],
        )
    }

    #[test]
    fn a_verified_context_cuts_the_user_text_at_each_module() {
        let request = request();
        let composition = fixture().compose_flow_context(&request).unwrap();
        let user_text = composition.user_text.clone();
        let verified = VerifiedContext::verified(&request, composition).unwrap();
        assert_eq!(verified.user_segments().len(), 2);
        assert_eq!(verified.user_segments().concat(), user_text.0);
        assert!(verified.user_segments()[0].starts_with(CONTEXT_OPENING));
        assert!(verified.user_segments()[1].starts_with("\n<!-- module: knowledge:flow:0 -->\n"));
        assert!(verified.system_text().contains("Spirit body."));
        assert_eq!(verified.address(), request.address);
    }

    #[test]
    fn a_payload_that_differs_from_its_hash_is_refused() {
        let request = request();
        let mut composition = fixture().compose_flow_context(&request).unwrap();
        composition.user_text.0.push('x');
        assert_eq!(
            VerifiedContext::verified(&request, composition),
            Err(ContextRefusal::PayloadMismatch)
        );
    }

    #[test]
    fn a_module_body_that_differs_from_its_rendered_hash_is_refused() {
        let request = request();
        let mut composition = fixture().compose_flow_context(&request).unwrap();
        composition.context_provenance_vector[2].rendered_sha256 =
            signal_curriculum_nexus::RenderedSha256("0".repeat(64));
        assert_eq!(
            VerifiedContext::verified(&request, composition),
            Err(ContextRefusal::PayloadMismatch)
        );
    }

    #[test]
    fn a_requested_module_absent_at_its_placement_is_refused() {
        let request = request();
        let mut composition = fixture().compose_flow_context(&request).unwrap();
        composition
            .context_provenance_vector
            .retain(|row| row.module_identity.identity_text() != "knowledge:flow:0");
        assert_eq!(
            VerifiedContext::verified(&request, composition),
            Err(ContextRefusal::ModuleMissing(Some(
                signal_flow::ModuleIdentity("knowledge:flow:0".into())
            )))
        );
    }

    #[test]
    fn an_identity_the_composer_does_not_hold_is_rejected() {
        let mut request = request();
        request
            .prompt_modules
            .0
            .push(signal_flow::ModuleIdentity("unknown-module".into()));
        assert_eq!(
            fixture().compose_flow_context(&request),
            Err(ContextRefusal::ModuleMissing(Some(
                signal_flow::ModuleIdentity("unknown-module".into())
            )))
        );
    }
    #[test]
    fn typed_identity_canonical_roundtrip_covers_every_kind_and_persona_name() {
        for kind in [
            "spirit",
            "intent",
            "vision",
            "knowledge",
            "notion",
            "inquiry",
            "compensation",
            "role",
            "raw",
        ] {
            for subject in ["core", "psyche-core-tertiary", "book-page2-description"] {
                for version in ["0", "1", "0-1", "1-9", "9223372036854775807"] {
                    let text = format!("{kind}:{subject}:{version}");
                    let parsed = text
                        .as_str()
                        .curriculum_identity()
                        .expect("canonical identity");
                    assert_eq!(parsed.identity_text(), text);
                    assert!(parsed.matches_binding(text.as_str()));
                }
            }
        }
    }

    #[test]
    fn notion_context_preserves_its_identity_source_path_and_user_placement() {
        let mut request = request();
        request.system_modules.0.clear();
        request.prompt_modules.0 = ["notion:harness-nexus:0", "notion:source-nexus:0"]
            .into_iter()
            .map(|identity| signal_flow::ModuleIdentity(identity.into()))
            .collect();
        let composer = FixtureComposer::from(
            &[
                ("notion:harness-nexus:0", "Harness notion."),
                ("notion:source-nexus:0", "Source notion."),
            ][..],
        );
        let composed = composer.compose_flow_context(&request).unwrap();
        let verified = VerifiedContext::verified(&request, &composed).unwrap();
        assert!(verified.system_text().is_empty());
        assert_eq!(verified.user_segments().len(), 2);
        for (row, identity) in composed
            .context_provenance_vector
            .iter()
            .zip(&request.prompt_modules.0)
        {
            assert_eq!(row.module_identity.identity_text(), identity.0);
            assert_eq!(
                row.source_relative_path.0,
                format!("{}.md", identity.0.replace(':', "/"))
            );
            assert_eq!(
                row.startup_placement,
                signal_curriculum_nexus::StartupPlacement::User
            );
        }
    }

    #[test]
    fn malformed_text_is_refused_before_the_curriculum_exchange() {
        for identity in [
            "spirit:core",
            "operation:core:0",
            "Spirit:core:0",
            "role::0",
            "role:Core:0",
            "role:some_name:0",
            "role:some.name:0",
            "role:é:0",
            "role:core:-1",
            "role:core:01",
            "role:core:+1",
            "role:core:1-1",
            "role:core:2-1",
            "role:core:0-01",
            "role:core:0-1-2",
            "role:core:9223372036854775808",
            "role:core:1:2",
            "role:core: 1",
        ] {
            assert!(identity.curriculum_identity().is_none(), "{identity}");
            let mut request = request();
            request.system_modules.0 = vec![signal_flow::ModuleIdentity(identity.into())];
            assert_eq!(
                request.startup_selection(),
                Err(ContextRefusal::ModuleMissing(Some(
                    signal_flow::ModuleIdentity(identity.into())
                )))
            );
            let composer = super::CurriculumComposer {
                socket: "/does/not/exist".into(),
            };
            assert_eq!(
                composer.compose_flow_context(&request),
                Err(ContextRefusal::ModuleMissing(Some(
                    signal_flow::ModuleIdentity(identity.into())
                )))
            );
            assert_eq!(
                super::ResolvesModule::resolve_module(
                    &composer,
                    &signal_flow::ModuleIdentity(identity.into())
                ),
                super::ModuleStanding::Unknown
            );
        }
    }

    #[test]
    fn numeric_binding_preserves_singleton_and_containing_span_actual_provenance() {
        let mut actual = request();
        actual.system_modules.0.clear();
        actual.prompt_modules.0 = ["role:persona:1", "role:persona:0-1"]
            .into_iter()
            .map(|s| signal_flow::ModuleIdentity(s.into()))
            .collect();
        let composer = FixtureComposer::from(
            &[
                ("role:persona:1", "Singleton persona."),
                ("role:persona:0-1", "Spanning persona."),
            ][..],
        );
        let composition = composer.compose_flow_context(&actual).unwrap();
        let mut binding = actual.clone();
        binding.prompt_modules.0.truncate(1);
        let verified = VerifiedContext::verified(&binding, &composition).unwrap();
        assert_eq!(verified.user_segments().len(), 2);
        assert_eq!(verified.user_segments().concat(), composition.user_text.0);
        assert_eq!(
            composition
                .context_provenance_vector
                .iter()
                .map(|r| r.module_identity.identity_text())
                .collect::<Vec<_>>(),
            ["role:persona:1", "role:persona:0-1"]
        );
        assert_eq!(
            composition.context_provenance_vector[1]
                .source_relative_path
                .0,
            "role/persona/0-1.md"
        );
        let singleton_zero = "role:persona:0".curriculum_identity().unwrap();
        assert!(!singleton_zero.matches_binding("role:persona:1"));
        let mut wrong = actual.clone();
        wrong.prompt_modules.0 = vec![signal_flow::ModuleIdentity("role:persona:0".into())];
        let zero = FixtureComposer::from(&[("role:persona:0", "Old persona.")][..])
            .compose_flow_context(&wrong)
            .unwrap();
        assert!(matches!(
            VerifiedContext::verified(&binding, zero),
            Err(ContextRefusal::ModuleMissing(_))
        ));
        let mut misplaced = composition.clone();
        for row in &mut misplaced.context_provenance_vector {
            row.startup_placement = signal_curriculum_nexus::StartupPlacement::System;
        }
        assert!(VerifiedContext::verified(&binding, misplaced).is_err());
        let mut reordered = composition.clone();
        reordered.context_provenance_vector[1].integer = 0;
        assert_eq!(
            VerifiedContext::verified(&binding, reordered),
            Err(ContextRefusal::PayloadMismatch)
        );
    }

    #[test]
    fn binding_checks_exact_spans_and_inclusive_numeric_bounds() {
        let actual = "knowledge:topic:1-3".curriculum_identity().unwrap();
        for binding in [
            "knowledge:topic:1",
            "knowledge:topic:2",
            "knowledge:topic:3",
            "knowledge:topic:1-3",
        ] {
            assert!(actual.matches_binding(binding));
        }
        for binding in [
            "knowledge:topic:0",
            "knowledge:topic:4",
            "knowledge:topic:1-2",
            "role:topic:2",
            "knowledge:other:2",
        ] {
            assert!(!actual.matches_binding(binding));
        }
        let mut invalid = actual;
        invalid.versions =
            signal_curriculum_nexus::Versions::Span(signal_curriculum_nexus::Span_Data {
                first: signal_curriculum_nexus::First(signal_curriculum_nexus::Version(3)),
                last: signal_curriculum_nexus::Last(signal_curriculum_nexus::Version(1)),
            });
        assert!(!invalid.matches_binding("knowledge:topic:2"));
    }
    #[test]
    fn repeated_actual_identity_is_refused_with_consistent_rendering_hashes_and_ordinals() {
        use super::CutsRenderedText;
        use sha2::{Digest, Sha256};
        use signal_curriculum_nexus::StartupPlacement;
        for duplicate_at in [StartupPlacement::System, StartupPlacement::User] {
            let mut request = request();
            request.system_modules.0 =
                vec![signal_flow::ModuleIdentity("knowledge:duplicate:1".into())];
            request.prompt_modules.0.clear();
            let mut reply_selection = request.clone();
            match duplicate_at {
                StartupPlacement::System => reply_selection
                    .system_modules
                    .0
                    .push(request.system_modules.0[0].clone()),
                StartupPlacement::User => reply_selection
                    .prompt_modules
                    .0
                    .push(request.system_modules.0[0].clone()),
            }
            // Compose the adversarial reply afresh, not by corrupting an old
            // hash: both rendered bodies, placement-local ordinals and all
            // three payload digests are internally valid.
            let composer =
                FixtureComposer::from(&[("knowledge:duplicate:1", "Repeated body.")][..]);
            let reply = composer.compose_flow_context(&reply_selection).unwrap();
            assert_eq!(reply.context_provenance_vector.len(), 2);
            for (text, hash) in [
                (&reply.system_text.0, &reply.system_payload_sha256.0),
                (&reply.user_text.0, &reply.user_payload_sha256.0),
                (&reply.records_text.0, &reply.records_payload_sha256.0),
            ] {
                assert_eq!(&format!("{:x}", Sha256::digest(text.as_bytes())), hash);
            }
            for placement in [StartupPlacement::System, StartupPlacement::User] {
                let placed = reply
                    .context_provenance_vector
                    .iter()
                    .filter(|r| r.startup_placement == placement)
                    .cloned()
                    .collect::<Vec<_>>();
                assert!(
                    placed
                        .iter()
                        .enumerate()
                        .all(|(i, r)| r.integer == i as i64)
                );
                let text = match placement {
                    StartupPlacement::System => &reply.system_text.0,
                    StartupPlacement::User => &reply.user_text.0,
                };
                assert!(text.as_str().rendered_segments(&placed).is_some());
            }
            assert_eq!(
                VerifiedContext::verified(&request, &reply),
                Err(ContextRefusal::PayloadMismatch),
                "duplicate at {duplicate_at:?}"
            );
        }
    }
}
