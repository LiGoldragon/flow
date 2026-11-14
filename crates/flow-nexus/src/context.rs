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
//! with `ComposeStartup` from its schema, copied into
//! `ethos/curriculum.ethos` from Curriculum 478cbc5 (schema SHA-256
//! 26011a94…48e28) but for `RolesDocument.[ Roles ]`, the bare variant
//! ethos-zero 22 requires, which keeps every form; its selection names the flows a launch remembers
//! and whose composition carries their raw records. Its Outcome has five
//! variants, the fifth `Rejected` with Curriculum's own text, which Flow
//! keeps whole in `ContextRefusal::ComposerRejected`, but for a remembered
//! flow's records missing, `RecordMissing`. Its answer names no
//! metaflow, so the metaflow check is Flow's own: the request carries the
//! launch's Address, and the composed launch keeps it with the identities
//! asked for it.

use crate::generated::curriculum::{
    CallerMetaflow, ContextProvenance, Outcome as CurriculumOutcome, Query as CurriculumQuery,
    StartupComposition, StartupPlacement, StartupSelection, Target,
};
pub use crate::generated::starter::{
    ComposesFlowContext, ContextRequest, ContextRequesting, CurriculumQuerying, CutsRenderedText,
    ExchangesCurriculumFrames, OpensCurriculumComposer, ProvenanceListing, ReadsVerifiedContext,
    SelectsStartup, StartupComposing, Textual, VerifiesContext,
};
use crate::model::RunsModel;
use sha2::{Digest, Sha256};
use signal_flow::{Address, Aspect, ContextRefusal, HarnessKind};
use std::{
    io::{Read, Write},
    os::unix::net::UnixStream,
    path::PathBuf,
    time::Duration,
};

/// The Curriculum Nexus, asked over its Unix socket: one big-endian u32
/// length-prefixed rkyv `Query` frame out, one `Outcome` frame back.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CurriculumComposer {
    socket: PathBuf,
}

/// How long one composition may take on the Curriculum socket.
const EXCHANGE_TIMEOUT: Duration = Duration::from_secs(30);
/// The largest Curriculum answer frame read: generous for two texts.
const ANSWER_FRAME_LIMIT: usize = 64 * 1024 * 1024;
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

/// The Curriculum Nexus resolves the one module by `ResolveSources`, as
/// composition does: an answer naming it is Held; Curriculum's own
/// "missing module" refusal is Unknown; a socket that cannot be reached, an
/// answer that cannot be read, or any other refusal is Unreachable.
impl ResolvesModule for CurriculumComposer {
    fn resolve_module(&self, identity: &signal_flow::ModuleIdentity) -> ModuleStanding {
        match self.exchange(CurriculumQuery::ResolveSources(vec![
            crate::generated::curriculum::ModuleIdentity(identity.0.clone()),
        ])) {
            Ok(CurriculumOutcome::ResolvedSources(modules))
                if modules
                    .iter()
                    .any(|module| module.module_identity.0 == identity.0) =>
            {
                ModuleStanding::Held
            }
            Ok(CurriculumOutcome::ResolvedSources(_)) => ModuleStanding::Unknown,
            Ok(CurriculumOutcome::Rejected(reason)) if reason.starts_with("missing module") => {
                ModuleStanding::Unknown
            }
            Ok(CurriculumOutcome::Rejected(reason)) => {
                eprintln!(
                    "flow-nexus: Curriculum could not resolve {}: {reason}",
                    identity.0
                );
                ModuleStanding::Unreachable
            }
            Ok(_) => ModuleStanding::Unreachable,
            Err(error) => {
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
    fn startup_selection(&self) -> StartupSelection {
        StartupSelection {
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
            system_modules: crate::generated::curriculum::SystemModules(
                self.system_modules
                    .0
                    .iter()
                    .map(|identity| {
                        crate::generated::curriculum::ModuleIdentity(identity.0.clone())
                    })
                    .collect(),
            ),
            user_modules: crate::generated::curriculum::UserModules(
                self.prompt_modules
                    .0
                    .iter()
                    .map(|identity| {
                        crate::generated::curriculum::ModuleIdentity(identity.0.clone())
                    })
                    .collect(),
            ),
            flow_id_vector: self
                .remembered_flows
                .0
                .iter()
                .map(|flow| crate::generated::curriculum::FlowId(flow.0.clone()))
                .collect(),
        }
    }
}

/// One big-endian u32 length-prefixed rkyv `Query` frame out, one
/// `Outcome` frame back.
impl ExchangesCurriculumFrames for CurriculumComposer {
    fn exchange<N: CurriculumQuerying>(&self, input: N) -> Result<CurriculumOutcome, String> {
        let request = rkyv::to_bytes::<rkyv::rancor::Error>(&input.query())
            .map_err(|error| format!("query not archived: {error}"))?;
        let length = u32::try_from(request.len()).map_err(|_| "query frame too large")?;
        let mut stream = UnixStream::connect(&self.socket)
            .map_err(|error| format!("Curriculum socket unreachable: {error}"))?;
        stream
            .set_read_timeout(Some(EXCHANGE_TIMEOUT))
            .and_then(|_| stream.set_write_timeout(Some(EXCHANGE_TIMEOUT)))
            .map_err(|error| error.to_string())?;
        stream
            .write_all(&length.to_be_bytes())
            .and_then(|_| stream.write_all(&request))
            .and_then(|_| stream.flush())
            .map_err(|error| format!("query not written: {error}"))?;
        let mut prefix = [0_u8; 4];
        stream
            .read_exact(&mut prefix)
            .map_err(|error| format!("answer not read: {error}"))?;
        let length = u32::from_be_bytes(prefix) as usize;
        if length > ANSWER_FRAME_LIMIT {
            return Err("answer frame exceeds its limit".into());
        }
        let mut frame = vec![0_u8; length];
        stream
            .read_exact(&mut frame)
            .map_err(|error| format!("answer not read: {error}"))?;
        rkyv::from_bytes::<CurriculumOutcome, rkyv::rancor::Error>(&frame)
            .map_err(|error| format!("answer unreadable: {error}"))
    }
}

impl ComposesFlowContext for CurriculumComposer {
    fn compose_flow_context<N: ContextRequesting>(
        &self,
        input: N,
    ) -> Result<StartupComposition, ContextRefusal> {
        match self.exchange(CurriculumQuery::ComposeStartup(
            input.context_request().startup_selection(),
        )) {
            Ok(CurriculumOutcome::StartupComposed(composition)) => Ok(composition),
            // Curriculum names a remembered flow whose records it could not
            // read "record missing".
            Ok(CurriculumOutcome::Rejected(reason)) if reason.starts_with("record missing") => {
                eprintln!("flow-nexus: Curriculum found no records: {reason}");
                Err(ContextRefusal::RecordMissing)
            }
            Ok(CurriculumOutcome::Rejected(reason)) => {
                eprintln!("flow-nexus: Curriculum rejected the context request: {reason}");
                Err(ContextRefusal::ComposerRejected(reason))
            }
            Ok(_) => {
                eprintln!("flow-nexus: Curriculum answered the context request with another reply");
                Err(ContextRefusal::ComposerRejected(
                    "Curriculum answered the context request with another reply".into(),
                ))
            }
            Err(error) => {
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
                .map(|row| row.module_identity.0.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        );
        let mut rest = self.strip_prefix(&opening)?;
        let mut segments = Vec::new();
        for (index, row) in rows.iter().enumerate() {
            let identity = &row.module_identity;
            let open = format!("\n<!-- module: {identity} -->\n", identity = identity.0);
            let close = format!("\n<!-- end-module: {identity} -->\n", identity = identity.0);
            let after_open = rest.strip_prefix(&open)?;
            let body_end = match rows.get(index + 1) {
                Some(next) => {
                    let next_open =
                        format!("{close}\n<!-- module: {} -->\n", next.module_identity.0);
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
                (!placed.iter().any(|row| row.module_identity.0 == identity.0))
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
                    module_identity: crate::generated::curriculum::ModuleIdentity(
                        identity.0.clone(),
                    ),
                    repository_name: crate::generated::curriculum::RepositoryName(
                        "fixture-context".into(),
                    ),
                    source_revision: crate::generated::curriculum::SourceRevision("0".repeat(40)),
                    source_relative_path: crate::generated::curriculum::SourceRelativePath(
                        format!("{}.md", identity.0.replace(':', "/")),
                    ),
                    source_sha256: crate::generated::curriculum::SourceSha256(digest(body)),
                    body_sha256: crate::generated::curriculum::BodySha256(digest(body)),
                    rendered_sha256: crate::generated::curriculum::RenderedSha256(digest(body)),
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
            system_payload_sha256: crate::generated::curriculum::SystemPayloadSha256(digest(
                &system_text,
            )),
            user_payload_sha256: crate::generated::curriculum::UserPayloadSha256(digest(
                &user_text,
            )),
            records_payload_sha256: crate::generated::curriculum::RecordsPayloadSha256(digest(
                &records_text,
            )),
            system_text: crate::generated::curriculum::SystemText(system_text),
            user_text: crate::generated::curriculum::UserText(user_text),
            records_text: crate::generated::curriculum::RecordsText(records_text),
            context_provenance_vector: rows,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{
        CONTEXT_OPENING, ComposesFlowContext, ContextRequest, FixtureComposer,
        ReadsVerifiedContext, VerifiedContext, VerifiesContext,
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
                format!("{}.md", row.module_identity.0.replace(':', "/")),
            );
        }
    }

    #[test]
    fn records_not_asked_for_or_not_matching_their_digest_are_refused() {
        use sha2::{Digest, Sha256};
        let composer = fixture();
        let mut composition = composer.compose_flow_context(request()).unwrap();
        composition.records_text =
            crate::generated::curriculum::RecordsText("## an unasked record\n".into());
        composition.records_payload_sha256 =
            crate::generated::curriculum::RecordsPayloadSha256(format!(
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
            crate::generated::curriculum::RecordsPayloadSha256("0".repeat(64));
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
                signal_flow::ModuleIdentity(
                    crate::generated::curriculum::ModuleIdentity("spirit:core:0".into()).0,
                ),
                signal_flow::ModuleIdentity(
                    crate::generated::curriculum::ModuleIdentity("intent:flow:0".into()).0,
                ),
            ]),
            prompt_modules: signal_flow::PromptModules(vec![
                signal_flow::ModuleIdentity(
                    crate::generated::curriculum::ModuleIdentity("operation:main-flow:0".into()).0,
                ),
                signal_flow::ModuleIdentity(
                    crate::generated::curriculum::ModuleIdentity("knowledge:flow:0".into()).0,
                ),
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
                    "operation:main-flow:0",
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
            crate::generated::curriculum::RenderedSha256("0".repeat(64));
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
            .retain(|row| row.module_identity.0 != "knowledge:flow:0");
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
}
