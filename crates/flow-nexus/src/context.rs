//! A flow's context: the module identities its launch names, composed by
//! Curriculum and verified before any of its text enters a prompt.
//!
//! A launch profile names its context only by module identity, for the
//! system prompt and for the first USER message. Flow asks the context
//! composer for both texts and the provenance of every module in them, and
//! the only way that text reaches a prompt is through [`VerifiedContext`],
//! whose one constructor is [`VerifiesContext::verified`]: the payload
//! SHA-256s are recomputed, every requested identity must be in the
//! provenance or in unique requested-and-absent omission evidence, and each text must be exactly the rendering
//! of its modules, each module's slice matching its rendered SHA-256. No
//! path, file or inline context text is accepted anywhere on this route.
//!
//! The production composer is the Curriculum Nexus, asked over the socket
//! the Nexus's configuration names (`CurriculumNexusPath`),
//! with `ComposeStartup` from its schema, copied byte for byte into
//! `ethos/curriculum.ethos` from Curriculum 73330192 (schema SHA-256
//! 3f6d7afd…1b46d), whose selection names the flows a launch remembers
//! and whose composition carries their raw records. Its answer names no
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
use sha2::{Digest, Sha256};
use signal_flow::{Address, Aspect, ContextRefusal, HarnessKind, Layer};
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
    timeout: Duration,
    answer_frame_limit: usize,
}
/// The opening line of a rendered context text.
pub const CONTEXT_OPENING: &str = "These are your skills, given here; do not reload them.";

/// A Curriculum composer at the socket a text names: the Nexus's
/// configured `CurriculumNexusPath`.
impl OpensCurriculumComposer for CurriculumComposer {
    fn at<N: Textual>(input: N) -> Self {
        Self {
            socket: PathBuf::from(input.text()),
            timeout: Duration::from_millis(
                <crate::store::timings_limits::StoredTimings as crate::store::timings_limits::ProvidesTimingSeed>::seed_timings()
                    .expect("authored timings seed")
                    .curriculum_timeout as u64,
            ),
            answer_frame_limit: <crate::store::timings_limits::StoredLimits as crate::store::timings_limits::ProvidesLimitSeed>::seed_limits()
                .expect("authored limits seed")
                .curriculum_answer as usize,
        }
    }
}

pub(crate) trait ConfiguresCurriculumComposer {
    fn configured(
        socket: &str,
        timings: &meta_signal_flow::Timings,
        limits: &meta_signal_flow::Limits,
    ) -> Self;
}

impl ConfiguresCurriculumComposer for CurriculumComposer {
    fn configured(
        socket: &str,
        timings: &meta_signal_flow::Timings,
        limits: &meta_signal_flow::Limits,
    ) -> Self {
        Self {
            socket: PathBuf::from(socket),
            timeout: Duration::from_millis(timings.curriculum_timeout as u64),
            answer_frame_limit: limits.curriculum_answer as usize,
        }
    }
}

/// The context composer a Nexus runs with: the Curriculum Nexus, or, under
/// test, the in-memory fixture.
#[derive(Debug, Clone)]
pub enum ContextComposer {
    Curriculum(CurriculumComposer),
    #[cfg(test)]
    Fixture(FixtureComposer),
}

impl ComposesFlowContext for ContextComposer {
    fn compose_flow_context<N: ContextRequesting>(
        &self,
        input: N,
    ) -> Result<StartupComposition, ContextRefusal> {
        match self {
            Self::Curriculum(composer) => composer.compose_flow_context(input),
            #[cfg(test)]
            Self::Fixture(composer) => composer.compose_flow_context(input),
        }
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
                second_string: self.address.topic.clone(),
                third_string: match self.address.layer {
                    Layer::Primary => "Primary",
                    Layer::Secondary => "Secondary",
                    Layer::Tertiary => "Tertiary",
                    Layer::Quaternary => "Quaternary",
                }
                .into(),
            },
            target: match self.harness_kind {
                HarnessKind::Claude => Target::Claude,
                HarnessKind::Codex => Target::Codex,
            },
            system_modules: self.system_modules.clone(),
            user_modules: self.user_modules.clone(),
            flow_id_vector: self.remembered_flows.clone(),
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
            .set_read_timeout(Some(self.timeout))
            .and_then(|_| stream.set_write_timeout(Some(self.timeout)))
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
        if length > self.answer_frame_limit {
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
                Err(ContextRefusal::ComposerRejected)
            }
            Ok(_) => {
                eprintln!("flow-nexus: Curriculum answered the context request with another reply");
                Err(ContextRefusal::ComposerRejected)
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
                .map(|row| row.module_identity.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        );
        let mut rest = self.strip_prefix(&opening)?;
        let mut segments = Vec::new();
        for (index, row) in rows.iter().enumerate() {
            let identity = &row.module_identity;
            let open = format!("\n<!-- module: {identity} -->\n");
            let close = format!("\n<!-- end-module: {identity} -->\n");
            let after_open = rest.strip_prefix(&open)?;
            let body_end = match rows.get(index + 1) {
                Some(next) => {
                    let next_open = format!("{close}\n<!-- module: {} -->\n", next.module_identity);
                    after_open.find(&next_open)?
                }
                None => after_open.strip_suffix(&close)?.len(),
            };
            let body = &after_open[..body_end];
            if format!("{:x}", Sha256::digest(body.as_bytes())) != row.rendered_sha256 {
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
        if digest(&composition.system_text) != composition.system_payload_sha256
            || digest(&composition.user_text) != composition.user_payload_sha256
            || digest(&composition.records_text) != composition.records_payload_sha256
        {
            return Err(ContextRefusal::PayloadMismatch);
        }
        // Records are composed only for a launch that remembers flows.
        if request.remembered_flows.is_empty() && !composition.records_text.is_empty() {
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
        // Curriculum applies a module header's declared placement and
        // reports the effective placement in each provenance row. The
        // request's two lists carry only the kind-based default, so verify
        // the requested identities independently of where Curriculum put
        // them.
        let requested: Vec<&String> = request
            .system_modules
            .iter()
            .chain(request.user_modules.iter())
            .collect();
        let omitted = &composition.omitted_modules;
        let unique_omissions = omitted.iter().collect::<std::collections::BTreeSet<_>>();
        if unique_omissions.len() != omitted.len()
            || omitted
                .iter()
                .any(|identity| !requested.contains(&identity))
            || omitted.iter().any(|identity| {
                rows.iter().any(|row| row.module_identity == *identity)
            })
        {
            return Err(ContextRefusal::PayloadMismatch);
        }
        if requested.iter().any(|identity| {
            !omitted.contains(*identity)
                && rows
                    .iter()
                    .filter(|row| &row.module_identity == *identity)
                    .count()
                    != 1
        }) {
            return Err(ContextRefusal::ModuleMissing);
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
            .rendered_segments(&system_rows)
            .is_none()
        {
            return Err(ContextRefusal::PayloadMismatch);
        }
        let user_segments = composition
            .user_text
            .rendered_segments(&user_rows)
            .ok_or(ContextRefusal::PayloadMismatch)?;
        Ok(Self {
            address: request.address.clone(),
            system_text: composition.system_text,
            user_segments,
            records_text: composition.records_text,
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
/// reported as omitted while every available identity is composed.
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
impl ComposesFlowContext for FixtureComposer {
    fn compose_flow_context<N: ContextRequesting>(
        &self,
        input: N,
    ) -> Result<StartupComposition, ContextRefusal> {
        let request = input.context_request();
        let digest = |text: &str| format!("{:x}", Sha256::digest(text.as_bytes()));
        let mut rows = Vec::new();
        let mut omitted_modules = Vec::new();
        let mut available_system = Vec::new();
        let mut available_user = Vec::new();
        for (identities, placement) in [
            (&request.system_modules, StartupPlacement::System),
            (&request.user_modules, StartupPlacement::User),
        ] {
            let available = if placement == StartupPlacement::System {
                &mut available_system
            } else {
                &mut available_user
            };
            for identity in identities {
                let Some(body) = self.bodies.get(identity) else {
                    omitted_modules.push(identity.clone());
                    continue;
                };
                let ordinal = available.len();
                available.push(identity.clone());
                rows.push(ContextProvenance {
                    module_identity: identity.clone(),
                    repository_name: "fixture-context".into(),
                    source_revision: "0".repeat(40),
                    source_relative_path: format!("{identity}.md"),
                    source_sha256: digest(body),
                    body_sha256: digest(body),
                    rendered_sha256: digest(body),
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
        let system_text = rendered(&available_system);
        let user_text = rendered(&available_user);
        // A remembered flow that logged no record composes nothing.
        let records_text = request
            .remembered_flows
            .iter()
            .filter_map(|flow_id| self.records.get(flow_id))
            .cloned()
            .collect::<String>();
        Ok(StartupComposition {
            system_payload_sha256: digest(&system_text),
            user_payload_sha256: digest(&user_text),
            records_payload_sha256: digest(&records_text),
            system_text,
            user_text,
            records_text,
            omitted_modules,
            context_provenance_vector: rows,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{
        CONTEXT_OPENING, ComposesFlowContext, ContextRequest, FixtureComposer,
        ReadsVerifiedContext, SelectsStartup, VerifiedContext, VerifiesContext,
    };
    use crate::generated::curriculum::Target;
    use signal_flow::{Address, Aspect, ContextRefusal, HarnessKind, Layer};

    /// The flows a launch remembers reach Curriculum as the selection's
    /// flow ids, in order.
    #[test]
    fn the_startup_selection_names_the_remembered_flows() {
        let mut remembering = request();
        remembering.remembered_flows = vec!["36b71b".into(), "e3c8c1".into()];
        assert_eq!(
            remembering.startup_selection().flow_id_vector,
            vec!["36b71b".to_owned(), "e3c8c1".to_owned()]
        );
        assert!(request().startup_selection().flow_id_vector.is_empty());
    }

    /// Records Flow did not ask for, or whose payload digest differs, are
    /// refused.
    #[test]
    fn records_not_asked_for_or_not_matching_their_digest_are_refused() {
        use sha2::{Digest, Sha256};
        let composer = fixture();
        let mut composition = composer.compose_flow_context(request()).unwrap();
        composition.records_text = "## an unasked record\n".into();
        composition.records_payload_sha256 =
            format!("{:x}", Sha256::digest(composition.records_text.as_bytes()));
        assert_eq!(
            VerifiedContext::verified(request(), composition.clone()),
            Err(ContextRefusal::PayloadMismatch)
        );
        let mut remembering = request();
        remembering.remembered_flows = vec!["36b71b".into()];
        assert!(VerifiedContext::verified(&remembering, composition.clone()).is_ok());
        composition.records_payload_sha256 = "0".repeat(64);
        assert_eq!(
            VerifiedContext::verified(&remembering, composition),
            Err(ContextRefusal::PayloadMismatch)
        );
    }

    fn request() -> ContextRequest {
        ContextRequest {
            address: Address {
                aspect: Aspect::Field,
                topic: "flow".into(),
                layer: Layer::Secondary,
            },
            harness_kind: HarnessKind::Claude,
            system_modules: vec!["spirit-core".into(), "intent-flow".into()],
            user_modules: vec!["operation-main-flow".into(), "knowledge-flow".into()],
            remembered_flows: vec![],
        }
    }

    fn fixture() -> FixtureComposer {
        FixtureComposer::from(
            &[
                ("spirit-core", "Spirit body."),
                ("intent-flow", "Intent body."),
                (
                    "operation-main-flow",
                    "Main flow body.\n\nSecond paragraph.",
                ),
                ("knowledge-flow", "Knowledge body."),
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
        assert_eq!(verified.user_segments().concat(), user_text);
        assert!(verified.user_segments()[0].starts_with(CONTEXT_OPENING));
        assert!(verified.user_segments()[1].starts_with("\n<!-- module: knowledge-flow -->\n"));
        assert!(verified.system_text().contains("Spirit body."));
        assert_eq!(verified.address(), request.address);
    }

    #[test]
    fn curriculums_effective_header_placement_overrides_the_kind_default() {
        let requested = request();
        let mut effective = requested.clone();
        effective
            .user_modules
            .retain(|module| module != "knowledge-flow");
        effective.system_modules.push("knowledge-flow".into());
        let composition = fixture().compose_flow_context(&effective).unwrap();
        let verified = VerifiedContext::verified(&requested, composition).unwrap();
        assert!(verified.system_text().contains("Knowledge body."));
        assert!(
            !verified
                .user_segments()
                .concat()
                .contains("Knowledge body.")
        );
    }

    #[test]
    fn a_payload_that_differs_from_its_hash_is_refused() {
        let request = request();
        let mut composition = fixture().compose_flow_context(&request).unwrap();
        composition.user_text.push('x');
        assert_eq!(
            VerifiedContext::verified(&request, composition),
            Err(ContextRefusal::PayloadMismatch)
        );
    }

    #[test]
    fn a_module_body_that_differs_from_its_rendered_hash_is_refused() {
        let request = request();
        let mut composition = fixture().compose_flow_context(&request).unwrap();
        composition.context_provenance_vector[2].rendered_sha256 = "0".repeat(64);
        assert_eq!(
            VerifiedContext::verified(&request, composition),
            Err(ContextRefusal::PayloadMismatch)
        );
    }

    #[test]
    fn a_requested_module_absent_from_curriculums_answer_is_refused() {
        let request = request();
        let mut composition = fixture().compose_flow_context(&request).unwrap();
        composition
            .context_provenance_vector
            .retain(|row| row.module_identity != "knowledge-flow");
        assert_eq!(
            VerifiedContext::verified(&request, composition),
            Err(ContextRefusal::ModuleMissing)
        );
    }

    #[test]
    fn unavailable_identities_are_named_and_available_modules_are_verified() {
        let mut request = request();
        request.user_modules.insert(1, "unknown-module".into());
        request.user_modules.push("another-unknown-module".into());
        let composition = fixture().compose_flow_context(&request).unwrap();
        assert_eq!(
            composition.omitted_modules,
            vec!["unknown-module", "another-unknown-module"]
        );
        assert!(VerifiedContext::verified(&request, composition).is_ok());
    }

    #[test]
    fn omission_evidence_must_be_requested_unique_and_absent() {
        let request = request();
        let composition = fixture().compose_flow_context(&request).unwrap();
        assert!(composition.omitted_modules.is_empty());

        let mut unrequested = composition.clone();
        unrequested.omitted_modules.push("unknown-module".into());
        assert_eq!(
            VerifiedContext::verified(&request, unrequested),
            Err(ContextRefusal::PayloadMismatch)
        );

        let mut present = composition.clone();
        present.omitted_modules.push("knowledge-flow".into());
        assert_eq!(
            VerifiedContext::verified(&request, present),
            Err(ContextRefusal::PayloadMismatch)
        );

        let mut duplicate = composition;
        duplicate.omitted_modules = vec!["unknown-module".into(), "unknown-module".into()];
        assert_eq!(
            VerifiedContext::verified(&request, duplicate),
            Err(ContextRefusal::PayloadMismatch)
        );
    }

    #[test]
    fn the_curriculum_selection_names_the_metaflow_and_the_harness_target() {
        let mut request = request();
        request.harness_kind = HarnessKind::Codex;
        let selection = request.startup_selection();
        assert_eq!(selection.caller_metaflow.first_string, "Field");
        assert_eq!(selection.caller_metaflow.second_string, "flow");
        assert_eq!(selection.caller_metaflow.third_string, "Secondary");
        assert_eq!(selection.target, Target::Codex);
        assert_eq!(selection.user_modules, request.user_modules);
    }
}
