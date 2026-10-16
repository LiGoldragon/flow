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
//! The production composer is the Curriculum Nexus, asked over its socket
//! with `ComposeStartup` from the frozen schema in `ethos/curriculum.ethos`
//! (candidate 925a55ca, schema SHA-256 7450ce7a…382ad). Its answer names no
//! metaflow, so the metaflow check is Flow's own: the request carries the
//! launch's Address, and the composed launch keeps it with the identities
//! asked for it.

use crate::generated::curriculum::{
    CallerMetaflow, ContextProvenance, Outcome as CurriculumOutcome, Query as CurriculumQuery,
    StartupComposition, StartupPlacement, StartupSelection, Target,
};
use crate::generated::starter::{ContextRequest, ReadsVerifiedContext, SelectsStartup};
use sha2::{Digest, Sha256};
use signal_flow::{Address, Aspect, ContextRefusal, HarnessKind, Layer};
use std::{
    io::{Read, Write},
    os::unix::net::UnixStream,
    path::PathBuf,
    time::Duration,
};

/// Composes a flow's system and USER context from module identities.
pub trait ComposesFlowContext: Send + Sync {
    fn compose_flow_context(
        &self,
        request: &ContextRequest,
    ) -> Result<StartupComposition, ContextRefusal>;
}

/// The Curriculum Nexus, asked over its Unix socket: one big-endian u32
/// length-prefixed rkyv `Query` frame out, one `Outcome` frame back.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CurriculumComposer {
    socket: PathBuf,
}

pub trait OpensCurriculumComposer: Sized {
    /// How long one composition may take on the socket.
    const EXCHANGE_TIMEOUT: Duration = Duration::from_secs(30);
    /// The largest answer frame read: generous for two texts.
    const ANSWER_FRAME_LIMIT: usize = 64 * 1024 * 1024;
    fn at(socket: impl Into<PathBuf>) -> Self;
    /// `$XDG_RUNTIME_DIR/curriculum/curriculum.sock`, as Curriculum serves it.
    fn from_environment() -> Self;
}

impl OpensCurriculumComposer for CurriculumComposer {
    fn at(socket: impl Into<PathBuf>) -> Self {
        Self {
            socket: socket.into(),
        }
    }

    fn from_environment() -> Self {
        Self::at(
            std::env::var_os("XDG_RUNTIME_DIR")
                .map(PathBuf::from)
                .unwrap_or_else(|| PathBuf::from("/run"))
                .join("curriculum")
                .join("curriculum.sock"),
        )
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
        }
    }
}

trait ExchangesCurriculumFrames {
    fn exchange(&self, query: &CurriculumQuery) -> Result<CurriculumOutcome, String>;
}

impl ExchangesCurriculumFrames for CurriculumComposer {
    fn exchange(&self, query: &CurriculumQuery) -> Result<CurriculumOutcome, String> {
        let request = rkyv::to_bytes::<rkyv::rancor::Error>(query)
            .map_err(|error| format!("query not archived: {error}"))?;
        let length = u32::try_from(request.len()).map_err(|_| "query frame too large")?;
        let mut stream = UnixStream::connect(&self.socket)
            .map_err(|error| format!("Curriculum socket unreachable: {error}"))?;
        stream
            .set_read_timeout(Some(Self::EXCHANGE_TIMEOUT))
            .and_then(|_| stream.set_write_timeout(Some(Self::EXCHANGE_TIMEOUT)))
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
        if length > Self::ANSWER_FRAME_LIMIT {
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
    fn compose_flow_context(
        &self,
        request: &ContextRequest,
    ) -> Result<StartupComposition, ContextRefusal> {
        match self.exchange(&CurriculumQuery::ComposeStartup(
            request.startup_selection(),
        )) {
            Ok(CurriculumOutcome::StartupComposed(composition)) => Ok(composition),
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
}

pub trait VerifiesContext: Sized {
    /// The opening line of a rendered context text.
    const OPENING: &'static str = "These are your skills, given here; do not reload them.";
    fn verified(
        request: &ContextRequest,
        composition: StartupComposition,
    ) -> Result<Self, ContextRefusal>;
}

/// Cuts a rendered text back into its modules, checking each module's slice
/// against its rendered SHA-256 and the whole against the rendering.
trait CutsRenderedText {
    fn rendered_segments(&self, rows: &[&ContextProvenance]) -> Option<Vec<String>>;
}

impl CutsRenderedText for str {
    fn rendered_segments(&self, rows: &[&ContextProvenance]) -> Option<Vec<String>> {
        if rows.is_empty() {
            return self.is_empty().then(Vec::new);
        }
        let opening = format!(
            "{}\n\nModule IDs: {}\n",
            VerifiedContext::OPENING,
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
    fn verified(
        request: &ContextRequest,
        composition: StartupComposition,
    ) -> Result<Self, ContextRefusal> {
        let digest = |text: &str| format!("{:x}", Sha256::digest(text.as_bytes()));
        if digest(&composition.system_text) != composition.system_payload_sha256
            || digest(&composition.user_text) != composition.user_payload_sha256
        {
            return Err(ContextRefusal::PayloadMismatch);
        }
        let rows = &composition.context_provenance_vector;
        let placed = |placement: StartupPlacement| -> Vec<&ContextProvenance> {
            rows.iter()
                .filter(|row| row.startup_placement == placement)
                .collect()
        };
        let system_rows = placed(StartupPlacement::System);
        let user_rows = placed(StartupPlacement::User);
        let present = |identities: &[String], placed: &[&ContextProvenance]| {
            identities
                .iter()
                .all(|identity| placed.iter().any(|row| &row.module_identity == identity))
        };
        if !present(&request.system_modules, &system_rows)
            || !present(&request.user_modules, &user_rows)
        {
            return Err(ContextRefusal::ModuleMissing);
        }
        let ordered = |placed: &[&ContextProvenance]| {
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
}

/// A composer for tests: each identity's body is held in memory and
/// rendered as Curriculum renders it. An identity it does not hold is
/// rejected, as Curriculum rejects one outside its repositories.
#[cfg(test)]
#[derive(Debug, Clone, Default)]
pub struct FixtureComposer {
    pub bodies: std::collections::BTreeMap<String, String>,
}

#[cfg(test)]
impl From<&[(&str, &str)]> for FixtureComposer {
    fn from(modules: &[(&str, &str)]) -> Self {
        Self {
            bodies: modules
                .iter()
                .map(|(identity, body)| ((*identity).to_owned(), (*body).to_owned()))
                .collect(),
        }
    }
}

#[cfg(test)]
impl ComposesFlowContext for FixtureComposer {
    fn compose_flow_context(
        &self,
        request: &ContextRequest,
    ) -> Result<StartupComposition, ContextRefusal> {
        let digest = |text: &str| format!("{:x}", Sha256::digest(text.as_bytes()));
        let mut rows = Vec::new();
        for (identities, placement) in [
            (&request.system_modules, StartupPlacement::System),
            (&request.user_modules, StartupPlacement::User),
        ] {
            for (ordinal, identity) in identities.iter().enumerate() {
                let body = self
                    .bodies
                    .get(identity)
                    .ok_or(ContextRefusal::ComposerRejected)?;
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
                VerifiedContext::OPENING,
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
        let system_text = rendered(&request.system_modules);
        let user_text = rendered(&request.user_modules);
        Ok(StartupComposition {
            system_payload_sha256: digest(&system_text),
            user_payload_sha256: digest(&user_text),
            system_text,
            user_text,
            context_provenance_vector: rows,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ComposesFlowContext, ContextRequest, FixtureComposer, ReadsVerifiedContext, SelectsStartup,
        VerifiedContext, VerifiesContext,
    };
    use crate::generated::curriculum::Target;
    use signal_flow::{Address, Aspect, ContextRefusal, HarnessKind, Layer};

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
        assert!(verified.user_segments()[0].starts_with(VerifiedContext::OPENING));
        assert!(verified.user_segments()[1].starts_with("\n<!-- module: knowledge-flow -->\n"));
        assert!(verified.system_text().contains("Spirit body."));
        assert_eq!(verified.address(), request.address);
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
    fn a_requested_module_absent_at_its_placement_is_refused() {
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
    fn an_identity_the_composer_does_not_hold_is_rejected() {
        let mut request = request();
        request.user_modules.push("unknown-module".into());
        assert_eq!(
            fixture().compose_flow_context(&request),
            Err(ContextRefusal::ComposerRejected)
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
