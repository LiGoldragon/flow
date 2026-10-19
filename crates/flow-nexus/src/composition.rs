//! Exact-byte composition of a launch: its system prompt and its first USER
//! message, from the module identities its profile names.
//!
//! The context is composed by the context composer and verified
//! ([`crate::context`]); nothing composed is written to disk. The system
//! prompt is the verified system text, followed after a blank line by the
//! launch's own section (its predecessor and remembered flows). The first
//! USER message is the verified USER text, then the brief, sent once. When
//! it does not fit one message, it is split where modules begin into parts,
//! each opening with a header naming it part k of n; the brief is in the
//! last part.

use crate::context::{ComposesFlowContext, ContextComposer, VerifiedContext, VerifiesContext};
pub use crate::generated::starter::{
    AssemblesComposedLaunch, ComposesLaunch, ComposesVerifiedContext, ContextRequest,
    FitsOneMessage, HarnessedProfile, JoinsContextAndBrief, LeanStart, LeaveOutListing,
    PartNumbering, Profiled, ProfilesFreshSeat, ReadsVerifiedContext, RendersBrief,
    RendersLeanStart, ResolvesLeaveOuts, SegmentListing, SplitsStartupMessage, SystemPrompted,
    Textual,
};
use crate::waking::WakePolicy;
use datom_codec::Datomizable;
use protos::{Compactable, Protosizable};
use sha2::{Digest, Sha256};
use signal_flow::{
    Address, Aspect, ComposedLaunch, ContextRefusal, FirstPromptPayload, HarnessKind,
    LaunchProfile, LeaveOut, LeaveOutRefusal, PowerLevel, PromptPart, QueuedRequest, SystemPrompt,
    TargetReceiptRequest,
};
use std::collections::HashSet;

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum CompositionError {
    #[error("launch profile field is invalid: {0}")]
    InvalidProfileField(&'static str),
    #[error("the launch's context was refused: {0:?}")]
    Context(ContextRefusal),
    #[error("the launch's leave-outs were refused: {0:?}")]
    LeaveOut(LeaveOutRefusal),
}

impl From<ContextRefusal> for CompositionError {
    fn from(refusal: ContextRefusal) -> Self {
        Self::Context(refusal)
    }
}

/// Composes launches through one context composer.
#[derive(Clone, Debug)]
pub struct LaunchComposer {
    context: ContextComposer,
}

pub trait OpensLaunchComposer {
    fn with_context(context: ContextComposer) -> Self;
}

impl OpensLaunchComposer for LaunchComposer {
    fn with_context(context: ContextComposer) -> Self {
        Self { context }
    }
}

/// The trailing section a launch appends to its system prompt:
/// `Predecessor:` when the profile names one, `Remembered:` when it
/// remembers any flows, and nothing otherwise. Each line ends with a
/// newline.
pub trait RendersLaunchSection {
    fn launch_section(&self) -> String;
}

impl RendersLaunchSection for LaunchProfile {
    fn launch_section(&self) -> String {
        let mut section = String::new();
        if let Some(predecessor) = &self.flow_id_option {
            section.push_str(&format!("Predecessor: {predecessor}\n"));
        }
        if !self.remembered_flow_vector.is_empty() {
            let remembered = self
                .remembered_flow_vector
                .iter()
                .map(|flow| flow.flow_id.as_str())
                .collect::<Vec<_>>()
                .join(", ");
            section.push_str(&format!("Remembered: {remembered}\n"));
        }
        section
    }
}

pub(crate) const HERDR_REQUEST_LINE_LIMIT: usize = 1024 * 1024;
const HERDR_ENVELOPE_ALLOWANCE: usize = 1024;
const CODEX_TEXT_CHARACTER_LIMIT: usize = 1 << 20;
const ARGUMENT_BYTE_LIMIT: usize = 131_071;

/// What one message and one argument may hold for a harness.
///
/// A Claude part is typed by Herdr's `agent.prompt`, whose JSON request line
/// Herdr 0.8.2 reads up to 1 MiB (`MAX_INITIAL_REQUEST_BYTES`); a part fits
/// when its JSON-encoded text with the request envelope stays under that.
/// A Codex part is one text item of `turn/start`, which the Codex
/// app-server refuses over `MAX_USER_INPUT_TEXT_CHARS` (1 << 20)
/// characters. The system prompt is held to what one process argument
/// holds, 131072 bytes with its terminating NUL, though it no longer travels
/// as one: Claude reads it from the Nexus's in-memory file and a Codex
/// thread is given it as its base instructions.
impl FitsOneMessage for HarnessKind {
    fn fits_one_message<N: Textual>(&self, input: N) -> bool {
        let text = input.text();
        match self {
            HarnessKind::Claude => serde_json::to_string(&text).is_ok_and(|encoded| {
                encoded.len() + HERDR_ENVELOPE_ALLOWANCE <= HERDR_REQUEST_LINE_LIMIT
            }),
            HarnessKind::Codex => text.chars().count() <= CODEX_TEXT_CHARACTER_LIMIT,
        }
    }

    fn system_argument<N: Textual>(&self, input: N) -> String {
        let text = input.text();
        match self {
            HarnessKind::Claude => text,
            // A JSON string is a TOML basic string; Codex reads `-c` values
            // as TOML.
            HarnessKind::Codex => format!(
                "instructions={}",
                serde_json::to_string(&text).unwrap_or_default()
            ),
        }
    }

    fn fits_one_argument<N: Textual>(&self, input: N) -> bool {
        let text = input.text();
        !text.contains('\0') && self.system_argument(&text).len() <= ARGUMENT_BYTE_LIMIT
    }
}

/// The body of a first USER message: the segments with the brief appended
/// to the last, after a blank line; the brief alone when there is no USER
/// context.
impl JoinsContextAndBrief for [String] {
    fn joined_with<N: Textual>(&self, input: N) -> Vec<String> {
        let brief = input.text();
        let mut pieces = self.to_vec();
        match pieces.last_mut() {
            Some(last) => {
                last.push_str(if last.ends_with('\n') { "\n" } else { "\n\n" });
                last.push_str(&brief);
            }
            None => pieces.push(brief),
        }
        pieces
    }
}

/// The parts of a first USER message: the message whole when it fits; else
/// split where modules begin, the brief in the last part, each part headed
/// with its number. A module that cannot fit one part with its header
/// refuses.
impl SplitsStartupMessage for HarnessKind {
    fn part_header<N: PartNumbering>(input: N) -> String {
        let (number, count) = (input.part_number(), input.part_count());
        if number == count {
            format!("Startup message, part {number} of {count}, the last.\n\n")
        } else {
            format!(
                "Startup message, part {number} of {count}. More of it follows: do nothing until part {count} of {count} arrives, and answer only that you are waiting for it.\n\n"
            )
        }
    }

    fn startup_parts<N: SegmentListing, O: Textual>(
        &self,
        input_0: N,
        input_1: O,
    ) -> Result<Vec<String>, ContextRefusal> {
        let pieces = input_0.segments().joined_with(input_1.text());
        let whole = pieces.concat();
        if self.fits_one_message(&whole) {
            return Ok(vec![whole]);
        }
        // Packed under the longest header a part can have, so the real one
        // never makes a part overflow.
        let widest = Self::part_header((1, 999_999));
        let mut chunks: Vec<String> = Vec::new();
        let mut current = String::new();
        for piece in pieces {
            if !current.is_empty() && !self.fits_one_message(format!("{widest}{current}{piece}")) {
                chunks.push(std::mem::take(&mut current));
            }
            current.push_str(&piece);
            if !self.fits_one_message(format!("{widest}{current}")) {
                return Err(ContextRefusal::ModuleTooLarge);
            }
        }
        chunks.push(current);
        let count = chunks.len();
        Ok(chunks
            .into_iter()
            .enumerate()
            .map(|(index, chunk)| format!("{}{chunk}", Self::part_header((index + 1, count))))
            .collect())
    }
}

/// Verifies the exact body and part digests, and that the parts are the
/// body, before a composed prompt crosses either native harness boundary.
pub trait ValidatesComposedPrompt {
    fn has_canonical_first_prompt(&self) -> bool;
}

impl ValidatesComposedPrompt for ComposedLaunch {
    fn has_canonical_first_prompt(&self) -> bool {
        let digest = |text: &str| format!("{:x}", Sha256::digest(text.as_bytes()));
        let payload = &self.first_prompt_payload;
        let parts = &payload.prompt_part_vector;
        if digest(&payload.first_prompt_body) != payload.prompt_sha256
            || self.target_receipt_request.launch_request_id
                != self.launch_profile.launch_request_id
            || self.target_receipt_request.prompt_sha256 != payload.prompt_sha256
            || digest(&self.system_prompt.system_prompt_text)
                != self.system_prompt.system_prompt_sha256
            || parts.is_empty()
            || parts
                .iter()
                .any(|part| digest(&part.prompt_part_text) != part.prompt_sha256)
        {
            return false;
        }
        if parts.len() == 1 {
            return parts[0].prompt_part_text == payload.first_prompt_body;
        }
        let mut body = String::new();
        for (index, part) in parts.iter().enumerate() {
            let header = HarnessKind::part_header((index + 1, parts.len()));
            let Some(chunk) = part.prompt_part_text.strip_prefix(&header) else {
                return false;
            };
            body.push_str(chunk);
        }
        body == payload.first_prompt_body
    }
}

trait NamesRole {
    fn aspect_name(&self) -> &'static str;
    fn power_name(&self) -> &'static str;
}

impl NamesRole for LaunchProfile {
    fn aspect_name(&self) -> &'static str {
        match self.address.aspect {
            Aspect::Psyche => "Psyche",
            Aspect::Mind => "Mind",
            Aspect::Field => "Field",
        }
    }

    fn power_name(&self) -> &'static str {
        match <crate::RunningNexus as WakePolicy>::power_of(&self.address.layer) {
            PowerLevel::High => "High",
            PowerLevel::Medium => "Medium",
            PowerLevel::Low => "Low",
            PowerLevel::UltraLow => "Ultra Low",
        }
    }
}

trait ValidatesLaunchProfile {
    fn validate(&self) -> Result<(), CompositionError>;
}

impl ValidatesLaunchProfile for LaunchProfile {
    fn validate(&self) -> Result<(), CompositionError> {
        let profile = self;
        if profile.launch_request_id.is_empty()
            || !profile.launch_request_id.bytes().all(|byte| {
                byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b':' | b'-')
            })
        {
            return Err(CompositionError::InvalidProfileField("launch_request_id"));
        }
        if profile.model_name.is_empty() || profile.model_name.contains(['\r', '\n']) {
            return Err(CompositionError::InvalidProfileField("model_name"));
        }
        if profile.effort.is_empty() || profile.effort.contains(['\r', '\n']) {
            return Err(CompositionError::InvalidProfileField("effort"));
        }
        if profile.herdr_session_name.is_empty()
            || profile.herdr_session_name.contains(['\r', '\n'])
        {
            return Err(CompositionError::InvalidProfileField("herdr_session_name"));
        }
        let identities_valid = |identities: &[String]| {
            let mut seen = HashSet::new();
            identities.iter().all(|identity| {
                !identity.is_empty()
                    && identity.bytes().all(|byte| {
                        byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-'
                    })
                    && seen.insert(identity)
            })
        };
        if profile.system_modules.is_empty() || !identities_valid(&profile.system_modules) {
            return Err(CompositionError::InvalidProfileField("system_modules"));
        }
        if !identities_valid(&profile.user_modules)
            || profile
                .user_modules
                .iter()
                .any(|identity| profile.system_modules.contains(identity))
        {
            return Err(CompositionError::InvalidProfileField("user_modules"));
        }
        if profile
            .remembered_flow_vector
            .iter()
            .any(|remembered| remembered.remembering_depth < 0)
        {
            return Err(CompositionError::InvalidProfileField(
                "remembered_flow_vector",
            ));
        }
        Ok(())
    }
}

/// The brief that follows the context: the instruction itself for Claude,
/// under a launch record for Codex.
impl RendersBrief for HarnessedProfile {
    fn brief(&self) -> String {
        let profile = &self.launch_profile;
        if self.harness_kind == HarnessKind::Claude {
            return profile.instruction_prompt.clone();
        }
        format!(
            "# Flow launch\n\nRole: {} {}\nHarness: Codex\nModel: {}\nEffort: {}\nHerdr session: {}\nRemote control: app-server endpoint\n\n{}",
            profile.aspect_name(),
            profile.power_name(),
            profile.model_name,
            profile.effort,
            profile.herdr_session_name,
            profile.instruction_prompt,
        )
    }
}

/// The leave-outs a launch takes: its own, else the default its metaflow
/// configures, checked against its harness.
impl ResolvesLeaveOuts for HarnessedProfile {
    fn effective_leave_outs<N: LeaveOutListing>(
        &self,
        input: N,
    ) -> Result<Vec<LeaveOut>, CompositionError> {
        let leave_outs = self
            .launch_profile
            .leave_outs_option
            .clone()
            .unwrap_or_else(|| input.leave_outs());
        let lean = LeanStart {
            harness_kind: self.harness_kind.clone(),
            leave_outs: leave_outs.clone(),
        };
        match lean.refusal() {
            Some(refusal) => Err(CompositionError::LeaveOut(refusal)),
            None => Ok(leave_outs),
        }
    }
}

/// The profile's context, composed and verified, and the system prompt it
/// gives the launch: the verified system text, then the launch's own
/// section after a blank line.
impl ComposesVerifiedContext for LaunchComposer {
    fn verified_context<N: Profiled>(&self, input: N) -> Result<VerifiedContext, CompositionError> {
        let profile = input.profile();
        profile.validate()?;
        let request = ContextRequest {
            address: profile.address.clone(),
            harness_kind: input.harness_kind(),
            system_modules: profile.system_modules.clone(),
            user_modules: profile.user_modules.clone(),
        };
        let composition = self.context.compose_flow_context(&request)?;
        let verified = VerifiedContext::verified(&request, composition)?;
        if verified.address() != profile.address {
            return Err(ContextRefusal::MetaflowMismatch.into());
        }
        Ok(verified)
    }

    fn launch_system_prompt<N: Profiled, O: ReadsVerifiedContext>(
        &self,
        input_0: N,
        input_1: O,
    ) -> Result<SystemPrompt, ContextRefusal> {
        let mut system_prompt_text = input_1.system_text();
        let section = input_0.profile().launch_section();
        if !section.is_empty() {
            if !system_prompt_text.is_empty() && !system_prompt_text.ends_with('\n') {
                system_prompt_text.push('\n');
            }
            system_prompt_text.push('\n');
            system_prompt_text.push_str(&section);
        }
        if !input_0
            .harness_kind()
            .fits_one_argument(&system_prompt_text)
        {
            return Err(ContextRefusal::SystemTooLarge);
        }
        let system_prompt_sha256 = format!("{:x}", Sha256::digest(system_prompt_text.as_bytes()));
        Ok(SystemPrompt {
            system_prompt_text,
            system_prompt_sha256,
        })
    }
}

/// A composed launch from its parts: `prompt_sha256` is SHA-256 over the
/// exact UTF-8 bytes of the whole first USER message, and each part carries
/// the SHA-256 of its own exact text. Hashes stay in the store and never
/// enter a prompt.
impl AssemblesComposedLaunch for LaunchComposer {
    fn assembled<
        N: Profiled,
        O: SystemPrompted,
        P: Textual,
        Q: SegmentListing,
        R: LeaveOutListing,
    >(
        input_0: N,
        input_1: O,
        input_2: P,
        input_3: Q,
        input_4: R,
    ) -> ComposedLaunch {
        let digest = |text: &str| format!("{:x}", Sha256::digest(text.as_bytes()));
        let profile = input_0.profile();
        let body = input_2.text();
        let prompt_sha256 = digest(&body);
        ComposedLaunch {
            target_receipt_request: TargetReceiptRequest {
                launch_request_id: profile.launch_request_id.clone(),
                prompt_sha256: prompt_sha256.clone(),
            },
            launch_profile: profile,
            harness_kind: input_0.harness_kind(),
            system_prompt: input_1.system_prompt(),
            first_prompt_payload: FirstPromptPayload {
                first_prompt_body: body,
                prompt_sha256,
                prompt_part_vector: input_3
                    .segments()
                    .into_iter()
                    .map(|text| PromptPart {
                        prompt_sha256: digest(&text),
                        prompt_part_text: text,
                    })
                    .collect(),
            },
            leave_outs: input_4.leave_outs(),
        }
    }
}

/// Composes one launch from a profile and the harness the Nexus derived
/// for it. The launch's leave-outs are its profile's when it names them,
/// else the metaflow's default; a set its harness refuses refuses the
/// launch. A resumption is the original profile with its system prompt
/// composed again, carrying the text as its one part; a resumed flow is not
/// observed as a launch, and the hash only names the text.
impl ComposesLaunch for LaunchComposer {
    fn compose<N: Profiled, O: LeaveOutListing>(
        &self,
        input_0: N,
        input_1: O,
    ) -> Result<ComposedLaunch, CompositionError> {
        let harnessed = HarnessedProfile {
            launch_profile: input_0.profile(),
            harness_kind: input_0.harness_kind(),
        };
        let leave_outs = harnessed.effective_leave_outs(input_1)?;
        let verified = self.verified_context(&harnessed)?;
        let system_prompt = self.launch_system_prompt(&harnessed, &verified)?;
        let brief = harnessed.brief();
        let segments = verified.user_segments();
        let body = segments.joined_with(&brief).concat();
        let parts = harnessed.harness_kind.startup_parts(&segments, &brief)?;
        Ok(Self::assembled(
            &harnessed,
            &system_prompt,
            &body,
            &parts,
            &leave_outs,
        ))
    }

    fn resumption<N: Profiled, O: LeaveOutListing, P: Textual>(
        &self,
        input_0: N,
        input_1: O,
        input_2: P,
    ) -> Result<ComposedLaunch, CompositionError> {
        let harnessed = HarnessedProfile {
            launch_profile: input_0.profile(),
            harness_kind: input_0.harness_kind(),
        };
        let leave_outs = harnessed.effective_leave_outs(input_1)?;
        let verified = self.verified_context(&harnessed)?;
        let system_prompt = self.launch_system_prompt(&harnessed, &verified)?;
        let text = input_2.text();
        Ok(Self::assembled(
            &harnessed,
            &system_prompt,
            &text,
            &[text.clone()][..],
            &leave_outs,
        ))
    }
}

/// The text a metaflow's queued requests become in a flow's prompt: one
/// opening line, then each queued request as its own datom, oldest first,
/// each on a line of its own: who sent it, then the Request, as
/// `{ Sender.{ Mind flow Tertiary } Order.«...» }`, or `{ Unknown ... }` for
/// one queued before Flow kept senders. It is typed as the datom it is, as a
/// Message is; a control character other than LF and TAB is a key to a
/// harness, not a word, and is typed as U+FFFD.
pub trait RendersQueuedWork {
    const QUEUED_WORK_OPENING: &'static str = "Work queued for this metaflow while no flow of it was running, oldest first, each with who sent it:";
    fn prompt_text(&self) -> String;
}

impl RendersQueuedWork for [QueuedRequest] {
    fn prompt_text(&self) -> String {
        let mut text = String::from(Self::QUEUED_WORK_OPENING);
        for request in self {
            text.push('\n');
            text.extend(
                request
                    .datomize(Vec::new())
                    .protosize()
                    .compact()
                    .chars()
                    .map(|glyph| match glyph {
                        '\n' | '\t' => glyph,
                        glyph if glyph.is_control() => '\u{fffd}',
                        glyph => glyph,
                    }),
            );
        }
        text
    }
}

/// Everything a fresh wake knows of the flow it starts, all of it read from
/// the meta configuration and the metaflow's last flow: its Address, the
/// layer's Model, the module identities and leave-outs the last launch
/// named, the Herdr session its last flow ran in, and the queued work.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FreshSeat {
    pub address: Address,
    pub launch_request_id: String,
    pub model_name: String,
    pub effort: String,
    pub herdr_session_name: String,
    pub system_modules: Vec<String>,
    pub user_modules: Vec<String>,
    pub leave_outs_option: Option<Vec<LeaveOut>>,
    pub prompt: String,
}

/// The launch profile of a fresh wake: the queued work is its brief, and
/// its context is the module identities the metaflow's last launch named,
/// composed again through the same route as any Start.
impl ProfilesFreshSeat for FreshSeat {
    fn fresh_profile(&self) -> LaunchProfile {
        LaunchProfile {
            launch_request_id: self.launch_request_id.clone(),
            system_modules: self.system_modules.clone(),
            user_modules: self.user_modules.clone(),
            address: self.address.clone(),
            model_name: self.model_name.clone(),
            effort: self.effort.clone(),
            flow_id_option: None,
            remembered_flow_vector: Vec::new(),
            herdr_session_name: self.herdr_session_name.clone(),
            leave_outs_option: self.leave_outs_option.clone(),
            instruction_prompt: self.prompt.clone(),
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::{
        ComposesLaunch, CompositionError, FitsOneMessage, LaunchComposer, OpensLaunchComposer,
        SplitsStartupMessage, ValidatesComposedPrompt,
    };
    use crate::context::FixtureComposer;
    use signal_flow::{
        Address, Aspect, ContextRefusal, HarnessKind, LaunchProfile, Layer, LeaveOut,
        RememberedFlow,
    };

    /// A launch profile and a composer holding its modules, for tests.
    pub(crate) fn fixture_profile() -> LaunchProfile {
        LaunchProfile {
            launch_request_id: "launch-42".into(),
            system_modules: vec!["spirit-core".into(), "intent-flow".into()],
            user_modules: vec!["operation-main-flow".into(), "knowledge-flow".into()],
            address: Address {
                aspect: Aspect::Field,
                topic: "flow".into(),
                layer: Layer::Secondary,
            },
            model_name: "gpt-6-astra".into(),
            effort: "medium".into(),
            flow_id_option: Some("1b8ac0".into()),
            remembered_flow_vector: vec![RememberedFlow {
                flow_id: "836818".into(),
                remembering_depth: 1,
            }],
            herdr_session_name: "messaging-build".into(),
            leave_outs_option: None,
            instruction_prompt: "Carry the bounded task.".into(),
        }
    }

    pub(crate) fn fixture_composer() -> LaunchComposer {
        LaunchComposer::with_context(crate::context::ContextComposer::Fixture(
            FixtureComposer::from(
                &[
                    ("spirit-core", "Spirit body."),
                    ("intent-flow", "Intent body."),
                    ("operation-main-flow", "Main flow body."),
                    ("knowledge-flow", "Knowledge body."),
                ][..],
            ),
        ))
    }

    /// A composer holding one large knowledge module per size.
    fn large(sizes: &[usize]) -> (LaunchComposer, Vec<String>) {
        let names = (0..sizes.len())
            .map(|index| format!("knowledge-large-{index}"))
            .collect::<Vec<_>>();
        let bodies = sizes
            .iter()
            .enumerate()
            .map(|(index, size)| char::from(b'a' + index as u8).to_string().repeat(*size))
            .collect::<Vec<_>>();
        let mut modules = vec![
            ("spirit-core", "Spirit body.".to_owned()),
            ("intent-flow", "Intent body.".to_owned()),
        ];
        for (name, body) in names.iter().zip(&bodies) {
            modules.push((name.as_str(), body.clone()));
        }
        let held = modules
            .iter()
            .map(|(name, body)| (*name, body.as_str()))
            .collect::<Vec<_>>();
        (
            LaunchComposer::with_context(crate::context::ContextComposer::Fixture(
                FixtureComposer::from(&held[..]),
            )),
            names,
        )
    }

    #[test]
    fn a_claude_message_is_the_user_context_then_the_brief_in_one_part() {
        let profile = fixture_profile();
        let harness = HarnessKind::Claude;
        let composed = fixture_composer()
            .compose(
                crate::composition::HarnessedProfile {
                    harness_kind: harness.clone(),
                    launch_profile: profile.clone(),
                },
                Vec::<signal_flow::LeaveOut>::new(),
            )
            .unwrap();
        let body = &composed.first_prompt_payload.first_prompt_body;
        assert!(body.starts_with("These are your skills, given here; do not reload them.\n\nModule IDs: operation-main-flow, knowledge-flow\n"));
        assert!(body.contains("\n<!-- module: knowledge-flow -->\nKnowledge body.\n<!-- end-module: knowledge-flow -->\n"));
        assert!(body.ends_with("<!-- end-module: knowledge-flow -->\n\nCarry the bounded task."));
        assert_eq!(body.matches("Main flow body.").count(), 1);
        assert!(!body.contains("Spirit body."));
        assert_eq!(composed.first_prompt_payload.prompt_part_vector.len(), 1);
        assert_eq!(
            &composed.first_prompt_payload.prompt_part_vector[0].prompt_part_text,
            body
        );
        assert!(!body.contains("Skill tool") && !body.contains("/operation-main-flow"));
        assert!(composed.has_canonical_first_prompt());
    }

    #[test]
    fn the_system_prompt_is_the_system_context_then_the_launch_section() {
        let composed = fixture_composer()
            .compose(
                crate::composition::HarnessedProfile {
                    harness_kind: HarnessKind::Codex,
                    launch_profile: fixture_profile().clone(),
                },
                Vec::<signal_flow::LeaveOut>::new(),
            )
            .unwrap();
        let system = &composed.system_prompt.system_prompt_text;
        assert!(system.contains("<!-- module: spirit-core -->\nSpirit body.\n"));
        assert!(system.ends_with(
            "<!-- end-module: intent-flow -->\n\nPredecessor: 1b8ac0\nRemembered: 836818\n"
        ));
        assert!(!system.contains("Knowledge body."));
        let body = &composed.first_prompt_payload.first_prompt_body;
        assert!(!body.contains("Predecessor") && !body.contains("Remembered"));
    }

    #[test]
    fn a_codex_message_puts_its_launch_record_after_the_context() {
        let composed = fixture_composer()
            .compose(
                crate::composition::HarnessedProfile {
                    harness_kind: HarnessKind::Codex,
                    launch_profile: fixture_profile().clone(),
                },
                Vec::<signal_flow::LeaveOut>::new(),
            )
            .unwrap();
        let body = &composed.first_prompt_payload.first_prompt_body;
        assert!(body.starts_with("These are your skills"));
        assert!(body.ends_with(concat!(
            "<!-- end-module: knowledge-flow -->\n\n# Flow launch\n\n",
            "Role: Field Medium\nHarness: Codex\nModel: gpt-6-astra\nEffort: medium\n",
            "Herdr session: messaging-build\nRemote control: app-server endpoint\n\n",
            "Carry the bounded task."
        )));
        assert!(!body.contains('$'));
        assert!(composed.has_canonical_first_prompt());
    }

    #[test]
    fn a_message_too_large_for_one_part_is_split_where_modules_begin() {
        let size = 400_000;
        let (composer, names) = large(&[size, size, size]);
        let mut profile = fixture_profile();
        profile.user_modules = names.clone();
        for harness in [HarnessKind::Claude, HarnessKind::Codex] {
            let composed = composer
                .compose(
                    crate::composition::HarnessedProfile {
                        harness_kind: harness.clone(),
                        launch_profile: profile.clone(),
                    },
                    Vec::<signal_flow::LeaveOut>::new(),
                )
                .unwrap();
            let parts = &composed.first_prompt_payload.prompt_part_vector;
            assert_eq!(parts.len(), 2, "{harness:?}");
            assert!(parts[0].prompt_part_text.starts_with(
                "Startup message, part 1 of 2. More of it follows: do nothing until part 2 of 2 arrives, and answer only that you are waiting for it.\n\nThese are your skills"
            ));
            assert!(parts[1].prompt_part_text.starts_with(
                "Startup message, part 2 of 2, the last.\n\n\n<!-- module: knowledge-large-2 -->\n"
            ));
            assert!(
                parts[1]
                    .prompt_part_text
                    .ends_with("Carry the bounded task.")
            );
            assert!(
                !parts[0]
                    .prompt_part_text
                    .contains("Carry the bounded task.")
            );
            for part in parts {
                assert!(harness.fits_one_message(&part.prompt_part_text));
                assert!(!part.prompt_part_text.contains("seat"));
            }
            assert!(composed.has_canonical_first_prompt());
        }
    }

    #[test]
    fn a_module_that_cannot_fit_one_part_refuses_the_launch() {
        let (composer, names) = large(&[1_100_000]);
        let mut profile = fixture_profile();
        profile.user_modules = names;
        assert_eq!(
            composer.compose(
                crate::composition::HarnessedProfile {
                    harness_kind: HarnessKind::Codex,
                    launch_profile: profile.clone()
                },
                Vec::<signal_flow::LeaveOut>::new(),
            ),
            Err(CompositionError::Context(ContextRefusal::ModuleTooLarge))
        );
    }

    #[test]
    fn a_system_prompt_past_one_argument_refuses_the_launch() {
        let (composer, names) = large(&[10, 140_000]);
        let mut profile = fixture_profile();
        profile.system_modules = vec!["spirit-core".into(), names[1].clone()];
        profile.user_modules = vec![names[0].clone()];
        assert_eq!(
            composer.compose(
                crate::composition::HarnessedProfile {
                    harness_kind: HarnessKind::Codex,
                    launch_profile: profile.clone()
                },
                Vec::<signal_flow::LeaveOut>::new(),
            ),
            Err(CompositionError::Context(ContextRefusal::SystemTooLarge))
        );
    }

    #[test]
    fn a_profile_naming_no_valid_identities_is_refused_before_composition() {
        let composer = fixture_composer();
        type ProfileChange = fn(&mut LaunchProfile);
        let cases: [(&str, ProfileChange); 4] = [
            ("system_modules", |profile| profile.system_modules.clear()),
            ("system_modules", |profile| {
                profile.system_modules.push("/abs/path.md".into())
            }),
            ("user_modules", |profile| {
                profile.user_modules.push("knowledge-flow".into())
            }),
            ("user_modules", |profile| {
                profile.user_modules.push("spirit-core".into())
            }),
        ];
        for (field, change) in cases {
            let mut profile = fixture_profile();
            change(&mut profile);
            assert_eq!(
                composer.compose(
                    crate::composition::HarnessedProfile {
                        harness_kind: HarnessKind::Codex,
                        launch_profile: profile.clone()
                    },
                    Vec::<signal_flow::LeaveOut>::new(),
                ),
                Err(CompositionError::InvalidProfileField(field))
            );
        }
    }

    #[test]
    fn an_identity_the_composer_rejects_refuses_the_launch() {
        let mut profile = fixture_profile();
        profile.user_modules.push("unknown-module".into());
        assert_eq!(
            fixture_composer().compose(
                crate::composition::HarnessedProfile {
                    harness_kind: HarnessKind::Codex,
                    launch_profile: profile.clone()
                },
                Vec::<signal_flow::LeaveOut>::new(),
            ),
            Err(CompositionError::Context(ContextRefusal::ComposerRejected))
        );
    }

    #[test]
    fn a_tampered_part_is_not_canonical() {
        let size = 400_000;
        let (composer, names) = large(&[size, size, size]);
        let mut profile = fixture_profile();
        profile.user_modules = names;
        let mut composed = composer
            .compose(
                crate::composition::HarnessedProfile {
                    harness_kind: HarnessKind::Codex,
                    launch_profile: profile.clone(),
                },
                Vec::<signal_flow::LeaveOut>::new(),
            )
            .unwrap();
        assert!(composed.has_canonical_first_prompt());
        composed.first_prompt_payload.prompt_part_vector[1]
            .prompt_part_text
            .push('x');
        assert!(!composed.has_canonical_first_prompt());
    }

    #[test]
    fn a_resumption_composes_the_system_prompt_again_and_carries_the_text() {
        let profile = fixture_profile();
        let harness = HarnessKind::Claude;
        let composer = fixture_composer();
        let launched = composer
            .compose(
                crate::composition::HarnessedProfile {
                    harness_kind: harness.clone(),
                    launch_profile: profile.clone(),
                },
                Vec::<signal_flow::LeaveOut>::new(),
            )
            .unwrap();
        let resumed = composer
            .resumption(
                crate::composition::HarnessedProfile {
                    harness_kind: harness.clone(),
                    launch_profile: profile.clone(),
                },
                Vec::<LeaveOut>::new(),
                "Work queued.",
            )
            .unwrap();
        assert_eq!(resumed.system_prompt, launched.system_prompt);
        assert_eq!(
            resumed.first_prompt_payload.first_prompt_body,
            "Work queued."
        );
        assert_eq!(resumed.first_prompt_payload.prompt_part_vector.len(), 1);
        assert!(resumed.has_canonical_first_prompt());
    }

    #[test]
    fn a_start_naming_leave_outs_replaces_its_metaflow_default() {
        let composer = fixture_composer();
        let mut profile = fixture_profile();
        let harness = HarnessKind::Claude;
        let default = vec![LeaveOut::SkillTool, LeaveOut::WorkflowTool];
        // None takes the default.
        let composed = composer
            .compose(
                crate::composition::HarnessedProfile {
                    harness_kind: harness.clone(),
                    launch_profile: profile.clone(),
                },
                default.clone(),
            )
            .unwrap();
        assert_eq!(composed.leave_outs, default);
        // A Start's own vector replaces it, even an empty one.
        profile.leave_outs_option = Some(vec![LeaveOut::GitInstructions]);
        let composed = composer
            .compose(
                crate::composition::HarnessedProfile {
                    harness_kind: harness.clone(),
                    launch_profile: profile.clone(),
                },
                default.clone(),
            )
            .unwrap();
        assert_eq!(composed.leave_outs, [LeaveOut::GitInstructions]);
        profile.leave_outs_option = Some(Vec::new());
        assert!(
            composer
                .compose(
                    crate::composition::HarnessedProfile {
                        harness_kind: harness.clone(),
                        launch_profile: profile.clone()
                    },
                    default.clone()
                )
                .unwrap()
                .leave_outs
                .is_empty()
        );
        // A set its harness refuses refuses the launch.
        profile.leave_outs_option = Some(vec![LeaveOut::ToolSearch]);
        assert_eq!(
            composer.compose(
                crate::composition::HarnessedProfile {
                    harness_kind: harness.clone(),
                    launch_profile: profile.clone()
                },
                default
            ),
            Err(CompositionError::LeaveOut(
                signal_flow::LeaveOutRefusal::Indispensable(LeaveOut::ToolSearch)
            ))
        );
    }

    #[test]
    fn the_part_header_names_its_place_and_never_a_seat() {
        assert_eq!(
            HarnessKind::part_header((3, 3)),
            "Startup message, part 3 of 3, the last.\n\n"
        );
        assert!(HarnessKind::part_header((1, 3)).contains("part 3 of 3"));
        assert!(!HarnessKind::part_header((1, 3)).contains("seat"));
    }
}
