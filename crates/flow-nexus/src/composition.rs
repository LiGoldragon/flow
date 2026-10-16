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

use crate::context::{ComposesFlowContext, VerifiedContext, VerifiesContext};
use crate::generated::starter::{
    ContextRequest, LeanStart, ProfilesFreshSeat, ReadsVerifiedContext, RendersBrief,
    RendersLeanStart,
};
use datom_codec::Datomizable;
use protos::{Compactable, Protosizable};
use sha2::{Digest, Sha256};
use signal_flow::{
    Address, Aspect, ComposedLaunch, ContextRefusal, FirstPromptPayload, HarnessKind,
    LaunchProfile, LeaveOut, LeaveOutRefusal, PowerLevel, PromptPart, Request, SystemPrompt,
    TargetReceiptRequest,
};
use std::{collections::HashSet, sync::Arc};

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
#[derive(Clone)]
pub struct LaunchComposer {
    context: Arc<dyn ComposesFlowContext>,
}

pub trait OpensLaunchComposer {
    fn with_context(context: Arc<dyn ComposesFlowContext>) -> Self;
}

impl OpensLaunchComposer for LaunchComposer {
    fn with_context(context: Arc<dyn ComposesFlowContext>) -> Self {
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

/// What one message and one argument may hold for a harness.
///
/// A Claude part is typed by Herdr's `agent.prompt`, whose JSON request line
/// Herdr 0.8.2 reads up to 1 MiB (`MAX_INITIAL_REQUEST_BYTES`); a part fits
/// when its JSON-encoded text with the request envelope stays under that.
/// A Codex part is one text item of `turn/start`, which the Codex
/// app-server refuses over `MAX_USER_INPUT_TEXT_CHARS` (1 << 20)
/// characters. The system prompt travels as one process argument
/// (`--system-prompt` for Claude, `instructions=` for Codex), which Linux
/// bounds at 131072 bytes with its terminating NUL.
pub trait FitsOneMessage {
    const HERDR_REQUEST_LINE_LIMIT: usize = 1024 * 1024;
    const HERDR_ENVELOPE_ALLOWANCE: usize = 1024;
    const CODEX_TEXT_CHARACTER_LIMIT: usize = 1 << 20;
    const ARGUMENT_BYTE_LIMIT: usize = 131_071;
    fn fits_one_message(&self, text: &str) -> bool;
    /// The process argument that carries the system prompt.
    fn system_argument(&self, text: &str) -> String;
    fn fits_one_argument(&self, text: &str) -> bool;
}

impl FitsOneMessage for HarnessKind {
    fn fits_one_message(&self, text: &str) -> bool {
        match self {
            HarnessKind::Claude => serde_json::to_string(text).is_ok_and(|encoded| {
                encoded.len() + Self::HERDR_ENVELOPE_ALLOWANCE <= Self::HERDR_REQUEST_LINE_LIMIT
            }),
            HarnessKind::Codex => text.chars().count() <= Self::CODEX_TEXT_CHARACTER_LIMIT,
        }
    }

    fn system_argument(&self, text: &str) -> String {
        match self {
            HarnessKind::Claude => text.to_owned(),
            // A JSON string is a TOML basic string; Codex reads `-c` values
            // as TOML.
            HarnessKind::Codex => format!(
                "instructions={}",
                serde_json::to_string(text).unwrap_or_default()
            ),
        }
    }

    fn fits_one_argument(&self, text: &str) -> bool {
        !text.contains('\0') && self.system_argument(text).len() <= Self::ARGUMENT_BYTE_LIMIT
    }
}

/// The parts of a first USER message.
pub trait SplitsStartupMessage {
    /// The header of part `number` (from 1) of `count`.
    fn part_header(number: usize, count: usize) -> String {
        if number == count {
            format!("Startup message, part {number} of {count}, the last.\n\n")
        } else {
            format!(
                "Startup message, part {number} of {count}. More of it follows: do nothing until part {count} of {count} arrives, and answer only that you are waiting for it.\n\n"
            )
        }
    }
    /// The message whole when it fits; else split where modules begin, the
    /// brief in the last part. A module that cannot fit one part with its
    /// header refuses.
    fn startup_parts(
        &self,
        segments: &[String],
        brief: &str,
    ) -> Result<Vec<String>, ContextRefusal>;
}

/// The body of a first USER message: the context, then the brief.
pub trait JoinsContextAndBrief {
    fn joined_with(&self, brief: &str) -> Vec<String>;
}

impl JoinsContextAndBrief for [String] {
    /// The segments with the brief appended to the last, after a blank line;
    /// the brief alone when there is no USER context.
    fn joined_with(&self, brief: &str) -> Vec<String> {
        let mut pieces = self.to_vec();
        match pieces.last_mut() {
            Some(last) => {
                last.push_str(if last.ends_with('\n') { "\n" } else { "\n\n" });
                last.push_str(brief);
            }
            None => pieces.push(brief.to_owned()),
        }
        pieces
    }
}

impl SplitsStartupMessage for HarnessKind {
    fn startup_parts(
        &self,
        segments: &[String],
        brief: &str,
    ) -> Result<Vec<String>, ContextRefusal> {
        let pieces = segments.joined_with(brief);
        let whole = pieces.concat();
        if self.fits_one_message(&whole) {
            return Ok(vec![whole]);
        }
        // Packed under the longest header a part can have, so the real one
        // never makes a part overflow.
        let widest = Self::part_header(1, 999_999);
        let mut chunks: Vec<String> = Vec::new();
        let mut current = String::new();
        for piece in pieces {
            if !current.is_empty() && !self.fits_one_message(&format!("{widest}{current}{piece}")) {
                chunks.push(std::mem::take(&mut current));
            }
            current.push_str(&piece);
            if !self.fits_one_message(&format!("{widest}{current}")) {
                return Err(ContextRefusal::ModuleTooLarge);
            }
        }
        chunks.push(current);
        let count = chunks.len();
        Ok(chunks
            .into_iter()
            .enumerate()
            .map(|(index, chunk)| format!("{}{chunk}", Self::part_header(index + 1, count)))
            .collect())
    }
}

/// Composes one launch from a profile.
///
/// `prompt_sha256` is SHA-256 over the exact UTF-8 bytes of
/// `first_prompt_body`, the whole first USER message; each part carries the
/// SHA-256 of its own exact text. Hashes stay in the store and never enter
/// a prompt.
///
/// The launch's effective leave-outs are its profile's when it names them,
/// else `default`, its metaflow's configured default; a set its harness
/// refuses refuses the launch.
pub trait ComposesLaunch {
    /// A launch whose metaflow names no default leave-outs.
    fn compose(&self, profile: &LaunchProfile) -> Result<ComposedLaunch, CompositionError> {
        self.compose_leaving_out(profile, Vec::new())
    }
    fn compose_leaving_out(
        &self,
        profile: &LaunchProfile,
        default: Vec<LeaveOut>,
    ) -> Result<ComposedLaunch, CompositionError>;
    /// A resumed flow's launch: its original profile with its system prompt
    /// composed again, carrying `text` as its one part. A resumed flow is
    /// not observed as a launch; the hash only names the text.
    fn resumption(
        &self,
        profile: &LaunchProfile,
        default: Vec<LeaveOut>,
        text: &str,
    ) -> Result<ComposedLaunch, CompositionError>;
}

/// The leave-outs a launch takes: its own, else the default, checked against
/// its harness.
trait ResolvesLeaveOuts {
    fn effective_leave_outs(
        &self,
        default: Vec<LeaveOut>,
    ) -> Result<Vec<LeaveOut>, CompositionError>;
}

impl ResolvesLeaveOuts for LaunchProfile {
    fn effective_leave_outs(
        &self,
        default: Vec<LeaveOut>,
    ) -> Result<Vec<LeaveOut>, CompositionError> {
        let leave_outs = self.leave_outs_option.clone().unwrap_or(default);
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
            let header = HarnessKind::part_header(index + 1, parts.len());
            let Some(chunk) = part.prompt_part_text.strip_prefix(&header) else {
                return false;
            };
            body.push_str(chunk);
        }
        body == payload.first_prompt_body
    }
}

/// Names how a launched Flow stays remotely controllable. A Claude launch
/// passes `--remote-control <name>` with this name; a Codex launch is already
/// driven through its app-server endpoint. The launch body records the same
/// line.
///
/// The name is unique per Flow. The Flow ID is claimed from the native
/// session only after the harness has started, and the flag must be given at
/// start, so the name is `flow-` followed by the launch request ID's short
/// form: the first sixteen hex digits of its SHA-256. Sixteen digits (64
/// bits) keep two launch requests from sharing a name, where eight digits
/// already collide between `launch-4646` and `launch-72333`; they also keep
/// it distinct from a six-digit Flow ID, and the request ID itself never
/// enters the prompt.
pub trait NamesRemoteControl {
    fn remote_control_name(&self) -> String;
    fn remote_control_record(&self) -> String;
}

/// The launch request ID's short form, used where a per-Flow name is needed
/// before the Flow ID is claimed.
pub trait ShortensLaunchRequest {
    const SHORT_FORM_LENGTH: usize = 16;
    fn launch_request_short_form(&self) -> String;
}

/// The launch request ID itself.
impl ShortensLaunchRequest for str {
    fn launch_request_short_form(&self) -> String {
        let digest = format!(
            "{:x}",
            Sha256::digest(format!("flow-remote-control-v1\0{self}").as_bytes())
        );
        digest[..Self::SHORT_FORM_LENGTH].to_owned()
    }
}

impl ShortensLaunchRequest for LaunchProfile {
    fn launch_request_short_form(&self) -> String {
        self.launch_request_id.launch_request_short_form()
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
        match self.power_level {
            PowerLevel::High => "High",
            PowerLevel::Medium => "Medium",
            PowerLevel::Low => "Low",
            PowerLevel::UltraLow => "Ultra Low",
        }
    }
}

impl NamesRemoteControl for LaunchProfile {
    fn remote_control_name(&self) -> String {
        format!("flow-{}", self.launch_request_short_form())
    }

    fn remote_control_record(&self) -> String {
        match self.harness_kind {
            HarnessKind::Claude => format!("--remote-control {}", self.remote_control_name()),
            HarnessKind::Codex => "app-server endpoint".into(),
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

impl RendersBrief for LaunchProfile {
    fn brief(&self) -> String {
        let profile = self;
        if profile.harness_kind == HarnessKind::Claude {
            return profile.instruction_prompt.clone();
        }
        format!(
            "# Flow launch\n\nRole: {} {}\nHarness: Codex\nModel: {}\nEffort: {}\nHerdr session: {}\nRemote control: {}\n\n{}",
            profile.aspect_name(),
            profile.power_name(),
            profile.model_name,
            profile.effort,
            profile.herdr_session_name,
            profile.remote_control_record(),
            profile.instruction_prompt,
        )
    }
}

trait ComposesVerifiedContext {
    /// The profile's context, composed and verified, and the system prompt
    /// it gives the launch.
    fn verified_context(
        &self,
        profile: &LaunchProfile,
    ) -> Result<(VerifiedContext, SystemPrompt), CompositionError>;
}

impl ComposesVerifiedContext for LaunchComposer {
    fn verified_context(
        &self,
        profile: &LaunchProfile,
    ) -> Result<(VerifiedContext, SystemPrompt), CompositionError> {
        profile.validate()?;
        let request = ContextRequest {
            address: profile.address.clone(),
            harness_kind: profile.harness_kind.clone(),
            system_modules: profile.system_modules.clone(),
            user_modules: profile.user_modules.clone(),
        };
        let composition = self.context.compose_flow_context(&request)?;
        let verified = VerifiedContext::verified(&request, composition)?;
        if verified.address() != profile.address {
            return Err(ContextRefusal::MetaflowMismatch.into());
        }
        let mut system_prompt_text = verified.system_text();
        let section = profile.launch_section();
        if !section.is_empty() {
            if !system_prompt_text.is_empty() && !system_prompt_text.ends_with('\n') {
                system_prompt_text.push('\n');
            }
            system_prompt_text.push('\n');
            system_prompt_text.push_str(&section);
        }
        if !profile.harness_kind.fits_one_argument(&system_prompt_text) {
            return Err(ContextRefusal::SystemTooLarge.into());
        }
        let system_prompt_sha256 = format!("{:x}", Sha256::digest(system_prompt_text.as_bytes()));
        Ok((
            verified,
            SystemPrompt {
                system_prompt_text,
                system_prompt_sha256,
            },
        ))
    }
}

trait AssemblesComposedLaunch {
    fn assembled(
        profile: &LaunchProfile,
        system_prompt: SystemPrompt,
        body: String,
        parts: Vec<String>,
        leave_outs: Vec<LeaveOut>,
    ) -> ComposedLaunch;
}

impl AssemblesComposedLaunch for LaunchComposer {
    fn assembled(
        profile: &LaunchProfile,
        system_prompt: SystemPrompt,
        body: String,
        parts: Vec<String>,
        leave_outs: Vec<LeaveOut>,
    ) -> ComposedLaunch {
        let digest = |text: &str| format!("{:x}", Sha256::digest(text.as_bytes()));
        let prompt_sha256 = digest(&body);
        ComposedLaunch {
            launch_profile: profile.clone(),
            system_prompt,
            first_prompt_payload: FirstPromptPayload {
                first_prompt_body: body,
                prompt_sha256: prompt_sha256.clone(),
                prompt_part_vector: parts
                    .into_iter()
                    .map(|text| PromptPart {
                        prompt_sha256: digest(&text),
                        prompt_part_text: text,
                    })
                    .collect(),
            },
            target_receipt_request: TargetReceiptRequest {
                launch_request_id: profile.launch_request_id.clone(),
                prompt_sha256,
            },
            leave_outs,
        }
    }
}

impl ComposesLaunch for LaunchComposer {
    fn compose_leaving_out(
        &self,
        profile: &LaunchProfile,
        default: Vec<LeaveOut>,
    ) -> Result<ComposedLaunch, CompositionError> {
        let leave_outs = profile.effective_leave_outs(default)?;
        let (verified, system_prompt) = self.verified_context(profile)?;
        let brief = profile.brief();
        let segments = verified.user_segments();
        let body = segments.joined_with(&brief).concat();
        let parts = profile.harness_kind.startup_parts(&segments, &brief)?;
        Ok(Self::assembled(
            profile,
            system_prompt,
            body,
            parts,
            leave_outs,
        ))
    }

    fn resumption(
        &self,
        profile: &LaunchProfile,
        default: Vec<LeaveOut>,
        text: &str,
    ) -> Result<ComposedLaunch, CompositionError> {
        let leave_outs = profile.effective_leave_outs(default)?;
        let (_, system_prompt) = self.verified_context(profile)?;
        Ok(Self::assembled(
            profile,
            system_prompt,
            text.to_owned(),
            vec![text.to_owned()],
            leave_outs,
        ))
    }
}

/// The text a metaflow's queued requests become in a flow's prompt: one
/// opening line, then each Request as its own datom, oldest first, each on
/// a line of its own. A Request is typed as the datom it is, as a Message
/// is; a control character other than LF and TAB is a key to a harness, not
/// a word, and is typed as U+FFFD.
pub trait RendersQueuedWork {
    const QUEUED_WORK_OPENING: &'static str =
        "Work queued for this metaflow while no flow of it was running, oldest first:";
    fn prompt_text(&self) -> String;
}

impl RendersQueuedWork for [Request] {
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
/// layer's Model and the harness that serves it, the module identities the
/// last launch named, the Herdr session its last flow ran in, and the
/// queued work.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FreshSeat {
    pub address: Address,
    pub launch_request_id: String,
    pub model_name: String,
    pub harness_kind: HarnessKind,
    pub power_level: PowerLevel,
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
            power_level: self.power_level.clone(),
            harness_kind: self.harness_kind.clone(),
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
        ComposesLaunch, CompositionError, FitsOneMessage, LaunchComposer, NamesRemoteControl,
        OpensLaunchComposer, ShortensLaunchRequest, SplitsStartupMessage, ValidatesComposedPrompt,
    };
    use crate::context::FixtureComposer;
    use signal_flow::{
        Address, Aspect, ContextRefusal, HarnessKind, LaunchProfile, Layer, PowerLevel,
        RememberedFlow,
    };
    use std::sync::Arc;

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
            power_level: PowerLevel::High,
            harness_kind: HarnessKind::Codex,
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
        LaunchComposer::with_context(Arc::new(FixtureComposer::from(
            &[
                ("spirit-core", "Spirit body."),
                ("intent-flow", "Intent body."),
                ("operation-main-flow", "Main flow body."),
                ("knowledge-flow", "Knowledge body."),
            ][..],
        )))
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
            LaunchComposer::with_context(Arc::new(FixtureComposer::from(&held[..]))),
            names,
        )
    }

    #[test]
    fn a_claude_message_is_the_user_context_then_the_brief_in_one_part() {
        let mut profile = fixture_profile();
        profile.harness_kind = HarnessKind::Claude;
        let composed = fixture_composer().compose(&profile).unwrap();
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
        let composed = fixture_composer().compose(&fixture_profile()).unwrap();
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
        let composed = fixture_composer().compose(&fixture_profile()).unwrap();
        let body = &composed.first_prompt_payload.first_prompt_body;
        assert!(body.starts_with("These are your skills"));
        assert!(body.ends_with(concat!(
            "<!-- end-module: knowledge-flow -->\n\n# Flow launch\n\n",
            "Role: Field High\nHarness: Codex\nModel: gpt-6-astra\nEffort: medium\n",
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
            profile.harness_kind = harness.clone();
            let composed = composer.compose(&profile).unwrap();
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
            composer.compose(&profile),
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
            composer.compose(&profile),
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
                composer.compose(&profile),
                Err(CompositionError::InvalidProfileField(field))
            );
        }
    }

    #[test]
    fn an_identity_the_composer_rejects_refuses_the_launch() {
        let mut profile = fixture_profile();
        profile.user_modules.push("unknown-module".into());
        assert_eq!(
            fixture_composer().compose(&profile),
            Err(CompositionError::Context(ContextRefusal::ComposerRejected))
        );
    }

    #[test]
    fn a_tampered_part_is_not_canonical() {
        let size = 400_000;
        let (composer, names) = large(&[size, size, size]);
        let mut profile = fixture_profile();
        profile.user_modules = names;
        let mut composed = composer.compose(&profile).unwrap();
        assert!(composed.has_canonical_first_prompt());
        composed.first_prompt_payload.prompt_part_vector[1]
            .prompt_part_text
            .push('x');
        assert!(!composed.has_canonical_first_prompt());
    }

    #[test]
    fn a_resumption_composes_the_system_prompt_again_and_carries_the_text() {
        let mut profile = fixture_profile();
        profile.harness_kind = HarnessKind::Claude;
        let composer = fixture_composer();
        let launched = composer.compose(&profile).unwrap();
        let resumed = composer
            .resumption(&profile, Vec::new(), "Work queued.")
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
        use signal_flow::LeaveOut;
        let composer = fixture_composer();
        let mut profile = fixture_profile();
        profile.harness_kind = HarnessKind::Claude;
        let default = vec![LeaveOut::SkillTool, LeaveOut::WorkflowTool];
        // None takes the default.
        let composed = composer
            .compose_leaving_out(&profile, default.clone())
            .unwrap();
        assert_eq!(composed.leave_outs, default);
        // A Start's own vector replaces it, even an empty one.
        profile.leave_outs_option = Some(vec![LeaveOut::GitInstructions]);
        let composed = composer
            .compose_leaving_out(&profile, default.clone())
            .unwrap();
        assert_eq!(composed.leave_outs, [LeaveOut::GitInstructions]);
        profile.leave_outs_option = Some(Vec::new());
        assert!(
            composer
                .compose_leaving_out(&profile, default.clone())
                .unwrap()
                .leave_outs
                .is_empty()
        );
        // A set its harness refuses refuses the launch.
        profile.leave_outs_option = Some(vec![LeaveOut::ToolSearch]);
        assert_eq!(
            composer.compose_leaving_out(&profile, default),
            Err(CompositionError::LeaveOut(
                signal_flow::LeaveOutRefusal::Indispensable(LeaveOut::ToolSearch)
            ))
        );
    }

    #[test]
    fn the_part_header_names_its_place_and_never_a_seat() {
        assert_eq!(
            HarnessKind::part_header(3, 3),
            "Startup message, part 3 of 3, the last.\n\n"
        );
        assert!(HarnessKind::part_header(1, 3).contains("part 3 of 3"));
        assert!(!HarnessKind::part_header(1, 3).contains("seat"));
    }

    #[test]
    fn launches_with_distinct_request_ids_never_share_a_name() {
        // These two request IDs share the first eight hex digits of their
        // SHA-256 (0ed0b35c), so an eight-digit short form named both
        // launches `flow-0ed0b35c`.
        let mut first = fixture_profile();
        first.launch_request_id = "launch-4646".into();
        let mut second = first.clone();
        second.launch_request_id = "launch-72333".into();
        assert_eq!(
            first.launch_request_short_form()[..8],
            second.launch_request_short_form()[..8]
        );
        assert_ne!(first.remote_control_name(), second.remote_control_name());
        let mut names = std::collections::BTreeSet::new();
        for index in 0..20_000 {
            let mut profile = first.clone();
            profile.launch_request_id = format!("launch-{index}");
            assert!(names.insert(profile.remote_control_name()));
        }
    }
}
