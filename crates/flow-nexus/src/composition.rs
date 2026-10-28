//! Exact-byte composition of a launch: its system prompt and its first USER
//! message, from the module identities its profile names.
//!
//! The context is composed by the context composer and verified
//! ([`crate::context`]); nothing composed is written to disk. A start is one
//! model call: every module the launch names is in the system prompt or the
//! first USER message. The system prompt is the verified system text
//! (spirit and intent), followed after a blank line by the launch's identity
//! section (its Address, Flow ID state, native-binding state, predecessor
//! and remembering). The first USER message is the verified USER text, the
//! remembered flows' records, then the brief; when it does not fit one
//! message it is sent in as many numbered parts as it takes, one right
//! after another. A simplified flow keeps its USER modules in its own
//! system prompt.

use crate::context::{ComposesFlowContext, ContextComposer, VerifiedContext, VerifiesContext};
use crate::delivery::body::RendersNativeContent;
pub use crate::generated::starter::{
    AssemblesComposedLaunch, CarriesFlowId, ComposesLaunch, ComposesVerifiedContext,
    ContextRequest, FitsOneMessage, HarnessedProfile, JoinsContextAndBrief, LeanStart,
    LeaveOutListing, PartNumbering, Profiled, ProfilesFreshSeat, ReadsVerifiedContext,
    RendersBrief, RendersLeanStart, ResolvesLeaveOuts, SegmentListing, SplitsStartupMessage,
    SystemPrompted, Textual,
};
use datom_codec::Datomizable;
use protos::{Compactable, Protosizable};
use sha2::{Digest, Sha256};
use signal_flow::{
    Address, Aspect, ComposedLaunch, CompositionRefusal, ContextRefusal, FirstPromptPayload,
    HarnessKind, LaunchProfile, Layer, LeaveOut, LeaveOutRefusal, PromptPart, QueueEntry,
    SystemPrompt, TargetReceiptRequest,
};
use std::collections::HashSet;

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum CompositionError {
    #[error("launch profile field is invalid: {0:?}")]
    InvalidProfileField(CompositionRefusal),
    #[error("the launch's context was refused: {0:?}")]
    Context(ContextRefusal),
    #[error("the launch's leave-outs were refused: {0:?}")]
    LeaveOut(LeaveOutRefusal),
    #[error("the Psyche core presentation prompt is unavailable: {0}")]
    PresentationPromptUnavailable(String),
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

trait ComposesPresentationRole {
    fn presentation_prompt(&self, address: &Address) -> Result<Option<String>, CompositionError>;
    fn presentation_prompt_from(
        &self,
        address: &Address,
        path: Option<std::ffi::OsString>,
    ) -> Result<Option<String>, CompositionError>;
    fn read_presentation_prompt(
        &self,
        path: std::ffi::OsString,
    ) -> Result<String, CompositionError>;
    fn compose_system_prompt<N: Profiled, O: ReadsVerifiedContext, P: SegmentListing>(
        &self,
        profile: N,
        context: O,
        overflow: P,
        presentation_prompt: Option<&str>,
    ) -> SystemPrompt;
}

impl ComposesPresentationRole for LaunchComposer {
    fn presentation_prompt(&self, address: &Address) -> Result<Option<String>, CompositionError> {
        self.presentation_prompt_from(address, std::env::var_os("FLOW_CORE_PRESENTATION_PROMPT"))
    }

    fn presentation_prompt_from(
        &self,
        address: &Address,
        path: Option<std::ffi::OsString>,
    ) -> Result<Option<String>, CompositionError> {
        if address.aspect != Aspect::Psyche
            || address.topic != "core"
            || address.layer != Layer::Primary
        {
            return Ok(None);
        }
        let path = path.ok_or_else(|| {
            CompositionError::PresentationPromptUnavailable(
                "FLOW_CORE_PRESENTATION_PROMPT is not set".into(),
            )
        })?;
        self.read_presentation_prompt(path).map(Some)
    }

    fn read_presentation_prompt(
        &self,
        path: std::ffi::OsString,
    ) -> Result<String, CompositionError> {
        let bytes = std::fs::read(&path).map_err(|error| {
            CompositionError::PresentationPromptUnavailable(format!(
                "could not read {}: {error}",
                std::path::PathBuf::from(&path).display()
            ))
        })?;
        if bytes.is_empty() {
            return Err(CompositionError::PresentationPromptUnavailable(format!(
                "{} is empty",
                std::path::PathBuf::from(&path).display()
            )));
        }
        String::from_utf8(bytes)
            .map_err(|error| CompositionError::PresentationPromptUnavailable(error.to_string()))
    }

    fn compose_system_prompt<N: Profiled, O: ReadsVerifiedContext, P: SegmentListing>(
        &self,
        input_0: N,
        input_1: O,
        input_2: P,
        presentation_prompt: Option<&str>,
    ) -> SystemPrompt {
        let mut text = input_1.system_text();
        for segment in input_2.segments() {
            if !text.is_empty() && !text.ends_with('\n') {
                text.push('\n');
            }
            text.push_str(&segment);
        }
        let section = input_0.profile().launch_section(None);
        if !section.is_empty() {
            if !text.is_empty() && !text.ends_with('\n') {
                text.push('\n');
            }
            text.push('\n');
            text.push_str(&section);
        }
        if let Some(prompt) = presentation_prompt {
            if !text.is_empty() && !text.ends_with('\n') {
                text.push('\n');
            }
            text.push('\n');
            text.push_str(prompt);
        }
        let system_prompt_sha256 = format!("{:x}", Sha256::digest(text.as_bytes()));
        SystemPrompt {
            system_prompt_text: text,
            system_prompt_sha256,
        }
    }
}

pub trait OpensLaunchComposer {
    fn with_context(context: ContextComposer) -> Self;
}

impl OpensLaunchComposer for LaunchComposer {
    fn with_context(context: ContextComposer) -> Self {
        Self { context }
    }
}

/// The launch's common identity section: the exact Address datom, a truthful
/// Flow ID state, and a distinct native-binding state, followed by any
/// predecessor or remembered-flow facts.
pub trait RendersLaunchSection {
    fn launch_section(&self, flow_id: Option<&str>) -> String;
}

impl RendersLaunchSection for LaunchProfile {
    fn launch_section(&self, flow_id: Option<&str>) -> String {
        let address = self.address.datomize(Vec::new()).protosize().compact();
        let mut section = format!(
            "Metaflow: {address}\nModel: {}\nEffort: {}\nFlow ID: {}\nNative binding: pending\n",
            self.model_name,
            self.effort,
            flow_id.unwrap_or("pending"),
        );
        if let Some(predecessor) = &self.flow_id_option {
            section.push_str(&format!("Predecessor: {predecessor}\n"));
        }
        match &self.remembering {
            signal_flow::Remembering::Predecessors(0) => {}
            signal_flow::Remembering::Predecessors(depth) => {
                section.push_str(&format!("Remembering: predecessors, {depth} deep\n"));
            }
            signal_flow::Remembering::Custom(flows) => {
                section.push_str(&format!("Remembering: {}\n", flows.join(", ")));
            }
        }
        section
    }
}

pub(crate) const HERDR_REQUEST_LINE_LIMIT: usize = 1024 * 1024;
const HERDR_ENVELOPE_ALLOWANCE: usize = 1024;
const CODEX_TEXT_CHARACTER_LIMIT: usize = 1 << 20;

/// What one message and one argument may hold for a harness.
///
/// A Claude part is typed by Herdr's `agent.prompt`, whose JSON request line
/// Herdr 0.8.2 reads up to 1 MiB (`MAX_INITIAL_REQUEST_BYTES`); a part fits
/// when its JSON-encoded text with the request envelope stays under that.
/// A Codex part is one text item of `turn/start`, which the Codex
/// app-server refuses over `MAX_USER_INPUT_TEXT_CHARS` (1 << 20)
/// characters. The system prompt travels in no message and no argument:
/// Claude reads it from the Nexus's in-memory file and a Codex thread is
/// given it as its base instructions.
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
            return Err(CompositionError::InvalidProfileField(
                CompositionRefusal::InvalidLaunchRequestId,
            ));
        }
        if profile.model_name.is_empty() || profile.model_name.contains(['\r', '\n']) {
            return Err(CompositionError::InvalidProfileField(
                CompositionRefusal::InvalidModelName,
            ));
        }
        if profile.effort.is_empty() || profile.effort.contains(['\r', '\n']) {
            return Err(CompositionError::InvalidProfileField(
                CompositionRefusal::InvalidEffort,
            ));
        }
        if profile.herdr_session_name.is_empty()
            || profile.herdr_session_name.contains(['\r', '\n'])
        {
            return Err(CompositionError::InvalidProfileField(
                CompositionRefusal::InvalidHerdrSessionName,
            ));
        }
        let identities_valid = |identities: &[String]| {
            let mut seen = HashSet::new();
            // An identity is `type:subject`, or a hyphenated name not yet
            // in that form: lowercase letters, digits and hyphens on each
            // side of at most one colon, neither side empty.
            let part = |part: &str| {
                !part.is_empty()
                    && part.bytes().all(|byte| {
                        byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-'
                    })
            };
            identities.iter().all(|identity| {
                let well_formed = match identity.split_once(':') {
                    Some((kind, subject)) => part(kind) && part(subject),
                    None => part(identity),
                };
                well_formed && seen.insert(identity)
            })
        };
        if profile.system_modules.is_empty() || !identities_valid(&profile.system_modules) {
            return Err(CompositionError::InvalidProfileField(
                CompositionRefusal::InvalidSystemModules,
            ));
        }
        if !identities_valid(&profile.user_modules)
            || profile
                .user_modules
                .iter()
                .any(|identity| profile.system_modules.contains(identity))
        {
            return Err(CompositionError::InvalidProfileField(
                CompositionRefusal::InvalidUserModules,
            ));
        }
        let remembering_valid = match &profile.remembering {
            signal_flow::Remembering::Predecessors(depth) => *depth >= 0,
            signal_flow::Remembering::Custom(flows) => {
                !flows.is_empty() && flows.iter().all(|flow| !flow.is_empty())
            }
        };
        if !remembering_valid {
            return Err(CompositionError::InvalidProfileField(
                CompositionRefusal::InvalidRemembering,
            ));
        }
        Ok(())
    }
}

/// The brief that follows the context: the instruction itself for Claude,
/// under a launch record for Codex, ending with the presentation line, since
/// a first turn ends in a presentation.
impl RendersBrief for HarnessedProfile {
    fn brief(&self) -> String {
        let profile = &self.launch_profile;
        let instruction = profile.instruction_prompt.clone();
        if self.harness_kind == HarnessKind::Claude {
            return instruction;
        }
        format!(
            "# Flow launch\n\nHarness: Codex\nHerdr session: {}\nRemote control: app-server endpoint\n\n{}",
            profile.herdr_session_name, instruction,
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
            tool_set: self.launch_profile.tool_set.clone(),
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
        // A module is placed by its kind: spirit and intent in the system
        // prompt, every other kind in the first message, whichever list of
        // the launch names it; each keeps its order.
        let (system_modules, user_modules): (Vec<String>, Vec<String>) = profile
            .system_modules
            .iter()
            .chain(profile.user_modules.iter())
            .cloned()
            .partition(|identity| identity.as_str().placed_in_system_prompt());
        let request = ContextRequest {
            address: profile.address.clone(),
            harness_kind: input.harness_kind(),
            system_modules,
            user_modules,
            remembered_flows: input.remembered_flows(),
        };
        let composition = self.context.compose_flow_context(&request)?;
        let verified = VerifiedContext::verified(&request, composition)?;
        if verified.address() != profile.address {
            return Err(ContextRefusal::MetaflowMismatch.into());
        }
        Ok(verified)
    }

    fn launch_system_prompt<N: Profiled, O: ReadsVerifiedContext, P: SegmentListing>(
        &self,
        input_0: N,
        input_1: O,
        input_2: P,
    ) -> Result<SystemPrompt, CompositionError> {
        let presentation_prompt = self.presentation_prompt(&input_0.profile().address)?;
        Ok(self.compose_system_prompt(input_0, input_1, input_2, presentation_prompt.as_deref()))
    }
}

/// Carries the known Flow ID as a fact distinct from the Address and native
/// binding. A launch whose harness has not yet bound retains `pending` for
/// its native binding. Only the system prompt and its hash change.
impl CarriesFlowId for ComposedLaunch {
    fn carrying_flow_id<N: Textual>(&self, input: N) -> ComposedLaunch {
        let previous_section = self.launch_profile.launch_section(None);
        let section = self
            .launch_profile
            .launch_section(Some(input.text().as_str()));
        let mut text = self.system_prompt.system_prompt_text.clone();
        match text.rfind(previous_section.as_str()) {
            Some(start) => {
                text.replace_range(start..start + previous_section.len(), &section);
            }
            None => {
                if !text.is_empty() && !text.ends_with('\n') {
                    text.push('\n');
                }
                text.push('\n');
                text.push_str(&section);
            }
        }
        let mut launch = self.clone();
        launch.system_prompt = SystemPrompt {
            system_prompt_sha256: format!("{:x}", Sha256::digest(text.as_bytes())),
            system_prompt_text: text,
        };
        launch
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
/// launch.
impl ComposesLaunch for LaunchComposer {
    fn compose<N: Profiled, O: LeaveOutListing>(
        &self,
        input_0: N,
        input_1: O,
    ) -> Result<ComposedLaunch, CompositionError> {
        let harnessed = HarnessedProfile {
            remembered_flows: input_0.remembered_flows(),
            launch_profile: input_0.profile(),
            harness_kind: input_0.harness_kind(),
        };
        let leave_outs = harnessed.effective_leave_outs(input_1)?;
        let verified = self.verified_context(&harnessed)?;
        let brief = if harnessed.launch_profile.flow_kind == signal_flow::FlowKind::Simplified {
            harnessed.launch_profile.instruction_prompt.clone()
        } else {
            harnessed.brief()
        };
        let mut segments = verified.user_segments();
        // The raw records of the flows the launch remembers follow its
        // USER modules, as one segment, ahead of the brief.
        let records = verified.records_text();
        if !records.is_empty() {
            segments.push(records);
        }
        if harnessed.launch_profile.flow_kind == signal_flow::FlowKind::Simplified {
            // A simplified flow must not receive a context-only startup turn:
            // its first user message belongs to the first queued request.
            // Keep every declared user module, but compose it into the system
            // prompt before the literal presentation role. A real initial
            // instruction remains the first user message; an empty one does
            // not create a context-only startup turn.
            let system_prompt = self.launch_system_prompt(&harnessed, &verified, &segments)?;
            let parts = if brief.is_empty() {
                Vec::new()
            } else {
                harnessed
                    .harness_kind
                    .startup_parts(Vec::<String>::new(), &brief)?
            };
            return Ok(Self::assembled(
                &harnessed,
                &system_prompt,
                &brief,
                &parts,
                &leave_outs,
            ));
        }
        // The system prompt holds spirit and intent alone. The rest of the
        // context and the brief are the first message, split into as many
        // prompts as it takes when it does not fit one, sent one right after
        // another, so the start is one model call.
        let system_prompt =
            self.launch_system_prompt(&harnessed, &verified, Vec::<String>::new())?;
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
}

/// The recipient-facing content a metaflow's queued requests become in a
/// flow's prompt, oldest first. Priority and sender accounting stay in the
/// durable queue and never enter native user text.
pub trait RendersQueuedWork {
    fn prompt_text(&self) -> String;
}

impl RendersQueuedWork for [QueueEntry] {
    fn prompt_text(&self) -> String {
        let mut text = String::new();
        for (index, request) in self.iter().enumerate() {
            if index > 0 {
                text.push('\n');
            }
            text.push_str(&request.message.native_content());
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
    pub flow_kind: signal_flow::FlowKind,
    pub tool_set: signal_flow::ToolSet,
    pub prompt: String,
}

/// Where a context module goes, by its kind: the type before the colon of
/// `type:subject`, or before the first hyphen of a name not yet in that
/// form. Spirit and intent sit in the system prompt; every other kind
/// (vision, knowledge, compensation, inquiry, operation, …) goes to the
/// first message with the raw records of the flows the launch remembers.
pub(crate) trait PlacesModuleByKind {
    fn placed_in_system_prompt(&self) -> bool;
}

impl PlacesModuleByKind for str {
    fn placed_in_system_prompt(&self) -> bool {
        let kind = match self.split_once(':') {
            Some((kind, _)) => kind,
            None => self.split('-').next().unwrap_or(self),
        };
        matches!(kind, "spirit" | "intent")
    }
}

/// A brief joined with the purpose its metaflow's declaration names: the
/// purpose first, then the brief; either alone when the other is empty.
pub(crate) trait JoinsPurpose {
    fn with_purpose(&self, purpose: &str) -> String;
}

impl JoinsPurpose for str {
    fn with_purpose(&self, purpose: &str) -> String {
        match (purpose.is_empty(), self.is_empty()) {
            (true, _) => self.to_owned(),
            (false, true) => purpose.to_owned(),
            (false, false) => format!("{purpose}\n\n{self}"),
        }
    }
}

/// The launch profile of a fresh wake: the queued work is its brief, and
/// its context is the module identities the metaflow's last launch named,
/// composed again through the same route as any Start. It remembers its
/// predecessor: the raw records of the metaflow's last flow.
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
            remembering: signal_flow::Remembering::Predecessors(1),
            herdr_session_name: self.herdr_session_name.clone(),
            leave_outs_option: self.leave_outs_option.clone(),
            flow_kind: self.flow_kind.clone(),
            tool_set: self.tool_set.clone(),
            instruction_prompt: self.prompt.clone(),
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::{
        ComposesLaunch, ComposesPresentationRole, ComposesVerifiedContext, CompositionError,
        FitsOneMessage, HarnessedProfile, LaunchComposer, OpensLaunchComposer,
        ReadsVerifiedContext, SplitsStartupMessage,
    };
    use crate::context::FixtureComposer;
    use signal_flow::{
        Address, Aspect, CompositionRefusal, ContextRefusal, HarnessKind, LaunchProfile, Layer,
        LeaveOut, Remembering,
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
            remembering: Remembering::Custom(vec!["836818".into()]),
            herdr_session_name: "messaging-build".into(),
            leave_outs_option: None,
            flow_kind: signal_flow::FlowKind::Regular,
            tool_set: signal_flow::ToolSet::Default,
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

    /// The raw records of the flows a launch remembers are composed into its
    /// first message after its USER modules and before its brief, and the
    /// composer is asked for exactly those flows; a launch remembering none
    /// carries none.
    #[test]
    fn remembered_flows_raw_records_follow_the_user_modules_in_the_first_message() {
        let mut context = FixtureComposer::from(
            &[
                ("spirit-core", "Spirit body."),
                ("intent-flow", "Intent body."),
                ("operation-main-flow", "Main flow body."),
                ("knowledge-flow", "Knowledge body."),
            ][..],
        );
        context.records.insert(
            "36b71b".into(),
            "## 2026-10-23, living, direct\n\"You can't leave me without a primary.\"\n".into(),
        );
        context
            .records
            .insert("e3c8c1".into(), "## Notion\nA crazy idea.\n".into());
        let composer =
            LaunchComposer::with_context(crate::context::ContextComposer::Fixture(context));
        let composed = composer
            .compose(
                HarnessedProfile {
                    remembered_flows: vec!["36b71b".into(), "e3c8c1".into()],
                    harness_kind: HarnessKind::Claude,
                    launch_profile: fixture_profile(),
                },
                Vec::<LeaveOut>::new(),
            )
            .unwrap();
        let body = &composed.first_prompt_payload.first_prompt_body;
        let user = body.find("Knowledge body.").expect("USER modules");
        let first = body
            .find("You can't leave me without a primary.")
            .expect("the first remembered flow's records");
        let second = body.find("A crazy idea.").expect("the second's records");
        let brief = body.find("Carry the bounded task.").expect("the brief");
        assert!(user < first && first < second && second < brief, "{body}");
        let forgetting = composer
            .compose(
                HarnessedProfile {
                    remembered_flows: Vec::new(),
                    harness_kind: HarnessKind::Claude,
                    launch_profile: fixture_profile(),
                },
                Vec::<LeaveOut>::new(),
            )
            .unwrap();
        assert!(
            !forgetting
                .first_prompt_payload
                .first_prompt_body
                .contains("A crazy idea.")
        );
    }

    /// A declaration's purpose goes ahead of the brief; either stands alone
    /// when the other is empty.
    #[test]
    fn a_purpose_is_composed_ahead_of_the_brief() {
        use super::JoinsPurpose;
        assert_eq!(
            "Draft the book.".with_purpose("Flow's design."),
            "Flow's design.\n\nDraft the book."
        );
        assert_eq!("Draft the book.".with_purpose(""), "Draft the book.");
        assert_eq!("".with_purpose("Flow's design."), "Flow's design.");
    }

    #[test]
    fn presentation_prompt_is_appended_verbatim_after_the_composed_system_prompt() {
        use sha2::{Digest, Sha256};
        let role = "Presentation-maker role\nsecond line\n";
        let composer = fixture_composer();
        let profile = HarnessedProfile {
            remembered_flows: Vec::new(),
            harness_kind: HarnessKind::Claude,
            launch_profile: fixture_profile(),
        };
        let verified = composer.verified_context(&profile).unwrap();
        let segments = verified.user_segments();
        let prompt = composer.compose_system_prompt(&profile, &verified, &segments, Some(role));
        assert!(prompt.system_prompt_text.ends_with(&format!("\n\n{role}")));
        assert_eq!(
            prompt.system_prompt_sha256,
            format!("{:x}", Sha256::digest(prompt.system_prompt_text.as_bytes()))
        );
    }

    #[test]
    fn presentation_prompt_file_read_preserves_bytes_and_fails_when_unreadable() {
        let path = std::env::temp_dir().join(format!(
            "flow-core-presentation-{}-{}.md",
            std::process::id(),
            std::thread::current().name().unwrap_or("test")
        ));
        let contents = b"Exact role line\nsecond line\n";
        std::fs::write(&path, contents).unwrap();
        assert_eq!(
            fixture_composer()
                .read_presentation_prompt(path.clone().into_os_string())
                .unwrap()
                .as_bytes(),
            contents
        );
        std::fs::remove_file(&path).unwrap();
        std::fs::write(&path, b"").unwrap();
        assert!(
            fixture_composer()
                .read_presentation_prompt(path.clone().into_os_string())
                .is_err()
        );
        std::fs::remove_file(&path).unwrap();
        let missing = path.with_extension("missing").into_os_string();
        assert!(
            fixture_composer()
                .read_presentation_prompt(missing)
                .is_err()
        );
    }

    #[test]
    fn other_addresses_do_not_read_the_presentation_prompt_environment() {
        let composer = fixture_composer();
        assert_eq!(
            composer
                .presentation_prompt_from(
                    &Address {
                        aspect: Aspect::Field,
                        topic: "core".into(),
                        layer: Layer::Primary,
                    },
                    None,
                )
                .unwrap(),
            None
        );
        assert!(
            composer
                .presentation_prompt_from(
                    &Address {
                        aspect: Aspect::Psyche,
                        topic: "core".into(),
                        layer: Layer::Primary,
                    },
                    None,
                )
                .is_err()
        );
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
                    remembered_flows: Vec::new(),
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
    }

    #[test]
    fn the_system_prompt_is_the_system_context_then_the_launch_section() {
        let composed = fixture_composer()
            .compose(
                crate::composition::HarnessedProfile {
                    remembered_flows: Vec::new(),
                    harness_kind: HarnessKind::Codex,
                    launch_profile: fixture_profile().clone(),
                },
                Vec::<signal_flow::LeaveOut>::new(),
            )
            .unwrap();
        let system = &composed.system_prompt.system_prompt_text;
        assert!(system.contains("<!-- module: spirit-core -->\nSpirit body.\n"));
        assert!(system.ends_with(
            "<!-- end-module: intent-flow -->\n\nMetaflow: { Field flow Secondary }\nModel: gpt-6-astra\nEffort: medium\nFlow ID: pending\nNative binding: pending\nPredecessor: 1b8ac0\nRemembering: 836818\n"
        ));
        assert!(!system.contains("Knowledge body."));
        let body = &composed.first_prompt_payload.first_prompt_body;
        assert!(!body.contains("Predecessor") && !body.contains("Remembered"));
    }

    #[test]
    fn a_launch_carries_its_flow_id_first_in_its_launch_section() {
        use super::CarriesFlowId;
        let composed = fixture_composer()
            .compose(
                crate::composition::HarnessedProfile {
                    remembered_flows: Vec::new(),
                    harness_kind: HarnessKind::Claude,
                    launch_profile: fixture_profile().clone(),
                },
                Vec::<signal_flow::LeaveOut>::new(),
            )
            .unwrap();
        let carried = composed.carrying_flow_id("a1b2c3");
        let system = &carried.system_prompt.system_prompt_text;
        assert!(system.ends_with(
            "<!-- end-module: intent-flow -->\n\nMetaflow: { Field flow Secondary }\nModel: gpt-6-astra\nEffort: medium\nFlow ID: a1b2c3\nNative binding: pending\nPredecessor: 1b8ac0\nRemembering: 836818\n"
        ));
        assert_eq!(carried.first_prompt_payload, composed.first_prompt_payload);
        // A launch with no predecessor and no memory gains the section.
        let mut profile = fixture_profile();
        profile.flow_id_option = None;
        profile.remembering = Remembering::Predecessors(0);
        let bare = fixture_composer()
            .compose(
                crate::composition::HarnessedProfile {
                    remembered_flows: Vec::new(),
                    harness_kind: HarnessKind::Claude,
                    launch_profile: profile,
                },
                Vec::<signal_flow::LeaveOut>::new(),
            )
            .unwrap()
            .carrying_flow_id("a1b2c3");
        assert!(
            bare.system_prompt
                .system_prompt_text
                .ends_with("<!-- end-module: intent-flow -->\n\nMetaflow: { Field flow Secondary }\nModel: gpt-6-astra\nEffort: medium\nFlow ID: a1b2c3\nNative binding: pending\n")
        );
        assert!(!system.contains("This flow belongs to metaflow"));
    }

    #[test]
    fn a_role_after_the_launch_section_stays_last_when_the_flow_id_arrives() {
        use super::CarriesFlowId;
        let mut composed = fixture_composer()
            .compose(
                crate::composition::HarnessedProfile {
                    remembered_flows: Vec::new(),
                    harness_kind: HarnessKind::Claude,
                    launch_profile: fixture_profile(),
                },
                Vec::<signal_flow::LeaveOut>::new(),
            )
            .unwrap();
        let role = "Presentation-maker role, unchanged\n";
        let composer = fixture_composer();
        let profile = HarnessedProfile {
            remembered_flows: Vec::new(),
            harness_kind: HarnessKind::Claude,
            launch_profile: composed.launch_profile.clone(),
        };
        let verified = composer.verified_context(&profile).unwrap();
        let segments = verified.user_segments();
        composed.system_prompt =
            composer.compose_system_prompt(&profile, &verified, &segments, Some(role));
        let carried = composed.carrying_flow_id("a1b2c3");
        use sha2::{Digest, Sha256};
        assert!(carried.system_prompt.system_prompt_text.ends_with(role));
        assert!(
            carried
                .system_prompt
                .system_prompt_text
                .contains("Flow ID: a1b2c3\n")
        );
        assert_eq!(
            carried.system_prompt.system_prompt_sha256,
            format!(
                "{:x}",
                Sha256::digest(carried.system_prompt.system_prompt_text.as_bytes())
            )
        );
    }

    #[test]
    fn a_codex_message_puts_its_launch_record_after_the_context() {
        let composed = fixture_composer()
            .compose(
                crate::composition::HarnessedProfile {
                    remembered_flows: Vec::new(),
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
            "Harness: Codex\n",
            "Herdr session: messaging-build\nRemote control: app-server endpoint\n\n",
            "Carry the bounded task."
        )));
        assert!(composed.system_prompt.system_prompt_text.contains(
            "Metaflow: { Field flow Secondary }\nModel: gpt-6-astra\nEffort: medium\nFlow ID: pending\nNative binding: pending\n"
        ));
        assert!(!body.contains("Role: Field Medium"));
        assert!(!body.contains('$'));
    }

    #[test]
    fn both_harnesses_receive_the_exact_typed_address_datoms() {
        for (aspect, aspect_name) in [
            (Aspect::Psyche, "Psyche"),
            (Aspect::Mind, "Mind"),
            (Aspect::Field, "Field"),
        ] {
            for (layer, layer_name) in [
                (Layer::Primary, "Primary"),
                (Layer::Secondary, "Secondary"),
                (Layer::Tertiary, "Tertiary"),
                (Layer::Quaternary, "Quaternary"),
            ] {
                for harness_kind in [HarnessKind::Claude, HarnessKind::Codex] {
                    let mut profile = fixture_profile();
                    profile.address = Address {
                        aspect: aspect.clone(),
                        topic: "coreTopic".into(),
                        layer: layer.clone(),
                    };
                    let composed = fixture_composer()
                        .compose(
                            HarnessedProfile {
                                remembered_flows: Vec::new(),
                                harness_kind,
                                launch_profile: profile,
                            },
                            Vec::<signal_flow::LeaveOut>::new(),
                        )
                        .unwrap();
                    let identity = format!(
                        "Metaflow: {{ {aspect_name} coreTopic {layer_name} }}\nModel: gpt-6-astra\nEffort: medium\nFlow ID: pending\nNative binding: pending\n"
                    );
                    assert!(
                        composed
                            .system_prompt
                            .system_prompt_text
                            .contains(&identity)
                    );
                    assert!(!composed.system_prompt.system_prompt_text.contains("Role:"));
                    assert!(
                        !composed
                            .first_prompt_payload
                            .first_prompt_body
                            .contains("Role:")
                    );
                }
            }
        }
    }

    /// A first message too large for one message is sent in as many parts
    /// as it takes, one right after another; no USER module joins the
    /// system prompt, which holds spirit and intent alone.
    #[test]
    fn a_message_too_large_for_one_message_is_sent_in_parts_and_never_moves_to_the_system_prompt() {
        let size = 400_000;
        let (composer, names) = large(&[size, size, size]);
        let mut profile = fixture_profile();
        profile.user_modules = names.clone();
        for harness in [HarnessKind::Claude, HarnessKind::Codex] {
            let composed = composer
                .compose(
                    crate::composition::HarnessedProfile {
                        remembered_flows: Vec::new(),
                        harness_kind: harness.clone(),
                        launch_profile: profile.clone(),
                    },
                    Vec::<signal_flow::LeaveOut>::new(),
                )
                .unwrap();
            let system = &composed.system_prompt.system_prompt_text;
            let parts = &composed.first_prompt_payload.prompt_part_vector;
            assert!(parts.len() > 1, "{harness:?}: sent in parts");
            for (index, part) in parts.iter().enumerate() {
                assert!(harness.fits_one_message(&part.prompt_part_text));
                assert!(part.prompt_part_text.starts_with(&format!(
                    "Startup message, part {} of {}",
                    index + 1,
                    parts.len()
                )));
            }
            assert!(
                parts
                    .last()
                    .unwrap()
                    .prompt_part_text
                    .ends_with("Carry the bounded task.")
            );
            for name in &names {
                let marker = format!("<!-- module: {name} -->");
                assert!(
                    !system.contains(&marker),
                    "{name} is not in the system prompt"
                );
                assert_eq!(
                    parts
                        .iter()
                        .filter(|part| part.prompt_part_text.contains(&marker))
                        .count(),
                    1,
                    "{name} is sent once"
                );
            }
        }
    }

    #[test]
    fn simplified_launch_keeps_user_modules_in_system_and_has_no_startup_turn() {
        // A simplified flow takes whatever tool set it is configured with,
        // the default included.
        let mut profile = fixture_profile();
        profile.flow_kind = signal_flow::FlowKind::Simplified;
        profile.tool_set = signal_flow::ToolSet::Default;
        profile.instruction_prompt.clear();
        let composed = fixture_composer()
            .compose(
                HarnessedProfile {
                    remembered_flows: Vec::new(),
                    harness_kind: HarnessKind::Claude,
                    launch_profile: profile,
                },
                Vec::<LeaveOut>::new(),
            )
            .unwrap();
        let system = &composed.system_prompt.system_prompt_text;
        assert!(system.contains("<!-- module: operation-main-flow -->"));
        assert!(system.contains("<!-- module: knowledge-flow -->"));
        assert!(composed.first_prompt_payload.first_prompt_body.is_empty());
        assert!(composed.first_prompt_payload.prompt_part_vector.is_empty());
    }

    #[test]
    fn simplified_launch_preserves_a_real_initial_instruction_as_the_first_user_message() {
        let mut profile = fixture_profile();
        profile.flow_kind = signal_flow::FlowKind::Simplified;
        profile.tool_set = signal_flow::ToolSet::Explicit(Vec::new());
        let composed = fixture_composer()
            .compose(
                HarnessedProfile {
                    remembered_flows: Vec::new(),
                    harness_kind: HarnessKind::Claude,
                    launch_profile: profile,
                },
                Vec::<LeaveOut>::new(),
            )
            .unwrap();
        assert_eq!(
            composed.first_prompt_payload.first_prompt_body,
            "Carry the bounded task."
        );
    }

    #[test]
    fn simplified_codex_launch_does_not_turn_an_empty_instruction_into_metadata() {
        let mut profile = fixture_profile();
        profile.flow_kind = signal_flow::FlowKind::Simplified;
        profile.tool_set = signal_flow::ToolSet::Explicit(Vec::new());
        profile.instruction_prompt.clear();
        let composed = fixture_composer()
            .compose(
                HarnessedProfile {
                    remembered_flows: Vec::new(),
                    harness_kind: HarnessKind::Codex,
                    launch_profile: profile,
                },
                Vec::<LeaveOut>::new(),
            )
            .unwrap();
        assert!(composed.first_prompt_payload.first_prompt_body.is_empty());
        assert!(composed.first_prompt_payload.prompt_part_vector.is_empty());
    }

    /// One USER module larger than one message is not cut and never moved
    /// into the system prompt: the launch is refused.
    #[test]
    fn a_module_too_large_for_one_message_refuses_the_launch() {
        let (composer, names) = large(&[1_100_000]);
        let mut profile = fixture_profile();
        profile.user_modules = names;
        assert_eq!(
            composer.compose(
                crate::composition::HarnessedProfile {
                    remembered_flows: Vec::new(),
                    harness_kind: HarnessKind::Codex,
                    launch_profile: profile.clone(),
                },
                Vec::<signal_flow::LeaveOut>::new(),
            ),
            Err(CompositionError::Context(ContextRefusal::ModuleTooLarge))
        );
    }

    #[test]
    fn a_brief_too_large_for_one_message_refuses_the_launch() {
        let mut profile = fixture_profile();
        profile.instruction_prompt = "x".repeat(1_100_000);
        assert_eq!(
            fixture_composer().compose(
                crate::composition::HarnessedProfile {
                    remembered_flows: Vec::new(),
                    harness_kind: HarnessKind::Codex,
                    launch_profile: profile.clone()
                },
                Vec::<signal_flow::LeaveOut>::new(),
            ),
            Err(CompositionError::Context(ContextRefusal::ModuleTooLarge))
        );
    }

    /// Spirit and intent modules go to the system prompt and every other
    /// kind to the first message, whichever list a declaration names them
    /// in: a fresh flow's first prompt carries the context modules relevant
    /// to it of any kind except spirit and intent.
    #[test]
    fn modules_are_placed_by_kind_spirit_and_intent_in_the_system_prompt() {
        let context = FixtureComposer::from(
            &[
                ("spirit:core", "Spirit body."),
                ("intent:flow", "Intent body."),
                ("vision:flow", "Vision body."),
                ("knowledge:flow", "Knowledge body."),
                ("compensation:truth", "Compensation body."),
                ("inquiry:flow", "Inquiry body."),
                ("spirit-legacy", "Legacy spirit body."),
                ("operation-main-flow", "Main flow body."),
            ][..],
        );
        let composer =
            LaunchComposer::with_context(crate::context::ContextComposer::Fixture(context));
        let mut profile = fixture_profile();
        profile.system_modules = vec![
            "spirit:core".into(),
            "vision:flow".into(),
            "spirit-legacy".into(),
        ];
        profile.user_modules = vec![
            "intent:flow".into(),
            "knowledge:flow".into(),
            "compensation:truth".into(),
            "inquiry:flow".into(),
            "operation-main-flow".into(),
        ];
        let composed = composer
            .compose(
                HarnessedProfile {
                    remembered_flows: Vec::new(),
                    harness_kind: HarnessKind::Claude,
                    launch_profile: profile,
                },
                Vec::<LeaveOut>::new(),
            )
            .unwrap();
        let system = &composed.system_prompt.system_prompt_text;
        let first = &composed.first_prompt_payload.first_prompt_body;
        for body in ["Spirit body.", "Intent body.", "Legacy spirit body."] {
            assert!(system.contains(body), "{body} in the system prompt");
            assert!(!first.contains(body), "{body} not in the first message");
        }
        for body in [
            "Vision body.",
            "Knowledge body.",
            "Compensation body.",
            "Inquiry body.",
            "Main flow body.",
        ] {
            assert!(first.contains(body), "{body} in the first message");
            assert!(!system.contains(body), "{body} not in the system prompt");
        }
    }

    #[test]
    fn a_system_prompt_past_one_argument_is_composed_whole() {
        let large_intent = "b".repeat(140_000);
        let composer = LaunchComposer::with_context(crate::context::ContextComposer::Fixture(
            FixtureComposer::from(
                &[
                    ("spirit-core", "Spirit body."),
                    ("intent-large", large_intent.as_str()),
                    ("knowledge-small", "Knowledge body."),
                ][..],
            ),
        ));
        let mut profile = fixture_profile();
        profile.system_modules = vec!["spirit-core".into(), "intent-large".into()];
        profile.user_modules = vec!["knowledge-small".into()];
        let composed = composer
            .compose(
                crate::composition::HarnessedProfile {
                    remembered_flows: Vec::new(),
                    harness_kind: HarnessKind::Codex,
                    launch_profile: profile.clone(),
                },
                Vec::<signal_flow::LeaveOut>::new(),
            )
            .unwrap();
        assert!(composed.system_prompt.system_prompt_text.len() > 140_000);
    }

    #[test]
    fn native_queue_text_contains_only_the_recipient_facing_content() {
        use super::RendersQueuedWork;
        use signal_flow::{QueueEntry, Request, SentBy};
        let order = QueueEntry {
            logical_delivery_id: "logical-delivery-0000000000000001".into(),
            sent_by: SentBy::Flow,
            delivery: signal_flow::Delivery::Queue,
            message: signal_flow::Message::Soft(Request::Order("investigate".into())),
        };
        assert_eq!([order].prompt_text(), "investigate");
    }

    #[test]
    fn a_profile_naming_no_valid_identities_is_refused_before_composition() {
        let composer = fixture_composer();
        type ProfileChange = fn(&mut LaunchProfile);
        let cases: [(CompositionRefusal, ProfileChange); 7] = [
            (CompositionRefusal::InvalidSystemModules, |profile| {
                profile.system_modules.clear()
            }),
            (CompositionRefusal::InvalidSystemModules, |profile| {
                profile.system_modules.push("/abs/path.md".into())
            }),
            (CompositionRefusal::InvalidUserModules, |profile| {
                profile.user_modules.push("knowledge-flow".into())
            }),
            (CompositionRefusal::InvalidUserModules, |profile| {
                profile.user_modules.push("spirit-core".into())
            }),
            // A Remembering of no flows names the field it refuses.
            (CompositionRefusal::InvalidRemembering, |profile| {
                profile.remembering = Remembering::Custom(vec![])
            }),
            (CompositionRefusal::InvalidRemembering, |profile| {
                profile.remembering = Remembering::Custom(vec![String::new()])
            }),
            (CompositionRefusal::InvalidRemembering, |profile| {
                profile.remembering = Remembering::Predecessors(-1)
            }),
        ];
        for (field, change) in cases {
            let mut profile = fixture_profile();
            change(&mut profile);
            assert_eq!(
                composer.compose(
                    crate::composition::HarnessedProfile {
                        remembered_flows: Vec::new(),
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
    fn an_identity_the_composer_reports_omitted_does_not_refuse_the_launch() {
        let mut profile = fixture_profile();
        profile.user_modules.push("unknown-module".into());
        let composed = fixture_composer()
            .compose(
                crate::composition::HarnessedProfile {
                    remembered_flows: Vec::new(),
                    harness_kind: HarnessKind::Codex,
                    launch_profile: profile.clone(),
                },
                Vec::<signal_flow::LeaveOut>::new(),
            )
            .unwrap();
        assert_eq!(composed.launch_profile.user_modules, profile.user_modules);
        assert!(
            !composed
                .first_prompt_payload
                .first_prompt_body
                .contains("unknown-module")
        );
        assert!(
            composed
                .first_prompt_payload
                .first_prompt_body
                .contains("Knowledge body.")
        );
    }

    #[test]
    fn a_tampered_part_is_not_canonical() {
        let mut composed = fixture_composer()
            .compose(
                crate::composition::HarnessedProfile {
                    remembered_flows: Vec::new(),
                    harness_kind: HarnessKind::Codex,
                    launch_profile: fixture_profile(),
                },
                Vec::<signal_flow::LeaveOut>::new(),
            )
            .unwrap();
        composed.first_prompt_payload.prompt_part_vector[0]
            .prompt_part_text
            .push('x');
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
                    remembered_flows: Vec::new(),
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
                    remembered_flows: Vec::new(),
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
                        remembered_flows: Vec::new(),
                        harness_kind: harness.clone(),
                        launch_profile: profile.clone()
                    },
                    default.clone()
                )
                .unwrap()
                .leave_outs
                .is_empty()
        );
        // A Start may leave out ToolSearch while other tools remain.
        profile.leave_outs_option = Some(vec![LeaveOut::ToolSearch]);
        assert!(
            composer
                .compose(
                    crate::composition::HarnessedProfile {
                        remembered_flows: Vec::new(),
                        harness_kind: harness.clone(),
                        launch_profile: profile.clone()
                    },
                    default
                )
                .is_ok()
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
