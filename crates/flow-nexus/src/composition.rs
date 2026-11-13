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
//! remembered flows' records, then the brief; a successor's message then
//! holds its predecessor's handover and, after it, the requests its
//! permaflow still owes an answer. When it does not fit one message it is
//! sent in as many numbered parts as it takes, one right after another. A
//! simplified flow keeps its USER modules in its own system prompt.

use crate::context::{ComposesFlowContext, ContextComposer, VerifiedContext, VerifiesContext};
pub use crate::generated::starter::{
    AssemblesComposedLaunch, CarriesFlowId, ComposesLaunch, ComposesVerifiedContext,
    ContextRequest, FitsOneMessage, HarnessedProfile, JoinsContextAndBrief, LeanStart,
    LeaveOutListing, PartNumbering, Profiled, ProfilesFreshSeat, ReadsVerifiedContext,
    RendersBrief, RendersLeanStart, ResolvesLeaveOuts, SegmentListing, SplitsStartupMessage,
    SystemPrompted, Textual,
};
use crate::handover::{Handover, HandoverFile, YieldsHandover};
use datom_codec::Datomizable;
use protos::{Compactable, Protosizable};
use sha2::{Digest, Sha256};
use signal_flow::{
    Address, Aspect, ComposedLaunch, CompositionRefusal, ContextRefusal, FirstPromptPayload,
    HandoverFault, HarnessKind, LaunchProfile, LeaveOut, LeaveOutRefusal, Model, PromptPart,
    QueueEntry, SystemPrompt, TargetReceiptRequest,
};
use std::collections::HashSet;
use std::path::Path;
use std::sync::Arc;

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
    #[error("the handover of {predecessor} could not be read: {fault:?}")]
    HandoverUnreadable {
        predecessor: String,
        fault: HandoverFault,
    },
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
    presentation_prompt_option: Option<String>,
    handover: Arc<dyn YieldsHandover>,
}

trait ComposesPresentationRole {
    fn presentation_prompt(&self, address: &Address) -> Result<Option<String>, CompositionError>;
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
        if address.aspect != Aspect::Psyche
            || address.topic.0 != "core"
            || address.model != Model::Fable
        {
            return Ok(None);
        }
        Ok(self
            .presentation_prompt_option
            .as_ref()
            .filter(|prompt| !prompt.trim().is_empty())
            .cloned())
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
            system_prompt_text: signal_flow::SystemPromptText(text),
            system_prompt_sha256: signal_flow::SystemPromptSha256(system_prompt_sha256),
        }
    }
}

pub trait OpensLaunchComposer {
    fn with_context(context: ContextComposer) -> Self;
    fn with_presentation_prompt(
        context: ContextComposer,
        presentation_prompt_option: Option<String>,
    ) -> Self;
    /// The composer with another source of predecessors' handovers, in place
    /// of the files of [`HandoverFile`].
    fn with_handover_source(self, handover: Arc<dyn YieldsHandover>) -> Self;
}

impl OpensLaunchComposer for LaunchComposer {
    fn with_context(context: ContextComposer) -> Self {
        Self {
            context,
            presentation_prompt_option: None,
            handover: Arc::new(HandoverFile),
        }
    }

    fn with_presentation_prompt(
        context: ContextComposer,
        presentation_prompt_option: Option<String>,
    ) -> Self {
        Self {
            context,
            presentation_prompt_option,
            handover: Arc::new(HandoverFile),
        }
    }

    fn with_handover_source(mut self, handover: Arc<dyn YieldsHandover>) -> Self {
        self.handover = handover;
        self
    }
}

/// The root a launch's predecessor's handover is read under.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HandoverRoot<'a> {
    /// A composition with no root ([`ComposesLaunch::compose`]): no handover
    /// is read.
    Unread,
    /// The root the launch runs in.
    At(&'a Path),
    /// The launch's root could not be resolved: a launch given a handover
    /// is refused `RootUnresolved`.
    Unresolved,
}

trait ReadsHandover {
    fn handover_text(
        &self,
        profile: &LaunchProfile,
        launch_root: HandoverRoot<'_>,
    ) -> Result<Option<String>, CompositionError>;
}

impl ReadsHandover for LaunchComposer {
    /// The text of a successor's handover, placed after the brief in its
    /// first prompt: what the source yields for the launch's predecessor.
    /// None for a launch with no predecessor, for one composed with no
    /// root, and when the source holds none. A handover the source could
    /// not read whole, or a root that could not be resolved, refuses the
    /// launch `HandoverUnreadable`, naming the predecessor and the fault.
    fn handover_text(
        &self,
        profile: &LaunchProfile,
        launch_root: HandoverRoot<'_>,
    ) -> Result<Option<String>, CompositionError> {
        let Some(predecessor) = profile
            .flow_id_option
            .as_ref()
            .map(|value| value.0.as_str())
        else {
            return Ok(None);
        };
        let refused = |fault| CompositionError::HandoverUnreadable {
            predecessor: predecessor.to_owned(),
            fault,
        };
        let launch_root = match launch_root {
            HandoverRoot::Unread => return Ok(None),
            HandoverRoot::At(root) => root,
            HandoverRoot::Unresolved if self.handover.requires_launch_root() => {
                return Err(refused(HandoverFault::RootUnresolved));
            }
            HandoverRoot::Unresolved => Path::new(""),
        };
        match self
            .handover
            .handover(&profile.address, predecessor, launch_root)
        {
            Handover::Text(text) => Ok(Some(text)),
            Handover::Absent => Ok(None),
            Handover::Unreadable(fault) => Err(refused(fault)),
        }
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
            self.model_name.0,
            self.effort.0,
            flow_id.unwrap_or("pending"),
        );
        if let Some(predecessor) = &self.flow_id_option {
            section.push_str(&format!(
                "Predecessor: {predecessor}\n",
                predecessor = predecessor.0
            ));
        }
        match &self.remembering {
            signal_flow::Remembering::Predecessors(signal_flow::RememberingDepth(0)) => {}
            signal_flow::Remembering::Predecessors(depth) => {
                section.push_str(&format!(
                    "Remembering: predecessors, {depth} deep\n",
                    depth = depth.0
                ));
            }
            signal_flow::Remembering::Custom(flows) => {
                let flows: Vec<&str> = flows.iter().map(|flow| flow.0.as_str()).collect();
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
        self.parts_of(input_0.segments().joined_with(input_1.text()))
    }
}

/// A first USER message already cut into pieces, cut into parts.
trait CutsIntoParts {
    fn parts_of(&self, pieces: Vec<String>) -> Result<Vec<String>, ContextRefusal>;
}

impl CutsIntoParts for HarnessKind {
    fn parts_of(&self, pieces: Vec<String>) -> Result<Vec<String>, ContextRefusal> {
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
        if profile.launch_request_id.0.is_empty()
            || !profile.launch_request_id.0.bytes().all(|byte| {
                byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b':' | b'-')
            })
        {
            return Err(CompositionError::InvalidProfileField(
                CompositionRefusal::InvalidLaunchRequestId,
            ));
        }
        if profile.model_name.0.is_empty() || profile.model_name.0.contains(['\r', '\n']) {
            return Err(CompositionError::InvalidProfileField(
                CompositionRefusal::InvalidModelName,
            ));
        }
        if profile.effort.0.is_empty() || profile.effort.0.contains(['\r', '\n']) {
            return Err(CompositionError::InvalidProfileField(
                CompositionRefusal::InvalidEffort,
            ));
        }
        if profile.herdr_session_name.0.is_empty()
            || profile.herdr_session_name.0.contains(['\r', '\n'])
        {
            return Err(CompositionError::InvalidProfileField(
                CompositionRefusal::InvalidHerdrSessionName,
            ));
        }
        let identities_valid = |identities: &[signal_flow::ModuleIdentity]| {
            let mut seen = HashSet::new();
            // An identity names its kind, topic and explicit canonical decimal version.
            let part = |part: &str| {
                !part.is_empty()
                    && part.bytes().all(|byte| {
                        byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-'
                    })
            };
            identities.iter().all(|identity| {
                let fields = identity.0.split(':').collect::<Vec<_>>();
                let well_formed = matches!(fields.as_slice(), [kind, topic, version]
                    if part(kind) && part(topic)
                        && !version.is_empty()
                        && (*version == "0" || !version.starts_with('0'))
                        && version.bytes().all(|byte| byte.is_ascii_digit()));
                well_formed && seen.insert(identity)
            })
        };
        if profile.system_modules.0.is_empty() || !identities_valid(&profile.system_modules.0) {
            return Err(CompositionError::InvalidProfileField(
                CompositionRefusal::InvalidSystemModules,
            ));
        }
        if !identities_valid(&profile.user_modules.0)
            || profile
                .user_modules
                .0
                .iter()
                .any(|identity| profile.system_modules.0.contains(identity))
        {
            return Err(CompositionError::InvalidProfileField(
                CompositionRefusal::InvalidUserModules,
            ));
        }
        let remembering_valid = match &profile.remembering {
            signal_flow::Remembering::Predecessors(depth) => {
                *depth >= signal_flow::RememberingDepth(0)
            }
            signal_flow::Remembering::Custom(flows) => {
                !flows.is_empty() && flows.iter().all(|flow| !flow.0.is_empty())
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
            return instruction.0;
        }
        format!(
            "# Flow launch\n\nHarness: Codex\nHerdr session: {}\nRemote control: app-server endpoint\n\n{}",
            profile.herdr_session_name.0, instruction.0,
        )
    }
}

/// The leave-outs a launch takes: its own, else the default its metaflow
/// configures, checked against its harness.
impl ResolvesLeaveOuts for HarnessedProfile {
    fn effective_leave_outs<N: LeaveOutListing>(
        &self,
        input: N,
    ) -> std::result::Result<signal_flow::LeaveOuts, CompositionError> {
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
        let (system_modules, user_modules): (Vec<_>, Vec<_>) = profile
            .system_modules
            .0
            .iter()
            .chain(profile.user_modules.0.iter())
            .cloned()
            .partition(|identity| identity.0.as_str().placed_in_system_prompt());
        let request = ContextRequest {
            address: profile.address.clone(),
            harness_kind: input.harness_kind(),
            system_modules: signal_flow::SystemModules(system_modules),
            user_modules: signal_flow::UserModules(user_modules),
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
        match text.0.rfind(previous_section.as_str()) {
            Some(start) => {
                text.0
                    .replace_range(start..start + previous_section.len(), &section);
            }
            None => {
                if !text.0.is_empty() && !text.0.ends_with('\n') {
                    text.0.push('\n');
                }
                text.0.push('\n');
                text.0.push_str(&section);
            }
        }
        let mut launch = self.clone();
        launch.system_prompt = SystemPrompt {
            system_prompt_sha256: signal_flow::SystemPromptSha256(format!(
                "{:x}",
                Sha256::digest(text.0.as_bytes())
            )),
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
                prompt_sha256: signal_flow::PromptSha256(prompt_sha256.clone()),
            },
            launch_profile: profile,
            harness_kind: input_0.harness_kind(),
            system_prompt: input_1.system_prompt(),
            first_prompt_payload: FirstPromptPayload {
                first_prompt_body: signal_flow::FirstPromptBody(body),
                prompt_sha256: signal_flow::PromptSha256(prompt_sha256),
                prompt_part_vector: input_3
                    .segments()
                    .into_iter()
                    .map(|text| PromptPart {
                        prompt_sha256: signal_flow::PromptSha256(digest(&text)),
                        prompt_part_text: signal_flow::PromptPartText(text),
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
        self.compose_in_root(input_0, input_1, HandoverRoot::Unread, &[])
    }
}

/// A first USER message with a text placed last, whole, after a blank line
/// (a line end when the message already ends in one).
trait PlacesLast {
    fn placed_last(self, text: &str) -> Vec<String>;
}

impl PlacesLast for Vec<String> {
    fn placed_last(mut self, text: &str) -> Vec<String> {
        let so_far = self.concat();
        let separator = if so_far.is_empty() || so_far.ends_with("\n\n") {
            ""
        } else if so_far.ends_with('\n') {
            "\n"
        } else {
            "\n\n"
        };
        self.push(format!("{separator}{text}"));
        self
    }
}

/// Composes a launch that runs in a root.
pub trait ComposesInRoot {
    /// As [`ComposesLaunch::compose`], for a launch that runs in
    /// `launch_root`. A launch that names a predecessor (a refresh's or a
    /// waking's successor) is given the predecessor's handover, whole, in
    /// its first prompt after the brief, or is refused `HandoverUnreadable`
    /// when it cannot be read whole. A launch composed with
    /// [`HandoverRoot::Unread`] reads none; a Restart names no predecessor,
    /// has no first prompt and is given none. A simplified flow with no
    /// first message is given no handover either, and reads none, since the
    /// handover alone would create a context-only startup turn.
    ///
    /// `open_requests` are the requests delivered to the permaflow's flows
    /// and still open, oldest first. They follow the handover as the last
    /// piece of the message: one line naming the predecessor that left them
    /// open, then one vector datom in the form each was delivered in, under
    /// the id it was delivered with, so the same Result or Done answers it;
    /// none places nothing, the line included.
    fn compose_in_root<N: Profiled, O: LeaveOutListing>(
        &self,
        input_0: N,
        input_1: O,
        launch_root: HandoverRoot<'_>,
        open_requests: &[QueueEntry],
    ) -> Result<ComposedLaunch, CompositionError>;
}

impl ComposesInRoot for LaunchComposer {
    fn compose_in_root<N: Profiled, O: LeaveOutListing>(
        &self,
        input_0: N,
        input_1: O,
        launch_root: HandoverRoot<'_>,
        open_requests: &[QueueEntry],
    ) -> Result<ComposedLaunch, CompositionError> {
        let harnessed = HarnessedProfile {
            remembered_flows: input_0.remembered_flows(),
            launch_profile: input_0.profile(),
            harness_kind: input_0.harness_kind(),
        };
        let leave_outs = harnessed.effective_leave_outs(input_1)?;
        let verified = self.verified_context(&harnessed)?;
        let brief = if harnessed.launch_profile.flow_kind == signal_flow::FlowKind::Simplified {
            harnessed.launch_profile.instruction_prompt.0.clone()
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
            let handover = if brief.is_empty() {
                None
            } else {
                self.handover_text(&harnessed.launch_profile, launch_root)?
            };
            let brief = match &handover {
                Some(handover) => vec![brief].placed_last(handover).concat(),
                None => brief,
            };
            let brief = match open_requests.successor_text(&harnessed.launch_profile) {
                Some(open) if !brief.is_empty() => vec![brief].placed_last(&open).concat(),
                _ => brief,
            };
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
        // The handover follows the brief, and the open requests follow the
        // handover, the last piece of the message.
        let mut pieces = segments.joined_with(&brief);
        if let Some(handover) = &self.handover_text(&harnessed.launch_profile, launch_root)? {
            pieces = pieces.placed_last(handover);
        }
        if let Some(open) = open_requests.successor_text(&harnessed.launch_profile) {
            pieces = pieces.placed_last(&open);
        }
        let body = pieces.concat();
        let parts = harnessed.harness_kind.parts_of(pieces)?;
        Ok(Self::assembled(
            &harnessed,
            &system_prompt,
            &body,
            &parts,
            &leave_outs,
        ))
    }
}

/// What a successor's open requests become in its first prompt: the line
/// naming the predecessor that left them open, directly followed by their
/// vector datom. None when nothing is open or the launch names no
/// predecessor.
pub trait RendersOpenRequests {
    fn successor_text(&self, profile: &LaunchProfile) -> Option<String>;
}

impl RendersOpenRequests for [QueueEntry] {
    fn successor_text(&self, profile: &LaunchProfile) -> Option<String> {
        use crate::handover::FramesOpenRequests;
        let predecessor = profile.flow_id_option.as_ref()?;
        if self.is_empty() {
            return None;
        }
        Some(format!(
            "{}\n{}",
            predecessor.open_requests_line(),
            self.prompt_text(&profile.address)
        ))
    }
}

/// What a metaflow's queued requests become in a flow's prompt: one vector
/// datom of their NativeInputs, oldest first.
pub trait RendersQueuedWork {
    fn prompt_text(&self, recipient: &signal_flow::Address) -> String;
}

impl RendersQueuedWork for [QueueEntry] {
    /// A cohort is one vector datom: each request as its NativeInput, its
    /// logical delivery id, who sent it, and its request under its
    /// priority, oldest first.
    fn prompt_text(&self, recipient: &signal_flow::Address) -> String {
        use crate::message_projection::ProjectsQueueEntry;
        self.iter()
            .map(|entry| entry.visible_input(recipient))
            .collect::<Vec<_>>()
            .datomize(Vec::new())
            .protosize()
            .compact()
    }
}

/// Everything a fresh wake knows of the flow it starts, all of it read from
/// the meta configuration and the metaflow's last flow: its Address, its
/// Model's native model, the module identities and leave-outs the last launch
/// named, the Herdr session its last flow ran in, and the queued work.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FreshSeat {
    pub address: Address,
    pub launch_request_id: String,
    pub model_name: String,
    pub effort: String,
    pub herdr_session_name: String,
    pub system_modules: signal_flow::SystemModules,
    pub user_modules: signal_flow::UserModules,
    pub leave_outs_option: Option<Vec<LeaveOut>>,
    pub flow_kind: signal_flow::FlowKind,
    pub tool_set: signal_flow::ToolSet,
    pub prompt: String,
}

/// Where a context module goes, by the kind of its explicit
/// `kind:topic:number` identity. Spirit and intent sit in the system prompt;
/// every other kind goes to the first message with the remembered raw records.
pub(crate) trait PlacesModuleByKind {
    fn placed_in_system_prompt(&self) -> bool;
}

impl PlacesModuleByKind for str {
    fn placed_in_system_prompt(&self) -> bool {
        matches!(self.split(':').next(), Some("spirit" | "intent"))
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
            launch_request_id: signal_flow::LaunchRequestId(self.launch_request_id.clone()),
            system_modules: self.system_modules.clone(),
            user_modules: self.user_modules.clone(),
            address: self.address.clone(),
            model_name: signal_flow::ModelName(self.model_name.clone()),
            effort: signal_flow::Effort(self.effort.clone()),
            flow_id_option: None,
            remembering: signal_flow::Remembering::Predecessors(signal_flow::RememberingDepth(1)),
            herdr_session_name: signal_flow::HerdrSessionName(self.herdr_session_name.clone()),
            leave_outs_option: self.leave_outs_option.clone().map(signal_flow::LeaveOuts),
            flow_kind: self.flow_kind.clone(),
            tool_set: self.tool_set.clone(),
            instruction_prompt: signal_flow::InstructionPrompt(self.prompt.clone()),
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
        Address, Aspect, CompositionRefusal, ContextRefusal, HarnessKind, LaunchProfile, LeaveOut,
        Model, Remembering,
    };

    /// A launch profile and a composer holding its modules, for tests.
    pub(crate) fn fixture_profile() -> LaunchProfile {
        LaunchProfile {
            launch_request_id: signal_flow::LaunchRequestId("launch-42".into()),
            system_modules: signal_flow::SystemModules(vec![
                signal_flow::ModuleIdentity("spirit:core:0".into()),
                signal_flow::ModuleIdentity("intent:flow:0".into()),
            ]),
            user_modules: signal_flow::UserModules(vec![
                signal_flow::ModuleIdentity("operation:main-flow:0".into()),
                signal_flow::ModuleIdentity("knowledge:flow:0".into()),
            ]),
            address: Address {
                aspect: Aspect::Field,
                topic: flow_ethos::Topic("flow".into()),
                model: Model::Sol,
            },
            model_name: flow_ethos::ModelName("gpt-6-astra".into()),
            effort: signal_flow::Effort("medium".into()),
            flow_id_option: Some(flow_ethos::FlowId("1b8ac0".into())),
            remembering: Remembering::Custom(vec![flow_ethos::FlowId("836818".into())]),
            herdr_session_name: signal_flow::HerdrSessionName("messaging-build".into()),
            leave_outs_option: None,
            flow_kind: signal_flow::FlowKind::Regular,
            tool_set: signal_flow::ToolSet::Default,
            instruction_prompt: signal_flow::InstructionPrompt("Carry the bounded task.".into()),
        }
    }

    pub(crate) fn fixture_composer() -> LaunchComposer {
        LaunchComposer::with_context(crate::context::ContextComposer(std::sync::Arc::new(
            FixtureComposer::from(
                &[
                    ("spirit:core:0", "Spirit body."),
                    ("intent:flow:0", "Intent body."),
                    ("operation:main-flow:0", "Main flow body."),
                    ("knowledge:flow:0", "Knowledge body."),
                ][..],
            ),
        )))
    }

    fn fixture_composer_with_presentation(prompt: Option<String>) -> LaunchComposer {
        LaunchComposer::with_presentation_prompt(
            crate::context::ContextComposer(std::sync::Arc::new(FixtureComposer::from(
                &[
                    ("spirit:core:0", "Spirit body."),
                    ("intent:flow:0", "Intent body."),
                    ("operation:main-flow:0", "Main flow body."),
                    ("knowledge:flow:0", "Knowledge body."),
                ][..],
            ))),
            prompt,
        )
    }

    /// The raw records of the flows a launch remembers are composed into its
    /// first message after its USER modules and before its brief, and the
    /// composer is asked for exactly those flows; a launch remembering none
    /// carries none.
    #[test]
    fn remembered_flows_raw_records_follow_the_user_modules_in_the_first_message() {
        let mut context = FixtureComposer::from(
            &[
                ("spirit:core:0", "Spirit body."),
                ("intent:flow:0", "Intent body."),
                ("operation:main-flow:0", "Main flow body."),
                ("knowledge:flow:0", "Knowledge body."),
            ][..],
        );
        context.records.insert(
            "36b71b".into(),
            "## 2026-10-23, living, direct\n\"You can't leave me without a primary.\"\n".into(),
        );
        context
            .records
            .insert("e3c8c1".into(), "## Notion\nA crazy idea.\n".into());
        let composer = LaunchComposer::with_context(crate::context::ContextComposer(
            std::sync::Arc::new(context),
        ));
        let composed = composer
            .compose(
                HarnessedProfile {
                    remembered_flows: crate::generated::starter::RememberedFlows(vec![
                        flow_ethos::FlowId("36b71b".into()),
                        flow_ethos::FlowId("e3c8c1".into()),
                    ]),
                    harness_kind: HarnessKind::Claude,
                    launch_profile: fixture_profile(),
                },
                Vec::<LeaveOut>::new(),
            )
            .unwrap();
        let body = &composed.first_prompt_payload.first_prompt_body;
        let user = body.0.find("Knowledge body.").expect("USER modules");
        let first = body
            .0
            .find("You can't leave me without a primary.")
            .expect("the first remembered flow's records");
        let second = body.0.find("A crazy idea.").expect("the second's records");
        let brief = body.0.find("Carry the bounded task.").expect("the brief");
        assert!(
            user < first && first < second && second < brief,
            "{body}",
            body = body.0
        );
        let forgetting = composer
            .compose(
                HarnessedProfile {
                    remembered_flows: crate::generated::starter::RememberedFlows(Vec::new()),
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
                .0
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
            remembered_flows: crate::generated::starter::RememberedFlows(Vec::new()),
            harness_kind: HarnessKind::Claude,
            launch_profile: fixture_profile(),
        };
        let verified = composer.verified_context(&profile).unwrap();
        let segments = verified.user_segments();
        let prompt = composer.compose_system_prompt(&profile, &verified, &segments, Some(role));
        assert!(
            prompt
                .system_prompt_text
                .0
                .ends_with(&format!("\n\n{role}"))
        );
        assert_eq!(
            prompt.system_prompt_sha256,
            signal_flow::SystemPromptSha256(format!(
                "{:x}",
                Sha256::digest(prompt.system_prompt_text.0.as_bytes())
            ))
        );
    }

    #[test]
    fn configured_presentation_prompt_preserves_exact_text_and_omits_blank_text() {
        let address = Address {
            aspect: Aspect::Psyche,
            topic: flow_ethos::Topic("core".into()),
            model: Model::Fable,
        };
        let exact = "  Exact role line\nsecond line\n  ".to_owned();
        assert_eq!(
            fixture_composer_with_presentation(Some(exact.clone()))
                .presentation_prompt(&address)
                .unwrap(),
            Some(exact)
        );
        for blank in [None, Some(String::new()), Some(" \n\t ".into())] {
            assert_eq!(
                fixture_composer_with_presentation(blank)
                    .presentation_prompt(&address)
                    .unwrap(),
                None
            );
        }
    }

    #[test]
    fn an_absent_presentation_prompt_leaves_every_address_unchanged() {
        let composer = fixture_composer_with_presentation(None);
        assert_eq!(
            composer
                .presentation_prompt(&Address {
                    aspect: Aspect::Field,
                    topic: flow_ethos::Topic("core".into()),
                    model: Model::Astra,
                })
                .unwrap(),
            None
        );
        assert_eq!(
            composer
                .presentation_prompt(&Address {
                    aspect: Aspect::Psyche,
                    topic: flow_ethos::Topic("core".into()),
                    model: Model::Fable,
                })
                .unwrap(),
            None
        );
    }

    /// A composer holding one large knowledge module per size.
    fn large(sizes: &[usize]) -> (LaunchComposer, Vec<String>) {
        let names = (0..sizes.len())
            .map(|index| format!("knowledge:large-{index}:0"))
            .collect::<Vec<_>>();
        let bodies = sizes
            .iter()
            .enumerate()
            .map(|(index, size)| char::from(b'a' + index as u8).to_string().repeat(*size))
            .collect::<Vec<_>>();
        let mut modules = vec![
            ("spirit:core:0", "Spirit body.".to_owned()),
            ("intent:flow:0", "Intent body.".to_owned()),
        ];
        for (name, body) in names.iter().zip(&bodies) {
            modules.push((name.as_str(), body.clone()));
        }
        let held = modules
            .iter()
            .map(|(name, body)| (*name, body.as_str()))
            .collect::<Vec<_>>();
        (
            LaunchComposer::with_context(crate::context::ContextComposer(std::sync::Arc::new(
                FixtureComposer::from(&held[..]),
            ))),
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
                    remembered_flows: crate::generated::starter::RememberedFlows(Vec::new()),
                    harness_kind: harness.clone(),
                    launch_profile: profile.clone(),
                },
                Vec::<signal_flow::LeaveOut>::new(),
            )
            .unwrap();
        let body = &composed.first_prompt_payload.first_prompt_body;
        assert!(body.0.starts_with("These are your skills, given here; do not reload them.\n\nModule IDs: operation:main-flow:0, knowledge:flow:0\n"));
        assert!(body.0.contains("\n<!-- module: knowledge:flow:0 -->\nKnowledge body.\n<!-- end-module: knowledge:flow:0 -->\n"));
        assert!(
            body.0
                .ends_with("<!-- end-module: knowledge:flow:0 -->\n\nCarry the bounded task.")
        );
        assert_eq!(body.0.matches("Main flow body.").count(), 1);
        assert!(!body.0.contains("Spirit body."));
        assert_eq!(composed.first_prompt_payload.prompt_part_vector.len(), 1);
        assert_eq!(
            composed.first_prompt_payload.prompt_part_vector[0]
                .prompt_part_text
                .0,
            body.0
        );
        assert!(!body.0.contains("Skill tool") && !body.0.contains("/operation:main-flow:0"));
    }

    #[test]
    fn the_system_prompt_is_the_system_context_then_the_launch_section() {
        let composed = fixture_composer()
            .compose(
                crate::composition::HarnessedProfile {
                    remembered_flows: crate::generated::starter::RememberedFlows(Vec::new()),
                    harness_kind: HarnessKind::Codex,
                    launch_profile: fixture_profile().clone(),
                },
                Vec::<signal_flow::LeaveOut>::new(),
            )
            .unwrap();
        let system = &composed.system_prompt.system_prompt_text;
        assert!(
            system
                .0
                .contains("<!-- module: spirit:core:0 -->\nSpirit body.\n")
        );
        assert!(system.0.ends_with(
            "<!-- end-module: intent:flow:0 -->\n\nMetaflow: { Field flow Sol }\nModel: gpt-6-astra\nEffort: medium\nFlow ID: pending\nNative binding: pending\nPredecessor: 1b8ac0\nRemembering: 836818\n"
        ));
        assert!(!system.0.contains("Knowledge body."));
        let body = &composed.first_prompt_payload.first_prompt_body;
        assert!(!body.0.contains("Predecessor") && !body.0.contains("Remembered"));
    }

    #[test]
    fn a_launch_carries_its_flow_id_first_in_its_launch_section() {
        use super::CarriesFlowId;
        let composed = fixture_composer()
            .compose(
                crate::composition::HarnessedProfile {
                    remembered_flows: crate::generated::starter::RememberedFlows(Vec::new()),
                    harness_kind: HarnessKind::Claude,
                    launch_profile: fixture_profile().clone(),
                },
                Vec::<signal_flow::LeaveOut>::new(),
            )
            .unwrap();
        let carried = composed.carrying_flow_id("a1b2c3");
        let system = &carried.system_prompt.system_prompt_text;
        assert!(system.0.ends_with(
            "<!-- end-module: intent:flow:0 -->\n\nMetaflow: { Field flow Sol }\nModel: gpt-6-astra\nEffort: medium\nFlow ID: a1b2c3\nNative binding: pending\nPredecessor: 1b8ac0\nRemembering: 836818\n"
        ));
        assert_eq!(carried.first_prompt_payload, composed.first_prompt_payload);
        // A launch with no predecessor and no memory gains the section.
        let mut profile = fixture_profile();
        profile.flow_id_option = None;
        profile.remembering = Remembering::Predecessors(signal_flow::RememberingDepth(0));
        let bare = fixture_composer()
            .compose(
                crate::composition::HarnessedProfile {
                    remembered_flows: crate::generated::starter::RememberedFlows(Vec::new()),
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
                .0.ends_with("<!-- end-module: intent:flow:0 -->\n\nMetaflow: { Field flow Sol }\nModel: gpt-6-astra\nEffort: medium\nFlow ID: a1b2c3\nNative binding: pending\n")
        );
        assert!(!system.0.contains("This flow belongs to metaflow"));
    }

    #[test]
    fn a_role_after_the_launch_section_stays_last_when_the_flow_id_arrives() {
        use super::CarriesFlowId;
        let mut composed = fixture_composer()
            .compose(
                crate::composition::HarnessedProfile {
                    remembered_flows: crate::generated::starter::RememberedFlows(Vec::new()),
                    harness_kind: HarnessKind::Claude,
                    launch_profile: fixture_profile(),
                },
                Vec::<signal_flow::LeaveOut>::new(),
            )
            .unwrap();
        let role = "Presentation-maker role, unchanged\n";
        let composer = fixture_composer();
        let profile = HarnessedProfile {
            remembered_flows: crate::generated::starter::RememberedFlows(Vec::new()),
            harness_kind: HarnessKind::Claude,
            launch_profile: composed.launch_profile.clone(),
        };
        let verified = composer.verified_context(&profile).unwrap();
        let segments = verified.user_segments();
        composed.system_prompt =
            composer.compose_system_prompt(&profile, &verified, &segments, Some(role));
        let carried = composed.carrying_flow_id("a1b2c3");
        use sha2::{Digest, Sha256};
        assert!(carried.system_prompt.system_prompt_text.0.ends_with(role));
        assert!(
            carried
                .system_prompt
                .system_prompt_text
                .0
                .contains("Flow ID: a1b2c3\n")
        );
        assert_eq!(
            carried.system_prompt.system_prompt_sha256,
            signal_flow::SystemPromptSha256(format!(
                "{:x}",
                Sha256::digest(carried.system_prompt.system_prompt_text.0.as_bytes())
            ))
        );
    }

    #[test]
    fn a_codex_message_puts_its_launch_record_after_the_context() {
        let composed = fixture_composer()
            .compose(
                crate::composition::HarnessedProfile {
                    remembered_flows: crate::generated::starter::RememberedFlows(Vec::new()),
                    harness_kind: HarnessKind::Codex,
                    launch_profile: fixture_profile().clone(),
                },
                Vec::<signal_flow::LeaveOut>::new(),
            )
            .unwrap();
        let body = &composed.first_prompt_payload.first_prompt_body;
        assert!(body.0.starts_with("These are your skills"));
        assert!(body.0.ends_with(concat!(
            "<!-- end-module: knowledge:flow:0 -->\n\n# Flow launch\n\n",
            "Harness: Codex\n",
            "Herdr session: messaging-build\nRemote control: app-server endpoint\n\n",
            "Carry the bounded task."
        )));
        assert!(composed.system_prompt.system_prompt_text.0.contains(
            "Metaflow: { Field flow Sol }\nModel: gpt-6-astra\nEffort: medium\nFlow ID: pending\nNative binding: pending\n"
        ));
        assert!(!body.0.contains("Role: Field Medium"));
        assert!(!body.0.contains('$'));
    }

    #[test]
    fn both_harnesses_receive_the_exact_typed_address_datoms() {
        for (aspect, aspect_name) in [
            (Aspect::Psyche, "Psyche"),
            (Aspect::Mind, "Mind"),
            (Aspect::Field, "Field"),
        ] {
            for (model, model_name) in [
                (Model::Fable, "Fable"),
                (Model::Opus, "Opus"),
                (Model::Sonnet, "Sonnet"),
                (Model::Haiku, "Haiku"),
                (Model::Astra, "Astra"),
                (Model::Sol, "Sol"),
                (Model::Luna, "Luna"),
                (Model::Terra, "Terra"),
            ] {
                for harness_kind in [HarnessKind::Claude, HarnessKind::Codex] {
                    let mut profile = fixture_profile();
                    profile.address = Address {
                        aspect: aspect.clone(),
                        topic: flow_ethos::Topic("coreTopic".into()),
                        model: model.clone(),
                    };
                    let composed = fixture_composer()
                        .compose(
                            HarnessedProfile {
                                remembered_flows: crate::generated::starter::RememberedFlows(
                                    Vec::new(),
                                ),
                                harness_kind,
                                launch_profile: profile,
                            },
                            Vec::<signal_flow::LeaveOut>::new(),
                        )
                        .unwrap();
                    let identity = format!(
                        "Metaflow: {{ {aspect_name} coreTopic {model_name} }}\nModel: gpt-6-astra\nEffort: medium\nFlow ID: pending\nNative binding: pending\n"
                    );
                    assert!(
                        composed
                            .system_prompt
                            .system_prompt_text
                            .0
                            .contains(&identity)
                    );
                    assert!(
                        !composed
                            .system_prompt
                            .system_prompt_text
                            .0
                            .contains("Role:")
                    );
                    assert!(
                        !composed
                            .first_prompt_payload
                            .first_prompt_body
                            .0
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
        profile.user_modules = signal_flow::UserModules(
            names
                .iter()
                .cloned()
                .map(signal_flow::ModuleIdentity)
                .collect(),
        );
        for harness in [HarnessKind::Claude, HarnessKind::Codex] {
            let composed = composer
                .compose(
                    crate::composition::HarnessedProfile {
                        remembered_flows: crate::generated::starter::RememberedFlows(Vec::new()),
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
                assert!(harness.fits_one_message(&part.prompt_part_text.0));
                assert!(part.prompt_part_text.0.starts_with(&format!(
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
                    .0
                    .ends_with("Carry the bounded task.")
            );
            for name in &names {
                let marker = format!("<!-- module: {name} -->");
                assert!(
                    !system.0.contains(&marker),
                    "{name} is not in the system prompt"
                );
                assert_eq!(
                    parts
                        .iter()
                        .filter(|part| part.prompt_part_text.0.contains(&marker))
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
        profile.instruction_prompt.0.clear();
        let composed = fixture_composer()
            .compose(
                HarnessedProfile {
                    remembered_flows: crate::generated::starter::RememberedFlows(Vec::new()),
                    harness_kind: HarnessKind::Claude,
                    launch_profile: profile,
                },
                Vec::<LeaveOut>::new(),
            )
            .unwrap();
        let system = &composed.system_prompt.system_prompt_text;
        assert!(system.0.contains("<!-- module: operation:main-flow:0 -->"));
        assert!(system.0.contains("<!-- module: knowledge:flow:0 -->"));
        assert!(composed.first_prompt_payload.first_prompt_body.0.is_empty());
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
                    remembered_flows: crate::generated::starter::RememberedFlows(Vec::new()),
                    harness_kind: HarnessKind::Claude,
                    launch_profile: profile,
                },
                Vec::<LeaveOut>::new(),
            )
            .unwrap();
        assert_eq!(
            composed.first_prompt_payload.first_prompt_body,
            signal_flow::FirstPromptBody("Carry the bounded task.".into())
        );
    }

    #[test]
    fn simplified_codex_launch_does_not_turn_an_empty_instruction_into_metadata() {
        let mut profile = fixture_profile();
        profile.flow_kind = signal_flow::FlowKind::Simplified;
        profile.tool_set = signal_flow::ToolSet::Explicit(Vec::new());
        profile.instruction_prompt.0.clear();
        let composed = fixture_composer()
            .compose(
                HarnessedProfile {
                    remembered_flows: crate::generated::starter::RememberedFlows(Vec::new()),
                    harness_kind: HarnessKind::Codex,
                    launch_profile: profile,
                },
                Vec::<LeaveOut>::new(),
            )
            .unwrap();
        assert!(composed.first_prompt_payload.first_prompt_body.0.is_empty());
        assert!(composed.first_prompt_payload.prompt_part_vector.is_empty());
    }

    /// One USER module larger than one message is not cut and never moved
    /// into the system prompt: the launch is refused.
    #[test]
    fn a_module_too_large_for_one_message_refuses_the_launch() {
        let (composer, names) = large(&[1_100_000]);
        let mut profile = fixture_profile();
        profile.user_modules =
            signal_flow::UserModules(names.into_iter().map(signal_flow::ModuleIdentity).collect());
        assert_eq!(
            composer.compose(
                crate::composition::HarnessedProfile {
                    remembered_flows: crate::generated::starter::RememberedFlows(Vec::new()),
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
        profile.instruction_prompt = signal_flow::InstructionPrompt("x".repeat(1_100_000));
        assert_eq!(
            fixture_composer().compose(
                crate::composition::HarnessedProfile {
                    remembered_flows: crate::generated::starter::RememberedFlows(Vec::new()),
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
                ("spirit:core:0", "Spirit body."),
                ("intent:flow:0", "Intent body."),
                ("vision:flow:0", "Vision body."),
                ("knowledge:flow:0", "Knowledge body."),
                ("compensation:truth:0", "Compensation body."),
                ("inquiry:flow:0", "Inquiry body."),
                ("spirit:legacy:0", "Legacy spirit body."),
                ("operation:main-flow:0", "Main flow body."),
            ][..],
        );
        let composer = LaunchComposer::with_context(crate::context::ContextComposer(
            std::sync::Arc::new(context),
        ));
        let mut profile = fixture_profile();
        profile.system_modules = signal_flow::SystemModules(vec![
            signal_flow::ModuleIdentity("spirit:core:0".into()),
            signal_flow::ModuleIdentity("vision:flow:0".into()),
            signal_flow::ModuleIdentity("spirit:legacy:0".into()),
        ]);
        profile.user_modules = signal_flow::UserModules(vec![
            signal_flow::ModuleIdentity("intent:flow:0".into()),
            signal_flow::ModuleIdentity("knowledge:flow:0".into()),
            signal_flow::ModuleIdentity("compensation:truth:0".into()),
            signal_flow::ModuleIdentity("inquiry:flow:0".into()),
            signal_flow::ModuleIdentity("operation:main-flow:0".into()),
        ]);
        let composed = composer
            .compose(
                HarnessedProfile {
                    remembered_flows: crate::generated::starter::RememberedFlows(Vec::new()),
                    harness_kind: HarnessKind::Claude,
                    launch_profile: profile,
                },
                Vec::<LeaveOut>::new(),
            )
            .unwrap();
        let system = &composed.system_prompt.system_prompt_text;
        let first = &composed.first_prompt_payload.first_prompt_body;
        for body in ["Spirit body.", "Intent body.", "Legacy spirit body."] {
            assert!(system.0.contains(body), "{body} in the system prompt");
            assert!(!first.0.contains(body), "{body} not in the first message");
        }
        for body in [
            "Vision body.",
            "Knowledge body.",
            "Compensation body.",
            "Inquiry body.",
            "Main flow body.",
        ] {
            assert!(first.0.contains(body), "{body} in the first message");
            assert!(!system.0.contains(body), "{body} not in the system prompt");
        }
    }

    #[test]
    fn a_system_prompt_past_one_argument_is_composed_whole() {
        let large_intent = "b".repeat(140_000);
        let composer = LaunchComposer::with_context(crate::context::ContextComposer(
            std::sync::Arc::new(FixtureComposer::from(
                &[
                    ("spirit:core:0", "Spirit body."),
                    ("intent:large:0", large_intent.as_str()),
                    ("knowledge:small:0", "Knowledge body."),
                ][..],
            )),
        ));
        let mut profile = fixture_profile();
        profile.system_modules = signal_flow::SystemModules(vec![
            signal_flow::ModuleIdentity("spirit:core:0".into()),
            signal_flow::ModuleIdentity("intent:large:0".into()),
        ]);
        profile.user_modules = signal_flow::UserModules(vec![signal_flow::ModuleIdentity(
            "knowledge:small:0".into(),
        )]);
        let composed = composer
            .compose(
                crate::composition::HarnessedProfile {
                    remembered_flows: crate::generated::starter::RememberedFlows(Vec::new()),
                    harness_kind: HarnessKind::Codex,
                    launch_profile: profile.clone(),
                },
                Vec::<signal_flow::LeaveOut>::new(),
            )
            .unwrap();
        assert!(composed.system_prompt.system_prompt_text.0.len() > 140_000);
    }

    #[test]
    fn native_queue_text_is_one_vector_of_native_input_datoms() {
        use super::RendersQueuedWork;
        use signal_flow::{QueueEntry, Request, SentBy};
        let order = QueueEntry {
            logical_delivery_id: flow_ethos::LogicalDeliveryId(
                "logical-delivery-0000000000000001".into(),
            ),
            request_id: flow_ethos::RequestId("test".into()),
            sent_by: SentBy::Flow,
            delivery: flow_ethos::Delivery::Queue,
            message: signal_flow::Message::Soft(Request::Order("investigate".into())),
        };
        assert_eq!(
            [order].prompt_text(&signal_flow::Address {
                aspect: signal_flow::Aspect::Psyche,
                topic: flow_ethos::Topic("flow".into()),
                model: signal_flow::Model::Opus
            }),
            "[ { Notification Soft.Order.{ test investigate } } ]"
        );
    }

    #[test]
    fn module_identities_require_an_explicit_canonical_version() {
        let composer = fixture_composer();
        for identity in [
            "spirit-core",
            "spirit:core",
            "spirit:core:",
            "spirit:core:01",
            "spirit:core:-1",
            "spirit:core:+1",
            "spirit:core:latest",
            "spirit:core:1:2",
            "spirit::0",
            ":core:0",
            "Spirit:core:0",
            "spirit:core:١",
        ] {
            for system in [true, false] {
                let mut profile = fixture_profile();
                let modules = if system {
                    &mut profile.system_modules.0
                } else {
                    &mut profile.user_modules.0
                };
                modules[0] = signal_flow::ModuleIdentity(identity.into());
                assert_eq!(
                    composer.compose(
                        HarnessedProfile {
                            remembered_flows: crate::generated::starter::RememberedFlows(Vec::new()),
                            harness_kind: HarnessKind::Claude,
                            launch_profile: profile,
                        },
                        Vec::<LeaveOut>::new(),
                    ),
                    Err(CompositionError::InvalidProfileField(if system {
                        CompositionRefusal::InvalidSystemModules
                    } else {
                        CompositionRefusal::InvalidUserModules
                    })),
                    "{identity} in system={system}",
                );
            }
        }
    }

    #[test]
    fn composition_selects_the_named_version_without_latest_resolution() {
        let context = FixtureComposer::from(
            &[
                ("spirit:core:0", "SYSTEM_ZERO"),
                ("knowledge:flow:0", "VERSION_ZERO"),
                ("knowledge:flow:1", "VERSION_ONE"),
            ][..],
        );
        let composer = LaunchComposer::with_context(crate::context::ContextComposer(
            std::sync::Arc::new(context),
        ));
        for (identity, wanted, other) in [
            ("knowledge:flow:0", "VERSION_ZERO", "VERSION_ONE"),
            ("knowledge:flow:1", "VERSION_ONE", "VERSION_ZERO"),
        ] {
            let mut profile = fixture_profile();
            profile.system_modules = signal_flow::SystemModules(vec![signal_flow::ModuleIdentity(
                "spirit:core:0".into(),
            )]);
            profile.user_modules =
                signal_flow::UserModules(vec![signal_flow::ModuleIdentity(identity.into())]);
            let composed = composer
                .compose(
                    HarnessedProfile {
                        remembered_flows: crate::generated::starter::RememberedFlows(Vec::new()),
                        harness_kind: HarnessKind::Claude,
                        launch_profile: profile,
                    },
                    Vec::<LeaveOut>::new(),
                )
                .unwrap();
            let body = &composed.first_prompt_payload.first_prompt_body.0;
            assert!(body.contains(wanted));
            assert!(!body.contains(other));
            assert!(body.contains(identity));
        }
    }

    #[test]
    fn a_profile_naming_no_valid_identities_is_refused_before_composition() {
        let composer = fixture_composer();
        type ProfileChange = fn(&mut LaunchProfile);
        let cases: [(CompositionRefusal, ProfileChange); 7] = [
            (CompositionRefusal::InvalidSystemModules, |profile| {
                profile.system_modules.0.clear()
            }),
            (CompositionRefusal::InvalidSystemModules, |profile| {
                profile
                    .system_modules
                    .0
                    .push(signal_flow::ModuleIdentity("/abs/path.md".into()))
            }),
            (CompositionRefusal::InvalidUserModules, |profile| {
                profile
                    .user_modules
                    .0
                    .push(signal_flow::ModuleIdentity("knowledge:flow:0".into()))
            }),
            (CompositionRefusal::InvalidUserModules, |profile| {
                profile
                    .user_modules
                    .0
                    .push(signal_flow::ModuleIdentity("spirit:core:0".into()))
            }),
            // A Remembering of no flows names the field it refuses.
            (CompositionRefusal::InvalidRemembering, |profile| {
                profile.remembering = Remembering::Custom(vec![])
            }),
            (CompositionRefusal::InvalidRemembering, |profile| {
                profile.remembering = Remembering::Custom(vec![flow_ethos::FlowId(String::new())])
            }),
            (CompositionRefusal::InvalidRemembering, |profile| {
                profile.remembering = Remembering::Predecessors(signal_flow::RememberingDepth(-1))
            }),
        ];
        for (field, change) in cases {
            let mut profile = fixture_profile();
            change(&mut profile);
            assert_eq!(
                composer.compose(
                    crate::composition::HarnessedProfile {
                        remembered_flows: crate::generated::starter::RememberedFlows(Vec::new()),
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
    fn a_missing_identity_refuses_the_launch_with_its_identity() {
        let mut profile = fixture_profile();
        profile
            .user_modules
            .0
            .push(signal_flow::ModuleIdentity("unknown-module".into()));
        assert_eq!(
            fixture_composer().compose(
                crate::composition::HarnessedProfile {
                    remembered_flows: crate::generated::starter::RememberedFlows(Vec::new()),
                    harness_kind: HarnessKind::Codex,
                    launch_profile: profile.clone()
                },
                Vec::<signal_flow::LeaveOut>::new(),
            ),
            Err(CompositionError::Context(ContextRefusal::ModuleMissing(
                Some(signal_flow::ModuleIdentity("unknown-module".into()))
            )))
        );
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
                    remembered_flows: crate::generated::starter::RememberedFlows(Vec::new()),
                    harness_kind: harness.clone(),
                    launch_profile: profile.clone(),
                },
                default.clone(),
            )
            .unwrap();
        assert_eq!(composed.leave_outs.0, default);
        // A Start's own vector replaces it, even an empty one.
        profile.leave_outs_option = Some(signal_flow::LeaveOuts(vec![LeaveOut::GitInstructions]));
        let composed = composer
            .compose(
                crate::composition::HarnessedProfile {
                    remembered_flows: crate::generated::starter::RememberedFlows(Vec::new()),
                    harness_kind: harness.clone(),
                    launch_profile: profile.clone(),
                },
                default.clone(),
            )
            .unwrap();
        assert_eq!(composed.leave_outs.0, [LeaveOut::GitInstructions]);
        profile.leave_outs_option = Some(signal_flow::LeaveOuts(Vec::new()));
        assert!(
            composer
                .compose(
                    crate::composition::HarnessedProfile {
                        remembered_flows: crate::generated::starter::RememberedFlows(Vec::new()),
                        harness_kind: harness.clone(),
                        launch_profile: profile.clone()
                    },
                    default.clone()
                )
                .unwrap()
                .leave_outs
                .0
                .is_empty()
        );
        // A Start may leave out ToolSearch while other tools remain.
        profile.leave_outs_option = Some(signal_flow::LeaveOuts(vec![LeaveOut::ToolSearch]));
        assert!(
            composer
                .compose(
                    crate::composition::HarnessedProfile {
                        remembered_flows: crate::generated::starter::RememberedFlows(Vec::new()),
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
