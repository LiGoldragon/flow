//! Exact-byte first-prompt composition for an authored Flow launch profile.

use sha2::{Digest, Sha256};
use signal_flow::{
    ComposedLaunch, FirstPromptPayload, FlowAspect, HarnessKind, LaunchProfile, LaunchSource,
    PowerLevel, TargetReceiptRequest,
};
use std::{
    collections::HashSet,
    fs,
    path::{Path, PathBuf},
};

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum CompositionError {
    #[error("launch profile field is invalid: {0}")]
    InvalidProfileField(&'static str),
    #[error("launch source is repeated: {0}")]
    DuplicateSource(String),
    #[error("launch source escapes the configured source root: {0}")]
    SourceOutsideRoot(String),
    #[error("launch source is missing: {0}")]
    MissingSource(String),
    #[error("launch source cannot be read: {0}")]
    UnreadableSource(String),
    #[error("launch source is not UTF-8: {0}")]
    NonUtf8Source(String),
    #[error("launch source hash is malformed: {0}")]
    InvalidSourceHash(String),
    #[error("launch source hash differs: {0}")]
    SourceHashMismatch(String),
    /// The per-launch copy of the system-prompt bundle could not be written.
    #[error("per-launch system-prompt bundle cannot be written: {0}")]
    LaunchBundleUnwritable(String),
}

pub struct LaunchComposer {
    source_root: PathBuf,
    launch_bundles: LaunchBundles,
}

pub trait OpensLaunchComposer {
    fn at(source_root: impl Into<PathBuf>, launch_bundles: LaunchBundles) -> Self;
    /// Where this composer writes each launch's copy of the bundle.
    fn launch_bundles(&self) -> &LaunchBundles;
}

/// Where the Nexus keeps each launch's own copy of the system-prompt bundle.
///
/// The caller's bundle is shared across launches and never edited. At Start
/// the composer copies it here, byte for byte, and appends the launch's
/// trailing section (its predecessor and remembered flows); a Claude launch
/// receives the copy as `--system-prompt-file` and is told to read it. The
/// file is named by the launch request's short form, as the remote-control
/// name is, so the request ID itself never enters the prompt.
///
/// A copy lives as long as its launch can still need it: it is removed when
/// the launch is refused and that outcome is stored, or when the Flow the
/// launch bound is stopped (by Stop, or by the reap of a Replace). A Started
/// Flow keeps its copy, since its harness was given the path and told to
/// read it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LaunchBundles {
    directory: PathBuf,
}

pub trait KeepsLaunchBundles {
    fn at(directory: impl Into<PathBuf>) -> Self;
    /// The per-launch copy a profile's launch receives.
    fn file_for(&self, profile: &LaunchProfile) -> PathBuf;
    /// The per-launch copy of the launch request this ID names.
    fn file_for_request(&self, launch_request_id: &str) -> PathBuf;
    /// Removes the per-launch copy of a launch request. A copy that was
    /// never written (a Codex launch, or one refused before composition) is
    /// already gone; any other failure is reported.
    fn remove_for_request(&self, launch_request_id: &str) -> std::io::Result<()>;
}

impl KeepsLaunchBundles for LaunchBundles {
    fn at(directory: impl Into<PathBuf>) -> Self {
        Self {
            directory: directory.into(),
        }
    }

    fn file_for(&self, profile: &LaunchProfile) -> PathBuf {
        self.file_for_request(&profile.launch_request_id)
    }

    fn file_for_request(&self, launch_request_id: &str) -> PathBuf {
        self.directory.join(format!(
            "launch-{}.md",
            launch_request_id.launch_request_short_form()
        ))
    }

    fn remove_for_request(&self, launch_request_id: &str) -> std::io::Result<()> {
        match fs::remove_file(self.file_for_request(launch_request_id)) {
            Err(error) if error.kind() != std::io::ErrorKind::NotFound => Err(error),
            _ => Ok(()),
        }
    }
}

/// The trailing section a launch appends to its bundle: `Predecessor:` when
/// the profile names one, `Remembered:` when it remembers any flows, and
/// nothing otherwise. Each line ends with a newline.
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

/// Composes one first prompt from a profile and a configured source root.
///
/// The prompt is the only prompt a fresh Flow receives, and it opens with the
/// harness's native skill invocation: Claude reads stacked `/name` commands at
/// the head of a block, Codex reads `$name` mentions beside its typed skill
/// inputs. Sources are named by absolute path, never inlined.
///
/// A short Claude prompt opens with native stacked commands. A prompt that
/// Claude Code will wrap as pasted content instead names every required skill
/// for the Skill tool. The adapter verifies either exact transcript shape
/// against the persisted prompt hash; neither form is truncated or refused.
///
/// `prompt_sha256` is SHA-256 over the exact UTF-8 bytes in
/// `first_prompt_body`; it stays in the store and the observer and never
/// enters the prompt. `first_prompt_text` is that body followed by the fixed
/// receipt footer, which carries a short marker and no hash or request ID. The
/// adapter sends `first_prompt_text` exactly.
pub trait ComposesLaunch {
    fn compose(&self, profile: &LaunchProfile) -> Result<ComposedLaunch, CompositionError>;
}

/// Verifies the exact body digest and the fixed receipt footer before a
/// composed prompt crosses either native harness boundary.
pub trait ValidatesComposedPrompt {
    fn has_canonical_first_prompt(&self) -> bool;
}

/// The receipt a launched Flow is asked for. Its single line is bound to the
/// Flow by native session, transcript cursor and the authenticated first
/// turn, so the marker itself carries no identity and no hash.
///
/// Asking for the marker and nothing else is what makes it verifiable, and
/// it is also what ends the seat's turn. The brief the same prompt carries
/// is therefore not begun by this footer; Flow begins it, sending
/// [`crate::launching::ContinuesIntoBrief::BRIEF_CONTINUATION`] over the
/// seat's bound route the moment the receipt is witnessed. The footer stays
/// exactly as it is.
pub trait AsksForLaunchReceipt {
    const LAUNCH_RECEIPT: &'static str = "FLOW_LAUNCH_RECEIPT_V2";

    /// The fixed footer that follows every composed body of the harness.
    /// Claude's footer continues its one line; Codex's is its own paragraph.
    fn receipt_footer(&self) -> String;
}

impl AsksForLaunchReceipt for HarnessKind {
    fn receipt_footer(&self) -> String {
        match self {
            HarnessKind::Claude => format!(
                " When every skill has loaded, reply once with exactly {} and nothing else.",
                Self::LAUNCH_RECEIPT
            ),
            HarnessKind::Codex => format!(
                "\n\nWhen every skill has loaded, reply once with exactly this line and nothing else:\n{}",
                Self::LAUNCH_RECEIPT
            ),
        }
    }
}

/// Claude Code's observed transition to a pasted-content transcript shape.
/// This selects the direct Skill-tool wording; it is not a maximum prompt
/// size. A line with a break is likewise sent in the direct form.
pub trait FitsClaudePaste {
    const CLAUDE_PASTE_LIMIT: usize = 800;

    fn claude_paste_length(&self) -> usize;

    /// Whether the text keeps Claude's native command expansion shape.
    fn fits_claude_paste(&self) -> bool;
}

impl FitsClaudePaste for str {
    fn claude_paste_length(&self) -> usize {
        self.encode_utf16().count()
    }

    fn fits_claude_paste(&self) -> bool {
        !self.contains(['\r', '\n']) && self.claude_paste_length() <= Self::CLAUDE_PASTE_LIMIT
    }
}

/// How a launch's skills split across Claude's command stack: the `/name`
/// commands Claude loads from the head of one prompt, and the rest.
///
/// Claude Code 2.1.280 loads stacked head commands until its stack limit,
/// then logs "Stacked command limit (5) reached — remaining input passed as
/// arguments" and passes every later `/name` as argument text, unloaded. A
/// launch therefore stacks at most this many commands and names any further
/// skill for the Skill tool. Every stacked command receives the same
/// argument: the text after the last stacked command.
pub trait StacksClaudeCommands {
    type Skill;
    const CLAUDE_COMMAND_STACK_LIMIT: usize = 5;

    /// The skills a launch stacks as head commands.
    fn claude_stacked(&self) -> &[Self::Skill];

    /// The skills past the stack, named for the Skill tool instead.
    fn claude_unstacked(&self) -> &[Self::Skill];
}

impl<Skill> StacksClaudeCommands for [Skill] {
    type Skill = Skill;

    fn claude_stacked(&self) -> &[Skill] {
        &self[..self.len().min(Self::CLAUDE_COMMAND_STACK_LIMIT)]
    }

    fn claude_unstacked(&self) -> &[Skill] {
        &self[self.len().min(Self::CLAUDE_COMMAND_STACK_LIMIT)..]
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
        match self.flow_aspect {
            FlowAspect::Psyche => "Psyche",
            FlowAspect::Mind => "Mind",
            FlowAspect::Field => "Field",
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
    fn validate(&self, profile: &LaunchProfile) -> Result<(), CompositionError>;
}

trait ReadsLaunchSource {
    /// Verifies the source bytes against the profile hash and returns the
    /// canonical absolute path the prompt names.
    ///
    /// A `SourcePath` is written either way and the rule is the same for
    /// both: absolute is taken as written, relative is taken under
    /// `FLOW_SOURCE_ROOT`, and the source is accepted exactly when the path
    /// it resolves to lies inside that root. Anything outside is
    /// `SourceOutsideRoot`, whichever spelling asked for it. This is the
    /// same shape `system_prompt_bundle_file` already required, so a caller
    /// no longer has to spell one profile field two ways.
    fn read(&self, source: &LaunchSource) -> Result<PathBuf, CompositionError>;
}

/// Reads the system-prompt bundle a Codex main Flow receives at the top of
/// its first block, with the launch section at its end. Claude receives the
/// per-launch copy as `--system-prompt-file`.
trait ReadsSystemPromptBundle {
    fn read_bundle(&self, profile: &LaunchProfile) -> Result<String, CompositionError>;
}

/// Writes a launch's own copy of the bundle: the caller's bytes unchanged,
/// then the launch section after a blank line when it has any line.
trait WritesLaunchBundle {
    fn write_launch_bundle(&self, profile: &LaunchProfile) -> Result<PathBuf, CompositionError>;
}

trait RendersLaunchProfile {
    fn render_native_head(&self, profile: &LaunchProfile, bundle: Option<&str>) -> String;
    fn render_claude_line(
        &self,
        profile: &LaunchProfile,
        bundle_file: &Path,
        sources: &[PathBuf],
    ) -> String;
    fn render_claude_direct(
        &self,
        profile: &LaunchProfile,
        bundle_file: &Path,
        sources: &[PathBuf],
    ) -> String;
    fn render_body(
        &self,
        profile: &LaunchProfile,
        bundle: &LaunchBundleText,
        sources: &[PathBuf],
    ) -> String;
}

/// What a harness receives of the bundle: Codex its text, Claude the path
/// of its per-launch copy.
enum LaunchBundleText {
    Inline(String),
    File(PathBuf),
}

trait HashesPromptBody {
    fn sha256(&self, bytes: &[u8]) -> String;
}

impl OpensLaunchComposer for LaunchComposer {
    fn at(source_root: impl Into<PathBuf>, launch_bundles: LaunchBundles) -> Self {
        Self {
            source_root: source_root.into(),
            launch_bundles,
        }
    }

    fn launch_bundles(&self) -> &LaunchBundles {
        &self.launch_bundles
    }
}

impl HashesPromptBody for LaunchComposer {
    fn sha256(&self, bytes: &[u8]) -> String {
        format!("{:x}", Sha256::digest(bytes))
    }
}

impl ValidatesComposedPrompt for ComposedLaunch {
    fn has_canonical_first_prompt(&self) -> bool {
        let body_hash = format!(
            "{:x}",
            Sha256::digest(self.first_prompt_payload.first_prompt_body.as_bytes())
        );
        if body_hash != self.first_prompt_payload.prompt_sha256
            || self.target_receipt_request.launch_request_id
                != self.launch_profile.launch_request_id
            || self.target_receipt_request.prompt_sha256 != body_hash
        {
            return false;
        }
        let text = &self.first_prompt_payload.first_prompt_text;
        *text
            == format!(
                "{}{}",
                self.first_prompt_payload.first_prompt_body,
                self.launch_profile.harness_kind.receipt_footer()
            )
    }
}

impl ValidatesLaunchProfile for LaunchComposer {
    fn validate(&self, profile: &LaunchProfile) -> Result<(), CompositionError> {
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
        let bundle = Path::new(&profile.system_prompt_bundle_file);
        if !bundle.is_absolute()
            || !bundle.is_file()
            || fs::symlink_metadata(bundle)
                .map(|m| m.file_type().is_symlink())
                .unwrap_or(true)
        {
            return Err(CompositionError::InvalidProfileField(
                "system_prompt_bundle_file",
            ));
        }
        if profile.skill_name_vector.iter().any(|name| {
            name.is_empty()
                || !name
                    .bytes()
                    .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
        }) {
            return Err(CompositionError::InvalidProfileField("skill_name_vector"));
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

        let mut names = HashSet::new();
        if profile
            .skill_name_vector
            .iter()
            .any(|name| !names.insert(name))
        {
            return Err(CompositionError::InvalidProfileField("skill_name_vector"));
        }
        let mut paths = HashSet::new();
        for source in &profile.launch_source_vector {
            if !paths.insert(&source.source_path) {
                return Err(CompositionError::DuplicateSource(
                    source.source_path.clone(),
                ));
            }
        }
        Ok(())
    }
}

impl ReadsLaunchSource for LaunchComposer {
    fn read(&self, source: &LaunchSource) -> Result<PathBuf, CompositionError> {
        let written = Path::new(&source.source_path);
        if written.as_os_str().is_empty() {
            return Err(CompositionError::SourceOutsideRoot(
                source.source_path.clone(),
            ));
        }
        if source.source_path.contains(['\r', '\n', '`']) {
            return Err(CompositionError::InvalidProfileField(
                "launch_source_vector",
            ));
        }
        if source.source_sha256.len() != 64
            || !source
                .source_sha256
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(CompositionError::InvalidSourceHash(
                source.source_path.clone(),
            ));
        }

        let root = self
            .source_root
            .canonicalize()
            .map_err(|_| CompositionError::UnreadableSource(source.source_path.clone()))?;
        // One rule for both spellings: an absolute path is taken as it is
        // written and a relative one is taken under the root, and either is
        // accepted exactly when what it resolves to lies inside the root.
        // `Path::join` yields the absolute path unchanged, so the two
        // spellings meet here. Escape by `..` or by symlink is caught below,
        // where the canonical path is required to start with the root.
        let candidate = root.join(written);
        if !candidate.exists() {
            return Err(CompositionError::MissingSource(source.source_path.clone()));
        }
        let canonical = candidate
            .canonicalize()
            .map_err(|_| CompositionError::UnreadableSource(source.source_path.clone()))?;
        if !canonical.starts_with(&root) {
            return Err(CompositionError::SourceOutsideRoot(
                source.source_path.clone(),
            ));
        }
        let bytes = fs::read(&canonical)
            .map_err(|_| CompositionError::UnreadableSource(source.source_path.clone()))?;
        if self.sha256(&bytes) != source.source_sha256 {
            return Err(CompositionError::SourceHashMismatch(
                source.source_path.clone(),
            ));
        }
        Ok(canonical)
    }
}

impl ReadsSystemPromptBundle for LaunchComposer {
    fn read_bundle(&self, profile: &LaunchProfile) -> Result<String, CompositionError> {
        let bytes = fs::read(&profile.system_prompt_bundle_file)
            .map_err(|_| CompositionError::InvalidProfileField("system_prompt_bundle_file"))?;
        let text = String::from_utf8(bytes)
            .map_err(|_| CompositionError::InvalidProfileField("system_prompt_bundle_file"))?;
        let text = text.trim();
        if text.is_empty() {
            return Err(CompositionError::InvalidProfileField(
                "system_prompt_bundle_file",
            ));
        }
        let section = profile.launch_section();
        if section.is_empty() {
            return Ok(text.to_owned());
        }
        Ok(format!("{text}\n\n{}", section.trim_end()))
    }
}

impl WritesLaunchBundle for LaunchComposer {
    fn write_launch_bundle(&self, profile: &LaunchProfile) -> Result<PathBuf, CompositionError> {
        let mut bytes = fs::read(&profile.system_prompt_bundle_file)
            .map_err(|_| CompositionError::InvalidProfileField("system_prompt_bundle_file"))?;
        let section = profile.launch_section();
        if !section.is_empty() {
            if !bytes.is_empty() && !bytes.ends_with(b"\n") {
                bytes.push(b'\n');
            }
            bytes.push(b'\n');
            bytes.extend_from_slice(section.as_bytes());
        }
        let file = self.launch_bundles.file_for(profile);
        let unwritable =
            |error: std::io::Error| CompositionError::LaunchBundleUnwritable(error.to_string());
        fs::create_dir_all(&self.launch_bundles.directory).map_err(unwritable)?;
        let partial = file.with_extension("md.partial");
        fs::write(&partial, &bytes).map_err(unwritable)?;
        fs::rename(&partial, &file).map_err(unwritable)?;
        Ok(file)
    }
}

impl RendersLaunchProfile for LaunchComposer {
    fn render_native_head(&self, profile: &LaunchProfile, bundle: Option<&str>) -> String {
        let skills = &profile.skill_name_vector;
        match profile.harness_kind {
            // Claude reads commands only at the head of the block, as
            // space-separated `/name` tokens, and loads at most a few of
            // them; the first other token ends the stack and the rest of the
            // block is the argument every stacked command receives.
            HarnessKind::Claude => skills
                .claude_stacked()
                .iter()
                .map(|skill| format!("/{skill} "))
                .collect(),
            // Codex keeps its stock base instructions. The main Flow alone
            // receives the system-prompt bundle at the top of its first
            // block, then its `$name` skill lines; native Codex descendants
            // never see this block and keep the stock base.
            HarnessKind::Codex => {
                let mut head = bundle.map(|text| format!("{text}\n\n")).unwrap_or_default();
                let mut lines = Vec::new();
                if !skills.iter().any(|skill| skill == "main-flow") {
                    lines.push("$main-flow".to_owned());
                }
                lines.extend(skills.iter().map(|skill| format!("${skill}")));
                head.push_str(&lines.join("\n"));
                head.push_str("\n\n");
                head
            }
        }
    }

    /// Claude's one line: the stacked commands, then one sentence naming
    /// the per-launch bundle to read, the skills past the stack, the goal,
    /// and the sources. Predecessor and remembered flows are in that bundle;
    /// role, model, effort and remote control reach Claude through its argv
    /// and stay in the store.
    fn render_claude_line(
        &self,
        profile: &LaunchProfile,
        bundle_file: &Path,
        sources: &[PathBuf],
    ) -> String {
        let mut line = self.render_native_head(profile, None);
        line.push_str(&format!(
            "Read {} for your launch mode",
            bundle_file.display()
        ));
        let unstacked = profile.skill_name_vector.claude_unstacked();
        if !unstacked.is_empty() {
            line.push_str(&format!(
                ", load {} through the Skill tool in this order",
                unstacked.join(", ")
            ));
        }
        line.push_str(", then: ");
        line.push_str(&profile.instruction_prompt);
        if !sources.is_empty() {
            line.push_str(" Sources: ");
            line.push_str(
                &sources
                    .iter()
                    .map(|source| source.to_string_lossy())
                    .collect::<Vec<_>>()
                    .join(", "),
            );
            line.push('.');
        }
        line
    }

    /// A Claude prompt that will be represented as pasted content cannot use
    /// native `/skill` commands: they remain literal text. Ask the Skill tool
    /// for every selected skill instead, preserving the exact selection order
    /// that the durable prompt intent binds.
    fn render_claude_direct(
        &self,
        profile: &LaunchProfile,
        bundle_file: &Path,
        sources: &[PathBuf],
    ) -> String {
        let mut text = format!(
            "Read {} for your launch mode, then load these skills through the Skill tool in this order: {}. Then: {}",
            bundle_file.display(),
            profile.skill_name_vector.join(", "),
            profile.instruction_prompt,
        );
        if !sources.is_empty() {
            text.push_str(" Sources: ");
            text.push_str(
                &sources
                    .iter()
                    .map(|source| source.to_string_lossy())
                    .collect::<Vec<_>>()
                    .join(", "),
            );
            text.push('.');
        }
        text
    }

    fn render_body(
        &self,
        profile: &LaunchProfile,
        bundle: &LaunchBundleText,
        sources: &[PathBuf],
    ) -> String {
        let bundle = match bundle {
            LaunchBundleText::File(file) => {
                return self.render_claude_line(profile, file, sources);
            }
            LaunchBundleText::Inline(text) => Some(text.as_str()),
        };
        // Predecessor and remembered flows are in the bundle text's trailing
        // section, which opens this block; the launch record does not repeat
        // them.
        let mut body = self.render_native_head(profile, bundle);
        body.push_str(&format!(
            "# Flow launch\n\nRole: {} {}\nHarness: Codex\nModel: {}\nEffort: {}\nHerdr session: {}\nRemote control: {}\n",
            profile.aspect_name(),
            profile.power_name(),
            profile.model_name,
            profile.effort,
            profile.herdr_session_name,
            profile.remote_control_record(),
        ));
        body.push('\n');
        body.push_str(&profile.instruction_prompt);
        if !sources.is_empty() {
            body.push_str("\n\nSources:");
            for source in sources {
                body.push_str("\n- ");
                body.push_str(&source.to_string_lossy());
            }
        }
        body
    }
}

impl ComposesLaunch for LaunchComposer {
    fn compose(&self, profile: &LaunchProfile) -> Result<ComposedLaunch, CompositionError> {
        self.validate(profile)?;
        let sources = profile
            .launch_source_vector
            .iter()
            .map(|source| self.read(source))
            .collect::<Result<Vec<_>, _>>()?;
        let bundle = match profile.harness_kind {
            HarnessKind::Codex => LaunchBundleText::Inline(self.read_bundle(profile)?),
            HarnessKind::Claude => LaunchBundleText::File(self.write_launch_bundle(profile)?),
        };
        let mut body = self.render_body(profile, &bundle, &sources);
        if profile.harness_kind == HarnessKind::Claude {
            let candidate = format!("{}{}", body, profile.harness_kind.receipt_footer());
            if !candidate.fits_claude_paste() {
                let LaunchBundleText::File(bundle_file) = &bundle else {
                    unreachable!("Claude launches always receive a bundle file")
                };
                body = self.render_claude_direct(profile, bundle_file, &sources);
            }
        }
        let prompt_sha256 = self.sha256(body.as_bytes());
        let target_receipt_request = TargetReceiptRequest {
            launch_request_id: profile.launch_request_id.clone(),
            prompt_sha256: prompt_sha256.clone(),
        };
        let first_prompt_text = format!("{}{}", body, profile.harness_kind.receipt_footer());
        Ok(ComposedLaunch {
            launch_profile: profile.clone(),
            first_prompt_payload: FirstPromptPayload {
                first_prompt_body: body,
                prompt_sha256,
                first_prompt_text,
            },
            target_receipt_request,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{
        AsksForLaunchReceipt, ComposesLaunch, CompositionError, FitsClaudePaste, LaunchBundles,
        LaunchComposer, NamesRemoteControl, OpensLaunchComposer, ShortensLaunchRequest,
        ValidatesComposedPrompt,
    };
    use crate::composition::KeepsLaunchBundles;
    use sha2::Digest as _;
    use signal_flow::{
        FlowAspect, HarnessKind, LaunchProfile, LaunchSource, PowerLevel, RememberedFlow,
    };
    use std::fs;

    trait BuildsProfile {
        fn profile(&self, sources: Vec<LaunchSource>) -> LaunchProfile;
        fn bundles(&self) -> LaunchBundles;
    }

    impl BuildsProfile for tempfile::TempDir {
        fn bundles(&self) -> LaunchBundles {
            LaunchBundles::at(self.path().join("launch-bundles"))
        }

        fn profile(&self, sources: Vec<LaunchSource>) -> LaunchProfile {
            let bundle = self.path().join("flow-system-prompt.md");
            fs::write(&bundle, "fixture bundle").unwrap();
            LaunchProfile {
                launch_request_id: "launch-42".into(),
                launch_source_vector: sources,
                skill_name_vector: vec!["spirit".into(), "main-flow".into()],
                flow_aspect: FlowAspect::Field,
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
                system_prompt_bundle_file: bundle.to_string_lossy().into_owned(),
                instruction_prompt: "Carry the bounded task.".into(),
            }
        }
    }

    trait FindsLongHexRun {
        fn has_long_hex_run(&self) -> bool;
    }

    impl FindsLongHexRun for str {
        fn has_long_hex_run(&self) -> bool {
            self.split(|character: char| !character.is_ascii_hexdigit())
                .any(|run| run.len() >= 16)
        }
    }

    #[test]
    fn codex_prompt_names_sources_by_path_and_opens_with_native_skills() {
        let root = tempfile::tempdir().unwrap();
        let first = b"first line\n\nfinal line\n";
        let second = b"second source has no trailing newline";
        fs::write(root.path().join("first.md"), first).unwrap();
        fs::write(root.path().join("second.md"), second).unwrap();
        let profile = root.profile(vec![
            LaunchSource {
                source_path: "first.md".into(),
                source_sha256: "56061831006650848af73c1976eef98f21be2d49f7f611f4dd9eb6d845ba0b1e"
                    .into(),
            },
            LaunchSource {
                source_path: "second.md".into(),
                source_sha256: "d54813b683a3efa7093709e4d5108dc8b708a5ea8d0aebce3cfc47697ece5477"
                    .into(),
            },
        ]);

        let composed = LaunchComposer::at(root.path(), root.bundles())
            .compose(&profile)
            .unwrap();
        let canonical = root.path().canonicalize().unwrap();
        let expected = format!(
            concat!(
                "fixture bundle\n\n",
                "Predecessor: 1b8ac0\n",
                "Remembered: 836818\n\n",
                "$spirit\n",
                "$main-flow\n\n",
                "# Flow launch\n\n",
                "Role: Field High\n",
                "Harness: Codex\n",
                "Model: gpt-6-astra\n",
                "Effort: medium\n",
                "Herdr session: messaging-build\n",
                "Remote control: app-server endpoint\n\n",
                "Carry the bounded task.\n\n",
                "Sources:\n",
                "- {root}/first.md\n",
                "- {root}/second.md"
            ),
            root = canonical.display(),
        );
        assert_eq!(composed.first_prompt_payload.first_prompt_body, expected);
        // One home per meaning: the bundle's trailing section names the
        // predecessor and the remembered flows, and nothing repeats them.
        let body = composed.first_prompt_payload.first_prompt_body.as_str();
        assert_eq!(body.matches("Predecessor").count(), 1, "{body}");
        assert_eq!(body.matches("Remembered").count(), 1, "{body}");
        assert_eq!(body.matches("1b8ac0").count(), 1, "{body}");
        assert_eq!(body.matches("836818").count(), 1, "{body}");
        assert_eq!(
            composed.first_prompt_payload.first_prompt_text,
            format!(
                "{expected}\n\nWhen every skill has loaded, reply once with exactly this line and nothing else:\nFLOW_LAUNCH_RECEIPT_V2"
            )
        );
        assert_eq!(
            composed.target_receipt_request.prompt_sha256,
            composed.first_prompt_payload.prompt_sha256
        );
        assert!(composed.has_canonical_first_prompt());
    }

    #[test]
    fn composed_prompt_carries_no_hash_request_id_or_source_text() {
        let root = tempfile::tempdir().unwrap();
        fs::write(root.path().join("first.md"), b"first line\n\nfinal line\n").unwrap();
        for harness in [HarnessKind::Codex, HarnessKind::Claude] {
            let mut profile = root.profile(vec![LaunchSource {
                source_path: "first.md".into(),
                source_sha256: "56061831006650848af73c1976eef98f21be2d49f7f611f4dd9eb6d845ba0b1e"
                    .into(),
            }]);
            profile.harness_kind = harness;
            let composed = LaunchComposer::at(root.path(), root.bundles())
                .compose(&profile)
                .unwrap();
            let text = composed.first_prompt_payload.first_prompt_text.as_str();
            // The Claude line names its copy by the 16-hex short form; no
            // other long hex run, hash or request ID enters the prompt.
            let without_copy_name = text.replace(&profile.launch_request_short_form(), "");
            assert!(!without_copy_name.has_long_hex_run(), "{text}");
            assert!(!text.contains(&profile.launch_request_id), "{text}");
            assert!(!text.contains(&composed.first_prompt_payload.prompt_sha256));
            assert!(!text.contains("final line"));
            assert_eq!(composed.first_prompt_payload.prompt_sha256.len(), 64);
        }
    }

    #[test]
    fn claude_prompt_stacks_its_skills_as_head_commands() {
        let root = tempfile::tempdir().unwrap();
        let mut profile = root.profile(vec![]);
        profile.harness_kind = HarnessKind::Claude;
        let composed = LaunchComposer::at(root.path(), root.bundles())
            .compose(&profile)
            .unwrap();
        let body = composed.first_prompt_payload.first_prompt_body.as_str();
        assert_eq!(
            body,
            format!(
                "/spirit /main-flow Read {} for your launch mode, then: Carry the bounded task.",
                root.bundles().file_for(&profile).display()
            ),
        );
        assert_eq!(
            composed.first_prompt_payload.first_prompt_text,
            format!(
                "{body} When every skill has loaded, reply once with exactly FLOW_LAUNCH_RECEIPT_V2 and nothing else."
            )
        );
        assert_eq!(body.matches("/spirit").count(), 1);
        assert_eq!(body.matches("/main-flow").count(), 1);
        assert!(!body.contains("$spirit"));
        assert!(!body.contains("Skill tool"));
        assert!(!body.contains("System prompt: read"));
        assert!(composed.has_canonical_first_prompt());
    }

    #[test]
    fn claude_prompt_stacks_at_most_five_commands_and_lists_the_rest() {
        let root = tempfile::tempdir().unwrap();
        let mut profile = root.profile(vec![]);
        profile.harness_kind = HarnessKind::Claude;
        profile.skill_name_vector = [
            "spirit",
            "psyche",
            "main-flow",
            "behavior",
            "herdr",
            "messaging",
            "datom",
        ]
        .map(String::from)
        .to_vec();
        let composed = LaunchComposer::at(root.path(), root.bundles())
            .compose(&profile)
            .unwrap();
        let body = composed.first_prompt_payload.first_prompt_body.as_str();
        let head = body.split(" Read ").next().unwrap();
        let commands = head.split(' ').collect::<Vec<_>>();
        assert!(commands.len() <= 5, "{head}");
        assert_eq!(
            commands,
            ["/spirit", "/psyche", "/main-flow", "/behavior", "/herdr"]
        );
        assert!(body.starts_with("/spirit /psyche /main-flow /behavior /herdr Read "));
        // The sixth and seventh skills arrive as text for the Skill tool in
        // the same line, never as a sixth `/name` command.
        assert!(
            body.contains(" for your launch mode, load messaging, datom through the Skill tool in this order, then: ")
        );
        assert!(
            composed
                .first_prompt_payload
                .first_prompt_text
                .fits_claude_paste()
        );
        assert!(!body.contains("/messaging"));
        assert!(!body.contains("/datom"));
        assert!(composed.has_canonical_first_prompt());
    }

    #[test]
    fn claude_launch_records_its_remote_control_flag() {
        let root = tempfile::tempdir().unwrap();
        let mut profile = root.profile(vec![]);
        profile.harness_kind = HarnessKind::Claude;
        let composed = LaunchComposer::at(root.path(), root.bundles())
            .compose(&profile)
            .unwrap();
        let name = profile.remote_control_name();
        assert_eq!(name.len(), "flow-".len() + 16);
        assert!(name.starts_with("flow-"));
        assert!(
            name["flow-".len()..]
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit())
        );
        assert!(!name.contains(&profile.launch_request_id));
        // The flag travels in the argv; the one line carries no launch
        // record block.
        assert!(
            !composed
                .first_prompt_payload
                .first_prompt_body
                .contains(&name)
        );
        assert!(
            !composed
                .first_prompt_payload
                .first_prompt_body
                .contains("Remote control:")
        );
        assert!(
            !composed
                .first_prompt_payload
                .first_prompt_text
                .contains("fixture bundle")
        );
        let mut sibling = profile.clone();
        sibling.launch_request_id = "launch-43".into();
        assert_ne!(sibling.remote_control_name(), name);
        profile.power_level = PowerLevel::UltraLow;
        assert_eq!(profile.remote_control_name(), name);
        assert!(composed.has_canonical_first_prompt());
    }

    #[test]
    fn launches_with_distinct_request_ids_never_share_a_name_or_a_copy() {
        // These two request IDs share the first eight hex digits of their
        // SHA-256 (0ed0b35c), so an eight-digit short form named both
        // launches `flow-0ed0b35c` and gave them one copy.
        let root = tempfile::tempdir().unwrap();
        let bundles = root.bundles();
        let mut first = root.profile(vec![]);
        first.launch_request_id = "launch-4646".into();
        let mut second = first.clone();
        second.launch_request_id = "launch-72333".into();
        assert_eq!(
            first.launch_request_short_form()[..8],
            second.launch_request_short_form()[..8]
        );
        assert_ne!(first.remote_control_name(), second.remote_control_name());
        assert_ne!(bundles.file_for(&first), bundles.file_for(&second));

        let mut names = std::collections::BTreeSet::new();
        let mut files = std::collections::BTreeSet::new();
        for index in 0..20_000 {
            let mut profile = first.clone();
            profile.launch_request_id = format!("launch-{index}");
            assert!(names.insert(profile.remote_control_name()));
            assert!(files.insert(bundles.file_for(&profile)));
        }
    }

    #[test]
    fn short_claude_prompt_keeps_native_command_expansion() {
        let root = tempfile::tempdir().unwrap();
        fs::write(root.path().join("first.md"), b"first line\n\nfinal line\n").unwrap();
        let mut profile = root.profile(vec![LaunchSource {
            source_path: "first.md".into(),
            source_sha256: "56061831006650848af73c1976eef98f21be2d49f7f611f4dd9eb6d845ba0b1e"
                .into(),
        }]);
        profile.harness_kind = HarnessKind::Claude;
        let composed = LaunchComposer::at(root.path(), root.bundles())
            .compose(&profile)
            .unwrap();
        let text = composed.first_prompt_payload.first_prompt_text.as_str();
        assert!(!text.contains('\n') && !text.contains('\r'), "{text:?}");
        assert!(text.chars().count() <= 800, "{}", text.chars().count());
        assert!(text.contains(&format!(
            " Sources: {}/first.md.",
            root.path().canonicalize().unwrap().display()
        )));
        assert!(composed.has_canonical_first_prompt());
    }

    #[test]
    fn claude_prompt_past_the_native_command_limit_uses_direct_skill_form() {
        let root = tempfile::tempdir().unwrap();
        let mut profile = root.profile(vec![]);
        profile.harness_kind = HarnessKind::Claude;
        profile.instruction_prompt = "x".repeat(34_369);
        let composed = LaunchComposer::at(root.path(), root.bundles())
            .compose(&profile)
            .unwrap();
        let text = &composed.first_prompt_payload.first_prompt_text;
        assert!(text.len() > 34_369);
        assert!(!text.fits_claude_paste());
        assert!(text.starts_with("Read "));
        assert!(text.contains(
            "then load these skills through the Skill tool in this order: spirit, main-flow."
        ));
        assert!(!text.starts_with("/spirit"));
        assert!(composed.has_canonical_first_prompt());
    }

    #[test]
    fn claude_instruction_with_line_breaks_uses_direct_skill_form() {
        let root = tempfile::tempdir().unwrap();
        let mut profile = root.profile(vec![]);
        profile.harness_kind = HarnessKind::Claude;
        profile.instruction_prompt = "Carry the task.\nThen report.".into();
        let composed = LaunchComposer::at(root.path(), root.bundles())
            .compose(&profile)
            .unwrap();
        assert!(
            composed
                .first_prompt_payload
                .first_prompt_text
                .contains('\n')
        );
        assert!(
            composed
                .first_prompt_payload
                .first_prompt_text
                .starts_with("Read ")
        );
        assert!(composed.has_canonical_first_prompt());
    }

    #[test]
    fn claude_canonical_check_keeps_a_hashed_long_prompt() {
        let root = tempfile::tempdir().unwrap();
        let mut profile = root.profile(vec![]);
        profile.harness_kind = HarnessKind::Claude;
        let mut composed = LaunchComposer::at(root.path(), root.bundles())
            .compose(&profile)
            .unwrap();
        assert!(composed.has_canonical_first_prompt());
        profile.instruction_prompt = "x".repeat(900);
        composed.launch_profile = profile;
        let body = format!("/spirit /main-flow {}", "x".repeat(900));
        composed.first_prompt_payload.first_prompt_body = body.clone();
        let hash = format!(
            "{:x}",
            <sha2::Sha256 as sha2::Digest>::digest(body.as_bytes())
        );
        composed.first_prompt_payload.prompt_sha256 = hash.clone();
        composed.target_receipt_request.prompt_sha256 = hash;
        composed.first_prompt_payload.first_prompt_text =
            format!("{body}{}", HarnessKind::Claude.receipt_footer());
        assert!(composed.has_canonical_first_prompt());
    }

    #[test]
    fn malformed_full_text_is_not_canonical_when_body_hash_is_unchanged() {
        let root = tempfile::tempdir().unwrap();
        let mut composed = LaunchComposer::at(root.path(), root.bundles())
            .compose(&root.profile(vec![]))
            .unwrap();
        let unchanged_body_hash = composed.first_prompt_payload.prompt_sha256.clone();
        composed.first_prompt_payload.first_prompt_text.push('x');

        assert_eq!(
            composed.first_prompt_payload.prompt_sha256,
            unchanged_body_hash
        );
        assert!(!composed.has_canonical_first_prompt());
    }

    /// One rule for both spellings. `system_prompt_bundle_file` must be
    /// absolute, so a caller that writes a source path absolutely is
    /// spelling the same profile the same way; the composer accepts it, and
    /// the prompt names the same canonical file either way.
    #[test]
    fn a_source_inside_the_root_is_read_whether_it_is_written_absolute_or_relative() {
        let root = tempfile::tempdir().unwrap();
        let bytes = b"exact source bytes\n";
        fs::write(root.path().join("inside.md"), bytes).unwrap();
        let hash = format!("{:x}", super::Sha256::digest(bytes));
        let composer = LaunchComposer::at(root.path(), root.bundles());

        let relative = composer
            .compose(&root.profile(vec![LaunchSource {
                source_path: "inside.md".into(),
                source_sha256: hash.clone(),
            }]))
            .expect("a relative source under the root is read");
        let absolute_path = root.path().join("inside.md").to_string_lossy().into_owned();
        let absolute = composer
            .compose(&root.profile(vec![LaunchSource {
                source_path: absolute_path.clone(),
                source_sha256: hash,
            }]))
            .expect("an absolute source inside the root is read");

        let named = root
            .path()
            .canonicalize()
            .unwrap()
            .join("inside.md")
            .to_string_lossy()
            .into_owned();
        assert!(
            relative
                .first_prompt_payload
                .first_prompt_body
                .contains(&named),
            "{}",
            relative.first_prompt_payload.first_prompt_body
        );
        assert_eq!(
            relative.first_prompt_payload.first_prompt_body,
            absolute.first_prompt_payload.first_prompt_body,
            "both spellings name the same canonical file"
        );
    }

    /// Outside is outside, however it is written: the absolute spelling is
    /// not a way past the root, and neither is `..`.
    #[test]
    fn a_source_outside_the_root_is_refused_in_either_spelling() {
        let outside = tempfile::tempdir().unwrap();
        let bytes = b"outside bytes\n";
        fs::write(outside.path().join("outside.md"), bytes).unwrap();
        let hash = format!("{:x}", super::Sha256::digest(bytes));
        let root = tempfile::tempdir().unwrap();
        let composer = LaunchComposer::at(root.path(), root.bundles());

        let absolute_path = outside
            .path()
            .join("outside.md")
            .to_string_lossy()
            .into_owned();
        assert_eq!(
            composer.compose(&root.profile(vec![LaunchSource {
                source_path: absolute_path.clone(),
                source_sha256: hash.clone(),
            }])),
            Err(CompositionError::SourceOutsideRoot(absolute_path))
        );

        let escaping = format!(
            "../{}/outside.md",
            outside.path().file_name().unwrap().to_string_lossy()
        );
        assert!(matches!(
            composer.compose(&root.profile(vec![LaunchSource {
                source_path: escaping,
                source_sha256: hash,
            }])),
            Err(CompositionError::SourceOutsideRoot(_) | CompositionError::MissingSource(_))
        ));
    }

    #[test]
    fn rejects_a_missing_source_before_composition() {
        let root = tempfile::tempdir().unwrap();
        let profile = root.profile(vec![LaunchSource {
            source_path: "missing.md".into(),
            source_sha256: "0000000000000000000000000000000000000000000000000000000000000000"
                .into(),
        }]);

        assert_eq!(
            LaunchComposer::at(root.path(), root.bundles()).compose(&profile),
            Err(CompositionError::MissingSource("missing.md".into()))
        );
    }

    #[test]
    fn rejects_source_bytes_that_do_not_match_the_profile_hash() {
        let root = tempfile::tempdir().unwrap();
        fs::write(root.path().join("changed.md"), b"changed bytes\n").unwrap();
        let profile = root.profile(vec![LaunchSource {
            source_path: "changed.md".into(),
            source_sha256: "0000000000000000000000000000000000000000000000000000000000000000"
                .into(),
        }]);

        assert_eq!(
            LaunchComposer::at(root.path(), root.bundles()).compose(&profile),
            Err(CompositionError::SourceHashMismatch("changed.md".into()))
        );
    }

    #[test]
    fn profile_keeps_skill_names_without_skill_bodies() {
        let root = tempfile::tempdir().unwrap();
        let composed = LaunchComposer::at(root.path(), root.bundles())
            .compose(&root.profile(vec![]))
            .unwrap();

        assert_eq!(
            composed.launch_profile.skill_name_vector,
            vec!["spirit", "main-flow"]
        );
        assert!(
            !composed
                .first_prompt_payload
                .first_prompt_text
                .contains("<skill>")
        );
    }

    #[test]
    fn codex_first_block_opens_with_the_bundle_text_and_no_read_line() {
        let root = tempfile::tempdir().unwrap();
        let profile = root.profile(vec![]);
        fs::write(
            &profile.system_prompt_bundle_file,
            "\n# Main-flow mode\n\nKeep the stock base.\n\n",
        )
        .unwrap();
        let composed = LaunchComposer::at(root.path(), root.bundles())
            .compose(&profile)
            .unwrap();
        let text = composed.first_prompt_payload.first_prompt_text.as_str();
        assert!(
            text.starts_with(concat!(
                "# Main-flow mode\n\nKeep the stock base.\n\n",
                "Predecessor: 1b8ac0\nRemembered: 836818\n\n",
                "$spirit\n$main-flow\n\n"
            )),
            "{text}"
        );
        assert!(!text.contains("System prompt: read"));
        assert!(!text.contains(&profile.system_prompt_bundle_file));
        assert!(composed.has_canonical_first_prompt());
    }

    #[test]
    fn codex_launch_refuses_an_empty_or_non_utf8_bundle() {
        let root = tempfile::tempdir().unwrap();
        let profile = root.profile(vec![]);
        for bytes in [&b"  \n"[..], &[0xff, 0xfe][..]] {
            fs::write(&profile.system_prompt_bundle_file, bytes).unwrap();
            assert_eq!(
                LaunchComposer::at(root.path(), root.bundles()).compose(&profile),
                Err(CompositionError::InvalidProfileField(
                    "system_prompt_bundle_file"
                ))
            );
        }
    }

    #[test]
    fn codex_head_adds_main_flow_when_the_profile_omits_it() {
        let root = tempfile::tempdir().unwrap();
        let mut profile = root.profile(vec![]);
        profile.skill_name_vector = vec!["spirit".into()];
        let composed = LaunchComposer::at(root.path(), root.bundles())
            .compose(&profile)
            .unwrap();
        assert!(
            composed
                .first_prompt_payload
                .first_prompt_body
                .starts_with(concat!(
                    "fixture bundle\n\nPredecessor: 1b8ac0\nRemembered: 836818\n\n",
                    "$main-flow\n$spirit\n\n# Flow launch\n"
                ))
        );
    }

    #[test]
    fn claude_launch_copy_carries_predecessor_and_remembered_and_leaves_the_caller_bundle() {
        let root = tempfile::tempdir().unwrap();
        let mut profile = root.profile(vec![]);
        profile.harness_kind = HarnessKind::Claude;
        profile.remembered_flow_vector.push(RememberedFlow {
            flow_id: "2c4f10".into(),
            remembering_depth: 2,
        });
        let caller = fs::read(&profile.system_prompt_bundle_file).unwrap();
        let composed = LaunchComposer::at(root.path(), root.bundles())
            .compose(&profile)
            .unwrap();
        let copy = root.bundles().file_for(&profile);
        assert_ne!(copy.to_string_lossy(), profile.system_prompt_bundle_file);
        assert_eq!(
            fs::read_to_string(&copy).unwrap(),
            "fixture bundle\n\nPredecessor: 1b8ac0\nRemembered: 836818, 2c4f10\n"
        );
        assert_eq!(
            fs::read(&profile.system_prompt_bundle_file).unwrap(),
            caller
        );
        let text = composed.first_prompt_payload.first_prompt_text.as_str();
        assert!(text.contains(&format!("Read {} for your launch mode", copy.display())));
        assert!(!text.contains(&profile.system_prompt_bundle_file));
        assert!(!text.contains("Predecessor") && !text.contains("Remembered"));
        assert!(text.fits_claude_paste(), "{text}");
        assert!(composed.has_canonical_first_prompt());
    }

    #[test]
    fn launch_copy_and_codex_bundle_carry_no_section_when_nothing_is_remembered() {
        let root = tempfile::tempdir().unwrap();
        let mut profile = root.profile(vec![]);
        profile.flow_id_option = None;
        profile.remembered_flow_vector.clear();
        fs::write(&profile.system_prompt_bundle_file, "fixture bundle\n").unwrap();
        profile.harness_kind = HarnessKind::Claude;
        LaunchComposer::at(root.path(), root.bundles())
            .compose(&profile)
            .unwrap();
        let copy = fs::read_to_string(root.bundles().file_for(&profile)).unwrap();
        assert_eq!(copy, "fixture bundle\n");
        assert_eq!(
            fs::read_to_string(&profile.system_prompt_bundle_file).unwrap(),
            "fixture bundle\n"
        );

        profile.harness_kind = HarnessKind::Codex;
        let composed = LaunchComposer::at(root.path(), root.bundles())
            .compose(&profile)
            .unwrap();
        let text = composed.first_prompt_payload.first_prompt_text.as_str();
        assert!(text.starts_with("fixture bundle\n\n$spirit\n"), "{text}");
        assert!(!text.contains("Remembered:"));
    }

    #[test]
    fn launch_copy_keeps_a_section_on_its_own_lines_after_a_trailing_newline() {
        let root = tempfile::tempdir().unwrap();
        let mut profile = root.profile(vec![]);
        profile.harness_kind = HarnessKind::Claude;
        profile.remembered_flow_vector.clear();
        fs::write(&profile.system_prompt_bundle_file, "# Mode\n\nBody.\n").unwrap();
        LaunchComposer::at(root.path(), root.bundles())
            .compose(&profile)
            .unwrap();
        assert_eq!(
            fs::read_to_string(root.bundles().file_for(&profile)).unwrap(),
            "# Mode\n\nBody.\n\nPredecessor: 1b8ac0\n"
        );
    }
}
