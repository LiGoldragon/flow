//! Exact-byte first-prompt composition for an authored Flow launch profile.

use sha2::{Digest, Sha256};
use signal_flow::{
    ComposedLaunch, FirstPromptPayload, FlowAspect, HarnessKind, LaunchProfile, LaunchSource,
    PowerLevel, TargetReceiptRequest,
};
use std::{
    collections::HashSet,
    fs,
    path::{Component, Path, PathBuf},
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
    /// A Claude first prompt must stay one line: Claude Code wraps a
    /// submission of four or more lines as pasted content, and a wrapped
    /// block expands no command.
    #[error("Claude first prompt would carry a line break")]
    ClaudeFirstLineBroken,
    /// A Claude first prompt must stay within [`ClaudeFirstLine::LIMIT`]:
    /// Claude Code wraps a longer single line as pasted content.
    #[error("Claude first prompt would be {0} characters, past the one-line limit of 800")]
    ClaudeFirstLineTooLong(usize),
}

pub struct LaunchComposer {
    source_root: PathBuf,
}

pub trait OpensLaunchComposer {
    fn at(source_root: impl Into<PathBuf>) -> Self;
}

/// Composes one first prompt from a profile and a configured source root.
///
/// The prompt is the only prompt a fresh Flow receives, and it opens with the
/// harness's native skill invocation: Claude reads stacked `/name` commands at
/// the head of a block, Codex reads `$name` mentions beside its typed skill
/// inputs. Sources are named by absolute path, never inlined.
///
/// A Claude prompt is one line of at most [`ClaudeFirstLine::LIMIT`]
/// characters: the stacked commands in profile order, then one instruction
/// sentence naming the system-prompt bundle to read, any skills past the
/// stack, the goal and any sources, then the receipt request. Everything else
/// lives in the bundle and the skills. A profile whose line would break or
/// run past the limit is refused, never truncated.
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
pub struct LaunchReceipt;

impl LaunchReceipt {
    pub const MARKER: &'static str = "FLOW_LAUNCH_RECEIPT_V2";

    /// The fixed footer that follows every composed body of the harness.
    /// Claude's footer continues its one line; Codex's is its own paragraph.
    pub fn footer_for(harness: &HarnessKind) -> String {
        match harness {
            HarnessKind::Claude => format!(
                " When every skill has loaded, reply once with exactly {} and nothing else.",
                Self::MARKER
            ),
            HarnessKind::Codex => format!(
                "\n\nWhen every skill has loaded, reply once with exactly this line and nothing else:\n{}",
                Self::MARKER
            ),
        }
    }
}

/// The one line a Claude first prompt must be.
///
/// Claude Code wraps a submission as `<pasted_content>` when it is longer
/// than 800 characters on one line, or when it has four or more lines, and a
/// wrapped block expands no command (flows/e51411 pasted-content-threshold:
/// 800 stayed plain, 801 wrapped). Length is counted in UTF-16 code units,
/// the terminal input's own string length, which is never less than the
/// character count.
pub struct ClaudeFirstLine;

impl ClaudeFirstLine {
    pub const LIMIT: usize = 800;

    pub fn length(text: &str) -> usize {
        text.encode_utf16().count()
    }

    /// Whether `text` can be typed as one Claude submission unwrapped.
    pub fn fits(text: &str) -> bool {
        !text.contains(['\r', '\n']) && Self::length(text) <= Self::LIMIT
    }
}

/// How many `/name` commands Claude loads from the head of one prompt.
///
/// Claude Code 2.1.280 loads stacked head commands until its stack limit,
/// then logs "Stacked command limit (5) reached — remaining input passed as
/// arguments" and passes every later `/name` as argument text, unloaded. A
/// launch therefore stacks at most this many commands and names any further
/// skill for the Skill tool. Every stacked command receives the same
/// argument: the text after the last stacked command.
pub struct ClaudeCommandStack;

impl ClaudeCommandStack {
    pub const LIMIT: usize = 5;

    /// The number of commands a launch with `skill_count` skills stacks.
    pub fn stacked(skill_count: usize) -> usize {
        skill_count.min(Self::LIMIT)
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
/// form: the first eight hex digits of its SHA-256. Eight digits keep it
/// distinct from a six-digit Flow ID, and the request ID itself never enters
/// the prompt.
pub trait NamesRemoteControl {
    fn remote_control_name(&self) -> String;
    fn remote_control_record(&self) -> String;
}

/// The launch request ID's short form, used where a per-Flow name is needed
/// before the Flow ID is claimed.
pub trait ShortensLaunchRequest {
    const SHORT_FORM_LENGTH: usize = 8;
    fn launch_request_short_form(&self) -> String;
}

impl ShortensLaunchRequest for LaunchProfile {
    fn launch_request_short_form(&self) -> String {
        let digest = format!(
            "{:x}",
            Sha256::digest(
                format!("flow-remote-control-v1\0{}", self.launch_request_id).as_bytes()
            )
        );
        digest[..Self::SHORT_FORM_LENGTH].to_owned()
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
    fn read(&self, source: &LaunchSource) -> Result<PathBuf, CompositionError>;
}

/// Reads the system-prompt bundle a Codex main Flow receives at the top of
/// its first block. Claude receives the same file as `--system-prompt-file`.
trait ReadsSystemPromptBundle {
    fn read_bundle(&self, profile: &LaunchProfile) -> Result<String, CompositionError>;
}

trait RendersLaunchProfile {
    fn render_native_head(&self, profile: &LaunchProfile, bundle: Option<&str>) -> String;
    fn render_claude_line(&self, profile: &LaunchProfile, sources: &[PathBuf]) -> String;
    fn render_body(
        &self,
        profile: &LaunchProfile,
        bundle: Option<&str>,
        sources: &[PathBuf],
    ) -> String;
}

trait HashesPromptBody {
    fn sha256(&self, bytes: &[u8]) -> String;
}

impl OpensLaunchComposer for LaunchComposer {
    fn at(source_root: impl Into<PathBuf>) -> Self {
        Self {
            source_root: source_root.into(),
        }
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
        let harness = &self.launch_profile.harness_kind;
        *text
            == format!(
                "{}{}",
                self.first_prompt_payload.first_prompt_body,
                LaunchReceipt::footer_for(harness)
            )
            && (*harness != HarnessKind::Claude || ClaudeFirstLine::fits(text))
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
        let relative = Path::new(&source.source_path);
        if relative.as_os_str().is_empty()
            || relative.is_absolute()
            || relative
                .components()
                .any(|component| !matches!(component, Component::Normal(_) | Component::CurDir))
        {
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
        let candidate = root.join(relative);
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
        Ok(text.to_owned())
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
                .iter()
                .take(ClaudeCommandStack::LIMIT)
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
    /// the bundle to read, the skills past the stack, the goal, and the
    /// sources. Role, model, effort and remote control reach Claude through
    /// its argv and the bundle, and stay in the store.
    fn render_claude_line(&self, profile: &LaunchProfile, sources: &[PathBuf]) -> String {
        let mut line = self.render_native_head(profile, None);
        line.push_str(&format!(
            "Read {} for your launch mode",
            profile.system_prompt_bundle_file
        ));
        if profile.skill_name_vector.len() > ClaudeCommandStack::LIMIT {
            line.push_str(&format!(
                ", load {} through the Skill tool in this order",
                profile.skill_name_vector[ClaudeCommandStack::LIMIT..].join(", ")
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

    fn render_body(
        &self,
        profile: &LaunchProfile,
        bundle: Option<&str>,
        sources: &[PathBuf],
    ) -> String {
        if profile.harness_kind == HarnessKind::Claude {
            return self.render_claude_line(profile, sources);
        }
        let predecessor = profile.flow_id_option.as_deref().unwrap_or("none");
        let remembered = profile
            .remembered_flow_vector
            .iter()
            .map(|flow| format!("{}@{}", flow.flow_id, flow.remembering_depth))
            .collect::<Vec<_>>()
            .join(", ");
        let mut body = self.render_native_head(profile, bundle);
        body.push_str(&format!(
            "# Flow launch\n\nRole: {} {}\nHarness: Codex\nModel: {}\nEffort: {}\nPredecessor: {}\nRemembered flows: {}\nHerdr session: {}\nRemote control: {}\n",
            profile.aspect_name(),
            profile.power_name(),
            profile.model_name,
            profile.effort,
            predecessor,
            remembered,
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
            HarnessKind::Codex => Some(self.read_bundle(profile)?),
            HarnessKind::Claude => None,
        };
        let body = self.render_body(profile, bundle.as_deref(), &sources);
        let prompt_sha256 = self.sha256(body.as_bytes());
        let target_receipt_request = TargetReceiptRequest {
            launch_request_id: profile.launch_request_id.clone(),
            prompt_sha256: prompt_sha256.clone(),
        };
        let first_prompt_text = format!(
            "{}{}",
            body,
            LaunchReceipt::footer_for(&profile.harness_kind)
        );
        if profile.harness_kind == HarnessKind::Claude {
            if first_prompt_text.contains(['\r', '\n']) {
                return Err(CompositionError::ClaudeFirstLineBroken);
            }
            let length = ClaudeFirstLine::length(&first_prompt_text);
            if length > ClaudeFirstLine::LIMIT {
                return Err(CompositionError::ClaudeFirstLineTooLong(length));
            }
        }
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
        ClaudeCommandStack, ClaudeFirstLine, ComposesLaunch, CompositionError, LaunchComposer,
        NamesRemoteControl, OpensLaunchComposer, ValidatesComposedPrompt,
    };
    use signal_flow::{
        FlowAspect, HarnessKind, LaunchProfile, LaunchSource, PowerLevel, RememberedFlow,
    };
    use std::fs;

    trait BuildsProfile {
        fn profile(&self, sources: Vec<LaunchSource>) -> LaunchProfile;
    }

    impl BuildsProfile for tempfile::TempDir {
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

        let composed = LaunchComposer::at(root.path()).compose(&profile).unwrap();
        let canonical = root.path().canonicalize().unwrap();
        let expected = format!(
            concat!(
                "fixture bundle\n\n",
                "$spirit\n",
                "$main-flow\n\n",
                "# Flow launch\n\n",
                "Role: Field High\n",
                "Harness: Codex\n",
                "Model: gpt-6-astra\n",
                "Effort: medium\n",
                "Predecessor: 1b8ac0\n",
                "Remembered flows: 836818@1\n",
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
            let composed = LaunchComposer::at(root.path()).compose(&profile).unwrap();
            let text = composed.first_prompt_payload.first_prompt_text.as_str();
            assert!(!text.has_long_hex_run(), "{text}");
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
        let composed = LaunchComposer::at(root.path()).compose(&profile).unwrap();
        let body = composed.first_prompt_payload.first_prompt_body.as_str();
        assert_eq!(
            body,
            format!(
                "/spirit /main-flow Read {} for your launch mode, then: Carry the bounded task.",
                profile.system_prompt_bundle_file
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
        let composed = LaunchComposer::at(root.path()).compose(&profile).unwrap();
        let body = composed.first_prompt_payload.first_prompt_body.as_str();
        let head = body.split(" Read ").next().unwrap();
        let commands = head.split(' ').collect::<Vec<_>>();
        assert!(commands.len() <= ClaudeCommandStack::LIMIT, "{head}");
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
        assert!(ClaudeFirstLine::fits(
            &composed.first_prompt_payload.first_prompt_text
        ));
        assert!(!body.contains("/messaging"));
        assert!(!body.contains("/datom"));
        assert!(composed.has_canonical_first_prompt());
    }

    #[test]
    fn claude_launch_records_its_remote_control_flag() {
        let root = tempfile::tempdir().unwrap();
        let mut profile = root.profile(vec![]);
        profile.harness_kind = HarnessKind::Claude;
        let composed = LaunchComposer::at(root.path()).compose(&profile).unwrap();
        let name = profile.remote_control_name();
        assert_eq!(name.len(), "flow-".len() + 8);
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
    fn claude_prompt_is_one_line_of_at_most_800_characters() {
        let root = tempfile::tempdir().unwrap();
        fs::write(root.path().join("first.md"), b"first line\n\nfinal line\n").unwrap();
        let mut profile = root.profile(vec![LaunchSource {
            source_path: "first.md".into(),
            source_sha256: "56061831006650848af73c1976eef98f21be2d49f7f611f4dd9eb6d845ba0b1e"
                .into(),
        }]);
        profile.harness_kind = HarnessKind::Claude;
        let composed = LaunchComposer::at(root.path()).compose(&profile).unwrap();
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
    fn claude_line_at_the_limit_is_kept_and_one_past_it_is_refused() {
        let root = tempfile::tempdir().unwrap();
        let mut profile = root.profile(vec![]);
        profile.harness_kind = HarnessKind::Claude;
        profile.instruction_prompt = String::new();
        let empty = LaunchComposer::at(root.path()).compose(&profile).unwrap();
        let room = 800 - empty.first_prompt_payload.first_prompt_text.chars().count();
        profile.instruction_prompt = "x".repeat(room);
        let exact = LaunchComposer::at(root.path()).compose(&profile).unwrap();
        assert_eq!(
            exact.first_prompt_payload.first_prompt_text.chars().count(),
            800
        );
        assert!(exact.has_canonical_first_prompt());
        profile.instruction_prompt.push('x');
        assert_eq!(
            LaunchComposer::at(root.path()).compose(&profile),
            Err(CompositionError::ClaudeFirstLineTooLong(801))
        );
    }

    #[test]
    fn claude_instruction_that_breaks_or_overruns_the_line_is_refused() {
        let root = tempfile::tempdir().unwrap();
        let mut profile = root.profile(vec![]);
        profile.harness_kind = HarnessKind::Claude;
        profile.instruction_prompt = "Carry the task.\nThen report.".into();
        assert_eq!(
            LaunchComposer::at(root.path()).compose(&profile),
            Err(CompositionError::ClaudeFirstLineBroken)
        );
        profile.instruction_prompt = "Carry the bounded task. ".repeat(40);
        assert!(matches!(
            LaunchComposer::at(root.path()).compose(&profile),
            Err(CompositionError::ClaudeFirstLineTooLong(length)) if length > 800
        ));
        // Codex has no one-line limit: the same instruction composes there.
        profile.harness_kind = HarnessKind::Codex;
        assert!(LaunchComposer::at(root.path()).compose(&profile).is_ok());
    }

    #[test]
    fn claude_canonical_check_refuses_a_line_that_is_no_longer_one_line() {
        let root = tempfile::tempdir().unwrap();
        let mut profile = root.profile(vec![]);
        profile.harness_kind = HarnessKind::Claude;
        let mut composed = LaunchComposer::at(root.path()).compose(&profile).unwrap();
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
        composed.first_prompt_payload.first_prompt_text = format!(
            "{body}{}",
            super::LaunchReceipt::footer_for(&HarnessKind::Claude)
        );
        assert!(!composed.has_canonical_first_prompt());
    }

    #[test]
    fn malformed_full_text_is_not_canonical_when_body_hash_is_unchanged() {
        let root = tempfile::tempdir().unwrap();
        let mut composed = LaunchComposer::at(root.path())
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

    #[test]
    fn rejects_a_missing_source_before_composition() {
        let root = tempfile::tempdir().unwrap();
        let profile = root.profile(vec![LaunchSource {
            source_path: "missing.md".into(),
            source_sha256: "0000000000000000000000000000000000000000000000000000000000000000"
                .into(),
        }]);

        assert_eq!(
            LaunchComposer::at(root.path()).compose(&profile),
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
            LaunchComposer::at(root.path()).compose(&profile),
            Err(CompositionError::SourceHashMismatch("changed.md".into()))
        );
    }

    #[test]
    fn profile_keeps_skill_names_without_skill_bodies() {
        let root = tempfile::tempdir().unwrap();
        let composed = LaunchComposer::at(root.path())
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
        let composed = LaunchComposer::at(root.path()).compose(&profile).unwrap();
        let text = composed.first_prompt_payload.first_prompt_text.as_str();
        assert!(
            text.starts_with("# Main-flow mode\n\nKeep the stock base.\n\n$spirit\n$main-flow\n\n"),
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
                LaunchComposer::at(root.path()).compose(&profile),
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
        let composed = LaunchComposer::at(root.path()).compose(&profile).unwrap();
        assert!(
            composed
                .first_prompt_payload
                .first_prompt_body
                .starts_with("fixture bundle\n\n$main-flow\n$spirit\n\n# Flow launch\n")
        );
    }
}
