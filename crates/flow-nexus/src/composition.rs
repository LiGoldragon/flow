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
}

pub struct LaunchComposer {
    source_root: PathBuf,
}

pub trait OpensLaunchComposer {
    fn at(source_root: impl Into<PathBuf>) -> Self;
}

/// Composes one prompt from a profile and a configured source root.
///
/// `prompt_sha256` is SHA-256 over the exact UTF-8 bytes in
/// `first_prompt_body`. `first_prompt_text` is that body followed by the
/// deterministic target-receipt footer, whose embedded digest is therefore
/// outside the digest preimage. The adapter sends `first_prompt_text` exactly.
pub trait ComposesLaunch {
    fn compose(&self, profile: &LaunchProfile) -> Result<ComposedLaunch, CompositionError>;
}

trait ValidatesLaunchProfile {
    fn validate(&self, profile: &LaunchProfile) -> Result<(), CompositionError>;
}

trait ReadsLaunchSource {
    fn read(&self, source: &LaunchSource) -> Result<Vec<u8>, CompositionError>;
}

trait RendersLaunchProfile {
    fn render_body(
        &self,
        profile: &LaunchProfile,
        sources: &[Vec<u8>],
    ) -> Result<String, CompositionError>;
    fn render_receipt_footer(&self, request: &TargetReceiptRequest) -> String;
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
    fn read(&self, source: &LaunchSource) -> Result<Vec<u8>, CompositionError> {
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
        Ok(bytes)
    }
}

impl RendersLaunchProfile for LaunchComposer {
    fn render_body(
        &self,
        profile: &LaunchProfile,
        sources: &[Vec<u8>],
    ) -> Result<String, CompositionError> {
        let aspect = match profile.flow_aspect {
            FlowAspect::Psyche => "Psyche",
            FlowAspect::Mind => "Mind",
            FlowAspect::Field => "Field",
        };
        let power = match profile.power_level {
            PowerLevel::High => "High",
            PowerLevel::Medium => "Medium",
            PowerLevel::Low => "Low",
            PowerLevel::UltraLow => "Ultra Low",
        };
        let harness = match profile.harness_kind {
            HarnessKind::Codex => "Codex",
            HarnessKind::Claude => "Claude",
        };
        let predecessor = profile.flow_id_option.as_deref().unwrap_or("none");
        let skills = profile.skill_name_vector.join(", ");
        let remembered = profile
            .remembered_flow_vector
            .iter()
            .map(|flow| format!("{}@{}", flow.flow_id, flow.remembering_depth))
            .collect::<Vec<_>>()
            .join(", ");

        let mut body = format!(
            "# Flow launch\n\nLaunch request: {}\nRole: {} {}\nHarness: {}\nModel: {}\nEffort: {}\nPredecessor: {}\nRemembered flows: {}\nHerdr session: {}\nLoadable skills: {}\n\n{}",
            profile.launch_request_id,
            aspect,
            power,
            harness,
            profile.model_name,
            profile.effort,
            predecessor,
            remembered,
            profile.herdr_session_name,
            skills,
            profile.instruction_prompt,
        );
        for (source, bytes) in profile.launch_source_vector.iter().zip(sources) {
            let text = std::str::from_utf8(bytes)
                .map_err(|_| CompositionError::NonUtf8Source(source.source_path.clone()))?;
            body.push_str("\n\n## Source: `");
            body.push_str(&source.source_path);
            body.push_str("`\n\n");
            body.push_str(text);
        }
        Ok(body)
    }

    fn render_receipt_footer(&self, request: &TargetReceiptRequest) -> String {
        format!(
            "\n\n## Target receipt request\n\nReply once with exactly this single line and no trailing newline:\nFLOW_LAUNCH_RECEIPT_V1 launch_request_id={} prompt_body_sha256={}",
            request.launch_request_id, request.prompt_sha256
        )
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
        let body = self.render_body(profile, &sources)?;
        let prompt_sha256 = self.sha256(body.as_bytes());
        let target_receipt_request = TargetReceiptRequest {
            launch_request_id: profile.launch_request_id.clone(),
            prompt_sha256: prompt_sha256.clone(),
        };
        let first_prompt_text = format!(
            "{}{}",
            body,
            self.render_receipt_footer(&target_receipt_request)
        );
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
    use super::{ComposesLaunch, CompositionError, LaunchComposer, OpensLaunchComposer};
    use signal_flow::{
        FlowAspect, HarnessKind, LaunchProfile, LaunchSource, PowerLevel, RememberedFlow,
    };
    use std::fs;

    trait BuildsProfile {
        fn profile(&self, sources: Vec<LaunchSource>) -> LaunchProfile;
    }

    impl BuildsProfile for tempfile::TempDir {
        fn profile(&self, sources: Vec<LaunchSource>) -> LaunchProfile {
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
                instruction_prompt: "Carry the bounded task.".into(),
            }
        }
    }

    #[test]
    fn preserves_exact_source_bytes_and_declared_order() {
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
        let expected = concat!(
            "# Flow launch\n\n",
            "Launch request: launch-42\n",
            "Role: Field High\n",
            "Harness: Codex\n",
            "Model: gpt-6-astra\n",
            "Effort: medium\n",
            "Predecessor: 1b8ac0\n",
            "Remembered flows: 836818@1\n",
            "Herdr session: messaging-build\n",
            "Loadable skills: spirit, main-flow\n\n",
            "Carry the bounded task.",
            "\n\n## Source: `first.md`\n\n",
            "first line\n\nfinal line\n",
            "\n\n## Source: `second.md`\n\n",
            "second source has no trailing newline"
        );
        assert_eq!(composed.first_prompt_payload.first_prompt_body, expected);
        assert_eq!(
            composed.first_prompt_payload.prompt_sha256,
            "0b05f0741623b2544845941417d0c011859132547590385ca02cc694364c1df2"
        );
        assert!(
            composed
                .first_prompt_payload
                .first_prompt_text
                .starts_with(expected)
        );
        assert!(composed
            .first_prompt_payload
            .first_prompt_text
            .ends_with(&format!(
                "## Target receipt request\n\nReply once with exactly this single line and no trailing newline:\nFLOW_LAUNCH_RECEIPT_V1 launch_request_id=launch-42 prompt_body_sha256={}",
                composed.first_prompt_payload.prompt_sha256
            )));
        assert_eq!(
            composed.target_receipt_request.prompt_sha256,
            composed.first_prompt_payload.prompt_sha256
        );
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
}
