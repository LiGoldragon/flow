//! The canonical native title of a started Flow.
//!
//! A title is a Datom struct, `<Aspect>.{ <Model> <FlowId> }`, built from
//! the launch profile's explicit aspect, the display name of its exact model
//! identifier, and the Flow ID claimed from the native session:
//! `Psyche.{ Fable 38de5b }`. Power never enters the title. An unmapped
//! model identifier is refused; no alias and no fallback is accepted.

use signal_flow::{FlowAspect, LaunchProfile};
use thiserror::Error;

/// Exact native model identifiers and their display names. This mirrors the
/// workspace's authoritative model-display map
/// (`config/model-display-names.json`, version 1); an identifier absent here
/// has no title.
pub trait NamesModel {
    const MODEL_DISPLAY_NAMES: &'static [(&'static str, &'static str)] = &[
        ("gpt-6-astra", "Astra"),
        ("gpt-6-sol", "Sol"),
        ("gpt-6-luna", "Luna"),
        ("gpt-5.6-sol", "Sol"),
        ("gpt-5.6-terra", "Terra"),
        ("gpt-5.6-luna", "Luna"),
        ("claude-fable-5-1", "Fable"),
        ("claude-fable-5-1[1m]", "Fable"),
        ("claude-opus-5-5", "Opus"),
        ("claude-opus-5", "Opus 5"),
        ("claude-opus-4-7", "OldOpus 4.7"),
        ("claude-opus-4-7[1m]", "OldOpus 4.7 1m"),
        ("claude-opus-4-6", "OldOpus 4.6"),
        ("claude-opus-4-6[1m]", "OldOpus 4.6 1m"),
        ("claude-sonnet-5", "Sonnet 5"),
        ("claude-haiku-4-5-20251001", "Haiku 4.5"),
    ];

    /// The display name of this exact model identifier.
    fn model_display_name(&self) -> Option<&'static str>;
}

impl NamesModel for str {
    fn model_display_name(&self) -> Option<&'static str> {
        Self::MODEL_DISPLAY_NAMES
            .iter()
            .find(|(identifier, _)| *identifier == self)
            .map(|(_, name)| *name)
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum TitleRefused {
    #[error("unmapped exact native model identifier: {0}")]
    UnmappedModel(String),
    #[error("a title requires the exact six-character Flow ID")]
    InvalidFlowId,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NativeTitle(String);

/// The title a launch profile gives the Flow it starts.
pub trait TitlesFlow {
    fn native_title(&self, flow_id: &str) -> Result<NativeTitle, TitleRefused>;
}

impl TitlesFlow for LaunchProfile {
    fn native_title(&self, flow_id: &str) -> Result<NativeTitle, TitleRefused> {
        if flow_id.len() != 6
            || !flow_id
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(TitleRefused::InvalidFlowId);
        }
        let model = self
            .model_name
            .model_display_name()
            .ok_or_else(|| TitleRefused::UnmappedModel(self.model_name.clone()))?;
        let aspect = match self.flow_aspect {
            FlowAspect::Psyche => "Psyche",
            FlowAspect::Mind => "Mind",
            FlowAspect::Field => "Field",
        };
        Ok(NativeTitle(format!("{aspect}.{{ {model} {flow_id} }}")))
    }
}

/// The title as written.
pub trait ShowsNativeTitle {
    fn as_str(&self) -> &str;
}

impl ShowsNativeTitle for NativeTitle {
    fn as_str(&self) -> &str {
        &self.0
    }
}

#[cfg(test)]
mod tests {
    use super::{ShowsNativeTitle, TitleRefused, TitlesFlow};
    use signal_flow::{FlowAspect, HarnessKind, LaunchProfile, PowerLevel};

    fn profile(aspect: FlowAspect, model: &str) -> LaunchProfile {
        LaunchProfile {
            launch_request_id: "launch-title".into(),
            launch_source_vector: vec![],
            skill_name_vector: vec![],
            flow_aspect: aspect,
            power_level: PowerLevel::High,
            harness_kind: HarnessKind::Claude,
            model_name: model.into(),
            effort: "high".into(),
            flow_id_option: None,
            remembered_flow_vector: vec![],
            herdr_session_name: "session".into(),
            system_prompt_bundle_file: "/tmp/bundle".into(),
            instruction_prompt: "work".into(),
        }
    }

    #[test]
    fn title_is_the_datom_of_aspect_model_and_flow() {
        assert_eq!(
            profile(FlowAspect::Psyche, "claude-fable-5-1")
                .native_title("38de5b")
                .unwrap()
                .as_str(),
            "Psyche.{ Fable 38de5b }"
        );
        assert_eq!(
            profile(FlowAspect::Mind, "gpt-6-sol")
                .native_title("00f95a")
                .unwrap()
                .as_str(),
            "Mind.{ Sol 00f95a }"
        );
        // Power never enters the title.
        let mut low = profile(FlowAspect::Field, "gpt-6-luna");
        low.power_level = PowerLevel::UltraLow;
        assert_eq!(
            low.native_title("e71dab").unwrap().as_str(),
            "Field.{ Luna e71dab }"
        );
    }

    #[test]
    fn unmapped_models_and_malformed_flow_ids_are_refused() {
        assert_eq!(
            profile(FlowAspect::Psyche, "fable").native_title("38de5b"),
            Err(TitleRefused::UnmappedModel("fable".into()))
        );
        for flow_id in ["38DE5B", "38de5", "38de5bb", "38de5g", ""] {
            assert_eq!(
                profile(FlowAspect::Psyche, "claude-fable-5-1").native_title(flow_id),
                Err(TitleRefused::InvalidFlowId)
            );
        }
    }
}
