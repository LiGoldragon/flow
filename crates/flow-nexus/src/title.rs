//! The canonical native title of a started Flow.
//!
//! A title is the Datom struct of the seat's Address and the Flow ID claimed
//! from its native session, `{ <Aspect> <Topic> <Layer> <FlowId> }`, the topic
//! exactly as the metaflow's Address holds it and always shown:
//! `{ Field flow Quaternary 2712d3 }`, `{ Field flowNexus Primary 2712d3 }`.
//! It is the seat's own datom, so the
//! model a seat runs never enters it, and no model identifier can refuse it.

use datom_codec::Datomizable;
use protos::{Compactable, Protosizable};
use signal_flow::LaunchProfile;
use thiserror::Error;

#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub enum TitleRefused {
    #[error("a title requires the exact six-character Flow ID")]
    InvalidFlowId,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NativeTitle(String);

/// The seat a title names, in the order its datom is written.
#[derive(datom_codec::Datomizable)]
struct TitledSeat {
    aspect: String,
    topic: String,
    layer: String,
    flow_id: String,
}

/// A typed Address part as its title spelling. The casing of aspect and
/// layer in every title is the one line marked below.
trait DisplaySpelling {
    fn display_spelling(&self) -> String;
}

impl<Part: Datomizable> DisplaySpelling for Part {
    fn display_spelling(&self) -> String {
        // casing choice: append `.to_lowercase()` for lowercase titles
        self.datomize(Vec::new()).protosize().compact()
    }
}

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
        let seat = TitledSeat {
            aspect: self.address.aspect.display_spelling(),
            topic: self.address.topic.clone(),
            layer: self.address.layer.display_spelling(),
            flow_id: flow_id.into(),
        };
        Ok(NativeTitle(seat.datomize(Vec::new()).protosize().compact()))
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
    use signal_flow::{Aspect, LaunchProfile, Layer};

    fn profile(aspect: Aspect, layer: Layer, model: &str) -> LaunchProfile {
        LaunchProfile {
            launch_request_id: "launch-title".into(),
            system_modules: vec!["spirit-core".into(), "intent-flow".into()],
            user_modules: vec!["operation-main-flow".into()],
            address: signal_flow::Address {
                aspect,
                topic: "flow".into(),
                layer,
            },
            model_name: model.into(),
            effort: "high".into(),
            flow_id_option: None,
            remembering: signal_flow::Remembering::Predecessors(0),
            herdr_session_name: "session".into(),
            leave_outs_option: None,
            flow_kind: signal_flow::FlowKind::Regular,
            tool_set: signal_flow::ToolSet::Default,
            instruction_prompt: "work".into(),
        }
    }

    #[test]
    fn title_is_the_datom_of_the_address_and_flow() {
        // A model alias names no title: the launch that failed on `haiku`.
        assert_eq!(
            profile(Aspect::Field, Layer::Quaternary, "haiku")
                .native_title("2712d3")
                .unwrap()
                .as_str(),
            "{ Field flow Quaternary 2712d3 }"
        );
        assert_eq!(
            profile(Aspect::Psyche, Layer::Primary, "claude-fable-5-1")
                .native_title("38de5b")
                .unwrap()
                .as_str(),
            "{ Psyche flow Primary 38de5b }"
        );
        // Power and model never enter the title.
        let low = profile(Aspect::Mind, Layer::Secondary, "gpt-6-sol");
        assert_eq!(
            low.native_title("00f95a").unwrap().as_str(),
            "{ Mind flow Secondary 00f95a }"
        );
    }

    #[test]
    fn the_topic_is_shown_exactly_as_the_address_holds_it() {
        for topic in ["flowNexus", "PrimaryCommitting", "Flow"] {
            let mut launch = profile(Aspect::Mind, Layer::Primary, "haiku");
            launch.address.topic = topic.into();
            assert_eq!(
                launch.native_title("2712d3").unwrap().as_str(),
                format!("{{ Mind {topic} Primary 2712d3 }}")
            );
        }
    }

    #[test]
    fn malformed_flow_ids_are_refused() {
        for flow_id in ["38DE5B", "38de5", "38de5bb", "38de5g", ""] {
            assert_eq!(
                profile(Aspect::Psyche, Layer::Primary, "haiku").native_title(flow_id),
                Err(TitleRefused::InvalidFlowId)
            );
        }
    }
}
