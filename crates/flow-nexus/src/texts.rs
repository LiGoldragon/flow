//! Rendering of authored text templates from the committed configuration.

use meta_signal_flow::{Piece, Slot, Template};

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum TextError {
    #[error("text template slot {0:?} has no value in this context")]
    Unresolved(Slot),
}

pub(crate) fn render_template(
    template: &Template,
    mut value: impl FnMut(&Slot) -> Option<String>,
) -> Result<String, TextError> {
    let mut rendered = String::new();
    for piece in template {
        match piece {
            Piece::Text(text) => rendered.push_str(text),
            Piece::Slot(slot) => {
                rendered.push_str(&value(slot).ok_or_else(|| TextError::Unresolved(slot.clone()))?)
            }
            Piece::Flagged(flagged) => {
                if let Some(value) = value(&flagged.slot) {
                    rendered.push_str(&flagged.first_string);
                    rendered.push_str(&value);
                    rendered.push_str(&flagged.second_string);
                }
            }
            Piece::Joined(joined) => {
                let value = value(&joined.slot)
                    .ok_or_else(|| TextError::Unresolved(joined.slot.clone()))?;
                rendered.push_str(&joined.string);
                rendered.push_str(&value);
            }
        }
    }
    Ok(rendered)
}

#[cfg(test)]
mod tests {
    use super::{TextError, render_template};
    use meta_signal_flow::{Flagged_Data, Piece, Slot};

    #[test]
    fn flagged_guards_both_strings_and_keeps_zero_present() {
        let template = vec![Piece::Flagged(Flagged_Data {
            first_string: "before".into(),
            slot: Slot::ContextSize,
            second_string: "after".into(),
        })];
        assert_eq!(render_template(&template, |_| None).unwrap(), "");
        assert_eq!(
            render_template(&template, |_| Some("0".into())).unwrap(),
            "before0after"
        );
        let empty_edges = vec![Piece::Flagged(Flagged_Data {
            first_string: String::new(),
            slot: Slot::ContextSize,
            second_string: String::new(),
        })];
        assert_eq!(
            render_template(&empty_edges, |_| Some("7".into())).unwrap(),
            "7"
        );
    }

    #[test]
    fn a_required_slot_without_a_value_is_not_silently_left_out() {
        let template = vec![Piece::Slot(Slot::TranscriptFile)];
        assert_eq!(
            render_template(&template, |_| None),
            Err(TextError::Unresolved(Slot::TranscriptFile))
        );
    }
}
