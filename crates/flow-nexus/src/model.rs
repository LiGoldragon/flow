//! What Flow reads off an Address's Model: the effort a seat of it runs at
//! and the name the datom writes it by. No model ranks above another.

use signal_flow::Model;

/// The effort a seat of a model runs at, and the model's datom name.
pub trait RunsModel {
    /// Low for Terra, medium for every other model.
    fn effort(&self) -> &'static str;
    /// The model's name, as the datom writes it.
    fn name(&self) -> &'static str;
}

impl RunsModel for Model {
    fn effort(&self) -> &'static str {
        match self {
            Model::Terra => "low",
            _ => "medium",
        }
    }

    fn name(&self) -> &'static str {
        match self {
            Model::Fable => "Fable",
            Model::Opus => "Opus",
            Model::Sonnet => "Sonnet",
            Model::Haiku => "Haiku",
            Model::Astra => "Astra",
            Model::Sol => "Sol",
            Model::Luna => "Luna",
            Model::Terra => "Terra",
        }
    }
}
