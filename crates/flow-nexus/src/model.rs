//! The name the datom writes each model by.

use signal_flow::Model;

/// The model's datom name.
pub trait RunsModel {
    /// The model's name, as the datom writes it.
    fn name(&self) -> &'static str;
}

impl RunsModel for Model {
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
