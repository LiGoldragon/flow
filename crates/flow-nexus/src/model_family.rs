//! What an Address's Model means to Flow: its family, its rank in that
//! family and what follows from the rank.
//!
//! The two families, each ordered from the dearest: Fable, Opus, Sonnet,
//! Haiku; Astra, Sol, Luna, Terra. Up is the model one rank before; the
//! first of a family has none above it. A launch's power is its rank.

use signal_flow::{Address, Aspect, Model, PowerLevel};

/// A model's place among the models.
pub trait RanksInFamily {
    /// The model one rank before this one in its family; None for Fable
    /// and Astra.
    fn above(&self) -> Option<Model>;
    /// The power a flow of this model launches with: its rank.
    fn power(&self) -> PowerLevel;
    /// The second model of this model's family: Opus or Sol.
    fn second(&self) -> Model;
    /// Whether this model is the first of its family: Fable or Astra.
    fn is_first(&self) -> bool;
    /// The effort a seat of this model runs at: low for Terra, medium for
    /// every other.
    fn effort(&self) -> &'static str;
    /// The model's name, as the datom writes it.
    fn name(&self) -> &'static str;
    /// The model of this one's rank in the family Flow ran the aspect on
    /// before 0.52.0: Claude in Psyche, OpenAI in Mind and Field.
    fn of_rank_in(&self, aspect: &Aspect) -> Model;
}

impl RanksInFamily for Model {
    fn above(&self) -> Option<Model> {
        match self {
            Model::Fable | Model::Astra => None,
            Model::Opus => Some(Model::Fable),
            Model::Sonnet => Some(Model::Opus),
            Model::Haiku => Some(Model::Sonnet),
            Model::Sol => Some(Model::Astra),
            Model::Luna => Some(Model::Sol),
            Model::Terra => Some(Model::Luna),
        }
    }

    fn power(&self) -> PowerLevel {
        match self {
            Model::Fable | Model::Astra => PowerLevel::High,
            Model::Opus | Model::Sol => PowerLevel::Medium,
            Model::Sonnet | Model::Luna => PowerLevel::Low,
            Model::Haiku | Model::Terra => PowerLevel::UltraLow,
        }
    }

    fn second(&self) -> Model {
        match self {
            Model::Fable | Model::Opus | Model::Sonnet | Model::Haiku => Model::Opus,
            Model::Astra | Model::Sol | Model::Luna | Model::Terra => Model::Sol,
        }
    }

    fn is_first(&self) -> bool {
        matches!(self, Model::Fable | Model::Astra)
    }

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

    fn of_rank_in(&self, aspect: &Aspect) -> Model {
        match (aspect, self) {
            (Aspect::Psyche, Model::Fable | Model::Astra) => Model::Fable,
            (Aspect::Psyche, Model::Opus | Model::Sol) => Model::Opus,
            (Aspect::Psyche, Model::Sonnet | Model::Luna) => Model::Sonnet,
            (Aspect::Psyche, Model::Haiku | Model::Terra) => Model::Haiku,
            (Aspect::Mind | Aspect::Field, Model::Fable | Model::Astra) => Model::Astra,
            (Aspect::Mind | Aspect::Field, Model::Opus | Model::Sol) => Model::Sol,
            (Aspect::Mind | Aspect::Field, Model::Sonnet | Model::Luna) => Model::Luna,
            (Aspect::Mind | Aspect::Field, Model::Haiku | Model::Terra) => Model::Terra,
        }
    }
}

/// An Address Flow archived before 0.52.0 carried a Layer where it now
/// carries a Model. Both archive as one byte in the same place, so an
/// archived Layer reads as the Claude model of its rank: Primary as Fable,
/// Secondary as Opus, Tertiary as Sonnet, Quaternary as Haiku. The model
/// it names is the one of that rank in its aspect's family: the Claude
/// family in Psyche, the OpenAI family in Mind and Field, as Flow's
/// declared models ran them.
pub trait NamesModelOfLayer {
    /// The Address this one names, read as a pre-0.52 Layer address.
    fn of_layer(&self) -> Address;
    /// The Layer word this Address's model held its place under before
    /// 0.52.0, read as a pre-0.52 Layer address: Primary for Fable, and so
    /// on. It names a record key Flow wrote then.
    fn layer_word(&self) -> &'static str;
}

impl NamesModelOfLayer for Address {
    fn of_layer(&self) -> Address {
        Address {
            aspect: self.aspect.clone(),
            topic: self.topic.clone(),
            model: self.model.of_rank_in(&self.aspect),
        }
    }

    fn layer_word(&self) -> &'static str {
        match &self.model {
            Model::Fable | Model::Astra => "Primary",
            Model::Opus | Model::Sol => "Secondary",
            Model::Sonnet | Model::Luna => "Tertiary",
            Model::Haiku | Model::Terra => "Quaternary",
        }
    }
}
