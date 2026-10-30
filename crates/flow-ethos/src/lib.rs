pub mod generated;

pub use generated::delivery::*;
pub use generated::library::*;
// The ruled message contract, a namespace of its own until the store
// migration retires the shapes library.rs gives the stored records.
pub use generated::message;

pub const ETHOS: &str = include_str!("../ethos/library.ethos");
