pub mod generated;

pub use generated::delivery::*;
pub use generated::library::*;

pub const ETHOS: &str = include_str!("../ethos/library.ethos");
