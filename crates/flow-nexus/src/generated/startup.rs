//! Startup's generated contract and the canonical startup refusal.
//!
//! The schema-generated items live apart so this authored module can keep
//! the public `flow_nexus::generated::startup::StartRefusal` path bound to
//! its single owner in flow-ethos.

pub use flow_ethos::{StartRefusal, StoreRefusal};
pub use super::startup_generated::*;

#[cfg(test)]
mod tests {
    use super::StartRefusal;

    #[test]
    fn startup_refusal_is_the_flow_ethos_type() {
        fn canonical(_: flow_ethos::StartRefusal) {}
        let _: fn(StartRefusal) = canonical;
    }
}
