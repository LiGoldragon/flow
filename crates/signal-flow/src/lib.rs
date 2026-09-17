//! Ordinary Flow Signal contract. `ethos/signal.ethos` is its authored form.
use rkyv::{Archive, Deserialize, Serialize};
/// The wire contract version is this crate's own semver, never the Nexus's.
pub const WIRE_VERSION: &str = env!("CARGO_PKG_VERSION");
#[derive(Archive, Deserialize, Serialize, Debug, Clone, PartialEq, Eq)]
#[cfg_attr(
    feature = "datom",
    derive(datom_codec::Datomizable, datom_codec::Composing)
)]
pub struct Origin {
    pub parent_flow_id: String,
    pub session: String,
    pub turn: String,
}
#[derive(Archive, Deserialize, Serialize, Debug, Clone, PartialEq, Eq)]
#[cfg_attr(
    feature = "datom",
    derive(datom_codec::Datomizable, datom_codec::Composing)
)]
pub enum Query {
    Start {
        flow_type: String,
        goal: String,
        origin: Origin,
    },
    Restart {
        flow_id: String,
        authority_flow_id: String,
    },
}
#[derive(Archive, Deserialize, Serialize, Debug, Clone, PartialEq, Eq)]
#[cfg_attr(
    feature = "datom",
    derive(datom_codec::Datomizable, datom_codec::Composing)
)]
pub enum Response {
    Started { flow_id: String, origin: Origin },
    Restarted { flow_id: String, generation: u64 },
    StartRejected,
    RestartRejected,
}

#[cfg(all(test, feature = "datom"))]
mod tests {
    use super::{Origin, Query, Response, WIRE_VERSION};
    use datom_codec::{Actualizing, Budget, Datomizable, Potential};
    use protos::{Protosizable, ReaderBudget, Textualizable};

    struct ContractBudget;
    trait CreatesBudget {
        fn budget(&self) -> Budget;
    }
    impl CreatesBudget for ContractBudget {
        fn budget(&self) -> Budget {
            Budget {
                remaining: 1024,
                reader: ReaderBudget { remaining: 1024 },
                depth: 0,
                maximum_depth: 1024,
            }
        }
    }
    trait RoundTripsDatom {
        fn round_trip(&self, text: &str);
    }
    impl RoundTripsDatom for ContractBudget {
        fn round_trip(&self, text: &str) {
            let query = Potential::<Query>::from(text)
                .actualize(&mut self.budget())
                .expect("query parses");
            assert_eq!(query.datomize(vec![]).protosize().textualize(), text);
        }
    }

    #[test]
    fn declared_query_examples_parse_and_round_trip() {
        let budget = ContractBudget;
        budget.round_trip("Start.{ codex-medium «map the store» { parent session turn } }");
        budget.round_trip("Restart.{ flow-0000000000000001 parent }");
    }

    #[test]
    fn every_reply_round_trips_through_archived_bytes() {
        for response in [
            Response::Started {
                flow_id: "flow-1".into(),
                origin: Origin {
                    parent_flow_id: "parent".into(),
                    session: "session".into(),
                    turn: "turn".into(),
                },
            },
            Response::Restarted {
                flow_id: "flow-1".into(),
                generation: 2,
            },
            Response::StartRejected,
            Response::RestartRejected,
        ] {
            let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&response).expect("archive");
            assert_eq!(
                rkyv::from_bytes::<Response, rkyv::rancor::Error>(&bytes).expect("restore"),
                response
            );
        }
        assert_eq!(WIRE_VERSION, "0.1.0");
    }
}
