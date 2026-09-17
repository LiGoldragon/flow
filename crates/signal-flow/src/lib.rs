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
pub enum Response {
    Started { flow_id: String, origin: Origin },
    Restarted { flow_id: String, generation: u64 },
    StartRejected,
    RestartRejected,
}

#[cfg(feature = "datom")]
impl datom_codec::Datomizable for Response {
    fn datomize(&self, at: datom_codec::Path) -> datom_codec::Datom {
        use datom_codec::{Pathing, Variantizing};
        match self {
            Self::Started { flow_id, origin } => (flow_id, origin)
                .datomize(at.child(1))
                .named_variant(at, "Started"),
            Self::Restarted {
                flow_id,
                generation,
            } => (flow_id, *generation as i64)
                .datomize(at.child(1))
                .named_variant(at, "Restarted"),
            Self::StartRejected => datom_codec::Datom {
                path: at,
                form: datom_codec::Form::Bare("StartRejected".into()),
            },
            Self::RestartRejected => datom_codec::Datom {
                path: at,
                form: datom_codec::Form::Bare("RestartRejected".into()),
            },
        }
    }
}

#[cfg(feature = "datom")]
impl datom_codec::Composing for Response {
    fn compose(
        datom: &datom_codec::Datom,
        budget: &mut datom_codec::Budget,
    ) -> Result<Self, datom_codec::Error> {
        use datom_codec::{Budgeting, Composable, ErrorKind, ErrorRaising, Variantizing};
        match &datom.form {
            datom_codec::Form::Bare(head) => {
                budget.spend(&datom.path)?;
                match head.as_str() {
                    "StartRejected" => Ok(Self::StartRejected),
                    "RestartRejected" => Ok(Self::RestartRejected),
                    found => Err(datom_codec::Error::composition(
                        datom.path.clone(),
                        ErrorKind::Variant {
                            expected: "Response".into(),
                            found: found.into(),
                        },
                    )),
                }
            }
            _ => {
                let (head, body) = datom.variant(budget, "Variant")?;
                match head {
                    "Started" => {
                        let (flow_id, origin) = body.compose_positions(budget)?;
                        Ok(Self::Started { flow_id, origin })
                    }
                    "Restarted" => {
                        let (flow_id, generation) =
                            body.compose_positions::<(String, i64)>(budget)?;
                        let generation = u64::try_from(generation).map_err(|_| {
                            datom_codec::Error::composition(
                                datom.path.clone(),
                                ErrorKind::Value {
                                    expected: "non-negative generation".into(),
                                    value: generation.to_string(),
                                },
                            )
                        })?;
                        Ok(Self::Restarted {
                            flow_id,
                            generation,
                        })
                    }
                    found => Err(datom_codec::Error::composition(
                        datom.path.clone(),
                        ErrorKind::Variant {
                            expected: "Response".into(),
                            found: found.into(),
                        },
                    )),
                }
            }
        }
    }
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
    fn every_reply_round_trips_through_archived_bytes_and_datom_text() {
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
            let text = response.datomize(vec![]).protosize().textualize();
            let restored = Potential::<Response>::from(text)
                .actualize(&mut ContractBudget.budget())
                .expect("reply datom restores");
            assert_eq!(restored, response);
        }
        assert_eq!(WIRE_VERSION, "0.1.0");
    }
}
