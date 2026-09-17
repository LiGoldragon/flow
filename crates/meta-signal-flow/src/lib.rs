//! Owner Flow Signal contract; configuration is only accepted on the meta socket.
use rkyv::{Archive, Deserialize, Serialize};
/// The owner wire contract version is this crate's own semver.
pub const WIRE_VERSION: &str = env!("CARGO_PKG_VERSION");
#[derive(Archive, Deserialize, Serialize, Debug, Clone, PartialEq, Eq)]
#[cfg_attr(
    feature = "datom",
    derive(datom_codec::Datomizable, datom_codec::Composing)
)]
pub struct Configuration {
    pub ordinary_socket: String,
    pub meta_socket: String,
}
#[derive(Archive, Deserialize, Serialize, Debug, Clone, PartialEq, Eq)]
#[cfg_attr(
    feature = "datom",
    derive(datom_codec::Datomizable, datom_codec::Composing)
)]
pub enum Query {
    Configure(Configuration),
}
#[derive(Archive, Deserialize, Serialize, Debug, Clone, PartialEq, Eq)]
#[cfg_attr(
    feature = "datom",
    derive(datom_codec::Datomizable, datom_codec::Composing)
)]
pub enum Response {
    Configured(Configuration),
}

#[cfg(all(test, feature = "datom"))]
mod tests {
    use super::{Configuration, Query, Response, WIRE_VERSION};
    use datom_codec::{Actualizing, Budget, Datomizable, Potential};
    use protos::{Protosizable, ReaderBudget, Textualizable};

    #[test]
    fn configure_example_parses_and_round_trips() {
        let text = "Configure.{ { /tmp/flow.sock /tmp/flow-meta.sock } }";
        let mut budget = Budget {
            remaining: 1024,
            reader: ReaderBudget { remaining: 1024 },
            depth: 0,
            maximum_depth: 1024,
        };
        let query = Potential::<Query>::from(text)
            .actualize(&mut budget)
            .expect("configure parses");
        assert_eq!(query.datomize(vec![]).protosize().textualize(), text);
    }

    #[test]
    fn configured_reply_round_trips_through_archived_bytes() {
        let response = Response::Configured(Configuration {
            ordinary_socket: "/tmp/flow.sock".into(),
            meta_socket: "/tmp/flow-meta.sock".into(),
        });
        let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&response).expect("archive");
        assert_eq!(
            rkyv::from_bytes::<Response, rkyv::rancor::Error>(&bytes).expect("restore"),
            response
        );
        assert_eq!(WIRE_VERSION, "0.1.0");
    }
}
