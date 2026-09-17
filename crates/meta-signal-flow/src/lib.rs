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
/// A saved configuration does not rebind live sockets; it activates at the
/// next Nexus start.
#[derive(Archive, Deserialize, Serialize, Debug, Clone, PartialEq, Eq)]
#[cfg_attr(
    feature = "datom",
    derive(datom_codec::Datomizable, datom_codec::Composing)
)]
pub enum ConfigurationActivation {
    NexusRestartRequired,
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
    Configured {
        configuration: Configuration,
        activation: ConfigurationActivation,
    },
}

#[cfg(all(test, feature = "datom"))]
mod tests {
    use super::{Configuration, ConfigurationActivation, Query, Response, WIRE_VERSION};
    use datom_codec::{Actualizing, Budget, Datomizable, Potential};
    use protos::{Protosizable, ReaderBudget, Textualizable};

    #[test]
    fn configure_example_parses_and_round_trips() {
        let text = "Configure.{ /tmp/flow.sock /tmp/flow-meta.sock }";
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
    fn configured_reply_round_trips_through_archived_bytes_and_datom_text() {
        let response = Response::Configured {
            configuration: Configuration {
                ordinary_socket: "/tmp/flow.sock".into(),
                meta_socket: "/tmp/flow-meta.sock".into(),
            },
            activation: ConfigurationActivation::NexusRestartRequired,
        };
        let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&response).expect("archive");
        assert_eq!(
            rkyv::from_bytes::<Response, rkyv::rancor::Error>(&bytes).expect("restore"),
            response
        );
        let text = response.datomize(vec![]).protosize().textualize();
        assert_eq!(
            text,
            "Configured.{ { /tmp/flow.sock /tmp/flow-meta.sock } NexusRestartRequired }"
        );
        let mut budget = Budget {
            remaining: 1024,
            reader: ReaderBudget { remaining: 1024 },
            depth: 0,
            maximum_depth: 1024,
        };
        assert_eq!(
            Potential::<Response>::from(text)
                .actualize(&mut budget)
                .expect("reply parses"),
            response
        );
        assert_eq!(WIRE_VERSION, "0.1.0");
    }
}
