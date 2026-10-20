use datom_codec::{Actualizing, Budget, Datomizable, Potential};
use flow_defaults::{DefaultConfiguration, LaysOutDefaults};
use meta_signal_flow::{Query, Response};
use protos::{Compactable, Protosizable, ReaderBudget};
use std::{
    env,
    io::{Read, Write},
    os::unix::net::UnixStream,
};

struct FlowMetaClient {
    socket: String,
}

trait ParsesMetaCommand {
    fn parse_command(&self, arguments: impl Iterator<Item = String>) -> Result<Query, String>;
}

trait CallsMetaNexus {
    fn call(&self, query: &Query) -> Result<Response, String>;
}

fn parse_datom<T: datom_codec::Composing>(source: String, subject: &str) -> Result<T, String> {
    let mut budget = Budget {
        remaining: 65_536,
        reader: ReaderBudget { remaining: 65_536 },
        depth: 0,
        maximum_depth: 1_024,
    };
    Potential::<T>::from(source)
        .actualize(&mut budget)
        .map_err(|error| format!("invalid {subject} datom: {error:?}"))
}

impl ParsesMetaCommand for FlowMetaClient {
    fn parse_command(&self, mut arguments: impl Iterator<Item = String>) -> Result<Query, String> {
        let Some(datom) = arguments.next() else {
            return Err("usage: flow-meta '<one typed Query Datom>'".into());
        };
        if arguments.next().is_some() {
            return Err("flow-meta accepts exactly one typed Query Datom".into());
        }
        parse_datom::<Query>(datom, "Flow meta query")
    }
}

impl CallsMetaNexus for FlowMetaClient {
    fn call(&self, query: &Query) -> Result<Response, String> {
        let mut peer = UnixStream::connect(&self.socket).map_err(|error| error.to_string())?;
        let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(query).map_err(|e| e.to_string())?;
        peer.write_all(&(bytes.len() as u32).to_be_bytes())
            .map_err(|e| e.to_string())?;
        peer.write_all(&bytes).map_err(|e| e.to_string())?;
        let mut length = [0; 4];
        peer.read_exact(&mut length).map_err(|e| e.to_string())?;
        let length = u32::from_be_bytes(length) as usize;
        if length > 1024 * 1024 {
            return Err("Signal frame exceeds 1 MiB".into());
        }
        let mut reply = vec![0; length];
        peer.read_exact(&mut reply).map_err(|e| e.to_string())?;
        rkyv::from_bytes::<Response, rkyv::rancor::Error>(&reply).map_err(|e| e.to_string())
    }
}

fn main() {
    let client = FlowMetaClient {
        socket: env::var("FLOW_META_SOCKET").unwrap_or_else(|_| {
            DefaultConfiguration::from_environment()
                .meta_socket_path()
                .to_string_lossy()
                .into_owned()
        }),
    };
    match client
        .parse_command(env::args().skip(1))
        .and_then(|query| client.call(&query))
    {
        Ok(reply) => println!("{}", reply.datomize(vec![]).protosize().compact()),
        Err(error) => {
            eprintln!("{error}");
            std::process::exit(2)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{FlowMetaClient, ParsesMetaCommand};
    use datom_codec::Datomizable;
    use meta_signal_flow::Query;
    use protos::{Compactable, Protosizable};

    #[test]
    fn accepts_one_typed_configuration_query() {
        let source = Query::Configuration
            .datomize(vec![])
            .protosize()
            .compact();
        assert_eq!(
            FlowMetaClient {
                socket: "unused".into()
            }
            .parse_command([source].into_iter())
            .expect("typed query parses"),
            Query::Configuration
        );
    }

    #[test]
    fn refuses_extra_arguments_instead_of_composing_fragments() {
        let source = Query::Configuration
            .datomize(vec![])
            .protosize()
            .compact();
        assert!(
            FlowMetaClient {
                socket: "unused".into()
            }
            .parse_command([source, "extra".into()].into_iter())
            .is_err()
        );
    }

    #[test]
    fn refuses_the_retired_configure_files_word_command() {
        assert!(
            FlowMetaClient {
                socket: "unused".into()
            }
            .parse_command(["configure-files".into()].into_iter())
            .is_err()
        );
    }
}
