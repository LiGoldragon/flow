use datom_codec::{Actualizing, Budget, Datomizable, Potential};
use meta_signal_flow::{Query, Response};
use protos::{Protosizable, ReaderBudget, Textualizable};
use std::{
    env,
    io::{Read, Write},
    os::unix::net::UnixStream,
};

struct FlowMetaClient {
    socket: String,
}
trait ParsesMetaDatom {
    fn parse_query(&self, text: &str) -> Result<Query, String>;
}
trait CallsMetaNexus {
    fn call(&self, query: &Query) -> Result<Response, String>;
}
trait TextualizesMetaReply {
    fn textualize_reply(&self, reply: &Response) -> String;
}
impl ParsesMetaDatom for FlowMetaClient {
    fn parse_query(&self, text: &str) -> Result<Query, String> {
        Potential::<Query>::from(text)
            .actualize(&mut Budget {
                remaining: 4096,
                reader: ReaderBudget { remaining: 4096 },
                depth: 0,
                maximum_depth: 1024,
            })
            .map_err(|fault| format!("invalid Flow meta Datom: {fault:?}"))
    }
}
impl CallsMetaNexus for FlowMetaClient {
    fn call(&self, query: &Query) -> Result<Response, String> {
        let mut peer = UnixStream::connect(&self.socket).map_err(|error| error.to_string())?;
        let bytes =
            rkyv::to_bytes::<rkyv::rancor::Error>(query).map_err(|error| error.to_string())?;
        peer.write_all(&(bytes.len() as u32).to_be_bytes())
            .map_err(|error| error.to_string())?;
        peer.write_all(&bytes).map_err(|error| error.to_string())?;
        let mut length = [0; 4];
        peer.read_exact(&mut length)
            .map_err(|error| error.to_string())?;
        let mut reply = vec![0; u32::from_be_bytes(length) as usize];
        peer.read_exact(&mut reply)
            .map_err(|error| error.to_string())?;
        rkyv::from_bytes::<Response, rkyv::rancor::Error>(&reply).map_err(|error| error.to_string())
    }
}
impl TextualizesMetaReply for FlowMetaClient {
    fn textualize_reply(&self, reply: &Response) -> String {
        reply.datomize(vec![]).protosize().textualize()
    }
}
fn main() {
    let mut arguments = env::args().skip(1);
    let Some(datom) = arguments.next() else {
        std::process::exit(2)
    };
    if arguments.next().is_some() {
        std::process::exit(2)
    };
    let client = FlowMetaClient {
        socket: env::var("FLOW_META_SOCKET").unwrap_or_else(|_| "/tmp/flow-nexus-meta.sock".into()),
    };
    match client
        .parse_query(&datom)
        .and_then(|query| client.call(&query))
    {
        Ok(reply) => println!("{}", client.textualize_reply(&reply)),
        Err(error) => {
            eprintln!("{error}");
            std::process::exit(2)
        }
    }
}
#[cfg(test)]
mod tests {
    use super::{FlowMetaClient, ParsesMetaDatom, TextualizesMetaReply};
    use meta_signal_flow::{Configuration, ConfigurationActivation, Response};
    #[test]
    fn client_actualizes_configure() {
        assert!(
            FlowMetaClient {
                socket: "unused".into()
            }
            .parse_query("Configure.{ /tmp/flow.sock /tmp/flow-meta.sock }")
            .is_ok()
        );
    }
    #[test]
    fn client_textualizes_reply() {
        let client = FlowMetaClient {
            socket: "unused".into(),
        };
        assert_eq!(
            client.textualize_reply(&Response::Configured {
                configuration: Configuration {
                    ordinary_socket: "/tmp/flow.sock".into(),
                    meta_socket: "/tmp/flow-meta.sock".into(),
                },
                activation: ConfigurationActivation::NexusRestartRequired,
            }),
            "Configured.{ { /tmp/flow.sock /tmp/flow-meta.sock } NexusRestartRequired }"
        );
    }
}
