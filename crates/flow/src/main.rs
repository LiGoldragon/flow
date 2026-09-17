use datom_codec::{Actualizing, Budget, Datomizable, Potential};
use protos::{Protosizable, ReaderBudget, Textualizable};
use signal_flow::{Query, Response};
use std::{
    env,
    io::{Read, Write},
    os::unix::net::UnixStream,
};

struct FlowClient {
    socket: String,
}
trait ParsesFlowDatom {
    fn parse_query(&self, text: &str) -> Result<Query, String>;
}
trait CallsFlowNexus {
    fn call(&self, query: &Query) -> Result<Response, String>;
}
trait TextualizesFlowReply {
    fn textualize_reply(&self, reply: &Response) -> String;
}
impl ParsesFlowDatom for FlowClient {
    fn parse_query(&self, text: &str) -> Result<Query, String> {
        Potential::<Query>::from(text)
            .actualize(&mut Budget {
                remaining: 4096,
                reader: ReaderBudget { remaining: 4096 },
                depth: 0,
                maximum_depth: 1024,
            })
            .map_err(|fault| format!("invalid Flow Datom: {fault:?}"))
    }
}
impl CallsFlowNexus for FlowClient {
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
impl TextualizesFlowReply for FlowClient {
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
    }
    let client = FlowClient {
        socket: env::var("FLOW_SOCKET").unwrap_or_else(|_| "/tmp/flow-nexus.sock".into()),
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
    use super::{FlowClient, ParsesFlowDatom, TextualizesFlowReply};
    use signal_flow::{Origin, Response};
    #[test]
    fn client_actualizes_examples() {
        let client = FlowClient {
            socket: "unused".into(),
        };
        assert!(
            client
                .parse_query("Start.{ codex-medium «read prior flow» { parent session turn } }")
                .is_ok()
        );
        assert!(
            client
                .parse_query("Restart.{ flow-0000000000000001 parent }")
                .is_ok()
        );
    }
    #[test]
    fn client_textualizes_reply() {
        let client = FlowClient {
            socket: "unused".into(),
        };
        assert_eq!(
            client.textualize_reply(&Response::Started {
                flow_id: "flow-1".into(),
                origin: Origin {
                    parent_flow_id: "parent".into(),
                    session: "session".into(),
                    turn: "turn".into()
                }
            }),
            "Started.{ flow-1 { parent session turn } }"
        );
    }
}
