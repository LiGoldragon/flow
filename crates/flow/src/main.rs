use datom_codec::Datomizable;
use protos::{Protosizable, Textualizable};
use signal_flow::{OriginClue, Query, Response, RestartRequest, StartRequest};
use std::{
    env,
    io::{Read, Write},
    os::unix::net::UnixStream,
};

struct FlowClient {
    socket: String,
}

trait ReadsCallerOrigin {
    fn caller_origin(&self) -> Result<OriginClue, String>;
}

trait ParsesFlowCommand {
    fn parse_command(&self, arguments: impl Iterator<Item = String>) -> Result<Query, String>;
}

trait CallsFlowNexus {
    fn call(&self, query: &Query) -> Result<Response, String>;
}

trait TextualizesFlowReply {
    fn textualize_reply(&self, reply: &Response) -> String;
}

impl ReadsCallerOrigin for FlowClient {
    fn caller_origin(&self) -> Result<OriginClue, String> {
        let flow_session = env::var("CODEX_SESSION_ID")
            .or_else(|_| env::var("CLAUDE_SESSION_ID"))
            .map_err(|_| "origin flow session unavailable".to_string())?;
        let session_id = env::var("CODEX_THREAD_ID")
            .or_else(|_| env::var("CODEX_SESSION_ID"))
            .or_else(|_| env::var("CLAUDE_SESSION_ID"))
            .map_err(|_| "origin session unavailable".to_string())?;
        let flow_id = env::var("FLOW_ID").unwrap_or_else(|_| {
            flow_session
                .rsplit('-')
                .next()
                .and_then(|tail| {
                    tail.get(tail.len().saturating_sub(9)..tail.len().saturating_sub(3))
                })
                .unwrap_or("unknown")
                .to_owned()
        });
        if flow_id == "unknown" {
            return Err("origin flow unavailable".into());
        }
        let turn_id = env::var("TURN_ID").unwrap_or_else(|_| "unavailable".into());
        Ok(OriginClue {
            flow_id,
            session_id,
            turn_id,
        })
    }
}

impl ParsesFlowCommand for FlowClient {
    fn parse_command(&self, mut arguments: impl Iterator<Item = String>) -> Result<Query, String> {
        match (arguments.next().as_deref(), arguments.next(), arguments.next()) {
            (Some("start"), Some(flow_type), None) => Ok(Query::Start(StartRequest {
                flow_type,
                origin_clue: self.caller_origin()?,
            })),
            (Some("restart"), Some(flow_id), None) => {
                let origin = self.caller_origin()?;
                Ok(Query::Restart(RestartRequest {
                    first_flow_id: flow_id,
                    second_flow_id: origin.flow_id,
                }))
            }
            (Some("resolve"), Some(flow_id), None) => Ok(Query::ResolveRecipient(flow_id)),
            _ => Err("usage: flow start <predefined-type> | flow restart <flow-id> | flow resolve <flow-id>".into()),
        }
    }
}

impl CallsFlowNexus for FlowClient {
    fn call(&self, query: &Query) -> Result<Response, String> {
        let mut peer = UnixStream::connect(&self.socket).map_err(|error| error.to_string())?;
        let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(query).map_err(|e| e.to_string())?;
        peer.write_all(&(bytes.len() as u32).to_be_bytes())
            .map_err(|e| e.to_string())?;
        peer.write_all(&bytes).map_err(|e| e.to_string())?;
        let mut length = [0; 4];
        peer.read_exact(&mut length).map_err(|e| e.to_string())?;
        let mut reply = vec![0; u32::from_be_bytes(length) as usize];
        peer.read_exact(&mut reply).map_err(|e| e.to_string())?;
        rkyv::from_bytes::<Response, rkyv::rancor::Error>(&reply).map_err(|e| e.to_string())
    }
}

impl TextualizesFlowReply for FlowClient {
    fn textualize_reply(&self, reply: &Response) -> String {
        reply.datomize(vec![]).protosize().textualize()
    }
}

fn main() {
    let client = FlowClient {
        socket: env::var("FLOW_SOCKET").unwrap_or_else(|_| "/run/user/1001/flow/flow.sock".into()),
    };
    match client
        .parse_command(env::args().skip(1))
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
    use super::{FlowClient, ParsesFlowCommand};
    use signal_flow::Query;

    #[test]
    fn resolve_is_nearly_argumentless_and_typed() {
        let client = FlowClient {
            socket: "unused".into(),
        };
        assert_eq!(
            client
                .parse_command(["resolve".into(), "fac697".into()].into_iter())
                .unwrap(),
            Query::ResolveRecipient("fac697".into())
        );
    }
}
