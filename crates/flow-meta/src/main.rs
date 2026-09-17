use datom_codec::Datomizable;
use meta_signal_flow::{Configuration, CreditSelection, Query, ResetRequest, Response};
use protos::{Protosizable, Textualizable};
use std::{
    env,
    io::{Read, Write},
    os::unix::net::UnixStream,
    time::{SystemTime, UNIX_EPOCH},
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

impl ParsesMetaCommand for FlowMetaClient {
    fn parse_command(&self, mut arguments: impl Iterator<Item = String>) -> Result<Query, String> {
        let operation = arguments.next();
        match operation.as_deref() {
            Some("reset") => {
                let credit_selection = arguments.next()
                    .map(CreditSelection::Specific)
                    .unwrap_or(CreditSelection::Next);
                if arguments.next().is_some() {
                    return Err("usage: flow-meta reset [credit-id]".into());
                }
                let nonce = SystemTime::now().duration_since(UNIX_EPOCH)
                    .map_err(|error| error.to_string())?.as_nanos();
                Ok(Query::ConsumeReset(ResetRequest {
                    idempotency_key: format!("flow-meta-{nonce}"),
                    credit_selection,
                }))
            }
            Some("configure") => {
                let Some(ordinary_socket_path) = arguments.next() else {
                    return Err("usage: flow-meta configure <ordinary-socket> <meta-socket>".into());
                };
                let Some(meta_socket_path) = arguments.next() else {
                    return Err("usage: flow-meta configure <ordinary-socket> <meta-socket>".into());
                };
                if arguments.next().is_some() {
                    return Err("usage: flow-meta configure <ordinary-socket> <meta-socket>".into());
                }
                Ok(Query::Configure(Configuration { ordinary_socket_path, meta_socket_path }))
            }
            _ => Err("usage: flow-meta reset [credit-id] | flow-meta configure <ordinary-socket> <meta-socket>".into()),
        }
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
        let mut reply = vec![0; u32::from_be_bytes(length) as usize];
        peer.read_exact(&mut reply).map_err(|e| e.to_string())?;
        rkyv::from_bytes::<Response, rkyv::rancor::Error>(&reply).map_err(|e| e.to_string())
    }
}

fn main() {
    let client = FlowMetaClient {
        socket: env::var("FLOW_META_SOCKET")
            .unwrap_or_else(|_| "/run/user/1001/flow/flow-meta.sock".into()),
    };
    match client
        .parse_command(env::args().skip(1))
        .and_then(|query| client.call(&query))
    {
        Ok(reply) => println!("{}", reply.datomize(vec![]).protosize().textualize()),
        Err(error) => {
            eprintln!("{error}");
            std::process::exit(2)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{FlowMetaClient, ParsesMetaCommand};
    use meta_signal_flow::{CreditSelection, Query};

    #[test]
    fn reset_selects_the_next_credit_by_default() {
        let client = FlowMetaClient {
            socket: "unused".into(),
        };
        let Query::ConsumeReset(request) = client
            .parse_command(["reset".into()].into_iter())
            .expect("reset command")
        else {
            panic!("reset query")
        };
        assert_eq!(request.credit_selection, CreditSelection::Next);
    }
}
