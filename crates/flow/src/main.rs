use datom_codec::{Actualizing, Budget, Datomizable, Potential};
use protos::{Protosizable, ReaderBudget, Textualizable};
use signal_flow::{Query, Response};
use std::{
    env,
    io::{Read, Write},
    os::unix::net::UnixStream,
    process::ExitCode,
};

struct FlowClient {
    socket: String,
}

trait ParsesFlowCommand {
    fn parse_query(&self, arguments: impl Iterator<Item = String>) -> Result<Query, String>;
}

trait CallsFlowNexus {
    fn call(&self, query: &Query) -> Result<Response, String>;
}

trait TextualizesFlowReply {
    fn textualize_reply(&self, reply: &Response) -> String;
}

impl ParsesFlowCommand for FlowClient {
    fn parse_query(&self, mut arguments: impl Iterator<Item = String>) -> Result<Query, String> {
        let source = match (arguments.next(), arguments.next()) {
            (Some(source), None) if !source.starts_with("--") => source,
            _ => return Err("accepts exactly one inline Datom query and no flags".into()),
        };
        let mut potential = Potential::<Query>::from(source);
        let mut budget = Budget {
            remaining: 10_000,
            reader: ReaderBudget { remaining: 10_000 },
            depth: 0,
            maximum_depth: 256,
        };
        potential
            .actualize(&mut budget)
            .map_err(|error| error.datomize(vec![]).protosize().textualize())
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
        let length = u32::from_be_bytes(length) as usize;
        if length > 1024 * 1024 {
            return Err("Signal frame exceeds 1 MiB".into());
        }
        let mut reply = vec![0; length];
        peer.read_exact(&mut reply).map_err(|e| e.to_string())?;
        rkyv::from_bytes::<Response, rkyv::rancor::Error>(&reply).map_err(|e| e.to_string())
    }
}

impl TextualizesFlowReply for FlowClient {
    fn textualize_reply(&self, reply: &Response) -> String {
        reply.datomize(vec![]).protosize().textualize()
    }
}

fn main() -> ExitCode {
    let client = FlowClient {
        socket: env::var("FLOW_SOCKET").unwrap_or_else(|_| "/run/user/1001/flow/flow.sock".into()),
    };
    match client
        .parse_query(env::args().skip(1))
        .and_then(|query| client.call(&query))
    {
        Ok(reply) => {
            println!("{}", client.textualize_reply(&reply));
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("flow: {error}");
            ExitCode::FAILURE
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        FlowClient, ParsesFlowCommand, TextualizesFlowReply,
    };
    use signal_flow::{Query, Response};

    #[test]
    fn compiles_one_datom_query() {
        let client = FlowClient {
            socket: "unused".into(),
        };
        assert_eq!(
            client
                .parse_query(["ResolveRecipient.fac697".into()].into_iter())
                .unwrap(),
            Query::ResolveRecipient("fac697".into())
        );
    }

    #[test]
    fn rejects_legacy_commands_and_flags() {
        let client = FlowClient {
            socket: "unused".into(),
        };
        assert!(client
            .parse_query(["start".into(), "codex-medium".into()].into_iter())
            .is_err());
        assert!(client
            .parse_query(["--help".into()].into_iter())
            .is_err());
    }

    #[test]
    fn native_resolution_serialization_matches_the_signal_contract() {
        let client = FlowClient {
            socket: "unused".into(),
        };
        let reply = Response::RecipientResolved(signal_flow::FlowNode {
            flow_id: "da1e3f".into(),
            session_id: "da1e3f9d-full".into(),
            harness_kind: signal_flow::HarnessKind::Claude,
            endpoint_selection: signal_flow::EndpointSelection::Unavailable,
            herdr_route_selection: signal_flow::HerdrRouteSelection::Available(
                signal_flow::HerdrRoute {
                    herdr_session_name: "messaging-build".into(),
                    herdr_agent_name: "recipient".into(),
                    herdr_pane_id: "w1:p2".into(),
                    herdr_terminal_id: "term-current".into(),
                },
            ),
            origin_clue: signal_flow::OriginClue {
                flow_id: "da1e3f".into(),
                session_id: "da1e3f9d-full".into(),
                turn_id: "unavailable".into(),
            },
            flow_lifecycle: signal_flow::FlowLifecycle::Active,
        });
        assert_eq!(
            client.textualize_reply(&reply),
            "RecipientResolved.{ da1e3f da1e3f9d-full Claude Unavailable Available.{ messaging-build recipient w1:p2 term-current } { da1e3f da1e3f9d-full unavailable } Active }"
        );
    }
}
