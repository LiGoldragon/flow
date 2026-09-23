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

trait ParsesFlowCommand {
    fn parse_command(&self, arguments: impl Iterator<Item = String>) -> Result<Query, String>;
}

trait CallsFlowNexus {
    fn call(&self, query: &Query) -> Result<Response, String>;
}

trait TextualizesFlowReply {
    fn textualize_reply(&self, reply: &Response) -> String;
}

impl ParsesFlowCommand for FlowClient {
    fn parse_command(&self, mut arguments: impl Iterator<Item = String>) -> Result<Query, String> {
        let Some(value) = arguments.next() else {
            return Err("usage: flow '<one inline Query datom>'".into());
        };
        if arguments.next().is_some() {
            return Err("usage: flow '<one inline Query datom>'".into());
        }
        let mut budget = Budget {
            remaining: 65_536,
            reader: ReaderBudget { remaining: 65_536 },
            depth: 0,
            maximum_depth: 1_024,
        };
        Potential::<Query>::from(value)
            .actualize(&mut budget)
            .map_err(|error| format!("invalid Flow query: {error:?}"))
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
    use super::{FlowClient, ParsesFlowCommand, TextualizesFlowReply};
    use signal_flow::{Query, Response};

    #[test]
    fn resolve_is_nearly_argumentless_and_typed() {
        let client = FlowClient {
            socket: "unused".into(),
        };
        assert_eq!(
            client
                .parse_command(["ResolveRecipient.fac697".into()].into_iter())
                .unwrap(),
            Query::ResolveRecipient("fac697".into())
        );
    }

    #[test]
    fn start_accepts_one_inline_typed_launch_profile() {
        let client = FlowClient {
            socket: "unused".into(),
        };
        let Query::Start(request) = client
            .parse_command([
                "Start.{ { request-7 [ { Vision/flowNexus.md 54c08e7190360a308e560935c120c69b81c4aacb4975751a4841912b599f4f5a } ] [ spirit main-flow ] Field High Codex gpt-6-astra medium Some.836818 [ { 1b8ac0 1 } ] messaging-build «Carry this bounded launch request.» } { fac697 session-1 turn-2 } }".into(),
            ]
            .into_iter())
            .expect("typed Start parses")
        else {
            panic!("Start fixture must remain a Start query")
        };
        assert_eq!(request.launch_profile.launch_request_id, "request-7");
        assert_eq!(request.launch_profile.launch_source_vector.len(), 1);
        assert_eq!(
            request.launch_profile.skill_name_vector,
            ["spirit", "main-flow"]
        );
    }

    #[test]
    fn command_refuses_more_than_one_inline_value() {
        let client = FlowClient {
            socket: "unused".into(),
        };
        assert!(
            client
                .parse_command(["ResolveRecipient.fac697".into(), "extra".into()].into_iter())
                .is_err()
        );
        assert!(
            client
                .parse_command(
                    ["Start.{ codex-medium { fac697 session-1 turn-2 } }".into()].into_iter()
                )
                .is_err()
        );
        assert!(
            client
                .parse_command(["Start.{".into()].into_iter())
                .is_err()
        );
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
