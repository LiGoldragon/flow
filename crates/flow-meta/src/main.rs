use datom_codec::Datomizable;
use meta_signal_flow::{Configuration, CreditSelection, Query, ResetRequest, Response};
use protos::{Protosizable, Textualizable};
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

impl ParsesMetaCommand for FlowMetaClient {
    fn parse_command(&self, mut arguments: impl Iterator<Item = String>) -> Result<Query, String> {
        let operation = arguments.next();
        match operation.as_deref() {
            Some("reset") => {
                let Some(idempotency_key) = arguments.next() else {
                    return Err("usage: flow-meta reset <idempotency-key> [credit-id]".into());
                };
                let credit_selection = arguments.next()
                    .map(CreditSelection::Specific)
                    .unwrap_or(CreditSelection::Next);
                if arguments.next().is_some() {
                    return Err("usage: flow-meta reset <idempotency-key> [credit-id]".into());
                }
                Ok(Query::ConsumeReset(ResetRequest { idempotency_key, credit_selection }))
            }
            Some("register-codex") | Some("register-claude") => {
                let harness = operation.expect("matched operation");
                let Some(flow_id) = arguments.next() else { return Err(Self::registration_usage()) };
                let Some(session_id) = arguments.next() else { return Err(Self::registration_usage()) };
                let Some(herdr_session_name) = arguments.next() else { return Err(Self::registration_usage()) };
                let Some(herdr_agent_name) = arguments.next() else { return Err(Self::registration_usage()) };
                let Some(herdr_pane_id) = arguments.next() else { return Err(Self::registration_usage()) };
                let Some(herdr_terminal_id) = arguments.next() else { return Err(Self::registration_usage()) };
                let endpoint = arguments.next();
                if arguments.next().is_some() { return Err(Self::registration_usage()) }
                let harness_kind = if harness == "register-codex" {
                    signal_flow::HarnessKind::Codex
                } else {
                    signal_flow::HarnessKind::Claude
                };
                let endpoint_path = endpoint.unwrap_or_else(|| {
                    if harness_kind == signal_flow::HarnessKind::Codex {
                        "/home/li/.codex/app-server-control/app-server-control.sock".into()
                    } else {
                        String::new()
                    }
                });
                let endpoint_selection = if endpoint_path.is_empty() {
                    signal_flow::EndpointSelection::Unavailable
                } else {
                    signal_flow::EndpointSelection::Available(signal_flow::Available_Data {
                        endpoint_path,
                        route_readiness: if harness_kind == signal_flow::HarnessKind::Codex {
                            signal_flow::RouteReadiness::Ready
                        } else {
                            signal_flow::RouteReadiness::Parked
                        },
                    })
                };
                Ok(Query::RegisterFlow(signal_flow::FlowNode {
                    flow_id: flow_id.clone(),
                    session_id: session_id.clone(),
                    harness_kind,
                    endpoint_selection,
                    herdr_route_selection: signal_flow::HerdrRouteSelection::Available(
                        signal_flow::HerdrRoute {
                            herdr_session_name,
                            herdr_agent_name,
                            herdr_pane_id,
                            herdr_terminal_id,
                        },
                    ),
                    origin_clue: signal_flow::OriginClue {
                        flow_id,
                        session_id,
                        turn_id: "unavailable".into(),
                    },
                    flow_lifecycle: signal_flow::FlowLifecycle::RegisteredUnconfirmed,
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
            _ => Err("usage: flow-meta reset <idempotency-key> [credit-id] | flow-meta register-codex|register-claude <flow-id> <session-id> <herdr-session> <herdr-agent> <herdr-pane> <herdr-terminal> [endpoint] | flow-meta configure <ordinary-socket> <meta-socket>".into()),
        }
    }
}

impl FlowMetaClient {
    fn registration_usage() -> String {
        "usage: flow-meta register-codex|register-claude <flow-id> <session-id> <herdr-session> <herdr-agent> <herdr-pane> <herdr-terminal> [endpoint]".into()
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
    fn reset_keeps_the_callers_retry_key() {
        let client = FlowMetaClient {
            socket: "unused".into(),
        };
        let Query::ConsumeReset(request) = client
            .parse_command(["reset".into(), "attempt-1".into()].into_iter())
            .expect("reset command")
        else {
            panic!("reset query")
        };
        assert_eq!(request.credit_selection, CreditSelection::Next);
        assert_eq!(request.idempotency_key, "attempt-1");
    }

    #[test]
    fn registration_carries_the_complete_herdr_binding() {
        let client = FlowMetaClient {
            socket: "unused".into(),
        };
        let Query::RegisterFlow(node) = client
            .parse_command(
                [
                    "register-claude",
                    "da1e3f",
                    "da1e3f9d-full",
                    "messaging-build",
                    "recipient",
                    "w1:p2",
                    "term-current",
                ]
                .map(String::from)
                .into_iter(),
            )
            .expect("registration query")
        else {
            panic!("registration query")
        };
        assert_eq!(
            node.herdr_route_selection,
            signal_flow::HerdrRouteSelection::Available(signal_flow::HerdrRoute {
                herdr_session_name: "messaging-build".into(),
                herdr_agent_name: "recipient".into(),
                herdr_pane_id: "w1:p2".into(),
                herdr_terminal_id: "term-current".into(),
            })
        );
    }
}
