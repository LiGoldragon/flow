use datom_codec::{Actualizing, Budget, Datomizable, Potential};
use flow_defaults::{DefaultConfiguration, LaysOutDefaults, ReadsAnchors};
use meta_signal_flow::{CreditSelection, Query, ResetRequest, Response};
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

    fn query_usage() -> String {
        "usage: flow-meta '<one inline Query Datom>'".into()
    }

    fn registration_usage() -> String {
        "usage: flow-meta register-codex|register-claude <flow-id> <session-id> <herdr-session> <herdr-agent> <herdr-pane> <herdr-terminal> [endpoint]".into()
    }
}
trait CallsMetaNexus {
    fn call(&self, query: &Query) -> Result<Response, String>;
}

trait ParsesMetaDatom {
    fn parse_datom<T: datom_codec::Composing>(source: String, subject: &str) -> Result<T, String>;
}
impl ParsesMetaDatom for FlowMetaClient {
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
}

impl ParsesMetaCommand for FlowMetaClient {
    fn parse_command(&self, mut arguments: impl Iterator<Item = String>) -> Result<Query, String> {
        let operation = arguments.next();
        match operation.as_deref() {
            Some("reset") => {
                let Some(idempotency_key) = arguments.next() else {
                    return Err("usage: flow-meta reset <idempotency-key> [credit-id]".into());
                };
                let credit_selection = arguments
                    .next()
                    .map(CreditSelection::Specific)
                    .unwrap_or(CreditSelection::Next);
                if arguments.next().is_some() {
                    return Err("usage: flow-meta reset <idempotency-key> [credit-id]".into());
                }
                Ok(Query::ConsumeReset(ResetRequest {
                    idempotency_key,
                    credit_selection,
                }))
            }
            Some("register-codex") | Some("register-claude") => {
                let harness = operation.expect("matched operation");
                let Some(flow_id) = arguments.next() else {
                    return Err(Self::registration_usage());
                };
                let Some(session_id) = arguments.next() else {
                    return Err(Self::registration_usage());
                };
                let Some(herdr_session_name) = arguments.next() else {
                    return Err(Self::registration_usage());
                };
                let Some(herdr_agent_name) = arguments.next() else {
                    return Err(Self::registration_usage());
                };
                let Some(herdr_pane_id) = arguments.next() else {
                    return Err(Self::registration_usage());
                };
                let Some(herdr_terminal_id) = arguments.next() else {
                    return Err(Self::registration_usage());
                };
                let endpoint = arguments.next();
                if arguments.next().is_some() {
                    return Err(Self::registration_usage());
                }
                let harness_kind = if harness == "register-codex" {
                    signal_flow::HarnessKind::Codex
                } else {
                    signal_flow::HarnessKind::Claude
                };
                let endpoint_path = endpoint.unwrap_or_else(|| {
                    if harness_kind == signal_flow::HarnessKind::Codex {
                        DefaultConfiguration::from_environment()
                            .stable_codex()
                            .socket
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
                    flow_id,
                    session_id,
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
                    // A flow registered by hand was started by no flow.
                    origin: signal_flow::Origin::Unbound,
                    flow_lifecycle: signal_flow::FlowLifecycle::Active,
                }))
            }
            // A capitalized argument is one inline meta Query datom, the
            // shape every request takes once the word commands retire.
            Some(datom) if datom.starts_with(|first: char| first.is_ascii_uppercase()) => {
                if arguments.next().is_some() {
                    return Err(Self::query_usage());
                }
                Self::parse_datom::<Query>(datom.to_owned(), "Flow meta query")
            }
            _ => Err(format!(
                "usage: flow-meta reset <idempotency-key> [credit-id] | flow-meta register-codex|register-claude <flow-id> <session-id> <herdr-session> <herdr-agent> <herdr-pane> <herdr-terminal> [endpoint] | {}",
                Self::query_usage().trim_start_matches("usage: flow-meta ")
            )),
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
    // The Nexus's default meta socket under this caller's runtime
    // directory, unless the caller names another Nexus's socket.
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
    use meta_signal_flow::{ConfigurationChange, ConfigureNexus, CreditSelection, Query};
    use protos::{Compactable, Protosizable};

    #[test]
    fn configuration_uses_the_current_typed_nexus_change() {
        let endpoint = || meta_signal_flow::CodexEndpoint {
            client_path: "/opt/codex".into(),
            home: "/tmp/codex".into(),
            control_socket_path: "/tmp/codex.sock".into(),
            model_name_vector: vec!["fixture".into()],
        };
        let nexus = ConfigureNexus {
            source_root: "/source".into(),
            stable_codex: endpoint(),
            next_codex: endpoint(),
            harness_profiles: vec![],
            meta_aspects: vec![meta_signal_flow::Aspect::Psyche],
            message_nexus_path: "/run/message.sock".into(),
            message_nexus_binary: "/opt/message-nexus".into(),
            lease: 60,
        };
        let query = Query::Configure(ConfigurationChange::Nexus(nexus.clone()));
        let datom = query.datomize(vec![]).protosize().compact();
        let client = FlowMetaClient {
            socket: "unused".into(),
        };
        let Query::Configure(ConfigurationChange::Nexus(actual)) = client
            .parse_command([datom.clone()].into_iter())
            .expect("typed Nexus Configure")
        else {
            panic!("wrong configuration variant")
        };
        assert_eq!(actual, nexus);
        assert!(
            client
                .parse_command([datom, "extra".into()].into_iter())
                .is_err()
        );
        // Sockets and StorePath belong to Start. The removed fragment wire
        // type cannot silently be lowered into a different Nexus contract.
        assert!(client.parse_command(["{ [] }".into()].into_iter()).is_err());
        assert!(
            client
                .parse_command(["configure-files".into(), "/tmp/old".into()].into_iter())
                .is_err()
        );
    }

    #[test]
    fn a_family_declaration_is_one_inline_datom_carrying_the_whole_family() {
        let client = FlowMetaClient {
            socket: "unused".into(),
        };
        let datom = "Configure.LeaveOuts.[ { { Field core Primary } [ CodexAgentsMd CodexSkillListing ] } { { Psyche flow Secondary } [ SkillTool ClaudeMd ] } ]";
        let query = client
            .parse_command([datom.to_owned()].into_iter())
            .expect("LeaveOuts declaration parses");
        let Query::Configure(ConfigurationChange::LeaveOuts(leave_outs)) = &query else {
            panic!("a LeaveOuts declaration must stay one Configure query")
        };
        assert_eq!(leave_outs.len(), 2);
        assert_eq!(query.datomize(vec![]).protosize().compact(), datom);
        // Reconfigure is not a request: Configure.Nexus replaces the Nexus.
        assert!(
            client
                .parse_command(["Reconfigure.{ /home/li/primary }".to_owned()].into_iter())
                .is_err()
        );
    }

    #[test]
    fn refresh_is_one_inline_datom_naming_a_metaflow() {
        let client = FlowMetaClient {
            socket: "unused".into(),
        };
        let datom = "Refresh.{ Mind flow Secondary }";
        let query = client
            .parse_command([datom.to_owned()].into_iter())
            .expect("Refresh datom parses");
        let Query::Refresh(address) = &query else {
            panic!("Refresh datom must stay a Refresh query")
        };
        assert_eq!(address.topic, "flow");
        assert_eq!(query.datomize(vec![]).protosize().compact(), datom);
    }

    #[test]
    fn deliver_is_one_inline_datom_carrying_a_typed_message() {
        let client = FlowMetaClient {
            socket: "unused".into(),
        };
        let Query::Deliver(request) = client
            .parse_command(
                ["Deliver.{ m-7f3a2c:7d41e0:0 7d41e0 Soft.{ m-7f3a2c Flow.e167d8 Text.«Stage 1 is deployed; run the tier tests.» } }".into()]
                    .into_iter(),
            )
            .expect("deliver datom parses")
        else {
            panic!("Deliver datom must stay a Deliver query")
        };
        assert_eq!(request.delivery_id, "m-7f3a2c:7d41e0:0");
        assert_eq!(request.flow_id, "7d41e0");
        assert!(matches!(
            request.message,
            meta_signal_flow::Message::Soft(_)
        ));
    }

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
