use datom_codec::{Actualizing, Budget, Datomizable, Potential};
use flow_meta::Invocation;
use meta_signal_flow::Response;
use protos::{Compactable, Protosizable, ReaderBudget};
use std::{
    env,
    io::{Read, Write},
    os::unix::net::UnixStream,
};

struct FlowMetaClient;

trait ParsesMetaCommand {
    fn parse_command(&self, arguments: impl Iterator<Item = String>) -> Result<Invocation, String>;
}
trait CallsMetaNexus {
    fn call(&self, invocation: &Invocation) -> Result<Response, String>;
}
#[cfg(test)]
trait ParsesMetaDatom {
    fn parse_datom<T: datom_codec::Composing>(source: String, subject: &str) -> Result<T, String>;
}

#[cfg(test)]
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
    fn parse_command(
        &self,
        mut arguments: impl Iterator<Item = String>,
    ) -> Result<Invocation, String> {
        let Some(source) = arguments.next() else {
            return Err("usage: flow-meta '<one inline Invocation datom>'".into());
        };
        if arguments.next().is_some() {
            return Err("usage: flow-meta '<one inline Invocation datom>'".into());
        }
        let mut budget = Budget {
            remaining: 65_536,
            reader: ReaderBudget { remaining: 65_536 },
            depth: 0,
            maximum_depth: 1_024,
        };
        Potential::<Invocation>::from(source)
            .actualize(&mut budget)
            .map_err(|error| format!("invalid Flow meta invocation: {error:?}"))
    }
}

impl CallsMetaNexus for FlowMetaClient {
    fn call(&self, invocation: &Invocation) -> Result<Response, String> {
        let mut peer =
            UnixStream::connect(&invocation.meta_socket_path).map_err(|error| error.to_string())?;
        let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&invocation.query)
            .map_err(|error| error.to_string())?;
        peer.write_all(&(bytes.len() as u32).to_be_bytes())
            .map_err(|error| error.to_string())?;
        peer.write_all(&bytes).map_err(|error| error.to_string())?;
        let mut length = [0; 4];
        peer.read_exact(&mut length)
            .map_err(|error| error.to_string())?;
        let length = u32::from_be_bytes(length) as usize;
        if length > 1024 * 1024 {
            return Err("Signal frame exceeds 1 MiB".into());
        }
        let mut reply = vec![0; length];
        peer.read_exact(&mut reply)
            .map_err(|error| error.to_string())?;
        rkyv::from_bytes::<Response, rkyv::rancor::Error>(&reply).map_err(|error| error.to_string())
    }
}

fn main() {
    let client = FlowMetaClient;
    match client
        .parse_command(env::args().skip(1))
        .and_then(|invocation| client.call(&invocation))
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
    use flow_meta::Invocation;

    trait ParsesTestQuery {
        fn parse_query(
            &self,
            arguments: impl Iterator<Item = String>,
        ) -> Result<meta_signal_flow::Query, String>;
    }

    impl ParsesTestQuery for FlowMetaClient {
        fn parse_query(
            &self,
            mut arguments: impl Iterator<Item = String>,
        ) -> Result<meta_signal_flow::Query, String> {
            let Some(query) = arguments.next() else {
                return Err("missing fixture".into());
            };
            if arguments.next().is_some() {
                return Err("extra fixture".into());
            }
            self.parse_command([format!("{{ unused {query} }}")].into_iter())
                .map(|Invocation { query, .. }| query)
        }
    }
    use datom_codec::Datomizable;
    use meta_signal_flow::{ConfigurationChange, ConfigureNexus, Query};
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
            curriculum_nexus_path: "/fixture/curriculum.sock".into(),
            lease: 60,
        };
        let query = Query::Configure(ConfigurationChange::Nexus(nexus.clone()));
        let datom = query.datomize(vec![]).protosize().compact();
        let client = FlowMetaClient;
        let Query::Configure(ConfigurationChange::Nexus(actual)) = client
            .parse_query([datom.clone()].into_iter())
            .expect("typed Nexus Configure")
        else {
            panic!("wrong configuration variant")
        };
        assert_eq!(actual, nexus);
        assert!(
            client
                .parse_query([datom, "extra".into()].into_iter())
                .is_err()
        );
        // Sockets and StorePath belong to Start. The removed fragment wire
        // type cannot silently be lowered into a different Nexus contract.
        assert!(client.parse_query(["{ [] }".into()].into_iter()).is_err());
        assert!(
            client
                .parse_query(["configure-files".into(), "/tmp/old".into()].into_iter())
                .is_err()
        );
    }

    #[test]
    fn a_family_declaration_is_one_inline_datom_carrying_the_whole_family() {
        let client = FlowMetaClient;
        let datom = "Configure.LeaveOuts.[ { { Field core Primary } Codex [ CodexAgentsMd CodexSkillListing ] } { { Psyche flow Secondary } Claude [ SkillTool ClaudeMd ] } ]";
        let query = client
            .parse_query([datom.to_owned()].into_iter())
            .expect("LeaveOuts declaration parses");
        let Query::Configure(ConfigurationChange::LeaveOuts(leave_outs)) = &query else {
            panic!("a LeaveOuts declaration must stay one Configure query")
        };
        assert_eq!(leave_outs.len(), 2);
        assert_eq!(leave_outs[0].harness_kind, signal_flow::HarnessKind::Codex);
        assert_eq!(query.datomize(vec![]).protosize().compact(), datom);
        // An entry that names no harness is not a leave-out default.
        assert!(
            client
                .parse_query(
                    ["Configure.LeaveOuts.[ { { Psyche flow Secondary } [ SkillTool ClaudeMd ] } ]"
                        .to_owned()]
                    .into_iter()
                )
                .is_err()
        );
        // Reconfigure is not a request: Configure.Nexus replaces the Nexus.
        assert!(
            client
                .parse_query(["Reconfigure.{ /home/li/primary }".to_owned()].into_iter())
                .is_err()
        );
    }

    #[test]
    fn refresh_is_not_a_request() {
        // Every wake is fresh, so no request marks one.
        let client = FlowMetaClient;
        assert!(
            client
                .parse_query(["Refresh.{ Mind flow Secondary }".to_owned()].into_iter())
                .is_err()
        );
    }

    #[test]
    fn deliver_is_one_inline_datom_carrying_a_typed_message() {
        let client = FlowMetaClient;
        let Query::Deliver(request) = client
            .parse_query(
                ["Deliver.{ m-7f3a2c:7d41e0:0 7d41e0 Soft.{ Nexus Text.«Stage 1 is deployed; run the tier tests.» } }".into()]
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
    fn thirty_one_printed_metaflows_roundtrip_without_head_whitespace() {
        use super::ParsesMetaDatom;
        use meta_signal_flow::{ConfigurationSnapshot, ConfigureMetaflow, Response};
        use signal_flow::{Address, Aspect, FlowKind, Layer, ToolSet};
        let endpoint = || meta_signal_flow::CodexEndpoint {
            client_path: "/opt/codex".into(),
            home: "/tmp/codex".into(),
            control_socket_path: "/tmp/codex.sock".into(),
            model_name_vector: vec!["fixture".into()],
        };
        let rows: Vec<_> = (0..31)
            .map(|index| ConfigureMetaflow {
                root_option: None,
                address: Address {
                    aspect: Aspect::Mind,
                    topic: format!("printer{index}"),
                    layer: Layer::Secondary,
                },
                system_modules: vec!["spirit-core".into()],
                user_modules: vec![],
                flow_kind: FlowKind::Regular,
                tool_set: match index % 3 {
                    0 => ToolSet::Default,
                    1 => ToolSet::Explicit(vec![]),
                    _ => ToolSet::Explicit(vec!["Bash".into(), "ToolSearch".into()]),
                },
                continuity: meta_signal_flow::Continuity::OnDemand,
                purpose: format!("Prints page {index}."),
                records: vec![],
            })
            .collect();
        let snapshot = ConfigurationSnapshot {
            nexus: ConfigureNexus {
                source_root: "/source".into(),
                stable_codex: endpoint(),
                next_codex: endpoint(),
                harness_profiles: vec![],
                meta_aspects: vec![Aspect::Psyche],
                message_nexus_path: "/run/message.sock".into(),
                message_nexus_binary: "/opt/message-nexus".into(),
                curriculum_nexus_path: "/fixture/curriculum.sock".into(),
                lease: 60,
            },
            models: vec![],
            thresholds: vec![],
            default_leave_outs: vec![],
            send_servers: vec![],
            metaflows: rows.clone(),
            investigation: None,
            default_minutes: 30,
            heartbeat: None,
            counters: meta_signal_flow::ConfigureCounters {
                logical_delivery: 1,
                queue_attempt: 3,
                send_outcome: 1,
            },
        };
        let printed = Response::Configuration(snapshot)
            .datomize(vec![])
            .protosize()
            .compact();
        assert!(!printed.contains("Explicit. ["), "{printed}");
        let Response::Configuration(parsed) =
            FlowMetaClient::parse_datom::<Response>(printed, "printed configuration").unwrap()
        else {
            panic!("wrong response")
        };
        let query = Query::Configure(ConfigurationChange::Metaflows(parsed.metaflows));
        let text = query.datomize(vec![]).protosize().compact();
        let client = FlowMetaClient;
        let Query::Configure(ConfigurationChange::Metaflows(actual)) =
            client.parse_query([text].into_iter()).unwrap()
        else {
            panic!("wrong configure")
        };
        assert_eq!(actual, rows);
    }
}
