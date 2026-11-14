use datom_codec::{Actualizing, Budget, Datomizable, Potential};
use flow_ethos::place::{
    CliEnvironment, NamesPlaceSockets, PlaceSockets, ReadsCliEnvironment, ResolvesCliPlace,
};
use flow_meta::Invocation;
use meta_signal_flow::{Input, Output};
use protos::{Compactable, Protosizable, ReaderBudget};
use std::{
    env,
    io::{Read, Write},
    os::unix::net::UnixStream,
};

struct FlowMetaClient;

/// The CLI is `flow-meta '<Input>'`: it takes the datom and nothing
/// else. The place is the Flow configuration's,
/// `$XDG_CONFIG_HOME/nexus/flow-nexus.datom`, the same file `flow`
/// reads, resolved under `$XDG_RUNTIME_DIR/nexus/` to the place's meta
/// socket; no place is typed.
trait ParsesMetaCommand {
    fn parse_command(
        &self,
        arguments: impl Iterator<Item = String>,
        place: &dyn ResolvesCliPlace,
    ) -> Result<Invocation, String>;
}
/// A connected meta peer settles its contract before sending a query.
trait GreetsMetaNexus {
    fn greet_meta(&mut self) -> Result<(), String>;
}
trait CallsMetaNexus {
    /// Sends one query and hands each reply frame on: one, or for
    /// ObserveConfiguration every frame until the Nexus ends the exchange.
    fn call(&self, invocation: &Invocation, each: &mut dyn FnMut(Output)) -> Result<(), String>;
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
            reader: ReaderBudget::try_from(65_536).expect("a constant reader budget"),
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
        place: &dyn ResolvesCliPlace,
    ) -> Result<Invocation, String> {
        let usage = || {
            "usage: flow-meta '<one inline Input datom>'; the place is read from $XDG_CONFIG_HOME/nexus/flow-nexus.datom".to_owned()
        };
        let (Some(source), None) = (arguments.next(), arguments.next()) else {
            return Err(usage());
        };
        let runtime = place.place_runtime()?;
        let mut budget = Budget {
            remaining: 65_536,
            reader: ReaderBudget::try_from(65_536).expect("a constant reader budget"),
            depth: 0,
            maximum_depth: 1_024,
        };
        let query = Potential::<Input>::from(source)
            .actualize(&mut budget)
            .map_err(|error| format!("invalid Flow meta Input: {error:?}"))?;
        Ok(Invocation {
            meta_socket_path: flow_ethos::MetaSocketPath(
                PlaceSockets::meta_in(&runtime)
                    .to_string_lossy()
                    .into_owned(),
            ),
            input: query,
        })
    }
}

impl GreetsMetaNexus for UnixStream {
    fn greet_meta(&mut self) -> Result<(), String> {
        use signal::Contracted;
        let greeting = Input::greeting();
        let bytes =
            rkyv::to_bytes::<rkyv::rancor::Error>(&greeting).map_err(|error| error.to_string())?;
        self.write_all(&(bytes.len() as u32).to_be_bytes())
            .and_then(|_| self.write_all(&bytes))
            .map_err(|error| error.to_string())?;
        let mut length = [0; 4];
        self.read_exact(&mut length)
            .map_err(|error| error.to_string())?;
        let length = u32::from_be_bytes(length) as usize;
        if length > 1024 * 1024 {
            return Err("Signal frame exceeds 1 MiB".into());
        }
        let mut bytes = vec![0; length];
        self.read_exact(&mut bytes)
            .map_err(|error| error.to_string())?;
        let receipt = rkyv::from_bytes::<signal::HandshakeReceipt, rkyv::rancor::Error>(&bytes)
            .map_err(|error| error.to_string())?;
        match receipt {
            signal::HandshakeReceipt::Greeted(digest) if digest == greeting.contract_digest => {
                Ok(())
            }
            receipt => Err(format!("meta greeting refused: {receipt:?}")),
        }
    }
}

impl CallsMetaNexus for FlowMetaClient {
    fn call(&self, invocation: &Invocation, each: &mut dyn FnMut(Output)) -> Result<(), String> {
        let mut peer = UnixStream::connect(&invocation.meta_socket_path.0)
            .map_err(|error| error.to_string())?;
        peer.greet_meta()?;
        let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&invocation.input)
            .map_err(|error| error.to_string())?;
        peer.write_all(&(bytes.len() as u32).to_be_bytes())
            .map_err(|error| error.to_string())?;
        peer.write_all(&bytes).map_err(|error| error.to_string())?;
        let streams = invocation.input == Input::ObserveConfiguration;
        loop {
            let mut length = [0; 4];
            match peer.read_exact(&mut length) {
                Ok(()) => {}
                Err(error) if streams && error.kind() == std::io::ErrorKind::UnexpectedEof => {
                    return Ok(());
                }
                Err(error) => return Err(error.to_string()),
            }
            let length = u32::from_be_bytes(length) as usize;
            if length > 1024 * 1024 {
                return Err("Signal frame exceeds 1 MiB".into());
            }
            let mut reply = vec![0; length];
            peer.read_exact(&mut reply)
                .map_err(|error| error.to_string())?;
            each(
                rkyv::from_bytes::<Output, rkyv::rancor::Error>(&reply)
                    .map_err(|error| error.to_string())?,
            );
            if !streams {
                return Ok(());
            }
        }
    }
}

fn main() {
    let client = FlowMetaClient;
    match client
        .parse_command(env::args().skip(1), &CliEnvironment::of_process())
        .and_then(|invocation| {
            client.call(&invocation, &mut |reply| {
                println!("{}", reply.datomize(vec![]).protosize().compact())
            })
        }) {
        Ok(()) => {}
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
        ) -> Result<meta_signal_flow::Input, String>;
    }

    impl ParsesTestQuery for FlowMetaClient {
        fn parse_query(
            &self,
            mut arguments: impl Iterator<Item = String>,
        ) -> Result<meta_signal_flow::Input, String> {
            let Some(query) = arguments.next() else {
                return Err("missing fixture".into());
            };
            if arguments.next().is_some() {
                return Err("extra fixture".into());
            }
            self.parse_command([query].into_iter(), &FixedPlace)
                .map(|Invocation { input, .. }| input)
        }
    }

    /// The place a test's configuration names, read from no file.
    struct FixedPlace;

    impl flow_ethos::place::ResolvesCliPlace for FixedPlace {
        fn place_runtime(&self) -> Result<std::path::PathBuf, String> {
            Ok("/run/user/test/nexus/main".into())
        }
    }
    use datom_codec::Datomizable;
    use meta_signal_flow::{ConfigurationChange, ConfigureNexus, Input};
    use protos::{Compactable, Protosizable};

    #[test]
    fn configuration_uses_the_current_typed_nexus_change() {
        let endpoint = || meta_signal_flow::CodexEndpoint {
            client_path: flow_ethos::ClientPath("/opt/codex".into()),
            home: flow_ethos::Home("/tmp/codex".into()),
            control_socket_path: flow_ethos::ControlSocketPath("/tmp/codex.sock".into()),
            model_name_vector: vec![flow_ethos::ModelName("fixture".into())],
        };
        let nexus = ConfigureNexus {
            source_root: flow_ethos::SourceRoot("/source".into()),
            first_codex_endpoint: endpoint(),
            second_codex_endpoint: endpoint(),
            harness_profiles: flow_ethos::HarnessProfiles(vec![]),
            meta_aspects: flow_ethos::MetaAspects(vec![meta_signal_flow::Aspect::Psyche]),
            message_nexus_path_option: Some(flow_ethos::MessageNexusPath(
                "/run/message.sock".into(),
            )),

            message_nexus_binary_option: Some(flow_ethos::MessageNexusBinary(
                "/opt/message-nexus".into(),
            )),

            curriculum_nexus_path: meta_signal_flow::CurriculumNexusPath(
                "/fixture/curriculum.sock".into(),
            ),
            lease: flow_ethos::Lease(60),
            presentation_prompt_option: None,
            herdr_session_name: signal_flow::HerdrSessionName("default".into()),
        };
        let query = Input::Configure(ConfigurationChange::Nexus(nexus.clone()));
        let datom = query.datomize(vec![]).protosize().compact();
        let client = FlowMetaClient;
        let Input::Configure(ConfigurationChange::Nexus(actual)) = client
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
        // Sockets belong to the place. The removed fragment wire type
        // cannot silently be lowered into a different Nexus contract.
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
        let datom = "Configure.Metaflows.[ { { Field core Astra } None [ CodexAgentsMd CodexSkillListing ] [ spirit:core:0 ] [] Regular Default OnDemand «» [] } { { Psyche flow Opus } None [ SkillTool WorkflowTool ClaudeMd ] [ spirit:core:0 ] [] Regular Default OnDemand «» [] } ]";
        let query = client
            .parse_query([datom.to_owned()].into_iter())
            .expect("Metaflows declaration parses");
        let Input::Configure(ConfigurationChange::Metaflows(metaflows)) = &query else {
            panic!("a Metaflows declaration must stay one Configure query")
        };
        assert_eq!(metaflows.len(), 2);
        assert_eq!(
            metaflows[0].leave_outs.0,
            [
                signal_flow::LeaveOut::CodexAgentsMd,
                signal_flow::LeaveOut::CodexSkillListing
            ]
        );
        assert_eq!(query.datomize(vec![]).protosize().compact(), datom);
        // The leave-out family is gone: a permaflow carries its own.
        assert!(
            client
                .parse_query(
                    [
                        "Configure.LeaveOuts.[ { { Psyche flow Opus } Claude [ SkillTool ClaudeMd ] } ]"
                            .to_owned()
                    ]
                    .into_iter()
                )
                .is_err()
        );
        // One value of one permaflow is one inline datom too.
        let amend = "Configure.Amend.{ { Psyche flow Opus } Add.LeaveOut.GitInstructions }";
        let query = client
            .parse_query([amend.to_owned()].into_iter())
            .expect("Amend parses");
        assert!(matches!(
            &query,
            Input::Configure(ConfigurationChange::Amend(_))
        ));
        assert_eq!(query.datomize(vec![]).protosize().compact(), amend);
        // Reconfigure is not a request: Configure.Nexus replaces the Nexus.
        assert!(
            client
                .parse_query(["Reconfigure.{ /home/li/primary }".to_owned()].into_iter())
                .is_err()
        );
    }

    #[test]
    fn refresh_and_restart_are_the_meta_surface_s_own() {
        // The meta surface refreshes an Address and restarts a flow, a
        // protected Address's included.
        let client = FlowMetaClient;
        assert_eq!(
            client
                .parse_query(["Refresh.{ Mind flow Sol }".to_owned()].into_iter())
                .expect("Refresh parses"),
            Input::Refresh(signal_flow::Address {
                aspect: signal_flow::Aspect::Mind,
                topic: flow_ethos::Topic("flow".into()),
                model: signal_flow::Model::Sol,
            })
        );
        assert_eq!(
            client
                .parse_query(["Restart.c1a0de".to_owned()].into_iter())
                .expect("Restart parses"),
            Input::Restart(flow_ethos::FlowId("c1a0de".into()))
        );
    }

    #[test]
    fn deliver_is_one_inline_datom_carrying_a_typed_message() {
        let client = FlowMetaClient;
        let Input::Deliver(request) = client
            .parse_query(
                ["Deliver.{ m-7f3a2c:7d41e0:0 7d41e0 Soft.{ Notification Text.«Stage 1 is deployed; run the tier tests.» } }".into()]
                    .into_iter(),
            )
            .expect("deliver datom parses")
        else {
            panic!("Deliver datom must stay a Deliver query")
        };
        assert_eq!(
            request.delivery_id,
            meta_signal_flow::DeliveryId("m-7f3a2c:7d41e0:0".into())
        );
        assert_eq!(request.flow_id, flow_ethos::FlowId("7d41e0".into()));
        assert!(matches!(
            request.message,
            meta_signal_flow::Message::Soft(_)
        ));
    }

    #[test]
    fn thirty_one_printed_metaflows_roundtrip_without_head_whitespace() {
        use super::ParsesMetaDatom;
        use meta_signal_flow::{ConfigurationSnapshot, ConfigureMetaflow, Output};
        use signal_flow::{Address, Aspect, FlowKind, Model, ToolSet};
        let endpoint = || meta_signal_flow::CodexEndpoint {
            client_path: flow_ethos::ClientPath("/opt/codex".into()),
            home: flow_ethos::Home("/tmp/codex".into()),
            control_socket_path: flow_ethos::ControlSocketPath("/tmp/codex.sock".into()),
            model_name_vector: vec![flow_ethos::ModelName("fixture".into())],
        };
        let rows: Vec<_> = (0..31)
            .map(|index| ConfigureMetaflow {
                root_option: None,
                leave_outs: signal_flow::LeaveOuts(match index % 2 {
                    0 => vec![],
                    _ => vec![signal_flow::LeaveOut::CodexAgentsMd],
                }),
                address: Address {
                    aspect: Aspect::Mind,
                    topic: flow_ethos::Topic(format!("printer{index}")),
                    model: Model::Sol,
                },
                system_modules: signal_flow::SystemModules(vec![signal_flow::ModuleIdentity(
                    "spirit:core:0".into(),
                )]),
                prompt_modules: signal_flow::PromptModules(vec![]),
                flow_kind: FlowKind::Regular,
                tool_set: match index % 3 {
                    0 => ToolSet::Default,
                    1 => ToolSet::Explicit(vec![]),
                    _ => ToolSet::Explicit(vec!["Bash".into(), "ToolSearch".into()]),
                },
                continuity: meta_signal_flow::Continuity::OnDemand,
                purpose: meta_signal_flow::Purpose(format!("Prints page {index}.")),
                records: meta_signal_flow::Records(vec![]),
            })
            .collect();
        let snapshot = ConfigurationSnapshot {
            configured_nexus: meta_signal_flow::ConfiguredNexus(ConfigureNexus {
                source_root: flow_ethos::SourceRoot("/source".into()),
                first_codex_endpoint: endpoint(),
                second_codex_endpoint: endpoint(),
                harness_profiles: flow_ethos::HarnessProfiles(vec![]),
                meta_aspects: flow_ethos::MetaAspects(vec![Aspect::Psyche]),
                message_nexus_path_option: Some(flow_ethos::MessageNexusPath(
                    "/run/message.sock".into(),
                )),

                message_nexus_binary_option: Some(flow_ethos::MessageNexusBinary(
                    "/opt/message-nexus".into(),
                )),

                curriculum_nexus_path: meta_signal_flow::CurriculumNexusPath(
                    "/fixture/curriculum.sock".into(),
                ),
                lease: flow_ethos::Lease(60),
                presentation_prompt_option: None,
                herdr_session_name: signal_flow::HerdrSessionName("default".into()),
            }),
            configured_models: meta_signal_flow::ConfiguredModels(vec![]),
            configured_thresholds: meta_signal_flow::ConfiguredThresholds(vec![]),
            configured_send_servers: meta_signal_flow::ConfiguredSendServers(vec![]),
            configured_metaflows: meta_signal_flow::ConfiguredMetaflows(rows.clone()),
            configured_harness_flags: meta_signal_flow::ConfiguredHarnessFlags(vec![]),
            configured_investigation: meta_signal_flow::ConfiguredInvestigation(None),
            configured_default_minutes: meta_signal_flow::ConfiguredDefaultMinutes(
                flow_ethos::Minutes(30),
            ),
            configured_heartbeat: meta_signal_flow::ConfiguredHeartbeat(None),
            configured_protected_addresses: meta_signal_flow::ConfiguredProtectedAddresses(vec![]),
            configured_handover_policies: meta_signal_flow::ConfiguredHandoverPolicies(
                meta_signal_flow::HandoverPoliciesConfiguration::Unconfigured,
            ),
            configured_handover_archive: meta_signal_flow::ConfiguredHandoverArchive(
                meta_signal_flow::HandoverArchiveConfiguration::Unconfigured,
            ),
            configured_handover_count: meta_signal_flow::ConfiguredHandoverCount(
                meta_signal_flow::HandoverCountConfiguration::Unconfigured,
            ),
            configured_publication_deadline: meta_signal_flow::ConfiguredPublicationDeadline(
                meta_signal_flow::Duration(60_000),
            ),
            configured_counters: meta_signal_flow::ConfiguredCounters(
                meta_signal_flow::ConfigureCounters {
                    logical_delivery: meta_signal_flow::LogicalDelivery(1),
                    queue_attempt: meta_signal_flow::QueueAttempt(3),
                    send_outcome: meta_signal_flow::SendOutcome(1),
                },
            ),
            configured_request_id_draws: meta_signal_flow::ConfiguredRequestIdDraws(
                meta_signal_flow::DrawBound(64),
            ),
            configured_announcement_tries: meta_signal_flow::ConfiguredAnnouncementTries(
                meta_signal_flow::TryBound(3),
            ),
            configured_harness_hook: meta_signal_flow::ConfiguredHarnessHook(
                meta_signal_flow::HarnessHookConfiguration::Unconfigured,
            ),
        };
        let printed = Output::Configuration(snapshot)
            .datomize(vec![])
            .protosize()
            .compact();
        assert!(!printed.contains("Explicit. ["), "{printed}");
        let Output::Configuration(parsed) =
            FlowMetaClient::parse_datom::<Output>(printed, "printed configuration").unwrap()
        else {
            panic!("wrong response")
        };
        let query = Input::Configure(ConfigurationChange::Metaflows(
            parsed.configured_metaflows.0,
        ));
        let text = query.datomize(vec![]).protosize().compact();
        let client = FlowMetaClient;
        let Input::Configure(ConfigurationChange::Metaflows(actual)) =
            client.parse_query([text].into_iter()).unwrap()
        else {
            panic!("wrong configure")
        };
        assert_eq!(actual, rows);
    }
}
