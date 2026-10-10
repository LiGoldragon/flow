use datom_codec::{Actualizing, Budget, Datomizable, Potential};
use flow_defaults::{DefaultConfiguration, LaysOutDefaults, ReadsAnchors};
use meta_signal_flow::{
    Configuration, ConfigurationAssignment, ConfigurationFile, ConfigurationFilePath,
    ConfigurationFragment, ConfigureFiles, CreditSelection, HarnessProfileVector, Query,
    ResetRequest, Response,
};
use protos::{Compactable, Protosizable, ReaderBudget};
use std::{
    env, fs,
    io::{Read, Write},
    os::unix::net::UnixStream,
};

struct FlowMetaClient {
    socket: String,
}

trait ParsesMetaCommand {
    fn parse_command(&self, arguments: impl Iterator<Item = String>) -> Result<Query, String>;

    fn query_usage() -> String {
        "usage: flow-meta '<one inline Query or ConfigureFiles Datom>'".into()
    }

    fn registration_usage() -> String {
        "usage: flow-meta register-codex|register-claude <flow-id> <session-id> <herdr-session> <herdr-agent> <herdr-pane> <herdr-terminal> [endpoint]".into()
    }
}
trait CallsMetaNexus {
    fn call(&self, query: &Query) -> Result<Response, String>;
}

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

fn record_assignment<T: PartialEq>(
    target: &mut Option<(T, String)>,
    value: T,
    field: &str,
    source: &str,
) -> Result<(), String> {
    match target {
        Some((previous, first_source)) if previous != &value => Err(format!(
            "conflicting {field} assignments in {first_source} and {source}"
        )),
        Some(_) => Ok(()),
        None => {
            *target = Some((value, source.to_owned()));
            Ok(())
        }
    }
}

fn required_assignment<T>(target: Option<(T, String)>, field: &str) -> Result<T, String> {
    target
        .map(|(value, _)| value)
        .ok_or_else(|| format!("configuration fragments omit required field {field}"))
}

fn compose_configuration_files(paths: Vec<ConfigurationFilePath>) -> Result<Configuration, String> {
    let mut files = Vec::with_capacity(paths.len());
    for path in paths {
        let contents = fs::read_to_string(&path.path).map_err(|error| {
            format!("cannot read configuration fragment {}: {error}", path.path)
        })?;
        let configuration_fragment =
            parse_datom::<ConfigurationFragment>(contents, "ConfigurationFragment")
                .map_err(|error| format!("{}: {error}", path.path))?;
        files.push(ConfigurationFile {
            configuration_file_path: path,
            configuration_fragment,
        });
    }

    let mut ordinary_socket_path = None;
    let mut meta_socket_path = None;
    let mut source_root = None;
    let mut stable_codex = None;
    let mut next_codex = None;
    let mut harness_profile_vector: Option<(HarnessProfileVector, String)> = None;
    let mut meta_aspects = None;
    let mut message_nexus_path = None;

    for file in files {
        let source = file.configuration_file_path.path;
        for assignment in file.configuration_fragment.configuration_assignment_vector {
            match assignment {
                ConfigurationAssignment::OrdinarySocketPath(value) => record_assignment(
                    &mut ordinary_socket_path,
                    value,
                    "OrdinarySocketPath",
                    &source,
                )?,
                ConfigurationAssignment::MetaSocketPath(value) => {
                    record_assignment(&mut meta_socket_path, value, "MetaSocketPath", &source)?
                }
                ConfigurationAssignment::SourceRoot(value) => {
                    record_assignment(&mut source_root, value, "SourceRoot", &source)?
                }
                ConfigurationAssignment::StableCodex(value) => {
                    record_assignment(&mut stable_codex, value, "StableCodex", &source)?
                }
                ConfigurationAssignment::NextCodex(value) => {
                    record_assignment(&mut next_codex, value, "NextCodex", &source)?
                }
                ConfigurationAssignment::HarnessProfileVector(value) => record_assignment(
                    &mut harness_profile_vector,
                    value,
                    "HarnessProfileVector",
                    &source,
                )?,
                ConfigurationAssignment::MetaAspects(value) => {
                    record_assignment(&mut meta_aspects, value, "MetaAspects", &source)?
                }
                ConfigurationAssignment::MessageNexusPath(value) => {
                    record_assignment(&mut message_nexus_path, value, "MessageNexusPath", &source)?
                }
            }
        }
    }

    let mut missing = Vec::new();
    if ordinary_socket_path.is_none() {
        missing.push("OrdinarySocketPath");
    }
    if meta_socket_path.is_none() {
        missing.push("MetaSocketPath");
    }
    if source_root.is_none() {
        missing.push("SourceRoot");
    }
    if stable_codex.is_none() {
        missing.push("StableCodex");
    }
    if next_codex.is_none() {
        missing.push("NextCodex");
    }
    if harness_profile_vector.is_none() {
        missing.push("HarnessProfileVector");
    }
    if meta_aspects.is_none() {
        missing.push("MetaAspects");
    }
    if message_nexus_path.is_none() {
        missing.push("MessageNexusPath");
    }
    if !missing.is_empty() {
        return Err(format!(
            "configuration fragments omit required fields: {}",
            missing.join(", ")
        ));
    }

    Ok(Configuration {
        ordinary_socket_path: required_assignment(ordinary_socket_path, "OrdinarySocketPath")?,
        meta_socket_path: required_assignment(meta_socket_path, "MetaSocketPath")?,
        source_root: required_assignment(source_root, "SourceRoot")?,
        stable_codex: required_assignment(stable_codex, "StableCodex")?,
        next_codex: required_assignment(next_codex, "NextCodex")?,
        harness_profile_vector: required_assignment(
            harness_profile_vector,
            "HarnessProfileVector",
        )?,
        meta_aspects: required_assignment(meta_aspects, "MetaAspects")?,
        message_nexus_path: required_assignment(message_nexus_path, "MessageNexusPath")?,
    })
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
                    flow_lifecycle: signal_flow::FlowLifecycle::Active,
                }))
            }
            // Ethos declares ConfigureFiles as a struct, whose generated
            // Datom form is positional and brace-delimited (unlike Query's
            // named variant forms).
            Some(datom) if datom.trim_start().starts_with('{') => {
                if arguments.next().is_some() {
                    return Err(Self::query_usage());
                }
                let input = parse_datom::<ConfigureFiles>(datom.to_owned(), "ConfigureFiles")?;
                compose_configuration_files(input.configuration_file_path_vector)
                    .map(Query::Configure)
            }
            // A capitalized argument is one inline meta Query datom, the
            // shape every request takes once the word commands retire.
            Some(datom) if datom.starts_with(|first: char| first.is_ascii_uppercase()) => {
                if arguments.next().is_some() {
                    return Err(Self::query_usage());
                }
                parse_datom::<Query>(datom.to_owned(), "Flow meta query")
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
    use meta_signal_flow::{ConfigurationFilePath, ConfigureFiles, CreditSelection, Query};
    use protos::{Compactable, Protosizable};
    use std::{fs, path::Path};

    fn write_fragment(directory: &Path, name: &str, fragment: &str) -> String {
        let path = directory.join(name);
        fs::write(&path, fragment).expect("write fixture fragment");
        path.to_string_lossy().into_owned()
    }

    fn configure_files_datom(paths: &[String]) -> String {
        ConfigureFiles {
            configuration_file_path_vector: paths
                .iter()
                .map(|path| ConfigurationFilePath { path: path.clone() })
                .collect(),
        }
        .datomize(vec![])
        .protosize()
        .compact()
    }

    fn parse_files(paths: &[String]) -> Result<Query, String> {
        FlowMetaClient {
            socket: "unused".into(),
        }
        .parse_command([configure_files_datom(paths)].into_iter())
    }

    fn fragment(assignments: &str) -> String {
        format!("{{ [ {assignments} ] }}")
    }

    const SOCKETS_AND_ROOT: &str = "OrdinarySocketPath./run/user/1000/flow.sock MetaSocketPath./run/user/1000/flow-meta.sock SourceRoot./home/test/primary";
    const CODEX_ENDPOINTS: &str = "StableCodex.{ /usr/bin/codex-stable /home/test/.codex /home/test/.codex/app-server-control/control.sock [ gpt-6-luna ] } NextCodex.{ /usr/bin/codex-next /home/test/.codex-next /home/test/.codex-next/app-server-control/control.sock [ gpt-6-sol ] }";
    const DELIVERY_SETTINGS: &str = "HarnessProfileVector.[ { Claude [ / «!» # ] [ esc ] [ enter ] } { Codex [ / «!» ] [ esc ] [] } ] MetaAspects.[ Psyche ] MessageNexusPath./usr/bin/message-nexus";

    #[test]
    fn configure_files_compose_multiple_fragments_and_accept_equal_duplicates() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let paths = [
            write_fragment(
                directory.path(),
                "sockets.datom",
                &fragment(SOCKETS_AND_ROOT),
            ),
            write_fragment(
                directory.path(),
                "codex.datom",
                &fragment(
                    "SourceRoot./home/test/primary ", /* identical assignment */
                ),
            ),
            write_fragment(
                directory.path(),
                "endpoints.datom",
                &fragment(CODEX_ENDPOINTS),
            ),
            write_fragment(
                directory.path(),
                "delivery.datom",
                &fragment(DELIVERY_SETTINGS),
            ),
        ];

        let Query::Configure(configuration) = parse_files(&paths).expect("complete config") else {
            panic!("ConfigureFiles must lower to the existing Configure query")
        };
        assert_eq!(
            configuration.ordinary_socket_path,
            "/run/user/1000/flow.sock"
        );
        assert_eq!(
            configuration.meta_socket_path,
            "/run/user/1000/flow-meta.sock"
        );
        assert_eq!(configuration.source_root, "/home/test/primary");
        assert_eq!(configuration.stable_codex.model_name_vector, ["gpt-6-luna"]);
        assert_eq!(configuration.next_codex.model_name_vector, ["gpt-6-sol"]);
        assert_eq!(configuration.harness_profile_vector.len(), 2);
        assert_eq!(
            configuration.meta_aspects,
            [signal_flow::FlowAspect::Psyche]
        );
        assert_eq!(configuration.message_nexus_path, "/usr/bin/message-nexus");
    }

    #[test]
    fn configure_files_reject_conflicting_assignments_with_both_sources() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let first = write_fragment(
            directory.path(),
            "first.datom",
            &fragment("OrdinarySocketPath./run/first.sock"),
        );
        let second = write_fragment(
            directory.path(),
            "second.datom",
            &fragment("OrdinarySocketPath./run/second.sock"),
        );

        let error = parse_files(&[first.clone(), second.clone()]).expect_err("conflict");
        assert!(error.contains("OrdinarySocketPath"));
        assert!(error.contains(&first));
        assert!(error.contains(&second));
    }

    #[test]
    fn configure_files_reject_incomplete_and_trailing_fragment_datoms() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let incomplete = write_fragment(
            directory.path(),
            "incomplete.datom",
            &fragment("OrdinarySocketPath./run/flow.sock"),
        );
        let error = parse_files(&[incomplete]).expect_err("missing fields");
        assert!(error.contains("MetaSocketPath"));
        assert!(error.contains("MessageNexusPath"));

        let trailing = write_fragment(
            directory.path(),
            "trailing.datom",
            "{ [] } Unexpected.{ value }",
        );
        let error = parse_files(&[trailing.clone()]).expect_err("trailing form");
        assert!(error.contains("invalid ConfigurationFragment datom"));
        assert!(error.contains(&trailing));
    }

    #[test]
    fn configure_files_is_one_cli_datom_without_extra_arguments() {
        let client = FlowMetaClient {
            socket: "unused".into(),
        };
        assert!(
            client
                .parse_command(["{ [] }".into(), "extra".into()].into_iter())
                .is_err()
        );
        assert!(
            client
                .parse_command(["configure-files".into(), "/tmp/a".into()].into_iter())
                .is_err()
        );
    }

    #[test]
    fn configure_is_one_inline_datom_carrying_the_whole_configuration() {
        let client = FlowMetaClient {
            socket: "unused".into(),
        };
        let Query::Configure(configuration) = client
            .parse_command(
                ["Configure.{ /run/user/1001/flow/flow.sock /run/user/1001/flow/flow-meta.sock /home/li/primary { /etc/profiles/per-user/li/bin/codex-stable-flow-client /home/li/.codex /home/li/.codex/app-server-control/app-server-control.sock [ gpt-5.6-terra gpt-5.6-sol gpt-5.6-luna ] } { /etc/profiles/per-user/li/bin/codex-next-flow-client /home/li/.codex-next /home/li/.codex-next/app-server-control/app-server-control.sock [ gpt-6-sol gpt-6-luna gpt-6-astra ] } [ { Claude [ / «!» # ] [ esc esc ] [ enter ] } { Codex [ / «!» ] [ esc ] [] } ] [ Psyche ] /etc/profiles/per-user/li/bin/message-nexus }".into()]
                    .into_iter(),
            )
            .expect("configure datom parses")
        else {
            panic!("Configure datom must stay a Configure query")
        };
        assert_eq!(configuration.source_root, "/home/li/primary");
        assert_eq!(
            configuration.next_codex.model_name_vector,
            ["gpt-6-sol", "gpt-6-luna", "gpt-6-astra"]
        );
        assert_eq!(
            configuration.harness_profile_vector[0].interrupt_keys,
            ["esc", "esc"]
        );
        assert_eq!(
            configuration.harness_profile_vector[1].command_sigil_vector,
            ["/", "!"]
        );
        assert_eq!(
            configuration.meta_aspects,
            [signal_flow::FlowAspect::Psyche]
        );
        assert!(
            client
                .parse_command(["configure".into(), "/a".into(), "/b".into()].into_iter())
                .is_err()
        );
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
