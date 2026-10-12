use datom_codec::{
    Actualizing, Budget, Composing, Datom, Datomizable, Error, ErrorKind, ErrorRaising, Potential,
    Variantizing,
};
use flow_defaults::{DefaultConfiguration, LaysOutDefaults, ReadsAnchors};
use protos::{Compactable, Protosizable, ReaderBudget};
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
    /// Sends one query and hands each reply frame on. Every query answers
    /// with one frame except Observe, whose frames run until the Nexus ends
    /// the exchange after the outcome.
    fn call(&self, query: &Query, each: &mut dyn FnMut(Response)) -> Result<(), String>;
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
    fn call(&self, query: &Query, each: &mut dyn FnMut(Response)) -> Result<(), String> {
        let mut peer = UnixStream::connect(&self.socket).map_err(|error| error.to_string())?;
        let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(query).map_err(|e| e.to_string())?;
        peer.write_all(&(bytes.len() as u32).to_be_bytes())
            .map_err(|e| e.to_string())?;
        peer.write_all(&bytes).map_err(|e| e.to_string())?;
        let streams = matches!(query, Query::Observe(_));
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
            peer.read_exact(&mut reply).map_err(|e| e.to_string())?;
            each(
                rkyv::from_bytes::<Response, rkyv::rancor::Error>(&reply)
                    .map_err(|e| e.to_string())?,
            );
            if !streams {
                return Ok(());
            }
        }
    }
}

impl TextualizesFlowReply for FlowClient {
    fn textualize_reply(&self, reply: &Response) -> String {
        reply.datomize(vec![]).protosize().compact()
    }
}

/// The one invocation that is not a datom: `--version` alone answers the
/// Cargo package version without reaching Flow Nexus. signal-flow carries no
/// Version query, so the version is answered at the CLI boundary.
trait AnswersVersion {
    fn version_answer(&self) -> Option<String>;
}

impl AnswersVersion for [String] {
    fn version_answer(&self) -> Option<String> {
        match self {
            [only] if only == "--version" => Some(format!("flow {}", env!("CARGO_PKG_VERSION"))),
            _ => None,
        }
    }
}

fn main() {
    let arguments = env::args().skip(1).collect::<Vec<_>>();
    if let Some(version) = arguments.version_answer() {
        println!("{version}");
        return;
    }
    // The Nexus's default ordinary socket under this caller's runtime
    // directory, unless the caller names another Nexus's socket.
    let client = FlowClient {
        socket: env::var("FLOW_SOCKET").unwrap_or_else(|_| {
            DefaultConfiguration::from_environment()
                .ordinary_socket_path()
                .to_string_lossy()
                .into_owned()
        }),
    };
    match client
        .parse_command(arguments.into_iter())
        .and_then(|query| {
            FlowClient::resolve_launch_skills(query, FlowClient::curriculum_resolve_skills)
        })
        .and_then(|query| {
            client.call(&query, &mut |reply| {
                println!("{}", client.textualize_reply(&reply))
            })
        }) {
        Ok(()) => {}
        Err(error) => {
            eprintln!("{error}");
            std::process::exit(2)
        }
    }
}

struct ResolvedSkills(Vec<String>);

impl Composing for ResolvedSkills {
    fn compose(datom: &Datom, budget: &mut Budget) -> Result<Self, Error> {
        let (name, body) = datom.variant(budget, "ResolvedSkills")?;
        if name != "ResolvedSkills" {
            return Err(Error::composition(
                datom.path.clone(),
                ErrorKind::Variant {
                    expected: "ResolvedSkills".into(),
                    found: name.into(),
                },
            ));
        }
        Vec::<String>::compose(body, budget).map(Self)
    }
}

trait ResolvesCurriculumSkills {
    fn curriculum_resolve_skills_argument(names: &[String]) -> String;
    fn parse_curriculum_skills_reply(reply: &str) -> Result<Vec<String>, String>;
    fn curriculum_resolve_skills(names: &[String]) -> Result<Vec<String>, String>;
    fn resolve_launch_skills(
        query: Query,
        resolve: impl FnOnce(&[String]) -> Result<Vec<String>, String>,
    ) -> Result<Query, String>;
}

impl ResolvesCurriculumSkills for FlowClient {
    fn curriculum_resolve_skills_argument(names: &[String]) -> String {
        names
            .to_vec()
            .datomize(vec![])
            .named_variant(vec![], "ResolveSkills")
            .protosize()
            .compact()
    }

    fn parse_curriculum_skills_reply(reply: &str) -> Result<Vec<String>, String> {
        let mut budget = Budget {
            remaining: 65_536,
            reader: ReaderBudget { remaining: 65_536 },
            depth: 0,
            maximum_depth: 1_024,
        };
        Potential::<ResolvedSkills>::from(reply.trim().to_owned())
            .actualize(&mut budget)
            .map(|resolved| resolved.0)
            .map_err(|error| format!("invalid Curriculum skill resolution reply: {error:?}"))
    }

    fn curriculum_resolve_skills(names: &[String]) -> Result<Vec<String>, String> {
        let argument = Self::curriculum_resolve_skills_argument(names);
        let output = std::process::Command::new("curriculum")
            .arg(&argument)
            .output()
            .map_err(|error| format!("could not run Curriculum skill resolution: {error}"))?;
        if !output.status.success() {
            let detail = String::from_utf8_lossy(&output.stderr);
            let detail = detail.trim();
            return Err(if detail.is_empty() {
                format!("Curriculum skill resolution failed with {}", output.status)
            } else {
                format!("Curriculum skill resolution failed: {detail}")
            });
        }
        let reply = String::from_utf8(output.stdout)
            .map_err(|error| format!("Curriculum returned non-UTF-8 skill resolution: {error}"))?;
        Self::parse_curriculum_skills_reply(&reply)
    }

    fn resolve_launch_skills(
        mut query: Query,
        resolve: impl FnOnce(&[String]) -> Result<Vec<String>, String>,
    ) -> Result<Query, String> {
        let profile = match &mut query {
            Query::Start(request) | Query::Replace(request) => &mut request.launch_profile,
            _ => return Ok(query),
        };
        let mut names = resolve(&profile.skill_name_vector)?;
        if names.iter().collect::<std::collections::HashSet<_>>().len() != names.len() {
            return Err("Curriculum returned duplicate resolved skills".into());
        }
        if let Some(index) = names.iter().position(|name| name == "operation-main-flow") {
            if index > 0 {
                let main_flow = names.remove(index);
                names.insert(0, main_flow);
            }
        }
        profile.skill_name_vector = names;
        Ok(query)
    }
}

#[cfg(test)]
mod tests {
    use super::{
        AnswersVersion, FlowClient, ParsesFlowCommand, ResolvesCurriculumSkills,
        TextualizesFlowReply,
    };
    use signal_flow::{Query, Response};

    #[test]
    fn version_is_the_one_non_datom_invocation() {
        assert_eq!(
            (&["--version".to_owned()] as &[String]).version_answer(),
            Some(format!("flow {}", env!("CARGO_PKG_VERSION")))
        );
        assert_eq!(
            (&["--version".to_owned(), "extra".to_owned()] as &[String]).version_answer(),
            None
        );
        assert_eq!(
            (&["List.{ }".to_owned()] as &[String]).version_answer(),
            None
        );
        assert_eq!((&[] as &[String]).version_answer(), None);
    }

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
                "Start.{ { request-7 [ { Vision/flowNexus.md 54c08e7190360a308e560935c120c69b81c4aacb4975751a4841912b599f4f5a } ] [ spirit operation-main-flow ] Field High Codex gpt-6-astra medium Some.836818 [ { 1b8ac0 1 } ] messaging-build /tmp/flow-system-prompt.md «Carry this bounded launch request.» } { fac697 session-1 turn-2 } }".into(),
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
            ["spirit", "operation-main-flow"]
        );
    }

    #[test]
    fn start_and_replace_expand_skills_before_submission() {
        let client = FlowClient {
            socket: "unused".into(),
        };
        let start = client
            .parse_command(["Start.{ { request-7 [] [ spirit operation-main-flow ] Field High Codex gpt-6-astra medium None [] messaging-build /tmp/flow-system-prompt.md «Carry this bounded launch request.» } { fac697 session-1 turn-2 } }".into()].into_iter())
            .unwrap();
        let start = FlowClient::resolve_launch_skills(start, |roots| {
            assert_eq!(roots, ["spirit", "operation-main-flow"]);
            Ok(vec![
                "compensation-behavior".into(),
                "spirit".into(),
                "operation-main-flow".into(),
            ])
        })
        .unwrap();
        let Query::Start(start) = start else {
            panic!("Start fixture must remain a Start query")
        };
        assert_eq!(
            start.launch_profile.skill_name_vector,
            ["operation-main-flow", "compensation-behavior", "spirit"]
        );

        let replace = client
            .parse_command(["Replace.{ { request-8 [] [ spirit ] Field High Claude opus-5-5 high Some.fac697 [] messaging-build /workspace/bundles/flow.md «Carry on from fac697.» } { fac697 session-1 turn-2 } }".into()].into_iter())
            .unwrap();
        let replace = FlowClient::resolve_launch_skills(replace, |roots| {
            assert_eq!(roots, ["spirit"]);
            Ok(vec!["compensation-behavior".into(), "spirit".into()])
        })
        .unwrap();
        let Query::Replace(replace) = replace else {
            panic!("Replace fixture must remain a Replace query")
        };
        assert_eq!(
            replace.launch_profile.skill_name_vector,
            ["compensation-behavior", "spirit"]
        );
    }

    #[test]
    fn skill_resolution_is_typed_and_non_launch_queries_pass_through() {
        assert_eq!(
            FlowClient::curriculum_resolve_skills_argument(&[
                "spirit".into(),
                "compensation-behavior".into(),
            ]),
            "ResolveSkills.[ spirit compensation-behavior ]"
        );
        let client = FlowClient {
            socket: "unused".into(),
        };
        let query = client
            .parse_command(["ResolveRecipient.fac697".into()].into_iter())
            .unwrap();
        let unchanged = FlowClient::resolve_launch_skills(query.clone(), |_| {
            panic!("non-launch queries must not resolve skills")
        })
        .unwrap();
        assert_eq!(unchanged, query);
    }

    #[test]
    fn curriculum_skill_resolution_reply_is_typed_and_ordered() {
        assert_eq!(
            FlowClient::parse_curriculum_skills_reply(
                "ResolvedSkills.[ compensation-behavior spirit operation-main-flow ]\n"
            )
            .unwrap(),
            ["compensation-behavior", "spirit", "operation-main-flow"]
        );
        assert!(FlowClient::parse_curriculum_skills_reply("Other.[ spirit ]").is_err());
    }

    #[test]
    fn resolver_failure_prevents_flow_submission() {
        let client = FlowClient {
            socket: "unused".into(),
        };
        let query = client
            .parse_command(["Start.{ { request-7 [] [ spirit ] Field High Codex gpt-6-astra medium None [] messaging-build /tmp/flow-system-prompt.md «Launch.» } { fac697 session-1 turn-2 } }".into()].into_iter())
            .unwrap();
        let mut submitted = false;
        let result = FlowClient::resolve_launch_skills(query, |_| Err("missing dependency".into()))
            .and_then(|resolved| {
                submitted = true;
                Ok(resolved)
            });
        assert_eq!(result.unwrap_err(), "missing dependency");
        assert!(!submitted);
    }

    #[test]
    fn main_flow_is_promoted_without_reordering_the_rest_of_the_closure() {
        let client = FlowClient {
            socket: "unused".into(),
        };
        let query = client
            .parse_command(["Start.{ { request-7 [] [ spirit operation-main-flow ] Field High Codex gpt-6-astra medium None [] messaging-build /tmp/flow-system-prompt.md «Launch.» } { fac697 session-1 turn-2 } }".into()].into_iter())
            .unwrap();
        let result = FlowClient::resolve_launch_skills(query, |_| {
            Ok(vec![
                "compensation-behavior".into(),
                "spirit".into(),
                "operation-main-flow".into(),
                "compensation-correction".into(),
            ])
        })
        .unwrap();
        let Query::Start(request) = result else {
            panic!("Start fixture must remain a Start query")
        };
        assert_eq!(
            request.launch_profile.skill_name_vector,
            [
                "operation-main-flow",
                "compensation-behavior",
                "spirit",
                "compensation-correction"
            ]
        );
    }

    #[test]
    fn replace_status_and_observe_are_inline_typed_queries() {
        let client = FlowClient {
            socket: "unused".into(),
        };
        assert_eq!(
            client
                .parse_command(["LaunchStatus.request-8".into()].into_iter())
                .unwrap(),
            Query::LaunchStatus("request-8".into())
        );
        assert_eq!(
            client
                .parse_command(["Observe.Launch.request-8".into()].into_iter())
                .unwrap(),
            Query::Observe(signal_flow::ObserveSelection::Launch("request-8".into()))
        );
        let Query::Replace(request) = client
            .parse_command(["Replace.{ { request-8 [] [ spirit ] Field High Claude opus-5-5 high Some.fac697 [] messaging-build /workspace/bundles/flow.md «Carry on from fac697.» } { fac697 session-1 turn-2 } }".into()].into_iter())
            .expect("typed Replace parses")
        else {
            panic!("Replace fixture must remain a Replace query")
        };
        assert_eq!(request.launch_profile.flow_id_option, Some("fac697".into()));
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
