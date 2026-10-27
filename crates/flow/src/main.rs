use datom_codec::{Budget, Composable, DatomForming, Datomizable, Form};
use flow_defaults::{DefaultConfiguration, LaysOutDefaults, ReadsAnchors};
use protos::{BoundedProtosizable, Compactable, Protosizable, ReaderBudget};
use signal_flow::{Delivery, Message, Query, Recipient, Response, SendRequest};
use std::{
    env,
    io::{Read, Write},
    os::unix::net::UnixStream,
};

struct FlowClient {
    socket: String,
}

/// The CLI takes exactly one inline Query datom. A Start names its flow's
/// context by module identity inside that datom; the Nexus has it composed.
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
        let parsed = value
            .protosize_with(&mut budget.reader)
            .map_err(|error| format!("invalid Flow query: {error:?}"))?;
        let datom = parsed
            .datom_form(vec![])
            .map_err(|error| format!("invalid Flow query: {error:?}"))?;
        // The CLI accepts omitted delivery as Wake. The Nexus wire
        // remains the explicit, typed three-position SendRequest.
        // Compose the original positions so errors retain input paths.
        if let Form::Variant(head, body) = &datom.form
            && head.0 == "Send"
            && matches!(&body.form, Form::Struct(fields) if fields.len() == 2)
        {
            let (recipient, message): (Recipient, Message) = body
                .compose(&mut budget)
                .map_err(|error| format!("invalid Flow query: {error:?}"))?;
            return Ok(Query::Send(SendRequest {
                recipient,
                delivery: Delivery::Wake,
                message,
            }));
        }
        datom
            .compose(&mut budget)
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
        let streams = matches!(
            query,
            Query::Observe(_) | Query::ObserveSends(_) | Query::ObserveSender(_)
        );
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

#[cfg(test)]
mod tests {
    use super::{AnswersVersion, FlowClient, ParsesFlowCommand, TextualizesFlowReply};
    use signal_flow::{Delivery, Message, Query, Recipient, Request, Response};

    #[test]
    fn send_names_queue_or_wake_and_deliver_all_is_typed() {
        let client = FlowClient {
            socket: "unused".into(),
        };
        let Query::Send(send) = client
            .parse_command(
                ["Send.{ Address.{ Mind core Secondary } Wake Soft.Order.«Review.» }".into()]
                    .into_iter(),
            )
            .expect("current Send parses")
        else {
            panic!("fixture must remain a Send")
        };
        assert!(matches!(send.recipient, Recipient::Address(_)));
        assert_eq!(send.delivery, Delivery::Wake);
        assert_eq!(
            send.message,
            Message::Soft(Request::Order("Review.".into()))
        );
        assert_eq!(
            client
                .parse_command(
                    ["Send.{ Address.{ Mind core Secondary } Soft.Order.«Review.» }".into()]
                        .into_iter()
                )
                .expect("omitted delivery defaults to Wake"),
            Query::Send(send)
        );
        let Query::Send(queued) = client
            .parse_command(["Send.{ Up Queue Soft.Result.«Ready.» }".into()].into_iter())
            .expect("explicit Queue parses")
        else {
            panic!("fixture must remain a Send")
        };
        assert_eq!(queued.recipient, Recipient::Up);
        assert_eq!(queued.delivery, Delivery::Queue);
        assert!(matches!(
            client
                .parse_command(["DeliverAll.{ Mind core Secondary }".into()].into_iter())
                .expect("DeliverAll parses"),
            Query::DeliverAll(_)
        ));
    }

    #[test]
    fn default_send_keeps_message_content_and_validates_positions() {
        let client = FlowClient {
            socket: "unused".into(),
        };
        for message in [
            "Soft.Result.«Wake Send.{ is content }»",
            "MiddleAbrupt.Notice.«Check the source.»",
            "HardAbrupt.Question.«Which revision?»",
            "Soft.Psyche.{ «context» «verbatim» }",
            "Soft.Psyches.[ { «context» «verbatim» } ]",
        ] {
            let defaulted = client
                .parse_command([format!("Send.{{ Up {message} }}")].into_iter())
                .expect("defaulted Send parses");
            let explicit = client
                .parse_command([format!("Send.{{ Up Wake {message} }}")].into_iter())
                .expect("explicit Wake parses");
            assert_eq!(defaulted, explicit);
        }
        for invalid in [
            "Send.{}",
            "Send.{ Up }",
            "Send.{ Up Wake Soft.Order.«Review.» extra }",
            "Send.{ Up Unknown Soft.Order.«Review.» }",
            "Send.{ Up Wake }",
            "Send.{ Unknown Soft.Order.«Review.» }",
            "Send.[ Up Soft.Order.«Review.» ]",
        ] {
            assert!(
                client.parse_command([invalid.into()].into_iter()).is_err(),
                "must reject {invalid}"
            );
        }
    }

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
                "Start.{ request-7 [ spirit-core intent-flow ] [ operation-main-flow compensation-behavior ] Custom.[ 1b8ac0 ] { Field flow Primary } gpt-6-astra medium Some.836818 messaging-build Some.[ SkillTool WorkflowTool ] Default «Carry this bounded launch request.» }".into(),
            ]
            .into_iter())
            .expect("typed Start parses")
        else {
            panic!("Start fixture must remain a Start query")
        };
        assert_eq!(request.launch_request_id, "request-7");
        assert_eq!(request.system_modules, ["spirit-core", "intent-flow"]);
        assert_eq!(
            request.user_modules,
            ["operation-main-flow", "compensation-behavior"]
        );
        assert_eq!(
            request.leave_outs_option,
            Some(vec![
                signal_flow::LeaveOut::SkillTool,
                signal_flow::LeaveOut::WorkflowTool
            ])
        );
        assert_eq!(
            request.instruction_prompt,
            "Carry this bounded launch request."
        );
    }

    #[test]
    fn a_start_carrying_a_source_path_or_a_flag_is_refused() {
        let client = FlowClient {
            socket: "unused".into(),
        };
        assert!(
            client
                .parse_command([
                    "Start.{ request-7 [ { Vision/flowNexus.md 54c08e7190360a308e560935c120c69b81c4aacb4975751a4841912b599f4f5a } ] [ operation-main-flow ] Predecessors.0 { Field flow Primary } gpt-6-astra medium None messaging-build None Default «Launch.» }".into(),
                ]
                .into_iter())
                .is_err()
        );
        assert!(
            client
                .parse_command(
                    [
                        "--system-modules".into(),
                        "[ spirit-core ]".into(),
                        "ResolveRecipient.fac697".into(),
                    ]
                    .into_iter()
                )
                .is_err()
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
            .parse_command(["Replace.{ request-8 [ spirit-core ] [] Predecessors.1 { Field flow Primary } opus-5-5 high Some.fac697 messaging-build None Default «Carry on from fac697.» }".into()].into_iter())
            .expect("typed Replace parses")
        else {
            panic!("Replace fixture must remain a Replace query")
        };
        assert_eq!(request.flow_id_option, Some("fac697".into()));
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
            origin: signal_flow::Origin::Unbound,
            flow_lifecycle: signal_flow::FlowLifecycle::Active,
        });
        assert_eq!(
            client.textualize_reply(&reply),
            "RecipientResolved.{ da1e3f da1e3f9d-full Claude Unavailable Available.{ messaging-build recipient w1:p2 term-current } Unbound Active }"
        );
    }
}
