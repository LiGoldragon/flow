use datom_codec::{Actualizing, Budget, Potential};
use flow::Invocation;
use flow::client::{CallsFlowNexus, FlowClient, TextualizesFlowReply};
use flow_ethos::place::{
    CliEnvironment, NamesPlaceSockets, PlaceSockets, ReadsCliEnvironment, ResolvesCliPlace,
};
use protos::ReaderBudget;
use std::env;

/// The CLI is `flow-nexus '<Query>'`: named after the Nexus, taking the datom
/// and nothing else. The place is its configuration's,
/// `$XDG_CONFIG_HOME/nexus/flow-nexus.datom`, resolved under
/// `$XDG_RUNTIME_DIR/nexus/` to the place's ordinary socket; no place is
/// typed.
trait ParsesFlowCommand {
    fn parse_command(
        &self,
        arguments: impl Iterator<Item = String>,
        place: &dyn ResolvesCliPlace,
    ) -> Result<Invocation, String>;
}

impl ParsesFlowCommand for FlowClient {
    fn parse_command(
        &self,
        mut arguments: impl Iterator<Item = String>,
        place: &dyn ResolvesCliPlace,
    ) -> Result<Invocation, String> {
        let usage = || {
            "usage: flow-nexus '<one inline Query datom>'; the place is read from $XDG_CONFIG_HOME/nexus/flow-nexus.datom".to_owned()
        };
        let (Some(value), None) = (arguments.next(), arguments.next()) else {
            return Err(usage());
        };
        let runtime = place.place_runtime()?;
        let mut budget = Budget {
            remaining: 65_536,
            reader: ReaderBudget::try_from(65_536).expect("a constant reader budget"),
            depth: 0,
            maximum_depth: 1_024,
        };
        let query = Potential::<signal_flow::Query>::from(value)
            .actualize(&mut budget)
            .map_err(|error| format!("invalid Flow Query: {error:?}"))?;
        Ok(Invocation {
            ordinary_socket_path: flow_ethos::OrdinarySocketPath(
                PlaceSockets::ordinary_in(&runtime)
                    .to_string_lossy()
                    .into_owned(),
            ),
            query,
        })
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
            [only] if only == "--version" => {
                Some(format!("flow-nexus {}", env!("CARGO_PKG_VERSION")))
            }
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
    let client = FlowClient;
    match client
        .parse_command(arguments.into_iter(), &CliEnvironment::of_process())
        .and_then(|invocation| {
            client.call(&invocation, &mut |reply| {
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
    use super::{AnswersVersion, ParsesFlowCommand};
    use flow::Invocation;
    use flow::client::{CallsFlowNexus, FlowClient, TextualizesFlowReply};
    use std::io::{Read, Write};
    use std::os::unix::net::UnixListener;

    trait ParsesTestQuery {
        fn parse_query(
            &self,
            arguments: impl Iterator<Item = String>,
        ) -> Result<signal_flow::Query, String>;
    }

    impl ParsesTestQuery for FlowClient {
        fn parse_query(
            &self,
            mut arguments: impl Iterator<Item = String>,
        ) -> Result<signal_flow::Query, String> {
            let Some(query) = arguments.next() else {
                return Err("missing fixture".into());
            };
            if arguments.next().is_some() {
                return Err("extra fixture".into());
            }
            self.parse_command([query].into_iter(), &FixedPlace)
                .map(|Invocation { query, .. }| query)
        }
    }

    /// The place a test's configuration names, read from no file.
    struct FixedPlace;

    impl flow_ethos::place::ResolvesCliPlace for FixedPlace {
        fn place_runtime(&self) -> Result<std::path::PathBuf, String> {
            Ok("/run/user/test/nexus/main".into())
        }
    }
    use signal_flow::{
        Address, Aspect, Model, ObserveSelection, Query, Response, SendableMessage, SendableRequest,
    };

    #[test]
    fn send_priority_and_deliver_all_are_typed() {
        let client = FlowClient;
        let Query::Send(send) = client
            .parse_query(["Send.{ { Mind core Sol } Soft.Order.«Review.» }".into()].into_iter())
            .expect("current Send parses")
        else {
            panic!("fixture must remain a Send")
        };
        assert_eq!(
            send.address,
            Address {
                aspect: Aspect::Mind,
                topic: flow_ethos::Topic("core".into()),
                model: Model::Sol,
            }
        );
        assert_eq!(
            send.sendable_message,
            SendableMessage::Soft(SendableRequest::Order("Review.".into()))
        );
        // A recipient is an Address: no route relative to the sender.
        for relative in [
            "Send.{ Up Soft.Result.{ Order.«Review.» «Ready.» } }",
            "Send.{ Address.{ Mind core Sol } Soft.Order.«Review.» }",
        ] {
            assert!(
                client.parse_query([relative.into()].into_iter()).is_err(),
                "must reject {relative}"
            );
        }
        assert!(matches!(
            client
                .parse_query(["DeliverAll.{ Mind core Sol }".into()].into_iter())
                .expect("DeliverAll parses"),
            Query::DeliverAll(_)
        ));
    }

    #[test]
    fn send_keeps_message_content_and_validates_positions() {
        let client = FlowClient;
        for message in [
            "Soft.Result.{ k7ph «Wake Send.{ is content }» }",
            "MiddleAbrupt.Notice.«Check the source.»",
            "HardAbrupt.Question.«Which revision?»",
            "Soft.Living.{ «context» «verbatim» }",
        ] {
            let explicit = client
                .parse_query([format!("Send.{{ {{ Psyche core Opus }} {message} }}")].into_iter())
                .expect("Send parses");
            assert!(matches!(explicit, Query::Send(_)));
        }
        for invalid in [
            "Send.{}",
            "Send.{ { Psyche core Opus } }",
            "Send.{ { Psyche core Opus } Soft.Order.«Review.» extra }",
            "Send.{ { Psyche core Opus } Unknown Soft.Order.«Review.» }",
            "Send.{ Unknown Soft.Order.«Review.» }",
            "Send.[ { Psyche core Opus } Soft.Order.«Review.» ]",
            // The living's words are one saying; Psyche and Psyches are gone.
            "Send.{ { Psyche core Opus } Soft.Psyche.{ «context» «verbatim» } }",
            "Send.{ { Psyche core Opus } Soft.Psyches.[ { «context» «verbatim» } ] }",
        ] {
            assert!(
                client.parse_query([invalid.into()].into_iter()).is_err(),
                "must reject {invalid}"
            );
        }
    }

    #[test]
    fn cli_parses_result_by_request_id() {
        let client = FlowClient;
        let Query::Send(send) = client
            .parse_query(
                ["Send.{ { Psyche core Opus } Soft.Result.{ k7ph «Reviewed; green.» } }".into()]
                    .into_iter(),
            )
            .expect("a Result names the request it answers")
        else {
            panic!("fixture must remain a Send")
        };
        assert_eq!(
            send.sendable_message,
            SendableMessage::Soft(SendableRequest::Result(
                signal_flow::SendableRequest_Result_Data {
                    request_id: flow_ethos::RequestId("k7ph".into()),
                    string: "Reviewed; green.".into(),
                }
            ))
        );
    }

    #[test]
    fn cli_prints_sent_with_request_id() {
        let client = FlowClient;
        assert_eq!(
            client.textualize_reply(&Response::Sent(signal_flow::VisibleSendReceipt(
                flow_ethos::RequestId("k7ph".into())
            ))),
            "Sent.k7ph"
        );
    }

    #[test]
    fn version_is_the_one_non_datom_invocation() {
        assert_eq!(
            (&["--version".to_owned()] as &[String]).version_answer(),
            Some(format!("flow-nexus {}", env!("CARGO_PKG_VERSION")))
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
        let client = FlowClient;
        assert_eq!(
            client
                .parse_query(["ResolveRecipient.fac697".into()].into_iter())
                .unwrap(),
            Query::ResolveRecipient(signal_flow::RecipientResolutionRequest(
                signal_flow::FlowId("fac697".into())
            ))
        );
    }

    #[test]
    fn start_accepts_one_inline_typed_launch_profile() {
        let client = FlowClient;
        let Query::Start(request) = client
            .parse_query([
                "Start.{ request-7 [ spirit:core:0 intent:flow:0 ] [ operation:main-flow:0 compensation:behavior:0 ] Custom.[ 1b8ac0 ] { Field flow Astra } gpt-6-astra medium Some.836818 messaging-build Some.[ SkillTool WorkflowTool ] Regular Default «Carry this bounded launch request.» }".into(),
            ]
            .into_iter())
            .expect("typed Start parses")
        else {
            panic!("Start fixture must remain a Start query")
        };
        assert_eq!(
            request.launch_request_id,
            signal_flow::LaunchRequestId("request-7".into())
        );
        assert_eq!(
            request
                .system_modules
                .0
                .iter()
                .map(|identity| identity.0.as_str())
                .collect::<Vec<_>>(),
            ["spirit:core:0", "intent:flow:0"]
        );
        assert_eq!(
            request
                .user_modules
                .0
                .iter()
                .map(|identity| identity.0.as_str())
                .collect::<Vec<_>>(),
            ["operation:main-flow:0", "compensation:behavior:0"]
        );
        assert_eq!(
            request.leave_outs_option,
            Some(vec![
                signal_flow::LeaveOut::SkillTool,
                signal_flow::LeaveOut::WorkflowTool
            ])
            .map(signal_flow::LeaveOuts)
        );
        assert_eq!(
            request.instruction_prompt,
            signal_flow::InstructionPrompt("Carry this bounded launch request.".into())
        );
    }

    #[test]
    fn a_start_carrying_a_source_path_or_a_flag_is_refused() {
        let client = FlowClient;
        assert!(
            client
                .parse_query([
                    "Start.{ request-7 [ { Vision/flowNexus.md 54c08e7190360a308e560935c120c69b81c4aacb4975751a4841912b599f4f5a } ] [ operation:main-flow:0 ] Predecessors.0 { Field flow Astra } gpt-6-astra medium None messaging-build None Regular Default «Launch.» }".into(),
                ]
                .into_iter())
                .is_err()
        );
        assert!(
            client
                .parse_query(
                    [
                        "--system-modules".into(),
                        "[ spirit:core:0 ]".into(),
                        "ResolveRecipient.fac697".into(),
                    ]
                    .into_iter()
                )
                .is_err()
        );
    }

    #[test]
    fn replace_status_and_observe_are_inline_typed_queries() {
        let client = FlowClient;
        assert_eq!(
            client
                .parse_query(["LaunchStatus.request-8".into()].into_iter())
                .unwrap(),
            Query::LaunchStatus(signal_flow::LaunchRequestId("request-8".into()))
        );
        assert_eq!(
            client
                .parse_query(["Observe.Launch.request-8".into()].into_iter())
                .unwrap(),
            Query::Observe(signal_flow::ObserveSelection::Launch(
                signal_flow::LaunchRequestId("request-8".into())
            ))
        );
        let Query::Replace(request) = client
            .parse_query(["Replace.{ request-8 [ spirit:core:0 ] [] Predecessors.1 { Field flow Astra } opus-5-5 high Some.fac697 messaging-build None Regular Default «Carry on from fac697.» }".into()].into_iter())
            .expect("typed Replace parses")
        else {
            panic!("Replace fixture must remain a Replace query")
        };
        assert_eq!(
            request.flow_id_option,
            Some(signal_flow::FlowId("fac697".into()))
        );
    }

    #[test]
    fn command_refuses_more_than_one_inline_value() {
        let client = FlowClient;
        assert!(
            client
                .parse_query(["ResolveRecipient.fac697".into(), "extra".into()].into_iter())
                .is_err()
        );
        assert!(
            client
                .parse_query(
                    ["Start.{ codex-medium { fac697 session-1 turn-2 } }".into()].into_iter()
                )
                .is_err()
        );
        assert!(client.parse_query(["Start.{".into()].into_iter()).is_err());
    }

    #[test]
    fn native_resolution_serialization_matches_the_signal_contract() {
        let client = FlowClient;
        let reply = Response::RecipientResolved(signal_flow::FlowNode {
            flow_id: signal_flow::FlowId("da1e3f".into()),
            session_id: signal_flow::SessionId("da1e3f9d-full".into()),
            harness_kind: signal_flow::HarnessKind::Claude,
            endpoint_selection: signal_flow::EndpointSelection::Unavailable,
            herdr_route_selection: signal_flow::HerdrRouteSelection::Available(
                signal_flow::HerdrRoute {
                    herdr_session_name: signal_flow::HerdrSessionName("messaging-build".into()),
                    herdr_agent_name: signal_flow::HerdrAgentName("recipient".into()),
                    herdr_pane_id: signal_flow::HerdrPaneId("w1:p2".into()),
                    herdr_terminal_id: signal_flow::HerdrTerminalId("term-current".into()),
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

    /// A reminder names whom it reminds and after how many minutes; a time
    /// of day is no Minutes. A Task carries its Minutes as a Send. Minutes
    /// that is no integer, or an integer beyond i64, is the CLI's compose
    /// error and never reaches Flow.
    #[test]
    fn remind_names_its_address_and_a_task_carries_its_minutes() {
        let client = FlowClient;
        assert!(
            client
                .parse_query(
                    ["Remind.{ 2026-10-29T23:30:00-06:00 «Check the capsule run.» }".into()]
                        .into_iter()
                )
                .is_err(),
            "a Remind at a time of day reads"
        );
        assert!(matches!(
            client
                .parse_query(
                    ["Remind.{ { Mind core Sol } 30 «Check the capsule run.» }".into()].into_iter()
                )
                .expect("a Remind reads"),
            Query::Remind(_)
        ));
        let Query::Send(send) = client
            .parse_query(
                ["Send.{ { Mind core Sol } Soft.Task.{ 30 «Check the capsule run.» } }".into()]
                    .into_iter(),
            )
            .expect("a Task reads")
        else {
            panic!("a Task is sent")
        };
        let SendableMessage::Soft(SendableRequest::Task(task)) = &send.sendable_message else {
            panic!("a Soft Task: {:?}", send.sendable_message)
        };
        assert_eq!(task.minutes, flow_ethos::Minutes(30));
        assert_eq!(task.string, "Check the capsule run.");
        for text in [
            "Send.{ { Mind core Sol } Soft.Task.{ soon «Check the capsule run.» } }",
            "Send.{ { Mind core Sol } Soft.Task.{ 9223372036854775808 «Check the capsule run.» } }",
        ] {
            assert!(
                client.parse_query([text.into()].into_iter()).is_err(),
                "{text} reads"
            );
        }
    }

    /// The fixture Nexus answers the client's greeting as Flow does.
    fn greeted(peer: &mut std::os::unix::net::UnixStream) {
        use signal_flow::Contracted;
        let mut length = [0_u8; 4];
        peer.read_exact(&mut length).unwrap();
        let mut frame = vec![0_u8; u32::from_be_bytes(length) as usize];
        peer.read_exact(&mut frame).unwrap();
        let greeting =
            rkyv::from_bytes::<signal_flow::Handshake, rkyv::rancor::Error>(&frame).unwrap();
        let receipt = rkyv::to_bytes::<rkyv::rancor::Error>(&Query::receipt(&greeting)).unwrap();
        peer.write_all(&(receipt.len() as u32).to_be_bytes())
            .unwrap();
        peer.write_all(&receipt).unwrap();
    }

    #[test]
    fn retirement_eof_and_truncated_frames_are_incomplete() {
        for reply in [Vec::new(), vec![0, 0], vec![0, 0, 0, 8, 1, 2]] {
            let directory = tempfile::tempdir().unwrap();
            let socket = directory.path().join("ordinary.sock");
            let listener = UnixListener::bind(&socket).unwrap();
            let server = std::thread::spawn(move || {
                let (mut peer, _) = listener.accept().unwrap();
                greeted(&mut peer);
                let mut length = [0_u8; 4];
                peer.read_exact(&mut length).unwrap();
                let mut query = vec![0_u8; u32::from_be_bytes(length) as usize];
                peer.read_exact(&mut query).unwrap();
                peer.write_all(&reply).unwrap();
            });
            let invocation = Invocation {
                ordinary_socket_path: flow_ethos::OrdinarySocketPath(
                    socket.to_string_lossy().into_owned(),
                ),
                query: Query::Observe(ObserveSelection::Retirement(Address {
                    aspect: Aspect::Mind,
                    topic: flow_ethos::Topic("retirementCli".into()),
                    model: Model::Sol,
                })),
            };
            assert!(FlowClient.call(&invocation, &mut |_| {}).is_err());
            server.join().unwrap();
        }
    }

    #[test]
    fn launch_observation_keeps_its_existing_clean_eof_semantics() {
        let directory = tempfile::tempdir().unwrap();
        let socket = directory.path().join("ordinary.sock");
        let listener = UnixListener::bind(&socket).unwrap();
        let server = std::thread::spawn(move || {
            let (mut peer, _) = listener.accept().unwrap();
            greeted(&mut peer);
            let mut length = [0_u8; 4];
            peer.read_exact(&mut length).unwrap();
            let mut query = vec![0_u8; u32::from_be_bytes(length) as usize];
            peer.read_exact(&mut query).unwrap();
        });
        let invocation = Invocation {
            ordinary_socket_path: flow_ethos::OrdinarySocketPath(
                socket.to_string_lossy().into_owned(),
            ),
            query: Query::Observe(ObserveSelection::Launch(signal_flow::LaunchRequestId(
                "request".into(),
            ))),
        };
        assert!(FlowClient.call(&invocation, &mut |_| {}).is_ok());
        server.join().unwrap();
    }

    #[test]
    fn a_terminal_retirement_failure_is_printed_and_fails_the_call() {
        let directory = tempfile::tempdir().unwrap();
        let socket = directory.path().join("ordinary.sock");
        let listener = UnixListener::bind(&socket).unwrap();
        let response = Response::RetirementObservationFailed(
            signal_flow::RetirementObservationFailure::Overflow,
        );
        let server = std::thread::spawn(move || {
            let (mut peer, _) = listener.accept().unwrap();
            greeted(&mut peer);
            let mut length = [0_u8; 4];
            peer.read_exact(&mut length).unwrap();
            let mut query = vec![0_u8; u32::from_be_bytes(length) as usize];
            peer.read_exact(&mut query).unwrap();
            let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&response).unwrap();
            peer.write_all(&(bytes.len() as u32).to_be_bytes()).unwrap();
            peer.write_all(&bytes).unwrap();
        });
        let invocation = Invocation {
            ordinary_socket_path: flow_ethos::OrdinarySocketPath(
                socket.to_string_lossy().into_owned(),
            ),
            query: Query::Observe(ObserveSelection::Retirement(Address {
                aspect: Aspect::Mind,
                topic: flow_ethos::Topic("retirementCli".into()),
                model: Model::Sol,
            })),
        };
        let mut seen = Vec::new();
        assert!(
            FlowClient
                .call(&invocation, &mut |response| seen.push(response))
                .is_err()
        );
        assert_eq!(
            seen,
            vec![Response::RetirementObservationFailed(
                signal_flow::RetirementObservationFailure::Overflow
            )]
        );
        server.join().unwrap();
    }
}
