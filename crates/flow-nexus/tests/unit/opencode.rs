//! A flow's OpenCode configuration directory and the two calls Flow makes to
//! its server, against a fake server on a loopback port that answers as
//! OpenCode 2.4.1 answered in a test capsule.

use crate::generated::emission::{OpenCodeAgent, OpenCodeAgentName};
use crate::opencode::{
    AsksOpenCode, OpenCodeFlowDirectory, OpenCodeRefusal, OpenCodeReportPlugin, OpenCodeServer,
    WritesOpenCodeFlow,
};
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpListener;
use std::sync::mpsc;
use std::time::Duration;

/// A request the fake server received: its request line and its body.
struct Received {
    line: String,
    authorization: Option<String>,
    body: String,
}

/// Answers each connection with the next scripted answer (status, body,
/// chunked or not), sending what it received.
fn fake_server(
    answers: Vec<(u16, &'static str, bool)>,
) -> (std::net::SocketAddr, mpsc::Receiver<Received>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let (sender, receiver) = mpsc::channel();
    std::thread::spawn(move || {
        for (status, body, chunked) in answers {
            let (stream, _) = listener.accept().unwrap();
            let mut reader = BufReader::new(stream);
            let mut line = String::new();
            reader.read_line(&mut line).unwrap();
            let mut length = 0;
            let mut authorization = None;
            loop {
                let mut header = String::new();
                reader.read_line(&mut header).unwrap();
                let header = header.trim_end().to_owned();
                if header.is_empty() {
                    break;
                }
                let (name, value) = header.split_once(": ").unwrap();
                match name.to_ascii_lowercase().as_str() {
                    "content-length" => length = value.parse().unwrap(),
                    "authorization" => authorization = Some(value.to_owned()),
                    _ => {}
                }
            }
            let mut request_body = vec![0; length];
            reader.read_exact(&mut request_body).unwrap();
            sender
                .send(Received {
                    line: line.trim_end().to_owned(),
                    authorization,
                    body: String::from_utf8(request_body).unwrap(),
                })
                .unwrap();
            let mut stream = reader.into_inner();
            let answer = if chunked {
                let (first, second) = body.split_at(body.len() / 2);
                format!(
                    "HTTP/1.1 {status} X\r\ncontent-type: application/json\r\ntransfer-encoding: chunked\r\n\r\n{:x}\r\n{first}\r\n{:x}\r\n{second}\r\n0\r\n\r\n",
                    first.len(),
                    second.len()
                )
            } else {
                format!(
                    "HTTP/1.1 {status} X\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{body}",
                    body.len()
                )
            };
            stream.write_all(answer.as_bytes()).unwrap();
        }
    });
    (address, receiver)
}

fn server(address: std::net::SocketAddr) -> OpenCodeServer {
    OpenCodeServer {
        address,
        password: "capsule-password".into(),
        exchange_timeout: Duration::from_secs(5),
        retry_interval: Duration::from_millis(10),
    }
}

const NOT_FOUND: &str = r#"{"_tag":"AgentNotFoundError","agentID":"flow-terra","message":"Agent not found: flow-terra"}"#;
const FOUND: &str = r#"{"location":{"directory":"/capsule/work"},"data":{"id":"flow-terra","name":"flow-terra","mode":"primary"}}"#;

/// The directory Flow writes is OpenCode's whole configuration: the agent as
/// default, the system prompt in its file, the report plugin declared with
/// its hook and the hook's configuration, and the variable a server is
/// started with to read it.
#[test]
fn a_flow_directory_holds_the_agent_its_prompt_and_the_report_plugin() {
    use crate::emission::WritesOpenCodeConfiguration;
    let root = tempfile::tempdir().unwrap();
    let plugin = OpenCodeReportPlugin {
        package: root.path().join("installed/flow-report"),
        hook: root.path().join("installed/flow-hook"),
        configuration: "HookConfiguration.capsule".into(),
    };
    let directory = OpenCodeFlowDirectory::under(&root.path().join("opencode"), "f0a1b2");
    let agent = OpenCodeAgent {
        open_code_agent_name: OpenCodeAgentName("flow-terra".into()),
        model_name: flow_ethos::ModelName("capsule/fake-model".into()),
        effort: signal_flow::Effort("high".into()),
        system_prompt_file: directory.system_prompt_file(),
    };
    directory.write(&agent, "Be brief.", &plugin).unwrap();
    // A second write, a restart's, replaces the first.
    directory.write(&agent, "Be briefer.", &plugin).unwrap();
    let written: serde_json::Value =
        serde_json::from_slice(&std::fs::read(directory.path.join("opencode.json")).unwrap())
            .unwrap();
    let mut expected = agent.opencode_configuration();
    expected["plugins"] = serde_json::json!([{
        "package": root.path().join("installed/flow-report"),
        "options": {
            "hook": root.path().join("installed/flow-hook"),
            "configuration": "HookConfiguration.capsule",
        },
    }]);
    assert_eq!(written, expected);
    assert_eq!(
        std::fs::read_to_string(&agent.system_prompt_file.0).unwrap(),
        "Be briefer."
    );
    assert_eq!(
        directory.server_environment(),
        ("OPENCODE_CONFIG_DIR", root.path().join("opencode/f0a1b2"))
    );
}

/// An agent still loading is looked up again until it stands; the server is
/// asked with its password.
#[test]
fn an_agent_still_loading_is_awaited() {
    let (address, received) = fake_server(vec![
        (404, NOT_FOUND, false),
        (404, NOT_FOUND, false),
        (200, FOUND, true),
    ]);
    server(address)
        .await_agent("flow-terra", Duration::from_secs(5))
        .unwrap();
    let first = received.recv().unwrap();
    assert_eq!(first.line, "GET /api/agent/flow-terra HTTP/1.1");
    use base64::Engine;
    assert_eq!(
        first.authorization.unwrap(),
        format!(
            "Basic {}",
            base64::engine::general_purpose::STANDARD.encode("opencode:capsule-password")
        )
    );
    assert_eq!(received.iter().count(), 2);
}

/// An agent that never stands ends the wait with OpenCode's words whole.
#[test]
fn an_absent_agent_is_refused_in_opencodes_words() {
    let (address, _received) = fake_server(vec![(404, NOT_FOUND, false); 64]);
    assert_eq!(
        server(address).await_agent("flow-terra", Duration::from_millis(50)),
        Err(OpenCodeRefusal::Answered {
            status: 404,
            words: NOT_FOUND.into()
        })
    );
}

/// The session is opened with the id Flow chose, for its agent; OpenCode
/// answers that session, new or standing, so a resume is the same call.
#[test]
fn a_session_is_opened_with_the_chosen_id() {
    let session = "ses_flow0000000001FlowTestPerm";
    let answer: &'static str = r#"{"data":{"id":"ses_flow0000000001FlowTestPerm","agent":"flow-terra","location":{"directory":"/capsule/work"}}}"#;
    let (address, received) = fake_server(vec![(200, answer, false), (200, answer, true)]);
    let server = server(address);
    server.open_session(session, "flow-terra").unwrap();
    server.open_session(session, "flow-terra").unwrap();
    let opened = received.recv().unwrap();
    assert_eq!(opened.line, "POST /api/session HTTP/1.1");
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&opened.body).unwrap(),
        serde_json::json!({ "id": session, "agent": "flow-terra" })
    );
}

/// A session answered under another id, or a refusal, is not taken as
/// opened; an unreachable server is said to be one.
#[test]
fn a_session_answered_otherwise_is_refused() {
    let other = r#"{"data":{"id":"ses_other"}}"#;
    let refused =
        r#"{"_tag":"InvalidRequestError","message":"Expected a string matching the pattern ^ses"}"#;
    let (address, _received) = fake_server(vec![(200, other, false), (400, refused, false)]);
    let server = server(address);
    assert_eq!(
        server.open_session("ses_flow0000000001FlowTestPerm", "flow-terra"),
        Err(OpenCodeRefusal::Answered {
            status: 200,
            words: other.into()
        })
    );
    assert_eq!(
        server.open_session("flow-not-a-session", "flow-terra"),
        Err(OpenCodeRefusal::Answered {
            status: 400,
            words: refused.into()
        })
    );
    let closed = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = closed.local_addr().unwrap();
    drop(closed);
    assert!(matches!(
        self::server(address).open_session("ses_flow0000000001FlowTestPerm", "flow-terra"),
        Err(OpenCodeRefusal::Unreachable(_))
    ));
}
