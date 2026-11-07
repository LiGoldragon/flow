use super::NexusFixture;
use crate::Dispatches;
use crate::harness_command::{
    HarnessCommandResources, ParsesHarnessFlags, RehearsesHarnessResources, RendersShellCommand,
};
use crate::herdr::HerdrCli;
use crate::herdr::launch::ConstructsHarnessCommand;
use crate::store::{ManagesMetaflowState, ReadsFlowRows, ReadsLaunchAttempt};
use signal_flow::{
    Address, Aspect, ComposedLaunch, FirstPromptPayload, HarnessFlagsRefusal, HarnessKind,
    LaunchProfile, Model, Remembering, SystemPrompt, TargetReceiptRequest, ToolSet,
};

trait RehearsalFixture {
    fn launch(harness: HarnessKind) -> ComposedLaunch;
    fn actual_resources(harness: &HarnessKind) -> HarnessCommandResources;
    fn substitute(words: Vec<String>, harness: &HarnessKind) -> Vec<String>;
}

struct Fixture;

impl RehearsalFixture for Fixture {
    fn launch(harness: HarnessKind) -> ComposedLaunch {
        let model = match harness {
            HarnessKind::Claude => "claude-opus-5-5",
            HarnessKind::Codex => "gpt-6.1-sol",
        };
        let profile = LaunchProfile {
            launch_request_id: "request-fixture".into(),
            system_modules: vec!["spirit:core".into()],
            user_modules: vec![],
            remembering: Remembering::Predecessors(0),
            address: Address {
                aspect: Aspect::Mind,
                topic: "core".into(),
                model: Model::Sol,
            },
            model_name: model.into(),
            effort: "medium".into(),
            flow_id_option: None,
            herdr_session_name: "actual-session".into(),
            leave_outs_option: None,
            flow_kind: signal_flow::FlowKind::Regular,
            tool_set: ToolSet::Default,
            instruction_prompt: "Begin.".into(),
        };
        ComposedLaunch {
            launch_profile: profile,
            harness_kind: harness,
            system_prompt: SystemPrompt {
                system_prompt_text: "system".into(),
                system_prompt_sha256: "system-sha".into(),
            },
            first_prompt_payload: FirstPromptPayload {
                first_prompt_body: "Begin.".into(),
                prompt_sha256: "prompt-sha".into(),
                prompt_part_vector: vec![],
            },
            target_receipt_request: TargetReceiptRequest {
                launch_request_id: "request-fixture".into(),
                prompt_sha256: "prompt-sha".into(),
            },
            leave_outs: vec![],
        }
    }

    fn actual_resources(harness: &HarnessKind) -> HarnessCommandResources {
        HarnessCommandResources {
            herdr_session: "actual-session".into(),
            agent: "actual-agent".into(),
            pane: "actual-pane".into(),
            program: match harness {
                HarnessKind::Claude => "claude".into(),
                HarnessKind::Codex => "/nix/store/codex/bin/codex".into(),
            },
            codex_socket: (harness == &HarnessKind::Codex).then(|| "/run/codex.sock".into()),
            claude_settings: (harness == &HarnessKind::Claude)
                .then(|| "{\"permissions\":{}}".into()),
            claude_title: (harness == &HarnessKind::Claude)
                .then(|| "{ Mind core Sol abc123 }".into()),
            prompt_fd: (harness == &HarnessKind::Claude).then(|| "/proc/self/fd/17".into()),
            claude_session: (harness == &HarnessKind::Claude).then(|| "claude-session".into()),
            claude_resume: None,
            codex_title: (harness == &HarnessKind::Codex)
                .then(|| "tui.terminal_title=['thread-name']".into()),
            thread: (harness == &HarnessKind::Codex).then(|| "codex-thread".into()),
        }
    }

    fn substitute(mut words: Vec<String>, harness: &HarnessKind) -> Vec<String> {
        let actual = Self::actual_resources(harness);
        for word in &mut words {
            *word = match word.as_str() {
                "<session>" => actual.herdr_session.clone(),
                "<agent>" => actual.agent.clone(),
                "<pane>" => actual.pane.clone(),
                "<codex-client>" => actual.program.clone(),
                "unix://<codex-socket>" => {
                    format!("unix://{}", actual.codex_socket.as_ref().unwrap())
                }
                "<settings>" => actual.claude_settings.clone().unwrap(),
                "<flow-id>" => actual.claude_title.clone().unwrap(),
                "<prompt-fd>" => actual.prompt_fd.clone().unwrap(),
                "<claude-session>" => actual.claude_session.clone().unwrap(),
                "<title>" => actual.codex_title.clone().unwrap(),
                "<thread>" => actual.thread.clone().unwrap(),
                other => other.to_owned(),
            };
        }
        words
    }
}

#[test]
fn configured_flags_follow_the_exact_non_expanding_grammar() {
    for (source, expected) in [
        ("", vec![]),
        (" \t\n ", vec![]),
        ("a b", vec!["a", "b"]),
        ("'' \"\"", vec!["", ""]),
        ("' a $ ` \\ ' tail", vec![" a $ ` \\ ", "tail"]),
        ("\"a b\"", vec!["a b"]),
        ("\"\\$ \\` \\\" \\\\\"", vec!["$ ` \" \\"]),
        ("\"\\q\"", vec!["\\q"]),
        ("one\\ two", vec!["one two"]),
        ("one\\\ntwo", vec!["onetwo"]),
        ("\"one\\\ntwo\"", vec!["onetwo"]),
        ("# ; | > Artifact*", vec!["#", ";", "|", ">", "Artifact*"]),
    ] {
        assert_eq!(
            source.harness_arguments().unwrap(),
            expected.into_iter().map(str::to_owned).collect::<Vec<_>>(),
            "{source:?}"
        );
    }
    for (source, refusal) in [
        ("'open", HarnessFlagsRefusal::UnclosedSingleQuote),
        ("\"open", HarnessFlagsRefusal::UnclosedDoubleQuote),
        ("tail\\", HarnessFlagsRefusal::TrailingEscape),
        ("\"tail\\", HarnessFlagsRefusal::TrailingEscape),
    ] {
        assert_eq!(source.harness_arguments(), Err(refusal));
    }
}

#[test]
fn one_constructor_gives_actual_and_rehearsal_the_same_argv_shape() {
    let herdr = HerdrCli::default();
    let tail = "--disallowedTools 'Artifact*' ''"
        .harness_arguments()
        .unwrap();
    for harness in [HarnessKind::Claude, HarnessKind::Codex] {
        let launch = Fixture::launch(harness.clone());
        let actual = herdr
            .harness_command(&launch, Fixture::actual_resources(&harness), &tail)
            .unwrap();
        let rehearsed = herdr
            .harness_command(&launch, HarnessCommandResources::rehearsal(&harness), &tail)
            .unwrap();
        assert_eq!(Fixture::substitute(rehearsed, &harness), actual);
        let tail_at = actual
            .windows(tail.len())
            .position(|window| window == tail)
            .expect("configured tail remains whole");
        assert!(
            tail_at
                > actual
                    .iter()
                    .position(|word| word == "medium" || word.ends_with("=medium"))
                    .unwrap()
        );
        if harness == HarnessKind::Codex {
            assert_eq!(&actual[tail_at + tail.len()..][..1], ["resume"]);
        } else {
            assert_eq!(tail_at + tail.len(), actual.len());
        }
    }
}

#[test]
fn shell_render_quotes_every_word_and_round_trips_through_posix_parsing() {
    let words = vec![
        "herdr".to_owned(),
        "<pane>".to_owned(),
        "".to_owned(),
        "a b".to_owned(),
        "single'quote".to_owned(),
        "$HOME;|>Artifact*`x`".to_owned(),
    ];
    let rendered = words.shell_command();
    assert_eq!(
        rendered,
        "'herdr' '<pane>' '' 'a b' 'single'\\''quote' '$HOME;|>Artifact*`x`'"
    );
    let script = format!("set -- {rendered}; printf '%s\\0' \"$@\"");
    let output = std::process::Command::new("sh")
        .args(["-c", &script])
        .output()
        .expect("controlled POSIX parser runs");
    assert!(output.status.success());
    let mut fields = output.stdout.split(|byte| *byte == 0).collect::<Vec<_>>();
    assert_eq!(fields.pop(), Some([].as_slice()));
    let parsed = fields
        .into_iter()
        .map(|word| String::from_utf8(word.to_vec()).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(parsed, words);
}

#[test]
fn production_rehearse_success_and_refusal_create_nothing() {
    let fixture = NexusFixture::new();
    let address = Address {
        aspect: Aspect::Field,
        topic: "rehearseTest".into(),
        model: Model::Sol,
    };
    let declaration = meta_signal_flow::ConfigureMetaflow {
        address: address.clone(),
        root_option: None,
        system_modules: vec!["spirit-core".into()],
        user_modules: vec!["operation-main-flow".into()],
        flow_kind: signal_flow::FlowKind::Regular,
        tool_set: ToolSet::Default,
        continuity: meta_signal_flow::Continuity::OnDemand,
        purpose: String::new(),
        records: vec![],
    };
    for change in [
        meta_signal_flow::ConfigurationChange::Metaflows(vec![declaration]),
        meta_signal_flow::ConfigurationChange::Models(vec![meta_signal_flow::ConfigureModel {
            model: Model::Sol,
            native: "fixture-model".into(),
        }]),
        meta_signal_flow::ConfigurationChange::HarnessFlags(vec![
            meta_signal_flow::ConfigureHarnessFlags {
                address: address.clone(),
                harness_flags: "--literal 'Artifact*'".into(),
            },
        ]),
    ] {
        fixture.nexus.store.configure_change(change).unwrap();
    }
    let request = signal_flow::LaunchRequest {
        address: address.clone(),
        remembering: Remembering::Predecessors(0),
        instruction_prompt: "Rehearse only.".into(),
    };
    let before_configuration = fixture.nexus.store.configuration_snapshot().unwrap();
    let before_flows = fixture.nexus.store.flow_nodes().unwrap();
    let call_log = fixture.directory.path().join("rehearse-herdr-calls");
    std::fs::write(
        &fixture.snapshot_program,
        format!(
            "#!/bin/sh\nprintf '%s\\n' \"$*\" >> '{}'\nexit 97\n",
            call_log.display()
        ),
    )
    .unwrap();
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(
        &fixture.snapshot_program,
        std::fs::Permissions::from_mode(0o755),
    )
    .unwrap();

    let signal_flow::Response::Rehearsed(command) = fixture
        .nexus
        .dispatch(signal_flow::Query::Rehearse(request.clone()))
    else {
        panic!("valid rehearsal answers its command")
    };
    assert!(command.contains("'<pane>'"));
    assert!(command.contains("'<thread>'"));
    assert!(command.ends_with("'resume' '<thread>'"));
    assert_eq!(
        fixture.nexus.store.configuration_snapshot().unwrap(),
        before_configuration
    );
    assert_eq!(fixture.nexus.store.flow_nodes().unwrap(), before_flows);
    assert!(
        fixture
            .nexus
            .store
            .launch_attempt("rehearse-launch")
            .unwrap()
            .is_none()
    );
    assert!(
        fixture
            .nexus
            .herdr
            .held_system_prompts
            .lock()
            .unwrap()
            .is_empty()
    );
    assert!(!call_log.exists(), "rehearsal made no Herdr/native call");

    fixture
        .nexus
        .store
        .configure_change(meta_signal_flow::ConfigurationChange::HarnessFlags(vec![
            meta_signal_flow::ConfigureHarnessFlags {
                address,
                harness_flags: "'unfinished".into(),
            },
        ]))
        .unwrap();
    let malformed_baseline = fixture.nexus.store.configuration_snapshot().unwrap();
    assert_eq!(
        fixture
            .nexus
            .dispatch(signal_flow::Query::Rehearse(request)),
        signal_flow::Response::RehearseRejected(
            signal_flow::StartRejection::MalformedHarnessFlags(
                HarnessFlagsRefusal::UnclosedSingleQuote
            )
        )
    );
    assert_eq!(
        fixture.nexus.store.configuration_snapshot().unwrap(),
        malformed_baseline
    );
    assert_eq!(fixture.nexus.store.flow_nodes().unwrap(), before_flows);
    assert!(!call_log.exists(), "refusal made no Herdr/native call");
}
