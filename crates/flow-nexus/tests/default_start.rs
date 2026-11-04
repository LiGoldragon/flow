//! The service takes an explicit typed Start and configuration reaches it
//! through the one-request meta listener before normal startup.
use datom_codec::Datomizable;
use flow_ethos::Start;
use flow_nexus::CarriesSignalFrames;
use flow_nexus::store::ConfiguresOnlyStore;
use protos::{Compactable, Protosizable};
use std::{
    fs,
    io::{BufRead, Read, Write},
    os::unix::fs::PermissionsExt,
    os::unix::net::{UnixListener, UnixStream},
    path::Path,
    process::{Child, Command, Stdio},
    time::Duration,
};

struct StartedNexus {
    child: Child,
}

impl Drop for StartedNexus {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn start(runtime: &Path, state: &Path) -> Start {
    Start {
        ordinary_socket_path: runtime
            .join("flow/flow.sock")
            .to_string_lossy()
            .into_owned(),
        meta_socket_path: runtime
            .join("flow/flow-meta.sock")
            .to_string_lossy()
            .into_owned(),
        store_path: state.join("flow.sema").to_string_lossy().into_owned(),
        landing_watch_option: None,
    }
}

fn spawn(home: &Path, runtime: &Path, start: &Start, configure_only: bool) -> StartedNexus {
    let mut command = Command::new(env!("CARGO_BIN_EXE_flow-nexus"));
    command
        .env_clear()
        .env("HOME", home)
        .env("XDG_RUNTIME_DIR", runtime)
        .env(
            "PATH",
            format!(
                "{}:{}",
                home.join("bin").display(),
                std::env::var("PATH").unwrap_or_default()
            ),
        )
        .stderr(Stdio::piped());
    if configure_only {
        command.arg("--configure-only");
    }
    command
        // Exercise the operator's literal argv, independently of Start's serializer.
        // These fixture paths are temporary ASCII paths without string delimiters.
        .arg(format!(
            "{{ «{}» «{}» «{}» None }}",
            start.ordinary_socket_path, start.meta_socket_path, start.store_path
        ))
        .spawn()
        .map(|child| StartedNexus { child })
        .expect("flow-nexus spawns with typed Start")
}

fn connect(socket: &Path, nexus: &mut StartedNexus) -> UnixStream {
    for _ in 0..500 {
        if let Ok(peer) = UnixStream::connect(socket) {
            return peer;
        }
        if let Some(status) = nexus.child.try_wait().expect("child status") {
            let mut stderr = String::new();
            if let Some(mut pipe) = nexus.child.stderr.take() {
                let _ = pipe.read_to_string(&mut stderr);
            }
            panic!("flow-nexus exited with {status}: {stderr}");
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    panic!("{} never listened", socket.display())
}

fn wait_successfully(nexus: &mut StartedNexus) {
    let status = nexus
        .child
        .wait()
        .expect("flow-nexus exits after Configure");
    if !status.success() {
        let mut stderr = String::new();
        if let Some(mut pipe) = nexus.child.stderr.take() {
            let _ = pipe.read_to_string(&mut stderr);
        }
        panic!("flow-nexus exited with {status}: {stderr}");
    }
}

fn endpoint(name: &str, home: &Path) -> meta_signal_flow::CodexEndpoint {
    meta_signal_flow::CodexEndpoint {
        client_path: format!("/opt/{name}-client"),
        home: home.join(name).to_string_lossy().into_owned(),
        control_socket_path: home
            .join(name)
            .join("control.sock")
            .to_string_lossy()
            .into_owned(),
        model_name_vector: vec![format!("{name}-model")],
    }
}

fn configuration(_start: &Start, home: &Path) -> meta_signal_flow::ConfigureNexus {
    fs::create_dir_all(home.join("source/flows")).expect("configured source root");
    meta_signal_flow::ConfigureNexus {
        source_root: home.join("source").to_string_lossy().into_owned(),
        stable_codex: endpoint("stable", home),
        next_codex: endpoint("next", home),
        harness_profiles: vec![meta_signal_flow::HarnessProfile {
            harness_kind: signal_flow::HarnessKind::Codex,
            command_sigil_vector: vec!["/".into()],
            interrupt_keys: vec!["esc".into()],
            submit_keys: Vec::new(),
        }],
        meta_aspects: vec![flow_ethos::Aspect::Psyche, flow_ethos::Aspect::Mind],
        message_nexus_path_option: Some("/run/message.sock".into()),

        message_nexus_binary_option: Some("/opt/message-nexus".into()),

        curriculum_nexus_path: home.join("curriculum.sock").to_string_lossy().into_owned(),
        lease: 60,
    }
}

fn install_script(path: &Path, body: &str) {
    fs::create_dir_all(path.parent().expect("script directory")).expect("script directory");
    fs::write(path, body).expect("fixture script");
    fs::set_permissions(path, fs::Permissions::from_mode(0o700)).expect("fixture executable");
}

fn install_launch_fakes(home: &Path, runtime: &Path, starts: usize) -> std::thread::JoinHandle<()> {
    let bin = home.join("bin");
    let herdr_socket = runtime.join("fake-herdr.sock");
    let calls = home.join("herdr-calls");
    let herdr = format!(
        r#"#!/bin/sh
printf '%s\n' "$*" >> '{calls}'
case "$*" in
  "session list --json") printf '%s\n' '{{"sessions":[{{"name":"test","socket_path":"{socket}"}}]}}' ;;
  *"workspace create"*) printf '%s\n' '{{"result":{{"workspace":{{"workspace_id":"w7"}},"root_pane":{{"pane_id":"w7:p1","terminal_id":"term-native"}}}}}}' ;;
  *"pane run"*) sh -c "$6" > "$HOME/marker" ;;
  *"pane wait-output"*) marker=$(printf '%s\n' "$*" | sed 's/.*--match \([^ ]*\).*/\1/'); line=$(grep -Fx "$marker" "$HOME/marker") || exit 10; printf '{{"result":{{"pane_id":"w7:p1","matched_line":"%s"}}}}\n' "$line" ;;
  *"agent start"*) prev=; for a in "$@"; do [ "$prev" = --session-id ] && printf '%s' "$a" > "$HOME/fake-session"; [ "$prev" = --name ] && printf '%s' "$a" > "$HOME/native-title"; prev=$a; done; session=$(cat "$HOME/fake-session"); title=$(cat "$HOME/native-title"); mkdir -p "$HOME/.claude/projects"; printf '{{"type":"custom-title","customTitle":"%s","sessionId":"%s"}}\n' "$title" "$session" > "$HOME/.claude/projects/$session.jsonl"; printf '%s\n' '{{"result":{{"agent":{{"name":"fixture"}}}}}}' ;;
  *"agent wait"*) printf '%s\n' '{{"result":{{"status":"idle"}}}}' ;;
  *"agent get"*) session=$(cat "$HOME/fake-session"); name="$5"; title=$(cat "$HOME/native-title"); printf '%s' "$name" > "$HOME/fake-agent"; printf '{{"result":{{"agent":{{"name":"%s","agent":"claude","workspace_id":"w7","pane_id":"w7:p1","terminal_id":"term-native","interactive_ready":true,"terminal_title_stripped":"%s","agent_session":{{"source":"herdr:claude","agent":"claude","kind":"id","value":"%s"}}}}}}}}\n' "$name" "$title" "$session" ;;
  *" api snapshot"*) session=$(cat "$HOME/fake-session"); name=$(cat "$HOME/fake-agent"); printf '{{"id":"fake:snapshot","result":{{"snapshot":{{"agents":[{{"name":"%s","agent":"claude","workspace_id":"w7","pane_id":"w7:p1","terminal_id":"term-native","interactive_ready":true,"agent_session":{{"source":"herdr:claude","agent":"claude","kind":"id","value":"%s"}}}}],"protocol":20,"version":"0.8.2"}},"type":"session_snapshot"}}}}\n' "$name" "$session" ;;
  *"pane rename w7:p1 "*) printf '%s' "$*" | sed 's/.*pane rename w7:p1 //' > "$HOME/pane-label"; printf '%s\n' '{{"result":{{}}}}' ;;
  *"pane report-metadata w7:p1 "*) printf '%s' "$*" | sed 's/.*--display-agent //' > "$HOME/pane-display"; printf '%s\n' '{{"result":{{}}}}' ;;
  *"pane get w7:p1"*) label=$(cat "$HOME/pane-label"); display=$(cat "$HOME/pane-display"); printf '{{"result":{{"pane":{{"pane_id":"w7:p1","terminal_id":"term-native","label":"%s","display_agent":"%s"}}}}}}\n' "$label" "$display" ;;
  *"workspace rename w7 "*) printf '%s' "$*" | sed 's/.*workspace rename w7 //' > "$HOME/workspace-label"; printf '%s\n' '{{"result":{{}}}}' ;;
  *"workspace get w7"*) label=$(cat "$HOME/workspace-label"); printf '{{"result":{{"workspace":{{"workspace_id":"w7","label":"%s"}}}}}}\n' "$label" ;;
  *) exit 8 ;;
esac
"#,
        calls = calls.display(),
        socket = herdr_socket.display(),
    );
    install_script(&bin.join("herdr"), &herdr);
    install_script(
        &bin.join("flow-id"),
        r#"#!/bin/sh
flows=; session=; prev=
for a in "$@"; do [ "$prev" = --flows-root ] && flows=$a; [ "$prev" = --parent-session ] && session=$a; prev=$a; done
identity=$(printf '%s' "$session" | tr -d '-')
alias=$(printf '%s' "$identity" | cut -c1-6)
mkdir -p "$flows/$alias"
: > "$flows/.$alias.flow-id.lock"
printf 'version=1\nharness=claude\nidentity=%s\nalias=%s\nuuid-version=uuid-v5\n' "$identity" "$alias" > "$flows/.$alias.flow-id"
printf '%s\n' "$alias"
"#,
    );
    let listener = UnixListener::bind(&herdr_socket).expect("fake Herdr socket");
    let home = home.to_path_buf();
    std::thread::spawn(move || {
        for _ in 0..starts {
            let (stream, _) = listener.accept().expect("Herdr prompt connection");
            let mut line = String::new();
            std::io::BufReader::new(&stream)
                .read_line(&mut line)
                .expect("Herdr prompt request");
            let request: serde_json::Value = serde_json::from_str(&line).expect("prompt JSON");
            let text = request["params"]["text"].as_str().expect("prompt text");
            let session = fs::read_to_string(home.join("fake-session")).expect("session");
            let transcript = home.join(format!(".claude/projects/{session}.jsonl"));
            let user = serde_json::json!({"type":"user","sessionId":session,"message":{"role":"user","content":text}});
            let answer = serde_json::json!({"type":"assistant","sessionId":session,"uuid":format!("turn-{session}"),"effort":"medium","perTurnEffort":"medium","message":{"model":"claude-test","content":[{"type":"text","text":"ready"}]}});
            let mut file = fs::OpenOptions::new()
                .append(true)
                .open(transcript)
                .expect("transcript");
            writeln!(file, "{user}\n{answer}").expect("receipt records");
            let mut stream = stream;
            stream
                .write_all(b"{\"id\":\"flow:prompt\",\"result\":{\"type\":\"agent_prompted\"}}\n")
                .expect("Herdr prompt reply");
        }
    })
}

fn serve_curriculum(socket: &Path, starts: usize) -> std::thread::JoinHandle<()> {
    use sha2::{Digest, Sha256};
    fs::create_dir_all(socket.parent().expect("Curriculum directory"))
        .expect("Curriculum directory");
    let listener = UnixListener::bind(socket).expect("fake Curriculum socket");
    std::thread::spawn(move || {
        for _ in 0..starts {
            let (mut stream, _) = listener.accept().expect("Curriculum connection");
            let mut length = [0; 4];
            stream
                .read_exact(&mut length)
                .expect("Curriculum query length");
            let mut frame = vec![0; u32::from_be_bytes(length) as usize];
            stream.read_exact(&mut frame).expect("Curriculum query");
            let query =
                rkyv::from_bytes::<flow_nexus::CurriculumQuery, rkyv::rancor::Error>(&frame)
                    .expect("Curriculum query decodes");
            let flow_nexus::CurriculumQuery::ComposeStartup(selection) = query else {
                panic!("only ComposeStartup is expected")
            };
            assert_eq!(selection.system_modules, ["spirit-core"]);
            assert!(selection.user_modules.is_empty());
            let body = "Spirit body.";
            let system_text = format!(
                "These are your skills, given here; do not reload them.\n\nModule IDs: spirit-core\n\n<!-- module: spirit-core -->\n{body}\n<!-- end-module: spirit-core -->\n"
            );
            let digest = |text: &str| format!("{:x}", Sha256::digest(text.as_bytes()));
            let empty_hash = digest("");
            let outcome =
                flow_nexus::CurriculumOutcome::StartupComposed(flow_nexus::StartupComposition {
                    system_text: system_text.clone(),
                    user_text: String::new(),
                    records_text: String::new(),
                    system_payload_sha256: digest(&system_text),
                    user_payload_sha256: empty_hash.clone(),
                    records_payload_sha256: empty_hash,
                    context_provenance_vector: vec![flow_nexus::ContextProvenance {
                        module_identity: "spirit-core".into(),
                        repository_name: "fixture-context".into(),
                        source_revision: "0".repeat(40),
                        source_relative_path: "spirit-core.md".into(),
                        source_sha256: digest(body),
                        body_sha256: digest(body),
                        rendered_sha256: digest(body),
                        startup_placement:
                            flow_nexus::generated::curriculum::StartupPlacement::System,
                        integer: 0,
                    }],
                });
            let answer =
                rkyv::to_bytes::<rkyv::rancor::Error>(&outcome).expect("Curriculum answer encodes");
            stream
                .write_all(&(answer.len() as u32).to_be_bytes())
                .and_then(|_| stream.write_all(&answer))
                .expect("Curriculum answer");
        }
    })
}

fn configure_fresh(home: &Path, runtime: &Path, start: &Start) {
    let mut nexus = spawn(home, runtime, start, true);
    let mut meta = connect(Path::new(&start.meta_socket_path), &mut nexus);
    let desired = configuration(start, home);
    let query = meta_signal_flow::Query::Configure(meta_signal_flow::ConfigurationChange::Nexus(
        desired.clone(),
    ));
    let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&query).expect("Configure encodes");
    meta.write_all(&(bytes.len() as u32).to_be_bytes())
        .and_then(|_| meta.write_all(&bytes))
        .expect("Configure written");
    let mut length = [0; 4];
    meta.read_exact(&mut length).expect("Configure length");
    let mut reply = vec![0; u32::from_be_bytes(length) as usize];
    meta.read_exact(&mut reply).expect("Configure reply");
    let reply = rkyv::from_bytes::<meta_signal_flow::Response, rkyv::rancor::Error>(&reply)
        .expect("Configure reply decodes");
    assert!(matches!(
        reply,
        meta_signal_flow::Response::Configured(meta_signal_flow::ConfigureOutcome::Replaced)
    ));
    drop(meta);
    wait_successfully(&mut nexus);
}

fn meta_call(
    nexus: &mut StartedNexus,
    socket: &str,
    query: &meta_signal_flow::Query,
) -> meta_signal_flow::Response {
    let mut peer = connect(Path::new(socket), nexus);
    let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(query).expect("meta query encodes");
    peer.write_all(&(bytes.len() as u32).to_be_bytes())
        .and_then(|_| peer.write_all(&bytes))
        .expect("meta query written");
    let mut length = [0; 4];
    peer.read_exact(&mut length).expect("meta response length");
    let mut reply = vec![0; u32::from_be_bytes(length) as usize];
    peer.read_exact(&mut reply).expect("meta response");
    rkyv::from_bytes::<meta_signal_flow::Response, rkyv::rancor::Error>(&reply)
        .expect("meta response decodes")
}

fn launch_profile(request: &str, topic: &str) -> signal_flow::LaunchProfile {
    signal_flow::LaunchProfile {
        launch_request_id: request.into(),
        system_modules: vec!["spirit-core".into()],
        user_modules: vec![],
        address: signal_flow::Address {
            aspect: signal_flow::Aspect::Mind,
            topic: topic.into(),
            layer: signal_flow::Layer::Secondary,
        },
        model_name: "gpt-test".into(),
        effort: "medium".into(),
        flow_id_option: None,
        remembering: signal_flow::Remembering::Predecessors(0),
        herdr_session_name: "test".into(),
        leave_outs_option: None,
        instruction_prompt: "test".into(),
        flow_kind: signal_flow::FlowKind::Regular,
        tool_set: signal_flow::ToolSet::Default,
    }
}

#[cfg(not(feature = "executable-default-configuration"))]
#[test]
fn ordinary_empty_start_configures_and_becomes_ready_without_restart() {
    let home = tempfile::tempdir().expect("temporary home");
    let runtime = tempfile::tempdir().expect("temporary runtime directory");
    let state = tempfile::tempdir().expect("dedicated state directory");
    let start = start(runtime.path(), state.path());
    let prompt_server = install_launch_fakes(home.path(), runtime.path(), 2);
    let curriculum = serve_curriculum(&home.path().join("curriculum.sock"), 2);
    let mut nexus = spawn(home.path(), runtime.path(), &start, false);

    assert_eq!(
        meta_call(
            &mut nexus,
            &start.meta_socket_path,
            &meta_signal_flow::Query::Readiness,
        ),
        meta_signal_flow::Response::Readiness(meta_signal_flow::NexusReadiness::ConfigServing,)
    );
    let mut ordinary = connect(Path::new(&start.ordinary_socket_path), &mut nexus);
    ordinary
        .write_query(&signal_flow::Query::Start(launch_profile(
            "not-configured-test",
            "readinessBefore",
        )))
        .expect("Start written while configuring");
    assert_eq!(
        ordinary.read_response().expect("Start answered"),
        signal_flow::Response::StartRejected(signal_flow::StartRejection::NotConfigured)
    );
    drop(ordinary);
    assert!(
        !home.path().join("herdr-calls").exists(),
        "no launch or recovery effect runs before configuration"
    );

    assert_eq!(
        meta_call(
            &mut nexus,
            &start.meta_socket_path,
            &meta_signal_flow::Query::Configure(meta_signal_flow::ConfigurationChange::Nexus(
                configuration(&start, home.path())
            ),),
        ),
        meta_signal_flow::Response::Configured(meta_signal_flow::ConfigureOutcome::Replaced)
    );
    assert_eq!(
        meta_call(
            &mut nexus,
            &start.meta_socket_path,
            &meta_signal_flow::Query::Readiness,
        ),
        meta_signal_flow::Response::Readiness(meta_signal_flow::NexusReadiness::Ready)
    );
    assert_eq!(
        meta_call(
            &mut nexus,
            &start.meta_socket_path,
            &meta_signal_flow::Query::Configure(meta_signal_flow::ConfigurationChange::Models(
                vec![meta_signal_flow::ConfigureModel {
                    layer: signal_flow::Layer::Secondary,
                    aspect: signal_flow::Aspect::Mind,
                    native: "claude-test".into(),
                }],
            )),
        ),
        meta_signal_flow::Response::Configured(meta_signal_flow::ConfigureOutcome::Replaced)
    );
    let mut ordinary = connect(Path::new(&start.ordinary_socket_path), &mut nexus);
    ordinary
        .write_query(&signal_flow::Query::Start(launch_profile(
            "configured-start",
            "configuredStart",
        )))
        .expect("configured Start written");
    let response = ordinary.read_response().expect("configured Start answered");
    let calls = fs::read_to_string(home.path().join("herdr-calls")).unwrap_or_default();
    assert!(
        matches!(response, signal_flow::Response::Started(_)),
        "{response:?}\nHerdr calls:\n{calls}"
    );
    drop(ordinary);

    assert!(terminate(&mut nexus).success());
    let mut reopened = spawn(home.path(), runtime.path(), &start, false);
    assert_eq!(
        meta_call(
            &mut reopened,
            &start.meta_socket_path,
            &meta_signal_flow::Query::Readiness,
        ),
        meta_signal_flow::Response::Readiness(meta_signal_flow::NexusReadiness::Ready)
    );
    let mut ordinary = connect(Path::new(&start.ordinary_socket_path), &mut reopened);
    ordinary
        .write_query(&signal_flow::Query::Start(launch_profile(
            "reopened-start",
            "reopenedStart",
        )))
        .expect("reopened Start written");
    let response = ordinary.read_response().expect("reopened Start answered");
    let calls = fs::read_to_string(home.path().join("herdr-calls")).unwrap_or_default();
    assert!(
        matches!(response, signal_flow::Response::Started(_)),
        "{response:?}\nHerdr calls:\n{calls}"
    );
    drop(ordinary);
    assert!(terminate(&mut reopened).success());
    prompt_server.join().expect("fake Herdr served both Starts");
    curriculum
        .join()
        .expect("fake Curriculum served both Starts");
    let calls = fs::read_to_string(home.path().join("herdr-calls")).expect("Herdr call ledger");
    let lines = calls.lines().collect::<Vec<_>>();
    let launches = lines
        .iter()
        .enumerate()
        .filter_map(|(index, line)| line.contains("workspace create").then_some(index))
        .collect::<Vec<_>>();
    assert_eq!(launches.len(), 2, "Herdr calls:\n{calls}");
    let expected = [
        "workspace create",
        "workspace rename",
        "pane report-metadata",
        "pane run",
        "pane wait-output",
        "agent start",
        "agent wait",
        "agent get named",
        "api snapshot",
        "agent wait",
        "agent get pane",
        "agent get pane",
        "agent get pane",
        "pane rename",
        "pane report-metadata",
        "pane get",
        "workspace rename",
        "workspace get",
        "agent get pane",
        "agent get pane",
        "agent wait",
        "session list",
    ];
    for (number, (start_at, request)) in launches
        .iter()
        .copied()
        .zip(["configured-start", "reopened-start"])
        .enumerate()
    {
        let end = launches.get(number + 1).copied().unwrap_or(lines.len());
        let launch = &lines[start_at..end];
        let operation = |line: &&str| {
            if line.contains("workspace create") {
                "workspace create"
            } else if line.contains("workspace rename") {
                "workspace rename"
            } else if line.contains("workspace get") {
                "workspace get"
            } else if line.contains("pane report-metadata") {
                "pane report-metadata"
            } else if line.contains("pane run") {
                "pane run"
            } else if line.contains("pane wait-output") {
                "pane wait-output"
            } else if line.contains("agent start") {
                "agent start"
            } else if line.contains("agent wait") {
                "agent wait"
            } else if line.contains("agent get w7:p1") {
                "agent get pane"
            } else if line.contains("agent get") {
                "agent get named"
            } else if line.contains("api snapshot") {
                "api snapshot"
            } else if line.contains("pane rename") {
                "pane rename"
            } else if line.contains("pane get") {
                "pane get"
            } else if *line == "session list --json" {
                "session list"
            } else {
                "unexpected"
            }
        };
        assert_eq!(
            launch.iter().map(operation).collect::<Vec<_>>(),
            expected,
            "Herdr calls:\n{calls}"
        );
        assert!(launch[0].contains(&format!("FLOW_LAUNCH_REQUEST_ID={request}")));
        let start = launch
            .iter()
            .find(|line| line.contains("agent start"))
            .expect("one agent start");
        assert!(start.contains(" --model claude-test --effort medium"));
        let session = start
            .split(" --session-id ")
            .nth(1)
            .and_then(|tail| tail.split_whitespace().next())
            .expect("selected session");
        let alias = session.replace('-', "").chars().take(6).collect::<String>();
        assert!(
            launch
                .iter()
                .any(|line| line.contains(&format!("export FLOW_ID={alias}"))),
            "selected session {session} binds its claimed FlowId; calls:\n{calls}"
        );
    }
}

#[test]
fn two_explicit_starts_serve_disjoint_sockets_and_memory() {
    let home_a = tempfile::tempdir().expect("home A");
    let home_b = tempfile::tempdir().expect("home B");
    let runtime_a = tempfile::tempdir().expect("runtime A");
    let runtime_b = tempfile::tempdir().expect("runtime B");
    let state_a = tempfile::tempdir().expect("state A");
    let state_b = tempfile::tempdir().expect("state B");
    let start_a = start(runtime_a.path(), state_a.path());
    let start_b = start(runtime_b.path(), state_b.path());
    configure_fresh(home_a.path(), runtime_a.path(), &start_a);
    configure_fresh(home_b.path(), runtime_b.path(), &start_b);

    let mut nexus_a = spawn(home_a.path(), runtime_a.path(), &start_a, false);
    let mut nexus_b = spawn(home_b.path(), runtime_b.path(), &start_b, false);
    for (nexus, start) in [(&mut nexus_a, &start_a), (&mut nexus_b, &start_b)] {
        let mut ordinary = connect(Path::new(&start.ordinary_socket_path), nexus);
        ordinary
            .write_query(&signal_flow::Query::Metaflows)
            .expect("Metaflows written");
        assert!(matches!(
            ordinary.read_response().expect("Metaflows answered"),
            signal_flow::Response::Listed(rows) if rows.is_empty()
        ));
    }

    let meta_a = meta_call(
        &mut nexus_a,
        &start_a.meta_socket_path,
        &meta_signal_flow::Query::Configuration,
    );
    let meta_b = meta_call(
        &mut nexus_b,
        &start_b.meta_socket_path,
        &meta_signal_flow::Query::Configuration,
    );
    let meta_signal_flow::Response::Configuration(configuration_a) = meta_a else {
        panic!("instance A returns its Configuration")
    };
    let meta_signal_flow::Response::Configuration(configuration_b) = meta_b else {
        panic!("instance B returns its Configuration")
    };
    assert_eq!(
        configuration_a.nexus.source_root,
        home_a.path().join("source").to_string_lossy()
    );
    assert_eq!(
        configuration_b.nexus.source_root,
        home_b.path().join("source").to_string_lossy()
    );
    assert_ne!(start_a.store_path, start_b.store_path);
    assert!(Path::new(&start_a.store_path).exists());
    assert!(Path::new(&start_b.store_path).exists());
}

#[test]
fn configure_only_commits_before_normal_start_uses_the_declared_store() {
    let home = tempfile::tempdir().expect("temporary home");
    let runtime = tempfile::tempdir().expect("temporary runtime directory");
    let state = tempfile::tempdir().expect("dedicated state directory");
    let start = start(runtime.path(), state.path());
    configure_fresh(home.path(), runtime.path(), &start);

    assert!(state.path().join("flow.sema").exists());
    assert!(
        !home.path().join(".local/state/flow/flow.sema").exists(),
        "typed Start does not open the HOME-derived store"
    );

    let mut nexus = spawn(home.path(), runtime.path(), &start, false);
    let mut ordinary = connect(Path::new(&start.ordinary_socket_path), &mut nexus);
    ordinary
        .write_query(&signal_flow::Query::Metaflows)
        .expect("List written");
    assert!(matches!(
        ordinary.read_response().expect("List answered"),
        signal_flow::Response::Listed(rows) if rows.is_empty()
    ));
}

#[test]
fn environment_overrides_do_not_replace_the_explicit_configuration() {
    use flow_nexus::store::ConfigurationOnlyStore;

    let home = tempfile::tempdir().expect("temporary home");
    let runtime = tempfile::tempdir().expect("temporary runtime directory");
    let state = tempfile::tempdir().expect("dedicated state directory");
    let start = start(runtime.path(), state.path());
    let mut nexus = Command::new(env!("CARGO_BIN_EXE_flow-nexus"))
        .env_clear()
        .env("HOME", home.path())
        .env("XDG_RUNTIME_DIR", runtime.path())
        .env("FLOW_SOURCE_ROOT", "/srv/elsewhere")
        .env("FLOW_CODEX_STABLE_CLIENT", "/opt/elsewhere-client")
        .arg("--configure-only")
        .arg(start.datomize(vec![]).protosize().compact())
        .stderr(Stdio::piped())
        .spawn()
        .map(|child| StartedNexus { child })
        .expect("flow-nexus spawns");
    let mut meta = connect(Path::new(&start.meta_socket_path), &mut nexus);
    let desired = configuration(&start, home.path());
    let query = meta_signal_flow::Query::Configure(meta_signal_flow::ConfigurationChange::Nexus(
        desired.clone(),
    ));
    let bytes = rkyv::to_bytes::<rkyv::rancor::Error>(&query).expect("Configure encodes");
    meta.write_all(&(bytes.len() as u32).to_be_bytes())
        .and_then(|_| meta.write_all(&bytes))
        .expect("Configure written");
    let mut length = [0; 4];
    meta.read_exact(&mut length).expect("Configure length");
    let mut reply = vec![0; u32::from_be_bytes(length) as usize];
    meta.read_exact(&mut reply).expect("Configure reply");
    let response = rkyv::from_bytes::<meta_signal_flow::Response, rkyv::rancor::Error>(&reply)
        .expect("Configure reply decodes");
    assert!(matches!(
        response,
        meta_signal_flow::Response::Configured(meta_signal_flow::ConfigureOutcome::Replaced)
    ));
    drop(meta);
    wait_successfully(&mut nexus);

    let stored = ConfigurationOnlyStore::open(Path::new(&start.store_path))
        .expect("typed store reopens")
        .configuration_snapshot()
        .expect("configuration reads")
        .expect("whole configuration was committed")
        .nexus;
    assert_eq!(stored, desired);
    assert_eq!(
        stored.source_root,
        home.path().join("source").to_string_lossy().into_owned()
    );
    assert_eq!(stored.stable_codex.client_path, "/opt/stable-client");
    assert!(
        !home.path().join(".local/state/flow/flow.sema").exists(),
        "FLOW_ variables and defaults do not redirect typed StorePath"
    );
}

fn recovery_flag(store: &Path) -> u8 {
    std::fs::read(store)
        .expect("store bytes read")
        .get(9)
        .map_or(0, |god| god & 2)
}

fn terminate(nexus: &mut StartedNexus) -> std::process::ExitStatus {
    // SAFETY: kill only sends a signal to the child this test started.
    let sent = unsafe { libc::kill(nexus.child.id() as libc::pid_t, libc::SIGTERM) };
    assert_eq!(sent, 0, "SIGTERM sent");
    for _ in 0..1_000 {
        if let Some(status) = nexus.child.try_wait().expect("child status") {
            return status;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    panic!("flow-nexus did not stop on SIGTERM")
}

fn plant_stale_socket(socket: &Path) {
    std::fs::create_dir_all(socket.parent().expect("socket directory")).expect("directory");
    drop(std::os::unix::net::UnixListener::bind(socket).expect("socket inode"));
    // A child forked by a concurrent test holds the listener until it execs.
    for _ in 0..500 {
        if UnixStream::connect(socket).is_err() {
            return;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    panic!("{} never became stale", socket.display())
}

#[test]
fn sigterm_stops_the_daemon_and_closes_its_store_cleanly() {
    let home = tempfile::tempdir().expect("temporary home");
    let runtime = tempfile::tempdir().expect("temporary runtime directory");
    let state = tempfile::tempdir().expect("dedicated state directory");
    let start = start(runtime.path(), state.path());
    let store = Path::new(&start.store_path);
    configure_fresh(home.path(), runtime.path(), &start);
    plant_stale_socket(Path::new(&start.ordinary_socket_path));
    plant_stale_socket(Path::new(&start.meta_socket_path));

    let mut nexus = spawn(home.path(), runtime.path(), &start, false);
    let mut ordinary = connect(Path::new(&start.ordinary_socket_path), &mut nexus);
    ordinary
        .write_query(&signal_flow::Query::Metaflows)
        .expect("List written");
    assert!(matches!(
        ordinary
            .read_response()
            .expect("List answered past the stale socket"),
        signal_flow::Response::Listed(_)
    ));
    drop(ordinary);
    connect(Path::new(&start.meta_socket_path), &mut nexus);
    assert_eq!(recovery_flag(store), 2, "a running writer holds the flag");

    assert!(terminate(&mut nexus).success());
    assert_eq!(recovery_flag(store), 0, "the store closed cleanly");
    redb::ReadOnlyDatabase::open(store).expect("read-only recognition opens");
    flow_nexus::store::ConfigurationOnlyStore::open(store).expect("the store is recognized");
    assert!(!Path::new(&start.ordinary_socket_path).exists());
    assert!(!Path::new(&start.meta_socket_path).exists());
}

#[test]
fn sigterm_stops_configure_only_and_closes_its_store_cleanly() {
    let home = tempfile::tempdir().expect("temporary home");
    let runtime = tempfile::tempdir().expect("temporary runtime directory");
    let state = tempfile::tempdir().expect("dedicated state directory");
    let start = start(runtime.path(), state.path());
    let store = Path::new(&start.store_path);
    plant_stale_socket(Path::new(&start.meta_socket_path));

    let mut nexus = spawn(home.path(), runtime.path(), &start, true);
    // A signal that arrives before the listener waits stays pending for the
    // watching thread, so the store being open is enough.
    for _ in 0..500 {
        if store.exists() && recovery_flag(store) == 2 {
            break;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(recovery_flag(store), 2, "a running writer holds the flag");

    let _ = terminate(&mut nexus);
    assert_eq!(recovery_flag(store), 0, "the store closed cleanly");
    flow_nexus::store::ConfigurationOnlyStore::open(store).expect("the store is recognized");
    assert!(!Path::new(&start.meta_socket_path).exists());
}

#[test]
fn a_live_nexus_socket_is_never_taken_over() {
    let home = tempfile::tempdir().expect("temporary home");
    let runtime = tempfile::tempdir().expect("temporary runtime directory");
    let state = tempfile::tempdir().expect("dedicated state directory");
    let other_state = tempfile::tempdir().expect("second state directory");
    let start = start(runtime.path(), state.path());
    configure_fresh(home.path(), runtime.path(), &start);
    let mut nexus = spawn(home.path(), runtime.path(), &start, false);
    drop(connect(Path::new(&start.meta_socket_path), &mut nexus));
    drop(connect(Path::new(&start.ordinary_socket_path), &mut nexus));

    let mut intruder = spawn(
        home.path(),
        runtime.path(),
        &Start {
            store_path: other_state
                .path()
                .join("flow.sema")
                .to_string_lossy()
                .into_owned(),
            ..start.clone()
        },
        true,
    );
    assert!(
        !intruder.child.wait().expect("intruder exits").success(),
        "a live meta socket is refused"
    );

    let mut ordinary = connect(Path::new(&start.ordinary_socket_path), &mut nexus);
    ordinary
        .write_query(&signal_flow::Query::Metaflows)
        .expect("List written");
    assert!(matches!(
        ordinary
            .read_response()
            .expect("the live Nexus still answers"),
        signal_flow::Response::Listed(_)
    ));
}

#[cfg(feature = "executable-default-configuration")]
#[test]
fn configured_executable_seeds_only_its_fresh_typed_nexus_row() {
    let home = tempfile::tempdir().expect("temporary home");
    let runtime = tempfile::tempdir().expect("temporary runtime directory");
    let state = tempfile::tempdir().expect("dedicated state directory");
    let start = start(runtime.path(), state.path());

    let mut nexus = spawn(home.path(), runtime.path(), &start, false);
    let mut ordinary = connect(Path::new(&start.ordinary_socket_path), &mut nexus);
    ordinary
        .write_query(&signal_flow::Query::Metaflows)
        .expect("List written");
    assert!(matches!(
        ordinary.read_response().expect("List answered"),
        signal_flow::Response::Listed(rows) if rows.is_empty()
    ));
    drop(ordinary);
    drop(nexus);

    let store = flow_nexus::store::ConfigurationOnlyStore::open(Path::new(&start.store_path))
        .expect("configured store reopens");
    let snapshot = store
        .configuration_snapshot()
        .expect("snapshot reads")
        .expect("fresh executable seeded Nexus");
    assert_eq!(snapshot.nexus.lease, 60);
    assert!(snapshot.nexus.message_nexus_path_option.is_none());
    assert!(snapshot.nexus.message_nexus_binary_option.is_none());
}
