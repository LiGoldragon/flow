//! Curriculum 60609's own frames. A Curriculum Nexus built at 60609c55 ran on
//! frozen context, and its own CLI asked it twice through a proxy that kept
//! each frame raw: a ComposeStartup naming a module it lacks, answered
//! `Rejected` (a typed Output variant), and one it
//! composes, answered `StartupComposed`. Flow asks as that CLI asks, byte
//! for byte; its schema reads each answer as written; and its production
//! composer, given each answer on a socket, refuses the first with
//! Curriculum's text kept whole and verifies the second.

use crate::context::{
    CONTEXT_OPENING, ComposesFlowContext, ContextRequest, CurriculumComposer,
    OpensCurriculumComposer, RendersCurriculumIdentity, SelectsStartup, VerifiedContext,
    VerifiesContext,
};
use signal_curriculum_nexus::{Input, Output};
use signal_flow::{Address, Aspect, ContextRefusal, HarnessKind, Model};
use std::io::{Read, Write};

/// The typed missing-module query, as the accepted producer CLI sent it.
const MISSING_MODULE_QUERY: &[u8] = include_bytes!("curriculum-60609/missing-module.query.rkyv");
/// Its answer: `Rejected.«missing module intent:absent:0»`.
const MISSING_MODULE_ANSWER: &[u8] = include_bytes!("curriculum-60609/missing-module.answer.rkyv");
/// The typed positive startup query, as the accepted producer CLI sent it.
const STARTUP_COMPOSED_QUERY: &[u8] =
    include_bytes!("curriculum-60609/startup-composed.query.rkyv");
/// Its answer: the composed startup.
const STARTUP_COMPOSED_ANSWER: &[u8] =
    include_bytes!("curriculum-60609/startup-composed.answer.rkyv");

fn request(address: Address, system: &[&str], user: &[&str]) -> ContextRequest {
    ContextRequest {
        address,
        harness_kind: HarnessKind::Codex,
        system_modules: signal_flow::SystemModules(
            system
                .iter()
                .map(|module| signal_flow::ModuleIdentity((*module).into()))
                .collect(),
        ),
        prompt_modules: signal_flow::PromptModules(
            user.iter()
                .map(|module| signal_flow::ModuleIdentity((*module).into()))
                .collect(),
        ),
        remembered_flows: crate::generated::starter::RememberedFlows(vec![]),
    }
}

fn missing_module_request() -> ContextRequest {
    request(
        Address {
            aspect: Aspect::Mind,
            topic: flow_ethos::Topic("core".into()),
            model: Model::Sol,
        },
        &["spirit:core:0", "intent:absent:0"],
        &[],
    )
}

fn startup_request() -> ContextRequest {
    request(
        Address {
            aspect: Aspect::Mind,
            topic: flow_ethos::Topic("core".into()),
            model: Model::Sol,
        },
        &["spirit:core:0", "intent:energy:0"],
        &[],
    )
}

/// One Curriculum exchange replayed on a Unix socket: the query Flow sends
/// is kept, and the recorded answer frame is sent back as it was.
fn replayed(answer: &'static [u8]) -> (CurriculumComposer, std::thread::JoinHandle<Vec<u8>>) {
    let directory = Box::leak(Box::new(tempfile::tempdir().expect("socket directory")));
    let socket = directory.path().join("curriculum.sock");
    let listener = std::os::unix::net::UnixListener::bind(&socket).expect("Curriculum socket");
    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("Flow connects");
        let mut prefix = [0_u8; 4];
        stream.read_exact(&mut prefix).expect("query length");
        let mut query = vec![0_u8; u32::from_be_bytes(prefix) as usize];
        stream.read_exact(&mut query).expect("query");
        stream
            .write_all(&(answer.len() as u32).to_be_bytes())
            .and_then(|_| stream.write_all(answer))
            .expect("answer written");
        query
    });
    (
        CurriculumComposer::at(socket.to_string_lossy().as_ref()),
        server,
    )
}

/// A real 60609 `Rejected` frame reads as `Rejected` with its text, never as
/// another variant, and the production composer refuses the request with
/// `ComposerRejected` carrying that text whole.
#[test]
fn a_real_60609_rejected_frame_is_a_composer_rejection_carrying_its_text() {
    let request = missing_module_request();
    assert_eq!(
        rkyv::to_bytes::<rkyv::rancor::Error>(&Input::ComposeStartup(
            request
                .startup_selection()
                .expect("canonical fixture selection")
        ))
        .expect("query archives")
        .as_slice(),
        MISSING_MODULE_QUERY,
        "Flow asks as Curriculum's own CLI asks"
    );
    assert_eq!(
        rkyv::from_bytes::<Output, rkyv::rancor::Error>(MISSING_MODULE_ANSWER)
            .expect("the 60609 answer reads"),
        Output::Rejected("missing module intent:absent:0".into())
    );
    let (composer, server) = replayed(MISSING_MODULE_ANSWER);
    assert_eq!(
        composer.compose_flow_context(&request),
        Err(ContextRefusal::ComposerRejected(
            "missing module intent:absent:0".into()
        ))
    );
    assert_eq!(server.join().expect("served"), MISSING_MODULE_QUERY);
}

/// A real 60609 `StartupComposed` frame reads as the composition it is, and
/// the production composer's answer passes Flow's verification: its
/// payload digests, its modules at their placements, its rendering.
#[test]
fn a_real_60609_startup_composed_frame_reads_and_verifies() {
    let request = startup_request();
    assert_eq!(
        rkyv::to_bytes::<rkyv::rancor::Error>(&Input::ComposeStartup(
            request
                .startup_selection()
                .expect("canonical fixture selection")
        ))
        .expect("query archives")
        .as_slice(),
        STARTUP_COMPOSED_QUERY,
        "Flow asks as Curriculum's own CLI asks"
    );
    let Output::StartupComposed(composition) =
        rkyv::from_bytes::<Output, rkyv::rancor::Error>(STARTUP_COMPOSED_ANSWER)
            .expect("the 60609 answer reads")
    else {
        panic!("a StartupComposed frame")
    };
    assert!(composition.system_text.0.starts_with(CONTEXT_OPENING));
    assert_eq!(
        composition
            .context_provenance_vector
            .iter()
            .map(|provenance| provenance.module_identity.identity_text())
            .collect::<Vec<_>>(),
        ["spirit:core:0", "intent:energy:0"]
    );
    let (composer, server) = replayed(STARTUP_COMPOSED_ANSWER);
    let composed = composer
        .compose_flow_context(&request)
        .expect("the composer takes the composition");
    assert_eq!(composed, composition);
    assert!(VerifiedContext::verified(&request, &composed).is_ok());
    assert_eq!(server.join().expect("served"), STARTUP_COMPOSED_QUERY);
}

/// The old producer archives remain immutable negative compatibility evidence:
/// they cannot stand for the typed ComposeStartup/StartupComposed operations.
#[test]
fn old_ordinary_archives_are_not_typed_startup_operations() {
    let old_query = include_bytes!("curriculum-478c/startup-composed.query.rkyv");
    let old_answer = include_bytes!("curriculum-478c/startup-composed.answer.rkyv");
    assert!(!matches!(
        rkyv::from_bytes::<Input, rkyv::rancor::Error>(old_query),
        Ok(Input::ComposeStartup(_))
    ));
    assert!(!matches!(
        rkyv::from_bytes::<Output, rkyv::rancor::Error>(old_answer),
        Ok(Output::StartupComposed(_))
    ));
}

/// Actual producer60609 selection Single1 expands to the singleton, containing
/// span and co-selected energy module. Flow keeps actual identities, source paths and
/// rendered slices rather than flattening them to the requested binding.
#[test]
fn real_numeric_binding_and_exact_span_frames_keep_actual_provenance() {
    for (identity, query, answer, expected) in [
        (
            "spirit:binding:1",
            include_bytes!("curriculum-60609/binding1-singleton.query.rkyv").as_slice(),
            include_bytes!("curriculum-60609/binding1-singleton.answer.rkyv").as_slice(),
            vec!["spirit:binding:1", "spirit:binding:0-1", "intent:energy:0"],
        ),
        (
            "spirit:binding:0-1",
            include_bytes!("curriculum-60609/binding0-1-span.query.rkyv").as_slice(),
            include_bytes!("curriculum-60609/binding0-1-span.answer.rkyv").as_slice(),
            vec!["spirit:binding:0-1", "intent:energy:0"],
        ),
    ] {
        let request = request(
            Address {
                aspect: Aspect::Mind,
                topic: flow_ethos::Topic("core".into()),
                model: Model::Sol,
            },
            &[identity, "intent:energy:0"],
            &[],
        );
        assert_eq!(
            rkyv::to_bytes::<rkyv::rancor::Error>(&Input::ComposeStartup(
                request.startup_selection().unwrap()
            ))
            .unwrap()
            .as_slice(),
            query
        );
        let Output::StartupComposed(composition) =
            rkyv::from_bytes::<Output, rkyv::rancor::Error>(answer).unwrap()
        else {
            panic!("typed composition")
        };
        let rows = &composition.context_provenance_vector;
        assert_eq!(
            rows.iter()
                .map(|r| r.module_identity.identity_text())
                .collect::<Vec<_>>(),
            expected
        );
        assert!(
            !rows
                .iter()
                .any(|r| r.module_identity.identity_text() == "spirit:binding:0")
        );
        let span = rows
            .iter()
            .find(|r| r.module_identity.identity_text() == "spirit:binding:0-1")
            .unwrap();
        assert_eq!(span.source_relative_path.0, "spirit/binding/0-1.md");
        assert!(VerifiedContext::verified(&request, &composition).is_ok());
        let (composer, server) = replayed(answer);
        let actual = composer.compose_flow_context(&request).unwrap();
        assert_eq!(actual, composition);
        assert!(VerifiedContext::verified(&request, actual).is_ok());
        assert_eq!(server.join().unwrap(), query);
    }
}

/// The capsule's pinned producer and registered Field source are served by
/// a private Curriculum process. Flow uses the shared catalog for explicit
/// discovery and resolution, and its own composer for each startup target.
#[test]
#[ignore = "requires the pinned Curriculum binary and Field source from the flake check"]
fn the_paired_curriculum_serves_state_on_demand_outside_every_startup() {
    use crate::context::{ParsesCurriculumIdentity, ResolvesModule};
    use crate::event_wait::AwaitsDirectory;
    use crate::generated::emission::{CurriculumExchange, Emittable};
    use sha2::{Digest, Sha256};
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::process::{Child, Command, Stdio};
    use std::time::{Duration, Instant};

    struct RunningCurriculum(Child);
    impl Drop for RunningCurriculum {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }

    let temporary = tempfile::tempdir().expect("Curriculum witness directory");
    let scratch = temporary.path();
    let field_source = PathBuf::from(std::env::var_os("FLOW_STATE_FIELD_SOURCE").unwrap());
    let field_revision = std::env::var("FLOW_STATE_FIELD_REVISION").unwrap();
    let roots = ["psyche-pattern", "mind-pattern", "field-pattern"].map(|name| scratch.join(name));
    let write = |root: &Path, relative: &str, text: &str| {
        let path = root.join(relative);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, text).unwrap();
    };
    for (root, revision) in
        roots
            .iter()
            .zip(["a".repeat(40), "b".repeat(40), field_revision.clone()])
    {
        write(root, ".git/HEAD", &format!("{revision}\n"));
    }
    assert!(
        Command::new("cp")
            .args(["-R", "--"])
            .arg(field_source.join("."))
            .arg(&roots[2])
            .status()
            .expect("copy the locked Field repository")
            .success()
    );
    assert_eq!(
        fs::read(roots[2].join("modules.datom")).unwrap(),
        fs::read(field_source.join("modules.datom")).unwrap(),
        "the witness must use the locked Field declaration corpus unchanged"
    );
    write(
        &roots[0],
        "modules.datom",
        "[ { { Spirit core Single.0 } spirit/core/0.md } { { Intent main-flow Single.0 } intent/main-flow/0.md } { { Vision energy-management Single.0 } vision/energy-management/0.md } ]",
    );
    write(&roots[0], "spirit/core/0.md", "Flow system control.");
    write(
        &roots[0],
        "intent/main-flow/0.md",
        "Flow seat intent control.",
    );
    write(
        &roots[0],
        "vision/energy-management/0.md",
        "Fixture dependency for the locked Field State corpus.",
    );
    write(
        &roots[1],
        "modules.datom",
        "[ { { Knowledge control Single.0 } knowledge/control/0.md } { { Notion leading Single.0 } notion/leading/0.md } { { Notion subagent-roles Single.0 } notion/subagent-roles/0.md } ]",
    );
    write(
        &roots[1],
        "knowledge/control/0.md",
        "---\ndescription: Flow knowledge control.\ndependencies: []\nplacement: user\n---\nFlow user control.",
    );
    write(
        &roots[1],
        "notion/leading/0.md",
        "Fixture dependency for the locked Field State corpus.",
    );
    write(
        &roots[1],
        "notion/subagent-roles/0.md",
        "Fixture dependency for the locked Field State corpus.",
    );
    write(scratch, "roles.datom", "Roles.{ [] }");

    let runtime = scratch.join("runtime");
    let socket_directory = runtime.join("curriculum");
    let socket = socket_directory.join("curriculum.sock");
    let records = scratch.join("records");
    fs::create_dir_all(&socket_directory).unwrap();
    fs::create_dir(&records).unwrap();

    let _child = RunningCurriculum(
        Command::new(std::env::var_os("FLOW_STATE_CURRICULUM_BIN").unwrap())
            .env_clear()
            .env("XDG_RUNTIME_DIR", &runtime)
            .env("CURRICULUM_CONTEXT_LAYOUT", "2")
            .env(
                "CURRICULUM_CONTEXT_REFRESH_PROGRAM",
                std::env::var_os("FLOW_STATE_REFRESH_PROGRAM").unwrap(),
            )
            .env("CURRICULUM_ROLES_FILE", scratch.join("roles.datom"))
            .env("CURRICULUM_PSYCHES_REPOSITORY_DIR", &roots[0])
            .env("CURRICULUM_MIND_CONTEXT_DIR", &roots[1])
            .env("CURRICULUM_FIELD_CONTEXT_DIR", &roots[2])
            .env("CURRICULUM_RECORDS_DIR", records)
            .stdin(Stdio::null())
            .spawn()
            .expect("start the paired Curriculum Nexus"),
    );
    assert!(
        socket_directory.until_holds(Instant::now() + Duration::from_secs(10), &mut || {
            socket.exists()
        }),
        "the paired Curriculum Nexus did not bind"
    );
    let composer = CurriculumComposer::at(socket.to_str().unwrap());
    let state_name = "state:hosts:0";
    let state = state_name.curriculum_identity().unwrap();
    let knowledge = "knowledge:control:0".curriculum_identity().unwrap();

    let CurriculumExchange::Answered(Output::ModulesVerbose(catalog)) =
        Input::ModulesVerbose.emit(&composer)
    else {
        panic!("the shared discovery request must list module descriptions");
    };
    let state_listing = catalog
        .iter()
        .find(|listed| listed.module.module_identity == state)
        .expect("State is offered independently of startup selection");
    assert_eq!(
        state_listing.description_text.0,
        "Which of his hosts are away, since when and until when."
    );
    assert!(
        catalog
            .iter()
            .any(|row| row.module.module_identity == knowledge)
    );

    let resolve = |identity| {
        let CurriculumExchange::Answered(Output::SourcesResolved(mut rows)) =
            Input::ResolveSources(vec![identity]).emit(&composer)
        else {
            panic!("the on-demand request must resolve the module");
        };
        assert_eq!(rows.len(), 1);
        rows.pop().unwrap()
    };
    let state_source = resolve(state.clone());
    assert_eq!(state_source.module_identity, state);
    assert_eq!(state_source.repository_name.0, "field-pattern");
    assert_eq!(state_source.source_revision.0, field_revision);
    assert_eq!(state_source.source_relative_path.0, "state/hosts/0.md");
    let raw = fs::read(field_source.join("state/hosts/0.md")).unwrap();
    assert_eq!(
        state_source.source_sha256.0,
        format!("{:x}", Sha256::digest(raw))
    );
    assert!(!state_source.string.trim().is_empty());
    assert_eq!(resolve(knowledge).string.trim(), "Flow user control.");
    assert_eq!(
        composer.resolve_module(&signal_flow::ModuleIdentity(state_name.into())),
        crate::context::ModuleStanding::Held
    );

    for aspect in [Aspect::Psyche, Aspect::Mind, Aspect::Field] {
        let startup = request(
            Address {
                aspect,
                topic: flow_ethos::Topic("stateCheck".into()),
                model: Model::Sol,
            },
            &["spirit:core:0", "intent:main-flow:0"],
            &["knowledge:control:0"],
        );
        let composition = composer
            .compose_flow_context(&startup)
            .expect("compose normal selected modules");
        assert!(VerifiedContext::verified(&startup, &composition).is_ok());
        assert!(composition.system_text.0.contains("Flow system control."));
        assert!(
            composition
                .system_text
                .0
                .contains("Flow seat intent control.")
        );
        assert!(composition.user_text.0.contains("Flow user control."));
        for payload in [&composition.system_text.0, &composition.user_text.0] {
            assert!(!payload.contains(state_name));
            for line in state_source
                .string
                .lines()
                .filter(|line| !line.trim().is_empty())
            {
                assert!(!payload.contains(line.trim()), "State body entered startup");
            }
        }
        assert!(
            composition
                .context_provenance_vector
                .iter()
                .all(|row| row.module_identity != state)
        );
    }
}
